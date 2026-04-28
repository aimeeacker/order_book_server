#![allow(unsafe_op_in_unsafe_fn)]
#![allow(non_local_definitions, unused_qualifications)]

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{Decimal128Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{TimeZone, Utc};
use clap as _;
use env_logger as _;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, Pool, PooledConn, Value};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use pyo3::prelude::*;
use rustls as _;
use serde::Deserialize;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_OUTPUT_DIR: &str = "/home/aimee/hl_runtime/binance_book";
const DEFAULT_SYMBOLS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];
const DEFAULT_DEPTH_INTERVAL_MS: u64 = 100;
const DEFAULT_DECIMAL_SCALE: i8 = 8;
const PRICE_PRECISION: u8 = 8;
const PRICE_SCALE: i8 = 2;
const SZ_PRECISION: u8 = 8;
const SZ_SCALE: i8 = 3;
const NOTIONAL_PRECISION: u8 = 18;
const NOTIONAL_SCALE: i8 = 2;
const IMBALANCE_PRECISION: u8 = 4;
const IMBALANCE_SCALE: i8 = 3;
const DEFAULT_BOOKTICKER_ROW_GROUP_ROWS: usize = 250_000;
const DEFAULT_DEPTH_ROW_GROUP_ROWS: usize = 10_000;
const ROTATION_WINDOW_MS: i64 = 8 * 60 * 60 * 1000;
const DEFAULT_WALL_NOTIONAL_USD: f64 = 10_000_000.0;
const DEFAULT_ETH_WALL_NOTIONAL_USD: f64 = 5_000_000.0;
const DEFAULT_SEARCH_BPS: f64 = 50.0;
const DEFAULT_RECONNECT_DELAY_MS: u64 = 1_000;
const DEFAULT_MYSQL_HOST: &str = "172.22.0.198";
const DEFAULT_MYSQL_PORT: u16 = 3306;
const DEFAULT_MYSQL_USER: &str = "aimee";
const DEFAULT_MYSQL_PASSWORD: &str = "02011";
const DEFAULT_MYSQL_DATABASE: &str = "eventContract";
const DEFAULT_MYSQL_TABLE: &str = "binance_depth_minute";
const BINANCE_USDM_PUBLIC_WS_BASE: &str = "wss://fstream.binance.com/public/stream?streams=";
static RUSTLS_PROVIDER_INIT: Once = Once::new();

#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
}

impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_MYSQL_HOST.to_owned(),
            port: DEFAULT_MYSQL_PORT,
            user: DEFAULT_MYSQL_USER.to_owned(),
            password: DEFAULT_MYSQL_PASSWORD.to_owned(),
            database: DEFAULT_MYSQL_DATABASE.to_owned(),
            table: DEFAULT_MYSQL_TABLE.to_owned(),
        }
    }
}

impl MySqlConfig {
    fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            bail!("mysql host must not be empty");
        }
        if self.user.trim().is_empty() {
            bail!("mysql user must not be empty");
        }
        if self.database.trim().is_empty() {
            bail!("mysql database must not be empty");
        }
        validate_mysql_identifier(&self.table).context("invalid mysql table name")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RecorderConfig {
    pub output_dir: PathBuf,
    pub symbols: Vec<String>,
    pub depth_interval_ms: u64,
    pub decimal_scale: i8,
    pub bookticker_row_group_rows: usize,
    pub depth_row_group_rows: usize,
    pub wall_notional_usd: f64,
    pub symbol_wall_notional_usd: HashMap<String, f64>,
    pub search_bps: f64,
    pub reconnect_delay_ms: u64,
    pub mysql: Option<MySqlConfig>,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
            symbols: DEFAULT_SYMBOLS.iter().map(|symbol| (*symbol).to_owned()).collect(),
            depth_interval_ms: DEFAULT_DEPTH_INTERVAL_MS,
            decimal_scale: DEFAULT_DECIMAL_SCALE,
            bookticker_row_group_rows: DEFAULT_BOOKTICKER_ROW_GROUP_ROWS,
            depth_row_group_rows: DEFAULT_DEPTH_ROW_GROUP_ROWS,
            wall_notional_usd: DEFAULT_WALL_NOTIONAL_USD,
            symbol_wall_notional_usd: default_symbol_wall_notional_usd(),
            search_bps: DEFAULT_SEARCH_BPS,
            reconnect_delay_ms: DEFAULT_RECONNECT_DELAY_MS,
            mysql: Some(MySqlConfig::default()),
        }
    }
}

impl RecorderConfig {
    pub fn validate(&mut self) -> Result<()> {
        if self.symbols.is_empty() {
            bail!("at least one symbol is required");
        }
        self.symbols = self
            .symbols
            .iter()
            .map(|symbol| symbol.trim().to_ascii_uppercase())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        self.symbols.sort();
        self.symbols.dedup();
        if self.symbols.is_empty() {
            bail!("at least one non-empty symbol is required");
        }
        if !matches!(self.depth_interval_ms, 100 | 250 | 500) {
            bail!("depth_interval_ms must be one of 100, 250, or 500");
        }
        if !(0..=18).contains(&self.decimal_scale) {
            bail!("decimal_scale must be between 0 and 18");
        }
        if self.bookticker_row_group_rows == 0 || self.depth_row_group_rows == 0 {
            bail!("row group sizes must be positive");
        }
        if !self.wall_notional_usd.is_finite() || self.wall_notional_usd <= 0.0 {
            bail!("wall_notional_usd must be a positive finite number");
        }
        let mut normalized_symbol_thresholds = HashMap::new();
        for (symbol, threshold) in &self.symbol_wall_notional_usd {
            let symbol = symbol.trim().to_ascii_uppercase();
            if symbol.is_empty() {
                bail!("symbol_wall_notional_usd contains an empty symbol");
            }
            if !threshold.is_finite() || *threshold <= 0.0 {
                bail!("symbol_wall_notional_usd for {symbol} must be a positive finite number");
            }
            normalized_symbol_thresholds.insert(symbol, *threshold);
        }
        self.symbol_wall_notional_usd = normalized_symbol_thresholds;
        if !self.search_bps.is_finite() || self.search_bps <= 0.0 || self.search_bps >= 10_000.0 {
            bail!("search_bps must be a positive finite number below 10000");
        }
        if let Some(mysql) = &self.mysql {
            mysql.validate()?;
        }
        Ok(())
    }

    fn wall_notional_usd_for(&self, symbol: &str) -> f64 {
        self.symbol_wall_notional_usd.get(&symbol.to_ascii_uppercase()).copied().unwrap_or(self.wall_notional_usd)
    }

    fn stream_url(&self) -> String {
        let streams = self
            .symbols
            .iter()
            .flat_map(|symbol| {
                let symbol = symbol.to_ascii_lowercase();
                [
                    format!("{symbol}@bookTicker"),
                    format!("{symbol}@depth{}", depth_interval_suffix(self.depth_interval_ms)),
                ]
            })
            .collect::<Vec<_>>()
            .join("/");
        format!("{BINANCE_USDM_PUBLIC_WS_BASE}{streams}")
    }
}

pub fn default_symbol_wall_notional_usd() -> HashMap<String, f64> {
    HashMap::from([
        ("BTCUSDT".to_owned(), DEFAULT_WALL_NOTIONAL_USD),
        ("ETHUSDT".to_owned(), DEFAULT_ETH_WALL_NOTIONAL_USD),
    ])
}

pub fn parse_symbol_wall_notional_usd(values: &[String]) -> Result<HashMap<String, f64>> {
    let mut thresholds = HashMap::new();
    for value in values {
        let (symbol, threshold) = value
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid symbol threshold '{value}', expected SYMBOL=USD_NOTIONAL"))?;
        let symbol = symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            bail!("invalid symbol threshold '{value}', symbol is empty");
        }
        let threshold =
            threshold.trim().parse::<f64>().with_context(|| format!("invalid USD notional threshold for {symbol}"))?;
        if !threshold.is_finite() || threshold <= 0.0 {
            bail!("USD notional threshold for {symbol} must be a positive finite number");
        }
        thresholds.insert(symbol, threshold);
    }
    Ok(thresholds)
}

pub struct RecorderHandle {
    shutdown: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<Result<()>>>,
}

impl RecorderHandle {
    pub fn start(config: RecorderConfig) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let join = thread::spawn(move || run_recorder_blocking(config, thread_shutdown));
        Self { shutdown, join: Some(join) }
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn stop(&mut self) -> Result<()> {
        self.request_shutdown();
        if let Some(join) = self.join.take() {
            join.join().map_err(|_| anyhow!("binance recorder thread panicked"))??;
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(thread::JoinHandle::is_finished)
    }
}

impl Drop for RecorderHandle {
    fn drop(&mut self) {
        drop(self.stop());
    }
}

pub fn run_recorder_blocking(mut config: RecorderConfig, shutdown: Arc<AtomicBool>) -> Result<()> {
    config.validate()?;
    install_rustls_provider();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("binance-book")
        .build()
        .context("failed to build Tokio runtime")?;
    runtime.block_on(run_recorder(config, shutdown))
}

pub async fn run_recorder(mut config: RecorderConfig, shutdown: Arc<AtomicBool>) -> Result<()> {
    config.validate()?;
    install_rustls_provider();
    let mut runtime = RecorderRuntime::new(&config)?;
    let url = config.stream_url();
    info!(
        "starting Binance USDT-M recorder streams={} output_dir={} bookticker_row_group_rows={} depth_row_group_rows={}",
        config.symbols.join(","),
        config.output_dir.display(),
        config.bookticker_row_group_rows,
        config.depth_row_group_rows
    );

    while !shutdown.load(Ordering::SeqCst) {
        match run_connection(&url, &config, &shutdown, &mut runtime).await {
            Ok(()) if shutdown.load(Ordering::SeqCst) => break,
            Ok(()) => warn!("Binance websocket connection ended; reconnecting"),
            Err(err) => warn!("Binance websocket connection failed: {err:#}; reconnecting"),
        }
        if !shutdown.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(config.reconnect_delay_ms)).await;
        }
    }

    runtime.close()?;
    Ok(())
}

async fn run_connection(
    url: &str,
    config: &RecorderConfig,
    shutdown: &Arc<AtomicBool>,
    runtime: &mut RecorderRuntime,
) -> Result<()> {
    info!("connecting Binance websocket url={url}");
    let (ws_stream, _) = connect_async(url).await.context("failed to connect Binance websocket")?;
    info!("connected Binance websocket");
    let (mut write, mut read) = ws_stream.split();

    while !shutdown.load(Ordering::SeqCst) {
        let Some(message) = timeout(Duration::from_secs(1), read.next()).await.ok().flatten() else {
            continue;
        };
        match message.context("websocket read failed")? {
            Message::Text(text) => runtime.process_text(text.as_ref(), config)?,
            Message::Binary(_) => {}
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await.context("failed to send websocket pong")?
            }
            Message::Pong(_) => {}
            Message::Close(frame) => {
                warn!("Binance websocket closed: {frame:?}");
                break;
            }
            Message::Frame(_) => {}
        }
    }
    Ok(())
}

struct RecorderRuntime {
    decimal_scale: i8,
    symbols: Vec<String>,
    depth_row_group_rows: usize,
    bookticker_by_symbol: HashMap<String, BookTickerSymbolState>,
    depth_sink: ParquetSink,
    depth_mysql_sink: Option<DepthMysqlSink>,
    depth_rows: DepthCombinedRows,
    pending_depth_by_symbol: HashMap<String, DepthMinuteRow>,
    finalized_depth_by_minute: BTreeMap<i64, HashMap<String, DepthMinuteRow>>,
    books: HashMap<String, ObservedBook>,
}

struct BookTickerSymbolState {
    sink: ParquetSink,
    rows: BookTickerRows,
}

impl RecorderRuntime {
    fn new(config: &RecorderConfig) -> Result<Self> {
        fs::create_dir_all(&config.output_dir)
            .with_context(|| format!("failed to create output dir {}", config.output_dir.display()))?;
        let bookticker_schema = bookticker_schema();
        let mut bookticker_by_symbol = HashMap::new();
        for symbol in &config.symbols {
            let prefix = format!("binance_usdm_{}_bookticker", symbol.to_ascii_lowercase());
            bookticker_by_symbol.insert(
                symbol.clone(),
                BookTickerSymbolState {
                    sink: ParquetSink::new(
                        config.output_dir.clone(),
                        &prefix,
                        Arc::clone(&bookticker_schema),
                        config.bookticker_row_group_rows,
                    ),
                    rows: BookTickerRows::new(Arc::clone(&bookticker_schema)),
                },
            );
        }
        let depth_schema = depth_minute_schema(&config.symbols);
        let depth_sink = ParquetSink::new(
            config.output_dir.clone(),
            "binance_usdm_depth_minute",
            Arc::clone(&depth_schema),
            config.depth_row_group_rows,
        );
        let depth_mysql_sink = config.mysql.as_ref().and_then(|mysql_config| match DepthMysqlSink::new(mysql_config) {
            Ok(sink) => Some(sink),
            Err(err) => {
                warn!("Binance depth_minute MySQL sink disabled: {err:#}");
                None
            }
        });
        Ok(Self {
            decimal_scale: config.decimal_scale,
            symbols: config.symbols.clone(),
            depth_row_group_rows: config.depth_row_group_rows,
            bookticker_by_symbol,
            depth_sink,
            depth_mysql_sink,
            depth_rows: DepthCombinedRows::new(depth_schema, config.symbols.clone()),
            pending_depth_by_symbol: HashMap::new(),
            finalized_depth_by_minute: BTreeMap::new(),
            books: HashMap::new(),
        })
    }

    fn process_text(&mut self, text: &str, config: &RecorderConfig) -> Result<()> {
        let message: CombinedMessage =
            serde_json::from_str(text).context("failed to decode combined stream message")?;
        if message.stream.ends_with("@bookTicker") {
            let event: BookTickerEvent =
                serde_json::from_value(message.data).context("failed to decode bookTicker payload")?;
            self.push_bookticker(event, config)?;
        } else if message.stream.contains("@depth") {
            let event: DepthUpdateEvent =
                serde_json::from_value(message.data).context("failed to decode depth payload")?;
            self.push_depth(event, config)?;
        }
        Ok(())
    }

    fn push_bookticker(&mut self, event: BookTickerEvent, config: &RecorderConfig) -> Result<()> {
        let row = BookTickerRow::from_event(event, self.decimal_scale)?;
        self.books.entry(row.symbol.clone()).or_default().set_best_quote(&row);
        let symbol = row.symbol.clone();
        let state = self
            .bookticker_by_symbol
            .get_mut(&symbol)
            .ok_or_else(|| anyhow!("received bookTicker for unconfigured symbol {symbol}"))?;
        rotate_sink_for_ts(&mut state.sink, &mut state.rows, self.decimal_scale, row.transaction_time_ms)?;
        state.rows.push(row);
        if state.rows.len() >= config.bookticker_row_group_rows {
            flush_rows(&mut state.sink, &mut state.rows, self.decimal_scale)?;
        }
        Ok(())
    }

    fn push_depth(&mut self, event: DepthUpdateEvent, config: &RecorderConfig) -> Result<()> {
        let symbol = event.symbol.clone();
        let book = self.books.entry(symbol.clone()).or_default();
        book.apply_depth_update(&event, config.decimal_scale)?;
        let Some(row) = book.depth_minute_row(event, config)? else {
            return Ok(());
        };
        if let Some(previous) = self.pending_depth_by_symbol.get(&symbol)
            && previous.minute_start_time_ms != row.minute_start_time_ms
        {
            let previous = self
                .pending_depth_by_symbol
                .remove(&symbol)
                .ok_or_else(|| anyhow!("missing pending depth row for {symbol}"))?;
            self.finalized_depth_by_minute
                .entry(previous.minute_start_time_ms)
                .or_default()
                .insert(symbol.clone(), previous);
            self.emit_ready_depth_minutes()?;
        }
        self.pending_depth_by_symbol.insert(symbol, row);
        if self.depth_rows.len() >= config.depth_row_group_rows {
            self.flush_depth()?;
        }
        Ok(())
    }

    fn flush_bookticker(&mut self) -> Result<()> {
        for state in self.bookticker_by_symbol.values_mut() {
            flush_rows(&mut state.sink, &mut state.rows, self.decimal_scale)?;
        }
        Ok(())
    }

    fn flush_depth(&mut self) -> Result<()> {
        if let Some(batch) = self.depth_rows.take_batch(self.decimal_scale)? {
            self.depth_sink.write(&batch)?;
        }
        Ok(())
    }

    fn rotate_depth_for_ts(&mut self, timestamp_ms: i64) -> Result<()> {
        if self.depth_sink.needs_rotation(timestamp_ms) {
            self.flush_depth()?;
            self.depth_sink.ensure_window(timestamp_ms)?;
        } else {
            self.depth_sink.ensure_window(timestamp_ms)?;
        }
        Ok(())
    }

    fn emit_ready_depth_minutes(&mut self) -> Result<()> {
        loop {
            let Some((&minute_start_time_ms, rows_by_symbol)) = self.finalized_depth_by_minute.iter().next() else {
                break;
            };
            if !self.symbols.iter().all(|symbol| rows_by_symbol.contains_key(symbol)) {
                break;
            }
            let mut rows_by_symbol = self
                .finalized_depth_by_minute
                .remove(&minute_start_time_ms)
                .ok_or_else(|| anyhow!("missing finalized depth minute {minute_start_time_ms}"))?;
            let mut rows = Vec::with_capacity(self.symbols.len());
            for symbol in &self.symbols {
                let row = rows_by_symbol
                    .remove(symbol)
                    .ok_or_else(|| anyhow!("missing finalized depth row for {symbol} at {minute_start_time_ms}"))?;
                rows.push(row);
            }
            let minute_close_time_ms =
                rows.first().map_or(minute_start_time_ms + 59_999, |row| row.minute_close_time_ms);
            self.rotate_depth_for_ts(minute_close_time_ms)?;
            let combined_row = DepthCombinedRow { minute_close_time_ms, rows };
            if let Some(mysql_sink) = self.depth_mysql_sink.as_mut()
                && let Err(err) = mysql_sink.upsert_depth_minute(&self.symbols, &combined_row, self.decimal_scale)
            {
                warn!("failed to upsert Binance depth_minute row to MySQL: {err:#}");
            }
            self.depth_rows.push(combined_row);
            if self.depth_rows.len() >= self.depth_row_group_rows {
                self.flush_depth()?;
            }
        }
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.emit_ready_depth_minutes()?;
        self.flush_bookticker()?;
        self.flush_depth()?;
        let mut bookticker_paths = Vec::new();
        for state in self.bookticker_by_symbol.into_values() {
            bookticker_paths.extend(state.sink.close()?);
        }
        let depth_paths = self.depth_sink.close()?;
        info!(
            "closed Binance parquet bookticker_files={} depth_minute_files={}",
            format_paths(&bookticker_paths),
            format_paths(&depth_paths)
        );
        Ok(())
    }
}

struct ParquetSink {
    output_dir: PathBuf,
    file_prefix: String,
    schema: Arc<Schema>,
    row_group_rows: usize,
    current_window_start_ms: Option<i64>,
    tmp_path: Option<PathBuf>,
    final_path: Option<PathBuf>,
    writer: Option<ArrowWriter<File>>,
    rows_written: usize,
    finalized_paths: Vec<PathBuf>,
}

impl ParquetSink {
    fn new(output_dir: PathBuf, file_prefix: &str, schema: Arc<Schema>, row_group_rows: usize) -> Self {
        Self {
            output_dir,
            file_prefix: file_prefix.to_owned(),
            schema,
            row_group_rows,
            current_window_start_ms: None,
            tmp_path: None,
            final_path: None,
            writer: None,
            rows_written: 0,
            finalized_paths: Vec::new(),
        }
    }

    fn needs_rotation(&self, timestamp_ms: i64) -> bool {
        self.current_window_start_ms.is_some_and(|start| rotation_window_start_ms(timestamp_ms) != start)
    }

    fn ensure_window(&mut self, timestamp_ms: i64) -> Result<()> {
        let window_start_ms = rotation_window_start_ms(timestamp_ms);
        if self.current_window_start_ms == Some(window_start_ms) {
            return Ok(());
        }
        self.close_current()?;
        let final_path = self.final_path_for_window(window_start_ms);
        let tmp_path = final_path.with_extension("parquet.tmp");
        let file = File::create(&tmp_path).with_context(|| format!("failed to create {}", tmp_path.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_size(self.row_group_rows)
            .build();
        let writer = ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(props))
            .context("failed to create parquet writer")?;
        info!("[file] open parquet_tmp={} window_start_utc={}", tmp_path.display(), format_ts(window_start_ms));
        self.current_window_start_ms = Some(window_start_ms);
        self.tmp_path = Some(tmp_path);
        self.final_path = Some(final_path);
        self.writer = Some(writer);
        self.rows_written = 0;
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| anyhow!("parquet writer already closed"))?;
        writer.write(batch).context("failed to write parquet batch")?;
        writer.flush().context("failed to flush parquet writer")?;
        self.rows_written = self.rows_written.saturating_add(batch.num_rows());
        Ok(())
    }

    fn close(mut self) -> Result<Vec<PathBuf>> {
        self.close_current()?;
        Ok(self.finalized_paths)
    }

    fn close_current(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().context("failed to close parquet writer")?;
        }
        let Some(tmp_path) = self.tmp_path.take() else {
            self.current_window_start_ms = None;
            self.final_path = None;
            self.rows_written = 0;
            return Ok(());
        };
        let final_path = self.final_path.take().ok_or_else(|| anyhow!("missing final parquet path"))?;
        fs::rename(&tmp_path, &final_path)
            .with_context(|| format!("failed to rename parquet {} -> {}", tmp_path.display(), final_path.display()))?;
        info!("[file] close parquet={} rows={}", final_path.display(), self.rows_written);
        self.finalized_paths.push(final_path);
        self.current_window_start_ms = None;
        self.rows_written = 0;
        Ok(())
    }

    fn final_path_for_window(&self, window_start_ms: i64) -> PathBuf {
        let window_end_ms = window_start_ms + ROTATION_WINDOW_MS - 1;
        self.output_dir.join(format!(
            "{}_{}_{}.parquet",
            self.file_prefix,
            format_ts(window_start_ms),
            format_ts(window_end_ms)
        ))
    }
}

struct DepthMysqlSink {
    pool: Pool,
    table: String,
}

impl DepthMysqlSink {
    fn new(config: &MySqlConfig) -> Result<Self> {
        config.validate()?;
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(config.host.clone()))
            .tcp_port(config.port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()));
        let pool = Pool::new(opts).context("failed to create MySQL pool")?;
        let sink = Self { pool, table: config.table.clone() };
        drop(sink.conn()?);
        info!("enabled Binance depth_minute MySQL sink table={}", sink.table);
        Ok(sink)
    }

    fn conn(&self) -> Result<PooledConn> {
        self.pool.get_conn().context("failed to get MySQL connection")
    }

    fn upsert_depth_minute(&mut self, symbols: &[String], row: &DepthCombinedRow, scale: i8) -> Result<()> {
        if symbols.len() != row.rows.len() {
            bail!("depth MySQL row symbol count mismatch");
        }
        let mut columns = vec!["ts".to_owned()];
        let mut values = vec![Value::from(mysql_datetime_utc_ms(row.minute_close_time_ms))];
        for (symbol, symbol_row) in symbols.iter().zip(row.rows.iter()) {
            let suffix = symbol_suffix(symbol);
            append_depth_mysql_values(&suffix, symbol_row, scale, &mut columns, &mut values);
        }
        let placeholders = std::iter::repeat_n("?", columns.len()).collect::<Vec<_>>().join(", ");
        let assignments = columns
            .iter()
            .filter(|column| column.as_str() != "ts")
            .map(|column| format!("`{column}`=VALUES(`{column}`)"))
            .chain(std::iter::once("`u_at`=CURRENT_TIMESTAMP(3)".to_owned()))
            .collect::<Vec<_>>()
            .join(", ");
        let column_list = columns.iter().map(|column| format!("`{column}`")).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "INSERT INTO `{}` ({column_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {assignments}",
            self.table
        );
        self.conn()?.exec_drop(sql, values).context("failed to upsert MySQL depth_minute row")
    }
}

fn append_depth_mysql_values(
    suffix: &str,
    row: &DepthMinuteRow,
    scale: i8,
    columns: &mut Vec<String>,
    values: &mut Vec<Value>,
) {
    columns.extend([
        format!("bb_px_{suffix}"),
        format!("bb_sz_{suffix}"),
        format!("ba_px_{suffix}"),
        format!("ba_sz_{suffix}"),
        format!("sup_px_{suffix}"),
        format!("sup_sz_{suffix}"),
        format!("sup_not_{suffix}"),
        format!("res_px_{suffix}"),
        format!("res_sz_{suffix}"),
        format!("res_not_{suffix}"),
    ]);
    values.extend([
        mysql_decimal_value(row.best_bid_px, scale, PRICE_SCALE),
        mysql_decimal_value(row.best_bid_qty, scale, SZ_SCALE),
        mysql_decimal_value(row.best_ask_px, scale, PRICE_SCALE),
        mysql_decimal_value(row.best_ask_qty, scale, SZ_SCALE),
        nullable_wall_decimal_value(row.support.as_ref(), |wall| wall.px, scale, PRICE_SCALE),
        nullable_wall_decimal_value(row.support.as_ref(), |wall| wall.qty, scale, SZ_SCALE),
        nullable_wall_decimal_value(row.support.as_ref(), |wall| wall.notional, scale, NOTIONAL_SCALE),
        nullable_wall_decimal_value(row.resistance.as_ref(), |wall| wall.px, scale, PRICE_SCALE),
        nullable_wall_decimal_value(row.resistance.as_ref(), |wall| wall.qty, scale, SZ_SCALE),
        nullable_wall_decimal_value(row.resistance.as_ref(), |wall| wall.notional, scale, NOTIONAL_SCALE),
    ]);
}

fn rotate_sink_for_ts(
    sink: &mut ParquetSink,
    rows: &mut BookTickerRows,
    decimal_scale: i8,
    timestamp_ms: i64,
) -> Result<()> {
    if sink.needs_rotation(timestamp_ms) {
        flush_rows(sink, rows, decimal_scale)?;
    }
    sink.ensure_window(timestamp_ms)?;
    Ok(())
}

fn flush_rows(sink: &mut ParquetSink, rows: &mut BookTickerRows, decimal_scale: i8) -> Result<()> {
    if let Some(batch) = rows.take_batch(decimal_scale)? {
        sink.write(&batch)?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct CombinedMessage {
    stream: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct BookTickerEvent {
    #[serde(rename = "T")]
    transaction_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid_px: String,
    #[serde(rename = "B")]
    bid_qty: String,
    #[serde(rename = "a")]
    ask_px: String,
    #[serde(rename = "A")]
    ask_qty: String,
}

#[derive(Debug)]
struct BookTickerRow {
    transaction_time_ms: i64,
    symbol: String,
    bid_px: i128,
    bid_qty: i128,
    ask_px: i128,
    ask_qty: i128,
    bid_notional: i128,
    ask_notional: i128,
    imbalance: i128,
}

impl BookTickerRow {
    fn from_event(event: BookTickerEvent, scale: i8) -> Result<Self> {
        let bid_px = parse_decimal_scaled(&event.bid_px, scale)?;
        let bid_qty = parse_decimal_scaled(&event.bid_qty, scale)?;
        let ask_px = parse_decimal_scaled(&event.ask_px, scale)?;
        let ask_qty = parse_decimal_scaled(&event.ask_qty, scale)?;
        let bid_notional = scaled_notional(bid_px, bid_qty, scale);
        let ask_notional = scaled_notional(ask_px, ask_qty, scale);
        let imbalance = scaled_ratio(bid_notional - ask_notional, bid_notional + ask_notional, IMBALANCE_SCALE);
        Ok(Self {
            transaction_time_ms: event.transaction_time_ms,
            symbol: event.symbol,
            bid_px,
            bid_qty,
            ask_px,
            ask_qty,
            bid_notional,
            ask_notional,
            imbalance,
        })
    }
}

#[derive(Debug, Deserialize)]
struct DepthUpdateEvent {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone)]
struct Level {
    px: i128,
    qty: i128,
}

#[derive(Debug, Clone)]
struct WallLevel {
    px: i128,
    qty: i128,
    notional: i128,
}

#[derive(Debug, Default)]
struct ObservedBook {
    bids: BTreeMap<i128, i128>,
    asks: BTreeMap<i128, i128>,
    best_bid: Option<Level>,
    best_ask: Option<Level>,
}

impl ObservedBook {
    fn set_best_quote(&mut self, row: &BookTickerRow) {
        self.best_bid = Some(Level { px: row.bid_px, qty: row.bid_qty });
        self.best_ask = Some(Level { px: row.ask_px, qty: row.ask_qty });
    }

    fn apply_depth_update(&mut self, event: &DepthUpdateEvent, scale: i8) -> Result<()> {
        apply_side_updates(&mut self.bids, &event.bids, scale)?;
        apply_side_updates(&mut self.asks, &event.asks, scale)?;
        Ok(())
    }

    fn depth_minute_row(&self, event: DepthUpdateEvent, config: &RecorderConfig) -> Result<Option<DepthMinuteRow>> {
        let (Some(best_bid), Some(best_ask)) = (self.best_bid.as_ref(), self.best_ask.as_ref()) else {
            return Ok(None);
        };
        let reference_px = (best_bid.px + best_ask.px) / 2;
        if reference_px <= 0 {
            return Ok(None);
        }
        let lower_px = bps_adjusted_px(reference_px, -config.search_bps);
        let upper_px = bps_adjusted_px(reference_px, config.search_bps);
        let threshold = scaled_usd(config.wall_notional_usd_for(&event.symbol), config.decimal_scale)?;
        let support = find_support_wall(&self.bids, reference_px, lower_px, threshold, config.decimal_scale);
        let resistance = find_resistance_wall(&self.asks, reference_px, upper_px, threshold, config.decimal_scale);
        let minute_start_time_ms = event.event_time_ms - event.event_time_ms.rem_euclid(60_000);
        Ok(Some(DepthMinuteRow {
            minute_start_time_ms,
            minute_close_time_ms: minute_start_time_ms + 59_999,
            best_bid_px: best_bid.px,
            best_bid_qty: best_bid.qty,
            best_ask_px: best_ask.px,
            best_ask_qty: best_ask.qty,
            support,
            resistance,
        }))
    }
}

#[derive(Debug)]
struct DepthMinuteRow {
    minute_start_time_ms: i64,
    minute_close_time_ms: i64,
    best_bid_px: i128,
    best_bid_qty: i128,
    best_ask_px: i128,
    best_ask_qty: i128,
    support: Option<WallLevel>,
    resistance: Option<WallLevel>,
}

struct BookTickerRows {
    schema: Arc<Schema>,
    rows: Vec<BookTickerRow>,
}

impl BookTickerRows {
    fn new(schema: Arc<Schema>) -> Self {
        Self { schema, rows: Vec::new() }
    }

    fn push(&mut self, row: BookTickerRow) {
        self.rows.push(row);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn take_batch(&mut self, scale: i8) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let mut transaction_time = timestamp_builder(self.rows.len());
        let mut symbol = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 8);
        let mut bid_px = price_builder(self.rows.len());
        let mut bid_sz = sz_builder(self.rows.len());
        let mut ask_px = price_builder(self.rows.len());
        let mut ask_sz = sz_builder(self.rows.len());
        let mut bid_notional = notional_builder(self.rows.len());
        let mut ask_notional = notional_builder(self.rows.len());
        let mut imbalance = imbalance_builder(self.rows.len());

        for row in self.rows.drain(..) {
            transaction_time.append_value(row.transaction_time_ms);
            symbol.append_value(&row.symbol);
            bid_px.append_value(rescale_decimal(row.bid_px, scale, PRICE_SCALE));
            bid_sz.append_value(rescale_decimal(row.bid_qty, scale, SZ_SCALE));
            ask_px.append_value(rescale_decimal(row.ask_px, scale, PRICE_SCALE));
            ask_sz.append_value(rescale_decimal(row.ask_qty, scale, SZ_SCALE));
            bid_notional.append_value(rescale_decimal(row.bid_notional, scale, NOTIONAL_SCALE));
            ask_notional.append_value(rescale_decimal(row.ask_notional, scale, NOTIONAL_SCALE));
            imbalance.append_value(row.imbalance);
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(transaction_time.finish()),
            Arc::new(symbol.finish()),
            Arc::new(bid_px.finish()),
            Arc::new(bid_sz.finish()),
            Arc::new(ask_px.finish()),
            Arc::new(ask_sz.finish()),
            Arc::new(bid_notional.finish()),
            Arc::new(ask_notional.finish()),
            Arc::new(imbalance.finish()),
        ];
        Ok(Some(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?))
    }
}

struct DepthCombinedRow {
    minute_close_time_ms: i64,
    rows: Vec<DepthMinuteRow>,
}

struct DepthCombinedRows {
    schema: Arc<Schema>,
    symbols: Vec<String>,
    rows: Vec<DepthCombinedRow>,
}

impl DepthCombinedRows {
    fn new(schema: Arc<Schema>, symbols: Vec<String>) -> Self {
        Self { schema, symbols, rows: Vec::new() }
    }

    fn push(&mut self, row: DepthCombinedRow) {
        self.rows.push(row);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn take_batch(&mut self, scale: i8) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let mut minute_close_time = timestamp_builder(self.rows.len());

        let mut symbol_builders =
            self.symbols.iter().map(|_| DepthSymbolBuilders::new(self.rows.len())).collect::<Vec<_>>();

        for row in self.rows.drain(..) {
            minute_close_time.append_value(row.minute_close_time_ms);
            if row.rows.len() != symbol_builders.len() {
                bail!("depth combined row symbol count mismatch");
            }
            for (builder, symbol_row) in symbol_builders.iter_mut().zip(row.rows.iter()) {
                builder.append(symbol_row, scale);
            }
        }

        let arrays: Vec<ArrayRef> = vec![Arc::new(minute_close_time.finish())];
        let mut arrays = arrays;
        for builder in symbol_builders {
            builder.finish_into(&mut arrays);
        }
        Ok(Some(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?))
    }
}

struct DepthSymbolBuilders {
    best_bid_px: Decimal128Builder,
    best_bid_sz: Decimal128Builder,
    best_ask_px: Decimal128Builder,
    best_ask_sz: Decimal128Builder,
    support_px: Decimal128Builder,
    support_sz: Decimal128Builder,
    support_notional: Decimal128Builder,
    resistance_px: Decimal128Builder,
    resistance_sz: Decimal128Builder,
    resistance_notional: Decimal128Builder,
}

impl DepthSymbolBuilders {
    fn new(capacity: usize) -> Self {
        Self {
            best_bid_px: price_builder(capacity),
            best_bid_sz: sz_builder(capacity),
            best_ask_px: price_builder(capacity),
            best_ask_sz: sz_builder(capacity),
            support_px: nullable_price_builder(capacity),
            support_sz: nullable_sz_builder(capacity),
            support_notional: nullable_notional_builder(capacity),
            resistance_px: nullable_price_builder(capacity),
            resistance_sz: nullable_sz_builder(capacity),
            resistance_notional: nullable_notional_builder(capacity),
        }
    }

    fn append(&mut self, row: &DepthMinuteRow, scale: i8) {
        self.best_bid_px.append_value(rescale_decimal(row.best_bid_px, scale, PRICE_SCALE));
        self.best_bid_sz.append_value(rescale_decimal(row.best_bid_qty, scale, SZ_SCALE));
        self.best_ask_px.append_value(rescale_decimal(row.best_ask_px, scale, PRICE_SCALE));
        self.best_ask_sz.append_value(rescale_decimal(row.best_ask_qty, scale, SZ_SCALE));
        append_wall(
            &mut self.support_px,
            &mut self.support_sz,
            &mut self.support_notional,
            row.support.as_ref(),
            scale,
        );
        append_wall(
            &mut self.resistance_px,
            &mut self.resistance_sz,
            &mut self.resistance_notional,
            row.resistance.as_ref(),
            scale,
        );
    }

    fn finish_into(mut self, arrays: &mut Vec<ArrayRef>) {
        arrays.push(Arc::new(self.best_bid_px.finish()));
        arrays.push(Arc::new(self.best_bid_sz.finish()));
        arrays.push(Arc::new(self.best_ask_px.finish()));
        arrays.push(Arc::new(self.best_ask_sz.finish()));
        arrays.push(Arc::new(self.support_px.finish()));
        arrays.push(Arc::new(self.support_sz.finish()));
        arrays.push(Arc::new(self.support_notional.finish()));
        arrays.push(Arc::new(self.resistance_px.finish()));
        arrays.push(Arc::new(self.resistance_sz.finish()));
        arrays.push(Arc::new(self.resistance_notional.finish()));
    }
}

fn bookticker_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("transaction_time"),
        Field::new("symbol", DataType::Utf8, false),
        price_field("bid_px"),
        sz_field("bid_sz"),
        price_field("ask_px"),
        sz_field("ask_sz"),
        notional_field("bid_notional"),
        notional_field("ask_notional"),
        imbalance_field("imbalance"),
    ]))
}

fn depth_minute_schema(symbols: &[String]) -> Arc<Schema> {
    let mut fields = vec![timestamp_field("minute_close_time")];
    for symbol in symbols {
        let suffix = symbol_suffix(symbol);
        fields.extend([
            price_field(&format!("best_bid_px_{suffix}")),
            sz_field(&format!("best_bid_sz_{suffix}")),
            price_field(&format!("best_ask_px_{suffix}")),
            sz_field(&format!("best_ask_sz_{suffix}")),
            nullable_price_field(&format!("support_px_{suffix}")),
            nullable_sz_field(&format!("support_sz_{suffix}")),
            nullable_notional_field(&format!("support_notional_{suffix}")),
            nullable_price_field(&format!("resistance_px_{suffix}")),
            nullable_sz_field(&format!("resistance_sz_{suffix}")),
            nullable_notional_field(&format!("resistance_notional_{suffix}")),
        ]);
    }
    Arc::new(Schema::new(fields))
}

fn timestamp_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false)
}

fn price_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(PRICE_PRECISION, PRICE_SCALE), false)
}

fn sz_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(SZ_PRECISION, SZ_SCALE), false)
}

fn notional_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(NOTIONAL_PRECISION, NOTIONAL_SCALE), false)
}

fn imbalance_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(IMBALANCE_PRECISION, IMBALANCE_SCALE), false)
}

fn nullable_price_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(PRICE_PRECISION, PRICE_SCALE), true)
}

fn nullable_sz_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(SZ_PRECISION, SZ_SCALE), true)
}

fn nullable_notional_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(NOTIONAL_PRECISION, NOTIONAL_SCALE), true)
}

fn timestamp_builder(capacity: usize) -> TimestampMillisecondBuilder {
    TimestampMillisecondBuilder::with_capacity(capacity)
        .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())))
}

fn price_builder(capacity: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity).with_data_type(DataType::Decimal128(PRICE_PRECISION, PRICE_SCALE))
}

fn sz_builder(capacity: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity).with_data_type(DataType::Decimal128(SZ_PRECISION, SZ_SCALE))
}

fn notional_builder(capacity: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity).with_data_type(DataType::Decimal128(NOTIONAL_PRECISION, NOTIONAL_SCALE))
}

fn imbalance_builder(capacity: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity)
        .with_data_type(DataType::Decimal128(IMBALANCE_PRECISION, IMBALANCE_SCALE))
}

fn nullable_price_builder(capacity: usize) -> Decimal128Builder {
    price_builder(capacity)
}

fn nullable_sz_builder(capacity: usize) -> Decimal128Builder {
    sz_builder(capacity)
}

fn nullable_notional_builder(capacity: usize) -> Decimal128Builder {
    notional_builder(capacity)
}

fn depth_interval_suffix(interval_ms: u64) -> &'static str {
    match interval_ms {
        100 => "@100ms",
        500 => "@500ms",
        _ => "",
    }
}

fn apply_side_updates(side: &mut BTreeMap<i128, i128>, values: &[[String; 2]], scale: i8) -> Result<()> {
    for [px, qty] in values {
        let px = parse_decimal_scaled(px, scale)?;
        let qty = parse_decimal_scaled(qty, scale)?;
        if qty == 0 {
            side.remove(&px);
        } else {
            side.insert(px, qty);
        }
    }
    Ok(())
}

fn find_support_wall(
    bids: &BTreeMap<i128, i128>,
    reference_px: i128,
    lower_px: i128,
    threshold: i128,
    scale: i8,
) -> Option<WallLevel> {
    for (&px, &qty) in bids.range(lower_px..=reference_px).rev() {
        let notional = scaled_notional(px, qty, scale);
        if notional >= threshold {
            return Some(WallLevel { px, qty, notional });
        }
    }
    None
}

fn find_resistance_wall(
    asks: &BTreeMap<i128, i128>,
    reference_px: i128,
    upper_px: i128,
    threshold: i128,
    scale: i8,
) -> Option<WallLevel> {
    for (&px, &qty) in asks.range(reference_px..=upper_px) {
        let notional = scaled_notional(px, qty, scale);
        if notional >= threshold {
            return Some(WallLevel { px, qty, notional });
        }
    }
    None
}

fn bps_adjusted_px(px: i128, bps: f64) -> i128 {
    (px as f64 * (1.0 + bps / 10_000.0)).round().max(0.0) as i128
}

fn scaled_usd(value: f64, scale: i8) -> Result<i128> {
    let factor = 10_i128.pow(u32::try_from(scale)?);
    Ok((value * factor as f64).round() as i128)
}

fn mysql_datetime_utc_ms(timestamp_ms: i64) -> String {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().expect("unix epoch is valid"))
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn mysql_decimal_value(value: i128, from_scale: i8, to_scale: i8) -> Value {
    Value::from(format_scaled_decimal(rescale_decimal(value, from_scale, to_scale), to_scale))
}

fn nullable_wall_decimal_value(
    wall: Option<&WallLevel>,
    value: impl FnOnce(&WallLevel) -> i128,
    from_scale: i8,
    to_scale: i8,
) -> Value {
    wall.map_or(Value::NULL, |wall| mysql_decimal_value(value(wall), from_scale, to_scale))
}

fn format_scaled_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let factor = 10_i128.pow(u32::try_from(scale).unwrap_or(0));
    let sign = if value < 0 { "-" } else { "" };
    let abs_value = value.abs();
    format!("{sign}{}.{:0width$}", abs_value / factor, abs_value % factor, width = usize::try_from(scale).unwrap_or(0))
}

fn validate_mysql_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        bail!("identifier is empty");
    }
    if !identifier.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        bail!("identifier '{identifier}' contains unsupported characters");
    }
    Ok(())
}

fn append_wall(
    px: &mut Decimal128Builder,
    sz: &mut Decimal128Builder,
    notional: &mut Decimal128Builder,
    wall: Option<&WallLevel>,
    scale: i8,
) {
    if let Some(wall) = wall {
        px.append_value(rescale_decimal(wall.px, scale, PRICE_SCALE));
        sz.append_value(rescale_decimal(wall.qty, scale, SZ_SCALE));
        notional.append_value(rescale_decimal(wall.notional, scale, NOTIONAL_SCALE));
    } else {
        px.append_null();
        sz.append_null();
        notional.append_null();
    }
}

fn parse_decimal_scaled(value: &str, scale: i8) -> Result<i128> {
    let value = value.trim();
    if value.is_empty() {
        bail!("empty decimal value");
    }
    let scale = usize::try_from(scale)?;
    let (negative, digits) = value.strip_prefix('-').map_or((false, value), |rest| (true, rest));
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if integer.is_empty() && fraction.is_empty() {
        bail!("invalid decimal value '{value}'");
    }
    let mut normalized = String::with_capacity(integer.len() + scale);
    normalized.push_str(if integer.is_empty() { "0" } else { integer });
    let kept_fraction = fraction.chars().take(scale).collect::<String>();
    normalized.push_str(&kept_fraction);
    for _ in kept_fraction.len()..scale {
        normalized.push('0');
    }
    if !normalized.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("invalid decimal value '{value}'");
    }
    let parsed = normalized.parse::<i128>().with_context(|| format!("invalid decimal value '{value}'"))?;
    Ok(if negative { -parsed } else { parsed })
}

fn scaled_notional(px: i128, qty: i128, scale: i8) -> i128 {
    let factor = 10_i128.pow(u32::try_from(scale).unwrap_or(0));
    px.saturating_mul(qty) / factor
}

fn scaled_ratio(numerator: i128, denominator: i128, scale: i8) -> i128 {
    if denominator == 0 {
        return 0;
    }
    let factor = 10_i128.pow(u32::try_from(scale).unwrap_or(0));
    let scaled = numerator.saturating_mul(factor);
    if scaled >= 0 { (scaled + denominator / 2) / denominator } else { (scaled - denominator / 2) / denominator }
}

fn rotation_window_start_ms(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(ROTATION_WINDOW_MS) * ROTATION_WINDOW_MS
}

fn format_ts(timestamp_ms: i64) -> String {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().expect("unix epoch is valid"))
        .format("%Y%m%dT%H%M%SZ")
        .to_string()
}

fn format_paths(paths: &[PathBuf]) -> String {
    if paths.is_empty() {
        return "[]".to_owned();
    }
    paths.iter().map(|path| path.display().to_string()).collect::<Vec<_>>().join(",")
}

fn symbol_suffix(symbol: &str) -> String {
    let mut suffix = symbol.trim().to_ascii_lowercase();
    for quote in ["usdt", "usdc", "busd"] {
        if let Some(base) = suffix.strip_suffix(quote) {
            suffix = base.to_owned();
            break;
        }
    }
    suffix
}

fn rescale_decimal(value: i128, from_scale: i8, to_scale: i8) -> i128 {
    match from_scale.cmp(&to_scale) {
        std::cmp::Ordering::Equal => value,
        std::cmp::Ordering::Less => {
            let factor = 10_i128.pow(u32::try_from(to_scale - from_scale).unwrap_or(0));
            value.saturating_mul(factor)
        }
        std::cmp::Ordering::Greater => {
            let divisor = 10_i128.pow(u32::try_from(from_scale - to_scale).unwrap_or(0));
            if value >= 0 { (value + divisor / 2) / divisor } else { (value - divisor / 2) / divisor }
        }
    }
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        drop(rustls::crypto::ring::default_provider().install_default());
    });
}

#[pyclass(unsendable, name = "BinanceRecorder")]
struct PyBinanceRecorder {
    handle: Option<RecorderHandle>,
}

#[pymethods]
impl PyBinanceRecorder {
    fn request_shutdown(&self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.request_shutdown();
        }
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(mut handle) = self.handle.take() {
            handle.stop().map_err(to_py_runtime_error)?;
        }
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(RecorderHandle::is_finished)
    }
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    output_dir=None,
    symbols=None,
    depth_interval_ms=DEFAULT_DEPTH_INTERVAL_MS,
    decimal_scale=DEFAULT_DECIMAL_SCALE,
    bookticker_row_group_rows=DEFAULT_BOOKTICKER_ROW_GROUP_ROWS,
    depth_row_group_rows=DEFAULT_DEPTH_ROW_GROUP_ROWS,
    wall_notional_usd=DEFAULT_WALL_NOTIONAL_USD,
    symbol_wall_notional_usd=None,
    search_bps=DEFAULT_SEARCH_BPS,
    reconnect_delay_ms=DEFAULT_RECONNECT_DELAY_MS,
    mysql_enabled=true,
    mysql_host=None,
    mysql_port=DEFAULT_MYSQL_PORT,
    mysql_user=None,
    mysql_password=None,
    mysql_db=None,
    mysql_table=None
))]
fn start_recorder(
    output_dir: Option<PathBuf>,
    symbols: Option<Vec<String>>,
    depth_interval_ms: u64,
    decimal_scale: i8,
    bookticker_row_group_rows: usize,
    depth_row_group_rows: usize,
    wall_notional_usd: f64,
    symbol_wall_notional_usd: Option<HashMap<String, f64>>,
    search_bps: f64,
    reconnect_delay_ms: u64,
    mysql_enabled: bool,
    mysql_host: Option<String>,
    mysql_port: u16,
    mysql_user: Option<String>,
    mysql_password: Option<String>,
    mysql_db: Option<String>,
    mysql_table: Option<String>,
) -> PyResult<PyBinanceRecorder> {
    let mut config = RecorderConfig {
        output_dir: output_dir.unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_DIR)),
        symbols: symbols.unwrap_or_else(|| DEFAULT_SYMBOLS.iter().map(|symbol| (*symbol).to_owned()).collect()),
        depth_interval_ms,
        decimal_scale,
        bookticker_row_group_rows,
        depth_row_group_rows,
        wall_notional_usd,
        symbol_wall_notional_usd: symbol_wall_notional_usd.unwrap_or_else(default_symbol_wall_notional_usd),
        search_bps,
        reconnect_delay_ms,
        mysql: mysql_enabled.then(|| MySqlConfig {
            host: mysql_host.unwrap_or_else(|| DEFAULT_MYSQL_HOST.to_owned()),
            port: mysql_port,
            user: mysql_user.unwrap_or_else(|| DEFAULT_MYSQL_USER.to_owned()),
            password: mysql_password.unwrap_or_else(|| DEFAULT_MYSQL_PASSWORD.to_owned()),
            database: mysql_db.unwrap_or_else(|| DEFAULT_MYSQL_DATABASE.to_owned()),
            table: mysql_table.unwrap_or_else(|| DEFAULT_MYSQL_TABLE.to_owned()),
        }),
    };
    config.validate().map_err(to_py_value_error)?;
    Ok(PyBinanceRecorder { handle: Some(RecorderHandle::start(config)) })
}

#[pymodule]
fn binance_book(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_class::<PyBinanceRecorder>()?;
    module.add_function(wrap_pyfunction!(start_recorder, module)?)?;
    Ok(())
}

fn to_py_runtime_error(err: anyhow::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
}

fn to_py_value_error(err: anyhow::Error) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimal_to_fixed_scale() {
        assert_eq!(parse_decimal_scaled("123.45", 8).expect("parse"), 12_345_000_000);
        assert_eq!(parse_decimal_scaled("0.000000019", 8).expect("parse"), 1);
        assert_eq!(parse_decimal_scaled("-1.2", 3).expect("parse"), -1_200);
    }

    #[test]
    fn depth_interval_uses_default_for_250ms() {
        assert_eq!(depth_interval_suffix(100), "@100ms");
        assert_eq!(depth_interval_suffix(250), "");
        assert_eq!(depth_interval_suffix(500), "@500ms");
    }

    #[test]
    fn bookticker_imbalance_uses_bid_minus_ask_notional() {
        let row = BookTickerRow::from_event(
            BookTickerEvent {
                transaction_time_ms: 1_777_347_200_000,
                symbol: "BTCUSDT".to_owned(),
                bid_px: "100.00".to_owned(),
                bid_qty: "2.000".to_owned(),
                ask_px: "100.00".to_owned(),
                ask_qty: "1.000".to_owned(),
            },
            8,
        )
        .expect("bookTicker row");

        assert_eq!(row.imbalance, 333);
    }

    #[test]
    fn default_symbol_threshold_uses_eth_five_million() {
        let mut config = RecorderConfig::default();
        config.validate().expect("valid config");

        assert_eq!(config.wall_notional_usd_for("BTCUSDT"), 10_000_000.0);
        assert_eq!(config.wall_notional_usd_for("ETHUSDT"), 5_000_000.0);
        assert_eq!(config.wall_notional_usd_for("SOLUSDT"), 10_000_000.0);
    }

    #[test]
    fn parses_symbol_threshold_overrides() {
        let values = vec!["ethusdt=5000000".to_owned(), "solusdt=2500000".to_owned()];
        let thresholds = parse_symbol_wall_notional_usd(&values).expect("thresholds");

        assert_eq!(thresholds.get("ETHUSDT").copied(), Some(5_000_000.0));
        assert_eq!(thresholds.get("SOLUSDT").copied(), Some(2_500_000.0));
    }

    #[test]
    fn formats_mysql_values_for_depth_minute() {
        assert_eq!(mysql_datetime_utc_ms(1_777_347_259_123), "2026-04-28 03:34:19.123");
        assert_eq!(format_scaled_decimal(123_456, 3), "123.456");
        assert_eq!(format_scaled_decimal(-123_456, 3), "-123.456");
        assert!(validate_mysql_identifier("binance_depth_minute").is_ok());
        assert!(validate_mysql_identifier("bad-name").is_err());
    }

    #[test]
    fn support_wall_searches_nearest_level_inside_window() {
        let mut bids = BTreeMap::new();
        bids.insert(99_400_00000000, 200_00000000);
        bids.insert(99_600_00000000, 20_00000000);
        bids.insert(99_800_00000000, 120_00000000);
        let wall =
            find_support_wall(&bids, 100_000_00000000, 99_500_00000000, 10_000_000_00000000, 8).expect("support wall");
        assert_eq!(wall.px, 99_800_00000000);
        assert_eq!(wall.notional, 1_197_600_000_000_000_i128);
    }
}
