use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use log::warn;
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;
use serde::Deserialize;

const ARCHIVE_BASE_DIR: &str = "/home/aimee/hl_runtime/dataset";
const DEFAULT_ARCHIVE_SYMBOLS: &[&str] = &["BTC", "ETH"];
const LITE_PRICE_SCALE: u32 = 8;
const LITE_SIZE_SCALE: u32 = 8;
const CHECKPOINT_BLOCKS: u64 = 10_000;
const ROW_GROUP_BLOCKS: u64 = 5_000;
static ROTATION_BLOCKS: AtomicU64 = AtomicU64::new(100_000);
static ARCHIVE_BASE_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static ARCHIVE_SYMBOLS_OVERRIDE: OnceLock<Mutex<Option<Vec<String>>>> = OnceLock::new();
pub(crate) const ARCHIVE_QUEUE_BLOCKS: usize = 127;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveMode {
    Lite = 1,
    Full = 2,
}

static ARCHIVE_MODE: AtomicU8 = AtomicU8::new(0);

pub fn set_rotation_blocks(n: u64) {
    if n > 0 {
        ROTATION_BLOCKS.store(n, Ordering::SeqCst);
    }
}

fn archive_base_dir_override() -> &'static Mutex<Option<PathBuf>> {
    ARCHIVE_BASE_DIR_OVERRIDE.get_or_init(|| Mutex::new(None))
}

fn archive_symbols_override() -> &'static Mutex<Option<Vec<String>>> {
    ARCHIVE_SYMBOLS_OVERRIDE.get_or_init(|| Mutex::new(None))
}

fn normalize_archive_symbols(symbols: Option<Vec<String>>) -> Vec<String> {
    let mut seen = HashSet::new();
    let normalized: Vec<String> = symbols
        .unwrap_or_else(|| DEFAULT_ARCHIVE_SYMBOLS.iter().map(|symbol| (*symbol).to_string()).collect())
        .into_iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .collect();
    if normalized.is_empty() {
        DEFAULT_ARCHIVE_SYMBOLS.iter().map(|symbol| (*symbol).to_string()).collect()
    } else {
        normalized
    }
}

pub fn current_archive_base_dir() -> PathBuf {
    archive_base_dir_override()
        .lock()
        .expect("archive base dir mutex")
        .clone()
        .unwrap_or_else(|| PathBuf::from(ARCHIVE_BASE_DIR))
}

pub fn set_archive_base_dir(path: Option<PathBuf>) -> PathBuf {
    let mut guard = archive_base_dir_override().lock().expect("archive base dir mutex");
    *guard = path;
    guard.clone().unwrap_or_else(|| PathBuf::from(ARCHIVE_BASE_DIR))
}

pub fn current_archive_symbols() -> Vec<String> {
    archive_symbols_override()
        .lock()
        .expect("archive symbols mutex")
        .clone()
        .map(|symbols| normalize_archive_symbols(Some(symbols)))
        .unwrap_or_else(|| normalize_archive_symbols(None))
}

pub fn set_archive_symbols(symbols: Option<Vec<String>>) -> Vec<String> {
    let normalized = normalize_archive_symbols(symbols);
    let mut guard = archive_symbols_override().lock().expect("archive symbols mutex");
    *guard = Some(normalized.clone());
    normalized
}

pub fn set_archive_mode(mode: Option<ArchiveMode>) {
    let val = match mode {
        None => 0,
        Some(ArchiveMode::Lite) => 1,
        Some(ArchiveMode::Full) => 2,
    };
    ARCHIVE_MODE.store(val, Ordering::SeqCst);
}

pub fn set_archive_enabled(enabled: bool) {
    if enabled {
        if ARCHIVE_MODE.load(Ordering::SeqCst) == 0 {
            ARCHIVE_MODE.store(1, Ordering::SeqCst); // Default to Lite
        }
    } else {
        ARCHIVE_MODE.store(0, Ordering::SeqCst);
    }
}

pub(crate) fn get_archive_mode() -> Option<ArchiveMode> {
    match ARCHIVE_MODE.load(Ordering::SeqCst) {
        1 => Some(ArchiveMode::Lite),
        2 => Some(ArchiveMode::Full),
        _ => None,
    }
}

pub(crate) fn is_archive_enabled() -> bool {
    ARCHIVE_MODE.load(Ordering::SeqCst) != 0
}

#[derive(Debug)]
pub(crate) struct ArchiveBlock {
    pub(crate) block_number: u64,
    pub(crate) fills_line: Vec<u8>,
    pub(crate) diffs_line: Vec<u8>,
    pub(crate) order_line: Vec<u8>,
}

impl ArchiveBlock {
    pub(crate) fn new(block_number: u64, fills_line: Vec<u8>, diffs_line: Vec<u8>, order_line: Vec<u8>) -> Self {
        Self { block_number, fills_line, diffs_line, order_line }
    }
}

#[derive(Deserialize)]
struct BatchLite<T> {
    #[serde(rename = "local_time")]
    _local_time: String,
    #[serde(rename = "block_time")]
    block_time: String,
    #[serde(rename = "block_number")]
    block_number: u64,
    events: Vec<T>,
}

#[derive(Deserialize)]
struct OrderStatusLite {
    time: String,
    user: Option<String>,
    hash: serde_json::Value,
    builder: serde_json::Value,
    status: String,
    order: OrderLite,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderLite {
    coin: String,
    side: String,
    limit_px: String,
    sz: String,
    oid: u64,
    timestamp: u64,
    trigger_condition: String,
    is_trigger: bool,
    trigger_px: String,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    orig_sz: String,
    tif: Option<String>,
    cloid: serde_json::Value,
}

#[derive(Deserialize)]
struct DiffLite {
    user: String,
    oid: u64,
    coin: String,
    side: String,
    px: String,
    #[serde(rename = "raw_book_diff", alias = "rawBookDiff")]
    raw_book_diff: RawDiffLite,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum RawDiffLite {
    #[serde(alias = "New")]
    New { sz: String },
    #[serde(alias = "Update")]
    Update {
        #[serde(rename = "origSz")]
        orig_sz: String,
        #[serde(rename = "newSz")]
        new_sz: String,
    },
    #[serde(alias = "Remove")]
    Remove,
}

#[derive(Deserialize)]
struct FillEvent(String, FillLite);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillLite {
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
    start_position: String,
    dir: String,
    closed_pnl: String,
    hash: String,
    oid: u64,
    crossed: bool,
    fee: String,
    tid: u64,
    fee_token: String,
    twap_id: Option<u64>,
}

#[derive(Debug)]
struct StatusOut {
    block_number: u64,
    block_time: String,
    coin: String,
    status: String,
    oid: u64,
    side: String,
    limit_px: i64,
    is_trigger: bool,
    tif: String,
    // Full
    user: String,
    hash: String,
    order_type: String,
    sz: i64,
    orig_sz: i64,
    time: String,
    builder: String,
    timestamp: i64,
    trigger_condition: String,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    cloid: String,
    raw_event: String,
}

#[derive(Debug)]
struct DiffOut {
    block_number: u64,
    block_time: String,
    coin: String,
    oid: u64,
    diff_type: u8,
    sz: i64,
    // Full
    user: String,
    side: String,
    px: i64,
    orig_sz: i64,
    raw_event: String,
}

#[derive(Debug)]
struct FillOut {
    block_number: u64,
    block_time: String,
    coin: String,
    side: String,
    px: i64,
    sz: i64,
    crossed: bool,
    // Full
    address: String,
    closed_pnl: i64,
    fee: i64,
    hash: String,
    oid: u64,
    tid: u64,
    time: i64,
    start_position: i64,
    dir: String,
    fee_token: String,
    twap_id: i64,
    raw_event: String,
}

#[derive(Debug)]
struct BlockIndexOut {
    block_number: u64,
    block_time: String,
    order_batch_ok: bool,
    diff_batch_ok: bool,
    fill_batch_ok: bool,
    order_n: i32,
    diff_n: i32,
    fill_n: i32,
    btc_status_n: i32,
    btc_diff_n: i32,
    btc_fill_n: i32,
    eth_status_n: i32,
    eth_diff_n: i32,
    eth_fill_n: i32,
    archive_mode: String,
    tracked_symbols: String,
}

#[derive(Clone, Copy)]
enum StreamKind {
    Blocks,
    Status,
    Diff,
    Fill,
}

impl StreamKind {
    fn name(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Status => "status",
            Self::Diff => "diff",
            Self::Fill => "fill",
        }
    }
}

struct ParquetFile {
    writer: SerializedFileWriter<std::fs::File>,
    start_block: u64,
}

struct ParquetStreamWriter<R> {
    stream: StreamKind,
    schema: std::sync::Arc<Type>,
    props: std::sync::Arc<WriterProperties>,
    file: Option<ParquetFile>,
    pending_rows: Vec<R>,
    pending_start_block: Option<u64>,
}

impl<R> ParquetStreamWriter<R> {
    fn new(stream: StreamKind, schema: std::sync::Arc<Type>, props: std::sync::Arc<WriterProperties>) -> Self {
        Self { stream, schema, props, file: None, pending_rows: Vec::new(), pending_start_block: None }
    }

    fn ensure_open(&mut self, coin: &str, mode: ArchiveMode, block: u64) -> parquet::errors::Result<()> {
        if self.file.is_none() {
            let (start, end) = rotation_bounds(block);
            self.open_file(coin, mode, start, end)?;
        }
        Ok(())
    }

    fn open_file(&mut self, coin: &str, mode: ArchiveMode, start: u64, end: u64) -> parquet::errors::Result<()> {
        let (dir, filename) = match self.stream {
            StreamKind::Blocks => {
                (current_archive_base_dir().join(self.stream.name()), format!("blocks_{start}_{end}.parquet"))
            }
            _ => {
                let coin_dir = match mode {
                    ArchiveMode::Lite => coin.to_string(),
                    ArchiveMode::Full => format!("{coin}_full"),
                };
                (
                    current_archive_base_dir().join(coin_dir).join(self.stream.name()),
                    format!("{}_{}_{start}_{end}.parquet", coin, self.stream.name()),
                )
            }
        };
        fs::create_dir_all(&dir).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        let path = dir.join(filename);
        let file = fs::File::create(&path).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        let writer = SerializedFileWriter::new(file, self.schema.clone(), self.props.clone())?;
        self.file = Some(ParquetFile { writer, start_block: start });
        Ok(())
    }

    fn close(&mut self) -> parquet::errors::Result<()> {
        if let Some(file) = self.file.take() {
            file.writer.close()?;
        }
        Ok(())
    }

    fn flush_pending<F>(&mut self, mode: ArchiveMode, write_rows: F) -> parquet::errors::Result<()>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        if self.pending_rows.is_empty() {
            self.pending_start_block = None;
            return Ok(());
        }
        if let Some(file) = self.file.as_mut() {
            write_rows(&mut file.writer, mode, &self.pending_rows)?;
        }
        self.pending_rows.clear();
        self.pending_start_block = None;
        Ok(())
    }

    fn advance_block<F>(&mut self, mode: ArchiveMode, block: u64, write_rows: F) -> parquet::errors::Result<()>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let current_start = self.file.as_ref().map(|current| current.start_block);
        let next_start = rotation_bounds(block).0;
        if current_start.is_some_and(|start| start != next_start) {
            self.flush_pending(mode, write_rows)?;
            self.close()?;
        }
        if self.pending_start_block.is_some_and(|start| block.saturating_sub(start) + 1 >= ROW_GROUP_BLOCKS) {
            self.flush_pending(mode, write_rows)?;
        }
        Ok(())
    }

    fn append_rows<F>(
        &mut self,
        coin: &str,
        mode: ArchiveMode,
        block: u64,
        rows: Vec<R>,
        write_rows: F,
    ) -> parquet::errors::Result<()>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        if rows.is_empty() {
            return Ok(());
        }
        self.ensure_open(coin, mode, block)?;
        if self.pending_start_block.is_none() {
            self.pending_start_block = Some(block);
        }
        self.pending_rows.extend(rows);
        if self.pending_start_block.is_some_and(|start| block.saturating_sub(start) + 1 >= ROW_GROUP_BLOCKS) {
            self.flush_pending(mode, write_rows)?;
        }
        Ok(())
    }

    fn close_with_flush<F>(&mut self, mode: ArchiveMode, write_rows: F) -> parquet::errors::Result<()>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        self.flush_pending(mode, write_rows)?;
        self.close()
    }
}

struct CoinWriters {
    status: ParquetStreamWriter<StatusOut>,
    diff: ParquetStreamWriter<DiffOut>,
    fill: ParquetStreamWriter<FillOut>,
}

struct ArchiveWriters {
    blocks: ParquetStreamWriter<BlockIndexOut>,
    coins: HashMap<String, CoinWriters>,
    symbols: Vec<String>,
    mode: ArchiveMode,
}

impl ArchiveWriters {
    fn new(mode: ArchiveMode, symbols: Vec<String>) -> Self {
        let zstd_level = ZstdLevel::try_new(3).unwrap_or_default();
        let props = std::sync::Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(zstd_level))
                .set_dictionary_enabled(true)
                .build(),
        );

        let blocks_schema = std::sync::Arc::new(
            parse_message_type(
                "message blocks_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BOOLEAN order_batch_ok;\n\
                    REQUIRED BOOLEAN diff_batch_ok;\n\
                    REQUIRED BOOLEAN fill_batch_ok;\n\
                    REQUIRED INT32 order_n;\n\
                    REQUIRED INT32 diff_n;\n\
                    REQUIRED INT32 fill_n;\n\
                    REQUIRED INT32 btc_status_n;\n\
                    REQUIRED INT32 btc_diff_n;\n\
                    REQUIRED INT32 btc_fill_n;\n\
                    REQUIRED INT32 eth_status_n;\n\
                    REQUIRED INT32 eth_diff_n;\n\
                    REQUIRED INT32 eth_fill_n;\n\
                    REQUIRED BINARY archive_mode (UTF8);\n\
                    REQUIRED BINARY tracked_symbols (UTF8);\n\
                }",
            )
            .expect("invalid blocks schema"),
        );

        let (status_schema_str, diff_schema_str, fill_schema_str) = match mode {
            ArchiveMode::Lite => (
                "message status_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY coin (UTF8);\n\
                    REQUIRED BINARY time (UTF8);\n\
                    REQUIRED BINARY user (UTF8);\n\
                    REQUIRED BINARY status (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 limit_px (DECIMAL(18, 8));\n\
                    REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                    REQUIRED INT64 orig_sz (DECIMAL(18, 8));\n\
                    REQUIRED INT64 timestamp;\n\
                    REQUIRED BOOLEAN is_trigger;\n\
                    REQUIRED BINARY tif (UTF8);\n\
                    REQUIRED BINARY trigger_condition (UTF8);\n\
                    REQUIRED INT64 trigger_px (DECIMAL(18, 8));\n\
                    REQUIRED BOOLEAN is_position_tpsl;\n\
                    REQUIRED BOOLEAN reduce_only;\n\
                    REQUIRED BINARY order_type (UTF8);\n\
                    REQUIRED BINARY cloid (UTF8);\n\
                }",
                "message diff_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 px (DECIMAL(18, 8));\n\
                    REQUIRED INT32 diff_type;\n\
                    REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                    REQUIRED INT64 orig_sz (DECIMAL(18, 8));\n\
                }",
                "message fill_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY coin (UTF8);\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 px (DECIMAL(18, 8));\n\
                    REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                    REQUIRED BOOLEAN crossed;\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED INT64 time;\n\
                }",
            ),
            ArchiveMode::Full => (
                "message status_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY coin (UTF8);\n\
                    REQUIRED BINARY status (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 limit_px (DECIMAL(18, 8));\n\
                    REQUIRED BOOLEAN is_trigger;\n\
                    REQUIRED BINARY tif (UTF8);\n\
                    REQUIRED BINARY user (UTF8);\n\
                    REQUIRED BINARY hash (UTF8);\n\
                    REQUIRED BINARY order_type (UTF8);\n\
                    REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                    REQUIRED INT64 orig_sz (DECIMAL(18, 8));\n\
                    REQUIRED BINARY time (UTF8);\n\
                    REQUIRED BINARY builder (UTF8);\n\
                    REQUIRED INT64 timestamp;\n\
                    REQUIRED BINARY trigger_condition (UTF8);\n\
                    REQUIRED INT64 trigger_px (DECIMAL(18, 8));\n\
                    REQUIRED BOOLEAN is_position_tpsl;\n\
                    REQUIRED BOOLEAN reduce_only;\n\
                    REQUIRED BINARY cloid (UTF8);\n\
                    REQUIRED BINARY raw_event (UTF8);\n\
                }",
                "message diff_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY coin (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED INT32 diff_type;\n\
                    REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                    REQUIRED BINARY user (UTF8);\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 px (DECIMAL(18, 8));\n\
                    REQUIRED INT64 orig_sz (DECIMAL(18, 8));\n\
                    REQUIRED BINARY raw_event (UTF8);\n\
                }",
                "message fill_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY coin (UTF8);\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 px (DECIMAL(18, 8));\n\
                    REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                    REQUIRED BOOLEAN crossed;\n\
                    REQUIRED BINARY address (UTF8);\n\
                    REQUIRED INT64 closed_pnl (DECIMAL(18, 8));\n\
                    REQUIRED INT64 fee (DECIMAL(18, 8));\n\
                    REQUIRED BINARY hash (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED INT64 tid;\n\
                    REQUIRED INT64 time;\n\
                    REQUIRED INT64 start_position (DECIMAL(18, 8));\n\
                    REQUIRED BINARY dir (UTF8);\n\
                    REQUIRED BINARY fee_token (UTF8);\n\
                    REQUIRED INT64 twap_id;\n\
                    REQUIRED BINARY raw_event (UTF8);\n\
                }",
            ),
        };

        let status_schema = std::sync::Arc::new(parse_message_type(status_schema_str).expect("invalid status schema"));
        let diff_schema = std::sync::Arc::new(parse_message_type(diff_schema_str).expect("invalid diff schema"));
        let fill_schema = std::sync::Arc::new(parse_message_type(fill_schema_str).expect("invalid fill schema"));

        let mut coins = HashMap::new();
        for coin in &symbols {
            coins.insert(
                coin.clone(),
                CoinWriters {
                    status: ParquetStreamWriter::new(StreamKind::Status, status_schema.clone(), props.clone()),
                    diff: ParquetStreamWriter::new(StreamKind::Diff, diff_schema.clone(), props.clone()),
                    fill: ParquetStreamWriter::new(StreamKind::Fill, fill_schema.clone(), props.clone()),
                },
            );
        }

        Self {
            blocks: ParquetStreamWriter::new(StreamKind::Blocks, blocks_schema, props.clone()),
            coins,
            symbols,
            mode,
        }
    }

    fn coin_mut(&mut self, coin: &str) -> Option<&mut CoinWriters> {
        self.coins.get_mut(coin)
    }

    fn has_coin(&self, coin: &str) -> bool {
        self.coins.contains_key(coin)
    }

    fn close_all(&mut self) -> parquet::errors::Result<()> {
        self.blocks.close_with_flush(self.mode, write_block_rows)?;
        for writers in self.coins.values_mut() {
            writers.status.close_with_flush(self.mode, write_status_rows)?;
            writers.diff.close_with_flush(self.mode, write_diff_rows)?;
            writers.fill.close_with_flush(self.mode, write_fill_rows)?;
        }
        Ok(())
    }
}

fn rotation_bounds(block: u64) -> (u64, u64) {
    let rotation = ROTATION_BLOCKS.load(Ordering::SeqCst);
    let start = ((block.saturating_sub(1)) / rotation) * rotation + 1;
    let end = start + rotation - 1;
    (start, end)
}

fn checkpoint_start(block: u64) -> u64 {
    ((block.saturating_sub(1)) / CHECKPOINT_BLOCKS) * CHECKPOINT_BLOCKS + 1
}

fn is_checkpoint_start(block: u64) -> bool {
    checkpoint_start(block) == block
}

fn parse_scaled(value: &str, scale: u32) -> Option<i64> {
    let s = value.trim();
    if s.is_empty() {
        return None;
    }
    let mut idx = 0;
    let mut neg = false;
    let bytes = s.as_bytes();
    if bytes[idx] == b'-' {
        neg = true;
        idx += 1;
    } else if bytes[idx] == b'+' {
        idx += 1;
    }
    let mut int_part: i64 = 0;
    let mut frac_part: i64 = 0;
    let mut frac_digits: u32 = 0;
    let mut seen_dot = false;
    let mut round_up = false;
    let mut precision_lost = false;

    while idx < bytes.len() {
        let b = bytes[idx];
        if b.is_ascii_digit() {
            let digit = i64::from(b - b'0');
            if !seen_dot {
                int_part = int_part.saturating_mul(10).saturating_add(digit);
            } else if frac_digits < scale {
                frac_part = frac_part.saturating_mul(10).saturating_add(digit);
                frac_digits += 1;
            } else {
                if digit > 0 {
                    precision_lost = true;
                }
                if !round_up && digit >= 5 {
                    round_up = true;
                }
            }
            idx += 1;
            continue;
        }
        if b == b'.' && !seen_dot {
            seen_dot = true;
            idx += 1;
            continue;
        }
        return None;
    }

    if precision_lost {
        warn!("Precision loss: truncated '{}' to {} decimal places", s, scale);
    }

    while frac_digits < scale {
        frac_part = frac_part.saturating_mul(10);
        frac_digits += 1;
    }
    if round_up {
        frac_part = frac_part.saturating_add(1);
        let scale_factor = 10i64.saturating_pow(scale);
        if frac_part >= scale_factor {
            frac_part -= scale_factor;
            int_part = int_part.saturating_add(1);
        }
    }
    let scale_factor = 10i64.saturating_pow(scale);
    let mut value = int_part.saturating_mul(scale_factor).saturating_add(frac_part);
    if neg {
        value = -value;
    }
    Some(value)
}

fn flatten_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        _ => v.to_string(),
    }
}

fn write_status_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    mode: ArchiveMode,
    rows: &[StatusOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers: Vec<i64> = rows.iter().map(|r| r.block_number as i64).collect();
    let block_times: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.block_time.as_bytes())).collect();
    let coins: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.coin.as_bytes())).collect();
    let times: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.time.as_bytes())).collect();
    let users: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.user.as_bytes())).collect();
    let statuses: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.status.as_bytes())).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let limit_px: Vec<i64> = rows.iter().map(|r| r.limit_px).collect();
    let sizes: Vec<i64> = rows.iter().map(|r| r.sz).collect();
    let orig_sizes: Vec<i64> = rows.iter().map(|r| r.orig_sz).collect();
    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp).collect();
    let is_trigger: Vec<bool> = rows.iter().map(|r| r.is_trigger).collect();
    let tifs: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.tif.as_bytes())).collect();
    let trigger_conditions: Vec<ByteArray> =
        rows.iter().map(|r| ByteArray::from(r.trigger_condition.as_bytes())).collect();
    let trigger_pxs: Vec<i64> = rows.iter().map(|r| r.trigger_px).collect();
    let is_position_tpsls: Vec<bool> = rows.iter().map(|r| r.is_position_tpsl).collect();
    let reduce_onlys: Vec<bool> = rows.iter().map(|r| r.reduce_only).collect();
    let order_types: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.order_type.as_bytes())).collect();
    let cloids: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.cloid.as_bytes())).collect();

    if mode == ArchiveMode::Lite {
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&coins, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&users, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&statuses, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sides, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&limit_px, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sizes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&orig_sizes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&timestamps, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&is_trigger, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&tifs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&trigger_conditions, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&trigger_pxs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&is_position_tpsls, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&reduce_onlys, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&order_types, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&cloids, None, None)?;
            }
            col.close()?;
        }
    } else {
        let hashes: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.hash.as_bytes())).collect();
        let builders: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.builder.as_bytes())).collect();
        let raw_events: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.raw_event.as_bytes())).collect();

        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&coins, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&statuses, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sides, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&limit_px, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&is_trigger, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&tifs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&users, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&hashes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&order_types, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sizes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&orig_sizes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&builders, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&timestamps, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&trigger_conditions, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&trigger_pxs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&is_position_tpsls, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&reduce_onlys, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&cloids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&raw_events, None, None)?;
            }
            col.close()?;
        }
    }

    row_group.close()?;
    Ok(())
}

fn write_diff_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    mode: ArchiveMode,
    rows: &[DiffOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers: Vec<i64> = rows.iter().map(|r| r.block_number as i64).collect();
    let block_times: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.block_time.as_bytes())).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let pxs: Vec<i64> = rows.iter().map(|r| r.px).collect();
    let diff_types: Vec<i32> = rows.iter().map(|r| i32::from(r.diff_type)).collect();
    let sizes: Vec<i64> = rows.iter().map(|r| r.sz).collect();
    let orig_szs: Vec<i64> = rows.iter().map(|r| r.orig_sz).collect();

    if mode == ArchiveMode::Lite {
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sides, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&pxs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&diff_types, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sizes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&orig_szs, None, None)?;
            }
            col.close()?;
        }
    } else {
        let coins: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.coin.as_bytes())).collect();
        let users: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.user.as_bytes())).collect();
        let raw_events: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.raw_event.as_bytes())).collect();

        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&coins, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&diff_types, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sizes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&users, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sides, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&pxs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&orig_szs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&raw_events, None, None)?;
            }
            col.close()?;
        }
    }

    row_group.close()?;
    Ok(())
}

fn write_fill_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    mode: ArchiveMode,
    rows: &[FillOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers: Vec<i64> = rows.iter().map(|r| r.block_number as i64).collect();
    let block_times: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.block_time.as_bytes())).collect();
    let coins: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.coin.as_bytes())).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let px: Vec<i64> = rows.iter().map(|r| r.px).collect();
    let sz: Vec<i64> = rows.iter().map(|r| r.sz).collect();
    let crossed: Vec<bool> = rows.iter().map(|r| r.crossed).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    let times: Vec<i64> = rows.iter().map(|r| r.time).collect();

    if mode == ArchiveMode::Lite {
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&coins, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sides, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&px, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sz, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&crossed, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&times, None, None)?;
            }
            col.close()?;
        }
    } else {
        let addresses: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.address.as_bytes())).collect();
        let pnls: Vec<i64> = rows.iter().map(|r| r.closed_pnl).collect();
        let fees: Vec<i64> = rows.iter().map(|r| r.fee).collect();
        let hashes: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.hash.as_bytes())).collect();
        let tids: Vec<i64> = rows.iter().map(|r| r.tid as i64).collect();
        let start_positions: Vec<i64> = rows.iter().map(|r| r.start_position).collect();
        let dirs: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.dir.as_bytes())).collect();
        let fee_tokens: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.fee_token.as_bytes())).collect();
        let twap_ids: Vec<i64> = rows.iter().map(|r| r.twap_id).collect();
        let raw_events: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.raw_event.as_bytes())).collect();

        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&coins, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sides, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&px, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sz, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&crossed, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&addresses, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&pnls, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&fees, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&hashes, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&tids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&times, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&start_positions, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&dirs, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&fee_tokens, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&twap_ids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&raw_events, None, None)?;
            }
            col.close()?;
        }
    }

    row_group.close()?;
    Ok(())
}

fn write_block_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    _mode: ArchiveMode,
    rows: &[BlockIndexOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
    let block_times: Vec<ByteArray> = rows.iter().map(|row| ByteArray::from(row.block_time.as_bytes())).collect();
    let order_batch_ok: Vec<bool> = rows.iter().map(|row| row.order_batch_ok).collect();
    let diff_batch_ok: Vec<bool> = rows.iter().map(|row| row.diff_batch_ok).collect();
    let fill_batch_ok: Vec<bool> = rows.iter().map(|row| row.fill_batch_ok).collect();
    let order_n: Vec<i32> = rows.iter().map(|row| row.order_n).collect();
    let diff_n: Vec<i32> = rows.iter().map(|row| row.diff_n).collect();
    let fill_n: Vec<i32> = rows.iter().map(|row| row.fill_n).collect();
    let btc_status_n: Vec<i32> = rows.iter().map(|row| row.btc_status_n).collect();
    let btc_diff_n: Vec<i32> = rows.iter().map(|row| row.btc_diff_n).collect();
    let btc_fill_n: Vec<i32> = rows.iter().map(|row| row.btc_fill_n).collect();
    let eth_status_n: Vec<i32> = rows.iter().map(|row| row.eth_status_n).collect();
    let eth_diff_n: Vec<i32> = rows.iter().map(|row| row.eth_diff_n).collect();
    let eth_fill_n: Vec<i32> = rows.iter().map(|row| row.eth_fill_n).collect();
    let archive_modes: Vec<ByteArray> = rows.iter().map(|row| ByteArray::from(row.archive_mode.as_bytes())).collect();
    let tracked_symbols: Vec<ByteArray> =
        rows.iter().map(|row| ByteArray::from(row.tracked_symbols.as_bytes())).collect();

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_numbers, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_times, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&order_batch_ok, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&diff_batch_ok, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&fill_batch_ok, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&order_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&diff_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&fill_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&btc_status_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&btc_diff_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&btc_fill_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&eth_status_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&eth_diff_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&eth_fill_n, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&archive_modes, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&tracked_symbols, None, None)?;
        }
        col.close()?;
    }

    row_group.close()?;
    Ok(())
}

pub(crate) fn run_archive_writer(rx: Receiver<ArchiveBlock>, stop: Arc<AtomicBool>) {
    let mut writers: Option<ArchiveWriters> = None;
    let mut current_mode: Option<ArchiveMode> = None;
    let mut current_symbols = current_archive_symbols();
    let mut waiting_for_rotation_start = false;
    let mut pending_rotation_start: Option<u64> = None;

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        let mode = get_archive_mode();
        let symbols = current_archive_symbols();
        if mode != current_mode || symbols != current_symbols {
            if let Some(mut w) = writers.take() {
                w.close_all().ok();
            }
            writers = mode.map(|mode| ArchiveWriters::new(mode, symbols.clone()));
            current_mode = mode;
            current_symbols = symbols;
            waiting_for_rotation_start = mode.is_some();
            pending_rotation_start = None;
        }

        let msg = match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(msg) => msg,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        let Some(writers) = writers.as_mut() else {
            continue;
        };
        let mode = writers.mode;

        let order_batch: BatchLite<OrderStatusLite> = match serde_json::from_slice(&msg.order_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!("Archive parse error for order batch at {}: {err}", msg.block_number);
                continue;
            }
        };
        let order_batch_raw: Option<BatchLite<serde_json::Value>> = if mode == ArchiveMode::Full {
            match serde_json::from_slice(&msg.order_line) {
                Ok(batch) => Some(batch),
                Err(err) => {
                    warn!("Archive parse error for raw order batch at {}: {err}", msg.block_number);
                    continue;
                }
            }
        } else {
            None
        };
        let diff_batch: BatchLite<DiffLite> = match serde_json::from_slice(&msg.diffs_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!("Archive parse error for diff batch at {}: {err}", msg.block_number);
                continue;
            }
        };
        let diff_batch_raw: Option<BatchLite<serde_json::Value>> = if mode == ArchiveMode::Full {
            match serde_json::from_slice(&msg.diffs_line) {
                Ok(batch) => Some(batch),
                Err(err) => {
                    warn!("Archive parse error for raw diff batch at {}: {err}", msg.block_number);
                    continue;
                }
            }
        } else {
            None
        };
        let fill_batch: BatchLite<FillEvent> = match serde_json::from_slice(&msg.fills_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!("Archive parse error for fill batch at {}: {err}", msg.block_number);
                continue;
            }
        };
        let fill_batch_raw: Option<BatchLite<serde_json::Value>> = if mode == ArchiveMode::Full {
            match serde_json::from_slice(&msg.fills_line) {
                Ok(batch) => Some(batch),
                Err(err) => {
                    warn!("Archive parse error for raw fill batch at {}: {err}", msg.block_number);
                    continue;
                }
            }
        } else {
            None
        };

        let height = order_batch.block_number;
        if diff_batch.block_number != height || fill_batch.block_number != height {
            warn!(
                "Archive height mismatch: order {} diff {} fill {}",
                height, diff_batch.block_number, fill_batch.block_number
            );
            continue;
        }

        if waiting_for_rotation_start {
            if !is_checkpoint_start(height) {
                let next_checkpoint_start = checkpoint_start(height).saturating_add(CHECKPOINT_BLOCKS);
                if pending_rotation_start != Some(next_checkpoint_start) {
                    warn!(
                        "Archive enabled mid-window at block {}; waiting for next checkpoint start {} before creating parquet files",
                        height, next_checkpoint_start
                    );
                    pending_rotation_start = Some(next_checkpoint_start);
                }
                continue;
            }
            waiting_for_rotation_start = false;
            pending_rotation_start = None;
        }

        let block_time = order_batch.block_time.clone();
        let order_n = order_batch.events.len() as i32;
        let diff_n = diff_batch.events.len() as i32;
        let fill_n = fill_batch.events.len() as i32;
        let tracked_symbols = writers.symbols.join(",");
        let archive_mode = match mode {
            ArchiveMode::Lite => "lite".to_string(),
            ArchiveMode::Full => "full".to_string(),
        };

        let mut status_rows: HashMap<String, Vec<StatusOut>> = HashMap::new();
        let mut btc_status_n = 0;
        let mut eth_status_n = 0;
        for (idx, status) in order_batch.events.into_iter().enumerate() {
            let OrderLite {
                coin,
                side,
                limit_px,
                sz,
                oid,
                timestamp,
                trigger_condition,
                is_trigger,
                trigger_px,
                is_position_tpsl,
                reduce_only,
                order_type,
                orig_sz,
                tif,
                cloid,
            } = status.order;
            if coin == "BTC" {
                btc_status_n += 1;
            } else if coin == "ETH" {
                eth_status_n += 1;
            }
            if !writers.has_coin(&coin) {
                continue;
            }
            let (s_px, s_sz, s_orig, s_trig) = if mode == ArchiveMode::Full {
                (
                    parse_scaled(&limit_px, 8).unwrap_or(0),
                    parse_scaled(&sz, 8).unwrap_or(0),
                    parse_scaled(&orig_sz, 8).unwrap_or(0),
                    parse_scaled(&trigger_px, 8).unwrap_or(0),
                )
            } else {
                (
                    parse_scaled(&limit_px, LITE_PRICE_SCALE).unwrap_or(0),
                    parse_scaled(&sz, LITE_SIZE_SCALE).unwrap_or(0),
                    parse_scaled(&orig_sz, LITE_SIZE_SCALE).unwrap_or(0),
                    parse_scaled(&trigger_px, LITE_PRICE_SCALE).unwrap_or(0),
                )
            };
            let out = StatusOut {
                block_number: height,
                block_time: block_time.clone(),
                coin: coin.clone(),
                status: status.status,
                oid,
                side,
                limit_px: s_px,
                is_trigger,
                tif: tif.unwrap_or_default(),
                user: status.user.unwrap_or_default(),
                hash: flatten_to_string(&status.hash),
                order_type,
                sz: s_sz,
                orig_sz: s_orig,
                time: status.time,
                builder: flatten_to_string(&status.builder),
                timestamp: timestamp as i64,
                trigger_condition,
                trigger_px: s_trig,
                is_position_tpsl,
                reduce_only,
                cloid: flatten_to_string(&cloid),
                raw_event: order_batch_raw
                    .as_ref()
                    .and_then(|batch| batch.events.get(idx))
                    .map_or_else(String::new, serde_json::Value::to_string),
            };
            status_rows.entry(coin).or_default().push(out);
        }

        let mut diff_rows: HashMap<String, Vec<DiffOut>> = HashMap::new();
        let mut btc_diff_n = 0;
        let mut eth_diff_n = 0;
        for (idx, diff) in diff_batch.events.into_iter().enumerate() {
            let DiffLite { user, oid, coin, side, px, raw_book_diff } = diff;
            if coin == "BTC" {
                btc_diff_n += 1;
            } else if coin == "ETH" {
                eth_diff_n += 1;
            }
            if !writers.has_coin(&coin) {
                continue;
            }
            let (d_px, d_sz, d_orig, diff_type) = if mode == ArchiveMode::Full {
                let (dt, sz, osz) = match raw_book_diff {
                    RawDiffLite::New { sz } => (0u8, parse_scaled(&sz, 8).unwrap_or(0), 0i64),
                    RawDiffLite::Update { orig_sz, new_sz } => {
                        (1u8, parse_scaled(&new_sz, 8).unwrap_or(0), parse_scaled(&orig_sz, 8).unwrap_or(0))
                    }
                    RawDiffLite::Remove => (2u8, 0, 0),
                };
                (parse_scaled(&px, 8).unwrap_or(0), sz, osz, dt)
            } else {
                let (dt, sz, osz) = match raw_book_diff {
                    RawDiffLite::New { sz } => (0u8, parse_scaled(&sz, LITE_SIZE_SCALE).unwrap_or(0), 0i64),
                    RawDiffLite::Update { orig_sz, new_sz } => (
                        1u8,
                        parse_scaled(&new_sz, LITE_SIZE_SCALE).unwrap_or(0),
                        parse_scaled(&orig_sz, LITE_SIZE_SCALE).unwrap_or(0),
                    ),
                    RawDiffLite::Remove => (2u8, 0, 0),
                };
                (parse_scaled(&px, LITE_PRICE_SCALE).unwrap_or(0), sz, osz, dt)
            };
            let out = DiffOut {
                block_number: height,
                block_time: block_time.clone(),
                coin: coin.clone(),
                oid,
                diff_type,
                sz: d_sz,
                user,
                side,
                px: d_px,
                orig_sz: d_orig,
                raw_event: diff_batch_raw
                    .as_ref()
                    .and_then(|batch| batch.events.get(idx))
                    .map_or_else(String::new, serde_json::Value::to_string),
            };
            diff_rows.entry(coin).or_default().push(out);
        }

        let mut fill_rows: HashMap<String, Vec<FillOut>> = HashMap::new();
        let mut btc_fill_n = 0;
        let mut eth_fill_n = 0;
        for (idx, fill_event) in fill_batch.events.into_iter().enumerate() {
            let FillEvent(address, fill_data) = fill_event;
            let FillLite {
                coin,
                px,
                sz,
                side,
                time,
                start_position,
                dir,
                closed_pnl,
                hash,
                oid,
                crossed,
                fee,
                tid,
                fee_token,
                twap_id,
                ..
            } = fill_data;
            if coin == "BTC" {
                btc_fill_n += 1;
            } else if coin == "ETH" {
                eth_fill_n += 1;
            }
            if !writers.has_coin(&coin) {
                continue;
            }
            let (f_px, f_sz, f_pnl, f_fee, f_start) = if mode == ArchiveMode::Full {
                (
                    parse_scaled(&px, 8).unwrap_or(0),
                    parse_scaled(&sz, 8).unwrap_or(0),
                    parse_scaled(&closed_pnl, 8).unwrap_or(0),
                    parse_scaled(&fee, 8).unwrap_or(0),
                    parse_scaled(&start_position, 8).unwrap_or(0),
                )
            } else {
                (
                    parse_scaled(&px, LITE_PRICE_SCALE).unwrap_or(0),
                    parse_scaled(&sz, LITE_SIZE_SCALE).unwrap_or(0),
                    0,
                    0,
                    0,
                )
            };
            let out = FillOut {
                block_number: height,
                block_time: block_time.clone(),
                coin: coin.clone(),
                side,
                px: f_px,
                sz: f_sz,
                crossed,
                address,
                closed_pnl: f_pnl,
                fee: f_fee,
                hash,
                oid,
                tid,
                time: time as i64,
                start_position: f_start,
                dir,
                fee_token,
                twap_id: twap_id.unwrap_or(0) as i64,
                raw_event: fill_batch_raw
                    .as_ref()
                    .and_then(|batch| batch.events.get(idx))
                    .map_or_else(String::new, serde_json::Value::to_string),
            };
            fill_rows.entry(coin).or_default().push(out);
        }

        if let Err(err) = writers.blocks.advance_block(mode, height, write_block_rows) {
            warn!("Archive advance blocks failed at {}: {err}", height);
        }
        if let Err(err) = writers.blocks.append_rows(
            "blocks",
            mode,
            height,
            vec![BlockIndexOut {
                block_number: height,
                block_time: block_time.clone(),
                order_batch_ok: true,
                diff_batch_ok: true,
                fill_batch_ok: true,
                order_n,
                diff_n,
                fill_n,
                btc_status_n,
                btc_diff_n,
                btc_fill_n,
                eth_status_n,
                eth_diff_n,
                eth_fill_n,
                archive_mode,
                tracked_symbols,
            }],
            write_block_rows,
        ) {
            warn!("Archive write blocks failed at {}: {err}", height);
        }

        for coin in writers.symbols.clone() {
            if let Some(coin_writers) = writers.coin_mut(&coin) {
                if let Err(err) = coin_writers.status.advance_block(mode, height, write_status_rows) {
                    warn!("Archive advance status {} failed at {}: {err}", coin, height);
                }
                if let Err(err) = coin_writers.diff.advance_block(mode, height, write_diff_rows) {
                    warn!("Archive advance diff {} failed at {}: {err}", coin, height);
                }
                if let Err(err) = coin_writers.fill.advance_block(mode, height, write_fill_rows) {
                    warn!("Archive advance fill {} failed at {}: {err}", coin, height);
                }
                if let Some(rows) = status_rows.remove(&coin).filter(|rows| !rows.is_empty()) {
                    if let Err(err) = coin_writers.status.append_rows(&coin, mode, height, rows, write_status_rows) {
                        warn!("Archive write status {} failed at {}: {err}", coin, height);
                    }
                }
                if let Some(rows) = diff_rows.remove(&coin).filter(|rows| !rows.is_empty()) {
                    if let Err(err) = coin_writers.diff.append_rows(&coin, mode, height, rows, write_diff_rows) {
                        warn!("Archive write diff {} failed at {}: {err}", coin, height);
                    }
                }
                if let Some(rows) = fill_rows.remove(&coin).filter(|rows| !rows.is_empty()) {
                    if let Err(err) = coin_writers.fill.append_rows(&coin, mode, height, rows, write_fill_rows) {
                        warn!("Archive write fill {} failed at {}: {err}", coin, height);
                    }
                }
            }
        }
    }

    if let Some(mut w) = writers {
        if let Err(err) = w.close_all() {
            warn!("Archive writer close failed: {err}");
        }
    }
}
