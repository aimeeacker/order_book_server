use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
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
const ROTATION_BLOCKS: u64 = 100_000;
pub(crate) const ARCHIVE_QUEUE_BLOCKS: usize = 127;

static ARCHIVE_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn set_archive_enabled(enabled: bool) {
    ARCHIVE_ENABLED.store(enabled, Ordering::SeqCst);
}

pub(crate) fn is_archive_enabled() -> bool {
    ARCHIVE_ENABLED.load(Ordering::SeqCst)
}

#[derive(Debug)]
pub(crate) struct ArchiveBlock {
    block_number: u64,
    fills_line: Vec<u8>,
    diffs_line: Vec<u8>,
    order_line: Vec<u8>,
}

impl ArchiveBlock {
    pub(crate) fn new(block_number: u64, fills_line: Vec<u8>, diffs_line: Vec<u8>, order_line: Vec<u8>) -> Self {
        Self { block_number, fills_line, diffs_line, order_line }
    }
}

#[derive(Deserialize)]
struct BatchLite<T> {
    #[serde(rename = "block_time")]
    block_time: String,
    #[serde(rename = "block_number")]
    block_number: u64,
    events: Vec<T>,
}

#[derive(Deserialize)]
struct OrderStatusLite {
    status: String,
    order: OrderLite,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderLite {
    coin: String,
    side: String,
    oid: u64,
    #[serde(deserialize_with = "deserialize_px_dec")]
    limit_px: i64,
    is_trigger: bool,
    tif: Option<String>,
}

#[derive(Deserialize)]
struct DiffLite {
    oid: u64,
    coin: String,
    #[serde(rename = "raw_book_diff", alias = "rawBookDiff")]
    raw_book_diff: RawDiffLite,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum RawDiffLite {
    #[serde(alias = "New")]
    New {
        #[serde(deserialize_with = "deserialize_sz_dec")]
        sz: i64,
    },
    #[serde(alias = "Update")]
    Update {
        #[serde(deserialize_with = "deserialize_sz_dec")]
        _orig_sz: i64,
        #[serde(deserialize_with = "deserialize_sz_dec")]
        new_sz: i64,
    },
    #[serde(alias = "Remove")]
    Remove,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillLite {
    coin: String,
    side: String,
    #[serde(deserialize_with = "deserialize_px_dec")]
    px: i64,
    #[serde(deserialize_with = "deserialize_sz_dec")]
    sz: i64,
    crossed: bool,
}

#[derive(Debug)]
struct StatusOut {
    status: String,
    oid: u64,
    side: String,
    limit_px: i64,
    is_trigger: bool,
    tif: String,
}

#[derive(Debug)]
struct DiffOut {
    oid: u64,
    diff_type: u8,
    sz: i64,
}

#[derive(Debug)]
struct FillOut {
    side: String,
    px: i64,
    sz: i64,
    crossed: bool,
}

#[derive(Clone, Copy)]
enum StreamKind {
    Status,
    Diff,
    Fill,
}

impl StreamKind {
    fn name(self) -> &'static str {
        match self {
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

struct ParquetStreamWriter {
    stream: StreamKind,
    schema: std::sync::Arc<Type>,
    props: std::sync::Arc<WriterProperties>,
    file: Option<ParquetFile>,
}

impl ParquetStreamWriter {
    fn new(stream: StreamKind, schema: std::sync::Arc<Type>, props: std::sync::Arc<WriterProperties>) -> Self {
        Self { stream, schema, props, file: None }
    }

    fn ensure_open(&mut self, coin: &str, block: u64) -> parquet::errors::Result<()> {
        let (start, end) = rotation_bounds(block);
        let needs_open = match self.file.as_ref() {
            Some(current) => current.start_block != start,
            None => true,
        };
        if needs_open {
            self.close()?;
            self.open_file(coin, start, end)?;
        }
        Ok(())
    }

    fn open_file(&mut self, coin: &str, start: u64, end: u64) -> parquet::errors::Result<()> {
        let dir = PathBuf::from(ARCHIVE_BASE_DIR).join(coin).join(self.stream.name());
        fs::create_dir_all(&dir).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        let filename = format!("{start}_{end}.parquet");
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
}

struct CoinWriters {
    status: ParquetStreamWriter,
    diff: ParquetStreamWriter,
    fill: ParquetStreamWriter,
}

struct ArchiveWriters {
    btc: CoinWriters,
    eth: CoinWriters,
}

impl ArchiveWriters {
    fn new() -> Self {
        let zstd_level = ZstdLevel::try_new(3).unwrap_or_default();
        let props = std::sync::Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(zstd_level))
                .set_dictionary_enabled(true)
                .build(),
        );

        let status_schema = std::sync::Arc::new(
            parse_message_type(
                "message status_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY status (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 limit_px (DECIMAL(7, 1));\n\
                    REQUIRED BOOLEAN is_trigger;\n\
                    REQUIRED BINARY tif (UTF8);\n\
                }",
            )
            .expect("invalid status schema"),
        );
        let diff_schema = std::sync::Arc::new(
            parse_message_type(
                "message diff_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED INT64 oid;\n\
                    REQUIRED INT32 diff_type;\n\
                    REQUIRED INT64 sz (DECIMAL(8, 2));\n\
                }",
            )
            .expect("invalid diff schema"),
        );
        let fill_schema = std::sync::Arc::new(
            parse_message_type(
                "message fill_schema {\n\
                    REQUIRED INT64 block_number;\n\
                    REQUIRED BINARY block_time (UTF8);\n\
                    REQUIRED BINARY side (UTF8);\n\
                    REQUIRED INT64 px (DECIMAL(7, 1));\n\
                    REQUIRED INT64 sz (DECIMAL(8, 2));\n\
                    REQUIRED BOOLEAN crossed;\n\
                }",
            )
            .expect("invalid fill schema"),
        );

        Self {
            btc: CoinWriters {
                status: ParquetStreamWriter::new(StreamKind::Status, status_schema.clone(), props.clone()),
                diff: ParquetStreamWriter::new(StreamKind::Diff, diff_schema.clone(), props.clone()),
                fill: ParquetStreamWriter::new(StreamKind::Fill, fill_schema.clone(), props.clone()),
            },
            eth: CoinWriters {
                status: ParquetStreamWriter::new(StreamKind::Status, status_schema, props.clone()),
                diff: ParquetStreamWriter::new(StreamKind::Diff, diff_schema, props.clone()),
                fill: ParquetStreamWriter::new(StreamKind::Fill, fill_schema, props),
            },
        }
    }

    fn coin_mut(&mut self, coin: &str) -> Option<&mut CoinWriters> {
        match coin {
            "BTC" => Some(&mut self.btc),
            "ETH" => Some(&mut self.eth),
            _ => None,
        }
    }

    fn close_all(&mut self) -> parquet::errors::Result<()> {
        self.btc.status.close()?;
        self.btc.diff.close()?;
        self.btc.fill.close()?;
        self.eth.status.close()?;
        self.eth.diff.close()?;
        self.eth.fill.close()?;
        Ok(())
    }
}

fn rotation_bounds(block: u64) -> (u64, u64) {
    let start = ((block.saturating_sub(1)) / ROTATION_BLOCKS) * ROTATION_BLOCKS + 1;
    let end = start + ROTATION_BLOCKS - 1;
    (start, end)
}

fn is_target_coin(coin: &str) -> bool {
    coin == "BTC" || coin == "ETH"
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
    while idx < bytes.len() {
        let b = bytes[idx];
        if b.is_ascii_digit() {
            let digit = i64::from(b - b'0');
            if !seen_dot {
                int_part = int_part.saturating_mul(10).saturating_add(digit);
            } else if frac_digits < scale {
                frac_part = frac_part.saturating_mul(10).saturating_add(digit);
                frac_digits += 1;
            } else if !round_up && digit >= 5 {
                round_up = true;
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

fn deserialize_px_dec<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct PxVisitor;

    impl serde::de::Visitor<'_> for PxVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("string or number for px")
        }

        fn visit_str<E>(self, v: &str) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            parse_scaled(v, 1).ok_or_else(|| E::custom("invalid px"))
        }

        fn visit_string<E>(self, v: String) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            self.visit_str(&v)
        }

        fn visit_f64<E>(self, v: f64) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            Ok((v * 10.0).round() as i64)
        }

        fn visit_u64<E>(self, v: u64) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            Ok((v as i64).saturating_mul(10))
        }

        fn visit_i64<E>(self, v: i64) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            Ok(v.saturating_mul(10))
        }
    }

    deserializer.deserialize_any(PxVisitor)
}

fn deserialize_sz_dec<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct SzVisitor;

    impl serde::de::Visitor<'_> for SzVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("string or number for sz")
        }

        fn visit_str<E>(self, v: &str) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            parse_scaled(v, 2).ok_or_else(|| E::custom("invalid sz"))
        }

        fn visit_string<E>(self, v: String) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            self.visit_str(&v)
        }

        fn visit_f64<E>(self, v: f64) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            Ok((v * 100.0).round() as i64)
        }

        fn visit_u64<E>(self, v: u64) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            Ok((v as i64).saturating_mul(100))
        }

        fn visit_i64<E>(self, v: i64) -> Result<i64, E>
        where
            E: serde::de::Error,
        {
            Ok(v.saturating_mul(100))
        }
    }

    deserializer.deserialize_any(SzVisitor)
}

fn write_status_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    block_number: u64,
    block_time: &str,
    rows: &[StatusOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers = vec![block_number as i64; rows.len()];
    let block_times: Vec<ByteArray> =
        std::iter::repeat(block_time.as_bytes()).take(rows.len()).map(ByteArray::from).collect();
    let statuses: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.status.as_bytes())).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let limit_px: Vec<i64> = rows.iter().map(|r| r.limit_px).collect();
    let is_trigger: Vec<bool> = rows.iter().map(|r| r.is_trigger).collect();
    let tifs: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.tif.as_bytes())).collect();

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

    row_group.close()?;
    Ok(())
}

fn write_diff_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    block_number: u64,
    block_time: &str,
    rows: &[DiffOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers = vec![block_number as i64; rows.len()];
    let block_times: Vec<ByteArray> =
        std::iter::repeat(block_time.as_bytes()).take(rows.len()).map(ByteArray::from).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    let diff_types: Vec<i32> = rows.iter().map(|r| i32::from(r.diff_type)).collect();
    let sizes: Vec<i64> = rows.iter().map(|r| r.sz).collect();

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

    row_group.close()?;
    Ok(())
}

fn write_fill_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    block_number: u64,
    block_time: &str,
    rows: &[FillOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers = vec![block_number as i64; rows.len()];
    let block_times: Vec<ByteArray> =
        std::iter::repeat(block_time.as_bytes()).take(rows.len()).map(ByteArray::from).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let px: Vec<i64> = rows.iter().map(|r| r.px).collect();
    let sz: Vec<i64> = rows.iter().map(|r| r.sz).collect();
    let crossed: Vec<bool> = rows.iter().map(|r| r.crossed).collect();

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

    row_group.close()?;
    Ok(())
}

pub(crate) fn run_archive_writer(rx: Receiver<ArchiveBlock>, stop: Arc<AtomicBool>) {
    let mut writers = ArchiveWriters::new();
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let msg = match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(msg) => msg,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        if !is_archive_enabled() {
            continue;
        }

        let order_batch: BatchLite<OrderStatusLite> = match serde_json::from_slice(&msg.order_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!("Archive parse error for order batch at {}: {err}", msg.block_number);
                continue;
            }
        };
        let diff_batch: BatchLite<DiffLite> = match serde_json::from_slice(&msg.diffs_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!("Archive parse error for diff batch at {}: {err}", msg.block_number);
                continue;
            }
        };
        let fill_batch: BatchLite<FillLite> = match serde_json::from_slice(&msg.fills_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!("Archive parse error for fill batch at {}: {err}", msg.block_number);
                continue;
            }
        };

        let height = order_batch.block_number;
        if diff_batch.block_number != height || fill_batch.block_number != height {
            warn!(
                "Archive height mismatch: order {} diff {} fill {}",
                height, diff_batch.block_number, fill_batch.block_number
            );
            continue;
        }
        let block_time = order_batch.block_time;

        let mut status_btc = Vec::new();
        let mut status_eth = Vec::new();
        for status in order_batch.events {
            let OrderLite { coin, side, oid, limit_px, is_trigger, tif } = status.order;
            if !is_target_coin(&coin) {
                continue;
            }
            let out =
                StatusOut { status: status.status, oid, side, limit_px, is_trigger, tif: tif.unwrap_or_default() };
            match coin.as_str() {
                "BTC" => status_btc.push(out),
                "ETH" => status_eth.push(out),
                _ => {}
            }
        }

        let mut diff_btc = Vec::new();
        let mut diff_eth = Vec::new();
        for diff in diff_batch.events {
            let DiffLite { oid, coin, raw_book_diff } = diff;
            if !is_target_coin(&coin) {
                continue;
            }
            let (diff_type, sz) = match raw_book_diff {
                RawDiffLite::New { sz } => (0u8, sz),
                RawDiffLite::Update { new_sz, .. } => (1u8, new_sz),
                RawDiffLite::Remove => (2u8, 0),
            };
            let out = DiffOut { oid, diff_type, sz };
            match coin.as_str() {
                "BTC" => diff_btc.push(out),
                "ETH" => diff_eth.push(out),
                _ => {}
            }
        }

        let mut fill_btc = Vec::new();
        let mut fill_eth = Vec::new();
        for fill in fill_batch.events {
            let FillLite { coin, side, px, sz, crossed } = fill;
            if !is_target_coin(&coin) {
                continue;
            }
            let out = FillOut { side, px, sz, crossed };
            match coin.as_str() {
                "BTC" => fill_btc.push(out),
                "ETH" => fill_eth.push(out),
                _ => {}
            }
        }

        if let Some(coin_writers) = writers.coin_mut("BTC") {
            if let Err(err) = coin_writers.status.ensure_open("BTC", height).and_then(|_| {
                if let Some(file) = coin_writers.status.file.as_mut() {
                    write_status_rows(&mut file.writer, height, &block_time, &status_btc)
                } else {
                    Ok(())
                }
            }) {
                warn!("Archive write status BTC failed at {}: {err}", height);
            }
            if let Err(err) = coin_writers.diff.ensure_open("BTC", height).and_then(|_| {
                if let Some(file) = coin_writers.diff.file.as_mut() {
                    write_diff_rows(&mut file.writer, height, &block_time, &diff_btc)
                } else {
                    Ok(())
                }
            }) {
                warn!("Archive write diff BTC failed at {}: {err}", height);
            }
            if let Err(err) = coin_writers.fill.ensure_open("BTC", height).and_then(|_| {
                if let Some(file) = coin_writers.fill.file.as_mut() {
                    write_fill_rows(&mut file.writer, height, &block_time, &fill_btc)
                } else {
                    Ok(())
                }
            }) {
                warn!("Archive write fill BTC failed at {}: {err}", height);
            }
        }

        if let Some(coin_writers) = writers.coin_mut("ETH") {
            if let Err(err) = coin_writers.status.ensure_open("ETH", height).and_then(|_| {
                if let Some(file) = coin_writers.status.file.as_mut() {
                    write_status_rows(&mut file.writer, height, &block_time, &status_eth)
                } else {
                    Ok(())
                }
            }) {
                warn!("Archive write status ETH failed at {}: {err}", height);
            }
            if let Err(err) = coin_writers.diff.ensure_open("ETH", height).and_then(|_| {
                if let Some(file) = coin_writers.diff.file.as_mut() {
                    write_diff_rows(&mut file.writer, height, &block_time, &diff_eth)
                } else {
                    Ok(())
                }
            }) {
                warn!("Archive write diff ETH failed at {}: {err}", height);
            }
            if let Err(err) = coin_writers.fill.ensure_open("ETH", height).and_then(|_| {
                if let Some(file) = coin_writers.fill.file.as_mut() {
                    write_fill_rows(&mut file.writer, height, &block_time, &fill_eth)
                } else {
                    Ok(())
                }
            }) {
                warn!("Archive write fill ETH failed at {}: {err}", height);
            }
        }
    }

    if let Err(err) = writers.close_all() {
        warn!("Archive writer close failed: {err}");
    }
}
