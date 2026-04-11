#![allow(unused_crate_dependencies)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArray, Decimal};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;

type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const STATUS_ROW_GROUP_BLOCKS_DEFAULT: u64 = 10_000;
const STATUS_ROW_GROUP_BLOCKS_BTC: u64 = 1_000;
const STATUS_ROW_GROUP_BLOCKS_ETH: u64 = 2_000;
const STATUS_ROW_GROUP_BLOCKS_SOL: u64 = 5_000;
const DIFF_ROW_GROUP_BLOCKS: u64 = 50_000;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StreamArg {
    Diff,
    Status,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum ModeArg {
    Lite,
    Full,
}

#[derive(Debug, Parser)]
#[command(about = "Offline merge-and-dedupe tool for archived diff/status parquet files")]
struct Args {
    #[arg(long, value_enum)]
    stream: StreamArg,

    #[arg(long, value_enum, default_value = "lite")]
    mode: ModeArg,

    #[arg(long)]
    coin: String,

    #[arg(long = "input", required = true)]
    inputs: Vec<PathBuf>,

    #[arg(long)]
    output: PathBuf,

    #[arg(long, default_value_t = false)]
    replace: bool,
}

#[derive(Debug, Clone)]
struct StatusLiteRow {
    block_number: u64,
    block_time: String,
    coin: String,
    time: String,
    user: String,
    status: String,
    oid: u64,
    side: String,
    limit_px: i64,
    sz: i64,
    orig_sz: i64,
    timestamp: i64,
    is_trigger: bool,
    tif: String,
    trigger_condition: String,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    cloid: String,
}

#[derive(Debug, Clone)]
struct StatusFullRow {
    block_number: u64,
    block_time: String,
    coin: String,
    status: String,
    oid: u64,
    side: String,
    limit_px: i64,
    is_trigger: bool,
    tif: String,
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

#[derive(Debug, Clone)]
struct DiffLiteRow {
    block_number: u64,
    block_time: String,
    oid: u64,
    side: String,
    px: i64,
    diff_type: i32,
    sz: i64,
    orig_sz: i64,
}

#[derive(Debug, Clone)]
struct DiffFullRow {
    block_number: u64,
    block_time: String,
    coin: String,
    oid: u64,
    diff_type: i32,
    sz: i64,
    user: String,
    side: String,
    px: i64,
    orig_sz: i64,
    raw_event: String,
}

#[derive(Debug, Clone)]
enum MergeRows {
    StatusLite(Vec<StatusLiteRow>),
    StatusFull(Vec<StatusFullRow>),
    DiffLite(Vec<DiffLiteRow>),
    DiffFull(Vec<DiffFullRow>),
}

fn main() -> AppResult<()> {
    let args = Args::parse();
    if args.inputs.len() < 2 {
        return Err("need at least two --input files to merge".into());
    }
    if args.output.exists() && !args.replace {
        return Err(format!("output already exists: {} (pass --replace to overwrite)", args.output.display()).into());
    }
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }

    let merged = match (args.stream, args.mode) {
        (StreamArg::Status, ModeArg::Lite) => MergeRows::StatusLite(merge_status_lite(&args.inputs, &args.coin)?),
        (StreamArg::Status, ModeArg::Full) => MergeRows::StatusFull(merge_status_full(&args.inputs, &args.coin)?),
        (StreamArg::Diff, ModeArg::Lite) => MergeRows::DiffLite(merge_diff_lite(&args.inputs)?),
        (StreamArg::Diff, ModeArg::Full) => MergeRows::DiffFull(merge_diff_full(&args.inputs, &args.coin)?),
    };

    write_merged_output(&args, &merged)?;

    let (blocks, rows, min_block, max_block) = merged_stats(&merged);
    println!(
        "archive merge repair OK stream={:?} mode={:?} coin={} inputs={} blocks={} rows={} min_block={} max_block={} output={}",
        args.stream,
        args.mode,
        args.coin,
        args.inputs.len(),
        blocks,
        rows,
        min_block,
        max_block,
        args.output.display()
    );
    Ok(())
}

fn merge_status_lite(inputs: &[PathBuf], expected_coin: &str) -> AppResult<Vec<StatusLiteRow>> {
    let mut merged: BTreeMap<u64, Vec<StatusLiteRow>> = BTreeMap::new();
    for input in inputs {
        let rows = read_status_lite_rows(input)?;
        for (block, block_rows) in group_by_block(rows) {
            validate_same_coin_status_lite(&block_rows, expected_coin, input)?;
            merged.insert(block, block_rows);
        }
    }
    Ok(merged.into_values().flatten().collect())
}

fn merge_status_full(inputs: &[PathBuf], expected_coin: &str) -> AppResult<Vec<StatusFullRow>> {
    let mut merged: BTreeMap<u64, Vec<StatusFullRow>> = BTreeMap::new();
    for input in inputs {
        let rows = read_status_full_rows(input)?;
        for (block, block_rows) in group_by_block(rows) {
            validate_same_coin_status_full(&block_rows, expected_coin, input)?;
            merged.insert(block, block_rows);
        }
    }
    Ok(merged.into_values().flatten().collect())
}

fn merge_diff_lite(inputs: &[PathBuf]) -> AppResult<Vec<DiffLiteRow>> {
    let mut merged: BTreeMap<u64, Vec<DiffLiteRow>> = BTreeMap::new();
    for input in inputs {
        let rows = read_diff_lite_rows(input)?;
        for (block, block_rows) in group_by_block(rows) {
            merged.insert(block, block_rows);
        }
    }
    Ok(merged.into_values().flatten().collect())
}

fn merge_diff_full(inputs: &[PathBuf], expected_coin: &str) -> AppResult<Vec<DiffFullRow>> {
    let mut merged: BTreeMap<u64, Vec<DiffFullRow>> = BTreeMap::new();
    for input in inputs {
        let rows = read_diff_full_rows(input)?;
        for (block, block_rows) in group_by_block(rows) {
            validate_same_coin_diff_full(&block_rows, expected_coin, input)?;
            merged.insert(block, block_rows);
        }
    }
    Ok(merged.into_values().flatten().collect())
}

trait HasBlockNumber: Sized {
    fn block_number(&self) -> u64;
}

impl HasBlockNumber for StatusLiteRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}
impl HasBlockNumber for StatusFullRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}
impl HasBlockNumber for DiffLiteRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}
impl HasBlockNumber for DiffFullRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

fn group_by_block<R: HasBlockNumber>(rows: Vec<R>) -> BTreeMap<u64, Vec<R>> {
    let mut grouped = BTreeMap::new();
    for row in rows {
        grouped.entry(row.block_number()).or_insert_with(Vec::new).push(row);
    }
    grouped
}

fn validate_same_coin_status_lite(rows: &[StatusLiteRow], expected_coin: &str, input: &Path) -> AppResult<()> {
    if rows.iter().any(|row| row.coin != expected_coin) {
        return Err(format!("status lite coin mismatch in {}", input.display()).into());
    }
    Ok(())
}

fn validate_same_coin_status_full(rows: &[StatusFullRow], expected_coin: &str, input: &Path) -> AppResult<()> {
    if rows.iter().any(|row| row.coin != expected_coin) {
        return Err(format!("status full coin mismatch in {}", input.display()).into());
    }
    Ok(())
}

fn validate_same_coin_diff_full(rows: &[DiffFullRow], expected_coin: &str, input: &Path) -> AppResult<()> {
    if rows.iter().any(|row| row.coin != expected_coin) {
        return Err(format!("diff full coin mismatch in {}", input.display()).into());
    }
    Ok(())
}

fn write_merged_output(args: &Args, merged: &MergeRows) -> AppResult<()> {
    let tmp_output = args.output.with_file_name(format!(
        "{}.tmp",
        args.output.file_name().and_then(|name| name.to_str()).unwrap_or("merged.parquet")
    ));
    if tmp_output.exists() {
        fs::remove_file(&tmp_output)?;
    }
    let file = File::create(&tmp_output)?;
    let props = std::sync::Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
            .set_dictionary_enabled(true)
            .build(),
    );
    let schema = std::sync::Arc::new(parse_message_type(schema_string(args.stream, args.mode)).expect("valid schema"));
    let mut writer = SerializedFileWriter::new(file, schema, props)?;

    match merged {
        MergeRows::StatusLite(rows) => write_status_lite_rows(&mut writer, rows, &args.coin)?,
        MergeRows::StatusFull(rows) => write_status_full_rows(&mut writer, rows, &args.coin)?,
        MergeRows::DiffLite(rows) => write_diff_lite_rows(&mut writer, rows)?,
        MergeRows::DiffFull(rows) => write_diff_full_rows(&mut writer, rows)?,
    }

    writer.close()?;
    if args.output.exists() {
        fs::remove_file(&args.output)?;
    }
    fs::rename(&tmp_output, &args.output)?;
    Ok(())
}

fn schema_string(stream: StreamArg, mode: ModeArg) -> &'static str {
    match (stream, mode) {
        (StreamArg::Status, ModeArg::Lite) => {
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
            }"
        }
        (StreamArg::Status, ModeArg::Full) => {
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
            }"
        }
        (StreamArg::Diff, ModeArg::Lite) => {
            "message diff_schema {\n\
                REQUIRED INT64 block_number;\n\
                REQUIRED BINARY block_time (UTF8);\n\
                REQUIRED INT64 oid;\n\
                REQUIRED BINARY side (UTF8);\n\
                REQUIRED INT64 px (DECIMAL(18, 8));\n\
                REQUIRED INT32 diff_type;\n\
                REQUIRED INT64 sz (DECIMAL(18, 8));\n\
                REQUIRED INT64 orig_sz (DECIMAL(18, 8));\n\
            }"
        }
        (StreamArg::Diff, ModeArg::Full) => {
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
            }"
        }
    }
}

fn merged_stats(rows: &MergeRows) -> (usize, usize, u64, u64) {
    match rows {
        MergeRows::StatusLite(rows) => stats_from_rows(rows),
        MergeRows::StatusFull(rows) => stats_from_rows(rows),
        MergeRows::DiffLite(rows) => stats_from_rows(rows),
        MergeRows::DiffFull(rows) => stats_from_rows(rows),
    }
}

fn stats_from_rows<R: HasBlockNumber>(rows: &[R]) -> (usize, usize, u64, u64) {
    let blocks = rows.iter().map(HasBlockNumber::block_number).collect::<std::collections::BTreeSet<_>>().len();
    let min_block = rows.first().map(HasBlockNumber::block_number).unwrap_or(0);
    let max_block = rows.last().map(HasBlockNumber::block_number).unwrap_or(0);
    (blocks, rows.len(), min_block, max_block)
}

fn status_row_group_blocks_for_coin(coin: &str) -> u64 {
    match coin {
        "BTC" => STATUS_ROW_GROUP_BLOCKS_BTC,
        "ETH" => STATUS_ROW_GROUP_BLOCKS_ETH,
        "SOL" => STATUS_ROW_GROUP_BLOCKS_SOL,
        _ => STATUS_ROW_GROUP_BLOCKS_DEFAULT,
    }
}

fn aligned_row_group_bounds(block: u64, span: u64) -> (u64, u64) {
    let end = ((block.saturating_sub(1)) / span + 1) * span;
    let start = end.saturating_sub(span) + 1;
    (start, end)
}

fn decimal_to_i64(value: &Decimal) -> AppResult<i64> {
    let data = value.data();
    match data.len() {
        8 => {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(data);
            Ok(i64::from_be_bytes(bytes))
        }
        4 => {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(data);
            Ok(i32::from_be_bytes(bytes) as i64)
        }
        len => Err(format!("unsupported decimal width {len}").into()),
    }
}

fn byte_array(value: &str) -> ByteArray {
    ByteArray::from(value.as_bytes().to_vec())
}

fn read_status_lite_rows(path: &Path) -> AppResult<Vec<StatusLiteRow>> {
    let reader = SerializedFileReader::try_from(path)?;
    let iter = reader.get_row_iter(None)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row?;
        rows.push(StatusLiteRow {
            block_number: row.get_long(0)? as u64,
            block_time: row.get_string(1)?.clone(),
            coin: row.get_string(2)?.clone(),
            time: row.get_string(3)?.clone(),
            user: row.get_string(4)?.clone(),
            status: row.get_string(5)?.clone(),
            oid: row.get_long(6)? as u64,
            side: row.get_string(7)?.clone(),
            limit_px: decimal_to_i64(row.get_decimal(8)?)?,
            sz: decimal_to_i64(row.get_decimal(9)?)?,
            orig_sz: decimal_to_i64(row.get_decimal(10)?)?,
            timestamp: row.get_long(11)?,
            is_trigger: row.get_bool(12)?,
            tif: row.get_string(13)?.clone(),
            trigger_condition: row.get_string(14)?.clone(),
            trigger_px: decimal_to_i64(row.get_decimal(15)?)?,
            is_position_tpsl: row.get_bool(16)?,
            reduce_only: row.get_bool(17)?,
            order_type: row.get_string(18)?.clone(),
            cloid: row.get_string(19)?.clone(),
        });
    }
    Ok(rows)
}

fn read_status_full_rows(path: &Path) -> AppResult<Vec<StatusFullRow>> {
    let reader = SerializedFileReader::try_from(path)?;
    let iter = reader.get_row_iter(None)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row?;
        rows.push(StatusFullRow {
            block_number: row.get_long(0)? as u64,
            block_time: row.get_string(1)?.clone(),
            coin: row.get_string(2)?.clone(),
            status: row.get_string(3)?.clone(),
            oid: row.get_long(4)? as u64,
            side: row.get_string(5)?.clone(),
            limit_px: decimal_to_i64(row.get_decimal(6)?)?,
            is_trigger: row.get_bool(7)?,
            tif: row.get_string(8)?.clone(),
            user: row.get_string(9)?.clone(),
            hash: row.get_string(10)?.clone(),
            order_type: row.get_string(11)?.clone(),
            sz: decimal_to_i64(row.get_decimal(12)?)?,
            orig_sz: decimal_to_i64(row.get_decimal(13)?)?,
            time: row.get_string(14)?.clone(),
            builder: row.get_string(15)?.clone(),
            timestamp: row.get_long(16)?,
            trigger_condition: row.get_string(17)?.clone(),
            trigger_px: decimal_to_i64(row.get_decimal(18)?)?,
            is_position_tpsl: row.get_bool(19)?,
            reduce_only: row.get_bool(20)?,
            cloid: row.get_string(21)?.clone(),
            raw_event: row.get_string(22)?.clone(),
        });
    }
    Ok(rows)
}

fn read_diff_lite_rows(path: &Path) -> AppResult<Vec<DiffLiteRow>> {
    let reader = SerializedFileReader::try_from(path)?;
    let iter = reader.get_row_iter(None)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row?;
        rows.push(DiffLiteRow {
            block_number: row.get_long(0)? as u64,
            block_time: row.get_string(1)?.clone(),
            oid: row.get_long(2)? as u64,
            side: row.get_string(3)?.clone(),
            px: decimal_to_i64(row.get_decimal(4)?)?,
            diff_type: row.get_int(5)?,
            sz: decimal_to_i64(row.get_decimal(6)?)?,
            orig_sz: decimal_to_i64(row.get_decimal(7)?)?,
        });
    }
    Ok(rows)
}

fn read_diff_full_rows(path: &Path) -> AppResult<Vec<DiffFullRow>> {
    let reader = SerializedFileReader::try_from(path)?;
    let iter = reader.get_row_iter(None)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row?;
        rows.push(DiffFullRow {
            block_number: row.get_long(0)? as u64,
            block_time: row.get_string(1)?.clone(),
            coin: row.get_string(2)?.clone(),
            oid: row.get_long(3)? as u64,
            diff_type: row.get_int(4)?,
            sz: decimal_to_i64(row.get_decimal(5)?)?,
            user: row.get_string(6)?.clone(),
            side: row.get_string(7)?.clone(),
            px: decimal_to_i64(row.get_decimal(8)?)?,
            orig_sz: decimal_to_i64(row.get_decimal(9)?)?,
            raw_event: row.get_string(10)?.clone(),
        });
    }
    Ok(rows)
}

fn write_status_lite_rows(
    writer: &mut SerializedFileWriter<File>,
    rows: &[StatusLiteRow],
    coin: &str,
) -> AppResult<()> {
    write_grouped_rows(writer, rows, status_row_group_blocks_for_coin(coin), |writer, group| {
        write_status_lite_group(writer, group)
    })
}

fn write_status_full_rows(
    writer: &mut SerializedFileWriter<File>,
    rows: &[StatusFullRow],
    coin: &str,
) -> AppResult<()> {
    write_grouped_rows(writer, rows, status_row_group_blocks_for_coin(coin), |writer, group| {
        write_status_full_group(writer, group)
    })
}

fn write_diff_lite_rows(writer: &mut SerializedFileWriter<File>, rows: &[DiffLiteRow]) -> AppResult<()> {
    write_grouped_rows(writer, rows, DIFF_ROW_GROUP_BLOCKS, |writer, group| write_diff_lite_group(writer, group))
}

fn write_diff_full_rows(writer: &mut SerializedFileWriter<File>, rows: &[DiffFullRow]) -> AppResult<()> {
    write_grouped_rows(writer, rows, DIFF_ROW_GROUP_BLOCKS, |writer, group| write_diff_full_group(writer, group))
}

fn write_grouped_rows<R: HasBlockNumber>(
    writer: &mut SerializedFileWriter<File>,
    rows: &[R],
    span: u64,
    write_group: impl Fn(&mut SerializedFileWriter<File>, &[R]) -> AppResult<()>,
) -> AppResult<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut start_idx = 0usize;
    while start_idx < rows.len() {
        let (_, group_end_block) = aligned_row_group_bounds(rows[start_idx].block_number(), span);
        let mut end_idx = start_idx;
        while end_idx < rows.len() && rows[end_idx].block_number() <= group_end_block {
            end_idx += 1;
        }
        write_group(writer, &rows[start_idx..end_idx])?;
        start_idx = end_idx;
    }
    Ok(())
}

fn write_status_lite_group(writer: &mut SerializedFileWriter<File>, rows: &[StatusLiteRow]) -> AppResult<()> {
    let mut row_group = writer.next_row_group()?;
    let block_number: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
    let block_time: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.block_time)).collect();
    let coin: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.coin)).collect();
    let time: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.time)).collect();
    let user: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.user)).collect();
    let status: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.status)).collect();
    let oid: Vec<i64> = rows.iter().map(|row| row.oid as i64).collect();
    let side: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.side)).collect();
    let limit_px: Vec<i64> = rows.iter().map(|row| row.limit_px).collect();
    let sz: Vec<i64> = rows.iter().map(|row| row.sz).collect();
    let orig_sz: Vec<i64> = rows.iter().map(|row| row.orig_sz).collect();
    let timestamp: Vec<i64> = rows.iter().map(|row| row.timestamp).collect();
    let is_trigger: Vec<bool> = rows.iter().map(|row| row.is_trigger).collect();
    let tif: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.tif)).collect();
    let trigger_condition: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.trigger_condition)).collect();
    let trigger_px: Vec<i64> = rows.iter().map(|row| row.trigger_px).collect();
    let is_position_tpsl: Vec<bool> = rows.iter().map(|row| row.is_position_tpsl).collect();
    let reduce_only: Vec<bool> = rows.iter().map(|row| row.reduce_only).collect();
    let order_type: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.order_type)).collect();
    let cloid: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.cloid)).collect();
    write_i64_column(&mut row_group, &block_number)?;
    write_byte_array_column(&mut row_group, &block_time)?;
    write_byte_array_column(&mut row_group, &coin)?;
    write_byte_array_column(&mut row_group, &time)?;
    write_byte_array_column(&mut row_group, &user)?;
    write_byte_array_column(&mut row_group, &status)?;
    write_i64_column(&mut row_group, &oid)?;
    write_byte_array_column(&mut row_group, &side)?;
    write_i64_column(&mut row_group, &limit_px)?;
    write_i64_column(&mut row_group, &sz)?;
    write_i64_column(&mut row_group, &orig_sz)?;
    write_i64_column(&mut row_group, &timestamp)?;
    write_bool_column(&mut row_group, &is_trigger)?;
    write_byte_array_column(&mut row_group, &tif)?;
    write_byte_array_column(&mut row_group, &trigger_condition)?;
    write_i64_column(&mut row_group, &trigger_px)?;
    write_bool_column(&mut row_group, &is_position_tpsl)?;
    write_bool_column(&mut row_group, &reduce_only)?;
    write_byte_array_column(&mut row_group, &order_type)?;
    write_byte_array_column(&mut row_group, &cloid)?;
    row_group.close()?;
    Ok(())
}

fn write_status_full_group(writer: &mut SerializedFileWriter<File>, rows: &[StatusFullRow]) -> AppResult<()> {
    let mut row_group = writer.next_row_group()?;
    let block_number: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
    let block_time: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.block_time)).collect();
    let coin: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.coin)).collect();
    let status: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.status)).collect();
    let oid: Vec<i64> = rows.iter().map(|row| row.oid as i64).collect();
    let side: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.side)).collect();
    let limit_px: Vec<i64> = rows.iter().map(|row| row.limit_px).collect();
    let is_trigger: Vec<bool> = rows.iter().map(|row| row.is_trigger).collect();
    let tif: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.tif)).collect();
    let user: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.user)).collect();
    let hash: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.hash)).collect();
    let order_type: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.order_type)).collect();
    let sz: Vec<i64> = rows.iter().map(|row| row.sz).collect();
    let orig_sz: Vec<i64> = rows.iter().map(|row| row.orig_sz).collect();
    let time: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.time)).collect();
    let builder: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.builder)).collect();
    let timestamp: Vec<i64> = rows.iter().map(|row| row.timestamp).collect();
    let trigger_condition: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.trigger_condition)).collect();
    let trigger_px: Vec<i64> = rows.iter().map(|row| row.trigger_px).collect();
    let is_position_tpsl: Vec<bool> = rows.iter().map(|row| row.is_position_tpsl).collect();
    let reduce_only: Vec<bool> = rows.iter().map(|row| row.reduce_only).collect();
    let cloid: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.cloid)).collect();
    let raw_event: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.raw_event)).collect();
    write_i64_column(&mut row_group, &block_number)?;
    write_byte_array_column(&mut row_group, &block_time)?;
    write_byte_array_column(&mut row_group, &coin)?;
    write_byte_array_column(&mut row_group, &status)?;
    write_i64_column(&mut row_group, &oid)?;
    write_byte_array_column(&mut row_group, &side)?;
    write_i64_column(&mut row_group, &limit_px)?;
    write_bool_column(&mut row_group, &is_trigger)?;
    write_byte_array_column(&mut row_group, &tif)?;
    write_byte_array_column(&mut row_group, &user)?;
    write_byte_array_column(&mut row_group, &hash)?;
    write_byte_array_column(&mut row_group, &order_type)?;
    write_i64_column(&mut row_group, &sz)?;
    write_i64_column(&mut row_group, &orig_sz)?;
    write_byte_array_column(&mut row_group, &time)?;
    write_byte_array_column(&mut row_group, &builder)?;
    write_i64_column(&mut row_group, &timestamp)?;
    write_byte_array_column(&mut row_group, &trigger_condition)?;
    write_i64_column(&mut row_group, &trigger_px)?;
    write_bool_column(&mut row_group, &is_position_tpsl)?;
    write_bool_column(&mut row_group, &reduce_only)?;
    write_byte_array_column(&mut row_group, &cloid)?;
    write_byte_array_column(&mut row_group, &raw_event)?;
    row_group.close()?;
    Ok(())
}

fn write_diff_lite_group(writer: &mut SerializedFileWriter<File>, rows: &[DiffLiteRow]) -> AppResult<()> {
    let mut row_group = writer.next_row_group()?;
    let block_number: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
    let block_time: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.block_time)).collect();
    let oid: Vec<i64> = rows.iter().map(|row| row.oid as i64).collect();
    let side: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.side)).collect();
    let px: Vec<i64> = rows.iter().map(|row| row.px).collect();
    let diff_type: Vec<i32> = rows.iter().map(|row| row.diff_type).collect();
    let sz: Vec<i64> = rows.iter().map(|row| row.sz).collect();
    let orig_sz: Vec<i64> = rows.iter().map(|row| row.orig_sz).collect();
    write_i64_column(&mut row_group, &block_number)?;
    write_byte_array_column(&mut row_group, &block_time)?;
    write_i64_column(&mut row_group, &oid)?;
    write_byte_array_column(&mut row_group, &side)?;
    write_i64_column(&mut row_group, &px)?;
    write_i32_column(&mut row_group, &diff_type)?;
    write_i64_column(&mut row_group, &sz)?;
    write_i64_column(&mut row_group, &orig_sz)?;
    row_group.close()?;
    Ok(())
}

fn write_diff_full_group(writer: &mut SerializedFileWriter<File>, rows: &[DiffFullRow]) -> AppResult<()> {
    let mut row_group = writer.next_row_group()?;
    let block_number: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
    let block_time: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.block_time)).collect();
    let coin: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.coin)).collect();
    let oid: Vec<i64> = rows.iter().map(|row| row.oid as i64).collect();
    let diff_type: Vec<i32> = rows.iter().map(|row| row.diff_type).collect();
    let sz: Vec<i64> = rows.iter().map(|row| row.sz).collect();
    let user: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.user)).collect();
    let side: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.side)).collect();
    let px: Vec<i64> = rows.iter().map(|row| row.px).collect();
    let orig_sz: Vec<i64> = rows.iter().map(|row| row.orig_sz).collect();
    let raw_event: Vec<ByteArray> = rows.iter().map(|row| byte_array(&row.raw_event)).collect();
    write_i64_column(&mut row_group, &block_number)?;
    write_byte_array_column(&mut row_group, &block_time)?;
    write_byte_array_column(&mut row_group, &coin)?;
    write_i64_column(&mut row_group, &oid)?;
    write_i32_column(&mut row_group, &diff_type)?;
    write_i64_column(&mut row_group, &sz)?;
    write_byte_array_column(&mut row_group, &user)?;
    write_byte_array_column(&mut row_group, &side)?;
    write_i64_column(&mut row_group, &px)?;
    write_i64_column(&mut row_group, &orig_sz)?;
    write_byte_array_column(&mut row_group, &raw_event)?;
    row_group.close()?;
    Ok(())
}

fn write_i64_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[i64],
) -> AppResult<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, None, None)?;
        }
        col.close()?;
    }
    Ok(())
}

fn write_i32_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[i32],
) -> AppResult<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, None, None)?;
        }
        col.close()?;
    }
    Ok(())
}

fn write_bool_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[bool],
) -> AppResult<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, None, None)?;
        }
        col.close()?;
    }
    Ok(())
}

fn write_byte_array_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[ByteArray],
) -> AppResult<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, None, None)?;
        }
        col.close()?;
    }
    Ok(())
}
