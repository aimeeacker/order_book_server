#![allow(unused_crate_dependencies)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use clap::Parser;
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use serde::Deserialize;

type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Parser)]
#[command(about = "Convert a date range of raw fills .lz4 files into daily main-perp-only HFT-like parquet files")]
struct Args {
    #[arg(long)]
    hourly_dir: PathBuf,

    #[arg(long)]
    output_dir: PathBuf,

    #[arg(long, help = "Inclusive start date, e.g. 20251103 or 2025-11-03")]
    start_date: String,

    #[arg(long, help = "Inclusive end date, e.g. 20251109 or 2025-11-09")]
    end_date: String,

    #[arg(long, default_value_t = 16)]
    max_concurrency: usize,

    #[arg(long, default_value_t = 500_000)]
    row_group_size: usize,

    #[arg(long, default_value_t = 3)]
    zstd_level: i32,

    #[arg(long, default_value_t = false)]
    replace: bool,
}

#[derive(Debug, Deserialize)]
struct RawBlock {
    block_time: String,
    block_number: u64,
    events: Vec<(String, RawFill)>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFill {
    coin: String,
    px: String,
    sz: String,
    side: String,
    start_position: String,
    dir: String,
    closed_pnl: String,
    oid: u64,
    crossed: bool,
    fee: String,
    tid: u64,
}

#[derive(Debug, Clone)]
struct TradeRow {
    block_number: i64,
    block_time_millis: i64,
    coin: String,
    address: String,
    oid: i64,
    side: String,
    px: i64,
    sz: i64,
    start_position: i64,
    dir: String,
    pnl: i64,
    fee: i64,
    address_m: String,
    oid_m: i64,
    start_position_m: i64,
    dir_m: String,
    pnl_m: i64,
    fee_m: i64,
    tid: i64,
}

#[derive(Debug, Clone)]
struct FillLeg {
    address: String,
    oid: i64,
    side: String,
    pnl: i64,
    fee: i64,
    px: i64,
    sz: i64,
    start_position: i64,
    dir: String,
}

#[derive(Debug, Default)]
struct TradePair {
    taker: Option<FillLeg>,
    maker: Option<FillLeg>,
}

#[derive(Debug, Default, Clone)]
struct ConvertStats {
    input_blocks: u64,
    input_events: u64,
    kept_main_perp_events: u64,
    skipped_non_main_perp: u64,
    skipped_tid_zero: u64,
    duplicate_taker: u64,
    duplicate_maker: u64,
    skipped_missing_taker: u64,
    skipped_missing_maker: u64,
    mismatched_px_or_sz: u64,
    output_rows: u64,
}

#[derive(Debug, Clone)]
struct DayJob {
    date: NaiveDate,
    inputs: Vec<PathBuf>,
    output: PathBuf,
}

#[derive(Debug, Clone)]
struct DaySummary {
    date: NaiveDate,
    output: PathBuf,
    bytes: u64,
    row_groups: u64,
    stats: ConvertStats,
}

struct DayParquetWriter {
    writer: SerializedFileWriter<File>,
    row_group_size: usize,
    buffer: Vec<TradeRow>,
    row_groups_written: u64,
}

fn main() -> AppResult<()> {
    let args = Args::parse();
    validate_args(&args)?;
    fs::create_dir_all(&args.output_dir)?;

    let start_date = parse_date(&args.start_date)?;
    let end_date = parse_date(&args.end_date)?;
    if start_date > end_date {
        return Err(format!("start_date {} is after end_date {}", args.start_date, args.end_date).into());
    }

    let jobs = discover_jobs(&args, start_date, end_date)?;
    let worker_count = args.max_concurrency.min(jobs.len()).max(1);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(worker_count).build()?;
    let args = Arc::new(args);

    let mut results = pool.install(|| {
        jobs.par_iter().map(|job| process_day(job, args.row_group_size, args.zstd_level)).collect::<Vec<_>>()
    });

    let mut summaries = Vec::with_capacity(results.len());
    for result in results.drain(..) {
        summaries.push(result?);
    }
    summaries.sort_by_key(|summary| summary.date);

    let total_bytes: u64 = summaries.iter().map(|summary| summary.bytes).sum();
    let total_row_groups: u64 = summaries.iter().map(|summary| summary.row_groups).sum();
    let total_stats = summaries.iter().fold(ConvertStats::default(), merge_stats);

    for summary in &summaries {
        println!(
            "day={} output={} bytes={} row_groups={} blocks={} input_events={} main_perp_events={} rows={} skipped_non_main_perp={} skipped_tid_zero={} dup_taker={} dup_maker={} missing_taker={} missing_maker={} mismatched_px_or_sz={}",
            summary.date.format("%Y%m%d"),
            summary.output.display(),
            summary.bytes,
            summary.row_groups,
            summary.stats.input_blocks,
            summary.stats.input_events,
            summary.stats.kept_main_perp_events,
            summary.stats.output_rows,
            summary.stats.skipped_non_main_perp,
            summary.stats.skipped_tid_zero,
            summary.stats.duplicate_taker,
            summary.stats.duplicate_maker,
            summary.stats.skipped_missing_taker,
            summary.stats.skipped_missing_maker,
            summary.stats.mismatched_px_or_sz,
        );
    }

    println!(
        "daily parquet OK hourly_dir={} output_dir={} start_date={} end_date={} max_concurrency={} row_group_size={} zstd_level={} days={} total_bytes={} total_row_groups={} blocks={} input_events={} main_perp_events={} rows={} skipped_non_main_perp={} skipped_tid_zero={} dup_taker={} dup_maker={} missing_taker={} missing_maker={} mismatched_px_or_sz={}",
        args.hourly_dir.display(),
        args.output_dir.display(),
        start_date.format("%Y%m%d"),
        end_date.format("%Y%m%d"),
        worker_count,
        args.row_group_size,
        args.zstd_level,
        summaries.len(),
        total_bytes,
        total_row_groups,
        total_stats.input_blocks,
        total_stats.input_events,
        total_stats.kept_main_perp_events,
        total_stats.output_rows,
        total_stats.skipped_non_main_perp,
        total_stats.skipped_tid_zero,
        total_stats.duplicate_taker,
        total_stats.duplicate_maker,
        total_stats.skipped_missing_taker,
        total_stats.skipped_missing_maker,
        total_stats.mismatched_px_or_sz,
    );
    Ok(())
}

fn validate_args(args: &Args) -> AppResult<()> {
    if args.max_concurrency == 0 {
        return Err("--max-concurrency must be greater than 0".into());
    }
    if args.row_group_size == 0 {
        return Err("--row-group-size must be greater than 0".into());
    }
    if !args.hourly_dir.exists() {
        return Err(format!("hourly_dir does not exist: {}", args.hourly_dir.display()).into());
    }
    if !args.hourly_dir.is_dir() {
        return Err(format!("hourly_dir is not a directory: {}", args.hourly_dir.display()).into());
    }
    Ok(())
}

fn discover_jobs(args: &Args, start_date: NaiveDate, end_date: NaiveDate) -> AppResult<Vec<DayJob>> {
    let mut jobs = Vec::new();
    let mut current = start_date;
    while current <= end_date {
        let date_key = current.format("%Y%m%d").to_string();
        let date_dir = args.hourly_dir.join(&date_key);
        if !date_dir.exists() {
            return Err(format!("missing date directory: {}", date_dir.display()).into());
        }
        let inputs = list_hourly_files(&date_dir)?;
        let output = args.output_dir.join(format!("main_perp_{date_key}.parquet"));
        if output.exists() && !args.replace {
            return Err(format!("output already exists: {} (pass --replace to overwrite)", output.display()).into());
        }
        jobs.push(DayJob { date: current, inputs, output });
        current += Duration::days(1);
    }
    Ok(jobs)
}

fn list_hourly_files(date_dir: &Path) -> AppResult<Vec<PathBuf>> {
    let mut files =
        fs::read_dir(date_dir)?.map(|entry| entry.map(|entry| entry.path())).collect::<Result<Vec<_>, _>>()?;
    files.retain(|path| path.extension().is_some_and(|ext| ext == "lz4"));
    files.sort_by_key(|path| {
        path.file_stem().and_then(|stem| stem.to_str()).and_then(|stem| stem.parse::<u32>().ok()).unwrap_or(u32::MAX)
    });
    if files.is_empty() {
        return Err(format!("no .lz4 files found under {}", date_dir.display()).into());
    }
    Ok(files)
}

fn process_day(job: &DayJob, row_group_size: usize, zstd_level: i32) -> AppResult<DaySummary> {
    if let Some(parent) = job.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = DayParquetWriter::new(&job.output, row_group_size, zstd_level)?;
    let mut stats = ConvertStats::default();

    for input in &job.inputs {
        process_hour(input, &mut writer, &mut stats)?;
    }

    let row_groups = writer.finish()?;
    let bytes = fs::metadata(&job.output)?.len();
    Ok(DaySummary { date: job.date, output: job.output.clone(), bytes, row_groups, stats })
}

fn process_hour(input: &Path, writer: &mut DayParquetWriter, stats: &mut ConvertStats) -> AppResult<()> {
    let mut child = Command::new("lz4").arg("-dc").arg(input).stdout(Stdio::piped()).spawn()?;
    let stdout = child.stdout.take().ok_or("failed to capture lz4 stdout")?;
    let reader = BufReader::new(stdout);

    for line in reader.lines() {
        let line = line?;
        let block: RawBlock = serde_json::from_str(&line)?;
        let block_time_millis = parse_timestamp_millis(&block.block_time)?;
        stats.input_blocks = stats.input_blocks.saturating_add(1);
        stats.input_events = stats.input_events.saturating_add(block.events.len() as u64);

        let mut by_trade: BTreeMap<(String, u64), TradePair> = BTreeMap::new();
        for (address, fill) in block.events {
            if !is_main_perp(&fill.coin) {
                stats.skipped_non_main_perp = stats.skipped_non_main_perp.saturating_add(1);
                continue;
            }
            if fill.tid == 0 {
                stats.skipped_tid_zero = stats.skipped_tid_zero.saturating_add(1);
                continue;
            }
            stats.kept_main_perp_events = stats.kept_main_perp_events.saturating_add(1);

            let leg = FillLeg {
                address,
                oid: fill.oid as i64,
                side: fill.side,
                pnl: parse_scaled(&fill.closed_pnl, 6).ok_or("invalid closedPnl")?,
                fee: parse_scaled(&fill.fee, 6).ok_or("invalid fee")?,
                px: parse_scaled(&fill.px, 8).ok_or("invalid px")?,
                sz: parse_scaled(&fill.sz, 8).ok_or("invalid sz")?,
                start_position: parse_scaled(&fill.start_position, 8).ok_or("invalid startPosition")?,
                dir: fill.dir,
            };

            let pair = by_trade.entry((fill.coin, fill.tid)).or_default();
            if fill.crossed {
                if pair.taker.replace(leg).is_some() {
                    stats.duplicate_taker = stats.duplicate_taker.saturating_add(1);
                }
            } else if pair.maker.replace(leg).is_some() {
                stats.duplicate_maker = stats.duplicate_maker.saturating_add(1);
            }
        }

        for ((coin, tid), pair) in by_trade {
            let Some(taker) = pair.taker else {
                stats.skipped_missing_taker = stats.skipped_missing_taker.saturating_add(1);
                continue;
            };
            let Some(maker) = pair.maker else {
                stats.skipped_missing_maker = stats.skipped_missing_maker.saturating_add(1);
                continue;
            };
            if taker.px != maker.px || taker.sz != maker.sz {
                stats.mismatched_px_or_sz = stats.mismatched_px_or_sz.saturating_add(1);
            }

            writer.push(TradeRow {
                block_number: block.block_number as i64,
                block_time_millis,
                coin,
                address: taker.address,
                oid: taker.oid,
                side: taker.side,
                px: taker.px,
                sz: taker.sz,
                start_position: taker.start_position,
                dir: taker.dir,
                pnl: taker.pnl,
                fee: taker.fee,
                address_m: maker.address,
                oid_m: maker.oid,
                start_position_m: maker.start_position,
                dir_m: maker.dir,
                pnl_m: maker.pnl,
                fee_m: maker.fee,
                tid: tid as i64,
            })?;
            stats.output_rows = stats.output_rows.saturating_add(1);
        }
    }

    let status = child.wait()?;
    if !status.success() {
        return Err(format!("lz4 failed for {} with status {status}", input.display()).into());
    }
    Ok(())
}

impl DayParquetWriter {
    fn new(output: &Path, row_group_size: usize, zstd_level: i32) -> AppResult<Self> {
        let schema = parse_message_type(
            "message main_perp_trade_schema {
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY address (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 px (DECIMAL(18, 8));
                REQUIRED INT64 sz (DECIMAL(18, 8));
                REQUIRED INT64 start_position (DECIMAL(18, 8));
                REQUIRED BINARY dir (UTF8);
                REQUIRED INT64 pnl (DECIMAL(18, 6));
                REQUIRED INT64 fee (DECIMAL(18, 6));
                REQUIRED BINARY address_m (UTF8);
                REQUIRED INT64 oid_m;
                REQUIRED INT64 start_position_m (DECIMAL(18, 8));
                REQUIRED BINARY dir_m (UTF8);
                REQUIRED INT64 pnl_m (DECIMAL(18, 6));
                REQUIRED INT64 fee_m (DECIMAL(18, 6));
                OPTIONAL INT32 mm_rate;
                OPTIONAL INT64 mm_share (DECIMAL(18, 6));
                OPTIONAL INT32 filltime;
                REQUIRED INT64 tid;
            }",
        )?;
        let zstd_level =
            ZstdLevel::try_new(zstd_level).map_err(|err| format!("invalid --zstd-level {}: {err}", zstd_level))?;
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(zstd_level))
                .set_dictionary_enabled(true)
                .build(),
        );
        let file = File::create(output)?;
        let writer = SerializedFileWriter::new(file, Arc::new(schema), props)?;
        Ok(Self {
            writer,
            row_group_size,
            buffer: Vec::with_capacity(row_group_size.min(500_000)),
            row_groups_written: 0,
        })
    }

    fn push(&mut self, row: TradeRow) -> AppResult<()> {
        self.buffer.push(row);
        if self.buffer.len() >= self.row_group_size {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> AppResult<u64> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.row_groups_written)
    }

    fn flush(&mut self) -> AppResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        write_row_group(&mut self.writer, &self.buffer)?;
        self.buffer.clear();
        self.row_groups_written = self.row_groups_written.saturating_add(1);
        Ok(())
    }
}

fn write_row_group(writer: &mut SerializedFileWriter<File>, rows: &[TradeRow]) -> AppResult<()> {
    let mut row_group = writer.next_row_group()?;

    let block_numbers: Vec<i64> = rows.iter().map(|r| r.block_number).collect();
    let block_times: Vec<i64> = rows.iter().map(|r| r.block_time_millis).collect();
    let coins: Vec<ByteArray> = rows.iter().map(|r| byte_array_from_str(&r.coin)).collect();
    let addresses: Vec<ByteArray> = rows.iter().map(|r| byte_array_from_str(&r.address)).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| byte_array_from_str(&r.side)).collect();
    let pxs: Vec<i64> = rows.iter().map(|r| r.px).collect();
    let szs: Vec<i64> = rows.iter().map(|r| r.sz).collect();
    let start_positions: Vec<i64> = rows.iter().map(|r| r.start_position).collect();
    let dirs: Vec<ByteArray> = rows.iter().map(|r| byte_array_from_str(&r.dir)).collect();
    let pnls: Vec<i64> = rows.iter().map(|r| r.pnl).collect();
    let fees: Vec<i64> = rows.iter().map(|r| r.fee).collect();
    let maker_addresses: Vec<ByteArray> = rows.iter().map(|r| byte_array_from_str(&r.address_m)).collect();
    let maker_oids: Vec<i64> = rows.iter().map(|r| r.oid_m).collect();
    let maker_start_positions: Vec<i64> = rows.iter().map(|r| r.start_position_m).collect();
    let maker_dirs: Vec<ByteArray> = rows.iter().map(|r| byte_array_from_str(&r.dir_m)).collect();
    let maker_pnls: Vec<i64> = rows.iter().map(|r| r.pnl_m).collect();
    let maker_fees: Vec<i64> = rows.iter().map(|r| r.fee_m).collect();
    let mm_rates = vec![None; rows.len()];
    let mm_shares = vec![None; rows.len()];
    let filltimes = vec![None; rows.len()];
    let tids: Vec<i64> = rows.iter().map(|r| r.tid).collect();

    write_i64_column(&mut row_group, &block_numbers)?;
    write_i64_column(&mut row_group, &block_times)?;
    write_byte_array_column(&mut row_group, &coins)?;
    write_byte_array_column(&mut row_group, &addresses)?;
    write_i64_column(&mut row_group, &oids)?;
    write_byte_array_column(&mut row_group, &sides)?;
    write_i64_column(&mut row_group, &pxs)?;
    write_i64_column(&mut row_group, &szs)?;
    write_i64_column(&mut row_group, &start_positions)?;
    write_byte_array_column(&mut row_group, &dirs)?;
    write_i64_column(&mut row_group, &pnls)?;
    write_i64_column(&mut row_group, &fees)?;
    write_byte_array_column(&mut row_group, &maker_addresses)?;
    write_i64_column(&mut row_group, &maker_oids)?;
    write_i64_column(&mut row_group, &maker_start_positions)?;
    write_byte_array_column(&mut row_group, &maker_dirs)?;
    write_i64_column(&mut row_group, &maker_pnls)?;
    write_i64_column(&mut row_group, &maker_fees)?;

    let (mm_rate_values, mm_rate_def_levels) = parse_optional_i32_column(&mm_rates);
    write_optional_i32_column(&mut row_group, &mm_rate_values, &mm_rate_def_levels)?;
    let (mm_share_values, mm_share_def_levels) = parse_optional_i64_column(&mm_shares);
    write_optional_i64_column(&mut row_group, &mm_share_values, &mm_share_def_levels)?;
    let (filltime_values, filltime_def_levels) = parse_optional_i32_column(&filltimes);
    write_optional_i32_column(&mut row_group, &filltime_values, &filltime_def_levels)?;
    write_i64_column(&mut row_group, &tids)?;

    row_group.close()?;
    Ok(())
}

fn merge_stats(mut acc: ConvertStats, current: &DaySummary) -> ConvertStats {
    acc.input_blocks = acc.input_blocks.saturating_add(current.stats.input_blocks);
    acc.input_events = acc.input_events.saturating_add(current.stats.input_events);
    acc.kept_main_perp_events = acc.kept_main_perp_events.saturating_add(current.stats.kept_main_perp_events);
    acc.skipped_non_main_perp = acc.skipped_non_main_perp.saturating_add(current.stats.skipped_non_main_perp);
    acc.skipped_tid_zero = acc.skipped_tid_zero.saturating_add(current.stats.skipped_tid_zero);
    acc.duplicate_taker = acc.duplicate_taker.saturating_add(current.stats.duplicate_taker);
    acc.duplicate_maker = acc.duplicate_maker.saturating_add(current.stats.duplicate_maker);
    acc.skipped_missing_taker = acc.skipped_missing_taker.saturating_add(current.stats.skipped_missing_taker);
    acc.skipped_missing_maker = acc.skipped_missing_maker.saturating_add(current.stats.skipped_missing_maker);
    acc.mismatched_px_or_sz = acc.mismatched_px_or_sz.saturating_add(current.stats.mismatched_px_or_sz);
    acc.output_rows = acc.output_rows.saturating_add(current.stats.output_rows);
    acc
}

fn parse_date(value: &str) -> AppResult<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
        .map_err(|err| format!("invalid date '{}': {}", value, err).into())
}

fn is_main_perp(coin: &str) -> bool {
    !(coin.starts_with('@') || coin.contains('/') || coin.contains(':') || coin.starts_with('#'))
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
    let mut omitted_digits = 0u32;
    let mut omitted_all_nines = true;
    let mut omitted_zero_prefix_then_single_one = true;
    let mut omitted_seen_single_one = false;

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
                omitted_digits += 1;
                let digit_u8 = b - b'0';
                omitted_all_nines &= digit_u8 == 9;
                if omitted_zero_prefix_then_single_one {
                    if omitted_seen_single_one {
                        omitted_zero_prefix_then_single_one = false;
                    } else if digit_u8 == 0 {
                    } else if digit_u8 == 1 {
                        omitted_seen_single_one = true;
                    } else {
                        omitted_zero_prefix_then_single_one = false;
                    }
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

    let _benign_float_tail =
        omitted_digits > 0 && (omitted_all_nines || (omitted_zero_prefix_then_single_one && omitted_seen_single_one));

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
    let mut scaled = int_part.saturating_mul(scale_factor).saturating_add(frac_part);
    if neg {
        scaled = -scaled;
    }
    Some(scaled)
}

fn parse_timestamp_millis(value: &str) -> AppResult<i64> {
    let value = value.trim();
    if let Ok(raw) = value.parse::<i64>() {
        return infer_epoch_timestamp_millis(raw);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_millis());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_millis());
    }
    Err(format!("invalid timestamp '{value}'").into())
}

fn infer_epoch_timestamp_millis(value: i64) -> AppResult<i64> {
    let abs = value.saturating_abs();
    if abs >= 1_000_000_000_000_000_000 {
        Ok(value / 1_000_000)
    } else if abs >= 1_000_000_000_000_000 {
        Ok(value / 1_000)
    } else if abs >= 1_000_000_000_000 {
        Ok(value)
    } else {
        value.checked_mul(1_000).ok_or_else(|| format!("timestamp seconds overflow: {value}").into())
    }
}

fn parse_optional_i32_column(values: &[Option<i32>]) -> (Vec<i32>, Vec<i16>) {
    let mut parsed = Vec::new();
    let mut def_levels = Vec::with_capacity(values.len());
    for value in values {
        if let Some(value) = value {
            parsed.push(*value);
            def_levels.push(1);
        } else {
            def_levels.push(0);
        }
    }
    (parsed, def_levels)
}

fn parse_optional_i64_column(values: &[Option<i64>]) -> (Vec<i64>, Vec<i16>) {
    let mut parsed = Vec::new();
    let mut def_levels = Vec::with_capacity(values.len());
    for value in values {
        if let Some(value) = value {
            parsed.push(*value);
            def_levels.push(1);
        } else {
            def_levels.push(0);
        }
    }
    (parsed, def_levels)
}

fn byte_array_from_str(value: &str) -> ByteArray {
    ByteArray::from(value.as_bytes().to_vec())
}

fn write_i64_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[i64],
) -> parquet::errors::Result<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, None, None)?;
        }
        col.close()?;
    }
    Ok(())
}

fn write_byte_array_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[ByteArray],
) -> parquet::errors::Result<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, None, None)?;
        }
        col.close()?;
    }
    Ok(())
}

fn write_optional_i32_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[i32],
    def_levels: &[i16],
) -> parquet::errors::Result<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, Some(def_levels), None)?;
        }
        col.close()?;
    }
    Ok(())
}

fn write_optional_i64_column(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<File>,
    values: &[i64],
    def_levels: &[i16],
) -> parquet::errors::Result<()> {
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(values, Some(def_levels), None)?;
        }
        col.close()?;
    }
    Ok(())
}
