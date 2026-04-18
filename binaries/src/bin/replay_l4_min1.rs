#![allow(unused_crate_dependencies)]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::File;
use std::io::Cursor;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{Decimal128Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow_array::{Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::RequestPayer;
use bytes::Bytes;
use clap::Parser;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;
use serde::Deserialize;

const MILLIS_PER_MINUTE: i64 = 60_000;
const MILLIS_PER_HOUR: i64 = 3_600_000;
const LIVE_STATE_TTL_MS: i64 = 24 * MILLIS_PER_HOUR;
const FILE_ROTATION_BLOCKS: u64 = 1_000_000;
const ROW_GROUP_BLOCKS: u64 = 10_000;
const DECIMAL_PRECISION: u8 = 18;
const USD_DECIMAL_PRECISION: u8 = 12;
const USD_DECIMAL_SCALE: i8 = 2;
const DEFAULT_BATCH_SIZE: usize = 65_536;

const S3_BUCKET: &str = "hyper0";
const S3_ROOT_PREFIX: &str = "hyperliquid";
const SNAPSHOT_PREFIX: &str = "snapshot";

const S3_CACHE_MB: usize = 1_024;
const S3_PAGE_MB: usize = 8;
const S3_METADATA_PREFETCH_KB: usize = 256;
const S3_RANGE_FETCH_CONCURRENCY: usize = 8;
const MAX_VALID_TIME_MS: i64 = 1_000_000; // 1000s
const INDEX_PX_SCALE: i8 = 8;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, help = "Replay output start block (inclusive)")]
    start: u64,

    #[arg(
        long = "span",
        value_parser = parse_height_span,
        help = "Replay block span; supports suffixes like 20k and 10m"
    )]
    span: u64,

    #[arg(long, default_value = "BTC", help = "Coin symbol (BTC/ETH/SOL/HYPE)")]
    coin: String,

    #[arg(long, default_value_t = false, help = "Upload output parquet to S3 in addition to local file")]
    upload: bool,

    #[arg(long, default_value_t = false, help = "Enable requester-pays for S3 reads")]
    requester_pays: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SnapshotOrderState {
    #[serde(default)]
    created_time_ms: i64,
    #[serde(default)]
    remaining_sz: Decimal,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct WarmupAssetSnapshot {
    #[serde(default)]
    live_orders: HashMap<u64, SnapshotOrderState>,
}

#[derive(Debug, Clone, Deserialize)]
struct WarmupStateSnapshot {
    #[serde(default)]
    format_version: Option<u32>,
    #[serde(default)]
    checkpoint_block_number: Option<u64>,
    #[serde(default)]
    assets: HashMap<String, WarmupAssetSnapshot>,
}

#[derive(Debug, Clone)]
struct LiveOrderState {
    created_time_ms: i64,
    remaining_sz: i128,
    side: Option<BookSide>,
    px: Option<i128>,
}

#[derive(Debug, Clone, Default)]
struct SideAccumulator {
    add_count: u64,
    add_sz: i128,
    add_notional: i128,
    rm_count: u64,
    rm_sz: i128,
    rm_notional: i128,
    fill_count: u64,
    fill_sz: i128,
    fill_notional: i128,
    rm_time_sum_ms: i128,
    rm_time_count: u64,
    fill_time_sum_ms: i128,
    fill_time_count: u64,
}

#[derive(Debug, Clone, Default)]
struct MinuteAccumulator {
    bid: SideAccumulator,
    ask: SideAccumulator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone)]
struct MinuteRow {
    minute_close_ts_ms: i64,
    coin: String,
    b_live_cnt: i32,
    b_live: i128,
    b_live_ap: Option<i128>,
    b_add_count: i32,
    b_add: i128,
    b_add_ap: Option<i128>,
    b_rm_count: i32,
    b_rm: i128,
    b_rm_ap: Option<i128>,
    b_fill_count: i32,
    b_fill: i128,
    b_fill_ap: Option<i128>,
    b_rm_ms: Option<i64>,
    b_fill_ms: Option<i64>,
    last_px: Option<i128>,
    index_px: Option<i128>,
    a_live_cnt: i32,
    a_live: i128,
    a_live_ap: Option<i128>,
    a_add_count: i32,
    a_add: i128,
    a_add_ap: Option<i128>,
    a_rm_count: i32,
    a_rm: i128,
    a_rm_ap: Option<i128>,
    a_fill_count: i32,
    a_fill: i128,
    a_fill_ap: Option<i128>,
    a_rm_ms: Option<i64>,
    a_fill_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct ColumnIndexes {
    block_number_idx: usize,
    block_time_idx: usize,
    coin_idx: usize,
    side_idx: usize,
    oid_idx: usize,
    px_idx: usize,
    event_idx: usize,
    sz_idx: usize,
    orig_sz_idx: usize,
    fill_sz_idx: usize,
}

#[derive(Debug)]
struct ReplayState {
    coin_filter: String,
    replay_start: u64,
    output_start: u64,
    output_end: u64,
    px_scale: i8,
    sz_scale: i8,
    notional_scale: i8,

    live_orders: HashMap<i64, LiveOrderState>,
    order_expiry_index: BTreeSet<(i64, i64)>,

    current_minute_start: Option<i64>,
    accum: MinuteAccumulator,
    minute_rows: Vec<MinuteRow>,
    last_fill_px: Option<i128>,
    index_px_by_close_ts_ms: Option<HashMap<i64, i128>>,

    processed_rows: u64,
    current_block: Option<u64>,
    next_progress_block: u64,
    run_started_at: Instant,
    pruned_orders_total: u64,
    stop_after_range: bool,
}

#[derive(Debug, Clone, Default)]
struct PageCacheState {
    pages: HashMap<u64, Bytes>,
    lru: VecDeque<u64>,
    cached_bytes: usize,
}

struct S3CachedAsyncReader {
    client: S3Client,
    bucket: String,
    key: String,
    requester_pays: bool,
    file_size: usize,
    page_size: usize,
    max_cache_bytes: usize,
    metadata_prefetch_bytes: usize,
    metadata: Option<Arc<ParquetMetaData>>,
    cache: PageCacheState,
}

fn parse_height_span(raw: &str) -> std::result::Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("height span cannot be empty".to_owned());
    }

    let lower = trimmed.to_ascii_lowercase();
    let (digits, multiplier) = if let Some(value) = lower.strip_suffix('k') {
        (value, 1_000u64)
    } else if let Some(value) = lower.strip_suffix('m') {
        (value, 1_000_000u64)
    } else {
        (lower.as_str(), 1u64)
    };

    if digits.is_empty() {
        return Err(format!("invalid --span '{raw}', expected integer or suffix k/m"));
    }

    let base = digits.replace('_', "");
    if base.is_empty() {
        return Err(format!("invalid --span '{raw}', expected integer or suffix k/m"));
    }

    let parsed = base.parse::<u64>().map_err(|_| format!("invalid --span '{raw}', expected integer or suffix k/m"))?;
    parsed.checked_mul(multiplier).ok_or_else(|| format!("--span overflow for '{raw}'"))
}

fn window_start_by_span(block_number: u64, span: u64) -> u64 {
    ((block_number - 1) / span) * span + 1
}

fn file_window_start_block(block_number: u64) -> u64 {
    window_start_by_span(block_number, FILE_ROTATION_BLOCKS)
}

fn row_group_window_start_block(block_number: u64) -> u64 {
    window_start_by_span(block_number, ROW_GROUP_BLOCKS)
}

fn decimal_to_i128_with_scale(mut value: Decimal, target_scale: i8) -> Result<i128> {
    if target_scale < 0 {
        bail!("negative decimal scale is unsupported: {}", target_scale);
    }
    value.rescale(u32::try_from(target_scale).map_err(|_| anyhow!("invalid decimal scale {}", target_scale))?);
    Ok(value.mantissa())
}

fn div_round_half_up(num: i128, den: i128) -> Option<i128> {
    if den == 0 {
        return None;
    }
    let half = den.abs() / 2;
    Some(if num >= 0 { (num + half) / den } else { (num - half) / den })
}

fn avg_px(notional_sum: i128, qty_sum: i128) -> Option<i128> {
    if qty_sum <= 0 {
        return None;
    }
    div_round_half_up(notional_sum, qty_sum)
}

fn avg_time_ms(sum_ms: i128, count: u64) -> Option<i64> {
    if count == 0 {
        return None;
    }
    let den = i128::from(count);
    let avg = div_round_half_up(sum_ms, den)?;
    i64::try_from(avg).ok()
}

fn coin_scales(coin: &str) -> Result<(i8, i8)> {
    match coin {
        "BTC" => Ok((1, 5)),
        "ETH" => Ok((3, 4)),
        "SOL" => Ok((3, 2)),
        "HYPE" => Ok((4, 2)),
        other => bail!("unsupported --coin '{}'; supported: BTC, ETH, SOL, HYPE", other),
    }
}

fn decimal_scale_from_field(schema: &Schema, field_name: &str) -> Result<i8> {
    let field = schema.field_with_name(field_name)?;
    match field.data_type() {
        DataType::Decimal128(_, scale) => Ok(*scale),
        other => bail!("field '{}' expected Decimal128, got {:?}", field_name, other),
    }
}

fn build_column_indexes(schema: &Schema) -> Result<ColumnIndexes> {
    Ok(ColumnIndexes {
        block_number_idx: schema.index_of("block_number")?,
        block_time_idx: schema.index_of("block_time")?,
        coin_idx: schema.index_of("coin")?,
        side_idx: schema.index_of("side")?,
        oid_idx: schema.index_of("oid")?,
        px_idx: schema.index_of("px")?,
        event_idx: schema.index_of("event")?,
        sz_idx: schema.index_of("sz")?,
        orig_sz_idx: schema.index_of("orig_sz")?,
        fill_sz_idx: schema.index_of("fill_sz")?,
    })
}

fn parse_book_side(raw: &str) -> Option<BookSide> {
    match raw.as_bytes().first().copied() {
        Some(b'B') | Some(b'b') => Some(BookSide::Bid),
        Some(b'A') | Some(b'a') => Some(BookSide::Ask),
        _ => None,
    }
}

fn side_accum_mut(accum: &mut MinuteAccumulator, side: BookSide) -> &mut SideAccumulator {
    match side {
        BookSide::Bid => &mut accum.bid,
        BookSide::Ask => &mut accum.ask,
    }
}

fn rescale_decimal_round_half_up(value: i128, src_scale: i8, dst_scale: i8) -> i128 {
    if src_scale == dst_scale {
        return value;
    }
    if src_scale > dst_scale {
        let diff = u32::try_from(src_scale - dst_scale).unwrap_or(0);
        let factor = 10_i128.saturating_pow(diff);
        div_round_half_up(value, factor).unwrap_or(0)
    } else {
        let diff = u32::try_from(dst_scale - src_scale).unwrap_or(0);
        let factor = 10_i128.saturating_pow(diff);
        value.saturating_mul(factor)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct LiveWindowSideSummary {
    orders: i64,
    sz: i128,
    notional: i128,
}

fn compute_live_window_summaries(
    state: &ReplayState,
    minute_close_ts_ms: i64,
) -> (LiveWindowSideSummary, LiveWindowSideSummary) {
    let cutoff_ms = minute_close_ts_ms.saturating_sub(MILLIS_PER_HOUR).saturating_add(1);
    let mut bid = LiveWindowSideSummary::default();
    let mut ask = LiveWindowSideSummary::default();

    for order in state.live_orders.values() {
        if order.remaining_sz <= 0 || order.created_time_ms < cutoff_ms {
            continue;
        }
        let Some(side) = order.side else {
            continue;
        };
        let side_summary = match side {
            BookSide::Bid => &mut bid,
            BookSide::Ask => &mut ask,
        };
        side_summary.orders = side_summary.orders.saturating_add(1);
        if let Some(px) = order.px {
            side_summary.sz = side_summary.sz.saturating_add(order.remaining_sz);
            side_summary.notional = side_summary.notional.saturating_add(order.remaining_sz.saturating_mul(px));
        }
    }
    (bid, ask)
}

fn remove_live_order(state: &mut ReplayState, oid: i64) -> Option<LiveOrderState> {
    let removed = state.live_orders.remove(&oid)?;
    state.order_expiry_index.remove(&(removed.created_time_ms, oid));
    Some(removed)
}

fn insert_live_order(
    state: &mut ReplayState,
    oid: i64,
    created_time_ms: i64,
    remaining_sz: i128,
    side: Option<BookSide>,
    px: Option<i128>,
) {
    if let Some(previous) = state.live_orders.insert(oid, LiveOrderState { created_time_ms, remaining_sz, side, px }) {
        state.order_expiry_index.remove(&(previous.created_time_ms, oid));
    }
    state.order_expiry_index.insert((created_time_ms, oid));
}

fn prune_stale_live_orders(state: &mut ReplayState, current_time_ms: i64) -> u64 {
    let cutoff_ms = current_time_ms.saturating_sub(LIVE_STATE_TTL_MS);
    let mut pruned_total = 0u64;
    while let Some((created_time_ms, oid)) = state.order_expiry_index.first().copied() {
        if created_time_ms > cutoff_ms {
            break;
        }
        state.order_expiry_index.pop_first();
        let should_prune =
            state.live_orders.get(&oid).is_some_and(|live_state| live_state.created_time_ms == created_time_ms);
        if !should_prune {
            continue;
        }
        state.live_orders.remove(&oid);
        pruned_total += 1;
    }
    pruned_total
}

fn finalize_minute(state: &mut ReplayState) {
    let Some(minute_start_ms) = state.current_minute_start else {
        return;
    };
    let minute_close_ts_ms = minute_start_ms + MILLIS_PER_MINUTE - 1;
    let b_add_usd =
        rescale_decimal_round_half_up(state.accum.bid.add_notional, state.notional_scale, USD_DECIMAL_SCALE);
    let b_rm_usd = rescale_decimal_round_half_up(state.accum.bid.rm_notional, state.notional_scale, USD_DECIMAL_SCALE);
    let b_fill_usd =
        rescale_decimal_round_half_up(state.accum.bid.fill_notional, state.notional_scale, USD_DECIMAL_SCALE);
    let a_add_usd =
        rescale_decimal_round_half_up(state.accum.ask.add_notional, state.notional_scale, USD_DECIMAL_SCALE);
    let a_rm_usd = rescale_decimal_round_half_up(state.accum.ask.rm_notional, state.notional_scale, USD_DECIMAL_SCALE);
    let a_fill_usd =
        rescale_decimal_round_half_up(state.accum.ask.fill_notional, state.notional_scale, USD_DECIMAL_SCALE);
    let (b_live, a_live) = compute_live_window_summaries(state, minute_close_ts_ms);
    let b_live_usd = rescale_decimal_round_half_up(b_live.notional, state.notional_scale, USD_DECIMAL_SCALE);
    let a_live_usd = rescale_decimal_round_half_up(a_live.notional, state.notional_scale, USD_DECIMAL_SCALE);
    let index_px =
        state.index_px_by_close_ts_ms.as_ref().and_then(|index_map| index_map.get(&minute_close_ts_ms).copied());

    state.minute_rows.push(MinuteRow {
        minute_close_ts_ms,
        coin: state.coin_filter.clone(),
        b_live_cnt: i32::try_from(b_live.orders).unwrap_or(i32::MAX),
        b_live: b_live_usd,
        b_live_ap: avg_px(b_live.notional, b_live.sz),
        b_add_count: i32::try_from(state.accum.bid.add_count).unwrap_or(i32::MAX),
        b_add: b_add_usd,
        b_add_ap: avg_px(state.accum.bid.add_notional, state.accum.bid.add_sz),
        b_rm_count: i32::try_from(state.accum.bid.rm_count).unwrap_or(i32::MAX),
        b_rm: b_rm_usd,
        b_rm_ap: avg_px(state.accum.bid.rm_notional, state.accum.bid.rm_sz),
        b_fill_count: i32::try_from(state.accum.bid.fill_count).unwrap_or(i32::MAX),
        b_fill: b_fill_usd,
        b_fill_ap: avg_px(state.accum.bid.fill_notional, state.accum.bid.fill_sz),
        b_rm_ms: avg_time_ms(state.accum.bid.rm_time_sum_ms, state.accum.bid.rm_time_count),
        b_fill_ms: avg_time_ms(state.accum.bid.fill_time_sum_ms, state.accum.bid.fill_time_count),
        last_px: state.last_fill_px,
        index_px,
        a_live_cnt: i32::try_from(a_live.orders).unwrap_or(i32::MAX),
        a_live: a_live_usd,
        a_live_ap: avg_px(a_live.notional, a_live.sz),
        a_add_count: i32::try_from(state.accum.ask.add_count).unwrap_or(i32::MAX),
        a_add: a_add_usd,
        a_add_ap: avg_px(state.accum.ask.add_notional, state.accum.ask.add_sz),
        a_rm_count: i32::try_from(state.accum.ask.rm_count).unwrap_or(i32::MAX),
        a_rm: a_rm_usd,
        a_rm_ap: avg_px(state.accum.ask.rm_notional, state.accum.ask.rm_sz),
        a_fill_count: i32::try_from(state.accum.ask.fill_count).unwrap_or(i32::MAX),
        a_fill: a_fill_usd,
        a_fill_ap: avg_px(state.accum.ask.fill_notional, state.accum.ask.fill_sz),
        a_rm_ms: avg_time_ms(state.accum.ask.rm_time_sum_ms, state.accum.ask.rm_time_count),
        a_fill_ms: avg_time_ms(state.accum.ask.fill_time_sum_ms, state.accum.ask.fill_time_count),
    });
    state.accum = MinuteAccumulator::default();
}

fn process_batch(batch: &RecordBatch, columns: ColumnIndexes, state: &mut ReplayState) -> Result<()> {
    let block_number_arr = batch
        .column(columns.block_number_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("block_number column type mismatch"))?;
    let block_time_arr = batch
        .column(columns.block_time_idx)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| anyhow!("block_time column type mismatch"))?;
    let coin_arr = batch
        .column(columns.coin_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("coin column type mismatch"))?;
    let side_arr = batch
        .column(columns.side_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("side column type mismatch"))?;
    let oid_arr = batch
        .column(columns.oid_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("oid column type mismatch"))?;
    let px_arr = batch
        .column(columns.px_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow!("px column type mismatch"))?;
    let event_arr = batch
        .column(columns.event_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("event column type mismatch"))?;
    let sz_arr = batch
        .column(columns.sz_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow!("sz column type mismatch"))?;
    let orig_sz_arr = batch
        .column(columns.orig_sz_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow!("orig_sz column type mismatch"))?;
    let fill_sz_arr = batch
        .column(columns.fill_sz_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow!("fill_sz column type mismatch"))?;

    for row in 0..batch.num_rows() {
        let raw_block_number = block_number_arr.value(row);
        let block_number = u64::try_from(raw_block_number)
            .with_context(|| format!("invalid negative block_number value: {raw_block_number}"))?;
        let block_time_ms = block_time_arr.value(row);

        if state.current_block != Some(block_number) {
            if block_number == row_group_window_start_block(block_number) {
                let pruned = prune_stale_live_orders(state, block_time_ms);
                state.pruned_orders_total = state.pruned_orders_total.saturating_add(pruned);
            }
            if block_number >= state.replay_start && block_number >= state.next_progress_block {
                let elapsed_secs = state.run_started_at.elapsed().as_secs_f64();
                let blocks = block_number.saturating_sub(state.replay_start).saturating_add(1);
                info!(
                    "[progress] blocks={} last={} minute_rows={} live={} rows={} elapsed={:.1}s",
                    blocks,
                    block_number,
                    state.minute_rows.len(),
                    state.live_orders.len(),
                    state.processed_rows,
                    elapsed_secs
                );
                while block_number >= state.next_progress_block {
                    state.next_progress_block = state.next_progress_block.saturating_add(FILE_ROTATION_BLOCKS);
                }
            }
            state.current_block = Some(block_number);
        }

        if block_number < state.replay_start {
            continue;
        }
        if block_number > state.output_end {
            state.stop_after_range = true;
            break;
        }
        if coin_arr.value(row) != state.coin_filter {
            continue;
        }

        let emit_output = block_number >= state.output_start;

        let oid: Option<i64> = if !oid_arr.is_null(row) { Some(oid_arr.value(row)) } else { None };
        let px: Option<i128> = if !px_arr.is_null(row) { Some(px_arr.value(row)) } else { None };
        let sz: Option<i128> = if !sz_arr.is_null(row) { Some(sz_arr.value(row)) } else { None };
        let orig_sz: Option<i128> = if !orig_sz_arr.is_null(row) { Some(orig_sz_arr.value(row)) } else { None };
        let fill_sz: Option<i128> = if !fill_sz_arr.is_null(row) { Some(fill_sz_arr.value(row)) } else { None };
        let side_from_row: Option<BookSide> =
            if !side_arr.is_null(row) { parse_book_side(side_arr.value(row)) } else { None };
        let event = event_arr.value(row);

        if emit_output {
            let row_minute_start = block_time_ms.div_euclid(MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE;
            if state.current_minute_start.is_none() {
                state.current_minute_start = Some(row_minute_start);
            }
            while let Some(current) = state.current_minute_start {
                if row_minute_start <= current {
                    break;
                }
                finalize_minute(state);
                state.current_minute_start = Some(current + MILLIS_PER_MINUTE);
            }
        }

        match event {
            "add" => {
                if let (Some(oid_val), Some(sz_val)) = (oid, sz)
                    && sz_val > 0
                {
                    insert_live_order(state, oid_val, block_time_ms, sz_val, side_from_row, px);
                }
                if emit_output
                    && let Some(side) = side_from_row
                    && let (Some(px_val), Some(sz_val)) = (px, sz)
                    && sz_val > 0
                {
                    let side_accum = side_accum_mut(&mut state.accum, side);
                    side_accum.add_count = side_accum.add_count.saturating_add(1);
                    side_accum.add_sz += sz_val;
                    side_accum.add_notional += sz_val.saturating_mul(px_val);
                }
            }
            "cancel" => {
                let mut resolved_side = side_from_row;
                let mut resolved_px = px;
                if emit_output
                    && let Some(oid_val) = oid
                    && let Some(order) = state.live_orders.get_mut(&oid_val)
                {
                    if order.side.is_none() && side_from_row.is_some() {
                        order.side = side_from_row;
                    }
                    if order.px.is_none() && px.is_some() {
                        order.px = px;
                    }
                    if resolved_side.is_none() {
                        resolved_side = order.side;
                    }
                    if resolved_px.is_none() {
                        resolved_px = order.px;
                    }
                    let age_ms = block_time_ms.saturating_sub(order.created_time_ms);
                    if age_ms > 0
                        && age_ms <= MAX_VALID_TIME_MS
                        && let Some(side) = resolved_side
                    {
                        let side_accum = side_accum_mut(&mut state.accum, side);
                        side_accum.rm_time_sum_ms += i128::from(age_ms);
                        side_accum.rm_time_count = side_accum.rm_time_count.saturating_add(1);
                    }
                }
                if let Some(oid_val) = oid {
                    drop(remove_live_order(state, oid_val));
                }
                if emit_output
                    && let Some(side) = resolved_side
                    && let Some(cancel_sz) = orig_sz
                    && cancel_sz > 0
                {
                    let side_accum = side_accum_mut(&mut state.accum, side);
                    side_accum.rm_count = side_accum.rm_count.saturating_add(1);
                    if let Some(px_val) = resolved_px {
                        side_accum.rm_sz += cancel_sz;
                        side_accum.rm_notional += cancel_sz.saturating_mul(px_val);
                    }
                }
            }
            "fill" => {
                let mut resolved_side = side_from_row;
                let mut resolved_px = px;
                if let (Some(oid_val), Some(fill_sz_val)) = (oid, fill_sz)
                    && fill_sz_val > 0
                {
                    let mut should_remove = false;
                    if let Some(order) = state.live_orders.get_mut(&oid_val) {
                        if order.side.is_none() && side_from_row.is_some() {
                            order.side = side_from_row;
                        }
                        if order.px.is_none() && px.is_some() {
                            order.px = px;
                        }
                        if resolved_side.is_none() {
                            resolved_side = order.side;
                        }
                        if resolved_px.is_none() {
                            resolved_px = order.px;
                        }
                        if emit_output {
                            let age_ms = block_time_ms.saturating_sub(order.created_time_ms);
                            if age_ms > 0
                                && age_ms <= MAX_VALID_TIME_MS
                                && let Some(side) = resolved_side
                            {
                                let side_accum = side_accum_mut(&mut state.accum, side);
                                side_accum.fill_time_sum_ms += i128::from(age_ms);
                                side_accum.fill_time_count = side_accum.fill_time_count.saturating_add(1);
                            }
                        }
                        order.remaining_sz = order.remaining_sz.saturating_sub(fill_sz_val);
                        should_remove = order.remaining_sz <= 0;
                    }
                    if should_remove {
                        drop(remove_live_order(state, oid_val));
                    }
                }
                if let (Some(px_val), Some(fill_sz_val)) = (resolved_px, fill_sz)
                    && fill_sz_val > 0
                {
                    state.last_fill_px = Some(px_val);
                    if emit_output
                        && let Some(side) = resolved_side
                    {
                        let side_accum = side_accum_mut(&mut state.accum, side);
                        side_accum.fill_count = side_accum.fill_count.saturating_add(1);
                        side_accum.fill_sz += fill_sz_val;
                        side_accum.fill_notional += fill_sz_val.saturating_mul(px_val);
                    }
                }
            }
            _ => {}
        }

        state.processed_rows += 1;
    }

    Ok(())
}

async fn fetch_s3_bytes(client: &S3Client, bucket: &str, key: &str, requester_pays: bool) -> Result<Bytes> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if requester_pays {
        request = request.request_payer(RequestPayer::Requester);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => {
            if let Some(service_error) = err.as_service_error()
                && !requester_pays
                && service_error.code().is_some_and(|code| code == "AccessDenied")
            {
                bail!("failed to fetch s3://{bucket}/{key}: AccessDenied (try --requester-pays)");
            }
            bail!("failed to fetch s3://{bucket}/{key}: {err}");
        }
    };
    let aggregated =
        response.body.collect().await.with_context(|| format!("failed to read body s3://{bucket}/{key}"))?;
    Ok(aggregated.into_bytes())
}

async fn head_s3_object_len(client: &S3Client, bucket: &str, key: &str, requester_pays: bool) -> Result<usize> {
    let mut request = client.head_object().bucket(bucket).key(key);
    if requester_pays {
        request = request.request_payer(RequestPayer::Requester);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => {
            if let Some(service_error) = err.as_service_error()
                && !requester_pays
                && service_error.code().is_some_and(|code| code == "AccessDenied")
            {
                bail!("failed to head s3://{bucket}/{key}: AccessDenied (try --requester-pays)");
            }
            bail!("failed to head s3://{bucket}/{key}: {err}");
        }
    };
    let content_length =
        response.content_length().ok_or_else(|| anyhow!("missing content-length for s3://{bucket}/{key}"))?;
    if content_length < 0 {
        bail!("invalid negative content-length for s3://{bucket}/{key}: {content_length}");
    }
    usize::try_from(content_length)
        .with_context(|| format!("content-length too large for usize in s3://{bucket}/{key}: {content_length}"))
}

impl S3CachedAsyncReader {
    async fn new(
        client: S3Client,
        bucket: String,
        key: String,
        requester_pays: bool,
        page_size: usize,
        max_cache_bytes: usize,
    ) -> Result<Self> {
        if page_size == 0 {
            bail!("s3 page size must be > 0");
        }
        if max_cache_bytes < page_size {
            bail!("s3 cache bytes ({max_cache_bytes}) must be >= page size ({page_size})");
        }
        let file_size = head_s3_object_len(&client, &bucket, &key, requester_pays).await?;
        Ok(Self {
            client,
            bucket,
            key,
            requester_pays,
            file_size,
            page_size,
            max_cache_bytes,
            metadata_prefetch_bytes: S3_METADATA_PREFETCH_KB * 1024,
            metadata: None,
            cache: PageCacheState::default(),
        })
    }

    fn touch_lru(&mut self, page_start: u64) {
        if let Some(pos) = self.cache.lru.iter().position(|v| *v == page_start) {
            self.cache.lru.remove(pos);
        }
        self.cache.lru.push_back(page_start);
    }

    fn evict_if_needed(&mut self) {
        while self.cache.cached_bytes > self.max_cache_bytes {
            let Some(oldest) = self.cache.lru.pop_front() else {
                break;
            };
            if let Some(bytes) = self.cache.pages.remove(&oldest) {
                self.cache.cached_bytes = self.cache.cached_bytes.saturating_sub(bytes.len());
            }
        }
    }

    async fn fetch_range(&self, range: Range<usize>) -> Result<Bytes> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        if range.end > self.file_size {
            bail!(
                "range out of bounds for s3://{}/{}: {}..{} (file_size={})",
                self.bucket,
                self.key,
                range.start,
                range.end,
                self.file_size
            );
        }
        let end_inclusive = range.end - 1;
        let mut request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(format!("bytes={}-{}", range.start, end_inclusive));
        if self.requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }
        let response = request.send().await.with_context(|| {
            format!("failed range get s3://{}/{} bytes={}-{}", self.bucket, self.key, range.start, end_inclusive)
        })?;
        let aggregated = response.body.collect().await.with_context(|| {
            format!("failed read range s3://{}/{} bytes={}-{}", self.bucket, self.key, range.start, end_inclusive)
        })?;
        let bytes = aggregated.into_bytes();
        if bytes.len() != range.end - range.start {
            bail!(
                "short range read s3://{}/{} bytes={}-{} got={} expected={}",
                self.bucket,
                self.key,
                range.start,
                end_inclusive,
                bytes.len(),
                range.end - range.start
            );
        }
        Ok(bytes)
    }

    async fn get_page(&mut self, page_start: u64) -> Result<Bytes> {
        if let Some(bytes) = self.cache.pages.get(&page_start).cloned() {
            self.touch_lru(page_start);
            return Ok(bytes);
        }
        let start = usize::try_from(page_start).with_context(|| format!("page start overflow: {page_start}"))?;
        let end = start.saturating_add(self.page_size).min(self.file_size);
        let bytes = self.fetch_range(start..end).await?;
        self.cache.cached_bytes = self.cache.cached_bytes.saturating_add(bytes.len());
        self.cache.pages.insert(page_start, bytes.clone());
        self.touch_lru(page_start);
        self.evict_if_needed();
        Ok(bytes)
    }

    async fn get_bytes_cached(&mut self, range: Range<usize>) -> Result<Bytes> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        if range.end > self.file_size {
            bail!(
                "range out of bounds for s3://{}/{}: {}..{} (file_size={})",
                self.bucket,
                self.key,
                range.start,
                range.end,
                self.file_size
            );
        }

        let page_size_u64 = u64::try_from(self.page_size).map_err(|_| anyhow!("page size overflow"))?;
        let first_page =
            (u64::try_from(range.start).map_err(|_| anyhow!("range start overflow"))? / page_size_u64) * page_size_u64;
        let last_page = ((u64::try_from(range.end - 1).map_err(|_| anyhow!("range end overflow"))?) / page_size_u64)
            * page_size_u64;

        if first_page == last_page {
            let page = self.get_page(first_page).await?;
            let offset = range.start
                - usize::try_from(first_page).with_context(|| format!("page offset overflow: {first_page}"))?;
            let len = range.end - range.start;
            return Ok(page.slice(offset..offset + len));
        }

        let mut out = Vec::with_capacity(range.end - range.start);
        let mut cursor = range.start;
        let mut page_start = first_page;
        while page_start <= last_page {
            let page = self.get_page(page_start).await?;
            let page_abs_start =
                usize::try_from(page_start).with_context(|| format!("page start usize overflow: {page_start}"))?;
            let page_abs_end = page_abs_start + page.len();
            let copy_start = cursor.max(page_abs_start);
            let copy_end = range.end.min(page_abs_end);
            if copy_start < copy_end {
                let rel_start = copy_start - page_abs_start;
                let rel_end = copy_end - page_abs_start;
                out.extend_from_slice(&page.slice(rel_start..rel_end));
                cursor = copy_end;
                if cursor >= range.end {
                    break;
                }
            }
            page_start = page_start.saturating_add(page_size_u64);
        }

        if out.len() != range.end - range.start {
            bail!(
                "assembled range size mismatch for s3://{}/{}: got={} expected={}",
                self.bucket,
                self.key,
                out.len(),
                range.end - range.start
            );
        }
        Ok(Bytes::from(out))
    }

    async fn load_metadata(&mut self) -> Result<Arc<ParquetMetaData>> {
        if let Some(existing) = &self.metadata {
            return Ok(existing.clone());
        }
        if self.file_size < FOOTER_SIZE {
            return Err(ParquetError::EOF(format!("file too small for parquet footer: {}", self.file_size)).into());
        }

        let prefetch = self.metadata_prefetch_bytes.max(FOOTER_SIZE);
        let footer_start = self.file_size.saturating_sub(prefetch);
        let suffix = self.get_bytes_cached(footer_start..self.file_size).await?;
        let suffix_len = suffix.len();
        if suffix_len < FOOTER_SIZE {
            return Err(ParquetError::EOF(format!("footer read too short: {}", suffix_len)).into());
        }

        let mut footer = [0u8; FOOTER_SIZE];
        footer.copy_from_slice(&suffix[suffix_len - FOOTER_SIZE..suffix_len]);
        let footer = ParquetMetaDataReader::decode_footer_tail(&footer)?;
        let metadata_len = footer.metadata_length();

        let metadata = if metadata_len > suffix_len - FOOTER_SIZE {
            let start = self.file_size.saturating_sub(metadata_len + FOOTER_SIZE);
            let meta = self.get_bytes_cached(start..self.file_size - FOOTER_SIZE).await?;
            ParquetMetaDataReader::decode_metadata(meta.as_ref())?
        } else {
            let meta_start = suffix_len - FOOTER_SIZE - metadata_len;
            ParquetMetaDataReader::decode_metadata(&suffix[meta_start..meta_start + metadata_len])?
        };

        let arc = Arc::new(metadata);
        self.metadata = Some(arc.clone());
        Ok(arc)
    }
}

impl AsyncFileReader for S3CachedAsyncReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move { self.get_bytes_cached(range).await.map_err(|err| ParquetError::General(err.to_string())) }.boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<usize>>) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            if ranges.is_empty() {
                return Ok(Vec::new());
            }
            let mut out = Vec::with_capacity(ranges.len());
            let mut offset = 0usize;
            while offset < ranges.len() {
                let end = (offset + S3_RANGE_FETCH_CONCURRENCY).min(ranges.len());
                let mut futures = Vec::with_capacity(end - offset);
                for range in &ranges[offset..end] {
                    futures.push(self.fetch_range(range.clone()));
                }
                let chunk = futures_util::future::try_join_all(futures)
                    .await
                    .map_err(|err| ParquetError::General(err.to_string()))?;
                out.extend(chunk);
                offset = end;
            }
            Ok(out)
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move { self.load_metadata().await.map_err(|err| ParquetError::General(err.to_string())) }.boxed()
    }
}

fn encode_minute_rows_to_parquet(rows: &[MinuteRow], px_scale: i8) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("closeTime", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("coin", DataType::Utf8, false),
        Field::new("bLiveCnt", DataType::Int32, false),
        Field::new("bLiveQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("bLiveAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("bAddCnt", DataType::Int32, false),
        Field::new("bAddQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("bAddAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("bRmCnt", DataType::Int32, false),
        Field::new("bRmQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("bRmAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("bRmMs", DataType::Int64, true),
        Field::new("bFillCnt", DataType::Int32, false),
        Field::new("bFillQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("bFillAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("bFillMs", DataType::Int64, true),
        Field::new("lastPx", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("indexPx", DataType::Decimal128(DECIMAL_PRECISION, INDEX_PX_SCALE), true),
        Field::new("aLiveCnt", DataType::Int32, false),
        Field::new("aLiveQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("aLiveAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("aAddCnt", DataType::Int32, false),
        Field::new("aAddQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("aAddAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("aRmCnt", DataType::Int32, false),
        Field::new("aRmQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("aRmAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("aRmMs", DataType::Int64, true),
        Field::new("aFillCnt", DataType::Int32, false),
        Field::new("aFillQv", DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE), false),
        Field::new("aFillAp", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("aFillMs", DataType::Int64, true),
    ]));

    let mut ts_builder = Int64Builder::with_capacity(rows.len());
    let mut close_time_builder = TimestampMillisecondBuilder::with_capacity(rows.len()).with_timezone("UTC");
    let mut coin_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 4);
    let mut b_live_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut b_live_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut b_live_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut b_add_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut b_add_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut b_add_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut b_rm_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut b_rm_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut b_rm_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut b_fill_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut b_fill_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut b_rm_ms_builder = Int64Builder::with_capacity(rows.len());
    let mut b_fill_ms_builder = Int64Builder::with_capacity(rows.len());
    let mut b_fill_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut last_px_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut index_px_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, INDEX_PX_SCALE));
    let mut a_live_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut a_live_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut a_live_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut a_add_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut a_add_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut a_add_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut a_rm_cnt_builder = Int32Builder::with_capacity(rows.len());
    let mut a_rm_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut a_rm_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut a_fill_builder = Decimal128Builder::with_capacity(rows.len())
        .with_data_type(DataType::Decimal128(USD_DECIMAL_PRECISION, USD_DECIMAL_SCALE));
    let mut a_fill_ap_builder =
        Decimal128Builder::with_capacity(rows.len()).with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
    let mut a_rm_ms_builder = Int64Builder::with_capacity(rows.len());
    let mut a_fill_ms_builder = Int64Builder::with_capacity(rows.len());
    let mut a_fill_cnt_builder = Int32Builder::with_capacity(rows.len());

    for row in rows {
        ts_builder.append_value(row.minute_close_ts_ms);
        close_time_builder.append_value(row.minute_close_ts_ms);
        coin_builder.append_value(&row.coin);
        b_live_cnt_builder.append_value(row.b_live_cnt);
        b_live_builder.append_value(row.b_live);
        if let Some(v) = row.b_live_ap {
            b_live_ap_builder.append_value(v);
        } else {
            b_live_ap_builder.append_null();
        }
        b_add_cnt_builder.append_value(row.b_add_count);
        b_add_builder.append_value(row.b_add);
        if let Some(v) = row.b_add_ap {
            b_add_ap_builder.append_value(v);
        } else {
            b_add_ap_builder.append_null();
        }
        b_rm_cnt_builder.append_value(row.b_rm_count);
        b_rm_builder.append_value(row.b_rm);
        if let Some(v) = row.b_rm_ap {
            b_rm_ap_builder.append_value(v);
        } else {
            b_rm_ap_builder.append_null();
        }
        b_fill_builder.append_value(row.b_fill);
        if let Some(v) = row.b_fill_ap {
            b_fill_ap_builder.append_value(v);
        } else {
            b_fill_ap_builder.append_null();
        }
        if let Some(v) = row.b_rm_ms {
            b_rm_ms_builder.append_value(v);
        } else {
            b_rm_ms_builder.append_null();
        }
        b_fill_cnt_builder.append_value(row.b_fill_count);
        if let Some(v) = row.b_fill_ms {
            b_fill_ms_builder.append_value(v);
        } else {
            b_fill_ms_builder.append_null();
        }
        if let Some(v) = row.last_px {
            last_px_builder.append_value(v);
        } else {
            last_px_builder.append_null();
        }
        if let Some(v) = row.index_px {
            index_px_builder.append_value(v);
        } else {
            index_px_builder.append_null();
        }
        a_live_cnt_builder.append_value(row.a_live_cnt);
        a_live_builder.append_value(row.a_live);
        if let Some(v) = row.a_live_ap {
            a_live_ap_builder.append_value(v);
        } else {
            a_live_ap_builder.append_null();
        }
        a_add_cnt_builder.append_value(row.a_add_count);
        a_add_builder.append_value(row.a_add);
        if let Some(v) = row.a_add_ap {
            a_add_ap_builder.append_value(v);
        } else {
            a_add_ap_builder.append_null();
        }
        a_rm_cnt_builder.append_value(row.a_rm_count);
        a_rm_builder.append_value(row.a_rm);
        if let Some(v) = row.a_rm_ap {
            a_rm_ap_builder.append_value(v);
        } else {
            a_rm_ap_builder.append_null();
        }
        a_fill_builder.append_value(row.a_fill);
        if let Some(v) = row.a_fill_ap {
            a_fill_ap_builder.append_value(v);
        } else {
            a_fill_ap_builder.append_null();
        }
        if let Some(v) = row.a_rm_ms {
            a_rm_ms_builder.append_value(v);
        } else {
            a_rm_ms_builder.append_null();
        }
        a_fill_cnt_builder.append_value(row.a_fill_count);
        if let Some(v) = row.a_fill_ms {
            a_fill_ms_builder.append_value(v);
        } else {
            a_fill_ms_builder.append_null();
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(ts_builder.finish()),
        Arc::new(close_time_builder.finish()),
        Arc::new(coin_builder.finish()),
        Arc::new(b_live_cnt_builder.finish()),
        Arc::new(b_live_builder.finish()),
        Arc::new(b_live_ap_builder.finish()),
        Arc::new(b_add_cnt_builder.finish()),
        Arc::new(b_add_builder.finish()),
        Arc::new(b_add_ap_builder.finish()),
        Arc::new(b_rm_cnt_builder.finish()),
        Arc::new(b_rm_builder.finish()),
        Arc::new(b_rm_ap_builder.finish()),
        Arc::new(b_rm_ms_builder.finish()),
        Arc::new(b_fill_cnt_builder.finish()),
        Arc::new(b_fill_builder.finish()),
        Arc::new(b_fill_ap_builder.finish()),
        Arc::new(b_fill_ms_builder.finish()),
        Arc::new(last_px_builder.finish()),
        Arc::new(index_px_builder.finish()),
        Arc::new(a_live_cnt_builder.finish()),
        Arc::new(a_live_builder.finish()),
        Arc::new(a_live_ap_builder.finish()),
        Arc::new(a_add_cnt_builder.finish()),
        Arc::new(a_add_builder.finish()),
        Arc::new(a_add_ap_builder.finish()),
        Arc::new(a_rm_cnt_builder.finish()),
        Arc::new(a_rm_builder.finish()),
        Arc::new(a_rm_ap_builder.finish()),
        Arc::new(a_rm_ms_builder.finish()),
        Arc::new(a_fill_cnt_builder.finish()),
        Arc::new(a_fill_builder.finish()),
        Arc::new(a_fill_ap_builder.finish()),
        Arc::new(a_fill_ms_builder.finish()),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays).context("failed to build output record batch")?;
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)).build();
    let sink = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(sink, schema, Some(props)).context("failed to create parquet writer")?;
    writer.write(&batch).context("failed to write output parquet batch")?;
    let sink = writer.into_inner().context("failed to finalize parquet writer")?;
    Ok(sink.into_inner())
}

fn build_seed_live_state(
    snapshot: &WarmupStateSnapshot,
    coin_filter: &str,
    sz_scale: i8,
) -> Result<(HashMap<i64, LiveOrderState>, BTreeSet<(i64, i64)>)> {
    let mut live_orders: HashMap<i64, LiveOrderState> = HashMap::new();
    let mut order_expiry_index: BTreeSet<(i64, i64)> = BTreeSet::new();
    if let Some(asset_snapshot) = snapshot.assets.get(coin_filter) {
        for (oid_u64, order) in &asset_snapshot.live_orders {
            let Ok(oid) = i64::try_from(*oid_u64) else {
                continue;
            };
            let remaining_sz = decimal_to_i128_with_scale(order.remaining_sz, sz_scale)?;
            if remaining_sz <= 0 {
                continue;
            }
            live_orders.insert(
                oid,
                LiveOrderState { created_time_ms: order.created_time_ms, remaining_sz, side: None, px: None },
            );
            order_expiry_index.insert((order.created_time_ms, oid));
        }
    }
    Ok((live_orders, order_expiry_index))
}

fn build_parquet_keys_for_range(coin_lower: &str, replay_start: u64, output_end: u64) -> Vec<String> {
    let mut keys = Vec::new();
    let mut file_start = file_window_start_block(replay_start);
    let last_file_start = file_window_start_block(output_end);
    while file_start <= last_file_start {
        let file_end = file_start + FILE_ROTATION_BLOCKS - 1;
        keys.push(format!(
            "{}/{coin_lower}_diff/{coin_lower}_diff_{}_{}.parquet",
            S3_ROOT_PREFIX, file_start, file_end
        ));
        file_start = file_start.saturating_add(FILE_ROTATION_BLOCKS);
    }
    keys
}

fn find_latest_index_parquet_path(coin_lower: &str) -> Option<PathBuf> {
    let file_prefix = format!("{coin_lower}_index_klines_");
    let mut search_dirs = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        search_dirs.push(PathBuf::from(home));
    }
    if let Ok(cwd) = std::env::current_dir()
        && !search_dirs.iter().any(|dir| dir == &cwd)
    {
        search_dirs.push(cwd);
    }

    let mut candidates = Vec::new();
    for dir in search_dirs {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name.starts_with(&file_prefix) && file_name.ends_with(".parquet") {
                candidates.push(path);
            }
        }
    }

    candidates.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    candidates.pop()
}

fn load_index_px_by_close_ts(index_path: &Path) -> Result<HashMap<i64, i128>> {
    let file =
        File::open(index_path).with_context(|| format!("failed to open index parquet {}", index_path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to create parquet reader for {}", index_path.display()))?;
    let schema = builder.schema().clone();

    let close_time_idx = schema
        .index_of("close_time_ms")
        .with_context(|| format!("missing close_time_ms in {}", index_path.display()))?;
    let close_px_idx =
        schema.index_of("close_price").with_context(|| format!("missing close_price in {}", index_path.display()))?;
    let close_px_scale = decimal_scale_from_field(&schema, "close_price")?;
    if close_px_scale != INDEX_PX_SCALE {
        bail!(
            "index close_price scale mismatch in {}: expected {} got {}",
            index_path.display(),
            INDEX_PX_SCALE,
            close_px_scale
        );
    }

    let projection = ProjectionMask::roots(builder.parquet_schema(), vec![close_time_idx, close_px_idx]);
    let mut reader = builder
        .with_projection(projection)
        .with_batch_size(DEFAULT_BATCH_SIZE)
        .build()
        .with_context(|| format!("failed to build parquet reader for {}", index_path.display()))?;

    let mut out: HashMap<i64, i128> = HashMap::new();
    let mut projected_columns: Option<(usize, usize)> = None;
    for next in &mut reader {
        let batch = next.with_context(|| format!("failed to read index batch from {}", index_path.display()))?;
        let (batch_time_idx, batch_px_idx) = if let Some(existing) = projected_columns {
            existing
        } else {
            let batch_schema = batch.schema();
            let built = (batch_schema.index_of("close_time_ms")?, batch_schema.index_of("close_price")?);
            projected_columns = Some(built);
            built
        };
        let close_time_arr = batch
            .column(batch_time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("index close_time_ms column type mismatch"))?;
        let close_px_arr = batch
            .column(batch_px_idx)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| anyhow!("index close_price column type mismatch"))?;

        for row in 0..batch.num_rows() {
            if close_time_arr.is_null(row) || close_px_arr.is_null(row) {
                continue;
            }
            out.insert(close_time_arr.value(row), close_px_arr.value(row));
        }
    }
    Ok(out)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    if args.start == 0 {
        bail!("--start must be >= 1");
    }
    if args.span == 0 {
        bail!("--span must be > 0");
    }

    let coin_filter = args.coin.trim().to_ascii_uppercase();
    let coin_lower = coin_filter.to_ascii_lowercase();
    let (px_scale, sz_scale) = coin_scales(&coin_filter)?;
    let notional_scale = px_scale.saturating_add(sz_scale);
    let output_end = args
        .start
        .checked_add(args.span - 1)
        .ok_or_else(|| anyhow!("block range overflow: start={} span={}", args.start, args.span))?;

    let start_file_start = file_window_start_block(args.start);
    let warmup_checkpoint = start_file_start
        .checked_sub(FILE_ROTATION_BLOCKS + 1)
        .ok_or_else(|| {
            anyhow!(
                "cannot auto-resolve previous 1m warmup for start={} (expected start >= {})",
                args.start,
                FILE_ROTATION_BLOCKS * 2 + 1
            )
        })?;
    let replay_start = warmup_checkpoint + 1;

    let warmup_key = format!("{}/{}/warmup_state_{}.msgpack.zst", S3_ROOT_PREFIX, SNAPSHOT_PREFIX, warmup_checkpoint);
    let parquet_keys = build_parquet_keys_for_range(&coin_lower, replay_start, output_end);
    if parquet_keys.is_empty() {
        bail!("no parquet keys resolved for range {}..{}", replay_start, output_end);
    }

    let output_file_name = format!("{}_l4min1_{}_{}.parquet", coin_lower, args.start, output_end);
    let output_local_path = PathBuf::from(&output_file_name);
    let upload_key = format!("{}/{}_l4min1/{}", S3_ROOT_PREFIX, coin_lower, output_file_name);

    info!(
        "[plan] coin={} output_range={}..{} replay_range={}..{} warmup_checkpoint={} start_file_start={} parquet_files={} upload={}",
        coin_filter,
        args.start,
        output_end,
        replay_start,
        output_end,
        warmup_checkpoint,
        start_file_start,
        parquet_keys.len(),
        args.upload
    );
    info!("[plan] warmup_source=s3://{}/{}", S3_BUCKET, warmup_key);
    info!("[plan] output_local={}", output_local_path.display());
    if args.upload {
        info!("[plan] output_s3=s3://{}/{}", S3_BUCKET, upload_key);
    }
    info!("[semantics] b* fill=aggressive sell hitting bids; a* fill=aggressive buy lifting asks");

    let index_px_by_close_ts_ms = if let Some(index_path) = find_latest_index_parquet_path(&coin_lower) {
        info!("[index] found source={}", index_path.display());
        let index_map = load_index_px_by_close_ts(&index_path)?;
        info!("[index] loaded rows={} source={}", index_map.len(), index_path.display());
        Some(index_map)
    } else {
        info!("[index] no local index parquet found for prefix='{}_index_klines_', skipping indexPx join", coin_lower);
        None
    };

    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
    let s3_client = S3Client::new(&aws_config);

    info!("[warmup] loading source=s3://{}/{}", S3_BUCKET, warmup_key);
    let warmup_bytes = fetch_s3_bytes(&s3_client, S3_BUCKET, &warmup_key, args.requester_pays).await?;
    let warmup_snapshot: WarmupStateSnapshot = {
        let decoder = zstd::Decoder::new(Cursor::new(warmup_bytes))
            .with_context(|| format!("failed to create zstd decoder for s3://{}/{}", S3_BUCKET, warmup_key))?;
        rmp_serde::from_read(decoder)
            .with_context(|| format!("failed to decode warmup snapshot s3://{}/{}", S3_BUCKET, warmup_key))?
    };
    info!(
        "[warmup] loaded format_version={:?} checkpoint_block={:?} assets={}",
        warmup_snapshot.format_version,
        warmup_snapshot.checkpoint_block_number,
        warmup_snapshot.assets.len()
    );

    let (live_orders, order_expiry_index) = build_seed_live_state(&warmup_snapshot, &coin_filter, sz_scale)?;
    info!("[warmup] seeded live_orders coin={} count={}", coin_filter, live_orders.len());

    let mut state = ReplayState {
        coin_filter: coin_filter.clone(),
        replay_start,
        output_start: args.start,
        output_end,
        px_scale,
        sz_scale,
        notional_scale,
        live_orders,
        order_expiry_index,
        current_minute_start: None,
        accum: MinuteAccumulator::default(),
        minute_rows: Vec::new(),
        last_fill_px: None,
        index_px_by_close_ts_ms,
        processed_rows: 0,
        current_block: None,
        next_progress_block: replay_start.saturating_add(FILE_ROTATION_BLOCKS - 1),
        run_started_at: Instant::now(),
        pruned_orders_total: 0,
        stop_after_range: false,
    };

    let page_size = S3_PAGE_MB * 1024 * 1024;
    let cache_size = S3_CACHE_MB * 1024 * 1024;

    for key in &parquet_keys {
        if state.stop_after_range {
            break;
        }
        let reader = S3CachedAsyncReader::new(
            s3_client.clone(),
            S3_BUCKET.to_owned(),
            key.clone(),
            args.requester_pays,
            page_size,
            cache_size,
        )
        .await?;
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .with_context(|| format!("failed to open async parquet stream for s3://{}/{}", S3_BUCKET, key))?;
        let schema = builder.schema().clone();
        let px_scale_in = decimal_scale_from_field(&schema, "px")?;
        let sz_scale_in = decimal_scale_from_field(&schema, "fill_sz")?;
        if px_scale_in != state.px_scale || sz_scale_in != state.sz_scale {
            bail!(
                "schema scale mismatch in s3://{}/{} expected px/sz={}/{} got {}/{}",
                S3_BUCKET,
                key,
                state.px_scale,
                state.sz_scale,
                px_scale_in,
                sz_scale_in
            );
        }
        let projection_roots = vec![
            schema.index_of("block_number")?,
            schema.index_of("block_time")?,
            schema.index_of("coin")?,
            schema.index_of("side")?,
            schema.index_of("oid")?,
            schema.index_of("px")?,
            schema.index_of("event")?,
            schema.index_of("sz")?,
            schema.index_of("orig_sz")?,
            schema.index_of("fill_sz")?,
        ];
        let projection_mask = ProjectionMask::roots(builder.parquet_schema(), projection_roots);

        let mut stream = builder.with_projection(projection_mask).with_batch_size(DEFAULT_BATCH_SIZE).build()?;
        let mut projected_columns: Option<ColumnIndexes> = None;

        while let Some(next) = stream.next().await {
            let batch = next.with_context(|| format!("failed reading batch from s3://{}/{}", S3_BUCKET, key))?;
            let columns = if let Some(existing) = projected_columns {
                existing
            } else {
                let built = build_column_indexes(batch.schema().as_ref())?;
                projected_columns = Some(built);
                built
            };
            process_batch(&batch, columns, &mut state)?;
            if state.stop_after_range {
                break;
            }
        }
    }

    if let Some(minute_start_ms) = state.current_minute_start {
        info!(
            "[tail] skip incomplete minute at EOF minute_start_ms={} minute_close_ts_ms={}",
            minute_start_ms,
            minute_start_ms + MILLIS_PER_MINUTE - 1
        );
    }
    if state.minute_rows.is_empty() {
        bail!("no minute rows produced for output range {}..{}", args.start, output_end);
    }

    let output_bytes = encode_minute_rows_to_parquet(&state.minute_rows, state.px_scale)?;
    std::fs::write(&output_local_path, &output_bytes)
        .with_context(|| format!("failed to write local output {}", output_local_path.display()))?;

    if args.upload {
        s3_client
            .put_object()
            .bucket(S3_BUCKET)
            .key(&upload_key)
            .body(ByteStream::from(output_bytes.clone()))
            .send()
            .await
            .with_context(|| format!("failed to upload output parquet to s3://{}/{}", S3_BUCKET, upload_key))?;
    }

    info!(
        "[done] output_local={} minute_rows={} processed_rows={} final_live_orders={} pruned_orders={} output_size_mb={:.2} uploaded={}",
        output_local_path.display(),
        state.minute_rows.len(),
        state.processed_rows,
        state.live_orders.len(),
        state.pruned_orders_total,
        output_bytes.len() as f64 / (1024.0 * 1024.0),
        args.upload
    );

    Ok(())
}
