#![allow(unused_crate_dependencies)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use chrono::NaiveDateTime;
use clap::Parser;
use parquet::data_type::Decimal;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde::Deserialize;

const SCALE: i64 = 100_000_000;
const SEGMENT_MAGIC: &[u8; 8] = b"L4SEG001";

type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Parser)]
#[command(about = "Offline checkpoint-to-checkpoint replay validator for archived order book data")]
struct Args {
    #[arg(long)]
    snapshot_index: PathBuf,

    #[arg(long, default_value = "/mnt")]
    parquet_root: PathBuf,

    #[arg(long, default_value = "BTC")]
    coin: String,

    #[arg(long)]
    start_height: u64,

    #[arg(long)]
    end_height: u64,
}

#[derive(Debug, Deserialize)]
struct DatasetIndex {
    segments: Vec<DatasetSegmentSummary>,
}

#[derive(Debug, Deserialize)]
struct DatasetSegmentSummary {
    segment_path: String,
    entries: Vec<SegmentIndexEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct SegmentIndexEntry {
    block_height: u64,
    offset: u64,
    len: u64,
}

#[derive(Debug, Deserialize)]
struct SegmentHeader {
    version: u32,
    segment_start: u64,
    segment_end: u64,
}

type CheckpointRecord = (u64, String, bool, bool, Option<Vec<String>>, SnapshotDocument);

type SnapshotDocument = Vec<(String, LevelsPayload)>;

#[derive(Debug, Deserialize)]
struct LevelsPayload(u64, String, [Vec<SnapshotOrder>; 2]);

#[derive(Debug, Clone, Deserialize)]
struct SnapshotOrder(
    String,
    String,
    String,
    String,
    u64,
    u64,
    String,
    bool,
    String,
    Vec<SnapshotOrder>,
    bool,
    bool,
    String,
    String,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Order {
    side: Side,
    limit_px: i64,
    sz: i64,
    oid: u64,
    timestamp: u64,
    trigger_condition: String,
    is_trigger: bool,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    tif: Option<String>,
    cloid: Option<String>,
}

#[derive(Debug, Clone)]
struct StatusRow {
    block_number: u64,
    time_ms: u64,
    status: String,
    oid: u64,
    side: Side,
    limit_px: i64,
    timestamp: u64,
    is_trigger: bool,
    tif: Option<String>,
    trigger_condition: String,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    cloid: Option<String>,
}

#[derive(Debug, Clone)]
enum DiffType {
    New { sz: i64 },
    Update { new_sz: i64 },
    Remove,
}

#[derive(Debug, Clone)]
struct DiffRow {
    block_number: u64,
    oid: u64,
    diff: DiffType,
}

#[derive(Debug)]
struct BlockStats {
    rows: u64,
    min_block: u64,
    max_block: u64,
    expected_rows: u64,
    block_times: HashMap<u64, String>,
    status_counts: HashMap<u64, i32>,
    diff_counts: HashMap<u64, i32>,
}

#[derive(Debug, Clone)]
struct ArchiveFileRange {
    path: PathBuf,
    start: u64,
    end: u64,
}

#[derive(Debug, Clone)]
struct OrderTrace {
    last_height: u64,
    last_action: &'static str,
    order_type: String,
}

#[derive(Default)]
struct OrderBook {
    oid_to_side_px: HashMap<u64, (Side, i64)>,
    bids: BTreeMap<i64, VecDeque<Order>>,
    asks: BTreeMap<i64, VecDeque<Order>>,
}

impl OrderBook {
    fn add_order(&mut self, mut order: Order) {
        let filled = match order.side {
            Side::Ask => Self::match_order(&mut self.bids, &mut order, true),
            Side::Bid => Self::match_order(&mut self.asks, &mut order, false),
        };
        for oid in filled {
            self.oid_to_side_px.remove(&oid);
        }
        if order.sz > 0 {
            self.oid_to_side_px.insert(order.oid, (order.side.clone(), order.limit_px));
            let book = match order.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            book.entry(order.limit_px).or_default().push_back(order);
        }
    }

    fn modify_sz(&mut self, oid: u64, new_sz: i64) -> bool {
        let Some((side, px)) = self.oid_to_side_px.get(&oid).cloned() else {
            return false;
        };
        let book = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let Some(queue) = book.get_mut(&px) else {
            return false;
        };
        if let Some(order) = queue.iter_mut().find(|order| order.oid == oid) {
            order.sz = new_sz;
            return true;
        }
        false
    }

    fn get_order(&self, oid: u64) -> Option<&Order> {
        let (side, px) = self.oid_to_side_px.get(&oid)?;
        let book = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        let queue = book.get(px)?;
        queue.iter().find(|order| order.oid == oid)
    }

    fn cancel_order(&mut self, oid: u64) -> bool {
        let Some((side, px)) = self.oid_to_side_px.remove(&oid) else {
            return false;
        };
        let book = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let Some(queue) = book.get_mut(&px) else {
            return false;
        };
        if let Some(idx) = queue.iter().position(|order| order.oid == oid) {
            let _unused = queue.remove(idx);
            if queue.is_empty() {
                book.remove(&px);
            }
            return true;
        }
        false
    }

    fn snapshot(&self) -> [Vec<Order>; 2] {
        let bids = self.bids.iter().rev().flat_map(|(_, q)| q.iter().cloned()).collect();
        let asks = self.asks.iter().flat_map(|(_, q)| q.iter().cloned()).collect();
        [bids, asks]
    }

    fn match_order(book: &mut BTreeMap<i64, VecDeque<Order>>, taker: &mut Order, taker_is_ask: bool) -> Vec<u64> {
        let prices: Vec<i64> =
            if taker_is_ask { book.keys().copied().rev().collect() } else { book.keys().copied().collect() };
        let mut removed = Vec::new();
        let mut empty = Vec::new();
        for px in prices {
            let matches = if taker_is_ask { px >= taker.limit_px } else { px <= taker.limit_px };
            if !matches {
                break;
            }
            let Some(queue) = book.get_mut(&px) else {
                continue;
            };
            while let Some(maker) = queue.front_mut() {
                let matched = taker.sz.min(maker.sz);
                taker.sz -= matched;
                maker.sz -= matched;
                if maker.sz == 0 {
                    removed.push(maker.oid);
                    let _unused = queue.pop_front();
                }
                if taker.sz == 0 {
                    break;
                }
            }
            if queue.is_empty() {
                empty.push(px);
            }
            if taker.sz == 0 {
                break;
            }
        }
        for px in empty {
            book.remove(&px);
        }
        removed
    }
}

struct RowStream<T> {
    rows: Vec<T>,
    idx: usize,
}

impl<T> RowStream<T> {
    fn new(rows: Vec<T>) -> Self {
        Self { rows, idx: 0 }
    }
}

impl RowStream<StatusRow> {
    fn next_block(&mut self, height: u64) -> Vec<StatusRow> {
        let mut out = Vec::new();
        while self.idx < self.rows.len() {
            let row = &self.rows[self.idx];
            if row.block_number < height {
                self.idx += 1;
                continue;
            }
            if row.block_number > height {
                break;
            }
            out.push(self.rows[self.idx].clone());
            self.idx += 1;
        }
        out
    }
}

impl RowStream<DiffRow> {
    fn next_block(&mut self, height: u64) -> Vec<DiffRow> {
        let mut out = Vec::new();
        while self.idx < self.rows.len() {
            let row = &self.rows[self.idx];
            if row.block_number < height {
                self.idx += 1;
                continue;
            }
            if row.block_number > height {
                break;
            }
            out.push(self.rows[self.idx].clone());
            self.idx += 1;
        }
        out
    }
}

fn main() -> AppResult<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let coin = args.coin.trim().to_ascii_uppercase();
    let event_start = args.start_height + 1;
    let event_end = args.end_height;

    let start_snapshot = extract_coin_snapshot(load_checkpoint(&args.snapshot_index, args.start_height)?, &coin)?;
    let expected_end_snapshot = extract_coin_snapshot(load_checkpoint(&args.snapshot_index, args.end_height)?, &coin)?;

    let status_dir = args.parquet_root.join(&coin).join("status");
    let diff_dir = args.parquet_root.join(&coin).join("diff");
    let blocks_dir = args.parquet_root.join("blocks");

    let status_files = select_archive_files(&status_dir, Some((&coin, "status")), event_start, event_end)?;
    let diff_files = select_archive_files(&diff_dir, Some((&coin, "diff")), event_start, event_end)?;
    let block_files = select_archive_files(&blocks_dir, None, event_start, event_end)?;

    let status_rows = read_status_rows(&status_files, event_start, event_end)?;
    let diff_rows = read_diff_rows(&diff_files, event_start, event_end)?;
    let block_stats = read_block_stats(&block_files, event_start, event_end, &coin)?;
    verify_block_counts(&coin, &status_rows, &diff_rows, &block_stats)?;

    let mut traces = seed_initial_traces(&start_snapshot, args.start_height);
    let mut book = build_initial_book(start_snapshot);
    let mut status_stream = RowStream::new(status_rows);
    let mut diff_stream = RowStream::new(diff_rows);

    for height in event_start..=event_end {
        let statuses = status_stream.next_block(height);
        let diffs = diff_stream.next_block(height);
        let mut order_map: HashMap<u64, StatusRow> =
            statuses.into_iter().filter(is_inserted_into_book).map(|status| (status.oid, status)).collect();
        for diff in diffs {
            match diff.diff {
                DiffType::New { sz } => {
                    let Some(status) = order_map.remove(&diff.oid) else {
                        return err(format!("missing opening status at height {} oid {}", height, diff.oid));
                    };
                    let mut order = Order {
                        side: status.side,
                        limit_px: status.limit_px,
                        sz,
                        oid: status.oid,
                        timestamp: status.timestamp,
                        trigger_condition: status.trigger_condition,
                        is_trigger: status.is_trigger,
                        trigger_px: status.trigger_px,
                        is_position_tpsl: status.is_position_tpsl,
                        reduce_only: status.reduce_only,
                        order_type: status.order_type,
                        tif: status.tif,
                        cloid: status.cloid,
                    };
                    convert_trigger(&mut order, status.time_ms);
                    traces.insert(
                        order.oid,
                        OrderTrace { last_height: height, last_action: "new", order_type: order.order_type.clone() },
                    );
                    book.add_order(order);
                }
                DiffType::Update { new_sz } => {
                    if !book.modify_sz(diff.oid, new_sz) {
                        return err(format!("update missing order at height {} oid {}", height, diff.oid));
                    }
                    if let Some(order) = book.get_order(diff.oid) {
                        traces.insert(
                            diff.oid,
                            OrderTrace {
                                last_height: height,
                                last_action: "update",
                                order_type: order.order_type.clone(),
                            },
                        );
                    }
                }
                DiffType::Remove => {
                    if !book.cancel_order(diff.oid) {
                        return err(format!("remove missing order at height {} oid {}", height, diff.oid));
                    }
                    let order_type = traces
                        .get(&diff.oid)
                        .map(|trace| trace.order_type.clone())
                        .unwrap_or_else(|| "unknown".to_string());
                    traces.insert(diff.oid, OrderTrace { last_height: height, last_action: "remove", order_type });
                }
            }
        }
    }

    let actual_end_snapshot = book.snapshot();
    compare_snapshots(actual_end_snapshot.clone(), expected_end_snapshot, &traces, &block_stats)?;

    println!(
        "offline replay check OK coin={} start={} end={} block_rows={} min_block={} max_block={} expected_rows={} final_bids={} final_asks={}",
        coin,
        args.start_height,
        args.end_height,
        block_stats.rows,
        block_stats.min_block,
        block_stats.max_block,
        block_stats.expected_rows,
        actual_end_snapshot[0].len(),
        actual_end_snapshot[1].len()
    );
    Ok(())
}

fn validate_args(args: &Args) -> AppResult<()> {
    if args.start_height == 0 || args.end_height == 0 {
        return err("checkpoint heights must be positive");
    }
    if args.start_height >= args.end_height {
        return err("start_height must be less than end_height");
    }
    if args.start_height % 10_000 != 0 || args.end_height % 10_000 != 0 {
        return err("checkpoint heights must be multiples of 10000");
    }
    Ok(())
}

fn extract_coin_snapshot(record: CheckpointRecord, coin: &str) -> AppResult<[Vec<Order>; 2]> {
    for (entry_coin, payload) in record.5 {
        if entry_coin != coin {
            continue;
        }
        let LevelsPayload(block_height, _block_time, [bids, asks]) = payload;
        if block_height != record.0 {
            return err("checkpoint height mismatch inside payload");
        }
        let bids = bids.into_iter().map(order_from_snapshot).collect::<AppResult<Vec<_>>>()?;
        let asks = asks.into_iter().map(order_from_snapshot).collect::<AppResult<Vec<_>>>()?;
        return Ok([bids, asks]);
    }
    err(format!("coin {} missing in checkpoint {}", coin, record.0))
}

fn order_from_snapshot(order: SnapshotOrder) -> AppResult<Order> {
    let SnapshotOrder(
        _coin,
        side,
        limit_px,
        sz,
        oid,
        timestamp,
        trigger_condition,
        is_trigger,
        trigger_px,
        _children,
        is_position_tpsl,
        reduce_only,
        order_type,
        _orig_sz,
        tif,
        cloid,
    ) = order;
    Ok(Order {
        side: parse_side(&side)?,
        limit_px: parse_scaled(&limit_px)?,
        sz: parse_scaled(&sz)?,
        oid,
        timestamp,
        trigger_condition,
        is_trigger,
        trigger_px: parse_scaled(&trigger_px)?,
        is_position_tpsl,
        reduce_only,
        order_type,
        tif,
        cloid,
    })
}

fn build_initial_book(snapshot: [Vec<Order>; 2]) -> OrderBook {
    let mut book = OrderBook::default();
    for side_orders in snapshot {
        for order in side_orders {
            book.add_order(order);
        }
    }
    book
}

fn seed_initial_traces(snapshot: &[Vec<Order>; 2], start_height: u64) -> HashMap<u64, OrderTrace> {
    let mut traces = HashMap::new();
    for side_orders in snapshot {
        for order in side_orders {
            traces.insert(
                order.oid,
                OrderTrace {
                    last_height: start_height,
                    last_action: "checkpoint_init",
                    order_type: order.order_type.clone(),
                },
            );
        }
    }
    traces
}

fn is_inserted_into_book(status: &StatusRow) -> bool {
    (status.status == "open" && !status.is_trigger && status.tif.as_deref() != Some("Ioc"))
        || (status.is_trigger && status.status == "triggered")
}

fn convert_trigger(order: &mut Order, ts: u64) {
    if order.is_trigger {
        order.trigger_px = 0;
        order.trigger_condition = "Triggered".to_string();
        order.is_trigger = false;
        order.timestamp = ts;
        order.tif = Some("Gtc".to_string());
    }
}

fn load_checkpoint(snapshot_index: &Path, height: u64) -> AppResult<CheckpointRecord> {
    let index: DatasetIndex = serde_json::from_slice(&fs::read(snapshot_index)?)?;
    let mut found: Option<(PathBuf, SegmentIndexEntry)> = None;
    for segment in index.segments {
        if let Some(entry) = segment.entries.into_iter().find(|entry| entry.block_height == height) {
            let raw_segment_path = PathBuf::from(segment.segment_path);
            let segment_path = if raw_segment_path.is_absolute() {
                raw_segment_path
            } else {
                snapshot_index.parent().unwrap_or_else(|| Path::new(".")).join(raw_segment_path)
            };
            found = Some((segment_path, entry));
            break;
        }
    }
    let Some((segment_path, entry)) = found else {
        return err(format!("checkpoint {} missing in {}", height, snapshot_index.display()));
    };
    let mut file = File::open(&segment_path)?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != SEGMENT_MAGIC {
        return err(format!("bad segment magic {}", segment_path.display()));
    }
    let mut header_len_bytes = [0_u8; 4];
    file.read_exact(&mut header_len_bytes)?;
    let header_len = u32::from_le_bytes(header_len_bytes) as usize;
    let mut header_bytes = vec![0_u8; header_len];
    file.read_exact(&mut header_bytes)?;
    let header: SegmentHeader = rmp_serde::from_slice(&header_bytes)?;
    if header.version != 2 || height < header.segment_start || height > header.segment_end {
        return err(format!("segment header mismatch for checkpoint {}", height));
    }
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut payload = vec![0_u8; entry.len as usize];
    file.read_exact(&mut payload)?;
    let decoded = zstd::stream::decode_all(Cursor::new(payload))?;
    let record: CheckpointRecord = rmp_serde::from_slice(&decoded)?;
    if record.0 != height {
        return err(format!("decoded checkpoint mismatch {} != {}", record.0, height));
    }
    Ok(record)
}

fn select_archive_files(
    dir: &Path,
    event_prefix: Option<(&str, &str)>,
    target_start: u64,
    target_end: u64,
) -> AppResult<Vec<ArchiveFileRange>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|name| name.to_str()).map(str::to_owned) else {
            continue;
        };
        let range = if let Some((coin, kind)) = event_prefix {
            let prefix = format!("{coin}_{kind}_");
            let Some(rest) = stem.strip_prefix(&prefix) else {
                continue;
            };
            parse_range_suffix(rest, path)?
        } else {
            let Some(rest) = stem.strip_prefix("blocks_") else {
                continue;
            };
            parse_range_suffix(rest, path)?
        };
        if range.end < target_start || range.start > target_end {
            continue;
        }
        out.push(range);
    }
    out.sort_by_key(|range| range.start);
    if out.is_empty() {
        return err(format!("no parquet files in {} cover {}..={}", dir.display(), target_start, target_end));
    }
    Ok(out)
}

fn parse_range_suffix(suffix: &str, path: PathBuf) -> AppResult<ArchiveFileRange> {
    let mut parts = suffix.split('_');
    let start = parts.next().ok_or_else(|| format!("bad parquet stem {}", path.display()))?.parse::<u64>()?;
    let end = parts.next().ok_or_else(|| format!("bad parquet stem {}", path.display()))?.parse::<u64>()?;
    if parts.next().is_some() {
        return err(format!("unexpected parquet stem {}", path.display()));
    }
    Ok(ArchiveFileRange { path, start, end })
}

fn column_indexes(reader: &SerializedFileReader<File>) -> HashMap<String, usize> {
    reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.name().to_string(), idx))
        .collect()
}

fn column_idx(columns: &HashMap<String, usize>, name: &str) -> AppResult<usize> {
    columns.get(name).copied().ok_or_else(|| format!("missing parquet column {}", name).into())
}

fn read_status_rows(files: &[ArchiveFileRange], start_height: u64, end_height: u64) -> AppResult<Vec<StatusRow>> {
    let mut out = Vec::new();
    for file in files {
        let reader = SerializedFileReader::new(File::open(&file.path)?)?;
        let cols = column_indexes(&reader);
        let block_number_idx = column_idx(&cols, "block_number")?;
        let time_idx = column_idx(&cols, "time")?;
        let status_idx = column_idx(&cols, "status")?;
        let oid_idx = column_idx(&cols, "oid")?;
        let side_idx = column_idx(&cols, "side")?;
        let limit_px_idx = column_idx(&cols, "limit_px")?;
        let timestamp_idx = column_idx(&cols, "timestamp")?;
        let is_trigger_idx = column_idx(&cols, "is_trigger")?;
        let tif_idx = column_idx(&cols, "tif")?;
        let trigger_condition_idx = column_idx(&cols, "trigger_condition")?;
        let trigger_px_idx = column_idx(&cols, "trigger_px")?;
        let is_position_tpsl_idx = column_idx(&cols, "is_position_tpsl")?;
        let reduce_only_idx = column_idx(&cols, "reduce_only")?;
        let order_type_idx = column_idx(&cols, "order_type")?;
        let cloid_idx = column_idx(&cols, "cloid")?;
        for row in reader.get_row_iter(None)? {
            let row = row?;
            let block_number = row.get_long(block_number_idx)? as u64;
            if block_number < start_height {
                continue;
            }
            if block_number > end_height {
                break;
            }
            out.push(StatusRow {
                block_number,
                time_ms: parse_time_ms(row.get_string(time_idx)?)?,
                status: row.get_string(status_idx)?.to_string(),
                oid: row.get_long(oid_idx)? as u64,
                side: parse_side(row.get_string(side_idx)?)?,
                limit_px: decimal_to_i64(row.get_decimal(limit_px_idx)?)?,
                timestamp: row.get_long(timestamp_idx)? as u64,
                is_trigger: row.get_bool(is_trigger_idx)?,
                tif: normalized_opt(row.get_string(tif_idx)?),
                trigger_condition: row.get_string(trigger_condition_idx)?.to_string(),
                trigger_px: decimal_to_i64(row.get_decimal(trigger_px_idx)?)?,
                is_position_tpsl: row.get_bool(is_position_tpsl_idx)?,
                reduce_only: row.get_bool(reduce_only_idx)?,
                order_type: row.get_string(order_type_idx)?.to_string(),
                cloid: normalized_opt(row.get_string(cloid_idx)?),
            });
        }
    }
    Ok(out)
}

fn read_diff_rows(files: &[ArchiveFileRange], start_height: u64, end_height: u64) -> AppResult<Vec<DiffRow>> {
    let mut out = Vec::new();
    for file in files {
        let reader = SerializedFileReader::new(File::open(&file.path)?)?;
        let cols = column_indexes(&reader);
        let block_number_idx = column_idx(&cols, "block_number")?;
        let oid_idx = column_idx(&cols, "oid")?;
        let diff_type_idx = column_idx(&cols, "diff_type")?;
        let sz_idx = column_idx(&cols, "sz")?;
        for row in reader.get_row_iter(None)? {
            let row = row?;
            let block_number = row.get_long(block_number_idx)? as u64;
            if block_number < start_height {
                continue;
            }
            if block_number > end_height {
                break;
            }
            let diff = match row.get_int(diff_type_idx)? {
                0 => DiffType::New { sz: decimal_to_i64(row.get_decimal(sz_idx)?)? },
                1 => DiffType::Update { new_sz: decimal_to_i64(row.get_decimal(sz_idx)?)? },
                2 => DiffType::Remove,
                other => return err(format!("unknown diff_type {}", other)),
            };
            out.push(DiffRow { block_number, oid: row.get_long(oid_idx)? as u64, diff });
        }
    }
    Ok(out)
}

fn read_block_stats(
    files: &[ArchiveFileRange],
    start_height: u64,
    end_height: u64,
    coin: &str,
) -> AppResult<BlockStats> {
    let mut seen = Vec::new();
    let mut block_times: HashMap<u64, String> = HashMap::new();
    let mut status_counts: HashMap<u64, i32> = HashMap::new();
    let mut diff_counts: HashMap<u64, i32> = HashMap::new();
    for file in files {
        let reader = SerializedFileReader::new(File::open(&file.path)?)?;
        let cols = column_indexes(&reader);
        let block_number_idx = column_idx(&cols, "block_number")?;
        let block_time_idx = column_idx(&cols, "block_time")?;
        let order_batch_ok_idx = column_idx(&cols, "order_batch_ok")?;
        let diff_batch_ok_idx = column_idx(&cols, "diff_batch_ok")?;
        let fill_batch_ok_idx = column_idx(&cols, "fill_batch_ok")?;
        let btc_status_n_idx = cols.get("btc_status_n").copied();
        let btc_diff_n_idx = cols.get("btc_diff_n").copied();
        let eth_status_n_idx = cols.get("eth_status_n").copied();
        let eth_diff_n_idx = cols.get("eth_diff_n").copied();
        for row in reader.get_row_iter(None)? {
            let row = row?;
            let block_number = row.get_long(block_number_idx)? as u64;
            if block_number < start_height {
                continue;
            }
            if block_number > end_height {
                break;
            }
            let order_ok = row.get_bool(order_batch_ok_idx)?;
            let diff_ok = row.get_bool(diff_batch_ok_idx)?;
            let fill_ok = row.get_bool(fill_batch_ok_idx)?;
            if !(order_ok && diff_ok && fill_ok) {
                return err(format!("batch_ok false at block {}", block_number));
            }
            seen.push(block_number);
            block_times.insert(block_number, row.get_string(block_time_idx)?.to_string());
            match coin {
                "BTC" => {
                    if let (Some(status_idx), Some(diff_idx)) = (btc_status_n_idx, btc_diff_n_idx) {
                        status_counts.insert(block_number, row.get_int(status_idx)?);
                        diff_counts.insert(block_number, row.get_int(diff_idx)?);
                    }
                }
                "ETH" => {
                    if let (Some(status_idx), Some(diff_idx)) = (eth_status_n_idx, eth_diff_n_idx) {
                        status_counts.insert(block_number, row.get_int(status_idx)?);
                        diff_counts.insert(block_number, row.get_int(diff_idx)?);
                    }
                }
                _ => {}
            }
        }
    }
    seen.sort_unstable();
    let expected_rows = end_height - start_height + 1;
    if seen.len() as u64 != expected_rows {
        return err(format!("blocks coverage mismatch: actual={} expected={}", seen.len(), expected_rows));
    }
    for (offset, block_number) in seen.iter().enumerate() {
        let expected = start_height + offset as u64;
        if *block_number != expected {
            return err(format!("missing block {} in blocks parquet", expected));
        }
    }
    if !status_counts.is_empty() || !diff_counts.is_empty() {
        let status_zero = status_counts.values().filter(|count| **count == 0).count();
        let diff_zero = diff_counts.values().filter(|count| **count == 0).count();
        println!("block index stats coin={} status_zero_blocks={} diff_zero_blocks={}", coin, status_zero, diff_zero);
    }
    Ok(BlockStats {
        rows: seen.len() as u64,
        min_block: *seen.first().ok_or("no block rows found")?,
        max_block: *seen.last().ok_or("no block rows found")?,
        expected_rows,
        block_times,
        status_counts,
        diff_counts,
    })
}

fn verify_block_counts(
    coin: &str,
    status_rows: &[StatusRow],
    diff_rows: &[DiffRow],
    block_stats: &BlockStats,
) -> AppResult<()> {
    if block_stats.status_counts.is_empty() && block_stats.diff_counts.is_empty() {
        println!("block index count check skipped coin={}", coin);
        return Ok(());
    }

    let mut actual_status: HashMap<u64, i32> = HashMap::new();
    for row in status_rows {
        *actual_status.entry(row.block_number).or_insert(0) += 1;
    }

    let mut actual_diff: HashMap<u64, i32> = HashMap::new();
    for row in diff_rows {
        *actual_diff.entry(row.block_number).or_insert(0) += 1;
    }

    for (block_number, expected_count) in &block_stats.status_counts {
        let actual_count = actual_status.get(block_number).copied().unwrap_or_default();
        if actual_count != *expected_count {
            return err(format!(
                "status count mismatch coin={} block={} actual={} expected={}",
                coin, block_number, actual_count, expected_count
            ));
        }
    }

    for (block_number, expected_count) in &block_stats.diff_counts {
        let actual_count = actual_diff.get(block_number).copied().unwrap_or_default();
        if actual_count != *expected_count {
            return err(format!(
                "diff count mismatch coin={} block={} actual={} expected={}",
                coin, block_number, actual_count, expected_count
            ));
        }
    }

    println!("block index count check OK coin={}", coin);
    Ok(())
}

fn format_trace(label: &str, oid: u64, traces: &HashMap<u64, OrderTrace>, block_stats: &BlockStats) -> String {
    match traces.get(&oid) {
        Some(trace) => {
            let block_time =
                block_stats.block_times.get(&trace.last_height).cloned().unwrap_or_else(|| "unknown".to_string());
            format!(
                "{}_oid={} {}_last_height={} {}_last_block_time={} {}_last_action={} {}_last_order_type={}",
                label,
                oid,
                label,
                trace.last_height,
                label,
                block_time,
                label,
                trace.last_action,
                label,
                trace.order_type
            )
        }
        None => format!(
            "{}_oid={} {}_last_height=unknown {}_last_block_time=unknown {}_last_action=unknown {}_last_order_type=unknown",
            label, oid, label, label, label, label
        ),
    }
}

fn side_diff_summary(
    side_label: &str,
    actual_side: &[Order],
    expected_side: &[Order],
    traces: &HashMap<u64, OrderTrace>,
    block_stats: &BlockStats,
) -> String {
    let actual_oids: BTreeSet<u64> = actual_side.iter().map(|order| order.oid).collect();
    let expected_oids: BTreeSet<u64> = expected_side.iter().map(|order| order.oid).collect();
    let actual_by_oid: BTreeMap<u64, &Order> = actual_side.iter().map(|order| (order.oid, order)).collect();
    let expected_by_oid: BTreeMap<u64, &Order> = expected_side.iter().map(|order| (order.oid, order)).collect();
    let extra: Vec<u64> = actual_oids.difference(&expected_oids).copied().collect();
    let missing: Vec<u64> = expected_oids.difference(&actual_oids).copied().collect();
    let field_mismatch: Vec<u64> = actual_oids
        .intersection(&expected_oids)
        .filter_map(|oid| {
            let actual_order = actual_by_oid.get(oid)?;
            let expected_order = expected_by_oid.get(oid)?;
            if *actual_order != *expected_order { Some(*oid) } else { None }
        })
        .collect();
    let extra_preview = extra
        .iter()
        .take(3)
        .map(|oid| format_trace("extra", *oid, traces, block_stats))
        .collect::<Vec<_>>()
        .join(" | ");
    let missing_preview = missing
        .iter()
        .take(3)
        .map(|oid| format_trace("missing", *oid, traces, block_stats))
        .collect::<Vec<_>>()
        .join(" | ");
    let field_mismatch_preview = field_mismatch
        .iter()
        .take(3)
        .map(|oid| format_trace("mismatch", *oid, traces, block_stats))
        .collect::<Vec<_>>()
        .join(" | ");
    format!(
        "{}_oid_diff extra_count={} missing_count={} field_mismatch_count={} extra_preview=[{}] missing_preview=[{}] field_mismatch_preview=[{}]",
        side_label,
        extra.len(),
        missing.len(),
        field_mismatch.len(),
        extra_preview,
        missing_preview,
        field_mismatch_preview
    )
}

fn compare_snapshots(
    actual: [Vec<Order>; 2],
    expected: [Vec<Order>; 2],
    traces: &HashMap<u64, OrderTrace>,
    block_stats: &BlockStats,
) -> AppResult<()> {
    for (side_idx, (actual_side, expected_side)) in actual.iter().zip(expected.iter()).enumerate() {
        let side_label = if side_idx == 0 { "bid" } else { "ask" };
        if actual_side.len() != expected_side.len() {
            let side_diff = side_diff_summary(side_label, &actual_side, &expected_side, traces, block_stats);
            return err(format!(
                "side {} length mismatch actual={} expected={}\n{}",
                side_idx,
                actual_side.len(),
                expected_side.len(),
                side_diff
            ));
        }
        for (idx, (actual_order, expected_order)) in actual_side.iter().zip(expected_side.iter()).enumerate() {
            if actual_order != expected_order {
                let actual_trace = format_trace("actual", actual_order.oid, traces, block_stats);
                let expected_trace = format_trace("expected", expected_order.oid, traces, block_stats);
                let side_diff = side_diff_summary(side_label, &actual_side, &expected_side, traces, block_stats);
                return err(format!(
                    "side {} order {} mismatch\nactual={:?}\nexpected={:?}\n{}\n{}\n{}",
                    side_idx, idx, actual_order, expected_order, actual_trace, expected_trace, side_diff
                ));
            }
        }
    }
    Ok(())
}

fn decimal_to_i64(decimal: &Decimal) -> AppResult<i64> {
    let bytes = decimal.data();
    match bytes.len() {
        4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| "invalid 4-byte decimal")?;
            Ok(i32::from_be_bytes(arr) as i64)
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| "invalid 8-byte decimal")?;
            Ok(i64::from_be_bytes(arr))
        }
        width => err(format!("unsupported decimal byte width {}", width)),
    }
}

fn parse_side(s: &str) -> AppResult<Side> {
    match s {
        "A" => Ok(Side::Ask),
        "B" => Ok(Side::Bid),
        other => err(format!("unknown side {}", other)),
    }
}

fn parse_time_ms(s: &str) -> AppResult<u64> {
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")?;
    Ok(dt.and_utc().timestamp_millis() as u64)
}

fn normalized_opt(s: &str) -> Option<String> {
    if s.is_empty() { None } else { Some(s.to_string()) }
}

fn parse_scaled(s: &str) -> AppResult<i64> {
    let s = s.trim();
    if s.is_empty() {
        return err("empty decimal");
    }
    let neg = s.starts_with('-');
    let s = if neg || s.starts_with('+') { &s[1..] } else { s };
    let mut parts = s.split('.');
    let int_part = parts.next().unwrap_or("0");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return err(format!("bad decimal {}", s));
    }
    let mut value = int_part.parse::<i64>()?.saturating_mul(SCALE);
    let mut frac_buf = frac.as_bytes().iter().take(8).copied().collect::<Vec<_>>();
    while frac_buf.len() < 8 {
        frac_buf.push(b'0');
    }
    if !frac_buf.is_empty() {
        value = value.saturating_add(String::from_utf8(frac_buf)?.parse::<i64>()?);
    }
    Ok(if neg { -value } else { value })
}

fn err<T>(msg: impl Into<String>) -> AppResult<T> {
    Err(msg.into().into())
}
