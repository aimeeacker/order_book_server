#![allow(unused_crate_dependencies)]

mod build_btc_diff_replica_day_index;
#[path = "lz4_parallel_common.rs"]
mod lz4_parallel_common;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{File, create_dir_all, rename};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_compression::tokio::bufread::Lz4Decoder;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::RequestPayer;
use chrono::{Datelike, NaiveDate, Timelike};
use clap::Parser;
use env_logger::{Builder, Env};
use log::{info, warn};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rusqlite::{Connection, params};
use rust_decimal::Decimal;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader as TokioBufReader};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};

use lz4_parallel_common::{ParallelLz4LineReader, ParallelLz4ReaderConfig};

use crate::build_btc_diff_replica_day_index::{
    REPLICA_CHUNK_INDEX, REPLICA_INDEX_END_EXCLUSIVE_CHUNK, REPLICA_SNAPSHOT_DIRS,
};

const ROW_GROUP_BLOCKS: u64 = 10_000;
const FILE_ROTATION_BLOCKS: u64 = 1_000_000;
const PARQUET_MAX_ROW_GROUP_ROWS: usize = 10_000_000;
const DECIMAL_PRECISION: u8 = 18;
const HOUR_MS: i64 = 3_600_000;
const LIVE_STATE_TTL_MS: i64 = 24 * HOUR_MS;
const UNKNOWN_OID_WINDOW_BLOCKS: u64 = 10_000;
const UNKNOWN_OID_SAMPLE_LIMIT_PER_WINDOW: usize = 200;
const PRUNE_SAMPLE_LIMIT_PER_RUN: usize = 200;
const REQUESTER_PAYS_ALWAYS: bool = true;
const PROGRESS_LOG_INTERVAL_BLOCKS: u64 = 10_000;
const ENABLE_MISSING_STATUS_WARN: bool = false;
const LZ4_IO_BUFFER_BYTES: usize = 4 << 20;
const MAX_REPLICA_JSON_WORKERS: usize = 8;
const DEFAULT_REPLICA_JSON_WORKERS: usize = 3;
const DEFAULT_REPLICA_LZ4_WORKERS: usize = 2;
const DEFAULT_REPLICA_JSON_QUEUE_DEPTH: usize = 16;
const DEFAULT_REPLICA_S3_RANGE_WORKERS: usize = 3;
const DEFAULT_REPLICA_PREFETCH_BUFFER_MB: usize = 256;
const TARGET_REPLICA_CPU_CORES: usize = 4;
const BLOCK_TIME_TOLERANCE_NS: i64 = 2_000_000;
const BLOCK_TIME_MATCH_TOLERANCE_MS: i64 = 0;
const REPLICA_PREFETCH_BUFFER_PERMIT_BYTES: usize = 1 << 20;
const REPLICA_S3_RANGE_PART_BYTES: usize = 16 << 20;
const WARMUP_STATE_FORMAT_VERSION: u32 = 3;
const WARMUP_STATE_ZSTD_LEVEL: i32 = 3;
const DEFAULT_OUTPUT_DIR: &str = "/home/aimee/hyperliquid";
const DEFAULT_UNKNOWN_OID_SQLITE_FILE: &str = "/home/aimee/hyperliquid/build_btc_diff_unknown_oid.sqlite";
const DEFAULT_COIN_SYMBOL: &str = "BTC";
const DISABLE_UNKNOWN_OID_SQLITE: bool = true;
const UPLOAD_BUCKET: &str = "hyper0";
const UPLOAD_ROOT_PREFIX: &str = "hyperliquid";
const UPLOAD_MAX_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Copy)]
struct CoinConfig {
    symbol: &'static str,
    asset_id: u64,
    px_scale: i8,
    sz_scale: i8,
}

const SUPPORTED_COINS: [CoinConfig; 4] = [
    CoinConfig { symbol: "BTC", asset_id: 0, px_scale: 1, sz_scale: 5 },
    CoinConfig { symbol: "ETH", asset_id: 1, px_scale: 3, sz_scale: 4 },
    CoinConfig { symbol: "SOL", asset_id: 5, px_scale: 3, sz_scale: 2 },
    CoinConfig { symbol: "HYPE", asset_id: 150, px_scale: 4, sz_scale: 2 },
];

fn coin_config_by_symbol(raw: &str) -> Option<CoinConfig> {
    let normalized = raw.trim().to_ascii_uppercase();
    SUPPORTED_COINS.iter().copied().find(|coin| coin.symbol == normalized)
}

fn parse_coin_configs(raw: &str) -> Result<Vec<CoinConfig>> {
    let mut seen_symbols: HashSet<&'static str> = HashSet::new();
    let mut selected = Vec::new();
    for token in raw
        .split(|ch: char| ch == ',' || ch == '+' || ch.is_ascii_whitespace())
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let coin = coin_config_by_symbol(token)
            .ok_or_else(|| anyhow!("unsupported --coin '{}'; supported: BTC, ETH, SOL, HYPE", token))?;
        if seen_symbols.insert(coin.symbol) {
            selected.push(coin);
        }
    }
    if selected.is_empty() {
        bail!("--coin cannot be empty; supported: BTC, ETH, SOL, HYPE");
    }
    Ok(selected)
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
        return Err(format!("invalid --span '{raw}', expected integer or suffix k/m (e.g. 20000, 20k, 2m)"));
    }

    let base = digits.replace('_', "");
    if base.is_empty() {
        return Err(format!("invalid --span '{raw}', expected integer or suffix k/m (e.g. 20000, 20k, 2m)"));
    }

    let parsed = base
        .parse::<u64>()
        .map_err(|_| format!("invalid --span '{raw}', expected integer or suffix k/m (e.g. 20000, 20k, 2m)"))?;

    parsed.checked_mul(multiplier).ok_or_else(|| format!("--span overflow for '{raw}'"))
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, visible_alias = "replica_cmds_lz4", default_value = "s3://hl-mainnet-node-data/replica_cmds")]
    replica_cmds: String,

    #[arg(
        long,
        visible_alias = "node_fills_by_block_lz4",
        default_value = "s3://hl-mainnet-node-data/node_fills_by_block"
    )]
    node_fills_by_block: String,

    #[arg(
        long,
        default_value = DEFAULT_OUTPUT_DIR,
        help = "Output parquet directory (files are named by actual start/end heights)"
    )]
    output_parquet: PathBuf,

    #[arg(long, default_value = DEFAULT_UNKNOWN_OID_SQLITE_FILE)]
    unknown_oid_log_sqlite: PathBuf,

    #[arg(
        long,
        default_value = DEFAULT_COIN_SYMBOL,
        help = "Target coin set (supported: BTC, ETH, SOL, HYPE). Accepts comma or plus separators, e.g. BTC,ETH or BTC+ETH"
    )]
    coin: String,

    #[arg(
        long,
        default_value_t = false,
        help = "Upload generated parquet and warmup msgpack.zst files to s3://hyper0; parquet is deleted locally after upload, latest warmup msgpack.zst is kept for local resume"
    )]
    upload: bool,

    #[arg(long = "start", visible_alias = "start-height")]
    start: Option<u64>,

    #[arg(
        long = "span",
        visible_alias = "height-span",
        value_parser = parse_height_span,
        help = "Block range size; supports suffixes like 20k and 2m"
    )]
    span: Option<u64>,

    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "1280000",
        value_parser = parse_height_span,
        help = "Warmup block count; supports suffixes like 20k and 2m. Omit --warmup to disable warmup, use --warmup without value for default 1280000"
    )]
    warmup: Option<u64>,

    #[arg(
        long,
        default_value = DEFAULT_OUTPUT_DIR,
        help = "Directory to emit warmup state snapshots every 1m block boundary (msgpack + zstd level 3)"
    )]
    warmup_state_output_dir: Option<PathBuf>,

    #[arg(long, help = "Warmup state snapshot file (.msgpack.zst) to restore state and reduce warmup replay")]
    warmup_state_file: Option<PathBuf>,

    #[arg(
        long = "json",
        visible_alias = "replica-json-workers",
        default_value_t = DEFAULT_REPLICA_JSON_WORKERS,
        help = "Replica JSON parse workers (default 3, capped at 8)"
    )]
    replica_json_workers: usize,

    #[arg(
        long = "lz4",
        visible_alias = "replica-lz4-workers",
        default_value_t = DEFAULT_REPLICA_LZ4_WORKERS,
        help = "Replica LZ4 decode workers (default 2)"
    )]
    replica_lz4_workers: usize,

    #[arg(
        long = "s3",
        visible_aliases = ["replica-s3-range-workers", "replica-s3-connections"],
        default_value_t = DEFAULT_REPLICA_S3_RANGE_WORKERS,
        help = "Replica S3 range download workers per active object (default 3)"
    )]
    replica_s3_range_workers: usize,

    #[arg(
        long,
        default_value_t = DEFAULT_REPLICA_PREFETCH_BUFFER_MB,
        help = "Replica compressed prefetch memory buffer in MiB (default 256)"
    )]
    replica_prefetch_buffer_mb: usize,
}

#[derive(Debug, Clone)]
enum InputSource {
    LocalFile(PathBuf),
    S3Prefix { bucket: String, prefix: String },
}

impl InputSource {
    fn parse(raw: &str) -> Result<Self> {
        if let Some(path_without_scheme) = raw.strip_prefix("s3://") {
            let mut parts = path_without_scheme.splitn(2, '/');
            let bucket = parts.next().unwrap_or_default().trim();
            let prefix = parts.next().unwrap_or_default().trim_matches('/');
            if bucket.is_empty() || prefix.is_empty() {
                bail!("invalid S3 URI: {raw}. expected format: s3://bucket/prefix");
            }
            return Ok(Self::S3Prefix { bucket: bucket.to_owned(), prefix: prefix.to_owned() });
        }

        Ok(Self::LocalFile(PathBuf::from(raw)))
    }

    fn requires_s3(&self) -> bool {
        matches!(self, Self::S3Prefix { .. })
    }
}

#[derive(Debug, Clone)]
enum ReaderSource {
    LocalFile(PathBuf),
    S3Keys { bucket: String, keys: Vec<String> },
    S3HourlyFrom { bucket: String, prefix: String, next_hour_ms: i64 },
}

#[derive(Debug, Deserialize)]
struct ReplicaLine {
    abci_block: AbciBlock,
    #[serde(default)]
    resps: Option<ReplicaResponses>,
}

#[derive(Debug, Deserialize)]
struct AbciBlock {
    time: String,
    #[serde(default)]
    signed_action_bundles: Vec<(String, SignedActionPayload)>,
}

#[derive(Debug, Default, Deserialize)]
struct SignedActionPayload {
    #[serde(default)]
    signed_actions: Vec<SignedActionEnvelope>,
}

#[derive(Debug, Default, Deserialize)]
struct SignedActionEnvelope {
    #[serde(default)]
    action: ActionInput,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum JsonU64OrString {
    U64(u64),
    String(String),
}

impl JsonU64OrString {
    fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(value) => Some(*value),
            Self::String(raw) => raw.parse::<u64>().ok(),
        }
    }

    fn to_order_ref(&self) -> OrderRef {
        match self {
            Self::U64(value) => OrderRef::Oid(*value),
            Self::String(raw) => match raw.parse::<u64>() {
                Ok(value) => OrderRef::Oid(value),
                Err(_err) => OrderRef::Cloid(raw.clone()),
            },
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ActionInput {
    #[serde(rename = "type", default)]
    action_type: Option<String>,
    #[serde(default)]
    orders: Vec<OrderInput>,
    #[serde(default)]
    cancels: Vec<CancelInput>,
    #[serde(default)]
    order: Option<OrderInput>,
    #[serde(default)]
    modifies: Vec<ModifyInput>,
    #[serde(default)]
    oid: Option<JsonU64OrString>,
    #[serde(default)]
    cloid: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct OrderInput {
    #[serde(default, alias = "asset")]
    a: Option<JsonU64OrString>,
    #[serde(default)]
    b: Option<bool>,
    #[serde(default, alias = "price")]
    p: Option<String>,
    #[serde(default, alias = "size")]
    s: Option<String>,
    #[serde(default)]
    t: Option<OrderTypeInput>,
    #[serde(default, alias = "cloid")]
    c: Option<String>,
    #[serde(default)]
    r: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct OrderTypeInput {
    #[serde(default)]
    trigger: Option<OrderTriggerInput>,
    #[serde(default)]
    limit: Option<OrderLimitInput>,
}

#[derive(Debug, Default, Deserialize)]
struct OrderTriggerInput {
    #[serde(rename = "triggerPx", default)]
    trigger_px: Option<String>,
    #[serde(default)]
    tpsl: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct OrderLimitInput {
    #[serde(default)]
    tif: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct CancelInput {
    #[serde(default)]
    o: Option<JsonU64OrString>,
    #[serde(default)]
    oid: Option<JsonU64OrString>,
    #[serde(default)]
    cloid: Option<String>,
    #[serde(default, alias = "asset")]
    a: Option<JsonU64OrString>,
}

#[derive(Debug, Default, Deserialize)]
struct ModifyInput {
    #[serde(default)]
    order: Option<OrderInput>,
    #[serde(default)]
    oid: Option<JsonU64OrString>,
    #[serde(default)]
    cloid: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ReplicaResponses {
    #[serde(rename = "Full", default)]
    full: Vec<(String, Vec<ResponseItem>)>,
}

#[derive(Debug, Deserialize)]
struct ResponseItem {
    user: Option<String>,
    res: ResponseResult,
}

#[derive(Debug, Default, Deserialize)]
struct ResponseResult {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    response: ResponseBody,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResponseBody {
    Text(String),
    Object(ResponseObject),
    Other(IgnoredAny),
}

impl Default for ResponseBody {
    fn default() -> Self {
        Self::Other(IgnoredAny)
    }
}

#[derive(Debug, Default, Deserialize)]
struct ResponseObject {
    #[serde(rename = "type", default)]
    response_type: Option<String>,
    #[serde(default)]
    data: Option<ResponseData>,
}

#[derive(Debug, Default, Deserialize)]
struct ResponseData {
    #[serde(default)]
    statuses: Vec<StatusEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StatusEntry {
    Text(String),
    Object(StatusObject),
}

impl StatusEntry {
    fn is_success(&self) -> bool {
        matches!(self, Self::Text(raw) if raw.eq_ignore_ascii_case("success"))
    }

    fn is_pending_trigger(&self) -> bool {
        match self {
            Self::Text(raw) => {
                raw.eq_ignore_ascii_case("waitingForTrigger") || raw.eq_ignore_ascii_case("waitingForFill")
            }
            Self::Object(value) => value.waiting_for_trigger || value.waiting_for_fill,
        }
    }

    fn has_resting(&self) -> bool {
        matches!(self, Self::Object(value) if value.resting.is_some())
    }

    fn resting_oid(&self) -> Option<u64> {
        match self {
            Self::Object(value) => {
                value.resting.as_ref().and_then(|resting| resting.oid.as_ref()).and_then(JsonU64OrString::as_u64)
            }
            Self::Text(_raw) => None,
        }
    }

    fn filled_oid(&self) -> Option<u64> {
        match self {
            Self::Object(value) => {
                value.filled.as_ref().and_then(|filled| filled.oid.as_ref()).and_then(JsonU64OrString::as_u64)
            }
            Self::Text(_raw) => None,
        }
    }

    fn filled_total_sz(&self) -> Option<&str> {
        match self {
            Self::Object(value) => value.filled.as_ref().and_then(|filled| filled.total_sz.as_deref()),
            Self::Text(_raw) => None,
        }
    }

    fn error(&self) -> Option<&str> {
        match self {
            Self::Object(value) => value.error.as_deref(),
            Self::Text(_raw) => None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct StatusObject {
    #[serde(default)]
    resting: Option<StatusOid>,
    #[serde(default)]
    filled: Option<FilledStatus>,
    #[serde(default)]
    error: Option<String>,
    #[serde(rename = "waitingForTrigger", default)]
    waiting_for_trigger: bool,
    #[serde(rename = "waitingForFill", default)]
    waiting_for_fill: bool,
}

#[derive(Debug, Default, Deserialize)]
struct StatusOid {
    #[serde(default)]
    oid: Option<JsonU64OrString>,
}

#[derive(Debug, Default, Deserialize)]
struct FilledStatus {
    #[serde(default)]
    oid: Option<JsonU64OrString>,
    #[serde(rename = "totalSz", default)]
    total_sz: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NodeFillsBlock {
    block_time: String,
    block_number: u64,
    #[serde(default)]
    events: Vec<(String, FillEvent)>,
}

#[derive(Debug, Default, Deserialize)]
struct FillEvent {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    px: String,
    #[serde(default)]
    side: String,
    oid: u64,
    #[serde(default)]
    sz: String,
    #[serde(default)]
    crossed: bool,
    tid: Option<u64>,
}

#[derive(Debug, Clone)]
struct FillRecord {
    asset_id: u64,
    block_number: u64,
    block_time_ms: i64,
    user: String,
    oid: u64,
    side: Option<String>,
    px: Option<Decimal>,
    sz: Decimal,
    crossed: bool,
    tid: Option<u64>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct UserCloidKey {
    user: String,
    cloid: String,
}

#[derive(Debug, Clone)]
enum OrderRef {
    Oid(u64),
    Cloid(String),
}

#[derive(Debug, Serialize)]
struct UnknownOidSample {
    block_number: u64,
    block_time_ms: i64,
    event: &'static str,
    phase: &'static str,
    reason: &'static str,
    user: Option<String>,
    ref_kind: &'static str,
    ref_oid: Option<u64>,
    ref_cloid: Option<String>,
    mapped_oid: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct UnknownOidWindowStats {
    unknown_cancel_total: u64,
    unknown_modify_total: u64,
    unresolved_user_cloid_cancel: u64,
    unresolved_user_cloid_modify: u64,
    sampled: u64,
    sample_dropped: u64,
}

#[derive(Debug, Serialize)]
struct PrunedOrderSample {
    oid: u64,
    user: String,
    cloid: Option<String>,
    created_block_number: u64,
    created_time_ms: i64,
    pruned_at_block_number: u64,
    pruned_at_time_ms: i64,
    age_ms: i64,
}

#[derive(Debug, Serialize)]
struct StalePruneRun {
    trigger_block_number: u64,
    trigger_block_time_ms: i64,
    cutoff_time_ms: i64,
    pruned_total: u64,
    sampled: u64,
    sample_dropped: u64,
    samples: Vec<PrunedOrderSample>,
}

#[derive(Debug)]
struct UnknownOidWindowPayload {
    asset_symbol: String,
    window_start_block: u64,
    stats: UnknownOidWindowStats,
    samples: Vec<UnknownOidSample>,
    stale_prune_runs: Vec<StalePruneRun>,
}

#[derive(Debug)]
enum UnknownOidSqliteTask {
    Flush(UnknownOidWindowPayload),
    Shutdown,
}

#[derive(Clone, Debug)]
struct UnknownOidSqliteSender {
    tx: std_mpsc::Sender<UnknownOidSqliteTask>,
}

#[derive(Debug)]
struct UnknownOidSqliteWorker {
    tx: Option<std_mpsc::Sender<UnknownOidSqliteTask>>,
    handle: Option<std::thread::JoinHandle<Result<()>>>,
}

fn init_unknown_oid_sqlite_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS unknown_oid_windows (
    asset_symbol TEXT NOT NULL,
    window_start_block INTEGER NOT NULL,
    window_end_block INTEGER NOT NULL,
    window_type TEXT NOT NULL,
    unknown_cancel_total INTEGER NOT NULL,
    unknown_modify_total INTEGER NOT NULL,
    unresolved_user_cloid_cancel INTEGER NOT NULL,
    unresolved_user_cloid_modify INTEGER NOT NULL,
    sampled INTEGER NOT NULL,
    sample_dropped INTEGER NOT NULL,
    stale_prune_runs INTEGER NOT NULL,
    PRIMARY KEY (asset_symbol, window_start_block)
);

CREATE TABLE IF NOT EXISTS unknown_oid_samples (
    asset_symbol TEXT NOT NULL,
    window_start_block INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_time_ms INTEGER NOT NULL,
    event TEXT NOT NULL,
    phase TEXT NOT NULL,
    reason TEXT NOT NULL,
    user_addr TEXT,
    ref_kind TEXT NOT NULL,
    ref_oid INTEGER,
    ref_cloid TEXT,
    mapped_oid INTEGER,
    PRIMARY KEY (asset_symbol, window_start_block, seq)
);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_block ON unknown_oid_samples(block_number);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_ref_oid ON unknown_oid_samples(ref_oid);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_mapped_oid ON unknown_oid_samples(mapped_oid);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_user_cloid ON unknown_oid_samples(user_addr, ref_cloid);

CREATE TABLE IF NOT EXISTS stale_prune_runs (
    asset_symbol TEXT NOT NULL,
    window_start_block INTEGER NOT NULL,
    run_seq INTEGER NOT NULL,
    trigger_block_number INTEGER NOT NULL,
    trigger_block_time_ms INTEGER NOT NULL,
    cutoff_time_ms INTEGER NOT NULL,
    pruned_total INTEGER NOT NULL,
    sampled INTEGER NOT NULL,
    sample_dropped INTEGER NOT NULL,
    PRIMARY KEY (asset_symbol, window_start_block, run_seq)
);
CREATE INDEX IF NOT EXISTS idx_stale_prune_runs_trigger_block ON stale_prune_runs(trigger_block_number);

CREATE TABLE IF NOT EXISTS stale_prune_samples (
    asset_symbol TEXT NOT NULL,
    window_start_block INTEGER NOT NULL,
    run_seq INTEGER NOT NULL,
    sample_seq INTEGER NOT NULL,
    oid INTEGER NOT NULL,
    user_addr TEXT NOT NULL,
    cloid TEXT,
    created_block_number INTEGER NOT NULL,
    created_time_ms INTEGER NOT NULL,
    pruned_at_block_number INTEGER NOT NULL,
    pruned_at_time_ms INTEGER NOT NULL,
    age_ms INTEGER NOT NULL,
    PRIMARY KEY (asset_symbol, window_start_block, run_seq, sample_seq)
);
CREATE INDEX IF NOT EXISTS idx_stale_prune_samples_oid ON stale_prune_samples(oid);
CREATE INDEX IF NOT EXISTS idx_stale_prune_samples_created_block ON stale_prune_samples(created_block_number);
"#,
    )
    .context("initializing unknown oid sqlite schema")?;
    conn.execute_batch(
        r#"
CREATE INDEX IF NOT EXISTS idx_unknown_oid_windows_asset_start ON unknown_oid_windows(asset_symbol, window_start_block);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_asset_window_seq ON unknown_oid_samples(asset_symbol, window_start_block, seq);
CREATE INDEX IF NOT EXISTS idx_stale_prune_runs_asset_window_seq ON stale_prune_runs(asset_symbol, window_start_block, run_seq);
CREATE INDEX IF NOT EXISTS idx_stale_prune_samples_asset_window_run_seq ON stale_prune_samples(asset_symbol, window_start_block, run_seq, sample_seq);
"#,
    )
    .context("initializing unknown oid sqlite asset indexes")?;
    Ok(())
}

fn persist_unknown_oid_window_payload(conn: &mut Connection, payload: UnknownOidWindowPayload) -> Result<()> {
    let window_start_block = payload.window_start_block;
    let window_end_block = window_start_block + UNKNOWN_OID_WINDOW_BLOCKS - 1;

    let tx = conn.transaction().context("begin unknown oid sqlite txn")?;
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO unknown_oid_windows (
            asset_symbol, window_start_block, window_end_block, window_type,
            unknown_cancel_total, unknown_modify_total,
            unresolved_user_cloid_cancel, unresolved_user_cloid_modify,
            sampled, sample_dropped, stale_prune_runs
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            payload.asset_symbol.as_str(),
            i64::try_from(window_start_block)?,
            i64::try_from(window_end_block)?,
            "unknown_oid_window_v4_sqlite_asset",
            i64::try_from(payload.stats.unknown_cancel_total)?,
            i64::try_from(payload.stats.unknown_modify_total)?,
            i64::try_from(payload.stats.unresolved_user_cloid_cancel)?,
            i64::try_from(payload.stats.unresolved_user_cloid_modify)?,
            i64::try_from(payload.stats.sampled)?,
            i64::try_from(payload.stats.sample_dropped)?,
            i64::try_from(payload.stale_prune_runs.len())?,
        ],
    )?;

    if inserted == 0 {
        warn!(
            "[warn] unknown-oid sqlite conflict at window_start_block={} (already exists); skipping this window",
            window_start_block
        );
        tx.commit()?;
        return Ok(());
    }

    for (seq, sample) in payload.samples.iter().enumerate() {
        tx.execute(
            "INSERT OR IGNORE INTO unknown_oid_samples (
                asset_symbol, window_start_block, seq, block_number, block_time_ms,
                event, phase, reason, user_addr, ref_kind, ref_oid, ref_cloid, mapped_oid
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                payload.asset_symbol.as_str(),
                i64::try_from(window_start_block)?,
                i64::try_from(seq)?,
                i64::try_from(sample.block_number)?,
                sample.block_time_ms,
                sample.event,
                sample.phase,
                sample.reason,
                sample.user.as_deref(),
                sample.ref_kind,
                sample.ref_oid.map(i64::try_from).transpose()?,
                sample.ref_cloid.as_deref(),
                sample.mapped_oid.map(i64::try_from).transpose()?,
            ],
        )?;
    }

    for (run_seq, run) in payload.stale_prune_runs.iter().enumerate() {
        tx.execute(
            "INSERT OR IGNORE INTO stale_prune_runs (
                asset_symbol, window_start_block, run_seq, trigger_block_number, trigger_block_time_ms,
                cutoff_time_ms, pruned_total, sampled, sample_dropped
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                payload.asset_symbol.as_str(),
                i64::try_from(window_start_block)?,
                i64::try_from(run_seq)?,
                i64::try_from(run.trigger_block_number)?,
                run.trigger_block_time_ms,
                run.cutoff_time_ms,
                i64::try_from(run.pruned_total)?,
                i64::try_from(run.sampled)?,
                i64::try_from(run.sample_dropped)?,
            ],
        )?;

        for (sample_seq, sample) in run.samples.iter().enumerate() {
            tx.execute(
                "INSERT OR IGNORE INTO stale_prune_samples (
                    asset_symbol, window_start_block, run_seq, sample_seq, oid, user_addr, cloid,
                    created_block_number, created_time_ms, pruned_at_block_number, pruned_at_time_ms, age_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    payload.asset_symbol.as_str(),
                    i64::try_from(window_start_block)?,
                    i64::try_from(run_seq)?,
                    i64::try_from(sample_seq)?,
                    i64::try_from(sample.oid)?,
                    sample.user.as_str(),
                    sample.cloid.as_deref(),
                    i64::try_from(sample.created_block_number)?,
                    sample.created_time_ms,
                    i64::try_from(sample.pruned_at_block_number)?,
                    sample.pruned_at_time_ms,
                    sample.age_ms,
                ],
            )?;
        }
    }

    tx.commit().context("commit unknown oid sqlite txn")?;
    Ok(())
}

impl UnknownOidSqliteWorker {
    fn start(path: &Path) -> Result<Self> {
        let (tx, rx) = std_mpsc::channel::<UnknownOidSqliteTask>();
        let sqlite_path = path.to_path_buf();
        let handle = std::thread::spawn(move || -> Result<()> {
            let mut conn = Connection::open(&sqlite_path).with_context(|| {
                format!("failed to open unknown oid sqlite for read/write: {}", sqlite_path.display())
            })?;
            init_unknown_oid_sqlite_schema(&conn)?;
            while let Ok(task) = rx.recv() {
                match task {
                    UnknownOidSqliteTask::Flush(payload) => persist_unknown_oid_window_payload(&mut conn, payload)?,
                    UnknownOidSqliteTask::Shutdown => break,
                }
            }
            Ok(())
        });
        Ok(Self { tx: Some(tx), handle: Some(handle) })
    }

    fn sender(&self) -> UnknownOidSqliteSender {
        UnknownOidSqliteSender {
            tx: self.tx.as_ref().expect("sqlite worker sender should exist while worker is alive").clone(),
        }
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(tx) = self.tx.take() {
            drop(tx.send(UnknownOidSqliteTask::Shutdown));
        }
        if let Some(handle) = self.handle.take() {
            match handle.join() {
                Ok(result) => result?,
                Err(_) => bail!("unknown oid sqlite worker thread panicked"),
            }
        }
        Ok(())
    }
}

impl Drop for UnknownOidSqliteWorker {
    fn drop(&mut self) {
        if let Err(err) = self.finish() {
            warn!("[warn] failed to finish unknown oid sqlite worker cleanly: {}", err);
        }
    }
}

#[derive(Debug)]
struct UnknownOidWindowLogger {
    sender: Option<UnknownOidSqliteSender>,
    asset_symbol: String,
    enabled: bool,
    current_window_start: Option<u64>,
    stats: UnknownOidWindowStats,
    samples: Vec<UnknownOidSample>,
    stale_prune_runs: Vec<StalePruneRun>,
}

impl UnknownOidWindowLogger {
    fn new(asset_symbol: &str, sender: Option<UnknownOidSqliteSender>) -> Self {
        Self {
            sender,
            asset_symbol: asset_symbol.to_owned(),
            enabled: true,
            current_window_start: None,
            stats: UnknownOidWindowStats::default(),
            samples: Vec::new(),
            stale_prune_runs: Vec::new(),
        }
    }

    fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    fn observe_block(&mut self, block_number: u64) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let window_start = window_start_block(block_number);
        match self.current_window_start {
            None => {
                self.current_window_start = Some(window_start);
            }
            Some(current) if current != window_start => {
                self.flush_current()?;
                self.current_window_start = Some(window_start);
            }
            _ => {}
        }
        Ok(())
    }

    fn record_unresolved_user_cloid(&mut self, event: &'static str) {
        if !self.enabled {
            return;
        }
        match event {
            "cancel" => {
                self.stats.unknown_cancel_total += 1;
                self.stats.unresolved_user_cloid_cancel += 1;
            }
            "modify" => {
                self.stats.unknown_modify_total += 1;
                self.stats.unresolved_user_cloid_modify += 1;
            }
            _ => {}
        }
    }

    fn record_sample(&mut self, sample: UnknownOidSample) {
        if !self.enabled {
            return;
        }
        match sample.event {
            "cancel" => self.stats.unknown_cancel_total += 1,
            "modify" => self.stats.unknown_modify_total += 1,
            _ => {}
        }
        if self.samples.len() < UNKNOWN_OID_SAMPLE_LIMIT_PER_WINDOW {
            self.samples.push(sample);
            self.stats.sampled += 1;
        } else {
            self.stats.sample_dropped += 1;
        }
    }

    fn record_stale_prune_run(&mut self, run: StalePruneRun) {
        if !self.enabled {
            return;
        }
        self.stale_prune_runs.push(run);
    }

    fn flush_current(&mut self) -> Result<()> {
        let Some(window_start_block) = self.current_window_start else {
            return Ok(());
        };
        let Some(sender) = self.sender.as_ref() else {
            self.stats = UnknownOidWindowStats::default();
            self.samples.clear();
            self.stale_prune_runs.clear();
            self.current_window_start = None;
            return Ok(());
        };
        let stats = std::mem::take(&mut self.stats);
        let samples = std::mem::take(&mut self.samples);
        let stale_prune_runs = std::mem::take(&mut self.stale_prune_runs);
        sender
            .tx
            .send(UnknownOidSqliteTask::Flush(UnknownOidWindowPayload {
                asset_symbol: self.asset_symbol.clone(),
                window_start_block,
                stats,
                samples,
                stale_prune_runs,
            }))
            .map_err(|_| anyhow!("unknown oid sqlite worker stopped unexpectedly"))?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.flush_current()
    }
}

#[derive(Debug, Clone)]
struct OrderMeta {
    tif: Option<String>,
    reduce_only: Option<bool>,
    is_trigger: bool,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
}

#[derive(Debug)]
struct ReplicaBatch {
    block_time_ns: i64,
    block_time_ms: i64,
    events: Vec<CmdEvent>,
}

#[derive(Debug)]
struct NodeFillsBatch {
    block_number: u64,
    block_time_ns: i64,
    block_time_ms: i64,
    fills: Vec<FillRecord>,
}

#[derive(Debug)]
enum CmdEvent {
    Add {
        asset_id: u64,
        user: String,
        oid: u64,
        cloid: Option<String>,
        side: String,
        px: Decimal,
        sz: Decimal,
        raw_sz: Option<Decimal>,
        meta: OrderMeta,
    },
    Cancel {
        asset_id: u64,
        user: Option<String>,
        order_ref: OrderRef,
        fallback_on_missing: bool,
    },
    Modify {
        asset_id: u64,
        user: String,
        old_order_ref: OrderRef,
        new_oid: Option<u64>,
        new_cloid: Option<String>,
        side: String,
        px: Decimal,
        sz: Decimal,
        raw_sz: Option<Decimal>,
        meta: OrderMeta,
    },
    TrackTriggerAdd {
        asset_id: u64,
        user: String,
        oid: u64,
        cloid: Option<String>,
    },
    TrackTriggerPending {
        asset_id: u64,
        user: String,
        cloid: Option<String>,
    },
    TrackTriggerModify {
        asset_id: u64,
        user: String,
        old_order_ref: OrderRef,
        new_oid: Option<u64>,
        new_cloid: Option<String>,
    },
    Skipped {
        asset_id: u64,
        user: Option<String>,
        oid: Option<u64>,
        event: String,
    },
}

impl CmdEvent {
    fn asset_id(&self) -> u64 {
        match self {
            CmdEvent::Add { asset_id, .. }
            | CmdEvent::Cancel { asset_id, .. }
            | CmdEvent::Modify { asset_id, .. }
            | CmdEvent::TrackTriggerAdd { asset_id, .. }
            | CmdEvent::TrackTriggerPending { asset_id, .. }
            | CmdEvent::TrackTriggerModify { asset_id, .. }
            | CmdEvent::Skipped { asset_id, .. } => *asset_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    user: String,
    cloid: Option<String>,
    created_block_number: u64,
    created_time_ms: i64,
    side: String,
    px: Decimal,
    remaining_sz: Decimal,
    tif: Option<String>,
    reduce_only: Option<bool>,
    is_trigger: bool,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct TriggerOrderRefs {
    oid_to_cloid: HashMap<u64, Option<UserCloidKey>>,
    cloid_to_oid: HashMap<String, HashMap<String, u64>>,
    pending_by_user: HashMap<String, VecDeque<Option<String>>>,
    #[serde(default)]
    oid_created_time_ms: HashMap<u64, i64>,
    #[serde(default)]
    #[serde(skip_serializing, skip_deserializing)]
    oid_created_queue: VecDeque<(i64, u64)>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct WarmupAssetSnapshot {
    live_orders: HashMap<u64, OrderState>,
    live_oid_by_user_cloid: HashMap<String, HashMap<String, u64>>,
    trigger_refs: TriggerOrderRefs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WarmupStateSnapshot {
    format_version: u32,
    checkpoint_block_number: u64,
    checkpoint_block_time_ms: i64,
    assets: HashMap<String, WarmupAssetSnapshot>,
}

#[derive(Debug)]
struct OutputRow {
    block_number: i64,
    block_time_ms: i64,
    coin: String,
    user: String,
    oid: Option<i64>,
    side: Option<String>,
    px: Option<Decimal>,
    event: String,
    sz: Option<Decimal>,
    orig_sz: Option<Decimal>,
    raw_sz: Option<Decimal>,
    fill_sz: Option<Decimal>,
    tif: Option<String>,
    reduce_only: Option<bool>,
    lifetime: Option<i32>,
}

#[derive(Default)]
struct RowBuffer {
    rows: Vec<OutputRow>,
}

impl RowBuffer {
    fn push(&mut self, row: OutputRow) {
        self.rows.push(row);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn take_batch(&mut self, schema: &Arc<Schema>, px_scale: i8, sz_scale: i8) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let mut block_number = Int64Builder::with_capacity(self.rows.len());
        let mut block_time = TimestampMillisecondBuilder::with_capacity(self.rows.len())
            .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())));
        let mut coin = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 8);
        let mut user = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 64);
        let mut oid = Int64Builder::with_capacity(self.rows.len());
        let mut side = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 4);
        let mut px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, px_scale));
        let mut event = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, sz_scale));
        let mut orig_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, sz_scale));
        let mut raw_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, sz_scale));
        let mut fill_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, sz_scale));
        let mut tif = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 8);
        let mut reduce_only = BooleanBuilder::with_capacity(self.rows.len());
        let mut lifetime = Int32Builder::with_capacity(self.rows.len());

        for row in self.rows.drain(..) {
            block_number.append_value(row.block_number);
            block_time.append_value(row.block_time_ms);
            coin.append_value(&row.coin);
            user.append_value(&row.user);
            match row.oid {
                Some(value) => oid.append_value(value),
                None => oid.append_null(),
            }
            match row.side {
                Some(value) => side.append_value(value),
                None => side.append_null(),
            }
            match row.px {
                Some(value) => px.append_value(decimal_to_i128(value, px_scale)),
                None => px.append_null(),
            }
            event.append_value(&row.event);
            match row.sz {
                Some(value) => sz.append_value(decimal_to_i128(value, sz_scale)),
                None => sz.append_null(),
            }
            match row.orig_sz {
                Some(value) => orig_sz.append_value(decimal_to_i128(value, sz_scale)),
                None => orig_sz.append_null(),
            }
            match row.raw_sz {
                Some(value) => raw_sz.append_value(decimal_to_i128(value, sz_scale)),
                None => raw_sz.append_null(),
            }
            match row.fill_sz {
                Some(value) => fill_sz.append_value(decimal_to_i128(value, sz_scale)),
                None => fill_sz.append_null(),
            }
            match row.tif {
                Some(value) => tif.append_value(value),
                None => tif.append_null(),
            }
            match row.reduce_only {
                Some(value) => reduce_only.append_value(value),
                None => reduce_only.append_null(),
            }
            match row.lifetime {
                Some(value) => lifetime.append_value(value),
                None => lifetime.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(block_number.finish()),
            Arc::new(block_time.finish()),
            Arc::new(coin.finish()),
            Arc::new(user.finish()),
            Arc::new(oid.finish()),
            Arc::new(side.finish()),
            Arc::new(px.finish()),
            Arc::new(event.finish()),
            Arc::new(sz.finish()),
            Arc::new(orig_sz.finish()),
            Arc::new(raw_sz.finish()),
            Arc::new(fill_sz.finish()),
            Arc::new(tif.finish()),
            Arc::new(reduce_only.finish()),
            Arc::new(lifetime.finish()),
        ];

        Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
    }
}

fn parse_timestamp_ms(raw: &str) -> Result<i64> {
    let (_ts_ns, ts_ms) = parse_timestamp_ns_ms(raw)?;
    Ok(ts_ms)
}

fn parse_fixed_digits_u32(bytes: &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
    }
    Some(value)
}

fn parse_timestamp_ns_ms(raw: &str) -> Result<(i64, i64)> {
    const BASE_LEN: usize = 19;
    const NS_SCALE: [i64; 10] = [1_000_000_000, 100_000_000, 10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];

    let bytes = raw.as_bytes();
    if bytes.len() < BASE_LEN
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        bail!("failed to parse timestamp: {raw}");
    }

    let year =
        i32::try_from(parse_fixed_digits_u32(&bytes[0..4]).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?)
            .map_err(|_| anyhow!("failed to parse timestamp: {raw}"))?;
    let month = parse_fixed_digits_u32(&bytes[5..7]).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
    let day = parse_fixed_digits_u32(&bytes[8..10]).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
    let hour = parse_fixed_digits_u32(&bytes[11..13]).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
    let minute = parse_fixed_digits_u32(&bytes[14..16]).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
    let second = parse_fixed_digits_u32(&bytes[17..19]).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;

    let mut idx = BASE_LEN;
    let mut frac_ns = 0i64;
    if idx < bytes.len() {
        if bytes[idx] == b'.' {
            idx += 1;
            let frac_start = idx;
            while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                idx += 1;
            }
            let frac_digits = idx.saturating_sub(frac_start);
            if frac_digits == 0 {
                bail!("failed to parse timestamp: {raw}");
            }

            let kept_digits = frac_digits.min(9);
            let frac_value = parse_fixed_digits_u32(&bytes[frac_start..(frac_start + kept_digits)])
                .ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
            frac_ns = i64::from(frac_value)
                .checked_mul(NS_SCALE[kept_digits])
                .ok_or_else(|| anyhow!("timestamp overflow for {raw}"))?;
        }

        if idx < bytes.len() {
            if bytes[idx] == b'Z' && idx + 1 == bytes.len() {
                idx += 1;
            } else {
                bail!("failed to parse timestamp: {raw}");
            }
        }
    }

    if idx != bytes.len() {
        bail!("failed to parse timestamp: {raw}");
    }

    let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
    let dt = date.and_hms_opt(hour, minute, second).ok_or_else(|| anyhow!("failed to parse timestamp: {raw}"))?;
    let ts_sec = dt.and_utc().timestamp();
    let ts_ns = ts_sec
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(frac_ns))
        .ok_or_else(|| anyhow!("timestamp overflow for {raw}"))?;
    let ts_ms = ts_ns.div_euclid(1_000_000);
    Ok((ts_ns, ts_ms))
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    raw.parse::<Decimal>().with_context(|| format!("failed to parse decimal: {raw}"))
}

fn parse_order_ref(oid: Option<&JsonU64OrString>, cloid: Option<&str>) -> Option<OrderRef> {
    oid.map(JsonU64OrString::to_order_ref).or_else(|| cloid.map(|value| OrderRef::Cloid(value.to_owned())))
}

fn parse_cancel_order_ref(cancel: &CancelInput) -> Option<OrderRef> {
    cancel
        .o
        .as_ref()
        .map(JsonU64OrString::to_order_ref)
        .or_else(|| cancel.oid.as_ref().map(JsonU64OrString::to_order_ref))
        .or_else(|| cancel.cloid.as_deref().map(|value| OrderRef::Cloid(value.to_owned())))
}

fn parse_order_fields(order: &OrderInput) -> Option<(u64, String, Decimal, Decimal, Option<String>, OrderMeta)> {
    let asset = order.a.as_ref().and_then(JsonU64OrString::as_u64)?;
    let is_bid = order.b?;
    let side = if is_bid { "B" } else { "A" }.to_owned();
    let px = parse_decimal(order.p.as_deref()?).ok()?;
    let sz = parse_decimal(order.s.as_deref()?).ok()?;
    let trigger = order.t.as_ref().and_then(|value| value.trigger.as_ref());
    let cloid = order.c.clone();
    let reduce_only = order.r;

    let meta = if let Some(trigger) = trigger {
        let trigger_px = parse_decimal(trigger.trigger_px.as_deref()?).ok()?;
        let tpsl = trigger.tpsl.as_deref();
        OrderMeta {
            tif: None,
            reduce_only,
            is_trigger: true,
            trigger_condition: None,
            trigger_px: Some(trigger_px),
            is_position_tpsl: None,
            tp_trigger_px: if tpsl == Some("tp") { Some(trigger_px) } else { None },
            sl_trigger_px: if tpsl == Some("sl") { Some(trigger_px) } else { None },
        }
    } else {
        let tif = order.t.as_ref().and_then(|value| value.limit.as_ref()).and_then(|value| value.tif.clone());
        OrderMeta {
            tif,
            reduce_only,
            is_trigger: false,
            trigger_condition: None,
            trigger_px: None,
            is_position_tpsl: None,
            tp_trigger_px: None,
            sl_trigger_px: None,
        }
    };

    Some((asset, side, px, sz, cloid, meta))
}

fn parse_statuses(response: &ResponseBody) -> &[StatusEntry] {
    match response {
        ResponseBody::Object(value) => value.data.as_ref().map(|data| data.statuses.as_slice()).unwrap_or(&[]),
        ResponseBody::Text(_) | ResponseBody::Other(_) => &[],
    }
}

fn response_type(response: &ResponseBody) -> Option<&str> {
    match response {
        ResponseBody::Object(value) => value.response_type.as_deref(),
        ResponseBody::Text(_) | ResponseBody::Other(_) => None,
    }
}

fn is_dead_order_error(raw: &str) -> bool {
    raw.contains("Cannot modify canceled or filled order")
}

fn is_dead_order_modify_response(result: &ResponseResult) -> bool {
    if result.status.as_deref().is_some_and(is_dead_order_error) {
        return true;
    }

    if let ResponseBody::Text(raw) = &result.response {
        return is_dead_order_error(raw);
    }

    parse_statuses(&result.response).iter().any(|status| status.error().is_some_and(is_dead_order_error))
}

fn is_success_status(status: Option<&StatusEntry>) -> bool {
    status.is_some_and(StatusEntry::is_success)
}

fn is_pending_trigger_status(status: &StatusEntry) -> bool {
    status.is_pending_trigger()
}

enum BookedSzDecision {
    Booked { booked_sz: Decimal, raw_sz: Option<Decimal> },
    FullyFilled,
    InvalidFilledTotalSz,
}

fn parse_booked_and_raw_sz(order_sz: Decimal, status: &StatusEntry) -> BookedSzDecision {
    let Some(raw_total_sz) = status.filled_total_sz() else {
        return BookedSzDecision::Booked { booked_sz: order_sz, raw_sz: None };
    };

    let Ok(filled_sz) = parse_decimal(raw_total_sz) else {
        return BookedSzDecision::InvalidFilledTotalSz;
    };

    let mut booked_sz = order_sz - filled_sz;
    if booked_sz < Decimal::ZERO {
        booked_sz = Decimal::ZERO;
    }
    if booked_sz <= Decimal::ZERO {
        return BookedSzDecision::FullyFilled;
    }

    let raw_sz = if booked_sz == order_sz { None } else { Some(order_sz) };
    BookedSzDecision::Booked { booked_sz, raw_sz }
}

fn trim_newline(line: &mut Vec<u8>) {
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
}

fn format_json_line_debug(line: &[u8]) -> String {
    const PREVIEW_LEN: usize = 64;

    let preview = &line[..line.len().min(PREVIEW_LEN)];
    let hex_preview = preview
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let text_preview = String::from_utf8_lossy(preview);
    let first_non_ws = line.iter().copied().find(|byte| !byte.is_ascii_whitespace());
    let first_non_ws_text = first_non_ws
        .map(|byte| format!("0x{byte:02x}"))
        .unwrap_or_else(|| "none".to_owned());

    format!(
        "len={} first_non_ws={} hex_prefix=[{}] text_prefix={:?}",
        line.len(),
        first_non_ws_text,
        hex_preview,
        text_preview
    )
}

fn parse_json_line<T>(line: &mut Vec<u8>) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    trim_newline(line);
    simd_json::serde::from_slice(line)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to parse json line with simd-json ({})", format_json_line_debug(line)))
}

fn floor_hour_ms(ts_ms: i64) -> i64 {
    ts_ms - ts_ms.rem_euclid(HOUR_MS)
}

async fn list_lz4_keys(client: &S3Client, bucket: &str, prefix: &str, requester_pays: bool) -> Result<Vec<String>> {
    let mut next_token: Option<String> = None;
    let mut keys = Vec::new();

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }
        if let Some(token) = &next_token {
            request = request.continuation_token(token);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                if let Some(service_error) = err.as_service_error()
                    && !requester_pays
                    && service_error.code().is_some_and(|code| code == "AccessDenied")
                {
                    bail!("failed to list s3://{bucket}/{prefix}: AccessDenied");
                }
                bail!("failed to list s3://{bucket}/{prefix}: {err}");
            }
        };

        for object in response.contents() {
            if let Some(key) = object.key()
                && key.ends_with(".lz4")
            {
                keys.push(key.to_owned());
            }
        }

        if response.is_truncated().unwrap_or(false) {
            next_token = response.next_continuation_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    keys.sort_unstable();
    Ok(keys)
}

async fn list_child_prefixes(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    requester_pays: bool,
) -> Result<Vec<String>> {
    let normalized_prefix = prefix.trim_matches('/');
    let list_prefix = format!("{normalized_prefix}/");

    let mut next_token: Option<String> = None;
    let mut prefixes = Vec::new();

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(&list_prefix).delimiter("/");
        if requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }
        if let Some(token) = &next_token {
            request = request.continuation_token(token);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                if let Some(service_error) = err.as_service_error()
                    && !requester_pays
                    && service_error.code().is_some_and(|code| code == "AccessDenied")
                {
                    bail!("failed to list child prefixes in s3://{bucket}/{normalized_prefix}: AccessDenied");
                }
                bail!("failed to list child prefixes in s3://{bucket}/{normalized_prefix}: {err}");
            }
        };

        for common_prefix in response.common_prefixes() {
            if let Some(child) = common_prefix.prefix() {
                let trimmed = child.trim_end_matches('/');
                if !trimmed.is_empty() {
                    prefixes.push(trimmed.to_owned());
                }
            }
        }

        if response.is_truncated().unwrap_or(false) {
            next_token = response.next_continuation_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    prefixes.sort_unstable();
    prefixes.dedup();
    Ok(prefixes)
}

async fn head_object_exists(client: &S3Client, bucket: &str, key: &str, requester_pays: bool) -> Result<bool> {
    let mut request = client.head_object().bucket(bucket).key(key);
    if requester_pays {
        request = request.request_payer(RequestPayer::Requester);
    }

    match request.send().await {
        Ok(_) => Ok(true),
        Err(err) => {
            if let Some(service_error) = err.as_service_error() {
                let not_found = service_error.is_not_found()
                    || service_error.code().is_some_and(|code| matches!(code, "NotFound" | "NoSuchKey" | "404"));
                if not_found {
                    return Ok(false);
                }
                if !requester_pays && service_error.code().is_some_and(|code| code == "AccessDenied") {
                    bail!("failed to head s3://{bucket}/{key}: AccessDenied");
                }
            }

            bail!("failed to head s3://{bucket}/{key}: {err}")
        }
    }
}

fn resolve_required_chunks_from_keys(required_chunks: &[u64], keys: Vec<String>) -> Option<Vec<String>> {
    let mut basename_to_key: HashMap<String, String> = HashMap::new();
    for key in keys {
        let Some(name) = key.rsplit('/').next() else {
            continue;
        };
        match basename_to_key.get(name) {
            Some(existing) if existing >= &key => {}
            _ => {
                basename_to_key.insert(name.to_owned(), key);
            }
        }
    }

    let mut resolved = Vec::with_capacity(required_chunks.len());
    for chunk in required_chunks {
        let file_name = format!("{chunk}.lz4");
        let full_key = basename_to_key.get(&file_name)?.clone();
        resolved.push(full_key);
    }
    Some(resolved)
}

async fn resolve_required_chunks_in_prefix(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    required_chunks: &[u64],
    requester_pays: bool,
) -> Result<Option<Vec<String>>> {
    let keys = list_lz4_keys(client, bucket, prefix, requester_pays).await?;
    Ok(resolve_required_chunks_from_keys(required_chunks, keys))
}

fn replica_chunk_starts_for_range(start_height: u64, height_span: u64) -> Result<Vec<u64>> {
    if start_height == 0 {
        bail!("--start must be >= 1 for replica_cmds naming rule <height-1>.lz4");
    }
    if height_span == 0 {
        bail!("--span must be > 0");
    }

    let end_height = start_height
        .checked_add(height_span - 1)
        .ok_or_else(|| anyhow!("height overflow: start_height={start_height} span={height_span}"))?;

    let start_chunk = ((start_height - 1) / 10_000) * 10_000;
    let end_chunk = ((end_height - 1) / 10_000) * 10_000;

    let mut chunk = start_chunk;
    let mut chunks = Vec::new();
    while chunk <= end_chunk {
        chunks.push(chunk);
        chunk = chunk.checked_add(10_000).ok_or_else(|| anyhow!("replica chunk overflow while covering range"))?;
    }
    Ok(chunks)
}

fn resolve_replica_keys_via_hardcoded_chunk_index(
    normalized_prefix: &str,
    required_chunks: &[u64],
) -> Result<Option<Vec<String>>> {
    if normalized_prefix != "replica_cmds" || required_chunks.is_empty() {
        return Ok(None);
    }

    let Some(first_entry) = REPLICA_CHUNK_INDEX.first() else {
        return Ok(None);
    };
    let first_chunk = first_entry.start_chunk;

    let mut resolved = Vec::with_capacity(required_chunks.len());
    for &chunk in required_chunks {
        if chunk < first_chunk || chunk >= REPLICA_INDEX_END_EXCLUSIVE_CHUNK {
            return Ok(None);
        }

        let pos = REPLICA_CHUNK_INDEX.partition_point(|entry| entry.start_chunk <= chunk);
        if pos == 0 {
            return Ok(None);
        }
        let segment_entry = REPLICA_CHUNK_INDEX[pos - 1];
        let snapshot = REPLICA_SNAPSHOT_DIRS.get(usize::from(segment_entry.snapshot_idx)).ok_or_else(|| {
            anyhow!("invalid snapshot idx in hardcoded replica index: {}", segment_entry.snapshot_idx)
        })?;
        let date_str = format!("{:08}", segment_entry.date_yyyymmdd);
        resolved.push(format!("{normalized_prefix}/{snapshot}/{date_str}/{chunk}.lz4"));
    }

    Ok(Some(resolved))
}

async fn resolve_replica_keys_for_range(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    start_height: u64,
    height_span: u64,
    requester_pays: bool,
) -> Result<Vec<String>> {
    let required_chunks = replica_chunk_starts_for_range(start_height, height_span)?;
    let normalized_prefix = prefix.trim_matches('/');

    if let Some(resolved) = resolve_replica_keys_via_hardcoded_chunk_index(normalized_prefix, &required_chunks)? {
        info!(
            "[index] resolved replica_cmds via hardcoded chunk index for {} chunks (range {}..{})",
            resolved.len(),
            start_height,
            start_height + height_span - 1
        );
        return Ok(resolved);
    }

    // Fast path: try deterministic keys directly under the provided prefix.
    let mut direct_keys = Vec::with_capacity(required_chunks.len());
    let mut all_direct_present = true;
    for chunk in &required_chunks {
        let key = format!("{normalized_prefix}/{chunk}.lz4");
        if head_object_exists(client, bucket, &key, requester_pays).await? {
            direct_keys.push(key);
        } else {
            all_direct_present = false;
            break;
        }
    }
    if all_direct_present {
        return Ok(direct_keys);
    }

    // Structured path optimization:
    // `replica_cmds/<snapshot>/<yyyymmdd>/<chunk>.lz4`
    // Scan snapshots from newest to oldest and only recurse inside one snapshot at a time.
    let child_prefixes = list_child_prefixes(client, bucket, normalized_prefix, requester_pays).await?;
    for child_prefix in child_prefixes.into_iter().rev() {
        if let Some(resolved) =
            resolve_required_chunks_in_prefix(client, bucket, &child_prefix, &required_chunks, requester_pays).await?
        {
            return Ok(resolved);
        }
    }

    // Fallback: recurse under the original prefix for compatibility with custom layouts.
    if let Some(resolved) =
        resolve_required_chunks_in_prefix(client, bucket, normalized_prefix, &required_chunks, requester_pays).await?
    {
        return Ok(resolved);
    }

    bail!(
        "missing replica keys for range start_height={} span={} under s3://{}/{}",
        start_height,
        height_span,
        bucket,
        normalized_prefix
    )
}

fn build_node_fills_hourly_key(prefix: &str, hour_start_ms: i64) -> Result<String> {
    let dt = chrono::DateTime::from_timestamp_millis(hour_start_ms)
        .ok_or_else(|| anyhow!("invalid hour timestamp millis: {hour_start_ms}"))?;
    Ok(format!("{prefix}/hourly/{:04}{:02}{:02}/{}.lz4", dt.year(), dt.month(), dt.day(), dt.hour()))
}

async fn first_replica_timestamp_ms(
    source: ReaderSource,
    s3_client: Option<Arc<S3Client>>,
    requester_pays: bool,
) -> Result<i64> {
    let mut reader = Lz4JsonLineReader::new(source, s3_client, requester_pays).await?;
    let Some(replica) = reader.next_json_line::<ReplicaLine>().await? else {
        bail!("replica_cmds has no readable json lines");
    };
    parse_timestamp_ms(&replica.abci_block.time)
}

struct Lz4JsonLineReader {
    source: ReaderSource,
    s3_client: Option<Arc<S3Client>>,
    requester_pays: bool,
    next_key_idx: usize,
    local_opened: bool,
    current_reader: Option<Box<dyn AsyncBufRead + Unpin + Send>>,
    current_input_label: Option<Arc<str>>,
    line: Vec<u8>,
    perf: ReaderReadPerfStats,
}

#[derive(Default, Debug, Clone, Copy)]
struct ReaderReadPerfStats {
    open_reader_ns: u128,
    open_reader_calls: u64,
    s3_get_ns: u128,
    s3_get_calls: u64,
    line_read_ns: u128,
    line_read_calls: u64,
    line_read_bytes: u64,
    json_parse_ns: u128,
    json_parse_calls: u64,
}

impl ReaderReadPerfStats {
    fn open_non_s3_ns(self) -> u128 {
        self.open_reader_ns.saturating_sub(self.s3_get_ns)
    }

    fn instrumented_total_ns(self) -> u128 {
        self.s3_get_ns + self.line_read_ns + self.json_parse_ns + self.open_non_s3_ns()
    }
}

impl Lz4JsonLineReader {
    async fn new(source: ReaderSource, s3_client: Option<Arc<S3Client>>, requester_pays: bool) -> Result<Self> {
        if matches!(source, ReaderSource::S3Keys { .. } | ReaderSource::S3HourlyFrom { .. }) && s3_client.is_none() {
            bail!("S3 input requires initialized S3 client");
        }
        Ok(Self {
            source,
            s3_client,
            requester_pays,
            next_key_idx: 0,
            local_opened: false,
            current_reader: None,
            current_input_label: None,
            line: Vec::with_capacity(1 << 20),
            perf: ReaderReadPerfStats::default(),
        })
    }

    fn make_lz4_reader<R>(reader: R) -> Box<dyn AsyncBufRead + Unpin + Send>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let buffered = TokioBufReader::with_capacity(LZ4_IO_BUFFER_BYTES, reader);
        let decoder = Lz4Decoder::new(buffered);
        Box::new(TokioBufReader::with_capacity(LZ4_IO_BUFFER_BYTES, decoder))
    }

    async fn fetch_s3_reader(
        &mut self,
        bucket: &str,
        key: &str,
        allow_missing: bool,
    ) -> Result<Option<Box<dyn AsyncBufRead + Unpin + Send>>> {
        let Some(client) = self.s3_client.as_ref() else {
            bail!("S3 input requires initialized S3 client");
        };

        let mut request = client.get_object().bucket(bucket).key(key);
        if self.requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }

        let s3_started_at = Instant::now();
        let output = match request.send().await {
            Ok(output) => output,
            Err(err) => {
                if let Some(service_error) = err.as_service_error() {
                    if allow_missing
                        && (service_error.is_no_such_key()
                            || service_error.code().is_some_and(|code| code == "NoSuchKey"))
                    {
                        return Ok(None);
                    }

                    if !self.requester_pays && service_error.code().is_some_and(|code| code == "AccessDenied") {
                        bail!("failed to fetch s3://{bucket}/{key}: AccessDenied");
                    }
                }

                bail!("failed to fetch s3://{bucket}/{key}: {err}");
            }
        };
        self.perf.s3_get_ns += s3_started_at.elapsed().as_nanos();
        self.perf.s3_get_calls = self.perf.s3_get_calls.saturating_add(1);

        Ok(Some(Self::make_lz4_reader(output.body.into_async_read())))
    }

    async fn open_next_reader(&mut self) -> Result<bool> {
        enum OpenPlan {
            Local(PathBuf),
            S3Key { bucket: String, key: String, allow_missing: bool, advance_hour: bool },
            End,
        }

        let plan = match &mut self.source {
            ReaderSource::LocalFile(path) => {
                if self.local_opened {
                    OpenPlan::End
                } else {
                    self.local_opened = true;
                    OpenPlan::Local(path.clone())
                }
            }
            ReaderSource::S3Keys { bucket, keys } => {
                if self.next_key_idx >= keys.len() {
                    OpenPlan::End
                } else {
                    let key = keys[self.next_key_idx].clone();
                    self.next_key_idx += 1;
                    OpenPlan::S3Key { bucket: bucket.clone(), key, allow_missing: false, advance_hour: false }
                }
            }
            ReaderSource::S3HourlyFrom { bucket, prefix, next_hour_ms } => {
                let key = build_node_fills_hourly_key(prefix, *next_hour_ms)?;
                OpenPlan::S3Key { bucket: bucket.clone(), key, allow_missing: true, advance_hour: true }
            }
        };

        let open_started_at = Instant::now();
        let result = match plan {
            OpenPlan::End => Ok(false),
            OpenPlan::Local(path) => {
                let file =
                    tokio::fs::File::open(&path).await.with_context(|| format!("failed to open {}", path.display()))?;
                self.current_reader = Some(Self::make_lz4_reader(file));
                self.current_input_label = Some(Arc::<str>::from(path.display().to_string()));
                Ok(true)
            }
            OpenPlan::S3Key { bucket, key, allow_missing, advance_hour } => {
                let maybe_reader = self.fetch_s3_reader(&bucket, &key, allow_missing).await?;
                let opened = if let Some(reader) = maybe_reader {
                    if advance_hour && let ReaderSource::S3HourlyFrom { next_hour_ms, .. } = &mut self.source {
                        *next_hour_ms += HOUR_MS;
                    }
                    self.current_reader = Some(reader);
                    self.current_input_label = Some(Arc::<str>::from(format!("s3://{bucket}/{key}")));
                    true
                } else {
                    self.current_input_label = None;
                    false
                };
                Ok(opened)
            }
        };
        self.perf.open_reader_ns += open_started_at.elapsed().as_nanos();
        self.perf.open_reader_calls = self.perf.open_reader_calls.saturating_add(1);
        result
    }

    async fn next_raw_line_into(&mut self, line: &mut Vec<u8>) -> Result<bool> {
        loop {
            if self.current_reader.is_none() && !self.open_next_reader().await? {
                return Ok(false);
            }

            line.clear();
            let line_read_started_at = Instant::now();
            let bytes_read =
                self.current_reader.as_mut().expect("current_reader checked").read_until(b'\n', line).await?;
            self.perf.line_read_ns += line_read_started_at.elapsed().as_nanos();
            self.perf.line_read_calls = self.perf.line_read_calls.saturating_add(1);
            self.perf.line_read_bytes = self.perf.line_read_bytes.saturating_add(u64::try_from(bytes_read)?);
            if bytes_read == 0 {
                self.current_reader = None;
                self.current_input_label = None;
                continue;
            }

            return Ok(true);
        }
    }

    async fn next_json_line<T>(&mut self) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut line = std::mem::take(&mut self.line);
        let has_line = self.next_raw_line_into(&mut line).await?;
        if !has_line {
            self.line = line;
            return Ok(None);
        }

        let parse_started_at = Instant::now();
        let parsed = parse_json_line(&mut line);
        self.perf.json_parse_ns += parse_started_at.elapsed().as_nanos();
        self.perf.json_parse_calls = self.perf.json_parse_calls.saturating_add(1);
        self.line = line;
        parsed.map(Some)
    }

    fn current_input_label_arc(&self) -> Option<Arc<str>> {
        self.current_input_label.clone()
    }

    fn current_input_label(&self) -> Option<&str> {
        self.current_input_label.as_deref()
    }

    fn perf_snapshot(&self) -> ReaderReadPerfStats {
        self.perf
    }
}

struct NodeFillsReader {
    reader: Lz4JsonLineReader,
    block_range: Option<(u64, u64)>,
    target_asset_ids: HashSet<u64>,
    last_ok_block_number: Option<u64>,
}

impl NodeFillsReader {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
        target_asset_ids: HashSet<u64>,
    ) -> Result<Self> {
        Ok(Self {
            reader: Lz4JsonLineReader::new(source, s3_client, requester_pays).await?,
            block_range,
            target_asset_ids,
            last_ok_block_number: None,
        })
    }

    async fn next_batch(&mut self) -> Result<Option<NodeFillsBatch>> {
        let block = loop {
            let current_input = self.reader.current_input_label().unwrap_or("unknown_input").to_owned();
            let Some(block) = self.reader.next_json_line::<NodeFillsBlock>().await.with_context(|| {
                format!(
                    "node_fills_by_block parse failed input={} last_ok_block={}",
                    current_input,
                    self.last_ok_block_number
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_owned())
                )
            })?
            else {
                return Ok(None);
            };

            if let Some((start_height, end_height)) = self.block_range {
                if block.block_number < start_height {
                    continue;
                }
                if block.block_number > end_height {
                    return Ok(None);
                }
            }
            break block;
        };
        self.last_ok_block_number = Some(block.block_number);

        let (block_time_ns, block_time_ms) = parse_timestamp_ns_ms(&block.block_time)?;
        let mut fills = Vec::with_capacity(block.events.len());
        for (address, event) in block.events {
            let Some(coin) = coin_config_by_symbol(&event.coin) else {
                continue;
            };
            if !self.target_asset_ids.contains(&coin.asset_id) {
                continue;
            }

            fills.push(FillRecord {
                asset_id: coin.asset_id,
                block_number: block.block_number,
                block_time_ms,
                user: address,
                oid: event.oid,
                side: (!event.side.is_empty()).then_some(event.side),
                px: (!event.px.is_empty()).then(|| parse_decimal(&event.px)).transpose()?,
                sz: parse_decimal(&event.sz)?,
                crossed: event.crossed,
                tid: event.tid,
            });
        }

        Ok(Some(NodeFillsBatch { block_number: block.block_number, block_time_ns, block_time_ms, fills }))
    }

    fn perf_snapshot(&self) -> ReaderReadPerfStats {
        self.reader.perf_snapshot()
    }
}

struct ReplicaCmdsReader {
    reader: ReplicaRawReader,
}

enum ReplicaRawReader {
    Stream(Lz4JsonLineReader),
    Parallel(ParallelReplicaLz4Reader),
}

struct ParallelReplicaLz4Reader {
    source: ReaderSource,
    s3_client: Option<Arc<S3Client>>,
    requester_pays: bool,
    parallel_workers: usize,
    s3_range_workers: usize,
    prefetch_buffer: Arc<Semaphore>,
    prefetch_stream_chunk_cap: usize,
    next_key_idx: usize,
    local_opened: bool,
    current_reader: Option<ParallelLz4LineReader>,
    current_input_label: Option<Arc<str>>,
    perf: ReaderReadPerfStats,
}

struct PrefetchChunk {
    chunk_idx: u64,
    bytes: Vec<u8>,
    _memory_permit: OwnedSemaphorePermit,
}

struct PrefetchedS3AsyncRead {
    rx: mpsc::Receiver<std::result::Result<PrefetchChunk, String>>,
    current_chunk: Option<PrefetchChunk>,
    current_chunk_offset: usize,
    pending_chunks: BTreeMap<u64, PrefetchChunk>,
    next_chunk_idx: u64,
    downloaders: Vec<JoinHandle<()>>,
    stream_done: bool,
}

impl AsyncRead for PrefetchedS3AsyncRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut wrote_any = false;
        loop {
            if let Some(chunk) = self.current_chunk.as_ref() {
                let remaining = chunk.bytes.len().saturating_sub(self.current_chunk_offset);
                if remaining > 0 {
                    let copy_len = remaining.min(buf.remaining());
                    let end = self.current_chunk_offset + copy_len;
                    buf.put_slice(&chunk.bytes[self.current_chunk_offset..end]);
                    self.current_chunk_offset = end;
                    wrote_any = true;
                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    continue;
                }
                self.current_chunk = None;
                self.current_chunk_offset = 0;
                continue;
            }

            let expected_chunk_idx = self.next_chunk_idx;
            if let Some(chunk) = self.pending_chunks.remove(&expected_chunk_idx) {
                self.current_chunk = Some(chunk);
                self.current_chunk_offset = 0;
                self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
                continue;
            }

            if self.stream_done {
                if self.pending_chunks.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                return Poll::Ready(Err(std::io::Error::other(format!(
                    "missing ordered replica prefetch chunk idx={} pending_chunks={}",
                    self.next_chunk_idx,
                    self.pending_chunks.len()
                ))));
            }

            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            match Pin::new(&mut self.rx).poll_recv(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.chunk_idx < self.next_chunk_idx {
                        continue;
                    }
                    self.pending_chunks.insert(chunk.chunk_idx, chunk);
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Err(std::io::Error::other(err)));
                }
                Poll::Ready(None) => {
                    self.stream_done = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    return if wrote_any { Poll::Ready(Ok(())) } else { Poll::Pending };
                }
            }
        }
    }
}

impl Drop for PrefetchedS3AsyncRead {
    fn drop(&mut self) {
        for handle in self.downloaders.drain(..) {
            handle.abort();
        }
    }
}

struct PrefetchedParallelS3Reader {
    reader: Option<PrefetchedS3AsyncRead>,
    open_reader_ns: u128,
    s3_get_ns: u128,
    s3_get_calls: u64,
}

async fn fetch_parallel_s3_reader(
    client: Arc<S3Client>,
    requester_pays: bool,
    bucket: String,
    key: String,
    allow_missing: bool,
    prefetch_buffer: Arc<Semaphore>,
    prefetch_stream_chunk_cap: usize,
    s3_range_workers: usize,
) -> Result<PrefetchedParallelS3Reader> {
    let open_started_at = Instant::now();
    let mut request = client.head_object().bucket(&bucket).key(&key);
    if requester_pays {
        request = request.request_payer(RequestPayer::Requester);
    }

    let s3_started_at = Instant::now();
    let head_output = match request.send().await {
        Ok(output) => output,
        Err(err) => {
            if let Some(service_error) = err.as_service_error() {
                if allow_missing
                    && (service_error.is_not_found()
                        || service_error.code().is_some_and(|code| matches!(code, "NotFound" | "NoSuchKey" | "404")))
                {
                    return Ok(PrefetchedParallelS3Reader {
                        reader: None,
                        open_reader_ns: open_started_at.elapsed().as_nanos(),
                        s3_get_ns: s3_started_at.elapsed().as_nanos(),
                        s3_get_calls: 1,
                    });
                }

                if !requester_pays && service_error.code().is_some_and(|code| code == "AccessDenied") {
                    bail!("failed to fetch s3://{bucket}/{key}: AccessDenied");
                }
            }

            bail!("failed to fetch s3://{bucket}/{key}: {err}");
        }
    };

    let Some(content_length) = head_output.content_length() else {
        bail!("missing content length for s3://{bucket}/{key}");
    };
    if content_length < 0 {
        bail!("invalid negative content length for s3://{bucket}/{key}: {}", content_length);
    }
    let content_length =
        usize::try_from(content_length).with_context(|| format!("content length overflow for s3://{bucket}/{key}"))?;

    if content_length == 0 {
        return Ok(PrefetchedParallelS3Reader {
            reader: Some(PrefetchedS3AsyncRead {
                rx: mpsc::channel(1).1,
                current_chunk: None,
                current_chunk_offset: 0,
                pending_chunks: BTreeMap::new(),
                next_chunk_idx: 0,
                downloaders: Vec::new(),
                stream_done: true,
            }),
            open_reader_ns: open_started_at.elapsed().as_nanos(),
            s3_get_ns: s3_started_at.elapsed().as_nanos(),
            s3_get_calls: 1,
        });
    }

    let chunk_count = (content_length + REPLICA_S3_RANGE_PART_BYTES - 1) / REPLICA_S3_RANGE_PART_BYTES;
    let next_chunk_idx = Arc::new(AtomicUsize::new(0));
    let queue_cap = prefetch_stream_chunk_cap.max(s3_range_workers * 2).max(1);
    let (tx, rx) = mpsc::channel::<std::result::Result<PrefetchChunk, String>>(queue_cap);
    let mut downloaders = Vec::with_capacity(s3_range_workers);

    for _ in 0..s3_range_workers {
        let worker_client = Arc::clone(&client);
        let worker_bucket = bucket.clone();
        let worker_key = key.clone();
        let worker_prefetch_buffer = prefetch_buffer.clone();
        let worker_chunk_idx = Arc::clone(&next_chunk_idx);
        let worker_tx = tx.clone();
        let worker_requester_pays = requester_pays;
        let downloader = tokio::spawn(async move {
            loop {
                let chunk_idx = worker_chunk_idx.fetch_add(1, Ordering::Relaxed);
                if chunk_idx >= chunk_count {
                    break;
                }

                let start = chunk_idx.saturating_mul(REPLICA_S3_RANGE_PART_BYTES);
                let end_exclusive = (start + REPLICA_S3_RANGE_PART_BYTES).min(content_length);
                if end_exclusive <= start {
                    continue;
                }
                let end_inclusive = end_exclusive.saturating_sub(1);
                let mut worker_request = worker_client
                    .get_object()
                    .bucket(&worker_bucket)
                    .key(&worker_key)
                    .range(format!("bytes={start}-{end_inclusive}"));
                if worker_requester_pays {
                    worker_request = worker_request.request_payer(RequestPayer::Requester);
                }

                let output = match worker_request.send().await {
                    Ok(value) => value,
                    Err(err) => {
                        drop(
                            worker_tx
                                .send(Err(format!(
                                    "failed to fetch s3://{}/{} range bytes={}-{}: {}",
                                    worker_bucket, worker_key, start, end_inclusive, err
                                )))
                                .await,
                        );
                        return;
                    }
                };

                let bytes = match output.body.collect().await {
                    Ok(collected) => collected.into_bytes().to_vec(),
                    Err(err) => {
                        drop(
                            worker_tx
                                .send(Err(format!(
                                    "failed to read s3://{}/{} range bytes={}-{}: {}",
                                    worker_bucket, worker_key, start, end_inclusive, err
                                )))
                                .await,
                        );
                        return;
                    }
                };

                if bytes.is_empty() {
                    continue;
                }

                let permits = ((bytes.len() + REPLICA_PREFETCH_BUFFER_PERMIT_BYTES - 1)
                    / REPLICA_PREFETCH_BUFFER_PERMIT_BYTES)
                    .max(1);
                let permits = u32::try_from(permits).unwrap_or(u32::MAX);
                let permit = match worker_prefetch_buffer.clone().acquire_many_owned(permits).await {
                    Ok(value) => value,
                    Err(_) => {
                        drop(
                            worker_tx
                                .send(Err("replica prefetch memory semaphore closed unexpectedly".to_owned()))
                                .await,
                        );
                        return;
                    }
                };

                if worker_tx
                    .send(Ok(PrefetchChunk {
                        chunk_idx: u64::try_from(chunk_idx).unwrap_or(u64::MAX),
                        bytes,
                        _memory_permit: permit,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        downloaders.push(downloader);
    }
    drop(tx);

    Ok(PrefetchedParallelS3Reader {
        reader: Some(PrefetchedS3AsyncRead {
            rx,
            current_chunk: None,
            current_chunk_offset: 0,
            pending_chunks: BTreeMap::new(),
            next_chunk_idx: 0,
            downloaders,
            stream_done: false,
        }),
        open_reader_ns: open_started_at.elapsed().as_nanos(),
        s3_get_ns: s3_started_at.elapsed().as_nanos(),
        s3_get_calls: u64::try_from(chunk_count).unwrap_or(u64::MAX).saturating_add(1),
    })
}

impl ParallelReplicaLz4Reader {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        parallel_workers: usize,
        requested_s3_range_workers: usize,
        replica_prefetch_buffer_mb: usize,
    ) -> Result<Self> {
        if matches!(source, ReaderSource::S3Keys { .. } | ReaderSource::S3HourlyFrom { .. }) && s3_client.is_none() {
            bail!("S3 input requires initialized S3 client");
        }
        let prefetch_buffer_bytes = replica_prefetch_buffer_mb.saturating_mul(1024 * 1024);
        let prefetch_buffer_permits = ((prefetch_buffer_bytes + REPLICA_PREFETCH_BUFFER_PERMIT_BYTES - 1)
            / REPLICA_PREFETCH_BUFFER_PERMIT_BYTES)
            .max(1);
        let s3_range_workers = requested_s3_range_workers.max(1);
        let range_part_permits = ((REPLICA_S3_RANGE_PART_BYTES + REPLICA_PREFETCH_BUFFER_PERMIT_BYTES - 1)
            / REPLICA_PREFETCH_BUFFER_PERMIT_BYTES)
            .max(1);
        let prefetch_stream_chunk_cap = (prefetch_buffer_permits / range_part_permits).max(s3_range_workers * 2).max(8);
        info!(
            "[config] replica_prefetch_ring_buffer_mb={} permit_bytes={} total_permits={} range_part_bytes={} chunk_queue_cap={}",
            replica_prefetch_buffer_mb,
            REPLICA_PREFETCH_BUFFER_PERMIT_BYTES,
            prefetch_buffer_permits,
            REPLICA_S3_RANGE_PART_BYTES,
            prefetch_stream_chunk_cap
        );
        Ok(Self {
            source,
            s3_client,
            requester_pays,
            parallel_workers: parallel_workers.max(1),
            s3_range_workers,
            prefetch_buffer: Arc::new(Semaphore::new(prefetch_buffer_permits)),
            prefetch_stream_chunk_cap,
            next_key_idx: 0,
            local_opened: false,
            current_reader: None,
            current_input_label: None,
            perf: ReaderReadPerfStats::default(),
        })
    }

    fn make_parallel_reader<R>(&self, reader: R) -> ParallelLz4LineReader
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        ParallelLz4LineReader::new(
            reader,
            ParallelLz4ReaderConfig { workers: self.parallel_workers, io_buffer_bytes: LZ4_IO_BUFFER_BYTES },
        )
    }

    fn next_s3_plan(&mut self) -> Result<Option<(String, String, bool)>> {
        let plan = match &mut self.source {
            ReaderSource::S3Keys { bucket, keys } => {
                if self.next_key_idx >= keys.len() {
                    return Ok(None);
                }
                let key = keys[self.next_key_idx].clone();
                self.next_key_idx += 1;
                (bucket.clone(), key, false)
            }
            ReaderSource::S3HourlyFrom { bucket, prefix, next_hour_ms } => {
                let key = build_node_fills_hourly_key(prefix, *next_hour_ms)?;
                *next_hour_ms += HOUR_MS;
                (bucket.clone(), key, true)
            }
            ReaderSource::LocalFile(_) => return Ok(None),
        };
        Ok(Some(plan))
    }

    async fn open_next_reader(&mut self) -> Result<bool> {
        if let ReaderSource::LocalFile(path) = &self.source {
            if self.local_opened {
                return Ok(false);
            }
            self.local_opened = true;
            let open_started_at = Instant::now();
            let file =
                tokio::fs::File::open(path).await.with_context(|| format!("failed to open {}", path.display()))?;
            self.current_reader = Some(self.make_parallel_reader(file));
            self.current_input_label = Some(Arc::<str>::from(path.display().to_string()));
            self.perf.open_reader_ns += open_started_at.elapsed().as_nanos();
            self.perf.open_reader_calls = self.perf.open_reader_calls.saturating_add(1);
            return Ok(true);
        }

        let Some(client) = self.s3_client.clone() else {
            bail!("S3 input requires initialized S3 client");
        };

        loop {
            let Some((bucket, key, allow_missing)) = self.next_s3_plan()? else {
                return Ok(false);
            };
            let input_label = Arc::<str>::from(format!("s3://{bucket}/{key}"));

            let prefetched = fetch_parallel_s3_reader(
                Arc::clone(&client),
                self.requester_pays,
                bucket,
                key,
                allow_missing,
                Arc::clone(&self.prefetch_buffer),
                self.prefetch_stream_chunk_cap,
                self.s3_range_workers,
            )
            .await?;
            self.perf.open_reader_ns += prefetched.open_reader_ns;
            self.perf.open_reader_calls = self.perf.open_reader_calls.saturating_add(1);
            self.perf.s3_get_ns += prefetched.s3_get_ns;
            self.perf.s3_get_calls = self.perf.s3_get_calls.saturating_add(prefetched.s3_get_calls);

            if let Some(reader) = prefetched.reader {
                self.current_reader = Some(self.make_parallel_reader(reader));
                self.current_input_label = Some(input_label);
                return Ok(true);
            }
            self.current_input_label = None;
        }
    }

    async fn next_raw_line_into(&mut self, line: &mut Vec<u8>) -> Result<bool> {
        loop {
            if self.current_reader.is_none() && !self.open_next_reader().await? {
                return Ok(false);
            }

            let line_started_at = Instant::now();
            let has_line = self.current_reader.as_mut().expect("current_reader checked").next_line_into(line).await?;
            self.perf.line_read_ns += line_started_at.elapsed().as_nanos();
            self.perf.line_read_calls = self.perf.line_read_calls.saturating_add(1);
            if has_line {
                self.perf.line_read_bytes = self.perf.line_read_bytes.saturating_add(u64::try_from(line.len())?);
                return Ok(true);
            }

            self.current_reader = None;
            self.current_input_label = None;
        }
    }

    fn current_input_label_arc(&self) -> Option<Arc<str>> {
        self.current_input_label.clone()
    }

    fn perf_snapshot(&self) -> ReaderReadPerfStats {
        self.perf
    }
}

impl ReplicaCmdsReader {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        parallel_lz4_workers: usize,
        replica_s3_range_workers: usize,
        replica_prefetch_buffer_mb: usize,
    ) -> Result<Self> {
        if parallel_lz4_workers > 1 {
            let reader = ParallelReplicaLz4Reader::new(
                source,
                s3_client,
                requester_pays,
                parallel_lz4_workers,
                replica_s3_range_workers,
                replica_prefetch_buffer_mb,
            )
            .await?;
            return Ok(Self { reader: ReplicaRawReader::Parallel(reader) });
        }

        Ok(Self { reader: ReplicaRawReader::Stream(Lz4JsonLineReader::new(source, s3_client, requester_pays).await?) })
    }

    async fn next_raw_line_into(&mut self, line: &mut Vec<u8>) -> Result<bool> {
        match &mut self.reader {
            ReplicaRawReader::Stream(reader) => reader.next_raw_line_into(line).await,
            ReplicaRawReader::Parallel(reader) => reader.next_raw_line_into(line).await,
        }
    }

    fn current_input_label_arc(&self) -> Option<Arc<str>> {
        match &self.reader {
            ReplicaRawReader::Stream(reader) => reader.current_input_label_arc(),
            ReplicaRawReader::Parallel(reader) => reader.current_input_label_arc(),
        }
    }

    fn parse_raw_line(line: &mut Vec<u8>, target_asset_ids: &HashSet<u64>) -> Result<ReplicaBatch> {
        let replica = parse_json_line::<ReplicaLine>(line)?;
        Self::parse_replica_line(replica, target_asset_ids)
    }

    fn parse_replica_line(replica: ReplicaLine, target_asset_ids: &HashSet<u64>) -> Result<ReplicaBatch> {
        let (block_time_ns, block_time_ms) = parse_timestamp_ns_ms(&replica.abci_block.time)?;
        let block_number = 0_u64;

        let mut action_map = HashMap::with_capacity(replica.abci_block.signed_action_bundles.len());
        for (tx_hash, payload) in replica.abci_block.signed_action_bundles {
            action_map.insert(tx_hash, payload);
        }
        let mut events = Vec::new();

        let replica_responses = replica.resps.unwrap_or_default();
        for (tx_hash, responses) in replica_responses.full {
            let Some(payload) = action_map.get(&tx_hash) else {
                continue;
            };

            for (action_item, response_item) in payload.signed_actions.iter().zip(responses.iter()) {
                let action = &action_item.action;
                match action.action_type.as_deref() {
                    Some("order") => {
                        let orders = action.orders.as_slice();
                        let statuses = parse_statuses(&response_item.res.response);

                        for (order, status) in orders.iter().zip(statuses.iter()) {
                            let Some((asset, side, px, sz, cloid, meta)) = parse_order_fields(order) else {
                                continue;
                            };
                            if !target_asset_ids.contains(&asset) {
                                continue;
                            }
                            let Some(user) = response_item.user.clone() else {
                                continue;
                            };

                            let has_resting = status.has_resting();
                            let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                            let oid_from_resting = status.resting_oid();
                            let oid_from_filled = status.filled_oid();
                            let oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                            if meta.is_trigger {
                                if let Some(oid) = oid {
                                    events.push(CmdEvent::TrackTriggerAdd { asset_id: asset, user, oid, cloid });
                                } else if is_pending_trigger_status(status) {
                                    events.push(CmdEvent::TrackTriggerPending { asset_id: asset, user, cloid });
                                }
                                continue;
                            }
                            let Some(oid) = oid else {
                                continue;
                            };

                            if !has_resting && !is_gtc {
                                continue;
                            }

                            match parse_booked_and_raw_sz(sz, status) {
                                BookedSzDecision::Booked { booked_sz, raw_sz } => {
                                    events.push(CmdEvent::Add {
                                        asset_id: asset,
                                        user,
                                        oid,
                                        cloid,
                                        side,
                                        px,
                                        sz: booked_sz,
                                        raw_sz,
                                        meta,
                                    });
                                }
                                BookedSzDecision::FullyFilled => continue,
                                BookedSzDecision::InvalidFilledTotalSz => {
                                    events.push(CmdEvent::Skipped {
                                        asset_id: asset,
                                        user: Some(user),
                                        oid: Some(oid),
                                        event: "add".to_owned(),
                                    });
                                }
                            }
                        }
                    }
                    Some("cancel") | Some("cancelByCloid") => {
                        let cancels = action.cancels.as_slice();
                        let statuses = parse_statuses(&response_item.res.response);
                        let is_cancel_by_cloid = action.action_type.as_deref() == Some("cancelByCloid");

                        for (idx, cancel) in cancels.iter().enumerate() {
                            let status = statuses.get(idx);
                            if is_cancel_by_cloid {
                                if !is_success_status(status) {
                                    continue;
                                }
                            } else if !is_success_status(status) {
                                continue;
                            }
                            let order_ref = parse_cancel_order_ref(cancel);
                            let Some(asset) = cancel.a.as_ref().and_then(JsonU64OrString::as_u64) else {
                                continue;
                            };

                            if !target_asset_ids.contains(&asset) {
                                continue;
                            }
                            let Some(order_ref) = order_ref else {
                                continue;
                            };
                            if matches!(order_ref, OrderRef::Oid(0)) {
                                continue;
                            }

                            events.push(CmdEvent::Cancel {
                                asset_id: asset,
                                user: response_item.user.clone(),
                                order_ref,
                                fallback_on_missing: true,
                            });
                        }
                    }
                    Some("modify") => {
                        let Some(order) = action.order.as_ref() else {
                            continue;
                        };
                        let Some((asset, side, px, sz, new_cloid, meta)) = parse_order_fields(order) else {
                            continue;
                        };
                        if !target_asset_ids.contains(&asset) {
                            continue;
                        }
                        let Some(user) = response_item.user.clone() else {
                            continue;
                        };
                        let Some(old_order_ref) = parse_order_ref(action.oid.as_ref(), action.cloid.as_deref()) else {
                            continue;
                        };
                        if is_dead_order_modify_response(&response_item.res) {
                            continue;
                        }

                        let statuses = parse_statuses(&response_item.res.response);
                        let Some(status) = statuses.first() else {
                            if meta.is_trigger {
                                if ENABLE_MISSING_STATUS_WARN {
                                    warn!(
                                        "[warn] block={} user={} trigger modify returned no statuses; skipping old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                }
                            } else {
                                if ENABLE_MISSING_STATUS_WARN {
                                    warn!(
                                        "[warn] block={} user={} modify returned no statuses; treating as cancel-only old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                }
                                events.push(CmdEvent::Cancel {
                                    asset_id: asset,
                                    user: Some(user.clone()),
                                    order_ref: old_order_ref,
                                    fallback_on_missing: true,
                                });
                            }
                            continue;
                        };
                        let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                        let oid_from_resting = status.resting_oid();
                        let oid_from_filled = status.filled_oid();
                        if meta.is_trigger {
                            let new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                            events.push(CmdEvent::TrackTriggerModify {
                                asset_id: asset,
                                user,
                                old_order_ref,
                                new_oid,
                                new_cloid,
                            });
                            continue;
                        }

                        let (new_sz, raw_sz, invalid_filled_total_sz) = match parse_booked_and_raw_sz(sz, status) {
                            BookedSzDecision::Booked { booked_sz, raw_sz } => (booked_sz, raw_sz, false),
                            BookedSzDecision::FullyFilled => (Decimal::ZERO, None, false),
                            BookedSzDecision::InvalidFilledTotalSz => (Decimal::ZERO, None, true),
                        };
                        let mut new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                        if new_sz <= Decimal::ZERO {
                            new_oid = None;
                        }
                        events.push(CmdEvent::Modify {
                            asset_id: asset,
                            user: user.clone(),
                            old_order_ref,
                            new_oid,
                            new_cloid,
                            side,
                            px,
                            sz: new_sz,
                            raw_sz,
                            meta,
                        });

                        if invalid_filled_total_sz {
                            events.push(CmdEvent::Skipped {
                                asset_id: asset,
                                user: Some(user),
                                oid: oid_from_resting.or(oid_from_filled),
                                event: "modify".to_owned(),
                            });
                        }
                    }
                    Some("batchModify") => {
                        let modifies = action.modifies.as_slice();
                        let statuses = parse_statuses(&response_item.res.response);

                        for (idx, modify) in modifies.iter().enumerate() {
                            let Some(order) = modify.order.as_ref() else {
                                continue;
                            };
                            let Some((asset, side, px, sz, new_cloid, meta)) = parse_order_fields(order) else {
                                continue;
                            };
                            if !target_asset_ids.contains(&asset) {
                                continue;
                            }
                            let Some(user) = response_item.user.clone() else {
                                continue;
                            };
                            let Some(old_order_ref) = parse_order_ref(modify.oid.as_ref(), modify.cloid.as_deref())
                            else {
                                continue;
                            };
                            let status = statuses.get(idx);
                            if status.is_none() && meta.is_trigger {
                                if ENABLE_MISSING_STATUS_WARN {
                                    warn!(
                                        "[warn] block={} user={} trigger batchModify item={} returned no statuses; skipping old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        idx,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                }
                                continue;
                            }
                            if status.is_none() && !meta.is_trigger {
                                if ENABLE_MISSING_STATUS_WARN {
                                    warn!(
                                        "[warn] block={} user={} batchModify item={} returned no statuses; treating as cancel-only old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        idx,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                }
                                events.push(CmdEvent::Cancel {
                                    asset_id: asset,
                                    user: Some(user),
                                    order_ref: old_order_ref,
                                    fallback_on_missing: true,
                                });
                                continue;
                            }
                            if status.and_then(StatusEntry::error).is_some_and(is_dead_order_error) {
                                continue;
                            }
                            let status = status.unwrap();
                            let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                            let oid_from_resting = status.resting_oid();
                            let oid_from_filled = status.filled_oid();
                            if meta.is_trigger {
                                let new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                events.push(CmdEvent::TrackTriggerModify {
                                    asset_id: asset,
                                    user,
                                    old_order_ref,
                                    new_oid,
                                    new_cloid,
                                });
                                continue;
                            }

                            let (new_sz, raw_sz, invalid_filled_total_sz) = match parse_booked_and_raw_sz(sz, status) {
                                BookedSzDecision::Booked { booked_sz, raw_sz } => (booked_sz, raw_sz, false),
                                BookedSzDecision::FullyFilled => (Decimal::ZERO, None, false),
                                BookedSzDecision::InvalidFilledTotalSz => (Decimal::ZERO, None, true),
                            };
                            let mut new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                            if new_sz <= Decimal::ZERO {
                                new_oid = None;
                            }
                            events.push(CmdEvent::Modify {
                                asset_id: asset,
                                user: user.clone(),
                                old_order_ref,
                                new_oid,
                                new_cloid,
                                side,
                                px,
                                sz: new_sz,
                                raw_sz,
                                meta,
                            });

                            if invalid_filled_total_sz {
                                events.push(CmdEvent::Skipped {
                                    asset_id: asset,
                                    user: Some(user),
                                    oid: oid_from_resting.or(oid_from_filled),
                                    event: "modify".to_owned(),
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(ReplicaBatch { block_time_ns, block_time_ms, events })
    }

    fn perf_snapshot(&self) -> ReaderReadPerfStats {
        match &self.reader {
            ReplicaRawReader::Stream(reader) => reader.perf_snapshot(),
            ReplicaRawReader::Parallel(reader) => reader.perf_snapshot(),
        }
    }
}

struct ReplicaParseTask {
    seq: u64,
    source_label: Arc<str>,
    line: Vec<u8>,
}

struct ReplicaParseResult {
    seq: u64,
    line: Vec<u8>,
    parsed: Result<ReplicaBatch>,
    parse_ns: u128,
}

type ReplicaPrefetchMessage = Result<(Option<ReplicaBatch>, ReaderReadPerfStats)>;

struct ReplicaPrefetchReader {
    rx: mpsc::Receiver<ReplicaPrefetchMessage>,
    worker: JoinHandle<()>,
    last_perf: ReaderReadPerfStats,
    active_workers: usize,
}

impl ReplicaPrefetchReader {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        target_asset_ids: HashSet<u64>,
        queue_depth: usize,
        requested_json_workers: usize,
        requested_lz4_workers: usize,
        requested_s3_range_workers: usize,
        replica_prefetch_buffer_mb: usize,
    ) -> Result<Self> {
        let host_parallelism = std::thread::available_parallelism().map(|parallelism| parallelism.get()).unwrap_or(1);
        let cpu_budget = host_parallelism.clamp(1, TARGET_REPLICA_CPU_CORES);
        let auto_s3_range_workers = DEFAULT_REPLICA_S3_RANGE_WORKERS;
        let auto_lz4_workers = DEFAULT_REPLICA_LZ4_WORKERS.min(host_parallelism.max(1));
        let auto_json_workers = DEFAULT_REPLICA_JSON_WORKERS.min(MAX_REPLICA_JSON_WORKERS);

        let s3_range_workers = requested_s3_range_workers.max(1);
        let replica_lz4_decode_workers = requested_lz4_workers.clamp(1, host_parallelism.max(1));
        if requested_lz4_workers != replica_lz4_decode_workers {
            warn!(
                "[config] replica_lz4_workers requested={} adjusted_to={} host_parallelism={}",
                requested_lz4_workers, replica_lz4_decode_workers, host_parallelism
            );
        }

        let effective_workers = requested_json_workers.clamp(1, MAX_REPLICA_JSON_WORKERS);
        if requested_json_workers != effective_workers {
            warn!(
                "[config] replica_json_workers requested={} adjusted_to={} max={}",
                requested_json_workers, effective_workers, MAX_REPLICA_JSON_WORKERS
            );
        }

        info!("[config.cpu] host_parallelism={} cpu_budget={}", host_parallelism, cpu_budget);
        info!(
            "[config.s3] workers[auto={}][override={}][effective={}]",
            auto_s3_range_workers, requested_s3_range_workers, s3_range_workers
        );
        info!(
            "[config.lz4] workers[auto={}][override={}][effective={}]",
            auto_lz4_workers, requested_lz4_workers, replica_lz4_decode_workers
        );
        info!(
            "[config.json] workers[auto={}][override={}][effective={}]",
            auto_json_workers, requested_json_workers, effective_workers
        );
        let channel_capacity = queue_depth.max(1);

        let mut reader = ReplicaCmdsReader::new(
            source,
            s3_client,
            requester_pays,
            replica_lz4_decode_workers,
            s3_range_workers,
            replica_prefetch_buffer_mb,
        )
        .await?;
        let (tx, rx) = mpsc::channel(channel_capacity);
        let worker = tokio::spawn(async move {
            let target_asset_ids = Arc::new(target_asset_ids);
            let parser_result_capacity = channel_capacity.max(effective_workers);
            let per_worker_capacity = (parser_result_capacity / effective_workers).max(1);
            let (result_tx, mut result_rx) = mpsc::channel::<ReplicaParseResult>(parser_result_capacity);

            let mut task_txs = Vec::with_capacity(effective_workers);
            for _ in 0..effective_workers {
                let (task_tx, mut task_rx) = mpsc::channel::<ReplicaParseTask>(per_worker_capacity);
                task_txs.push(task_tx);
                let worker_result_tx = result_tx.clone();
                let worker_target_asset_ids = Arc::clone(&target_asset_ids);
                tokio::spawn(async move {
                    while let Some(mut task) = task_rx.recv().await {
                        let parse_started_at = Instant::now();
                        let parsed = ReplicaCmdsReader::parse_raw_line(&mut task.line, worker_target_asset_ids.as_ref())
                            .with_context(|| format!("replica_cmds parse failed input={}", task.source_label));
                        let parse_ns = parse_started_at.elapsed().as_nanos();
                        task.line.clear();
                        if worker_result_tx
                            .send(ReplicaParseResult { seq: task.seq, line: task.line, parsed, parse_ns })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            }
            drop(result_tx);

            let mut next_seq_to_assign = 0_u64;
            let mut next_seq_to_emit = 0_u64;
            let mut next_worker_idx = 0_usize;
            let mut inflight = 0_usize;
            let max_inflight = channel_capacity.max(effective_workers);
            let mut reader_exhausted = false;
            let mut pending: BTreeMap<u64, Result<ReplicaBatch>> = BTreeMap::new();
            let mut recycled_line_buffers: Vec<Vec<u8>> = Vec::with_capacity(max_inflight + effective_workers);
            let mut parse_ns_total = 0_u128;
            let mut parse_calls_total = 0_u64;

            loop {
                while !reader_exhausted && inflight < max_inflight {
                    let mut line =
                        recycled_line_buffers.pop().unwrap_or_else(|| Vec::with_capacity(LZ4_IO_BUFFER_BYTES));
                    let has_line = match reader.next_raw_line_into(&mut line).await {
                        Ok(has_line) => has_line,
                        Err(err) => {
                            drop(tx.send(Err(err)).await);
                            return;
                        }
                    };
                    if !has_line {
                        recycled_line_buffers.push(line);
                        reader_exhausted = true;
                        break;
                    }

                    let worker_slot = next_worker_idx % effective_workers;
                    next_worker_idx = next_worker_idx.wrapping_add(1);
                    let source_label =
                        reader.current_input_label_arc().unwrap_or_else(|| Arc::<str>::from("unknown_input"));
                    if task_txs[worker_slot]
                        .send(ReplicaParseTask { seq: next_seq_to_assign, source_label, line })
                        .await
                        .is_err()
                    {
                        drop(tx.send(Err(anyhow!("replica prefetch parser worker channel closed unexpectedly"))).await);
                        return;
                    }
                    next_seq_to_assign = next_seq_to_assign.saturating_add(1);
                    inflight = inflight.saturating_add(1);
                }

                if inflight == 0 {
                    if reader_exhausted {
                        break;
                    }
                    continue;
                }

                let Some(result) = result_rx.recv().await else {
                    drop(tx.send(Err(anyhow!("replica prefetch parser workers stopped unexpectedly"))).await);
                    return;
                };
                inflight = inflight.saturating_sub(1);
                parse_ns_total = parse_ns_total.saturating_add(result.parse_ns);
                parse_calls_total = parse_calls_total.saturating_add(1);
                recycled_line_buffers.push(result.line);
                pending.insert(result.seq, result.parsed);

                while let Some(parsed) = pending.remove(&next_seq_to_emit) {
                    let mut perf = reader.perf_snapshot();
                    perf.json_parse_ns = parse_ns_total;
                    perf.json_parse_calls = parse_calls_total;
                    let had_error = parsed.is_err();
                    if tx.send(parsed.map(|batch| (Some(batch), perf))).await.is_err() {
                        return;
                    }
                    next_seq_to_emit = next_seq_to_emit.saturating_add(1);
                    if had_error {
                        return;
                    }
                }
            }

            drop(task_txs);
            let mut perf = reader.perf_snapshot();
            perf.json_parse_ns = parse_ns_total;
            perf.json_parse_calls = parse_calls_total;
            drop(tx.send(Ok((None, perf))).await);
        });

        Ok(Self { rx, worker, last_perf: ReaderReadPerfStats::default(), active_workers: effective_workers })
    }

    async fn next_batch(&mut self) -> Result<Option<ReplicaBatch>> {
        let Some(message) = self.rx.recv().await else {
            if self.worker.is_finished() {
                match (&mut self.worker).await {
                    Ok(()) => return Ok(None),
                    Err(err) => bail!("replica prefetch worker failed: {err}"),
                }
            }
            bail!("replica prefetch channel closed unexpectedly");
        };

        let (batch, perf) = message?;
        self.last_perf = perf;
        Ok(batch)
    }

    fn worker_count(&self) -> usize {
        self.active_workers
    }

    fn perf_snapshot(&self) -> ReaderReadPerfStats {
        self.last_perf
    }
}

impl Drop for ReplicaPrefetchReader {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

fn output_schema(px_scale: i8, sz_scale: i8) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int64, false),
        Field::new("block_time", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("coin", DataType::Utf8, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("oid", DataType::Int64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("px", DataType::Decimal128(DECIMAL_PRECISION, px_scale), true),
        Field::new("event", DataType::Utf8, false),
        Field::new("sz", DataType::Decimal128(DECIMAL_PRECISION, sz_scale), true),
        Field::new("orig_sz", DataType::Decimal128(DECIMAL_PRECISION, sz_scale), true),
        Field::new("raw_sz", DataType::Decimal128(DECIMAL_PRECISION, sz_scale), true),
        Field::new("fill_sz", DataType::Decimal128(DECIMAL_PRECISION, sz_scale), true),
        Field::new("tif", DataType::Utf8, true),
        Field::new("reduce_only", DataType::Boolean, true),
        Field::new("lifetime", DataType::Int32, true),
    ]))
}

fn decimal_to_i128(mut value: Decimal, scale: i8) -> i128 {
    value.rescale(scale as u32);
    value.mantissa()
}

fn encode_lifetime(created_time_ms: i64, current_time_ms: i64) -> Option<i32> {
    let delta = current_time_ms - created_time_ms;
    if delta < 0 {
        return None;
    }
    if delta <= 999_999 {
        return i32::try_from(delta).ok();
    }
    let rounded_minutes = delta.checked_add(30_000)?.checked_div(60_000)?;
    i32::try_from(rounded_minutes).ok().and_then(|minutes| minutes.checked_neg())
}

fn resolve_order_ref(
    order_ref: &OrderRef,
    user: Option<&str>,
    cloid_to_oid: &HashMap<String, HashMap<String, u64>>,
) -> Option<u64> {
    match order_ref {
        OrderRef::Oid(oid) => Some(*oid),
        OrderRef::Cloid(cloid) => {
            let user = user?;
            cloid_to_oid.get(user).and_then(|by_cloid| by_cloid.get(cloid)).copied()
        }
    }
}

fn window_start_by_span(block_number: u64, span: u64) -> u64 {
    ((block_number - 1) / span) * span + 1
}

fn window_start_block(block_number: u64) -> u64 {
    window_start_by_span(block_number, UNKNOWN_OID_WINDOW_BLOCKS)
}

fn row_group_window_start_block(block_number: u64) -> u64 {
    window_start_by_span(block_number, ROW_GROUP_BLOCKS)
}

fn file_window_start_block(block_number: u64) -> u64 {
    window_start_by_span(block_number, FILE_ROTATION_BLOCKS)
}

fn order_ref_parts(order_ref: &OrderRef) -> (&'static str, Option<u64>, Option<String>) {
    match order_ref {
        OrderRef::Oid(oid) => ("oid", Some(*oid), None),
        OrderRef::Cloid(cloid) => ("cloid", None, Some(cloid.clone())),
    }
}

fn resolve_trigger_ref(order_ref: &OrderRef, user: Option<&str>, trigger_refs: &TriggerOrderRefs) -> Option<u64> {
    match order_ref {
        OrderRef::Oid(oid) => trigger_refs.oid_to_cloid.contains_key(oid).then_some(*oid),
        OrderRef::Cloid(cloid) => {
            let user = user?;
            trigger_refs.cloid_to_oid.get(user).and_then(|by_cloid| by_cloid.get(cloid)).copied()
        }
    }
}

fn remove_trigger_ref(trigger_refs: &mut TriggerOrderRefs, oid: u64) -> bool {
    let Some(cloid_key) = trigger_refs.oid_to_cloid.remove(&oid) else {
        return false;
    };
    trigger_refs.oid_created_time_ms.remove(&oid);
    if let Some(ref cloid_key) = cloid_key {
        let mut remove_user_bucket = false;
        if let Some(by_cloid) = trigger_refs.cloid_to_oid.get_mut(&cloid_key.user) {
            by_cloid.remove(&cloid_key.cloid);
            remove_user_bucket = by_cloid.is_empty();
        }
        if remove_user_bucket {
            trigger_refs.cloid_to_oid.remove(&cloid_key.user);
        }
    }
    true
}

fn insert_pending_trigger_ref(trigger_refs: &mut TriggerOrderRefs, user: &str, cloid: Option<&str>) {
    trigger_refs.pending_by_user.entry(user.to_owned()).or_default().push_back(cloid.map(ToOwned::to_owned));
}

fn remove_pending_trigger_ref_by_cloid(trigger_refs: &mut TriggerOrderRefs, user: &str, cloid: &str) -> bool {
    let Some(pending) = trigger_refs.pending_by_user.get_mut(user) else {
        return false;
    };
    let before_len = pending.len();
    pending.retain(|pending_cloid| pending_cloid.as_deref() != Some(cloid));
    if before_len == pending.len() {
        return false;
    }
    if pending.is_empty() {
        trigger_refs.pending_by_user.remove(user);
    }
    true
}

fn consume_pending_trigger_ref_for_user(trigger_refs: &mut TriggerOrderRefs, user: &str) -> bool {
    let Some(pending) = trigger_refs.pending_by_user.get_mut(user) else {
        return false;
    };
    if pending.pop_front().is_none() {
        return false;
    }
    if pending.is_empty() {
        trigger_refs.pending_by_user.remove(user);
    }
    true
}

fn insert_trigger_ref(
    trigger_refs: &mut TriggerOrderRefs,
    user: &str,
    oid: u64,
    cloid: Option<&str>,
    created_time_ms: i64,
) {
    remove_trigger_ref(trigger_refs, oid);
    if let Some(cloid) = cloid {
        if let Some(existing_oid) =
            trigger_refs.cloid_to_oid.get(user).and_then(|by_cloid| by_cloid.get(cloid)).copied()
            && existing_oid != oid
        {
            remove_trigger_ref(trigger_refs, existing_oid);
        }
        remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid);
    }
    let cloid_key = cloid.map(|cloid| UserCloidKey { user: user.to_owned(), cloid: cloid.to_owned() });
    if let Some(cloid_key) = &cloid_key {
        trigger_refs.cloid_to_oid.entry(cloid_key.user.clone()).or_default().insert(cloid_key.cloid.clone(), oid);
    }
    trigger_refs.oid_created_time_ms.insert(oid, created_time_ms);
    trigger_refs.oid_created_queue.push_back((created_time_ms, oid));
    trigger_refs.oid_to_cloid.insert(oid, cloid_key);
}

fn remove_live_order(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    order_expiry_index: &mut BTreeSet<(i64, u64)>,
    oid: u64,
) -> Option<OrderState> {
    let state = orders.remove(&oid)?;
    order_expiry_index.remove(&(state.created_time_ms, oid));
    if let Some(cloid) = &state.cloid {
        let mut remove_user_bucket = false;
        if let Some(by_cloid) = cloid_to_oid.get_mut(&state.user) {
            by_cloid.remove(cloid);
            remove_user_bucket = by_cloid.is_empty();
        }
        if remove_user_bucket {
            cloid_to_oid.remove(&state.user);
        }
    }
    Some(state)
}

fn insert_live_order(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    order_expiry_index: &mut BTreeSet<(i64, u64)>,
    user: &str,
    oid: u64,
    cloid: Option<&str>,
    created_block_number: u64,
    created_time_ms: i64,
    side: &str,
    px: Decimal,
    sz: Decimal,
    meta: &OrderMeta,
) {
    drop(remove_live_order(orders, cloid_to_oid, order_expiry_index, oid));
    if let Some(cloid) = cloid {
        if let Some(existing_oid) = cloid_to_oid.get(user).and_then(|by_cloid| by_cloid.get(cloid)).copied()
            && existing_oid != oid
        {
            drop(remove_live_order(orders, cloid_to_oid, order_expiry_index, existing_oid));
        }
        cloid_to_oid.entry(user.to_owned()).or_default().insert(cloid.to_owned(), oid);
    }
    orders.insert(
        oid,
        OrderState {
            user: user.to_owned(),
            cloid: cloid.map(ToOwned::to_owned),
            created_block_number,
            created_time_ms,
            side: side.to_owned(),
            px,
            remaining_sz: sz,
            tif: meta.tif.clone(),
            reduce_only: meta.reduce_only,
            is_trigger: meta.is_trigger,
            trigger_condition: meta.trigger_condition.clone(),
            trigger_px: meta.trigger_px,
            is_position_tpsl: meta.is_position_tpsl,
            tp_trigger_px: meta.tp_trigger_px,
            sl_trigger_px: meta.sl_trigger_px,
        },
    );
    order_expiry_index.insert((created_time_ms, oid));
}

fn prune_stale_live_orders(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    order_expiry_index: &mut BTreeSet<(i64, u64)>,
    current_block_number: u64,
    current_time_ms: i64,
) -> StalePruneRun {
    let cutoff_ms = current_time_ms.saturating_sub(LIVE_STATE_TTL_MS);
    let mut samples = Vec::new();
    let mut pruned_total = 0u64;
    while let Some((created_time_ms, oid)) = order_expiry_index.first().copied() {
        if created_time_ms > cutoff_ms {
            break;
        }
        order_expiry_index.pop_first();
        let should_prune = orders.get(&oid).is_some_and(|state| state.created_time_ms == created_time_ms);
        if !should_prune {
            continue;
        }
        if let Some(state) = remove_live_order(orders, cloid_to_oid, order_expiry_index, oid) {
            pruned_total += 1;
            if samples.len() < PRUNE_SAMPLE_LIMIT_PER_RUN {
                samples.push(PrunedOrderSample {
                    oid,
                    user: state.user,
                    cloid: state.cloid,
                    created_block_number: state.created_block_number,
                    created_time_ms: state.created_time_ms,
                    pruned_at_block_number: current_block_number,
                    pruned_at_time_ms: current_time_ms,
                    age_ms: current_time_ms.saturating_sub(state.created_time_ms),
                });
            }
        }
    }
    let sampled = u64::try_from(samples.len()).unwrap_or(0);
    let sample_dropped = pruned_total.saturating_sub(sampled);
    StalePruneRun {
        trigger_block_number: current_block_number,
        trigger_block_time_ms: current_time_ms,
        cutoff_time_ms: cutoff_ms,
        pruned_total,
        sampled,
        sample_dropped,
        samples,
    }
}

fn prune_stale_trigger_oids(trigger_refs: &mut TriggerOrderRefs, current_time_ms: i64) -> usize {
    let cutoff_ms = current_time_ms.saturating_sub(LIVE_STATE_TTL_MS);
    let mut pruned = 0usize;
    while let Some((created_time_ms, oid)) = trigger_refs.oid_created_queue.front().copied() {
        if created_time_ms > cutoff_ms {
            break;
        }
        trigger_refs.oid_created_queue.pop_front();
        let should_prune =
            trigger_refs.oid_created_time_ms.get(&oid).is_some_and(|known_created| *known_created == created_time_ms);
        if !should_prune {
            continue;
        }
        if remove_trigger_ref(trigger_refs, oid) {
            pruned += 1;
        }
    }
    pruned
}

fn rebuild_trigger_cloid_index(trigger_refs: &mut TriggerOrderRefs) {
    trigger_refs.cloid_to_oid.clear();
    for (&oid, cloid_key) in trigger_refs.oid_to_cloid.iter() {
        if let Some(cloid_key) = cloid_key {
            trigger_refs.cloid_to_oid.entry(cloid_key.user.clone()).or_default().insert(cloid_key.cloid.clone(), oid);
        }
    }
}

fn rebuild_trigger_created_queue(trigger_refs: &mut TriggerOrderRefs) {
    let mut ordered: Vec<(i64, u64)> =
        trigger_refs.oid_created_time_ms.iter().map(|(&oid, &created_time_ms)| (created_time_ms, oid)).collect();
    ordered.sort_unstable();
    trigger_refs.oid_created_queue = VecDeque::from(ordered);
}

fn push_add_row(
    rows: &mut RowBuffer,
    coin_symbol: &str,
    block_number: u64,
    block_time_ms: i64,
    user: &str,
    oid: u64,
    side: &str,
    px: Decimal,
    sz: Decimal,
    raw_sz: Option<Decimal>,
    meta: &OrderMeta,
) -> Result<()> {
    rows.push(OutputRow {
        block_number: i64::try_from(block_number)?,
        block_time_ms,
        coin: coin_symbol.to_owned(),
        user: user.to_owned(),
        oid: Some(i64::try_from(oid)?),
        side: Some(side.to_owned()),
        px: Some(px),
        event: "add".to_owned(),
        sz: Some(sz),
        orig_sz: None,
        raw_sz,
        fill_sz: None,
        tif: meta.tif.clone(),
        reduce_only: meta.reduce_only,
        lifetime: None,
    });
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
struct BlockPruneStats {
    live_pruned_orders: u64,
    trigger_pruned_oids: u64,
}

fn process_block(
    coin_symbol: &str,
    target_asset_id: u64,
    block_number: u64,
    block_time_ms: i64,
    cmd_events: Option<&[CmdEvent]>,
    fills: Option<&[FillRecord]>,
    emit_rows: bool,
    emit_json_logs: bool,
    rows: &mut RowBuffer,
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    order_expiry_index: &mut BTreeSet<(i64, u64)>,
    trigger_refs: &mut TriggerOrderRefs,
    unknown_oid_logger: &mut UnknownOidWindowLogger,
    perf_stats: &mut ProcessingPerfStats,
) -> Result<BlockPruneStats> {
    let process_block_started_at = Instant::now();
    let mut block_prune_stats = BlockPruneStats::default();

    let prune_started_at = Instant::now();
    unknown_oid_logger.set_enabled(emit_json_logs);
    unknown_oid_logger.observe_block(block_number)?;
    if block_number == row_group_window_start_block(block_number) {
        let prune_run = prune_stale_live_orders(orders, cloid_to_oid, order_expiry_index, block_number, block_time_ms);
        block_prune_stats.live_pruned_orders = prune_run.pruned_total;
        let pruned_trigger_oids = prune_stale_trigger_oids(trigger_refs, block_time_ms);
        block_prune_stats.trigger_pruned_oids = u64::try_from(pruned_trigger_oids).unwrap_or(u64::MAX);
        if emit_json_logs {
            unknown_oid_logger.record_stale_prune_run(prune_run);
        }
    }
    perf_stats.process_block_prune_ns += prune_started_at.elapsed().as_nanos();

    if let Some(events) = cmd_events {
        let cmd_started_at = Instant::now();
        let mut canceled_oids_in_block: HashSet<u64> = HashSet::new();
        let mut deferred_cancels: Vec<(Option<String>, OrderRef, bool)> = Vec::new();
        for event in events {
            match event {
                CmdEvent::Add { asset_id, user, oid, cloid, side, px, sz, raw_sz, meta } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    insert_live_order(
                        orders,
                        cloid_to_oid,
                        order_expiry_index,
                        user,
                        *oid,
                        cloid.as_deref(),
                        block_number,
                        block_time_ms,
                        side,
                        *px,
                        *sz,
                        meta,
                    );
                    if emit_rows {
                        push_add_row(
                            rows,
                            coin_symbol,
                            block_number,
                            block_time_ms,
                            user,
                            *oid,
                            side,
                            *px,
                            *sz,
                            *raw_sz,
                            meta,
                        )?;
                    }
                }
                CmdEvent::Cancel { asset_id, user, order_ref, fallback_on_missing } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    if let Some(trigger_oid) = resolve_trigger_ref(order_ref, user.as_deref(), trigger_refs) {
                        remove_trigger_ref(trigger_refs, trigger_oid);
                        continue;
                    }
                    if let (Some(user), OrderRef::Cloid(cloid)) = (user.as_deref(), order_ref)
                        && remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid)
                    {
                        continue;
                    }
                    let resolved_oid = resolve_order_ref(order_ref, user.as_deref(), cloid_to_oid);
                    if resolved_oid.is_none() && matches!(order_ref, OrderRef::Cloid(_)) && user.is_some() {
                        deferred_cancels.push((user.clone(), order_ref.clone(), *fallback_on_missing));
                        continue;
                    }
                    let mut emitted_remove = false;
                    let mut logged_unknown = false;

                    if let Some(oid) = resolved_oid {
                        if canceled_oids_in_block.contains(&oid) {
                            continue;
                        }
                        if let Some(state) = remove_live_order(orders, cloid_to_oid, order_expiry_index, oid) {
                            canceled_oids_in_block.insert(oid);
                            if emit_rows {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: coin_symbol.to_owned(),
                                    user: state.user,
                                    oid: Some(i64::try_from(oid)?),
                                    side: Some(state.side),
                                    px: Some(state.px),
                                    event: "cancel".to_owned(),
                                    sz: Some(Decimal::ZERO),
                                    orig_sz: Some(state.remaining_sz),
                                    raw_sz: None,
                                    fill_sz: None,
                                    tif: state.tif.clone(),
                                    reduce_only: state.reduce_only,
                                    lifetime: encode_lifetime(state.created_time_ms, block_time_ms),
                                });
                            }
                            emitted_remove = true;
                        }
                    } else {
                        match order_ref {
                            OrderRef::Cloid(_) => {
                                unknown_oid_logger.record_unresolved_user_cloid("cancel");
                                logged_unknown = true;
                            }
                            OrderRef::Oid(_) => {
                                let (ref_kind, ref_oid, ref_cloid) = order_ref_parts(order_ref);
                                unknown_oid_logger.record_sample(UnknownOidSample {
                                    block_number,
                                    block_time_ms,
                                    event: "cancel",
                                    phase: "immediate",
                                    reason: "resolve_ref_none",
                                    user: user.clone(),
                                    ref_kind,
                                    ref_oid,
                                    ref_cloid,
                                    mapped_oid: None,
                                });
                                logged_unknown = true;
                            }
                        }
                    }

                    if !emitted_remove {
                        if let (Some(user), OrderRef::Oid(oid)) = (user.as_deref(), order_ref)
                            && !orders.contains_key(oid)
                            && consume_pending_trigger_ref_for_user(trigger_refs, user)
                        {
                            continue;
                        }
                        if !logged_unknown {
                            let (ref_kind, ref_oid, ref_cloid) = order_ref_parts(order_ref);
                            unknown_oid_logger.record_sample(UnknownOidSample {
                                block_number,
                                block_time_ms,
                                event: "cancel",
                                phase: "immediate",
                                reason: "live_state_miss_on_cancel",
                                user: user.clone(),
                                ref_kind,
                                ref_oid,
                                ref_cloid,
                                mapped_oid: resolved_oid,
                            });
                        }
                    }
                }
                CmdEvent::Modify { asset_id, user, old_order_ref, new_oid, new_cloid, side, px, sz, raw_sz, meta } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    let resolved_old_oid = resolve_order_ref(old_order_ref, Some(user.as_str()), cloid_to_oid);
                    if let Some(old_oid) = resolved_old_oid {
                        if let Some(old_state) = remove_live_order(orders, cloid_to_oid, order_expiry_index, old_oid) {
                            if emit_rows {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: coin_symbol.to_owned(),
                                    user: old_state.user,
                                    oid: Some(i64::try_from(old_oid)?),
                                    side: Some(old_state.side),
                                    px: Some(old_state.px),
                                    event: "cancel".to_owned(),
                                    sz: Some(Decimal::ZERO),
                                    orig_sz: Some(old_state.remaining_sz),
                                    raw_sz: None,
                                    fill_sz: None,
                                    tif: old_state.tif.clone(),
                                    reduce_only: old_state.reduce_only,
                                    lifetime: encode_lifetime(old_state.created_time_ms, block_time_ms),
                                });
                            }
                        } else {
                            let (ref_kind, ref_oid, ref_cloid) = order_ref_parts(old_order_ref);
                            unknown_oid_logger.record_sample(UnknownOidSample {
                                block_number,
                                block_time_ms,
                                event: "modify",
                                phase: "immediate",
                                reason: "live_state_miss_on_modify_old_ref",
                                user: Some(user.clone()),
                                ref_kind,
                                ref_oid,
                                ref_cloid,
                                mapped_oid: Some(old_oid),
                            });
                        }
                    } else {
                        match old_order_ref {
                            OrderRef::Cloid(_) => {
                                unknown_oid_logger.record_unresolved_user_cloid("modify");
                            }
                            OrderRef::Oid(_) => {
                                let (ref_kind, ref_oid, ref_cloid) = order_ref_parts(old_order_ref);
                                unknown_oid_logger.record_sample(UnknownOidSample {
                                    block_number,
                                    block_time_ms,
                                    event: "modify",
                                    phase: "immediate",
                                    reason: "resolve_ref_none",
                                    user: Some(user.clone()),
                                    ref_kind,
                                    ref_oid,
                                    ref_cloid,
                                    mapped_oid: None,
                                });
                            }
                        }
                    }

                    if !meta.is_trigger {
                        if let Some(new_oid) = new_oid {
                            insert_live_order(
                                orders,
                                cloid_to_oid,
                                order_expiry_index,
                                user,
                                *new_oid,
                                new_cloid.as_deref(),
                                block_number,
                                block_time_ms,
                                side,
                                *px,
                                *sz,
                                meta,
                            );
                            if emit_rows {
                                push_add_row(
                                    rows,
                                    coin_symbol,
                                    block_number,
                                    block_time_ms,
                                    user,
                                    *new_oid,
                                    side,
                                    *px,
                                    *sz,
                                    *raw_sz,
                                    meta,
                                )?;
                            }
                        }
                    }
                }
                CmdEvent::TrackTriggerAdd { asset_id, user, oid, cloid } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    insert_trigger_ref(trigger_refs, user, *oid, cloid.as_deref(), block_time_ms);
                }
                CmdEvent::TrackTriggerPending { asset_id, user, cloid } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    insert_pending_trigger_ref(trigger_refs, user, cloid.as_deref());
                }
                CmdEvent::TrackTriggerModify { asset_id, user, old_order_ref, new_oid, new_cloid } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    if let Some(old_oid) = resolve_trigger_ref(old_order_ref, Some(user.as_str()), trigger_refs) {
                        remove_trigger_ref(trigger_refs, old_oid);
                    } else if let OrderRef::Cloid(cloid) = old_order_ref {
                        remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid);
                    }
                    if let Some(new_oid) = new_oid {
                        insert_trigger_ref(trigger_refs, user, *new_oid, new_cloid.as_deref(), block_time_ms);
                    } else if new_cloid.is_some() {
                        insert_pending_trigger_ref(trigger_refs, user, new_cloid.as_deref());
                    }
                }
                CmdEvent::Skipped { asset_id, user, oid, event } => {
                    if *asset_id != target_asset_id {
                        continue;
                    }
                    if emit_rows {
                        let oid = oid.map(i64::try_from).transpose()?;
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: coin_symbol.to_owned(),
                            user: user.clone().unwrap_or_default(),
                            oid,
                            side: None,
                            px: None,
                            event: event.clone(),
                            sz: None,
                            orig_sz: None,
                            raw_sz: None,
                            fill_sz: None,
                            tif: None,
                            reduce_only: None,
                            lifetime: None,
                        });
                    }
                }
            }
        }

        for (user, order_ref, _fallback_on_missing) in deferred_cancels.drain(..) {
            if let Some(trigger_oid) = resolve_trigger_ref(&order_ref, user.as_deref(), trigger_refs) {
                remove_trigger_ref(trigger_refs, trigger_oid);
                continue;
            }
            if let (Some(user), OrderRef::Cloid(cloid)) = (user.as_deref(), &order_ref)
                && remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid)
            {
                continue;
            }
            let resolved_oid = resolve_order_ref(&order_ref, user.as_deref(), cloid_to_oid);
            let mut emitted_remove = false;
            let mut logged_unknown = false;

            if let Some(oid) = resolved_oid {
                if canceled_oids_in_block.contains(&oid) {
                    continue;
                }
                if let Some(state) = remove_live_order(orders, cloid_to_oid, order_expiry_index, oid) {
                    canceled_oids_in_block.insert(oid);
                    if emit_rows {
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: coin_symbol.to_owned(),
                            user: state.user,
                            oid: Some(i64::try_from(oid)?),
                            side: Some(state.side),
                            px: Some(state.px),
                            event: "cancel".to_owned(),
                            sz: Some(Decimal::ZERO),
                            orig_sz: Some(state.remaining_sz),
                            raw_sz: None,
                            fill_sz: None,
                            tif: state.tif.clone(),
                            reduce_only: state.reduce_only,
                            lifetime: encode_lifetime(state.created_time_ms, block_time_ms),
                        });
                    }
                    emitted_remove = true;
                }
            } else {
                match &order_ref {
                    OrderRef::Cloid(_) => {
                        unknown_oid_logger.record_unresolved_user_cloid("cancel");
                        logged_unknown = true;
                    }
                    OrderRef::Oid(_) => {
                        let (ref_kind, ref_oid, ref_cloid) = order_ref_parts(&order_ref);
                        unknown_oid_logger.record_sample(UnknownOidSample {
                            block_number,
                            block_time_ms,
                            event: "cancel",
                            phase: "deferred",
                            reason: "resolve_ref_none",
                            user: user.clone(),
                            ref_kind,
                            ref_oid,
                            ref_cloid,
                            mapped_oid: None,
                        });
                        logged_unknown = true;
                    }
                }
            }

            if !emitted_remove {
                if let (Some(user), OrderRef::Oid(oid)) = (user.as_deref(), &order_ref)
                    && !orders.contains_key(oid)
                    && consume_pending_trigger_ref_for_user(trigger_refs, user)
                {
                    continue;
                }
                if !logged_unknown {
                    let (ref_kind, ref_oid, ref_cloid) = order_ref_parts(&order_ref);
                    unknown_oid_logger.record_sample(UnknownOidSample {
                        block_number,
                        block_time_ms,
                        event: "cancel",
                        phase: "deferred",
                        reason: "live_state_miss_on_cancel",
                        user: user.clone(),
                        ref_kind,
                        ref_oid,
                        ref_cloid,
                        mapped_oid: resolved_oid,
                    });
                }
            }
        }
        perf_stats.process_block_cmd_ns += cmd_started_at.elapsed().as_nanos();
    }

    if let Some(fills) = fills {
        let fills_started_at = Instant::now();
        let taker_tids = if emit_rows {
            Some(
                fills
                    .iter()
                    .filter(|fill| fill.asset_id == target_asset_id)
                    .filter(|fill| fill.crossed)
                    .filter_map(|fill| fill.tid)
                    .collect::<HashSet<u64>>(),
            )
        } else {
            None
        };

        for fill in fills {
            if fill.asset_id != target_asset_id {
                continue;
            }
            let fill_time_ms = fill.block_time_ms;

            // Filled/crossed trigger orders should not remain in live trigger refs.
            remove_trigger_ref(trigger_refs, fill.oid);

            if fill.crossed {
                continue;
            }

            let has_matching_taker = taker_tids
                .as_ref()
                .is_some_and(|known_taker_tids| fill.tid.is_some_and(|tid| known_taker_tids.contains(&tid)));

            if emit_rows && has_matching_taker {
                rows.push(OutputRow {
                    block_number: i64::try_from(fill.block_number)?,
                    block_time_ms: fill_time_ms,
                    coin: coin_symbol.to_owned(),
                    user: fill.user.clone(),
                    oid: Some(i64::try_from(fill.oid)?),
                    side: fill.side.clone(),
                    px: fill.px,
                    event: "fill".to_owned(),
                    sz: None,
                    orig_sz: None,
                    raw_sz: None,
                    fill_sz: Some(fill.sz),
                    tif: None,
                    reduce_only: None,
                    lifetime: None,
                });
            }

            let Some(state) = orders.get_mut(&fill.oid) else {
                continue;
            };

            if state.remaining_sz <= Decimal::ZERO {
                remove_live_order(orders, cloid_to_oid, order_expiry_index, fill.oid);
                continue;
            }

            let mut new_remaining = state.remaining_sz - fill.sz;
            if new_remaining < Decimal::ZERO {
                new_remaining = Decimal::ZERO;
            }

            state.remaining_sz = new_remaining;

            if new_remaining <= Decimal::ZERO {
                remove_live_order(orders, cloid_to_oid, order_expiry_index, fill.oid);
            }
        }
        perf_stats.process_block_fill_ns += fills_started_at.elapsed().as_nanos();
    }

    perf_stats.process_block_total_ns += process_block_started_at.elapsed().as_nanos();

    Ok(block_prune_stats)
}

fn cleanup_stale_parquet_tmp_files(output_dir: &Path) -> Result<usize> {
    let mut removed = 0usize;
    for entry in std::fs::read_dir(output_dir)
        .with_context(|| format!("failed to read output directory: {}", output_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", output_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !name.ends_with(".parquet.tmp") {
            continue;
        }
        std::fs::remove_file(&path)
            .with_context(|| format!("failed to remove stale parquet tmp file {}", path.display()))?;
        removed += 1;
    }
    Ok(removed)
}

fn build_output_tmp_path(output_dir: &Path, coin_lower: &str, actual_start_block: u64) -> Result<PathBuf> {
    create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory: {}", output_dir.display()))?;
    let pid = std::process::id();
    let mut candidate =
        output_dir.join(format!(".tmp_{coin_lower}_diff_start{actual_start_block}_pid{pid}.parquet.tmp"));
    if !candidate.exists() {
        return Ok(candidate);
    }
    for idx in 1..=9_999usize {
        candidate =
            output_dir.join(format!(".tmp_{coin_lower}_diff_start{actual_start_block}_pid{pid}_{idx}.parquet.tmp"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("failed to allocate temp parquet file for start block {} under {}", actual_start_block, output_dir.display());
}

fn build_final_output_path(
    output_dir: &Path,
    coin_lower: &str,
    actual_start_block: u64,
    actual_end_block: u64,
) -> Result<PathBuf> {
    create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory: {}", output_dir.display()))?;
    let mut candidate = output_dir.join(format!("{coin_lower}_diff_{actual_start_block}_{actual_end_block}.parquet"));
    if !candidate.exists() {
        return Ok(candidate);
    }
    for idx in 1..=9_999usize {
        candidate =
            output_dir.join(format!("{coin_lower}_diff_{actual_start_block}_{actual_end_block}_dup{idx}.parquet"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!(
        "failed to allocate final parquet file name for range {}..{} under {}",
        actual_start_block,
        actual_end_block,
        output_dir.display()
    );
}

fn build_warmup_state_output_path(output_dir: &Path, checkpoint_block_number: u64) -> Result<PathBuf> {
    create_dir_all(output_dir)
        .with_context(|| format!("failed to create warmup state output directory: {}", output_dir.display()))?;
    Ok(output_dir.join(format!("warmup_state_{checkpoint_block_number}.msgpack.zst")))
}

fn build_upload_key(path: &Path, coin_lower: &str) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    if file_name.ends_with(".parquet") {
        return Some(format!("{UPLOAD_ROOT_PREFIX}/{coin_lower}_diff/{file_name}"));
    }
    if file_name.ends_with(".msgpack.zst") {
        return Some(format!("{UPLOAD_ROOT_PREFIX}/snapshot/{file_name}"));
    }
    None
}

fn snapshot_checkpoint_from_path(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let checkpoint = file_name.strip_prefix("warmup_state_")?.strip_suffix(".msgpack.zst")?;
    checkpoint.parse::<u64>().ok()
}

fn latest_snapshot_path(snapshot_files: &[PathBuf]) -> Option<PathBuf> {
    snapshot_files
        .iter()
        .filter_map(|path| snapshot_checkpoint_from_path(path).map(|checkpoint| (checkpoint, path.clone())))
        .max_by_key(|(checkpoint, _)| *checkpoint)
        .map(|(_, path)| path)
        .or_else(|| snapshot_files.last().cloned())
}

fn spawn_upload_job(
    uploads: &mut JoinSet<Result<(PathBuf, String, bool)>>,
    s3_client: &S3Client,
    local_path: PathBuf,
    remote_key: String,
    keep_local: bool,
) {
    let client = s3_client.clone();
    uploads.spawn(async move {
        let body = ByteStream::from_path(&local_path)
            .await
            .with_context(|| format!("failed to open upload source file {}", local_path.display()))?;
        client.put_object().bucket(UPLOAD_BUCKET).key(&remote_key).body(body).send().await.with_context(|| {
            format!("failed to upload {} to s3://{}/{}", local_path.display(), UPLOAD_BUCKET, remote_key)
        })?;
        Ok((local_path, remote_key, keep_local))
    });
}

fn fill_upload_workers(
    uploads: &mut JoinSet<Result<(PathBuf, String, bool)>>,
    upload_jobs: &mut VecDeque<(PathBuf, String, bool)>,
    s3_client: &S3Client,
) {
    let target_in_flight = UPLOAD_MAX_CONCURRENCY.max(1);
    while uploads.len() < target_in_flight {
        let Some((local_path, remote_key, keep_local)) = upload_jobs.pop_front() else {
            break;
        };
        spawn_upload_job(uploads, s3_client, local_path, remote_key, keep_local);
    }
}

fn missing_snapshot_assets(snapshot: &WarmupStateSnapshot, selected_coins: &[CoinConfig]) -> Vec<&'static str> {
    selected_coins
        .iter()
        .filter_map(|coin| (!snapshot.assets.contains_key(coin.symbol)).then_some(coin.symbol))
        .collect()
}

async fn upload_and_cleanup_generated_files(
    s3_client: &S3Client,
    coin_symbol: &str,
    parquet_files: &[PathBuf],
    snapshot_files: &[PathBuf],
) -> Result<()> {
    let coin_lower = coin_symbol.to_ascii_lowercase();

    let retained_snapshot = latest_snapshot_path(snapshot_files);

    let mut upload_jobs: VecDeque<(PathBuf, String, bool)> = VecDeque::new();

    for path in parquet_files {
        let Some(key) = build_upload_key(path, &coin_lower) else {
            continue;
        };
        upload_jobs.push_back((path.clone(), key, false));
    }

    for path in snapshot_files {
        let Some(key) = build_upload_key(path, &coin_lower) else {
            continue;
        };
        let keep_local = retained_snapshot.as_ref().is_some_and(|retained| retained == path);
        upload_jobs.push_back((path.clone(), key, keep_local));
    }

    let mut uploads = JoinSet::new();
    fill_upload_workers(&mut uploads, &mut upload_jobs, s3_client);

    while let Some(joined) = uploads.join_next().await {
        let (path, _remote_key, keep_local) = joined.context("upload worker join failure")??;
        let file_name = path.file_name().and_then(|value| value.to_str()).unwrap_or("unknown_file");
        info!("[upload] success {}", file_name);
        if !keep_local {
            std::fs::remove_file(&path)
                .with_context(|| format!("failed to remove uploaded local file {}", path.display()))?;
        }
        fill_upload_workers(&mut uploads, &mut upload_jobs, s3_client);
    }

    Ok(())
}

async fn upload_newly_generated_files(
    s3_client: &S3Client,
    coin_symbol: &str,
    generated_output_files: &mut Vec<PathBuf>,
    generated_snapshot_files: &mut Vec<PathBuf>,
    latest_local_snapshot: &mut Option<PathBuf>,
    uploaded_parquet_files: &mut usize,
) -> Result<()> {
    if !generated_output_files.is_empty() {
        let parquet_files = std::mem::take(generated_output_files);
        *uploaded_parquet_files = uploaded_parquet_files.saturating_add(parquet_files.len());
        upload_and_cleanup_generated_files(s3_client, coin_symbol, &parquet_files, &[]).await?;
    }

    if generated_snapshot_files.is_empty() {
        return Ok(());
    }

    let snapshot_files = std::mem::take(generated_snapshot_files);
    let retained_snapshot = latest_snapshot_path(&snapshot_files);
    upload_and_cleanup_generated_files(s3_client, coin_symbol, &[], &snapshot_files).await?;

    if let Some(path) = retained_snapshot
        && let Some(previous) = latest_local_snapshot.replace(path.clone())
        && previous != path
        && previous.is_file()
    {
        std::fs::remove_file(&previous)
            .with_context(|| format!("failed to remove superseded local warmup snapshot {}", previous.display()))?;
    }

    Ok(())
}

fn auto_match_warmup_state_path(
    output_dir: Option<&Path>,
    output_block_range: Option<(u64, u64)>,
    warmup_blocks: u64,
) -> Option<PathBuf> {
    if warmup_blocks == 0 {
        return None;
    }
    let Some((start_height, _)) = output_block_range else {
        return None;
    };
    let Some(dir) = output_dir else {
        return None;
    };
    let checkpoint_block = start_height.saturating_sub(1);
    let candidate = dir.join(format!("warmup_state_{checkpoint_block}.msgpack.zst"));
    candidate.is_file().then_some(candidate)
}

fn write_warmup_state_snapshot(
    output_dir: &Path,
    checkpoint_block_number: u64,
    checkpoint_block_time_ms: i64,
    assets: HashMap<String, WarmupAssetSnapshot>,
) -> Result<PathBuf> {
    let snapshot = WarmupStateSnapshot {
        format_version: WARMUP_STATE_FORMAT_VERSION,
        checkpoint_block_number,
        checkpoint_block_time_ms,
        assets,
    };
    let packed = rmp_serde::to_vec_named(&snapshot).context("serializing warmup state snapshot as msgpack")?;
    let compressed =
        zstd::stream::encode_all(packed.as_slice(), WARMUP_STATE_ZSTD_LEVEL).context("compressing warmup state")?;

    create_dir_all(output_dir)
        .with_context(|| format!("failed to create warmup state output directory: {}", output_dir.display()))?;
    let pid = std::process::id();
    let tmp_path = output_dir.join(format!(".tmp_warmup_state_{checkpoint_block_number}_pid{pid}.msgpack.zst"));
    std::fs::write(&tmp_path, compressed)
        .with_context(|| format!("failed to write warmup state temp file {}", tmp_path.display()))?;

    let final_path = build_warmup_state_output_path(output_dir, checkpoint_block_number)?;
    rename(&tmp_path, &final_path).with_context(|| {
        format!("failed to rename warmup state temp file {} -> {}", tmp_path.display(), final_path.display())
    })?;
    Ok(final_path)
}

fn read_warmup_state_snapshot(path: &Path) -> Result<WarmupStateSnapshot> {
    let compressed =
        std::fs::read(path).with_context(|| format!("failed to read warmup state file {}", path.display()))?;
    let decoded = zstd::stream::decode_all(compressed.as_slice())
        .with_context(|| format!("failed to decompress warmup state file {}", path.display()))?;
    let snapshot: WarmupStateSnapshot = rmp_serde::from_slice(&decoded)
        .with_context(|| format!("invalid warmup state msgpack in {}", path.display()))?;
    if snapshot.format_version != WARMUP_STATE_FORMAT_VERSION {
        bail!(
            "unsupported warmup state format_version={} in {}, expected {}",
            snapshot.format_version,
            path.display(),
            WARMUP_STATE_FORMAT_VERSION
        );
    }
    Ok(snapshot)
}

async fn maybe_write_warmup_state_snapshot(
    output_dir: Option<&Path>,
    block_number: u64,
    block_time_ms: i64,
    in_warmup: bool,
    warmup_just_ended: bool,
    block_prune_stats: BlockPruneStats,
    states: &[CoinRuntimeState],
    generated_snapshot_files: &mut Vec<PathBuf>,
) -> Result<()> {
    let Some(output_dir) = output_dir else {
        return Ok(());
    };
    if in_warmup && !warmup_just_ended {
        return Ok(());
    }
    let hit_file_boundary = block_number % FILE_ROTATION_BLOCKS == 0;
    if !warmup_just_ended && !hit_file_boundary {
        return Ok(());
    }
    let reason = if warmup_just_ended { "warmup_end" } else { "file_boundary_1m" };
    let (live_orders, user_cloid_oid_map_entries, trigger_oids, _buffered_rows) = aggregate_coin_state_metrics(states);
    let output_dir = output_dir.to_path_buf();
    let snapshot_assets = build_multi_asset_warmup_snapshot(states);
    let final_path = tokio::task::spawn_blocking(move || {
        write_warmup_state_snapshot(output_dir.as_path(), block_number, block_time_ms, snapshot_assets)
    })
    .await
    .context("join warmup snapshot export task")??;
    info!(
        "[warmup.state] reason={} checkpoint_block={} checkpoint_time_ms={} purged_live={} purged_trigger={} path={} live_orders={} user_cloid_oid_map_entries={} trigger_oids={}",
        reason,
        block_number,
        block_time_ms,
        block_prune_stats.live_pruned_orders,
        block_prune_stats.trigger_pruned_oids,
        final_path.display(),
        live_orders,
        user_cloid_oid_map_entries,
        trigger_oids,
    );
    generated_snapshot_files.push(final_path);
    Ok(())
}

fn warmup_just_finished(
    warmup_end_logged: bool,
    read_block_range: Option<(u64, u64)>,
    output_block_range: Option<(u64, u64)>,
    block_number: u64,
) -> bool {
    if warmup_end_logged {
        return false;
    }
    let (Some((warmup_start, _)), Some((output_start, _))) = (read_block_range, output_block_range) else {
        return false;
    };
    warmup_start < output_start && block_number == output_start.saturating_sub(1)
}

#[allow(clippy::too_many_arguments)]
fn restore_warmup_state_snapshot(
    snapshot: &WarmupStateSnapshot,
    asset_symbol: &str,
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    order_expiry_index: &mut BTreeSet<(i64, u64)>,
    trigger_refs: &mut TriggerOrderRefs,
) -> Result<()> {
    let checkpoint_time_ms = snapshot.checkpoint_block_time_ms;
    let Some(primary_asset) = snapshot.assets.get(asset_symbol) else {
        orders.clear();
        cloid_to_oid.clear();
        order_expiry_index.clear();
        *trigger_refs = TriggerOrderRefs::default();
        return Ok(());
    };

    *orders = primary_asset.live_orders.clone();
    *cloid_to_oid = primary_asset.live_oid_by_user_cloid.clone();
    *trigger_refs = primary_asset.trigger_refs.clone();

    for &oid in trigger_refs.oid_to_cloid.keys() {
        trigger_refs.oid_created_time_ms.entry(oid).or_insert(checkpoint_time_ms);
    }
    trigger_refs.oid_created_time_ms.retain(|oid, _| trigger_refs.oid_to_cloid.contains_key(oid));
    rebuild_trigger_cloid_index(trigger_refs);
    rebuild_trigger_created_queue(trigger_refs);

    order_expiry_index.clear();
    let rebuild_live_cloid_map = cloid_to_oid.is_empty();
    if rebuild_live_cloid_map {
        cloid_to_oid.clear();
    }
    for (&oid, state) in orders.iter() {
        if rebuild_live_cloid_map && let Some(cloid) = state.cloid.as_deref() {
            cloid_to_oid.entry(state.user.clone()).or_default().insert(cloid.to_owned(), oid);
        }
        order_expiry_index.insert((state.created_time_ms, oid));
    }

    Ok(())
}

fn open_output_writer(path: &Path, schema: &Arc<Schema>) -> Result<ArrowWriter<File>> {
    let file = File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(PARQUET_MAX_ROW_GROUP_ROWS)
        .build();
    ArrowWriter::try_new(file, Arc::clone(schema), Some(props)).context("creating parquet writer")
}

async fn finish_pending_output_flush(state: &mut CoinRuntimeState) -> Result<()> {
    if let Some(task) = state.pending_flush_writer.take() {
        let writer = task.await.context("join output flush task")??;
        state.writer = Some(writer);
    }
    Ok(())
}

async fn collect_finished_output_flush_if_ready(state: &mut CoinRuntimeState) -> Result<()> {
    let should_collect = state.pending_flush_writer.as_ref().is_some_and(JoinHandle::is_finished);
    if should_collect {
        finish_pending_output_flush(state).await?;
    }
    Ok(())
}

async fn dispatch_output_flush_if_needed(state: &mut CoinRuntimeState) -> Result<()> {
    if state.rows.len() == 0 {
        return Ok(());
    }

    finish_pending_output_flush(state).await?;

    let Some(batch) = state.rows.take_batch(&state.schema, state.config.px_scale, state.config.sz_scale)? else {
        return Ok(());
    };

    let writer =
        state.writer.take().ok_or_else(|| anyhow!("missing parquet writer while dispatching row-group flush"))?;
    let coin = state.config.symbol.to_owned();

    state.pending_flush_writer = Some(tokio::task::spawn_blocking(move || -> Result<ArrowWriter<File>> {
        let mut writer = writer;
        writer.write(&batch).with_context(|| format!("writing parquet batch for {}", coin))?;
        writer.flush().with_context(|| format!("flushing parquet writer for {}", coin))?;
        Ok(writer)
    }));
    Ok(())
}

async fn close_current_output_file(
    output_dir: &Path,
    state: &mut CoinRuntimeState,
    fallback_block: u64,
) -> Result<Option<(PathBuf, u64, u64)>> {
    dispatch_output_flush_if_needed(state).await?;
    finish_pending_output_flush(state).await?;

    let Some(active_writer) = state.writer.take() else {
        return Ok(None);
    };

    let closed_start = state.current_file_actual_start_block.unwrap_or(fallback_block);
    let closed_end = state.last_emitted_block.unwrap_or(fallback_block);
    let tmp_path = state
        .current_tmp_output_path
        .take()
        .ok_or_else(|| anyhow!("missing current tmp parquet path while closing output"))?;
    let output_dir = output_dir.to_path_buf();
    let coin_lower = state.coin_lower.clone();

    let final_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        let writer = active_writer;
        writer.close().context("closing parquet writer")?;
        let final_path = build_final_output_path(&output_dir, &coin_lower, closed_start, closed_end)?;
        rename(&tmp_path, &final_path).with_context(|| {
            format!("failed to rename parquet temp file {} -> {}", tmp_path.display(), final_path.display())
        })?;
        Ok(final_path)
    })
    .await
    .context("join output close task")??;

    Ok(Some((final_path, closed_start, closed_end)))
}

async fn ensure_output_ready_for_block(
    output_dir: &Path,
    block_number: u64,
    emit_rows: bool,
    state: &mut CoinRuntimeState,
) -> Result<()> {
    if !emit_rows {
        return Ok(());
    }

    collect_finished_output_flush_if_ready(state).await?;

    let next_file_window = file_window_start_block(block_number);
    let next_row_group_window = row_group_window_start_block(block_number);

    if state.writer.is_none() && state.pending_flush_writer.is_none() {
        let tmp_path = build_output_tmp_path(output_dir, state.coin_lower.as_str(), block_number)?;
        state.writer = Some(open_output_writer(&tmp_path, &state.schema)?);
        state.current_tmp_output_path = Some(tmp_path.clone());
        state.current_file_window_start = Some(next_file_window);
        state.current_row_group_window_start = Some(next_row_group_window);
        state.current_file_actual_start_block = Some(block_number);
        info!("[file] open parquet_tmp={} start_block={}", tmp_path.display(), block_number);
        return Ok(());
    }

    if state.current_file_window_start != Some(next_file_window) {
        if let Some((final_path, closed_start, closed_end)) =
            close_current_output_file(output_dir, state, block_number).await?
        {
            state.generated_output_files.push(final_path.clone());
            info!("[file] close parquet={} actual_range={}..{}", final_path.display(), closed_start, closed_end);
        }

        let tmp_path = build_output_tmp_path(output_dir, state.coin_lower.as_str(), block_number)?;
        state.writer = Some(open_output_writer(&tmp_path, &state.schema)?);
        state.current_tmp_output_path = Some(tmp_path.clone());
        state.current_file_window_start = Some(next_file_window);
        state.current_row_group_window_start = Some(next_row_group_window);
        state.current_file_actual_start_block = Some(block_number);
        info!("[file] open parquet_tmp={} start_block={}", tmp_path.display(), block_number);
        return Ok(());
    }

    if state.current_row_group_window_start != Some(next_row_group_window) {
        dispatch_output_flush_if_needed(state).await?;
        state.current_row_group_window_start = Some(next_row_group_window);
    }

    Ok(())
}

async fn close_open_output_for_coin_state(
    output_dir: &Path,
    state: &mut CoinRuntimeState,
    fallback_block: u64,
) -> Result<()> {
    if let Some((final_path, closed_start, closed_end)) =
        close_current_output_file(output_dir, state, fallback_block).await?
    {
        state.generated_output_files.push(final_path.clone());
        info!("[file] close parquet={} actual_range={}..{}", final_path.display(), closed_start, closed_end);
    }
    Ok(())
}

async fn upload_pending_files_for_states(
    upload_client: Option<&Arc<S3Client>>,
    coin_states: &mut [CoinRuntimeState],
    generated_snapshot_files: &mut Vec<PathBuf>,
    latest_local_uploaded_snapshot: &mut Option<PathBuf>,
) -> Result<()> {
    let Some(client) = upload_client else {
        return Ok(());
    };

    if generated_snapshot_files.is_empty() && coin_states.iter().all(|state| state.generated_output_files.is_empty()) {
        return Ok(());
    }

    for (idx, state) in coin_states.iter_mut().enumerate() {
        if idx == 0 {
            upload_newly_generated_files(
                client.as_ref(),
                state.config.symbol,
                &mut state.generated_output_files,
                generated_snapshot_files,
                latest_local_uploaded_snapshot,
                &mut state.uploaded_parquet_files,
            )
            .await?;
        } else {
            let mut no_snapshot_files = Vec::new();
            upload_newly_generated_files(
                client.as_ref(),
                state.config.symbol,
                &mut state.generated_output_files,
                &mut no_snapshot_files,
                latest_local_uploaded_snapshot,
                &mut state.uploaded_parquet_files,
            )
            .await?;
        }
    }

    Ok(())
}

async fn fetch_next_pair(
    replica_reader: &mut ReplicaPrefetchReader,
    fills_reader: &mut NodeFillsReader,
) -> Result<(Option<ReplicaBatch>, Option<NodeFillsBatch>)> {
    let (next_replica, next_fills) = tokio::try_join!(
        async { replica_reader.next_batch().await.context("replica_reader.next_batch inside fetch_next_pair") },
        async { fills_reader.next_batch().await.context("fills_reader.next_batch inside fetch_next_pair") }
    )?;
    Ok((next_replica, next_fills))
}

#[derive(Default, Debug)]
struct ProcessingPerfStats {
    fetch_pair_ns: u128,
    fetch_replica_only_ns: u128,
    fetch_fills_only_ns: u128,
    ensure_output_ns: u128,
    process_block_total_ns: u128,
    process_block_prune_ns: u128,
    process_block_cmd_ns: u128,
    process_block_fill_ns: u128,
}

impl ProcessingPerfStats {
    fn ns_to_secs(ns: u128) -> f64 {
        ns as f64 / 1_000_000_000.0
    }

    fn pct_of_total(ns: u128, total_secs: f64) -> f64 {
        if total_secs <= 0.0 { 0.0 } else { (Self::ns_to_secs(ns) / total_secs) * 100.0 }
    }

    fn log_summary(&self, processed_blocks: u64, total_elapsed_secs: f64) {
        info!(
            "[perf] blocks={} elapsed={:.1}s read_pair={:.1}s({:.1}%) read_replica_only={:.1}s read_fills_only={:.1}s ensure_output={:.1}s({:.1}%) process_block={:.1}s({:.1}%) prune={:.1}s cmd={:.1}s fills={:.1}s",
            processed_blocks,
            total_elapsed_secs,
            Self::ns_to_secs(self.fetch_pair_ns),
            Self::pct_of_total(self.fetch_pair_ns, total_elapsed_secs),
            Self::ns_to_secs(self.fetch_replica_only_ns),
            Self::ns_to_secs(self.fetch_fills_only_ns),
            Self::ns_to_secs(self.ensure_output_ns),
            Self::pct_of_total(self.ensure_output_ns, total_elapsed_secs),
            Self::ns_to_secs(self.process_block_total_ns),
            Self::pct_of_total(self.process_block_total_ns, total_elapsed_secs),
            Self::ns_to_secs(self.process_block_prune_ns),
            Self::ns_to_secs(self.process_block_cmd_ns),
            Self::ns_to_secs(self.process_block_fill_ns),
        );
    }

    fn pct_of_reader(ns: u128, reader_total_ns: u128) -> f64 {
        if reader_total_ns == 0 { 0.0 } else { (ns as f64 / reader_total_ns as f64) * 100.0 }
    }

    fn log_reader_breakdown(label: &str, perf: ReaderReadPerfStats) {
        let open_other_ns = perf.open_non_s3_ns();
        let reader_total_ns = perf.instrumented_total_ns();
        info!(
            "[perf.read_pair] reader={} cpu_sum_total={:.1}s s3_get={:.1}s({:.1}%) read_line_lz4={:.1}s({:.1}%) json_parse={:.1}s({:.1}%) open_other={:.1}s({:.1}%) s3_get_calls={} line_reads={} json_parses={} line_bytes={}",
            label,
            Self::ns_to_secs(reader_total_ns),
            Self::ns_to_secs(perf.s3_get_ns),
            Self::pct_of_reader(perf.s3_get_ns, reader_total_ns),
            Self::ns_to_secs(perf.line_read_ns),
            Self::pct_of_reader(perf.line_read_ns, reader_total_ns),
            Self::ns_to_secs(perf.json_parse_ns),
            Self::pct_of_reader(perf.json_parse_ns, reader_total_ns),
            Self::ns_to_secs(open_other_ns),
            Self::pct_of_reader(open_other_ns, reader_total_ns),
            perf.s3_get_calls,
            perf.line_read_calls,
            perf.json_parse_calls,
            perf.line_read_bytes,
        );
    }

    fn log_read_pair_breakdown(&self, replica_perf: ReaderReadPerfStats, fills_perf: ReaderReadPerfStats) {
        let replica_total_ns = replica_perf.instrumented_total_ns();
        let fills_total_ns = fills_perf.instrumented_total_ns();
        info!(
            "[perf.read_pair] wall_read_pair={:.1}s replica_instrumented_cpu_sum={:.1}s fills_instrumented_cpu_sum={:.1}s combined_cpu_sum={:.1}s",
            Self::ns_to_secs(self.fetch_pair_ns),
            Self::ns_to_secs(replica_total_ns),
            Self::ns_to_secs(fills_total_ns),
            Self::ns_to_secs(replica_total_ns + fills_total_ns),
        );
        Self::log_reader_breakdown("replica_cmds", replica_perf);
        Self::log_reader_breakdown("node_fills", fills_perf);
    }
}

fn cloid_mapping_entry_count(cloid_to_oid: &HashMap<String, HashMap<String, u64>>) -> usize {
    cloid_to_oid.values().map(std::collections::HashMap::len).sum()
}

fn format_u64_thousands(value: u64) -> String {
    let raw = value.to_string();
    let mut out = String::with_capacity(raw.len() + raw.len() / 3);
    let raw_len = raw.len();
    for (idx, ch) in raw.chars().enumerate() {
        out.push(ch);
        let remaining = raw_len - idx - 1;
        if remaining > 0 && remaining % 3 == 0 {
            out.push(',');
        }
    }
    out
}

fn format_usize_thousands(value: usize) -> String {
    format_u64_thousands(u64::try_from(value).unwrap_or(u64::MAX))
}

fn format_weekday_short(weekday: chrono::Weekday) -> &'static str {
    match weekday {
        chrono::Weekday::Mon => "Mon.",
        chrono::Weekday::Tue => "Tue.",
        chrono::Weekday::Wed => "Wed.",
        chrono::Weekday::Thu => "Thu.",
        chrono::Weekday::Fri => "Fri.",
        chrono::Weekday::Sat => "Sat.",
        chrono::Weekday::Sun => "Sun.",
    }
}

fn format_timestamp_utc(block_time_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(block_time_ms)
        .map(|value| format!("{} {}", value.to_rfc3339(), format_weekday_short(value.weekday())))
        .unwrap_or_else(|| "invalid_timestamp".to_owned())
}

fn format_elapsed_clock(total_secs: f64) -> String {
    if !total_secs.is_finite() {
        return "invalid_duration".to_owned();
    }
    let total_tenths = (total_secs.max(0.0) * 10.0).round() as u64;
    let total_seconds = total_tenths / 10;
    let tenths = total_tenths % 10;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}.{tenths}")
}

fn log_processing_progress(
    processed_blocks: u64,
    block_number: u64,
    block_time_ms: i64,
    live_orders: usize,
    cloid_map_entries: usize,
    trigger_oids: usize,
    buffered_rows: usize,
    pruned_live_orders: u64,
    pruned_trigger_oids: u64,
    elapsed_secs: f64,
    total_secs: f64,
    in_warmup: bool,
) {
    let phase = if in_warmup { "[warmup]" } else { "[progress]" };
    let block_time = format_timestamp_utc(block_time_ms);
    let block_window = block_number / ROW_GROUP_BLOCKS;
    info!(
        "{} blocks={} last={} time={} live={}|-{} cloid={} trigger={}|-{} rows={} elapsed={:.1}s total={}",
        phase,
        format_u64_thousands(processed_blocks),
        format_u64_thousands(block_window),
        block_time,
        format_usize_thousands(live_orders),
        format_u64_thousands(pruned_live_orders),
        format_usize_thousands(cloid_map_entries),
        format_usize_thousands(trigger_oids),
        format_u64_thousands(pruned_trigger_oids),
        format_usize_thousands(buffered_rows),
        elapsed_secs,
        format_elapsed_clock(total_secs)
    );
}

struct CoinRuntimeState {
    config: CoinConfig,
    coin_lower: String,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    pending_flush_writer: Option<JoinHandle<Result<ArrowWriter<File>>>>,
    current_tmp_output_path: Option<PathBuf>,
    current_file_window_start: Option<u64>,
    current_row_group_window_start: Option<u64>,
    current_file_actual_start_block: Option<u64>,
    last_emitted_block: Option<u64>,
    generated_output_files: Vec<PathBuf>,
    rows: RowBuffer,
    orders: HashMap<u64, OrderState>,
    cloid_to_oid: HashMap<String, HashMap<String, u64>>,
    order_expiry_index: BTreeSet<(i64, u64)>,
    trigger_refs: TriggerOrderRefs,
    unknown_oid_logger: UnknownOidWindowLogger,
    uploaded_parquet_files: usize,
}

fn aggregate_coin_state_metrics(states: &[CoinRuntimeState]) -> (usize, usize, usize, usize) {
    let live_orders = states.iter().map(|state| state.orders.len()).sum();
    let cloid_entries = states.iter().map(|state| cloid_mapping_entry_count(&state.cloid_to_oid)).sum();
    let trigger_oids = states.iter().map(|state| state.trigger_refs.oid_to_cloid.len()).sum();
    let buffered_rows = states.iter().map(|state| state.rows.len()).sum();
    (live_orders, cloid_entries, trigger_oids, buffered_rows)
}

fn build_multi_asset_warmup_snapshot(states: &[CoinRuntimeState]) -> HashMap<String, WarmupAssetSnapshot> {
    let mut assets = HashMap::new();
    for state in states {
        assets.insert(
            state.config.symbol.to_owned(),
            WarmupAssetSnapshot {
                live_orders: state.orders.clone(),
                live_oid_by_user_cloid: state.cloid_to_oid.clone(),
                trigger_refs: state.trigger_refs.clone(),
            },
        );
    }
    assets
}

fn split_cmd_events_by_coin_state(
    events: Vec<CmdEvent>,
    coin_state_idx_by_asset: &HashMap<u64, usize>,
    coin_state_count: usize,
) -> Vec<Vec<CmdEvent>> {
    let mut events_by_coin_state = (0..coin_state_count).map(|_| Vec::new()).collect::<Vec<_>>();
    for event in events {
        if let Some(state_idx) = coin_state_idx_by_asset.get(&event.asset_id()).copied() {
            events_by_coin_state[state_idx].push(event);
        }
    }
    events_by_coin_state
}

fn split_fills_by_coin_state(
    fills: Vec<FillRecord>,
    coin_state_idx_by_asset: &HashMap<u64, usize>,
    coin_state_count: usize,
) -> Vec<Vec<FillRecord>> {
    let mut fills_by_coin_state = (0..coin_state_count).map(|_| Vec::new()).collect::<Vec<_>>();
    for fill in fills {
        if let Some(state_idx) = coin_state_idx_by_asset.get(&fill.asset_id).copied() {
            fills_by_coin_state[state_idx].push(fill);
        }
    }
    fills_by_coin_state
}

#[tokio::main]
async fn main() -> Result<()> {
    Builder::from_env(Env::default().default_filter_or("info")).format_timestamp_micros().init();

    let args = Args::parse();
    let selected_coins = parse_coin_configs(&args.coin)?;
    let selected_coin_text = selected_coins.iter().map(|coin| coin.symbol).collect::<Vec<_>>().join(",");
    let selected_asset_ids: HashSet<u64> = selected_coins.iter().map(|coin| coin.asset_id).collect();
    let requested_json_workers = args.replica_json_workers;
    if requested_json_workers == 0 {
        bail!("--json must be >= 1");
    }
    let replica_json_workers = requested_json_workers.min(MAX_REPLICA_JSON_WORKERS);
    if requested_json_workers > MAX_REPLICA_JSON_WORKERS {
        warn!("[config] json_workers={} exceeds max {}; clamping", requested_json_workers, MAX_REPLICA_JSON_WORKERS);
    }
    let replica_queue_depth = DEFAULT_REPLICA_JSON_QUEUE_DEPTH;
    let replica_lz4_workers = args.replica_lz4_workers;
    if replica_lz4_workers == 0 {
        bail!("--lz4 must be >= 1");
    }
    let replica_s3_range_workers = args.replica_s3_range_workers;
    if replica_s3_range_workers == 0 {
        bail!("--s3 must be >= 1");
    }
    let replica_prefetch_buffer_mb = args.replica_prefetch_buffer_mb;
    if replica_prefetch_buffer_mb == 0 {
        bail!("--replica-prefetch-buffer-mb must be >= 1");
    }
    let requester_pays = REQUESTER_PAYS_ALWAYS;
    let startup_started_at = Instant::now();
    info!(
        "[config] json_parser=simd-json replica_queue_depth={} replica_prefetch_buffer_mb={}",
        replica_queue_depth, replica_prefetch_buffer_mb
    );
    info!("[config.coin] selected={} count={}", selected_coin_text, selected_coins.len());
    for coin in &selected_coins {
        info!(
            "[config.coin.detail] symbol={} asset_id={} px_scale={} sz_scale={}",
            coin.symbol, coin.asset_id, coin.px_scale, coin.sz_scale
        );
    }
    info!("[config.upload] enabled={} mode=incremental_parquet_and_snapshot", args.upload);
    if args.start.is_some() ^ args.span.is_some() {
        bail!("--start and --span must be provided together");
    }
    let output_block_range = match (args.start, args.span) {
        (Some(start_height), Some(height_span)) => {
            if height_span == 0 {
                bail!("--span must be > 0");
            }
            let end_height = start_height
                .checked_add(height_span - 1)
                .ok_or_else(|| anyhow!("height overflow: start_height={start_height} span={height_span}"))?;
            Some((start_height, end_height))
        }
        _ => None,
    };
    let warmup_blocks = args.warmup.unwrap_or(0);
    let mut selected_warmup_state_path = args.warmup_state_file.clone();
    let mut auto_matched_warmup_state = false;
    if selected_warmup_state_path.is_none() {
        selected_warmup_state_path =
            auto_match_warmup_state_path(args.warmup_state_output_dir.as_deref(), output_block_range, warmup_blocks);
        auto_matched_warmup_state = selected_warmup_state_path.is_some();
    }

    let mut warmup_state_snapshot = if let Some(path) = selected_warmup_state_path.as_deref() {
        let source_flag =
            if auto_matched_warmup_state { "auto-matched local warmup state" } else { "--warmup-state-file" };
        Some(
            read_warmup_state_snapshot(path)
                .with_context(|| format!("failed to load {} {}", source_flag, path.display()))?,
        )
    } else {
        None
    };

    if let Some(snapshot) = warmup_state_snapshot.as_ref() {
        let missing_assets = missing_snapshot_assets(snapshot, &selected_coins);
        if !missing_assets.is_empty() {
            if let Some(path) = selected_warmup_state_path.as_deref() {
                warn!(
                    "[warmup.state] ignore_snapshot_missing_assets file={} missing_assets={} selected={} checkpoint_block={}",
                    path.display(),
                    missing_assets.join(","),
                    selected_coin_text,
                    snapshot.checkpoint_block_number
                );
            }
            warmup_state_snapshot = None;
            selected_warmup_state_path = None;
            auto_matched_warmup_state = false;
        }
    }

    let mut read_block_range = output_block_range.map(|(start_height, end_height)| {
        let warmup_start = start_height.saturating_sub(warmup_blocks).max(1);
        (warmup_start, end_height)
    });

    if let Some(snapshot) = warmup_state_snapshot.as_ref() {
        let Some((output_start, output_end)) = output_block_range else {
            bail!("--warmup-state-file requires --start and --span");
        };
        let resume_start = snapshot.checkpoint_block_number.saturating_add(1).max(1);
        if resume_start > output_start {
            bail!(
                "warmup state checkpoint block {} is ahead of output start {}",
                snapshot.checkpoint_block_number,
                output_start
            );
        }
        if resume_start > output_end {
            bail!(
                "warmup state checkpoint block {} implies replay start {} > output end {}",
                snapshot.checkpoint_block_number,
                resume_start,
                output_end
            );
        }
        read_block_range = Some((resume_start, output_end));
        if let Some(path) = selected_warmup_state_path.as_deref() {
            let warmup_skipped = resume_start >= output_start;
            let source = if auto_matched_warmup_state { "auto_local_match" } else { "explicit" };
            info!(
                "[warmup.state] using_local_snapshot source={} file={} checkpoint_block={} replay_start={} output_start={} warmup_skipped={}",
                source,
                path.display(),
                snapshot.checkpoint_block_number,
                resume_start,
                output_start,
                warmup_skipped
            );
        }
    }

    let mut warmup_end_logged = false;
    if let (Some((warmup_start, _)), Some((output_start, _))) = (read_block_range, output_block_range)
        && warmup_start < output_start
    {
        info!(
            "[warmup] start warmup_blocks={} warmup_range={}..{} output_start={}",
            output_start - warmup_start,
            warmup_start,
            output_start - 1,
            output_start
        );
    }

    let replica_source = InputSource::parse(&args.replica_cmds).context("parsing --replica_cmds")?;
    let node_fills_source = InputSource::parse(&args.node_fills_by_block).context("parsing --node_fills_by_block")?;
    let uses_s3 = replica_source.requires_s3() || node_fills_source.requires_s3() || args.upload;
    let mut startup_aws_config_secs: Option<f64> = None;
    let mut startup_replica_keys_resolve_secs: Option<f64> = None;
    let mut startup_replica_keys_count: Option<usize> = None;
    let s3_client = if uses_s3 {
        let aws_config_started_at = Instant::now();
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
        startup_aws_config_secs = Some(aws_config_started_at.elapsed().as_secs_f64());
        Some(Arc::new(S3Client::new(&config)))
    } else {
        None
    };
    let upload_client = if args.upload {
        Some(s3_client.as_ref().ok_or_else(|| anyhow!("--upload requires initialized S3 client"))?.clone())
    } else {
        None
    };

    let replica_reader_source = match &replica_source {
        InputSource::LocalFile(path) => {
            if output_block_range.is_some() {
                bail!("--start/--span currently require S3 replica_cmds prefix input");
            }
            ReaderSource::LocalFile(path.clone())
        }
        InputSource::S3Prefix { bucket, prefix } => {
            let Some((start_height, end_height)) = read_block_range else {
                bail!("S3 replica_cmds requires --start and --span");
            };
            let height_span = end_height - start_height + 1;
            let Some(client) = s3_client.as_ref() else {
                bail!("S3 replica_cmds requires initialized S3 client");
            };
            let resolve_replica_keys_started_at = Instant::now();
            let keys =
                resolve_replica_keys_for_range(client, bucket, prefix, start_height, height_span, requester_pays)
                    .await
                    .context("resolving replica_cmds keys from S3 layout")?;
            startup_replica_keys_resolve_secs = Some(resolve_replica_keys_started_at.elapsed().as_secs_f64());
            startup_replica_keys_count = Some(keys.len());
            ReaderSource::S3Keys { bucket: bucket.clone(), keys }
        }
    };

    let fills_reader_source = match &node_fills_source {
        InputSource::LocalFile(path) => ReaderSource::LocalFile(path.clone()),
        InputSource::S3Prefix { bucket, prefix } => {
            let first_replica_ts_ms =
                first_replica_timestamp_ms(replica_reader_source.clone(), s3_client.clone(), requester_pays)
                    .await
                    .context("reading first replica_cmds timestamp")?;
            ReaderSource::S3HourlyFrom {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                next_hour_ms: floor_hour_ms(first_replica_ts_ms),
            }
        }
    };

    let mut fills_reader = NodeFillsReader::new(
        fills_reader_source.clone(),
        s3_client.clone(),
        requester_pays,
        read_block_range,
        selected_asset_ids.clone(),
    )
    .await
    .context("opening node_fills_by_block")?;
    let mut replica_reader = ReplicaPrefetchReader::new(
        replica_reader_source,
        s3_client.clone(),
        requester_pays,
        selected_asset_ids,
        replica_queue_depth,
        replica_json_workers,
        replica_lz4_workers,
        replica_s3_range_workers,
        replica_prefetch_buffer_mb,
    )
    .await
    .context("opening replica_cmds with prefetch workers")?;

    if args.output_parquet.exists() && !args.output_parquet.is_dir() {
        bail!(
            "--output-parquet now expects a directory path, but got existing non-directory: {}",
            args.output_parquet.display()
        );
    }
    create_dir_all(&args.output_parquet)
        .with_context(|| format!("failed to create output parquet directory: {}", args.output_parquet.display()))?;
    let cleaned_stale_tmp_files = cleanup_stale_parquet_tmp_files(&args.output_parquet)?;
    if cleaned_stale_tmp_files > 0 {
        info!(
            "[startup] cleaned_stale_parquet_tmp_files={} dir={}",
            cleaned_stale_tmp_files,
            args.output_parquet.display()
        );
    }
    if let Some(parent) = args.unknown_oid_log_sqlite.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)
            .with_context(|| format!("failed to create unknown oid sqlite parent directory: {}", parent.display()))?;
    }
    if let Some(path) = args.warmup_state_output_dir.as_deref() {
        create_dir_all(path)
            .with_context(|| format!("failed to create warmup state output directory: {}", path.display()))?;
    }
    let mut unknown_oid_sqlite_worker = if DISABLE_UNKNOWN_OID_SQLITE {
        None
    } else {
        Some(UnknownOidSqliteWorker::start(&args.unknown_oid_log_sqlite)?)
    };
    let unknown_oid_sqlite_sender = unknown_oid_sqlite_worker.as_ref().map(|worker| worker.sender());
    let mut generated_snapshot_files: Vec<PathBuf> = Vec::new();
    let mut latest_local_uploaded_snapshot = selected_warmup_state_path.clone();

    let mut coin_states = Vec::with_capacity(selected_coins.len());
    for coin in selected_coins {
        let mut state = CoinRuntimeState {
            config: coin,
            coin_lower: coin.symbol.to_ascii_lowercase(),
            schema: output_schema(coin.px_scale, coin.sz_scale),
            writer: None,
            pending_flush_writer: None,
            current_tmp_output_path: None,
            current_file_window_start: None,
            current_row_group_window_start: None,
            current_file_actual_start_block: None,
            last_emitted_block: None,
            generated_output_files: Vec::new(),
            rows: RowBuffer::default(),
            orders: HashMap::new(),
            cloid_to_oid: HashMap::new(),
            order_expiry_index: BTreeSet::new(),
            trigger_refs: TriggerOrderRefs::default(),
            unknown_oid_logger: UnknownOidWindowLogger::new(coin.symbol, unknown_oid_sqlite_sender.clone()),
            uploaded_parquet_files: 0,
        };

        if let Some(snapshot) = warmup_state_snapshot.as_ref() {
            let checkpoint_block_number = snapshot.checkpoint_block_number;
            restore_warmup_state_snapshot(
                snapshot,
                coin.symbol,
                &mut state.orders,
                &mut state.cloid_to_oid,
                &mut state.order_expiry_index,
                &mut state.trigger_refs,
            )?;
            info!(
                "[warmup.state] restored coin={} checkpoint_block={} live_orders={} cloid={} trigger={}",
                coin.symbol,
                checkpoint_block_number,
                state.orders.len(),
                cloid_mapping_entry_count(&state.cloid_to_oid),
                state.trigger_refs.oid_to_cloid.len()
            );
        }

        coin_states.push(state);
    }

    let coin_state_idx_by_asset: HashMap<u64, usize> =
        coin_states.iter().enumerate().map(|(idx, state)| (state.config.asset_id, idx)).collect();

    let started_at = Instant::now();
    let mut processed_blocks_total: u64 = 0;
    let mut warmup_processed_blocks: u64 = 0;
    let mut progress_processed_blocks: u64 = 0;
    let warmup_phase_started_at = Instant::now();
    let mut warmup_interval_started_at = warmup_phase_started_at;
    let mut progress_phase_started_at = Instant::now();
    let mut progress_interval_started_at = progress_phase_started_at;
    let mut warmup_pruned_live_orders_interval: u64 = 0;
    let mut warmup_pruned_trigger_oids_interval: u64 = 0;
    let mut progress_pruned_live_orders_interval: u64 = 0;
    let mut progress_pruned_trigger_oids_interval: u64 = 0;
    let mut last_seen_fills_ts_ns: Option<i64> = None;
    let mut last_seen_fills_block_number: Option<u64> = None;
    let mut perf_stats = ProcessingPerfStats::default();
    let startup_aws_config =
        startup_aws_config_secs.map(|secs| format!("{secs:.3}s")).unwrap_or_else(|| "-".to_owned());
    let startup_replica_keys_resolve =
        startup_replica_keys_resolve_secs.map(|secs| format!("{secs:.3}s")).unwrap_or_else(|| "-".to_owned());
    let startup_replica_keys =
        startup_replica_keys_count.map(|count| count.to_string()).unwrap_or_else(|| "-".to_owned());
    let read_range_text = read_block_range
        .map(|(start_height, end_height)| format!("{start_height}..{end_height}"))
        .unwrap_or_else(|| "all".to_owned());
    let output_range_text = output_block_range
        .map(|(start_height, end_height)| format!("{start_height}..{end_height}"))
        .unwrap_or_else(|| "all".to_owned());
    info!(
        "[startup] ready_for_processing elapsed_total={:.3}s s3={} aws_cfg={} replica_keys={} key_resolve={} reader_workers={} cleaned_tmp={} read_range={} output_range={}",
        startup_started_at.elapsed().as_secs_f64(),
        uses_s3,
        startup_aws_config,
        startup_replica_keys,
        startup_replica_keys_resolve,
        replica_reader.worker_count(),
        cleaned_stale_tmp_files,
        read_range_text,
        output_range_text
    );
    info!("[phase] processing blocks ...");
    let fetch_pair_started_at = Instant::now();
    let (mut next_replica, mut next_fills) = fetch_next_pair(&mut replica_reader, &mut fills_reader)
        .await
        .with_context(|| {
            format!(
                "initial fetch_next_pair failed last_seen_fills_block={}",
                last_seen_fills_block_number
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned())
            )
        })?;
    perf_stats.fetch_pair_ns += fetch_pair_started_at.elapsed().as_nanos();

    while next_replica.is_some() || next_fills.is_some() {
        match (&next_replica, &next_fills) {
            (Some(replica), Some(fills)) => {
                let delta_ns = (replica.block_time_ns - fills.block_time_ns).abs();
                if delta_ns <= BLOCK_TIME_TOLERANCE_NS {
                    let block_time_delta = (replica.block_time_ms - fills.block_time_ms).abs();
                    if block_time_delta > BLOCK_TIME_MATCH_TOLERANCE_MS {
                        for state in &mut coin_states {
                            state.unknown_oid_logger.finish()?;
                            close_open_output_for_coin_state(&args.output_parquet, state, fills.block_number).await?;
                        }
                        upload_pending_files_for_states(
                            upload_client.as_ref(),
                            &mut coin_states,
                            &mut generated_snapshot_files,
                            &mut latest_local_uploaded_snapshot,
                        )
                        .await?;
                        bail!(
                            "block {} timestamp mismatch: replica={}ms fills={}ms delta={}ms tolerance={}ms",
                            fills.block_number,
                            replica.block_time_ms,
                            fills.block_time_ms,
                            block_time_delta,
                            BLOCK_TIME_MATCH_TOLERANCE_MS
                        );
                    }

                    let replica = next_replica.take().unwrap();
                    let fills = next_fills.take().unwrap();
                    let ReplicaBatch { events, .. } = replica;
                    let NodeFillsBatch {
                        block_number,
                        block_time_ns: fills_block_time_ns,
                        block_time_ms: fills_block_time_ms,
                        fills,
                    } = fills;
                    let cmd_events_by_coin_state =
                        split_cmd_events_by_coin_state(events, &coin_state_idx_by_asset, coin_states.len());
                    let fills_by_coin_state =
                        split_fills_by_coin_state(fills, &coin_state_idx_by_asset, coin_states.len());
                    last_seen_fills_ts_ns = Some(fills_block_time_ns);
                    last_seen_fills_block_number = Some(block_number);
                    let emit_rows =
                        output_block_range.map(|(start_height, _)| block_number >= start_height).unwrap_or(true);

                    let mut block_prune_stats = BlockPruneStats::default();
                    let needs_maintenance = block_number == row_group_window_start_block(block_number);
                    for (state_idx, state) in coin_states.iter_mut().enumerate() {
                        let ensure_output_started_at = Instant::now();
                        ensure_output_ready_for_block(&args.output_parquet, block_number, emit_rows, state).await?;
                        perf_stats.ensure_output_ns += ensure_output_started_at.elapsed().as_nanos();

                        let cmd_events_slice = {
                            let entries = &cmd_events_by_coin_state[state_idx];
                            (!entries.is_empty()).then_some(entries.as_slice())
                        };
                        let fills_slice = {
                            let entries = &fills_by_coin_state[state_idx];
                            (!entries.is_empty()).then_some(entries.as_slice())
                        };

                        if cmd_events_slice.is_none() && fills_slice.is_none() && !needs_maintenance {
                            if emit_rows {
                                state.last_emitted_block = Some(block_number);
                            }
                            continue;
                        }

                        let coin_prune_stats = process_block(
                            state.config.symbol,
                            state.config.asset_id,
                            block_number,
                            fills_block_time_ms,
                            cmd_events_slice,
                            fills_slice,
                            emit_rows,
                            emit_rows,
                            &mut state.rows,
                            &mut state.orders,
                            &mut state.cloid_to_oid,
                            &mut state.order_expiry_index,
                            &mut state.trigger_refs,
                            &mut state.unknown_oid_logger,
                            &mut perf_stats,
                        )?;
                        block_prune_stats.live_pruned_orders =
                            block_prune_stats.live_pruned_orders.saturating_add(coin_prune_stats.live_pruned_orders);
                        block_prune_stats.trigger_pruned_oids =
                            block_prune_stats.trigger_pruned_oids.saturating_add(coin_prune_stats.trigger_pruned_oids);

                        if emit_rows {
                            state.last_emitted_block = Some(block_number);
                        }
                    }

                    let warmup_just_ended =
                        warmup_just_finished(warmup_end_logged, read_block_range, output_block_range, block_number);
                    if warmup_just_ended {
                        let output_start = output_block_range
                            .map(|(start_height, _)| start_height)
                            .unwrap_or(block_number.saturating_add(1));
                        info!("[warmup] end at block={} (output starts from block {})", block_number, output_start);
                        progress_processed_blocks = 0;
                        progress_phase_started_at = Instant::now();
                        progress_interval_started_at = progress_phase_started_at;
                        progress_pruned_live_orders_interval = 0;
                        progress_pruned_trigger_oids_interval = 0;
                        warmup_end_logged = true;
                    }

                    processed_blocks_total += 1;
                    if !emit_rows {
                        warmup_processed_blocks += 1;
                        warmup_pruned_live_orders_interval += block_prune_stats.live_pruned_orders;
                        warmup_pruned_trigger_oids_interval += block_prune_stats.trigger_pruned_oids;
                    } else {
                        progress_processed_blocks += 1;
                        progress_pruned_live_orders_interval += block_prune_stats.live_pruned_orders;
                        progress_pruned_trigger_oids_interval += block_prune_stats.trigger_pruned_oids;
                    }

                    if !emit_rows && warmup_processed_blocks % PROGRESS_LOG_INTERVAL_BLOCKS == 0 {
                        let (live_orders, cloid_map_entries, trigger_oids, buffered_rows) =
                            aggregate_coin_state_metrics(&coin_states);
                        let elapsed_secs = warmup_interval_started_at.elapsed().as_secs_f64();
                        let total_secs = warmup_phase_started_at.elapsed().as_secs_f64();
                        log_processing_progress(
                            warmup_processed_blocks,
                            block_number,
                            fills_block_time_ms,
                            live_orders,
                            cloid_map_entries,
                            trigger_oids,
                            buffered_rows,
                            warmup_pruned_live_orders_interval,
                            warmup_pruned_trigger_oids_interval,
                            elapsed_secs,
                            total_secs,
                            true,
                        );
                        warmup_interval_started_at = Instant::now();
                        warmup_pruned_live_orders_interval = 0;
                        warmup_pruned_trigger_oids_interval = 0;
                    }

                    if emit_rows && progress_processed_blocks % PROGRESS_LOG_INTERVAL_BLOCKS == 0 {
                        let (live_orders, cloid_map_entries, trigger_oids, buffered_rows) =
                            aggregate_coin_state_metrics(&coin_states);
                        let elapsed_secs = progress_interval_started_at.elapsed().as_secs_f64();
                        let total_secs = progress_phase_started_at.elapsed().as_secs_f64();
                        log_processing_progress(
                            progress_processed_blocks,
                            block_number,
                            fills_block_time_ms,
                            live_orders,
                            cloid_map_entries,
                            trigger_oids,
                            buffered_rows,
                            progress_pruned_live_orders_interval,
                            progress_pruned_trigger_oids_interval,
                            elapsed_secs,
                            total_secs,
                            false,
                        );
                        progress_interval_started_at = Instant::now();
                        progress_pruned_live_orders_interval = 0;
                        progress_pruned_trigger_oids_interval = 0;
                    }

                    maybe_write_warmup_state_snapshot(
                        args.warmup_state_output_dir.as_deref(),
                        block_number,
                        fills_block_time_ms,
                        !emit_rows,
                        warmup_just_ended,
                        block_prune_stats,
                        &coin_states,
                        &mut generated_snapshot_files,
                    )
                    .await?;

                    upload_pending_files_for_states(
                        upload_client.as_ref(),
                        &mut coin_states,
                        &mut generated_snapshot_files,
                        &mut latest_local_uploaded_snapshot,
                    )
                    .await?;

                    let fetch_pair_started_at = Instant::now();
                    let pair = fetch_next_pair(&mut replica_reader, &mut fills_reader).await.with_context(|| {
                        format!(
                            "fetch_next_pair failed last_seen_fills_block={}",
                            last_seen_fills_block_number
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "none".to_owned())
                        )
                    })?;
                    perf_stats.fetch_pair_ns += fetch_pair_started_at.elapsed().as_nanos();
                    next_replica = pair.0;
                    next_fills = pair.1;
                    continue;
                }

                if replica.block_time_ns < fills.block_time_ns {
                    let fetch_replica_started_at = Instant::now();
                    next_replica = replica_reader.next_batch().await.with_context(|| {
                        format!(
                            "replica_reader.next_batch failed last_seen_fills_block={}",
                            last_seen_fills_block_number
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "none".to_owned())
                        )
                    })?;
                    perf_stats.fetch_replica_only_ns += fetch_replica_started_at.elapsed().as_nanos();
                    continue;
                }

                let fills = next_fills.take().unwrap();
                let NodeFillsBatch {
                    block_number,
                    block_time_ns: fills_block_time_ns,
                    block_time_ms: fills_block_time_ms,
                    fills,
                } = fills;
                let fills_by_coin_state = split_fills_by_coin_state(fills, &coin_state_idx_by_asset, coin_states.len());
                last_seen_fills_ts_ns = Some(fills_block_time_ns);
                last_seen_fills_block_number = Some(block_number);
                let emit_rows =
                    output_block_range.map(|(start_height, _)| block_number >= start_height).unwrap_or(true);

                let mut block_prune_stats = BlockPruneStats::default();
                let needs_maintenance = block_number == row_group_window_start_block(block_number);
                for (state_idx, state) in coin_states.iter_mut().enumerate() {
                    let ensure_output_started_at = Instant::now();
                    ensure_output_ready_for_block(&args.output_parquet, block_number, emit_rows, state).await?;
                    perf_stats.ensure_output_ns += ensure_output_started_at.elapsed().as_nanos();

                    let fills_slice = {
                        let entries = &fills_by_coin_state[state_idx];
                        (!entries.is_empty()).then_some(entries.as_slice())
                    };

                    if fills_slice.is_none() && !needs_maintenance {
                        if emit_rows {
                            state.last_emitted_block = Some(block_number);
                        }
                        continue;
                    }

                    let coin_prune_stats = process_block(
                        state.config.symbol,
                        state.config.asset_id,
                        block_number,
                        fills_block_time_ms,
                        None,
                        fills_slice,
                        emit_rows,
                        emit_rows,
                        &mut state.rows,
                        &mut state.orders,
                        &mut state.cloid_to_oid,
                        &mut state.order_expiry_index,
                        &mut state.trigger_refs,
                        &mut state.unknown_oid_logger,
                        &mut perf_stats,
                    )?;
                    block_prune_stats.live_pruned_orders =
                        block_prune_stats.live_pruned_orders.saturating_add(coin_prune_stats.live_pruned_orders);
                    block_prune_stats.trigger_pruned_oids =
                        block_prune_stats.trigger_pruned_oids.saturating_add(coin_prune_stats.trigger_pruned_oids);

                    if emit_rows {
                        state.last_emitted_block = Some(block_number);
                    }
                }

                let warmup_just_ended =
                    warmup_just_finished(warmup_end_logged, read_block_range, output_block_range, block_number);
                if warmup_just_ended {
                    let output_start = output_block_range
                        .map(|(start_height, _)| start_height)
                        .unwrap_or(block_number.saturating_add(1));
                    info!("[warmup] end at block={} (output starts from block {})", block_number, output_start);
                    progress_processed_blocks = 0;
                    progress_phase_started_at = Instant::now();
                    progress_interval_started_at = progress_phase_started_at;
                    progress_pruned_live_orders_interval = 0;
                    progress_pruned_trigger_oids_interval = 0;
                    warmup_end_logged = true;
                }

                processed_blocks_total += 1;
                if !emit_rows {
                    warmup_processed_blocks += 1;
                    warmup_pruned_live_orders_interval += block_prune_stats.live_pruned_orders;
                    warmup_pruned_trigger_oids_interval += block_prune_stats.trigger_pruned_oids;
                } else {
                    progress_processed_blocks += 1;
                    progress_pruned_live_orders_interval += block_prune_stats.live_pruned_orders;
                    progress_pruned_trigger_oids_interval += block_prune_stats.trigger_pruned_oids;
                }

                if !emit_rows && warmup_processed_blocks % PROGRESS_LOG_INTERVAL_BLOCKS == 0 {
                    let (live_orders, cloid_map_entries, trigger_oids, buffered_rows) =
                        aggregate_coin_state_metrics(&coin_states);
                    let elapsed_secs = warmup_interval_started_at.elapsed().as_secs_f64();
                    let total_secs = warmup_phase_started_at.elapsed().as_secs_f64();
                    log_processing_progress(
                        warmup_processed_blocks,
                        block_number,
                        fills_block_time_ms,
                        live_orders,
                        cloid_map_entries,
                        trigger_oids,
                        buffered_rows,
                        warmup_pruned_live_orders_interval,
                        warmup_pruned_trigger_oids_interval,
                        elapsed_secs,
                        total_secs,
                        true,
                    );
                    warmup_interval_started_at = Instant::now();
                    warmup_pruned_live_orders_interval = 0;
                    warmup_pruned_trigger_oids_interval = 0;
                }

                if emit_rows && progress_processed_blocks % PROGRESS_LOG_INTERVAL_BLOCKS == 0 {
                    let (live_orders, cloid_map_entries, trigger_oids, buffered_rows) =
                        aggregate_coin_state_metrics(&coin_states);
                    let elapsed_secs = progress_interval_started_at.elapsed().as_secs_f64();
                    let total_secs = progress_phase_started_at.elapsed().as_secs_f64();
                    log_processing_progress(
                        progress_processed_blocks,
                        block_number,
                        fills_block_time_ms,
                        live_orders,
                        cloid_map_entries,
                        trigger_oids,
                        buffered_rows,
                        progress_pruned_live_orders_interval,
                        progress_pruned_trigger_oids_interval,
                        elapsed_secs,
                        total_secs,
                        false,
                    );
                    progress_interval_started_at = Instant::now();
                    progress_pruned_live_orders_interval = 0;
                    progress_pruned_trigger_oids_interval = 0;
                }

                maybe_write_warmup_state_snapshot(
                    args.warmup_state_output_dir.as_deref(),
                    block_number,
                    fills_block_time_ms,
                    !emit_rows,
                    warmup_just_ended,
                    block_prune_stats,
                    &coin_states,
                    &mut generated_snapshot_files,
                )
                .await?;

                upload_pending_files_for_states(
                    upload_client.as_ref(),
                    &mut coin_states,
                    &mut generated_snapshot_files,
                    &mut latest_local_uploaded_snapshot,
                )
                .await?;

                let fetch_fills_started_at = Instant::now();
                next_fills = fills_reader.next_batch().await.with_context(|| {
                    format!(
                        "fills_reader.next_batch failed last_seen_fills_block={}",
                        last_seen_fills_block_number
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "none".to_owned())
                    )
                })?;
                perf_stats.fetch_fills_only_ns += fetch_fills_started_at.elapsed().as_nanos();
            }
            (Some(replica), None) => {
                if last_seen_fills_ts_ns.is_some_and(|last_ts_ns| {
                    replica.block_time_ns > last_ts_ns.saturating_add(BLOCK_TIME_TOLERANCE_NS)
                }) {
                    break;
                }

                for state in &mut coin_states {
                    state.unknown_oid_logger.finish()?;
                    let fallback_block = state.last_emitted_block.unwrap_or(0);
                    close_open_output_for_coin_state(&args.output_parquet, state, fallback_block).await?;
                }
                upload_pending_files_for_states(
                    upload_client.as_ref(),
                    &mut coin_states,
                    &mut generated_snapshot_files,
                    &mut latest_local_uploaded_snapshot,
                )
                .await?;
                bail!(
                    "node_fills_by_block hit EOF before replica_cmds; unpaired replica timestamp remains: {}ms",
                    replica.block_time_ms
                );
            }
            (None, Some(fills)) => {
                for state in &mut coin_states {
                    state.unknown_oid_logger.finish()?;
                    close_open_output_for_coin_state(&args.output_parquet, state, fills.block_number).await?;
                }
                upload_pending_files_for_states(
                    upload_client.as_ref(),
                    &mut coin_states,
                    &mut generated_snapshot_files,
                    &mut latest_local_uploaded_snapshot,
                )
                .await?;
                bail!(
                    "replica_cmds hit EOF before node_fills_by_block; unpaired fills block {} remains",
                    fills.block_number
                );
            }
            (None, None) => break,
        }
    }

    for state in &mut coin_states {
        state.unknown_oid_logger.finish()?;
        let fallback_block = state.last_emitted_block.unwrap_or(0);
        close_open_output_for_coin_state(&args.output_parquet, state, fallback_block).await?;
    }

    upload_pending_files_for_states(
        upload_client.as_ref(),
        &mut coin_states,
        &mut generated_snapshot_files,
        &mut latest_local_uploaded_snapshot,
    )
    .await?;

    perf_stats.log_summary(processed_blocks_total, started_at.elapsed().as_secs_f64());
    perf_stats.log_read_pair_breakdown(replica_reader.perf_snapshot(), fills_reader.perf_snapshot());

    for state in &coin_states {
        info!(
            "Wrote {} diff parquet files={} output_dir={} unknown_oid_sqlite={} requester_pays={} remaining_live_orders={}",
            state.config.symbol,
            if args.upload { state.uploaded_parquet_files } else { state.generated_output_files.len() },
            args.output_parquet.display(),
            args.unknown_oid_log_sqlite.display(),
            requester_pays,
            state.orders.len()
        );
        if !args.upload {
            for path in &state.generated_output_files {
                info!("  - {}", path.display());
            }
        }
    }

    drop(coin_states);
    if let Some(worker) = unknown_oid_sqlite_worker.as_mut() {
        worker.finish()?;
    }

    Ok(())
}
