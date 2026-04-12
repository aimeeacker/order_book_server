#![allow(unused_crate_dependencies)]

mod build_btc_diff_replica_day_index;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, create_dir_all, rename};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_compression::tokio::bufread::Lz4Decoder;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::RequestPayer;
use chrono::{Datelike, Days, NaiveDate, NaiveDateTime, Timelike};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rusqlite::{Connection, params};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader as TokioBufReader};

use crate::build_btc_diff_replica_day_index::{
    REPLICA_DAY_INDEX, REPLICA_INDEX_END_DATE_YYYYMMDD, REPLICA_INDEX_END_EXCLUSIVE_CHUNK,
    REPLICA_INDEX_START_DATE_YYYYMMDD, REPLICA_SNAPSHOT_DIRS,
};

const BTC_ASSET_ID: u64 = 0;
const ROW_GROUP_BLOCKS: u64 = 10_000;
const FILE_ROTATION_BLOCKS: u64 = 1_000_000;
const PARQUET_MAX_ROW_GROUP_ROWS: usize = 10_000_000;
const DECIMAL_PRECISION: u8 = 18;
const PX_SCALE: i8 = 1;
const SZ_SCALE: i8 = 5;
const HOUR_MS: i64 = 3_600_000;
const LIVE_STATE_TTL_MS: i64 = 24 * HOUR_MS;
const UNKNOWN_OID_WINDOW_BLOCKS: u64 = 10_000;
const UNKNOWN_OID_SAMPLE_LIMIT_PER_WINDOW: usize = 200;
const PRUNE_SAMPLE_LIMIT_PER_RUN: usize = 200;
const REQUESTER_PAYS_ALWAYS: bool = true;
const CANCEL_STATUS_CANCELED: &str = "canceled";
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

    #[arg(long, help = "Output parquet directory (files are named by actual start/end heights)")]
    output_parquet: PathBuf,

    #[arg(long, default_value = "/tmp/build_btc_diff_unknown_oid.sqlite")]
    unknown_oid_log_sqlite: PathBuf,

    #[arg(long)]
    start_height: Option<u64>,

    #[arg(long)]
    height_span: Option<u64>,

    #[arg(long, default_value_t = 1_200_000)]
    warmup: u64,

    #[arg(long, default_value_t = 2_000_000)]
    block_time_tolerance_ns: i64,

    #[arg(long, default_value_t = 0)]
    block_time_match_tolerance_ms: i64,
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
    resps: ReplicaResponses,
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
    action: Value,
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
    response: Value,
}

#[derive(Debug, Deserialize)]
struct NodeFillsBlock {
    block_time: String,
    block_number: u64,
    #[serde(default)]
    events: Vec<(String, FillEvent)>,
}

#[derive(Debug, Deserialize)]
struct NodeFillsTimeBlock {
    block_time: String,
    block_number: u64,
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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
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

#[derive(Debug, Serialize)]
struct PrunedKnownOidSample {
    oid: u64,
    created_block_number: u64,
    created_time_ms: i64,
    pruned_at_block_number: u64,
    pruned_at_time_ms: i64,
    age_ms: i64,
}

#[derive(Debug, Serialize)]
struct KnownOidPruneRun {
    trigger_block_number: u64,
    trigger_block_time_ms: i64,
    cutoff_time_ms: i64,
    pruned_total: u64,
    sampled: u64,
    sample_dropped: u64,
    samples: Vec<PrunedKnownOidSample>,
}

#[derive(Debug)]
struct UnknownOidWindowLogger {
    conn: Connection,
    enabled: bool,
    current_window_start: Option<u64>,
    stats: UnknownOidWindowStats,
    samples: Vec<UnknownOidSample>,
    stale_prune_runs: Vec<StalePruneRun>,
    known_oid_prune_runs: Vec<KnownOidPruneRun>,
}

impl UnknownOidWindowLogger {
    fn new(path: &PathBuf) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open unknown oid sqlite for read/write: {}", path.display()))?;
        conn.execute_batch(
            r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS unknown_oid_windows (
    window_start_block INTEGER PRIMARY KEY,
    window_end_block INTEGER NOT NULL,
    window_type TEXT NOT NULL,
    unknown_cancel_total INTEGER NOT NULL,
    unknown_modify_total INTEGER NOT NULL,
    unresolved_user_cloid_cancel INTEGER NOT NULL,
    unresolved_user_cloid_modify INTEGER NOT NULL,
    sampled INTEGER NOT NULL,
    sample_dropped INTEGER NOT NULL,
    stale_prune_runs INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS unknown_oid_samples (
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
    PRIMARY KEY (window_start_block, seq)
);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_block ON unknown_oid_samples(block_number);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_ref_oid ON unknown_oid_samples(ref_oid);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_mapped_oid ON unknown_oid_samples(mapped_oid);
CREATE INDEX IF NOT EXISTS idx_unknown_oid_samples_user_cloid ON unknown_oid_samples(user_addr, ref_cloid);

CREATE TABLE IF NOT EXISTS stale_prune_runs (
    window_start_block INTEGER NOT NULL,
    run_seq INTEGER NOT NULL,
    trigger_block_number INTEGER NOT NULL,
    trigger_block_time_ms INTEGER NOT NULL,
    cutoff_time_ms INTEGER NOT NULL,
    pruned_total INTEGER NOT NULL,
    sampled INTEGER NOT NULL,
    sample_dropped INTEGER NOT NULL,
    PRIMARY KEY (window_start_block, run_seq)
);
CREATE INDEX IF NOT EXISTS idx_stale_prune_runs_trigger_block ON stale_prune_runs(trigger_block_number);

CREATE TABLE IF NOT EXISTS stale_prune_samples (
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
    PRIMARY KEY (window_start_block, run_seq, sample_seq)
);
CREATE INDEX IF NOT EXISTS idx_stale_prune_samples_oid ON stale_prune_samples(oid);
CREATE INDEX IF NOT EXISTS idx_stale_prune_samples_created_block ON stale_prune_samples(created_block_number);

CREATE TABLE IF NOT EXISTS known_oid_prune_runs (
    window_start_block INTEGER NOT NULL,
    run_seq INTEGER NOT NULL,
    trigger_block_number INTEGER NOT NULL,
    trigger_block_time_ms INTEGER NOT NULL,
    cutoff_time_ms INTEGER NOT NULL,
    pruned_total INTEGER NOT NULL,
    sampled INTEGER NOT NULL,
    sample_dropped INTEGER NOT NULL,
    PRIMARY KEY (window_start_block, run_seq)
);
CREATE INDEX IF NOT EXISTS idx_known_oid_prune_runs_trigger_block ON known_oid_prune_runs(trigger_block_number);

CREATE TABLE IF NOT EXISTS known_oid_prune_samples (
    window_start_block INTEGER NOT NULL,
    run_seq INTEGER NOT NULL,
    sample_seq INTEGER NOT NULL,
    oid INTEGER NOT NULL,
    created_block_number INTEGER NOT NULL,
    created_time_ms INTEGER NOT NULL,
    pruned_at_block_number INTEGER NOT NULL,
    pruned_at_time_ms INTEGER NOT NULL,
    age_ms INTEGER NOT NULL,
    PRIMARY KEY (window_start_block, run_seq, sample_seq)
);
CREATE INDEX IF NOT EXISTS idx_known_oid_prune_samples_oid ON known_oid_prune_samples(oid);
CREATE INDEX IF NOT EXISTS idx_known_oid_prune_samples_created_block ON known_oid_prune_samples(created_block_number);
"#,
        )
        .context("initializing unknown oid sqlite schema")?;

        Ok(Self {
            conn,
            enabled: true,
            current_window_start: None,
            stats: UnknownOidWindowStats::default(),
            samples: Vec::new(),
            stale_prune_runs: Vec::new(),
            known_oid_prune_runs: Vec::new(),
        })
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

    fn record_known_oid_prune_run(&mut self, run: KnownOidPruneRun) {
        if !self.enabled {
            return;
        }
        self.known_oid_prune_runs.push(run);
    }

    fn flush_current(&mut self) -> Result<()> {
        let Some(window_start_block) = self.current_window_start else {
            return Ok(());
        };
        let window_end_block = window_start_block + UNKNOWN_OID_WINDOW_BLOCKS - 1;
        let stats = std::mem::take(&mut self.stats);
        let samples = std::mem::take(&mut self.samples);
        let stale_prune_runs = std::mem::take(&mut self.stale_prune_runs);
        let known_oid_prune_runs = std::mem::take(&mut self.known_oid_prune_runs);

        let tx = self.conn.transaction().context("begin unknown oid sqlite txn")?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO unknown_oid_windows (
                window_start_block, window_end_block, window_type,
                unknown_cancel_total, unknown_modify_total,
                unresolved_user_cloid_cancel, unresolved_user_cloid_modify,
                sampled, sample_dropped, stale_prune_runs
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                i64::try_from(window_start_block)?,
                i64::try_from(window_end_block)?,
                "unknown_oid_window_v3_sqlite",
                i64::try_from(stats.unknown_cancel_total)?,
                i64::try_from(stats.unknown_modify_total)?,
                i64::try_from(stats.unresolved_user_cloid_cancel)?,
                i64::try_from(stats.unresolved_user_cloid_modify)?,
                i64::try_from(stats.sampled)?,
                i64::try_from(stats.sample_dropped)?,
                i64::try_from(stale_prune_runs.len())?,
            ],
        )?;

        if inserted == 0 {
            eprintln!(
                "[warn] unknown-oid sqlite conflict at window_start_block={} (already exists); skipping this window",
                window_start_block
            );
            tx.commit()?;
            return Ok(());
        }

        for (seq, sample) in samples.iter().enumerate() {
            tx.execute(
                "INSERT OR IGNORE INTO unknown_oid_samples (
                    window_start_block, seq, block_number, block_time_ms,
                    event, phase, reason, user_addr, ref_kind, ref_oid, ref_cloid, mapped_oid
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
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

        for (run_seq, run) in stale_prune_runs.iter().enumerate() {
            tx.execute(
                "INSERT OR IGNORE INTO stale_prune_runs (
                    window_start_block, run_seq, trigger_block_number, trigger_block_time_ms,
                    cutoff_time_ms, pruned_total, sampled, sample_dropped
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
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
                        window_start_block, run_seq, sample_seq, oid, user_addr, cloid,
                        created_block_number, created_time_ms, pruned_at_block_number, pruned_at_time_ms, age_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
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

        for (run_seq, run) in known_oid_prune_runs.iter().enumerate() {
            tx.execute(
                "INSERT OR IGNORE INTO known_oid_prune_runs (
                    window_start_block, run_seq, trigger_block_number, trigger_block_time_ms,
                    cutoff_time_ms, pruned_total, sampled, sample_dropped
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
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
                    "INSERT OR IGNORE INTO known_oid_prune_samples (
                        window_start_block, run_seq, sample_seq, oid,
                        created_block_number, created_time_ms, pruned_at_block_number, pruned_at_time_ms, age_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        i64::try_from(window_start_block)?,
                        i64::try_from(run_seq)?,
                        i64::try_from(sample_seq)?,
                        i64::try_from(sample.oid)?,
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

    fn finish(&mut self) -> Result<()> {
        self.flush_current()
    }
}

#[derive(Debug, Clone)]
struct OrderMeta {
    tif: Option<String>,
    reduce_only: Option<bool>,
    order_type: Option<String>,
    is_trigger: bool,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct BlockIndex {
    ts_ns: i64,
    block_number: u64,
}

#[derive(Debug)]
struct ReplicaBatch {
    block_number: u64,
    block_time_ms: i64,
    events: Vec<CmdEvent>,
}

#[derive(Debug)]
struct NodeFillsBatch {
    block_number: u64,
    block_time_ms: i64,
    fills: Vec<FillRecord>,
}

#[derive(Debug)]
enum CmdEvent {
    Add {
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
        user: Option<String>,
        order_ref: OrderRef,
        fallback_on_missing: bool,
    },
    Modify {
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
        user: String,
        oid: u64,
        cloid: Option<String>,
    },
    TrackTriggerPending {
        user: String,
        cloid: Option<String>,
    },
    TrackTriggerModify {
        user: String,
        old_order_ref: OrderRef,
        new_oid: Option<u64>,
        new_cloid: Option<String>,
    },
    Skipped {
        user: Option<String>,
        oid: Option<u64>,
        event: String,
        status: String,
    },
}

#[derive(Debug, Clone)]
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
    order_type: Option<String>,
    is_trigger: bool,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct KnownOidState {
    created_block_number: u64,
    created_time_ms: i64,
}

#[derive(Debug, Default)]
struct TriggerOrderRefs {
    oid_to_cloid: HashMap<u64, Option<UserCloidKey>>,
    cloid_to_oid: HashMap<String, HashMap<String, u64>>,
    pending_by_user: HashMap<String, VecDeque<Option<String>>>,
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
    diff_type: String,
    event: String,
    sz: Option<Decimal>,
    orig_sz: Option<Decimal>,
    raw_sz: Option<Decimal>,
    is_trigger: Option<bool>,
    tif: Option<String>,
    reduce_only: Option<bool>,
    order_type: Option<String>,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
    status: Option<String>,
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

    fn take_batch(&mut self, schema: &Arc<Schema>) -> Result<Option<RecordBatch>> {
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
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut diff_type = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut event = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE));
        let mut orig_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE));
        let mut raw_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE));
        let mut is_trigger = BooleanBuilder::with_capacity(self.rows.len());
        let mut tif = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 8);
        let mut reduce_only = BooleanBuilder::with_capacity(self.rows.len());
        let mut order_type = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut trigger_condition = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 24);
        let mut trigger_px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut is_position_tpsl = BooleanBuilder::with_capacity(self.rows.len());
        let mut tp_trigger_px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut sl_trigger_px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut status = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 24);
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
                Some(value) => px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => px.append_null(),
            }
            diff_type.append_value(&row.diff_type);
            event.append_value(&row.event);
            match row.sz {
                Some(value) => sz.append_value(decimal_to_i128(value, SZ_SCALE)),
                None => sz.append_null(),
            }
            match row.orig_sz {
                Some(value) => orig_sz.append_value(decimal_to_i128(value, SZ_SCALE)),
                None => orig_sz.append_null(),
            }
            match row.raw_sz {
                Some(value) => raw_sz.append_value(decimal_to_i128(value, SZ_SCALE)),
                None => raw_sz.append_null(),
            }
            match row.is_trigger {
                Some(value) => is_trigger.append_value(value),
                None => is_trigger.append_null(),
            }
            match row.tif {
                Some(value) => tif.append_value(value),
                None => tif.append_null(),
            }
            match row.reduce_only {
                Some(value) => reduce_only.append_value(value),
                None => reduce_only.append_null(),
            }
            match row.order_type {
                Some(value) => order_type.append_value(value),
                None => order_type.append_null(),
            }
            match row.trigger_condition {
                Some(value) => trigger_condition.append_value(value),
                None => trigger_condition.append_null(),
            }
            match row.trigger_px {
                Some(value) => trigger_px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => trigger_px.append_null(),
            }
            match row.is_position_tpsl {
                Some(value) => is_position_tpsl.append_value(value),
                None => is_position_tpsl.append_null(),
            }
            match row.tp_trigger_px {
                Some(value) => tp_trigger_px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => tp_trigger_px.append_null(),
            }
            match row.sl_trigger_px {
                Some(value) => sl_trigger_px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => sl_trigger_px.append_null(),
            }
            match row.status {
                Some(value) => status.append_value(value),
                None => status.append_null(),
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
            Arc::new(diff_type.finish()),
            Arc::new(event.finish()),
            Arc::new(sz.finish()),
            Arc::new(orig_sz.finish()),
            Arc::new(raw_sz.finish()),
            Arc::new(is_trigger.finish()),
            Arc::new(tif.finish()),
            Arc::new(reduce_only.finish()),
            Arc::new(order_type.finish()),
            Arc::new(trigger_condition.finish()),
            Arc::new(trigger_px.finish()),
            Arc::new(is_position_tpsl.finish()),
            Arc::new(tp_trigger_px.finish()),
            Arc::new(sl_trigger_px.finish()),
            Arc::new(status.finish()),
            Arc::new(lifetime.finish()),
        ];

        Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
    }
}

fn parse_timestamp_ns(raw: &str) -> Result<i64> {
    let (ts_ns, _ts_ms) = parse_timestamp_ns_ms(raw)?;
    Ok(ts_ns)
}

fn parse_timestamp_ms(raw: &str) -> Result<i64> {
    let (_ts_ns, ts_ms) = parse_timestamp_ns_ms(raw)?;
    Ok(ts_ms)
}

fn parse_timestamp_ns_ms(raw: &str) -> Result<(i64, i64)> {
    let dt = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
        .with_context(|| format!("failed to parse timestamp: {raw}"))?;
    let utc = dt.and_utc();
    let ts_ns = utc.timestamp_nanos_opt().ok_or_else(|| anyhow!("timestamp overflow for {raw}"))?;
    let ts_ms = utc.timestamp_millis();
    Ok((ts_ns, ts_ms))
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    raw.parse::<Decimal>().with_context(|| format!("failed to parse decimal: {raw}"))
}

fn get_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        let Some(v) = value.get(*key) else {
            continue;
        };
        if let Some(parsed) = v.as_u64() {
            return Some(parsed);
        }
        if let Some(raw) = v.as_str()
            && let Ok(parsed) = raw.parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

fn get_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(raw) = value.get(*key).and_then(Value::as_str) {
            return Some(raw.to_owned());
        }
    }
    None
}

fn get_order_ref(value: &Value, keys: &[&str]) -> Option<OrderRef> {
    for key in keys {
        let Some(v) = value.get(*key) else {
            continue;
        };
        if *key == "cloid" {
            if let Some(raw) = v.as_str() {
                return Some(OrderRef::Cloid(raw.to_owned()));
            }
            continue;
        }
        if let Some(parsed) = v.as_u64() {
            return Some(OrderRef::Oid(parsed));
        }
        if let Some(raw) = v.as_str() {
            if let Ok(parsed) = raw.parse::<u64>() {
                return Some(OrderRef::Oid(parsed));
            }
            return Some(OrderRef::Cloid(raw.to_owned()));
        }
    }
    None
}

fn parse_order_fields(order: &Value) -> Option<(u64, String, Decimal, Decimal, Option<String>, OrderMeta)> {
    let asset = get_u64(order, &["a", "asset"])?;
    let is_bid = order.get("b")?.as_bool()?;
    let side = if is_bid { "B" } else { "A" }.to_owned();
    let px_raw = get_string(order, &["p", "price"])?;
    let px = parse_decimal(&px_raw).ok()?;
    let sz_raw = get_string(order, &["s", "size"])?;
    let sz = parse_decimal(&sz_raw).ok()?;
    let trigger = order.get("t").and_then(|t| t.get("trigger"));
    let cloid = get_string(order, &["c", "cloid"]);
    let reduce_only = order.get("r").and_then(Value::as_bool);

    let meta = if let Some(trigger) = trigger {
        let trigger_px = get_string(trigger, &["triggerPx"]).and_then(|raw| parse_decimal(&raw).ok())?;
        let is_market = trigger.get("isMarket").and_then(Value::as_bool).unwrap_or(false);
        let tpsl = trigger.get("tpsl").and_then(Value::as_str);
        OrderMeta {
            tif: None,
            reduce_only,
            order_type: Some(if is_market { "Stop Market".to_owned() } else { "Stop Limit".to_owned() }),
            is_trigger: true,
            trigger_condition: None,
            trigger_px: Some(trigger_px),
            is_position_tpsl: None,
            tp_trigger_px: if tpsl == Some("tp") { Some(trigger_px) } else { None },
            sl_trigger_px: if tpsl == Some("sl") { Some(trigger_px) } else { None },
        }
    } else {
        let tif = order
            .get("t")
            .and_then(|t| t.get("limit"))
            .and_then(|limit| limit.get("tif"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        OrderMeta {
            tif,
            reduce_only,
            order_type: Some("Limit".to_owned()),
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

fn parse_statuses(response: &Value) -> &[Value] {
    response
        .get("data")
        .and_then(|data| data.get("statuses"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn response_type(response: &Value) -> Option<&str> {
    response.get("type").and_then(Value::as_str)
}

fn is_dead_order_error(raw: &str) -> bool {
    raw.contains("Cannot modify canceled or filled order")
}

fn is_dead_order_modify_response(result: &ResponseResult) -> bool {
    if result.status.as_deref().is_some_and(is_dead_order_error) {
        return true;
    }

    if let Some(raw) = result.response.as_str() {
        return is_dead_order_error(raw);
    }

    parse_statuses(&result.response)
        .iter()
        .any(|status| status.get("error").and_then(Value::as_str).is_some_and(is_dead_order_error))
}

fn is_success_status(status: Option<&Value>) -> bool {
    status.and_then(Value::as_str).is_some_and(|raw| raw.eq_ignore_ascii_case("success"))
}

fn is_pending_trigger_status(status: &Value) -> bool {
    status
        .as_str()
        .is_some_and(|raw| raw.eq_ignore_ascii_case("waitingForTrigger") || raw.eq_ignore_ascii_case("waitingForFill"))
        || status.get("waitingForTrigger").and_then(Value::as_bool).unwrap_or(false)
        || status.get("waitingForFill").and_then(Value::as_bool).unwrap_or(false)
}

enum BookedSzDecision {
    Booked { booked_sz: Decimal, raw_sz: Option<Decimal> },
    FullyFilled,
    InvalidFilledTotalSz,
}

fn parse_booked_and_raw_sz(order_sz: Decimal, status: &Value) -> BookedSzDecision {
    let Some(filled) = status.get("filled") else {
        return BookedSzDecision::Booked { booked_sz: order_sz, raw_sz: None };
    };

    let Some(raw_total_sz) = get_string(filled, &["totalSz"]) else {
        return BookedSzDecision::InvalidFilledTotalSz;
    };

    let Ok(filled_sz) = parse_decimal(&raw_total_sz) else {
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

fn parse_json_line<T>(line: &mut Vec<u8>) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    trim_newline(line);
    simd_json::serde::from_slice(line).map_err(anyhow::Error::from).context("failed to parse json line with simd-json")
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
        bail!("--start-height must be >= 1 for replica_cmds naming rule <height-1>.lz4");
    }
    if height_span == 0 {
        bail!("--height-span must be > 0");
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

fn resolve_replica_keys_via_hardcoded_day_index(
    normalized_prefix: &str,
    required_chunks: &[u64],
) -> Result<Option<Vec<String>>> {
    if normalized_prefix != "replica_cmds" || required_chunks.is_empty() {
        return Ok(None);
    }

    let Some(first_entry) = REPLICA_DAY_INDEX.first() else {
        return Ok(None);
    };
    let first_chunk = first_entry.start_chunk;

    let base_date = NaiveDate::parse_from_str(REPLICA_INDEX_START_DATE_YYYYMMDD, "%Y%m%d")
        .with_context(|| format!("invalid hardcoded start date: {REPLICA_INDEX_START_DATE_YYYYMMDD}"))?;
    let end_date = NaiveDate::parse_from_str(REPLICA_INDEX_END_DATE_YYYYMMDD, "%Y%m%d")
        .with_context(|| format!("invalid hardcoded end date: {REPLICA_INDEX_END_DATE_YYYYMMDD}"))?;

    let mut resolved = Vec::with_capacity(required_chunks.len());
    for &chunk in required_chunks {
        if chunk < first_chunk || chunk >= REPLICA_INDEX_END_EXCLUSIVE_CHUNK {
            return Ok(None);
        }

        let pos = REPLICA_DAY_INDEX.partition_point(|entry| entry.start_chunk <= chunk);
        if pos == 0 {
            return Ok(None);
        }
        let day_idx = pos - 1;
        let day_entry = REPLICA_DAY_INDEX[day_idx];
        let snapshot = REPLICA_SNAPSHOT_DIRS
            .get(usize::from(day_entry.snapshot_idx))
            .ok_or_else(|| anyhow!("invalid snapshot idx in hardcoded replica index: {}", day_entry.snapshot_idx))?;
        let date = base_date
            .checked_add_days(Days::new(day_idx as u64))
            .ok_or_else(|| anyhow!("hardcoded replica day index overflow at idx {}", day_idx))?;
        if date > end_date {
            return Ok(None);
        }
        let date_str = date.format("%Y%m%d");
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

    if let Some(resolved) = resolve_replica_keys_via_hardcoded_day_index(normalized_prefix, &required_chunks)? {
        eprintln!(
            "[index] resolved replica_cmds via hardcoded day index for {} chunks (range {}..{})",
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
    line: Vec<u8>,
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
            line: Vec::with_capacity(1 << 20),
        })
    }

    fn make_lz4_reader<R>(reader: R) -> Box<dyn AsyncBufRead + Unpin + Send>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let buffered = TokioBufReader::new(reader);
        let decoder = Lz4Decoder::new(buffered);
        Box::new(TokioBufReader::new(decoder))
    }

    async fn fetch_s3_reader(
        &self,
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

        match plan {
            OpenPlan::End => Ok(false),
            OpenPlan::Local(path) => {
                let file =
                    tokio::fs::File::open(&path).await.with_context(|| format!("failed to open {}", path.display()))?;
                self.current_reader = Some(Self::make_lz4_reader(file));
                Ok(true)
            }
            OpenPlan::S3Key { bucket, key, allow_missing, advance_hour } => {
                let maybe_reader = self.fetch_s3_reader(&bucket, &key, allow_missing).await?;
                let Some(reader) = maybe_reader else {
                    return Ok(false);
                };
                if advance_hour && let ReaderSource::S3HourlyFrom { next_hour_ms, .. } = &mut self.source {
                    *next_hour_ms += HOUR_MS;
                }
                self.current_reader = Some(reader);
                Ok(true)
            }
        }
    }

    async fn next_json_line<T>(&mut self) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        loop {
            if self.current_reader.is_none() && !self.open_next_reader().await? {
                return Ok(None);
            }

            self.line.clear();
            let bytes_read =
                self.current_reader.as_mut().expect("current_reader checked").read_until(b'\n', &mut self.line).await?;
            if bytes_read == 0 {
                self.current_reader = None;
                continue;
            }

            return parse_json_line(&mut self.line).map(Some);
        }
    }
}

struct NodeFillsReader {
    reader: Lz4JsonLineReader,
    block_range: Option<(u64, u64)>,
}

impl NodeFillsReader {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
    ) -> Result<Self> {
        Ok(Self { reader: Lz4JsonLineReader::new(source, s3_client, requester_pays).await?, block_range })
    }

    async fn next_batch(&mut self) -> Result<Option<NodeFillsBatch>> {
        let block = loop {
            let Some(block) = self.reader.next_json_line::<NodeFillsBlock>().await? else {
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

        let block_time_ms = parse_timestamp_ms(&block.block_time)?;
        let mut fills = Vec::with_capacity(block.events.len());
        for (address, event) in block.events {
            if event.coin != "BTC" {
                continue;
            }

            fills.push(FillRecord {
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

        Ok(Some(NodeFillsBatch { block_number: block.block_number, block_time_ms, fills }))
    }
}

enum BlockMatch {
    Matched(u64),
    Unmatched,
    PastEnd,
}

struct FillsTimeCursor {
    reader: Lz4JsonLineReader,
    block_range: Option<(u64, u64)>,
    prev: Option<BlockIndex>,
    curr: Option<BlockIndex>,
    exhausted: bool,
}

impl FillsTimeCursor {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
    ) -> Result<Self> {
        let mut cursor = Self {
            reader: Lz4JsonLineReader::new(source, s3_client, requester_pays).await?,
            block_range,
            prev: None,
            curr: None,
            exhausted: false,
        };
        cursor.curr = cursor.read_next_index().await?;
        Ok(cursor)
    }

    async fn read_next_index(&mut self) -> Result<Option<BlockIndex>> {
        loop {
            let Some(block) = self.reader.next_json_line::<NodeFillsTimeBlock>().await? else {
                self.exhausted = true;
                return Ok(None);
            };

            if let Some((start_height, end_height)) = self.block_range {
                if block.block_number < start_height {
                    continue;
                }
                if block.block_number > end_height {
                    self.exhausted = true;
                    return Ok(None);
                }
            }

            let ts_ns = parse_timestamp_ns(&block.block_time)?;
            return Ok(Some(BlockIndex { ts_ns, block_number: block.block_number }));
        }
    }

    async fn match_block(&mut self, ts_ns: i64, tolerance_ns: i64) -> Result<BlockMatch> {
        while self.curr.as_ref().is_some_and(|curr| curr.ts_ns < ts_ns) {
            self.prev = self.curr.take();
            self.curr = self.read_next_index().await?;
        }

        let mut best: Option<(i64, u64)> = None;
        for candidate in [self.prev.as_ref(), self.curr.as_ref()].into_iter().flatten() {
            let delta = (candidate.ts_ns - ts_ns).abs();
            if delta <= tolerance_ns {
                match best {
                    Some((best_delta, _)) if best_delta <= delta => {}
                    _ => best = Some((delta, candidate.block_number)),
                }
            }
        }

        if let Some((_, block_number)) = best {
            return Ok(BlockMatch::Matched(block_number));
        }

        if self.exhausted
            && self.curr.is_none()
            && self.prev.as_ref().is_some_and(|last| ts_ns > last.ts_ns.saturating_add(tolerance_ns))
        {
            return Ok(BlockMatch::PastEnd);
        }

        Ok(BlockMatch::Unmatched)
    }
}

struct ReplicaCmdsReader {
    reader: Lz4JsonLineReader,
    fills_time_cursor: FillsTimeCursor,
    tolerance_ns: i64,
    block_range: Option<(u64, u64)>,
}

impl ReplicaCmdsReader {
    async fn new(
        source: ReaderSource,
        fills_time_source: ReaderSource,
        tolerance_ns: i64,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
    ) -> Result<Self> {
        Ok(Self {
            reader: Lz4JsonLineReader::new(source, s3_client.clone(), requester_pays).await?,
            fills_time_cursor: FillsTimeCursor::new(fills_time_source, s3_client, requester_pays, block_range).await?,
            tolerance_ns,
            block_range,
        })
    }

    async fn next_batch(&mut self) -> Result<Option<ReplicaBatch>> {
        loop {
            let Some(replica) = self.reader.next_json_line::<ReplicaLine>().await? else {
                return Ok(None);
            };

            let (ts_ns, block_time_ms) = parse_timestamp_ns_ms(&replica.abci_block.time)?;
            let block_number = match self.fills_time_cursor.match_block(ts_ns, self.tolerance_ns).await? {
                BlockMatch::Matched(block_number) => block_number,
                BlockMatch::Unmatched => continue,
                BlockMatch::PastEnd => return Ok(None),
            };
            if let Some((start_height, end_height)) = self.block_range {
                if block_number < start_height {
                    continue;
                }
                if block_number > end_height {
                    return Ok(None);
                }
            }

            let action_map: HashMap<_, _> = replica.abci_block.signed_action_bundles.into_iter().collect();
            let mut events = Vec::new();

            for (tx_hash, responses) in replica.resps.full {
                let Some(payload) = action_map.get(&tx_hash) else {
                    continue;
                };

                for (action_item, response_item) in payload.signed_actions.iter().zip(responses.iter()) {
                    let action = &action_item.action;
                    let action_type = action.get("type").and_then(Value::as_str);
                    match action_type {
                        Some("order") => {
                            let orders =
                                action.get("orders").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);

                            for (order, status) in orders.iter().zip(statuses.iter()) {
                                let Some((asset, side, px, sz, cloid, meta)) = parse_order_fields(order) else {
                                    continue;
                                };
                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(user) = response_item.user.clone() else {
                                    continue;
                                };

                                let has_resting = status.get("resting").is_some();
                                let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                                let oid_from_resting =
                                    status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                                let oid_from_filled = status.get("filled").and_then(|filled| get_u64(filled, &["oid"]));
                                let oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                if meta.is_trigger {
                                    if let Some(oid) = oid {
                                        events.push(CmdEvent::TrackTriggerAdd { user, oid, cloid });
                                    } else if is_pending_trigger_status(status) {
                                        events.push(CmdEvent::TrackTriggerPending { user, cloid });
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
                                            user: Some(user),
                                            oid: Some(oid),
                                            event: "add".to_owned(),
                                            status: "skipped_invalid_filled_total_sz".to_owned(),
                                        });
                                    }
                                }
                            }
                        }
                        Some("cancel") | Some("cancelByCloid") => {
                            let cancels =
                                action.get("cancels").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);
                            let is_cancel_by_cloid = action_type == Some("cancelByCloid");

                            for (idx, cancel) in cancels.iter().enumerate() {
                                let status = statuses.get(idx);
                                if is_cancel_by_cloid {
                                    if !is_success_status(status) {
                                        continue;
                                    }
                                } else if !is_success_status(status) {
                                    continue;
                                }
                                let order_ref = get_order_ref(cancel, &["o", "oid", "cloid"]);
                                let Some(asset) = get_u64(cancel, &["a", "asset"]) else {
                                    continue;
                                };

                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(order_ref) = order_ref else {
                                    continue;
                                };
                                if matches!(order_ref, OrderRef::Oid(0)) {
                                    continue;
                                }

                                events.push(CmdEvent::Cancel {
                                    user: response_item.user.clone(),
                                    order_ref,
                                    fallback_on_missing: true,
                                });
                            }
                        }
                        Some("modify") => {
                            let Some(order) = action.get("order") else {
                                continue;
                            };
                            let Some((asset, side, px, sz, new_cloid, meta)) = parse_order_fields(order) else {
                                continue;
                            };
                            if asset != BTC_ASSET_ID {
                                continue;
                            }
                            let Some(user) = response_item.user.clone() else {
                                continue;
                            };
                            let Some(old_order_ref) = get_order_ref(action, &["oid", "cloid"]) else {
                                continue;
                            };
                            if is_dead_order_modify_response(&response_item.res) {
                                continue;
                            }

                            let statuses = parse_statuses(&response_item.res.response);
                            let Some(status) = statuses.first() else {
                                if meta.is_trigger {
                                    eprintln!(
                                        "[warn] block={} user={} trigger modify returned no statuses; skipping old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                } else {
                                    eprintln!(
                                        "[warn] block={} user={} modify returned no statuses; treating as cancel-only old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                    events.push(CmdEvent::Cancel {
                                        user: Some(user.clone()),
                                        order_ref: old_order_ref,
                                        fallback_on_missing: true,
                                    });
                                }
                                continue;
                            };
                            let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                            let oid_from_resting = status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                            let oid_from_filled = status.get("filled").and_then(|filled| get_u64(filled, &["oid"]));
                            if meta.is_trigger {
                                let new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                events.push(CmdEvent::TrackTriggerModify { user, old_order_ref, new_oid, new_cloid });
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
                                    user: Some(user),
                                    oid: oid_from_resting.or(oid_from_filled),
                                    event: "modify".to_owned(),
                                    status: "skipped_invalid_filled_total_sz".to_owned(),
                                });
                            }
                        }
                        Some("batchModify") => {
                            let modifies =
                                action.get("modifies").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);

                            for (idx, modify) in modifies.iter().enumerate() {
                                let Some(order) = modify.get("order") else {
                                    continue;
                                };
                                let Some((asset, side, px, sz, new_cloid, meta)) = parse_order_fields(order) else {
                                    continue;
                                };
                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(user) = response_item.user.clone() else {
                                    continue;
                                };
                                let Some(old_order_ref) = get_order_ref(modify, &["oid", "cloid"]) else {
                                    continue;
                                };
                                let status = statuses.get(idx);
                                if status.is_none() && meta.is_trigger {
                                    eprintln!(
                                        "[warn] block={} user={} trigger batchModify item={} returned no statuses; skipping old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        idx,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                    continue;
                                }
                                if status.is_none() && !meta.is_trigger {
                                    eprintln!(
                                        "[warn] block={} user={} batchModify item={} returned no statuses; treating as cancel-only old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        idx,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                    events.push(CmdEvent::Cancel {
                                        user: Some(user),
                                        order_ref: old_order_ref,
                                        fallback_on_missing: true,
                                    });
                                    continue;
                                }
                                if status
                                    .and_then(|status| status.get("error").and_then(Value::as_str))
                                    .is_some_and(is_dead_order_error)
                                {
                                    continue;
                                }
                                let status = status.unwrap();
                                let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                                let oid_from_resting =
                                    status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                                let oid_from_filled = status.get("filled").and_then(|filled| get_u64(filled, &["oid"]));
                                if meta.is_trigger {
                                    let new_oid =
                                        oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                    events.push(CmdEvent::TrackTriggerModify {
                                        user,
                                        old_order_ref,
                                        new_oid,
                                        new_cloid,
                                    });
                                    continue;
                                }

                                let (new_sz, raw_sz, invalid_filled_total_sz) =
                                    match parse_booked_and_raw_sz(sz, status) {
                                        BookedSzDecision::Booked { booked_sz, raw_sz } => (booked_sz, raw_sz, false),
                                        BookedSzDecision::FullyFilled => (Decimal::ZERO, None, false),
                                        BookedSzDecision::InvalidFilledTotalSz => (Decimal::ZERO, None, true),
                                    };
                                let mut new_oid =
                                    oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                if new_sz <= Decimal::ZERO {
                                    new_oid = None;
                                }
                                events.push(CmdEvent::Modify {
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
                                        user: Some(user),
                                        oid: oid_from_resting.or(oid_from_filled),
                                        event: "modify".to_owned(),
                                        status: "skipped_invalid_filled_total_sz".to_owned(),
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            return Ok(Some(ReplicaBatch { block_number, block_time_ms, events }));
        }
    }
}

fn output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int64, false),
        Field::new("block_time", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("coin", DataType::Utf8, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("oid", DataType::Int64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("diff_type", DataType::Utf8, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), true),
        Field::new("orig_sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), true),
        Field::new("raw_sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), true),
        Field::new("is_trigger", DataType::Boolean, true),
        Field::new("tif", DataType::Utf8, true),
        Field::new("reduce_only", DataType::Boolean, true),
        Field::new("order_type", DataType::Utf8, true),
        Field::new("trigger_condition", DataType::Utf8, true),
        Field::new("trigger_px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("is_position_tpsl", DataType::Boolean, true),
        Field::new("tp_trigger_px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("sl_trigger_px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("status", DataType::Utf8, true),
        Field::new("lifetime", DataType::Int32, true),
    ]))
}

fn decimal_to_i128(mut value: Decimal, scale: i8) -> i128 {
    value.rescale(scale as u32);
    value.mantissa()
}

fn clamp_lifetime_ms(created_time_ms: i64, current_time_ms: i64) -> Option<i32> {
    let delta = current_time_ms - created_time_ms;
    i32::try_from(delta).ok()
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
    let Some(idx) = pending.iter().position(|pending_cloid| pending_cloid.as_deref() == Some(cloid)) else {
        return false;
    };
    pending.remove(idx);
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

fn insert_trigger_ref(trigger_refs: &mut TriggerOrderRefs, user: &str, oid: u64, cloid: Option<&str>) {
    remove_trigger_ref(trigger_refs, oid);
    if let Some(cloid) = cloid {
        remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid);
    }
    let cloid_key = cloid.map(|cloid| UserCloidKey { user: user.to_owned(), cloid: cloid.to_owned() });
    if let Some(cloid_key) = &cloid_key {
        trigger_refs.cloid_to_oid.entry(cloid_key.user.clone()).or_default().insert(cloid_key.cloid.clone(), oid);
    }
    trigger_refs.oid_to_cloid.insert(oid, cloid_key);
}

fn remove_live_order(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    oid: u64,
) -> Option<OrderState> {
    let state = orders.remove(&oid)?;
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
    if let Some(cloid) = cloid {
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
            order_type: meta.order_type.clone(),
            is_trigger: meta.is_trigger,
            trigger_condition: meta.trigger_condition.clone(),
            trigger_px: meta.trigger_px,
            is_position_tpsl: meta.is_position_tpsl,
            tp_trigger_px: meta.tp_trigger_px,
            sl_trigger_px: meta.sl_trigger_px,
        },
    );
}

fn prune_stale_live_orders(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    current_block_number: u64,
    current_time_ms: i64,
) -> StalePruneRun {
    let cutoff_ms = current_time_ms.saturating_sub(LIVE_STATE_TTL_MS);
    let stale_oids: Vec<u64> =
        orders.iter().filter_map(|(oid, state)| (state.created_time_ms <= cutoff_ms).then_some(*oid)).collect();
    let mut samples = Vec::new();
    let mut pruned_total = 0u64;
    for oid in stale_oids {
        if let Some(state) = remove_live_order(orders, cloid_to_oid, oid) {
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

fn prune_known_non_trigger_oids(
    known_non_trigger_oids: &mut HashMap<u64, KnownOidState>,
    current_block_number: u64,
    current_time_ms: i64,
) -> KnownOidPruneRun {
    let cutoff_ms = current_time_ms.saturating_sub(LIVE_STATE_TTL_MS);
    let stale_oids: Vec<u64> = known_non_trigger_oids
        .iter()
        .filter_map(|(oid, state)| (state.created_time_ms <= cutoff_ms).then_some(*oid))
        .collect();
    let mut samples = Vec::new();
    let mut pruned_total = 0u64;
    for oid in stale_oids {
        if let Some(state) = known_non_trigger_oids.remove(&oid) {
            pruned_total += 1;
            if samples.len() < PRUNE_SAMPLE_LIMIT_PER_RUN {
                samples.push(PrunedKnownOidSample {
                    oid,
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
    KnownOidPruneRun {
        trigger_block_number: current_block_number,
        trigger_block_time_ms: current_time_ms,
        cutoff_time_ms: cutoff_ms,
        pruned_total,
        sampled,
        sample_dropped,
        samples,
    }
}

fn push_add_row(
    rows: &mut RowBuffer,
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
        coin: "BTC".to_owned(),
        user: user.to_owned(),
        oid: Some(i64::try_from(oid)?),
        side: Some(side.to_owned()),
        px: Some(px),
        diff_type: "new".to_owned(),
        event: "add".to_owned(),
        sz: Some(sz),
        orig_sz: None,
        raw_sz,
        is_trigger: Some(meta.is_trigger),
        tif: meta.tif.clone(),
        reduce_only: meta.reduce_only,
        order_type: meta.order_type.clone(),
        trigger_condition: meta.trigger_condition.clone(),
        trigger_px: meta.trigger_px,
        is_position_tpsl: meta.is_position_tpsl,
        tp_trigger_px: meta.tp_trigger_px,
        sl_trigger_px: meta.sl_trigger_px,
        status: None,
        lifetime: None,
    });
    Ok(())
}

fn process_block(
    block_number: u64,
    block_time_ms: i64,
    cmd_events: Option<Vec<CmdEvent>>,
    fills: Option<Vec<FillRecord>>,
    emit_rows: bool,
    emit_json_logs: bool,
    rows: &mut RowBuffer,
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, HashMap<String, u64>>,
    trigger_refs: &mut TriggerOrderRefs,
    known_non_trigger_oids: &mut HashMap<u64, KnownOidState>,
    unknown_oid_logger: &mut UnknownOidWindowLogger,
) -> Result<()> {
    unknown_oid_logger.set_enabled(emit_json_logs);
    unknown_oid_logger.observe_block(block_number)?;
    if block_number == row_group_window_start_block(block_number) {
        let prune_run = prune_stale_live_orders(orders, cloid_to_oid, block_number, block_time_ms);
        let known_oid_prune_run = prune_known_non_trigger_oids(known_non_trigger_oids, block_number, block_time_ms);
        if emit_json_logs {
            unknown_oid_logger.record_stale_prune_run(prune_run);
            unknown_oid_logger.record_known_oid_prune_run(known_oid_prune_run);
        }
    }

    if let Some(events) = cmd_events {
        let mut canceled_oids_in_block: HashSet<u64> = HashSet::new();
        let mut deferred_cancels: Vec<(Option<String>, OrderRef, bool)> = Vec::new();
        for event in &events {
            match event {
                CmdEvent::Add { user, oid, cloid, side, px, sz, raw_sz, meta } => {
                    known_non_trigger_oids.insert(
                        *oid,
                        KnownOidState { created_block_number: block_number, created_time_ms: block_time_ms },
                    );
                    insert_live_order(
                        orders,
                        cloid_to_oid,
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
                        push_add_row(rows, block_number, block_time_ms, user, *oid, side, *px, *sz, *raw_sz, meta)?;
                    }
                }
                CmdEvent::Cancel { user, order_ref, fallback_on_missing } => {
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
                        if let Some(state) = remove_live_order(orders, cloid_to_oid, oid) {
                            canceled_oids_in_block.insert(oid);
                            if emit_rows {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: "BTC".to_owned(),
                                    user: state.user,
                                    oid: Some(i64::try_from(oid)?),
                                    side: Some(state.side),
                                    px: Some(state.px),
                                    diff_type: "remove".to_owned(),
                                    event: "cancel".to_owned(),
                                    sz: Some(Decimal::ZERO),
                                    orig_sz: Some(state.remaining_sz),
                                    raw_sz: None,
                                    is_trigger: Some(state.is_trigger),
                                    tif: state.tif.clone(),
                                    reduce_only: state.reduce_only,
                                    order_type: state.order_type.clone(),
                                    trigger_condition: state.trigger_condition.clone(),
                                    trigger_px: state.trigger_px,
                                    is_position_tpsl: state.is_position_tpsl,
                                    tp_trigger_px: state.tp_trigger_px,
                                    sl_trigger_px: state.sl_trigger_px,
                                    status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                                    lifetime: clamp_lifetime_ms(state.created_time_ms, block_time_ms),
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
                            && !known_non_trigger_oids.contains_key(oid)
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
                CmdEvent::Modify { user, old_order_ref, new_oid, new_cloid, side, px, sz, raw_sz, meta } => {
                    let resolved_old_oid = resolve_order_ref(old_order_ref, Some(user.as_str()), cloid_to_oid);
                    if let Some(old_oid) = resolved_old_oid {
                        if let Some(old_state) = remove_live_order(orders, cloid_to_oid, old_oid) {
                            if emit_rows {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: "BTC".to_owned(),
                                    user: old_state.user,
                                    oid: Some(i64::try_from(old_oid)?),
                                    side: Some(old_state.side),
                                    px: Some(old_state.px),
                                    diff_type: "remove".to_owned(),
                                    event: "cancel".to_owned(),
                                    sz: Some(Decimal::ZERO),
                                    orig_sz: Some(old_state.remaining_sz),
                                    raw_sz: None,
                                    is_trigger: Some(old_state.is_trigger),
                                    tif: old_state.tif.clone(),
                                    reduce_only: old_state.reduce_only,
                                    order_type: old_state.order_type.clone(),
                                    trigger_condition: old_state.trigger_condition.clone(),
                                    trigger_px: old_state.trigger_px,
                                    is_position_tpsl: old_state.is_position_tpsl,
                                    tp_trigger_px: old_state.tp_trigger_px,
                                    sl_trigger_px: old_state.sl_trigger_px,
                                    status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                                    lifetime: clamp_lifetime_ms(old_state.created_time_ms, block_time_ms),
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
                            known_non_trigger_oids.insert(
                                *new_oid,
                                KnownOidState { created_block_number: block_number, created_time_ms: block_time_ms },
                            );
                            insert_live_order(
                                orders,
                                cloid_to_oid,
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
                CmdEvent::TrackTriggerAdd { user, oid, cloid } => {
                    insert_trigger_ref(trigger_refs, user, *oid, cloid.as_deref());
                }
                CmdEvent::TrackTriggerPending { user, cloid } => {
                    insert_pending_trigger_ref(trigger_refs, user, cloid.as_deref());
                }
                CmdEvent::TrackTriggerModify { user, old_order_ref, new_oid, new_cloid } => {
                    if let Some(old_oid) = resolve_trigger_ref(old_order_ref, Some(user.as_str()), trigger_refs) {
                        remove_trigger_ref(trigger_refs, old_oid);
                    } else if let OrderRef::Cloid(cloid) = old_order_ref {
                        remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid);
                    }
                    if let Some(new_oid) = new_oid {
                        insert_trigger_ref(trigger_refs, user, *new_oid, new_cloid.as_deref());
                    } else if new_cloid.is_some() {
                        insert_pending_trigger_ref(trigger_refs, user, new_cloid.as_deref());
                    }
                }
                CmdEvent::Skipped { user, oid, event, status } => {
                    if emit_rows {
                        let oid = oid.map(i64::try_from).transpose()?;
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: user.clone().unwrap_or_default(),
                            oid,
                            side: None,
                            px: None,
                            diff_type: "skip".to_owned(),
                            event: event.clone(),
                            sz: None,
                            orig_sz: None,
                            raw_sz: None,
                            is_trigger: None,
                            tif: None,
                            reduce_only: None,
                            order_type: None,
                            trigger_condition: None,
                            trigger_px: None,
                            is_position_tpsl: None,
                            tp_trigger_px: None,
                            sl_trigger_px: None,
                            status: Some(status.clone()),
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
                if let Some(state) = remove_live_order(orders, cloid_to_oid, oid) {
                    canceled_oids_in_block.insert(oid);
                    if emit_rows {
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: state.user,
                            oid: Some(i64::try_from(oid)?),
                            side: Some(state.side),
                            px: Some(state.px),
                            diff_type: "remove".to_owned(),
                            event: "cancel".to_owned(),
                            sz: Some(Decimal::ZERO),
                            orig_sz: Some(state.remaining_sz),
                            raw_sz: None,
                            is_trigger: Some(state.is_trigger),
                            tif: state.tif.clone(),
                            reduce_only: state.reduce_only,
                            order_type: state.order_type.clone(),
                            trigger_condition: state.trigger_condition.clone(),
                            trigger_px: state.trigger_px,
                            is_position_tpsl: state.is_position_tpsl,
                            tp_trigger_px: state.tp_trigger_px,
                            sl_trigger_px: state.sl_trigger_px,
                            status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                            lifetime: clamp_lifetime_ms(state.created_time_ms, block_time_ms),
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
                    && !known_non_trigger_oids.contains_key(oid)
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
    }

    if let Some(fills) = fills {
        let taker_tids: HashSet<u64> = fills.iter().filter(|fill| fill.crossed).filter_map(|fill| fill.tid).collect();

        for fill in fills {
            let fill_time_ms = fill.block_time_ms;

            if fill.crossed {
                continue;
            }

            let has_matching_taker = fill.tid.is_some_and(|tid| taker_tids.contains(&tid));

            if emit_rows && has_matching_taker {
                rows.push(OutputRow {
                    block_number: i64::try_from(fill.block_number)?,
                    block_time_ms: fill_time_ms,
                    coin: "BTC".to_owned(),
                    user: fill.user.clone(),
                    oid: Some(i64::try_from(fill.oid)?),
                    side: fill.side.clone(),
                    px: fill.px,
                    diff_type: "update".to_owned(),
                    event: "fill".to_owned(),
                    sz: None,
                    orig_sz: None,
                    raw_sz: None,
                    is_trigger: None,
                    tif: None,
                    reduce_only: None,
                    order_type: None,
                    trigger_condition: None,
                    trigger_px: None,
                    is_position_tpsl: None,
                    tp_trigger_px: None,
                    sl_trigger_px: None,
                    status: None,
                    lifetime: None,
                });
            }

            let Some(state) = orders.get_mut(&fill.oid) else {
                continue;
            };

            if state.remaining_sz <= Decimal::ZERO {
                remove_live_order(orders, cloid_to_oid, fill.oid);
                continue;
            }

            let mut new_remaining = state.remaining_sz - fill.sz;
            if new_remaining < Decimal::ZERO {
                new_remaining = Decimal::ZERO;
            }

            state.remaining_sz = new_remaining;

            if new_remaining <= Decimal::ZERO {
                remove_live_order(orders, cloid_to_oid, fill.oid);
            }
        }
    }

    Ok(())
}

fn build_output_tmp_path(output_dir: &Path, actual_start_block: u64) -> Result<PathBuf> {
    create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory: {}", output_dir.display()))?;
    let pid = std::process::id();
    let mut candidate = output_dir.join(format!(".tmp_btc_diff_start{actual_start_block}_pid{pid}.parquet"));
    if !candidate.exists() {
        return Ok(candidate);
    }
    for idx in 1..=9_999usize {
        candidate = output_dir.join(format!(".tmp_btc_diff_start{actual_start_block}_pid{pid}_{idx}.parquet"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("failed to allocate temp parquet file for start block {} under {}", actual_start_block, output_dir.display());
}

fn build_final_output_path(output_dir: &Path, actual_start_block: u64, actual_end_block: u64) -> Result<PathBuf> {
    create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory: {}", output_dir.display()))?;
    let mut candidate = output_dir.join(format!("btc_diff_{actual_start_block}_{actual_end_block}.parquet"));
    if !candidate.exists() {
        return Ok(candidate);
    }
    for idx in 1..=9_999usize {
        candidate = output_dir.join(format!("btc_diff_{actual_start_block}_{actual_end_block}_dup{idx}.parquet"));
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

fn open_output_writer(path: &Path, schema: &Arc<Schema>) -> Result<ArrowWriter<File>> {
    let file = File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(PARQUET_MAX_ROW_GROUP_ROWS)
        .build();
    ArrowWriter::try_new(file, Arc::clone(schema), Some(props)).context("creating parquet writer")
}

fn flush_rows_to_writer(writer: &mut ArrowWriter<File>, rows: &mut RowBuffer, schema: &Arc<Schema>) -> Result<()> {
    if let Some(batch) = rows.take_batch(schema)? {
        writer.write(&batch)?;
        writer.flush()?;
    }
    Ok(())
}

fn close_output_writer(mut writer: ArrowWriter<File>, rows: &mut RowBuffer, schema: &Arc<Schema>) -> Result<()> {
    flush_rows_to_writer(&mut writer, rows, schema)?;
    writer.close()?;
    Ok(())
}

fn close_and_finalize_output_file(
    output_dir: &Path,
    writer: ArrowWriter<File>,
    rows: &mut RowBuffer,
    schema: &Arc<Schema>,
    tmp_path: &Path,
    actual_start_block: u64,
    actual_end_block: u64,
) -> Result<PathBuf> {
    close_output_writer(writer, rows, schema)?;
    let final_path = build_final_output_path(output_dir, actual_start_block, actual_end_block)?;
    rename(tmp_path, &final_path).with_context(|| {
        format!("failed to rename parquet temp file {} -> {}", tmp_path.display(), final_path.display())
    })?;
    Ok(final_path)
}

#[allow(clippy::too_many_arguments)]
fn ensure_output_ready_for_block(
    output_dir: &Path,
    schema: &Arc<Schema>,
    block_number: u64,
    emit_rows: bool,
    writer: &mut Option<ArrowWriter<File>>,
    current_tmp_output_path: &mut Option<PathBuf>,
    current_file_window_start: &mut Option<u64>,
    current_row_group_window_start: &mut Option<u64>,
    current_file_actual_start_block: &mut Option<u64>,
    last_emitted_block: Option<u64>,
    generated_output_files: &mut Vec<PathBuf>,
    rows: &mut RowBuffer,
) -> Result<()> {
    if !emit_rows {
        return Ok(());
    }

    let next_file_window = file_window_start_block(block_number);
    let next_row_group_window = row_group_window_start_block(block_number);

    if writer.is_none() {
        let tmp_path = build_output_tmp_path(output_dir, block_number)?;
        *writer = Some(open_output_writer(&tmp_path, schema)?);
        *current_tmp_output_path = Some(tmp_path.clone());
        *current_file_window_start = Some(next_file_window);
        *current_row_group_window_start = Some(next_row_group_window);
        *current_file_actual_start_block = Some(block_number);
        eprintln!("[file] open parquet_tmp={} start_block={}", tmp_path.display(), block_number);
        return Ok(());
    }

    if *current_file_window_start != Some(next_file_window) {
        if let Some(active_writer) = writer.as_mut() {
            flush_rows_to_writer(active_writer, rows, schema)?;
        }
        if let Some(active_writer) = writer.take() {
            let closed_start = current_file_actual_start_block.unwrap_or(block_number);
            let closed_end = last_emitted_block.unwrap_or(block_number);
            let tmp_path =
                current_tmp_output_path.take().ok_or_else(|| anyhow!("missing current tmp parquet path on rotate"))?;
            let final_path = close_and_finalize_output_file(
                output_dir,
                active_writer,
                rows,
                schema,
                &tmp_path,
                closed_start,
                closed_end,
            )?;
            generated_output_files.push(final_path.clone());
            eprintln!("[file] close parquet={} actual_range={}..{}", final_path.display(), closed_start, closed_end);
        }

        let tmp_path = build_output_tmp_path(output_dir, block_number)?;
        *writer = Some(open_output_writer(&tmp_path, schema)?);
        *current_tmp_output_path = Some(tmp_path.clone());
        *current_file_window_start = Some(next_file_window);
        *current_row_group_window_start = Some(next_row_group_window);
        *current_file_actual_start_block = Some(block_number);
        eprintln!("[file] open parquet_tmp={} start_block={}", tmp_path.display(), block_number);
        return Ok(());
    }

    if *current_row_group_window_start != Some(next_row_group_window) {
        if let Some(active_writer) = writer.as_mut() {
            flush_rows_to_writer(active_writer, rows, schema)?;
        }
        *current_row_group_window_start = Some(next_row_group_window);
    }

    Ok(())
}

async fn fetch_next_pair(
    replica_reader: &mut ReplicaCmdsReader,
    fills_reader: &mut NodeFillsReader,
) -> Result<(Option<ReplicaBatch>, Option<NodeFillsBatch>)> {
    let (next_replica, next_fills) = tokio::try_join!(replica_reader.next_batch(), fills_reader.next_batch())?;
    Ok((next_replica, next_fills))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let requester_pays = REQUESTER_PAYS_ALWAYS;
    eprintln!("[config] json_parser=simd-json");
    if args.start_height.is_some() ^ args.height_span.is_some() {
        bail!("--start-height and --height-span must be provided together");
    }
    let output_block_range = match (args.start_height, args.height_span) {
        (Some(start_height), Some(height_span)) => {
            if height_span == 0 {
                bail!("--height-span must be > 0");
            }
            let end_height = start_height
                .checked_add(height_span - 1)
                .ok_or_else(|| anyhow!("height overflow: start_height={start_height} span={height_span}"))?;
            Some((start_height, end_height))
        }
        _ => None,
    };
    let read_block_range = output_block_range.map(|(start_height, end_height)| {
        let warmup_start = start_height.saturating_sub(args.warmup).max(1);
        (warmup_start, end_height)
    });
    let mut warmup_end_logged = false;
    if let (Some((warmup_start, _)), Some((output_start, _))) = (read_block_range, output_block_range)
        && warmup_start < output_start
    {
        eprintln!(
            "[warmup] start warmup_blocks={} warmup_range={}..{} output_start={}",
            output_start - warmup_start,
            warmup_start,
            output_start - 1,
            output_start
        );
    }

    let replica_source = InputSource::parse(&args.replica_cmds).context("parsing --replica_cmds")?;
    let node_fills_source = InputSource::parse(&args.node_fills_by_block).context("parsing --node_fills_by_block")?;
    let s3_client = if replica_source.requires_s3() || node_fills_source.requires_s3() {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
        Some(Arc::new(S3Client::new(&config)))
    } else {
        None
    };

    let replica_reader_source = match &replica_source {
        InputSource::LocalFile(path) => {
            if output_block_range.is_some() {
                bail!("--start-height/--height-span currently require S3 replica_cmds prefix input");
            }
            ReaderSource::LocalFile(path.clone())
        }
        InputSource::S3Prefix { bucket, prefix } => {
            let Some((start_height, end_height)) = read_block_range else {
                bail!("S3 replica_cmds requires --start-height and --height-span");
            };
            let height_span = end_height - start_height + 1;
            let Some(client) = s3_client.as_ref() else {
                bail!("S3 replica_cmds requires initialized S3 client");
            };
            let keys =
                resolve_replica_keys_for_range(client, bucket, prefix, start_height, height_span, requester_pays)
                    .await
                    .context("resolving replica_cmds keys from S3 layout")?;
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

    let mut fills_reader =
        NodeFillsReader::new(fills_reader_source.clone(), s3_client.clone(), requester_pays, read_block_range)
            .await
            .context("opening node_fills_by_block")?;
    let mut replica_reader = ReplicaCmdsReader::new(
        replica_reader_source,
        fills_reader_source.clone(),
        args.block_time_tolerance_ns,
        s3_client.clone(),
        requester_pays,
        read_block_range,
    )
    .await
    .context("opening replica_cmds")?;

    if args.output_parquet.exists() && !args.output_parquet.is_dir() {
        bail!(
            "--output-parquet now expects a directory path, but got existing non-directory: {}",
            args.output_parquet.display()
        );
    }
    create_dir_all(&args.output_parquet)
        .with_context(|| format!("failed to create output parquet directory: {}", args.output_parquet.display()))?;

    let schema = output_schema();
    let mut writer: Option<ArrowWriter<File>> = None;
    let mut current_tmp_output_path: Option<PathBuf> = None;
    let mut current_file_window_start: Option<u64> = None;
    let mut current_row_group_window_start: Option<u64> = None;
    let mut current_file_actual_start_block: Option<u64> = None;
    let mut last_emitted_block: Option<u64> = None;
    let mut generated_output_files: Vec<PathBuf> = Vec::new();

    let mut rows = RowBuffer::default();
    let mut orders: HashMap<u64, OrderState> = HashMap::new();
    let mut cloid_to_oid: HashMap<String, HashMap<String, u64>> = HashMap::new();
    let mut trigger_refs = TriggerOrderRefs::default();
    let mut known_non_trigger_oids: HashMap<u64, KnownOidState> = HashMap::new();
    let mut unknown_oid_logger = UnknownOidWindowLogger::new(&args.unknown_oid_log_sqlite)?;
    let started_at = Instant::now();
    let mut processed_blocks: u64 = 0;
    eprintln!("[phase] processing blocks ...");
    let (mut next_replica, mut next_fills) = fetch_next_pair(&mut replica_reader, &mut fills_reader).await?;

    while next_replica.is_some() || next_fills.is_some() {
        match (&next_replica, &next_fills) {
            (Some(replica), Some(fills)) if replica.block_number == fills.block_number => {
                let block_time_delta = (replica.block_time_ms - fills.block_time_ms).abs();
                if block_time_delta > args.block_time_match_tolerance_ms {
                    unknown_oid_logger.finish()?;
                    if let Some(active_writer) = writer.take() {
                        let closed_start = current_file_actual_start_block.unwrap_or(replica.block_number);
                        let closed_end = last_emitted_block.unwrap_or(replica.block_number);
                        let tmp_path = current_tmp_output_path
                            .take()
                            .ok_or_else(|| anyhow!("missing current tmp parquet path at timestamp mismatch"))?;
                        let final_path = close_and_finalize_output_file(
                            &args.output_parquet,
                            active_writer,
                            &mut rows,
                            &schema,
                            &tmp_path,
                            closed_start,
                            closed_end,
                        )?;
                        generated_output_files.push(final_path.clone());
                        eprintln!(
                            "[file] close parquet={} actual_range={}..{}",
                            final_path.display(),
                            closed_start,
                            closed_end
                        );
                    }
                    bail!(
                        "block {} timestamp mismatch: replica={}ms fills={}ms delta={}ms tolerance={}ms",
                        replica.block_number,
                        replica.block_time_ms,
                        fills.block_time_ms,
                        block_time_delta,
                        args.block_time_match_tolerance_ms
                    );
                }

                let replica = next_replica.take().unwrap();
                let fills = next_fills.take().unwrap();
                let emit_rows =
                    output_block_range.map(|(start_height, _)| replica.block_number >= start_height).unwrap_or(true);
                ensure_output_ready_for_block(
                    &args.output_parquet,
                    &schema,
                    replica.block_number,
                    emit_rows,
                    &mut writer,
                    &mut current_tmp_output_path,
                    &mut current_file_window_start,
                    &mut current_row_group_window_start,
                    &mut current_file_actual_start_block,
                    last_emitted_block,
                    &mut generated_output_files,
                    &mut rows,
                )?;
                process_block(
                    replica.block_number,
                    replica.block_time_ms,
                    Some(replica.events),
                    Some(fills.fills),
                    emit_rows,
                    emit_rows,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    &mut trigger_refs,
                    &mut known_non_trigger_oids,
                    &mut unknown_oid_logger,
                )?;
                if emit_rows {
                    last_emitted_block = Some(replica.block_number);
                    if !warmup_end_logged
                        && let (Some((warmup_start, _)), Some((output_start, _))) =
                            (read_block_range, output_block_range)
                        && warmup_start < output_start
                    {
                        eprintln!(
                            "[warmup] end at block={} (output starts from block {})",
                            replica.block_number, output_start
                        );
                        warmup_end_logged = true;
                    }
                }
                processed_blocks += 1;
                if processed_blocks % 100 == 0 {
                    eprintln!(
                        "[progress] blocks={} last_block={} live_orders={} buffered_rows={} elapsed={:.1}s",
                        processed_blocks,
                        replica.block_number,
                        orders.len(),
                        rows.len(),
                        started_at.elapsed().as_secs_f64()
                    );
                }
                let (fetched_replica, fetched_fills) = fetch_next_pair(&mut replica_reader, &mut fills_reader).await?;
                next_replica = fetched_replica;
                next_fills = fetched_fills;
            }
            (Some(replica), Some(fills)) if replica.block_number < fills.block_number => {
                let replica = next_replica.take().unwrap();
                let emit_rows =
                    output_block_range.map(|(start_height, _)| replica.block_number >= start_height).unwrap_or(true);
                ensure_output_ready_for_block(
                    &args.output_parquet,
                    &schema,
                    replica.block_number,
                    emit_rows,
                    &mut writer,
                    &mut current_tmp_output_path,
                    &mut current_file_window_start,
                    &mut current_row_group_window_start,
                    &mut current_file_actual_start_block,
                    last_emitted_block,
                    &mut generated_output_files,
                    &mut rows,
                )?;
                process_block(
                    replica.block_number,
                    replica.block_time_ms,
                    Some(replica.events),
                    None,
                    emit_rows,
                    emit_rows,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    &mut trigger_refs,
                    &mut known_non_trigger_oids,
                    &mut unknown_oid_logger,
                )?;
                if emit_rows {
                    last_emitted_block = Some(replica.block_number);
                    if !warmup_end_logged
                        && let (Some((warmup_start, _)), Some((output_start, _))) =
                            (read_block_range, output_block_range)
                        && warmup_start < output_start
                    {
                        eprintln!(
                            "[warmup] end at block={} (output starts from block {})",
                            replica.block_number, output_start
                        );
                        warmup_end_logged = true;
                    }
                }
                processed_blocks += 1;
                if processed_blocks % 100 == 0 {
                    eprintln!(
                        "[progress] blocks={} last_block={} live_orders={} buffered_rows={} elapsed={:.1}s",
                        processed_blocks,
                        replica.block_number,
                        orders.len(),
                        rows.len(),
                        started_at.elapsed().as_secs_f64()
                    );
                }
                next_replica = replica_reader.next_batch().await?;
            }
            (Some(_), Some(_)) => {
                let fills = next_fills.take().unwrap();
                let emit_rows =
                    output_block_range.map(|(start_height, _)| fills.block_number >= start_height).unwrap_or(true);
                ensure_output_ready_for_block(
                    &args.output_parquet,
                    &schema,
                    fills.block_number,
                    emit_rows,
                    &mut writer,
                    &mut current_tmp_output_path,
                    &mut current_file_window_start,
                    &mut current_row_group_window_start,
                    &mut current_file_actual_start_block,
                    last_emitted_block,
                    &mut generated_output_files,
                    &mut rows,
                )?;
                process_block(
                    fills.block_number,
                    fills.block_time_ms,
                    None,
                    Some(fills.fills),
                    emit_rows,
                    emit_rows,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    &mut trigger_refs,
                    &mut known_non_trigger_oids,
                    &mut unknown_oid_logger,
                )?;
                if emit_rows {
                    last_emitted_block = Some(fills.block_number);
                    if !warmup_end_logged
                        && let (Some((warmup_start, _)), Some((output_start, _))) =
                            (read_block_range, output_block_range)
                        && warmup_start < output_start
                    {
                        eprintln!(
                            "[warmup] end at block={} (output starts from block {})",
                            fills.block_number, output_start
                        );
                        warmup_end_logged = true;
                    }
                }
                processed_blocks += 1;
                if processed_blocks % 100 == 0 {
                    eprintln!(
                        "[progress] blocks={} last_block={} live_orders={} buffered_rows={} elapsed={:.1}s",
                        processed_blocks,
                        fills.block_number,
                        orders.len(),
                        rows.len(),
                        started_at.elapsed().as_secs_f64()
                    );
                }
                next_fills = fills_reader.next_batch().await?;
            }
            (Some(replica), None) => {
                unknown_oid_logger.finish()?;
                if let Some(active_writer) = writer.take() {
                    let closed_start = current_file_actual_start_block.unwrap_or(replica.block_number);
                    let closed_end = last_emitted_block.unwrap_or(replica.block_number);
                    let tmp_path = current_tmp_output_path
                        .take()
                        .ok_or_else(|| anyhow!("missing current tmp parquet path at replica eof"))?;
                    let final_path = close_and_finalize_output_file(
                        &args.output_parquet,
                        active_writer,
                        &mut rows,
                        &schema,
                        &tmp_path,
                        closed_start,
                        closed_end,
                    )?;
                    generated_output_files.push(final_path.clone());
                    eprintln!(
                        "[file] close parquet={} actual_range={}..{}",
                        final_path.display(),
                        closed_start,
                        closed_end
                    );
                }
                bail!(
                    "node_fills_by_block hit EOF before replica_cmds; unpaired replica block {} remains",
                    replica.block_number
                );
            }
            (None, Some(fills)) => {
                unknown_oid_logger.finish()?;
                if let Some(active_writer) = writer.take() {
                    let closed_start = current_file_actual_start_block.unwrap_or(fills.block_number);
                    let closed_end = last_emitted_block.unwrap_or(fills.block_number);
                    let tmp_path = current_tmp_output_path
                        .take()
                        .ok_or_else(|| anyhow!("missing current tmp parquet path at fills eof"))?;
                    let final_path = close_and_finalize_output_file(
                        &args.output_parquet,
                        active_writer,
                        &mut rows,
                        &schema,
                        &tmp_path,
                        closed_start,
                        closed_end,
                    )?;
                    generated_output_files.push(final_path.clone());
                    eprintln!(
                        "[file] close parquet={} actual_range={}..{}",
                        final_path.display(),
                        closed_start,
                        closed_end
                    );
                }
                bail!(
                    "replica_cmds hit EOF before node_fills_by_block; unpaired fills block {} remains",
                    fills.block_number
                );
            }
            (None, None) => break,
        }
    }

    unknown_oid_logger.finish()?;
    if let Some(active_writer) = writer.take() {
        let closed_start = current_file_actual_start_block.unwrap_or(0);
        let closed_end = last_emitted_block.unwrap_or(0);
        let tmp_path =
            current_tmp_output_path.take().ok_or_else(|| anyhow!("missing current tmp parquet path at final close"))?;
        let final_path = close_and_finalize_output_file(
            &args.output_parquet,
            active_writer,
            &mut rows,
            &schema,
            &tmp_path,
            closed_start,
            closed_end,
        )?;
        generated_output_files.push(final_path.clone());
        eprintln!("[file] close parquet={} actual_range={}..{}", final_path.display(), closed_start, closed_end);
    }

    println!(
        "Wrote BTC diff parquet files={} output_dir={} unknown_oid_sqlite={} requester_pays={} remaining_live_orders={}",
        generated_output_files.len(),
        args.output_parquet.display(),
        args.unknown_oid_log_sqlite.display(),
        requester_pays,
        orders.len()
    );
    for path in &generated_output_files {
        println!("  - {}", path.display());
    }

    Ok(())
}
