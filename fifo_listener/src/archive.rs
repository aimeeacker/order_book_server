use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TryRecvError, channel, sync_channel};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use aliyun_oss_rust_sdk::oss::OSS;
use aliyun_oss_rust_sdk::request::RequestBuilder;
use compute_l4::{ComputeOptions, append_l4_checkpoint_from_snapshot_json};
use log::{error, info, warn};
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;

const ARCHIVE_BASE_DIR: &str = "/home/aimee/hl_runtime/hl/dataset";
const DEFAULT_ARCHIVE_FINALIZE_DIR: &str = "/mnt";
const DEFAULT_ARCHIVE_SYMBOLS: &[&str] = &["BTC", "ETH"];
const DEFAULT_OSS_PREFIX: &str = "hyper_data";
const INFO_SNAPSHOT_URL: &str = "http://localhost:3001/info";
const TEMP_FILE_SUFFIX: &str = ".tmp";
const LITE_PRICE_SCALE: u32 = 8;
const LITE_SIZE_SCALE: u32 = 8;
const CHECKPOINT_BLOCKS: u64 = 10_000;
const ARCHIVE_OUTPUT_ALIGN_BLOCKS: u64 = 1_000;
const STATUS_ROW_GROUP_BLOCKS_DEFAULT: u64 = 10_000;
const STATUS_ROW_GROUP_BLOCKS_BTC: u64 = 1_000;
const STATUS_ROW_GROUP_BLOCKS_ETH: u64 = 2_000;
const STATUS_ROW_GROUP_BLOCKS_SOL_HYPE: u64 = 5_000;
const DIFF_ROW_GROUP_BLOCKS: u64 = 50_000;
const DIFF_DELAYED_FLUSH_LOOKAHEAD_BLOCKS: u64 = 2_000;
const DIFF_STAGGER_DELAY: Duration = Duration::from_millis(1_500);
const BLOCKS_FILL_ROTATION_BLOCKS: u64 = 1_000_000;
const BLOCKS_FILL_ROW_GROUP_BLOCKS: u64 = 250_000;
const BLOCKS_FILL_DELAYED_FLUSH_LOOKAHEAD_BLOCKS: u64 = 2_000;
const MIN_HANDOFF_BLOCK_SPAN: u64 = 5_000;
const MIN_ARCHIVE_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_DISK_USED_BPS: u64 = 9_500;
const STATUS_WORKER_QUEUE_BLOCKS: usize = 16;
const DIFF_WORKER_QUEUE_BLOCKS: usize = 16;
static ROTATION_BLOCKS: AtomicU64 = AtomicU64::new(100_000);
static ARCHIVE_ALIGN_START_TO_10K_BOUNDARY: AtomicBool = AtomicBool::new(true);
static ARCHIVE_ALIGN_OUTPUT_TO_1000_BOUNDARY: AtomicBool = AtomicBool::new(true);
static ARCHIVE_RECOVER_BLOCKS_FILL_ON_STOP: AtomicBool = AtomicBool::new(false);
static ARCHIVE_BASE_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static ARCHIVE_SYMBOLS_OVERRIDE: OnceLock<Mutex<Option<Vec<String>>>> = OnceLock::new();
static ARCHIVE_HANDOFF_CONFIG: OnceLock<Mutex<ArchiveHandoffConfig>> = OnceLock::new();
pub(crate) const ARCHIVE_QUEUE_BLOCKS: usize = 127;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveMode {
    Lite = 1,
    Full = 2,
}

static ARCHIVE_MODE: AtomicU8 = AtomicU8::new(0);

#[derive(Clone, PartialEq, Eq)]
pub struct ArchiveOssConfig {
    access_key_id: String,
    access_key_secret: String,
    endpoint: String,
    bucket: String,
    prefix: String,
}

impl ArchiveOssConfig {
    pub fn new(
        access_key_id: String,
        access_key_secret: String,
        endpoint: String,
        bucket: String,
        prefix: Option<String>,
    ) -> Self {
        Self { access_key_id, access_key_secret, endpoint, bucket, prefix: normalize_oss_prefix(prefix) }
    }
}

impl std::fmt::Debug for ArchiveOssConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveOssConfig")
            .field("access_key_id", &redact_secret(&self.access_key_id))
            .field("access_key_secret", &"<redacted>")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArchiveHandoffConfig {
    pub move_to_nas: bool,
    pub nas_output_dir: PathBuf,
    pub upload_to_oss: bool,
    pub oss: Option<ArchiveOssConfig>,
}

impl ArchiveHandoffConfig {
    pub fn new(
        move_to_nas: bool,
        nas_output_dir: Option<PathBuf>,
        upload_to_oss: bool,
        oss: Option<ArchiveOssConfig>,
    ) -> Self {
        Self {
            move_to_nas,
            nas_output_dir: nas_output_dir.unwrap_or_else(default_archive_finalize_dir),
            upload_to_oss,
            oss,
        }
    }
}

impl Default for ArchiveHandoffConfig {
    fn default() -> Self {
        Self { move_to_nas: true, nas_output_dir: default_archive_finalize_dir(), upload_to_oss: false, oss: None }
    }
}

impl std::fmt::Debug for ArchiveHandoffConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveHandoffConfig")
            .field("move_to_nas", &self.move_to_nas)
            .field("nas_output_dir", &self.nas_output_dir)
            .field("upload_to_oss", &self.upload_to_oss)
            .field("oss", &self.oss)
            .finish()
    }
}

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

fn archive_handoff_config() -> &'static Mutex<ArchiveHandoffConfig> {
    ARCHIVE_HANDOFF_CONFIG.get_or_init(|| Mutex::new(ArchiveHandoffConfig::default()))
}

fn redact_secret(value: &str) -> String {
    match value.len() {
        0 => String::new(),
        1..=4 => "*".repeat(value.len()),
        len => format!("{}***{}", &value[..2], &value[len - 2..]),
    }
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

fn normalize_oss_prefix(prefix: Option<String>) -> String {
    prefix.unwrap_or_else(|| DEFAULT_OSS_PREFIX.to_string()).trim_matches('/').to_string()
}

fn default_archive_finalize_dir() -> PathBuf {
    PathBuf::from(DEFAULT_ARCHIVE_FINALIZE_DIR)
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

pub(crate) fn current_archive_handoff_config() -> ArchiveHandoffConfig {
    archive_handoff_config().lock().expect("archive handoff config mutex").clone()
}

pub fn set_archive_handoff_config(config: ArchiveHandoffConfig) -> ArchiveHandoffConfig {
    let mut guard = archive_handoff_config().lock().expect("archive handoff config mutex");
    *guard = config;
    guard.clone()
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

pub fn set_archive_align_start_to_10k_boundary(enabled: bool) {
    ARCHIVE_ALIGN_START_TO_10K_BOUNDARY.store(enabled, Ordering::SeqCst);
}

pub fn set_archive_align_output_to_1000_boundary(enabled: bool) {
    ARCHIVE_ALIGN_OUTPUT_TO_1000_BOUNDARY.store(enabled, Ordering::SeqCst);
}

pub fn set_archive_recover_blocks_fill_on_stop(enabled: bool) {
    ARCHIVE_RECOVER_BLOCKS_FILL_ON_STOP.store(enabled, Ordering::SeqCst);
}

pub(crate) fn current_archive_align_start_to_10k_boundary() -> bool {
    ARCHIVE_ALIGN_START_TO_10K_BOUNDARY.load(Ordering::SeqCst)
}

pub(crate) fn current_archive_align_output_to_1000_boundary() -> bool {
    ARCHIVE_ALIGN_OUTPUT_TO_1000_BOUNDARY.load(Ordering::SeqCst)
}

pub(crate) fn current_archive_recover_blocks_fill_on_stop() -> bool {
    ARCHIVE_RECOVER_BLOCKS_FILL_ON_STOP.load(Ordering::SeqCst)
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

#[derive(Debug, Default)]
struct StatusLiteColumns {
    block_number: Vec<i64>,
    block_time: Vec<ByteArray>,
    time: Vec<ByteArray>,
    user: Vec<ByteArray>,
    status: Vec<ByteArray>,
    oid: Vec<i64>,
    side: Vec<ByteArray>,
    limit_px: Vec<i64>,
    sz: Vec<i64>,
    orig_sz: Vec<i64>,
    timestamp: Vec<i64>,
    is_trigger: Vec<bool>,
    tif: Vec<ByteArray>,
    trigger_condition: Vec<ByteArray>,
    trigger_px: Vec<i64>,
    is_position_tpsl: Vec<bool>,
    reduce_only: Vec<bool>,
    order_type: Vec<ByteArray>,
    cloid: Vec<ByteArray>,
}

impl StatusLiteColumns {
    fn is_empty(&self) -> bool {
        self.block_number.is_empty()
    }

    fn append(&mut self, other: StatusLiteColumns) {
        self.block_number.extend(other.block_number);
        self.block_time.extend(other.block_time);
        self.time.extend(other.time);
        self.user.extend(other.user);
        self.status.extend(other.status);
        self.oid.extend(other.oid);
        self.side.extend(other.side);
        self.limit_px.extend(other.limit_px);
        self.sz.extend(other.sz);
        self.orig_sz.extend(other.orig_sz);
        self.timestamp.extend(other.timestamp);
        self.is_trigger.extend(other.is_trigger);
        self.tif.extend(other.tif);
        self.trigger_condition.extend(other.trigger_condition);
        self.trigger_px.extend(other.trigger_px);
        self.is_position_tpsl.extend(other.is_position_tpsl);
        self.reduce_only.extend(other.reduce_only);
        self.order_type.extend(other.order_type);
        self.cloid.extend(other.cloid);
    }

    fn trim_to_block(&mut self, max_block: u64) {
        let keep = self.block_number.partition_point(|block| *block <= max_block as i64);
        self.block_number.truncate(keep);
        self.block_time.truncate(keep);
        self.time.truncate(keep);
        self.user.truncate(keep);
        self.status.truncate(keep);
        self.oid.truncate(keep);
        self.side.truncate(keep);
        self.limit_px.truncate(keep);
        self.sz.truncate(keep);
        self.orig_sz.truncate(keep);
        self.timestamp.truncate(keep);
        self.is_trigger.truncate(keep);
        self.tif.truncate(keep);
        self.trigger_condition.truncate(keep);
        self.trigger_px.truncate(keep);
        self.is_position_tpsl.truncate(keep);
        self.reduce_only.truncate(keep);
        self.order_type.truncate(keep);
        self.cloid.truncate(keep);
    }
}

#[derive(Debug, Default)]
struct StatusFullColumns {
    block_number: Vec<i64>,
    block_time: Vec<ByteArray>,
    status: Vec<ByteArray>,
    oid: Vec<i64>,
    side: Vec<ByteArray>,
    limit_px: Vec<i64>,
    is_trigger: Vec<bool>,
    tif: Vec<ByteArray>,
    user: Vec<ByteArray>,
    hash: Vec<ByteArray>,
    order_type: Vec<ByteArray>,
    sz: Vec<i64>,
    orig_sz: Vec<i64>,
    time: Vec<ByteArray>,
    builder: Vec<ByteArray>,
    timestamp: Vec<i64>,
    trigger_condition: Vec<ByteArray>,
    trigger_px: Vec<i64>,
    is_position_tpsl: Vec<bool>,
    reduce_only: Vec<bool>,
    cloid: Vec<ByteArray>,
    raw_event: Vec<ByteArray>,
}

impl StatusFullColumns {
    fn is_empty(&self) -> bool {
        self.block_number.is_empty()
    }

    fn append(&mut self, other: StatusFullColumns) {
        self.block_number.extend(other.block_number);
        self.block_time.extend(other.block_time);
        self.status.extend(other.status);
        self.oid.extend(other.oid);
        self.side.extend(other.side);
        self.limit_px.extend(other.limit_px);
        self.is_trigger.extend(other.is_trigger);
        self.tif.extend(other.tif);
        self.user.extend(other.user);
        self.hash.extend(other.hash);
        self.order_type.extend(other.order_type);
        self.sz.extend(other.sz);
        self.orig_sz.extend(other.orig_sz);
        self.time.extend(other.time);
        self.builder.extend(other.builder);
        self.timestamp.extend(other.timestamp);
        self.trigger_condition.extend(other.trigger_condition);
        self.trigger_px.extend(other.trigger_px);
        self.is_position_tpsl.extend(other.is_position_tpsl);
        self.reduce_only.extend(other.reduce_only);
        self.cloid.extend(other.cloid);
        self.raw_event.extend(other.raw_event);
    }

    fn trim_to_block(&mut self, max_block: u64) {
        let keep = self.block_number.partition_point(|block| *block <= max_block as i64);
        self.block_number.truncate(keep);
        self.block_time.truncate(keep);
        self.status.truncate(keep);
        self.oid.truncate(keep);
        self.side.truncate(keep);
        self.limit_px.truncate(keep);
        self.is_trigger.truncate(keep);
        self.tif.truncate(keep);
        self.user.truncate(keep);
        self.hash.truncate(keep);
        self.order_type.truncate(keep);
        self.sz.truncate(keep);
        self.orig_sz.truncate(keep);
        self.time.truncate(keep);
        self.builder.truncate(keep);
        self.timestamp.truncate(keep);
        self.trigger_condition.truncate(keep);
        self.trigger_px.truncate(keep);
        self.is_position_tpsl.truncate(keep);
        self.reduce_only.truncate(keep);
        self.cloid.truncate(keep);
        self.raw_event.truncate(keep);
    }
}

#[derive(Debug)]
enum StatusBlockBatch {
    Lite(StatusLiteColumns),
    Full(StatusFullColumns),
}

impl StatusBlockBatch {
    fn new(mode: ArchiveMode) -> Self {
        match mode {
            ArchiveMode::Lite => Self::Lite(StatusLiteColumns::default()),
            ArchiveMode::Full => Self::Full(StatusFullColumns::default()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Lite(columns) => columns.is_empty(),
            Self::Full(columns) => columns.is_empty(),
        }
    }

    fn trim_to_block(&mut self, max_block: u64) {
        match self {
            Self::Lite(columns) => columns.trim_to_block(max_block),
            Self::Full(columns) => columns.trim_to_block(max_block),
        }
    }
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

impl HasBlockNumber for DiffOut {
    fn block_number(&self) -> u64 {
        self.block_number
    }
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

impl HasBlockNumber for FillOut {
    fn block_number(&self) -> u64 {
        self.block_number
    }
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

impl HasBlockNumber for BlockIndexOut {
    fn block_number(&self) -> u64 {
        self.block_number
    }
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

    fn row_group_block_limit(self) -> Option<u64> {
        match self {
            Self::Status => Some(STATUS_ROW_GROUP_BLOCKS_DEFAULT),
            Self::Diff => Some(DIFF_ROW_GROUP_BLOCKS),
            Self::Blocks | Self::Fill => Some(BLOCKS_FILL_ROW_GROUP_BLOCKS),
        }
    }

    fn uses_delayed_flush(self) -> bool {
        matches!(self, Self::Diff | Self::Blocks | Self::Fill)
    }

    fn rotation_block_limit(self) -> u64 {
        match self {
            Self::Blocks | Self::Fill => BLOCKS_FILL_ROTATION_BLOCKS,
            Self::Status | Self::Diff => ROTATION_BLOCKS.load(Ordering::SeqCst),
        }
    }

    fn delayed_flush_lookahead_blocks(self) -> Option<u64> {
        match self {
            Self::Diff => Some(DIFF_DELAYED_FLUSH_LOOKAHEAD_BLOCKS),
            Self::Blocks | Self::Fill => Some(BLOCKS_FILL_DELAYED_FLUSH_LOOKAHEAD_BLOCKS),
            Self::Status => None,
        }
    }

    fn supports_local_recovery(self) -> bool {
        matches!(self, Self::Blocks | Self::Fill)
    }
}

struct ParquetFile {
    writer: SerializedFileWriter<std::fs::File>,
    window_start_block: u64,
    window_end_block: u64,
    replay_skip_until_block: u64,
    name_start_block: u64,
    name_end_block: u64,
    last_block: u64,
    tmp_path: PathBuf,
    local_final_path: PathBuf,
    relative_dir: PathBuf,
    final_filename_prefix: String,
    final_filename_suffix: String,
}

fn status_row_group_blocks_for_coin(coin: &str) -> u64 {
    match coin {
        "BTC" => STATUS_ROW_GROUP_BLOCKS_BTC,
        "ETH" => STATUS_ROW_GROUP_BLOCKS_ETH,
        "SOL" => STATUS_ROW_GROUP_BLOCKS_SOL_HYPE,
        _ => STATUS_ROW_GROUP_BLOCKS_DEFAULT,
    }
}

fn archive_coin_dir(mode: ArchiveMode, coin: &str) -> PathBuf {
    match mode {
        ArchiveMode::Lite => PathBuf::from(coin),
        ArchiveMode::Full => PathBuf::from(format!("{coin}_full")),
    }
}

fn archive_relative_dir(stream: StreamKind, mode: ArchiveMode, coin: &str) -> PathBuf {
    match stream {
        StreamKind::Blocks => PathBuf::new(),
        StreamKind::Fill => archive_coin_dir(mode, coin),
        StreamKind::Diff | StreamKind::Status => archive_coin_dir(mode, coin).join(stream.name()),
    }
}

fn aligned_archive_output_end(block: u64) -> Option<u64> {
    let aligned = block.saturating_sub(block % ARCHIVE_OUTPUT_ALIGN_BLOCKS);
    (aligned > 0).then_some(aligned)
}

struct ArchiveDiskStatus {
    available_bytes: u64,
    used_basis_points: u64,
}

#[derive(Clone)]
struct HandoffTask {
    path: PathBuf,
    relative_path: PathBuf,
    config: ArchiveHandoffConfig,
}

enum HandoffMessage {
    File(HandoffTask),
    Barrier(Sender<()>),
}

struct ArchiveHandoffWorker {
    tx: Sender<HandoffMessage>,
    handle: thread::JoinHandle<()>,
}

struct PendingRowGroup<R> {
    rows: Vec<R>,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

impl<R> PendingRowGroup<R> {
    fn new() -> Self {
        Self { rows: Vec::new(), start_block: None, end_block: None }
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.start_block = None;
        self.end_block = None;
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn take_rows(&mut self) -> Vec<R> {
        self.start_block = None;
        self.end_block = None;
        std::mem::take(&mut self.rows)
    }
}

trait HasBlockNumber {
    fn block_number(&self) -> u64;
}

trait RecoverableArchiveRow: HasBlockNumber + Sized {
    fn read_local_rows(path: &Path, mode: ArchiveMode) -> parquet::errors::Result<Vec<Self>>;
    fn write_rows(
        file: &mut SerializedFileWriter<std::fs::File>,
        mode: ArchiveMode,
        rows: &[Self],
    ) -> parquet::errors::Result<()>;
}

impl RecoverableArchiveRow for DiffOut {
    fn read_local_rows(_path: &Path, _mode: ArchiveMode) -> parquet::errors::Result<Vec<Self>> {
        Err(io_to_parquet_error(io_other("diff rows do not support local parquet recovery")))
    }

    fn write_rows(
        file: &mut SerializedFileWriter<std::fs::File>,
        mode: ArchiveMode,
        rows: &[Self],
    ) -> parquet::errors::Result<()> {
        write_diff_rows(file, mode, rows)
    }
}

impl RecoverableArchiveRow for FillOut {
    fn read_local_rows(path: &Path, mode: ArchiveMode) -> parquet::errors::Result<Vec<Self>> {
        read_local_fill_rows(path, mode)
    }

    fn write_rows(
        file: &mut SerializedFileWriter<std::fs::File>,
        mode: ArchiveMode,
        rows: &[Self],
    ) -> parquet::errors::Result<()> {
        write_fill_rows(file, mode, rows)
    }
}

impl RecoverableArchiveRow for BlockIndexOut {
    fn read_local_rows(path: &Path, _mode: ArchiveMode) -> parquet::errors::Result<Vec<Self>> {
        read_local_blocks_rows(path)
    }

    fn write_rows(
        file: &mut SerializedFileWriter<std::fs::File>,
        mode: ArchiveMode,
        rows: &[Self],
    ) -> parquet::errors::Result<()> {
        write_block_rows(file, mode, rows)
    }
}

impl<R: HasBlockNumber> PendingRowGroup<R> {
    fn trim_to_block(&mut self, max_block: u64) {
        self.rows.retain(|row| row.block_number() <= max_block);
        if self.rows.is_empty() {
            self.start_block = None;
            self.end_block = None;
        } else {
            self.start_block = self.rows.first().map(HasBlockNumber::block_number);
            self.end_block = self.rows.last().map(HasBlockNumber::block_number);
        }
    }
}

struct ParquetStreamWriter<R> {
    stream: StreamKind,
    schema: std::sync::Arc<Type>,
    props: std::sync::Arc<WriterProperties>,
    handoff_tx: Sender<HandoffMessage>,
    file: Option<ParquetFile>,
    pending_start_block: Option<u64>,
    active: PendingRowGroup<R>,
    delayed: PendingRowGroup<R>,
}

impl<R: HasBlockNumber + RecoverableArchiveRow> ParquetStreamWriter<R> {
    fn new(
        stream: StreamKind,
        schema: std::sync::Arc<Type>,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
    ) -> Self {
        Self {
            stream,
            schema,
            props,
            handoff_tx,
            file: None,
            pending_start_block: None,
            active: PendingRowGroup::new(),
            delayed: PendingRowGroup::new(),
        }
    }

    fn ensure_open(&mut self, coin: &str, mode: ArchiveMode, block: u64) -> parquet::errors::Result<()> {
        if self.file.is_none() {
            let (window_start, end) = rotation_bounds_for(self.stream, block);
            let name_start = self.pending_start_block.unwrap_or(block);
            self.open_file(coin, mode, name_start, block, window_start, end)?;
        }
        Ok(())
    }

    fn supports_local_recovery(&self) -> bool {
        self.stream.supports_local_recovery()
    }

    fn keep_local_on_close(&self) -> bool {
        self.stream.supports_local_recovery() && current_archive_recover_blocks_fill_on_stop()
    }

    fn local_recovery_relative_path(
        &self,
        coin: &str,
        mode: ArchiveMode,
        name_start_block: u64,
        last_block: u64,
        filename_prefix: &str,
        filename_suffix: &str,
    ) -> PathBuf {
        archive_relative_dir(self.stream, mode, coin)
            .join(format!("{}_{}_{}{}", filename_prefix, name_start_block, last_block, filename_suffix))
    }

    fn load_local_recovery(
        &self,
        base_dir: &Path,
        coin: &str,
        mode: ArchiveMode,
        window_end_block: u64,
        filename_prefix: &str,
        filename_suffix: &str,
    ) -> parquet::errors::Result<Option<LocalRecoveryFile<R>>> {
        if !self.supports_local_recovery() {
            return Ok(None);
        }
        let Some((path, name_start_block)) = find_local_recovery_path(base_dir, filename_prefix, window_end_block)?
        else {
            return Ok(None);
        };
        let rows = R::read_local_rows(&path, mode)?;
        let last_block = rows.last().map(HasBlockNumber::block_number).unwrap_or(0);
        if last_block == 0 {
            fs::remove_file(&path).map_err(io_to_parquet_error)?;
            return Ok(None);
        }
        let _unused = filename_suffix;
        let _unused = coin;
        Ok(Some(LocalRecoveryFile { path, name_start_block, rows, last_block }))
    }

    fn open_file(
        &mut self,
        coin: &str,
        mode: ArchiveMode,
        name_start: u64,
        actual_start: u64,
        window_start: u64,
        end: u64,
    ) -> parquet::errors::Result<()> {
        let base_dir = current_archive_base_dir();
        let relative_dir = archive_relative_dir(self.stream, mode, coin);
        let filename_prefix = match self.stream {
            StreamKind::Blocks => "blocks".to_string(),
            StreamKind::Fill | StreamKind::Diff | StreamKind::Status => format!("{}_{}", coin, self.stream.name()),
        };
        let filename_suffix = ".parquet".to_string();
        fs::create_dir_all(&base_dir).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        let mut effective_name_start = name_start;
        let mut local_final_path = base_dir.join(format!("{filename_prefix}_{name_start}_{end}{filename_suffix}"));
        let recovery = self.load_local_recovery(&base_dir, coin, mode, end, &filename_prefix, &filename_suffix)?;
        if let Some(existing) = recovery.as_ref() {
            if actual_start > existing.last_block.saturating_add(1) {
                let staged_path = stage_local_file_for_handoff(&existing.path)?;
                let relative_path = self.local_recovery_relative_path(
                    coin,
                    mode,
                    existing.name_start_block,
                    existing.last_block,
                    &filename_prefix,
                    &filename_suffix,
                );
                enqueue_handoff_task(&self.handoff_tx, staged_path, relative_path)?;
            } else {
                effective_name_start = existing.name_start_block;
                local_final_path = existing.path.clone();
            }
        }
        let tmp_path = local_final_path.with_file_name(format!(
            "{}{}",
            local_final_path.file_name().and_then(|name| name.to_str()).unwrap_or("archive.parquet"),
            TEMP_FILE_SUFFIX
        ));
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        }
        let file = fs::File::create(&tmp_path).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        let mut writer = SerializedFileWriter::new(file, self.schema.clone(), self.props.clone())?;
        let mut recovered_last_block = actual_start;
        let mut replay_skip_until_block = 0;
        if let Some(existing) = recovery {
            if actual_start <= existing.last_block.saturating_add(1) {
                rewrite_rows_into_writer(&mut writer, mode, self.stream, existing.rows, R::write_rows)?;
                recovered_last_block = existing.last_block;
                replay_skip_until_block = existing.last_block;
                info!(
                    "Recovered local {} parquet for {} window_end={} last_block={} and resumed writer",
                    self.stream.name(),
                    coin,
                    end,
                    recovered_last_block
                );
            }
        }
        self.file = Some(ParquetFile {
            writer,
            window_start_block: window_start,
            window_end_block: end,
            replay_skip_until_block,
            name_start_block: effective_name_start,
            name_end_block: recovered_last_block,
            last_block: recovered_last_block,
            tmp_path,
            local_final_path,
            relative_dir,
            final_filename_prefix: filename_prefix,
            final_filename_suffix: filename_suffix,
        });
        self.pending_start_block = None;
        Ok(())
    }

    fn close(&mut self) -> parquet::errors::Result<()> {
        if let Some(file) = self.file.take() {
            let tmp_path = file.tmp_path.clone();
            let final_path = file.local_final_path.clone();
            let relative_path = file.relative_dir.join(format!(
                "{}_{}_{}{}",
                file.final_filename_prefix, file.name_start_block, file.name_end_block, file.final_filename_suffix
            ));
            file.writer.close()?;
            fs::rename(&tmp_path, &final_path).map_err(io_to_parquet_error)?;
            let span_blocks = file.name_end_block.saturating_sub(file.name_start_block) + 1;
            if self.keep_local_on_close() && file.name_end_block < file.window_end_block {
                info!(
                    "Archive finalized locally without handoff for {} span={} path={}",
                    self.stream.name(),
                    span_blocks,
                    final_path.display()
                );
            } else if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
                info!(
                    "Archive finalized but dropping short parquet span={} path={}",
                    span_blocks,
                    final_path.display()
                );
                fs::remove_file(&final_path).map_err(io_to_parquet_error)?;
            } else {
                enqueue_handoff_task(&self.handoff_tx, final_path, relative_path)?;
            }
        }
        self.pending_start_block = None;
        Ok(())
    }

    fn abort(&mut self) {
        self.active.clear();
        self.delayed.clear();
        self.pending_start_block = None;
        if let Some(file) = self.file.take() {
            drop(file.writer);
            if let Err(err) = fs::remove_file(&file.tmp_path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to remove incomplete parquet {}: {err}", file.tmp_path.display());
                }
            }
        }
    }

    fn write_buffer<F>(&mut self, mode: ArchiveMode, rows: Vec<R>, write_rows: F) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        if rows.is_empty() {
            return Ok(false);
        }
        if let Some(file) = self.file.as_mut() {
            write_rows(&mut file.writer, mode, &rows)?;
        }
        Ok(true)
    }

    fn flush_immediate<F>(&mut self, mode: ArchiveMode, write_rows: F) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let rows = self.active.take_rows();
        self.write_buffer(mode, rows, write_rows)
    }

    fn flush_delayed_buffer<F>(&mut self, mode: ArchiveMode, write_rows: F) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let rows = self.delayed.take_rows();
        self.write_buffer(mode, rows, write_rows)
    }

    fn finalize_active_window<F>(&mut self, mode: ArchiveMode, write_rows: F) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let mut flushed = false;
        if !self.delayed.is_empty() {
            flushed |= self.flush_delayed_buffer(mode, write_rows)?;
        }
        if !self.active.is_empty() {
            let start_block = self.active.start_block.take();
            let end_block = self.active.end_block.take();
            self.delayed.rows = self.active.take_rows();
            self.delayed.start_block = start_block;
            self.delayed.end_block = end_block;
        } else {
            self.active.start_block = None;
            self.active.end_block = None;
        }
        Ok(flushed)
    }

    fn ensure_active_window(&mut self, block: u64) {
        if self.active.end_block.is_none() {
            if let Some(span) = self.stream.row_group_block_limit() {
                let (start, end) = aligned_row_group_bounds(block, span);
                self.active.start_block = Some(start);
                self.active.end_block = Some(end);
            }
        }
    }

    fn maybe_flush_delayed_after_lookahead<F>(
        &mut self,
        mode: ArchiveMode,
        block: u64,
        write_rows: F,
    ) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let Some(lookahead) = self.stream.delayed_flush_lookahead_blocks() else {
            return Ok(false);
        };
        let Some(active_start) = self.active.start_block else {
            return Ok(false);
        };
        if !self.delayed.is_empty() && block.saturating_sub(active_start) + 1 > lookahead {
            return self.flush_delayed_buffer(mode, write_rows);
        }
        Ok(false)
    }

    fn advance_delayed_windows<F>(
        &mut self,
        mode: ArchiveMode,
        block: u64,
        write_rows: F,
    ) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let mut flushed = false;
        self.ensure_active_window(block);
        while self.active.end_block.is_some_and(|end| block > end) {
            let current_end = self.active.end_block.expect("active end checked above");
            flushed |= self.finalize_active_window(mode, write_rows)?;
            if let Some(span) = self.stream.row_group_block_limit() {
                self.active.start_block = Some(current_end + 1);
                self.active.end_block = Some(current_end + span);
            }
        }
        Ok(flushed)
    }

    fn advance_block<F>(&mut self, mode: ArchiveMode, block: u64, write_rows: F) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        let mut flushed = false;
        if self.file.as_ref().is_some_and(|file| {
            self.supports_local_recovery() && file.replay_skip_until_block > 0 && block <= file.replay_skip_until_block
        }) {
            return Ok(false);
        }
        let current_start = self.file.as_ref().map(|current| current.window_start_block);
        let next_start = rotation_bounds_for(self.stream, block).0;
        if self.file.is_none() {
            match self.pending_start_block {
                Some(start) if rotation_bounds_for(self.stream, start).0 != next_start => {
                    self.pending_start_block = Some(block)
                }
                Some(_) => {}
                None => self.pending_start_block = Some(block),
            }
        }
        if current_start.is_some_and(|start| start != next_start) {
            self.close_with_flush(mode, write_rows)?;
            self.pending_start_block = Some(block);
            flushed = true;
        }
        if let Some(file) = self.file.as_mut() {
            file.last_block = block;
            file.name_end_block = block;
        }
        if self.stream.uses_delayed_flush() {
            flushed |= self.advance_delayed_windows(mode, block, write_rows)?;
            flushed |= self.maybe_flush_delayed_after_lookahead(mode, block, write_rows)?;
        } else if self
            .stream
            .row_group_block_limit()
            .is_some_and(|limit| self.active.start_block.is_some_and(|start| block.saturating_sub(start) + 1 >= limit))
        {
            flushed |= self.flush_immediate(mode, write_rows)?;
        }
        Ok(flushed)
    }

    fn append_rows<F>(
        &mut self,
        coin: &str,
        mode: ArchiveMode,
        block: u64,
        rows: Vec<R>,
        write_rows: F,
    ) -> parquet::errors::Result<bool>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
    {
        if rows.is_empty() {
            return Ok(false);
        }
        self.ensure_open(coin, mode, block)?;
        if self.file.as_ref().is_some_and(|file| {
            self.supports_local_recovery() && file.replay_skip_until_block > 0 && block <= file.replay_skip_until_block
        }) {
            return Ok(false);
        }
        let mut flushed = false;
        if let Some(file) = self.file.as_mut() {
            file.last_block = block;
            file.name_end_block = block;
        }
        if self.stream.uses_delayed_flush() {
            self.ensure_active_window(block);
            flushed |= self.maybe_flush_delayed_after_lookahead(mode, block, write_rows)?;
        } else if self.active.start_block.is_none() {
            self.active.start_block = Some(block);
        }
        self.active.rows.extend(rows);
        if self.stream.uses_delayed_flush() {
            if self.active.end_block == Some(block) {
                flushed |= self.finalize_active_window(mode, write_rows)?;
            }
        } else if self
            .stream
            .row_group_block_limit()
            .is_some_and(|limit| self.active.start_block.is_some_and(|start| block.saturating_sub(start) + 1 >= limit))
        {
            flushed |= self.flush_immediate(mode, write_rows)?;
        }
        Ok(flushed)
    }

    fn should_merge_tail(&self) -> bool {
        let Some(span) = self.stream.row_group_block_limit() else {
            return false;
        };
        let Some(file) = self.file.as_ref() else {
            return false;
        };
        let Some(active_start) = self.active.start_block else {
            return false;
        };
        let observed_span = file.last_block.saturating_sub(active_start) + 1;
        observed_span < span
    }

    fn close_with_flush<F>(&mut self, mode: ArchiveMode, write_rows: F) -> parquet::errors::Result<()>
    where
        F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
        R: HasBlockNumber,
    {
        if current_archive_align_output_to_1000_boundary() {
            if let Some(file) = self.file.as_mut() {
                match aligned_archive_output_end(file.last_block) {
                    Some(aligned_end) if aligned_end >= file.name_start_block => {
                        self.active.trim_to_block(aligned_end);
                        self.delayed.trim_to_block(aligned_end);
                        file.name_end_block = aligned_end;
                    }
                    _ => {
                        self.active.clear();
                        self.delayed.clear();
                        self.pending_start_block = None;
                        if let Some(file) = self.file.take() {
                            drop(file.writer);
                            if let Err(err) = fs::remove_file(&file.tmp_path) {
                                if err.kind() != std::io::ErrorKind::NotFound {
                                    warn!("Failed to remove incomplete parquet {}: {err}", file.tmp_path.display());
                                }
                            }
                        }
                        return Ok(());
                    }
                }
            }
        }
        if self.stream.uses_delayed_flush() {
            if !self.delayed.is_empty() {
                if !self.active.is_empty() && self.should_merge_tail() {
                    let mut rows = self.delayed.take_rows();
                    rows.extend(self.active.take_rows());
                    self.write_buffer(mode, rows, write_rows)?;
                } else {
                    self.flush_delayed_buffer(mode, write_rows)?;
                    self.flush_immediate(mode, write_rows)?;
                }
            } else {
                self.flush_immediate(mode, write_rows)?;
            }
            self.active.start_block = None;
            self.active.end_block = None;
            self.delayed.start_block = None;
            self.delayed.end_block = None;
        } else {
            self.flush_immediate(mode, write_rows)?;
        }
        self.close()
    }
}

struct StatusParquetWriter {
    coin: String,
    mode: ArchiveMode,
    schema: std::sync::Arc<Type>,
    props: std::sync::Arc<WriterProperties>,
    handoff_tx: Sender<HandoffMessage>,
    file: Option<ParquetFile>,
    pending_start_block: Option<u64>,
    active_start_block: Option<u64>,
    active_end_block: Option<u64>,
    active: StatusBlockBatch,
}

impl StatusParquetWriter {
    fn new(
        coin: String,
        mode: ArchiveMode,
        schema: std::sync::Arc<Type>,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
    ) -> Self {
        Self {
            coin,
            mode,
            schema,
            props,
            handoff_tx,
            file: None,
            pending_start_block: None,
            active_start_block: None,
            active_end_block: None,
            active: StatusBlockBatch::new(mode),
        }
    }

    fn ensure_open(&mut self, coin: &str, block: u64) -> parquet::errors::Result<()> {
        if self.file.is_none() {
            let (window_start, end) = rotation_bounds_for(StreamKind::Status, block);
            let name_start = self.pending_start_block.unwrap_or(block);
            self.open_file(coin, name_start, block, window_start, end)?;
        }
        Ok(())
    }

    fn open_file(
        &mut self,
        coin: &str,
        name_start: u64,
        actual_start: u64,
        window_start: u64,
        end: u64,
    ) -> parquet::errors::Result<()> {
        let base_dir = current_archive_base_dir();
        let relative_dir = archive_relative_dir(StreamKind::Status, self.mode, coin);
        let filename_prefix = format!("{}_{}", coin, StreamKind::Status.name());
        let filename_suffix = ".parquet".to_string();
        fs::create_dir_all(&base_dir).map_err(io_to_parquet_error)?;
        let local_final_path = base_dir.join(format!("{filename_prefix}_{name_start}_{end}{filename_suffix}"));
        let tmp_path = local_final_path.with_file_name(format!(
            "{}{}",
            local_final_path.file_name().and_then(|name| name.to_str()).unwrap_or("archive.parquet"),
            TEMP_FILE_SUFFIX
        ));
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(io_to_parquet_error)?;
        }
        let file = fs::File::create(&tmp_path).map_err(io_to_parquet_error)?;
        let writer = SerializedFileWriter::new(file, self.schema.clone(), self.props.clone())?;
        self.file = Some(ParquetFile {
            writer,
            window_start_block: window_start,
            window_end_block: end,
            replay_skip_until_block: 0,
            name_start_block: name_start,
            name_end_block: actual_start,
            last_block: actual_start,
            tmp_path,
            local_final_path,
            relative_dir,
            final_filename_prefix: filename_prefix,
            final_filename_suffix: filename_suffix,
        });
        self.pending_start_block = None;
        Ok(())
    }

    fn flush_active(&mut self) -> parquet::errors::Result<()> {
        if self.active.is_empty() {
            self.active_start_block = None;
            self.active_end_block = None;
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.active, StatusBlockBatch::new(self.mode));
        let file = self.file.as_mut().ok_or_else(|| io_to_parquet_error(io_other("status writer missing file")))?;
        match batch {
            StatusBlockBatch::Lite(columns) => write_status_lite_columns(&mut file.writer, &self.coin, columns)?,
            StatusBlockBatch::Full(columns) => write_status_full_columns(&mut file.writer, &self.coin, columns)?,
        }
        trim_allocator();
        self.active_start_block = None;
        self.active_end_block = None;
        Ok(())
    }

    fn advance_block(&mut self, block: u64) -> parquet::errors::Result<()> {
        let current_start = self.file.as_ref().map(|current| current.window_start_block);
        let next_start = rotation_bounds_for(StreamKind::Status, block).0;
        if self.file.is_none() {
            match self.pending_start_block {
                Some(start) if rotation_bounds_for(StreamKind::Status, start).0 != next_start => {
                    self.pending_start_block = Some(block)
                }
                Some(_) => {}
                None => self.pending_start_block = Some(block),
            }
        }
        if current_start.is_some_and(|start| start != next_start) {
            self.close_with_flush()?;
            self.pending_start_block = Some(block);
        }
        if let Some(file) = self.file.as_mut() {
            file.last_block = block;
            file.name_end_block = block;
        }
        if self.active_end_block.is_none() {
            let (start, end) = aligned_row_group_bounds(block, status_row_group_blocks_for_coin(&self.coin));
            self.active_start_block = Some(start);
            self.active_end_block = Some(end);
        }
        while self.active_end_block.is_some_and(|end| block > end) {
            self.flush_active()?;
            let (start, end) = aligned_row_group_bounds(block, status_row_group_blocks_for_coin(&self.coin));
            self.active_start_block = Some(start);
            self.active_end_block = Some(end);
        }
        Ok(())
    }

    fn append_block(&mut self, coin: &str, block: u64, rows: StatusBlockBatch) -> parquet::errors::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.ensure_open(coin, block)?;
        if let Some(file) = self.file.as_mut() {
            file.last_block = block;
            file.name_end_block = block;
        }
        if self.active_end_block.is_none() {
            let (start, end) = aligned_row_group_bounds(block, status_row_group_blocks_for_coin(&self.coin));
            self.active_start_block = Some(start);
            self.active_end_block = Some(end);
        }
        match (&mut self.active, rows) {
            (StatusBlockBatch::Lite(active), StatusBlockBatch::Lite(batch)) => active.append(batch),
            (StatusBlockBatch::Full(active), StatusBlockBatch::Full(batch)) => active.append(batch),
            _ => {
                return Err(io_to_parquet_error(io_other(
                    "status block batch mode mismatch while appending status archive rows",
                )));
            }
        }
        Ok(())
    }

    fn close_with_flush(&mut self) -> parquet::errors::Result<()> {
        if current_archive_align_output_to_1000_boundary() {
            if let Some(file) = self.file.as_mut() {
                match aligned_archive_output_end(file.last_block) {
                    Some(aligned_end) if aligned_end >= file.name_start_block => {
                        self.active.trim_to_block(aligned_end);
                        file.name_end_block = aligned_end;
                    }
                    _ => {
                        self.active = StatusBlockBatch::new(self.mode);
                        self.active_start_block = None;
                        self.active_end_block = None;
                        self.pending_start_block = None;
                        if let Some(file) = self.file.take() {
                            drop(file.writer);
                            if let Err(err) = fs::remove_file(&file.tmp_path) {
                                if err.kind() != std::io::ErrorKind::NotFound {
                                    warn!("Failed to remove incomplete parquet {}: {err}", file.tmp_path.display());
                                }
                            }
                        }
                        return Ok(());
                    }
                }
            }
        }
        self.flush_active()?;
        if let Some(file) = self.file.take() {
            let tmp_path = file.tmp_path.clone();
            let final_path = file.local_final_path.clone();
            let relative_path = file.relative_dir.join(format!(
                "{}_{}_{}{}",
                file.final_filename_prefix, file.name_start_block, file.name_end_block, file.final_filename_suffix
            ));
            file.writer.close()?;
            fs::rename(&tmp_path, &final_path).map_err(io_to_parquet_error)?;
            let span_blocks = file.name_end_block.saturating_sub(file.name_start_block) + 1;
            if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
                info!(
                    "Archive finalized but dropping short parquet span={} path={}",
                    span_blocks,
                    final_path.display()
                );
                fs::remove_file(&final_path).map_err(io_to_parquet_error)?;
            } else {
                enqueue_handoff_task(&self.handoff_tx, final_path, relative_path)?;
            }
        }
        self.pending_start_block = None;
        Ok(())
    }

    fn abort(&mut self) {
        self.active = StatusBlockBatch::new(self.mode);
        self.active_start_block = None;
        self.active_end_block = None;
        self.pending_start_block = None;
        if let Some(file) = self.file.take() {
            drop(file.writer);
            if let Err(err) = fs::remove_file(&file.tmp_path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to remove incomplete parquet {}: {err}", file.tmp_path.display());
                }
            }
        }
    }
}

enum StatusWorkerMessage {
    Block { height: u64, rows: StatusBlockBatch },
    Close,
    Abort,
}

enum DiffWorkerMessage {
    Block { height: u64, rows_by_coin: HashMap<String, Vec<DiffOut>> },
    Close,
    Abort,
}

struct DiffWorkerHandle {
    tx: SyncSender<DiffWorkerMessage>,
    error_rx: Receiver<String>,
    handle: Option<thread::JoinHandle<()>>,
}

impl DiffWorkerHandle {
    fn new(
        mode: ArchiveMode,
        symbols: Vec<String>,
        schema: std::sync::Arc<Type>,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
    ) -> Self {
        let (tx, rx) = sync_channel(DIFF_WORKER_QUEUE_BLOCKS);
        let (error_tx, error_rx) = channel();
        let handle = thread::spawn(move || {
            let mut writers: HashMap<String, ParquetStreamWriter<DiffOut>> = symbols
                .iter()
                .map(|coin| {
                    (
                        coin.clone(),
                        ParquetStreamWriter::new(StreamKind::Diff, schema.clone(), props.clone(), handoff_tx.clone()),
                    )
                })
                .collect();
            while let Ok(message) = rx.recv() {
                let should_break = matches!(message, DiffWorkerMessage::Close | DiffWorkerMessage::Abort);
                let result = (|| -> parquet::errors::Result<()> {
                    match message {
                        DiffWorkerMessage::Block { height, mut rows_by_coin } => {
                            for (idx, coin) in symbols.iter().enumerate() {
                                let Some(writer) = writers.get_mut(coin) else {
                                    continue;
                                };
                                let rows = rows_by_coin.remove(coin).unwrap_or_default();
                                let flushed = writer.advance_block(mode, height, write_diff_rows).and_then(
                                    |advance_flushed| {
                                        writer
                                            .append_rows(coin, mode, height, rows, write_diff_rows)
                                            .map(|append_flushed| advance_flushed || append_flushed)
                                    },
                                )?;
                                if flushed && idx + 1 < symbols.len() {
                                    thread::sleep(DIFF_STAGGER_DELAY);
                                }
                            }
                            Ok(())
                        }
                        DiffWorkerMessage::Close => {
                            for writer in writers.values_mut() {
                                writer.close_with_flush(mode, write_diff_rows)?;
                            }
                            Ok(())
                        }
                        DiffWorkerMessage::Abort => {
                            for writer in writers.values_mut() {
                                writer.abort();
                            }
                            Ok(())
                        }
                    }
                })();
                if let Err(err) = result {
                    for writer in writers.values_mut() {
                        writer.abort();
                    }
                    let _unused = error_tx.send(err.to_string());
                    break;
                }
                if should_break {
                    break;
                }
            }
        });
        Self { tx, error_rx, handle: Some(handle) }
    }

    fn check_error(&self) -> parquet::errors::Result<()> {
        match self.error_rx.try_recv() {
            Ok(err) => Err(io_to_parquet_error(io_other(format!("diff worker failed: {err}")))),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => Ok(()),
        }
    }

    fn send_block(&self, height: u64, rows_by_coin: HashMap<String, Vec<DiffOut>>) -> parquet::errors::Result<()> {
        self.check_error()?;
        self.tx
            .send(DiffWorkerMessage::Block { height, rows_by_coin })
            .map_err(|err| io_to_parquet_error(io_other(format!("diff worker send failed: {err}"))))?;
        self.check_error()
    }

    fn close(&mut self) -> parquet::errors::Result<()> {
        self.check_error()?;
        self.tx
            .send(DiffWorkerMessage::Close)
            .map_err(|err| io_to_parquet_error(io_other(format!("diff worker close send failed: {err}"))))?;
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|err| io_to_parquet_error(io_other(format!("diff worker join failed: {err:?}"))))?;
        }
        self.check_error()
    }

    fn abort(&mut self) {
        let _unused = self.tx.send(DiffWorkerMessage::Abort);
        if let Some(handle) = self.handle.take() {
            let _unused = handle.join();
        }
    }
}

struct StatusWorkerHandle {
    coin: String,
    tx: SyncSender<StatusWorkerMessage>,
    ack_rx: Receiver<u64>,
    error_rx: Receiver<String>,
    handle: Option<thread::JoinHandle<()>>,
}

impl StatusWorkerHandle {
    fn new(
        coin: String,
        mode: ArchiveMode,
        schema: std::sync::Arc<Type>,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
    ) -> Self {
        let (tx, rx) = sync_channel(STATUS_WORKER_QUEUE_BLOCKS);
        let (ack_tx, ack_rx) = channel();
        let (error_tx, error_rx) = channel();
        let worker_coin = coin.clone();
        let handle = thread::spawn(move || {
            let mut writer = StatusParquetWriter::new(worker_coin.clone(), mode, schema, props, handoff_tx);
            while let Ok(message) = rx.recv() {
                let result = match message {
                    StatusWorkerMessage::Block { height, rows } => writer.advance_block(height).and_then(|_| {
                        if rows.is_empty() {
                            ack_tx.send(height).map_err(|err| {
                                io_to_parquet_error(io_other(format!(
                                    "status worker ack send failed for {} at {}: {err}",
                                    worker_coin, height
                                )))
                            })
                        } else {
                            writer.append_block(&worker_coin, height, rows).and_then(|_| {
                                ack_tx.send(height).map_err(|err| {
                                    io_to_parquet_error(io_other(format!(
                                        "status worker ack send failed for {} at {}: {err}",
                                        worker_coin, height
                                    )))
                                })
                            })
                        }
                    }),
                    StatusWorkerMessage::Close => {
                        let result = writer.close_with_flush();
                        if result.is_err() {
                            writer.abort();
                        }
                        if let Err(err) = result {
                            let _unused = error_tx.send(err.to_string());
                        }
                        break;
                    }
                    StatusWorkerMessage::Abort => {
                        writer.abort();
                        break;
                    }
                };
                if let Err(err) = result {
                    writer.abort();
                    let _unused = error_tx.send(err.to_string());
                    break;
                }
            }
        });
        Self { coin, tx, ack_rx, error_rx, handle: Some(handle) }
    }

    fn check_error(&self) -> parquet::errors::Result<()> {
        match self.error_rx.try_recv() {
            Ok(err) => Err(io_to_parquet_error(io_other(format!("status worker failed for {}: {err}", self.coin)))),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => Ok(()),
        }
    }

    fn send_block(&self, height: u64, rows: StatusBlockBatch) -> parquet::errors::Result<()> {
        self.check_error()?;
        self.tx.send(StatusWorkerMessage::Block { height, rows }).map_err(|err| {
            io_to_parquet_error(io_other(format!("status worker send failed for {}: {err}", self.coin)))
        })?;
        self.check_error()
    }

    fn drain_acked_heights(&self) -> parquet::errors::Result<Vec<u64>> {
        self.check_error()?;
        let mut heights = Vec::new();
        loop {
            match self.ack_rx.try_recv() {
                Ok(height) => heights.push(height),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        self.check_error()?;
        Ok(heights)
    }

    fn close(mut self) -> parquet::errors::Result<()> {
        self.check_error()?;
        self.tx.send(StatusWorkerMessage::Close).map_err(|err| {
            io_to_parquet_error(io_other(format!("status worker close send failed for {}: {err}", self.coin)))
        })?;
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|err| {
                io_to_parquet_error(io_other(format!("status worker join failed for {}: {err:?}", self.coin)))
            })?;
        }
        self.check_error()
    }

    fn abort(mut self) {
        let _unused = self.tx.send(StatusWorkerMessage::Abort);
        if let Some(handle) = self.handle.take() {
            let _unused = handle.join();
        }
    }
}

struct CoinWriters {
    status: StatusWorkerHandle,
    fill: ParquetStreamWriter<FillOut>,
}

struct ArchiveWriters {
    blocks: ParquetStreamWriter<BlockIndexOut>,
    diff: DiffWorkerHandle,
    coins: HashMap<String, CoinWriters>,
    symbols: Vec<String>,
    mode: ArchiveMode,
    pending_status_acks: HashMap<u64, usize>,
    next_uncommitted_height: Option<u64>,
    last_archived_height: Option<u64>,
}

impl ArchiveWriters {
    fn new(mode: ArchiveMode, symbols: Vec<String>, handoff_tx: Sender<HandoffMessage>) -> Self {
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
        let diff = DiffWorkerHandle::new(mode, symbols.clone(), diff_schema.clone(), props.clone(), handoff_tx.clone());
        for coin in &symbols {
            coins.insert(
                coin.clone(),
                CoinWriters {
                    status: StatusWorkerHandle::new(
                        coin.clone(),
                        mode,
                        status_schema.clone(),
                        props.clone(),
                        handoff_tx.clone(),
                    ),
                    fill: ParquetStreamWriter::new(
                        StreamKind::Fill,
                        fill_schema.clone(),
                        props.clone(),
                        handoff_tx.clone(),
                    ),
                },
            );
        }

        Self {
            blocks: ParquetStreamWriter::new(StreamKind::Blocks, blocks_schema, props.clone(), handoff_tx),
            diff,
            coins,
            symbols,
            mode,
            pending_status_acks: HashMap::new(),
            next_uncommitted_height: None,
            last_archived_height: None,
        }
    }

    fn coin_mut(&mut self, coin: &str) -> Option<&mut CoinWriters> {
        self.coins.get_mut(coin)
    }

    fn has_coin(&self, coin: &str) -> bool {
        self.coins.contains_key(coin)
    }

    fn last_archived_height(&self) -> Option<u64> {
        self.last_archived_height
    }

    fn register_status_block(&mut self, height: u64) {
        if self.symbols.is_empty() {
            self.last_archived_height = Some(height);
            return;
        }
        self.pending_status_acks.insert(height, self.symbols.len());
        if self.next_uncommitted_height.is_none() {
            self.next_uncommitted_height = Some(height);
        }
    }

    fn advance_last_archived_height(&mut self) {
        let Some(mut next) = self.next_uncommitted_height else {
            return;
        };
        while self.pending_status_acks.get(&next).is_some_and(|remaining| *remaining == 0) {
            self.pending_status_acks.remove(&next);
            self.last_archived_height = Some(next);
            next = next.saturating_add(1);
        }
        self.next_uncommitted_height = if self.pending_status_acks.is_empty() { None } else { Some(next) };
    }

    fn drain_status_progress(&mut self) -> parquet::errors::Result<()> {
        for writers in self.coins.values() {
            for height in writers.status.drain_acked_heights()? {
                let Some(remaining) = self.pending_status_acks.get_mut(&height) else {
                    warn!("Dropping unexpected status worker ack at height {}", height);
                    continue;
                };
                if *remaining == 0 {
                    warn!("Dropping duplicate status worker ack at height {}", height);
                    continue;
                }
                *remaining -= 1;
            }
        }
        self.advance_last_archived_height();
        Ok(())
    }

    fn close_all(&mut self) -> parquet::errors::Result<()> {
        self.blocks.close_with_flush(self.mode, write_block_rows)?;
        self.diff.close()?;
        for (_, writers) in std::mem::take(&mut self.coins) {
            let mut writers = writers;
            writers.status.close()?;
            writers.fill.close_with_flush(self.mode, write_fill_rows)?;
        }
        Ok(())
    }

    fn abort_all(&mut self) {
        self.blocks.abort();
        self.diff.abort();
        for (_, writers) in std::mem::take(&mut self.coins) {
            let mut writers = writers;
            writers.status.abort();
            writers.fill.abort();
        }
    }
}

fn rotation_bounds_for(stream: StreamKind, block: u64) -> (u64, u64) {
    let rotation = stream.rotation_block_limit();
    let start = ((block.saturating_sub(1)) / rotation) * rotation + 1;
    let end = start + rotation - 1;
    (start, end)
}

fn aligned_row_group_bounds(block: u64, span: u64) -> (u64, u64) {
    let end = ((block.saturating_sub(1)) / span + 1) * span;
    let start = end.saturating_sub(span) + 1;
    (start, end)
}

fn checkpoint_start(block: u64) -> u64 {
    ((block.saturating_sub(1)) / CHECKPOINT_BLOCKS) * CHECKPOINT_BLOCKS + 1
}

fn is_checkpoint_start(block: u64) -> bool {
    checkpoint_start(block) == block
}

fn io_to_parquet_error<E>(err: E) -> parquet::errors::ParquetError
where
    E: std::error::Error + Send + Sync + 'static,
{
    parquet::errors::ParquetError::External(Box::new(err))
}

fn io_other<M: Into<String>>(msg: M) -> std::io::Error {
    std::io::Error::other(msg.into())
}

#[derive(Debug)]
struct LocalRecoveryFile<R> {
    path: PathBuf,
    name_start_block: u64,
    rows: Vec<R>,
    last_block: u64,
}

fn parse_local_archive_filename_bounds(path: &Path, filename_prefix: &str) -> Option<(u64, u64)> {
    let stem = path.file_stem()?.to_str()?;
    let prefix = format!("{filename_prefix}_");
    let remainder = stem.strip_prefix(&prefix)?;
    let (start, end) = remainder.rsplit_once('_')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

fn find_local_recovery_path(
    base_dir: &Path,
    filename_prefix: &str,
    window_end_block: u64,
) -> parquet::errors::Result<Option<(PathBuf, u64)>> {
    let Ok(entries) = fs::read_dir(base_dir) else {
        return Ok(None);
    };
    for entry in entries {
        let entry = entry.map_err(io_to_parquet_error)?;
        let path = entry.path();
        if !entry.file_type().map_err(io_to_parquet_error)?.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        let Some((name_start_block, name_end_block)) = parse_local_archive_filename_bounds(&path, filename_prefix)
        else {
            continue;
        };
        if name_end_block == window_end_block {
            return Ok(Some((path, name_start_block)));
        }
    }
    Ok(None)
}

fn rewrite_rows_into_writer<R, F>(
    writer: &mut SerializedFileWriter<std::fs::File>,
    mode: ArchiveMode,
    stream: StreamKind,
    rows: Vec<R>,
    write_rows: F,
) -> parquet::errors::Result<()>
where
    R: HasBlockNumber,
    F: Copy + Fn(&mut SerializedFileWriter<std::fs::File>, ArchiveMode, &[R]) -> parquet::errors::Result<()>,
{
    if rows.is_empty() {
        return Ok(());
    }
    let Some(span) = stream.row_group_block_limit() else {
        return write_rows(writer, mode, &rows);
    };
    let mut batch = Vec::new();
    let mut current_end = None;
    for row in rows {
        let block = row.block_number();
        let (_, next_end) = aligned_row_group_bounds(block, span);
        if current_end.is_some_and(|end| block > end) && !batch.is_empty() {
            write_rows(writer, mode, &batch)?;
            batch.clear();
        }
        current_end = Some(next_end);
        batch.push(row);
    }
    if !batch.is_empty() {
        write_rows(writer, mode, &batch)?;
    }
    Ok(())
}

fn stage_local_file_for_handoff(path: &Path) -> parquet::errors::Result<PathBuf> {
    let staged = path.with_file_name(format!(
        "{}.handoff",
        path.file_name().and_then(|name| name.to_str()).unwrap_or("archive.parquet")
    ));
    if staged.exists() {
        fs::remove_file(&staged).map_err(io_to_parquet_error)?;
    }
    fs::rename(path, &staged).map_err(io_to_parquet_error)?;
    Ok(staged)
}

fn read_local_blocks_rows(path: &Path) -> parquet::errors::Result<Vec<BlockIndexOut>> {
    let reader = SerializedFileReader::try_from(path).map_err(io_to_parquet_error)?;
    let iter = reader.get_row_iter(None).map_err(io_to_parquet_error)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row.map_err(io_to_parquet_error)?;
        rows.push(BlockIndexOut {
            block_number: row.get_long(0).map_err(io_to_parquet_error)? as u64,
            block_time: row.get_string(1).map_err(io_to_parquet_error)?.clone(),
            order_batch_ok: row.get_bool(2).map_err(io_to_parquet_error)?,
            diff_batch_ok: row.get_bool(3).map_err(io_to_parquet_error)?,
            fill_batch_ok: row.get_bool(4).map_err(io_to_parquet_error)?,
            order_n: row.get_int(5).map_err(io_to_parquet_error)?,
            diff_n: row.get_int(6).map_err(io_to_parquet_error)?,
            fill_n: row.get_int(7).map_err(io_to_parquet_error)?,
            btc_status_n: row.get_int(8).map_err(io_to_parquet_error)?,
            btc_diff_n: row.get_int(9).map_err(io_to_parquet_error)?,
            btc_fill_n: row.get_int(10).map_err(io_to_parquet_error)?,
            eth_status_n: row.get_int(11).map_err(io_to_parquet_error)?,
            eth_diff_n: row.get_int(12).map_err(io_to_parquet_error)?,
            eth_fill_n: row.get_int(13).map_err(io_to_parquet_error)?,
            archive_mode: row.get_string(14).map_err(io_to_parquet_error)?.clone(),
            tracked_symbols: row.get_string(15).map_err(io_to_parquet_error)?.clone(),
        });
    }
    Ok(rows)
}

fn read_local_fill_rows(path: &Path, mode: ArchiveMode) -> parquet::errors::Result<Vec<FillOut>> {
    let reader = SerializedFileReader::try_from(path).map_err(io_to_parquet_error)?;
    let iter = reader.get_row_iter(None).map_err(io_to_parquet_error)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row.map_err(io_to_parquet_error)?;
        let mut out = FillOut {
            block_number: row.get_long(0).map_err(io_to_parquet_error)? as u64,
            block_time: row.get_string(1).map_err(io_to_parquet_error)?.clone(),
            coin: row.get_string(2).map_err(io_to_parquet_error)?.clone(),
            side: row.get_string(3).map_err(io_to_parquet_error)?.clone(),
            px: decimal_to_i64(row.get_decimal(4).map_err(io_to_parquet_error)?)?,
            sz: decimal_to_i64(row.get_decimal(5).map_err(io_to_parquet_error)?)?,
            crossed: row.get_bool(6).map_err(io_to_parquet_error)?,
            address: String::new(),
            closed_pnl: 0,
            fee: 0,
            hash: String::new(),
            oid: 0,
            tid: 0,
            time: 0,
            start_position: 0,
            dir: String::new(),
            fee_token: String::new(),
            twap_id: 0,
            raw_event: String::new(),
        };
        match mode {
            ArchiveMode::Lite => {
                out.oid = row.get_long(7).map_err(io_to_parquet_error)? as u64;
                out.time = row.get_long(8).map_err(io_to_parquet_error)?;
            }
            ArchiveMode::Full => {
                out.address = row.get_string(7).map_err(io_to_parquet_error)?.clone();
                out.closed_pnl = decimal_to_i64(row.get_decimal(8).map_err(io_to_parquet_error)?)?;
                out.fee = decimal_to_i64(row.get_decimal(9).map_err(io_to_parquet_error)?)?;
                out.hash = row.get_string(10).map_err(io_to_parquet_error)?.clone();
                out.oid = row.get_long(11).map_err(io_to_parquet_error)? as u64;
                out.tid = row.get_long(12).map_err(io_to_parquet_error)? as u64;
                out.time = row.get_long(13).map_err(io_to_parquet_error)?;
                out.start_position = decimal_to_i64(row.get_decimal(14).map_err(io_to_parquet_error)?)?;
                out.dir = row.get_string(15).map_err(io_to_parquet_error)?.clone();
                out.fee_token = row.get_string(16).map_err(io_to_parquet_error)?.clone();
                out.twap_id = row.get_long(17).map_err(io_to_parquet_error)?;
                out.raw_event = row.get_string(18).map_err(io_to_parquet_error)?.clone();
            }
        }
        rows.push(out);
    }
    Ok(rows)
}

fn decimal_to_i64(value: &parquet::data_type::Decimal) -> parquet::errors::Result<i64> {
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
        len => Err(io_to_parquet_error(io_other(format!(
            "unsupported decimal width {len} while decoding recovered parquet rows"
        )))),
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(unsafe_code)]
fn trim_allocator() {
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_allocator() {}

#[allow(unsafe_code)]
fn archive_disk_status(path: &Path) -> std::io::Result<ArchiveDiskStatus> {
    let path_bytes = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| io_other(format!("path contains interior nul byte: {}", path.display())))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(path_bytes.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    let block_size = u128::from(stat.f_frsize.max(stat.f_bsize));
    let total_blocks = u128::from(stat.f_blocks);
    let available_blocks = u128::from(stat.f_bavail);
    let total_bytes = block_size.saturating_mul(total_blocks);
    let available_bytes = block_size.saturating_mul(available_blocks);
    let used_basis_points = if total_bytes == 0 {
        0
    } else {
        let used_bytes = total_bytes.saturating_sub(available_bytes);
        ((used_bytes.saturating_mul(10_000)) / total_bytes) as u64
    };
    Ok(ArchiveDiskStatus { available_bytes: available_bytes as u64, used_basis_points })
}

fn ensure_archive_disk_headroom(base_dir: &Path) -> std::io::Result<()> {
    let status = archive_disk_status(base_dir)?;
    if status.available_bytes < MIN_ARCHIVE_FREE_BYTES || status.used_basis_points >= MAX_ARCHIVE_DISK_USED_BPS {
        return Err(io_other(format!(
            "archive disk headroom low at {}: available_bytes={} used_pct={:.2}",
            base_dir.display(),
            status.available_bytes,
            status.used_basis_points as f64 / 100.0
        )));
    }
    Ok(())
}

fn cleanup_stale_temp_files(base_dir: &Path) {
    let mut stack = vec![base_dir.to_path_buf()];
    let mut removed = 0usize;
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.ends_with(TEMP_FILE_SUFFIX)) {
                match fs::remove_file(&path) {
                    Ok(()) => removed += 1,
                    Err(err) => warn!("Failed to remove stale in-progress parquet {}: {err}", path.display()),
                }
            }
        }
    }
    if removed > 0 {
        warn!("Removed {} stale in-progress parquet files under {}", removed, base_dir.display());
    }
}

fn start_archive_handoff_worker(stop: Arc<AtomicBool>) -> ArchiveHandoffWorker {
    let (tx, rx) = channel::<HandoffMessage>();
    let handle = thread::spawn(move || {
        while let Ok(message) = rx.recv() {
            match message {
                HandoffMessage::File(task) => {
                    if let Err(err) = handoff_finalized_parquet(&task) {
                        warn!(
                            "Archive handoff failed for {} (stop={}): {err}",
                            task.path.display(),
                            stop.load(Ordering::SeqCst)
                        );
                    }
                }
                HandoffMessage::Barrier(done_tx) => {
                    let _unused = done_tx.send(());
                }
            }
        }
    });
    ArchiveHandoffWorker { tx, handle }
}

fn enqueue_handoff_task(
    handoff_tx: &Sender<HandoffMessage>,
    path: PathBuf,
    relative_path: PathBuf,
) -> parquet::errors::Result<()> {
    let config = current_archive_handoff_config();
    let task = HandoffTask { path, relative_path, config };
    let task_path = task.path.display().to_string();
    handoff_tx.send(HandoffMessage::File(task)).map_err(|err| {
        io_to_parquet_error(io_other(format!("failed to enqueue archive handoff for {task_path}: {err}")))
    })?;
    Ok(())
}

fn drain_handoff_tasks(handoff_tx: &Sender<HandoffMessage>) -> parquet::errors::Result<()> {
    let (done_tx, done_rx) = channel::<()>();
    handoff_tx
        .send(HandoffMessage::Barrier(done_tx))
        .map_err(|err| io_to_parquet_error(io_other(format!("failed to enqueue archive handoff barrier: {err}"))))?;
    done_rx
        .recv()
        .map_err(|err| io_to_parquet_error(io_other(format!("failed to wait for archive handoff barrier: {err}"))))?;
    Ok(())
}

fn snapshot_bootstrap_path(base_dir: &Path, height: u64) -> PathBuf {
    base_dir.join(format!(".archive_bootstrap_snapshot_{height}.json"))
}

fn fetch_snapshot_to_path(output_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });
    Client::new()
        .post(INFO_SNAPSHOT_URL)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()?
        .error_for_status()?;
    Ok(())
}

fn write_mid_window_checkpoint(
    output_dir: PathBuf,
    trigger_height: u64,
    block_time: String,
    symbols: Vec<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let snapshot_dir = current_archive_base_dir();
    let snapshot_path = snapshot_bootstrap_path(&snapshot_dir, trigger_height);
    fetch_snapshot_to_path(&snapshot_path)?;
    let options = ComputeOptions { include_users: true, include_trigger_orders: false, assets: Some(symbols.clone()) };
    let result =
        append_l4_checkpoint_from_snapshot_json(&snapshot_path, Some(output_dir.clone()), &options, block_time)?;
    info!(
        "Archive bootstrap checkpoint written from snapshot json trigger_height={} checkpoint_height={} segment={}",
        trigger_height,
        result.block_height,
        result.segment_path.display()
    );
    if let Err(err) = fs::remove_file(&snapshot_path) {
        warn!("Failed to remove bootstrap snapshot {}: {err}", snapshot_path.display());
    }
    Ok(())
}

fn spawn_mid_window_checkpoint_fetch(trigger_height: u64, block_time: String, symbols: Vec<String>) {
    let output_dir = current_archive_handoff_config().nas_output_dir;
    thread::spawn(move || {
        if let Err(err) = write_mid_window_checkpoint(output_dir.clone(), trigger_height, block_time, symbols.clone()) {
            warn!(
                "Archive bootstrap checkpoint fetch failed trigger_height={} output_dir={} symbols={}: {err}",
                trigger_height,
                output_dir.display(),
                symbols.join(",")
            );
        }
    });
}

fn copy_to_nas(path: &PathBuf, relative_path: &PathBuf, dest_root: &Path) -> parquet::errors::Result<PathBuf> {
    let dest_path = dest_root.join(relative_path);
    if *path == dest_path {
        return Ok(dest_path);
    }
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent).map_err(io_to_parquet_error)?;
    }
    let tmp_name = format!("{}.tmp", dest_path.file_name().and_then(|name| name.to_str()).unwrap_or("archive.parquet"));
    let tmp_path = dest_path.with_file_name(tmp_name);
    if tmp_path.exists() {
        fs::remove_file(&tmp_path).map_err(io_to_parquet_error)?;
    }
    fs::copy(path, &tmp_path).map_err(io_to_parquet_error)?;
    fs::rename(&tmp_path, &dest_path).map_err(io_to_parquet_error)?;
    info!("Archive finalized and copied to NAS: {} -> {}", path.display(), dest_path.display());
    Ok(dest_path)
}

fn upload_finalized_parquet_to_oss(
    source_path: &PathBuf,
    relative_path: &PathBuf,
    oss_config: &ArchiveOssConfig,
) -> parquet::errors::Result<()> {
    let oss = OSS::new(
        oss_config.access_key_id.clone(),
        oss_config.access_key_secret.clone(),
        oss_config.endpoint.clone(),
        oss_config.bucket.clone(),
    );
    let object_key = if oss_config.prefix.is_empty() {
        relative_path.to_string_lossy().replace('\\', "/")
    } else {
        format!("{}/{}", oss_config.prefix, relative_path.to_string_lossy().replace('\\', "/"))
    };
    let file_path = source_path.to_string_lossy().into_owned();
    oss.put_object_from_file(object_key.as_str(), file_path.as_str(), RequestBuilder::new())
        .map_err(|err| io_to_parquet_error(io_other(format!("OSS upload failed for {}: {err}", object_key))))?;
    info!(
        "Archive finalized and uploaded to OSS bucket={} object_key={} source={}",
        oss_config.bucket,
        object_key,
        source_path.display()
    );
    Ok(())
}

fn handoff_finalized_parquet(task: &HandoffTask) -> parquet::errors::Result<()> {
    let path = &task.path;
    let relative_path = &task.relative_path;
    let config = &task.config;
    let nas_dest =
        if config.move_to_nas { Some(copy_to_nas(path, relative_path, &config.nas_output_dir)?) } else { None };

    if config.upload_to_oss {
        let Some(oss_config) = config.oss.as_ref() else {
            return Err(io_to_parquet_error(io_other(
                "upload_to_oss enabled but OSS credentials/config were not provided",
            )));
        };
        upload_finalized_parquet_to_oss(path, relative_path, oss_config)?;
    }

    if config.move_to_nas {
        fs::remove_file(path).map_err(io_to_parquet_error)?;
        if let Some(dest_path) = nas_dest {
            info!("Archive handoff complete: local={} nas={}", path.display(), dest_path.display());
        }
    } else if config.upload_to_oss {
        info!("Archive handoff complete: local={} retained after OSS upload", path.display());
    } else {
        info!("Archive finalized locally without handoff: {}", path.display());
    }
    Ok(())
}

fn disable_archive_after_failure(
    writers: &mut Option<ArchiveWriters>,
    current_mode: &mut Option<ArchiveMode>,
    current_symbols: &mut Vec<String>,
    height: u64,
    context: &str,
) {
    error!("Disabling archive after fatal failure at block {}: {}", height, context);
    if let Some(mut active) = writers.take() {
        active.abort_all();
    }
    *current_mode = None;
    *current_symbols = current_archive_symbols();
    set_archive_mode(None);
}

fn restart_archive_after_handoff(
    writers: &mut Option<ArchiveWriters>,
    handoff_tx: &Sender<HandoffMessage>,
    current_mode: &mut Option<ArchiveMode>,
    current_symbols: &mut Vec<String>,
    bootstrap_checkpoint_evaluated: &mut bool,
    height: u64,
) {
    let Some(mode) = *current_mode else {
        return;
    };
    let symbols = current_symbols.clone();
    let mut active = writers.take().expect("archive writers should exist when restarting");
    match active.close_all() {
        Ok(()) => {}
        Err(err) => {
            warn!("Archive close during disk-pressure restart failed at {}: {err}", height);
            active.abort_all();
            disable_archive_after_failure(
                writers,
                current_mode,
                current_symbols,
                height,
                &format!("archive restart close failed: {err}"),
            );
            return;
        }
    }
    if let Err(err) = drain_handoff_tasks(handoff_tx) {
        disable_archive_after_failure(
            writers,
            current_mode,
            current_symbols,
            height,
            &format!("archive restart handoff drain failed: {err}"),
        );
        return;
    }
    match ensure_archive_disk_headroom(&current_archive_base_dir()) {
        Ok(()) => {
            info!(
                "Archive restarted after disk-pressure handoff at block {} with symbols={}",
                height,
                symbols.join(",")
            );
            *writers = Some(ArchiveWriters::new(mode, symbols, handoff_tx.clone()));
            *bootstrap_checkpoint_evaluated = false;
        }
        Err(err) => {
            disable_archive_after_failure(
                writers,
                current_mode,
                current_symbols,
                height,
                &format!("archive restart still blocked after handoff: {err}"),
            );
        }
    }
}

fn rotate_archive_after_discontinuity(
    writers: &mut Option<ArchiveWriters>,
    handoff_tx: &Sender<HandoffMessage>,
    current_mode: &mut Option<ArchiveMode>,
    current_symbols: &mut Vec<String>,
    bootstrap_checkpoint_evaluated: &mut bool,
    last_input_height: &mut Option<u64>,
    previous_height: u64,
    next_height: u64,
) {
    let Some(mode) = *current_mode else {
        return;
    };
    let symbols = current_symbols.clone();
    let mut active = writers.take().expect("archive writers should exist when rotating after discontinuity");
    match active.close_all() {
        Ok(()) => {}
        Err(err) => {
            warn!(
                "Archive close during discontinuity rotate failed at prev_height={} next_height={}: {err}",
                previous_height, next_height
            );
            active.abort_all();
            disable_archive_after_failure(
                writers,
                current_mode,
                current_symbols,
                next_height,
                &format!("archive discontinuity rotate close failed: {err}"),
            );
            return;
        }
    }
    if let Err(err) = drain_handoff_tasks(handoff_tx) {
        disable_archive_after_failure(
            writers,
            current_mode,
            current_symbols,
            next_height,
            &format!("archive discontinuity rotate handoff drain failed: {err}"),
        );
        return;
    }
    match ensure_archive_disk_headroom(&current_archive_base_dir()) {
        Ok(()) => {
            info!(
                "Archive rotated after discontinuity prev_height={} next_height={} symbols={}",
                previous_height,
                next_height,
                symbols.join(",")
            );
            *writers = Some(ArchiveWriters::new(mode, symbols, handoff_tx.clone()));
            *bootstrap_checkpoint_evaluated = false;
            *last_input_height = None;
        }
        Err(err) => {
            disable_archive_after_failure(
                writers,
                current_mode,
                current_symbols,
                next_height,
                &format!("archive discontinuity rotate blocked by disk headroom: {err}"),
            );
        }
    }
}

#[derive(Default)]
struct ReplaySkipLogState {
    first_height: Option<u64>,
    last_height: Option<u64>,
    last_archived_height: Option<u64>,
    started_at: Option<Instant>,
    last_warn_at: Option<Instant>,
    skipped_blocks: u64,
}

impl ReplaySkipLogState {
    fn record_skip(&mut self, height: u64, last_archived_height: u64) {
        let now = Instant::now();
        if self.skipped_blocks == 0 {
            self.first_height = Some(height);
            self.started_at = Some(now);
            self.last_warn_at = Some(now);
            self.last_archived_height = Some(last_archived_height);
        }
        self.last_height = Some(height);
        self.last_archived_height = Some(last_archived_height);
        self.skipped_blocks = self.skipped_blocks.saturating_add(1);
    }

    fn should_warn(&self) -> bool {
        self.skipped_blocks > 0
            && self.last_warn_at.is_some_and(|last_warn_at| {
                Instant::now().saturating_duration_since(last_warn_at) >= Duration::from_secs(10)
            })
    }

    fn log_warn(&mut self) {
        let Some(first_height) = self.first_height else {
            return;
        };
        let Some(last_height) = self.last_height else {
            return;
        };
        let Some(last_archived_height) = self.last_archived_height else {
            return;
        };
        let elapsed =
            self.started_at.map(|started_at| Instant::now().saturating_duration_since(started_at)).unwrap_or_default();
        warn!(
            "Archive replay dedupe skipping old blocks first_height={} last_height={} count={} last_archived_height={} elapsed_s={:.1}",
            first_height,
            last_height,
            self.skipped_blocks,
            last_archived_height,
            elapsed.as_secs_f64()
        );
        self.last_warn_at = Some(Instant::now());
    }

    fn log_resume_and_reset(&mut self, next_height: u64) {
        let Some(first_height) = self.first_height else {
            return;
        };
        let Some(last_height) = self.last_height else {
            return;
        };
        let Some(last_archived_height) = self.last_archived_height else {
            return;
        };
        let elapsed =
            self.started_at.map(|started_at| Instant::now().saturating_duration_since(started_at)).unwrap_or_default();
        info!(
            "Archive replay dedupe resumed writes at height={} after skipping old blocks first_height={} last_height={} count={} last_archived_height={} elapsed_s={:.1}",
            next_height,
            first_height,
            last_height,
            self.skipped_blocks,
            last_archived_height,
            elapsed.as_secs_f64()
        );
        *self = Self::default();
    }
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

fn byte_array_from_string(value: String) -> ByteArray {
    ByteArray::from(value.into_bytes())
}

fn byte_array_from_str(value: &str) -> ByteArray {
    ByteArray::from(value.as_bytes().to_vec())
}

fn write_status_lite_columns(
    file: &mut SerializedFileWriter<std::fs::File>,
    coin: &str,
    rows: StatusLiteColumns,
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let StatusLiteColumns {
        block_number,
        block_time,
        time,
        user,
        status,
        oid,
        side,
        limit_px,
        sz,
        orig_sz,
        timestamp,
        is_trigger,
        tif,
        trigger_condition,
        trigger_px,
        is_position_tpsl,
        reduce_only,
        order_type,
        cloid,
    } = rows;
    let mut row_group = file.next_row_group()?;
    let coins = vec![byte_array_from_str(coin); block_number.len()];

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_number, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time, None, None)?;
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
            typed.write_batch(&time, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&user, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&status, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&oid, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&side, None, None)?;
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
            typed.write_batch(&sz, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&orig_sz, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&timestamp, None, None)?;
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
            typed.write_batch(&tif, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&trigger_condition, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&trigger_px, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&is_position_tpsl, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&reduce_only, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&order_type, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&cloid, None, None)?;
        }
        col.close()?;
    }

    row_group.close()?;
    trim_allocator();
    Ok(())
}

fn write_status_full_columns(
    file: &mut SerializedFileWriter<std::fs::File>,
    coin: &str,
    rows: StatusFullColumns,
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let StatusFullColumns {
        block_number,
        block_time,
        status,
        oid,
        side,
        limit_px,
        is_trigger,
        tif,
        user,
        hash,
        order_type,
        sz,
        orig_sz,
        time,
        builder,
        timestamp,
        trigger_condition,
        trigger_px,
        is_position_tpsl,
        reduce_only,
        cloid,
        raw_event,
    } = rows;
    let mut row_group = file.next_row_group()?;
    let coins = vec![byte_array_from_str(coin); block_number.len()];

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_number, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time, None, None)?;
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
            typed.write_batch(&status, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&oid, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&side, None, None)?;
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
            typed.write_batch(&tif, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&user, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&hash, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&order_type, None, None)?;
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
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&orig_sz, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&time, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&builder, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&timestamp, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&trigger_condition, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&trigger_px, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&is_position_tpsl, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&reduce_only, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&cloid, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&raw_event, None, None)?;
        }
        col.close()?;
    }

    row_group.close()?;
    trim_allocator();
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
    trim_allocator();
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
    trim_allocator();
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
    trim_allocator();
    Ok(())
}

pub(crate) fn run_archive_writer(rx: Receiver<ArchiveBlock>, stop: Arc<AtomicBool>) {
    let handoff_worker = start_archive_handoff_worker(stop.clone());
    let handoff_tx = handoff_worker.tx.clone();
    let mut writers: Option<ArchiveWriters> = None;
    let mut current_mode: Option<ArchiveMode> = None;
    let mut current_symbols = current_archive_symbols();
    let mut bootstrap_checkpoint_evaluated = false;
    let mut start_alignment_wait_logged = false;
    let mut last_input_height: Option<u64> = None;
    let mut replay_skip_log = ReplaySkipLogState::default();

    loop {
        let mode = get_archive_mode();
        let symbols = current_archive_symbols();
        if mode != current_mode || symbols != current_symbols {
            if let Some(mut w) = writers.take() {
                if let Err(err) = w.close_all() {
                    warn!("Archive writer close failed during reconfigure: {err}");
                    w.abort_all();
                }
            }
            if mode.is_some() {
                let base_dir = current_archive_base_dir();
                cleanup_stale_temp_files(&base_dir);
                if let Err(err) = ensure_archive_disk_headroom(&base_dir) {
                    error!("Refusing to enable archive: {err}");
                    set_archive_mode(None);
                    current_mode = None;
                    current_symbols = symbols;
                    bootstrap_checkpoint_evaluated = false;
                    continue;
                }
            }
            writers = mode.map(|mode| ArchiveWriters::new(mode, symbols.clone(), handoff_tx.clone()));
            current_mode = mode;
            current_symbols = symbols;
            bootstrap_checkpoint_evaluated = false;
            start_alignment_wait_logged = false;
            last_input_height = None;
            replay_skip_log = ReplaySkipLogState::default();
        }

        let msg = match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(msg) => msg,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    continue;
                }
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if writers.is_none() {
            continue;
        }
        let mode = writers.as_ref().expect("writers checked above").mode;
        if let Some(writers_ref) = writers.as_mut() {
            if let Err(err) = writers_ref.drain_status_progress() {
                disable_archive_after_failure(
                    &mut writers,
                    &mut current_mode,
                    &mut current_symbols,
                    msg.block_number,
                    &format!("status progress drain failed: {err}"),
                );
                continue;
            }
        }

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

        if !bootstrap_checkpoint_evaluated
            && current_archive_align_start_to_10k_boundary()
            && !is_checkpoint_start(height)
        {
            if !start_alignment_wait_logged {
                info!(
                    "Archive waiting for next 10k boundary start; skipping blocks until ..00001, first_seen={}",
                    height
                );
                start_alignment_wait_logged = true;
            }
            continue;
        }

        if writers.as_ref().and_then(ArchiveWriters::last_archived_height).is_some_and(|last_archived_height| {
            if height <= last_archived_height {
                replay_skip_log.record_skip(height, last_archived_height);
                if replay_skip_log.should_warn() {
                    replay_skip_log.log_warn();
                }
                true
            } else {
                false
            }
        }) {
            continue;
        }

        if replay_skip_log.skipped_blocks > 0 {
            replay_skip_log.log_resume_and_reset(height);
        }

        if let Some(previous_height) = last_input_height.filter(|last| height > last.saturating_add(1)) {
            rotate_archive_after_discontinuity(
                &mut writers,
                &handoff_tx,
                &mut current_mode,
                &mut current_symbols,
                &mut bootstrap_checkpoint_evaluated,
                &mut last_input_height,
                previous_height,
                height,
            );
            if writers.is_none() {
                continue;
            }
        }

        if !bootstrap_checkpoint_evaluated {
            bootstrap_checkpoint_evaluated = true;
            start_alignment_wait_logged = false;
            if current_archive_align_start_to_10k_boundary() {
                info!("Archive aligned start at checkpoint boundary block {}", height);
            } else if !is_checkpoint_start(height) {
                info!(
                    "Archive enabled mid-window at block {}; starting parquet immediately and bootstrapping snapshot checkpoint in background",
                    height
                );
                let bootstrap_symbols = writers.as_ref().expect("writers checked above").symbols.clone();
                spawn_mid_window_checkpoint_fetch(height, order_batch.block_time.clone(), bootstrap_symbols);
            }
        }

        let block_time = order_batch.block_time.clone();
        let order_n = order_batch.events.len() as i32;
        let diff_n = diff_batch.events.len() as i32;
        let fill_n = fill_batch.events.len() as i32;
        let tracked_symbols = writers.as_ref().expect("writers checked above").symbols.join(",");
        let archive_mode = match mode {
            ArchiveMode::Lite => "lite".to_string(),
            ArchiveMode::Full => "full".to_string(),
        };

        let mut status_rows: HashMap<String, StatusBlockBatch> = HashMap::new();
        let mut btc_status_n = 0;
        let mut eth_status_n = 0;
        let block_time_bytes = byte_array_from_str(&block_time);
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
            if !writers.as_ref().expect("writers checked above").has_coin(&coin) {
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
            let entry = status_rows.entry(coin.clone()).or_insert_with(|| StatusBlockBatch::new(mode));
            match entry {
                StatusBlockBatch::Lite(columns) => {
                    columns.block_number.push(height as i64);
                    columns.block_time.push(block_time_bytes.clone());
                    columns.time.push(byte_array_from_string(status.time));
                    columns.user.push(byte_array_from_string(status.user.unwrap_or_default()));
                    columns.status.push(byte_array_from_string(status.status));
                    columns.oid.push(oid as i64);
                    columns.side.push(byte_array_from_string(side));
                    columns.limit_px.push(s_px);
                    columns.sz.push(s_sz);
                    columns.orig_sz.push(s_orig);
                    columns.timestamp.push(timestamp as i64);
                    columns.is_trigger.push(is_trigger);
                    columns.tif.push(byte_array_from_string(tif.unwrap_or_default()));
                    columns.trigger_condition.push(byte_array_from_string(trigger_condition));
                    columns.trigger_px.push(s_trig);
                    columns.is_position_tpsl.push(is_position_tpsl);
                    columns.reduce_only.push(reduce_only);
                    columns.order_type.push(byte_array_from_string(order_type));
                    columns.cloid.push(byte_array_from_string(flatten_to_string(&cloid)));
                }
                StatusBlockBatch::Full(columns) => {
                    columns.block_number.push(height as i64);
                    columns.block_time.push(block_time_bytes.clone());
                    columns.status.push(byte_array_from_string(status.status));
                    columns.oid.push(oid as i64);
                    columns.side.push(byte_array_from_string(side));
                    columns.limit_px.push(s_px);
                    columns.is_trigger.push(is_trigger);
                    columns.tif.push(byte_array_from_string(tif.unwrap_or_default()));
                    columns.user.push(byte_array_from_string(status.user.unwrap_or_default()));
                    columns.hash.push(byte_array_from_string(flatten_to_string(&status.hash)));
                    columns.order_type.push(byte_array_from_string(order_type));
                    columns.sz.push(s_sz);
                    columns.orig_sz.push(s_orig);
                    columns.time.push(byte_array_from_string(status.time));
                    columns.builder.push(byte_array_from_string(flatten_to_string(&status.builder)));
                    columns.timestamp.push(timestamp as i64);
                    columns.trigger_condition.push(byte_array_from_string(trigger_condition));
                    columns.trigger_px.push(s_trig);
                    columns.is_position_tpsl.push(is_position_tpsl);
                    columns.reduce_only.push(reduce_only);
                    columns.cloid.push(byte_array_from_string(flatten_to_string(&cloid)));
                    columns.raw_event.push(byte_array_from_string(
                        order_batch_raw
                            .as_ref()
                            .and_then(|batch| batch.events.get(idx))
                            .map_or_else(String::new, serde_json::Value::to_string),
                    ));
                }
            }
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
            if !writers.as_ref().expect("writers checked above").has_coin(&coin) {
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
            if !writers.as_ref().expect("writers checked above").has_coin(&coin) {
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

        let mut fatal_error: Option<String> = None;
        let writers_ref = writers.as_mut().expect("writers checked above");
        if let Err(err) = writers_ref.blocks.advance_block(mode, height, write_block_rows) {
            disable_archive_after_failure(
                &mut writers,
                &mut current_mode,
                &mut current_symbols,
                height,
                &format!("blocks advance failed: {err}"),
            );
            continue;
        }
        if let Err(err) = writers_ref.blocks.append_rows(
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
            disable_archive_after_failure(
                &mut writers,
                &mut current_mode,
                &mut current_symbols,
                height,
                &format!("blocks write failed: {err}"),
            );
            continue;
        }

        writers_ref.register_status_block(height);

        if let Err(err) = writers_ref.diff.send_block(height, diff_rows) {
            disable_archive_after_failure(
                &mut writers,
                &mut current_mode,
                &mut current_symbols,
                height,
                &format!("diff write failed: {err}"),
            );
            continue;
        }

        for coin in writers_ref.symbols.clone() {
            if let Some(coin_writers) = writers_ref.coin_mut(&coin) {
                if let Err(err) = coin_writers.fill.advance_block(mode, height, write_fill_rows) {
                    fatal_error = Some(format!("fill advance failed for {coin}: {err}"));
                    break;
                }
                let status_for_block = status_rows.remove(&coin).unwrap_or_else(|| StatusBlockBatch::new(mode));
                if let Err(err) = coin_writers.status.send_block(height, status_for_block) {
                    fatal_error = Some(format!("status write failed for {coin}: {err}"));
                    break;
                }
                if let Some(rows) = fill_rows.remove(&coin).filter(|rows| !rows.is_empty()) {
                    if let Err(err) = coin_writers.fill.append_rows(&coin, mode, height, rows, write_fill_rows) {
                        fatal_error = Some(format!("fill write failed for {coin}: {err}"));
                        break;
                    }
                }
            }
        }
        if let Some(context) = fatal_error {
            disable_archive_after_failure(&mut writers, &mut current_mode, &mut current_symbols, height, &context);
            continue;
        }
        if let Some(writers_ref) = writers.as_mut() {
            if let Err(err) = writers_ref.drain_status_progress() {
                disable_archive_after_failure(
                    &mut writers,
                    &mut current_mode,
                    &mut current_symbols,
                    height,
                    &format!("status progress drain failed: {err}"),
                );
                continue;
            }
        }
        last_input_height = Some(height);

        if height % CHECKPOINT_BLOCKS == 0 {
            match ensure_archive_disk_headroom(&current_archive_base_dir()) {
                Ok(()) => {}
                Err(err) => {
                    warn!(
                        "Archive disk headroom low at checkpoint block {}; closing current parquet files and draining handoff before restart: {err}",
                        height
                    );
                    restart_archive_after_handoff(
                        &mut writers,
                        &handoff_tx,
                        &mut current_mode,
                        &mut current_symbols,
                        &mut bootstrap_checkpoint_evaluated,
                        height,
                    );
                }
            }
        }
    }

    if let Some(mut w) = writers.take() {
        if let Err(err) = w.close_all() {
            warn!("Archive writer close failed: {err}");
            w.abort_all();
        }
    }
    let ArchiveHandoffWorker { tx: handoff_worker_tx, handle } = handoff_worker;
    drop(handoff_tx);
    drop(handoff_worker_tx);
    if let Err(err) = handle.join() {
        warn!("Archive handoff worker panicked: {err:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock before epoch").as_nanos();
        let dir = std::env::temp_dir().join(format!("fifo_listener_{label}_{nanos}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn parse_local_archive_filename_bounds_works() {
        let path = Path::new("/tmp/BTC_fill_928200001_929000000.parquet");
        assert_eq!(parse_local_archive_filename_bounds(path, "BTC_fill"), Some((928200001, 929000000)));
    }

    #[test]
    fn blocks_local_recovery_resumes_without_duplicates() {
        let _guard = test_lock();
        let base_dir = unique_temp_dir("blocks_recovery");
        let previous_base_dir = current_archive_base_dir();
        let previous_align = current_archive_align_output_to_1000_boundary();
        let previous_recovery = current_archive_recover_blocks_fill_on_stop();
        let previous_handoff = current_archive_handoff_config();
        set_archive_base_dir(Some(base_dir.clone()));
        set_archive_align_output_to_1000_boundary(false);
        set_archive_recover_blocks_fill_on_stop(true);
        set_archive_handoff_config(ArchiveHandoffConfig::new(false, Some(base_dir.clone()), false, None));

        let schema = std::sync::Arc::new(
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
        let props = std::sync::Arc::new(WriterProperties::builder().set_dictionary_enabled(true).build());
        let (handoff_tx, _handoff_rx) = channel::<HandoffMessage>();

        let make_row = |block_number: u64| BlockIndexOut {
            block_number,
            block_time: format!("t{block_number}"),
            order_batch_ok: true,
            diff_batch_ok: true,
            fill_batch_ok: true,
            order_n: 1,
            diff_n: 2,
            fill_n: 3,
            btc_status_n: 4,
            btc_diff_n: 5,
            btc_fill_n: 6,
            eth_status_n: 7,
            eth_diff_n: 8,
            eth_fill_n: 9,
            archive_mode: "lite".to_string(),
            tracked_symbols: "BTC,ETH".to_string(),
        };

        let mut writer =
            ParquetStreamWriter::new(StreamKind::Blocks, schema.clone(), props.clone(), handoff_tx.clone());
        writer.advance_block(ArchiveMode::Lite, 1001, write_block_rows).expect("advance 1001");
        writer
            .append_rows("blocks", ArchiveMode::Lite, 1001, vec![make_row(1001)], write_block_rows)
            .expect("append 1001");
        writer.advance_block(ArchiveMode::Lite, 1002, write_block_rows).expect("advance 1002");
        writer
            .append_rows("blocks", ArchiveMode::Lite, 1002, vec![make_row(1002)], write_block_rows)
            .expect("append 1002");
        writer.close_with_flush(ArchiveMode::Lite, write_block_rows).expect("close first writer");

        let mut recovered =
            ParquetStreamWriter::new(StreamKind::Blocks, schema.clone(), props.clone(), handoff_tx.clone());
        recovered.advance_block(ArchiveMode::Lite, 1001, write_block_rows).expect("re-advance 1001");
        recovered
            .append_rows("blocks", ArchiveMode::Lite, 1001, vec![make_row(1001)], write_block_rows)
            .expect("re-append 1001");
        recovered.advance_block(ArchiveMode::Lite, 1003, write_block_rows).expect("advance 1003");
        recovered
            .append_rows("blocks", ArchiveMode::Lite, 1003, vec![make_row(1003)], write_block_rows)
            .expect("append 1003");
        recovered.close_with_flush(ArchiveMode::Lite, write_block_rows).expect("close recovered writer");

        let final_path = base_dir.join("blocks_1001_1000000.parquet");
        let rows = read_local_blocks_rows(&final_path).expect("read recovered blocks parquet");
        let heights: Vec<u64> = rows.iter().map(|row| row.block_number).collect();
        assert_eq!(heights, vec![1001, 1002, 1003]);

        set_archive_handoff_config(previous_handoff);
        set_archive_recover_blocks_fill_on_stop(previous_recovery);
        set_archive_align_output_to_1000_boundary(previous_align);
        set_archive_base_dir(Some(previous_base_dir));
        fs::remove_dir_all(&base_dir).expect("cleanup temp dir");
    }

    #[test]
    fn fill_lite_local_reader_round_trips() {
        let _guard = test_lock();
        let dir = unique_temp_dir("fill_reader");
        let path = dir.join("BTC_fill_928200001_929000000.parquet");
        let schema = std::sync::Arc::new(
            parse_message_type(
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
            )
            .expect("invalid fill schema"),
        );
        let file = fs::File::create(&path).expect("create fill parquet");
        let mut writer =
            SerializedFileWriter::new(file, schema, std::sync::Arc::new(WriterProperties::builder().build()))
                .expect("create writer");
        let rows = vec![
            FillOut {
                block_number: 928200002,
                block_time: "t1".to_string(),
                coin: "BTC".to_string(),
                side: "B".to_string(),
                px: 11,
                sz: 22,
                crossed: false,
                address: String::new(),
                closed_pnl: 0,
                fee: 0,
                hash: String::new(),
                oid: 33,
                tid: 0,
                time: 44,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: 0,
                raw_event: String::new(),
            },
            FillOut {
                block_number: 928200010,
                block_time: "t2".to_string(),
                coin: "BTC".to_string(),
                side: "A".to_string(),
                px: 55,
                sz: 66,
                crossed: true,
                address: String::new(),
                closed_pnl: 0,
                fee: 0,
                hash: String::new(),
                oid: 77,
                tid: 0,
                time: 88,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: 0,
                raw_event: String::new(),
            },
        ];
        write_fill_rows(&mut writer, ArchiveMode::Lite, &rows).expect("write fill rows");
        writer.close().expect("close fill writer");

        let recovered = read_local_fill_rows(&path, ArchiveMode::Lite).expect("read fill rows");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].block_number, 928200002);
        assert_eq!(recovered[0].oid, 33);
        assert_eq!(recovered[1].block_number, 928200010);
        assert_eq!(recovered[1].time, 88);

        fs::remove_dir_all(&dir).expect("cleanup temp dir");
    }
}
