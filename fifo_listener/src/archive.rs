use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TryRecvError, TrySendError, channel, sync_channel};
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
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

use crate::protocol::{OrderDiff as ParsedOrderDiff, ParsedBlock, RejectOrderEvent};

const ARCHIVE_BASE_DIR: &str = "/home/aimee/hl_runtime/hl/dataset";
const DEFAULT_ARCHIVE_FINALIZE_DIR: &str = "/mnt";
const DEFAULT_ARCHIVE_SYMBOLS: &[&str] = &["BTC", "ETH"];
const DEFAULT_OSS_PREFIX: &str = "hyper_data";
const INFO_SNAPSHOT_URL: &str = "http://localhost:3001/info";
const LOCAL_INFO_URL: &str = "http://127.0.0.1:3001/info";
const USER_FEE_FEATURE_CACHE_CAP: usize = 1023;
const TEMP_FILE_SUFFIX: &str = ".tmp";
const FINAL_PARQUET_FILE_SUFFIX: &str = ".parquet";
const LOCAL_RECOVERY_FILE_SUFFIX: &str = ".parquet.0";
const LITE_PRICE_SCALE: u32 = 8;
const LITE_SIZE_SCALE: u32 = 8;
const CHECKPOINT_BLOCKS: u64 = 10_000;
const ARCHIVE_OUTPUT_ALIGN_BLOCKS: u64 = 1_000;
const STATUS_ROW_GROUP_BLOCKS_DEFAULT: u64 = 10_000;
const REJECT_ROW_GROUP_BLOCKS: u64 = 10_000;
const STATUS_LIVE_DELAYED_FLUSH_LOOKAHEAD_BLOCKS: u64 = 1_000;
const STATUS_LIVE_TRIM_EVERY_ROW_GROUPS: u64 = 10;
const DIFF_ROW_GROUP_BLOCKS: u64 = 10_000;
const DIFF_DELAYED_FLUSH_LOOKAHEAD_BLOCKS: u64 = 2_000;
const MAX_HFT_LIFETIME_MS: i64 = 299_999;
const DIFF_STAGGER_DELAY: Duration = Duration::from_millis(1_500);
const BLOCKS_FILL_ROTATION_BLOCKS: u64 = 1_000_000;
const BLOCKS_FILL_ROW_GROUP_BLOCKS: u64 = 250_000;
const BLOCKS_FILL_DELAYED_FLUSH_LOOKAHEAD_BLOCKS: u64 = 2_000;
const MIN_HANDOFF_BLOCK_SPAN: u64 = 5_000;
const MIN_ARCHIVE_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_DISK_USED_BPS: u64 = 9_500;
const MAX_CONCURRENT_HANDOFF_TASKS: usize = 15;
const STATUS_WORKER_QUEUE_BLOCKS: usize = 16;
const DIFF_WORKER_QUEUE_BLOCKS: usize = 16;
static ROTATION_BLOCKS: AtomicU64 = AtomicU64::new(100_000);
static ARCHIVE_ALIGN_START_TO_10K_BOUNDARY: AtomicBool = AtomicBool::new(true);
static ARCHIVE_ALIGN_OUTPUT_TO_1000_BOUNDARY: AtomicBool = AtomicBool::new(true);
static ARCHIVE_RECOVER_BLOCKS_FILL_ON_STOP: AtomicBool = AtomicBool::new(false);
static ARCHIVE_BASE_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static ARCHIVE_SYMBOLS_OVERRIDE: OnceLock<Mutex<Option<Vec<String>>>> = OnceLock::new();
static ARCHIVE_HANDOFF_CONFIG: OnceLock<Mutex<ArchiveHandoffConfig>> = OnceLock::new();
static ARCHIVE_SESSION_REGISTRY: OnceLock<Mutex<ArchiveSessionRegistry>> = OnceLock::new();
static REJECT_ARCHIVE_SESSION_REGISTRY: OnceLock<Mutex<RejectArchiveSessionRegistry>> = OnceLock::new();
static ARCHIVE_SHARED_BLOCKS: OnceLock<Mutex<SharedBlocksIndex>> = OnceLock::new();
static ARCHIVE_STALE_TMP_CLEANED: AtomicBool = AtomicBool::new(false);
static REJECT_ARCHIVE_DROPPED_BLOCKS: AtomicU64 = AtomicU64::new(0);
static USER_FEE_FEATURE_CACHE: OnceLock<Mutex<UserFeeFeatureCache>> = OnceLock::new();
static LOCAL_INFO_CLIENT: OnceLock<Client> = OnceLock::new();
const OUTPUT_TIMESTAMP_MILLIS_UTC: &[time::format_description::FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");
const INPUT_TIMESTAMP_NO_TZ_WITH_SUBSEC: &[time::format_description::FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");
const INPUT_TIMESTAMP_NO_TZ_NO_SUBSEC: &[time::format_description::FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
pub(crate) const ARCHIVE_QUEUE_BLOCKS: usize = 511;

thread_local! {
    static ARCHIVE_THREAD_CONTEXT: RefCell<Option<ArchiveThreadContext>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveMode {
    Lite = 1,
    Full = 2,
    Hft = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveDecimalScales {
    pub px_scale: u32,
    pub sz_scale: u32,
}

impl Default for ArchiveDecimalScales {
    fn default() -> Self {
        Self { px_scale: LITE_PRICE_SCALE, sz_scale: LITE_SIZE_SCALE }
    }
}

impl ArchiveMode {
    fn uses_full_scaling(self) -> bool {
        matches!(self, Self::Full | Self::Hft)
    }
}

pub type ArchiveSessionId = u64;

#[derive(Clone, Debug)]
pub struct ArchiveSessionOptions {
    pub mode: ArchiveMode,
    pub rotation_blocks: u64,
    pub output_dir: PathBuf,
    pub symbols: Vec<String>,
    pub symbol_decimals: HashMap<String, ArchiveDecimalScales>,
    pub archive_height: Option<u64>,
    pub stop_height: Option<u64>,
    pub align_start_to_10k_boundary: bool,
    pub align_output_to_1000_boundary: bool,
    pub handoff: ArchiveHandoffConfig,
}

#[derive(Clone, Debug)]
struct ArchiveThreadContext {
    mode: ArchiveMode,
    enabled: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    phase_state: Arc<Mutex<ArchivePhaseState>>,
    rotation_blocks: u64,
    base_dir: PathBuf,
    shared_blocks_base_dir: PathBuf,
    symbols: Vec<String>,
    symbol_decimals: HashMap<String, ArchiveDecimalScales>,
    archive_height: Option<u64>,
    stop_height: Option<u64>,
    handoff: ArchiveHandoffConfig,
    align_start_to_10k_boundary: bool,
    align_output_to_1000_boundary: bool,
    recover_blocks_fill_on_stop: Arc<AtomicBool>,
}

impl ArchiveThreadContext {
    fn from_options(
        options: &ArchiveSessionOptions,
        shared_blocks_base_dir: PathBuf,
        enabled: Arc<AtomicBool>,
        stop_requested: Arc<AtomicBool>,
        phase_state: Arc<Mutex<ArchivePhaseState>>,
        recover_blocks_fill_on_stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            mode: options.mode,
            enabled,
            stop_requested,
            phase_state,
            rotation_blocks: options.rotation_blocks,
            base_dir: options.output_dir.clone(),
            shared_blocks_base_dir,
            symbols: options.symbols.clone(),
            symbol_decimals: options.symbol_decimals.clone(),
            archive_height: options.archive_height,
            stop_height: options.stop_height,
            handoff: options.handoff.clone(),
            align_start_to_10k_boundary: options.align_start_to_10k_boundary,
            align_output_to_1000_boundary: options.align_output_to_1000_boundary,
            recover_blocks_fill_on_stop,
        }
    }
}

struct ArchiveSessionHandle {
    tx: SyncSender<ArchiveBlock>,
    enabled: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    phase_state: Arc<Mutex<ArchivePhaseState>>,
    recover_blocks_fill_on_stop: Arc<AtomicBool>,
    cleanup_dir: Option<PathBuf>,
    join_handle: thread::JoinHandle<()>,
}

struct RejectArchiveSessionHandle {
    tx: SyncSender<RejectArchiveBlock>,
    enabled: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    phase_state: Arc<Mutex<ArchivePhaseState>>,
    cleanup_dir: Option<PathBuf>,
    join_handle: thread::JoinHandle<()>,
}

#[derive(Debug)]
struct ArchivePhaseState {
    block_height: Option<u64>,
    phase: String,
    phase_since: Instant,
}

impl Default for ArchivePhaseState {
    fn default() -> Self {
        Self { block_height: None, phase: "starting".to_string(), phase_since: Instant::now() }
    }
}

#[derive(Default)]
struct ArchiveSessionRegistry {
    next_id: ArchiveSessionId,
    sessions: HashMap<ArchiveSessionId, ArchiveSessionHandle>,
}

#[derive(Default)]
struct RejectArchiveSessionRegistry {
    next_id: ArchiveSessionId,
    sessions: HashMap<ArchiveSessionId, RejectArchiveSessionHandle>,
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

fn current_rotation_blocks() -> u64 {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.rotation_blocks))
        .unwrap_or_else(|| ROTATION_BLOCKS.load(Ordering::SeqCst))
}

pub fn current_rotation_blocks_value() -> u64 {
    current_rotation_blocks()
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

fn archive_session_registry() -> &'static Mutex<ArchiveSessionRegistry> {
    ARCHIVE_SESSION_REGISTRY.get_or_init(|| Mutex::new(ArchiveSessionRegistry { next_id: 1, ..Default::default() }))
}

fn reject_archive_session_registry() -> &'static Mutex<RejectArchiveSessionRegistry> {
    REJECT_ARCHIVE_SESSION_REGISTRY
        .get_or_init(|| Mutex::new(RejectArchiveSessionRegistry { next_id: 1, ..Default::default() }))
}

fn shared_blocks_index() -> &'static Mutex<SharedBlocksIndex> {
    ARCHIVE_SHARED_BLOCKS.get_or_init(|| Mutex::new(SharedBlocksIndex::new()))
}

fn finalize_shared_blocks_after_last_session(recover_for_restart: bool) {
    let no_active_sessions = archive_session_registry().lock().expect("archive session registry").sessions.is_empty();
    if !no_active_sessions {
        return;
    }
    let mut shared = shared_blocks_index().lock().expect("shared blocks index");
    if recover_for_restart {
        if let Err(err) = shared.persist_for_recovery() {
            warn!("Shared blocks recovery persist failed: {err}");
        }
    } else {
        shared.reset_runtime_state();
    }
}

fn reap_finished_archive_handle_sessions(
    registry_mutex: &'static Mutex<ArchiveSessionRegistry>,
    label: &str,
    finalize_shared: bool,
) {
    let finished: Vec<(ArchiveSessionId, ArchiveSessionHandle)> = {
        let mut registry = registry_mutex.lock().expect("archive session registry");
        let finished_ids: Vec<ArchiveSessionId> = registry
            .sessions
            .iter()
            .filter_map(|(session_id, handle)| handle.join_handle.is_finished().then_some(*session_id))
            .collect();
        finished_ids
            .into_iter()
            .filter_map(|session_id| registry.sessions.remove(&session_id).map(|handle| (session_id, handle)))
            .collect()
    };
    if finished.is_empty() {
        return;
    }
    for (session_id, handle) in finished {
        if let Err(err) = handle.join_handle.join() {
            warn!("{label} session {session_id} panicked while reaping: {err:?}");
        } else {
            warn!("{label} session {} thread exited", session_id);
            cleanup_empty_session_dir(handle.cleanup_dir.as_deref());
        }
    }
    if finalize_shared {
        finalize_shared_blocks_after_last_session(false);
    }
}

fn reap_finished_archive_sessions() {
    reap_finished_archive_handle_sessions(archive_session_registry(), "Archive", true);
}

fn reap_finished_reject_archive_sessions() {
    let finished: Vec<(ArchiveSessionId, RejectArchiveSessionHandle)> = {
        let mut registry = reject_archive_session_registry().lock().expect("reject archive session registry");
        let finished_ids: Vec<ArchiveSessionId> = registry
            .sessions
            .iter()
            .filter_map(|(session_id, handle)| handle.join_handle.is_finished().then_some(*session_id))
            .collect();
        finished_ids
            .into_iter()
            .filter_map(|session_id| registry.sessions.remove(&session_id).map(|handle| (session_id, handle)))
            .collect()
    };
    if finished.is_empty() {
        return;
    }
    for (session_id, handle) in finished {
        if let Err(err) = handle.join_handle.join() {
            warn!("Reject archive session {session_id} panicked while reaping: {err:?}");
        } else {
            warn!("Reject archive session {} thread exited", session_id);
            cleanup_empty_session_dir(handle.cleanup_dir.as_deref());
        }
    }
}

fn archive_thread_context<T>(f: impl FnOnce(Option<&ArchiveThreadContext>) -> T) -> T {
    ARCHIVE_THREAD_CONTEXT.with(|ctx| {
        let borrow = ctx.borrow();
        f(borrow.as_ref())
    })
}

fn cleanup_empty_session_dir(path: Option<&Path>) {
    let Some(path) = path else {
        return;
    };
    match fs::remove_dir(path) {
        Ok(()) => info!("Removed empty archive session dir {}", path.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
        Err(err) => warn!("Failed to remove archive session dir {}: {err}", path.display()),
    }
}

fn set_archive_thread_context(context: Option<ArchiveThreadContext>) {
    ARCHIVE_THREAD_CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = context;
    });
}

fn update_archive_thread_context(f: impl FnOnce(&mut ArchiveThreadContext)) {
    ARCHIVE_THREAD_CONTEXT.with(|ctx| {
        if let Some(current) = ctx.borrow_mut().as_mut() {
            f(current);
        }
    });
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
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.base_dir.clone())).unwrap_or_else(|| {
        archive_base_dir_override()
            .lock()
            .expect("archive base dir mutex")
            .clone()
            .unwrap_or_else(|| PathBuf::from(ARCHIVE_BASE_DIR))
    })
}

fn current_archive_shared_blocks_base_dir() -> PathBuf {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.shared_blocks_base_dir.clone()))
        .unwrap_or_else(current_archive_base_dir)
}

pub fn set_archive_base_dir(path: Option<PathBuf>) -> PathBuf {
    let mut guard = archive_base_dir_override().lock().expect("archive base dir mutex");
    *guard = path;
    guard.clone().unwrap_or_else(|| PathBuf::from(ARCHIVE_BASE_DIR))
}

pub fn current_archive_symbols() -> Vec<String> {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.symbols.clone())).unwrap_or_else(|| {
        archive_symbols_override()
            .lock()
            .expect("archive symbols mutex")
            .clone()
            .map(|symbols| normalize_archive_symbols(Some(symbols)))
            .unwrap_or_else(|| normalize_archive_symbols(None))
    })
}

pub fn set_archive_symbols(symbols: Option<Vec<String>>) -> Vec<String> {
    let normalized = normalize_archive_symbols(symbols);
    let mut guard = archive_symbols_override().lock().expect("archive symbols mutex");
    *guard = Some(normalized.clone());
    normalized
}

fn current_archive_symbol_decimals() -> HashMap<String, ArchiveDecimalScales> {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.symbol_decimals.clone())).unwrap_or_else(|| {
        current_archive_symbols().into_iter().map(|symbol| (symbol, ArchiveDecimalScales::default())).collect()
    })
}

fn current_archive_coin_decimal_scales(coin: &str) -> ArchiveDecimalScales {
    current_archive_symbol_decimals().get(coin).copied().unwrap_or_default()
}

pub(crate) fn current_archive_handoff_config() -> ArchiveHandoffConfig {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.handoff.clone()))
        .unwrap_or_else(|| archive_handoff_config().lock().expect("archive handoff config mutex").clone())
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
        Some(ArchiveMode::Hft) => 3,
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
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.align_start_to_10k_boundary))
        .unwrap_or_else(|| ARCHIVE_ALIGN_START_TO_10K_BOUNDARY.load(Ordering::SeqCst))
}

pub(crate) fn current_archive_align_output_to_1000_boundary() -> bool {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.align_output_to_1000_boundary))
        .unwrap_or_else(|| ARCHIVE_ALIGN_OUTPUT_TO_1000_BOUNDARY.load(Ordering::SeqCst))
}

pub(crate) fn current_archive_recover_blocks_fill_on_stop() -> bool {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.recover_blocks_fill_on_stop.load(Ordering::SeqCst)))
        .unwrap_or_else(|| ARCHIVE_RECOVER_BLOCKS_FILL_ON_STOP.load(Ordering::SeqCst))
}

pub(crate) fn current_archive_stop_height() -> Option<u64> {
    archive_thread_context(|ctx| ctx.and_then(|ctx| ctx.stop_height))
}

pub(crate) fn current_archive_height_span() -> Option<u64> {
    archive_thread_context(|ctx| ctx.and_then(|ctx| ctx.archive_height))
}

pub(crate) fn current_archive_stop_requested() -> bool {
    archive_thread_context(|ctx| ctx.map(|ctx| ctx.stop_requested.load(Ordering::SeqCst))).unwrap_or(false)
}

fn set_archive_phase(block_height: Option<u64>, phase: impl Into<String>) {
    archive_thread_context(|ctx| {
        if let Some(ctx) = ctx {
            if let Ok(mut state) = ctx.phase_state.lock() {
                state.block_height = block_height;
                state.phase = phase.into();
                state.phase_since = Instant::now();
            }
        }
    });
}

pub(crate) fn get_archive_mode() -> Option<ArchiveMode> {
    if let Some(mode) =
        archive_thread_context(|ctx| ctx.and_then(|ctx| ctx.enabled.load(Ordering::SeqCst).then_some(ctx.mode)))
    {
        Some(mode)
    } else {
        match ARCHIVE_MODE.load(Ordering::SeqCst) {
            1 => Some(ArchiveMode::Lite),
            2 => Some(ArchiveMode::Full),
            3 => Some(ArchiveMode::Hft),
            _ => None,
        }
    }
}

pub fn start_archive_session(options: ArchiveSessionOptions) -> ArchiveSessionId {
    reap_finished_archive_sessions();
    let mut registry = archive_session_registry().lock().expect("archive session registry");
    let session_id = registry.next_id;
    registry.next_id = registry.next_id.saturating_add(1);
    let mut options = options;
    let shared_blocks_base_dir = options.output_dir.clone();
    let mut cleanup_dir = None;
    if options.stop_height.is_some() || options.archive_height.is_some() {
        options.output_dir = options.output_dir.join(format!("session_{session_id}"));
        if let Err(err) = fs::create_dir_all(&options.output_dir) {
            panic!("failed to create per-session archive output dir {}: {err}", options.output_dir.display());
        }
        cleanup_dir = Some(options.output_dir.clone());
    }
    let (tx, rx) = sync_channel(ARCHIVE_QUEUE_BLOCKS);
    let enabled = Arc::new(AtomicBool::new(true));
    let stop_requested = Arc::new(AtomicBool::new(false));
    let phase_state = Arc::new(Mutex::new(ArchivePhaseState::default()));
    let recover_blocks_fill_on_stop = Arc::new(AtomicBool::new(false));
    let thread_context = ArchiveThreadContext::from_options(
        &options,
        shared_blocks_base_dir,
        enabled.clone(),
        stop_requested.clone(),
        phase_state.clone(),
        recover_blocks_fill_on_stop.clone(),
    );
    let stop = stop_requested.clone();
    let join_handle = thread::spawn(move || {
        set_archive_thread_context(Some(thread_context));
        run_archive_writer(rx, stop);
        set_archive_thread_context(None);
    });
    registry.sessions.insert(
        session_id,
        ArchiveSessionHandle {
            tx,
            enabled,
            stop_requested,
            phase_state,
            recover_blocks_fill_on_stop,
            cleanup_dir,
            join_handle,
        },
    );
    session_id
}

pub fn start_reject_archive_session(mut options: ArchiveSessionOptions) -> ArchiveSessionId {
    if options.mode != ArchiveMode::Hft {
        options.mode = ArchiveMode::Hft;
    }
    reap_finished_reject_archive_sessions();
    let mut registry = reject_archive_session_registry().lock().expect("reject archive session registry");
    let session_id = registry.next_id;
    registry.next_id = registry.next_id.saturating_add(1);
    let mut cleanup_dir = None;
    if options.stop_height.is_some() || options.archive_height.is_some() {
        options.output_dir = options.output_dir.join(format!("session_{session_id}"));
        if let Err(err) = fs::create_dir_all(&options.output_dir) {
            panic!("failed to create per-session reject archive output dir {}: {err}", options.output_dir.display());
        }
        cleanup_dir = Some(options.output_dir.clone());
    }
    let (tx, rx) = sync_channel(ARCHIVE_QUEUE_BLOCKS);
    let enabled = Arc::new(AtomicBool::new(true));
    let stop_requested = Arc::new(AtomicBool::new(false));
    let phase_state = Arc::new(Mutex::new(ArchivePhaseState::default()));
    let recover_blocks_fill_on_stop = Arc::new(AtomicBool::new(false));
    let thread_context = ArchiveThreadContext::from_options(
        &options,
        options.output_dir.clone(),
        enabled.clone(),
        stop_requested.clone(),
        phase_state.clone(),
        recover_blocks_fill_on_stop.clone(),
    );
    let stop = stop_requested.clone();
    let join_handle = thread::spawn(move || {
        set_archive_thread_context(Some(thread_context));
        run_reject_archive_writer(rx, stop);
        set_archive_thread_context(None);
    });
    registry.sessions.insert(
        session_id,
        RejectArchiveSessionHandle { tx, enabled, stop_requested, phase_state, cleanup_dir, join_handle },
    );
    session_id
}

pub fn stop_archive_session(session_id: ArchiveSessionId, recover_blocks_fill_locally: bool) -> bool {
    reap_finished_archive_sessions();
    let handle = {
        let mut registry = archive_session_registry().lock().expect("archive session registry");
        registry.sessions.remove(&session_id)
    };
    let Some(handle) = handle else {
        return true;
    };
    handle.recover_blocks_fill_on_stop.store(recover_blocks_fill_locally, Ordering::SeqCst);
    handle.stop_requested.store(true, Ordering::SeqCst);
    handle.enabled.store(false, Ordering::SeqCst);
    drop(handle.tx);
    while !handle.join_handle.is_finished() {
        thread::sleep(Duration::from_secs(5));
        if handle.join_handle.is_finished() {
            break;
        }
        if let Ok(state) = handle.phase_state.lock() {
            warn!(
                "Archive session {} stop stuck phase={} block={:?} elapsed_ms={}",
                session_id,
                state.phase,
                state.block_height,
                state.phase_since.elapsed().as_millis()
            );
        }
    }
    if let Err(err) = handle.join_handle.join() {
        warn!("Archive session {session_id} panicked while stopping: {err:?}");
    } else {
        warn!("Archive session {} thread exited", session_id);
        cleanup_empty_session_dir(handle.cleanup_dir.as_deref());
    }
    finalize_shared_blocks_after_last_session(recover_blocks_fill_locally);
    true
}

pub fn stop_reject_archive_session(session_id: ArchiveSessionId) -> bool {
    reap_finished_reject_archive_sessions();
    let handle = {
        let mut registry = reject_archive_session_registry().lock().expect("reject archive session registry");
        registry.sessions.remove(&session_id)
    };
    let Some(handle) = handle else {
        return true;
    };
    handle.stop_requested.store(true, Ordering::SeqCst);
    handle.enabled.store(false, Ordering::SeqCst);
    drop(handle.tx);
    while !handle.join_handle.is_finished() {
        thread::sleep(Duration::from_secs(5));
        if handle.join_handle.is_finished() {
            break;
        }
        if let Ok(state) = handle.phase_state.lock() {
            warn!(
                "Reject archive session {} stop stuck phase={} block={:?} elapsed_ms={}",
                session_id,
                state.phase,
                state.block_height,
                state.phase_since.elapsed().as_millis()
            );
        }
    }
    if let Err(err) = handle.join_handle.join() {
        warn!("Reject archive session {session_id} panicked while stopping: {err:?}");
    } else {
        warn!("Reject archive session {} thread exited", session_id);
        cleanup_empty_session_dir(handle.cleanup_dir.as_deref());
    }
    true
}

pub fn stop_all_archive_sessions(recover_blocks_fill_locally: bool) {
    reap_finished_archive_sessions();
    let handles: Vec<(ArchiveSessionId, ArchiveSessionHandle)> = {
        let mut registry = archive_session_registry().lock().expect("archive session registry");
        registry.sessions.drain().collect()
    };
    for (session_id, handle) in handles {
        handle.recover_blocks_fill_on_stop.store(recover_blocks_fill_locally, Ordering::SeqCst);
        handle.stop_requested.store(true, Ordering::SeqCst);
        handle.enabled.store(false, Ordering::SeqCst);
        drop(handle.tx);
        while !handle.join_handle.is_finished() {
            thread::sleep(Duration::from_secs(5));
            if handle.join_handle.is_finished() {
                break;
            }
            if let Ok(state) = handle.phase_state.lock() {
                warn!(
                    "Archive session {} stop stuck phase={} block={:?} elapsed_ms={}",
                    session_id,
                    state.phase,
                    state.block_height,
                    state.phase_since.elapsed().as_millis()
                );
            }
        }
        if let Err(err) = handle.join_handle.join() {
            warn!("Archive session {session_id} panicked while stopping: {err:?}");
        } else {
            warn!("Archive session {} thread exited via stop-all", session_id);
            cleanup_empty_session_dir(handle.cleanup_dir.as_deref());
        }
    }
    finalize_shared_blocks_after_last_session(recover_blocks_fill_locally);
}

pub fn stop_all_reject_archive_sessions() {
    reap_finished_reject_archive_sessions();
    let handles: Vec<(ArchiveSessionId, RejectArchiveSessionHandle)> = {
        let mut registry = reject_archive_session_registry().lock().expect("reject archive session registry");
        registry.sessions.drain().collect()
    };
    for (session_id, handle) in handles {
        handle.stop_requested.store(true, Ordering::SeqCst);
        handle.enabled.store(false, Ordering::SeqCst);
        drop(handle.tx);
        while !handle.join_handle.is_finished() {
            thread::sleep(Duration::from_secs(5));
            if handle.join_handle.is_finished() {
                break;
            }
            if let Ok(state) = handle.phase_state.lock() {
                warn!(
                    "Reject archive session {} stop stuck phase={} block={:?} elapsed_ms={}",
                    session_id,
                    state.phase,
                    state.block_height,
                    state.phase_since.elapsed().as_millis()
                );
            }
        }
        if let Err(err) = handle.join_handle.join() {
            warn!("Reject archive session {session_id} panicked while stopping: {err:?}");
        } else {
            warn!("Reject archive session {} thread exited via stop-all", session_id);
            cleanup_empty_session_dir(handle.cleanup_dir.as_deref());
        }
    }
}

pub(crate) fn has_archive_sessions() -> bool {
    reap_finished_archive_sessions();
    archive_session_registry()
        .lock()
        .expect("archive session registry")
        .sessions
        .values()
        .any(|handle| handle.enabled.load(Ordering::SeqCst))
}

pub(crate) fn has_reject_archive_sessions() -> bool {
    reap_finished_reject_archive_sessions();
    reject_archive_session_registry()
        .lock()
        .expect("reject archive session registry")
        .sessions
        .values()
        .any(|handle| handle.enabled.load(Ordering::SeqCst))
}

pub(crate) fn dispatch_archive_block(block: ArchiveBlock, shutdown: bool) {
    reap_finished_archive_sessions();
    let session_txs: Vec<(ArchiveSessionId, SyncSender<ArchiveBlock>)> = {
        let registry = archive_session_registry().lock().expect("archive session registry");
        registry
            .sessions
            .iter()
            .filter(|(_, handle)| handle.enabled.load(Ordering::SeqCst))
            .map(|(id, handle)| (*id, handle.tx.clone()))
            .collect()
    };
    if session_txs.is_empty() {
        return;
    }

    let mut stale_sessions = Vec::new();
    for (session_id, tx) in session_txs {
        let mut message = Some(block.clone());
        while let Some(msg) = message.take() {
            let send_res =
                if shutdown { tx.send(msg).map_err(|err| TrySendError::Disconnected(err.0)) } else { tx.try_send(msg) };
            match send_res {
                Ok(()) => break,
                Err(TrySendError::Full(msg)) => {
                    message = Some(msg);
                    thread::sleep(Duration::from_millis(5));
                }
                Err(TrySendError::Disconnected(_msg)) => {
                    stale_sessions.push(session_id);
                    break;
                }
            }
        }
    }

    if !stale_sessions.is_empty() {
        let mut handles = Vec::new();
        {
            let mut registry = archive_session_registry().lock().expect("archive session registry");
            for session_id in stale_sessions {
                if let Some(handle) = registry.sessions.remove(&session_id) {
                    handles.push((session_id, handle));
                }
            }
        }
        for (session_id, handle) in handles {
            if let Err(err) = handle.join_handle.join() {
                warn!("Archive session {session_id} panicked after disconnect: {err:?}");
            }
        }
    }
}

pub(crate) fn dispatch_reject_archive_block(block: RejectArchiveBlock, shutdown: bool) {
    reap_finished_reject_archive_sessions();
    let session_txs: Vec<(ArchiveSessionId, SyncSender<RejectArchiveBlock>)> = {
        let registry = reject_archive_session_registry().lock().expect("reject archive session registry");
        registry
            .sessions
            .iter()
            .filter(|(_, handle)| handle.enabled.load(Ordering::SeqCst))
            .map(|(id, handle)| (*id, handle.tx.clone()))
            .collect()
    };
    if session_txs.is_empty() {
        return;
    }

    let mut stale_sessions = Vec::new();
    for (session_id, tx) in session_txs {
        let mut message = Some(block.clone());
        while let Some(msg) = message.take() {
            let send_res =
                if shutdown { tx.send(msg).map_err(|err| TrySendError::Disconnected(err.0)) } else { tx.try_send(msg) };
            match send_res {
                Ok(()) => break,
                Err(TrySendError::Full(msg)) => {
                    if shutdown {
                        message = Some(msg);
                        thread::sleep(Duration::from_millis(5));
                    } else {
                        let dropped = REJECT_ARCHIVE_DROPPED_BLOCKS.fetch_add(1, Ordering::Relaxed) + 1;
                        if dropped == 1 || dropped % 100 == 0 {
                            warn!(
                                "Reject archive queue full; dropped {} block(s) so far, latest block={} to avoid stalling main order splitter",
                                dropped, msg.block_number
                            );
                        }
                        break;
                    }
                }
                Err(TrySendError::Disconnected(_msg)) => {
                    stale_sessions.push(session_id);
                    break;
                }
            }
        }
    }

    if !stale_sessions.is_empty() {
        let mut handles = Vec::new();
        {
            let mut registry = reject_archive_session_registry().lock().expect("reject archive session registry");
            for session_id in stale_sessions {
                if let Some(handle) = registry.sessions.remove(&session_id) {
                    handles.push((session_id, handle));
                }
            }
        }
        for (session_id, handle) in handles {
            if let Err(err) = handle.join_handle.join() {
                warn!("Reject archive session {session_id} panicked after disconnect: {err:?}");
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArchiveBlock {
    pub(crate) block_number: u64,
    pub(crate) payload: Arc<ParsedBlock>,
}

impl ArchiveBlock {
    pub(crate) fn new(block_number: u64, payload: Arc<ParsedBlock>) -> Self {
        Self { block_number, payload }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RejectArchiveBlock {
    pub(crate) block_number: u64,
    pub(crate) events: Arc<Vec<RejectOrderEvent>>,
}

impl RejectArchiveBlock {
    pub(crate) fn new(block_number: u64, events: Arc<Vec<RejectOrderEvent>>) -> Self {
        Self { block_number, events }
    }
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
    tp_trigger_px: Vec<Option<i64>>,
    sl_trigger_px: Vec<Option<i64>>,
    is_position_tpsl: Vec<bool>,
    reduce_only: Vec<bool>,
    cloid: Vec<ByteArray>,
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
        self.tp_trigger_px.extend(other.tp_trigger_px);
        self.sl_trigger_px.extend(other.sl_trigger_px);
        self.is_position_tpsl.extend(other.is_position_tpsl);
        self.reduce_only.extend(other.reduce_only);
        self.cloid.extend(other.cloid);
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
        self.tp_trigger_px.truncate(keep);
        self.sl_trigger_px.truncate(keep);
        self.is_position_tpsl.truncate(keep);
        self.reduce_only.truncate(keep);
        self.cloid.truncate(keep);
    }
}

#[derive(Debug)]
enum StatusBlockBatch {
    Lite(StatusLiteColumns),
    Full(StatusFullColumns),
    Hft(StatusFullColumns),
}

impl StatusBlockBatch {
    fn new(mode: ArchiveMode) -> Self {
        match mode {
            ArchiveMode::Lite => Self::Lite(StatusLiteColumns::default()),
            ArchiveMode::Full => Self::Full(StatusFullColumns::default()),
            ArchiveMode::Hft => Self::Hft(StatusFullColumns::default()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Lite(columns) => columns.is_empty(),
            Self::Full(columns) => columns.is_empty(),
            Self::Hft(columns) => columns.is_empty(),
        }
    }

    fn trim_to_block(&mut self, max_block: u64) {
        match self {
            Self::Lite(columns) => columns.trim_to_block(max_block),
            Self::Full(columns) => columns.trim_to_block(max_block),
            Self::Hft(columns) => columns.trim_to_block(max_block),
        }
    }
}

fn status_block_batch_start(batch: &StatusBlockBatch) -> Option<u64> {
    match batch {
        StatusBlockBatch::Lite(columns) => columns.block_number.first().map(|value| *value as u64),
        StatusBlockBatch::Full(columns) | StatusBlockBatch::Hft(columns) => {
            columns.block_number.first().map(|value| *value as u64)
        }
    }
}

fn status_block_batch_end(batch: &StatusBlockBatch) -> Option<u64> {
    match batch {
        StatusBlockBatch::Lite(columns) => columns.block_number.last().map(|value| *value as u64),
        StatusBlockBatch::Full(columns) | StatusBlockBatch::Hft(columns) => {
            columns.block_number.last().map(|value| *value as u64)
        }
    }
}

fn split_status_recovered_rows_for_resume(
    actual_start: u64,
    rows: Vec<HftStatusRecoveryRow>,
    span: u64,
) -> RecoveredResumeState<HftStatusRecoveryRow> {
    if rows.is_empty() {
        return RecoveredResumeState { committed_rows: Vec::new(), delayed_rows: Vec::new(), active_rows: Vec::new() };
    }

    let current_group_start = aligned_row_group_bounds(actual_start, span).0;
    let keep_previous_group_delayed = if actual_start <= current_group_start {
        true
    } else {
        actual_start.saturating_sub(current_group_start) < STATUS_LIVE_DELAYED_FLUSH_LOOKAHEAD_BLOCKS
    };
    let delayed_group_start = current_group_start.saturating_sub(span);

    let mut committed_rows = Vec::new();
    let mut delayed_rows = Vec::new();
    let mut active_rows = Vec::new();

    for row in rows {
        let block = row.block_number();
        if block >= current_group_start {
            active_rows.push(row);
        } else if keep_previous_group_delayed && block >= delayed_group_start {
            delayed_rows.push(row);
        } else {
            committed_rows.push(row);
        }
    }

    RecoveredResumeState { committed_rows, delayed_rows, active_rows }
}

#[derive(Debug)]
struct DiffOut {
    block_number: u64,
    block_time: String,
    coin: String,
    oid: u64,
    diff_type: String,
    sz: i64,
    // Full
    user: String,
    side: String,
    px: i64,
    orig_sz: i64,
    event: String,
    status: Option<String>,
    limit_px: Option<i64>,
    _sz: Option<i64>,
    _orig_sz: Option<i64>,
    is_trigger: Option<bool>,
    tif: String,
    reduce_only: Option<bool>,
    order_type: Option<String>,
    trigger_condition: Option<String>,
    trigger_px: Option<i64>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<i64>,
    sl_trigger_px: Option<i64>,
    lifetime: Option<i32>,
}

impl HasBlockNumber for DiffOut {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

#[derive(Debug)]
struct HftStatusRecoveryRow {
    block_number: u64,
    block_time: String,
    user: String,
    oid: u64,
    status: String,
    side: String,
    limit_px: i64,
    sz: i64,
    orig_sz: i64,
    is_trigger: bool,
    tif: String,
    trigger_condition: String,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    tp_trigger_px: Option<i64>,
    sl_trigger_px: Option<i64>,
}

#[derive(Debug, Clone)]
struct HftStatusArchiveRow {
    block_number: u64,
    block_time: String,
    coin: String,
    user: String,
    oid: u64,
    status: String,
    side: String,
    limit_px: i64,
    sz: i64,
    orig_sz: i64,
    is_trigger: bool,
    tif: String,
    trigger_condition: String,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    tp_trigger_px: Option<i64>,
    sl_trigger_px: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HftStatusMigrationWarning {
    block_number: u64,
    coin: String,
    user: String,
    oid: u64,
    diff_type: String,
    statuses: Vec<String>,
}

impl HasBlockNumber for HftStatusRecoveryRow {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

#[derive(Debug)]
struct RejectOut {
    block_number: u64,
    block_time: String,
    coin: String,
    user: String,
    // Reserved for possible future reject full-detail restore. Not written to parquet for now.
    // oid: u64,
    status: String,
    side: String,
    limit_px: i64,
    sz: i64,
    is_trigger: bool,
    tif: String,
    trigger_condition: String,
    trigger_px: i64,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
}

impl HasBlockNumber for RejectOut {
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
    address_m: String,
    oid_m: u64,
    pnl_m: i64,
    fee_m: i64,
    mm_rate: Option<i32>,
    mm_share: Option<i64>,
    filltime: Option<i32>,
    tid: u64,
    start_position: i64,
    dir: String,
    fee_token: String,
    twap_id: Option<i64>,
}

#[derive(Clone, Debug)]
struct UserFeeFeatures {
    mm_rate: i32,
    mm_share: i64,
}

#[derive(Default)]
struct UserFeeFeatureCache {
    day: Option<time::Date>,
    by_user: HashMap<String, Option<UserFeeFeatures>>,
    fifo: VecDeque<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserFeesResponse {
    daily_user_vlm: Vec<DailyUserVlmEntry>,
    user_add_rate: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DailyUserVlmEntry {
    user_add: String,
    exchange: String,
}

impl HasBlockNumber for FillOut {
    fn block_number(&self) -> u64 {
        self.block_number
    }
}

#[derive(Default)]
struct HftTradePair {
    taker: Option<FillOut>,
    maker: Option<FillOut>,
}

#[derive(Clone, Copy, Debug, Default)]
struct HftRemoveStatusSummary {
    has_filled: bool,
    canceled_sz: Option<i64>,
}

fn aggregate_hft_trades(rows: Vec<FillOut>) -> Vec<FillOut> {
    let mut by_tid: BTreeMap<u64, HftTradePair> = BTreeMap::new();
    for row in rows {
        let tid = row.tid;
        let pair = by_tid.entry(tid).or_default();
        if row.crossed {
            if pair.taker.replace(row).is_some() {
                warn!("Dropping duplicate taker-side HFT fill for tid={tid}");
            }
        } else if pair.maker.replace(row).is_some() {
            warn!("Dropping duplicate maker-side HFT fill for tid={tid}");
        }
    }

    let mut trades = Vec::with_capacity(by_tid.len());
    for (tid, pair) in by_tid {
        let Some(mut taker) = pair.taker else {
            warn!("Dropping incomplete HFT trade without taker side for tid={tid}");
            continue;
        };
        let Some(maker) = pair.maker else {
            warn!("Dropping incomplete HFT trade without maker side for tid={tid}");
            continue;
        };
        if taker.block_number != maker.block_number
            || taker.coin != maker.coin
            || taker.px != maker.px
            || taker.sz != maker.sz
        {
            warn!(
                "HFT trade tid={} has mismatched taker/maker fields; keeping taker block/coin/px/sz and maker account fields",
                tid
            );
        }
        taker.address_m = maker.address;
        taker.oid_m = maker.oid;
        taker.pnl_m = maker.closed_pnl;
        taker.fee_m = maker.fee;
        trades.push(taker);
    }
    trades
}

fn hft_status_is_filled(status: &str) -> bool {
    status.eq_ignore_ascii_case("filled")
}

fn hft_status_is_canceled(status: &str) -> bool {
    let normalized = status.to_ascii_lowercase();
    normalized.ends_with("canceled") || normalized.ends_with("cancel")
}

fn hft_status_matches_new(status: &str) -> bool {
    status.eq_ignore_ascii_case("open") || status.eq_ignore_ascii_case("triggered")
}

fn hft_status_matches_remove(status: &str) -> bool {
    hft_status_is_filled(status) || hft_status_is_canceled(status)
}

fn encode_hft_lifetime(lag_ms: i64) -> Option<i32> {
    if lag_ms < 0 {
        return None;
    }
    if lag_ms <= MAX_HFT_LIFETIME_MS {
        return i32::try_from(lag_ms).ok();
    }
    let rounded_minutes = lag_ms.checked_add(30_000)?.checked_div(60_000)?;
    i32::try_from(rounded_minutes).ok().and_then(|minutes| minutes.checked_neg())
}

fn apply_hft_status_to_diff_row(row: &mut DiffOut, status: &HftStatusArchiveRow) {
    row.status = Some(status.status.clone());
    row.limit_px = Some(status.limit_px);
    row._sz = Some(status.sz);
    row._orig_sz = Some(status.orig_sz);
    row.is_trigger = Some(status.is_trigger);
    row.tif = status.tif.clone();
    row.reduce_only = Some(status.reduce_only);
    row.order_type = Some(status.order_type.clone());
    row.trigger_condition = Some(status.trigger_condition.clone());
    row.trigger_px = Some(status.trigger_px);
    row.is_position_tpsl = Some(status.is_position_tpsl);
    row.tp_trigger_px = status.tp_trigger_px;
    row.sl_trigger_px = status.sl_trigger_px;
}

fn resolve_hft_new_candidate_indices(
    candidate_indices: &[usize],
    statuses: &[HftStatusArchiveRow],
) -> Option<(usize, Vec<usize>)> {
    if candidate_indices.len() != 2 {
        return None;
    }
    let mut open_idx = None;
    let mut triggered_idx = None;
    for idx in candidate_indices {
        let status = statuses[*idx].status.as_str();
        if status.eq_ignore_ascii_case("open") {
            open_idx = Some(*idx);
        } else if status.eq_ignore_ascii_case("triggered") {
            triggered_idx = Some(*idx);
        }
    }
    match (open_idx, triggered_idx) {
        (Some(open_idx), Some(triggered_idx)) => Some((open_idx, vec![open_idx, triggered_idx])),
        _ => None,
    }
}

fn push_hft_status_row(columns: &mut StatusFullColumns, row: HftStatusArchiveRow) {
    columns.block_number.push(row.block_number as i64);
    columns.block_time.push(byte_array_from_string(row.block_time));
    columns.status.push(byte_array_from_string(row.status));
    columns.oid.push(row.oid as i64);
    columns.side.push(byte_array_from_string(row.side));
    columns.limit_px.push(row.limit_px);
    columns.is_trigger.push(row.is_trigger);
    columns.tif.push(byte_array_from_string(row.tif));
    columns.user.push(byte_array_from_string(row.user));
    columns.hash.push(byte_array_from_str(""));
    columns.order_type.push(byte_array_from_string(row.order_type));
    columns.sz.push(row.sz);
    columns.orig_sz.push(row.orig_sz);
    columns.time.push(byte_array_from_str(""));
    columns.builder.push(byte_array_from_str(""));
    columns.timestamp.push(0);
    columns.trigger_condition.push(byte_array_from_string(row.trigger_condition));
    columns.trigger_px.push(row.trigger_px);
    columns.tp_trigger_px.push(row.tp_trigger_px);
    columns.sl_trigger_px.push(row.sl_trigger_px);
    columns.is_position_tpsl.push(row.is_position_tpsl);
    columns.reduce_only.push(row.reduce_only);
    columns.cloid.push(byte_array_from_str(""));
}

fn migrate_hft_status_into_diffs(
    statuses: Vec<HftStatusArchiveRow>,
    diff_rows: &mut HashMap<String, Vec<DiffOut>>,
) -> (Vec<HftStatusArchiveRow>, Vec<HftStatusMigrationWarning>) {
    let mut new_candidates: HashMap<(String, String, u64), Vec<usize>> = HashMap::new();
    let mut remove_candidates: HashMap<(String, String, u64), Vec<usize>> = HashMap::new();
    for (idx, status) in statuses.iter().enumerate() {
        let key = (status.coin.clone(), status.user.clone(), status.oid);
        if hft_status_matches_new(&status.status) {
            new_candidates.entry(key.clone()).or_default().push(idx);
        }
        if hft_status_matches_remove(&status.status) {
            remove_candidates.entry(key).or_default().push(idx);
        }
    }

    let mut migrated = vec![false; statuses.len()];
    let mut warnings = Vec::new();
    for (coin, rows) in diff_rows.iter_mut() {
        for row in rows.iter_mut() {
            let key = (coin.clone(), row.user.clone(), row.oid);
            let (candidate_indices, diff_type) = match row.diff_type.as_str() {
                "new" => (new_candidates.get(&key), "new"),
                "remove" => (remove_candidates.get(&key), "remove"),
                _ => (None, ""),
            };
            let Some(candidate_indices) = candidate_indices else {
                continue;
            };
            if diff_type == "new" {
                if let Some((selected_idx, consumed_indices)) =
                    resolve_hft_new_candidate_indices(candidate_indices, &statuses)
                {
                    if consumed_indices.iter().all(|idx| !migrated[*idx]) {
                        apply_hft_status_to_diff_row(row, &statuses[selected_idx]);
                        for idx in consumed_indices {
                            migrated[idx] = true;
                        }
                        continue;
                    }
                }
            }
            match candidate_indices.as_slice() {
                [idx] if !migrated[*idx] => {
                    apply_hft_status_to_diff_row(row, &statuses[*idx]);
                    migrated[*idx] = true;
                }
                [idx] => warnings.push(HftStatusMigrationWarning {
                    block_number: statuses[*idx].block_number,
                    coin: coin.clone(),
                    user: row.user.clone(),
                    oid: row.oid,
                    diff_type: diff_type.to_string(),
                    statuses: vec![statuses[*idx].status.clone()],
                }),
                indices => warnings.push(HftStatusMigrationWarning {
                    block_number: indices.first().map(|idx| statuses[*idx].block_number).unwrap_or(row.block_number),
                    coin: coin.clone(),
                    user: row.user.clone(),
                    oid: row.oid,
                    diff_type: diff_type.to_string(),
                    statuses: indices.iter().map(|idx| statuses[*idx].status.clone()).collect(),
                }),
            }
        }
    }

    let residual =
        statuses.into_iter().enumerate().filter_map(|(idx, status)| (!migrated[idx]).then_some(status)).collect();
    (residual, warnings)
}

fn assign_hft_trade_filltimes(
    trades: &mut [FillOut],
    block_time_ms: Option<i64>,
    same_block_filled_timestamp_ms_by_oid: &HashMap<(String, u64), i64>,
) {
    for trade in trades.iter_mut() {
        trade.filltime = None;
    }

    let Some(block_time_ms) = block_time_ms else {
        return;
    };

    let mut last_trade_idx_by_maker_oid: HashMap<(String, u64), usize> = HashMap::new();
    for (idx, trade) in trades.iter().enumerate() {
        last_trade_idx_by_maker_oid.insert((trade.coin.clone(), trade.oid_m), idx);
    }

    for ((coin, oid), idx) in last_trade_idx_by_maker_oid {
        let Some(order_timestamp_ms) = same_block_filled_timestamp_ms_by_oid.get(&(coin, oid)).copied() else {
            continue;
        };
        trades[idx].filltime = block_time_ms.checked_sub(order_timestamp_ms).and_then(encode_hft_lifetime);
    }
}

fn classify_hft_remove_from_status_summary(
    summary: Option<HftRemoveStatusSummary>,
) -> (&'static str, Option<i64>, bool) {
    match summary {
        Some(summary) if summary.has_filled => ("fill", None, false),
        Some(summary) if summary.canceled_sz.is_some() => ("cancel", summary.canceled_sz, false),
        _ => ("cancel", None, true),
    }
}

fn user_fee_feature_cache() -> &'static Mutex<UserFeeFeatureCache> {
    USER_FEE_FEATURE_CACHE.get_or_init(|| Mutex::new(UserFeeFeatureCache::default()))
}

fn local_info_client() -> &'static Client {
    LOCAL_INFO_CLIENT
        .get_or_init(|| Client::builder().timeout(Duration::from_secs(2)).build().expect("build local info client"))
}

fn parse_ratio_percent_scaled_2(numerator: &str, denominator: &str) -> Option<i64> {
    let numerator = numerator.parse::<f64>().ok()?;
    let denominator = denominator.parse::<f64>().ok()?;
    if !numerator.is_finite() || !denominator.is_finite() || denominator <= 0.0 {
        return None;
    }
    let scaled = (numerator * 10_000.0 / denominator).round();
    if !scaled.is_finite() {
        return None;
    }
    i64::try_from(scaled as i128).ok()
}

fn user_add_rate_to_mm_rate(user_add_rate: &str) -> Option<i32> {
    let scaled = parse_scaled(user_add_rate, 8)?;
    if scaled >= 0 || scaled % 1_000 != 0 {
        return None;
    }
    let tier = scaled / 1_000;
    i32::try_from(tier).ok()
}

fn fetch_user_fee_features(user: &str) -> Option<UserFeeFeatures> {
    let payload = json!({
        "type": "userFees",
        "user": user,
    });
    let response: UserFeesResponse = local_info_client()
        .post(LOCAL_INFO_URL)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .ok()?;
    let mm_rate = user_add_rate_to_mm_rate(&response.user_add_rate)?;
    let recent = response.daily_user_vlm.iter().rev().take(7).collect::<Vec<_>>();
    if recent.is_empty() {
        return None;
    }
    let mut sum = 0i64;
    let mut count = 0usize;
    for day in recent {
        let share = parse_ratio_percent_scaled_2(&day.user_add, &day.exchange)?;
        sum = sum.checked_add(share)?;
        count += 1;
    }
    let mm_share = i64::try_from((sum as i128).checked_div(count as i128)?).ok()?;
    Some(UserFeeFeatures { mm_rate, mm_share })
}

fn cached_user_fee_features(user: &str) -> Option<UserFeeFeatures> {
    let today = OffsetDateTime::now_utc().date();
    {
        let mut cache = user_fee_feature_cache().lock().expect("user fee feature cache");
        if cache.day != Some(today) {
            cache.day = Some(today);
            cache.by_user.clear();
            cache.fifo.clear();
        }
        if let Some(features) = cache.by_user.get(user) {
            return features.clone();
        }
    }

    let fetched = fetch_user_fee_features(user);

    let mut cache = user_fee_feature_cache().lock().expect("user fee feature cache");
    if cache.day != Some(today) {
        cache.day = Some(today);
        cache.by_user.clear();
        cache.fifo.clear();
    }
    if !cache.by_user.contains_key(user) {
        if cache.by_user.len() >= USER_FEE_FEATURE_CACHE_CAP {
            if let Some(oldest) = cache.fifo.pop_front() {
                cache.by_user.remove(&oldest);
            }
        }
        cache.fifo.push_back(user.to_string());
    }
    cache.by_user.insert(user.to_string(), fetched.clone());
    fetched
}

fn enrich_hft_trade_rows_with_user_fees(rows: &mut [FillOut]) {
    let maker_users: HashSet<String> =
        rows.iter().filter(|row| row.fee_m < 0 && !row.address_m.is_empty()).map(|row| row.address_m.clone()).collect();
    if maker_users.is_empty() {
        return;
    }

    let mut features_by_user = HashMap::with_capacity(maker_users.len());
    for user in maker_users {
        features_by_user.insert(user.clone(), cached_user_fee_features(&user));
    }

    for row in rows {
        if row.fee_m >= 0 {
            continue;
        }
        if let Some(Some(features)) = features_by_user.get(&row.address_m) {
            row.mm_rate = Some(features.mm_rate);
            row.mm_share = Some(features.mm_share);
        }
    }
}

#[derive(Clone, Debug)]
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

struct SharedBlocksIndex {
    base_dir: Option<PathBuf>,
    current_window_start: Option<u64>,
    current_window_end: Option<u64>,
    current_name_start: Option<u64>,
    current_rows: Vec<BlockIndexOut>,
    startup_recovery_prepared: bool,
}

impl SharedBlocksIndex {
    fn new() -> Self {
        Self {
            base_dir: None,
            current_window_start: None,
            current_window_end: None,
            current_name_start: None,
            current_rows: Vec::new(),
            startup_recovery_prepared: false,
        }
    }

    fn set_base_dir(&mut self, base_dir: &Path) {
        if self.base_dir.is_none() {
            self.base_dir = Some(base_dir.to_path_buf());
        }
    }

    fn prepare_for_height(
        &mut self,
        observed_height: u64,
        handoff_tx: &Sender<HandoffMessage>,
    ) -> parquet::errors::Result<()> {
        if self.startup_recovery_prepared {
            return Ok(());
        }
        self.startup_recovery_prepared = true;
        let base_dir = current_archive_shared_blocks_base_dir();
        self.set_base_dir(&base_dir);
        preflight_local_recovery_discontinuity::<BlockIndexOut>(
            StreamKind::Blocks,
            "",
            ArchiveMode::Lite,
            observed_height,
            handoff_tx,
            true,
        )?;
        let (_, window_end) = rotation_bounds_for(StreamKind::Blocks, observed_height);
        let filename_prefix = local_recovery_filename_prefix(StreamKind::Blocks, ArchiveMode::Lite, "");
        let Some((path, name_start_block)) = find_local_recovery_path(&base_dir, &filename_prefix, window_end)? else {
            return Ok(());
        };
        let rows = read_local_blocks_rows(&path)?;
        if rows.is_empty() {
            let _unused = fs::remove_file(path);
            return Ok(());
        }
        if let Err(err) = fs::remove_file(&path) {
            warn!("Failed to remove stale shared blocks recovery file {} after loading: {err}", path.display());
        }
        self.current_window_start = Some(rotation_bounds_for(StreamKind::Blocks, observed_height).0);
        self.current_window_end = Some(window_end);
        self.current_name_start = Some(name_start_block);
        self.current_rows = rows;
        info!(
            "Recovered shared blocks locally window_end={} last_block={} and removed recovery file",
            window_end,
            self.current_rows.last().map(|row| row.block_number).unwrap_or(0)
        );
        Ok(())
    }

    fn flush_current_window_to_local(&mut self) -> parquet::errors::Result<()> {
        let Some(base_dir) = self.base_dir.clone() else {
            return Ok(());
        };
        let (Some(name_start), Some(window_end)) = (self.current_name_start, self.current_window_end) else {
            return Ok(());
        };
        if self.current_rows.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&base_dir).map_err(io_to_parquet_error)?;
        let final_path = base_dir.join(format!("blocks_{}_{}{}", name_start, window_end, LOCAL_RECOVERY_FILE_SUFFIX));
        let tmp_path = final_path.with_file_name(format!(
            "{}{}",
            final_path.file_name().and_then(|name| name.to_str()).unwrap_or("blocks.parquet.0"),
            TEMP_FILE_SUFFIX
        ));
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(io_to_parquet_error)?;
        }
        let file = fs::File::create(&tmp_path).map_err(io_to_parquet_error)?;
        let mut writer = SerializedFileWriter::new(file, blocks_schema(), archive_writer_props())?;
        write_block_rows(&mut writer, ArchiveMode::Lite, &self.current_rows)?;
        writer.close()?;
        fs::rename(tmp_path, final_path).map_err(io_to_parquet_error)?;
        info!(
            "Shared blocks persisted locally for recovery span={} window_end={}",
            self.current_rows
                .last()
                .map(|row| row.block_number.saturating_sub(name_start).saturating_add(1))
                .unwrap_or(0),
            window_end
        );
        Ok(())
    }

    fn rotate_if_needed(&mut self, height: u64) -> parquet::errors::Result<()> {
        let (window_start, window_end) = rotation_bounds_for(StreamKind::Blocks, height);
        if self.current_window_end.is_some_and(|end| end == window_end) {
            return Ok(());
        }
        if self.current_window_end.is_some() {
            self.flush_current_window_to_local()?;
            self.current_rows.clear();
        }
        self.current_window_start = Some(window_start);
        self.current_window_end = Some(window_end);
        self.current_name_start = Some(height);
        Ok(())
    }

    fn append(&mut self, row: BlockIndexOut, handoff_tx: &Sender<HandoffMessage>) -> parquet::errors::Result<()> {
        self.prepare_for_height(row.block_number, handoff_tx)?;
        self.rotate_if_needed(row.block_number)?;
        if self.current_rows.last().is_some_and(|last| last.block_number >= row.block_number) {
            return Ok(());
        }
        self.current_name_start.get_or_insert(row.block_number);
        self.current_rows.push(row);
        Ok(())
    }

    fn persist_for_recovery(&mut self) -> parquet::errors::Result<()> {
        self.flush_current_window_to_local()
    }

    fn reset_runtime_state(&mut self) {
        self.current_window_start = None;
        self.current_window_end = None;
        self.current_name_start = None;
        self.current_rows.clear();
        self.startup_recovery_prepared = false;
    }

    fn collect_rows(&self, start_block: u64, end_block: u64) -> parquet::errors::Result<Vec<BlockIndexOut>> {
        let mut rows = Vec::new();
        let Some(base_dir) = self.base_dir.as_ref() else {
            return Ok(rows);
        };
        if let Ok(entries) = fs::read_dir(base_dir) {
            let mut paths = Vec::new();
            for entry in entries {
                let entry = entry.map_err(io_to_parquet_error)?;
                let path = entry.path();
                if !entry.file_type().map_err(io_to_parquet_error)?.is_file() {
                    continue;
                }
                let Some((file_start, file_end)) = parse_local_archive_filename_bounds(&path, "blocks") else {
                    continue;
                };
                if file_end < start_block || file_start > end_block {
                    continue;
                }
                let is_current =
                    self.current_window_end == Some(file_end) && self.current_name_start == Some(file_start);
                if is_current {
                    continue;
                }
                paths.push((file_start, path));
            }
            paths.sort_by_key(|(file_start, _)| *file_start);
            for (_, path) in paths {
                rows.extend(
                    read_local_blocks_rows(&path)?
                        .into_iter()
                        .filter(|row| row.block_number >= start_block && row.block_number <= end_block),
                );
            }
        }
        rows.extend(
            self.current_rows
                .iter()
                .filter(|row| row.block_number >= start_block && row.block_number <= end_block)
                .cloned(),
        );
        Ok(rows)
    }
}

#[derive(Clone, Copy)]
enum StreamKind {
    Blocks,
    Status,
    StatusReject,
    Diff,
    Fill,
}

impl StreamKind {
    fn name(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Status => "status",
            Self::StatusReject => "reject",
            Self::Diff => "diff",
            Self::Fill => "fill",
        }
    }

    fn row_group_block_limit(self) -> Option<u64> {
        match self {
            Self::Status => Some(STATUS_ROW_GROUP_BLOCKS_DEFAULT),
            Self::StatusReject => Some(REJECT_ROW_GROUP_BLOCKS),
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
            Self::Status | Self::StatusReject | Self::Diff => current_rotation_blocks(),
        }
    }

    fn delayed_flush_lookahead_blocks(self) -> Option<u64> {
        match self {
            Self::Diff => Some(DIFF_DELAYED_FLUSH_LOOKAHEAD_BLOCKS),
            Self::Blocks | Self::Fill => Some(BLOCKS_FILL_DELAYED_FLUSH_LOOKAHEAD_BLOCKS),
            Self::Status | Self::StatusReject => None,
        }
    }

    fn supports_local_recovery(self) -> bool {
        matches!(self, Self::Blocks | Self::Fill)
    }
}

fn archive_stream_name(stream: StreamKind, mode: ArchiveMode) -> &'static str {
    match (stream, mode) {
        (StreamKind::Fill, ArchiveMode::Hft) => "trade",
        _ => stream.name(),
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

fn status_live_row_group_blocks() -> u64 {
    STATUS_ROW_GROUP_BLOCKS_DEFAULT
}

fn is_direct_reject_status(status: &str) -> bool {
    status.ends_with("Rejected")
}

fn archive_coin_dir(mode: ArchiveMode, coin: &str) -> PathBuf {
    match mode {
        ArchiveMode::Lite => PathBuf::from(coin),
        ArchiveMode::Full => PathBuf::from(format!("{coin}_full")),
        ArchiveMode::Hft => PathBuf::from(format!("{coin}_hft")),
    }
}

fn archive_mode_filename_suffix(mode: ArchiveMode) -> &'static str {
    match mode {
        ArchiveMode::Lite => "",
        ArchiveMode::Full => "_FULL",
        ArchiveMode::Hft => "_HFT",
    }
}

fn archive_filename_prefix(stream: StreamKind, mode: ArchiveMode, coin: &str) -> String {
    let suffix = archive_mode_filename_suffix(mode);
    match stream {
        StreamKind::Blocks => format!("blocks{suffix}"),
        StreamKind::StatusReject => format!("reject{suffix}"),
        StreamKind::Fill | StreamKind::Diff | StreamKind::Status => {
            format!("{}_{}{}", coin, archive_stream_name(stream, mode), suffix)
        }
    }
}

fn archive_relative_dir(stream: StreamKind, mode: ArchiveMode, coin: &str) -> PathBuf {
    match stream {
        StreamKind::Blocks => PathBuf::new(),
        StreamKind::StatusReject => PathBuf::from("reject"),
        StreamKind::Fill | StreamKind::Diff | StreamKind::Status => archive_coin_dir(mode, coin),
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
    fn read_local_rows(path: &Path, mode: ArchiveMode) -> parquet::errors::Result<Vec<Self>> {
        read_local_diff_rows(path, mode)
    }

    fn write_rows(
        file: &mut SerializedFileWriter<std::fs::File>,
        mode: ArchiveMode,
        rows: &[Self],
    ) -> parquet::errors::Result<()> {
        write_diff_rows(file, mode, rows)
    }
}

impl RecoverableArchiveRow for RejectOut {
    fn read_local_rows(_path: &Path, _mode: ArchiveMode) -> parquet::errors::Result<Vec<Self>> {
        Err(io_to_parquet_error(io_other("reject rows do not support local parquet recovery")))
    }

    fn write_rows(
        file: &mut SerializedFileWriter<std::fs::File>,
        _mode: ArchiveMode,
        rows: &[Self],
    ) -> parquet::errors::Result<()> {
        write_reject_rows(file, rows)
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

    fn replace_rows(&mut self, rows: Vec<R>) {
        self.rows = rows;
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
    handoff_config: ArchiveHandoffConfig,
    local_recovery_enabled: bool,
    recover_blocks_fill_on_stop: bool,
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
        handoff_config: ArchiveHandoffConfig,
    ) -> Self {
        Self {
            stream,
            schema,
            props,
            handoff_tx,
            handoff_config,
            local_recovery_enabled: stream.supports_local_recovery(),
            recover_blocks_fill_on_stop: false,
            file: None,
            pending_start_block: None,
            active: PendingRowGroup::new(),
            delayed: PendingRowGroup::new(),
        }
    }

    fn ensure_open(&mut self, coin: &str, mode: ArchiveMode, block: u64) -> parquet::errors::Result<()> {
        if self.file.is_none() {
            let (window_start, end) = rotation_bounds_for(self.stream, block);
            let logical_start = self.pending_start_block.unwrap_or(block);
            self.open_file(coin, mode, logical_start, logical_start, window_start, end)?;
        }
        Ok(())
    }

    fn supports_local_recovery(&self) -> bool {
        self.local_recovery_enabled
    }

    fn keep_local_on_close(&self) -> bool {
        self.supports_local_recovery() && self.recover_blocks_fill_on_stop
    }

    fn set_local_recovery_enabled(&mut self, enabled: bool) {
        self.local_recovery_enabled = enabled;
    }

    fn set_recover_blocks_fill_on_stop(&mut self, enabled: bool) {
        self.recover_blocks_fill_on_stop = enabled;
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
        let filename_prefix = local_recovery_filename_prefix(self.stream, mode, coin);
        let filename_suffix = FINAL_PARQUET_FILE_SUFFIX.to_string();
        fs::create_dir_all(&base_dir).map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        let mut effective_name_start = name_start;
        let mut local_final_path = base_dir.join(format!(
            "{filename_prefix}_{name_start}_{end}{}",
            if self.supports_local_recovery() { LOCAL_RECOVERY_FILE_SUFFIX } else { FINAL_PARQUET_FILE_SUFFIX }
        ));
        let recovery = self.load_local_recovery(&base_dir, coin, mode, end, &filename_prefix, &filename_suffix)?;
        if let Some(existing) = recovery.as_ref() {
            let resume_end_block = local_recovery_resume_end(self.stream, actual_start, end, existing.last_block);
            if actual_start > resume_end_block.saturating_add(1) {
                let staged_path = stage_local_file_for_handoff(&existing.path)?;
                let logical_end_block = actual_start.saturating_sub(1).min(end);
                let relative_path = self.local_recovery_relative_path(
                    coin,
                    mode,
                    existing.name_start_block,
                    logical_end_block,
                    &filename_prefix,
                    &filename_suffix,
                );
                enqueue_handoff_task(&self.handoff_tx, &self.handoff_config, staged_path, relative_path)?;
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
            let resume_end_block = local_recovery_resume_end(self.stream, actual_start, end, existing.last_block);
            if actual_start <= resume_end_block.saturating_add(1) {
                let resume = split_recovered_rows_for_resume(self.stream, actual_start, existing.rows);
                rewrite_rows_into_writer(&mut writer, mode, self.stream, resume.committed_rows, R::write_rows)?;
                self.delayed.replace_rows(resume.delayed_rows);
                self.active.replace_rows(resume.active_rows);
                recovered_last_block = resume_end_block;
                replay_skip_until_block = resume_end_block;
                // Delete the old recovery file now that its contents have been loaded into memory.
                if let Err(err) = fs::remove_file(&existing.path) {
                    warn!("Failed to remove old recovery file {} after loading: {err}", existing.path.display());
                }
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
            let log_label = file.final_filename_prefix.clone();
            let relative_path = file.relative_dir.join(format!(
                "{}_{}_{}{}",
                file.final_filename_prefix, file.name_start_block, file.name_end_block, file.final_filename_suffix
            ));
            file.writer.close()?;
            fs::rename(&tmp_path, &final_path).map_err(io_to_parquet_error)?;
            let span_blocks = file.name_end_block.saturating_sub(file.name_start_block) + 1;
            if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
                info!("Archive finalized but dropping short parquet {} span={}", log_label, span_blocks);
                fs::remove_file(&final_path).map_err(io_to_parquet_error)?;
            } else if self.keep_local_on_close() && file.name_end_block < file.window_end_block {
                info!("Archive finalized locally without handoff for {} span={}", log_label, span_blocks);
            } else {
                enqueue_handoff_task(&self.handoff_tx, &self.handoff_config, final_path, relative_path)?;
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
    stream: StreamKind,
    row_group_blocks: u64,
    schema: std::sync::Arc<Type>,
    props: std::sync::Arc<WriterProperties>,
    handoff_tx: Sender<HandoffMessage>,
    handoff_config: ArchiveHandoffConfig,
    local_recovery_enabled: bool,
    recover_blocks_fill_on_stop: bool,
    file: Option<ParquetFile>,
    pending_start_block: Option<u64>,
    active_start_block: Option<u64>,
    active_end_block: Option<u64>,
    active: StatusBlockBatch,
    delayed_start_block: Option<u64>,
    delayed_end_block: Option<u64>,
    delayed: StatusBlockBatch,
    flushed_row_groups: u64,
}

impl StatusParquetWriter {
    fn new(
        coin: String,
        mode: ArchiveMode,
        stream: StreamKind,
        row_group_blocks: u64,
        schema: std::sync::Arc<Type>,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
        handoff_config: ArchiveHandoffConfig,
    ) -> Self {
        Self {
            coin,
            mode,
            stream,
            row_group_blocks,
            schema,
            props,
            handoff_tx,
            handoff_config,
            local_recovery_enabled: false,
            recover_blocks_fill_on_stop: false,
            file: None,
            pending_start_block: None,
            active_start_block: None,
            active_end_block: None,
            active: StatusBlockBatch::new(mode),
            delayed_start_block: None,
            delayed_end_block: None,
            delayed: StatusBlockBatch::new(mode),
            flushed_row_groups: 0,
        }
    }

    fn supports_local_recovery(&self) -> bool {
        self.local_recovery_enabled
    }

    fn keep_local_on_close(&self) -> bool {
        self.supports_local_recovery() && self.recover_blocks_fill_on_stop
    }

    fn set_local_recovery_enabled(&mut self, enabled: bool) {
        self.local_recovery_enabled = enabled;
    }

    fn set_recover_blocks_fill_on_stop(&mut self, enabled: bool) {
        self.recover_blocks_fill_on_stop = enabled;
    }

    fn uses_delayed_flush(&self) -> bool {
        matches!(self.stream, StreamKind::Status)
    }

    fn maybe_trim_allocator(&mut self) {
        if !self.uses_delayed_flush() {
            trim_allocator();
            return;
        }
        self.flushed_row_groups = self.flushed_row_groups.saturating_add(1);
        if self.flushed_row_groups % STATUS_LIVE_TRIM_EVERY_ROW_GROUPS == 0 {
            trim_allocator();
        }
    }

    fn ensure_open(&mut self, coin: &str, block: u64) -> parquet::errors::Result<()> {
        if self.file.is_none() {
            let (window_start, end) = rotation_bounds_for(self.stream, block);
            let logical_start = self.pending_start_block.unwrap_or(block);
            self.open_file(coin, logical_start, logical_start, window_start, end)?;
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
        let relative_dir = archive_relative_dir(self.stream, self.mode, coin);
        let filename_prefix = archive_filename_prefix(self.stream, self.mode, coin);
        let filename_suffix = FINAL_PARQUET_FILE_SUFFIX.to_string();
        fs::create_dir_all(&base_dir).map_err(io_to_parquet_error)?;
        let mut effective_name_start = name_start;
        let mut recovered_path: Option<PathBuf> = None;
        let mut local_final_path = base_dir.join(format!(
            "{filename_prefix}_{name_start}_{end}{}",
            if self.supports_local_recovery() { LOCAL_RECOVERY_FILE_SUFFIX } else { FINAL_PARQUET_FILE_SUFFIX }
        ));
        if self.supports_local_recovery() {
            if let Some((path, recovery_name_start)) = find_local_recovery_path(&base_dir, &filename_prefix, end)? {
                let rows = read_local_hft_status_rows(&path)?;
                let last_block = rows.last().map(HasBlockNumber::block_number).unwrap_or(0);
                if last_block == 0 {
                    fs::remove_file(&path).map_err(io_to_parquet_error)?;
                } else {
                    let resume_end_block = local_recovery_resume_end(self.stream, actual_start, end, last_block);
                    if actual_start > resume_end_block.saturating_add(1) {
                        let logical_end_block = local_recovery_logical_end(self.stream, actual_start, end);
                        let span_blocks = logical_end_block.saturating_sub(recovery_name_start).saturating_add(1);
                        if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
                            fs::remove_file(&path).map_err(io_to_parquet_error)?;
                        } else {
                            let staged_path = stage_local_file_for_handoff(&path)?;
                            let relative_path = self.relative_recovery_handoff_path(
                                recovery_name_start,
                                logical_end_block,
                                &filename_prefix,
                                &filename_suffix,
                            );
                            enqueue_handoff_task(&self.handoff_tx, &self.handoff_config, staged_path, relative_path)?;
                        }
                    } else {
                        effective_name_start = recovery_name_start;
                        recovered_path = Some(path.clone());
                        local_final_path = path.clone();
                    }
                }
            }
        }
        let tmp_path = local_final_path.with_file_name(format!(
            "{}{}",
            local_final_path.file_name().and_then(|name| name.to_str()).unwrap_or("archive.parquet"),
            TEMP_FILE_SUFFIX
        ));
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(io_to_parquet_error)?;
        }
        let file = fs::File::create(&tmp_path).map_err(io_to_parquet_error)?;
        let mut writer = SerializedFileWriter::new(file, self.schema.clone(), self.props.clone())?;
        let mut recovered_last_block = actual_start;
        let mut replay_skip_until_block = 0u64;
        if let Some(recovered_path) = recovered_path.as_ref() {
            let rows = read_local_hft_status_rows(recovered_path)?;
            let last_block = rows.last().map(HasBlockNumber::block_number).unwrap_or(0);
            if last_block > 0 {
                let resume_end_block = local_recovery_resume_end(self.stream, actual_start, end, last_block);
                if actual_start <= resume_end_block.saturating_add(1) {
                    let resume = split_status_recovered_rows_for_resume(actual_start, rows, self.row_group_blocks);
                    let committed = hft_status_rows_to_batch(resume.committed_rows);
                    if let StatusBlockBatch::Hft(columns) = committed {
                        write_status_hft_columns(&mut writer, coin, columns)?;
                    }
                    self.delayed = hft_status_rows_to_batch(resume.delayed_rows);
                    self.active = hft_status_rows_to_batch(resume.active_rows);
                    self.delayed_start_block = status_block_batch_start(&self.delayed);
                    self.delayed_end_block = status_block_batch_end(&self.delayed);
                    self.active_start_block = status_block_batch_start(&self.active);
                    self.active_end_block = status_block_batch_end(&self.active);
                    recovered_last_block = resume_end_block;
                    replay_skip_until_block = resume_end_block;
                    if let Err(err) = fs::remove_file(recovered_path) {
                        warn!("Failed to remove old recovery file {} after loading: {err}", recovered_path.display());
                    }
                    info!(
                        "Recovered local {} parquet for {} window_end={} last_block={} and resumed writer",
                        self.stream.name(),
                        coin,
                        end,
                        recovered_last_block
                    );
                }
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

    fn relative_recovery_handoff_path(
        &self,
        name_start_block: u64,
        name_end_block: u64,
        filename_prefix: &str,
        filename_suffix: &str,
    ) -> PathBuf {
        archive_relative_dir(self.stream, self.mode, &self.coin)
            .join(format!("{}_{}_{}{}", filename_prefix, name_start_block, name_end_block, filename_suffix))
    }

    fn flush_batch(&mut self, batch: StatusBlockBatch) -> parquet::errors::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().ok_or_else(|| io_to_parquet_error(io_other("status writer missing file")))?;
        match batch {
            StatusBlockBatch::Lite(columns) => write_status_lite_columns(&mut file.writer, &self.coin, columns)?,
            StatusBlockBatch::Full(columns) => write_status_full_columns(&mut file.writer, &self.coin, columns)?,
            StatusBlockBatch::Hft(columns) => write_status_hft_columns(&mut file.writer, &self.coin, columns)?,
        }
        self.maybe_trim_allocator();
        Ok(())
    }

    fn flush_active(&mut self) -> parquet::errors::Result<()> {
        if self.active.is_empty() {
            self.active_start_block = None;
            self.active_end_block = None;
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.active, StatusBlockBatch::new(self.mode));
        self.flush_batch(batch)?;
        self.active_start_block = None;
        self.active_end_block = None;
        Ok(())
    }

    fn flush_delayed(&mut self) -> parquet::errors::Result<()> {
        if self.delayed.is_empty() {
            self.delayed_start_block = None;
            self.delayed_end_block = None;
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.delayed, StatusBlockBatch::new(self.mode));
        self.flush_batch(batch)?;
        self.delayed_start_block = None;
        self.delayed_end_block = None;
        Ok(())
    }

    fn advance_block(&mut self, block: u64) -> parquet::errors::Result<()> {
        if self.file.as_ref().is_some_and(|file| {
            self.supports_local_recovery() && file.replay_skip_until_block > 0 && block <= file.replay_skip_until_block
        }) {
            return Ok(());
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
            self.close_with_flush()?;
            self.pending_start_block = Some(block);
        }
        if let Some(file) = self.file.as_mut() {
            file.last_block = block;
            file.name_end_block = block;
        }
        if self.active_end_block.is_none() {
            let (start, end) = aligned_row_group_bounds(block, self.row_group_blocks);
            self.active_start_block = Some(start);
            self.active_end_block = Some(end);
        }
        while self.active_end_block.is_some_and(|end| block > end) {
            if self.uses_delayed_flush() {
                self.flush_delayed()?;
                self.delayed = std::mem::replace(&mut self.active, StatusBlockBatch::new(self.mode));
                self.delayed_start_block = self.active_start_block.take();
                self.delayed_end_block = self.active_end_block.take();
            } else {
                self.flush_active()?;
            }
            let (start, end) = aligned_row_group_bounds(block, self.row_group_blocks);
            self.active_start_block = Some(start);
            self.active_end_block = Some(end);
        }
        if self.uses_delayed_flush() {
            if let Some(active_start) = self.active_start_block {
                if !self.delayed.is_empty()
                    && block.saturating_sub(active_start).saturating_add(1) > STATUS_LIVE_DELAYED_FLUSH_LOOKAHEAD_BLOCKS
                {
                    self.flush_delayed()?;
                }
            }
        }
        Ok(())
    }

    fn append_block(&mut self, coin: &str, block: u64, rows: StatusBlockBatch) -> parquet::errors::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.ensure_open(coin, block)?;
        if self.file.as_ref().is_some_and(|file| {
            self.supports_local_recovery() && file.replay_skip_until_block > 0 && block <= file.replay_skip_until_block
        }) {
            return Ok(());
        }
        if let Some(file) = self.file.as_mut() {
            file.last_block = block;
            file.name_end_block = block;
        }
        if self.active_end_block.is_none() {
            let (start, end) = aligned_row_group_bounds(block, self.row_group_blocks);
            self.active_start_block = Some(start);
            self.active_end_block = Some(end);
        }
        match (&mut self.active, rows) {
            (StatusBlockBatch::Lite(active), StatusBlockBatch::Lite(batch)) => active.append(batch),
            (StatusBlockBatch::Full(active), StatusBlockBatch::Full(batch)) => active.append(batch),
            (StatusBlockBatch::Hft(active), StatusBlockBatch::Hft(batch)) => active.append(batch),
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
                        self.delayed.trim_to_block(aligned_end);
                        file.name_end_block = aligned_end;
                    }
                    _ => {
                        self.active = StatusBlockBatch::new(self.mode);
                        self.delayed = StatusBlockBatch::new(self.mode);
                        self.active_start_block = None;
                        self.active_end_block = None;
                        self.delayed_start_block = None;
                        self.delayed_end_block = None;
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
        self.flush_delayed()?;
        self.flush_active()?;
        if let Some(file) = self.file.take() {
            let tmp_path = file.tmp_path.clone();
            let final_path = file.local_final_path.clone();
            let log_label = file.final_filename_prefix.clone();
            let relative_path = file.relative_dir.join(format!(
                "{}_{}_{}{}",
                file.final_filename_prefix, file.name_start_block, file.name_end_block, file.final_filename_suffix
            ));
            file.writer.close()?;
            fs::rename(&tmp_path, &final_path).map_err(io_to_parquet_error)?;
            let span_blocks = file.name_end_block.saturating_sub(file.name_start_block) + 1;
            if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
                info!("Archive finalized but dropping short parquet {} span={}", log_label, span_blocks);
                fs::remove_file(&final_path).map_err(io_to_parquet_error)?;
            } else if self.keep_local_on_close() && file.name_end_block < file.window_end_block {
                info!("Archive finalized locally without handoff for {} span={}", log_label, span_blocks);
            } else {
                enqueue_handoff_task(&self.handoff_tx, &self.handoff_config, final_path, relative_path)?;
            }
        }
        self.pending_start_block = None;
        Ok(())
    }

    fn abort(&mut self) {
        self.active = StatusBlockBatch::new(self.mode);
        self.delayed = StatusBlockBatch::new(self.mode);
        self.active_start_block = None;
        self.active_end_block = None;
        self.delayed_start_block = None;
        self.delayed_end_block = None;
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
    recover_blocks_fill_on_stop: Arc<AtomicBool>,
}

impl DiffWorkerHandle {
    fn new(
        mode: ArchiveMode,
        symbols: Vec<String>,
        symbol_decimals: HashMap<String, ArchiveDecimalScales>,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
        handoff_config: ArchiveHandoffConfig,
        local_recovery_enabled: bool,
    ) -> Self {
        let (tx, rx) = sync_channel(DIFF_WORKER_QUEUE_BLOCKS);
        let (error_tx, error_rx) = channel();
        let recover_blocks_fill_on_stop = Arc::new(AtomicBool::new(false));
        let worker_recover_blocks_fill_on_stop = recover_blocks_fill_on_stop.clone();
        let thread_context = archive_thread_context(|ctx| ctx.cloned());
        let handle = thread::spawn(move || {
            set_archive_thread_context(thread_context);
            let mut writers: HashMap<String, ParquetStreamWriter<DiffOut>> = symbols
                .iter()
                .map(|coin| {
                    let scales = decimal_scales_for_coin(&symbol_decimals, coin);
                    (
                        coin.clone(),
                        ParquetStreamWriter::new(
                            StreamKind::Diff,
                            diff_schema_for(mode, scales),
                            props.clone(),
                            handoff_tx.clone(),
                            handoff_config.clone(),
                        ),
                    )
                })
                .collect();
            for writer in writers.values_mut() {
                writer.set_local_recovery_enabled(local_recovery_enabled);
                writer.set_recover_blocks_fill_on_stop(worker_recover_blocks_fill_on_stop.load(Ordering::SeqCst));
            }
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
                                writer.set_recover_blocks_fill_on_stop(
                                    worker_recover_blocks_fill_on_stop.load(Ordering::SeqCst),
                                );
                            }
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
            set_archive_thread_context(None);
        });
        Self { tx, error_rx, handle: Some(handle), recover_blocks_fill_on_stop }
    }

    fn set_recover_blocks_fill_on_stop(&mut self, enabled: bool) {
        self.recover_blocks_fill_on_stop.store(enabled, Ordering::SeqCst);
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
        let mut message = DiffWorkerMessage::Block { height, rows_by_coin };
        loop {
            if current_archive_stop_requested() {
                return Ok(());
            }
            match self.tx.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    message = returned;
                    std::thread::yield_now();
                }
                Err(TrySendError::Disconnected(_returned)) => {
                    return Err(io_to_parquet_error(io_other("diff worker send failed: worker disconnected")));
                }
            }
        }
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
    recover_blocks_fill_on_stop: Arc<AtomicBool>,
}

impl StatusWorkerHandle {
    fn new(
        coin: String,
        mode: ArchiveMode,
        stream: StreamKind,
        row_group_blocks: u64,
        scales: ArchiveDecimalScales,
        props: std::sync::Arc<WriterProperties>,
        handoff_tx: Sender<HandoffMessage>,
        handoff_config: ArchiveHandoffConfig,
        local_recovery_enabled: bool,
    ) -> Self {
        let (tx, rx) = sync_channel(STATUS_WORKER_QUEUE_BLOCKS);
        let (ack_tx, ack_rx) = channel();
        let (error_tx, error_rx) = channel();
        let recover_blocks_fill_on_stop = Arc::new(AtomicBool::new(false));
        let worker_recover_blocks_fill_on_stop = recover_blocks_fill_on_stop.clone();
        let worker_coin = coin.clone();
        let thread_context = archive_thread_context(|ctx| ctx.cloned());
        let handle = thread::spawn(move || {
            set_archive_thread_context(thread_context);
            let schema = status_schema_for(mode, scales);
            let mut writer = StatusParquetWriter::new(
                worker_coin.clone(),
                mode,
                stream,
                row_group_blocks,
                schema,
                props,
                handoff_tx,
                handoff_config,
            );
            writer.set_local_recovery_enabled(local_recovery_enabled);
            writer.set_recover_blocks_fill_on_stop(worker_recover_blocks_fill_on_stop.load(Ordering::SeqCst));
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
                        writer
                            .set_recover_blocks_fill_on_stop(worker_recover_blocks_fill_on_stop.load(Ordering::SeqCst));
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
            set_archive_thread_context(None);
        });
        Self { coin, tx, ack_rx, error_rx, handle: Some(handle), recover_blocks_fill_on_stop }
    }

    fn coin(&self) -> &str {
        &self.coin
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
        let mut message = StatusWorkerMessage::Block { height, rows };
        loop {
            if current_archive_stop_requested() {
                return Ok(());
            }
            match self.tx.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    message = returned;
                    std::thread::yield_now();
                }
                Err(TrySendError::Disconnected(_returned)) => {
                    return Err(io_to_parquet_error(io_other(format!(
                        "status worker send failed for {}: worker disconnected",
                        self.coin
                    ))));
                }
            }
        }
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

    fn set_recover_blocks_fill_on_stop(&mut self, enabled: bool) {
        self.recover_blocks_fill_on_stop.store(enabled, Ordering::SeqCst);
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

fn decimal_scales_for_coin(
    symbol_decimals: &HashMap<String, ArchiveDecimalScales>,
    coin: &str,
) -> ArchiveDecimalScales {
    symbol_decimals.get(coin).copied().unwrap_or_default()
}

fn archive_writer_props() -> std::sync::Arc<WriterProperties> {
    let zstd_level = ZstdLevel::try_new(3).unwrap_or_default();
    std::sync::Arc::new(
        WriterProperties::builder().set_compression(Compression::ZSTD(zstd_level)).set_dictionary_enabled(true).build(),
    )
}

fn blocks_schema() -> std::sync::Arc<Type> {
    std::sync::Arc::new(
        parse_message_type(
            "message blocks_schema {\n\
                REQUIRED INT64 block_number;\n\
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));\n\
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
    )
}

fn reject_schema() -> std::sync::Arc<Type> {
    std::sync::Arc::new(
        parse_message_type(
            "message reject_schema {\n\
                REQUIRED INT64 block_number;\n\
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));\n\
                REQUIRED BINARY coin (UTF8);\n\
                REQUIRED BINARY user (UTF8);\n\
                REQUIRED BINARY status (UTF8);\n\
                REQUIRED BINARY side (UTF8);\n\
                REQUIRED INT64 limit_px (DECIMAL(18, 5));\n\
                REQUIRED INT64 sz (DECIMAL(18, 5));\n\
                REQUIRED BOOLEAN is_trigger;\n\
                REQUIRED BINARY tif (UTF8);\n\
                REQUIRED BINARY trigger_condition (UTF8);\n\
                REQUIRED INT64 trigger_px (DECIMAL(18, 5));\n\
                REQUIRED BOOLEAN is_position_tpsl;\n\
                REQUIRED BOOLEAN reduce_only;\n\
                REQUIRED BINARY order_type (UTF8);\n\
            }",
        )
        .expect("invalid reject schema"),
    )
}

fn status_schema_for(mode: ArchiveMode, scales: ArchiveDecimalScales) -> std::sync::Arc<Type> {
    let p = scales.px_scale;
    let s = scales.sz_scale;
    let schema = match mode {
        ArchiveMode::Lite => format!(
            "message status_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY user (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY status (UTF8);
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 limit_px (DECIMAL(18, {p}));
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED INT64 orig_sz (DECIMAL(18, {s}));
                REQUIRED BOOLEAN is_trigger;
                REQUIRED BINARY tif (UTF8);
                REQUIRED BINARY trigger_condition (UTF8);
                REQUIRED INT64 trigger_px (DECIMAL(18, {p}));
                REQUIRED BOOLEAN is_position_tpsl;
                REQUIRED BOOLEAN reduce_only;
                REQUIRED BINARY order_type (UTF8);
            }}"
        ),
        ArchiveMode::Hft => format!(
            "message status_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY user (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY status (UTF8);
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 limit_px (DECIMAL(18, {p}));
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                OPTIONAL INT64 orig_sz (DECIMAL(18, {s}));
                REQUIRED BOOLEAN is_trigger;
                REQUIRED BINARY tif (UTF8);
                REQUIRED BINARY trigger_condition (UTF8);
                REQUIRED INT64 trigger_px (DECIMAL(18, {p}));
                REQUIRED BOOLEAN is_position_tpsl;
                REQUIRED BOOLEAN reduce_only;
                REQUIRED BINARY order_type (UTF8);
                OPTIONAL INT64 tp_trigger_px (DECIMAL(18, {p}));
                OPTIONAL INT64 sl_trigger_px (DECIMAL(18, {p}));
            }}"
        ),
        ArchiveMode::Full => format!(
            "message status_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY user (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY status (UTF8);
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 limit_px (DECIMAL(18, {p}));
                REQUIRED BOOLEAN is_trigger;
                REQUIRED BINARY tif (UTF8);
                REQUIRED BINARY hash (UTF8);
                REQUIRED BINARY order_type (UTF8);
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED INT64 orig_sz (DECIMAL(18, {s}));
                REQUIRED BINARY builder (UTF8);
                REQUIRED BINARY trigger_condition (UTF8);
                REQUIRED INT64 trigger_px (DECIMAL(18, {p}));
                REQUIRED BOOLEAN is_position_tpsl;
                REQUIRED BOOLEAN reduce_only;
                OPTIONAL BINARY cloid (UTF8);
            }}"
        ),
    };
    std::sync::Arc::new(parse_message_type(&schema).expect("invalid status schema"))
}

fn diff_schema_for(mode: ArchiveMode, scales: ArchiveDecimalScales) -> std::sync::Arc<Type> {
    let p = scales.px_scale;
    let s = scales.sz_scale;
    let schema = match mode {
        ArchiveMode::Lite => format!(
            "message diff_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 px (DECIMAL(18, {p}));
                REQUIRED BINARY diff_type (UTF8);
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED INT64 orig_sz (DECIMAL(18, {s}));
            }}"
        ),
        ArchiveMode::Hft => format!(
            "message diff_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY user (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 px (DECIMAL(18, {p}));
                REQUIRED BINARY diff_type (UTF8);
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED INT64 orig_sz (DECIMAL(18, {s}));
                REQUIRED BINARY event (UTF8);
                OPTIONAL BINARY status (UTF8);
                OPTIONAL INT64 limit_px (DECIMAL(18, {p}));
                OPTIONAL INT64 _sz (DECIMAL(18, {s}));
                OPTIONAL INT64 _orig_sz (DECIMAL(18, {s}));
                OPTIONAL BOOLEAN is_trigger;
                OPTIONAL BINARY tif (UTF8);
                OPTIONAL BOOLEAN reduce_only;
                OPTIONAL BINARY order_type (UTF8);
                OPTIONAL BINARY trigger_condition (UTF8);
                OPTIONAL INT64 trigger_px (DECIMAL(18, {p}));
                OPTIONAL BOOLEAN is_position_tpsl;
                OPTIONAL INT64 tp_trigger_px (DECIMAL(18, {p}));
                OPTIONAL INT64 sl_trigger_px (DECIMAL(18, {p}));
                OPTIONAL INT32 lifetime;
            }}"
        ),
        ArchiveMode::Full => format!(
            "message diff_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY user (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY diff_type (UTF8);
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 px (DECIMAL(18, {p}));
                REQUIRED INT64 orig_sz (DECIMAL(18, {s}));
            }}"
        ),
    };
    std::sync::Arc::new(parse_message_type(&schema).expect("invalid diff schema"))
}

fn fill_schema_for(mode: ArchiveMode, scales: ArchiveDecimalScales) -> std::sync::Arc<Type> {
    let p = scales.px_scale;
    let s = scales.sz_scale;
    let schema = match mode {
        ArchiveMode::Lite => format!(
            "message fill_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 px (DECIMAL(18, {p}));
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED BOOLEAN crossed;
            }}"
        ),
        ArchiveMode::Hft => format!(
            "message fill_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY address (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 pnl (DECIMAL(18, 6));
                REQUIRED INT64 fee (DECIMAL(18, 6));
                REQUIRED INT64 px (DECIMAL(18, {p}));
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED BINARY address_m (UTF8);
                REQUIRED INT64 oid_m;
                REQUIRED INT64 pnl_m (DECIMAL(18, 6));
                REQUIRED INT64 fee_m (DECIMAL(18, 6));
                OPTIONAL INT32 mm_rate;
                OPTIONAL INT64 mm_share (DECIMAL(4, 2));
                OPTIONAL INT32 filltime;
                REQUIRED INT64 tid;
            }}"
        ),
        ArchiveMode::Full => format!(
            "message fill_schema {{
                REQUIRED INT64 block_number;
                REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));
                REQUIRED BINARY coin (UTF8);
                REQUIRED BINARY address (UTF8);
                REQUIRED INT64 oid;
                REQUIRED BINARY side (UTF8);
                REQUIRED INT64 px (DECIMAL(18, {p}));
                REQUIRED INT64 sz (DECIMAL(18, {s}));
                REQUIRED BOOLEAN crossed;
                REQUIRED INT64 closed_pnl (DECIMAL(18, 6));
                REQUIRED INT64 fee (DECIMAL(18, 6));
                REQUIRED BINARY hash (UTF8);
                REQUIRED INT64 tid;
                REQUIRED INT64 start_position (DECIMAL(18, 8));
                REQUIRED BINARY dir (UTF8);
                REQUIRED BINARY fee_token (UTF8);
                OPTIONAL INT64 twap_id;
            }}"
        ),
    };
    std::sync::Arc::new(parse_message_type(&schema).expect("invalid fill schema"))
}

struct ArchiveWriters {
    diff: DiffWorkerHandle,
    coins: HashMap<String, CoinWriters>,
    symbols: Vec<String>,
    mode: ArchiveMode,
    handoff_tx: Sender<HandoffMessage>,
    handoff_config: ArchiveHandoffConfig,
    recover_blocks_fill_on_stop: bool,
    blocks_segment_start: Option<u64>,
    blocks_last_height: Option<u64>,
    pending_status_acks: HashMap<u64, usize>,
    next_uncommitted_height: Option<u64>,
    last_archived_height: Option<u64>,
}

impl ArchiveWriters {
    fn new(
        mode: ArchiveMode,
        symbols: Vec<String>,
        symbol_decimals: HashMap<String, ArchiveDecimalScales>,
        handoff_tx: Sender<HandoffMessage>,
    ) -> Self {
        let handoff_config = current_archive_handoff_config();
        let props = archive_writer_props();

        let mut coins = HashMap::new();
        let hft_local_recovery_enabled = matches!(mode, ArchiveMode::Hft);
        let diff = DiffWorkerHandle::new(
            mode,
            symbols.clone(),
            symbol_decimals.clone(),
            props.clone(),
            handoff_tx.clone(),
            handoff_config.clone(),
            hft_local_recovery_enabled,
        );
        for coin in &symbols {
            let scales = decimal_scales_for_coin(&symbol_decimals, coin);
            let mut fill = ParquetStreamWriter::new(
                StreamKind::Fill,
                fill_schema_for(mode, scales),
                props.clone(),
                handoff_tx.clone(),
                handoff_config.clone(),
            );
            if matches!(mode, ArchiveMode::Hft) {
                fill.set_local_recovery_enabled(true);
            }
            coins.insert(
                coin.clone(),
                CoinWriters {
                    status: StatusWorkerHandle::new(
                        coin.clone(),
                        mode,
                        StreamKind::Status,
                        status_live_row_group_blocks(),
                        scales,
                        props.clone(),
                        handoff_tx.clone(),
                        handoff_config.clone(),
                        hft_local_recovery_enabled,
                    ),
                    fill,
                },
            );
        }

        Self {
            diff,
            coins,
            symbols,
            mode,
            handoff_tx,
            handoff_config,
            recover_blocks_fill_on_stop: false,
            blocks_segment_start: None,
            blocks_last_height: None,
            pending_status_acks: HashMap::new(),
            next_uncommitted_height: None,
            last_archived_height: None,
        }
    }

    fn set_recover_blocks_fill_on_stop(&mut self, enabled: bool) {
        self.recover_blocks_fill_on_stop = enabled;
        if matches!(self.mode, ArchiveMode::Hft) {
            self.diff.set_recover_blocks_fill_on_stop(enabled);
        }
        for writers in self.coins.values_mut() {
            writers.fill.set_recover_blocks_fill_on_stop(enabled);
            if matches!(self.mode, ArchiveMode::Hft) {
                writers.status.set_recover_blocks_fill_on_stop(enabled);
            }
        }
    }

    fn record_blocks_height(&mut self, height: u64) {
        self.blocks_segment_start.get_or_insert(height);
        self.blocks_last_height = Some(height);
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
        if self.recover_blocks_fill_on_stop {
            return self.close_all_parallel_for_signal_stop();
        }
        export_shared_blocks_segment(
            self.mode,
            &self.symbols,
            self.blocks_segment_start,
            self.blocks_last_height,
            &self.handoff_tx,
            &self.handoff_config,
            self.recover_blocks_fill_on_stop,
        )?;
        warn!("Archive close_all closed blocks");
        self.diff.close()?;
        warn!("Archive close_all closed diff");
        for (_, writers) in std::mem::take(&mut self.coins) {
            let mut writers = writers;
            let coin = writers.status.coin().to_string();
            writers.status.close()?;
            warn!("Archive close_all closed status coin={}", coin);
            writers.fill.close_with_flush(self.mode, write_fill_rows)?;
            warn!("Archive close_all closed fill coin={}", coin);
        }
        Ok(())
    }

    fn close_all_parallel_for_signal_stop(&mut self) -> parquet::errors::Result<()> {
        let mode = self.mode;
        let blocks_segment_start = self.blocks_segment_start;
        let blocks_last_height = self.blocks_last_height;
        let blocks_symbols = self.symbols.clone();
        let blocks_handoff_tx = self.handoff_tx.clone();
        let blocks_handoff_config = self.handoff_config.clone();
        let recover_blocks_fill_on_stop = self.recover_blocks_fill_on_stop;
        let diff = &mut self.diff;
        let coins = &mut self.coins;
        let mut first_error: Option<parquet::errors::ParquetError> = None;

        thread::scope(|scope| {
            let blocks_handle = scope.spawn(move || -> parquet::errors::Result<()> {
                export_shared_blocks_segment(
                    mode,
                    &blocks_symbols,
                    blocks_segment_start,
                    blocks_last_height,
                    &blocks_handoff_tx,
                    &blocks_handoff_config,
                    recover_blocks_fill_on_stop,
                )?;
                warn!("Archive close_all closed blocks");
                Ok(())
            });

            let diff_handle = scope.spawn(move || -> parquet::errors::Result<()> {
                diff.close()?;
                warn!("Archive close_all closed diff");
                Ok(())
            });

            let coin_handles: Vec<_> = coins
                .drain()
                .map(|(coin_key, mut writers)| {
                    scope.spawn(move || -> parquet::errors::Result<()> {
                        let coin = writers.status.coin().to_string();
                        let _unused = coin_key;
                        writers.status.close()?;
                        warn!("Archive close_all closed status coin={}", coin);
                        writers.fill.close_with_flush(mode, write_fill_rows)?;
                        warn!("Archive close_all closed fill coin={}", coin);
                        Ok(())
                    })
                })
                .collect();

            match blocks_handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => first_error = Some(err),
                Err(err) => {
                    first_error = Some(io_to_parquet_error(io_other(format!("blocks close thread panicked: {err:?}"))))
                }
            }
            match diff_handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) if first_error.is_none() => first_error = Some(err),
                Ok(Err(_)) => {}
                Err(err) if first_error.is_none() => {
                    first_error = Some(io_to_parquet_error(io_other(format!("diff close thread panicked: {err:?}"))))
                }
                Err(_) => {}
            }
            for handle in coin_handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) if first_error.is_none() => first_error = Some(err),
                    Ok(Err(_)) => {}
                    Err(err) if first_error.is_none() => {
                        first_error =
                            Some(io_to_parquet_error(io_other(format!("coin close thread panicked: {err:?}"))))
                    }
                    Err(_) => {}
                }
            }
        });

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    fn abort_all(&mut self) {
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

#[allow(unsafe_code)]
fn best_effort_drop_file_cache(path: &Path) {
    let file = match fs::OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(_) => match fs::File::open(path) {
            Ok(file) => file,
            Err(err) => {
                warn!("Failed to open {} for cache drop: {err}", path.display());
                return;
            }
        },
    };
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc != 0 {
        warn!("posix_fadvise(DONTNEED) failed for {}: {}", path.display(), std::io::Error::from_raw_os_error(rc));
    }
}

#[derive(Debug)]
struct LocalRecoveryFile<R> {
    path: PathBuf,
    name_start_block: u64,
    rows: Vec<R>,
    last_block: u64,
}

struct RecoveredResumeState<R> {
    committed_rows: Vec<R>,
    delayed_rows: Vec<R>,
    active_rows: Vec<R>,
}

fn split_recovered_rows_for_resume<R: HasBlockNumber>(
    stream: StreamKind,
    actual_start: u64,
    rows: Vec<R>,
) -> RecoveredResumeState<R> {
    let Some(span) = stream.row_group_block_limit() else {
        return RecoveredResumeState { committed_rows: rows, delayed_rows: Vec::new(), active_rows: Vec::new() };
    };
    if rows.is_empty() {
        return RecoveredResumeState { committed_rows: rows, delayed_rows: Vec::new(), active_rows: Vec::new() };
    }

    let current_group_start = aligned_row_group_bounds(actual_start, span).0;
    let keep_previous_group_delayed = stream.uses_delayed_flush()
        && stream.delayed_flush_lookahead_blocks().is_some_and(|lookahead| {
            if actual_start <= current_group_start {
                true
            } else {
                actual_start.saturating_sub(current_group_start) < lookahead
            }
        });
    let delayed_group_start = current_group_start.saturating_sub(span);

    let mut committed_rows = Vec::new();
    let mut delayed_rows = Vec::new();
    let mut active_rows = Vec::new();

    for row in rows {
        let block = row.block_number();
        if block >= current_group_start {
            active_rows.push(row);
        } else if keep_previous_group_delayed && block >= delayed_group_start {
            delayed_rows.push(row);
        } else {
            committed_rows.push(row);
        }
    }

    RecoveredResumeState { committed_rows, delayed_rows, active_rows }
}

fn parse_local_archive_filename_bounds(path: &Path, filename_prefix: &str) -> Option<(u64, u64)> {
    let file_name = path.file_name()?.to_str()?;
    let stem = if let Some(stem) = file_name.strip_suffix(LOCAL_RECOVERY_FILE_SUFFIX) {
        stem
    } else if let Some(stem) = file_name.strip_suffix(FINAL_PARQUET_FILE_SUFFIX) {
        stem
    } else {
        return None;
    };
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
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(LOCAL_RECOVERY_FILE_SUFFIX))
        {
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

fn local_recovery_filename_prefix(stream: StreamKind, mode: ArchiveMode, coin: &str) -> String {
    archive_filename_prefix(stream, mode, coin)
}

fn local_recovery_logical_end(stream: StreamKind, observed_height: u64, window_end_block: u64) -> u64 {
    let logical_end = if current_archive_align_output_to_1000_boundary() {
        aligned_archive_output_end(observed_height).unwrap_or_else(|| observed_height.saturating_sub(1))
    } else {
        observed_height.saturating_sub(1)
    };
    match stream {
        StreamKind::Blocks | StreamKind::Fill => logical_end.min(window_end_block),
        StreamKind::Diff | StreamKind::Status | StreamKind::StatusReject => {
            observed_height.saturating_sub(1).min(window_end_block)
        }
    }
}

fn local_recovery_resume_end(
    stream: StreamKind,
    observed_height: u64,
    window_end_block: u64,
    last_row_block: u64,
) -> u64 {
    last_row_block.max(local_recovery_logical_end(stream, observed_height, window_end_block))
}

fn preflight_local_recovery_discontinuity<R: RecoverableArchiveRow>(
    stream: StreamKind,
    coin: &str,
    mode: ArchiveMode,
    observed_height: u64,
    handoff_tx: &Sender<HandoffMessage>,
    supports_local_recovery: bool,
) -> parquet::errors::Result<()> {
    if !supports_local_recovery {
        return Ok(());
    }
    let (_, window_end_block) = rotation_bounds_for(stream, observed_height);
    let filename_prefix = local_recovery_filename_prefix(stream, mode, coin);
    let Some((path, name_start_block)) =
        find_local_recovery_path(&current_archive_base_dir(), &filename_prefix, window_end_block)?
    else {
        return Ok(());
    };
    let rows = R::read_local_rows(&path, mode)?;
    let last_block = rows.last().map(HasBlockNumber::block_number).unwrap_or(0);
    if last_block == 0 {
        fs::remove_file(&path).map_err(io_to_parquet_error)?;
        return Ok(());
    }
    let resume_end_block = local_recovery_resume_end(stream, observed_height, window_end_block, last_block);
    if observed_height <= resume_end_block.saturating_add(1) {
        return Ok(());
    }

    let logical_end_block = local_recovery_logical_end(stream, observed_height, window_end_block);
    let span_blocks = logical_end_block.saturating_sub(name_start_block).saturating_add(1);
    if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
        fs::remove_file(&path).map_err(io_to_parquet_error)?;
        return Ok(());
    }

    let staged_path = stage_local_file_for_handoff(&path)?;
    let relative_path = archive_relative_dir(stream, mode, coin)
        .join(format!("{}_{}_{}{}", filename_prefix, name_start_block, logical_end_block, FINAL_PARQUET_FILE_SUFFIX));
    let handoff_config = current_archive_handoff_config();
    enqueue_handoff_task(handoff_tx, &handoff_config, staged_path, relative_path)
}

fn preflight_hft_status_local_recovery(
    coin: &str,
    observed_height: u64,
    handoff_tx: &Sender<HandoffMessage>,
    supports_local_recovery: bool,
) -> parquet::errors::Result<()> {
    if !supports_local_recovery {
        return Ok(());
    }
    let (_, window_end_block) = rotation_bounds_for(StreamKind::Status, observed_height);
    let filename_prefix = local_recovery_filename_prefix(StreamKind::Status, ArchiveMode::Hft, coin);
    let Some((path, name_start_block)) =
        find_local_recovery_path(&current_archive_base_dir(), &filename_prefix, window_end_block)?
    else {
        return Ok(());
    };
    let rows = read_local_hft_status_rows(&path)?;
    let last_block = rows.last().map(HasBlockNumber::block_number).unwrap_or(0);
    if last_block == 0 {
        fs::remove_file(&path).map_err(io_to_parquet_error)?;
        return Ok(());
    }
    let resume_end_block = local_recovery_resume_end(StreamKind::Status, observed_height, window_end_block, last_block);
    if observed_height <= resume_end_block.saturating_add(1) {
        return Ok(());
    }

    let logical_end_block = local_recovery_logical_end(StreamKind::Status, observed_height, window_end_block);
    let span_blocks = logical_end_block.saturating_sub(name_start_block).saturating_add(1);
    if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
        fs::remove_file(&path).map_err(io_to_parquet_error)?;
        return Ok(());
    }

    let staged_path = stage_local_file_for_handoff(&path)?;
    let relative_path = archive_relative_dir(StreamKind::Status, ArchiveMode::Hft, coin)
        .join(format!("{}_{}_{}{}", filename_prefix, name_start_block, logical_end_block, FINAL_PARQUET_FILE_SUFFIX));
    let handoff_config = current_archive_handoff_config();
    enqueue_handoff_task(handoff_tx, &handoff_config, staged_path, relative_path)
}

fn preflight_startup_local_recovery(
    mode: ArchiveMode,
    symbols: &[String],
    observed_height: u64,
    handoff_tx: &Sender<HandoffMessage>,
) -> parquet::errors::Result<()> {
    for coin in symbols {
        preflight_local_recovery_discontinuity::<FillOut>(
            StreamKind::Fill,
            coin,
            mode,
            observed_height,
            handoff_tx,
            true,
        )?;
        if matches!(mode, ArchiveMode::Hft) {
            preflight_local_recovery_discontinuity::<DiffOut>(
                StreamKind::Diff,
                coin,
                mode,
                observed_height,
                handoff_tx,
                true,
            )?;
            preflight_hft_status_local_recovery(coin, observed_height, handoff_tx, true)?;
        }
    }
    Ok(())
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
            block_time: format_timestamp_millis(read_row_timestamp_millis(&row, 1)?)?,
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

fn shared_blocks_row(
    block_number: u64,
    block_time: String,
    order_n: i32,
    diff_n: i32,
    fill_n: i32,
    btc_status_n: i32,
    btc_diff_n: i32,
    btc_fill_n: i32,
    eth_status_n: i32,
    eth_diff_n: i32,
    eth_fill_n: i32,
) -> BlockIndexOut {
    BlockIndexOut {
        block_number,
        block_time,
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
        archive_mode: "shared".to_string(),
        tracked_symbols: String::new(),
    }
}

fn export_shared_blocks_segment(
    mode: ArchiveMode,
    symbols: &[String],
    segment_start_block: Option<u64>,
    last_block: Option<u64>,
    handoff_tx: &Sender<HandoffMessage>,
    handoff_config: &ArchiveHandoffConfig,
    recover_locally: bool,
) -> parquet::errors::Result<()> {
    if recover_locally {
        return Ok(());
    }
    let Some(start_block) = segment_start_block else {
        return Ok(());
    };
    let Some(mut logical_end_block) = last_block else {
        return Ok(());
    };
    if current_archive_align_output_to_1000_boundary() {
        let Some(aligned_end) = aligned_archive_output_end(logical_end_block) else {
            return Ok(());
        };
        logical_end_block = aligned_end;
    }
    if logical_end_block < start_block {
        return Ok(());
    }
    let span_blocks = logical_end_block.saturating_sub(start_block) + 1;
    if span_blocks < MIN_HANDOFF_BLOCK_SPAN {
        info!("Archive finalized but dropping short parquet blocks span={}", span_blocks);
        return Ok(());
    }
    let shared_rows = {
        let shared = shared_blocks_index().lock().expect("shared blocks index");
        shared.collect_rows(start_block, logical_end_block)?
    };
    if shared_rows.is_empty() {
        return Ok(());
    }
    let archive_mode = match mode {
        ArchiveMode::Lite => "lite".to_string(),
        ArchiveMode::Full => "full".to_string(),
        ArchiveMode::Hft => "hft".to_string(),
    };
    let tracked_symbols = symbols.join(",");
    let rows: Vec<BlockIndexOut> = shared_rows
        .into_iter()
        .map(|row| BlockIndexOut {
            archive_mode: archive_mode.clone(),
            tracked_symbols: tracked_symbols.clone(),
            ..row
        })
        .collect();
    let base_dir = current_archive_base_dir();
    fs::create_dir_all(&base_dir).map_err(io_to_parquet_error)?;
    let final_path =
        base_dir.join(format!("blocks_{}_{}{}", start_block, logical_end_block, FINAL_PARQUET_FILE_SUFFIX));
    let tmp_path = final_path.with_file_name(format!(
        "{}{}",
        final_path.file_name().and_then(|name| name.to_str()).unwrap_or("blocks.parquet"),
        TEMP_FILE_SUFFIX
    ));
    if tmp_path.exists() {
        fs::remove_file(&tmp_path).map_err(io_to_parquet_error)?;
    }
    let file = fs::File::create(&tmp_path).map_err(io_to_parquet_error)?;
    let mut writer = SerializedFileWriter::new(file, blocks_schema(), archive_writer_props())?;
    write_block_rows(&mut writer, mode, &rows)?;
    writer.close()?;
    fs::rename(&tmp_path, &final_path).map_err(io_to_parquet_error)?;
    let relative_path = archive_relative_dir(StreamKind::Blocks, ArchiveMode::Lite, "")
        .join(format!("blocks_{}_{}{}", start_block, logical_end_block, FINAL_PARQUET_FILE_SUFFIX));
    enqueue_handoff_task(handoff_tx, handoff_config, final_path, relative_path)
}

fn read_local_fill_rows(path: &Path, mode: ArchiveMode) -> parquet::errors::Result<Vec<FillOut>> {
    let reader = SerializedFileReader::try_from(path).map_err(io_to_parquet_error)?;
    let iter = reader.get_row_iter(None).map_err(io_to_parquet_error)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row.map_err(io_to_parquet_error)?;
        let column_count = row.get_column_iter().count();
        let mut out = FillOut {
            block_number: row.get_long(0).map_err(io_to_parquet_error)? as u64,
            block_time: format_timestamp_millis(read_row_timestamp_millis(&row, 1)?)?,
            coin: row.get_string(2).map_err(io_to_parquet_error)?.clone(),
            side: String::new(),
            px: 0,
            sz: 0,
            crossed: false,
            address: String::new(),
            closed_pnl: 0,
            fee: 0,
            hash: String::new(),
            oid: 0,
            address_m: String::new(),
            oid_m: 0,
            pnl_m: 0,
            fee_m: 0,
            mm_rate: None,
            mm_share: None,
            filltime: None,
            tid: 0,
            start_position: 0,
            dir: String::new(),
            fee_token: String::new(),
            twap_id: None,
        };
        match mode {
            ArchiveMode::Lite => {
                out.oid = row.get_long(3).map_err(io_to_parquet_error)? as u64;
                out.side = row.get_string(4).map_err(io_to_parquet_error)?.clone();
                out.px = decimal_to_i64(row.get_decimal(5).map_err(io_to_parquet_error)?)?;
                out.sz = decimal_to_i64(row.get_decimal(6).map_err(io_to_parquet_error)?)?;
                out.crossed = row.get_bool(7).map_err(io_to_parquet_error)?;
            }
            ArchiveMode::Hft => {
                out.address = row.get_string(3).map_err(io_to_parquet_error)?.clone();
                out.oid = row.get_long(4).map_err(io_to_parquet_error)? as u64;
                out.side = row.get_string(5).map_err(io_to_parquet_error)?.clone();
                out.closed_pnl = decimal_to_i64(row.get_decimal(6).map_err(io_to_parquet_error)?)?;
                out.fee = decimal_to_i64(row.get_decimal(7).map_err(io_to_parquet_error)?)?;
                out.px = decimal_to_i64(row.get_decimal(8).map_err(io_to_parquet_error)?)?;
                out.sz = decimal_to_i64(row.get_decimal(9).map_err(io_to_parquet_error)?)?;
                out.address_m = row.get_string(10).map_err(io_to_parquet_error)?.clone();
                out.oid_m = row.get_long(11).map_err(io_to_parquet_error)? as u64;
                out.pnl_m = decimal_to_i64(row.get_decimal(12).map_err(io_to_parquet_error)?)?;
                out.fee_m = decimal_to_i64(row.get_decimal(13).map_err(io_to_parquet_error)?)?;
                out.mm_rate = read_row_optional_i32(&row, 14)?;
                out.mm_share = read_row_optional_decimal_i64(&row, 15)?;
                if column_count != 18 {
                    return Err(io_to_parquet_error(io_other(format!(
                        "obsolete HFT fill local recovery schema with {column_count} columns is unsupported"
                    ))));
                }
                out.filltime = read_row_optional_i32(&row, 16)?;
                out.tid = row.get_long(17).map_err(io_to_parquet_error)? as u64;
            }
            ArchiveMode::Full => {
                out.address = row.get_string(3).map_err(io_to_parquet_error)?.clone();
                out.oid = row.get_long(4).map_err(io_to_parquet_error)? as u64;
                out.side = row.get_string(5).map_err(io_to_parquet_error)?.clone();
                out.px = decimal_to_i64(row.get_decimal(6).map_err(io_to_parquet_error)?)?;
                out.sz = decimal_to_i64(row.get_decimal(7).map_err(io_to_parquet_error)?)?;
                out.crossed = row.get_bool(8).map_err(io_to_parquet_error)?;
                out.closed_pnl = decimal_to_i64(row.get_decimal(9).map_err(io_to_parquet_error)?)?;
                out.fee = decimal_to_i64(row.get_decimal(10).map_err(io_to_parquet_error)?)?;
                out.hash = row.get_string(11).map_err(io_to_parquet_error)?.clone();
                out.tid = row.get_long(12).map_err(io_to_parquet_error)? as u64;
                out.start_position = decimal_to_i64(row.get_decimal(13).map_err(io_to_parquet_error)?)?;
                out.dir = row.get_string(14).map_err(io_to_parquet_error)?.clone();
                out.fee_token = row.get_string(15).map_err(io_to_parquet_error)?.clone();
                out.twap_id = match row.get_column_iter().nth(16).map(|(_, field)| field) {
                    Some(parquet::record::Field::Long(value)) => Some(*value),
                    Some(parquet::record::Field::Null) => None,
                    Some(field) => {
                        return Err(io_to_parquet_error(io_other(format!(
                            "unexpected twap_id parquet field value {field:?}"
                        ))));
                    }
                    None => None,
                };
            }
        }
        rows.push(out);
    }
    Ok(rows)
}

fn read_local_diff_rows(path: &Path, mode: ArchiveMode) -> parquet::errors::Result<Vec<DiffOut>> {
    let reader = SerializedFileReader::try_from(path).map_err(io_to_parquet_error)?;
    let iter = reader.get_row_iter(None).map_err(io_to_parquet_error)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row.map_err(io_to_parquet_error)?;
        let column_count = row.get_column_iter().count();
        let mut out = DiffOut {
            block_number: row.get_long(0).map_err(io_to_parquet_error)? as u64,
            block_time: format_timestamp_millis(read_row_timestamp_millis(&row, 1)?)?,
            coin: row.get_string(2).map_err(io_to_parquet_error)?.clone(),
            oid: 0,
            diff_type: String::new(),
            sz: 0,
            user: String::new(),
            side: String::new(),
            px: 0,
            orig_sz: 0,
            event: String::new(),
            status: None,
            limit_px: None,
            _sz: None,
            _orig_sz: None,
            is_trigger: None,
            tif: String::new(),
            reduce_only: None,
            order_type: None,
            trigger_condition: None,
            trigger_px: None,
            is_position_tpsl: None,
            tp_trigger_px: None,
            sl_trigger_px: None,
            lifetime: None,
        };
        match mode {
            ArchiveMode::Lite => {
                out.oid = row.get_long(3).map_err(io_to_parquet_error)? as u64;
                out.side = row.get_string(4).map_err(io_to_parquet_error)?.clone();
                out.px = decimal_to_i64(row.get_decimal(5).map_err(io_to_parquet_error)?)?;
                out.diff_type = row.get_string(6).map_err(io_to_parquet_error)?.clone();
                out.sz = decimal_to_i64(row.get_decimal(7).map_err(io_to_parquet_error)?)?;
                out.orig_sz = decimal_to_i64(row.get_decimal(8).map_err(io_to_parquet_error)?)?;
            }
            ArchiveMode::Hft => {
                if column_count != 25 {
                    return Err(io_to_parquet_error(io_other(format!(
                        "obsolete HFT diff local recovery schema with {column_count} columns is unsupported"
                    ))));
                }
                out.user = row.get_string(3).map_err(io_to_parquet_error)?.clone();
                out.oid = row.get_long(4).map_err(io_to_parquet_error)? as u64;
                out.side = row.get_string(5).map_err(io_to_parquet_error)?.clone();
                out.px = decimal_to_i64(row.get_decimal(6).map_err(io_to_parquet_error)?)?;
                out.diff_type = row.get_string(7).map_err(io_to_parquet_error)?.clone();
                out.sz = decimal_to_i64(row.get_decimal(8).map_err(io_to_parquet_error)?)?;
                out.orig_sz = decimal_to_i64(row.get_decimal(9).map_err(io_to_parquet_error)?)?;
                out.event = row.get_string(10).map_err(io_to_parquet_error)?.clone();
                out.status = read_row_optional_utf8(&row, 11)?;
                out.limit_px = read_row_optional_decimal_i64(&row, 12)?;
                out._sz = read_row_optional_decimal_i64(&row, 13)?;
                out._orig_sz = read_row_optional_decimal_i64(&row, 14)?;
                out.is_trigger = read_row_optional_bool(&row, 15)?;
                out.tif = read_row_optional_utf8(&row, 16)?.unwrap_or_default();
                out.reduce_only = read_row_optional_bool(&row, 17)?;
                out.order_type = read_row_optional_utf8(&row, 18)?;
                out.trigger_condition = read_row_optional_utf8(&row, 19)?;
                out.trigger_px = read_row_optional_decimal_i64(&row, 20)?;
                out.is_position_tpsl = read_row_optional_bool(&row, 21)?;
                out.tp_trigger_px = read_row_optional_decimal_i64(&row, 22)?;
                out.sl_trigger_px = read_row_optional_decimal_i64(&row, 23)?;
                out.lifetime = read_row_optional_i32(&row, 24)?;
            }
            ArchiveMode::Full => {
                out.user = row.get_string(3).map_err(io_to_parquet_error)?.clone();
                out.oid = row.get_long(4).map_err(io_to_parquet_error)? as u64;
                out.diff_type = row.get_string(5).map_err(io_to_parquet_error)?.clone();
                out.sz = decimal_to_i64(row.get_decimal(6).map_err(io_to_parquet_error)?)?;
                out.side = row.get_string(7).map_err(io_to_parquet_error)?.clone();
                out.px = decimal_to_i64(row.get_decimal(8).map_err(io_to_parquet_error)?)?;
                out.orig_sz = decimal_to_i64(row.get_decimal(9).map_err(io_to_parquet_error)?)?;
            }
        }
        rows.push(out);
    }
    Ok(rows)
}

fn read_local_hft_status_rows(path: &Path) -> parquet::errors::Result<Vec<HftStatusRecoveryRow>> {
    let reader = SerializedFileReader::try_from(path).map_err(io_to_parquet_error)?;
    let iter = reader.get_row_iter(None).map_err(io_to_parquet_error)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row.map_err(io_to_parquet_error)?;
        let _coin = row.get_string(2).map_err(io_to_parquet_error)?.clone();
        let sz = decimal_to_i64(row.get_decimal(8).map_err(io_to_parquet_error)?)?;
        rows.push(HftStatusRecoveryRow {
            block_number: row.get_long(0).map_err(io_to_parquet_error)? as u64,
            block_time: format_timestamp_millis(read_row_timestamp_millis(&row, 1)?)?,
            user: row.get_string(3).map_err(io_to_parquet_error)?.clone(),
            oid: row.get_long(4).map_err(io_to_parquet_error)? as u64,
            status: row.get_string(5).map_err(io_to_parquet_error)?.clone(),
            side: row.get_string(6).map_err(io_to_parquet_error)?.clone(),
            limit_px: decimal_to_i64(row.get_decimal(7).map_err(io_to_parquet_error)?)?,
            sz,
            orig_sz: read_row_optional_decimal_i64(&row, 9)?.unwrap_or(sz),
            is_trigger: row.get_bool(10).map_err(io_to_parquet_error)?,
            tif: row.get_string(11).map_err(io_to_parquet_error)?.clone(),
            trigger_condition: row.get_string(12).map_err(io_to_parquet_error)?.clone(),
            trigger_px: decimal_to_i64(row.get_decimal(13).map_err(io_to_parquet_error)?)?,
            is_position_tpsl: row.get_bool(14).map_err(io_to_parquet_error)?,
            reduce_only: row.get_bool(15).map_err(io_to_parquet_error)?,
            order_type: row.get_string(16).map_err(io_to_parquet_error)?.clone(),
            tp_trigger_px: read_row_optional_decimal_i64(&row, 17)?,
            sl_trigger_px: read_row_optional_decimal_i64(&row, 18)?,
        });
    }
    Ok(rows)
}

fn hft_status_rows_to_batch(rows: Vec<HftStatusRecoveryRow>) -> StatusBlockBatch {
    let mut columns = StatusFullColumns::default();
    for row in rows {
        columns.block_number.push(row.block_number as i64);
        columns.block_time.push(byte_array_from_string(row.block_time));
        columns.status.push(byte_array_from_string(row.status));
        columns.oid.push(row.oid as i64);
        columns.side.push(byte_array_from_string(row.side));
        columns.limit_px.push(row.limit_px);
        columns.is_trigger.push(row.is_trigger);
        columns.tif.push(byte_array_from_string(row.tif));
        columns.user.push(byte_array_from_string(row.user));
        columns.hash.push(byte_array_from_str(""));
        columns.order_type.push(byte_array_from_string(row.order_type));
        columns.sz.push(row.sz);
        columns.orig_sz.push(row.orig_sz);
        columns.time.push(byte_array_from_str(""));
        columns.builder.push(byte_array_from_str(""));
        columns.timestamp.push(0);
        columns.trigger_condition.push(byte_array_from_string(row.trigger_condition));
        columns.trigger_px.push(row.trigger_px);
        columns.tp_trigger_px.push(row.tp_trigger_px);
        columns.sl_trigger_px.push(row.sl_trigger_px);
        columns.is_position_tpsl.push(row.is_position_tpsl);
        columns.reduce_only.push(row.reduce_only);
        columns.cloid.push(byte_array_from_str(""));
    }
    StatusBlockBatch::Hft(columns)
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
    if ARCHIVE_STALE_TMP_CLEANED.swap(true, Ordering::SeqCst) {
        return;
    }
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
        let mut in_flight: Vec<thread::JoinHandle<()>> = Vec::new();

        let reap_finished = |in_flight: &mut Vec<thread::JoinHandle<()>>| {
            let mut idx = 0;
            while idx < in_flight.len() {
                if in_flight[idx].is_finished() {
                    let handle = in_flight.swap_remove(idx);
                    let _unused = handle.join();
                } else {
                    idx += 1;
                }
            }
        };

        let wait_for_slot = |in_flight: &mut Vec<thread::JoinHandle<()>>| {
            while in_flight.len() >= MAX_CONCURRENT_HANDOFF_TASKS {
                reap_finished(in_flight);
                if in_flight.len() < MAX_CONCURRENT_HANDOFF_TASKS {
                    break;
                }
                let handle = in_flight.swap_remove(0);
                let _unused = handle.join();
            }
        };

        let drain_all = |in_flight: &mut Vec<thread::JoinHandle<()>>| {
            while let Some(handle) = in_flight.pop() {
                let _unused = handle.join();
            }
        };

        while let Ok(message) = rx.recv() {
            match message {
                HandoffMessage::File(task) => {
                    wait_for_slot(&mut in_flight);
                    let stop = stop.clone();
                    in_flight.push(thread::spawn(move || {
                        if let Err(err) = handoff_finalized_parquet(&task) {
                            warn!(
                                "Archive handoff failed for {} (stop={}): {err}",
                                task.path.display(),
                                stop.load(Ordering::SeqCst)
                            );
                        }
                    }));
                }
                HandoffMessage::Barrier(done_tx) => {
                    drain_all(&mut in_flight);
                    let _unused = done_tx.send(());
                }
            }
        }
        while let Some(handle) = in_flight.pop() {
            let _unused = handle.join();
        }
    });
    ArchiveHandoffWorker { tx, handle }
}

fn enqueue_handoff_task(
    handoff_tx: &Sender<HandoffMessage>,
    config: &ArchiveHandoffConfig,
    path: PathBuf,
    relative_path: PathBuf,
) -> parquet::errors::Result<()> {
    let task = HandoffTask { path, relative_path, config: config.clone() };
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
    best_effort_drop_file_cache(path);
    best_effort_drop_file_cache(&dest_path);
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
    Ok(())
}

fn archive_handoff_label(relative_path: &Path) -> String {
    let Some(file_name) = relative_path.file_name().and_then(|name| name.to_str()) else {
        return relative_path.display().to_string();
    };
    file_name
        .strip_suffix(FINAL_PARQUET_FILE_SUFFIX)
        .or_else(|| file_name.strip_suffix(LOCAL_RECOVERY_FILE_SUFFIX))
        .unwrap_or(file_name)
        .to_string()
}

fn archive_handoff_span(relative_path: &Path) -> Option<(u64, u64)> {
    let file_name = relative_path.file_name()?.to_str()?;
    let stem = file_name
        .strip_suffix(FINAL_PARQUET_FILE_SUFFIX)
        .or_else(|| file_name.strip_suffix(LOCAL_RECOVERY_FILE_SUFFIX))
        .unwrap_or(file_name);
    let mut parts = stem.rsplitn(3, '_');
    let end = parts.next()?.parse::<u64>().ok()?;
    let start = parts.next()?.parse::<u64>().ok()?;
    Some((start, end))
}

fn handoff_finalized_parquet(task: &HandoffTask) -> parquet::errors::Result<()> {
    let path = &task.path;
    let relative_path = &task.relative_path;
    let config = &task.config;
    let label = archive_handoff_label(relative_path);
    let span = archive_handoff_span(relative_path)
        .map(|(start, end)| format!("{}..{}", start, end))
        .unwrap_or_else(|| "unknown".to_string());
    let nas_dest =
        if config.move_to_nas { Some(copy_to_nas(path, relative_path, &config.nas_output_dir)?) } else { None };

    if config.upload_to_oss {
        let Some(oss_config) = config.oss.as_ref() else {
            return Err(io_to_parquet_error(io_other(
                "upload_to_oss enabled but OSS credentials/config were not provided",
            )));
        };
        upload_finalized_parquet_to_oss(path, relative_path, oss_config)?;
        best_effort_drop_file_cache(path);
    }

    if config.move_to_nas {
        best_effort_drop_file_cache(path);
        fs::remove_file(path).map_err(io_to_parquet_error)?;
        let target = if config.upload_to_oss { "NAS+OSS" } else { "NAS" };
        let _unused = nas_dest;
        info!("Archive handoff complete: {} [{}] -> {}", label, span, target);
    } else if config.upload_to_oss {
        best_effort_drop_file_cache(path);
        info!("Archive handoff complete: {} [{}] -> OSS", label, span);
    } else {
        best_effort_drop_file_cache(path);
        info!("Archive finalized locally without handoff: {} [{}]", label, span);
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
    if archive_thread_context(|ctx| ctx.is_none()) {
        set_archive_mode(None);
    } else {
        update_archive_thread_context(|ctx| ctx.enabled.store(false, Ordering::SeqCst));
    }
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
            *writers = Some(ArchiveWriters::new(mode, symbols, current_archive_symbol_decimals(), handoff_tx.clone()));
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
            *writers = Some(ArchiveWriters::new(mode, symbols, current_archive_symbol_decimals(), handoff_tx.clone()));
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

    let benign_float_tail =
        omitted_digits > 0 && (omitted_all_nines || (omitted_zero_prefix_then_single_one && omitted_seen_single_one));
    if precision_lost && !benign_float_tail {
        warn!("Precision loss: rounded '{}' to {} decimal places", s, scale);
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

fn byte_array_from_string(value: String) -> ByteArray {
    ByteArray::from(value.into_bytes())
}

fn byte_array_from_str(value: &str) -> ByteArray {
    ByteArray::from(value.as_bytes().to_vec())
}

fn byte_array_to_string(value: &ByteArray) -> parquet::errors::Result<String> {
    String::from_utf8(value.data().to_vec())
        .map_err(|err| io_to_parquet_error(io_other(format!("invalid utf8 in archive byte array: {err}"))))
}

fn infer_epoch_timestamp_millis(value: i64) -> parquet::errors::Result<i64> {
    let abs = value.saturating_abs();
    if abs >= 1_000_000_000_000_000_000 {
        Ok(value / 1_000_000)
    } else if abs >= 1_000_000_000_000_000 {
        Ok(value / 1_000)
    } else if abs >= 1_000_000_000_000 {
        Ok(value)
    } else {
        value
            .checked_mul(1_000)
            .ok_or_else(|| io_to_parquet_error(io_other(format!("timestamp seconds overflow: {value}"))))
    }
}

fn parse_timestamp_millis(value: &str) -> parquet::errors::Result<i64> {
    let value = value.trim();
    if let Ok(raw) = value.parse::<i64>() {
        return infer_epoch_timestamp_millis(raw);
    }
    if let Ok(dt) = OffsetDateTime::parse(value, &Rfc3339) {
        return Ok(dt.unix_timestamp_nanos().div_euclid(1_000_000) as i64);
    }
    if let Ok(dt) = PrimitiveDateTime::parse(value, INPUT_TIMESTAMP_NO_TZ_WITH_SUBSEC) {
        return Ok(dt.assume_utc().unix_timestamp_nanos().div_euclid(1_000_000) as i64);
    }
    if let Ok(dt) = PrimitiveDateTime::parse(value, INPUT_TIMESTAMP_NO_TZ_NO_SUBSEC) {
        return Ok(dt.assume_utc().unix_timestamp_nanos().div_euclid(1_000_000) as i64);
    }
    Err(io_to_parquet_error(io_other(format!("invalid timestamp '{value}'"))))
}

fn parse_timestamp_column_millis(values: &[ByteArray]) -> parquet::errors::Result<Vec<i64>> {
    let mut out = Vec::with_capacity(values.len());
    let mut last_raw = None::<String>;
    let mut last_parsed = 0i64;
    for value in values {
        let raw = byte_array_to_string(value)?;
        if last_raw.as_deref() == Some(raw.as_str()) {
            out.push(last_parsed);
            continue;
        }
        let parsed = parse_timestamp_millis(&raw)?;
        last_raw = Some(raw);
        last_parsed = parsed;
        out.push(parsed);
    }
    Ok(out)
}

fn format_timestamp_millis(value: i64) -> parquet::errors::Result<String> {
    let normalized = infer_epoch_timestamp_millis(value)?;
    let nanos = (normalized as i128)
        .checked_mul(1_000_000)
        .ok_or_else(|| io_to_parquet_error(io_other(format!("timestamp millis overflow: {value}"))))?;
    let dt = OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|_| io_to_parquet_error(io_other(format!("timestamp millis overflow: {value}"))))?
        .to_offset(UtcOffset::UTC);
    dt.format(OUTPUT_TIMESTAMP_MILLIS_UTC)
        .map_err(|err| io_to_parquet_error(io_other(format!("format timestamp millis failed for {value}: {err}"))))
}

fn read_row_timestamp_millis(row: &parquet::record::Row, index: usize) -> parquet::errors::Result<i64> {
    match row.get_column_iter().nth(index).map(|(_, field)| field) {
        Some(parquet::record::Field::TimestampMillis(value)) => Ok(*value),
        Some(parquet::record::Field::TimestampMicros(value)) => Ok(*value / 1_000),
        Some(parquet::record::Field::Long(value)) => infer_epoch_timestamp_millis(*value),
        Some(field) => Err(io_to_parquet_error(io_other(format!(
            "unexpected parquet timestamp field value at column {index}: {field:?}"
        )))),
        None => Err(io_to_parquet_error(io_other(format!("missing parquet timestamp field at column {index}")))),
    }
}

fn read_row_optional_utf8(row: &parquet::record::Row, index: usize) -> parquet::errors::Result<Option<String>> {
    match row.get_column_iter().nth(index).map(|(_, field)| field) {
        Some(parquet::record::Field::Str(value)) => Ok(Some(value.clone())),
        Some(parquet::record::Field::Null) => Ok(None),
        Some(field) => Err(io_to_parquet_error(io_other(format!(
            "unexpected parquet utf8 field value at column {index}: {field:?}"
        )))),
        None => Err(io_to_parquet_error(io_other(format!("missing parquet utf8 field at column {index}")))),
    }
}

fn read_row_optional_bool(row: &parquet::record::Row, index: usize) -> parquet::errors::Result<Option<bool>> {
    match row.get_column_iter().nth(index).map(|(_, field)| field) {
        Some(parquet::record::Field::Bool(value)) => Ok(Some(*value)),
        Some(parquet::record::Field::Null) => Ok(None),
        Some(field) => Err(io_to_parquet_error(io_other(format!(
            "unexpected parquet bool field value at column {index}: {field:?}"
        )))),
        None => Err(io_to_parquet_error(io_other(format!("missing parquet bool field at column {index}")))),
    }
}

fn read_row_optional_i32(row: &parquet::record::Row, index: usize) -> parquet::errors::Result<Option<i32>> {
    match row.get_column_iter().nth(index).map(|(_, field)| field) {
        Some(parquet::record::Field::Int(value)) => Ok(Some(*value)),
        Some(parquet::record::Field::Null) => Ok(None),
        Some(field) => Err(io_to_parquet_error(io_other(format!(
            "unexpected parquet int32 field value at column {index}: {field:?}"
        )))),
        None => Err(io_to_parquet_error(io_other(format!("missing parquet int32 field at column {index}")))),
    }
}

fn read_row_optional_decimal_i64(row: &parquet::record::Row, index: usize) -> parquet::errors::Result<Option<i64>> {
    match row.get_column_iter().nth(index).map(|(_, field)| field) {
        Some(parquet::record::Field::Decimal(value)) => Ok(Some(decimal_to_i64(value)?)),
        Some(parquet::record::Field::Null) => Ok(None),
        Some(field) => Err(io_to_parquet_error(io_other(format!(
            "unexpected parquet decimal field value at column {index}: {field:?}"
        )))),
        None => Err(io_to_parquet_error(io_other(format!("missing parquet decimal field at column {index}")))),
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

fn parse_optional_utf8_column(values: &[ByteArray]) -> parquet::errors::Result<(Vec<ByteArray>, Vec<i16>)> {
    let mut parsed = Vec::new();
    let mut def_levels = Vec::with_capacity(values.len());
    for value in values {
        let text = byte_array_to_string(value)?;
        if text.is_empty() {
            def_levels.push(0);
            continue;
        }
        parsed.push(ByteArray::from(text.into_bytes()));
        def_levels.push(1);
    }
    Ok((parsed, def_levels))
}

fn parse_optional_byte_array_column(values: &[Option<ByteArray>]) -> (Vec<ByteArray>, Vec<i16>) {
    let mut parsed = Vec::new();
    let mut def_levels = Vec::with_capacity(values.len());
    for value in values {
        if let Some(value) = value {
            parsed.push(value.clone());
            def_levels.push(1);
        } else {
            def_levels.push(0);
        }
    }
    (parsed, def_levels)
}

fn diff_type_name(diff_type: u8) -> &'static str {
    match diff_type {
        0 => "new",
        1 => "update",
        2 => "remove",
        _ => "unknown",
    }
}

fn protocol_side_str(side: crate::protocol::Side) -> &'static str {
    match side {
        crate::protocol::Side::Ask => "A",
        crate::protocol::Side::Bid => "B",
    }
}

fn classify_hft_diff_event(diff_type: u8, sz: i64, orig_sz: i64, same_block_trade_sz: Option<i64>) -> &'static str {
    match diff_type {
        0 => "add",
        1 => {
            if same_block_trade_sz.is_some() {
                "fill"
            } else {
                let delta = sz - orig_sz;
                let cause = if delta >= 0 { "add" } else { "cancel" };
                cause
            }
        }
        2 => {
            if same_block_trade_sz.is_some() {
                "fill"
            } else {
                "cancel"
            }
        }
        _ => "unknown",
    }
}

fn run_reject_archive_writer(rx: Receiver<RejectArchiveBlock>, stop: Arc<AtomicBool>) {
    let handoff_worker = start_archive_handoff_worker(stop.clone());
    let handoff_tx = handoff_worker.tx.clone();
    let handoff_config = current_archive_handoff_config();
    let props = archive_writer_props();
    let symbols = current_archive_symbols();
    let symbols_set: HashSet<String> = symbols.iter().cloned().collect();
    let mut writer = ParquetStreamWriter::new(
        StreamKind::StatusReject,
        reject_schema(),
        props,
        handoff_tx.clone(),
        handoff_config.clone(),
    );
    let mut effective_stop_height = current_archive_stop_height();
    let mut bootstrap_checkpoint_evaluated = false;

    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(msg) => {
                set_archive_phase(Some(msg.block_number), "build_reject_status_rows");
                let reject_events = msg.events.as_ref();
                let Some(height) = reject_events.first().map(|event| event.block_number).or(Some(msg.block_number))
                else {
                    continue;
                };
                if !bootstrap_checkpoint_evaluated {
                    if effective_stop_height.is_none() {
                        if let Some(archive_height) = current_archive_height_span() {
                            effective_stop_height = Some(height.saturating_add(archive_height).saturating_sub(1));
                        }
                    }
                    bootstrap_checkpoint_evaluated = true;
                }

                let mut reject_rows = Vec::new();
                for status in reject_events {
                    if !is_direct_reject_status(&status.status) {
                        continue;
                    }
                    if !symbols_set.contains(&status.coin) {
                        continue;
                    }
                    let s_px = parse_scaled(&status.limit_px, 5).unwrap_or(0);
                    let s_sz = parse_scaled(&status.sz, 5).unwrap_or(0);
                    let s_trig = parse_scaled(&status.trigger_px, 5).unwrap_or(0);
                    reject_rows.push(RejectOut {
                        block_number: height,
                        block_time: status.block_time.clone(),
                        coin: status.coin.clone(),
                        user: status.user.clone(),
                        status: status.status.clone(),
                        side: match status.side {
                            crate::protocol::Side::Ask => "A".to_string(),
                            crate::protocol::Side::Bid => "B".to_string(),
                        },
                        limit_px: s_px,
                        sz: s_sz,
                        is_trigger: status.is_trigger,
                        tif: status.tif.clone().unwrap_or_default(),
                        trigger_condition: status.trigger_condition.clone(),
                        trigger_px: s_trig,
                        is_position_tpsl: status.is_position_tpsl,
                        reduce_only: status.reduce_only,
                        order_type: status.order_type.clone(),
                    });
                }
                reject_rows.sort_unstable_by(|lhs, rhs| {
                    lhs.user
                        .cmp(&rhs.user)
                        .then_with(|| lhs.coin.cmp(&rhs.coin))
                        .then_with(|| lhs.status.cmp(&rhs.status))
                        .then_with(|| lhs.side.cmp(&rhs.side))
                        .then_with(|| lhs.limit_px.cmp(&rhs.limit_px))
                });

                set_archive_phase(Some(height), "write_reject_status_rows");
                if let Err(err) = writer.advance_block(ArchiveMode::Hft, height, RejectOut::write_rows) {
                    warn!(
                        "Reject archive disabling after fatal failure at block {}: reject advance failed: {}",
                        height, err
                    );
                    archive_thread_context(|ctx| {
                        if let Some(ctx) = ctx {
                            ctx.enabled.store(false, Ordering::SeqCst);
                            ctx.stop_requested.store(true, Ordering::SeqCst);
                        }
                    });
                    writer.abort();
                    break;
                }
                if let Err(err) = writer.append_rows("", ArchiveMode::Hft, height, reject_rows, RejectOut::write_rows) {
                    warn!("Reject archive disabling after fatal failure at block {}: {}", height, err);
                    archive_thread_context(|ctx| {
                        if let Some(ctx) = ctx {
                            ctx.enabled.store(false, Ordering::SeqCst);
                            ctx.stop_requested.store(true, Ordering::SeqCst);
                        }
                    });
                    writer.abort();
                    break;
                }
                if let Some(stop_height) = effective_stop_height.filter(|stop_height| height >= *stop_height) {
                    info!("Reject archive auto-stopping after reaching configured stop height {}", stop_height);
                    archive_thread_context(|ctx| {
                        if let Some(ctx) = ctx {
                            ctx.enabled.store(false, Ordering::SeqCst);
                            ctx.stop_requested.store(true, Ordering::SeqCst);
                        }
                    });
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    archive_thread_context(|ctx| {
        if let Some(ctx) = ctx {
            ctx.enabled.store(false, Ordering::SeqCst);
        }
    });

    if let Err(err) = writer.close_with_flush(ArchiveMode::Hft, RejectOut::write_rows) {
        warn!("Reject archive writer close failed: {err}");
        writer.abort();
    }
    if let Err(err) = drain_handoff_tasks(&handoff_tx) {
        warn!("Reject archive handoff drain failed: {err}");
    }
    let ArchiveHandoffWorker { tx: handoff_worker_tx, handle } = handoff_worker;
    drop(handoff_tx);
    drop(handoff_worker_tx);
    if let Err(err) = handle.join() {
        warn!("Reject archive handoff worker panicked: {err:?}");
    }
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

fn parse_optional_bool_column(values: &[Option<bool>]) -> (Vec<bool>, Vec<i16>) {
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

fn write_reject_rows(
    file: &mut SerializedFileWriter<std::fs::File>,
    rows: &[RejectOut],
) -> parquet::errors::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut row_group = file.next_row_group()?;

    let block_numbers: Vec<i64> = rows.iter().map(|row| row.block_number as i64).collect();
    let block_times: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.block_time)).collect();
    let block_time_millis = parse_timestamp_column_millis(&block_times)?;
    let coins: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.coin)).collect();
    let users: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.user)).collect();
    let statuses: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.status)).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.side)).collect();
    let limit_pxs: Vec<i64> = rows.iter().map(|row| row.limit_px).collect();
    let szs: Vec<i64> = rows.iter().map(|row| row.sz).collect();
    let is_triggers: Vec<bool> = rows.iter().map(|row| row.is_trigger).collect();
    let tifs: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.tif)).collect();
    let trigger_conditions: Vec<ByteArray> =
        rows.iter().map(|row| byte_array_from_str(&row.trigger_condition)).collect();
    let trigger_pxs: Vec<i64> = rows.iter().map(|row| row.trigger_px).collect();
    let is_position_tpsls: Vec<bool> = rows.iter().map(|row| row.is_position_tpsl).collect();
    let reduce_onlys: Vec<bool> = rows.iter().map(|row| row.reduce_only).collect();
    let order_types: Vec<ByteArray> = rows.iter().map(|row| byte_array_from_str(&row.order_type)).collect();

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_numbers, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time_millis, None, None)?;
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
        if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
            typed.write_batch(&sides, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&limit_pxs, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&szs, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
            typed.write_batch(&is_triggers, None, None)?;
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

    row_group.close()?;
    trim_allocator();
    Ok(())
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
        user,
        status,
        oid,
        side,
        limit_px,
        sz,
        orig_sz,
        is_trigger,
        tif,
        trigger_condition,
        trigger_px,
        is_position_tpsl,
        reduce_only,
        order_type,
        time: _,
        timestamp: _,
    } = rows;
    let mut row_group = file.next_row_group()?;
    let coins = vec![byte_array_from_str(coin); block_number.len()];
    let block_time_millis = parse_timestamp_column_millis(&block_time)?;

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_number, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time_millis, None, None)?;
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
            typed.write_batch(&user, None, None)?;
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
            typed.write_batch(&status, None, None)?;
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
        builder,
        trigger_condition,
        trigger_px,
        tp_trigger_px: _,
        sl_trigger_px: _,
        is_position_tpsl,
        reduce_only,
        cloid,
        time: _,
        timestamp: _,
    } = rows;
    let mut row_group = file.next_row_group()?;
    let coins = vec![byte_array_from_str(coin); block_number.len()];
    let block_time_millis = parse_timestamp_column_millis(&block_time)?;
    let (cloid_values, cloid_def_levels) = parse_optional_utf8_column(&cloid)?;

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_number, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time_millis, None, None)?;
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
            typed.write_batch(&user, None, None)?;
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
            typed.write_batch(&status, None, None)?;
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
            typed.write_batch(&builder, None, None)?;
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
            typed.write_batch(&cloid_values, Some(&cloid_def_levels), None)?;
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
    let block_time_millis = parse_timestamp_column_millis(&block_times)?;
    let coins: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.coin.as_bytes())).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let pxs: Vec<i64> = rows.iter().map(|r| r.px).collect();
    let diff_types: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.diff_type.as_bytes())).collect();
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
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_time_millis, None, None)?;
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
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
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
    } else if mode == ArchiveMode::Hft {
        let users: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.user.as_bytes())).collect();
        let events: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.event.as_bytes())).collect();
        let statuses: Vec<Option<ByteArray>> =
            rows.iter().map(|r| r.status.as_ref().map(|value| ByteArray::from(value.as_bytes()))).collect();
        let (status_values, status_def_levels) = parse_optional_byte_array_column(&statuses);
        let limit_pxs: Vec<Option<i64>> = rows.iter().map(|r| r.limit_px).collect();
        let (limit_px_values, limit_px_def_levels) = parse_optional_i64_column(&limit_pxs);
        let status_szs: Vec<Option<i64>> = rows.iter().map(|r| r._sz).collect();
        let (status_sz_values, status_sz_def_levels) = parse_optional_i64_column(&status_szs);
        let status_orig_szs: Vec<Option<i64>> = rows.iter().map(|r| r._orig_sz).collect();
        let (status_orig_sz_values, status_orig_sz_def_levels) = parse_optional_i64_column(&status_orig_szs);
        let is_triggers: Vec<Option<bool>> = rows.iter().map(|r| r.is_trigger).collect();
        let (is_trigger_values, is_trigger_def_levels) = parse_optional_bool_column(&is_triggers);
        let tifs: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.tif.as_bytes())).collect();
        let (tif_values, tif_def_levels) = parse_optional_utf8_column(&tifs)?;
        let reduce_onlys: Vec<Option<bool>> = rows.iter().map(|r| r.reduce_only).collect();
        let (reduce_only_values, reduce_only_def_levels) = parse_optional_bool_column(&reduce_onlys);
        let order_types: Vec<Option<ByteArray>> =
            rows.iter().map(|r| r.order_type.as_ref().map(|value| ByteArray::from(value.as_bytes()))).collect();
        let (order_type_values, order_type_def_levels) = parse_optional_byte_array_column(&order_types);
        let trigger_conditions: Vec<Option<ByteArray>> =
            rows.iter().map(|r| r.trigger_condition.as_ref().map(|value| ByteArray::from(value.as_bytes()))).collect();
        let (trigger_condition_values, trigger_condition_def_levels) =
            parse_optional_byte_array_column(&trigger_conditions);
        let trigger_pxs: Vec<Option<i64>> = rows.iter().map(|r| r.trigger_px).collect();
        let (trigger_px_values, trigger_px_def_levels) = parse_optional_i64_column(&trigger_pxs);
        let is_position_tpsls: Vec<Option<bool>> = rows.iter().map(|r| r.is_position_tpsl).collect();
        let (is_position_tpsl_values, is_position_tpsl_def_levels) = parse_optional_bool_column(&is_position_tpsls);
        let tp_trigger_pxs: Vec<Option<i64>> = rows.iter().map(|r| r.tp_trigger_px).collect();
        let (tp_trigger_px_values, tp_trigger_px_def_levels) = parse_optional_i64_column(&tp_trigger_pxs);
        let sl_trigger_pxs: Vec<Option<i64>> = rows.iter().map(|r| r.sl_trigger_px).collect();
        let (sl_trigger_px_values, sl_trigger_px_def_levels) = parse_optional_i64_column(&sl_trigger_pxs);
        let lifetimes: Vec<Option<i32>> = rows.iter().map(|r| r.lifetime).collect();
        let (lifetime_values, lifetime_def_levels) = parse_optional_i32_column(&lifetimes);

        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_time_millis, None, None)?;
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
                typed.write_batch(&users, None, None)?;
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
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
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
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&events, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&status_values, Some(&status_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&limit_px_values, Some(&limit_px_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&status_sz_values, Some(&status_sz_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&status_orig_sz_values, Some(&status_orig_sz_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&is_trigger_values, Some(&is_trigger_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&tif_values, Some(&tif_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&reduce_only_values, Some(&reduce_only_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&order_type_values, Some(&order_type_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&trigger_condition_values, Some(&trigger_condition_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&trigger_px_values, Some(&trigger_px_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::BoolColumnWriter(typed) = col.untyped() {
                typed.write_batch(&is_position_tpsl_values, Some(&is_position_tpsl_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&tp_trigger_px_values, Some(&tp_trigger_px_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&sl_trigger_px_values, Some(&sl_trigger_px_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&lifetime_values, Some(&lifetime_def_levels), None)?;
            }
            col.close()?;
        }
    } else {
        let users: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.user.as_bytes())).collect();

        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_time_millis, None, None)?;
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
                typed.write_batch(&users, None, None)?;
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
    let block_time_millis = parse_timestamp_column_millis(&block_times)?;
    let coins: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.coin.as_bytes())).collect();
    let addresses: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.address.as_bytes())).collect();
    let sides: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.side.as_bytes())).collect();
    let px: Vec<i64> = rows.iter().map(|r| r.px).collect();
    let sz: Vec<i64> = rows.iter().map(|r| r.sz).collect();
    let crossed: Vec<bool> = rows.iter().map(|r| r.crossed).collect();
    let oids: Vec<i64> = rows.iter().map(|r| r.oid as i64).collect();
    if mode == ArchiveMode::Lite {
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_time_millis, None, None)?;
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
    } else if mode == ArchiveMode::Hft {
        let pnls: Vec<i64> = rows.iter().map(|r| r.closed_pnl).collect();
        let fees: Vec<i64> = rows.iter().map(|r| r.fee).collect();
        let maker_addresses: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.address_m.as_bytes())).collect();
        let maker_oids: Vec<i64> = rows.iter().map(|r| r.oid_m as i64).collect();
        let maker_pnls: Vec<i64> = rows.iter().map(|r| r.pnl_m).collect();
        let maker_fees: Vec<i64> = rows.iter().map(|r| r.fee_m).collect();
        let maker_mm_rates: Vec<Option<i32>> = rows.iter().map(|r| r.mm_rate).collect();
        let (maker_mm_rate_values, maker_mm_rate_def_levels) = parse_optional_i32_column(&maker_mm_rates);
        let maker_mm_shares: Vec<Option<i64>> = rows.iter().map(|r| r.mm_share).collect();
        let (maker_mm_share_values, maker_mm_share_def_levels) = parse_optional_i64_column(&maker_mm_shares);
        let filltimes: Vec<Option<i32>> = rows.iter().map(|r| r.filltime).collect();
        let (filltime_values, filltime_def_levels) = parse_optional_i32_column(&filltimes);
        let tids: Vec<i64> = rows.iter().map(|r| r.tid as i64).collect();
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_time_millis, None, None)?;
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
                typed.write_batch(&addresses, None, None)?;
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
            if let ColumnWriter::ByteArrayColumnWriter(typed) = col.untyped() {
                typed.write_batch(&maker_addresses, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&maker_oids, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&maker_pnls, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&maker_fees, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&maker_mm_rate_values, Some(&maker_mm_rate_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&maker_mm_share_values, Some(&maker_mm_share_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int32ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&filltime_values, Some(&filltime_def_levels), None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&tids, None, None)?;
            }
            col.close()?;
        }
    } else {
        let pnls: Vec<i64> = rows.iter().map(|r| r.closed_pnl).collect();
        let fees: Vec<i64> = rows.iter().map(|r| r.fee).collect();
        let hashes: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.hash.as_bytes())).collect();
        let tids: Vec<i64> = rows.iter().map(|r| r.tid as i64).collect();
        let start_positions: Vec<i64> = rows.iter().map(|r| r.start_position).collect();
        let dirs: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.dir.as_bytes())).collect();
        let fee_tokens: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.fee_token.as_bytes())).collect();
        let twap_ids: Vec<Option<i64>> = rows.iter().map(|r| r.twap_id).collect();
        let (twap_id_values, twap_id_def_levels) = parse_optional_i64_column(&twap_ids);

        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_numbers, None, None)?;
            }
            col.close()?;
        }
        if let Some(mut col) = row_group.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
                typed.write_batch(&block_time_millis, None, None)?;
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
                typed.write_batch(&addresses, None, None)?;
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
                typed.write_batch(&tids, None, None)?;
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
                typed.write_batch(&twap_id_values, Some(&twap_id_def_levels), None)?;
            }
            col.close()?;
        }
    }

    row_group.close()?;
    trim_allocator();
    Ok(())
}

fn write_status_hft_columns(
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
        hash: _,
        order_type,
        sz,
        orig_sz,
        builder: _,
        trigger_condition,
        trigger_px,
        tp_trigger_px,
        sl_trigger_px,
        is_position_tpsl,
        reduce_only,
        cloid: _,
        time: _,
        timestamp: _,
    } = rows;
    let mut row_group = file.next_row_group()?;
    let coins = vec![byte_array_from_str(coin); block_number.len()];
    let block_time_millis = parse_timestamp_column_millis(&block_time)?;
    let sparse_orig_sz: Vec<Option<i64>> =
        sz.iter().zip(orig_sz.iter()).map(|(sz, orig_sz)| if sz == orig_sz { None } else { Some(*orig_sz) }).collect();
    let (orig_sz_values, orig_sz_def_levels) = parse_optional_i64_column(&sparse_orig_sz);
    let (tp_trigger_px_values, tp_trigger_px_def_levels) = parse_optional_i64_column(&tp_trigger_px);
    let (sl_trigger_px_values, sl_trigger_px_def_levels) = parse_optional_i64_column(&sl_trigger_px);

    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_number, None, None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time_millis, None, None)?;
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
            typed.write_batch(&user, None, None)?;
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
            typed.write_batch(&status, None, None)?;
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
            typed.write_batch(&orig_sz_values, Some(&orig_sz_def_levels), None)?;
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
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&tp_trigger_px_values, Some(&tp_trigger_px_def_levels), None)?;
        }
        col.close()?;
    }
    if let Some(mut col) = row_group.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&sl_trigger_px_values, Some(&sl_trigger_px_def_levels), None)?;
        }
        col.close()?;
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
    let block_time_millis = parse_timestamp_column_millis(&block_times)?;
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
        if let ColumnWriter::Int64ColumnWriter(typed) = col.untyped() {
            typed.write_batch(&block_time_millis, None, None)?;
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
    let mut effective_stop_height = current_archive_stop_height();
    let mut bootstrap_checkpoint_evaluated = false;
    let mut startup_recovery_prepared = false;
    let mut start_alignment_wait_logged = false;
    let mut last_input_height: Option<u64> = None;
    let mut replay_skip_log = ReplaySkipLogState::default();

    loop {
        set_archive_phase(last_input_height, "waiting_for_block");
        let mode = get_archive_mode();
        let symbols = current_archive_symbols();
        if mode != current_mode || symbols != current_symbols {
            set_archive_phase(last_input_height, "reconfigure");
            if let Some(mut w) = writers.take() {
                w.set_recover_blocks_fill_on_stop(current_archive_recover_blocks_fill_on_stop());
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
                    startup_recovery_prepared = false;
                    continue;
                }
            }
            writers = mode.map(|mode| {
                ArchiveWriters::new(mode, symbols.clone(), current_archive_symbol_decimals(), handoff_tx.clone())
            });
            current_mode = mode;
            current_symbols = symbols;
            effective_stop_height = current_archive_stop_height();
            bootstrap_checkpoint_evaluated = false;
            startup_recovery_prepared = false;
            start_alignment_wait_logged = false;
            last_input_height = None;
            replay_skip_log = ReplaySkipLogState::default();
        }

        let msg = match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(msg) => msg,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if stop.load(Ordering::SeqCst) {
            break;
        }

        if writers.is_none() {
            continue;
        }
        let mode = writers.as_ref().expect("writers checked above").mode;
        set_archive_phase(Some(msg.block_number), "drain_status_progress");
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

        set_archive_phase(Some(msg.block_number), "parse_batches");
        let payload = msg.payload.as_ref();
        let height = payload.block_number;
        if msg.block_number != height {
            warn!("Archive block header mismatch: msg {} payload {}", msg.block_number, height);
            continue;
        }
        if !payload.rejects.is_empty() {
            warn!("Archive payload unexpectedly included reject rows at {}", height);
        }

        if last_input_height.is_none() && current_archive_stop_height().is_some_and(|stop_height| stop_height < height)
        {
            info!(
                "Archive ignoring stale stop height {} because current height is {}",
                current_archive_stop_height().unwrap_or(height),
                height
            );
            archive_thread_context(|ctx| {
                if let Some(ctx) = ctx {
                    ctx.enabled.store(false, Ordering::SeqCst);
                    ctx.stop_requested.store(true, Ordering::SeqCst);
                }
            });
            break;
        }

        if !startup_recovery_prepared {
            if let Err(err) = preflight_startup_local_recovery(mode, &current_symbols, height, &handoff_tx) {
                warn!("Archive startup local recovery precheck failed at {height}: {err}");
            }
            startup_recovery_prepared = true;
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
            if effective_stop_height.is_none() {
                if let Some(archive_height) = current_archive_height_span() {
                    effective_stop_height = Some(height.saturating_add(archive_height).saturating_sub(1));
                }
            }
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
                spawn_mid_window_checkpoint_fetch(height, payload.block_time.clone(), bootstrap_symbols);
            }
        }

        let block_time = payload.block_time.clone();
        let order_n = payload.statuses.len() as i32;
        let diff_n = payload.diffs.len() as i32;
        let fill_n = payload.fills.len() as i32;
        set_archive_phase(Some(height), "build_status_rows");
        let mut status_rows: HashMap<String, StatusBlockBatch> = HashMap::new();
        let mut hft_status_rows = Vec::new();
        let mut same_block_status_timestamp_ms_by_oid: HashMap<(String, u64), i64> = HashMap::new();
        let mut same_block_filled_timestamp_ms_by_oid: HashMap<(String, u64), i64> = HashMap::new();
        let mut same_block_remove_status_by_oid: HashMap<(String, u64), HftRemoveStatusSummary> = HashMap::new();
        let mut btc_status_n = 0;
        let mut eth_status_n = 0;
        let block_time_bytes = byte_array_from_str(&block_time);
        for status in &payload.statuses {
            let coin = status.coin.clone();
            let side = protocol_side_str(status.side).to_string();
            let limit_px = status.limit_px.clone();
            let sz = status.sz.clone();
            let oid = status.oid;
            let timestamp = status.timestamp;
            let trigger_condition = status.trigger_condition.clone();
            let is_trigger = status.is_trigger;
            let trigger_px = status.trigger_px.clone();
            let is_position_tpsl = status.is_position_tpsl;
            let reduce_only = status.reduce_only;
            let order_type = status.order_type.clone();
            let orig_sz = status.orig_sz.clone();
            let tif = status.tif.clone();
            let cloid = status.cloid.clone();
            if coin == "BTC" {
                btc_status_n += 1;
            } else if coin == "ETH" {
                eth_status_n += 1;
            }
            if !writers.as_ref().expect("writers checked above").has_coin(&coin) {
                continue;
            }
            let scales = current_archive_coin_decimal_scales(&coin);
            let (s_px, s_sz, s_orig, s_trig) = if mode.uses_full_scaling() {
                (
                    parse_scaled(&limit_px, scales.px_scale).unwrap_or(0),
                    parse_scaled(&sz, scales.sz_scale).unwrap_or(0),
                    parse_scaled(&orig_sz, scales.sz_scale).unwrap_or(0),
                    parse_scaled(&trigger_px, scales.px_scale).unwrap_or(0),
                )
            } else {
                (
                    parse_scaled(&limit_px, scales.px_scale).unwrap_or(0),
                    parse_scaled(&sz, scales.sz_scale).unwrap_or(0),
                    parse_scaled(&orig_sz, scales.sz_scale).unwrap_or(0),
                    parse_scaled(&trigger_px, scales.px_scale).unwrap_or(0),
                )
            };
            let tp_child_trigger_px =
                status.tp_trigger_px.as_deref().and_then(|value| parse_scaled(value, scales.px_scale));
            let sl_child_trigger_px =
                status.sl_trigger_px.as_deref().and_then(|value| parse_scaled(value, scales.px_scale));
            let status_text = status.status.clone();
            let status_user = status.user.clone();
            if mode == ArchiveMode::Hft {
                let summary = same_block_remove_status_by_oid.entry((coin.clone(), oid)).or_default();
                if hft_status_is_filled(&status_text) {
                    summary.has_filled = true;
                } else if hft_status_is_canceled(&status_text) {
                    summary.canceled_sz = Some(s_sz);
                }
            }
            if mode == ArchiveMode::Hft {
                match infer_epoch_timestamp_millis(timestamp as i64) {
                    Ok(timestamp_ms) => {
                        same_block_status_timestamp_ms_by_oid.insert((coin.clone(), oid), timestamp_ms);
                        if hft_status_is_filled(&status_text) {
                            same_block_filled_timestamp_ms_by_oid.insert((coin.clone(), oid), timestamp_ms);
                        }
                    }
                    Err(err) => {
                        warn!(
                            "Skipping HFT same-block status timestamp for coin={} oid={} height={} due to invalid order timestamp: {}",
                            coin, oid, height, err
                        );
                    }
                }
            }
            if mode == ArchiveMode::Hft {
                hft_status_rows.push(HftStatusArchiveRow {
                    block_number: height,
                    block_time: block_time.clone(),
                    coin,
                    user: status_user,
                    oid,
                    status: status_text,
                    side,
                    limit_px: s_px,
                    sz: s_sz,
                    orig_sz: s_orig,
                    is_trigger,
                    tif: tif.unwrap_or_default(),
                    trigger_condition,
                    trigger_px: s_trig,
                    is_position_tpsl,
                    reduce_only,
                    order_type,
                    tp_trigger_px: tp_child_trigger_px,
                    sl_trigger_px: sl_child_trigger_px,
                });
                continue;
            }
            let entry = status_rows.entry(coin.clone()).or_insert_with(|| StatusBlockBatch::new(mode));
            match entry {
                StatusBlockBatch::Lite(columns) => {
                    columns.block_number.push(height as i64);
                    columns.block_time.push(block_time_bytes.clone());
                    columns.time.push(byte_array_from_string(status.time.clone()));
                    columns.user.push(byte_array_from_string(status_user.clone()));
                    columns.status.push(byte_array_from_string(status_text.clone()));
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
                }
                StatusBlockBatch::Full(columns) => {
                    columns.block_number.push(height as i64);
                    columns.block_time.push(block_time_bytes.clone());
                    columns.status.push(byte_array_from_string(status_text.clone()));
                    columns.oid.push(oid as i64);
                    columns.side.push(byte_array_from_string(side));
                    columns.limit_px.push(s_px);
                    columns.is_trigger.push(is_trigger);
                    columns.tif.push(byte_array_from_string(tif.unwrap_or_default()));
                    columns.user.push(byte_array_from_string(status_user.clone()));
                    columns.hash.push(byte_array_from_string(status.hash.clone()));
                    columns.order_type.push(byte_array_from_string(order_type));
                    columns.sz.push(s_sz);
                    columns.orig_sz.push(s_orig);
                    columns.time.push(byte_array_from_string(status.time.clone()));
                    columns.builder.push(byte_array_from_string(status.builder.clone()));
                    columns.timestamp.push(timestamp as i64);
                    columns.trigger_condition.push(byte_array_from_string(trigger_condition));
                    columns.trigger_px.push(s_trig);
                    columns.tp_trigger_px.push(None);
                    columns.sl_trigger_px.push(None);
                    columns.is_position_tpsl.push(is_position_tpsl);
                    columns.reduce_only.push(reduce_only);
                    columns.cloid.push(byte_array_from_string(cloid.unwrap_or_default()));
                }
                StatusBlockBatch::Hft(columns) => {
                    columns.block_number.push(height as i64);
                    columns.block_time.push(block_time_bytes.clone());
                    columns.status.push(byte_array_from_string(status_text.clone()));
                    columns.oid.push(oid as i64);
                    columns.side.push(byte_array_from_string(side));
                    columns.limit_px.push(s_px);
                    columns.is_trigger.push(is_trigger);
                    columns.tif.push(byte_array_from_string(tif.unwrap_or_default()));
                    columns.user.push(byte_array_from_string(status_user));
                    columns.hash.push(byte_array_from_string(status.hash.clone()));
                    columns.order_type.push(byte_array_from_string(order_type));
                    columns.sz.push(s_sz);
                    columns.orig_sz.push(s_orig);
                    columns.time.push(byte_array_from_string(status.time.clone()));
                    columns.builder.push(byte_array_from_string(status.builder.clone()));
                    columns.timestamp.push(timestamp as i64);
                    columns.trigger_condition.push(byte_array_from_string(trigger_condition));
                    columns.trigger_px.push(s_trig);
                    columns.tp_trigger_px.push(tp_child_trigger_px);
                    columns.sl_trigger_px.push(sl_child_trigger_px);
                    columns.is_position_tpsl.push(is_position_tpsl);
                    columns.reduce_only.push(reduce_only);
                    columns.cloid.push(byte_array_from_string(cloid.unwrap_or_default()));
                }
            }
        }

        set_archive_phase(Some(height), "build_diff_rows");
        let mut diff_rows: HashMap<String, Vec<DiffOut>> = HashMap::new();
        let mut btc_diff_n = 0;
        let mut eth_diff_n = 0;
        for diff in &payload.diffs {
            let user = diff.user.clone();
            let oid = diff.oid;
            let coin = diff.coin.clone();
            let side = protocol_side_str(diff.side).to_string();
            let px = diff.px.clone();
            if coin == "BTC" {
                btc_diff_n += 1;
            } else if coin == "ETH" {
                eth_diff_n += 1;
            }
            if !writers.as_ref().expect("writers checked above").has_coin(&coin) {
                continue;
            }
            let scales = current_archive_coin_decimal_scales(&coin);
            let (d_px, d_sz, d_orig, diff_type) = if mode.uses_full_scaling() {
                let (dt, sz, osz) = match &diff.raw_book_diff {
                    ParsedOrderDiff::New { sz } => (0u8, parse_scaled(sz, scales.sz_scale).unwrap_or(0), 0i64),
                    ParsedOrderDiff::Update { orig_sz, new_sz } => (
                        1u8,
                        parse_scaled(new_sz, scales.sz_scale).unwrap_or(0),
                        parse_scaled(orig_sz, scales.sz_scale).unwrap_or(0),
                    ),
                    ParsedOrderDiff::Remove => (2u8, 0, 0),
                };
                (parse_scaled(&px, scales.px_scale).unwrap_or(0), sz, osz, dt)
            } else {
                let (dt, sz, osz) = match &diff.raw_book_diff {
                    ParsedOrderDiff::New { sz } => (0u8, parse_scaled(sz, scales.sz_scale).unwrap_or(0), 0i64),
                    ParsedOrderDiff::Update { orig_sz, new_sz } => (
                        1u8,
                        parse_scaled(new_sz, scales.sz_scale).unwrap_or(0),
                        parse_scaled(orig_sz, scales.sz_scale).unwrap_or(0),
                    ),
                    ParsedOrderDiff::Remove => (2u8, 0, 0),
                };
                (parse_scaled(&px, scales.px_scale).unwrap_or(0), sz, osz, dt)
            };
            let out = DiffOut {
                block_number: height,
                block_time: block_time.clone(),
                coin: coin.clone(),
                oid,
                diff_type: diff_type_name(diff_type).to_string(),
                sz: d_sz,
                user,
                side,
                px: d_px,
                orig_sz: d_orig,
                event: String::new(),
                status: None,
                limit_px: None,
                _sz: None,
                _orig_sz: None,
                is_trigger: None,
                tif: String::new(),
                reduce_only: None,
                order_type: None,
                trigger_condition: None,
                trigger_px: None,
                is_position_tpsl: None,
                tp_trigger_px: None,
                sl_trigger_px: None,
                lifetime: None,
            };
            diff_rows.entry(coin).or_default().push(out);
        }

        if mode == ArchiveMode::Hft {
            let (residual_status_rows, migration_warnings) =
                migrate_hft_status_into_diffs(hft_status_rows, &mut diff_rows);
            for warning in migration_warnings {
                warn!(
                    "Skipping HFT status-to-diff migration for diff_type={} coin={} oid={} user={} height={} due to multiple matching status rows: {}",
                    warning.diff_type,
                    warning.coin,
                    warning.oid,
                    warning.user,
                    warning.block_number,
                    warning.statuses.join(",")
                );
            }
            for row in residual_status_rows {
                let entry = status_rows.entry(row.coin.clone()).or_insert_with(|| StatusBlockBatch::new(mode));
                match entry {
                    StatusBlockBatch::Hft(columns) => push_hft_status_row(columns, row),
                    _ => unreachable!("HFT residual status rows must target HFT status batch"),
                }
            }
        }

        set_archive_phase(Some(height), "build_fill_rows");
        let mut fill_rows: HashMap<String, Vec<FillOut>> = HashMap::new();
        let mut same_block_trade_sz_by_oid: HashMap<(String, u64), i64> = HashMap::new();
        let mut btc_fill_n = 0;
        let mut eth_fill_n = 0;
        let block_time_ms = if mode == ArchiveMode::Hft {
            match parse_timestamp_millis(&block_time) {
                Ok(value) => Some(value),
                Err(err) => {
                    warn!(
                        "Skipping HFT trade filltime for height={} due to invalid block_time '{}': {}",
                        height, block_time, err
                    );
                    None
                }
            }
        } else {
            None
        };
        for fill_event in &payload.fills {
            let coin = fill_event.coin.clone();
            let px = fill_event.px.clone();
            let sz = fill_event.sz.clone();
            let side = protocol_side_str(fill_event.side).to_string();
            let start_position = fill_event.start_position.clone();
            let dir = fill_event.dir.clone();
            let closed_pnl = fill_event.closed_pnl.clone();
            let hash = fill_event.hash.clone();
            let oid = fill_event.oid;
            let crossed = fill_event.crossed;
            let fee = fill_event.fee.clone();
            let tid = fill_event.tid;
            let fee_token = fill_event.fee_token.clone();
            let twap_id = fill_event.twap_id;
            if coin == "BTC" {
                btc_fill_n += 1;
            } else if coin == "ETH" {
                eth_fill_n += 1;
            }
            if !writers.as_ref().expect("writers checked above").has_coin(&coin) {
                continue;
            }
            let scales = current_archive_coin_decimal_scales(&coin);
            let (f_px, f_sz, f_pnl, f_fee, f_start) = if mode.uses_full_scaling() {
                (
                    parse_scaled(&px, scales.px_scale).unwrap_or(0),
                    parse_scaled(&sz, scales.sz_scale).unwrap_or(0),
                    parse_scaled(&closed_pnl, 6).unwrap_or(0),
                    parse_scaled(&fee, 6).unwrap_or(0),
                    parse_scaled(&start_position, 8).unwrap_or(0),
                )
            } else {
                (
                    parse_scaled(&px, scales.px_scale).unwrap_or(0),
                    parse_scaled(&sz, scales.sz_scale).unwrap_or(0),
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
                address: fill_event.user.clone(),
                closed_pnl: f_pnl,
                fee: f_fee,
                hash,
                oid,
                address_m: String::new(),
                oid_m: 0,
                pnl_m: 0,
                fee_m: 0,
                mm_rate: None,
                mm_share: None,
                filltime: None,
                tid,
                start_position: f_start,
                dir,
                fee_token,
                twap_id: twap_id.map(|value| value as i64),
            };
            fill_rows.entry(coin).or_default().push(out);
        }
        if mode == ArchiveMode::Hft {
            for rows in fill_rows.values_mut() {
                let aggregated = aggregate_hft_trades(std::mem::take(rows));
                let mut aggregated = aggregated;
                enrich_hft_trade_rows_with_user_fees(&mut aggregated);
                assign_hft_trade_filltimes(&mut aggregated, block_time_ms, &same_block_filled_timestamp_ms_by_oid);
                for trade in &aggregated {
                    *same_block_trade_sz_by_oid.entry((trade.coin.clone(), trade.oid_m)).or_default() += trade.sz;
                }
                *rows = aggregated;
            }
            for (coin, rows) in &mut diff_rows {
                let mut missing_remove_lifetime = 0u64;
                let mut missing_remove_terminal_status = 0u64;
                for row in rows {
                    let same_block_trade_sz = same_block_trade_sz_by_oid.get(&(coin.clone(), row.oid)).copied();
                    let diff_type = match row.diff_type.as_str() {
                        "new" => 0,
                        "update" => 1,
                        "remove" => 2,
                        _ => 255,
                    };
                    if diff_type == 2 {
                        let (event, canceled_orig_sz, should_warn) = classify_hft_remove_from_status_summary(
                            same_block_remove_status_by_oid.get(&(coin.clone(), row.oid)).copied(),
                        );
                        row.event = event.to_string();
                        if let Some(canceled_orig_sz) = canceled_orig_sz {
                            row.orig_sz = canceled_orig_sz;
                        }
                        if should_warn {
                            missing_remove_terminal_status += 1;
                        }
                    } else {
                        row.event =
                            classify_hft_diff_event(diff_type, row.sz, row.orig_sz, same_block_trade_sz).to_string();
                    }
                    row.lifetime = if diff_type == 2 {
                        match same_block_status_timestamp_ms_by_oid.get(&(coin.clone(), row.oid)).copied() {
                            Some(timestamp_ms) => block_time_ms
                                .and_then(|block_ms| block_ms.checked_sub(timestamp_ms))
                                .and_then(encode_hft_lifetime),
                            None => {
                                missing_remove_lifetime += 1;
                                None
                            }
                        }
                    } else {
                        None
                    };
                }
                if missing_remove_lifetime > 0 {
                    warn!(
                        "HFT diff remove lifetime missing same-block status timestamp for {} row(s) coin={} height={}",
                        missing_remove_lifetime, coin, height
                    );
                }
                if missing_remove_terminal_status > 0 {
                    warn!(
                        "HFT diff remove missing same-block filled/*canceled status for {} row(s) coin={} height={}",
                        missing_remove_terminal_status, coin, height
                    );
                }
            }
        }

        set_archive_phase(Some(height), "blocks_advance_append");
        let mut fatal_error: Option<String> = None;
        let writers_ref = writers.as_mut().expect("writers checked above");
        let shared_blocks_row = shared_blocks_row(
            height,
            block_time.clone(),
            order_n,
            diff_n,
            fill_n,
            btc_status_n,
            btc_diff_n,
            btc_fill_n,
            eth_status_n,
            eth_diff_n,
            eth_fill_n,
        );
        let append_blocks_result = {
            let mut shared = shared_blocks_index().lock().expect("shared blocks index");
            shared.append(shared_blocks_row, &handoff_tx)
        };
        if let Err(err) = append_blocks_result {
            disable_archive_after_failure(
                &mut writers,
                &mut current_mode,
                &mut current_symbols,
                height,
                &format!("blocks write failed: {err}"),
            );
            continue;
        }
        writers_ref.record_blocks_height(height);

        writers_ref.register_status_block(height);

        set_archive_phase(Some(height), "diff_send");
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
                set_archive_phase(Some(height), format!("fill_advance:{coin}"));
                if let Err(err) = coin_writers.fill.advance_block(mode, height, write_fill_rows) {
                    fatal_error = Some(format!("fill advance failed for {coin}: {err}"));
                    break;
                }
                let status_for_block = status_rows.remove(&coin).unwrap_or_else(|| StatusBlockBatch::new(mode));
                set_archive_phase(Some(height), format!("status_send:{coin}"));
                if let Err(err) = coin_writers.status.send_block(height, status_for_block) {
                    fatal_error = Some(format!("status write failed for {coin}: {err}"));
                    break;
                }
                if let Some(rows) = fill_rows.remove(&coin).filter(|rows| !rows.is_empty()) {
                    set_archive_phase(Some(height), format!("fill_append:{coin}"));
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
        set_archive_phase(Some(height), "drain_status_progress_post_block");
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
        set_archive_phase(Some(height), "block_complete");
        last_input_height = Some(height);

        if effective_stop_height.is_some_and(|stop_height| height >= stop_height) {
            info!("Archive auto-stopping after reaching configured stop height {}", height);
            archive_thread_context(|ctx| {
                if let Some(ctx) = ctx {
                    ctx.enabled.store(false, Ordering::SeqCst);
                    ctx.stop_requested.store(true, Ordering::SeqCst);
                }
            });
            break;
        }

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
                    startup_recovery_prepared = false;
                }
            }
        }
    }

    if let Some(mut w) = writers.take() {
        set_archive_phase(last_input_height, "close_all");
        w.set_recover_blocks_fill_on_stop(current_archive_recover_blocks_fill_on_stop());
        if let Err(err) = w.close_all() {
            warn!("Archive writer close failed: {err}");
            w.abort_all();
        }
    }
    let ArchiveHandoffWorker { tx: handoff_worker_tx, handle } = handoff_worker;
    set_archive_phase(last_input_height, "handoff_worker_join");
    drop(handoff_tx);
    drop(handoff_worker_tx);
    if let Err(err) = handle.join() {
        warn!("Archive handoff worker panicked: {err:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::channel;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Clone, Debug)]
    struct TestRow(u64);

    impl HasBlockNumber for TestRow {
        fn block_number(&self) -> u64 {
            self.0
        }
    }

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
        let recovery = Path::new("/tmp/BTC_fill_928200001_929000000.parquet.0");
        assert_eq!(parse_local_archive_filename_bounds(recovery, "BTC_fill"), Some((928200001, 929000000)));
    }

    #[test]
    fn split_recovered_rows_keeps_same_group_tail_active() {
        let rows = vec![TestRow(1001), TestRow(1002), TestRow(1003)];
        let state = split_recovered_rows_for_resume(StreamKind::Blocks, 1004, rows);
        assert!(state.committed_rows.is_empty());
        assert!(state.delayed_rows.is_empty());
        let active: Vec<u64> = state.active_rows.into_iter().map(|row| row.0).collect();
        assert_eq!(active, vec![1001, 1002, 1003]);
    }

    #[test]
    fn split_recovered_rows_restores_previous_group_as_delayed() {
        let rows = vec![TestRow(1), TestRow(250000)];
        let state = split_recovered_rows_for_resume(StreamKind::Blocks, 250001, rows);
        let committed: Vec<u64> = state.committed_rows.into_iter().map(|row| row.0).collect();
        let delayed: Vec<u64> = state.delayed_rows.into_iter().map(|row| row.0).collect();
        let active: Vec<u64> = state.active_rows.into_iter().map(|row| row.0).collect();
        assert!(committed.is_empty());
        assert_eq!(delayed, vec![1, 250000]);
        assert!(active.is_empty());
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
                    REQUIRED INT64 block_time (TIMESTAMP(MILLIS,true));\n\
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
            block_time: format!("2026-03-21T07:{:02}:00.000000Z", block_number % 60),
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

        let mut writer = ParquetStreamWriter::new(
            StreamKind::Blocks,
            schema.clone(),
            props.clone(),
            handoff_tx.clone(),
            current_archive_handoff_config(),
        );
        writer.advance_block(ArchiveMode::Lite, 1001, write_block_rows).expect("advance 1001");
        writer
            .append_rows("blocks", ArchiveMode::Lite, 1001, vec![make_row(1001)], write_block_rows)
            .expect("append 1001");
        writer.advance_block(ArchiveMode::Lite, 6000, write_block_rows).expect("advance 6000");
        writer
            .append_rows("blocks", ArchiveMode::Lite, 6000, vec![make_row(6000)], write_block_rows)
            .expect("append 6000");
        writer.close_with_flush(ArchiveMode::Lite, write_block_rows).expect("close first writer");

        let mut recovered = ParquetStreamWriter::new(
            StreamKind::Blocks,
            schema.clone(),
            props.clone(),
            handoff_tx.clone(),
            current_archive_handoff_config(),
        );
        recovered.advance_block(ArchiveMode::Lite, 1001, write_block_rows).expect("re-advance 1001");
        recovered
            .append_rows("blocks", ArchiveMode::Lite, 1001, vec![make_row(1001)], write_block_rows)
            .expect("re-append 1001");
        recovered.advance_block(ArchiveMode::Lite, 6001, write_block_rows).expect("advance 6001");
        recovered
            .append_rows("blocks", ArchiveMode::Lite, 6001, vec![make_row(6001)], write_block_rows)
            .expect("append 6001");
        recovered.close_with_flush(ArchiveMode::Lite, write_block_rows).expect("close recovered writer");

        let final_path = base_dir.join("blocks_1001_1000000.parquet.0");
        let rows = read_local_blocks_rows(&final_path).expect("read recovered blocks parquet");
        let heights: Vec<u64> = rows.iter().map(|row| row.block_number).collect();
        assert_eq!(heights, vec![1001, 6000, 6001]);

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
        let schema = fill_schema_for(ArchiveMode::Lite, ArchiveDecimalScales { px_scale: 8, sz_scale: 8 });
        let file = fs::File::create(&path).expect("create fill parquet");
        let mut writer =
            SerializedFileWriter::new(file, schema, std::sync::Arc::new(WriterProperties::builder().build()))
                .expect("create writer");
        let rows = vec![
            FillOut {
                block_number: 928200002,
                block_time: "2026-03-21T07:00:01.000000Z".to_string(),
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
                address_m: String::new(),
                oid_m: 0,
                pnl_m: 0,
                fee_m: 0,
                mm_rate: None,
                mm_share: None,
                filltime: None,
                tid: 0,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: None,
            },
            FillOut {
                block_number: 928200010,
                block_time: "2026-03-21T07:00:02.000000Z".to_string(),
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
                address_m: String::new(),
                oid_m: 0,
                pnl_m: 0,
                fee_m: 0,
                mm_rate: None,
                mm_share: None,
                filltime: None,
                tid: 0,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: None,
            },
        ];
        write_fill_rows(&mut writer, ArchiveMode::Lite, &rows).expect("write fill rows");
        writer.close().expect("close fill writer");

        let recovered = read_local_fill_rows(&path, ArchiveMode::Lite).expect("read fill rows");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].block_number, 928200002);
        assert_eq!(recovered[0].oid, 33);
        assert_eq!(recovered[1].block_number, 928200010);
        assert_eq!(recovered[1].oid, 77);

        fs::remove_dir_all(&dir).expect("cleanup temp dir");
    }

    #[test]
    fn fill_hft_local_reader_round_trips() {
        let _guard = test_lock();
        let dir = unique_temp_dir("fill_hft_reader");
        let path = dir.join("BTC_trade_HFT_928200001_929000000.parquet");
        let schema = fill_schema_for(ArchiveMode::Hft, ArchiveDecimalScales { px_scale: 2, sz_scale: 5 });
        let file = fs::File::create(&path).expect("create fill parquet");
        let mut writer =
            SerializedFileWriter::new(file, schema, std::sync::Arc::new(WriterProperties::builder().build()))
                .expect("create writer");
        let rows = vec![FillOut {
            block_number: 928200002,
            block_time: "2026-03-21T07:00:01.000000Z".to_string(),
            coin: "BTC".to_string(),
            side: "B".to_string(),
            px: 7071200,
            sz: 12345,
            crossed: true,
            address: "taker".to_string(),
            closed_pnl: 101,
            fee: 22,
            hash: String::new(),
            oid: 33,
            address_m: "maker".to_string(),
            oid_m: 44,
            pnl_m: 303,
            fee_m: 44,
            mm_rate: Some(-3),
            mm_share: Some(283),
            filltime: Some(41290),
            tid: 55,
            start_position: 0,
            dir: String::new(),
            fee_token: String::new(),
            twap_id: None,
        }];
        write_fill_rows(&mut writer, ArchiveMode::Hft, &rows).expect("write fill rows");
        writer.close().expect("close fill writer");

        let recovered = read_local_fill_rows(&path, ArchiveMode::Hft).expect("read fill rows");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].oid, 33);
        assert_eq!(recovered[0].oid_m, 44);
        assert_eq!(recovered[0].mm_rate, Some(-3));
        assert_eq!(recovered[0].mm_share, Some(283));
        assert_eq!(recovered[0].filltime, Some(41290));
        assert_eq!(recovered[0].tid, 55);

        fs::remove_dir_all(&dir).expect("cleanup temp dir");
    }

    #[test]
    fn diff_hft_local_reader_round_trips() {
        let _guard = test_lock();
        let dir = unique_temp_dir("diff_hft_reader");
        let path = dir.join("BTC_diff_HFT_928200001_929000000.parquet");
        let schema = diff_schema_for(ArchiveMode::Hft, ArchiveDecimalScales { px_scale: 2, sz_scale: 5 });
        let file = fs::File::create(&path).expect("create diff parquet");
        let mut writer =
            SerializedFileWriter::new(file, schema, std::sync::Arc::new(WriterProperties::builder().build()))
                .expect("create writer");
        let rows = vec![
            DiffOut {
                block_number: 928200002,
                block_time: "2026-03-21T07:00:01.000Z".to_string(),
                coin: "BTC".to_string(),
                oid: 33,
                diff_type: "new".to_string(),
                sz: 12345,
                user: "u1".to_string(),
                side: "B".to_string(),
                px: 7071200,
                orig_sz: 0,
                event: "add".to_string(),
                status: Some("open".to_string()),
                limit_px: Some(7071200),
                _sz: Some(12345),
                _orig_sz: Some(12345),
                is_trigger: Some(false),
                tif: "Alo".to_string(),
                reduce_only: Some(false),
                order_type: Some("Limit".to_string()),
                trigger_condition: Some("N/A".to_string()),
                trigger_px: Some(0),
                is_position_tpsl: Some(false),
                tp_trigger_px: Some(7075000),
                sl_trigger_px: None,
                lifetime: None,
            },
            DiffOut {
                block_number: 928200010,
                block_time: "2026-03-21T07:00:02.000Z".to_string(),
                coin: "BTC".to_string(),
                oid: 77,
                diff_type: "update".to_string(),
                sz: 10000,
                user: "u2".to_string(),
                side: "A".to_string(),
                px: 7071500,
                orig_sz: 12000,
                event: "fill".to_string(),
                status: None,
                limit_px: None,
                _sz: None,
                _orig_sz: None,
                is_trigger: None,
                tif: String::new(),
                reduce_only: None,
                order_type: None,
                trigger_condition: None,
                trigger_px: None,
                is_position_tpsl: None,
                tp_trigger_px: None,
                sl_trigger_px: None,
                lifetime: Some(281),
            },
        ];
        write_diff_rows(&mut writer, ArchiveMode::Hft, &rows).expect("write diff rows");
        writer.close().expect("close diff writer");

        let recovered = read_local_diff_rows(&path, ArchiveMode::Hft).expect("read diff rows");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].oid, 33);
        assert_eq!(recovered[0].event, "add");
        assert_eq!(recovered[0].status.as_deref(), Some("open"));
        assert_eq!(recovered[0].limit_px, Some(7071200));
        assert_eq!(recovered[0]._sz, Some(12345));
        assert_eq!(recovered[0]._orig_sz, Some(12345));
        assert_eq!(recovered[0].is_trigger, Some(false));
        assert_eq!(recovered[0].tif, "Alo");
        assert_eq!(recovered[0].reduce_only, Some(false));
        assert_eq!(recovered[0].order_type.as_deref(), Some("Limit"));
        assert_eq!(recovered[0].trigger_condition.as_deref(), Some("N/A"));
        assert_eq!(recovered[0].trigger_px, Some(0));
        assert_eq!(recovered[0].is_position_tpsl, Some(false));
        assert_eq!(recovered[0].tp_trigger_px, Some(7075000));
        assert_eq!(recovered[0].sl_trigger_px, None);
        assert_eq!(recovered[0].lifetime, None);
        assert_eq!(recovered[1].oid, 77);
        assert_eq!(recovered[1].event, "fill");
        assert_eq!(recovered[1].status, None);
        assert_eq!(recovered[1].tif, "");
        assert_eq!(recovered[1].reduce_only, None);
        assert_eq!(recovered[1].lifetime, Some(281));

        fs::remove_dir_all(&dir).expect("cleanup temp dir");
    }

    #[test]
    fn encode_hft_lifetime_switches_to_negative_minutes_after_five_minutes() {
        assert_eq!(encode_hft_lifetime(281), Some(281));
        assert_eq!(encode_hft_lifetime(299_999), Some(299_999));
        assert_eq!(encode_hft_lifetime(300_001), Some(-5));
        assert_eq!(encode_hft_lifetime(330_000), Some(-6));
        assert_eq!(encode_hft_lifetime(359_999), Some(-6));
    }

    #[test]
    fn hft_remove_status_summary_prefers_filled_over_canceled() {
        let summary = HftRemoveStatusSummary { has_filled: true, canceled_sz: Some(12345) };
        assert_eq!(classify_hft_remove_from_status_summary(Some(summary)), ("fill", None, false));
    }

    #[test]
    fn hft_remove_status_summary_uses_canceled_suffix_case_insensitively() {
        assert!(hft_status_is_canceled("reduceOnlyCanceled"));
        assert!(hft_status_is_canceled("scheduledcanceled"));
        assert!(hft_status_is_canceled("scheduledCancel"));
        assert!(!hft_status_is_canceled("filled"));
        assert_eq!(classify_hft_remove_from_status_summary(None), ("cancel", None, true));
    }

    #[test]
    fn migrate_hft_status_into_diffs_moves_unique_new_and_remove_rows() {
        let statuses = vec![
            HftStatusArchiveRow {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                status: "open".to_string(),
                side: "B".to_string(),
                limit_px: 7071200,
                sz: 12345,
                orig_sz: 12345,
                is_trigger: false,
                tif: "Alo".to_string(),
                trigger_condition: "N/A".to_string(),
                trigger_px: 0,
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Limit".to_string(),
                tp_trigger_px: None,
                sl_trigger_px: None,
            },
            HftStatusArchiveRow {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                status: "canceled".to_string(),
                side: "B".to_string(),
                limit_px: 7071200,
                sz: 0,
                orig_sz: 12345,
                is_trigger: false,
                tif: "Alo".to_string(),
                trigger_condition: "N/A".to_string(),
                trigger_px: 0,
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Limit".to_string(),
                tp_trigger_px: None,
                sl_trigger_px: None,
            },
        ];
        let mut diff_rows = HashMap::from([(
            "BTC".to_string(),
            vec![
                DiffOut {
                    block_number: 1,
                    block_time: "2026-03-25T00:00:10.000Z".to_string(),
                    coin: "BTC".to_string(),
                    user: "u1".to_string(),
                    oid: 11,
                    side: "B".to_string(),
                    px: 7071200,
                    diff_type: "new".to_string(),
                    sz: 12345,
                    orig_sz: 0,
                    event: "add".to_string(),
                    status: None,
                    limit_px: None,
                    _sz: None,
                    _orig_sz: None,
                    is_trigger: None,
                    tif: String::new(),
                    reduce_only: None,
                    order_type: None,
                    trigger_condition: None,
                    trigger_px: None,
                    is_position_tpsl: None,
                    tp_trigger_px: None,
                    sl_trigger_px: None,
                    lifetime: None,
                },
                DiffOut {
                    block_number: 1,
                    block_time: "2026-03-25T00:00:10.000Z".to_string(),
                    coin: "BTC".to_string(),
                    user: "u1".to_string(),
                    oid: 11,
                    side: "B".to_string(),
                    px: 7071200,
                    diff_type: "remove".to_string(),
                    sz: 0,
                    orig_sz: 12345,
                    event: "cancel".to_string(),
                    status: None,
                    limit_px: None,
                    _sz: None,
                    _orig_sz: None,
                    is_trigger: None,
                    tif: String::new(),
                    reduce_only: None,
                    order_type: None,
                    trigger_condition: None,
                    trigger_px: None,
                    is_position_tpsl: None,
                    tp_trigger_px: None,
                    sl_trigger_px: None,
                    lifetime: None,
                },
            ],
        )]);

        let (residual, warnings) = migrate_hft_status_into_diffs(statuses, &mut diff_rows);

        assert!(residual.is_empty());
        assert!(warnings.is_empty());
        let rows = diff_rows.get("BTC").expect("btc rows");
        assert_eq!(rows[0].status.as_deref(), Some("open"));
        assert_eq!(rows[0].limit_px, Some(7071200));
        assert_eq!(rows[0]._sz, Some(12345));
        assert_eq!(rows[0]._orig_sz, Some(12345));
        assert_eq!(rows[0].tif, "Alo");
        assert_eq!(rows[1].status.as_deref(), Some("canceled"));
        assert_eq!(rows[1]._orig_sz, Some(12345));
    }

    #[test]
    fn migrate_hft_status_into_diffs_prefers_open_over_triggered_for_new() {
        let statuses = vec![
            HftStatusArchiveRow {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                status: "open".to_string(),
                side: "B".to_string(),
                limit_px: 7071200,
                sz: 100,
                orig_sz: 100,
                is_trigger: false,
                tif: "Alo".to_string(),
                trigger_condition: "N/A".to_string(),
                trigger_px: 0,
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Limit".to_string(),
                tp_trigger_px: None,
                sl_trigger_px: None,
            },
            HftStatusArchiveRow {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                status: "triggered".to_string(),
                side: "B".to_string(),
                limit_px: 7071200,
                sz: 100,
                orig_sz: 100,
                is_trigger: true,
                tif: String::new(),
                trigger_condition: "triggered".to_string(),
                trigger_px: 7071200,
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Stop Limit".to_string(),
                tp_trigger_px: None,
                sl_trigger_px: None,
            },
        ];
        let mut diff_rows = HashMap::from([(
            "BTC".to_string(),
            vec![DiffOut {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                side: "B".to_string(),
                px: 7071200,
                diff_type: "new".to_string(),
                sz: 100,
                orig_sz: 0,
                event: "add".to_string(),
                status: None,
                limit_px: None,
                _sz: None,
                _orig_sz: None,
                is_trigger: None,
                tif: String::new(),
                reduce_only: None,
                order_type: None,
                trigger_condition: None,
                trigger_px: None,
                is_position_tpsl: None,
                tp_trigger_px: None,
                sl_trigger_px: None,
                lifetime: None,
            }],
        )]);

        let (residual, warnings) = migrate_hft_status_into_diffs(statuses, &mut diff_rows);

        assert!(warnings.is_empty());
        assert!(residual.is_empty());
        let row = &diff_rows.get("BTC").expect("btc rows")[0];
        assert_eq!(row.status.as_deref(), Some("open"));
        assert_eq!(row.limit_px, Some(7071200));
        assert_eq!(row.tif, "Alo");
    }

    #[test]
    fn migrate_hft_status_into_diffs_warns_and_keeps_rows_when_new_has_multiple_open_candidates() {
        let statuses = vec![
            HftStatusArchiveRow {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                status: "open".to_string(),
                side: "B".to_string(),
                limit_px: 7071200,
                sz: 100,
                orig_sz: 100,
                is_trigger: false,
                tif: "Alo".to_string(),
                trigger_condition: "N/A".to_string(),
                trigger_px: 0,
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Limit".to_string(),
                tp_trigger_px: None,
                sl_trigger_px: None,
            },
            HftStatusArchiveRow {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                status: "open".to_string(),
                side: "B".to_string(),
                limit_px: 7071200,
                sz: 100,
                orig_sz: 100,
                is_trigger: false,
                tif: "Alo".to_string(),
                trigger_condition: "N/A".to_string(),
                trigger_px: 0,
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Limit".to_string(),
                tp_trigger_px: None,
                sl_trigger_px: None,
            },
        ];
        let mut diff_rows = HashMap::from([(
            "BTC".to_string(),
            vec![DiffOut {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                user: "u1".to_string(),
                oid: 11,
                side: "B".to_string(),
                px: 7071200,
                diff_type: "new".to_string(),
                sz: 100,
                orig_sz: 0,
                event: "add".to_string(),
                status: None,
                limit_px: None,
                _sz: None,
                _orig_sz: None,
                is_trigger: None,
                tif: String::new(),
                reduce_only: None,
                order_type: None,
                trigger_condition: None,
                trigger_px: None,
                is_position_tpsl: None,
                tp_trigger_px: None,
                sl_trigger_px: None,
                lifetime: None,
            }],
        )]);

        let (residual, warnings) = migrate_hft_status_into_diffs(statuses, &mut diff_rows);

        assert_eq!(residual.len(), 2);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].diff_type, "new");
        assert_eq!(warnings[0].statuses, vec!["open".to_string(), "open".to_string()]);
        let row = &diff_rows.get("BTC").expect("btc rows")[0];
        assert_eq!(row.status, None);
        assert_eq!(row.limit_px, None);
        assert_eq!(row.tif, "");
    }

    #[test]
    fn assign_hft_trade_filltimes_only_marks_last_same_block_filled_trade() {
        let mut trades = vec![
            FillOut {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                side: "B".to_string(),
                px: 100,
                sz: 1,
                crossed: true,
                address: "taker_a".to_string(),
                closed_pnl: 0,
                fee: 0,
                hash: String::new(),
                oid: 11,
                address_m: "maker".to_string(),
                oid_m: 77,
                pnl_m: 0,
                fee_m: 0,
                mm_rate: None,
                mm_share: None,
                filltime: Some(123),
                tid: 1,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: None,
            },
            FillOut {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                side: "B".to_string(),
                px: 101,
                sz: 2,
                crossed: true,
                address: "taker_b".to_string(),
                closed_pnl: 0,
                fee: 0,
                hash: String::new(),
                oid: 12,
                address_m: "maker".to_string(),
                oid_m: 77,
                pnl_m: 0,
                fee_m: 0,
                mm_rate: None,
                mm_share: None,
                filltime: Some(456),
                tid: 2,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: None,
            },
            FillOut {
                block_number: 1,
                block_time: "2026-03-25T00:00:10.000Z".to_string(),
                coin: "BTC".to_string(),
                side: "B".to_string(),
                px: 102,
                sz: 3,
                crossed: true,
                address: "taker_c".to_string(),
                closed_pnl: 0,
                fee: 0,
                hash: String::new(),
                oid: 13,
                address_m: "maker".to_string(),
                oid_m: 88,
                pnl_m: 0,
                fee_m: 0,
                mm_rate: None,
                mm_share: None,
                filltime: Some(789),
                tid: 3,
                start_position: 0,
                dir: String::new(),
                fee_token: String::new(),
                twap_id: None,
            },
        ];
        let same_block_filled_timestamp_ms_by_oid =
            HashMap::from([(("BTC".to_string(), 77_u64), 9_000_i64), (("BTC".to_string(), 88_u64), 8_500_i64)]);

        assign_hft_trade_filltimes(&mut trades, Some(10_000), &same_block_filled_timestamp_ms_by_oid);

        assert_eq!(trades[0].filltime, None);
        assert_eq!(trades[1].filltime, Some(1_000));
        assert_eq!(trades[2].filltime, Some(1_500));
    }

    #[test]
    fn status_hft_local_reader_round_trips() {
        let _guard = test_lock();
        let dir = unique_temp_dir("status_hft_reader");
        let path = dir.join("BTC_status_HFT_928200001_929000000.parquet");
        let schema = status_schema_for(ArchiveMode::Hft, ArchiveDecimalScales { px_scale: 2, sz_scale: 5 });
        let file = fs::File::create(&path).expect("create status parquet");
        let mut writer =
            SerializedFileWriter::new(file, schema, std::sync::Arc::new(WriterProperties::builder().build()))
                .expect("create writer");
        let mut rows = StatusFullColumns::default();
        rows.block_number.extend([928200002, 928200010].map(|v| v as i64));
        rows.block_time
            .extend([byte_array_from_str("2026-03-21T07:00:01.000Z"), byte_array_from_str("2026-03-21T07:00:02.000Z")]);
        rows.status.extend([byte_array_from_str("open"), byte_array_from_str("canceled")]);
        rows.oid.extend([33, 77]);
        rows.side.extend([byte_array_from_str("B"), byte_array_from_str("A")]);
        rows.limit_px.extend([7071200, 7071500]);
        rows.is_trigger.extend([false, true]);
        rows.tif.extend([byte_array_from_str("Alo"), byte_array_from_str("Gtc")]);
        rows.user.extend([byte_array_from_str("u1"), byte_array_from_str("u2")]);
        rows.order_type.extend([byte_array_from_str("Limit"), byte_array_from_str("Stop Market")]);
        rows.sz.extend([12345, 54321]);
        rows.orig_sz.extend([12345, 60000]);
        rows.trigger_condition.extend([byte_array_from_str("N/A"), byte_array_from_str("Price below")]);
        rows.trigger_px.extend([0, 7069900]);
        rows.tp_trigger_px.extend([Some(7075000), None]);
        rows.sl_trigger_px.extend([None, Some(7069900)]);
        rows.is_position_tpsl.extend([false, true]);
        rows.reduce_only.extend([false, true]);
        write_status_hft_columns(&mut writer, "BTC", rows).expect("write status rows");
        writer.close().expect("close status writer");

        let recovered = read_local_hft_status_rows(&path).expect("read status rows");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].oid, 33);
        assert_eq!(recovered[0].orig_sz, 12345);
        assert_eq!(recovered[0].tif, "Alo");
        assert_eq!(recovered[0].tp_trigger_px, Some(7075000));
        assert_eq!(recovered[1].oid, 77);
        assert_eq!(recovered[1].orig_sz, 60000);
        assert_eq!(recovered[1].reduce_only, true);
        assert_eq!(recovered[1].sl_trigger_px, Some(7069900));

        fs::remove_dir_all(&dir).expect("cleanup temp dir");
    }

    #[test]
    fn worker_threads_inherit_session_rotation_blocks() {
        let _guard = test_lock();
        let previous_base_dir = current_archive_base_dir();
        let previous_handoff = current_archive_handoff_config();
        let base_dir = unique_temp_dir("rotation_ctx");
        set_archive_base_dir(Some(base_dir.clone()));
        set_archive_handoff_config(ArchiveHandoffConfig::new(false, Some(base_dir.clone()), false, None));
        set_rotation_blocks(500_000);

        let enabled = Arc::new(AtomicBool::new(true));
        let stop_requested = Arc::new(AtomicBool::new(false));
        let phase_state = Arc::new(Mutex::new(ArchivePhaseState::default()));
        let recover_blocks_fill_on_stop = Arc::new(AtomicBool::new(false));
        let options = ArchiveSessionOptions {
            mode: ArchiveMode::Hft,
            rotation_blocks: 250_000,
            output_dir: base_dir.clone(),
            symbols: vec!["BTC".to_string()],
            symbol_decimals: HashMap::new(),
            archive_height: None,
            stop_height: None,
            align_start_to_10k_boundary: true,
            align_output_to_1000_boundary: true,
            handoff: current_archive_handoff_config(),
        };
        let thread_context = ArchiveThreadContext::from_options(
            &options,
            base_dir.clone(),
            enabled,
            stop_requested,
            phase_state,
            recover_blocks_fill_on_stop,
        );
        set_archive_thread_context(Some(thread_context.clone()));

        let inherited = archive_thread_context(|ctx| ctx.cloned());
        let worker_rotation = std::thread::spawn(move || {
            set_archive_thread_context(inherited);
            let rotation = current_rotation_blocks();
            set_archive_thread_context(None);
            rotation
        })
        .join()
        .expect("worker join");

        set_archive_thread_context(None);
        set_archive_handoff_config(previous_handoff);
        set_archive_base_dir(Some(previous_base_dir));
        fs::remove_dir_all(&base_dir).expect("cleanup temp dir");

        assert_eq!(worker_rotation, 250_000);
    }

    #[test]
    fn timestamp_millis_parser_accepts_rfc3339_and_naive_iso() {
        let z = parse_timestamp_millis("2026-03-22T06:14:19.685Z").expect("parse rfc3339");
        let naive = parse_timestamp_millis("2026-03-22T06:14:19.685").expect("parse naive iso");
        assert_eq!(z, naive);
        assert_eq!(format_timestamp_millis(z).expect("format millis"), "2026-03-22T06:14:19.685Z");
    }

    #[test]
    fn parse_scaled_rounds_benign_float_tails() {
        assert_eq!(parse_scaled("526.2344799999", 6), Some(526_234_480));
        assert_eq!(parse_scaled("-684.2497800001", 6), Some(-684_249_780));
        assert_eq!(parse_scaled("-14448.7409919999", 6), Some(-14_448_740_992));
    }
}
