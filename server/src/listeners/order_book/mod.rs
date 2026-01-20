use crate::{
    HL_NODE,
    listeners::order_book::state::OrderBookState,
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json_filtered},
    },
    prelude::*,
    types::{
        SnapshotOrder,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use log::{error, info, warn};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{BufRead, BufReader},
    mem::size_of,
    os::unix::{net::UnixStream, io::AsRawFd},
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{Sender as MpscSender, UnboundedSender, channel, unbounded_channel},
    },
    time::{Instant, interval, sleep},
};
use tokio_cron_scheduler::{Job, JobScheduler};
use utils::{process_rmp_file, validate_snapshot_hash};

mod state;
mod utils;
pub(crate) mod lite;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let dropped_updates = Arc::new(AtomicBool::new(false));
    let warmup_deadline = {
        let listener = listener.lock().await;
        listener.warmup_deadline()
    };
    info!("Listening for merged stream at {}", UDS_PATH);

    let (stream_tx, mut stream_rx) = channel::<AlignedMessage>(FIFO_QUEUE_MAX_LEN);
    spawn_uds_reader(
        stream_tx.clone(),
        dropped_updates.clone(),
        warmup_deadline.clone(),
    );
    let parser_listener = listener.clone();
    let parser_dropped_updates = dropped_updates.clone();
    tokio::spawn(async move {
        while let Some(message) = stream_rx.recv().await {
            if parser_dropped_updates.swap(false, AtomicOrdering::SeqCst) {
                let mut listener = parser_listener.lock().await;
                listener.reset_after_drop();
            }
            if let Err(err) = parser_listener.lock().await.process_aligned_lines(message) {
                error!("Stream processing error: {err}");
            }
        }
        error!("Stream channel closed. Parser exiting");
    });

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();
    let (snapshot_request_tx, mut snapshot_request_rx) = unbounded_channel::<()>();
    let snapshot_inflight = {
        let mut listener = listener.lock().await;
        listener.set_snapshot_requester(snapshot_request_tx);
        listener.snapshot_request_inflight()
    };
    let snapshot_pending = {
        let listener = listener.lock().await;
        listener.snapshot_request_pending()
    };

    // Initialize Scheduler
    let sched = JobScheduler::new().await.map_err(|e| format!("Failed to create scheduler: {e}"))?;
    
    // Create the job to run at 30th second of minute 0 and 30 (twice per hour)
    let job_dir = dir.clone();
    let job_listener = listener.clone();
    let job_tx = snapshot_fetch_task_tx.clone();
    let job_inflight = snapshot_inflight.clone();
    let job = Job::new("30 0,30 * * * *", move |_uuid, _l| {
        let listener = job_listener.clone();
        let snapshot_fetch_task_tx = job_tx.clone();
        trigger_snapshot_fetch(job_dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot, job_inflight.clone());
    }).map_err(|e| format!("Failed to create cron job: {e}"))?;

    sched.add(job).await.map_err(|e| format!("Failed to add job: {e}"))?;
    sched.start().await.map_err(|e| format!("Failed to start scheduler: {e}"))?;

    info!("Cron scheduler started: snapshot validation set to run at xx:00:30 and xx:30:30.");
    
    // Trigger initial snapshot fetch (prefetch cache 200ms before request)
    info!("Triggering initial snapshot fetch");
    {
        let mut listener = listener.lock().await;
        listener.start_rebuild();
        listener.request_snapshot_rebuild();
    }

    let mut health_check = interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        warn!("Snapshot fetch task sender dropped; backing off for 60s.");
                        schedule_snapshot_retry(
                            dir.clone(),
                            listener.clone(),
                            snapshot_fetch_task_tx.clone(),
                            ignore_spot,
                            snapshot_inflight.clone(),
                        );
                    }
                    Some(Err(err)) => {
                        warn!("Snapshot fetch failed: {err}. Backing off for 60s.");
                        schedule_snapshot_retry(
                            dir.clone(),
                            listener.clone(),
                            snapshot_fetch_task_tx.clone(),
                            ignore_spot,
                            snapshot_inflight.clone(),
                        );
                    }
                    Some(Ok(())) => {
                        snapshot_inflight.store(false, AtomicOrdering::SeqCst);
                        if snapshot_pending.swap(false, AtomicOrdering::SeqCst) {
                            trigger_snapshot_fetch_after_delay(
                                dir.clone(),
                                listener.clone(),
                                snapshot_fetch_task_tx.clone(),
                                ignore_spot,
                                snapshot_inflight.clone(),
                            );
                        }
                    }
                }
            }
            snapshot_request = snapshot_request_rx.recv() => {
                if snapshot_request.is_some() {
                    if snapshot_inflight.load(AtomicOrdering::SeqCst) {
                        snapshot_pending.store(true, AtomicOrdering::SeqCst);
                    } else {
                        trigger_snapshot_fetch_after_delay(
                            dir.clone(),
                            listener.clone(),
                            snapshot_fetch_task_tx.clone(),
                            ignore_spot,
                            snapshot_inflight.clone(),
                        );
                    }
                }
            }
            _ = health_check.tick() => {
                let listener = listener.lock().await;
                if listener.is_ready() && listener.is_stalled(Duration::from_secs(5)) {
                    warn!("Stream has fallen behind ({HL_NODE} failed?). Continuing to wait.");
                }
            }
        }
    }
}

fn trigger_snapshot_fetch(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
    inflight: Arc<AtomicBool>,
) {
    if inflight.swap(true, AtomicOrdering::SeqCst) {
        return;
    }
    fetch_snapshot(dir, listener, tx, ignore_spot);
}

fn trigger_snapshot_fetch_after_delay(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
    inflight: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        sleep(Duration::from_millis(INITIAL_SNAPSHOT_PREFETCH_MS)).await;
        trigger_snapshot_fetch(dir, listener, tx, ignore_spot, inflight);
    });
}

fn schedule_snapshot_retry(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
    inflight: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(60)).await;
        inflight.store(false, AtomicOrdering::SeqCst);
        trigger_snapshot_fetch(dir, listener, tx, ignore_spot, inflight);
    });
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let request_start = Instant::now();
        let res = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let request_ms = request_start.elapsed().as_secs_f64() * 1000.0;
                info!("Snapshot request completed in {:.3} ms", request_ms);
                let state = {
                    let mut listener = listener.lock().await;
                    if listener.initializing || listener.order_book_state.is_none() {
                        None
                    } else {
                        listener.begin_caching();
                        listener.clone_state()
                    }
                };
                let parse_start = Instant::now();
                let snapshot =
                    load_snapshots_from_json_filtered::<InnerL4Order, (Address, SnapshotOrder)>(&output_fln, &MAIN_COINS)
                        .await;
                let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
                info!("Snapshot parse completed in {:.3} ms", parse_ms);
                let initializing = {
                    let listener = listener.lock().await;
                    listener.initializing || listener.order_book_state.is_none()
                };
                let mut cache = VecDeque::new();
                if state.is_some() && !initializing {
                    // sleep to let some updates build up for validation
                    sleep(Duration::from_millis(200)).await;
                    cache = {
                        let mut listener = listener.lock().await;
                        listener.take_cache()
                    };
                    info!("Cache has {} elements", cache.len());
                } else if state.is_some() {
                    let mut listener = listener.lock().await;
                    let _unused = listener.take_cache();
                }
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        info!("Snapshot parsed at height {}", height);
                        if initializing || state.is_none() {
                            let initialized = {
                                let mut listener = listener.lock().await;
                                listener.init_from_snapshot(expected_snapshot, height)
                            };
                            if initialized {
                                let inflight = {
                                    let listener = listener.lock().await;
                                    listener.snapshot_request_inflight()
                                };
                                schedule_snapshot_validation(
                                    dir.clone(),
                                    listener.clone(),
                                    tx.clone(),
                                    ignore_spot,
                                    inflight,
                                );
                            }
                            Ok(())
                        } else if let Some(mut state) = state {
                            let mut validation_cache = cache.clone();
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = validation_cache.pop_front() {
                                    state.apply_updates(order_statuses, order_diffs)?;
                                } else {
                                    return Err::<(), Error>("Not enough cached updates".into());
                                }
                            }
                            if state.height() > height {
                                return Err("Fetched snapshot lagging stored state".into());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating L4 snapshot hash");

                            let stored_snapshot_for_validation = stored_snapshot.clone();
                            let expected_snapshot_for_validation = expected_snapshot.clone();
                            let validation = tokio::task::spawn_blocking(move || {
                                validate_snapshot_hash(&stored_snapshot_for_validation, &expected_snapshot_for_validation, ignore_spot)
                            })
                            .await;

                            match validation {
                                Ok(Ok(())) => {
                                    info!("L4Book snapshot validated successfully at height {}", height);

                                    struct LiteValidationIssue {
                                        coin: Coin,
                                        snapshot: Option<Snapshot<InnerL4Order>>,
                                        expected_hash: Option<u64>,
                                        actual_hash: u64,
                                        initialized: bool,
                                    }

                                    let (lite_inputs, lite_state) = {
                                        let listener = listener.lock().await;
                                        let lite_inputs = listener
                                            .lite_books
                                            .iter()
                                            .map(|(coin, lite_book)| {
                                                let initialized = lite_book.is_initialized();
                                                let actual_hash = lite_book.snapshot_hash();
                                                (coin.clone(), (initialized, actual_hash))
                                            })
                                            .collect::<HashMap<_, _>>();
                                        let lite_state = listener.order_book_state.clone();
                                        (lite_inputs, lite_state)
                                    };
                                    let lite_coin_keys: Vec<Coin> = lite_inputs.keys().cloned().collect();

                                    if !lite_inputs.is_empty() {
                                        match lite_state {
                                            Some(lite_state) => {
                                                info!("Validating L4Lite snapshots against L4Book state");
                                                let lite_validation = tokio::task::spawn_blocking(move || -> std::result::Result<(u64, Vec<LiteValidationIssue>), Error> {
                                                    let validation_height = lite_state.height();
                                                    let l4_snapshot = lite_state.compute_snapshot().snapshot;

                                                    let mut issues = Vec::new();
                                                    for (coin, (initialized, actual_hash)) in lite_inputs {
                                                        if let Some(coin_snapshot) = l4_snapshot.as_ref().get(&coin) {
                                                            let expected_hash = lite::expected_snapshot_hash(coin_snapshot);
                                                            if !initialized || actual_hash != expected_hash {
                                                                issues.push(LiteValidationIssue {
                                                                    coin,
                                                                    snapshot: Some(coin_snapshot.clone()),
                                                                    expected_hash: Some(expected_hash),
                                                                    actual_hash,
                                                                    initialized,
                                                                });
                                                            }
                                                        } else {
                                                            issues.push(LiteValidationIssue {
                                                                coin,
                                                                snapshot: None,
                                                                expected_hash: None,
                                                                actual_hash,
                                                                initialized,
                                                            });
                                                        }
                                                    }
                                                    Ok((validation_height, issues))
                                                })
                                                .await;

                                                match lite_validation {
                                                    Ok(Ok((validation_height, issues))) => {
                                                        if issues.is_empty() {
                                                            info!("L4Lite snapshots validated successfully at height {}", validation_height);
                                                            for coin in &lite_coin_keys {
                                                                info!(
                                                                    "L4Lite snapshot status for {}: OK",
                                                                    coin.value()
                                                                );
                                                            }
                                                        } else {
                                                            let mut issue_coins = HashSet::new();
                                                            let mut rebuilds = Vec::new();
                                                            for issue in issues {
                                                                issue_coins.insert(issue.coin.clone());
                                                                match issue.snapshot {
                                                                    Some(snapshot) => {
                                                                        let expected_hash = issue.expected_hash.unwrap_or_default();
                                                                        warn!(
                                                                            "L4Lite snapshot status for {}: FAIL: expected hash {}, actual hash {}, initialized {}. Rebuilding from L4Book state.",
                                                                            issue.coin.value(),
                                                                            expected_hash,
                                                                            issue.actual_hash,
                                                                            issue.initialized
                                                                        );
                                                                        rebuilds.push((issue.coin, snapshot));
                                                                    }
                                                                    None => {
                                                                        warn!(
                                                                            "L4Lite snapshot status for {}: FAIL: missing L4Book snapshot",
                                                                            issue.coin.value(),
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                            for coin in &lite_coin_keys {
                                                                if issue_coins.contains(coin) {
                                                                    continue;
                                                                }
                                                                info!(
                                                                    "L4Lite snapshot status for {}: OK",
                                                                    coin.value()
                                                                );
                                                            }

                                                            if !rebuilds.is_empty() {
                                                                let mut rebuilt = false;
                                                                let mut listener_guard = listener.lock().await;
                                                                for (coin, snapshot) in rebuilds {
                                                                    let lite_book = listener_guard
                                                                        .lite_books
                                                                        .entry(coin.clone())
                                                                        .or_insert_with(lite::BookState::new);
                                                                    lite_book.init_from_snapshot(&snapshot, validation_height);
                                                                    rebuilt = true;
                                                                }
                                                                drop(listener_guard);
                                                                if rebuilt {
                                                                    let inflight = {
                                                                        let listener = listener.lock().await;
                                                                        listener.snapshot_request_inflight()
                                                                    };
                                                                    schedule_snapshot_validation(
                                                                        dir.clone(),
                                                                        listener.clone(),
                                                                        tx.clone(),
                                                                        ignore_spot,
                                                                        inflight,
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Ok(Err(err)) => {
                                                        warn!("L4Lite validation failed: {}", err);
                                                    }
                                                    Err(err) => {
                                                        warn!("L4Lite validation task failed: {}", err);
                                                    }
                                                }
                                            }
                                            None => {
                                                warn!("Skipping L4Lite validation: L4Book state unavailable");
                                            }
                                        }
                                    }

                                    Ok(())
                                }
                                Ok(Err(err)) => {
                                    warn!("L4Book snapshot hash mismatch at height {}: {}", height, err);
                                    let mut listener_guard = listener.lock().await;
                                    let schedule_validation = listener_guard.init_from_snapshot(expected_snapshot, height);
                                    let rebuilt_from_snapshot = listener_guard.order_book_state.is_some();

                                    let mut replay_failed = false;
                                    while let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                        let target_height = order_statuses.block_number();

                                        if let Some(book) = listener_guard.order_book_state.as_mut() {
                                            let mut state_statuses = order_statuses.clone();
                                            state_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));

                                            let mut state_diffs = order_diffs.clone();
                                            state_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));

                                            if let Err(err) = book.apply_updates(state_statuses, state_diffs) {
                                                warn!(
                                                    "Failed to replay cached updates at height {} after snapshot rebuild: {}",
                                                    target_height,
                                                    err
                                                );
                                                replay_failed = true;
                                                break;
                                            }
                                        }
                                    }

                                    let inflight = listener_guard.snapshot_request_inflight();
                                    if replay_failed {
                                        listener_guard.reset_after_drop();
                                    } else if rebuilt_from_snapshot {
                                        listener_guard.rebuild_lite_from_state();
                                    }
                                    drop(listener_guard);
                                    if schedule_validation {
                                        schedule_snapshot_validation(
                                            dir.clone(),
                                            listener.clone(),
                                            tx.clone(),
                                            ignore_spot,
                                            inflight,
                                        );
                                    }
                                    Ok(())
                                }
                                Err(err) => {
                                    warn!("L4Book snapshot validation task failed at height {}: {}", height, err);
                                    let mut listener_guard = listener.lock().await;
                                    let schedule_validation = listener_guard.init_from_snapshot(expected_snapshot, height);
                                    let rebuilt_from_snapshot = listener_guard.order_book_state.is_some();
                                    let mut replay_failed = false;
                                    while let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                        let target_height = order_statuses.block_number();

                                        if let Some(book) = listener_guard.order_book_state.as_mut() {
                                            let mut state_statuses = order_statuses.clone();
                                            state_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));

                                            let mut state_diffs = order_diffs.clone();
                                            state_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));

                                            if let Err(err) = book.apply_updates(state_statuses, state_diffs) {
                                                warn!(
                                                    "Failed to replay cached updates at height {} after snapshot rebuild: {}",
                                                    target_height,
                                                    err
                                                );
                                                replay_failed = true;
                                                break;
                                            }
                                        }
                                    }

                                    let inflight = listener_guard.snapshot_request_inflight();
                                    if replay_failed {
                                        listener_guard.reset_after_drop();
                                    } else if rebuilt_from_snapshot {
                                        listener_guard.rebuild_lite_from_state();
                                    }
                                    drop(listener_guard);
                                    if schedule_validation {
                                        schedule_snapshot_validation(
                                            dir.clone(),
                                            listener.clone(),
                                            tx.clone(),
                                            ignore_spot,
                                            inflight,
                                        );
                                    }
                                    Ok(())
                                }
                            }
                        } else {
                            Ok(())
                        }
                    }
                    Err(err) => {
                        if initializing {
                            let mut listener = listener.lock().await;
                            listener.clear_pending_aligned();
                        }
                        Err(err)
                    }
                }
            }
            Err(err) => {
                let initializing = {
                    let listener = listener.lock().await;
                    listener.initializing || listener.order_book_state.is_none()
                };
                if initializing {
                    let mut listener = listener.lock().await;
                    listener.clear_pending_aligned();
                }
                Err(err)
            }
        };
        let _unused = tx.send(res);
        Ok(())
    });
}

fn schedule_snapshot_validation(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
    inflight: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(59)).await;
        trigger_snapshot_fetch(dir, listener, tx, ignore_spot, inflight);
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_event_time: Option<Instant>,
    pending_aligned: VecDeque<AlignedTriple>,
    pending_window_start: Option<u64>,
    pending_drop_warned: bool,
    last_stream_height: Option<u64>,
    initializing: bool,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    snapshot_cache_dropped: bool,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    active_coins: Option<Arc<Mutex<HashMap<String, usize>>>>,
    pub lite_books: HashMap<Coin, lite::BookState>,
    warmup_deadline: Arc<AtomicU64>,
    snapshot_request_tx: Option<UnboundedSender<()>>,
    snapshot_request_inflight: Arc<AtomicBool>,
    snapshot_request_pending: Arc<AtomicBool>,
}

const FIFO_QUEUE_MAX_LEN: usize = 255;
const PENDING_ALIGNED_MAX_LEN: usize = 512;
const SNAPSHOT_CACHE_MAX_LEN: usize = 512;
const UDS_PATH: &str = "/home/aimee/hl_runtime/hl_book/fifo_listener.sock";
const UDS_SOCKET_BUFFER: i32 = 16 * 1024 * 1024;
const MAIN_COINS: [&str; 2] = ["BTC", "ETH"];
const WARMUP_MS: u64 = 250;
const INITIAL_SNAPSHOT_PREFETCH_MS: u64 = 200;

type AlignedTriple = (
    Batch<NodeDataOrderStatus>,
    Batch<NodeDataOrderDiff>,
    Batch<NodeDataFill>,
);

struct AlignedMessage {
    fills_line: String,
    diffs_line: String,
    order_line: String,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>, 
        active_coins: Option<Arc<Mutex<HashMap<String, usize>>>>,
        ignore_spot: bool
    ) -> Self {
        let warmup_deadline = Arc::new(AtomicU64::new(now_millis() + WARMUP_MS));
        let snapshot_request_inflight = Arc::new(AtomicBool::new(false));
        let snapshot_request_pending = Arc::new(AtomicBool::new(false));
        Self {
            ignore_spot,
            order_book_state: None,
            last_event_time: None,
            fetched_snapshot_cache: None,
            snapshot_cache_dropped: false,
            internal_message_tx,
            pending_aligned: VecDeque::new(),
            pending_window_start: None,
            pending_drop_warned: false,
            last_stream_height: None,
            initializing: false,
            active_coins,
            lite_books: HashMap::new(),
            warmup_deadline,
            snapshot_request_tx: None,
            snapshot_request_inflight,
            snapshot_request_pending,
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    fn reset_after_drop(&mut self) {
        warn!("Dropped FIFO messages; resetting state and waiting for next snapshot.");
        self.order_book_state = None;
        self.initializing = true;
        self.clear_pending_aligned();
        self.last_stream_height = None;
        self.fetched_snapshot_cache = None;
        self.snapshot_cache_dropped = false;
        self.bump_warmup_deadline();
        self.request_snapshot_rebuild();
    }

    fn is_stalled(&self, threshold: Duration) -> bool {
        match self.last_event_time {
            Some(last_event_time) => last_event_time.elapsed() >= threshold,
            None => false,
        }
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    fn warmup_deadline(&self) -> Arc<AtomicU64> {
        self.warmup_deadline.clone()
    }

    fn bump_warmup_deadline(&self) {
        self.warmup_deadline.store(now_millis() + WARMUP_MS, AtomicOrdering::SeqCst);
    }

    fn set_snapshot_requester(&mut self, tx: UnboundedSender<()>) {
        self.snapshot_request_tx = Some(tx);
    }

    fn snapshot_request_inflight(&self) -> Arc<AtomicBool> {
        self.snapshot_request_inflight.clone()
    }

    fn snapshot_request_pending(&self) -> Arc<AtomicBool> {
        self.snapshot_request_pending.clone()
    }

    fn request_snapshot_rebuild(&self) -> bool {
        let Some(tx) = &self.snapshot_request_tx else {
            return false;
        };
        if !self.snapshot_request_inflight.load(AtomicOrdering::SeqCst) {
            let _unused = tx.send(());
            return true;
        }
        self.snapshot_request_pending.store(true, AtomicOrdering::SeqCst);
        false
    }

    fn start_rebuild(&mut self) -> bool {
        if self.initializing {
            return false;
        }
        self.initializing = true;
        self.order_book_state = None;
        self.clear_pending_aligned();
        self.fetched_snapshot_cache = None;
        self.snapshot_cache_dropped = false;
        true
    }

    fn clear_pending_aligned(&mut self) {
        self.pending_aligned.clear();
        self.pending_window_start = None;
        self.pending_drop_warned = false;
    }


    fn queue_pending_aligned(&mut self, aligned: AlignedTriple) {
        if let Some(window_start) = self.pending_window_start {
            if aligned.0.block_number() < window_start {
                if !self.pending_drop_warned {
                    warn!(
                        "Pending aligned window advanced; dropping batches below height {window_start}"
                    );
                    self.pending_drop_warned = true;
                }
                return;
            }
        }
        self.pending_aligned.push_back(aligned);
    }

    fn update_pending_window(&mut self, height: u64) {
        self.last_stream_height = Some(height);
        if self.order_book_state.is_some() {
            return;
        }
        let window = PENDING_ALIGNED_MAX_LEN.saturating_sub(1) as u64;
        let window_start = height.saturating_sub(window);
        self.pending_window_start = Some(window_start);
        let mut dropped = false;
        while matches!(
            self.pending_aligned.front(),
            Some(front) if front.0.block_number() < window_start
        ) {
            self.pending_aligned.pop_front();
            dropped = true;
        }
        if dropped && !self.pending_drop_warned {
            warn!(
                "Pending aligned window advanced; dropping batches below height {window_start}"
            );
            self.pending_drop_warned = true;
        }
    }

    fn process_aligned_lines(&mut self, message: AlignedMessage) -> Result<()> {
        if now_millis() < self.warmup_deadline.load(AtomicOrdering::SeqCst) {
            return Ok(());
        }
        self.last_event_time = Some(Instant::now());

        let order_statuses = match serde_json::from_str::<Batch<NodeDataOrderStatus>>(&message.order_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!(
                    "OrderStatuses serialization error {err}, height: {:?}, drop bad stream: {:?}",
                    self.order_book_state.as_ref().map(OrderBookState::height),
                    &message.order_line[..message.order_line.len().min(100)],
                );
                return Ok(());
            }
        };
        let order_diffs = match serde_json::from_str::<Batch<NodeDataOrderDiff>>(&message.diffs_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!(
                    "OrderDiffs serialization error {err}, height: {:?}, drop bad stream: {:?}",
                    self.order_book_state.as_ref().map(OrderBookState::height),
                    &message.diffs_line[..message.diffs_line.len().min(100)],
                );
                return Ok(());
            }
        };
        let fills = match serde_json::from_str::<Batch<NodeDataFill>>(&message.fills_line) {
            Ok(batch) => batch,
            Err(err) => {
                warn!(
                    "Fills serialization error {err}, height: {:?}, drop bad stream: {:?}",
                    self.order_book_state.as_ref().map(OrderBookState::height),
                    &message.fills_line[..message.fills_line.len().min(100)],
                );
                return Ok(());
            }
        };

        let height = order_statuses.block_number();
        if order_diffs.block_number() != height || fills.block_number() != height {
            warn!(
                "Aligned payload height mismatch: order {}, diffs {}, fills {}",
                height,
                order_diffs.block_number(),
                fills.block_number()
            );
            return Ok(());
        }
        self.update_pending_window(height);

        self.process_aligned_batches(order_statuses, order_diffs, fills)
    }

    fn process_aligned_batches(
        &mut self,
        mut order_statuses: Batch<NodeDataOrderStatus>,
        mut order_diffs: Batch<NodeDataOrderDiff>,
        mut fills: Batch<NodeDataFill>,
    ) -> Result<()> {
        let active_coins = if let Some(am) = &self.active_coins {
            match am.try_lock() {
                Ok(g) => g.clone(),
                Err(_) => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        order_statuses.retain(|ev| {
            let coin = &ev.order.coin;
            MAIN_COINS.contains(&coin.as_str()) || active_coins.contains_key(coin)
        });
        order_diffs.retain(|ev| {
            let coin = ev.coin();
            MAIN_COINS.contains(&coin.value().as_str()) || active_coins.contains_key(coin.value().as_str())
        });
        fills.retain(|ev| {
            // NodeDataFill(pub Address, pub Fill) -> ev.1 is Fill
            let coin = &ev.1.coin;
            MAIN_COINS.contains(&coin.as_str()) || active_coins.contains_key(coin)
        });

        if self.initializing || self.order_book_state.is_none() {
            self.queue_pending_aligned((order_statuses, order_diffs, fills));
            return Ok(());
        }

        self.apply_aligned_batches(order_statuses, order_diffs, fills)
    }

    fn apply_aligned_batches(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
        fills: Batch<NodeDataFill>,
    ) -> Result<()> {
        let target_height = order_statuses.block_number();

        // Main coins update state
        let mut state_statuses = order_statuses.clone();
        state_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));

        let mut state_diffs = order_diffs.clone();
        state_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));

        // L4Lite Updates
        let mut all_lite_updates = Vec::new();
        let mut all_analysis_updates_b = Vec::new();
        let mut all_analysis_updates_a = Vec::new();
        let mut all_analysis_rollup_b = Vec::new();
        let mut all_analysis_rollup_a = Vec::new();
        let mut all_analysis_rollup_sum_b: Vec<(Coin, [String; 4])> = Vec::new();
        let mut all_analysis_rollup_sum_a: Vec<(Coin, [String; 4])> = Vec::new();

        let mut coin_statuses: HashMap<Coin, Vec<NodeDataOrderStatus>> = HashMap::new();
        for status in state_statuses.clone().events() {
            coin_statuses.entry(Coin::new(&status.order.coin)).or_default().push(status);
        }

        let mut coin_diffs: HashMap<Coin, Vec<NodeDataOrderDiff>> = HashMap::new();
        for diff in state_diffs.clone().events() {
            coin_diffs.entry(diff.coin()).or_default().push(diff);
        }

        let mut coin_fills: HashMap<Coin, Vec<NodeDataFill>> = HashMap::new();
        for fill in fills.clone().events() {
            coin_fills.entry(Coin::new(&fill.1.coin)).or_default().push(fill);
        }

        let mut lite_errors: Vec<(Coin, u64, String)> = Vec::new();
        let mut lite_needs_rebuild = false;
        for (coin, lb) in &mut self.lite_books {
            if !lb.is_initialized() {
                continue;
            }
            let statuses = coin_statuses.remove(coin).unwrap_or_default();
            let diffs = coin_diffs.remove(coin).unwrap_or_default();
            let fills = coin_fills.remove(coin).unwrap_or_default();

            match lb.process_block(coin.clone(), &statuses, &diffs, &fills, target_height) {
                Ok(block_event) => {
                    if let Some(du) = block_event.depth_updates {
                        all_lite_updates.push(du);
                    }
                    if !block_event.analysis_updates_b.is_empty() {
                        all_analysis_updates_b.extend(block_event.analysis_updates_b);
                    }
                    if !block_event.analysis_updates_a.is_empty() {
                        all_analysis_updates_a.extend(block_event.analysis_updates_a);
                    }
                    if !block_event.analysis_rollup_b.is_empty() {
                        all_analysis_rollup_b.extend(block_event.analysis_rollup_b);
                    }
                    if !block_event.analysis_rollup_a.is_empty() {
                        all_analysis_rollup_a.extend(block_event.analysis_rollup_a);
                    }
                    if let Some(sum_b) = block_event.analysis_rollup_sum_b {
                        all_analysis_rollup_sum_b.push((coin.clone(), sum_b));
                    }
                    if let Some(sum_a) = block_event.analysis_rollup_sum_a {
                        all_analysis_rollup_sum_a.push((coin.clone(), sum_a));
                    }
                }
                Err(err) => {
                    lite_errors.push((coin.clone(), target_height, err.to_string()));
                    lb.reset();
                }
            }
        }

        if !lite_errors.is_empty() {
            lite_needs_rebuild = true;
            for (coin, target_height, err) in &lite_errors {
                warn!(
                    "L4Lite out of sync for {} at height {}: {}. Rebuilding from L4Book state.",
                    coin.value(),
                    target_height,
                    err
                );
            }
            // Skip sending partial L4Lite updates; rebuild after L4Book applies this block.
        }

        // Always send L4Lite message to propagate block height, even if empty
        if lite_errors.is_empty() {
            if let Some(tx) = &self.internal_message_tx {
                let analysis_rollup_height = if target_height % 10 == 0 {
                    Some(target_height)
                } else {
                    None
                };
                let msg = Arc::new(InternalMessage::L4Lite {
                    updates: all_lite_updates,
                    analysis_b: all_analysis_updates_b,
                    analysis_a: all_analysis_updates_a,
                    analysis_rollup_b: all_analysis_rollup_b,
                    analysis_rollup_a: all_analysis_rollup_a,
                    analysis_rollup_sum_b: all_analysis_rollup_sum_b,
                    analysis_rollup_sum_a: all_analysis_rollup_sum_a,
                    analysis_rollup_height,
                    height: target_height,
                });
                let _unused = tx.send(msg);
            }
        }

        if let Err(err) = self
            .order_book_state
            .as_mut()
            .map(|book| book.apply_updates(state_statuses, state_diffs))
            .transpose()
        {
            let started = self.start_rebuild();
            let requested = if started {
                self.request_snapshot_rebuild()
            } else {
                false
            };
            if started && requested {
                warn!("L4Book out of sync: {}. Triggering snapshot rebuild.", err);
            } else {
                warn!("L4Book out of sync: {}. Snapshot rebuild already queued.", err);
            }
            self.queue_pending_aligned((order_statuses, order_diffs, fills));
            return Ok(());
        }

        if lite_needs_rebuild {
            self.rebuild_lite_from_state();
        }

        if let Some(cache) = &mut self.fetched_snapshot_cache {
            let mut cache_statuses = order_statuses.clone();
            cache_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));

            let mut cache_diffs = order_diffs.clone();
            cache_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));

            if cache.len() >= SNAPSHOT_CACHE_MAX_LEN {
                cache.pop_front();
                if !self.snapshot_cache_dropped {
                    warn!(
                        "Snapshot cache exceeded {SNAPSHOT_CACHE_MAX_LEN} entries; dropping oldest updates"
                    );
                    self.snapshot_cache_dropped = true;
                }
            }
            cache.push_back((cache_statuses, cache_diffs));
        }

        if let Some(tx) = &self.internal_message_tx {
            let updates = Arc::new(InternalMessage::L4BookUpdates {
                diff_batch: order_diffs,
                status_batch: order_statuses,
            });
            let _unused = tx.send(updates);

            let fills_msg = Arc::new(InternalMessage::Fills { batch: fills });
            let _unused = tx.send(fills_msg);
        }

        Ok(())
    }

    fn begin_caching(&mut self) {
        if self.fetched_snapshot_cache.is_some() {
            return;
        }
        self.fetched_snapshot_cache = Some(VecDeque::new());
        self.snapshot_cache_dropped = false;
        if let Some(height) = self.last_stream_height {
            let window = PENDING_ALIGNED_MAX_LEN.saturating_sub(1) as u64;
            self.pending_window_start = Some(height.saturating_sub(window));
        }
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.snapshot_cache_dropped = false;
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) -> bool {
        // We only maintain LOCAL state for MAIN_COINS.
        // Filter the snapshot before creating OrderBookState.
        let mut books = snapshot.value();
        books.retain(|coin, _| MAIN_COINS.contains(&coin.value().as_str()));

        info!("No existing snapshot");
        while matches!(
            self.pending_aligned.front(),
            Some(front) if front.0.block_number() <= height
        ) {
            self.pending_aligned.pop_front();
        }
        let mut init_height = height;
        if let Some(front) = self.pending_aligned.front() {
            let expected = height.saturating_add(1);
            let first = front.0.block_number();
            if first > expected {
                info!(
                    "Pending aligned gap after snapshot: expected {}, found {}. Fast-forwarding init height to {}",
                    expected,
                    first,
                    first.saturating_sub(1)
                );
                init_height = first.saturating_sub(1);
            }
        }

        // Initialize Lite books
        self.lite_books.clear();
        for (coin, orders) in &books {
            let mut lb = lite::BookState::new();
            lb.init_from_snapshot(orders, init_height);
            self.lite_books.insert(coin.clone(), lb);
        }

        let snapshot = Snapshots::new(books);

        let mut new_order_book =
            OrderBookState::from_snapshot(snapshot, init_height, 0, true, self.ignore_spot);
        let mut retry = false;
        let mut lite_needs_rebuild = false;
        while let Some((order_statuses, order_diffs, fills)) = self.pending_aligned.pop_front() {
            // Filter cached updates for MAIN_COINS before applying to initial state.
            // pending_aligned already stores Active | Main batches.
            
            let mut state_statuses = order_statuses.clone();
            state_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));
            
            let mut state_diffs = order_diffs.clone();
            state_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));

            let mut state_fills = fills.clone();
            state_fills.retain(|ev| MAIN_COINS.contains(&ev.1.coin.as_str()));

            // Update LiteBooks
            // We need to calculate target height here too
            let target_height = order_statuses.block_number();

            let mut coin_statuses: HashMap<Coin, Vec<NodeDataOrderStatus>> = HashMap::new();
            for status in state_statuses.clone().events() {
                coin_statuses.entry(Coin::new(&status.order.coin)).or_default().push(status);
            }
            
            let mut coin_diffs: HashMap<Coin, Vec<NodeDataOrderDiff>> = HashMap::new();
            for diff in state_diffs.clone().events() {
                coin_diffs.entry(diff.coin()).or_default().push(diff);
            }
            
            let mut coin_fills: HashMap<Coin, Vec<NodeDataFill>> = HashMap::new();
            for fill in state_fills.clone().events() {
                 coin_fills.entry(Coin::new(&fill.1.coin)).or_default().push(fill);
            }

            let mut lite_errors: Vec<(Coin, u64, String)> = Vec::new();
            for (coin, lb) in &mut self.lite_books {
                 if !lb.is_initialized() {
                     continue;
                 }
                 let statuses = coin_statuses.remove(coin).unwrap_or_default();
                 let diffs = coin_diffs.remove(coin).unwrap_or_default();
                 let fills = coin_fills.remove(coin).unwrap_or_default();
                 
                 if let Err(err) = lb.process_block(coin.clone(), &statuses, &diffs, &fills, target_height) {
                     lite_errors.push((coin.clone(), target_height, err.to_string()));
                     lb.reset();
                 }
            }

            if !lite_errors.is_empty() {
                lite_needs_rebuild = true;
                for (coin, target_height, err) in lite_errors {
                    warn!(
                        "L4Lite out of sync for {} at height {} during snapshot catch-up: {}. Rebuilding from L4Book state.",
                        coin.value(),
                        target_height,
                        err
                    );
                }
            }

            if new_order_book.apply_updates(state_statuses, state_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        let initialized = !retry;
        self.clear_pending_aligned();
        self.initializing = !initialized;
        if initialized {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
            if lite_needs_rebuild {
                self.rebuild_lite_from_state();
            } else {
                self.broadcast_rebuilt_snapshots();
            }
        } else {
            self.request_snapshot_rebuild();
        }
        initialized
    }

    fn rebuild_lite_from_state(&mut self) {
        let Some(book) = self.order_book_state.as_ref() else {
            return;
        };
        let height = book.height();
        let snapshot = book.compute_snapshot().snapshot;

        self.lite_books.clear();
        for (coin, orders) in snapshot.as_ref() {
            if !MAIN_COINS.contains(&coin.value().as_str()) {
                continue;
            }
            let mut lb = lite::BookState::new();
            lb.init_from_snapshot(orders, height);
            self.lite_books.insert(coin.clone(), lb);
        }

        self.broadcast_rebuilt_snapshots();
    }

    fn broadcast_rebuilt_snapshots(&self) {
        let Some(tx) = &self.internal_message_tx else {
            return;
        };
        let Some(book) = self.order_book_state.as_ref() else {
            return;
        };

        let active_filter = self
            .active_coins
            .as_ref()
            .and_then(|active| active.try_lock().ok())
            .map(|active| active.keys().cloned().collect::<HashSet<String>>());
        if let Some(active) = active_filter.as_ref() {
            if active.is_empty() {
                return;
            }
        }

        let TimedSnapshots { time, height, snapshot } = book.compute_snapshot();
        let mut snapshot_map = snapshot.value();
        if let Some(active) = active_filter.as_ref() {
            snapshot_map.retain(|coin, _| active.contains(coin.value().as_str()));
        }
        let l4_snapshot = Snapshots::new(snapshot_map);

        let mut lite_snapshots = HashMap::new();
        for (coin, lb) in &self.lite_books {
            if let Some(active) = active_filter.as_ref() {
                if !active.contains(coin.value().as_str()) {
                    continue;
                }
            }
            if lb.is_initialized() {
                lite_snapshots.insert(coin.clone(), lb.get_snapshot());
            }
        }

        let msg = InternalMessage::SnapshotRebuilt {
            time,
            height,
            l4_snapshot,
            lite_snapshots,
        };
        let _unused = tx.send(Arc::new(msg));
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    #[allow(dead_code)]
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    #[allow(dead_code)]
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    SnapshotRebuilt {
        time: u64,
        height: u64,
        l4_snapshot: Snapshots<InnerL4Order>,
        lite_snapshots: HashMap<Coin, lite::L2Snapshot>,
    },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
    L4Lite {
        updates: Vec<lite::L2BlockUpdate>,
        analysis_b: Vec<lite::AnalysisUpdate>,
        analysis_a: Vec<lite::AnalysisUpdate>,
        analysis_rollup_b: Vec<lite::AnalysisUpdate>,
        analysis_rollup_a: Vec<lite::AnalysisUpdate>,
        analysis_rollup_sum_b: Vec<(Coin, [String; 4])>,
        analysis_rollup_sum_a: Vec<(Coin, [String; 4])>,
        analysis_rollup_height: Option<u64>,
        height: u64,
    },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn split_merged_array(payload: &[u8]) -> Option<[&[u8]; 3]> {
    let mut idx = 0;
    while idx < payload.len() && payload[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if idx >= payload.len() || payload[idx] != b'[' {
        return None;
    }
    idx += 1;
    let mut parts: Vec<&[u8]> = Vec::with_capacity(3);
    while parts.len() < 3 {
        while idx < payload.len()
            && (payload[idx].is_ascii_whitespace() || payload[idx] == b',')
        {
            idx += 1;
        }
        if idx >= payload.len() || payload[idx] != b'{' {
            return None;
        }
        let start = idx;
        let mut depth = 0i32;
        let mut in_str = false;
        let mut escaped = false;
        while idx < payload.len() {
            let b = payload[idx];
            if in_str {
                if escaped {
                    escaped = false;
                } else if b == b'\\' {
                    escaped = true;
                } else if b == b'"' {
                    in_str = false;
                }
                idx += 1;
                continue;
            }
            match b {
                b'"' => {
                    in_str = true;
                    idx += 1;
                }
                b'{' => {
                    depth += 1;
                    idx += 1;
                }
                b'}' => {
                    depth -= 1;
                    idx += 1;
                    if depth == 0 {
                        parts.push(&payload[start..idx]);
                        break;
                    }
                }
                _ => idx += 1,
            }
        }
        if depth != 0 {
            return None;
        }
    }
    if parts.len() == 3 {
        Some([parts[0], parts[1], parts[2]])
    } else {
        None
    }
}

#[allow(unsafe_code)]
fn spawn_uds_reader(
    tx: MpscSender<AlignedMessage>,
    dropped_updates: Arc<AtomicBool>,
    warmup_deadline: Arc<AtomicU64>,
) {
    thread::spawn(move || loop {
        // 1. Connect using UnixStream (Stream mode)
        let stream = match UnixStream::connect(UDS_PATH) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to connect to UDS {UDS_PATH}: {e}");
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        if let Err(e) = stream.set_nonblocking(false) {
            error!("Failed to set UDS blocking mode: {e}");
        }

        let fd = stream.as_raw_fd();
        let buf_size = UDS_SOCKET_BUFFER;
        let buf_ptr: *const i32 = &buf_size;
        let buf_ptr = buf_ptr.cast::<libc::c_void>();
        let buf_len = size_of::<i32>() as libc::socklen_t;

        // 2. Set Receive Buffer
        unsafe {
            libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, buf_ptr, buf_len);
        }
        
        info!("Connected to merged stream at {UDS_PATH}");
        
        let mut reader = BufReader::new(stream);
        let mut line_buffer = Vec::new();

        // 3. Reader Loop
        loop {
            // Check warmup deadline
            let is_warmup = now_millis() < warmup_deadline.load(AtomicOrdering::SeqCst);

            line_buffer.clear();
            match reader.read_until(b'\n', &mut line_buffer) {
                Ok(0) => {
                    warn!("UDS connection closed (EOF); reconnecting");
                    break;
                }
                Ok(_) => {
                    process_message(&line_buffer, &tx, &dropped_updates, is_warmup);
                }
                Err(e) => {
                    error!("UDS read error: {e}");
                    break;
                }
            }
        }

        thread::sleep(Duration::from_secs(1));
    });
}

fn process_message(
    buffer: &[u8], 
    tx: &MpscSender<AlignedMessage>, 
    dropped_updates: &Arc<AtomicBool>,
    is_warmup: bool,
) {
    // Remove trailing newline if present for parsing
    let payload = if buffer.ends_with(&[b'\n']) {
        &buffer[..buffer.len()-1]
    } else {
        buffer
    };

    if payload.is_empty() { return; }

    let Some(parts) = split_merged_array(payload) else {
        // Log only if it's not a trivial empty line or something, acts as filter
        if payload.len() > 10 {
            warn!("Failed to split merged payload (len={})", payload.len());
        }
        return;
    };

    let message = AlignedMessage {
        fills_line: String::from_utf8_lossy(parts[0]).to_string(),
        diffs_line: String::from_utf8_lossy(parts[1]).to_string(),
        order_line: String::from_utf8_lossy(parts[2]).to_string(),
    };

    if !is_warmup {
         // Optimization: check drop flag loosely before trying (already present in original logic)
         if dropped_updates.load(AtomicOrdering::SeqCst) {
             // In original logic, once dropped, it seemed to persist? 
             // Or maybe it was just a transient aligned batch drop.
             // We'll mimic sending attempt which can update the flag.
         }
         
         if let Err(err) = tx.try_send(message) {
            if !dropped_updates.swap(true, AtomicOrdering::SeqCst) {
                 warn!("UDS queue full; dropping aligned batch: {err}");
            }
         }
    }
}
