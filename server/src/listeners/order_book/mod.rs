use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_slice},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use chrono::{Timelike, Utc};
use fs::File;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::thread;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, sleep, sleep_until},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency_with_hashes};

mod state;
mod utils;

#[allow(variant_size_differences)]
enum SnapshotFetchOutcome {
    Validated { height: u64 },
    Initialize { height: u64, expected_snapshot: Snapshots<InnerL4Order> },
    Skipped { local_height: u64, snapshot_height: u64 },
}

const SNAPSHOT_CACHE_DELAY_MS: u64 = 100;
const NEW_FILE_WINDOW_MS: i64 = 60_000;

fn next_snapshot_instant() -> Instant {
    let now = Utc::now();
    let base = now.with_second(30).unwrap_or(now);
    let at_half_hour = base.with_minute(30).unwrap_or(base);
    let at_hour = base.with_minute(0).unwrap_or(base);
    let target = if now <= at_hour {
        at_hour
    } else if now <= at_half_hour {
        at_half_hour
    } else {
        let next_hour = now + chrono::Duration::hours(1);
        next_hour
            .with_minute(0)
            .and_then(|next| next.with_second(30))
            .unwrap_or(next_hour)
    };
    let duration = target.signed_duration_since(now);
    let wait = duration.to_std().unwrap_or_else(|_| Duration::from_secs(0));
    Instant::now() + wait
}

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(
    listener: Arc<Mutex<OrderBookListener>>,
    dir: PathBuf,
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let mut next_snapshot = Box::pin(sleep_until(next_snapshot_instant()));
    let mut startup_snapshot = Some(Box::pin(sleep(Duration::from_secs(59))));
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        let mut listener_guard = listener.lock().await;
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener_guard
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener_guard
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener_guard
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                        if !listener_guard.is_ready() {
                            listener_guard.request_snapshot();
                        }
                        let snapshot_requested = listener_guard.take_snapshot_request();
                        let should_fetch = snapshot_requested && listener_guard.begin_snapshot_fetch();
                        drop(listener_guard);
                        if should_fetch {
                            let listener = listener.clone();
                            let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                            let active_symbols = active_symbols.clone();
                            fetch_snapshot(listener, snapshot_fetch_task_tx, ignore_spot, active_symbols);
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            () = &mut next_snapshot => {
                let should_fetch = {
                    let mut listener_guard = listener.lock().await;
                    listener_guard.begin_snapshot_fetch()
                };
                if should_fetch {
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    let active_symbols = active_symbols.clone();
                    fetch_snapshot(listener, snapshot_fetch_task_tx, ignore_spot, active_symbols);
                }
                next_snapshot.as_mut().reset(next_snapshot_instant());
            }
            () = async {
                if let Some(timer) = &mut startup_snapshot {
                    timer.as_mut().await;
                }
            }, if startup_snapshot.is_some() => {
                let should_fetch = {
                    let mut listener_guard = listener.lock().await;
                    listener_guard.begin_snapshot_fetch()
                };
                if should_fetch {
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    let active_symbols = active_symbols.clone();
                    fetch_snapshot(listener, snapshot_fetch_task_tx, ignore_spot, active_symbols);
                }
                startup_snapshot = None;
            }
            () = sleep(Duration::from_secs(5)) => {
                let listener = listener.lock().await;
                if listener.is_ready() {
                    return Err(format!("Stream has fallen behind ({HL_NODE} failed?)").into());
                }
            }
        }
    }
}

fn fetch_snapshot(
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res: Result<()> = (async {
            let state = {
                let mut listener = listener.lock().await;
                listener.begin_caching();
                listener.clone_state()
            };
            // allow some updates to queue and ensure snapshot is recent
            sleep(Duration::from_millis(SNAPSHOT_CACHE_DELAY_MS)).await;
            let snapshot_bytes = process_rmp_file().await.map_err(|err| {
                let listener = listener.clone();
                tokio::spawn(async move {
                    let _unused = listener.lock().await.finish_validation();
                });
                err
            })?;
            let cache = {
                let listener = listener.lock().await;
                listener.clone_cache()
            };
            // let cache_len = cache.len();
            let active_symbols = {
                let active_symbols = active_symbols.lock().await;
                active_symbols
                    .iter()
                    .filter(|(_, count)| **count > 0)
                    .map(|(coin, _)| coin.clone())
                    .collect::<HashSet<_>>()
            };
            // info!("Cache has {} elements", cache.len());
            let blocking_result = tokio::task::spawn_blocking(move || {
                let mut snapshot_bytes = snapshot_bytes;
                let mut cache = cache;
                let (height, expected_snapshot) =
                    load_snapshots_from_slice::<InnerL4Order, (Address, L4Order)>(&mut snapshot_bytes)?;
                info!("Snapshot height received: {height}");
                let cache_len = cache.len();
                info!("Cache has {} elements", cache_len);
                if let Some(mut state) = state {
                    let local_height = state.height();
                    if local_height >= height {
                        return Ok(SnapshotFetchOutcome::Skipped { local_height, snapshot_height: height });
                    }
                    let full_cache = cache.clone();
                    while state.height() < height {
                        if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                            state.apply_updates(order_statuses, order_diffs)?;
                        } else {
                            return Err::<SnapshotFetchOutcome, Error>("Not enough cached updates".into());
                        }
                    }
                    let stored_snapshot = state.compute_snapshot().snapshot;
                    info!("Validating snapshot");
                    if let Err(_err) = validate_snapshot_consistency_with_hashes(
                        &stored_snapshot,
                        expected_snapshot.clone(),
                        &active_symbols,
                        ignore_spot,
                    ) {
                        info!("Snapshot validation failed at height {height}; reinitializing from snapshot.");
                        let mut new_state = OrderBookState::from_snapshot(expected_snapshot, height, 0, true, ignore_spot);
                        let mut full_cache = full_cache;
                        while let Some((order_statuses, order_diffs)) = full_cache.pop_front() {
                            new_state.apply_updates(order_statuses, order_diffs)?;
                        }
                        return Ok(SnapshotFetchOutcome::Initialize {
                            height: new_state.height(),
                            expected_snapshot: new_state.compute_snapshot().snapshot,
                        });
                    }
                    while let Some((order_statuses, order_diffs)) = cache.pop_front() {
                        state.apply_updates(order_statuses, order_diffs)?;
                    }
                    Ok(SnapshotFetchOutcome::Validated { height })
                } else {
                    Ok(SnapshotFetchOutcome::Initialize { height, expected_snapshot })
                }
            })
            .await
            .map_err(|err| format!("Snapshot validation task join error: {err}"))?;

            match blocking_result {
                Ok(SnapshotFetchOutcome::Validated { height }) => {
                    info!("Scheduled snapshot validation succeeded at height {height}");
                    listener.lock().await.finish_validation().map_err(|err| err.into())
                }
                Ok(SnapshotFetchOutcome::Skipped { local_height, snapshot_height }) => {
                    info!(
                        "Snapshot validation skipped: snapshot height {snapshot_height} not newer than local state {local_height}."
                    );
                    listener.lock().await.finish_validation().map_err(|err| err.into())
                }
                Ok(SnapshotFetchOutcome::Initialize { height, expected_snapshot }) => {
                    info!("Initializing from snapshot at height {height}");
                    listener.lock().await.init_from_snapshot(expected_snapshot, height);
                    let result = listener.lock().await.finish_validation().map_err(|err| err.into());
                    if result.is_ok() {
                        listener.lock().await.broadcast_l4_snapshot();
                    }
                    result
                }
                Err(err) => {
                    if err.to_string().contains("Not enough cached updates") {
                        info!("Snapshot validation skipped: not enough cached updates; will retry next snapshot.");
                        Ok(())
                    } else {
                        Err(err)
                    }
                }
            }
        })
        .await;
        listener.lock().await.finish_snapshot_fetch();
        let _unused = tx.send(res);
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    pending_fill_line: String,
    pending_order_status_line: String,
    pending_order_diff_line: String,
    last_fill_len: u64,
    last_order_status_len: u64,
    last_order_diff_len: u64,
    last_fill_offset: u64,
    last_order_status_offset: u64,
    last_order_diff_offset: u64,
    last_sent_height: u64,
    validation_in_progress: bool,
    snapshot_requested: bool,
    snapshot_fetch_in_progress: bool,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            pending_fill_line: String::new(),
            pending_order_status_line: String::new(),
            pending_order_diff_line: String::new(),
            last_fill_len: 0,
            last_order_status_len: 0,
            last_order_diff_len: 0,
            last_fill_offset: 0,
            last_order_status_offset: 0,
            last_order_diff_offset: 0,
            last_sent_height: 0,
            validation_in_progress: false,
            snapshot_requested: false,
            snapshot_fetch_in_progress: false,
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        let diff = self.order_diff_cache.front()?;
        let status = self.order_status_cache.front()?;
        if diff.block_number() != status.block_number() {
            return None;
        }
        self.order_status_cache.pop_front().and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)))
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if self.validation_in_progress {
                while let Some((order_statuses, order_diffs)) = self.pop_cache() {
                    if let Some(cache) = &mut self.fetched_snapshot_cache {
                        cache.push_back((order_statuses.clone(), order_diffs.clone()));
                    } else {
                        self.fetched_snapshot_cache = Some(VecDeque::new());
                        if let Some(cache) = &mut self.fetched_snapshot_cache {
                            cache.push_back((order_statuses.clone(), order_diffs.clone()));
                        }
                    }
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let updates = Arc::new(InternalMessage::L4BookUpdates {
                                diff_batch: order_diffs,
                                status_batch: order_statuses,
                            });
                            let _unused = tx.send(updates);
                        });
                    }
                }
            } else {
                while let Some((order_statuses, order_diffs)) = self.pop_cache() {
                    if let Some(state) = &self.order_book_state {
                        let expected = state.height() + 1;
                        let height = order_statuses.block_number();
                        if height > expected {
                            error!("Order statuses jumped from {expected} to {height}. Waiting for next snapshot.");
                            self.reset_state_for_snapshot();
                            return Ok(());
                        }
                    }
                    self.order_book_state
                        .as_mut()
                        .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                        .transpose()?;
                    if let Some(cache) = &mut self.fetched_snapshot_cache {
                        cache.push_back((order_statuses.clone(), order_diffs.clone()));
                    }
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let updates = Arc::new(InternalMessage::L4BookUpdates {
                                diff_batch: order_diffs,
                                status_batch: order_statuses,
                            });
                            let _unused = tx.send(updates);
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        if self.fetched_snapshot_cache.is_none() {
            self.fetched_snapshot_cache = Some(VecDeque::new());
        }
        self.validation_in_progress = true;
    }

    fn clone_cache(&self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.clone().unwrap_or_default()
    }

    fn finish_validation(&mut self) -> Result<()> {
        self.validation_in_progress = false;
        let mut queued = self.fetched_snapshot_cache.take().unwrap_or_default();
        while let Some((order_statuses, order_diffs)) = queued.pop_front() {
            self.order_book_state.as_mut().map(|book| book.apply_updates(order_statuses, order_diffs)).transpose()?;
        }
        Ok(())
    }

    fn broadcast_l4_snapshot(&mut self) {
        let snapshot = self.compute_snapshot();
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::L4Snapshot { snapshot });
                    let _unused = tx.send(snapshot);
                });
            }
        }
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready at height {height}");
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }

    fn pending_line_mut(&mut self, event_source: EventSource) -> &mut String {
        match event_source {
            EventSource::Fills => &mut self.pending_fill_line,
            EventSource::OrderStatuses => &mut self.pending_order_status_line,
            EventSource::OrderDiffs => &mut self.pending_order_diff_line,
        }
    }

    fn last_len_mut(&mut self, event_source: EventSource) -> &mut u64 {
        match event_source {
            EventSource::Fills => &mut self.last_fill_len,
            EventSource::OrderStatuses => &mut self.last_order_status_len,
            EventSource::OrderDiffs => &mut self.last_order_diff_len,
        }
    }

    fn last_offset_mut(&mut self, event_source: EventSource) -> &mut u64 {
        match event_source {
            EventSource::Fills => &mut self.last_fill_offset,
            EventSource::OrderStatuses => &mut self.last_order_status_offset,
            EventSource::OrderDiffs => &mut self.last_order_diff_offset,
        }
    }

    fn request_snapshot(&mut self) {
        self.snapshot_requested = true;
    }

    fn take_snapshot_request(&mut self) -> bool {
        std::mem::take(&mut self.snapshot_requested)
    }

    fn begin_snapshot_fetch(&mut self) -> bool {
        if self.snapshot_fetch_in_progress {
            return false;
        }
        self.snapshot_fetch_in_progress = true;
        true
    }

    fn finish_snapshot_fetch(&mut self) {
        self.snapshot_fetch_in_progress = false;
    }

    fn reset_state_for_snapshot(&mut self) {
        self.order_book_state = None;
        self.order_diff_cache = BatchQueue::new();
        self.order_status_cache = BatchQueue::new();
        self.fetched_snapshot_cache = None;
        self.validation_in_progress = true;
        self.last_sent_height = 0;
        self.request_snapshot();
    }

    fn should_read_from_start(&self, new_path: &PathBuf, event_source: EventSource) -> bool {
        let file = match File::open(new_path) {
            Ok(file) => file,
            Err(_) => return false,
        };
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        for _ in 0..10 {
            line.clear();
            if reader.read_line(&mut line).ok().is_none() {
                return false;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let block_time = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(trimmed)
                    .map(|batch| batch.block_time())
                    .ok(),
                EventSource::OrderStatuses => serde_json::from_str::<Batch<NodeDataOrderStatus>>(trimmed)
                    .map(|batch| batch.block_time())
                    .ok(),
                EventSource::OrderDiffs => serde_json::from_str::<Batch<NodeDataOrderDiff>>(trimmed)
                    .map(|batch| batch.block_time())
                    .ok(),
            };
            if let Some(block_time) = block_time {
                let now = Utc::now().timestamp_millis();
                let diff = now - block_time as i64;
                return diff.abs() <= NEW_FILE_WINDOW_MS;
            }
            return false;
        }
        false
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            let read_from_start = self.should_read_from_start(new_path, event_source);
            self.on_file_creation(new_path.clone(), event_source, read_from_start)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                if let Ok(size) = stable_size(new_path) {
                    let last_len = *self.last_len_mut(event_source);
                    if size == last_len {
                        return Ok(());
                    }
                    if size < last_len {
                        *self.pending_line_mut(event_source) = String::new();
                        if let Some(file) = self.file_mut(event_source).as_mut() {
                            file.seek(SeekFrom::Start(0))?;
                        }
                        *self.last_offset_mut(event_source) = 0;
                    }
                    *self.last_len_mut(event_source) = size;
                }
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let read_from_start = self.should_read_from_start(new_path, event_source);
                let mut new_file = File::open(new_path)?;
                if read_from_start {
                    let mut buf = String::new();
                    new_file.read_to_string(&mut buf)?;
                    if !buf.is_empty() {
                        self.process_data(buf, event_source)?;
                    }
                } else {
                    new_file.seek(SeekFrom::End(0))?;
                }
                if let Ok(metadata) = new_path.metadata() {
                    *self.last_len_mut(event_source) = metadata.len();
                    *self.last_offset_mut(event_source) = metadata.len();
                }
                *self.pending_line_mut(event_source) = String::new();
                *self.file_mut(event_source) = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource, read_from_start: bool) -> Result<()> {
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        }
        let mut file = File::open(new_file)?;
        if read_from_start {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        } else {
            file.seek(SeekFrom::End(0))?;
        }
        let new_offset = file.seek(SeekFrom::Current(0))?;
        *self.file_mut(event_source) = Some(file);
        *self.last_offset_mut(event_source) = new_offset;
        Ok(())
    }

    fn on_file_modification(&mut self, event_source: EventSource) -> Result<()> {
        let mut buf = String::new();
        let last_offset = *self.last_offset_mut(event_source);
        let file = self.file_mut(event_source).as_mut().ok_or("No file being tracked")?;
        file.seek(SeekFrom::Start(last_offset))?;
        file.read_to_string(&mut buf)?;
        let new_offset = file.seek(SeekFrom::Current(0))?;
        *self.last_offset_mut(event_source) = new_offset;
        self.process_data(buf, event_source)?;
        Ok(())
    }

    fn process_data(&mut self, incoming: String, event_source: EventSource) -> Result<()> {
        let pending = self.pending_line_mut(event_source).clone();
        let mut combined = pending;
        combined.push_str(&incoming);
        let ends_with_newline = combined.ends_with('\n');
        let mut lines: Vec<&str> = combined.split('\n').collect();
        let pending_line = if ends_with_newline { None } else { lines.pop() };
        if let Some(pending_line) = pending_line {
            *self.pending_line_mut(event_source) = pending_line.to_string();
        } else {
            self.pending_line_mut(event_source).clear();
        }
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    // if we run into a serialization error (hitting EOF), just return to last line.
                    error!(
                        "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        line.chars().take(100).collect::<String>(),
                    );
                    *self.pending_line_mut(event_source) = line.to_string();
                    break;
                }
            };
            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }
            if let Err(err) = self.receive_batch(event_batch) {
                error!("{event_source} update error: {err}. Waiting for next snapshot.");
                self.reset_state_for_snapshot();
                return Ok(());
            }
        }
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                    let _unused = tx.send(snapshot);
                });
            }
        }
        Ok(())
    }
}

fn stable_size(path: &PathBuf) -> Result<u64> {
    let _first = path.metadata()?.len();
    thread::sleep(Duration::from_millis(5));
    let second = path.metadata()?.len();
    Ok(second)
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
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
    L4Snapshot { snapshot: TimedSnapshots },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
