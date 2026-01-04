use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, InnerOrder, Oid, Px, Side, Snapshot, Sz,
        multi_book::{Snapshots, load_snapshots_from_slice},
    },
    prelude::*,
    types::{
        L4BookLiteAction, L4BookLiteSnapshot, L4BookLiteUpdate, L4BookLiteUpdates, L4Order,
        inner::{InnerL4Order, InnerLevel, InnerOrderDiff},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::Subscription,
    },
};
use alloy::primitives::Address;
use chrono::{Timelike, Utc};
use fs::File;
use log::{debug, error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::thread;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    os::unix::{fs::MetadataExt, io::AsRawFd},
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
const FETCHED_SNAPSHOT_CACHE_LIMIT: usize = 2048;
const FILE_PUNCH_TRIGGER_BYTES: u64 = 511 * 1024 * 1024;
const FILE_PUNCH_KEEP_BYTES: u64 = 15 * 1024 * 1024;
const FILE_PUNCH_ALIGNMENT_BYTES: u64 = 4096;
const FILE_PUNCH_MIN_BYTES: u64 = 32 * 1024 * 1024;
const ENABLE_L2_SNAPSHOTS: bool = false;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitializationState {
    Uninitialized,
    Initializing,
    Initialized,
}

#[derive(Clone, Debug)]
struct LiteOrderInfo {
    coin: Coin,
    side: Side,
    px: Px,
    sz: Sz,
}

fn next_snapshot_instant() -> Instant {
    let now = Utc::now();
    let base = now.with_second(30).unwrap_or(now);
    let at_hour = base.with_minute(0).unwrap_or(base);
    let at_half_hour = base.with_minute(30).unwrap_or(base);
    let at_last_minute = base.with_minute(59).unwrap_or(base);
    let target = [at_hour, at_half_hour, at_last_minute]
        .into_iter()
        .filter(|candidate| *candidate >= now)
        .min()
        .unwrap_or_else(|| {
            let next_hour = now + chrono::Duration::hours(1);
            next_hour
                .with_minute(0)
                .and_then(|next| next.with_second(30))
                .unwrap_or(next_hour)
        });
    let duration = target.signed_duration_since(now);
    let wait = duration.to_std().unwrap_or_else(|_| Duration::from_secs(0));
    Instant::now() + wait
}

fn filter_l4_snapshot(snapshot: Snapshots<InnerL4Order>) -> Snapshots<InnerL4Order> {
    Snapshots::new(
        snapshot
            .value()
            .into_iter()
            .filter(|(coin, _)| Subscription::is_l4_snapshot_coin(&coin.value()))
            .collect(),
    )
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
                let expected_snapshot = filter_l4_snapshot(expected_snapshot);
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
                    let result = listener.lock().await.finish_validation().map_err(|err| err.into());
                    if result.is_ok() {
                        listener.lock().await.broadcast_l4_snapshot();
                    }
                    result
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
    initialization_state: InitializationState,
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
    last_fill_punch_offset: u64,
    last_order_status_punch_offset: u64,
    last_order_diff_punch_offset: u64,
    l4_book_lite: HashMap<Coin, [BTreeMap<Px, Sz>; 2]>,
    l4_order_index: HashMap<Oid, LiteOrderInfo>,
    last_sent_height: u64,
    validation_in_progress: bool,
    snapshot_requested: bool,
    snapshot_fetch_in_progress: bool,
}

impl OrderBookListener {
    pub(crate) fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            initialization_state: InitializationState::Uninitialized,
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
            last_fill_punch_offset: 0,
            last_order_status_punch_offset: 0,
            last_order_diff_punch_offset: 0,
            l4_book_lite: HashMap::new(),
            l4_order_index: HashMap::new(),
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

    fn cache_update(&mut self, order_statuses: Batch<NodeDataOrderStatus>, order_diffs: Batch<NodeDataOrderDiff>) {
        if !self.validation_in_progress {
            return;
        }
        let cache = self.fetched_snapshot_cache.get_or_insert_with(VecDeque::new);
        cache.push_back((order_statuses, order_diffs));
        while cache.len() > FETCHED_SNAPSHOT_CACHE_LIMIT {
            cache.pop_front();
        }
    }

    fn rebuild_l4_book_lite(&mut self, snapshot: &Snapshots<InnerL4Order>) {
        self.l4_book_lite.clear();
        self.l4_order_index.clear();
        for (coin, book) in snapshot.as_ref() {
            let entry = self
                .l4_book_lite
                .entry(coin.clone())
                .or_insert_with(|| [BTreeMap::new(), BTreeMap::new()]);
            for (side_idx, orders) in book.as_ref().iter().enumerate() {
                for order in orders {
                    let px = order.limit_px;
                    let sz = order.sz;
                    let book_side = &mut entry[side_idx];
                    let new_sz = book_side.get(&px).copied().unwrap_or_else(|| Sz::new(0)) + sz;
                    book_side.insert(px, new_sz);
                    self.l4_order_index.insert(
                        Oid::new(order.oid),
                        LiteOrderInfo { coin: coin.clone(), side: order.side, px, sz },
                    );
                }
            }
        }
    }

    pub(crate) fn l4_book_lite_snapshot(&self, coin: String) -> Option<L4BookLiteSnapshot> {
        let state = self.order_book_state.as_ref()?;
        let levels = self.l4_book_lite.get(&Coin::new(&coin))?;
        // BTreeMap iterates in ascending order by price (stored as keys)
        let bids = levels[0]
            .iter()
            .map(|(px, sz)| [px.to_str(), sz.to_str()])
            .collect();
        // BTreeMap iterates in ascending order by price (stored as keys)
        let asks = levels[1]
            .iter()
            .map(|(px, sz)| [px.to_str(), sz.to_str()])
            .collect();
        Some(L4BookLiteSnapshot { coin, time: state.time(), height: state.height(), bids, asks })
    }

    fn apply_l4_book_lite_order(
        &mut self,
        info: &LiteOrderInfo,
        sz: Sz,
        action: L4BookLiteAction,
        updates: &mut HashMap<(Coin, Side, Px, L4BookLiteAction), Sz>,
    ) {
        let entry = self
            .l4_book_lite
            .entry(info.coin.clone())
            .or_insert_with(|| [BTreeMap::new(), BTreeMap::new()]);
        let side_idx = match info.side {
            Side::Bid => 0,
            Side::Ask => 1,
        };
        let book_side = &mut entry[side_idx];
        let current = book_side.get(&info.px).copied().unwrap_or_else(|| Sz::new(0));
        let new_sz = if matches!(action, L4BookLiteAction::Add) {
            current + sz
        } else {
            Sz::new(current.value().saturating_sub(sz.value()))
        };
        if new_sz.value() == 0 {
            book_side.remove(&info.px);
        } else {
            book_side.insert(info.px, new_sz);
        }
        let key = (info.coin.clone(), info.side, info.px, action);
        let agg = updates.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + sz;
        updates.insert(key, agg);
    }

    fn build_l4_book_lite_updates(
        &mut self,
        order_statuses: &Batch<NodeDataOrderStatus>,
        order_diffs: &Batch<NodeDataOrderDiff>,
    ) -> Vec<L4BookLiteUpdates> {
        let mut order_map = order_statuses
            .clone()
            .events()
            .into_iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        let mut updates = HashMap::new();
        let diffs = order_diffs.clone().events();
        for diff in diffs {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = match diff.diff().try_into() {
                Ok(inner) => inner,
                Err(_) => continue,
            };
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    if let Some(order_status) = order_map.remove(&oid) {
                        let mut order: InnerL4Order = match order_status.try_into() {
                            Ok(order) => order,
                            Err(_) => continue,
                        };
                        order.modify_sz(sz);
                        let info = LiteOrderInfo { coin: order.coin.clone(), side: order.side, px: order.limit_px, sz: order.sz };
                        self.l4_order_index.insert(oid, info.clone());
                        self.apply_l4_book_lite_order(&info, info.sz, L4BookLiteAction::Add, &mut updates);
                    }
                }
                InnerOrderDiff::Update { orig_sz, new_sz } => {
                    if let Some(mut info) = self.l4_order_index.get(&oid).cloned() {
                        if new_sz.value() >= orig_sz.value() {
                            let delta = Sz::new(new_sz.value() - orig_sz.value());
                            if delta.value() > 0 {
                                self.apply_l4_book_lite_order(&info, delta, L4BookLiteAction::Add, &mut updates);
                            }
                        } else {
                            let delta = Sz::new(orig_sz.value() - new_sz.value());
                            if delta.value() > 0 {
                                self.apply_l4_book_lite_order(&info, delta, L4BookLiteAction::Remove, &mut updates);
                            }
                        }
                        info.sz = new_sz;
                        self.l4_order_index.insert(oid, info);
                    }
                }
                InnerOrderDiff::Remove => {
                    if let Some(info) = self.l4_order_index.remove(&oid) {
                        self.apply_l4_book_lite_order(&info, info.sz, L4BookLiteAction::Remove, &mut updates);
                    }
                }
            }
        }

        let mut by_coin: HashMap<Coin, Vec<L4BookLiteUpdate>> = HashMap::new();
        for ((coin, side, px, action), sz) in updates {
            by_coin
                .entry(coin)
                .or_insert_with(Vec::new)
                .push(L4BookLiteUpdate { px: px.to_str(), sz: sz.to_str(), side, action });
        }
        let time = order_statuses.block_time();
        let height = order_statuses.block_number();
        by_coin
            .into_iter()
            .map(|(coin, updates)| L4BookLiteUpdates { coin: coin.value(), time, height, updates })
            .collect()
    }

    fn build_l4_book_lite_trade_updates(&self, batch: &Batch<NodeDataFill>) -> Vec<L4BookLiteUpdates> {
        let mut by_coin: HashMap<Coin, HashMap<(Side, Px, L4BookLiteAction), Sz>> = HashMap::new();
        for fill in batch.clone().events() {
            let order = fill.1;
            let px = match Px::parse_from_str(&order.px) {
                Ok(px) => px,
                Err(_) => continue,
            };
            let sz = match Sz::parse_from_str(&order.sz) {
                Ok(sz) => sz,
                Err(_) => continue,
            };
            let coin = Coin::new(&order.coin);
            let entry = by_coin.entry(coin).or_insert_with(HashMap::new);
            let key = (order.side, px, L4BookLiteAction::Trade);
            let agg = entry.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + sz;
            entry.insert(key, agg);
        }
        let time = batch.block_time();
        let height = batch.block_number();
        by_coin
            .into_iter()
            .map(|(coin, updates)| {
                let updates = updates
                    .into_iter()
                    .map(|((side, px, action), sz)| L4BookLiteUpdate { px: px.to_str(), sz: sz.to_str(), side, action })
                    .collect();
                L4BookLiteUpdates { coin: coin.value(), time, height, updates }
            })
            .collect()
    }

    fn broadcast_update(&self, order_statuses: Batch<NodeDataOrderStatus>, order_diffs: Batch<NodeDataOrderDiff>) {
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
                        let batch = batch.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
                // Only send l4BookLite trade updates if we have a baseline state.
                // This ensures consistency with add/remove updates which also require baseline state.
                // During Uninitialized state, we don't send any l4BookLite updates.
                // During Initializing/Initialized states, we send both trade and add/remove updates.
                if self.initialization_state != InitializationState::Uninitialized {
                    let lite_updates = self.build_l4_book_lite_trade_updates(&batch);
                    for updates in lite_updates {
                        if let Some(tx) = &self.internal_message_tx {
                            let tx = tx.clone();
                            tokio::spawn(async move {
                                let update = Arc::new(InternalMessage::L4BookLiteUpdates { updates });
                                let _unused = tx.send(update);
                            });
                        }
                    }
                }
            }
        }
        
        // Hot path optimization: early return for uninitialized/initializing states
        match self.initialization_state {
            InitializationState::Uninitialized => return Ok(()),
            InitializationState::Initializing => {
                // During initialization, accumulate updates in fetched_snapshot_cache if validation is in progress
                if self.validation_in_progress {
                    while let Some((order_statuses, order_diffs)) = self.pop_cache() {
                        let filtered_statuses =
                            order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                        let filtered_diffs =
                            order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                        let lite_updates = self.build_l4_book_lite_updates(&filtered_statuses, &filtered_diffs);
                        self.cache_update(filtered_statuses, filtered_diffs);
                        self.broadcast_update(order_statuses, order_diffs);
                        for updates in lite_updates {
                            if let Some(tx) = &self.internal_message_tx {
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    let update = Arc::new(InternalMessage::L4BookLiteUpdates { updates });
                                    let _unused = tx.send(update);
                                });
                            }
                        }
                    }
                }
                return Ok(());
            }
            InitializationState::Initialized => {
                // Fast path: process updates normally
            }
        }
        
        // Initialized state processing
        if self.validation_in_progress {
            while let Some((order_statuses, order_diffs)) = self.pop_cache() {
                let filtered_statuses =
                    order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                let filtered_diffs =
                    order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                let lite_updates = self.build_l4_book_lite_updates(&filtered_statuses, &filtered_diffs);
                self.cache_update(filtered_statuses, filtered_diffs);
                self.broadcast_update(order_statuses, order_diffs);
                for updates in lite_updates {
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let update = Arc::new(InternalMessage::L4BookLiteUpdates { updates });
                            let _unused = tx.send(update);
                        });
                    }
                }
            }
        } else {
            while let Some((order_statuses, order_diffs)) = self.pop_cache() {
                // Block continuity detection
                if let Some(state) = &self.order_book_state {
                    let expected = state.height() + 1;
                    let height = order_statuses.block_number();
                    if height > expected {
                        error!("Block height gap detected: expected {expected}, got {height}. Triggering full update.");
                        self.reset_state_for_snapshot();
                        return Ok(());
                    }
                }
                let filtered_statuses =
                    order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                let filtered_diffs =
                    order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                let lite_updates = self.build_l4_book_lite_updates(&filtered_statuses, &filtered_diffs);
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(filtered_statuses, filtered_diffs))
                    .transpose()?;
                self.broadcast_update(order_statuses, order_diffs);
                for updates in lite_updates {
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let update = Arc::new(InternalMessage::L4BookLiteUpdates { updates });
                            let _unused = tx.send(update);
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
        self.validation_in_progress = true;
        
        // For uninitialized state, don't transfer accumulated updates yet
        // Blocks accumulated during startup may be too new relative to the snapshot we'll receive
        // They will be evaluated after snapshot is received in init_from_snapshot()
        if self.initialization_state == InitializationState::Uninitialized {
            self.initialization_state = InitializationState::Initializing;
            return;
        }
        
        // Transfer any accumulated updates from order_status_cache/order_diff_cache to fetched_snapshot_cache
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            let order_statuses =
                order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            let order_diffs = order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            #[allow(clippy::unwrap_used)]
            self.fetched_snapshot_cache.as_mut().unwrap().push_back((order_statuses, order_diffs));
        }
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
        info!("Initializing from snapshot at height {height}");
        let snapshot = filter_l4_snapshot(snapshot);
        self.rebuild_l4_book_lite(&snapshot);
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        
        // Apply only blocks that are newer than snapshot
        let mut applied_count = 0;
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            let order_statuses =
                order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            let order_diffs = order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            let _unused = self.build_l4_book_lite_updates(&order_statuses, &order_diffs);
            let block_height = order_statuses.block_number();
            
            // Skip blocks older than or equal to snapshot
            if block_height <= height {
                info!("Skipping block {block_height} (snapshot is at {height})");
                continue;
            }
            
            // Check for block continuity
            let expected_height = new_order_book.height() + 1;
            if block_height > expected_height {
                info!(
                    "Block height gap detected: expected {expected_height}, got {block_height}. Discarding accumulated blocks and waiting for next snapshot."
                );
                retry = true;
                break;
            }
            
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
            applied_count += 1;
        }
        
        if !retry {
            let final_height = new_order_book.height();
            self.order_book_state = Some(new_order_book);
            self.initialization_state = InitializationState::Initialized;
            info!("Order book ready at height {} (applied {} cached blocks)", final_height, applied_count);
        } else {
            // Clear caches and wait for next snapshot
            self.clear_caches();
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

    fn last_offset(&self, event_source: EventSource) -> u64 {
        match event_source {
            EventSource::Fills => self.last_fill_offset,
            EventSource::OrderStatuses => self.last_order_status_offset,
            EventSource::OrderDiffs => self.last_order_diff_offset,
        }
    }

    fn last_punch_offset_mut(&mut self, event_source: EventSource) -> &mut u64 {
        match event_source {
            EventSource::Fills => &mut self.last_fill_punch_offset,
            EventSource::OrderStatuses => &mut self.last_order_status_punch_offset,
            EventSource::OrderDiffs => &mut self.last_order_diff_punch_offset,
        }
    }

    fn last_punch_offset(&self, event_source: EventSource) -> u64 {
        match event_source {
            EventSource::Fills => self.last_fill_punch_offset,
            EventSource::OrderStatuses => self.last_order_status_punch_offset,
            EventSource::OrderDiffs => self.last_order_diff_punch_offset,
        }
    }

    fn align_down(value: u64, alignment: u64) -> u64 {
        if alignment == 0 {
            return value;
        }
        value - (value % alignment)
    }

    fn punch_file_if_needed(
        &mut self,
        event_source: EventSource,
        fd: i32,
        file_len: u64,
        allocated_bytes: u64,
    ) {
        if allocated_bytes <= FILE_PUNCH_TRIGGER_BYTES {
            return;
        }
        let desired_end = file_len.saturating_sub(FILE_PUNCH_KEEP_BYTES);
        let last_offset = self.last_offset(event_source);
        if last_offset < desired_end {
            return;
        }
        let last_punch_offset = self.last_punch_offset(event_source);
        if last_punch_offset >= desired_end {
            return;
        }
        let start = Self::align_down(last_punch_offset, FILE_PUNCH_ALIGNMENT_BYTES);
        let end = Self::align_down(desired_end, FILE_PUNCH_ALIGNMENT_BYTES);
        if end <= start {
            return;
        }
        let len = end - start;
        if len < FILE_PUNCH_MIN_BYTES {
            return;
        }
        #[allow(unsafe_code)]
        let result = unsafe {
            libc::fallocate(
                fd,
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                start as libc::off_t,
                len as libc::off_t,
            )
        };
        if result != 0 {
            let err = io::Error::last_os_error();
            warn!("Failed to punch hole for {event_source} file: {err}");
            return;
        }
        *self.last_punch_offset_mut(event_source) = end;
        info!(
            "Punched {event_source} file from {} to {} (file len {})",
            start, end, file_len
        );
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

    fn clear_caches(&mut self) {
        self.order_status_cache = BatchQueue::new();
        self.order_diff_cache = BatchQueue::new();
    }

    fn reset_state_for_snapshot(&mut self) {
        self.order_book_state = None;
        self.initialization_state = InitializationState::Uninitialized;
        self.clear_caches();
        self.fetched_snapshot_cache = None;
        self.l4_book_lite.clear();
        self.l4_order_index.clear();
        // Keep validation_in_progress as false since we're starting fresh
        // It will be set to true when begin_caching() is called during next snapshot fetch
        self.validation_in_progress = false;
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
                        *self.last_punch_offset_mut(event_source) = 0;
                    }
                    *self.last_len_mut(event_source) = size;
                }
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let read_from_start = self.should_read_from_start(new_path, event_source);
                let mut new_file = OpenOptions::new().read(true).write(true).open(new_path)?;
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
                *self.last_punch_offset_mut(event_source) = 0;
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
        let mut file = OpenOptions::new().read(true).write(true).open(new_file)?;
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
        *self.last_punch_offset_mut(event_source) = 0;
        Ok(())
    }

    fn on_file_modification(&mut self, event_source: EventSource) -> Result<()> {
        let mut buf = String::new();
        let last_offset = *self.last_offset_mut(event_source);
        let (new_offset, file_len, allocated_bytes, fd) = {
            let file = self.file_mut(event_source).as_mut().ok_or("No file being tracked")?;
            file.seek(SeekFrom::Start(last_offset))?;
            file.read_to_string(&mut buf)?;
            let new_offset = file.seek(SeekFrom::Current(0))?;
            let metadata = file.metadata().ok();
            let file_len = metadata.as_ref().map(|meta| meta.len());
            let allocated_bytes = metadata.as_ref().map(|meta| meta.blocks() * 512);
            (new_offset, file_len, allocated_bytes, file.as_raw_fd())
        };
        *self.last_offset_mut(event_source) = new_offset;
        if let (Some(file_len), Some(allocated_bytes)) = (file_len, allocated_bytes) {
            self.punch_file_if_needed(event_source, fd, file_len, allocated_bytes);
        }
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
                debug!("{event_source} block: {height}");
            }
            if let Err(err) = self.receive_batch(event_batch) {
                error!("{event_source} update error: {err}. Waiting for next snapshot.");
                self.reset_state_for_snapshot();
                return Ok(());
            }
        }
        if ENABLE_L2_SNAPSHOTS {
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
    L4BookLiteUpdates { updates: L4BookLiteUpdates },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
