use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, InnerOrder, Oid, Px, Side, Snapshot, Sz,
        multi_book::{Snapshots, load_snapshots_from_slice},
    },
    prelude::*,
    types::{
        L4BookLiteAction, L4BookLiteDepthLevel, L4BookLiteDepthUpdate, L4BookLiteSnapshot, L4BookLiteUpdate,
        L4BookLiteUpdates, L4Order,
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
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};
use tokio::{
    sync::{
        Mutex as TokioMutex,
        broadcast::Sender,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time::{Instant, sleep, sleep_until},
};
use utils::{BatchQueue, EventBatch, process_rmp_file};

mod state;
mod utils;

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
struct OrderInfo {
    coin: Coin,
    side: Side,
    px: Px,
    sz: Sz,
}

#[derive(Clone, Debug)]
struct LevelState {
    sum_sz: Sz,
    shadow_sum: Sz,
    orders: HashMap<Oid, Sz>,
}

impl Default for LevelState {
    fn default() -> Self {
        Self { sum_sz: Sz::new(0), shadow_sum: Sz::new(0), orders: HashMap::new() }
    }
}

#[derive(Clone, Debug)]
struct BookState {
    bids: BTreeMap<Px, LevelState>,
    asks: BTreeMap<Px, LevelState>,
    oid_index: HashMap<Oid, OrderInfo>,
    dirty_levels: HashSet<(Side, Px)>,
}

impl Default for BookState {
    fn default() -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), oid_index: HashMap::new(), dirty_levels: HashSet::new() }
    }
}

impl BookState {
    fn from_snapshot(snapshot: &Snapshot<InnerL4Order>) -> Self {
        let mut state = Self::default();
        for orders in snapshot.as_ref().iter() {
            for order in orders {
                let side = order.side;
                let px = order.limit_px;
                let sz = order.sz;
                let level = state.level_mut(side, px);
                level.sum_sz = level.sum_sz + sz;
                level.orders.insert(Oid::new(order.oid), sz);
                state.oid_index.insert(
                    Oid::new(order.oid),
                    OrderInfo { coin: order.coin.clone(), side, px, sz },
                );
            }
        }
        state
    }

    fn level_mut(&mut self, side: Side, px: Px) -> &mut LevelState {
        let book_side = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        book_side.entry(px).or_insert_with(LevelState::default)
    }

    fn level(&self, side: Side, px: Px) -> Option<&LevelState> {
        match side {
            Side::Bid => self.bids.get(&px),
            Side::Ask => self.asks.get(&px),
        }
    }

    fn mark_dirty(&mut self, side: Side, px: Px) {
        self.dirty_levels.insert((side, px));
        let _unused = self.level_mut(side, px);
    }

    fn apply_new(&mut self, oid: Oid, info: OrderInfo, updates: &mut HashMap<(Side, Px, L4BookLiteAction), Sz>) {
        let level = self.level_mut(info.side, info.px);
        level.sum_sz = level.sum_sz + info.sz;
        level.orders.insert(oid.clone(), info.sz);
        self.oid_index.insert(oid, info.clone());
        self.mark_dirty(info.side, info.px);
        let key = (info.side, info.px, L4BookLiteAction::Add);
        let agg = updates.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + info.sz;
        updates.insert(key, agg);
    }

    fn apply_update(
        &mut self,
        oid: Oid,
        new_sz: Sz,
        updates: &mut HashMap<(Side, Px, L4BookLiteAction), Sz>,
    ) {
        if let Some(mut info) = self.oid_index.get(&oid).cloned() {
            let orig_sz = info.sz;
            if new_sz.value() >= orig_sz.value() {
                let delta = Sz::new(new_sz.value() - orig_sz.value());
                if delta.value() > 0 {
                    let level = self.level_mut(info.side, info.px);
                    level.sum_sz = level.sum_sz + delta;
                    if let Some(order_sz) = level.orders.get_mut(&oid) {
                        *order_sz = new_sz;
                    } else {
                        level.orders.insert(oid.clone(), new_sz);
                    }
                    let key = (info.side, info.px, L4BookLiteAction::Add);
                    let agg = updates.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + delta;
                    updates.insert(key, agg);
                }
            } else {
                let delta = Sz::new(orig_sz.value() - new_sz.value());
                if delta.value() > 0 {
                    let level = self.level_mut(info.side, info.px);
                    level.sum_sz = Sz::new(level.sum_sz.value().saturating_sub(delta.value()));
                    if new_sz.value() == 0 {
                        level.orders.remove(&oid);
                    } else {
                        level.orders.insert(oid.clone(), new_sz);
                    }
                    let key = (info.side, info.px, L4BookLiteAction::Remove);
                    let agg = updates.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + delta;
                    updates.insert(key, agg);
                }
            }
            info.sz = new_sz;
            self.oid_index.insert(oid, info.clone());
            self.mark_dirty(info.side, info.px);
        }
    }

    fn apply_remove(&mut self, oid: Oid, updates: &mut HashMap<(Side, Px, L4BookLiteAction), Sz>) {
        if let Some(info) = self.oid_index.remove(&oid) {
            let level = self.level_mut(info.side, info.px);
            level.sum_sz = Sz::new(level.sum_sz.value().saturating_sub(info.sz.value()));
            level.orders.remove(&oid);
            self.mark_dirty(info.side, info.px);
            let key = (info.side, info.px, L4BookLiteAction::Remove);
            let agg = updates.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + info.sz;
            updates.insert(key, agg);
        }
    }

    fn snapshot(&self, coin: &Coin, time: u64, height: u64) -> L4BookLiteSnapshot {
        let bids = self
            .bids
            .iter()
            .map(|(px, level)| [px.to_str(), level.sum_sz.to_str()])
            .collect();
        let asks = self
            .asks
            .iter()
            .map(|(px, level)| [px.to_str(), level.sum_sz.to_str()])
            .collect();
        L4BookLiteSnapshot { coin: coin.value(), time, height, bids, asks }
    }

    fn depth_update(
        &mut self,
        coin: &Coin,
        time: u64,
        height: u64,
        trade_levels: &HashSet<(Side, Px)>,
    ) -> L4BookLiteDepthUpdate {
        let mut updates = Vec::new();
        let mut levels: HashSet<(Side, Px)> = self.dirty_levels.drain().collect();
        levels.extend(trade_levels.iter().copied());
        for (side, px) in levels {
            let level = self.level_mut(side, px);
            let should_emit = level.sum_sz.value() != level.shadow_sum.value() || trade_levels.contains(&(side, px));
            if should_emit {
                updates.push(L4BookLiteDepthLevel { px: px.to_str(), sz: level.sum_sz.to_str(), side });
                level.shadow_sum = level.sum_sz;
            }
        }
        self.cleanup_empty_levels();
        L4BookLiteDepthUpdate { coin: coin.value(), time, height, updates }
    }

    fn cleanup_empty_levels(&mut self) {
        self.bids.retain(|_, level| !(level.sum_sz.value() == 0 && level.shadow_sum.value() == 0 && level.orders.is_empty()));
        self.asks.retain(|_, level| !(level.sum_sz.value() == 0 && level.shadow_sum.value() == 0 && level.orders.is_empty()));
    }
}

enum CoinWorkerCommand {
    InitSnapshot { snapshot: Snapshot<InnerL4Order>, height: u64, time: u64 },
    Block {
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
        fills: Batch<NodeDataFill>,
    },
}

struct CoinWorkerState {
    coin: Coin,
    ignore_spot: bool,
    order_book_state: Option<OrderBookState>,
    lite_state: BookState,
}

impl CoinWorkerState {
    fn new(coin: Coin, ignore_spot: bool) -> Self {
        Self { coin, ignore_spot, order_book_state: None, lite_state: BookState::default() }
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshot<InnerL4Order>, height: u64, time: u64) {
        let mut map = HashMap::new();
        map.insert(self.coin.clone(), snapshot.clone());
        let snapshots = Snapshots::new(map);
        self.order_book_state = Some(OrderBookState::from_snapshot(snapshots, height, time, true, self.ignore_spot));
        self.lite_state = BookState::from_snapshot(&snapshot);
    }

    fn l4_snapshot(&self) -> Option<TimedSnapshots> {
        self.order_book_state.as_ref().map(OrderBookState::compute_snapshot)
    }

    fn l4_book_lite_snapshot(&self) -> Option<L4BookLiteSnapshot> {
        self.order_book_state
            .as_ref()
            .map(|state| self.lite_state.snapshot(&self.coin, state.time(), state.height()))
    }

    fn apply_block(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
        fills: Batch<NodeDataFill>,
    ) -> Result<(L4BookLiteDepthUpdate, L4BookLiteUpdates)> {
        let time = order_statuses.block_time();
        let height = order_statuses.block_number();
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
        for diff in order_diffs.clone().events() {
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
                        let info = OrderInfo { coin: order.coin.clone(), side: order.side, px: order.limit_px, sz: order.sz };
                        self.lite_state.apply_new(oid, info, &mut updates);
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    self.lite_state.apply_update(oid, new_sz, &mut updates);
                }
                InnerOrderDiff::Remove => {
                    self.lite_state.apply_remove(oid, &mut updates);
                }
            }
        }

        let mut trade_levels = HashSet::new();
        for fill in fills.clone().events() {
            let order = fill.1;
            let px = match Px::parse_from_str(&order.px) {
                Ok(px) => px,
                Err(_) => continue,
            };
            let sz = match Sz::parse_from_str(&order.sz) {
                Ok(sz) => sz,
                Err(_) => continue,
            };
            let key = (order.side, px, L4BookLiteAction::Trade);
            let agg = updates.get(&key).copied().unwrap_or_else(|| Sz::new(0)) + sz;
            updates.insert(key, agg);
            trade_levels.insert((order.side, px));
        }

        let l4_updates = {
            let updates = updates
                .into_iter()
                .map(|((side, px, action), sz)| L4BookLiteUpdate { px: px.to_str(), sz: sz.to_str(), side, action })
                .collect();
            L4BookLiteUpdates { coin: self.coin.value(), time, height, updates }
        };
        let depth_update = self.lite_state.depth_update(&self.coin, time, height, &trade_levels);

        if let Some(book) = self.order_book_state.as_mut() {
            book.apply_updates(order_statuses, order_diffs)?;
        }
        Ok((depth_update, l4_updates))
    }
}

struct CoinWorker {
    coin: Coin,
    state: Arc<StdMutex<CoinWorkerState>>,
    tx: UnboundedSender<CoinWorkerCommand>,
}

impl CoinWorker {
    fn new(coin: Coin, ignore_spot: bool, internal_message_tx: Sender<Arc<InternalMessage>>) -> Self {
        let (tx, rx) = unbounded_channel();
        let state = Arc::new(StdMutex::new(CoinWorkerState::new(coin.clone(), ignore_spot)));
        Self::spawn_worker(coin.clone(), state.clone(), internal_message_tx, rx);
        Self { coin, state, tx }
    }

    fn spawn_worker(
        coin: Coin,
        state: Arc<StdMutex<CoinWorkerState>>,
        internal_message_tx: Sender<Arc<InternalMessage>>,
        mut rx: UnboundedReceiver<CoinWorkerCommand>,
    ) {
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    CoinWorkerCommand::InitSnapshot { snapshot, height, time } => {
                        let mut state = state.lock().expect("coin worker state lock");
                        state.init_from_snapshot(snapshot, height, time);
                        if let Some(snapshot) = state.l4_snapshot() {
                            let snapshot = Arc::new(InternalMessage::L4Snapshot { snapshot });
                            let _unused = internal_message_tx.send(snapshot);
                        }
                    }
                    CoinWorkerCommand::Block { order_statuses, order_diffs, fills } => {
                        let mut state = state.lock().expect("coin worker state lock");
                        match state.apply_block(order_statuses, order_diffs, fills) {
                            Ok((depth_update, updates)) => {
                                let depth_update = Arc::new(InternalMessage::L4BookLiteDepthUpdates { updates: depth_update });
                                let _unused = internal_message_tx.send(depth_update);
                                let updates = Arc::new(InternalMessage::L4BookLiteUpdates { updates });
                                let _unused = internal_message_tx.send(updates);
                            }
                            Err(err) => {
                                warn!("Worker failed to apply block for {}: {err}", coin.value());
                            }
                        }
                    }
                }
            }
        });
    }
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
    listener: Arc<TokioMutex<OrderBookListener>>,
    dir: PathBuf,
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
                            fetch_snapshot(listener, snapshot_fetch_task_tx);
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
                    fetch_snapshot(listener, snapshot_fetch_task_tx);
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
                    fetch_snapshot(listener, snapshot_fetch_task_tx);
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
    listener: Arc<TokioMutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res: Result<()> = (async {
            let mut listener_guard = listener.lock().await;
            listener_guard.begin_caching();
            drop(listener_guard);
            // allow some updates to queue and ensure snapshot is recent
            sleep(Duration::from_millis(SNAPSHOT_CACHE_DELAY_MS)).await;
            let snapshot_bytes = process_rmp_file().await.map_err(|err| {
                let listener = listener.clone();
                tokio::spawn(async move {
                    let _unused = listener.lock().await.finish_validation();
                });
                err
            })?;
            let blocking_result = tokio::task::spawn_blocking(move || {
                let mut snapshot_bytes = snapshot_bytes;
                let (height, expected_snapshot) =
                    load_snapshots_from_slice::<InnerL4Order, (Address, L4Order)>(&mut snapshot_bytes)?;
                let expected_snapshot = filter_l4_snapshot(expected_snapshot);
                info!("Snapshot height received: {height}");
                Ok::<_, Error>((height, expected_snapshot))
            })
            .await
            .map_err(|err| format!("Snapshot load task join error: {err}"))?;

            match blocking_result {
                Ok((height, expected_snapshot)) => {
                    info!("Initializing from snapshot at height {height}");
                    listener.lock().await.init_from_snapshot(expected_snapshot, height);
                    let result = listener.lock().await.finish_validation().map_err(|err| err.into());
                    if result.is_ok() {
                        listener.lock().await.broadcast_l4_snapshot();
                    }
                    result
                }
                Err(err) => Err(err),
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
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    fill_cache: BatchQueue<NodeDataFill>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>, Batch<NodeDataFill>)>>,
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
    validation_in_progress: bool,
    snapshot_requested: bool,
    snapshot_fetch_in_progress: bool,
    workers: HashMap<Coin, CoinWorker>,
    initialization_state: InitializationState,
    active_symbols: Arc<StdMutex<HashMap<String, usize>>>,
    known_universe: HashSet<Coin>,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        active_symbols: Arc<StdMutex<HashMap<String, usize>>>,
    ) -> Self {
        let mut workers = HashMap::new();
        if let Some(tx) = &internal_message_tx {
            let btc = Coin::new("BTC");
            let eth = Coin::new("ETH");
            workers.insert(btc.clone(), CoinWorker::new(btc, ignore_spot, tx.clone()));
            workers.insert(eth.clone(), CoinWorker::new(eth, ignore_spot, tx.clone()));
        }
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            fill_cache: BatchQueue::new(),
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
            validation_in_progress: false,
            snapshot_requested: false,
            snapshot_fetch_in_progress: false,
            workers,
            initialization_state: InitializationState::Uninitialized,
            active_symbols,
            known_universe: HashSet::new(),
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.workers
            .values()
            .all(|worker| worker.state.lock().expect("coin worker state lock").order_book_state.is_some())
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.known_universe.clone()
    }

    #[allow(clippy::type_complexity)]
    // pops earliest set of cached updates that have the same height if possible
    fn pop_cache(
        &mut self,
    ) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>, Batch<NodeDataFill>)> {
        let diff = self.order_diff_cache.front()?;
        let status = self.order_status_cache.front()?;
        let fills = self.fill_cache.front()?;
        if diff.block_number() != status.block_number() || status.block_number() != fills.block_number() {
            return None;
        }
        let statuses = self.order_status_cache.pop_front()?;
        let diffs = self.order_diff_cache.pop_front()?;
        let fills = self.fill_cache.pop_front()?;
        Some((statuses, diffs, fills))
    }

    fn cache_update(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
        fills: Batch<NodeDataFill>,
    ) {
        if !self.validation_in_progress {
            return;
        }
        let cache = self.fetched_snapshot_cache.get_or_insert_with(VecDeque::new);
        cache.push_back((order_statuses, order_diffs, fills));
        while cache.len() > FETCHED_SNAPSHOT_CACHE_LIMIT {
            cache.pop_front();
        }
    }
    pub(crate) fn l4_book_lite_snapshot(&self, coin: String) -> Option<L4BookLiteSnapshot> {
        let coin = Coin::new(&coin);
        let worker = self.workers.get(&coin)?;
        worker.state.lock().expect("coin worker state lock").l4_book_lite_snapshot()
    }

    pub(crate) fn l4_snapshot_for_coin(&self, coin: &str) -> Option<TimedSnapshots> {
        let coin = Coin::new(coin);
        let worker = self.workers.get(&coin)?;
        worker.state.lock().expect("coin worker state lock").l4_snapshot()
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

    fn broadcast_fills(&self, fills: Batch<NodeDataFill>) {
        if let Some(tx) = &self.internal_message_tx {
            let tx = tx.clone();
            tokio::spawn(async move {
                let snapshot = Arc::new(InternalMessage::Fills { batch: fills });
                let _unused = tx.send(snapshot);
            });
        }
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                for order in batch.events_ref() {
                    self.known_universe.insert(Coin::new(&order.order.coin));
                }
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                for diff in batch.events_ref() {
                    self.known_universe.insert(diff.coin());
                }
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                for fill in batch.events_ref() {
                    self.known_universe.insert(Coin::new(&fill.1.coin));
                }
                self.fill_cache.push(batch);
            }
        }
        while let Some((order_statuses, order_diffs, fills)) = self.pop_cache() {
            let active_coins: HashSet<String> = self
                .active_symbols
                .lock()
                .expect("active_symbols lock")
                .iter()
                .filter(|(_, count)| **count > 0)
                .map(|(coin, _)| coin.clone())
                .collect();

            let filtered_statuses = order_statuses.filter_by_coin(|coin| active_coins.contains(coin));
            let filtered_diffs = order_diffs.filter_by_coin(|coin| active_coins.contains(coin));
            let filtered_fills = fills.filter_by_coin(|coin| active_coins.contains(coin));
            if !filtered_statuses.is_empty() || !filtered_diffs.is_empty() {
                self.broadcast_update(filtered_statuses, filtered_diffs);
            }
            if !filtered_fills.is_empty() {
                self.broadcast_fills(filtered_fills);
            }

            if self.validation_in_progress {
                let cached_statuses =
                    order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                let cached_diffs = order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                let cached_fills = fills.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
                self.cache_update(cached_statuses, cached_diffs, cached_fills);
                continue;
            }

            if self.initialization_state != InitializationState::Initialized {
                continue;
            }

            for coin in [Coin::new("BTC"), Coin::new("ETH")] {
                if let Some(worker) = self.workers.get(&coin) {
                    let statuses = order_statuses.filter_by_coin(|c| c == coin.value());
                    let diffs = order_diffs.filter_by_coin(|c| c == coin.value());
                    let fills = fills.filter_by_coin(|c| c == coin.value());
                    let _unused = worker.tx.send(CoinWorkerCommand::Block { order_statuses: statuses, order_diffs: diffs, fills });
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
        
        // Transfer any accumulated updates from order_status_cache/order_diff_cache/fill_cache to fetched_snapshot_cache
        while let Some((order_statuses, order_diffs, fills)) = self.pop_cache() {
            let order_statuses = order_statuses.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            let order_diffs = order_diffs.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            let fills = fills.filter_by_coin(|coin| Subscription::is_l4_snapshot_coin(coin));
            #[allow(clippy::unwrap_used)]
            self.fetched_snapshot_cache.as_mut().unwrap().push_back((order_statuses, order_diffs, fills));
        }
    }

    fn finish_validation(&mut self) -> Result<()> {
        self.validation_in_progress = false;
        let mut queued = self.fetched_snapshot_cache.take().unwrap_or_default();
        while let Some((order_statuses, order_diffs, fills)) = queued.pop_front() {
            for coin in [Coin::new("BTC"), Coin::new("ETH")] {
                if let Some(worker) = self.workers.get(&coin) {
                    let statuses = order_statuses.filter_by_coin(|c| c == coin.value());
                    let diffs = order_diffs.filter_by_coin(|c| c == coin.value());
                    let fills = fills.filter_by_coin(|c| c == coin.value());
                    if !(statuses.is_empty() && diffs.is_empty() && fills.is_empty()) {
                        let _unused = worker.tx.send(CoinWorkerCommand::Block { order_statuses: statuses, order_diffs: diffs, fills });
                    }
                }
            }
        }
        Ok(())
    }

    fn broadcast_l4_snapshot(&mut self) {
        if let Some(tx) = &self.internal_message_tx {
            for worker in self.workers.values() {
                if let Some(snapshot) = worker.state.lock().expect("coin worker state lock").l4_snapshot() {
                    let snapshot = Arc::new(InternalMessage::L4Snapshot { snapshot });
                    let _unused = tx.send(snapshot);
                }
            }
        }
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("Initializing from snapshot at height {height}");
        let snapshot = filter_l4_snapshot(snapshot);
        for (coin, book) in snapshot.as_ref() {
            if let Some(worker) = self.workers.get(coin) {
                let _unused = worker.tx.send(CoinWorkerCommand::InitSnapshot {
                    snapshot: book.clone(),
                    height,
                    time: 0,
                });
            }
        }
        self.initialization_state = InitializationState::Initialized;
        info!("Order book ready at height {height}");
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, _prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        None
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
        self.fill_cache = BatchQueue::new();
    }

    fn reset_state_for_snapshot(&mut self) {
        self.initialization_state = InitializationState::Uninitialized;
        self.clear_caches();
        self.fetched_snapshot_cache = None;
        for worker in self.workers.values() {
            let mut state = worker.state.lock().expect("coin worker state lock");
            state.order_book_state = None;
            state.lite_state = BookState::default();
        }
        // Keep validation_in_progress as false since we're starting fresh
        // It will be set to true when begin_caching() is called during next snapshot fetch
        self.validation_in_progress = false;
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
                        self.workers
                            .get(&Coin::new("BTC"))
                            .and_then(|worker| {
                                worker
                                    .state
                                    .lock()
                                    .expect("coin worker state lock")
                                    .order_book_state
                                    .as_ref()
                                    .map(OrderBookState::height)
                            }),
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
    L4BookLiteDepthUpdates { updates: L4BookLiteDepthUpdate },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
