use crate::{
    HL_NODE,
    listeners::order_book::state::OrderBookState,
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info, warn};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering as AtomicOrdering},
    sync::Arc,
    thread,
    time::Duration,
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
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;
pub(crate) mod lite;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let dropped_updates = Arc::new(AtomicBool::new(false));
    let fifo_paths = [
        (EventSource::OrderStatuses, fifo_path(EventSource::OrderStatuses)),
        (EventSource::Fills, fifo_path(EventSource::Fills)),
        (EventSource::OrderDiffs, fifo_path(EventSource::OrderDiffs)),
    ];
    for (source, path) in &fifo_paths {
        info!("Monitoring FIFO for {source}: {}", path.display());
    }

    let (stream_tx, mut stream_rx) = channel::<StreamMessage>(FIFO_QUEUE_MAX_LEN);
    for (source, path) in fifo_paths {
        spawn_fifo_reader(source, path, stream_tx.clone(), dropped_updates.clone());
    }
    let parser_listener = listener.clone();
    let parser_dropped_updates = dropped_updates.clone();
    tokio::spawn(async move {
        while let Some(message) = stream_rx.recv().await {
            if parser_dropped_updates.swap(false, AtomicOrdering::SeqCst) {
                let mut listener = parser_listener.lock().await;
                listener.reset_after_drop();
            }
            if let Err(err) = parser_listener.lock().await.process_stream_line(message.line, message.event_source) {
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

    // Initialize Scheduler
    let sched = JobScheduler::new().await.map_err(|e| format!("Failed to create scheduler: {e}"))?;
    
    // Create the job to run at 30th second of every minute
    let job_dir = dir.clone();
    let job_listener = listener.clone();
    let job_tx = snapshot_fetch_task_tx.clone();
    let job = Job::new("30 * * * * *", move |_uuid, _l| {
        let listener = job_listener.clone();
        let snapshot_fetch_task_tx = job_tx.clone();
        fetch_snapshot(job_dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
    }).map_err(|e| format!("Failed to create cron job: {e}"))?;

    sched.add(job).await.map_err(|e| format!("Failed to add job: {e}"))?;
    sched.start().await.map_err(|e| format!("Failed to start scheduler: {e}"))?;

    info!("Cron scheduler started: snapshot validation set to run at 30s mark of every minute.");
    
    // Trigger initial snapshot fetch
    info!("Triggering initial snapshot fetch");
    fetch_snapshot(dir.clone(), listener.clone(), snapshot_fetch_task_tx.clone(), ignore_spot);

    let mut health_check = interval(Duration::from_secs(5));
    loop {
        tokio::select! {
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
            _ = health_check.tick() => {
                let listener = listener.lock().await;
                if listener.is_ready() && listener.is_stalled(Duration::from_secs(5)) {
                    return Err(format!("Stream has fallen behind ({HL_NODE} failed?)").into());
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let state = {
                    let mut listener = listener.lock().await;
                    listener.begin_caching();
                    listener.clone_state()
                };
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let mut cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(mut state) = state {
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                    state.apply_updates(order_statuses, order_diffs)?;
                                } else {
                                    return Err::<(), Error>("Not enough cached updates".into());
                                }
                            }
                            if state.height() > height {
                                return Err("Fetched snapshot lagging stored state".into());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating snapshot");
                            tokio::task::spawn_blocking(move || {
                                validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot)
                            })
                            .await
                            .map_err(|e| format!("Snapshot validation task failed: {}", e))?
                        } else {
                            listener.lock().await.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_event_time: Option<Instant>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    fills_cache: BatchQueue<NodeDataFill>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    active_coins: Option<Arc<Mutex<HashMap<String, usize>>>>,
    pub lite_books: HashMap<Coin, lite::BookState>,
}

const FIFO_QUEUE_MAX_LEN: usize = 255;
const FIFO_BASE_DIR: &str = "/dev/shm/book_tmpfs";
const MAIN_COINS: [&str; 2] = ["BTC", "ETH"];

struct StreamMessage {
    event_source: EventSource,
    line: String,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>, 
        active_coins: Option<Arc<Mutex<HashMap<String, usize>>>>,
        ignore_spot: bool
    ) -> Self {
        Self {
            ignore_spot,
            order_book_state: None,
            last_event_time: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            fills_cache: BatchQueue::new(),
            active_coins,
            lite_books: HashMap::new(),
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
        self.order_diff_cache = BatchQueue::new();
        self.order_status_cache = BatchQueue::new();
        self.fills_cache = BatchQueue::new();
        self.fetched_snapshot_cache = None;
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

    #[allow(clippy::type_complexity)]
    // pops triplets of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>, Batch<NodeDataFill>)> {
        // synchronize to same block
        // Assuming all streams advance roughly together.
        // We use min() to ensure we process every block available across streams.
        // If a stream is "ahead" (higher block), we assume it has no data (empty) for the current target (lower block).
        while let (Some(diff), Some(status), Some(fill)) = (
            self.order_diff_cache.front(),
            self.order_status_cache.front(),
            self.fills_cache.front(),
        ) {
            let diff_n = diff.block_number();
            let status_n = status.block_number();
            let fill_n = fill.block_number();

            // Target the earliest available block
            let target = diff_n.min(status_n).min(fill_n);
            
            // If any stream is lagging (empty?), we essentially require all streams to have AT LEAST reached target.
            // But since we picked min(), all streams are >= target.
            // If stream.block == target, we use it. 
            // If stream.block > target, it means that stream has NO events for `target`. Use empty batch.
            
            let status_batch = if status_n == target {
                self.order_status_cache.pop_front().unwrap()
            } else {
                Batch::new(target, status.block_time(), Vec::new()) // Synthetic empty batch
            };
            
            let diff_batch = if diff_n == target {
                self.order_diff_cache.pop_front().unwrap()
            } else {
                Batch::new(target, diff.block_time(), Vec::new())
            };

            let fill_batch = if fill_n == target {
                self.fills_cache.pop_front().unwrap()
            } else {
                 Batch::new(target, fill.block_time(), Vec::new())
            };
            
            return Some((status_batch, diff_batch, fill_batch));
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(mut batch) => {
                let active_coins = if let Some(am) = &self.active_coins {
                    match am.try_lock() {
                        Ok(g) => g.clone(),
                        Err(_) => HashMap::new(),
                    }
                } else {
                    HashMap::new()
                };
                batch.retain(|ev| {
                    let coin = &ev.order.coin;
                    MAIN_COINS.contains(&coin.as_str()) || active_coins.contains_key(coin)
                });
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(mut batch) => {
                let active_coins = if let Some(am) = &self.active_coins {
                    match am.try_lock() {
                        Ok(g) => g.clone(),
                        Err(_) => HashMap::new(),
                    }
                } else {
                    HashMap::new()
                };
                batch.retain(|ev| {
                    let coin = ev.coin();
                    MAIN_COINS.contains(&coin.value().as_str()) || active_coins.contains_key(coin.value().as_str())
                });
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(mut batch) => {
                let active_coins = if let Some(am) = &self.active_coins {
                    match am.try_lock() {
                        Ok(g) => g.clone(),
                        Err(_) => HashMap::new(),
                    }
                } else {
                    HashMap::new()
                };
                batch.retain(|ev| {
                     // NodeDataFill(pub Address, pub Fill) -> ev.1 is Fill
                     let coin = &ev.1.coin;
                     MAIN_COINS.contains(&coin.as_str()) || active_coins.contains_key(coin)
                });
                self.fills_cache.push(batch);
            }
        }    
        if self.is_ready() {
            while let Some((order_statuses, order_diffs, fills)) = self.pop_cache() {
                // Get aligned block height (target)
                // pop_cache aligns all to same target
                let target_height = order_statuses.block_number();

                // Main coins update state
                // We clone the batches for state update, keeping only Main coins
                let mut state_statuses = order_statuses.clone();
                state_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));
                
                let mut state_diffs = order_diffs.clone();
                state_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));

                // L4Lite Updates
                let mut all_lite_updates = Vec::new();
                let mut all_analysis_updates_b = Vec::new();
                let mut all_analysis_updates_a = Vec::new();

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

                for (coin, lb) in &mut self.lite_books {
                     let statuses = coin_statuses.remove(coin).unwrap_or_default();
                     let diffs = coin_diffs.remove(coin).unwrap_or_default();
                     let fills = coin_fills.remove(coin).unwrap_or_default();
                     
                     let block_event = lb.process_block(coin.clone(), &statuses, &diffs, &fills, target_height);
                     
                     if let Some(du) = block_event.depth_updates {
                         all_lite_updates.push(du);
                     }
                     if !block_event.analysis_updates_b.is_empty() {
                         all_analysis_updates_b.extend(block_event.analysis_updates_b);
                     }
                     if !block_event.analysis_updates_a.is_empty() {
                         all_analysis_updates_a.extend(block_event.analysis_updates_a);
                     }
                }


                // Always send L4Lite message to propagate block height, even if empty
                if let Some(tx) = &self.internal_message_tx {
                    let msg = Arc::new(InternalMessage::L4Lite { 
                        updates: all_lite_updates,
                        analysis_b: all_analysis_updates_b,
                        analysis_a: all_analysis_updates_a,
                        height: target_height,
                    });
                    // broadcast::Sender::send is synchronous and non-blocking.
                    // Do NOT spawn a task here, or messages will be reordered.
                    let _unused = tx.send(msg);
                }

                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(state_statuses, state_diffs))
                    .transpose()?;
                
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    // Cache full updates (Filtered by Active | Main already) for snapshot catch-up
                    // The cache is used to replay state when fetching a new snapshot
                    // Since we only maintain MAIN_COIN state, we should only cache MAIN_COIN updates for replay.
                    
                    let mut cache_statuses = order_statuses.clone();
                    cache_statuses.retain(|ev| MAIN_COINS.contains(&ev.order.coin.as_str()));
                    
                    let mut cache_diffs = order_diffs.clone();
                    cache_diffs.retain(|ev| MAIN_COINS.contains(&ev.coin().value().as_str()));
                    
                    cache.push_back((cache_statuses, cache_diffs));
                }

                // Forward Active | Main updates to Websocket
                // order_statuses/diffs are already filtered by (Active | Main) at the top of receive_batch
                if let Some(tx) = &self.internal_message_tx {
                    let updates = Arc::new(InternalMessage::L4BookUpdates {
                        diff_batch: order_diffs,
                        status_batch: order_statuses,
                    });
                    let _unused = tx.send(updates);

                    let fills_msg = Arc::new(InternalMessage::Fills {
                        batch: fills,
                    });
                    let _unused = tx.send(fills_msg);
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        // We only maintain LOCAL state for MAIN_COINS.
        // Filter the snapshot before creating OrderBookState.
        let mut books = snapshot.value();
        books.retain(|coin, _| MAIN_COINS.contains(&coin.value().as_str()));

        // Initialize Lite books
        self.lite_books.clear();
        for (coin, orders) in &books {
            let mut lb = lite::BookState::new();
            lb.init_from_snapshot(orders, height);
            self.lite_books.insert(coin.clone(), lb);
        }

        let snapshot = Snapshots::new(books);

        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs, fills)) = self.pop_cache() {
            // Filter cache updates for MAIN_COINS before applying to initial state.
            // Pop cache returns full batches (active | main). 
            // We need to filter them the same way we do in receive_batch before apply_updates.
            
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

            for (coin, lb) in &mut self.lite_books {
                 let statuses = coin_statuses.remove(coin).unwrap_or_default();
                 let diffs = coin_diffs.remove(coin).unwrap_or_default();
                 let fills = coin_fills.remove(coin).unwrap_or_default();
                 
                 lb.process_block(coin.clone(), &statuses, &diffs, &fills, target_height);
            }

            if new_order_book.apply_updates(state_statuses, state_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
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

impl OrderBookListener {
    fn parse_batch_line(&self, line: &str, event_source: EventSource) -> Result<(u64, EventBatch)> {
        match event_source {
            EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line)
                .map(|batch| (batch.block_number(), EventBatch::Fills(batch)))
                .map_err(Into::into),
            EventSource::OrderStatuses => serde_json::from_str::<Batch<NodeDataOrderStatus>>(line)
                .map(|batch| (batch.block_number(), EventBatch::Orders(batch)))
                .map_err(Into::into),
            EventSource::OrderDiffs => serde_json::from_str::<Batch<NodeDataOrderDiff>>(line)
                .map(|batch| (batch.block_number(), EventBatch::BookDiffs(batch)))
                .map_err(Into::into),
        }
    }

    fn process_stream_line(&mut self, line: String, event_source: EventSource) -> Result<()> {
        if line.is_empty() {
            return Ok(());
        }
        self.last_event_time = Some(Instant::now());
        let (height, event_batch) = match self.parse_batch_line(&line, event_source) {
            Ok(data) => data,
            Err(err) => {
                error!(
                    "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                    self.order_book_state.as_ref().map(OrderBookState::height),
                    &line[..line.len().min(100)],
                );
                return Ok(());
            }
        };
        if height % 100 == 0 {
            info!("{event_source} block: {height}");
        }
        if let Err(err) = self.receive_batch(event_batch) {
            self.order_book_state = None;
            return Err(err);
        }
        // global L2Book logic disabled for now
        /*
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
        */
        Ok(())
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
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
    L4Lite { updates: Vec<lite::L2BlockUpdate>, analysis_b: Vec<lite::AnalysisUpdate>, analysis_a: Vec<lite::AnalysisUpdate>, height: u64 },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

fn fifo_path(event_source: EventSource) -> PathBuf {
    let name = match event_source {
        EventSource::Fills => "fills",
        EventSource::OrderStatuses => "order",
        EventSource::OrderDiffs => "diffs",
    };
    PathBuf::from(FIFO_BASE_DIR).join(name)
}

fn spawn_fifo_reader(
    event_source: EventSource,
    path: PathBuf,
    tx: MpscSender<StreamMessage>,
    dropped_updates: Arc<AtomicBool>,
) {
    thread::spawn(move || loop {
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(err) => {
                error!("Failed to open FIFO for {event_source} at {}: {err}", path.display());
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim_end_matches('\n');
                    if trimmed.is_empty() {
                        continue;
                    }
                    let message = StreamMessage {
                        event_source,
                        line: trimmed.to_string(),
                    };
                    if let Err(err) = tx.try_send(message) {
                        dropped_updates.store(true, AtomicOrdering::SeqCst);
                        warn!("FIFO queue full for {event_source}. Dropping message: {err}");
                    }
                }
                Err(err) => {
                    error!("FIFO read error for {event_source}: {err}");
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    });
}
