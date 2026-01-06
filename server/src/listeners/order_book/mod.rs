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
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{BufRead, BufReader},
    path::PathBuf,
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
    time::{Instant, interval, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
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
        spawn_fifo_reader(source, path, stream_tx.clone());
    }
    let parser_listener = listener.clone();
    tokio::spawn(async move {
        while let Some(message) = stream_rx.recv().await {
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

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
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
            _ = ticker.tick() => {
                let listener = listener.clone();
                let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
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
                            validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot)
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
    last_fill: Option<u64>,
    last_event_time: Option<Instant>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
}

const FIFO_QUEUE_MAX_LEN: usize = 30;
const FIFO_BASE_DIR: &str = "/dev/shm/book_tmpfs";

struct StreamMessage {
    event_source: EventSource,
    line: String,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            order_book_state: None,
            last_fill: None,
            last_event_time: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
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
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
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
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
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
            info!("Order book ready");
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

fn spawn_fifo_reader(event_source: EventSource, path: PathBuf, tx: MpscSender<StreamMessage>) {
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
