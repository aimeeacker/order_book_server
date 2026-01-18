use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::{NaiveDateTime, TimeZone, Utc};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const FIFO_BASE_DIR: &str = "/home/aimee/hl_runtime/hl_book";
const PIPE_CAPACITY: i32 = 16 * 1024 * 1024;
const MAX_PENDING_HEIGHTS: usize = 200;
const MAIN_COINS: [&str; 2] = ["BTC", "ETH"];

#[derive(Copy, Clone, Debug)]
enum FifoSource {
    Order,
    Fills,
    Diffs,
}

impl FifoSource {
    fn name(self) -> &'static str {
        match self {
            Self::Order => "order",
            Self::Fills => "fills",
            Self::Diffs => "diffs",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchEnvelope {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    events: Vec<Value>,
}

#[derive(Debug)]
struct StreamLine {
    source: FifoSource,
    block_number: u64,
    line: Vec<u8>,
    received_at: Instant,
}

#[derive(Debug, Default)]
struct StreamQueues {
    order: VecDeque<StreamLine>,
    fills: VecDeque<StreamLine>,
    diffs: VecDeque<StreamLine>,
}

impl StreamQueues {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            fills: VecDeque::new(),
            diffs: VecDeque::new(),
        }
    }

    fn pop_aligned(&mut self) -> Option<(StreamLine, StreamLine, StreamLine)> {
        loop {
            let Some(order) = self.order.front() else { return None };
            let Some(diffs) = self.diffs.front() else { return None };
            let Some(fills) = self.fills.front() else { return None };

            let order_height = order.block_number;
            let diff_height = diffs.block_number;
            let fill_height = fills.block_number;

            if order_height == diff_height && diff_height == fill_height {
                let order = self.order.pop_front()?;
                let diffs = self.diffs.pop_front()?;
                let fills = self.fills.pop_front()?;
                return Some((order, diffs, fills));
            }

            let max_height = order_height.max(diff_height).max(fill_height);
            if order_height < max_height {
                warn!(
                    "Aligning streams: dropping order batch at {} (diff {}, fill {}, target {})",
                    order_height, diff_height, fill_height, max_height
                );
                self.order.pop_front();
                continue;
            }
            if diff_height < max_height {
                warn!(
                    "Aligning streams: dropping diff batch at {} (order {}, fill {}, target {})",
                    diff_height, order_height, fill_height, max_height
                );
                self.diffs.pop_front();
                continue;
            }
            if fill_height < max_height {
                warn!(
                    "Aligning streams: dropping fill batch at {} (order {}, diff {}, target {})",
                    fill_height, order_height, diff_height, max_height
                );
                self.fills.pop_front();
                continue;
            }
            return None;
        }
    }
}

#[derive(Debug, Serialize)]
struct MergedBatch {
    block_number: u64,
    last_local_time: NaiveDateTime,
    order: BatchEnvelope,
    diffs: BatchEnvelope,
    fills: BatchEnvelope,
}

fn fifo_path(source: FifoSource) -> PathBuf {
    PathBuf::from(FIFO_BASE_DIR).join(source.name())
}

fn set_pipe_capacity(fd: i32, source: FifoSource) {
    #[allow(unsafe_code)]
    let ret = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, PIPE_CAPACITY) };
    if ret == -1 {
        warn!("Failed to set FIFO capacity");
    } else {
        info!(
            "Set FIFO capacity for {:?} to {} bytes",
            source, PIPE_CAPACITY
        );
    }
}

fn is_main_coin(coin: &str) -> bool {
    MAIN_COINS.iter().any(|main| coin.eq_ignore_ascii_case(main))
}

fn filter_batch(batch: &mut BatchEnvelope, source: FifoSource) {
    batch.events.retain(|event| match source {
        FifoSource::Order => event
            .get("order")
            .and_then(|order| order.get("coin"))
            .and_then(Value::as_str)
            .is_some_and(is_main_coin),
        FifoSource::Diffs => event
            .get("coin")
            .and_then(Value::as_str)
            .is_some_and(is_main_coin),
        FifoSource::Fills => event
            .as_array()
            .and_then(|arr| arr.get(1))
            .and_then(|fill| fill.get("coin"))
            .and_then(Value::as_str)
            .is_some_and(is_main_coin),
    });
}

fn parse_batch_line(source: FifoSource, bytes: &mut [u8]) -> Option<(BatchEnvelope, f64, f64)> {
    let parse_start = Instant::now();
    let mut batch: BatchEnvelope = match simd_json::serde::from_slice(bytes) {
        Ok(batch) => batch,
        Err(err) => {
            let preview = String::from_utf8_lossy(&bytes[..bytes.len().min(120)]);
            warn!("{source:?} parse error {err}, payload head: {preview:?}");
            return None;
        }
    };
    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
    let filter_start = Instant::now();
    filter_batch(&mut batch, source);
    let filter_ms = filter_start.elapsed().as_secs_f64() * 1000.0;
    Some((batch, parse_ms, filter_ms))
}

fn extract_block_number(bytes: &[u8]) -> Option<u64> {
    const NEEDLE: &[u8] = b"\"block_number\":";
    let pos = bytes
        .windows(NEEDLE.len())
        .position(|window| window == NEEDLE)?;
    let mut idx = pos + NEEDLE.len();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    let mut value: u64 = 0;
    let mut found = false;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b.is_ascii_digit() {
            found = true;
            value = value * 10 + u64::from(b - b'0');
            idx += 1;
        } else {
            break;
        }
    }
    if found { Some(value) } else { None }
}

fn listen_fifo(
    source: FifoSource,
    path: PathBuf,
    stop: Arc<AtomicBool>,
    tx: SyncSender<StreamLine>,
) {
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(err) => {
                warn!("Failed to open FIFO {:?} at {}: {err}", source, path.display());
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        info!("Listening FIFO {:?} at {}", source, path.display());
        set_pipe_capacity(file.as_raw_fd(), source);
        let mut reader = BufReader::new(file);
        let mut line: Vec<u8> = Vec::new();
        let mut last_height: Option<u64> = None;
        loop {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            line.clear();
            match reader.read_until(b'\n', &mut line) {
                Ok(0) => {
                    warn!("FIFO EOF for {:?} at {}; reopening", source, path.display());
                    break;
                }
                Ok(_) => {
                    while matches!(line.last(), Some(b'\n' | b'\r')) {
                        line.pop();
                    }
                    if line.is_empty() {
                        continue;
                    }
                    let received_at = Instant::now();
                    let Some(height) = extract_block_number(&line) else {
                        let preview = String::from_utf8_lossy(&line[..line.len().min(120)]);
                        warn!(
                            "{source:?} missing block_number; payload head: {preview:?}"
                        );
                        continue;
                    };
                    if last_height.is_some_and(|last| height <= last) {
                        warn!(
                            "Dropping out-of-order {source:?} batch at height {height} (last {last_height:?})"
                        );
                        continue;
                    }
                    last_height = Some(height);
                    let msg = StreamLine {
                        source,
                        block_number: height,
                        line: line.clone(),
                        received_at,
                    };
                    if tx.send(msg).is_err() {
                        warn!("Aggregator dropped; exiting {source:?} listener");
                        return;
                    }
                }
                Err(err) => {
                    warn!("FIFO read error for {:?} at {}: {err}", source, path.display());
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn run_aggregator(rx: Receiver<StreamLine>) {
    let mut queues = StreamQueues::new();
    let mut last_emitted: Option<u64> = None;

    while let Ok(msg) = rx.recv() {
        let height = msg.block_number;
        if last_emitted.is_some_and(|last| height <= last) {
            warn!("Dropping out-of-order batch at height {height}");
            continue;
        }
        match msg.source {
            FifoSource::Order => queues.order.push_back(msg),
            FifoSource::Diffs => queues.diffs.push_back(msg),
            FifoSource::Fills => queues.fills.push_back(msg),
        }

        while queues.order.len() > MAX_PENDING_HEIGHTS {
            if let Some(dropped) = queues.order.pop_front() {
                warn!(
                    "Dropping order batch at height {} (window limit)",
                    dropped.block_number
                );
            }
        }
        while queues.diffs.len() > MAX_PENDING_HEIGHTS {
            if let Some(dropped) = queues.diffs.pop_front() {
                warn!(
                    "Dropping diff batch at height {} (window limit)",
                    dropped.block_number
                );
            }
        }
        while queues.fills.len() > MAX_PENDING_HEIGHTS {
            if let Some(dropped) = queues.fills.pop_front() {
                warn!(
                    "Dropping fill batch at height {} (window limit)",
                    dropped.block_number
                );
            }
        }

        while let Some((order, diffs, fills)) = queues.pop_aligned() {
            let height = order.block_number;
            let mut order_line = order.line;
            let mut diffs_line = diffs.line;
            let mut fills_line = fills.line;

            let Some((order_batch, order_parse_ms, order_filter_ms)) =
                parse_batch_line(FifoSource::Order, &mut order_line)
            else {
                warn!("Failed to parse order batch at height {height}");
                continue;
            };
            let Some((diffs_batch, diffs_parse_ms, diffs_filter_ms)) =
                parse_batch_line(FifoSource::Diffs, &mut diffs_line)
            else {
                warn!("Failed to parse diff batch at height {height}");
                continue;
            };
            let Some((fills_batch, fills_parse_ms, fills_filter_ms)) =
                parse_batch_line(FifoSource::Fills, &mut fills_line)
            else {
                warn!("Failed to parse fill batch at height {height}");
                continue;
            };

            let last_local_time = order_batch
                .local_time
                .max(diffs_batch.local_time)
                .max(fills_batch.local_time);
            let earliest = order
                .received_at
                .min(diffs.received_at)
                .min(fills.received_at);
            let latest = order
                .received_at
                .max(diffs.received_at)
                .max(fills.received_at);
            let align_wait_ms = (latest - earliest).as_secs_f64() * 1000.0;

            let merge_start = Instant::now();
            let merged = MergedBatch {
                block_number: height,
                last_local_time,
                order: order_batch,
                diffs: diffs_batch,
                fills: fills_batch,
            };
            let encode_start = Instant::now();
            let _encoded = match serde_json::to_string(&merged) {
                Ok(encoded) => encoded,
                Err(err) => {
                    warn!("Failed to encode merged batch at height {height}: {err}");
                    continue;
                }
            };
            let json_encode_ms = encode_start.elapsed().as_secs_f64() * 1000.0;
            let merge_total_ms = merge_start.elapsed().as_secs_f64() * 1000.0;
            let merge_delay_ms = (merge_start - latest).as_secs_f64() * 1000.0;
            let end_to_end_ms = localtime_to_now_ms(last_local_time).unwrap_or(-1.0);
            let merge_overhead_ms = (merge_total_ms - json_encode_ms).max(0.0);

            info!(
                "Merged height {height} parse_ms={{order:{order_parse_ms:.3},diffs:{diffs_parse_ms:.3},fills:{fills_parse_ms:.3}}} filter_ms={{order:{order_filter_ms:.3},diffs:{diffs_filter_ms:.3},fills:{fills_filter_ms:.3}}} align_wait_ms={align_wait_ms:.3} merge_delay_ms={merge_delay_ms:.3} json_encode_ms={json_encode_ms:.3} merge_overhead_ms={merge_overhead_ms:.3} end_to_end_ms={end_to_end_ms:.3}",
            );
            last_emitted = Some(height);
        }
    }
}

fn localtime_to_now_ms(local_time: NaiveDateTime) -> Option<f64> {
    let utc_dt = Utc.from_utc_datetime(&local_time);
    let delta = Utc::now().signed_duration_since(utc_dt);
    let micros = delta.num_microseconds()?;
    Some(micros as f64 / 1000.0)
}

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_micros()
        .init();
    info!("fifo_listener starting");

    let (tx, rx) = sync_channel(512);
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for source in [FifoSource::Order, FifoSource::Fills, FifoSource::Diffs] {
        let stop = stop.clone();
        let tx = tx.clone();
        let path = fifo_path(source);
        let handle = thread::spawn(move || listen_fifo(source, path, stop, tx));
        handles.push(handle);
    }

    run_aggregator(rx);

    stop.store(true, Ordering::SeqCst);
    for handle in handles {
        let _unused = handle.join();
    }
}
