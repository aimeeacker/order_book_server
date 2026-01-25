use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::time::Duration;

use log::{info, warn};
use memchr::{memchr, memmem};

const FIFO_BASE_DIR: &str = "/home/aimee/hl_runtime/hl_book/node_fifo";
const ORDER_PIPE_CAPACITY: i32 = 16 * 1024 * 1024;
const DIFFS_FILLS_PIPE_CAPACITY: i32 = 8 * 1024 * 1024;
const MAX_PENDING_HEIGHTS: usize = 200;
const UDS_PATH: &str = "/home/aimee/hl_runtime/hl_book/fifo_listener.sock";
const SOCKET_BUFFER: i32 = 16 * 1024 * 1024;
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(1);
const DROP_LOG_THRESHOLD: u64 = 50;

static LOG_INIT: Once = Once::new();

pub type HeightCallback = Arc<dyn Fn(u64) + Send + Sync + 'static>;

struct RollbackTracker {
    state: Mutex<RollbackState>,
}

struct RollbackState {
    flags: [bool; 3],
    generation: u64,
}

impl RollbackTracker {
    fn new() -> Self {
        Self {
            state: Mutex::new(RollbackState {
                flags: [false; 3],
                generation: 0,
            }),
        }
    }

    fn generation(&self) -> Option<u64> {
        self.state.lock().ok().map(|state| state.generation)
    }

    fn record_rollback(&self, source: FifoSource) -> Option<u64> {
        self.state.lock().ok().map(|mut state| {
            state.flags[source.idx()] = true;
            if state.flags.iter().all(|flag| *flag) {
                state.flags = [false; 3];
                state.generation += 1;
            }
            state.generation
        })
    }

    fn clear_rollback(&self, source: FifoSource) {
        if let Ok(mut state) = self.state.lock() {
            state.flags[source.idx()] = false;
        }
    }
}

pub struct ListenerHandle {
    stop: Arc<AtomicBool>,
    stop_eventfd: RawFd,
    threads: Vec<thread::JoinHandle<()>>,
}

impl ListenerHandle {
    pub fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        signal_eventfd(self.stop_eventfd);
        for thread in self.threads {
            let _unused = thread.join();
        }
        close_fd(self.stop_eventfd);
    }
}

pub fn init_cli_logging() {
    LOG_INIT.call_once(|| {
        let _unused = env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .format_timestamp_micros()
            .try_init();
    });
}

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

    fn idx(self) -> usize {
        match self {
            Self::Order => 0,
            Self::Diffs => 1,
            Self::Fills => 2,
        }
    }
}

#[derive(Debug)]
struct StreamLine {
    source: FifoSource,
    block_number: u64,
    line: Vec<u8>,
}

struct StreamQueues {
    order: VecDeque<StreamLine>,
    fills: VecDeque<StreamLine>,
    diffs: VecDeque<StreamLine>,
    drops: DropLog,
}

impl StreamQueues {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            fills: VecDeque::new(),
            diffs: VecDeque::new(),
            drops: DropLog::new(),
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
                self.drops.record_align_drop(
                    FifoSource::Order,
                    AlignDropDetail {
                        order: order_height,
                        diff: diff_height,
                        fill: fill_height,
                        target: max_height,
                    },
                );
                self.order.pop_front();
                continue;
            }
            if diff_height < max_height {
                self.drops.record_align_drop(
                    FifoSource::Diffs,
                    AlignDropDetail {
                        order: order_height,
                        diff: diff_height,
                        fill: fill_height,
                        target: max_height,
                    },
                );
                self.diffs.pop_front();
                continue;
            }
            if fill_height < max_height {
                self.drops.record_align_drop(
                    FifoSource::Fills,
                    AlignDropDetail {
                        order: order_height,
                        diff: diff_height,
                        fill: fill_height,
                        target: max_height,
                    },
                );
                self.fills.pop_front();
                continue;
            }

            return None;
        }
    }

    fn record_window_drop(&mut self, source: FifoSource, height: u64) {
        self.drops.record_window_drop(source, height);
    }
}

#[derive(Clone, Copy)]
struct AlignDropDetail {
    order: u64,
    diff: u64,
    fill: u64,
    target: u64,
}

struct DropLog {
    last_align_log: std::time::Instant,
    align_counts: [u64; 3],
    align_last: [Option<AlignDropDetail>; 3],
    last_window_log: std::time::Instant,
    window_counts: [u64; 3],
    window_last: [Option<u64>; 3],
}

impl DropLog {
    fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            last_align_log: now,
            align_counts: [0; 3],
            align_last: [None, None, None],
            last_window_log: now,
            window_counts: [0; 3],
            window_last: [None, None, None],
        }
    }

    fn record_align_drop(&mut self, source: FifoSource, detail: AlignDropDetail) {
        let idx = source.idx();
        self.align_counts[idx] += 1;
        self.align_last[idx] = Some(detail);
        self.maybe_log_align();
    }

    fn record_window_drop(&mut self, source: FifoSource, height: u64) {
        let idx = source.idx();
        self.window_counts[idx] += 1;
        self.window_last[idx] = Some(height);
        self.maybe_log_window();
    }

    fn maybe_log_align(&mut self) {
        let total: u64 = self.align_counts.iter().sum();
        if total == 0 {
            return;
        }
        if total < DROP_LOG_THRESHOLD && self.last_align_log.elapsed() < DROP_LOG_INTERVAL {
            return;
        }
        let elapsed_ms = self.last_align_log.elapsed().as_secs_f64() * 1000.0;
        warn!(
            "Align drops last {:.0}ms: order={} (last {}) diffs={} (last {}) fills={} (last {})",
            elapsed_ms,
            self.align_counts[0],
            format_align_detail(self.align_last[0]),
            self.align_counts[1],
            format_align_detail(self.align_last[1]),
            self.align_counts[2],
            format_align_detail(self.align_last[2]),
        );
        self.align_counts = [0; 3];
        self.align_last = [None, None, None];
        self.last_align_log = std::time::Instant::now();
    }

    fn maybe_log_window(&mut self) {
        let total: u64 = self.window_counts.iter().sum();
        if total == 0 {
            return;
        }
        if total < DROP_LOG_THRESHOLD && self.last_window_log.elapsed() < DROP_LOG_INTERVAL {
            return;
        }
        let elapsed_ms = self.last_window_log.elapsed().as_secs_f64() * 1000.0;
        warn!(
            "Window drops last {:.0}ms: order={} (last {}) diffs={} (last {}) fills={} (last {})",
            elapsed_ms,
            self.window_counts[0],
            format_window_detail(self.window_last[0]),
            self.window_counts[1],
            format_window_detail(self.window_last[1]),
            self.window_counts[2],
            format_window_detail(self.window_last[2]),
        );
        self.window_counts = [0; 3];
        self.window_last = [None, None, None];
        self.last_window_log = std::time::Instant::now();
    }
}

fn format_align_detail(detail: Option<AlignDropDetail>) -> String {
    match detail {
        Some(detail) => format!(
            "o={} d={} f={} target={}",
            detail.order, detail.diff, detail.fill, detail.target
        ),
        None => "n/a".to_string(),
    }
}

fn format_window_detail(height: Option<u64>) -> String {
    match height {
        Some(height) => height.to_string(),
        None => "n/a".to_string(),
    }
}

struct UdsServer {
    listener_fd: i32,
    client_fd: Option<i32>,
    path: PathBuf,
    pending_payload: Option<Vec<u8>>,
}

impl UdsServer {
    #[allow(unsafe_code)]
    fn bind(path: PathBuf) -> std::io::Result<Self> {
        let _unused = std::fs::remove_file(&path);
        // Use SOCK_STREAM for byte stream (no message size limits)
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let bytes = path.as_os_str().as_bytes();
        if bytes.len() >= addr.sun_path.len() {
            close_fd(fd);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "UDS path too long",
            ));
        }
        for (dst, src) in addr.sun_path.iter_mut().zip(bytes.iter()) {
            *dst = *src as libc::c_char;
        }

        let addr_len = size_of::<libc::sockaddr_un>() as libc::socklen_t;
        let addr_ptr: *const libc::sockaddr_un = &addr;
        let addr_ptr = addr_ptr.cast::<libc::sockaddr>();
        let bind_rc = unsafe {
            libc::bind(
                fd,
                addr_ptr,
                addr_len,
            )
        };
        if bind_rc < 0 {
            close_fd(fd);
            return Err(std::io::Error::last_os_error());
        }

        if unsafe { libc::listen(fd, 1) } < 0 {
            close_fd(fd);
            return Err(std::io::Error::last_os_error());
        }

        set_fd_nonblocking(fd)?;
        set_socket_buffers(fd)?;

        Ok(Self {
            listener_fd: fd,
            client_fd: None,
            path,
            pending_payload: None,
        })
    }

    fn try_accept(&mut self) {
        if self.client_fd.is_some() {
            return;
        }
        #[allow(unsafe_code)]
        let fd = unsafe {
            libc::accept(self.listener_fd, std::ptr::null_mut(), std::ptr::null_mut())
        };
        if fd < 0 {
            return;
        }
        if let Err(err) = set_fd_nonblocking(fd) {
            warn!("Failed to set UDS client nonblocking: {err}");
        }
        if let Err(err) = set_socket_buffers(fd) {
            warn!("Failed to set UDS client buffers: {err}");
        }
        info!("UDS client connected at {}", self.path.display());
        self.client_fd = Some(fd);
        self.flush_pending();
    }

    fn send(&mut self, payload: &[u8]) {
        if self.client_fd.is_none() {
            self.pending_payload = Some(payload.to_vec());
            return;
        }
        if self.pending_payload.is_some() {
            self.pending_payload = Some(payload.to_vec());
            self.flush_pending();
            return;
        }
        if !self.send_now(payload) {
            self.pending_payload = Some(payload.to_vec());
        }
    }

    fn flush_pending(&mut self) {
        let Some(payload) = self.pending_payload.take() else {
            return;
        };
        if !self.send_now(&payload) {
            self.pending_payload = Some(payload);
        }
    }

    fn send_now(&mut self, payload: &[u8]) -> bool {
        let Some(fd) = self.client_fd else {
            return false;
        };
        #[allow(unsafe_code)]
        let rc = unsafe {
            libc::send(
                fd,
                payload.as_ptr() as *const libc::c_void,
                payload.len(),
                libc::MSG_DONTWAIT,
            )
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::BrokenPipe {
                info!("UDS client disconnected (Broken Pipe); closing connection");
                close_fd(fd);
                self.client_fd = None;
                return false;
            }

            let mut queued: i32 = 0;
            // TIOCOUTQ gets the amount of data in the output buffer
            #[allow(unsafe_code)]
            unsafe { libc::ioctl(fd, libc::TIOCOUTQ, &mut queued) };

            let mut sndbuf: i32 = 0;
            let mut len = size_of::<i32>() as libc::socklen_t;
            #[allow(unsafe_code)]
            unsafe {
                libc::getsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    std::ptr::addr_of_mut!(sndbuf) as *mut libc::c_void,
                    &mut len,
                )
            };

            warn!(
                "UDS send failed: {err} (queued: {:.3} MB, sndbuf: {:.3} MB, payload: {:.3} MB); dropping payload",
                queued as f64 / 1024.0 / 1024.0,
                sndbuf as f64 / 1024.0 / 1024.0,
                payload.len() as f64 / 1024.0 / 1024.0
            );
            return false;
        }
        if rc as usize != payload.len() {
            warn!(
                "UDS send short write: {}/{} bytes; closing connection to avoid stream corruption",
                rc,
                payload.len()
            );
            close_fd(fd);
            self.client_fd = None;
            return false;
        }
        true
    }
}

impl Drop for UdsServer {
    fn drop(&mut self) {
        if let Some(fd) = self.client_fd.take() {
            close_fd(fd);
        }
        close_fd(self.listener_fd);
        let _unused = std::fs::remove_file(&self.path);
    }
}

#[allow(unsafe_code)]
fn close_fd(fd: i32) {
    unsafe { libc::close(fd) };
}

#[allow(unsafe_code)]
fn create_eventfd() -> std::io::Result<RawFd> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

fn signal_eventfd(fd: RawFd) {
    let value: u64 = 1;
    #[allow(unsafe_code)]
    let rc = unsafe {
        libc::write(
            fd,
            std::ptr::addr_of!(value).cast::<libc::c_void>(),
            size_of::<u64>(),
        )
    };
    if rc < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::WouldBlock {
            warn!("Failed to signal eventfd: {err}");
        }
    }
}

fn drain_eventfd(fd: RawFd) {
    let mut value: u64 = 0;
    #[allow(unsafe_code)]
    unsafe {
        libc::read(
            fd,
            std::ptr::addr_of_mut!(value).cast::<libc::c_void>(),
            size_of::<u64>(),
        )
    };
}

#[allow(unsafe_code)]
fn set_fd_nonblocking(fd: i32) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[allow(unsafe_code)]
fn set_socket_buffers(fd: i32) -> std::io::Result<()> {
    let size = SOCKET_BUFFER;
    let size_ptr: *const i32 = &size;
    let size_ptr = size_ptr.cast::<libc::c_void>();
    let size_len = size_of::<i32>() as libc::socklen_t;
    
    // Set Send Buffer
    let snd_rc = unsafe {
        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, size_ptr, size_len)
    };
    if snd_rc < 0 {
        return Err(std::io::Error::last_os_error());
    }

    // Set Receive Buffer
    let rcv_rc = unsafe {
        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, size_ptr, size_len)
    };
    if rcv_rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    
    Ok(())
}

fn fifo_path(source: FifoSource) -> PathBuf {
    PathBuf::from(FIFO_BASE_DIR).join(source.name())
}

fn pipe_capacity_for(source: FifoSource) -> i32 {
    match source {
        FifoSource::Order => ORDER_PIPE_CAPACITY,
        FifoSource::Diffs | FifoSource::Fills => DIFFS_FILLS_PIPE_CAPACITY,
    }
}

fn set_pipe_capacity(fd: i32, capacity: i32) -> std::io::Result<i32> {
    #[allow(unsafe_code)]
    let ret = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, capacity) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(ret)
}

fn extract_block_number(bytes: &[u8]) -> Option<u64> {
    const NEEDLE: &[u8] = b"\"block_number\":";
    let pos = memmem::find(bytes, NEEDLE)?;
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
    stop_eventfd: RawFd,
    rollback: Arc<RollbackTracker>,
    tx: SyncSender<StreamLine>,
) {
    let mut scratch = [0u8; 8192];
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        thread::sleep(Duration::from_secs(1));
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(err) => {
                warn!("Failed to open FIFO {:?} at {}: {err}", source, path.display());
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        if let Err(err) = set_fd_nonblocking(file.as_raw_fd()) {
            warn!("Failed to set FIFO {:?} nonblocking at {}: {err}", source, path.display());
        }
        match set_pipe_capacity(file.as_raw_fd(), pipe_capacity_for(source)) {
            Ok(size) => info!(
                "Listening FIFO {:?} at {} (pipe_capacity={:.2} MB)",
                source,
                path.display(),
                size as f64 / 1024.0 / 1024.0
            ),
            Err(err) => warn!(
                "Listening FIFO {:?} at {} (pipe_capacity=failed: {err})",
                source,
                path.display()
            ),
        }

        let warmup_deadline = std::time::Instant::now() + Duration::from_millis(500);
        let mut warmup_active = true;
        let mut pending: Vec<u8> = Vec::new();
        let mut last_height: Option<u64> = None;
        let mut rollback_generation = rollback.generation().unwrap_or(0);

        loop {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            if warmup_active && std::time::Instant::now() >= warmup_deadline {
                warmup_active = false;
                pending.clear();
            }

            let mut fds = [
                libc::pollfd {
                    fd: file.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: stop_eventfd,
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            #[allow(unsafe_code)]
            let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
            if rc < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                warn!("FIFO poll error for {:?} at {}: {err}", source, path.display());
                break;
            }

            if (fds[1].revents & libc::POLLIN) != 0 {
                drain_eventfd(stop_eventfd);
                return;
            }

            if (fds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)) == 0 {
                continue;
            }

            let mut reopen = false;
            loop {
                #[allow(unsafe_code)]
                let n = unsafe {
                    libc::read(
                        file.as_raw_fd(),
                        scratch.as_mut_ptr().cast::<libc::c_void>(),
                        scratch.len(),
                    )
                };
                if n > 0 {
                    pending.extend_from_slice(&scratch[..n as usize]);
                    while let Some(pos) = memchr(b'\n', &pending) {
                        let mut line = pending.drain(..=pos).collect::<Vec<u8>>();
                        if line.last() == Some(&b'\n') {
                            line.pop();
                        }
                        if line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        if warmup_active {
                            continue;
                        }
                        if line.is_empty() {
                            continue;
                        }
                        let Some(height) = extract_block_number(&line) else {
                            let preview = String::from_utf8_lossy(&line[..line.len().min(120)]);
                            warn!("{source:?} missing block_number; payload head: {preview:?}");
                            continue;
                        };
                        if let Some(generation) = rollback.generation() {
                            if generation != rollback_generation {
                                rollback_generation = generation;
                                last_height = None;
                            }
                        }
                        if last_height.is_some_and(|last| height <= last) {
                            let mut reset = false;
                            if let Some(generation) = rollback.record_rollback(source) {
                                if generation != rollback_generation {
                                    rollback_generation = generation;
                                    last_height = None;
                                    reset = true;
                                }
                            }
                            if !reset {
                                warn!(
                                    "Dropping out-of-order {source:?} batch at height {height} (last {last_height:?})"
                                );
                                continue;
                            }
                        }
                        last_height = Some(height);
                        rollback.clear_rollback(source);
                        let msg = StreamLine {
                            source,
                            block_number: height,
                            line,
                        };
                        if tx.send(msg).is_err() {
                            warn!("Aggregator dropped; exiting {source:?} listener");
                            return;
                        }
                    }
                    continue;
                }
                if n == 0 {
                    warn!("FIFO EOF for {:?} at {}; reopening", source, path.display());
                    reopen = true;
                    break;
                }
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    break;
                }
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                warn!("FIFO read error for {:?} at {}: {err}", source, path.display());
                reopen = true;
                break;
            }

            if reopen {
                break;
            }
        }

        thread::sleep(Duration::from_millis(100));
    }
}

fn run_aggregator(rx: Receiver<StreamLine>, stop: Arc<AtomicBool>, callback: Option<HeightCallback>) {
    let mut queues = StreamQueues::new();
    let uds_path = PathBuf::from(UDS_PATH);
    let mut uds = match bind_uds_with_retry(uds_path) {
        Some(server) => Some(server),
        None => {
            warn!("Failed to bind UDS {} after 3 attempts; exiting", UDS_PATH);
            return;
        }
    };

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let msg = match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(msg) => msg,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match msg.source {
            FifoSource::Order => queues.order.push_back(msg),
            FifoSource::Diffs => queues.diffs.push_back(msg),
            FifoSource::Fills => queues.fills.push_back(msg),
        }

        while queues.order.len() > MAX_PENDING_HEIGHTS {
            if let Some(dropped) = queues.order.pop_front() {
                queues.record_window_drop(FifoSource::Order, dropped.block_number);
            }
        }
        while queues.diffs.len() > MAX_PENDING_HEIGHTS {
            if let Some(dropped) = queues.diffs.pop_front() {
                queues.record_window_drop(FifoSource::Diffs, dropped.block_number);
            }
        }
        while queues.fills.len() > MAX_PENDING_HEIGHTS {
            if let Some(dropped) = queues.fills.pop_front() {
                queues.record_window_drop(FifoSource::Fills, dropped.block_number);
            }
        }

        while let Some((order, diffs, fills)) = queues.pop_aligned() {
            let height = order.block_number;
            
            let mut merged = Vec::with_capacity(
                fills.line.len() + diffs.line.len() + order.line.len() + 3,
            );
            merged.push(b'[');
            merged.extend_from_slice(&fills.line);
            merged.push(b',');
            merged.extend_from_slice(&diffs.line);
            merged.push(b',');
            merged.extend_from_slice(&order.line);
            merged.push(b']');
            merged.push(b'\n'); // Newline delimiter for stream mode

            if let Some(server) = uds.as_mut() {
                server.try_accept();
                server.send(&merged);
            }

            if height % 100 == 0 {
                if let Some(cb) = callback.as_ref() {
                    cb(height);
                }
            }

            if height % 2000 == 0 {
                info!("Processed block height {}", height);
            }
        }
    }
}

fn bind_uds_with_retry(path: PathBuf) -> Option<UdsServer> {
    for attempt in 1..=3 {
        match UdsServer::bind(path.clone()) {
            Ok(server) => return Some(server),
            Err(err) => {
                warn!(
                    "Failed to bind UDS {} (attempt {}/3): {err}",
                    path.display(),
                    attempt
                );
                if attempt < 3 {
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
    None
}

pub fn run_forever() {
    init_cli_logging();
    info!("fifo_listener starting");

    let (tx, rx) = sync_channel(512);
    let stop = Arc::new(AtomicBool::new(false));
    let rollback = Arc::new(RollbackTracker::new());
    let stop_eventfd = match create_eventfd() {
        Ok(fd) => fd,
        Err(err) => {
            warn!("Failed to create eventfd for fifo_listener: {err}");
            return;
        }
    };
    let mut threads = Vec::new();

    for source in [FifoSource::Order, FifoSource::Fills, FifoSource::Diffs] {
        let stop = stop.clone();
        let tx = tx.clone();
        let path = fifo_path(source);
        let rollback = rollback.clone();
        let stop_eventfd = stop_eventfd;
        threads.push(thread::spawn(move || {
            listen_fifo(source, path, stop, stop_eventfd, rollback, tx);
        }));
    }

    run_aggregator(rx, stop.clone(), None);

    stop.store(true, Ordering::SeqCst);
    signal_eventfd(stop_eventfd);
    for thread in threads {
        let _unused = thread.join();
    }
    close_fd(stop_eventfd);
}

pub fn start_listener(callback: Option<HeightCallback>) -> std::io::Result<ListenerHandle> {
    init_cli_logging();
    info!("fifo_listener starting");

    let (tx, rx) = sync_channel(512);
    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    let rollback = Arc::new(RollbackTracker::new());
    let stop_eventfd = create_eventfd()?;

    for source in [FifoSource::Order, FifoSource::Fills, FifoSource::Diffs] {
        let stop = stop.clone();
        let tx = tx.clone();
        let path = fifo_path(source);
        let rollback = rollback.clone();
        let stop_eventfd = stop_eventfd;
        threads.push(thread::spawn(move || {
            listen_fifo(source, path, stop, stop_eventfd, rollback, tx);
        }));
    }

    let agg_stop = stop.clone();
    threads.push(thread::spawn(move || run_aggregator(rx, agg_stop, callback)));

    Ok(ListenerHandle {
        stop,
        stop_eventfd,
        threads,
    })
}
