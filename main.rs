use anyhow::{Context, Result};
use chrono::Local;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use nix::fcntl;
use nix::sys::stat::Mode;
use nix::unistd;
use std::fs;
use std::os::fd::RawFd;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use serde::Deserialize;
use simd_json;
use std::io::Write;

// ================= 配置区域 =================

const PIPE_CAPACITY: usize = 16 * 1024 * 1024;
const READ_BUFFER_SIZE: usize = 16 * 1024 * 1024;
const FAST_FORWARD_THRESHOLD: usize = 512 * 1024;
const WARMUP_MS: u64 = 250;
const FIFO_MODE: Mode = Mode::S_IRUSR.union(Mode::S_IWUSR).union(Mode::S_IRGRP).union(Mode::S_IROTH);
const FIXED_BASE_DIR: &str = "/dev/shm/book_tmpfs";

static WARMUP_LOGGED: AtomicBool = AtomicBool::new(false);
static WARMUP_START: OnceLock<std::time::Instant> = OnceLock::new();


// ================= 数据结构 =================

#[derive(Debug, Deserialize)]
struct HlMessage<'a> {
    #[serde(borrow)]
    #[allow(dead_code)]
    local_time: &'a str,
    block_number: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum StreamId {
    Fills,
    Orders,
    Diffs,
}

impl StreamId {
    fn fixed_path(&self) -> PathBuf {
        let name = match self {
            StreamId::Fills => "fills",
            StreamId::Orders => "order", // 注意：单数
            StreamId::Diffs => "diffs",
        };
        Path::new(FIXED_BASE_DIR).join(name)
    }
    
    fn as_str(&self) -> &'static str {
        match self {
            StreamId::Fills => "Fills",
            StreamId::Orders => "Orders",
            StreamId::Diffs => "Diffs",
        }
    }
}

// ================= 核心流读取 =================

struct FixedStream {
    id: StreamId,
    fd: Option<RawFd>,
    buffer: Vec<u8>,
    valid_len: usize,
    last_block_number: Option<u64>,
    warming_up: bool,
    warmup_started: Option<std::time::Instant>,
}

impl FixedStream {
    fn new(id: StreamId) -> Self {
        Self {
            id,
            fd: None,
            buffer: vec![0u8; READ_BUFFER_SIZE],
            valid_len: 0,
            last_block_number: None,
            warming_up: true,
            warmup_started: Some(std::time::Instant::now()),
        }
    }

    fn setup_and_open(&mut self) -> Result<RawFd> {
        let fixed_path = self.id.fixed_path();
        if let Some(parent) = fixed_path.parent() {
            fs::create_dir_all(parent).context("Creating fixed dir")?;
        }
        if !fixed_path.exists() {
            unistd::mkfifo(&fixed_path, FIFO_MODE).context("Creating fixed fifo")?;
        } else {
            let meta = fs::symlink_metadata(&fixed_path)?;
            if !meta.file_type().is_fifo() {
                fs::remove_file(&fixed_path)?;
                unistd::mkfifo(&fixed_path, FIFO_MODE)?;
            }
        }

        let fd = fcntl::open(
            &fixed_path,
            fcntl::OFlag::O_RDWR | fcntl::OFlag::O_NONBLOCK,
            Mode::empty(),
        ).context("Opening FIFO")?;

        unsafe { let _ = libc::fcntl(fd, 1031, PIPE_CAPACITY as i32); }
        self.fd = Some(fd);
        Ok(fd)
    }

    fn read_chunk(&mut self) -> Result<()> {
        let Some(fd) = self.fd else { return Ok(()); };
        loop {
            if self.valid_len >= self.buffer.len() {
                self.valid_len = 0; 
            }
            match unistd::read(fd, &mut self.buffer[self.valid_len..]) {
                Ok(0) => break,
                Ok(n) => {
                    if self.warming_up {
                        self.update_warmup();
                        if self.warming_up {
                            self.valid_len = 0;
                            continue;
                        }
                    }

                    self.valid_len += n;
                    let backlog = self.valid_len + Self::pending_bytes(fd).unwrap_or(0);
                    if backlog >= FAST_FORWARD_THRESHOLD {
                        self.process_buffer_latest_only()?;
                    } else {
                        self.process_buffer()?;
                    }
                }
                Err(nix::errno::Errno::EAGAIN) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn pending_bytes(fd: RawFd) -> Option<usize> {
        let mut bytes: libc::c_int = 0;
        let res = unsafe { libc::ioctl(fd, libc::FIONREAD, &mut bytes) };
        if res == -1 {
            return None;
        }
        Some(bytes as usize)
    }

    fn update_warmup(&mut self) {
        let now = std::time::Instant::now();
        let started = self.warmup_started.get_or_insert(now);
        let elapsed = now.duration_since(*started);
        if elapsed >= std::time::Duration::from_millis(WARMUP_MS) {
            self.warming_up = false;
            self.last_block_number = None;
            if !WARMUP_LOGGED.swap(true, Ordering::SeqCst) {
                let total = WARMUP_START
                    .get()
                    .map(|start| start.elapsed())
                    .unwrap_or(elapsed);
                println!("✅ Warm-up complete in {:.3}s.", total.as_secs_f64());
            }
        }
    }

    fn process_buffer(&mut self) -> Result<()> {
        let mut start = 0;
        while let Some(offset) = memchr::memchr(b'\n', &self.buffer[start..self.valid_len]) {
            let end = start + offset;
            let parsed_block = {
                let line_slice = &mut self.buffer[start..end];
                if !line_slice.is_empty() {
                    match simd_json::from_slice::<HlMessage>(line_slice) {
                        Ok(msg) => Some(msg.block_number),
                        Err(_) => None,
                    }
                } else {
                    None
                }
            };
            if let Some(bn) = parsed_block {
                self.print_log(bn);
            }
            start = end + 1;
        }
        if start < self.valid_len {
            let remaining = self.valid_len - start;
            self.buffer.copy_within(start..self.valid_len, 0);
            self.valid_len = remaining;
        } else {
            self.valid_len = 0;
        }
        Ok(())
    }

    fn process_buffer_latest_only(&mut self) -> Result<()> {
        if self.valid_len == 0 {
            return Ok(());
        }

        let Some(last_nl) = memchr::memrchr(b'\n', &self.buffer[..self.valid_len]) else {
            return Ok(());
        };
        let prev_nl = memchr::memrchr(b'\n', &self.buffer[..last_nl]);
        let line_start = prev_nl.map(|idx| idx + 1).unwrap_or(0);
        let line_end = last_nl;

        let parsed_block = {
            let line_slice = &mut self.buffer[line_start..line_end];
            if !line_slice.is_empty() {
                simd_json::from_slice::<HlMessage>(line_slice)
                    .ok()
                    .map(|msg| msg.block_number)
            } else {
                None
            }
        };
        if let Some(block) = parsed_block {
            self.print_log(block);
        }

        let remaining = self.valid_len - (last_nl + 1);
        if remaining > 0 {
            self.buffer.copy_within(last_nl + 1..self.valid_len, 0);
        }
        self.valid_len = remaining;
        Ok(())
    }

    fn print_log(&mut self, current: u64) {
        if let Some(last) = self.last_block_number {
            if current > last + 1 {
                let gap = current - last - 1;
                let _ = writeln!(std::io::stderr(), "🚨 [{:?}] GAP! Last: {}, Curr: {}, Missing: {}", self.id, last, current, gap);
            }
        }
        self.last_block_number = Some(current);

        let now = Local::now().format("%H:%M:%S%.6f");
        match self.id {
            StreamId::Orders => println!("{} | {}@{}", now, self.id.as_str(), current),
            StreamId::Diffs  => println!("{} | \t\t{}@{}", now, self.id.as_str(), current),
            StreamId::Fills  => println!("{} | \t\t\t\t{}@{}", now, self.id.as_str(), current),
        }
    }
}

// ================= Main =================

fn main() -> Result<()> {
    println!("🧪 HL-Node Engine (MIO Reader)");

    let stream_ids = vec![StreamId::Fills, StreamId::Orders, StreamId::Diffs];
    let _ = WARMUP_START.set(std::time::Instant::now());

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);
    let mut streams = Vec::new();

    println!("🚀 IO Loop started. Opening fixed pipes...");

    for (i, id) in stream_ids.iter().enumerate() {
        let mut s = FixedStream::new(*id);
        println!("📂 [{}] Listening on {:?}", id.as_str(), id.fixed_path());

        let fd = s.setup_and_open()?;
        poll.registry().register(&mut SourceFd(&fd), Token(i), Interest::READABLE)?;
        streams.push(s);
    }

    println!("📅 Ready. Waiting for data...");

    loop {
        if let Err(e) = poll.poll(&mut events, None) {
            if e.kind() != std::io::ErrorKind::Interrupted {
                eprintln!("Poll error: {}", e);
            }
            continue;
        }
        for event in events.iter() {
            let idx = event.token().0;
            if idx < streams.len() {
                if let Err(e) = streams[idx].read_chunk() {
                    eprintln!("IO Error: {}", e);
                }
            }
        }
    }
}
