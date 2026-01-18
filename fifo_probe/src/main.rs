use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use log::{info, warn};

const FIFO_BASE_DIR: &str = "/home/aimee/hl_runtime/hl_book";
const PIPE_CAPACITY: i32 = 16 * 1024 * 1024;

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

fn fifo_path(source: FifoSource) -> PathBuf {
    PathBuf::from(FIFO_BASE_DIR).join(source.name())
}

fn set_pipe_capacity(fd: i32, source: FifoSource) {
    #[allow(unsafe_code)]
    let ret = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, PIPE_CAPACITY) };
    if ret == -1 {
        warn!("Failed to set FIFO capacity for {:?}", source);
    } else {
        info!(
            "Set FIFO capacity for {:?} to {} bytes",
            source, PIPE_CAPACITY
        );
    }
}

fn extract_block_number(line: &str) -> Option<u64> {
    let needle = "\"block_number\":";
    let idx = line.find(needle)?;
    let mut slice = &line[idx + needle.len()..];
    let bytes = slice.as_bytes();
    let mut start = 0;
    while start < bytes.len() && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if start == end {
        return None;
    }
    slice = &slice[start..end];
    slice.parse().ok()
}

fn listen_fifo(source: FifoSource) {
    let path = fifo_path(source);
    loop {
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
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    warn!("FIFO EOF for {:?} at {}; reopening", source, path.display());
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim_end_matches('\n');
                    if trimmed.is_empty() {
                        continue;
                    }
                    match extract_block_number(trimmed) {
                        Some(height) => {
                            info!("{:?} block_number={}", source, height);
                        }
                        None => {
                            warn!(
                                "{:?} missing block_number; payload head: {:?}",
                                source,
                                &trimmed[..trimmed.len().min(120)]
                            );
                        }
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

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_micros()
        .init();

    info!("fifo_probe starting");
    for source in [FifoSource::Order, FifoSource::Fills, FifoSource::Diffs] {
        thread::spawn(move || listen_fifo(source));
    }

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
