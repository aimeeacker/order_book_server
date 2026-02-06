use memchr::memmem;
use memmap2::Mmap;
use rayon::prelude::*;
use simd_json::to_owned_value;
use std::{
    env,
    fs::{File, read},
    io::Write,
    time::Instant,
};

type DynError = Box<dyn std::error::Error + Send + Sync>;

struct ScanResult {
    coin: String,
    entry_start: usize,
    scan_ms: f64,
}

struct EntryRanges {
    coin: String,
    bids: std::ops::Range<usize>,
    asks: std::ops::Range<usize>,
    range_ms: f64,
}

const MINIFY_CHUNK_BYTES: usize = 16 * 1024 * 1024;

fn main() -> Result<(), DynError> {
    let mut args = env::args().skip(1);
    let path = args.next().unwrap_or_else(|| "/home/aimee/hl_runtime/hl_book/snapshot.json".to_string());
    let coins_arg = args.next().unwrap_or_else(|| "BTC,ETH".to_string());
    let coins: Vec<&str> = coins_arg.split(',').filter(|c| !c.is_empty()).collect();
    if coins.is_empty() {
        return Err("no coins specified".into());
    }

    let read_start = Instant::now();
    let bytes = read(&path)?;
    let read_ms = read_start.elapsed().as_secs_f64() * 1000.0;
    let size_mb = bytes.len() as f64 / 1024.0 / 1024.0;

    let (height, snapshots_start) = parse_snapshot_header(&bytes)?;
    let scan_results = scan_for_coins(&bytes, snapshots_start, &coins)?;
    let scan_ms = scan_results.iter().map(|r| r.scan_ms).sum::<f64>();
    let entry_ranges =
        scan_results.iter().map(|scan| build_entry_ranges(&bytes, scan)).collect::<Result<Vec<_>, DynError>>()?;

    let minify_start = Instant::now();
    let minified = minify_parallel(&bytes);
    let minify_ms = minify_start.elapsed().as_secs_f64() * 1000.0;
    let min_size_mb = minified.len() as f64 / 1024.0 / 1024.0;

    let (min_height, min_snapshots_start) = parse_snapshot_header(&minified)?;
    if height != min_height {
        return Err(format!("height mismatch: {height} vs {min_height}").into());
    }
    let min_scan_results = scan_for_coins(&minified, min_snapshots_start, &coins)?;
    let min_scan_ms = min_scan_results.iter().map(|r| r.scan_ms).sum::<f64>();

    let mmap_out = env::var("SNAPSHOT_BENCH_OUT").unwrap_or_else(|_| "/dev/shm/snapshot.min.mmap.json".to_string());
    let (mmap_out, mmap_minify_ms, mmap_write_ms, mmap_total_ms, mmap_size_mb) = minify_mmap_to_file(&path, &mmap_out)?;

    println!(
        "snapshot_bench path={} height={} coins={} read_ms={:.3} scan_ms={:.3} minify_ms={:.3} minified_scan_ms={:.3} size_mb={:.3} min_size_mb={:.3}",
        path,
        height,
        coins.join(","),
        read_ms,
        scan_ms,
        minify_ms,
        min_scan_ms,
        size_mb,
        min_size_mb
    );
    println!(
        "snapshot_bench mmap_out={} mmap_minify_ms={:.3} mmap_write_ms={:.3} mmap_total_ms={:.3} mmap_size_mb={:.3}",
        mmap_out, mmap_minify_ms, mmap_write_ms, mmap_total_ms, mmap_size_mb
    );

    let copy_timings = parse_with_copy(&bytes, &entry_ranges)?;
    let (inplace_copy_ms, inplace_timings, inplace_parse_total_ms) = parse_inplace_with_copy(&bytes, &entry_ranges)?;
    println!(
        "parse_bench_inplace copy_ms={:.3} parse_ms_total={:.3} total_ms={:.3}",
        inplace_copy_ms,
        inplace_parse_total_ms,
        inplace_copy_ms + inplace_parse_total_ms
    );
    for (copy, inplace) in copy_timings.iter().zip(inplace_timings.iter()) {
        println!(
            "parse_bench coin={} range_ms={:.3} copy_ms={:.3} copy_parse_ms={:.3} inplace_parse_ms={:.3}",
            copy.coin, copy.range_ms, copy.copy_ms, copy.copy_parse_ms, inplace.inplace_parse_ms
        );
    }

    for (orig, min) in scan_results.iter().zip(min_scan_results.iter()) {
        println!(
            "coin={} entry_start={} scan_ms={:.3} min_entry_start={} min_scan_ms={:.3}",
            orig.coin, orig.entry_start, orig.scan_ms, min.entry_start, min.scan_ms
        );
    }

    Ok(())
}

fn minify_parallel(bytes: &[u8]) -> Vec<u8> {
    let mut parts: Vec<Vec<u8>> = bytes
        .par_chunks(MINIFY_CHUNK_BYTES)
        .map(|chunk| {
            let mut out = Vec::with_capacity(chunk.len());
            let mut start = 0usize;
            let mut idx = 0usize;
            while idx < chunk.len() {
                if chunk[idx].is_ascii_whitespace() {
                    if start < idx {
                        out.extend_from_slice(&chunk[start..idx]);
                    }
                    idx += 1;
                    start = idx;
                } else {
                    idx += 1;
                }
            }
            if start < chunk.len() {
                out.extend_from_slice(&chunk[start..]);
            }
            out
        })
        .collect();
    let total_len: usize = parts.iter().map(|part| part.len()).sum();
    let mut out = Vec::with_capacity(total_len);
    for part in &mut parts {
        out.append(part);
    }
    out
}

#[allow(unsafe_code)]
#[allow(unsafe_code)]
fn minify_mmap_to_file(path: &str, out_path: &str) -> Result<(String, f64, f64, f64, f64), DynError> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let total_start = Instant::now();
    let minify_start = Instant::now();
    let minified = minify_parallel(&mmap);
    let minify_ms = minify_start.elapsed().as_secs_f64() * 1000.0;
    let write_start = Instant::now();
    let (mut out, out_path) = create_out_file(out_path)?;
    out.write_all(&minified)?;
    out.flush()?;
    out.sync_all()?;
    let write_ms = write_start.elapsed().as_secs_f64() * 1000.0;
    let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    let size_mb = minified.len() as f64 / 1024.0 / 1024.0;
    Ok((out_path, minify_ms, write_ms, total_ms, size_mb))
}

fn create_out_file(path: &str) -> Result<(File, String), DynError> {
    match File::create(path) {
        Ok(file) => Ok((file, path.to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            let fallback = "/tmp/snapshot.min.mmap.json";
            let file = File::create(fallback)?;
            Ok((file, fallback.to_string()))
        }
        Err(err) => Err(err.into()),
    }
}

struct ParseCopyTiming {
    coin: String,
    range_ms: f64,
    copy_ms: f64,
    copy_parse_ms: f64,
}

struct ParseInplaceTiming {
    inplace_parse_ms: f64,
}

fn build_entry_ranges(bytes: &[u8], scan: &ScanResult) -> Result<EntryRanges, DynError> {
    let range_start = Instant::now();
    let (bids, asks) = extract_entry_ranges(bytes, scan.entry_start)?;
    let range_ms = range_start.elapsed().as_secs_f64() * 1000.0;
    Ok(EntryRanges { coin: scan.coin.clone(), bids, asks, range_ms })
}

fn parse_with_copy(bytes: &[u8], ranges: &[EntryRanges]) -> Result<Vec<ParseCopyTiming>, DynError> {
    let mut timings = Vec::with_capacity(ranges.len());
    for entry in ranges {
        let copy_start = Instant::now();
        let mut bids = bytes[entry.bids.clone()].to_vec();
        let mut asks = bytes[entry.asks.clone()].to_vec();
        let copy_ms = copy_start.elapsed().as_secs_f64() * 1000.0;
        let parse_start = Instant::now();
        let _bids_value = to_owned_value(&mut bids)?;
        let _asks_value = to_owned_value(&mut asks)?;
        let copy_parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
        timings.push(ParseCopyTiming { coin: entry.coin.clone(), range_ms: entry.range_ms, copy_ms, copy_parse_ms });
    }
    Ok(timings)
}

fn parse_inplace_with_copy(
    bytes: &[u8],
    ranges: &[EntryRanges],
) -> Result<(f64, Vec<ParseInplaceTiming>, f64), DynError> {
    let copy_start = Instant::now();
    let mut buffer = bytes.to_vec();
    let copy_ms = copy_start.elapsed().as_secs_f64() * 1000.0;
    let mut timings = Vec::with_capacity(ranges.len());
    let mut parse_total_ms = 0.0;
    for entry in ranges {
        let (bids, asks) = split_ranges_mut(&mut buffer, entry.bids.clone(), entry.asks.clone());
        let parse_start = Instant::now();
        let _bids_value = to_owned_value(bids)?;
        let _asks_value = to_owned_value(asks)?;
        let inplace_parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
        parse_total_ms += inplace_parse_ms;
        timings.push(ParseInplaceTiming { inplace_parse_ms });
    }
    Ok((copy_ms, timings, parse_total_ms))
}

fn split_ranges_mut<'a>(
    buf: &'a mut [u8],
    first: std::ops::Range<usize>,
    second: std::ops::Range<usize>,
) -> (&'a mut [u8], &'a mut [u8]) {
    if first.start <= second.start {
        let (left, right) = buf.split_at_mut(second.start);
        let first_slice = &mut left[first];
        let second_slice = &mut right[..(second.end - second.start)];
        (first_slice, second_slice)
    } else {
        let (left, right) = buf.split_at_mut(first.start);
        let second_slice = &mut left[second];
        let first_slice = &mut right[..(first.end - first.start)];
        (first_slice, second_slice)
    }
}

fn extract_entry_ranges(
    bytes: &[u8],
    start: usize,
) -> Result<(std::ops::Range<usize>, std::ops::Range<usize>), DynError> {
    let mut idx = start;
    let mut depth = 0usize;
    let mut orders_depth: Option<usize> = None;
    let mut bids_start: Option<usize> = None;
    let mut bids_end: Option<usize> = None;
    let mut asks_start: Option<usize> = None;
    let mut asks_end: Option<usize> = None;
    let mut in_string = false;
    let mut escape = false;

    while idx < bytes.len() {
        let b = bytes[idx];
        if in_string {
            if escape {
                escape = false;
                idx += 1;
                continue;
            }
            if b == b'\\' {
                escape = true;
                idx += 1;
                continue;
            }
            if b == b'"' {
                in_string = false;
            }
            idx += 1;
            continue;
        }

        match b {
            b'"' => in_string = true,
            b'[' => {
                depth += 1;
                if orders_depth.is_none() && depth == 2 {
                    orders_depth = Some(2);
                } else if orders_depth == Some(2) && depth == 3 {
                    if bids_start.is_none() {
                        bids_start = Some(idx);
                    } else if asks_start.is_none() {
                        asks_start = Some(idx);
                    }
                }
            }
            b']' => {
                if orders_depth == Some(2) && depth == 3 {
                    if bids_start.is_some() && bids_end.is_none() {
                        bids_end = Some(idx);
                    } else if asks_start.is_some() && asks_end.is_none() {
                        asks_end = Some(idx);
                    }
                }
                if depth == 0 {
                    return Err("snapshot entry has unmatched ']'".into());
                }
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        idx += 1;
    }

    if in_string || depth != 0 {
        return Err("snapshot entry truncated while scanning orders".into());
    }
    let bids_start = bids_start.ok_or_else(|| "snapshot entry missing bids array".to_string())?;
    let bids_end = bids_end.ok_or_else(|| "snapshot entry missing bids end".to_string())?;
    let asks_start = asks_start.ok_or_else(|| "snapshot entry missing asks array".to_string())?;
    let asks_end = asks_end.ok_or_else(|| "snapshot entry missing asks end".to_string())?;
    if bids_end < bids_start || asks_end < asks_start {
        return Err("snapshot entry invalid bids/asks array bounds".into());
    }
    Ok((bids_start..(bids_end + 1), asks_start..(asks_end + 1)))
}

fn scan_for_coins(bytes: &[u8], snapshots_start: usize, coins: &[&str]) -> Result<Vec<ScanResult>, DynError> {
    let mut results = Vec::with_capacity(coins.len());
    for &coin in coins {
        let pattern = build_coin_pattern(coin);
        let scan_start = Instant::now();
        let entry_start = find_entry_start_in_range(bytes, snapshots_start, bytes.len(), &pattern)
            .ok_or_else(|| format!("coin {} not found", coin))?;
        let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;
        results.push(ScanResult { coin: coin.to_string(), entry_start, scan_ms });
    }
    Ok(results)
}

fn build_coin_pattern(coin: &str) -> Vec<u8> {
    let mut pattern = Vec::with_capacity(coin.len() + 2);
    pattern.push(b'"');
    pattern.extend_from_slice(coin.as_bytes());
    pattern.push(b'"');
    pattern
}

fn entry_start_from_match(bytes: &[u8], quote_pos: usize, pattern_len: usize) -> Option<usize> {
    if quote_pos == 0 || quote_pos + pattern_len > bytes.len() {
        return None;
    }
    let mut idx = quote_pos;
    while idx > 0 && bytes[idx - 1].is_ascii_whitespace() {
        idx -= 1;
    }
    if idx == 0 || bytes[idx - 1] != b'[' {
        return None;
    }
    let mut tail = quote_pos + pattern_len;
    while tail < bytes.len() && bytes[tail].is_ascii_whitespace() {
        tail += 1;
    }
    if tail >= bytes.len() || bytes[tail] != b',' {
        return None;
    }
    Some(idx - 1)
}

fn find_entry_start_in_range(bytes: &[u8], start: usize, end: usize, pattern: &[u8]) -> Option<usize> {
    let mut cursor = start.min(bytes.len());
    let end = end.min(bytes.len());
    while cursor < end {
        let slice = &bytes[cursor..end];
        let pos = memmem::find(slice, pattern)?;
        let abs_pos = cursor + pos;
        if let Some(entry_start) = entry_start_from_match(bytes, abs_pos, pattern.len()) {
            return Some(entry_start);
        }
        cursor = abs_pos + pattern.len();
    }
    None
}

fn parse_snapshot_header(bytes: &[u8]) -> Result<(u64, usize), DynError> {
    let mut idx = 0;
    skip_ws(bytes, &mut idx);
    if idx >= bytes.len() || bytes[idx] != b'[' {
        return Err("snapshot json missing opening [".into());
    }
    idx += 1;
    skip_ws(bytes, &mut idx);
    let height_start = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == height_start {
        return Err("snapshot json missing height".into());
    }
    let height = std::str::from_utf8(&bytes[height_start..idx])?.parse::<u64>()?;
    skip_ws(bytes, &mut idx);
    if idx >= bytes.len() || bytes[idx] != b',' {
        return Err("snapshot json missing height separator".into());
    }
    idx += 1;
    skip_ws(bytes, &mut idx);
    if idx >= bytes.len() || bytes[idx] != b'[' {
        return Err("snapshot json missing snapshots array".into());
    }
    Ok((height, idx))
}

fn skip_ws(bytes: &[u8], idx: &mut usize) {
    while *idx < bytes.len() && bytes[*idx].is_ascii_whitespace() {
        *idx += 1;
    }
}
