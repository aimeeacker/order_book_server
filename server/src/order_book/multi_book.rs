use crate::{
    order_book::{Coin, InnerOrder, Oid, OrderBook, Snapshot, Sz},
    prelude::*,
};
use log::{info, warn};
use memchr::memmem;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::{Mutex, OnceLock},
    time::Instant,
};
use tokio::fs::read;

#[derive(Clone)]
pub(crate) struct Snapshots<O>(HashMap<Coin, Snapshot<O>>)
where
    O: Clone;

impl<O: Clone> Snapshots<O> {
    pub(crate) const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Snapshot<O>> {
        &self.0
    }

    pub(crate) fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
    }
}

#[derive(Clone)]
pub(crate) struct OrderBooks<O> {
    order_books: BTreeMap<Coin, OrderBook<O>>,
}

impl<O: InnerOrder> OrderBooks<O> {
    pub(crate) const fn as_ref(&self) -> &BTreeMap<Coin, OrderBook<O>> {
        &self.order_books
    }
    #[must_use]
    pub(crate) fn from_snapshots(snapshot: Snapshots<O>, ignore_triggers: bool) -> Self {
        Self {
            order_books: snapshot
                .value()
                .into_iter()
                .map(|(coin, book)| (coin, OrderBook::from_snapshot(book, ignore_triggers)))
                .collect(),
        }
    }

    pub(crate) fn add_order(&mut self, order: O) {
        let coin = &order.coin();
        self.order_books.entry(coin.clone()).or_insert_with(OrderBook::new).add_order(order);
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid, coin: Coin) -> bool {
        self.order_books.get_mut(&coin).is_some_and(|book| book.cancel_order(oid))
    }

    // change size to reflect how much gets matched during the block
    pub(crate) fn modify_sz(&mut self, oid: Oid, coin: Coin, sz: Sz) -> bool {
        self.order_books.get_mut(&coin).is_some_and(|book| book.modify_sz(oid, sz))
    }
}

impl<O: Send + Sync + InnerOrder> OrderBooks<O> {
    #[must_use]
    pub(crate) fn to_snapshots_par(&self) -> Snapshots<O> {
        let snapshots = self.order_books.par_iter().map(|(c, book)| (c.clone(), book.to_snapshot())).collect();
        Snapshots(snapshots)
    }
}

#[cfg(test)]
pub(crate) fn load_snapshots_from_str<O, R>(str: &str) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error>,
    R: Serialize + for<'a> Deserialize<'a> + Send,
{
    #[allow(clippy::type_complexity)]
    let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = serde_json::from_str(str)?;
    build_snapshots(height, snapshot)
}

#[cfg(test)]
pub(crate) async fn load_snapshots_from_json<O, R>(path: &Path) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error> + Clone,
    R: Serialize + for<'a> Deserialize<'a>,
{
    let mut file_contents = read(path).await?;
    #[allow(clippy::type_complexity)]
    let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = simd_json::from_slice(&mut file_contents)?;
    build_snapshots(height, snapshot)
}

pub(crate) async fn load_snapshots_from_json_filtered<O, R>(
    path: &Path,
    allowed_coins: &[&str],
) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error> + Clone,
    R: Serialize + for<'a> Deserialize<'a> + Send,
{
    let mut file_contents = read(path).await?;
    if allowed_coins.is_empty() {
        #[allow(clippy::type_complexity)]
        let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = simd_json::from_slice(&mut file_contents)?;
        return build_snapshots_filtered(height, snapshot, allowed_coins);
    }

    let (height, snapshots_start) = parse_snapshot_header(&file_contents)?;
    let allowed_set: HashSet<&str> = allowed_coins.iter().copied().collect();
    let scan_start = Instant::now();
    let entries_bytes = match fast_extract_snapshot_entries(&file_contents, snapshots_start, allowed_coins) {
        Ok(entries) => entries,
        Err(err) => {
            warn!("Fast snapshot scan failed: {err}; falling back to full scan");
            extract_snapshot_entries(&file_contents, snapshots_start, &allowed_set)?
        }
    };
    let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;
    update_snapshot_index(&entries_bytes);
    let entries: Vec<ParsedEntry<R>> =
        entries_bytes.into_par_iter().map(|entry| parse_snapshot_entry(entry)).collect::<Result<Vec<_>>>()?;
    let mut out = HashMap::new();
    let mut timings = Vec::with_capacity(entries.len());
    for ParsedEntry { coin, orders, slice_ms, parse_ms } in entries {
        let build_start = Instant::now();
        let [bids, asks] = orders;
        let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
        let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
        let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
        out.insert(Coin::new(&coin), Snapshot([bids, asks]));
        timings.push(SnapshotTiming { coin, scan_ms, slice_ms, parse_ms, build_ms });
    }
    for timing in timings {
        info!(
            "Snapshot timing coin={} scan_ms={:.3} slice_ms={:.3} parse_ms={:.3} build_ms={:.3}",
            timing.coin, timing.scan_ms, timing.slice_ms, timing.parse_ms, timing.build_ms
        );
    }
    Ok((height, Snapshots::new(out)))
}

#[cfg(test)]
fn build_snapshots<O, R>(height: u64, snapshot: Vec<(String, [Vec<R>; 2])>) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error> + Clone,
    R: Serialize + for<'a> Deserialize<'a>,
{
    Ok((height, Snapshots::new(build_snapshot_map(snapshot, &[])?)))
}

fn build_snapshots_filtered<O, R>(
    height: u64,
    snapshot: Vec<(String, [Vec<R>; 2])>,
    allowed_coins: &[&str],
) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error> + Clone,
    R: Serialize + for<'a> Deserialize<'a>,
{
    Ok((height, Snapshots::new(build_snapshot_map(snapshot, allowed_coins)?)))
}

fn build_snapshot_map<O, R>(
    snapshot: Vec<(String, [Vec<R>; 2])>,
    allowed_coins: &[&str],
) -> Result<HashMap<Coin, Snapshot<O>>>
where
    O: TryFrom<R, Error = Error> + Clone,
    R: Serialize + for<'a> Deserialize<'a>,
{
    let mut out = HashMap::new();
    for (coin, [bids, asks]) in snapshot {
        if !allowed_coins.is_empty() && !allowed_coins.iter().any(|c| *c == coin) {
            continue;
        }
        let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
        let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
        out.insert(Coin::new(&coin), Snapshot([bids, asks]));
    }
    Ok(out)
}

fn parse_snapshot_header(bytes: &[u8]) -> Result<(u64, usize)> {
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

struct SnapshotEntryParts {
    coin: String,
    bids: Vec<u8>,
    asks: Vec<u8>,
    slice_ms: f64,
    entry_start: usize,
    entry_len: usize,
}

const INDEX_WINDOW_MARGIN: usize = 4 * 1024 * 1024;
const INDEX_WINDOW_MIN: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy)]
struct EntryHint {
    start: usize,
    len: usize,
}

fn snapshot_index() -> &'static Mutex<HashMap<String, EntryHint>> {
    static INDEX: OnceLock<Mutex<HashMap<String, EntryHint>>> = OnceLock::new();
    INDEX.get_or_init(|| Mutex::new(HashMap::new()))
}

fn update_snapshot_index(entries: &[SnapshotEntryParts]) {
    let mut map = snapshot_index().lock().expect("snapshot index poisoned");
    for entry in entries {
        map.insert(entry.coin.clone(), EntryHint { start: entry.entry_start, len: entry.entry_len });
    }
}

fn fast_extract_snapshot_entries(
    bytes: &[u8],
    snapshots_start: usize,
    allowed: &[&str],
) -> Result<Vec<SnapshotEntryParts>> {
    let mut entries = Vec::with_capacity(allowed.len());
    let mut search_start = snapshots_start;

    for coin in allowed {
        let pattern = build_coin_pattern(coin);
        let mut entry_start = None;
        if let Some(hint) = {
            let map = snapshot_index().lock().expect("snapshot index poisoned");
            map.get(*coin).copied()
        } {
            if entry_header_matches(bytes, hint.start, coin) {
                entry_start = Some(hint.start);
            } else {
                let mut window = hint.len.saturating_add(INDEX_WINDOW_MARGIN);
                if window < INDEX_WINDOW_MIN {
                    window = INDEX_WINDOW_MIN;
                }
                let start = hint.start.saturating_sub(window);
                let end = (hint.start + hint.len + window).min(bytes.len());
                entry_start = find_entry_start_in_range(bytes, start, end, &pattern);
            }
        }
        if entry_start.is_none() {
            entry_start = find_entry_start_in_range(bytes, search_start, bytes.len(), &pattern);
        }

        let Some(start) = entry_start else {
            return Err(format!("fast scan could not locate coin {}", coin).into());
        };
        let slice_start = Instant::now();
        let (bids, asks, end) = extract_entry_orders_and_end(bytes, start)?;
        let slice_ms = slice_start.elapsed().as_secs_f64() * 1000.0;
        let entry_len = end.saturating_sub(start).saturating_add(1);
        entries.push(SnapshotEntryParts {
            coin: coin.to_string(),
            bids,
            asks,
            slice_ms,
            entry_start: start,
            entry_len,
        });
        search_start = end;
    }

    Ok(entries)
}

fn build_coin_pattern(coin: &str) -> Vec<u8> {
    let mut pattern = Vec::with_capacity(coin.len() + 3);
    pattern.push(b'"');
    pattern.extend_from_slice(coin.as_bytes());
    pattern.push(b'"');
    pattern.push(b',');
    pattern
}

fn entry_header_matches(bytes: &[u8], start: usize, coin: &str) -> bool {
    if start >= bytes.len() || bytes[start] != b'[' {
        return false;
    }
    let mut idx = start + 1;
    skip_ws(bytes, &mut idx);
    if idx >= bytes.len() || bytes[idx] != b'"' {
        return false;
    }
    idx += 1;
    let coin_start = idx;
    while idx < bytes.len() && bytes[idx] != b'"' {
        idx += 1;
    }
    if idx >= bytes.len() {
        return false;
    }
    if &bytes[coin_start..idx] != coin.as_bytes() {
        return false;
    }
    idx += 1;
    skip_ws(bytes, &mut idx);
    if idx >= bytes.len() || bytes[idx] != b',' {
        return false;
    }
    idx += 1;
    skip_ws(bytes, &mut idx);
    idx < bytes.len() && bytes[idx] == b'['
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
    if tail >= bytes.len() || bytes[tail] != b'[' {
        return None;
    }
    Some(idx - 1)
}

fn find_entry_start_in_range(bytes: &[u8], start: usize, end: usize, pattern: &[u8]) -> Option<usize> {
    let mut cursor = start.min(bytes.len());
    let end = end.min(bytes.len());
    while cursor < end {
        let slice = &bytes[cursor..end];
        let Some(pos) = memmem::find(slice, pattern) else {
            return None;
        };
        let abs_pos = cursor + pos;
        if let Some(entry_start) = entry_start_from_match(bytes, abs_pos, pattern.len()) {
            return Some(entry_start);
        }
        cursor = abs_pos + pattern.len();
    }
    None
}

fn extract_snapshot_entries(
    bytes: &[u8],
    snapshots_start: usize,
    allowed: &HashSet<&str>,
) -> Result<Vec<SnapshotEntryParts>> {
    let mut entries = Vec::new();
    let mut found = HashSet::new();
    let mut idx = snapshots_start + 1;
    let mut array_depth = 0usize;
    let mut entry_start: Option<usize> = None;
    let mut entry_coin_allowed = false;
    let mut entry_coin: Option<String> = None;
    let mut in_string = false;
    let mut escape = false;
    let mut string_start = 0usize;

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
                if entry_start.is_some() && array_depth == 1 && entry_coin.is_none() {
                    let coin_slice = &bytes[string_start..idx];
                    let coin = std::str::from_utf8(coin_slice)?;
                    entry_coin = Some(coin.to_string());
                    if allowed.contains(coin) {
                        entry_coin_allowed = true;
                    }
                }
            }
            idx += 1;
            continue;
        }

        match b {
            b'"' => {
                in_string = true;
                string_start = idx + 1;
            }
            b'[' => {
                array_depth += 1;
                if array_depth == 1 {
                    entry_start = Some(idx);
                    entry_coin_allowed = false;
                    entry_coin = None;
                }
            }
            b']' => {
                if array_depth == 0 {
                    break;
                }
                if array_depth == 1 {
                    if let Some(start) = entry_start {
                        if entry_coin_allowed {
                            let coin = entry_coin.take().ok_or_else(|| "snapshot entry missing coin".to_string())?;
                            let slice_start = Instant::now();
                            let entry_slice = &bytes[start..=idx];
                            let (bids, asks) = extract_entry_orders(entry_slice)?;
                            let slice_ms = slice_start.elapsed().as_secs_f64() * 1000.0;
                            let entry_len = idx.saturating_sub(start).saturating_add(1);
                            entries.push(SnapshotEntryParts {
                                coin: coin.clone(),
                                bids,
                                asks,
                                slice_ms,
                                entry_start: start,
                                entry_len,
                            });
                            found.insert(coin);
                            if found.len() == allowed.len() {
                                return Ok(entries);
                            }
                        }
                    }
                    entry_start = None;
                    entry_coin_allowed = false;
                    entry_coin = None;
                }
                array_depth = array_depth.saturating_sub(1);
            }
            _ => {}
        }
        idx += 1;
    }

    if in_string || array_depth != 0 {
        return Err("snapshot json truncated while scanning entries".into());
    }
    Ok(entries)
}

struct ParsedEntry<R> {
    coin: String,
    orders: [Vec<R>; 2],
    slice_ms: f64,
    parse_ms: f64,
}

struct SnapshotTiming {
    coin: String,
    scan_ms: f64,
    slice_ms: f64,
    parse_ms: f64,
    build_ms: f64,
}

fn parse_snapshot_entry<R>(entry: SnapshotEntryParts) -> Result<ParsedEntry<R>>
where
    R: Serialize + for<'a> Deserialize<'a> + Send,
{
    let SnapshotEntryParts { coin, mut bids, mut asks, slice_ms, entry_start: _, entry_len: _ } = entry;
    let parse_start = Instant::now();
    let (bids_res, asks_res) =
        rayon::join(|| simd_json::from_slice::<Vec<R>>(&mut bids), || simd_json::from_slice::<Vec<R>>(&mut asks));
    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
    let bids = bids_res?;
    let asks = asks_res?;
    Ok(ParsedEntry { coin, orders: [bids, asks], slice_ms, parse_ms })
}

fn extract_entry_orders(entry: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut idx = 0usize;
    let mut depth = 0usize;
    let mut orders_depth: Option<usize> = None;
    let mut bids_start: Option<usize> = None;
    let mut bids_end: Option<usize> = None;
    let mut asks_start: Option<usize> = None;
    let mut asks_end: Option<usize> = None;
    let mut in_string = false;
    let mut escape = false;

    while idx < entry.len() {
        let b = entry[idx];
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
    let bids = entry[bids_start..=bids_end].to_vec();
    let asks = entry[asks_start..=asks_end].to_vec();
    Ok((bids, asks))
}

fn extract_entry_orders_and_end(bytes: &[u8], start: usize) -> Result<(Vec<u8>, Vec<u8>, usize)> {
    let mut idx = start;
    let mut depth = 0usize;
    let mut orders_depth: Option<usize> = None;
    let mut bids_start: Option<usize> = None;
    let mut bids_end: Option<usize> = None;
    let mut asks_start: Option<usize> = None;
    let mut asks_end: Option<usize> = None;
    let mut in_string = false;
    let mut escape = false;
    let mut entry_end: Option<usize> = None;

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
                    entry_end = Some(idx);
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
    let entry_end = entry_end.ok_or_else(|| "snapshot entry missing closing ']'".to_string())?;
    if bids_end < bids_start || asks_end < asks_start {
        return Err("snapshot entry invalid bids/asks array bounds".into());
    }
    let bids = bytes[bids_start..=bids_end].to_vec();
    let asks = bytes[asks_start..=asks_end].to_vec();
    Ok((bids, asks, entry_end))
}

#[cfg(test)]
mod tests {
    use crate::{
        order_book::{
            InnerOrder, OrderBook, Px, Side, Snapshot, Sz,
            levels::build_l2_level,
            multi_book::{Coin, Snapshots, load_snapshots_from_json, load_snapshots_from_str},
        },
        prelude::*,
        types::{
            L4Order, Level,
            inner::{InnerL4Order, InnerLevel},
        },
    };
    use alloy::primitives::Address;
    use itertools::Itertools;
    use std::{
        fs::{create_dir_all, write},
        path::PathBuf,
    };

    #[must_use]
    fn snapshot_to_l2_snapshot<O: InnerOrder>(
        snapshot: &Snapshot<O>,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let [bids, asks] = &snapshot.0;
        let bids = orders_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = orders_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }

    #[must_use]
    fn orders_to_l2_levels<O: InnerOrder>(
        orders: &[O],
        side: Side,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Vec<InnerLevel> {
        let mut levels = Vec::new();
        if n_levels == Some(0) {
            return levels;
        }
        let mut cur_level: Option<InnerLevel> = None;

        for order in orders {
            if build_l2_level(
                &mut cur_level,
                &mut levels,
                n_levels,
                n_sig_figs,
                mantissa,
                side,
                InnerLevel { px: order.limit_px(), sz: order.sz(), n: 1 },
            ) {
                break;
            }
        }
        levels.extend(cur_level.take());
        levels
    }

    #[derive(Default)]
    struct OrderManager {
        next_oid: u64,
    }

    fn simple_inner_order(oid: u64, side: Side, sz: String, px: String) -> Result<InnerL4Order> {
        let px = Px::parse_from_str(&px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(""),
            side,
            limit_px: px,
            sz,
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        })
    }

    impl OrderManager {
        fn order(&mut self, sz: &str, limit_px: &str, side: Side) -> Result<InnerL4Order> {
            let order = simple_inner_order(self.next_oid, side, sz.to_string(), limit_px.to_string())?;
            self.next_oid += 1;
            Ok(order)
        }

        fn batch_order(&mut self, sz: &str, limit_px: &str, side: Side, mult: u64) -> Result<Vec<InnerL4Order>> {
            (0..mult).map(|_| self.order(sz, limit_px, side)).try_collect()
        }
    }

    fn setup_book(book: &mut OrderBook<InnerL4Order>) -> Snapshots<InnerL4Order> {
        let mut o = OrderManager::default();
        let buy_orders1 = o.batch_order("100", "34.01", Side::Bid, 4).unwrap();
        let buy_orders2 = o.batch_order("200", "34.5", Side::Bid, 2).unwrap();
        let buy_orders3 = o.batch_order("300", "34.6", Side::Bid, 1).unwrap();
        let sell_orders1 = o.batch_order("100", "35", Side::Ask, 4).unwrap();
        let sell_orders2 = o.batch_order("200", "35.1", Side::Ask, 2).unwrap();
        let sell_orders3 = o.batch_order("300", "35.5", Side::Ask, 1).unwrap();
        for orders in [buy_orders1, buy_orders2, buy_orders3, sell_orders1, sell_orders2, sell_orders3] {
            for o in orders {
                book.add_order(o);
            }
        }
        Snapshots(vec![(Coin::new(""), book.to_snapshot()); 2].into_iter().collect())
    }

    const SNAPSHOT_JSON: &str = r#"[100, 
    [
        [
            "@1",
            [
                [
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.444",
                            "sz": "100.0",
                            "oid": 105338503859,
                            "timestamp": 1750660644034,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "100.0",
                            "tif": "Alo",
                            "cloid": null
                        }
                    ],
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.385",
                            "sz": "5.45",
                            "oid": 105337808436,
                            "timestamp": 1750660453608,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "5.45",
                            "tif": "Gtc",
                            "cloid": null
                        }
                    ]
                ],
                []
            ]
        ]
    ]
]"#;

    #[tokio::test]
    async fn test_deserialization_from_json() -> Result<()> {
        create_dir_all("tmp/deserialization_test")?;
        write("tmp/deserialization_test/out.json", SNAPSHOT_JSON)?;
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&PathBuf::from(
            "tmp/deserialization_test/out.json",
        ))
        .await?;
        Ok(())
    }

    #[test]
    fn test_deserialization() -> Result<()> {
        load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON)?;
        Ok(())
    }

    #[test]
    fn test_l4_snapshot_to_l2_snapshot() {
        let mut book = OrderBook::new();
        let coin = Coin::new("");
        let snapshot = setup_book(&mut book);
        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(2), Some(1));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![Level::new("34".to_string(), "1100".to_string(), 7)],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("36".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(3), Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.5".to_string(), "700".to_string(), 3),
                Level::new("34".to_string(), "400".to_string(), 4),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.5".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);
        let snapshot_from_book = book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_book = snapshot_from_book.export_inner_snapshot();
        let snapshot_from_book = book.to_l2_snapshot(None, None, None);
        let snapshot_from_snapshot = snapshot_from_book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_snapshot = snapshot_from_snapshot.export_inner_snapshot();
        assert_eq!(raw_levels_from_book, ans);
        assert_eq!(raw_levels_from_snapshot, ans);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), None, Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.6".to_string(), "300".to_string(), 1),
                Level::new("34.5".to_string(), "400".to_string(), 2),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.1".to_string(), "400".to_string(), 2),
            ],
        ];
        assert_eq!(ans, raw_levels);
    }
}
