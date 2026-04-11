use crate::order_book::types::{Coin, Oid, Px, Side, Sz};
use crate::order_book::{InnerOrder, Snapshot};
use crate::prelude::Result;
use crate::types::inner::{InnerL4Order, InnerOrderDiff};
use crate::types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus};
use serde::ser::{SerializeTuple, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

const PRICE_SCALE: u64 = 100_000_000;
const BTC_BUCKET_SIZE: u64 = 50 * PRICE_SCALE;
const ETH_BUCKET_SIZE: u64 = 5 * PRICE_SCALE;
const ANALYSIS_ROLLUP_BLOCKS: u64 = 10;
/// ±0.66% of mid price for near-mid WAP computation
const NEAR_MID_BPS: u64 = 66; // basis points (66 = 0.66%)
const MILLIS_PER_MINUTE: u64 = 60_000;
/// Cap individual order lifetimes at 120s to exclude stale resting orders from avg.
const LIFETIME_CAP_MS: u64 = 120_000;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L2BlockUpdate {
    #[serde(skip)]
    pub coin: String,
    pub b: Vec<(String, String)>,
    pub a: Vec<(String, String)>,
    #[serde(rename = "block_height")]
    pub block_height: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AnalysisUpdate {
    #[serde(skip, default = "Coin::default")]
    pub coin: Coin, // Captured by map key in response
    #[serde(skip, default = "Side::default")]
    pub side: Side,
    pub px: String,
    pub fill: String,
    pub fill_notional: String,
    pub change: String,
    pub change_notional: String,
}

impl Serialize for AnalysisUpdate {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_tuple(5)?;
        seq.serialize_element(&self.px)?;
        seq.serialize_element(&self.fill)?;
        seq.serialize_element(&self.fill_notional)?;
        seq.serialize_element(&self.change)?;
        seq.serialize_element(&self.change_notional)?;
        seq.end()
    }
}

fn fmt_px(v: u64) -> String {
    format!("{:.2}", v as f64 / 1e8)
}

fn fmt_sz(v: u64) -> String {
    if v == 0 { "0".to_string() } else { format!("{:.5}", v as f64 / 1e8) }
}

fn fmt_sz_signed(v: i64) -> String {
    if v == 0 {
        "0".to_string()
    } else if v > 0 {
        format!("+{:.5}", v as f64 / 1e8)
    } else {
        format!("-{:.5}", (-v) as f64 / 1e8)
    }
}

fn fmt_notional(v: u128) -> String {
    if v == 0 {
        return "0".to_string();
    }
    let rounded = (v + 500_000) / 1_000_000;
    let int_part = rounded / 100;
    let frac = rounded % 100;
    format!("{}.{:02}", int_part, frac)
}

fn fmt_notional_signed(v: i128) -> String {
    if v == 0 {
        return "0".to_string();
    }
    let neg = v < 0;
    let abs = if neg { -v } else { v } as u128;
    let rounded = (abs + 500_000) / 1_000_000;
    let int_part = rounded / 100;
    let frac = rounded % 100;
    if neg { format!("-{}.{:02}", int_part, frac) } else { format!("+{}.{:02}", int_part, frac) }
}

fn bucket_size_for_coin(coin: &Coin) -> u64 {
    let coin_value = coin.value();
    match coin_value.as_str() {
        "BTC" => BTC_BUCKET_SIZE,
        "ETH" => ETH_BUCKET_SIZE,
        _ => PRICE_SCALE,
    }
}

fn bucket_lower_px(coin: &Coin, px: u64) -> u64 {
    let size = bucket_size_for_coin(coin);
    if size == 0 {
        return px;
    }
    (px / size) * size
}

fn merge_bucket_maps(target: &mut HashMap<u64, BucketAgg>, source: &HashMap<u64, BucketAgg>) {
    for (px, agg) in source {
        let entry = target.entry(*px).or_default();
        entry.fill += agg.fill;
        entry.fill_notional += agg.fill_notional;
        entry.change += agg.change;
        entry.change_notional += agg.change_notional;
        entry.add_sz += agg.add_sz;
        entry.add_notional += agg.add_notional;
        entry.remove_sz += agg.remove_sz;
        entry.remove_notional += agg.remove_notional;
    }
}

fn format_sum_values(
    fill: u64,
    fill_notional: u128,
    change: i64,
    change_notional: i128,
    add_sz: u64,
    add_notional: u128,
    remove_sz: u64,
    remove_notional: u128,
    near_mid_wap: &str,
) -> [String; 9] {
    [
        fmt_sz(fill),
        fmt_notional(fill_notional),
        fmt_sz_signed(change),
        fmt_notional_signed(change_notional),
        fmt_sz(add_sz),
        fmt_notional(add_notional),
        fmt_sz(remove_sz),
        fmt_notional(remove_notional),
        near_mid_wap.to_string(),
    ]
}

fn bucket_map_to_updates(coin: &Coin, side: Side, buckets: &HashMap<u64, BucketAgg>) -> Vec<AnalysisUpdate> {
    let mut levels: Vec<(&u64, &BucketAgg)> = buckets.iter().collect();
    levels.sort_by_key(|(px, _)| *px);
    if side == Side::Bid {
        levels.reverse();
    }

    let mut updates = Vec::new();
    for (_bucket_px, agg) in levels {
        if agg.fill == 0 && agg.change == 0 {
            continue;
        }
        updates.push(AnalysisUpdate {
            coin: coin.clone(),
            side,
            px: fmt_px(*_bucket_px),
            fill: fmt_sz(agg.fill),
            fill_notional: fmt_notional(agg.fill_notional),
            change: fmt_sz_signed(agg.change),
            change_notional: fmt_notional_signed(agg.change_notional),
        });
    }
    updates
}

/// Minute-aligned aggregation emitted once per minute boundary crossing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MinuteAgg {
    /// Floored minute timestamp in milliseconds (start of the closed minute).
    pub ts_ms: u64,
    /// bid side: [fill_vol, fill_notional, add_vol, add_notional, remove_vol, remove_notional]
    pub bid: [String; 6],
    /// ask side: same layout
    pub ask: [String; 6],
    /// Near-mid WAP snapshot at minute close: (bid_wap, ask_wap)
    pub bid_near_wap: String,
    pub ask_near_wap: String,
    // ── Order book health metrics ──
    /// Bid-side depth slope: notional (USD) per bps within 50bps of mid
    pub slope_bid: String,
    /// Ask-side depth slope
    pub slope_ask: String,
    /// Bid-ask spread in basis points
    pub spread_bps: String,
    /// Herfindahl depth-concentration index near mid (0 = uniform, 1 = single level)
    pub concentration: String,
    /// Average time (ms) from order placement to fill this minute
    pub avg_fill_life_ms: String,
    /// Average time (ms) from order placement to cancel this minute
    pub avg_cancel_life_ms: String,
    /// Liquidity resilience: add_notional_near / (fill_notional_near + 1)
    pub resilience: String,
    /// Composite health score 0–100
    pub health_score: String,
    /// 0 = preview (57-58s pre-close), 1 = final minute close
    pub close: u8,
}

/// Snapshot of computed book-health metrics (internal, not serialized over WS).
struct BookHealthSnapshot {
    slope_bid: String,
    slope_ask: String,
    spread_bps: String,
    concentration: String,
    avg_fill_life_ms: String,
    avg_cancel_life_ms: String,
    resilience: String,
    health_score: String,
}

impl Default for BookHealthSnapshot {
    fn default() -> Self {
        Self {
            slope_bid: "0".to_string(),
            slope_ask: "0".to_string(),
            spread_bps: "0".to_string(),
            concentration: "0".to_string(),
            avg_fill_life_ms: "0".to_string(),
            avg_cancel_life_ms: "0".to_string(),
            resilience: "0".to_string(),
            health_score: "0".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct L4LiteBlockEvent {
    pub depth_updates: Option<L2BlockUpdate>,
    pub analysis_updates_b: Vec<AnalysisUpdate>,
    pub analysis_updates_a: Vec<AnalysisUpdate>,
    pub analysis_rollup_b: Vec<AnalysisUpdate>,
    pub analysis_rollup_a: Vec<AnalysisUpdate>,
    pub analysis_rollup_sum_b: Option<[String; 9]>,
    pub analysis_rollup_sum_a: Option<[String; 9]>,
    /// Minute aggregation(s) emitted this block (preview at 57s and/or close at minute boundary).
    pub minute_aggs: Vec<MinuteAgg>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L2Snapshot {
    pub bids: Vec<(String, String)>, // [px, sz]
    pub asks: Vec<(String, String)>,
    #[serde(rename = "block_height")]
    pub block_height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OrderInfo {
    pub coin: Coin,
    pub side: Side,
    pub px: Px,
    pub sz: Sz,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LevelState {
    pub sum_sz: Sz,               // Current aggregated quantity (real-time update)
    pub shadow_sum: Sz,           // Last sum sent to client
    pub orders: HashMap<Oid, Sz>, // Optional: for debugging/auditing
    pub dirty: bool,
}

impl LevelState {
    pub(crate) fn new() -> Self {
        Self { sum_sz: Sz::new(0), shadow_sum: Sz::new(0), orders: HashMap::new(), dirty: false }
    }
}

pub(crate) struct BookState {
    pub bids: BTreeMap<Px, LevelState>, // BTreeMap automatically sorted by Px
    pub asks: BTreeMap<Px, LevelState>,
    pub oid_index: HashMap<Oid, OrderInfo>,
    pub dirty_levels: Vec<(Side, Px)>,
    pub last_block_height: u64,
    analysis_window_b: HashMap<u64, BucketAgg>,
    analysis_window_a: HashMap<u64, BucketAgg>,
    analysis_sum_fill_b: u64,
    analysis_sum_fill_notional_b: u128,
    analysis_sum_change_b: i64,
    analysis_sum_change_notional_b: i128,
    analysis_sum_fill_a: u64,
    analysis_sum_fill_notional_a: u128,
    analysis_sum_change_a: i64,
    analysis_sum_change_notional_a: i128,
    analysis_sum_add_b: u64,
    analysis_sum_add_notional_b: u128,
    analysis_sum_remove_b: u64,
    analysis_sum_remove_notional_b: u128,
    analysis_sum_add_a: u64,
    analysis_sum_add_notional_a: u128,
    analysis_sum_remove_a: u64,
    analysis_sum_remove_notional_a: u128,
    initialized: bool,
    // ── Minute-level accumulator (independent of analysis rollup) ──
    min_current_minute: u64, // floored minute ts in ms (0 = not set)
    min_fill_b: u64,
    min_fill_notional_b: u128,
    min_fill_a: u64,
    min_fill_notional_a: u128,
    min_add_b: u64,
    min_add_notional_b: u128,
    min_add_a: u64,
    min_add_notional_a: u128,
    min_remove_b: u64,
    min_remove_notional_b: u128,
    min_remove_a: u64,
    min_remove_notional_a: u128,
    // ── Order lifecycle & health metrics per minute ──
    min_fill_life_sum_ms: u64,
    min_fill_life_count: u32,
    min_cancel_life_sum_ms: u64,
    min_cancel_life_count: u32,
    min_near_fill_notional: u128,
    min_near_add_notional: u128,
    /// Whether the 57-second preview has been sent for the current minute.
    min_preview_sent: bool,
    /// Ring buffer of recent closed minute aggs for snapshot warmup (capacity 5).
    recent_minute_aggs: VecDeque<MinuteAgg>,
}

#[derive(Debug, Clone, Default)]
struct BucketAgg {
    fill: u64,
    fill_notional: u128,
    change: i64,
    change_notional: i128,
    add_sz: u64,
    add_notional: u128,
    remove_sz: u64,
    remove_notional: u128,
}

#[derive(Debug, Clone, Default)]
struct BucketWindow {
    bids: HashMap<u64, BucketAgg>,
    asks: HashMap<u64, BucketAgg>,
}

impl BookState {
    pub(crate) fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            oid_index: HashMap::new(),
            dirty_levels: Vec::new(),
            last_block_height: 0,
            analysis_window_b: HashMap::new(),
            analysis_window_a: HashMap::new(),
            analysis_sum_fill_b: 0,
            analysis_sum_fill_notional_b: 0,
            analysis_sum_change_b: 0,
            analysis_sum_change_notional_b: 0,
            analysis_sum_fill_a: 0,
            analysis_sum_fill_notional_a: 0,
            analysis_sum_change_a: 0,
            analysis_sum_change_notional_a: 0,
            analysis_sum_add_b: 0,
            analysis_sum_add_notional_b: 0,
            analysis_sum_remove_b: 0,
            analysis_sum_remove_notional_b: 0,
            analysis_sum_add_a: 0,
            analysis_sum_add_notional_a: 0,
            analysis_sum_remove_a: 0,
            analysis_sum_remove_notional_a: 0,
            initialized: false,
            min_current_minute: 0,
            min_fill_b: 0,
            min_fill_notional_b: 0,
            min_fill_a: 0,
            min_fill_notional_a: 0,
            min_add_b: 0,
            min_add_notional_b: 0,
            min_add_a: 0,
            min_add_notional_a: 0,
            min_remove_b: 0,
            min_remove_notional_b: 0,
            min_remove_a: 0,
            min_remove_notional_a: 0,
            min_fill_life_sum_ms: 0,
            min_fill_life_count: 0,
            min_cancel_life_sum_ms: 0,
            min_cancel_life_count: 0,
            min_near_fill_notional: 0,
            min_near_add_notional: 0,
            min_preview_sent: false,
            recent_minute_aggs: VecDeque::with_capacity(5),
        }
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub(crate) fn snapshot_hash(&self) -> u64 {
        self.compute_snapshot_hash()
    }

    /// Compute size-weighted average price within ±0.66% of mid for each side.
    /// Returns (bid_wap_str, ask_wap_str) formatted as price strings.
    fn compute_near_mid_wap(&self) -> (String, String) {
        let best_bid = self.bids.keys().next_back();
        let best_ask = self.asks.keys().next();
        match (best_bid, best_ask) {
            (Some(bb), Some(ba)) => {
                let mid = (bb.value() + ba.value()) / 2;
                // threshold = mid * NEAR_MID_BPS / 10000
                let threshold = mid / 10_000 * NEAR_MID_BPS;
                let lower = mid.saturating_sub(threshold);
                let upper = mid.saturating_add(threshold);

                // Bid WAP: levels in [lower, best_bid], iterate from best down
                let mut bid_notional: u128 = 0;
                let mut bid_sz: u128 = 0;
                for (px, level) in self.bids.iter().rev() {
                    let pv = px.value();
                    if pv < lower {
                        break;
                    }
                    let sz = u128::from(level.sum_sz.value());
                    bid_sz += sz;
                    bid_notional += u128::from(pv) * sz;
                }

                // Ask WAP: levels in [best_ask, upper], iterate from best up
                let mut ask_notional: u128 = 0;
                let mut ask_sz: u128 = 0;
                for (px, level) in self.asks.iter() {
                    let pv = px.value();
                    if pv > upper {
                        break;
                    }
                    let sz = u128::from(level.sum_sz.value());
                    ask_sz += sz;
                    ask_notional += u128::from(pv) * sz;
                }

                let bid_wap = if bid_sz > 0 { fmt_px((bid_notional / bid_sz) as u64) } else { "0".to_string() };
                let ask_wap = if ask_sz > 0 { fmt_px((ask_notional / ask_sz) as u64) } else { "0".to_string() };
                (bid_wap, ask_wap)
            }
            _ => ("0".to_string(), "0".to_string()),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.oid_index.clear();
        self.dirty_levels.clear();
        self.last_block_height = 0;
        self.analysis_window_b.clear();
        self.analysis_window_a.clear();
        self.analysis_sum_fill_b = 0;
        self.analysis_sum_fill_notional_b = 0;
        self.analysis_sum_change_b = 0;
        self.analysis_sum_change_notional_b = 0;
        self.analysis_sum_fill_a = 0;
        self.analysis_sum_fill_notional_a = 0;
        self.analysis_sum_change_a = 0;
        self.analysis_sum_change_notional_a = 0;
        self.analysis_sum_add_b = 0;
        self.analysis_sum_add_notional_b = 0;
        self.analysis_sum_remove_b = 0;
        self.analysis_sum_remove_notional_b = 0;
        self.analysis_sum_add_a = 0;
        self.analysis_sum_add_notional_a = 0;
        self.analysis_sum_remove_a = 0;
        self.analysis_sum_remove_notional_a = 0;
        self.initialized = false;
        self.min_current_minute = 0;
        self.min_preview_sent = false;
        // Note: do NOT clear recent_minute_aggs on reset – they survive reconnects for warmup.
        self.reset_min_accumulators();
    }

    pub(crate) fn get_snapshot(&self) -> L2Snapshot {
        fn fmt_level(px: &Px, level: &LevelState) -> (String, String) {
            (fmt_px(px.value()), fmt_sz(level.sum_sz.value()))
        }

        // Collect bids (Reverse order for bids usually in display, High to Low)
        let bids = self.bids.iter().rev().map(|(px, lvl)| fmt_level(px, lvl)).collect();
        // Collect asks (Low to High)
        let asks = self.asks.iter().map(|(px, lvl)| fmt_level(px, lvl)).collect();

        L2Snapshot { bids, asks, block_height: self.last_block_height }
    }

    fn reset_min_accumulators(&mut self) {
        self.min_fill_b = 0;
        self.min_fill_notional_b = 0;
        self.min_fill_a = 0;
        self.min_fill_notional_a = 0;
        self.min_add_b = 0;
        self.min_add_notional_b = 0;
        self.min_add_a = 0;
        self.min_add_notional_a = 0;
        self.min_remove_b = 0;
        self.min_remove_notional_b = 0;
        self.min_remove_a = 0;
        self.min_remove_notional_a = 0;
        self.min_fill_life_sum_ms = 0;
        self.min_fill_life_count = 0;
        self.min_cancel_life_sum_ms = 0;
        self.min_cancel_life_count = 0;
        self.min_near_fill_notional = 0;
        self.min_near_add_notional = 0;
    }

    fn take_minute_agg(&mut self) -> MinuteAgg {
        let (bid_wap, ask_wap) = self.compute_near_mid_wap();
        let health = self.compute_book_health_metrics();
        let agg = MinuteAgg {
            ts_ms: self.min_current_minute + MILLIS_PER_MINUTE - 1,
            bid: [
                fmt_sz(self.min_fill_b),
                fmt_notional(self.min_fill_notional_b),
                fmt_sz(self.min_add_b),
                fmt_notional(self.min_add_notional_b),
                fmt_sz(self.min_remove_b),
                fmt_notional(self.min_remove_notional_b),
            ],
            ask: [
                fmt_sz(self.min_fill_a),
                fmt_notional(self.min_fill_notional_a),
                fmt_sz(self.min_add_a),
                fmt_notional(self.min_add_notional_a),
                fmt_sz(self.min_remove_a),
                fmt_notional(self.min_remove_notional_a),
            ],
            bid_near_wap: bid_wap,
            ask_near_wap: ask_wap,
            slope_bid: health.slope_bid,
            slope_ask: health.slope_ask,
            spread_bps: health.spread_bps,
            concentration: health.concentration,
            avg_fill_life_ms: health.avg_fill_life_ms,
            avg_cancel_life_ms: health.avg_cancel_life_ms,
            resilience: health.resilience,
            health_score: health.health_score,
            close: 1,
        };
        // Store in ring buffer for snapshot warmup (keep last 5 closed minutes).
        if self.recent_minute_aggs.len() >= 5 {
            self.recent_minute_aggs.pop_front();
        }
        self.recent_minute_aggs.push_back(agg.clone());
        self.reset_min_accumulators();
        agg
    }

    /// Build a MinuteAgg snapshot from current accumulators WITHOUT resetting them.
    /// Used for the 57-second preview push (close=0).
    fn snapshot_minute_agg(&self) -> MinuteAgg {
        let (bid_wap, ask_wap) = self.compute_near_mid_wap();
        let health = self.compute_book_health_metrics();
        MinuteAgg {
            ts_ms: self.min_current_minute + MILLIS_PER_MINUTE - 1,
            bid: [
                fmt_sz(self.min_fill_b),
                fmt_notional(self.min_fill_notional_b),
                fmt_sz(self.min_add_b),
                fmt_notional(self.min_add_notional_b),
                fmt_sz(self.min_remove_b),
                fmt_notional(self.min_remove_notional_b),
            ],
            ask: [
                fmt_sz(self.min_fill_a),
                fmt_notional(self.min_fill_notional_a),
                fmt_sz(self.min_add_a),
                fmt_notional(self.min_add_notional_a),
                fmt_sz(self.min_remove_a),
                fmt_notional(self.min_remove_notional_a),
            ],
            bid_near_wap: bid_wap,
            ask_near_wap: ask_wap,
            slope_bid: health.slope_bid,
            slope_ask: health.slope_ask,
            spread_bps: health.spread_bps,
            concentration: health.concentration,
            avg_fill_life_ms: health.avg_fill_life_ms,
            avg_cancel_life_ms: health.avg_cancel_life_ms,
            resilience: health.resilience,
            health_score: health.health_score,
            close: 0,
        }
    }

    /// Return cloned recent minute aggs for snapshot warmup on subscriber connect.
    pub(crate) fn get_recent_minute_aggs(&self) -> Vec<MinuteAgg> {
        self.recent_minute_aggs.iter().cloned().collect()
    }

    /// Compute depth slope for one side.
    /// Returns (slope_usd_per_bps, total_depth_usd) within ±`threshold` of `mid`.
    fn side_slope(&self, levels: &BTreeMap<Px, LevelState>, mid: u64, threshold: u64, is_bid: bool) -> (f64, f64) {
        let scale_sq = (PRICE_SCALE as f64) * (PRICE_SCALE as f64);
        let mut total_notional: f64 = 0.0;
        let iter: Box<dyn Iterator<Item = (&Px, &LevelState)>> =
            if is_bid { Box::new(levels.iter().rev()) } else { Box::new(levels.iter()) };
        for (px, level) in iter {
            let pv = px.value();
            let dist = if pv > mid { pv - mid } else { mid - pv };
            if dist > threshold {
                break;
            }
            let sz_usd = (level.sum_sz.value() as f64 * pv as f64) / scale_sq;
            total_notional += sz_usd;
        }
        let window_bps = (threshold as f64 / mid as f64) * 10000.0;
        let slope = if window_bps > 0.0 { total_notional / window_bps } else { 0.0 };
        (slope, total_notional)
    }

    /// Compute Herfindahl concentration index for one side within ±`threshold` of `mid`.
    fn side_concentration(&self, levels: &BTreeMap<Px, LevelState>, mid: u64, threshold: u64, is_bid: bool) -> f64 {
        let iter: Box<dyn Iterator<Item = (&Px, &LevelState)>> =
            if is_bid { Box::new(levels.iter().rev()) } else { Box::new(levels.iter()) };
        let mut depths: Vec<f64> = Vec::new();
        let mut total: f64 = 0.0;
        for (px, level) in iter {
            let pv = px.value();
            let dist = if pv > mid { pv - mid } else { mid - pv };
            if dist > threshold {
                break;
            }
            let d = level.sum_sz.value() as f64;
            if d > 0.0 {
                depths.push(d);
                total += d;
            }
        }
        if total <= 0.0 {
            return 0.0;
        }
        depths
            .iter()
            .map(|d| {
                let s = d / total;
                s * s
            })
            .sum()
    }

    /// Compute book-health metrics from current snapshot + minute accumulators.
    fn compute_book_health_metrics(&self) -> BookHealthSnapshot {
        let best_bid = self.bids.keys().next_back().map(|p| p.value()).unwrap_or(0);
        let best_ask = self.asks.keys().next().map(|p| p.value()).unwrap_or(0);
        if best_bid == 0 || best_ask == 0 {
            return BookHealthSnapshot::default();
        }

        let mid = (best_bid + best_ask) / 2;
        let spread_bps_f = (best_ask as f64 - best_bid as f64) / mid as f64 * 10000.0;

        // ±50 bps for slope
        let slope_threshold = mid / 10_000 * 50;
        // ±100 bps for concentration
        let conc_threshold = mid / 100;

        let (slope_bid_f, _) = self.side_slope(&self.bids, mid, slope_threshold, true);
        let (slope_ask_f, _) = self.side_slope(&self.asks, mid, slope_threshold, false);

        let bid_conc = self.side_concentration(&self.bids, mid, conc_threshold, true);
        let ask_conc = self.side_concentration(&self.asks, mid, conc_threshold, false);
        let concentration_f = (bid_conc + ask_conc) / 2.0;

        // Average lifetimes
        let avg_fill = if self.min_fill_life_count > 0 {
            self.min_fill_life_sum_ms as f64 / self.min_fill_life_count as f64
        } else {
            0.0
        };
        let avg_cancel = if self.min_cancel_life_count > 0 {
            self.min_cancel_life_sum_ms as f64 / self.min_cancel_life_count as f64
        } else {
            0.0
        };

        // Resilience: add_near_usd / (fill_near_usd + 1) — values store USD * 1e8
        let fill_near_usd = self.min_near_fill_notional as f64 / PRICE_SCALE as f64;
        let add_near_usd = self.min_near_add_notional as f64 / PRICE_SCALE as f64;
        let resilience_f = add_near_usd / (fill_near_usd + 1.0);

        // ── Composite health score 0–100 ──
        // Spread: reference 5 bps (HL perp books are usually tight)
        let spread_score = (1.0 - spread_bps_f / 5.0).clamp(0.0, 1.0);
        // Slope: higher = healthier; reference $50k notional per bps
        let slope_score = ((slope_bid_f + slope_ask_f) / 2.0 / 50_000.0).min(1.0);
        // Concentration: lower HHI = more evenly distributed = healthier
        let conc_score = (1.0 - concentration_f).clamp(0.0, 1.0);
        // Resilience: ratio capped at 1.5 for scoring, normalized to [0,1]
        let resil_score = (resilience_f / 1.5).min(1.0);
        let health =
            (spread_score * 25.0 + slope_score * 25.0 + conc_score * 25.0 + resil_score * 25.0).clamp(0.0, 100.0);

        BookHealthSnapshot {
            slope_bid: format!("{:.2}", slope_bid_f),
            slope_ask: format!("{:.2}", slope_ask_f),
            spread_bps: format!("{:.2}", spread_bps_f),
            concentration: format!("{:.4}", concentration_f),
            avg_fill_life_ms: format!("{:.0}", avg_fill),
            avg_cancel_life_ms: format!("{:.0}", avg_cancel),
            resilience: format!("{:.4}", resilience_f),
            health_score: format!("{:.1}", health),
        }
    }

    pub(crate) fn process_block(
        &mut self,
        coin: Coin,
        statuses: &Vec<NodeDataOrderStatus>,
        diffs: &Vec<NodeDataOrderDiff>,
        fills: &Vec<NodeDataFill>,
        block_height: u64,
        block_time_ms: u64,
    ) -> Result<L4LiteBlockEvent> {
        if !self.initialized {
            return Err("L4Lite book not initialized; waiting for snapshot".into());
        }
        if block_height <= self.last_block_height {
            return Ok(L4LiteBlockEvent {
                depth_updates: None,
                analysis_updates_b: Vec::new(),
                analysis_updates_a: Vec::new(),
                analysis_rollup_b: Vec::new(),
                analysis_rollup_a: Vec::new(),
                analysis_rollup_sum_b: None,
                analysis_rollup_sum_a: None,
                minute_aggs: Vec::new(),
            });
        }

        // ── Minute boundary detection ──
        let block_minute = (block_time_ms / MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE;
        let mut pending_minute_aggs: Vec<MinuteAgg> = Vec::new();
        if self.min_current_minute == 0 {
            // First block after init: just set the minute, don't emit
            self.min_current_minute = block_minute;
        } else if block_minute > self.min_current_minute {
            // New minute — emit the final close agg (close=1) and reset
            pending_minute_aggs.push(self.take_minute_agg());
            self.min_current_minute = block_minute;
            self.min_preview_sent = false;
        }
        if block_height != self.last_block_height + 1 {
            return Err(format!("Expecting block {}, got {}", self.last_block_height + 1, block_height).into());
        }

        // 1. Identify all affected levels (sides/px)
        let mut affected_levels: HashSet<(Side, Px)> = HashSet::new();

        // Aggregate fills for this block: (Side, Px) -> Sz
        let mut filled_amts: HashMap<(Side, Px), u64> = HashMap::new();
        let mut filled_oids: HashSet<Oid> = HashSet::new();
        for fill in fills {
            // Fill struct: pub struct Fill { pub coin: String, pub px: String, pub sz: String, pub side: Side, ... }
            // NodeDataFill(pub Address, pub Fill);
            let raw_fill = &fill.1;

            if raw_fill.crossed {
                continue;
            }

            // Note: Fill side is "Maker" side usually? Or "Taker"?
            // We checked: Trade logic implies Fill side is side of the order (Maker if not crossed).
            let side = raw_fill.side;
            let px = match Px::parse_from_str(&raw_fill.px) {
                Ok(px) => px,
                Err(err) => {
                    log::warn!(
                        "[{:?}] Skipping fill with invalid px {} at height {}: {}",
                        coin,
                        raw_fill.px,
                        block_height,
                        err
                    );
                    continue;
                }
            };
            let sz = match Sz::parse_from_str(&raw_fill.sz) {
                Ok(sz) => sz,
                Err(err) => {
                    log::warn!(
                        "[{:?}] Skipping fill with invalid sz {} at height {}: {}",
                        coin,
                        raw_fill.sz,
                        block_height,
                        err
                    );
                    continue;
                }
            };

            *filled_amts.entry((side, px)).or_default() += sz.value();
            affected_levels.insert((side, px));
            // Track fill lifetime: order placement → fill
            let fill_oid = Oid::new(raw_fill.oid);
            filled_oids.insert(fill_oid.clone());
            if let Some(info) = self.oid_index.get(&fill_oid) {
                if info.created_at_ms > 0 && raw_fill.time >= info.created_at_ms {
                    let lifetime = raw_fill.time - info.created_at_ms;
                    if lifetime <= LIFETIME_CAP_MS {
                        self.min_fill_life_sum_ms = self.min_fill_life_sum_ms.saturating_add(lifetime);
                        self.min_fill_life_count += 1;
                    }
                }
            }
        }

        // Update Index from Statuses
        // Build a map from Oid -> status for orders that are inserted into book (same logic as L4Book)
        // Do NOT insert into oid_index yet - wait for New diffs to trigger insertion
        let mut order_map: HashMap<Oid, NodeDataOrderStatus> = HashMap::new();
        for status in statuses {
            let oid = Oid::new(status.order.oid);
            // Only record statuses that represent orders inserted into the book (same as L4Book)
            if status.is_inserted_into_book() {
                order_map.insert(oid, status.clone());
            }
        }

        log::debug!(
            "[{:?}] Block {}: {} statuses -> {} in order_map, {} diffs to process",
            coin,
            block_height,
            statuses.len(),
            order_map.len(),
            diffs.len()
        );

        // Process Diffs & Calculate Deltas
        let mut level_deltas: HashMap<(Side, Px), i64> = HashMap::new();
        let mut removed_levels: HashSet<(Side, Px)> = HashSet::new();
        for diff in diffs {
            let oid = diff.oid();

            // Process diffs matching L4Book logic exactly:
            // - New: must have matching status in this block
            // - Update/Remove: must already exist in oid_index
            let inner_diff: InnerOrderDiff =
                diff.diff().try_into().map_err(|e| format!("Unable to parse diff {diff:?}: {e}"))?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    let status =
                        order_map.remove(&oid).ok_or_else(|| format!("Missing status for New diff {diff:?}"))?;
                    let mut inner: InnerL4Order = status
                        .clone()
                        .try_into()
                        .map_err(|e| format!("Unable to build order from status {status:?}: {e}"))?;
                    let time = status.time.and_utc().timestamp_millis();
                    inner.modify_sz(sz);
                    #[allow(clippy::unwrap_used)]
                    inner.convert_trigger(time.try_into().unwrap());
                    let created_ms = inner.timestamp;
                    self.add_order(
                        inner.oid(),
                        inner.coin.clone(),
                        inner.side(),
                        inner.limit_px(),
                        inner.sz(),
                        created_ms,
                    );
                    affected_levels.insert((inner.side(), inner.limit_px()));
                    *level_deltas.entry((inner.side(), inner.limit_px())).or_default() += sz.value() as i64;
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    let info =
                        self.oid_index.get(&oid).ok_or_else(|| format!("Missing order for Update diff {diff:?}"))?;
                    if info.coin != coin {
                        return Err(format!("Coin mismatch for Update diff {diff:?}").into());
                    }
                    let side = info.side;
                    let px = info.px;
                    let old_sz_val = info.sz.value();
                    affected_levels.insert((side, px));

                    let parsed_new_sz = new_sz.value();
                    let delta = parsed_new_sz as i64 - old_sz_val as i64;
                    *level_deltas.entry((side, px)).or_default() += delta;

                    self.update_order_sz(oid, new_sz);
                }
                InnerOrderDiff::Remove => {
                    let info =
                        self.oid_index.get(&oid).ok_or_else(|| format!("Missing order for Remove diff {diff:?}"))?;
                    if info.coin != coin {
                        return Err(format!("Coin mismatch for Remove diff {diff:?}").into());
                    }
                    let side = info.side;
                    let px = info.px;
                    let old_sz_val = info.sz.value();
                    // Track cancel lifetime: Remove WITHOUT fill = user cancel
                    if !filled_oids.contains(&oid) && info.created_at_ms > 0 && block_time_ms >= info.created_at_ms {
                        let lifetime = block_time_ms - info.created_at_ms;
                        if lifetime <= LIFETIME_CAP_MS {
                            self.min_cancel_life_sum_ms = self.min_cancel_life_sum_ms.saturating_add(lifetime);
                            self.min_cancel_life_count += 1;
                        }
                    }
                    affected_levels.insert((side, px));

                    let delta = (0i64) - (old_sz_val as i64);
                    *level_deltas.entry((side, px)).or_default() += delta;

                    if self.remove_order(oid) {
                        removed_levels.insert((side, px));
                    }
                }
            }
        }

        // 4. Generate Analysis & Updates
        let mut b_updates = Vec::new();
        let mut a_updates = Vec::new();
        let mut block_buckets_b: HashMap<u64, BucketAgg> = HashMap::new();
        let mut block_buckets_a: HashMap<u64, BucketAgg> = HashMap::new();
        let mut block_fill_b: u64 = 0;
        let mut block_fill_notional_b: u128 = 0;
        let mut block_change_b: i64 = 0;
        let mut block_change_notional_b: i128 = 0;
        let mut block_add_b: u64 = 0;
        let mut block_add_notional_b: u128 = 0;
        let mut block_remove_b: u64 = 0;
        let mut block_remove_notional_b: u128 = 0;
        let mut block_fill_a: u64 = 0;
        let mut block_fill_notional_a: u128 = 0;
        let mut block_change_a: i64 = 0;
        let mut block_change_notional_a: i128 = 0;
        let mut block_add_a: u64 = 0;
        let mut block_add_notional_a: u128 = 0;
        let mut block_remove_a: u64 = 0;
        let mut block_remove_notional_a: u128 = 0;

        // Mid price for near-mid resilience tracking
        let mid_px = {
            let bb = self.bids.keys().next_back().map(|p| p.value()).unwrap_or(0);
            let ba = self.asks.keys().next().map(|p| p.value()).unwrap_or(0);
            if bb > 0 && ba > 0 { (bb + ba) / 2 } else { 0 }
        };
        let near_threshold = if mid_px > 0 { mid_px / 10_000 * NEAR_MID_BPS } else { 0 };

        for (side, px) in affected_levels {
            let filled_val = *filled_amts.get(&(side, px)).unwrap_or(&0);
            let delta_total = *level_deltas.get(&(side, px)).unwrap_or(&0);

            // Isolate pure order placement/cancellation from fill-induced sz decrease.
            // delta_total includes fill effect (fills reduce sz → negative delta);
            // adding filled_val back yields the non-fill book change.
            let change = delta_total.saturating_add(filled_val as i64);

            if filled_val > 0 || change != 0 {
                let px_u128 = u128::from(px.value());
                let change_notional = (i128::from(px.value()) * i128::from(change)) / i128::from(PRICE_SCALE);
                let fill_notional = (px_u128 * u128::from(filled_val)) / u128::from(PRICE_SCALE);
                // Near-mid tracking for resilience score
                if near_threshold > 0 {
                    let dist = if px.value() > mid_px { px.value() - mid_px } else { mid_px - px.value() };
                    if dist <= near_threshold {
                        if filled_val > 0 {
                            self.min_near_fill_notional = self.min_near_fill_notional.saturating_add(fill_notional);
                        }
                        if change > 0 {
                            self.min_near_add_notional = self
                                .min_near_add_notional
                                .saturating_add((px_u128 * u128::from(change as u64)) / u128::from(PRICE_SCALE));
                        }
                    }
                }
                match side {
                    Side::Bid => {
                        block_fill_b = block_fill_b.saturating_add(filled_val);
                        block_fill_notional_b = block_fill_notional_b.saturating_add(fill_notional);
                        block_change_b = block_change_b.saturating_add(change);
                        block_change_notional_b = block_change_notional_b.saturating_add(change_notional);
                        if change > 0 {
                            let add_val = change as u64;
                            block_add_b = block_add_b.saturating_add(add_val);
                            block_add_notional_b = block_add_notional_b
                                .saturating_add((px_u128 * u128::from(add_val)) / u128::from(PRICE_SCALE));
                        } else {
                            let rm_val = (-change) as u64;
                            block_remove_b = block_remove_b.saturating_add(rm_val);
                            block_remove_notional_b = block_remove_notional_b
                                .saturating_add((px_u128 * u128::from(rm_val)) / u128::from(PRICE_SCALE));
                        }
                    }
                    Side::Ask => {
                        block_fill_a = block_fill_a.saturating_add(filled_val);
                        block_fill_notional_a = block_fill_notional_a.saturating_add(fill_notional);
                        block_change_a = block_change_a.saturating_add(change);
                        block_change_notional_a = block_change_notional_a.saturating_add(change_notional);
                        if change > 0 {
                            let add_val = change as u64;
                            block_add_a = block_add_a.saturating_add(add_val);
                            block_add_notional_a = block_add_notional_a
                                .saturating_add((px_u128 * u128::from(add_val)) / u128::from(PRICE_SCALE));
                        } else {
                            let rm_val = (-change) as u64;
                            block_remove_a = block_remove_a.saturating_add(rm_val);
                            block_remove_notional_a = block_remove_notional_a
                                .saturating_add((px_u128 * u128::from(rm_val)) / u128::from(PRICE_SCALE));
                        }
                    }
                }
                let bucket_px = bucket_lower_px(&coin, px.value());
                let target = match side {
                    Side::Bid => &mut block_buckets_b,
                    Side::Ask => &mut block_buckets_a,
                };
                let entry = target.entry(bucket_px).or_default();
                entry.fill += filled_val;
                entry.fill_notional += fill_notional;
                entry.change += change;
                entry.change_notional += change_notional;
                if change > 0 {
                    entry.add_sz += change as u64;
                    entry.add_notional += (px_u128 * u128::from(change as u64)) / u128::from(PRICE_SCALE);
                } else {
                    entry.remove_sz += (-change) as u64;
                    entry.remove_notional += (px_u128 * u128::from((-change) as u64)) / u128::from(PRICE_SCALE);
                }
            }

            let state = match side {
                Side::Bid => self.bids.get_mut(&px),
                Side::Ask => self.asks.get_mut(&px),
            };

            if let Some(lvl) = state {
                if lvl.dirty {
                    match side {
                        Side::Bid => b_updates.push((fmt_px(px.value()), fmt_sz(lvl.sum_sz.value()))),
                        Side::Ask => a_updates.push((fmt_px(px.value()), fmt_sz(lvl.sum_sz.value()))),
                    }
                    lvl.shadow_sum = lvl.sum_sz;
                    lvl.dirty = false;
                }
            } else if removed_levels.contains(&(side, px)) {
                match side {
                    Side::Bid => b_updates.push((fmt_px(px.value()), "0".to_string())),
                    Side::Ask => a_updates.push((fmt_px(px.value()), "0".to_string())),
                }
            }
        }

        let depth_updates = Some(L2BlockUpdate { coin: coin.value(), b: b_updates, a: a_updates, block_height });

        // ── Accumulate into minute-level accumulators ──
        self.min_fill_b = self.min_fill_b.saturating_add(block_fill_b);
        self.min_fill_notional_b = self.min_fill_notional_b.saturating_add(block_fill_notional_b);
        self.min_fill_a = self.min_fill_a.saturating_add(block_fill_a);
        self.min_fill_notional_a = self.min_fill_notional_a.saturating_add(block_fill_notional_a);
        self.min_add_b = self.min_add_b.saturating_add(block_add_b);
        self.min_add_notional_b = self.min_add_notional_b.saturating_add(block_add_notional_b);
        self.min_add_a = self.min_add_a.saturating_add(block_add_a);
        self.min_add_notional_a = self.min_add_notional_a.saturating_add(block_add_notional_a);
        self.min_remove_b = self.min_remove_b.saturating_add(block_remove_b);
        self.min_remove_notional_b = self.min_remove_notional_b.saturating_add(block_remove_notional_b);
        self.min_remove_a = self.min_remove_a.saturating_add(block_remove_a);
        self.min_remove_notional_a = self.min_remove_notional_a.saturating_add(block_remove_notional_a);

        merge_bucket_maps(&mut self.analysis_window_b, &block_buckets_b);
        merge_bucket_maps(&mut self.analysis_window_a, &block_buckets_a);
        self.analysis_sum_fill_b = self.analysis_sum_fill_b.saturating_add(block_fill_b);
        self.analysis_sum_fill_notional_b = self.analysis_sum_fill_notional_b.saturating_add(block_fill_notional_b);
        self.analysis_sum_change_b = self.analysis_sum_change_b.saturating_add(block_change_b);
        self.analysis_sum_change_notional_b =
            self.analysis_sum_change_notional_b.saturating_add(block_change_notional_b);
        self.analysis_sum_fill_a = self.analysis_sum_fill_a.saturating_add(block_fill_a);
        self.analysis_sum_fill_notional_a = self.analysis_sum_fill_notional_a.saturating_add(block_fill_notional_a);
        self.analysis_sum_change_a = self.analysis_sum_change_a.saturating_add(block_change_a);
        self.analysis_sum_change_notional_a =
            self.analysis_sum_change_notional_a.saturating_add(block_change_notional_a);
        self.analysis_sum_add_b = self.analysis_sum_add_b.saturating_add(block_add_b);
        self.analysis_sum_add_notional_b = self.analysis_sum_add_notional_b.saturating_add(block_add_notional_b);
        self.analysis_sum_remove_b = self.analysis_sum_remove_b.saturating_add(block_remove_b);
        self.analysis_sum_remove_notional_b =
            self.analysis_sum_remove_notional_b.saturating_add(block_remove_notional_b);
        self.analysis_sum_add_a = self.analysis_sum_add_a.saturating_add(block_add_a);
        self.analysis_sum_add_notional_a = self.analysis_sum_add_notional_a.saturating_add(block_add_notional_a);
        self.analysis_sum_remove_a = self.analysis_sum_remove_a.saturating_add(block_remove_a);
        self.analysis_sum_remove_notional_a =
            self.analysis_sum_remove_notional_a.saturating_add(block_remove_notional_a);

        let analysis_b = bucket_map_to_updates(&coin, Side::Bid, &block_buckets_b);
        let analysis_a = bucket_map_to_updates(&coin, Side::Ask, &block_buckets_a);

        let mut analysis_rollup_b = Vec::new();
        let mut analysis_rollup_a = Vec::new();
        let mut analysis_rollup_sum_b = None;
        let mut analysis_rollup_sum_a = None;
        if block_height % ANALYSIS_ROLLUP_BLOCKS == 0 {
            let window = BucketWindow {
                bids: std::mem::take(&mut self.analysis_window_b),
                asks: std::mem::take(&mut self.analysis_window_a),
            };
            // Original rolling-window logic (kept for reference):
            /*
            self.analysis_rollups.push_back(window);
            if self.analysis_rollups.len() > ANALYSIS_ROLLUP_WINDOWS {
                self.analysis_rollups.pop_front();
            }

            let mut agg_b = HashMap::new();
            let mut agg_a = HashMap::new();
            for window in &self.analysis_rollups {
                merge_bucket_maps(&mut agg_b, &window.bids);
                merge_bucket_maps(&mut agg_a, &window.asks);
            }
            analysis_rollup_b = bucket_map_to_updates(&coin, Side::Bid, &agg_b);
            analysis_rollup_a = bucket_map_to_updates(&coin, Side::Ask, &agg_a);
            let (fill_b, fill_notional_b, change_b, change_notional_b) = sum_bucket_map(&agg_b);
            let (fill_a, fill_notional_a, change_a, change_notional_a) = sum_bucket_map(&agg_a);
            analysis_rollup_sum_b = Some(format_sum_values(fill_b, fill_notional_b, change_b, change_notional_b));
            analysis_rollup_sum_a = Some(format_sum_values(fill_a, fill_notional_a, change_a, change_notional_a));
            */

            // New behavior: only aggregate the latest 10-block window (no rolling accumulation).
            analysis_rollup_b = bucket_map_to_updates(&coin, Side::Bid, &window.bids);
            analysis_rollup_a = bucket_map_to_updates(&coin, Side::Ask, &window.asks);
            let (bid_wap, ask_wap) = self.compute_near_mid_wap();
            analysis_rollup_sum_b = Some(format_sum_values(
                self.analysis_sum_fill_b,
                self.analysis_sum_fill_notional_b,
                self.analysis_sum_change_b,
                self.analysis_sum_change_notional_b,
                self.analysis_sum_add_b,
                self.analysis_sum_add_notional_b,
                self.analysis_sum_remove_b,
                self.analysis_sum_remove_notional_b,
                &bid_wap,
            ));
            analysis_rollup_sum_a = Some(format_sum_values(
                self.analysis_sum_fill_a,
                self.analysis_sum_fill_notional_a,
                self.analysis_sum_change_a,
                self.analysis_sum_change_notional_a,
                self.analysis_sum_add_a,
                self.analysis_sum_add_notional_a,
                self.analysis_sum_remove_a,
                self.analysis_sum_remove_notional_a,
                &ask_wap,
            ));
            self.analysis_sum_fill_b = 0;
            self.analysis_sum_fill_notional_b = 0;
            self.analysis_sum_change_b = 0;
            self.analysis_sum_change_notional_b = 0;
            self.analysis_sum_add_b = 0;
            self.analysis_sum_add_notional_b = 0;
            self.analysis_sum_remove_b = 0;
            self.analysis_sum_remove_notional_b = 0;
            self.analysis_sum_fill_a = 0;
            self.analysis_sum_fill_notional_a = 0;
            self.analysis_sum_change_a = 0;
            self.analysis_sum_change_notional_a = 0;
            self.analysis_sum_add_a = 0;
            self.analysis_sum_add_notional_a = 0;
            self.analysis_sum_remove_a = 0;
            self.analysis_sum_remove_notional_a = 0;
        }

        // ── 58-second preview check (after block data accumulated) ──
        let ms_in_minute = block_time_ms % MILLIS_PER_MINUTE;
        if ms_in_minute >= 58_000 && !self.min_preview_sent && self.min_current_minute > 0 {
            pending_minute_aggs.push(self.snapshot_minute_agg());
            self.min_preview_sent = true;
        }

        let event = L4LiteBlockEvent {
            depth_updates,
            analysis_updates_b: analysis_b,
            analysis_updates_a: analysis_a,
            analysis_rollup_b,
            analysis_rollup_a,
            analysis_rollup_sum_b,
            analysis_rollup_sum_a,
            minute_aggs: pending_minute_aggs,
        };
        self.last_block_height = block_height;
        Ok(event)
    }

    pub(crate) fn init_from_snapshot(&mut self, snapshot: &Snapshot<InnerL4Order>, block_height: u64) {
        // Clear existing state
        self.bids.clear();
        self.asks.clear();
        self.oid_index.clear();
        self.dirty_levels.clear();
        self.last_block_height = block_height;
        self.analysis_window_b.clear();
        self.analysis_window_a.clear();
        self.analysis_sum_fill_b = 0;
        self.analysis_sum_fill_notional_b = 0;
        self.analysis_sum_change_b = 0;
        self.analysis_sum_change_notional_b = 0;
        self.analysis_sum_fill_a = 0;
        self.analysis_sum_fill_notional_a = 0;
        self.analysis_sum_change_a = 0;
        self.analysis_sum_change_notional_a = 0;
        self.analysis_sum_add_b = 0;
        self.analysis_sum_add_notional_b = 0;
        self.analysis_sum_remove_b = 0;
        self.analysis_sum_remove_notional_b = 0;
        self.analysis_sum_add_a = 0;
        self.analysis_sum_add_notional_a = 0;
        self.analysis_sum_remove_a = 0;
        self.analysis_sum_remove_notional_a = 0;
        self.initialized = true;
        self.min_current_minute = 0;
        self.min_preview_sent = false;
        // Note: do NOT clear recent_minute_aggs on snapshot rebuild – they survive for warmup.
        self.reset_min_accumulators();

        for orders in snapshot.as_ref() {
            for order in orders {
                self.add_order(
                    order.oid(),
                    order.coin.clone(),
                    order.side(),
                    order.limit_px(),
                    order.sz(),
                    order.timestamp,
                );
            }
        }
    }

    fn add_order(&mut self, oid: Oid, coin: Coin, side: Side, px: Px, sz: Sz, created_at_ms: u64) {
        if let Some(_) = self.oid_index.get(&oid) {
            // Already exists, just update?
            self.update_order_sz(oid, sz);
            return;
        }

        let info = OrderInfo { coin, side, px, sz, created_at_ms };
        self.oid_index.insert(oid.clone(), info);

        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        let level = levels.entry(px).or_insert_with(LevelState::new);
        level.sum_sz = Sz::new(level.sum_sz.value().saturating_add(sz.value()));
        level.orders.insert(oid, sz);
        level.dirty = true;
        self.dirty_levels.push((side, px));
    }

    fn update_order_sz(&mut self, oid: Oid, new_sz: Sz) {
        if let Some(info) = self.oid_index.get_mut(&oid) {
            let old_sz = info.sz;
            if old_sz.value() == new_sz.value() {
                return;
            }
            info.sz = new_sz;
            let side = info.side;
            let px = info.px;

            let levels = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };

            if let Some(level) = levels.get_mut(&px) {
                // Update or remove order entry
                level.orders.insert(oid.clone(), new_sz);

                // Update sum via delta: sum = sum - old + new
                let mut val = level.sum_sz.value();
                val = val.saturating_sub(old_sz.value());
                val = val.saturating_add(new_sz.value());
                level.sum_sz = Sz::new(val);

                if let Some(level) = levels.get_mut(&px) {
                    level.dirty = true;
                }
                self.dirty_levels.push((side, px));
            }
        }
    }

    fn remove_order(&mut self, oid: Oid) -> bool {
        let mut removed_level = false;
        if let Some(info) = self.oid_index.remove(&oid) {
            let side = info.side;
            let px = info.px;
            let sz = info.sz;

            let levels = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };

            if let Some(level) = levels.get_mut(&px) {
                level.orders.remove(&oid);

                let mut val = level.sum_sz.value();
                val = val.saturating_sub(sz.value());
                level.sum_sz = Sz::new(val);

                if level.orders.is_empty() {
                    levels.remove(&px);
                    removed_level = true;
                } else if let Some(level) = levels.get_mut(&px) {
                    level.dirty = true;
                }
                self.dirty_levels.push((side, px));
            }
        }
        removed_level
    }

    /// Compute hash of entire order book state
    /// Hash includes all bid/ask prices and quantities for quick consistency check
    #[allow(dead_code)]
    fn compute_snapshot_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash bids in descending order (BTreeMap::rev) and asks in ascending order
        for (px, level) in self.bids.iter().rev() {
            px.value().hash(&mut hasher);
            level.sum_sz.value().hash(&mut hasher);
        }
        for (px, level) in &self.asks {
            px.value().hash(&mut hasher);
            level.sum_sz.value().hash(&mut hasher);
        }

        hasher.finish()
    }

    #[allow(dead_code)]
    /// Validate L2 snapshot against L4 order snapshot by comparing hashes
    /// Returns false on mismatch (warns with details).
    /// Aggregates all L4 orders by (side, price) and compares hash with current L2 state
    pub(crate) fn validate_snapshot(
        &self,
        l4_snapshot: &Snapshot<InnerL4Order>,
        coin: &Coin,
        block_height: u64,
    ) -> bool {
        use log::warn;

        // Only validate main coins
        const MAIN_COINS: [&str; 2] = ["BTC", "ETH"];
        if !MAIN_COINS.contains(&coin.value().as_str()) {
            return true;
        }

        // Build expected L2 state from L4 orders (use BTreeMap for deterministic order)
        let mut expected_bids: BTreeMap<Px, Sz> = BTreeMap::new();
        let mut expected_asks: BTreeMap<Px, Sz> = BTreeMap::new();

        for orders_by_side in l4_snapshot.as_ref() {
            for order in orders_by_side {
                let px = order.limit_px();
                let sz = order.sz();

                match order.side() {
                    Side::Bid => {
                        expected_bids.entry(px).or_insert_with(|| Sz::new(0));
                        if let Some(acc) = expected_bids.get_mut(&px) {
                            *acc = Sz::new(acc.value().saturating_add(sz.value()));
                        }
                    }
                    Side::Ask => {
                        expected_asks.entry(px).or_insert_with(|| Sz::new(0));
                        if let Some(acc) = expected_asks.get_mut(&px) {
                            *acc = Sz::new(acc.value().saturating_add(sz.value()));
                        }
                    }
                }
            }
        }

        // Compute hash of expected state (BTreeMap iteration is deterministic)
        let mut expected_hasher = DefaultHasher::new();
        // bids should be hashed in descending order to match L4Book convention
        for (px, sz) in expected_bids.iter().rev() {
            px.value().hash(&mut expected_hasher);
            sz.value().hash(&mut expected_hasher);
        }
        // asks hashed ascending
        for (px, sz) in &expected_asks {
            px.value().hash(&mut expected_hasher);
            sz.value().hash(&mut expected_hasher);
        }
        let expected_hash = expected_hasher.finish();

        // Compare hashes
        let actual_hash = self.compute_snapshot_hash();

        if actual_hash != expected_hash {
            // Log totals for easier debugging
            let expected_bids_total: u64 = expected_bids.values().map(|s| s.value()).sum();
            let expected_asks_total: u64 = expected_asks.values().map(|s| s.value()).sum();
            let actual_bids_total: u64 = self.bids.values().map(|lvl| lvl.sum_sz.value()).sum();
            let actual_asks_total: u64 = self.asks.values().map(|lvl| lvl.sum_sz.value()).sum();

            warn!(
                "L4Lite snapshot hash mismatch for {} at height {}: expected hash={}, actual hash={}; totals bids expected={}, actual={}, asks expected={}, actual={}",
                coin.value(),
                block_height,
                expected_hash,
                actual_hash,
                fmt_sz(expected_bids_total),
                fmt_sz(actual_bids_total),
                fmt_sz(expected_asks_total),
                fmt_sz(actual_asks_total)
            );
            return false;
        }
        true
    }
}

pub(crate) fn expected_snapshot_hash(l4_snapshot: &Snapshot<InnerL4Order>) -> u64 {
    let mut expected_bids: BTreeMap<Px, Sz> = BTreeMap::new();
    let mut expected_asks: BTreeMap<Px, Sz> = BTreeMap::new();

    for orders_by_side in l4_snapshot.as_ref() {
        for order in orders_by_side {
            let px = order.limit_px();
            let sz = order.sz();
            match order.side() {
                Side::Bid => {
                    expected_bids.entry(px).or_insert_with(|| Sz::new(0));
                    if let Some(acc) = expected_bids.get_mut(&px) {
                        *acc = Sz::new(acc.value().saturating_add(sz.value()));
                    }
                }
                Side::Ask => {
                    expected_asks.entry(px).or_insert_with(|| Sz::new(0));
                    if let Some(acc) = expected_asks.get_mut(&px) {
                        *acc = Sz::new(acc.value().saturating_add(sz.value()));
                    }
                }
            }
        }
    }

    let mut expected_hasher = DefaultHasher::new();
    for (px, sz) in expected_bids.iter().rev() {
        px.value().hash(&mut expected_hasher);
        sz.value().hash(&mut expected_hasher);
    }
    for (px, sz) in &expected_asks {
        px.value().hash(&mut expected_hasher);
        sz.value().hash(&mut expected_hasher);
    }
    expected_hasher.finish()
}
