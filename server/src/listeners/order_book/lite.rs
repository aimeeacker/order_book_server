use crate::order_book::types::{Coin, Oid, Px, Side, Sz};
use crate::order_book::{InnerOrder, Snapshot};
use crate::prelude::Result;
use crate::types::inner::{InnerL4Order, InnerOrderDiff};
use crate::types::node_data::{NodeDataOrderDiff, NodeDataOrderStatus, NodeDataFill};
use serde::ser::{Serializer, SerializeTuple};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const PRICE_SCALE: u64 = 100_000_000;
const BTC_BUCKET_SIZE: u64 = 50 * PRICE_SCALE;
const ETH_BUCKET_SIZE: u64 = 5 * PRICE_SCALE;
const ANALYSIS_ROLLUP_BLOCKS: u64 = 10;
const ANALYSIS_ROLLUP_WINDOWS: usize = 180;

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
    if v == 0 {
        "0".to_string()
    } else {
        format!("{:.5}", v as f64 / 1e8)
    }
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
    if neg {
        format!("-{}.{:02}", int_part, frac)
    } else {
        format!("+{}.{:02}", int_part, frac)
    }
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
        entry.change += agg.change;
    }
}

fn sum_bucket_map(buckets: &HashMap<u64, BucketAgg>) -> (u64, u128, i64, i128) {
    let mut fill = 0u64;
    let mut fill_notional = 0u128;
    let mut change = 0i64;
    let mut change_notional = 0i128;
    for (px, agg) in buckets {
        fill = fill.saturating_add(agg.fill);
        change = change.saturating_add(agg.change);
        let px_u128 = u128::from(*px);
        fill_notional = fill_notional.saturating_add((px_u128 * u128::from(agg.fill)) / u128::from(PRICE_SCALE));
        let change_part = (i128::from(*px) * i128::from(agg.change)) / i128::from(PRICE_SCALE);
        change_notional = change_notional.saturating_add(change_part);
    }
    (fill, fill_notional, change, change_notional)
}

fn format_sum_values(fill: u64, fill_notional: u128, change: i64, change_notional: i128) -> [String; 4] {
    [
        fmt_sz(fill),
        fmt_notional(fill_notional),
        fmt_sz_signed(change),
        fmt_notional_signed(change_notional),
    ]
}

fn bucket_map_to_updates(coin: &Coin, side: Side, buckets: &HashMap<u64, BucketAgg>) -> Vec<AnalysisUpdate> {
    let mut levels: Vec<(&u64, &BucketAgg)> = buckets.iter().collect();
    levels.sort_by_key(|(px, _)| *px);
    if side == Side::Bid {
        levels.reverse();
    }

    let mut updates = Vec::new();
    for (px, agg) in levels {
        if agg.fill == 0 && agg.change == 0 {
            continue;
        }
        let fill_notional = (u128::from(*px) * u128::from(agg.fill)) / u128::from(PRICE_SCALE);
        let change_notional = (i128::from(*px) * i128::from(agg.change)) / i128::from(PRICE_SCALE);
        updates.push(AnalysisUpdate {
            coin: coin.clone(),
            side,
            px: fmt_px(*px),
            fill: fmt_sz(agg.fill),
            fill_notional: fmt_notional(fill_notional),
            change: fmt_sz_signed(agg.change),
            change_notional: fmt_notional_signed(change_notional),
        });
    }
    updates
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct L4LiteBlockEvent {
    pub depth_updates: Option<L2BlockUpdate>,
    pub analysis_updates_b: Vec<AnalysisUpdate>,
    pub analysis_updates_a: Vec<AnalysisUpdate>,
    pub analysis_rollup_b: Vec<AnalysisUpdate>,
    pub analysis_rollup_a: Vec<AnalysisUpdate>,
    pub analysis_rollup_sum_b: Option<[String; 4]>,
    pub analysis_rollup_sum_a: Option<[String; 4]>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LevelState {
    pub sum_sz: Sz,        // Current aggregated quantity (real-time update)
    pub shadow_sum: Sz,    // Last sum sent to client
    pub orders: HashMap<Oid, Sz>, // Optional: for debugging/auditing
    pub dirty: bool,
}

impl LevelState {
    pub(crate) fn new() -> Self {
        Self {
            sum_sz: Sz::new(0),
            shadow_sum: Sz::new(0),
            orders: HashMap::new(),
            dirty: false,
        }
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
    analysis_rollups: VecDeque<BucketWindow>,
    initialized: bool,
}

#[derive(Debug, Clone, Default)]
struct BucketAgg {
    fill: u64,
    change: i64,
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
            analysis_rollups: VecDeque::new(),
            initialized: false,
        }
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub(crate) fn snapshot_hash(&self) -> u64 {
        self.compute_snapshot_hash()
    }

    pub(crate) fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.oid_index.clear();
        self.dirty_levels.clear();
        self.last_block_height = 0;
        self.analysis_window_b.clear();
        self.analysis_window_a.clear();
        self.initialized = false;
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

    pub(crate) fn process_block(
        &mut self,
        coin: Coin,
        statuses: &Vec<NodeDataOrderStatus>, 
        diffs: &Vec<NodeDataOrderDiff>, 
        fills: &Vec<NodeDataFill>,
        block_height: u64,
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
            });
        }
        if block_height != self.last_block_height + 1 {
            return Err(format!(
                "Expecting block {}, got {}",
                self.last_block_height + 1,
                block_height
            ).into());
        }

        // 1. Identify all affected levels (sides/px)
        let mut affected_levels: HashSet<(Side, Px)> = HashSet::new();
        
        // Aggregate fills for this block: (Side, Px) -> Sz
        let mut filled_amts: HashMap<(Side, Px), u64> = HashMap::new();
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
        
        log::debug!("[{:?}] Block {}: {} statuses -> {} in order_map, {} diffs to process",
            coin, block_height, statuses.len(), order_map.len(), diffs.len());
        
        // Process Diffs & Calculate Deltas
        let mut level_deltas: HashMap<(Side, Px), i64> = HashMap::new();
        let mut removed_levels: HashSet<(Side, Px)> = HashSet::new();
        for diff in diffs {
            let oid = diff.oid();
            
            // Process diffs matching L4Book logic exactly:
            // - New: must have matching status in this block
            // - Update/Remove: must already exist in oid_index
            let inner_diff: InnerOrderDiff = diff
                .diff()
                .try_into()
                .map_err(|e| format!("Unable to parse diff {diff:?}: {e}"))?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    let status = order_map
                        .remove(&oid)
                        .ok_or_else(|| format!("Missing status for New diff {diff:?}"))?;
                    let mut inner: InnerL4Order = status
                        .clone()
                        .try_into()
                        .map_err(|e| format!("Unable to build order from status {status:?}: {e}"))?;
                    let time = status.time.and_utc().timestamp_millis();
                    inner.modify_sz(sz);
                    #[allow(clippy::unwrap_used)]
                    inner.convert_trigger(time.try_into().unwrap());
                    self.add_order(inner.oid(), inner.coin.clone(), inner.side(), inner.limit_px(), inner.sz());
                    affected_levels.insert((inner.side(), inner.limit_px()));
                    *level_deltas.entry((inner.side(), inner.limit_px())).or_default() += sz.value() as i64;
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    let info = self
                        .oid_index
                        .get(&oid)
                        .ok_or_else(|| format!("Missing order for Update diff {diff:?}"))?;
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
                    let info = self
                        .oid_index
                        .get(&oid)
                        .ok_or_else(|| format!("Missing order for Remove diff {diff:?}"))?;
                    if info.coin != coin {
                        return Err(format!("Coin mismatch for Remove diff {diff:?}").into());
                    }
                    let side = info.side;
                    let px = info.px;
                    let old_sz_val = info.sz.value();
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

        for (side, px) in affected_levels {
             let filled_val = *filled_amts.get(&(side, px)).unwrap_or(&0);
             let delta_total = *level_deltas.get(&(side, px)).unwrap_or(&0);
             
             // Book change comes from diffs; fills reduce book size on the maker side.
             // change = add/cancel component after accounting for fills.
             let change = delta_total + (filled_val as i64);
             
             if filled_val > 0 || change != 0 {
                 let bucket_px = bucket_lower_px(&coin, px.value());
                 let target = match side {
                     Side::Bid => &mut block_buckets_b,
                     Side::Ask => &mut block_buckets_a,
                 };
                 let entry = target.entry(bucket_px).or_default();
                 entry.fill += filled_val;
                 entry.change += change;
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
        
        let depth_updates = Some(L2BlockUpdate {
             coin: coin.value(),
             b: b_updates,
             a: a_updates,
             block_height,
        });

        merge_bucket_maps(&mut self.analysis_window_b, &block_buckets_b);
        merge_bucket_maps(&mut self.analysis_window_a, &block_buckets_a);

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
        }

        let event = L4LiteBlockEvent {
            depth_updates,
            analysis_updates_b: analysis_b,
            analysis_updates_a: analysis_a,
            analysis_rollup_b,
            analysis_rollup_a,
            analysis_rollup_sum_b,
            analysis_rollup_sum_a,
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
        self.initialized = true;

        for orders in snapshot.as_ref() {
            for order in orders {
                self.add_order(order.oid(), order.coin.clone(), order.side(), order.limit_px(), order.sz());
            }
        }
    }

    fn add_order(&mut self, oid: Oid, coin: Coin, side: Side, px: Px, sz: Sz) {
        if let Some(_) = self.oid_index.get(&oid) {
            // Already exists, just update?
            self.update_order_sz(oid, sz);
            return;
        }

        let info = OrderInfo {
            coin,
            side,
            px,
            sz,
        };
        self.oid_index.insert(oid.clone(), info);

        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        let level = levels.entry(px).or_insert_with(LevelState::new);
        level.sum_sz = Sz::new(level.sum_sz.value() + sz.value());
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
    pub(crate) fn validate_snapshot(&self, l4_snapshot: &Snapshot<InnerL4Order>, coin: &Coin, block_height: u64) -> bool {
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
                            *acc = Sz::new(acc.value() + sz.value());
                        }
                    }
                    Side::Ask => {
                        expected_asks.entry(px).or_insert_with(|| Sz::new(0));
                        if let Some(acc) = expected_asks.get_mut(&px) {
                            *acc = Sz::new(acc.value() + sz.value());
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
                        *acc = Sz::new(acc.value() + sz.value());
                    }
                }
                Side::Ask => {
                    expected_asks.entry(px).or_insert_with(|| Sz::new(0));
                    if let Some(acc) = expected_asks.get_mut(&px) {
                        *acc = Sz::new(acc.value() + sz.value());
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
