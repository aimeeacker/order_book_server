use crate::order_book::types::{Coin, Oid, Px, Side, Sz};
use crate::order_book::{InnerOrder, Snapshot};
use crate::types::inner::InnerL4Order;
use crate::types::node_data::{NodeDataOrderDiff, NodeDataOrderStatus, NodeDataFill};
use crate::types::OrderDiff;
use serde::ser::{Serializer, SerializeTuple};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

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

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AnalysisUpdate {
    #[serde(skip, default = "Coin::default")]
    pub coin: Coin, // Captured by map key in response
    #[serde(skip, default = "Side::default")]
    pub side: Side,
    pub px: String,
    pub fill: String,
    pub add: String,
    pub cancel: String,
}

impl Serialize for AnalysisUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_tuple(4)?;
        seq.serialize_element(&self.px)?;
        seq.serialize_element(&self.fill)?;
        seq.serialize_element(&self.add)?;
        seq.serialize_element(&self.cancel)?;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct L4LiteBlockEvent {
    pub depth_updates: Option<L2BlockUpdate>,
    pub analysis_updates_b: Vec<AnalysisUpdate>,
    pub analysis_updates_a: Vec<AnalysisUpdate>,
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
}

impl BookState {
    pub(crate) fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            oid_index: HashMap::new(),
            dirty_levels: Vec::new(),
            last_block_height: 0,
        }
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
    ) -> L4LiteBlockEvent {
        // 0. Update time/block
        self.last_block_height = block_height;

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
            // Fills from node use float strings for px/sz.
            let side = raw_fill.side;
            let px_float = raw_fill.px.parse::<f64>().unwrap_or(0.0);
            let px_val = (px_float * 1e8).round() as u64;
            let px = Px::new(px_val);

            let sz_float = raw_fill.sz.parse::<f64>().unwrap_or(0.0);
            let sz_val = (sz_float * 1e8).round() as u64;
            
            *filled_amts.entry((side, px)).or_default() += sz_val;
            affected_levels.insert((side, px));
        }

        // Update Index from Statuses
        for status in statuses {
             let oid = Oid::new(status.order.oid);
             let px_val = status.order.limit_px.parse::<u64>().unwrap_or(0);
             let px = Px::new(px_val);
             let sz_val = status.order.sz.parse::<u64>().unwrap_or(0);
             let sz = Sz::new(sz_val);
             let side = status.order.side;

             if sz.value() > 0 {
                 self.oid_index.insert(oid, OrderInfo { coin: coin.clone(), side, px, sz });
             }
             affected_levels.insert((side, px));
        }
        
        // Process Diffs & Calculate Deltas
        let mut level_deltas: HashMap<(Side, Px), i64> = HashMap::new();

        for diff in diffs {
             let oid = diff.oid();
             if let Some(info) = self.oid_index.get(&oid) {
                 affected_levels.insert((info.side, info.px));
                 
                 let (old_val, new_val) = match &diff.raw_book_diff {
                     OrderDiff::New { sz } => (0, sz.parse::<u64>().unwrap_or(0)),
                     OrderDiff::Update { orig_sz, new_sz } => (
                         orig_sz.parse::<u64>().unwrap_or(0),
                         new_sz.parse::<u64>().unwrap_or(0)
                     ),
                     OrderDiff::Remove => (info.sz.value(), 0),
                 };

                 let delta = (new_val as i64) - (old_val as i64);
                 *level_deltas.entry((info.side, info.px)).or_default() += delta;
                 
                 // Apply to Book State
                 let state = match info.side {
                     Side::Bid => self.bids.entry(info.px).or_insert_with(LevelState::new),
                     Side::Ask => self.asks.entry(info.px).or_insert_with(LevelState::new),
                 };
                 let new_sum = (state.sum_sz.value() as i64) + delta;
                 state.sum_sz = Sz::new(new_sum.max(0) as u64);
                 state.dirty = true;
             }
        }
        
        // Cleanup Index
        for status in statuses {
             if status.order.sz == "0" {
                 self.oid_index.remove(&Oid::new(status.order.oid));
             }
        }

        // 4. Generate Analysis & Updates
        let mut b_updates = Vec::new();
        let mut a_updates = Vec::new();
        let mut analysis_b = Vec::new();
        let mut analysis_a = Vec::new();

        for (side, px) in affected_levels {
             let filled_val = *filled_amts.get(&(side, px)).unwrap_or(&0);
             let delta_total = *level_deltas.get(&(side, px)).unwrap_or(&0);
             
             let net_activity = delta_total + (filled_val as i64);
             let (add, cancel) = if net_activity > 0 {
                 (net_activity as u64, 0u64)
             } else {
                 (0u64, (-net_activity) as u64)
             };
             
             if filled_val > 0 || add > 0 || cancel > 0 {
                 let update = AnalysisUpdate {
                     coin: coin.clone(),
                     side,
                     px: fmt_px(px.value()),
                     fill: fmt_sz(filled_val),
                     add: fmt_sz(add),
                     cancel: fmt_sz(cancel),
                 };
                 match side {
                     Side::Bid => analysis_b.push(update),
                     Side::Ask => analysis_a.push(update),
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
             }
        }
        
        let depth_updates = Some(L2BlockUpdate {
             coin: coin.value(),
             b: b_updates,
             a: a_updates,
             block_height,
        });

        L4LiteBlockEvent {
            depth_updates,
            analysis_updates_b: analysis_b,
            analysis_updates_a: analysis_a,
        }
    }

    pub(crate) fn init_from_snapshot(&mut self, snapshot: &Snapshot<InnerL4Order>, block_height: u64) {
        // Clear existing state
        self.bids.clear();
        self.asks.clear();
        self.oid_index.clear();
        self.dirty_levels.clear();
        self.last_block_height = block_height;

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
                // Update map
                level.orders.insert(oid, new_sz);
                
                // Update sum
                // We could recalculate sum from orders, or do delta.
                // Delta:
                // sum = sum - old + new
                let mut val = level.sum_sz.value();
                val = val.saturating_sub(old_sz.value());
                val = val.saturating_add(new_sz.value());
                level.sum_sz = Sz::new(val);
                
                level.dirty = true;
                self.dirty_levels.push((side, px));
            }
        }
    }
}
