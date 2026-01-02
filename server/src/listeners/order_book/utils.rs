use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use ahash::AHasher;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::{Client, StatusCode};
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::Path;
use tokio::fs::read;

pub(super) async fn process_rmp_file(_dir: &Path) -> Result<Vec<u8>> {
    let payload = json!({
        "type": "l4Snapshots",
        "includeUsers": true,
        "includeTriggerOrders": false,
        "includeHeightInOutput": true
    });

    let client = Client::new();
    let response = client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?;
    if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
        let output_path = _dir.join("out.json");
        let payload = json!({
            "type": "fileSnapshot",
            "request": {
                "type": "l4Snapshots",
                "includeUsers": true,
                "includeTriggerOrders": false
            },
            "outPath": output_path,
            "includeHeightInOutput": true
        });
        client
            .post("http://localhost:3001/info")
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;
        let bytes = read(_dir.join("out.json")).await?;
        return Ok(bytes);
    }
    let response = response.error_for_status()?;
    let bytes = response.bytes().await?;
    Ok(bytes.to_vec())
}

#[allow(dead_code)]
/// Legacy full snapshot comparison kept for reference/testing.
pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut snapshot_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    if *order1 != *order2 {
                        return Err(
                            format!("Orders do not match, expected: {:?} received: {:?}", *order2, *order1).into()
                        );
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !snapshot_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

pub(super) fn validate_snapshot_consistency_with_hashes(
    snapshot: &Snapshots<InnerL4Order>,
    expected: Snapshots<InnerL4Order>,
    active_symbols: &HashSet<String>,
    ignore_spot: bool,
) -> Result<()> {
    let mut expected_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = expected_map.remove(coin) {
            let coin_value = coin.value();
            if active_symbols.contains(&coin_value) {
                for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                    for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                        if *order1 != *order2 {
                            return Err(format!(
                                "Orders do not match, expected: {:?} received: {:?}",
                                *order2, *order1
                            )
                            .into());
                        }
                    }
                }
            } else {
                let stored_hash = hash_snapshot(book);
                let expected_hash = hash_snapshot(&book2);
                if stored_hash != expected_hash {
                    return Err(format!("Snapshot hash mismatch for {}", coin_value).into());
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !expected_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub(super) fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }
}

fn hash_snapshot(snapshot: &Snapshot<InnerL4Order>) -> u64 {
    let mut hasher = AHasher::default();
    for orders in snapshot.as_ref() {
        for order in orders {
            update_hasher_bytes(&mut hasher, order.user.as_slice());
            update_hasher_bytes(&mut hasher, order.coin.value().as_bytes());
            update_hasher_u8(
                &mut hasher,
                match order.side {
                    crate::order_book::Side::Ask => 0,
                    crate::order_book::Side::Bid => 1,
                },
            );
            update_hasher_u64(&mut hasher, order.limit_px.value());
            update_hasher_u64(&mut hasher, order.sz.value());
            update_hasher_u64(&mut hasher, order.oid);
            update_hasher_u64(&mut hasher, order.timestamp);
            update_hasher_bytes(&mut hasher, order.trigger_condition.as_bytes());
            update_hasher_u8(&mut hasher, u8::from(order.is_trigger));
            update_hasher_bytes(&mut hasher, order.trigger_px.as_bytes());
            update_hasher_u8(&mut hasher, u8::from(order.is_position_tpsl));
            update_hasher_u8(&mut hasher, u8::from(order.reduce_only));
            update_hasher_bytes(&mut hasher, order.order_type.as_bytes());
            if let Some(tif) = &order.tif {
                update_hasher_bytes(&mut hasher, tif.as_bytes());
            } else {
                update_hasher_u8(&mut hasher, 0);
            }
            if let Some(cloid) = &order.cloid {
                update_hasher_bytes(&mut hasher, cloid.as_bytes());
            } else {
                update_hasher_u8(&mut hasher, 0);
            }
        }
    }
    hasher.finish()
}

fn update_hasher_u64(hasher: &mut AHasher, value: u64) {
    hasher.write_u64(value);
    hasher.write_u8(0);
}

fn update_hasher_u8(hasher: &mut AHasher, value: u8) {
    hasher.write_u8(value);
    hasher.write_u8(0);
}

fn update_hasher_bytes(hasher: &mut AHasher, value: &[u8]) {
    value.hash(hasher);
    hasher.write_u8(0);
}
