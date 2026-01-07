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
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let _unused = dir;
    let output_path = PathBuf::from("/dev/shm/snapshot.json");
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

    let client = Client::new();
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(output_path)
}

fn hash_l4_order(order: &InnerL4Order, hasher: &mut DefaultHasher) {
    order.user.hash(hasher);
    order.coin.hash(hasher);
    order.side.hash(hasher);
    order.limit_px.value().hash(hasher);
    order.sz.value().hash(hasher);
    order.oid.hash(hasher);
    order.timestamp.hash(hasher);
    order.trigger_condition.hash(hasher);
    order.is_trigger.hash(hasher);
    order.trigger_px.hash(hasher);
    order.is_position_tpsl.hash(hasher);
    order.reduce_only.hash(hasher);
    order.order_type.hash(hasher);
    order.tif.hash(hasher);
    order.cloid.hash(hasher);
}

pub(super) fn validate_snapshot_hash(
    snapshot: &Snapshots<InnerL4Order>,
    expected: &Snapshots<InnerL4Order>,
    ignore_spot: bool,
) -> Result<()> {
    let mut hasher_snapshot = DefaultHasher::new();
    let mut hasher_expected = DefaultHasher::new();

    let mut coins: Vec<_> = snapshot.as_ref().iter().collect();
    coins.sort_by_key(|(coin, _)| coin.value());

    for (coin, book) in coins {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = expected.as_ref().get(coin) {
            coin.value().hash(&mut hasher_snapshot);
            coin.value().hash(&mut hasher_expected);

            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                if orders1.len() != orders2.len() {
                    return Err(format!(
                        "Order count mismatch for {}: {} vs {}",
                        coin.value(),
                        orders1.len(),
                        orders2.len()
                    ).into());
                }
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    hash_l4_order(order1, &mut hasher_snapshot);
                    hash_l4_order(order2, &mut hasher_expected);
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book in expected snapshot", coin.value()).into());
        }
    }

    let hash_snapshot = hasher_snapshot.finish();
    let hash_expected = hasher_expected.finish();
    if hash_snapshot != hash_expected {
        return Err(format!(
            "Snapshot hash mismatch: expected={}, actual={}",
            hash_expected, hash_snapshot
        ).into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

#[allow(dead_code)]
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
