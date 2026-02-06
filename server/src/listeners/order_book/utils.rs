use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Coin, Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::inner::{InnerL4Order, InnerLevel},
};
use log::{info, warn};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let _unused = dir;
    let output_path = PathBuf::from("/home/aimee/hl_runtime/hl_book/snapshot.json");
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

fn build_order_map<'a>(orders: &'a [InnerL4Order]) -> Result<BTreeMap<u64, &'a InnerL4Order>> {
    let mut by_oid: BTreeMap<u64, &InnerL4Order> = BTreeMap::new();
    for order in orders {
        if by_oid.insert(order.oid, order).is_some() {
            return Err(format!("Duplicate oid {} in snapshot", order.oid).into());
        }
    }
    Ok(by_oid)
}

fn collect_mismatches(
    snapshot: &Snapshots<InnerL4Order>,
    expected: &Snapshots<InnerL4Order>,
    ignore_spot: bool,
) -> BTreeMap<String, Vec<String>> {
    const MAX_MISMATCHES: usize = 10;
    let mut all_mismatches: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let mut coins: BTreeSet<Coin> = BTreeSet::new();
    coins.extend(snapshot.as_ref().keys().cloned());
    coins.extend(expected.as_ref().keys().cloned());

    for coin in coins {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let mut mismatches = Vec::new();
        let mut extra_count = 0usize;
        let mut push_mismatch = |msg: String| {
            if mismatches.len() < MAX_MISMATCHES {
                mismatches.push(msg);
            } else {
                extra_count += 1;
            }
        };

        match (snapshot.as_ref().get(&coin), expected.as_ref().get(&coin)) {
            (None, None) => continue,
            (Some(book1), None) => {
                if !book1.as_ref()[0].is_empty() || !book1.as_ref()[1].is_empty() {
                    push_mismatch(format!("coin {} missing in expected snapshot", coin.value()));
                }
            }
            (None, Some(book2)) => {
                if !book2.as_ref()[0].is_empty() || !book2.as_ref()[1].is_empty() {
                    push_mismatch(format!("coin {} missing in local snapshot", coin.value()));
                }
            }
            (Some(book1), Some(book2)) => {
                for (idx, (orders1, orders2)) in book1.as_ref().iter().zip(book2.as_ref()).enumerate() {
                    let side = if idx == 0 { "bids" } else { "asks" };
                    let map1 = match build_order_map(orders1) {
                        Ok(map) => map,
                        Err(err) => {
                            push_mismatch(format!("coin {} {side}: {err}", coin.value()));
                            continue;
                        }
                    };
                    let map2 = match build_order_map(orders2) {
                        Ok(map) => map,
                        Err(err) => {
                            push_mismatch(format!("coin {} {side}: {err}", coin.value()));
                            continue;
                        }
                    };

                    for oid in map1.keys() {
                        if !map2.contains_key(oid) {
                            push_mismatch(format!(
                                "coin {} {side}: oid {} missing in expected snapshot",
                                coin.value(),
                                oid
                            ));
                        }
                    }
                    for oid in map2.keys() {
                        if !map1.contains_key(oid) {
                            push_mismatch(format!(
                                "coin {} {side}: oid {} missing in local snapshot",
                                coin.value(),
                                oid
                            ));
                        }
                    }

                    for (oid, order1) in &map1 {
                        let Some(order2) = map2.get(oid) else {
                            continue;
                        };
                        let core1 = (order1.side, order1.limit_px.to_str(), order1.sz.to_str());
                        let core2 = (order2.side, order2.limit_px.to_str(), order2.sz.to_str());
                        if core1 != core2 {
                            push_mismatch(format!(
                                "coin {} {side}: oid {} core mismatch local({:?}, px {}, sz {}) expected({:?}, px {}, sz {})",
                                coin.value(),
                                oid,
                                core1.0,
                                core1.1,
                                core1.2,
                                core2.0,
                                core2.1,
                                core2.2
                            ));
                            continue;
                        }

                        let mut diffs = Vec::new();
                        if order1.timestamp != order2.timestamp {
                            diffs.push(format!("timestamp {} vs {}", order1.timestamp, order2.timestamp));
                        }
                        if order1.is_trigger != order2.is_trigger {
                            diffs.push(format!("is_trigger {} vs {}", order1.is_trigger, order2.is_trigger));
                        }
                        if order1.trigger_condition != order2.trigger_condition {
                            diffs.push(format!(
                                "trigger_condition {} vs {}",
                                order1.trigger_condition, order2.trigger_condition
                            ));
                        }
                        if order1.trigger_px != order2.trigger_px {
                            diffs.push(format!("trigger_px {} vs {}", order1.trigger_px, order2.trigger_px));
                        }
                        if order1.is_position_tpsl != order2.is_position_tpsl {
                            diffs.push(format!(
                                "is_position_tpsl {} vs {}",
                                order1.is_position_tpsl, order2.is_position_tpsl
                            ));
                        }
                        if order1.reduce_only != order2.reduce_only {
                            diffs.push(format!("reduce_only {} vs {}", order1.reduce_only, order2.reduce_only));
                        }
                        if order1.order_type != order2.order_type {
                            diffs.push(format!("order_type {} vs {}", order1.order_type, order2.order_type));
                        }
                        if order1.tif != order2.tif {
                            diffs.push(format!("tif {:?} vs {:?}", order1.tif, order2.tif));
                        }
                        if order1.user != order2.user {
                            diffs.push(format!("user {:?} vs {:?}", order1.user, order2.user));
                        }
                        if order1.cloid != order2.cloid {
                            diffs.push(format!("cloid {:?} vs {:?}", order1.cloid, order2.cloid));
                        }
                        if !diffs.is_empty() {
                            push_mismatch(format!(
                                "coin {} {side}: oid {} meta mismatch: {}",
                                coin.value(),
                                oid,
                                diffs.join(", ")
                            ));
                        }
                    }
                }
            }
        }

        if extra_count > 0 && mismatches.len() < MAX_MISMATCHES {
            mismatches.push(format!("... and {} more mismatches", extra_count));
        }
        if !mismatches.is_empty() {
            all_mismatches.insert(coin.value(), mismatches);
        }
    }

    all_mismatches
}

fn log_mismatch_diagnostics(snapshot: &Snapshots<InnerL4Order>, expected: &Snapshots<InnerL4Order>, ignore_spot: bool) {
    let mismatches = collect_mismatches(snapshot, expected, ignore_spot);
    let mut coins: BTreeSet<Coin> = BTreeSet::new();
    coins.extend(snapshot.as_ref().keys().cloned());
    coins.extend(expected.as_ref().keys().cloned());

    if coins.is_empty() {
        warn!("Snapshot mismatch detail: no coins to compare");
        return;
    }
    if mismatches.is_empty() {
        warn!("Snapshot mismatch detail: unable to locate differing order");
    }

    for coin in coins {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let key = coin.value();
        if let Some(items) = mismatches.get(&key) {
            warn!("L4Book snapshot status for {}: FAIL: {}", key, items.join("; "));
        } else if mismatches.is_empty() {
            warn!("L4Book snapshot status for {}: FAIL: unable to locate differing order", key);
        } else {
            info!("L4Book snapshot status for {}: OK", key);
        }
    }
}

fn log_snapshot_success(snapshot: &Snapshots<InnerL4Order>, expected: &Snapshots<InnerL4Order>, ignore_spot: bool) {
    let mut coins: BTreeSet<Coin> = BTreeSet::new();
    coins.extend(snapshot.as_ref().keys().cloned());
    coins.extend(expected.as_ref().keys().cloned());
    for coin in coins {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        info!("L4Book snapshot status for {}: OK", coin.value());
    }
}
pub(super) fn validate_snapshot_hash(
    snapshot: &Snapshots<InnerL4Order>,
    expected: &Snapshots<InnerL4Order>,
    ignore_spot: bool,
) -> Result<()> {
    let mut hasher_snapshot = DefaultHasher::new();
    let mut hasher_expected = DefaultHasher::new();

    fn hash_orders_btree(orders: &[InnerL4Order], hasher: &mut DefaultHasher) -> Result<usize> {
        let by_oid = build_order_map(orders)?;
        for order in by_oid.values() {
            hash_l4_order(order, hasher);
        }
        Ok(by_oid.len())
    }

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
                    log_mismatch_diagnostics(snapshot, expected, ignore_spot);
                    return Err(format!(
                        "Order count mismatch for {}: {} vs {}",
                        coin.value(),
                        orders1.len(),
                        orders2.len()
                    )
                    .into());
                }
                let count1 = hash_orders_btree(orders1, &mut hasher_snapshot)?;
                let count2 = hash_orders_btree(orders2, &mut hasher_expected)?;
                if count1 != count2 {
                    log_mismatch_diagnostics(snapshot, expected, ignore_spot);
                    return Err(format!(
                        "Order count mismatch for {} after sorting: {} vs {}",
                        coin.value(),
                        count1,
                        count2
                    )
                    .into());
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            log_mismatch_diagnostics(snapshot, expected, ignore_spot);
            return Err(format!("Missing {} book in expected snapshot", coin.value()).into());
        }
    }

    let hash_snapshot = hasher_snapshot.finish();
    let hash_expected = hasher_expected.finish();
    if hash_snapshot != hash_expected {
        log_mismatch_diagnostics(snapshot, expected, ignore_spot);
        return Err(format!("Snapshot hash mismatch: expected={}, actual={}", hash_expected, hash_snapshot).into());
    }
    log_snapshot_success(snapshot, expected, ignore_spot);
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
