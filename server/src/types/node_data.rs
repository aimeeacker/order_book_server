#[cfg(test)]
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::{Coin, Oid},
    types::{Fill, L4Order, OrderDiff},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderDiff {
    user: Address,
    oid: u64,
    px: String,
    coin: String,
    pub(crate) raw_book_diff: OrderDiff,
}

impl NodeDataOrderDiff {
    pub(crate) fn diff(&self) -> OrderDiff {
        self.raw_book_diff.clone()
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub(crate) fn coin(&self) -> Coin {
        Coin::new(&self.coin)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataFill(pub Address, pub Fill);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderStatus {
    pub time: NaiveDateTime,
    pub user: Address,
    pub status: String,
    pub order: L4Order,
}

impl NodeDataOrderStatus {
    pub(crate) fn is_inserted_into_book(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger && (self.order.tif != Some("Ioc".to_string())))
            || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Clone, Copy, strum_macros::Display)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
}

impl EventSource {
    #[must_use]
    #[cfg(test)]
    pub(crate) fn event_source_dir(self, dir: &Path) -> PathBuf {
        match self {
            Self::Fills => dir.join("hl/data/node_fills_by_block"),
            Self::OrderStatuses => dir.join("hl/data/node_order_statuses_by_block"),
            Self::OrderDiffs => dir.join("hl/data/node_raw_book_diffs_by_block"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch<E> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    events: Vec<E>,
}

impl<E> Batch<E> {
    #[allow(clippy::unwrap_used)]
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn events(self) -> Vec<E> {
        self.events
    }

    pub(crate) fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&E) -> bool,
    {
        self.events.retain(f);
    }
    
    pub(crate) fn new(block_number: u64, block_time_millis: u64, events: Vec<E>) -> Self {
        let block_time = NaiveDateTime::from_timestamp_millis(block_time_millis as i64).unwrap_or_default();
        Self {
            local_time: chrono::Local::now().naive_local(),
            block_time,
            block_number,
            events
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}
