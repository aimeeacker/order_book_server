//! Binary protocol types for UDS communication from fifo_listener.
//!
//! These types are deserialized from MessagePack for efficient binary transport.
//! Compatible with fifo_listener/src/protocol.rs types.

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::order_book::types::Side;

/// Order diff type for book updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BinaryOrderDiff {
    New { sz: String },
    Update { orig_sz: String, new_sz: String },
    Remove,
}

/// Fill event in binary protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BinaryFillEvent {
    pub user: String,
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: Side,
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub twap_id: Option<u64>,
}

/// Order status event in binary protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BinaryOrderStatusEvent {
    pub time: String,
    pub user: String,
    pub hash: String,
    pub builder: String,
    pub status: String,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    pub orig_sz: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
    pub tp_trigger_px: Option<String>,
    pub sl_trigger_px: Option<String>,
}

/// Order diff event in binary protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BinaryOrderDiffEvent {
    pub user: String,
    pub oid: u64,
    pub px: String,
    pub coin: String,
    pub side: Side,
    pub raw_book_diff: BinaryOrderDiff,
}

/// Aligned block payload received over UDS via MessagePack
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AlignedBlockPayload {
    pub block_number: u64,
    pub block_time: String,
    pub local_time: String,
    pub fills: Vec<BinaryFillEvent>,
    pub diffs: Vec<BinaryOrderDiffEvent>,
    pub statuses: Vec<BinaryOrderStatusEvent>,
}

impl AlignedBlockPayload {
    /// Decode payload from MessagePack bytes (without length prefix)
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

// Conversion implementations to existing server types
use crate::types::node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus};
use crate::types::{Fill, L4Order, OrderDiff};
use chrono::NaiveDateTime;

impl BinaryFillEvent {
    /// Convert to NodeDataFill
    fn into_node_data_fill(self) -> Result<NodeDataFill, String> {
        let user: Address = self.user.parse().map_err(|e| format!("invalid user address: {e}"))?;
        let fill = Fill {
            coin: self.coin,
            px: self.px,
            sz: self.sz,
            side: self.side,
            time: self.time,
            start_position: self.start_position,
            dir: self.dir,
            closed_pnl: self.closed_pnl,
            hash: self.hash,
            oid: self.oid,
            crossed: self.crossed,
            fee: self.fee,
            tid: self.tid,
            fee_token: self.fee_token,
            liquidation: None,
        };
        Ok(NodeDataFill(user, fill))
    }
}

impl BinaryOrderStatusEvent {
    /// Convert to NodeDataOrderStatus
    fn into_node_data_status(self) -> Result<NodeDataOrderStatus, String> {
        let time = parse_datetime(&self.time)?;
        let user: Address = self.user.parse().map_err(|e| format!("invalid user address: {e}"))?;
        let order = L4Order {
            user: Some(user),
            coin: self.coin,
            side: self.side,
            limit_px: self.limit_px,
            sz: self.sz,
            oid: self.oid,
            timestamp: self.timestamp,
            trigger_condition: self.trigger_condition,
            is_trigger: self.is_trigger,
            trigger_px: self.trigger_px,
            is_position_tpsl: self.is_position_tpsl,
            reduce_only: self.reduce_only,
            order_type: self.order_type,
            tif: self.tif,
            cloid: self.cloid,
        };
        Ok(NodeDataOrderStatus { time, user, status: self.status, order })
    }
}

impl BinaryOrderDiffEvent {
    /// Convert to NodeDataOrderDiff
    fn into_node_data_diff(self) -> Result<NodeDataOrderDiff, String> {
        let raw_book_diff = match self.raw_book_diff {
            BinaryOrderDiff::New { sz } => OrderDiff::New { sz },
            BinaryOrderDiff::Update { orig_sz, new_sz } => OrderDiff::Update { orig_sz, new_sz },
            BinaryOrderDiff::Remove => OrderDiff::Remove,
        };
        Ok(NodeDataOrderDiff::new(self.user, self.oid, self.px, self.coin, raw_book_diff))
    }
}

impl AlignedBlockPayload {
    /// Convert to batches matching the existing server types
    pub(crate) fn into_batches(
        self,
    ) -> Result<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>, Batch<NodeDataFill>), String> {
        let block_time = parse_datetime(&self.block_time)?;
        let local_time = parse_datetime(&self.local_time)?;

        let mut status_events = Vec::with_capacity(self.statuses.len());
        for status in self.statuses {
            status_events.push(status.into_node_data_status()?);
        }

        let mut diff_events = Vec::with_capacity(self.diffs.len());
        for diff in self.diffs {
            diff_events.push(diff.into_node_data_diff()?);
        }

        let mut fill_events = Vec::with_capacity(self.fills.len());
        for fill in self.fills {
            fill_events.push(fill.into_node_data_fill()?);
        }

        let status_batch = Batch::new(local_time, block_time, self.block_number, status_events);
        let diff_batch = Batch::new(local_time, block_time, self.block_number, diff_events);
        let fill_batch = Batch::new(local_time, block_time, self.block_number, fill_events);

        Ok((status_batch, diff_batch, fill_batch))
    }
}

fn parse_datetime(s: &str) -> Result<NaiveDateTime, String> {
    // Try multiple formats
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
        return Ok(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt);
    }
    Err(format!("invalid datetime format: {s}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_datetime() {
        assert!(parse_datetime("2025-03-24T01:00:00.000Z").is_ok());
        assert!(parse_datetime("2025-03-24T01:00:00.123456").is_ok());
        assert!(parse_datetime("2025-03-24T01:00:00").is_ok());
    }
}
