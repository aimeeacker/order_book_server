//! Binary protocol types for UDS communication between fifo_listener and server.
//!
//! These types are serialized via MessagePack for efficient binary transport.
//! The server crate defines compatible types for deserialization.

use serde::{Deserialize, Serialize};

/// Side of an order (Ask or Bid)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid,
}

/// Order diff type for book updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderDiff {
    New { sz: String },
    Update { orig_sz: String, new_sz: String },
    Remove,
}

/// Fill event in binary protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FillEvent {
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

/// Order status event in binary protocol (non-reject only)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderStatusEvent {
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
pub struct OrderDiffEvent {
    pub user: String,
    pub oid: u64,
    pub px: String,
    pub coin: String,
    pub side: Side,
    pub raw_book_diff: OrderDiff,
}

/// Aligned block payload sent over UDS via MessagePack
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlignedBlockPayload {
    pub block_number: u64,
    pub block_time: String,
    pub local_time: String,
    pub fills: Vec<FillEvent>,
    pub diffs: Vec<OrderDiffEvent>,
    pub statuses: Vec<OrderStatusEvent>,
}

impl AlignedBlockPayload {
    pub fn new(
        block_number: u64,
        block_time: String,
        local_time: String,
        fills: Vec<FillEvent>,
        diffs: Vec<OrderDiffEvent>,
        statuses: Vec<OrderStatusEvent>,
    ) -> Self {
        Self { block_number, block_time, local_time, fills, diffs, statuses }
    }

    /// Encode payload to MessagePack bytes with length prefix
    pub fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        let msgpack_bytes = rmp_serde::to_vec(self)?;
        let len = msgpack_bytes.len() as u32;
        let mut buf = Vec::with_capacity(4 + msgpack_bytes.len());
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&msgpack_bytes);
        Ok(buf)
    }

    /// Decode payload from MessagePack bytes (without length prefix)
    pub fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

/// Reject order event (for separate archive thread only, never sent downstream)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectOrderEvent {
    pub block_number: u64,
    pub block_time: String,
    pub time: String,
    pub user: String,
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
}

/// Parsed block data from FIFO streams (after SIMD JSON parsing)
#[derive(Debug)]
pub struct ParsedBlock {
    pub block_number: u64,
    pub block_time: String,
    pub local_time: String,
    pub fills: Vec<FillEvent>,
    pub diffs: Vec<OrderDiffEvent>,
    pub statuses: Vec<OrderStatusEvent>,
    pub rejects: Vec<RejectOrderEvent>,
}

impl ParsedBlock {
    pub fn from_parts(order: ParsedOrderBatch, diffs: ParsedDiffBatch, fills: ParsedFillBatch) -> Result<Self, String> {
        if order.block_number != diffs.block_number || order.block_number != fills.block_number {
            return Err(format!(
                "height mismatch order={} diff={} fill={}",
                order.block_number, diffs.block_number, fills.block_number
            ));
        }
        Ok(Self {
            block_number: order.block_number,
            block_time: order.block_time,
            local_time: order.local_time,
            fills: fills.fills,
            diffs: diffs.diffs,
            statuses: order.statuses,
            rejects: order.rejects,
        })
    }

    pub fn to_aligned_payload(&self) -> AlignedBlockPayload {
        AlignedBlockPayload::new(
            self.block_number,
            self.block_time.clone(),
            self.local_time.clone(),
            self.fills.clone(),
            self.diffs.clone(),
            self.statuses.clone(),
        )
    }

    /// Split into aligned payload (for UDS) and reject events (for archive)
    pub fn into_parts(self) -> (AlignedBlockPayload, Vec<RejectOrderEvent>) {
        let payload = AlignedBlockPayload::new(
            self.block_number,
            self.block_time,
            self.local_time,
            self.fills,
            self.diffs,
            self.statuses,
        );
        (payload, self.rejects)
    }
}

#[derive(Debug, Clone)]
pub struct ParsedFillBatch {
    pub block_number: u64,
    pub block_time: String,
    pub local_time: String,
    pub fills: Vec<FillEvent>,
}

#[derive(Debug, Clone)]
pub struct ParsedDiffBatch {
    pub block_number: u64,
    pub block_time: String,
    pub local_time: String,
    pub diffs: Vec<OrderDiffEvent>,
}

#[derive(Debug, Clone)]
pub struct ParsedOrderBatch {
    pub block_number: u64,
    pub block_time: String,
    pub local_time: String,
    pub statuses: Vec<OrderStatusEvent>,
    pub rejects: Vec<RejectOrderEvent>,
}

// ----- JSON Batch Parsing Types (internal use) -----

#[derive(Debug, Deserialize)]
struct JsonBatch<T> {
    local_time: String,
    block_time: String,
    block_number: u64,
    events: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct JsonFillTuple(String, JsonFill);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonFill {
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
    start_position: String,
    dir: String,
    closed_pnl: String,
    hash: String,
    oid: u64,
    crossed: bool,
    fee: String,
    tid: u64,
    fee_token: String,
    #[serde(default)]
    twap_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonOrderStatus {
    time: String,
    user: Option<String>,
    #[serde(default)]
    hash: serde_json::Value,
    #[serde(default)]
    builder: serde_json::Value,
    status: String,
    order: JsonOrder,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonOrder {
    coin: String,
    side: String,
    limit_px: String,
    sz: String,
    oid: u64,
    timestamp: u64,
    trigger_condition: String,
    is_trigger: bool,
    trigger_px: String,
    is_position_tpsl: bool,
    reduce_only: bool,
    order_type: String,
    orig_sz: String,
    tif: Option<String>,
    cloid: Option<serde_json::Value>,
    #[serde(default)]
    children: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct JsonDiff {
    user: String,
    oid: u64,
    px: String,
    coin: String,
    side: String,
    #[serde(alias = "raw_book_diff", alias = "rawBookDiff")]
    raw_book_diff: JsonRawDiff,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum JsonRawDiff {
    #[serde(alias = "New")]
    New { sz: String },
    #[serde(alias = "Update")]
    Update {
        #[serde(alias = "origSz")]
        orig_sz: String,
        #[serde(alias = "newSz")]
        new_sz: String,
    },
    #[serde(alias = "Remove")]
    Remove,
}

fn parse_side(s: &str) -> Side {
    match s {
        "A" => Side::Ask,
        _ => Side::Bid,
    }
}

fn cloid_to_option(v: Option<serde_json::Value>) -> Option<String> {
    match v {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

fn flatten_json_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn extract_child_trigger_pxs(children: &serde_json::Value) -> (Option<String>, Option<String>) {
    let Some(items) = children.as_array() else {
        return (None, None);
    };
    let mut tp_trigger_px = None;
    let mut sl_trigger_px = None;
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let trigger_px = obj.get("triggerPx").map(flatten_json_value).filter(|value| !value.is_empty());
        let Some(trigger_px) = trigger_px else {
            continue;
        };
        match obj.get("isTp").and_then(serde_json::Value::as_bool) {
            Some(true) => tp_trigger_px = Some(trigger_px),
            Some(false) => sl_trigger_px = Some(trigger_px),
            None => {}
        }
    }
    (tp_trigger_px, sl_trigger_px)
}

/// Parse aligned JSON lines and convert to binary protocol types.
/// This handles the split of rejects from live order statuses.
pub fn parse_aligned_json(fills_line: &[u8], diffs_line: &[u8], order_line: &[u8]) -> Result<ParsedBlock, String> {
    let fills_batch = parse_fills_json(fills_line)?;
    let diffs_batch = parse_diffs_json(diffs_line)?;
    let order_batch = parse_order_json(order_line)?;
    ParsedBlock::from_parts(order_batch, diffs_batch, fills_batch)
}

pub fn parse_fills_json(fills_line: &[u8]) -> Result<ParsedFillBatch, String> {
    let mut fills_owned = fills_line.to_vec();
    let fills_batch: JsonBatch<JsonFillTuple> =
        simd_json::from_slice(&mut fills_owned).map_err(|e| format!("fills parse error: {e}"))?;
    let fills: Vec<FillEvent> = fills_batch
        .events
        .into_iter()
        .map(|tuple| {
            let user = tuple.0;
            let f = tuple.1;
            FillEvent {
                user,
                coin: f.coin,
                px: f.px,
                sz: f.sz,
                side: parse_side(&f.side),
                time: f.time,
                start_position: f.start_position,
                dir: f.dir,
                closed_pnl: f.closed_pnl,
                hash: f.hash,
                oid: f.oid,
                crossed: f.crossed,
                fee: f.fee,
                tid: f.tid,
                fee_token: f.fee_token,
                twap_id: f.twap_id,
            }
        })
        .collect();
    Ok(ParsedFillBatch {
        block_number: fills_batch.block_number,
        block_time: fills_batch.block_time,
        local_time: fills_batch.local_time,
        fills,
    })
}

pub fn parse_diffs_json(diffs_line: &[u8]) -> Result<ParsedDiffBatch, String> {
    let mut diffs_owned = diffs_line.to_vec();
    let diffs_batch: JsonBatch<JsonDiff> =
        simd_json::from_slice(&mut diffs_owned).map_err(|e| format!("diffs parse error: {e}"))?;
    let diffs: Vec<OrderDiffEvent> = diffs_batch
        .events
        .into_iter()
        .map(|d| OrderDiffEvent {
            user: d.user,
            oid: d.oid,
            px: d.px,
            coin: d.coin,
            side: parse_side(&d.side),
            raw_book_diff: match d.raw_book_diff {
                JsonRawDiff::New { sz } => OrderDiff::New { sz },
                JsonRawDiff::Update { orig_sz, new_sz } => OrderDiff::Update { orig_sz, new_sz },
                JsonRawDiff::Remove => OrderDiff::Remove,
            },
        })
        .collect();
    Ok(ParsedDiffBatch {
        block_number: diffs_batch.block_number,
        block_time: diffs_batch.block_time,
        local_time: diffs_batch.local_time,
        diffs,
    })
}

pub fn parse_order_json(order_line: &[u8]) -> Result<ParsedOrderBatch, String> {
    let mut order_owned = order_line.to_vec();
    let order_batch: JsonBatch<JsonOrderStatus> =
        simd_json::from_slice(&mut order_owned).map_err(|e| format!("order parse error: {e}"))?;

    let block_number = order_batch.block_number;
    let block_time = order_batch.block_time.clone();
    let local_time = order_batch.local_time.clone();
    let mut statuses = Vec::new();
    let mut rejects = Vec::new();

    for status in order_batch.events {
        let is_reject = status.status.ends_with("Rejected");
        let user = status.user.unwrap_or_default();
        let o = status.order;
        let cloid = cloid_to_option(o.cloid.clone());
        let (tp_trigger_px, sl_trigger_px) = extract_child_trigger_pxs(&o.children);
        let hash = flatten_json_value(&status.hash);
        let builder = flatten_json_value(&status.builder);

        if is_reject {
            rejects.push(RejectOrderEvent {
                block_number,
                block_time: block_time.clone(),
                time: status.time,
                user,
                status: status.status,
                coin: o.coin,
                side: parse_side(&o.side),
                limit_px: o.limit_px,
                sz: o.sz,
                oid: o.oid,
                timestamp: o.timestamp,
                trigger_condition: o.trigger_condition,
                is_trigger: o.is_trigger,
                trigger_px: o.trigger_px,
                is_position_tpsl: o.is_position_tpsl,
                reduce_only: o.reduce_only,
                order_type: o.order_type,
                orig_sz: o.orig_sz,
                tif: o.tif,
                cloid,
            });
        } else {
            statuses.push(OrderStatusEvent {
                time: status.time,
                user,
                hash,
                builder,
                status: status.status,
                coin: o.coin,
                side: parse_side(&o.side),
                limit_px: o.limit_px,
                sz: o.sz,
                oid: o.oid,
                timestamp: o.timestamp,
                trigger_condition: o.trigger_condition,
                is_trigger: o.is_trigger,
                trigger_px: o.trigger_px,
                is_position_tpsl: o.is_position_tpsl,
                reduce_only: o.reduce_only,
                order_type: o.order_type,
                orig_sz: o.orig_sz,
                tif: o.tif,
                cloid,
                tp_trigger_px,
                sl_trigger_px,
            });
        }
    }

    Ok(ParsedOrderBatch { block_number, block_time, local_time, statuses, rejects })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_block_payload_roundtrip() {
        let payload = AlignedBlockPayload {
            block_number: 12345,
            block_time: "2025-03-24T01:00:00.000Z".to_string(),
            local_time: "2025-03-24T01:00:00.001Z".to_string(),
            fills: vec![FillEvent {
                user: "0x1234".to_string(),
                coin: "BTC".to_string(),
                px: "50000.0".to_string(),
                sz: "0.1".to_string(),
                side: Side::Bid,
                time: 1711234567890,
                start_position: "0".to_string(),
                dir: "Open Long".to_string(),
                closed_pnl: "0".to_string(),
                hash: "0xabc".to_string(),
                oid: 100,
                crossed: true,
                fee: "0.5".to_string(),
                tid: 200,
                fee_token: "USDC".to_string(),
                twap_id: None,
            }],
            diffs: vec![OrderDiffEvent {
                user: "0x5678".to_string(),
                oid: 101,
                px: "50001.0".to_string(),
                coin: "BTC".to_string(),
                side: Side::Ask,
                raw_book_diff: OrderDiff::New { sz: "0.5".to_string() },
            }],
            statuses: vec![OrderStatusEvent {
                time: "2025-03-24T01:00:00.000Z".to_string(),
                user: "0x5678".to_string(),
                hash: String::new(),
                builder: String::new(),
                status: "open".to_string(),
                coin: "BTC".to_string(),
                side: Side::Ask,
                limit_px: "50001.0".to_string(),
                sz: "0.5".to_string(),
                oid: 101,
                timestamp: 1711234567890,
                trigger_condition: "N/A".to_string(),
                is_trigger: false,
                trigger_px: "0".to_string(),
                is_position_tpsl: false,
                reduce_only: false,
                order_type: "Limit".to_string(),
                orig_sz: "0.5".to_string(),
                tif: Some("Gtc".to_string()),
                cloid: None,
                tp_trigger_px: None,
                sl_trigger_px: None,
            }],
        };

        let encoded = payload.encode().expect("encode failed");
        assert!(encoded.len() > 4);

        // Decode (skip 4-byte length prefix)
        let decoded = AlignedBlockPayload::decode(&encoded[4..]).expect("decode failed");
        assert_eq!(decoded.block_number, 12345);
        assert_eq!(decoded.fills.len(), 1);
        assert_eq!(decoded.diffs.len(), 1);
        assert_eq!(decoded.statuses.len(), 1);
    }
}
