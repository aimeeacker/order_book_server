#![allow(unused_crate_dependencies)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::NaiveDateTime;
use clap::Parser;
use lz4::Decoder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

const BTC_ASSET_ID: u64 = 0;
const ROW_GROUP_SIZE: usize = 10_000;
const DECIMAL_PRECISION: u8 = 18;
const PX_SCALE: i8 = 1;
const SZ_SCALE: i8 = 5;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    replica_cmds_lz4: PathBuf,

    #[arg(long)]
    node_fills_by_block_lz4: PathBuf,

    #[arg(long)]
    output_parquet: PathBuf,

    #[arg(long, default_value_t = 2_000_000)]
    block_time_tolerance_ns: i64,
}

#[derive(Debug, Deserialize)]
struct ReplicaLine {
    abci_block: AbciBlock,
    #[serde(default)]
    resps: ReplicaResponses,
}

#[derive(Debug, Deserialize)]
struct AbciBlock {
    time: String,
    #[serde(default)]
    signed_action_bundles: Vec<(String, SignedActionPayload)>,
}

#[derive(Debug, Default, Deserialize)]
struct SignedActionPayload {
    #[serde(default)]
    signed_actions: Vec<SignedActionEnvelope>,
}

#[derive(Debug, Default, Deserialize)]
struct SignedActionEnvelope {
    #[serde(default)]
    action: Value,
}

#[derive(Debug, Default, Deserialize)]
struct ReplicaResponses {
    #[serde(rename = "Full", default)]
    full: Vec<(String, Vec<ResponseItem>)>,
}

#[derive(Debug, Deserialize)]
struct ResponseItem {
    user: Option<String>,
    res: ResponseResult,
}

#[derive(Debug, Default, Deserialize)]
struct ResponseResult {
    #[serde(default)]
    response: Value,
}

#[derive(Debug, Deserialize)]
struct NodeFillsBlock {
    block_time: String,
    block_number: u64,
    #[serde(default)]
    events: Vec<(String, FillEvent)>,
}

#[derive(Debug, Default, Deserialize)]
struct FillEvent {
    #[serde(default)]
    coin: String,
    oid: u64,
    #[serde(default)]
    sz: String,
}

#[derive(Debug, Clone)]
struct FillRecord {
    block_number: u64,
    block_time: String,
    oid: u64,
    sz: Decimal,
}

#[derive(Debug, Clone)]
enum OrderRef {
    Oid(u64),
    Cloid(String),
}

#[derive(Debug, Clone)]
struct OrderMeta {
    tif: Option<String>,
    reduce_only: Option<bool>,
    order_type: Option<String>,
    is_trigger: bool,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct BlockIndex {
    ts_ns: i64,
    block_number: u64,
}

#[derive(Debug)]
struct ReplicaBatch {
    block_number: u64,
    block_time_ms: i64,
    events: Vec<CmdEvent>,
}

#[derive(Debug)]
struct NodeFillsBatch {
    block_number: u64,
    block_time_ms: i64,
    fills: Vec<FillRecord>,
}

#[derive(Debug)]
enum CmdEvent {
    Add {
        user: String,
        oid: u64,
        cloid: Option<String>,
        side: String,
        px: Decimal,
        sz: Decimal,
        meta: OrderMeta,
    },
    Cancel {
        order_ref: OrderRef,
    },
    Modify {
        user: String,
        old_order_ref: OrderRef,
        new_oid: Option<u64>,
        new_cloid: Option<String>,
        side: String,
        px: Decimal,
        sz: Decimal,
        meta: OrderMeta,
        placed: bool,
    },
}

#[derive(Debug, Clone)]
struct OrderState {
    user: String,
    cloid: Option<String>,
    created_time_ms: i64,
    side: String,
    px: Decimal,
    remaining_sz: Decimal,
    tif: Option<String>,
    reduce_only: Option<bool>,
    order_type: Option<String>,
    is_trigger: bool,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
}

#[derive(Debug)]
struct OutputRow {
    block_number: i64,
    block_time_ms: i64,
    coin: String,
    user: String,
    oid: i64,
    side: String,
    px: Decimal,
    diff_type: String,
    event: String,
    sz: Decimal,
    orig_sz: Option<Decimal>,
    raw_sz: Option<Decimal>,
    is_trigger: Option<bool>,
    tif: Option<String>,
    reduce_only: Option<bool>,
    order_type: Option<String>,
    trigger_condition: Option<String>,
    trigger_px: Option<Decimal>,
    is_position_tpsl: Option<bool>,
    tp_trigger_px: Option<Decimal>,
    sl_trigger_px: Option<Decimal>,
    status: Option<String>,
    lifetime: Option<i32>,
}

#[derive(Default)]
struct RowBuffer {
    rows: Vec<OutputRow>,
}

impl RowBuffer {
    fn push(&mut self, row: OutputRow) {
        self.rows.push(row);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn take_batch(&mut self, schema: &Arc<Schema>) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let mut block_number = Int64Builder::with_capacity(self.rows.len());
        let mut block_time = TimestampMillisecondBuilder::with_capacity(self.rows.len())
            .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())));
        let mut coin = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 8);
        let mut user = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 64);
        let mut oid = Int64Builder::with_capacity(self.rows.len());
        let mut side = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 4);
        let mut px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut diff_type = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut event = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE));
        let mut orig_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE));
        let mut raw_sz = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE));
        let mut is_trigger = BooleanBuilder::with_capacity(self.rows.len());
        let mut tif = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 8);
        let mut reduce_only = BooleanBuilder::with_capacity(self.rows.len());
        let mut order_type = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 16);
        let mut trigger_condition = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 24);
        let mut trigger_px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut is_position_tpsl = BooleanBuilder::with_capacity(self.rows.len());
        let mut tp_trigger_px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut sl_trigger_px = Decimal128Builder::with_capacity(self.rows.len())
            .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE));
        let mut status = StringBuilder::with_capacity(self.rows.len(), self.rows.len() * 24);
        let mut lifetime = Int32Builder::with_capacity(self.rows.len());

        for row in self.rows.drain(..) {
            block_number.append_value(row.block_number);
            block_time.append_value(row.block_time_ms);
            coin.append_value(&row.coin);
            user.append_value(&row.user);
            oid.append_value(row.oid);
            side.append_value(&row.side);
            px.append_value(decimal_to_i128(row.px, PX_SCALE));
            diff_type.append_value(&row.diff_type);
            event.append_value(&row.event);
            sz.append_value(decimal_to_i128(row.sz, SZ_SCALE));
            match row.orig_sz {
                Some(value) => orig_sz.append_value(decimal_to_i128(value, SZ_SCALE)),
                None => orig_sz.append_null(),
            }
            match row.raw_sz {
                Some(value) => raw_sz.append_value(decimal_to_i128(value, SZ_SCALE)),
                None => raw_sz.append_null(),
            }
            match row.is_trigger {
                Some(value) => is_trigger.append_value(value),
                None => is_trigger.append_null(),
            }
            match row.tif {
                Some(value) => tif.append_value(value),
                None => tif.append_null(),
            }
            match row.reduce_only {
                Some(value) => reduce_only.append_value(value),
                None => reduce_only.append_null(),
            }
            match row.order_type {
                Some(value) => order_type.append_value(value),
                None => order_type.append_null(),
            }
            match row.trigger_condition {
                Some(value) => trigger_condition.append_value(value),
                None => trigger_condition.append_null(),
            }
            match row.trigger_px {
                Some(value) => trigger_px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => trigger_px.append_null(),
            }
            match row.is_position_tpsl {
                Some(value) => is_position_tpsl.append_value(value),
                None => is_position_tpsl.append_null(),
            }
            match row.tp_trigger_px {
                Some(value) => tp_trigger_px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => tp_trigger_px.append_null(),
            }
            match row.sl_trigger_px {
                Some(value) => sl_trigger_px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => sl_trigger_px.append_null(),
            }
            match row.status {
                Some(value) => status.append_value(value),
                None => status.append_null(),
            }
            match row.lifetime {
                Some(value) => lifetime.append_value(value),
                None => lifetime.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(block_number.finish()),
            Arc::new(block_time.finish()),
            Arc::new(coin.finish()),
            Arc::new(user.finish()),
            Arc::new(oid.finish()),
            Arc::new(side.finish()),
            Arc::new(px.finish()),
            Arc::new(diff_type.finish()),
            Arc::new(event.finish()),
            Arc::new(sz.finish()),
            Arc::new(orig_sz.finish()),
            Arc::new(raw_sz.finish()),
            Arc::new(is_trigger.finish()),
            Arc::new(tif.finish()),
            Arc::new(reduce_only.finish()),
            Arc::new(order_type.finish()),
            Arc::new(trigger_condition.finish()),
            Arc::new(trigger_px.finish()),
            Arc::new(is_position_tpsl.finish()),
            Arc::new(tp_trigger_px.finish()),
            Arc::new(sl_trigger_px.finish()),
            Arc::new(status.finish()),
            Arc::new(lifetime.finish()),
        ];

        Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
    }
}

fn parse_timestamp_ns(raw: &str) -> Result<i64> {
    let dt = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
        .with_context(|| format!("failed to parse timestamp: {raw}"))?;
    dt.and_utc().timestamp_nanos_opt().ok_or_else(|| anyhow!("timestamp overflow for {raw}"))
}

fn parse_timestamp_ms(raw: &str) -> Result<i64> {
    let dt = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
        .with_context(|| format!("failed to parse timestamp: {raw}"))?;
    Ok(dt.and_utc().timestamp_millis())
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    raw.parse::<Decimal>().with_context(|| format!("failed to parse decimal: {raw}"))
}

fn nearest_block_number(indices: &[BlockIndex], ts_ns: i64, tolerance_ns: i64) -> Option<u64> {
    let idx = indices.partition_point(|item| item.ts_ns < ts_ns);
    let mut best: Option<(i64, u64)> = None;

    for candidate_idx in [idx.checked_sub(1), Some(idx)].into_iter().flatten() {
        let candidate = indices.get(candidate_idx)?;
        let delta = (candidate.ts_ns - ts_ns).abs();
        if delta <= tolerance_ns {
            match best {
                Some((best_delta, _)) if best_delta <= delta => {}
                _ => best = Some((delta, candidate.block_number)),
            }
        }
    }

    best.map(|(_, block_number)| block_number)
}

fn get_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        let v = value.get(*key)?;
        if let Some(parsed) = v.as_u64() {
            return Some(parsed);
        }
        if let Some(raw) = v.as_str()
            && let Ok(parsed) = raw.parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

fn get_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(raw) = value.get(*key).and_then(Value::as_str) {
            return Some(raw.to_owned());
        }
    }
    None
}

fn get_order_ref(value: &Value, keys: &[&str]) -> Option<OrderRef> {
    for key in keys {
        let v = value.get(*key)?;
        if let Some(parsed) = v.as_u64() {
            return Some(OrderRef::Oid(parsed));
        }
        if let Some(raw) = v.as_str() {
            if let Ok(parsed) = raw.parse::<u64>() {
                return Some(OrderRef::Oid(parsed));
            }
            return Some(OrderRef::Cloid(raw.to_owned()));
        }
    }
    None
}

fn parse_order_fields(order: &Value) -> Option<(u64, String, Decimal, Decimal, Option<String>, OrderMeta)> {
    let asset = get_u64(order, &["a", "asset"])?;
    let is_bid = order.get("b")?.as_bool()?;
    let side = if is_bid { "B" } else { "A" }.to_owned();
    let px_raw = get_string(order, &["p", "price"])?;
    let px = parse_decimal(&px_raw).ok()?;
    let sz_raw = get_string(order, &["s", "size"])?;
    let sz = parse_decimal(&sz_raw).ok()?;
    let trigger = order.get("t").and_then(|t| t.get("trigger"));
    let cloid = get_string(order, &["c", "cloid"]);
    let reduce_only = order.get("r").and_then(Value::as_bool);

    let meta = if let Some(trigger) = trigger {
        let trigger_px = get_string(trigger, &["triggerPx"]).and_then(|raw| parse_decimal(&raw).ok())?;
        let is_market = trigger.get("isMarket").and_then(Value::as_bool).unwrap_or(false);
        let tpsl = trigger.get("tpsl").and_then(Value::as_str);
        OrderMeta {
            tif: None,
            reduce_only,
            order_type: Some(if is_market { "Stop Market".to_owned() } else { "Stop Limit".to_owned() }),
            is_trigger: true,
            trigger_condition: None,
            trigger_px: Some(trigger_px),
            is_position_tpsl: None,
            tp_trigger_px: if tpsl == Some("tp") { Some(trigger_px) } else { None },
            sl_trigger_px: if tpsl == Some("sl") { Some(trigger_px) } else { None },
        }
    } else {
        let tif = order
            .get("t")
            .and_then(|t| t.get("limit"))
            .and_then(|limit| limit.get("tif"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        OrderMeta {
            tif,
            reduce_only,
            order_type: Some("Limit".to_owned()),
            is_trigger: false,
            trigger_condition: None,
            trigger_px: None,
            is_position_tpsl: None,
            tp_trigger_px: None,
            sl_trigger_px: None,
        }
    };

    Some((asset, side, px, sz, cloid, meta))
}

fn parse_statuses(response: &Value) -> &[Value] {
    response
        .get("data")
        .and_then(|data| data.get("statuses"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn trim_newline(line: &mut Vec<u8>) {
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
}

fn parse_json_line<T>(line: &mut Vec<u8>) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    trim_newline(line);
    simd_json::serde::from_slice(line).map_err(anyhow::Error::from).context("failed to parse json line with simd-json")
}

fn load_block_indices(path: &PathBuf) -> Result<Vec<BlockIndex>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(Decoder::new(file)?);
    let mut reader = reader;
    let mut line = Vec::with_capacity(1 << 16);

    let mut block_indices = Vec::new();

    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }

        let block: NodeFillsBlock = parse_json_line(&mut line)?;
        let ts_ns = parse_timestamp_ns(&block.block_time)?;
        block_indices.push(BlockIndex { ts_ns, block_number: block.block_number });
    }

    block_indices.sort_by_key(|item| item.ts_ns);
    Ok(block_indices)
}

struct NodeFillsReader {
    reader: BufReader<Decoder<File>>,
    line: Vec<u8>,
}

impl NodeFillsReader {
    fn new(path: &PathBuf) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        Ok(Self { reader: BufReader::new(Decoder::new(file)?), line: Vec::with_capacity(1 << 16) })
    }

    fn next_batch(&mut self) -> Result<Option<NodeFillsBatch>> {
        self.line.clear();
        let read = self.reader.read_until(b'\n', &mut self.line)?;
        if read == 0 {
            return Ok(None);
        }

        let block: NodeFillsBlock = parse_json_line(&mut self.line)?;
        let block_time_ms = parse_timestamp_ms(&block.block_time)?;
        let mut fills = Vec::new();
        for (_address, event) in block.events {
            if event.coin != "BTC" {
                continue;
            }

            fills.push(FillRecord {
                block_number: block.block_number,
                block_time: block.block_time.clone(),
                oid: event.oid,
                sz: parse_decimal(&event.sz)?,
            });
        }

        Ok(Some(NodeFillsBatch { block_number: block.block_number, block_time_ms, fills }))
    }
}

struct ReplicaCmdsReader<'a> {
    reader: BufReader<Decoder<File>>,
    line: Vec<u8>,
    block_indices: &'a [BlockIndex],
    tolerance_ns: i64,
}

impl<'a> ReplicaCmdsReader<'a> {
    fn new(path: &PathBuf, block_indices: &'a [BlockIndex], tolerance_ns: i64) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        Ok(Self {
            reader: BufReader::new(Decoder::new(file)?),
            line: Vec::with_capacity(1 << 20),
            block_indices,
            tolerance_ns,
        })
    }

    fn next_batch(&mut self) -> Result<Option<ReplicaBatch>> {
        loop {
            self.line.clear();
            let read = self.reader.read_until(b'\n', &mut self.line)?;
            if read == 0 {
                return Ok(None);
            }

            let replica: ReplicaLine = parse_json_line(&mut self.line)?;
            let ts_ns = parse_timestamp_ns(&replica.abci_block.time)?;
            let Some(block_number) = nearest_block_number(self.block_indices, ts_ns, self.tolerance_ns) else {
                continue;
            };
            let block_time_ms = parse_timestamp_ms(&replica.abci_block.time)?;

            let action_map: HashMap<_, _> = replica.abci_block.signed_action_bundles.into_iter().collect();
            let mut events = Vec::new();

            for (tx_hash, responses) in replica.resps.full {
                let Some(payload) = action_map.get(&tx_hash) else {
                    continue;
                };

                for (action_item, response_item) in payload.signed_actions.iter().zip(responses.iter()) {
                    let action = &action_item.action;
                    let action_type = action.get("type").and_then(Value::as_str);
                    match action_type {
                        Some("order") => {
                            let orders =
                                action.get("orders").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);

                            for (order, status) in orders.iter().zip(statuses.iter()) {
                                let Some((asset, side, px, sz, cloid, meta)) = parse_order_fields(order) else {
                                    continue;
                                };
                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(user) = response_item.user.clone() else {
                                    continue;
                                };

                                let Some(resting) = status.get("resting") else {
                                    continue;
                                };
                                let Some(oid) = get_u64(resting, &["oid"]) else {
                                    continue;
                                };

                                events.push(CmdEvent::Add { user, oid, cloid, side, px, sz, meta });
                            }
                        }
                        Some("cancel") | Some("cancelByCloid") => {
                            let cancels =
                                action.get("cancels").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);

                            for (cancel, status) in cancels.iter().zip(statuses.iter()) {
                                if status.as_str() != Some("success") {
                                    continue;
                                }

                                let asset = get_u64(cancel, &["a", "asset"]).unwrap_or(BTC_ASSET_ID);
                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(order_ref) = get_order_ref(cancel, &["o", "oid", "cloid"]) else {
                                    continue;
                                };

                                events.push(CmdEvent::Cancel { order_ref });
                            }
                        }
                        Some("modify") => {
                            let Some(order) = action.get("order") else {
                                continue;
                            };
                            let Some((asset, side, px, sz, new_cloid, meta)) = parse_order_fields(order) else {
                                continue;
                            };
                            if asset != BTC_ASSET_ID {
                                continue;
                            }
                            let Some(user) = response_item.user.clone() else {
                                continue;
                            };
                            let Some(old_order_ref) = get_order_ref(action, &["oid", "cloid"]) else {
                                continue;
                            };

                            let statuses = parse_statuses(&response_item.res.response);
                            let Some(status) = statuses.first() else {
                                continue;
                            };
                            let new_oid = status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                            let placed = status.get("resting").is_some() || status.get("filled").is_some();

                            events.push(CmdEvent::Modify {
                                user,
                                old_order_ref,
                                new_oid,
                                new_cloid,
                                side,
                                px,
                                sz,
                                meta,
                                placed,
                            });
                        }
                        Some("batchModify") => {
                            let modifies =
                                action.get("modifies").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);

                            for (modify, status) in modifies.iter().zip(statuses.iter()) {
                                let Some(order) = modify.get("order") else {
                                    continue;
                                };
                                let Some((asset, side, px, sz, new_cloid, meta)) = parse_order_fields(order) else {
                                    continue;
                                };
                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(user) = response_item.user.clone() else {
                                    continue;
                                };
                                let Some(old_order_ref) = get_order_ref(modify, &["oid", "cloid"]) else {
                                    continue;
                                };
                                let new_oid = status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                                let placed = status.get("resting").is_some() || status.get("filled").is_some();

                                events.push(CmdEvent::Modify {
                                    user,
                                    old_order_ref,
                                    new_oid,
                                    new_cloid,
                                    side,
                                    px,
                                    sz,
                                    meta,
                                    placed,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }

            return Ok(Some(ReplicaBatch { block_number, block_time_ms, events }));
        }
    }
}

fn output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int64, false),
        Field::new("block_time", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("coin", DataType::Utf8, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("oid", DataType::Int64, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), false),
        Field::new("diff_type", DataType::Utf8, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), false),
        Field::new("orig_sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), true),
        Field::new("raw_sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), true),
        Field::new("is_trigger", DataType::Boolean, true),
        Field::new("tif", DataType::Utf8, true),
        Field::new("reduce_only", DataType::Boolean, true),
        Field::new("order_type", DataType::Utf8, true),
        Field::new("trigger_condition", DataType::Utf8, true),
        Field::new("trigger_px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("is_position_tpsl", DataType::Boolean, true),
        Field::new("tp_trigger_px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("sl_trigger_px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("status", DataType::Utf8, true),
        Field::new("lifetime", DataType::Int32, true),
    ]))
}

fn decimal_to_i128(mut value: Decimal, scale: i8) -> i128 {
    value.rescale(scale as u32);
    value.mantissa()
}

fn clamp_lifetime_ms(created_time_ms: i64, current_time_ms: i64) -> Option<i32> {
    let delta = current_time_ms - created_time_ms;
    i32::try_from(delta).ok()
}

fn resolve_order_ref(order_ref: &OrderRef, cloid_to_oid: &HashMap<String, u64>) -> Option<u64> {
    match order_ref {
        OrderRef::Oid(oid) => Some(*oid),
        OrderRef::Cloid(cloid) => cloid_to_oid.get(cloid).copied(),
    }
}

fn remove_live_order(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, u64>,
    oid: u64,
) -> Option<OrderState> {
    let state = orders.remove(&oid)?;
    if let Some(cloid) = &state.cloid {
        cloid_to_oid.remove(cloid);
    }
    Some(state)
}

fn process_block(
    block_number: u64,
    block_time_ms: i64,
    cmd_events: Option<Vec<CmdEvent>>,
    fills: Option<Vec<FillRecord>>,
    rows: &mut RowBuffer,
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<String, u64>,
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
) -> Result<()> {
    if let Some(events) = cmd_events {
        for event in &events {
            match event {
                CmdEvent::Add { user, oid, cloid, side, px, sz, meta } => {
                    if let Some(cloid) = cloid.clone() {
                        cloid_to_oid.insert(cloid, *oid);
                    }
                    orders.insert(
                        *oid,
                        OrderState {
                            user: user.clone(),
                            cloid: cloid.clone(),
                            created_time_ms: block_time_ms,
                            side: side.clone(),
                            px: *px,
                            remaining_sz: *sz,
                            tif: meta.tif.clone(),
                            reduce_only: meta.reduce_only,
                            order_type: meta.order_type.clone(),
                            is_trigger: meta.is_trigger,
                            trigger_condition: meta.trigger_condition.clone(),
                            trigger_px: meta.trigger_px,
                            is_position_tpsl: meta.is_position_tpsl,
                            tp_trigger_px: meta.tp_trigger_px,
                            sl_trigger_px: meta.sl_trigger_px,
                        },
                    );
                    rows.push(OutputRow {
                        block_number: i64::try_from(block_number)?,
                        block_time_ms,
                        coin: "BTC".to_owned(),
                        user: user.clone(),
                        oid: i64::try_from(*oid)?,
                        side: side.clone(),
                        px: *px,
                        diff_type: "new".to_owned(),
                        event: "add".to_owned(),
                        sz: *sz,
                        orig_sz: None,
                        raw_sz: None,
                        is_trigger: Some(meta.is_trigger),
                        tif: meta.tif.clone(),
                        reduce_only: meta.reduce_only,
                        order_type: meta.order_type.clone(),
                        trigger_condition: meta.trigger_condition.clone(),
                        trigger_px: meta.trigger_px,
                        is_position_tpsl: meta.is_position_tpsl,
                        tp_trigger_px: meta.tp_trigger_px,
                        sl_trigger_px: meta.sl_trigger_px,
                        status: None,
                        lifetime: None,
                    });
                }
                CmdEvent::Cancel { order_ref } => {
                    let Some(oid) = resolve_order_ref(order_ref, cloid_to_oid) else {
                        continue;
                    };
                    if let Some(state) = remove_live_order(orders, cloid_to_oid, oid) {
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: state.user,
                            oid: i64::try_from(oid)?,
                            side: state.side,
                            px: state.px,
                            diff_type: "remove".to_owned(),
                            event: "cancel".to_owned(),
                            sz: Decimal::ZERO,
                            orig_sz: Some(state.remaining_sz),
                            raw_sz: None,
                            is_trigger: Some(state.is_trigger),
                            tif: state.tif.clone(),
                            reduce_only: state.reduce_only,
                            order_type: state.order_type.clone(),
                            trigger_condition: state.trigger_condition.clone(),
                            trigger_px: state.trigger_px,
                            is_position_tpsl: state.is_position_tpsl,
                            tp_trigger_px: state.tp_trigger_px,
                            sl_trigger_px: state.sl_trigger_px,
                            status: None,
                            lifetime: clamp_lifetime_ms(state.created_time_ms, block_time_ms),
                        });
                    }
                }
                CmdEvent::Modify { user, old_order_ref, new_oid, new_cloid, side, px, sz, meta, placed } => {
                    if *placed {
                        if let Some(old_oid) = resolve_order_ref(old_order_ref, cloid_to_oid) {
                            if let Some(old_state) = remove_live_order(orders, cloid_to_oid, old_oid) {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: "BTC".to_owned(),
                                    user: old_state.user,
                                    oid: i64::try_from(old_oid)?,
                                    side: old_state.side,
                                    px: old_state.px,
                                    diff_type: "remove".to_owned(),
                                    event: "cancel".to_owned(),
                                    sz: Decimal::ZERO,
                                    orig_sz: Some(old_state.remaining_sz),
                                    raw_sz: None,
                                    is_trigger: Some(old_state.is_trigger),
                                    tif: old_state.tif.clone(),
                                    reduce_only: old_state.reduce_only,
                                    order_type: old_state.order_type.clone(),
                                    trigger_condition: old_state.trigger_condition.clone(),
                                    trigger_px: old_state.trigger_px,
                                    is_position_tpsl: old_state.is_position_tpsl,
                                    tp_trigger_px: old_state.tp_trigger_px,
                                    sl_trigger_px: old_state.sl_trigger_px,
                                    status: None,
                                    lifetime: clamp_lifetime_ms(old_state.created_time_ms, block_time_ms),
                                });
                            }
                        }

                        if let Some(new_oid) = new_oid {
                            if let Some(cloid) = new_cloid.clone() {
                                cloid_to_oid.insert(cloid, *new_oid);
                            }
                            orders.insert(
                                *new_oid,
                                OrderState {
                                    user: user.clone(),
                                    cloid: new_cloid.clone(),
                                    created_time_ms: block_time_ms,
                                    side: side.clone(),
                                    px: *px,
                                    remaining_sz: *sz,
                                    tif: meta.tif.clone(),
                                    reduce_only: meta.reduce_only,
                                    order_type: meta.order_type.clone(),
                                    is_trigger: meta.is_trigger,
                                    trigger_condition: meta.trigger_condition.clone(),
                                    trigger_px: meta.trigger_px,
                                    is_position_tpsl: meta.is_position_tpsl,
                                    tp_trigger_px: meta.tp_trigger_px,
                                    sl_trigger_px: meta.sl_trigger_px,
                                },
                            );
                            rows.push(OutputRow {
                                block_number: i64::try_from(block_number)?,
                                block_time_ms,
                                coin: "BTC".to_owned(),
                                user: user.clone(),
                                oid: i64::try_from(*new_oid)?,
                                side: side.clone(),
                                px: *px,
                                diff_type: "new".to_owned(),
                                event: "add".to_owned(),
                                sz: *sz,
                                orig_sz: None,
                                raw_sz: None,
                                is_trigger: Some(meta.is_trigger),
                                tif: meta.tif.clone(),
                                reduce_only: meta.reduce_only,
                                order_type: meta.order_type.clone(),
                                trigger_condition: meta.trigger_condition.clone(),
                                trigger_px: meta.trigger_px,
                                is_position_tpsl: meta.is_position_tpsl,
                                tp_trigger_px: meta.tp_trigger_px,
                                sl_trigger_px: meta.sl_trigger_px,
                                status: None,
                                lifetime: None,
                            });
                        }
                    }
                }
            }

            if rows.len() >= ROW_GROUP_SIZE
                && let Some(batch) = rows.take_batch(schema)?
            {
                writer.write(&batch)?;
            }
        }
    }

    if let Some(fills) = fills {
        for fill in fills {
            let Some(state) = orders.get_mut(&fill.oid) else {
                continue;
            };

            if state.remaining_sz <= Decimal::ZERO {
                remove_live_order(orders, cloid_to_oid, fill.oid);
                continue;
            }

            let fill_time_ms = parse_timestamp_ms(&fill.block_time)?;
            let old_remaining = state.remaining_sz;
            let mut new_remaining = old_remaining - fill.sz;
            if new_remaining < Decimal::ZERO {
                new_remaining = Decimal::ZERO;
            }

            rows.push(OutputRow {
                block_number: i64::try_from(fill.block_number)?,
                block_time_ms: fill_time_ms,
                coin: "BTC".to_owned(),
                user: state.user.clone(),
                oid: i64::try_from(fill.oid)?,
                side: state.side.clone(),
                px: state.px,
                diff_type: "update".to_owned(),
                event: "fill".to_owned(),
                sz: new_remaining,
                orig_sz: Some(old_remaining),
                raw_sz: None,
                is_trigger: None,
                tif: None,
                reduce_only: None,
                order_type: None,
                trigger_condition: None,
                trigger_px: None,
                is_position_tpsl: None,
                tp_trigger_px: None,
                sl_trigger_px: None,
                status: None,
                lifetime: None,
            });

            state.remaining_sz = new_remaining;

            if new_remaining <= Decimal::ZERO {
                let removed_state = remove_live_order(orders, cloid_to_oid, fill.oid)
                    .ok_or_else(|| anyhow!("missing state for oid {} on remove", fill.oid))?;
                rows.push(OutputRow {
                    block_number: i64::try_from(fill.block_number)?,
                    block_time_ms: fill_time_ms,
                    coin: "BTC".to_owned(),
                    user: removed_state.user,
                    oid: i64::try_from(fill.oid)?,
                    side: removed_state.side,
                    px: removed_state.px,
                    diff_type: "remove".to_owned(),
                    event: "fill".to_owned(),
                    sz: Decimal::ZERO,
                    orig_sz: None,
                    raw_sz: Some(old_remaining),
                    is_trigger: Some(removed_state.is_trigger),
                    tif: removed_state.tif.clone(),
                    reduce_only: removed_state.reduce_only,
                    order_type: removed_state.order_type.clone(),
                    trigger_condition: removed_state.trigger_condition.clone(),
                    trigger_px: removed_state.trigger_px,
                    is_position_tpsl: removed_state.is_position_tpsl,
                    tp_trigger_px: removed_state.tp_trigger_px,
                    sl_trigger_px: removed_state.sl_trigger_px,
                    status: None,
                    lifetime: clamp_lifetime_ms(removed_state.created_time_ms, fill_time_ms),
                });
            }

            if rows.len() >= ROW_GROUP_SIZE
                && let Some(batch) = rows.take_batch(schema)?
            {
                writer.write(&batch)?;
            }
        }
    }

    Ok(())
}

fn finalize_output(
    writer: ArrowWriter<File>,
    rows: &mut RowBuffer,
    schema: &Arc<Schema>,
) -> Result<()> {
    let mut writer = writer;
    if let Some(batch) = rows.take_batch(schema)? {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let block_indices = load_block_indices(&args.node_fills_by_block_lz4).context("indexing node_fills_by_block")?;
    let mut fills_reader =
        NodeFillsReader::new(&args.node_fills_by_block_lz4).context("opening node_fills_by_block")?;
    let mut replica_reader =
        ReplicaCmdsReader::new(&args.replica_cmds_lz4, &block_indices, args.block_time_tolerance_ns)
            .context("opening replica_cmds")?;

    let schema = output_schema();
    let file = File::create(&args.output_parquet)
        .with_context(|| format!("failed to create {}", args.output_parquet.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(ROW_GROUP_SIZE)
        .build();
    let mut writer = Some(ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?);

    let mut rows = RowBuffer::default();
    let mut orders: HashMap<u64, OrderState> = HashMap::new();
    let mut cloid_to_oid: HashMap<String, u64> = HashMap::new();
    let mut next_replica = replica_reader.next_batch()?;
    let mut next_fills = fills_reader.next_batch()?;

    while next_replica.is_some() || next_fills.is_some() {
        match (&next_replica, &next_fills) {
            (Some(replica), Some(fills)) if replica.block_number == fills.block_number => {
                let replica = next_replica.take().unwrap();
                let fills = next_fills.take().unwrap();
                process_block(
                    replica.block_number,
                    fills.block_time_ms,
                    Some(replica.events),
                    Some(fills.fills),
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    writer.as_mut().unwrap(),
                    &schema,
                )?;
                next_replica = replica_reader.next_batch()?;
                next_fills = fills_reader.next_batch()?;
            }
            (Some(replica), Some(fills)) if replica.block_number < fills.block_number => {
                let replica = next_replica.take().unwrap();
                process_block(
                    replica.block_number,
                    replica.block_time_ms,
                    Some(replica.events),
                    None,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    writer.as_mut().unwrap(),
                    &schema,
                )?;
                next_replica = replica_reader.next_batch()?;
            }
            (Some(_), Some(_)) => {
                let fills = next_fills.take().unwrap();
                process_block(
                    fills.block_number,
                    fills.block_time_ms,
                    None,
                    Some(fills.fills),
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    writer.as_mut().unwrap(),
                    &schema,
                )?;
                next_fills = fills_reader.next_batch()?;
            }
            (Some(replica), None) => {
                finalize_output(writer.take().unwrap(), &mut rows, &schema)?;
                bail!(
                    "node_fills_by_block hit EOF before replica_cmds; unpaired replica block {} remains",
                    replica.block_number
                );
            }
            (None, Some(fills)) => {
                finalize_output(writer.take().unwrap(), &mut rows, &schema)?;
                bail!(
                    "replica_cmds hit EOF before node_fills_by_block; unpaired fills block {} remains",
                    fills.block_number
                );
            }
            (None, None) => break,
        }
    }

    finalize_output(writer.take().unwrap(), &mut rows, &schema)?;

    println!(
        "Wrote BTC diff parquet to {}. Remaining live orders in memory at EOF: {}",
        args.output_parquet.display(),
        orders.len()
    );

    Ok(())
}
