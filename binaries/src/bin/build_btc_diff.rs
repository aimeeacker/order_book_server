#![allow(unused_crate_dependencies)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_compression::tokio::bufread::Lz4Decoder;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::RequestPayer;
use chrono::{Datelike, NaiveDateTime, Timelike};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader as TokioBufReader};

const BTC_ASSET_ID: u64 = 0;
const ROW_GROUP_SIZE: usize = 10_000;
const DECIMAL_PRECISION: u8 = 18;
const PX_SCALE: i8 = 1;
const SZ_SCALE: i8 = 5;
const HOUR_MS: i64 = 3_600_000;
const REQUESTER_PAYS_ALWAYS: bool = true;
const CANCEL_STATUS_CANCELED: &str = "canceled";
#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, visible_alias = "replica_cmds_lz4", default_value = "s3://hl-mainnet-node-data/replica_cmds")]
    replica_cmds: String,

    #[arg(
        long,
        visible_alias = "node_fills_by_block_lz4",
        default_value = "s3://hl-mainnet-node-data/node_fills_by_block"
    )]
    node_fills_by_block: String,

    #[arg(long)]
    output_parquet: PathBuf,

    #[arg(long)]
    start_height: Option<u64>,

    #[arg(long)]
    height_span: Option<u64>,

    #[arg(long, default_value_t = 2_000_000)]
    block_time_tolerance_ns: i64,

    #[arg(long, default_value_t = 0)]
    block_time_match_tolerance_ms: i64,
}

#[derive(Debug, Clone)]
enum InputSource {
    LocalFile(PathBuf),
    S3Prefix { bucket: String, prefix: String },
}

impl InputSource {
    fn parse(raw: &str) -> Result<Self> {
        if let Some(path_without_scheme) = raw.strip_prefix("s3://") {
            let mut parts = path_without_scheme.splitn(2, '/');
            let bucket = parts.next().unwrap_or_default().trim();
            let prefix = parts.next().unwrap_or_default().trim_matches('/');
            if bucket.is_empty() || prefix.is_empty() {
                bail!("invalid S3 URI: {raw}. expected format: s3://bucket/prefix");
            }
            return Ok(Self::S3Prefix { bucket: bucket.to_owned(), prefix: prefix.to_owned() });
        }

        Ok(Self::LocalFile(PathBuf::from(raw)))
    }

    fn requires_s3(&self) -> bool {
        matches!(self, Self::S3Prefix { .. })
    }
}

#[derive(Debug, Clone)]
enum ReaderSource {
    LocalFile(PathBuf),
    S3Keys { bucket: String, keys: Vec<String> },
    S3HourlyFrom { bucket: String, prefix: String, next_hour_ms: i64 },
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
    status: Option<String>,
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

#[derive(Debug, Deserialize)]
struct NodeFillsTimeBlock {
    block_time: String,
    block_number: u64,
}

#[derive(Debug, Default, Deserialize)]
struct FillEvent {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    px: String,
    #[serde(default)]
    side: String,
    oid: u64,
    #[serde(default)]
    sz: String,
    #[serde(default)]
    crossed: bool,
    tid: Option<u64>,
}

#[derive(Debug, Clone)]
struct FillRecord {
    block_number: u64,
    block_time_ms: i64,
    user: String,
    oid: u64,
    side: Option<String>,
    px: Option<Decimal>,
    sz: Decimal,
    crossed: bool,
    tid: Option<u64>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct UserCloidKey {
    user: String,
    cloid: String,
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
        raw_sz: Option<Decimal>,
        meta: OrderMeta,
    },
    Cancel {
        user: Option<String>,
        order_ref: OrderRef,
        fallback_on_missing: bool,
    },
    Modify {
        user: String,
        old_order_ref: OrderRef,
        new_oid: Option<u64>,
        new_cloid: Option<String>,
        side: String,
        px: Decimal,
        sz: Decimal,
        raw_sz: Option<Decimal>,
        meta: OrderMeta,
    },
    TrackTriggerAdd {
        user: String,
        oid: u64,
        cloid: Option<String>,
    },
    TrackTriggerPending {
        user: String,
        cloid: Option<String>,
    },
    TrackTriggerModify {
        user: String,
        old_order_ref: OrderRef,
        new_oid: Option<u64>,
        new_cloid: Option<String>,
    },
    Skipped {
        user: Option<String>,
        oid: Option<u64>,
        event: String,
        status: String,
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

#[derive(Debug, Default)]
struct TriggerOrderRefs {
    oid_to_cloid: HashMap<u64, Option<UserCloidKey>>,
    cloid_to_oid: HashMap<UserCloidKey, u64>,
    pending_by_user: HashMap<String, VecDeque<Option<String>>>,
}

#[derive(Debug)]
struct OutputRow {
    block_number: i64,
    block_time_ms: i64,
    coin: String,
    user: String,
    oid: Option<i64>,
    side: Option<String>,
    px: Option<Decimal>,
    diff_type: String,
    event: String,
    sz: Option<Decimal>,
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
            match row.oid {
                Some(value) => oid.append_value(value),
                None => oid.append_null(),
            }
            match row.side {
                Some(value) => side.append_value(value),
                None => side.append_null(),
            }
            match row.px {
                Some(value) => px.append_value(decimal_to_i128(value, PX_SCALE)),
                None => px.append_null(),
            }
            diff_type.append_value(&row.diff_type);
            event.append_value(&row.event);
            match row.sz {
                Some(value) => sz.append_value(decimal_to_i128(value, SZ_SCALE)),
                None => sz.append_null(),
            }
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
    let (ts_ns, _ts_ms) = parse_timestamp_ns_ms(raw)?;
    Ok(ts_ns)
}

fn parse_timestamp_ms(raw: &str) -> Result<i64> {
    let (_ts_ns, ts_ms) = parse_timestamp_ns_ms(raw)?;
    Ok(ts_ms)
}

fn parse_timestamp_ns_ms(raw: &str) -> Result<(i64, i64)> {
    let dt = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
        .with_context(|| format!("failed to parse timestamp: {raw}"))?;
    let utc = dt.and_utc();
    let ts_ns = utc.timestamp_nanos_opt().ok_or_else(|| anyhow!("timestamp overflow for {raw}"))?;
    let ts_ms = utc.timestamp_millis();
    Ok((ts_ns, ts_ms))
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    raw.parse::<Decimal>().with_context(|| format!("failed to parse decimal: {raw}"))
}

fn get_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        let Some(v) = value.get(*key) else {
            continue;
        };
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
        let Some(v) = value.get(*key) else {
            continue;
        };
        if *key == "cloid" {
            if let Some(raw) = v.as_str() {
                return Some(OrderRef::Cloid(raw.to_owned()));
            }
            continue;
        }
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

fn response_type(response: &Value) -> Option<&str> {
    response.get("type").and_then(Value::as_str)
}

fn is_dead_order_error(raw: &str) -> bool {
    raw.contains("Cannot modify canceled or filled order")
}

fn is_dead_order_modify_response(result: &ResponseResult) -> bool {
    if result.status.as_deref().is_some_and(is_dead_order_error) {
        return true;
    }

    if let Some(raw) = result.response.as_str() {
        return is_dead_order_error(raw);
    }

    parse_statuses(&result.response)
        .iter()
        .any(|status| status.get("error").and_then(Value::as_str).is_some_and(is_dead_order_error))
}

fn is_success_status(status: Option<&Value>) -> bool {
    status.and_then(Value::as_str).is_some_and(|raw| raw.eq_ignore_ascii_case("success"))
}

fn is_pending_trigger_status(status: &Value) -> bool {
    status
        .as_str()
        .is_some_and(|raw| raw.eq_ignore_ascii_case("waitingForTrigger") || raw.eq_ignore_ascii_case("waitingForFill"))
        || status.get("waitingForTrigger").and_then(Value::as_bool).unwrap_or(false)
        || status.get("waitingForFill").and_then(Value::as_bool).unwrap_or(false)
}

enum BookedSzDecision {
    Booked { booked_sz: Decimal, raw_sz: Option<Decimal> },
    FullyFilled,
    InvalidFilledTotalSz,
}

fn parse_booked_and_raw_sz(order_sz: Decimal, status: &Value) -> BookedSzDecision {
    let Some(filled) = status.get("filled") else {
        return BookedSzDecision::Booked { booked_sz: order_sz, raw_sz: None };
    };

    let Some(raw_total_sz) = get_string(filled, &["totalSz"]) else {
        return BookedSzDecision::InvalidFilledTotalSz;
    };

    let Ok(filled_sz) = parse_decimal(&raw_total_sz) else {
        return BookedSzDecision::InvalidFilledTotalSz;
    };

    let mut booked_sz = order_sz - filled_sz;
    if booked_sz < Decimal::ZERO {
        booked_sz = Decimal::ZERO;
    }
    if booked_sz <= Decimal::ZERO {
        return BookedSzDecision::FullyFilled;
    }

    let raw_sz = if booked_sz == order_sz { None } else { Some(order_sz) };
    BookedSzDecision::Booked { booked_sz, raw_sz }
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

fn floor_hour_ms(ts_ms: i64) -> i64 {
    ts_ms - ts_ms.rem_euclid(HOUR_MS)
}

async fn list_lz4_keys(client: &S3Client, bucket: &str, prefix: &str, requester_pays: bool) -> Result<Vec<String>> {
    let mut next_token: Option<String> = None;
    let mut keys = Vec::new();

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }
        if let Some(token) = &next_token {
            request = request.continuation_token(token);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                if let Some(service_error) = err.as_service_error()
                    && !requester_pays
                    && service_error.code().is_some_and(|code| code == "AccessDenied")
                {
                    bail!("failed to list s3://{bucket}/{prefix}: AccessDenied");
                }
                bail!("failed to list s3://{bucket}/{prefix}: {err}");
            }
        };

        for object in response.contents() {
            if let Some(key) = object.key()
                && key.ends_with(".lz4")
            {
                keys.push(key.to_owned());
            }
        }

        if response.is_truncated().unwrap_or(false) {
            next_token = response.next_continuation_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    keys.sort_unstable();
    Ok(keys)
}

async fn list_child_prefixes(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    requester_pays: bool,
) -> Result<Vec<String>> {
    let normalized_prefix = prefix.trim_matches('/');
    let list_prefix = format!("{normalized_prefix}/");

    let mut next_token: Option<String> = None;
    let mut prefixes = Vec::new();

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(&list_prefix).delimiter("/");
        if requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }
        if let Some(token) = &next_token {
            request = request.continuation_token(token);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                if let Some(service_error) = err.as_service_error()
                    && !requester_pays
                    && service_error.code().is_some_and(|code| code == "AccessDenied")
                {
                    bail!("failed to list child prefixes in s3://{bucket}/{normalized_prefix}: AccessDenied");
                }
                bail!("failed to list child prefixes in s3://{bucket}/{normalized_prefix}: {err}");
            }
        };

        for common_prefix in response.common_prefixes() {
            if let Some(child) = common_prefix.prefix() {
                let trimmed = child.trim_end_matches('/');
                if !trimmed.is_empty() {
                    prefixes.push(trimmed.to_owned());
                }
            }
        }

        if response.is_truncated().unwrap_or(false) {
            next_token = response.next_continuation_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        } else {
            break;
        }
    }

    prefixes.sort_unstable();
    prefixes.dedup();
    Ok(prefixes)
}

async fn head_object_exists(client: &S3Client, bucket: &str, key: &str, requester_pays: bool) -> Result<bool> {
    let mut request = client.head_object().bucket(bucket).key(key);
    if requester_pays {
        request = request.request_payer(RequestPayer::Requester);
    }

    match request.send().await {
        Ok(_) => Ok(true),
        Err(err) => {
            if let Some(service_error) = err.as_service_error() {
                let not_found = service_error.is_not_found()
                    || service_error.code().is_some_and(|code| matches!(code, "NotFound" | "NoSuchKey" | "404"));
                if not_found {
                    return Ok(false);
                }
                if !requester_pays && service_error.code().is_some_and(|code| code == "AccessDenied") {
                    bail!("failed to head s3://{bucket}/{key}: AccessDenied");
                }
            }

            bail!("failed to head s3://{bucket}/{key}: {err}")
        }
    }
}

fn resolve_required_chunks_from_keys(required_chunks: &[u64], keys: Vec<String>) -> Option<Vec<String>> {
    let mut basename_to_key: HashMap<String, String> = HashMap::new();
    for key in keys {
        let Some(name) = key.rsplit('/').next() else {
            continue;
        };
        match basename_to_key.get(name) {
            Some(existing) if existing >= &key => {}
            _ => {
                basename_to_key.insert(name.to_owned(), key);
            }
        }
    }

    let mut resolved = Vec::with_capacity(required_chunks.len());
    for chunk in required_chunks {
        let file_name = format!("{chunk}.lz4");
        let full_key = basename_to_key.get(&file_name)?.clone();
        resolved.push(full_key);
    }
    Some(resolved)
}

async fn resolve_required_chunks_in_prefix(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    required_chunks: &[u64],
    requester_pays: bool,
) -> Result<Option<Vec<String>>> {
    let keys = list_lz4_keys(client, bucket, prefix, requester_pays).await?;
    Ok(resolve_required_chunks_from_keys(required_chunks, keys))
}

fn replica_chunk_starts_for_range(start_height: u64, height_span: u64) -> Result<Vec<u64>> {
    if start_height == 0 {
        bail!("--start-height must be >= 1 for replica_cmds naming rule <height-1>.lz4");
    }
    if height_span == 0 {
        bail!("--height-span must be > 0");
    }

    let end_height = start_height
        .checked_add(height_span - 1)
        .ok_or_else(|| anyhow!("height overflow: start_height={start_height} span={height_span}"))?;

    let start_chunk = ((start_height - 1) / 10_000) * 10_000;
    let end_chunk = ((end_height - 1) / 10_000) * 10_000;

    let mut chunk = start_chunk;
    let mut chunks = Vec::new();
    while chunk <= end_chunk {
        chunks.push(chunk);
        chunk = chunk.checked_add(10_000).ok_or_else(|| anyhow!("replica chunk overflow while covering range"))?;
    }
    Ok(chunks)
}

async fn resolve_replica_keys_for_range(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    start_height: u64,
    height_span: u64,
    requester_pays: bool,
) -> Result<Vec<String>> {
    let required_chunks = replica_chunk_starts_for_range(start_height, height_span)?;
    let normalized_prefix = prefix.trim_matches('/');

    // Fast path: try deterministic keys directly under the provided prefix.
    let mut direct_keys = Vec::with_capacity(required_chunks.len());
    let mut all_direct_present = true;
    for chunk in &required_chunks {
        let key = format!("{normalized_prefix}/{chunk}.lz4");
        if head_object_exists(client, bucket, &key, requester_pays).await? {
            direct_keys.push(key);
        } else {
            all_direct_present = false;
            break;
        }
    }
    if all_direct_present {
        return Ok(direct_keys);
    }

    // Structured path optimization:
    // `replica_cmds/<snapshot>/<yyyymmdd>/<chunk>.lz4`
    // Scan snapshots from newest to oldest and only recurse inside one snapshot at a time.
    let child_prefixes = list_child_prefixes(client, bucket, normalized_prefix, requester_pays).await?;
    for child_prefix in child_prefixes.into_iter().rev() {
        if let Some(resolved) =
            resolve_required_chunks_in_prefix(client, bucket, &child_prefix, &required_chunks, requester_pays).await?
        {
            return Ok(resolved);
        }
    }

    // Fallback: recurse under the original prefix for compatibility with custom layouts.
    if let Some(resolved) =
        resolve_required_chunks_in_prefix(client, bucket, normalized_prefix, &required_chunks, requester_pays).await?
    {
        return Ok(resolved);
    }

    bail!(
        "missing replica keys for range start_height={} span={} under s3://{}/{}",
        start_height,
        height_span,
        bucket,
        normalized_prefix
    )
}

fn build_node_fills_hourly_key(prefix: &str, hour_start_ms: i64) -> Result<String> {
    let dt = chrono::DateTime::from_timestamp_millis(hour_start_ms)
        .ok_or_else(|| anyhow!("invalid hour timestamp millis: {hour_start_ms}"))?;
    Ok(format!("{prefix}/hourly/{:04}{:02}{:02}/{}.lz4", dt.year(), dt.month(), dt.day(), dt.hour()))
}

async fn first_replica_timestamp_ms(
    source: ReaderSource,
    s3_client: Option<Arc<S3Client>>,
    requester_pays: bool,
) -> Result<i64> {
    let mut reader = Lz4JsonLineReader::new(source, s3_client, requester_pays).await?;
    let Some(replica) = reader.next_json_line::<ReplicaLine>().await? else {
        bail!("replica_cmds has no readable json lines");
    };
    parse_timestamp_ms(&replica.abci_block.time)
}

struct Lz4JsonLineReader {
    source: ReaderSource,
    s3_client: Option<Arc<S3Client>>,
    requester_pays: bool,
    next_key_idx: usize,
    local_opened: bool,
    current_reader: Option<Box<dyn AsyncBufRead + Unpin + Send>>,
    line: Vec<u8>,
}

impl Lz4JsonLineReader {
    async fn new(source: ReaderSource, s3_client: Option<Arc<S3Client>>, requester_pays: bool) -> Result<Self> {
        if matches!(source, ReaderSource::S3Keys { .. } | ReaderSource::S3HourlyFrom { .. }) && s3_client.is_none() {
            bail!("S3 input requires initialized S3 client");
        }
        Ok(Self {
            source,
            s3_client,
            requester_pays,
            next_key_idx: 0,
            local_opened: false,
            current_reader: None,
            line: Vec::with_capacity(1 << 20),
        })
    }

    fn make_lz4_reader<R>(reader: R) -> Box<dyn AsyncBufRead + Unpin + Send>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let buffered = TokioBufReader::new(reader);
        let decoder = Lz4Decoder::new(buffered);
        Box::new(TokioBufReader::new(decoder))
    }

    async fn fetch_s3_reader(
        &self,
        bucket: &str,
        key: &str,
        allow_missing: bool,
    ) -> Result<Option<Box<dyn AsyncBufRead + Unpin + Send>>> {
        let Some(client) = self.s3_client.as_ref() else {
            bail!("S3 input requires initialized S3 client");
        };

        let mut request = client.get_object().bucket(bucket).key(key);
        if self.requester_pays {
            request = request.request_payer(RequestPayer::Requester);
        }

        let output = match request.send().await {
            Ok(output) => output,
            Err(err) => {
                if let Some(service_error) = err.as_service_error() {
                    if allow_missing
                        && (service_error.is_no_such_key()
                            || service_error.code().is_some_and(|code| code == "NoSuchKey"))
                    {
                        return Ok(None);
                    }

                    if !self.requester_pays && service_error.code().is_some_and(|code| code == "AccessDenied") {
                        bail!("failed to fetch s3://{bucket}/{key}: AccessDenied");
                    }
                }

                bail!("failed to fetch s3://{bucket}/{key}: {err}");
            }
        };

        Ok(Some(Self::make_lz4_reader(output.body.into_async_read())))
    }

    async fn open_next_reader(&mut self) -> Result<bool> {
        enum OpenPlan {
            Local(PathBuf),
            S3Key { bucket: String, key: String, allow_missing: bool, advance_hour: bool },
            End,
        }

        let plan = match &mut self.source {
            ReaderSource::LocalFile(path) => {
                if self.local_opened {
                    OpenPlan::End
                } else {
                    self.local_opened = true;
                    OpenPlan::Local(path.clone())
                }
            }
            ReaderSource::S3Keys { bucket, keys } => {
                if self.next_key_idx >= keys.len() {
                    OpenPlan::End
                } else {
                    let key = keys[self.next_key_idx].clone();
                    self.next_key_idx += 1;
                    OpenPlan::S3Key { bucket: bucket.clone(), key, allow_missing: false, advance_hour: false }
                }
            }
            ReaderSource::S3HourlyFrom { bucket, prefix, next_hour_ms } => {
                let key = build_node_fills_hourly_key(prefix, *next_hour_ms)?;
                OpenPlan::S3Key { bucket: bucket.clone(), key, allow_missing: true, advance_hour: true }
            }
        };

        match plan {
            OpenPlan::End => Ok(false),
            OpenPlan::Local(path) => {
                let file =
                    tokio::fs::File::open(&path).await.with_context(|| format!("failed to open {}", path.display()))?;
                self.current_reader = Some(Self::make_lz4_reader(file));
                Ok(true)
            }
            OpenPlan::S3Key { bucket, key, allow_missing, advance_hour } => {
                let maybe_reader = self.fetch_s3_reader(&bucket, &key, allow_missing).await?;
                let Some(reader) = maybe_reader else {
                    return Ok(false);
                };
                if advance_hour && let ReaderSource::S3HourlyFrom { next_hour_ms, .. } = &mut self.source {
                    *next_hour_ms += HOUR_MS;
                }
                self.current_reader = Some(reader);
                Ok(true)
            }
        }
    }

    async fn next_json_line<T>(&mut self) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        loop {
            if self.current_reader.is_none() && !self.open_next_reader().await? {
                return Ok(None);
            }

            self.line.clear();
            let bytes_read =
                self.current_reader.as_mut().expect("current_reader checked").read_until(b'\n', &mut self.line).await?;
            if bytes_read == 0 {
                self.current_reader = None;
                continue;
            }

            return parse_json_line(&mut self.line).map(Some);
        }
    }
}

struct NodeFillsReader {
    reader: Lz4JsonLineReader,
    block_range: Option<(u64, u64)>,
}

impl NodeFillsReader {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
    ) -> Result<Self> {
        Ok(Self { reader: Lz4JsonLineReader::new(source, s3_client, requester_pays).await?, block_range })
    }

    async fn next_batch(&mut self) -> Result<Option<NodeFillsBatch>> {
        let block = loop {
            let Some(block) = self.reader.next_json_line::<NodeFillsBlock>().await? else {
                return Ok(None);
            };

            if let Some((start_height, end_height)) = self.block_range {
                if block.block_number < start_height {
                    continue;
                }
                if block.block_number > end_height {
                    return Ok(None);
                }
            }
            break block;
        };

        let block_time_ms = parse_timestamp_ms(&block.block_time)?;
        let mut fills = Vec::with_capacity(block.events.len());
        for (address, event) in block.events {
            if event.coin != "BTC" {
                continue;
            }

            fills.push(FillRecord {
                block_number: block.block_number,
                block_time_ms,
                user: address,
                oid: event.oid,
                side: (!event.side.is_empty()).then_some(event.side),
                px: (!event.px.is_empty()).then(|| parse_decimal(&event.px)).transpose()?,
                sz: parse_decimal(&event.sz)?,
                crossed: event.crossed,
                tid: event.tid,
            });
        }

        Ok(Some(NodeFillsBatch { block_number: block.block_number, block_time_ms, fills }))
    }
}

enum BlockMatch {
    Matched(u64),
    Unmatched,
    PastEnd,
}

struct FillsTimeCursor {
    reader: Lz4JsonLineReader,
    block_range: Option<(u64, u64)>,
    prev: Option<BlockIndex>,
    curr: Option<BlockIndex>,
    exhausted: bool,
}

impl FillsTimeCursor {
    async fn new(
        source: ReaderSource,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
    ) -> Result<Self> {
        let mut cursor = Self {
            reader: Lz4JsonLineReader::new(source, s3_client, requester_pays).await?,
            block_range,
            prev: None,
            curr: None,
            exhausted: false,
        };
        cursor.curr = cursor.read_next_index().await?;
        Ok(cursor)
    }

    async fn read_next_index(&mut self) -> Result<Option<BlockIndex>> {
        loop {
            let Some(block) = self.reader.next_json_line::<NodeFillsTimeBlock>().await? else {
                self.exhausted = true;
                return Ok(None);
            };

            if let Some((start_height, end_height)) = self.block_range {
                if block.block_number < start_height {
                    continue;
                }
                if block.block_number > end_height {
                    self.exhausted = true;
                    return Ok(None);
                }
            }

            let ts_ns = parse_timestamp_ns(&block.block_time)?;
            return Ok(Some(BlockIndex { ts_ns, block_number: block.block_number }));
        }
    }

    async fn match_block(&mut self, ts_ns: i64, tolerance_ns: i64) -> Result<BlockMatch> {
        while self.curr.as_ref().is_some_and(|curr| curr.ts_ns < ts_ns) {
            self.prev = self.curr.take();
            self.curr = self.read_next_index().await?;
        }

        let mut best: Option<(i64, u64)> = None;
        for candidate in [self.prev.as_ref(), self.curr.as_ref()].into_iter().flatten() {
            let delta = (candidate.ts_ns - ts_ns).abs();
            if delta <= tolerance_ns {
                match best {
                    Some((best_delta, _)) if best_delta <= delta => {}
                    _ => best = Some((delta, candidate.block_number)),
                }
            }
        }

        if let Some((_, block_number)) = best {
            return Ok(BlockMatch::Matched(block_number));
        }

        if self.exhausted
            && self.curr.is_none()
            && self.prev.as_ref().is_some_and(|last| ts_ns > last.ts_ns.saturating_add(tolerance_ns))
        {
            return Ok(BlockMatch::PastEnd);
        }

        Ok(BlockMatch::Unmatched)
    }
}

struct ReplicaCmdsReader {
    reader: Lz4JsonLineReader,
    fills_time_cursor: FillsTimeCursor,
    tolerance_ns: i64,
    block_range: Option<(u64, u64)>,
}

impl ReplicaCmdsReader {
    async fn new(
        source: ReaderSource,
        fills_time_source: ReaderSource,
        tolerance_ns: i64,
        s3_client: Option<Arc<S3Client>>,
        requester_pays: bool,
        block_range: Option<(u64, u64)>,
    ) -> Result<Self> {
        Ok(Self {
            reader: Lz4JsonLineReader::new(source, s3_client.clone(), requester_pays).await?,
            fills_time_cursor: FillsTimeCursor::new(fills_time_source, s3_client, requester_pays, block_range).await?,
            tolerance_ns,
            block_range,
        })
    }

    async fn next_batch(&mut self) -> Result<Option<ReplicaBatch>> {
        loop {
            let Some(replica) = self.reader.next_json_line::<ReplicaLine>().await? else {
                return Ok(None);
            };

            let (ts_ns, block_time_ms) = parse_timestamp_ns_ms(&replica.abci_block.time)?;
            let block_number = match self.fills_time_cursor.match_block(ts_ns, self.tolerance_ns).await? {
                BlockMatch::Matched(block_number) => block_number,
                BlockMatch::Unmatched => continue,
                BlockMatch::PastEnd => return Ok(None),
            };
            if let Some((start_height, end_height)) = self.block_range {
                if block_number < start_height {
                    continue;
                }
                if block_number > end_height {
                    return Ok(None);
                }
            }

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

                                let has_resting = status.get("resting").is_some();
                                let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                                let oid_from_resting =
                                    status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                                let oid_from_filled = status.get("filled").and_then(|filled| get_u64(filled, &["oid"]));
                                let oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                if meta.is_trigger {
                                    if let Some(oid) = oid {
                                        events.push(CmdEvent::TrackTriggerAdd { user, oid, cloid });
                                    } else if is_pending_trigger_status(status) {
                                        events.push(CmdEvent::TrackTriggerPending { user, cloid });
                                    }
                                    continue;
                                }
                                let Some(oid) = oid else {
                                    continue;
                                };

                                if !has_resting && !is_gtc {
                                    continue;
                                }

                                match parse_booked_and_raw_sz(sz, status) {
                                    BookedSzDecision::Booked { booked_sz, raw_sz } => {
                                        events.push(CmdEvent::Add {
                                            user,
                                            oid,
                                            cloid,
                                            side,
                                            px,
                                            sz: booked_sz,
                                            raw_sz,
                                            meta,
                                        });
                                    }
                                    BookedSzDecision::FullyFilled => continue,
                                    BookedSzDecision::InvalidFilledTotalSz => {
                                        events.push(CmdEvent::Skipped {
                                            user: Some(user),
                                            oid: Some(oid),
                                            event: "add".to_owned(),
                                            status: "skipped_invalid_filled_total_sz".to_owned(),
                                        });
                                    }
                                }
                            }
                        }
                        Some("cancel") | Some("cancelByCloid") => {
                            let cancels =
                                action.get("cancels").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);
                            let is_cancel_by_cloid = action_type == Some("cancelByCloid");

                            for (idx, cancel) in cancels.iter().enumerate() {
                                let status = statuses.get(idx);
                                if is_cancel_by_cloid {
                                    if !is_success_status(status) {
                                        continue;
                                    }
                                } else if !is_success_status(status) {
                                    continue;
                                }
                                let order_ref = get_order_ref(cancel, &["o", "oid", "cloid"]);
                                let Some(asset) = get_u64(cancel, &["a", "asset"]) else {
                                    continue;
                                };

                                if asset != BTC_ASSET_ID {
                                    continue;
                                }
                                let Some(order_ref) = order_ref else {
                                    continue;
                                };
                                if matches!(order_ref, OrderRef::Oid(0)) {
                                    continue;
                                }

                                events.push(CmdEvent::Cancel {
                                    user: response_item.user.clone(),
                                    order_ref,
                                    fallback_on_missing: true,
                                });
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
                            if is_dead_order_modify_response(&response_item.res) {
                                continue;
                            }

                            let statuses = parse_statuses(&response_item.res.response);
                            let Some(status) = statuses.first() else {
                                if meta.is_trigger {
                                    eprintln!(
                                        "[warn] block={} user={} trigger modify returned no statuses; skipping old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                } else {
                                    eprintln!(
                                        "[warn] block={} user={} modify returned no statuses; treating as cancel-only old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                    events.push(CmdEvent::Cancel {
                                        user: Some(user.clone()),
                                        order_ref: old_order_ref,
                                        fallback_on_missing: true,
                                    });
                                }
                                continue;
                            };
                            let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                            let oid_from_resting = status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                            let oid_from_filled = status.get("filled").and_then(|filled| get_u64(filled, &["oid"]));
                            if meta.is_trigger {
                                let new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                events.push(CmdEvent::TrackTriggerModify { user, old_order_ref, new_oid, new_cloid });
                                continue;
                            }

                            let (new_sz, raw_sz, invalid_filled_total_sz) = match parse_booked_and_raw_sz(sz, status) {
                                BookedSzDecision::Booked { booked_sz, raw_sz } => (booked_sz, raw_sz, false),
                                BookedSzDecision::FullyFilled => (Decimal::ZERO, None, false),
                                BookedSzDecision::InvalidFilledTotalSz => (Decimal::ZERO, None, true),
                            };
                            let mut new_oid = oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                            if new_sz <= Decimal::ZERO {
                                new_oid = None;
                            }
                            events.push(CmdEvent::Modify {
                                user: user.clone(),
                                old_order_ref,
                                new_oid,
                                new_cloid,
                                side,
                                px,
                                sz: new_sz,
                                raw_sz,
                                meta,
                            });

                            if invalid_filled_total_sz {
                                events.push(CmdEvent::Skipped {
                                    user: Some(user),
                                    oid: oid_from_resting.or(oid_from_filled),
                                    event: "modify".to_owned(),
                                    status: "skipped_invalid_filled_total_sz".to_owned(),
                                });
                            }
                        }
                        Some("batchModify") => {
                            let modifies =
                                action.get("modifies").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
                            let statuses = parse_statuses(&response_item.res.response);

                            for (idx, modify) in modifies.iter().enumerate() {
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
                                let status = statuses.get(idx);
                                if status.is_none() && meta.is_trigger {
                                    eprintln!(
                                        "[warn] block={} user={} trigger batchModify item={} returned no statuses; skipping old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        idx,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                    continue;
                                }
                                if status.is_none() && !meta.is_trigger {
                                    eprintln!(
                                        "[warn] block={} user={} batchModify item={} returned no statuses; treating as cancel-only old_ref={:?} response_type={:?}",
                                        block_number,
                                        user,
                                        idx,
                                        old_order_ref,
                                        response_type(&response_item.res.response)
                                    );
                                    events.push(CmdEvent::Cancel {
                                        user: Some(user),
                                        order_ref: old_order_ref,
                                        fallback_on_missing: true,
                                    });
                                    continue;
                                }
                                if status
                                    .and_then(|status| status.get("error").and_then(Value::as_str))
                                    .is_some_and(is_dead_order_error)
                                {
                                    continue;
                                }
                                let status = status.unwrap();
                                let is_gtc = meta.tif.as_deref().is_some_and(|tif| tif.eq_ignore_ascii_case("Gtc"));
                                let oid_from_resting =
                                    status.get("resting").and_then(|resting| get_u64(resting, &["oid"]));
                                let oid_from_filled = status.get("filled").and_then(|filled| get_u64(filled, &["oid"]));
                                if meta.is_trigger {
                                    let new_oid =
                                        oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                    events.push(CmdEvent::TrackTriggerModify {
                                        user,
                                        old_order_ref,
                                        new_oid,
                                        new_cloid,
                                    });
                                    continue;
                                }

                                let (new_sz, raw_sz, invalid_filled_total_sz) =
                                    match parse_booked_and_raw_sz(sz, status) {
                                        BookedSzDecision::Booked { booked_sz, raw_sz } => (booked_sz, raw_sz, false),
                                        BookedSzDecision::FullyFilled => (Decimal::ZERO, None, false),
                                        BookedSzDecision::InvalidFilledTotalSz => (Decimal::ZERO, None, true),
                                    };
                                let mut new_oid =
                                    oid_from_resting.or_else(|| if is_gtc { oid_from_filled } else { None });
                                if new_sz <= Decimal::ZERO {
                                    new_oid = None;
                                }
                                events.push(CmdEvent::Modify {
                                    user: user.clone(),
                                    old_order_ref,
                                    new_oid,
                                    new_cloid,
                                    side,
                                    px,
                                    sz: new_sz,
                                    raw_sz,
                                    meta,
                                });

                                if invalid_filled_total_sz {
                                    events.push(CmdEvent::Skipped {
                                        user: Some(user),
                                        oid: oid_from_resting.or(oid_from_filled),
                                        event: "modify".to_owned(),
                                        status: "skipped_invalid_filled_total_sz".to_owned(),
                                    });
                                }
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
        Field::new("oid", DataType::Int64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("px", DataType::Decimal128(DECIMAL_PRECISION, PX_SCALE), true),
        Field::new("diff_type", DataType::Utf8, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("sz", DataType::Decimal128(DECIMAL_PRECISION, SZ_SCALE), true),
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

fn resolve_order_ref(
    order_ref: &OrderRef,
    user: Option<&str>,
    cloid_to_oid: &HashMap<UserCloidKey, u64>,
) -> Option<u64> {
    match order_ref {
        OrderRef::Oid(oid) => Some(*oid),
        OrderRef::Cloid(cloid) => {
            let user = user?;
            cloid_to_oid.get(&UserCloidKey { user: user.to_owned(), cloid: cloid.clone() }).copied()
        }
    }
}

fn resolve_trigger_ref(order_ref: &OrderRef, user: Option<&str>, trigger_refs: &TriggerOrderRefs) -> Option<u64> {
    match order_ref {
        OrderRef::Oid(oid) => trigger_refs.oid_to_cloid.contains_key(oid).then_some(*oid),
        OrderRef::Cloid(cloid) => {
            let user = user?;
            trigger_refs.cloid_to_oid.get(&UserCloidKey { user: user.to_owned(), cloid: cloid.clone() }).copied()
        }
    }
}

fn remove_trigger_ref(trigger_refs: &mut TriggerOrderRefs, oid: u64) -> bool {
    let Some(cloid_key) = trigger_refs.oid_to_cloid.remove(&oid) else {
        return false;
    };
    if let Some(ref cloid_key) = cloid_key {
        trigger_refs.cloid_to_oid.remove(&cloid_key);
    }
    true
}

fn insert_pending_trigger_ref(trigger_refs: &mut TriggerOrderRefs, user: &str, cloid: Option<&str>) {
    trigger_refs
        .pending_by_user
        .entry(user.to_owned())
        .or_default()
        .push_back(cloid.map(ToOwned::to_owned));
}

fn remove_pending_trigger_ref_by_cloid(trigger_refs: &mut TriggerOrderRefs, user: &str, cloid: &str) -> bool {
    let Some(pending) = trigger_refs.pending_by_user.get_mut(user) else {
        return false;
    };
    let Some(idx) = pending.iter().position(|pending_cloid| pending_cloid.as_deref() == Some(cloid)) else {
        return false;
    };
    pending.remove(idx);
    if pending.is_empty() {
        trigger_refs.pending_by_user.remove(user);
    }
    true
}

fn consume_pending_trigger_ref_for_user(trigger_refs: &mut TriggerOrderRefs, user: &str) -> bool {
    let Some(pending) = trigger_refs.pending_by_user.get_mut(user) else {
        return false;
    };
    if pending.pop_front().is_none() {
        return false;
    }
    if pending.is_empty() {
        trigger_refs.pending_by_user.remove(user);
    }
    true
}

fn insert_trigger_ref(trigger_refs: &mut TriggerOrderRefs, user: &str, oid: u64, cloid: Option<&str>) {
    remove_trigger_ref(trigger_refs, oid);
    if let Some(cloid) = cloid {
        remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid);
    }
    let cloid_key = cloid.map(|cloid| UserCloidKey { user: user.to_owned(), cloid: cloid.to_owned() });
    if let Some(cloid_key) = &cloid_key {
        trigger_refs.cloid_to_oid.insert(cloid_key.clone(), oid);
    }
    trigger_refs.oid_to_cloid.insert(oid, cloid_key);
}

fn remove_live_order(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<UserCloidKey, u64>,
    oid: u64,
) -> Option<OrderState> {
    let state = orders.remove(&oid)?;
    if let Some(cloid) = &state.cloid {
        cloid_to_oid.remove(&UserCloidKey { user: state.user.clone(), cloid: cloid.clone() });
    }
    Some(state)
}

fn insert_live_order(
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<UserCloidKey, u64>,
    user: &str,
    oid: u64,
    cloid: Option<&str>,
    created_time_ms: i64,
    side: &str,
    px: Decimal,
    sz: Decimal,
    meta: &OrderMeta,
) {
    if let Some(cloid) = cloid {
        cloid_to_oid.insert(UserCloidKey { user: user.to_owned(), cloid: cloid.to_owned() }, oid);
    }
    orders.insert(
        oid,
        OrderState {
            user: user.to_owned(),
            cloid: cloid.map(ToOwned::to_owned),
            created_time_ms,
            side: side.to_owned(),
            px,
            remaining_sz: sz,
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
}

fn push_add_row(
    rows: &mut RowBuffer,
    block_number: u64,
    block_time_ms: i64,
    user: &str,
    oid: u64,
    side: &str,
    px: Decimal,
    sz: Decimal,
    raw_sz: Option<Decimal>,
    meta: &OrderMeta,
) -> Result<()> {
    rows.push(OutputRow {
        block_number: i64::try_from(block_number)?,
        block_time_ms,
        coin: "BTC".to_owned(),
        user: user.to_owned(),
        oid: Some(i64::try_from(oid)?),
        side: Some(side.to_owned()),
        px: Some(px),
        diff_type: "new".to_owned(),
        event: "add".to_owned(),
        sz: Some(sz),
        orig_sz: None,
        raw_sz,
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
    Ok(())
}

fn process_block(
    block_number: u64,
    block_time_ms: i64,
    cmd_events: Option<Vec<CmdEvent>>,
    fills: Option<Vec<FillRecord>>,
    emit_rows: bool,
    rows: &mut RowBuffer,
    orders: &mut HashMap<u64, OrderState>,
    cloid_to_oid: &mut HashMap<UserCloidKey, u64>,
    trigger_refs: &mut TriggerOrderRefs,
    known_non_trigger_oids: &mut HashSet<u64>,
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
) -> Result<()> {
    if let Some(events) = cmd_events {
        let mut canceled_oids_in_block: HashSet<u64> = HashSet::new();
        let mut deferred_cancels: Vec<(Option<String>, OrderRef, bool)> = Vec::new();
        for event in &events {
            match event {
                CmdEvent::Add { user, oid, cloid, side, px, sz, raw_sz, meta } => {
                    known_non_trigger_oids.insert(*oid);
                    insert_live_order(
                        orders,
                        cloid_to_oid,
                        user,
                        *oid,
                        cloid.as_deref(),
                        block_time_ms,
                        side,
                        *px,
                        *sz,
                        meta,
                    );
                    if emit_rows {
                        push_add_row(rows, block_number, block_time_ms, user, *oid, side, *px, *sz, *raw_sz, meta)?;
                    }
                }
                CmdEvent::Cancel { user, order_ref, fallback_on_missing } => {
                    if let Some(trigger_oid) = resolve_trigger_ref(order_ref, user.as_deref(), trigger_refs) {
                        remove_trigger_ref(trigger_refs, trigger_oid);
                        continue;
                    }
                    if let (Some(user), OrderRef::Cloid(cloid)) = (user.as_deref(), order_ref)
                        && remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid)
                    {
                        continue;
                    }
                    let resolved_oid = resolve_order_ref(order_ref, user.as_deref(), cloid_to_oid);
                    if resolved_oid.is_none() && matches!(order_ref, OrderRef::Cloid(_)) && user.is_some() {
                        deferred_cancels.push((user.clone(), order_ref.clone(), *fallback_on_missing));
                        continue;
                    }
                    let mut emitted_remove = false;

                    if let Some(oid) = resolved_oid {
                        if canceled_oids_in_block.contains(&oid) {
                            continue;
                        }
                        if let Some(state) = remove_live_order(orders, cloid_to_oid, oid) {
                            canceled_oids_in_block.insert(oid);
                            if emit_rows {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: "BTC".to_owned(),
                                    user: state.user,
                                    oid: Some(i64::try_from(oid)?),
                                    side: Some(state.side),
                                    px: Some(state.px),
                                    diff_type: "remove".to_owned(),
                                    event: "cancel".to_owned(),
                                    sz: Some(Decimal::ZERO),
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
                                    status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                                    lifetime: clamp_lifetime_ms(state.created_time_ms, block_time_ms),
                                });
                            }
                            emitted_remove = true;
                        }
                    }

                    if !emitted_remove && *fallback_on_missing {
                        if let (Some(user), OrderRef::Oid(oid)) = (user.as_deref(), order_ref)
                            && !known_non_trigger_oids.contains(oid)
                            && consume_pending_trigger_ref_for_user(trigger_refs, user)
                        {
                            continue;
                        }
                    }

                    if emit_rows && !emitted_remove && *fallback_on_missing {
                        let fallback_oid = match order_ref {
                            OrderRef::Oid(oid) => Some(i64::try_from(*oid)?),
                            OrderRef::Cloid(_) => None,
                        };
                        if let Some(oid) = resolved_oid {
                            canceled_oids_in_block.insert(oid);
                        }
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: user.clone().unwrap_or_default(),
                            oid: fallback_oid,
                            side: None,
                            px: None,
                            diff_type: "remove".to_owned(),
                            event: "cancel".to_owned(),
                            sz: None,
                            orig_sz: None,
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
                            status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                            lifetime: None,
                        });
                    }
                }
                CmdEvent::Modify { user, old_order_ref, new_oid, new_cloid, side, px, sz, raw_sz, meta } => {
                    let mut emitted_remove = false;

                    if let Some(old_oid) = resolve_order_ref(old_order_ref, Some(user.as_str()), cloid_to_oid) {
                        if let Some(old_state) = remove_live_order(orders, cloid_to_oid, old_oid) {
                            if emit_rows {
                                rows.push(OutputRow {
                                    block_number: i64::try_from(block_number)?,
                                    block_time_ms,
                                    coin: "BTC".to_owned(),
                                    user: old_state.user,
                                    oid: Some(i64::try_from(old_oid)?),
                                    side: Some(old_state.side),
                                    px: Some(old_state.px),
                                    diff_type: "remove".to_owned(),
                                    event: "cancel".to_owned(),
                                    sz: Some(Decimal::ZERO),
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
                                    status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                                    lifetime: clamp_lifetime_ms(old_state.created_time_ms, block_time_ms),
                                });
                            }
                            emitted_remove = true;
                        }
                    }

                    if emit_rows && !emitted_remove {
                        let fallback_oid = match old_order_ref {
                            OrderRef::Oid(oid) => Some(i64::try_from(*oid)?),
                            OrderRef::Cloid(_) => None,
                        };
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: user.clone(),
                            oid: fallback_oid,
                            side: None,
                            px: None,
                            diff_type: "remove".to_owned(),
                            event: "cancel".to_owned(),
                            sz: None,
                            orig_sz: None,
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
                            status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                            lifetime: None,
                        });
                    }

                    if !meta.is_trigger {
                        if let Some(new_oid) = new_oid {
                            known_non_trigger_oids.insert(*new_oid);
                            insert_live_order(
                                orders,
                                cloid_to_oid,
                                user,
                                *new_oid,
                                new_cloid.as_deref(),
                                block_time_ms,
                                side,
                                *px,
                                *sz,
                                meta,
                            );
                            if emit_rows {
                                push_add_row(
                                    rows,
                                    block_number,
                                    block_time_ms,
                                    user,
                                    *new_oid,
                                    side,
                                    *px,
                                    *sz,
                                    *raw_sz,
                                    meta,
                                )?;
                            }
                        }
                    }
                }
                CmdEvent::TrackTriggerAdd { user, oid, cloid } => {
                    insert_trigger_ref(trigger_refs, user, *oid, cloid.as_deref());
                }
                CmdEvent::TrackTriggerPending { user, cloid } => {
                    insert_pending_trigger_ref(trigger_refs, user, cloid.as_deref());
                }
                CmdEvent::TrackTriggerModify { user, old_order_ref, new_oid, new_cloid } => {
                    if let Some(old_oid) = resolve_trigger_ref(old_order_ref, Some(user.as_str()), trigger_refs) {
                        remove_trigger_ref(trigger_refs, old_oid);
                    } else if let OrderRef::Cloid(cloid) = old_order_ref {
                        remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid);
                    }
                    if let Some(new_oid) = new_oid {
                        insert_trigger_ref(trigger_refs, user, *new_oid, new_cloid.as_deref());
                    } else if new_cloid.is_some() {
                        insert_pending_trigger_ref(trigger_refs, user, new_cloid.as_deref());
                    }
                }
                CmdEvent::Skipped { user, oid, event, status } => {
                    if emit_rows {
                        let oid = oid.map(i64::try_from).transpose()?;
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: user.clone().unwrap_or_default(),
                            oid,
                            side: None,
                            px: None,
                            diff_type: "skip".to_owned(),
                            event: event.clone(),
                            sz: None,
                            orig_sz: None,
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
                            status: Some(status.clone()),
                            lifetime: None,
                        });
                    }
                }
            }

            if rows.len() >= ROW_GROUP_SIZE
                && let Some(batch) = rows.take_batch(schema)?
            {
                writer.write(&batch)?;
            }
        }

        for (user, order_ref, fallback_on_missing) in deferred_cancels.drain(..) {
            if let Some(trigger_oid) = resolve_trigger_ref(&order_ref, user.as_deref(), trigger_refs) {
                remove_trigger_ref(trigger_refs, trigger_oid);
                continue;
            }
            if let (Some(user), OrderRef::Cloid(cloid)) = (user.as_deref(), &order_ref)
                && remove_pending_trigger_ref_by_cloid(trigger_refs, user, cloid)
            {
                continue;
            }
            let resolved_oid = resolve_order_ref(&order_ref, user.as_deref(), cloid_to_oid);
            let mut emitted_remove = false;

            if let Some(oid) = resolved_oid {
                if canceled_oids_in_block.contains(&oid) {
                    continue;
                }
                if let Some(state) = remove_live_order(orders, cloid_to_oid, oid) {
                    canceled_oids_in_block.insert(oid);
                    if emit_rows {
                        rows.push(OutputRow {
                            block_number: i64::try_from(block_number)?,
                            block_time_ms,
                            coin: "BTC".to_owned(),
                            user: state.user,
                            oid: Some(i64::try_from(oid)?),
                            side: Some(state.side),
                            px: Some(state.px),
                            diff_type: "remove".to_owned(),
                            event: "cancel".to_owned(),
                            sz: Some(Decimal::ZERO),
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
                            status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                            lifetime: clamp_lifetime_ms(state.created_time_ms, block_time_ms),
                        });
                    }
                    emitted_remove = true;
                }
            }

            if !emitted_remove && fallback_on_missing {
                if let (Some(user), OrderRef::Oid(oid)) = (user.as_deref(), &order_ref)
                    && !known_non_trigger_oids.contains(oid)
                    && consume_pending_trigger_ref_for_user(trigger_refs, user)
                {
                    continue;
                }
            }

            if emit_rows && !emitted_remove && fallback_on_missing {
                rows.push(OutputRow {
                    block_number: i64::try_from(block_number)?,
                    block_time_ms,
                    coin: "BTC".to_owned(),
                    user: user.unwrap_or_default(),
                    oid: None,
                    side: None,
                    px: None,
                    diff_type: "remove".to_owned(),
                    event: "cancel".to_owned(),
                    sz: None,
                    orig_sz: None,
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
                    status: Some(CANCEL_STATUS_CANCELED.to_owned()),
                    lifetime: None,
                });
            }
        }
    }

    if let Some(fills) = fills {
        let taker_tids: HashSet<u64> = fills.iter().filter(|fill| fill.crossed).filter_map(|fill| fill.tid).collect();

        for fill in fills {
            let fill_time_ms = fill.block_time_ms;

            if fill.crossed {
                if rows.len() >= ROW_GROUP_SIZE
                    && let Some(batch) = rows.take_batch(schema)?
                {
                    writer.write(&batch)?;
                }
                continue;
            }

            let has_matching_taker = fill.tid.is_some_and(|tid| taker_tids.contains(&tid));

            if emit_rows && has_matching_taker {
                rows.push(OutputRow {
                    block_number: i64::try_from(fill.block_number)?,
                    block_time_ms: fill_time_ms,
                    coin: "BTC".to_owned(),
                    user: fill.user.clone(),
                    oid: Some(i64::try_from(fill.oid)?),
                    side: fill.side.clone(),
                    px: fill.px,
                    diff_type: "update".to_owned(),
                    event: "fill".to_owned(),
                    sz: None,
                    orig_sz: None,
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
            }

            let Some(state) = orders.get_mut(&fill.oid) else {
                if rows.len() >= ROW_GROUP_SIZE
                    && let Some(batch) = rows.take_batch(schema)?
                {
                    writer.write(&batch)?;
                }
                continue;
            };

            if state.remaining_sz <= Decimal::ZERO {
                remove_live_order(orders, cloid_to_oid, fill.oid);
                continue;
            }

            let old_remaining = state.remaining_sz;
            let mut new_remaining = old_remaining - fill.sz;
            if new_remaining < Decimal::ZERO {
                new_remaining = Decimal::ZERO;
            }

            state.remaining_sz = new_remaining;

            if new_remaining <= Decimal::ZERO {
                let removed_state = remove_live_order(orders, cloid_to_oid, fill.oid)
                    .ok_or_else(|| anyhow!("missing state for oid {} on remove", fill.oid))?;
                if emit_rows {
                    rows.push(OutputRow {
                        block_number: i64::try_from(fill.block_number)?,
                        block_time_ms: fill_time_ms,
                        coin: "BTC".to_owned(),
                        user: removed_state.user,
                        oid: Some(i64::try_from(fill.oid)?),
                        side: Some(removed_state.side),
                        px: Some(removed_state.px),
                        diff_type: "remove".to_owned(),
                        event: "fill".to_owned(),
                        sz: Some(Decimal::ZERO),
                        orig_sz: Some(old_remaining),
                        raw_sz: None,
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

fn finalize_output(writer: ArrowWriter<File>, rows: &mut RowBuffer, schema: &Arc<Schema>) -> Result<()> {
    let mut writer = writer;
    if let Some(batch) = rows.take_batch(schema)? {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let requester_pays = REQUESTER_PAYS_ALWAYS;
    eprintln!("[config] json_parser=simd-json");
    if args.start_height.is_some() ^ args.height_span.is_some() {
        bail!("--start-height and --height-span must be provided together");
    }
    let target_block_range = match (args.start_height, args.height_span) {
        (Some(start_height), Some(height_span)) => {
            if height_span == 0 {
                bail!("--height-span must be > 0");
            }
            let end_height = start_height
                .checked_add(height_span - 1)
                .ok_or_else(|| anyhow!("height overflow: start_height={start_height} span={height_span}"))?;
            Some((start_height, end_height))
        }
        _ => None,
    };

    let replica_source = InputSource::parse(&args.replica_cmds).context("parsing --replica_cmds")?;
    let node_fills_source = InputSource::parse(&args.node_fills_by_block).context("parsing --node_fills_by_block")?;
    let s3_client = if replica_source.requires_s3() || node_fills_source.requires_s3() {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
        Some(Arc::new(S3Client::new(&config)))
    } else {
        None
    };

    let replica_reader_source = match &replica_source {
        InputSource::LocalFile(path) => {
            if target_block_range.is_some() {
                bail!("--start-height/--height-span currently require S3 replica_cmds prefix input");
            }
            ReaderSource::LocalFile(path.clone())
        }
        InputSource::S3Prefix { bucket, prefix } => {
            let Some((start_height, end_height)) = target_block_range else {
                bail!("S3 replica_cmds requires --start-height and --height-span");
            };
            let height_span = end_height - start_height + 1;
            let Some(client) = s3_client.as_ref() else {
                bail!("S3 replica_cmds requires initialized S3 client");
            };
            let keys =
                resolve_replica_keys_for_range(client, bucket, prefix, start_height, height_span, requester_pays)
                    .await
                    .context("resolving replica_cmds keys from S3 layout")?;
            ReaderSource::S3Keys { bucket: bucket.clone(), keys }
        }
    };

    let fills_reader_source = match &node_fills_source {
        InputSource::LocalFile(path) => ReaderSource::LocalFile(path.clone()),
        InputSource::S3Prefix { bucket, prefix } => {
            let first_replica_ts_ms =
                first_replica_timestamp_ms(replica_reader_source.clone(), s3_client.clone(), requester_pays)
                    .await
                    .context("reading first replica_cmds timestamp")?;
            ReaderSource::S3HourlyFrom {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                next_hour_ms: floor_hour_ms(first_replica_ts_ms),
            }
        }
    };

    let mut fills_reader =
        NodeFillsReader::new(fills_reader_source.clone(), s3_client.clone(), requester_pays, target_block_range)
            .await
            .context("opening node_fills_by_block")?;
    let mut replica_reader = ReplicaCmdsReader::new(
        replica_reader_source,
        fills_reader_source.clone(),
        args.block_time_tolerance_ns,
        s3_client.clone(),
        requester_pays,
        target_block_range,
    )
    .await
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
    let mut cloid_to_oid: HashMap<UserCloidKey, u64> = HashMap::new();
    let mut trigger_refs = TriggerOrderRefs::default();
    let mut known_non_trigger_oids: HashSet<u64> = HashSet::new();
    let started_at = Instant::now();
    let mut processed_blocks: u64 = 0;
    eprintln!("[phase] processing blocks ...");
    let mut next_replica = replica_reader.next_batch().await?;
    let mut next_fills = fills_reader.next_batch().await?;

    while next_replica.is_some() || next_fills.is_some() {
        match (&next_replica, &next_fills) {
            (Some(replica), Some(fills)) if replica.block_number == fills.block_number => {
                let block_time_delta = (replica.block_time_ms - fills.block_time_ms).abs();
                if block_time_delta > args.block_time_match_tolerance_ms {
                    finalize_output(writer.take().unwrap(), &mut rows, &schema)?;
                    bail!(
                        "block {} timestamp mismatch: replica={}ms fills={}ms delta={}ms tolerance={}ms",
                        replica.block_number,
                        replica.block_time_ms,
                        fills.block_time_ms,
                        block_time_delta,
                        args.block_time_match_tolerance_ms
                    );
                }

                let replica = next_replica.take().unwrap();
                let fills = next_fills.take().unwrap();
                let emit_rows =
                    target_block_range.map(|(start_height, _)| replica.block_number >= start_height).unwrap_or(true);
                process_block(
                    replica.block_number,
                    replica.block_time_ms,
                    Some(replica.events),
                    Some(fills.fills),
                    emit_rows,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    &mut trigger_refs,
                    &mut known_non_trigger_oids,
                    writer.as_mut().unwrap(),
                    &schema,
                )?;
                processed_blocks += 1;
                if processed_blocks % 100 == 0 {
                    eprintln!(
                        "[progress] blocks={} last_block={} live_orders={} buffered_rows={} elapsed={:.1}s",
                        processed_blocks,
                        replica.block_number,
                        orders.len(),
                        rows.len(),
                        started_at.elapsed().as_secs_f64()
                    );
                }
                next_replica = replica_reader.next_batch().await?;
                next_fills = fills_reader.next_batch().await?;
            }
            (Some(replica), Some(fills)) if replica.block_number < fills.block_number => {
                let replica = next_replica.take().unwrap();
                let emit_rows =
                    target_block_range.map(|(start_height, _)| replica.block_number >= start_height).unwrap_or(true);
                process_block(
                    replica.block_number,
                    replica.block_time_ms,
                    Some(replica.events),
                    None,
                    emit_rows,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    &mut trigger_refs,
                    &mut known_non_trigger_oids,
                    writer.as_mut().unwrap(),
                    &schema,
                )?;
                processed_blocks += 1;
                if processed_blocks % 100 == 0 {
                    eprintln!(
                        "[progress] blocks={} last_block={} live_orders={} buffered_rows={} elapsed={:.1}s",
                        processed_blocks,
                        replica.block_number,
                        orders.len(),
                        rows.len(),
                        started_at.elapsed().as_secs_f64()
                    );
                }
                next_replica = replica_reader.next_batch().await?;
            }
            (Some(_), Some(_)) => {
                let fills = next_fills.take().unwrap();
                let emit_rows =
                    target_block_range.map(|(start_height, _)| fills.block_number >= start_height).unwrap_or(true);
                process_block(
                    fills.block_number,
                    fills.block_time_ms,
                    None,
                    Some(fills.fills),
                    emit_rows,
                    &mut rows,
                    &mut orders,
                    &mut cloid_to_oid,
                    &mut trigger_refs,
                    &mut known_non_trigger_oids,
                    writer.as_mut().unwrap(),
                    &schema,
                )?;
                processed_blocks += 1;
                if processed_blocks % 100 == 0 {
                    eprintln!(
                        "[progress] blocks={} last_block={} live_orders={} buffered_rows={} elapsed={:.1}s",
                        processed_blocks,
                        fills.block_number,
                        orders.len(),
                        rows.len(),
                        started_at.elapsed().as_secs_f64()
                    );
                }
                next_fills = fills_reader.next_batch().await?;
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
        "Wrote BTC diff parquet to {}. requester_pays={}. Remaining live orders in memory at EOF: {}",
        args.output_parquet.display(),
        requester_pays,
        orders.len()
    );

    Ok(())
}
