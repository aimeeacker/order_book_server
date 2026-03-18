#![allow(unsafe_op_in_unsafe_fn)]
#![allow(non_local_definitions, unused_qualifications)]

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use clap as _;
use pyo3 as _;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rmp_serde as _;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde as _;
use serde::{Deserialize, Serialize, de::IgnoredAny};
use serde_json as _;
use simd_json as _;

#[derive(Debug, Clone, Default)]
pub struct ComputeOptions {
    pub include_users: bool,
    pub include_trigger_orders: bool,
    pub assets: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct AppError(String);

pub type AppResult<T> = Result<T, AppError>;
pub const DEFAULT_DATASET_DIR: &str = "/home/aimee/hl_runtime/hl/dataset";
const SEGMENT_MAGIC: &[u8; 8] = b"L4SEG001";
const SEGMENT_VERSION: u32 = 2;
const ZSTD_LEVEL: i32 = 3;
const CODEC_ZSTD: &str = "zstd";
const CODEC_MSGPACK: &str = "msgpack";
const SNAPSHOT_INDEX_DB_FILENAME: &str = "snapshot_index.sqlite";
static DATASET_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<rmp_serde::decode::Error> for AppError {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<rmp_serde::encode::Error> for AppError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<simd_json::Error> for AppError {
    fn from(value: simd_json::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(value: rusqlite::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<std::string::FromUtf8Error> for AppError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Deserialize)]
struct Root {
    exchange: Exchange,
}

#[derive(Debug, Deserialize)]
struct Exchange {
    locus: Locus,
    perp_dexs: Vec<PerpDex>,
    spot_books: Vec<Book>,
}

#[derive(Debug, Deserialize)]
struct Locus {
    cls: Vec<PerpLocus>,
    scl: SpotLocus,
    ctx: LocusContext,
}

#[derive(Debug, Deserialize)]
struct LocusContext {
    height: u64,
    time: String,
}

#[derive(Debug, Deserialize)]
struct PerpLocus {
    meta: PerpMeta,
}

#[derive(Debug, Deserialize)]
struct PerpMeta {
    universe: Vec<AssetMeta>,
}

#[derive(Debug, Deserialize)]
struct AssetMeta {
    name: String,
    #[serde(rename = "szDecimals")]
    sz_decimals: u32,
}

#[derive(Debug, Deserialize)]
struct SpotLocus {
    meta: SpotMeta,
}

#[derive(Debug, Deserialize)]
struct SpotMeta {
    spot_infos: Vec<SpotInfo>,
    token_infos: Vec<TokenInfo>,
}

#[derive(Debug, Deserialize)]
struct SpotInfo {
    tokens: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct TokenInfo {
    spec: TokenSpec,
}

#[derive(Debug, Deserialize)]
struct TokenSpec {
    name: String,
    #[serde(rename = "szDecimals")]
    sz_decimals: u32,
    #[serde(rename = "weiDecimals")]
    wei_decimals: u32,
}

#[derive(Debug, Deserialize)]
struct PerpDex {
    books: Vec<Book>,
}

#[derive(Debug, Deserialize)]
struct Book {
    asset: u64,
    halfs: Vec<Half>,
    book_orders: HashMap<u64, BookOrder>,
    #[serde(default)]
    user_states: Vec<(String, UserState)>,
}

#[derive(Debug, Deserialize)]
struct Half {
    levels: Vec<(IgnoredAny, LevelEndpoints)>,
}

#[derive(Debug, Deserialize)]
struct LevelEndpoints {
    f: u64,
    l: u64,
}

#[derive(Debug, Deserialize)]
struct BookOrder {
    #[serde(default)]
    n: Option<u64>,
    #[serde(rename = "r")]
    resting_sz: u64,
    #[serde(rename = "O", default)]
    orig_sz: Option<u64>,
    #[serde(rename = "u")]
    user: String,
    #[serde(rename = "t", default)]
    tif: Option<String>,
    #[serde(rename = "c")]
    core: OrderCore,
    #[serde(rename = "o")]
    oid: u64,
    #[serde(rename = "i", default)]
    is_position_tpsl: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OrderCore {
    #[serde(rename = "t")]
    timestamp: u64,
    #[serde(rename = "s")]
    side: String,
    #[serde(rename = "l")]
    limit_px: u64,
    #[serde(rename = "r", default)]
    reduce_only: Option<bool>,
    #[serde(rename = "d", default)]
    order_type_code: Option<String>,
    #[serde(rename = "c", default)]
    cloid: Option<String>,
    #[serde(rename = "R", default)]
    relationships: Option<Relationships>,
}

#[derive(Debug, Deserialize, Default)]
struct Relationships {
    #[serde(default)]
    c: Vec<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct UserState {
    #[serde(rename = "u", default)]
    untriggered_orders: Vec<(u64, TriggerOrder)>,
}

#[derive(Debug, Deserialize)]
struct TriggerOrder {
    core: TriggerCore,
    trigger: TriggerMeta,
}

#[derive(Debug, Deserialize)]
struct TriggerCore {
    #[serde(rename = "t")]
    timestamp: u64,
    #[serde(rename = "S", default)]
    size: Option<u64>,
    #[serde(rename = "r", default)]
    reduce_only: Option<bool>,
    #[serde(rename = "d", default)]
    order_type_code: Option<String>,
    #[serde(rename = "s")]
    side: String,
    #[serde(rename = "l")]
    limit_px: u64,
    #[serde(rename = "c", default)]
    cloid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TriggerMeta {
    #[serde(rename = "triggerPx")]
    trigger_px: u64,
}

#[derive(Clone, Debug)]
enum BookSource {
    Perp { dex_index: usize, book_index: usize },
    Spot { book_index: usize },
}

#[derive(Clone, Debug)]
struct BookSpec {
    name: String,
    sz_decimals: u32,
    price_scale: u32,
    source: BookSource,
}

#[derive(Debug, Clone, Copy)]
struct TriggerRecord<'a> {
    oid: u64,
    user: &'a str,
    entry: &'a TriggerOrder,
    is_position_tpsl: bool,
}

struct BookContext<'a> {
    book_orders: &'a HashMap<u64, BookOrder>,
    trigger_records: HashMap<u64, TriggerRecord<'a>>,
    trigger_sequence: Vec<TriggerRecord<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotOrder {
    coin: String,
    side: String,
    #[serde(rename = "limitPx")]
    limit_px: String,
    sz: String,
    oid: u64,
    timestamp: u64,
    #[serde(rename = "triggerCondition")]
    trigger_condition: String,
    #[serde(rename = "isTrigger")]
    is_trigger: bool,
    #[serde(rename = "triggerPx")]
    trigger_px: String,
    children: Vec<SnapshotOrder>,
    #[serde(rename = "isPositionTpsl")]
    is_position_tpsl: bool,
    #[serde(rename = "reduceOnly")]
    reduce_only: bool,
    #[serde(rename = "orderType")]
    order_type: String,
    #[serde(rename = "origSz")]
    orig_sz: String,
    tif: Option<String>,
    cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserWrappedOrder(String, SnapshotOrder);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum OutputOrderEntry {
    Plain(SnapshotOrder),
    WithUser(UserWrappedOrder),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LevelsPayload {
    block_height: u64,
    block_time: String,
    levels: [Vec<OutputOrderEntry>; 2],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TriggerPayload {
    block_height: u64,
    block_time: String,
    book_orders: [Vec<OutputOrderEntry>; 2],
    untriggered_orders: Vec<OutputOrderEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum BookPayload {
    Levels(LevelsPayload),
    Trigger(TriggerPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BookSnapshotEntry(String, BookPayload);

type SnapshotDocument = Vec<BookSnapshotEntry>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointRecord {
    block_height: u64,
    block_time: String,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
    snapshot: SnapshotDocument,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentHeader {
    version: u32,
    segment_start: u64,
    segment_end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentIndexEntry {
    block_height: u64,
    block_time: String,
    offset: u64,
    len: u64,
    #[serde(default = "default_codec")]
    codec: String,
    #[serde(default)]
    compression_level: Option<i32>,
    #[serde(default)]
    raw_len: Option<u64>,
    asset_count: usize,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct CheckpointWriteResult {
    pub block_height: u64,
    pub block_time: String,
    pub segment_path: PathBuf,
    pub segment_start: u64,
    pub segment_end: u64,
    pub snapshot_index_path: PathBuf,
    pub checkpoint_count: usize,
    pub asset_count: usize,
    pub codec: String,
    pub compression_level: Option<i32>,
    pub raw_len: u64,
    pub stored_len: u64,
}

pub fn compute_l4_to_file(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    options: &ComputeOptions,
) -> AppResult<()> {
    let (root, specs) = load_root_and_specs(input, options)?;

    let output = File::create(output)?;
    let mut writer = BufWriter::new(output);
    render_snapshot(&mut writer, &root.exchange, &specs, options)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

pub fn compute_l4_json(input: impl AsRef<Path>, options: &ComputeOptions) -> AppResult<String> {
    let (root, specs) = load_root_and_specs(input, options)?;

    let mut output = Vec::new();
    render_snapshot(&mut output, &root.exchange, &specs, options)?;
    output.push(b'\n');
    Ok(String::from_utf8(output)?)
}

pub fn append_l4_checkpoint(
    input: impl AsRef<Path>,
    output_dir: Option<PathBuf>,
    options: &ComputeOptions,
) -> AppResult<CheckpointWriteResult> {
    let (root, specs) = load_root_and_specs(input, options)?;
    let block_height = root.exchange.locus.ctx.height;
    let block_time = root.exchange.locus.ctx.time.clone();
    let snapshot = build_snapshot_document(&root.exchange, &specs, options)?;
    let record = CheckpointRecord {
        block_height,
        block_time: block_time.clone(),
        include_users: options.include_users,
        include_trigger_orders: options.include_trigger_orders,
        assets: options.assets.clone(),
        snapshot,
    };
    write_checkpoint_record(output_dir.unwrap_or_else(current_dataset_dir), record, true)
}

pub fn append_l4_checkpoint_from_snapshot_json(
    input: impl AsRef<Path>,
    output_dir: Option<PathBuf>,
    options: &ComputeOptions,
    block_time: impl Into<String>,
) -> AppResult<CheckpointWriteResult> {
    let bytes = fs::read(input)?;
    let record = checkpoint_record_from_snapshot_json_bytes(&bytes, options, block_time.into())?;
    write_checkpoint_record(output_dir.unwrap_or_else(current_dataset_dir), record, false)
}

fn load_root_and_specs(input: impl AsRef<Path>, options: &ComputeOptions) -> AppResult<(Root, Vec<BookSpec>)> {
    let bytes = fs::read(input)?;
    let root: Root = rmp_serde::from_slice(&bytes)?;
    let specs = select_specs(collect_book_specs(&root.exchange)?, options)?;
    Ok((root, specs))
}

fn default_dataset_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DATASET_DIR)
}

fn default_codec() -> String {
    CODEC_MSGPACK.to_string()
}

fn dataset_dir_override() -> &'static Mutex<Option<PathBuf>> {
    DATASET_DIR_OVERRIDE.get_or_init(|| Mutex::new(None))
}

pub fn current_dataset_dir() -> PathBuf {
    dataset_dir_override().lock().expect("dataset dir mutex").clone().unwrap_or_else(default_dataset_dir)
}

pub fn set_current_dataset_dir(path: Option<PathBuf>) -> PathBuf {
    let mut guard = dataset_dir_override().lock().expect("dataset dir mutex");
    *guard = path;
    guard.clone().unwrap_or_else(default_dataset_dir)
}

fn select_specs(mut specs: Vec<BookSpec>, options: &ComputeOptions) -> AppResult<Vec<BookSpec>> {
    specs.sort_by(|lhs, rhs| lhs.name.cmp(&rhs.name));

    if let Some(assets) = options.assets.as_ref() {
        specs.retain(|spec| assets.iter().any(|asset| spec.name == *asset));
        if specs.is_empty() {
            return Err(AppError(format!("asset filter matched no books: {}", assets.join(","))));
        }
    }

    Ok(specs)
}

fn render_snapshot<W: Write>(
    writer: &mut W,
    exchange: &Exchange,
    specs: &[BookSpec],
    options: &ComputeOptions,
) -> AppResult<()> {
    let block_height = exchange.locus.ctx.height;
    let block_time = &exchange.locus.ctx.time;
    writer.write_all(b"[")?;
    let mut first_book = true;

    for spec in specs {
        if !first_book {
            writer.write_all(b",")?;
        }
        first_book = false;

        let book = book_value(exchange, &spec.source)?;
        let context = build_book_context(book);
        writer.write_all(b"[")?;
        serde_json::to_writer(&mut *writer, &spec.name)?;
        if options.include_trigger_orders {
            writer.write_all(br#",{"block_height":"#)?;
            serde_json::to_writer(&mut *writer, &block_height)?;
            writer.write_all(br#","block_time":"#)?;
            serde_json::to_writer(&mut *writer, block_time)?;
            writer.write_all(br#","book_orders":["#)?;
            render_side(writer, book, spec, &context, options, 0)?;
            writer.write_all(b",")?;
            render_side(writer, book, spec, &context, options, 1)?;
            writer.write_all(b"],\"untriggered_orders\":")?;
            render_untriggered_orders(writer, spec, &context, options)?;
            writer.write_all(b"}]")?;
        } else {
            writer.write_all(br#",{"block_height":"#)?;
            serde_json::to_writer(&mut *writer, &block_height)?;
            writer.write_all(br#","block_time":"#)?;
            serde_json::to_writer(&mut *writer, block_time)?;
            writer.write_all(br#","levels":["#)?;
            render_side(writer, book, spec, &context, options, 0)?;
            writer.write_all(b",")?;
            render_side(writer, book, spec, &context, options, 1)?;
            writer.write_all(b"]}]")?;
        }
    }

    writer.write_all(b"]")?;
    Ok(())
}

fn render_side<W: Write>(
    writer: &mut W,
    book: &Book,
    spec: &BookSpec,
    context: &BookContext<'_>,
    options: &ComputeOptions,
    side_index: usize,
) -> AppResult<()> {
    writer.write_all(b"[")?;
    let mut first_item = true;

    let half = book.halfs.get(side_index).ok_or_else(|| AppError(format!("missing half side index {side_index}")))?;

    for (_, endpoints) in &half.levels {
        let mut current_idx = endpoints.f;
        loop {
            let order = context
                .book_orders
                .get(&current_idx)
                .ok_or_else(|| AppError(format!("missing book order index {current_idx}")))?;
            let snapshot = build_visible_order(spec, order, &context.trigger_records)?;
            if options.include_users {
                write_json_value(writer, &mut first_item, &UserWrappedOrder(order.user.clone(), snapshot))?;
            } else {
                write_json_value(writer, &mut first_item, &snapshot)?;
            }

            if current_idx == endpoints.l {
                break;
            }
            current_idx = order.n.ok_or_else(|| AppError(format!("missing next order index from {current_idx}")))?;
        }
    }

    writer.write_all(b"]")?;
    Ok(())
}

fn render_untriggered_orders<W: Write>(
    writer: &mut W,
    spec: &BookSpec,
    context: &BookContext<'_>,
    options: &ComputeOptions,
) -> AppResult<()> {
    writer.write_all(b"[")?;
    let mut first_item = true;
    for record in &context.trigger_sequence {
        let snapshot = build_trigger_order(spec, record)?;
        if options.include_users {
            write_json_value(writer, &mut first_item, &UserWrappedOrder(record.user.to_owned(), snapshot))?;
        } else {
            write_json_value(writer, &mut first_item, &snapshot)?;
        }
    }
    writer.write_all(b"]")?;
    Ok(())
}

fn build_snapshot_document(
    exchange: &Exchange,
    specs: &[BookSpec],
    options: &ComputeOptions,
) -> AppResult<SnapshotDocument> {
    let block_height = exchange.locus.ctx.height;
    let block_time = exchange.locus.ctx.time.clone();
    let mut out = Vec::with_capacity(specs.len());

    for spec in specs {
        let book = book_value(exchange, &spec.source)?;
        let context = build_book_context(book);
        if options.include_trigger_orders {
            out.push(BookSnapshotEntry(
                spec.name.clone(),
                BookPayload::Trigger(TriggerPayload {
                    block_height,
                    block_time: block_time.clone(),
                    book_orders: [
                        build_side_entries(book, spec, &context, options, 0)?,
                        build_side_entries(book, spec, &context, options, 1)?,
                    ],
                    untriggered_orders: build_untriggered_entries(spec, &context, options)?,
                }),
            ));
        } else {
            out.push(BookSnapshotEntry(
                spec.name.clone(),
                BookPayload::Levels(LevelsPayload {
                    block_height,
                    block_time: block_time.clone(),
                    levels: [
                        build_side_entries(book, spec, &context, options, 0)?,
                        build_side_entries(book, spec, &context, options, 1)?,
                    ],
                }),
            ));
        }
    }

    Ok(out)
}

fn build_side_entries(
    book: &Book,
    spec: &BookSpec,
    context: &BookContext<'_>,
    options: &ComputeOptions,
    side_index: usize,
) -> AppResult<Vec<OutputOrderEntry>> {
    let half = book.halfs.get(side_index).ok_or_else(|| AppError(format!("missing half side index {side_index}")))?;
    let mut out = Vec::new();

    for (_, endpoints) in &half.levels {
        let mut current_idx = endpoints.f;
        loop {
            let order = context
                .book_orders
                .get(&current_idx)
                .ok_or_else(|| AppError(format!("missing book order index {current_idx}")))?;
            let snapshot = build_visible_order(spec, order, &context.trigger_records)?;
            out.push(wrap_output_entry(order.user.as_str(), snapshot, options.include_users));

            if current_idx == endpoints.l {
                break;
            }
            current_idx = order.n.ok_or_else(|| AppError(format!("missing next order index from {current_idx}")))?;
        }
    }

    Ok(out)
}

fn build_untriggered_entries(
    spec: &BookSpec,
    context: &BookContext<'_>,
    options: &ComputeOptions,
) -> AppResult<Vec<OutputOrderEntry>> {
    let mut out = Vec::with_capacity(context.trigger_sequence.len());
    for record in &context.trigger_sequence {
        let snapshot = build_trigger_order(spec, record)?;
        out.push(wrap_output_entry(record.user, snapshot, options.include_users));
    }
    Ok(out)
}

fn wrap_output_entry(user: &str, snapshot: SnapshotOrder, include_users: bool) -> OutputOrderEntry {
    if include_users {
        OutputOrderEntry::WithUser(UserWrappedOrder(user.to_owned(), snapshot))
    } else {
        OutputOrderEntry::Plain(snapshot)
    }
}

fn build_book_context(book: &Book) -> BookContext<'_> {
    let (trigger_records, trigger_sequence) = build_trigger_records(book);
    BookContext { book_orders: &book.book_orders, trigger_records, trigger_sequence }
}

fn build_visible_order(
    spec: &BookSpec,
    order: &BookOrder,
    trigger_records: &HashMap<u64, TriggerRecord<'_>>,
) -> AppResult<SnapshotOrder> {
    let children = child_orders(spec, order, trigger_records)?;

    Ok(SnapshotOrder {
        coin: spec.name.clone(),
        side: order.core.side.clone(),
        limit_px: scaled_to_string(order.core.limit_px, spec.price_scale),
        sz: scaled_to_string(order.resting_sz, spec.sz_decimals),
        oid: order.oid,
        timestamp: order.core.timestamp,
        trigger_condition: if order.core.order_type_code.is_some() {
            "Triggered".to_string()
        } else {
            "N/A".to_string()
        },
        is_trigger: false,
        trigger_px: "0.0".to_string(),
        children,
        is_position_tpsl: order.is_position_tpsl.unwrap_or(false),
        reduce_only: order.core.reduce_only.unwrap_or(false),
        order_type: order_type_from_code(order.core.order_type_code.as_deref()),
        orig_sz: scaled_to_string(order.orig_sz.unwrap_or(0), spec.sz_decimals),
        tif: order.tif.clone(),
        cloid: order.core.cloid.clone(),
    })
}

fn child_orders(
    spec: &BookSpec,
    order: &BookOrder,
    trigger_records: &HashMap<u64, TriggerRecord<'_>>,
) -> AppResult<Vec<SnapshotOrder>> {
    let Some(relationships) = order.core.relationships.as_ref() else {
        return Ok(Vec::new());
    };

    let mut children = Vec::with_capacity(relationships.c.len());
    for oid in &relationships.c {
        if let Some(record) = trigger_records.get(oid) {
            children.push(build_trigger_order(spec, record)?);
        }
    }
    Ok(children)
}

fn build_trigger_order(spec: &BookSpec, record: &TriggerRecord<'_>) -> AppResult<SnapshotOrder> {
    let core = &record.entry.core;
    let trigger = &record.entry.trigger;

    Ok(SnapshotOrder {
        coin: spec.name.clone(),
        side: core.side.clone(),
        limit_px: scaled_to_string(core.limit_px, spec.price_scale),
        sz: scaled_to_string(core.size.unwrap_or(0), spec.sz_decimals),
        oid: record.oid,
        timestamp: core.timestamp,
        trigger_condition: trigger_condition(
            &core.side,
            core.order_type_code.as_deref(),
            trigger.trigger_px,
            spec.price_scale,
        ),
        is_trigger: true,
        trigger_px: scaled_to_string(trigger.trigger_px, spec.price_scale),
        children: Vec::new(),
        is_position_tpsl: record.is_position_tpsl,
        reduce_only: core.reduce_only.unwrap_or(false),
        order_type: order_type_from_code(core.order_type_code.as_deref()),
        orig_sz: scaled_to_string(core.size.unwrap_or(0), spec.sz_decimals),
        tif: None,
        cloid: core.cloid.clone(),
    })
}

fn build_trigger_records<'a>(book: &'a Book) -> (HashMap<u64, TriggerRecord<'a>>, Vec<TriggerRecord<'a>>) {
    let mut records = HashMap::new();
    let mut sequence = Vec::new();
    for (user, state) in &book.user_states {
        for (oid, entry) in &state.untriggered_orders {
            let record = TriggerRecord { oid: *oid, user, entry, is_position_tpsl: entry.core.size.is_none() };
            records.insert(*oid, record);
            sequence.push(record);
        }
    }
    (records, sequence)
}

fn normalize_output_entry(entry: OutputOrderEntry, include_users: bool) -> OutputOrderEntry {
    match (include_users, entry) {
        (true, entry) => entry,
        (false, OutputOrderEntry::Plain(order)) => OutputOrderEntry::Plain(order),
        (false, OutputOrderEntry::WithUser(UserWrappedOrder(_, order))) => OutputOrderEntry::Plain(order),
    }
}

fn checkpoint_record_from_snapshot_json_bytes(
    bytes: &[u8],
    options: &ComputeOptions,
    block_time: String,
) -> AppResult<CheckpointRecord> {
    if options.include_trigger_orders {
        return Err(AppError(
            "snapshot json checkpoint append does not support include_trigger_orders=true".to_string(),
        ));
    }

    let mut owned = bytes.to_vec();
    #[allow(clippy::type_complexity)]
    let (block_height, snapshot): (u64, Vec<(String, [Vec<OutputOrderEntry>; 2])>) = simd_json::from_slice(&mut owned)?;
    let assets = options.assets.clone();
    let filtered = snapshot
        .into_iter()
        .filter(|(coin, _)| assets.as_ref().is_none_or(|items| items.iter().any(|asset| asset == coin)))
        .map(|(coin, levels)| {
            let levels = levels.map(|side| {
                side.into_iter().map(|entry| normalize_output_entry(entry, options.include_users)).collect::<Vec<_>>()
            });
            (coin, BookPayload::Levels(LevelsPayload { block_height, block_time: block_time.clone(), levels }))
        })
        .map(|(coin, payload)| BookSnapshotEntry(coin, payload))
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        return Err(AppError("asset filter matched no books in snapshot json".to_string()));
    }

    Ok(CheckpointRecord {
        block_height,
        block_time: match &filtered[0] {
            BookSnapshotEntry(_, BookPayload::Levels(levels)) => levels.block_time.clone(),
            BookSnapshotEntry(_, BookPayload::Trigger(trigger)) => trigger.block_time.clone(),
        },
        include_users: options.include_users,
        include_trigger_orders: false,
        assets: options.assets.clone(),
        snapshot: filtered,
    })
}

fn write_checkpoint_record(
    output_dir: PathBuf,
    record: CheckpointRecord,
    enforce_10k_boundary: bool,
) -> AppResult<CheckpointWriteResult> {
    fs::create_dir_all(&output_dir)?;
    validate_checkpoint_height(record.block_height, enforce_10k_boundary)?;
    let (segment_start, segment_end) = checkpoint_segment_bounds(record.block_height);
    let segment_path = output_dir.join(format!("segment_{segment_start}_{segment_end}.l4seg"));

    let mut file = OpenOptions::new().create(true).read(true).append(true).open(&segment_path)?;
    ensure_segment_header(&mut file, segment_start, segment_end)?;

    let payload = rmp_serde::to_vec(&record)?;
    let raw_len = u64::try_from(payload.len()).map_err(|_| AppError("checkpoint payload too large".to_string()))?;
    let payload = zstd::stream::encode_all(Cursor::new(payload), ZSTD_LEVEL)?;
    let len =
        u64::try_from(payload.len()).map_err(|_| AppError("compressed checkpoint payload too large".to_string()))?;
    let prefix_offset = file.seek(SeekFrom::End(0))?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&payload)?;
    file.flush()?;
    let snapshot_index_path = output_dir.join(SNAPSHOT_INDEX_DB_FILENAME);
    let entry = SegmentIndexEntry {
        block_height: record.block_height,
        block_time: record.block_time.clone(),
        offset: prefix_offset + 8,
        len,
        codec: CODEC_ZSTD.to_string(),
        compression_level: Some(ZSTD_LEVEL),
        raw_len: Some(raw_len),
        asset_count: record.snapshot.len(),
        include_users: record.include_users,
        include_trigger_orders: record.include_trigger_orders,
        assets: record.assets.clone(),
    };
    let checkpoint_count =
        insert_checkpoint_manifest_entry(&snapshot_index_path, segment_start, segment_end, &segment_path, &entry)?;

    Ok(CheckpointWriteResult {
        block_height: record.block_height,
        block_time: record.block_time,
        segment_path,
        segment_start,
        segment_end,
        snapshot_index_path,
        checkpoint_count,
        asset_count: record.snapshot.len(),
        codec: CODEC_ZSTD.to_string(),
        compression_level: Some(ZSTD_LEVEL),
        raw_len,
        stored_len: len,
    })
}

fn validate_checkpoint_height(block_height: u64, enforce_10k_boundary: bool) -> AppResult<()> {
    if block_height == 0 {
        return Err(AppError("checkpoint height must be positive".to_string()));
    }
    if enforce_10k_boundary && block_height % 10_000 != 0 {
        return Err(AppError(format!("checkpoint height must be a positive multiple of 10000, got {block_height}")));
    }
    Ok(())
}

fn checkpoint_segment_bounds(block_height: u64) -> (u64, u64) {
    let segment_end =
        if block_height % 1_000_000 == 0 { block_height } else { ((block_height / 1_000_000) + 1) * 1_000_000 };
    let segment_start = segment_end - 990_000;
    (segment_start, segment_end)
}

fn ensure_segment_header(file: &mut File, segment_start: u64, segment_end: u64) -> AppResult<()> {
    if file.metadata()?.len() == 0 {
        let header = SegmentHeader { version: SEGMENT_VERSION, segment_start, segment_end };
        let header_bytes = rmp_serde::to_vec(&header)?;
        let header_len =
            u32::try_from(header_bytes.len()).map_err(|_| AppError("segment header too large".to_string()))?;
        file.write_all(SEGMENT_MAGIC)?;
        file.write_all(&header_len.to_le_bytes())?;
        file.write_all(&header_bytes)?;
        file.flush()?;
        return Ok(());
    }

    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != SEGMENT_MAGIC {
        return Err(AppError("invalid segment magic".to_string()));
    }
    let mut header_len_bytes = [0_u8; 4];
    file.read_exact(&mut header_len_bytes)?;
    let header_len = u32::from_le_bytes(header_len_bytes) as usize;
    let mut header_bytes = vec![0_u8; header_len];
    file.read_exact(&mut header_bytes)?;
    let header: SegmentHeader = rmp_serde::from_slice(&header_bytes)?;
    if header.segment_start != segment_start || header.segment_end != segment_end {
        return Err(AppError(format!(
            "segment bounds mismatch: existing {}-{}, expected {}-{}",
            header.segment_start, header.segment_end, segment_start, segment_end
        )));
    }
    file.seek(SeekFrom::End(0))?;
    Ok(())
}

fn open_snapshot_index_db(path: &Path) -> AppResult<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.execute_batch(
        "BEGIN;
         CREATE TABLE IF NOT EXISTS manifest_meta (
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS segment_summaries (
             segment_start INTEGER NOT NULL,
             segment_end INTEGER NOT NULL,
             segment_path TEXT NOT NULL,
             checkpoint_count INTEGER NOT NULL,
             min_block_height INTEGER,
             max_block_height INTEGER,
             PRIMARY KEY (segment_start, segment_end)
         );
         CREATE TABLE IF NOT EXISTS checkpoint_entries (
             block_height INTEGER PRIMARY KEY,
             block_time TEXT NOT NULL,
             segment_start INTEGER NOT NULL,
             segment_end INTEGER NOT NULL,
             segment_path TEXT NOT NULL,
             offset INTEGER NOT NULL,
             len INTEGER NOT NULL,
             codec TEXT NOT NULL,
             compression_level INTEGER,
             raw_len INTEGER,
             asset_count INTEGER NOT NULL,
             include_users INTEGER NOT NULL,
             include_trigger_orders INTEGER NOT NULL,
             assets_json TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_checkpoint_entries_segment
         ON checkpoint_entries (segment_start, segment_end, block_height);
         COMMIT;",
    )?;
    conn.execute(
        "INSERT INTO manifest_meta(key, value) VALUES('version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![SEGMENT_VERSION.to_string()],
    )?;
    Ok(conn)
}

fn insert_checkpoint_manifest_entry(
    manifest_path: &Path,
    segment_start: u64,
    segment_end: u64,
    segment_path: &Path,
    entry: &SegmentIndexEntry,
) -> AppResult<usize> {
    let mut conn = open_snapshot_index_db(manifest_path)?;
    let tx = conn.transaction()?;
    ensure_checkpoint_absent(&tx, entry.block_height, segment_path)?;
    let assets_json = entry.assets.as_ref().map(serde_json::to_string).transpose()?;
    tx.execute(
        "INSERT INTO checkpoint_entries (
             block_height, block_time, segment_start, segment_end, segment_path,
             offset, len, codec, compression_level, raw_len, asset_count,
             include_users, include_trigger_orders, assets_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            entry.block_height,
            entry.block_time,
            segment_start,
            segment_end,
            segment_path.to_string_lossy().into_owned(),
            entry.offset,
            entry.len,
            entry.codec,
            entry.compression_level,
            entry.raw_len,
            i64::try_from(entry.asset_count).map_err(|_| AppError("asset_count overflow".to_string()))?,
            entry.include_users,
            entry.include_trigger_orders,
            assets_json,
        ],
    )?;
    tx.execute(
        "INSERT INTO segment_summaries (
             segment_start, segment_end, segment_path, checkpoint_count, min_block_height, max_block_height
         ) VALUES (?1, ?2, ?3, 1, ?4, ?4)
         ON CONFLICT(segment_start, segment_end) DO UPDATE SET
             segment_path = excluded.segment_path,
             checkpoint_count = segment_summaries.checkpoint_count + 1,
             min_block_height = MIN(segment_summaries.min_block_height, excluded.min_block_height),
             max_block_height = MAX(segment_summaries.max_block_height, excluded.max_block_height)",
        params![segment_start, segment_end, segment_path.to_string_lossy().into_owned(), entry.block_height,],
    )?;
    let checkpoint_count: usize = tx.query_row(
        "SELECT checkpoint_count FROM segment_summaries WHERE segment_start = ?1 AND segment_end = ?2",
        params![segment_start, segment_end],
        |row| row.get(0),
    )?;
    tx.commit()?;
    Ok(checkpoint_count)
}

fn ensure_checkpoint_absent(tx: &Transaction<'_>, block_height: u64, segment_path: &Path) -> AppResult<()> {
    let existing: Option<u64> = tx
        .query_row(
            "SELECT block_height FROM checkpoint_entries WHERE block_height = ?1",
            params![block_height],
            |row| row.get(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(AppError(format!("checkpoint {} already exists in {}", block_height, segment_path.display())));
    }
    Ok(())
}

fn collect_book_specs(exchange: &Exchange) -> AppResult<Vec<BookSpec>> {
    let mut specs = Vec::new();

    for (dex_index, perp_dex) in exchange.perp_dexs.iter().enumerate() {
        let universe = &exchange
            .locus
            .cls
            .get(dex_index)
            .ok_or_else(|| AppError(format!("missing perp locus {dex_index}")))?
            .meta
            .universe;

        for (book_index, book) in perp_dex.books.iter().enumerate() {
            let universe_index = (book.asset as usize) % 10_000;
            let asset = universe
                .get(universe_index)
                .ok_or_else(|| AppError(format!("missing perp universe asset id {}", book.asset)))?;
            specs.push(BookSpec {
                name: asset.name.clone(),
                sz_decimals: asset.sz_decimals,
                price_scale: price_scale(asset.sz_decimals),
                source: BookSource::Perp { dex_index, book_index },
            });
        }
    }

    let spot_meta = &exchange.locus.scl.meta;
    for (book_index, book) in exchange.spot_books.iter().enumerate() {
        let asset_id = book.asset as usize;
        let spot_info = spot_meta
            .spot_infos
            .get(asset_id)
            .ok_or_else(|| AppError(format!("missing spot_info for asset id {asset_id}")))?;
        let [base_token_id, quote_token_id] = spot_info.tokens.as_slice() else {
            return Err(AppError(format!("spot tokens missing base/quote for asset id {asset_id}")));
        };
        let base_spec = &spot_meta
            .token_infos
            .get(*base_token_id as usize)
            .ok_or_else(|| AppError(format!("missing token info for id {base_token_id}")))?
            .spec;
        let quote_spec = &spot_meta
            .token_infos
            .get(*quote_token_id as usize)
            .ok_or_else(|| AppError(format!("missing token info for id {quote_token_id}")))?
            .spec;
        specs.push(BookSpec {
            name: if asset_id == 0 { format!("{}/USDC", base_spec.name) } else { format!("@{asset_id}") },
            sz_decimals: base_spec.sz_decimals,
            price_scale: quote_spec.wei_decimals.saturating_sub(base_spec.sz_decimals),
            source: BookSource::Spot { book_index },
        });
    }

    Ok(specs)
}

fn book_value<'a>(exchange: &'a Exchange, source: &BookSource) -> AppResult<&'a Book> {
    match source {
        BookSource::Perp { dex_index, book_index } => exchange
            .perp_dexs
            .get(*dex_index)
            .and_then(|dex| dex.books.get(*book_index))
            .ok_or_else(|| AppError(format!("missing perp book {dex_index}:{book_index}"))),
        BookSource::Spot { book_index } => {
            exchange.spot_books.get(*book_index).ok_or_else(|| AppError(format!("missing spot book {book_index}")))
        }
    }
}

fn trigger_condition(side: &str, code: Option<&str>, raw_trigger_px: u64, scale: u32) -> String {
    let relation = match (side, code.unwrap_or_default()) {
        ("A", "t") | ("A", "T") | ("B", "S") | ("B", "s") => "above",
        ("A", "S") | ("A", "s") | ("B", "t") | ("B", "T") => "below",
        _ => "above",
    };
    format!("Price {relation} {}", scaled_to_condition_string(raw_trigger_px, scale))
}

fn order_type_from_code(code: Option<&str>) -> String {
    match code {
        None => "Limit",
        Some("s") => "Stop Limit",
        Some("S") => "Stop Market",
        Some("t") => "Take Profit Limit",
        Some("T") => "Take Profit Market",
        Some(_) => "Limit",
    }
    .to_string()
}

fn price_scale(sz_decimals: u32) -> u32 {
    6_u32.saturating_sub(sz_decimals)
}

fn scaled_to_string(raw: u64, scale: u32) -> String {
    if scale == 0 {
        return format!("{raw}.0");
    }

    let divisor = 10_u64.pow(scale);
    let whole = raw / divisor;
    let frac = raw % divisor;
    let mut frac_str = format!("{frac:0width$}", width = scale as usize);
    while frac_str.ends_with('0') && frac_str.len() > 1 {
        frac_str.pop();
    }
    format!("{whole}.{frac_str}")
}

fn scaled_to_condition_string(raw: u64, scale: u32) -> String {
    let scaled = scaled_to_string(raw, scale);
    scaled.strip_suffix(".0").unwrap_or(&scaled).to_owned()
}

fn write_json_value<T: Serialize, W: Write>(writer: &mut W, first: &mut bool, value: &T) -> AppResult<()> {
    if !*first {
        writer.write_all(b",")?;
    }
    *first = false;
    serde_json::to_writer(writer, value)?;
    Ok(())
}

fn into_py_err(err: AppError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
}

fn checkpoint_result_to_py(py: Python<'_>, result: CheckpointWriteResult) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("block_height", result.block_height)?;
    out.set_item("block_time", result.block_time)?;
    out.set_item("segment_path", result.segment_path.to_string_lossy().into_owned())?;
    out.set_item("segment_start", result.segment_start)?;
    out.set_item("segment_end", result.segment_end)?;
    out.set_item("snapshot_index_path", result.snapshot_index_path.to_string_lossy().into_owned())?;
    out.set_item("checkpoint_count", result.checkpoint_count)?;
    out.set_item("asset_count", result.asset_count)?;
    out.set_item("codec", result.codec)?;
    out.set_item("compression_level", result.compression_level)?;
    out.set_item("raw_len", result.raw_len)?;
    out.set_item("stored_len", result.stored_len)?;
    Ok(out.into())
}

#[pyfunction(name = "compute_json")]
#[pyo3(signature = (input, include_users=false, include_trigger_orders=false, assets=None))]
fn py_compute_json(
    input: PathBuf,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> PyResult<String> {
    let options = ComputeOptions { include_users, include_trigger_orders, assets };
    compute_l4_json(input, &options).map_err(into_py_err)
}

#[pyfunction(name = "compute_to_file")]
#[pyo3(signature = (input, output, include_users=false, include_trigger_orders=false, assets=None))]
fn py_compute_to_file(
    input: PathBuf,
    output: PathBuf,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> PyResult<()> {
    let options = ComputeOptions { include_users, include_trigger_orders, assets };
    compute_l4_to_file(input, output, &options).map_err(into_py_err)
}

#[pyfunction(name = "append_checkpoint")]
#[pyo3(signature = (input, output_dir=None, include_users=false, include_trigger_orders=false, assets=None))]
fn py_append_checkpoint(
    py: Python<'_>,
    input: PathBuf,
    output_dir: Option<PathBuf>,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let options = ComputeOptions { include_users, include_trigger_orders, assets };
    let result = append_l4_checkpoint(input, output_dir, &options).map_err(into_py_err)?;
    checkpoint_result_to_py(py, result)
}

#[pyfunction(name = "get_dataset_dir")]
fn py_get_dataset_dir() -> String {
    current_dataset_dir().to_string_lossy().into_owned()
}

#[pyfunction(name = "set_dataset_dir")]
#[pyo3(signature = (output_dir=None))]
fn py_set_dataset_dir(output_dir: Option<PathBuf>) -> String {
    set_current_dataset_dir(output_dir).to_string_lossy().into_owned()
}

#[pymodule]
fn _compute_l4_native(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_compute_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_compute_to_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_checkpoint, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_dataset_dir, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_dataset_dir, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BookPayload, BookSnapshotEntry, BookSource, BookSpec, ComputeOptions, DEFAULT_DATASET_DIR, LevelsPayload,
        OutputOrderEntry, SnapshotOrder, UserWrappedOrder, checkpoint_record_from_snapshot_json_bytes,
        checkpoint_segment_bounds, current_dataset_dir, order_type_from_code, scaled_to_condition_string,
        scaled_to_string, select_specs, trigger_condition,
    };
    use serde_json::json;

    #[test]
    fn formats_scaled_strings() {
        assert_eq!(scaled_to_string(711_230, 1), "71123.0");
        assert_eq!(scaled_to_string(697_500, 6), "0.6975");
        assert_eq!(scaled_to_string(59, 2), "0.59");
        assert_eq!(scaled_to_string(0, 5), "0.0");
        assert_eq!(scaled_to_condition_string(711_230, 1), "71123");
        assert_eq!(scaled_to_condition_string(697_500, 6), "0.6975");
    }

    #[test]
    fn maps_order_type_codes() {
        assert_eq!(order_type_from_code(None), "Limit");
        assert_eq!(order_type_from_code(Some("s")), "Stop Limit");
        assert_eq!(order_type_from_code(Some("S")), "Stop Market");
        assert_eq!(order_type_from_code(Some("t")), "Take Profit Limit");
        assert_eq!(order_type_from_code(Some("T")), "Take Profit Market");
    }

    #[test]
    fn builds_trigger_conditions() {
        assert_eq!(trigger_condition("B", Some("t"), 709_810, 1), "Price below 70981");
        assert_eq!(trigger_condition("B", Some("S"), 776_260, 1), "Price above 77626");
        assert_eq!(trigger_condition("A", Some("s"), 255_400, 6), "Price below 0.2554");
        assert_eq!(trigger_condition("A", Some("T"), 440_910, 6), "Price above 0.44091");
    }

    #[test]
    fn filters_assets() {
        let specs = vec![
            BookSpec {
                name: "ETH".to_string(),
                sz_decimals: 3,
                price_scale: 3,
                source: BookSource::Spot { book_index: 0 },
            },
            BookSpec {
                name: "BTC".to_string(),
                sz_decimals: 3,
                price_scale: 3,
                source: BookSource::Perp { dex_index: 0, book_index: 0 },
            },
        ];
        let options =
            ComputeOptions { assets: Some(vec!["BTC".to_string(), "ETH".to_string()]), ..ComputeOptions::default() };
        let selected = select_specs(specs, &options).expect("select specs");
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].name, "BTC");
        assert_eq!(selected[1].name, "ETH");
    }

    #[test]
    fn computes_segment_bounds() {
        assert_eq!(checkpoint_segment_bounds(923_370_000), (923_010_000, 924_000_000));
        assert_eq!(checkpoint_segment_bounds(924_000_000), (923_010_000, 924_000_000));
        assert_eq!(checkpoint_segment_bounds(924_010_000), (924_010_000, 925_000_000));
    }

    #[test]
    fn reports_default_dataset_dir() {
        assert_eq!(current_dataset_dir().to_string_lossy(), DEFAULT_DATASET_DIR);
    }

    #[test]
    fn snapshot_json_checkpoint_record_matches_levels_schema_with_users() {
        let snapshot_json = json!([
            925072353u64,
            [
                [
                    "ETH",
                    [
                        [
                            [
                                "0xabc",
                                {
                                    "coin": "ETH",
                                    "side": "A",
                                    "limitPx": "35621.0",
                                    "sz": "1.5",
                                    "oid": 350184080623u64,
                                    "timestamp": 1u64,
                                    "triggerCondition": "PriceBelow",
                                    "isTrigger": true,
                                    "triggerPx": "35621.0",
                                    "children": [],
                                    "isPositionTpsl": false,
                                    "reduceOnly": false,
                                    "orderType": "Stop Market",
                                    "origSz": "1.5",
                                    "tif": null,
                                    "cloid": null
                                }
                            ]
                        ],
                        []
                    ]
                ]
            ]
        ]);
        let options = ComputeOptions {
            include_users: true,
            include_trigger_orders: false,
            assets: Some(vec!["ETH".to_string()]),
        };

        let json_text = serde_json::to_string(&snapshot_json).expect("snapshot json");
        let record = checkpoint_record_from_snapshot_json_bytes(
            json_text.as_bytes(),
            &options,
            "2026-03-16T02:00:12.176475729".to_string(),
        )
        .expect("record from snapshot json");

        let expected_snapshot = vec![BookSnapshotEntry(
            "ETH".to_string(),
            BookPayload::Levels(LevelsPayload {
                block_height: 925072353,
                block_time: "2026-03-16T02:00:12.176475729".to_string(),
                levels: [
                    vec![OutputOrderEntry::WithUser(UserWrappedOrder(
                        "0xabc".to_string(),
                        SnapshotOrder {
                            coin: "ETH".to_string(),
                            side: "A".to_string(),
                            limit_px: "35621.0".to_string(),
                            sz: "1.5".to_string(),
                            oid: 350184080623,
                            timestamp: 1,
                            trigger_condition: "PriceBelow".to_string(),
                            is_trigger: true,
                            trigger_px: "35621.0".to_string(),
                            children: Vec::new(),
                            is_position_tpsl: false,
                            reduce_only: false,
                            order_type: "Stop Market".to_string(),
                            orig_sz: "1.5".to_string(),
                            tif: None,
                            cloid: None,
                        },
                    ))],
                    vec![],
                ],
            }),
        )];

        assert_eq!(
            serde_json::to_value(&record.snapshot).expect("encode record snapshot"),
            serde_json::to_value(expected_snapshot).expect("encode expected snapshot")
        );
    }

    #[test]
    fn snapshot_json_checkpoint_record_strips_user_wrapper_when_include_users_false() {
        let snapshot_json = json!([
            925072353u64,
            [
                [
                    "ETH",
                    [
                        [
                            [
                                "0xabc",
                                {
                                    "coin": "ETH",
                                    "side": "A",
                                    "limitPx": "35621.0",
                                    "sz": "1.5",
                                    "oid": 350184080623u64,
                                    "timestamp": 1u64,
                                    "triggerCondition": "PriceBelow",
                                    "isTrigger": true,
                                    "triggerPx": "35621.0",
                                    "children": [],
                                    "isPositionTpsl": false,
                                    "reduceOnly": false,
                                    "orderType": "Stop Market",
                                    "origSz": "1.5",
                                    "tif": null,
                                    "cloid": null
                                }
                            ]
                        ],
                        []
                    ]
                ]
            ]
        ]);
        let options = ComputeOptions {
            include_users: false,
            include_trigger_orders: false,
            assets: Some(vec!["ETH".to_string()]),
        };

        let json_text = serde_json::to_string(&snapshot_json).expect("snapshot json");
        let record = checkpoint_record_from_snapshot_json_bytes(
            json_text.as_bytes(),
            &options,
            "2026-03-16T02:00:12.176475729".to_string(),
        )
        .expect("record from snapshot json");

        assert_eq!(
            serde_json::to_value(&record.snapshot).expect("encode record snapshot"),
            json!([
                [
                    "ETH",
                    {
                        "block_height": 925072353u64,
                        "block_time": "2026-03-16T02:00:12.176475729",
                        "levels": [
                            [{
                                "coin": "ETH",
                                "side": "A",
                                "limitPx": "35621.0",
                                "sz": "1.5",
                                "oid": 350184080623u64,
                                "timestamp": 1u64,
                                "triggerCondition": "PriceBelow",
                                "isTrigger": true,
                                "triggerPx": "35621.0",
                                "children": [],
                                "isPositionTpsl": false,
                                "reduceOnly": false,
                                "orderType": "Stop Market",
                                "origSz": "1.5",
                                "tif": null,
                                "cloid": null
                            }],
                            []
                        ]
                    }
                ]
            ])
        );
    }
}
