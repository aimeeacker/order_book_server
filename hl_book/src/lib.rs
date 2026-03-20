#![allow(unsafe_op_in_unsafe_fn)]
#![allow(non_local_definitions, unused_qualifications)]

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};

use compute_l4::{
    ComputeOptions, append_l4_checkpoint, append_l4_checkpoint_from_snapshot_json, compute_l4_json, compute_l4_to_file,
    current_dataset_dir, set_current_dataset_dir,
};
use fifo_listener::{
    ArchiveHandoffConfig, ArchiveMode, ArchiveOssConfig, ArchiveSessionId, ArchiveSessionOptions, HeightCallback,
    ListenerHandle, current_archive_base_dir, current_archive_symbols, current_rotation_blocks_value,
    set_archive_base_dir, set_archive_symbols, set_rotation_blocks, start_archive_session, start_listener,
    stop_all_archive_sessions_api, stop_archive_session,
};
use log::{Level, LevelFilter, Log, Metadata, Record};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_asyncio::TaskLocals;

use compute_l4 as _;
use fifo_listener as _;
use log as _;
use pyo3 as _;
use pyo3_asyncio as _;

static PY_LOG_INIT: Once = Once::new();
static PY_BRIDGE_ENABLED: AtomicBool = AtomicBool::new(true);
const APPEND_CHECKPOINT_WORKER_BIN: &str = "snapshot_checkpoint";
const APPEND_CHECKPOINT_WORKER_BIN_ENV: &str = "HL_APPEND_CHECKPOINT_WORKER_BIN";

fn parse_archive_mode(mode: Option<&str>) -> PyResult<Option<ArchiveMode>> {
    match mode {
        Some("FULL") => Ok(Some(ArchiveMode::Full)),
        Some("LITE") | None => Ok(Some(ArchiveMode::Lite)),
        _ => Err(pyo3::exceptions::PyValueError::new_err("mode must be 'FULL' or 'LITE'")),
    }
}

fn build_archive_handoff_config(
    move_to_nas: bool,
    nas_output_dir: Option<PathBuf>,
    upload_to_oss: bool,
    oss_access_key_id: Option<String>,
    oss_access_key_secret: Option<String>,
    oss_endpoint: Option<String>,
    oss_bucket: Option<String>,
    oss_prefix: Option<String>,
) -> PyResult<ArchiveHandoffConfig> {
    let oss = match (oss_access_key_id, oss_access_key_secret, oss_endpoint, oss_bucket, oss_prefix) {
        (Some(access_key_id), Some(access_key_secret), Some(endpoint), Some(bucket), prefix) => {
            Some(ArchiveOssConfig::new(access_key_id, access_key_secret, endpoint, bucket, prefix))
        }
        (None, None, None, None, None) => None,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "OSS config must include access_key_id, access_key_secret, endpoint, and bucket",
            ));
        }
    };
    if upload_to_oss && oss.is_none() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "upload_to_oss requires OSS access_key_id, access_key_secret, endpoint, and bucket",
        ));
    }
    Ok(ArchiveHandoffConfig::new(move_to_nas, nas_output_dir, upload_to_oss, oss))
}

fn build_archive_session_options(
    mode: Option<&str>,
    rotation_blocks: Option<u64>,
    output_dir: Option<PathBuf>,
    symbols: Option<Vec<String>>,
    align_start_to_10k_boundary: bool,
    align_output_to_1000_boundary: bool,
    move_to_nas: bool,
    nas_output_dir: Option<PathBuf>,
    upload_to_oss: bool,
    oss_access_key_id: Option<String>,
    oss_access_key_secret: Option<String>,
    oss_endpoint: Option<String>,
    oss_bucket: Option<String>,
    oss_prefix: Option<String>,
) -> PyResult<ArchiveSessionOptions> {
    let mode = parse_archive_mode(mode)?;
    let handoff = build_archive_handoff_config(
        move_to_nas,
        nas_output_dir,
        upload_to_oss,
        oss_access_key_id,
        oss_access_key_secret,
        oss_endpoint,
        oss_bucket,
        oss_prefix,
    )?;
    if let Some(output_dir) = output_dir.as_ref() {
        set_current_dataset_dir(Some(output_dir.clone()));
        set_archive_base_dir(Some(output_dir.clone()));
    }
    if let Some(n) = rotation_blocks {
        set_rotation_blocks(n);
    }
    let output_dir = output_dir.unwrap_or_else(current_archive_base_dir);
    let symbols = symbols.unwrap_or_else(current_archive_symbols);
    Ok(ArchiveSessionOptions {
        mode: mode.unwrap_or(ArchiveMode::Lite),
        rotation_blocks: rotation_blocks.unwrap_or_else(current_rotation_blocks_value),
        output_dir,
        symbols,
        align_start_to_10k_boundary,
        align_output_to_1000_boundary,
        handoff,
    })
}

#[pyclass(unsendable, name = "ArchiveHandle")]
struct PyArchiveHandle {
    session_id: ArchiveSessionId,
}

#[pymethods]
impl PyArchiveHandle {
    #[pyo3(signature = (recover_blocks_fill_locally=false))]
    fn stop_archive(&self, recover_blocks_fill_locally: bool) -> PyResult<()> {
        if stop_archive_session(self.session_id, recover_blocks_fill_locally) {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyRuntimeError::new_err("archive session already stopped"))
        }
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }
}

struct PyLogger {
    logger: Py<PyAny>,
}

impl Log for PyLogger {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        if !PY_BRIDGE_ENABLED.load(Ordering::SeqCst) {
            return;
        }
        let level = match record.level() {
            Level::Error => 40,
            Level::Warn => 30,
            Level::Info => 20,
            Level::Debug => 10,
            Level::Trace => 5,
        };
        let msg = format!("{}", record.args());
        Python::with_gil(|py| {
            let logger = self.logger.as_ref(py);
            let _unused = logger.call_method1("log", (level, msg));
        });
    }

    fn flush(&self) {}
}

fn init_python_logging() {
    PY_LOG_INIT.call_once(|| {
        let logger = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let logging = py.import("logging")?;
            let logger = logging.getattr("getLogger")?.call1(("hl_book",))?;
            Ok(logger.into())
        });
        let logger = match logger {
            Ok(logger) => logger,
            Err(err) => {
                Python::with_gil(|py| err.print(py));
                return;
            }
        };
        let _unused = log::set_boxed_logger(Box::new(PyLogger { logger }));
        log::set_max_level(LevelFilter::Info);
    });
}

#[pyclass(unsendable, name = "FifoListener")]
struct PyFifoListener {
    handle: Option<ListenerHandle>,
}

#[pymethods]
impl PyFifoListener {
    #[new]
    fn new() -> Self {
        Self { handle: None }
    }

    #[pyo3(signature = (callback=None, event_loop=None))]
    fn start(&mut self, callback: Option<PyObject>, event_loop: Option<PyObject>) -> PyResult<()> {
        if self.handle.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("fifo_listener already started"));
        }

        PY_BRIDGE_ENABLED.store(true, Ordering::SeqCst);
        init_python_logging();

        let event_loop: Option<Py<PyAny>> = event_loop.map(Into::into);
        let (callback, is_async, locals) = match callback {
            Some(callback) => {
                let callback: Py<PyAny> = callback.into();
                let (is_async, locals) = Python::with_gil(|py| -> PyResult<(bool, Option<TaskLocals>)> {
                    let inspect = py.import("inspect")?;
                    let is_async =
                        inspect.call_method1("iscoroutinefunction", (callback.as_ref(py),))?.extract::<bool>()?;
                    let locals = if is_async {
                        let loop_handle = event_loop.as_ref().ok_or_else(|| {
                            pyo3::exceptions::PyRuntimeError::new_err("async callback requires event_loop")
                        })?;
                        Some(TaskLocals::new(loop_handle.as_ref(py)).copy_context(py)?)
                    } else {
                        None
                    };
                    Ok((is_async, locals))
                })?;
                (Some(callback), is_async, locals)
            }
            None => (None, false, None),
        };

        let callback: Option<HeightCallback> = callback.map(move |cb| {
            let locals = locals;
            let handler: HeightCallback = Arc::new(move |height| {
                if !PY_BRIDGE_ENABLED.load(Ordering::SeqCst) {
                    return;
                }
                Python::with_gil(|py| {
                    if is_async {
                        let Some(locals) = locals.as_ref() else {
                            return;
                        };
                        match cb.call1(py, (height,)) {
                            Ok(coro) => {
                                let event_loop = locals.event_loop(py);
                                let context = locals.context(py);
                                if let Ok(create_task) = event_loop.getattr("create_task") {
                                    let kwargs = PyDict::new(py);
                                    if kwargs.set_item("context", context).is_ok() {
                                        if let Err(err) = event_loop.call_method(
                                            "call_soon_threadsafe",
                                            (create_task, coro),
                                            Some(kwargs),
                                        ) {
                                            err.print(py);
                                        }
                                    }
                                }
                            }
                            Err(err) => err.print(py),
                        }
                    } else if let Err(err) = cb.call1(py, (height,)) {
                        err.print(py);
                    }
                });
            });
            handler
        });

        self.handle = Some(start_listener(callback)?);
        Ok(())
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            PY_BRIDGE_ENABLED.store(false, Ordering::SeqCst);
            handle.stop();
        }
        Ok(())
    }

    #[pyo3(signature = (
        mode=None,
        rotation_blocks=None,
        output_dir=None,
        symbols=None,
        align_start_to_10k_boundary=true,
        align_output_to_1000_boundary=true,
        move_to_nas=true,
        nas_output_dir=None,
        upload_to_oss=false,
        oss_access_key_id=None,
        oss_access_key_secret=None,
        oss_endpoint=None,
        oss_bucket=None,
        oss_prefix=None
    ))]
    fn start_archive(
        &self,
        mode: Option<&str>,
        rotation_blocks: Option<u64>,
        output_dir: Option<PathBuf>,
        symbols: Option<Vec<String>>,
        align_start_to_10k_boundary: bool,
        align_output_to_1000_boundary: bool,
        move_to_nas: bool,
        nas_output_dir: Option<PathBuf>,
        upload_to_oss: bool,
        oss_access_key_id: Option<String>,
        oss_access_key_secret: Option<String>,
        oss_endpoint: Option<String>,
        oss_bucket: Option<String>,
        oss_prefix: Option<String>,
    ) -> PyResult<PyArchiveHandle> {
        let options = build_archive_session_options(
            mode,
            rotation_blocks,
            output_dir,
            symbols,
            align_start_to_10k_boundary,
            align_output_to_1000_boundary,
            move_to_nas,
            nas_output_dir,
            upload_to_oss,
            oss_access_key_id,
            oss_access_key_secret,
            oss_endpoint,
            oss_bucket,
            oss_prefix,
        )?;
        let session_id = start_archive_session(options);
        Ok(PyArchiveHandle { session_id })
    }

    #[pyo3(signature = (recover_blocks_fill_locally=false))]
    fn stop_archive(&self, recover_blocks_fill_locally: bool) -> PyResult<()> {
        stop_all_archive_sessions_api(recover_blocks_fill_locally);
        Ok(())
    }

    fn get_archive_dir(&self) -> String {
        current_archive_base_dir().to_string_lossy().into_owned()
    }

    fn get_archive_symbols(&self) -> Vec<String> {
        current_archive_symbols()
    }

    #[pyo3(signature = (output_dir=None))]
    fn set_archive_dir(&self, output_dir: Option<PathBuf>) -> String {
        let snapshot_dir = set_current_dataset_dir(output_dir.clone());
        let archive_dir = set_archive_base_dir(output_dir);
        if snapshot_dir != archive_dir {
            log::warn!(
                "snapshot dataset dir ({}) differs from archive dataset dir ({}) after set_archive_dir",
                snapshot_dir.display(),
                archive_dir.display()
            );
        }
        archive_dir.to_string_lossy().into_owned()
    }

    #[pyo3(signature = (symbols=None))]
    fn set_archive_symbols(&self, symbols: Option<Vec<String>>) -> Vec<String> {
        set_archive_symbols(symbols)
    }
}

impl Drop for PyFifoListener {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            PY_BRIDGE_ENABLED.store(false, Ordering::SeqCst);
            handle.stop();
        }
    }
}

fn into_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
}

fn checkpoint_result_to_py(py: Python<'_>, result: compute_l4::CheckpointWriteResult) -> PyResult<PyObject> {
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

fn checkpoint_result_from_json(
    value: serde_json::Value,
) -> Result<compute_l4::CheckpointWriteResult, Box<dyn std::error::Error + Send + Sync>> {
    Ok(compute_l4::CheckpointWriteResult {
        block_height: value
            .get("block_height")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| io_error("missing block_height"))?,
        block_time: value
            .get("block_time")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| io_error("missing block_time"))?
            .to_string(),
        segment_path: PathBuf::from(
            value
                .get("segment_path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| io_error("missing segment_path"))?,
        ),
        segment_start: value
            .get("segment_start")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| io_error("missing segment_start"))?,
        segment_end: value
            .get("segment_end")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| io_error("missing segment_end"))?,
        snapshot_index_path: PathBuf::from(
            value
                .get("snapshot_index_path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| io_error("missing snapshot_index_path"))?,
        ),
        checkpoint_count: value
            .get("checkpoint_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| io_error("missing checkpoint_count"))? as usize,
        asset_count: value
            .get("asset_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| io_error("missing asset_count"))? as usize,
        codec: value
            .get("codec")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| io_error("missing codec"))?
            .to_string(),
        compression_level: value.get("compression_level").and_then(serde_json::Value::as_i64).map(|level| level as i32),
        raw_len: value.get("raw_len").and_then(serde_json::Value::as_u64).ok_or_else(|| io_error("missing raw_len"))?,
        stored_len: value
            .get("stored_len")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| io_error("missing stored_len"))?,
    })
}

fn io_error(message: &str) -> std::io::Error {
    std::io::Error::other(message.to_string())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().expect("workspace root").to_path_buf()
}

fn resolve_append_checkpoint_worker_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(path) = std::env::var(APPEND_CHECKPOINT_WORKER_BIN_ENV) {
        return Ok(PathBuf::from(path));
    }
    if let Ok(cwd) = std::env::current_dir() {
        for candidate in [
            cwd.join(APPEND_CHECKPOINT_WORKER_BIN),
            cwd.join("bin").join(APPEND_CHECKPOINT_WORKER_BIN),
            cwd.join("target").join("release").join(APPEND_CHECKPOINT_WORKER_BIN),
            cwd.join("target").join("debug").join(APPEND_CHECKPOINT_WORKER_BIN),
        ] {
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    let workspace_root = workspace_root();
    for profile in ["release", "debug"] {
        let candidate = workspace_root.join("target").join(profile).join(APPEND_CHECKPOINT_WORKER_BIN);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(format!(
        "failed to locate native append checkpoint worker binary {}; searched {} env, current working directory, and workspace target dirs",
        APPEND_CHECKPOINT_WORKER_BIN, APPEND_CHECKPOINT_WORKER_BIN_ENV
    )
    .into())
}

fn append_checkpoint_direct(
    input: PathBuf,
    output_dir: Option<PathBuf>,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> Result<compute_l4::CheckpointWriteResult, Box<dyn std::error::Error + Send + Sync>> {
    let options = ComputeOptions { include_users, include_trigger_orders, assets };
    append_l4_checkpoint(input, output_dir, &options).map_err(|err| err.into())
}

fn append_checkpoint_isolated(
    input: PathBuf,
    output_dir: Option<PathBuf>,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> Result<compute_l4::CheckpointWriteResult, Box<dyn std::error::Error + Send + Sync>> {
    let worker_bin = resolve_append_checkpoint_worker_binary()?;
    let output = Command::new(&worker_bin)
        .arg(&input)
        .arg(output_dir.as_ref().map_or_else(String::new, |path| path.display().to_string()))
        .arg(if include_users { "1" } else { "0" })
        .arg(if include_trigger_orders { "1" } else { "0" })
        .args(assets.clone().unwrap_or_default())
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "append_checkpoint child failed status={} exe={} stderr={stderr}",
            output.status,
            worker_bin.display()
        )
        .into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let meta: serde_json::Value = serde_json::from_str(stdout.trim())?;
    checkpoint_result_from_json(meta)
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
    let result = append_checkpoint_isolated(input, output_dir, include_users, include_trigger_orders, assets)
        .map_err(into_py_err)?;
    checkpoint_result_to_py(py, result)
}

#[pyfunction(name = "append_checkpoint_direct")]
#[pyo3(signature = (input, output_dir=None, include_users=false, include_trigger_orders=false, assets=None))]
fn py_append_checkpoint_direct(
    py: Python<'_>,
    input: PathBuf,
    output_dir: Option<PathBuf>,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let result = append_checkpoint_direct(input, output_dir, include_users, include_trigger_orders, assets)
        .map_err(into_py_err)?;
    checkpoint_result_to_py(py, result)
}

#[pyfunction(name = "append_checkpoint_from_snapshot_json")]
#[pyo3(signature = (input, block_time, output_dir=None, include_users=false, include_trigger_orders=false, assets=None))]
fn py_append_checkpoint_from_snapshot_json(
    py: Python<'_>,
    input: PathBuf,
    block_time: String,
    output_dir: Option<PathBuf>,
    include_users: bool,
    include_trigger_orders: bool,
    assets: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let options = ComputeOptions { include_users, include_trigger_orders, assets };
    let result =
        append_l4_checkpoint_from_snapshot_json(input, output_dir, &options, block_time).map_err(into_py_err)?;
    checkpoint_result_to_py(py, result)
}

#[pyfunction(name = "get_dataset_dir")]
fn py_get_dataset_dir() -> String {
    let snapshot_dir = current_dataset_dir();
    let archive_dir = current_archive_base_dir();
    if snapshot_dir != archive_dir {
        log::warn!(
            "snapshot dataset dir ({}) differs from archive dataset dir ({})",
            snapshot_dir.display(),
            archive_dir.display()
        );
    }
    snapshot_dir.to_string_lossy().into_owned()
}

#[pyfunction(name = "set_dataset_dir")]
#[pyo3(signature = (output_dir=None))]
fn py_set_dataset_dir(output_dir: Option<PathBuf>) -> String {
    let snapshot_dir = set_current_dataset_dir(output_dir.clone());
    let archive_dir = set_archive_base_dir(output_dir);
    if snapshot_dir != archive_dir {
        log::warn!(
            "snapshot dataset dir ({}) differs from archive dataset dir ({}) after set_dataset_dir",
            snapshot_dir.display(),
            archive_dir.display()
        );
    }
    snapshot_dir.to_string_lossy().into_owned()
}

#[pymodule]
fn _hl_book_native(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyFifoListener>()?;
    m.add_class::<PyArchiveHandle>()?;
    m.add_function(wrap_pyfunction!(py_compute_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_compute_to_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_checkpoint, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_checkpoint_direct, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_checkpoint_from_snapshot_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_dataset_dir, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_dataset_dir, m)?)?;
    Ok(())
}
