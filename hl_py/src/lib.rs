#![allow(unsafe_op_in_unsafe_fn)]
#![allow(non_local_definitions, unused_qualifications)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};

use compute_l4::{
    ComputeOptions, append_l4_checkpoint, compute_l4_json, compute_l4_to_file, current_dataset_dir,
    set_current_dataset_dir,
};
use fifo_listener::{
    ArchiveMode, HeightCallback, ListenerHandle, current_archive_base_dir, current_archive_symbols,
    set_archive_base_dir, set_archive_mode, set_archive_symbols, set_rotation_blocks, start_listener,
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
            let logger = logging.getattr("getLogger")?.call1(("HL_module",))?;
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

    #[pyo3(signature = (mode=None, rotation_blocks=None, output_dir=None, symbols=None))]
    fn start_write_dataset(
        &self,
        mode: Option<&str>,
        rotation_blocks: Option<u64>,
        output_dir: Option<PathBuf>,
        symbols: Option<Vec<String>>,
    ) -> PyResult<()> {
        let mode = match mode {
            Some("FULL") => Some(ArchiveMode::Full),
            Some("LITE") | None => Some(ArchiveMode::Lite),
            _ => return Err(pyo3::exceptions::PyValueError::new_err("mode must be 'FULL' or 'LITE'")),
        };
        if output_dir.is_some() {
            set_archive_base_dir(output_dir);
        }
        if let Some(n) = rotation_blocks {
            set_rotation_blocks(n);
        }
        set_archive_symbols(symbols);
        set_archive_mode(mode);
        Ok(())
    }

    fn stop_write_dataset(&self) -> PyResult<()> {
        set_archive_mode(None);
        Ok(())
    }

    fn get_write_dataset_dir(&self) -> String {
        current_archive_base_dir().to_string_lossy().into_owned()
    }

    fn get_write_dataset_symbols(&self) -> Vec<String> {
        current_archive_symbols()
    }

    #[pyo3(signature = (output_dir=None))]
    fn set_write_dataset_dir(&self, output_dir: Option<PathBuf>) -> String {
        set_archive_base_dir(output_dir).to_string_lossy().into_owned()
    }

    #[pyo3(signature = (symbols=None))]
    fn set_write_dataset_symbols(&self, symbols: Option<Vec<String>>) -> Vec<String> {
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
fn _hl_py_native(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyFifoListener>()?;
    m.add_function(wrap_pyfunction!(py_compute_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_compute_to_file, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_checkpoint, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_dataset_dir, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_dataset_dir, m)?)?;
    Ok(())
}
