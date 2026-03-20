#![allow(unsafe_op_in_unsafe_fn)]
#![allow(non_local_definitions, unused_qualifications)]

mod archive;
mod listener;

pub use listener::{
    ArchiveHandoffConfig, ArchiveMode, ArchiveOssConfig, HeightCallback, ListenerHandle, current_archive_base_dir,
    current_archive_symbols, init_cli_logging, run_forever, set_archive_align_output_to_1000_boundary,
    set_archive_align_start_to_10k_boundary, set_archive_base_dir, set_archive_enable_blocks_fill_local_recovery,
    set_archive_enabled, set_archive_handoff_config, set_archive_mode, set_archive_symbols, set_rotation_blocks,
    start_listener,
};

use compute_l4::set_current_dataset_dir;
use log::{Level, LevelFilter, Log, Metadata, Record};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_asyncio::TaskLocals;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};

static PY_LOG_INIT: Once = Once::new();
static PY_BRIDGE_ENABLED: AtomicBool = AtomicBool::new(true);

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

fn configure_archive(
    mode: Option<&str>,
    rotation_blocks: Option<u64>,
    output_dir: Option<PathBuf>,
    symbols: Option<Vec<String>>,
    align_start_to_10k_boundary: bool,
    align_output_to_1000_boundary: bool,
    enable_blocks_fill_local_recovery: bool,
    move_to_nas: bool,
    nas_output_dir: Option<PathBuf>,
    upload_to_oss: bool,
    oss_access_key_id: Option<String>,
    oss_access_key_secret: Option<String>,
    oss_endpoint: Option<String>,
    oss_bucket: Option<String>,
    oss_prefix: Option<String>,
) -> PyResult<()> {
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
    if let Some(output_dir) = output_dir {
        set_current_dataset_dir(Some(output_dir.clone()));
        set_archive_base_dir(Some(output_dir));
    }
    if let Some(n) = rotation_blocks {
        set_rotation_blocks(n);
    }
    set_archive_align_start_to_10k_boundary(align_start_to_10k_boundary);
    set_archive_align_output_to_1000_boundary(align_output_to_1000_boundary);
    set_archive_enable_blocks_fill_local_recovery(enable_blocks_fill_local_recovery);
    set_archive_handoff_config(handoff);
    set_archive_symbols(symbols);
    set_archive_mode(mode);
    Ok(())
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
            let logger = logging.getattr("getLogger")?.call1(("FIFO_module",))?;
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

#[allow(non_local_definitions, unused_qualifications)]
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
        let event_loop: Option<Py<PyAny>> = event_loop.map(|event_loop| event_loop.into());
        let (callback, is_async, locals) = match callback {
            Some(callback) => {
                let callback: Py<PyAny> = callback.into();
                let (is_async, locals) = Python::with_gil(|py| -> PyResult<(bool, Option<TaskLocals>)> {
                    let inspect = py.import("inspect")?;
                    let is_async =
                        inspect.call_method1("iscoroutinefunction", (callback.as_ref(py),))?.extract::<bool>()?;
                    let locals = if is_async {
                        let loop_handle = event_loop.ok_or_else(|| {
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
        enable_blocks_fill_local_recovery=false,
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
        enable_blocks_fill_local_recovery: bool,
        move_to_nas: bool,
        nas_output_dir: Option<PathBuf>,
        upload_to_oss: bool,
        oss_access_key_id: Option<String>,
        oss_access_key_secret: Option<String>,
        oss_endpoint: Option<String>,
        oss_bucket: Option<String>,
        oss_prefix: Option<String>,
    ) -> PyResult<()> {
        configure_archive(
            mode,
            rotation_blocks,
            output_dir,
            symbols,
            align_start_to_10k_boundary,
            align_output_to_1000_boundary,
            enable_blocks_fill_local_recovery,
            move_to_nas,
            nas_output_dir,
            upload_to_oss,
            oss_access_key_id,
            oss_access_key_secret,
            oss_endpoint,
            oss_bucket,
            oss_prefix,
        )
    }

    fn stop_archive(&self) -> PyResult<()> {
        set_archive_mode(None);
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

#[pymodule]
fn fifo_listener(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyFifoListener>()?;
    Ok(())
}
