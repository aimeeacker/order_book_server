#![allow(unsafe_op_in_unsafe_fn)]
#![allow(non_local_definitions, unused_qualifications)]

mod archive;
mod listener;

pub use listener::{
    HeightCallback, ListenerHandle, init_cli_logging, run_forever, set_archive_enabled, start_listener,
};

use log::{Level, LevelFilter, Log, Metadata, Record};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_asyncio::TaskLocals;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};

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

    fn start_write_dataset(&self) -> PyResult<()> {
        set_archive_enabled(true);
        Ok(())
    }

    fn stop_write_dataset(&self) -> PyResult<()> {
        set_archive_enabled(false);
        Ok(())
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
