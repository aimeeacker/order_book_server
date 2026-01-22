#![allow(unsafe_op_in_unsafe_fn)]

mod listener;

pub use listener::{HeightCallback, ListenerHandle, init_cli_logging, run_forever, start_listener};

use log::{Level, LevelFilter, Log, Metadata, Record};
use pyo3::prelude::*;
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
            let logger = self.logger.bind(py);
            let _unused = logger.call_method1("log", (level, msg));
        });
    }

    fn flush(&self) {}
}

fn init_python_logging() {
    PY_LOG_INIT.call_once(|| {
        let logger = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let logging = py.import_bound("logging")?;
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

#[pymethods]
impl PyFifoListener {
    #[new]
    fn new() -> Self {
        Self { handle: None }
    }

    #[pyo3(signature = (callback=None))]
    fn start(&mut self, callback: Option<PyObject>) -> PyResult<()> {
        if self.handle.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "fifo_listener already started",
            ));
        }
        PY_BRIDGE_ENABLED.store(true, Ordering::SeqCst);
        init_python_logging();
        let callback: Option<HeightCallback> = callback.map(|cb| {
            let cb: Py<PyAny> = cb;
            let handler: HeightCallback = Arc::new(move |height| {
                if !PY_BRIDGE_ENABLED.load(Ordering::SeqCst) {
                    return;
                }
                Python::with_gil(|py| {
                    if let Err(err) = cb.call1(py, (height,)) {
                        err.print(py);
                    }
                });
            });
            handler
        });
        self.handle = Some(start_listener(callback));
        Ok(())
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            PY_BRIDGE_ENABLED.store(false, Ordering::SeqCst);
            handle.stop();
        }
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
fn fifo_listener(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyFifoListener>()?;
    Ok(())
}
