//! The engine, the memory limiter, and how a failed call is described.

use std::sync::OnceLock;
use std::time::Duration;
use wasmi::{Engine, TrapCode};

/// The node's one engine: fuel metering on, everything else wasmi's default.
/// Modules compiled for it can be instantiated by any embedder.
pub fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = wasmi::Config::default();
        config.consume_fuel(true);
        Engine::new(&config)
    })
}

/// Memory cap for one instance (`Store::limiter`). It remembers that it
/// refused, so the trap that follows (the module's allocator gives up) reads
/// as the limit it is.
#[derive(Debug)]
pub struct MemoryCap {
    pub max: usize,
    pub refused: bool,
}

impl MemoryCap {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            refused: false,
        }
    }
}

impl wasmi::ResourceLimiter for MemoryCap {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmi_core::LimiterError> {
        if desired > self.max {
            self.refused = true;
            return Ok(false);
        }
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmi_core::LimiterError> {
        Ok(desired <= 100_000)
    }

    fn instances(&self) -> usize {
        1
    }

    fn tables(&self) -> usize {
        4
    }

    fn memories(&self) -> usize {
        1
    }
}

/// Describe why a call into `export` failed, the same way for every
/// embedder: the memory limit (when `cap` refused), exhausted fuel (read
/// against `deadline`, which the fuel budget stands for), a `proc_exit`, or
/// a trap. Any of these leaves the instance in an unknown state, so the
/// embedder must not call it again.
pub fn describe_failure(
    err: &wasmi::Error,
    cap: &MemoryCap,
    deadline: Duration,
    export: &str,
) -> String {
    if cap.refused {
        format!("exceeded its memory limit ({} bytes)", cap.max)
    } else if err.as_trap_code() == Some(TrapCode::OutOfFuel) {
        format!(
            "exceeded its {} deadline (ran out of fuel)",
            humanize(deadline)
        )
    } else if let Some(code) = err.i32_exit_status() {
        format!("exited (proc_exit {code})")
    } else {
        format!("trapped in {export}: {err}")
    }
}

/// A duration as a limit is written: `10ms`, `1s`, `500µs`.
pub fn humanize(duration: Duration) -> String {
    if duration.as_millis() >= 1000 && duration.as_millis().is_multiple_of(1000) {
        format!("{}s", duration.as_secs())
    } else if duration.as_micros().is_multiple_of(1000) {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{}µs", duration.as_micros())
    }
}
