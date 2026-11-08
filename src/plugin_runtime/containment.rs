//! Panic containment for dylib plugins (review finding PLG1; docs/plugin-
//! system.md, "Panics and quarantine").
//!
//! A dylib plugin is a `cdylib` with **its own copy of the Rust standard
//! library**. A panic that unwinds out of plugin code into the host is, to
//! the host's `std`, a foreign exception: `catch_unwind` in the host aborts
//! the process rather than catching it. So containment has two halves:
//!
//! - **Plugin side.** The plugin's exported constructor
//!   (`legion_contained_get`, written by the `export_*!` macros) wraps the
//!   plugin's value in [`Contained`]. Its trait impls are generic, so they
//!   are compiled into the plugin, and every method runs under the plugin's
//!   own `catch_unwind` ([`contained`]). A panic is recorded in the plugin's
//!   copy of [`PANIC_SLOT`] and the method returns a fallback: an `Err` where
//!   it returns a `Result`, a neutral value otherwise.
//! - **Host side.** Every call into a loaded plugin goes through [`guard`]:
//!   it refuses a quarantined plugin, runs the call under the host's
//!   `catch_unwind` (which does catch a panic from an in-process
//!   implementation), then asks the plugin, through the C-ABI
//!   `legion_plugin_take_panic` symbol ([`PanicProbe`]), whether the call
//!   panicked. A panic **quarantines** the plugin for the life of the
//!   process: later calls fail fast with [`PluginQuarantined`], the
//!   quarantine is logged at `error`, counted in
//!   `legion_plugin_panics_total{kind,name}`, and listed by
//!   [`quarantined`] (and in a worker's health checks under `plugins`).
//!
//! Both sides need `panic = "unwind"`; a plugin built with `panic = "abort"`
//! aborts on any panic. The ABI fingerprint already refuses a plugin whose
//! panic strategy differs from the host's. A panic *inside* the exported
//! `extern "C"` constructor itself cannot be caught and aborts; the macros
//! keep that constructor to a single allocation.

use std::collections::BTreeMap;
use std::ffi::{c_char, CStr, CString};
use std::panic::AssertUnwindSafe;
use std::sync::{LazyLock, Mutex};

use serde::Serialize;

use crate::prelude::*;

/// The symbol the `export_*!` macros give the contained constructor. A
/// plugin that exports only the old `get` is refused, because nothing in it
/// would catch a panic.
pub const CONTAINED_GET_SYMBOL: &[u8] = b"legion_contained_get";

/// The plugin-side record of the last panic its [`Contained`] caught.
/// Each dylib has its own copy (it links its own copy of this crate).
pub static PANIC_SLOT: Mutex<Option<String>> = Mutex::new(None);

/// Text of a panic payload.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a panic with a non-string payload".to_string()
    }
}

/// Plugin side: run one trait method under `catch_unwind`. A panic is
/// recorded in [`PANIC_SLOT`] (with `op`) and `fallback` answers instead.
/// Generic, so it is compiled into whichever crate calls it: in a plugin,
/// the plugin's own `std` catches the plugin's own panic.
pub fn contained<T>(op: &str, fallback: impl FnOnce() -> T, call: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(AssertUnwindSafe(call)) {
        Ok(value) => value,
        Err(payload) => {
            let message = format!("{op}: {}", panic_message(payload.as_ref()));
            if let Ok(mut slot) = PANIC_SLOT.lock() {
                *slot = Some(message);
            }
            fallback()
        }
    }
}

/// Plugin side: take the recorded panic, if any. Behind the
/// `legion_plugin_take_panic` symbol.
pub fn take_recorded_panic() -> Option<String> {
    PANIC_SLOT.lock().ok().and_then(|mut slot| slot.take())
}

/// The C string the `legion_plugin_take_panic` symbol hands back; freed by
/// `legion_plugin_free_panic` in the same dylib.
pub fn take_recorded_panic_c() -> *mut c_char {
    match take_recorded_panic() {
        Some(message) => CString::new(message.replace('\0', " "))
            .map(CString::into_raw)
            .unwrap_or(std::ptr::null_mut()),
        None => std::ptr::null_mut(),
    }
}

/// Frees a string [`take_recorded_panic_c`] returned.
///
/// # Safety
///
/// `ptr` must come from [`take_recorded_panic_c`] in the same dylib, once.
pub unsafe fn free_recorded_panic_c(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

/// A plugin value wrapped so every trait method it exposes runs under
/// [`contained`]. The trait impls live next to each trait.
pub struct Contained<T>(pub T);

/// Host side: how to ask a loaded dylib whether its last call panicked.
#[derive(Clone, Copy)]
pub struct PanicProbe {
    take: unsafe extern "C" fn() -> *mut c_char,
    free: unsafe extern "C" fn(*mut c_char),
}

impl std::fmt::Debug for PanicProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PanicProbe")
    }
}

impl PanicProbe {
    /// The probe a dylib built by `declare_plugin_abi!` exports.
    ///
    /// # Safety
    ///
    /// `lib` must have passed [`super::open_plugin_library`]'s fingerprint
    /// check, so its symbols have the expected types.
    pub unsafe fn from_library(kind: &str, name: &str, lib: &libloading::Library) -> R<Self> {
        let take = lib
            .get::<unsafe extern "C" fn() -> *mut c_char>(b"legion_plugin_take_panic")
            .map(|symbol| *symbol);
        let free = lib
            .get::<unsafe extern "C" fn(*mut c_char)>(b"legion_plugin_free_panic")
            .map(|symbol| *symbol);
        match (take, free) {
            (Ok(take), Ok(free)) => Ok(PanicProbe { take, free }),
            _ => bail!(
                "{kind} plugin {name} exports no panic probe; rebuild it against this legion \
                 version with `legion::declare_plugin_abi!();` (see docs/plugin-system.md)"
            ),
        }
    }

    /// The probe over this process's own slot: for in-process plugins and
    /// tests, which share the host's copy of [`PANIC_SLOT`].
    pub fn in_process() -> Self {
        unsafe extern "C" fn take() -> *mut c_char {
            take_recorded_panic_c()
        }
        unsafe extern "C" fn free(ptr: *mut c_char) {
            free_recorded_panic_c(ptr)
        }
        PanicProbe { take, free }
    }

    fn take(&self) -> Option<String> {
        // SAFETY: both functions come from the same dylib (or this
        // process), and the pointer is freed exactly once by its owner.
        unsafe {
            let ptr = (self.take)();
            if ptr.is_null() {
                return None;
            }
            let message = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            (self.free)(ptr);
            Some(message)
        }
    }
}

/// A plugin call panicked; the plugin is now quarantined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginPanicked {
    pub kind: String,
    pub name: String,
    pub op: String,
    pub message: String,
}

impl std::fmt::Display for PluginPanicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} plugin {} panicked in `{}` ({}); it is quarantined until this process restarts",
            self.kind, self.name, self.op, self.message
        )
    }
}

impl std::error::Error for PluginPanicked {}

/// A call to a plugin quarantined by an earlier panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginQuarantined {
    pub kind: String,
    pub name: String,
    pub reason: String,
}

impl std::fmt::Display for PluginQuarantined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} plugin {} is quarantined: it panicked earlier ({}). Fix or remove the plugin, \
             then restart this process (docs/plugin-system.md, \"Panics and quarantine\")",
            self.kind, self.name, self.reason
        )
    }
}

impl std::error::Error for PluginQuarantined {}

/// One quarantined plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuarantineRecord {
    pub kind: String,
    pub name: String,
    pub op: String,
    pub message: String,
    pub at_unix: i64,
}

static QUARANTINE: LazyLock<Mutex<BTreeMap<(String, String), QuarantineRecord>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// Every plugin quarantined in this process.
pub fn quarantined() -> Vec<QuarantineRecord> {
    QUARANTINE
        .lock()
        .map(|map| map.values().cloned().collect())
        .unwrap_or_default()
}

/// The quarantine as a worker health-check value (`{"<kind>/<name>":
/// "quarantined: ..."}`), or `None` when nothing is quarantined. A
/// quarantined plugin is a degraded node, so the value deliberately reads as
/// a failing check.
pub fn quarantine_health() -> Option<Value> {
    let records = quarantined();
    if records.is_empty() {
        return None;
    }
    let mut map = Map::new();
    for record in records {
        map.insert(
            format!("{}/{}", record.kind, record.name),
            Value::String(format!(
                "quarantined: panicked in `{}`: {}",
                record.op, record.message
            )),
        );
    }
    Some(Value::Object(map))
}

/// Refuse a call to a quarantined plugin.
pub fn check_quarantine(kind: &str, name: &str) -> R<()> {
    let found = QUARANTINE.lock().ok().and_then(|map| {
        map.get(&(kind.to_string(), name.to_string()))
            .map(|record| record.message.clone())
    });
    match found {
        Some(reason) => Err(anyhow::Error::new(PluginQuarantined {
            kind: kind.to_string(),
            name: name.to_string(),
            reason,
        })),
        None => Ok(()),
    }
}

fn quarantine(kind: &str, name: &str, op: &str, message: &str) {
    error!(
        plugin_kind = kind,
        plugin = name,
        op,
        panic = message,
        "Plugin panicked; quarantining it until this process restarts"
    );
    crate::manager::telemetry::metrics()
        .plugin_panics_total
        .inc(&[kind, name]);
    if let Ok(mut map) = QUARANTINE.lock() {
        map.entry((kind.to_string(), name.to_string()))
            .or_insert_with(|| QuarantineRecord {
                kind: kind.to_string(),
                name: name.to_string(),
                op: op.to_string(),
                message: message.to_string(),
                at_unix: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or_default(),
            });
    }
}

/// Host side: one call into a plugin. Refuses a quarantined plugin; a panic
/// (caught here for an in-process implementation, or reported by `probe`
/// for a dylib whose [`Contained`] caught it) quarantines the plugin and
/// returns [`PluginPanicked`] in place of whatever the call returned.
pub fn guard<T>(
    kind: &str,
    name: &str,
    op: &str,
    probe: Option<&PanicProbe>,
    call: impl FnOnce() -> T,
) -> R<T> {
    check_quarantine(kind, name)?;
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(call));
    let panicked = match &outcome {
        Err(payload) => Some(panic_message(payload.as_ref())),
        Ok(_) => probe.and_then(PanicProbe::take),
    };
    if let Some(message) = panicked {
        quarantine(kind, name, op, &message);
        return Err(anyhow::Error::new(PluginPanicked {
            kind: kind.to_string(),
            name: name.to_string(),
            op: op.to_string(),
            message,
        }));
    }
    match outcome {
        Ok(value) => Ok(value),
        Err(_) => unreachable!("a caught panic returned above"),
    }
}

/// [`guard`] for a call that returns a `Result`: one error either way.
pub fn guard_r<T>(
    kind: &str,
    name: &str,
    op: &str,
    probe: Option<&PanicProbe>,
    call: impl FnOnce() -> R<T>,
) -> R<T> {
    guard(kind, name, op, probe, call)?
}

/// Lift a quarantine (tests only: a real quarantine lasts until restart).
#[cfg(test)]
pub fn release_quarantine(kind: &str, name: &str) {
    if let Ok(mut map) = QUARANTINE.lock() {
        map.remove(&(kind.to_string(), name.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Greeter {
        fn greet(&self, who: &str) -> R<String>;
        fn count(&self) -> usize;
    }

    struct Panicky;

    impl Greeter for Panicky {
        fn greet(&self, who: &str) -> R<String> {
            if who == "boom" {
                panic!("greeting {who} exploded");
            }
            Ok(format!("hello {who}"))
        }
        fn count(&self) -> usize {
            panic!("count is broken")
        }
    }

    // What the trait impl next to a real plugin trait looks like.
    impl<T: Greeter> Greeter for Contained<T> {
        fn greet(&self, who: &str) -> R<String> {
            contained(
                "greet",
                || Err(anyhow!("the plugin panicked")),
                || self.0.greet(who),
            )
        }
        fn count(&self) -> usize {
            contained("count", || 0, || self.0.count())
        }
    }

    /// A deliberately panicking plugin, behind the same two layers a dylib
    /// gets: the plugin-side wrapper catches and records, the host-side
    /// guard turns it into an error and quarantines the plugin.
    #[test]
    fn a_panicking_plugin_is_contained_and_quarantined_test() {
        let plugin: Box<dyn Greeter> = Box::new(Contained(Panicky));
        let probe = PanicProbe::in_process();
        let (kind, name) = ("test", "panicky-contained");

        let ok = guard_r(kind, name, "greet", Some(&probe), || plugin.greet("you"));
        assert_eq!(ok.unwrap(), "hello you");

        let err = guard_r(kind, name, "greet", Some(&probe), || plugin.greet("boom")).unwrap_err();
        let panicked = err.downcast_ref::<PluginPanicked>().expect("typed error");
        assert!(panicked.message.contains("exploded"), "{panicked}");
        assert!(quarantined()
            .iter()
            .any(|record| record.name == name && record.op == "greet"));
        let health = quarantine_health().expect("quarantine shows in health");
        assert!(health[format!("{kind}/{name}")]
            .as_str()
            .unwrap()
            .starts_with("quarantined"));

        // Quarantined: the plugin is not called again.
        let mut called = false;
        let err = guard(kind, name, "count", Some(&probe), || {
            called = true;
            plugin.count()
        })
        .unwrap_err();
        assert!(err.downcast_ref::<PluginQuarantined>().is_some(), "{err}");
        assert!(!called);
        release_quarantine(kind, name);
    }

    /// An in-process implementation with no wrapper: the host's own
    /// `catch_unwind` catches it.
    #[test]
    fn an_unwrapped_in_process_panic_is_caught_by_the_host_test() {
        let plugin: Box<dyn Greeter> = Box::new(Panicky);
        let (kind, name) = ("test", "panicky-bare");
        let err = guard(kind, name, "count", None, || plugin.count()).unwrap_err();
        assert!(err.to_string().contains("count is broken"), "{err}");
        assert!(check_quarantine(kind, name).is_err());
        release_quarantine(kind, name);
    }
}
