//! One plugin module instance on the proxy-wasm ABI, inside wasmi.
//!
//! A [`Vm`] is one instance of a compiled module with its root context
//! started and configured. Stream contexts (one per request or connection)
//! live inside the instance's memory; the host keeps each context's
//! host-side state in a [`ContextState`] and lends it to the instance for
//! the duration of one callback, so a host function the module calls reads
//! and writes exactly that context's headers, bodies, and properties.
//!
//! Every callback runs under the plugin's fuel budget and memory cap and is
//! timed against its deadline. A trap, exhausted fuel, a module that exits,
//! or a memory limit breaks the instance: it is never called again, and the
//! pool that owns it starts a fresh one.

use super::abi::{self, buffer, map, status, stream, Pairs};
use crate::prelude::*;

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use wasmi::{Caller, Engine, Extern, Linker, Module, Store, TrapCode, Val, ValType};

/// The root context's id; stream contexts count up from 2.
pub const ROOT_CONTEXT: u32 = 1;

/// The node's one engine: fuel metering on, everything else wasmi's default.
pub fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = wasmi::Config::default();
        config.consume_fuel(true);
        Engine::new(&config)
    })
}

/// The proxy-wasm ABI version a module exports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiVersion {
    V0_2_0,
    V0_2_1,
}

/// A module compiled and linked once per checksum.
pub struct Compiled {
    pub module: Module,
    pub linker: Arc<Linker<HostState>>,
    pub abi: AbiVersion,
}

impl std::fmt::Debug for Compiled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compiled").field("abi", &self.abi).finish()
    }
}

/// Compile and link a module. Fails for bytes that are not WASM, a module
/// that does not export a proxy-wasm 0.2 ABI marker, or one that imports
/// memory or tables.
pub fn compile(bytes: &[u8]) -> Result<Compiled, String> {
    let module =
        Module::new(engine(), bytes).map_err(|err| format!("not a valid module: {err}"))?;
    let exports: Vec<String> = module.exports().map(|e| e.name().to_string()).collect();
    let abi = if exports.iter().any(|e| e == "proxy_abi_version_0_2_1") {
        AbiVersion::V0_2_1
    } else if exports.iter().any(|e| e == "proxy_abi_version_0_2_0") {
        AbiVersion::V0_2_0
    } else {
        return Err(
            "not a proxy-wasm module: it exports no proxy_abi_version_0_2_0 or _0_2_1 marker \
             (build it with a proxy-wasm SDK for ABI 0.2, e.g. proxy-wasm 0.2 for Rust)"
                .to_string(),
        );
    };
    if !exports.iter().any(|e| e == "memory") {
        return Err("the module exports no memory".to_string());
    }
    if !exports
        .iter()
        .any(|e| e == "proxy_on_memory_allocate" || e == "malloc")
    {
        return Err(
            "the module exports neither proxy_on_memory_allocate nor malloc, so the host cannot \
             hand it data"
                .to_string(),
        );
    }
    let linker = host_linker(&module)?;
    Ok(Compiled {
        module,
        linker: Arc::new(linker),
        abi,
    })
}

/// A `send_local_response` the module asked for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalReply {
    pub status: u16,
    pub details: String,
    pub body: Vec<u8>,
    pub headers: Pairs,
}

/// A `dispatch_http_call` the module made during a callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCall {
    pub token: u32,
    /// The plugin's `targets` name.
    pub target: String,
    pub headers: Pairs,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

/// What a finished `dispatch_http_call` returned, for
/// `proxy_on_http_call_response`.
#[derive(Debug, Clone, Default)]
pub struct CallResponse {
    pub headers: Pairs,
    pub body: Vec<u8>,
    pub trailers: Pairs,
}

/// The host side of one stream context.
#[derive(Debug, Default)]
pub struct ContextState {
    pub id: u32,
    /// Properties by path, the segments joined with NUL: the flow's facts,
    /// plus anything the module set with `set_property`.
    pub properties: BTreeMap<String, Vec<u8>>,
    pub request_headers: Option<Pairs>,
    pub request_trailers: Option<Pairs>,
    pub response_headers: Option<Pairs>,
    pub response_trailers: Option<Pairs>,
    pub request_body: Option<Vec<u8>>,
    pub response_body: Option<Vec<u8>>,
    pub downstream: Option<Vec<u8>>,
    pub call_response: Option<CallResponse>,
    pub local_reply: Option<LocalReply>,
    /// The module called `continue_stream` / `resume_http_*`.
    pub resumed: bool,
    /// The module called `close_stream` (on a connection, a refusal).
    pub closed: bool,
    pub calls: Vec<PendingCall>,
    /// The module changed request or response headers.
    pub request_headers_changed: bool,
    pub response_headers_changed: bool,
    pub request_body_changed: bool,
    pub response_body_changed: bool,
}

/// Property key for a path.
pub fn prop_key(path: &[&str]) -> String {
    path.join("\0")
}

/// Signs with a plugin's declared credentials, never handing the key over.
pub trait Signer: Send + Sync {
    /// `request` is the `legion_sign` foreign-function argument (JSON).
    fn sign(&self, request: &[u8]) -> Result<Vec<u8>, String>;
}

/// A plugin's key-value store shared by its instances on one node
/// (`get_shared_data` / `set_shared_data`), scoped by the pool that owns it.
#[derive(Debug, Default)]
pub struct SharedData {
    entries: Mutex<HashMap<String, (Vec<u8>, u32)>>,
}

/// The most keys and bytes one scope's shared data holds.
pub const SHARED_DATA_MAX_KEYS: usize = 4096;
pub const SHARED_DATA_MAX_BYTES: usize = 16 * 1024 * 1024;

impl SharedData {
    pub fn get(&self, key: &str) -> Option<(Vec<u8>, u32)> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .cloned()
    }

    /// Set with an optional compare-and-swap (`cas` 0 means unconditional).
    pub fn set(&self, key: &str, value: Vec<u8>, cas: u32) -> i32 {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let current = entries.get(key).map(|(_, cas)| *cas);
        if cas != 0 && current.is_some_and(|c| c != cas) {
            return status::CAS_MISMATCH;
        }
        if current.is_none() && entries.len() >= SHARED_DATA_MAX_KEYS {
            return status::INTERNAL_FAILURE;
        }
        let used: usize = entries
            .iter()
            .filter(|(k, _)| k.as_str() != key)
            .map(|(k, (v, _))| k.len() + v.len())
            .sum();
        if used + key.len() + value.len() > SHARED_DATA_MAX_BYTES {
            return status::INTERNAL_FAILURE;
        }
        let next = current.unwrap_or(0).wrapping_add(1).max(1);
        entries.insert(key.to_string(), (value, next));
        status::OK
    }
}

/// A plugin's metrics (`define_metric` and friends), per scope.
#[derive(Debug, Default)]
pub struct PluginMetrics {
    pub entries: Mutex<Vec<(String, i32, i64)>>,
}

/// The most metrics one plugin scope may define.
pub const MAX_PLUGIN_METRICS: usize = 64;

/// One log line from a module.
#[derive(Debug, Clone)]
pub struct LogLine {
    pub level: i32,
    pub message: String,
    pub context: u32,
}

/// What every instance of one (plugin revision, scope, config) shares.
pub struct Scope {
    pub plugin: String,
    pub revision: u64,
    /// `get_plugin_configuration`: the reference's `config` as JSON.
    pub configuration: Vec<u8>,
    pub fuel: u64,
    pub deadline: Duration,
    pub memory_bytes: usize,
    pub max_body_bytes: u64,
    pub shared: Arc<SharedData>,
    pub metrics: Arc<PluginMetrics>,
    pub signer: Option<Arc<dyn Signer>>,
    /// `dispatch_http_call` targets by name.
    pub targets: BTreeMap<String, String>,
    /// Root-level properties (`legion.plugin`, `legion.node`, `plugin_name`).
    pub properties: BTreeMap<String, Vec<u8>>,
    pub log: Arc<dyn Fn(&Scope, &LogLine) + Send + Sync>,
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scope")
            .field("plugin", &self.plugin)
            .field("revision", &self.revision)
            .finish()
    }
}

/// Memory cap for one instance. It remembers that it refused, so the trap
/// that follows (the module's allocator gives up) reads as the limit it is.
struct MemoryCap {
    max: usize,
    refused: bool,
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

/// The store's data: the scope, the context the current callback is for,
/// and the instance's bookkeeping.
pub struct HostState {
    cap: MemoryCap,
    pub scope: Arc<Scope>,
    pub ctx: Option<ContextState>,
    next_token: u32,
}

/// How one callback went.
#[derive(Debug)]
pub struct CallOutcome {
    /// The callback's return value (an `Action` or a bool), or 0 when it
    /// returns nothing or the module does not export it.
    pub result: Result<i32, String>,
    pub elapsed: Duration,
    /// Fuel the callback burned.
    pub fuel: u64,
    /// Whether the module exports the callback at all.
    pub exported: bool,
}

/// One started, configured instance.
pub struct Vm {
    store: Store<HostState>,
    instance: wasmi::Instance,
    pub abi: AbiVersion,
    next_context: u32,
    /// Why the instance may not run again.
    pub broken: Option<String>,
}

impl std::fmt::Debug for Vm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vm")
            .field("abi", &self.abi)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Vm {
    /// Instantiate, run the module's initializer, and start and configure
    /// the root context.
    pub fn start(compiled: &Compiled, scope: Arc<Scope>) -> Result<Vm, String> {
        let mut store = Store::new(
            engine(),
            HostState {
                cap: MemoryCap {
                    max: scope.memory_bytes,
                    refused: false,
                },
                scope: scope.clone(),
                ctx: None,
                next_token: 1,
            },
        );
        store.limiter(|state| &mut state.cap);
        // Initialization runs the module's constructors and its root context;
        // it gets ten callbacks' worth of fuel.
        store
            .set_fuel(scope.fuel.saturating_mul(10))
            .map_err(|err| err.to_string())?;
        let instance = compiled
            .linker
            .instantiate_and_start(&mut store, &compiled.module)
            .map_err(|err| format!("failed to instantiate: {err}"))?;
        let mut vm = Vm {
            store,
            instance,
            abi: compiled.abi,
            next_context: ROOT_CONTEXT + 1,
            broken: None,
        };
        for init in ["_initialize", "_start"] {
            if vm.instance.get_func(&vm.store, init).is_some() {
                let outcome = vm.invoke(init, &[], None, false).0;
                if let Err(err) = outcome.result {
                    return Err(format!("{init} failed: {err}"));
                }
                break;
            }
        }
        let vm_configuration = 0;
        let configuration = scope.configuration.len() as i32;
        let steps: [(&str, Vec<i32>, bool); 3] = [
            (
                "proxy_on_context_create",
                vec![ROOT_CONTEXT as i32, 0],
                false,
            ),
            (
                "proxy_on_vm_start",
                vec![ROOT_CONTEXT as i32, vm_configuration],
                true,
            ),
            (
                "proxy_on_configure",
                vec![ROOT_CONTEXT as i32, configuration],
                true,
            ),
        ];
        // Start-up runs on the initialization budget above, not the
        // per-callback deadline: it happens once per instance, off the
        // request's critical path only when a pool grows.
        for (name, args, must_accept) in steps {
            let outcome = vm.invoke(name, &args, None, false).0;
            match outcome.result {
                Err(err) => return Err(format!("{name} failed: {err}")),
                Ok(0) if must_accept && outcome.exported => {
                    return Err(format!(
                        "the module refused to start ({name} returned false); its logs say why"
                    ))
                }
                Ok(_) => {}
            }
        }
        Ok(vm)
    }

    /// Create a stream context for one request or connection.
    pub fn new_context(&mut self) -> Result<ContextState, String> {
        let id = self.next_context;
        self.next_context = self.next_context.wrapping_add(1).max(ROOT_CONTEXT + 1);
        let outcome = self
            .invoke(
                "proxy_on_context_create",
                &[id as i32, ROOT_CONTEXT as i32],
                None,
                true,
            )
            .0;
        outcome.result.map(|_| ContextState {
            id,
            ..Default::default()
        })
    }

    /// Run one callback for `ctx`. The context comes back with whatever the
    /// module did to it.
    pub fn call(
        &mut self,
        name: &str,
        args: &[i32],
        ctx: ContextState,
    ) -> (CallOutcome, ContextState) {
        let (outcome, ctx) = self.invoke(name, args, Some(ctx), true);
        (outcome, ctx.unwrap_or_default())
    }

    /// The bytes `get_plugin_configuration` returns, for tests.
    pub fn scope(&self) -> &Arc<Scope> {
        &self.store.data().scope
    }

    /// The headers callback's arguments for this ABI: 0.2.0 has no
    /// `end_of_stream`.
    pub fn headers_args(&self, context: u32, count: usize, end_of_stream: bool) -> Vec<i32> {
        match self.abi {
            AbiVersion::V0_2_0 => vec![context as i32, count as i32],
            AbiVersion::V0_2_1 => vec![context as i32, count as i32, end_of_stream as i32],
        }
    }

    fn invoke(
        &mut self,
        name: &str,
        args: &[i32],
        ctx: Option<ContextState>,
        metered: bool,
    ) -> (CallOutcome, Option<ContextState>) {
        if let Some(broken) = self.broken.clone() {
            return (
                CallOutcome {
                    result: Err(broken),
                    elapsed: Duration::ZERO,
                    fuel: 0,
                    exported: true,
                },
                ctx,
            );
        }
        let Some(func) = self.instance.get_func(&self.store, name) else {
            return (
                CallOutcome {
                    result: Ok(0),
                    elapsed: Duration::ZERO,
                    fuel: 0,
                    exported: false,
                },
                ctx,
            );
        };
        let ty = func.ty(&self.store);
        let inputs: Vec<Val> = ty
            .params()
            .iter()
            .enumerate()
            .map(|(i, ty)| match ty {
                ValType::I64 => Val::I64(args.get(i).copied().unwrap_or(0) as i64),
                _ => Val::I32(args.get(i).copied().unwrap_or(0)),
            })
            .collect();
        let mut outputs: Vec<Val> = ty.results().iter().map(|_| Val::I32(0)).collect();
        if metered {
            let fuel = self.store.data().scope.fuel;
            let _ = self.store.set_fuel(fuel);
        }
        self.store.data_mut().ctx = ctx;
        let started = Instant::now();
        let called = func.call(&mut self.store, &inputs, &mut outputs);
        let elapsed = started.elapsed();
        let fuel = if metered {
            self.store
                .data()
                .scope
                .fuel
                .saturating_sub(self.store.get_fuel().unwrap_or(0))
        } else {
            0
        };
        let ctx = self.store.data_mut().ctx.take();
        let result = match called {
            Ok(()) => {
                let deadline = self.store.data().scope.deadline;
                if metered && elapsed > deadline {
                    Err(format!(
                        "exceeded its {} deadline ({} µs)",
                        humanize(deadline),
                        elapsed.as_micros()
                    ))
                } else {
                    Ok(match outputs.first() {
                        Some(Val::I32(value)) => *value,
                        Some(Val::I64(value)) => *value as i32,
                        _ => 0,
                    })
                }
            }
            Err(err) => {
                let message = if self.store.data().cap.refused {
                    format!(
                        "exceeded its memory limit ({} bytes)",
                        self.store.data().scope.memory_bytes
                    )
                } else if err.as_trap_code() == Some(TrapCode::OutOfFuel) {
                    format!(
                        "exceeded its {} deadline (ran out of fuel)",
                        humanize(self.store.data().scope.deadline)
                    )
                } else if let Some(code) = err.i32_exit_status() {
                    format!("exited (proc_exit {code})")
                } else {
                    format!("trapped in {name}: {err}")
                };
                self.broken = Some(message.clone());
                Err(message)
            }
        };
        (
            CallOutcome {
                result,
                elapsed,
                fuel,
                exported: true,
            },
            ctx,
        )
    }
}

fn humanize(duration: Duration) -> String {
    if duration.as_millis() >= 1000 && duration.as_millis() % 1000 == 0 {
        format!("{}s", duration.as_secs())
    } else if duration.as_micros() % 1000 == 0 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{}µs", duration.as_micros())
    }
}

type HostCaller<'a> = Caller<'a, HostState>;

fn memory(caller: &HostCaller<'_>) -> Result<wasmi::Memory, wasmi::Error> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| wasmi::Error::new("the module exports no memory"))
}

fn read(caller: &HostCaller<'_>, ptr: i32, len: i32) -> Result<Vec<u8>, wasmi::Error> {
    let (ptr, len) = (ptr as u32 as usize, len as u32 as usize);
    let memory = memory(caller)?;
    if ptr.saturating_add(len) > memory.data_size(caller) {
        return Err(wasmi::Error::new("read outside the module's memory"));
    }
    let mut out = vec![0u8; len];
    memory
        .read(caller, ptr, &mut out)
        .map_err(|_| wasmi::Error::new("read outside the module's memory"))?;
    Ok(out)
}

fn read_string(caller: &HostCaller<'_>, ptr: i32, len: i32) -> Result<String, wasmi::Error> {
    Ok(String::from_utf8_lossy(&read(caller, ptr, len)?).into_owned())
}

fn write(caller: &mut HostCaller<'_>, ptr: i32, data: &[u8]) -> Result<(), wasmi::Error> {
    let memory = memory(caller)?;
    memory
        .write(caller, ptr as u32 as usize, data)
        .map_err(|_| wasmi::Error::new("write outside the module's memory"))
}

fn write_u32(caller: &mut HostCaller<'_>, ptr: i32, value: u32) -> Result<(), wasmi::Error> {
    write(caller, ptr, &value.to_le_bytes())
}

/// Copy `data` into memory the module allocates, and write the pointer and
/// length to the return slots. Empty data returns a null pointer, which the
/// SDKs read as "none".
fn give(
    caller: &mut HostCaller<'_>,
    data: &[u8],
    ptr_ptr: i32,
    size_ptr: i32,
) -> Result<i32, wasmi::Error> {
    if data.is_empty() {
        write_u32(caller, ptr_ptr, 0)?;
        write_u32(caller, size_ptr, 0)?;
        return Ok(status::OK);
    }
    let alloc = caller
        .get_export("proxy_on_memory_allocate")
        .or_else(|| caller.get_export("malloc"))
        .and_then(Extern::into_func)
        .ok_or_else(|| wasmi::Error::new("the module exports no allocator"))?;
    let mut out = [Val::I32(0)];
    alloc.call(&mut *caller, &[Val::I32(data.len() as i32)], &mut out)?;
    let Val::I32(ptr) = out[0] else {
        return Err(wasmi::Error::new("the allocator returned a non-i32"));
    };
    if ptr == 0 {
        return Err(wasmi::Error::new("the module's allocator returned null"));
    }
    write(caller, ptr, data)?;
    write_u32(caller, ptr_ptr, ptr as u32)?;
    write_u32(caller, size_ptr, data.len() as u32)?;
    Ok(status::OK)
}

fn header_map<'a>(ctx: &'a mut ContextState, map_type: i32) -> Option<&'a mut Option<Pairs>> {
    match map_type {
        map::HTTP_REQUEST_HEADERS => Some(&mut ctx.request_headers),
        map::HTTP_REQUEST_TRAILERS => Some(&mut ctx.request_trailers),
        map::HTTP_RESPONSE_HEADERS => Some(&mut ctx.response_headers),
        map::HTTP_RESPONSE_TRAILERS => Some(&mut ctx.response_trailers),
        _ => None,
    }
}

fn mark_changed(ctx: &mut ContextState, map_type: i32) {
    match map_type {
        map::HTTP_REQUEST_HEADERS | map::HTTP_REQUEST_TRAILERS => {
            ctx.request_headers_changed = true
        }
        map::HTTP_RESPONSE_HEADERS | map::HTTP_RESPONSE_TRAILERS => {
            ctx.response_headers_changed = true
        }
        _ => {}
    }
}

/// A header map the current context exposes, for reads (call responses
/// included).
fn read_map(state: &HostState, map_type: i32) -> Option<Pairs> {
    let ctx = state.ctx.as_ref()?;
    match map_type {
        map::HTTP_REQUEST_HEADERS => ctx.request_headers.clone(),
        map::HTTP_REQUEST_TRAILERS => ctx.request_trailers.clone(),
        map::HTTP_RESPONSE_HEADERS => ctx.response_headers.clone(),
        map::HTTP_RESPONSE_TRAILERS => ctx.response_trailers.clone(),
        map::HTTP_CALL_RESPONSE_HEADERS => ctx.call_response.as_ref().map(|r| r.headers.clone()),
        map::HTTP_CALL_RESPONSE_TRAILERS => ctx.call_response.as_ref().map(|r| r.trailers.clone()),
        _ => None,
    }
}

fn read_buffer(state: &HostState, buffer_type: i32) -> Option<Vec<u8>> {
    match buffer_type {
        buffer::PLUGIN_CONFIGURATION => Some(state.scope.configuration.clone()),
        buffer::VM_CONFIGURATION => Some(Vec::new()),
        _ => {
            let ctx = state.ctx.as_ref()?;
            match buffer_type {
                buffer::HTTP_REQUEST_BODY => ctx.request_body.clone(),
                buffer::HTTP_RESPONSE_BODY => ctx.response_body.clone(),
                buffer::DOWNSTREAM_DATA => ctx.downstream.clone(),
                buffer::HTTP_CALL_RESPONSE_BODY => {
                    ctx.call_response.as_ref().map(|r| r.body.clone())
                }
                _ => None,
            }
        }
    }
}

fn property(state: &HostState, key: &str) -> Option<Vec<u8>> {
    if let Some(value) = state.ctx.as_ref().and_then(|ctx| ctx.properties.get(key)) {
        return Some(value.clone());
    }
    state.scope.properties.get(key).cloned()
}

/// The host functions every module may import, plus a stub for any other
/// import so a module that imports a call this host does not implement
/// still instantiates and gets `Unimplemented` (or `ENOSYS` for WASI) if it
/// ever makes it.
fn host_linker(module: &Module) -> Result<Linker<HostState>, String> {
    let mut linker: Linker<HostState> = Linker::new(engine());
    let mut defined: HashSet<(String, String)> = HashSet::new();
    macro_rules! env {
        ($name:literal, $func:expr) => {{
            linker
                .func_wrap("env", $name, $func)
                .map_err(|err| err.to_string())?;
            defined.insert(("env".to_string(), $name.to_string()));
        }};
    }
    macro_rules! wasi {
        ($name:literal, $func:expr) => {{
            linker
                .func_wrap("wasi_snapshot_preview1", $name, $func)
                .map_err(|err| err.to_string())?;
            defined.insert(("wasi_snapshot_preview1".to_string(), $name.to_string()));
        }};
    }

    env!("proxy_log", |caller: HostCaller<'_>,
                       level: i32,
                       ptr: i32,
                       len: i32|
     -> Result<i32, wasmi::Error> {
        let message = read_string(&caller, ptr, len)?;
        let state = caller.data();
        let line = LogLine {
            level,
            message,
            context: state.ctx.as_ref().map(|c| c.id).unwrap_or(ROOT_CONTEXT),
        };
        (state.scope.log)(&state.scope, &line);
        Ok(status::OK)
    });
    env!("proxy_get_log_level", |mut caller: HostCaller<'_>,
                                 ptr: i32|
     -> Result<i32, wasmi::Error> {
        write_u32(&mut caller, ptr, 1)?;
        Ok(status::OK)
    });
    env!(
        "proxy_get_current_time_nanoseconds",
        |mut caller: HostCaller<'_>, ptr: i32| -> Result<i32, wasmi::Error> {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default();
            write(&mut caller, ptr, &now.to_le_bytes())?;
            Ok(status::OK)
        }
    );
    env!(
        "proxy_set_tick_period_milliseconds",
        |_caller: HostCaller<'_>, _period: i32| -> i32 { status::UNIMPLEMENTED }
    );
    env!("proxy_get_buffer_bytes", |mut caller: HostCaller<'_>,
                                    buffer_type: i32,
                                    start: i32,
                                    max: i32,
                                    ptr_ptr: i32,
                                    size_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let Some(data) = read_buffer(caller.data(), buffer_type) else {
            return Ok(status::NOT_FOUND);
        };
        // Offsets and lengths are u32 in the ABI: an SDK asks for
        // everything with `usize::MAX`, which arrives as -1.
        let start = (start as u32 as usize).min(data.len());
        let end = start.saturating_add(max as u32 as usize).min(data.len());
        let slice = data[start..end].to_vec();
        give(&mut caller, &slice, ptr_ptr, size_ptr)
    });
    env!("proxy_set_buffer_bytes", |mut caller: HostCaller<'_>,
                                    buffer_type: i32,
                                    start: i32,
                                    size: i32,
                                    ptr: i32,
                                    len: i32|
     -> Result<i32, wasmi::Error> {
        let data = read(&caller, ptr, len)?;
        let Some(ctx) = caller.data_mut().ctx.as_mut() else {
            return Ok(status::NOT_FOUND);
        };
        let (slot, changed) = match buffer_type {
            buffer::HTTP_REQUEST_BODY => (&mut ctx.request_body, &mut ctx.request_body_changed),
            buffer::HTTP_RESPONSE_BODY => (&mut ctx.response_body, &mut ctx.response_body_changed),
            _ => return Ok(status::BAD_ARGUMENT),
        };
        let Some(current) = slot.as_mut() else {
            return Ok(status::NOT_FOUND);
        };
        // Replace `size` bytes at `start` with the data: (0, len) is a
        // full replace in every SDK, (len, 0) an append.
        let start = (start as u32 as usize).min(current.len());
        let end = start
            .saturating_add(size as u32 as usize)
            .min(current.len());
        current.splice(start..end, data);
        *changed = true;
        Ok(status::OK)
    });
    env!("proxy_get_header_map_pairs", |mut caller: HostCaller<
        '_,
    >,
                                        map_type: i32,
                                        ptr_ptr: i32,
                                        size_ptr: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let Some(pairs) = read_map(caller.data(), map_type) else {
            return Ok(status::NOT_FOUND);
        };
        give(&mut caller, &abi::encode_pairs(&pairs), ptr_ptr, size_ptr)
    });
    env!("proxy_set_header_map_pairs", |mut caller: HostCaller<
        '_,
    >,
                                        map_type: i32,
                                        ptr: i32,
                                        len: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let bytes = read(&caller, ptr, len)?;
        let Some(pairs) = abi::decode_pairs(&bytes) else {
            return Ok(status::PARSE_FAILURE);
        };
        let Some(ctx) = caller.data_mut().ctx.as_mut() else {
            return Ok(status::NOT_FOUND);
        };
        let Some(slot) = header_map(ctx, map_type) else {
            return Ok(status::BAD_ARGUMENT);
        };
        *slot = Some(pairs);
        mark_changed(ctx, map_type);
        Ok(status::OK)
    });
    env!("proxy_get_header_map_value", |mut caller: HostCaller<
        '_,
    >,
                                        map_type: i32,
                                        key_ptr: i32,
                                        key_len: i32,
                                        ptr_ptr: i32,
                                        size_ptr: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let key = read_string(&caller, key_ptr, key_len)?.to_ascii_lowercase();
        let Some(pairs) = read_map(caller.data(), map_type) else {
            return Ok(status::NOT_FOUND);
        };
        match pairs.iter().find(|(name, _)| *name == key) {
            Some((_, value)) => {
                let value = value.clone();
                if value.is_empty() {
                    // Present but empty: a non-null, zero-length pointer
                    // (the SDKs read null as absent), which needs no
                    // allocation to free.
                    write_u32(&mut caller, ptr_ptr, 1)?;
                    write_u32(&mut caller, size_ptr, 0)?;
                    Ok(status::OK)
                } else {
                    give(&mut caller, &value, ptr_ptr, size_ptr)
                }
            }
            None => Ok(status::NOT_FOUND),
        }
    });
    env!(
        "proxy_replace_header_map_value",
        |mut caller: HostCaller<'_>,
         map_type: i32,
         key_ptr: i32,
         key_len: i32,
         value_ptr: i32,
         value_len: i32|
         -> Result<i32, wasmi::Error> {
            let key = read_string(&caller, key_ptr, key_len)?.to_ascii_lowercase();
            let value = read(&caller, value_ptr, value_len)?;
            edit_map(&mut caller, map_type, |pairs| {
                pairs.retain(|(name, _)| *name != key);
                pairs.push((key.clone(), value.clone()));
            })
        }
    );
    env!("proxy_add_header_map_value", |mut caller: HostCaller<
        '_,
    >,
                                        map_type: i32,
                                        key_ptr: i32,
                                        key_len: i32,
                                        value_ptr: i32,
                                        value_len: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let key = read_string(&caller, key_ptr, key_len)?.to_ascii_lowercase();
        let value = read(&caller, value_ptr, value_len)?;
        edit_map(&mut caller, map_type, |pairs| {
            pairs.push((key.clone(), value.clone()))
        })
    });
    env!("proxy_remove_header_map_value", |mut caller: HostCaller<
        '_,
    >,
                                           map_type: i32,
                                           key_ptr: i32,
                                           key_len: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let key = read_string(&caller, key_ptr, key_len)?.to_ascii_lowercase();
        edit_map(&mut caller, map_type, |pairs| {
            pairs.retain(|(name, _)| *name != key)
        })
    });
    env!("proxy_get_property", |mut caller: HostCaller<'_>,
                                path_ptr: i32,
                                path_len: i32,
                                ptr_ptr: i32,
                                size_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let path = read(&caller, path_ptr, path_len)?;
        let key = abi::decode_path(&path).join("\0");
        match property(caller.data(), &key) {
            Some(value) => give(&mut caller, &value, ptr_ptr, size_ptr),
            None => Ok(status::NOT_FOUND),
        }
    });
    env!("proxy_set_property", |mut caller: HostCaller<'_>,
                                path_ptr: i32,
                                path_len: i32,
                                value_ptr: i32,
                                value_len: i32|
     -> Result<i32, wasmi::Error> {
        let path = read(&caller, path_ptr, path_len)?;
        let value = read(&caller, value_ptr, value_len)?;
        let key = abi::decode_path(&path).join("\0");
        // The host's own facts are read-only.
        if key.starts_with("legion\0") && !key.starts_with("legion\0tags\0") {
            return Ok(status::BAD_ARGUMENT);
        }
        let Some(ctx) = caller.data_mut().ctx.as_mut() else {
            return Ok(status::NOT_FOUND);
        };
        if ctx.properties.len() >= 256 {
            return Ok(status::INTERNAL_FAILURE);
        }
        ctx.properties.insert(key, value);
        Ok(status::OK)
    });
    env!("proxy_get_shared_data", |mut caller: HostCaller<'_>,
                                   key_ptr: i32,
                                   key_len: i32,
                                   ptr_ptr: i32,
                                   size_ptr: i32,
                                   cas_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let key = read_string(&caller, key_ptr, key_len)?;
        let entry = caller.data().scope.shared.get(&key);
        match entry {
            Some((value, cas)) => {
                give(&mut caller, &value, ptr_ptr, size_ptr)?;
                write_u32(&mut caller, cas_ptr, cas)?;
                Ok(status::OK)
            }
            None => Ok(status::NOT_FOUND),
        }
    });
    env!("proxy_set_shared_data", |caller: HostCaller<'_>,
                                   key_ptr: i32,
                                   key_len: i32,
                                   value_ptr: i32,
                                   value_len: i32,
                                   cas: i32|
     -> Result<i32, wasmi::Error> {
        let key = read_string(&caller, key_ptr, key_len)?;
        let value = read(&caller, value_ptr, value_len)?;
        Ok(caller.data().scope.shared.set(&key, value, cas as u32))
    });
    env!("proxy_continue_stream", |mut caller: HostCaller<'_>,
                                   _stream: i32|
     -> i32 {
        match caller.data_mut().ctx.as_mut() {
            Some(ctx) => {
                ctx.resumed = true;
                status::OK
            }
            None => status::NOT_FOUND,
        }
    });
    env!("proxy_close_stream", |mut caller: HostCaller<'_>,
                                stream_type: i32|
     -> i32 {
        match caller.data_mut().ctx.as_mut() {
            Some(ctx) => {
                if matches!(
                    stream_type,
                    stream::DOWNSTREAM
                        | stream::UPSTREAM
                        | stream::HTTP_REQUEST
                        | stream::HTTP_RESPONSE
                ) {
                    ctx.closed = true;
                }
                status::OK
            }
            None => status::NOT_FOUND,
        }
    });
    // The 0.2.0 names of the same two calls.
    env!(
        "proxy_continue_request",
        |mut caller: HostCaller<'_>| -> i32 {
            if let Some(ctx) = caller.data_mut().ctx.as_mut() {
                ctx.resumed = true;
            }
            status::OK
        }
    );
    env!(
        "proxy_continue_response",
        |mut caller: HostCaller<'_>| -> i32 {
            if let Some(ctx) = caller.data_mut().ctx.as_mut() {
                ctx.resumed = true;
            }
            status::OK
        }
    );
    env!("proxy_send_local_response", |mut caller: HostCaller<'_>,
                                       status_code: i32,
                                       details_ptr: i32,
                                       details_len: i32,
                                       body_ptr: i32,
                                       body_len: i32,
                                       headers_ptr: i32,
                                       headers_len: i32,
                                       _grpc_status: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let details = read_string(&caller, details_ptr, details_len)?;
        let body = read(&caller, body_ptr, body_len)?;
        let headers = if headers_len > 0 {
            abi::decode_pairs(&read(&caller, headers_ptr, headers_len)?).unwrap_or_default()
        } else {
            Vec::new()
        };
        let Some(ctx) = caller.data_mut().ctx.as_mut() else {
            return Ok(status::NOT_FOUND);
        };
        if !(100..=599).contains(&status_code) {
            return Ok(status::BAD_ARGUMENT);
        }
        ctx.local_reply = Some(LocalReply {
            status: status_code as u16,
            details,
            body,
            headers,
        });
        Ok(status::OK)
    });
    env!("proxy_http_call", |mut caller: HostCaller<'_>,
                             upstream_ptr: i32,
                             upstream_len: i32,
                             headers_ptr: i32,
                             headers_len: i32,
                             body_ptr: i32,
                             body_len: i32,
                             _trailers_ptr: i32,
                             _trailers_len: i32,
                             timeout_ms: i32,
                             token_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let target = read_string(&caller, upstream_ptr, upstream_len)?;
        if !caller.data().scope.targets.contains_key(&target) {
            return Ok(status::BAD_ARGUMENT);
        }
        let headers =
            abi::decode_pairs(&read(&caller, headers_ptr, headers_len)?).unwrap_or_default();
        let body = read(&caller, body_ptr, body_len)?;
        let token = caller.data().next_token;
        {
            let state = caller.data_mut();
            state.next_token = state.next_token.wrapping_add(1).max(1);
            let Some(ctx) = state.ctx.as_mut() else {
                return Ok(status::BAD_ARGUMENT);
            };
            if ctx.calls.len() >= 8 {
                return Ok(status::INTERNAL_FAILURE);
            }
            ctx.calls.push(PendingCall {
                token,
                target,
                headers,
                body,
                timeout: Duration::from_millis(timeout_ms.clamp(1, 60_000) as u64),
            });
        }
        write_u32(&mut caller, token_ptr, token)?;
        Ok(status::OK)
    });
    env!("proxy_call_foreign_function", |mut caller: HostCaller<
        '_,
    >,
                                         name_ptr: i32,
                                         name_len: i32,
                                         args_ptr: i32,
                                         args_len: i32,
                                         ptr_ptr: i32,
                                         size_ptr: i32|
     -> Result<
        i32,
        wasmi::Error,
    > {
        let name = read_string(&caller, name_ptr, name_len)?;
        let args = read(&caller, args_ptr, args_len)?;
        if name != "legion_sign" {
            return Ok(status::NOT_FOUND);
        }
        let Some(signer) = caller.data().scope.signer.clone() else {
            return Ok(status::NOT_FOUND);
        };
        match signer.sign(&args) {
            Ok(signature) => give(&mut caller, &signature, ptr_ptr, size_ptr),
            Err(err) => {
                let line = LogLine {
                    level: 3,
                    message: format!("legion_sign refused: {err}"),
                    context: caller
                        .data()
                        .ctx
                        .as_ref()
                        .map(|c| c.id)
                        .unwrap_or(ROOT_CONTEXT),
                };
                let scope = caller.data().scope.clone();
                (scope.log)(&scope, &line);
                Ok(status::BAD_ARGUMENT)
            }
        }
    });
    env!("proxy_set_effective_context", |_caller: HostCaller<'_>,
                                         _context: i32|
     -> i32 { status::OK });
    env!("proxy_done", |_caller: HostCaller<'_>| -> i32 {
        status::OK
    });
    env!("proxy_define_metric", |mut caller: HostCaller<'_>,
                                 metric_type: i32,
                                 name_ptr: i32,
                                 name_len: i32,
                                 id_ptr: i32|
     -> Result<i32, wasmi::Error> {
        let name = read_string(&caller, name_ptr, name_len)?;
        let id = {
            let metrics = caller.data().scope.metrics.clone();
            let mut entries = metrics.entries.lock().unwrap_or_else(|p| p.into_inner());
            match entries.iter().position(|(n, _, _)| *n == name) {
                Some(index) => index,
                None if entries.len() >= MAX_PLUGIN_METRICS => return Ok(status::INTERNAL_FAILURE),
                None => {
                    entries.push((name, metric_type, 0));
                    entries.len() - 1
                }
            }
        };
        write_u32(&mut caller, id_ptr, id as u32)?;
        Ok(status::OK)
    });
    env!("proxy_increment_metric", |caller: HostCaller<'_>,
                                    id: i32,
                                    offset: i64|
     -> i32 {
        let metrics = caller.data().scope.metrics.clone();
        let mut entries = metrics.entries.lock().unwrap_or_else(|p| p.into_inner());
        match entries.get_mut(id as usize) {
            Some(entry) => {
                entry.2 = entry.2.saturating_add(offset);
                status::OK
            }
            None => status::NOT_FOUND,
        }
    });
    env!("proxy_record_metric", |caller: HostCaller<'_>,
                                 id: i32,
                                 value: i64|
     -> i32 {
        let metrics = caller.data().scope.metrics.clone();
        let mut entries = metrics.entries.lock().unwrap_or_else(|p| p.into_inner());
        match entries.get_mut(id as usize) {
            Some(entry) => {
                entry.2 = value;
                status::OK
            }
            None => status::NOT_FOUND,
        }
    });
    env!("proxy_get_metric", |mut caller: HostCaller<'_>,
                              id: i32,
                              ptr: i32|
     -> Result<i32, wasmi::Error> {
        let value = {
            let metrics = caller.data().scope.metrics.clone();
            let entries = metrics.entries.lock().unwrap_or_else(|p| p.into_inner());
            entries.get(id as usize).map(|entry| entry.2)
        };
        match value {
            Some(value) => {
                write(&mut caller, ptr, &(value as u64).to_le_bytes())?;
                Ok(status::OK)
            }
            None => Ok(status::NOT_FOUND),
        }
    });

    // WASI, as much as a Rust or TinyGo module's runtime touches: no
    // filesystem, no environment, no arguments. Standard output and error go
    // to the plugin log.
    wasi!("fd_write", |mut caller: HostCaller<'_>,
                       fd: i32,
                       iovs: i32,
                       count: i32,
                       written_ptr: i32|
     -> Result<i32, wasmi::Error> {
        if fd != 1 && fd != 2 {
            return Ok(8);
        }
        let mut text = Vec::new();
        for i in 0..count.max(0) {
            let iov = read(&caller, iovs + i * 8, 8)?;
            let ptr = u32::from_le_bytes(iov[0..4].try_into().unwrap()) as i32;
            let len = u32::from_le_bytes(iov[4..8].try_into().unwrap()) as i32;
            text.extend(read(&caller, ptr, len)?);
        }
        write_u32(&mut caller, written_ptr, text.len() as u32)?;
        let message = String::from_utf8_lossy(&text).trim_end().to_string();
        if !message.is_empty() {
            let state = caller.data();
            let line = LogLine {
                level: if fd == 2 { 3 } else { 2 },
                message,
                context: state.ctx.as_ref().map(|c| c.id).unwrap_or(ROOT_CONTEXT),
            };
            (state.scope.log)(&state.scope, &line);
        }
        Ok(0)
    });
    wasi!("environ_sizes_get", |mut caller: HostCaller<'_>,
                                count_ptr: i32,
                                size_ptr: i32|
     -> Result<i32, wasmi::Error> {
        write_u32(&mut caller, count_ptr, 0)?;
        write_u32(&mut caller, size_ptr, 0)?;
        Ok(0)
    });
    wasi!("environ_get", |_caller: HostCaller<'_>,
                          _a: i32,
                          _b: i32|
     -> i32 { 0 });
    wasi!("args_sizes_get", |mut caller: HostCaller<'_>,
                             count_ptr: i32,
                             size_ptr: i32|
     -> Result<i32, wasmi::Error> {
        write_u32(&mut caller, count_ptr, 0)?;
        write_u32(&mut caller, size_ptr, 0)?;
        Ok(0)
    });
    wasi!("args_get", |_caller: HostCaller<'_>,
                       _a: i32,
                       _b: i32|
     -> i32 { 0 });
    wasi!("clock_time_get", |mut caller: HostCaller<'_>,
                             id: i32,
                             _precision: i64,
                             ptr: i32|
     -> Result<i32, wasmi::Error> {
        let now = if id == 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default()
        } else {
            monotonic_nanos()
        };
        write(&mut caller, ptr, &now.to_le_bytes())?;
        Ok(0)
    });
    wasi!("random_get", |mut caller: HostCaller<'_>,
                         ptr: i32,
                         len: i32|
     -> Result<i32, wasmi::Error> {
        let mut bytes = vec![0u8; len.clamp(0, 65_536) as usize];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
            .map_err(|_| wasmi::Error::new("no randomness"))?;
        write(&mut caller, ptr, &bytes)?;
        Ok(0)
    });
    wasi!("proc_exit", |_caller: HostCaller<'_>,
                        code: i32|
     -> Result<(), wasmi::Error> {
        Err(wasmi::Error::i32_exit(code))
    });
    wasi!("sched_yield", |_caller: HostCaller<'_>| -> i32 { 0 });

    // Everything else the module imports: a stub that answers
    // `Unimplemented` (env) or `ENOSYS` (WASI), and traps for any other
    // module, so an unused import never stops a module from loading.
    for import in module.imports() {
        let key = (import.module().to_string(), import.name().to_string());
        if defined.contains(&key) {
            continue;
        }
        let wasmi::ExternType::Func(ty) = import.ty() else {
            return Err(format!(
                "the module imports {}.{}, which is not a function; a plugin may import only \
                 functions",
                key.0, key.1
            ));
        };
        let ty = ty.clone();
        let answer = match key.0.as_str() {
            "env" => Some(status::UNIMPLEMENTED as i64),
            "wasi_snapshot_preview1" => Some(52),
            _ => None,
        };
        let label = format!("{}.{}", key.0, key.1);
        linker
            .func_new(&key.0, &key.1, ty, move |_caller, _params, results| {
                let Some(answer) = answer else {
                    return Err(wasmi::Error::new(format!(
                        "called {label}, which this host does not provide"
                    )));
                };
                for result in results.iter_mut() {
                    *result = match result {
                        Val::I64(_) => Val::I64(answer),
                        _ => Val::I32(answer as i32),
                    };
                }
                Ok(())
            })
            .map_err(|err| err.to_string())?;
        defined.insert(key);
    }
    Ok(linker)
}

fn edit_map(
    caller: &mut HostCaller<'_>,
    map_type: i32,
    edit: impl FnOnce(&mut Pairs),
) -> Result<i32, wasmi::Error> {
    let Some(ctx) = caller.data_mut().ctx.as_mut() else {
        return Ok(status::NOT_FOUND);
    };
    let Some(slot) = header_map(ctx, map_type) else {
        return Ok(status::BAD_ARGUMENT);
    };
    let Some(pairs) = slot.as_mut() else {
        return Ok(status::NOT_FOUND);
    };
    edit(pairs);
    mark_changed(ctx, map_type);
    Ok(status::OK)
}

fn monotonic_nanos() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}
