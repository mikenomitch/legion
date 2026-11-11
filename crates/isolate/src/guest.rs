//! One isolate's store data, and the host side of everything a guest can
//! call: WASI (from wasmtime-wasi, filesystem from [`crate::scratch`]),
//! `wasi:http/outgoing-handler` (to the worker), and `legion:isolate`.

use std::collections::BTreeMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use legion_proto::isolate_host::{
    vm_request, EgressErrorKind, EgressRequest, Header, HttpCall, Lifetime, Origin, VmAcquire,
    VmAttach, VmErrorKind, VmExec, VmFetch, VmGet, VmPut, VmRelease, VmReply, VmRequest,
    VmResize,
};
use rand_010::rngs::{StdRng, SysRng};
use rand_010::{RngExt as _, SeedableRng};
use wasmtime::component::{Resource, ResourceTable};
use wasmtime::ResourceLimiter;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiBody, WasiHttpCtx, WasiHttpCtxView, WasiHttpHooks, WasiHttpView};
use wasmtime_wasi_io::streams::{OutputStream, StreamResult};

use crate::account::MemoryAccount;
use crate::scratch::{Scratch, ScratchCtx, ScratchView, RUN_DIR};
use crate::turn;
use crate::world::{control, state, vm};
use crate::worker_api::WorkerApi;

// ---------------------------------------------------------------------------
// Stops
// ---------------------------------------------------------------------------

/// Why an invocation was stopped before it finished by itself; becomes
/// `InvokeReply.stopped`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stop {
    /// `limits`, `unsupported`, `trap`, or `ended`.
    pub reason: &'static str,
    /// For `limits`: `cpu`, `wall`, `memory`, or `output`.
    pub limit: &'static str,
    pub detail: String,
}

impl Stop {
    pub fn limit(limit: &'static str, detail: String) -> Self {
        Self {
            reason: "limits",
            limit,
            detail,
        }
    }
    pub fn new(reason: &'static str, detail: impl Into<String>) -> Self {
        Self {
            reason,
            limit: "",
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invocation stopped ({}): {}", self.reason, self.detail)
    }
}

impl std::error::Error for Stop {}

/// A duration the way limits are written (`100ms`, `2.5s`).
pub fn fmt_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us}µs")
    } else if us < 10_000_000 && us % 1_000_000 != 0 {
        let ms = us as f64 / 1000.0;
        if ms.fract() == 0.0 {
            format!("{}ms", ms as u64)
        } else {
            format!("{ms:.1}ms")
        }
    } else {
        let s = us as f64 / 1_000_000.0;
        if s.fract() == 0.0 {
            format!("{}s", s as u64)
        } else {
            format!("{s:.1}s")
        }
    }
}

// ---------------------------------------------------------------------------
// The current invocation
// ---------------------------------------------------------------------------

/// One invocation's budgets and meters. The store holds the current one;
/// the shard executor charges CPU to it after every turn.
#[derive(Debug)]
pub struct Invocation {
    pub origin: Origin,
    /// CPU charged by finished turns.
    pub cpu_ns: AtomicU64,
    /// 0 = unlimited.
    pub cpu_limit_ns: u64,
    pub wall_limit: Duration,
    pub started: Instant,
    /// Turns longer than this yield to the shard.
    pub slice_ns: u64,
    pub turns: AtomicU64,
    stop: Mutex<Option<Stop>>,
}

impl Invocation {
    pub fn new(origin: Origin, cpu_limit: Duration, wall_limit: Duration, slice: Duration) -> Self {
        Self {
            origin,
            cpu_ns: AtomicU64::new(0),
            cpu_limit_ns: cpu_limit.as_nanos() as u64,
            wall_limit,
            started: Instant::now(),
            slice_ns: slice.as_nanos().max(1) as u64,
            turns: AtomicU64::new(0),
            stop: Mutex::new(None),
        }
    }

    /// An invocation with no limits (host-internal calls).
    pub fn unlimited(origin: Origin) -> Self {
        Self::new(
            origin,
            Duration::ZERO,
            Duration::from_secs(365 * 24 * 3600),
            Duration::from_millis(5),
        )
    }

    pub fn deadline(&self) -> Instant {
        self.started + self.wall_limit
    }

    /// CPU used so far, including the turn running now on this thread.
    pub fn cpu_now_ns(&self) -> u64 {
        self.cpu_ns.load(Ordering::Relaxed) + turn::cpu_in_turn_ns()
    }

    /// Record why the invocation is stopping; the first reason wins.
    pub fn stop(&self, s: Stop) -> Stop {
        let mut g = self.stop.lock().unwrap();
        g.get_or_insert(s).clone()
    }

    pub fn stopped(&self) -> Option<Stop> {
        self.stop.lock().unwrap().clone()
    }

    /// The CPU limit, as a stop, if it has been passed.
    pub fn check_cpu(&self) -> Option<Stop> {
        if self.cpu_limit_ns == 0 {
            return None;
        }
        let used = self.cpu_now_ns();
        (used > self.cpu_limit_ns).then(|| {
            Stop::limit(
                "cpu",
                format!(
                    "exceeded its {} CPU limit ({})",
                    fmt_duration(Duration::from_nanos(self.cpu_limit_ns)),
                    fmt_duration(Duration::from_nanos(used))
                ),
            )
        })
    }

    pub fn wall_stop(&self) -> Stop {
        Stop::limit(
            "wall",
            format!(
                "exceeded its {} wall-clock limit",
                fmt_duration(self.wall_limit)
            ),
        )
    }

    pub fn check_wall(&self) -> Option<Stop> {
        (Instant::now() >= self.deadline()).then(|| self.wall_stop())
    }

    pub fn remaining_wall(&self) -> Duration {
        self.deadline().saturating_duration_since(Instant::now())
    }
}

// ---------------------------------------------------------------------------
// Output capture
// ---------------------------------------------------------------------------

/// stdout and stderr of the current invocation, sharing one byte budget.
/// Past the budget output is dropped and `truncated` set; the guest is
/// never told (`limits.output` truncates, brief item 18).
#[derive(Debug, Default)]
pub struct Output {
    inner: Mutex<OutBuf>,
}

#[derive(Debug, Default)]
struct OutBuf {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    left: u64,
    truncated: bool,
}

/// What one invocation printed.
#[derive(Debug, Default, Clone)]
pub struct Printed {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub truncated: bool,
    /// Budget left for returned files.
    pub left: u64,
}

impl Output {
    pub fn new(budget: u64) -> Arc<Self> {
        let o = Arc::new(Self::default());
        o.reset(budget);
        o
    }

    /// Start a new invocation's capture.
    pub fn reset(&self, budget: u64) {
        let mut g = self.inner.lock().unwrap();
        *g = OutBuf {
            left: budget,
            ..Default::default()
        };
    }

    pub fn push(&self, stderr: bool, bytes: &[u8]) {
        let mut g = self.inner.lock().unwrap();
        let n = (bytes.len() as u64).min(g.left) as usize;
        if n < bytes.len() {
            g.truncated = true;
        }
        g.left -= n as u64;
        let buf = if stderr { &mut g.stderr } else { &mut g.stdout };
        buf.extend_from_slice(&bytes[..n]);
    }

    pub fn take(&self) -> Printed {
        let mut g = self.inner.lock().unwrap();
        Printed {
            stdout: std::mem::take(&mut g.stdout),
            stderr: std::mem::take(&mut g.stderr),
            truncated: g.truncated,
            left: g.left,
        }
    }
}

/// The `StdoutStream` handed to WASI for stdout or stderr.
#[derive(Clone)]
pub struct OutPipe {
    out: Arc<Output>,
    stderr: bool,
}

impl IsTerminal for OutPipe {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for OutPipe {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }
}

impl tokio::io::AsyncWrite for OutPipe {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.out.push(self.stderr, buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[wasmtime_wasi_io::async_trait]
impl wasmtime_wasi_io::poll::Pollable for OutPipe {
    async fn ready(&mut self) {}
}

#[wasmtime_wasi_io::async_trait]
impl OutputStream for OutPipe {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.out.push(self.stderr, &bytes);
        Ok(())
    }
    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }
    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(64 << 10)
    }
}

// ---------------------------------------------------------------------------
// Memory limiter
// ---------------------------------------------------------------------------

/// Charges linear memory to the isolate's [`MemoryAccount`] (shared with
/// its scratch files). A refusal makes `memory.grow` return -1, and the
/// account remembers it so the trap that usually follows is classified as
/// the memory limit.
pub struct Limiter {
    pub account: Arc<MemoryAccount>,
    linear: u64,
    peak_linear: u64,
}

impl Limiter {
    pub fn new(account: Arc<MemoryAccount>) -> Self {
        Self {
            account,
            linear: 0,
            peak_linear: 0,
        }
    }
    /// Linear memory mapped now, across the component's memories.
    pub fn linear(&self) -> u64 {
        self.linear
    }
}

/// Elements one table may grow to.
const MAX_TABLE_ELEMENTS: usize = 200_000;

impl ResourceLimiter for Limiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let next = (self.linear + desired as u64).saturating_sub(current as u64);
        if self.account.set_linear(next) {
            self.linear = next;
            self.peak_linear = self.peak_linear.max(next);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= MAX_TABLE_ELEMENTS)
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// `legion:isolate/state`: bytes a tenant or session keeps, counted
/// key + value against a quota.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StateMap {
    pub map: BTreeMap<String, Vec<u8>>,
    pub bytes: u64,
}

impl StateMap {
    pub fn from_map(map: BTreeMap<String, Vec<u8>>) -> Self {
        let bytes = map.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
        Self { map, bytes }
    }
}

// ---------------------------------------------------------------------------
// The store data
// ---------------------------------------------------------------------------

/// Who an instance is (`control.info`).
#[derive(Debug, Clone)]
pub struct Ident {
    pub job: String,
    pub task: String,
    pub lifetime: Lifetime,
    pub tenant: Option<String>,
    pub session: Option<String>,
}

/// What a new instance's context is built from.
pub struct GuestSetup {
    pub ident: Ident,
    pub env: Vec<(String, String)>,
    pub args: Vec<String>,
    pub memory_max: u64,
    pub has_vm: bool,
    pub egress: bool,
    pub max_body: u64,
    pub state_quota: u64,
    /// `None` for request isolates (state is not available).
    pub state: Option<StateMap>,
    pub resumed: bool,
    pub worker: Arc<dyn WorkerApi>,
}

/// A child VM handle (`legion:isolate/vm.machine`).
pub struct Machine {
    pub id: String,
}

pub struct Ctx {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    pub scratch: Scratch,
    pub http: WasiHttpCtx,
    pub hooks: EgressHooks,
    pub limiter: Limiter,
    pub ident: Ident,
    pub inv: Arc<Invocation>,
    pub out: Arc<Output>,
    pub state: Option<StateMap>,
    pub state_quota: u64,
    /// True until the first invocation after a restore from a checkpoint
    /// finishes.
    pub resumed: bool,
    /// `control.park()` was called during the current invocation.
    pub park: bool,
    pub has_vm: bool,
    pub worker: Arc<dyn WorkerApi>,
}

fn kernel_rng() -> StdRng {
    // A fresh seed from the kernel per instance: two instances made from one
    // artifact (or one pre-initialized image) never share RNG output.
    StdRng::try_from_rng(&mut SysRng).expect("the kernel's random source failed")
}

impl Ctx {
    pub fn new(setup: GuestSetup) -> Self {
        let account = Arc::new(MemoryAccount::new(setup.memory_max));
        let out = Output::new(0);
        let mut secure = kernel_rng();
        let insecure = StdRng::from_rng(&mut secure);
        let seed: u128 = secure.random();
        let mut b = WasiCtx::builder();
        b.stdout(OutPipe {
            out: out.clone(),
            stderr: false,
        })
        .stderr(OutPipe {
            out: out.clone(),
            stderr: true,
        })
        .envs(&setup.env)
        .args(&setup.args)
        .initial_cwd(RUN_DIR)
        .wall_clock(turn::FrozenWallClock)
        .monotonic_clock(turn::FrozenMonotonicClock)
        .secure_random(secure)
        .insecure_random(insecure)
        .insecure_random_seed(seed)
        // Sockets are linked (images import them) but everything is
        // denied: TCP and UDP off, no name lookups, the address check at
        // its deny-all default.
        .allow_tcp(false)
        .allow_udp(false)
        .allow_ip_name_lookup(false);
        let inv = Arc::new(Invocation::unlimited(Origin::default()));
        Self {
            wasi: b.build(),
            table: ResourceTable::new(),
            scratch: Scratch::new(account.clone()),
            http: WasiHttpCtx::new(),
            hooks: EgressHooks {
                worker: setup.worker.clone(),
                inv: inv.clone(),
                enabled: setup.egress,
                max_body: setup.max_body,
            },
            limiter: Limiter::new(account),
            ident: setup.ident,
            inv,
            out,
            state: setup.state,
            state_quota: setup.state_quota,
            resumed: setup.resumed,
            park: false,
            has_vm: setup.has_vm,
            worker: setup.worker,
        }
    }

    /// Point the instance at a new invocation.
    pub fn begin(&mut self, inv: Arc<Invocation>, output_budget: u64) {
        self.hooks.inv = inv.clone();
        self.inv = inv;
        self.out.reset(output_budget);
        self.park = false;
    }

    pub fn account(&self) -> &Arc<MemoryAccount> {
        &self.limiter.account
    }

    fn origin(&self) -> Origin {
        self.inv.origin.clone()
    }
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl wasmtime_wasi_io::IoView for Ctx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl ScratchView for Ctx {
    fn scratch(&mut self) -> ScratchCtx<'_> {
        ScratchCtx {
            scratch: &mut self.scratch,
            table: &mut self.table,
            cwd: RUN_DIR,
        }
    }
}

impl WasiHttpView for Ctx {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.hooks,
        }
    }
}

// ---------------------------------------------------------------------------
// Outbound HTTP: to the worker
// ---------------------------------------------------------------------------

/// `wasi:http/outgoing-handler` sends each request to the worker's
/// `Egress` with the isolate's origin. The host opens no socket.
pub struct EgressHooks {
    worker: Arc<dyn WorkerApi>,
    inv: Arc<Invocation>,
    enabled: bool,
    max_body: u64,
}

type HttpErr = wasmtime_wasi_http::Error;

fn egress_error(kind: EgressErrorKind) -> HttpErr {
    match kind {
        EgressErrorKind::EgressDenied => HttpErr::HttpRequestDenied,
        EgressErrorKind::EgressDns => HttpErr::DestinationNotFound,
        EgressErrorKind::EgressConnect => HttpErr::ConnectionRefused,
        EgressErrorKind::EgressTimeout => HttpErr::ConnectionReadTimeout,
        EgressErrorKind::EgressTooLarge => HttpErr::HttpResponseBodySize(None),
        EgressErrorKind::EgressProtocol => HttpErr::HttpProtocolError,
        EgressErrorKind::EgressUnavailable | EgressErrorKind::EgressErrorUnspecified => {
            HttpErr::DestinationUnavailable
        }
    }
}

fn header_list(h: &http::HeaderMap) -> Vec<Header> {
    h.iter()
        .map(|(n, v)| Header {
            name: n.as_str().to_string(),
            value: v.as_bytes().to_vec(),
        })
        .collect()
}

/// Read a body into memory, refusing past `max` bytes.
pub async fn collect_body<B>(body: B, max: u64) -> Result<Bytes, Option<B::Error>>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
{
    let mut body = body;
    let mut out = bytes::BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(Some)?;
        if let Ok(data) = frame.into_data() {
            if out.len() as u64 + data.len() as u64 > max {
                return Err(None);
            }
            out.extend_from_slice(&data);
        }
    }
    Ok(out.freeze())
}

fn full_body(b: Bytes) -> WasiBody {
    http_body_util::Full::new(b)
        .map_err(|never| match never {})
        .boxed_unsync()
}

impl WasiHttpHooks for EgressHooks {
    fn send_request(
        &mut self,
        request: http::Request<WasiBody>,
        options: Option<wasmtime_wasi_http::RequestOptions>,
        _fut: Box<dyn std::future::Future<Output = Result<(), HttpErr>> + Send>,
    ) -> Box<
        dyn std::future::Future<
                Output = Result<
                    (
                        http::Response<WasiBody>,
                        Box<dyn std::future::Future<Output = Result<(), HttpErr>> + Send>,
                    ),
                    HttpErr,
                >,
            > + Send,
    > {
        let enabled = self.enabled;
        let worker = self.worker.clone();
        let inv = self.inv.clone();
        let max = self.max_body;
        Box::new(async move {
            if !enabled {
                return Err(HttpErr::HttpRequestDenied);
            }
            let (parts, body) = request.into_parts();
            let body = match collect_body(body, max).await {
                Ok(b) => b,
                Err(Some(e)) => return Err(e),
                Err(None) => return Err(HttpErr::HttpRequestBodySize(Some(max))),
            };
            let mut timeout = inv.remaining_wall();
            if let Some(o) = &options {
                for t in [o.connect_timeout, o.first_byte_timeout].into_iter().flatten() {
                    timeout = timeout.min(t);
                }
            }
            let req = EgressRequest {
                origin: Some(inv.origin.clone()),
                method: parts.method.as_str().to_string(),
                url: parts.uri.to_string(),
                headers: header_list(&parts.headers),
                body: body.to_vec(),
                timeout_millis: timeout.as_millis().clamp(1, u32::MAX as u128) as u32,
            };
            let reply = match worker.egress(req).await {
                Ok(r) => r,
                Err(_) => return Err(HttpErr::DestinationUnavailable),
            };
            if let Some(e) = reply.error {
                return Err(egress_error(
                    EgressErrorKind::try_from(e.kind)
                        .unwrap_or(EgressErrorKind::EgressErrorUnspecified),
                ));
            }
            if reply.body.len() as u64 > max {
                return Err(HttpErr::HttpResponseBodySize(Some(max)));
            }
            let mut resp = http::Response::builder().status(
                http::StatusCode::from_u16(reply.status as u16)
                    .map_err(|_| HttpErr::HttpProtocolError)?,
            );
            for h in &reply.headers {
                resp = resp.header(h.name.as_str(), h.value.as_slice());
            }
            let resp = resp
                .body(full_body(Bytes::from(reply.body)))
                .map_err(|_| HttpErr::HttpProtocolError)?;
            Ok((
                resp,
                Box::new(async { Ok(()) }) as Box<dyn std::future::Future<Output = _> + Send>,
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// legion:isolate/control
// ---------------------------------------------------------------------------

fn lifetime(l: Lifetime) -> control::Lifetime {
    match l {
        Lifetime::Request => control::Lifetime::Request,
        Lifetime::Tenant => control::Lifetime::Tenant,
        Lifetime::Session => control::Lifetime::Session,
    }
}

impl control::Host for Ctx {
    async fn info(&mut self) -> wasmtime::Result<control::IsolateInfo> {
        Ok(control::IsolateInfo {
            job: self.ident.job.clone(),
            task: self.ident.task.clone(),
            lifetime: lifetime(self.ident.lifetime),
            tenant: self.ident.tenant.clone(),
            session: self.ident.session.clone(),
            resumed: self.resumed,
        })
    }

    async fn unsupported(&mut self, what: String) -> wasmtime::Result<()> {
        let detail: String = what.chars().take(1024).collect();
        let stop = self.inv.stop(Stop::new("unsupported", detail));
        Err(wasmtime::Error::new(stop))
    }

    async fn park(&mut self) -> wasmtime::Result<()> {
        if self.ident.lifetime == Lifetime::Session {
            self.park = true;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// legion:isolate/state
// ---------------------------------------------------------------------------

impl state::Host for Ctx {
    async fn get(
        &mut self,
        key: String,
    ) -> wasmtime::Result<Result<Option<Vec<u8>>, state::StateError>> {
        Ok(match &self.state {
            None => Err(state::StateError::NotAvailable),
            Some(s) => Ok(s.map.get(&key).cloned()),
        })
    }

    async fn set(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> wasmtime::Result<Result<(), state::StateError>> {
        let quota = self.state_quota;
        Ok(match &mut self.state {
            None => Err(state::StateError::NotAvailable),
            Some(s) => {
                let old = s.map.get(&key).map(|v| (key.len() + v.len()) as u64);
                let new = (key.len() + value.len()) as u64;
                let total = s.bytes - old.unwrap_or(0) + new;
                if total > quota {
                    Err(state::StateError::Quota(quota))
                } else {
                    s.bytes = total;
                    s.map.insert(key, value);
                    Ok(())
                }
            }
        })
    }

    async fn delete(&mut self, key: String) -> wasmtime::Result<Result<(), state::StateError>> {
        Ok(match &mut self.state {
            None => Err(state::StateError::NotAvailable),
            Some(s) => {
                if let Some(v) = s.map.remove(&key) {
                    s.bytes -= (key.len() + v.len()) as u64;
                }
                Ok(())
            }
        })
    }

    async fn keys(&mut self) -> wasmtime::Result<Result<Vec<String>, state::StateError>> {
        Ok(match &self.state {
            None => Err(state::StateError::NotAvailable),
            Some(s) => Ok(s.map.keys().cloned().collect()),
        })
    }
}

// ---------------------------------------------------------------------------
// legion:isolate/vm
// ---------------------------------------------------------------------------

fn vm_error(reply: &VmReply) -> Option<vm::VmError> {
    let e = reply.error.as_ref()?;
    let msg = e.message.clone();
    Some(
        match VmErrorKind::try_from(e.kind).unwrap_or(VmErrorKind::VmFailed) {
            VmErrorKind::VmNotConfigured => vm::VmError::NotConfigured,
            VmErrorKind::VmResourceExhausted => vm::VmError::ResourceExhausted(msg),
            VmErrorKind::VmChildGone => vm::VmError::ChildGone(msg),
            VmErrorKind::VmNeedsRestart => vm::VmError::NeedsRestart(msg),
            VmErrorKind::VmFailed | VmErrorKind::VmErrorUnspecified => vm::VmError::Failed(msg),
        },
    )
}

impl Ctx {
    async fn vm_call(&mut self, op: vm_request::Op) -> Result<VmReply, vm::VmError> {
        if !self.has_vm {
            return Err(vm::VmError::NotConfigured);
        }
        let req = VmRequest {
            origin: Some(self.origin()),
            op: Some(op),
        };
        let reply = self
            .worker
            .vm(req)
            .await
            .map_err(|s| vm::VmError::Failed(format!("the worker refused: {}", s.message())))?;
        match vm_error(&reply) {
            Some(e) => Err(e),
            None => Ok(reply),
        }
    }

    fn machine_id(&self, m: &Resource<Machine>) -> wasmtime::Result<String> {
        Ok(self.table.get(m)?.id.clone())
    }

    fn new_machine(&mut self, id: String) -> wasmtime::Result<Resource<Machine>> {
        Ok(self.table.push(Machine { id })?)
    }
}

impl vm::Host for Ctx {
    async fn acquire(&mut self) -> wasmtime::Result<Result<Resource<Machine>, vm::VmError>> {
        match self.vm_call(vm_request::Op::Acquire(VmAcquire {})).await {
            Ok(r) => Ok(Ok(self.new_machine(r.id)?)),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn attach(
        &mut self,
        id: String,
    ) -> wasmtime::Result<Result<Resource<Machine>, vm::VmError>> {
        match self.vm_call(vm_request::Op::Attach(VmAttach { id })).await {
            Ok(r) => Ok(Ok(self.new_machine(r.id)?)),
            Err(e) => Ok(Err(e)),
        }
    }
}

impl vm::HostMachine for Ctx {
    async fn id(&mut self, m: Resource<Machine>) -> wasmtime::Result<String> {
        self.machine_id(&m)
    }

    async fn exec(
        &mut self,
        m: Resource<Machine>,
        argv: Vec<String>,
        stdin: Vec<u8>,
        timeout_ms: u32,
    ) -> wasmtime::Result<Result<vm::ExecResult, vm::VmError>> {
        let id = self.machine_id(&m)?;
        let op = vm_request::Op::Exec(VmExec {
            id,
            argv,
            stdin,
            timeout_millis: timeout_ms,
        });
        Ok(self.vm_call(op).await.map(|r| {
            let e = r.exec.unwrap_or_default();
            vm::ExecResult {
                exit: e.exit,
                stdout: e.stdout,
                stderr: e.stderr,
                timed_out: e.timed_out,
            }
        }))
    }

    async fn put(
        &mut self,
        m: Resource<Machine>,
        path: String,
        bytes: Vec<u8>,
    ) -> wasmtime::Result<Result<(), vm::VmError>> {
        let id = self.machine_id(&m)?;
        let op = vm_request::Op::Put(VmPut {
            id,
            path,
            content: bytes,
        });
        Ok(self.vm_call(op).await.map(|_| ()))
    }

    async fn get(
        &mut self,
        m: Resource<Machine>,
        path: String,
    ) -> wasmtime::Result<Result<Vec<u8>, vm::VmError>> {
        let id = self.machine_id(&m)?;
        let op = vm_request::Op::Get(VmGet { id, path });
        Ok(self.vm_call(op).await.map(|r| r.content))
    }

    async fn fetch(
        &mut self,
        m: Resource<Machine>,
        port: u16,
        request: vm::HttpRequest,
    ) -> wasmtime::Result<Result<vm::HttpResponse, vm::VmError>> {
        let id = self.machine_id(&m)?;
        let op = vm_request::Op::Fetch(VmFetch {
            id,
            port: port as u32,
            request: Some(HttpCall {
                method: request.method,
                path: request.path,
                authority: String::new(),
                scheme: "http".into(),
                headers: request
                    .headers
                    .into_iter()
                    .map(|(name, value)| Header { name, value })
                    .collect(),
                body: request.body,
            }),
        });
        Ok(self.vm_call(op).await.map(|r| {
            let resp = r.response.unwrap_or_default();
            vm::HttpResponse {
                status: resp.status.min(u16::MAX as u32) as u16,
                headers: resp
                    .headers
                    .into_iter()
                    .map(|h| (h.name, h.value))
                    .collect(),
                body: resp.body,
            }
        }))
    }

    async fn resize(
        &mut self,
        m: Resource<Machine>,
        to: vm::Size,
    ) -> wasmtime::Result<Result<(), vm::VmError>> {
        let id = self.machine_id(&m)?;
        let op = vm_request::Op::Resize(VmResize {
            id,
            memory_mib: to.memory_mib,
            memory_max_mib: to.memory_max_mib,
            cpu_mhz: to.cpu_mhz,
        });
        Ok(self.vm_call(op).await.map(|_| ()))
    }

    async fn release(&mut self, m: Resource<Machine>) -> wasmtime::Result<()> {
        let id = self.machine_id(&m)?;
        // `release` has no error channel in the WIT; a failure leaves the
        // child to end with its owner.
        let _ = self
            .vm_call(vm_request::Op::Release(VmRelease { id }))
            .await;
        Ok(())
    }

    async fn drop(&mut self, m: Resource<Machine>) -> wasmtime::Result<()> {
        // Dropping the handle does not end the child (WIT: a child ends
        // with its owner, or when released).
        self.table.delete(m)?;
        Ok(())
    }
}

#[cfg(test)]
mod guest_test {
    use super::*;

    #[test]
    fn durations_read_like_limits_test() {
        assert_eq!(fmt_duration(Duration::from_millis(100)), "100ms");
        assert_eq!(fmt_duration(Duration::from_micros(100_400)), "100.4ms");
        assert_eq!(fmt_duration(Duration::from_secs(2)), "2s");
        assert_eq!(fmt_duration(Duration::from_millis(12_500)), "12.5s");
        assert_eq!(fmt_duration(Duration::from_micros(250)), "250µs");
    }

    #[test]
    fn output_budget_is_shared_and_truncates_test() {
        let o = Output::new(10);
        o.push(false, b"hello");
        o.push(true, b"world!!");
        let p = o.take();
        assert_eq!(p.stdout, b"hello");
        assert_eq!(p.stderr, b"world");
        assert!(p.truncated);
        assert_eq!(p.left, 0);
    }

    #[test]
    fn state_quota_counts_keys_and_values_test() {
        let s = StateMap::from_map(BTreeMap::from([("ab".to_string(), vec![1, 2, 3])]));
        assert_eq!(s.bytes, 5);
    }
}
