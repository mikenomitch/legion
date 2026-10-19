//! Datapath benchmarks: what the node's connect forwarder costs compared to
//! dialing a backend directly (network-handlers plan §7.12; public write-up in
//! `docs/testing-and-benchmarks.md`).
//!
//! Everything runs over loopback against the real `UpstreamForwarder`, so the
//! numbers isolate the proxy hop: no NIC, no network round trip, no TLS.
//! Compare a group's `direct` row with its forwarder rows, not absolute
//! numbers across machines.
//!
//! Groups:
//! - `l4_rr`: one kept-open connection, 100-byte request, 100-byte reply.
//! - `l4_conn_rr`: a new connection per request; the backend closes after
//!   replying, so client ports never pile up in TIME_WAIT.
//! - `l4_throughput`: one 64 MiB one-way transfer per iteration, then
//!   `l4_cpu`: CPU-seconds per GiB moved, printed (not a Criterion group),
//!   because CPU time per byte survives a busy host and wall clock doesn't.
//! - `http_rr`: HTTP/1.1 GETs on one kept-open client connection, through
//!   HTTP mode (`connect.upstreams[].http`), and through HTTP mode with a
//!   no-op WASM plugin (`tests/netplugins/conformance.wasm`, mode `noop`) at
//!   the `request` stage, and at `request`, `response`, and `done` (mesh
//!   interfaces plan §6.B; the B1 acceptance number).
//! - `slow_endpoint`: three backends, one of them 2 ms slower. Mean
//!   per-request latency under each balancing strategy this build has.
//! - `balancer_pick`: the cost of one pick across 16 endpoints.
//! - `kernel_conn_rr` (Linux, root, cgroup v2; mesh interfaces plan §6.A,
//!   phase A0): a new connection per request from a process inside an
//!   allocation's cgroup leaf, with no classifier attached, with the kernel
//!   passing the flow after its first rule or after 32 rules, through the
//!   forwarder (the netd hop a pass flow skips), redirected through the
//!   capture listener, and refused (`EPERM`). Printed, not a Criterion
//!   group: the client is a child process in the leaf, so the datapath's
//!   own dials are not classified. `kernel_connect_cost` is the pass rows
//!   minus the unclassified row.

use std::convert::Infallible;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

use legion::structs::UpstreamHttpConfig;
use legion::worker::balance::{balancer, balancer_names, Endpoint};
use legion::worker::forwarder::{UpstreamForwarder, UpstreamSpec};
use legion::worker::netd::attach::Attachment;

const MESSAGE: usize = 100;
const BULK_BYTES: u64 = 64 * 1024 * 1024;
const SLOW_DELAY: Duration = Duration::from_millis(2);

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Echo every read back. A first byte of `0xFF` turns the connection into a
/// sink, for the throughput group.
async fn echo_backend() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 256 * 1024];
                let mut sink = false;
                loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    sink |= buf[0] == 0xFF;
                    if !sink && stream.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                // Tell a sink-mode client every byte arrived.
                let _ = stream.write_all(b"k").await;
            });
        }
    });
    addr
}

/// Reply once (after `delay`), then close: one request per connection.
async fn reply_and_close_backend(delay: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            tokio::spawn(async move {
                let mut buf = [0u8; MESSAGE];
                if stream.read_exact(&mut buf).await.is_err() {
                    return;
                }
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let _ = stream.write_all(&buf).await;
            });
        }
    });
    addr
}

/// A keep-alive HTTP/1.1 server answering every request with a short body
/// after `delay`.
async fn http_backend(delay: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let _ = stream.set_nodelay(true);
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |_req: Request<hyper::body::Incoming>| async move {
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

fn endpoint(addr: &str, allocation_id: i64) -> Endpoint {
    Endpoint {
        addr: addr.to_string(),
        allocation_id,
        node_id: "bench-node".into(),
        tags: Vec::new(),
        mesh_tls: false,
    }
}

async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Start one forwarder listener in front of `backends` and return its port.
/// The forwarder is leaked: it lives for the whole bench process.
async fn forwarder(backends: &[String], balance: &str, http: bool) -> u16 {
    let forwarder: &'static UpstreamForwarder = Box::leak(Box::new(UpstreamForwarder::new()));
    let port = free_port().await;
    let spec = UpstreamSpec {
        allocation_id: 1,
        service: "bench".into(),
        local_port: Some(port),
        http_protocol: http,
        balance: balance.into(),
        to: None,
        http: http.then(UpstreamHttpConfig::default),
        handlers: Vec::new(),
        credentials: Vec::new(),
        ca: None,
        vip: None,
        canonical_port: None,
        attachment: Attachment::Host,
    };
    forwarder.sync(std::slice::from_ref(&spec)).await;
    let endpoints = backends
        .iter()
        .enumerate()
        .map(|(i, addr)| endpoint(addr, i as i64 + 1))
        .collect();
    forwarder.set_endpoints(1, "bench", endpoints);
    port
}

/// An HTTP-mode forwarder whose upstream runs the no-op conformance plugin
/// at `stages`.
async fn plugin_forwarder(backend: &str, stages: &[&str]) -> (u16, &'static UpstreamForwarder) {
    let forwarder: &'static UpstreamForwarder = Box::leak(Box::new(UpstreamForwarder::new()));
    let mut object: legion::structs::netplugin::NetPluginObject =
        serde_json::from_value(serde_json::json!({
            "name": "noop",
            "module": { "source": "file:///unused", "checksum": format!("sha256:{}", "0".repeat(64)) },
            "stages": stages,
            "usableBy": "jobs",
        }))
        .unwrap();
    object.normalize_and_validate().unwrap();
    let module = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/netplugins/conformance.wasm"
    ))
    .unwrap();
    forwarder
        .context()
        .plugins()
        .install_compiled(object, 1, &module)
        .unwrap();
    let hop: legion::structs::hops::HopConfig = serde_json::from_value(serde_json::json!({
        "plugin": { "name": "noop", "config": { "mode": "noop" } }
    }))
    .unwrap();
    let port = free_port().await;
    let spec = UpstreamSpec {
        allocation_id: 1,
        service: "bench".into(),
        local_port: Some(port),
        http_protocol: true,
        balance: "round_robin".into(),
        to: None,
        http: Some(UpstreamHttpConfig::default()),
        handlers: vec![hop],
        credentials: Vec::new(),
        ca: None,
        vip: None,
        canonical_port: None,
        attachment: legion::worker::netd::attach::Attachment::Host,
    };
    forwarder.sync(std::slice::from_ref(&spec)).await;
    forwarder.set_endpoints(1, "bench", vec![endpoint(backend, 1)]);
    (port, forwarder)
}

async fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}

async fn round_trip(stream: &mut TcpStream) {
    let mut buf = [0u8; MESSAGE];
    stream.write_all(&[1u8; MESSAGE]).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
}

/// A bare tokio proxy (accept, dial, `copy_bidirectional`) in front of
/// `port`: the floor for any userspace hop, to tell the forwarder's own
/// costs from the harness's.
async fn plain_proxy(port: u16) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut upstream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    local
}

/// One [`BULK_BYTES`] one-way transfer on a new connection, acknowledged
/// by the backend once every byte has arrived.
async fn bulk_transfer(port: u16) {
    let mut stream = connect(port).await;
    let chunk = vec![0xFFu8; 256 * 1024];
    let mut sent = 0u64;
    while sent < BULK_BYTES {
        stream.write_all(&chunk).await.unwrap();
        sent += chunk.len() as u64;
    }
    stream.shutdown().await.unwrap();
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await.unwrap();
}

/// User plus system CPU time this process has used, in seconds.
fn cpu_seconds() -> f64 {
    // SAFETY: getrusage fills the zeroed struct it is handed.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

async fn conn_round_trip(port: u16) {
    let mut stream = connect(port).await;
    round_trip(&mut stream).await;
    // Wait for the backend's close so the next iteration starts clean.
    let mut rest = Vec::new();
    let _ = stream.read_to_end(&mut rest).await;
}

type HttpSender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;

async fn http_client(port: u16) -> HttpSender {
    let stream = connect(port).await;
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    sender
}

async fn http_get(sender: &mut HttpSender) {
    sender.ready().await.unwrap();
    let request = Request::get("/")
        .header("host", "bench")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    response.into_body().collect().await.unwrap();
}

fn l4(c: &mut Criterion) {
    let rt = runtime();
    let (direct, via) = rt.block_on(async {
        let backend = echo_backend().await;
        let port = forwarder(std::slice::from_ref(&backend), "round_robin", false).await;
        let direct: u16 = backend.rsplit(':').next().unwrap().parse().unwrap();
        (direct, port)
    });
    let plain = rt.block_on(plain_proxy(direct));
    let targets = [
        ("direct", direct),
        ("plain_proxy", plain),
        ("forwarder", via),
    ];

    let mut group = c.benchmark_group("l4_rr");
    group.measurement_time(Duration::from_secs(4));
    for (name, port) in targets {
        group.bench_function(name, |b| {
            let mut stream = rt.block_on(connect(port));
            b.iter(|| rt.block_on(round_trip(&mut stream)));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("l4_throughput");
    group.throughput(Throughput::Bytes(BULK_BYTES));
    group.sample_size(10);
    for (name, port) in targets {
        group.bench_function(name, |b| b.iter(|| rt.block_on(bulk_transfer(port))));
    }
    group.finish();

    // Wall-clock throughput through extra threads mostly measures the
    // scheduler on a busy host. CPU time per byte is what the copy path
    // actually costs, and holds up under contention. The process runs the
    // client and the backend too, so the forwarder's own cost is the gap
    // between the two rows.
    let rounds = 16;
    let gib = (rounds as f64 * BULK_BYTES as f64) / (1u64 << 30) as f64;
    let mut direct_cost = 0.0;
    for (name, port) in targets {
        let started = cpu_seconds();
        for _ in 0..rounds {
            rt.block_on(bulk_transfer(port));
        }
        let per_gib = (cpu_seconds() - started) / gib;
        if name == "direct" {
            direct_cost = per_gib;
            println!("l4_cpu/{name:<20} {per_gib:.3} CPU-s/GiB");
        } else {
            println!(
                "l4_cpu/{name:<20} {per_gib:.3} CPU-s/GiB ({:.3} for the hop)",
                per_gib - direct_cost
            );
        }
    }

    let (direct, via) = rt.block_on(async {
        let backend = reply_and_close_backend(Duration::ZERO).await;
        let port = forwarder(std::slice::from_ref(&backend), "round_robin", false).await;
        let direct: u16 = backend.rsplit(':').next().unwrap().parse().unwrap();
        (direct, port)
    });
    let mut group = c.benchmark_group("l4_conn_rr");
    group.measurement_time(Duration::from_secs(3));
    for (name, port) in [("direct", direct), ("forwarder", via)] {
        group.bench_function(name, |b| b.iter(|| rt.block_on(conn_round_trip(port))));
    }
    group.finish();
}

fn http(c: &mut Criterion) {
    let rt = runtime();
    let (direct, via, inert, request_plugin, all_plugin) = rt.block_on(async {
        let backend = http_backend(Duration::ZERO).await;
        let port = forwarder(std::slice::from_ref(&backend), "round_robin", true).await;
        // A plugin with no HTTP stage: the exchange runs, no callback does.
        let inert = plugin_forwarder(&backend, &["connect"]).await;
        let request_plugin = plugin_forwarder(&backend, &["request"]).await;
        let all_plugin = plugin_forwarder(&backend, &["request", "response", "done"]).await;
        let direct: u16 = backend.rsplit(':').next().unwrap().parse().unwrap();
        (direct, port, inert, request_plugin, all_plugin)
    });
    let mut group = c.benchmark_group("http_rr");
    group.measurement_time(Duration::from_secs(4));
    for (name, port) in [
        ("direct", direct),
        ("http_mode", via),
        ("http_mode_plugin_without_http_stages", inert.0),
        ("http_mode_noop_plugin_request", request_plugin.0),
        ("http_mode_noop_plugin_all_stages", all_plugin.0),
    ] {
        group.bench_function(name, |b| {
            let mut sender = rt.block_on(http_client(port));
            b.iter(|| rt.block_on(http_get(&mut sender)));
        });
    }
    group.finish();
    // What the plugin's callbacks cost on the real path, from its stats.
    for (name, (_, forwarder)) in [("request", request_plugin), ("all_stages", all_plugin)] {
        for row in forwarder.context().plugins().report() {
            let entry = forwarder.context().plugins().entry(&row.plugin).unwrap();
            let micros = entry
                .stats
                .micros
                .load(std::sync::atomic::Ordering::Relaxed);
            println!(
                "http_rr/plugin_{name:<12} {} stage callbacks, mean {:.2} µs, p99 {} µs",
                row.calls,
                micros as f64 / row.calls.max(1) as f64,
                row.p99_micros
            );
        }
    }
}

/// Mean latency of `requests` sequential requests through a forwarder whose
/// third backend is [`SLOW_DELAY`] slower than the other two. A strategy that
/// learns to avoid the slow backend approaches the fast backends' latency;
/// one that spreads evenly pays a third of the delay on average.
fn slow_endpoint(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("slow_endpoint");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    for strategy in balancer_names() {
        let (http_port, l4_port) = rt.block_on(async {
            let http_backends = vec![
                http_backend(Duration::ZERO).await,
                http_backend(Duration::ZERO).await,
                http_backend(SLOW_DELAY).await,
            ];
            let l4_backends = vec![
                reply_and_close_backend(Duration::ZERO).await,
                reply_and_close_backend(Duration::ZERO).await,
                reply_and_close_backend(SLOW_DELAY).await,
            ];
            (
                forwarder(&http_backends, strategy, true).await,
                forwarder(&l4_backends, strategy, false).await,
            )
        });
        group.bench_function(BenchmarkId::new("http", strategy), |b| {
            let mut sender = rt.block_on(http_client(http_port));
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let started = Instant::now();
                    for _ in 0..iters {
                        http_get(&mut sender).await;
                    }
                    started.elapsed()
                })
            });
        });
        group.bench_function(BenchmarkId::new("l4_conn", strategy), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let started = Instant::now();
                    for _ in 0..iters {
                        conn_round_trip(l4_port).await;
                    }
                    started.elapsed()
                })
            });
        });
    }
    group.finish();
}

fn balancer_pick(c: &mut Criterion) {
    let endpoints: Vec<Endpoint> = (0..16)
        .map(|i| endpoint(&format!("10.0.0.{i}:8080"), i))
        .collect();
    let inflight = std::collections::HashMap::new();
    let mut group = c.benchmark_group("balancer_pick");
    for strategy in balancer_names() {
        let picker = balancer(strategy).unwrap();
        group.bench_function(strategy, |b| {
            b.iter(|| std::hint::black_box(picker.pick(&endpoints, &inflight)))
        });
    }
    group.finish();
}

/// The plugin host in-process, no network: one no-op callback on a live
/// context, a context's whole life (create, `on_done`, `on_delete`), and a
/// request-stage exchange end to end. Tells the host's own cost from the
/// HTTP path's in `http_rr`.
fn plugin_exchange(c: &mut Criterion) {
    use legion::worker::netd::handlers::{HopFacts, Pipeline, RequestHead};
    let rt = runtime();
    let context = std::sync::Arc::new(legion::worker::forwarder::ForwarderContext::new(
        std::sync::Arc::new(legion::worker::netd::NetPolicyCache::new()),
        std::sync::Arc::new(legion::worker::netd::Denials::new()),
    ));
    let mut object: legion::structs::netplugin::NetPluginObject =
        serde_json::from_value(serde_json::json!({
            "name": "noop",
            "module": { "source": "file:///unused", "checksum": format!("sha256:{}", "0".repeat(64)) },
            "stages": ["request"],
        }))
        .unwrap();
    object.normalize_and_validate().unwrap();
    let module = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/netplugins/conformance.wasm"
    ))
    .unwrap();
    context
        .plugins()
        .install_compiled(object, 1, &module)
        .unwrap();
    let reference = legion::structs::netplugin::PluginConfig {
        name: "noop".into(),
        config: Some(serde_json::json!({ "mode": "noop" })),
    };
    let (_, pool) = context.plugins().pool(&reference, "policy").unwrap();
    let slot = pool.pick().unwrap();
    let mut group = c.benchmark_group("plugin_exchange");
    group.measurement_time(Duration::from_secs(3));
    {
        let mut vm = slot.vm.lock().unwrap();
        let mut ctx = Some(vm.new_context().unwrap());
        group.bench_function("vm_request_headers_callback", |b| {
            b.iter(|| {
                let args = vm.headers_args(ctx.as_ref().unwrap().id, 0, true);
                let (outcome, back) =
                    vm.call("proxy_on_request_headers", &args, ctx.take().unwrap());
                assert!(outcome.result.is_ok());
                ctx = Some(back);
            })
        });
        group.bench_function("vm_context_lifecycle", |b| {
            b.iter(|| {
                let fresh = vm.new_context().unwrap();
                let id = fresh.id as i32;
                let (_, fresh) = vm.call("proxy_on_done", &[id], fresh);
                let _ = vm.call("proxy_on_delete", &[id], fresh);
            })
        });
    }
    let hop: legion::structs::hops::HopConfig = serde_json::from_value(serde_json::json!({
        "plugin": { "name": "noop", "config": { "mode": "noop" } }
    }))
    .unwrap();
    let chain = legion::structs::netpolicy::EffectiveChain {
        protocol: "http".into(),
        policy: "allow".into(),
        access_log: false,
        callers: None,
        rules: Vec::new(),
        handlers: vec![legion::structs::netpolicy::EffectiveHandler {
            handler: hop,
            source: "policy/bench#0".into(),
        }],
    };
    let pipeline = std::sync::Arc::new(Pipeline::compile(Some(&chain), None, &[]));
    let facts = HopFacts {
        allocation_id: 1,
        direction: "egress",
        protocol: "http",
        destination: "bench:80",
        host: Some("bench"),
        port: 80,
        ..Default::default()
    };
    let headers = vec![
        ("host".to_string(), "bench".to_string()),
        ("accept".to_string(), "*/*".to_string()),
    ];
    group.bench_function("exchange_request_stage", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut exchange = pipeline.exchange(&context, &facts);
                let mut head = RequestHead {
                    method: "GET".into(),
                    path: "/".into(),
                    authority: "bench".into(),
                    scheme: "http".into(),
                    headers: headers.clone(),
                    end_of_stream: true,
                    changed: false,
                };
                let _ = exchange.request(&facts, &mut head).await;
                drop(exchange);
            })
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    l4,
    http,
    slow_endpoint,
    balancer_pick,
    plugin_exchange
);

fn main() {
    if let Ok(spec) = std::env::var(kernel::CHILD_ENV) {
        kernel::child(&spec);
        return;
    }
    benches();
    kernel::run();
    Criterion::default().configure_from_args().final_summary();
}

mod kernel {
    /// Set on the client child: `<addr>:<iterations>:<mode>`.
    pub const CHILD_ENV: &str = "LEGION_NET_BENCH_CHILD";

    /// The client: `rr` connects, sends 100 bytes (an HTTP head naming
    /// `bench.test`, so a redirected flow carries a name), reads 100 back,
    /// and waits for the close; `deny` only attempts the connect. Prints the
    /// mean nanoseconds per iteration.
    pub fn child(spec: &str) {
        use std::io::{Read, Write};
        let mut parts = spec.rsplitn(3, ':');
        let mode = parts.next().unwrap().to_string();
        let iterations: u32 = parts.next().unwrap().parse().unwrap();
        let addr: std::net::SocketAddr = parts.next().unwrap().parse().unwrap();
        let message = request();
        let started = std::time::Instant::now();
        for _ in 0..iterations {
            match std::net::TcpStream::connect(addr) {
                Ok(mut stream) if mode == "rr" => {
                    let _ = stream.set_nodelay(true);
                    stream.write_all(&message).unwrap();
                    let mut buf = [0u8; super::MESSAGE];
                    stream.read_exact(&mut buf).unwrap();
                    let mut rest = Vec::new();
                    let _ = stream.read_to_end(&mut rest);
                }
                Ok(_) => {}
                Err(err) if mode == "deny" => {
                    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied)
                }
                Err(err) => panic!("connect {addr}: {err}"),
            }
        }
        println!("{}", started.elapsed().as_nanos() / u128::from(iterations));
    }

    fn request() -> Vec<u8> {
        let mut head = b"GET / HTTP/1.1\r\nHost: bench.test\r\nX-Pad: ".to_vec();
        while head.len() < super::MESSAGE - 4 {
            head.push(b'a');
        }
        head.extend_from_slice(b"\r\n\r\n");
        head
    }

    #[cfg(not(target_os = "linux"))]
    pub fn run() {}

    #[cfg(target_os = "linux")]
    pub fn run() {
        use legion::structs::netpolicy::{merge_policies, ChainConfig, InlinePolicy};
        use legion::worker::forwarder::ForwarderContext;
        use legion::worker::netd::kernel::runtime::{cgroup2_mount, KernelNet, KernelSettings};
        use legion::worker::netd::kernel_guard::{GuardTable, GuardedAllocation};
        use legion::worker::netd::{Denials, NetPolicyCache};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::Arc;

        let filter = std::env::args().skip(1).find(|arg| !arg.starts_with('-'));
        if filter
            .as_deref()
            .is_some_and(|filter| !"kernel_conn_rr".contains(filter))
        {
            return;
        }
        // SAFETY: geteuid has no preconditions.
        let root_user = unsafe { libc::geteuid() } == 0;
        let (Some(root), true) = (cgroup2_mount(), root_user) else {
            println!("kernel_conn_rr: skipped (needs root and a cgroup v2 mount)");
            return;
        };
        let Some(ip) = legion::structs::host_guard::interface_addresses()
            .into_iter()
            .find_map(|ip| match ip {
                IpAddr::V4(v4) if !v4.is_loopback() => Some(v4),
                _ => None,
            })
        else {
            println!("kernel_conn_rr: skipped (no non-loopback IPv4 address)");
            return;
        };
        let rt = super::runtime();
        let backend_port = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
                .await
                .unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                while let Ok((mut stream, _)) = listener.accept().await {
                    let _ = stream.set_nodelay(true);
                    tokio::spawn(async move {
                        let mut buf = [0u8; super::MESSAGE];
                        if stream.read_exact(&mut buf).await.is_ok() {
                            let _ = stream.write_all(&buf).await;
                        }
                    });
                }
            });
            port
        });
        let backend = SocketAddr::from((ip, backend_port));
        let parent = format!("legion-net-bench-{}", std::process::id());
        let leaf = format!("{parent}/share/legion-1");
        std::fs::create_dir_all(root.join(&leaf)).unwrap();
        let iterations = 2000u32;
        let measure = |addr: SocketAddr, mode: &str| -> f64 {
            let exe = std::env::current_exe().unwrap();
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "echo $$ > {} && exec \"$0\"",
                    root.join(&leaf).join("cgroup.procs").display()
                ))
                .arg(exe)
                .env(CHILD_ENV, format!("{addr}:{iterations}:{mode}"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<f64>()
                .unwrap()
                / 1000.0
        };
        let mut rows: Vec<(&str, f64)> = Vec::new();
        rows.push(("unclassified", measure(backend, "rr")));

        let cache = Arc::new(NetPolicyCache::new());
        let guard = legion::structs::host_guard::HostGuard::new(
            &legion::structs::host_guard::HostGuardConfig {
                allow: vec![format!("{ip}:{backend_port}")],
                deny: vec![],
            },
        )
        .unwrap();
        struct Names(Ipv4Addr);
        #[tonic::async_trait]
        impl legion::worker::netd::guard::Resolver for Names {
            async fn resolve(&self, _host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
                Ok(vec![SocketAddr::from((self.0, port))])
            }
        }
        let net = KernelNet::open(KernelSettings {
            cgroup_root: root.clone(),
            cgroup_parent: parent.clone(),
            pin_dir: None,
        })
        .unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache.clone(), Arc::new(Denials::new()))
                .with_host_guard(Arc::new(guard))
                .with_resolver(Arc::new(Names(ip)))
                .with_kernel_classifier(
                    GuardTable {
                        cgroup_parent: parent.clone(),
                        ..Default::default()
                    },
                    net,
                ),
        );
        let capture = rt
            .block_on(legion::worker::netd::capture::HostCapture::start(
                context.clone(),
            ))
            .unwrap();
        context.set_host_capture(Some(capture.addr));
        let apply = |chain: serde_json::Value| {
            let mut config: ChainConfig = serde_json::from_value(chain).unwrap();
            config.normalize_and_validate("egress").unwrap();
            let policy = merge_policies(
                &[],
                InlinePolicy {
                    egress: Some(&config),
                    ..Default::default()
                },
            );
            cache.apply_assignments(&[legion::structs::AllocNetPolicy {
                allocation_id: 1,
                policy_hash: policy.hash.clone(),
            }]);
            let hash = policy.hash.clone();
            cache.insert_body(&hash, policy).unwrap();
            context.reconcile_kernel_guard(vec![GuardedAllocation {
                allocation_id: 1,
                cgroup: leaf.clone(),
                vip_block: None,
                loopback_ports: vec![],
                resolver: None,
                gated: false,
                host_sockets: true,
            }]);
        };
        apply(serde_json::json!({ "policy": "deny", "allow": [format!("{ip}/32")] }));
        rows.push(("pass_first_rule", measure(backend, "rr")));
        let mut rules: Vec<serde_json::Value> = (1..=32)
            .map(|port| serde_json::json!({ "when": { "port": port }, "action": "deny" }))
            .collect();
        rules
            .push(serde_json::json!({ "when": { "cidr": format!("{ip}/32") }, "action": "allow" }));
        apply(serde_json::json!({ "policy": "deny", "rules": rules }));
        rows.push(("pass_after_32_rules", measure(backend, "rr")));
        let via = rt.block_on(super::forwarder(
            &[backend.to_string()],
            "round_robin",
            false,
        ));
        rows.push((
            "forwarder",
            measure(SocketAddr::from((Ipv4Addr::LOCALHOST, via)), "rr"),
        ));
        apply(serde_json::json!({
            "policy": "deny",
            "rules": [{ "when": { "host": "bench.test" }, "action": "allow" }],
        }));
        rows.push(("redirect", measure(backend, "rr")));
        apply(serde_json::json!({ "policy": "deny" }));
        rows.push(("deny", measure(backend, "deny")));
        context.shutdown_kernel();
        drop(capture);
        let _ = std::fs::remove_dir(root.join(&leaf));
        let _ = std::fs::remove_dir(root.join(&parent).join("share"));
        let _ = std::fs::remove_dir(root.join(&parent));

        let base = rows[0].1;
        for (name, micros) in &rows {
            println!("kernel_conn_rr/{name:<22} {micros:>8.1} µs");
        }
        for (name, micros) in &rows[1..3] {
            println!(
                "kernel_connect_cost/{name:<17} {:>8.2} µs per connection",
                micros - base
            );
        }
    }
}
