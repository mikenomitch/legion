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
//! - `l4_throughput`: one 64 MiB one-way transfer per iteration.
//! - `http_rr`: HTTP/1.1 GETs on one kept-open client connection, through
//!   HTTP mode (`connect.upstreams[].http`).
//! - `slow_endpoint`: three backends, one of them 2 ms slower. Mean
//!   per-request latency under each balancing strategy this build has.
//! - `balancer_pick`: the cost of one pick across 16 endpoints.

use std::convert::Infallible;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

use legion::structs::UpstreamHttpConfig;
use legion::worker::balance::{balancer, balancer_names, Endpoint};
use legion::worker::forwarder::{UpstreamForwarder, UpstreamSpec};

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
        local_port: port,
        balance: balance.into(),
        to: None,
        http: http.then(UpstreamHttpConfig::default),
        handlers: Vec::new(),
        credentials: Vec::new(),
        vip: None,
        canonical_port: None,
        netns: None,
        vsock: None,
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
    let targets = [("direct", direct), ("forwarder", via)];

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
        group.bench_function(name, |b| {
            b.iter(|| {
                rt.block_on(async {
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
                })
            });
        });
    }
    group.finish();

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
    let (direct, via) = rt.block_on(async {
        let backend = http_backend(Duration::ZERO).await;
        let port = forwarder(std::slice::from_ref(&backend), "round_robin", true).await;
        let direct: u16 = backend.rsplit(':').next().unwrap().parse().unwrap();
        (direct, port)
    });
    let mut group = c.benchmark_group("http_rr");
    group.measurement_time(Duration::from_secs(4));
    for (name, port) in [("direct", direct), ("http_mode", via)] {
        group.bench_function(name, |b| {
            let mut sender = rt.block_on(http_client(port));
            b.iter(|| rt.block_on(http_get(&mut sender)));
        });
    }
    group.finish();
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

criterion_group!(benches, l4, http, slow_endpoint, balancer_pick);
criterion_main!(benches);
