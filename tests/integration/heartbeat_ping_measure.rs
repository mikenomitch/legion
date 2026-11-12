//! What a liveness ping costs next to a full heartbeat
//! (`internal_docs/plans/liveness-ping-split-plan.md` §9; the numbers are in
//! `docs/heartbeat-cadence-and-scale.md`, "Pings and heartbeats").
//!
//! Two measurements, each for `Manager.Ping` and for a steady full
//! `Manager.Heartbeat` from an idle node (no allocations, nothing to report:
//! the cheapest full heartbeat there is, so the comparison flatters the
//! heartbeat, never the ping):
//!
//! - **Handler**: the manager's own work per call, the service called
//!   in-process with no transport.
//! - **Loopback**: client and server CPU per call (process `getrusage`) over a
//!   real tonic connection on 127.0.0.1, `LEGION_PING_MEASURE_CONCURRENCY`
//!   callers at once. This includes HTTP/2 and protobuf on both ends, which a
//!   ping pays like any other RPC.
//!
//! Ignored by default:
//!
//! ```text
//! cargo test --release --test integration heartbeat_ping_measure:: -- --ignored --nocapture
//! ```
//!
//! Results print as one JSON line per measurement, tagged
//! `heartbeat_ping_measure`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, R};
use legion::storage::store::Store;
use legion::structs::manager_client::ManagerClient;
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::{HeartbeatRequest, NodeMsg, PingRequest};
use tonic::Request;

const NODES: usize = 64;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(default)
}

fn node_id(index: usize) -> String {
    format!("ping-measure-{index}")
}

fn fleet_store() -> R<Store> {
    let store = crate::support::temp_store("ping-measure")?;
    for index in 0..NODES {
        store.insert_node(NodeMsg {
            id: node_id(index),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
            health_checks: "{}".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
        })?;
    }
    let active: Vec<String> = (0..NODES).map(node_id).collect();
    store.liveness().become_responsible(&active);
    store
        .liveness()
        .set_served_intervals(Some(Duration::from_secs(2)), Some(Duration::from_secs(21)));
    Ok(store)
}

/// User plus system CPU this process has used.
fn process_cpu() -> Duration {
    // SAFETY: `getrusage` writes into the zeroed struct we hand it.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let user = Duration::new(
        usage.ru_utime.tv_sec as u64,
        usage.ru_utime.tv_usec as u32 * 1_000,
    );
    let system = Duration::new(
        usage.ru_stime.tv_sec as u64,
        usage.ru_stime.tv_usec as u32 * 1_000,
    );
    user + system
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Ping,
    Heartbeat,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::Ping => "ping",
            Kind::Heartbeat => "heartbeat",
        }
    }
}

#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn ping_versus_heartbeat_cost() -> R<()> {
    let calls = env_usize("LEGION_PING_MEASURE_CALLS", 20_000);
    let concurrency = env_usize("LEGION_PING_MEASURE_CONCURRENCY", 16);
    let store = fleet_store()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    // Handler only: the service in-process, one caller, no transport.
    let svc = ManagerDaemonService::new(store.clone());
    for kind in [Kind::Ping, Kind::Heartbeat] {
        // Warm up (first heartbeats per node record health and feeds).
        for index in 0..NODES {
            rt.block_on(call_in_process(&svc, kind, index))?;
        }
        let started = Instant::now();
        let cpu_before = process_cpu();
        for call in 0..calls {
            rt.block_on(call_in_process(&svc, kind, call % NODES))?;
        }
        let wall = started.elapsed();
        let cpu = process_cpu() - cpu_before;
        println!(
            "heartbeat_ping_measure {}",
            json!({
                "measure": "handler",
                "kind": kind.label(),
                "calls": calls,
                "wall_us_per_call": wall.as_secs_f64() * 1e6 / calls as f64,
                "cpu_us_per_call": cpu.as_secs_f64() * 1e6 / calls as f64,
            })
        );
    }

    // Over a real loopback connection, client and server in this process.
    let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let addr: SocketAddr = listener.local_addr()?;
    let service = ManagerServer::new(ManagerDaemonService::new(store.clone()));
    rt.spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    let client = rt.block_on(ManagerClient::connect(format!("http://{addr}")))?;
    for kind in [Kind::Ping, Kind::Heartbeat] {
        rt.block_on(run_callers(&client, kind, NODES, concurrency))?;
        let started = Instant::now();
        let cpu_before = process_cpu();
        rt.block_on(run_callers(&client, kind, calls, concurrency))?;
        let wall = started.elapsed();
        let cpu = process_cpu() - cpu_before;
        println!(
            "heartbeat_ping_measure {}",
            json!({
                "measure": "loopback",
                "kind": kind.label(),
                "calls": calls,
                "concurrency": concurrency,
                "calls_per_sec": calls as f64 / wall.as_secs_f64(),
                "cpu_us_per_call_client_and_server": cpu.as_secs_f64() * 1e6 / calls as f64,
            })
        );
    }
    Ok(())
}

async fn call_in_process(svc: &ManagerDaemonService, kind: Kind, index: usize) -> R<()> {
    match kind {
        Kind::Ping => {
            svc.ping(Request::new(PingRequest {
                node_id: node_id(index),
            }))
            .await?;
        }
        Kind::Heartbeat => {
            svc.heartbeat(Request::new(HeartbeatRequest {
                id: node_id(index),
                ..Default::default()
            }))
            .await?;
        }
    }
    Ok(())
}

async fn run_callers(
    client: &ManagerClient<tonic::transport::Channel>,
    kind: Kind,
    calls: usize,
    concurrency: usize,
) -> R<()> {
    let concurrency = concurrency.max(1);
    let mut callers = Vec::with_capacity(concurrency);
    for caller in 0..concurrency {
        let client = client.clone();
        let share = calls / concurrency + usize::from(caller < calls % concurrency);
        callers.push(tokio::spawn(async move {
            let mut client = client;
            for call in 0..share {
                let index = (caller + call * concurrency) % NODES;
                match kind {
                    Kind::Ping => {
                        client
                            .ping(PingRequest {
                                node_id: node_id(index),
                            })
                            .await?;
                    }
                    Kind::Heartbeat => {
                        client
                            .heartbeat(HeartbeatRequest {
                                id: node_id(index),
                                ..Default::default()
                            })
                            .await?;
                    }
                }
            }
            Ok::<(), tonic::Status>(())
        }));
    }
    for caller in callers {
        caller.await??;
    }
    Ok(())
}
