//! Exec session benchmark: proxied vs direct exec (sandbox-exec plan
//! Phase 0, §7).
//!
//! Starts a manager and a separate worker process on isolated ports (never
//! 4747/4848), runs one `exec` task, then for each concurrency level opens
//! that many sessions at once, each printing 64 KiB and exiting:
//!
//! - **proxied**: `Manager/ExecAllocation`, the default path.
//! - **direct**: `Manager/MintExecTicket`, then `Worker/ExecTask` with the
//!   ticket over the mutual-TLS worker plane.
//!
//! Per cell it reports open latency (request sent to first output byte, p50
//! and p99; for direct, from the mint request), and the manager process's
//! CPU time and bytes moved (`/proc/<pid>/io` `rchar + wchar`, which count
//! socket and disk I/O alike) per session. It also measures mint throughput.
//!
//! ```bash
//! LEGION_EXEC_BENCH_CONCURRENCY=1,10,100 \
//!   cargo test --release --test exec_session_benchmark -- --ignored --nocapture
//! ```
//!
//! Linux only (`/proc`). Results land in
//! `target/benchmark-results/exec-sessions.json`; recorded numbers live in
//! `benchmarks/scheduler/README.md`.

#![cfg(target_os = "linux")]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use legion::structs::{exec_client_frame, exec_server_frame, ExecClientFrame, ExecStart};
use tokio_stream::StreamExt;

fn free_port_pair() -> u16 {
    for _ in 0..50 {
        let first = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("bind")
            .port();
        if first < u16::MAX - 2 && TcpListener::bind(("127.0.0.1", first + 1)).is_ok() {
            return first;
        }
    }
    panic!("no adjacent free ports");
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind")
        .port()
}

struct Processes(Vec<Child>);

impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            // SAFETY: plain kill(2) on our own child.
            unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
        }
        for child in &mut self.0 {
            let started = Instant::now();
            while child.try_wait().ok().flatten().is_none() {
                if started.elapsed() > Duration::from_secs(20) {
                    let _ = child.kill();
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// CPU seconds and bytes moved (read plus written, sockets and disk) by a
/// process so far.
fn process_usage(pid: u32) -> (f64, u64) {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    // Fields after the parenthesized command; utime and stime are 14 and 15.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let ticks = |index: usize| -> f64 {
        fields
            .get(index - 3)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    // SAFETY: sysconf has no preconditions.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let cpu = (ticks(14) + ticks(15)) / hz.max(1.0);
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).unwrap_or_default();
    let counter = |name: &str| -> u64 {
        io.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    };
    (cpu, counter("rchar: ") + counter("wchar: "))
}

fn pctl(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[derive(serde::Serialize)]
struct Cell {
    mode: &'static str,
    concurrency: usize,
    ok: usize,
    open_p50_ms: f64,
    open_p99_ms: f64,
    manager_cpu_ms_per_session: f64,
    manager_bytes_per_session: f64,
    bytes_per_session: usize,
}

const PAYLOAD: usize = 64 * 1024;

fn command() -> Vec<String> {
    vec![
        "head".into(),
        "-c".into(),
        PAYLOAD.to_string(),
        "/dev/zero".into(),
    ]
}

/// Drives one session to its exit, returning (open latency, bytes read).
async fn drive(
    opened: Instant,
    stream: Result<
        tonic::Response<tonic::Streaming<legion::structs::ExecServerFrame>>,
        tonic::Status,
    >,
) -> Result<(Duration, usize), String> {
    let mut stream = stream
        .map_err(|status| status.message().to_string())?
        .into_inner();
    let mut first = None;
    let mut bytes = 0;
    while let Some(frame) = stream.next().await {
        match frame.map_err(|status| status.message().to_string())?.frame {
            Some(exec_server_frame::Frame::Stdout(out)) => {
                first.get_or_insert_with(|| opened.elapsed());
                bytes += out.data.len();
            }
            Some(exec_server_frame::Frame::Exit(_)) => break,
            _ => {}
        }
    }
    Ok((first.unwrap_or_else(|| opened.elapsed()), bytes))
}

fn start_frames(start: ExecStart) -> impl tokio_stream::Stream<Item = ExecClientFrame> {
    tokio_stream::iter(vec![
        ExecClientFrame {
            frame: Some(exec_client_frame::Frame::Start(start)),
        },
        ExecClientFrame {
            frame: Some(exec_client_frame::Frame::Stdin(
                legion::structs::ExecInput {
                    data: Vec::new(),
                    eof: true,
                },
            )),
        },
    ])
    .chain(tokio_stream::pending())
}

#[test]
#[ignore]
fn exec_session_benchmark() {
    let levels: Vec<usize> = std::env::var("LEGION_EXEC_BENCH_CONCURRENCY")
        .unwrap_or_else(|_| "1,10,100".into())
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .collect();
    let dir = tempfile::tempdir().expect("tempdir");
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    let port = free_port_pair();
    let worker_port = free_port();

    let workers = dir.path().join("workers");
    std::fs::create_dir_all(&workers).expect("workers");
    let default =
        std::fs::read_to_string(repo.join("configs/workers/default.ncl")).expect("worker config");
    let end = default.rfind('}').expect("record");
    std::fs::write(
        workers.join("default.ncl"),
        format!("{}    exec = {{ direct = true }},\n}}\n", &default[..end]),
    )
    .expect("write worker config");
    let config = serde_json::json!({
        "server": { "host": "127.0.0.1", "port": port },
        "storePath": dir.path().join("store").to_str().expect("utf-8"),
        "workerConfigPath": workers.to_str().expect("utf-8"),
        "telemetry": { "enabled": false },
        "workloadIdentity": { "enabled": true },
        "taskResourceDefaults": { "cpu": 0, "memory": 0, "allocations": 100 },
    });
    let config_path = dir.path().join("manager.json");
    std::fs::write(&config_path, config.to_string()).expect("config");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let token_file = home.join(".legion/token");
    let addr = format!("http://127.0.0.1:{port}");
    let legion = env!("CARGO_BIN_EXE_legion");

    let log = |name: &str| std::fs::File::create(dir.path().join(name)).expect("log");
    let manager = Command::new(legion)
        .args(["start", "manager", "-c"])
        .arg(&config_path)
        .current_dir(dir.path())
        .env("LEGION_DATA_DIR", dir.path().join("manager-data"))
        .env("LEGION_TOKEN_FILE", &token_file)
        .env("HOME", &home)
        .env("RUST_LOG", "warn")
        .env_remove("LEGION_TOKEN")
        .stdout(log("manager.log"))
        .stderr(log("manager.err"))
        .spawn()
        .expect("spawn manager");
    let manager_pid = manager.id();
    let mut processes = Processes(vec![manager]);
    let started = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() || !token_file.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "manager never listened"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    let worker = Command::new(legion)
        .args(["join", "--auto"])
        .current_dir(dir.path())
        .env("LEGION_ADDR", &addr)
        .env("LEGION_TOKEN_FILE", &token_file)
        .env("HOME", &home)
        .env("LEGION_DATA_DIR", dir.path().join("worker-data"))
        .env("LEGION_WORKER_PORT", worker_port.to_string())
        .env("LEGION_WORKER_HOST", "127.0.0.1")
        .env("RUST_LOG", "warn")
        .env_remove("LEGION_TOKEN")
        .stdout(log("worker.log"))
        .stderr(log("worker.err"))
        .spawn()
        .expect("spawn worker");
    processes.0.push(worker);

    let cli = |args: &[&str]| {
        Command::new(legion)
            .args(args)
            .env("LEGION_ADDR", &addr)
            .env("LEGION_TOKEN_FILE", &token_file)
            .env("HOME", &home)
            .env_remove("LEGION_TOKEN")
            .output()
            .expect("cli")
    };
    let job = dir.path().join("bench.json");
    std::fs::write(
        &job,
        serde_json::json!({
            "name": "bench",
            "task": {
                "name": "main",
                "runtime": "exec",
                "lifecycle": "service",
                "config": { "command": ["sleep", "3600"] },
                "resources": { "cpus": 0.1, "memory": 32 },
            },
        })
        .to_string(),
    )
    .expect("job");
    let started = Instant::now();
    let allocation = loop {
        assert!(started.elapsed() < Duration::from_secs(90), "no allocation");
        let _ = cli(&["job", "run", job.to_str().expect("utf-8")]);
        let ledger: serde_json::Value =
            serde_json::from_slice(&cli(&["ledger", "-j"]).stdout).unwrap_or_default();
        let found = ledger["data"].as_array().and_then(|entries| {
            entries
                .iter()
                .flat_map(|entry| entry["allocations"].as_array().cloned().unwrap_or_default())
                .find(|alloc| alloc["jobId"] == "bench" && alloc["status"] == "placed")
                .and_then(|alloc| alloc["id"].as_i64())
        });
        if let Some(id) = found {
            break id;
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    std::env::set_var("LEGION_ADDR", &addr);
    std::env::set_var("LEGION_TOKEN_FILE", &token_file);
    std::env::remove_var("LEGION_TOKEN");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut cells = Vec::new();
    runtime.block_on(async {
        let client = legion::cmd::helpers::connect_manager_client()
            .await
            .expect("manager client");
        // Wait until both paths answer.
        let started = Instant::now();
        loop {
            let mut probe = client.clone();
            let proxied = drive(
                Instant::now(),
                probe
                    .exec_allocation(start_frames(ExecStart {
                        allocation_id: allocation,
                        command: command(),
                        ..Default::default()
                    }))
                    .await,
            )
            .await;
            let minted = probe
                .mint_exec_ticket(legion::structs::MintExecTicketRequest {
                    allocation_id: allocation,
                    command: command(),
                    ..Default::default()
                })
                .await;
            if proxied.is_ok() && minted.is_ok() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "exec never worked: {proxied:?} {:?}",
                minted.err()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        for &concurrency in &levels {
            for mode in ["proxied", "direct"] {
                let (cpu_before, bytes_before) = process_usage(manager_pid);
                let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
                let mut tasks = Vec::new();
                for _ in 0..concurrency {
                    let mut client = client.clone();
                    let barrier = barrier.clone();
                    tasks.push(tokio::spawn(async move {
                        barrier.wait().await;
                        let opened = Instant::now();
                        if mode == "proxied" {
                            let stream = client
                                .exec_allocation(start_frames(ExecStart {
                                    allocation_id: allocation,
                                    command: command(),
                                    ..Default::default()
                                }))
                                .await;
                            drive(opened, stream).await
                        } else {
                            let reply = client
                                .mint_exec_ticket(legion::structs::MintExecTicketRequest {
                                    allocation_id: allocation,
                                    command: command(),
                                    ..Default::default()
                                })
                                .await
                                .map_err(|status| status.message().to_string())?
                                .into_inner();
                            let mut worker = legion::cmd::exec_ticket::worker_client(&reply)
                                .await
                                .map_err(|err| format!("{err:#}"))?;
                            let stream = worker
                                .exec_task(start_frames(ExecStart {
                                    allocation_id: reply.allocation_id,
                                    exec_ticket: reply.ticket,
                                    ..Default::default()
                                }))
                                .await;
                            drive(opened, stream).await
                        }
                    }));
                }
                let mut opens = Vec::new();
                let mut bytes = 0;
                let mut errors = Vec::new();
                for task in tasks {
                    match task.await.expect("session task") {
                        Ok((open, read)) => {
                            opens.push(open.as_secs_f64() * 1000.0);
                            bytes = bytes.max(read);
                        }
                        Err(err) => errors.push(err),
                    }
                }
                // Let the manager finish writing anything the sessions caused.
                tokio::time::sleep(Duration::from_millis(500)).await;
                let (cpu_after, bytes_after) = process_usage(manager_pid);
                opens.sort_by(|a, b| a.total_cmp(b));
                let ok = opens.len();
                if !errors.is_empty() {
                    eprintln!(
                        "{mode} x{concurrency}: {} failed, first: {}",
                        errors.len(),
                        errors[0]
                    );
                }
                cells.push(Cell {
                    mode,
                    concurrency,
                    ok,
                    open_p50_ms: pctl(&opens, 0.50),
                    open_p99_ms: pctl(&opens, 0.99),
                    manager_cpu_ms_per_session: (cpu_after - cpu_before) * 1000.0
                        / ok.max(1) as f64,
                    manager_bytes_per_session: (bytes_after - bytes_before) as f64
                        / ok.max(1) as f64,
                    bytes_per_session: bytes,
                });
            }
        }

        // Mint throughput: one manager, 32 concurrent callers.
        let mints = 2000;
        let started = Instant::now();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        let mut tasks = Vec::new();
        for _ in 0..mints {
            let mut client = client.clone();
            let permit = semaphore.clone().acquire_owned().await.expect("permit");
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                client
                    .mint_exec_ticket(legion::structs::MintExecTicketRequest {
                        allocation_id: allocation,
                        command: command(),
                        ..Default::default()
                    })
                    .await
                    .is_ok()
            }));
        }
        let mut minted = 0;
        for task in tasks {
            if task.await.unwrap_or(false) {
                minted += 1;
            }
        }
        let rate = minted as f64 / started.elapsed().as_secs_f64();
        println!(
            "\nmint throughput: {minted} tickets in {:?} = {rate:.0}/s",
            started.elapsed()
        );
        cells.push(Cell {
            mode: "mint-throughput",
            concurrency: 32,
            ok: minted,
            open_p50_ms: rate,
            open_p99_ms: 0.0,
            manager_cpu_ms_per_session: 0.0,
            manager_bytes_per_session: 0.0,
            bytes_per_session: 0,
        });
    });

    println!(
        "\n| mode | sessions | ok | open p50 ms | open p99 ms | manager CPU ms/session | manager bytes moved/session |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- |");
    for cell in cells.iter().filter(|cell| cell.mode != "mint-throughput") {
        println!(
            "| {} | {} | {} | {:.1} | {:.1} | {:.2} | {:.0} |",
            cell.mode,
            cell.concurrency,
            cell.ok,
            cell.open_p50_ms,
            cell.open_p99_ms,
            cell.manager_cpu_ms_per_session,
            cell.manager_bytes_per_session
        );
    }
    let out = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/benchmark-results");
    let _ = std::fs::create_dir_all(&out);
    let _ = std::fs::write(
        out.join("exec-sessions.json"),
        serde_json::to_string_pretty(&cells).unwrap_or_default(),
    );
    drop(processes);
}
