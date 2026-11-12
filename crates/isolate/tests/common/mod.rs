//! Shared helpers for the host's integration tests: compiled fixtures, an
//! in-process host, and a fake worker that records what the host asks.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use legion_isolate::compile;
use legion_isolate::engine::EngineParams;
use legion_isolate::host::{Host, HostSetup};
use legion_isolate::worker_api::WorkerApi;
use legion_proto::isolate_host::*;
use tonic::Status;

pub fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.wasm"))
}

/// A directory of compiled fixtures, made once per test binary.
pub fn artifacts() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = tempfile::Builder::new()
            .prefix("isolate-artifacts")
            .tempdir()
            .unwrap()
            .keep();
        dir
    })
}

/// Compile a fixture (once) and return the artifact path.
pub fn artifact(name: &str) -> PathBuf {
    static LOCK: Mutex<()> = Mutex::new(());
    let _g = LOCK.lock().unwrap();
    let out = artifacts().join(format!("{name}.cwasm"));
    if !out.exists() {
        compile::compile_file(&EngineParams::default(), &fixture(name), &out)
            .unwrap_or_else(|e| panic!("compiling {name}: {e:?}"));
    }
    out
}

pub fn config() -> HostConfig {
    HostConfig {
        protocol_version: PROTOCOL_VERSION,
        cordon: "test".into(),
        shards: 1,
        slots: 64,
        slot_bytes: 128 << 20,
        epoch_millis: 1,
        slice_millis: 5,
        layout: SlotLayout::GuardPages as i32,
        confine: Confine::Off as i32,
        read_dirs: vec![artifacts().display().to_string()],
        max_body_bytes: 1 << 20,
        async_stack_bytes: 512 << 10,
        require_spectre_mitigations: true,
        ..Default::default()
    }
}

/// A fake worker. Egress answers 200 with the request echoed in headers
/// unless `egress_reply` is set; it "injects" a credential only on its own
/// side (recorded as `injected`), never in what it returns.
#[derive(Default)]
pub struct FakeWorker {
    pub egress_seen: Mutex<Vec<EgressRequest>>,
    pub egress_reply: Mutex<Option<EgressReply>>,
    pub vm_seen: Mutex<Vec<VmRequest>>,
    pub checkpoints: Mutex<HashMap<String, (CheckpointHeader, Vec<u8>)>>,
    pub saves: Mutex<u64>,
    /// Checkpoint saves started (counted before `save_delay`), and how long
    /// each waits before it is kept (when set).
    pub saves_started: std::sync::atomic::AtomicUsize,
    pub save_delay: Mutex<Option<Duration>>,
    pub logs: Mutex<Vec<LogBatch>>,
    /// Egress waits this long before answering (when set).
    pub egress_delay: Mutex<Option<Duration>>,
    /// Egress calls running now, the most ever at once, and how many were
    /// dropped (cancelled by the host) before they answered.
    pub egress_in_flight: std::sync::atomic::AtomicUsize,
    pub egress_max_in_flight: std::sync::atomic::AtomicUsize,
    pub egress_cancelled: std::sync::atomic::AtomicUsize,
}

/// Counts an egress call while it runs; a drop before `done` is a cancel.
struct InFlight<'a>(&'a FakeWorker, bool);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        self.0.egress_in_flight.fetch_sub(1, Ordering::SeqCst);
        if !self.1 {
            self.0.egress_cancelled.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl WorkerApi for FakeWorker {
    async fn egress(&self, req: EgressRequest) -> Result<EgressReply, Status> {
        self.egress_seen.lock().unwrap().push(req.clone());
        {
            use std::sync::atomic::Ordering;
            let now = self.egress_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.egress_max_in_flight.fetch_max(now, Ordering::SeqCst);
        }
        let mut guard = InFlight(self, false);
        let delay = *self.egress_delay.lock().unwrap();
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        guard.1 = true;
        drop(guard);
        if let Some(r) = self.egress_reply.lock().unwrap().clone() {
            return Ok(r);
        }
        // The worker's upstream call would carry `authorization: Bearer
        // secret`; the reply never does.
        Ok(EgressReply {
            status: 200,
            headers: vec![Header {
                name: "x-upstream".into(),
                value: b"yes".to_vec(),
            }],
            body: format!("hello from {}", req.url).into_bytes(),
            error: None,
        })
    }

    async fn vm(&self, req: VmRequest) -> Result<VmReply, Status> {
        self.vm_seen.lock().unwrap().push(req.clone());
        Ok(match req.op {
            Some(vm_request::Op::Acquire(_)) => VmReply {
                id: "child-1".into(),
                ..Default::default()
            },
            Some(vm_request::Op::Exec(e)) => VmReply {
                exec: Some(VmExecResult {
                    exit: 0,
                    stdout: format!("ran {}", e.argv.join(" ")).into_bytes(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            _ => VmReply::default(),
        })
    }

    async fn save_checkpoint(
        &self,
        header: CheckpointHeader,
        body: Vec<u8>,
    ) -> Result<SaveCheckpointReply, Status> {
        self.saves_started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let delay = *self.save_delay.lock().unwrap();
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let id = header.origin.as_ref().unwrap().isolate_id.clone();
        self.checkpoints.lock().unwrap().insert(id, (header, body));
        let mut n = self.saves.lock().unwrap();
        *n += 1;
        Ok(SaveCheckpointReply { generation: *n })
    }

    async fn load_checkpoint(
        &self,
        origin: Origin,
    ) -> Result<Option<(CheckpointHeader, Vec<u8>)>, Status> {
        Ok(self
            .checkpoints
            .lock()
            .unwrap()
            .get(&origin.isolate_id)
            .cloned())
    }

    async fn log(&self, batch: LogBatch) -> Result<(), Status> {
        self.logs.lock().unwrap().push(batch);
        Ok(())
    }
}

pub struct TestHost {
    pub host: Host,
    pub worker: Arc<FakeWorker>,
}

impl Drop for TestHost {
    fn drop(&mut self) {
        self.host.shutdown();
    }
}

pub fn start(cfg: HostConfig) -> TestHost {
    let worker = Arc::new(FakeWorker::default());
    let host = Host::new(HostSetup {
        config: cfg,
        worker: worker.clone(),
        rt: tokio::runtime::Handle::current(),
        confinement: Confinement::default(),
        statm: std::fs::File::open("/proc/self/statm").ok(),
    })
    .unwrap();
    TestHost { host, worker }
}

pub fn spec(job: &str, task: &str, module: &str) -> TaskSpec {
    TaskSpec {
        job_id: job.into(),
        task_name: task.into(),
        module_id: module.into(),
        lifetime: Lifetime::Request as i32,
        limits: Some(Limits {
            cpu_micros: 2_000_000,
            wall_micros: 5_000_000,
            output_bytes: 64 << 10,
        }),
        memory_max_bytes: 64 << 20,
        memory_reservation_bytes: 1 << 20,
        cpu_weight: 1,
        queue_timeout_millis: 2000,
        egress: true,
        state_quota_bytes: 1 << 10,
        ..Default::default()
    }
}

impl TestHost {
    pub fn load(&self, name: &str) {
        self.host
            .load_module(LoadModuleRequest {
                module_id: name.into(),
                artifact_path: artifact(name).display().to_string(),
            })
            .unwrap();
    }

    /// Load `module` and configure `spec`.
    pub fn task(&self, spec: TaskSpec) {
        self.load(&spec.module_id.clone());
        self.host.configure_task(spec).unwrap();
    }

    pub async fn run(&self, job: &str, task: &str, args: &[&str]) -> InvokeReply {
        self.run_with(job, task, "", "", run(args)).await.unwrap()
    }

    pub async fn run_with(
        &self,
        job: &str,
        task: &str,
        tenant: &str,
        session: &str,
        call: RunCall,
    ) -> Result<InvokeReply, Status> {
        self.host
            .invoke(InvokeRequest {
                job_id: job.into(),
                task_name: task.into(),
                invocation_id: next_id(),
                tenant: tenant.into(),
                session_id: session.into(),
                call: Some(invoke_request::Call::Run(call)),
            })
            .await
    }
}

pub fn run(args: &[&str]) -> RunCall {
    RunCall {
        args: args.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

pub fn next_id() -> String {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    format!(
        "inv-{}",
        N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

pub fn stdout(r: &InvokeReply) -> String {
    String::from_utf8_lossy(&r.stdout).into_owned()
}

pub fn exit(r: &InvokeReply) -> Option<i32> {
    match &r.outcome {
        Some(invoke_reply::Outcome::Run(o)) => o.exit,
        _ => None,
    }
}

pub fn outcome(r: &InvokeReply) -> &RunOutcome {
    match &r.outcome {
        Some(invoke_reply::Outcome::Run(o)) => o,
        other => panic!("not a run outcome: {other:?}"),
    }
}

pub fn stopped(r: &InvokeReply) -> (&str, &str, &str) {
    let s = r.stopped.as_ref().unwrap_or_else(|| {
        panic!(
            "not stopped: exit {:?} stdout {:?} stderr {:?}",
            exit(r),
            stdout(r),
            String::from_utf8_lossy(&r.stderr)
        )
    });
    (s.reason.as_str(), s.limit.as_str(), s.detail.as_str())
}

/// Poll `probe` until `done` holds of what it returns, for up to 10 s (a
/// loaded CI machine runs many test binaries at once), and return the last
/// value either way: the caller asserts on it.
pub async fn wait_for<T>(mut probe: impl FnMut() -> T, done: impl Fn(&T) -> bool) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let v = probe();
        if done(&v) || std::time::Instant::now() >= deadline {
            return v;
        }
        sleep_ms(5).await;
    }
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await
}
