//! The supervisor against a fake host: an in-process `IsolateHost` served
//! over the same socket pairs a real host gets, which calls back into the
//! worker's `IsolateWorker` end for checkpoints. Everything above the
//! launcher runs as it does on a node.

use super::binary::{ImageCatalog, ImageEntry, IsolateBinary};
use super::host::{
    client_over, serve_over, socket_pair, tokio_stream, HostLauncher, HostSpawn, Launched,
};
use super::modules::{ModuleArtifact, ModuleSource, ModuleWant};
use super::sessions::CheckpointStore;
use super::{IsolateEnvelope, IsolateSupervisor, SupervisorParts};
use crate::prelude::*;
use crate::structs::isolate::WorkerIsolateConfigFull;
use crate::structs::isolate_host::isolate_host_server::{IsolateHost, IsolateHostServer};
use crate::structs::isolate_host::isolate_worker_client::IsolateWorkerClient;
use crate::structs::isolate_host::*;
use crate::structs::{RunFile, RunSessionRequest, TaskConfig};
use crate::worker::task_manager::session_runs::{RunCall, RunRegistry};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tonic::{Request, Response, Status};

const HEX: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

#[derive(Default)]
struct HostState {
    tasks: HashMap<(String, String), TaskSpec>,
    modules: Vec<String>,
    /// session id -> (counter, resident)
    sessions: HashMap<String, (u64, bool)>,
    /// session id -> (job, task), from `OpenSession`.
    session_tasks: HashMap<String, (String, String)>,
    /// Every `Reclaim` asked of this host.
    reclaims: Vec<ReclaimRequest>,
    invocations: u64,
    stopped: BTreeMap<String, u64>,
    last_args: Vec<String>,
}

#[derive(Clone)]
struct FakeHost {
    pid: u32,
    state: Arc<Mutex<HostState>>,
    worker: IsolateWorkerClient<tonic::transport::Channel>,
    /// An invocation of a file whose content is `hang` blocks until the
    /// host is killed.
    hang: Arc<tokio::sync::Notify>,
}

fn origin(job: &str, task: &str, session: &str) -> Origin {
    Origin {
        job_id: job.into(),
        task_name: task.into(),
        isolate_id: session.into(),
        tenant: String::new(),
        session: true,
    }
}

#[tonic::async_trait]
impl IsolateHost for FakeHost {
    async fn hello(&self, _: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        Ok(Response::new(HelloReply {
            protocol_version: crate::structs::isolate_host::PROTOCOL_VERSION,
            engine_version: "wasmtime-fake".into(),
            compat_key: "fake".into(),
            pid: self.pid,
            shards: 1,
            slots: 4,
            layout: 0,
            confinement: Some(Confinement {
                user_namespace: true,
                network_namespace: true,
                no_new_privs: true,
                landlock: true,
                seccomp: true,
                notes: vec![],
            }),
        }))
    }

    async fn load_module(
        &self,
        request: Request<LoadModuleRequest>,
    ) -> Result<Response<LoadModuleReply>, Status> {
        self.state
            .lock()
            .unwrap()
            .modules
            .push(request.into_inner().module_id);
        Ok(Response::new(LoadModuleReply::default()))
    }

    async fn unload_module(
        &self,
        _: Request<UnloadModuleRequest>,
    ) -> Result<Response<UnloadModuleReply>, Status> {
        Ok(Response::new(UnloadModuleReply { unloaded: true }))
    }

    async fn configure_task(
        &self,
        request: Request<TaskSpec>,
    ) -> Result<Response<ConfigureTaskReply>, Status> {
        let spec = request.into_inner();
        self.state
            .lock()
            .unwrap()
            .tasks
            .insert((spec.job_id.clone(), spec.task_name.clone()), spec);
        Ok(Response::new(ConfigureTaskReply {}))
    }

    async fn remove_task(
        &self,
        request: Request<RemoveTaskRequest>,
    ) -> Result<Response<RemoveTaskReply>, Status> {
        let request = request.into_inner();
        self.state
            .lock()
            .unwrap()
            .tasks
            .remove(&(request.job_id, request.task_name));
        Ok(Response::new(RemoveTaskReply { dropped: 0 }))
    }

    async fn invoke(
        &self,
        request: Request<InvokeRequest>,
    ) -> Result<Response<InvokeReply>, Status> {
        let request = request.into_inner();
        if !self
            .state
            .lock()
            .unwrap()
            .tasks
            .contains_key(&(request.job_id.clone(), request.task_name.clone()))
        {
            return Err(Status::not_found("unknown task"));
        }
        let Some(invoke_request::Call::Run(run)) = request.call else {
            return Ok(Response::new(InvokeReply {
                outcome: Some(invoke_reply::Outcome::Http(HttpOutcome {
                    status: 200,
                    body: b"hello".to_vec(),
                    ..Default::default()
                })),
                ..Default::default()
            }));
        };
        self.state.lock().unwrap().last_args = run.args.clone();
        let entry = run
            .files
            .iter()
            .find(|f| f.path == run.entry)
            .map(|f| f.content.clone())
            .unwrap_or_default();
        if entry == b"hang" {
            // Until the process is "killed": the call never gets an answer
            // from the host, only the broken connection.
            self.hang.notified().await;
            return Err(Status::internal("h2 protocol error: connection reset"));
        }
        let mut start = "fresh".to_string();
        let mut stdout = entry.clone();
        if !request.session_id.is_empty() {
            let known = self
                .state
                .lock()
                .unwrap()
                .sessions
                .get(&request.session_id)
                .copied();
            let Some((mut counter, resident)) = known else {
                return Err(Status::not_found("unknown session"));
            };
            if !resident {
                // Wake from the checkpoint the worker keeps.
                if let Ok(reply) = self
                    .worker
                    .clone()
                    .load_checkpoint(LoadCheckpointRequest {
                        origin: Some(origin(
                            &request.job_id,
                            &request.task_name,
                            &request.session_id,
                        )),
                    })
                    .await
                {
                    let mut stream = reply.into_inner();
                    let mut body = Vec::new();
                    while let Some(chunk) = stream.message().await? {
                        if let Some(checkpoint_chunk::Part::Data(data)) = chunk.part {
                            body.extend(data);
                        }
                    }
                    counter = String::from_utf8_lossy(&body).parse().unwrap_or(0);
                    start = "hibernated".into();
                }
            } else {
                start = "warm".into();
            }
            counter += 1;
            self.state
                .lock()
                .unwrap()
                .sessions
                .insert(request.session_id.clone(), (counter, true));
            stdout = counter.to_string().into_bytes();
        }
        let mut state = self.state.lock().unwrap();
        state.invocations += 1;
        if entry == b"spin" {
            *state.stopped.entry("limits".into()).or_default() += 1;
            return Ok(Response::new(InvokeReply {
                outcome: Some(invoke_reply::Outcome::Run(RunOutcome::default())),
                stopped: Some(Stopped {
                    reason: "limits".into(),
                    limit: "cpu".into(),
                    detail: "exceeded its 100ms CPU limit".into(),
                }),
                cpu_micros: 100_400,
                wall_micros: 101_000,
                start,
                ..Default::default()
            }));
        }
        Ok(Response::new(InvokeReply {
            outcome: Some(invoke_reply::Outcome::Run(RunOutcome {
                exit: Some(0),
                result: run.input.unwrap_or_default(),
                files: run
                    .get
                    .iter()
                    .map(|path| File {
                        path: path.clone(),
                        missing: true,
                        ..Default::default()
                    })
                    .collect(),
            })),
            stdout,
            cpu_micros: 1_500,
            wall_micros: 2_000,
            instantiate_micros: 40,
            start,
            ..Default::default()
        }))
    }

    async fn open_session(
        &self,
        request: Request<OpenSessionRequest>,
    ) -> Result<Response<OpenSessionReply>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().unwrap();
        let existed = state.sessions.contains_key(&request.session_id);
        state.session_tasks.insert(
            request.session_id.clone(),
            (request.job_id.clone(), request.task_name.clone()),
        );
        state
            .sessions
            .entry(request.session_id)
            .or_insert((0, false));
        Ok(Response::new(OpenSessionReply { existed }))
    }

    async fn close_session(
        &self,
        request: Request<CloseSessionRequest>,
    ) -> Result<Response<CloseSessionReply>, Status> {
        let closed = self
            .state
            .lock()
            .unwrap()
            .sessions
            .remove(&request.into_inner().session_id)
            .is_some();
        Ok(Response::new(CloseSessionReply { closed }))
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<SessionInfo>, Status> {
        let id = request.into_inner().session_id;
        let state = self.state.lock().unwrap();
        match state.sessions.get(&id) {
            Some((counter, true)) => Ok(Response::new(SessionInfo {
                session_id: id,
                tier: "hot".into(),
                invocations: *counter,
                ..Default::default()
            })),
            _ => Err(Status::not_found("not held")),
        }
    }

    async fn resize_session(
        &self,
        request: Request<ResizeSessionRequest>,
    ) -> Result<Response<ResizeSessionReply>, Status> {
        let request = request.into_inner();
        Ok(Response::new(ResizeSessionReply {
            memory_max_bytes: request.memory_max_bytes,
            memory_reservation_bytes: request.memory_reservation_bytes,
        }))
    }

    async fn reclaim(
        &self,
        request: Request<ReclaimRequest>,
    ) -> Result<Response<ReclaimReply>, Status> {
        let request = request.into_inner();
        self.state.lock().unwrap().reclaims.push(request.clone());
        if request.level != ReclaimLevel::ReclaimHibernate as i32 {
            return Ok(Response::new(ReclaimReply::default()));
        }
        // The filters, as the real host honors them: named sessions, one
        // job's isolates, one task's.
        let resident: Vec<(String, u64, String, String)> = {
            let state = self.state.lock().unwrap();
            state
                .sessions
                .iter()
                .filter(|(_, (_, resident))| *resident)
                .map(|(id, (counter, _))| {
                    let (job, task) = state.session_tasks.get(id).cloned().unwrap_or_else(|| {
                        state.tasks.keys().next().cloned().unwrap_or_default()
                    });
                    (id.clone(), *counter, job, task)
                })
                .filter(|(id, _, job, task)| {
                    (request.session_ids.is_empty() || request.session_ids.contains(id))
                        && (request.job_id.is_empty() || request.job_id == *job)
                        && (request.task_name.is_empty() || request.task_name == *task)
                })
                .collect()
        };
        let mut hibernated = 0;
        for (id, counter, job, task) in resident {
            let body = counter.to_string().into_bytes();
            let chunks = vec![
                CheckpointChunk {
                    part: Some(checkpoint_chunk::Part::Header(CheckpointHeader {
                        origin: Some(origin(&job, &task, &id)),
                        engine_version: "wasmtime-fake".into(),
                        module_id: "m".into(),
                        bytes: body.len() as u64,
                        freed_bytes: 0,
                    })),
                },
                CheckpointChunk {
                    part: Some(checkpoint_chunk::Part::Data(body)),
                },
            ];
            self.worker
                .clone()
                .save_checkpoint(futures::stream::iter(chunks))
                .await?;
            self.state.lock().unwrap().sessions.insert(id, (0, false));
            hibernated += 1;
        }
        Ok(Response::new(ReclaimReply {
            hibernated,
            ..Default::default()
        }))
    }

    async fn stats(&self, _: Request<StatsRequest>) -> Result<Response<StatsReply>, Status> {
        let state = self.state.lock().unwrap();
        Ok(Response::new(StatsReply {
            tasks: state
                .tasks
                .keys()
                .map(|(job, task)| TaskStats {
                    job_id: job.clone(),
                    task_name: task.clone(),
                    invocations: state.invocations,
                    resident: state.sessions.values().filter(|(_, r)| *r).count() as u32,
                    stopped: state.stopped.clone().into_iter().collect(),
                    ..Default::default()
                })
                .collect(),
            rss_bytes: 1 << 20,
            slots: 4,
            slots_used: 1,
            shards: 1,
            started_unix_ms: 1,
        }))
    }

    async fn drain(&self, _: Request<DrainRequest>) -> Result<Response<DrainReply>, Status> {
        Ok(Response::new(DrainReply::default()))
    }
}

type Killer = Arc<dyn Fn() + Send + Sync>;

/// Starts [`FakeHost`]s and remembers them.
#[derive(Default)]
struct FakeLauncher {
    next_pid: AtomicU32,
    hosts: Mutex<Vec<(HostSpawn, FakeHost, Killer)>>,
    /// When set, each host gets a stand-in cgroup directory here.
    cgroup_root: Mutex<Option<std::path::PathBuf>>,
}

impl FakeLauncher {
    fn hosts(&self) -> Vec<(HostSpawn, FakeHost)> {
        self.hosts
            .lock()
            .unwrap()
            .iter()
            .map(|(spawn, host, _)| (spawn.clone(), host.clone()))
            .collect()
    }

    fn kill(&self, index: usize) {
        let kill = self.hosts.lock().unwrap()[index].2.clone();
        kill();
    }
}

#[tonic::async_trait]
impl HostLauncher for FakeLauncher {
    async fn launch(
        &self,
        spawn: &HostSpawn,
        worker: super::service::WorkerService,
    ) -> R<Launched> {
        let pid = 1000 + self.next_pid.fetch_add(1, Ordering::SeqCst);
        let (control_ours, control_theirs) = socket_pair()?;
        let (worker_ours, worker_theirs) = socket_pair()?;
        serve_over(tokio_stream(worker_ours)?, worker);
        // The host's client of the worker's service.
        let worker_stream = tokio_stream(worker_theirs)?;
        let slot = Arc::new(Mutex::new(Some(worker_stream)));
        let channel = tonic::transport::Endpoint::try_from("http://worker.local")?
            .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                let stream = slot.lock().unwrap().take();
                async move {
                    stream
                        .map(hyper_util::rt::TokioIo::new)
                        .ok_or_else(|| std::io::Error::other("used"))
                }
            }))
            .await?;
        let host = FakeHost {
            pid,
            state: Arc::default(),
            worker: IsolateWorkerClient::new(channel),
            hang: Arc::default(),
        };
        let server = IsolateHostServer::new(host.clone());
        let theirs = tokio_stream(control_theirs)?;
        let (exit_tx, exited) = tokio::sync::watch::channel(None);
        let exit_tx = Arc::new(exit_tx);
        let serving = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(theirs)))
                .await;
        });
        let client = client_over(tokio_stream(control_ours)?).await?;
        let abort = serving.abort_handle();
        let exit = exit_tx.clone();
        let hang = host.hang.clone();
        let kill: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = exit.send(Some("was killed".into()));
            abort.abort();
            hang.notify_waiters();
        });
        self.hosts
            .lock()
            .unwrap()
            .push((spawn.clone(), host, kill.clone()));
        let cgroup = self.cgroup_root.lock().unwrap().clone().map(|root| {
            let leaf = root.join(format!("{}-{}", spawn.cordon, spawn.replica));
            std::fs::create_dir_all(&leaf).unwrap();
            if let Some(max) = spawn.memory_max_bytes {
                std::fs::write(leaf.join("memory.max"), max.to_string()).unwrap();
            }
            leaf
        });
        Ok(Launched {
            client,
            pid,
            exited,
            kill: Box::new(move || kill()),
            cgroup,
        })
    }
}

struct FakeModules;

impl ModuleSource for FakeModules {
    fn compiled(&self, want: &ModuleWant) -> Result<ModuleArtifact, String> {
        match want {
            ModuleWant::Component(module) if module.source.contains("broken") => {
                Err("the module failed to compile (invalid): not a component".into())
            }
            _ => Ok(ModuleArtifact {
                module_id: "m".repeat(64),
                path: "/nonexistent/module.cwasm".into(),
            }),
        }
    }

    fn entry_file(
        &self,
        _: &str,
        _: &TaskConfig,
        _: i64,
        entry: &str,
    ) -> Result<(String, Vec<u8>), String> {
        Ok((entry.to_string(), b"print(1)".to_vec()))
    }
}

struct Harness {
    supervisor: Arc<IsolateSupervisor>,
    launcher: Arc<FakeLauncher>,
    gate: Arc<crate::worker::runtime_state::RuntimeGate>,
    _dir: tempfile::TempDir,
}

fn harness_with(config: WorkerIsolateConfigFull, max_map_count: u64) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let launcher = Arc::new(FakeLauncher::default());
    let gate = Arc::new(crate::worker::runtime_state::RuntimeGate::beside(
        &dir.path().join("worker_state.json"),
    ));
    let images = ImageCatalog {
        dir: dir.path().to_path_buf(),
        images: BTreeMap::from([(
            "python".to_string(),
            ImageEntry {
                file: "python.wasm".into(),
                modules: vec!["json".into(), "re".into()],
                ..Default::default()
            },
        )]),
    };
    let supervisor = IsolateSupervisor::new(SupervisorParts {
        node_id: "node-1".into(),
        config,
        binary: IsolateBinary {
            path: "/nonexistent/legion-isolate".into(),
            engine_version: "wasmtime-fake".into(),
            compat_key: "fake".into(),
        },
        images: Some(images),
        launcher: launcher.clone(),
        modules: Arc::new(FakeModules),
        read_dirs: vec![],
        book_path: Some(dir.path().join("isolates/sessions.json")),
        checkpoints: CheckpointStore {
            store: None,
            dir: dir.path().join("checkpoints"),
        },
        run_registry: RunRegistry::new(dir.path().join("no-sessions.db")),
        gate: gate.clone(),
        queue_timeout: Duration::from_secs(1),
        max_map_count,
        paging_unavailable: Some("tests".into()),
    });
    Harness {
        supervisor,
        launcher,
        gate,
        _dir: dir,
    }
}

fn harness() -> Harness {
    harness_with(
        WorkerIsolateConfigFull {
            confine: crate::structs::isolate::ConfineMode::BestEffort,
            ..Default::default()
        },
        1 << 24,
    )
}

fn isolate_task(name: &str, config: Value) -> TaskConfig {
    let mut task = TaskConfig::new(name, "isolate");
    task.config = config;
    task.resources = json!({ "cpu": 10, "memory": 16, "memoryMax": 128 });
    task
}

fn module_config(lifetime: &str) -> Value {
    json!({
        "module": { "source": "file:///guests/cmd.wasm", "sha256": HEX },
        "lifetime": lifetime,
        "limits": { "cpu": "100ms", "wall": "1s" },
    })
}

fn envelope(job: &str, task: TaskConfig, memory_mib: f64) -> IsolateEnvelope {
    IsolateEnvelope {
        job_id: job.into(),
        allocation_id: 7,
        spec: crate::worker::task_manager::SessionSpec {
            job_config_id: "jc".into(),
            task_config_id: 1,
            task,
        },
        room: BTreeMap::from([
            ("memory".to_string(), memory_mib),
            ("cpu".to_string(), 1000.0),
        ]),
    }
}

fn run_call(job: &str, task: &str, files: &[(&str, &str)]) -> RunCall {
    RunCall {
        job_id: job.into(),
        task_name: task.into(),
        request: RunSessionRequest {
            job_id: job.into(),
            task_name: task.into(),
            files: files
                .iter()
                .map(|(path, content)| RunFile {
                    path: path.to_string(),
                    content: content.as_bytes().to_vec(),
                    executable: false,
                })
                .collect(),
            wait_timeout_seconds: Some(10),
            ..Default::default()
        },
        ticket_jti: String::new(),
    }
}

async fn eventually<F: Fn() -> bool>(what: &str, check: F) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_task_is_prepared_and_runs_in_its_cordons_host() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    let reply = h
        .supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "hi")]), None)
        .await
        .unwrap();
    assert_eq!(reply.exit, Some(0));
    assert_eq!(reply.stdout, b"hi");
    assert_eq!(reply.tier, "isolate");
    assert_eq!(reply.tier_reason, "auto: no imports outside the image");
    assert_eq!(reply.isolate_start, "fresh");
    assert_eq!((reply.cpu_ms, reply.cpu_micros), (2, 1_500));
    let hosts = h.launcher.hosts();
    assert_eq!(hosts.len(), 1);
    assert_eq!(hosts[0].0.cordon, "ns", "the namespace cordon by default");
    let spec =
        hosts[0].1.state.lock().unwrap().tasks[&("ns/app".to_string(), "run".to_string())].clone();
    assert_eq!(
        spec.max_resident, 64,
        "1024 MiB envelope over a 16 MiB reservation"
    );
    assert_eq!(spec.max_active, 100, "1000 MHz over a 10 MHz weight");
    assert_eq!(spec.limits.unwrap().cpu_micros, 100_000);
    assert_eq!(spec.memory_max_bytes, 128 << 20);
    assert_eq!(spec.queue_timeout_millis, 1000);
    // A second job of the namespace shares the host; a job cordon does not.
    let mut job_cordon = module_config("request");
    job_cordon["cordon"] = json!("job");
    h.supervisor.sync(vec![
        envelope(
            "ns/app",
            isolate_task("run", module_config("request")),
            1024.0,
        ),
        envelope(
            "ns/other",
            isolate_task("run", module_config("request")),
            1024.0,
        ),
        envelope("ns/alone", isolate_task("run", job_cordon), 1024.0),
    ]);
    h.supervisor
        .run(run_call("ns/other", "run", &[("main.wasm", "x")]), None)
        .await
        .unwrap();
    h.supervisor
        .run(run_call("ns/alone", "run", &[("main.wasm", "x")]), None)
        .await
        .unwrap();
    let cordons: Vec<String> = h
        .launcher
        .hosts()
        .iter()
        .map(|(s, _)| s.cordon.clone())
        .collect();
    assert_eq!(cordons, vec!["ns", "job:ns/alone"]);
    let (_, hosts) = h.supervisor.stats("", "").await;
    assert_eq!(hosts.len(), 2);
    // The last task of a cordon leaving stops its host.
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    let mut hosts = 2;
    for _ in 0..200 {
        hosts = h.supervisor.stats("", "").await.1.len();
        if hosts == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(hosts, 1, "the job cordon's host stopped");
}

#[tokio::test]
async fn isolate_only_fields_are_checked() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.command = vec!["python3".into()];
    let err = h.supervisor.run(call, None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.tier = "gpu".into();
    assert_eq!(
        h.supervisor.run(call, None).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.tenant = "has space".into();
    assert_eq!(
        h.supervisor.run(call, None).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.session_id = "s".into();
    let err = h.supervisor.run(call, None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("lifetime = \"session\""),
        "{}",
        err.message()
    );
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.input = Some("{not json".into());
    assert_eq!(
        h.supervisor.run(call, None).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.tier = "vm".into();
    let err = h.supervisor.run(call, None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("config.vm"), "{}", err.message());
    let err = h
        .supervisor
        .run(run_call("ns/app", "missing", &[("main.wasm", "hi")]), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn a_module_that_does_not_compile_is_the_tasks_problem() {
    let h = harness();
    let mut config = module_config("request");
    config["module"]["source"] = json!("file:///guests/broken.wasm");
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", config),
        1024.0,
    )]);
    let err = h
        .supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "hi")]), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("not a component"),
        "{}",
        err.message()
    );
    let (tasks, _) = h.supervisor.stats("ns/app", "").await;
    assert!(
        tasks[0].problem.contains("failed to compile"),
        "{:?}",
        tasks[0]
    );
}

#[tokio::test]
async fn stopped_runs_set_next_tier_and_teach_the_memo() {
    let h = harness();
    let mut config = module_config("request");
    config["image"] = json!("python");
    config.as_object_mut().unwrap().remove("module");
    config["vm"] = json!({ "task": "machine" });
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", config),
        1024.0,
    )]);
    let reply = h
        .supervisor
        .run(run_call("ns/app", "run", &[("main.py", "spin")]), None)
        .await
        .unwrap();
    assert_eq!(reply.exit, None);
    let stop = reply.stopped.clone().unwrap();
    assert_eq!(
        (stop.reason.as_str(), stop.limit.as_str()),
        ("limits", "cpu")
    );
    assert_eq!(reply.next_tier, "vm");
    // The same code now goes to a VM on its own: a dry run says so.
    let mut dry = run_call("ns/app", "run", &[("main.py", "spin")]);
    dry.request.dry_run = true;
    let reply = h.supervisor.run(dry, None).await.unwrap();
    let dry = reply.dry_run.unwrap();
    assert_eq!(dry.tier, "vm");
    assert!(
        dry.tier_reason.starts_with("auto: this code needed a VM"),
        "{}",
        dry.tier_reason
    );
    // An import the image lacks goes to the VM too, and the dry run lists it.
    let mut dry = run_call(
        "ns/app",
        "run",
        &[
            ("main.py", "import json\nimport torch\n"),
            ("helper.py", ""),
        ],
    );
    dry.request.dry_run = true;
    let dry = h.supervisor.run(dry, None).await.unwrap().dry_run.unwrap();
    assert_eq!(dry.tier, "vm");
    assert_eq!(dry.tier_reason, "auto: import torch is not in image python");
    assert_eq!(dry.imports, vec!["json", "torch"]);
    assert_eq!(dry.missing_imports, vec!["torch"]);
    // Without a VM task it would run in the isolate.
    let mut dry = run_call("ns/app", "run", &[("main.py", "import json")]);
    dry.request.dry_run = true;
    let dry = h.supervisor.run(dry, None).await.unwrap().dry_run.unwrap();
    assert_eq!((dry.tier.as_str(), dry.has_room), ("isolate", true));
    // A VM run on a node without the task manager is refused, not faked.
    let mut vm = run_call("ns/app", "run", &[("main.py", "import torch")]);
    vm.request.tier = "vm".into();
    assert_eq!(
        h.supervisor.run(vm, None).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn a_disabled_runtime_refuses_or_diverts() {
    use crate::structs::runtime_state::{RuntimeDisable, RuntimeDisableSet};
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    h.gate.replace(RuntimeDisableSet::new(vec![RuntimeDisable {
        runtime: "isolate".into(),
        namespaces: vec![],
        reason: "CVE".into(),
        since: "now".into(),
        by: "op".into(),
        drain: false,
    }]));
    let err = h
        .supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "hi")]), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(
        err.metadata()
            .get(crate::structs::worker_protocol::ERROR_CODE_METADATA)
            .and_then(|v| v.to_str().ok()),
        Some(crate::structs::runtime_state::RUNTIME_DISABLED_CODE)
    );
}

#[tokio::test]
async fn idempotent_retries_are_deduplicated() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    let mut call = run_call("ns/app", "run", &[("main.wasm", "hi")]);
    call.request.idempotency_key = Some("k1".into());
    let first = h.supervisor.run(call.clone(), None).await.unwrap();
    let second = h.supervisor.run(call.clone(), None).await.unwrap();
    assert!(!first.deduplicated);
    assert!(second.deduplicated);
    assert_eq!(first.run_id, second.run_id);
    let invocations = h.launcher.hosts()[0].1.state.lock().unwrap().invocations;
    assert_eq!(invocations, 1, "the retry ran nothing");
    // The same key for another tenant is another request.
    call.request.tenant = "user-1".into();
    let third = h.supervisor.run(call, None).await.unwrap();
    assert!(!third.deduplicated);
}

#[tokio::test]
async fn a_killed_host_fails_in_flight_runs_and_is_restarted() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    h.supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "warm up")]), None)
        .await
        .unwrap();
    let supervisor = h.supervisor.clone();
    let in_flight = tokio::spawn(async move {
        supervisor
            .run(run_call("ns/app", "run", &[("main.wasm", "hang")]), None)
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.launcher.kill(0);
    let err = in_flight.await.unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    eventually("a second host", || h.launcher.hosts().len() == 2).await;
    eventually("the task configured in the new host", || {
        h.launcher.hosts()[1].1.state.lock().unwrap().tasks.len() == 1
    })
    .await;
    let reply = h
        .supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "again")]), None)
        .await
        .unwrap();
    assert_eq!(reply.stdout, b"again");
    let (_, hosts) = h.supervisor.stats("", "").await;
    assert_eq!(hosts[0].restarts, 1);
    assert_eq!(hosts[0].pid, 1001);
}

#[tokio::test]
async fn replicas_split_a_task_when_one_host_has_too_few_slots() {
    // A max_map_count that leaves two slots per host.
    let h = harness_with(
        WorkerIsolateConfigFull {
            confine: crate::structs::isolate::ConfineMode::BestEffort,
            slots_per_host: 2,
            ..Default::default()
        },
        1 << 24,
    );
    assert_eq!(h.supervisor.slots_per_host(), 2);
    // 80 MiB over 16 MiB: five residents, so three replicas of two slots.
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("tenant")),
        80.0,
    )]);
    h.supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "x")]), None)
        .await
        .unwrap();
    let hosts = h.launcher.hosts();
    assert_eq!(hosts.len(), 3);
    let split: Vec<u32> = hosts
        .iter()
        .map(|(_, host)| {
            host.state
                .lock()
                .unwrap()
                .tasks
                .values()
                .next()
                .unwrap()
                .max_resident
        })
        .collect();
    assert_eq!(split, vec![2, 2, 1]);
    // A tenant's runs keep going to the same replica.
    for _ in 0..3 {
        let mut call = run_call("ns/app", "run", &[("main.wasm", "x")]);
        call.request.tenant = "user-42".into();
        h.supervisor.run(call, None).await.unwrap();
    }
    let counts: Vec<u64> = h
        .launcher
        .hosts()
        .iter()
        .map(|(_, host)| host.state.lock().unwrap().invocations)
        .collect();
    assert!(counts.contains(&3) || counts.contains(&4), "{counts:?}");
}

#[test]
fn the_slot_clamp_is_applied_to_hosts() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let h = harness_with(
            WorkerIsolateConfigFull {
                confine: crate::structs::isolate::ConfineMode::BestEffort,
                ..Default::default()
            },
            65_530,
        );
        h.supervisor.sync(vec![envelope(
            "ns/app",
            isolate_task("run", module_config("request")),
            64.0,
        )]);
        h.supervisor
            .run(run_call("ns/app", "run", &[("main.wasm", "x")]), None)
            .await
            .unwrap();
        assert_eq!(h.launcher.hosts()[0].0.config.slots, (65_530 - 2048) / 8);
    });
}

#[tokio::test]
async fn session_isolates_start_run_hibernate_wake_and_stop() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("session")),
        1024.0,
    )]);
    // Request lifetime tasks have nothing to start.
    let started = h
        .supervisor
        .start_session("ns/app", "run", "run-abc.1", "user-1", "manager", "", "")
        .await
        .unwrap();
    assert!(!started.deduplicated);
    let again = h
        .supervisor
        .start_session("ns/app", "run", "run-abc.1", "user-1", "manager", "", "")
        .await
        .unwrap();
    assert!(again.deduplicated);
    let session_run = || {
        let mut call = run_call("ns/app", "run", &[("main.wasm", "count")]);
        call.request.session_id = "run-abc.1".into();
        call
    };
    let first = h.supervisor.run(session_run(), None).await.unwrap();
    let second = h.supervisor.run(session_run(), None).await.unwrap();
    assert_eq!(
        (first.stdout.as_slice(), second.stdout.as_slice()),
        (&b"1"[..], &b"2"[..])
    );
    assert_eq!(second.session_id, "run-abc.1");
    assert_eq!(
        h.supervisor.session_status("run-abc.1").unwrap().state,
        "running"
    );
    // Hibernate: the host writes its checkpoint through the worker.
    let (bytes, generation, already) = h.supervisor.hibernate("run-abc.1").await.unwrap();
    assert_eq!((bytes, generation, already), (1, 1, false));
    assert_eq!(
        h.supervisor.session_status("run-abc.1").unwrap().state,
        "suspended"
    );
    let isolate = h.supervisor.get_isolate("run-abc.1").await.unwrap();
    assert_eq!(
        (isolate.state.as_str(), isolate.checkpoint_generation),
        ("hibernated", 1)
    );
    let (tasks, _) = h.supervisor.stats("ns/app", "run").await;
    assert_eq!(tasks[0].hibernated, 1);
    // The next call wakes it with its state.
    let woke = h.supervisor.run(session_run(), None).await.unwrap();
    assert_eq!(woke.stdout, b"3");
    assert_eq!(woke.isolate_start, "hibernated");
    // A worker restart keeps the record of a hibernated session.
    h.supervisor.hibernate("run-abc.1").await.unwrap();
    let book =
        super::sessions::SessionBook::open(Some(h._dir.path().join("isolates/sessions.json")));
    assert_eq!(book.get("run-abc.1").unwrap().generation, 2);
    // Resize: above the worker's maxMemory needs a restart; cpu is not
    // per session.
    let (outcome, message, ..) = h
        .supervisor
        .resize_session("run-abc.1", None, Some(8192.0), None, false)
        .await
        .unwrap();
    assert_eq!(outcome, "needs_restart");
    assert!(message.contains("isolate.maxMemory"), "{message}");
    let (outcome, ..) = h
        .supervisor
        .resize_session("run-abc.1", None, None, Some(100.0), false)
        .await
        .unwrap();
    assert_eq!(outcome, "not_supported");
    let (outcome, _, _, to, ..) = h
        .supervisor
        .resize_session("run-abc.1", Some(32.0), Some(256.0), None, false)
        .await
        .unwrap();
    assert_eq!(outcome, "applied");
    assert_eq!(to, json!({ "memory": 32, "memoryMax": 256 }));
    // Stop: gone from the host and the book, checkpoint dropped.
    assert!(h.supervisor.stop_session("run-abc.1", "requested").await);
    assert!(!h.supervisor.knows_session("run-abc.1"));
    assert!(!h.supervisor.stop_session("run-abc.1", "requested").await);
    let mut call = session_run();
    call.request.session_id = "run-abc.1".into();
    assert_eq!(
        h.supervisor.run(call, None).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    assert!(std::fs::read_dir(h._dir.path().join("checkpoints"))
        .unwrap()
        .next()
        .is_none());
}

#[tokio::test]
async fn request_lifetime_tasks_have_nothing_to_start() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    let err = h
        .supervisor
        .start_session("ns/app", "run", "run-x.1", "", "manager", "", "")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("there is nothing to start"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn a_host_only_acts_as_its_own_cordons_jobs() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    h.supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "x")]), None)
        .await
        .unwrap();
    let host = h.launcher.hosts()[0].1.clone();
    let mut worker = host.worker.clone();
    let foreign = worker
        .egress(EgressRequest {
            origin: Some(Origin {
                job_id: "other-ns/app".into(),
                task_name: "run".into(),
                ..Default::default()
            }),
            url: "http://example.com".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(foreign.code(), tonic::Code::PermissionDenied);
    let own = worker
        .egress(EgressRequest {
            origin: Some(Origin {
                job_id: "ns/app".into(),
                task_name: "run".into(),
                ..Default::default()
            }),
            url: "http://example.com".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        own.error.unwrap().kind,
        EgressErrorKind::EgressUnavailable as i32,
        "no datapath wired: a typed refusal"
    );
}

#[tokio::test]
async fn stats_fold_and_count() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    for _ in 0..3 {
        h.supervisor
            .run(run_call("ns/app", "run", &[("main.wasm", "x")]), None)
            .await
            .unwrap();
    }
    h.supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "spin")]), None)
        .await
        .unwrap();
    let (tasks, hosts) = h.supervisor.stats("", "").await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].invocations, 4);
    assert_eq!(tasks[0].stopped["limits"], 1);
    assert_eq!(tasks[0].reservation_bytes, 16 << 20);
    assert_eq!(tasks[0].cordon, "ns");
    assert_eq!(hosts[0].confinement.len(), 5);
    assert_eq!(hosts[0].engine_version, "wasmtime-fake");
    let (filtered, _) = h.supervisor.stats("other/job", "").await;
    assert!(filtered.is_empty());
}

#[tokio::test]
async fn args_reach_the_host_and_are_bounded() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("request")),
        1024.0,
    )]);
    let mut call = run_call("ns/app", "run", &[("main.wasm", "x")]);
    call.request.args = vec!["spin".into(), "1".into()];
    h.supervisor.run(call.clone(), None).await.unwrap();
    assert_eq!(
        h.launcher.hosts()[0].1.state.lock().unwrap().last_args,
        vec!["spin", "1"]
    );
    // A keyed retry with other args is another request.
    call.request.idempotency_key = Some("k".into());
    let first = h.supervisor.run(call.clone(), None).await.unwrap();
    call.request.args = vec!["echo".into()];
    let other = h.supervisor.run(call.clone(), None).await.unwrap();
    assert!(!other.deduplicated);
    assert_ne!(first.run_id, other.run_id);
    call.request.args = vec!["a".into(); 65];
    assert_eq!(
        h.supervisor
            .run(call.clone(), None)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    call.request.args = vec!["a".repeat(70 << 10)];
    assert_eq!(
        h.supervisor.run(call, None).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    // Elsewhere args are an isolate-only field.
    let generic = RunSessionRequest {
        args: vec!["x".into()],
        ..Default::default()
    };
    assert_eq!(
        crate::worker::task_manager::session_runs::isolate_only_field(&generic),
        Some("args")
    );
}

#[tokio::test]
async fn restored_session_isolates_are_registered_as_child_owners() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("session")),
        1024.0,
    )]);
    h.supervisor
        .start_session("ns/app", "run", "run-own.1", "", "manager", "", "")
        .await
        .unwrap();
    // A worker restart: a new supervisor over the same book.
    let restored =
        super::sessions::SessionBook::open(Some(h._dir.path().join("isolates/sessions.json")));
    assert!(restored.get("run-own.1").is_some());
    let again = harness_with_book(h._dir.path());
    let owners = again.session_owner_refs();
    assert_eq!(owners.len(), 1);
    assert!(owners[0].session);
    let dir = tempfile::tempdir().unwrap();
    let task_manager = Arc::new(Mutex::new(
        crate::worker::task_manager::WorkerTaskManager::with_state_store(
            crate::worker::state::WorkerStateStore::new(dir.path().join("state.json")),
        ),
    ));
    let vms = super::vm::ChildVms::new(
        task_manager.clone(),
        "node-1",
        Arc::new(crate::worker::launch_pool::LaunchPool::new(
            1,
            Duration::from_secs(1),
        )),
        1 << 20,
    );
    for owner in &owners {
        vms.register_owner(owner);
    }
    let owner = super::vm::ChildVms::owner_of(&owners[0]);
    assert!(task_manager.lock().unwrap().isolate_owner(&owner).is_some());
}

fn harness_with_book(dir: &std::path::Path) -> Arc<IsolateSupervisor> {
    IsolateSupervisor::new(SupervisorParts {
        node_id: "node-1".into(),
        config: WorkerIsolateConfigFull::default(),
        binary: IsolateBinary {
            path: "/nonexistent".into(),
            engine_version: "wasmtime-fake".into(),
            compat_key: "fake".into(),
        },
        images: None,
        launcher: Arc::new(FakeLauncher::default()),
        modules: Arc::new(FakeModules),
        read_dirs: vec![],
        book_path: Some(dir.join("isolates/sessions.json")),
        checkpoints: CheckpointStore {
            store: None,
            dir: dir.join("checkpoints"),
        },
        run_registry: RunRegistry::new(dir.join("no-sessions.db")),
        gate: Arc::new(crate::worker::runtime_state::RuntimeGate::beside(
            &dir.join("worker_state.json"),
        )),
        queue_timeout: Duration::from_secs(1),
        max_map_count: 1 << 24,
        paging_unavailable: None,
    })
}

/// Whether the fake host holds this session resident.
fn host_resident(h: &Harness, session_id: &str) -> bool {
    h.launcher.hosts().iter().any(|(_, host)| {
        host.state
            .lock()
            .unwrap()
            .sessions
            .get(session_id)
            .is_some_and(|(_, resident)| *resident)
    })
}

fn host_reclaims(h: &Harness) -> Vec<ReclaimRequest> {
    h.launcher
        .hosts()
        .iter()
        .flat_map(|(_, host)| host.state.lock().unwrap().reclaims.clone())
        .collect()
}

#[tokio::test]
async fn suspending_one_session_isolate_hibernates_only_that_one() {
    let h = harness();
    h.supervisor.sync(vec![envelope(
        "ns/app",
        isolate_task("run", module_config("session")),
        1024.0,
    )]);
    for id in ["run-a.1", "run-b.1"] {
        h.supervisor
            .start_session("ns/app", "run", id, "", "manager", "", "")
            .await
            .unwrap();
        let mut call = run_call("ns/app", "run", &[("main.wasm", "count")]);
        call.request.session_id = id.into();
        h.supervisor.run(call, None).await.unwrap();
    }
    assert!(host_resident(&h, "run-a.1") && host_resident(&h, "run-b.1"));
    h.supervisor.hibernate("run-a.1").await.unwrap();
    assert!(!host_resident(&h, "run-a.1"), "the named session hibernated");
    assert!(host_resident(&h, "run-b.1"), "the other stays resident");
    assert_eq!(
        h.supervisor.session_status("run-b.1").unwrap().state,
        "running"
    );
    let asked = host_reclaims(&h);
    assert_eq!(asked.len(), 1, "{asked:?}");
    assert_eq!(asked[0].session_ids, vec!["run-a.1".to_string()]);
    assert!(asked[0].job_id.is_empty() && asked[0].task_name.is_empty());
}

#[tokio::test]
async fn a_drain_hibernates_only_the_covered_jobs_session_isolates() {
    use crate::structs::runtime_state::{RuntimeDisable, RuntimeDisableSet};
    let h = harness();
    let shared = || {
        let mut config = module_config("session");
        config["cordon"] = json!("shared");
        isolate_task("run", config)
    };
    // Two jobs in different namespaces sharing one host.
    let mut other = envelope("b/app", shared(), 1024.0);
    other.allocation_id = 8;
    h.supervisor
        .sync(vec![envelope("a/app", shared(), 1024.0), other]);
    for (job, id) in [("a/app", "run-a.1"), ("b/app", "run-b.1")] {
        h.supervisor
            .start_session(job, "run", id, "", "manager", "", "")
            .await
            .unwrap();
        let mut call = run_call(job, "run", &[("main.wasm", "count")]);
        call.request.session_id = id.into();
        h.supervisor.run(call, None).await.unwrap();
    }
    assert_eq!(h.launcher.hosts().len(), 1, "one shared host");
    h.gate.replace(RuntimeDisableSet::new(vec![RuntimeDisable {
        runtime: "isolate".into(),
        namespaces: vec!["a".into()],
        reason: "CVE".into(),
        since: "now".into(),
        by: "op".into(),
        drain: true,
    }]));
    h.supervisor.maintain().await;
    assert!(!host_resident(&h, "run-a.1"), "the covered job's session");
    assert!(host_resident(&h, "run-b.1"), "another job's session stays");
    assert_eq!(
        h.supervisor.session_status("run-a.1").unwrap().state,
        "suspended"
    );
    let asked = host_reclaims(&h);
    assert!(
        asked
            .iter()
            .all(|r| r.job_id == "a/app" && r.session_ids.is_empty()),
        "{asked:?}"
    );
    // The pressure step stays host-wide.
    h.supervisor
        .reclaim(ReclaimLevel::ReclaimHibernate, 0, Duration::ZERO, None, "")
        .await;
    assert!(!host_resident(&h, "run-b.1"), "pressure hibernates every idle session");
}

#[tokio::test]
async fn the_host_cgroup_follows_envelope_resizes() {
    let h = harness();
    let root = h._dir.path().join("cgroups");
    *h.launcher.cgroup_root.lock().unwrap() = Some(root.clone());
    let task = || isolate_task("run", module_config("request"));
    h.supervisor.sync(vec![envelope("ns/app", task(), 1024.0)]);
    h.supervisor
        .run(run_call("ns/app", "run", &[("main.wasm", "x")]), None)
        .await
        .unwrap();
    let max = || std::fs::read_to_string(root.join("ns-0/memory.max")).unwrap();
    assert_eq!(max(), (1024u64 << 20).to_string());
    // The envelope grows: memory.max follows.
    h.supervisor.sync(vec![envelope("ns/app", task(), 2048.0)]);
    for _ in 0..100 {
        if max() == (2048u64 << 20).to_string() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(max(), (2048u64 << 20).to_string());
    // A second task in the cordon adds its envelope.
    let mut other = envelope("ns/two", task(), 512.0);
    other.allocation_id = 8;
    h.supervisor
        .sync(vec![envelope("ns/app", task(), 2048.0), other]);
    for _ in 0..100 {
        if max() == (2560u64 << 20).to_string() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(max(), (2560u64 << 20).to_string());
}
