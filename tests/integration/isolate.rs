//! The isolate runtime's worker side against the real `legion-isolate`
//! binary (docs/task-runtime-isolate.md): the engine probe, compiling a
//! component into derived content, and the supervisor driving real host
//! processes.
//!
//! The binary comes from `LEGION_ISOLATE_BINARY`, else the build profile
//! directory this test binary runs from, else it is built once with
//! `cargo build -p legion-isolate`. A test that cannot get it fails with a
//! message saying so: it never passes by skipping.

use legion::content_store::{ContentStore, ContentStoreConfig};
use legion::prelude::*;
use legion::structs::isolate::{ConfineMode, WorkerIsolateConfigFull};
use legion::structs::manager_server::Manager as _;
use legion::structs::{RunFile, RunSessionRequest, TaskConfig};
use legion::worker::isolates::binary::{self, ImageCatalog, ImageEntry, IsolateBinary};
use legion::worker::isolates::host::ProcessLauncher;
use legion::worker::isolates::modules::{Compiler, ModuleSource, ModuleWant};
use legion::worker::isolates::sessions::CheckpointStore;
use legion::worker::isolates::{IsolateEnvelope, IsolateSupervisor, SupervisorParts};
use legion::worker::task_manager::session_runs::{RunCall, RunRegistry};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("crates/isolate/tests/fixtures")
        .join(name)
}

/// The `legion-isolate` binary, found or built once.
fn isolate_binary() -> PathBuf {
    static FOUND: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    FOUND
        .get_or_init(|| {
            if let Some(path) = std::env::var_os("LEGION_ISOLATE_BINARY") {
                return Ok(PathBuf::from(path));
            }
            // target/<profile>/deps/integration-<hash> -> target/<profile>.
            let profile_dir = std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent()?.parent().map(Path::to_path_buf));
            if let Some(path) = profile_dir
                .as_ref()
                .map(|dir| dir.join("legion-isolate"))
                .filter(|path| path.is_file())
            {
                return Ok(path);
            }
            let status = std::process::Command::new(env!("CARGO"))
                .args(["build", "-p", "legion-isolate", "-j", "3"])
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .status()
                .map_err(|err| format!("could not run cargo to build legion-isolate: {err}"))?;
            if !status.success() {
                return Err(format!("cargo build -p legion-isolate failed ({status})"));
            }
            profile_dir
                .map(|dir| dir.join("legion-isolate"))
                .filter(|path| path.is_file())
                .ok_or_else(|| "legion-isolate built but not found beside the test binary".into())
        })
        .clone()
        .unwrap_or_else(|why| {
            panic!(
                "the isolate integration tests need the legion-isolate binary: {why}. Set \
                 LEGION_ISOLATE_BINARY or run `cargo build -p legion-isolate`."
            )
        })
}

/// Hosts confine themselves as far as the kernel allows
/// (`isolate.confine = "best-effort"`), as a node's would.
fn worker_config() -> WorkerIsolateConfigFull {
    config_confined(ConfineMode::BestEffort)
}

fn config_confined(confine: ConfineMode) -> WorkerIsolateConfigFull {
    WorkerIsolateConfigFull {
        confine,
        max_memory: "256MiB".into(),
        ..Default::default()
    }
}

fn probed() -> IsolateBinary {
    binary::probe(&isolate_binary(), &worker_config()).expect("legion-isolate engine-info")
}

fn sha256_of(path: &Path) -> String {
    legion::content_store::ContentDigest::of_file(path)
        .expect("digest")
        .as_str()
        .to_string()
}

/// A catalog whose `python` image is `file` (any component compiles the
/// same way).
fn catalog_with(dir: &Path, file: &Path) -> ImageCatalog {
    std::fs::copy(file, dir.join("image.wasm")).expect("copy fixture");
    ImageCatalog {
        dir: dir.to_path_buf(),
        images: BTreeMap::from([(
            "python".to_string(),
            ImageEntry {
                file: "image.wasm".into(),
                sha256: sha256_of(&dir.join("image.wasm")),
                ..Default::default()
            },
        )]),
    }
}

#[test]
fn engine_info_speaks_this_workers_protocol() {
    let found = probed();
    assert!(found.engine_version.starts_with("wasmtime-"), "{found:?}");
    assert!(!found.compat_key.is_empty());
}

#[test]
fn a_component_compiles_once_into_derived_content() {
    let dir = tempfile::tempdir().unwrap();
    let images = dir.path().join("images");
    std::fs::create_dir_all(&images).unwrap();
    let store = Arc::new(
        ContentStore::open(dir.path().join("content"), ContentStoreConfig::default())
            .expect("content store"),
    );
    let compiler = Compiler {
        binary: probed(),
        config: worker_config(),
        stager: None,
        store: Some(store.clone()),
        images: Some(catalog_with(&images, &fixture("cmd.wasm"))),
        fallback_dir: dir.path().join("cwasm"),
    };
    let want = ModuleWant::Image("python".into());
    let first = compiler.compiled(&want).expect("compile cmd.wasm");
    assert!(first.path.is_file(), "{first:?}");
    assert!(
        first
            .path
            .starts_with(legion::worker::isolates::modules::derived_dir(&store)),
        "kept as derived content: {first:?}"
    );
    let modified = std::fs::metadata(&first.path).unwrap().modified().unwrap();
    let second = compiler.compiled(&want).expect("compile again");
    assert_eq!(first, second, "the same key and artifact");
    assert_eq!(
        std::fs::metadata(&second.path).unwrap().modified().unwrap(),
        modified,
        "served from the store, not compiled again"
    );
}

#[test]
fn a_file_that_is_not_a_component_is_the_tasks_problem() {
    let dir = tempfile::tempdir().unwrap();
    let images = dir.path().join("images");
    std::fs::create_dir_all(&images).unwrap();
    let junk = dir.path().join("junk.wasm");
    std::fs::write(&junk, b"\0asm junk that is not a component").unwrap();
    let compiler = Compiler {
        binary: probed(),
        config: worker_config(),
        stager: None,
        store: None,
        images: Some(catalog_with(&images, &junk)),
        fallback_dir: dir.path().join("cwasm"),
    };
    let problem = compiler
        .compiled(&ModuleWant::Image("python".into()))
        .unwrap_err();
    assert!(problem.contains("failed to compile"), "{problem}");
    // A pinned digest that is not the node's build is refused before
    // anything compiles.
    let pinned = format!("python@sha256:{}", "0".repeat(64));
    let problem = compiler.compiled(&ModuleWant::Image(pinned)).unwrap_err();
    assert!(problem.contains("but this node has"), "{problem}");
}

// ---------------------------------------------------------------------------
// End to end: the supervisor and real host processes
// ---------------------------------------------------------------------------

struct Node {
    supervisor: Arc<IsolateSupervisor>,
    _dir: tempfile::TempDir,
}

/// A supervisor with the real launcher and compiler, whose `python` image
/// is the given fixture.
fn node(image_fixture: &str) -> Node {
    node_confined(image_fixture, ConfineMode::BestEffort)
}

fn node_confined(image_fixture: &str, confine: ConfineMode) -> Node {
    // The hosts' stderr goes to the worker log: show it on a failure.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("legion_worker::worker::isolates=debug")
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().unwrap();
    let images = dir.path().join("images");
    std::fs::create_dir_all(&images).unwrap();
    let store = Arc::new(
        ContentStore::open(dir.path().join("content"), ContentStoreConfig::default()).unwrap(),
    );
    let found = probed();
    let fallback = dir.path().join("cwasm");
    std::fs::create_dir_all(&fallback).unwrap();
    let derived = legion::worker::isolates::modules::derived_dir(&store);
    std::fs::create_dir_all(&derived).unwrap();
    let catalog = catalog_with(&images, &fixture(image_fixture));
    let supervisor = IsolateSupervisor::new(SupervisorParts {
        node_id: "node-1".into(),
        config: config_confined(confine),
        images: Some(catalog.clone()),
        launcher: Arc::new(ProcessLauncher {
            binary: found.path.clone(),
            cgroup_dir: None,
        }),
        modules: Arc::new(Compiler {
            binary: found.clone(),
            config: config_confined(confine),
            stager: None,
            store: Some(store.clone()),
            images: Some(catalog),
            fallback_dir: fallback.clone(),
        }),
        binary: found,
        read_dirs: vec![fallback, derived],
        book_path: Some(dir.path().join("isolates/sessions.json")),
        checkpoints: CheckpointStore {
            store: Some(store),
            dir: dir.path().join("checkpoints"),
        },
        run_registry: RunRegistry::new(dir.path().join("none.db")),
        gate: Arc::new(legion::worker::runtime_state::RuntimeGate::beside(
            &dir.path().join("worker_state.json"),
        )),
        queue_timeout: Duration::from_secs(1),
        max_map_count: legion::worker::isolates::plan::read_max_map_count(),
        paging_unavailable: Some("tests".into()),
    });
    Node {
        supervisor,
        _dir: dir,
    }
}

fn task(name: &str, lifetime: &str, cordon: &str) -> TaskConfig {
    let mut task = TaskConfig::new(name, "isolate");
    task.config = json!({
        "image": "python",
        "lifetime": lifetime,
        "cordon": cordon,
        "limits": { "cpu": "200ms", "wall": "2s", "output": "4KiB" },
    });
    task.resources = json!({ "cpu": 10, "memory": 16, "memoryMax": 128 });
    task
}

fn envelope(job: &str, task: TaskConfig) -> IsolateEnvelope {
    IsolateEnvelope {
        job_id: job.into(),
        allocation_id: 9,
        spec: legion::worker::task_manager::SessionSpec {
            job_config_id: "jc".into(),
            task_config_id: 1,
            task,
        },
        room: BTreeMap::from([("memory".to_string(), 1024.0), ("cpu".to_string(), 1000.0)]),
    }
}

/// A run whose `args` pick the fixture's mode.
fn run(job: &str, args: &[&str]) -> RunCall {
    RunCall {
        job_id: job.into(),
        task_name: "run".into(),
        request: RunSessionRequest {
            job_id: job.into(),
            task_name: "run".into(),
            args: args.iter().map(|a| a.to_string()).collect(),
            files: vec![RunFile {
                path: "main.py".into(),
                content: b"# the fixture ignores its source".to_vec(),
                executable: false,
            }],
            wait_timeout_seconds: Some(120),
            ..Default::default()
        },
        ticket_jti: String::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_executes_in_a_real_host_and_hosts_follow_cordons() {
    let node = node("cmd.wasm");
    let mut alone = task("run", "request", "job");
    alone.name = "run".into();
    node.supervisor.sync(vec![
        envelope("ns/a", task("run", "request", "namespace")),
        envelope("ns/b", task("run", "request", "namespace")),
        envelope("ns/c", alone),
    ]);
    let reply = node
        .supervisor
        .run(run("ns/a", &[]), None)
        .await
        .expect("run");
    assert_eq!(reply.exit, Some(0));
    assert!(String::from_utf8_lossy(&reply.stdout).contains("args="));
    assert_eq!(reply.tier, "isolate");
    node.supervisor
        .run(run("ns/b", &[]), None)
        .await
        .expect("run b");
    node.supervisor
        .run(run("ns/c", &[]), None)
        .await
        .expect("run c");
    let (_, hosts) = node.supervisor.stats("", "").await;
    assert_eq!(
        hosts.len(),
        2,
        "one host for namespace ns, one for job ns/c"
    );
    let (tasks, _) = node.supervisor.stats("", "").await;
    assert!(tasks.iter().all(|t| t.problem.is_empty()), "{tasks:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn limits_stop_runs_and_output_truncates() {
    let node = node("cmd.wasm");
    node.supervisor
        .sync(vec![envelope("ns/a", task("run", "request", "namespace"))]);
    let spin = node
        .supervisor
        .run(run("ns/a", &["spin"]), None)
        .await
        .unwrap();
    assert_eq!(spin.stopped.unwrap().limit, "cpu");
    let alloc = node
        .supervisor
        .run(run("ns/a", &["alloc"]), None)
        .await
        .unwrap();
    assert_eq!(alloc.stopped.unwrap().limit, "memory");
    let print = node
        .supervisor
        .run(run("ns/a", &["print", "100000"]), None)
        .await
        .unwrap();
    assert!(print.truncated);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_isolate_keeps_state_across_hibernation() {
    let node = node("runner.wasm");
    node.supervisor
        .sync(vec![envelope("ns/a", task("run", "session", "namespace"))]);
    node.supervisor
        .start_session("ns/a", "run", "run-s.1", "", "manager", "", "")
        .await
        .unwrap();
    let mut set = run("ns/a", &["set", "k", "v"]);
    set.request.session_id = "run-s.1".into();
    node.supervisor.run(set, None).await.unwrap();
    node.supervisor.hibernate("run-s.1").await.unwrap();
    let mut get = run("ns/a", &["get", "k"]);
    get.request.session_id = "run-s.1".into();
    let reply = node.supervisor.run(get, None).await.unwrap();
    assert_eq!(reply.isolate_start, "hibernated");
    assert!(reply.result.contains('v'), "{reply:?}");
    assert!(node.supervisor.stop_session("run-s.1", "test").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_host_is_restarted_and_the_next_run_succeeds() {
    let node = node("cmd.wasm");
    node.supervisor
        .sync(vec![envelope("ns/a", task("run", "request", "namespace"))]);
    node.supervisor.run(run("ns/a", &[]), None).await.unwrap();
    let (_, hosts) = node.supervisor.stats("", "").await;
    let pid = hosts[0].pid;
    // The host forks at start into its own PID namespace: the process the
    // worker spawned (`pid`) waits for the real host, its child.
    let children = |pid: u32| -> Vec<u32> {
        std::fs::read_dir(format!("/proc/{pid}/task"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|task| std::fs::read_to_string(task.path().join("children")).ok())
            .flat_map(|list| {
                list.split_whitespace()
                    .filter_map(|p| p.parse().ok())
                    .collect::<Vec<u32>>()
            })
            .collect()
    };
    let real_hosts = children(pid);
    // SAFETY: killing the host process this test's supervisor started.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    // Killing it kills the real host too (its parent-death signal).
    let gone = |pid: u32| {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|stat| stat.contains(") Z "))
            .unwrap_or(true)
    };
    for _ in 0..100 {
        if real_hosts.iter().all(|c| gone(*c)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        real_hosts.iter().all(|c| gone(*c)),
        "the forked host outlived the process the worker killed: {real_hosts:?}"
    );
    println!("killed host {pid}; its forked host(s) {real_hosts:?} are gone too");
    let mut last = None;
    for _ in 0..100 {
        match node.supervisor.run(run("ns/a", &[]), None).await {
            Ok(reply) => {
                last = Some(reply);
                break;
            }
            Err(status) => assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(last.expect("a run after the restart").exit, Some(0));
    let (_, hosts) = node.supervisor.stats("", "").await;
    assert_eq!(hosts[0].restarts, 1);
    assert_ne!(hosts[0].pid, pid);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_best_effort_confined_host_answers_hello() {
    let node = node_confined("cmd.wasm", ConfineMode::BestEffort);
    node.supervisor
        .sync(vec![envelope("ns/a", task("run", "request", "namespace"))]);
    let reply = node
        .supervisor
        .run(run("ns/a", &[]), None)
        .await
        .expect("run");
    assert_eq!(reply.exit, Some(0));
    let (_, hosts) = node.supervisor.stats("", "").await;
    assert!(!hosts[0].confinement.is_empty(), "{hosts:?}");
}

// ---------------------------------------------------------------------------
// Through the whole stack: a manager on raft, a real worker (task manager,
// worker service on the mTLS worker plane, isolate supervisor), and real
// `legion-isolate` hosts. The manager's `RunSession` is the entry, as the
// CLI's `legion session run`.
// ---------------------------------------------------------------------------

pub(crate) mod cluster {
    use super::*;
    use legion::cluster_tls::{
        certified_key, server_config, tls_incoming, ClusterClientVerifier, SwappableIdentity,
        SwappableRoots,
    };
    use legion::manager::eval::EvalExt;
    use legion::manager::server::ManagerDaemonService;
    use legion::mesh_ca::{generate_csr, sign_csr, CsrSubject};
    use legion::prelude::{Context, Value, R};
    use legion::storage::store::Store;
    use legion::structs::config::{ManagerConfigFull, PersistenceType};
    use legion::structs::job::JobConfig;
    use legion::structs::manager_server::Manager;
    use legion::structs::worker_server::WorkerServer;
    use legion::structs::{
        GetAllocationLogsRequest, Node, NodeMsg, RunSessionResponse, Submission,
    };
    use legion::worker::server::WorkerDaemonService;
    use legion::worker::state::WorkerStateStore;
    use legion::worker::task_manager::WorkerTaskManager;
    use std::sync::Mutex;
    use tonic::{Request, Status};

    pub const JOB: &str = "code-runner";
    pub const NODE: &str = "w-iso";

    /// Where the node's isolate images come from.
    pub enum Images {
        /// A catalog whose `python` image is this test fixture.
        Fixture(&'static str),
        /// A built images directory (`crates/isolate/images/build.sh`).
        Built(PathBuf),
    }

    pub struct Cluster {
        pub svc: ManagerDaemonService,
        pub task_manager: Arc<Mutex<WorkerTaskManager>>,
        pub supervisor: Arc<IsolateSupervisor>,
        pub dir: tempfile::TempDir,
    }

    fn fixture_images(dir: &Path, name: &str) -> PathBuf {
        let images = dir.join("images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::copy(fixture(name), images.join("python.wasm")).unwrap();
        let manifest = json!({ "images": { "python": {
            "file": "python.wasm",
            "sha256": sha256_of(&images.join("python.wasm")),
            "extensions": [".py"],
            "modules": ["json", "re", "math"],
            "vmEntry": "main.py",
        } } });
        std::fs::write(images.join("manifest.json"), manifest.to_string()).unwrap();
        images
    }

    impl Cluster {
        /// One manager, one worker running isolates, and job [`JOB`] with
        /// these tasks placed (its envelopes running on the worker).
        pub async fn start(images: Images, tasks: Value) -> R<Self> {
            let _ = tracing_subscriber::fmt()
                .with_env_filter("legion_worker::worker::isolates=debug")
                .with_test_writer()
                .try_init();
            let dir = tempfile::tempdir()?;
            let images = match images {
                Images::Fixture(name) => fixture_images(dir.path(), name),
                Images::Built(path) => path,
            };
            let mut config = ManagerConfigFull {
                store_path: dir.path().join("store").to_str().context("utf-8")?.into(),
                scheduling_policy: "bin_pack".to_string(),
                scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
                ..Default::default()
            };
            config.persistence.r#type = PersistenceType::Raft;
            config.persistence.raft.bootstrap = true;
            config.persistence.raft.node_id = "mgr-iso".into();
            config.persistence.raft.data_dir =
                dir.path().join("raft").to_str().context("utf-8")?.into();
            let store = tokio::task::block_in_place(|| Store::start_storage(&config))?;
            let svc = ManagerDaemonService::new(store);

            let mut manager = WorkerTaskManager::with_state_store(WorkerStateStore::new(
                dir.path().join("worker").join("worker_state.json"),
            ));
            manager.load_builtin_plugins();
            let task_manager = Arc::new(Mutex::new(manager));
            let isolate_config = WorkerIsolateConfigFull {
                binary: Some(isolate_binary().display().to_string()),
                images: Some(images.display().to_string()),
                confine: ConfineMode::BestEffort,
                max_memory: "512MiB".into(),
                ..Default::default()
            };
            let enforcement = legion::structs::WorkerResourceEnforcementConfig {
                enabled: false,
                ..Default::default()
            };
            let supervisor = legion::worker::isolates::supervisor_setup::start(
                &task_manager,
                NODE,
                isolate_config.clone(),
                Duration::from_secs(1),
                &enforcement,
            )
            .await
            .context("the isolate supervisor did not start")?;
            let launch_pool = Arc::new(legion::worker::launch_pool::LaunchPool::new(
                8,
                Duration::from_secs(1),
            ));
            let child_vms = Arc::new(legion::worker::isolates::vm::ChildVms::new(
                task_manager.clone(),
                NODE,
                launch_pool.clone(),
                16 << 20,
            ));
            supervisor.set_vm_backend(child_vms);
            let service = WorkerDaemonService {
                node_id: NODE.into(),
                task_manager: task_manager.clone(),
                identity: None,
                edge_boot: None,
                direct_exec: None,
                launch_pool,
                node_identity: None,
                netd: None,
            };

            let ca = svc.store.ensure_mesh_ca()?;
            let roots = SwappableRoots::from_pem(&ca.cert_pem)?;
            let (key, csr) = generate_csr(NODE)?;
            let (cert, _) = sign_csr(
                &ca.cert_pem,
                &ca.key_pem,
                &csr,
                &CsrSubject::Node(NODE.to_string()),
            )?;
            let identity = SwappableIdentity::new();
            identity.set(certified_key(&cert, &key)?);
            let tls = server_config(identity, Some(ClusterClientVerifier::new(roots)), false)?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let served = WorkerServer::new(service);
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(served)
                    .serve_with_incoming(tls_incoming(listener, tls))
                    .await;
            });
            svc.store.insert_node(NodeMsg::from(Node {
                id: NODE.to_string(),
                pool: "default".into(),
                status: "ready".into(),
                availability: "available".into(),
                last_heartbeat: "2023-01-01T00:00:00Z".into(),
                heartbeat_source: String::new(),
                config_attrs: json!({
                    "node_id": NODE,
                    "maxAllocations": 50,
                    "workerAddress": format!("https://{address}"),
                }),
                // The scheduler sizes a node's `cpu` and `memory` from its
                // fingerprint (`ResourcePool::from_node`; a `cpu`/`memory`
                // under `config_attrs.resources` is ignored), so this is
                // the node's size: 16 cores of 2500 MHz (40000) and
                // 64 GiB. One core of 2000 MHz held only two 1000-MHz
                // envelopes, and a job's third session task went unplaced.
                fingerprint_attrs: json!({
                    "memory_total": "68719476736",
                    "cpus": (1..=16)
                        .map(|core| (format!("cpu-{core}"), json!({ "frequency": 2500.0 })))
                        .collect::<serde_json::Map<String, Value>>(),
                }),
                dynamic_attrs: json!({
                    "capabilities": ["sessions", "isolates"],
                    "protocolVersion": 3,
                }),
                health_checks: json!({}),
            }))?;
            let cluster = Cluster {
                svc,
                task_manager,
                supervisor,
                dir,
            };
            cluster.submit(tasks)?;
            Ok(cluster)
        }

        pub fn store(&self) -> &Store {
            &self.svc.store
        }

        /// Store the job and place it. Straight into the store: a test that
        /// needs a job the `JobSubmit` RPC would refuse (an `exec` VM task
        /// standing in for a sandbox) says so where it builds it.
        pub fn submit(&self, tasks: Value) -> R<()> {
            let config: JobConfig = serde_json::from_value(json!({ "name": JOB, "tasks": tasks }))?;
            tokio::task::block_in_place(|| -> R<()> {
                self.store().handle_job_submit(
                    &config,
                    &[Submission {
                        path: format!("{JOB}.json"),
                        content: "{}".to_string(),
                        is_primary: true,
                    }],
                )?;
                let config_id = self.store().get_job_config(JOB)?.id();
                self.store().enqueue_eval(
                    "job_reschedule",
                    Some(JOB),
                    Some(&config_id),
                    None,
                    None,
                )?;
                while let Some(eval) = self.store().claim_eval()? {
                    eval.handle(self.store())?;
                    self.store().complete_eval(&eval.id, true)?;
                }
                Ok(())
            })
        }

        /// Wait until the worker runs `task`'s envelope (and, for an isolate
        /// task, the supervisor has prepared it).
        pub async fn wait_for(&self, task: &str, isolate: bool) -> R<()> {
            for _ in 0..2400 {
                let envelope = self
                    .task_manager
                    .lock()
                    .expect("task manager")
                    .session_spec(JOB, task)
                    .is_some();
                if envelope
                    && (!isolate || {
                        self.supervisor.sync_from_task_manager();
                        self.supervisor.has_task(JOB, task)
                    })
                {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let held: Vec<String> = self
                .task_manager
                .lock()
                .expect("task manager")
                .isolate_envelopes()
                .into_iter()
                .map(|e| e.spec.task.name)
                .collect();
            let stats = self.supervisor.stats(JOB, "").await.0;
            let events: Vec<String> = self
                .store()
                .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
                    job_id: Some(JOB.into()),
                    limit: Some(100),
                    ..Default::default()
                })
                .map(|events| {
                    events
                        .into_iter()
                        .filter(|e| e.level != "info")
                        .map(|e| format!("{}: {}", e.kind, e.message))
                        .collect()
                })
                .unwrap_or_default();
            eprintln!("scheduler events for {JOB}: {events:#?}");
            anyhow::bail!(
                "the worker never ran the envelope of {JOB}/{task} (isolate envelopes here: \
                 {held:?}; tasks: {:?})",
                stats
                    .iter()
                    .map(|t| (&t.task_name, &t.problem))
                    .collect::<Vec<_>>()
            )
        }

        /// Wait until an isolate task's module is compiled and loaded (its
        /// stats show no problem), up to `limit`.
        pub async fn wait_ready(&self, task: &str, limit: Duration) -> R<()> {
            let started = std::time::Instant::now();
            loop {
                let (tasks, _) = self.supervisor.stats(JOB, task).await;
                match tasks.first() {
                    Some(stats) if stats.problem.is_empty() => return Ok(()),
                    Some(stats) if !stats.problem.contains("still compiling") => {
                        anyhow::bail!("task {task}: {}", stats.problem)
                    }
                    _ => {}
                }
                if started.elapsed() > limit {
                    anyhow::bail!("task {task} was not ready within {limit:?}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        pub async fn run(&self, request: RunSessionRequest) -> Result<RunSessionResponse, Status> {
            self.svc
                .run_session(Request::new(RunSessionRequest {
                    job_id: JOB.into(),
                    wait_timeout_seconds: request.wait_timeout_seconds.or(Some(120)),
                    ..request
                }))
                .await
                .map(|reply| reply.into_inner())
        }

        /// The job's task logs as `legion job logs` reads them: the
        /// manager's `GetAllocationLogs`, forwarded to the worker.
        pub async fn logs(&self, stream: &str) -> R<Vec<(String, String)>> {
            let reply = self
                .svc
                .get_allocation_logs(Request::new(GetAllocationLogsRequest {
                    job: JOB.into(),
                    stream: stream.into(),
                    ..Default::default()
                }))
                .await?
                .into_inner();
            Ok(reply
                .allocations
                .into_iter()
                .flat_map(|alloc| {
                    let task = alloc.task_name.clone();
                    alloc
                        .streams
                        .into_iter()
                        .map(move |s| (format!("{task}:{}", s.stream), s.content))
                })
                .collect())
        }

        /// Start a session isolate of `task` through the manager
        /// (`CreateInstance`, as `legion instance create`); its id.
        pub async fn start_session_isolate(&self, task: &str) -> R<String> {
            Ok(self
                .svc
                .create_instance(Request::new(legion::structs::CreateInstanceRequest {
                    job_id: JOB.into(),
                    task: Some(task.into()),
                    ..Default::default()
                }))
                .await?
                .into_inner()
                .instance_id)
        }

        /// Session ids the worker's task manager holds (VM sessions; never
        /// isolates).
        pub fn sessions(&self) -> Vec<String> {
            self.task_manager
                .lock()
                .expect("task manager")
                .session_statuses()
                .into_iter()
                .map(|status| status.session_id)
                .collect()
        }
    }

    impl Drop for Cluster {
        fn drop(&mut self) {
            if let Ok(mut manager) = self.task_manager.lock() {
                for claim in manager.claims_snapshot() {
                    manager.kill_claim(&claim.claim_id, "test over");
                }
            }
        }
    }

    /// An isolate task `run` of the `python` image (limits are required;
    /// generous ones unless the config sets its own).
    pub fn isolate_task(mut config: Value) -> Value {
        if config.get("limits").is_none() {
            config["limits"] = json!({ "cpu": "2s", "wall": "10s" });
        }
        json!({
            "name": "run",
            "runtime": "isolate",
            "config": config,
            "resources": { "cpu": 10, "memory": 16, "memoryMax": 128 },
            "stateful": {
                "mode": "session",
                "envelope": { "resources": { "cpu": 1000, "memory": 1024 } },
            },
        })
    }

    pub fn main_py(content: &str) -> RunFile {
        RunFile {
            path: "main.py".into(),
            content: content.as_bytes().to_vec(),
            executable: false,
        }
    }
}

use cluster::{isolate_task, main_py, Cluster, Images, JOB};

/// A run through the manager: routed to the worker, executed by a real
/// host, the isolate reply shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_through_the_manager_reaches_a_real_host() -> legion::prelude::R<()> {
    let cluster = Cluster::start(
        Images::Fixture("runner.wasm"),
        json!([isolate_task(
            json!({ "image": "python", "lifetime": "request" })
        )]),
    )
    .await?;
    cluster.wait_for("run", true).await?;
    let reply = cluster
        .run(RunSessionRequest {
            task_name: "run".into(),
            args: vec!["entry".into()],
            files: vec![main_py("print('hi')\n")],
            input: Some("{\"n\":1}".into()),
            ..Default::default()
        })
        .await?;
    assert_eq!(reply.tier, "isolate", "{reply:?}");
    assert_eq!(reply.tier_reason, "auto: no imports outside the image");
    assert_eq!(reply.exit, Some(0));
    assert_eq!(reply.task_name, "run");
    assert_eq!(reply.node_id, cluster::NODE);
    let result: Value = serde_json::from_str(&reply.result)?;
    assert_eq!(result["body"], "print('hi')\n", "{result}");
    assert_eq!(result["input"], "{\"n\":1}", "{result}");
    assert!(reply.cpu_micros > 0 && reply.wall_micros > 0, "{reply:?}");
    assert!(
        cluster.sessions().is_empty(),
        "an isolate run takes no session"
    );
    // A request isolate's output is in its reply, never in the task log.
    assert!(String::from_utf8_lossy(&reply.stdout).contains("ran entry"));
    let logs = cluster.logs("stdout").await?;
    assert!(
        logs.iter()
            .all(|(_, content)| !content.contains("[isolate")),
        "{logs:?}"
    );
    Ok(())
}

/// Plan §6.9: a session isolate's output is in the task's log stream,
/// where `legion job logs` finds it, each line naming its isolate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_isolates_output_is_in_the_job_logs() -> legion::prelude::R<()> {
    let cluster = Cluster::start(
        Images::Fixture("runner.wasm"),
        json!([isolate_task(
            json!({ "image": "python", "lifetime": "session" })
        )]),
    )
    .await?;
    cluster.wait_for("run", true).await?;
    let started = cluster
        .svc
        .create_instance(tonic::Request::new(
            legion::structs::CreateInstanceRequest {
                job_id: JOB.into(),
                task: Some("run".into()),
                ..Default::default()
            },
        ))
        .await?
        .into_inner();
    let id = started.instance_id.clone();
    for mode in ["count", "nope"] {
        cluster
            .run(RunSessionRequest {
                task_name: "run".into(),
                session_id: id.clone(),
                args: vec![mode.into()],
                files: vec![main_py("")],
                ..Default::default()
            })
            .await?;
    }
    // The host forwards the lines after the invocation; the worker appends.
    let want_out = format!("[isolate {id}] ran count\n");
    let want_err = format!("[isolate {id}] unknown mode nope\n");
    let mut seen = Vec::new();
    for _ in 0..100 {
        seen = cluster.logs("").await?;
        let has = |stream: &str, want: &str| {
            seen.iter()
                .any(|(name, content)| name.ends_with(stream) && content.contains(want))
        };
        if has(":stdout", &want_out) && has(":stderr", &want_err) {
            // Both in the envelope allocation's streams.
            assert!(
                seen.iter()
                    .filter(|(_, content)| content.contains("[isolate "))
                    .all(|(name, _)| name.starts_with("run-envelope:")),
                "{seen:?}"
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the session isolate's output never reached the job logs: {seen:?}");
}

// ---------------------------------------------------------------------------
// The VM tier (plan §6.12). A VM task must be a sandbox session task, and
// `JobSubmit` refuses anything else. This container has no /dev/kvm, and
// the sandbox runtime's Linux dev backend runs no guest agent, so it takes
// no exec and therefore no session runs (`SandboxPlugin::exec`: "sandbox
// exec is not supported on ...: the Linux dev path runs no guest agent").
// The tests below that need a real sandbox are `#[ignore]`d for that
// reason; the ones that run here store the job straight into the store
// with an `exec` session task as the VM task (the RPC's rule is not
// changed), so the supervisor's VM path, the session-run machinery and
// child ownership all run for real.
// ---------------------------------------------------------------------------

/// Why the sandbox variants cannot run in this container.
const NO_SANDBOX_RUNS: &str = "needs /dev/kvm: a sandbox session run needs exec, which the Linux \
                               dev backend does not take (no guest agent)";

/// A session task `machine` the isolate task's VM runs go to: `exec`
/// standing in for a sandbox, or a real `sandbox`.
fn machine(runtime: &str) -> Value {
    machine_running(runtime, json!(["sh", "/legion/run/main.py"]))
}

/// [`machine`] with this `run.command`.
fn machine_running(runtime: &str, command: Value) -> Value {
    let config = match runtime {
        "exec" => json!({ "command": ["sleep", "600"] }),
        _ => json!({ "image": "busybox:1.36", "command": ["sleep", "600"] }),
    };
    json!({
        "name": "machine",
        "runtime": runtime,
        "config": config,
        "resources": { "cpu": 100, "memory": 64 },
        "stateful": {
            "mode": "session",
            "envelope": { "resources": { "cpu": 400, "memory": 512 } },
        },
        // Runs the entry (staged as main.py); a run's args follow it.
        "run": { "command": command, "wall": "30s" },
        "children": { "max": 4, "orphanAfter": "5m" },
    })
}

/// The isolate task with `config.vm` naming `machine`.
fn vm_isolate(lifetime: &str, children_max: Option<u32>) -> Value {
    let mut task = isolate_task(json!({
        "image": "python",
        "lifetime": lifetime,
        "limits": { "cpu": "200ms", "wall": "5s" },
        "vm": { "task": "machine", "memoFor": "10m" },
    }));
    task["children"] = json!({ "tasks": ["machine"] });
    if let Some(max) = children_max {
        task["children"]["max"] = json!(max);
    }
    task
}

async fn vm_cluster(runtime: &str, lifetime: &str, children_max: Option<u32>) -> Cluster {
    vm_cluster_with(vm_isolate(lifetime, children_max), machine(runtime)).await
}

async fn vm_cluster_with(isolate: Value, machine: Value) -> Cluster {
    let cluster = Cluster::start(Images::Fixture("runner.wasm"), json!([isolate, machine]))
        .await
        .expect("cluster");
    cluster.wait_for("run", true).await.expect("isolate task");
    cluster.wait_for("machine", false).await.expect("VM task");
    cluster
}

async fn invocations(cluster: &Cluster) -> u64 {
    let (tasks, _) = cluster.supervisor.stats(JOB, "run").await;
    tasks.iter().map(|t| t.invocations).sum()
}

async fn vm_runs(cluster: &Cluster) -> u64 {
    let (tasks, _) = cluster.supervisor.stats(JOB, "run").await;
    tasks.iter().map(|t| t.vm_runs).sum()
}

fn vm_run_request(tier: &str, args: &[&str], source: &str) -> RunSessionRequest {
    RunSessionRequest {
        task_name: "run".into(),
        tier: tier.into(),
        args: args.iter().map(|a| a.to_string()).collect(),
        files: vec![main_py(source)],
        ..Default::default()
    }
}

/// `tier = "vm"`: the run executes in a fresh session of the VM task, with
/// the isolate reply's shape, its args appended to the VM task's command,
/// and the session gone afterwards (never reused).
async fn check_vm_tier_runs(cluster: &Cluster) {
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let reply = cluster
            .run(vm_run_request(
                "vm",
                &["a", "b c"],
                "echo hello from the vm; echo args=$*; echo env=$LEGION_RUN_ARGS",
            ))
            .await
            .expect("vm run");
        assert_eq!(reply.tier, "vm", "{reply:?}");
        assert_eq!(reply.tier_reason, "requested");
        assert_eq!(reply.next_tier, "vm");
        assert_eq!(reply.task_name, "machine");
        assert!(reply.vm_ms > 0, "{reply:?}");
        assert_eq!(reply.exit, Some(0), "{reply:?}");
        assert_eq!(
            String::from_utf8_lossy(&reply.stdout),
            "hello from the vm\nargs=a b c\nenv=[\"a\",\"b c\"]\n",
            "the entry staged as main.py, the args appended to run.command (which ends \
             with it) and exported as LEGION_RUN_ARGS"
        );
        assert!(reply.isolate_start.is_empty(), "{reply:?}");
        sessions.push(reply.session_id.clone());
        assert!(cluster.sessions().is_empty(), "the VM session ended");
    }
    assert_ne!(sessions[0], sessions[1], "a VM session is never reused");
    assert_eq!(vm_runs(cluster).await, 2);
    assert_eq!(invocations(cluster).await, 0, "nothing ran in an isolate");
}

/// `auto` with the isolate runtime disabled for the job goes to the VM.
async fn check_disabled_runtime_goes_to_the_vm(cluster: &Cluster) {
    use legion::structs::runtime_state::{RuntimeDisable, RuntimeDisableSet};
    let gate = cluster.task_manager.lock().unwrap().runtime_gate();
    gate.replace(RuntimeDisableSet::new(vec![RuntimeDisable {
        runtime: "isolate".into(),
        reason: "CVE-2026-0001".into(),
        ..Default::default()
    }]));
    let reply = cluster
        .run(vm_run_request("auto", &[], "echo two"))
        .await
        .expect("auto run");
    assert_eq!(reply.tier, "vm", "{reply:?}");
    assert_eq!(reply.tier_reason, "auto: the isolate runtime is disabled");
    gate.replace(RuntimeDisableSet::new(vec![]));
}

/// A stopped isolate run says where to go next and is not rerun; the next
/// `auto` run of the same source goes straight to the VM.
async fn check_stops_set_next_tier_and_the_memo(cluster: &Cluster) {
    for (mode, reason, source) in [
        ("unsupported", "unsupported", "x = 1  # unsupported"),
        ("spin", "limits", "while True: pass"),
    ] {
        let before = (invocations(cluster).await, vm_runs(cluster).await);
        let reply = cluster
            .run(vm_run_request("auto", &[mode], source))
            .await
            .expect("stopped run");
        assert_eq!(reply.tier, "isolate", "{reply:?}");
        let stopped = reply.stopped.clone().expect("stopped");
        assert_eq!(stopped.reason, reason, "{reply:?}");
        assert_eq!(reply.next_tier, "vm", "{reply:?}");
        assert_eq!(
            (invocations(cluster).await, vm_runs(cluster).await),
            (before.0 + 1, before.1),
            "exactly one invocation, and no VM"
        );
        assert!(cluster.sessions().is_empty(), "no VM session was created");
        let again = cluster
            .run(vm_run_request("auto", &[mode], source))
            .await
            .expect("memo run");
        assert_eq!(again.tier, "vm", "{again:?}");
        assert!(
            again.tier_reason.starts_with("auto: this code"),
            "the memo reason: {again:?}"
        );
        assert_eq!(invocations(cluster).await, before.0 + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_vm_tier_runs_in_a_fresh_vm_session() {
    let cluster = vm_cluster("exec", "request", None).await;
    check_vm_tier_runs(&cluster).await;
    check_disabled_runtime_goes_to_the_vm(&cluster).await;
    check_stops_set_next_tier_and_the_memo(&cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs /dev/kvm: a sandbox session run needs exec, which the Linux dev backend does not take (no guest agent)"]
async fn the_vm_tier_runs_in_a_fresh_sandbox_session() {
    let _ = NO_SANDBOX_RUNS;
    let cluster = vm_cluster("sandbox", "request", None).await;
    check_vm_tier_runs(&cluster).await;
    check_disabled_runtime_goes_to_the_vm(&cluster).await;
    check_stops_set_next_tier_and_the_memo(&cluster).await;
}

/// The child VMs the task manager holds that an owner of this kind and id
/// owns.
fn children_of(cluster: &Cluster, kind: &str, id: &str) -> Vec<String> {
    cluster
        .task_manager
        .lock()
        .unwrap()
        .session_statuses()
        .into_iter()
        .filter(|status| {
            status
                .owner
                .as_ref()
                .is_some_and(|owner| owner.kind == kind && owner.id == id)
        })
        .map(|status| status.session_id)
        .collect()
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what} never happened");
}

/// Child VMs from isolates (plan §6.12): the runner's `vm` mode acquires a
/// child of the VM task and execs `echo hi` in it. A request isolate's
/// child ends with the request.
async fn check_request_child_vm(cluster: &Cluster) {
    let reply = cluster
        .run(vm_run_request("isolate", &["vm"], ""))
        .await
        .expect("vm mode");
    assert_eq!(reply.exit, Some(0), "{reply:?}");
    let result: Value = serde_json::from_str(&reply.result).expect("result");
    assert_eq!(result["stdout"], "hi\n", "{result}");
    let child = result["id"].as_str().unwrap().to_string();
    eventually("the request isolate's child ends", || {
        !cluster.sessions().contains(&child)
    })
    .await;
}

/// A session isolate keeps its child across hibernation, `GetIsolate`
/// lists it, and stopping the isolate ends the child.
async fn check_session_child_vm(cluster: &Cluster) {
    let started = cluster
        .start_session_isolate("run")
        .await
        .expect("session isolate");
    let id = started;
    let mut request = vm_run_request("isolate", &["vm"], "");
    request.session_id = id.clone();
    let reply = cluster.run(request.clone()).await.expect("vm in session");
    assert_eq!(reply.exit, Some(0), "{reply:?}");
    let result: Value = serde_json::from_str(&reply.result).expect("result");
    let child = result["id"].as_str().unwrap().to_string();
    assert_eq!(children_of(cluster, "session", &id), vec![child.clone()]);
    // GetIsolate through the manager (forwarded to the node).
    let isolate = cluster
        .svc
        .get_isolate(tonic::Request::new(legion::structs::GetIsolateRequest {
            session_id: id.clone(),
            ..Default::default()
        }))
        .await
        .expect("GetIsolate")
        .into_inner();
    assert!(
        isolate.children.iter().any(|c| c.session_id == child),
        "GetIsolate lists the child: {isolate:?}"
    );
    cluster.supervisor.hibernate(&id).await.expect("hibernate");
    assert_eq!(
        children_of(cluster, "session", &id),
        vec![child.clone()],
        "the child outlives hibernation"
    );
    assert!(cluster.supervisor.stop_session(&id, "test").await);
    eventually("the session isolate's child ends", || {
        !cluster.sessions().contains(&child)
    })
    .await;
}

/// `children.max` on the isolate task bounds one isolate's children.
async fn check_children_max(cluster: &Cluster) {
    let mut request = vm_run_request("isolate", &["vm"], "");
    let started = cluster
        .start_session_isolate("run")
        .await
        .expect("session isolate");
    request.session_id = started.clone();
    let first = cluster.run(request.clone()).await.expect("first child");
    assert_eq!(first.exit, Some(0), "{first:?}");
    let second = cluster.run(request).await.expect("second acquire");
    assert_eq!(second.exit, Some(3), "the acquire is refused: {second:?}");
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("acquire"),
        "{second:?}"
    );
    cluster.supervisor.stop_session(&started, "test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolates_own_child_vms() {
    let cluster = vm_cluster("exec", "request", None).await;
    check_request_child_vm(&cluster).await;
    drop(cluster);
    let cluster = vm_cluster("exec", "session", Some(1)).await;
    check_session_child_vm(&cluster).await;
    check_children_max(&cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs /dev/kvm: a sandbox session run needs exec, which the Linux dev backend does not take (no guest agent)"]
async fn isolates_own_sandbox_child_vms() {
    let cluster = vm_cluster("sandbox", "request", None).await;
    check_request_child_vm(&cluster).await;
    drop(cluster);
    let cluster = vm_cluster("sandbox", "session", Some(1)).await;
    check_session_child_vm(&cluster).await;
    check_children_max(&cluster).await;
}

/// `args` on the VM tier are appended only to a `run.command` that ends
/// with the staged entry; anything else is refused before it runs, and the
/// args are still readable as `LEGION_RUN_ARGS` without them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vm_tier_args_need_a_command_that_ends_with_the_entry() {
    let cluster = vm_cluster_with(
        vm_isolate("request", None),
        machine_running(
            "exec",
            json!(["sh", "-c", "echo env=$LEGION_RUN_ARGS; echo argv=$*", "sh"]),
        ),
    )
    .await;
    use legion::structs::runtime_state::{RuntimeDisable, RuntimeDisableSet};
    let gate = cluster.task_manager.lock().unwrap().runtime_gate();
    for tier in ["vm", "auto"] {
        if tier == "auto" {
            // auto goes to the VM here because the runtime is disabled: the
            // refusal comes with the tier decision, before anything runs.
            gate.replace(RuntimeDisableSet::new(vec![RuntimeDisable {
                runtime: "isolate".into(),
                ..Default::default()
            }]));
        }
        let err = cluster
            .run(vm_run_request(tier, &["--rm", "-rf"], "echo hi"))
            .await
            .expect_err("args refused");
        assert_eq!(
            err.code(),
            tonic::Code::FailedPrecondition,
            "{tier}: {err:?}"
        );
        assert!(
            err.message()
                .contains("run.command does not end with /legion/run/main.py"),
            "{err:?}"
        );
    }
    gate.replace(RuntimeDisableSet::new(vec![]));
    assert_eq!(invocations(&cluster).await, 0, "nothing ran in an isolate");
    assert_eq!(vm_runs(&cluster).await, 0, "nothing ran in a VM");
    assert!(cluster.sessions().is_empty());
    let reply = cluster
        .run(vm_run_request("vm", &[], "echo hi"))
        .await
        .expect("no args");
    assert_eq!(
        String::from_utf8_lossy(&reply.stdout),
        "env=[]\nargv=\n",
        "{reply:?}"
    );
}

// ---------------------------------------------------------------------------
// HTTP in and out with the real host. Not through netd's TCP ingress
// listener (that needs the worker daemon's datapath, which the harness does
// not run): the supervisor's ingress socket (`isolates::ingress::serve`,
// the socket netd's listener dials for an isolate envelope) takes the
// request, and egress goes through the real `NetdEgress` over a forwarder
// built here (policy, host guard, upstream listener with credential
// injection), as the worker builds it.
// ---------------------------------------------------------------------------

mod http_e2e {
    use super::*;
    use legion::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use legion::structs::AllocNetPolicy;
    use legion::worker::forwarder::{
        ForwarderContext, SecretValue, UpstreamForwarder, UpstreamSpec,
    };
    use legion::worker::netd::attach::Attachment;
    use legion::worker::netd::proxy::{EgressProxies, ProxySpec};
    use legion::worker::netd::{Denials, NetPolicyCache};
    use std::net::{IpAddr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The envelope allocation `envelope()` gives the task.
    const ENVELOPE: i64 = 9;
    const TOKEN: &str = "eyJ.isolate-e2e-secret";

    /// `*.test` names are loopback.
    struct TestResolver;

    #[tonic::async_trait]
    impl legion::worker::netd::guard::Resolver for TestResolver {
        async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
            if host.ends_with(".test") {
                return Ok(vec![SocketAddr::new(IpAddr::from([127, 0, 0, 1]), port)]);
            }
            Err(std::io::Error::other(format!("no such host {host}")))
        }
    }

    /// An origin that sends every request it reads on the channel and
    /// answers `ok`.
    async fn origin() -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 << 10];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nX-Origin: yes\r\nConnection: close\r\n\r\norigin",
                        )
                        .await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (port, rx)
    }

    async fn datapath(dir: &Path, port: u16) -> Arc<UpstreamForwarder> {
        let cache = Arc::new(NetPolicyCache::new());
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require",
            "egress": { "protocol": "http", "allow": ["origin.test"] }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: ENVELOPE,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let guard = legion::structs::host_guard::HostGuard::new(
            &legion::structs::host_guard::HostGuardConfig {
                allow: vec![format!("127.0.0.1:{port}")],
                ..Default::default()
            },
        )
        .unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, Arc::new(Denials::new()))
                .with_resolver(Arc::new(TestResolver))
                .with_host_guard(Arc::new(guard)),
        );
        let proxies = Arc::new(EgressProxies::new(context.clone()));
        let forwarder = Arc::new(UpstreamForwarder::with_context(context).with_proxies(proxies));
        let attachment = Attachment::Isolate {
            dir: dir.to_path_buf(),
        };
        forwarder
            .proxies()
            .unwrap()
            .sync(&[ProxySpec {
                allocation_id: ENVELOPE,
                attachment: attachment.clone(),
                listen: None,
            }])
            .await;
        let credentials: Vec<legion::structs::UpstreamCredential> = serde_json::from_value(json!([
            { "header": "authorization", "prefix": "Bearer ", "from": { "kv": "secrets/api" } },
        ]))
        .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: ENVELOPE,
                service: "api".into(),
                local_port: Some(20020),
                http_protocol: true,
                balance: "round_robin".into(),
                to: Some(format!("http://origin.test:{port}")),
                http: None,
                handlers: Vec::new(),
                credentials,
                ca: None,
                vip: None,
                canonical_port: None,
                attachment,
            }])
            .await;
        // What the manager's FetchDatapathMaterial hands the node for the
        // envelope (the KV value), as the heartbeat would install it.
        forwarder.context().set_credential(
            ENVELOPE,
            "api",
            "authorization",
            "kv secrets/api",
            4_000_000_000,
            SecretValue::new(TOKEN),
        );
        forwarder
    }

    async fn send(socket: &Path, raw: &str) -> String {
        let mut stream = tokio::net::UnixStream::connect(socket).await.unwrap();
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        String::from_utf8_lossy(&out).to_string()
    }

    fn get(path: &str, headers: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nHost: app.test\r\nConnection: close\r\n{headers}\r\n")
    }

    fn body(response: &str) -> &str {
        response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_in_and_out_through_a_real_host() {
        let node = node("handler.wasm");
        let logs = node._dir.path().join("logs");
        node.supervisor.set_log_root(logs.clone());
        let mut tenant_task = task("run", "tenant", "namespace");
        tenant_task.network =
            Some(serde_json::from_value(json!({ "ports": [{ "name": "http" }] })).unwrap());
        node.supervisor.sync(vec![envelope("ns/a", tenant_task)]);
        let (port, mut seen) = origin().await;
        let netdir = node._dir.path().join("envelope-net");
        std::fs::create_dir_all(&netdir).unwrap();
        let forwarder = datapath(&netdir, port).await;
        node.supervisor.set_egress_backend(Arc::new(
            legion::worker::isolates::egress::NetdEgress::new(forwarder, 1 << 20),
        ));
        let socket = node._dir.path().join("ingress.sock_http");
        let target: Arc<dyn legion::worker::isolates::IngressTarget> = node.supervisor.clone();
        let _server = legion::worker::isolates::ingress::serve(
            socket.clone(),
            "ns/a",
            "run",
            1 << 20,
            target,
        )
        .await
        .unwrap();

        // In: the request reaches the isolate, the response comes back.
        // `Legion-Tenant` picks the tenant's warm instance (its own
        // counter) and the guest never sees the header.
        let count = |tenant: &'static str| {
            let socket = socket.clone();
            async move {
                body(
                    &send(
                        &socket,
                        &get("/count", &format!("Legion-Tenant: {tenant}\r\n")),
                    )
                    .await,
                )
                .to_string()
            }
        };
        assert_eq!(count("acme").await, "1");
        assert_eq!(count("acme").await, "2");
        assert_eq!(
            count("globex").await,
            "1",
            "another tenant, another instance"
        );
        assert_eq!(count("acme").await, "3");
        let echo = send(
            &socket,
            &get("/echo", "Legion-Tenant: acme\r\nX-Mine: 1\r\n"),
        )
        .await;
        assert!(echo.starts_with("HTTP/1.1 200"), "{echo}");
        assert!(body(&echo).contains("x-mine=1"), "{echo}");
        assert!(
            !body(&echo).to_ascii_lowercase().contains("legion-tenant"),
            "the guest never sees the routing header: {echo}"
        );

        // Out: an upstream named by key gets the credential the node
        // injects; the guest's view never contains it.
        let fetched = send(
            &socket,
            &get("/fetch?http://api/v1/models", "Legion-Tenant: acme\r\n"),
        )
        .await;
        assert!(fetched.starts_with("HTTP/1.1 200"), "{fetched}");
        assert!(body(&fetched).starts_with("upstream 200\n"), "{fetched}");
        assert!(body(&fetched).ends_with("origin"), "{fetched}");
        assert!(!fetched.contains(TOKEN), "the guest's view: {fetched}");
        let received = seen.recv().await.unwrap();
        assert!(
            received.starts_with("GET /v1/models HTTP/1.1"),
            "{received}"
        );
        assert!(
            received.to_ascii_lowercase().contains(&format!(
                "authorization: bearer {}",
                TOKEN.to_ascii_lowercase()
            )),
            "the origin got the injected credential: {received}"
        );
        // A host the egress policy does not allow fails in the guest.
        let denied = send(
            &socket,
            &get(&format!("/fetch?http://denied.test:{port}/x"), ""),
        )
        .await;
        assert!(denied.starts_with("HTTP/1.1 502"), "{denied}");
        assert!(body(&denied).contains("Denied"), "{denied}");
        assert!(seen.try_recv().is_err(), "nothing reached the origin");

        // A handler's output goes to the envelope's task log.
        let stdout = logs.join(ENVELOPE.to_string()).join("stdout.log");
        for _ in 0..100 {
            if std::fs::read_to_string(&stdout).is_ok_and(|log| log.contains("echo /echo")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let log = std::fs::read_to_string(&stdout).unwrap_or_default();
        assert!(log.contains("tenant=acme] echo /echo"), "{log}");
        assert!(!log.contains(TOKEN));
    }
}


/// Every session task of a job gets its envelope on the node: three isolate
/// tasks and an `exec` session task, each envelope 1000 MHz.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_session_task_of_a_job_gets_its_envelope() -> legion::prelude::R<()> {
    let mut tasks = Vec::new();
    for name in ["a", "b", "c"] {
        let mut task = isolate_task(json!({ "image": "python", "lifetime": "request" }));
        task["name"] = json!(name);
        tasks.push(task);
    }
    let mut vm = machine("exec");
    vm["stateful"]["envelope"]["resources"] = json!({ "cpu": 1000, "memory": 512 });
    tasks.push(vm);
    let cluster = Cluster::start(Images::Fixture("runner.wasm"), Value::Array(tasks)).await?;
    let mut placed: Vec<String> = cluster
        .store()
        .task_allocation_nodes(&[JOB.to_string()])?
        .into_iter()
        .map(|((_, _, task), nodes)| format!("{task}@{}", nodes.join(",")))
        .collect();
    placed.sort();
    assert_eq!(
        placed,
        ["a-envelope@w-iso", "b-envelope@w-iso", "c-envelope@w-iso", "machine-envelope@w-iso"]
    );
    for task in ["a", "b", "c"] {
        cluster.wait_for(task, true).await?;
    }
    cluster.wait_for("machine", false).await?;
    Ok(())
}
