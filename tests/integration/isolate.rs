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
    // SAFETY: killing the host process this test's supervisor started.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
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
