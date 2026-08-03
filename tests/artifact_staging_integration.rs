//! Artifact system integration tests (artifact plan "Testing Plan §Integration"):
//! the full worker staging lifecycle against a local HTTP fixture, the
//! failure path through `record_failure`, the `artifact_states` heartbeat
//! flow, and manager admission (normalization, pinning, regression guards).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use legion::manager::dispatcher::build_task_allocation;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, manager_server::Manager, Allocation, AllocationHealth,
    ArtifactAuthMatcher, ArtifactsManagerConfigFull, GetRolloutStatusRequest, NodeMsg,
    RunJobRequest, Submission, TaskAllocation,
};
use legion::worker::state::WorkerStateStore;
use legion::worker::task_manager::{launch_task_allocation, WorkerTaskManager};
use tempfile::{Builder, TempDir};
use tonic::Request;

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("legion-artifact-it-")
        .tempdir()
        .context("error creating temp store directory")?
        .keep();
    let config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("error parsing store path")?
            .to_string(),
        ..Default::default()
    };
    let store = Store::start_storage(&config)?;
    Ok(store)
}

fn insert_node(store: &Store, id: &str) -> R<()> {
    let node = NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
            }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: json!({ "worker": "pass" }).to_string(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
    };
    store.insert_node(node)?;
    Ok(())
}

fn run_request(jobspec: String, name: &str) -> RunJobRequest {
    RunJobRequest {
        parsed_jobspec: jobspec,
        submissions: vec![Submission {
            path: format!("{name}.ncl"),
            content: name.to_string(),
            is_primary: true,
        }],
        priority: None,
        force_reschedule: false,
        idempotency_key: None,
    }
}

/// Serves `body` for every request until dropped; returns the base URL.
fn serve_archive(body: Vec<u8>, requests_to_serve: usize) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test http server");
    let addr = listener.local_addr().expect("addr");
    let handle = std::thread::spawn(move || {
        for _ in 0..requests_to_serve {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    (format!("http://{addr}"), handle)
}

fn serve_404(requests_to_serve: usize) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test http server");
    let addr = listener.local_addr().expect("addr");
    let handle = std::thread::spawn(move || {
        for _ in 0..requests_to_serve {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    (format!("http://{addr}"), handle)
}

/// Builds a tar.gz with a `run.sh` that writes `marker` into its cwd.
fn tool_archive() -> Vec<u8> {
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::default(),
    ));
    let script = "#!/bin/sh\necho staged-ok > marker\n";
    let mut header = tar::Header::new_gnu();
    header.set_size(script.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();
    builder
        .append_data(&mut header, "tool/run.sh", script.as_bytes())
        .expect("append script");
    builder.into_inner().expect("tar").finish().expect("gzip")
}

fn worker_manager(temp_dir: &TempDir) -> Arc<Mutex<WorkerTaskManager>> {
    let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
    let mut manager = WorkerTaskManager::with_state_store(state_store);
    manager.load_builtin_plugins();
    // Tests fetch over plain http from the local fixture.
    manager.configure_artifacts(
        legion::structs::WorkerArtifactFetchConfigFull {
            attempts: 2,
            allowed_schemes: vec!["http".into(), "https".into(), "file".into()],
            ..Default::default()
        },
        Box::new(legion::artifacts::AnonymousCredentials),
    );
    Arc::new(Mutex::new(manager))
}

fn task_allocation(
    id: i64,
    config: serde_json::Value,
    artifacts: serde_json::Value,
) -> TaskAllocation {
    TaskAllocation {
        provisional: None,
        task_volumes: String::new(),
        allocation_id: id,
        job_id: "job".into(),
        job_config_id: "job-1".into(),
        node_id: "node-1".into(),
        task_config_id: 1,
        task_name: "tool-task".into(),
        task_runtime: "exec".into(),
        task_config: config.to_string(),
        task_checks: String::new(),
        task_on_unhealthy: String::new(),
        task_identities: String::new(),
        self_terminate_after_seconds: 0,
        task_artifacts: artifacts.to_string(),
        task_resources: String::new(),
        network: String::new(),
        task_connect: String::new(),
        ..Default::default()
    }
}

fn wait_until(timeout: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}

/// The fixture script creates `marker` and then writes into it as separate
/// steps, so waiting on existence alone can observe a still-empty file. Wait
/// on the contents instead.
fn marker_written(marker: &Path) -> bool {
    std::fs::read_to_string(marker)
        .map(|s| s.trim() == "staged-ok")
        .unwrap_or(false)
}

/// S1 end to end: an exec task fetches a release tarball over HTTP, the
/// archive unpacks into the task directory, `${artifact.tool}` resolves to
/// the staged path, the process runs from the task dir, and destroy cleans
/// everything up.
#[test]
fn exec_task_stages_http_artifact_and_runs_from_task_dir() -> R<()> {
    let (base_url, server) = serve_archive(tool_archive(), 1);
    let temp_dir = TempDir::new()?;
    let manager = worker_manager(&temp_dir);

    let allocation = task_allocation(
        21,
        json!({"command": ["sh", "${artifact.tool}/tool/run.sh"]}),
        json!([{"source": format!("{base_url}/tool-linux-amd64.tar.gz"), "name": "tool"}]),
    );
    launch_task_allocation(&manager, &allocation)?;

    let task_dir = temp_dir.path().join("allocs").join("21").join("tool-task");
    // Staging then the script run: marker appears in the task dir because
    // exec cwd defaults there.
    let marker = task_dir.join("marker");
    assert!(
        wait_until(Duration::from_secs(15), || marker_written(&marker)),
        "marker not written; staging or start failed: {:?}",
        manager.lock().unwrap().allocation_health_snapshot()
    );
    assert!(task_dir.join("tool/run.sh").is_file(), "archive unpacked");
    assert!(!task_dir.join(".staging").exists(), "staging temp cleaned");

    {
        let guard = manager.lock().unwrap();
        let health = guard.allocation_health_snapshot();
        let entry = health
            .iter()
            .find(|h| h.allocation_id == 21)
            .expect("health");
        let states = entry.artifact_states.as_deref().expect("artifact states");
        assert!(states.contains("\"state\":\"staged\""), "{states}");
        assert!(states.contains("\"digest\":\"sha256:"), "{states}");
        assert!(states.contains("\"name\":\"tool\""), "{states}");
    }

    // Manager drops the allocation -> the worker signals it, then reaps it
    // once the process is gone. Teardown deliberately waits for the exit:
    // the alloc dir (and any volumes under it) must not be removed while a
    // task can still be writing to them (docs/graceful-stop.md).
    let _ = manager.lock().unwrap().reconcile_allocations(&[]);
    assert!(
        wait_until(Duration::from_secs(15), || {
            manager.lock().unwrap().reap_terminating();
            !temp_dir.path().join("allocs").join("21").exists()
        }),
        "alloc dir removed once the stopped task has exited"
    );

    server.join().ok();
    Ok(())
}

/// Fetch failure path: a 404 source exhausts its retries, the allocation
/// fails through `record_failure`, the failure report carries the redacted
/// source, and the per-artifact row ends `failed` — the documented
/// `on_unhealthy` semantics start from exactly this report.
#[test]
fn fetch_failure_after_retries_fails_allocation() -> R<()> {
    let (base_url, server) = serve_404(2);
    let temp_dir = TempDir::new()?;
    let manager = worker_manager(&temp_dir);

    let allocation = task_allocation(
        33,
        json!({"command": ["true"]}),
        json!([{"source": format!("{base_url}/missing.tar.gz?token=secret"), "name": "gone"}]),
    );
    launch_task_allocation(&manager, &allocation)?;

    assert!(
        wait_until(Duration::from_secs(15), || {
            !manager.lock().unwrap().is_allocation_active(33)
        }),
        "allocation should fail after retries"
    );

    let mut guard = manager.lock().unwrap();
    let failures = guard.drain_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].allocation_id, 33);
    assert!(
        failures[0].reason.contains("artifact staging failed"),
        "{}",
        failures[0].reason
    );
    assert!(
        !failures[0].reason.contains("token=secret"),
        "failure reasons must carry redacted sources: {}",
        failures[0].reason
    );
    // The failure report carries the final per-artifact states so the
    // manager can emit artifact.failed events with structured detail.
    let states = failures[0]
        .artifact_states
        .as_deref()
        .expect("failure report carries artifact states");
    assert!(states.contains("\"state\":\"failed\""), "{states}");
    assert!(states.contains("\"name\":\"gone\""), "{states}");
    assert!(!states.contains("token=secret"), "{states}");
    drop(guard);

    assert!(
        !temp_dir.path().join("allocs").join("33").exists(),
        "alloc dir cleaned after failure"
    );
    server.join().ok();
    Ok(())
}

/// Checksum pinning: corrupt bytes (checksum mismatch) never reach the task
/// directory and fail the allocation with a clear reason.
#[test]
fn checksum_mismatch_fails_staging() -> R<()> {
    let (base_url, server) = serve_archive(b"not the expected bytes".to_vec(), 2);
    let temp_dir = TempDir::new()?;
    let manager = worker_manager(&temp_dir);

    let allocation = task_allocation(
        44,
        json!({"command": ["true"]}),
        json!([{
            "source": format!("{base_url}/tool.bin"),
            "name": "tool",
            "checksum": format!("sha256:{}", "0".repeat(64)),
            "unpack": "file",
        }]),
    );
    launch_task_allocation(&manager, &allocation)?;

    assert!(
        wait_until(Duration::from_secs(15), || {
            !manager.lock().unwrap().is_allocation_active(44)
        }),
        "allocation should fail on checksum mismatch"
    );
    let mut guard = manager.lock().unwrap();
    let failures = guard.drain_failures();
    assert!(
        failures[0].reason.contains("checksum mismatch"),
        "{}",
        failures[0].reason
    );
    server.join().ok();
    Ok(())
}

/// `artifact_states` flow: worker heartbeat snapshots persist on the
/// allocation and are served back through rollout status (S6's data path).
/// Sync test: eval handling dispatches allocations, which needs to run
/// outside a current-thread async runtime.
#[test]
fn artifact_states_flow_through_store_to_rollout_status() -> R<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store.clone());
    insert_node(&store, "node-a")?;

    let jobspec = json!({
        "name": "artifact-status-job",
        "tasks": [{
            "name": "tool",
            "runtime": "exec",
            "artifacts": ["https://releases.example.com/tool-v1.tar.gz"],
            "config": {"command": ["./tool"]},
        }],
    })
    .to_string();
    let reply = runtime
        .block_on(manager.run_job(Request::new(run_request(jobspec, "artifact-status-job"))))
        .map_err(|status| anyhow::anyhow!("run_job failed: {status}"))?;
    assert!(!reply.into_inner().deduplicated);

    // Drive queued evals so allocations exist.
    while let Some(eval) = store.get_next_eval()? {
        let id = eval.id.clone();
        eval.handle(&store)?;
        store.set_eval_status(&id, "complete")?;
    }

    let job_config_id = store.get_job_config("artifact-status-job")?.id();
    let allocations = store.allocs_for_job_config(&job_config_id)?;
    assert!(!allocations.is_empty(), "allocation placed");
    let allocation = &allocations[0];

    // The worker would send this through its heartbeat.
    let states = json!([{
        "name": "tool",
        "source": "https://releases.example.com/tool-v1.tar.gz",
        "state": "fetching",
        "bytesFetched": 1024,
        "bytesTotal": 4096,
        "attempts": 1,
    }])
    .to_string();
    store.upsert_allocation_health_snapshots(
        &allocation.node_id,
        &[AllocationHealth {
            allocation_id: allocation.id,
            healthy: false,
            status: "staging artifacts".into(),
            artifact_states: Some(states.clone()),
        }],
    )?;

    let snapshots = store.allocation_health_for_allocations(&[allocation.id])?;
    let snapshot = snapshots.get(&allocation.id).context("snapshot")?;
    assert_eq!(snapshot.artifact_states.as_deref(), Some(states.as_str()));
    assert_eq!(snapshot.status, "staging artifacts");

    // Served through rollout status when a rollout exists for the config.
    let rollouts = store.list_rollouts(Some("artifact-status-job"), false)?;
    if let Some(rollout) = rollouts.first() {
        let status = runtime
            .block_on(
                manager
                    .get_rollout_status(Request::new(GetRolloutStatusRequest { id: rollout.id })),
            )
            .map_err(|status| anyhow::anyhow!("rollout status failed: {status}"))?
            .into_inner();
        let allocation_status = status
            .groups
            .iter()
            .flat_map(|group| group.allocations.iter())
            .find(|a| a.allocation_id == allocation.id);
        if let Some(allocation_status) = allocation_status {
            assert_eq!(
                allocation_status.artifact_states.as_deref(),
                Some(states.as_str())
            );
        }
    }
    Ok(())
}

/// Admission end to end: artifacts round-trip through the store normalized
/// (cluster auth baked in), reach dispatch as `task_artifacts`, and the
/// regression guard holds — jobs without artifacts dispatch byte-identically
/// with an empty `task_artifacts`.
#[tokio::test]
async fn admission_normalizes_artifacts_and_preserves_plain_jobs() -> R<()> {
    let store = setup_store()?;
    let cluster = ManagerConfigFull {
        artifacts: ArtifactsManagerConfigFull {
            auth: vec![ArtifactAuthMatcher {
                r#match: "https://private.example.com/**".into(),
                kv: Some("secrets/private-pull".into()),
                node_identity: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    };
    let manager = ManagerDaemonService::with_manager_config(store.clone(), &cluster);
    insert_node(&store, "node-a")?;

    let jobspec = json!({
        "name": "artifact-admission-job",
        "tasks": [
            {
                "name": "with-artifacts",
                "runtime": "exec",
                "artifacts": [
                    {"source": "https://private.example.com/build.tar.gz", "name": "build"},
                    {"source": "https://public.example.com/tool.tar.gz", "name": "tool",
                     "auth": {"kv": "secrets/own"}},
                ],
                "config": {"command": ["${artifact.build}/run"]},
            },
            {
                "name": "plain",
                "runtime": "exec",
                "config": {"command": ["true"]},
            },
        ],
    })
    .to_string();
    manager
        .run_job(Request::new(run_request(jobspec, "artifact-admission-job")))
        .await
        .map_err(|status| anyhow::anyhow!("run_job failed: {status}"))?;

    let job_config_id = store.get_job_config("artifact-admission-job")?.id();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;

    let with_artifacts = tasks
        .iter()
        .map(|(_, task)| task)
        .find(|task| task.name == "with-artifacts")
        .context("with-artifacts task")?;
    assert_eq!(with_artifacts.artifacts.len(), 2);
    // Cluster matcher filled the unauthenticated artifact's auth.
    let build = &with_artifacts.artifacts[0];
    assert_eq!(build.auth_origin.as_deref(), Some("cluster"));
    assert_eq!(
        build.auth.as_ref().and_then(|auth| auth.kv.as_deref()),
        Some("secrets/private-pull")
    );
    // Task-level auth wins untouched.
    let tool = &with_artifacts.artifacts[1];
    assert_eq!(tool.auth_origin.as_deref(), Some("task"));
    assert_eq!(
        tool.auth.as_ref().and_then(|auth| auth.kv.as_deref()),
        Some("secrets/own")
    );

    // Dispatch carries the normalized list; plain tasks stay empty-string.
    let allocation = Allocation {
        id: 1,
        status: "running".into(),
        resources: json!({}),
        node_id: "node-a".into(),
        job_config_id: job_config_id.clone(),
        task_config_id: 1,
        job_id: "artifact-admission-job".into(),
        instance_id: None,
        network: None,
    };
    let dispatched = build_task_allocation(with_artifacts, &allocation, None, &[])?;
    assert!(dispatched.task_artifacts.contains("secrets/private-pull"));

    let plain = tasks
        .iter()
        .map(|(_, task)| task)
        .find(|task| task.name == "plain")
        .context("plain task")?;
    assert!(plain.artifacts.is_empty());
    let dispatched_plain = build_task_allocation(plain, &allocation, None, &[])?;
    assert_eq!(dispatched_plain.task_artifacts, "");
    Ok(())
}

/// Disallowed schemes fail at `job run`, not on a node twenty minutes later.
#[tokio::test]
async fn plain_http_artifact_rejected_at_admission_by_default() -> R<()> {
    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store.clone());
    insert_node(&store, "node-a")?;

    let jobspec = json!({
        "name": "http-artifact-job",
        "tasks": [{
            "name": "tool",
            "runtime": "exec",
            "artifacts": ["http://insecure.example.com/tool.tar.gz"],
            "config": {"command": ["true"]},
        }],
    })
    .to_string();
    let err = manager
        .run_job(Request::new(run_request(jobspec, "http-artifact-job")))
        .await
        .expect_err("http scheme is not allowed by default");
    assert!(
        err.message().contains("allowedSchemes"),
        "{}",
        err.message()
    );
    Ok(())
}

/// `resolve: "pin"` at submit: a git ref pins to its commit SHA in the
/// stored config, with the original reference kept for provenance.
#[tokio::test(flavor = "multi_thread")]
async fn git_ref_pins_to_commit_sha_at_submit() -> R<()> {
    // Local fixture repository.
    let repo_dir = TempDir::new()?;
    let git = |args: &[&str]| -> R<()> {
        let output = std::process::Command::new("git")
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(repo_dir.path())
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .context("run git")?;
        if !output.status.success() {
            anyhow::bail!(
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    };
    git(&["init", "--quiet", "--initial-branch=main", "."])?;
    std::fs::write(repo_dir.path().join("tool.sh"), "echo hi")?;
    git(&["add", "."])?;
    git(&["commit", "--quiet", "-m", "init"])?;

    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store.clone());
    insert_node(&store, "node-a")?;

    let source = format!("git::file://{}?ref=main", repo_dir.path().display());
    let jobspec = json!({
        "name": "git-pin-job",
        "tasks": [{
            "name": "repo",
            "runtime": "exec",
            "artifacts": [{"source": source.clone(), "name": "repo"}],
            "config": {"command": ["true"]},
        }],
    })
    .to_string();
    manager
        .run_job(Request::new(run_request(jobspec, "git-pin-job")))
        .await
        .map_err(|status| anyhow::anyhow!("run_job failed: {status}"))?;

    let job_config_id = store.get_job_config("git-pin-job")?.id();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let artifact = &tasks[0].1.artifacts[0];
    assert!(
        artifact
            .pinned_from
            .as_deref()
            .unwrap()
            .contains("ref=main"),
        "{:?}",
        artifact.pinned_from
    );
    // Pinned source carries a 40-hex commit SHA instead of the branch name.
    let pinned_ref = artifact
        .source
        .split("ref=")
        .nth(1)
        .context("pinned ref missing")?;
    assert_eq!(pinned_ref.len(), 40, "{}", artifact.source);
    assert!(pinned_ref.chars().all(|c| c.is_ascii_hexdigit()));

    // Pinning joins the event stream: one artifact.pinned event for the
    // submit, with the resolution in metadata.
    let pin_events = store.list_scheduler_events(&SchedulerEventFilter {
        kind: Some("artifact.pinned".into()),
        ..Default::default()
    })?;
    assert_eq!(pin_events.len(), 1, "{pin_events:?}");
    assert_eq!(pin_events[0].job_id.as_deref(), Some("git-pin-job"));
    assert!(
        pin_events[0].metadata_json.contains(pinned_ref),
        "{}",
        pin_events[0].metadata_json
    );

    // An unchanged re-submit resolves the same digest and stays quiet.
    let jobspec = json!({
        "name": "git-pin-job",
        "tasks": [{
            "name": "repo",
            "runtime": "exec",
            "artifacts": [{"source": source, "name": "repo"}],
            "config": {"command": ["true"]},
        }],
    })
    .to_string();
    manager
        .run_job(Request::new(run_request(jobspec, "git-pin-job")))
        .await
        .map_err(|status| anyhow::anyhow!("re-run_job failed: {status}"))?;
    let pin_events = store.list_scheduler_events(&SchedulerEventFilter {
        kind: Some("artifact.pinned".into()),
        ..Default::default()
    })?;
    assert_eq!(pin_events.len(), 1, "unchanged re-submit must not re-emit");
    Ok(())
}

/// `resolve: "always"` keeps the mutable reference for staging-time
/// resolution.
#[tokio::test]
async fn resolve_always_passes_reference_through() -> R<()> {
    let store = setup_store()?;
    let manager = ManagerDaemonService::new(store.clone());
    insert_node(&store, "node-a")?;

    let jobspec = json!({
        "name": "resolve-always-job",
        "tasks": [{
            "name": "repo",
            "runtime": "exec",
            "artifacts": [{
                "source": "git::https://example.invalid/repo?ref=main",
                "resolve": "always",
            }],
            "config": {"command": ["true"]},
        }],
    })
    .to_string();
    // example.invalid is unreachable; with resolve: always no submit-time
    // resolution happens, so admission succeeds.
    manager
        .run_job(Request::new(run_request(jobspec, "resolve-always-job")))
        .await
        .map_err(|status| anyhow::anyhow!("run_job failed: {status}"))?;

    let job_config_id = store.get_job_config("resolve-always-job")?.id();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let artifact = &tasks[0].1.artifacts[0];
    assert!(artifact.source.contains("ref=main"), "{}", artifact.source);
    assert!(artifact.pinned_from.is_none());
    Ok(())
}

/// Worker restart: an allocation whose artifacts staged successfully is
/// re-launched (same id, same task dir) without refetching. The HTTP fixture
/// serves exactly one request, so any refetch attempt would fail the test.
#[test]
fn worker_restart_reuses_staged_artifacts() -> R<()> {
    let (base_url, server) = serve_archive(tool_archive(), 1);
    let temp_dir = TempDir::new()?;

    let allocation = task_allocation(
        66,
        json!({"command": ["sh", "${artifact.tool}/tool/run.sh"]}),
        json!([{"source": format!("{base_url}/tool.tar.gz"), "name": "tool"}]),
    );

    let task_dir = temp_dir.path().join("allocs").join("66").join("tool-task");
    let marker = task_dir.join("marker");
    {
        let manager = worker_manager(&temp_dir);
        launch_task_allocation(&manager, &allocation)?;
        assert!(
            wait_until(Duration::from_secs(15), || marker_written(&marker)),
            "first launch failed: {:?}",
            manager.lock().unwrap().allocation_health_snapshot()
        );
        // Worker "dies": the manager instance is dropped without stopping the
        // allocation, leaving the staged task dir (and its manifest) behind.
    }
    server.join().ok();
    assert!(
        task_dir.join(".staged.json").is_file(),
        "staging manifest persisted across restart"
    );

    // Fresh worker process over the same state dir. The script's process has
    // exited, so restore skips it and the manager re-dispatches the same
    // allocation. The fixture is gone — only reuse can succeed.
    std::fs::remove_file(&marker)?;
    let restarted = worker_manager(&temp_dir);
    launch_task_allocation(&restarted, &allocation)?;
    assert!(
        wait_until(Duration::from_secs(15), || marker_written(&marker)),
        "relaunch after restart failed (artifacts should be reused): {:?}",
        restarted.lock().unwrap().allocation_health_snapshot()
    );

    let guard = restarted.lock().unwrap();
    let health = guard.allocation_health_snapshot();
    let entry = health
        .iter()
        .find(|h| h.allocation_id == 66)
        .expect("health");
    let states = entry.artifact_states.as_deref().expect("artifact states");
    assert!(states.contains("\"state\":\"staged\""), "{states}");
    Ok(())
}

/// In-place re-dispatch (same allocation id, changed task config but the
/// same artifact list) stops the old process and reuses the staged
/// artifacts instead of refetching.
#[test]
fn redispatch_with_unchanged_artifacts_reuses_staging() -> R<()> {
    let (base_url, server) = serve_archive(tool_archive(), 1);
    let temp_dir = TempDir::new()?;
    let manager = worker_manager(&temp_dir);

    let artifacts = json!([{"source": format!("{base_url}/tool.tar.gz"), "name": "tool"}]);
    let allocation = task_allocation(
        77,
        json!({"command": ["sh", "${artifact.tool}/tool/run.sh"]}),
        artifacts.clone(),
    );
    let task_dir = temp_dir.path().join("allocs").join("77").join("tool-task");
    let marker = task_dir.join("marker");

    launch_task_allocation(&manager, &allocation)?;
    assert!(
        wait_until(Duration::from_secs(15), || marker_written(&marker)),
        "first launch failed"
    );
    server.join().ok();

    // Re-dispatch with a changed env (config update) but identical artifacts;
    // the single-shot fixture is gone, so success proves reuse.
    std::fs::remove_file(&marker)?;
    let updated = task_allocation(
        77,
        json!({
            "command": ["sh", "${artifact.tool}/tool/run.sh"],
            "env": {"V": "2"},
        }),
        artifacts,
    );
    launch_task_allocation(&manager, &updated)?;
    assert!(
        wait_until(Duration::from_secs(15), || marker_written(&marker)),
        "re-dispatch failed (artifacts should be reused): {:?}",
        manager.lock().unwrap().allocation_health_snapshot()
    );
    Ok(())
}

/// Worker-side parse guard: a `TaskAllocation` without `task_artifacts`
/// behaves exactly as before the artifact system existed.
#[test]
fn allocations_without_artifacts_skip_staging_entirely() -> R<()> {
    let temp_dir = TempDir::new()?;
    let manager = worker_manager(&temp_dir);

    let allocation = task_allocation(55, json!({"command": ["true"]}), json!([]));
    let mut plain = allocation;
    plain.task_artifacts = String::new();
    launch_task_allocation(&manager, &plain)?;

    let guard = manager.lock().unwrap();
    assert!(guard.is_allocation_active(55));
    let health = guard.allocation_health_snapshot();
    let entry = health
        .iter()
        .find(|h| h.allocation_id == 55)
        .expect("health");
    // The no-checks health monitor may already have marked it healthy; the
    // load-bearing assertion is that it never entered the staging state and
    // carries no artifact rows.
    assert_ne!(entry.status, "staging artifacts");
    assert!(
        entry.artifact_states.is_none(),
        "no artifact rows for plain tasks"
    );
    Ok(())
}
