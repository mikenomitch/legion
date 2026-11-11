//! The language images through the whole stack (plan §15, Phase 1): a
//! manager's `RunSession` → a real worker → its isolate supervisor → a real
//! `legion-isolate` host running the built `python` / `js` images.
//!
//! The images are not checked in. These tests are `#[ignore]`d and read
//! `LEGION_ISOLATE_IMAGES`, else `target/isolate-images` (what
//! `crates/isolate/images/build.sh` writes). Run them with
//! `cargo test --test integration isolate_images:: -- --ignored --nocapture`;
//! set `LEGION_ISOLATE_BINARY` to a release `legion-isolate` (compiling the
//! 30 MB Python image with a debug build takes minutes).
//!
//! The VM task here is an `exec` session task standing in for a sandbox
//! (see the VM-tier section of `isolate.rs` for why), stored straight into
//! the store.

use crate::isolate::cluster::{isolate_task, main_py, Cluster, Images, JOB};
use legion::prelude::*;
use legion::structs::{RunFile, RunSessionRequest, RunSessionResponse};
use legion::structs::manager_server::Manager as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const NEEDS_IMAGES: &str = "needs built images: crates/isolate/images/build.sh";

fn images_dir() -> PathBuf {
    std::env::var_os("LEGION_ISOLATE_IMAGES")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/isolate-images"))
}

fn built_images() -> Images {
    let dir = images_dir();
    assert!(
        dir.join("manifest.json").is_file(),
        "{NEEDS_IMAGES} (no manifest.json in {})",
        dir.display()
    );
    Images::Built(dir)
}

fn python_task(name: &str, lifetime: &str, vm: bool) -> Value {
    let mut config = json!({
        "image": "python",
        "lifetime": lifetime,
        "limits": { "cpu": "500ms", "wall": "10s" },
    });
    if vm {
        config["vm"] = json!({ "task": "machine" });
    }
    let mut task = isolate_task(config);
    task["name"] = json!(name);
    if vm {
        task["children"] = json!({ "tasks": ["machine"] });
    }
    task
}

fn js_task() -> Value {
    let mut task = isolate_task(json!({ "image": "js", "lifetime": "request" }));
    task["name"] = json!("js");
    task
}

/// An `exec` session task standing in for the sandbox VM task.
fn machine() -> Value {
    json!({
        "name": "machine",
        "runtime": "exec",
        "config": { "command": ["sleep", "600"] },
        "resources": { "cpu": 100, "memory": 64 },
        "stateful": {
            "mode": "session",
            "envelope": { "resources": { "cpu": 400, "memory": 256 } },
        },
        "run": { "command": ["sh", "-c", "cat main.py", "sh"], "wall": "30s" },
    })
}

async fn run(cluster: &Cluster, task: &str, file: RunFile) -> RunSessionResponse {
    cluster
        .run(RunSessionRequest {
            task_name: task.into(),
            files: vec![file],
            ..Default::default()
        })
        .await
        .unwrap_or_else(|status| panic!("run in {task}: {status:?}"))
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The images' run semantics, through the manager.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn the_language_images_run_through_the_manager() -> R<()> {
    let cluster = Cluster::start(
        built_images(),
        json!([
            python_task("run", "request", true),
            python_task("bare", "request", false),
            js_task(),
            machine(),
        ]),
    )
    .await?;
    for task in ["run", "bare", "js"] {
        cluster.wait_for(task, true).await?;
    }
    cluster.wait_for("machine", false).await?;

    // Python: stdout, and a top-level `result`.
    let started = Instant::now();
    let reply = run(
        &cluster,
        "run",
        main_py("import json\nprint('hello')\nresult = {'n': 2, 'ok': True}\n"),
    )
    .await;
    println!("first python run (compile included): {:?}", started.elapsed());
    assert_eq!(reply.exit, Some(0), "{reply:?} {}", text(&reply.stderr));
    assert_eq!(text(&reply.stdout), "hello\n");
    assert_eq!(serde_json::from_str::<Value>(&reply.result)?, json!({ "n": 2, "ok": true }));
    assert_eq!(reply.tier, "isolate");

    // sys.exit(3)
    let reply = run(&cluster, "run", main_py("import sys\nsys.exit(3)\n")).await;
    assert_eq!(reply.exit, Some(3), "{reply:?}");

    // JavaScript: console.log and the returned value.
    let reply = run(
        &cluster,
        "js",
        RunFile {
            path: "main.js".into(),
            content: b"console.log('hi from js');\nreturn { n: 1 + 1 };\n".to_vec(),
            executable: false,
        },
    )
    .await;
    assert_eq!(reply.exit, Some(0), "{reply:?} {}", text(&reply.stderr));
    assert_eq!(text(&reply.stdout), "hi from js\n");
    assert_eq!(serde_json::from_str::<Value>(&reply.result)?, json!({ "n": 2 }));

    // A CPU-bound loop is stopped by the CPU limit.
    let reply = run(&cluster, "run", main_py("while True:\n    pass\n")).await;
    let stopped = reply.stopped.clone().expect("stopped");
    assert_eq!((stopped.reason.as_str(), stopped.limit.as_str()), ("limits", "cpu"));
    assert_eq!(reply.next_tier, "vm", "the task has a VM task");

    // subprocess.run is unsupported, with the documented detail; next_tier
    // only where there is a VM task.
    let source = "import subprocess\nsubprocess.run(['ls'])\n";
    let reply = run(&cluster, "run", main_py(source)).await;
    let stopped = reply.stopped.clone().expect("stopped");
    assert_eq!(stopped.reason, "unsupported", "{reply:?}");
    assert!(
        stopped
            .detail
            .starts_with("subprocess.run: processes are not available in an isolate"),
        "{stopped:?}"
    );
    assert_eq!(reply.next_tier, "vm");
    let reply = run(&cluster, "bare", main_py(source)).await;
    assert_eq!(reply.stopped.expect("stopped").reason, "unsupported");
    assert_eq!(reply.next_tier, "", "no VM task, no next tier");

    // The dry run's import scan.
    for (task, tier) in [("run", "vm"), ("bare", "isolate")] {
        let reply = cluster
            .run(RunSessionRequest {
                task_name: task.into(),
                dry_run: true,
                files: vec![main_py("import json\nimport torch\n")],
                ..Default::default()
            })
            .await?;
        let dry = reply.dry_run.expect("dry run");
        assert_eq!(dry.missing_imports, vec!["torch".to_string()], "{dry:?}");
        assert!(dry.imports.contains(&"json".to_string()), "{dry:?}");
        assert_eq!(dry.tier, tier, "{task}: {dry:?}");
    }
    Ok(())
}

/// A `lifetime = "session"` Python isolate keeps a global across runs and
/// loses it across hibernation, while `legion.state` survives and the
/// isolate says it `resumed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn a_python_session_isolate_keeps_state_across_hibernation() -> R<()> {
    let cluster = Cluster::start(built_images(), json!([python_task("run", "session", false)]))
        .await?;
    cluster.wait_for("run", true).await?;
    let started = cluster
        .svc
        .create_instance(tonic::Request::new(legion::structs::CreateInstanceRequest {
            job_id: JOB.into(),
            task: Some("run".into()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    let id = started.instance_id;
    let source = "import builtins, legion\n\
                  builtins.n = getattr(builtins, 'n', 0) + 1\n\
                  if legion.state.get('k') is None:\n    legion.state.set('k', 'kept')\n\
                  info = legion.info()\n\
                  resumed = info['resumed'] if isinstance(info, dict) else info.resumed\n\
                  result = {'n': builtins.n, 'k': legion.state.get('k').decode(), \
                  'resumed': resumed}\n";
    let session_run = || RunSessionRequest {
        task_name: "run".into(),
        session_id: id.clone(),
        files: vec![main_py(source)],
        ..Default::default()
    };
    let mut results = Vec::new();
    for _ in 0..2 {
        let reply = cluster.run(session_run()).await?;
        assert_eq!(reply.exit, Some(0), "{reply:?} {}", text(&reply.stderr));
        results.push(serde_json::from_str::<Value>(&reply.result)?);
    }
    cluster.supervisor.hibernate(&id).await?;
    let reply = cluster.run(session_run()).await?;
    assert_eq!(reply.isolate_start, "hibernated", "{reply:?}");
    results.push(serde_json::from_str::<Value>(&reply.result)?);
    assert_eq!(
        results,
        vec![
            json!({ "n": 1, "k": "kept", "resumed": false }),
            json!({ "n": 2, "k": "kept", "resumed": false }),
            json!({ "n": 1, "k": "kept", "resumed": true }),
        ]
    );
    assert!(cluster.supervisor.stop_session(&id, "test").await);
    Ok(())
}

fn p50(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

async fn hello_p50(cluster: &Cluster, file: RunFile, n: usize) -> (Duration, Duration) {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let started = Instant::now();
        let reply = run(cluster, "run", file.clone()).await;
        samples.push(started.elapsed());
        assert_eq!(reply.exit, Some(0), "{reply:?}");
    }
    let min = *samples.iter().min().unwrap();
    (p50(samples), min)
}

/// Plan §15 Phase 1 asks `legion session run` on a component task to take
/// under 2 ms end to end on one node. Measured here from the manager's
/// `RunSession` (no CLI, no network hop beyond loopback mTLS), after a
/// warm-up run that compiles the module: a container measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs built images: crates/isolate/images/build.sh"]
async fn hello_world_end_to_end_timing() -> R<()> {
    const RUNS: usize = 30;
    let python = Cluster::start(built_images(), json!([python_task("run", "request", false)]))
        .await?;
    python.wait_for("run", true).await?;
    let hello = main_py("print('hello')\n");
    run(&python, "run", hello.clone()).await;
    let (p50_py, min_py) = hello_p50(&python, hello, RUNS).await;
    drop(python);

    let component = Cluster::start(
        Images::Fixture("cmd.wasm"),
        json!([isolate_task(json!({ "image": "python", "lifetime": "request" }))]),
    )
    .await?;
    component.wait_for("run", true).await?;
    let file = main_py("");
    run(&component, "run", file.clone()).await;
    let (p50_cmd, min_cmd) = hello_p50(&component, file, RUNS).await;
    println!(
        "[container measurement] RunSession through the manager, {RUNS} runs after a warm-up: \
         python hello p50 {p50_py:?} (min {min_py:?}); Rust cmd component p50 {p50_cmd:?} \
         (min {min_cmd:?})"
    );
    Ok(())
}
