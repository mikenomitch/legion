//! Scaling plugins and standalone policies end to end (docs/autoscaling.md,
//! docs/autoscaling-plugins.md; autoscaler-v2 plan §9, Phases 3 and 4).
//!
//! - The conformance suite (`legion scaling plugin test`) passes the example
//!   plugins whose interpreter is here (python3 always on CI; Go when a `go`
//!   toolchain is installed, skipped otherwise), and fails broken fixtures
//!   for the right reason: a timeout, a malformed reply, a crash.
//! - The built-in `prometheus` and `http` sources against tiny in-test HTTP
//!   servers: no real Prometheus.
//! - A Python strategy installed through the autoscaler's app config and
//!   used by a task's policy (acceptance P4); a policy in a namespace
//!   outside its allowlist is refused at apply.
//! - A standalone policy on the `exec` target, scaled by an `http` source,
//!   paused, and deleted.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion::apps::autoscaler::conformance::{self, Outcome, SuiteOptions};
use legion::apps::autoscaler::host::{PluginHost, SourceCall, SourceReading};
use legion::apps::autoscaler::rpc::Endpoint;
use legion::apps::autoscaler::EngineOptions;
use legion::autoscaler::plugins::PluginCatalog;
use legion::prelude::{json, Value, R};
use legion::structs::{
    ApplyScalingPolicyRequest, DeleteScalingPolicyRequest, GetScaleHistoryRequest,
    GetScalingStatusRequest, ListScalingPoliciesRequest, PauseScalingRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::autoscaler::{Harness, T0, TICK_MS};

fn examples() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/scaling-plugins")
}

fn have(program: &str) -> bool {
    std::process::Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success())
        || std::process::Command::new(program)
            .arg("version")
            .output()
            .is_ok_and(|out| out.status.success())
}

fn python_example() -> Option<Vec<String>> {
    if !have("python3") {
        eprintln!("python3 is not installed: skipping the Python example plugin");
        return None;
    }
    Some(vec![
        "python3".to_string(),
        examples()
            .join("python/calendar_strategy.py")
            .display()
            .to_string(),
    ])
}

fn failures(report: &conformance::SuiteReport) -> Vec<String> {
    report
        .checks
        .iter()
        .filter(|check| check.outcome == Outcome::Fail)
        .map(|check| format!("{}: {}", check.name, check.detail))
        .collect()
}

/// Acceptance P4, first half: the Python strategy from `examples/` passes
/// the conformance suite.
#[tokio::test(flavor = "multi_thread")]
async fn the_python_example_passes_the_conformance_suite_test() -> R<()> {
    let Some(argv) = python_example() else {
        return Ok(());
    };
    let report = conformance::run(
        Endpoint::Command { argv, dir: None },
        SuiteOptions {
            config: json!({ "windows": [{ "start": "02:00", "end": "03:30" }] }),
            policy_config: json!({ "replicas": 6, "lead": "15m" }),
            ..Default::default()
        },
    )
    .await;
    assert!(report.passed, "{:#?}", report.checks);
    let plugin = report.plugin.as_ref().expect("handshake");
    assert_eq!(plugin.kinds, vec!["strategy".to_string()]);
    let outcome = |name: &str| {
        report
            .checks
            .iter()
            .find(|check| check.name == name)
            .map(|check| check.outcome)
    };
    assert_eq!(outcome("strategy/recommend"), Some(Outcome::Pass));
    assert_eq!(outcome("unknown method"), Some(Outcome::Pass));
    assert_eq!(outcome("malformed request"), Some(Outcome::Pass));
    assert_eq!(outcome("crash and restart"), Some(Outcome::Pass));
    assert_eq!(outcome("schema"), Some(Outcome::Pass));
    assert_eq!(outcome("target/status"), None);

    // A policy config its schema refuses is caught before the plugin sees it.
    let refused = conformance::run(
        Endpoint::Command {
            argv: python_example().expect("checked above"),
            dir: None,
        },
        SuiteOptions {
            policy_config: json!({ "replicas": "six" }),
            ..Default::default()
        },
    )
    .await;
    assert!(!refused.passed);
    assert!(
        failures(&refused)
            .iter()
            .any(|failure| failure.starts_with("schema: ")),
        "{:#?}",
        refused.checks
    );
    Ok(())
}

/// The Go example (a source and a target) passes too, when a Go toolchain
/// is here to build it. CI needs no network: it uses the standard library
/// only.
#[tokio::test(flavor = "multi_thread")]
async fn the_go_example_passes_the_conformance_suite_test() -> R<()> {
    if !have("go") {
        eprintln!("go is not installed: skipping the Go example plugin");
        return Ok(());
    }
    let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
    let binary = dir.path().join("spool");
    let built = std::process::Command::new("go")
        .args(["build", "-o"])
        .arg(&binary)
        .arg("./spool")
        .current_dir(examples().join("go"))
        .env("GOTOOLCHAIN", "local")
        .env("GOFLAGS", "-mod=mod")
        .env("GOCACHE", dir.path().join("gocache"))
        .output()?;
    assert!(
        built.status.success(),
        "go build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    let spool = dir.path().join("spool-dir");
    std::fs::create_dir_all(spool.join("queues/render"))?;
    std::fs::write(spool.join("queues/render/job-1"), "")?;
    let report = conformance::run(
        Endpoint::Command {
            argv: vec![binary.display().to_string()],
            dir: None,
        },
        SuiteOptions {
            config: json!({ "dir": spool }),
            policy_config: json!({ "label": "render" }),
            query: "render".to_string(),
            ..Default::default()
        },
    )
    .await;
    assert!(report.passed, "{:#?}", report.checks);
    let plugin = report.plugin.as_ref().expect("handshake");
    assert_eq!(
        plugin.kinds,
        vec!["source".to_string(), "target".to_string()]
    );
    Ok(())
}

/// The Rust SDK's example passes too. `cargo test --all` builds the SDK
/// crate's examples into the shared target directory before any test runs;
/// a narrower run that did not build it skips.
#[tokio::test(flavor = "multi_thread")]
async fn the_rust_sdk_example_passes_the_conformance_suite_test() -> R<()> {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("target"));
    let binary = target.join("debug/examples/queue-ratio");
    if !binary.exists() {
        eprintln!(
            "{} is not built (cargo test --all builds it): skipping the Rust example plugin",
            binary.display()
        );
        return Ok(());
    }
    let report = conformance::run(
        Endpoint::Command {
            argv: vec![binary.display().to_string()],
            dir: None,
        },
        SuiteOptions {
            policy_config: json!({ "rate": 5, "drainWithin": 600 }),
            ..Default::default()
        },
    )
    .await;
    assert!(report.passed, "{:#?}", report.checks);
    let plugin = report.plugin.as_ref().expect("handshake");
    assert_eq!(plugin.kinds, vec!["strategy".to_string()]);
    assert!(plugin.has_schema);
    Ok(())
}

/// The first-party `aws-asg` plugin passes the conformance suite as a node
/// target against a mock of the AWS API (no AWS involved). It is its own
/// crate (`plugins/scaling/aws-asg/`), built by `LEGION_BUILD_PLUGINS=1
/// cargo build` or its CI job; a run that did not build it skips.
#[tokio::test(flavor = "multi_thread")]
async fn the_aws_asg_plugin_passes_the_conformance_suite_test() -> R<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Some(binary) = ["release", "debug"]
        .iter()
        .map(|profile| {
            root.join("target/plugin-build/scaling/aws-asg")
                .join(profile)
                .join("aws-asg")
        })
        .find(|binary| binary.exists())
    else {
        eprintln!("the aws-asg plugin is not built (LEGION_BUILD_PLUGINS=1 cargo build): skipping");
        return Ok(());
    };
    let groups = std::fs::read_to_string(
        root.join("plugins/scaling/aws-asg/fixtures/describe-auto-scaling-groups.xml"),
    )?;
    // A mock AWS: DescribeAutoScalingGroups answers the recorded group;
    // every other call (SetDesiredCapacity, Terminate…) succeeds.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let actions = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = actions.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let groups = groups.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 8192];
                // Read the headers, then the body by its content-length.
                loop {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&request).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text[..end]
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&request).to_string();
                let action = text
                    .split_once("\r\n\r\n")
                    .map(|(_, body)| body)
                    .unwrap_or_default()
                    .split('&')
                    .find_map(|part| part.strip_prefix("Action="))
                    .unwrap_or("?")
                    .to_string();
                seen.lock().expect("lock").push(action.clone());
                let body = if action == "DescribeAutoScalingGroups" {
                    groups
                } else {
                    "<Response><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></Response>"
                        .to_string()
                };
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(reply.as_bytes()).await;
            });
        }
    });
    let report = conformance::run(
        Endpoint::Command {
            argv: vec![
                "env".into(),
                "AWS_ACCESS_KEY_ID=AKIDEXAMPLE".into(),
                "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
                binary.display().to_string(),
            ],
            dir: None,
        },
        SuiteOptions {
            config: json!({ "region": "us-east-1", "endpoint": endpoint, "imds": false }),
            policy_config: json!({ "asg": "legion-workers" }),
            ..Default::default()
        },
    )
    .await;
    server.abort();
    assert!(report.passed, "{:#?}", report.checks);
    let plugin = report.plugin.as_ref().expect("handshake");
    assert_eq!(plugin.kinds, vec!["target".to_string()]);
    assert!(plugin.nodes && plugin.has_schema);
    let actions = actions.lock().expect("lock").clone();
    assert!(
        actions
            .iter()
            .any(|action| action == "DescribeAutoScalingGroups"),
        "{actions:?}"
    );
    Ok(())
}

/// A plugin that is broken in each documented way fails the check that
/// names it: a timeout, a malformed reply, a crash it does not survive.
#[tokio::test(flavor = "multi_thread")]
async fn broken_plugins_fail_the_check_that_names_the_problem_test() -> R<()> {
    let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
    let fixture = |name: &str, recommend: &str| -> R<Vec<String>> {
        let path = dir.path().join(name);
        std::fs::write(
            &path,
            format!(
                r#"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"initialize"'*) printf '{{"jsonrpc":"2.0","id":%s,"result":{{"name":"{name}","version":"1","kinds":["strategy"]}}}}\n' "$id" ;;
    *'"strategy/recommend"'*) {recommend} ;;
    *) printf '{{"jsonrpc":"2.0","id":%s,"error":{{"code":-32601,"message":"method not found"}}}}\n' "$id" ;;
  esac
done
"#
            ),
        )?;
        Ok(vec!["sh".to_string(), path.display().to_string()])
    };
    let run = |argv: Vec<String>| {
        conformance::run(
            Endpoint::Command { argv, dir: None },
            SuiteOptions {
                timeout: Some(Duration::from_millis(800)),
                ..Default::default()
            },
        )
    };

    let slow = run(fixture("slow", "sleep 3")?).await;
    assert!(!slow.passed);
    assert!(
        failures(&slow)
            .iter()
            .any(|f| f.starts_with("strategy/recommend: strategy/recommend timed out")),
        "{:#?}",
        slow.checks
    );
    assert!(failures(&slow).iter().any(|f| f.starts_with("timeouts: ")));

    let garbage = run(fixture("garbage", "echo 'desired: lots'")?).await;
    assert!(!garbage.passed);
    assert!(
        failures(&garbage)
            .iter()
            .any(|f| f.contains("malformed reply")),
        "{:#?}",
        garbage.checks
    );

    let shape = run(fixture(
        "shape",
        r#"printf '{"jsonrpc":"2.0","id":%s,"result":{"want":5}}\n' "$id""#,
    )?)
    .await;
    assert!(
        failures(&shape)
            .iter()
            .any(|f| f.contains("needs `desired`")),
        "{:#?}",
        shape.checks
    );

    let crash = run(fixture("crash", "exit 1")?).await;
    assert!(!crash.passed);
    assert!(
        failures(&crash)
            .iter()
            .any(|f| f.contains("the plugin exited")),
        "{:#?}",
        crash.checks
    );

    let missing = run(vec!["/nonexistent/plugin".to_string()]).await;
    assert!(!missing.passed);
    assert!(failures(&missing)[0].starts_with("handshake: cannot start"));
    Ok(())
}

/// A tiny HTTP server: answers every GET with `respond(path_and_query)`,
/// and records the requests.
pub(crate) struct FakeHttp {
    pub url: String,
    pub requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeHttp {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeHttp {
    pub(crate) async fn start(respond: impl Fn(&str) -> Value + Send + Sync + 'static) -> R<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let respond = Arc::new(respond);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let respond = respond.clone();
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 * 1024];
                    let mut read = 0;
                    loop {
                        match stream.read(&mut buf[read..]).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => read += n,
                        }
                        if buf[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let head = String::from_utf8_lossy(&buf[..read]).to_string();
                    let target = head
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_string();
                    seen.lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .push(target.clone());
                    let body = respond(&target).to_string();
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(reply.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Ok(Self {
            url,
            requests,
            task,
        })
    }

    pub(crate) fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// The `prometheus` source reads an instant query, and a range over the
/// check's window; the `http` source reads a JSON pointer.
#[tokio::test(flavor = "multi_thread")]
async fn builtin_sources_read_prometheus_and_http_test() -> R<()> {
    let prometheus = FakeHttp::start(|target| {
        if target.starts_with("/api/v1/query_range") {
            json!({ "status": "success", "data": { "resultType": "matrix",
                "result": [{ "metric": {}, "values": [[1, "100"], [2, "300"]] }] } })
        } else if target.contains("query=up") {
            json!({ "status": "success", "data": { "resultType": "vector",
                "result": [{ "value": [1, "1"] }, { "value": [1, "1"] }] } })
        } else {
            json!({ "status": "success", "data": { "resultType": "vector",
                "result": [{ "metric": {}, "value": [1, "2310"] }] } })
        }
    })
    .await?;
    let stats = FakeHttp::start(|_| json!({ "queue": { "depth": 42 } })).await?;
    let catalog = PluginCatalog::from_app_config(&json!({ "plugins": {
        "team-prom": { "builtin": "prometheus", "config": { "address": prometheus.url } },
        "stats": { "builtin": "http", "config": { "url": format!("{}/stats", stats.url) } },
    } }))
    .map_err(|err| anyhow::anyhow!(err))?;
    let host = PluginHost::new(catalog, None);
    let call = |query: &'static str, window: u64| SourceCall {
        policy: "web/default/web",
        check: "lag",
        query,
        window: Duration::from_secs(window),
    };
    let now = T0;
    assert_eq!(
        host.query("team-prom", &call("sum(lag)", 0), now).await,
        Ok(SourceReading::value(2310.0, true))
    );
    assert_eq!(
        host.query("team-prom", &call("sum(lag)", 120), now).await,
        Ok(SourceReading::value(200.0, true))
    );
    let requests = prometheus.requests();
    assert!(requests[0].starts_with("/api/v1/query?"), "{requests:?}");
    assert!(
        requests[1].starts_with("/api/v1/query_range?"),
        "{requests:?}"
    );
    assert!(requests[1].contains("step=10s"), "{requests:?}");
    let err = host
        .query("team-prom", &call("up", 0), now)
        .await
        .unwrap_err();
    assert!(err.contains("returned 2 series"), "{err}");
    assert_eq!(
        host.query("stats", &call("/queue/depth", 60), now).await,
        Ok(SourceReading::value(42.0, false))
    );
    let err = host
        .query("stats", &call("/queue/size", 60), now)
        .await
        .unwrap_err();
    assert!(err.contains("nothing at /queue/size"), "{err}");
    Ok(())
}

fn calendar_plugins() -> Option<Value> {
    let argv = python_example()?;
    Some(json!({ "plugins": {
        "calendar": {
            "command": argv,
            // A window that ends where it starts lasts all day, so the test
            // does not depend on the time it runs.
            "config": { "windows": [{ "start": "00:00", "end": "00:00" }] },
            "namespaces": ["default"],
        },
    } }))
}

/// Acceptance P4, second half: the Python strategy, installed through the
/// autoscaler's app config, drives a task's policy; a policy in a namespace
/// outside its allowlist is refused at apply.
#[tokio::test(flavor = "multi_thread")]
async fn a_python_strategy_installed_in_app_config_drives_a_policy_test() -> R<()> {
    let Some(config) = calendar_plugins() else {
        return Ok(());
    };
    let mut h = Harness::start(2).await?;
    h.install_plugins(config.clone())?;
    let spec = json!({
        "name": "batch",
        "task": {
            "name": "batch", "runtime": "exec", "count": 2,
            "config": { "command": ["sleep", "3600"] },
            "resources": { "cpu": 500, "memory": 64 },
            "autoscale": { "max": 8, "checks": [{
                "name": "window", "source": "legion",
                "strategy": { "name": "calendar", "replicas": 4 },
                "scaleIn": false,
            }] },
        }
    });
    h.run(spec.clone()).await?;
    let options = EngineOptions {
        plugins: PluginCatalog::from_app_config(&config).map_err(|err| anyhow::anyhow!(err))?,
        ..Default::default()
    };
    let (mut engine, watch) = h.engine(options).await?;
    let report = engine.tick(T0).await;
    watch.abort();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    h.drain_evals()?;
    assert_eq!(h.count("batch")?, 4);
    let history = h.history("batch").await?;
    assert!(
        history[0].summary.contains("batch window"),
        "{:?}",
        history[0]
    );

    // The same plugin from a namespace its allowlist does not name.
    h.store
        .apply_namespace(legion::structs::namespace::Namespace {
            name: "payments".into(),
            ..Default::default()
        })?;
    let mut other = spec.clone();
    other["namespace"] = json!("payments");
    let err = h.run(other).await.unwrap_err().to_string();
    assert!(
        err.contains("check 'window' uses strategy 'calendar', which is not available to namespace payments (allowed: default)"),
        "{err}"
    );
    // A plugin nobody installed.
    let mut unknown = spec;
    unknown["name"] = json!("batch-2");
    unknown["task"]["autoscale"]["checks"][0]["source"] = json!("team-prom");
    unknown["task"]["autoscale"]["checks"][0]["query"] = json!("sum(lag)");
    let err = h.run(unknown).await.unwrap_err().to_string();
    assert!(
        err.contains("uses source 'team-prom', which is not installed"),
        "{err}"
    );
    Ok(())
}

/// A standalone policy on the `exec` target, scaled by the `http` source:
/// applied (and diffed), evaluated, recorded in history, paused, and
/// deleted.
#[tokio::test(flavor = "multi_thread")]
async fn a_standalone_policy_scales_an_exec_target_test() -> R<()> {
    let depth = Arc::new(Mutex::new(25.0));
    let served = depth.clone();
    let queue = FakeHttp::start(move |_| json!({ "depth": *served.lock().unwrap() })).await?;
    let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
    let script = dir.path().join("render.sh");
    let state = dir.path().join("count");
    std::fs::write(&state, "1")?;
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nread -r request\ncase \"$1\" in\n  status) echo \"{{\\\"count\\\": $(cat {state})}}\" ;;\n  scale) printf '%s' \"$request\" | sed -n 's/.*\"desired\":\\([0-9]*\\).*/\\1/p' > {state}; echo '{{}}' ;;\nesac\n",
            state = state.display()
        ),
    )?;
    let config = json!({ "plugins": {
        "queue": { "builtin": "http", "config": { "url": format!("{}/depth", queue.url) } },
        "render": { "builtin": "exec", "config": { "command": ["sh", script.display().to_string()] } },
    } });
    let mut h = Harness::start(1).await?;
    h.install_plugins(config.clone())?;
    let policy = json!({
        "name": "render-farm",
        "description": "GPU render workers",
        "target": { "plugin": "render", "config": { "pool": "gpu" } },
        "min": 1, "max": 10,
        "checks": [{ "name": "queue", "source": "queue", "query": "/depth",
                     "strategy": { "name": "per-replica", "value": 5 } }],
    });
    let apply = |spec: &Value, dry_run: bool| ApplyScalingPolicyRequest {
        spec_json: spec.to_string(),
        namespace: String::new(),
        dry_run,
        idempotency_key: None,
    };
    let dry = h
        .client
        .apply_scaling_policy(apply(&policy, true))
        .await?
        .into_inner();
    assert!(dry.created && dry.dry_run, "{dry:?}");
    assert!(h.store.scaling_policy("default", "render-farm")?.is_none());
    let created = h
        .client
        .apply_scaling_policy(apply(&policy, false))
        .await?
        .into_inner();
    assert_eq!(created.id, "render-farm");
    assert!(created.created, "{created:?}");
    let again = h
        .client
        .apply_scaling_policy(apply(&policy, false))
        .await?
        .into_inner();
    assert!(
        !again.changed && again.version == created.version,
        "{again:?}"
    );
    let mut wider = policy.clone();
    wider["max"] = json!(20);
    let diff = h
        .client
        .apply_scaling_policy(apply(&wider, true))
        .await?
        .into_inner();
    assert_eq!(diff.diff, vec!["max: 10 -> 20".to_string()], "{diff:?}");

    let listed = h
        .client
        .list_scaling_policies(ListScalingPoliciesRequest::default())
        .await?
        .into_inner();
    let entry = listed
        .policies
        .iter()
        .find(|policy| policy.id == "render-farm")
        .expect("listed");
    assert_eq!(
        (entry.kind.as_str(), entry.min, entry.max),
        ("external", 1, 10)
    );
    assert_eq!(entry.target, "plugin render");

    let options = EngineOptions {
        plugins: PluginCatalog::from_app_config(&config).map_err(|err| anyhow::anyhow!(err))?,
        ..Default::default()
    };
    let (mut engine, watch) = h.engine(options).await?;
    // 25 queued at 5 per worker: 5 workers (1 → 5, inside the +4 step).
    let report = engine.tick(T0).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(std::fs::read_to_string(&state)?.trim(), "5");
    let history = h
        .client
        .get_scale_history(GetScaleHistoryRequest {
            policy: "render-farm".into(),
            limit: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!((history.entries[0].from, history.entries[0].to), (1, 5));
    assert_eq!(history.entries[0].policy, "render-farm");
    assert_eq!(history.entries[0].kind, "scaled");

    // Paused: the server refuses the scale, nothing moves.
    h.client
        .pause_scaling(PauseScalingRequest {
            policy: "render-farm".into(),
            reason: "maintenance".into(),
            ..Default::default()
        })
        .await?;
    *depth.lock().unwrap() = 50.0;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let report = engine.tick(T0 + TICK_MS).await;
        if report.states.get("render-farm").map(String::as_str) == Some("paused") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{report:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(std::fs::read_to_string(&state)?.trim(), "5");
    let status = h
        .client
        .get_scaling_status(GetScalingStatusRequest {
            policy: "render-farm".into(),
            detail: true,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(status.targets[0].state, "paused");
    assert_eq!(status.targets[0].kind, "external");
    watch.abort();

    let deleted = h
        .client
        .delete_scaling_policy(DeleteScalingPolicyRequest {
            name: "render-farm".into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(deleted.message.contains("deleted"), "{deleted:?}");
    assert!(h.store.scaling_policy("default", "render-farm")?.is_none());
    Ok(())
}
