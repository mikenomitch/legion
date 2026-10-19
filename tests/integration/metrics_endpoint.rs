//! Integration tests for the telemetry listener (/metrics, /healthz,
//! /readyz) against an in-process store.
//!
//! The listener is bound on an ephemeral port (bind `127.0.0.1:0`, read back
//! `local_addr`) and driven with a hand-rolled HTTP/1.1 client over
//! `tokio::net::TcpStream` — no HTTP client crate needed.

use std::net::SocketAddr;

use legion::manager::telemetry::{self, TelemetryState};
use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use tempfile::Builder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("legion-metrics-endpoint-")
        .tempdir_in(crate::support::scratch_root())
        .context("Error creating temp store directory")?
        .keep();

    let store_config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("Error parsing store path")?
            .to_string(),
        ..Default::default()
    };

    Store::start_storage(&store_config)
}

async fn start_listener(state: TelemetryState) -> R<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = telemetry::serve_telemetry_on(listener, state).await;
    });
    Ok(addr)
}

struct HttpReply {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl HttpReply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

async fn http_get(addr: SocketAddr, path: &str) -> R<HttpReply> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n").as_bytes(),
        )
        .await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let raw = String::from_utf8(raw).context("response was not utf-8")?;

    let (head, body) = raw
        .split_once("\r\n\r\n")
        .context("malformed http response: no header/body separator")?;
    let mut lines = head.lines();
    let status_line = lines.next().context("missing status line")?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .context("missing status code")?
        .parse()?;
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect();
    Ok(HttpReply {
        status,
        headers,
        body: body.to_string(),
    })
}

/// Checks one non-comment exposition line against the grammar we emit:
/// `name{label="value",...} value` with the metric name matching
/// `[a-zA-Z_:][a-zA-Z0-9_:]*`, properly quoted/escaped label values, and a
/// parseable float sample value.
fn assert_exposition_line(line: &str) {
    let (name_and_labels, value) = line
        .rsplit_once(' ')
        .unwrap_or_else(|| panic!("no sample value in line: {line}"));
    assert!(
        value.parse::<f64>().is_ok(),
        "sample value {value:?} is not a float in line: {line}"
    );

    let name = match name_and_labels.split_once('{') {
        None => name_and_labels,
        Some((name, rest)) => {
            let labels = rest
                .strip_suffix('}')
                .unwrap_or_else(|| panic!("unterminated label block in line: {line}"));
            for pair in split_label_pairs(labels) {
                let (label_name, label_value) = pair
                    .split_once('=')
                    .unwrap_or_else(|| panic!("label without '=' in line: {line}"));
                assert!(
                    is_valid_name(label_name),
                    "bad label name {label_name:?} in line: {line}"
                );
                assert!(
                    label_value.len() >= 2
                        && label_value.starts_with('"')
                        && label_value.ends_with('"'),
                    "label value not quoted in line: {line}"
                );
                let inner = &label_value[1..label_value.len() - 1];
                // No raw quotes/newlines may survive inside a value.
                let mut chars = inner.chars();
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => {
                            let next = chars
                                .next()
                                .unwrap_or_else(|| panic!("dangling escape in line: {line}"));
                            assert!(
                                matches!(next, '\\' | '"' | 'n'),
                                "invalid escape \\{next} in line: {line}"
                            );
                        }
                        '"' | '\n' => panic!("unescaped {c:?} in line: {line}"),
                        _ => {}
                    }
                }
            }
            name
        }
    };
    assert!(is_valid_name(name), "bad metric name {name:?} in: {line}");
}

fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
            .unwrap_or(false)
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// Splits a label block on commas that are outside quoted values.
fn split_label_pairs(labels: &str) -> Vec<String> {
    let mut pairs = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for c in labels.chars() {
        if escaped {
            current.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_quotes => {
                current.push(c);
                escaped = true;
            }
            '"' => {
                current.push(c);
                in_quotes = !in_quotes;
            }
            ',' if !in_quotes => {
                pairs.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        pairs.push(current);
    }
    pairs
}

fn parse_sample(body: &str, name_with_labels: &str) -> Option<f64> {
    body.lines()
        .find(|line| line.starts_with(name_with_labels))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, value)| value.parse().ok())
}

#[tokio::test]
async fn healthz_readyz_and_metrics_serve_on_ephemeral_port() -> R<()> {
    let store = setup_store()?;
    let addr = start_listener(TelemetryState {
        store,
        raft_required: false,
        diverged_since_ms: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
    })
    .await?;

    // Liveness: 200 "ok" once serving.
    let healthz = http_get(addr, "/healthz").await?;
    assert_eq!(healthz.status, 200);
    assert_eq!(healthz.body.trim(), "ok");

    // Readiness: the local-disk store answers SELECT 1, no raft required.
    let readyz = http_get(addr, "/readyz").await?;
    assert_eq!(readyz.status, 200, "readyz body: {}", readyz.body);

    // Unknown paths 404, non-GET 405 is covered by the handler; spot-check 404.
    let missing = http_get(addr, "/nope").await?;
    assert_eq!(missing.status, 404);

    // Metrics: content type, grammar of every non-comment line, and a counter
    // that moves when incremented. Counters are process-global and other
    // tests in this binary may also touch them, so assert monotonic growth by
    // at least our own increments rather than exact values.
    let before = http_get(addr, "/metrics").await?;
    assert_eq!(before.status, 200);
    assert_eq!(
        before.header("content-type"),
        Some("text/plain; version=0.0.4")
    );
    let heartbeats_before =
        parse_sample(&before.body, "legion_heartbeats_total ").unwrap_or_default();

    telemetry::metrics().heartbeats_total.inc();
    telemetry::metrics().heartbeats_total.inc();
    telemetry::metrics()
        .evals_processed_total
        .inc(&["job_submit", "ok"]);
    telemetry::metrics()
        .store_write_lock_wait_seconds
        .observe(0.003);
    telemetry::metrics()
        .scheduler_plan_phase_seconds
        .observe(&["build_plan"], 0.004);

    let after = http_get(addr, "/metrics").await?;
    assert_eq!(after.status, 200);

    for line in after.body.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        assert_exposition_line(line);
    }

    let heartbeats_after = parse_sample(&after.body, "legion_heartbeats_total ")
        .context("legion_heartbeats_total missing from /metrics")?;
    assert!(
        heartbeats_after >= heartbeats_before + 2.0,
        "counter did not move: before={heartbeats_before} after={heartbeats_after}"
    );

    // Histogram exposition shape: buckets with le labels, +Inf, sum, count.
    assert!(after
        .body
        .contains("# TYPE legion_store_write_lock_wait_seconds histogram"));
    assert!(after
        .body
        .contains("legion_store_write_lock_wait_seconds_bucket{le=\"+Inf\"}"));
    assert!(after
        .body
        .lines()
        .any(|l| l.starts_with("legion_store_write_lock_wait_seconds_sum ")));
    assert!(after
        .body
        .lines()
        .any(|l| l.starts_with("legion_store_write_lock_wait_seconds_count ")));
    assert!(after
        .body
        .contains("legion_evals_processed_total{kind=\"job_submit\",status=\"ok\"}"));
    // Labeled histogram family (scheduler plan phases).
    assert!(after
        .body
        .contains("# TYPE legion_scheduler_plan_phase_seconds histogram"));
    assert!(after
        .body
        .contains("legion_scheduler_plan_phase_seconds_bucket{phase=\"build_plan\",le=\"+Inf\"}"));

    Ok(())
}

/// The WORKER telemetry listener: `/metrics` serves the `legion_worker_*`
/// registry in valid exposition format, `/healthz` answers, `/readyz` does
/// not exist (a worker has no store or quorum to probe), and instrumented
/// values show up.
#[tokio::test]
async fn worker_metrics_listener_serves_worker_registry() -> R<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = telemetry::serve_worker_telemetry_on(listener).await;
    });

    telemetry::worker_metrics().heartbeats_total.inc(&["ok"]);
    telemetry::worker_metrics().allocations_running.set(3);
    telemetry::worker_metrics()
        .heartbeat_duration_seconds
        .observe(0.012);

    let healthz = http_get(addr, "/healthz").await?;
    assert_eq!(healthz.status, 200);

    let readyz = http_get(addr, "/readyz").await?;
    assert_eq!(readyz.status, 404, "workers expose no readiness probe");

    let metrics = http_get(addr, "/metrics").await?;
    assert_eq!(metrics.status, 200);
    assert_eq!(
        metrics.header("content-type"),
        Some("text/plain; version=0.0.4")
    );
    for line in metrics.body.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        assert_exposition_line(line);
    }
    assert!(metrics
        .body
        .contains("legion_worker_heartbeats_total{outcome=\"ok\"}"));
    assert_eq!(
        parse_sample(&metrics.body, "legion_worker_allocations_running "),
        Some(3.0)
    );
    assert!(metrics
        .body
        .contains("legion_worker_heartbeat_duration_seconds_bucket{le=\"+Inf\"}"));
    // Manager families must NOT leak onto the worker listener.
    assert!(!metrics.body.contains("legion_evals_processed_total"));

    Ok(())
}

/// Forced readiness failure: a telemetry state that requires raft (as a
/// raft-mode manager would set) against a local-disk store, whose persistence
/// driver reports no raft status and therefore no known leader. Removing the
/// DB directory does not fail the `SELECT 1` probe (SQLite keeps serving from
/// its open file descriptors on Linux), so the leaderless-raft path is the
/// reliable equivalent forced-failure for the 503 branch.
#[tokio::test]
async fn readyz_returns_503_when_raft_leader_is_unknown() -> R<()> {
    let store = setup_store()?;
    let addr = start_listener(TelemetryState {
        store,
        raft_required: true,
        diverged_since_ms: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
    })
    .await?;

    let readyz = http_get(addr, "/readyz").await?;
    assert_eq!(readyz.status, 503);
    assert!(
        readyz.body.contains("not ready") && readyz.body.contains("raft"),
        "reason body should name the raft leader gap, got: {}",
        readyz.body
    );

    // The same listener still reports live and serves metrics.
    let healthz = http_get(addr, "/healthz").await?;
    assert_eq!(healthz.status, 200);
    let metrics = http_get(addr, "/metrics").await?;
    assert_eq!(metrics.status, 200);

    Ok(())
}

/// A telemetry port another process holds fails the manager's startup. The
/// listener used to bind inside a spawned task and only log the failure, so
/// the manager came up with no `/metrics` and no health probes.
#[tokio::test]
async fn a_telemetry_port_that_is_taken_fails_startup() -> R<()> {
    let taken = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = taken.local_addr()?;
    let err = telemetry::start_telemetry(
        addr,
        TelemetryState {
            store: setup_store()?,
            raft_required: false,
            diverged_since_ms: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
        },
    )
    .await
    .expect_err("the port is taken");
    assert!(
        format!("{err:#}").contains("failed to bind the telemetry listener"),
        "{err:#}"
    );
    Ok(())
}
