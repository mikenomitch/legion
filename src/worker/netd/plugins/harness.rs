//! Run a plugin against one synthetic request, without a cluster: what
//! `legion net plugin test` and the `legion_net_plugin_test` MCP tool do.
//!
//! The request goes through the same [`Pipeline`] and [`Exchange`] the
//! datapath runs (one `plugin` hop, sourced `policy/test#0`), on a node with
//! no network: the response stage sees a synthetic upstream answer, and
//! `dispatch_http_call` targets are refused unless the caller allows them.
//! The report says, per stage, what the plugin did, how long it took, and
//! how much fuel it burned.

use super::PluginHost;
use crate::prelude::*;
use crate::structs::netplugin::NetPluginObject;
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::handlers::{ExchangeOutcome, HopBody, HopFacts, Pipeline, RequestHead};
use crate::worker::netd::policy::AllocIdentity;

use http_body_util::BodyExt;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// The synthetic request (and upstream answer) to run.
#[derive(Debug, Clone, Default)]
pub struct TestInput {
    pub method: String,
    /// An absolute URL (`https://api.github.com/repos/x`).
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    /// The calling task's `network.labels`.
    pub labels: BTreeMap<String, String>,
    pub job: String,
    pub task: String,
    /// The reference's `config`.
    pub config: Option<Value>,
    /// The upstream's answer, for the `response` stage.
    pub response_status: u16,
    pub response_body: Vec<u8>,
}

/// One stage's result.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StageReport {
    pub stage: String,
    /// `continue`, `answered` (a local response), `denied`, or `failed`.
    /// `micros` and `fuel` count the plugin's callbacks only, not starting
    /// its instance.
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub micros: u64,
    pub fuel: u64,
    /// What the stage changed, one line each.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<String>,
}

/// What a test run found.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestReport {
    pub plugin: String,
    pub stages: Vec<StageReport>,
    /// The status the client would have received.
    pub status: u16,
    /// The body the client would have received (UTF-8, lossy).
    pub body: String,
    /// Log lines are written to stderr as they happen.
    pub errors: u64,
}

/// Run `input` through the plugin.
pub async fn run(
    object: NetPluginObject,
    module: &[u8],
    input: TestInput,
) -> Result<TestReport, String> {
    let cache = Arc::new(crate::worker::netd::NetPolicyCache::new());
    let mut identities = HashMap::new();
    identities.insert(
        1,
        AllocIdentity {
            job_id: input.job.clone(),
            task_name: input.task.clone(),
            group: input.task.clone(),
            labels: input.labels.clone(),
            ports: Default::default(),
        },
    );
    cache.set_identities(identities);
    let context = Arc::new(ForwarderContext::new(
        cache,
        Arc::new(crate::worker::netd::Denials::new()),
    ));
    let plugins: &Arc<PluginHost> = context.plugins();
    let name = object.name.clone();
    let stages = object.stage_set();
    let entry = plugins.install_compiled(object, 1, module)?;

    let url = input.url.trim();
    let (scheme, rest) = url.split_once("://").ok_or_else(|| {
        format!("--request needs an absolute URL (https://host/path), got {url:?}")
    })?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let default_port = if scheme == "https" { 443 } else { 80 };
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(default_port)),
        None => (authority.to_string(), default_port),
    };
    let destination = format!("{host}:{port}");
    let hop: crate::structs::hops::HopConfig = serde_json::from_value(json!({
        "plugin": { "name": name, "config": input.config },
    }))
    .map_err(|err| err.to_string())?;
    let chain = crate::structs::netpolicy::EffectiveChain {
        protocol: "http".into(),
        policy: "allow".into(),
        access_log: false,
        callers: None,
        rules: Vec::new(),
        handlers: vec![crate::structs::netpolicy::EffectiveHandler {
            handler: hop,
            source: "policy/test#0".into(),
        }],
    };
    let pipeline = Arc::new(Pipeline::compile(Some(&chain), None, &[]));
    let mut headers: Vec<(String, String)> = input
        .headers
        .iter()
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    if let Some(body) = &input.body {
        headers.push(("content-length".into(), body.len().to_string()));
    }
    let facts = HopFacts {
        allocation_id: 1,
        direction: "egress",
        protocol: "http",
        destination: &destination,
        host: Some(&host),
        port,
        ..Default::default()
    };
    let mut head = RequestHead {
        method: input.method.to_ascii_uppercase(),
        path: path.to_string(),
        authority: authority.to_string(),
        scheme: scheme.to_string(),
        headers: headers.clone(),
        end_of_stream: input.body.is_none(),
        changed: false,
    };
    let stats = entry.stats.clone();
    let mut reports = Vec::new();
    let mut exchange = pipeline.exchange(&context, &facts);
    let fuel = stats.fuel.load(Ordering::Relaxed);
    let micros = stats.micros.load(Ordering::Relaxed);
    let outcome = exchange.request(&facts, &mut head).await;
    let changes = if head.changed {
        diff_head(path, &headers, &head)
    } else {
        Vec::new()
    };
    if stages.contains(&crate::structs::hops::Stage::Request) {
        reports.push(report("request", micros, fuel, &stats, &outcome, changes));
    }
    if let Some(answer) = stopped(&outcome) {
        exchange.answered(answer.0);
        drop(exchange);
        return Ok(finish(name, reports, answer, &stats));
    }
    if exchange.reads_body() {
        let mut body = bytes::Bytes::from(input.body.clone().unwrap_or_default());
        let before = body.clone();
        let fuel = stats.fuel.load(Ordering::Relaxed);
        let micros = stats.micros.load(Ordering::Relaxed);
        let outcome = exchange.body(&mut body).await;
        let changes = if body != before {
            vec![format!(
                "body rewritten ({} -> {} bytes)",
                before.len(),
                body.len()
            )]
        } else {
            Vec::new()
        };
        reports.push(report("body", micros, fuel, &stats, &outcome, changes));
        if let Some(answer) = stopped(&outcome) {
            exchange.answered(answer.0);
            drop(exchange);
            return Ok(finish(name, reports, answer, &stats));
        }
    }
    let upstream: hyper::Response<HopBody> = hyper::Response::builder()
        .status(input.response_status.max(100))
        .body(
            http_body_util::Full::new(bytes::Bytes::from(input.response_body.clone()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .map_err(|err| err.to_string())?;
    let fuel = stats.fuel.load(Ordering::Relaxed);
    let micros = stats.micros.load(Ordering::Relaxed);
    let answered = exchange.respond(upstream).await;
    let status = answered.status().as_u16();
    let body = answered
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes().to_vec())
        .unwrap_or_else(|err| format!("(the response was aborted: {err})").into_bytes());
    if stages.contains(&crate::structs::hops::Stage::Response)
        || stages.contains(&crate::structs::hops::Stage::Done)
    {
        let mut changes = Vec::new();
        if status != input.response_status {
            changes.push(format!("status {} -> {status}", input.response_status));
        }
        if body != input.response_body {
            changes.push(format!(
                "response body rewritten ({} -> {} bytes)",
                input.response_body.len(),
                body.len()
            ));
        }
        reports.push(StageReport {
            stage: "response".into(),
            outcome: "continue".into(),
            status: Some(status),
            reason: None,
            micros: stats.micros.load(Ordering::Relaxed).saturating_sub(micros),
            fuel: stats.fuel.load(Ordering::Relaxed).saturating_sub(fuel),
            changes,
        });
    }
    Ok(finish(name, reports, (status, body), &stats))
}

fn report(
    stage: &str,
    micros_before: u64,
    fuel_before: u64,
    stats: &super::PluginStats,
    outcome: &ExchangeOutcome,
    changes: Vec<String>,
) -> StageReport {
    let (outcome, status, reason) = match outcome {
        ExchangeOutcome::Continue | ExchangeOutcome::Proxy(_) => ("continue", None, None),
        ExchangeOutcome::Deny { reason, .. } => ("denied", Some(403), reason.clone()),
        // The 503 a failed plugin answers with carries the `legion` error
        // body; anything else is the plugin's own answer.
        ExchangeOutcome::Respond(local)
            if local.status == 503 && local.body.starts_with(b"{\"legion\"") =>
        {
            ("failed", Some(503), local.reason.clone())
        }
        ExchangeOutcome::Respond(local) => ("answered", Some(local.status), local.reason.clone()),
    };
    StageReport {
        stage: stage.to_string(),
        outcome: outcome.to_string(),
        status,
        reason,
        micros: stats
            .micros
            .load(Ordering::Relaxed)
            .saturating_sub(micros_before),
        fuel: stats
            .fuel
            .load(Ordering::Relaxed)
            .saturating_sub(fuel_before),
        changes,
    }
}

fn stopped(outcome: &ExchangeOutcome) -> Option<(u16, Vec<u8>)> {
    match outcome {
        ExchangeOutcome::Continue | ExchangeOutcome::Proxy(_) => None,
        ExchangeOutcome::Deny { reason, .. } => {
            Some((403, reason.clone().unwrap_or_default().into_bytes()))
        }
        ExchangeOutcome::Respond(local) => Some((local.status, local.body.clone())),
    }
}

fn finish(
    plugin: String,
    stages: Vec<StageReport>,
    (status, body): (u16, Vec<u8>),
    stats: &super::PluginStats,
) -> TestReport {
    TestReport {
        plugin,
        stages,
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
        errors: stats.errors.load(Ordering::Relaxed),
    }
}

/// What a plugin changed in the request head.
fn diff_head(path: &str, before: &[(String, String)], head: &RequestHead) -> Vec<String> {
    let mut changes = Vec::new();
    if head.path != path {
        changes.push(format!(":path {path} -> {}", head.path));
    }
    for (name, value) in &head.headers {
        match before.iter().find(|(n, _)| n == name) {
            None => changes.push(format!("+ {name}: {value}")),
            Some((_, old)) if old != value => changes.push(format!("~ {name}: {old} -> {value}")),
            _ => {}
        }
    }
    for (name, _) in before {
        if !head.headers.iter().any(|(n, _)| n == name) {
            changes.push(format!("- {name}"));
        }
    }
    changes
}

#[cfg(test)]
mod harness_test {
    use super::*;

    #[tokio::test]
    async fn the_documented_example_reports_its_denial_test() {
        let module = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/netplugins/read-only-free-tier.wasm"
        ))
        .unwrap();
        let mut object: NetPluginObject = serde_json::from_value(json!({
            "name": "read-only-free-tier",
            "module": { "source": "file:///x.wasm", "checksum": format!("sha256:{}", "0".repeat(64)) },
            "stages": ["request"],
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let input = TestInput {
            method: "POST".into(),
            url: "https://api.github.com/repos/x/issues".into(),
            labels: [("tier".to_string(), "free".to_string())]
                .into_iter()
                .collect(),
            job: "agent".into(),
            task: "main".into(),
            response_status: 200,
            ..Default::default()
        };
        let report = run(object.clone(), &module, input.clone()).await.unwrap();
        assert_eq!(report.status, 403);
        assert_eq!(report.stages.len(), 1);
        assert_eq!(report.stages[0].outcome, "answered");
        assert!(report.stages[0].fuel > 0);
        let report = run(
            object,
            &module,
            TestInput {
                method: "GET".into(),
                ..input
            },
        )
        .await
        .unwrap();
        assert_eq!(report.status, 200);
        assert_eq!(report.stages[0].outcome, "continue");
    }
}
