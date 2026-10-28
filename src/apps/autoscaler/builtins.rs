//! The plugins built into the autoscaler (docs/autoscaling.md, "Sources" and
//! "Standalone policies"): the `prometheus` and `http` sources, and the
//! `exec` and `noop` targets. They implement the same calls the protocol
//! carries, in process.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};

use super::host::{SourceReading, TargetState};

/// The default timeout of a builtin source's HTTP request.
const SOURCE_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// The default timeout of one `exec` target command.
const EXEC_TIMEOUT: Duration = Duration::from_secs(120);

/// Most points a Prometheus range query asks for over a window.
const RANGE_POINTS: u64 = 12;

fn headers_of(config: &Value) -> Vec<(String, String)> {
    config
        .get("headers")
        .and_then(Value::as_object)
        .map(|headers| {
            headers
                .iter()
                .filter_map(|(name, value)| value.as_str().map(|v| (name.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn timeout_of(config: &Value, default: Duration) -> Duration {
    config
        .get("timeout")
        .and_then(Value::as_str)
        .and_then(|text| humantime::parse_duration(text.trim()).ok())
        .filter(|timeout| !timeout.is_zero())
        .unwrap_or(default)
}

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    query: &[(&str, String)],
    headers: &[(String, String)],
    timeout: Duration,
) -> Result<Value, String> {
    let mut request = client.get(url).query(query).timeout(timeout);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send().await.map_err(|err| {
        if err.is_timeout() {
            format!("timeout after {}s", timeout.as_secs_f64())
        } else {
            format!("GET {url}: {err}")
        }
    })?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("GET {url}: {err}"))?;
    let body: Value = serde_json::from_str(&text).map_err(|err| {
        format!(
            "GET {url}: HTTP {status}, not JSON ({err}): {}",
            text.chars().take(200).collect::<String>()
        )
    })?;
    if !status.is_success() && body.get("status").and_then(Value::as_str) != Some("error") {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    Ok(body)
}

/// A number from JSON: a number, or a string holding one (Prometheus sends
/// sample values as strings). `NaN` and infinities are not numbers here.
pub fn number(value: &Value) -> Option<f64> {
    let parsed = match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }?;
    parsed.is_finite().then_some(parsed)
}

/// Read one value from a Prometheus query response (`/api/v1/query` or
/// `/api/v1/query_range`). A range is averaged over its points.
pub fn prometheus_value(body: &Value) -> Result<SourceReading, String> {
    if body.get("status").and_then(Value::as_str) == Some("error") {
        return Err(format!(
            "prometheus: {} ({})",
            body.get("error").and_then(Value::as_str).unwrap_or("error"),
            body.get("errorType").and_then(Value::as_str).unwrap_or("-")
        ));
    }
    let data = body
        .get("data")
        .ok_or_else(|| "prometheus: the response has no data".to_string())?;
    let result = data.get("result").cloned().unwrap_or(Value::Null);
    match data.get("resultType").and_then(Value::as_str) {
        Some("scalar") => {
            let value = result
                .get(1)
                .and_then(number)
                .ok_or_else(|| format!("prometheus: the scalar {result} is not a number"))?;
            Ok(SourceReading::value(value, true))
        }
        Some(kind @ ("vector" | "matrix")) => {
            let series = result.as_array().cloned().unwrap_or_default();
            match series.len() {
                0 => Ok(SourceReading::Missing("the query returned no series".into())),
                1 => {
                    let values: Vec<f64> = if kind == "vector" {
                        series[0]
                            .get("value")
                            .and_then(|sample| sample.get(1))
                            .and_then(number)
                            .into_iter()
                            .collect()
                    } else {
                        series[0]
                            .get("values")
                            .and_then(Value::as_array)
                            .map(|points| {
                                points
                                    .iter()
                                    .filter_map(|point| point.get(1).and_then(number))
                                    .collect()
                            })
                            .unwrap_or_default()
                    };
                    if values.is_empty() {
                        return Ok(SourceReading::Missing(
                            "the series has no numeric samples".into(),
                        ));
                    }
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    Ok(SourceReading::value(mean, true))
                }
                n => Err(format!(
                    "the query returned {n} series; a check needs one number. Hint: aggregate \
                     it, as in sum(...) or max(...)"
                )),
            }
        }
        other => Err(format!(
            "prometheus: unexpected result type {}",
            other.unwrap_or("(none)")
        )),
    }
}

/// The `prometheus` source.
pub struct PrometheusSource {
    address: String,
    headers: Vec<(String, String)>,
    timeout: Duration,
    client: reqwest::Client,
}

impl PrometheusSource {
    /// From the operator's config (`{ address, headers, timeout }`).
    pub fn new(config: &Value, client: reqwest::Client) -> Self {
        Self {
            address: config
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim_end_matches('/')
                .to_string(),
            headers: headers_of(config),
            timeout: timeout_of(config, SOURCE_HTTP_TIMEOUT),
            client,
        }
    }

    /// Run `query` at `now_ms`: an instant query, or with a `window`, a
    /// range query over it averaged across its points.
    pub async fn query(&self, query: &str, window: Duration, now_ms: i64) -> Result<SourceReading, String> {
        if query.trim().is_empty() {
            return Err("the prometheus source needs a PromQL query".into());
        }
        let seconds = |ms: i64| format!("{:.3}", ms as f64 / 1000.0);
        let body = if window.is_zero() {
            get_json(
                &self.client,
                &format!("{}/api/v1/query", self.address),
                &[("query", query.to_string()), ("time", seconds(now_ms))],
                &self.headers,
                self.timeout,
            )
            .await?
        } else {
            let window_ms = i64::try_from(window.as_millis()).unwrap_or(i64::MAX / 4);
            let step = (window.as_secs() / RANGE_POINTS).max(1);
            get_json(
                &self.client,
                &format!("{}/api/v1/query_range", self.address),
                &[
                    ("query", query.to_string()),
                    ("start", seconds(now_ms - window_ms)),
                    ("end", seconds(now_ms)),
                    ("step", format!("{step}s")),
                ],
                &self.headers,
                self.timeout,
            )
            .await?
        };
        prometheus_value(&body)
    }
}

/// The `http` source: GET a JSON document, read a number at a JSON pointer.
pub struct HttpSource {
    url: String,
    headers: Vec<(String, String)>,
    timeout: Duration,
    client: reqwest::Client,
}

/// Read the number at `pointer` (empty: the whole document).
pub fn json_pointer_value(body: &Value, pointer: &str) -> Result<f64, String> {
    let pointer = pointer.trim();
    let found = if pointer.is_empty() || pointer == "/" {
        Some(body)
    } else if !pointer.starts_with('/') {
        return Err(format!(
            "the http source's query is a JSON pointer, like \"/queue/depth\"; got {pointer:?}"
        ));
    } else {
        body.pointer(pointer)
    };
    let found = found.ok_or_else(|| format!("nothing at {pointer} in the response"))?;
    number(found).ok_or_else(|| format!("{found} at {pointer:?} is not a number"))
}

impl HttpSource {
    /// From the operator's config (`{ url, headers, timeout }`).
    pub fn new(config: &Value, client: reqwest::Client) -> Self {
        Self {
            url: config
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            headers: headers_of(config),
            timeout: timeout_of(config, SOURCE_HTTP_TIMEOUT),
            client,
        }
    }

    /// GET the document and read `pointer` in it. The app averages these
    /// readings over the check's window, like the `legion` source's.
    pub async fn query(&self, pointer: &str) -> Result<SourceReading, String> {
        let body = get_json(&self.client, &self.url, &[], &self.headers, self.timeout).await?;
        Ok(SourceReading::value(json_pointer_value(&body, pointer)?, false))
    }
}

/// The `exec` target: one command per operation, volume-driver style.
///
/// ```text
/// <command…> status|scale|terminate   < {"apiVersion":1,"op":"scale","policy":"…","config":{…},"desired":9,"decision":"d-…"}
///                                     > {"count":7,"ready":7}   (status; anything for the others)
/// ```
pub struct ExecTarget {
    command: Vec<String>,
    timeout: Duration,
}

impl ExecTarget {
    /// From the operator's config (`{ command, timeout }`).
    pub fn new(config: &Value) -> Self {
        Self {
            command: config
                .get("command")
                .and_then(|command| crate::autoscaler::plugins::command_argv(command).ok())
                .unwrap_or_default(),
            timeout: timeout_of(config, EXEC_TIMEOUT),
        }
    }

    /// Run one operation and return its JSON reply.
    pub async fn run(&self, op: &str, mut request: Value) -> Result<Value, String> {
        let mut argv = self.command.clone();
        argv.push(op.to_string());
        if let Some(object) = request.as_object_mut() {
            object.insert("apiVersion".into(), json!(1));
            object.insert("op".into(), json!(op));
        }
        let timeout = self.timeout;
        tokio::task::spawn_blocking(move || {
            crate::util::run_json_command("exec scaling target", &argv, &request, timeout)
        })
        .await
        .map_err(|err| format!("the exec target's command panicked: {err}"))?
        .map_err(|err| format!("{err:#}"))
    }
}

/// The `noop` target: holds the count the policy last chose, in memory, and
/// changes nothing anywhere.
#[derive(Default)]
pub struct NoopTarget {
    counts: std::sync::Mutex<HashMap<String, u32>>,
}

impl NoopTarget {
    /// The count `policy` last scaled to, or `start` the first time.
    pub fn status(&self, policy: &str, start: u32) -> TargetState {
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let count = *counts.entry(policy.to_string()).or_insert(start);
        TargetState {
            count,
            ready: count,
            members: Vec::new(),
        }
    }

    /// Record the new count.
    pub fn scale(&self, policy: &str, desired: u32) {
        self.counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(policy.to_string(), desired);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_responses_test() {
        let vector = json!({ "status": "success", "data": { "resultType": "vector",
            "result": [{ "metric": {}, "value": [1700000000.0, "2310"] }] } });
        assert_eq!(prometheus_value(&vector), Ok(SourceReading::value(2310.0, true)));
        let matrix = json!({ "status": "success", "data": { "resultType": "matrix",
            "result": [{ "metric": {}, "values": [[1, "1"], [2, "3"], [3, "NaN"]] }] } });
        assert_eq!(prometheus_value(&matrix), Ok(SourceReading::value(2.0, true)));
        let scalar = json!({ "status": "success", "data": { "resultType": "scalar", "result": [1, "7"] } });
        assert_eq!(prometheus_value(&scalar), Ok(SourceReading::value(7.0, true)));
        let empty = json!({ "status": "success", "data": { "resultType": "vector", "result": [] } });
        assert!(matches!(prometheus_value(&empty), Ok(SourceReading::Missing(_))));
        let two = json!({ "status": "success", "data": { "resultType": "vector",
            "result": [{ "value": [1, "1"] }, { "value": [1, "2"] }] } });
        assert!(prometheus_value(&two).unwrap_err().contains("aggregate"));
        let error = json!({ "status": "error", "errorType": "bad_data", "error": "parse error at char 4" });
        assert!(prometheus_value(&error).unwrap_err().contains("parse error at char 4"));
    }

    #[test]
    fn json_pointers_test() {
        let body = json!({ "queue": { "depth": 42, "lag": "7.5", "name": "x" } });
        assert_eq!(json_pointer_value(&body, "/queue/depth"), Ok(42.0));
        assert_eq!(json_pointer_value(&body, "/queue/lag"), Ok(7.5));
        assert!(json_pointer_value(&body, "/queue/name").unwrap_err().contains("not a number"));
        assert!(json_pointer_value(&body, "/nope").unwrap_err().contains("nothing at"));
        assert!(json_pointer_value(&body, "queue").unwrap_err().contains("JSON pointer"));
        assert_eq!(json_pointer_value(&json!(12), ""), Ok(12.0));
    }

    #[test]
    fn noop_target_holds_its_count_test() {
        let target = NoopTarget::default();
        assert_eq!(target.status("p", 2).count, 2);
        target.scale("p", 5);
        assert_eq!(target.status("p", 2).count, 5);
    }

    #[tokio::test]
    async fn exec_target_runs_one_command_per_operation_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let script = dir.path().join("target.sh");
        std::fs::write(
            &script,
            "#!/bin/sh\nread -r request\ncase \"$1\" in\n  status) echo '{\"count\": 3, \"ready\": 2}' ;;\n  scale) echo \"$request\" > \"$(dirname \"$0\")/last\"; echo '{}' ;;\n  *) echo \"unknown $1\" >&2; exit 2 ;;\nesac\n",
        )
        .expect("write");
        let target = ExecTarget::new(&json!({ "command": ["sh", script.display().to_string()] }));
        let status = target.run("status", json!({ "policy": "p" })).await.unwrap();
        assert_eq!(status, json!({ "count": 3, "ready": 2 }));
        target
            .run("scale", json!({ "policy": "p", "desired": 4 }))
            .await
            .unwrap();
        let last: Value =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("last")).unwrap()).unwrap();
        assert_eq!(last["op"], "scale");
        assert_eq!(last["apiVersion"], 1);
        assert_eq!(last["desired"], 4);
        let err = target.run("terminate", json!({})).await.unwrap_err();
        assert!(err.contains("unknown terminate"), "{err}");
    }
}
