//! OpenTelemetry (OTLP) trace export for Legion's *own* spans.
//!
//! This makes the control plane's `tracing` spans OpenTelemetry-compliant and
//! shippable to any OTLP-capable backend (Tempo, Jaeger, Honeycomb, an OTel
//! Collector, …). It is opt-in and configured entirely through the standard
//! OpenTelemetry environment variables, so the baseline is "set one env var":
//!
//! ```bash
//! # Point Legion at an OTLP/HTTP collector and traces start flowing:
//! export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
//! legion start manager
//! ```
//!
//! Recognized configuration (all optional):
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` — base collector URL (default
//!   `http://localhost:4318`). `/v1/traces` is appended for the trace signal.
//! - `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` — exact traces endpoint (used as-is).
//! - `OTEL_SERVICE_NAME` — service.name resource attribute (default `legion`).
//! - `LEGION_OTLP=1` — force-enable export even when no endpoint var is set
//!   (uses the default endpoint).
//!
//! The exporter is HTTP/protobuf so it does not pull a second gRPC stack into
//! the build.

use crate::prelude::*;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::OnceLock;

/// Default OTLP/HTTP collector endpoint (the conventional port 4318).
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4318";

/// Keep the provider alive for the process lifetime so the batch exporter's
/// background worker keeps flushing.
static PROVIDER: OnceLock<TracerProvider> = OnceLock::new();

/// Resolve whether OTLP export is requested and where it should send traces.
///
/// Returns the fully-qualified traces endpoint when enabled, otherwise `None`.
pub fn resolved_traces_endpoint() -> Option<String> {
    let has_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok()
        || std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_ok();
    let forced = std::env::var("LEGION_OTLP")
        .map(|v| matches!(v.trim(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if !has_endpoint && !forced {
        return None;
    }

    // Signal-specific endpoint is used verbatim; the base endpoint gets the
    // conventional `/v1/traces` path appended.
    if let Ok(traces) = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
        if !traces.trim().is_empty() {
            return Some(traces.trim().to_string());
        }
    }
    let base = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_string());
    Some(format!("{}/v1/traces", base.trim_end_matches('/')))
}

fn service_name() -> String {
    std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "legion".to_string())
}

/// Build the OTLP tracer provider for the given traces endpoint and register it
/// as the global provider. Returns a `tracing` layer to add to the subscriber.
///
/// The returned layer is boxed so callers can compose it with the existing
/// layer stack regardless of concrete types.
pub fn build_tracing_layer<S>(
    endpoint: &str,
) -> R<Box<dyn tracing_subscriber::Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber
        + for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + Send
        + Sync
        + 'static,
{
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .context("failed to build OTLP span exporter")?;

    let resource = Resource::new(vec![
        KeyValue::new("service.name", service_name()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("legion");
    opentelemetry::global::set_tracer_provider(provider.clone());
    let _ = PROVIDER.set(provider);

    use tracing_subscriber::Layer;
    Ok(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
}

/// Flush any buffered spans. Best-effort; safe to call when export is disabled.
pub fn shutdown() {
    if let Some(provider) = PROVIDER.get() {
        for result in provider.force_flush() {
            let _ = result;
        }
    }
}

// ---------------------------------------------------------------------------
// OTLP metrics push
// ---------------------------------------------------------------------------

/// Resolve whether OTLP METRICS export is requested and where to send it.
/// Same gating convention as traces: enabled by any OTLP endpoint var (or
/// `LEGION_OTLP=1`); `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is used
/// verbatim, otherwise the base endpoint gets `/v1/metrics` appended.
pub fn resolved_metrics_endpoint() -> Option<String> {
    let has_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok()
        || std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_ok();
    let forced = std::env::var("LEGION_OTLP")
        .map(|v| matches!(v.trim(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if !has_endpoint && !forced {
        return None;
    }
    if let Ok(metrics) = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") {
        if !metrics.trim().is_empty() {
            return Some(metrics.trim().to_string());
        }
    }
    let base = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_OTLP_ENDPOINT.to_string());
    Some(format!("{}/v1/metrics", base.trim_end_matches('/')))
}

/// Export interval: `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds, the OTel
/// SDK convention), default 60s.
fn metrics_export_interval() -> std::time::Duration {
    std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(std::time::Duration::from_millis)
        .unwrap_or(std::time::Duration::from_secs(60))
}

/// Start the periodic OTLP metrics push if an endpoint is configured; no-op
/// otherwise. Call once per process after the tokio runtime is up (manager
/// and worker daemons both do). Every tick snapshots whichever metric
/// registries the process has initialized (manager `legion_*`, worker
/// `legion_worker_*`) and POSTs one OTLP/HTTP protobuf
/// `ExportMetricsServiceRequest` — cumulative temporality, full histogram
/// buckets. A failed export is logged at debug and retried on the next tick;
/// metrics are never worth crashing (or back-pressuring) the daemon for.
pub fn start_metrics_push() {
    static STARTED: OnceLock<()> = OnceLock::new();
    let Some(endpoint) = resolved_metrics_endpoint() else {
        return;
    };
    if STARTED.set(()).is_err() {
        return;
    }
    let interval = metrics_export_interval();
    info!(%endpoint, interval_ms = interval.as_millis() as u64, "OTLP metrics push enabled");
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let started_unix_nano = unix_nano_now();
        loop {
            tokio::time::sleep(interval).await;
            let request = export_request(started_unix_nano);
            let body = prost::Message::encode_to_vec(&request);
            match client
                .post(&endpoint)
                .header("content-type", "application/x-protobuf")
                .body(body)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => {
                    debug!(status = %response.status(), "OTLP metrics export rejected");
                }
                Err(error) => {
                    debug!(%error, "OTLP metrics export failed");
                }
            }
        }
    });
}

fn unix_nano_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Build one OTLP export request from the live registries.
fn export_request(
    started_unix_nano: u64,
) -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, InstrumentationScope, KeyValue as ProtoKeyValue,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, number_data_point, AggregationTemporality, Gauge, Histogram, HistogramDataPoint,
        Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;

    let now = unix_nano_now();
    let string_kv = |key: &str, value: String| ProtoKeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    };
    let attributes = |names: &[&str], values: &[String]| -> Vec<ProtoKeyValue> {
        names
            .iter()
            .zip(values.iter())
            .map(|(name, value)| string_kv(name, value.clone()))
            .collect()
    };

    let snapshots = crate::manager::telemetry::snapshot_initialized_families();
    let mut metrics_out = Vec::with_capacity(snapshots.len());
    for family in snapshots {
        use crate::manager::telemetry::{SampleValue, SnapshotKind};
        let data = match family.kind {
            SnapshotKind::Counter => metric::Data::Sum(Sum {
                data_points: family
                    .samples
                    .iter()
                    .map(|sample| NumberDataPoint {
                        attributes: attributes(family.label_names, &sample.label_values),
                        start_time_unix_nano: started_unix_nano,
                        time_unix_nano: now,
                        value: Some(number_data_point::Value::AsInt(match sample.value {
                            SampleValue::Counter(v) => v as i64,
                            _ => 0,
                        })),
                        ..Default::default()
                    })
                    .collect(),
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            }),
            SnapshotKind::Gauge => metric::Data::Gauge(Gauge {
                data_points: family
                    .samples
                    .iter()
                    .map(|sample| NumberDataPoint {
                        attributes: attributes(family.label_names, &sample.label_values),
                        start_time_unix_nano: started_unix_nano,
                        time_unix_nano: now,
                        value: Some(number_data_point::Value::AsInt(match sample.value {
                            SampleValue::Gauge(v) => v,
                            _ => 0,
                        })),
                        ..Default::default()
                    })
                    .collect(),
            }),
            SnapshotKind::Histogram => metric::Data::Histogram(Histogram {
                data_points: family
                    .samples
                    .iter()
                    .filter_map(|sample| match &sample.value {
                        SampleValue::Histogram {
                            bounds,
                            bucket_counts,
                            count,
                            sum,
                        } => Some(HistogramDataPoint {
                            attributes: attributes(family.label_names, &sample.label_values),
                            start_time_unix_nano: started_unix_nano,
                            time_unix_nano: now,
                            count: *count,
                            sum: Some(*sum),
                            bucket_counts: bucket_counts.clone(),
                            explicit_bounds: bounds.to_vec(),
                            ..Default::default()
                        }),
                        _ => None,
                    })
                    .collect(),
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            }),
        };
        metrics_out.push(Metric {
            name: family.name.to_string(),
            description: family.help.to_string(),
            unit: String::new(),
            data: Some(data),
            ..Default::default()
        });
    }

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(ProtoResource {
                attributes: vec![
                    string_kv("service.name", service_name()),
                    string_kv("service.version", env!("CARGO_PKG_VERSION").to_string()),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "legion".to_string(),
                    ..Default::default()
                }),
                metrics: metrics_out,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // This test is the sole toucher of the OTEL_* / LEGION_OTLP env vars, so
    // mutating process env here does not race other tests.
    #[test]
    fn endpoint_resolution_follows_otel_env_conventions() {
        for key in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "LEGION_OTLP",
        ] {
            std::env::remove_var(key);
        }
        assert_eq!(resolved_traces_endpoint(), None, "disabled by default");
        assert_eq!(resolved_metrics_endpoint(), None, "disabled by default");

        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318");
        assert_eq!(
            resolved_traces_endpoint().as_deref(),
            Some("http://collector:4318/v1/traces"),
            "base endpoint gets /v1/traces appended"
        );
        assert_eq!(
            resolved_metrics_endpoint().as_deref(),
            Some("http://collector:4318/v1/metrics"),
            "base endpoint gets /v1/metrics appended"
        );

        std::env::set_var(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://collector:4318/custom",
        );
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "http://collector:4318/custom-metrics",
        );
        assert_eq!(
            resolved_traces_endpoint().as_deref(),
            Some("http://collector:4318/custom"),
            "signal-specific endpoint used verbatim"
        );
        assert_eq!(
            resolved_metrics_endpoint().as_deref(),
            Some("http://collector:4318/custom-metrics"),
            "signal-specific metrics endpoint used verbatim"
        );

        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
        std::env::set_var("LEGION_OTLP", "1");
        assert_eq!(
            resolved_traces_endpoint().as_deref(),
            Some("http://localhost:4318/v1/traces"),
            "LEGION_OTLP forces export at the default endpoint"
        );
        assert_eq!(
            resolved_metrics_endpoint().as_deref(),
            Some("http://localhost:4318/v1/metrics"),
            "LEGION_OTLP forces metrics export at the default endpoint"
        );
        std::env::remove_var("LEGION_OTLP");
    }

    /// The OTLP export request carries the live registries with full
    /// fidelity: counters as cumulative monotonic sums, histograms with
    /// their real bucket counts and bounds, and label tuples as attributes.
    #[test]
    fn export_request_carries_registry_families() {
        use opentelemetry_proto::tonic::metrics::v1::metric;

        crate::manager::telemetry::metrics()
            .plan_apply_total
            .inc(&["applied"]);
        crate::manager::telemetry::worker_metrics()
            .heartbeat_duration_seconds
            .observe(0.004);

        let request = export_request(1);
        let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;

        let plan_apply = metrics
            .iter()
            .find(|m| m.name == "legion_plan_apply_total")
            .expect("manager family exported");
        match plan_apply.data.as_ref().expect("data") {
            metric::Data::Sum(sum) => {
                assert!(sum.is_monotonic);
                let point = sum
                    .data_points
                    .iter()
                    .find(|p| p.attributes.iter().any(|attr| attr.key == "outcome"))
                    .expect("labeled data point");
                assert_eq!(point.time_unix_nano > 0, true);
            }
            other => panic!("counter must export as Sum, got {other:?}"),
        }

        let heartbeat = metrics
            .iter()
            .find(|m| m.name == "legion_worker_heartbeat_duration_seconds")
            .expect("worker family exported");
        match heartbeat.data.as_ref().expect("data") {
            metric::Data::Histogram(histogram) => {
                let point = &histogram.data_points[0];
                assert!(point.count >= 1);
                assert_eq!(
                    point.bucket_counts.len(),
                    point.explicit_bounds.len() + 1,
                    "OTLP bucket_counts must be bounds + overflow"
                );
                assert!(point.bucket_counts.iter().sum::<u64>() >= 1);
            }
            other => panic!("histogram must export as Histogram, got {other:?}"),
        }
    }
}
