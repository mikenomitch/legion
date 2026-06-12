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
            "LEGION_OTLP",
        ] {
            std::env::remove_var(key);
        }
        assert_eq!(resolved_traces_endpoint(), None, "disabled by default");

        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318");
        assert_eq!(
            resolved_traces_endpoint().as_deref(),
            Some("http://collector:4318/v1/traces"),
            "base endpoint gets /v1/traces appended"
        );

        std::env::set_var(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://collector:4318/custom",
        );
        assert_eq!(
            resolved_traces_endpoint().as_deref(),
            Some("http://collector:4318/custom"),
            "signal-specific endpoint used verbatim"
        );

        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        std::env::set_var("LEGION_OTLP", "1");
        assert_eq!(
            resolved_traces_endpoint().as_deref(),
            Some("http://localhost:4318/v1/traces"),
            "LEGION_OTLP forces export at the default endpoint"
        );
        std::env::remove_var("LEGION_OTLP");
    }
}
