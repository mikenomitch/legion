//! Pluggable task-log sinks.
//!
//! A [`LogSink`] is the worker-side extensibility seam for what happens to a
//! task's captured logs. The always-on default is [`FileLogSink`], which owns
//! the rotating `stdout.log` / `stderr.log` files and writes a small `meta.json`
//! resource manifest next to them.
//!
//! Network shipping (to Loki, an OTLP collector, Elasticsearch, …) is delegated
//! to an external collector that tails the log files — the standard pattern
//! (Fluent Bit / Vector / the OpenTelemetry Collector `filelog` receiver), and
//! the same division of labour Nomad uses. The manifest exists precisely so that
//! collector can attach correct OpenTelemetry **resource** attributes
//! (`service.name`, `legion.allocation.id`, …) without re-deriving them. Future
//! in-process sinks (e.g. a native OTLP pusher) implement this same trait.

use crate::prelude::*;
use crate::worker::logs::{maintain_logs, LogConfig};
use std::path::PathBuf;

/// Identity + location of one allocation's captured logs. Doubles as the source
/// of OpenTelemetry resource attributes for downstream collectors.
#[derive(Debug, Clone)]
pub struct LogResource {
    pub allocation_id: i64,
    pub job_id: String,
    pub node_id: String,
    pub task_name: String,
    pub task_kind: String,
    pub log_dir: PathBuf,
}

impl LogResource {
    /// OpenTelemetry resource attributes for this allocation's logs.
    ///
    /// `service.name` follows OTel semantic conventions; the `legion.*` keys
    /// carry orchestrator-specific identity a collector can promote to labels.
    pub fn otel_attributes(&self) -> serde_json::Map<String, Value> {
        let mut attrs = serde_json::Map::new();
        attrs.insert("service.name".into(), json!(self.job_id));
        attrs.insert("legion.job.id".into(), json!(self.job_id));
        attrs.insert("legion.allocation.id".into(), json!(self.allocation_id));
        attrs.insert("legion.node.id".into(), json!(self.node_id));
        attrs.insert("legion.task.name".into(), json!(self.task_name));
        attrs.insert("legion.task.kind".into(), json!(self.task_kind));
        attrs
    }
}

/// Worker-side extension point for task-log handling.
///
/// Implementations must be cheap and resilient: `maintain` runs on a periodic
/// sweep across every active allocation, and a failure for one allocation must
/// not abort the others (the caller logs and continues).
pub trait LogSink: Send + Sync {
    /// Stable identifier for diagnostics.
    fn name(&self) -> &'static str;

    /// Invoked once when an allocation's task starts.
    fn on_start(&self, _resource: &LogResource) -> R<()> {
        Ok(())
    }

    /// Invoked periodically for each active allocation.
    fn maintain(&self, _resource: &LogResource) -> R<()> {
        Ok(())
    }
}

/// The default sink: rotates/prunes the on-disk log files and publishes the
/// resource manifest used by external log shippers.
pub struct FileLogSink {
    config: LogConfig,
}

impl FileLogSink {
    pub fn new(config: LogConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> LogConfig {
        self.config
    }

    /// Write `<log_dir>/meta.json` describing the allocation's log streams and
    /// OTel resource attributes. Best-effort: shippers tolerate its absence.
    fn write_manifest(&self, resource: &LogResource) -> R<()> {
        std::fs::create_dir_all(&resource.log_dir)
            .with_context(|| format!("failed to create log dir {}", resource.log_dir.display()))?;
        // Either this sink or log capture may create the directory first;
        // both enforce the owner-only isolation boundary.
        crate::util::restrict_dir_to_owner(&resource.log_dir)?;
        let manifest = json!({
            "schema": "legion.task-logs/v1",
            "resource": resource.otel_attributes(),
            "streams": {
                "stdout": crate::task_plugins::STDOUT_LOG_FILE,
                "stderr": crate::task_plugins::STDERR_LOG_FILE,
            },
            "rotation": {
                "maxFileBytes": self.config.max_file_bytes,
                "maxFiles": self.config.max_files,
                "retentionDays": self.config.retention_days,
            },
        });
        let path = resource.log_dir.join("meta.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&manifest)?)
            .with_context(|| format!("failed to write manifest {}", path.display()))?;
        Ok(())
    }
}

impl LogSink for FileLogSink {
    fn name(&self) -> &'static str {
        "file"
    }

    fn on_start(&self, resource: &LogResource) -> R<()> {
        self.write_manifest(resource)
    }

    fn maintain(&self, resource: &LogResource) -> R<()> {
        maintain_logs(&resource.log_dir, &self.config)
    }
}

/// Build the default set of sinks for a worker (currently just the file sink).
/// The rotation policy is resolved from the environment.
pub fn default_sinks() -> Vec<Box<dyn LogSink>> {
    vec![Box::new(FileLogSink::new(LogConfig::from_env()))]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn resource(dir: &std::path::Path) -> LogResource {
        LogResource {
            allocation_id: 42,
            job_id: "web".into(),
            node_id: "node-1".into(),
            task_name: "api".into(),
            task_kind: "exec".into(),
            log_dir: dir.to_path_buf(),
        }
    }

    #[test]
    fn file_sink_writes_manifest_with_otel_resource() -> R<()> {
        let dir = TempDir::new()?;
        let sink = FileLogSink::new(LogConfig::default());
        sink.on_start(&resource(dir.path()))?;

        let manifest: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("meta.json"))?)?;
        assert_eq!(manifest["resource"]["service.name"], "web");
        assert_eq!(manifest["resource"]["legion.allocation.id"], 42);
        assert_eq!(manifest["resource"]["legion.task.name"], "api");
        assert_eq!(manifest["streams"]["stdout"], "stdout.log");
        Ok(())
    }

    #[test]
    fn otel_attributes_use_service_name_convention() {
        let dir = TempDir::new().unwrap();
        let attrs = resource(dir.path()).otel_attributes();
        assert_eq!(attrs.get("service.name").unwrap(), "web");
        assert!(attrs.contains_key("legion.node.id"));
    }
}
