use crate::prelude::*;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

const LOG_RETENTION_DAYS: i64 = 7;
const TRACE_RETENTION_DAYS: i64 = 3;

static O11Y_ENABLED: AtomicBool = AtomicBool::new(false);
static APPENDER_GUARDS: OnceLock<Mutex<Vec<WorkerGuard>>> = OnceLock::new();

pub fn o11y_enabled() -> bool {
    O11Y_ENABLED.load(Ordering::Relaxed)
}

#[instrument(skip_all)]
pub fn init_tracing(enable_o11y: bool) -> R<()> {
    init_tracing_with_options(enable_o11y, false, false)
}

/// Initialize tracing, optionally routing the human-readable console layer to
/// stderr instead of stdout.
///
/// Routing to stderr is required for stdio protocol servers (for example
/// `legion mcp serve`), where stdout is reserved for protocol messages and any
/// stray log line would corrupt the stream.
///
/// When `server_mode` is set and OTLP export is configured via the standard
/// OpenTelemetry environment variables (see [`crate::otel`]), Legion's own spans
/// are also exported over OTLP/HTTP. OTLP export is limited to long-running
/// servers so short-lived CLI invocations do not spin up an exporter.
#[instrument(skip_all)]
pub fn init_tracing_with_options(
    enable_o11y: bool,
    console_to_stderr: bool,
    server_mode: bool,
) -> R<()> {
    O11Y_ENABLED.store(enable_o11y, Ordering::Relaxed);

    let mut console_guard = None;
    let stdout_log = if console_to_stderr {
        tracing_subscriber::fmt::layer()
            .pretty()
            .with_writer(std::io::stderr)
            .boxed()
    } else if server_mode {
        // A server's console is read by journald or a container runtime,
        // which falls behind under load. A blocking write then stalled every
        // thread that logged, heartbeats included, until the reader caught
        // up: a worker bursting 1,200 sandbox starts went a minute without a
        // heartbeat and was marked lost. A full buffer drops lines instead.
        let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
        console_guard = Some(guard);
        tracing_subscriber::fmt::layer()
            .pretty()
            .with_writer(writer)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer().pretty().boxed()
    };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    std::fs::create_dir_all("./logs")?;
    std::fs::create_dir_all("./traces")?;
    prune_dir("./logs", LOG_RETENTION_DAYS)?;
    prune_dir("./traces", TRACE_RETENTION_DAYS)?;

    let file_appender = rolling::daily("./logs", "events.log");
    let (file_appender, log_guard) = tracing_appender::non_blocking(file_appender);
    let file_log_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(file_appender);

    let mut trace_guard_opt = None;
    let trace_layer = if enable_o11y {
        let trace_appender = rolling::daily("./traces", "spans.json");
        let (trace_appender, trace_guard) = tracing_appender::non_blocking(trace_appender);
        trace_guard_opt = Some(trace_guard);
        Some(
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_writer(trace_appender)
                .with_filter(EnvFilter::new("info")),
        )
    } else {
        None
    };

    // Optional OTLP export of Legion's own spans (servers only, opt-in by env).
    let otlp_layer = if server_mode {
        match crate::otel::resolved_traces_endpoint() {
            Some(endpoint) => match crate::otel::build_tracing_layer(&endpoint) {
                Ok(layer) => {
                    eprintln!("legion: exporting OTLP traces to {endpoint}");
                    Some(layer)
                }
                Err(err) => {
                    eprintln!("legion: failed to initialize OTLP trace export: {err:#}");
                    None
                }
            },
            None => None,
        }
    } else {
        None
    };

    let subscriber = Registry::default()
        .with(stdout_log)
        .with(file_log_layer)
        .with(env_filter)
        .with(trace_layer)
        .with(otlp_layer);

    APPENDER_GUARDS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("appender guard lock")
        .push(log_guard);
    if let Some(guard) = console_guard {
        APPENDER_GUARDS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("appender guard lock")
            .push(guard);
    }
    if let Some(trace_guard) = trace_guard_opt {
        APPENDER_GUARDS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .expect("appender guard lock")
            .push(trace_guard);
    }

    tracing::subscriber::set_global_default(subscriber)
        .context("unable to set global tracing subscriber")?;

    Ok(())
}

fn prune_dir(dir: &str, retention_days: i64) -> R<()> {
    let now = std::time::SystemTime::now();
    let retention = std::time::Duration::from_secs((retention_days.max(1) as u64) * 24 * 60 * 60);
    let entries = std::fs::read_dir(dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let metadata = std::fs::metadata(&path)?;
        let modified = metadata.modified().unwrap_or(now);
        if now.duration_since(modified).unwrap_or_default() > retention {
            std::fs::remove_file(&path)?;
        }
    }

    Ok(())
}

pub fn read_recent_observability() -> R<(Vec<String>, Vec<String>)> {
    let logs = read_recent_lines(Path::new("./logs"), "events.log", 300)?;
    let traces = if o11y_enabled() {
        read_recent_lines(Path::new("./traces"), "spans.json", 300)?
    } else {
        Vec::new()
    };
    Ok((logs, traces))
}

fn read_recent_lines(dir: &Path, prefix: &str, max_lines: usize) -> R<Vec<String>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with(prefix))
                .unwrap_or(false)
        })
        .collect();
    files.sort();

    let mut lines = Vec::new();
    for file in files.into_iter().rev() {
        let content = std::fs::read_to_string(file)?;
        for line in content.lines().rev() {
            if line.trim().is_empty() {
                continue;
            }
            lines.push(line.to_string());
            if lines.len() >= max_lines {
                lines.reverse();
                return Ok(lines);
            }
        }
    }
    lines.reverse();
    Ok(lines)
}
