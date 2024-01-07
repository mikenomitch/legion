use crate::prelude::*;

use tracing_appender::rolling;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

#[instrument(skip_all)]
pub fn init_tracing() -> R<()> {
    // Ideally, I have stdout logging that is Info Level or above, but
    // I can change the level based on the env variable (or argument)

    // And then I also have an OpenTelemetry thing, that can also
    // be configured with a separate env variable (or argument)
    // and that is logging to a file with appender

    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    let stdout_layer = Registry::default()
        .with(stdout_log)
        .with(EnvFilter::from_default_env());

    let path = std::path::Path::new("./logs/events.log");
    let prefix = path.parent().unwrap();
    std::fs::create_dir_all(prefix).unwrap();

    // Note: This layer currently does not work
    // it should be modified to ship otel-compatible to
    // rolling logfiles
    let file_appender = rolling::daily("./logs", "events.log");
    let (file_appender, _guard) = tracing_appender::non_blocking(file_appender);

    let otel_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(file_appender);

    let subscriber = stdout_layer.with(otel_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

    Ok(())
}
