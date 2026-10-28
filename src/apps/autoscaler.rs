//! The bundled autoscaler app's body (`legion app run autoscaler`;
//! autoscaler-v2 plan §6.9).
//!
//! Phase 2 of the autoscaler plan wires the engine (`crate::autoscaler`) in
//! here: read policies and scaling inputs through [`AppContext::client`],
//! decide, and act within the app's grant. Until then the body only reports
//! that the engine is not wired, stays healthy so the app shows `running`,
//! and stops cleanly when asked.

use crate::apps::{AppContext, AppFuture};
use crate::prelude::*;

/// The app body [`crate::apps::BUNDLED`] registers.
pub fn run(context: AppContext) -> AppFuture {
    Box::pin(run_body(context))
}

async fn run_body(mut context: AppContext) -> R<()> {
    let plugins = context
        .config()
        .get("plugins")
        .and_then(Value::as_object)
        .map(|plugins| plugins.len())
        .unwrap_or(0);
    info!(
        app = context.name(),
        plugins,
        legion = context.legion_addr().is_some(),
        "The autoscaler app is running, but its engine is not wired in this build; \
         it scales nothing yet"
    );
    context.set_health(true, "ok: engine not wired yet");
    context.stopped().await;
    info!(app = context.name(), "The autoscaler app is stopping");
    Ok(())
}
