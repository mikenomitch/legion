//! `queue-ratio`: a strategy that sizes workers to drain a queue in time.
//!
//! A queue of `reading` items, worked at `rate` items per second by each
//! replica, drains within `drainWithin` with
//! `ceil(reading / (rate × drainWithin))` replicas. The built-in
//! `per-replica` strategy can't say "drain it in ten minutes"; this is the
//! kind of rule a strategy plugin is for.
//!
//! ```nickel
//! checks = [{
//!   name = "backlog", source = "team-prom", query = "sum(render_queue_depth)",
//!   strategy = { name = "queue-ratio", rate = 5, drainWithin = 600 },
//! }]
//! ```
//!
//! Build with `cargo build --release --example queue-ratio` (from
//! `examples/scaling-plugins/rust`) and check it with
//! `legion scaling plugin test target/release/examples/queue-ratio`.

use legion_scaling_plugin::{
    serve, Error, Initialize, Kind, Plugin, PluginInfo, Recommend, Recommendation,
};
use serde_json::json;

struct QueueRatio;

impl Plugin for QueueRatio {
    fn initialize(&mut self, _request: Initialize) -> Result<PluginInfo, Error> {
        Ok(
            PluginInfo::new("queue-ratio", env!("CARGO_PKG_VERSION"), &[Kind::Strategy])
                .with_schema(json!({
                    "type": "object",
                    "properties": {
                        "rate": { "type": "number", "exclusiveMinimum": 0 },
                        "drainWithin": { "type": "integer", "minimum": 1 }
                    },
                    "required": ["rate", "drainWithin"],
                    "additionalProperties": false
                })),
        )
    }

    fn recommend(&mut self, request: Recommend) -> Result<Recommendation, Error> {
        let reading = request
            .reading
            .ok_or_else(|| Error::invalid_params("queue-ratio reads the queue depth: give the check a query"))?;
        let rate = request.config["rate"].as_f64().unwrap_or(1.0);
        let seconds = request.config["drainWithin"].as_f64().unwrap_or(600.0);
        let per_replica = rate * seconds;
        let desired = (reading.max(0.0) / per_replica).ceil() as u32;
        Ok(Recommendation::new(
            desired,
            format!("{reading} queued, {per_replica} per replica within {seconds}s"),
        ))
    }
}

fn main() -> std::io::Result<()> {
    serve(QueueRatio)
}
