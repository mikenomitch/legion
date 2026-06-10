//! Scheduler event journal types.
//!
//! The manager records a `SchedulerEventRecord` whenever the scheduler does
//! something observable (eval queue activity, allocation changes, node and job
//! lifecycle transitions, rollout activity). Events are persisted to the
//! `scheduler_event` table for history/cursor queries and simultaneously
//! published on an in-process broadcast channel that backs the `StreamEvents`
//! gRPC stream. See `Store::emit_event`.

use serde::Serialize;
use serde_json::{json, Value};

/// Severity levels used by scheduler events.
pub const EVENT_LEVEL_INFO: &str = "info";
pub const EVENT_LEVEL_WARN: &str = "warn";
pub const EVENT_LEVEL_ERROR: &str = "error";

/// One persisted scheduler event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerEventRecord {
    pub id: i64,
    pub kind: String,
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_id: Option<i64>,
    pub message: String,
    pub metadata_json: String,
    pub created_at: String,
}

/// A scheduler event waiting to be recorded.
///
/// Built fluently at emission sites and handed to `Store::emit_event`:
///
/// ```ignore
/// store.emit_event(
///     EventDraft::info("eval.completed", "Eval 7 (job_submit) completed")
///         .job("demo")
///         .eval("7"),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct EventDraft {
    pub kind: String,
    pub level: String,
    pub message: String,
    pub job_id: Option<String>,
    pub node_id: Option<String>,
    pub eval_id: Option<String>,
    pub allocation_id: Option<i64>,
    pub rollout_id: Option<i64>,
    pub metadata: Value,
}

impl EventDraft {
    pub fn new(level: &str, kind: &str, message: impl Into<String>) -> Self {
        EventDraft {
            kind: kind.to_string(),
            level: level.to_string(),
            message: message.into(),
            job_id: None,
            node_id: None,
            eval_id: None,
            allocation_id: None,
            rollout_id: None,
            metadata: json!({}),
        }
    }

    pub fn info(kind: &str, message: impl Into<String>) -> Self {
        Self::new(EVENT_LEVEL_INFO, kind, message)
    }

    pub fn warn(kind: &str, message: impl Into<String>) -> Self {
        Self::new(EVENT_LEVEL_WARN, kind, message)
    }

    pub fn error(kind: &str, message: impl Into<String>) -> Self {
        Self::new(EVENT_LEVEL_ERROR, kind, message)
    }

    /// Scope the event to a job. Blank IDs are ignored.
    pub fn job(mut self, job_id: &str) -> Self {
        if !job_id.trim().is_empty() {
            self.job_id = Some(job_id.to_string());
        }
        self
    }

    /// Scope the event to a node. Blank IDs are ignored.
    pub fn node(mut self, node_id: &str) -> Self {
        if !node_id.trim().is_empty() {
            self.node_id = Some(node_id.to_string());
        }
        self
    }

    /// Scope the event to an eval. Blank IDs are ignored.
    pub fn eval(mut self, eval_id: &str) -> Self {
        if !eval_id.trim().is_empty() {
            self.eval_id = Some(eval_id.to_string());
        }
        self
    }

    pub fn allocation(mut self, allocation_id: i64) -> Self {
        self.allocation_id = Some(allocation_id);
        self
    }

    pub fn rollout(mut self, rollout_id: i64) -> Self {
        self.rollout_id = Some(rollout_id);
        self
    }

    /// Attach kind-specific structured details. Must be a JSON object.
    pub fn meta(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Filter applied when listing or streaming scheduler events.
#[derive(Debug, Clone, Default)]
pub struct SchedulerEventFilter {
    /// Exclusive lower-bound event ID.
    pub after_id: Option<i64>,
    /// Max events returned by a list call (default 100, clamped to 1..=1000).
    pub limit: Option<u32>,
    /// Kind filter: exact match or category prefix (`eval` matches `eval.*`).
    pub kind: Option<String>,
    pub job_id: Option<String>,
    pub node_id: Option<String>,
    pub level: Option<String>,
}

impl SchedulerEventFilter {
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(100).clamp(1, 1_000)
    }

    /// Whether a live event passes this filter. Mirrors the SQL applied by
    /// `Store::list_scheduler_events` so streamed and listed results agree.
    pub fn matches(&self, event: &SchedulerEventRecord) -> bool {
        if let Some(after_id) = self.after_id {
            if event.id <= after_id {
                return false;
            }
        }
        if let Some(kind) = self.kind.as_deref() {
            let prefix_match = event
                .kind
                .strip_prefix(kind)
                .is_some_and(|rest| rest.starts_with('.'));
            if event.kind != kind && !prefix_match {
                return false;
            }
        }
        if let Some(job_id) = self.job_id.as_deref() {
            if event.job_id.as_deref() != Some(job_id) {
                return false;
            }
        }
        if let Some(node_id) = self.node_id.as_deref() {
            if event.node_id.as_deref() != Some(node_id) {
                return false;
            }
        }
        if let Some(level) = self.level.as_deref() {
            if event.level != level {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: i64, kind: &str, level: &str) -> SchedulerEventRecord {
        SchedulerEventRecord {
            id,
            kind: kind.into(),
            level: level.into(),
            job_id: Some("demo".into()),
            node_id: None,
            eval_id: None,
            allocation_id: None,
            rollout_id: None,
            message: "test".into(),
            metadata_json: "{}".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn draft_builder_skips_blank_scopes() {
        let draft = EventDraft::info("eval.completed", "done")
            .job("  ")
            .node("node-1")
            .eval("9");
        assert_eq!(draft.job_id, None);
        assert_eq!(draft.node_id.as_deref(), Some("node-1"));
        assert_eq!(draft.eval_id.as_deref(), Some("9"));
        assert_eq!(draft.level, EVENT_LEVEL_INFO);
    }

    #[test]
    fn filter_kind_matches_exact_and_category_prefix() {
        let filter = SchedulerEventFilter {
            kind: Some("eval".into()),
            ..Default::default()
        };
        assert!(filter.matches(&record(1, "eval.completed", "info")));
        assert!(filter.matches(&record(2, "eval", "info")));
        assert!(!filter.matches(&record(3, "evaluation.completed", "info")));
        assert!(!filter.matches(&record(4, "job.submitted", "info")));
    }

    #[test]
    fn filter_applies_after_id_level_and_scopes() {
        let filter = SchedulerEventFilter {
            after_id: Some(5),
            level: Some("warn".into()),
            job_id: Some("demo".into()),
            ..Default::default()
        };
        assert!(filter.matches(&record(6, "node.lost", "warn")));
        assert!(!filter.matches(&record(5, "node.lost", "warn")));
        assert!(!filter.matches(&record(6, "node.lost", "info")));

        let other_job = SchedulerEventRecord {
            job_id: Some("other".into()),
            ..record(7, "node.lost", "warn")
        };
        assert!(!filter.matches(&other_job));
    }

    #[test]
    fn effective_limit_defaults_and_clamps() {
        assert_eq!(SchedulerEventFilter::default().effective_limit(), 100);
        let big = SchedulerEventFilter {
            limit: Some(9_999),
            ..Default::default()
        };
        assert_eq!(big.effective_limit(), 1_000);
        let zero = SchedulerEventFilter {
            limit: Some(0),
            ..Default::default()
        };
        assert_eq!(zero.effective_limit(), 1);
    }
}
