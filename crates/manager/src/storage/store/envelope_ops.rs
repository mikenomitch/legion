//! Store half of elastic session envelopes (elastic-session-envelopes plan
//! §6.3; docs/session-mode.md, "Elastic envelopes").
//!
//! A node asks to grow or shrink one of its elastic envelopes on its
//! heartbeat. The resize is an in-place `UPDATE` of the envelope's
//! allocation row, so everything that reads rows sees it: the scheduler's
//! read model and free pools, plan apply's capacity recheck, preemption,
//! `legion node usage`, the session router, and the work stream that
//! re-sends the payload. It bumps the node's and the job config's
//! allocation epochs like a placement does, which fails the compare-and-set
//! of any plan that sampled the node before it and invalidates the node's
//! cached usage aggregate.
//!
//! **A grow is a placement of the difference**: it takes only free room,
//! recomputed from committed rows inside this transaction with the
//! envelope's current size given back, and takes what fits on each key when
//! not all of it does. A refused grow writes nothing, so it costs no raft
//! entry. **A shrink is trusted to the node**, which capped its own
//! admission at the target before asking (plan §6.4); the store only checks
//! the bounds.

use super::*;
use crate::structs::session::{amounts_json, elastic_bounds, envelope_serves, resource_amounts};

/// Resource amounts by key.
pub type EnvelopeAmounts = BTreeMap<String, f64>;

/// One resize a node asked for (`EnvelopeResize`), parsed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeResizeRequest {
    pub allocation_id: i64,
    /// The size the node believes is committed: a compare-and-set.
    pub from: EnvelopeAmounts,
    pub to: EnvelopeAmounts,
}

/// What a resize did (`EnvelopeResizeResult`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeResizeOutcome {
    /// `applied`, `partial`, `refused`, `stale`, `gone`, or `invalid`.
    pub outcome: String,
    /// The committed size after this answer, when there is a row to read.
    pub size: Option<EnvelopeAmounts>,
    pub message: String,
}

impl EnvelopeResizeOutcome {
    fn with_size(outcome: &str, size: &EnvelopeAmounts, message: impl Into<String>) -> Self {
        EnvelopeResizeOutcome {
            outcome: outcome.to_string(),
            size: Some(size.clone()),
            message: message.into(),
        }
    }

    fn without_size(outcome: &str, message: impl Into<String>) -> Self {
        EnvelopeResizeOutcome {
            outcome: outcome.to_string(),
            size: None,
            message: message.into(),
        }
    }
}

/// Whether two sizes hold the same keys at the same amounts.
fn same_size(a: &EnvelopeAmounts, b: &EnvelopeAmounts) -> bool {
    a.len() == b.len()
        && a.iter().all(|(key, amount)| {
            b.get(key)
                .is_some_and(|other| (other - amount).abs() < 1e-9)
        })
}

impl Store {
    /// Resize one of `node_id`'s envelopes as its node asked. Runs as a
    /// scheduler-lane write, group-committed with other small scheduler
    /// writes, and forwarded to the lane leader when this manager does not
    /// lead it (a heartbeat can land on any manager).
    #[instrument(skip_all, fields(node_id = %node_id, allocation_id = request.allocation_id))]
    pub fn resize_envelope(
        &self,
        node_id: &str,
        request: &EnvelopeResizeRequest,
    ) -> R<EnvelopeResizeOutcome> {
        match self.resize_envelope_local(node_id, request) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::ResizeEnvelope {
                        node_id: node_id.to_owned(),
                        request: request.clone(),
                    },
                    err,
                ),
            other => other,
        }
    }

    /// [`Self::resize_envelope`] without forwarding: what a forwarded
    /// `ResizeEnvelope` runs on the lane leader.
    pub(crate) fn resize_envelope_local(
        &self,
        node_id: &str,
        request: &EnvelopeResizeRequest,
    ) -> R<EnvelopeResizeOutcome> {
        let node_id = node_id.to_owned();
        let request = request.clone();
        self.scheduler_group_commit("resize_envelope", move |s| {
            s.resize_envelope_apply(&node_id, &request)
        })
    }

    fn resize_envelope_apply(
        &self,
        node_id: &str,
        request: &EnvelopeResizeRequest,
    ) -> R<EnvelopeResizeOutcome> {
        let mut conn = self.write_connection()?;
        let tx = Self::write_tx(&mut conn)?;
        let row: Option<(String, String, String, i64, Value)> = tx
            .prepare_cached(
                "SELECT node_id, status, job_config_id, task_config_id, resources \
                 FROM allocation WHERE id = ?1;",
            )?
            .query_row([request.allocation_id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .optional()?;
        let Some((row_node, status, job_config_id, task_config_id, resources)) = row else {
            return Ok(EnvelopeResizeOutcome::without_size(
                "gone",
                format!("allocation {} does not exist", request.allocation_id),
            ));
        };
        if row_node != node_id || crate::constants::allocation_status_is_terminal(&status) {
            return Ok(EnvelopeResizeOutcome::without_size(
                "gone",
                format!(
                    "allocation {} is not live on node {node_id}",
                    request.allocation_id
                ),
            ));
        }

        // The bounds come from the session task in the envelope's own job
        // config, never from the request: a node cannot size its envelope
        // past what the job allows.
        let tasks = self.shared_task_configs_for_job_config(&job_config_id)?;
        let Some(serves) = tasks
            .iter()
            .find(|(id, _)| *id == task_config_id)
            .and_then(|(_, task)| envelope_serves(task))
        else {
            return Ok(EnvelopeResizeOutcome::without_size(
                "gone",
                format!("allocation {} is not an envelope", request.allocation_id),
            ));
        };
        let Some(bounds) = tasks
            .iter()
            .find(|(_, task)| task.name == serves)
            .and_then(|(_, task)| elastic_bounds(task))
        else {
            return Ok(EnvelopeResizeOutcome::without_size(
                "invalid",
                format!("the envelope of {serves} is fixed: its session task sets no envelope.max"),
            ));
        };
        if !bounds.admits(&request.to) {
            return Ok(EnvelopeResizeOutcome::without_size(
                "invalid",
                format!(
                    "{} is outside the envelope's bounds ({} to {})",
                    amounts_json(&request.to),
                    amounts_json(&bounds.floor),
                    amounts_json(&bounds.max)
                ),
            ));
        }

        let current = resource_amounts(&resources);
        if same_size(&current, &request.to) {
            // A retried request whose first answer was lost.
            return Ok(EnvelopeResizeOutcome::with_size("applied", &current, ""));
        }
        if !same_size(&current, &request.from) {
            return Ok(EnvelopeResizeOutcome::with_size(
                "stale",
                &current,
                format!("the envelope holds {}", amounts_json(&current)),
            ));
        }
        let amount = |size: &EnvelopeAmounts, key: &str| size.get(key).copied().unwrap_or(0.0);
        let grows = request
            .to
            .iter()
            .all(|(key, to)| *to >= amount(&current, key));
        let shrinks = request
            .to
            .iter()
            .all(|(key, to)| *to <= amount(&current, key));
        if !grows && !shrinks {
            return Ok(EnvelopeResizeOutcome::with_size(
                "invalid",
                &current,
                "a resize either grows every key or shrinks every key",
            ));
        }

        let mut size = request.to.clone();
        if grows {
            let node = tx
                .prepare_cached(&GET_NODE)?
                .query_row([node_id], |row| self.node_from_row(row))
                .optional()?;
            let Some(node) = node else {
                return Ok(EnvelopeResizeOutcome::without_size(
                    "gone",
                    format!("node {node_id} does not exist"),
                ));
            };
            if node.availability != "available" {
                return Ok(EnvelopeResizeOutcome::with_size(
                    "refused",
                    &current,
                    format!(
                        "node {node_id} is {} and takes no new room",
                        node.availability
                    ),
                ));
            }
            // The node's free room with this envelope's current size given
            // back: what a placement of the new size would see.
            let rows = Self::collect_rows(
                tx.prepare_cached(
                    "SELECT id, resources FROM allocation \
                     WHERE node_id = ?1 AND id != ?2 AND status NOT IN ('complete', 'failed');",
                )?
                .query_map(params![node_id, request.allocation_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Value>(1)?))
                })?,
            )?;
            let pool = ResourcePool::free_from_rows(
                &node,
                rows.iter().map(|(id, resources)| (*id, resources)),
                &self.resource_policy,
            );
            for (key, wanted) in size.iter_mut() {
                if let Some(free) = pool.amount(key) {
                    *wanted = wanted.min(free.floor()).max(amount(&current, key));
                }
            }
            let fits = self
                .resource_policy
                .usage_request(&amounts_json(&size))
                .is_ok_and(|request| pool.can_fit(&request));
            if same_size(&size, &current) || !fits {
                return Ok(EnvelopeResizeOutcome::with_size(
                    "refused",
                    &current,
                    format!("node {node_id} has no free room to grow the envelope into"),
                ));
            }
        }

        tx.prepare_cached("UPDATE allocation SET resources = ?1 WHERE id = ?2;")?
            .execute(params![amounts_json(&size), request.allocation_id])?;
        self.bump_job_config_epoch_tx(&tx, &job_config_id)?;
        self.bump_scope_epoch_tx(&tx, "node", node_id)?;
        tx.commit()?;
        let outcome = if same_size(&size, &request.to) {
            "applied"
        } else {
            "partial"
        };
        Ok(EnvelopeResizeOutcome::with_size(outcome, &size, ""))
    }
}
