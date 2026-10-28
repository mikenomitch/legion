//! Scheduler-lane group commit for small instance-lifecycle writes
//! (sandbox-scaling plan, Phase 1).
//!
//! Every keyed-instance RPC used to be its own raft entry: a create, a
//! suspend, a resume, a destroy, each node's snapshot reports. They are
//! single-row writes on one lane, so concurrent callers can share one
//! entry the way plan applies and eval enqueues already do. A caller hands
//! [`Store::scheduler_group_commit`] the closure it would have passed to
//! [`Store::replicated`]; whoever wins the commit slot runs every queued
//! closure in one scheduler-lane op, each inside its own savepoint, and
//! hands each caller its own result.
//!
//! **Isolation is per request.** A closure that fails rolls back to its own
//! savepoint and fails only its caller; its neighbours commit. A failure of
//! the whole op (a lost leadership, a capture-time ownership violation)
//! fails every member, exactly as it would have failed each of them alone.
//! A batch of one runs as a plain [`Store::replicated`] op under its own op
//! name, so a lone caller pays nothing extra and the op-count metrics keep
//! naming it.
//!
//! **Only scheduler-lane writes belong here.** Under `topology = lanes` a
//! batch is one scheduler-lane changeset, so a closure that writes another
//! lane's tables fails the ownership guard for everyone in its batch. The
//! commutative tables (`scheduler_event`, `idempotency_record`) are fine,
//! which is what lets an op carry its own event and idempotency record.

use super::*;
use std::any::Any;

/// The erased closure a group-commit caller submits.
pub(crate) type GroupCommitOp = Box<dyn FnOnce(&Store) -> R<Box<dyn Any + Send>> + Send>;

/// One queued request: the op name it would have committed under alone,
/// and its closure.
pub(crate) struct GroupCommitRequest {
    pub(crate) op: &'static str,
    pub(crate) run: GroupCommitOp,
}

/// What a request's closure returned, still erased.
pub(crate) type GroupCommitResult = Box<dyn Any + Send>;

/// Cap on closures per group-commit entry. These are single-row writes
/// (an instance row, a snapshot row, an eval row, an event), so 64 keeps
/// the entry small while still absorbing a burst of concurrent callers.
pub(crate) const GROUP_COMMIT_BATCH_MAX: usize = 64;

impl Store {
    /// Run `f` as a scheduler-lane write, batched with whatever other
    /// scheduler-lane group-commit writes are queued at the same moment.
    ///
    /// Semantically identical to `self.replicated(Lane::Scheduler, op, f)`
    /// for the caller: `f` runs once, its effects commit atomically or not
    /// at all, and its return value comes back. Inside an open capture it
    /// runs inline, like any nested store write.
    pub(crate) fn scheduler_group_commit<T: Send + 'static>(
        &self,
        op: &'static str,
        f: impl FnOnce(&Store) -> R<T> + Send + 'static,
    ) -> R<T> {
        if capture::in_capture() || capture::in_local_projection() || !self.group_commit_enabled() {
            return self.replicated(Lane::Scheduler, op, f);
        }
        let request = GroupCommitRequest {
            op,
            run: Box::new(move |store: &Store| {
                f(store).map(|value| Box::new(value) as Box<dyn Any + Send>)
            }),
        };
        let coalescer = Arc::clone(&self.group_commit_coalescer);
        let erased = coalescer.submit(request, |batch| self.run_group_commit_batch(batch))?;
        erased
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| anyhow!("group-commit op {op} returned a value of an unexpected type"))
    }

    /// `schedulerSettings.instance_group_commit` — default true. Off runs
    /// every instance-lifecycle write as its own entry, as before Phase 1.
    fn group_commit_enabled(&self) -> bool {
        self.scheduler_settings
            .as_ref()
            .and_then(|settings| settings.get("instance_group_commit"))
            .and_then(Value::as_bool)
            .unwrap_or(true)
    }

    fn run_group_commit_batch(&self, batch: Vec<GroupCommitRequest>) -> Vec<R<GroupCommitResult>> {
        if batch.len() == 1 {
            let request = batch.into_iter().next().expect("one request");
            return vec![self.replicated(Lane::Scheduler, request.op, request.run)];
        }
        let size = batch.len();
        let outcome = self.replicated(Lane::Scheduler, "group_commit", move |s| {
            let mut results = Vec::with_capacity(batch.len());
            for request in batch {
                s.group_commit_savepoint("SAVEPOINT legion_group_item;")?;
                match (request.run)(s) {
                    Ok(value) => {
                        s.group_commit_savepoint("RELEASE legion_group_item;")?;
                        results.push(Ok(value));
                    }
                    Err(err) => {
                        s.group_commit_savepoint(
                            "ROLLBACK TO legion_group_item; RELEASE legion_group_item;",
                        )?;
                        results.push(Err(err));
                    }
                }
            }
            Ok(results)
        });
        match outcome {
            Ok(results) => results,
            Err(err) => {
                // The op itself failed, so nothing in it committed: every
                // member gets the error it would have got alone. A typed
                // `NotRaftLeader` stays typed for every member, so each
                // caller's leader hint and forwarding still work.
                let message = format!("{err:#}");
                let not_leader = err
                    .chain()
                    .find_map(|cause| {
                        cause.downcast_ref::<crate::storage::raft::driver::NotRaftLeader>()
                    })
                    .cloned();
                (0..size)
                    .map(|_| match &not_leader {
                        Some(typed) => {
                            Err(anyhow::Error::new(typed.clone()).context(message.clone()))
                        }
                        None => Err(anyhow!("{message}")),
                    })
                    .collect()
            }
        }
    }

    /// Run `f` inside its own savepoint: kept when it returns `Ok(Ok(_))`,
    /// rolled back when it returns `Ok(Err(_))` (a refusal that must leave
    /// no trace, such as a rejected ratification) or an error. Only
    /// meaningful inside an op body.
    pub(crate) fn within_savepoint<T, E>(
        &self,
        f: impl FnOnce(&Store) -> R<Result<T, E>>,
    ) -> R<Result<T, E>> {
        self.group_commit_savepoint("SAVEPOINT legion_scoped;")?;
        match f(self) {
            Ok(Ok(value)) => {
                self.group_commit_savepoint("RELEASE legion_scoped;")?;
                Ok(Ok(value))
            }
            Ok(Err(refusal)) => {
                self.group_commit_savepoint("ROLLBACK TO legion_scoped; RELEASE legion_scoped;")?;
                Ok(Err(refusal))
            }
            Err(err) => {
                self.group_commit_savepoint("ROLLBACK TO legion_scoped; RELEASE legion_scoped;")?;
                Err(err)
            }
        }
    }

    /// Savepoint control between group-commit members. The write connection
    /// is taken only for the statement: each member's closure takes it
    /// again for its own writes.
    fn group_commit_savepoint(&self, sql: &str) -> R<()> {
        let conn = self.write_connection()?;
        conn.execute_batch(sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod group_commit_test {
    use super::*;
    use crate::structs::job::{JobConfig, StatefulConfig, TaskConfig};
    use crate::structs::{InstanceMode, Submission};

    fn keyed_store() -> R<(tempfile::TempDir, Store)> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&crate::structs::config::ManagerConfigFull {
            store_path: dir.path().to_str().context("utf-8")?.to_string(),
            ..Default::default()
        })?;
        let task = TaskConfig {
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("workspace", "noop")
        };
        store.handle_job_submit(
            &JobConfig {
                priority: Default::default(),
                rollout: None,
                rollout_by_group: Default::default(),
                name: "agents".into(),
                tasks: vec![task],
                version: None,
                pending_extensions: Default::default(),
            },
            &[Submission {
                path: "agents.json".into(),
                content: "{}".into(),
                is_primary: true,
            }],
        )?;
        Ok((dir, store))
    }

    fn create(id: &'static str, fail: bool) -> GroupCommitRequest {
        GroupCommitRequest {
            op: "create_instance",
            run: Box::new(move |s: &Store| {
                s.create_keyed_instance("agents", "default", "workspace", Some(id))?;
                if fail {
                    bail!("refused after writing");
                }
                Ok(Box::new(id) as Box<dyn Any + Send>)
            }),
        }
    }

    /// A batch commits its members together, and a member that fails rolls
    /// back alone: its write is gone and its neighbours' are not (plan
    /// §10, Phase 1).
    #[test]
    fn a_failing_member_rolls_back_alone() -> R<()> {
        let (_dir, store) = keyed_store()?;
        let results = store.run_group_commit_batch(vec![
            create("user-1", false),
            create("user-2", true),
            create("user-3", false),
        ]);
        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_ok());
        assert!(store.get_instance("user-1")?.is_some());
        assert!(store.get_instance("user-2")?.is_none());
        assert!(store.get_instance("user-3")?.is_some());
        Ok(())
    }

    /// A refusal inside `within_savepoint` leaves no trace, and a success
    /// keeps its writes.
    #[test]
    fn within_savepoint_keeps_only_successes() -> R<()> {
        let (_dir, store) = keyed_store()?;
        let refused: Result<(), &str> = store.scheduler_group_commit("test", |s| {
            s.within_savepoint(|s| {
                s.create_keyed_instance("agents", "default", "workspace", Some("user-9"))?;
                Ok(Err("no room"))
            })
        })?;
        assert_eq!(refused, Err("no room"));
        assert!(store.get_instance("user-9")?.is_none());
        let kept: Result<(), &str> = store.scheduler_group_commit("test", |s| {
            s.within_savepoint(|s| {
                s.create_keyed_instance("agents", "default", "workspace", Some("user-9"))?;
                Ok(Ok(()))
            })
        })?;
        assert!(kept.is_ok());
        assert!(store.get_instance("user-9")?.is_some());
        Ok(())
    }
}
