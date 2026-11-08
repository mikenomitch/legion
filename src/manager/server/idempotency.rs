//! Telling the loser of a concurrent idempotency-key race that it lost.
//!
//! A keyed mutation looks its key up before doing anything, so a retry that
//! arrives after the first request recorded is replayed. Two requests that
//! carry the same key *concurrently* both miss that lookup; the record is
//! `INSERT OR IGNORE`, so exactly one of them wins it, and
//! [`Store::idempotency_record`] says which. These helpers are how every
//! keyed RPC acts on that answer, so no caller is told `deduplicated: false`
//! when another request already holds its key.
//!
//! Two shapes, by where the record is written:
//!
//! - **After the mutation, as its own write** ([`record_idempotent`]): the
//!   loser's mutation has already run. Use it where running twice converges
//!   (a stop, a delete, a set to the same value). The loser is handed the
//!   winner's recorded message and `deduplicated: true`.
//! - **Inside the mutation's own store write** ([`claim_idempotency_key`]):
//!   a lost claim fails the write body with [`IdempotencyKeyTaken`], and
//!   the body runs in [`claimed_body`]'s savepoint, so the loser's changes
//!   roll back with it and nothing is done twice. The
//!   handler then replays the winner's record exactly as a sequential retry
//!   would be replayed.
//!
//! [`record_idempotent`]: ManagerDaemonService::record_idempotent

use super::*;

/// A write body's idempotency claim lost: another request recorded the same
/// key first. Returned from inside a store write so the body rolls back.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IdempotencyKeyTaken;

impl std::fmt::Display for IdempotencyKeyTaken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("idempotency key already recorded by a concurrent request")
    }
}

impl std::error::Error for IdempotencyKeyTaken {}

impl IdempotencyKeyTaken {
    /// Whether `err` is (or wraps) a lost claim.
    pub(crate) fn in_chain(err: &anyhow::Error) -> bool {
        err.chain()
            .any(|cause| cause.downcast_ref::<IdempotencyKeyTaken>().is_some())
    }
}

/// Records `message` under `key` from inside a store write body, failing
/// the body with [`IdempotencyKeyTaken`] when another request holds the key.
/// A blank key (no deduplication asked for) always succeeds.
pub(crate) fn claim_idempotency_key(
    store: &Store,
    key: &str,
    operation: &str,
    message: &str,
) -> anyhow::Result<()> {
    if store.idempotency_record(key, operation, message)? {
        Ok(())
    } else {
        Err(anyhow::Error::new(IdempotencyKeyTaken))
    }
}

/// Runs a write body that ends in [`claim_idempotency_key`] inside its own
/// savepoint, so a lost claim (or any other error) takes the body's earlier
/// writes with it. A store write does not roll its body back on error by
/// itself in every mode: in `local_disk` mode each statement commits as it
/// runs.
pub(crate) fn claimed_body<T>(store: &Store, body: impl FnOnce(&Store) -> R<T>) -> R<T> {
    store.within_savepoint(|store| Ok(body(store)))?
}

impl ManagerDaemonService {
    /// Records a finished keyed mutation's reply `message` under `key`, and
    /// returns what the caller should be told: `(message, false)` when this
    /// request's record stands, or `(the winner's recorded message, true)`
    /// when a concurrent request with the same key recorded first. Only for
    /// mutations where running twice converges (see the module docs).
    pub(crate) fn record_idempotent(
        &self,
        key: &str,
        operation: &str,
        message: String,
    ) -> Result<(String, bool), Status> {
        let first = self
            .store
            .idempotency_record(key, operation, &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        if first {
            return Ok((message, false));
        }
        debug!(operation, "Lost a concurrent idempotency-key race");
        // The winner's recorded reply is the authoritative one, and is what
        // a later retry of this key will be handed.
        let recorded = self.idempotency_winner(key)?;
        Ok((recorded.unwrap_or(message), true))
    }

    /// The reply recorded under `key` by the request that won it.
    pub(crate) fn idempotency_winner(&self, key: &str) -> Result<Option<String>, Status> {
        self.store
            .idempotency_lookup(key)
            .map_err(|e| map_store_error("Error checking idempotency", e))
    }

    /// For a write whose body claimed `key` with [`claim_idempotency_key`]:
    /// `Ok(Ok(value))` when it committed, `Ok(Err(recorded))` with the
    /// winner's record when the claim lost (the body rolled back), and the
    /// mapped store error otherwise.
    pub(crate) fn claimed_write<T>(
        &self,
        key: &str,
        action: &str,
        result: anyhow::Result<T>,
    ) -> Result<Result<T, String>, Status> {
        match result {
            Ok(value) => Ok(Ok(value)),
            Err(err) if IdempotencyKeyTaken::in_chain(&err) => {
                debug!(action, "Lost a concurrent idempotency-key race; replaying");
                // The winner committed before our body ran, so its record
                // is there; an empty one only if it was pruned in between.
                Ok(Err(self.idempotency_winner(key)?.unwrap_or_default()))
            }
            Err(err) => Err(map_store_error(action, err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::config::ManagerConfigFull;

    fn service() -> R<ManagerDaemonService> {
        let dir = tempfile::Builder::new()
            .prefix("legion-idempotency-")
            .tempdir_in(crate::test_scratch::root())?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("store path")?.to_string(),
            ..Default::default()
        })?;
        store.reset_all_tables()?;
        Ok(ManagerDaemonService::new(store))
    }

    /// The race's second recorder is the loser: it is told so, and handed
    /// the winner's reply, not its own.
    #[test]
    fn the_second_recorder_is_told_it_was_deduplicated() -> R<()> {
        let svc = service()?;
        let (message, deduplicated) = svc
            .record_idempotent("stop_job/k", "stop_job", "Stopped web".into())
            .map_err(|status| anyhow!("{status}"))?;
        assert_eq!((message.as_str(), deduplicated), ("Stopped web", false));
        let (message, deduplicated) = svc
            .record_idempotent("stop_job/k", "stop_job", "Stopped web (again)".into())
            .map_err(|status| anyhow!("{status}"))?;
        assert_eq!((message.as_str(), deduplicated), ("Stopped web", true));
        // No key asked for: never deduplicated.
        let (_, deduplicated) = svc
            .record_idempotent("", "stop_job", "Stopped web".into())
            .map_err(|status| anyhow!("{status}"))?;
        assert!(!deduplicated);
        Ok(())
    }

    /// A lost claim inside a write body rolls the body back, so the loser's
    /// mutation never lands, and the handler gets the winner's record.
    #[test]
    fn a_lost_claim_rolls_its_write_back() -> R<()> {
        let svc = service()?;
        let first = svc.store.scheduler_group_commit("test_create", |s| {
            claimed_body(s, |s| {
                claim_idempotency_key(s, "create/k", "create", "instance-a")?;
                Ok("instance-a".to_string())
            })
        });
        assert_eq!(
            svc.claimed_write("create/k", "Error creating", first)
                .map_err(|status| anyhow!("{status}"))?,
            Ok("instance-a".to_string())
        );
        // The loser's body makes a write of its own before it claims; it
        // must not survive the lost claim.
        let second = svc.store.scheduler_group_commit("test_create", |s| {
            claimed_body(s, |s| {
                s.idempotency_record("side-effect/b", "create", "instance-b")?;
                claim_idempotency_key(s, "create/k", "create", "instance-b")?;
                Ok("instance-b".to_string())
            })
        });
        assert_eq!(
            svc.claimed_write("create/k", "Error creating", second)
                .map_err(|status| anyhow!("{status}"))?,
            Err("instance-a".to_string())
        );
        assert_eq!(svc.store.idempotency_lookup("side-effect/b")?, None);
        Ok(())
    }

    #[test]
    fn lost_claim_is_found_through_context() {
        let err = anyhow::Error::new(IdempotencyKeyTaken).context("creating instance");
        assert!(IdempotencyKeyTaken::in_chain(&err));
        assert!(!IdempotencyKeyTaken::in_chain(&anyhow::anyhow!("other")));
    }
}
