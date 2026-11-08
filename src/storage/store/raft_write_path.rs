//! The store-side half of the replicated write path
//! (`internal_docs/plans/raft-write-path-plan.md`):
//!
//! - the **purge barrier** ([`Store::sync_state_db_to_disk`]): the state DB
//!   runs `synchronous = NORMAL`, and the raft log store calls this before
//!   every purge so the applied marker a purge relies on is durable first
//!   (plan §6.1);
//! - **replicated data migrations** ([`Store::run_data_migrations`], plan
//!   §6.5; the steps themselves are in `storage::data_migrations`).

use std::path::Path;

use crate::prelude::*;
use crate::storage::raft::Lane;
use crate::storage::store::Store;

impl Store {
    /// Run every pending data step as ONE replicated control-lane op: the
    /// steps' rewrites and their `data_migration` records ride a single raft
    /// entry, so every replica applies the same rows at one log index
    /// (`storage::data_migrations` module docs). Pending is read inside the
    /// capture, so a step runs exactly once cluster-wide. Returns the ids
    /// that ran (none when nothing was pending). In raft mode this must run
    /// on the control-lane leader, and only once every voter can apply the
    /// result — the driver's data-migration task checks both.
    pub fn run_data_migrations(&self) -> R<Vec<String>> {
        self.replicated(Lane::Control, "run_data_migrations", |store| {
            let mut conn = store.write_connection()?;
            let tx = Store::write_tx(&mut conn)?;
            let ran = crate::storage::data_migrations::run_pending_on(&tx)?;
            tx.commit()?;
            Ok(ran.into_iter().map(str::to_string).collect())
        })
    }

    /// How many data steps this replica has not applied yet.
    pub(crate) fn data_migrations_pending(&self) -> R<usize> {
        let conn = self.read_connection()?;
        Ok(crate::storage::data_migrations::pending_on(&conn)?.len())
    }

    /// Startup: a local store runs its pending data steps now (it is the
    /// only replica, so there is no log position to agree on). Under raft
    /// the driver's data-migration task runs them on the control-lane
    /// leader instead. Logs failures; never fails startup.
    pub(crate) fn run_data_migrations_at_start(&self) {
        // A streaming standby does not write; its primary migrates the
        // database it ships.
        if self.raft_mode() || self.persistence_driver.write_refusal().is_some() {
            return;
        }
        match self.run_data_migrations() {
            Ok(ran) if ran.is_empty() => {}
            Ok(ran) => info!(steps = ?ran, "Ran data migrations"),
            Err(err) => warn!(error = ?err, "Data migrations failed; they retry at the next start"),
        }
    }

    /// Make every transaction committed to the state DB so far durable:
    /// fsync its WAL, its main file, and the directory holding them.
    ///
    /// The state DB runs `synchronous = NORMAL`, so a commit has written its
    /// frames to the WAL (they are in the page cache of the WAL's inode) but
    /// not fsynced them. A checkpoint only resets the WAL after it has copied
    /// the frames into the main file and fsynced that, so after an fsync of
    /// the WAL every committed transaction survives a power loss. The main
    /// file and the directory are synced too: the main file is cheap when
    /// clean, and the directory covers a WAL created since the last sync.
    ///
    /// Called by the raft log store before every purge (the purge barrier),
    /// so openraft's startup reapply can never find the applied marker below
    /// the purged log. Takes no SQLite lock: it runs on the raft core's log
    /// I/O path, which must never wait on the capture mutex.
    pub(crate) fn sync_state_db_to_disk(&self) -> R<()> {
        sync_state_files(self.state_db_path())
    }
}

/// [`Store::sync_state_db_to_disk`] for the state DB at `db_path`. A WAL
/// that does not exist (checkpointed and removed after the last connection
/// closed) has nothing to sync.
pub(crate) fn sync_state_files(db_path: &Path) -> R<()> {
    let wal = db_path.with_extension("sqlite-wal");
    match std::fs::File::open(&wal) {
        Ok(file) => file
            .sync_all()
            .with_context(|| format!("failed fsyncing {}", wal.display()))?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed opening {}", wal.display()));
        }
    }
    std::fs::File::open(db_path)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("failed fsyncing {}", db_path.display()))?;
    #[cfg(unix)]
    if let Some(dir) = db_path.parent() {
        std::fs::File::open(dir)
            .and_then(|file| file.sync_all())
            .with_context(|| format!("failed fsyncing directory {}", dir.display()))?;
    }
    Ok(())
}
