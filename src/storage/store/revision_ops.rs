//! The store side of revisions and watches (revisions-and-watches plan;
//! docs/watches-and-blocking-queries.md): where a revision comes from, how
//! the changeset projection feeds the journal, and the waits built on it.
//!
//! The journal itself is [`crate::storage::revisions::RevisionHub`]. This
//! module decides what a revision *is* on this store — the `raft_applied`
//! markers in raft mode, a persisted counter in `local_disk` mode — and
//! turns a committed changeset into the jobs, allocations, and KV keys it
//! changed.

use super::*;
use crate::storage::revisions::{ChangeKind, ObjectChange, Revision, RevisionHub};

/// The group index a lane's applied marker fills in a [`Revision`]
/// (`Lane::ALL` order).
fn lane_group(lane: Lane) -> usize {
    match lane {
        Lane::Control => 0,
        Lane::Scheduler => 1,
        Lane::Status => 2,
    }
}

/// What one committed changeset adds to the revision journal
/// ([`Store::prepare_revision_record`]).
#[derive(Debug)]
pub(crate) enum RevisionRecord {
    /// It touched nothing the journal tracks.
    Nothing,
    /// Record `changes` at `(group, index)`.
    Record {
        group: usize,
        index: u64,
        alloc_group: usize,
        changes: Vec<ObjectChange>,
    },
    /// It could not be read: restart the journal.
    Failed,
}

/// How long [`Store::wait_for_revision`] sleeps between looks at the
/// applied markers when no journal record wakes it first (blank and
/// membership entries advance the markers without recording anything).
const REVISION_POLL: std::time::Duration = std::time::Duration::from_millis(5);

impl Store {
    /// The change journal behind blocking reads, `Watch`, and `StreamWork`
    /// resume.
    pub fn revisions(&self) -> Arc<RevisionHub> {
        Arc::clone(&self.revisions)
    }

    /// Whether revisions come from raft applied markers (raft persistence)
    /// rather than the `local_disk` counter. Known before the engine starts.
    fn revisions_from_raft(&self) -> bool {
        self.persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
            .is_some()
    }

    /// Open the journal at this store's current revision, and seed the
    /// event-id floor from the journal on disk. Called once at start, after
    /// migrations and before the raft engine replays anything. Never fails:
    /// a revision that cannot be read starts the journal at zero, which only
    /// makes early resumes resync.
    pub(crate) fn start_revisions(&self) {
        if !self.revisions_from_raft() {
            let reserved: Option<i64> = self.read_connection().ok().and_then(|conn| {
                conn.query_row(
                    "SELECT reserved FROM store_revision WHERE id = 1;",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .ok()
                .flatten()
            });
            self.revisions
                .start_local(reserved.unwrap_or(0).max(0) as u64);
        }
        match self.current_revision() {
            Ok(at) => self.revisions.reset(at),
            Err(err) => warn!(error = ?err, "Could not read the store's revision at start"),
        }
        if let Err(err) = self.seed_event_id_floor() {
            warn!(error = ?err, "Could not seed the event id floor from the journal");
        }
    }

    /// Every lane's applied index, as the state machines recorded them.
    /// Empty outside raft mode and on a replica that applied nothing.
    fn applied_lanes(&self) -> R<HashMap<Lane, u64>> {
        let conn = self.read_connection()?;
        let mut stmt = conn.prepare_cached("SELECT lane, last_applied FROM raft_applied;")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<Vec<u8>>>(1)?))
        })?;
        let mut applied = HashMap::new();
        for row in rows {
            let (lane, bytes) = row?;
            let (Some(lane), Some(bytes)) = (Lane::from_wire_name(&lane), bytes) else {
                continue;
            };
            let log_id: openraft::LogId<u64> = serde_json::from_slice(&bytes)
                .context("failed decoding raft_applied last_applied")?;
            applied.insert(lane, log_id.index);
        }
        Ok(applied)
    }

    /// This replica's revision now: every change committed at or below it
    /// is in what this manager reads. Each raft group's applied index
    /// (`raft_applied`, committed with the rows), or the `local_disk`
    /// counter.
    pub fn current_revision(&self) -> R<Revision> {
        if !self.revisions_from_raft() {
            return Ok(Revision::single(self.revisions.local()));
        }
        let mut revision = Revision::default();
        for (lane, index) in self.applied_lanes()? {
            revision.raise(lane_group(lane), index);
        }
        Ok(revision)
    }

    /// [`Self::current_revision`], rendered, for a reply. An unreadable
    /// revision is reported as empty rather than failing a read that
    /// otherwise succeeded.
    pub fn current_revision_string(&self) -> String {
        match self.current_revision() {
            Ok(revision) => revision.render(),
            Err(err) => {
                debug!(error = ?err, "Could not read the store's revision for a reply");
                String::new()
            }
        }
    }

    /// Wait until this replica's revision covers `target` (read-after-write
    /// through a follower, a blocking read whose revision came from a newer
    /// manager). `true` when it does, `false` on timeout.
    pub async fn wait_for_revision(&self, target: &Revision, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut wake = self.revisions.subscribe();
        loop {
            match self.current_revision() {
                Ok(current) if current.covers(target) => return true,
                Ok(_) => {}
                Err(err) => {
                    debug!(error = ?err, "Could not read the store's revision while waiting");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::select! {
                _ = wake.changed() => {}
                _ = tokio::time::sleep(REVISION_POLL) => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// A linearizable-read barrier (revisions-and-watches plan §6): on the
    /// leader, confirm leadership with a quorum and wait until everything
    /// committed before now has applied here; on a follower, the typed
    /// not-the-leader error (which the leader-forward layer replays on the
    /// leader). Every lane's group is checked, since a job read spans all
    /// three. A no-op outside raft mode, where this manager is the only
    /// writer. Blocking: call it off the async runtime.
    pub fn linearizable_read_barrier(&self) -> R<()> {
        let Some(driver) = self
            .persistence_driver
            .as_any()
            .downcast_ref::<crate::storage::raft::driver::MultiRaftDriver>()
        else {
            return Ok(());
        };
        for lane in Lane::ALL {
            driver.read_barrier(lane)?;
        }
        Ok(())
    }

    /// The latest spec version of `job_id` (`job_config.version`), or 0 when
    /// the job has no config. The value `RunJobRequest.expected_version`
    /// compares against.
    pub fn latest_job_version(&self, job_id: &str) -> R<i64> {
        let conn = self.read_connection()?;
        let version: Option<i64> = conn
            .prepare_cached("SELECT MAX(version) FROM job_config WHERE job_id = ?1;")?
            .query_row([job_id], |row| row.get(0))?;
        Ok(version.unwrap_or(0))
    }

    /// Restart the journal at the current revision: every open watch
    /// resyncs, and every node's resume window closes. After a snapshot
    /// install replaced rows under the projection, or a projection failure.
    pub(crate) fn reset_revisions(&self) {
        let at = self
            .current_revision()
            .unwrap_or_else(|_| self.revisions.recorded());
        self.revisions.reset(at.max(&self.revisions.recorded()));
    }

    /// Resolve what one committed changeset adds to the journal: the jobs,
    /// allocations, and KV keys it changed, and the revision it committed
    /// at. DB reads only; [`Self::publish_revision_record`] publishes it
    /// under the journal's publish lock, together with the work log's notes
    /// (`work`), so a `StreamWork` message tagged with the journal's
    /// position never claims a change the log has not seen, and a worker
    /// resuming here misses none. Never fails: a changeset it cannot read
    /// becomes [`RevisionRecord::Failed`], which restarts the journal.
    pub(super) fn prepare_revision_record(
        &self,
        bytes: &[u8],
        work: &WorkChanges,
    ) -> RevisionRecord {
        let prepared = (|| -> R<RevisionRecord> {
            let (lane, changes) = self.watched_changes(bytes)?;
            if changes.is_empty()
                && work.pairs.is_empty()
                && work.unknown_nodes.is_empty()
                && !work.unattributed
            {
                return Ok(RevisionRecord::Nothing);
            }
            let (group, index, alloc_group) = if self.revisions_from_raft() {
                let applied = self.applied_lanes()?;
                // `lanes` topology keeps one applied marker per lane; under
                // `single` only the control group's exists, and it carries
                // every lane's entries. The marker of the lane being applied
                // is written in the same transaction as its rows, so it
                // exists by the time its changeset is projected here.
                let lanes =
                    applied.contains_key(&Lane::Scheduler) || applied.contains_key(&Lane::Status);
                let lane = lane.unwrap_or(Lane::Scheduler);
                let physical = if lanes { lane } else { Lane::Control };
                let index = applied.get(&physical).copied().unwrap_or(0);
                (
                    lane_group(physical),
                    index,
                    if lanes {
                        lane_group(Lane::Scheduler)
                    } else {
                        0
                    },
                )
            } else {
                // `local_disk`: projections are serialized by the capture
                // mutex, so the counter moves in commit order.
                let (next, persist) = self.revisions.next_local();
                if let Some(bound) = persist {
                    let conn = self.write_connection()?;
                    conn.execute(
                        "INSERT INTO store_revision (id, reserved) VALUES (1, ?1)
                         ON CONFLICT(id) DO UPDATE SET reserved = excluded.reserved;",
                        [bound as i64],
                    )?;
                }
                (0, next, 0)
            };
            Ok(RevisionRecord::Record {
                group,
                index,
                alloc_group,
                changes,
            })
        })();
        prepared.unwrap_or_else(|err| {
            warn!(
                error = ?err,
                "Could not record a committed changeset in the revision journal; restarting \
                 the journal (open watches resync)"
            );
            RevisionRecord::Failed
        })
    }

    /// Publish a prepared record (see [`Self::prepare_revision_record`]).
    /// Memory only; called under the journal's publish lock.
    pub(super) fn publish_revision_record(&self, record: RevisionRecord, work: &WorkChanges) {
        match record {
            RevisionRecord::Nothing => {}
            RevisionRecord::Failed => self.reset_revisions(),
            RevisionRecord::Record {
                group,
                index,
                alloc_group,
                changes,
            } => {
                if work.unattributed {
                    self.revisions.forget_nodes(Revision([index; 3]));
                }
                self.revisions.record(
                    group,
                    index,
                    alloc_group,
                    changes,
                    &work.pairs,
                    &work.unknown_nodes,
                );
            }
        }
    }

    /// The jobs, allocations, and KV keys `bytes` changed, and the lane the
    /// changeset belongs to (by its tables' single writer), when it touched
    /// any of them. One object per key: several rows of one job make one
    /// job change. Rows an UPDATE record does not name (unchanged columns
    /// are absent) are re-read by primary key.
    fn watched_changes(&self, bytes: &[u8]) -> R<(Option<Lane>, Vec<ObjectChange>)> {
        use fallible_streaming_iterator::FallibleStreamingIterator;
        use rusqlite::hooks::Action;
        use rusqlite::session::ChangesetIter;

        let mut out: BTreeMap<(u8, String), ObjectChange> = BTreeMap::new();
        let mut lane: Option<Lane> = None;
        if bytes.is_empty() {
            return Ok((None, Vec::new()));
        }
        let mut column_cache: HashMap<String, Vec<String>> = HashMap::new();
        // Allocations whose job config is known, to resolve to jobs after the
        // scan, and ones an UPDATE record did not fully name.
        let mut allocations: Vec<(i64, Option<String>, Option<String>, bool)> = Vec::new();
        let mut job_config_rereads: Vec<String> = Vec::new();
        let mut task_status_rereads: Vec<i64> = Vec::new();

        let reader: &mut dyn std::io::Read = &mut &*bytes;
        let mut iter = ChangesetIter::start_strm(&reader)
            .context("failed opening changeset iterator for the revision journal")?;
        while let Some(item) = iter
            .next()
            .context("failed iterating changeset for the revision journal")?
        {
            let op = item.op()?;
            let table = op.table_name().to_string();
            if !matches!(
                table.as_str(),
                "job" | "job_config" | "task_status" | "allocation" | "kv_entry"
            ) {
                continue;
            }
            let action = op.code();
            let deleted = action == Action::SQLITE_DELETE;
            // The value of a column this record carries: the new row of an
            // insert, the old row otherwise (always present for the primary
            // key; for other columns only when they changed), then the new
            // row of an update.
            let mut column = |name: &str| -> R<Option<rusqlite::types::Value>> {
                let idx = self.changeset_column_index(&mut column_cache, &table, name)?;
                let value = match action {
                    Action::SQLITE_INSERT => item.new_value(idx).ok(),
                    Action::SQLITE_DELETE => item.old_value(idx).ok(),
                    _ => item
                        .old_value(idx)
                        .ok()
                        .or_else(|| item.new_value(idx).ok()),
                };
                Ok(value.map(rusqlite::types::Value::from))
            };
            let as_text = |value: Option<rusqlite::types::Value>| match value {
                Some(rusqlite::types::Value::Text(text)) => Some(text),
                _ => None,
            };
            let as_int = |value: Option<rusqlite::types::Value>| match value {
                Some(rusqlite::types::Value::Integer(id)) => Some(id),
                _ => None,
            };
            match table.as_str() {
                "job" => {
                    lane.get_or_insert(Lane::Control);
                    if let Some(id) = as_text(column("id")?) {
                        out.insert(
                            (0, id.clone()),
                            ObjectChange {
                                kind: ChangeKind::Job,
                                key: id.clone(),
                                job: Some(id),
                                node: None,
                                deleted,
                            },
                        );
                    }
                }
                "job_config" => {
                    lane.get_or_insert(Lane::Control);
                    match as_text(column("job_id")?) {
                        Some(job) => {
                            out.entry((0, job.clone())).or_insert(ObjectChange {
                                kind: ChangeKind::Job,
                                key: job.clone(),
                                job: Some(job),
                                node: None,
                                deleted: false,
                            });
                        }
                        None => {
                            if let Some(id) = as_text(column("id")?) {
                                job_config_rereads.push(id);
                            }
                        }
                    }
                }
                "task_status" => {
                    lane.get_or_insert(Lane::Status);
                    match as_text(column("job_id")?) {
                        Some(job) => {
                            out.entry((0, job.clone())).or_insert(ObjectChange {
                                kind: ChangeKind::Job,
                                key: job.clone(),
                                job: Some(job),
                                node: None,
                                deleted: false,
                            });
                        }
                        None => {
                            if let Some(id) = as_int(column("task_config_id")?) {
                                task_status_rereads.push(id);
                            }
                        }
                    }
                }
                "allocation" => {
                    lane.get_or_insert(Lane::Scheduler);
                    let Some(id) = as_int(column("id")?) else {
                        continue;
                    };
                    let node = as_text(column("node_id")?);
                    let job_config = as_text(column("job_config_id")?);
                    allocations.push((id, node, job_config, deleted));
                }
                _ => {
                    lane.get_or_insert(Lane::Control);
                    if let Some(key) = as_text(column("key")?) {
                        out.insert(
                            (2, key.clone()),
                            ObjectChange {
                                kind: ChangeKind::Kv,
                                key,
                                job: None,
                                node: None,
                                deleted,
                            },
                        );
                    }
                }
            }
        }
        drop(iter);

        if job_config_rereads.is_empty() && task_status_rereads.is_empty() && allocations.is_empty()
        {
            return Ok((lane, out.into_values().collect()));
        }
        let conn = self.read_connection()?;
        let mut job_of_config: HashMap<String, Option<String>> = HashMap::new();
        let mut resolve_config = |config: &str| -> R<Option<String>> {
            if let Some(job) = job_of_config.get(config) {
                return Ok(job.clone());
            }
            let job: Option<String> = conn
                .prepare_cached("SELECT job_id FROM job_config WHERE id = ?1;")?
                .query_row([config], |row| row.get(0))
                .optional()?;
            job_of_config.insert(config.to_string(), job.clone());
            Ok(job)
        };
        let job_change = |job: String| ObjectChange {
            kind: ChangeKind::Job,
            key: job.clone(),
            job: Some(job),
            node: None,
            deleted: false,
        };
        for config in job_config_rereads {
            if let Some(job) = resolve_config(&config)? {
                out.entry((0, job.clone()))
                    .or_insert_with(|| job_change(job));
            }
        }
        for task_config_id in task_status_rereads {
            let job: Option<String> = conn
                .prepare_cached("SELECT job_id FROM task_status WHERE task_config_id = ?1;")?
                .query_row([task_config_id], |row| row.get(0))
                .optional()?;
            if let Some(job) = job {
                out.entry((0, job.clone()))
                    .or_insert_with(|| job_change(job));
            }
        }
        for (id, node, job_config, deleted) in allocations {
            let (node, job_config) = if node.is_some() && job_config.is_some() {
                (node, job_config)
            } else {
                let row: Option<(String, String)> = conn
                    .prepare_cached("SELECT node_id, job_config_id FROM allocation WHERE id = ?1;")?
                    .query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))
                    .optional()?;
                match row {
                    Some((row_node, row_config)) => {
                        (node.or(Some(row_node)), job_config.or(Some(row_config)))
                    }
                    None => (node, job_config),
                }
            };
            let job = match job_config.as_deref() {
                Some(config) => resolve_config(config)?,
                None => None,
            };
            if let Some(job) = &job {
                out.entry((0, job.clone()))
                    .or_insert_with(|| job_change(job.clone()));
            }
            out.insert(
                (1, id.to_string()),
                ObjectChange {
                    kind: ChangeKind::Allocation,
                    key: id.to_string(),
                    job,
                    node,
                    deleted,
                },
            );
        }
        Ok((lane, out.into_values().collect()))
    }

    /// Raise the event-id floor to every id already in the journal, so the
    /// first id this process mints sorts above them whatever its clock says.
    fn seed_event_id_floor(&self) -> R<()> {
        let conn = self.read_connection()?;
        let mut stmt = conn
            .prepare("SELECT id % ?1 AS residue, MAX(id) FROM scheduler_event GROUP BY residue;")?;
        let rows = stmt.query_map([Lane::ID_STRIDE], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (_, id) = row?;
            Self::raise_event_id_floor(id);
        }
        Ok(())
    }
}
