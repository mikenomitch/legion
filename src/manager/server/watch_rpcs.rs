//! Blocking reads and `Manager.Watch` (revisions-and-watches plan §2-§3,
//! §6; docs/watches-and-blocking-queries.md).
//!
//! Both are built on the store's change journal
//! ([`crate::storage::revisions::RevisionHub`]), which every replica's
//! changeset projection feeds. A blocking read waits until the journal holds
//! a change its answer depends on after the caller's revision, then reads as
//! usual. A watch sends the journal's matching changes as they arrive, with
//! each object read when the message is sent.
//!
//! Read options travel as request headers (`x-legion-min-revision`,
//! `x-legion-wait`, `x-legion-consistency`), like Nomad's query parameters,
//! so every read takes them without a request field of its own.

use super::*;
use crate::storage::revisions::{Change, ChangeKind, Revision};
use crate::structs::{WatchAllocation, WatchChange, WatchEvent, WatchKind, WatchRequest};
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

/// Request header: block until something the read returns changed after
/// this revision.
pub const MIN_REVISION_HEADER: &str = "x-legion-min-revision";
/// Request header: how long a blocking read waits, in seconds.
pub const WAIT_HEADER: &str = "x-legion-wait";
/// Request header: `linearizable` or `default`.
pub const CONSISTENCY_HEADER: &str = "x-legion-consistency";

/// The default and longest wait of a blocking read (Nomad's default).
pub const MAX_READ_WAIT: Duration = Duration::from_secs(300);

/// A watch with nothing to say sends a progress event this often.
const WATCH_PROGRESS: Duration = Duration::from_secs(30);

/// Messages buffered toward one watcher before the sender waits for it.
const WATCH_BUFFER: usize = 64;

/// Changes one watch message carries at most; more are split.
const WATCH_BATCH_MAX: usize = 256;

/// Every raft group: a resume or a blocking read is only served when the
/// journal covers its revision in all of them. Under `lanes` the window is
/// shared, so a rolled-out group answers "too old" for every kind; under
/// `single` and `local_disk` only the first group ever moves.
const ALL_GROUPS: [usize; crate::storage::revisions::GROUPS] = [0, 1, 2];

/// How a read was asked to be served, from its request headers.
#[derive(Debug, Clone)]
pub(crate) struct ReadDirectives {
    /// Block until a matching change after this revision.
    pub min_revision: Option<Revision>,
    /// How long to block.
    pub wait: Duration,
    /// Confirm leadership with a quorum before reading.
    pub linearizable: bool,
}

impl Default for ReadDirectives {
    fn default() -> Self {
        ReadDirectives {
            min_revision: None,
            wait: MAX_READ_WAIT,
            linearizable: false,
        }
    }
}

impl ReadDirectives {
    /// Parse the read headers. A malformed value is `INVALID_ARGUMENT`.
    pub(crate) fn from_metadata(metadata: &tonic::metadata::MetadataMap) -> Result<Self, Status> {
        let header = |key: &str| -> Result<Option<String>, Status> {
            match metadata.get(key) {
                None => Ok(None),
                Some(value) => value
                    .to_str()
                    .map(|text| Some(text.trim().to_string()).filter(|text| !text.is_empty()))
                    .map_err(|_| Status::invalid_argument(format!("{key} is not text"))),
            }
        };
        let min_revision = match header(MIN_REVISION_HEADER)? {
            Some(raw) => Some(Revision::parse(&raw).map_err(|message| {
                Status::invalid_argument(format!("{MIN_REVISION_HEADER}: {message}"))
            })?),
            None => None,
        };
        let wait = match header(WAIT_HEADER)? {
            Some(raw) => {
                let seconds: u64 = raw.trim_end_matches('s').parse().map_err(|_| {
                    Status::invalid_argument(format!(
                        "{WAIT_HEADER} {raw:?} is not a number of seconds (at most {})",
                        MAX_READ_WAIT.as_secs()
                    ))
                })?;
                if seconds == 0 {
                    MAX_READ_WAIT
                } else {
                    Duration::from_secs(seconds).min(MAX_READ_WAIT)
                }
            }
            None => MAX_READ_WAIT,
        };
        let linearizable = match header(CONSISTENCY_HEADER)?.as_deref() {
            None | Some("default") => false,
            Some("linearizable") => true,
            Some(other) => {
                return Err(Status::invalid_argument(format!(
                    "{CONSISTENCY_HEADER} {other:?} is not a consistency: use \"default\" or \
                     \"linearizable\""
                )))
            }
        };
        Ok(ReadDirectives {
            min_revision,
            wait,
            linearizable,
        })
    }
}

/// What a blocking read's answer depends on.
#[derive(Debug, Clone)]
pub(crate) enum ReadScope {
    /// One job: its row, versions, allocations, task outcomes.
    Job(String),
    /// Every job.
    Jobs,
    /// Every allocation.
    Allocations,
    /// One KV key.
    Key(String),
    /// Every KV key under a prefix.
    Prefix(String),
}

impl ReadScope {
    fn matches(&self, change: &Change) -> bool {
        let object = &change.change;
        match self {
            ReadScope::Job(id) => object.kind == ChangeKind::Job && object.key == *id,
            ReadScope::Jobs => object.kind == ChangeKind::Job,
            ReadScope::Allocations => object.kind == ChangeKind::Allocation,
            ReadScope::Key(key) => object.kind == ChangeKind::Kv && object.key == *key,
            ReadScope::Prefix(prefix) => {
                object.kind == ChangeKind::Kv && object.key.starts_with(prefix.as_str())
            }
        }
    }
}

/// Wait until the journal holds a change `scope` cares about after
/// `min_revision`, or until `wait` passes. Returns at once when
/// `min_revision` is older than the journal (nothing can be proven), and
/// early when the journal restarts.
pub(crate) async fn wait_for_change(
    store: &Store,
    min_revision: &Revision,
    wait: Duration,
    scope: &ReadScope,
) {
    let hub = store.revisions();
    let deadline = tokio::time::Instant::now() + wait;
    let mut wake = hub.subscribe();
    let batch = match hub.changes_since(min_revision, &ALL_GROUPS) {
        Ok(batch) => batch,
        Err(_) => return,
    };
    if batch.changes.iter().any(|change| scope.matches(change)) {
        return;
    }
    let generation = batch.generation;
    let mut last_seq = batch.last_seq;
    loop {
        tokio::select! {
            changed = wake.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep_until(deadline) => return,
        }
        let (batch, missed) = hub.changes_after(last_seq);
        if missed || batch.generation != generation {
            return;
        }
        if batch
            .changes
            .iter()
            .any(|change| change.after(min_revision) && scope.matches(change))
        {
            return;
        }
        last_seq = batch.last_seq;
    }
}

impl ManagerDaemonService {
    /// Serve a read's headers before it reads: confirm leadership for a
    /// linearizable read, then block for `x-legion-min-revision`. Returns
    /// the revision to report, read before the data so the answer is at
    /// least that new (a change that lands between the two shows up in the
    /// data and wakes the next blocking read again, never the reverse).
    pub(crate) async fn prepare_read(
        &self,
        directives: &ReadDirectives,
        scope: ReadScope,
    ) -> Result<String, Status> {
        if directives.linearizable {
            self.linearizable_barrier().await?;
        }
        if let Some(min_revision) = &directives.min_revision {
            wait_for_change(&self.store, min_revision, directives.wait, &scope).await;
            if directives.linearizable {
                self.linearizable_barrier().await?;
            }
        }
        Ok(self.store.current_revision_string())
    }

    /// The linearizable-read barrier, off the runtime. On a follower this is
    /// the typed not-the-leader rejection, which the leader-forward layer
    /// replays on the leader.
    async fn linearizable_barrier(&self) -> Result<(), Status> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.linearizable_read_barrier())
            .await
            .map_err(|e| Status::internal(format!("linearizable read barrier failed: {e}")))?
            .map_err(|e| map_store_error("Linearizable read", e))
    }

    /// The `Watch` handler: resume or start, then send matching changes as
    /// the journal records them.
    pub(super) async fn watch_rpc(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<ReceiverStream<Result<WatchEvent, Status>>>, Status> {
        let kind = WatchKind::try_from(request.get_ref().kind).unwrap_or(WatchKind::Unspecified);
        let resource = match kind {
            WatchKind::Job | WatchKind::Allocation => AclResource::Job,
            WatchKind::Kv => AclResource::Kv,
            WatchKind::Unspecified => {
                // Gate before saying anything about the request's shape.
                self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
                return Err(Status::invalid_argument(
                    "Watch needs a kind: job, allocation, or kv",
                ));
            }
        };
        // Coarse gate: the caller can read SOMETHING of the kind. Each
        // change is filtered to what it may read before it is sent.
        let identity = self.check_acl(&request, resource, AclAction::Read, AclScope::any())?;
        let req = request.into_inner();
        let from = match req.from_revision.trim() {
            "" => None,
            raw => Some(Revision::parse(raw).map_err(|message| {
                Status::invalid_argument(format!("from_revision: {message}"))
            })?),
        };
        let filter = WatchFilter::new(kind, &req);
        let hub = self.store.revisions();
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(WATCH_BUFFER);

        // Where the stream starts. A resume is served from the journal when
        // it still covers `from`; otherwise the client relists.
        let mut wake = hub.subscribe();
        let (floor, start) = match &from {
            Some(from) => match hub.changes_since(from, &ALL_GROUPS) {
                Ok(batch) => (*from, batch),
                Err(too_old) => {
                    let event = WatchEvent {
                        revision: String::new(),
                        changes: Vec::new(),
                        resync_required: true,
                        oldest_revision: too_old.oldest.render(),
                        progress: false,
                    };
                    let _ = tx.send(Ok(event)).await;
                    return Ok(Response::new(ReceiverStream::new(rx)));
                }
            },
            None => {
                let start = hub.position();
                let current = self
                    .store
                    .current_revision()
                    .map_err(|e| map_store_error("Error reading the store's revision", e))?;
                (current.max(&start.recorded), start)
            }
        };
        info!(
            kind = kind.as_str_name(),
            from = %from.map(|from| from.render()).unwrap_or_default(),
            at = %floor,
            "Watch opened"
        );

        tokio::spawn(async move {
            let mut watch = WatchState {
                store,
                identity,
                filter,
                floor,
                position: floor.max(&start.recorded),
                allowed_jobs: HashMap::new(),
            };
            // The opening progress event says where the stream starts.
            if tx.send(Ok(watch.progress())).await.is_err() {
                return;
            }
            let generation = start.generation;
            let mut last_seq = start.last_seq;
            if !watch.deliver(&tx, start.changes).await {
                return;
            }
            let mut idle_until = tokio::time::Instant::now() + WATCH_PROGRESS;
            loop {
                tokio::select! {
                    changed = wake.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep_until(idle_until) => {
                        watch.position = watch.position.max(&hub.recorded());
                        if tx.send(Ok(watch.progress())).await.is_err() {
                            break;
                        }
                        idle_until = tokio::time::Instant::now() + WATCH_PROGRESS;
                        continue;
                    }
                    _ = tx.closed() => break,
                }
                let (batch, missed) = hub.changes_after(last_seq);
                if missed || batch.generation != generation {
                    // The journal rolled past this watcher, or restarted
                    // under it (a snapshot install): it cannot be told what
                    // it missed, so it relists.
                    let event = WatchEvent {
                        revision: watch.position.render(),
                        changes: Vec::new(),
                        resync_required: true,
                        oldest_revision: hub.horizon().render(),
                        progress: false,
                    };
                    let _ = tx.send(Ok(event)).await;
                    break;
                }
                last_seq = batch.last_seq;
                let recorded = batch.recorded;
                let sent_any = !batch.changes.is_empty();
                if !watch.deliver(&tx, batch.changes).await {
                    break;
                }
                watch.position = watch.position.max(&recorded);
                if sent_any {
                    idle_until = tokio::time::Instant::now() + WATCH_PROGRESS;
                }
            }
            debug!("Watch closed");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Which objects a watch wants.
#[derive(Debug, Clone)]
struct WatchFilter {
    kind: ChangeKind,
    namespace: Option<String>,
    /// A canonical job id.
    job: Option<String>,
    node: Option<String>,
    prefix: String,
}

impl WatchFilter {
    fn new(kind: WatchKind, req: &WatchRequest) -> Self {
        let namespace = Some(req.namespace.trim().to_string()).filter(|ns| !ns.is_empty());
        let job = Some(req.job.trim())
            .filter(|job| !job.is_empty())
            .map(|job| match &namespace {
                Some(namespace) => crate::structs::namespace::qualify_in(namespace, job),
                None => job.to_string(),
            });
        WatchFilter {
            kind: match kind {
                WatchKind::Allocation => ChangeKind::Allocation,
                WatchKind::Kv => ChangeKind::Kv,
                _ => ChangeKind::Job,
            },
            namespace,
            job,
            node: Some(req.node_id.trim().to_string()).filter(|node| !node.is_empty()),
            prefix: req.prefix.clone(),
        }
    }

    fn matches(&self, change: &Change) -> bool {
        let object = &change.change;
        if object.kind != self.kind {
            return false;
        }
        match self.kind {
            ChangeKind::Kv => object.key.starts_with(self.prefix.as_str()),
            ChangeKind::Job | ChangeKind::Allocation => {
                let job = object.job.as_deref().unwrap_or_default();
                if self.job.as_deref().is_some_and(|wanted| wanted != job) {
                    return false;
                }
                if self.namespace.as_deref().is_some_and(|namespace| {
                    job.is_empty() || crate::structs::namespace::job_namespace(job) != namespace
                }) {
                    return false;
                }
                if self.kind == ChangeKind::Allocation
                    && self
                        .node
                        .as_deref()
                        .is_some_and(|node| object.node.as_deref() != Some(node))
                {
                    return false;
                }
                true
            }
        }
    }
}

/// One open watch's state.
struct WatchState {
    store: Store,
    identity: AclIdentity,
    filter: WatchFilter,
    /// Changes at or below this were before the stream's start.
    floor: Revision,
    /// Every matching change at or below this has been sent.
    position: Revision,
    /// Per-job read decisions, cached for the stream's life.
    allowed_jobs: HashMap<String, bool>,
}

impl WatchState {
    fn progress(&self) -> WatchEvent {
        WatchEvent {
            revision: self.position.render(),
            changes: Vec::new(),
            resync_required: false,
            oldest_revision: String::new(),
            progress: true,
        }
    }

    fn may_read(&mut self, change: &Change) -> bool {
        let object = &change.change;
        match object.kind {
            ChangeKind::Kv => self.identity.allows(
                AclResource::Kv,
                AclAction::Read,
                &AclScope::path(&object.key),
            ),
            ChangeKind::Job | ChangeKind::Allocation => {
                let Some(job) = object.job.as_deref() else {
                    // An allocation whose job is gone: only a caller who can
                    // read every job learns of it.
                    return self.identity.allows(
                        AclResource::Job,
                        AclAction::Read,
                        &AclScope::job("*"),
                    );
                };
                if let Some(allowed) = self.allowed_jobs.get(job) {
                    return *allowed;
                }
                let provenance = self
                    .store
                    .job_template_provenance(job)
                    .unwrap_or_default()
                    .map(|provenance| provenance.name);
                let allowed = self.identity.allows(
                    AclResource::Job,
                    AclAction::Read,
                    &AclScope::job(job).with_template(provenance),
                );
                self.allowed_jobs.insert(job.to_string(), allowed);
                allowed
            }
        }
    }

    /// Send the matching, readable changes among `changes` (oldest first),
    /// one message per [`WATCH_BATCH_MAX`] objects, each object read now.
    /// `false` when the watcher went away.
    async fn deliver(
        &mut self,
        tx: &tokio::sync::mpsc::Sender<Result<WatchEvent, Status>>,
        changes: Vec<Change>,
    ) -> bool {
        // One entry per object, at its newest change, in first-seen order.
        let mut order: Vec<(ChangeKind, String)> = Vec::new();
        let mut newest: HashMap<(ChangeKind, String), Change> = HashMap::new();
        for change in changes {
            if !change.after(&self.floor) || !self.filter.matches(&change) {
                continue;
            }
            if !self.may_read(&change) {
                continue;
            }
            let key = (change.change.kind, change.change.key.clone());
            if !newest.contains_key(&key) {
                order.push(key.clone());
            }
            newest.insert(key, change);
        }
        if order.is_empty() {
            return true;
        }
        let picked: Vec<Change> = order
            .into_iter()
            .filter_map(|key| newest.remove(&key))
            .collect();
        for chunk in picked.chunks(WATCH_BATCH_MAX) {
            let mut position = self.position;
            for change in chunk {
                position.raise(change.group, change.index);
            }
            let store = self.store.clone();
            let chunk = chunk.to_vec();
            let materialized =
                tokio::task::spawn_blocking(move || materialize(&store, chunk)).await;
            let changes = match materialized {
                Ok(changes) => changes,
                Err(err) => {
                    let _ = tx
                        .send(Err(Status::internal(format!(
                            "watch could not read the changed objects: {err}"
                        ))))
                        .await;
                    return false;
                }
            };
            self.position = position;
            let event = WatchEvent {
                revision: self.position.render(),
                changes,
                resync_required: false,
                oldest_revision: String::new(),
                progress: false,
            };
            if tx.send(Ok(event)).await.is_err() {
                return false;
            }
        }
        true
    }
}

/// Read each changed object as it is now. Blocking.
fn materialize(store: &Store, changes: Vec<Change>) -> Vec<WatchChange> {
    let mut tasks: HashMap<String, HashMap<i64, String>> = HashMap::new();
    changes
        .into_iter()
        .map(|change| {
            let mut revision = Revision::default();
            revision.raise(change.group, change.index);
            let object = change.change;
            let job = object.job.clone().unwrap_or_default();
            let mut out = WatchChange {
                kind: match object.kind {
                    ChangeKind::Job => WatchKind::Job,
                    ChangeKind::Allocation => WatchKind::Allocation,
                    ChangeKind::Kv => WatchKind::Kv,
                } as i32,
                key: object.key.clone(),
                namespace: if job.is_empty() {
                    String::new()
                } else {
                    crate::structs::namespace::job_namespace(&job).to_string()
                },
                job,
                node_id: object.node.clone().unwrap_or_default(),
                allocation_id: 0,
                deleted: object.deleted,
                revision: revision.render(),
                job_info: None,
                kv: None,
                allocation: None,
            };
            match object.kind {
                ChangeKind::Job => match store.get_job_with_status(&object.key) {
                    Ok(job) => out.job_info = Some(job),
                    Err(_) => out.deleted = true,
                },
                ChangeKind::Kv => match store.get_kv(&object.key) {
                    Ok(Some(entry)) => out.kv = Some(entry),
                    Ok(None) => out.deleted = true,
                    Err(err) => debug!(key = %object.key, error = ?err, "Watch could not read a key"),
                },
                ChangeKind::Allocation => {
                    let id: i64 = object.key.parse().unwrap_or_default();
                    out.allocation_id = id;
                    match store.get_allocation_by_id(id) {
                        Ok(Some(allocation)) => {
                            let names = tasks
                                .entry(allocation.job_config_id.clone())
                                .or_insert_with(|| {
                                    store
                                        .get_task_configs_for_job_config(&allocation.job_config_id)
                                        .unwrap_or_default()
                                        .into_iter()
                                        .map(|(id, task)| (id, task.name))
                                        .collect()
                                });
                            out.node_id = allocation.node_id.clone();
                            out.allocation = Some(WatchAllocation {
                                id,
                                status: allocation.status.clone(),
                                node_id: allocation.node_id.clone(),
                                job_id: allocation.job_id.clone(),
                                job_config_id: allocation.job_config_id.clone(),
                                task_name: names
                                    .get(&allocation.task_config_id)
                                    .cloned()
                                    .unwrap_or_default(),
                                instance_id: allocation.instance_id.clone(),
                            });
                        }
                        Ok(None) => out.deleted = true,
                        Err(err) => {
                            debug!(allocation_id = id, error = ?err, "Watch could not read an allocation")
                        }
                    }
                }
            }
            if out.deleted {
                out.job_info = None;
                out.kv = None;
                out.allocation = None;
            }
            out
        })
        .collect()
}

#[cfg(test)]
mod watch_rpcs_test {
    use super::*;

    fn metadata(pairs: &[(&'static str, &str)]) -> tonic::metadata::MetadataMap {
        let mut map = tonic::metadata::MetadataMap::new();
        for (key, value) in pairs {
            map.insert(*key, value.parse().unwrap());
        }
        map
    }

    #[test]
    fn read_headers_parse_and_reject_garbage() {
        let none = ReadDirectives::from_metadata(&metadata(&[])).unwrap();
        assert!(none.min_revision.is_none());
        assert!(!none.linearizable);
        assert_eq!(none.wait, MAX_READ_WAIT);

        let some = ReadDirectives::from_metadata(&metadata(&[
            (MIN_REVISION_HEADER, "12"),
            (WAIT_HEADER, "30s"),
            (CONSISTENCY_HEADER, "linearizable"),
        ]))
        .unwrap();
        assert_eq!(some.min_revision, Some(Revision::single(12)));
        assert_eq!(some.wait, Duration::from_secs(30));
        assert!(some.linearizable);

        let capped = ReadDirectives::from_metadata(&metadata(&[(WAIT_HEADER, "9999")])).unwrap();
        assert_eq!(capped.wait, MAX_READ_WAIT);

        for bad in [
            (MIN_REVISION_HEADER, "abc"),
            (WAIT_HEADER, "soon"),
            (CONSISTENCY_HEADER, "strong"),
        ] {
            let err = ReadDirectives::from_metadata(&metadata(&[bad])).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{bad:?}");
        }
    }

    #[test]
    fn a_watch_filter_matches_by_job_namespace_node_and_prefix() {
        use crate::storage::revisions::ObjectChange;
        let change = |kind, key: &str, job: Option<&str>, node: Option<&str>| Change {
            seq: 1,
            group: 0,
            index: 1,
            change: ObjectChange {
                kind,
                key: key.into(),
                job: job.map(str::to_string),
                node: node.map(str::to_string),
                deleted: false,
            },
        };
        let jobs = WatchFilter::new(
            WatchKind::Job,
            &WatchRequest {
                kind: WatchKind::Job as i32,
                namespace: "team".into(),
                job: "web".into(),
                ..Default::default()
            },
        );
        assert!(jobs.matches(&change(ChangeKind::Job, "team/web", Some("team/web"), None)));
        assert!(!jobs.matches(&change(ChangeKind::Job, "web", Some("web"), None)));
        assert!(!jobs.matches(&change(ChangeKind::Allocation, "7", Some("team/web"), None)));

        let allocations = WatchFilter::new(
            WatchKind::Allocation,
            &WatchRequest {
                kind: WatchKind::Allocation as i32,
                node_id: "n1".into(),
                ..Default::default()
            },
        );
        assert!(allocations.matches(&change(
            ChangeKind::Allocation,
            "7",
            Some("web"),
            Some("n1")
        )));
        assert!(!allocations.matches(&change(
            ChangeKind::Allocation,
            "8",
            Some("web"),
            Some("n2")
        )));

        let keys = WatchFilter::new(
            WatchKind::Kv,
            &WatchRequest {
                kind: WatchKind::Kv as i32,
                prefix: "app/".into(),
                ..Default::default()
            },
        );
        assert!(keys.matches(&change(ChangeKind::Kv, "app/config", None, None)));
        assert!(!keys.matches(&change(ChangeKind::Kv, "other", None, None)));
    }
}
