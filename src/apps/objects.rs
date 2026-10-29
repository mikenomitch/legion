//! Owning an object kind, from inside the app (docs/app-objects.md,
//! "Writing an app that owns a kind"): the watch cache, status writes, and
//! the reconcile loop.
//!
//! An app declares kinds in its manifest's `kinds`; people apply objects of
//! them with `legion object apply`. The app's side is level-triggered:
//!
//! ```ignore
//! context.reconcile("Repo", |request| async move {
//!     let spec: RepoSpec = request.spec_as()?;
//!     match sync(&spec).await {
//!         Ok(commit) => Ok(ObjectOutcome::ready()
//!             .because("Synced", format!("at {commit}"))
//!             .with_fields(json!({ "commit": commit }))),
//!         Err(err) => Ok(ObjectOutcome::not_ready("SyncFailed", err.to_string())),
//!     }
//! })?;
//! ```
//!
//! The handler runs for every object of the kind when the loop starts, when
//! an object's spec changes, on a periodic resync (5 minutes by default),
//! and after a failure, with exponential backoff. Calls for one object never
//! overlap. Its [`ObjectOutcome`] becomes the object's `Accepted` and
//! `Ready` conditions and status `fields`, with `observedGeneration` set to
//! the generation the handler saw, written only when something changed. An
//! object being deleted goes to the cleanup handler
//! ([`Reconciler::on_cleanup`]) instead, and is released when it returns.
//!
//! [`ObjectCache`], [`watch_objects`] and [`write_object_status`] are the
//! pieces the loop is made of, for an app that wants its own.

use crate::cmd::helpers::AuthedManagerClient;
use crate::prelude::*;
use crate::structs::app_object::{
    CONDITION_ACCEPTED, CONDITION_READY, MAX_CONDITION_MESSAGE_BYTES,
};
use crate::structs::{
    AppObject, ObjectCondition, ObjectEvent, ReleaseObjectReply, ReleaseObjectRequest,
    UpdateObjectStatusReply, UpdateObjectStatusRequest, WatchObjectsRequest,
};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};
use tonic::Status;

/// The largest `WatchObjects` message the framework decodes. The manager
/// splits the snapshot into messages of at most 1,000 objects and about 2
/// MiB, so this leaves room for any one object.
pub const MAX_WATCH_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// How often every object is reconciled again when nothing changed.
pub const DEFAULT_RESYNC: Duration = Duration::from_secs(300);

/// How many objects are reconciled at once, by default.
pub const DEFAULT_CONCURRENCY: usize = 4;

/// The first retry after a failed reconcile; it doubles per failure.
const RETRY_MIN: Duration = Duration::from_secs(1);
/// The longest wait between retries.
const RETRY_MAX: Duration = Duration::from_secs(300);
/// The longest wait before reopening the watch.
const WATCH_RETRY_MAX: Duration = Duration::from_secs(30);
/// How often due work is dispatched.
const DISPATCH_TICK: Duration = Duration::from_millis(100);

/// What changed in an [`ObjectCache`] when it applied an event.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheChange {
    /// New, or its spec or deletion state changed.
    Upserted(u64),
    /// Gone: deleted, or no longer watched.
    Deleted(u64),
}

/// The objects an app watches, by uid, kept by applying the
/// `WatchObjects` stream's events. A snapshot (which may span several
/// messages) replaces the set when it completes, so a uid missing from it
/// reads as deleted: after a reconnect, the cache catches up on its own.
#[derive(Debug, Default, Clone)]
pub struct ObjectCache {
    objects: HashMap<u64, AppObject>,
    staging: Option<HashMap<u64, AppObject>>,
    synced: bool,
}

/// Whether two views of one object differ in what a reconcile acts on.
fn object_moved(before: &AppObject, after: &AppObject) -> bool {
    before.spec_version != after.spec_version
        || before.deleting != after.deleting
        || before.state != after.state
        || before.generation != after.generation
}

impl ObjectCache {
    /// Apply one event, returning what changed (nothing until a snapshot
    /// completes).
    pub fn apply(&mut self, event: ObjectEvent) -> Vec<CacheChange> {
        if event.snapshot {
            self.staging = Some(HashMap::new());
        }
        if let Some(staging) = self.staging.as_mut() {
            for object in event.upserts {
                staging.insert(object.uid, object);
            }
            if !event.snapshot_done {
                return Vec::new();
            }
            let next = self.staging.take().unwrap_or_default();
            let mut changes: Vec<CacheChange> = self
                .objects
                .keys()
                .filter(|uid| !next.contains_key(uid))
                .map(|uid| CacheChange::Deleted(*uid))
                .collect();
            changes.extend(
                next.values()
                    .filter(|object| {
                        self.objects
                            .get(&object.uid)
                            .is_none_or(|before| object_moved(before, object))
                    })
                    .map(|object| CacheChange::Upserted(object.uid)),
            );
            self.objects = next;
            self.synced = true;
            changes.sort_by_key(|change| match change {
                CacheChange::Upserted(uid) | CacheChange::Deleted(uid) => *uid,
            });
            return changes;
        }
        let mut changes = Vec::new();
        for gone in event.deletes {
            if self.objects.remove(&gone.uid).is_some() {
                changes.push(CacheChange::Deleted(gone.uid));
            }
        }
        for object in event.upserts {
            let moved = self
                .objects
                .get(&object.uid)
                .is_none_or(|before| object_moved(before, &object));
            let uid = object.uid;
            self.objects.insert(uid, object);
            if moved {
                changes.push(CacheChange::Upserted(uid));
            }
        }
        changes
    }

    /// Start over after a reconnect: keep the objects until the next
    /// snapshot completes.
    pub fn reconnecting(&mut self) {
        self.staging = None;
        self.synced = false;
    }

    /// Whether a snapshot has completed since the last reconnect.
    pub fn synced(&self) -> bool {
        self.synced
    }

    pub fn get(&self, uid: u64) -> Option<&AppObject> {
        self.objects.get(&uid)
    }

    pub fn objects(&self) -> impl Iterator<Item = &AppObject> {
        self.objects.values()
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

/// The app-plane calls an object reconciler makes: the manager's, through
/// the app's own client, or a stand-in in tests.
#[async_trait::async_trait]
pub trait ObjectPlane: Clone + Send + Sync + 'static {
    async fn watch(
        &self,
        request: WatchObjectsRequest,
    ) -> Result<BoxStream<'static, Result<ObjectEvent, Status>>, Status>;
    async fn update_status(
        &self,
        request: UpdateObjectStatusRequest,
    ) -> Result<UpdateObjectStatusReply, Status>;
    async fn release(&self, request: ReleaseObjectRequest) -> Result<ReleaseObjectReply, Status>;
}

#[async_trait::async_trait]
impl ObjectPlane for AuthedManagerClient {
    async fn watch(
        &self,
        request: WatchObjectsRequest,
    ) -> Result<BoxStream<'static, Result<ObjectEvent, Status>>, Status> {
        let stream = watch_objects(self, request).await?;
        Ok(stream.boxed())
    }

    async fn update_status(
        &self,
        request: UpdateObjectStatusRequest,
    ) -> Result<UpdateObjectStatusReply, Status> {
        Ok(self
            .clone()
            .update_object_status(request)
            .await?
            .into_inner())
    }

    async fn release(&self, request: ReleaseObjectRequest) -> Result<ReleaseObjectReply, Status> {
        Ok(self.clone().release_object(request).await?.into_inner())
    }
}

/// Open `WatchObjects` with the decode limit a snapshot message needs.
pub async fn watch_objects(
    client: &AuthedManagerClient,
    request: WatchObjectsRequest,
) -> Result<tonic::Streaming<ObjectEvent>, Status> {
    Ok(client
        .clone()
        .max_decoding_message_size(MAX_WATCH_MESSAGE_BYTES)
        .watch_objects(request)
        .await?
        .into_inner())
}

/// One condition, as a reconcile sets it.
#[derive(Debug, Clone, PartialEq)]
struct Condition {
    status: &'static str,
    reason: String,
    message: String,
}

/// What reconciling one object concluded: its conditions and status
/// fields, and when to look again.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectOutcome {
    accepted: Option<Condition>,
    ready: Option<Condition>,
    extra: Vec<(String, Condition)>,
    fields: Value,
    requeue_after: Option<Duration>,
}

/// A reason as conditions take it: letters, digits, `_` and `-`.
fn reason_text(reason: &str) -> String {
    reason
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .take(128)
        .collect()
}

/// A message cut to what conditions take.
fn message_text(message: &str) -> String {
    if message.len() <= MAX_CONDITION_MESSAGE_BYTES {
        return message.to_string();
    }
    let mut end = MAX_CONDITION_MESSAGE_BYTES - 3;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &message[..end])
}

fn condition(status: &'static str, reason: &str, message: &str) -> Condition {
    Condition {
        status,
        reason: reason_text(reason),
        message: message_text(message),
    }
}

impl ObjectOutcome {
    /// Reconciled and working: `Accepted=true`, `Ready=true`. The object
    /// reads `Ready`.
    pub fn ready() -> ObjectOutcome {
        ObjectOutcome {
            accepted: Some(condition("true", "", "")),
            ready: Some(condition("true", "", "")),
            extra: Vec::new(),
            fields: json!({}),
            requeue_after: None,
        }
    }

    /// Accepted, but not working yet (or any more): `Ready=false` with
    /// `reason`. The object reads `NotReady: <reason>`.
    pub fn not_ready(reason: &str, message: impl AsRef<str>) -> ObjectOutcome {
        ObjectOutcome {
            ready: Some(condition("false", reason, message.as_ref())),
            ..ObjectOutcome::ready()
        }
    }

    /// The app understood the spec and refuses it: `Accepted=false`. The
    /// object reads `Rejected: <reason>` until its spec changes.
    pub fn rejected(reason: &str, message: impl AsRef<str>) -> ObjectOutcome {
        ObjectOutcome {
            accepted: Some(condition("false", reason, message.as_ref())),
            ready: None,
            ..ObjectOutcome::ready()
        }
    }

    /// Accepted, for a kind with nothing further to report: only
    /// `Accepted=true`. The object reads `Accepted`.
    pub fn accepted() -> ObjectOutcome {
        ObjectOutcome {
            ready: None,
            ..ObjectOutcome::ready()
        }
    }

    /// The reason and message of the outcome's main condition (`Ready`, or
    /// `Accepted` when there is no `Ready`): `ObjectOutcome::ready()
    /// .because("Synced", "at 3f9a1c2")`.
    pub fn because(mut self, reason: &str, message: impl AsRef<str>) -> ObjectOutcome {
        let main = match self.ready.as_mut() {
            Some(ready) => Some(ready),
            None => self.accepted.as_mut(),
        };
        if let Some(main) = main {
            main.reason = reason_text(reason);
            main.message = message_text(message.as_ref());
        }
        self
    }

    /// Status fields, checked by the manager against the kind's `status`
    /// schema (at most 16 KiB: facts, not telemetry).
    pub fn with_fields(mut self, fields: Value) -> ObjectOutcome {
        self.fields = fields;
        self
    }

    /// One more condition of the app's own (`type` is PascalCase).
    pub fn with_condition(
        mut self,
        kind: &str,
        status: bool,
        reason: &str,
        message: impl AsRef<str>,
    ) -> ObjectOutcome {
        self.extra.push((
            kind.to_string(),
            condition(
                if status { "true" } else { "false" },
                reason,
                message.as_ref(),
            ),
        ));
        self
    }

    /// Reconcile again after `after`, even when nothing changes (sooner
    /// than the resync).
    pub fn requeue_after(mut self, after: Duration) -> ObjectOutcome {
        self.requeue_after = Some(after);
        self
    }

    /// The conditions, as `UpdateObjectStatus` takes them.
    pub fn conditions(&self) -> Vec<ObjectCondition> {
        let named = [
            (CONDITION_ACCEPTED, self.accepted.as_ref()),
            (CONDITION_READY, self.ready.as_ref()),
        ];
        named
            .into_iter()
            .filter_map(|(kind, condition)| condition.map(|c| (kind.to_string(), c.clone())))
            .chain(self.extra.iter().cloned())
            .map(|(kind, condition)| ObjectCondition {
                r#type: kind,
                status: condition.status.to_string(),
                reason: condition.reason,
                message: condition.message,
                since: String::new(),
            })
            .collect()
    }

    pub fn fields(&self) -> &Value {
        &self.fields
    }
}

/// What a status write sets, compared to skip writes that change nothing.
#[derive(Debug, Clone, PartialEq)]
struct Reported {
    generation: u64,
    conditions: Vec<(String, String, String, String)>,
    fields: Value,
}

impl Reported {
    fn of(generation: u64, conditions: &[ObjectCondition], fields: &Value) -> Reported {
        let mut conditions: Vec<(String, String, String, String)> = conditions
            .iter()
            .map(|c| {
                (
                    c.r#type.clone(),
                    c.status.clone(),
                    c.reason.clone(),
                    c.message.clone(),
                )
            })
            .collect();
        conditions.sort();
        let fields = match fields {
            Value::Null => json!({}),
            other => other.clone(),
        };
        Reported {
            generation,
            conditions,
            fields,
        }
    }

    /// What the object's stored status already says.
    fn stored(object: &AppObject) -> Option<Reported> {
        let status = object.status.as_ref()?;
        let fields = serde_json::from_str(&status.fields_json).unwrap_or_else(|_| json!({}));
        Some(Reported::of(
            status.observed_generation,
            &status.conditions,
            &fields,
        ))
    }
}

/// Write `outcome` as `object`'s status at the generation it was
/// reconciled from. `Ok(false)` when the manager already held it.
pub async fn write_object_status<P: ObjectPlane>(
    plane: &P,
    object: &AppObject,
    outcome: &ObjectOutcome,
) -> Result<bool, Status> {
    let reply = plane
        .update_status(UpdateObjectStatusRequest {
            kind: object.qualified_kind.clone(),
            namespace: object.namespace.clone(),
            name: object.name.clone(),
            uid: object.uid,
            observed_generation: object.generation,
            conditions: outcome.conditions(),
            fields_json: outcome.fields.to_string(),
            idempotency_key: None,
        })
        .await?;
    Ok(reply.written)
}

/// One object handed to a reconcile or cleanup handler.
#[derive(Debug, Clone)]
pub struct ObjectRequest {
    /// The object as the watch delivered it, `writeOnly` fields included.
    pub object: AppObject,
    /// Its effective spec: the stored spec with the schema's defaults.
    pub spec: Value,
}

impl ObjectRequest {
    fn new(object: AppObject) -> ObjectRequest {
        let spec = serde_json::from_str(&object.effective_json)
            .or_else(|_| serde_json::from_str(&object.spec_json))
            .unwrap_or(Value::Null);
        ObjectRequest { object, spec }
    }

    /// The spec as the app's own type.
    pub fn spec_as<T: serde::de::DeserializeOwned>(&self) -> R<T> {
        serde_json::from_value(self.spec.clone()).with_context(|| {
            format!(
                "{} {}: the spec does not match the app's type",
                self.object.qualified_kind, self.object.id
            )
        })
    }

    /// `namespace/name`, or `name` for a cluster kind.
    pub fn id(&self) -> &str {
        &self.object.id
    }
}

type ReconcileHandler =
    Arc<dyn Fn(ObjectRequest) -> BoxFuture<'static, R<ObjectOutcome>> + Send + Sync>;
type CleanupHandler = Arc<dyn Fn(ObjectRequest) -> BoxFuture<'static, R<()>> + Send + Sync>;

/// A reconcile loop for one kind, configured before [`Reconciler::run`].
#[derive(Clone)]
pub struct Reconciler {
    app: String,
    kind: String,
    resync: Duration,
    shard_count: u32,
    shards: Vec<u32>,
    concurrency: usize,
    cleanup: Option<CleanupHandler>,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler")
            .field("app", &self.app)
            .field("kind", &self.kind)
            .field("resync", &self.resync)
            .field("shard_count", &self.shard_count)
            .field("shards", &self.shards)
            .field("concurrency", &self.concurrency)
            .field("cleanup", &self.cleanup.is_some())
            .finish()
    }
}

impl Reconciler {
    /// A reconciler for `app`'s kind `kind` (as declared, `Repo`, or
    /// qualified).
    pub fn new(app: &str, kind: &str) -> Reconciler {
        Reconciler {
            app: app.to_string(),
            kind: kind.to_string(),
            resync: DEFAULT_RESYNC,
            shard_count: 0,
            shards: Vec::new(),
            concurrency: DEFAULT_CONCURRENCY,
            cleanup: None,
        }
    }

    /// Reconcile every object again this often (default 5 minutes).
    pub fn resync(mut self, every: Duration) -> Reconciler {
        self.resync = every.max(Duration::from_secs(1));
        self
    }

    /// Reconcile only the objects in `mine` of `count` shards: one replica
    /// of a sharded app each (a static split; the shard of an object is a
    /// hash of its kind, namespace and name).
    pub fn shards(mut self, count: u32, mine: Vec<u32>) -> Reconciler {
        self.shard_count = count;
        self.shards = mine;
        self
    }

    /// Reconcile at most this many objects at once (default 4).
    pub fn concurrency(mut self, limit: usize) -> Reconciler {
        self.concurrency = limit.max(1);
        self
    }

    /// Clean up after a deleted object (a kind with `cleanup` in its
    /// manifest): the object is released, and removed, when `handler`
    /// returns `Ok`; an error retries with backoff until the kind's cleanup
    /// window ends. Without a handler, deleting objects are released at
    /// once.
    pub fn on_cleanup<F, Fut>(mut self, handler: F) -> Reconciler
    where
        F: Fn(ObjectRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<()>> + Send + 'static,
    {
        self.cleanup = Some(Arc::new(move |request| Box::pin(handler(request))));
        self
    }

    /// Run the loop against `plane` until `stop` turns true.
    pub fn run<P, F, Fut>(
        self,
        plane: P,
        stop: tokio::sync::watch::Receiver<bool>,
        handler: F,
    ) -> tokio::task::JoinHandle<()>
    where
        P: ObjectPlane,
        F: Fn(ObjectRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<ObjectOutcome>> + Send + 'static,
    {
        let handler: ReconcileHandler = Arc::new(move |request| Box::pin(handler(request)));
        tokio::spawn(reconcile_loop(self, plane, stop, handler))
    }
}

/// The loop's bookkeeping for one object.
#[derive(Debug, Default)]
struct Work {
    /// When to reconcile next; `None` waits for a change or the resync.
    due: Option<Instant>,
    running: bool,
    /// It changed while a reconcile ran: run again when that one ends.
    dirty: bool,
    failures: u32,
    reported: Option<Reported>,
}

/// What one reconcile run ended with.
struct Done {
    uid: u64,
    result: Result<Option<Duration>, String>,
    reported: Option<Reported>,
    /// The object is gone or changed under the run: wait for the watch.
    stale: bool,
}

fn retry_after(failures: u32) -> Duration {
    RETRY_MIN
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(16)))
        .min(RETRY_MAX)
}

async fn stopped(stop: &mut tokio::sync::watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            // The sender is gone: nobody can stop the loop any more.
            std::future::pending::<()>().await;
        }
    }
}

/// Reconcile one object: its cleanup when deleting, else the handler and a
/// status write when the outcome differs from what is stored.
async fn reconcile_one<P: ObjectPlane>(
    plane: P,
    handler: ReconcileHandler,
    cleanup: Option<CleanupHandler>,
    object: AppObject,
    reported: Option<Reported>,
) -> Done {
    let uid = object.uid;
    let done = |result, reported, stale| Done {
        uid,
        result,
        reported,
        stale,
    };
    if object.deleting {
        if let Some(cleanup) = cleanup {
            if let Err(err) = cleanup(ObjectRequest::new(object.clone())).await {
                return done(Err(format!("cleanup: {err:#}")), reported, false);
            }
        }
        let released = plane
            .release(ReleaseObjectRequest {
                kind: object.qualified_kind.clone(),
                namespace: object.namespace.clone(),
                name: object.name.clone(),
                uid,
                idempotency_key: None,
            })
            .await;
        return match released {
            Ok(_) => {
                info!(kind = %object.qualified_kind, object = %object.id, uid, "Object released");
                done(Ok(None), reported, true)
            }
            Err(status) if is_stale(&status) => done(Ok(None), reported, true),
            Err(status) => done(
                Err(format!("release: {}", status.message())),
                reported,
                false,
            ),
        };
    }
    let outcome = match handler(ObjectRequest::new(object.clone())).await {
        Ok(outcome) => outcome,
        Err(err) => {
            // Keep what the app said about acceptance; report the failure
            // as not ready.
            let message = format!("{err:#}");
            let mut outcome = ObjectOutcome::not_ready("ReconcileFailed", &message);
            let accepted = reported
                .as_ref()
                .or(Reported::stored(&object).as_ref())
                .and_then(|reported| {
                    reported
                        .conditions
                        .iter()
                        .find(|c| c.0 == CONDITION_ACCEPTED)
                        .cloned()
                });
            outcome.accepted = accepted.map(|(_, status, reason, message)| Condition {
                status: if status == "false" { "false" } else { "true" },
                reason,
                message,
            });
            let next = Reported::of(object.generation, &outcome.conditions(), &outcome.fields);
            let stored = reported.or_else(|| Reported::stored(&object));
            if stored.as_ref() != Some(&next) {
                if let Err(status) = write_object_status(&plane, &object, &outcome).await {
                    let stale = is_stale(&status);
                    return done(Err(message), stored, stale);
                }
                return done(Err(message), Some(next), false);
            }
            return done(Err(message), stored, false);
        }
    };
    let next = Reported::of(object.generation, &outcome.conditions(), &outcome.fields);
    let stored = reported.or_else(|| Reported::stored(&object));
    if stored.as_ref() == Some(&next) {
        return done(Ok(outcome.requeue_after), stored, false);
    }
    match write_object_status(&plane, &object, &outcome).await {
        Ok(_) => done(Ok(outcome.requeue_after), Some(next), false),
        Err(status) if is_stale(&status) => done(Ok(None), stored, true),
        Err(status) => done(Err(format!("status: {}", status.message())), stored, false),
    }
}

/// A refusal that means the object moved on (deleted, replaced, or a newer
/// generation): the watch brings the change, so the loop waits for it.
fn is_stale(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::NotFound | tonic::Code::Aborted | tonic::Code::FailedPrecondition
    )
}

async fn reconcile_loop<P: ObjectPlane>(
    config: Reconciler,
    plane: P,
    mut stop: tokio::sync::watch::Receiver<bool>,
    handler: ReconcileHandler,
) {
    let request = WatchObjectsRequest {
        app: config.app.clone(),
        kinds: vec![config.kind.clone()],
        shard_count: config.shard_count,
        shards: config.shards.clone(),
    };
    let mut cache = ObjectCache::default();
    let mut work: HashMap<u64, Work> = HashMap::new();
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<Done>();
    let mut running = 0usize;
    let mut watch_retry = Duration::from_millis(500);
    info!(app = %config.app, kind = %config.kind, "Reconciling objects");
    loop {
        if *stop.borrow() {
            return;
        }
        let mut stream = match plane.watch(request.clone()).await {
            Ok(stream) => {
                watch_retry = Duration::from_millis(500);
                stream
            }
            Err(status) => {
                warn!(kind = %config.kind, "Cannot watch objects: {}", status.message());
                tokio::select! {
                    _ = stopped(&mut stop) => return,
                    _ = tokio::time::sleep(watch_retry) => {}
                }
                watch_retry = (watch_retry * 2).min(WATCH_RETRY_MAX);
                continue;
            }
        };
        cache.reconnecting();
        let mut resync = tokio::time::interval(config.resync);
        resync.tick().await;
        let mut tick = tokio::time::interval(DISPATCH_TICK);
        loop {
            tokio::select! {
                _ = stopped(&mut stop) => return,
                message = stream.next() => match message {
                    Some(Ok(event)) => {
                        for change in cache.apply(event) {
                            match change {
                                CacheChange::Upserted(uid) => {
                                    let entry = work.entry(uid).or_default();
                                    if entry.running {
                                        entry.dirty = true;
                                    } else {
                                        entry.due = Some(Instant::now());
                                        entry.failures = 0;
                                    }
                                }
                                CacheChange::Deleted(uid) => {
                                    if work.get(&uid).is_some_and(|entry| entry.running) {
                                        if let Some(entry) = work.get_mut(&uid) {
                                            entry.dirty = false;
                                            entry.due = None;
                                        }
                                    } else {
                                        work.remove(&uid);
                                    }
                                }
                            }
                        }
                    }
                    Some(Err(status)) => {
                        warn!(kind = %config.kind, "The object watch ended: {}", status.message());
                        break;
                    }
                    None => {
                        debug!(kind = %config.kind, "The object watch closed");
                        break;
                    }
                },
                Some(done) = done_rx.recv() => {
                    running = running.saturating_sub(1);
                    let exists = cache.get(done.uid).is_some();
                    let Some(entry) = work.get_mut(&done.uid) else { continue };
                    entry.running = false;
                    entry.reported = done.reported;
                    if !exists {
                        work.remove(&done.uid);
                        continue;
                    }
                    match done.result {
                        Ok(requeue) => {
                            entry.failures = 0;
                            entry.due = requeue.map(|after| Instant::now() + after);
                        }
                        Err(why) => {
                            entry.failures += 1;
                            let after = retry_after(entry.failures);
                            let id = cache.get(done.uid).map(|o| o.id.clone()).unwrap_or_default();
                            warn!(
                                kind = %config.kind,
                                object = %id,
                                failures = entry.failures,
                                retry_in_s = after.as_secs(),
                                "Reconcile failed: {why}"
                            );
                            entry.due = Some(Instant::now() + after);
                        }
                    }
                    if done.stale {
                        entry.due = None;
                    }
                    if entry.dirty {
                        entry.dirty = false;
                        entry.due = Some(Instant::now());
                    }
                },
                _ = resync.tick() => {
                    let now = Instant::now();
                    for object in cache.objects() {
                        let entry = work.entry(object.uid).or_default();
                        if entry.running {
                            entry.dirty = true;
                        } else if entry.due.is_none_or(|due| due > now) && entry.failures == 0 {
                            entry.due = Some(now);
                        }
                    }
                },
                _ = tick.tick() => {
                    if !cache.synced() {
                        continue;
                    }
                    let now = Instant::now();
                    let mut ready: Vec<(Instant, u64)> = work
                        .iter()
                        .filter(|(_, entry)| !entry.running && entry.due.is_some_and(|due| due <= now))
                        .map(|(uid, entry)| (entry.due.unwrap_or(now), *uid))
                        .collect();
                    ready.sort();
                    for (_, uid) in ready {
                        if running >= config.concurrency {
                            break;
                        }
                        let Some(object) = cache.get(uid).cloned() else {
                            work.remove(&uid);
                            continue;
                        };
                        let Some(entry) = work.get_mut(&uid) else { continue };
                        entry.running = true;
                        entry.due = None;
                        running += 1;
                        let reported = entry.reported.clone();
                        let (plane, handler, cleanup, tx) = (
                            plane.clone(),
                            handler.clone(),
                            config.cleanup.clone(),
                            done_tx.clone(),
                        );
                        tokio::spawn(async move {
                            let done = reconcile_one(plane, handler, cleanup, object, reported).await;
                            let _ = tx.send(done);
                        });
                    }
                },
            }
        }
        tokio::select! {
            _ = stopped(&mut stop) => return,
            _ = tokio::time::sleep(watch_retry) => {}
        }
        watch_retry = (watch_retry * 2).min(WATCH_RETRY_MAX);
    }
}

impl super::AppContext {
    /// Reconcile the app's objects of `kind` (as declared, `Repo`) with
    /// `handler`, with the default resync and no cleanup handler. See
    /// [`Self::reconciler`] to configure one, and the module docs.
    pub fn reconcile<F, Fut>(&self, kind: &str, handler: F) -> R<tokio::task::JoinHandle<()>>
    where
        F: Fn(ObjectRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<ObjectOutcome>> + Send + 'static,
    {
        self.start_reconciler(self.reconciler(kind), handler)
    }

    /// A [`Reconciler`] for `kind`, to configure and start with
    /// [`Self::start_reconciler`].
    pub fn reconciler(&self, kind: &str) -> Reconciler {
        Reconciler::new(self.name(), kind)
    }

    /// Run `reconciler` with `handler` over the app's own client until the
    /// app stops.
    pub fn start_reconciler<F, Fut>(
        &self,
        reconciler: Reconciler,
        handler: F,
    ) -> R<tokio::task::JoinHandle<()>>
    where
        F: Fn(ObjectRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<ObjectOutcome>> + Send + 'static,
    {
        let client = self.connection()?.client();
        Ok(reconciler.run(client, self.stop.clone(), handler))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{ObjectRef, ObjectStatus};
    use std::sync::Mutex as StdMutex;

    fn object(uid: u64, generation: u64, spec_version: u64) -> AppObject {
        AppObject {
            app: "gitops".into(),
            kind: "Repo".into(),
            qualified_kind: "gitops/Repo".into(),
            namespace: "default".into(),
            name: format!("r{uid}"),
            id: format!("default/r{uid}"),
            uid,
            generation,
            spec_version,
            effective_json: json!({ "url": format!("https://x/{uid}") }).to_string(),
            state: "valid".into(),
            ..Default::default()
        }
    }

    #[test]
    fn cache_applies_chunked_snapshots_and_tombstones_test() {
        let mut cache = ObjectCache::default();
        assert!(cache
            .apply(ObjectEvent {
                snapshot: true,
                upserts: vec![object(1, 1, 1)],
                ..Default::default()
            })
            .is_empty());
        assert!(!cache.synced());
        let changes = cache.apply(ObjectEvent {
            snapshot_done: true,
            upserts: vec![object(2, 1, 2)],
            ..Default::default()
        });
        assert_eq!(
            changes,
            vec![CacheChange::Upserted(1), CacheChange::Upserted(2)]
        );
        assert!(cache.synced());
        // A delta: a spec change, a status-only resend, a tombstone.
        let changes = cache.apply(ObjectEvent {
            upserts: vec![object(1, 2, 3), object(2, 1, 2)],
            deletes: vec![ObjectRef {
                uid: 7,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(changes, vec![CacheChange::Upserted(1)]);
        let changes = cache.apply(ObjectEvent {
            deletes: vec![ObjectRef {
                uid: 2,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(changes, vec![CacheChange::Deleted(2)]);
        // A reconnect's snapshot lacks uid 1: deleted.
        cache.reconnecting();
        let changes = cache.apply(ObjectEvent {
            snapshot: true,
            snapshot_done: true,
            upserts: vec![object(3, 1, 4)],
            ..Default::default()
        });
        assert_eq!(
            changes,
            vec![CacheChange::Deleted(1), CacheChange::Upserted(3)]
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn outcomes_become_conditions_test() {
        let types = |outcome: &ObjectOutcome| -> Vec<(String, String)> {
            outcome
                .conditions()
                .into_iter()
                .map(|c| (c.r#type, c.status))
                .collect()
        };
        assert_eq!(
            types(&ObjectOutcome::ready()),
            vec![
                ("Accepted".into(), "true".into()),
                ("Ready".into(), "true".into())
            ]
        );
        assert_eq!(
            types(&ObjectOutcome::rejected("BadUrl", "no")),
            vec![("Accepted".into(), "false".into())]
        );
        assert_eq!(
            types(&ObjectOutcome::accepted()),
            vec![("Accepted".into(), "true".into())]
        );
        let outcome = ObjectOutcome::not_ready("Sync failed!", "x".repeat(5000))
            .with_condition("Synced", false, "Net", "down");
        let conditions = outcome.conditions();
        assert_eq!(conditions[1].reason, "Syncfailed");
        assert!(conditions[1].message.len() <= MAX_CONDITION_MESSAGE_BYTES);
        assert_eq!(conditions[2].r#type, "Synced");
        let ready = ObjectOutcome::ready()
            .because("Synced", "at 3f9a")
            .conditions();
        assert_eq!(
            (ready[1].reason.as_str(), ready[1].message.as_str()),
            ("Synced", "at 3f9a")
        );
        assert_eq!(ready[0].reason, "");
        assert_eq!(retry_after(1), Duration::from_secs(1));
        assert_eq!(retry_after(3), Duration::from_secs(4));
        assert_eq!(retry_after(40), RETRY_MAX);
    }

    /// A stand-in manager: a scripted watch, and the status writes and
    /// releases it receives.
    #[derive(Clone, Default)]
    struct FakePlane {
        events: Arc<StdMutex<Option<tokio::sync::mpsc::UnboundedReceiver<ObjectEvent>>>>,
        statuses: Arc<StdMutex<Vec<UpdateObjectStatusRequest>>>,
        released: Arc<StdMutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl ObjectPlane for FakePlane {
        async fn watch(
            &self,
            _request: WatchObjectsRequest,
        ) -> Result<BoxStream<'static, Result<ObjectEvent, Status>>, Status> {
            let receiver = self
                .events
                .lock()
                .expect("lock")
                .take()
                .ok_or_else(|| Status::unavailable("one watch only"))?;
            Ok(
                tokio_stream::wrappers::UnboundedReceiverStream::new(receiver)
                    .map(Ok)
                    .boxed(),
            )
        }

        async fn update_status(
            &self,
            request: UpdateObjectStatusRequest,
        ) -> Result<UpdateObjectStatusReply, Status> {
            self.statuses.lock().expect("lock").push(request);
            Ok(UpdateObjectStatusReply {
                written: true,
                ..Default::default()
            })
        }

        async fn release(
            &self,
            request: ReleaseObjectRequest,
        ) -> Result<ReleaseObjectReply, Status> {
            self.released.lock().expect("lock").push(request.uid);
            Ok(ReleaseObjectReply::default())
        }
    }

    async fn eventually(what: &str, check: impl Fn() -> bool) {
        for _ in 0..200 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test]
    async fn reconcile_loop_writes_changed_status_and_releases_test() {
        let (events, receiver) = tokio::sync::mpsc::unbounded_channel();
        let plane = FakePlane {
            events: Arc::new(StdMutex::new(Some(receiver))),
            ..Default::default()
        };
        let calls = Arc::new(StdMutex::new(Vec::<(u64, u64)>::new()));
        let seen = calls.clone();
        let (stop_tx, stop) = tokio::sync::watch::channel(false);
        let task = Reconciler::new("gitops", "Repo")
            .resync(Duration::from_secs(3600))
            .on_cleanup(|_| async { Ok(()) })
            .run(plane.clone(), stop, move |request| {
                let seen = seen.clone();
                async move {
                    seen.lock()
                        .expect("lock")
                        .push((request.object.uid, request.object.generation));
                    if request.spec["url"] == "https://x/2" {
                        return Ok(ObjectOutcome::rejected("BadUrl", "not a repo"));
                    }
                    Ok(ObjectOutcome::ready().with_fields(json!({ "commit": "abc" })))
                }
            });
        // Object 3 already reports exactly what the handler will say.
        let mut current = object(3, 1, 3);
        current.status = Some(ObjectStatus {
            observed_generation: 1,
            conditions: ObjectOutcome::ready().conditions(),
            fields_json: json!({ "commit": "abc" }).to_string(),
            ..Default::default()
        });
        events
            .send(ObjectEvent {
                snapshot: true,
                snapshot_done: true,
                upserts: vec![object(1, 1, 1), object(2, 1, 2), current],
                ..Default::default()
            })
            .expect("send");
        eventually("three reconciles", || {
            calls.lock().expect("lock").len() == 3
        })
        .await;
        eventually("two status writes", || {
            plane.statuses.lock().expect("lock").len() == 2
        })
        .await;
        {
            let statuses = plane.statuses.lock().expect("lock");
            let rejected = statuses.iter().find(|s| s.uid == 2).expect("uid 2");
            assert_eq!(rejected.conditions[0].status, "false");
            assert_eq!(rejected.observed_generation, 1);
        }
        // A new generation of 1 reconciles and writes again; deleting 2
        // releases it without the handler.
        let mut deleting = object(2, 1, 5);
        deleting.deleting = true;
        events
            .send(ObjectEvent {
                upserts: vec![object(1, 2, 4), deleting],
                ..Default::default()
            })
            .expect("send");
        eventually("the release", || {
            plane.released.lock().expect("lock").as_slice() == [2]
        })
        .await;
        eventually("generation 2", || {
            plane
                .statuses
                .lock()
                .expect("lock")
                .iter()
                .any(|s| s.uid == 1 && s.observed_generation == 2)
        })
        .await;
        // The deleting object went to cleanup, not to the handler.
        assert_eq!(
            calls
                .lock()
                .expect("lock")
                .iter()
                .filter(|(uid, _)| *uid == 2)
                .count(),
            1
        );
        stop_tx.send(true).expect("stop");
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("stops")
            .expect("joins");
    }
}
