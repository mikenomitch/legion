//! The app reconciler (apps plan §6.3; docs/apps.md, "How an app stays
//! running"; calls entries 13 and 14).
//!
//! It keeps each app's job in `legion-system` matching the app's record,
//! and makes no decisions about what the app does. It runs on the Control
//! lane's leader only, one app at a time, with at most one submit in flight
//! per app:
//!
//! - **Render** the job as a pure function of the record and the build
//!   marker ([`crate::structs::app::render_app_job`]).
//! - **Compare a digest**, not the stored job: the render's digest against
//!   the `managedBy` stamp on the job's latest version. The stored job has
//!   defaults filled in and fields normalized, so a structural diff would
//!   find differences that are not there.
//! - **Check the job's state** too: an enabled app whose job was stopped is
//!   resubmitted, one whose job failed is restarted with backoff, and one
//!   whose job does not run healthy at the rendered version is resubmitted
//!   with a forced reschedule after an operator's write (`legion app
//!   enable`); a disabled app's running job is stopped, and so is a job
//!   still stamped for an app that is no longer installed.
//! - **Submit compare-and-set** on the record it rendered from: a submit
//!   that loses to an uninstall, a disable, or any other write is refused
//!   in the same entry, and the next pass renders from the new record.
//! - **Submit once per transition**, through the normal RunJob path as
//!   `legion:app-reconciler`, checked against the grant recorded on the app,
//!   with `idempotency_key = app:<name>:<digest>:<from>`, where `<from>` is
//!   the state the transition starts from (the latest version id, `none`,
//!   or `stopped-<version>`).
//!
//! Triggers: an app write (the RPC handlers nudge [`AppNudge`]), any job
//! event of an app's job (a version stored some other way is reverted), node
//! events (a node joining or rebuilding moves bundled apps' build markers),
//! acquiring Control leadership (which also seeds bundled apps), and a full
//! pass every [`FULL_PASS_INTERVAL`]. Bursts coalesce: one pass per app at a
//! time, and anything that arrives during a pass queues exactly one more.
//!
//! Failures back off from 5 seconds to 5 minutes, with the app `degraded:
//! reconcile failing: <error>` and `app.reconcile_failed` at most every 10
//! minutes; a submit the manager refuses on validation (a schema, grant, or
//! quota problem) waits for the next change to the app or to its build
//! marker instead. A source that could not be reached (a registry blip) is
//! not a refusal: it backs off and retries. Everything the reconciler keeps
//! in memory is cleared when this manager acquires Control leadership.

use crate::manager::server::ManagerDaemonService;
use crate::prelude::*;
use crate::storage::app_store::{AppJobAllocations, AppJobVersion};
use crate::structs::app::{
    app_name_of_job, decide_build_marker, render_app_job, AppGrant, AppReconcileState, AppRecord,
    AppSettings, AppSource, BuildMarker, ManagedBy, NodeBuild, Placement, RenderContext,
    RenderedApp, BOOTSTRAP_PRINCIPAL, BUILD_ID_ATTR, BUILD_VERSION_ATTR,
};
use crate::structs::config::AppSeedConfig;
use crate::structs::namespace::SYSTEM_NAMESPACE;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// The backstop: a full pass this often, in case an event was missed.
pub const FULL_PASS_INTERVAL: Duration = Duration::from_secs(60);

/// How often the Control-lane leader looks for object cleanups that ran out
/// (docs/app-objects.md, "Deleting").
pub const OBJECT_CLEANUP_SWEEP: Duration = Duration::from_secs(5);
/// The first retry after a failed submit; doubles up to [`MAX_BACKOFF`].
pub const FIRST_BACKOFF: Duration = Duration::from_secs(5);
pub const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// `app.reconcile_failed` repeats at most this often for one app.
pub const FAILURE_EVENT_INTERVAL: Duration = Duration::from_secs(600);
/// How often a manager that does not lead looks again.
const LEADER_POLL: Duration = Duration::from_secs(1);

/// Wakes the reconciler: `Some(app)` for one app, `None` for a full pass.
#[derive(Debug, Clone)]
pub struct AppNudge(tokio::sync::mpsc::UnboundedSender<Option<String>>);

impl AppNudge {
    /// A nudge with nothing listening (tests that drive passes by hand).
    pub fn detached() -> (Self, tokio::sync::mpsc::UnboundedReceiver<Option<String>>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (AppNudge(tx), rx)
    }

    /// Reconcile `app` soon.
    pub fn app(&self, app: &str) {
        let _ = self.0.send(Some(app.to_string()));
    }

    /// Reconcile every app soon.
    pub fn all(&self) {
        let _ = self.0.send(None);
    }
}

/// What the reconciler needs besides the service.
#[derive(Debug, Clone)]
pub struct ReconcilerSettings {
    /// The manager config's `apps` block, read only when seeding.
    pub seed: BTreeMap<String, AppSeedConfig>,
    /// The build this manager runs: a new bundled manifest is adopted only
    /// when it is the build marker's (the leader can read only manifests
    /// compiled into its own binary).
    pub own_build: BuildMarker,
}

/// What one pass did for one app.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassOutcome {
    /// The record is gone.
    Missing,
    /// The job already matches.
    Unchanged,
    /// Submitted the job (`deduplicated` when the key was seen before and
    /// the job shows the submit took).
    Submitted { action: String, deduplicated: bool },
    /// Stopped a disabled app's job.
    Stopped,
    /// Nothing to submit yet (a bundled app with no build marker).
    Waiting(String),
    /// Still in backoff after a failure, or parked after a refusal: no
    /// submit this time.
    Skipped,
    /// The submit failed and will be retried with backoff.
    Failed(String),
    /// The manager refused the submit; waiting for a change.
    Refused(String),
    /// The record does not decode; nothing is done until it is uninstalled.
    Corrupt(String),
}

/// Per-app memory of failures, leader-local. Cleared when this manager
/// (re)acquires Control leadership: a new leader retries everything once.
#[derive(Debug, Clone, Default)]
struct AppRuntime {
    failures: u32,
    retry_at: Option<Instant>,
    /// The record's `updated_at` when it failed: a write resets the backoff.
    failed_input: String,
    last_failure_event: Option<Instant>,
    /// (record `updated_at`, render digest) of a refused submit.
    refused: Option<(String, String)>,
    /// Restarts of a job that failed (its reschedule policy gave up, a KV
    /// `onDelete = "fail"`, a gang that lost its quorum), and when the next
    /// is due: the same 5s to 5m backoff as a failed submit.
    restarts: u32,
    restart_at: Option<Instant>,
}

/// The app reconciler.
pub struct AppReconciler {
    svc: Arc<ManagerDaemonService>,
    settings: ReconcilerSettings,
    runtime: BTreeMap<String, AppRuntime>,
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn backoff_for(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    FIRST_BACKOFF
        .saturating_mul(1u32 << doublings)
        .min(MAX_BACKOFF)
}

/// Whether the manager refused a submit for a reason a retry cannot fix:
/// the job fails validation, the grant check, or a quota. A source that
/// could not be reached is `Unavailable`, and retried with backoff.
fn is_refusal(status: &tonic::Status) -> bool {
    use tonic::Code;
    match status.code() {
        Code::InvalidArgument
        | Code::PermissionDenied
        | Code::ResourceExhausted
        | Code::AlreadyExists
        | Code::OutOfRange => true,
        // A follower's not-the-leader answer is transient; any other
        // precondition is the job's.
        Code::FailedPrecondition => !status.message().contains("not the raft leader"),
        _ => false,
    }
}

/// How many tasks one replica of an app's job has.
pub fn task_count(record: &AppRecord) -> u32 {
    let mut job = record.manifest.job.clone();
    let mut count = 0;
    crate::structs::app::for_each_task(&mut job, |_| count += 1);
    count.max(1)
}

/// An app's status, as `legion app list` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStatusView {
    /// `running`, `upgrading`, `degraded`, `pending`, or `stopped` (and
    /// `corrupt` for a record that does not decode, which never gets here).
    pub status: &'static str,
    pub detail: String,
    /// Allocations the app should run.
    pub desired: u32,
}

/// What an app's status is derived from, besides its record.
#[derive(Debug, Clone, Default)]
pub struct AppJobState {
    /// The job's latest version, when the job exists.
    pub version: Option<AppJobVersion>,
    pub allocations: AppJobAllocations,
    /// The newest rollout to the latest version: (status, message).
    pub rollout: Option<(String, String)>,
}

impl AppJobState {
    /// Read app `record`'s job as status derives it.
    pub fn read(store: &crate::storage::store::Store, record: &AppRecord) -> R<AppJobState> {
        let job_id = record.job_id();
        let version = store.app_job_version(&job_id)?;
        let allocations = store.app_job_allocations(&job_id)?;
        let rollout = match &version {
            Some(version) => store.rollout_to_version(&version.job_config_id)?,
            None => None,
        };
        Ok(AppJobState {
            version,
            allocations,
            rollout,
        })
    }

    /// Every allocation the record asks for runs healthy at the job's
    /// latest version.
    pub fn healthy(&self, record: &AppRecord) -> bool {
        self.version
            .as_ref()
            .is_some_and(|version| !version.is_stopped() && !version.is_failed())
            && self.allocations.running
                >= record.settings.replicas.saturating_mul(task_count(record))
    }
}

/// Derive an app's status from its record, its job, and its allocations
/// (docs/apps.md, "Status"). Only healthy allocations of the job's latest
/// version count as `running`: an upgrade whose new version is not healthy
/// while the old one still serves is `upgrading`, and one whose rollout
/// failed (or rolled back) is `degraded: upgrade failing`, never `running`
/// on the old version.
pub fn derive_status(
    record: &AppRecord,
    job: &AppJobState,
    blocked: Option<&str>,
) -> AppStatusView {
    if !record.enabled {
        return AppStatusView {
            status: "stopped",
            detail: "disabled".to_string(),
            desired: 0,
        };
    }
    let desired = record.settings.replicas.saturating_mul(task_count(record));
    if !record.state.last_error.is_empty() {
        return AppStatusView {
            status: "degraded",
            detail: record.state.last_error.clone(),
            desired,
        };
    }
    let waiting = record.state.marker_reason.clone();
    if waiting.starts_with(crate::structs::app::UPGRADE_HELD) {
        return AppStatusView {
            status: "degraded",
            detail: waiting,
            desired,
        };
    }
    let Some(version) = job.version.as_ref() else {
        return AppStatusView {
            status: "pending",
            detail: if waiting.is_empty() {
                "waiting for the app reconciler to submit its job".to_string()
            } else {
                waiting
            },
            desired,
        };
    };
    if version.is_stopped() {
        return AppStatusView {
            status: "pending",
            detail: "its job was stopped; the reconciler restarts it".to_string(),
            desired,
        };
    }
    if version.is_failed() {
        return AppStatusView {
            status: "degraded",
            detail: "its job failed; the reconciler restarts it with backoff".to_string(),
            desired,
        };
    }
    let running = job.allocations.running;
    let previous = job.allocations.previous;
    if running >= desired {
        return AppStatusView {
            status: "running",
            detail: waiting,
            desired,
        };
    }
    if let Some((rollout, message)) = &job.rollout {
        if rollout == "failed" {
            let serving = if previous > 0 {
                format!("; the previous version still serves {previous}")
            } else {
                String::new()
            };
            let why = if message.trim().is_empty() {
                String::new()
            } else {
                format!(" ({})", message.trim())
            };
            return AppStatusView {
                status: "degraded",
                detail: format!(
                    "upgrade failing: the rollout to job version {} failed{why}, {running}/{desired} \
                     of it healthy{serving}; fix the app and run `legion app enable {}` to retry",
                    version.version, record.name
                ),
                desired,
            };
        }
    }
    if previous > 0 {
        return AppStatusView {
            status: "upgrading",
            detail: format!(
                "{running}/{desired} of job version {} healthy; the previous version still serves \
                 {previous}",
                version.version
            ),
            desired,
        };
    }
    if running == 0 {
        return AppStatusView {
            status: "pending",
            detail: blocked
                .map(str::to_string)
                .unwrap_or_else(|| "no allocation is running yet".to_string()),
            desired,
        };
    }
    AppStatusView {
        status: "degraded",
        detail: format!("{running}/{desired} running"),
        desired,
    }
}

/// The record a bundled app is seeded with: its manifest's defaults, the
/// manager config's `apps.<name>` overrides, and its permissions granted by
/// `legion:bootstrap` (calls entry 17).
pub fn seed_record(
    manifest: &crate::structs::app::AppManifest,
    seed: Option<&AppSeedConfig>,
) -> AppRecord {
    let now = now_rfc3339();
    let mut settings = AppSettings::from_manifest(manifest);
    let mut config = manifest.default_config();
    let mut enabled = true;
    if let Some(seed) = seed {
        enabled = seed.enabled.unwrap_or(true);
        if let Some(placement) = &seed.placement {
            match Placement::parse(placement) {
                Ok(placement) => settings.placement = placement,
                Err(err) => {
                    warn!(app = %manifest.name, "apps.{}.placement: {err}; using the manifest's", manifest.name)
                }
            }
        }
        if let Some(replicas) = seed.replicas {
            let candidate = AppSettings {
                replicas,
                ..settings.clone()
            };
            match candidate.validate() {
                Ok(()) => settings = candidate,
                Err(err) => {
                    warn!(app = %manifest.name, "apps.{}.replicas: {err}; using the manifest's", manifest.name)
                }
            }
        }
        if let Some(seed_config) = &seed.config {
            match crate::structs::app::validate_app_config(manifest, seed_config) {
                Ok(()) => config = seed_config.clone(),
                Err(err) => warn!(
                    app = %manifest.name,
                    "apps.{}.config does not match the app's config schema ({err}); seeding its default config",
                    manifest.name
                ),
            }
        }
    }
    AppRecord {
        name: manifest.name.clone(),
        source: AppSource::Bundled,
        source_ref: String::new(),
        manifest_digest: manifest.digest(),
        enabled,
        settings,
        config,
        grant: AppGrant {
            rules: manifest.permissions.clone(),
            granted_by: BOOTSTRAP_PRINCIPAL.to_string(),
            granted_at: now.clone(),
        },
        installed_by: BOOTSTRAP_PRINCIPAL.to_string(),
        installed_at: now.clone(),
        updated_at: now,
        state: AppReconcileState::default(),
        manifest: manifest.clone(),
        // The store assigns it when the record is inserted.
        uid: 0,
    }
}

/// Seed a record for every bundled app that has none (apps plan §6.3,
/// "Seeding"). The manager config's `apps` block is read only here: a
/// bundled app that already has a record keeps it, and the block is logged
/// as not applied. Returns the apps it seeded.
pub fn seed_bundled_apps(
    store: &crate::storage::store::Store,
    seed: &BTreeMap<String, AppSeedConfig>,
    build: &BuildMarker,
) -> R<Vec<String>> {
    seed_bundled_apps_noting(store, seed, build, true)
}

/// [`seed_bundled_apps`], saying what the manager config's `apps` block did
/// not apply only when `note` (once per leadership term, not every pass).
fn seed_bundled_apps_noting(
    store: &crate::storage::store::Store,
    seed: &BTreeMap<String, AppSeedConfig>,
    build: &BuildMarker,
    note: bool,
) -> R<Vec<String>> {
    let manifests = crate::apps::bundled_manifests()?;
    for name in seed.keys().filter(|_| note) {
        if !crate::apps::is_bundled(name) {
            warn!(
                app = %name,
                "The manager config's apps.{name} names no app bundled with this build; ignoring it"
            );
        }
    }
    let mut seeded = Vec::new();
    for manifest in manifests {
        let configured = seed.get(&manifest.name);
        if store.app_exists(&manifest.name)? {
            if configured.is_some() && note {
                info!(
                    app = %manifest.name,
                    "The manager config's apps.{} applies only when the cluster is first seeded; \
                     this cluster already has the app, so it was not applied (use `legion app`)",
                    manifest.name
                );
            }
            continue;
        }
        let record = seed_record(manifest, configured);
        let enabled = record.enabled;
        if store.seed_app(record)? {
            info!(
                app = %manifest.name,
                enabled,
                "Seeded bundled app; its permissions are granted by legion:bootstrap"
            );
            store.events().app_installed(
                &manifest.name,
                &build.short(),
                "bundled",
                BOOTSTRAP_PRINCIPAL,
            );
            seeded.push(manifest.name.clone());
        }
    }
    Ok(seeded)
}

/// What adopting a bundled app's embedded manifest came to.
enum Adoption {
    /// Nothing to adopt: the same manifest, no embedded one, or this
    /// manager does not run the marker's version.
    Kept,
    /// The record now holds the embedded manifest.
    Adopted(Box<AppRecord>),
    /// The stored config does not match the embedded manifest's config
    /// schema: the reason, for `marker_reason`.
    Held(String),
}

impl AppReconciler {
    pub fn new(svc: Arc<ManagerDaemonService>, settings: ReconcilerSettings) -> Self {
        AppReconciler {
            svc,
            settings,
            runtime: BTreeMap::new(),
        }
    }

    fn store(&self) -> &crate::storage::store::Store {
        &self.svc.store
    }

    /// Seed bundled apps (see [`seed_bundled_apps`]).
    pub fn seed(&self) -> R<Vec<String>> {
        seed_bundled_apps(self.store(), &self.settings.seed, &self.settings.own_build)
    }

    /// [`Self::seed`] without repeating what the manager config's `apps`
    /// block did not apply: the full pass's re-seed, after the first of a
    /// term.
    fn seed_quietly(&self) -> R<Vec<String>> {
        seed_bundled_apps_noting(
            self.store(),
            &self.settings.seed,
            &self.settings.own_build,
            false,
        )
    }

    /// Forget every app's failures, backoffs and parked refusals: what a
    /// manager does when it (re)acquires Control leadership, so a new term
    /// retries everything once instead of trusting what an earlier term of
    /// its own decided.
    pub fn reset_runtime(&mut self) {
        self.runtime.clear();
    }

    /// Reconcile every installed app once, and every job in
    /// `legion-system` still stamped for an app that is no longer installed.
    pub async fn reconcile_all(&mut self) -> Vec<(String, PassOutcome)> {
        let mut names: BTreeSet<String> = match self.store().app_names() {
            Ok(names) => names.into_iter().collect(),
            Err(err) => {
                warn!(error = ?err, "App reconciler could not list apps");
                return Vec::new();
            }
        };
        let installed = names.clone();
        match self.store().stamped_app_jobs() {
            Ok(jobs) => names.extend(
                jobs.into_iter()
                    .filter(|(_, app, status)| status != "stopped" && !installed.contains(app))
                    .map(|(_, app, _)| app),
            ),
            Err(err) => warn!(error = ?err, "App reconciler could not list app jobs"),
        }
        let mut outcomes = Vec::new();
        for name in names {
            let outcome = self.reconcile_app(&name).await;
            outcomes.push((name, outcome));
        }
        outcomes
    }

    /// When the next backoff retry (or restart of a failed job) is due, if
    /// any.
    pub fn next_retry(&self) -> Option<(Instant, String)> {
        self.runtime
            .iter()
            .flat_map(|(name, runtime)| {
                [runtime.retry_at, runtime.restart_at]
                    .into_iter()
                    .flatten()
                    .map(move |at| (at, name.clone()))
            })
            .min()
    }

    /// Report `legion_apps{status}` from the stored records; a record that
    /// does not decode counts as `corrupt`.
    pub fn publish_metrics(&self) {
        let Ok(rows) = self.store().list_app_rows() else {
            return;
        };
        let mut counts: BTreeMap<&'static str, i64> = BTreeMap::new();
        for status in [
            "running",
            "upgrading",
            "degraded",
            "pending",
            "stopped",
            "corrupt",
        ] {
            counts.insert(status, 0);
        }
        for row in &rows {
            let status = match row {
                Ok(record) => {
                    let job = AppJobState::read(self.store(), record).unwrap_or_default();
                    derive_status(record, &job, None).status
                }
                Err(_) => "corrupt",
            };
            *counts.entry(status).or_default() += 1;
        }
        let entries: Vec<(Vec<String>, i64)> = counts
            .into_iter()
            .map(|(status, count)| (vec![status.to_string()], count))
            .collect();
        crate::manager::telemetry::metrics().apps.set_all(&entries);
    }

    /// The nodes an app's placement allows, with their builds: what its
    /// build marker is decided from.
    fn eligible_builds(&self, record: &AppRecord) -> R<Vec<NodeBuild>> {
        let rendered =
            render_app_job(record, RenderContext { marker: None }).map_err(|err| anyhow!(err))?;
        let mut job = rendered.jobspec;
        let mut rules: Option<(Value, Value)> = None;
        crate::structs::app::for_each_task(&mut job, |task| {
            if rules.is_none() {
                rules = Some((
                    task.get("permit").cloned().unwrap_or(Value::Null),
                    task.get("constraints").cloned().unwrap_or(Value::Null),
                ));
            }
        });
        let (permit, constraints) = rules.unwrap_or((Value::Null, Value::Null));
        let permit = crate::constraints::PermitSet::new(&permit, SYSTEM_NAMESPACE)?;
        let constraints = crate::constraints::ConstraintSet::new(&constraints)?;
        Ok(self
            .store()
            .get_nodes()?
            .into_iter()
            .filter(|node| permit.is_permitted(node) && constraints.is_satisfied(node, None))
            .map(|node| {
                let attr = |key: &str| {
                    node.dynamic_attrs
                        .get(key)
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                };
                NodeBuild {
                    version: attr(BUILD_VERSION_ATTR),
                    build_id: attr(BUILD_ID_ATTR),
                    ready: matches!(node.status.as_str(), "active" | "ready")
                        && node.availability != "unavailable",
                    node_id: node.id,
                }
            })
            .collect())
    }

    /// Adopt the bundled manifest this binary carries for `record`, when it
    /// differs from the stored one and this manager runs the marker's
    /// version (apps plan §6.3, "New bundled manifests"). The version, not
    /// the build id: builds of one version for other architectures or
    /// packagings carry the same manifest, and a fleet that mixes them
    /// would otherwise never adopt. Its permissions are recorded as granted
    /// by `legion:bootstrap`.
    ///
    /// A stored config that does not match the new manifest's config schema
    /// holds the adoption: the new build's app would refuse that config and
    /// crash-loop, so the job stays on the previous marker until a config
    /// that matches is set (`legion app config set` accepts one that matches
    /// the held manifest).
    fn adopt_bundled_manifest(&self, record: &AppRecord, marker: &BuildMarker) -> R<Adoption> {
        if marker.version != self.settings.own_build.version {
            return Ok(Adoption::Kept);
        }
        let Some(embedded) = crate::apps::bundled_manifest(&record.name) else {
            return Ok(Adoption::Kept);
        };
        let digest = embedded.digest();
        if digest == record.manifest_digest {
            return Ok(Adoption::Kept);
        }
        if let Err(err) = crate::structs::app::validate_app_config(embedded, &record.config) {
            return Ok(Adoption::Held(format!(
                "{}build {} changes {}'s config schema, and the stored config does not match it \
                 ({err}); the app stays on its previous build until `legion app config set {} \
                 <file>` sets a config that does",
                crate::structs::app::UPGRADE_HELD,
                marker.short(),
                record.name,
                record.name
            )));
        }
        let diff = crate::structs::app::permission_diff(&record.grant.rules, &embedded.permissions);
        let now = now_rfc3339();
        let mut adopted = record.clone();
        adopted.manifest = embedded.clone();
        adopted.manifest_digest = digest;
        adopted.grant = AppGrant {
            rules: embedded.permissions.clone(),
            granted_by: BOOTSTRAP_PRINCIPAL.to_string(),
            granted_at: now.clone(),
        };
        adopted.updated_at = now;
        let mut change = format!("manifest from build {}", marker.short());
        if !diff.added.is_empty() || !diff.removed.is_empty() {
            change.push_str(&format!(
                "; permissions: {} added, {} removed (granted by legion:bootstrap)",
                diff.added.len(),
                diff.removed.len()
            ));
        }
        adopted.state.pending_change = change;
        self.store()
            .update_app(adopted, &record.updated_at)
            .context("adopting the bundled manifest")?;
        let from = record
            .state
            .marker
            .as_ref()
            .map(BuildMarker::short)
            .unwrap_or_else(|| "-".to_string());
        self.store().events().app_upgraded(
            &record.name,
            &from,
            &marker.short(),
            diff.added.len(),
            diff.removed.len(),
            BOOTSTRAP_PRINCIPAL,
        );
        info!(
            app = %record.name,
            build = %marker.short(),
            added = diff.added.len(),
            removed = diff.removed.len(),
            "Adopted the bundled manifest of the app's new build"
        );
        Ok(Adoption::Adopted(Box::new(
            self.store()
                .get_app(&record.name)?
                .context("the app vanished while its manifest was adopted")?,
        )))
    }

    fn write_state(&self, name: &str, before: &AppReconcileState, after: AppReconcileState) {
        if *before == after {
            return;
        }
        if let Err(err) = self.store().set_app_state(name, after) {
            warn!(app = name, error = ?err, "App reconciler could not record its status");
        }
    }

    /// One pass for one app.
    #[instrument(skip(self), fields(app = name))]
    pub async fn reconcile_app(&mut self, name: &str) -> PassOutcome {
        match self.reconcile_app_inner(name).await {
            Ok(outcome) => outcome,
            Err(err) => {
                warn!(app = name, error = ?err, "App reconcile pass failed");
                PassOutcome::Failed(format!("{err:#}"))
            }
        }
    }

    /// No record for `name`: stop and revoke a job still stamped for it.
    /// An uninstall that raced a submit in flight could otherwise leave
    /// `legion-system/<name>` running, with a fresh grant and nothing that
    /// manages it.
    fn sweep_orphan(&mut self, name: &str) -> R<PassOutcome> {
        let job_id = crate::structs::app::app_job_id(name);
        let Some(version) = self.store().app_job_version(&job_id)? else {
            return Ok(PassOutcome::Missing);
        };
        let stamped = version
            .managed_by
            .as_ref()
            .is_some_and(|stamp| stamp.app == name);
        if !stamped || version.is_stopped() {
            return Ok(PassOutcome::Missing);
        }
        let key = format!("app:{name}:orphan-stop:{}", version.job_config_id);
        let svc = self.svc.clone();
        let stop_job = job_id.clone();
        crate::manager::server::run_blocking(move || svc.stop_app_job(&stop_job, &key)).map_err(
            |status| {
                anyhow!(
                    "stopping the job of uninstalled app {name}: {}",
                    status.message()
                )
            },
        )?;
        let revoked = self.store().revoke_job_grants(
            &job_id,
            None,
            crate::structs::app::RECONCILER_PRINCIPAL,
        )?;
        warn!(
            app = name,
            job = %job_id,
            revoked,
            "Stopped a job stamped for an app that is not installed (an uninstall raced its submit)"
        );
        self.store().events().app_reconciled(
            name,
            "stopped the job of an app that is not installed",
            &version
                .managed_by
                .as_ref()
                .map(|stamp| stamp.digest.clone())
                .unwrap_or_default(),
            &format!("{revoked} grant(s) revoked"),
        );
        Ok(PassOutcome::Stopped)
    }

    async fn reconcile_app_inner(&mut self, name: &str) -> R<PassOutcome> {
        let mut record = match self.store().get_app_row(name)? {
            None => {
                self.runtime.remove(name);
                return self.sweep_orphan(name);
            }
            Some(Err(corrupt)) => {
                // Nothing to render from: the job is left as it is, and
                // status says why (`legion app uninstall` removes it).
                self.runtime.remove(name);
                return Ok(PassOutcome::Corrupt(corrupt.error));
            }
            Some(Ok(record)) => record,
        };
        let job_id = record.job_id();
        let runtime = self.runtime.entry(name.to_string()).or_default().clone();
        if let Some(retry_at) = runtime.retry_at {
            if Instant::now() < retry_at && runtime.failed_input == record.updated_at {
                return Ok(PassOutcome::Skipped);
            }
        }
        let before = record.state.clone();
        let mut state = record.state.clone();
        let version = self.store().app_job_version(&job_id)?;

        if !record.enabled {
            state.last_error.clear();
            state.pending_change.clear();
            state.marker_reason.clear();
            let running = version.as_ref().is_some_and(|v| !v.is_stopped());
            if !running {
                self.write_state(name, &before, state);
                self.clear_failure(name);
                return Ok(PassOutcome::Unchanged);
            }
            let version = version.expect("running");
            let key = format!("app:{name}:stop:{}", version.job_config_id);
            let svc = self.svc.clone();
            let stop_job = job_id.clone();
            let stopped =
                crate::manager::server::run_blocking(move || svc.stop_app_job(&stop_job, &key));
            return match stopped {
                Ok(_) => {
                    state.last_action = "stopped (app disabled)".to_string();
                    state.last_at = now_rfc3339();
                    self.store().events().app_reconciled(
                        name,
                        "stopped (app disabled)",
                        &state.last_digest,
                        &version.job_config_id,
                    );
                    self.write_state(name, &before, state);
                    self.clear_failure(name);
                    Ok(PassOutcome::Stopped)
                }
                Err(status) => Ok(self.failed(name, &record, &before, state, &status, "")),
            };
        }

        let marker = if record.is_bundled() {
            let eligible = self.eligible_builds(&record)?;
            let decision = decide_build_marker(record.state.marker.as_ref(), &eligible);
            state.marker = decision.marker.clone();
            state.marker_reason = decision.reason;
            let Some(marker) = decision.marker else {
                let reason = state.marker_reason.clone();
                self.write_state(name, &before, state);
                return Ok(PassOutcome::Waiting(reason));
            };
            match self.adopt_bundled_manifest(&record, &marker)? {
                Adoption::Kept => Some(marker),
                Adoption::Adopted(adopted) => {
                    state.pending_change = adopted.state.pending_change.clone();
                    record = *adopted;
                    Some(marker)
                }
                Adoption::Held(reason) => {
                    // The job stays where it is: rendered for the marker it
                    // already has (or, with none yet, the agreed one).
                    if before.marker_reason != reason {
                        warn!(app = name, "{reason}");
                        self.store()
                            .events()
                            .app_reconcile_failed(name, &reason, true);
                    }
                    let held = record.state.marker.clone().unwrap_or(marker);
                    state.marker = Some(held.clone());
                    state.marker_reason = reason;
                    Some(held)
                }
            }
        } else {
            None
        };

        let rendered = match render_app_job(
            &record,
            RenderContext {
                marker: marker.as_ref(),
            },
        ) {
            Ok(rendered) => rendered,
            Err(err) => {
                let status = tonic::Status::invalid_argument(err);
                return Ok(self.failed(name, &record, &before, state, &status, ""));
            }
        };

        let stamp = ManagedBy {
            app: name.to_string(),
            digest: rendered.digest.clone(),
            object: None,
        };
        let failed = version.as_ref().is_some_and(AppJobVersion::is_failed);
        let current = version.as_ref().is_some_and(|v| {
            !v.is_stopped() && !v.is_failed() && v.managed_by.as_ref() == Some(&stamp)
        });
        // The job already matches the record. A write to the record since
        // the last action (`legion app enable`, or `legion app upgrade` with
        // the same manifest) on a job that does not run healthy at that
        // version is a retry: resubmitted with a forced reschedule, which is
        // how an upgrade whose rollout failed or rolled back is tried again.
        let job = if current {
            Some(AppJobState::read(self.store(), &record)?)
        } else {
            None
        };
        let healthy = job.as_ref().is_some_and(|job| job.healthy(&record));
        let retry = current && !record.state.pending_change.trim().is_empty() && !healthy;
        if current && !retry {
            state.last_digest = rendered.digest.clone();
            state.last_error.clear();
            state.pending_change.clear();
            self.write_state(name, &before, state);
            self.clear_failure(name);
            if healthy {
                if let Some(runtime) = self.runtime.get_mut(name) {
                    runtime.restarts = 0;
                    runtime.restart_at = None;
                }
            }
            return Ok(PassOutcome::Unchanged);
        }
        // Checked after "already current": a refusal is parked on the
        // render, and a job that already runs it needs no submit at all.
        if runtime.refused.as_ref() == Some(&(record.updated_at.clone(), rendered.digest.clone())) {
            self.write_state(name, &before, state);
            return Ok(PassOutcome::Skipped);
        }
        if failed {
            if let Some(at) = runtime.restart_at {
                let now = Instant::now();
                if now < at {
                    state.last_error = format!(
                        "its job failed; restarting it in {}s (restart {})",
                        at.saturating_duration_since(now).as_secs().max(1),
                        runtime.restarts + 1
                    );
                    self.write_state(name, &before, state);
                    return Ok(PassOutcome::Skipped);
                }
            }
        }

        let (from, action, detail) = if retry {
            let version = version.as_ref().expect("current");
            (
                format!("{}@{}", version.job_config_id, record.updated_at),
                format!(
                    "resubmitted the job, which did not run healthy at version {} ({})",
                    version.version,
                    record.state.pending_change.trim()
                ),
                format!("from {}", version.job_config_id),
            )
        } else {
            describe_transition(name, &record, version.as_ref(), &stamp)
        };
        let key = format!("app:{name}:{}:{from}", rendered.digest);
        let svc = self.svc.clone();
        let submit_record = record.clone();
        let submit_render = rendered.clone();
        let submitted = crate::manager::server::run_blocking(move || {
            svc.submit_app_job(&submit_record, &submit_render, &key, retry)
        });
        match submitted {
            Ok(submit) => {
                let action = if failed {
                    let runtime = self.runtime.entry(name.to_string()).or_default();
                    runtime.restarts += 1;
                    runtime.restart_at = Some(Instant::now() + backoff_for(runtime.restarts));
                    format!("{action} (restart {})", runtime.restarts)
                } else {
                    action
                };
                state.last_digest = rendered.digest.clone();
                state.last_action = action.clone();
                state.last_error.clear();
                state.last_at = now_rfc3339();
                state.pending_change.clear();
                if submit.deduplicated {
                    debug!(
                        app = name,
                        "App submit deduplicated: the previous leader's took"
                    );
                } else {
                    self.store()
                        .events()
                        .app_reconciled(name, &action, &rendered.digest, &detail);
                }
                info!(
                    app = name,
                    digest = %rendered.digest,
                    action = %action,
                    deduplicated = submit.deduplicated,
                    outcome = %submit.message,
                    "Reconciled app"
                );
                self.write_state(name, &before, state);
                self.clear_failure(name);
                Ok(PassOutcome::Submitted {
                    action,
                    deduplicated: submit.deduplicated,
                })
            }
            Err(status) if status.code() == tonic::Code::Aborted => {
                // The record moved under the submit (compare-and-set): the
                // write that moved it has queued a pass of its own, which
                // renders from the new record.
                debug!(app = name, "{}", status.message());
                Ok(PassOutcome::Skipped)
            }
            Err(status) => {
                Ok(self.failed(name, &record, &before, state, &status, &rendered.digest))
            }
        }
    }

    fn clear_failure(&mut self, name: &str) {
        if let Some(runtime) = self.runtime.get_mut(name) {
            runtime.failures = 0;
            runtime.retry_at = None;
            runtime.refused = None;
            runtime.failed_input.clear();
        }
    }

    /// Record a failed submit: a refusal parks until the record or the
    /// render changes; anything else backs off.
    fn failed(
        &mut self,
        name: &str,
        record: &AppRecord,
        before: &AppReconcileState,
        mut state: AppReconcileState,
        status: &tonic::Status,
        digest: &str,
    ) -> PassOutcome {
        let message = status
            .message()
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let refused = is_refusal(status);
        let now = Instant::now();
        let runtime = self.runtime.entry(name.to_string()).or_default();
        let announce = runtime
            .last_failure_event
            .is_none_or(|at| now.duration_since(at) >= FAILURE_EVENT_INTERVAL)
            || (refused && runtime.refused.is_none());
        if refused {
            runtime.refused = Some((record.updated_at.clone(), digest.to_string()));
            runtime.retry_at = None;
            runtime.failures = 0;
            state.last_error = format!("reconcile refused: {message}");
        } else {
            runtime.failures += 1;
            runtime.retry_at = Some(now + backoff_for(runtime.failures));
            runtime.failed_input = record.updated_at.clone();
            state.last_error = format!("reconcile failing: {message}");
        }
        if announce {
            runtime.last_failure_event = Some(now);
        }
        state.last_at = now_rfc3339();
        warn!(
            app = name,
            refused,
            code = ?status.code(),
            error = %message,
            "App reconcile failed"
        );
        if announce {
            self.svc
                .store
                .events()
                .app_reconcile_failed(name, &message, refused);
        }
        self.write_state(name, before, state);
        if refused {
            PassOutcome::Refused(message)
        } else {
            PassOutcome::Failed(message)
        }
    }
}

/// The `<from>` of the idempotency key, and what the submit does, in the
/// words of the `app.reconciled` event.
fn describe_transition(
    name: &str,
    record: &AppRecord,
    version: Option<&AppJobVersion>,
    stamp: &ManagedBy,
) -> (String, String, String) {
    let pending = record.state.pending_change.trim();
    match version {
        None => (
            "none".to_string(),
            if pending.is_empty() {
                "created the job".to_string()
            } else {
                format!("created the job ({pending})")
            },
            String::new(),
        ),
        Some(version) if version.is_failed() => (
            format!("failed-{}", version.job_config_id),
            "restarted the job, which had failed".to_string(),
            format!("from {}", version.job_config_id),
        ),
        Some(version) if version.is_stopped() => (
            format!("stopped-{}", version.job_config_id),
            if pending.is_empty() || pending == "enabled" {
                "started the job, which was stopped".to_string()
            } else {
                format!("started the job, which was stopped ({pending})")
            },
            format!("from {}", version.job_config_id),
        ),
        Some(version) => {
            let action = match &version.managed_by {
                Some(previous) if previous.app == name && previous.digest != stamp.digest => {
                    if pending.is_empty() {
                        "re-rendered the job".to_string()
                    } else {
                        pending.to_string()
                    }
                }
                _ => format!(
                    "reverted a direct edit by {}",
                    version.submitted_by.as_deref().unwrap_or("someone else")
                ),
            };
            (
                version.job_config_id.clone(),
                action,
                format!("from {}", version.job_config_id),
            )
        }
    }
}

/// What the reconcile loop should look at next.
#[derive(Debug, Default)]
struct Pending {
    full: bool,
    apps: BTreeSet<String>,
}

impl Pending {
    fn take(&mut self) -> Pending {
        std::mem::take(self)
    }

    fn note_event(&mut self, event: &crate::storage::events::SchedulerEventRecord) {
        let kind = event.kind.as_str();
        if kind.starts_with("app.") {
            return;
        }
        if kind.starts_with("job.") {
            if let Some(app) = event.job_id.as_deref().and_then(app_name_of_job) {
                self.apps.insert(app.to_string());
            }
            return;
        }
        // A node joining, leaving, or re-registering on a new build can
        // move a bundled app's marker.
        if kind.starts_with("node.") {
            self.apps
                .extend(crate::apps::BUNDLED.iter().map(|app| app.name.to_string()));
        }
    }
}

/// Start the reconcile loop on this manager. It acts only while this
/// manager leads the Control lane. Returns the nudge the RPC handlers wake
/// it with.
pub fn spawn_reconciler(svc: Arc<ManagerDaemonService>, settings: ReconcilerSettings) -> AppNudge {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Option<String>>();
    let nudge = AppNudge(tx);
    let mut events = svc.store.subscribe_events();
    let mut reconciler = AppReconciler::new(svc.clone(), settings);
    tokio::spawn(async move {
        let mut was_leader = false;
        let mut pending = Pending::default();
        let mut last_full: Option<Instant> = None;
        let mut last_cleanup_sweep: Option<Instant> = None;
        // The first full pass of a term says what the manager config's
        // `apps` block did not apply; later passes seed quietly.
        let mut first_pass = true;
        loop {
            let leader = svc.store.is_raft_leader(crate::storage::Lane::Control);
            if !leader {
                if was_leader {
                    info!("Lost Control-lane leadership; the app reconciler stands by");
                    crate::manager::telemetry::metrics().apps.set_all(&[]);
                }
                was_leader = false;
                pending = Pending::default();
                while rx.try_recv().is_ok() {}
                events = events.resubscribe();
                tokio::time::sleep(LEADER_POLL).await;
                continue;
            }
            if !was_leader {
                was_leader = true;
                // A new term starts from nothing: failures, backoffs and
                // parked refusals from an earlier term of this manager's are
                // stale (another leader may have fixed what they were about).
                reconciler.reset_runtime();
                first_pass = true;
                pending.full = true;
            }
            if last_full.is_none_or(|at| at.elapsed() >= FULL_PASS_INTERVAL) {
                pending.full = true;
            }
            if let Some((at, name)) = reconciler.next_retry() {
                if at <= Instant::now() {
                    pending.apps.insert(name);
                }
            }
            let work = pending.take();
            if work.full {
                last_full = Some(Instant::now());
                // Every full pass, not only at acquisition: a bundled app
                // whose record was removed (a corrupt one, uninstalled) is
                // seeded again from this build's manifest.
                let seeded = if first_pass {
                    reconciler.seed()
                } else {
                    reconciler.seed_quietly()
                };
                first_pass = false;
                match seeded {
                    Ok(seeded) if !seeded.is_empty() => {
                        info!(apps = ?seeded, "Seeded bundled apps")
                    }
                    Ok(_) => {}
                    Err(err) => warn!(error = ?err, "Failed seeding bundled apps"),
                }
                reconciler.reconcile_all().await;
                reconciler.publish_metrics();
                // Object kinds follow the app records (a bundled app's new
                // manifest, a write a crash interrupted); app-object-kinds
                // plan §6.2.
                if let Err(status) = svc.sync_all_object_kinds() {
                    warn!("Syncing object kinds failed: {}", status.message());
                }
                // The scaling state of the autoscaler's deleted objects
                // (their `object:<uid>` action rows, pauses, and history),
                // which the generic object delete leaves (app-object-kinds
                // plan §8 O2).
                match svc.store.sweep_object_scaling_state() {
                    Ok(0) => {}
                    Ok(swept) => info!(swept, "Swept the scaling state of deleted objects"),
                    Err(err) => {
                        warn!(error = ?err, "Sweeping deleted objects' scaling state failed")
                    }
                }
            } else if !work.apps.is_empty() {
                for name in &work.apps {
                    reconciler.reconcile_app(name).await;
                    if let Err(status) = svc.sync_object_kinds_for(name) {
                        warn!(app = %name, "Syncing object kinds failed: {}", status.message());
                    }
                }
                reconciler.publish_metrics();
            }
            // Bounded object cleanups that ran out (app-object-kinds plan
            // §6.9): at most every few seconds, and nothing is proposed
            // unless one is due.
            if last_cleanup_sweep.is_none_or(|at| at.elapsed() >= OBJECT_CLEANUP_SWEEP) {
                last_cleanup_sweep = Some(Instant::now());
                if let Err(status) = svc.expire_object_cleanups() {
                    warn!("Expiring object cleanups failed: {}", status.message());
                }
            }

            // Wait for the next trigger, the next retry, the backstop, or
            // the next leadership check, whichever is first.
            let mut wake = LEADER_POLL.max(Duration::from_millis(1));
            if let Some((at, _)) = reconciler.next_retry() {
                wake = wake.min(at.saturating_duration_since(Instant::now()));
            }
            tokio::select! {
                message = rx.recv() => match message {
                    Some(Some(app)) => { pending.apps.insert(app); }
                    Some(None) => pending.full = true,
                    None => return,
                },
                event = events.recv() => match event {
                    Ok(event) => pending.note_event(&event),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => pending.full = true,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                },
                _ = tokio::time::sleep(wake) => {}
            }
            // Everything else already queued joins this one pass.
            while let Ok(message) = rx.try_recv() {
                match message {
                    Some(app) => {
                        pending.apps.insert(app);
                    }
                    None => pending.full = true,
                }
            }
            loop {
                match events.try_recv() {
                    Ok(event) => pending.note_event(&event),
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                        pending.full = true
                    }
                    Err(_) => break,
                }
            }
        }
    });
    nudge
}

/// The status a record's settings render placement into, for `legion app
/// list`.
pub fn placement_label(settings: &AppSettings) -> String {
    match (&settings.placement, &settings.pool) {
        (Placement::Managers, _) | (_, None) => settings.placement.as_str().to_string(),
        (placement, Some(pool)) => format!("{} ({pool})", placement.as_str()),
    }
}

/// Submit `rendered` for `record` exactly as the reconciler does, with
/// idempotency key `key`: `(message, deduplicated)`. For tests that play a
/// leader which dies after its submit lands.
pub fn submit_rendered(
    svc: &ManagerDaemonService,
    record: &AppRecord,
    rendered: &RenderedApp,
    key: &str,
) -> R<(String, bool)> {
    crate::manager::server::run_blocking(|| svc.submit_app_job(record, rendered, key, false))
        .map(|submit| (submit.message, submit.deduplicated))
        .map_err(|status| anyhow!("{}", status.message()))
}

/// The rendered job of `record` as the reconciler would submit it now
/// (`marker` for bundled apps), for tests and `legion app status`.
pub fn render_for(record: &AppRecord, marker: Option<&BuildMarker>) -> Result<RenderedApp, String> {
    render_app_job(record, RenderContext { marker })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reconciler_over_a_store() -> R<(AppReconciler, crate::storage::store::Store)> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?.keep();
        let store = crate::storage::store::Store::start_storage(
            &crate::structs::config::ManagerConfigFull {
                store_path: dir.to_str().context("utf-8")?.to_string(),
                ..Default::default()
            },
        )?;
        let svc = Arc::new(ManagerDaemonService::new(store.clone()));
        let reconciler = AppReconciler::new(
            svc,
            ReconcilerSettings {
                seed: BTreeMap::new(),
                own_build: BuildMarker {
                    version: "0.1.0".into(),
                    build_id: "a".into(),
                },
            },
        );
        Ok((reconciler, store))
    }

    /// A transient failure backs off (5s, then 10s), skips passes until the
    /// retry is due unless the record changes, marks the app degraded, and
    /// announces itself once per 10 minutes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_submit_backs_off() -> R<()> {
        let (mut reconciler, store) = reconciler_over_a_store()?;
        let manifest = crate::apps::bundled_manifest("autoscaler").context("bundled")?;
        let record = seed_record(manifest, None);
        store.seed_app(record.clone())?;
        let unavailable = tonic::Status::unavailable("the store is busy");

        let started = Instant::now();
        let outcome = reconciler.failed(
            "autoscaler",
            &record,
            &record.state,
            record.state.clone(),
            &unavailable,
            "d",
        );
        assert_eq!(outcome, PassOutcome::Failed("the store is busy".into()));
        let retry = reconciler.next_retry().context("a retry")?.0;
        assert!(retry >= started + FIRST_BACKOFF && retry < started + 2 * FIRST_BACKOFF);
        let stored = store.get_app("autoscaler")?.context("app")?;
        assert_eq!(
            stored.state.last_error,
            "reconcile failing: the store is busy"
        );

        reconciler.failed(
            "autoscaler",
            &stored,
            &stored.state,
            stored.state.clone(),
            &unavailable,
            "d",
        );
        let retry = reconciler.next_retry().context("a retry")?.0;
        assert!(retry >= started + 2 * FIRST_BACKOFF, "the backoff doubled");
        let announced =
            store.list_scheduler_events(&crate::storage::events::SchedulerEventFilter {
                kind: Some("app.reconcile_failed".into()),
                ..Default::default()
            })?;
        assert_eq!(announced.len(), 1, "announced once per 10 minutes");

        // In backoff, with the record unchanged: skipped.
        assert_eq!(
            reconciler.reconcile_app("autoscaler").await,
            PassOutcome::Skipped
        );
        // A record write is a new input: tried now (and, with no node to
        // run on, it waits for a build marker).
        let mut changed = stored.clone();
        changed.updated_at = "later".into();
        store.update_app(changed, &stored.updated_at)?;
        assert!(matches!(
            reconciler.reconcile_app("autoscaler").await,
            PassOutcome::Waiting(_)
        ));
        Ok(())
    }

    /// Status reflects the version the job runs: only the latest version's
    /// healthy allocations count as running; an upgrade the old version
    /// still serves is `upgrading`, a failed rollout is `upgrade failing`,
    /// and a failed job is degraded, not pending (hardening after the
    /// adversarial review, M1 and M2).
    #[test]
    fn status_reflects_the_version_that_runs() {
        let manifest = crate::apps::bundled_manifest("autoscaler").expect("bundled");
        let record = seed_record(manifest, None);
        let version = |status: &str| AppJobVersion {
            status: status.into(),
            version: 2,
            job_config_id: "jc-2".into(),
            managed_by: None,
            submitted_by: None,
        };
        let job = |status: &str, running: u32, previous: u32, rollout: Option<&str>| AppJobState {
            version: Some(version(status)),
            allocations: AppJobAllocations {
                running,
                previous,
                live: running + previous,
                nodes: Vec::new(),
            },
            rollout: rollout.map(|status| (status.to_string(), "health deadline".to_string())),
        };
        let running = derive_status(&record, &job("active", 1, 0, Some("completed")), None);
        assert_eq!(running.status, "running");

        // The new version is not healthy yet; the old one serves.
        let upgrading = derive_status(&record, &job("active", 0, 1, Some("running")), None);
        assert_eq!(upgrading.status, "upgrading", "{upgrading:?}");
        assert!(upgrading.detail.contains("previous version still serves 1"));

        // The rollout rolled back: the old version runs, the record says new.
        let failing = derive_status(&record, &job("active", 0, 1, Some("failed")), None);
        assert_eq!(failing.status, "degraded");
        assert!(
            failing
                .detail
                .starts_with("upgrade failing: the rollout to job version 2 failed"),
            "{}",
            failing.detail
        );
        assert!(failing.detail.contains("legion app enable autoscaler"));

        // A destroy-first rollout whose new version crash-loops.
        let crashing = derive_status(&record, &job("active", 0, 0, Some("failed")), None);
        assert!(
            crashing.detail.starts_with("upgrade failing"),
            "{}",
            crashing.detail
        );

        // A failed job is degraded and says it is restarted.
        let failed = derive_status(&record, &job("failed", 0, 0, None), None);
        assert_eq!(failed.status, "degraded");
        assert!(failed.detail.contains("job failed"), "{}", failed.detail);

        // A held bundled upgrade is degraded with its reason.
        let mut held = record.clone();
        held.state.marker_reason = format!(
            "{}build x changes the schema",
            crate::structs::app::UPGRADE_HELD
        );
        let view = derive_status(&held, &job("active", 1, 0, None), None);
        assert_eq!(view.status, "degraded");
        assert!(view.detail.starts_with("upgrade held: "));
    }

    #[test]
    fn backoff_doubles_from_five_seconds_to_five_minutes() {
        assert_eq!(backoff_for(1), Duration::from_secs(5));
        assert_eq!(backoff_for(2), Duration::from_secs(10));
        assert_eq!(backoff_for(3), Duration::from_secs(20));
        assert_eq!(backoff_for(7), Duration::from_secs(300));
        assert_eq!(backoff_for(40), Duration::from_secs(300));
    }

    #[test]
    fn refusals_are_the_errors_a_retry_cannot_fix() {
        assert!(is_refusal(&tonic::Status::invalid_argument("bad")));
        assert!(is_refusal(&tonic::Status::permission_denied("no")));
        assert!(is_refusal(&tonic::Status::resource_exhausted("quota")));
        assert!(!is_refusal(&tonic::Status::unavailable("down")));
        assert!(!is_refusal(&tonic::Status::internal("bug")));
        assert!(!is_refusal(&tonic::Status::failed_precondition(
            "Error running job: this manager is not the raft leader"
        )));
        assert!(is_refusal(&tonic::Status::failed_precondition("quota")));
    }

    #[test]
    fn job_and_node_events_queue_passes_and_app_events_do_not() {
        let event = |kind: &str, job: Option<&str>| crate::storage::events::SchedulerEventRecord {
            id: 1,
            kind: kind.into(),
            level: "info".into(),
            job_id: job.map(str::to_string),
            node_id: None,
            eval_id: None,
            allocation_id: None,
            rollout_id: None,
            message: String::new(),
            metadata_json: "{}".into(),
            created_at: String::new(),
        };
        let mut pending = Pending::default();
        pending.note_event(&event("app.reconciled", Some("legion-system/autoscaler")));
        pending.note_event(&event("job.updated", Some("web")));
        assert!(pending.apps.is_empty());
        pending.note_event(&event("job.updated", Some("legion-system/gitops")));
        pending.note_event(&event("job.stopped", Some("legion-system/gitops")));
        assert_eq!(pending.apps.len(), 1);
        pending.note_event(&event("node.registered", None));
        assert!(pending.apps.contains("autoscaler"));
    }
}
