//! Jobspec extensions on the manager (apps plan §6.6; docs/apps.md,
//! "Jobspec extensions"): the registry of installed keys a submit resolves
//! against, the checks a submit runs beyond the schema (`requires` as the
//! submitter, the qualified-spelling error after a core field shadows a
//! key, and the app's own validate call), the read-side view of a job's
//! values, and the two RPCs: `GetJobSchema` (the cluster's jobspec schema)
//! and `WatchAppExtensions` (an app's own values, app-plane).

use super::*;
use crate::storage::store::ExtensionJob;
use crate::structs::app::{app_name_of_job, AppRecord};
use crate::structs::app_ui::{inputs_digest, AssertionKind, AssertionScope, ViewerAssertion};
use crate::structs::extensions::{
    job_extension_entries, point_plural, shard_of, validate_context, written_on, ExtensionEntry,
    ExtensionPath as ValuePath, ExtensionRegistry, ExtensionState, ExtensionValue, InstalledKey,
    ValidateAnswer, ValidateCall, ValidateCallValue, APPS_KEY, VALIDATE_PATH, VALIDATE_TIMEOUT,
};
use crate::structs::spec_fields::ExtensionPoint;
use crate::structs::{
    AppExtensionEvent, AppExtensionKey, AppExtensionValue, AppExtensionValueRef,
    ExtensionPath as PathProto, GetJobSchemaReply, GetJobSchemaRequest, JobExtensionValue,
    WatchAppExtensionsRequest,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio_stream::wrappers::ReceiverStream;

/// How often `WatchAppExtensions` checks for changes.
const WATCH_POLL: Duration = Duration::from_secs(1);
/// How often it re-reads every value even when the fingerprints say
/// nothing changed (a job's status, say, moves without a new version).
const WATCH_FULL_READ: Duration = Duration::from_secs(30);

/// The registry cache: the apps' fingerprint it was built for, and the
/// registry.
pub(crate) type RegistryCache = Arc<Mutex<Option<(String, Arc<ExtensionRegistry>)>>>;

fn path_proto(path: &ValuePath) -> PathProto {
    PathProto {
        group: path.group.clone(),
        task: path.task.clone(),
        volume: path.volume.clone(),
        port: path.port.clone(),
        service: path.service.clone(),
    }
}

fn json_text(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// The proto of an installed key.
pub(crate) fn key_proto(installed: &InstalledKey) -> AppExtensionKey {
    AppExtensionKey {
        app: installed.app.clone(),
        app_version: installed.app_version.clone(),
        manifest_digest: installed.digest.clone(),
        app_enabled: installed.enabled,
        point: installed.decl.point.as_str().to_string(),
        key: installed.key.clone(),
        declared_key: installed.decl.declared_key(&installed.app).to_string(),
        description: installed.decl.description.clone(),
        schema_json: json_text(&installed.decl.schema),
        examples_json: json_text(&Value::Array(installed.decl.examples.clone())),
        validate: installed.decl.validate,
        requires: installed
            .decl
            .requires
            .as_ref()
            .map(|requires| requires.label())
            .unwrap_or_default(),
        shadowed: installed.shadowed,
    }
}

/// The proto of one of a job's values, as it reads now.
pub(crate) fn entry_proto(entry: &ExtensionEntry) -> JobExtensionValue {
    JobExtensionValue {
        app: entry.app.clone(),
        point: entry.point.as_str().to_string(),
        path: Some(path_proto(&entry.path)),
        key: entry.key.clone(),
        value_json: json_text(&entry.value),
        effective_json: json_text(&entry.effective),
        state: entry.state.as_str().to_string(),
        detail: entry.detail.clone(),
        validated_by: entry.validated_by.clone(),
        adopted: entry.adopted,
    }
}

/// One job's entries owned by `app` (stored, or pending and adopted).
fn owned_by<'a>(entries: &'a [ExtensionEntry], app: &str) -> Vec<&'a ExtensionEntry> {
    entries
        .iter()
        .filter(|entry| entry.app == app && entry.state != ExtensionState::Pending)
        .collect()
}

/// The identity of a value within a job, for the watch's diff.
fn watch_key(job_id: &str, point: ExtensionPoint, path: &ValuePath) -> String {
    format!(
        "{job_id}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        point.as_str(),
        path.group,
        path.task,
        path.volume,
        path.port,
        path.service
    )
}

/// What an install or upgrade would do to existing values, for the prompt
/// (apps plan §6.6, "Lifecycle").
pub(crate) fn lifecycle_report(
    app: &str,
    jobs: &[ExtensionJob],
    registry: &ExtensionRegistry,
    installing: bool,
) -> Vec<String> {
    let mut lines = Vec::new();
    let mut by_key: BTreeMap<(String, ExtensionPoint), (BTreeSet<String>, Vec<String>)> =
        BTreeMap::new();
    let mut adopted: Vec<String> = Vec::new();
    let mut held: Vec<String> = Vec::new();
    for job in jobs {
        let entries = job_extension_entries(&job.config, registry);
        for entry in &entries {
            if entry.app != app {
                continue;
            }
            if entry.state == ExtensionState::Pending {
                // Claimed, but its key needs a permission no submit checked.
                held.push(entry.label(&job.job_id));
                continue;
            }
            let installed_key = registry
                .for_value(app, entry.point)
                .map(|installed| installed.key.clone())
                .unwrap_or_else(|| entry.key.clone());
            let slot = by_key
                .entry((installed_key, entry.point))
                .or_insert_with(|| (BTreeSet::new(), Vec::new()));
            slot.0.insert(job.job_id.clone());
            if entry.state == ExtensionState::Invalid {
                slot.1
                    .push(format!("  {}: {}", entry.label(&job.job_id), entry.detail));
            }
            if entry.adopted {
                adopted.push(entry.label(&job.job_id));
            }
        }
    }
    for ((key, point), (jobs, failing)) in by_key {
        let count = jobs.len();
        let noun = if count == 1 { "job sets" } else { "jobs set" };
        if failing.is_empty() {
            lines.push(format!(
                "{count} {noun} {key} on {}; every value passes this version's schema.",
                point_plural(point)
            ));
        } else {
            lines.push(format!(
                "{count} {noun} {key} on {}; {} fail{} this version's schema (flagged invalid; \
                 each job's next submit must conform):",
                point_plural(point),
                failing.len(),
                if failing.len() == 1 { "s" } else { "" }
            ));
            lines.extend(failing);
        }
    }
    if installing && !adopted.is_empty() {
        lines.push(format!(
            "Adopts {} pending value{} kept under --allow-unknown-fields: {}",
            adopted.len(),
            if adopted.len() == 1 { "" } else { "s" },
            adopted.join(", ")
        ));
    }
    if installing && !held.is_empty() {
        lines.push(format!(
            "Leaves pending (its key requires a permission no submit was checked for; resubmit \
             those jobs to adopt them): {}",
            held.join(", ")
        ));
    }
    lines
}

impl ManagerDaemonService {
    /// Treat `name` as a core field at `point` (tests: a simulated Legion
    /// release that adds a core field named like an installed key).
    pub fn with_simulated_core_field(mut self, point: ExtensionPoint, name: &str) -> Self {
        self.simulated_core_fields.push((point, name.to_string()));
        self
    }

    /// The installed keys, rebuilt only when an app record changed. Logs
    /// each key a core field shadows when it is (re)built, which happens at
    /// the manager's start.
    pub(crate) fn extension_registry(&self) -> Result<Arc<ExtensionRegistry>, Status> {
        cached_registry(
            &self.store,
            &self.extension_registry,
            &self.simulated_core_fields,
            true,
        )
    }

    /// One record's keys, as a registry of their own (`App.extension_keys`
    /// and the notes `legion app status` shows, also for a record not yet
    /// installed).
    pub(crate) fn record_registry(&self, record: &AppRecord) -> ExtensionRegistry {
        let (mut registry, _) = ExtensionRegistry::from_records(std::slice::from_ref(record));
        for (point, name) in &self.simulated_core_fields {
            registry = registry.with_simulated_core_field(*point, name);
        }
        registry
    }

    /// The jobs whose values (stored, or pending and adopted) `app`'s keys
    /// own, as `registry` reads them.
    pub(crate) fn jobs_using_app(
        &self,
        app: &str,
        registry: &ExtensionRegistry,
    ) -> Result<Vec<String>, Status> {
        if !registry.has_app(app) {
            return Ok(Vec::new());
        }
        Ok(self
            .extension_jobs()?
            .into_iter()
            .filter(|job| {
                job_extension_entries(&job.config, registry)
                    .iter()
                    .any(|entry| entry.app == app && entry.state != ExtensionState::Pending)
            })
            .map(|job| job.job_id)
            .collect())
    }

    /// A job's extension values as they read now (GetJobInfo).
    pub(crate) fn job_extension_values(&self, config: &JobConfig) -> Vec<JobExtensionValue> {
        if config.extensions.is_empty() && !has_pending(config) {
            return Vec::new();
        }
        let Ok(registry) = self.extension_registry() else {
            return Vec::new();
        };
        job_extension_entries(config, &registry)
            .iter()
            .map(entry_proto)
            .collect()
    }

    /// Every job whose latest version holds values or pending keys.
    pub(crate) fn extension_jobs(&self) -> Result<Vec<ExtensionJob>, Status> {
        self.store
            .extension_jobs()
            .map_err(|e| map_store_error("Error reading jobs' extension values", e))
    }

    /// `requires` (apps plan §6.6, "At submit", step 3): a value that is new
    /// or changed since the job's previous version needs the permission its
    /// key declares, held by the submitter on the job.
    pub(super) fn enforce_extension_requires(
        &self,
        identity: &AclIdentity,
        config: &JobConfig,
        previous: Option<&JobConfig>,
        registry: &ExtensionRegistry,
    ) -> Result<(), Status> {
        for value in &config.extensions {
            let Some(installed) = registry.for_value(&value.app, value.point) else {
                continue;
            };
            let Some(requires) = &installed.decl.requires else {
                continue;
            };
            let unchanged = previous.is_some_and(|previous| {
                previous
                    .extensions
                    .iter()
                    .any(|old| old.slot() == value.slot() && old.value == value.value)
            });
            if unchanged {
                continue;
            }
            let (resource, action) = requires.parsed().map_err(|err| {
                Status::failed_precondition(format!(
                    "the {} app's `requires` is invalid: {err}",
                    installed.app
                ))
            })?;
            let scope = if resource.is_namespaced() {
                AclScope::job(&config.name)
            } else {
                AclScope::none()
            };
            if acl::enforce(identity, resource, action, &scope).is_err() {
                return Err(Status::permission_denied(format!(
                    "{}: {}{} ({} app) requires {} to set, which your token does not hold{}\nHint: \
                     have someone who holds it submit the job, or leave the value as it was",
                    config.name,
                    value.path.scope(value.point),
                    value.key,
                    installed.app,
                    requires.label(),
                    if resource.is_namespaced() {
                        " on this job"
                    } else {
                        ""
                    }
                )));
            }
        }
        Ok(())
    }

    /// Collision rule 3 (apps plan §6.6): once a core field shadows an
    /// app's key, a submit is refused, with a pointer to the qualified
    /// spelling, only when the job's previous version stored that app's
    /// value under the short key and this submit writes the key again (now
    /// the core field). Every other submit takes the key as the core field.
    pub(super) fn check_shadowed_keys(
        &self,
        config: &JobConfig,
        previous: Option<&JobConfig>,
        jobspec: &str,
        registry: &ExtensionRegistry,
    ) -> Result<(), Status> {
        let Some(previous) = previous else {
            return Ok(());
        };
        let shadowed: Vec<&InstalledKey> = registry
            .keys()
            .iter()
            .filter(|installed| installed.shadowed)
            .collect();
        if shadowed.is_empty() {
            return Ok(());
        }
        let raw: Value = serde_json::from_str(jobspec).unwrap_or(Value::Null);
        let mut lines = Vec::new();
        for installed in shadowed {
            for old in previous
                .extensions
                .iter()
                .filter(|old| old.app == installed.app && old.point == installed.decl.point)
                .filter(|old| !old.key.starts_with(&format!("{APPS_KEY}.")))
            {
                let rewritten = config.extensions.iter().any(|new| new.slot() == old.slot());
                if rewritten || !raw_block_has_key(&raw, old, &installed.key) {
                    continue;
                }
                lines.push(format!(
                    "{}: {}`{}` is now a core {} field, so it no longer sets the {} app's value; \
                     write it as `{APPS_KEY}.{}` (the qualified spelling) to keep the app's \
                     setting, or rename the field to use the core one",
                    config.name,
                    old.path.scope(old.point),
                    installed.key,
                    written_on(installed.decl.point).as_str(),
                    installed.app,
                    installed.app
                ));
            }
        }
        if lines.is_empty() {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!(
                "{}\nHint: the job's previous version set the app's value under the short key",
                lines.join("\n")
            )))
        }
    }

    /// Ask each app whose keys declare `validate = true` about this
    /// submit's values for them (apps plan §6.6, "At submit", step 4):
    /// `POST /legion/v1/extensions/validate`, through the same worker-plane
    /// forward and viewer assertion as app views, with the submitter as the
    /// viewer and a 2-second budget per app. Errors refuse the submit;
    /// warnings come back for the reply. An app that cannot be reached, or
    /// does not answer in time, passes the values with a warning: a broken
    /// add-on must not block deploys.
    ///
    /// Runs before the submit itself, on the async side, and only when an
    /// installed key asks for it. A jobspec that does not parse, or that the
    /// caller may not submit, asks nobody: the submit reports why.
    pub(super) async fn ask_apps_to_validate(
        &self,
        identity: &AclIdentity,
        jobspec: &str,
        namespace: Option<&str>,
        mode: UnknownFieldMode,
    ) -> Result<Vec<String>, Status> {
        let registry = self.extension_registry()?;
        if !registry.any_validate() {
            return Ok(Vec::new());
        }
        let parsed = run_blocking_admission(|| {
            resolve_jobspec_kv_references(&self.store, jobspec, mode, &registry)
        });
        let Ok((mut config, _, _)) = parsed else {
            return Ok(Vec::new());
        };
        let Ok(namespace) = self.admit_job_namespace(&mut config, jobspec, namespace) else {
            return Ok(Vec::new());
        };
        // Nobody's app hears about a job the caller could not submit.
        if !identity.allows(
            AclResource::Job,
            AclAction::Write,
            &AclScope::job(&config.name),
        ) {
            return Ok(Vec::new());
        }
        let mut by_app: BTreeMap<String, Vec<(&ExtensionValue, &InstalledKey)>> = BTreeMap::new();
        for value in &config.extensions {
            if let Some(installed) = registry.for_value(&value.app, value.point) {
                if installed.decl.validate {
                    by_app
                        .entry(value.app.clone())
                        .or_default()
                        .push((value, installed));
                }
            }
        }
        if by_app.is_empty() {
            return Ok(Vec::new());
        }
        let calls = by_app.into_iter().map(|(app, values)| {
            let config = &config;
            let namespace = namespace.clone();
            async move {
                let outcome = tokio::time::timeout(
                    VALIDATE_TIMEOUT,
                    self.validate_call(identity, &app, config, &namespace, &values),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(format!(
                        "did not answer within {}s",
                        VALIDATE_TIMEOUT.as_secs()
                    ))
                });
                (app, values, outcome)
            }
        });
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        for (app, values, outcome) in futures::future::join_all(calls).await {
            let place = |value: &ExtensionValue| {
                format!(
                    "{}: {}{}",
                    config.name,
                    value.path.scope(value.point),
                    value.key
                )
            };
            match outcome {
                Ok(answer) => {
                    for (index, (value, _)) in values.iter().enumerate() {
                        let result = answer.results.get(index).cloned().unwrap_or_default();
                        for error in result.errors {
                            errors.push(format!("{} ({app} app): {error}", place(value)));
                        }
                        for warning in result.warnings {
                            warnings.push(format!("{} ({app} app): {warning}", place(value)));
                        }
                    }
                }
                Err(why) => {
                    info!(app = %app, job = %config.name, "Extension values not validated by the app: {why}");
                    for (value, _) in &values {
                        warnings.push(format!(
                            "{} was not checked by the {app} app ({why}); the schema check passed",
                            place(value)
                        ));
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(warnings)
        } else {
            Err(Status::invalid_argument(format!(
                "{}\nHint: the app refused these values; fix them and submit again",
                errors.join("\n")
            )))
        }
    }

    /// One app's validate call; `Err` says why it could not answer (which
    /// passes the values with a warning).
    async fn validate_call(
        &self,
        identity: &AclIdentity,
        app: &str,
        config: &JobConfig,
        namespace: &str,
        values: &[(&ExtensionValue, &InstalledKey)],
    ) -> Result<ValidateAnswer, String> {
        let record: AppRecord = self
            .store
            .get_app(app)
            .ok()
            .flatten()
            .ok_or_else(|| "not installed".to_string())?;
        if !record.enabled {
            return Err("disabled".into());
        }
        let ui = record
            .manifest
            .ui_spec()
            .map_err(|err| format!("its manifest's ui block is invalid: {err}"))?;
        let call = ValidateCall {
            job: config.name.clone(),
            namespace: namespace.to_string(),
            values: values
                .iter()
                .map(|(value, installed)| ValidateCallValue {
                    point: value.point,
                    path: value.path.clone(),
                    key: value.key.clone(),
                    value: value.value.clone(),
                    effective: installed
                        .check(&value.value)
                        .unwrap_or_else(|_| value.value.clone()),
                })
                .collect(),
        };
        let body = serde_json::to_value(&call).map_err(|err| err.to_string())?;
        let assertion = ViewerAssertion {
            kind: AssertionKind::Validate,
            app: app.to_string(),
            name: "validate".to_string(),
            principal: identity.principal.clone(),
            accessor: identity.accessor_id.clone(),
            permission: None,
            scope: AssertionScope::None,
            namespaces: Vec::new(),
            context: validate_context(&config.name),
            inputs: Some(inputs_digest(&body)),
        };
        let token = self
            .sign_viewer_assertion(identity, &assertion)
            .map_err(|status| status.message().to_string())?;
        let bytes = serde_json::to_vec(&body).map_err(|err| err.to_string())?;
        let (reply, _) = self
            .forward_to_app(
                &record,
                &ui,
                "POST",
                VALIDATE_PATH.to_string(),
                token,
                bytes,
                "application/json",
            )
            .await
            .map_err(|status| status.message().to_string())?;
        if reply.status != 200 {
            return Err(format!(
                "it answered HTTP {}: {}",
                reply.status,
                String::from_utf8_lossy(&reply.body)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ));
        }
        serde_json::from_slice(&reply.body)
            .map_err(|err| format!("its answer is not a validate reply: {err}"))
    }

    // ACL: job:read, any scope (the schema describes jobspecs, not jobs).
    pub(super) async fn get_job_schema_rpc(
        &self,
        request: Request<GetJobSchemaRequest>,
    ) -> Result<Response<GetJobSchemaReply>, Status> {
        self.check_acl(&request, AclResource::Job, AclAction::Read, AclScope::any())?;
        let registry = self.extension_registry()?;
        let mut schema = crate::structs::job_schema::core_jobspec_schema();
        crate::structs::extensions::add_keys_to_schema(&mut schema, &registry);
        schema["$comment"] = json!(format!(
            "This cluster's jobspec schema: the core fields, and {} key(s) installed apps add.",
            registry.keys().len()
        ));
        Ok(Response::new(GetJobSchemaReply {
            schema_json: json_text(&schema),
            extensions: registry.keys().iter().map(key_proto).collect(),
        }))
    }

    /// The app a caller is: its bearer is this cluster's `legion` token for
    /// a job in `legion-system`, or it carries a grant reference to an
    /// allocation of an app's job (an ACL token minted from the identity).
    fn calling_app<T>(&self, request: &Request<T>, identity: &AclIdentity) -> Option<String> {
        if let (Some(state), Some(bearer)) = (
            self.workload_identity.as_ref(),
            acl::extract_bearer(request.metadata()),
        ) {
            if let Ok(claims) = state.verify_control_plane(&bearer) {
                let namespace = claims.get("legion.io/namespace").and_then(Value::as_str);
                let job = claims.get("legion.io/job").and_then(Value::as_str);
                if let (Some(namespace), Some(job)) = (namespace, job) {
                    if namespace == crate::structs::namespace::SYSTEM_NAMESPACE {
                        return Some(job.to_string());
                    }
                }
            }
        }
        identity.policies.iter().find_map(|policy| {
            let allocation = crate::structs::grant::parse_grant_policy_ref(policy)?;
            let job = self.store.allocation_job_id(allocation).ok().flatten()?;
            app_name_of_job(&job).map(str::to_string)
        })
    }

    // ACL: app-plane (an app's own workload identity, or management naming
    // the app).
    pub(super) async fn watch_app_extensions_rpc(
        &self,
        request: Request<WatchAppExtensionsRequest>,
    ) -> Result<Response<ReceiverStream<Result<AppExtensionEvent, Status>>>, Status> {
        let identity = self.authenticate_acl(&request)?;
        let named = request.get_ref().app.trim().to_string();
        let caller = self.calling_app(&request, &identity);
        let app = match (caller, identity.is_management) {
            (Some(caller), _) if named.is_empty() || named == caller => caller,
            (Some(caller), false) => {
                return Err(Status::permission_denied(format!(
                    "the {caller} app may watch only its own extension values, not {named}'s"
                )))
            }
            (_, true) if !named.is_empty() => named,
            (_, true) => {
                return Err(Status::invalid_argument(
                    "a management token names the app to watch (`app`)",
                ))
            }
            (None, false) if identity.audit_only => {
                // Audit-only ACLs log the denial and let the call through,
                // as for every other endpoint; the caller names the app.
                warn!(
                    target: "acl_audit",
                    principal = %identity.principal,
                    resource = "app-plane",
                    action = "watch_app_extensions",
                    enforced = false,
                    "ACL denial"
                );
                if named.is_empty() {
                    return Err(Status::invalid_argument(
                        "a caller that is not an app names the app to watch (`app`)",
                    ));
                }
                named
            }
            (None, false) => {
                warn!(
                    target: "acl_audit",
                    principal = %identity.principal,
                    accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
                    source = identity.source.as_str(),
                    resource = "app-plane",
                    action = "watch_app_extensions",
                    enforced = !identity.audit_only,
                    "ACL denial"
                );
                if identity.source == crate::structs::acl::AclSource::Anonymous {
                    return Err(Status::unauthenticated(
                        "WatchAppExtensions is called by an app with its own workload identity",
                    ));
                }
                return Err(Status::permission_denied(
                    "WatchAppExtensions is called by an app with its own workload identity (its \
                     `legion` token); it returns that app's own jobspec extension values",
                ));
            }
        };
        if self
            .store
            .get_app(&app)
            .map_err(|e| map_store_error("Error loading the app", e))?
            .is_none()
        {
            return Err(Status::not_found(format!(
                "no app named {app:?} is installed"
            )));
        }
        let inner = request.get_ref();
        let shard_count = inner.shard_count.max(1);
        let shards: BTreeSet<u32> = inner.shards.iter().copied().collect();
        if let Some(bad) = shards.iter().find(|shard| **shard >= shard_count) {
            return Err(Status::invalid_argument(format!(
                "shard {bad} is not below shard_count {shard_count}"
            )));
        }
        let credential = self.stream_credential(&request);
        let svc = self.clone_for_watch();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut reauth = tokio::time::interval(credential.interval);
            reauth.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            reauth.tick().await;
            let mut poll = tokio::time::interval(WATCH_POLL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut fingerprint: Option<String> = None;
            let mut last_full = std::time::Instant::now();
            let mut sent: Option<BTreeMap<String, AppExtensionValue>> = None;
            let mut generation = 0u64;
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    _ = reauth.tick() => {
                        if let Err(status) = credential.reauthenticate(&svc.store) {
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                        continue;
                    }
                    _ = poll.tick() => {}
                }
                let probe = match (
                    svc.store.app_fingerprint(),
                    svc.store.job_versions_fingerprint(),
                ) {
                    (Ok(apps), Ok(jobs)) => format!("{apps}|{jobs}"),
                    (Err(err), _) | (_, Err(err)) => {
                        debug!("WatchAppExtensions: reading fingerprints: {err:#}");
                        continue;
                    }
                };
                let stale = last_full.elapsed() >= WATCH_FULL_READ;
                if sent.is_some() && fingerprint.as_deref() == Some(probe.as_str()) && !stale {
                    continue;
                }
                let current = match svc.app_values(&app, shard_count, &shards) {
                    Ok(current) => current,
                    Err(status) => {
                        warn!(app = %app, "WatchAppExtensions: {}", status.message());
                        continue;
                    }
                };
                fingerprint = Some(probe);
                last_full = std::time::Instant::now();
                let event = match &sent {
                    None => AppExtensionEvent {
                        snapshot: true,
                        upserts: current.values().cloned().collect(),
                        deletes: Vec::new(),
                        generation: 0,
                    },
                    Some(previous) => {
                        let upserts: Vec<AppExtensionValue> = current
                            .iter()
                            .filter(|(key, value)| previous.get(*key) != Some(*value))
                            .map(|(_, value)| value.clone())
                            .collect();
                        let deletes: Vec<AppExtensionValueRef> = previous
                            .iter()
                            .filter(|(key, _)| !current.contains_key(*key))
                            .map(|(_, value)| AppExtensionValueRef {
                                job_id: value.job_id.clone(),
                                point: value.point.clone(),
                                path: value.path.clone(),
                            })
                            .collect();
                        if upserts.is_empty() && deletes.is_empty() {
                            continue;
                        }
                        AppExtensionEvent {
                            snapshot: false,
                            upserts,
                            deletes,
                            generation: 0,
                        }
                    }
                };
                generation += 1;
                let event = AppExtensionEvent {
                    generation,
                    ..event
                };
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
                sent = Some(current);
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// The service handle a watch task keeps: a clone sharing the store and
    /// caches.
    fn clone_for_watch(&self) -> Arc<WatchSource> {
        Arc::new(WatchSource {
            store: self.store.clone(),
            extension_registry: self.extension_registry.clone(),
            simulated_core_fields: self.simulated_core_fields.clone(),
        })
    }
}

/// What a `WatchAppExtensions` task reads from: the store and the registry
/// cache, without holding the whole service.
pub(crate) struct WatchSource {
    store: Store,
    extension_registry: RegistryCache,
    simulated_core_fields: Vec<(ExtensionPoint, String)>,
}

impl WatchSource {
    fn registry(&self) -> Result<Arc<ExtensionRegistry>, Status> {
        cached_registry(
            &self.store,
            &self.extension_registry,
            &self.simulated_core_fields,
            false,
        )
    }

    /// `app`'s values in every job, keyed for the diff, in the watched
    /// shards.
    fn app_values(
        &self,
        app: &str,
        shard_count: u32,
        shards: &BTreeSet<u32>,
    ) -> Result<BTreeMap<String, AppExtensionValue>, Status> {
        let registry = self.registry()?;
        let jobs = self
            .store
            .extension_jobs()
            .map_err(|e| map_store_error("Error reading jobs' extension values", e))?;
        let mut values = BTreeMap::new();
        for job in jobs {
            let shard = if shard_count > 1 {
                shard_of(&job.job_id, shard_count)
            } else {
                0
            };
            if !shards.is_empty() && !shards.contains(&shard) {
                continue;
            }
            let entries = job_extension_entries(&job.config, &registry);
            for entry in owned_by(&entries, app) {
                let (namespace, _) = crate::structs::namespace::split_job_id(&job.job_id);
                values.insert(
                    watch_key(&job.job_id, entry.point, &entry.path),
                    AppExtensionValue {
                        job_id: job.job_id.clone(),
                        namespace: namespace.to_string(),
                        config_version: job.config.version.unwrap_or_default(),
                        job_status: job.status.clone(),
                        point: entry.point.as_str().to_string(),
                        path: Some(path_proto(&entry.path)),
                        key: entry.key.clone(),
                        value_json: json_text(&entry.value),
                        effective_json: json_text(&entry.effective),
                        state: entry.state.as_str().to_string(),
                        detail: entry.detail.clone(),
                        validated_by: entry.validated_by.clone(),
                        adopted: entry.adopted,
                        shard,
                    },
                );
            }
        }
        Ok(values)
    }
}

/// The installed keys from `cache`, rebuilt when the apps' fingerprint
/// moved. `log` warns about problems and shadowed keys on a rebuild.
fn cached_registry(
    store: &Store,
    cache: &RegistryCache,
    simulated: &[(ExtensionPoint, String)],
    log: bool,
) -> Result<Arc<ExtensionRegistry>, Status> {
    let fingerprint = store
        .app_fingerprint()
        .map_err(|e| map_store_error("Error reading installed apps", e))?;
    if let Some((cached, registry)) = cache
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
    {
        if *cached == fingerprint {
            return Ok(registry.clone());
        }
    }
    let records = store
        .list_apps()
        .map_err(|e| map_store_error("Error reading installed apps", e))?;
    let (mut registry, problems) = ExtensionRegistry::from_records(&records);
    for (point, name) in simulated {
        registry = registry.with_simulated_core_field(*point, name);
    }
    if log {
        for problem in problems {
            warn!("Ignoring an app's jobspec keys: {problem}");
        }
        for note in registry.shadowed_notes(None) {
            warn!("Jobspec key shadowed by a core field: {note}");
        }
    }
    let registry = Arc::new(registry);
    *cache.lock().unwrap_or_else(|poison| poison.into_inner()) =
        Some((fingerprint, registry.clone()));
    Ok(registry)
}

/// Whether any block of `config` keeps pending keys.
fn has_pending(config: &JobConfig) -> bool {
    !config.pending_extensions.is_empty()
        || config.tasks.iter().any(|task| {
            !task.pending_extensions.is_empty()
                || task
                    .volumes
                    .iter()
                    .any(|volume| !volume.pending_extensions.is_empty())
                || task.network.as_ref().is_some_and(|network| {
                    network
                        .ports
                        .iter()
                        .any(|port| !port.pending_extensions.is_empty())
                })
                || task
                    .service
                    .as_ref()
                    .is_some_and(|service| !service.pending_extensions.is_empty())
        })
}

/// Whether the raw submitted jobspec writes `key` on the block where
/// `stored` sits (its task, volume, port, or service; the top level for the
/// job point).
fn raw_block_has_key(raw: &Value, stored: &ExtensionValue, key: &str) -> bool {
    let block = match stored.point {
        ExtensionPoint::Job => Some(raw),
        _ => raw_task(raw, &stored.path.task).and_then(|task| match stored.point {
            ExtensionPoint::Task | ExtensionPoint::Group => Some(task),
            ExtensionPoint::Volume => task
                .get("volumes")
                .and_then(Value::as_array)
                .and_then(|volumes| named(volumes, &stored.path.volume)),
            ExtensionPoint::Port => task
                .get("network")
                .and_then(|network| network.get("ports"))
                .and_then(Value::as_array)
                .and_then(|ports| named(ports, &stored.path.port)),
            ExtensionPoint::Service => task.get("service"),
            ExtensionPoint::Job => None,
        }),
    };
    block.is_some_and(|block| block.get(key).is_some())
}

fn named<'a>(items: &'a [Value], name: &str) -> Option<&'a Value> {
    items
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(name))
}

/// The raw task named `name`: the `task` shorthand, a `tasks` list entry,
/// or a `tasks` map entry.
fn raw_task<'a>(raw: &'a Value, name: &str) -> Option<&'a Value> {
    if let Some(task) = raw.get("task") {
        let task_name = task.get("name").and_then(Value::as_str).unwrap_or("task");
        if task_name == name {
            return Some(task);
        }
    }
    match raw.get("tasks") {
        Some(Value::Array(tasks)) => tasks.iter().enumerate().find_map(|(index, task)| {
            let fallback = format!("task-{}", index + 1);
            let task_name = task
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(&fallback);
            (task_name == name).then_some(task)
        }),
        Some(Value::Object(tasks)) => tasks.iter().find_map(|(key, task)| {
            let task_name = task.get("name").and_then(Value::as_str).unwrap_or(key);
            (task_name == name).then_some(task)
        }),
        _ => None,
    }
}

#[cfg(test)]
mod extension_rpcs_tests {
    use super::*;

    #[test]
    fn raw_blocks_are_found_by_name_test() {
        let raw = json!({
            "name": "orders-db",
            "uptime": { "url": "x" },
            "tasks": [{
                "name": "postgres",
                "volumes": [{ "name": "data", "backup": { "schedule": "@daily" } }],
                "network": { "ports": [{ "name": "http", "dns": {} }] },
                "service": { "port": "http", "probe": {} },
            }],
        });
        let at = |point, path: ValuePath| ExtensionValue {
            app: "a".into(),
            point,
            path,
            key: "k".into(),
            value: json!({}),
            validated_by: String::new(),
        };
        let task = ValuePath {
            group: "default".into(),
            task: "postgres".into(),
            ..Default::default()
        };
        assert!(raw_block_has_key(
            &raw,
            &at(ExtensionPoint::Job, ValuePath::default()),
            "uptime"
        ));
        assert!(raw_block_has_key(
            &raw,
            &at(
                ExtensionPoint::Volume,
                ValuePath {
                    volume: "data".into(),
                    ..task.clone()
                }
            ),
            "backup"
        ));
        assert!(raw_block_has_key(
            &raw,
            &at(
                ExtensionPoint::Port,
                ValuePath {
                    port: "http".into(),
                    ..task.clone()
                }
            ),
            "dns"
        ));
        assert!(raw_block_has_key(
            &raw,
            &at(ExtensionPoint::Service, task.clone()),
            "probe"
        ));
        assert!(!raw_block_has_key(
            &raw,
            &at(ExtensionPoint::Task, task),
            "backup"
        ));
    }
}
