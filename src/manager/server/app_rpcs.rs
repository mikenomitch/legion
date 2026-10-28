//! App RPCs (docs/apps.md; apps plan §6.3, "RPCs"), and the two writes the
//! app reconciler makes through this service: submitting an app's job and
//! stopping it.
//!
//! ACL resource `app` is cluster-scoped, with a `names` selector: `app:read`
//! lists and shows, `app:write` changes. Installing and upgrading also run
//! the job-grant check (docs/workload-identity.md, "Permissions") against
//! the caller for every permission the manifest asks for, and so does a
//! config change that touches one of the manifest's code fields: `app:write`
//! alone is never a path to an app's grant (apps plan calls entry 5).
//!
//! Every write nudges the reconciler, which renders the app's job from the
//! new record and submits it as `legion:app-reconciler`, checked against the
//! grant recorded here.

use super::*;
use crate::storage::app_store::AppJobVersion;
use crate::structs::acl::AclRule;
use crate::structs::app::{
    changed_code_fields, install_prompt, permission_diff, validate_app_config, AppGrant,
    AppManifest, AppReconcileState, AppRecord, AppSettings, AppSource, ManagedBy, Placement,
    RenderedApp, RECONCILER_PRINCIPAL,
};
use crate::structs::config::{MANAGER_ROLE_ATTR, MANAGER_ROLE_VALUE};
use crate::structs::namespace::SYSTEM_NAMESPACE;
use crate::structs::{
    App, AppGrantInfo, AppReconcileInfo, AppSettingsInfo, GetAppReply, GetAppRequest,
    InstallAppReply, InstallAppRequest, ListAppsReply, ListAppsRequest, PermissionDiff,
    SetAppConfigReply, SetAppConfigRequest, SetAppEnabledReply, SetAppEnabledRequest,
    UninstallAppReply, UninstallAppRequest, UpdateAppSettingsReply, UpdateAppSettingsRequest,
    UpgradeAppReply, UpgradeAppRequest,
};
use time::format_description::well_known::Rfc3339;

const GRANT_HINT: &str =
    "Hint: have someone who holds it do this, or ask for it to be granted to you";

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn rules_json(rules: &[AclRule]) -> String {
    serde_json::to_string(rules).unwrap_or_else(|_| "[]".to_string())
}

fn settings_info(settings: &AppSettings) -> AppSettingsInfo {
    AppSettingsInfo {
        replicas: settings.replicas,
        placement: settings.placement.as_str().to_string(),
        cpu: settings.resources.and_then(|resources| resources.cpu),
        memory: settings.resources.and_then(|resources| resources.memory),
        pool: settings.pool.clone(),
    }
}

/// What a submit by the app reconciler did.
#[derive(Debug, Clone)]
pub(crate) struct AppSubmit {
    pub message: String,
    pub deduplicated: bool,
}

/// The first permission in `rules` that `identity` does not hold, worded as
/// the refusal; `None` when it holds them all (or in audit-only mode, which
/// logs the miss).
pub(crate) fn app_grant_refusal(
    identity: &AclIdentity,
    who: &str,
    rules: &[AclRule],
) -> Option<String> {
    use crate::structs::grant::{first_uncovered, UncoveredReason};
    let miss = first_uncovered(identity, rules)?;
    let message = match miss.reason {
        UncoveredReason::NotHeld => {
            format!(
                "{who} asks for {}, which your token does not hold",
                miss.what
            )
        }
        UncoveredReason::Reserved => format!(
            "{who} asks for {}, which your token does not hold (`*` does not reach reserved \
             `legion-*` namespaces or `legion/` keys; only a rule that names them covers a \
             request for them)",
            miss.what
        ),
        UncoveredReason::DenyRules => format!(
            "{who} asks for {}; your token holds it, but it also carries deny rules on {}, so \
             granting it takes acl:write",
            miss.what, miss.resource
        ),
    };
    if identity.audit_only {
        warn!(
            target: "acl_audit",
            principal = %identity.principal,
            enforced = false,
            "app grant check would deny (audit-only): {message}"
        );
        return None;
    }
    Some(message)
}

/// The identity the reconciler submits an app's job as: the app's recorded
/// grant (allow rules), plus `job:write` (host-level) on exactly the app's
/// job. The submit's grant check therefore checks the render against the
/// recorded grant, not against any principal's own rights (apps plan §6.4).
fn reconciler_identity(record: &AppRecord) -> AclIdentity {
    let mut rules: Vec<AclRule> = record
        .grant
        .rules
        .iter()
        .filter(|rule| !rule.is_deny())
        .cloned()
        .collect();
    rules.push(AclRule {
        resources: vec!["job".to_string()],
        actions: vec!["read".to_string(), "write".to_string()],
        namespaces: Some(vec![SYSTEM_NAMESPACE.to_string()]),
        names: Some(vec![record.name.clone()]),
        host: Some(true),
        ..Default::default()
    });
    AclIdentity {
        principal: RECONCILER_PRINCIPAL.to_string(),
        accessor_id: None,
        is_management: false,
        is_worker: false,
        policies: Vec::new(),
        rules,
        source: crate::structs::acl::AclSource::Token,
        expires_at: None,
        audit_only: false,
    }
}

fn parse_manifest(manifest_json: &str) -> Result<AppManifest, Status> {
    let manifest = AppManifest::from_json(manifest_json).map_err(Status::invalid_argument)?;
    manifest.validate(false).map_err(Status::invalid_argument)?;
    Ok(manifest)
}

fn parse_config(config_json: &str) -> Result<Value, Status> {
    serde_json::from_str(config_json)
        .map_err(|err| Status::invalid_argument(format!("the config is not JSON: {err}")))
}

fn not_found(name: &str) -> Status {
    Status::not_found(format!(
        "no app named {name:?} is installed (`legion app list` shows the installed apps)"
    ))
}

impl ManagerDaemonService {
    /// Hand the service the nudge that wakes this manager's app reconciler.
    pub fn set_app_nudge(&self, nudge: crate::manager::apps::AppNudge) {
        let _ = self.app_nudge.set(nudge);
    }

    /// `systemWorker.appUser`, which app exec tasks run as.
    pub fn app_user(&self) -> &str {
        &self.app_user
    }

    fn nudge_app(&self, name: &str) {
        if let Some(nudge) = self.app_nudge.get() {
            nudge.app(name);
        }
    }

    /// Submit an app's rendered job, as the app reconciler (apps plan
    /// §6.3): through the normal submit path, as `legion:app-reconciler`
    /// holding the app's recorded grant, with the version stamped
    /// `managedBy` and its grant recorded in the name of whoever approved
    /// the app's. `key` is `app:<name>:<digest>:<from>`: a key a previous
    /// leader recorded is `deduplicated` when the job shows its submit took,
    /// and submitted again otherwise (a resubmit of the same render is
    /// `Unchanged`, so it never adds a version).
    pub(crate) fn submit_app_job(
        &self,
        record: &AppRecord,
        rendered: &RenderedApp,
        key: &str,
    ) -> Result<AppSubmit, Status> {
        let job_id = record.job_id();
        let managed_by = ManagedBy {
            app: record.name.clone(),
            digest: rendered.digest.clone(),
        };
        let stored_key = format!("app-reconciler/{key}");
        if let Some(message) = self
            .store
            .idempotency_lookup(&stored_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let took = self
                .store
                .app_job_version(&job_id)
                .map_err(|e| map_store_error("Error loading the app's job", e))?
                .is_some_and(|version| {
                    !version.is_stopped() && version.managed_by.as_ref() == Some(&managed_by)
                });
            if took {
                return Ok(AppSubmit {
                    message,
                    deduplicated: true,
                });
            }
        }
        let jobspec = serde_json::to_string(&rendered.jobspec)
            .map_err(|err| Status::internal(format!("rendering the app's job: {err}")))?;
        let primary = Submission {
            path: format!("app/{}/job.json", record.name),
            content: serde_json::to_string_pretty(&rendered.jobspec).unwrap_or(jobspec.clone()),
            is_primary: true,
        };
        let identity = reconciler_identity(record);
        let submitted = self.submit_rendered_job_from(
            &identity,
            &jobspec,
            vec![primary],
            None,
            false,
            None,
            Some(SYSTEM_NAMESPACE),
            SubmitOrigin::App {
                granted_by: format!("{} (app {})", record.grant.granted_by, record.name),
                managed_by,
            },
        )?;
        let message = submitted.message_with_warnings();
        self.store
            .idempotency_record(&stored_key, "app_reconcile", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(AppSubmit {
            message,
            deduplicated: false,
        })
    }

    /// Stop an app's job (a disabled app, or an uninstall), as StopJob
    /// does: the job's status, then a `job_stop` eval. `key` is
    /// `app:<name>:stop:<from>`.
    pub(crate) fn stop_app_job(&self, job_id: &str, key: &str) -> Result<String, Status> {
        let stored_key = format!("app-reconciler/{key}");
        if let Some(message) = self
            .store
            .idempotency_lookup(&stored_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            let stopped = self
                .store
                .app_job_version(job_id)
                .map_err(|e| map_store_error("Error loading the app's job", e))?
                .is_none_or(|version| version.is_stopped());
            if stopped {
                return Ok(message);
            }
        }
        self.stop_job_now(job_id)?;
        let message = format!("Stopped {job_id}");
        self.store
            .idempotency_record(&stored_key, "app_stop", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(message)
    }

    fn stop_job_now(&self, job_id: &str) -> Result<(), Status> {
        let job_config_id = self
            .store
            .get_job_config(job_id)
            .map_err(|e| map_store_error("Error stopping the app's job", e))?
            .id();
        self.store
            .stop_job(job_id)
            .map_err(|e| map_store_error("Error stopping the app's job", e))?;
        if let Err(status) = self
            .store
            .enqueue_eval(
                "job_stop",
                Some(job_id),
                Some(&job_config_id),
                None,
                Some(self.store.default_eval_priority),
            )
            .map_err(|e| map_store_error("Error stopping the app's job", e))
        {
            if !status_is_not_raft_leader(&status) {
                return Err(status);
            }
            warn!(
                job = job_id,
                "stop eval enqueue deferred: the scheduler leader's regeneration pass will \
                 re-derive it"
            );
        }
        Ok(())
    }

    /// The `App` message for a record: the record, plus its job's state and
    /// the status derived from both.
    pub(crate) fn app_info(&self, record: &AppRecord) -> Result<App, Status> {
        let job_id = record.job_id();
        let version: Option<AppJobVersion> = self
            .store
            .app_job_version(&job_id)
            .map_err(|e| map_store_error("Error loading the app's job", e))?;
        let allocations = self
            .store
            .app_job_allocations(&job_id)
            .map_err(|e| map_store_error("Error loading the app's allocations", e))?;
        let blocked = match &version {
            Some(_) => self
                .store
                .get_job_with_status(&job_id)
                .ok()
                .and_then(|job| {
                    let mut jobs = [job];
                    self.store.overlay_blocked_jobs(&mut jobs).ok()?;
                    let [job] = jobs;
                    job.blocked.map(|blocked| blocked.message)
                }),
            None => None,
        };
        let view = crate::manager::apps::derive_status(
            record,
            version.as_ref(),
            &allocations,
            crate::manager::apps::task_count(record),
            blocked.as_deref(),
        );
        let mut notes = Vec::new();
        if self.workload_identity.is_none() {
            notes.push(
                "workload identity is off on this manager (workloadIdentity.enabled = false): the \
                 app runs, but its `legion` identity gets no token, so it cannot reach Legion"
                    .to_string(),
            );
        }
        if record.enabled && record.settings.placement == Placement::Managers {
            let manager_nodes = self
                .store
                .get_nodes()
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter(|node| {
                            node.config_attrs
                                .get(MANAGER_ROLE_ATTR)
                                .and_then(Value::as_str)
                                == Some(MANAGER_ROLE_VALUE)
                        })
                        .count()
                })
                .unwrap_or(0);
            if manager_nodes == 0 {
                notes.push(format!(
                    "no manager runs a system worker, so `placement = managers` has nowhere to run; \
                     `legion app set {} --placement any` runs it on workers",
                    record.name
                ));
            }
        }
        Ok(App {
            name: record.name.clone(),
            description: record.manifest.description.clone(),
            version: record.display_version(),
            source: record.source.as_str().to_string(),
            source_ref: record.source_ref.clone(),
            enabled: record.enabled,
            settings: Some(settings_info(&record.settings)),
            status: view.status.to_string(),
            status_detail: view.detail,
            job_id,
            running: allocations.running,
            desired: view.desired,
            manifest_digest: record.manifest_digest.clone(),
            manifest_json: serde_json::to_string(&record.manifest).unwrap_or_default(),
            config_json: serde_json::to_string(&record.config).unwrap_or_default(),
            grant: Some(AppGrantInfo {
                rules_json: rules_json(&record.grant.rules),
                granted_by: record.grant.granted_by.clone(),
                granted_at: record.grant.granted_at.clone(),
            }),
            installed_by: record.installed_by.clone(),
            installed_at: record.installed_at.clone(),
            updated_at: record.updated_at.clone(),
            reconcile: Some(AppReconcileInfo {
                last_digest: record.state.last_digest.clone(),
                last_action: record.state.last_action.clone(),
                last_error: record.state.last_error.clone(),
                last_at: record.state.last_at.clone(),
                build_marker: record
                    .state
                    .marker
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                marker_reason: record.state.marker_reason.clone(),
                job_digest: version
                    .and_then(|version| version.managed_by)
                    .filter(|stamp| stamp.app == record.name)
                    .map(|stamp| stamp.digest)
                    .unwrap_or_default(),
            }),
            ui_slots: record.manifest.ui_slots(),
            notes,
            running_on: allocations.nodes,
        })
    }

    fn load_app(&self, name: &str) -> Result<AppRecord, Status> {
        self.store
            .get_app(name)
            .map_err(|e| map_store_error("Error loading the app", e))?
            .ok_or_else(|| not_found(name))
    }

    fn current_app(&self, name: &str) -> Result<Option<App>, Status> {
        match self
            .store
            .get_app(name)
            .map_err(|e| map_store_error("Error loading the app", e))?
        {
            Some(record) => Ok(Some(self.app_info(&record)?)),
            None => Ok(None),
        }
    }

    /// Write `next` over `record` (compare-and-set on `updated_at`).
    fn write_app(&self, record: &AppRecord, mut next: AppRecord) -> Result<AppRecord, Status> {
        next.updated_at = now_rfc3339();
        if next.updated_at == record.updated_at {
            // Two writes inside one clock tick: keep the stamp moving.
            next.updated_at.push_str("+1");
        }
        self.store
            .update_app(next.clone(), &record.updated_at)
            .map_err(|e| map_store_error("Error updating the app", e))?;
        Ok(next)
    }

    pub(super) async fn list_apps_rpc(
        &self,
        request: Request<ListAppsRequest>,
    ) -> Result<Response<ListAppsReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::App, AclAction::Read, AclScope::any())?;
        let records = self
            .store
            .list_apps()
            .map_err(|e| map_store_error("Error listing apps", e))?;
        let mut apps = Vec::new();
        for record in records.iter().filter(|record| {
            identity.allows(
                AclResource::App,
                AclAction::Read,
                &AclScope::app(&record.name),
            )
        }) {
            apps.push(self.app_info(record)?);
        }
        Ok(Response::new(ListAppsReply { apps }))
    }

    pub(super) async fn get_app_rpc(
        &self,
        request: Request<GetAppRequest>,
    ) -> Result<Response<GetAppReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        self.check_acl(
            &request,
            AclResource::App,
            AclAction::Read,
            AclScope::app(&name),
        )?;
        let record = self.load_app(&name)?;
        Ok(Response::new(GetAppReply {
            app: Some(self.app_info(&record)?),
        }))
    }

    pub(super) async fn install_app_rpc(
        &self,
        request: Request<InstallAppRequest>,
    ) -> Result<Response<InstallAppReply>, Status> {
        // ACL: coarse gate before the body is read; the app's name and its
        // permissions are checked once the manifest is parsed.
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let manifest = parse_manifest(&inner.manifest_json)?;
        let name = manifest.name.clone();
        acl::enforce(
            &identity,
            AclResource::App,
            AclAction::Write,
            &AclScope::app(&name),
        )?;
        if crate::apps::is_bundled(&name) {
            return Err(Status::failed_precondition(format!(
                "{name} is an app bundled with Legion; it is installed with the cluster, and \
                 upgrades with the binary"
            )));
        }
        let source = match inner.source.trim() {
            "" => AppSource::File,
            raw => match AppSource::parse(raw) {
                Ok(AppSource::Bundled) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "source {raw:?} is not one an install can name (file or url)"
                    )))
                }
                Ok(source) => source,
            },
        };
        let config = match inner.config_json.as_deref() {
            Some(raw) if !raw.trim().is_empty() => parse_config(raw)?,
            _ => manifest.default_config(),
        };
        validate_app_config(&manifest, &config).map_err(|err| {
            Status::invalid_argument(format!("the config does not match {name}'s schema: {err}"))
        })?;
        let who = match &manifest.version {
            Some(version) => format!("app {name} {version}"),
            None => format!("app {name}"),
        };
        let uncovered = app_grant_refusal(&identity, &who, &manifest.permissions);
        let prompt = install_prompt(&manifest, &self.app_user);
        let now = now_rfc3339();
        let record = AppRecord {
            name: name.clone(),
            source,
            source_ref: inner.source_ref.trim().to_string(),
            manifest_digest: manifest.digest(),
            enabled: true,
            settings: AppSettings::from_manifest(&manifest),
            config,
            grant: AppGrant {
                rules: manifest.permissions.clone(),
                granted_by: identity.principal.clone(),
                granted_at: now.clone(),
            },
            installed_by: identity.principal.clone(),
            installed_at: now.clone(),
            updated_at: now,
            state: AppReconcileState {
                pending_change: "installed".to_string(),
                ..Default::default()
            },
            manifest: manifest.clone(),
        };
        if inner.dry_run {
            return Ok(Response::new(InstallAppReply {
                app: Some(self.app_info(&record)?),
                message: match &uncovered {
                    None => format!("{who} can be installed by this token"),
                    Some(_) => format!("{who} needs more than this token holds"),
                },
                deduplicated: false,
                prompt,
                uncovered: uncovered.unwrap_or_default(),
            }));
        }
        if let Some(refusal) = uncovered {
            return Err(Status::permission_denied(format!(
                "{refusal}\n{GRANT_HINT}"
            )));
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "install_app",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(InstallAppReply {
                app: self.current_app(&name)?,
                message,
                deduplicated: true,
                prompt,
                uncovered: String::new(),
            }));
        }
        crate::storage::app_store::check_record(&record)
            .map_err(|e| map_store_error("Error installing the app", e))?;
        self.store
            .insert_app(record.clone())
            .map_err(|e| map_store_error("Error installing the app", e))?;
        let version = manifest.version.clone().unwrap_or_default();
        self.store
            .events()
            .app_installed(&name, &version, source.as_str(), &identity.principal);
        info!(app = %name, version = %version, by = %identity.principal, "Installed app");
        let message = format!(
            "installed {name} {version}; job {} starting",
            record.job_id()
        );
        self.store
            .idempotency_record(&idempotency_key, "install_app", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        self.nudge_app(&name);
        Ok(Response::new(InstallAppReply {
            app: Some(self.app_info(&record)?),
            message,
            deduplicated: false,
            prompt,
            uncovered: String::new(),
        }))
    }

    pub(super) async fn upgrade_app_rpc(
        &self,
        request: Request<UpgradeAppRequest>,
    ) -> Result<Response<UpgradeAppReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Write,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let record = self.load_app(&name)?;
        if record.is_bundled() {
            return Err(Status::failed_precondition(format!(
                "{name} is bundled with Legion and upgrades with the binary: its manifest moves \
                 when every node it can run on runs the new build"
            )));
        }
        let manifest = parse_manifest(&inner.manifest_json)?;
        if manifest.name != name {
            return Err(Status::invalid_argument(format!(
                "the manifest is for app {}, not {name}",
                manifest.name
            )));
        }
        let config = match inner.config_json.as_deref() {
            Some(raw) if !raw.trim().is_empty() => parse_config(raw)?,
            _ => record.config.clone(),
        };
        validate_app_config(&manifest, &config).map_err(|err| {
            Status::invalid_argument(format!(
                "the config does not match the new manifest's schema: {err}\nHint: pass a config \
                 that does with `legion app upgrade {name} <manifest> --config <file>`"
            ))
        })?;
        let diff = permission_diff(&record.grant.rules, &manifest.permissions);
        let diff_message = PermissionDiff {
            added_json: rules_json(&diff.added),
            removed_json: rules_json(&diff.removed),
            unchanged_json: rules_json(&diff.unchanged),
        };
        let prompt = install_prompt(&manifest, &self.app_user);
        let from = record.display_version();
        let to = manifest.version.clone().unwrap_or_default();
        let who = format!("app {name} {to}");
        // A new manifest is new code running with the grant: the caller must
        // hold all of it, as at install.
        let uncovered = app_grant_refusal(&identity, &who, &manifest.permissions);
        if inner.dry_run {
            return Ok(Response::new(UpgradeAppReply {
                app: Some(self.app_info(&record)?),
                diff: Some(diff_message),
                upgraded: false,
                message: match uncovered {
                    None => format!("dry run: {name} {from} → {to} can be upgraded by this token"),
                    Some(refusal) => format!("dry run: {refusal}"),
                },
                deduplicated: false,
                prompt,
            }));
        }
        if let Some(refusal) = uncovered {
            return Err(Status::permission_denied(format!(
                "{refusal}\n{GRANT_HINT}"
            )));
        }
        if !diff.added.is_empty() && !inner.approve {
            return Ok(Response::new(UpgradeAppReply {
                app: Some(self.app_info(&record)?),
                diff: Some(diff_message),
                upgraded: false,
                message: format!(
                    "{name} {to} asks for {} permission(s) the installed version does not have; \
                     approve them to upgrade",
                    diff.added.len()
                ),
                deduplicated: false,
                prompt,
            }));
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "upgrade_app",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(UpgradeAppReply {
                app: self.current_app(&name)?,
                diff: Some(diff_message),
                upgraded: true,
                message,
                deduplicated: true,
                prompt,
            }));
        }
        let mut next = record.clone();
        next.manifest_digest = manifest.digest();
        next.manifest = manifest;
        next.config = config;
        if !inner.source_ref.trim().is_empty() {
            next.source_ref = inner.source_ref.trim().to_string();
            if next.source_ref.starts_with("http://") || next.source_ref.starts_with("https://") {
                next.source = AppSource::Url;
            } else {
                next.source = AppSource::File;
            }
        }
        next.grant = AppGrant {
            rules: next.manifest.permissions.clone(),
            granted_by: identity.principal.clone(),
            granted_at: now_rfc3339(),
        };
        next.state.pending_change = format!("upgraded {from} → {to}");
        let next = self.write_app(&record, next)?;
        self.store.events().app_upgraded(
            &name,
            &from,
            &to,
            diff.added.len(),
            diff.removed.len(),
            &identity.principal,
        );
        let message = format!("upgraded {name} {from} → {to}");
        self.store
            .idempotency_record(&idempotency_key, "upgrade_app", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        self.nudge_app(&name);
        Ok(Response::new(UpgradeAppReply {
            app: Some(self.app_info(&next)?),
            diff: Some(diff_message),
            upgraded: true,
            message,
            deduplicated: false,
            prompt,
        }))
    }

    pub(super) async fn uninstall_app_rpc(
        &self,
        request: Request<UninstallAppRequest>,
    ) -> Result<Response<UninstallAppReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Write,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "uninstall_app",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(UninstallAppReply {
                message,
                revoked_grants: 0,
                deduplicated: true,
            }));
        }
        let record = self.load_app(&name)?;
        if record.is_bundled() {
            return Err(Status::failed_precondition(format!(
                "{name} is bundled with Legion and cannot be uninstalled; `legion app disable \
                 {name}` stops it and keeps its settings"
            )));
        }
        // Disable first, so a reconcile pass that runs in the middle stops
        // the job rather than restarting it, and a retry after a failure
        // below finds the record and finishes.
        let record = if record.enabled {
            let mut next = record.clone();
            next.enabled = false;
            next.state.pending_change = "uninstalling".to_string();
            self.write_app(&record, next)?
        } else {
            record
        };
        let job_id = record.job_id();
        let running = self
            .store
            .app_job_version(&job_id)
            .map_err(|e| map_store_error("Error loading the app's job", e))?
            .is_some_and(|version| !version.is_stopped());
        if running {
            self.stop_job_now(&job_id)?;
        }
        let revoked = self
            .store
            .revoke_job_grants(&job_id, None, &identity.principal)
            .map_err(|e| map_store_error("Error revoking the app's grants", e))?;
        self.store
            .delete_app(&name)
            .map_err(|e| map_store_error("Error uninstalling the app", e))?;
        self.store
            .events()
            .app_uninstalled(&name, &identity.principal, revoked);
        info!(app = %name, by = %identity.principal, revoked, "Uninstalled app");
        let message = format!(
            "uninstalled {name}; job {job_id} stopped and {revoked} grant(s) revoked (`legion job \
             destroy {job_id}` removes the job)"
        );
        self.store
            .idempotency_record(&idempotency_key, "uninstall_app", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        Ok(Response::new(UninstallAppReply {
            message,
            revoked_grants: u32::try_from(revoked).unwrap_or(u32::MAX),
            deduplicated: false,
        }))
    }

    pub(super) async fn set_app_enabled_rpc(
        &self,
        request: Request<SetAppEnabledRequest>,
    ) -> Result<Response<SetAppEnabledReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Write,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "set_app_enabled",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(SetAppEnabledReply {
                app: self.current_app(&name)?,
                message,
                deduplicated: true,
            }));
        }
        let record = self.load_app(&name)?;
        let mut next = record.clone();
        next.enabled = inner.enabled;
        next.state.pending_change = if inner.enabled { "enabled" } else { "disabled" }.to_string();
        let next = self.write_app(&record, next)?;
        if record.enabled != inner.enabled {
            self.store
                .events()
                .app_enabled(&name, inner.enabled, &identity.principal);
        }
        let message = match (record.enabled, inner.enabled) {
            (false, true) => format!("enabled {name}; its job starts"),
            (true, false) => format!("disabled {name}; its job stops, and its settings are kept"),
            (true, true) => format!("{name} is enabled; reconciling it again"),
            (false, false) => format!("{name} is already disabled"),
        };
        self.store
            .idempotency_record(&idempotency_key, "set_app_enabled", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        self.nudge_app(&name);
        Ok(Response::new(SetAppEnabledReply {
            app: Some(self.app_info(&next)?),
            message,
            deduplicated: false,
        }))
    }

    pub(super) async fn update_app_settings_rpc(
        &self,
        request: Request<UpdateAppSettingsRequest>,
    ) -> Result<Response<UpdateAppSettingsReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Write,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "update_app_settings",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(UpdateAppSettingsReply {
                app: self.current_app(&name)?,
                message,
                deduplicated: true,
            }));
        }
        let record = self.load_app(&name)?;
        let mut settings = record.settings.clone();
        if let Some(replicas) = inner.replicas {
            settings.replicas = replicas;
        }
        if let Some(placement) = inner.placement.as_deref() {
            settings.placement = Placement::parse(placement).map_err(Status::invalid_argument)?;
            // Managers placement has no pool; moving there drops one that
            // was set for workers, unless this request names one too.
            if settings.placement == Placement::Managers && inner.pool.is_none() {
                settings.pool = None;
            }
        }
        if let Some(pool) = inner.pool.as_deref() {
            settings.pool = (!pool.trim().is_empty()).then(|| pool.trim().to_string());
        }
        if inner.cpu.is_some() || inner.memory.is_some() {
            let mut resources = settings.resources.unwrap_or_default();
            if let Some(cpu) = inner.cpu {
                resources.cpu = (cpu > 0).then_some(cpu);
            }
            if let Some(memory) = inner.memory {
                resources.memory = (memory > 0).then_some(memory);
            }
            settings.resources =
                (resources.cpu.is_some() || resources.memory.is_some()).then_some(resources);
        }
        settings.validate().map_err(Status::invalid_argument)?;
        let change = record.settings.describe_change(&settings);
        if settings == record.settings {
            return Ok(Response::new(UpdateAppSettingsReply {
                app: Some(self.app_info(&record)?),
                message: format!("{name}: settings unchanged"),
                deduplicated: false,
            }));
        }
        let mut next = record.clone();
        next.settings = settings;
        next.state.pending_change = change.clone();
        let next = self.write_app(&record, next)?;
        info!(app = %name, by = %identity.principal, change = %change, "Changed app settings");
        let message = format!("{name}: {change}");
        self.store
            .idempotency_record(&idempotency_key, "update_app_settings", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        self.nudge_app(&name);
        Ok(Response::new(UpdateAppSettingsReply {
            app: Some(self.app_info(&next)?),
            message,
            deduplicated: false,
        }))
    }

    pub(super) async fn set_app_config_rpc(
        &self,
        request: Request<SetAppConfigRequest>,
    ) -> Result<Response<SetAppConfigReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        let identity = self.check_acl(
            &request,
            AclResource::App,
            AclAction::Write,
            AclScope::app(&name),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "set_app_config",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(SetAppConfigReply {
                app: self.current_app(&name)?,
                message,
                code_fields_changed: Vec::new(),
                deduplicated: true,
            }));
        }
        let record = self.load_app(&name)?;
        let config = parse_config(&inner.config_json)?;
        validate_app_config(&record.manifest, &config).map_err(|err| {
            Status::invalid_argument(format!("the config does not match {name}'s schema: {err}"))
        })?;
        let code_fields = changed_code_fields(&record.manifest, &record.config, &config);
        if !code_fields.is_empty() {
            // Changing what code the app runs needs everything the app's
            // grant holds, as installing it does (calls entry 5).
            let who = format!(
                "changing app {name}'s code ({}) runs code with its grant, which",
                code_fields.join(", ")
            );
            if let Some(refusal) = app_grant_refusal(&identity, &who, &record.grant.rules) {
                return Err(Status::permission_denied(format!(
                    "{refusal}\n{GRANT_HINT}"
                )));
            }
        }
        if config == record.config {
            return Ok(Response::new(SetAppConfigReply {
                app: Some(self.app_info(&record)?),
                message: format!("{name}: config unchanged"),
                code_fields_changed: Vec::new(),
                deduplicated: false,
            }));
        }
        let mut next = record.clone();
        next.config = config;
        next.state.pending_change = if code_fields.is_empty() {
            "config changed".to_string()
        } else {
            format!(
                "config changed, including code fields {}",
                code_fields.join(", ")
            )
        };
        let next = self.write_app(&record, next)?;
        self.store
            .events()
            .app_config_changed(&name, &identity.principal, &code_fields);
        let message = format!("{name}: config changed; its job rolls with the new config");
        self.store
            .idempotency_record(&idempotency_key, "set_app_config", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        self.nudge_app(&name);
        Ok(Response::new(SetAppConfigReply {
            app: Some(self.app_info(&next)?),
            message,
            code_fields_changed: code_fields,
            deduplicated: false,
        }))
    }
}
