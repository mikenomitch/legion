//! Apps (apps plan §9 and §12, phase A3; docs/apps.md).
//!
//! In process, against a manager service with ACLs on, an app reconciler
//! driven one pass at a time, and simulated nodes reporting `buildVersion`
//! and `buildId`:
//!
//! - seeding happens once, honors the manager config's `apps` block only
//!   then, and grants bundled apps' permissions as `legion:bootstrap`;
//! - an unchanged record submits nothing (the digests match, though the
//!   stored config was normalized);
//! - a direct submit is reverted within one pass, and the event names who
//!   made it; the same edit twice is reverted twice;
//! - disable then enable runs the job again; a direct `job stop` is
//!   restarted;
//! - a KV re-render keeps the `managedBy` stamp; a RunJobTemplate by another
//!   principal clears it;
//! - a leader killed between the submit and recording it leaves one version;
//! - a burst of record changes coalesces;
//! - a validation-refused submit waits for the next change;
//! - uninstall stops the job and revokes the grant; a placement change
//!   re-renders permits and constraints;
//! - a code-field config change is refused for `app:write` without the
//!   app's grant;
//! - the build marker: agreement, one roll for a rolling upgrade, a rebuild
//!   with a new `buildId` rolls, an older node joining does not move it
//!   back, `unknown` is left out, the constraint, the straggler status, and
//!   manifest adoption only when the leader runs the marker's build;
//! - the §6.8 GitOps-shaped manifest: the prompt, the grant, the job.
//!
//! The end to end case, `legion app list` on a fresh single-manager cluster
//! showing the autoscaler running on the system worker, starts the real
//! binary (`fresh_cluster_runs_the_autoscaler_on_the_system_worker`).

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::manager::acl::Authenticator;
use legion::manager::apps::{AppReconciler, PassOutcome, ReconcilerSettings};
use legion::manager::server::ManagerDaemonService;
use legion::storage::store::Store;
use legion::structs::app::{app_job_id, BuildMarker, BOOTSTRAP_PRINCIPAL};
use legion::structs::config::AppSeedConfig;
use legion::structs::manager_server::Manager;
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const AUTOSCALER: &str = "legion-system/autoscaler";

struct Fixture {
    svc: Arc<ManagerDaemonService>,
    store: Store,
    admin: String,
}

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-apps-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    store.ensure_system_namespace()?;
    let auth = Arc::new(Authenticator::from_config(
        &AclConfig {
            enabled: Some(true),
            ..Default::default()
        }
        .with_defaults(),
    )?);
    let (_, admin) = auth
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .context("a fresh store has no management token")?;
    let svc = Arc::new(ManagerDaemonService::with_acl(store.clone(), auth));
    Ok(Fixture { svc, store, admin })
}

/// A manager's system worker on `version`/`build`.
fn manager_node(id: &str, version: &str, build: &str) -> NodeMsg {
    NodeMsg {
        id: id.into(),
        pool: "legion-system".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: json!({
            "legion.role": "manager",
            "namespaces": ["legion-system"],
            "maxAllocations": 10,
        })
        .to_string(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: json!({ "buildVersion": version, "buildId": build }).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    }
}

fn marker(version: &str, build: &str) -> BuildMarker {
    BuildMarker {
        version: version.into(),
        build_id: build.into(),
    }
}

impl Fixture {
    fn reconciler(&self, own: BuildMarker) -> AppReconciler {
        self.reconciler_seeded(own, BTreeMap::new())
    }

    fn reconciler_seeded(
        &self,
        own: BuildMarker,
        seed: BTreeMap<String, AppSeedConfig>,
    ) -> AppReconciler {
        AppReconciler::new(
            self.svc.clone(),
            ReconcilerSettings {
                app_user: "nobody".into(),
                seed,
                own_build: own,
            },
        )
    }

    async fn token(&self, name: &str, rules: Value) -> R<String> {
        self.svc
            .upsert_acl_policy(req(
                UpsertAclPolicyRequest {
                    name: name.into(),
                    description: String::new(),
                    rules_json: rules.to_string(),
                },
                &self.admin,
            ))
            .await?;
        Ok(self
            .svc
            .create_acl_token(req(
                CreateAclTokenRequest {
                    name: name.into(),
                    token_type: "client".into(),
                    policies: vec![name.into()],
                    ttl: None,
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .secret)
    }

    /// A token that holds the autoscaler's grant and may write its job:
    /// someone who could edit the app's job directly.
    async fn editor(&self, name: &str) -> R<String> {
        self.token(name, editor_rules()).await
    }

    fn version(&self, job: &str) -> R<Option<legion::storage::app_store::AppJobVersion>> {
        self.store.app_job_version(job)
    }

    fn version_number(&self, job: &str) -> R<i64> {
        Ok(self
            .version(job)?
            .map(|version| version.version)
            .unwrap_or(0))
    }

    fn app(&self, name: &str) -> R<legion::structs::app::AppRecord> {
        self.store.get_app(name)?.context("app record")
    }

    /// The stored config of `job`'s latest version, as JSON.
    fn stored(&self, job: &str) -> R<Value> {
        Ok(serde_json::to_value(self.store.get_job_config(job)?)?)
    }

    /// Submit `spec` as `token` through RunJob.
    async fn run(&self, spec: &Value, token: &str) -> Result<RunJobReply, tonic::Status> {
        self.svc
            .run_job(req(
                RunJobRequest {
                    parsed_jobspec: spec.to_string(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    fn events(&self, kind: &str) -> R<Vec<legion::storage::events::SchedulerEventRecord>> {
        self.store
            .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
                kind: Some(kind.into()),
                limit: Some(500),
                ..Default::default()
            })
    }

    async fn set_enabled(&self, app: &str, enabled: bool) -> R<()> {
        self.svc
            .set_app_enabled(req(
                SetAppEnabledRequest {
                    name: app.into(),
                    enabled,
                    idempotency_key: None,
                },
                &self.admin,
            ))
            .await?;
        Ok(())
    }

    async fn set_config(&self, app: &str, config: Value, token: &str) -> Result<(), tonic::Status> {
        self.svc
            .set_app_config(req(
                SetAppConfigRequest {
                    name: app.into(),
                    config_json: config.to_string(),
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ())
    }

    async fn get_app(&self, app: &str) -> R<App> {
        self.svc
            .get_app(req(GetAppRequest { name: app.into() }, &self.admin))
            .await?
            .into_inner()
            .app
            .context("app")
    }
}

/// Rules that hold the autoscaler's grant and `job:write` (host) on its job.
fn editor_rules() -> Value {
    json!([
        { "resources": ["job", "scale"], "actions": ["read", "write"],
          "namespaces": ["*", "legion-*"], "host": true },
        { "resources": ["node"], "actions": ["read", "write"] },
        { "resources": ["kv"], "actions": ["read", "write"], "paths": ["legion/*"] },
    ])
}

/// A fixture with three managers on one build, the autoscaler seeded, and
/// its job submitted once.
async fn seeded() -> R<(Fixture, AppReconciler)> {
    let fx = fixture()?;
    for id in ["m1", "m2", "m3"] {
        fx.store.insert_node(manager_node(id, "0.1.0", "a"))?;
    }
    let mut reconciler = fx.reconciler(marker("0.1.0", "a"));
    assert_eq!(reconciler.seed()?, vec!["autoscaler".to_string()]);
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(
        matches!(&outcome, PassOutcome::Submitted { action, deduplicated: false } if action == "created the job"),
        "{outcome:?}"
    );
    Ok((fx, reconciler))
}

fn submitted(outcome: &PassOutcome) -> bool {
    matches!(outcome, PassOutcome::Submitted { .. })
}

/// Seeding happens once, honors the manager config's `apps` block only
/// then, and records bundled grants as granted by `legion:bootstrap`.
#[tokio::test(flavor = "multi_thread")]
async fn seeding_happens_once_test() -> R<()> {
    let fx = fixture()?;
    let mut seed = BTreeMap::new();
    seed.insert(
        "autoscaler".to_string(),
        AppSeedConfig {
            enabled: Some(false),
            ..Default::default()
        },
    );
    let reconciler = fx.reconciler_seeded(marker("0.1.0", "a"), seed);
    assert_eq!(reconciler.seed()?, vec!["autoscaler".to_string()]);
    let record = fx.app("autoscaler")?;
    assert!(!record.enabled, "the seed-time override applied");
    assert_eq!(record.grant.granted_by, BOOTSTRAP_PRINCIPAL);
    assert_eq!(record.installed_by, BOOTSTRAP_PRINCIPAL);
    assert_eq!(
        record.grant.rules,
        legion::apps::bundled_manifest("autoscaler")
            .context("bundled")?
            .permissions
    );
    assert_eq!(fx.events("app.installed")?.len(), 1);

    // An operator enables it; a later leader's seed pass, with a config
    // that still says disabled, changes nothing.
    fx.set_enabled("autoscaler", true).await?;
    let mut seed = BTreeMap::new();
    seed.insert(
        "autoscaler".to_string(),
        AppSeedConfig {
            enabled: Some(false),
            ..Default::default()
        },
    );
    let later = fx.reconciler_seeded(marker("0.1.0", "a"), seed);
    assert!(later.seed()?.is_empty());
    assert!(fx.app("autoscaler")?.enabled);
    assert_eq!(fx.events("app.installed")?.len(), 1);
    Ok(())
}

/// An unchanged record makes no submit, although the stored config was
/// normalized: the reconciler compares digests, not jobs.
#[tokio::test(flavor = "multi_thread")]
async fn an_unchanged_record_makes_no_submit_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    let stamp = fx.version(AUTOSCALER)?.context("job")?.managed_by;
    assert_eq!(
        stamp.as_ref().map(|stamp| stamp.app.as_str()),
        Some("autoscaler")
    );
    // The stored config is normalized (defaults filled in), so it is not
    // the render; the digest is what matches.
    let rendered = legion::manager::apps::render_for(
        &fx.app("autoscaler")?,
        Some(&marker("0.1.0", "a")),
        "nobody",
    )
    .map_err(|err| anyhow!(err))?;
    assert_ne!(fx.stored(AUTOSCALER)?, rendered.jobspec);
    for _ in 0..3 {
        assert_eq!(
            reconciler.reconcile_app("autoscaler").await,
            PassOutcome::Unchanged
        );
    }
    assert_eq!(fx.version_number(AUTOSCALER)?, 1);
    assert_eq!(fx.app("autoscaler")?.state.last_digest, rendered.digest);
    let info = fx
        .svc
        .get_job_info(req(
            GetJobInfoRequest {
                name: AUTOSCALER.into(),
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    assert_eq!(
        info.managed_by.map(|stamp| stamp.digest),
        Some(rendered.digest)
    );
    Ok(())
}

/// A direct submit is reverted within one pass and the event names who made
/// it; the same edit made twice is reverted twice (the second revert starts
/// from another version, so its key is new).
#[tokio::test(flavor = "multi_thread")]
async fn direct_edits_are_reverted_every_time_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    let alice = fx.editor("alice").await?;
    let mut edited: Value = serde_json::to_value(fx.store.get_job_config(AUTOSCALER)?)?;
    edited["tasks"][0]["count"] = json!(2);
    edited["version"] = Value::Null;

    for round in 1..=2 {
        fx.run(&edited, &alice).await?;
        let version = fx.version(AUTOSCALER)?.context("job")?;
        assert_eq!(version.managed_by, None, "a direct edit clears the stamp");
        assert_eq!(version.submitted_by.as_deref(), Some("alice"));
        let outcome = reconciler.reconcile_app("autoscaler").await;
        assert_eq!(
            outcome,
            PassOutcome::Submitted {
                action: "reverted a direct edit by alice".into(),
                deduplicated: false,
            },
            "round {round}"
        );
        let reverted = fx.version(AUTOSCALER)?.context("job")?;
        assert_eq!(reverted.version, 1 + 2 * round);
        assert!(reverted.managed_by.is_some());
        assert_eq!(fx.stored(AUTOSCALER)?["tasks"][0]["count"], 1);
    }
    let reverted = fx.events("app.reconciled")?;
    let reverted: Vec<&String> = reverted
        .iter()
        .map(|event| &event.message)
        .filter(|message| message.contains("reverted a direct edit by alice"))
        .collect();
    assert_eq!(reverted.len(), 2, "{reverted:?}");
    Ok(())
}

/// Disable stops the job; enable within the hour runs it again (a new key,
/// from `stopped-<version>`); a direct `job stop` of an enabled app is
/// restarted.
#[tokio::test(flavor = "multi_thread")]
async fn disable_enable_and_a_direct_stop_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    fx.set_enabled("autoscaler", false).await?;
    assert_eq!(
        reconciler.reconcile_app("autoscaler").await,
        PassOutcome::Stopped
    );
    assert!(fx.version(AUTOSCALER)?.context("job")?.is_stopped());
    assert_eq!(fx.get_app("autoscaler").await?.status, "stopped");
    assert_eq!(
        reconciler.reconcile_app("autoscaler").await,
        PassOutcome::Unchanged
    );

    fx.set_enabled("autoscaler", true).await?;
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(submitted(&outcome), "{outcome:?}");
    assert!(!fx.version(AUTOSCALER)?.context("job")?.is_stopped());

    // A direct stop.
    fx.svc
        .stop_job(req(
            StopJobRequest {
                name: AUTOSCALER.into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    assert!(fx.version(AUTOSCALER)?.context("job")?.is_stopped());
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(
        matches!(&outcome, PassOutcome::Submitted { action, .. } if action.starts_with("started the job")),
        "{outcome:?}"
    );
    assert!(!fx.version(AUTOSCALER)?.context("job")?.is_stopped());
    // Restarting an unchanged spec stores no new version.
    assert_eq!(fx.version_number(AUTOSCALER)?, 1);
    Ok(())
}

/// A KV re-render (Legion storing the version itself) keeps the stamp, so
/// the reconciler leaves it alone; RunJobTemplate by another principal
/// clears it, and the reconciler reverts it.
#[tokio::test(flavor = "multi_thread")]
async fn kv_rerenders_keep_the_stamp_and_template_submits_clear_it_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    let stamp = fx.version(AUTOSCALER)?.context("job")?.managed_by;

    // What the KV re-render path stores: the job with re-rendered values.
    let mut rerendered = fx.store.get_job_config(AUTOSCALER)?;
    rerendered.tasks[0]
        .config
        .as_object_mut()
        .context("config")?
        .insert("cwd".into(), json!("/tmp"));
    rerendered.version = None;
    fx.store.handle_job_submit(
        &rerendered,
        &[Submission {
            path: "rerender.json".into(),
            content: "{}".into(),
            is_primary: true,
        }],
    )?;
    let version = fx.version(AUTOSCALER)?.context("job")?;
    assert_eq!(version.version, 2);
    assert_eq!(version.managed_by, stamp, "a re-render keeps the stamp");
    assert_eq!(
        reconciler.reconcile_app("autoscaler").await,
        PassOutcome::Unchanged
    );

    // A template submit by the admin.
    let mut through_template: Value = serde_json::to_value(&rerendered)?;
    through_template["tasks"][0]["count"] = json!(3);
    fx.svc
        .upsert_job_template(req(
            UpsertJobTemplateRequest {
                name: "autoscaler-template".into(),
                source_nickel: format!(
                    "{{ description = \"apps\", variables = [], job | not_exported = fun vars \
                     => std.deserialize 'Json {} }}",
                    serde_json::to_string(&through_template.to_string())?
                ),
            },
            &fx.admin,
        ))
        .await?;
    fx.svc
        .run_job_template(req(
            RunJobTemplateRequest {
                name: "autoscaler-template".into(),
                variables_json: "{}".into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let version = fx.version(AUTOSCALER)?.context("job")?;
    assert_eq!(version.version, 3);
    assert_eq!(
        version.managed_by, None,
        "a template submit clears the stamp"
    );
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(
        matches!(&outcome, PassOutcome::Submitted { action, .. } if action.starts_with("reverted a direct edit")),
        "{outcome:?}"
    );
    Ok(())
}

/// A leader killed between submitting and recording that it did: the next
/// leader's pass finds the stamp and does nothing, and a resubmit with the
/// same key is deduplicated. Either way, one version.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_killed_mid_reconcile_leaves_one_version_test() -> R<()> {
    let fx = fixture()?;
    fx.store.insert_node(manager_node("m1", "0.1.0", "a"))?;
    let reconciler = fx.reconciler(marker("0.1.0", "a"));
    reconciler.seed()?;
    let record = fx.app("autoscaler")?;
    let rendered =
        legion::manager::apps::render_for(&record, Some(&marker("0.1.0", "a")), "nobody")
            .map_err(|err| anyhow!(err))?;
    let key = format!("app:autoscaler:{}:none", rendered.digest);

    // The old leader's submit landed; it died before recording anything.
    let first = legion::manager::apps::submit_rendered(&fx.svc, &record, &rendered, &key)?;
    assert!(!first.1, "the first submit is not deduplicated");
    assert_eq!(fx.version_number(AUTOSCALER)?, 1);
    assert_eq!(fx.app("autoscaler")?.state.last_digest, "");

    // A retry with the same key is deduplicated.
    let again = legion::manager::apps::submit_rendered(&fx.svc, &record, &rendered, &key)?;
    assert!(again.1, "the same transition's key is deduplicated");

    // The new leader's pass sees the stamp and submits nothing.
    let mut new_leader = fx.reconciler(marker("0.1.0", "a"));
    assert_eq!(
        new_leader.reconcile_app("autoscaler").await,
        PassOutcome::Unchanged
    );
    assert_eq!(fx.version_number(AUTOSCALER)?, 1);
    assert_eq!(fx.app("autoscaler")?.state.last_digest, rendered.digest);
    Ok(())
}

/// A burst of record changes makes one submit per settled state, not one
/// per change: the loop coalesces what arrives during a pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_burst_of_changes_coalesces_test() -> R<()> {
    let fx = fixture()?;
    fx.store.insert_node(manager_node("m1", "0.1.0", "a"))?;
    let nudge = legion::manager::apps::spawn_reconciler(
        fx.svc.clone(),
        ReconcilerSettings {
            app_user: "nobody".into(),
            seed: BTreeMap::new(),
            own_build: marker("0.1.0", "a"),
        },
    );
    fx.svc.set_app_nudge(nudge);
    // The loop seeds and submits on its own.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while fx.version_number(AUTOSCALER)? == 0 {
        assert!(std::time::Instant::now() < deadline, "no first submit");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(fx.version_number(AUTOSCALER)?, 1);

    let changes = 8;
    for replicas in 0..changes {
        fx.svc
            .update_app_settings(req(
                UpdateAppSettingsRequest {
                    name: "autoscaler".into(),
                    replicas: Some(2 + replicas),
                    ..Default::default()
                },
                &fx.admin,
            ))
            .await?;
    }
    // Settled: the job carries the final record's digest.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let record = fx.app("autoscaler")?;
        let rendered =
            legion::manager::apps::render_for(&record, Some(&marker("0.1.0", "a")), "nobody")
                .map_err(|err| anyhow!(err))?;
        let stamp = fx.version(AUTOSCALER)?.and_then(|v| v.managed_by);
        if stamp.as_ref().map(|stamp| stamp.digest.as_str()) == Some(rendered.digest.as_str()) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never settled");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let versions = fx.version_number(AUTOSCALER)?;
    assert!(
        versions < 1 + i64::from(changes),
        "{changes} changes in a burst made {} submits",
        versions - 1
    );
    assert_eq!(fx.stored(AUTOSCALER)?["tasks"][0]["count"], 2 + changes - 1);
    Ok(())
}

/// A manifest whose job installs (it parses) but that submit refuses: an
/// exec task as root in `legion-system`.
fn root_manifest() -> Value {
    json!({
        "name": "rooted",
        "version": "1.0.0",
        "job": { "task": { "name": "main", "runtime": "exec",
                            "config": { "command": ["sleep", "1000"], "user": "root" } } },
    })
}

async fn install(
    fx: &Fixture,
    manifest: &Value,
    token: &str,
) -> Result<InstallAppReply, tonic::Status> {
    fx.svc
        .install_app(req(
            InstallAppRequest {
                manifest_json: manifest.to_string(),
                source: "file".into(),
                source_ref: "./app.ncl".into(),
                ..Default::default()
            },
            token,
        ))
        .await
        .map(tonic::Response::into_inner)
}

/// A submit refused on validation does not retry on its own: it waits for
/// the next change to the app.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_submit_waits_for_a_change_test() -> R<()> {
    let fx = fixture()?;
    install(&fx, &root_manifest(), &fx.admin).await?;
    let mut reconciler = fx.reconciler(marker("0.1.0", "a"));
    let outcome = reconciler.reconcile_app("rooted").await;
    assert!(
        matches!(&outcome, PassOutcome::Refused(message) if message.contains("root")),
        "{outcome:?}"
    );
    let app = fx.get_app("rooted").await?;
    assert_eq!(app.status, "degraded");
    assert!(
        app.status_detail.starts_with("reconcile refused:"),
        "{}",
        app.status_detail
    );
    for _ in 0..3 {
        assert_eq!(
            reconciler.reconcile_app("rooted").await,
            PassOutcome::Skipped
        );
    }
    assert_eq!(fx.events("app.reconcile_failed")?.len(), 1);
    assert!(fx.version(&app_job_id("rooted"))?.is_none());

    // A change to the app is a new input: tried again.
    fx.svc
        .update_app_settings(req(
            UpdateAppSettingsRequest {
                name: "rooted".into(),
                replicas: Some(2),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    assert!(matches!(
        reconciler.reconcile_app("rooted").await,
        PassOutcome::Refused(_)
    ));
    Ok(())
}

/// The §6.8 GitOps-shaped manifest: the prompt says what runs where, as
/// whom, with what grant and UI; a token lacking the grant cannot install
/// it; the admin can, the grant is recorded in their name, and the
/// reconciled job carries it. Uninstall stops the job and revokes the grant.
#[tokio::test(flavor = "multi_thread")]
async fn a_third_party_app_installs_reconciles_and_uninstalls_test() -> R<()> {
    let fx = fixture()?;
    let manifest = json!({
        "name": "gitops",
        "version": "0.2.0",
        "placement": "managers",
        "job": { "task": { "name": "sync", "runtime": "exec",
            "artifacts": ["https://example.com/gitops/0.2.0/legion-gitops-linux-amd64.tar.gz"],
            "config": { "command": ["legion-gitops"] } } },
        "permissions": [
            { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["team-*"] },
            { "resources": ["template"], "actions": ["read"] },
            { "resources": ["kv"], "actions": ["read"], "paths": ["legion/apps/gitops/*"] },
        ],
        "ui": {
            "nav": [{ "title": "GitOps", "view": "repos" }],
            "job": [{ "title": "Sync", "view": "job-sync",
                      "permission": { "resource": "job", "action": "read" } }],
            "actions": [{ "name": "sync-now", "title": "Sync now",
                          "permission": { "resource": "job", "action": "write" } }],
        },
        "config": {
            "schema": { "type": "object", "properties": { "repos": { "type": "array" } } },
            "default": { "repos": [] },
        },
    });

    // The prompt, from a dry run.
    let preview = fx
        .svc
        .install_app(req(
            InstallAppRequest {
                manifest_json: manifest.to_string(),
                source: "url".into(),
                source_ref: "https://example.com/gitops/0.2.0/app.ncl".into(),
                dry_run: true,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    let prompt = preview.prompt.join("\n");
    assert!(
        prompt.starts_with(
            "gitops 0.2.0 runs its own code (1 artifact) on your managers, as user nobody,"
        ),
        "{prompt}"
    );
    assert!(
        prompt.contains("in legion-system, and asks for:"),
        "{prompt}"
    );
    assert!(prompt.contains("namespaces: team-*"), "{prompt}");
    assert!(
        prompt.contains(
            "UI: a GitOps page, a Sync panel on job pages, one action (Sync now, needs job:write)"
        ),
        "{prompt}"
    );
    assert!(preview.uncovered.is_empty());
    assert!(
        fx.store.get_app("gitops")?.is_none(),
        "a dry run installs nothing"
    );

    // `app:write` alone does not cover the grant.
    let operator = fx
        .token(
            "operator",
            json!([{ "resources": ["app"], "actions": ["read", "write"] }]),
        )
        .await?;
    let refused = install(&fx, &manifest, &operator)
        .await
        .expect_err("the operator holds none of the grant");
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(
        refused.message().starts_with(
            "app gitops 0.2.0 asks for job:write in team-*, which your token does not hold"
        ),
        "{}",
        refused.message()
    );

    let installed = install(&fx, &manifest, &fx.admin).await?;
    assert_eq!(
        installed.message,
        "installed gitops 0.2.0; job legion-system/gitops starting"
    );
    let record = fx.app("gitops")?;
    assert_eq!(record.grant.rules.len(), 3);
    let mut reconciler = fx.reconciler(marker("0.1.0", "a"));
    let outcome = reconciler.reconcile_app("gitops").await;
    assert!(submitted(&outcome), "{outcome:?}");
    let stored = fx.stored("legion-system/gitops")?;
    assert_eq!(stored["tasks"][0]["identities"][0]["name"], "legion");
    assert_eq!(
        stored["tasks"][0]["identities"][0]["permissions"],
        serde_json::to_value(&record.grant.rules)?
    );
    assert_eq!(stored["tasks"][0]["config"]["user"], "nobody");
    let grants = fx
        .svc
        .list_grants(req(
            ListGrantsRequest {
                job: Some("legion-system/gitops".into()),
                include_revoked: false,
            },
            &fx.admin,
        ))
        .await?
        .into_inner()
        .grants;
    assert_eq!(grants.len(), 1, "{grants:?}");
    assert!(grants[0].granted_by.ends_with("(app gitops)"), "{grants:?}");

    // Editing the job directly is reverted.
    let mut edited = stored.clone();
    edited["tasks"][0]["count"] = json!(2);
    edited["version"] = Value::Null;
    fx.run(&edited, &fx.admin).await?;
    let outcome = reconciler.reconcile_app("gitops").await;
    assert!(
        matches!(&outcome, PassOutcome::Submitted { action, .. } if action.starts_with("reverted a direct edit by")),
        "{outcome:?}"
    );

    // Uninstall: the job stops, its grants are revoked, the record is gone.
    let reply = fx
        .svc
        .uninstall_app(req(
            UninstallAppRequest {
                name: "gitops".into(),
                idempotency_key: None,
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    assert!(reply.revoked_grants >= 1, "{reply:?}");
    assert!(fx.store.get_app("gitops")?.is_none());
    assert!(fx
        .version("legion-system/gitops")?
        .context("job")?
        .is_stopped());
    let live = fx
        .svc
        .list_grants(req(
            ListGrantsRequest {
                job: Some("legion-system/gitops".into()),
                include_revoked: false,
            },
            &fx.admin,
        ))
        .await?
        .into_inner()
        .grants;
    assert!(live.is_empty(), "{live:?}");
    assert_eq!(
        reconciler.reconcile_app("gitops").await,
        PassOutcome::Missing
    );
    // A bundled app cannot be uninstalled.
    reconciler.seed()?;
    let refused = fx
        .svc
        .uninstall_app(req(
            UninstallAppRequest {
                name: "autoscaler".into(),
                idempotency_key: None,
            },
            &fx.admin,
        ))
        .await
        .expect_err("bundled");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    Ok(())
}

/// A placement change re-renders the permit and the constraints.
#[tokio::test(flavor = "multi_thread")]
async fn a_placement_change_rerenders_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    let before = fx.stored(AUTOSCALER)?;
    assert!(before["tasks"][0]["constraints"]
        .to_string()
        .contains("legion.role"));
    fx.svc
        .update_app_settings(req(
            UpdateAppSettingsRequest {
                name: "autoscaler".into(),
                placement: Some("any".into()),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(
        matches!(&outcome, PassOutcome::Submitted { action, .. } if action == "settings changed (placement managers → any)"),
        "{outcome:?}"
    );
    let after = fx.stored(AUTOSCALER)?;
    assert!(
        after["tasks"][0]["affinities"]
            .to_string()
            .contains("legion.role"),
        "{after}"
    );
    assert!(
        !after["tasks"][0]["constraints"]
            .to_string()
            .contains("legion.role"),
        "{after}"
    );
    // `any` permits both pools, as `managers` does; `workers` permits
    // only the worker pool.
    assert_eq!(before["tasks"][0]["permit"], after["tasks"][0]["permit"]);
    fx.svc
        .update_app_settings(req(
            UpdateAppSettingsRequest {
                name: "autoscaler".into(),
                placement: Some("workers".into()),
                pool: Some("ops".into()),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    assert!(submitted(&reconciler.reconcile_app("autoscaler").await));
    let workers = fx.stored(AUTOSCALER)?;
    assert_eq!(
        workers["tasks"][0]["permit"],
        json!([{ "key": "pool", "value": "ops" }])
    );
    assert!(
        workers["tasks"][0]["constraints"]
            .to_string()
            .contains("is_not_set"),
        "{workers}"
    );
    Ok(())
}

/// A config change that touches a code field needs the app's whole grant;
/// `app:write` covers the rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_code_field_change_needs_the_grant_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    let operator = fx
        .token(
            "operator",
            json!([{ "resources": ["app"], "actions": ["read", "write"] }]),
        )
        .await?;
    // Not a code field: a builtin plugin's config.
    fx.set_config(
        "autoscaler",
        json!({ "plugins": { "prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" } } } }),
        &operator,
    )
    .await?;
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(
        matches!(&outcome, PassOutcome::Submitted { action, .. } if action == "config changed"),
        "{outcome:?}"
    );

    // A plugin source is code.
    let with_source = json!({ "plugins": {
        "prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" } },
        "aws": {
            "source": "https://example.com/aws.tar.gz",
            "checksum": format!("sha256:{}", "0".repeat(64)),
        },
    } });
    let refused = fx
        .set_config("autoscaler", with_source.clone(), &operator)
        .await
        .expect_err("app:write is not the grant");
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(
        refused.message().contains("/plugins/aws/source"),
        "{}",
        refused.message()
    );
    // So is an `exec` target's command: a program the app would run.
    let refused = fx
        .set_config(
            "autoscaler",
            json!({ "plugins": {
                "prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" } },
                "run": { "builtin": "exec", "config": { "command": ["/bin/sh", "-c", "id"] } },
            } }),
            &operator,
        )
        .await
        .expect_err("an exec command is code");
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(
        refused.message().contains("/plugins/run/config/command"),
        "{}",
        refused.message()
    );
    // Someone holding the grant (and app:write) may.
    let mut holder_rules = editor_rules();
    holder_rules
        .as_array_mut()
        .context("rules")?
        .push(json!({ "resources": ["app"], "actions": ["read", "write"] }));
    let holder_with_apps = fx.token("holder", holder_rules).await?;
    fx.set_config("autoscaler", with_source, &holder_with_apps)
        .await
        .map_err(|status| anyhow!("{status}"))?;
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(submitted(&outcome), "{outcome:?}");
    // The plugin source is now an artifact of the app's job.
    let stored = fx.stored(AUTOSCALER)?;
    assert!(
        stored["tasks"][0]["artifacts"]
            .to_string()
            .contains("https://example.com/aws.tar.gz"),
        "{stored}"
    );
    let changed = fx.events("app.config_changed")?;
    assert_eq!(changed.len(), 2);
    Ok(())
}

/// Move one manager's build.
fn rebuild(fx: &Fixture, id: &str, version: &str, build: &str) -> R<()> {
    fx.store.insert_node(manager_node(id, version, build))?;
    Ok(())
}

/// The build marker: defined only when the eligible ready nodes agree; a
/// rolling upgrade of three managers rolls the app once, after the third;
/// the straggler is named; the constraint keeps it off older nodes; an
/// older node joining later does not move it back; `unknown` is left out; a
/// rebuild with a new `buildId` rolls it.
#[tokio::test(flavor = "multi_thread")]
async fn the_build_marker_rolls_once_per_agreed_build_test() -> R<()> {
    let (fx, mut reconciler) = seeded().await?;
    let constraint = |fx: &Fixture| -> R<String> {
        Ok(fx.stored(AUTOSCALER)?["tasks"][0]["constraints"].to_string())
    };
    assert!(constraint(&fx)?.contains(">=0.1.0"));
    let mut rolls = 0;
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        rebuild(&fx, id, "0.2.0", "b")?;
        let outcome = reconciler.reconcile_app("autoscaler").await;
        if submitted(&outcome) {
            rolls += 1;
        }
        if index < 2 {
            assert_eq!(outcome, PassOutcome::Unchanged, "after {id}");
            let app = fx.get_app("autoscaler").await?;
            let reconcile = app.reconcile.context("reconcile")?;
            assert_eq!(reconcile.build_marker, "0.1.0+a");
            assert!(
                reconcile.marker_reason.starts_with(&format!(
                    "upgrade waiting: {} of 3 eligible nodes on 0.2.0 (waiting for",
                    index + 1
                )),
                "{}",
                reconcile.marker_reason
            );
            assert!(
                reconcile.marker_reason.contains("m3"),
                "{}",
                reconcile.marker_reason
            );
        }
    }
    assert_eq!(rolls, 1, "one roll, after the last manager");
    assert!(constraint(&fx)?.contains(">=0.2.0"));
    assert_eq!(
        fx.app("autoscaler")?.state.marker,
        Some(marker("0.2.0", "b"))
    );
    assert_eq!(fx.version_number(AUTOSCALER)?, 2);

    // An older node joining later holds nothing back and moves nothing.
    fx.store.insert_node(manager_node("late", "0.1.0", "a"))?;
    assert_eq!(
        reconciler.reconcile_app("autoscaler").await,
        PassOutcome::Unchanged
    );
    assert_eq!(
        fx.app("autoscaler")?.state.marker,
        Some(marker("0.2.0", "b"))
    );
    // It is out of placement: the constraint does not match it.
    let late = fx.store.get_node("late")?;
    let constraints = legion::constraints::ConstraintSet::new(
        &fx.stored(AUTOSCALER)?["tasks"][0]["constraints"],
    )?;
    assert!(!constraints.is_satisfied(&late, None));
    rebuild(&fx, "late", "0.2.0", "b")?;

    // A node reporting `unknown` (a worker from before versions) is left
    // out of the marker.
    fx.store.insert_node(manager_node("old", "unknown", ""))?;
    assert_eq!(
        reconciler.reconcile_app("autoscaler").await,
        PassOutcome::Unchanged
    );
    assert!(fx.app("autoscaler")?.state.marker_reason.is_empty());

    // A rebuild without a version bump rolls it: same version, new id.
    for id in ["m1", "m2", "m3", "late"] {
        rebuild(&fx, id, "0.2.0", "c")?;
    }
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(submitted(&outcome), "{outcome:?}");
    assert_eq!(
        fx.app("autoscaler")?.state.marker,
        Some(marker("0.2.0", "c"))
    );
    assert_eq!(fx.version_number(AUTOSCALER)?, 3);
    Ok(())
}

/// With no eligible ready node there is no marker yet, and the app waits
/// instead of submitting.
#[tokio::test(flavor = "multi_thread")]
async fn no_eligible_node_means_no_marker_test() -> R<()> {
    let fx = fixture()?;
    let mut reconciler = fx.reconciler(marker("0.1.0", "a"));
    reconciler.seed()?;
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(
        matches!(&outcome, PassOutcome::Waiting(reason) if reason.contains("waiting for an eligible node")),
        "{outcome:?}"
    );
    assert!(fx.version(AUTOSCALER)?.is_none());
    let app = fx.get_app("autoscaler").await?;
    assert_eq!(app.status, "pending");
    assert!(
        app.status_detail.contains("eligible node"),
        "{}",
        app.status_detail
    );
    assert!(
        app.notes
            .iter()
            .any(|note| note.contains("no manager runs a system worker")),
        "{:?}",
        app.notes
    );
    fx.store.insert_node(manager_node("m1", "0.1.0", "a"))?;
    assert!(submitted(&reconciler.reconcile_app("autoscaler").await));
    Ok(())
}

/// A new bundled manifest is adopted only when the leader runs the
/// marker's build; its permissions are then recorded as granted by
/// `legion:bootstrap`, and the upgrade is reported.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_bundled_manifest_is_adopted_with_the_leaders_build_test() -> R<()> {
    let fx = fixture()?;
    for id in ["m1", "m2"] {
        fx.store.insert_node(manager_node(id, "0.1.0", "a"))?;
    }
    // The record an older build seeded: the same app with fewer
    // permissions (none; so another manifest digest).
    let current = legion::apps::bundled_manifest("autoscaler").context("bundled")?;
    assert!(!current.permissions.is_empty());
    let mut older = current.clone();
    older.permissions.clear();
    let mut record = legion::manager::apps::seed_record(&older, None);
    record.manifest_digest = older.digest();
    assert!(fx.store.seed_app(record)?);

    // This leader runs build b; the nodes still run a: no adoption.
    let mut reconciler = fx.reconciler(marker("0.2.0", "b"));
    assert!(submitted(&reconciler.reconcile_app("autoscaler").await));
    let record = fx.app("autoscaler")?;
    assert_eq!(record.manifest_digest, older.digest());
    assert!(record.grant.rules.is_empty());

    // The nodes move to b, the leader's build: adopted, one roll.
    for id in ["m1", "m2"] {
        rebuild(&fx, id, "0.2.0", "b")?;
    }
    let outcome = reconciler.reconcile_app("autoscaler").await;
    assert!(submitted(&outcome), "{outcome:?}");
    let record = fx.app("autoscaler")?;
    assert_eq!(record.manifest_digest, current.digest());
    assert_eq!(record.grant.rules, current.permissions);
    assert_eq!(record.grant.granted_by, BOOTSTRAP_PRINCIPAL);
    let upgraded = fx.events("app.upgraded")?;
    assert_eq!(upgraded.len(), 1);
    assert!(
        upgraded[0].message.contains(&format!(
            "{} permission(s) added",
            current.permissions.len()
        )),
        "{}",
        upgraded[0].message
    );
    let stored = fx.stored(AUTOSCALER)?;
    assert_eq!(
        stored["tasks"][0]["identities"][0]["permissions"],
        serde_json::to_value(&current.permissions)?
    );
    assert_eq!(fx.version_number(AUTOSCALER)?, 2);
    Ok(())
}

/// `app:read` scoped by name filters the list; anonymous is refused.
#[tokio::test(flavor = "multi_thread")]
async fn app_reads_filter_by_name_test() -> R<()> {
    let fx = fixture()?;
    fx.reconciler(marker("0.1.0", "a")).seed()?;
    install(
        &fx,
        &json!({ "name": "other", "version": "1",
                 "job": { "task": { "name": "main", "runtime": "noop", "config": {} } } }),
        &fx.admin,
    )
    .await?;
    let reader = fx
        .token(
            "reader",
            json!([{ "resources": ["app"], "actions": ["read"], "names": ["auto*"] }]),
        )
        .await?;
    let apps = fx
        .svc
        .list_apps(req(ListAppsRequest {}, &reader))
        .await?
        .into_inner()
        .apps;
    let names: Vec<&str> = apps.iter().map(|app| app.name.as_str()).collect();
    assert_eq!(names, vec!["autoscaler"]);
    let denied = fx
        .svc
        .get_app(req(
            GetAppRequest {
                name: "other".into(),
            },
            &reader,
        ))
        .await
        .expect_err("not readable");
    assert_eq!(denied.code(), Code::PermissionDenied);
    Ok(())
}

/// Scenario A (apps plan §3 and §12 A3), with the real binary: a fresh
/// single manager with no workers seeds the autoscaler, and `legion app
/// list` shows it running on the manager's system worker, from the node's
/// own binary (`legion:self`). The worker reports its build id, the app's
/// job carries the build marker, and `job info` says the job is managed.
///
/// An exec task in `legion-system` runs as `nobody`, which takes a worker
/// that can switch users: run unprivileged, the test checks the placement
/// instead of the process.
#[cfg(unix)]
#[test]
fn fresh_cluster_runs_the_autoscaler_on_the_system_worker_test() {
    let _serial = crate::system_worker::SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // The autoscaler binds a port from the dynamic range.
    let _ports = crate::support::DYNAMIC_PORT_GATE.blocking_lock();
    let cluster = crate::system_worker::Cluster::start_with_autoscaler(false);
    let node = cluster.first_node_id().expect("start waited for the node");
    // SAFETY: geteuid has no preconditions.
    let privileged = unsafe { libc::geteuid() } == 0;
    let autoscaler = |list: &Value| -> Option<Value> {
        list["data"]["apps"]
            .as_array()?
            .iter()
            .find(|app| app["name"] == "autoscaler")
            .cloned()
    };
    cluster.wait_for("the autoscaler to run on the system worker", || {
        let list = cluster.cli_json(&["app", "list"]);
        let Some(app) = autoscaler(&list) else {
            return false;
        };
        if privileged {
            app["status"] == "running"
                && app["runningOn"]
                    .as_array()
                    .is_some_and(|nodes| nodes.iter().any(|id| id == node.as_str()))
        } else {
            cluster.usage(&node)["allocations"]
                .as_array()
                .is_some_and(|allocations| {
                    allocations
                        .iter()
                        .any(|allocation| allocation["jobId"] == AUTOSCALER)
                })
        }
    });

    let list = cluster.cli(&["app", "list"]);
    let table = String::from_utf8_lossy(&list.stdout);
    assert!(table.contains("autoscaler"), "{table}");
    assert!(table.contains("bundled"), "{table}");

    let status = cluster.cli_json(&["app", "status", "autoscaler"]);
    assert_eq!(status["ok"], true, "{status}");
    let app = &status["data"];
    assert_eq!(app["grant"]["grantedBy"], BOOTSTRAP_PRINCIPAL, "{app}");
    assert_eq!(app["source"], "bundled", "{app}");
    let marker = app["reconcile"]["buildMarker"].as_str().unwrap_or_default();
    assert!(
        marker.starts_with(&format!("{}+", env!("CARGO_PKG_VERSION"))),
        "{app}"
    );
    let node_info = cluster.cli_json(&["node", "info", &node]);
    let build_id = node_info["data"]["dynamicAttrs"]["buildId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert_eq!(build_id.len(), 32, "{node_info}");
    assert!(marker.ends_with(&build_id), "{marker} vs {build_id}");

    let info = cluster.cli(&["job", "info", AUTOSCALER]);
    let text = String::from_utf8_lossy(&info.stdout);
    assert!(
        text.contains("managed by app autoscaler"),
        "{text} / {}",
        String::from_utf8_lossy(&info.stderr)
    );
}
