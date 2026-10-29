//! Jobspec extensions (apps plan §6.6 and §9 A6; docs/apps.md, "Jobspec
//! extensions"): apps declare keys at the job, group, task, volume, port,
//! and service points, and jobs set them like any other field.
//!
//! In process, against a manager service with ACLs and workload identity
//! on, and a fake app built on the app framework (`legion::apps::ui`) that
//! answers the validate call, reached through a forwarder that delivers the
//! way a worker does:
//!
//! - every point resolves its key, and the qualified `apps.<app>` spelling
//!   works; values are stored as written and read back with defaults;
//! - schema-invalid values fail with their full path, even with
//!   `--allow-unknown-fields`; did-you-mean suggests installed keys; a group
//!   key on two tasks of one group is refused;
//! - install refuses a core field, another app's key (task and group as
//!   one point), a remote `$ref` (without any network request), and an
//!   example that fails its schema; `--key` renames, and values follow the
//!   app;
//! - a simulated new core field: the qualified-spelling error for a job
//!   whose previous version used the short key, the core field otherwise;
//! - `requires` is enforced as the submitter, on new or changed values;
//! - the validate call: an error rejects, a warning passes, an unreachable
//!   or slow app accepts with a warning;
//! - a resubmit that changes only extension values moves allocations in
//!   place: the same allocation ids, no rollout;
//! - `WatchAppExtensions` returns only the caller's own values, with their
//!   point and path, and refuses other identities;
//! - `GetJobSchema` has each installed key at its point;
//! - install adopts matching pending values; an upgrade re-validates and
//!   moves defaults; uninstall needs `--force` and leaves values orphaned.

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::apps::ui::{
    AssertionVerifier, ClusterKeys, StaticKeys, UiRegistry, UiServer, ValidateRequest,
};
use legion::manager::acl::Authenticator;
use legion::manager::app_forward::AppForwarder;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::extensions::ValidateResult;
use legion::structs::manager_server::Manager;
use legion::structs::spec_fields::ExtensionPoint;
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
const MGR_NODE: &str = "mgr-1";
const NODE: &str = "node-1";
const APP: &str = "ext";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties,
    })
}

/// The `ext` app: a key at every point (a group key, since an app declares
/// one of `task` and `group`). `uptime` asks the app at submit; `dns`
/// requires `net:write`.
fn ext_manifest() -> Value {
    json!({
        "name": APP,
        "version": "1.0.0",
        "description": "A fake app with a key at every point.",
        "job": { "task": { "name": "main", "runtime": "exec",
                            "config": { "command": ["sleep", "3600"] },
                            "network": { "ports": [{ "name": "api" }] } } },
        "extensions": [
            { "point": "job", "key": "uptime", "validate": true,
              "description": "Watch this job's URL.",
              "schema": object_schema(json!({
                  "url": { "type": "string" },
                  "alert": { "type": "string", "default": "#oncall" },
              }), &["url"]),
              "examples": [{ "url": "https://orders.acme.dev/healthz" }] },
            { "point": "group", "key": "tier",
              "schema": object_schema(json!({
                  "level": { "type": "string", "enum": ["gold", "standard"], "default": "standard" },
              }), &[]) },
            { "point": "volume", "key": "backup",
              "description": "Back up this volume on a schedule.",
              "schema": object_schema(json!({
                  "schedule": { "type": "string" },
                  "keep": { "type": "integer", "minimum": 1, "default": 7 },
              }), &["schedule"]),
              "examples": [{ "schedule": "0 3 * * *", "keep": 14 }] },
            { "point": "port", "key": "dns",
              "requires": { "resource": "net", "action": "write" },
              "schema": object_schema(json!({ "hostname": { "type": "string" } }), &["hostname"]) },
            { "point": "service", "key": "probe",
              "schema": object_schema(json!({
                  "path": { "type": "string", "default": "/healthz" },
              }), &[]) },
        ],
    })
}

/// A minimal app declaring `extensions`, never run.
fn small_manifest(name: &str, version: &str, extensions: Value) -> Value {
    json!({
        "name": name,
        "version": version,
        "job": { "task": { "name": "main", "runtime": "exec",
                            "config": { "command": ["sleep", "3600"] } } },
        "extensions": extensions,
    })
}

/// How the forwarder answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    /// To the fake app.
    App,
    /// The allocation cannot be reached.
    Unreachable,
    /// Longer than the validate budget.
    Slow,
}

/// Delivers what the manager forwards to the fake app's port, the way a
/// worker does (its request checks, then one HTTP/1.1 exchange).
#[derive(Debug)]
struct LoopbackForwarder {
    addr: std::net::SocketAddr,
    calls: AtomicU64,
    delivery: std::sync::Mutex<Delivery>,
}

#[tonic::async_trait]
impl AppForwarder for LoopbackForwarder {
    async fn forward(
        &self,
        _store: &Store,
        node_id: &str,
        request: ForwardAppRequestRequest,
    ) -> Result<ForwardAppRequestReply, tonic::Status> {
        assert_eq!(node_id, MGR_NODE);
        legion::worker::app_forward::check_request(&request)?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let delivery = *self.delivery.lock().unwrap();
        match delivery {
            Delivery::Unreachable => {
                return Err(tonic::Status::unavailable("connection refused"));
            }
            Delivery::Slow => tokio::time::sleep(Duration::from_secs(4)).await,
            Delivery::App => {}
        }
        let stream = tokio::net::TcpStream::connect(self.addr)
            .await
            .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
        legion::worker::app_forward::exchange(stream, &request).await
    }
}

/// The fake app's UI port: its validate call refuses an `uptime` URL with
/// `bad` in it and warns about one with `slow` in it.
async fn fake_app(state: &WorkloadIdentityState) -> R<std::net::SocketAddr> {
    let registry = Arc::new(UiRegistry::default());
    registry.validate(|request: ValidateRequest| async move {
        Ok(request
            .call
            .values
            .iter()
            .map(|value| {
                let url = value.value["url"].as_str().unwrap_or_default();
                let mut result = ValidateResult::default();
                if url.contains("bad") {
                    result.errors.push(format!(
                        "{url} does not resolve (asked by {})",
                        request.viewer.principal
                    ));
                }
                if url.contains("slow") {
                    result.warnings.push(format!("{url} answers slowly"));
                }
                result
            })
            .collect())
    });
    let keys = ClusterKeys {
        issuer: state.issuer_url()?,
        jwks: state.jwks()?,
    };
    let verifier = AssertionVerifier::new(APP, Arc::new(StaticKeys(keys)));
    let server = Arc::new(UiServer::new(registry, Some(Arc::new(verifier))));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(legion::apps::ui::serve(listener, server));
    Ok(addr)
}

struct Fixture {
    svc: Arc<ManagerDaemonService>,
    auth: Arc<Authenticator>,
    store: Store,
    state: Arc<WorkloadIdentityState>,
    admin: String,
    forwarder: Arc<LoopbackForwarder>,
}

fn node(id: &str, pool: &str, manager: bool) -> NodeMsg {
    let mut config = json!({
        "node_id": id,
        "maxAllocations": 20,
        "resources": { "memory": 65536, "cpu": 40000 },
    });
    if manager {
        config["legion.role"] = json!("manager");
        config["namespaces"] = json!(["legion-system"]);
    }
    NodeMsg::from(Node {
        id: id.into(),
        pool: pool.into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: config,
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
        }),
        dynamic_attrs: json!({ "capabilities": [] }),
        health_checks: json!({}),
    })
}

impl Fixture {
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

    async fn install(
        &self,
        manifest: &Value,
        keys: &[&str],
    ) -> Result<InstallAppReply, tonic::Status> {
        self.svc
            .install_app(req(
                InstallAppRequest {
                    manifest_json: manifest.to_string(),
                    source: "file".into(),
                    source_ref: "./app.json".into(),
                    keys: keys.iter().map(|key| key.to_string()).collect(),
                    ..Default::default()
                },
                &self.admin,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn run_as(
        &self,
        spec: &Value,
        allow_unknown_fields: bool,
        token: &str,
    ) -> Result<RunJobReply, tonic::Status> {
        self.svc
            .run_job(req(
                RunJobRequest {
                    parsed_jobspec: spec.to_string(),
                    allow_unknown_fields,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn run(&self, spec: &Value) -> Result<RunJobReply, tonic::Status> {
        self.run_as(spec, false, &self.admin).await
    }

    async fn info(&self, job: &str) -> R<GetJobInfoReply> {
        Ok(self
            .svc
            .get_job_info(req(GetJobInfoRequest { name: job.into() }, &self.admin))
            .await?
            .into_inner())
    }

    /// Run every queued eval.
    fn drain_evals(&self) -> R<()> {
        while let Some(eval) = self.store.get_next_eval()? {
            let outcome = eval.handle(&self.store);
            self.store.set_eval_status(
                &eval.id,
                if outcome.is_ok() {
                    "complete"
                } else {
                    "failed"
                },
            )?;
        }
        Ok(())
    }

    /// The `legion` token the manager would issue the app's allocation.
    fn app_token(&self, app: &str) -> R<String> {
        let job = format!("legion-system/{app}");
        let allocation = self
            .store
            .allocs_for_job(&job)?
            .into_iter()
            .next()
            .with_context(|| format!("{job} should be placed"))?;
        let task = self
            .store
            .get_task_configs_for_job_config(&allocation.job_config_id)?
            .into_iter()
            .find(|(id, _)| *id == allocation.task_config_id)
            .map(|(_, task)| task)
            .context("task config")?;
        let claims = claims_for_allocation(&self.store, &allocation, &task, "auto")?;
        let identity = WorkloadIdentity {
            name: CONTROL_PLANE_IDENTITY_NAME.into(),
            ..Default::default()
        };
        Ok(self
            .state
            .issue(&claims, &identity, std::time::SystemTime::now())?
            .token)
    }

    fn set_delivery(&self, delivery: Delivery) {
        *self.forwarder.delivery.lock().unwrap() = delivery;
    }

    /// Reconcile `app` and place its job on the manager node.
    async fn start_app(&self, app: &str) -> R<()> {
        let mut reconciler = AppReconciler::new(
            self.svc.clone(),
            ReconcilerSettings {
                seed: BTreeMap::new(),
                own_build: BuildMarker {
                    version: "0.1.0".into(),
                    build_id: "a".into(),
                },
            },
        );
        let outcome = reconciler.reconcile_app(app).await;
        assert!(
            matches!(
                outcome,
                legion::manager::apps::PassOutcome::Submitted { .. }
            ),
            "{outcome:?}"
        );
        let job = format!("legion-system/{app}");
        let config_id = self.store.get_job_config(&job)?.id();
        self.store
            .enqueue_eval("job_reschedule", Some(&job), Some(&config_id), None, None)?;
        self.drain_evals()?;
        anyhow::ensure!(
            !self.store.app_ui_allocations(&job)?.is_empty(),
            "{job} should be placed"
        );
        Ok(())
    }
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-app-extensions-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    })?;
    store.ensure_system_namespace()?;
    store.insert_worker_type(&WorkerConfig {
        name: "legion-system".into(),
        pool: "legion-system".into(),
        namespaces: vec!["legion-system".into()],
        ..Default::default()
    })?;
    store.insert_worker_type(&WorkerConfig {
        name: "default".into(),
        pool: "default".into(),
        ..Default::default()
    })?;
    store.insert_node(node(MGR_NODE, "legion-system", true))?;
    store.insert_node(node(NODE, "default", false))?;
    let state = Arc::new(WorkloadIdentityState::initialize(
        &store,
        &WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some(ISSUER.into()),
            ..Default::default()
        },
        ISSUER,
        std::time::SystemTime::now(),
    )?);
    let mut auth = Authenticator::from_config(
        &AclConfig {
            enabled: Some(true),
            ..Default::default()
        }
        .with_defaults(),
    )?;
    auth.register_method(Box::new(ControlPlaneIdentityMethod::new(
        state.clone(),
        Vec::new(),
        Duration::from_secs(3600),
        store.clone(),
    )))?;
    let auth = Arc::new(auth);
    let (_, admin) = auth
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .context("a fresh store has no management token")?;
    let addr = fake_app(&state).await?;
    let forwarder = Arc::new(LoopbackForwarder {
        addr,
        calls: AtomicU64::new(0),
        delivery: std::sync::Mutex::new(Delivery::App),
    });
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth.clone())
            .with_workload_identity(Some(state.clone()))
            .with_app_forwarder(forwarder.clone()),
    );
    let fx = Fixture {
        svc,
        auth,
        store,
        state,
        admin,
        forwarder,
    };
    fx.install(&ext_manifest(), &[]).await?;
    fx.start_app(APP).await?;
    Ok(fx)
}

/// A job setting `ext`'s key at every point.
fn orders() -> Value {
    json!({
        "name": "orders-db",
        "uptime": { "url": "https://orders.acme.dev/healthz" },
        "tasks": [{
            "name": "postgres",
            "group": "db",
            "runtime": "exec",
            "config": { "command": ["sleep", "3600"] },
            "resources": { "cpu": 100, "memory": 64 },
            "tier": { "level": "gold" },
            "volumes": [
                { "name": "data", "backup": { "schedule": "0 3 * * *" } },
                { "name": "logs", "apps": { "ext": { "schedule": "@hourly", "keep": 2 } } },
            ],
            "network": { "ports": [{ "name": "http", "dns": { "hostname": "orders.acme.dev" } }] },
            "service": { "port": "http", "probe": {} },
        }],
    })
}

fn find<'a>(values: &'a [JobExtensionValue], point: &str, key: &str) -> R<&'a JobExtensionValue> {
    values
        .iter()
        .find(|value| value.point == point && value.key == key)
        .with_context(|| format!("no {key} on {point} in {values:#?}"))
}

fn parsed(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_point_resolves_and_reads_back_with_defaults_test() -> R<()> {
    let fx = fixture().await?;
    let reply = fx.run(&orders()).await?;
    assert!(reply.warnings.is_empty(), "{:?}", reply.warnings);

    let info = fx.info("orders-db").await?;
    let values = &info.extensions;
    assert_eq!(values.len(), 6, "{values:#?}");
    for value in values {
        assert_eq!(value.app, APP);
        assert_eq!(value.state, "valid", "{value:?}");
        assert!(value.validated_by.starts_with("ext@"), "{value:?}");
    }

    let uptime = find(values, "job", "uptime")?;
    assert_eq!(uptime.path, Some(ExtensionPath::default()));
    assert_eq!(
        parsed(&uptime.effective_json),
        json!({ "url": "https://orders.acme.dev/healthz", "alert": "#oncall" })
    );

    let tier = find(values, "group", "tier")?;
    let path = tier.path.clone().unwrap_or_default();
    assert_eq!(
        (path.group.as_str(), path.task.as_str()),
        ("db", "postgres")
    );

    let backup = find(values, "volume", "backup")?;
    assert_eq!(backup.path.clone().unwrap_or_default().volume, "data");
    assert_eq!(
        parsed(&backup.value_json),
        json!({ "schedule": "0 3 * * *" })
    );
    assert_eq!(
        parsed(&backup.effective_json),
        json!({ "schedule": "0 3 * * *", "keep": 7 })
    );

    // The qualified spelling, on the second volume.
    let qualified = find(values, "volume", "apps.ext")?;
    assert_eq!(qualified.path.clone().unwrap_or_default().volume, "logs");
    assert_eq!(
        parsed(&qualified.effective_json),
        json!({ "schedule": "@hourly", "keep": 2 })
    );

    let dns = find(values, "port", "dns")?;
    assert_eq!(dns.path.clone().unwrap_or_default().port, "http");
    let probe = find(values, "service", "probe")?;
    assert_eq!(probe.path.clone().unwrap_or_default().service, "postgres");
    assert_eq!(parsed(&probe.effective_json), json!({ "path": "/healthz" }));

    // Stored as written, by app: no defaults in the version.
    let stored = fx.store.get_job_config("orders-db")?;
    let data = stored
        .extensions
        .iter()
        .find(|value| value.path.volume == "data")
        .context("stored backup value")?;
    assert_eq!(data.app, APP);
    assert_eq!(data.value, json!({ "schedule": "0 3 * * *" }));
    // And not inline in any block.
    assert!(stored.tasks[0].volumes[0].pending_extensions.is_empty());

    // An unchanged resubmit is unchanged.
    let again = fx.run(&orders()).await?;
    assert!(again.message.contains("unchanged"), "{}", again.message);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_values_and_unknown_keys_are_refused_test() -> R<()> {
    let fx = fixture().await?;
    let with = |edit: &dyn Fn(&mut Value)| {
        let mut spec = orders();
        edit(&mut spec);
        spec
    };

    // A value that fails its schema fails, with its full path, with or
    // without the flag.
    let bad_keep = with(&|spec| {
        spec["tasks"][0]["volumes"][0]["backup"]["keep"] = json!(0);
    });
    for allow in [false, true] {
        let status = fx
            .run_as(&bad_keep, allow, &fx.admin)
            .await
            .expect_err("schema-invalid");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(
            status.message().contains(
                "orders-db: task postgres: volume data: backup.keep: 0 is less than the minimum \
                 of 1"
            ),
            "{}",
            status.message()
        );
        assert!(
            !status.message().contains("--allow-unknown-fields"),
            "an invalid value's hint does not offer the flag: {}",
            status.message()
        );
    }

    // A typo of an installed key suggests it.
    let typo = with(&|spec| {
        let volume = spec["tasks"][0]["volumes"][0].as_object_mut().unwrap();
        let value = volume.remove("backup").unwrap();
        volume.insert("bakup".into(), value);
    });
    let status = fx.run(&typo).await.expect_err("typo");
    assert!(
        status.message().contains(
            "volume data: unknown field `bakup` (did you mean `backup`, the ext app's key?)"
        ),
        "{}",
        status.message()
    );

    // A key no app declares says both checks came up empty.
    let unknown = with(&|spec| {
        spec["tasks"][0]["volumes"][0]["snapshot"] = json!({ "every": "1h" });
    });
    let status = fx.run(&unknown).await.expect_err("unknown");
    assert!(
        status.message().contains(
            "unknown field `snapshot`: not a volume field, and no installed app declares a \
             `snapshot` key on volumes"
        ),
        "{}",
        status.message()
    );
    assert!(status.message().contains("legion app extensions"));

    // A group key on two tasks of one group.
    let twice = with(&|spec| {
        let mut second = spec["tasks"][0].clone();
        second["name"] = json!("replica");
        second.as_object_mut().unwrap().remove("service");
        second.as_object_mut().unwrap().remove("network");
        spec["tasks"].as_array_mut().unwrap().push(second);
    });
    let status = fx.run(&twice).await.expect_err("group key twice");
    assert!(
        status
            .message()
            .contains("group db: the ext app's group key is set on tasks postgres and replica"),
        "{}",
        status.message()
    );

    // The short key and the qualified spelling on one block.
    let both = with(&|spec| {
        spec["tasks"][0]["volumes"][0]["apps"] = json!({ "ext": { "schedule": "@daily" } });
    });
    let status = fx.run(&both).await.expect_err("both spellings");
    assert!(
        status.message().contains("both set the ext app's key"),
        "{}",
        status.message()
    );

    // `apps.<app>` for an app that is not installed.
    let missing = with(&|spec| {
        spec["tasks"][0]["volumes"][0]["apps"] = json!({ "nosuch": {} });
    });
    let status = fx.run(&missing).await.expect_err("no such app");
    assert!(
        status
            .message()
            .contains("unknown field `apps.nosuch`: no app named nosuch is installed"),
        "{}",
        status.message()
    );

    // Resolved values are Legion's to write.
    let forged = with(&|spec| {
        spec["extensions"] = json!([{ "app": "ext", "point": "job", "key": "uptime",
                                      "value": { "url": "x" } }]);
    });
    let status = fx.run(&forged).await.expect_err("hand-written extensions");
    assert!(
        status.message().contains("is not written by hand"),
        "{}",
        status.message()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn install_refuses_collisions_and_renames_with_key_test() -> R<()> {
    let fx = fixture().await?;
    let volume_backup = json!([{ "point": "volume", "key": "backup",
        "schema": object_schema(json!({ "every": { "type": "string" } }), &[]) }]);

    // Another app's key at the same point, with the `--key` hint.
    let status = fx
        .install(
            &small_manifest("other", "0.1.0", volume_backup.clone()),
            &[],
        )
        .await
        .expect_err("collides with ext's backup");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status
            .message()
            .contains("other declares `backup` on volumes, which the ext app already owns"),
        "{}",
        status.message()
    );
    assert!(status.message().contains("--key volume.backup=other"));

    // Installed under another key, values follow the app, not the key.
    fx.install(
        &small_manifest("other", "0.1.0", volume_backup),
        &["volume.backup=pgbackup"],
    )
    .await?;
    let spec = json!({
        "name": "cache",
        "task": { "name": "redis", "runtime": "exec", "config": { "command": ["true"] },
                  "volumes": [
                      { "name": "a", "pgbackup": { "every": "1h" } },
                      { "name": "b", "apps": { "other": { "every": "2h" } } },
                      { "name": "c", "backup": { "schedule": "@daily" } },
                  ] },
    });
    fx.run(&spec).await?;
    let stored = fx.store.get_job_config("cache")?.extensions;
    let owners: Vec<(&str, &str, &str)> = stored
        .iter()
        .map(|value| {
            (
                value.path.volume.as_str(),
                value.app.as_str(),
                value.key.as_str(),
            )
        })
        .collect();
    assert_eq!(
        owners,
        vec![
            ("c", "ext", "backup"),
            ("a", "other", "pgbackup"),
            ("b", "other", "apps.other")
        ]
    );

    // `task` and `group` count as one point.
    fx.install(
        &small_manifest(
            "teams",
            "0.1.0",
            json!([{ "point": "task", "key": "team", "schema": { "type": "object" } }]),
        ),
        &[],
    )
    .await?;
    let status = fx
        .install(
            &small_manifest(
                "groups",
                "0.1.0",
                json!([{ "point": "group", "key": "team", "schema": { "type": "object" } }]),
            ),
            &[],
        )
        .await
        .expect_err("a group key over a task key");
    assert!(
        status
            .message()
            .contains("which the teams app already owns"),
        "{}",
        status.message()
    );

    // A core field at its point.
    let status = fx
        .install(
            &small_manifest(
                "ro",
                "0.1.0",
                json!([{ "point": "volume", "key": "readOnly", "schema": { "type": "object" } }]),
            ),
            &[],
        )
        .await
        .expect_err("a core field");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("is a core volume field"),
        "{}",
        status.message()
    );

    // A remote `$ref`, refused without any network request.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();
    let status = fx
        .install(
            &small_manifest(
                "remote",
                "0.1.0",
                json!([{ "point": "job", "key": "remote", "schema": {
                    "type": "object",
                    "properties": { "x": { "$ref": format!("http://127.0.0.1:{port}/schema.json") } },
                } }]),
            ),
            &[],
        )
        .await
        .expect_err("a remote ref");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("is not local"),
        "{}",
        status.message()
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        listener.accept().is_err(),
        "the install made a network request"
    );

    // An example that fails its own schema.
    let status = fx
        .install(
            &small_manifest(
                "drift",
                "0.1.0",
                json!([{ "point": "job", "key": "drift",
                         "schema": object_schema(json!({ "n": { "type": "integer" } }), &["n"]),
                         "examples": [{ "n": "three" }] }]),
            ),
            &[],
        )
        .await
        .expect_err("a failing example");
    assert!(
        status.message().contains("examples[0] does not match"),
        "{}",
        status.message()
    );

    // `legion job schema`: each installed key at its point.
    let reply = fx
        .svc
        .get_job_schema(req(GetJobSchemaRequest {}, &fx.admin))
        .await?
        .into_inner();
    let schema = parsed(&reply.schema_json);
    let task = &schema["$defs"]["task"]["properties"];
    for (at, key) in [
        (&schema["properties"], "uptime"),
        (task, "tier"),
        (task, "team"),
        (&task["volumes"]["items"]["properties"], "backup"),
        (&task["volumes"]["items"]["properties"], "pgbackup"),
        (
            &task["network"]["properties"]["ports"]["items"]["properties"],
            "dns",
        ),
        (&task["service"]["properties"], "probe"),
    ] {
        assert!(at.get(key).is_some(), "{key} missing from the schema");
    }
    assert!(schema["properties"]["uptime"]["description"]
        .as_str()
        .unwrap_or_default()
        .starts_with("From the ext app, 1.0.0"));
    assert!(task["volumes"]["items"]["properties"]["apps"]["properties"]["other"].is_object());
    assert_eq!(
        reply.extensions.iter().filter(|key| key.app == APP).count(),
        5
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_simulated_new_core_field_shadows_a_key_test() -> R<()> {
    let fx = fixture().await?;
    let spec = |name: &str, volume: Value| {
        json!({
            "name": name,
            "task": { "name": "main", "runtime": "exec", "config": { "command": ["true"] },
                      "volumes": [volume] },
        })
    };
    // Before the release: `backup` is the ext app's key.
    fx.run(&spec(
        "old",
        json!({ "name": "data", "backup": { "schedule": "@daily" } }),
    ))
    .await?;

    // A release that adds a core `backup` field on volumes.
    let released = ManagerDaemonService::with_acl(fx.store.clone(), fx.auth.clone())
        .with_workload_identity(Some(fx.state.clone()))
        .with_simulated_core_field(ExtensionPoint::Volume, "backup");

    // The job whose previous version used the short key is pointed at the
    // qualified spelling.
    let status = released
        .run_job(req(
            RunJobRequest {
                parsed_jobspec: spec(
                    "old",
                    json!({ "name": "data", "backup": { "schedule": "@hourly" } }),
                )
                .to_string(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await
        .expect_err("short key after the release");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("`backup` is now a core volume field")
            && status.message().contains("apps.ext"),
        "{}",
        status.message()
    );

    // The qualified spelling keeps the app's value.
    released
        .run_job(req(
            RunJobRequest {
                parsed_jobspec: spec(
                    "old",
                    json!({ "name": "data", "apps": { "ext": { "schedule": "@hourly" } } }),
                )
                .to_string(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let stored = fx.store.get_job_config("old")?.extensions;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        (stored[0].app.as_str(), stored[0].key.as_str()),
        (APP, "apps.ext")
    );

    // A new job using the name gets the core field: no app value.
    released
        .run_job(req(
            RunJobRequest {
                parsed_jobspec: spec(
                    "fresh",
                    json!({ "name": "data", "backup": { "schedule": "@daily" } }),
                )
                .to_string(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    assert!(fx.store.get_job_config("fresh")?.extensions.is_empty());

    // `legion app status` names the conflict.
    let app = released
        .get_app(req(GetAppRequest { name: APP.into() }, &fx.admin))
        .await?
        .into_inner()
        .app
        .context("app")?;
    assert!(
        app.notes
            .iter()
            .any(|note| note.contains("`backup` on volumes is now a core volume field")),
        "{:?}",
        app.notes
    );
    assert!(app
        .extension_keys
        .iter()
        .any(|key| key.key == "backup" && key.shadowed));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn requires_is_enforced_as_the_submitter_test() -> R<()> {
    let fx = fixture().await?;
    let deployer = fx
        .token(
            "deployer",
            json!([{ "resources": ["job"], "actions": ["read", "write"], "namespaces": ["default"], "host": true }]),
        )
        .await?;
    let netops = fx
        .token(
            "netops",
            json!([
                { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["default"], "host": true },
                { "resources": ["net"], "actions": ["write"] },
            ]),
        )
        .await?;

    let status = fx
        .run_as(&orders(), false, &deployer)
        .await
        .expect_err("dns needs net:write");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status
            .message()
            .contains("task postgres: port http: dns (ext app) requires net:write"),
        "{}",
        status.message()
    );

    fx.run_as(&orders(), false, &netops).await?;

    // The deployer may change the job without touching the value...
    let mut resized = orders();
    resized["tasks"][0]["resources"]["memory"] = json!(128);
    fx.run_as(&resized, false, &deployer).await?;

    // ...but not the value.
    let mut moved = resized.clone();
    moved["tasks"][0]["network"]["ports"][0]["dns"]["hostname"] = json!("evil.acme.dev");
    let status = fx
        .run_as(&moved, false, &deployer)
        .await
        .expect_err("a changed dns value needs net:write");
    assert_eq!(status.code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_validate_call_rejects_warns_and_fails_open_test() -> R<()> {
    let fx = fixture().await?;
    let with_url = |url: &str| {
        let mut spec = orders();
        spec["uptime"]["url"] = json!(url);
        spec
    };

    // An error from the app rejects, naming the place, the app, and the
    // submitter it was asked for.
    let status = fx
        .run(&with_url("https://bad.acme.dev"))
        .await
        .expect_err("the app refuses");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("orders-db: uptime (ext app): https://bad.acme.dev does not resolve"),
        "{}",
        status.message()
    );
    assert!(fx.store.get_job_config_optional("orders-db")?.is_none());

    // A warning passes through to the reply.
    let reply = fx.run(&with_url("https://slow.acme.dev")).await?;
    assert!(
        reply.warnings.iter().any(|warning| warning
            == "orders-db: uptime (ext app): https://slow.acme.dev answers slowly"),
        "{:?}",
        reply.warnings
    );

    // An unreachable app accepts, with a warning.
    fx.set_delivery(Delivery::Unreachable);
    let reply = fx.run(&with_url("https://bad.acme.dev/1")).await?;
    assert!(
        reply.warnings.iter().any(|warning| warning
            .contains("orders-db: uptime was not checked by the ext app")
            && warning.contains("the schema check passed")),
        "{:?}",
        reply.warnings
    );

    // So does one that does not answer within the budget.
    fx.set_delivery(Delivery::Slow);
    let started = std::time::Instant::now();
    let reply = fx.run(&with_url("https://bad.acme.dev/2")).await?;
    assert!(started.elapsed() < Duration::from_secs(4));
    assert!(
        reply
            .warnings
            .iter()
            .any(|warning| warning.contains("did not answer within 2s")),
        "{:?}",
        reply.warnings
    );

    // A job with no `validate` keys asks nobody.
    fx.set_delivery(Delivery::App);
    let before = fx.forwarder.calls.load(Ordering::SeqCst);
    fx.run(&json!({
        "name": "quiet",
        "task": { "name": "main", "runtime": "exec", "config": { "command": ["true"] },
                  "volumes": [{ "name": "data", "backup": { "schedule": "@daily" } }] },
    }))
    .await?;
    assert_eq!(fx.forwarder.calls.load(Ordering::SeqCst), before);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_extension_only_resubmit_moves_allocations_in_place_test() -> R<()> {
    let fx = fixture().await?;
    let spec = |keep: u64, pending: bool| {
        let mut spec = json!({
            "name": "cache",
            "task": { "name": "redis", "runtime": "exec", "count": 2,
                      "config": { "command": ["sleep", "3600"] },
                      "resources": { "cpu": 100, "memory": 64 },
                      "volumes": [{ "name": "data",
                                    "backup": { "schedule": "@daily", "keep": keep } }] },
        });
        if pending {
            spec["task"]["owner"] = json!({ "team": "cache" });
        }
        spec
    };
    fx.run(&spec(3, false)).await?;
    fx.drain_evals()?;
    let before: Vec<i64> = {
        let mut ids: Vec<i64> = fx
            .store
            .allocs_for_job("cache")?
            .iter()
            .map(|allocation| allocation.id)
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(before.len(), 2, "the job should be placed");

    let reply = fx.run(&spec(9, false)).await?;
    assert!(
        reply
            .message
            .contains("only jobspec extension values changed"),
        "{}",
        reply.message
    );
    fx.drain_evals()?;
    let config = fx.store.get_job_config("cache")?;
    assert_eq!(config.version, Some(2));
    let allocations = fx.store.allocs_for_job("cache")?;
    let mut after: Vec<i64> = allocations.iter().map(|allocation| allocation.id).collect();
    after.sort_unstable();
    assert_eq!(after, before, "the same allocations");
    assert!(allocations
        .iter()
        .all(|allocation| allocation.job_config_id == config.id()));
    assert!(fx.store.list_rollouts(Some("cache"), false)?.is_empty());

    // A pending key alone (kept under the flag) moves in place too.
    let reply = fx.run_as(&spec(9, true), true, &fx.admin).await?;
    assert!(reply.message.contains("in place"), "{}", reply.message);
    fx.drain_evals()?;
    let mut ids: Vec<i64> = fx
        .store
        .allocs_for_job("cache")?
        .iter()
        .map(|allocation| allocation.id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, before);
    assert!(fx.store.list_rollouts(Some("cache"), false)?.is_empty());

    // Anything else rolls out.
    let mut resized = spec(9, true);
    resized["task"]["resources"]["memory"] = json!(128);
    let reply = fx.run_as(&resized, true, &fx.admin).await?;
    assert!(!reply.message.contains("in place"), "{}", reply.message);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_app_extensions_streams_only_the_callers_values_test() -> R<()> {
    let fx = fixture().await?;
    fx.install(
        &small_manifest(
            "other",
            "0.1.0",
            json!([{ "point": "task", "key": "owner", "schema": { "type": "object" } }]),
        ),
        &[],
    )
    .await?;
    let mut spec = orders();
    spec["tasks"][0]["owner"] = json!({ "team": "payments" });
    fx.run(&spec).await?;

    let token = fx.app_token(APP)?;
    let mut stream = fx
        .svc
        .watch_app_extensions(req(WatchAppExtensionsRequest::default(), &token))
        .await?
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await?
        .context("a first message")??;
    assert!(first.snapshot);
    assert_eq!(first.upserts.len(), 6, "{:#?}", first.upserts);
    for value in &first.upserts {
        assert_eq!(value.job_id, "orders-db");
        assert_eq!(value.namespace, "default");
        assert_eq!(value.config_version, 1);
        assert_eq!(value.state, "valid");
    }
    let backup = first
        .upserts
        .iter()
        .find(|value| value.key == "backup")
        .context("backup")?;
    assert_eq!(backup.point, "volume");
    let path = backup.path.clone().unwrap_or_default();
    assert_eq!(
        (
            path.group.as_str(),
            path.task.as_str(),
            path.volume.as_str()
        ),
        ("db", "postgres", "data")
    );
    assert_eq!(
        parsed(&backup.effective_json),
        json!({ "schedule": "0 3 * * *", "keep": 7 })
    );
    // The other app's value is not the caller's.
    assert!(first.upserts.iter().all(|value| value.key != "owner"));

    // A change arrives as an upsert; a removal as a delete.
    let mut changed = spec.clone();
    changed["tasks"][0]["volumes"][0]["backup"]["keep"] = json!(30);
    changed["tasks"][0]["volumes"].as_array_mut().unwrap().pop();
    fx.run(&changed).await?;
    let next = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await?
        .context("an update")??;
    assert!(!next.snapshot);
    assert_eq!(next.generation, 2);
    let upserted: Vec<&str> = next
        .upserts
        .iter()
        .map(|value| value.key.as_str())
        .collect();
    assert!(upserted.contains(&"backup"), "{upserted:?}");
    assert_eq!(next.deletes.len(), 1, "{:?}", next.deletes);
    assert_eq!(next.deletes[0].point, "volume");
    assert_eq!(
        next.deletes[0].path.clone().unwrap_or_default().volume,
        "logs"
    );

    // Shards split the values by job.
    let mut sharded = fx
        .svc
        .watch_app_extensions(req(
            WatchAppExtensionsRequest {
                shards: vec![legion::structs::extensions::shard_of("orders-db", 4)],
                shard_count: 4,
                app: String::new(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), sharded.next())
        .await?
        .context("a first message")??;
    assert_eq!(first.upserts.len(), 5);
    let mut elsewhere = fx
        .svc
        .watch_app_extensions(req(
            WatchAppExtensionsRequest {
                shards: vec![(legion::structs::extensions::shard_of("orders-db", 4) + 1) % 4],
                shard_count: 4,
                app: String::new(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), elsewhere.next())
        .await?
        .context("a first message")??;
    assert!(first.upserts.is_empty());

    // Other identities are refused.
    let client = fx
        .token(
            "reader",
            json!([
                { "resources": ["job"], "actions": ["read"], "namespaces": ["*"] },
                { "resources": ["app"], "actions": ["read"] },
            ]),
        )
        .await?;
    let status = fx
        .svc
        .watch_app_extensions(req(WatchAppExtensionsRequest::default(), &client))
        .await
        .expect_err("a client token");
    assert_eq!(status.code(), Code::PermissionDenied);
    let status = fx
        .svc
        .watch_app_extensions(Request::new(WatchAppExtensionsRequest::default()))
        .await
        .expect_err("no credential");
    assert_eq!(status.code(), Code::Unauthenticated);
    let status = fx
        .svc
        .watch_app_extensions(req(
            WatchAppExtensionsRequest {
                app: "other".into(),
                ..Default::default()
            },
            &token,
        ))
        .await
        .expect_err("another app's values");
    assert_eq!(status.code(), Code::PermissionDenied);
    // A management token names the app.
    let mut managed = fx
        .svc
        .watch_app_extensions(req(
            WatchAppExtensionsRequest {
                app: "other".into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), managed.next())
        .await?
        .context("a first message")??;
    assert_eq!(first.upserts.len(), 1);
    assert_eq!(first.upserts[0].key, "owner");
    assert_eq!(first.upserts[0].point, "task");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn install_adopts_pending_upgrade_revalidates_and_uninstall_orphans_test() -> R<()> {
    let fx = fixture().await?;
    let snap = |keep_min: u64, keep_default: u64, version: &str| {
        small_manifest(
            "snaps",
            version,
            json!([{ "point": "volume", "key": "snap",
                     "schema": object_schema(json!({
                         "every": { "type": "string" },
                         "keep": { "type": "integer", "minimum": keep_min, "default": keep_default },
                     }), &["every"]) }]),
        )
    };
    let job = |name: &str, keep: Option<u64>| {
        let mut value = json!({ "every": "1h" });
        if let Some(keep) = keep {
            value["keep"] = json!(keep);
        }
        json!({
            "name": name,
            "task": { "name": "main", "runtime": "exec", "config": { "command": ["true"] },
                      "volumes": [{ "name": "data", "snap": value }] },
        })
    };

    // Before the app exists, the key is kept pending under the flag.
    fx.run_as(&job("early", None), true, &fx.admin).await?;
    let info = fx.info("early").await?;
    let pending = find(&info.extensions, "volume", "snap")?;
    assert_eq!(pending.state, "pending");
    assert!(pending.app.is_empty());

    // Installing the app adopts it, and the prompt says so.
    let dry = fx
        .svc
        .install_app(req(
            InstallAppRequest {
                manifest_json: snap(1, 3, "0.1.0").to_string(),
                source: "file".into(),
                dry_run: true,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    assert!(
        dry.prompt
            .iter()
            .any(|line| line.contains("Adopts 1 pending value")
                && line.contains("early/main/data: snap")),
        "{:?}",
        dry.prompt
    );
    fx.install(&snap(1, 3, "0.1.0"), &[]).await?;
    let info = fx.info("early").await?;
    let adopted = find(&info.extensions, "volume", "snap")?;
    assert_eq!(
        (
            adopted.app.as_str(),
            adopted.state.as_str(),
            adopted.adopted
        ),
        ("snaps", "valid", true)
    );
    assert_eq!(
        parsed(&adopted.effective_json),
        json!({ "every": "1h", "keep": 3 })
    );

    // A value stored by the app, and one that sets `keep`.
    fx.run(&job("later", Some(2))).await?;

    // An upgrade that changes the default reaches the values that did not
    // set it; one that tightens the schema flags those that fail it.
    let reply = fx
        .svc
        .upgrade_app(req(
            UpgradeAppRequest {
                name: "snaps".into(),
                manifest_json: snap(3, 5, "0.2.0").to_string(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    assert!(reply.upgraded, "{}", reply.message);
    assert!(
        reply
            .prompt
            .iter()
            .any(|line| line
                .starts_with("2 jobs set snap on volumes; 1 fails this version's schema")),
        "{:?}",
        reply.prompt
    );
    assert!(
        reply
            .prompt
            .iter()
            .any(|line| line.contains("later/main/data: snap") && line.contains("keep")),
        "{:?}",
        reply.prompt
    );
    // Adoption is an install's report, not an upgrade's.
    assert!(
        !reply.prompt.iter().any(|line| line.starts_with("Adopts")),
        "{:?}",
        reply.prompt
    );
    let early = fx.info("early").await?;
    assert_eq!(
        parsed(&find(&early.extensions, "volume", "snap")?.effective_json),
        json!({ "every": "1h", "keep": 5 })
    );
    let later = fx.info("later").await?;
    let invalid = find(&later.extensions, "volume", "snap")?;
    assert_eq!(invalid.state, "invalid");
    assert!(
        invalid
            .detail
            .contains("snap.keep: 2 is less than the minimum of 3"),
        "{}",
        invalid.detail
    );

    // Uninstall needs --force while jobs set the keys.
    let status = fx
        .svc
        .uninstall_app(req(
            UninstallAppRequest {
                name: "snaps".into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await
        .expect_err("jobs use its keys");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("early") && status.message().contains("later"),
        "{}",
        status.message()
    );
    assert!(status.message().contains("--force"));
    let reply = fx
        .svc
        .uninstall_app(req(
            UninstallAppRequest {
                name: "snaps".into(),
                force: true,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?
        .into_inner();
    assert_eq!(
        reply.orphaned_jobs,
        vec!["early".to_string(), "later".to_string()]
    );

    // The stored value is orphaned; the adopted pending one is pending again.
    let later = fx.info("later").await?;
    let orphaned = find(&later.extensions, "volume", "snap")?;
    assert_eq!(
        (orphaned.app.as_str(), orphaned.state.as_str()),
        ("snaps", "orphaned")
    );
    let early = fx.info("early").await?;
    assert_eq!(find(&early.extensions, "volume", "snap")?.state, "pending");

    // Resubmitting the orphaned job fails unless the key goes, or the flag
    // keeps it pending.
    let status = fx
        .run(&job("later", Some(2)))
        .await
        .expect_err("an orphaned key is unknown");
    assert!(
        status.message().contains("unknown field `snap`"),
        "{}",
        status.message()
    );
    fx.run_as(&job("later", Some(2)), true, &fx.admin).await?;
    Ok(())
}

/// A `$path` KV reference needs `kv:read` on its key wherever it sits in a
/// jobspec, and the check comes before anything resolves it (hardening
/// after the adversarial review): in a pending key planned with
/// `--allow-unknown-fields`, in an installed key's value (which the app's
/// validate call would otherwise see), and in a core field whose parse
/// error would otherwise echo the value.
#[tokio::test(flavor = "multi_thread")]
async fn kv_references_need_kv_read_wherever_they_are_test() -> R<()> {
    const SECRET: &str = "hunter2";
    const KEY: &str = "team-b/db/password";
    let fx = fixture().await?;
    fx.svc
        .put_kv(req(
            PutKvRequest {
                key: KEY.into(),
                value: SECRET.into(),
                value_type: KvValueType::String as i32,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let tenant = fx
        .token(
            "tenant",
            json!([{ "resources": ["job"], "actions": ["read", "write"], "namespaces": ["*"] }]),
        )
        .await?;
    let refused = |outcome: Result<String, tonic::Status>, what: &str| match outcome {
        Ok(shown) => panic!("{what} was not refused, and showed: {shown}"),
        Err(status) => {
            assert_eq!(status.code(), Code::PermissionDenied, "{what}: {status}");
            assert!(
                !status.message().contains(SECRET),
                "{what} leaked: {status}"
            );
            assert!(status.message().contains(KEY), "{what}: {status}");
        }
    };
    let noop = json!({ "name": "t", "runtime": "container",
                       "config": { "image": "alpine", "command": ["true"] } });

    // A pending key: `job:read` was enough to read any key back.
    let leak = json!({ "name": "planned", "leak": format!("${KEY}"), "task": noop });
    let plan = fx
        .svc
        .plan_job(req(
            PlanJobRequest {
                parsed_jobspec: leak.to_string(),
                allow_unknown_fields: true,
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .map(|reply| format!("{:?}", reply.into_inner().extensions));
    refused(plan, "PlanJob with a pending key");

    // An installed key's value: nobody's app hears it, nothing is stored.
    let before = fx.forwarder.calls.load(Ordering::SeqCst);
    let stored = json!({ "name": "stored", "uptime": { "url": format!("${KEY}") }, "task": noop });
    let run = fx
        .run_as(&stored, false, &tenant)
        .await
        .map(|reply| reply.message);
    refused(run, "RunJob with an installed key");
    assert_eq!(
        fx.forwarder.calls.load(Ordering::SeqCst),
        before,
        "the validate call heard a value its submitter may not read"
    );
    assert!(fx.store.get_job_config_optional("stored")?.is_none());

    // A core field whose type the value does not fit.
    let echoed = json!({ "name": "echoed", "priority": format!("${KEY}"), "task": noop });
    let run = fx
        .run_as(&echoed, false, &tenant)
        .await
        .map(|reply| reply.message);
    refused(run, "RunJob with a mistyped core field");

    // Holding `kv:read` on the key is what it takes.
    let reader = fx
        .token(
            "reader",
            json!([
                { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["*"] },
                { "resources": ["kv"], "actions": ["read"], "paths": ["team-b/*"] },
            ]),
        )
        .await?;
    fx.run_as(&stored, false, &reader).await?;
    Ok(())
}

/// An allocation of `legion-system/<app>` speaks for the app only while the
/// version it runs carries the reconciler's `managedBy` stamp for that app
/// (hardening after the adversarial review): a version someone submitted
/// straight to the job is not the app's.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_stamped_allocation_speaks_for_its_app_test() -> R<()> {
    let fx = fixture().await?;
    let job = format!("legion-system/{APP}");
    let token = fx.app_token(APP)?;
    fx.svc
        .watch_app_extensions(req(WatchAppExtensionsRequest::default(), &token))
        .await?;

    // A version of the app's job stored without the stamp. RunJob refuses
    // a direct submit of an installed app's job, so this is what an older
    // release could have left.
    let mut edited = serde_json::to_value(fx.store.get_job_config(&job)?)?;
    edited["version"] = Value::Null;
    edited["tasks"][0]["count"] = json!(2);
    let refused = fx.run(&edited).await.expect_err("an app's job");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    let edited: JobConfig = serde_json::from_value(edited)?;
    fx.store.handle_job_submit_as(
        &edited,
        &[Submission {
            path: "job.json".into(),
            content: "{}".into(),
            is_primary: true,
        }],
        None,
        "admin",
        None,
    )?;
    let config_id = fx.store.get_job_config(&job)?.id();
    assert!(fx.store.job_managed_by(&job)?.is_none());
    fx.store
        .enqueue_eval("job_reschedule", Some(&job), Some(&config_id), None, None)?;
    fx.drain_evals()?;
    let allocation = fx
        .store
        .allocs_for_job(&job)?
        .into_iter()
        .find(|allocation| allocation.job_config_id == config_id)
        .context("the unstamped version should be placed")?;
    let task = fx
        .store
        .get_task_configs_for_job_config(&allocation.job_config_id)?
        .into_iter()
        .find(|(id, _)| *id == allocation.task_config_id)
        .map(|(_, task)| task)
        .context("task config")?;
    let claims = claims_for_allocation(&fx.store, &allocation, &task, "auto")?;
    let identity = WorkloadIdentity {
        name: CONTROL_PLANE_IDENTITY_NAME.into(),
        ..Default::default()
    };
    let unstamped = fx
        .state
        .issue(&claims, &identity, std::time::SystemTime::now())?
        .token;
    let status = fx
        .svc
        .watch_app_extensions(req(WatchAppExtensionsRequest::default(), &unstamped))
        .await
        .expect_err("an unstamped version's allocation is not the app");
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    Ok(())
}

/// An upgrade that drops an extension point jobs set a key at is refused
/// without `force`, naming the jobs, as uninstall is; the dry run lists the
/// jobs and reports their values as orphaned, not as passing (hardening
/// after the adversarial review, M7).
#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_that_drops_a_used_point_needs_force_test() -> R<()> {
    let fx = fixture().await?;
    let two = small_manifest(
        "pair",
        "0.1.0",
        json!([
            { "point": "task", "key": "slaTier",
              "schema": object_schema(json!({ "level": { "type": "string" } }), &[]) },
            { "point": "job", "key": "costCenter",
              "schema": object_schema(json!({ "team": { "type": "string" } }), &[]) },
        ]),
    );
    let one = small_manifest(
        "pair",
        "0.2.0",
        json!([
            { "point": "job", "key": "costCenter",
              "schema": object_schema(json!({ "team": { "type": "string" } }), &[]) },
        ]),
    );
    fx.install(&two, &[]).await?;
    fx.run(&json!({
        "name": "tiered",
        "costCenter": { "team": "a" },
        "task": { "name": "main", "runtime": "exec", "config": { "command": ["true"] },
                  "slaTier": { "level": "gold" } },
    }))
    .await?;
    let upgrade = |dry_run: bool, force: bool| {
        fx.svc.upgrade_app(req(
            UpgradeAppRequest {
                name: "pair".into(),
                manifest_json: one.to_string(),
                dry_run,
                force,
                ..Default::default()
            },
            &fx.admin,
        ))
    };
    let dry = upgrade(true, false).await?.into_inner();
    assert_eq!(dry.orphaned_jobs, vec!["tiered".to_string()]);
    assert!(dry.uncovered.is_empty());
    assert!(dry.message.contains("--force"), "{}", dry.message);
    assert!(
        dry.prompt
            .iter()
            .any(|line| line.starts_with("1 value of slaTier on tasks is orphaned")),
        "{:?}",
        dry.prompt
    );
    assert!(
        !dry.prompt
            .iter()
            .any(|line| line.contains("slaTier") && line.contains("every value passes")),
        "an orphaned value is not passing: {:?}",
        dry.prompt
    );
    let refused = upgrade(false, false)
        .await
        .expect_err("jobs use the dropped point");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().contains("tiered") && refused.message().contains("--force"),
        "{}",
        refused.message()
    );
    let reply = upgrade(false, true).await?.into_inner();
    assert!(reply.upgraded, "{}", reply.message);
    assert_eq!(reply.orphaned_jobs, vec!["tiered".to_string()]);
    let info = fx.info("tiered").await?;
    assert_eq!(find(&info.extensions, "task", "slaTier")?.state, "orphaned");
    Ok(())
}
