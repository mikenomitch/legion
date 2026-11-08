//! The bundled cert manager end to end (cert-manager plan, "Testing plan";
//! docs/cert-manager.md).
//!
//! In process, against a manager with ACLs and workload identity on, served
//! over gRPC: the cert manager's manifest installed (as `certsx`, since a
//! bundled name cannot be installed, with a job that runs nothing: the
//! engine runs here, as the app's own workload identity), and its engine
//! driven over the real API:
//!
//! - writing a Certificate needs `kv` read and write on the key it names
//!   (the kind's `requires`), and the CEL rules refuse a malformed Issuer;
//! - the app generates a CA in KV and publishes its certificate, issues a
//!   Certificate into its writer's key, acting for the Certificate, and
//!   reports `Ready` with the expiry; its own credential cannot touch that
//!   key (`via = "objects"`);
//! - the `certs/renew` label re-issues once; an Issuer's `namespaces` turns
//!   other namespaces away;
//! - an ACME Issuer registers with a fake RFC 8555 CA, keeps its account
//!   key under the app's own KV prefix, and issues a wildcard through
//!   DNS-01 and the webhook provider.

#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::apps::certs::engine::{Engine, EngineOptions, LegionPlane, RENEW_LABEL};
use legion::apps::certs::issue::{verify_chain, Bundle};
use legion::apps::certs::spec::Usage;
use legion::apps::certs::test_support::{FakeAcme, FakeAcmeOptions};
use legion::cmd::helpers::{manager_connection_with_token, ClientTimeouts, ManagerConnection};
use legion::manager::acl::Authenticator;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::manager_server::{Manager, ManagerServer};
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
/// The cert manager, installed under another name.
const APP: &str = "certsx";
const NAMESPACE: &str = "team-a";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

fn node(id: &str, pool: &str, manager: bool) -> NodeMsg {
    let mut config = json!({
        "node_id": id,
        "maxAllocations": 20,
        "resources": { "memory": 65536, "cpu": 40000 },
    });
    if manager {
        config["legion.role"] = json!("manager");
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

/// The bundled manifest, as an installable app named [`APP`] whose job runs
/// nothing.
fn manifest() -> R<Value> {
    let bundled = legion::apps::bundled_manifest("certs").context("the bundled cert manager")?;
    let mut manifest = serde_json::to_value(bundled)?;
    manifest["name"] = json!(APP);
    manifest["version"] = json!("1.0.0");
    manifest["job"]["task"]["config"]["command"] = json!(["sleep", "3600"]);
    manifest["permissions"][0]["paths"] = json!([format!("legion/apps/{APP}/*")]);
    Ok(manifest)
}

struct Fixture {
    svc: Arc<ManagerDaemonService>,
    store: Store,
    state: Arc<WorkloadIdentityState>,
    admin: String,
    addr: String,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
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

    /// Install the app, reconcile its job, and place it.
    async fn install(&self) -> R<()> {
        self.svc
            .install_app(req(
                InstallAppRequest {
                    manifest_json: manifest()?.to_string(),
                    source: "file".into(),
                    source_ref: "./certs.json".into(),
                    ..Default::default()
                },
                &self.admin,
            ))
            .await
            .map_err(|status| anyhow!("install: {}", status.message()))?;
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
        let outcome = reconciler.reconcile_app(APP).await;
        anyhow::ensure!(
            matches!(
                outcome,
                legion::manager::apps::PassOutcome::Submitted { .. }
            ),
            "{outcome:?}"
        );
        let job = format!("legion-system/{APP}");
        let config_id = self.store.get_job_config(&job)?.id();
        self.store
            .enqueue_eval("job_reschedule", Some(&job), Some(&config_id), None, None)?;
        self.drain_evals()?;
        let allocation = self
            .store
            .allocs_for_job(&job)?
            .into_iter()
            .next()
            .with_context(|| format!("{job} should be placed"))?;
        self.store.upsert_allocation_health_snapshots(
            &allocation.node_id,
            &[AllocationHealth {
                allocation_id: allocation.id,
                healthy: true,
                status: "running".into(),
                artifact_states: None,
            }],
        )?;
        Ok(())
    }

    /// The `legion` token the app's allocation would hold.
    fn app_token(&self) -> R<String> {
        let job = format!("legion-system/{APP}");
        let allocation = self
            .store
            .allocs_for_job(&job)?
            .into_iter()
            .next()
            .context("the app is placed")?;
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

    fn connection(&self, token: &str) -> R<ManagerConnection> {
        manager_connection_with_token(
            &self.addr,
            token,
            ClientTimeouts {
                connect: Some(Duration::from_secs(5)),
                request: Some(Duration::from_secs(30)),
            },
        )
    }

    /// Run the cert manager's engine as the app, until the returned sender
    /// is dropped or sent `true`.
    fn run_engine(&self, options: EngineOptions) -> R<tokio::sync::watch::Sender<bool>> {
        let connection = self.connection(&self.app_token()?)?;
        let engine = Engine::new(Arc::new(LegionPlane::new(connection.clone())), options);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        legion::apps::certs::start(engine, connection, APP.to_string(), stop_rx);
        Ok(stop_tx)
    }

    async fn apply(
        &self,
        kind: &str,
        name: &str,
        namespace: &str,
        labels: Value,
        spec: Value,
        token: &str,
    ) -> Result<ApplyObjectReply, tonic::Status> {
        let mut object = json!({
            "kind": format!("{APP}/{kind}"), "name": name, "labels": labels, "spec": spec,
        });
        if !namespace.is_empty() {
            object["namespace"] = json!(namespace);
        }
        self.svc
            .apply_object(req(
                ApplyObjectRequest {
                    object_json: object.to_string(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn get(&self, kind: &str, name: &str, namespace: &str) -> R<AppObject> {
        self.svc
            .get_object(req(
                GetObjectRequest {
                    kind: format!("{APP}/{kind}"),
                    name: name.into(),
                    namespace: namespace.into(),
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .object
            .context("the object")
    }

    async fn kv(&self, key: &str) -> R<Option<String>> {
        Ok(self
            .svc
            .get_kv(req(
                GetKvRequest {
                    key: key.into(),
                    metadata_only: false,
                },
                &self.admin,
            ))
            .await?
            .into_inner()
            .entry
            .map(|entry| entry.value))
    }

    /// Wait until `name`'s status has `Ready` with `reason` and `check`
    /// holds on its fields.
    async fn wait_ready(
        &self,
        kind: &str,
        name: &str,
        namespace: &str,
        reason: &str,
        check: impl Fn(&Value) -> bool,
    ) -> R<AppObject> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let object = self.get(kind, name, namespace).await?;
            let status = object.status.clone().unwrap_or_default();
            let fields: Value = serde_json::from_str(&status.fields_json).unwrap_or(Value::Null);
            let ready = status
                .conditions
                .iter()
                .find(|c| c.r#type == "Ready")
                .map(|c| (c.status.clone(), c.reason.clone(), c.message.clone()));
            if ready
                .as_ref()
                .is_some_and(|(status, why, _)| status == "true" && why == reason)
                && check(&fields)
            {
                return Ok(object);
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!(
                    "{kind} {namespace}/{name} did not turn {reason}: {} {} {ready:?} {fields}",
                    object.summary,
                    object.summary_detail
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-certs-")
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
    store.insert_node(node("mgr-1", "legion-system", true))?;
    store.insert_node(node("node-1", "default", false))?;
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
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth.clone())
            .with_workload_identity(Some(state.clone())),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://127.0.0.1:{}", listener.local_addr()?.port());
    let incoming = legion::manager::daemon::manager_incoming(&store, listener, None)?;
    let served = svc.clone();
    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ManagerServer::from_arc(served))
            .serve_with_incoming(incoming)
            .await;
    });
    let fx = Fixture {
        svc,
        store,
        state,
        admin,
        addr,
        server,
    };
    fx.svc
        .apply_namespace(req(
            ApplyNamespaceRequest {
                namespace: Some(Namespace {
                    name: NAMESPACE.into(),
                    ..Default::default()
                }),
                dry_run: false,
            },
            &fx.admin,
        ))
        .await?;
    fx.install().await?;
    Ok(fx)
}

fn options() -> EngineOptions {
    EngineOptions {
        propagation_delay: Some(Duration::ZERO),
        acme_timeout: Duration::from_secs(30),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_private_ca_issues_into_its_writers_key_test() -> R<()> {
    let fx = fixture().await?;
    // The writer may write Certificates in team-a, and keys under
    // certs/team-a/ only.
    let writer = fx
        .token(
            "writer",
            json!([
                { "resources": ["object"], "actions": ["read", "write"],
                  "namespaces": [NAMESPACE], "kinds": [format!("{APP}/Certificate")] },
                { "resources": ["kv"], "actions": ["read", "write"], "paths": ["certs/team-a/*"] },
            ]),
        )
        .await?;

    // The CEL rules refuse an Issuer of two types, whatever the app says.
    let err = fx
        .apply(
            "Issuer",
            "both",
            "",
            json!({}),
            json!({ "selfSigned": {}, "ca": { "kv": "certs/ca/x" } }),
            &fx.admin,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert!(
        err.message()
            .contains("exactly one of selfSigned, ca, and acme"),
        "{}",
        err.message()
    );

    fx.apply(
        "Issuer",
        "internal",
        "",
        json!({}),
        json!({ "namespaces": [NAMESPACE], "ca": { "kv": "certs/ca/internal", "trust": "certs/ca/internal.pem" } }),
        &fx.admin,
    )
    .await?;

    // A Certificate whose key the writer may not write is refused.
    let err = fx
        .apply(
            "Certificate",
            "web",
            NAMESPACE,
            json!({}),
            json!({ "issuer": "internal", "kv": "certs/elsewhere", "dnsNames": ["web.internal"] }),
            &writer,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    assert!(
        err.message().contains("certs/elsewhere"),
        "{}",
        err.message()
    );
    // No names: the kind's rule.
    let err = fx
        .apply(
            "Certificate",
            "web",
            NAMESPACE,
            json!({}),
            json!({ "issuer": "internal", "kv": "certs/team-a/web" }),
            &writer,
        )
        .await
        .unwrap_err();
    assert!(
        err.message().contains("at least one of dnsNames"),
        "{}",
        err.message()
    );

    fx.apply(
        "Certificate",
        "web",
        NAMESPACE,
        json!({}),
        json!({ "issuer": "internal", "kv": "certs/team-a/web", "dnsNames": ["web.internal"],
                "parts": { "key": "certs/team-a/web.key", "ca": "certs/team-a/ca.pem" } }),
        &writer,
    )
    .await?;

    let stop = fx.run_engine(options())?;
    let issuer = fx
        .wait_ready("Issuer", "internal", "", "Generated", |_| true)
        .await?;
    assert!(issuer
        .status
        .unwrap()
        .fields_json
        .contains("\"generated\":true"));
    let trust = fx
        .kv("certs/ca/internal.pem")
        .await?
        .context("the trust key")?;
    assert!(!trust.contains("PRIVATE KEY"));

    let object = fx
        .wait_ready("Certificate", "web", NAMESPACE, "Issued", |fields| {
            fields["notAfter"].is_string()
        })
        .await?;
    assert_eq!(object.summary, "Ready");
    let bundle = Bundle::parse(&fx.kv("certs/team-a/web").await?.context("issued")?)?;
    verify_chain(&bundle.chain_pem, &trust, "web.internal", Usage::Server)?;
    verify_chain(&bundle.chain_pem, &trust, "web.internal", Usage::Client)?;
    assert_eq!(
        fx.kv("certs/team-a/web.key").await?,
        Some(bundle.key_pem.clone())
    );
    assert_eq!(fx.kv("certs/team-a/ca.pem").await?, Some(trust.clone()));

    // The app's own credential cannot write the key: its user-key grant is
    // only for its objects.
    let own = fx.connection(&fx.app_token()?)?;
    let err = own
        .client()
        .put_kv(PutKvRequest {
            key: "certs/team-a/web".into(),
            value: "x".into(),
            value_type: KvValueType::String as i32,
            expected_version: None,
            lock_id: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");

    // `legion cert renew`: a new label value re-issues once.
    fx.apply(
        "Certificate",
        "web",
        NAMESPACE,
        json!({ RENEW_LABEL: "2026-11-07T10:00:00.000Z" }),
        json!({ "issuer": "internal", "kv": "certs/team-a/web", "dnsNames": ["web.internal"],
                "parts": { "key": "certs/team-a/web.key", "ca": "certs/team-a/ca.pem" } }),
        &writer,
    )
    .await?;
    fx.wait_ready("Certificate", "web", NAMESPACE, "Issued", |fields| {
        fields["renewRequest"] == "2026-11-07T10:00:00.000Z"
    })
    .await?;
    let renewed = Bundle::parse(&fx.kv("certs/team-a/web").await?.context("renewed")?)?;
    assert_ne!(renewed.leaf.serial, bundle.leaf.serial);
    assert_eq!(
        fx.kv("certs/team-a/web.key").await?,
        Some(renewed.key_pem.clone())
    );

    // An Issuer that does not serve the namespace turns it away.
    fx.apply(
        "Issuer",
        "elsewhere",
        "",
        json!({}),
        json!({ "namespaces": ["team-b"], "selfSigned": {} }),
        &fx.admin,
    )
    .await?;
    fx.apply(
        "Certificate",
        "other",
        NAMESPACE,
        json!({}),
        json!({ "issuer": "elsewhere", "kv": "certs/team-a/other", "dnsNames": ["o.internal"] }),
        &writer,
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let object = fx.get("Certificate", "other", NAMESPACE).await?;
        if object.summary.starts_with("Rejected") {
            assert!(
                object.summary_detail.contains("IssuerNotAllowed"),
                "{}",
                object.summary_detail
            );
            break;
        }
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "{}", object.summary);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(fx.kv("certs/team-a/other").await?.is_none());
    let _ = stop.send(true);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acme_issues_a_wildcard_through_dns01_test() -> R<()> {
    let fx = fixture().await?;
    let ca = FakeAcme::start(FakeAcmeOptions::default()).await?;
    let hook = ca.dns_webhook().await?;
    fx.apply(
        "Issuer",
        "letsencrypt",
        "",
        json!({}),
        json!({ "acme": { "server": ca.directory_url(), "email": "ops@example.com",
                          "dns01": { "webhook": { "url": hook }, "propagationDelay": "0s" } } }),
        &fx.admin,
    )
    .await?;
    fx.apply(
        "Certificate",
        "shop",
        NAMESPACE,
        json!({}),
        json!({ "issuer": "letsencrypt", "kv": "certs/shop",
                "dnsNames": ["shop.example.com", "*.shop.example.com"] }),
        &fx.admin,
    )
    .await?;
    let stop = fx.run_engine(options())?;

    let issuer = fx
        .wait_ready("Issuer", "letsencrypt", "", "Registered", |fields| {
            fields["thumbprint"].is_string()
        })
        .await?;
    let fields: Value = serde_json::from_str(&issuer.status.unwrap().fields_json)?;
    // The account key is the app's own, under its reserved prefix.
    let key = fx
        .kv(&format!("legion/apps/{APP}/acme/{}.key", issuer.uid))
        .await?
        .context("the account key")?;
    assert_eq!(
        legion::apps::certs::acme::AccountKey::from_pem(&key)?.thumbprint(),
        fields["thumbprint"].as_str().unwrap()
    );

    fx.wait_ready("Certificate", "shop", NAMESPACE, "Issued", |fields| {
        fields["names"]
            .as_array()
            .is_some_and(|names| names.len() == 2)
    })
    .await?;
    let bundle = Bundle::parse(&fx.kv("certs/shop").await?.context("issued")?)?;
    verify_chain(
        &bundle.chain_pem,
        &ca.root_pem(),
        "api.shop.example.com",
        Usage::Server,
    )?;
    assert_eq!(
        ca.validations()
            .get("*.shop.example.com")
            .map(String::as_str),
        Some("dns-01")
    );
    assert!(ca.dns_records().is_empty(), "the TXT records were removed");
    assert_eq!(ca.orders(), 1);
    let _ = stop.send(true);
    Ok(())
}

/// The plan's scenario 2 with the real binary: a fresh single manager
/// seeds the cert manager and runs it on its system worker
/// (`legion:self app run certs`, as the app's own uid, through its API
/// socket); a CA Issuer and a Certificate applied with the CLI end up as a
/// bundle in KV that chains to the published CA; and `legion cert renew
/// --wait` re-issues it.
///
/// An app's exec task runs as the app's own uid, which takes a worker
/// that can switch users: unprivileged, the test is skipped.
#[cfg(unix)]
#[test]
fn a_fresh_cluster_runs_the_cert_manager_test() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipped: an app's exec task runs as its own uid, which needs root");
        return;
    }
    let _serial = crate::system_worker::SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ports = crate::support::DYNAMIC_PORT_GATE.blocking_lock();
    let cluster = crate::system_worker::Cluster::start_with_certs(false);
    cluster.wait_for("the cert manager to run on the system worker", || {
        let list = cluster.cli_json(&["app", "list"]);
        list["data"]["apps"].as_array().is_some_and(|apps| {
            apps.iter()
                .any(|app| app["name"] == "certs" && app["status"] == "running")
        })
    });

    let write = |name: &str, object: Value| -> String {
        let path = cluster.dir().join(name);
        std::fs::write(&path, object.to_string()).expect("write object");
        path.to_str().expect("utf-8").to_string()
    };
    let issuer = write(
        "issuer.json",
        json!({ "kind": "certs/Issuer", "name": "internal",
                "spec": { "ca": { "kv": "certs/ca/internal", "trust": "certs/ca/internal.pem" } } }),
    );
    let certificate = write(
        "certificate.json",
        json!({ "kind": "certs/Certificate", "name": "web", "namespace": "default",
                "spec": { "issuer": "internal", "kv": "certs/web", "dnsNames": ["web.internal"] } }),
    );
    for file in [&issuer, &certificate] {
        let applied = cluster.cli_json(&["object", "apply", "-f", file]);
        assert_eq!(applied["ok"], true, "{applied}");
    }
    let fields = |object: &Value| -> Value {
        object["status"]["fieldsJson"]
            .as_str()
            .and_then(|text| serde_json::from_str(text).ok())
            .unwrap_or(Value::Null)
    };
    let mut first = Value::Null;
    cluster.wait_for("the certificate to be issued", || {
        let got = cluster.cli_json(&["object", "get", "certificate", "web", "-n", "default"]);
        first = got["data"]["object"].clone();
        first["summary"] == "Ready"
    });
    let kv = |key: &str| -> String {
        let got = cluster.cli_json(&["kv", "get", key]);
        assert_eq!(got["ok"], true, "{got}");
        got["data"]["value"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };
    let trust = kv("certs/ca/internal.pem");
    let bundle = Bundle::parse(&kv("certs/web")).expect("a servable bundle");
    verify_chain(&bundle.chain_pem, &trust, "web.internal", Usage::Server).expect("chains");
    assert_eq!(fields(&first)["serial"], json!(bundle.leaf.serial));

    let renewed = cluster.cli_json(&["cert", "renew", "web", "-n", "default", "--wait"]);
    assert_eq!(renewed["ok"], true, "{renewed}");
    let after = fields(&renewed["data"]["renewed"]);
    assert_eq!(
        after["renewRequest"], renewed["data"]["requested"],
        "{renewed}"
    );
    assert_ne!(after["serial"], json!(bundle.leaf.serial));
    let again = Bundle::parse(&kv("certs/web")).expect("a servable bundle");
    assert_eq!(after["serial"], json!(again.leaf.serial));
}
