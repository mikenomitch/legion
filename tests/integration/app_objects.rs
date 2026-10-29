//! App-owned object kinds (app-object-kinds plan §9; docs/app-objects.md).
//!
//! In process, against a manager service with ACLs and workload identity
//! on, a `gitops` app declaring a `Repo` kind, and a fake app port behind a
//! loopback forwarder answering the object validate call:
//!
//! - apply, get, list, delete with ACLs: the `object` resource by namespace
//!   and kind, what `requires` asks of the writer (job:write on the jobs a
//!   Repo names), check-before-fetch (PermissionDenied for a missing object
//!   outside the grant), unknown fields, immutable fields, `writeOnly`
//!   redaction, the dry-run diff, CAS and idempotency;
//! - the validate call: a refusal, a warning, failing open;
//! - the app plane: the snapshot (writeOnly revealed to the app), status
//!   and the summary, stale generations, managed objects and `force`, the
//!   cleanup window and `ReleaseObject`;
//! - the framework's reconcile loop against the real manager;
//! - an unknown kind is retryable (`KIND_NOT_INSTALLED`);
//! - uninstall needs `--force` and orphans; reinstall adopts;
//! - a namespace holding objects is not deleted without `--cascade`.

#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use futures::stream::BoxStream;
use futures::StreamExt;
use legion::apps::objects::{ObjectOutcome, ObjectPlane, Reconciler};
use legion::apps::ui::{
    AssertionVerifier, ClusterKeys, ObjectValidateRequest, StaticKeys, UiRegistry, UiServer,
};
use legion::manager::acl::Authenticator;
use legion::manager::app_forward::AppForwarder;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::app_object::ObjectValidateAnswer;
use legion::structs::manager_server::Manager;
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Request, Status};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
const MGR_NODE: &str = "mgr-1";
const NODE: &str = "node-1";
const APP: &str = "gitops";
const KIND: &str = "gitops/Repo";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

/// The `gitops` app: a `Repo` kind whose writer needs job:write on the jobs
/// it names, with a cleanup window, a write-only key, and the validate call.
fn manifest(version: &str, with_kind: bool) -> Value {
    let mut manifest = json!({
        "name": APP,
        "version": version,
        "job": { "task": { "name": "sync", "runtime": "exec",
                            "config": { "command": ["sleep", "3600"] },
                            "network": { "ports": [{ "name": "api" }] } } },
        // The app writes objects of its own kind only as its grant allows,
        // like anyone else.
        "permissions": [
            { "resources": ["object"], "namespaces": ["team-a"], "kinds": [KIND],
              "actions": ["read", "write"] },
            { "resources": ["job"], "namespaces": ["team-a"], "actions": ["read", "write"] },
        ],
    });
    if with_kind {
        manifest["kinds"] = json!([{
            "name": "Repo",
            "description": "A git repository to deploy from.",
            "schema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string" },
                    "branch": { "type": "string", "default": "main" },
                    "jobs": { "type": "array", "items": { "type": "string" } },
                    "deployKey": { "type": "string", "writeOnly": true },
                },
            },
            "status": { "schema": { "type": "object",
                                    "properties": { "commit": { "type": "string" } } } },
            "validate": true,
            "requires": [{ "resource": "job", "actions": ["write"], "name": "/spec/jobs" }],
            "immutable": ["/spec/url"],
            "cleanup": "10m",
            "columns": ["/spec/branch"],
        }]);
    }
    manifest
}

/// How the forwarder answers.
#[derive(Debug)]
struct LoopbackForwarder {
    addr: std::net::SocketAddr,
    unreachable: AtomicBool,
}

#[tonic::async_trait]
impl AppForwarder for LoopbackForwarder {
    async fn forward(
        &self,
        _store: &Store,
        node_id: &str,
        request: ForwardAppRequestRequest,
    ) -> Result<ForwardAppRequestReply, Status> {
        assert_eq!(node_id, MGR_NODE);
        legion::worker::app_forward::check_request(&request)?;
        if self.unreachable.load(Ordering::SeqCst) {
            return Err(Status::unavailable("connection refused"));
        }
        let stream = tokio::net::TcpStream::connect(self.addr)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        legion::worker::app_forward::exchange(stream, &request).await
    }
}

/// The fake app's port: its object validate call refuses a URL with `bad`
/// in it and warns about one with `slow` in it.
async fn fake_app(state: &WorkloadIdentityState) -> R<std::net::SocketAddr> {
    let registry = Arc::new(UiRegistry::default());
    registry.object_validate("Repo", |request: ObjectValidateRequest| async move {
        let url = request.call.spec["url"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let mut answer = ObjectValidateAnswer::default();
        if url.contains("bad") {
            answer.errors.push(format!(
                "{url} is not a repository (asked by {})",
                request.viewer.principal
            ));
        }
        if url.contains("slow") {
            answer.warnings.push(format!("{url} answers slowly"));
        }
        Ok(answer)
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

    async fn install(&self, manifest: &Value) -> Result<InstallAppReply, Status> {
        self.svc
            .install_app(req(
                InstallAppRequest {
                    manifest_json: manifest.to_string(),
                    source: "file".into(),
                    source_ref: "./app.json".into(),
                    ..Default::default()
                },
                &self.admin,
            ))
            .await
            .map(tonic::Response::into_inner)
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

    /// Reconcile the app's job, place it, and report it healthy.
    async fn start_app(&self) -> R<()> {
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
        assert!(
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

    /// The `legion` token the manager would issue the app's allocation.
    fn app_token(&self) -> R<String> {
        let job = format!("legion-system/{APP}");
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

    async fn namespace(&self, name: &str) -> R<()> {
        self.svc
            .apply_namespace(req(
                ApplyNamespaceRequest {
                    namespace: Some(Namespace {
                        name: name.into(),
                        ..Default::default()
                    }),
                    dry_run: false,
                },
                &self.admin,
            ))
            .await?;
        Ok(())
    }

    async fn apply(&self, object: Value, token: &str) -> Result<ApplyObjectReply, Status> {
        self.apply_with(object, token, |_| {}).await
    }

    async fn apply_with(
        &self,
        object: Value,
        token: &str,
        edit: impl FnOnce(&mut ApplyObjectRequest),
    ) -> Result<ApplyObjectReply, Status> {
        let mut request = ApplyObjectRequest {
            object_json: object.to_string(),
            ..Default::default()
        };
        edit(&mut request);
        self.svc
            .apply_object(req(request, token))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn get(&self, kind: &str, id: &str, token: &str) -> Result<AppObject, Status> {
        let (namespace, name) = id.split_once('/').unwrap_or(("", id));
        Ok(self
            .svc
            .get_object(req(
                GetObjectRequest {
                    kind: kind.into(),
                    name: name.into(),
                    namespace: namespace.into(),
                },
                token,
            ))
            .await?
            .into_inner()
            .object
            .expect("an object"))
    }

    async fn delete(
        &self,
        id: &str,
        token: &str,
        edit: impl FnOnce(&mut DeleteObjectRequest),
    ) -> Result<DeleteObjectReply, Status> {
        let (namespace, name) = id.split_once('/').unwrap_or(("", id));
        let mut request = DeleteObjectRequest {
            kind: KIND.into(),
            name: name.into(),
            namespace: namespace.into(),
            ..Default::default()
        };
        edit(&mut request);
        self.svc
            .delete_object(req(request, token))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn set_status(
        &self,
        object: &AppObject,
        generation: u64,
        conditions: Vec<ObjectCondition>,
        token: &str,
    ) -> Result<UpdateObjectStatusReply, Status> {
        self.svc
            .update_object_status(req(
                UpdateObjectStatusRequest {
                    kind: object.kind.clone(),
                    namespace: object.namespace.clone(),
                    name: object.name.clone(),
                    uid: object.uid,
                    observed_generation: generation,
                    conditions,
                    fields_json: json!({ "commit": "3f9a" }).to_string(),
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
    }
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-app-objects-")
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
        unreachable: AtomicBool::new(false),
    });
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth.clone())
            .with_workload_identity(Some(state.clone()))
            .with_app_forwarder(forwarder.clone()),
    );
    let fx = Fixture {
        svc,
        store,
        state,
        admin,
        forwarder,
    };
    fx.install(&manifest("1.0.0", true)).await?;
    fx.start_app().await?;
    fx.namespace("team-a").await?;
    fx.namespace("team-b").await?;
    Ok(fx)
}

fn repo(name: &str, namespace: &str, spec: Value) -> Value {
    json!({ "kind": KIND, "name": name, "namespace": namespace, "spec": spec })
}

fn condition(kind: &str, status: &str, reason: &str) -> ObjectCondition {
    ObjectCondition {
        r#type: kind.into(),
        status: status.into(),
        reason: reason.into(),
        ..Default::default()
    }
}

fn assert_code<T: std::fmt::Debug>(result: Result<T, Status>, code: Code, needle: &str) {
    let status = result.expect_err(&format!("expected {code:?} ({needle})"));
    assert_eq!(status.code(), code, "{}", status.message());
    assert!(
        status.message().contains(needle),
        "{:?} lacks {needle:?}",
        status.message()
    );
}

/// The person's side, with ACLs: the object resource by namespace and
/// kind, `requires` as the writer, check-before-fetch, schemas, the
/// validate call, the diff, CAS, and idempotency.
#[tokio::test(flavor = "multi_thread")]
async fn objects_are_written_as_their_writer_test() -> R<()> {
    let fx = fixture().await?;
    let writer = fx
        .token(
            "team-a-writer",
            json!([
                { "resources": ["object"], "namespaces": ["team-a"], "kinds": [KIND],
                  "actions": ["read", "write"] },
                { "resources": ["job"], "namespaces": ["team-a"], "names": ["web"],
                  "actions": ["read", "write"] },
            ]),
        )
        .await?;

    // Written with the defaults filled, the deploy key hidden from people.
    let created = fx
        .apply(
            repo(
                "web",
                "team-a",
                json!({ "url": "https://git/web", "jobs": ["web"], "deployKey": "s3cret" }),
            ),
            &writer,
        )
        .await?;
    assert!(created.created, "{created:?}");
    let object = created.object.clone().expect("object");
    assert_eq!(object.qualified_kind, KIND);
    assert_eq!(object.generation, 1);
    let effective: Value = serde_json::from_str(&object.effective_json)?;
    assert_eq!(effective["branch"], "main");
    assert_eq!(effective["deployKey"], "<redacted>");
    let authority = object.authority.clone().expect("authority");
    assert_eq!(authority.permissions.len(), 1, "{authority:?}");
    assert!(
        authority.permissions[0].starts_with("job:write in team-a on web"),
        "{authority:?}"
    );

    // Short names work where they are unambiguous.
    let read = fx.get("repos", "team-a/web", &writer).await?;
    assert_eq!(read.uid, object.uid);
    assert_eq!(read.summary, "Pending");

    // `requires`: naming a job the writer cannot write is refused.
    assert_code(
        fx.apply(
            repo(
                "api",
                "team-a",
                json!({ "url": "https://git/api", "jobs": ["api"] }),
            ),
            &writer,
        )
        .await,
        Code::PermissionDenied,
        "job:write",
    );
    // Naming none means every job in the namespace.
    assert_code(
        fx.apply(
            repo("all", "team-a", json!({ "url": "https://git/all" })),
            &writer,
        )
        .await,
        Code::PermissionDenied,
        "job:write",
    );

    // Outside the grant: PermissionDenied, also for an object that does not
    // exist (no probing).
    assert_code(
        fx.apply(
            repo("web", "team-b", json!({ "url": "https://git/web" })),
            &writer,
        )
        .await,
        Code::PermissionDenied,
        "object",
    );
    assert_code(
        fx.get(KIND, "team-b/missing", &writer).await,
        Code::PermissionDenied,
        "object",
    );
    let nobody = fx
        .token(
            "nobody",
            json!([{ "resources": ["job"], "namespaces": ["team-a"], "actions": ["read"] }]),
        )
        .await?;
    assert_code(
        fx.get("no-such-kind", "team-a/web", &nobody).await,
        Code::PermissionDenied,
        "object",
    );

    // Unknown and immutable fields.
    assert_code(
        fx.apply(
            repo(
                "web",
                "team-a",
                json!({ "url": "https://git/web", "brnch": "dev", "jobs": ["web"] }),
            ),
            &writer,
        )
        .await,
        Code::InvalidArgument,
        "branch",
    );
    assert_code(
        fx.apply(
            repo(
                "web",
                "team-a",
                json!({ "url": "https://git/other", "jobs": ["web"] }),
            ),
            &writer,
        )
        .await,
        Code::InvalidArgument,
        "immutable",
    );

    // A dry run shows the diff and writes nothing.
    let next = repo(
        "web",
        "team-a",
        json!({ "url": "https://git/web", "branch": "dev", "jobs": ["web"], "deployKey": "s3cret" }),
    );
    let plan = fx
        .apply_with(next.clone(), &writer, |r| r.dry_run = true)
        .await?;
    assert!(plan.dry_run && plan.changed, "{plan:?}");
    assert!(
        plan.diff.iter().any(|line| line.contains("branch")),
        "{:?}",
        plan.diff
    );
    assert_eq!(fx.get(KIND, "team-a/web", &writer).await?.generation, 1);

    // CAS: a stale version is refused; the current one applies.
    assert_code(
        fx.apply_with(next.clone(), &writer, |r| {
            r.expected_version = Some(object.spec_version + 7)
        })
        .await,
        Code::Aborted,
        "changed since you read it",
    );
    assert_code(
        fx.apply_with(next.clone(), &writer, |r| r.expected_version = Some(0))
            .await,
        Code::Aborted,
        "already exists",
    );
    let updated = fx
        .apply_with(next.clone(), &writer, |r| {
            r.expected_version = Some(object.spec_version);
            r.idempotency_key = Some("bump-branch".into());
        })
        .await?;
    assert!(updated.changed && !updated.created);
    assert_eq!(updated.object.as_ref().expect("object").generation, 2);
    // The same request again, with its key: answered from the record.
    let again = fx
        .apply_with(next.clone(), &writer, |r| {
            r.expected_version = Some(object.spec_version);
            r.idempotency_key = Some("bump-branch".into());
        })
        .await?;
    assert!(again.deduplicated);
    // Unchanged: no new generation.
    let same = fx.apply(next, &writer).await?;
    assert!(!same.changed);
    assert_eq!(same.object.expect("object").generation, 2);

    // The validate call: a refusal, a warning, and failing open.
    assert_code(
        fx.apply(
            repo(
                "bad",
                "team-a",
                json!({ "url": "https://bad", "jobs": ["web"] }),
            ),
            &writer,
        )
        .await,
        Code::InvalidArgument,
        "is not a repository (asked by",
    );
    let slow = fx
        .apply(
            repo(
                "slow",
                "team-a",
                json!({ "url": "https://slow", "jobs": ["web"] }),
            ),
            &writer,
        )
        .await?;
    assert!(
        slow.warnings.iter().any(|w| w.contains("answers slowly")),
        "{:?}",
        slow.warnings
    );
    fx.forwarder.unreachable.store(true, Ordering::SeqCst);
    let open = fx
        .apply(
            repo(
                "bad2",
                "team-a",
                json!({ "url": "https://bad", "jobs": ["web"] }),
            ),
            &writer,
        )
        .await?;
    assert!(
        open.warnings.iter().any(|w| w.contains("was not checked")),
        "{:?}",
        open.warnings
    );
    fx.forwarder.unreachable.store(false, Ordering::SeqCst);

    // The list shows what the writer may read.
    let listed = fx
        .svc
        .list_objects(req(
            ListObjectsRequest {
                kind: "repo".into(),
                namespaces: vec!["*".into()],
                ..Default::default()
            },
            &writer,
        ))
        .await?
        .into_inner();
    let names: Vec<&str> = listed.objects.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(names, vec!["bad2", "slow", "web"]);

    // Deleting needs what writing needs; a Repo with cleanup waits for it.
    let deleting = fx.delete("team-a/web", &writer, |_| {}).await?;
    assert!(deleting.deleting && !deleting.deleted, "{deleting:?}");
    assert_eq!(
        fx.get(KIND, "team-a/web", &writer).await?.summary,
        "Deleting"
    );
    assert_code(
        fx.apply(
            repo(
                "web",
                "team-a",
                json!({ "url": "https://git/web", "jobs": ["web"] }),
            ),
            &writer,
        )
        .await,
        Code::FailedPrecondition,
        "being deleted",
    );
    let gone = fx.delete("team-a/slow", &writer, |r| r.now = true).await?;
    assert!(gone.deleted, "{gone:?}");
    assert_code(
        fx.get(KIND, "team-a/slow", &writer).await,
        Code::NotFound,
        "not found",
    );
    Ok(())
}

/// The app's side: the snapshot (with write-only values), status and the
/// summary, stale generations, managed objects, and releasing a deleted
/// object; and the refusals around it.
#[tokio::test(flavor = "multi_thread")]
async fn the_app_watches_reports_and_releases_test() -> R<()> {
    let fx = fixture().await?;
    let app = fx.app_token()?;
    let created = fx
        .apply(
            repo(
                "web",
                "team-a",
                json!({ "url": "https://git/web", "deployKey": "s3cret" }),
            ),
            &fx.admin,
        )
        .await?;
    let object = created.object.expect("object");

    // The snapshot reveals the write-only value to its app only.
    let mut stream = fx
        .svc
        .watch_objects(req(WatchObjectsRequest::default(), &app))
        .await?
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await?
        .context("an event")??;
    assert!(first.snapshot && first.snapshot_done, "{first:?}");
    assert_eq!(first.upserts.len(), 1);
    let watched: Value = serde_json::from_str(&first.upserts[0].effective_json)?;
    assert_eq!(watched["deployKey"], "s3cret");

    // A person's token is not the app.
    assert_code(
        fx.svc
            .update_object_status(req(
                UpdateObjectStatusRequest {
                    kind: KIND.into(),
                    namespace: "team-a".into(),
                    name: "web".into(),
                    uid: object.uid,
                    observed_generation: 1,
                    ..Default::default()
                },
                &fx.token("reader", json!([{ "resources": ["object"], "namespaces": ["*"], "actions": ["read", "write"] }]))
                    .await?,
            ))
            .await,
        Code::PermissionDenied,
        "workload identity",
    );

    // Status: Ready, then a generation the object does not have yet.
    let reply = fx
        .set_status(
            &object,
            1,
            vec![
                condition("Accepted", "true", ""),
                condition("Ready", "true", "Synced"),
            ],
            &app,
        )
        .await?;
    assert!(reply.written);
    let read = fx.get(KIND, "team-a/web", &fx.admin).await?;
    assert_eq!(read.summary, "Ready", "{read:?}");
    let status = read.status.expect("status");
    assert_eq!(status.observed_generation, 1);
    assert!(status.conditions.iter().all(|c| !c.since.is_empty()));
    let unchanged = fx
        .set_status(
            &object,
            1,
            vec![
                condition("Accepted", "true", ""),
                condition("Ready", "true", "Synced"),
            ],
            &app,
        )
        .await?;
    assert!(!unchanged.written);
    assert_code(
        fx.set_status(&object, 5, vec![condition("Ready", "true", "")], &app)
            .await,
        Code::Aborted,
        "generation",
    );
    // A spec change makes the status stale: Pending until the app catches up.
    fx.apply(
        repo(
            "web",
            "team-a",
            json!({ "url": "https://git/web", "branch": "dev", "deployKey": "s3cret" }),
        ),
        &fx.admin,
    )
    .await?;
    assert_eq!(
        fx.get(KIND, "team-a/web", &fx.admin).await?.summary,
        "Pending"
    );
    fx.set_status(
        &object,
        2,
        vec![condition("Accepted", "false", "NoSuchBranch")],
        &app,
    )
    .await?;
    assert_eq!(
        fx.get(KIND, "team-a/web", &fx.admin).await?.summary,
        "Rejected: NoSuchBranch"
    );

    // An object the app writes is managed: a person changes it with force.
    fx.apply(
        repo("made", "team-a", json!({ "url": "https://git/made" })),
        &app,
    )
    .await?;
    assert_eq!(
        fx.get(KIND, "team-a/made", &fx.admin).await?.managed_by,
        APP
    );
    assert_code(
        fx.apply(
            repo(
                "made",
                "team-a",
                json!({ "url": "https://git/made", "branch": "x" }),
            ),
            &fx.admin,
        )
        .await,
        Code::FailedPrecondition,
        "--force",
    );
    fx.apply_with(
        repo(
            "made",
            "team-a",
            json!({ "url": "https://git/made", "branch": "x" }),
        ),
        &fx.admin,
        |r| r.force = true,
    )
    .await?;

    // Deleting waits for the app; the watch sends the deleting object, and
    // the release removes it.
    let deleting = fx.delete("team-a/web", &fx.admin, |_| {}).await?;
    assert!(deleting.deleting);
    let mut saw_deleting = false;
    for _ in 0..20 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await?
            .context("an event")??;
        if event
            .upserts
            .iter()
            .any(|o| o.uid == object.uid && o.deleting)
        {
            saw_deleting = true;
            break;
        }
    }
    assert!(saw_deleting, "the watch should send the deleting object");
    assert_code(
        fx.svc
            .release_object(req(
                ReleaseObjectRequest {
                    kind: KIND.into(),
                    namespace: "team-a".into(),
                    name: "web".into(),
                    uid: object.uid + 1000,
                    idempotency_key: None,
                },
                &app,
            ))
            .await,
        Code::NotFound,
        "with uid",
    );
    fx.svc
        .release_object(req(
            ReleaseObjectRequest {
                kind: KIND.into(),
                namespace: "team-a".into(),
                name: "web".into(),
                uid: object.uid,
                idempotency_key: None,
            },
            &app,
        ))
        .await?;
    assert_code(
        fx.get(KIND, "team-a/web", &fx.admin).await,
        Code::NotFound,
        "",
    );
    let mut saw_tombstone = false;
    for _ in 0..20 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await?
            .context("an event")??;
        if event.deletes.iter().any(|gone| gone.uid == object.uid) {
            saw_tombstone = true;
            break;
        }
    }
    assert!(saw_tombstone, "the watch should send a tombstone");
    Ok(())
}

/// The app-plane calls, made as the app over the service, for the
/// framework's reconcile loop.
#[derive(Clone)]
struct ServicePlane {
    svc: Arc<ManagerDaemonService>,
    token: String,
}

#[tonic::async_trait]
impl ObjectPlane for ServicePlane {
    async fn watch(
        &self,
        request: WatchObjectsRequest,
    ) -> Result<BoxStream<'static, Result<ObjectEvent, Status>>, Status> {
        Ok(self
            .svc
            .watch_objects(req(request, &self.token))
            .await?
            .into_inner()
            .boxed())
    }

    async fn update_status(
        &self,
        request: UpdateObjectStatusRequest,
    ) -> Result<UpdateObjectStatusReply, Status> {
        Ok(self
            .svc
            .update_object_status(req(request, &self.token))
            .await?
            .into_inner())
    }

    async fn release(&self, request: ReleaseObjectRequest) -> Result<ReleaseObjectReply, Status> {
        Ok(self
            .svc
            .release_object(req(request, &self.token))
            .await?
            .into_inner())
    }
}

/// An object's summary and status fields, as a person reads them; `None`
/// once it is gone.
async fn summary_of(
    svc: &ManagerDaemonService,
    admin: &str,
    id: &str,
) -> Option<(String, Option<String>)> {
    let (namespace, name) = id.split_once('/').expect("namespace/name");
    svc.get_object(req(
        GetObjectRequest {
            kind: KIND.into(),
            name: name.into(),
            namespace: namespace.into(),
        },
        admin,
    ))
    .await
    .ok()
    .and_then(|reply| reply.into_inner().object)
    .map(|object| (object.summary, object.status.map(|s| s.fields_json)))
}

async fn eventually<F, Fut>(what: &str, check: F) -> R<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..150 {
        if check().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("timed out waiting for {what}")
}

/// The framework's loop against the manager: objects become Ready or
/// Rejected, a new generation is reconciled, and a deleted object is
/// cleaned up and released.
#[tokio::test(flavor = "multi_thread")]
async fn the_reconcile_loop_drives_objects_to_ready_test() -> R<()> {
    let fx = fixture().await?;
    let plane = ServicePlane {
        svc: fx.svc.clone(),
        token: fx.app_token()?,
    };
    fx.apply(
        repo("web", "team-a", json!({ "url": "https://git/web" })),
        &fx.admin,
    )
    .await?;
    fx.apply(
        repo("nope", "team-a", json!({ "url": "https://git/nope" })),
        &fx.admin,
    )
    .await?;
    let cleaned = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let seen = cleaned.clone();
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let task = Reconciler::new(APP, "Repo")
        .on_cleanup(move |request| {
            let seen = seen.clone();
            async move {
                seen.lock().expect("lock").push(request.object.id.clone());
                Ok(())
            }
        })
        .run(plane, stop, |request| async move {
            if request.spec["url"]
                .as_str()
                .unwrap_or_default()
                .ends_with("nope")
            {
                return Ok(ObjectOutcome::rejected("NotARepo", "no such repository"));
            }
            let branch = request.spec["branch"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            Ok(ObjectOutcome::ready().with_fields(json!({ "commit": format!("{branch}-1") })))
        });
    let (svc, admin) = (&*fx.svc, fx.admin.as_str());
    eventually("web Ready", || async move {
        summary_of(svc, admin, "team-a/web")
            .await
            .is_some_and(|(s, _)| s == "Ready")
    })
    .await?;
    eventually("nope Rejected", || async move {
        summary_of(svc, admin, "team-a/nope")
            .await
            .is_some_and(|(s, _)| s == "Rejected: NotARepo")
    })
    .await?;
    fx.apply(
        repo(
            "web",
            "team-a",
            json!({ "url": "https://git/web", "branch": "dev" }),
        ),
        &fx.admin,
    )
    .await?;
    eventually("web reconciled at generation 2", || async move {
        summary_of(svc, admin, "team-a/web")
            .await
            .is_some_and(|(s, fields)| s == "Ready" && fields.is_some_and(|f| f.contains("dev-1")))
    })
    .await?;
    fx.delete("team-a/web", &fx.admin, |_| {}).await?;
    eventually("web released", || async move {
        summary_of(svc, admin, "team-a/web").await.is_none()
    })
    .await?;
    assert_eq!(cleaned.lock().expect("lock").as_slice(), ["team-a/web"]);
    stop_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(10), task).await??;
    Ok(())
}

/// Lifecycle: an unknown kind is retryable; uninstall needs force and
/// orphans the objects, which stay readable and deletable but not
/// writable; reinstalling adopts them. A namespace holding an object is
/// deleted only with cascade.
#[tokio::test(flavor = "multi_thread")]
async fn kinds_follow_their_app_test() -> R<()> {
    let fx = fixture().await?;
    let unknown = fx
        .apply(
            json!({ "kind": "certs/Certificate", "name": "c", "spec": {} }),
            &fx.admin,
        )
        .await
        .expect_err("no such kind");
    assert_eq!(unknown.code(), Code::Unavailable, "{}", unknown.message());
    assert_eq!(
        unknown
            .metadata()
            .get("legion-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("KIND_NOT_INSTALLED")
    );
    assert_code(
        fx.apply(
            json!({ "kind": "Repo", "name": "r", "spec": {} }),
            &fx.admin,
        )
        .await,
        Code::InvalidArgument,
        "gitops/Repo",
    );

    fx.apply(
        repo("web", "team-a", json!({ "url": "https://git/web" })),
        &fx.admin,
    )
    .await?;

    // The namespace holds it.
    assert_code(
        fx.svc
            .delete_namespace(req(
                DeleteNamespaceRequest {
                    name: "team-a".into(),
                    ..Default::default()
                },
                &fx.admin,
            ))
            .await,
        Code::FailedPrecondition,
        "1 object",
    );

    // An upgrade dropping the kind needs force, and orphans the object.
    let upgrade = |force: bool| {
        fx.svc.upgrade_app(req(
            UpgradeAppRequest {
                name: APP.into(),
                manifest_json: manifest("2.0.0", false).to_string(),
                source_ref: "./app.json".into(),
                force,
                ..Default::default()
            },
            &fx.admin,
        ))
    };
    assert_code(upgrade(false).await, Code::FailedPrecondition, "--force");
    upgrade(true).await?;
    let orphan = fx.get(KIND, "team-a/web", &fx.admin).await?;
    assert_eq!(orphan.summary, "Orphaned");
    assert_code(
        fx.apply(
            repo(
                "web",
                "team-a",
                json!({ "url": "https://git/web", "branch": "x" }),
            ),
            &fx.admin,
        )
        .await,
        Code::FailedPrecondition,
        "orphaned",
    );

    // Declaring it again adopts the object.
    fx.svc
        .upgrade_app(req(
            UpgradeAppRequest {
                name: APP.into(),
                manifest_json: manifest("3.0.0", true).to_string(),
                source_ref: "./app.json".into(),
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    let adopted = fx.get(KIND, "team-a/web", &fx.admin).await?;
    assert_ne!(adopted.summary, "Orphaned");
    assert_eq!(adopted.uid, orphan.uid);

    // Uninstall: refused while objects exist, then orphans with force.
    let uninstall = |force: bool| {
        fx.svc.uninstall_app(req(
            UninstallAppRequest {
                name: APP.into(),
                idempotency_key: None,
                force,
            },
            &fx.admin,
        ))
    };
    assert_code(uninstall(false).await, Code::FailedPrecondition, "--force");
    let reply = uninstall(true).await?.into_inner();
    assert_eq!(reply.orphaned_objects, 1);
    assert_eq!(
        fx.get(KIND, "team-a/web", &fx.admin).await?.summary,
        "Orphaned"
    );

    // Cascade removes it with the namespace.
    fx.svc
        .delete_namespace(req(
            DeleteNamespaceRequest {
                name: "team-a".into(),
                cascade: true,
                ..Default::default()
            },
            &fx.admin,
        ))
        .await?;
    assert_code(
        fx.get(KIND, "team-a/web", &fx.admin).await,
        Code::NotFound,
        "",
    );
    Ok(())
}
