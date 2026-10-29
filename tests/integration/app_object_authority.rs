//! Object-scoped credentials: an app acting for one of its objects
//! (app-object-kinds plan §8, O3; docs/app-objects.md, "Acting for an
//! object").
//!
//! In process, against a manager service with ACLs and workload identity
//! on, a `gitops` app whose `Repo` kind requires job:write on the jobs a
//! Repo names in the namespace it targets, and whose own job:write is
//! `via = "objects"`:
//!
//! - an object-scoped call holds the Repo's recorded authority: it deploys
//!   the Repo's jobs, and is refused another namespace, another resource,
//!   and jobs the Repo's author was not checked for;
//! - it never exceeds the app's grant, even where the author's authority
//!   reaches further;
//! - another app, a management token, a person's token, and a call also
//!   acting for a viewer are refused; so are streams and the app plane;
//! - a wrong uid, a mismatched reference, and a deleted object are refused;
//!   a deleting object is not;
//! - a `via = "objects"` rule does nothing for the app's plain credential;
//! - an installer must hold `via` rules like any other;
//! - jobs written this way are stamped with the object, and a person's
//!   submit over one says who will write it back;
//! - `--authority-lapsed` finds Repos whose author lost a right or a token,
//!   while the app keeps acting within the recorded authority; re-applying
//!   re-authorizes.

#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::manager::acl::Authenticator;
use legion::manager::apps::{AppReconciler, ReconcilerSettings};
use legion::manager::auth_method::ControlPlaneIdentityMethod;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::{claims_for_allocation, WorkloadIdentityState};
use legion::storage::store::Store;
use legion::structs::app::BuildMarker;
use legion::structs::app_ui::{FOR_OBJECT_HEADER, ON_BEHALF_OF_HEADER};
use legion::structs::manager_server::Manager;
use legion::structs::workload_identity::{WorkloadIdentity, CONTROL_PLANE_IDENTITY_NAME};
use legion::structs::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Request, Status};

type R<T> = anyhow::Result<T>;

const ISSUER: &str = "https://legion.test:4747";
const MGR_NODE: &str = "mgr-1";
const NODE: &str = "node-1";
const APP: &str = "gitops";
const OTHER_APP: &str = "other";
const KIND: &str = "gitops/Repo";

fn req<T>(inner: T, token: &str) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("ascii token"),
    );
    request
}

/// `req`, acting for the object `reference` names
/// (`<app>/<Kind>/<ns>/<name>#<uid>@<generation>`).
fn for_object<T>(inner: T, token: &str, reference: &str) -> Request<T> {
    let mut request = req(inner, token);
    request.metadata_mut().insert(
        FOR_OBJECT_HEADER,
        reference.parse().expect("ascii reference"),
    );
    request
}

/// The reference that acts for `object`.
fn reference(object: &AppObject) -> String {
    format!(
        "{}/{}/{}/{}#{}@{}",
        object.app, object.kind, object.namespace, object.name, object.uid, object.generation
    )
}

/// An app's job: one exec task (as the app's own uid, so not host-level).
fn app_job() -> Value {
    json!({ "task": { "name": "sync", "runtime": "exec",
                      "config": { "command": ["sleep", "3600"] } } })
}

/// `gitops`: reads jobs in team-*, writes them there only for its Repos,
/// and keeps state in its own KV prefix. A Repo requires job:write on the
/// jobs it names in the namespace it targets.
fn gitops_manifest(name: &str) -> Value {
    json!({
        "name": name,
        "version": "1.0.0",
        "job": app_job(),
        "permissions": [
            { "resources": ["job"], "actions": ["read"], "namespaces": ["team-*"] },
            { "resources": ["job"], "actions": ["write"], "namespaces": ["team-*"],
              "via": "objects" },
            { "resources": ["kv"], "actions": ["read", "write"],
              "paths": [format!("legion/apps/{name}/*")] },
        ],
        "kinds": [{
            "name": "Repo",
            "schema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": { "type": "string" },
                    "targetNamespace": { "type": "string" },
                    "jobs": { "type": "array", "items": { "type": "string" } },
                },
            },
            "requires": [{ "resource": "job", "actions": ["write"],
                           "namespace": "/spec/targetNamespace", "name": "/spec/jobs" }],
            "cleanup": "10m",
        }],
    })
}

/// Another app, with a plain grant of the same shape and no kinds.
fn other_manifest() -> Value {
    json!({
        "name": OTHER_APP,
        "version": "1.0.0",
        "job": app_job(),
        "permissions": [
            { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["team-*"] },
        ],
    })
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

/// A job the Repos deploy; `version` tells its specs apart.
fn job(namespace: &str, name: &str, version: u64) -> Value {
    json!({
        "name": name,
        "namespace": namespace,
        "task": { "name": "main", "runtime": "container",
                  "config": { "image": "alpine", "command": ["sleep", "30"] },
                  "resources": { "cpu": 100 + version, "memory": 64 } },
    })
}

fn repo(name: &str, spec: Value) -> Value {
    json!({ "kind": KIND, "name": name, "namespace": "team-a", "spec": spec })
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

struct Fixture {
    svc: Arc<ManagerDaemonService>,
    store: Store,
    state: Arc<WorkloadIdentityState>,
    admin: String,
}

/// A person's token: its secret and accessor.
struct Person {
    secret: String,
    accessor: String,
}

impl Fixture {
    async fn policy(&self, name: &str, rules: Value) -> R<()> {
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
        Ok(())
    }

    async fn person(&self, name: &str, rules: Value) -> R<Person> {
        self.policy(name, rules).await?;
        let reply = self
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
            .into_inner();
        Ok(Person {
            secret: reply.secret,
            accessor: reply.token.context("token info")?.accessor_id,
        })
    }

    async fn install(&self, manifest: &Value, token: &str) -> Result<InstallAppReply, Status> {
        self.svc
            .install_app(req(
                InstallAppRequest {
                    manifest_json: manifest.to_string(),
                    source: "file".into(),
                    source_ref: "./app.json".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(tonic::Response::into_inner)
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

    /// Reconcile app `name`'s job, place it, and report it healthy.
    async fn start_app(&self, name: &str) -> R<()> {
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
        let outcome = reconciler.reconcile_app(name).await;
        assert!(
            matches!(
                outcome,
                legion::manager::apps::PassOutcome::Submitted { .. }
            ),
            "{outcome:?}"
        );
        let job = format!("legion-system/{name}");
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

    /// The `legion` token the manager would issue app `name`'s allocation.
    fn app_token(&self, name: &str) -> R<String> {
        let job = format!("legion-system/{name}");
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

    async fn apply(&self, object: Value, token: &str) -> R<AppObject> {
        self.svc
            .apply_object(req(
                ApplyObjectRequest {
                    object_json: object.to_string(),
                    ..Default::default()
                },
                token,
            ))
            .await?
            .into_inner()
            .object
            .context("the applied object")
    }

    /// RunJob as `request` says (a plain or an object-scoped request).
    async fn run(&self, request: Request<RunJobRequest>) -> Result<RunJobReply, Status> {
        self.svc
            .run_job(request)
            .await
            .map(tonic::Response::into_inner)
    }

    fn submit(spec: &Value) -> RunJobRequest {
        RunJobRequest {
            parsed_jobspec: spec.to_string(),
            ..Default::default()
        }
    }

    async fn managed_by(&self, job: &str) -> R<Option<ManagedByInfo>> {
        Ok(self
            .svc
            .get_job_info(req(GetJobInfoRequest { name: job.into() }, &self.admin))
            .await?
            .into_inner()
            .managed_by)
    }

    async fn lapsed(&self, token: &str) -> Result<Vec<AppObject>, Status> {
        self.svc
            .list_objects(req(
                ListObjectsRequest {
                    kind: KIND.into(),
                    namespaces: vec!["*".into()],
                    authority_lapsed: true,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|reply| reply.into_inner().objects)
    }
}

async fn fixture() -> R<Fixture> {
    let dir = tempfile::Builder::new()
        .prefix("legion-object-authority-")
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
    let svc = Arc::new(
        ManagerDaemonService::with_acl(store.clone(), auth.clone())
            .with_workload_identity(Some(state.clone())),
    );
    let fx = Fixture {
        svc,
        store,
        state,
        admin,
    };
    for namespace in ["team-a", "team-b", "other"] {
        fx.namespace(namespace).await?;
    }
    fx.install(&gitops_manifest(APP), &fx.admin.clone()).await?;
    fx.start_app(APP).await?;
    fx.install(&other_manifest(), &fx.admin.clone()).await?;
    fx.start_app(OTHER_APP).await?;
    Ok(fx)
}

/// Alice may write Repos in team-a, and jobs in team-a, team-b, and other.
fn alice_rules() -> Value {
    json!([
        { "resources": ["object"], "actions": ["read", "write"], "namespaces": ["team-a"],
          "kinds": [KIND] },
        { "resources": ["job"], "actions": ["read", "write"],
          "namespaces": ["team-a", "team-b", "other"] },
    ])
}

/// What an object-scoped call holds, and where it stops: the Repo's
/// authority within the app's grant, never the app's plain grant, never
/// another app's object; a wrong or deleted object; streams and the app
/// plane; both headers at once.
#[tokio::test(flavor = "multi_thread")]
async fn acting_for_an_object_holds_its_authority_test() -> R<()> {
    let fx = fixture().await?;
    let alice = fx.person("alice", alice_rules()).await?;
    let gitops = fx.app_token(APP)?;
    let storefront = fx
        .apply(
            repo(
                "storefront",
                json!({ "url": "https://git.example/storefront", "targetNamespace": "team-a",
                        "jobs": ["storefront", "storefront-worker"] }),
            ),
            &alice.secret,
        )
        .await?;
    let authority = storefront.authority.clone().context("authority")?;
    assert_eq!(authority.authorized_by, "alice");
    let acting = reference(&storefront);

    // 1. Within the authority: the Repo's jobs in team-a.
    let reply = fx
        .run(for_object(
            Fixture::submit(&job("team-a", "storefront", 1)),
            &gitops,
            &acting,
        ))
        .await?;
    assert!(
        !reply.warnings.iter().any(|w| w.contains("managed by")),
        "{:?}",
        reply.warnings
    );
    fx.run(for_object(
        Fixture::submit(&job("team-a", "storefront-worker", 1)),
        &gitops,
        &acting,
    ))
    .await?;
    // Outside it: a job the Repo does not name, another namespace, another
    // resource (the app's own KV prefix, which its plain grant reaches),
    // and a read the authority does not include.
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "billing", 1)),
            &gitops,
            &acting,
        ))
        .await,
        Code::PermissionDenied,
        "outside what gitops for Repo team-a/storefront holds",
    );
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-b", "storefront", 1)),
            &gitops,
            &acting,
        ))
        .await,
        Code::PermissionDenied,
        "gitops for Repo team-a/storefront",
    );
    assert_code(
        fx.svc
            .put_kv(for_object(
                PutKvRequest {
                    key: "legion/apps/gitops/state".into(),
                    value: "{}".into(),
                    value_type: KvValueType::String.into(),
                    ..Default::default()
                },
                &gitops,
                &acting,
            ))
            .await,
        Code::PermissionDenied,
        "kv:write",
    );
    fx.svc
        .put_kv(req(
            PutKvRequest {
                key: "legion/apps/gitops/state".into(),
                value: "{}".into(),
                value_type: KvValueType::String.into(),
                ..Default::default()
            },
            &gitops,
        ))
        .await
        .context("the app's own credential reaches its KV prefix")?;
    assert_code(
        fx.svc
            .get_job_info(for_object(
                GetJobInfoRequest {
                    name: "team-a/storefront".into(),
                },
                &gitops,
                &acting,
            ))
            .await,
        Code::PermissionDenied,
        "job:read",
    );
    // WhoAmI through the same client says who it is and what it holds.
    let who = fx
        .svc
        .who_am_i(for_object(WhoAmIRequest {}, &gitops, &acting))
        .await?
        .into_inner();
    assert_eq!(who.source, "for-object");
    assert_eq!(who.principal, "gitops for Repo team-a/storefront");
    assert!(who.policies.is_empty());

    // 5. The app's `via = "objects"` job:write does nothing for its plain
    // credential.
    assert_code(
        fx.run(req(
            Fixture::submit(&job("team-a", "storefront", 2)),
            &gitops,
        ))
        .await,
        Code::PermissionDenied,
        "job:write",
    );

    // 2. Never beyond the app's grant: alice may write jobs in `other`, and
    // a Repo targeting it records that, but gitops writes jobs only in
    // team-*.
    let wide = fx
        .apply(
            repo(
                "wide",
                json!({ "url": "https://git.example/wide", "targetNamespace": "other" }),
            ),
            &alice.secret,
        )
        .await?;
    assert!(
        wide.authority
            .as_ref()
            .is_some_and(|a| a.permissions.iter().any(|p| p.contains("in other"))),
        "{:?}",
        wide.authority
    );
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("other", "anything", 1)),
            &gitops,
            &reference(&wide),
        ))
        .await,
        Code::PermissionDenied,
        "gitops for Repo team-a/wide",
    );

    // 3. Only the owning app's own identity names an object.
    let other = fx.app_token(OTHER_APP)?;
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "storefront", 3)),
            &other,
            &acting,
        ))
        .await,
        Code::PermissionDenied,
        "the other app acts only for its own objects",
    );
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "storefront", 3)),
            &fx.admin,
            &acting,
        ))
        .await,
        Code::PermissionDenied,
        "is not an app's",
    );
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "storefront", 3)),
            &alice.secret,
            &acting,
        ))
        .await,
        Code::PermissionDenied,
        "is not an app's",
    );
    assert_code(
        fx.run(Request::from_parts(
            {
                let mut metadata = tonic::metadata::MetadataMap::new();
                metadata.insert(FOR_OBJECT_HEADER, acting.parse()?);
                metadata
            },
            Default::default(),
            Fixture::submit(&job("team-a", "storefront", 3)),
        ))
        .await,
        Code::Unauthenticated,
        "has none",
    );
    // 9. A call acting for a viewer too (a delegated identity naming an
    // object) is refused before either is checked.
    let mut both = for_object(
        Fixture::submit(&job("team-a", "storefront", 3)),
        &gitops,
        &acting,
    );
    both.metadata_mut()
        .insert(ON_BEHALF_OF_HEADER, "a.b.c".parse()?);
    assert_code(fx.run(both).await, Code::InvalidArgument, "not both");

    // Streams and the app plane take the app's own credential.
    assert_code(
        fx.svc
            .watch_objects(for_object(WatchObjectsRequest::default(), &gitops, &acting))
            .await,
        Code::PermissionDenied,
        "legion-for-object",
    );
    assert_code(
        fx.svc
            .update_object_status(for_object(
                UpdateObjectStatusRequest {
                    kind: KIND.into(),
                    namespace: "team-a".into(),
                    name: "storefront".into(),
                    uid: storefront.uid,
                    observed_generation: 1,
                    ..Default::default()
                },
                &gitops,
                &acting,
            ))
            .await,
        Code::PermissionDenied,
        "legion-for-object",
    );

    // 4. A wrong uid, a reference that does not match its uid, a malformed
    // one, a deleted object.
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "storefront", 3)),
            &gitops,
            &format!("gitops/Repo/team-a/storefront#{}@1", storefront.uid + 1000),
        ))
        .await,
        Code::PermissionDenied,
        "no object has uid",
    );
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "storefront", 3)),
            &gitops,
            &format!("gitops/Repo/team-a/billing#{}@1", storefront.uid),
        ))
        .await,
        Code::PermissionDenied,
        "not gitops/Repo/team-a/billing",
    );
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "storefront", 3)),
            &gitops,
            "gitops/Repo/storefront",
        ))
        .await,
        Code::Unauthenticated,
        "legion-for-object",
    );
    // A deleting object (its cleanup window open) still lets the app act
    // for it; a deleted one does not.
    let leaving = fx
        .apply(
            repo(
                "leaving",
                json!({ "url": "https://git.example/leaving", "targetNamespace": "team-a",
                        "jobs": ["leaving"] }),
            ),
            &alice.secret,
        )
        .await?;
    let doomed = fx
        .apply(
            repo(
                "doomed",
                json!({ "url": "https://git.example/doomed", "targetNamespace": "team-a",
                        "jobs": ["doomed"] }),
            ),
            &alice.secret,
        )
        .await?;
    for (name, now) in [("leaving", false), ("doomed", true)] {
        fx.svc
            .delete_object(req(
                DeleteObjectRequest {
                    kind: KIND.into(),
                    name: name.into(),
                    namespace: "team-a".into(),
                    now,
                    ..Default::default()
                },
                &alice.secret,
            ))
            .await?;
    }
    fx.run(for_object(
        Fixture::submit(&job("team-a", "leaving", 1)),
        &gitops,
        &reference(&leaving),
    ))
    .await
    .context("a deleting object is acted for (its cleanup)")?;
    assert_code(
        fx.run(for_object(
            Fixture::submit(&job("team-a", "doomed", 1)),
            &gitops,
            &reference(&doomed),
        ))
        .await,
        Code::PermissionDenied,
        "no object has uid",
    );

    // The generation is advisory: a stale one acts with the current
    // authority.
    let updated = fx
        .apply(
            repo(
                "storefront",
                json!({ "url": "https://git.example/storefront", "targetNamespace": "team-a",
                        "jobs": ["storefront", "storefront-worker", "storefront-cron"] }),
            ),
            &alice.secret,
        )
        .await?;
    assert_eq!(updated.generation, storefront.generation + 1);
    fx.run(for_object(
        Fixture::submit(&job("team-a", "storefront-cron", 1)),
        &gitops,
        &acting,
    ))
    .await
    .context("a stale generation acts with the current authority")?;
    Ok(())
}

/// 7. Jobs written for an object are stamped with it; a person's submit
/// over one replaces the stamp and says who will write it back.
#[tokio::test(flavor = "multi_thread")]
async fn jobs_written_for_an_object_are_stamped_test() -> R<()> {
    let fx = fixture().await?;
    let alice = fx.person("alice", alice_rules()).await?;
    let gitops = fx.app_token(APP)?;
    let storefront = fx
        .apply(
            repo(
                "storefront",
                json!({ "url": "https://git.example/storefront", "targetNamespace": "team-a",
                        "jobs": ["storefront"] }),
            ),
            &alice.secret,
        )
        .await?;
    fx.run(for_object(
        Fixture::submit(&job("team-a", "storefront", 1)),
        &gitops,
        &reference(&storefront),
    ))
    .await?;
    let stamp = fx
        .managed_by("team-a/storefront")
        .await?
        .context("stamped")?;
    assert_eq!(stamp.app, APP);
    assert_eq!(
        stamp.object,
        format!("gitops/Repo/team-a/storefront#{}", storefront.uid)
    );
    assert!(stamp.digest.is_empty());

    // An unchanged resubmit by the app keeps the stamp; alice's changed
    // submit replaces it, with a warning naming who will write it back.
    fx.run(for_object(
        Fixture::submit(&job("team-a", "storefront", 1)),
        &gitops,
        &reference(&storefront),
    ))
    .await?;
    let reply = fx
        .run(req(
            Fixture::submit(&job("team-a", "storefront", 99)),
            &alice.secret,
        ))
        .await?;
    assert!(
        reply
            .warnings
            .iter()
            .any(|w| w.contains("was managed by gitops for Repo team-a/storefront")),
        "{:?}",
        reply.warnings
    );
    assert!(fx.managed_by("team-a/storefront").await?.is_none());
    // The app writing it back stamps it again.
    fx.run(for_object(
        Fixture::submit(&job("team-a", "storefront", 1)),
        &gitops,
        &reference(&storefront),
    ))
    .await?;
    assert!(fx
        .managed_by("team-a/storefront")
        .await?
        .is_some_and(|stamp| !stamp.object.is_empty()));
    Ok(())
}

/// 6. Installing an app that asks for a `via = "objects"` rule needs the
/// installer to hold it, like any other permission.
#[tokio::test(flavor = "multi_thread")]
async fn installers_must_hold_via_rules_test() -> R<()> {
    let fx = fixture().await?;
    let reader = fx
        .person(
            "reader",
            json!([
                { "resources": ["app"], "actions": ["read", "write"] },
                { "resources": ["job"], "actions": ["read"], "namespaces": ["*"] },
                { "resources": ["kv"], "actions": ["read", "write"], "paths": ["legion/apps/*"] },
            ]),
        )
        .await?;
    assert_code(
        fx.install(&gitops_manifest("gitops2"), &reader.secret)
            .await,
        Code::PermissionDenied,
        "job:write in team-* (via objects)",
    );
    let writer = fx
        .person(
            "writer",
            json!([
                { "resources": ["app"], "actions": ["read", "write"] },
                { "resources": ["job"], "actions": ["read", "write"], "namespaces": ["*"] },
                { "resources": ["kv"], "actions": ["read", "write"], "paths": ["legion/apps/*"] },
            ]),
        )
        .await?;
    let installed = fx
        .install(&gitops_manifest("gitops2"), &writer.secret)
        .await?;
    assert!(
        installed
            .prompt
            .iter()
            .any(|line| line.contains("only for its objects")),
        "{:?}",
        installed.prompt
    );
    // A stored policy may not carry `via`: no token ever acts for an object.
    assert_code(
        fx.policy(
            "via-policy",
            json!([{ "resources": ["job"], "actions": ["write"], "namespaces": ["*"],
                     "via": "objects" }]),
        )
        .await
        .map_err(|err| {
            err.downcast::<Status>()
                .unwrap_or_else(|err| Status::unknown(err.to_string()))
        }),
        Code::InvalidArgument,
        "app manifest's permissions",
    );
    Ok(())
}

/// 8. Authority is recorded, not revoked: after alice loses a right, or
/// her token, gitops keeps acting within what she authorized, and
/// `--authority-lapsed` finds those Repos; re-applying re-authorizes.
#[tokio::test(flavor = "multi_thread")]
async fn lapsed_authority_is_listed_not_revoked_test() -> R<()> {
    let fx = fixture().await?;
    let alice = fx.person("alice", alice_rules()).await?;
    let bob = fx.person("bob", alice_rules()).await?;
    let gitops = fx.app_token(APP)?;
    let spec = |name: &str| {
        json!({ "url": format!("https://git.example/{name}"), "targetNamespace": "team-a",
                "jobs": [name] })
    };
    let storefront = fx
        .apply(repo("storefront", spec("storefront")), &alice.secret)
        .await?;
    let billing = fx
        .apply(repo("billing", spec("billing")), &bob.secret)
        .await?;
    assert!(fx.lapsed(&fx.admin).await?.is_empty());
    // Reading what others hold is ACL state.
    assert_code(
        fx.lapsed(&alice.secret).await,
        Code::PermissionDenied,
        "acl:read",
    );

    // Alice loses job:write in team-a; bob's token goes.
    fx.policy(
        "alice",
        json!([
            { "resources": ["object"], "actions": ["read", "write"], "namespaces": ["team-a"],
              "kinds": [KIND] },
            { "resources": ["job"], "actions": ["read"], "namespaces": ["team-a"] },
        ]),
    )
    .await?;
    fx.svc
        .delete_acl_token(req(
            DeleteAclTokenRequest {
                accessor_id: bob.accessor.clone(),
            },
            &fx.admin,
        ))
        .await?;
    let lapsed = fx.lapsed(&fx.admin).await?;
    let check = |name: &str| {
        lapsed
            .iter()
            .find(|object| object.name == name)
            .and_then(|object| object.authority.clone())
            .map(|authority| (authority.check, authority.check_detail))
    };
    let (state, detail) = check("storefront").context("storefront lapsed")?;
    assert_eq!(state, "lapsed");
    assert!(
        detail.contains("alice no longer holds job:write in team-a on storefront"),
        "{detail}"
    );
    let (state, detail) = check("billing").context("billing lapsed")?;
    assert_eq!(state, "lapsed");
    assert!(detail.contains("was deleted"), "{detail}");

    // Not revoked: gitops still deploys both within what was recorded.
    for object in [&storefront, &billing] {
        fx.run(for_object(
            Fixture::submit(&job("team-a", &object.name, 1)),
            &gitops,
            &reference(object),
        ))
        .await
        .with_context(|| format!("{} still acts with its recorded authority", object.name))?;
    }

    // Re-applying as someone who holds it re-authorizes.
    let reapplied = fx
        .apply(repo("storefront", spec("storefront")), &fx.admin)
        .await?;
    assert_ne!(
        reapplied.authority.context("authority")?.authorized_by,
        "alice"
    );
    let lapsed = fx.lapsed(&fx.admin).await?;
    let names: Vec<&str> = lapsed.iter().map(|object| object.name.as_str()).collect();
    assert_eq!(names, vec!["billing"]);
    Ok(())
}
