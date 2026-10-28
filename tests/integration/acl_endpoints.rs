//! ACL coverage for every Manager RPC.
//!
//! Every endpoint on the Manager service must enforce ACLs (or be explicitly
//! classified as worker-plane or open). This suite drives that as a hard
//! gate:
//!
//! 1. `every_manager_rpc_is_classified` parses `proto/legion.proto` and fails
//!    when an RPC is added without an entry in [`endpoint_table`] — so a new
//!    endpoint cannot ship without deciding (and testing) its ACL behavior.
//! 2. The deny/allow matrix calls every endpoint three ways: anonymous with
//!    ACLs enabled (protected endpoints must return Unauthenticated), with a
//!    management token (must never fail with an auth error), and with ACLs
//!    disabled (must never fail with an auth error).
//! 3. Scenario tests prove scoped policies behave correctly end-to-end
//!    (job-name scoping, KV path scoping, node-pool filtering, audit mode).
//!
//! When you add an RPC: add it to `endpoint_table` with a request builder
//! that references the seeded fixtures, and a one-line ACL check in the
//! handler. See AGENTS.md ("Auth & ACL checks").

// Helpers return `tonic::Status` as the RPCs do (see the allow in src/lib.rs).
#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use legion::manager::acl::Authenticator;
use legion::manager::server::ManagerDaemonService;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::*;
use std::collections::BTreeSet;
use std::sync::Arc;
use tonic::{Code, Request};

type R<T> = anyhow::Result<T>;

const NODE_ID: &str = "acl-node";
const JOB_NAME: &str = "acl-job";
const TEMPLATE_NAME: &str = "acl-template";
const TEMPLATE_JOB_NAME: &str = "acl-template-job";
const KV_KEY: &str = "team/config";
const FIXTURE_POLICY: &str = "fixture-policy";
const DELETABLE_POLICY: &str = "deletable-policy";
const FIXTURE_NET_POLICY: &str = "fixture-net-policy";
const DELETABLE_NET_POLICY: &str = "deletable-net-policy";
const FIXTURE_NET_PLUGIN: &str = "fixture-net-plugin";
const DELETABLE_NET_PLUGIN: &str = "deletable-net-plugin";
const DELETABLE_USER: &str = "deletable-user";
const FIXTURE_NAMESPACE: &str = "acl-ns";
const DELETABLE_NAMESPACE: &str = "deletable-ns";

/// How an endpoint participates in the ACL system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plane {
    /// Requires a policy granting the named capability; anonymous is denied.
    Protected(&'static str),
    /// Credential exchange / introspection; must work without a token.
    Open,
    /// Worker control plane: requires a join token (registration) or a node
    /// token (heartbeats/health) when ACLs are enforced; anonymous callers
    /// are rejected as Unauthenticated.
    Worker,
}

struct Fixtures {
    job_config_id: String,
    deletable_token_accessor: String,
    join_token_id: String,
}

/// One row per Manager RPC: proto method name, plane, request builder.
fn endpoint_table() -> Vec<(&'static str, Plane)> {
    vec![
        ("WorkerPreRegister", Plane::Worker),
        ("WorkerRegister", Plane::Worker),
        ("Heartbeat", Plane::Worker),
        ("StreamWork", Plane::Worker),
        ("HealthCheck", Plane::Worker),
        ("RunJob", Plane::Protected("job:write")),
        ("GetJobSpec", Plane::Protected("job:read")),
        ("GetJobSpecByConfig", Plane::Protected("job:read")),
        ("ListJobSpecHistory", Plane::Protected("job:read")),
        ("GetJobInfo", Plane::Protected("job:read")),
        ("ListRollouts", Plane::Protected("job:read")),
        ("GetRollout", Plane::Protected("job:read")),
        ("GetRolloutStatus", Plane::Protected("job:read")),
        ("ListRolloutEvents", Plane::Protected("job:read")),
        ("AdvanceRollout", Plane::Protected("job:write")),
        ("StopRollout", Plane::Protected("job:write")),
        ("PlanJob", Plane::Protected("job:read")),
        ("StopJob", Plane::Protected("job:write")),
        // The `scale` resource is deliberately separate from `job`
        // (docs/autoscaling.md): resize rights without redeploy rights.
        ("ScaleTask", Plane::Protected("scale:write")),
        ("GetJobScaleStatus", Plane::Protected("scale:read")),
        ("GetScalingInputs", Plane::Protected("scale:read")),
        ("DestroyJob", Plane::Protected("job:write")),
        ("ListJobs", Plane::Protected("job:read")),
        // Namespaces (docs/namespaces.md). ListNamespaces filters per item
        // and is denied outright only to a caller that reaches none.
        (
            "ListNamespaces",
            Plane::Protected("namespace:read (per item)"),
        ),
        ("GetNamespace", Plane::Protected("namespace:read")),
        ("ApplyNamespace", Plane::Protected("namespace:write")),
        ("DeleteNamespace", Plane::Protected("namespace:write")),
        // Namespace policies are governance: ACL-policy privilege.
        ("ListNamespacePolicies", Plane::Protected("acl:read")),
        ("GetNamespacePolicy", Plane::Protected("acl:read")),
        ("ApplyNamespacePolicy", Plane::Protected("acl:write")),
        ("DeleteNamespacePolicy", Plane::Protected("acl:write")),
        ("UpsertJobTemplate", Plane::Protected("template:write")),
        ("ListJobTemplates", Plane::Protected("template:read")),
        ("GetJobTemplate", Plane::Protected("template:read")),
        ("RenderJobTemplate", Plane::Protected("job:write")),
        ("RunJobTemplate", Plane::Protected("job:write")),
        ("DeleteJobTemplate", Plane::Protected("template:write")),
        ("ListServices", Plane::Protected("job:read")),
        ("ListServiceEdges", Plane::Protected("job:read")),
        ("ResolveService", Plane::Protected("job:read")),
        ("UpsertNetPolicy", Plane::Protected("net:write")),
        ("GetNetPolicyObject", Plane::Protected("net:read")),
        ("ListNetPolicies", Plane::Protected("net:read")),
        ("DeleteNetPolicy", Plane::Protected("net:write")),
        ("UpsertNetPlugin", Plane::Protected("net:write")),
        ("GetNetPlugin", Plane::Protected("net:read")),
        ("ListNetPlugins", Plane::Protected("net:read")),
        ("DeleteNetPlugin", Plane::Protected("net:write")),
        ("GetNetPluginStatus", Plane::Protected("net:read")),
        ("ShowNetPolicy", Plane::Protected("net:read")),
        ("TestNetPolicy", Plane::Protected("net:read")),
        ("GetMeshCa", Plane::Protected("net:read")),
        ("RotateMeshCa", Plane::Protected("net:write")),
        ("FetchDatapathMaterial", Plane::Worker),
        ("GetNodes", Plane::Protected("node:read")),
        ("GetHeartbeatCadence", Plane::Protected("node:read")),
        ("GetNodeUsage", Plane::Protected("node:read")),
        ("ListNodeUsage", Plane::Protected("node:read")),
        ("GetNodeContent", Plane::Protected("node:read")),
        ("GetNode", Plane::Protected("node:read")),
        ("RestartWorker", Plane::Protected("node:write")),
        ("SetNodeAvailability", Plane::Protected("node:write")),
        ("GetLedger", Plane::Protected("node:read")),
        ("UpdateNodeDynamic", Plane::Protected("node:write")),
        ("GetManagerConfigs", Plane::Protected("operator:read")),
        ("ListManagers", Plane::Protected("operator:read")),
        ("GetObservability", Plane::Protected("operator:read")),
        ("GetSchedulerConcurrency", Plane::Protected("operator:read")),
        ("GetRaftStatus", Plane::Protected("operator:read")),
        ("ListRaftHistory", Plane::Protected("operator:read")),
        ("IssueRaftCommand", Plane::Protected("operator:write")),
        ("GetRaftJoinToken", Plane::Protected("operator:write")),
        ("GetStateBackup", Plane::Protected("operator:write")),
        // KV encryption keyring (docs/kv-encryption.md): `operator`, like
        // raft and manager config — keys are cluster plumbing, not KV data.
        ("GetKeyringStatus", Plane::Protected("operator:read")),
        ("RotateDataKey", Plane::Protected("operator:write")),
        ("RewrapKv", Plane::Protected("operator:write")),
        ("RewrapRootKey", Plane::Protected("operator:write")),
        ("ResetKeyring", Plane::Protected("operator:write")),
        ("PutKv", Plane::Protected("kv:write")),
        ("GetKv", Plane::Protected("kv:read")),
        ("ListKv", Plane::Protected("kv:read")),
        ("DeleteKv", Plane::Protected("kv:write")),
        ("PatchKv", Plane::Protected("kv:write")),
        ("AcquireKvLock", Plane::Protected("kv:write")),
        ("RenewKvLock", Plane::Protected("kv:write")),
        ("ReleaseKvLock", Plane::Protected("kv:write")),
        ("GetKvLock", Plane::Protected("kv:read")),
        ("QueryStateStore", Plane::Protected("state:read")),
        ("GetStateStoreSchema", Plane::Protected("state:read")),
        ("ListSavedStateQueries", Plane::Protected("state:read")),
        ("UpsertSavedStateQuery", Plane::Protected("state:write")),
        ("DeleteSavedStateQuery", Plane::Protected("state:write")),
        ("CreateInstance", Plane::Protected("job:write")),
        ("CreateInstances", Plane::Protected("job:write")),
        ("DestroyInstance", Plane::Protected("job:write")),
        ("StopInstance", Plane::Protected("job:write")),
        ("StartInstance", Plane::Protected("job:write")),
        // SuspendInstance also needs `snapshot:write` for a memory checkpoint.
        ("SuspendInstance", Plane::Protected("job:write")),
        // Phase 6: ForkInstance also needs `snapshot:write` (a fork reads
        // everything the origin held in memory); the coarse gate it reports
        // first is `job:write`.
        ("ForkInstance", Plane::Protected("job:write")),
        ("ReplaceInstance", Plane::Protected("job:write")),
        ("ListInstances", Plane::Protected("job:read")),
        ("GetInstance", Plane::Protected("job:read")),
        // The session's job is known only after the index lookup, so the
        // coarse gate is `job:write` on any job.
        ("PromoteInstance", Plane::Protected("job:write")),
        ("ListSessionNodes", Plane::Protected("job:read")),
        // Snapshots are their own resource: a memory snapshot holds the
        // workload's in-memory secrets, which `job:write` must not imply.
        // RestoreInstance also needs `job:write` (it replaces an allocation);
        // the coarse gate it reports first is `snapshot:read`.
        ("CreateSnapshot", Plane::Protected("snapshot:write")),
        ("ListSnapshots", Plane::Protected("snapshot:read")),
        ("GetSnapshot", Plane::Protected("snapshot:read")),
        ("DeleteSnapshot", Plane::Protected("snapshot:write")),
        ("RestoreInstance", Plane::Protected("snapshot:read")),
        // Snapshot names.
        ("SetSnapshotAlias", Plane::Protected("snapshot:write")),
        ("DeleteSnapshotAlias", Plane::Protected("snapshot:write")),
        // Warming only reads the snapshot (its published parts).
        ("WarmSnapshot", Plane::Protected("snapshot:read")),
        // Volumes are job-scoped storage, so they read/write under `job`.
        // ListVolumePlugins is cluster capability data, like node fingerprints.
        ("ListVolumes", Plane::Protected("job:read")),
        ("GetVolume", Plane::Protected("job:read")),
        ("DeleteVolume", Plane::Protected("job:write")),
        ("ListVolumeDrivers", Plane::Protected("node:read")),
        ("IssueWorkloadIdentityToken", Plane::Worker),
        ("IssueNodeIdentityToken", Plane::Worker),
        ("IssueNodeCertificate", Plane::Worker),
        ("GetAllocationLogs", Plane::Protected("job:read")),
        ("ExecAllocation", Plane::Protected("exec:write")),
        // Direct exec: the same coarse gate as ExecAllocation, then its
        // precise job scope once the target is resolved.
        ("MintExecTicket", Plane::Protected("exec:write")),
        // Delegated exec minting keys are credentials: the acl resource.
        ("CreateExecKey", Plane::Protected("acl:write")),
        ("ListExecKeys", Plane::Protected("acl:read")),
        ("DeleteExecKey", Plane::Protected("acl:write")),
        ("ListJobActions", Plane::Protected("job:read")),
        ("GetArtifactCredential", Plane::Worker),
        ("ListEvents", Plane::Protected("operator:read")),
        ("StreamEvents", Plane::Protected("operator:read")),
        ("WhoAmI", Plane::Open),
        ("Login", Plane::Open),
        ("GetAuthMethods", Plane::Open),
        ("CreateJoinToken", Plane::Protected("node:write")),
        ("ListJoinTokens", Plane::Protected("node:read")),
        ("DeleteJoinToken", Plane::Protected("node:write")),
        // The `boot` resource is deliberately separate from `job`
        // (docs/edge-boot.md): a platform's ticket minter authorizes
        // individual starts without holding the right to redeploy the fleet.
        ("IssueBootTicket", Plane::Protected("boot:write")),
        ("ListBootTickets", Plane::Protected("boot:read")),
        // Worker plane: the node that started the work asks the manager to
        // commit it, authenticated with its node token.
        ("RatifyClaim", Plane::Worker),
        ("UpsertAclPolicy", Plane::Protected("acl:write")),
        ("GetAclPolicy", Plane::Protected("acl:read")),
        ("ListAclPolicies", Plane::Protected("acl:read")),
        ("DeleteAclPolicy", Plane::Protected("acl:write")),
        ("CreateAclToken", Plane::Protected("acl:write")),
        ("ListAclTokens", Plane::Protected("acl:read")),
        ("DeleteAclToken", Plane::Protected("acl:write")),
        ("UpsertAclUser", Plane::Protected("acl:write")),
        ("ListAclUsers", Plane::Protected("acl:read")),
        ("DeleteAclUser", Plane::Protected("acl:write")),
        // Job grants (docs/workload-identity.md, "Permissions"): ACL state.
        ("ListGrants", Plane::Protected("acl:read")),
        ("RevokeGrant", Plane::Protected("acl:write")),
        // Apps (docs/apps.md): the cluster-scoped `app` resource. Install,
        // upgrade, and code-field config changes also run the grant check.
        ("ListApps", Plane::Protected("app:read")),
        ("GetApp", Plane::Protected("app:read")),
        ("InstallApp", Plane::Protected("app:write")),
        ("UpgradeApp", Plane::Protected("app:write")),
        ("UninstallApp", Plane::Protected("app:write")),
        ("SetAppEnabled", Plane::Protected("app:write")),
        ("UpdateAppSettings", Plane::Protected("app:write")),
        ("SetAppConfig", Plane::Protected("app:write")),
    ]
}

const FIXTURE_APP: &str = "acl-app";
const DELETABLE_APP: &str = "deletable-app";

/// A minimal third-party app manifest, as JSON.
fn app_manifest_json(name: &str, version: &str) -> String {
    serde_json::json!({
        "name": name,
        "version": version,
        "job": { "task": { "name": "main", "runtime": "noop", "config": {} } },
    })
    .to_string()
}

/// An installed (file) app record for the fixtures.
fn app_fixture(name: &str) -> R<legion::structs::app::AppRecord> {
    use legion::structs::app::*;
    let manifest =
        AppManifest::from_json(&app_manifest_json(name, "1.0.0")).map_err(|e| anyhow!(e))?;
    Ok(AppRecord {
        name: name.into(),
        source: AppSource::File,
        source_ref: "./app.json".into(),
        manifest_digest: manifest.digest(),
        enabled: true,
        settings: AppSettings::from_manifest(&manifest),
        config: serde_json::json!({}),
        grant: AppGrant {
            rules: Vec::new(),
            granted_by: "fixture".into(),
            granted_at: "2026-01-01T00:00:00Z".into(),
        },
        installed_by: "fixture".into(),
        installed_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        state: AppReconcileState::default(),
        manifest,
    })
}

/// An empty client-streaming request body, for exercising the ACL gate of
/// `ExecAllocation` (which authorizes before reading any frame).
fn empty_exec_stream() -> tonic::Streaming<ExecClientFrame> {
    use tonic::codec::{Codec, ProstCodec};
    let mut codec: ProstCodec<ExecClientFrame, ExecClientFrame> = ProstCodec::default();
    tonic::Streaming::new_empty(codec.decoder(), tonic::body::empty_body())
}

/// An empty client-streaming body for `StreamWork`, which authenticates
/// the caller before it reads the `open` frame.
fn empty_work_stream() -> tonic::Streaming<legion::structs::StreamWorkRequest> {
    use tonic::codec::{Codec, ProstCodec};
    let mut codec: ProstCodec<
        legion::structs::StreamWorkRequest,
        legion::structs::StreamWorkRequest,
    > = ProstCodec::default();
    tonic::Streaming::new_empty(codec.decoder(), tonic::body::empty_body())
}

fn req<T>(inner: T, token: Option<&str>) -> Request<T> {
    let mut request = Request::new(inner);
    if let Some(token) = token {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("ascii token"),
        );
    }
    request
}

/// A valid, variable-free template so render/run builders exercise the full
/// path under a management token. Uses the built-in `exec` kind so
/// validation does not depend on locally built plugin dylibs.
fn template_source(job_name: &str) -> String {
    format!(
        r#"{{
  description = "acl fixture template",
  variables = [],
  job | not_exported = fun vars => {{
    name = "{job_name}",
    tasks = [{{
      name = "main",
      runtime = "container",
      count = 1,
      config = {{ image = "alpine", command = ["echo", "ok"] }},
      resources = {{}},
      constraints = {{}},
    }}],
  }},
}}"#
    )
}

fn jobspec(name: &str) -> String {
    serde_json::json!({
        "name": name,
        "tasks": [{
            "runtime": "noop",
            "count": 1,
            "config": {},
            "resources": {},
            "constraints": {},
        }],
    })
    .to_string()
}

/// Invokes one RPC by proto method name with fixture-backed arguments.
///
/// Requests reference existing fixtures wherever the handler fetches the
/// resource before authorizing, so denials genuinely exercise the ACL check
/// rather than a not-found path.
async fn call_endpoint(
    svc: &ManagerDaemonService,
    fx: &Fixtures,
    method: &str,
    token: Option<&str>,
) -> Result<(), tonic::Status> {
    match method {
        "WorkerPreRegister" => svc
            .worker_pre_register(req(
                WorkerPreRegisterRequest {
                    worker_type: "default".into(),
                    join_token: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "WorkerRegister" => svc
            .worker_register(req(
                WorkerRegisterRequest {
                    name: "acl-node-registered".into(),
                    pool: "default".into(),
                    config_attrs: "{}".into(),
                    fingerprint_attrs: "{}".into(),
                    health_checks: "[]".into(),
                    join_token: None,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "Heartbeat" => svc
            .heartbeat(req(
                HeartbeatRequest {
                    id: NODE_ID.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "StreamWork" => svc
            .stream_work(req(empty_work_stream(), token))
            .await
            .map(|_| ()),
        "HealthCheck" => svc
            .health_check(req(
                HealthCheckRequest {
                    id: NODE_ID.into(),
                    health_checks: "[]".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "RunJob" => svc
            .run_job(req(
                RunJobRequest {
                    parsed_jobspec: jobspec("acl-job-2"),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpsertJobTemplate" => svc
            .upsert_job_template(req(
                UpsertJobTemplateRequest {
                    name: TEMPLATE_NAME.into(),
                    source_nickel: template_source(TEMPLATE_JOB_NAME),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListJobTemplates" => svc
            .list_job_templates(req(ListJobTemplatesRequest::default(), token))
            .await
            .map(|_| ()),
        "GetJobTemplate" => svc
            .get_job_template(req(
                GetJobTemplateRequest {
                    name: TEMPLATE_NAME.into(),
                    version: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "RenderJobTemplate" => svc
            .render_job_template(req(
                RenderJobTemplateRequest { namespace: Default::default(),
                    name: TEMPLATE_NAME.into(),
                    version: None,
                    variables_json: "{}".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "RunJobTemplate" => svc
            .run_job_template(req(
                RunJobTemplateRequest { namespace: Default::default(),
                    name: TEMPLATE_NAME.into(),
                    version: None,
                    variables_json: "{}".into(),
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
                    allow_unknown_fields: Default::default(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteJobTemplate" => svc
            .delete_job_template(req(
                DeleteJobTemplateRequest {
                    name: TEMPLATE_NAME.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetJobSpec" => svc
            .get_job_spec(req(
                GetJobSpecRequest {
                    name: JOB_NAME.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetJobSpecByConfig" => svc
            .get_job_spec_by_config(req(
                GetJobSpecByConfigRequest {
                    config_id: fx.job_config_id.clone(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListJobSpecHistory" => svc
            .list_job_spec_history(req(
                ListJobSpecHistoryRequest {
                    name: JOB_NAME.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetJobInfo" => svc
            .get_job_info(req(
                GetJobInfoRequest {
                    name: JOB_NAME.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListRollouts" => svc
            .list_rollouts(req(ListRolloutsRequest::default(), token))
            .await
            .map(|_| ()),
        "GetRollout" => svc
            .get_rollout(req(GetRolloutRequest { id: 1 }, token))
            .await
            .map(|_| ()),
        "GetRolloutStatus" => svc
            .get_rollout_status(req(GetRolloutStatusRequest { id: 1 }, token))
            .await
            .map(|_| ()),
        "ListRolloutEvents" => svc
            .list_rollout_events(req(
                ListRolloutEventsRequest {
                    rollout_id: 1,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "AdvanceRollout" => svc
            .advance_rollout(req(
                AdvanceRolloutRequest {
                    id: 1,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "StopRollout" => svc
            .stop_rollout(req(
                StopRolloutRequest {
                    id: 1,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "PlanJob" => svc
            .plan_job(req(
                PlanJobRequest { namespace: Default::default(),
                    parsed_jobspec: jobspec(JOB_NAME),
                    allow_unknown_fields: Default::default(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "StopJob" => svc
            .stop_job(req(
                StopJobRequest {
                    name: JOB_NAME.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ScaleTask" => svc
            .scale_task(req(
                ScaleTaskRequest {
                    name: JOB_NAME.into(),
                    count: Some(2),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetJobScaleStatus" => svc
            .get_job_scale_status(req(
                GetJobScaleStatusRequest {
                    name: JOB_NAME.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetScalingInputs" => svc
            .get_scaling_inputs(req(
                GetScalingInputsRequest {
                    targets: vec![ScalingTargetSelector {
                        job: JOB_NAME.into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DestroyJob" => svc
            .destroy_job(req(
                DestroyJobRequest {
                    name: JOB_NAME.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListJobs" => svc
            .list_jobs(req(ListJobsRequest::default(), token))
            .await
            .map(|_| ()),
        "ListServices" => svc
            .list_services(req(ListServicesRequest::default(), token))
            .await
            .map(|_| ()),
        "ListServiceEdges" => svc
            .list_service_edges(req(ListServiceEdgesRequest::default(), token))
            .await
            .map(|_| ()),
        "ResolveService" => svc
            .resolve_service(req(ResolveServiceRequest::default(), token))
            .await
            .map(|_| ()),
        "ListNamespaces" => svc
            .list_namespaces(req(ListNamespacesRequest::default(), token))
            .await
            .map(|_| ()),
        "GetNamespace" => svc
            .get_namespace(req(
                GetNamespaceRequest {
                    name: FIXTURE_NAMESPACE.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ApplyNamespace" => svc
            .apply_namespace(req(
                ApplyNamespaceRequest {
                    namespace: Some(Namespace {
                        name: "acl-applied-ns".into(),
                        ..Default::default()
                    }),
                    dry_run: false,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteNamespace" => svc
            .delete_namespace(req(
                DeleteNamespaceRequest {
                    name: DELETABLE_NAMESPACE.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListNamespacePolicies" => svc
            .list_namespace_policies(req(ListNamespacePoliciesRequest::default(), token))
            .await
            .map(|_| ()),
        "GetNamespacePolicy" => svc
            .get_namespace_policy(req(
                GetNamespacePolicyRequest {
                    name: "acl-namespace-policy".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ApplyNamespacePolicy" => svc
            .apply_namespace_policy(req(
                ApplyNamespacePolicyRequest {
                    object_json: r#"{"name":"acl-namespace-policy","namespaces":["acl-*"],"maxJobs":100}"#.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteNamespacePolicy" => svc
            .delete_namespace_policy(req(
                DeleteNamespacePolicyRequest {
                    name: "acl-namespace-policy-deletable".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpsertNetPolicy" => svc
            .upsert_net_policy(req(
                UpsertNetPolicyRequest {
                    object_json: r#"{"name":"acl-net-policy","enforcement":"default","priority":1,"egress":{"allow":["*.example.com"]}}"#.into(),
                    dry_run: false,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetNetPolicyObject" => svc
            .get_net_policy_object(req(
                GetNetPolicyObjectRequest {
                    name: FIXTURE_NET_POLICY.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListNetPolicies" => svc
            .list_net_policies(req(ListNetPoliciesRequest::default(), token))
            .await
            .map(|_| ()),
        "DeleteNetPolicy" => svc
            .delete_net_policy(req(
                DeleteNetPolicyRequest {
                    name: DELETABLE_NET_POLICY.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpsertNetPlugin" => svc
            .upsert_net_plugin(req(
                UpsertNetPluginRequest {
                    object_json: format!(
                        r#"{{"name":"acl-net-plugin","module":{{"source":"https://example.com/p.wasm","checksum":"sha256:{}"}},"stages":["request"]}}"#,
                        "a".repeat(64)
                    ),
                    dry_run: false,
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetNetPlugin" => svc
            .get_net_plugin(req(
                GetNetPluginRequest {
                    name: FIXTURE_NET_PLUGIN.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListNetPlugins" => svc
            .list_net_plugins(req(ListNetPluginsRequest::default(), token))
            .await
            .map(|_| ()),
        "DeleteNetPlugin" => svc
            .delete_net_plugin(req(
                DeleteNetPluginRequest {
                    name: DELETABLE_NET_PLUGIN.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetNetPluginStatus" => svc
            .get_net_plugin_status(req(
                GetNetPluginStatusRequest {
                    name: FIXTURE_NET_PLUGIN.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ShowNetPolicy" => svc
            .show_net_policy(req(
                ShowNetPolicyRequest {
                    job_id: JOB_NAME.into(),
                    task_name: String::new(),
                    node_pool: String::new(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "TestNetPolicy" => svc
            .test_net_policy(req(
                TestNetPolicyRequest {
                    job_id: JOB_NAME.into(),
                    to: "https://api.example.com".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetMeshCa" => svc
            .get_mesh_ca(req(GetMeshCaRequest {}, token))
            .await
            .map(|_| ()),
        "RotateMeshCa" => svc
            .rotate_mesh_ca(req(RotateMeshCaRequest {}, token))
            .await
            .map(|_| ()),
        "FetchDatapathMaterial" => svc
            .fetch_datapath_material(req(
                FetchDatapathMaterialRequest {
                    node_id: NODE_ID.into(),
                    refs: vec![MaterialRef {
                        kind: "policy".into(),
                        key: "0000".into(),
                        ..Default::default()
                    }],
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetNodes" => svc
            .get_nodes(req(GetNodesRequest::default(), token))
            .await
            .map(|_| ()),
        "GetHeartbeatCadence" => svc
            .get_heartbeat_cadence(req(GetHeartbeatCadenceRequest {}, token))
            .await
            .map(|_| ()),
        "GetNodeUsage" => svc
            .get_node_usage(req(
                GetNodeUsageRequest {
                    node_id: NODE_ID.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListNodeUsage" => svc
            .list_node_usage(req(ListNodeUsageRequest::default(), token))
            .await
            .map(|_| ()),
        "GetNodeContent" => svc
            .get_node_content(req(
                GetNodeContentRequest {
                    node_id: NODE_ID.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetSchedulerConcurrency" => svc
            .get_scheduler_concurrency(req(GetSchedulerConcurrencyRequest {}, token))
            .await
            .map(|_| ()),
        "GetNode" => svc
            .get_node(req(GetNodeRequest { id: NODE_ID.into() }, token))
            .await
            .map(|_| ()),
        "RestartWorker" => svc
            .restart_worker(req(
                RestartWorkerRequest {
                    node_id: NODE_ID.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "SetNodeAvailability" => svc
            .set_node_availability(req(
                SetNodeAvailabilityRequest {
                    node_id: NODE_ID.into(),
                    // Cordon-only (no evict) keeps the fixture node's
                    // allocations untouched for the other endpoints.
                    availability: "drained".into(),
                    evict: false,
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetLedger" => svc
            .get_ledger(req(GetLedgerRequest::default(), token))
            .await
            .map(|_| ()),
        "UpdateNodeDynamic" => svc
            .update_node_dynamic(req(
                UpdateNodeDynamicRequest {
                    id: NODE_ID.into(),
                    dynamic_diff: "{}".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetManagerConfigs" => svc
            .get_manager_configs(req(GetManagerConfigsRequest::default(), token))
            .await
            .map(|_| ()),
        "ListManagers" => svc
            .list_managers(req(ListManagersRequest::default(), token))
            .await
            .map(|_| ()),
        "GetObservability" => svc
            .get_observability(req(GetObservabilityRequest::default(), token))
            .await
            .map(|_| ()),
        "GetRaftStatus" => svc
            .get_raft_status(req(GetRaftStatusRequest::default(), token))
            .await
            .map(|_| ()),
        "ListRaftHistory" => svc
            .list_raft_history(req(ListRaftHistoryRequest::default(), token))
            .await
            .map(|_| ()),
        "IssueRaftCommand" => svc
            .issue_raft_command(req(
                IssueRaftCommandRequest {
                    command: "trigger_election".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetRaftJoinToken" => svc
            .get_raft_join_token(req(GetRaftJoinTokenRequest::default(), token))
            .await
            .map(|_| ()),
        // Default (None) out_path: the server picks a unique timestamped
        // destination under the fixture store's directory each call.
        "GetStateBackup" => svc
            .get_state_backup(req(GetStateBackupRequest::default(), token))
            .await
            .map(|_| ()),
        "GetKeyringStatus" => svc
            .get_keyring_status(req(GetKeyringStatusRequest::default(), token))
            .await
            .map(|_| ()),
        "RotateDataKey" => svc
            .rotate_data_key(req(RotateDataKeyRequest::default(), token))
            .await
            .map(|_| ()),
        "RewrapKv" => svc
            .rewrap_kv(req(RewrapKvRequest::default(), token))
            .await
            .map(|_| ()),
        "RewrapRootKey" => svc
            .rewrap_root_key(req(RewrapRootKeyRequest::default(), token))
            .await
            .map(|_| ()),
        // An empty `confirm` is refused as InvalidArgument once authorized —
        // the matrix only checks that the refusal is not an auth error, and
        // a fixture store must never actually be reset.
        "ResetKeyring" => svc
            .reset_keyring(req(ResetKeyringRequest::default(), token))
            .await
            .map(|_| ()),
        "PutKv" => svc
            .put_kv(req(
                PutKvRequest {
                    key: KV_KEY.into(),
                    value: "updated".into(),
                    value_type: KvValueType::String.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetKv" => svc
            .get_kv(req(GetKvRequest {
                    key: KV_KEY.into(),
                    ..Default::default()
                }, token))
            .await
            .map(|_| ()),
        "ListKv" => svc
            .list_kv(req(
                ListKvRequest {
                    prefix: String::new(),
                    include_values: true,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteKv" => svc
            .delete_kv(req(
                DeleteKvRequest {
                    key: "team/deletable".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "PatchKv" => svc
            .patch_kv(req(
                PatchKvRequest {
                    key: KV_KEY.into(),
                    patch_json: "[]".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "AcquireKvLock" => svc
            .acquire_kv_lock(req(
                AcquireKvLockRequest {
                    key: KV_KEY.into(),
                    ttl_seconds: 30,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "RenewKvLock" => svc
            .renew_kv_lock(req(
                RenewKvLockRequest {
                    key: KV_KEY.into(),
                    lock_id: "missing-lock".into(),
                    ttl_seconds: 30,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ReleaseKvLock" => svc
            .release_kv_lock(req(
                ReleaseKvLockRequest {
                    key: KV_KEY.into(),
                    lock_id: "missing-lock".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetKvLock" => svc
            .get_kv_lock(req(GetKvLockRequest { key: KV_KEY.into() }, token))
            .await
            .map(|_| ()),
        "QueryStateStore" => svc
            .query_state_store(req(
                QueryStateStoreRequest {
                    sql: "SELECT 1;".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetStateStoreSchema" => svc
            .get_state_store_schema(req(GetStateStoreSchemaRequest::default(), token))
            .await
            .map(|_| ()),
        "ListSavedStateQueries" => svc
            .list_saved_state_queries(req(ListSavedStateQueriesRequest::default(), token))
            .await
            .map(|_| ()),
        "UpsertSavedStateQuery" => svc
            .upsert_saved_state_query(req(
                UpsertSavedStateQueryRequest {
                    name: "acl-test-query".into(),
                    sql: "SELECT 1;".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteSavedStateQuery" => svc
            .delete_saved_state_query(req(
                DeleteSavedStateQueryRequest {
                    name: "acl-test-query".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "CreateInstance" => svc
            .create_instance(req(
                CreateInstanceRequest {
                    job_id: JOB_NAME.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "CreateInstances" => svc
            .create_instances(req(
                CreateInstancesRequest {
                    job_id: JOB_NAME.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DestroyInstance" => svc
            .destroy_instance(req(
                DestroyInstanceRequest {
                    instance_id: "missing-instance".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "StopInstance" => svc
            .stop_instance(req(
                StopInstanceRequest {
                    instance_id: "missing-instance".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "StartInstance" => svc
            .start_instance(req(
                StartInstanceRequest {
                    instance_id: "missing-instance".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "SuspendInstance" => svc
            .suspend_instance(req(
                legion::structs::SuspendInstanceRequest {
                    instance_id: "missing-instance".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListSessionNodes" => svc
            .list_session_nodes(req(
                legion::structs::ListSessionNodesRequest {
                    job_id: JOB_NAME.into(),
                    task_name: String::new(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "PromoteInstance" => svc
            .promote_instance(req(
                PromoteInstanceRequest {
                    instance_id: "missing-instance".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ForkInstance" => svc
            .fork_instance(req(
                ForkInstanceRequest {
                    instance_id: "missing-instance".into(),
                    count: 1,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "SetSnapshotAlias" => svc
            .set_snapshot_alias(req(
                SetSnapshotAliasRequest {
                    job_id: JOB_NAME.into(),
                    name: "golden".into(),
                    snapshot: "snap-missing".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteSnapshotAlias" => svc
            .delete_snapshot_alias(req(
                DeleteSnapshotAliasRequest {
                    job_id: JOB_NAME.into(),
                    name: "golden".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ReplaceInstance" => svc
            .replace_instance(req(
                ReplaceInstanceRequest {
                    instance_id: "missing-instance".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListInstances" => svc
            .list_instances(req(
                ListInstancesRequest {
                    job_id: JOB_NAME.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetInstance" => svc
            .get_instance(req(
                GetInstanceRequest {
                    instance_id: "missing-instance".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "CreateSnapshot" => svc
            .create_snapshot(req(
                CreateSnapshotRequest {
                    instance_id: "missing-instance".into(),
                    kind: "disk".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListSnapshots" => svc
            .list_snapshots(req(ListSnapshotsRequest::default(), token))
            .await
            .map(|_| ()),
        "GetSnapshot" => svc
            .get_snapshot(req(
                GetSnapshotRequest {
                    snapshot: "snap-missing".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteSnapshot" => svc
            .delete_snapshot(req(
                DeleteSnapshotRequest {
                    snapshot_id: "snap-missing".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "RestoreInstance" => svc
            .restore_instance(req(
                RestoreInstanceRequest {
                    instance_id: "missing-instance".into(),
                    snapshot: "snap-missing".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "WarmSnapshot" => svc
            .warm_snapshot(req(
                WarmSnapshotRequest {
                    snapshot: "snap-missing".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListVolumes" => svc
            .list_volumes(req(ListVolumesRequest::default(), token))
            .await
            .map(|_| ()),
        "GetVolume" => svc
            .get_volume(req(
                GetVolumeRequest {
                    volume_id: "missing-volume".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteVolume" => svc
            .delete_volume(req(
                DeleteVolumeRequest {
                    volume_id: "missing-volume".into(),
                    force: false,
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListVolumeDrivers" => svc
            .list_volume_drivers(req(ListVolumeDriversRequest {}, token))
            .await
            .map(|_| ()),
        "IssueWorkloadIdentityToken" => svc
            .issue_workload_identity_token(req(
                IssueWorkloadIdentityTokenRequest {
                    alloc_id: 1,
                    identity_name: "default".into(),
                    node_id: NODE_ID.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "IssueNodeCertificate" => svc
            .issue_node_certificate(req(
                IssueNodeCertificateRequest {
                    node_id: NODE_ID.into(),
                    csr_pem: legion::manager::mesh_ca::generate_csr(NODE_ID)
                        .expect("csr")
                        .1,
                    fleet: String::new(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "IssueNodeIdentityToken" => svc
            .issue_node_identity_token(req(
                IssueNodeIdentityTokenRequest {
                    node_id: NODE_ID.into(),
                    identity_name: "default".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetAllocationLogs" => svc
            .get_allocation_logs(req(
                GetAllocationLogsRequest {
                    job: JOB_NAME.into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ExecAllocation" => svc
            .exec_allocation(req(empty_exec_stream(), token))
            .await
            .map(|_| ()),
        "MintExecTicket" => svc
            .mint_exec_ticket(req(
                MintExecTicketRequest {
                    allocation_id: 404,
                    command: vec!["true".into()],
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "CreateExecKey" => svc
            .create_exec_key(req(
                CreateExecKeyRequest {
                    name: "acl-fixture".into(),
                    jobs: "acl-*".into(),
                    public_key_pem: "not a key".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListExecKeys" => svc
            .list_exec_keys(req(ListExecKeysRequest {}, token))
            .await
            .map(|_| ()),
        "DeleteExecKey" => svc
            .delete_exec_key(req(
                DeleteExecKeyRequest {
                    id: "xk_missing".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListJobActions" => svc
            .list_job_actions(req(
                ListJobActionsRequest {
                    job: JOB_NAME.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetArtifactCredential" => svc
            .get_artifact_credential(req(
                GetArtifactCredentialRequest {
                    node_id: NODE_ID.into(),
                    kv_path: KV_KEY.into(),
                    source: "https://example.test/artifact".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListEvents" => svc
            .list_events(req(ListEventsRequest::default(), token))
            .await
            .map(|_| ()),
        "StreamEvents" => svc
            .stream_events(req(StreamEventsRequest::default(), token))
            .await
            .map(|_| ()),
        "WhoAmI" => svc.who_am_i(req(WhoAmIRequest {}, token)).await.map(|_| ()),
        "Login" => svc
            .login(req(
                LoginRequest {
                    method: "userpass".into(),
                    username: Some("nobody".into()),
                    password: Some("nothing".into()),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetAuthMethods" => svc
            .get_auth_methods(req(GetAuthMethodsRequest {}, token))
            .await
            .map(|_| ()),
        "CreateJoinToken" => svc
            .create_join_token(req(
                CreateJoinTokenRequest {
                    worker_type: "default".into(),
                    ttl: None,
                    description: "test".into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListJoinTokens" => svc
            .list_join_tokens(req(ListJoinTokensRequest {}, token))
            .await
            .map(|_| ()),
        "IssueBootTicket" => svc
            .issue_boot_ticket(req(
                IssueBootTicketRequest {
                    job: JOB_NAME.into(),
                    task: "web".into(),
                    job_config_id: String::new(),
                    instance_id: "acl-instance".into(),
                    instance_prefix: String::new(),
                    pool: "default".into(),
                    gate: GateLevel::GateStage as i32,
                    ttl: "5m".into(),
                    uses: 1,
                    idempotency_key: None,
                    scope: String::new(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListBootTickets" => svc
            .list_boot_tickets(req(ListBootTicketsRequest { job: String::new() }, token))
            .await
            .map(|_| ()),
        "RatifyClaim" => svc
            .ratify_claim(req(
                RatifyClaimRequest {
                    node_id: NODE_ID.into(),
                    claim_id: "clm_acl".into(),
                    job_config_id: fx.job_config_id.clone(),
                    task_name: "web".into(),
                    instance_id: String::new(),
                    gate: GateLevel::GateStage as i32,
                    ticket: String::new(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "DeleteJoinToken" => svc
            .delete_join_token(req(
                DeleteJoinTokenRequest {
                    id: fx.join_token_id.clone(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpsertAclPolicy" => svc
            .upsert_acl_policy(req(
                UpsertAclPolicyRequest {
                    name: "acl-test-policy".into(),
                    description: String::new(),
                    rules_json: r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "GetAclPolicy" => svc
            .get_acl_policy(req(
                GetAclPolicyRequest {
                    name: FIXTURE_POLICY.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListAclPolicies" => svc
            .list_acl_policies(req(ListAclPoliciesRequest {}, token))
            .await
            .map(|_| ()),
        "DeleteAclPolicy" => svc
            .delete_acl_policy(req(
                DeleteAclPolicyRequest {
                    name: DELETABLE_POLICY.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "CreateAclToken" => svc
            .create_acl_token(req(
                CreateAclTokenRequest {
                    name: "acl-test-token".into(),
                    token_type: "client".into(),
                    policies: vec![FIXTURE_POLICY.into()],
                    ttl: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListAclTokens" => svc
            .list_acl_tokens(req(ListAclTokensRequest {}, token))
            .await
            .map(|_| ()),
        "DeleteAclToken" => svc
            .delete_acl_token(req(
                DeleteAclTokenRequest {
                    accessor_id: fx.deletable_token_accessor.clone(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpsertAclUser" => svc
            .upsert_acl_user(req(
                UpsertAclUserRequest {
                    username: "acl-test-user".into(),
                    password: "test-password".into(),
                    policies: vec![FIXTURE_POLICY.into()],
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListAclUsers" => svc
            .list_acl_users(req(ListAclUsersRequest {}, token))
            .await
            .map(|_| ()),
        "DeleteAclUser" => svc
            .delete_acl_user(req(
                DeleteAclUserRequest {
                    username: DELETABLE_USER.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListGrants" => svc
            .list_grants(req(
                ListGrantsRequest {
                    job: Some(JOB_NAME.into()),
                    include_revoked: true,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "RevokeGrant" => svc
            .revoke_grant(req(
                RevokeGrantRequest {
                    job: JOB_NAME.into(),
                    version: None,
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "ListApps" => svc
            .list_apps(req(ListAppsRequest {}, token))
            .await
            .map(|_| ()),
        "GetApp" => svc
            .get_app(req(
                GetAppRequest {
                    name: FIXTURE_APP.into(),
                },
                token,
            ))
            .await
            .map(|_| ()),
        "InstallApp" => svc
            .install_app(req(
                InstallAppRequest {
                    manifest_json: app_manifest_json("acl-new-app", "1.0.0"),
                    source: "file".into(),
                    source_ref: "./new.json".into(),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpgradeApp" => svc
            .upgrade_app(req(
                UpgradeAppRequest {
                    name: FIXTURE_APP.into(),
                    manifest_json: app_manifest_json(FIXTURE_APP, "2.0.0"),
                    approve: true,
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UninstallApp" => svc
            .uninstall_app(req(
                UninstallAppRequest {
                    name: DELETABLE_APP.into(),
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "SetAppEnabled" => svc
            .set_app_enabled(req(
                SetAppEnabledRequest {
                    name: FIXTURE_APP.into(),
                    enabled: true,
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        "UpdateAppSettings" => svc
            .update_app_settings(req(
                UpdateAppSettingsRequest {
                    name: FIXTURE_APP.into(),
                    replicas: Some(2),
                    ..Default::default()
                },
                token,
            ))
            .await
            .map(|_| ()),
        "SetAppConfig" => svc
            .set_app_config(req(
                SetAppConfigRequest {
                    name: FIXTURE_APP.into(),
                    config_json: "{}".into(),
                    idempotency_key: None,
                },
                token,
            ))
            .await
            .map(|_| ()),
        other => {
            panic!("endpoint_table contains `{other}` but call_endpoint has no builder for it")
        }
    }
}

fn setup_store() -> R<Store> {
    let store_dir = tempfile::Builder::new()
        .prefix("legion-acl-endpoints-")
        .tempdir_in(crate::support::scratch_root())
        .context("Error creating temp store directory")?
        .keep();
    let store_config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("Error parsing store path")?
            .to_string(),
        ..Default::default()
    };
    Store::start_storage(&store_config)
}

/// Seeds a node, job, KV entries, and ACL fixtures so fetch-then-authorize
/// endpoints reach their ACL checks instead of returning NotFound.
fn seed_fixtures(store: &Store) -> R<Fixtures> {
    // Worker types pin a pool; join tokens pin a worker type.
    for (name, pool) in [("default", "default"), ("gpu", "gpu")] {
        store.insert_worker_type(&WorkerConfig {
            name: name.into(),
            pool: pool.into(),
            ..Default::default()
        })?;
    }
    store.insert_node(NodeMsg {
        id: NODE_ID.into(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "[]".into(),
        last_heartbeat: "2026-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;

    let config: JobConfig = serde_json::from_str(&jobspec(JOB_NAME))?;
    store.handle_job_submit(
        &config,
        &[Submission {
            path: "acl-test.json".into(),
            content: jobspec(JOB_NAME),
            is_primary: true,
        }],
    )?;
    let job_config_id = store.get_job_config(JOB_NAME)?.id();

    // Network policy fixtures (docs/network-handlers.md): one to read, one
    // to delete.
    for name in [FIXTURE_NET_POLICY, DELETABLE_NET_POLICY] {
        let object: legion::structs::netpolicy::NetPolicyObject =
            serde_json::from_value(serde_json::json!({
                "name": name,
                "enforcement": "require",
                "priority": 50,
                "selector": { "jobs": ["never-matches-*"] },
                "egress": { "allow": ["*.example.com"] },
            }))?;
        store.upsert_net_policy(object)?;
    }
    // Network plugin fixtures (docs/network-plugins.md): one to read, one to
    // delete.
    for name in [FIXTURE_NET_PLUGIN, DELETABLE_NET_PLUGIN] {
        let object: legion::structs::netplugin::NetPluginObject =
            serde_json::from_value(serde_json::json!({
                "name": name,
                "module": {
                    "source": "https://example.com/p.wasm",
                    "checksum": format!("sha256:{}", "b".repeat(64)),
                },
                "stages": ["request"],
            }))?;
        store.upsert_net_plugin(object)?;
    }

    for name in [FIXTURE_NAMESPACE, DELETABLE_NAMESPACE] {
        store.apply_namespace(legion::structs::namespace::Namespace {
            name: name.into(),
            ..Default::default()
        })?;
    }
    for name in ["acl-namespace-policy", "acl-namespace-policy-deletable"] {
        store.apply_namespace_policy(
            legion::structs::namespace_policy::NamespacePolicy::from_json(
                &serde_json::json!({ "name": name, "namespaces": ["acl-*"] }).to_string(),
            )
            .map_err(|err| anyhow!(err))?,
        )?;
    }

    store.upsert_job_template_version(
        TEMPLATE_NAME,
        Some("acl fixture template"),
        &template_source(TEMPLATE_JOB_NAME),
        &[],
        "fixture",
    )?;

    for name in [FIXTURE_APP, DELETABLE_APP] {
        store.insert_app(app_fixture(name)?)?;
    }

    store.put_kv(KV_KEY, "v1", KvValueType::String, None, None)?;
    store.put_kv("team/deletable", "v1", KvValueType::String, None, None)?;
    store.put_kv("secret/hidden", "v1", KvValueType::String, None, None)?;

    store.upsert_acl_policy(
        FIXTURE_POLICY,
        "fixture",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
    )?;
    store.upsert_acl_policy(
        DELETABLE_POLICY,
        "fixture",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
    )?;
    store.upsert_acl_user(
        DELETABLE_USER,
        Some("$argon2id$fixture"),
        &[FIXTURE_POLICY.to_string()],
    )?;

    // A join token for the DeleteJoinToken builder (inserted directly so the
    // ACL-disabled setup has one too).
    let join_token_id = "fixture-join-token".to_string();
    store.insert_join_token(&legion::storage::acl_store::JoinTokenRecord {
        id: join_token_id.clone(),
        secret_hash: "fixture-join-hash".into(),
        worker_type: "default".into(),
        description: "fixture".into(),
        expires_at: None,
        created_at: "2026-01-01T00:00:00Z".into(),
        pinned_attrs: None,
    })?;

    Ok(Fixtures {
        job_config_id,
        deletable_token_accessor: String::new(),
        join_token_id,
    })
}

fn enabled_authenticator() -> Arc<Authenticator> {
    Arc::new(
        Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )
        .expect("authenticator should build"),
    )
}

/// Service with ACLs enabled, plus a management token and a deletable client
/// token for the DeleteAclToken builder.
fn setup_enabled() -> R<(ManagerDaemonService, Fixtures, String)> {
    let store = setup_store()?;
    let mut fixtures = seed_fixtures(&store)?;
    let authenticator = enabled_authenticator();

    let (_, management_secret) = authenticator
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .expect("a fresh store has no management token");
    let (deletable, _) = authenticator
        .mint_token(
            &store,
            "deletable",
            "client",
            &[FIXTURE_POLICY.to_string()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;
    fixtures.deletable_token_accessor = deletable.accessor_id;

    Ok((
        ManagerDaemonService::with_acl(store, authenticator),
        fixtures,
        management_secret,
    ))
}

fn is_auth_error(code: Code) -> bool {
    matches!(code, Code::PermissionDenied | Code::Unauthenticated)
}

/// Guard: every RPC in the Manager proto service must have a row in
/// `endpoint_table`, and vice versa. Adding an endpoint without classifying
/// its ACL behavior fails this test.
#[test]
fn every_manager_rpc_is_classified() -> R<()> {
    let proto_path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/legion.proto");
    let proto = std::fs::read_to_string(proto_path)?;
    let service_start = proto
        .find("service Manager {")
        .context("service Manager not found in proto")?;
    let service_body = &proto[service_start..];
    let service_end = service_body
        .find("\n}")
        .context("end of service Manager not found")?;
    let service_body = &service_body[..service_end];

    let rpc_regex = regex::Regex::new(r"rpc\s+(\w+)\s*\(")?;
    let proto_rpcs: BTreeSet<String> = rpc_regex
        .captures_iter(service_body)
        .map(|capture| capture[1].to_string())
        .collect();
    let table_rpcs: BTreeSet<String> = endpoint_table()
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();

    let missing: Vec<_> = proto_rpcs.difference(&table_rpcs).collect();
    let stale: Vec<_> = table_rpcs.difference(&proto_rpcs).collect();
    assert!(
        missing.is_empty(),
        "Manager RPCs missing ACL test coverage (add them to endpoint_table \
         in tests/integration/acl_endpoints.rs and an ACL check in the handler): {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "endpoint_table entries with no matching proto RPC: {stale:?}"
    );
    Ok(())
}

/// With ACLs enabled, anonymous callers are rejected as unauthenticated on
/// every protected endpoint and on the worker plane, and never blocked on
/// open endpoints.
#[tokio::test]
async fn anonymous_is_denied_on_every_protected_endpoint() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;

    for (method, plane) in endpoint_table() {
        let result = call_endpoint(&svc, &fixtures, method, None).await;
        match plane {
            Plane::Protected(capability) => {
                let status = result.expect_err(&format!(
                    "{method} must deny anonymous callers when ACLs are enabled"
                ));
                assert_eq!(
                    status.code(),
                    Code::Unauthenticated,
                    "{method} (requires {capability}) returned `{}` instead of Unauthenticated: {}",
                    status.code(),
                    status.message(),
                );
            }
            Plane::Worker => {
                // Without a join token (registration) or node token
                // (heartbeats), the worker plane rejects anonymous callers.
                let status = result.expect_err(&format!(
                    "{method} must reject credential-less workers when ACLs are enabled"
                ));
                assert_eq!(
                    status.code(),
                    Code::Unauthenticated,
                    "{method} returned `{}` instead of Unauthenticated: {}",
                    status.code(),
                    status.message(),
                );
            }
            Plane::Open => {
                if let Err(status) = result {
                    // Login may return Unauthenticated for the bogus fixture
                    // credentials — that is its job. Nothing here may return
                    // PermissionDenied for lacking a token.
                    assert_ne!(
                        status.code(),
                        Code::PermissionDenied,
                        "{method} is {plane:?} and must not deny anonymous callers: {}",
                        status.message(),
                    );
                }
            }
        }
    }
    Ok(())
}

/// A node's `namespaces` restriction and `legion.role` come from its
/// registration's grant, never from what the worker reports (apps plan
/// calls entry 19): a join token's worker type, the attributes a manager
/// pinned on its own in-process worker's token, or, on a re-registration
/// with the node's token, what the node registered with.
#[tokio::test]
async fn registration_pins_namespaces_and_role_test() -> R<()> {
    let store = setup_store()?;
    seed_fixtures(&store)?;
    store.insert_worker_type(&WorkerConfig {
        name: "ci".into(),
        pool: "ci".into(),
        namespaces: vec!["ci-*".into()],
        ..Default::default()
    })?;
    let authenticator = enabled_authenticator();
    let (_, admin) = authenticator
        .mint_initial_management_token(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?
        .expect("a fresh store has no management token");
    let svc = ManagerDaemonService::with_acl(store.clone(), authenticator.clone());
    let attrs = |node: &str| -> R<serde_json::Value> { Ok(store.get_node(node)?.config_attrs) };

    // A forged registration through a `ci` token: the restriction and role
    // it reports are replaced by the type's; other attributes stay.
    let created = svc
        .create_join_token(req(
            CreateJoinTokenRequest {
                worker_type: "ci".into(),
                ttl: Some("1h".into()),
                description: String::new(),
            },
            Some(&admin),
        ))
        .await?
        .into_inner();
    let secret = legion::structs::token::JoinToken::decode(&created.token)?.secret;
    let forged = serde_json::json!({
        "namespaces": ["*", "legion-*"],
        "legion.role": "manager",
        "region": "eu",
    });
    let registered = svc
        .worker_register(req(
            WorkerRegisterRequest {
                name: "ci-1".into(),
                pool: "ci".into(),
                config_attrs: forged.to_string(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: Some(secret),
                ..Default::default()
            },
            None,
        ))
        .await?
        .into_inner();
    let ci = attrs("ci-1")?;
    assert_eq!(ci["namespaces"], serde_json::json!(["ci-*"]), "{ci}");
    assert!(ci.get("legion.role").is_none(), "{ci}");
    assert_eq!(ci["region"], "eu", "{ci}");

    // Re-registering with the node's token keeps what it registered with,
    // whatever it reports now.
    let node_token = registered.node_token.expect("a node token");
    svc.worker_register(req(
        WorkerRegisterRequest {
            name: "ci-1".into(),
            pool: "ci".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            health_checks: "[]".into(),
            ..Default::default()
        },
        Some(&node_token),
    ))
    .await?;
    assert_eq!(attrs("ci-1")?["namespaces"], serde_json::json!(["ci-*"]));

    // A manager's own in-process worker: its token pins the manager's
    // settings, and a registration reporting none still gets them.
    let pinned = serde_json::json!({ "namespaces": ["legion-system"], "legion.role": "manager" });
    let (_, composite) = authenticator
        .mint_local_worker_join_token(
            &store,
            "http://127.0.0.1:4747",
            "default",
            Some(std::time::Duration::from_secs(60)),
            "system worker",
            &pinned,
        )
        .map_err(|s| anyhow!(s.to_string()))?;
    svc.worker_register(req(
        WorkerRegisterRequest {
            name: "legion-system-1".into(),
            pool: "default".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            health_checks: "[]".into(),
            join_token: Some(legion::structs::token::JoinToken::decode(&composite)?.secret),
            ..Default::default()
        },
        None,
    ))
    .await?;
    let system = attrs("legion-system-1")?;
    assert_eq!(
        system["namespaces"],
        serde_json::json!(["legion-system"]),
        "{system}"
    );
    assert_eq!(system["legion.role"], "manager", "{system}");
    Ok(())
}

/// End-to-end worker join flow with ACLs enforced: a join token admits a
/// worker, registration mints a node token for heartbeats, re-registration
/// rotates it, and revoking the join token closes the door.
#[tokio::test]
async fn worker_join_flow_with_acls_enabled() -> R<()> {
    let (svc, _, management_secret) = setup_enabled()?;

    // Operator mints a join token (requires node:write).
    let created = svc
        .create_join_token(req(
            CreateJoinTokenRequest {
                worker_type: "default".into(),
                ttl: Some("1h".into()),
                description: "test rack".into(),
            },
            Some(&management_secret),
        ))
        .await?
        .into_inner();
    let join_token = legion::structs::token::JoinToken::decode(&created.token)?;
    assert!(!join_token.secret.is_empty());

    // Pre-register fails without the secret, succeeds with it.
    let denied = svc
        .worker_pre_register(req(
            WorkerPreRegisterRequest {
                worker_type: "default".into(),
                join_token: None,
            },
            None,
        ))
        .await
        .expect_err("pre-register without join token must fail");
    assert_eq!(denied.code(), Code::Unauthenticated);
    let gated = svc
        .worker_pre_register(req(
            WorkerPreRegisterRequest {
                worker_type: "default".into(),
                join_token: Some(join_token.secret.clone()),
            },
            None,
        ))
        .await;
    if let Err(status) = gated {
        assert_ne!(status.code(), Code::Unauthenticated, "{}", status.message());
    }

    // Register with the join secret; the reply carries a node token.
    let registered = svc
        .worker_register(req(
            WorkerRegisterRequest {
                name: "joined-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: Some(join_token.secret.clone()),
                ..Default::default()
            },
            None,
        ))
        .await?
        .into_inner();
    let node_token = registered
        .node_token
        .expect("registration mints a node token");

    // Heartbeats require the node token.
    let denied = svc
        .heartbeat(req(
            HeartbeatRequest {
                id: "joined-worker".into(),
                ..Default::default()
            },
            None,
        ))
        .await
        .expect_err("heartbeat without node token must fail");
    assert_eq!(denied.code(), Code::Unauthenticated);
    svc.heartbeat(req(
        HeartbeatRequest {
            id: "joined-worker".into(),
            ..Default::default()
        },
        Some(&node_token),
    ))
    .await
    .expect("heartbeat with node token succeeds");
    svc.health_check(req(
        HealthCheckRequest {
            id: "joined-worker".into(),
            health_checks: "[]".into(),
        },
        Some(&node_token),
    ))
    .await
    .expect("health check with node token succeeds");

    // The node token is worker-plane only: no user-facing API access.
    let status = svc
        .list_jobs(req(ListJobsRequest::default(), Some(&node_token)))
        .await
        .expect_err("node tokens must not grant API access");
    assert_eq!(status.code(), Code::PermissionDenied);

    // The join secret alone may not take over the live node's name: every
    // node of the worker type holds that secret.
    let re_register = |bearer: Option<&str>| {
        req(
            WorkerRegisterRequest {
                name: "joined-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: Some(join_token.secret.clone()),
                ..Default::default()
            },
            bearer,
        )
    };
    let takeover = svc
        .worker_register(re_register(None))
        .await
        .expect_err("a join token must not take over a live node");
    assert_eq!(
        takeover.code(),
        Code::PermissionDenied,
        "{}",
        takeover.message()
    );

    // With the node's own token alongside, re-registering rotates it; the
    // old one stops working.
    let re_registered = svc
        .worker_register(re_register(Some(&node_token)))
        .await?
        .into_inner();
    let rotated = re_registered
        .node_token
        .expect("re-registration mints a fresh token");
    let stale = svc
        .heartbeat(req(
            HeartbeatRequest {
                id: "joined-worker".into(),
                ..Default::default()
            },
            Some(&node_token),
        ))
        .await
        .expect_err("rotated-out node token must be rejected");
    assert_eq!(stale.code(), Code::Unauthenticated);
    svc.heartbeat(req(
        HeartbeatRequest {
            id: "joined-worker".into(),
            ..Default::default()
        },
        Some(&rotated),
    ))
    .await
    .expect("rotated node token works");

    // Revoking the join token closes the door for future joins.
    let info = created.info.expect("join token info");
    svc.delete_join_token(req(
        DeleteJoinTokenRequest { id: info.id },
        Some(&management_secret),
    ))
    .await?;
    let closed = svc
        .worker_register(req(
            WorkerRegisterRequest {
                name: "late-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: Some(join_token.secret),
                ..Default::default()
            },
            None,
        ))
        .await
        .expect_err("revoked join token must be rejected");
    assert_eq!(closed.code(), Code::Unauthenticated);

    // ...but the fleet that already joined is unaffected: a worker that
    // restarts after its join token was revoked (or expired) comes back on
    // its persisted node token alone (spot-check P0-8).
    svc.worker_pre_register(req(
        WorkerPreRegisterRequest {
            worker_type: "default".into(),
            join_token: None,
        },
        Some(&rotated),
    ))
    .await
    .expect("a node token pre-registers its node after the join token is gone");
    let restarted = svc
        .worker_register(req(
            WorkerRegisterRequest {
                name: "joined-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: None,
                ..Default::default()
            },
            Some(&rotated),
        ))
        .await
        .expect("a node token re-registers its node after the join token is gone")
        .into_inner();
    assert_eq!(restarted.node_token, None, "the presented token is kept");
    svc.heartbeat(req(
        HeartbeatRequest {
            id: "joined-worker".into(),
            ..Default::default()
        },
        Some(&rotated),
    ))
    .await
    .expect("the kept node token still heartbeats");
    Ok(())
}

/// A worker credential only speaks for its own node, and a join token only
/// admits its pinned worker type and pool. Without this, one compromised node
/// could re-register as a peer (rotating away the peer's token) or heartbeat,
/// report health, and ratify claims in its name.
/// A fleet certificate names every node of the fleet, so the fleet comes
/// from the operator's worker type: a join token's worker type must grant
/// the `workerFleet` a registration claims, and a node token cannot move
/// its node to another fleet. Before, any node could claim any fleet and
/// get its certificate.
#[tokio::test]
async fn a_fleet_claim_needs_a_worker_type_grant() -> R<()> {
    let (svc, _, management_secret) = setup_enabled()?;
    let join_secret = |worker_type: &'static str| {
        let svc = &svc;
        let management_secret = management_secret.clone();
        async move {
            let created = svc
                .create_join_token(req(
                    CreateJoinTokenRequest {
                        worker_type: worker_type.into(),
                        ttl: Some("1h".into()),
                        description: "fleet test".into(),
                    },
                    Some(&management_secret),
                ))
                .await?
                .into_inner();
            R::Ok(legion::structs::token::JoinToken::decode(&created.token)?.secret)
        }
    };
    let register = |name: &str, fleet: &str, join_token: Option<String>, bearer: Option<&str>| {
        req(
            WorkerRegisterRequest {
                name: name.into(),
                pool: "default".into(),
                config_attrs: serde_json::json!({ "workerFleet": fleet }).to_string(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token,
                ..Default::default()
            },
            bearer,
        )
    };

    // `default` grants no fleet.
    let refused = svc
        .worker_register(register(
            "sim-a-web-0",
            "sim-a",
            Some(join_secret("default").await?),
            None,
        ))
        .await
        .expect_err("a worker type without workerFleet grants no fleet");
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(
        refused.message().contains("does not grant"),
        "{}",
        refused.message()
    );

    // A simulator worker type grants `sim-*`.
    svc.store.insert_worker_type(&WorkerConfig {
        name: "sims".into(),
        pool: "default".into(),
        attributes: [("workerFleet".to_string(), "sim-*".to_string())].into(),
        ..WorkerConfig::default()
    })?;
    let sims = join_secret("sims").await?;
    let refused = svc
        .worker_register(register("prod-0", "prod", Some(sims.clone()), None))
        .await
        .expect_err("the glob does not cover `prod`");
    assert_eq!(refused.code(), Code::PermissionDenied);
    let node_token = svc
        .worker_register(register("sim-a-web-0", "sim-a", Some(sims), None))
        .await?
        .into_inner()
        .node_token
        .expect("registration mints a node token");

    // The node's own token re-registers it in its fleet, but not in another.
    let moved = svc
        .worker_register(register("sim-a-web-0", "sim-b", None, Some(&node_token)))
        .await
        .expect_err("a node token keeps the node's fleet");
    assert_eq!(moved.code(), Code::PermissionDenied);
    svc.worker_register(register("sim-a-web-0", "sim-a", None, Some(&node_token)))
        .await
        .expect("re-registering in the same fleet");
    Ok(())
}

#[tokio::test]
async fn worker_plane_binds_node_ids_to_the_caller() -> R<()> {
    let (svc, fixtures, management_secret) = setup_enabled()?;
    let created = svc
        .create_join_token(req(
            CreateJoinTokenRequest {
                worker_type: "default".into(),
                ttl: Some("1h".into()),
                description: "binding test".into(),
            },
            Some(&management_secret),
        ))
        .await?
        .into_inner();
    let secret = legion::structs::token::JoinToken::decode(&created.token)?.secret;

    // The join token is pinned to `default`: no other worker type or pool.
    let status = svc
        .worker_pre_register(req(
            WorkerPreRegisterRequest {
                worker_type: "gpu".into(),
                join_token: Some(secret.clone()),
            },
            None,
        ))
        .await
        .expect_err("a join token must not pre-register another worker type");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{}",
        status.message()
    );
    let register = |name: &str, pool: &str, join: Option<String>, token: Option<&str>| {
        req(
            WorkerRegisterRequest {
                name: name.into(),
                pool: pool.into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: join,
                ..Default::default()
            },
            token,
        )
    };
    let status = svc
        .worker_register(register("intruder", "gpu", Some(secret.clone()), None))
        .await
        .expect_err("a join token must not register into another pool");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{}",
        status.message()
    );

    let intruder = svc
        .worker_register(register("intruder", "default", Some(secret.clone()), None))
        .await?
        .into_inner()
        .node_token
        .expect("registration mints a node token");

    // The intruder's node token cannot register, heartbeat, or act as the
    // fixture node.
    for method in [
        "WorkerRegister",
        "Heartbeat",
        "HealthCheck",
        "FetchDatapathMaterial",
        "GetArtifactCredential",
        "RatifyClaim",
    ] {
        let status = call_endpoint(&svc, &fixtures, method, Some(&intruder))
            .await
            .expect_err(&format!("{method} must refuse a foreign node token"));
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{method}: {}",
            status.message()
        );
    }
    let status = svc
        .worker_register(register(NODE_ID, "default", None, Some(&intruder)))
        .await
        .expect_err("a node token must not take over another node");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{}",
        status.message()
    );

    // Its own node: re-registering with the node token alone works, but it
    // cannot move itself into another pool.
    let status = svc
        .worker_register(register("intruder", "gpu", None, Some(&intruder)))
        .await
        .expect_err("a node token must not change its node's pool");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{}",
        status.message()
    );
    // Re-registering with the node token itself (what a restarted worker
    // does, from its persisted token) keeps that token: the reply mints
    // none, and the presented token stays valid. That is what lets a worker
    // restart after its join token expired (spot-check P0-8).
    let kept = svc
        .worker_register(register("intruder", "default", None, Some(&intruder)))
        .await?
        .into_inner()
        .node_token;
    assert_eq!(
        kept, None,
        "a node-token re-registration keeps the presented token"
    );
    svc.heartbeat(req(
        HeartbeatRequest {
            id: "intruder".into(),
            ..Default::default()
        },
        Some(&intruder),
    ))
    .await
    .expect("a node token heartbeats for its own node");
    Ok(())
}

/// In audit-only mode the worker plane logs but admits credential-less
/// workers (the migration ramp for fleets that predate ACLs).
#[tokio::test]
async fn audit_only_admits_workers_without_join_tokens() -> R<()> {
    let store = setup_store()?;
    let fixtures = seed_fixtures(&store)?;
    let authenticator = Arc::new(
        Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                audit_only: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )
        .expect("authenticator should build"),
    );
    let svc = ManagerDaemonService::with_acl(store, authenticator);

    for method in [
        "WorkerPreRegister",
        "WorkerRegister",
        "Heartbeat",
        "HealthCheck",
    ] {
        if let Err(status) = call_endpoint(&svc, &fixtures, method, None).await {
            assert_ne!(
                status.code(),
                Code::Unauthenticated,
                "{method} must not reject workers in audit-only mode: {}",
                status.message(),
            );
        }
    }
    Ok(())
}

/// A management token passes the ACL gate on every endpoint (other failures,
/// like NotFound for absent fixtures, are fine — they prove the handler ran).
#[tokio::test]
async fn management_token_passes_every_endpoint() -> R<()> {
    let (svc, fixtures, management_secret) = setup_enabled()?;

    for (method, _) in endpoint_table() {
        if method == "Login" {
            continue; // Login validates the request-body credentials, not the bearer token.
        }
        if let Err(status) = call_endpoint(&svc, &fixtures, method, Some(&management_secret)).await
        {
            assert!(
                !is_auth_error(status.code()),
                "{method} must not fail auth with a management token, got {}: {}",
                status.code(),
                status.message(),
            );
        }
    }
    Ok(())
}

/// With ACLs disabled (the default), no endpoint ever fails with an auth
/// error — except Login, whose job is credential validation.
#[tokio::test]
async fn acl_disabled_never_returns_auth_errors() -> R<()> {
    let store = setup_store()?;
    let fixtures = seed_fixtures(&store)?;
    let svc = ManagerDaemonService::new(store);

    for (method, _) in endpoint_table() {
        if method == "Login" {
            continue; // Login validates the supplied credentials by design.
        }
        if let Err(status) = call_endpoint(&svc, &fixtures, method, None).await {
            assert!(
                !is_auth_error(status.code()),
                "{method} must not fail auth when ACLs are disabled, got {}: {}",
                status.code(),
                status.message(),
            );
        }
    }
    Ok(())
}

/// An invalid or revoked token is rejected as Unauthenticated everywhere
/// (distinct from PermissionDenied so clients can react differently).
#[tokio::test]
async fn unknown_token_is_unauthenticated() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;
    let status = call_endpoint(&svc, &fixtures, "ListJobs", Some("lgn_not_a_real_token"))
        .await
        .expect_err("unknown token must be rejected");
    assert_eq!(status.code(), Code::Unauthenticated);
    Ok(())
}

/// Job-name-scoped policies allow matching jobs and deny others, and list
/// endpoints filter rather than fail.
#[tokio::test]
async fn job_name_scoped_policy_limits_job_access() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();

    svc.store.upsert_acl_policy(
        "acl-job-only",
        "",
        &format!(
            r#"[{{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"], "names": ["{JOB_NAME}"]}}]"#
        ),
    )?;
    let (_, secret) = authenticator
        .mint_token(
            &svc.store,
            "scoped",
            "client",
            &["acl-job-only".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    // Allowed: reading and stopping the matching job.
    call_endpoint(&svc, &fixtures, "GetJobInfo", Some(&secret))
        .await
        .expect("scoped token reads its job");
    call_endpoint(&svc, &fixtures, "StopJob", Some(&secret))
        .await
        .expect("scoped token writes its job");

    // Denied: submitting a different job (RunJob builder uses `acl-job-2`).
    let status = call_endpoint(&svc, &fixtures, "RunJob", Some(&secret))
        .await
        .expect_err("scoped token must not write other jobs");
    assert_eq!(status.code(), Code::PermissionDenied);

    // List endpoints filter instead of failing.
    let reply = svc
        .list_jobs(req(ListJobsRequest::default(), Some(&secret)))
        .await?
        .into_inner();
    assert!(
        reply.jobs.iter().all(|job| job.name == JOB_NAME),
        "list must only contain the scoped job, got {:?}",
        reply.jobs.iter().map(|job| &job.name).collect::<Vec<_>>(),
    );
    Ok(())
}

/// Template-scoped delegation: a token whose only `job:write` comes from a
/// `templates` selector can submit through the template (and see and stop
/// the job it creates) but not through raw RunJob, cannot author templates,
/// and the collision guard stops it from hijacking unrelated jobs through a
/// template that renders their names.
#[tokio::test]
async fn template_scoped_policy_gates_submission_channel() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();

    svc.store.upsert_acl_policy(
        "template-runner",
        "",
        &format!(
            r#"[
                {{"resources": ["template"], "actions": ["read"],
                  "templates": ["{TEMPLATE_NAME}", "acl-hijack"]}},
                {{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"],
                  "templates": ["{TEMPLATE_NAME}", "acl-hijack"]}}
            ]"#
        ),
    )?;
    let (_, secret) = authenticator
        .mint_token(
            &svc.store,
            "template-runner",
            "client",
            &["template-runner".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    // Allowed: browsing and running the granted template.
    call_endpoint(&svc, &fixtures, "GetJobTemplate", Some(&secret))
        .await
        .expect("scoped token reads its template");
    call_endpoint(&svc, &fixtures, "RenderJobTemplate", Some(&secret))
        .await
        .expect("scoped token previews its template");
    call_endpoint(&svc, &fixtures, "RunJobTemplate", Some(&secret))
        .await
        .expect("scoped token submits through its template");

    // Denied: raw RunJob carries no template attribute, so the
    // templates-scoped job rule never matches it.
    let status = call_endpoint(&svc, &fixtures, "RunJob", Some(&secret))
        .await
        .expect_err("template-scoped token must not submit raw specs");
    assert_eq!(status.code(), Code::PermissionDenied);
    // Denied: read+run does not grant template authoring.
    let status = call_endpoint(&svc, &fixtures, "UpsertJobTemplate", Some(&secret))
        .await
        .expect_err("template-scoped token must not author templates");
    assert_eq!(status.code(), Code::PermissionDenied);

    // The job created through the template is visible (provenance feeds the
    // templates selector on reads); the unrelated fixture job is filtered.
    let reply = svc
        .list_jobs(req(ListJobsRequest::default(), Some(&secret)))
        .await?
        .into_inner();
    assert!(
        reply.jobs.iter().any(|job| job.name == TEMPLATE_JOB_NAME),
        "template-created job must be visible, got {:?}",
        reply.jobs.iter().map(|job| &job.name).collect::<Vec<_>>(),
    );
    assert!(
        reply.jobs.iter().all(|job| job.name != JOB_NAME),
        "unrelated jobs must stay filtered"
    );
    // Lifecycle write on the template's job is allowed.
    svc.stop_job(req(
        StopJobRequest {
            name: TEMPLATE_JOB_NAME.into(),
            ..Default::default()
        },
        Some(&secret),
    ))
    .await
    .expect("template-scoped token stops its template's job");

    // Collision guard: a granted template that renders an existing raw
    // job's name must not let this token overwrite it.
    svc.store.upsert_job_template_version(
        "acl-hijack",
        None,
        &template_source(JOB_NAME),
        &[],
        "fixture",
    )?;
    let status = svc
        .run_job_template(req(
            RunJobTemplateRequest {
                namespace: Default::default(),
                name: "acl-hijack".into(),
                version: None,
                variables_json: "{}".into(),
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
                allow_unknown_fields: Default::default(),
            },
            Some(&secret),
        ))
        .await
        .expect_err("collision guard must block hijacking unrelated jobs");
    assert_eq!(status.code(), Code::FailedPrecondition);
    Ok(())
}

/// Worker-plane-auth plan §6.16: a job with a host-level task (`exec`, a
/// host-network or floor-less container) needs a `job:write` rule with
/// `host = true`. Same reason as the band test for living here: the check is
/// conditional logic inside an endpoint that already has an ACL check.
#[tokio::test]
async fn host_level_jobs_need_the_host_grant_test() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    svc.store.upsert_acl_policy(
        "deployer",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    svc.store.upsert_acl_policy(
        "platform",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"], "host": true}]"#,
    )?;
    let mint = |policy: &str| {
        authenticator
            .mint_token(&svc.store, policy, "client", &[policy.into()], None)
            .map(|(_, secret)| secret)
            .map_err(|s| anyhow!(s.to_string()))
    };
    let deployer = mint("deployer")?;
    let platform = mint("platform")?;
    let spec = |name: &str, task: serde_json::Value| {
        serde_json::json!({ "name": name, "tasks": [task] }).to_string()
    };
    let exec_task = serde_json::json!({
        "name": "dump", "runtime": "exec", "count": 1,
        "config": { "command": ["echo", "ok"] }, "resources": {}, "constraints": {},
    });
    let container = |extra: serde_json::Value| {
        let mut task = serde_json::json!({
            "name": "web", "runtime": "container", "count": 1,
            "config": { "image": "alpine", "command": ["echo", "ok"] },
            "resources": {}, "constraints": {},
        });
        if let Some(object) = extra.as_object() {
            for (key, value) in object {
                task[key] = value.clone();
            }
        }
        task
    };
    let run = |spec: String, secret: String| {
        let svc = &svc;
        async move {
            svc.run_job(req(
                RunJobRequest {
                    namespace: Default::default(),
                    parsed_jobspec: spec,
                    submissions: vec![],
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
                    allow_unknown_fields: Default::default(),
                },
                Some(&secret),
            ))
            .await
        }
    };

    // A container with the floor and gated (default) networking is not
    // host-level: any job:write may submit it.
    run(
        spec("web", container(serde_json::json!({}))),
        deployer.clone(),
    )
    .await
    .expect("an ordinary container needs no host grant");

    // An exec task, a host-network container, and a floor-less container
    // are host-level: refused without the grant, naming the task and why.
    for (name, task, why) in [
        ("backup", exec_task.clone(), "uses runtime \"exec\""),
        (
            "hostnet",
            container(serde_json::json!({ "network": { "mode": "host" } })),
            "network.mode = \"host\"",
        ),
        (
            "rootful",
            container(serde_json::json!({
                "config": { "image": "alpine", "command": ["true"],
                            "security": { "userNamespace": false } }
            })),
            "security.userNamespace = false",
        ),
    ] {
        let status = run(spec(name, task.clone()), deployer.clone())
            .await
            .expect_err("a host-level job needs the host grant");
        assert_eq!(status.code(), Code::PermissionDenied, "{name}");
        assert!(
            status.message().contains("host-level") && status.message().contains(why),
            "{name}: {}",
            status.message()
        );
        // The host grant admits it.
        run(spec(name, task), platform.clone())
            .await
            .unwrap_or_else(|status| panic!("{name} with the host grant: {status}"));
    }
    Ok(())
}

/// Priority-band scoping: the `bands` selector on `job` rules is what makes
/// `critical` a privilege instead of a label anyone can claim.
///
/// This test exists because `every_manager_rpc_is_classified` cannot catch
/// its regression: band enforcement is CONDITIONAL logic inside an endpoint
/// that already has an ACL check, so dropping it leaves that suite green
/// while the top band silently opens to everyone.
#[tokio::test]
async fn band_scoped_policy_gates_the_priority_a_job_may_claim() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();

    // May submit anything up to `high`, and nothing above it.
    svc.store.upsert_acl_policy(
        "band-limited",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"],
             "bands": ["high", "elevated", "normal", "low", "background"]}]"#,
    )?;
    let (_, secret) = authenticator
        .mint_token(
            &svc.store,
            "band-limited",
            "client",
            &["band-limited".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    let banded_spec = |name: &str, priority: Option<&str>| {
        let mut spec = serde_json::json!({
            "name": name,
            "tasks": [{
                "name": "main",
                "runtime": "container",
                "count": 1,
                "config": { "image": "alpine", "command": ["echo", "ok"] },
                "resources": {},
                "constraints": {},
            }],
        });
        if let Some(priority) = priority {
            spec["priority"] = serde_json::Value::String(priority.into());
        }
        spec.to_string()
    };
    let run = |spec: String, secret: String| {
        let svc = &svc;
        async move {
            svc.run_job(req(
                RunJobRequest {
                    namespace: Default::default(),
                    parsed_jobspec: spec,
                    submissions: vec![],
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
                    allow_unknown_fields: Default::default(),
                },
                Some(&secret),
            ))
            .await
        }
    };

    // Allowed: a band the policy names.
    run(banded_spec("band-high", Some("high")), secret.clone())
        .await
        .expect("a token granted `high` may submit `high`");

    // Allowed: an UNBANDED job. This is the normalization rule doing its
    // work — absent `priority` means `normal`, and the scope carries the
    // defaulted band, so a `bands` policy still matches. Without that, every
    // band-scoped policy would silently stop granting ordinary submissions.
    run(banded_spec("band-absent", None), secret.clone())
        .await
        .expect("an unbanded submit is a `normal` submit, and `normal` is granted");

    // Denied: the one band the policy withholds.
    let status = run(
        banded_spec("band-critical", Some("critical")),
        secret.clone(),
    )
    .await
    .expect_err("a token without `critical` must not claim it");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status.message().contains("critical"),
        "the denial names the band that was refused: {}",
        status.message()
    );

    // Denied through a template too: rendering is not a way around the gate.
    // A RunJob-only check would be bypassable exactly here.
    svc.store.upsert_job_template_version(
        "band-escalation",
        None,
        r#"{
  description = "renders a critical job",
  variables = [],
  job | not_exported = fun vars => {
    name = "band-template-job",
    "priority" = "critical",
    tasks = [{
      name = "main",
      runtime = "noop",
      count = 1,
      config = {},
      resources = {},
      constraints = {},
    }],
  },
}"#,
        &[],
        "fixture",
    )?;
    svc.store.upsert_acl_policy(
        "band-limited-template",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"],
             "bands": ["high", "normal"], "templates": ["band-escalation"]},
            {"resources": ["template"], "actions": ["read"],
             "templates": ["band-escalation"]}]"#,
    )?;
    let (_, template_secret) = authenticator
        .mint_token(
            &svc.store,
            "band-limited-template",
            "client",
            &["band-limited-template".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;
    let status = svc
        .run_job_template(req(
            RunJobTemplateRequest {
                namespace: Default::default(),
                name: "band-escalation".into(),
                version: None,
                variables_json: "{}".into(),
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
                allow_unknown_fields: Default::default(),
            },
            Some(&template_secret),
        ))
        .await
        .expect_err("a template that renders `critical` is gated like a hand-written spec");
    assert_eq!(status.code(), Code::PermissionDenied);

    // A policy with no `bands` selector covers every band — bands are open
    // by default and become a privilege only where a policy says so.
    svc.store.upsert_acl_policy(
        "band-unrestricted",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    let (_, open_secret) = authenticator
        .mint_token(
            &svc.store,
            "band-unrestricted",
            "client",
            &["band-unrestricted".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;
    run(banded_spec("band-open", Some("critical")), open_secret)
        .await
        .expect("an unscoped job:write policy still grants every band");
    Ok(())
}

/// KV path scoping: reads/writes under the granted prefix succeed, others are
/// denied, deny rules carve out subtrees, and ListKv filters entries.
#[tokio::test]
async fn kv_path_scoped_policy_limits_kv_access() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();

    svc.store.upsert_acl_policy(
        "kv-team",
        "",
        r#"[
            {"resources": ["kv"], "actions": ["read", "write"], "paths": ["team/*"]},
            {"resources": ["kv"], "actions": ["*"], "effect": "deny", "paths": ["secret/*"]}
        ]"#,
    )?;
    let (_, secret) = authenticator
        .mint_token(&svc.store, "kv-scoped", "client", &["kv-team".into()], None)
        .map_err(|s| anyhow!(s.to_string()))?;

    call_endpoint(&svc, &fixtures, "GetKv", Some(&secret))
        .await
        .expect("scoped token reads team/ keys");
    call_endpoint(&svc, &fixtures, "PutKv", Some(&secret))
        .await
        .expect("scoped token writes team/ keys");

    // Outside the prefix: denied.
    let status = svc
        .get_kv(req(
            GetKvRequest {
                key: "secret/hidden".into(),
                ..Default::default()
            },
            Some(&secret),
        ))
        .await
        .expect_err("deny rule must block secret/ reads");
    assert_eq!(status.code(), Code::PermissionDenied);

    // ListKv filters to visible paths.
    let reply = svc
        .list_kv(req(
            ListKvRequest {
                prefix: String::new(),
                include_values: true,
            },
            Some(&secret),
        ))
        .await?
        .into_inner();
    assert!(
        reply
            .entries
            .iter()
            .all(|entry| entry.key.starts_with("team/")),
        "list must only contain team/ keys, got {:?}",
        reply
            .entries
            .iter()
            .map(|entry| &entry.key)
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// Reserved names are out of `*`'s reach (apps plan §6.2, calls entry 6): a
/// tenant admin holding `namespaces = ["*"]` and `paths = ["*"]` deploys to
/// every tenant namespace and writes every tenant key, but cannot run a job
/// in `legion-system` or write under `legion/`. A rule that names the prefix
/// reaches both, and even then a root exec task is refused there.
#[tokio::test]
async fn star_policies_do_not_reach_legion_system_or_legion_kv_test() -> R<()> {
    let (svc, _fixtures, management) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    assert!(svc.store.ensure_system_namespace()?);
    svc.store
        .apply_namespace(legion::structs::namespace::Namespace {
            name: "payments".into(),
            ..Default::default()
        })?;
    svc.store.upsert_acl_policy(
        "tenant-admin",
        "",
        r#"[
            {"resources": ["job", "exec", "scale"], "namespaces": ["*"], "actions": ["*"], "host": true},
            {"resources": ["kv"], "actions": ["read", "write"], "paths": ["*"]}
        ]"#,
    )?;
    svc.store.upsert_acl_policy(
        "system-admin",
        "",
        r#"[
            {"resources": ["job"], "namespaces": ["*", "legion-*"], "actions": ["*"], "host": true},
            {"resources": ["kv"], "actions": ["read", "write"], "paths": ["*", "legion/*"]}
        ]"#,
    )?;
    let mint = |policy: &str| {
        authenticator
            .mint_token(&svc.store, policy, "client", &[policy.into()], None)
            .map(|(_, secret)| secret)
            .map_err(|s| anyhow!(s.to_string()))
    };
    let tenant = mint("tenant-admin")?;
    let system = mint("system-admin")?;
    let run = |namespace: &str, task: serde_json::Value, secret: String| {
        let svc = &svc;
        let namespace = namespace.to_string();
        async move {
            svc.run_job(req(
                RunJobRequest {
                    namespace: Some(namespace),
                    parsed_jobspec: serde_json::json!({ "name": "agent", "tasks": [task] })
                        .to_string(),
                    submissions: vec![],
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
                    allow_unknown_fields: false,
                },
                Some(&secret),
            ))
            .await
        }
    };
    let task = serde_json::json!({
        "name": "main", "runtime": "exec",
        "config": { "command": ["sleep", "600"], "user": "nobody" },
    });

    run("payments", task.clone(), tenant.clone())
        .await
        .expect("`*` reaches a tenant namespace");
    let status = run("legion-system", task.clone(), tenant.clone())
        .await
        .expect_err("`*` must not reach legion-system");
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    assert!(
        status
            .message()
            .contains("does not reach reserved `legion-*`"),
        "the denial says why: {status:?}"
    );
    run("legion-system", task.clone(), system.clone())
        .await
        .expect("a pattern starting legion- reaches it");

    // Even with the right to deploy there, a root exec task is refused.
    let mut root_exec = task.clone();
    root_exec["config"]
        .as_object_mut()
        .expect("config")
        .remove("user");
    for secret in [system.clone(), management.clone()] {
        let status = run("legion-system", root_exec.clone(), secret)
            .await
            .expect_err("a root exec task in legion-system");
        assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
        assert!(status.message().contains("runs as root"), "{status:?}");
    }

    let put = |key: &str, secret: &str| {
        let svc = &svc;
        let request = req(
            PutKvRequest {
                key: key.into(),
                value: "v".into(),
                value_type: KvValueType::String.into(),
                ..Default::default()
            },
            Some(secret),
        );
        async move { svc.put_kv(request).await }
    };
    put("team-a/config", &tenant)
        .await
        .expect("`*` reaches tenant keys");
    let status = put("legion/apps/autoscaler/lease", &tenant)
        .await
        .expect_err("`*` must not reach legion/");
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
    assert!(
        status.message().contains("reserved `legion/` prefix"),
        "the denial says why: {status:?}"
    );
    put("legion/apps/autoscaler/lease", &system)
        .await
        .expect("a pattern starting legion/ reaches it");
    Ok(())
}

/// A port's `tls.cert = { kv }` (and `tls.clients.ca`, and an upstream's
/// `ca`) hands a KV value to the datapath, so naming one in a job needs
/// `kv:read` on that path: job-write access alone must not be a way to read
/// private keys.
#[tokio::test]
async fn kv_backed_ingress_cert_needs_kv_read_on_the_path() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    svc.store.upsert_acl_policy(
        "deployer",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    svc.store.upsert_acl_policy(
        "cert-reader",
        "",
        r#"[{"resources": ["kv"], "actions": ["read"], "paths": ["certs/*"]}]"#,
    )?;
    let (_, deployer) = authenticator
        .mint_token(&svc.store, "deployer", "client", &["deployer".into()], None)
        .map_err(|s| anyhow!(s.to_string()))?;
    let (_, trusted) = authenticator
        .mint_token(
            &svc.store,
            "trusted-deployer",
            "client",
            &["deployer".into(), "cert-reader".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    let run = |token: String| {
        let jobspec = serde_json::json!({
            "name": "edge",
            "task": {
                "runtime": "container",
                "config": { "image": "alpine", "command": ["sleep", "30"] },
                "network": {
                    "ports": [{ "name": "https", "static": 8443, "tls": { "cert": { "kv": "certs/edge" } } }],
                },
            },
        });
        let svc = &svc;
        async move {
            svc.run_job(req(
                RunJobRequest {
                    namespace: Default::default(),
                    parsed_jobspec: jobspec.to_string(),
                    submissions: Vec::new(),
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
                    allow_unknown_fields: Default::default(),
                },
                Some(&token),
            ))
            .await
        }
    };

    let status = run(deployer)
        .await
        .expect_err("job:write alone must not name a KV certificate");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status
            .message()
            .contains("network.ports[https].tls.cert names kv certs/edge"),
        "{}",
        status.message()
    );
    run(trusted)
        .await
        .expect("kv:read on the path allows naming it");
    Ok(())
}

/// `connect.upstreams[].credentials` hands a KV value to the datapath, which
/// sends it to the upstream's origin, so naming one in a job needs `kv:read`
/// on that path: job-write access alone must not be a way to send any secret
/// in KV to a host of the submitter's choosing.
#[tokio::test]
async fn upstream_credential_needs_kv_read_on_the_path() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    svc.store.upsert_acl_policy(
        "deployer",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    svc.store.upsert_acl_policy(
        "secret-reader",
        "",
        r#"[{"resources": ["kv"], "actions": ["read"], "paths": ["secrets/*"]}]"#,
    )?;
    let (_, deployer) = authenticator
        .mint_token(&svc.store, "deployer", "client", &["deployer".into()], None)
        .map_err(|s| anyhow!(s.to_string()))?;
    let (_, trusted) = authenticator
        .mint_token(
            &svc.store,
            "trusted-deployer",
            "client",
            &["deployer".into(), "secret-reader".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    let run = |token: String| {
        let jobspec = serde_json::json!({
            "name": "agent",
            "task": {
                // A container, not `exec`: submitting a host-level task
                // needs `host = true`, which is not what this test is about.
                "runtime": "container",
                "config": { "image": "alpine", "command": ["sleep", "30"] },
                "connect": {
                    "upstreams": [{
                        "name": "openai",
                        "to": "https://api.openai.com",
                        "credentials": [{ "header": "authorization", "prefix": "Bearer ", "from": { "kv": "secrets/openai" } }],
                    }],
                },
            },
        });
        let svc = &svc;
        async move {
            svc.run_job(req(
                RunJobRequest {
                    namespace: Default::default(),
                    parsed_jobspec: jobspec.to_string(),
                    submissions: Vec::new(),
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
                    allow_unknown_fields: Default::default(),
                },
                Some(&token),
            ))
            .await
        }
    };

    let status = run(deployer)
        .await
        .expect_err("job:write alone must not name a KV credential");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status
            .message()
            .contains("connect.upstreams[openai].credentials names kv secrets/openai"),
        "{}",
        status.message()
    );
    run(trusted)
        .await
        .expect("kv:read on the path allows naming it");
    Ok(())
}

/// The mesh CA signs every node's certificate, so rotating it needs
/// cluster-wide `net:write`. A pool-scoped net admin may not rotate it.
#[tokio::test]
async fn rotate_mesh_ca_requires_cluster_wide_net_write() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    let mut secrets = Vec::new();
    for (name, rules) in [
        (
            "net-pool-admin",
            r#"[{"resources": ["net"], "actions": ["write"], "pools": ["gpu"]}]"#,
        ),
        (
            "net-cluster-admin",
            r#"[{"resources": ["net"], "actions": ["write"]}]"#,
        ),
    ] {
        svc.store.upsert_acl_policy(name, "", rules)?;
        let (_, secret) = authenticator
            .mint_token(&svc.store, name, "client", &[name.into()], None)
            .map_err(|s| anyhow!(s.to_string()))?;
        secrets.push(secret);
    }

    let status = call_endpoint(&svc, &fixtures, "RotateMeshCa", Some(&secrets[0]))
        .await
        .expect_err("a pool-scoped net admin must not rotate the mesh CA");
    assert_eq!(status.code(), Code::PermissionDenied);
    call_endpoint(&svc, &fixtures, "RotateMeshCa", Some(&secrets[1]))
        .await
        .map_err(|s| anyhow!(s.to_string()))?;
    Ok(())
}

/// Mints a client token carrying one fresh policy with the given rules.
fn token_with_rules(svc: &ManagerDaemonService, name: &str, rules: &str) -> R<(String, String)> {
    svc.store.upsert_acl_policy(name, "", rules)?;
    let (info, secret) = svc
        .auth
        .mint_token(&svc.store, name, "client", &[name.into()], None)
        .map_err(|s| anyhow!(s.to_string()))?;
    Ok((info.accessor_id, secret))
}

/// GetScalingInputs gates before it fetches and filters listings: a caller
/// with `scale:read` on one job reads that job, is refused (not told "job not
/// found") for a job outside its grant, and sees only its job in a namespace
/// listing; a caller with no `scale` grant at all is refused outright.
#[tokio::test]
async fn scaling_inputs_check_before_fetch_and_filter_listings() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let (_, scoped) = token_with_rules(
        &svc,
        "scale-one-job",
        &format!(
            r#"[{{"resources": ["scale"], "namespaces": ["*"], "actions": ["read"], "names": ["{JOB_NAME}"]}}]"#
        ),
    )?;
    let named = |job: &str| GetScalingInputsRequest {
        targets: vec![ScalingTargetSelector {
            job: job.into(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let reply = svc
        .get_scaling_inputs(req(named(JOB_NAME), Some(&scoped)))
        .await?
        .into_inner();
    assert!(!reply.inputs.is_empty());
    assert!(reply
        .inputs
        .iter()
        .all(|inputs| inputs.job_id == JOB_NAME && inputs.error.is_empty()));

    let status = svc
        .get_scaling_inputs(req(named("no-such-job"), Some(&scoped)))
        .await
        .expect_err("a job outside the grant is refused before any lookup");
    assert_eq!(status.code(), Code::PermissionDenied);

    let listing = svc
        .get_scaling_inputs(req(
            GetScalingInputsRequest {
                namespaces: vec!["*".into()],
                ..Default::default()
            },
            Some(&scoped),
        ))
        .await?
        .into_inner();
    assert!(
        listing
            .inputs
            .iter()
            .all(|inputs| inputs.job_id == JOB_NAME),
        "a listing shows only jobs the caller may read, got {:?}",
        listing
            .inputs
            .iter()
            .map(|inputs| &inputs.job_id)
            .collect::<Vec<_>>()
    );

    let (_, job_reader) = token_with_rules(
        &svc,
        "job-read-only",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
    )?;
    let status = svc
        .get_scaling_inputs(req(named(JOB_NAME), Some(&job_reader)))
        .await
        .expect_err("job:read is not scale:read");
    assert_eq!(status.code(), Code::PermissionDenied);
    Ok(())
}

/// Node lookups gate before they fetch: a caller with no node access gets
/// PermissionDenied for a node that does not exist, not NotFound, so it
/// cannot probe which node ids exist.
#[tokio::test]
async fn node_lookups_check_before_fetch() -> R<()> {
    let (svc, _, _) = setup_enabled()?;
    let (_, secret) = token_with_rules(
        &svc,
        "jobs-only",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    let missing = "no-such-node".to_string();
    let results = [
        (
            "GetNode",
            svc.get_node(req(
                GetNodeRequest {
                    id: missing.clone(),
                },
                Some(&secret),
            ))
            .await
            .map(|_| ()),
        ),
        (
            "RestartWorker",
            svc.restart_worker(req(
                RestartWorkerRequest {
                    node_id: missing.clone(),
                },
                Some(&secret),
            ))
            .await
            .map(|_| ()),
        ),
        (
            "UpdateNodeDynamic",
            svc.update_node_dynamic(req(
                UpdateNodeDynamicRequest {
                    id: missing.clone(),
                    dynamic_diff: "{}".into(),
                },
                Some(&secret),
            ))
            .await
            .map(|_| ()),
        ),
    ];
    for (method, result) in results {
        let status = result.expect_err(&format!("{method} must deny"));
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{method}: {}",
            status.message()
        );
    }
    Ok(())
}

/// A volume no job owns takes cluster-wide `job:write` to delete; a
/// name-scoped job writer may not delete it.
#[tokio::test]
async fn jobless_volume_delete_requires_cluster_wide_job_write() -> R<()> {
    let (svc, _, management_secret) = setup_enabled()?;
    svc.query_state_store(req(
        QueryStateStoreRequest {
            sql: "INSERT INTO volume (volume_id, driver, state, created_at, updated_at) \
                  VALUES ('vol-orphan', 'host', 'pending', '2026-01-01T00:00:00Z', \
                  '2026-01-01T00:00:00Z')"
                .into(),
            allow_write: true,
        },
        Some(&management_secret),
    ))
    .await?;
    let delete = |secret: &str| {
        req(
            DeleteVolumeRequest {
                volume_id: "vol-orphan".into(),
                force: true,
                idempotency_key: None,
            },
            Some(secret),
        )
    };

    let (_, scoped) = token_with_rules(
        &svc,
        "acl-jobs-writer",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["write"], "names": ["acl-*"]}]"#,
    )?;
    let status = svc
        .delete_volume(delete(&scoped))
        .await
        .expect_err("a name-scoped job writer must not delete a jobless volume");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "{}",
        status.message()
    );

    let (_, cluster) = token_with_rules(
        &svc,
        "all-jobs-writer",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["write"]}]"#,
    )?;
    svc.delete_volume(delete(&cluster)).await?;
    Ok(())
}

/// A stream outlives no credential: revoking the token closes `StreamEvents`
/// within one re-check interval.
#[tokio::test]
async fn stream_events_closes_when_its_token_is_revoked() -> R<()> {
    use futures::StreamExt;
    let (svc, _, management_secret) = setup_enabled()?;
    let svc = svc.with_stream_reauth_interval(std::time::Duration::from_millis(50));
    let (accessor, secret) = token_with_rules(
        &svc,
        "event-reader",
        r#"[{"resources": ["operator"], "actions": ["read"]}]"#,
    )?;
    let mut stream = svc
        .stream_events(req(StreamEventsRequest::default(), Some(&secret)))
        .await?
        .into_inner();

    svc.delete_acl_token(req(
        DeleteAclTokenRequest {
            accessor_id: accessor,
        },
        Some(&management_secret),
    ))
    .await?;

    let next = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .context("stream must close after revocation")?;
    let status = next
        .context("stream ended without a status")?
        .expect_err("a revoked token must end the stream with an error");
    assert_eq!(status.code(), Code::Unauthenticated, "{}", status.message());
    Ok(())
}

/// Idempotency keys are scoped to one operation and one caller. A key
/// reused on a different operation must not skip it, and another caller's
/// key must not replay their reply.
#[tokio::test]
async fn idempotency_keys_are_scoped_to_operation_and_caller() -> R<()> {
    let (svc, _, _) = setup_enabled()?;
    let rules = r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#;
    let (_, alice) = token_with_rules(&svc, "alice-jobs", rules)?;
    let (_, bob) = token_with_rules(&svc, "bob-jobs", rules)?;
    let stop = |secret: &str| {
        req(
            StopJobRequest {
                name: JOB_NAME.into(),
                idempotency_key: Some("shared-key".into()),
                ..Default::default()
            },
            Some(secret),
        )
    };
    let destroy = |secret: &str| {
        req(
            DestroyJobRequest {
                name: JOB_NAME.into(),
                idempotency_key: Some("shared-key".into()),
                ..Default::default()
            },
            Some(secret),
        )
    };

    let first = svc.stop_job(stop(&alice)).await?.into_inner();
    assert!(!first.deduplicated);
    // Same caller, same operation, same key: replayed.
    let replay = svc.stop_job(stop(&alice)).await?.into_inner();
    assert!(replay.deduplicated);
    assert_eq!(replay.message, first.message);
    // Another caller with the same key is a new request.
    let other = svc.stop_job(stop(&bob)).await?.into_inner();
    assert!(!other.deduplicated);
    // The same key on another operation runs it.
    let destroyed = svc.destroy_job(destroy(&alice)).await?.into_inner();
    assert!(!destroyed.deduplicated);
    Ok(())
}

/// Node-pool scoping: a token limited to one pool cannot read or restart
/// nodes in other pools, and GetNodes filters.
#[tokio::test]
async fn node_pool_scoped_policy_limits_node_access() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();

    svc.store.insert_node(NodeMsg {
        id: "gpu-node".into(),
        pool: "gpu".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: "{}".into(),
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "[]".into(),
        last_heartbeat: "2026-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
    })?;

    svc.store.upsert_acl_policy(
        "gpu-pool-only",
        "",
        r#"[{"resources": ["node"], "actions": ["read"], "pools": ["gpu"]}]"#,
    )?;
    let (_, secret) = authenticator
        .mint_token(
            &svc.store,
            "pool-scoped",
            "client",
            &["gpu-pool-only".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    // The gpu node is visible; the default-pool node is not.
    svc.get_node(req(
        GetNodeRequest {
            id: "gpu-node".into(),
        },
        Some(&secret),
    ))
    .await
    .expect("pool-scoped token reads its pool");
    let status = call_endpoint(&svc, &fixtures, "GetNode", Some(&secret))
        .await
        .expect_err("default pool must be denied");
    assert_eq!(status.code(), Code::PermissionDenied);

    let reply = svc
        .get_nodes(req(GetNodesRequest::default(), Some(&secret)))
        .await?
        .into_inner();
    assert!(
        reply.nodes.iter().all(|node| node.pool == "gpu"),
        "node list must be filtered to the gpu pool",
    );

    // Read-only pool access must not grant writes.
    let status = call_endpoint(&svc, &fixtures, "RestartWorker", Some(&secret))
        .await
        .expect_err("read-only token must not restart workers");
    assert_eq!(status.code(), Code::PermissionDenied);
    Ok(())
}

/// The anonymous policy grants tokenless access during migrations.
#[tokio::test]
async fn anonymous_policy_grants_tokenless_access() -> R<()> {
    let (svc, fixtures, _) = setup_enabled()?;

    svc.store.upsert_acl_policy(
        "anonymous",
        "migration ramp",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"]}]"#,
    )?;

    call_endpoint(&svc, &fixtures, "ListJobs", None)
        .await
        .expect("anonymous policy must allow job reads");
    let status = call_endpoint(&svc, &fixtures, "StopJob", None)
        .await
        .expect_err("anonymous policy must not allow job writes");
    // Beyond what the anonymous policy grants, a credential-less caller is
    // told to authenticate, not that its (nonexistent) policies fall short.
    assert_eq!(status.code(), Code::Unauthenticated);
    Ok(())
}

/// Audit-only mode allows everything while ACL evaluation still runs.
#[tokio::test]
async fn audit_only_mode_allows_denied_requests() -> R<()> {
    let store = setup_store()?;
    let fixtures = seed_fixtures(&store)?;
    let authenticator = Arc::new(
        Authenticator::from_config(
            &AclConfig {
                enabled: Some(true),
                audit_only: Some(true),
                ..Default::default()
            }
            .with_defaults(),
        )
        .expect("authenticator should build"),
    );
    let svc = ManagerDaemonService::with_acl(store, authenticator);

    for (method, _) in endpoint_table() {
        if method == "Login" {
            continue; // Login still validates credentials in audit mode.
        }
        if let Err(status) = call_endpoint(&svc, &fixtures, method, None).await {
            assert!(
                !is_auth_error(status.code()),
                "{method} must not be denied in audit-only mode, got {}: {}",
                status.code(),
                status.message(),
            );
        }
    }
    Ok(())
}

/// WhoAmI reports ACL state to anonymous, scoped, and management callers.
#[tokio::test]
async fn who_am_i_reports_identity() -> R<()> {
    let (svc, _, management_secret) = setup_enabled()?;

    let anonymous = svc
        .who_am_i(req(WhoAmIRequest {}, None))
        .await?
        .into_inner();
    assert!(anonymous.acl_enabled);
    assert_eq!(anonymous.source, "anonymous");
    assert_eq!(anonymous.token_type, "none");

    let management = svc
        .who_am_i(req(WhoAmIRequest {}, Some(&management_secret)))
        .await?
        .into_inner();
    assert_eq!(management.source, "token");
    assert_eq!(management.token_type, "management");
    assert!(management.accessor_id.is_some());

    let store = setup_store()?;
    let disabled = ManagerDaemonService::new(store);
    let reply = disabled
        .who_am_i(req(WhoAmIRequest {}, None))
        .await?
        .into_inner();
    assert!(!reply.acl_enabled);
    assert_eq!(reply.source, "disabled");
    Ok(())
}

/// Login end-to-end: a userpass login mints a token that authenticates with
/// the user's policies; bad credentials are rejected.
#[tokio::test]
async fn login_flow_mints_usable_token() -> R<()> {
    let (svc, fixtures, management_secret) = setup_enabled()?;

    svc.upsert_acl_user(req(
        UpsertAclUserRequest {
            username: "alice".into(),
            password: "hunter2".into(),
            policies: vec![FIXTURE_POLICY.into()],
        },
        Some(&management_secret),
    ))
    .await?;

    let bad = svc
        .login(req(
            LoginRequest {
                method: "userpass".into(),
                username: Some("alice".into()),
                password: Some("wrong".into()),
                ..Default::default()
            },
            None,
        ))
        .await;
    assert_eq!(bad.unwrap_err().code(), Code::Unauthenticated);

    let reply = svc
        .login(req(
            LoginRequest {
                method: "userpass".into(),
                username: Some("alice".into()),
                password: Some("hunter2".into()),
                ..Default::default()
            },
            None,
        ))
        .await?
        .into_inner();
    let secret = reply.secret;

    // fixture-policy grants job:read only.
    call_endpoint(&svc, &fixtures, "ListJobs", Some(&secret))
        .await
        .expect("login token must grant the user's policies");
    let status = call_endpoint(&svc, &fixtures, "PutKv", Some(&secret))
        .await
        .expect_err("login token must not exceed the user's policies");
    assert_eq!(status.code(), Code::PermissionDenied);
    Ok(())
}

/// Privilege escalation guard: acl:write alone cannot mint management tokens.
#[tokio::test]
async fn client_token_cannot_mint_management_tokens() -> R<()> {
    let (svc, _, management_secret) = setup_enabled()?;

    svc.store.upsert_acl_policy(
        "acl-admin",
        "",
        r#"[{"resources": ["acl"], "actions": ["read", "write"]}]"#,
    )?;
    let (_, secret) = svc
        .auth
        .mint_token(
            &svc.store,
            "acl-admin-token",
            "client",
            &["acl-admin".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    // The acl:write client token can mint client tokens...
    svc.create_acl_token(req(
        CreateAclTokenRequest {
            name: "minted-client".into(),
            token_type: "client".into(),
            policies: vec![FIXTURE_POLICY.into()],
            ttl: None,
        },
        Some(&secret),
    ))
    .await
    .expect("acl:write must allow minting client tokens");

    // ...but not management tokens.
    let status = svc
        .create_acl_token(req(
            CreateAclTokenRequest {
                name: "sneaky".into(),
                token_type: "management".into(),
                policies: vec![],
                ttl: None,
            },
            Some(&secret),
        ))
        .await
        .expect_err("client tokens must not mint management tokens");
    assert_eq!(status.code(), Code::PermissionDenied);

    // A management token can.
    svc.create_acl_token(req(
        CreateAclTokenRequest {
            name: "second-mgmt".into(),
            token_type: "management".into(),
            policies: vec![],
            ttl: None,
        },
        Some(&management_secret),
    ))
    .await
    .expect("management tokens may mint management tokens");
    Ok(())
}

/// Version handshake round-trip: registration stores the worker's build and
/// protocol versions into the node's `dynamic_attrs` (so `legion node list`
/// and the UI show fleet skew) and the reply carries the manager's versions.
/// Workers reporting protocol version 0 (pre-versioning builds) are admitted;
/// workers reporting a version newer than the manager's are rejected with
/// FAILED_PRECONDITION (managers must be upgraded first).
#[tokio::test]
async fn worker_register_version_handshake_round_trip() -> R<()> {
    use legion::constants::PROTOCOL_VERSION;

    let store = setup_store()?;
    seed_fixtures(&store)?;
    let svc = ManagerDaemonService::new(store.clone());

    // A current-version worker registers; the reply carries manager versions.
    let reply = svc
        .worker_register(req(
            WorkerRegisterRequest {
                name: "versioned-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: None,
                build_version: "9.9.9-test".into(),
                protocol_version: PROTOCOL_VERSION,
                capabilities: vec!["checkpoint".into()],
                build_id: "test-build".into(),
            },
            None,
        ))
        .await?
        .into_inner();
    assert_eq!(reply.manager_build_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(reply.manager_protocol_version, PROTOCOL_VERSION);

    // The versions land in the node's dynamic attrs.
    let node = store.get_node("versioned-worker")?;
    assert_eq!(node.dynamic_attrs["buildVersion"], "9.9.9-test");
    // Capabilities are recorded, so checkpoint work only goes to workers
    // that advertised it.
    assert_eq!(
        node.dynamic_attrs["capabilities"],
        serde_json::json!(["checkpoint"])
    );
    assert_eq!(
        node.dynamic_attrs["protocolVersion"],
        serde_json::json!(PROTOCOL_VERSION)
    );

    // A pre-versioning worker (proto3 defaults: "" / 0) is still admitted so
    // rolling upgrades from older releases work; its attrs say "unknown".
    svc.worker_register(req(
        WorkerRegisterRequest {
            name: "legacy-worker".into(),
            pool: "default".into(),
            config_attrs: "{}".into(),
            fingerprint_attrs: "{}".into(),
            health_checks: "[]".into(),
            join_token: None,
            ..Default::default()
        },
        None,
    ))
    .await
    .expect("pre-versioning workers must be admitted");
    let legacy = store.get_node("legacy-worker")?;
    assert_eq!(legacy.dynamic_attrs["buildVersion"], "unknown");
    assert_eq!(
        legacy.dynamic_attrs["protocolVersion"],
        serde_json::json!(0)
    );

    // A worker from the future (upgraded before the manager) is rejected
    // with FAILED_PRECONDITION and an actionable upgrade-order message.
    let status = svc
        .worker_register(req(
            WorkerRegisterRequest {
                name: "future-worker".into(),
                pool: "default".into(),
                config_attrs: "{}".into(),
                fingerprint_attrs: "{}".into(),
                health_checks: "[]".into(),
                join_token: None,
                build_version: "99.0.0".into(),
                protocol_version: PROTOCOL_VERSION + 1,
                capabilities: Vec::new(),
                build_id: String::new(),
            },
            None,
        ))
        .await
        .expect_err("newer-protocol workers must be rejected");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("Upgrade all managers first"),
        "rejection must tell the operator the upgrade order, got: {}",
        status.message()
    );
    assert!(
        store.get_node("future-worker").is_err(),
        "rejected workers must not be inserted into the node table"
    );
    Ok(())
}

/// SuspendInstance is `job:write`, plus `snapshot:write` for a memory
/// checkpoint (a memory checkpoint holds the workload's in-memory secrets).
/// An explicit `checkpoint = "memory"` is refused before any lookup.
#[tokio::test]
async fn memory_suspend_needs_snapshot_write() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    svc.store.upsert_acl_policy(
        "job-writer",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    let (_, secret) = authenticator
        .mint_token(
            &svc.store,
            "job-writer",
            "client",
            &["job-writer".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;
    let suspend = |checkpoint: Option<&str>| legion::structs::SuspendInstanceRequest {
        instance_id: "missing-instance".into(),
        checkpoint: checkpoint.map(str::to_string),
        idempotency_key: None,
    };

    let status = svc
        .suspend_instance(req(suspend(Some("memory")), Some(&secret)))
        .await
        .expect_err("memory needs snapshot:write");
    assert_eq!(status.code(), Code::PermissionDenied);
    // A disk suspend passes the ACL check and reaches the lookup.
    let status = svc
        .suspend_instance(req(suspend(Some("disk")), Some(&secret)))
        .await
        .expect_err("no such instance");
    assert_eq!(status.code(), Code::NotFound);
    Ok(())
}

/// Phase 6: ForkInstance needs `snapshot:write` as well as `job:write` (a
/// fork reads everything the origin held in memory), and a create from a
/// snapshot needs `snapshot:read` as well as `job:write`. Both are refused
/// before any lookup.
#[tokio::test]
async fn fork_and_create_from_snapshot_need_snapshot_grants() -> R<()> {
    let (svc, _fixtures, _) = setup_enabled()?;
    let authenticator = svc.auth.clone();
    svc.store.upsert_acl_policy(
        "job-writer",
        "",
        r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read", "write"]}]"#,
    )?;
    let (_, secret) = authenticator
        .mint_token(
            &svc.store,
            "job-writer",
            "client",
            &["job-writer".into()],
            None,
        )
        .map_err(|s| anyhow!(s.to_string()))?;

    let status = svc
        .fork_instance(req(
            ForkInstanceRequest {
                instance_id: "missing-instance".into(),
                count: 3,
                ..Default::default()
            },
            Some(&secret),
        ))
        .await
        .expect_err("fork needs snapshot:write");
    assert_eq!(status.code(), Code::PermissionDenied);

    let status = svc
        .create_instances(req(
            CreateInstancesRequest {
                job_id: JOB_NAME.into(),
                count: 2,
                from_snapshot: Some("golden".into()),
                ..Default::default()
            },
            Some(&secret),
        ))
        .await
        .expect_err("a create from a snapshot needs snapshot:read");
    assert_eq!(status.code(), Code::PermissionDenied);
    Ok(())
}

// ---------------------------------------------------------------------------
// Namespaces (docs/namespaces.md)
// ---------------------------------------------------------------------------

fn run_request(spec: &str, namespace: Option<&str>, token: &str) -> Request<RunJobRequest> {
    req(
        RunJobRequest {
            parsed_jobspec: spec.to_string(),
            namespace: namespace.map(str::to_string),
            ..Default::default()
        },
        Some(token),
    )
}

/// A spec that validates without locally built plugin dylibs and is not
/// host-level (the built-in `container` runtime, as `template_source`), in
/// `namespace` when given.
fn namespaced_jobspec(name: &str, namespace: Option<&str>) -> String {
    let mut spec = serde_json::json!({
        "name": name,
        "tasks": [{
            "name": "main",
            "runtime": "container",
            "count": 1,
            "config": { "image": "alpine", "command": ["echo", "ok"] },
            "resources": {},
            "constraints": {},
        }],
    });
    if let Some(namespace) = namespace {
        spec["namespace"] = serde_json::json!(namespace);
    }
    spec.to_string()
}

async fn create_namespaces(svc: &ManagerDaemonService, secret: &str, names: &[&str]) -> R<()> {
    for name in names {
        svc.apply_namespace(req(
            ApplyNamespaceRequest {
                namespace: Some(Namespace {
                    name: name.to_string(),
                    ..Default::default()
                }),
                dry_run: false,
            },
            Some(secret),
        ))
        .await?;
    }
    Ok(())
}

/// Scenario 1 and 2 of the plan: two namespaces hold a job of the same name,
/// and a token scoped to one cannot see, stop or destroy the other's — with
/// or without asking for every namespace.
#[tokio::test]
async fn a_namespace_scoped_token_stays_inside_its_namespaces() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    create_namespaces(&svc, &management, &["payments-prod", "checkout-prod"]).await?;
    for namespace in ["payments-prod", "checkout-prod"] {
        svc.run_job(run_request(
            &namespaced_jobspec("web", None),
            Some(namespace),
            &management,
        ))
        .await?;
    }
    // Same name, two namespaces, two jobs.
    let all = svc
        .list_jobs(req(
            ListJobsRequest {
                namespaces: vec!["*".into()],
                ..Default::default()
            },
            Some(&management),
        ))
        .await?
        .into_inner();
    let ids: BTreeSet<_> = all.jobs.iter().map(|job| job.id.clone()).collect();
    assert!(ids.contains("payments-prod/web") && ids.contains("checkout-prod/web"));
    let payments_web = all
        .jobs
        .iter()
        .find(|job| job.id == "payments-prod/web")
        .expect("payments-prod/web");
    assert_eq!(payments_web.name, "web");
    assert_eq!(payments_web.namespace, "payments-prod");

    let (_, team) = token_with_rules(
        &svc,
        "payments-team",
        r#"[{"resources": ["job"], "actions": ["read", "write"], "namespaces": ["payments-*"]}]"#,
    )?;

    // Every namespace it may read, and nothing else — `*` widens what is
    // considered, never what is visible.
    for selection in [
        vec!["*".to_string()],
        vec!["payments-prod,checkout-prod".to_string()],
    ] {
        let listed = svc
            .list_jobs(req(
                ListJobsRequest {
                    namespaces: selection,
                    ..Default::default()
                },
                Some(&team),
            ))
            .await?
            .into_inner();
        let ids: Vec<_> = listed.jobs.iter().map(|job| job.id.as_str()).collect();
        assert_eq!(ids, vec!["payments-prod/web"], "{ids:?}");
    }
    // The default selection is `default`, and the reply says where the
    // caller's jobs are instead of looking empty.
    let default = svc
        .list_jobs(req(ListJobsRequest::default(), Some(&team)))
        .await?
        .into_inner();
    assert!(default.jobs.is_empty());
    assert_eq!(default.elsewhere_count, 1);
    assert_eq!(default.elsewhere_namespaces, 1);

    svc.get_job_info(req(
        GetJobInfoRequest {
            name: "payments-prod/web".into(),
        },
        Some(&team),
    ))
    .await?;
    for (rpc, result) in [
        (
            "GetJobInfo",
            svc.get_job_info(req(
                GetJobInfoRequest {
                    name: "checkout-prod/web".into(),
                },
                Some(&team),
            ))
            .await
            .map(|_| ()),
        ),
        (
            "StopJob",
            svc.stop_job(req(
                StopJobRequest {
                    name: "checkout-prod/web".into(),
                    ..Default::default()
                },
                Some(&team),
            ))
            .await
            .map(|_| ()),
        ),
        (
            "DestroyJob",
            svc.destroy_job(req(
                DestroyJobRequest {
                    name: "checkout-prod/web".into(),
                    ..Default::default()
                },
                Some(&team),
            ))
            .await
            .map(|_| ()),
        ),
        (
            "RunJob",
            svc.run_job(run_request(
                &namespaced_jobspec("api", None),
                Some("checkout-prod"),
                &team,
            ))
            .await
            .map(|_| ()),
        ),
    ] {
        let status = result.expect_err(rpc);
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{rpc}: {}",
            status.message()
        );
    }
    // Its own namespace works end to end.
    svc.run_job(run_request(
        &namespaced_jobspec("api", None),
        Some("payments-prod"),
        &team,
    ))
    .await?;
    Ok(())
}

/// The spec's namespace and the request's must agree, authorization runs
/// against the resolved one (Nomad's CVE-2024-10975 was a spec field
/// overriding an authorized request field), and namespaces are never
/// created implicitly.
#[tokio::test]
async fn submit_resolves_the_namespace_server_side() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    create_namespaces(&svc, &management, &["dev", "prod"]).await?;
    let (_, dev_only) = token_with_rules(
        &svc,
        "dev-only",
        r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["dev"]}]"#,
    )?;

    let conflict = svc
        .run_job(run_request(
            &namespaced_jobspec("api", Some("prod")),
            Some("dev"),
            &dev_only,
        ))
        .await
        .expect_err("a spec/request conflict is rejected");
    assert_eq!(conflict.code(), Code::InvalidArgument);
    assert!(
        conflict
            .message()
            .contains("jobspec declares namespace 'prod' but the request names 'dev'"),
        "{}",
        conflict.message()
    );

    // The spec's namespace is what is authorized when the request names none.
    let denied = svc
        .run_job(run_request(
            &namespaced_jobspec("api", Some("prod")),
            None,
            &dev_only,
        ))
        .await
        .expect_err("a dev token may not write into prod via the spec");
    assert_eq!(denied.code(), Code::PermissionDenied);

    svc.run_job(run_request(
        &namespaced_jobspec("api", Some("dev")),
        Some("dev"),
        &dev_only,
    ))
    .await?;

    let missing = svc
        .run_job(run_request(
            &namespaced_jobspec("api", None),
            Some("does-not-exist"),
            &management,
        ))
        .await
        .expect_err("a missing namespace is refused");
    assert_eq!(missing.code(), Code::NotFound);
    assert!(
        missing
            .message()
            .contains("namespace 'does-not-exist' does not exist"),
        "{}",
        missing.message()
    );
    // A caller with no grant there learns nothing about whether it exists.
    let probe = svc
        .run_job(run_request(
            &namespaced_jobspec("api", None),
            Some("does-not-exist"),
            &dev_only,
        ))
        .await
        .expect_err("probe");
    assert_eq!(probe.code(), Code::PermissionDenied);
    Ok(())
}

/// Namespace creation is delegable by glob, listing is per item, and a
/// cascade delete needs job:write as well as namespace:write.
#[tokio::test]
async fn namespace_administration_is_delegable_and_cascades_need_job_write() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    let (_, lead) = token_with_rules(
        &svc,
        "payments-lead",
        r#"[{"resources": ["namespace"], "actions": ["read", "write"], "namespaces": ["payments-*"]}]"#,
    )?;
    create_namespaces(&svc, &lead, &["payments-loadtest"]).await?;
    let denied = svc
        .apply_namespace(req(
            ApplyNamespaceRequest {
                namespace: Some(Namespace {
                    name: "checkout-prod".into(),
                    ..Default::default()
                }),
                dry_run: false,
            },
            Some(&lead),
        ))
        .await
        .expect_err("outside its glob");
    assert_eq!(denied.code(), Code::PermissionDenied);

    create_namespaces(&svc, &management, &["checkout-prod"]).await?;
    let visible: Vec<_> = svc
        .list_namespaces(req(ListNamespacesRequest::default(), Some(&lead)))
        .await?
        .into_inner()
        .namespaces
        .into_iter()
        .map(|namespace| namespace.name)
        .collect();
    assert_eq!(visible, vec!["payments-loadtest".to_string()]);
    let who = svc
        .who_am_i(req(WhoAmIRequest::default(), Some(&lead)))
        .await?
        .into_inner();
    assert_eq!(
        who.readable_namespaces,
        vec!["payments-loadtest".to_string()]
    );

    // A job lands in it; deleting without --cascade is refused and says why.
    svc.run_job(run_request(
        &namespaced_jobspec("soak", None),
        Some("payments-loadtest"),
        &management,
    ))
    .await?;
    let refused = svc
        .delete_namespace(req(
            DeleteNamespaceRequest {
                name: "payments-loadtest".into(),
                ..Default::default()
            },
            Some(&lead),
        ))
        .await
        .expect_err("not empty");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().contains("is not empty: 1 job"),
        "{}",
        refused.message()
    );
    // A cascade would destroy a job the lead cannot otherwise touch.
    let cascade = svc
        .delete_namespace(req(
            DeleteNamespaceRequest {
                name: "payments-loadtest".into(),
                cascade: true,
                ..Default::default()
            },
            Some(&lead),
        ))
        .await
        .expect_err("cascade without job:write");
    assert_eq!(cascade.code(), Code::PermissionDenied);

    // The dry run lists the contents; the management cascade empties it.
    let dry = svc
        .delete_namespace(req(
            DeleteNamespaceRequest {
                name: "payments-loadtest".into(),
                dry_run: true,
                ..Default::default()
            },
            Some(&lead),
        ))
        .await?
        .into_inner();
    assert_eq!(
        dry.contents.expect("contents").jobs,
        vec!["payments-loadtest/soak".to_string()]
    );
    let deleted = svc
        .delete_namespace(req(
            DeleteNamespaceRequest {
                name: "payments-loadtest".into(),
                cascade: true,
                ..Default::default()
            },
            Some(&management),
        ))
        .await?
        .into_inner();
    assert!(deleted.deleted);
    assert!(
        deleted
            .steps
            .iter()
            .any(|step| step == "Destroyed payments-loadtest/soak"),
        "{:?}",
        deleted.steps
    );
    let gone = svc
        .get_namespace(req(
            GetNamespaceRequest {
                name: "payments-loadtest".into(),
            },
            Some(&management),
        ))
        .await
        .expect_err("deleted");
    assert_eq!(gone.code(), Code::NotFound);

    let default = svc
        .delete_namespace(req(
            DeleteNamespaceRequest {
                name: "default".into(),
                ..Default::default()
            },
            Some(&management),
        ))
        .await
        .expect_err("default stays");
    assert_eq!(default.code(), Code::FailedPrecondition);
    Ok(())
}

/// ACL rules must say their namespace reach, so a policy written for one
/// namespace never silently reaches the next one created.
#[tokio::test]
async fn policies_must_name_their_namespaces() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    let status = svc
        .upsert_acl_policy(req(
            UpsertAclPolicyRequest {
                name: "implicit".into(),
                description: String::new(),
                rules_json: r#"[{"resources": ["job"], "actions": ["read"]}]"#.into(),
            },
            Some(&management),
        ))
        .await
        .expect_err("a job rule without namespaces");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("must say which namespaces it covers"),
        "{}",
        status.message()
    );
    Ok(())
}

/// A namespace tenant reads its own events without operator:read, and only
/// its own: other namespaces' job events and cluster events are left out,
/// and a namespace filter narrows but never widens that.
#[tokio::test]
async fn a_tenant_reads_only_its_own_namespaces_events() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    create_namespaces(&svc, &management, &["payments-prod", "checkout-prod"]).await?;
    for namespace in ["payments-prod", "checkout-prod"] {
        svc.run_job(run_request(
            &namespaced_jobspec("web", None),
            Some(namespace),
            &management,
        ))
        .await?;
    }
    let (_, team) = token_with_rules(
        &svc,
        "payments-reader",
        r#"[{"resources": ["job"], "actions": ["read"], "namespaces": ["payments-*"]}]"#,
    )?;
    let operator = svc
        .list_events(req(ListEventsRequest::default(), Some(&management)))
        .await?
        .into_inner();
    assert!(operator
        .events
        .iter()
        .any(|event| event.namespace == "checkout-prod"));

    for namespaces in [
        vec![],
        vec!["*".to_string()],
        vec!["checkout-prod".to_string()],
    ] {
        let events = svc
            .list_events(req(
                ListEventsRequest {
                    namespaces: namespaces.clone(),
                    ..Default::default()
                },
                Some(&team),
            ))
            .await?
            .into_inner()
            .events;
        assert!(
            events
                .iter()
                .all(|event| event.namespace == "payments-prod"),
            "{namespaces:?}: {:?}",
            events
                .iter()
                .map(|event| (&event.kind, &event.namespace))
                .collect::<Vec<_>>()
        );
        if namespaces != vec!["checkout-prod".to_string()] {
            assert!(!events.is_empty(), "{namespaces:?}");
        }
    }

    // A token with neither operator:read nor any job:read is still refused.
    let (_, nothing) = token_with_rules(
        &svc,
        "kv-only",
        r#"[{"resources": ["kv"], "actions": ["read"]}]"#,
    )?;
    let status = svc
        .list_events(req(ListEventsRequest::default(), Some(&nothing)))
        .await
        .expect_err("no grant");
    assert_eq!(status.code(), Code::PermissionDenied);
    Ok(())
}

/// Phase 4: a namespace policy is written at acl privilege, refuses a
/// submit outside its runtimes and over its aggregate quota, and shows the
/// tenant its effective limits through `GetNamespace` without `acl:read`.
#[tokio::test]
async fn namespace_policies_govern_admission_and_show_tenants_their_limits() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    create_namespaces(&svc, &management, &["payments-prod", "payments-qa"]).await?;
    let (_, team) = token_with_rules(
        &svc,
        "payments-team",
        r#"[{"resources": ["job", "namespace"], "actions": ["read", "write"], "namespaces": ["payments-*"]}]"#,
    )?;
    let policy = serde_json::json!({
        "name": "payments",
        "namespaces": ["payments-*"],
        "quota": { "normal": { "cpu": 1500 } },
        "runtimes": ["container"],
    })
    .to_string();

    // A namespace writer cannot raise its own limits.
    let status = svc
        .apply_namespace_policy(req(
            ApplyNamespacePolicyRequest {
                object_json: policy.clone(),
                ..Default::default()
            },
            Some(&team),
        ))
        .await
        .expect_err("needs acl:write");
    assert_eq!(status.code(), Code::PermissionDenied);
    svc.apply_namespace_policy(req(
        ApplyNamespacePolicyRequest {
            object_json: policy,
            ..Default::default()
        },
        Some(&management),
    ))
    .await?;

    let spec = |name: &str, runtime: &str, count: u32| {
        serde_json::json!({
            "name": name,
            "tasks": [{
                "name": "main",
                "runtime": runtime,
                "count": count,
                "config": { "image": "alpine", "command": ["echo", "ok"] },
                "resources": { "cpu": 500, "memory": 64 },
                "constraints": {},
            }],
        })
        .to_string()
    };
    svc.run_job(run_request(
        &spec("web", "container", 2),
        Some("payments-prod"),
        &team,
    ))
    .await?;
    // 1000 of 1500 used in prod; qa shares the cap.
    let status = svc
        .run_job(run_request(
            &spec("load", "container", 2),
            Some("payments-qa"),
            &team,
        ))
        .await
        .expect_err("over the aggregate quota");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status
            .message()
            .contains("caps band 'normal' at 1500 MHz CPU across payments-*"),
        "{}",
        status.message()
    );
    let status = svc
        .run_job(run_request(
            &spec("tool", "sandbox", 1),
            Some("payments-qa"),
            &team,
        ))
        .await
        .expect_err("runtime not allowed");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("allows runtimes [container]"),
        "{}",
        status.message()
    );

    // The tenant reads its limits and the aggregate without acl:read.
    let reply = svc
        .get_namespace(req(
            GetNamespaceRequest {
                name: "payments-qa".into(),
            },
            Some(&team),
        ))
        .await?
        .into_inner();
    assert!(reply.reaching_rules.is_empty(), "rule names need acl:read");
    let governing = reply.policies.first().expect("one policy governs it");
    assert_eq!(
        governing.policy.as_ref().map(|p| p.name.as_str()),
        Some("payments")
    );
    let normal = governing
        .usage
        .iter()
        .find(|usage| usage.band == "normal")
        .expect("normal band usage");
    assert_eq!(
        normal.cpu, 1000.0,
        "prod's declared demand counts toward qa's cap"
    );
    assert!(svc
        .list_namespace_policies(req(ListNamespacePoliciesRequest::default(), Some(&team)))
        .await
        .is_err());

    // The dry run of a tighter policy names what it would have refused.
    let reply = svc
        .apply_namespace_policy(req(
            ApplyNamespacePolicyRequest {
                object_json: serde_json::json!({
                    "name": "payments",
                    "namespaces": ["payments-*"],
                    "maxJobs": 0,
                    "runtimes": ["sandbox"],
                })
                .to_string(),
                dry_run: true,
                ..Default::default()
            },
            Some(&management),
        ))
        .await?
        .into_inner();
    let rules: BTreeSet<_> = reply.violations.iter().map(|v| v.rule.clone()).collect();
    assert!(
        rules.contains("maxJobs") && rules.contains("runtimes"),
        "{rules:?}"
    );
    assert!(reply
        .violations
        .iter()
        .any(|v| v.job_id == "payments-prod/web" && v.rule == "runtimes"));
    Ok(())
}

/// The plan's error surfaces: a not-found names where else the caller can
/// see a job of that name (and only there), a denial names where the token
/// can act, and an ACL rule naming a namespace that does not exist warns.
#[tokio::test]
async fn namespace_errors_say_where_to_look_without_leaking() -> R<()> {
    let (svc, _, management) = setup_enabled()?;
    create_namespaces(&svc, &management, &["payments-prod", "secret-team"]).await?;
    for namespace in ["payments-prod", "secret-team"] {
        svc.run_job(run_request(
            &namespaced_jobspec("api", None),
            Some(namespace),
            &management,
        ))
        .await?;
    }
    let (_, team) = token_with_rules(
        &svc,
        "payments-team",
        r#"[{"resources": ["job"], "actions": ["read", "write"], "namespaces": ["payments-*", "default"]}]"#,
    )?;

    let status = svc
        .get_job_info(req(GetJobInfoRequest { name: "api".into() }, Some(&team)))
        .await
        .expect_err("no api in default");
    assert_eq!(status.code(), Code::NotFound);
    assert!(
        status
            .message()
            .contains("not found in namespace 'default'"),
        "{}",
        status.message()
    );
    assert!(
        status
            .message()
            .contains("exists in namespace 'payments-prod'"),
        "{}",
        status.message()
    );
    assert!(
        !status.message().contains("secret-team"),
        "the hint names only readable namespaces: {}",
        status.message()
    );

    let status = svc
        .stop_job(req(
            StopJobRequest {
                name: "secret-team/api".into(),
                ..Default::default()
            },
            Some(&team),
        ))
        .await
        .expect_err("no grant there");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status
            .message()
            .contains("this token can write job in default, payments-*"),
        "{}",
        status.message()
    );

    let reply = svc
        .upsert_acl_policy(req(
            UpsertAclPolicyRequest {
                name: "typo".into(),
                description: String::new(),
                rules_json: r#"[{"resources": ["job"], "actions": ["read"], "namespaces": ["payments-prdo", "payments-*"]}]"#.into(),
            },
            Some(&management),
        ))
        .await?
        .into_inner();
    assert_eq!(
        reply.warnings,
        vec!["rule 0 names namespace 'payments-prdo', which does not exist".to_string()]
    );
    Ok(())
}
