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
//!    ACLs enabled (protected endpoints must return PermissionDenied), with a
//!    management token (must never fail with an auth error), and with ACLs
//!    disabled (must never fail with an auth error).
//! 3. Scenario tests prove scoped policies behave correctly end-to-end
//!    (job-name scoping, KV path scoping, node-pool filtering, audit mode).
//!
//! When you add an RPC: add it to `endpoint_table` with a request builder
//! that references the seeded fixtures, and a one-line ACL check in the
//! handler. See AGENTS.md ("Auth & ACL checks").

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
const DELETABLE_USER: &str = "deletable-user";

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
        ("DestroyJob", Plane::Protected("job:write")),
        ("ListJobs", Plane::Protected("job:read")),
        ("UpsertJobTemplate", Plane::Protected("template:write")),
        ("ListJobTemplates", Plane::Protected("template:read")),
        ("GetJobTemplate", Plane::Protected("template:read")),
        ("RenderJobTemplate", Plane::Protected("job:write")),
        ("RunJobTemplate", Plane::Protected("job:write")),
        ("DeleteJobTemplate", Plane::Protected("template:write")),
        ("ListServices", Plane::Protected("job:read")),
        ("ResolveService", Plane::Protected("job:read")),
        ("GetNodes", Plane::Protected("node:read")),
        ("GetHeartbeatCadence", Plane::Protected("node:read")),
        ("GetNode", Plane::Protected("node:read")),
        ("RestartWorker", Plane::Protected("node:write")),
        ("SetNodeAvailability", Plane::Protected("node:write")),
        ("GetLedger", Plane::Protected("node:read")),
        ("UpdateNodeDynamic", Plane::Protected("node:write")),
        ("GetManagerConfigs", Plane::Protected("operator:read")),
        ("GetObservability", Plane::Protected("operator:read")),
        ("GetRaftStatus", Plane::Protected("operator:read")),
        ("ListRaftHistory", Plane::Protected("operator:read")),
        ("IssueRaftCommand", Plane::Protected("operator:write")),
        ("GetRaftJoinToken", Plane::Protected("operator:write")),
        ("GetStateBackup", Plane::Protected("operator:write")),
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
        ("ReplaceInstance", Plane::Protected("job:write")),
        ("ListInstances", Plane::Protected("job:read")),
        ("GetInstance", Plane::Protected("job:read")),
        // Volumes are job-scoped storage, so they read/write under `job`.
        // ListVolumePlugins is cluster capability data, like node fingerprints.
        ("ListVolumes", Plane::Protected("job:read")),
        ("GetVolume", Plane::Protected("job:read")),
        ("DeleteVolume", Plane::Protected("job:write")),
        ("ListVolumeDrivers", Plane::Protected("node:read")),
        ("IssueWorkloadIdentityToken", Plane::Worker),
        ("IssueNodeIdentityToken", Plane::Worker),
        ("GetAllocationLogs", Plane::Protected("job:read")),
        ("ExecAllocation", Plane::Protected("exec:write")),
        ("ListJobActions", Plane::Protected("job:read")),
        ("GetArtifactCredential", Plane::Worker),
        ("ListEvents", Plane::Protected("operator:read")),
        ("StreamEvents", Plane::Protected("operator:read")),
        ("WhoAmI", Plane::Open),
        ("Login", Plane::Open),
        ("BootstrapAcl", Plane::Open),
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
    ]
}

/// An empty client-streaming request body, for exercising the ACL gate of
/// `ExecAllocation` (which authorizes before reading any frame).
fn empty_exec_stream() -> tonic::Streaming<ExecClientFrame> {
    use tonic::codec::{Codec, ProstCodec};
    let mut codec: ProstCodec<ExecClientFrame, ExecClientFrame> = ProstCodec::default();
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
      runtime = "exec",
      count = 1,
      config = {{ command = ["echo", "ok"] }},
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
                RenderJobTemplateRequest {
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
                RunJobTemplateRequest {
                    name: TEMPLATE_NAME.into(),
                    version: None,
                    variables_json: "{}".into(),
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
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
                PlanJobRequest {
                    parsed_jobspec: jobspec(JOB_NAME),
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
        "ResolveService" => svc
            .resolve_service(req(ResolveServiceRequest::default(), token))
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
            .get_kv(req(GetKvRequest { key: KV_KEY.into() }, token))
            .await
            .map(|_| ()),
        "ListKv" => svc
            .list_kv(req(
                ListKvRequest {
                    prefix: String::new(),
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
        "BootstrapAcl" => svc
            .bootstrap_acl(req(BootstrapAclRequest { secret: None }, token))
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
                    rules_json: r#"[{"resources": ["job"], "actions": ["read"]}]"#.into(),
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
        other => {
            panic!("endpoint_table contains `{other}` but call_endpoint has no builder for it")
        }
    }
}

fn setup_store() -> R<Store> {
    let store_dir = tempfile::Builder::new()
        .prefix("legion-acl-endpoints-")
        .tempdir()
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

    store.upsert_job_template_version(
        TEMPLATE_NAME,
        Some("acl fixture template"),
        &template_source(TEMPLATE_JOB_NAME),
        &[],
        "fixture",
    )?;

    store.put_kv(KV_KEY, "v1", KvValueType::String, None, None)?;
    store.put_kv("team/deletable", "v1", KvValueType::String, None, None)?;
    store.put_kv("secret/hidden", "v1", KvValueType::String, None, None)?;

    store.upsert_acl_policy(
        FIXTURE_POLICY,
        "fixture",
        r#"[{"resources": ["job"], "actions": ["read"]}]"#,
    )?;
    store.upsert_acl_policy(
        DELETABLE_POLICY,
        "fixture",
        r#"[{"resources": ["job"], "actions": ["read"]}]"#,
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
        .bootstrap(&store, None)
        .map_err(|s| anyhow!(s.to_string()))?;
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
         in tests/acl_endpoints.rs and an ACL check in the handler): {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "endpoint_table entries with no matching proto RPC: {stale:?}"
    );
    Ok(())
}

/// With ACLs enabled, anonymous callers are denied on every protected
/// endpoint, rejected as unauthenticated on the worker plane, and never
/// blocked on open endpoints.
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
                    Code::PermissionDenied,
                    "{method} (requires {capability}) returned `{}` instead of PermissionDenied: {}",
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

    // Pre-register fails without the secret, succeeds with it. (The fixture
    // store has no worker types, so success here is "passed the join gate"
    // and failed later with NotFound rather than Unauthenticated.)
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

    // Re-registering rotates the node token; the old one stops working.
    let re_registered = svc
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
            r#"[{{"resources": ["job"], "actions": ["read", "write"], "names": ["{JOB_NAME}"]}}]"#
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
                {{"resources": ["job"], "actions": ["read", "write"],
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
                name: "acl-hijack".into(),
                version: None,
                variables_json: "{}".into(),
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            },
            Some(&secret),
        ))
        .await
        .expect_err("collision guard must block hijacking unrelated jobs");
    assert_eq!(status.code(), Code::FailedPrecondition);
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
        r#"[{"resources": ["job"], "actions": ["read", "write"],
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
                "runtime": "exec",
                "count": 1,
                "config": { "command": ["echo", "ok"] },
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
                    parsed_jobspec: spec,
                    submissions: vec![],
                    priority: None,
                    force_reschedule: false,
                    idempotency_key: None,
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
        r#"[{"resources": ["job"], "actions": ["read", "write"],
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
                name: "band-escalation".into(),
                version: None,
                variables_json: "{}".into(),
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
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
        r#"[{"resources": ["job"], "actions": ["read", "write"]}]"#,
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
        r#"[{"resources": ["job"], "actions": ["read"]}]"#,
    )?;

    call_endpoint(&svc, &fixtures, "ListJobs", None)
        .await
        .expect("anonymous policy must allow job reads");
    let status = call_endpoint(&svc, &fixtures, "StopJob", None)
        .await
        .expect_err("anonymous policy must not allow job writes");
    assert_eq!(status.code(), Code::PermissionDenied);
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
