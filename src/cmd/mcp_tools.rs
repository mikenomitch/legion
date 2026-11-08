//! MCP tools that map one-to-one onto a Manager RPC (agent-governance plan
//! §6.7, review finding AI4): KV, ACL, rollouts, apps, raft, join tokens,
//! approvals, the audit journal, usage, and the rest of the user-facing
//! API that had no tool.
//!
//! Each row names the tool, the RPC, the request type, whether it mutates,
//! and the argument schema. Arguments are snake_case (like every Legion
//! tool) and are decoded straight into the request message, so a field the
//! schema names is a field of the proto request (`proto/legion.proto` is the
//! reference for each). Nested objects keep the proto's camelCase JSON.
//!
//! `every_user_facing_rpc_has_a_tool_or_an_exclusion` (below) parses the
//! proto and fails when a user-facing Manager RPC has neither a tool here or
//! in `mcp.rs`, nor an entry in [`EXCLUDED`] with the reason.

use super::{connect, grpc, ToolDef};
use crate::prelude::*;
use crate::structs::*;

/// User-facing Manager RPCs deliberately without an MCP tool, and why.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const EXCLUDED: &[(&str, &str)] = &[
    (
        "Login",
        "the MCP server already runs with a credential; a login would mint a token the server \
         could not use (legion_acl_token_create_child hands a sub-agent a credential instead)",
    ),
    (
        "WatchScalingPolicies",
        "a long-lived watch stream the autoscaler app consumes; legion_scaling_status and \
         legion_scaling_inputs answer the same questions as bounded reads",
    ),
    (
        "GetStateBackup",
        "streams the whole state database as bytes; `legion raft backup` writes it to a file",
    ),
    (
        "RewrapRootKey",
        "reads the new root key from the manager's own disk; an operator action at the manager",
    ),
    (
        "ResetKeyring",
        "irreversibly discards every encrypted KV value; deliberately CLI-only",
    ),
    (
        "GetAppPage",
        "serves an app page's HTML bundle to the UI; nothing an agent reads",
    ),
];

/// RPCs a tool in `mcp.rs` reaches through a helper in another module, so
/// the source scan in the coverage test does not see the call.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const INDIRECT: &[(&str, &str)] = &[
    ("GetJobSchema", "legion_job_schema, legion_job_validate"),
    ("GetCallerCapabilities", "legion_capabilities"),
];

/// A JSON Schema object for `(name, type, description)` properties.
fn props(fields: &[(&str, &str, &str)], required: &[&str]) -> Value {
    let mut properties = Map::new();
    for (name, kind, description) in fields {
        let schema = match *kind {
            "string[]" => {
                json!({ "type": "array", "items": { "type": "string" }, "description": description })
            }
            "object" => json!({ "type": "object", "description": description }),
            "object[]" => {
                json!({ "type": "array", "items": { "type": "object" }, "description": description })
            }
            other => json!({ "type": other, "description": description }),
        };
        properties.insert(name.to_string(), schema);
    }
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

/// snake_case top-level argument keys to the proto JSON's camelCase.
fn camel_args(args: Value) -> Value {
    let Value::Object(fields) = args else {
        return json!({});
    };
    let mut out = Map::new();
    for (key, value) in fields {
        let mut camel = String::with_capacity(key.len());
        let mut upper = false;
        for ch in key.chars() {
            if ch == '_' {
                upper = true;
            } else if upper {
                camel.extend(ch.to_uppercase());
                upper = false;
            } else {
                camel.push(ch);
            }
        }
        out.insert(camel, value);
    }
    Value::Object(out)
}

fn request<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, String> {
    serde_json::from_value(camel_args(args)).map_err(|e| format!("invalid arguments: {e}"))
}

macro_rules! rpc_tools {
    ($( $tool:literal => $rpc:literal, $method:ident($req:ty), $mutates:literal, $desc:literal, $schema:expr; )*) => {
        /// The tools this module adds, in registry order.
        pub(super) fn tool_defs() -> Vec<ToolDef> {
            vec![ $( ToolDef { name: $tool, description: $desc, input_schema: || $schema, mutates: $mutates }, )* ]
        }

        /// `(tool, RPC)` for every tool here.
        #[cfg_attr(not(test), allow(dead_code))]
        pub(super) const TOOL_RPCS: &[(&str, &str)] = &[ $( ($tool, $rpc), )* ];

        /// Runs a tool from this module; `None` when the name is not one.
        pub(super) async fn dispatch(name: &str, mut args: Value) -> Option<Result<Value, String>> {
            // A KV write without a type stores a string (the API rejects
            // an unspecified type).
            if name == "legion_kv_put" {
                if let Some(fields) = args.as_object_mut() {
                    fields.entry("value_type").or_insert(json!(1));
                }
            }
            match name {
                $( $tool => Some(async {
                    let req: $req = request(args)?;
                    let mut client = connect().await?;
                    grpc(client.$method(req)).await
                }.await), )*
                "legion_events_stream" => Some(events_stream(args).await),
                _ => None,
            }
        }
    };
}

rpc_tools! {
    // --- Jobs and rollouts ---
    "legion_job_spec_by_config" => "GetJobSpecByConfig", get_job_spec_by_config(GetJobSpecByConfigRequest), false,
        "Fetch the submissions of one job config version by its config id.",
        props(&[("config_id", "string", "Job config id (from job info or spec history).")], &["config_id"]);
    "legion_job_spec_history" => "ListJobSpecHistory", list_job_spec_history(ListJobSpecHistoryRequest), false,
        "List a job's config versions, newest first: what changed when, for a diff or a rollback.",
        props(&[("name", "string", "Job name (ns/name or a bare name in default).")], &["name"]);
    "legion_rollout_get" => "GetRollout", get_rollout(GetRolloutRequest), false,
        "Show one rollout: its strategy, groups, and progress.",
        props(&[("id", "integer", "Rollout id (legion_rollout_list).")], &["id"]);
    "legion_rollout_events" => "ListRolloutEvents", list_rollout_events(ListRolloutEventsRequest), false,
        "List a rollout's events, newest first.",
        props(&[("rollout_id", "integer", "Rollout id."), ("limit", "integer", "Maximum events.")], &["rollout_id"]);
    "legion_rollout_advance" => "AdvanceRollout", advance_rollout(AdvanceRolloutRequest), true,
        "Advance a paused or manual rollout to its next step (optionally only some groups).",
        props(&[("id", "integer", "Rollout id."), ("groups", "string[]", "Only these task groups."), ("priority", "integer", "Eval priority override.")], &["id"]);
    "legion_rollout_stop" => "StopRollout", stop_rollout(StopRolloutRequest), true,
        "Stop a rollout where it is, or revert it to the previous version (revert = true).",
        props(&[("id", "integer", "Rollout id."), ("revert", "boolean", "Roll back to the previous version."), ("priority", "integer", "Eval priority override.")], &["id"]);
    // --- Scaling ---
    "legion_scaling_inputs" => "GetScalingInputs", get_scaling_inputs(GetScalingInputsRequest), false,
        "Read the merged scaling inputs (per-task usage and load signals) the autoscaler decides on.",
        props(&[("namespaces", "string[]", "Namespaces or globs."), ("targets", "object[]", "ScalingTargetSelector objects (camelCase proto JSON)."), ("history_seconds", "integer", "Seconds of history to include.")], &[]);
    "legion_scaling_defaults_get" => "GetScalingDefaults", get_scaling_defaults(GetScalingDefaultsRequest), false,
        "Show the cluster's (or a namespace's) task-autoscaling defaults.",
        props(&[("namespace", "string", "Namespace (empty for the cluster).")], &[]);
    "legion_scaling_defaults_apply" => "ApplyScalingDefaults", apply_scaling_defaults(ApplyScalingDefaultsRequest), true,
        "Apply task-autoscaling defaults (operator:write). dry_run reports the effect.",
        props(&[("defaults_json", "string", "The defaults document as JSON."), ("dry_run", "boolean", "Validate and report only."), ("force", "boolean", "Apply despite warnings."), ("idempotency_key", "string", "Retry-safety key.")], &["defaults_json"]);
    // --- Snapshots ---
    "legion_snapshot_alias_delete" => "DeleteSnapshotAlias", delete_snapshot_alias(DeleteSnapshotAliasRequest), true,
        "Delete a snapshot's name (the snapshot itself stays).",
        props(&[("job_id", "string", "Job the snapshot belongs to."), ("name", "string", "Alias to delete."), ("idempotency_key", "string", "Retry-safety key.")], &["job_id", "name"]);
    // --- Nodes and operator ---
    "legion_node_restart_worker" => "RestartWorker", restart_worker(RestartWorkerRequest), true,
        "Restart a node's worker process (its tasks keep running).",
        props(&[("node_id", "string", "Node id.")], &["node_id"]);
    "legion_node_update_dynamic" => "UpdateNodeDynamic", update_node_dynamic(UpdateNodeDynamicRequest), true,
        "Patch a node's dynamic attributes (JSON merge diff).",
        props(&[("id", "string", "Node id."), ("dynamic_diff", "string", "JSON object of attributes to set (null deletes).")], &["id", "dynamic_diff"]);
    "legion_manager_configs" => "GetManagerConfigs", get_manager_configs(GetManagerConfigsRequest), false,
        "List each manager's effective configuration.",
        props(&[("query", "string", "Filter text."), ("page", "integer", "Page."), ("page_size", "integer", "Page size.")], &[]);
    "legion_observability" => "GetObservability", get_observability(GetObservabilityRequest), false,
        "Read the manager's recent observability log lines.",
        props(&[("max_lines", "integer", "Maximum lines.")], &[]);
    "legion_raft_status" => "GetRaftStatus", get_raft_status(GetRaftStatusRequest), false,
        "Show raft membership, leaders per lane, and replication health.",
        props(&[], &[]);
    "legion_raft_history" => "ListRaftHistory", list_raft_history(ListRaftHistoryRequest), false,
        "List recent raft membership and leadership events.",
        props(&[("limit", "integer", "Maximum entries.")], &[]);
    "legion_raft_command" => "IssueRaftCommand", issue_raft_command(IssueRaftCommandRequest), true,
        "Issue a raft membership command (for example remove a member or trigger an election).",
        props(&[("command", "string", "The command (see `legion raft command --help`)."), ("target_node_id", "string", "The member it applies to.")], &["command"]);
    "legion_raft_join_token" => "GetRaftJoinToken", get_raft_join_token(GetRaftJoinTokenRequest), true,
        "Mint a join token for adding a manager to the raft cluster (returns a secret).",
        props(&[], &[]);
    "legion_keyring_rewrap_kv" => "RewrapKv", rewrap_kv(RewrapKvRequest), true,
        "Re-encrypt every KV value under the current data key.",
        props(&[("plaintext", "boolean", "Also encrypt values still stored in plaintext."), ("idempotency_key", "string", "Retry-safety key.")], &[]);
    // --- KV ---
    "legion_kv_get" => "GetKv", get_kv(GetKvRequest), false,
        "Read one KV entry (value, version, lock).",
        props(&[("key", "string", "Key."), ("metadata_only", "boolean", "Omit the value.")], &["key"]);
    "legion_kv_list" => "ListKv", list_kv(ListKvRequest), false,
        "List KV entries under a prefix.",
        props(&[("prefix", "string", "Key prefix."), ("include_values", "boolean", "Include values."), ("include_locks", "boolean", "Include lock holders.")], &[]);
    "legion_kv_put" => "PutKv", put_kv(PutKvRequest), true,
        "Write a KV entry. expected_version makes it a compare-and-set (0 = create only).",
        props(&[("key", "string", "Key."), ("value", "string", "Value."), ("value_type", "integer", "KvValueType: 1 string (default), 2 number, 3 JSON."), ("expected_version", "integer", "Compare-and-set version."), ("lock_id", "string", "Lock held on the key, if any.")], &["key", "value"]);
    "legion_kv_patch" => "PatchKv", patch_kv(PatchKvRequest), true,
        "Apply a JSON merge patch to a JSON-typed KV entry.",
        props(&[("key", "string", "Key."), ("patch_json", "string", "JSON merge patch."), ("expected_version", "integer", "Compare-and-set version."), ("lock_id", "string", "Lock held on the key, if any.")], &["key", "patch_json"]);
    "legion_kv_delete" => "DeleteKv", delete_kv(DeleteKvRequest), true,
        "Delete a KV entry.",
        props(&[("key", "string", "Key."), ("expected_version", "integer", "Compare-and-set version."), ("lock_id", "string", "Lock held on the key, if any.")], &["key"]);
    "legion_kv_lock_get" => "GetKvLock", get_kv_lock(GetKvLockRequest), false,
        "Show who holds a KV lock.",
        props(&[("key", "string", "Key.")], &["key"]);
    "legion_kv_lock_acquire" => "AcquireKvLock", acquire_kv_lock(AcquireKvLockRequest), true,
        "Acquire (or re-acquire) a KV lock for ttl_seconds.",
        props(&[("key", "string", "Key."), ("lock_id", "string", "Reuse this lock id."), ("owner", "string", "Owner label."), ("ttl_seconds", "integer", "Lease length."), ("force", "boolean", "Break another holder's lock.")], &["key", "ttl_seconds"]);
    "legion_kv_lock_renew" => "RenewKvLock", renew_kv_lock(RenewKvLockRequest), true,
        "Extend a KV lock you hold.",
        props(&[("key", "string", "Key."), ("lock_id", "string", "Lock id."), ("ttl_seconds", "integer", "New lease length.")], &["key", "lock_id", "ttl_seconds"]);
    "legion_kv_lock_release" => "ReleaseKvLock", release_kv_lock(ReleaseKvLockRequest), true,
        "Release a KV lock you hold.",
        props(&[("key", "string", "Key."), ("lock_id", "string", "Lock id.")], &["key", "lock_id"]);
    // --- Networking ---
    "legion_net_policy_get" => "GetNetPolicyObject", get_net_policy_object(GetNetPolicyObjectRequest), false,
        "Show one stored network policy object.",
        props(&[("name", "string", "Policy name.")], &["name"]);
    "legion_net_policy_delete" => "DeleteNetPolicy", delete_net_policy(DeleteNetPolicyRequest), true,
        "Delete a stored network policy.",
        props(&[("name", "string", "Policy name.")], &["name"]);
    "legion_net_plugin_delete" => "DeleteNetPlugin", delete_net_plugin(DeleteNetPluginRequest), true,
        "Delete a registered network plugin.",
        props(&[("name", "string", "Plugin name.")], &["name"]);
    // --- State explorer ---
    "legion_state_saved_query_list" => "ListSavedStateQueries", list_saved_state_queries(ListSavedStateQueriesRequest), false,
        "List saved state-store queries.",
        props(&[], &[]);
    "legion_state_saved_query_upsert" => "UpsertSavedStateQuery", upsert_saved_state_query(UpsertSavedStateQueryRequest), true,
        "Save (or replace) a named state-store query.",
        props(&[("name", "string", "Query name."), ("sql", "string", "Read-only SQL.")], &["name", "sql"]);
    "legion_state_saved_query_delete" => "DeleteSavedStateQuery", delete_saved_state_query(DeleteSavedStateQueryRequest), true,
        "Delete a saved state-store query.",
        props(&[("name", "string", "Query name.")], &["name"]);
    // --- ACL ---
    "legion_acl_policy_list" => "ListAclPolicies", list_acl_policies(ListAclPoliciesRequest), false,
        "List ACL policies (acl:read).",
        props(&[], &[]);
    "legion_acl_policy_get" => "GetAclPolicy", get_acl_policy(GetAclPolicyRequest), false,
        "Show one ACL policy and its rules (acl:read).",
        props(&[("name", "string", "Policy name.")], &["name"]);
    "legion_acl_policy_upsert" => "UpsertAclPolicy", upsert_acl_policy(UpsertAclPolicyRequest), true,
        "Create or replace an ACL policy (acl:write).",
        props(&[("name", "string", "Policy name."), ("description", "string", "Description."), ("rules_json", "string", "JSON array of rules (docs/auth-and-acls.md).")], &["name", "rules_json"]);
    "legion_acl_policy_delete" => "DeleteAclPolicy", delete_acl_policy(DeleteAclPolicyRequest), true,
        "Delete an ACL policy; tokens holding it lose what it granted (acl:write).",
        props(&[("name", "string", "Policy name.")], &["name"]);
    "legion_acl_token_list" => "ListAclTokens", list_acl_tokens(ListAclTokensRequest), false,
        "List ACL tokens with expiry and parent (secrets are never shown; acl:read).",
        props(&[], &[]);
    "legion_acl_token_create" => "CreateAclToken", create_acl_token(CreateAclTokenRequest), true,
        "Create an ACL token (acl:write). Expires after acl.tokenDefaultTtl (30d) unless ttl is set. Returns the secret once.",
        props(&[("name", "string", "Token name."), ("token_type", "string", "client (default) or management."), ("policies", "string[]", "Policies to attach."), ("ttl", "string", "Lifetime, e.g. 8h.")], &["name"]);
    "legion_acl_token_create_child" => "CreateChildToken", create_child_token(CreateChildTokenRequest), true,
        "Mint a child of the server's own token for a sub-agent: a subset of its policies, expiring no later than it (default 1h, max 24h), revoked with it. Needs no acl:write. Returns the secret once.",
        props(&[("name", "string", "Child token name."), ("policies", "string[]", "Subset of your policies (default: all)."), ("ttl", "string", "Lifetime, e.g. 30m."), ("write_rate", "string", "Write rate limit, e.g. 5/s or 5/s,burst=20.")], &["name"]);
    "legion_acl_token_delete" => "DeleteAclToken", delete_acl_token(DeleteAclTokenRequest), true,
        "Revoke an ACL token by accessor id, and every child token minted from it (acl:write).",
        props(&[("accessor_id", "string", "Accessor id.")], &["accessor_id"]);
    "legion_acl_user_list" => "ListAclUsers", list_acl_users(ListAclUsersRequest), false,
        "List username/password users (acl:read).",
        props(&[], &[]);
    "legion_acl_user_upsert" => "UpsertAclUser", upsert_acl_user(UpsertAclUserRequest), true,
        "Create or update a username/password user (acl:write).",
        props(&[("username", "string", "Username."), ("password", "string", "Password."), ("policies", "string[]", "Policies a login gets.")], &["username", "password"]);
    "legion_acl_user_delete" => "DeleteAclUser", delete_acl_user(DeleteAclUserRequest), true,
        "Delete a username/password user (acl:write).",
        props(&[("username", "string", "Username.")], &["username"]);
    "legion_acl_grant_revoke" => "RevokeGrant", revoke_grant(RevokeGrantRequest), true,
        "Revoke the grants a job's legion identities received at submit (acl:write).",
        props(&[("job", "string", "Job."), ("version", "integer", "Only this config version."), ("idempotency_key", "string", "Retry-safety key.")], &["job"]);
    "legion_acl_auth_methods" => "GetAuthMethods", get_auth_methods(GetAuthMethodsRequest), false,
        "List the manager's login methods (userpass, JWT, OIDC).",
        props(&[], &[]);
    // --- Join tokens ---
    "legion_join_token_list" => "ListJoinTokens", list_join_tokens(ListJoinTokensRequest), false,
        "List worker join tokens (secrets are never shown).",
        props(&[], &[]);
    "legion_join_token_create" => "CreateJoinToken", create_join_token(CreateJoinTokenRequest), true,
        "Mint a worker join token for a worker type (returns the secret once).",
        props(&[("worker_type", "string", "Worker type the joining node takes."), ("ttl", "string", "Lifetime."), ("description", "string", "Description."), ("max_uses", "integer", "Uses before it expires."), ("attributes", "object", "Attributes pinned on joining nodes.")], &["worker_type"]);
    "legion_join_token_delete" => "DeleteJoinToken", delete_join_token(DeleteJoinTokenRequest), true,
        "Revoke a worker join token.",
        props(&[("id", "string", "Join token id.")], &["id"]);
    // --- Apps ---
    "legion_app_install" => "InstallApp", install_app(InstallAppRequest), true,
        "Install an app from its manifest. dry_run shows the permission diff without installing; installing needs every permission the app asks for.",
        props(&[("manifest_json", "string", "The app manifest as JSON."), ("source", "string", "Where it came from."), ("source_ref", "string", "Version or ref."), ("config_json", "string", "Initial config."), ("keys", "string[]", "Accepted signing keys."), ("dry_run", "boolean", "Report only."), ("idempotency_key", "string", "Retry-safety key.")], &["manifest_json"]);
    "legion_app_upgrade" => "UpgradeApp", upgrade_app(UpgradeAppRequest), true,
        "Upgrade an installed app. A permission increase needs approve = true.",
        props(&[("name", "string", "App."), ("manifest_json", "string", "New manifest."), ("source_ref", "string", "Version or ref."), ("approve", "boolean", "Accept a permission increase."), ("config_json", "string", "Config."), ("force", "boolean", "Upgrade despite warnings."), ("dry_run", "boolean", "Report only."), ("idempotency_key", "string", "Retry-safety key.")], &["name", "manifest_json"]);
    "legion_app_uninstall" => "UninstallApp", uninstall_app(UninstallAppRequest), true,
        "Uninstall an app (purge also deletes its objects and data).",
        props(&[("name", "string", "App."), ("force", "boolean", "Uninstall despite dependents."), ("purge", "boolean", "Delete its objects and data."), ("dry_run", "boolean", "Report only."), ("idempotency_key", "string", "Retry-safety key.")], &["name"]);
    "legion_app_set_enabled" => "SetAppEnabled", set_app_enabled(SetAppEnabledRequest), true,
        "Enable or disable an installed app.",
        props(&[("name", "string", "App."), ("enabled", "boolean", "Enable (true) or disable (false)."), ("dry_run", "boolean", "Report only."), ("idempotency_key", "string", "Retry-safety key.")], &["name", "enabled"]);
    "legion_app_update_settings" => "UpdateAppSettings", update_app_settings(UpdateAppSettingsRequest), true,
        "Change an app's replicas (a count, or one per node with per_node), placement, resources, or pool.",
        props(&[("name", "string", "App."), ("replicas", "integer", "A fixed number of replicas, 1-32; turns per-node off."), ("per_node", "boolean", "true: one replica on every node the placement and pool allow. Not with replicas; false alone is refused."), ("placement", "string", "Placement."), ("cpu", "integer", "CPU MHz."), ("memory", "integer", "Memory MiB."), ("pool", "string", "Node pool."), ("dry_run", "boolean", "Report only."), ("idempotency_key", "string", "Retry-safety key.")], &["name"]);
    "legion_app_set_config" => "SetAppConfig", set_app_config(SetAppConfigRequest), true,
        "Replace an app's config.",
        props(&[("name", "string", "App."), ("config_json", "string", "Config as JSON."), ("dry_run", "boolean", "Report only."), ("idempotency_key", "string", "Retry-safety key.")], &["name", "config_json"]);
    "legion_app_ui_extensions" => "ListUiExtensions", list_ui_extensions(ListUiExtensionsRequest), false,
        "List the UI extensions (views, actions) apps contribute to a slot.",
        props(&[("slot", "string", "UI slot."), ("app", "string", "Only this app."), ("context", "object", "AppViewContext (camelCase proto JSON).")], &[]);
    "legion_app_invoke_action" => "InvokeAppAction", invoke_app_action(InvokeAppActionRequest), true,
        "Run an app's declared action with typed inputs.",
        props(&[("app", "string", "App."), ("action", "string", "Action name."), ("inputs_json", "string", "Inputs as JSON."), ("context", "object", "AppViewContext (camelCase proto JSON)."), ("idempotency_key", "string", "Retry-safety key.")], &["app", "action"]);
    // --- Agent governance ---
    "legion_audit_list" => "ListAuditEvents", list_audit_events(ListAuditEventsRequest), false,
        "Query the audit journal (audit:read): who did which write or was denied, acting for whom, on what target, with which outcome and approval. Newest first; pass next_page_token as page_token for older records.",
        props(&[("accessor_id", "string", "Exact accessor."), ("principal", "string", "Exact principal."), ("operation", "string", "RPC name, e.g. DestroyJob."), ("target_prefix", "string", "e.g. job:prod/."), ("namespace", "string", "Namespace."), ("outcome", "string", "ok, PermissionDenied, APPROVAL_REQUIRED, ..."), ("kind", "string", "write, denied, audited."), ("since", "string", "RFC3339 or 1h/7d."), ("until", "string", "RFC3339 or duration."), ("limit", "integer", "Max records (100)."), ("page_token", "string", "Paging cursor."), ("after_cursor", "string", "Follow cursor (oldest first).")], &[]);
    "legion_approval_list" => "ListApprovals", list_approvals(ListApprovalsRequest), false,
        "List approval requests (all with approval:read, else your own). Poll this after APPROVAL_REQUIRED.",
        props(&[("status", "string", "pending, approved, denied, consumed, expired."), ("mine", "boolean", "Only your own."), ("limit", "integer", "Max.")], &[]);
    "legion_approval_get" => "GetApproval", get_approval(GetApprovalRequest), false,
        "Show one approval request.",
        props(&[("id", "string", "Approval id (apr_...).")], &["id"]);
    "legion_approval_decide" => "DecideApproval", decide_approval(DecideApprovalRequest), true,
        "Approve or deny someone else's pending approval request (approval:write).",
        props(&[("id", "string", "Approval id."), ("approve", "boolean", "true approves, false denies."), ("reason", "string", "Why."), ("idempotency_key", "string", "Retry-safety key.")], &["id", "approve"]);
    "legion_usage" => "GetUsage", get_usage(GetUsageRequest), false,
        "Time-integrated reserved usage (CPU core-hours, memory GiB-hours, cores, allocation-hours, custom resources) per namespace, job, submitter, or hour.",
        props(&[("namespaces", "string[]", "Namespaces or globs."), ("job", "string", "Only this job."), ("submitter", "string", "Accessor or principal."), ("since", "string", "RFC3339 or 7d (default 24h)."), ("until", "string", "RFC3339 or duration."), ("group_by", "string", "namespace, job, submitter, hour.")], &[]);
}

/// The bounded read of `StreamEvents`: up to `max_events` (default 100, at
/// most 1000) events after `after_id`, waiting at most `timeout_seconds`
/// (default 5, at most 60) for new ones.
async fn events_stream(args: Value) -> Result<Value, String> {
    let max_events = args
        .get("max_events")
        .and_then(Value::as_u64)
        .unwrap_or(100)
        .clamp(1, 1000) as usize;
    let timeout = std::time::Duration::from_secs(
        args.get("timeout_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(5)
            .clamp(1, 60),
    );
    let mut fields = args.clone();
    if let Some(object) = fields.as_object_mut() {
        object.remove("max_events");
        object.remove("timeout_seconds");
    }
    let req: StreamEventsRequest = request(fields)?;
    let mut client = connect().await?;
    let mut stream = client
        .stream_events(req)
        .await
        .map_err(|status| super::status_error(&status))?
        .into_inner();
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while events.len() < max_events {
        match tokio::time::timeout_at(deadline, stream.message()).await {
            Ok(Ok(Some(event))) => events.push(event),
            Ok(Ok(None)) | Err(_) => break,
            Ok(Err(status)) => return Err(super::status_error(&status)),
        }
    }
    let last_id = events.last().map(|event: &SchedulerEvent| event.id);
    Ok(json!({ "events": events, "lastId": last_id }))
}

/// The `legion_events_stream` tool (dispatched beside the table above).
pub(super) fn stream_tool() -> ToolDef {
    ToolDef {
        name: "legion_events_stream",
        description: "Bounded read of the live scheduler event stream: events after after_id, up to max_events, waiting at most timeout_seconds for new ones. Pass lastId back as after_id to continue.",
        input_schema: || {
            props(
                &[
                    ("after_id", "integer", "Resume after this event id."),
                    ("kind", "string", "Only this event kind."),
                    ("job_id", "string", "Only this job."),
                    ("node_id", "string", "Only this node."),
                    ("level", "string", "info, warn, error."),
                    ("namespaces", "string[]", "Namespaces or globs."),
                    ("max_events", "integer", "Stop after this many (100, max 1000)."),
                    ("timeout_seconds", "integer", "Stop after this long (5, max 60)."),
                ],
                &[],
            )
        },
        mutates: false,
    }
}

#[cfg(test)]
mod mcp_tools_test {
    use super::*;

    #[test]
    fn args_decode_into_requests_test() {
        let req: PutKvRequest = request(json!({
            "key": "team/db", "value": "x", "expected_version": 3, "lock_id": "l1"
        }))
        .unwrap();
        assert_eq!(req.key, "team/db");
        assert_eq!(req.expected_version, Some(3));
        assert_eq!(req.lock_id.as_deref(), Some("l1"));
        let req: CreateChildTokenRequest =
            request(json!({ "name": "sub", "policies": ["ci"], "write_rate": "1/s" })).unwrap();
        assert_eq!(req.policies, vec!["ci".to_string()]);
        assert_eq!(req.write_rate.as_deref(), Some("1/s"));
    }

    /// The headline finding: every user-facing Manager RPC has a tool, or
    /// an exclusion that says why.
    #[test]
    fn every_user_facing_rpc_has_a_tool_or_an_exclusion() {
        use crate::manager::rpc_access::{access_of, proto_manager_rpcs};
        let sources = [include_str!("mcp.rs"), include_str!("mcp_tools.rs")].concat();
        let call = regex::Regex::new(r"\.\s*([a-z][a-z0-9_]*)\s*\(").unwrap();
        let called: std::collections::HashSet<String> = call
            .captures_iter(&sources)
            .map(|capture| capture[1].to_string())
            .collect();
        let snake = |rpc: &str| {
            let mut out = String::new();
            for (i, ch) in rpc.chars().enumerate() {
                if ch.is_uppercase() && i > 0 {
                    out.push('_');
                }
                out.extend(ch.to_lowercase());
            }
            out
        };
        let mut missing = Vec::new();
        for rpc in proto_manager_rpcs() {
            if access_of(rpc).is_some_and(|access| !access.user_facing()) {
                continue;
            }
            let covered = called.contains(&snake(rpc))
                || TOOL_RPCS.iter().any(|(_, r)| *r == rpc)
                || INDIRECT.iter().any(|(r, _)| *r == rpc)
                || rpc == "StreamEvents" && called.contains("stream_events");
            let excluded = EXCLUDED.iter().any(|(r, _)| *r == rpc);
            if !covered && !excluded {
                missing.push(rpc);
            }
        }
        assert!(
            missing.is_empty(),
            "user-facing Manager RPCs with no MCP tool (add one in src/cmd/mcp.rs or \
             src/cmd/mcp_tools.rs, or an EXCLUDED entry with the reason): {missing:?}"
        );
        for (rpc, reason) in EXCLUDED {
            assert!(!reason.is_empty());
            assert!(
                proto_manager_rpcs().contains(rpc),
                "EXCLUDED names {rpc}, which is not a Manager RPC"
            );
        }
    }

    #[test]
    fn mutating_tools_are_flagged_test() {
        for tool in tool_defs() {
            let rpc = TOOL_RPCS
                .iter()
                .find(|(name, _)| *name == tool.name)
                .map(|(_, rpc)| *rpc)
                .unwrap();
            // Gated on `app:read` plus the action's own declared permission,
            // which may be a write: an action can change state.
            if rpc == "InvokeAppAction" {
                assert!(tool.mutates);
                continue;
            }
            if let Some(access) = crate::manager::rpc_access::access_of(rpc) {
                assert_eq!(
                    tool.mutates,
                    access.mutates(),
                    "{} ({rpc}) mutates={} but the RPC's access says {:?}",
                    tool.name,
                    tool.mutates,
                    access
                );
            }
        }
    }
}
