//! What each Manager RPC needs, as data: the source for the caller-aware
//! capability catalog (`GetCallerCapabilities`) and the MCP coverage test.
//!
//! The ACL check itself lives in each handler; this table restates it so a
//! client can ask "may I?" before calling. `tests/integration/acl_endpoints.rs`
//! (`rpc_access_table_matches_the_endpoint_table`) fails when a row here
//! disagrees with the endpoint table the deny/allow matrix tests, and the
//! unit test below fails when a Manager RPC has no row. Keep both in step
//! when adding an RPC.

/// How an RPC is authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Needs `resource:action` (in some scope).
    Protected(&'static str, &'static str),
    /// Needs no permission (credential exchange, introspection).
    Open,
    /// Any caller presenting a Legion ACL token (CreateChildToken).
    SelfService,
    /// Worker plane: node and join tokens.
    Worker,
    /// App plane: the bundled autoscaler's identity.
    App,
}

impl Access {
    /// `resource:action`, or empty when the RPC needs no permission.
    pub fn permission(&self) -> String {
        match self {
            Access::Protected(resource, action) => format!("{resource}:{action}"),
            _ => String::new(),
        }
    }

    /// Whether the RPC needs a write permission.
    pub fn mutates(&self) -> bool {
        matches!(self, Access::Protected(_, "write") | Access::SelfService)
    }

    /// Whether a user (or agent) calls it, as opposed to a worker or an app.
    pub fn user_facing(&self) -> bool {
        !matches!(self, Access::Worker | Access::App)
    }
}

/// Every Manager RPC, in endpoint-table order.
pub const RPC_ACCESS: &[(&str, Access)] = &[
    ("WorkerPreRegister", Access::Worker),
    ("WorkerRegister", Access::Worker),
    ("Heartbeat", Access::Worker),
    ("Ping", Access::Worker),
    ("StreamWork", Access::Worker),
    ("HealthCheck", Access::Worker),
    ("RunJob", Access::Protected("job", "write")),
    ("GetJobSpec", Access::Protected("job", "read")),
    ("GetJobSpecByConfig", Access::Protected("job", "read")),
    ("ListJobSpecHistory", Access::Protected("job", "read")),
    ("GetJobInfo", Access::Protected("job", "read")),
    ("ListRollouts", Access::Protected("job", "read")),
    ("GetRollout", Access::Protected("job", "read")),
    ("GetRolloutStatus", Access::Protected("job", "read")),
    ("ListRolloutEvents", Access::Protected("job", "read")),
    ("AdvanceRollout", Access::Protected("job", "write")),
    ("StopRollout", Access::Protected("job", "write")),
    ("PlanJob", Access::Protected("job", "read")),
    ("StopJob", Access::Protected("job", "write")),
    ("ScaleTask", Access::Protected("scale", "write")),
    ("GetJobScaleStatus", Access::Protected("scale", "read")),
    ("GetScalingInputs", Access::Protected("scale", "read")),
    ("WatchScalingPolicies", Access::Protected("scale", "read")),
    (
        "ApplyScalingDefaults",
        Access::Protected("operator", "write"),
    ),
    ("GetScalingDefaults", Access::Protected("scale", "read")),
    ("ReportScalingStatus", Access::App),
    ("RecordScalingAction", Access::App),
    ("GetScalingStatus", Access::Protected("scale", "read")),
    ("GetScaleHistory", Access::Protected("scale", "read")),
    ("PauseScaling", Access::Protected("scale", "write")),
    ("GetCapacityDemand", Access::Protected("node", "read")),
    ("DestroyJob", Access::Protected("job", "write")),
    ("ListJobs", Access::Protected("job", "read")),
    ("ListNamespaces", Access::Protected("namespace", "read")),
    ("GetNamespace", Access::Protected("namespace", "read")),
    ("ApplyNamespace", Access::Protected("namespace", "write")),
    ("DeleteNamespace", Access::Protected("namespace", "write")),
    ("ListNamespacePolicies", Access::Protected("acl", "read")),
    ("GetNamespacePolicy", Access::Protected("acl", "read")),
    ("ApplyNamespacePolicy", Access::Protected("acl", "write")),
    ("DeleteNamespacePolicy", Access::Protected("acl", "write")),
    ("SetRuntimeEnabled", Access::Protected("operator", "write")),
    ("ListRuntimeStates", Access::Protected("operator", "read")),
    ("UpsertJobTemplate", Access::Protected("template", "write")),
    ("ListJobTemplates", Access::Protected("template", "read")),
    ("GetJobTemplate", Access::Protected("template", "read")),
    ("RenderJobTemplate", Access::Protected("job", "write")),
    ("RunJobTemplate", Access::Protected("job", "write")),
    ("DeleteJobTemplate", Access::Protected("template", "write")),
    ("ListServices", Access::Protected("job", "read")),
    ("ListServiceEdges", Access::Protected("job", "read")),
    ("ResolveService", Access::Protected("job", "read")),
    ("UpsertNetPolicy", Access::Protected("net", "write")),
    ("GetNetPolicyObject", Access::Protected("net", "read")),
    ("ListNetPolicies", Access::Protected("net", "read")),
    ("DeleteNetPolicy", Access::Protected("net", "write")),
    ("UpsertNetPlugin", Access::Protected("net", "write")),
    ("GetNetPlugin", Access::Protected("net", "read")),
    ("ListNetPlugins", Access::Protected("net", "read")),
    ("DeleteNetPlugin", Access::Protected("net", "write")),
    ("GetNetPluginStatus", Access::Protected("net", "read")),
    ("ShowNetPolicy", Access::Protected("net", "read")),
    ("TestNetPolicy", Access::Protected("net", "read")),
    ("GetMeshCa", Access::Protected("net", "read")),
    ("RotateMeshCa", Access::Protected("net", "write")),
    ("FetchDatapathMaterial", Access::Worker),
    ("GetNodes", Access::Protected("node", "read")),
    ("GetHeartbeatCadence", Access::Protected("node", "read")),
    ("GetNodeUsage", Access::Protected("node", "read")),
    ("ListNodeUsage", Access::Protected("node", "read")),
    ("GetNodeContent", Access::Protected("node", "read")),
    ("GetNode", Access::Protected("node", "read")),
    ("RestartWorker", Access::Protected("node", "write")),
    ("SetNodeAvailability", Access::Protected("node", "write")),
    ("ForgetNode", Access::Protected("node", "write")),
    ("GetLedger", Access::Protected("node", "read")),
    ("UpdateNodeDynamic", Access::Protected("node", "write")),
    ("GetManagerConfigs", Access::Protected("operator", "read")),
    ("ListManagers", Access::Protected("operator", "read")),
    ("GetObservability", Access::Protected("operator", "read")),
    (
        "GetSchedulerConcurrency",
        Access::Protected("operator", "read"),
    ),
    ("GetRaftStatus", Access::Protected("operator", "read")),
    ("ListRaftHistory", Access::Protected("operator", "read")),
    ("IssueRaftCommand", Access::Protected("operator", "write")),
    ("GetRaftJoinToken", Access::Protected("operator", "write")),
    ("GetStateBackup", Access::Protected("operator", "write")),
    ("GetKeyringStatus", Access::Protected("operator", "read")),
    ("RotateDataKey", Access::Protected("operator", "write")),
    ("RewrapKv", Access::Protected("operator", "write")),
    ("RewrapRootKey", Access::Protected("operator", "write")),
    ("ResetKeyring", Access::Protected("operator", "write")),
    ("PutKv", Access::Protected("kv", "write")),
    ("GetKv", Access::Protected("kv", "read")),
    ("ListKv", Access::Protected("kv", "read")),
    ("DeleteKv", Access::Protected("kv", "write")),
    ("PatchKv", Access::Protected("kv", "write")),
    ("AcquireKvLock", Access::Protected("kv", "write")),
    ("RenewKvLock", Access::Protected("kv", "write")),
    ("ReleaseKvLock", Access::Protected("kv", "write")),
    ("GetKvLock", Access::Protected("kv", "read")),
    ("QueryStateStore", Access::Protected("state", "read")),
    ("GetStateStoreSchema", Access::Protected("state", "read")),
    ("ListSavedStateQueries", Access::Protected("state", "read")),
    ("UpsertSavedStateQuery", Access::Protected("state", "write")),
    ("DeleteSavedStateQuery", Access::Protected("state", "write")),
    ("CreateInstance", Access::Protected("job", "write")),
    ("CreateInstances", Access::Protected("job", "write")),
    ("DestroyInstance", Access::Protected("job", "write")),
    ("StopInstance", Access::Protected("job", "write")),
    ("StartInstance", Access::Protected("job", "write")),
    ("SuspendInstance", Access::Protected("job", "write")),
    ("ForkInstance", Access::Protected("job", "write")),
    ("ReplaceInstance", Access::Protected("job", "write")),
    ("ListInstances", Access::Protected("job", "read")),
    ("GetInstance", Access::Protected("job", "read")),
    ("PromoteInstance", Access::Protected("job", "write")),
    ("ResizeAllocation", Access::Protected("scale", "write")),
    ("ResizeInstance", Access::Protected("scale", "write")),
    ("ResizeTask", Access::Protected("scale", "write")),
    ("ListSessionNodes", Access::Protected("job", "read")),
    ("RunSession", Access::Protected("job", "write")),
    ("GetIsolateStats", Access::Protected("job", "read")),
    ("GetIsolate", Access::Protected("job", "read")),
    ("CreateSnapshot", Access::Protected("snapshot", "write")),
    ("ListSnapshots", Access::Protected("snapshot", "read")),
    ("GetSnapshot", Access::Protected("snapshot", "read")),
    ("DeleteSnapshot", Access::Protected("snapshot", "write")),
    ("RestoreInstance", Access::Protected("snapshot", "read")),
    ("SetSnapshotAlias", Access::Protected("snapshot", "write")),
    (
        "DeleteSnapshotAlias",
        Access::Protected("snapshot", "write"),
    ),
    ("WarmSnapshot", Access::Protected("snapshot", "read")),
    ("ListVolumes", Access::Protected("job", "read")),
    ("GetVolume", Access::Protected("job", "read")),
    ("DeleteVolume", Access::Protected("job", "write")),
    ("ListVolumeDrivers", Access::Protected("node", "read")),
    ("IssueWorkloadIdentityToken", Access::Worker),
    ("CreateChildInstance", Access::Worker),
    ("IssueNodeIdentityToken", Access::Worker),
    ("IssueNodeCertificate", Access::Worker),
    ("GetAllocationLogs", Access::Protected("job", "read")),
    ("ExecAllocation", Access::Protected("exec", "write")),
    ("MintExecTicket", Access::Protected("exec", "write")),
    ("CreateExecKey", Access::Protected("acl", "write")),
    ("ListExecKeys", Access::Protected("acl", "read")),
    ("DeleteExecKey", Access::Protected("acl", "write")),
    ("ListJobActions", Access::Protected("job", "read")),
    ("GetArtifactCredential", Access::Worker),
    ("ListEvents", Access::Protected("operator", "read")),
    ("StreamEvents", Access::Protected("operator", "read")),
    ("WhoAmI", Access::Open),
    ("Login", Access::Open),
    ("GetAuthMethods", Access::Open),
    ("CreateJoinToken", Access::Protected("node", "write")),
    ("ListJoinTokens", Access::Protected("node", "read")),
    ("DeleteJoinToken", Access::Protected("node", "write")),
    ("IssueBootTicket", Access::Protected("boot", "write")),
    ("ListBootTickets", Access::Protected("boot", "read")),
    ("RatifyClaim", Access::Worker),
    ("UpsertAclPolicy", Access::Protected("acl", "write")),
    ("GetAclPolicy", Access::Protected("acl", "read")),
    ("ListAclPolicies", Access::Protected("acl", "read")),
    ("DeleteAclPolicy", Access::Protected("acl", "write")),
    ("CreateAclToken", Access::Protected("acl", "write")),
    ("ListAclTokens", Access::Protected("acl", "read")),
    ("DeleteAclToken", Access::Protected("acl", "write")),
    ("UpsertAclUser", Access::Protected("acl", "write")),
    ("ListAclUsers", Access::Protected("acl", "read")),
    ("DeleteAclUser", Access::Protected("acl", "write")),
    ("ListGrants", Access::Protected("acl", "read")),
    ("RevokeGrant", Access::Protected("acl", "write")),
    ("ListApps", Access::Protected("app", "read")),
    ("GetApp", Access::Protected("app", "read")),
    ("InstallApp", Access::Protected("app", "write")),
    ("UpgradeApp", Access::Protected("app", "write")),
    ("UninstallApp", Access::Protected("app", "write")),
    ("SetAppEnabled", Access::Protected("app", "write")),
    ("UpdateAppSettings", Access::Protected("app", "write")),
    ("SetAppConfig", Access::Protected("app", "write")),
    ("ListUiExtensions", Access::Protected("app", "read")),
    ("GetAppView", Access::Protected("app", "read")),
    ("InvokeAppAction", Access::Protected("app", "read")),
    ("GetAppPage", Access::Protected("app", "read")),
    ("GetJobSchema", Access::Protected("job", "read")),
    ("WatchAppExtensions", Access::App),
    ("ListObjectKinds", Access::Protected("object", "read")),
    ("ApplyObject", Access::Protected("object", "write")),
    ("GetObject", Access::Protected("object", "read")),
    ("ListObjects", Access::Protected("object", "read")),
    ("DeleteObject", Access::Protected("object", "write")),
    ("WatchObjects", Access::App),
    ("UpdateObjectStatus", Access::App),
    ("ReleaseObject", Access::App),
    // Agent governance (agent-governance plan).
    ("ListAuditEvents", Access::Protected("audit", "read")),
    ("CreateChildToken", Access::SelfService),
    ("ListApprovals", Access::Protected("approval", "read")),
    ("GetApproval", Access::Protected("approval", "read")),
    ("DecideApproval", Access::Protected("approval", "write")),
    ("GetCallerCapabilities", Access::Open),
    ("GetUsage", Access::Protected("namespace", "read")),
    // Revisions and watches (revisions-and-watches plan): gated as `job:read`
    // before the stream opens (a KV watch checks `kv:read` instead) and
    // filtered per change after.
    ("Watch", Access::Protected("job", "read")),
];

/// The access row for `rpc`, if this manager classifies it.
pub fn access_of(rpc: &str) -> Option<Access> {
    RPC_ACCESS
        .iter()
        .find(|(name, _)| *name == rpc)
        .map(|(_, access)| *access)
}

/// The Manager RPC names in `proto/legion.proto`, in proto order.
pub fn proto_manager_rpcs() -> Vec<&'static str> {
    let proto = crate::structs::PROTO_SOURCE;
    let service = proto
        .split("service Manager {")
        .nth(1)
        .and_then(|rest| rest.split("\n}").next())
        .unwrap_or_default();
    service
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("rpc "))
        .filter_map(|rest| rest.split([' ', '(']).next())
        .collect()
}

#[cfg(test)]
mod rpc_access_test {
    use super::*;

    /// Every Manager RPC in the proto has exactly one row.
    #[test]
    fn every_manager_rpc_has_one_access_row_test() {
        let rpcs: std::collections::BTreeSet<&str> = proto_manager_rpcs().into_iter().collect();
        let rows: std::collections::BTreeSet<&str> = RPC_ACCESS.iter().map(|(n, _)| *n).collect();
        assert_eq!(rows.len(), RPC_ACCESS.len(), "duplicate rows in RPC_ACCESS");
        let missing: Vec<_> = rpcs.difference(&rows).collect();
        let stale: Vec<_> = rows.difference(&rpcs).collect();
        assert!(
            missing.is_empty(),
            "RPCs without an RPC_ACCESS row: {missing:?}"
        );
        assert!(stale.is_empty(), "RPC_ACCESS rows with no RPC: {stale:?}");
        for (rpc, access) in RPC_ACCESS {
            if let Access::Protected(resource, action) = access {
                assert!(
                    crate::structs::acl::AclResource::parse(resource).is_some(),
                    "{rpc}: unknown resource {resource}"
                );
                assert!(
                    crate::structs::acl::AclAction::parse(action).is_some(),
                    "{rpc}: unknown action {action}"
                );
            }
        }
    }
}
