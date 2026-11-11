//! Network plugin objects (mesh interfaces plan §6.B.1;
//! docs/network-plugins.md): UpsertNetPlugin, GetNetPlugin, ListNetPlugins,
//! DeleteNetPlugin, GetNetPluginStatus, and the submit-time checks that every
//! `{ plugin = { name, config } }` hop in a policy object or a job names a
//! plugin it may use, at a stage its path runs, with a config the plugin's
//! schema accepts.

use super::*;
use crate::structs::netplugin::{check_plugin_reference, hop_plugin, NetPluginObject, PluginUse};
use crate::structs::netpolicy::{ChainConfig, NetPolicyObject};
use crate::structs::{
    DeleteNetPluginReply, DeleteNetPluginRequest, GetNetPluginReply, GetNetPluginRequest,
    GetNetPluginStatusReply, GetNetPluginStatusRequest, ListNetPluginsReply, ListNetPluginsRequest,
    NetPlugin, NetPluginNodeStatus, UpsertNetPluginReply, UpsertNetPluginRequest,
};

impl ManagerDaemonService {
    pub(super) async fn upsert_net_plugin_rpc(
        &self,
        request: Request<UpsertNetPluginRequest>,
    ) -> Result<Response<UpsertNetPluginReply>, Status> {
        // Coarse gate before the body is parsed, precise pool scope after.
        let identity = self.check_acl(
            &request,
            AclResource::Net,
            AclAction::Write,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        let mut object: NetPluginObject = serde_json::from_str(&req.object_json)
            .map_err(|e| Status::invalid_argument(format!("invalid net plugin object: {e}")))?;
        object
            .normalize_and_validate()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        enforce_net_plugin_scope(&identity, AclAction::Write, &object)?;
        // A credential hands a KV value to the datapath to sign with, so
        // naming one needs the same right as reading it.
        for (name, source) in &object.credentials {
            if let Some(path) = source.kv_path() {
                acl::enforce(
                    &identity,
                    AclResource::Kv,
                    AclAction::Read,
                    &AclScope::path(path),
                )
                .map_err(|status| {
                    Status::new(
                        status.code(),
                        format!(
                            "{} (plugin {} credentials.{name} names kv {path}; applying it needs \
                             kv read on it)",
                            status.message(),
                            object.name
                        ),
                    )
                })?;
            }
        }
        // Every stored policy that already uses this plugin must still be
        // able to: a new schema or a dropped stage is refused here, not
        // discovered on the datapath.
        let referencing = self.policies_referencing_plugin(&object.name)?;
        for policy in &referencing {
            check_policy_against_plugin(policy, &object)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
        }
        // And so must every running job that uses it: a job keeps its config
        // until it is resubmitted, so a plugin change it could not pass at
        // submit is refused here rather than handed to the module.
        self.check_jobs_against_plugin(&identity, &object)?;
        if req.dry_run {
            let mut plugin = net_plugin_to_proto(&object, 0, "", "");
            plugin.referenced_by = referencing.iter().map(|p| p.name.clone()).collect();
            return Ok(Response::new(UpsertNetPluginReply {
                plugin: Some(plugin),
                deduplicated: false,
            }));
        }
        // The fingerprint is the normalized object: a retry with the same key
        // and a different plugin is refused rather than answered with the
        // first one.
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "upsert_net_plugin",
            req.idempotency_key.as_deref(),
            &object,
        );
        if self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
            .is_some()
        {
            let record = self
                .store
                .get_net_plugin(&object.name)
                .map_err(|e| map_store_error("Error loading net plugin", e))?
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "net plugin '{}' was written by this idempotency key and has since been \
                         deleted",
                        object.name
                    ))
                })?;
            return Ok(Response::new(UpsertNetPluginReply {
                plugin: Some(record_to_proto(&record)),
                deduplicated: true,
            }));
        }
        let record = self
            .store
            .upsert_net_plugin(object)
            .map_err(|e| map_store_error("Error saving net plugin", e))?;
        let (_, deduplicated) = self.record_idempotent(
            &idempotency_key,
            "upsert_net_plugin",
            record.object.name.clone(),
        )?;
        self.store.events().net_plugin_applied(
            &record.object.name,
            record.revision,
            &record.object.module.checksum,
        );
        let mut plugin = record_to_proto(&record);
        plugin.referenced_by = referencing.iter().map(|p| p.name.clone()).collect();
        Ok(Response::new(UpsertNetPluginReply {
            plugin: Some(plugin),
            deduplicated,
        }))
    }

    pub(super) async fn get_net_plugin_rpc(
        &self,
        request: Request<GetNetPluginRequest>,
    ) -> Result<Response<GetNetPluginReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let name = request.into_inner().name;
        let plugin = self.described_plugin(&identity, &name)?;
        Ok(Response::new(GetNetPluginReply {
            plugin: Some(plugin),
        }))
    }

    pub(super) async fn list_net_plugins_rpc(
        &self,
        request: Request<ListNetPluginsRequest>,
    ) -> Result<Response<ListNetPluginsReply>, Status> {
        self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let plugins = self
            .store
            .list_net_plugins()
            .map_err(|e| map_store_error("Error listing net plugins", e))?
            .iter()
            .map(record_to_proto)
            .collect();
        let generation = self
            .store
            .net_plugin_generation()
            .map_err(|e| map_store_error("Error reading the net plugin generation", e))?;
        Ok(Response::new(ListNetPluginsReply {
            plugins,
            generation,
        }))
    }

    pub(super) async fn delete_net_plugin_rpc(
        &self,
        request: Request<DeleteNetPluginRequest>,
    ) -> Result<Response<DeleteNetPluginReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Net,
            AclAction::Write,
            AclScope::any(),
        )?;
        let name = request.into_inner().name;
        let record = self
            .store
            .get_net_plugin(&name)
            .map_err(|e| map_store_error("Error loading net plugin", e))?
            .ok_or_else(|| Status::not_found(format!("net plugin '{name}' not found")))?;
        enforce_net_plugin_scope(&identity, AclAction::Write, &record.object)?;
        let referencing = self.policies_referencing_plugin(&name)?;
        if !referencing.is_empty() {
            return Err(Status::failed_precondition(format!(
                "net plugin '{name}' is referenced by policy object(s) {}; remove those \
                 references first",
                referencing
                    .iter()
                    .map(|p| p.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let jobs = self.jobs_using_plugin(&name)?;
        if !jobs.is_empty() {
            let (readable, hidden): (Vec<&String>, Vec<&String>) = jobs.iter().partition(|job| {
                identity.allows(AclResource::Job, AclAction::Read, &AclScope::job(job))
            });
            let mut named: Vec<String> = readable.iter().map(|job| format!("'{job}'")).collect();
            if !hidden.is_empty() {
                named.push(format!("{} job(s) you cannot read", hidden.len()));
            }
            return Err(Status::failed_precondition(format!(
                "net plugin '{name}' is used by job(s) {}; update or stop them first",
                named.join(", ")
            )));
        }
        self.store
            .delete_net_plugin(&name)
            .map_err(|e| map_store_error("Error deleting net plugin", e))?;
        self.store.events().net_plugin_deleted(&name);
        Ok(Response::new(DeleteNetPluginReply {}))
    }

    pub(super) async fn get_net_plugin_status_rpc(
        &self,
        request: Request<GetNetPluginStatusRequest>,
    ) -> Result<Response<GetNetPluginStatusReply>, Status> {
        let identity =
            self.check_acl(&request, AclResource::Net, AclAction::Read, AclScope::any())?;
        let name = request.into_inner().name;
        let plugin = self.described_plugin(&identity, &name)?;
        let mut nodes: Vec<NetPluginNodeStatus> = self
            .net_plugin_status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .flat_map(|(node_id, statuses)| {
                statuses
                    .iter()
                    .filter(|status| status.plugin == name)
                    .map(|status| NetPluginNodeStatus {
                        node_id: node_id.clone(),
                        ..status.clone()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(Response::new(GetNetPluginStatusReply {
            plugin: Some(plugin),
            nodes,
        }))
    }

    /// One plugin with `referenced_by` and `referenced_by_jobs` filled, or
    /// NotFound. Jobs the caller cannot read are left out.
    fn described_plugin(
        &self,
        identity: &crate::structs::AclIdentity,
        name: &str,
    ) -> Result<NetPlugin, Status> {
        let record = self
            .store
            .get_net_plugin(name)
            .map_err(|e| map_store_error("Error loading net plugin", e))?
            .ok_or_else(|| Status::not_found(format!("net plugin '{name}' not found")))?;
        let mut plugin = record_to_proto(&record);
        plugin.referenced_by = self
            .policies_referencing_plugin(name)?
            .into_iter()
            .map(|p| p.name)
            .collect();
        plugin.referenced_by_jobs = self
            .jobs_using_plugin(name)?
            .into_iter()
            .filter(|job| identity.allows(AclResource::Job, AclAction::Read, &AclScope::job(job)))
            .collect();
        Ok(plugin)
    }

    /// Ids of the jobs that are not stopped and whose latest config names
    /// the plugin in a chain or an upstream's handlers, sorted. A stopped
    /// job is checked when it is next run.
    fn jobs_using_plugin(&self, name: &str) -> Result<Vec<String>, Status> {
        let jobs = self
            .store
            .get_jobs()
            .map_err(|e| map_store_error("Error listing jobs", e))?;
        let mut using = Vec::new();
        for job in jobs.iter().filter(|job| job.status != "stopped") {
            let Some(config) = self
                .store
                .get_job_config_optional(&job.id)
                .map_err(|e| map_store_error("Error loading job config", e))?
            else {
                continue;
            };
            if config
                .tasks
                .iter()
                .flat_map(job_plugin_hops)
                .any(|hop| hop.reference.name == name)
            {
                using.push(job.id.clone());
            }
        }
        using.sort();
        Ok(using)
    }

    /// Stored policy objects whose chains reference `name`.
    fn policies_referencing_plugin(&self, name: &str) -> Result<Vec<NetPolicyObject>, Status> {
        Ok(self
            .store
            .list_net_policies()
            .map_err(|e| map_store_error("Error listing net policies", e))?
            .into_iter()
            .map(|record| record.object)
            .filter(|policy| {
                chains(policy)
                    .flat_map(|(_, chain)| chain.handlers.iter())
                    .any(|hop| hop_plugin(hop).is_some_and(|p| p.name == name))
            })
            .collect())
    }

    /// Every plugin hop in a policy object's chains names a stored plugin it
    /// may use, at a stage the chain runs, with a config the schema accepts.
    pub(super) fn check_policy_plugins(&self, policy: &NetPolicyObject) -> Result<(), Status> {
        let mut checked: HashMap<String, Option<NetPluginObject>> = HashMap::new();
        for (direction, chain) in chains(policy) {
            for (index, hop) in chain.handlers.iter().enumerate() {
                let Some(reference) = hop_plugin(hop) else {
                    continue;
                };
                let object = self.plugin_for_check(&mut checked, &reference.name)?;
                let what = format!("policy '{}' {direction}.handlers[{index}]", policy.name);
                check_plugin_reference(
                    &what,
                    reference,
                    object.as_ref(),
                    PluginUse::Policy,
                    chain.is_http(),
                    &not_http(direction),
                )
                .and_then(|()| match object.as_ref() {
                    Some(object) => check_policy_pools(&what, policy, object),
                    None => Ok(()),
                })
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Every plugin hop in a job's tasks (inline chains and upstream
    /// handlers) names a stored `usableBy = "jobs"` plugin, at a stage the
    /// path runs, with a config the schema accepts. Applied at RunJob and
    /// template runs.
    pub(super) fn check_job_plugins(&self, tasks: &[TaskConfig]) -> Result<(), Status> {
        let mut checked: HashMap<String, Option<NetPluginObject>> = HashMap::new();
        for hop in tasks.iter().flat_map(job_plugin_hops) {
            let object = self.plugin_for_check(&mut checked, &hop.reference.name)?;
            hop.check(object.as_ref())
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        Ok(())
    }

    /// Re-check every job that uses the plugin against its new object: the
    /// latest config of each job that is not stopped. A stopped job is
    /// checked again when it is next run. The first job the new object
    /// would refuse refuses the write, named when the caller may read it.
    fn check_jobs_against_plugin(
        &self,
        identity: &crate::structs::AclIdentity,
        object: &NetPluginObject,
    ) -> Result<(), Status> {
        let jobs = self
            .store
            .get_jobs()
            .map_err(|e| map_store_error("Error listing jobs", e))?;
        for job in jobs.iter().filter(|job| job.status != "stopped") {
            let Some(config) = self
                .store
                .get_job_config_optional(&job.id)
                .map_err(|e| map_store_error("Error loading job config", e))?
            else {
                continue;
            };
            for hop in config
                .tasks
                .iter()
                .flat_map(job_plugin_hops)
                .filter(|hop| hop.reference.name == object.name)
            {
                if let Err(err) = hop.check(Some(object)) {
                    let job_name = if identity.allows(
                        AclResource::Job,
                        AclAction::Read,
                        &AclScope::job(&job.id),
                    ) {
                        format!("job '{}'", job.id)
                    } else {
                        "a job you cannot read".to_string()
                    };
                    return Err(Status::failed_precondition(format!(
                        "{job_name} uses plugin '{}' and the new object would refuse it: {err}. \
                         Update or stop the job first",
                        object.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn plugin_for_check(
        &self,
        checked: &mut HashMap<String, Option<NetPluginObject>>,
        name: &str,
    ) -> Result<Option<NetPluginObject>, Status> {
        if let Some(object) = checked.get(name) {
            return Ok(object.clone());
        }
        let object = self
            .store
            .get_net_plugin(name)
            .map_err(|e| map_store_error("Error loading net plugin", e))?
            .map(|record| record.object);
        checked.insert(name.to_string(), object.clone());
        Ok(object)
    }

    /// Keep the node's latest plugin report (in memory, replaced per node).
    pub(super) fn record_net_plugin_status(&self, node_id: &str, report: &[NetPluginNodeStatus]) {
        let mut status = self
            .net_plugin_status
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if report.is_empty() {
            status.remove(node_id);
        } else {
            status.insert(node_id.to_string(), report.to_vec());
        }
    }
}

/// A policy object's chains, with their direction.
fn chains(policy: &NetPolicyObject) -> impl Iterator<Item = (&'static str, &ChainConfig)> {
    [("egress", &policy.egress), ("ingress", &policy.ingress)]
        .into_iter()
        .filter_map(|(direction, chain)| chain.as_ref().map(|chain| (direction, chain)))
}

fn not_http(direction: &str) -> String {
    format!(
        "{direction}.protocol is \"tcp\". Set protocol = \"http\" for a plugin that acts on \
         requests, or declare stage \"connect\""
    )
}

/// One `{ plugin = ... }` hop in a job's task: where it is, what it names,
/// and whether its path is HTTP (and what to say when it is not).
struct JobPluginHop<'a> {
    what: String,
    reference: &'a crate::structs::netplugin::PluginConfig,
    is_http: bool,
    where_http: String,
}

impl JobPluginHop<'_> {
    fn check(&self, object: Option<&NetPluginObject>) -> R<()> {
        check_plugin_reference(
            &self.what,
            self.reference,
            object,
            PluginUse::Job,
            self.is_http,
            &self.where_http,
        )
    }
}

/// Every plugin hop in a task: its inline chains and its upstreams'
/// handlers.
fn job_plugin_hops(task: &TaskConfig) -> Vec<JobPluginHop<'_>> {
    let mut hops = Vec::new();
    if let Some(network) = task.network.as_ref() {
        for (direction, chain) in [("egress", &network.egress), ("ingress", &network.ingress)] {
            let Some(chain) = chain.as_ref() else {
                continue;
            };
            for (index, hop) in chain.handlers.iter().enumerate() {
                if let Some(reference) = hop_plugin(hop) {
                    hops.push(JobPluginHop {
                        what: format!("task {} network.{direction}.handlers[{index}]", task.name),
                        reference,
                        is_http: chain.is_http(),
                        where_http: not_http(direction),
                    });
                }
            }
        }
    }
    for upstream in task.connect.iter().flat_map(|c| c.upstreams.iter()) {
        let key = upstream.key();
        for (index, hop) in upstream.handlers.iter().enumerate() {
            if let Some(reference) = hop_plugin(hop) {
                hops.push(JobPluginHop {
                    what: format!(
                        "task {} connect.upstreams entry '{key}' handlers[{index}]",
                        task.name
                    ),
                    reference,
                    is_http: upstream.is_http(),
                    where_http: format!(
                        "upstream '{key}' is not HTTP. Give it an http:// or https:// target"
                    ),
                });
            }
        }
    }
    hops
}

/// The plugin re-checked against one stored policy that uses it.
fn check_policy_against_plugin(policy: &NetPolicyObject, object: &NetPluginObject) -> R<()> {
    for (direction, chain) in chains(policy) {
        for (index, hop) in chain.handlers.iter().enumerate() {
            let Some(reference) = hop_plugin(hop).filter(|p| p.name == object.name) else {
                continue;
            };
            let what = format!("policy '{}' {direction}.handlers[{index}]", policy.name);
            check_plugin_reference(
                &what,
                reference,
                Some(object),
                PluginUse::Policy,
                chain.is_http(),
                &not_http(direction),
            )?;
            check_policy_pools(&what, policy, object)?;
        }
    }
    Ok(())
}

/// A pool-scoped plugin may be used only by a policy that selects pools the
/// plugin covers; otherwise the policy would put the plugin on nodes it may
/// not run on.
fn check_policy_pools(what: &str, policy: &NetPolicyObject, object: &NetPluginObject) -> R<()> {
    if object.node_pools.is_empty() {
        return Ok(());
    }
    let pools = &policy.selector.node_pools;
    let covered = !pools.is_empty()
        && pools.iter().all(|pool| {
            object
                .node_pools
                .iter()
                .any(|glob| crate::structs::netpolicy::glob_matches(glob, pool))
        });
    if !covered {
        bail!(
            "{what}: plugin '{}' runs only on node pools {}; the policy must select node pools \
             within those (selector.nodePools)",
            object.name,
            object.node_pools.join(", ")
        );
    }
    Ok(())
}

/// `net:write` on each pool a pool-scoped `usableBy = "policies"` plugin
/// names; unscoped otherwise (a cluster-wide plugin, one every job may
/// reference, or one whose module is a node file).
fn enforce_net_plugin_scope(
    identity: &crate::structs::AclIdentity,
    action: AclAction,
    object: &NetPluginObject,
) -> Result<(), Status> {
    // A `file://` module is read from the node's disk as root, so it is
    // cluster-wide however the plugin is scoped (like a `file://` artifact,
    // which makes a job host-level).
    let local_module = object.module.source.starts_with("file://");
    if object.node_pools.is_empty() || object.usable_by_jobs() || local_module {
        return acl::enforce(identity, AclResource::Net, action, &AclScope::none());
    }
    for pool in &object.node_pools {
        acl::enforce(identity, AclResource::Net, action, &AclScope::pool(pool))?;
    }
    Ok(())
}

fn record_to_proto(record: &crate::storage::netplugin_store::NetPluginRecord) -> NetPlugin {
    net_plugin_to_proto(
        &record.object,
        record.revision,
        &record.created_at,
        &record.updated_at,
    )
}

fn net_plugin_to_proto(
    object: &NetPluginObject,
    revision: u64,
    created_at: &str,
    updated_at: &str,
) -> NetPlugin {
    NetPlugin {
        name: object.name.clone(),
        object_json: serde_json::to_string(object).unwrap_or_default(),
        revision,
        created_at: created_at.to_string(),
        updated_at: updated_at.to_string(),
        stages: object.stages.clone(),
        usable_by: object.usable_by.clone(),
        module_checksum: object.module.checksum.clone(),
        referenced_by: Vec::new(),
        referenced_by_jobs: Vec::new(),
    }
}

#[cfg(test)]
mod plugin_rpcs_test {
    use super::*;

    fn plugin(pools: &[&str]) -> NetPluginObject {
        let mut object: NetPluginObject = serde_json::from_value(json!({
            "name": "p",
            "module": { "source": "https://x/p.wasm", "checksum": format!("sha256:{}", "c".repeat(64)) },
            "stages": ["request"],
            "nodePools": pools,
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        object
    }

    fn policy(pools: &[&str]) -> NetPolicyObject {
        let mut policy: NetPolicyObject = serde_json::from_value(json!({
            "name": "pol",
            "selector": { "nodePools": pools },
            "egress": { "protocol": "http", "allow": ["*"], "handlers": [{ "plugin": { "name": "p" } }] },
        }))
        .unwrap();
        policy.normalize_and_validate().unwrap();
        policy
    }

    #[test]
    fn a_pool_scoped_plugin_needs_a_policy_within_its_pools_test() {
        check_policy_against_plugin(&policy(&[]), &plugin(&[])).unwrap();
        check_policy_against_plugin(&policy(&["gpu-a"]), &plugin(&["gpu-*"])).unwrap();
        let err = check_policy_against_plugin(&policy(&[]), &plugin(&["gpu-*"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("runs only on node pools gpu-*"), "{err}");
        assert!(check_policy_against_plugin(&policy(&["cpu"]), &plugin(&["gpu-*"])).is_err());
    }
}
