//! Namespace policy RPCs and the policy half of job admission
//! (docs/namespaces.md, "Namespace policies").
//!
//! The static rules (runtimes, pools, host volumes) need only the jobspec
//! and run here, before the submit, so a refusal names the task to fix and
//! a policy's default pool is written into the stored config. The rules
//! that need cluster state (quota, `maxJobs`, `maxNamespaces`) run in the
//! store, inside the replicated write they guard
//! (`crate::storage::namespace_policy_store`).

use std::collections::BTreeMap;

use super::*;
use crate::storage::namespace_policy_store::NamespacePolicyRecord;
use crate::storage::namespace_store::NamespaceUsage;
use crate::structs::namespace::job_namespace;
use crate::structs::namespace_policy::{admit_job, NamespacePolicy as PolicyObject};
use crate::structs::{
    ApplyNamespacePolicyReply, ApplyNamespacePolicyRequest, BandUsage, DeleteNamespacePolicyReply,
    DeleteNamespacePolicyRequest, GetNamespacePolicyReply, GetNamespacePolicyRequest,
    ListNamespacePoliciesReply, ListNamespacePoliciesRequest, NamespaceBandUsages, NamespacePolicy,
    NamespacePolicyStatus, NamespacePolicyViolation,
};

fn policy_to_proto(policy: &PolicyObject, created_at: &str, updated_at: &str) -> NamespacePolicy {
    NamespacePolicy {
        name: policy.name.clone(),
        namespaces: policy.namespaces.clone(),
        object_json: serde_json::to_string(policy).unwrap_or_default(),
        description: policy.description.clone(),
        created_at: created_at.to_string(),
        updated_at: updated_at.to_string(),
    }
}

fn record_to_proto(record: &NamespacePolicyRecord) -> NamespacePolicy {
    policy_to_proto(&record.policy, &record.created_at, &record.updated_at)
}

/// Band usages, in band order (critical first).
pub(super) fn band_usages(usage: &NamespaceUsage) -> Vec<BandUsage> {
    crate::structs::Band::ALL
        .iter()
        .filter_map(|band| {
            let reservation = usage.bands.get(band.as_str())?;
            Some(BandUsage {
                band: band.as_str().to_string(),
                cpu: reservation.cpu,
                memory: reservation.memory,
                cores: reservation.cores,
                allocations: reservation.allocations,
            })
        })
        .collect()
}

/// A policy with its aggregate over every namespace it matches.
fn policy_status(
    record: &NamespacePolicyRecord,
    charges: &BTreeMap<String, NamespaceUsage>,
    namespaces: &[String],
) -> NamespacePolicyStatus {
    let mut total = NamespaceUsage::default();
    for (namespace, usage) in charges {
        if record.policy.matches(namespace) {
            total.absorb(usage);
        }
    }
    NamespacePolicyStatus {
        policy: Some(record_to_proto(record)),
        usage: band_usages(&total),
        jobs: total.jobs,
        namespaces: namespaces
            .iter()
            .filter(|name| record.policy.matches(name))
            .count() as u64,
    }
}

/// The rule a static refusal came from, for dry-run reports.
fn static_rules(policy: &PolicyObject) -> Vec<(&'static str, PolicyObject)> {
    let only = |rule: &'static str, f: &dyn Fn(&mut PolicyObject)| {
        let mut narrowed = PolicyObject {
            name: policy.name.clone(),
            namespaces: policy.namespaces.clone(),
            ..Default::default()
        };
        f(&mut narrowed);
        (rule, narrowed)
    };
    let mut rules = Vec::new();
    if policy.runtimes.is_some() {
        rules.push(only("runtimes", &|p| p.runtimes = policy.runtimes.clone()));
    }
    if policy.pools.is_some() {
        rules.push(only("pools", &|p| p.pools = policy.pools.clone()));
    }
    if policy.host_volumes.is_some() {
        rules.push(only("hostVolumes", &|p| {
            p.host_volumes = policy.host_volumes.clone()
        }));
    }
    rules
}

impl ManagerDaemonService {
    /// The static half of admission for a job about to be submitted: every
    /// policy governing its namespace, checked against the spec, with a
    /// policy's default pool written into tasks that name none.
    pub(super) fn admit_namespace_policies(&self, config: &mut JobConfig) -> Result<(), Status> {
        let policies: Vec<PolicyObject> = self
            .store
            .list_namespace_policies()
            .map_err(|e| map_store_error("Error loading namespace policies", e))?
            .into_iter()
            .map(|record| record.policy)
            .collect();
        admit_job(&policies, config).map_err(Status::failed_precondition)
    }

    /// The policies governing `namespace`, each with its aggregate.
    pub(super) fn namespace_policy_statuses(
        &self,
        namespace: &str,
    ) -> Result<(Vec<BandUsage>, Vec<NamespacePolicyStatus>), Status> {
        let charges = self
            .store
            .namespace_charges()
            .map_err(|e| map_store_error("Error loading namespace usage", e))?;
        let records = self
            .store
            .list_namespace_policies()
            .map_err(|e| map_store_error("Error loading namespace policies", e))?;
        let names: Vec<String> = self
            .store
            .list_namespaces()
            .map_err(|e| map_store_error("Error listing namespaces", e))?
            .into_iter()
            .map(|record| record.namespace.name)
            .collect();
        let own = charges.get(namespace).map(band_usages).unwrap_or_default();
        let statuses = records
            .iter()
            .filter(|record| record.policy.matches(namespace))
            .map(|record| policy_status(record, &charges, &names))
            .collect();
        Ok((own, statuses))
    }

    /// What `policy` would have refused among existing jobs and counts.
    fn namespace_policy_violations(
        &self,
        policy: &PolicyObject,
    ) -> Result<Vec<NamespacePolicyViolation>, Status> {
        let mut violations = Vec::new();
        let names: Vec<String> = self
            .store
            .list_namespaces()
            .map_err(|e| map_store_error("Error listing namespaces", e))?
            .into_iter()
            .map(|record| record.namespace.name)
            .filter(|name| policy.matches(name))
            .collect();
        if let Some(max) = policy.max_namespaces {
            if names.len() as u64 > max {
                violations.push(NamespacePolicyViolation {
                    namespace: policy.selector(),
                    job_id: String::new(),
                    rule: "maxNamespaces".into(),
                    message: format!(
                        "{} namespaces match {}, over maxNamespaces {max}",
                        names.len(),
                        policy.selector()
                    ),
                });
            }
        }
        let jobs: Vec<_> = self
            .store
            .get_jobs()
            .map_err(|e| map_store_error("Error listing jobs", e))?
            .into_iter()
            .filter(|job| policy.matches(job_namespace(&job.id)))
            .collect();
        if let Some(max) = policy.max_jobs {
            if jobs.len() as u64 > max {
                violations.push(NamespacePolicyViolation {
                    namespace: policy.selector(),
                    job_id: String::new(),
                    rule: "maxJobs".into(),
                    message: format!(
                        "{} jobs across {}, over maxJobs {max}",
                        jobs.len(),
                        policy.selector()
                    ),
                });
            }
        }
        let rules = static_rules(policy);
        if !rules.is_empty() {
            for job in &jobs {
                let Ok(config) = self.store.get_job_config(&job.id) else {
                    continue;
                };
                for (rule, narrowed) in &rules {
                    let mut candidate = config.clone();
                    if let Err(message) = admit_job(std::slice::from_ref(narrowed), &mut candidate)
                    {
                        violations.push(NamespacePolicyViolation {
                            namespace: job_namespace(&job.id).to_string(),
                            job_id: job.id.clone(),
                            rule: (*rule).into(),
                            message,
                        });
                    }
                }
            }
        }
        if !policy.quota.is_empty() {
            let charges = self
                .store
                .namespace_charges()
                .map_err(|e| map_store_error("Error loading namespace usage", e))?;
            let mut total = NamespaceUsage::default();
            for (namespace, usage) in &charges {
                if policy.matches(namespace) {
                    total.absorb(usage);
                }
            }
            for (band, limit) in &policy.quota {
                let used = total.bands.get(band).copied().unwrap_or_default();
                for (dimension, cap, value) in [
                    ("cpu", limit.cpu, used.cpu),
                    ("memory", limit.memory, used.memory),
                    ("cores", limit.cores, used.cores),
                ] {
                    if let Some(cap) = cap.filter(|cap| value > *cap) {
                        violations.push(NamespacePolicyViolation {
                            namespace: policy.selector(),
                            job_id: String::new(),
                            rule: "quota".into(),
                            message: format!(
                                "band '{band}' holds {value} {dimension} across {}, over the \
                                 cap of {cap}; admitted jobs keep running, and new or growing \
                                 ones are refused until it drops",
                                policy.selector()
                            ),
                        });
                    }
                }
            }
        }
        Ok(violations)
    }

    // ACL: acl:read — policies are governance objects.
    pub(super) async fn list_namespace_policies_rpc(
        &self,
        request: Request<ListNamespacePoliciesRequest>,
    ) -> Result<Response<ListNamespacePoliciesReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let policies = self
            .store
            .list_namespace_policies()
            .map_err(|e| map_store_error("Error listing namespace policies", e))?
            .iter()
            .map(record_to_proto)
            .collect();
        Ok(Response::new(ListNamespacePoliciesReply { policies }))
    }

    // ACL: acl:read.
    pub(super) async fn get_namespace_policy_rpc(
        &self,
        request: Request<GetNamespacePolicyRequest>,
    ) -> Result<Response<GetNamespacePolicyReply>, Status> {
        self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Read,
            AclScope::none(),
        )?;
        let name = request.get_ref().name.trim().to_string();
        let record = self
            .store
            .get_namespace_policy(&name)
            .map_err(|e| map_store_error("Error loading namespace policy", e))?
            .ok_or_else(|| {
                Status::not_found(format!("namespace policy '{name}' does not exist"))
            })?;
        let charges = self
            .store
            .namespace_charges()
            .map_err(|e| map_store_error("Error loading namespace usage", e))?;
        let names: Vec<String> = self
            .store
            .list_namespaces()
            .map_err(|e| map_store_error("Error listing namespaces", e))?
            .into_iter()
            .map(|record| record.namespace.name)
            .collect();
        let by_namespace = names
            .iter()
            .filter(|name| record.policy.matches(name))
            .map(|name| {
                let usage = charges.get(name).map(band_usages).unwrap_or_default();
                (name.clone(), NamespaceBandUsages { usage })
            })
            .collect();
        Ok(Response::new(GetNamespacePolicyReply {
            status: Some(policy_status(&record, &charges, &names)),
            by_namespace,
        }))
    }

    // ACL: acl:write — whoever may write ACL policy could grant anything
    // anyway, so governance at the same privilege adds no escalation path.
    pub(super) async fn apply_namespace_policy_rpc(
        &self,
        request: Request<ApplyNamespacePolicyRequest>,
    ) -> Result<Response<ApplyNamespacePolicyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let inner = request.into_inner();
        let policy =
            PolicyObject::from_json(&inner.object_json).map_err(Status::invalid_argument)?;
        let existing = self
            .store
            .get_namespace_policy(&policy.name)
            .map_err(|e| map_store_error("Error loading namespace policy", e))?;
        // `autoscale`: every autoscaled task the policy governs must still
        // resolve (docs/autoscaling.md, "Overriding the defaults").
        let autoscale_violations = self.autoscale_policy_violations(&policy)?;
        if inner.dry_run {
            let mut violations = self.namespace_policy_violations(&policy)?;
            violations.extend(autoscale_violations);
            let (created_at, updated_at) = existing
                .as_ref()
                .map(|record| (record.created_at.clone(), record.updated_at.clone()))
                .unwrap_or_default();
            return Ok(Response::new(ApplyNamespacePolicyReply {
                policy: Some(policy_to_proto(&policy, &created_at, &updated_at)),
                created: existing.is_none(),
                violations,
                deduplicated: false,
            }));
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "apply_namespace_policy",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if let Some(recorded) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            if let Some(record) = self
                .store
                .get_namespace_policy(&recorded)
                .map_err(|e| map_store_error("Error loading namespace policy", e))?
            {
                return Ok(Response::new(ApplyNamespacePolicyReply {
                    policy: Some(record_to_proto(&record)),
                    created: false,
                    violations: Vec::new(),
                    deduplicated: true,
                }));
            }
        }
        if !autoscale_violations.is_empty() && !inner.force {
            let listed: Vec<String> = autoscale_violations
                .iter()
                .take(10)
                .map(|violation| format!("  {}", violation.message))
                .collect();
            return Err(Status::failed_precondition(format!(
                "namespace policy '{}': its autoscale would leave {} autoscaled task(s) with an \
                 invalid policy:\n{}\nHint: fix those tasks first, or apply with --force to \
                 hold them `invalid` until fixed",
                policy.name,
                autoscale_violations.len(),
                listed.join("\n")
            )));
        }
        let (record, created) = self
            .store
            .apply_namespace_policy(policy)
            .map_err(|e| map_store_error("Error applying namespace policy", e))?;
        if !idempotency_key.is_empty() {
            self.store
                .idempotency_record(
                    &idempotency_key,
                    "apply_namespace_policy",
                    &record.policy.name,
                )
                .map_err(|e| map_store_error("Error recording idempotency", e))?;
        }
        info!(
            policy = %record.policy.name,
            namespaces = %record.policy.selector(),
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            created,
            "Namespace policy applied"
        );
        Ok(Response::new(ApplyNamespacePolicyReply {
            policy: Some(record_to_proto(&record)),
            created,
            violations: Vec::new(),
            deduplicated: false,
        }))
    }

    // ACL: acl:write.
    pub(super) async fn delete_namespace_policy_rpc(
        &self,
        request: Request<DeleteNamespacePolicyRequest>,
    ) -> Result<Response<DeleteNamespacePolicyReply>, Status> {
        let identity = self.check_acl(
            &request,
            AclResource::Acl,
            AclAction::Write,
            AclScope::none(),
        )?;
        let inner = request.into_inner();
        let name = inner.name.trim().to_string();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "delete_namespace_policy",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
            .is_some()
        {
            return Ok(Response::new(DeleteNamespacePolicyReply {
                deleted: true,
                deduplicated: true,
            }));
        }
        let deleted = self
            .store
            .delete_namespace_policy(&name)
            .map_err(|e| map_store_error("Error deleting namespace policy", e))?;
        if !deleted {
            return Err(Status::not_found(format!(
                "namespace policy '{name}' does not exist"
            )));
        }
        if !idempotency_key.is_empty() {
            self.store
                .idempotency_record(&idempotency_key, "delete_namespace_policy", &name)
                .map_err(|e| map_store_error("Error recording idempotency", e))?;
        }
        info!(
            policy = %name,
            accessor_id = identity.accessor_id.as_deref().unwrap_or("-"),
            "Namespace policy deleted"
        );
        Ok(Response::new(DeleteNamespacePolicyReply {
            deleted: true,
            deduplicated: false,
        }))
    }
}
