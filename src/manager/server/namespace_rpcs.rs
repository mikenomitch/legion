//! Namespace RPCs and the namespace half of job admission
//! (docs/namespaces.md).
//!
//! A namespace is a name scope, a permission scope and a policy anchor —
//! not an isolation boundary. Its contents are not linked to it by a foreign
//! key: a job's namespace is part of its id (`ns/name`, or the bare name in
//! `default`), so the handlers here never "attach" anything. They create and
//! delete the namespace row, answer who may see it, and drive the
//! synchronous `--cascade` delete through the ordinary stop/destroy paths.

use super::*;
use crate::storage::namespace_store::{NamespaceContents, NamespaceRecord};
use crate::structs::acl::{rule_reaches_namespace, AclResource};
use crate::structs::namespace::{
    job_id, job_name, qualified_display, validate_namespace_name, Namespace, DEFAULT_NAMESPACE,
};
use crate::structs::{
    ApplyNamespaceReply, ApplyNamespaceRequest, DeleteNamespaceReply, DeleteNamespaceRequest,
    GetNamespaceReply, GetNamespaceRequest, ListNamespacesReply, ListNamespacesRequest,
    NamespaceCounts, NamespaceReach,
};

/// How long a `--cascade` waits for a namespace's stopped jobs to drain
/// their allocations before destroying the rows anyway. Long enough for the
/// default graceful-stop ladder; a job whose kill timeout is longer is
/// destroyed with its stragglers still stopping, and the worker reclaims them
/// when their rows disappear.
const CASCADE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// The namespace a jobspec *declares*: its `namespace` field, or the
/// namespace segment of a qualified `name`. Read from the raw JSON, because
/// a parsed `JobConfig` folds `namespace = "default"` into a bare name and so
/// can no longer say whether `default` was written or implied.
pub(super) fn declared_namespace(jobspec: &str) -> Option<String> {
    let value: Value = serde_json::from_str(jobspec).ok()?;
    if let Some(namespace) = value.get("namespace").and_then(Value::as_str) {
        return Some(namespace.to_string());
    }
    value
        .get("name")
        .and_then(Value::as_str)
        .and_then(|name| name.split_once('/'))
        .map(|(namespace, _)| namespace.to_string())
}

fn record_to_proto(
    record: &NamespaceRecord,
    counts: Option<NamespaceCounts>,
) -> crate::structs::Namespace {
    crate::structs::Namespace {
        name: record.namespace.name.clone(),
        description: record.namespace.description.clone(),
        meta: record.namespace.meta.clone().into_iter().collect(),
        created_at: record.created_at.clone(),
        updated_at: record.updated_at.clone(),
        counts,
    }
}

fn counts_of(contents: &NamespaceContents) -> NamespaceCounts {
    NamespaceCounts {
        jobs: contents.jobs.len() as u32,
        volumes: contents.volumes.len() as u32,
        snapshots: contents.snapshots.len() as u32,
        objects: contents.objects.len() as u32,
    }
}

fn contents_to_proto(contents: &NamespaceContents) -> crate::structs::NamespaceContents {
    crate::structs::NamespaceContents {
        jobs: contents.jobs.clone(),
        volumes: contents.volumes.clone(),
        snapshots: contents.snapshots.clone(),
        objects: contents.objects.clone(),
    }
}

impl ManagerDaemonService {
    /// Resolves a parsed job's namespace and rewrites `config.name` to the
    /// canonical id. The jobspec's declared namespace and the request's must
    /// agree when both are set: two inputs that disagree have no correct
    /// precedence, and Nomad's CVE-2024-10975 was exactly a spec field
    /// overriding an authorized request field. Returns the namespace;
    /// authorization runs against it, and existence is checked after
    /// authorization (see [`Self::ensure_namespace_exists`]).
    pub(super) fn admit_job_namespace(
        &self,
        config: &mut JobConfig,
        jobspec: &str,
        requested: Option<&str>,
    ) -> Result<String, Status> {
        let requested = requested.map(str::trim).filter(|ns| !ns.is_empty());
        let declared = declared_namespace(jobspec);
        let namespace = match (declared.as_deref(), requested) {
            (Some(declared), Some(requested)) if declared != requested => {
                return Err(Status::invalid_argument(format!(
                    "jobspec declares namespace '{declared}' but the request names \
                     '{requested}'; remove one (a jobspec that names its namespace does not \
                     need -n)"
                )));
            }
            (Some(declared), _) => declared.to_string(),
            (None, Some(requested)) => requested.to_string(),
            (None, None) => DEFAULT_NAMESPACE.to_string(),
        };
        validate_namespace_name(&namespace).map_err(Status::invalid_argument)?;
        config.name = job_id(&namespace, job_name(&config.name));
        Ok(namespace)
    }

    /// NOT_FOUND unless `namespace` exists. Namespaces are never created
    /// implicitly: a typo must not fork a tenant. Call after authorization,
    /// so a caller with no grant there cannot probe which namespaces exist.
    pub(super) fn ensure_namespace_exists(&self, namespace: &str) -> Result<(), Status> {
        let exists = self
            .store
            .namespace_exists(namespace)
            .map_err(|e| map_store_error("Error checking namespace", e))?;
        if exists {
            Ok(())
        } else {
            Err(Status::not_found(format!(
                "namespace '{namespace}' does not exist; `legion namespace list` shows the \
                 namespaces you can read, and `legion namespace create {namespace}` creates it"
            )))
        }
    }

    /// A job lookup's error, rewritten when it is NotFound to name the
    /// namespace that was searched and, when jobs of that name exist in
    /// other namespaces the caller may read, where they are. Only readable
    /// namespaces are named, so the hint is no existence oracle.
    pub(super) fn job_lookup_error(
        &self,
        identity: &AclIdentity,
        job_id: &str,
        status: Status,
    ) -> Status {
        if status.code() != tonic::Code::NotFound {
            return status;
        }
        let namespace = crate::structs::namespace::job_namespace(job_id);
        let name = job_name(job_id);
        let mut elsewhere: Vec<String> = self
            .store
            .get_jobs()
            .unwrap_or_default()
            .into_iter()
            .filter(|job| job_name(&job.id) == name && job.id != job_id)
            .filter(|job| {
                identity.allows(AclResource::Job, AclAction::Read, &AclScope::job(&job.id))
            })
            .map(|job| crate::structs::namespace::job_namespace(&job.id).to_string())
            .collect();
        elsewhere.sort();
        let message = format!("job '{name}' not found in namespace '{namespace}'");
        match elsewhere.as_slice() {
            [] => Status::not_found(message),
            [one] => Status::not_found(format!(
                "{message}\nHint: a job named '{name}' exists in namespace '{one}'; retry with \
                 -n {one}"
            )),
            many => Status::not_found(format!(
                "{message}\nHint: jobs named '{name}' exist in namespaces {}; pick one with -n",
                many.join(", ")
            )),
        }
    }

    /// The namespaces `identity` holds any allow in, by name (every
    /// namespace for a management identity).
    pub(super) fn readable_namespaces(&self, identity: &AclIdentity) -> Vec<String> {
        self.store
            .list_namespaces()
            .unwrap_or_default()
            .into_iter()
            .map(|record| record.namespace.name)
            .filter(|name| identity.reaches_namespace(name))
            .collect()
    }

    /// Every allow rule, across all ACL policies, that reaches `namespace`:
    /// the blast-radius report `namespace create --dry-run` and `namespace
    /// get` show.
    fn namespace_reach(&self, namespace: &str) -> Vec<NamespaceReach> {
        let mut reach = Vec::new();
        for policy in self.store.list_acl_policies().unwrap_or_default() {
            let Ok(rules) = crate::structs::acl::parse_rules(&policy.rules_json) else {
                continue;
            };
            for (index, rule) in rules.iter().enumerate() {
                if !rule_reaches_namespace(rule, namespace) {
                    continue;
                }
                let grants = rule
                    .resources
                    .iter()
                    .filter(|resource| {
                        resource.as_str() == "*"
                            || AclResource::namespaced().contains(&resource.as_str())
                    })
                    .flat_map(|resource| {
                        rule.actions
                            .iter()
                            .map(move |action| format!("{resource}:{action}"))
                    })
                    .collect();
                reach.push(NamespaceReach {
                    policy: policy.name.clone(),
                    rule_index: index as u32,
                    grants,
                    namespaces: rule.namespaces.clone().unwrap_or_default(),
                });
            }
        }
        reach
    }

    pub(super) async fn list_namespaces_rpc(
        &self,
        request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesReply>, Status> {
        // ACL: per-item. A caller sees only the namespaces it holds an allow
        // in — the filtered list Kubernetes never shipped (kubernetes#48537).
        let identity = self.authenticate_acl(&request)?;
        let records = self
            .store
            .list_namespaces()
            .map_err(|e| map_store_error("Error listing namespaces", e))?;
        let mut namespaces = Vec::new();
        for record in records {
            if !identity.reaches_namespace(&record.namespace.name) {
                continue;
            }
            let counts = self
                .store
                .namespace_contents(&record.namespace.name)
                .map(|contents| counts_of(&contents))
                .ok();
            namespaces.push(record_to_proto(&record, counts));
        }
        // A caller that reaches no namespace at all gets the ordinary ACL
        // error (Unauthenticated for an anonymous caller) rather than an
        // empty list that reads like "there are none".
        if namespaces.is_empty() {
            acl::enforce(
                &identity,
                AclResource::Namespace,
                AclAction::Read,
                &AclScope::any(),
            )?;
        }
        Ok(Response::new(ListNamespacesReply { namespaces }))
    }

    pub(super) async fn get_namespace_rpc(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        validate_namespace_name(&name).map_err(Status::invalid_argument)?;
        let identity = self.check_acl(
            &request,
            AclResource::Namespace,
            AclAction::Read,
            AclScope::namespace(&name),
        )?;
        let record = self
            .store
            .get_namespace(&name)
            .map_err(|e| map_store_error("Error loading namespace", e))?
            .ok_or_else(|| Status::not_found(format!("namespace '{name}' does not exist")))?;
        let contents = self
            .store
            .namespace_contents(&name)
            .map_err(|e| map_store_error("Error loading namespace contents", e))?;
        let reaching_rules =
            if identity.allows(AclResource::Acl, AclAction::Read, &AclScope::none()) {
                self.namespace_reach(&name)
            } else {
                Vec::new()
            };
        let (usage, policies) = self.namespace_policy_statuses(&name)?;
        Ok(Response::new(GetNamespaceReply {
            namespace: Some(record_to_proto(&record, Some(counts_of(&contents)))),
            reaching_rules,
            usage,
            policies,
        }))
    }

    pub(super) async fn apply_namespace_rpc(
        &self,
        request: Request<ApplyNamespaceRequest>,
    ) -> Result<Response<ApplyNamespaceReply>, Status> {
        let incoming = request
            .get_ref()
            .namespace
            .clone()
            .ok_or_else(|| Status::invalid_argument("namespace is required"))?;
        let name = incoming.name.trim().to_string();
        validate_namespace_name(&name).map_err(Status::invalid_argument)?;
        let identity = self.check_acl(
            &request,
            AclResource::Namespace,
            AclAction::Write,
            AclScope::namespace(&name),
        )?;
        let dry_run = request.get_ref().dry_run;
        let namespace = Namespace {
            name: name.clone(),
            description: incoming.description,
            meta: incoming.meta.into_iter().collect(),
        };
        let reaching_rules =
            if identity.allows(AclResource::Acl, AclAction::Read, &AclScope::none()) {
                self.namespace_reach(&name)
            } else {
                Vec::new()
            };
        if dry_run {
            let existing = self
                .store
                .get_namespace(&name)
                .map_err(|e| map_store_error("Error loading namespace", e))?;
            if existing.is_none() {
                crate::structs::namespace::validate_new_namespace_name(&name)
                    .map_err(Status::invalid_argument)?;
            }
            let record = existing.clone().unwrap_or(NamespaceRecord {
                namespace: namespace.clone(),
                created_at: String::new(),
                updated_at: String::new(),
            });
            let record = NamespaceRecord {
                namespace,
                ..record
            };
            return Ok(Response::new(ApplyNamespaceReply {
                namespace: Some(record_to_proto(&record, None)),
                created: existing.is_none(),
                reaching_rules,
            }));
        }
        let (record, created) = self
            .store
            .apply_namespace(namespace)
            .map_err(|e| map_store_error("Error saving namespace", e))?;
        self.store.events().namespace_applied(&name, created);
        info!(
            namespace = %name,
            created,
            principal = %identity.principal,
            "namespace applied"
        );
        Ok(Response::new(ApplyNamespaceReply {
            namespace: Some(record_to_proto(&record, None)),
            created,
            reaching_rules,
        }))
    }

    pub(super) async fn delete_namespace_rpc(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceReply>, Status> {
        let name = request.get_ref().name.trim().to_string();
        validate_namespace_name(&name).map_err(Status::invalid_argument)?;
        let identity = self.check_acl(
            &request,
            AclResource::Namespace,
            AclAction::Write,
            AclScope::namespace(&name),
        )?;
        let inner = request.into_inner();
        if name == DEFAULT_NAMESPACE {
            return Err(Status::failed_precondition(
                "the default namespace cannot be deleted",
            ));
        }
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "delete_namespace",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        if !inner.dry_run {
            if let Some(recorded) = self
                .store
                .idempotency_lookup(&idempotency_key)
                .map_err(|e| map_store_error("Error checking idempotency", e))?
            {
                let steps = serde_json::from_str(&recorded).unwrap_or_default();
                return Ok(Response::new(DeleteNamespaceReply {
                    deleted: true,
                    contents: None,
                    steps,
                    deduplicated: true,
                }));
            }
        }
        if self
            .store
            .get_namespace(&name)
            .map_err(|e| map_store_error("Error loading namespace", e))?
            .is_none()
        {
            return Err(Status::not_found(format!(
                "namespace '{name}' does not exist"
            )));
        }
        let contents = self
            .store
            .namespace_contents(&name)
            .map_err(|e| map_store_error("Error loading namespace contents", e))?;

        if inner.dry_run {
            return Ok(Response::new(DeleteNamespaceReply {
                deleted: false,
                contents: Some(contents_to_proto(&contents)),
                steps: Vec::new(),
                deduplicated: false,
            }));
        }
        if !contents.is_empty() && !inner.cascade {
            return Err(Status::failed_precondition(format!(
                "namespace '{name}' is not empty: {}. `legion namespace delete {name} \
                 --dry-run` lists them; --cascade removes them",
                contents.summary()
            )));
        }

        // A cascade destroys workloads, so it needs the rights a caller would
        // need to destroy them one by one: the right to delete an environment
        // must not be a back door to destroying jobs the caller could not
        // otherwise touch.
        let mut steps = Vec::new();
        if inner.cascade && !contents.is_empty() {
            for job in &contents.jobs {
                acl::enforce(
                    &identity,
                    AclResource::Job,
                    AclAction::Write,
                    &self.job_read_scope(job),
                )?;
            }
            for snapshot in &contents.snapshots {
                if let Ok(Some(record)) = self.store.get_snapshot(snapshot) {
                    acl::enforce(
                        &identity,
                        AclResource::Snapshot,
                        AclAction::Write,
                        &AclScope::job(&record.job_id),
                    )?;
                }
            }
            // Objects of app-declared kinds: the caller's access and each
            // object's `requires`, as deleting them one by one would check
            // (docs/app-objects.md, "Namespaces"). Checked for every object
            // before any is deleted, then deleted first, so their apps stop
            // acting on them before the jobs go.
            let objects = self.namespace_objects_for_cascade(&identity, &name)?;
            steps.extend(self.cascade_delete_objects(&identity, objects)?);
            let store = self.store.clone();
            let priority = self.store.default_eval_priority;
            let jobs = contents.jobs.clone();
            let volumes = contents.volumes.clone();
            let snapshots = contents.snapshots.clone();
            steps.extend(run_blocking_admission(move || {
                cascade_namespace(&store, &jobs, &volumes, &snapshots, priority)
            })?);
        }

        self.store
            .delete_namespace_if_empty(&name)
            .map_err(|e| map_store_error("Error deleting namespace", e))?;
        steps.push(format!("Deleted namespace {name}"));
        self.store.events().namespace_deleted(&name);
        info!(
            namespace = %name,
            cascade = inner.cascade,
            principal = %identity.principal,
            "namespace deleted"
        );
        let _ = self.store.idempotency_record(
            &idempotency_key,
            "delete_namespace",
            &serde_json::to_string(&steps).unwrap_or_default(),
        );
        Ok(Response::new(DeleteNamespaceReply {
            deleted: true,
            contents: Some(contents_to_proto(&contents)),
            steps,
            deduplicated: false,
        }))
    }
}

/// The cascade's work, in its fixed order: stop every job (the ordinary
/// graceful stop), wait for their allocations to drain, destroy the job
/// rows, then delete volumes and snapshots. Net policies are cluster
/// objects, not namespace contents, so nothing protecting a running task is
/// removed before the task stops. Uses only the ordinary store paths, so it
/// depends on no optional component being healthy.
fn cascade_namespace(
    store: &Store,
    jobs: &[String],
    volumes: &[String],
    snapshots: &[String],
    priority: i64,
) -> Result<Vec<String>, Status> {
    let mut steps = Vec::new();
    for job in jobs {
        let status = store
            .get_job(job)
            .map(|job| job.status)
            .map_err(|e| map_store_error("Error loading job", e))?;
        if status == "stopped" {
            continue;
        }
        let config = store
            .get_job_config(job)
            .map_err(|e| map_store_error("Error stopping job", e))?;
        store
            .stop_job(job)
            .map_err(|e| map_store_error("Error stopping job", e))?;
        store
            .enqueue_eval(
                "job_stop",
                Some(job),
                Some(&config.id()),
                None,
                Some(priority),
            )
            .map_err(|e| map_store_error("Error stopping job", e))?;
        steps.push(format!("Stopped {}", qualified_display(job)));
    }

    let deadline = std::time::Instant::now() + CASCADE_DRAIN_TIMEOUT;
    let mut draining: Vec<&String> = jobs.iter().collect();
    while !draining.is_empty() && std::time::Instant::now() < deadline {
        draining.retain(|job| {
            store.allocs_for_job(job).is_ok_and(|allocs| {
                allocs
                    .iter()
                    .any(|alloc| crate::constants::allocation_status_is_dispatchable(&alloc.status))
            })
        });
        if !draining.is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }
    for job in &draining {
        steps.push(format!(
            "{} still had live allocations after {}s; destroying it anyway (its worker \
             reclaims them)",
            qualified_display(job),
            CASCADE_DRAIN_TIMEOUT.as_secs()
        ));
    }

    for job in jobs {
        store
            .destroy_job(job)
            .map_err(|e| map_store_error("Error destroying job", e))?;
        steps.push(format!("Destroyed {}", qualified_display(job)));
    }
    for volume in volumes {
        store
            .delete_volume(volume, false)
            .map_err(|e| map_store_error("Error deleting volume", e))?;
        steps.push(format!("Deleting volume {volume}"));
    }
    for snapshot in snapshots {
        store
            .delete_snapshot(snapshot)
            .map_err(|e| map_store_error("Error deleting snapshot", e))?;
        store.snapshot_warms().forget_snapshot(snapshot);
        steps.push(format!("Deleting snapshot {snapshot}"));
    }
    Ok(steps)
}

#[cfg(test)]
mod namespace_rpcs_test {
    use super::*;

    #[test]
    fn declared_namespace_reads_the_field_or_a_qualified_name() {
        assert_eq!(
            declared_namespace(r#"{"name": "web", "namespace": "prod"}"#).as_deref(),
            Some("prod")
        );
        assert_eq!(
            declared_namespace(r#"{"name": "prod/web"}"#).as_deref(),
            Some("prod")
        );
        assert_eq!(
            declared_namespace(r#"{"name": "web", "namespace": "default"}"#).as_deref(),
            Some("default")
        );
        assert_eq!(declared_namespace(r#"{"name": "web"}"#), None);
        assert_eq!(declared_namespace("not json"), None);
    }
}
