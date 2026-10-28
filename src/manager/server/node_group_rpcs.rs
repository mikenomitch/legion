//! The node-autoscaling substrate (autoscaler v2 plan §6.11): `ForgetNode`,
//! `GetCapacityDemand`, and the join rules node groups need, a join token's
//! `max_uses` and the attributes a registration stamps on its node.
//!
//! # Join attributes
//!
//! A node's config attributes come from three places, and the more
//! specific one wins: the join token's `attributes`, then the host's
//! `--attr` (`WorkerRegisterRequest.join_attributes`), then what the worker
//! reports (its worker type's attributes). Keys under `legion.` are
//! Legion's: the worker may not report them (only what the token, the
//! worker type, or for a restart the node's own record vouches for
//! survives), and `--attr` may set only the per-host ones
//! (`legion.provider-id`). The token's attributes are recorded on the node
//! as `legion.join`, so they survive restarts that re-register with the
//! node token, and so a re-registration through the same token is not a
//! new use of it.

use super::*;
use crate::manager::capacity_demand::{self, Template, TemplateSpec};
use crate::storage::acl_store::{JoinTokenRecord, JoinTokenUse};
use crate::structs::{
    check_join_attributes, CapacityRefusal, ForgetNodeReply, ForgetNodeRequest,
    GetCapacityDemandReply, GetCapacityDemandRequest, JoinAttrSource, NodeGroupAllocation,
    NodeGroupDemand, NodeGroupMember, PendingCapacity, TemplateRefusal, UnfittableCapacity, JOIN_ATTR, RESERVED_ATTR_PREFIX,
};
use std::collections::BTreeMap;

/// Most templates one `GetCapacityDemand` call sizes.
pub(super) const MAX_DEMAND_TEMPLATES: usize = 64;

/// Whether a key is one the join rules govern: `legion.*`, except
/// `legion.role`, which `pin_registration_attrs` already pins.
fn governed(key: &str) -> bool {
    key.starts_with(RESERVED_ATTR_PREFIX) && key != crate::structs::MANAGER_ROLE_ATTR
}

/// The token id and stamped attributes a node's `legion.join` records.
fn recorded_join(attrs: &Value) -> Option<(String, BTreeMap<String, String>)> {
    let join = attrs.get(JOIN_ATTR)?;
    let token = join.get("token")?.as_str()?.to_string();
    let stamped = join
        .get("attributes")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    Some((token, stamped))
}

impl ManagerDaemonService {
    /// The join token a registration presented, when one is known: the one
    /// the grant verified, or, with ACLs off (or a management caller), the
    /// one its secret resolves to, unexpired. A node-token re-registration
    /// presents none.
    pub(super) fn presented_join_token(
        &self,
        grant: &RegistrationGrant,
        secret: Option<&str>,
    ) -> Option<JoinTokenRecord> {
        match grant {
            RegistrationGrant::JoinToken { record, .. } => Some((**record).clone()),
            RegistrationGrant::Node(_) => None,
            RegistrationGrant::Unrestricted => {
                let secret = secret.map(str::trim).filter(|s| !s.is_empty())?;
                self.store
                    .get_join_token_by_hash(&acl::hash_secret(secret))
                    .ok()
                    .flatten()
                    .filter(|record| !record.is_expired(OffsetDateTime::now_utc()))
            }
        }
    }

    /// The registration's config attributes with the join's applied (the
    /// module docs give the precedence). `host` must already be checked.
    pub(super) fn apply_join_attributes(
        &self,
        grant: &RegistrationGrant,
        token: Option<&JoinTokenRecord>,
        node_id: &str,
        reported: &str,
        host: &HashMap<String, String>,
    ) -> Result<String, Status> {
        let mut attrs: Value = serde_json::from_str(reported).unwrap_or_else(|_| json!({}));
        if !attrs.is_object() {
            attrs = json!({});
        }
        // What vouches for the worker's reported `legion.*` keys.
        let vouched: Option<serde_json::Map<String, Value>> = match grant {
            RegistrationGrant::Unrestricted => None,
            RegistrationGrant::JoinToken { record, .. } => Some(match &record.pinned_attrs {
                Some(raw) => serde_json::from_str(raw).unwrap_or_default(),
                None => self
                    .store
                    .get_worker_type(record.worker_type.clone())
                    .map_err(|e| map_store_error("Error loading worker type", e))?
                    .attributes
                    .into_iter()
                    .map(|(key, value)| (key, Value::String(value)))
                    .collect(),
            }),
            RegistrationGrant::Node(_) => Some(match self.store.get_node(node_id) {
                Ok(existing) => existing
                    .config_attrs
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
                Err(_) => serde_json::Map::new(),
            }),
        };
        let object = attrs.as_object_mut().expect("made an object above");
        if let Some(vouched) = &vouched {
            let reported_keys: Vec<String> =
                object.keys().filter(|key| governed(key)).cloned().collect();
            for key in reported_keys {
                if !vouched.contains_key(&key) {
                    warn!(
                        node_id,
                        attribute = %key,
                        "Registration reported a reserved node attribute nothing vouches for; \
                         dropping it"
                    );
                    object.remove(&key);
                }
            }
            for (key, value) in vouched.iter().filter(|(key, _)| governed(key)) {
                object.insert(key.clone(), value.clone());
            }
        }
        for (key, value) in host {
            object.insert(key.clone(), Value::String(value.clone()));
        }
        match (grant, token) {
            (RegistrationGrant::Node(_), _) => {
                // A restart: what the introducing token stamped still holds.
                if let Some((_, stamped)) = recorded_join(&Value::Object(object.clone())) {
                    for (key, value) in stamped {
                        object.insert(key, Value::String(value));
                    }
                }
            }
            (_, Some(token)) => {
                for (key, value) in &token.attributes {
                    object.insert(key.clone(), Value::String(value.clone()));
                }
                object.insert(
                    JOIN_ATTR.to_string(),
                    json!({ "token": token.id, "attributes": token.attributes }),
                );
            }
            (_, None) => {}
        }
        Ok(attrs.to_string())
    }

    /// Count a registration against its join token's `max_uses`. A node
    /// re-registering under the name this token introduced is not a new
    /// use; a node `ForgetNode` removed is.
    pub(super) fn consume_join_use(
        &self,
        token: &JoinTokenRecord,
        node_id: &str,
    ) -> Result<(), Status> {
        let same_node = self
            .store
            .get_node(node_id)
            .ok()
            .and_then(|node| recorded_join(&node.config_attrs))
            .is_some_and(|(id, _)| id == token.id);
        if same_node {
            return Ok(());
        }
        let refusal = match self
            .store
            .consume_join_token_use(&token.id)
            .map_err(|e| map_store_error("Error counting the join token's use", e))?
        {
            JoinTokenUse::Counted(uses) => {
                debug!(token = %token.id, uses, node_id, "Join token use counted");
                return Ok(());
            }
            JoinTokenUse::Exhausted { uses, max_uses } => Status::permission_denied(format!(
                "join token {} is spent: {uses} of its {max_uses} uses (max_uses) are taken, so \
                 it admits no new node. Hint: mint a new token with `legion join-token create`",
                token.id
            )),
            JoinTokenUse::Missing => {
                Status::unauthenticated("unknown or revoked join token".to_string())
            }
        };
        if self.auth.enabled && self.auth.audit_only {
            warn!(
                target: "acl_audit",
                resource = "worker-plane",
                action = "register",
                enforced = false,
                "join token use refused (audit-only): {}",
                refusal.message()
            );
            return Ok(());
        }
        Err(refusal)
    }

    #[instrument(skip_all, fields(node_id = %request.get_ref().node_id))]
    pub(super) async fn forget_node_rpc(
        &self,
        request: Request<ForgetNodeRequest>,
    ) -> Result<Response<ForgetNodeReply>, Status> {
        // Coarse gate before the node lookup ("check before you fetch"); the
        // pool scope is enforced once the node is loaded.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Write,
            AclScope::any(),
        )?;
        let inner = request.into_inner();
        let idempotency_key = scoped_idempotency_key(
            &identity,
            "forget_node",
            inner.idempotency_key.as_deref(),
            &inner,
        );
        let node_id = inner.node_id.trim().to_string();
        if node_id.is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }
        if let Some(message) = self
            .store
            .idempotency_lookup(&idempotency_key)
            .map_err(|e| map_store_error("Error checking idempotency", e))?
        {
            return Ok(Response::new(ForgetNodeReply {
                message,
                deduplicated: true,
                ..Default::default()
            }));
        }
        let node = self
            .store
            .get_node(&node_id)
            .map_err(|e| map_store_error("Error loading node", e))?;
        acl::enforce(
            &identity,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool(&node.pool),
        )?;

        if !inner.force {
            let allocations = self
                .store
                .allocs_for_node(&node_id)
                .map_err(|e| map_store_error("Error loading the node's allocations", e))?;
            if !allocations.is_empty() {
                let mut jobs: Vec<String> = allocations
                    .iter()
                    .map(|allocation| allocation.job_id.clone())
                    .collect();
                jobs.sort();
                jobs.dedup();
                let shown = jobs.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
                let more = if jobs.len() > 3 {
                    format!(" and {} more", jobs.len() - 3)
                } else {
                    String::new()
                };
                return Err(Status::failed_precondition(format!(
                    "node {node_id} still has {} allocation(s) (jobs {shown}{more}); forgetting \
                     it would drop them. Drain it first (`legion node drain {node_id}`) and \
                     wait until it is empty, or pass --force if the machine is gone for good",
                    allocations.len()
                )));
            }
            if node.status == "active" && node.availability != "drained" {
                return Err(Status::failed_precondition(format!(
                    "node {node_id} is live and schedulable (status active, availability {}); \
                     forgetting it would leave a running worker without an identity. Cordon \
                     or drain it first (`legion node drain {node_id}`), wait until it is \
                     marked lost, or pass --force if the machine is gone for good",
                    node.availability
                )));
            }
            let volumes = self
                .store
                .node_local_volumes_on(&node_id)
                .map_err(|e| map_store_error("Error loading the node's volumes", e))?;
            if volumes > 0 {
                return Err(Status::failed_precondition(format!(
                    "node {node_id} holds {volumes} node-local volume(s), whose data lives on \
                     that machine; their instances could not be placed anywhere else. Replace \
                     those instances first (`legion instance replace`), or pass --force if the \
                     machine and its data are gone for good"
                )));
            }
        }

        let outcome = self
            .store
            .forget_node(&node_id)
            .map_err(|e| map_store_error("Error forgetting node", e))?;
        let message = format!("Forgot node {node_id}");
        info!(
            %node_id,
            force = inner.force,
            allocations_removed = outcome.allocations_removed,
            tokens_revoked = outcome.tokens_revoked,
            snapshots_lost = outcome.snapshots_lost,
            volumes_orphaned = outcome.volumes_orphaned,
            "Node forgotten"
        );
        self.store
            .idempotency_record(&idempotency_key, "forget_node", &message)
            .map_err(|e| map_store_error("Error recording idempotency", e))?;
        let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        Ok(Response::new(ForgetNodeReply {
            message,
            deduplicated: false,
            allocations_removed: count(outcome.allocations_removed),
            tokens_revoked: count(outcome.tokens_revoked),
            volumes_orphaned: count(outcome.volumes_orphaned),
            snapshots_lost: count(outcome.snapshots_lost),
        }))
    }

    #[instrument(skip_all, fields(templates = request.get_ref().templates.len()))]
    pub(super) async fn get_capacity_demand_rpc(
        &self,
        request: Request<GetCapacityDemandRequest>,
    ) -> Result<Response<GetCapacityDemandReply>, Status> {
        // Coarse gate before anything is read; per template pool below.
        let identity = self.check_acl(
            &request,
            AclResource::Node,
            AclAction::Read,
            AclScope::any(),
        )?;
        let req = request.into_inner();
        if req.templates.is_empty() {
            return Err(Status::invalid_argument(
                "name at least one node-group template to size",
            ));
        }
        if req.templates.len() > MAX_DEMAND_TEMPLATES {
            return Err(Status::invalid_argument(format!(
                "{} templates; at most {MAX_DEMAND_TEMPLATES} per call",
                req.templates.len()
            )));
        }
        let mut seen = std::collections::HashSet::new();
        let mut templates = Vec::with_capacity(req.templates.len());
        // Per template: (describe members, simulate fits).
        let mut member_asks: Vec<(bool, bool)> = Vec::with_capacity(req.templates.len());
        for template in req.templates {
            member_asks.push((template.members, template.members && template.fit_check));
            let name = template.name.trim().to_string();
            if name.is_empty() {
                return Err(Status::invalid_argument("every template needs a name"));
            }
            if !seen.insert(name.clone()) {
                return Err(Status::invalid_argument(format!(
                    "two templates are named {name:?}"
                )));
            }
            let shape = template.shape.unwrap_or_default();
            let resources = match shape.resources_json.trim() {
                "" => None,
                raw => Some(
                    serde_json::from_str::<BTreeMap<String, Value>>(raw).map_err(|e| {
                        Status::invalid_argument(format!(
                            "template {name}: shape.resources_json is not a JSON object: {e}"
                        ))
                    })?,
                ),
            };
            let node_shape = crate::autoscaler::nodes::NodeShape {
                cpu: shape.cpu,
                memory: shape.memory,
                cores: shape.cores,
                attributes: (!shape.attributes.is_empty())
                    .then(|| shape.attributes.into_iter().collect()),
                resources,
                max_allocations: shape.max_allocations,
            };
            node_shape
                .validate("shape")
                .map_err(|e| Status::invalid_argument(format!("template {name}: {e}")))?;
            let worker_type_name = match template.worker_type.trim() {
                "" => "default".to_string(),
                other => other.to_string(),
            };
            let worker_type = match self.store.get_worker_type(worker_type_name.clone()) {
                Ok(worker_type) => Some(worker_type),
                Err(err)
                    if matches!(
                        StorageError::from_anyhow(&err),
                        Some(StorageError::NotFound { .. })
                    ) =>
                {
                    None
                }
                Err(err) => return Err(map_store_error("Error loading worker type", err)),
            };
            let like = match template
                .like_node
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                Some(id) => Some(self.store.get_node(id).map_err(|e| {
                    if matches!(
                        StorageError::from_anyhow(&e),
                        Some(StorageError::NotFound { .. })
                    ) {
                        Status::not_found(format!(
                            "template {name}: like_node {id:?} does not exist"
                        ))
                    } else {
                        map_store_error("Error loading node", e)
                    }
                })?),
                None => None,
            };
            let spec = TemplateSpec {
                name: name.clone(),
                pool: template
                    .pool
                    .map(|pool| pool.trim().to_string())
                    .filter(|pool| !pool.is_empty()),
                worker_type,
                shape: node_shape,
                like,
                upcoming: template.upcoming,
            };
            let node = capacity_demand::synthetic_node(&spec);
            acl::enforce(
                &identity,
                AclResource::Node,
                AclAction::Read,
                &AclScope::pool(&node.pool),
            )?;
            templates.push(Template {
                name,
                node,
                upcoming: spec.upcoming,
            });
        }

        let store = self.store.clone();
        let members = {
            let store = store.clone();
            let asks: Vec<(String, bool, bool)> = templates
                .iter()
                .zip(&member_asks)
                .map(|(template, (members, fits))| (template.name.clone(), *members, *fits))
                .collect();
            tokio::task::spawn_blocking(
                move || -> R<Vec<Vec<crate::manager::node_group_members::Member>>> {
                    asks.iter()
                        .map(|(name, members, fits)| {
                            if *members {
                                crate::manager::node_group_members::members(&store, name, *fits)
                            } else {
                                Ok(Vec::new())
                            }
                        })
                        .collect()
                },
            )
            .await
            .map_err(|e| Status::internal(format!("reading node-group members failed: {e}")))?
            .map_err(|e| map_store_error("Error reading node-group members", e))?
        };
        let simulated = {
            let templates = templates.clone();
            tokio::task::spawn_blocking(move || -> R<capacity_demand::DemandReport> {
                let work = store.pending_capacity_work()?;
                let mut nodes = HashMap::new();
                for id in work.tasks.iter().flat_map(|row| row.placed_nodes.iter()) {
                    if nodes.contains_key(id) {
                        continue;
                    }
                    if let Ok(node) = store.get_node(id) {
                        nodes.insert(id.clone(), node);
                    }
                }
                Ok(capacity_demand::simulate(
                    work,
                    &templates,
                    &store.resource_policy,
                    &nodes,
                ))
            })
            .await
            .map_err(|e| Status::internal(format!("capacity simulation failed: {e}")))?
            .map_err(|e| map_store_error("Error reading pending work", e))?
        };

        let readable: Vec<bool> = simulated
            .units
            .iter()
            .map(|unit| {
                identity.allows(
                    AclResource::Job,
                    AclAction::Read,
                    &self.job_read_scope(&unit.job_id),
                )
            })
            .collect();
        let pending = simulated
            .units
            .iter()
            .zip(&readable)
            .zip(&simulated.absorbed)
            .map(|((unit, readable), absorbed)| PendingCapacity {
                job_id: if *readable {
                    unit.job_id.clone()
                } else {
                    String::new()
                },
                namespace: unit.namespace.clone(),
                task_group: if *readable {
                    unit.group.clone()
                } else {
                    String::new()
                },
                tasks: if *readable {
                    unit.tasks.clone()
                } else {
                    Vec::new()
                },
                desired: unit.desired,
                placed: unit.placed,
                pending: unit.pending,
                gang_quorum: unit.gang_quorum,
                resources_json: unit.request_json.to_string(),
                absorbed: *absorbed,
                redacted: !readable,
            })
            .collect();
        let index = |unit: usize| u32::try_from(unit).unwrap_or(u32::MAX);
        let groups = simulated
            .groups
            .iter()
            .zip(&templates)
            .zip(&members)
            .map(|((group, template), members)| NodeGroupDemand {
                name: group.name.clone(),
                nodes: group.nodes,
                replicas: group.replicas,
                absorbed: group.absorbed,
                cpu_requested_mhz: group.cpu_requested_mhz,
                memory_requested_mib: group.memory_requested_mib,
                cpu_capacity_mhz: group.cpu_capacity_mhz,
                memory_capacity_mib: group.memory_capacity_mib,
                refusals: group
                    .refusals
                    .iter()
                    .map(|refusal| CapacityRefusal {
                        pending_index: index(refusal.unit),
                        replicas: refusal.replicas,
                        rule: refusal.rule.as_str().to_string(),
                        detail: refusal.detail.clone(),
                    })
                    .collect(),
                node_pool: template.node.pool.clone(),
                node_attributes_json: template.node.config_attrs.to_string(),
                members: members
                    .iter()
                    .filter(|member| {
                        identity.allows(
                            AclResource::Node,
                            AclAction::Read,
                            &AclScope::pool(&member.node.pool),
                        )
                    })
                    .map(|member| self.member_proto(&identity, member))
                    .collect(),
            })
            .collect();
        let unfittable = simulated
            .unfittable
            .iter()
            .map(|entry| UnfittableCapacity {
                pending_index: index(entry.unit),
                replicas: entry.replicas,
                reasons: entry
                    .reasons
                    .iter()
                    .map(|(template, rule, detail)| TemplateRefusal {
                        template: template.clone(),
                        rule: rule.as_str().to_string(),
                        detail: detail.clone(),
                    })
                    .collect(),
            })
            .collect();
        let mut awaiting_scheduler = Vec::new();
        let mut awaiting_scheduler_redacted = 0;
        for job in &simulated.awaiting_scheduler {
            if identity.allows(AclResource::Job, AclAction::Read, &self.job_read_scope(job)) {
                awaiting_scheduler.push(job.clone());
            } else {
                awaiting_scheduler_redacted += 1;
            }
        }
        Ok(Response::new(GetCapacityDemandReply {
            groups,
            pending,
            unfittable,
            awaiting_scheduler,
            awaiting_scheduler_redacted,
            truncated: simulated.truncated,
            computed_at: OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        }))
    }

    /// One node-group member as `GetCapacityDemand` reports it: job names
    /// only where the caller has `job:read`.
    fn member_proto(
        &self,
        identity: &AclIdentity,
        member: &crate::manager::node_group_members::Member,
    ) -> NodeGroupMember {
        NodeGroupMember {
            node_id: member.node.id.clone(),
            status: member.node.status.clone(),
            availability: member.node.availability.clone(),
            provider_id: member.provider_id.clone().unwrap_or_default(),
            scale_in: member.scale_in.clone().unwrap_or_default(),
            reserved_cpu: member.reserved_cpu,
            reserved_memory: member.reserved_memory,
            allocations: member
                .allocations
                .iter()
                .map(|allocation| {
                    let readable = identity.allows(
                        AclResource::Job,
                        AclAction::Read,
                        &self.job_read_scope(&allocation.job_id),
                    );
                    NodeGroupAllocation {
                        allocation_id: allocation.id,
                        job_id: if readable {
                            allocation.job_id.clone()
                        } else {
                            String::new()
                        },
                        namespace: allocation.namespace.clone(),
                        task_group: if readable {
                            allocation.group.clone()
                        } else {
                            String::new()
                        },
                        task_name: if readable {
                            allocation.task.clone()
                        } else {
                            String::new()
                        },
                        kind: allocation.kind.to_string(),
                        sessions: allocation.sessions,
                        node_local_storage: allocation.node_local_storage,
                        breaks_min_available: allocation.min_available.is_some(),
                        min_available_detail: if readable {
                            allocation.min_available.clone().unwrap_or_default()
                        } else {
                            String::new()
                        },
                        redacted: !readable,
                    }
                })
                .collect(),
            fits_elsewhere: member.fits.as_ref().map(Result::is_ok),
            fit_detail: match &member.fits {
                Some(Ok(nodes)) if nodes.is_empty() => "nothing to move".to_string(),
                Some(Ok(nodes)) => format!("fits on {}", nodes.join(", ")),
                Some(Err(why)) => why.clone(),
                None => String::new(),
            },
        }
    }

    /// `CreateJoinToken`'s `max_uses` and `attributes`, checked.
    pub(super) fn join_token_limits(
        max_uses: Option<u32>,
        attributes: HashMap<String, String>,
    ) -> Result<(Option<u32>, BTreeMap<String, String>), Status> {
        if max_uses == Some(0) {
            return Err(Status::invalid_argument(
                "max_uses must be at least 1; leave it unset for a token without a limit",
            ));
        }
        let attributes: BTreeMap<String, String> = attributes.into_iter().collect();
        check_join_attributes(&attributes, JoinAttrSource::Token)
            .map_err(Status::invalid_argument)?;
        Ok((max_uses, attributes))
    }
}
