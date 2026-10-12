//! Manager-side edge boot: minting boot tickets and ratifying claims
//! (docs/edge-boot.md).
//!
//! **Ratification is not a new consistency mechanism.** It is the existing
//! plan apply with the node already chosen: a plan submission with one
//! placement and one named instance, applied through
//! `apply_plan_submission_apply` with its epoch compare-and-set, its
//! constraint re-validation on the fresh node row, and — critically — its
//! `assign_stateful_instance_tx` call. If another allocation already owns
//! `user-42`, that apply fails exactly the way it fails today, and *that
//! failure is the refusal*. No new invariant, no second code path for
//! correctness, and no weakening of the at-most-one guarantee.
//!
//! Two starts racing for the same instance therefore resolve the same way two
//! concurrent plans resolve today: the store serializes them, the first binds,
//! the second is told no.

use crate::manager::workload_identity::WorkloadIdentityState;
use crate::prelude::*;
use crate::storage::boot_ticket_store::BootTicketRecord;
use crate::storage::store::{PlanApplyOutcome, Store};
use crate::structs::edge_boot::{
    BootTicketClaims, Gate, RefusalReason, CLAIM_GATE, CLAIM_INSTANCE, CLAIM_INSTANCE_PREFIX,
    CLAIM_JOB, CLAIM_JOB_CONFIG, CLAIM_POOL, CLAIM_SCOPE, CLAIM_TASK, CLAIM_USES,
};
use crate::structs::{EdgeBootConfigFull, TaskConfig};
use serde_json::{json, Map};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A ratification verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Confirmed {
        allocation_id: i64,
    },
    Refused {
        reason: RefusalReason,
        detail: String,
    },
}

impl Verdict {
    fn refuse(reason: RefusalReason, detail: impl Into<String>) -> Self {
        Verdict::Refused {
            reason,
            detail: detail.into(),
        }
    }
}

/// What a node is asking the manager to ratify.
#[derive(Debug, Clone)]
pub struct ClaimRatification<'a> {
    pub node_id: &'a str,
    pub claim_id: &'a str,
    pub job_config_id: &'a str,
    pub task_name: &'a str,
    /// Empty for stateless tasks.
    pub instance_id: &'a str,
    pub gate: Gate,
}

/// The ticket a ratification redeems: its `jti` and the time the spend is
/// judged against.
#[derive(Debug, Clone)]
pub struct TicketSpend {
    pub jti: String,
    pub now_unix: i64,
}

/// Ratify a claim by committing its placement.
///
/// Refusals are classified rather than returned as errors: the node needs a
/// machine-readable reason to decide between killing the work and retrying
/// inside its lease, and an on-call responder needs to tell "id taken" from
/// "no capacity" without reading manager logs.
///
/// **One scheduler-lane entry** (sandbox-scaling plan, Phase 1): the ticket
/// spend, the instance row when the ticket names an id that has never
/// existed, the one-placement plan apply, and the `alloc.ratified` event
/// all commit together, group-committed with other instance writes. They
/// used to be four entries on two lanes, and the spend came first, so a
/// ratification refused for capacity still burned a single-use ticket and
/// the node's retry inside its lease could only fail. Now a refusal commits
/// nothing and the ticket stays redeemable. The status-lane `task_status`
/// projection follows the entry, as it follows every plan apply.
#[instrument(skip_all, fields(claim_id = claim.claim_id, node_id = claim.node_id))]
pub fn ratify(
    store: &Store,
    claim: &ClaimRatification<'_>,
    spend: Option<TicketSpend>,
    started: std::time::Instant,
) -> R<Verdict> {
    let task = store
        .get_task_configs_for_job_config(claim.job_config_id)?
        .into_iter()
        .find(|(_, task)| task.name == claim.task_name);
    let Some((task_config_id, task)) = task else {
        return Ok(Verdict::refuse(
            RefusalReason::UnknownTarget,
            format!(
                "job config {} has no task {}",
                claim.job_config_id, claim.task_name
            ),
        ));
    };
    let job_id = store
        .job_id_for_job_config(claim.job_config_id)?
        .unwrap_or_default();

    // A ticket may name an instance that does not exist yet: the edge start
    // is then the create. Only a keyed task's id that has NEVER existed
    // qualifies — a tombstone is a row, so a destroyed id is refused by the
    // bind below exactly as before, and ids are never recycled.
    let create = if claim.instance_id.is_empty()
        || store.get_instance(claim.instance_id)?.is_some()
        || task
            .stateful
            .as_ref()
            .is_none_or(|stateful| stateful.mode != crate::structs::InstanceMode::Keyed)
    {
        None
    } else {
        if let Err(err) = crate::structs::instance::validate_keyed_instance_id(claim.instance_id) {
            return Ok(Verdict::refuse(
                RefusalReason::UnknownTarget,
                format!("instance id {:?} is invalid: {err}", claim.instance_id),
            ));
        }
        Some((job_id.clone(), task.group.clone(), task.name.clone()))
    };

    // Sampled immediately before the submit, exactly as the scheduler does:
    // the apply refuses the plan if any of these epochs moved, which is what
    // makes two ratifications for one instance serialize instead of
    // double-binding.
    let expected_versions =
        store.allocation_version_vector_for_candidates(claim.job_config_id, &[claim.node_id])?;
    let placements = vec![(claim.node_id.to_string(), task_config_id)];

    let job_config_id = claim.job_config_id.to_string();
    let instance_id = claim.instance_id.to_string();
    let node_id = claim.node_id.to_string();
    let claim_id = claim.claim_id.to_string();
    let gate = claim.gate.as_str().to_string();
    let fused = store.scheduler_group_commit("ratify_claim", move |s| {
        s.within_savepoint(|s| {
            if let Some(spend) = &spend {
                if !s.spend_boot_ticket(&spend.jti, spend.now_unix)? {
                    return Ok(Err((
                        RefusalReason::TicketInvalid,
                        format!(
                            "boot ticket {} has no redemptions left (single-use tickets are \
                             spent by the first ratification, even if it has since exited)",
                            spend.jti
                        ),
                    )));
                }
            }
            if let Some((job_id, group, task_name)) = &create {
                s.create_keyed_instance(job_id, group, task_name, Some(&instance_id))?;
            }
            let (_, outcome) = s.submit_and_apply_plan_for_instance(
                &job_config_id,
                "reschedule",
                &placements,
                &expected_versions,
                &[],
                &instance_id,
            )?;
            let allocation_id = match &outcome {
                PlanApplyOutcome::Applied(applied) => match applied.allocations.first() {
                    Some(allocation) => allocation.id,
                    // An applied plan that placed nothing means the placement
                    // was silently dropped, which the apply path does not do;
                    // treat it as internal rather than pretending it
                    // succeeded.
                    None => {
                        return Ok(Err((
                            RefusalReason::Internal,
                            "plan applied without producing an allocation".to_string(),
                        )))
                    }
                },
                PlanApplyOutcome::Rejected { reason, .. } => {
                    return Ok(Err((classify(reason), reason.clone())));
                }
            };
            s.events().provisional_ratified(
                &claim_id,
                allocation_id,
                &node_id,
                &job_id,
                &instance_id,
                &gate,
                started.elapsed().as_millis() as u64,
            );
            Ok(Ok((allocation_id, outcome)))
        })
    });

    match fused {
        Ok(Ok((allocation_id, outcome))) => {
            store.plan_apply_bookkeeping(&outcome)?;
            Ok(Verdict::Confirmed { allocation_id })
        }
        Ok(Err((reason, detail))) => Ok(Verdict::refuse(reason, detail)),
        Err(err) => {
            let detail = format!("{err:#}");
            Ok(Verdict::refuse(classify(&detail), detail))
        }
    }
}

/// Turn a plan-apply rejection into the reason code a node acts on.
///
/// The strings come from `apply_plan_submission_apply` and
/// `assign_keyed_instance_tx`; matching on them keeps the refusal vocabulary
/// in one place instead of threading a typed error through the whole plan
/// path for the benefit of one caller.
fn classify(reason: &str) -> RefusalReason {
    let lowered = reason.to_ascii_lowercase();
    if lowered.contains("not available to bind") || lowered.contains("no unbound keyed instance") {
        RefusalReason::InstanceClaimed
    } else if lowered.contains("no longer has capacity") {
        RefusalReason::NoCapacity
    } else if lowered.contains("does not satisfy constraints")
        || lowered.contains("cordoned, draining, or lost")
    {
        RefusalReason::Ineligible
    } else if lowered.contains("is stale") {
        // The epoch compare-and-set lost to a competing plan. For a claim
        // racing on one instance this is the same fact as "someone else got
        // there first", but the node may still be able to win a retry inside
        // its lease, so it is reported as retryable.
        RefusalReason::Internal
    } else {
        RefusalReason::Internal
    }
}

/// Everything a mint needs, already validated.
#[derive(Debug, Clone)]
pub struct TicketRequest {
    pub job: String,
    pub task: String,
    pub job_config_id: String,
    pub instance_id: String,
    pub instance_prefix: String,
    pub pool: String,
    pub gate: Gate,
    pub ttl: Duration,
    pub uses: u32,
    pub accessor_id: String,
}

/// A minted ticket: the token (returned once) plus the row that records it.
pub struct MintedTicket {
    pub token: String,
    pub record: BootTicketRecord,
}

/// Validate a mint request against the cluster's edge-boot settings and the
/// task it names.
///
/// The checks that matter are the narrowing ones. A ticket that names neither
/// an instance nor a prefix would authorize every sandbox of the task; a
/// ticket whose gate exceeds the task's own `speculativeStart.mode` would
/// promise a ceiling the node will not honor, so it is refused at mint rather
/// than silently downgraded at redemption — the operator finds out now, not
/// from a latency graph.
pub fn validate_ticket_request(
    request: &TicketRequest,
    task: &TaskConfig,
    config: &EdgeBootConfigFull,
) -> R<Duration> {
    if request.job.trim().is_empty() || request.task.trim().is_empty() {
        bail!("a boot ticket must name a job and a task");
    }
    if request.gate == Gate::Off {
        bail!("a boot ticket with gate `off` authorizes nothing");
    }
    if request.instance_id.trim().is_empty() && request.instance_prefix.trim().is_empty() {
        bail!(
            "a boot ticket must name an instance or a non-empty instance prefix; \
             an unbounded ticket would authorize every instance of {}",
            request.task
        );
    }
    if !request.instance_id.trim().is_empty() && !request.instance_prefix.trim().is_empty() {
        bail!("a boot ticket names an instance or an instance prefix, not both");
    }
    if request.pool.trim().is_empty() {
        bail!("a boot ticket must name the node pool it may be redeemed in");
    }
    if request.uses == 0 {
        bail!("a boot ticket must allow at least one use");
    }

    let task_mode = task
        .speculative_start
        .as_ref()
        .map(|block| block.resolve())
        .transpose()?
        .map(|resolved| resolved.mode)
        .unwrap_or(Gate::Off);
    if task_mode == Gate::Off {
        bail!(
            "task {} does not declare speculativeStart, so no node will honor a boot ticket \
             for it",
            request.task
        );
    }
    if request.gate > task_mode {
        bail!(
            "boot ticket gate `{}` exceeds task {}'s speculativeStart.mode `{}`; the lower \
             always wins, so this ticket would not do what it says",
            request.gate,
            request.task,
            task_mode
        );
    }

    let max_ttl = humantime::parse_duration(config.max_ticket_ttl.trim())
        .context("invalid edgeBoot.maxTicketTtl")?;
    if request.ttl > max_ttl {
        bail!(
            "requested ticket TTL {} exceeds edgeBoot.maxTicketTtl {}",
            humantime::format_duration(request.ttl),
            humantime::format_duration(max_ttl)
        );
    }
    if request.ttl.is_zero() {
        bail!("a boot ticket TTL must be greater than zero");
    }
    Ok(request.ttl)
}

/// Mint and sign a ticket. The caller records [`MintedTicket::record`].
pub fn mint(
    identity: &WorkloadIdentityState,
    request: &TicketRequest,
    jti: &str,
    now: SystemTime,
) -> R<MintedTicket> {
    let iat = now
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the unix epoch")?
        .as_secs();
    let exp = iat + request.ttl.as_secs();

    let mut claims: Map<String, Value> = Map::new();
    claims.insert(CLAIM_SCOPE.into(), json!("boot"));
    claims.insert(CLAIM_JOB.into(), json!(request.job));
    claims.insert(CLAIM_JOB_CONFIG.into(), json!(request.job_config_id));
    claims.insert(CLAIM_TASK.into(), json!(request.task));
    if !request.instance_id.is_empty() {
        claims.insert(CLAIM_INSTANCE.into(), json!(request.instance_id));
    } else {
        claims.insert(CLAIM_INSTANCE_PREFIX.into(), json!(request.instance_prefix));
    }
    claims.insert(CLAIM_POOL.into(), json!(request.pool));
    claims.insert(CLAIM_GATE.into(), json!(request.gate.as_str()));
    claims.insert(CLAIM_USES.into(), json!(request.uses));

    let subject = format!("boot:{}/{}", request.job, request.task);
    let issued = identity.issue_boot_ticket(jti, &subject, claims, iat, exp)?;

    Ok(MintedTicket {
        token: issued.token,
        record: BootTicketRecord {
            jti: jti.to_string(),
            job: request.job.clone(),
            task: request.task.clone(),
            job_config_id: request.job_config_id.clone(),
            instance_id: request.instance_id.clone(),
            instance_prefix: request.instance_prefix.clone(),
            pool: request.pool.clone(),
            gate: request.gate.as_str().to_string(),
            uses: request.uses,
            uses_spent: 0,
            expires_at_unix: issued.expires_at as i64,
            accessor_id: request.accessor_id.clone(),
            created_at: Store::boot_ticket_now()?,
        },
    })
}

/// Re-verify a ticket presented with a ratification: its signature, its
/// lifetime, and that it authorizes this instance in this node's pool.
///
/// The node already verified this ticket offline; the manager verifies it
/// again because nodes are honest but are not trusted to be the *only*
/// holder. The spend — where globally-single-use is enforced, since the
/// node-local check can only stop the same ticket being presented twice to
/// the same node — happens inside the ratification's own entry
/// ([`ratify`]), so a refused ratification does not burn the ticket.
pub fn verify_ticket(
    identity: &WorkloadIdentityState,
    ticket: &str,
    instance_id: &str,
    node_pool: &str,
) -> R<BootTicketClaims> {
    let claims = identity.verify(ticket, crate::structs::edge_boot::BOOT_TICKET_AUDIENCE)?;
    let claims = BootTicketClaims::from_claims(&claims)?;
    claims.authorizes(instance_id, node_pool)?;
    Ok(claims)
}

#[cfg(test)]
mod edge_boot_manager_test {
    use super::*;
    use crate::structs::edge_boot::SpeculativeStartConfig;

    fn task(mode: Option<&str>) -> TaskConfig {
        TaskConfig {
            speculative_start: mode.map(|mode| SpeculativeStartConfig {
                mode: Some(mode.into()),
                ..Default::default()
            }),
            ..TaskConfig::new("workspace", "sandbox")
        }
    }

    fn request() -> TicketRequest {
        TicketRequest {
            job: "agent-sandboxes".into(),
            task: "workspace".into(),
            job_config_id: "jc-1".into(),
            instance_id: "user-42".into(),
            instance_prefix: String::new(),
            pool: "edge-eu".into(),
            gate: Gate::Boot,
            ttl: Duration::from_secs(300),
            uses: 1,
            accessor_id: "acc-1".into(),
        }
    }

    #[test]
    fn a_well_formed_request_validates() -> R<()> {
        validate_ticket_request(
            &request(),
            &task(Some("boot")),
            &EdgeBootConfigFull::default(),
        )?;
        Ok(())
    }

    #[test]
    fn a_ticket_must_narrow_to_an_instance_or_a_prefix() {
        let mut req = request();
        req.instance_id = String::new();
        let err =
            validate_ticket_request(&req, &task(Some("boot")), &EdgeBootConfigFull::default())
                .expect_err("unbounded");
        assert!(err.to_string().contains("every instance"), "{err}");

        let mut both = request();
        both.instance_prefix = "user-".into();
        assert!(validate_ticket_request(
            &both,
            &task(Some("boot")),
            &EdgeBootConfigFull::default()
        )
        .is_err());
    }

    #[test]
    fn a_ticket_cannot_exceed_the_tasks_own_mode() {
        // The lower gate always wins, so minting a ticket the task will not
        // honor is a mistake worth surfacing at mint rather than as an
        // unexplained latency regression later.
        let mut req = request();
        req.gate = Gate::Serve;
        let err =
            validate_ticket_request(&req, &task(Some("boot")), &EdgeBootConfigFull::default())
                .expect_err("gate too high");
        assert!(err.to_string().contains("lower always wins"), "{err}");
    }

    #[test]
    fn a_task_without_speculative_start_cannot_be_ticketed() {
        let err = validate_ticket_request(&request(), &task(None), &EdgeBootConfigFull::default())
            .expect_err("no speculativeStart");
        assert!(err.to_string().contains("speculativeStart"), "{err}");
    }

    #[test]
    fn ttl_is_clamped_by_the_cluster_maximum() {
        let mut req = request();
        req.ttl = Duration::from_secs(7200);
        assert!(
            validate_ticket_request(&req, &task(Some("boot")), &EdgeBootConfigFull::default())
                .is_err()
        );
    }

    #[test]
    fn a_lost_race_is_classified_as_instance_claimed() {
        // The refusal an on-call responder must be able to tell apart from a
        // capacity problem.
        assert_eq!(
            classify("keyed instance user-42 is not available to bind (already bound ...)"),
            RefusalReason::InstanceClaimed
        );
        assert_eq!(
            classify("no unbound keyed instance for task workspace"),
            RefusalReason::InstanceClaimed
        );
        assert_eq!(
            classify("Node node-a no longer has capacity for task 7"),
            RefusalReason::NoCapacity
        );
        assert_eq!(
            classify("Node node-a does not satisfy constraints for task 7"),
            RefusalReason::Ineligible
        );
        assert_eq!(
            classify(
                "Node node-a is draining (cordoned, draining, or lost since the plan snapshot)"
            ),
            RefusalReason::Ineligible
        );
        // A stale epoch is retryable inside the lease, not a final answer.
        assert!(
            classify("plan epoch for job config jc-1 is stale; expected 2, latest 3").retryable()
        );
    }
}
