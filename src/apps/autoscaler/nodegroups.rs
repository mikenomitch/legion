//! Node groups, run by the autoscaler app (docs/autoscaling.md, "Node
//! groups"; autoscaler-v2 plan §6.11, calls entries 12 and 16).
//!
//! Every tick, for every node-group policy this replica owns:
//!
//! 1. **The provider.** The target plugin's `target/status`: the count it
//!    holds and its instances (after checking it declares the `nodes`
//!    capability, and that the policy's config matches its schema).
//! 2. **Legion's side, in one read.** One `GetCapacityDemand` for every
//!    group: the nodes the pending work needs from each template (net of
//!    the nodes on their way), and each group's members with what scale-in
//!    needs (allocations, protections, reserved share, the fits-elsewhere
//!    simulation for `underutilized`, the `disruption.minAvailable` check).
//! 3. **The expander** picks the one group that grows this tick.
//! 4. **The engine** ([`crate::autoscaler::nodes::decide`]) decides: the
//!    count to hold, the victims, and each drain's next step.
//! 5. **An action intent first.** Before any of it touches the provider or
//!    a node, the app opens an intent on the policy's action row
//!    (`RecordScalingAction`, compare-and-set on its version). Only the
//!    winner acts: mints the group's join token (`max_uses` = the nodes
//!    asked for, stamping `legion.nodegroup`), calls `target/scale` with it,
//!    cordons and drains victims (`SetNodeAvailability`), calls
//!    `target/terminate` once a victim is empty, and forgets it
//!    (`ForgetNode`) once the provider no longer lists its instance. Then
//!    it closes the intent with the outcome. A replica that loses reads the
//!    winner's intent instead, and an intent a crashed replica left open is
//!    closed (`reconciled`) once it is older than a target call can take.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::host::{self, Problem};
use super::{hash_of, plain_report, Engine, TickReport};
use crate::autoscaler::nodes::{
    decide, expand, AllocationKind, AllocationView, Drain, DrainObservation, EffectiveNodeGroup,
    Expander, ExpanderOption, GroupDemand, NodeDecision, NodeFlag, NodeGroupObservation,
    NodeGroupState, NodeIntent, NodeStage, NodeView, ProviderStatus, ScaleInWhen,
};
use crate::autoscaler::DecisionKind;
use crate::prelude::*;
use crate::structs::{
    CreateJoinTokenRequest, ForgetNodeRequest, GetCapacityDemandRequest, NodeGroupDemand,
    NodeGroupMember, NodeGroupShape, NodeGroupTemplate, RecordScalingActionRequest, ScaleDecision,
    ScalingAction, ScalingActionIntent, ScalingActionNode, ScalingEventFlag, ScalingTargetReport,
    SetNodeAvailabilityRequest, StandaloneScalingPolicy,
};

/// An intent another replica opened is left alone this long, by this
/// replica's clock from when it first saw it (the longest a target call may
/// take, plus a tick), before it counts as abandoned and is reconciled.
pub const RECONCILE_AFTER_MS: i64 = 150_000;

/// Provider member states that mean the instance is gone or going.
const GONE: [&str; 5] = [
    "terminated",
    "terminating",
    "shutting-down",
    "stopped",
    "stopping",
];

/// What the app keeps per node group between ticks.
#[derive(Debug, Default)]
pub(super) struct GroupRuntime {
    /// The pure engine's memory.
    state: NodeGroupState,
    /// The action row version this replica last saw.
    action_version: u64,
    /// An intent another replica left open, and when this replica first
    /// saw it (its own clock).
    open: Option<(ScalingAction, i64)>,
    /// The last full decision reported (its digest).
    reported: Option<u64>,
    /// The last dry-run "would" recorded.
    last_would: Option<String>,
    /// A `nodegroup.pending` episode is under way.
    pending_flagged: bool,
    /// Why the policy was invalid at the last evaluation.
    invalid: Option<String>,
    /// A ready member to copy as the template next tick.
    like_node: Option<String>,
    /// Members at the last read.
    joined: u32,
}

impl GroupRuntime {
    /// Take in the replicated action row (from the policy stream, or a
    /// lost compare-and-set): its version, and the drains it carries.
    fn adopt(&mut self, action: &ScalingAction, me: &str) {
        if action.version <= self.action_version {
            return;
        }
        self.action_version = action.version;
        if let Some(intent) = &action.intent {
            if let Ok(drains) = serde_json::from_str::<Vec<Drain>>(&intent.drains_json) {
                self.state.replace_drains(drains);
            }
        }
        self.open = (action.state == "open" && action.owner != me).then(|| (action.clone(), 0));
    }
}

/// The member states the provider reports, as the engine reads them.
fn provider_of(status: &host::TargetState) -> ProviderStatus {
    let instances = status
        .members
        .iter()
        .filter(|member| {
            member
                .get("state")
                .and_then(Value::as_str)
                .is_none_or(|state| !GONE.contains(&state.to_ascii_lowercase().as_str()))
        })
        .filter_map(|member| member.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    ProviderStatus {
        desired: status.count,
        running: status.ready,
        instances,
    }
}

/// Whether the provider listed its instances (so a provider id can be
/// checked against them): it listed some, or it holds none.
fn lists_instances(status: &host::TargetState) -> bool {
    !status.members.is_empty() || status.count == 0
}

/// A member as the engine sees it, with the provider-id cross-check: a
/// member whose `legion.provider-id` the provider does not list, or that
/// shares it with another member, or that has none, is never a victim (its
/// terminate would stop the wrong machine, or none).
fn node_view(
    member: &NodeGroupMember,
    provider: Option<(&ProviderStatus, bool)>,
    duplicates: &BTreeSet<String>,
) -> NodeView {
    let provider_id = (!member.provider_id.is_empty()).then(|| member.provider_id.clone());
    let problem = match (&provider_id, provider) {
        (None, _) => Some("it joined without legion.provider-id, so it cannot be terminated".to_string()),
        (Some(id), _) if duplicates.contains(id) => Some(format!(
            "another member also claims legion.provider-id {id}"
        )),
        (Some(id), Some((status, true))) if !status.instances.contains(id) => Some(format!(
            "the provider does not list its instance {id}"
        )),
        _ => None,
    };
    NodeView {
        id: member.node_id.clone(),
        provider_id,
        ready: member.status == "active",
        cordoned: member.availability != "available",
        scale_in: (!member.scale_in.is_empty()).then(|| member.scale_in.clone()),
        allocations: member
            .allocations
            .iter()
            .map(|allocation| AllocationView {
                id: allocation.allocation_id,
                namespace: allocation.namespace.clone(),
                job: allocation.job_id.clone(),
                task: allocation.task_name.clone(),
                kind: match allocation.kind.as_str() {
                    "session-envelope" => AllocationKind::SessionEnvelope {
                        sessions: allocation.sessions,
                    },
                    "per-node" => AllocationKind::PerNode,
                    _ => AllocationKind::Work,
                },
                node_local_storage: allocation.node_local_storage,
                breaks_min_available: allocation.breaks_min_available,
            })
            .collect(),
        reserved_cpu: member.reserved_cpu,
        reserved_memory: member.reserved_memory,
        provider_problem: problem,
    }
}

fn template_of(policy: &EffectiveNodeGroup, runtime: &GroupRuntime) -> NodeGroupTemplate {
    let shape = &policy.shape;
    let upcoming = runtime
        .state
        .pending_up()
        .map_or(0, |pending| pending.expect.saturating_sub(runtime.joined));
    NodeGroupTemplate {
        name: policy.name.clone(),
        worker_type: policy.worker_type.clone(),
        pool: policy.pool.clone(),
        shape: Some(NodeGroupShape {
            cpu: shape.cpu,
            memory: shape.memory,
            cores: shape.cores,
            attributes: shape
                .attributes
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect(),
            resources_json: shape
                .resources
                .as_ref()
                .map(|resources| serde_json::to_string(resources).unwrap_or_default())
                .unwrap_or_default(),
            max_allocations: shape.max_allocations,
        }),
        like_node: runtime.like_node.clone(),
        upcoming,
        members: true,
        fit_check: policy.down.when == ScaleInWhen::Underutilized,
    }
}

fn group_demand(demand: &NodeGroupDemand) -> GroupDemand {
    GroupDemand {
        nodes: demand.nodes,
        replicas: demand.replicas,
        cpu_requested_mhz: demand.cpu_requested_mhz,
        memory_requested_mib: demand.memory_requested_mib,
        cpu_capacity_mhz: demand.cpu_capacity_mhz,
        memory_capacity_mib: demand.memory_capacity_mib,
    }
}

/// The intent a decision carries out, with the drains it leaves in flight.
fn intent_of(decision: &NodeDecision, decision_id: &str, state: &NodeGroupState) -> ScalingActionIntent {
    let mut intent = ScalingActionIntent {
        decision_id: decision_id.to_string(),
        from: decision.from,
        summary: decision.summary.clone(),
        drains_json: serde_json::to_string(&state.drains().filter(|d| !d.finished()).collect::<Vec<_>>())
            .unwrap_or_else(|_| "[]".to_string()),
        ..Default::default()
    };
    let provider = |node: &str| {
        state
            .drains()
            .find(|drain| drain.node == node)
            .and_then(|drain| drain.provider_id.clone())
            .unwrap_or_default()
    };
    for action in &decision.intents {
        match action {
            NodeIntent::SetDesired { count } => intent.desired = Some(*count),
            NodeIntent::Cordon { node } => intent.cordon.push(ScalingActionNode {
                node: node.clone(),
                provider_id: provider(node),
            }),
            NodeIntent::Uncordon { node } => intent.uncordon.push(node.clone()),
            NodeIntent::Terminate { node, provider_id } => {
                intent.terminate.push(ScalingActionNode {
                    node: node.clone(),
                    provider_id: provider_id.clone().unwrap_or_default(),
                })
            }
            NodeIntent::Forget { node } => intent.forget.push(node.clone()),
        }
    }
    intent
}

/// The state word a node-group decision reports.
fn state_word(decision: &NodeDecision, state: &NodeGroupState, now_ms: i64) -> &'static str {
    if decision
        .stage(NodeStage::Pause)
        .is_some_and(|note| note.note.starts_with("paused"))
    {
        return "paused";
    }
    match decision.kind {
        DecisionKind::Would => return "dry-run",
        DecisionKind::Applied if decision.to > decision.from => return "scaled",
        _ => {}
    }
    if !decision.victims.is_empty() || state.drains().any(|drain| !drain.finished()) {
        return "draining";
    }
    if decision
        .stage(NodeStage::Bounds)
        .is_some_and(|note| note.note.starts_with("clamped"))
    {
        return "limited";
    }
    if state.backoff_until(now_ms).is_some() {
        return "held";
    }
    "ok"
}

fn flag_event(flag: &NodeFlag) -> ScalingEventFlag {
    match flag {
        NodeFlag::JoinTimeout {
            missing,
            backoff_until_ms,
            ..
        } => ScalingEventFlag {
            kind: "join_timeout".to_string(),
            message: format!(
                "{missing} node(s) asked for did not join in time; backing off until {}",
                crate::cmd::scaling::clock(*backoff_until_ms)
            ),
            outcome: String::new(),
        },
        NodeFlag::ScaleIn {
            node,
            outcome,
            detail,
        } => ScalingEventFlag {
            kind: "scale_in".to_string(),
            message: format!("{node}: {detail}"),
            outcome: outcome.clone(),
        },
        NodeFlag::Limited { reason } => ScalingEventFlag {
            kind: "limited".to_string(),
            message: reason.clone(),
            outcome: String::new(),
        },
    }
}

impl Engine {
    /// Evaluate every node group this replica owns. Returns one report per
    /// group.
    pub(super) async fn evaluate_node_groups(
        &mut self,
        groups: &[(&StandaloneScalingPolicy, EffectiveNodeGroup)],
        now_ms: i64,
        report: &mut TickReport,
    ) -> Vec<ScalingTargetReport> {
        let live: BTreeSet<&str> = groups.iter().map(|(policy, _)| policy.id.as_str()).collect();
        self.groups.retain(|id, _| live.contains(id.as_str()));
        let mut out = Vec::new();
        if groups.is_empty() {
            return out;
        }
        let me = self.options.instance.clone();
        let host = self.host.clone();

        // 1. The provider, and the plugin's fitness for the job.
        struct Prepared<'a> {
            policy: &'a StandaloneScalingPolicy,
            effective: &'a EffectiveNodeGroup,
            status: Option<host::TargetState>,
            status_error: Option<String>,
        }
        let mut prepared: Vec<Prepared<'_>> = Vec::new();
        for (policy, effective) in groups {
            let runtime = self.groups.entry(policy.id.clone()).or_default();
            if let Some(action) = &policy.action {
                runtime.adopt(action, &me);
            }
            let base = node_report(policy);
            match host.check_node_target(&effective.plugin, &effective.config).await {
                Err(Problem::Invalid(err)) => {
                    let mut held = plain_report(base, "invalid", 0, &format!("target: {err}"), now_ms);
                    if runtime.invalid.as_deref() != Some(err.as_str()) {
                        held.events.push(ScalingEventFlag {
                            kind: "check_failed".to_string(),
                            message: format!("target: {err}"),
                            outcome: String::new(),
                        });
                        runtime.invalid = Some(err);
                    }
                    out.push(held);
                    continue;
                }
                Err(Problem::Unavailable(err)) => {
                    report.errors.push(format!("{}: {err}", policy.id));
                    out.push(plain_report(base, "error", 0, &format!("target: {err}"), now_ms));
                    continue;
                }
                Ok(()) => runtime.invalid = None,
            }
            let (status, status_error) = match host
                .status(&effective.plugin, &policy.id, &effective.config, 0)
                .await
            {
                Ok(status) => (Some(status), None),
                Err(err) => {
                    report.errors.push(format!("{}: {err}", policy.id));
                    (None, Some(err))
                }
            };
            prepared.push(Prepared {
                policy,
                effective,
                status,
                status_error,
            });
        }
        if prepared.is_empty() {
            return out;
        }

        // 2. Demand and members, every group in one read.
        let templates: Vec<NodeGroupTemplate> = prepared
            .iter()
            .map(|p| template_of(p.effective, &self.groups[&p.policy.id]))
            .collect();
        let demand = match self
            .client
            .get_capacity_demand(GetCapacityDemandRequest { templates })
            .await
        {
            Ok(reply) => reply.into_inner(),
            Err(status) => {
                let why = format!("reading capacity demand: {}", status.message());
                report.errors.push(why.clone());
                for p in &prepared {
                    out.push(plain_report(node_report(p.policy), "error", 0, &why, now_ms));
                }
                return out;
            }
        };

        // 3. The expander: one group grows per tick.
        let options: Vec<ExpanderOption> = prepared
            .iter()
            .zip(&demand.groups)
            .map(|(p, group)| {
                let runtime = &self.groups[&p.policy.id];
                let held = p
                    .status
                    .as_ref()
                    .map_or(runtime.joined, |status| status.count);
                let blocked = if p.policy.pause.is_some() {
                    Some("paused".to_string())
                } else if held >= p.effective.max {
                    Some(format!("at max {}", p.effective.max))
                } else {
                    runtime
                        .state
                        .backoff_until(now_ms)
                        .map(|until| format!(
                            "backing off after a join timeout until {}",
                            crate::cmd::scaling::clock(until)
                        ))
                };
                ExpanderOption {
                    group: p.effective.name.clone(),
                    demand: group_demand(group),
                    blocked,
                }
            })
            .collect();
        let choice = expand(Expander::LeastWaste, &options);

        // 4. Each group.
        for (p, group) in prepared.iter().zip(demand.groups) {
            let target_report = self
                .evaluate_node_group(p.policy, p.effective, p.status.as_ref(), p.status_error.as_deref(), &group, &choice.chosen, &choice.summary, now_ms, report)
                .await;
            out.push(target_report);
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    async fn evaluate_node_group(
        &mut self,
        policy: &StandaloneScalingPolicy,
        effective: &EffectiveNodeGroup,
        status: Option<&host::TargetState>,
        status_error: Option<&str>,
        group: &NodeGroupDemand,
        chosen: &Option<String>,
        expander_note: &str,
        now_ms: i64,
        report: &mut TickReport,
    ) -> ScalingTargetReport {
        let me = self.options.instance.clone();
        let mut base = node_report(policy);
        let provider = status.map(provider_of);
        let listed = status.is_some_and(lists_instances);
        let mut ids: HashMap<&str, u32> = HashMap::new();
        for member in &group.members {
            if !member.provider_id.is_empty() {
                *ids.entry(member.provider_id.as_str()).or_default() += 1;
            }
        }
        let duplicates: BTreeSet<String> = ids
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(id, _)| id.to_string())
            .collect();
        let nodes: Vec<NodeView> = group
            .members
            .iter()
            .map(|member| node_view(member, provider.as_ref().map(|p| (p, listed)), &duplicates))
            .collect();
        let fits: HashMap<String, bool> = group
            .members
            .iter()
            .filter_map(|member| member.fits_elsewhere.map(|fits| (member.node_id.clone(), fits)))
            .collect();
        let runtime = self.groups.entry(policy.id.clone()).or_default();
        runtime.joined = group.members.len() as u32;
        runtime.like_node = group
            .members
            .iter()
            .find(|member| member.status == "active" && member.availability == "available")
            .map(|member| member.node_id.clone());

        // An intent another replica left open: wait for it, or reconcile
        // it from the provider's status once it is older than any call.
        if let Some((open, seen)) = runtime.open.clone() {
            let seen = if seen == 0 { now_ms } else { seen };
            runtime.open = Some((open.clone(), seen));
            if now_ms - seen < RECONCILE_AFTER_MS {
                let summary = format!(
                    "waiting for {}'s action {} to finish",
                    open.owner,
                    open.intent.as_ref().map(|i| i.decision_id.as_str()).unwrap_or("-")
                );
                return plain_report(base, "held", provider.as_ref().map_or(runtime.joined, |p| p.desired), &summary, now_ms);
            }
            let seen = match (&provider, open.intent.as_ref().and_then(|i| i.desired)) {
                (Some(provider), Some(desired)) => format!(
                    "target/status holds {} (the intent asked for {desired})",
                    provider.desired
                ),
                (Some(provider), None) => format!("target/status holds {}", provider.desired),
                (None, _) => "target/status did not answer".to_string(),
            };
            let closed = self
                .client
                .record_scaling_action(RecordScalingActionRequest {
                    kind: "outcome".to_string(),
                    policy: policy.id.clone(),
                    expected_action_version: open.version,
                    outcome: "reconciled".to_string(),
                    detail: format!("left open by {}; {seen}", open.owner),
                    instance: me.clone(),
                    ..Default::default()
                })
                .await;
            let runtime = self.groups.get_mut(&policy.id).expect("inserted above");
            match closed {
                Ok(reply) => {
                    let reply = reply.into_inner();
                    if let Some(action) = &reply.action {
                        runtime.action_version = action.version;
                    }
                    runtime.open = None;
                }
                Err(status) => report
                    .errors
                    .push(format!("{}: reconciling an open intent: {}", policy.id, status.message())),
            }
        }

        let runtime = self.groups.get_mut(&policy.id).expect("inserted above");
        let pause = policy.pause.as_ref().map(super::pause_of);
        let obs = NodeGroupObservation {
            joined: runtime.joined,
            provider: provider.clone(),
            demand: Some(group_demand(group)),
            chosen: chosen.as_deref() == Some(effective.name.as_str()),
            expander_note: expander_note.to_string(),
            nodes,
            drains: BTreeMap::new(),
            pause,
        };
        let mut effective = effective.clone();
        if let Some(mode) = self.options.force_mode {
            effective.mode = mode;
        }
        let before = runtime.state.clone();
        let mut obs = obs;
        // A drain's instance is gone once the provider (when it lists its
        // instances) no longer does.
        for drain in runtime.state.drains() {
            let member = group.members.iter().find(|m| m.node_id == drain.node);
            let terminated = listed
                && drain
                    .provider_id
                    .as_ref()
                    .is_some_and(|id| provider.as_ref().is_some_and(|p| !p.instances.contains(id)));
            obs.drains.insert(
                drain.node.clone(),
                DrainObservation {
                    exists: member.is_some(),
                    cordoned: member.is_some_and(|m| m.availability != "available"),
                    occupied_by: member.map_or(0, |m| {
                        nodes_occupying(m)
                    }),
                    terminated,
                },
            );
        }
        let decision = decide(&effective, &mut runtime.state, &obs, now_ms, &|node: &NodeView| {
            fits.get(&node.id).copied().unwrap_or(false)
        });
        let decision_id = super::decision_id(&super::standalone_key(&policy.id), now_ms);
        let mut events: Vec<ScalingEventFlag> = decision.flags.iter().map(flag_event).collect();
        if group.nodes > 0 && !runtime.pending_flagged {
            runtime.pending_flagged = true;
            events.push(ScalingEventFlag {
                kind: "pending".to_string(),
                message: format!(
                    "pending work needs {} node(s) of {} ({} replica(s))",
                    group.nodes, effective.name, group.replicas
                ),
                outcome: String::new(),
            });
        } else if group.nodes == 0 {
            runtime.pending_flagged = false;
        }
        let scale_decision = ScaleDecision {
            id: decision_id.clone(),
            summary: decision.summary.clone(),
            policy_digest: policy.digest.clone(),
            policy_json: policy.policy_json.clone(),
            detail_json: serde_json::to_string(&decision).unwrap_or_default(),
            evaluated_at_unix_ms: now_ms,
        };
        let mut summary = decision.summary.clone();
        let mut word = state_word(&decision, &runtime.state, now_ms).to_string();
        if let Some(err) = status_error {
            summary = format!("{summary}; target/status failed: {err}");
        }
        // Intents come from a new decision (a scale-up, a victim's drain)
        // and from drains already in flight, which move on (terminate,
        // forget, uncordon) whatever this evaluation decided.
        if !decision.intents.is_empty() {
            match self
                .act_node_group(policy, &effective, &decision, &scale_decision, before, now_ms, report)
                .await
            {
                Ok(note) => {
                    if let Some(note) = note {
                        summary = format!("{summary}; {note}");
                    }
                }
                Err((state, why)) => {
                    word = state.to_string();
                    summary = why;
                }
            }
        }
        match decision.kind {
            DecisionKind::Would => {
                let key = format!(
                    "{}>{}:{}",
                    decision.from,
                    decision.to,
                    decision.victims.iter().map(|v| v.node.as_str()).collect::<Vec<_>>().join(",")
                );
                let runtime = self.groups.get_mut(&policy.id).expect("inserted above");
                if runtime.last_would.as_deref() != Some(key.as_str()) {
                    let recorded = self
                        .client
                        .record_scaling_action(RecordScalingActionRequest {
                            kind: "would".to_string(),
                            from: decision.from,
                            to: decision.to,
                            decision: Some(scale_decision.clone()),
                            policy: policy.id.clone(),
                            instance: me.clone(),
                            ..Default::default()
                        })
                        .await;
                    match recorded {
                        Ok(_) => {
                            if let Some(runtime) = self.groups.get_mut(&policy.id) {
                                runtime.last_would = Some(key);
                            }
                            report.would += 1;
                        }
                        Err(status) => report.errors.push(format!(
                            "recording a dry-run decision: {}",
                            status.message()
                        )),
                    }
                }
            }
            _ => {
                if let Some(runtime) = self.groups.get_mut(&policy.id) {
                    if decision.from == decision.to && decision.victims.is_empty() {
                        runtime.last_would = None;
                    }
                }
            }
        }
        base.state = word;
        base.current = decision.from;
        base.want = decision.to;
        base.evaluated_at_unix_ms = now_ms;
        base.summary = summary;
        base.events = events;
        let runtime = self.groups.get_mut(&policy.id).expect("inserted above");
        let mut stable = decision.clone();
        stable.at_ms = 0;
        let digest = hash_of(&serde_json::to_string(&stable).unwrap_or_default());
        base.decision_json = if runtime.reported == Some(digest) {
            String::new()
        } else {
            runtime.reported = Some(digest);
            serde_json::to_string(&decision).unwrap_or_default()
        };
        base
    }

    /// Open the intent, and when it wins, carry the decision out and close
    /// it. `Ok(note)` on success (a note for the summary); `Err((state,
    /// summary))` when nothing was done.
    #[allow(clippy::too_many_arguments)]
    async fn act_node_group(
        &mut self,
        policy: &StandaloneScalingPolicy,
        effective: &EffectiveNodeGroup,
        decision: &NodeDecision,
        scale_decision: &ScaleDecision,
        before: NodeGroupState,
        now_ms: i64,
        report: &mut TickReport,
    ) -> Result<Option<String>, (&'static str, String)> {
        let me = self.options.instance.clone();
        let runtime = self.groups.get_mut(&policy.id).expect("evaluated above");
        let intent = intent_of(decision, &scale_decision.id, &runtime.state);
        let expected = runtime.action_version;
        let opened = self
            .client
            .record_scaling_action(RecordScalingActionRequest {
                kind: "intent".to_string(),
                policy: policy.id.clone(),
                expected_action_version: expected,
                intent: Some(intent.clone()),
                decision: Some(scale_decision.clone()),
                instance: me.clone(),
                ..Default::default()
            })
            .await;
        let runtime = self.groups.get_mut(&policy.id).expect("evaluated above");
        let won = match opened {
            Err(status) => {
                // Paused in between, or the manager is unreachable: none of
                // it happened.
                runtime.state = before;
                let held = status.code() == tonic::Code::FailedPrecondition;
                let why = format!("intent refused: {}", status.message());
                report.errors.push(format!("{}: {why}", policy.id));
                return Err((if held { "held" } else { "error" }, why));
            }
            Ok(reply) => reply.into_inner(),
        };
        if !won.won {
            // Another replica acted on this decision (overlapping leases):
            // take its intent as ours, and act on nothing.
            runtime.state = before;
            runtime.action_version = 0;
            if let Some(action) = &won.action {
                runtime.adopt(action, &me);
            }
            let owner = won.action.as_ref().map(|a| a.owner.clone()).unwrap_or_default();
            return Err(("held", format!("{owner} acted on this group first; following its intent")));
        }
        if let Some(action) = &won.action {
            runtime.action_version = action.version;
        }
        let host = self.host.clone();
        let mut failures: Vec<String> = Vec::new();
        let mut scaled = false;
        for action in &decision.intents {
            match action {
                NodeIntent::SetDesired { count } => {
                    let new_nodes = count.saturating_sub(decision.from).max(1);
                    let token = match self.mint_join_token(effective, new_nodes, &scale_decision.id).await {
                        Ok(token) => token,
                        Err(err) => {
                            failures.push(err);
                            continue;
                        }
                    };
                    let join = json!({
                        "token": token,
                        "attributes": effective.member_attributes(),
                    });
                    match host
                        .scale_nodes(&effective.plugin, &policy.id, &effective.config, *count, &scale_decision.id, join)
                        .await
                    {
                        Ok(()) => {
                            if let Some(runtime) = self.groups.get_mut(&policy.id) {
                                runtime.state.record_scale(now_ms, decision.from, *count);
                            }
                            scaled = true;
                            report.scaled += 1;
                        }
                        Err(err) => failures.push(format!("target/scale to {count}: {err}")),
                    }
                }
                NodeIntent::Cordon { node } => {
                    if let Err(status) = self
                        .client
                        .set_node_availability(SetNodeAvailabilityRequest {
                            node_id: node.clone(),
                            availability: "drained".to_string(),
                            evict: true,
                            idempotency_key: None,
                        })
                        .await
                    {
                        failures.push(format!("draining {node}: {}", status.message()));
                    }
                }
                NodeIntent::Uncordon { node } => {
                    if let Err(status) = self
                        .client
                        .set_node_availability(SetNodeAvailabilityRequest {
                            node_id: node.clone(),
                            availability: "available".to_string(),
                            evict: false,
                            idempotency_key: None,
                        })
                        .await
                    {
                        if status.code() != tonic::Code::NotFound {
                            failures.push(format!("uncordoning {node}: {}", status.message()));
                        }
                    }
                }
                NodeIntent::Terminate { node, provider_id } => {
                    let Some(id) = provider_id else {
                        failures.push(format!("{node} has no legion.provider-id to terminate"));
                        continue;
                    };
                    if let Err(err) = host
                        .terminate(&effective.plugin, &policy.id, &effective.config, std::slice::from_ref(id), &scale_decision.id)
                        .await
                    {
                        failures.push(format!("target/terminate {id} ({node}): {err}"));
                    }
                }
                NodeIntent::Forget { node } => {
                    if let Err(status) = self
                        .client
                        .forget_node(ForgetNodeRequest {
                            node_id: node.clone(),
                            force: false,
                            idempotency_key: Some(format!("{}-forget-{node}", scale_decision.id)),
                        })
                        .await
                    {
                        if status.code() != tonic::Code::NotFound {
                            failures.push(format!("forgetting {node}: {}", status.message()));
                        }
                    }
                }
            }
        }
        let version = self.groups.get(&policy.id).map_or(0, |r| r.action_version);
        let closed = self
            .client
            .record_scaling_action(RecordScalingActionRequest {
                kind: "outcome".to_string(),
                policy: policy.id.clone(),
                expected_action_version: version,
                outcome: if failures.is_empty() { "succeeded" } else { "failed" }.to_string(),
                detail: failures.join("; "),
                instance: me,
                ..Default::default()
            })
            .await;
        match closed {
            Ok(reply) => {
                if let (Some(action), Some(runtime)) =
                    (reply.into_inner().action, self.groups.get_mut(&policy.id))
                {
                    runtime.action_version = action.version;
                }
            }
            Err(status) => report.errors.push(format!(
                "{}: closing the intent: {}",
                policy.id,
                status.message()
            )),
        }
        if failures.is_empty() {
            Ok(None)
        } else {
            report.errors.extend(failures.iter().map(|f| format!("{}: {f}", policy.id)));
            if scaled {
                Ok(Some(failures.join("; ")))
            } else {
                Err(("error", failures.join("; ")))
            }
        }
    }

    /// A join token for `count` new members of the group: `max_uses`
    /// `count`, stamping `legion.nodegroup` (and the shape's attributes),
    /// expiring after twice the join timeout.
    async fn mint_join_token(
        &mut self,
        policy: &EffectiveNodeGroup,
        count: u32,
        decision: &str,
    ) -> Result<String, String> {
        let ttl = crate::autoscaler::spec::format_duration(policy.up.join_timeout * 2);
        let reply = self
            .client
            .create_join_token(CreateJoinTokenRequest {
                worker_type: policy.worker_type.clone(),
                ttl: Some(ttl),
                description: format!("node group {} scale-up {decision}", policy.name),
                max_uses: Some(count.max(1)),
                attributes: policy.member_attributes().into_iter().collect(),
            })
            .await
            .map_err(|status| format!("minting the group's join token: {}", status.message()))?;
        Ok(reply.into_inner().token)
    }
}

/// Allocations that keep a member occupied.
fn nodes_occupying(member: &NodeGroupMember) -> u32 {
    member
        .allocations
        .iter()
        .filter(|allocation| match allocation.kind.as_str() {
            "session-envelope" => allocation.sessions > 0,
            "per-node" => false,
            _ => true,
        })
        .count() as u32
}

/// A report naming a node group.
fn node_report(policy: &StandaloneScalingPolicy) -> ScalingTargetReport {
    ScalingTargetReport {
        policy: policy.id.clone(),
        policy_digest: policy.digest.clone(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::NodeGroupAllocation;

    fn status(count: u32, members: Value) -> host::TargetState {
        host::TargetState {
            count,
            ready: count,
            members: members.as_array().cloned().unwrap_or_default(),
        }
    }

    fn member(id: &str, provider: &str) -> NodeGroupMember {
        NodeGroupMember {
            node_id: id.to_string(),
            status: "active".to_string(),
            availability: "available".to_string(),
            provider_id: provider.to_string(),
            ..Default::default()
        }
    }

    /// Terminated instances are not the provider's; a provider that lists
    /// none (with a count) cannot vouch for ids.
    #[test]
    fn the_provider_vouches_for_the_instances_it_lists_test() {
        let listed = status(
            3,
            json!([{ "id": "i-1", "state": "running" }, { "id": "i-2", "state": "terminated" },
                   { "id": "i-3" }]),
        );
        let provider = provider_of(&listed);
        assert_eq!(provider.desired, 3);
        assert_eq!(provider.instances, vec!["i-1".to_string(), "i-3".to_string()]);
        assert!(lists_instances(&listed));
        assert!(!lists_instances(&status(2, json!([]))));
        assert!(lists_instances(&status(0, json!([]))));

        let duplicates: BTreeSet<String> = ["i-3".to_string()].into_iter().collect();
        let view = |m: &NodeGroupMember, listed: bool| {
            node_view(m, Some((&provider, listed)), &duplicates)
        };
        assert_eq!(view(&member("w-1", "i-1"), true).provider_problem, None);
        assert!(view(&member("w-2", "i-2"), true)
            .provider_problem
            .unwrap()
            .contains("does not list its instance i-2"));
        assert!(view(&member("w-3", "i-3"), true)
            .provider_problem
            .unwrap()
            .contains("another member also claims"));
        assert!(view(&member("w-4", ""), true)
            .provider_problem
            .unwrap()
            .contains("without legion.provider-id"));
        // Unlisted, the id is taken on trust.
        assert_eq!(view(&member("w-2", "i-2"), false).provider_problem, None);
    }

    #[test]
    fn members_become_node_views_test() {
        let mut m = member("w-1", "i-1");
        m.availability = "drained".to_string();
        m.reserved_cpu = 0.25;
        m.allocations = vec![
            NodeGroupAllocation {
                allocation_id: 7,
                job_id: "legion-system/autoscaler".to_string(),
                namespace: "legion-system".to_string(),
                kind: "work".to_string(),
                ..Default::default()
            },
            NodeGroupAllocation {
                allocation_id: 8,
                kind: "session-envelope".to_string(),
                sessions: 0,
                ..Default::default()
            },
        ];
        let view = node_view(&m, None, &BTreeSet::new());
        assert!(view.cordoned && view.ready);
        assert_eq!(view.reserved_cpu, 0.25);
        assert_eq!(
            view.allocations[1].kind,
            AllocationKind::SessionEnvelope { sessions: 0 }
        );
        assert!(matches!(
            view.protection(),
            Some(crate::autoscaler::nodes::Protection::SystemAllocation { .. })
        ));
        assert_eq!(nodes_occupying(&m), 1);
    }

    #[test]
    fn an_intent_names_every_action_and_the_drains_left_test() {
        let decision: NodeDecision = serde_json::from_value(json!({
            "atMs": 1, "from": 2, "to": 4, "kind": "applied", "stages": [], "victims": [],
            "protected": [],
            "intents": [
                { "intent": "set-desired", "count": 4 },
                { "intent": "terminate", "node": "w-1", "provider_id": "i-1" },
                { "intent": "forget", "node": "w-0" },
                { "intent": "uncordon", "node": "w-9" },
            ],
            "flags": [], "summary": "2 → 4",
        }))
        .unwrap();
        let mut state = NodeGroupState::default();
        state.adopt_drain(Drain {
            node: "w-1".into(),
            provider_id: Some("i-1".into()),
            started_ms: 0,
            phase: crate::autoscaler::nodes::DrainPhase::Terminating { asked_at_ms: 1 },
        });
        let intent = intent_of(&decision, "d-9", &state);
        assert_eq!(intent.desired, Some(4));
        assert_eq!(intent.from, 2);
        assert_eq!(intent.terminate[0].provider_id, "i-1");
        assert_eq!(intent.forget, vec!["w-0".to_string()]);
        assert_eq!(intent.uncordon, vec!["w-9".to_string()]);
        let drains: Vec<Drain> = serde_json::from_str(&intent.drains_json).unwrap();
        assert_eq!(drains.len(), 1);
        assert_eq!(state_word(&decision, &state, 0), "scaled");
        let flag = flag_event(&NodeFlag::ScaleIn {
            node: "w-1".into(),
            outcome: "aborted".into(),
            detail: "uncordoned".into(),
        });
        assert_eq!((flag.kind.as_str(), flag.outcome.as_str()), ("scale_in", "aborted"));
    }
}
