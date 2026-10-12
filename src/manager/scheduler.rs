// Scheduler planning entry points pass many parameters (nodes, policy, caches,
// epochs, …) by design; bundling them into structs would not improve clarity.
#![allow(clippy::too_many_arguments)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;

use libloading::Library;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::constraints::{AffinitySet, ConstraintSet, DistinctTracker, PermitSet};
use crate::prelude::*;
use crate::resources::{ResourcePolicy, ResourcePool, ResourceRequest, ResourceValue};
use crate::scheduling_index::{IndexMode, IndexQueryOutcome};
pub use crate::storage::content_index::{ContentRef, LocalityScores};
use crate::storage::errors::StorageError;
use crate::storage::store::{
    AllocationRef, AvailableNodes, JobAllocationsByNode, PlanApplyOutcome, PlanSubmissionPlacement,
    ScopeEpochs, SharedNodeAllocations, SharedNodePools, Store,
};
use crate::structs::{
    Allocation, Band, JobConfig, Node, TaskConfig, TaskDependencyCondition, TaskLifecycle,
    TaskStatusState,
};
use policy::score_node_for_context;
pub use policy::{
    load_policy, SchedulerWeights, SchedulingContext, SchedulingPolicy,
    DEF_SCHEDULER_LOCALITY_WEIGHT,
};
use preemption::{eligible_victims, preemption_cost, select_victims, VictimCandidate};
pub use preemption::{
    PlannedPreemption, PreemptionMode, PreemptionSettings, DEFAULT_VICTIM_EXEMPT_AGE,
};

mod policy;
mod preemption;

const RESCHEDULE_SCORE_JITTER_THRESHOLD: f64 = 0.01;

#[derive(Debug, Clone)]
pub struct PlannedAllocation {
    pub node_id: String,
    pub task_config_id: i64,
    /// Ranked candidate nodes for this placement, best first;
    /// `candidates[0]` is always `node_id`. The apply walk binds the first
    /// candidate whose node epoch is unchanged since planning. Length 1
    /// means no fallback — the epoch compare-and-set is exact, as it was
    /// before candidate lists (docs/late-binding-plan-apply.md).
    pub candidates: Vec<String>,
    /// The keyed instance this placement is for, when the plan chose the
    /// node for one instance in particular (a restore, whose `compat` and
    /// pin are that instance's). The apply binds exactly this instance, so
    /// two restores placed in one plan cannot swap nodes. `None` keeps the
    /// ordinary "any unbound instance" binding.
    pub instance_id: Option<String>,
}

/// Replicas a plan wanted to place but could not, because no node had room
/// for them (capacity, a static port, a `distinct` rule).
///
/// A plan places what fits and reports the rest here instead of failing as
/// a whole: one replica that cannot fit no longer keeps every other replica
/// of the job from running (docs/scheduler-and-eval-loop.md, "Partial
/// placement"). The remainder is placed by the next pass that has room — a
/// node joining or being uncordoned queues one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnplacedTask {
    pub task_config_id: i64,
    pub task_name: String,
    /// How many replicas of the task this plan left unplaced.
    pub count: u32,
    /// Why, in the same words a plan that placed nothing fails with.
    pub reason: String,
    /// The static port that kept the task off every node it is otherwise
    /// allowed on, when a port was the cause.
    pub static_port: Option<u16>,
}

/// The error a plan returns when it could place nothing at all because no
/// node has room for any of its placements. Typed so callers can tell a
/// placement that is blocked (retry when the cluster changes) from a broken
/// job, and so a rollout can see that a static port is what blocks it.
#[derive(Debug, Clone)]
pub struct PlacementBlocked {
    pub unplaced: Vec<UnplacedTask>,
}

impl PlacementBlocked {
    /// The static port that blocked the plan, if any task was blocked by one.
    pub fn static_port(&self) -> Option<u16> {
        self.unplaced.iter().find_map(|task| task.static_port)
    }
}

impl std::fmt::Display for PlacementBlocked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut reasons = self.unplaced.iter().map(|task| task.reason.as_str());
        match reasons.next() {
            Some(first) => formatter.write_str(first)?,
            None => formatter.write_str("No placements could be made")?,
        }
        for reason in reasons {
            write!(formatter, "; {reason}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PlacementBlocked {}

/// How the planner judges a task's `dependsOn` conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencyGate {
    /// Real placement. A `complete` or `success` dependency is met only by
    /// the status a worker reported for the dependency's task: the task is
    /// held (not placed) until its dependency has actually exited, and the
    /// job is re-evaluated when it does (`Store::queue_dependency_progress`).
    Live,
    /// A dry run (`job plan`). A batch dependency planned in the same pass
    /// counts as met, so the preview shows where every task of the job would
    /// eventually run rather than only the first step of a DAG.
    Preview,
}

/// The tasks a pass may place, and the ones still waiting on a dependency.
struct ReadyTasks {
    ready: Vec<(i64, TaskConfig, u32)>,
    held: Vec<String>,
}

/// Parsed `schedulerSettings.plan_candidates`: how many ranked candidates
/// each placement carries into the apply walk, and the score band the
/// fallback tail is sampled from. `count: 0` disables candidate lists and
/// restores single-node submissions (the mixed-version kill switch).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanCandidatesSetting {
    pub count: usize,
    pub band: f64,
}

impl PlanCandidatesSetting {
    pub const DEFAULT_COUNT: usize = 8;
    pub const MAX_COUNT: usize = 32;

    fn disabled(&self) -> bool {
        self.count == 0
    }
}

impl Default for PlanCandidatesSetting {
    fn default() -> Self {
        PlanCandidatesSetting {
            count: Self::DEFAULT_COUNT,
            band: RESCHEDULE_SCORE_JITTER_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LedgerDelta {
    pub node_id: String,
    pub added: Vec<Allocation>,
    pub removed: Vec<Allocation>,
}

#[derive(Debug, Clone)]
pub enum PlanMode {
    Fresh,
    Reschedule,
}

impl PlanMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Reschedule => "reschedule",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlanOutcome {
    pub allocations: Vec<Allocation>,
    pub deltas: Vec<LedgerDelta>,
    /// Allocations this plan evicted, committed in the same transaction as
    /// `allocations`. Empty on every plan that did not preempt.
    pub evictions: Vec<crate::storage::store::PlanEviction>,
    /// Where the plan's wall-clock went, phase by phase (µs). Logged on
    /// completion, observed into `legion_scheduler_plan_phase_seconds`,
    /// and aggregated by the benchmark runners
    /// (docs/testing-and-benchmarks.md).
    pub timings: PlanTimings,
    /// Replicas this plan wanted but could not place (see
    /// [`UnplacedTask`]). Empty when everything it wanted fit.
    pub unplaced: Vec<UnplacedTask>,
    /// Tasks this pass did not consider because a `dependsOn` condition is
    /// not met yet (a batch dependency that has not exited). They are placed
    /// by the eval queued when the dependency reaches a terminal state.
    pub held_tasks: Vec<String>,
}

impl PlanOutcome {
    /// A plan that placed and evicted nothing.
    fn empty(timings: PlanTimings) -> Self {
        Self {
            allocations: Vec::new(),
            deltas: Vec::new(),
            evictions: Vec::new(),
            timings,
            unplaced: Vec::new(),
            held_tasks: Vec::new(),
        }
    }

    /// One-line summary of what the plan left undone, for eval outcomes:
    /// empty when it placed everything it was asked to.
    pub fn shortfall_summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.unplaced.is_empty() {
            let count: u32 = self.unplaced.iter().map(|task| task.count).sum();
            let reasons = self
                .unplaced
                .iter()
                .map(|task| task.reason.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            parts.push(format!("{count} unplaced ({reasons})"));
        }
        if !self.held_tasks.is_empty() {
            parts.push(format!(
                "waiting on dependencies: {}",
                self.held_tasks.join(", ")
            ));
        }
        parts.join("; ")
    }
}

/// Per-phase wall-clock breakdown of one `plan_and_apply`, in microseconds.
/// Phases are disjoint; `total_us` covers the whole call, so `total_us -
/// sum(phases)` is glue (mode bookkeeping, logging) and should stay small.
/// In execution order: `job_lookup`, `available_nodes`, `task_fetch`,
/// `capacity_snapshot`, `build_plan`, `version_sample`,
/// `speculative_dispatch`, `apply`, `post_apply`.
#[derive(Debug, Clone, Default)]
pub struct PlanTimings {
    /// The plan's job lookups: the preemptor band and, for a `Fresh`
    /// plan, the job whose allocations it replaces.
    pub job_lookup_us: u64,
    /// Node set fetch (`Store::available_nodes`): an `Arc` clone, or a
    /// rebuild after a node change.
    pub available_nodes_us: u64,
    /// Optimistic-concurrency epoch sample over candidate nodes.
    pub version_sample_us: u64,
    /// Task-config load (cached configs + per-plan task list).
    pub task_fetch_us: u64,
    /// Capacity snapshot (allocations by node + free pools).
    pub capacity_snapshot_us: u64,
    /// Candidate construction, scoring, and placement (`build_plan`).
    pub build_plan_us: u64,
    /// `submit_and_apply_plan`: submission insert, epoch compare-and-set,
    /// and allocation writes in one replicated op — on raft backends this
    /// includes propose→quorum-commit→apply.
    pub apply_us: u64,
    /// Speculative dispatch of opted-in tasks before the apply
    /// (docs/edge-boot.md). Near zero when no task opts in.
    pub speculative_dispatch_us: u64,
    /// After the apply: ratification dispatch and the ledger deltas.
    pub post_apply_us: u64,
    /// Whole `plan_and_apply` call.
    pub total_us: u64,
}

impl PlanTimings {
    /// Observe every phase into the
    /// `legion_scheduler_plan_phase_seconds{phase}` histogram family.
    fn observe(&self) {
        let metrics = crate::manager::telemetry::metrics();
        for (phase, us) in [
            ("job_lookup", self.job_lookup_us),
            ("available_nodes", self.available_nodes_us),
            ("version_sample", self.version_sample_us),
            ("task_fetch", self.task_fetch_us),
            ("capacity_snapshot", self.capacity_snapshot_us),
            ("build_plan", self.build_plan_us),
            ("speculative_dispatch", self.speculative_dispatch_us),
            ("apply", self.apply_us),
            ("post_apply", self.post_apply_us),
            ("total", self.total_us),
        ] {
            metrics
                .scheduler_plan_phase_seconds
                .observe(&[phase], us as f64 / 1_000_000.0);
        }
    }
}

/// How candidate construction went for one planned task — the scheduling
/// index's explain record, surfaced by `job plan` (docs/scheduling-indexes.md).
#[derive(Debug, Clone)]
pub struct TaskIndexUsage {
    pub task_name: String,
    /// The index that served candidates, `None` when the scan ran instead.
    pub index: Option<String>,
    /// `"on"` or `"shadow"` when an index participated.
    pub mode: Option<&'static str>,
    /// Candidate count handed to placement.
    pub candidates: usize,
    /// True when the candidate list was cut to the budget (fit-best-first).
    pub truncated: bool,
    /// Times placement exhausted truncated candidates and widened to a scan.
    pub refills: u32,
    /// True when the scan had to serve after the index came up short.
    pub fell_back: bool,
    /// Shadow mode only: did the index's candidate set match the scan's?
    pub shadow_agreement: Option<bool>,
    /// Why no index served, when one was expected to (`None` otherwise).
    pub reason: Option<String>,
    /// `Some(sample)` when bounded candidate scoring
    /// (`schedulerSettings.score_sample`) engaged for any of this task's
    /// placements: the placement was chosen as the best of `sample`
    /// uniformly drawn eligible nodes instead of exact scoring over all of
    /// them. `None` = every placement scored exactly.
    pub score_sampled: Option<usize>,
}

impl TaskIndexUsage {
    fn scan(task_name: &str) -> Self {
        TaskIndexUsage {
            task_name: task_name.to_string(),
            index: None,
            mode: None,
            candidates: 0,
            truncated: false,
            refills: 0,
            fell_back: false,
            shadow_agreement: None,
            reason: None,
            score_sampled: None,
        }
    }
}

/// `build_plan`'s result: the placements plus the per-task index report.
struct BuiltPlan {
    placements: Vec<PlannedAllocation>,
    index_report: Vec<TaskIndexUsage>,
    /// Allocations this plan will evict to make room for `placements`.
    /// Committed in the same transaction as the placements, never
    /// separately — that atomicity is the design's central claim.
    evictions: Vec<PlannedPreemption>,
    /// Victims `observe` mode identified and did not take. Never committed;
    /// reported through `allocation.preempt_planned` and `job plan`.
    observed_preemptions: Vec<PlannedPreemption>,
    /// Replicas no node had room for (only when the plan allows partial
    /// placement; otherwise the first such replica fails the plan).
    unplaced: Vec<UnplacedTask>,
}

/// The planned config's own allocations, indexed by node: every node that
/// holds at least one allocation of the config being planned, with the
/// task config id of each. Built ONCE per plan from the capacity snapshot
/// — in the same pass that drops a Fresh plan's replaced rows — and
/// consulted by every step that used to walk the whole cluster's
/// allocations for the same answer: required counts, group pinning,
/// `distinct` seeding, and the scoring cache's per-node task presence.
/// Small by construction (one job's allocations), where each of the walks
/// it replaces was O(every allocation in the cluster) per plan and so
/// scaled with cluster fill rather than with the job being placed.
type OwnAllocationsByNode = HashMap<String, Vec<i64>>;

/// Which allocation rows a plan drops from its capacity snapshot before
/// planning, so they are not counted against node capacity.
#[derive(Debug, Clone, Copy)]
enum SnapshotDrop<'a> {
    /// None — a Reschedule plan counts everything that is running.
    Nothing,
    /// Every allocation of a job: a Fresh plan replaces the job wholesale
    /// (its handler already dropped the rows, or drops them the moment
    /// the plan applies), so counting them would demand headroom for two
    /// copies of the job.
    Job(&'a str),
    /// Every allocation of one config: the read-only preview of the same.
    JobConfig(&'a str),
}

impl SnapshotDrop<'_> {
    /// Whether the rule drops this row.
    fn matches(&self, allocation: &Allocation) -> bool {
        match self {
            SnapshotDrop::Nothing => false,
            SnapshotDrop::Job(job_id) => allocation.job_id == *job_id,
            SnapshotDrop::JobConfig(job_config_id) => allocation.job_config_id == *job_config_id,
        }
    }

    /// [`Self::matches`] for an entry of `job_id`'s job-index entry, which
    /// carries no job id of its own.
    fn matches_ref(&self, job_id: &str, reference: &AllocationRef) -> bool {
        match self {
            SnapshotDrop::Nothing => false,
            SnapshotDrop::Job(dropped) => *dropped == job_id,
            SnapshotDrop::JobConfig(job_config_id) => reference.job_config_id == *job_config_id,
        }
    }
}

/// One plan's capacity state, taken from the read model in a single lock
/// acquisition: the allocations the plan places against, the cached free
/// pools, and the nodes whose allocation set the plan itself changed by
/// dropping the job's own allocations (a Fresh plan replaces them) — the
/// ledger recomputes exactly those nodes from scratch.
struct CapacitySnapshot {
    allocations_by_node: SharedNodeAllocations,
    pools: SharedNodePools,
    freed_nodes: HashSet<String>,
    /// The scope-epoch mirror as of this snapshot, captured under the same
    /// read guard as the allocations and pools. The plan-apply fence
    /// samples the affected scopes out of it after planning
    /// (`Store::expected_versions_from_snapshot`).
    scope_epochs: ScopeEpochs,
    /// The planned config's surviving allocations, by node (see
    /// [`OwnAllocationsByNode`]).
    own_by_node: OwnAllocationsByNode,
    /// Host ports held by the rows this snapshot dropped, by node. The
    /// capacity math ignores those rows, but the apply transaction's port
    /// check still sees them until the handler removes them.
    dropped_ports: HashMap<String, HashSet<u16>>,
}

#[derive(Debug, Clone, Copy)]
enum NodesConsidered {
    Count(u64),
    Percent(f64),
}

/// Bounded candidate scoring (`schedulerSettings.score_sample`,
/// bounded-candidate-scoring plan): below `min_nodes` eligible candidates a
/// placement scores every one of them (exactly today's behavior); at or
/// above it, scoring runs on `sample` uniformly drawn candidates —
/// best-of-k selection, which keeps per-placement cost flat as the fleet
/// grows. Feasibility (constraints, permits, resources, distinct) is never
/// sampled: a drawn candidate is always a valid placement, only the score
/// *ranking* is approximate.
#[derive(Debug, Clone, Copy)]
struct ScoreSample {
    min_nodes: usize,
    sample: usize,
}

/// Who is doing the preempting, as `build_plan` needs to know it.
struct PreemptorContext<'a> {
    /// Preempting job's name.
    job: &'a str,
    /// Its priority band, which decides what it may displace at all.
    band: Band,
    /// Whether observe-mode findings are written to the event journal.
    /// Only the applying path journals; dry runs preview silently.
    journal_observed: bool,
}

/// One node's preemption answer: where to place, and what it costs.
#[derive(Debug, Clone)]
struct PreemptionChoice {
    node_id: String,
    victims: Vec<VictimCandidate>,
}

/// The preemption state of one `build_plan` run.
///
/// Threaded through every placement in the plan rather than rebuilt per
/// placement, for two reasons that are both correctness, not speed:
///
/// - the `disruption.minAvailable` floor has to hold across the whole plan
///   (`taken_by_task`), or eight placements each take "one below the floor"
///   and together take eight;
/// - the victim metadata is loaded once and only on demand, so a plan that
///   never needs preemption never touches the store for it.
struct PreemptionPass {
    /// The band of the job being placed.
    preemptor_band: Band,
    /// Job name of the preemptor, for events and stop notices.
    preemptor_job: String,
    /// Config id of the job being placed, for the observe-mode event.
    preemptor_config_id: String,
    /// Whether observe-mode findings are journalled. False for dry runs
    /// (`job plan`), which must observe without writing anything — a preview
    /// that emitted events would make repeatedly previewing a job look like
    /// repeatedly nearly-preempting one.
    journal_observed: bool,
    /// Victim metadata by node, loaded lazily for the nodes actually asked
    /// about. `None` until the first preemption attempt in this plan.
    by_node: Option<HashMap<String, Vec<VictimCandidate>>>,
    /// Victims this plan has committed to evicting.
    victims: Vec<PlannedPreemption>,
    /// Victims `observe` mode would have taken, for the event and preview.
    observed: Vec<PlannedPreemption>,
    /// How many victims this plan has already taken from each task, so
    /// disruption floors bind across placements.
    taken_by_task: HashMap<i64, u32>,
    /// Why candidates were rejected, accumulated for the placement error.
    rejected: preemption::IneligibleTally,
    /// How many placements ran a preemption pass at all.
    considered: usize,
    /// True when a pass had feasible nodes to consider and every one of them
    /// was in a pool with preemption disabled.
    pool_blocked: bool,
    /// False when the pass never ran because the mode or the pool disabled
    /// it — which the "could not place" error should say, since it is the
    /// most likely reason an operator is surprised.
    enabled: bool,
    /// Whether the whole cluster (not just a pool) has preemption off.
    mode: PreemptionMode,
}

impl PreemptionPass {
    fn new(
        preemptor_band: Band,
        preemptor_job: &str,
        preemptor_config_id: &str,
        journal_observed: bool,
        settings: &PreemptionSettings,
    ) -> Self {
        Self {
            preemptor_band,
            preemptor_job: preemptor_job.to_string(),
            preemptor_config_id: preemptor_config_id.to_string(),
            journal_observed,
            by_node: None,
            victims: Vec::new(),
            observed: Vec::new(),
            taken_by_task: HashMap::new(),
            rejected: preemption::IneligibleTally::default(),
            considered: 0,
            pool_blocked: false,
            enabled: !matches!(settings.mode, PreemptionMode::Off),
            mode: settings.mode,
        }
    }

    /// Load victim metadata for any of `candidates` not already indexed.
    fn load_candidates(
        &mut self,
        store: &Store,
        resource_policy: &ResourcePolicy,
        candidates: &[&Node],
    ) -> R<()> {
        let index = self.by_node.get_or_insert_with(HashMap::new);
        let missing: Vec<&str> = candidates
            .iter()
            .map(|node| node.id.as_str())
            .filter(|node_id| !index.contains_key(*node_id))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        // Seed every requested node, so an empty node is "loaded, nothing
        // here" rather than "not loaded" and is never re-queried.
        for node_id in &missing {
            index.entry((*node_id).to_string()).or_default();
        }
        let rows = store.preemption_victim_metadata(&missing)?;
        // The floor is measured against the task's live allocation count
        // across the fleet, not just this node's share.
        let mut running_by_task: HashMap<i64, u32> = HashMap::new();
        for row in &rows {
            *running_by_task.entry(row.task_config_id).or_default() += 1;
        }
        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        for row in rows {
            // An allocation whose resources will not parse is skipped rather
            // than treated as free: the same fits-nothing posture the free
            // pool takes for a bad row. Evicting something whose usage we
            // cannot compute would not provably free what the placement
            // needs.
            let Ok(request) = resource_policy
                .normalize_task_resources(&row.resources, false)
                .and_then(|normalized| ResourceRequest::from_value(&normalized))
            else {
                warn!(
                    allocation_id = row.allocation_id,
                    "Allocation resources failed to parse; it is not a preemption candidate"
                );
                continue;
            };
            let disruption = row
                .disruption_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<crate::structs::DisruptionConfig>(raw).ok());
            let stop = row
                .stop_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<crate::structs::StopConfig>(raw).ok())
                .unwrap_or_default();
            index
                .entry(row.node_id.clone())
                .or_default()
                .push(VictimCandidate {
                    allocation_id: row.allocation_id,
                    node_id: row.node_id.clone(),
                    job_id: row.job_id,
                    job_config_id: row.job_config_id,
                    task_config_id: row.task_config_id,
                    task_name: row.task_name,
                    band: row.band,
                    request,
                    age_secs: row.created_at_unix.map(|created| now_unix - created),
                    min_available: disruption
                        .map(|disruption| disruption.min_available_for(row.task_count))
                        .unwrap_or(0),
                    task_running: running_by_task
                        .get(&row.task_config_id)
                        .copied()
                        .unwrap_or(1),
                    node_pinned: row.node_pinned,
                    kill_after_secs: stop.kill_after.as_secs(),
                });
        }
        Ok(())
    }

    /// Drop victims this plan has committed to from the candidate index, so
    /// a later placement in the same plan cannot pick them again.
    fn retire_victims(&mut self, node_id: &str, victims: &[VictimCandidate]) {
        let Some(on_node) = self
            .by_node
            .as_mut()
            .and_then(|index| index.get_mut(node_id))
        else {
            return;
        };
        on_node.retain(|candidate| {
            !victims
                .iter()
                .any(|victim| victim.allocation_id == candidate.allocation_id)
        });
    }

    fn candidates_on_node(&self, node_id: &str) -> Vec<VictimCandidate> {
        self.by_node
            .as_ref()
            .and_then(|index| index.get(node_id))
            .cloned()
            .unwrap_or_default()
    }

    fn record_tally(&mut self, tally: preemption::IneligibleTally) {
        self.rejected.same_or_higher_band += tally.same_or_higher_band;
        self.rejected.too_young += tally.too_young;
        self.rejected.node_pinned += tally.node_pinned;
        self.rejected.floor_bound += tally.floor_bound;
    }

    /// The "could not place" message, in band terms.
    ///
    /// Nomad's discoverability failure inverted: a user whose `high` job did
    /// not place should not have to discover from a troubleshooting guide
    /// that preemption was off, or that a floor held. The message names
    /// whichever of those actually happened.
    fn no_placement_error(&self, task_config: &TaskConfig) -> String {
        let base = format!(
            "Failed to place task {}: no available nodes matching constraints, permit, or resources",
            task_config.name
        );
        if !self.preemptor_band.can_preempt() {
            return format!(
                "{base}. This job is in the lowest priority band ({}), so preemption cannot \
                 make room for it (see docs/preemption-and-eviction.md)",
                self.preemptor_band
            );
        }
        if !self.enabled {
            return format!(
                "{base}. Preemption is off for this cluster \
                 (schedulerSettings.preemption.mode), so a {} job cannot displace lower-band \
                 work (see docs/preemption-and-eviction.md)",
                self.preemptor_band
            );
        }
        if matches!(self.mode, PreemptionMode::Observe) && !self.observed.is_empty() {
            return format!(
                "{base}. Preemption is in observe mode: it identified {} evictable \
                 allocation(s) but committed none. Set \
                 schedulerSettings.preemption.mode to 'enforce' to act on it",
                self.observed.len()
            );
        }
        if self.pool_blocked {
            return format!(
                "{base}. Preemption did not run: every candidate node is in a pool where \
                 preemption is disabled (schedulerSettings.preemption.disabledPools)"
            );
        }
        if self.considered == 0 {
            return format!(
                "{base}. Preemption did not run: no node this task is allowed on had any \
                 allocation to consider"
            );
        }
        if self.rejected.is_empty() {
            return format!(
                "{base}. Preemption ran but found nothing to displace: every feasible node is \
                 full of work at this band or above"
            );
        }
        let detail = self.rejected.describe();
        if detail.is_empty() {
            format!(
                "{base}. Preemption found no eligible victims on any feasible node. \
                 Job stays under its desired count"
            )
        } else {
            format!(
                "{base}; preemption found no eligible victims ({detail}). \
                 Job stays under its desired count"
            )
        }
    }
}

/// Plan-time view of the host ports in use on each node, so the planner only
/// offers a static-port task nodes where its ports are free — the check the
/// apply transaction makes (`Store::assign_allocation_network`), made before
/// the plan is built instead of after it is rejected.
///
/// Lazy: a node's set is read from the snapshot the first time a static-port
/// task asks about it, so a plan with no static ports pays nothing.
#[derive(Debug, Default)]
struct PortLedger {
    /// Ports in use per node: the snapshot's allocations, plus ports this
    /// plan already claimed. Built on first use per node.
    used: HashMap<String, HashSet<u16>>,
    /// Ports held by rows the snapshot dropped but the apply still sees (a
    /// Fresh plan's own previous allocations), merged in on first use.
    dropped: HashMap<String, HashSet<u16>>,
}

impl PortLedger {
    fn new(dropped: HashMap<String, HashSet<u16>>) -> Self {
        Self {
            used: HashMap::new(),
            dropped,
        }
    }

    fn used_on(
        &mut self,
        node_id: &str,
        allocations_by_node: &SharedNodeAllocations,
    ) -> &mut HashSet<u16> {
        if !self.used.contains_key(node_id) {
            let mut used: HashSet<u16> = allocations_by_node
                .get(node_id)
                .map(|allocations| {
                    allocations
                        .iter()
                        .flat_map(allocation_host_ports)
                        .collect::<HashSet<u16>>()
                })
                .unwrap_or_default();
            if let Some(dropped) = self.dropped.get(node_id) {
                used.extend(dropped.iter().copied());
            }
            self.used.insert(node_id.to_string(), used);
        }
        self.used.get_mut(node_id).expect("inserted above")
    }

    /// The first of `ports` already in use on `node_id`, if any.
    fn conflict(
        &mut self,
        node_id: &str,
        ports: &[u16],
        allocations_by_node: &SharedNodeAllocations,
    ) -> Option<u16> {
        let used = self.used_on(node_id, allocations_by_node);
        ports.iter().copied().find(|port| used.contains(port))
    }

    /// Record that this plan placed a task holding `ports` on `node_id`.
    /// Only called after [`Self::conflict`] has loaded the node's set.
    fn claim(&mut self, node_id: &str, ports: &[u16]) {
        self.used
            .entry(node_id.to_string())
            .or_default()
            .extend(ports.iter().copied());
    }
}

/// The host ports an allocation holds on its node: its assigned task ports
/// and its upstream loopback ports. The same set the apply transaction
/// treats as taken.
fn allocation_host_ports(allocation: &Allocation) -> impl Iterator<Item = u16> + '_ {
    allocation.network.iter().flat_map(|network| {
        network
            .ports
            .iter()
            .map(|port| port.host_port)
            .chain(network.upstreams.iter().map(|upstream| upstream.local_port))
    })
}

/// The static host ports a task claims wherever it lands: `network.ports`
/// entries with a `static` port and `connect.upstreams` with a fixed
/// `localPort`. Empty for the common all-dynamic task.
fn task_static_ports(task: &TaskConfig) -> Vec<u16> {
    let mut ports: Vec<u16> = task
        .network
        .iter()
        .flat_map(|network| network.ports.iter().filter_map(|port| port.static_port))
        .chain(task.connect.iter().flat_map(|connect| {
            connect
                .upstreams
                .iter()
                .filter_map(|upstream| upstream.local_port)
        }))
        .collect();
    ports.sort_unstable();
    ports.dedup();
    ports
}

/// Plan-time free-capacity ledger. Pools are `Arc`'d straight off the
/// read-model snapshot; `consume` copies-on-write only the pools this plan
/// actually debits, so building the ledger never deep-clones the fleet.
#[derive(Debug, Clone)]
struct ResourceLedger {
    /// The snapshot's persistent pool map: debits path-copy only the
    /// entries they touch, so the plan's private view never copies the
    /// fleet.
    pools: SharedNodePools,
}

struct TaskPlan<'a> {
    /// The task's hard filters (constraints, permit), parsed once per plan
    /// in `build_plan` and shared with the group-filter pass.
    filters: Arc<(ConstraintSet, PermitSet)>,
    affinity_set: AffinitySet,
    task_request: ResourceRequest,
    base_eligible: Vec<&'a Node>,
    distinct_tracker: Option<DistinctTracker>,
    index_usage: TaskIndexUsage,
}

impl TaskPlan<'_> {
    fn constraint_set(&self) -> &ConstraintSet {
        &self.filters.0
    }

    fn permit_set(&self) -> &PermitSet {
        &self.filters.1
    }
}

#[derive(Debug, Clone, Default)]
struct CommonResourceDemand {
    memory: f64,
    cpu: f64,
    cores: f64,
    allocations: f64,
}

#[derive(Debug, Clone, Default)]
struct CommonResourceAvailability {
    memory: f64,
    cpu: f64,
    cores: f64,
    allocations: f64,
}

#[derive(Debug, Clone)]
struct NodeScoringState {
    assigned_count: usize,
    same_task_allocations: usize,
    same_group_allocations: usize,
    task_presence: HashSet<i64>,
    base_affinity: f64,
    can_fit_task_request: bool,
    common_resources: CommonResourceAvailability,
    common_feasibility_bitmap: u8,
    /// Fraction of the placement's content set this node holds (0 when the
    /// set is empty or the node has not reported). Filled by
    /// [`PlanScoringCache::apply_locality`], once per task.
    locality: f64,
}

#[derive(Debug, Clone)]
struct TaskAffinityCache {
    preferences: Vec<(i64, f64)>,
    total_weight: f64,
}

/// A candidate's identity within one plan: the address of its `Node` in
/// the plan's node snapshot. Every candidate list a plan builds borrows
/// from the same `Vec<Arc<Node>>`, so two `&Node`s naming the same node
/// share an address, and the scoring cache can key its per-node state by
/// that address instead of by id string — one multiply to hash, where the
/// id was a SipHash of the string, three times per candidate per
/// placement. Never dereferenced: only compared and hashed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeKey(usize);

impl NodeKey {
    fn of(node: &Node) -> Self {
        NodeKey(node as *const Node as usize)
    }
}

/// The hasher behind [`NodeKey`]: a multiplicative mix of the address.
/// Heap addresses share their low bits (alignment) and their high bits
/// (the arena), so the high half is folded in before and after the
/// multiply to spread both into the bits the table indexes by.
#[derive(Debug, Default, Clone, Copy)]
struct NodeKeyHasher(u64);

impl std::hash::Hasher for NodeKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // Not reached for `NodeKey` (a `usize` hashes through
        // `write_usize`); kept total for the trait.
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }

    fn write_usize(&mut self, value: usize) {
        let mut mixed = value as u64;
        mixed ^= mixed >> 32;
        mixed = mixed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        mixed ^= mixed >> 29;
        self.0 = mixed;
    }
}

type NodeKeyIndex = HashMap<NodeKey, usize, std::hash::BuildHasherDefault<NodeKeyHasher>>;

/// Per-candidate scoring state for one task's placements, built once per
/// candidate list and patched as placements land.
///
/// State lives in a `Vec` parallel to the candidate list, found by
/// [`NodeKey`] — so the per-placement passes (eligibility, load bounds,
/// scoring) index by node address rather than hashing each node's id
/// string, and construction clones no ids.
#[derive(Debug, Clone)]
struct PlanScoringCache {
    states: Vec<NodeScoringState>,
    index: NodeKeyIndex,
    affinity: TaskAffinityCache,
    common_demand: CommonResourceDemand,
    group_name: Option<String>,
    /// This task's locality scores, kept for nodes outside `states` and for
    /// the policy-plugin path. `None` skips the term.
    locality: Option<Arc<crate::storage::content_index::LocalityScores>>,
}

impl PlanScoringCache {
    const COMMON_MEMORY: u8 = 1 << 0;
    const COMMON_CPU: u8 = 1 << 1;
    const COMMON_CORES: u8 = 1 << 2;
    const COMMON_ALLOCATIONS: u8 = 1 << 3;

    fn from_plan(
        candidates: &[&Node],
        task_id: i64,
        task_config: &TaskConfig,
        affinity_set: &AffinitySet,
        allocations_by_node: &SharedNodeAllocations,
        own_by_node: &OwnAllocationsByNode,
        task_name_to_id: &HashMap<String, i64>,
        task_id_to_group: &HashMap<i64, String>,
        planned_task_allocations: &HashMap<String, Vec<i64>>,
        planned: &HashMap<String, usize>,
        resource_ledger: &ResourceLedger,
        task_request: &ResourceRequest,
    ) -> Self {
        let group_name = task_id_to_group.get(&task_id).cloned();
        let affinity = TaskAffinityCache::new(task_config, task_name_to_id);
        let common_demand = CommonResourceDemand::from_request(task_request);
        let in_group = |other_task_id: i64| {
            group_name.as_ref().is_some_and(|group| {
                task_id_to_group
                    .get(&other_task_id)
                    .is_some_and(|other_group| other_group == group)
            })
        };

        let mut states = Vec::with_capacity(candidates.len());
        let mut index =
            NodeKeyIndex::with_capacity_and_hasher(candidates.len(), Default::default());
        for (position, node) in candidates.iter().enumerate() {
            let node_id = node.id.as_str();
            let assigned_count = allocations_by_node
                .get(node_id)
                .map_or(0, |allocations| allocations.len())
                + planned.get(node_id).copied().unwrap_or_default();

            // The planned config's allocations already on this node come
            // from the plan's own-allocation index — not from a walk of
            // every allocation the node holds, which made building this
            // cache O(allocations on the candidate fleet) per task.
            let existing_own = own_by_node.get(node_id).map(Vec::as_slice).unwrap_or(&[]);
            let planned_own = planned_task_allocations
                .get(node_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let mut task_presence = HashSet::new();
            let mut same_task_allocations = 0usize;
            let mut same_group_allocations = 0usize;
            for other_task_id in existing_own.iter().chain(planned_own) {
                task_presence.insert(*other_task_id);
                if *other_task_id == task_id {
                    same_task_allocations += 1;
                }
                if in_group(*other_task_id) {
                    same_group_allocations += 1;
                }
            }

            let (common_resources, common_feasibility_bitmap, can_fit_task_request) =
                resource_ledger
                    .pools
                    .get(node_id)
                    .map(|pool| {
                        let common_resources = CommonResourceAvailability::from_pool(pool);
                        let bitmap = common_resources.feasibility_bitmap(&common_demand);
                        (common_resources, bitmap, pool.can_fit(task_request))
                    })
                    .unwrap_or_else(|| {
                        (
                            CommonResourceAvailability::default(),
                            0,
                            resource_ledger.can_fit(node_id, task_request),
                        )
                    });

            index.insert(NodeKey::of(node), position);
            states.push(NodeScoringState {
                assigned_count,
                same_task_allocations,
                same_group_allocations,
                task_presence,
                base_affinity: affinity_set.score(node),
                can_fit_task_request,
                common_resources,
                common_feasibility_bitmap,
                locality: 0.0,
            });
        }

        Self {
            states,
            index,
            affinity,
            common_demand,
            group_name,
            locality: None,
        }
    }

    /// Fold this task's locality scores into the per-candidate state: one
    /// lookup per candidate per task, so the per-placement loop reads a
    /// field instead of hashing a node id.
    fn apply_locality(
        &mut self,
        candidates: &[&Node],
        locality: Option<&Arc<crate::storage::content_index::LocalityScores>>,
    ) {
        // Shared, not copied: on a large fleet the score map holds an entry
        // per warm node, and the refill path applies it a second time.
        self.locality = locality.cloned();
        let Some(scores) = locality else {
            return;
        };
        for node in candidates {
            if let Some(position) = self.index.get(&NodeKey::of(node)) {
                self.states[*position].locality = scores.score(&node.id);
            }
        }
    }

    /// This candidate's state, or `None` for a node the cache was not
    /// built from.
    fn state_of(&self, node: &Node) -> Option<&NodeScoringState> {
        self.index
            .get(&NodeKey::of(node))
            .map(|position| &self.states[*position])
    }

    fn can_fit_task_request(&self, node: &Node) -> bool {
        self.state_of(node)
            .map(|state| {
                let required_bitmap = self.required_common_bitmap();
                let common_feasible =
                    (state.common_feasibility_bitmap & required_bitmap) == required_bitmap;
                common_feasible && state.can_fit_task_request
            })
            .unwrap_or(false)
    }

    fn update_after_placement(
        &mut self,
        node: &Node,
        task_id: i64,
        task_id_to_group: &HashMap<i64, String>,
        resource_ledger: &ResourceLedger,
        task_request: &ResourceRequest,
    ) {
        let Some(position) = self.index.get(&NodeKey::of(node)).copied() else {
            return;
        };
        let state = &mut self.states[position];
        state.assigned_count += 1;
        state.same_task_allocations += 1;
        if self.group_name.as_ref().is_some_and(|group| {
            task_id_to_group
                .get(&task_id)
                .is_some_and(|task_group| task_group == group)
        }) {
            state.same_group_allocations += 1;
        }
        state.task_presence.insert(task_id);
        state.common_resources.consume(&self.common_demand);
        state.common_feasibility_bitmap = state
            .common_resources
            .feasibility_bitmap(&self.common_demand);
        state.can_fit_task_request = resource_ledger.can_fit(&node.id, task_request);
    }

    /// Recompute one node's capacity view from the ledger.
    ///
    /// [`Self::update_after_placement`] only ever *debits*; preemption
    /// credits a node when its victims are booked, so the cached
    /// feasibility bitmap has to be rebuilt from the pool rather than
    /// adjusted by a delta.
    fn refresh_node(
        &mut self,
        node: &Node,
        resource_ledger: &ResourceLedger,
        task_request: &ResourceRequest,
    ) {
        let Some(position) = self.index.get(&NodeKey::of(node)).copied() else {
            return;
        };
        let state = &mut self.states[position];
        match resource_ledger.pools.get(node.id.as_str()) {
            Some(pool) => {
                state.common_resources = CommonResourceAvailability::from_pool(pool);
                state.common_feasibility_bitmap = state
                    .common_resources
                    .feasibility_bitmap(&self.common_demand);
                state.can_fit_task_request = pool.can_fit(task_request);
            }
            None => {
                state.common_resources = CommonResourceAvailability::default();
                state.common_feasibility_bitmap = 0;
                state.can_fit_task_request = false;
            }
        }
    }

    fn required_common_bitmap(&self) -> u8 {
        let mut bitmap = 0u8;
        if self.common_demand.memory > 0.0 {
            bitmap |= Self::COMMON_MEMORY;
        }
        if self.common_demand.cpu > 0.0 {
            bitmap |= Self::COMMON_CPU;
        }
        if self.common_demand.cores > 0.0 {
            bitmap |= Self::COMMON_CORES;
        }
        if self.common_demand.allocations > 0.0 {
            bitmap |= Self::COMMON_ALLOCATIONS;
        }
        bitmap
    }
}

impl CommonResourceDemand {
    fn from_request(request: &ResourceRequest) -> Self {
        let mut demand = CommonResourceDemand::default();
        for (name, value) in &request.values {
            if let ResourceValue::Amount(amount) = value {
                match name.as_str() {
                    "memory" => demand.memory = *amount,
                    "cpu" => demand.cpu = *amount,
                    "cores" => demand.cores = *amount,
                    "allocations" => demand.allocations = *amount,
                    _ => {}
                }
            }
        }
        demand
    }
}

impl CommonResourceAvailability {
    /// The four common dims of a free pool. One pass over the pool's
    /// handful of entries, matching names by string compare, rather than
    /// four hash lookups: this runs once per candidate per task, so at
    /// fleet scale the lookups were a measurable share of building the
    /// scoring cache. A dim the pool lacks, or carries as items, reads as
    /// `0.0`, exactly as the lookups did.
    fn from_pool(pool: &ResourcePool) -> Self {
        let mut availability = CommonResourceAvailability::default();
        for (name, value) in &pool.values {
            let ResourceValue::Amount(amount) = value else {
                continue;
            };
            match name.as_str() {
                "memory" => availability.memory = *amount,
                "cpu" => availability.cpu = *amount,
                "cores" => availability.cores = *amount,
                "allocations" => availability.allocations = *amount,
                _ => {}
            }
        }
        availability
    }

    fn consume(&mut self, demand: &CommonResourceDemand) {
        self.memory -= demand.memory;
        self.cpu -= demand.cpu;
        self.cores -= demand.cores;
        self.allocations -= demand.allocations;
    }

    fn feasibility_bitmap(&self, demand: &CommonResourceDemand) -> u8 {
        let mut bitmap = 0u8;
        if self.memory >= demand.memory {
            bitmap |= PlanScoringCache::COMMON_MEMORY;
        }
        if self.cpu >= demand.cpu {
            bitmap |= PlanScoringCache::COMMON_CPU;
        }
        if self.cores >= demand.cores {
            bitmap |= PlanScoringCache::COMMON_CORES;
        }
        if self.allocations >= demand.allocations {
            bitmap |= PlanScoringCache::COMMON_ALLOCATIONS;
        }
        bitmap
    }
}

impl TaskAffinityCache {
    fn new(task_config: &TaskConfig, task_name_to_id: &HashMap<String, i64>) -> Self {
        let mut preferences = Vec::new();
        let mut total_weight = 0.0;
        for (task_name, multiplier) in task_colocation_preferences(&task_config.affinities) {
            if let Some(task_id) = task_name_to_id.get(task_name) {
                preferences.push((*task_id, multiplier));
                total_weight += multiplier.abs();
            }
        }

        TaskAffinityCache {
            preferences,
            total_weight,
        }
    }

    fn colocation_score(&self, state: &NodeScoringState) -> f64 {
        if self.preferences.is_empty() || self.total_weight == 0.0 {
            return 0.0;
        }

        let matched = self
            .preferences
            .iter()
            .filter_map(|(task_id, multiplier)| {
                state.task_presence.contains(task_id).then_some(*multiplier)
            })
            .sum::<f64>();
        matched / self.total_weight
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    name: String,
    token: String,
}

pub struct Scheduler<'a> {
    store: &'a Store,
    policy: Box<dyn SchedulingPolicy>,
    #[allow(dead_code)]
    policy_lib: Option<Library>,
    /// Settings parsed ONCE at construction (`scheduler_settings` is fixed
    /// for the store's lifetime): `build_plan` used to re-walk the settings
    /// JSON on every plan. Malformed settings therefore fail at
    /// `Scheduler::new` rather than at the first plan.
    weights: SchedulerWeights,
    nodes_considered: Option<NodesConsidered>,
    score_sample: Option<ScoreSample>,
    plan_candidates: PlanCandidatesSetting,
    /// `schedulerSettings.plan_apply_batch_max` — how many plans one group
    /// commit may carry, and so how many plans can race for the same node
    /// scores at once. Sizes the fallback sampling pool
    /// ([`Self::fallback_pool_width`]).
    plan_apply_batch_max: usize,
    /// `schedulerSettings.preemption`. Parsed here for the same reason as the
    /// settings above — and so a typo'd `mode` fails the manager at startup
    /// rather than quietly evicting production work on the first busy plan.
    preemption: PreemptionSettings,
}

#[derive(Debug, Clone)]
pub struct PlanConfigOutcome {
    pub allocations: Vec<PlannedAllocation>,
    pub task_lookup: HashMap<i64, String>,
    /// Per-task scheduling-index usage (docs/scheduling-indexes.md).
    pub index_report: Vec<TaskIndexUsage>,
    /// Allocations this plan would displace. The whole point of the dry run
    /// for anyone submitting work that outranks something: the submit becomes
    /// a decision rather than a surprise.
    pub preemptions: Vec<PlannedPreemption>,
}

impl<'a> Scheduler<'a> {
    pub fn new(store: &'a Store) -> R<Self> {
        let (policy_lib, policy) = load_policy(&store.scheduling_policy)?;

        // Two-phase: the parsers below read `self.policy`/`self.store`, so
        // construct with the policy's default weights first and overwrite.
        let mut scheduler = Scheduler {
            weights: SchedulerWeights::for_policy(policy.name()),
            store,
            policy,
            policy_lib,
            nodes_considered: None,
            score_sample: None,
            plan_candidates: PlanCandidatesSetting::default(),
            plan_apply_batch_max: Store::plan_apply_batch_max(store.scheduler_settings.as_ref()),
            preemption: PreemptionSettings::default(),
        };
        scheduler.weights = scheduler.scheduler_weights()?;
        scheduler.nodes_considered = scheduler.nodes_considered_setting()?;
        scheduler.score_sample = scheduler.score_sample_setting()?;
        scheduler.plan_candidates = scheduler.plan_candidates_setting()?;
        scheduler.preemption =
            PreemptionSettings::from_scheduler_settings(store.scheduler_settings.as_ref())?;
        if scheduler.retired_lazy_version_sample().is_some() {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                warn!(
                    "schedulerSettings.lazy_version_sample is retired and has no effect: \
                     plans now sample the fence epochs from their own snapshot"
                );
            });
        }
        Ok(scheduler)
    }

    /// The cluster's preemption settings, for callers that need to report
    /// them (the submit-time warning, `job plan`).
    pub fn preemption_settings(&self) -> &PreemptionSettings {
        &self.preemption
    }

    #[instrument(skip_all)]
    pub fn plan_only(&self, job_config_id: &str, mode: PlanMode) -> R<Vec<PlannedAllocation>> {
        Ok(self.plan_only_with_report(job_config_id, mode)?.0)
    }

    /// `plan_only`, additionally returning the per-task scheduling-index
    /// usage report for the `job plan` explain surface, and the preemptions
    /// the plan would make.
    #[instrument(skip_all)]
    pub fn plan_only_with_report(
        &self,
        job_config_id: &str,
        mode: PlanMode,
    ) -> R<(
        Vec<PlannedAllocation>,
        Vec<TaskIndexUsage>,
        Vec<PlannedPreemption>,
    )> {
        let started = std::time::Instant::now();
        let mode_label = mode.as_str();
        info!(%job_config_id, mode = mode_label, "Starting scheduler plan_only");
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let all_tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?;
        let mut tasks = self
            .tasks_for_plan_from_store(job_config_id, DependencyGate::Preview)?
            .ready;
        if matches!(mode, PlanMode::Reschedule) {
            if let Some(job_id) = self.store.job_id_for_job_config(job_config_id)? {
                Self::apply_slot_staged_counts(self.store, &job_id, job_config_id, &mut tasks)?;
            }
        }

        // The config's job names its entry in the read model's job index,
        // and its band decides what the plan may preempt.
        let preemptor = self.store.job_band_for_job_config(job_config_id)?;
        let snapshot = self.capacity_snapshot(
            job_config_id,
            &preemptor.0,
            if matches!(mode, PlanMode::Fresh) {
                SnapshotDrop::JobConfig(job_config_id)
            } else {
                SnapshotDrop::Nothing
            },
        )?;

        let required_counts =
            Self::required_counts_for_mode(&tasks, mode.clone(), &snapshot.own_by_node);
        let BuiltPlan {
            placements,
            index_report,
            evictions,
            observed_preemptions,
            unplaced: _,
        } = self.build_plan(
            job_config_id,
            nodes,
            tasks,
            all_tasks,
            required_counts,
            snapshot,
            mode,
            None,
            // A preview keeps all-or-nothing: it reports the first replica
            // that would not fit as the plan's error.
            false,
            PreemptorContext {
                job: &preemptor.0,
                band: preemptor.1,
                // A read-only plan preview writes nothing, events included.
                journal_observed: false,
            },
        )?;
        info!(
            %job_config_id,
            mode = mode_label,
            planned_allocations = placements.len(),
            preempts = evictions.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Completed scheduler plan_only"
        );
        // A dry run reports the enforce-mode decision when there is one, and
        // otherwise what observe mode declined to act on — so the preview
        // answers "who would this displace" the same way in both modes.
        let preemptions = if evictions.is_empty() {
            observed_preemptions
        } else {
            evictions
        };
        Ok((placements, index_report, preemptions))
    }

    /// Takes the plan's capacity snapshot from the read model, optionally
    /// dropping (and recording the nodes of) the allocations this plan
    /// replaces.
    fn capacity_snapshot(
        &self,
        job_config_id: &str,
        job_id: &str,
        drop: SnapshotDrop<'_>,
    ) -> R<CapacitySnapshot> {
        let (mut allocations_by_node, pools, _generation, job_allocations, scope_epochs) =
            self.store.plan_capacity_snapshot(job_id)?;
        let (own_by_node, freed_nodes, dropped_ports) = Self::index_own_allocations(
            job_config_id,
            job_id,
            &job_allocations,
            &mut allocations_by_node,
            drop,
        );
        Ok(CapacitySnapshot {
            allocations_by_node,
            pools,
            freed_nodes,
            scope_epochs,
            own_by_node,
            dropped_ports,
        })
    }

    /// Drops the rows `drop` names from the snapshot (recording the nodes
    /// it touched, whose free pools the ledger recomputes) and indexes the
    /// surviving allocations of the planned config by node
    /// ([`OwnAllocationsByNode`]) — both from the read model's job-index
    /// entry for `job_id`, so the work is O(the job's allocations).
    ///
    /// No step of a plan walks the cluster's allocations any more. Each
    /// consumer of the index used to run its own full walk — five or six
    /// per plan, every one filtering for the same job — which made
    /// planning cost proportional to how full the cluster was rather than
    /// to the job being placed (the fill slope `plan_under_fill` in
    /// `benchmarks/micro/scheduler_bench.rs` measures). The drop still
    /// re-tests the actual rows of each listed node, so a stale index
    /// entry can only cost a needless pass over one node's list, never a
    /// wrong drop.
    fn index_own_allocations(
        job_config_id: &str,
        job_id: &str,
        job_allocations: &JobAllocationsByNode,
        allocations_by_node: &mut SharedNodeAllocations,
        drop: SnapshotDrop<'_>,
    ) -> (
        OwnAllocationsByNode,
        HashSet<String>,
        HashMap<String, HashSet<u16>>,
    ) {
        let mut own_by_node = OwnAllocationsByNode::new();
        let mut freed_nodes = HashSet::new();
        let mut dropped_ports: HashMap<String, HashSet<u16>> = HashMap::new();
        for (node_id, references) in job_allocations {
            let mut drops = false;
            let mut own: Vec<i64> = Vec::new();
            for reference in references {
                if drop.matches_ref(job_id, reference) {
                    drops = true;
                    continue;
                }
                if reference.job_config_id == job_config_id {
                    own.push(reference.task_config_id);
                }
            }
            if drops {
                if let Some(allocations) = allocations_by_node.get_mut(node_id) {
                    let ports: HashSet<u16> = allocations
                        .iter()
                        .filter(|allocation| drop.matches(allocation))
                        .flat_map(allocation_host_ports)
                        .collect();
                    if !ports.is_empty() {
                        dropped_ports.insert(node_id.clone(), ports);
                    }
                    // `Arc::make_mut` copies the vector, so only a node
                    // that actually loses a row pays for one.
                    Arc::make_mut(allocations).retain(|allocation| !drop.matches(allocation));
                    freed_nodes.insert(node_id.clone());
                }
            }
            if !own.is_empty() {
                own_by_node.insert(node_id.clone(), own);
            }
        }
        (own_by_node, freed_nodes, dropped_ports)
    }

    #[instrument(skip_all)]
    pub fn plan_for_config(
        &self,
        job_config: &JobConfig,
        mode: PlanMode,
        clear_job_id: Option<&str>,
    ) -> R<PlanConfigOutcome> {
        let started = std::time::Instant::now();
        let mode_label = mode.as_str();
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let mut tasks = Self::tasks_for_plan(job_config)?;
        // Count overrides apply to dry-run plans too, so `job plan` previews
        // what a submit would actually converge to (job id == job name).
        Self::apply_count_overrides(self.store, &job_config.name, &mut tasks)?;
        // Keyed tasks plan against live instances, not count.num, even for
        // not-yet-submitted configs (a fresh job simply has zero instances).
        Self::apply_keyed_instance_counts(self.store, &job_config.name, &mut tasks)?;
        let all_tasks = job_config
            .tasks
            .iter()
            .enumerate()
            .map(|(index, config)| ((index + 1) as i64, config.clone()))
            .collect::<Vec<_>>();
        let task_lookup = tasks
            .iter()
            .map(|(id, task, _count)| (*id, task.name.clone()))
            .collect::<HashMap<_, _>>();

        // The preview can only clear the planned job's own rows: that is
        // the entry the read model's job index serves, and every caller
        // asks for exactly that (a dry run of a resubmit).
        if let Some(clear_job_id) = clear_job_id {
            if clear_job_id != job_config.name {
                bail!(
                    "plan_for_config can only clear the planned job's own allocations \
                     (planning {}, asked to clear {clear_job_id})",
                    job_config.name
                );
            }
        }
        let snapshot = self.capacity_snapshot(
            &job_config.id(),
            &job_config.name,
            match clear_job_id {
                Some(job_id) => SnapshotDrop::Job(job_id),
                None => SnapshotDrop::Nothing,
            },
        )?;

        let required_counts =
            Self::required_counts_for_mode(&tasks, mode.clone(), &snapshot.own_by_node);

        let BuiltPlan {
            placements,
            index_report,
            evictions,
            observed_preemptions,
            unplaced: _,
        } = self.build_plan(
            &job_config.id(),
            nodes,
            tasks,
            all_tasks,
            required_counts,
            snapshot,
            mode,
            None,
            false,
            PreemptorContext {
                job: &job_config.name,
                band: job_config.priority,
                journal_observed: false,
            },
        )?;
        info!(
            job_id = %job_config.name,
            job_config_id = %job_config.id(),
            mode = mode_label,
            planned_allocations = placements.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Completed scheduler plan_for_config"
        );

        Ok(PlanConfigOutcome {
            allocations: placements,
            task_lookup,
            index_report,
            // A dry run reports what enforce mode WOULD evict, and what
            // observe mode already declined to — so `job plan` previews the
            // same decision either way (S3 in the plan).
            preemptions: if evictions.is_empty() {
                observed_preemptions
            } else {
                evictions
            },
        })
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply(&self, job_config_id: &str, mode: PlanMode) -> R<PlanOutcome> {
        self.plan_and_apply_with_exclusions(job_config_id, mode, None)
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply_with_targets(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        task_targets: &HashMap<i64, u32>,
    ) -> R<PlanOutcome> {
        self.plan_and_apply_with_targets_and_exclusions(job_config_id, mode, task_targets, None)
    }

    pub fn plan_and_apply_with_targets_and_exclusions(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        task_targets: &HashMap<i64, u32>,
        excluded_nodes: Option<&HashSet<String>>,
    ) -> R<PlanOutcome> {
        self.plan_and_apply_internal(job_config_id, mode, excluded_nodes, Some(task_targets))
    }

    pub fn plan_and_apply_with_exclusions(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        excluded_nodes: Option<&HashSet<String>>,
    ) -> R<PlanOutcome> {
        self.plan_and_apply_internal(job_config_id, mode, excluded_nodes, None)
    }

    fn plan_and_apply_internal(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        excluded_nodes: Option<&HashSet<String>>,
        task_targets: Option<&HashMap<i64, u32>>,
    ) -> R<PlanOutcome> {
        let started = std::time::Instant::now();
        let mut timings = PlanTimings::default();
        info!(%job_config_id, mode = mode.as_str(), target_override = task_targets.is_some(), "Starting scheduler plan_and_apply");
        // Evals wait in a queue, and the job can be stopped or move on to a
        // newer config meanwhile. Planning for either would place the full
        // count of something nobody wants, so bail before doing the work.
        // The apply transaction re-checks this, so it is a fence, not a hint.
        if let Some(reason) = self.store.job_config_unwanted_reason(job_config_id)? {
            info!(%job_config_id, %reason, "Skipping plan for a config that is no longer wanted");
            return Ok(PlanOutcome::empty(timings));
        }
        // The band governs whether this plan may preempt at all, and names
        // the preemptor in every victim's stop notice and event.
        let job_lookup_started = std::time::Instant::now();
        let (preemptor_job, preemptor_band) = self.store.job_band_for_job_config(job_config_id)?;
        timings.job_lookup_us = job_lookup_started.elapsed().as_micros() as u64;

        // Optimistic-concurrency fence. Every transaction that mutates a
        // scope's allocations bumps that scope's epoch
        // (`bump_epochs_for_allocations_tx`), and `apply_plan_submission`
        // re-reads these epochs inside its write transaction and refuses the
        // plan if any of them moved. So what the epochs fence is: "nothing
        // changed the allocations I planned against between the moment I
        // read them and the moment I write". That only holds if the epoch
        // sample is taken no later than the capacity read the plan is built
        // against — a sample taken after planning could see a *post-bump*
        // value from a competing plan, pass the compare-and-set, and commit
        // placements against capacity that no longer exists.
        //
        // The capacity snapshot carries the epoch mirror captured under the
        // SAME read guard as the allocations and pools
        // (`Store::plan_capacity_snapshot`), so the sample is taken *at* the
        // capacity read: the old eager path's guarantee, for only the
        // affected scopes. It replaces two paths (plan-path-fleet-scaling
        // plan § 6.1): an eager O(fleet) sample before planning, and a lazy
        // post-plan sample that was only valid if nothing had published in
        // between — and re-ran the whole plan eagerly when something had,
        // which under a concurrent burst was 86-96% of plans
        // (`benchmarks/micro/scheduler_bench.rs`, `plan_fleet_scaling`).
        let available_started = std::time::Instant::now();
        let mut nodes = self.available_nodes()?;
        if let Some(excluded) = excluded_nodes {
            // Shared as-is unless an excluded node is actually in the set.
            if let Some(filtered) = nodes.without(excluded) {
                nodes = Arc::new(filtered);
            }
        }
        timings.available_nodes_us = available_started.elapsed().as_micros() as u64;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let task_fetch_started = std::time::Instant::now();
        let all_tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?;
        let ReadyTasks {
            ready: mut tasks,
            held: held_tasks,
        } = self.tasks_for_plan_from_store(job_config_id, DependencyGate::Live)?;
        if task_targets.is_none() && matches!(mode, PlanMode::Reschedule) {
            if let Some(job_id) = self.store.job_id_for_job_config(job_config_id)? {
                Self::apply_slot_staged_counts(self.store, &job_id, job_config_id, &mut tasks)?;
            }
        }
        timings.task_fetch_us = task_fetch_started.elapsed().as_micros() as u64;

        // A `Fresh` plan replaces the job's allocations wholesale: whichever
        // handler asked for it either already dropped the previous ones
        // (`job_update`) or drops them the moment this plan applies
        // (`job_submit`). Counting them against node capacity here would
        // demand headroom for two copies of the job and fail a plain
        // resubmit of anything using more than half a node — and it would
        // contradict `plan_only` / `plan_for_config`, which drop them and so
        // report a plan the apply path then refuses.
        let job_lookup_started = std::time::Instant::now();
        let replaced_job_id = if matches!(mode, PlanMode::Fresh) {
            self.store.job_id_for_job_config(job_config_id)?
        } else {
            None
        };
        timings.job_lookup_us += job_lookup_started.elapsed().as_micros() as u64;
        let capacity_snapshot_started = std::time::Instant::now();
        let snapshot = self.capacity_snapshot(
            job_config_id,
            &preemptor_job,
            match replaced_job_id.as_deref() {
                Some(job_id) => SnapshotDrop::Job(job_id),
                None => SnapshotDrop::Nothing,
            },
        )?;
        timings.capacity_snapshot_us = capacity_snapshot_started.elapsed().as_micros() as u64;
        // O(1): a persistent map clone. Taken before `build_plan` consumes
        // the snapshot.
        let fence_epochs = snapshot.scope_epochs.clone();
        let existing_counts = Self::existing_alloc_counts_by_task(&snapshot.own_by_node);
        if let Some(task_targets) = task_targets {
            for (task_id, _config, desired) in &mut tasks {
                if let Some(target) = task_targets.get(task_id) {
                    *desired = (*target).min(*desired);
                } else if matches!(mode, PlanMode::Reschedule) {
                    *desired = existing_counts.get(task_id).copied().unwrap_or_default();
                }
            }
        }

        let required_counts =
            Self::required_counts_for_mode(&tasks, mode.clone(), &snapshot.own_by_node);

        let build_started = std::time::Instant::now();
        let BuiltPlan {
            placements,
            index_report,
            evictions,
            observed_preemptions,
            unplaced,
        } = self.build_plan(
            job_config_id,
            nodes,
            tasks,
            all_tasks,
            required_counts,
            snapshot,
            mode.clone(),
            task_targets,
            true,
            PreemptorContext {
                job: &preemptor_job,
                band: preemptor_band,
                journal_observed: true,
            },
        )?;
        timings.build_plan_us = build_started.elapsed().as_micros() as u64;
        if !held_tasks.is_empty() {
            info!(
                %job_config_id,
                held = %held_tasks.join(", "),
                "Tasks held until their dependencies reach a terminal state"
            );
        }
        if !unplaced.is_empty() {
            let unplaced_count: u32 = unplaced.iter().map(|task| task.count).sum();
            if placements.is_empty() {
                // Nothing fits anywhere: the plan fails as it always has, but
                // with a typed error a caller can inspect. Not a conflict, so
                // the eval is not requeued in a hot loop.
                timings.total_us = started.elapsed().as_micros() as u64;
                timings.observe();
                warn!(
                    %job_config_id,
                    unplaced = unplaced_count,
                    "Plan could not place any allocation"
                );
                return Err(anyhow::Error::new(PlacementBlocked { unplaced }));
            }
            warn!(
                %job_config_id,
                unplaced = unplaced_count,
                placing = placements.len(),
                reasons = %unplaced
                    .iter()
                    .map(|task| task.reason.as_str())
                    .collect::<Vec<_>>()
                    .join("; "),
                "Plan places what fits; the rest stays unplaced"
            );
        }
        let indexed_tasks = index_report
            .iter()
            .filter(|usage| usage.index.is_some())
            .count();
        debug!(
            %job_config_id,
            mode = mode.as_str(),
            placement_count = placements.len(),
            indexed_tasks,
            planned_tasks = index_report.len(),
            build_plan_ms = build_started.elapsed().as_millis() as u64,
            "Scheduler build_plan completed"
        );
        let _ = &observed_preemptions;
        if placements.is_empty() {
            info!(%job_config_id, "No placements generated during plan_and_apply");
            timings.total_us = started.elapsed().as_micros() as u64;
            timings.observe();
            return Ok(PlanOutcome {
                allocations: Vec::new(),
                deltas: Vec::new(),
                evictions: Vec::new(),
                timings,
                unplaced,
                held_tasks,
            });
        }

        let plan_evictions = evictions
            .iter()
            .map(|victim| crate::storage::store::PlanEviction {
                allocation_id: victim.allocation_id,
                node_id: victim.node_id.clone(),
                job_id: victim.job_id.clone(),
                job_config_id: victim.job_config_id.clone(),
                task_config_id: victim.task_config_id,
                band: victim.band.as_str().to_string(),
            })
            .collect::<Vec<_>>();
        // The expected vector covers the UNION of every placement's
        // candidate list — the apply walk needs each candidate's sampled
        // epoch to judge whether that candidate is still free. With
        // candidates disabled the lists are singletons and this is exactly
        // the placed-on node set.
        //
        // Victim nodes are always placement nodes (a victim only ever
        // frees the node its preemptor lands on), so the epoch fence
        // already covers them. Union them in anyway so the invariant is
        // enforced by the code rather than by a comment.
        let affected_nodes = placements
            .iter()
            .flat_map(|placement| placement.candidates.iter().cloned())
            .chain(plan_evictions.iter().map(|victim| victim.node_id.clone()))
            .collect::<HashSet<_>>();
        let version_sample_started = std::time::Instant::now();
        let expected_versions =
            Store::expected_versions_from_snapshot(&fence_epochs, job_config_id, &affected_nodes);
        timings.version_sample_us = version_sample_started.elapsed().as_micros() as u64;
        let candidates_setting = self.plan_candidates;
        // Speculative dispatch (docs/edge-boot.md): tasks that opted into
        // `speculativeStart` get their payload NOW, while the apply is in
        // flight, so the node's image pull and root-disk build overlap the
        // commit instead of queueing behind it. Nothing here can fail the
        // plan — a claim that does not land simply misses the head start, and
        // one whose plan is rejected dies at its own lease.
        //
        // The node speculated on is `candidates[0]`, the planner's first
        // choice, which is what a pre-candidate submission would have bound
        // outright. When the apply walk falls back to a later candidate
        // (`candidate_ranks[i] > 0`) the winning node is not the one holding
        // the claim, so the claim is never ratified and dies at its lease —
        // the same outcome as a rejected plan, and the reason a fallback
        // costs the head start rather than correctness.
        let speculative_started = std::time::Instant::now();
        let speculative_claims = crate::manager::dispatcher::dispatch_speculative(
            self.store,
            job_config_id,
            &placements,
        );
        timings.speculative_dispatch_us = speculative_started.elapsed().as_micros() as u64;

        // One replicated op for insert + apply: on raft backends the split
        // form paid a second quorum round-trip per plan (see
        // `Store::submit_and_apply_plan`).
        let apply_started = std::time::Instant::now();
        let (submission, apply_outcome) = if candidates_setting.disabled() {
            // Kill switch: emit the pre-candidate (format-1) submission shape,
            // byte-for-byte, so mixed-version manager fleets can hold format-2
            // rows off the raft log until every peer can parse them
            // (docs/late-binding-plan-apply.md).
            let placement_pairs = placements
                .iter()
                .map(|placement| (placement.node_id.clone(), placement.task_config_id))
                .collect::<Vec<_>>();
            self.store.submit_and_apply_plan(
                job_config_id,
                mode.as_str(),
                &placement_pairs,
                &expected_versions,
                &plan_evictions,
            )?
        } else {
            let submission_placements = placements
                .iter()
                .map(|placement| PlanSubmissionPlacement {
                    task_id: placement.task_config_id,
                    candidates: placement.candidates.clone(),
                    instance_id: placement.instance_id.clone(),
                })
                .collect::<Vec<_>>();
            self.store.submit_and_apply_plan_with_candidates(
                job_config_id,
                mode.as_str(),
                &submission_placements,
                &expected_versions,
                &plan_evictions,
            )?
        };
        let applied = match apply_outcome {
            PlanApplyOutcome::Applied(applied) => applied,
            PlanApplyOutcome::Rejected { reason, .. }
                if reason.starts_with(crate::storage::store::PLAN_NOT_WANTED_PREFIX) =>
            {
                info!(%job_config_id, %reason, "Plan rejected at apply: config no longer wanted");
                timings.total_us = started.elapsed().as_micros() as u64;
                return Ok(PlanOutcome::empty(timings));
            }
            PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            } => {
                let scope_key = format!("job_config:{job_config_id}");
                let expected_epoch = submission
                    .expected_versions
                    .get(&scope_key)
                    .copied()
                    .unwrap_or_default();
                let latest_epoch = latest_versions.get(&scope_key).copied().unwrap_or_default();
                // A rejected attempt is real work the eval paid for (and
                // retries), so it is observed too, up to the apply.
                timings.apply_us = apply_started.elapsed().as_micros() as u64;
                timings.total_us = started.elapsed().as_micros() as u64;
                timings.observe();
                warn!(
                    %job_config_id,
                    submission_id = submission.id,
                    mode = mode.as_str(),
                    apply_plan_submission_ms = apply_started.elapsed().as_millis() as u64,
                    "Scheduler plan submission rejected: {reason}"
                );
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "plan submission rejected: {reason} (expected epoch {}, latest epoch {})",
                        expected_epoch, latest_epoch
                    ),
                }));
            }
        };
        timings.apply_us = apply_started.elapsed().as_micros() as u64;
        let fallbacks = applied
            .candidate_ranks
            .iter()
            .filter(|rank| **rank > 0)
            .count();
        let allocations = applied.allocations;
        let post_apply_started = std::time::Instant::now();
        // The apply outcome IS the ratification for the claims dispatched
        // above: deliver it before anything else, because a `stage`-gated
        // node is holding a prepared workload waiting for exactly this.
        // Matching is by (node, task), so a placement that fell back to
        // another candidate ratifies nothing here by construction.
        crate::manager::dispatcher::dispatch_ratifications(
            self.store,
            &allocations,
            &speculative_claims,
        );
        let deltas = Self::allocations_delta(&affected_nodes, &allocations);
        timings.post_apply_us = post_apply_started.elapsed().as_micros() as u64;
        timings.total_us = started.elapsed().as_micros() as u64;
        timings.observe();

        let outcome = PlanOutcome {
            allocations,
            deltas,
            evictions: plan_evictions,
            timings,
            unplaced,
            held_tasks,
        };
        info!(
            %job_config_id,
            mode = mode.as_str(),
            applied_allocations = outcome.allocations.len(),
            preempted_allocations = outcome.evictions.len(),
            affected_nodes = outcome.deltas.len(),
            fallbacks,
            apply_plan_submission_ms = apply_started.elapsed().as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            available_nodes_us = outcome.timings.available_nodes_us,
            version_sample_us = outcome.timings.version_sample_us,
            task_fetch_us = outcome.timings.task_fetch_us,
            capacity_snapshot_us = outcome.timings.capacity_snapshot_us,
            build_plan_us = outcome.timings.build_plan_us,
            apply_us = outcome.timings.apply_us,
            "Completed scheduler plan_and_apply"
        );
        Ok(outcome)
    }

    /// Allocations of the planned config per task, from the plan's
    /// own-allocation index.
    fn existing_alloc_counts_by_task(own_by_node: &OwnAllocationsByNode) -> HashMap<i64, u32> {
        let mut counts = HashMap::new();
        for task_ids in own_by_node.values() {
            for task_id in task_ids {
                *counts.entry(*task_id).or_default() += 1;
            }
        }
        counts
    }

    fn required_counts_for_mode(
        tasks: &[(i64, TaskConfig, u32)],
        mode: PlanMode,
        own_by_node: &OwnAllocationsByNode,
    ) -> HashMap<i64, u32> {
        if matches!(mode, PlanMode::Fresh) {
            return tasks
                .iter()
                .map(|(id, _config, desired)| (*id, *desired))
                .collect();
        }

        let placed_by_task = Self::existing_alloc_counts_by_task(own_by_node);
        tasks
            .iter()
            .map(|(id, _config, desired)| {
                let placed = placed_by_task.get(id).copied().unwrap_or_default();
                (*id, desired.saturating_sub(placed))
            })
            .collect()
    }

    fn existing_alloc_counts_by_task_node(
        own_by_node: &OwnAllocationsByNode,
    ) -> HashMap<(i64, String), u32> {
        let mut counts = HashMap::new();
        for (node_id, task_ids) in own_by_node {
            for task_id in task_ids {
                *counts.entry((*task_id, node_id.clone())).or_default() += 1;
            }
        }

        counts
    }

    fn count_existing_allocations_in_group(
        existing_alloc_counts_by_task_node: &HashMap<(i64, String), u32>,
        task_id: i64,
        nodes: &[&Node],
    ) -> u32 {
        nodes
            .iter()
            .map(|node| {
                existing_alloc_counts_by_task_node
                    .get(&(task_id, node.id.clone()))
                    .copied()
                    .unwrap_or_default()
            })
            .sum()
    }

    /// The plan's node set: every available node, sorted by id, with its
    /// by-id lookup. One shared `Arc` from the read model's cache, so
    /// taking it is O(1) in fleet size ([`Store::available_nodes`]).
    fn available_nodes(&self) -> R<Arc<AvailableNodes>> {
        self.store.available_nodes()
    }

    /// `applying` is true for a plan that will be applied (as opposed to a
    /// `job plan` preview). Such a plan places what fits and reports the rest
    /// as [`UnplacedTask`]s, and it treats the static ports of the rows a
    /// Fresh plan dropped as still taken, because the apply transaction
    /// still sees those rows (`Store::assign_allocation_network`).
    fn build_plan(
        &self,
        job_config_id: &str,
        node_set: Arc<AvailableNodes>,
        mut tasks: Vec<(i64, TaskConfig, u32)>,
        all_tasks: Vec<(i64, TaskConfig)>,
        required_counts: HashMap<i64, u32>,
        capacity: CapacitySnapshot,
        mode: PlanMode,
        task_targets: Option<&HashMap<i64, u32>>,
        applying: bool,
        preemptor: PreemptorContext<'_>,
    ) -> R<BuiltPlan> {
        tasks.sort_by_key(|a| a.0);
        let mut unplaced: Vec<UnplacedTask> = Vec::new();
        // Preemption state for the whole plan: floors bind across placements,
        // and the victim index loads at most once (and only if a placement
        // actually fails for capacity).
        let mut preemption_pass = PreemptionPass::new(
            preemptor.band,
            preemptor.job,
            job_config_id,
            preemptor.journal_observed,
            &self.preemption,
        );

        let CapacitySnapshot {
            allocations_by_node,
            pools,
            freed_nodes,
            scope_epochs: _,
            own_by_node,
            dropped_ports,
        } = capacity;
        // The body reads allocations by reference throughout (the owned map
        // outlives the plan); the pools move into the ledger below.
        let allocations_by_node = &allocations_by_node;
        // Static host ports: what the snapshot's allocations hold per node,
        // plus what this plan has claimed so far. Empty (and never filled)
        // for a plan whose tasks declare no static port.
        let mut port_ledger = PortLedger::new(if applying {
            dropped_ports
        } else {
            HashMap::new()
        });
        let mut placements = Vec::new();
        let mut index_report: Vec<TaskIndexUsage> = Vec::new();
        // Indexes arm only above the configured node threshold; below it the
        // scan is cheaper than any bookkeeping (docs/scheduling-indexes.md).
        let index_armed = self.store.index_settings.armed_for(node_set.len());
        let mut planned: HashMap<String, usize> = HashMap::new();
        let weights = self.weights;
        let nodes_considered_setting = self.nodes_considered;
        let score_sample_setting = self.score_sample;
        let candidates_setting = self.plan_candidates;
        let mut reschedule_rng = if matches!(mode, PlanMode::Reschedule) {
            Some(reschedule_rng())
        } else {
            None
        };
        let mut nodes_considered_rng =
            StdRng::from_rng(rand::thread_rng()).context("Failed to seed nodes_considered RNG")?;
        let existing_alloc_counts_by_task_node =
            Self::existing_alloc_counts_by_task_node(&own_by_node);
        // The by-id lookup is the cached one: building a map over every
        // available node here, per plan, was O(fleet) on every eval.
        let node_lookup: &AvailableNodes = &node_set;
        let nodes = node_set.as_slice();
        let resource_policy = &self.store.resource_policy;
        let mut resource_ledger = ResourceLedger::from_snapshot(
            pools,
            &freed_nodes,
            node_lookup,
            allocations_by_node,
            resource_policy,
        );
        let mut group_per_key: HashMap<String, Option<String>> = HashMap::new();
        let mut group_sizes: HashMap<String, usize> = HashMap::new();
        // Distinct-constraint keys per task, for the coupling partition below:
        // placements of a task that carries a `distinct` constraint must not
        // fall back independently (the tracker couples their choices).
        let mut distinct_keys_by_task: HashMap<i64, Vec<String>> = HashMap::new();
        // Hard filters (constraints + permit) of every member of a group. A
        // multi-task group is placed on a single node, so that node must
        // satisfy all of them. Arc'd and shared with `build_task_plan` via
        // `parsed_filters`, so each task's constraints/permit parse exactly
        // once per plan.
        let mut group_filters: HashMap<String, Vec<Arc<(ConstraintSet, PermitSet)>>> =
            HashMap::new();
        let mut parsed_filters: HashMap<i64, Arc<(ConstraintSet, PermitSet)>> = HashMap::new();
        let mut task_group_map: HashMap<i64, (String, Option<String>)> = HashMap::new();
        let mut task_id_to_group: HashMap<i64, String> = HashMap::new();
        let task_name_to_id: HashMap<String, i64> = all_tasks
            .iter()
            .map(|(task_id, task_config)| (task_config.name.clone(), *task_id))
            .collect();
        let mut planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();

        for (task_id, task_config) in &all_tasks {
            let per_key = task_config.count.per.clone();
            *group_sizes.entry(task_config.group.clone()).or_default() += 1;
            match group_per_key.get(&task_config.group) {
                Some(existing) if existing.as_deref() != per_key.as_deref() => {
                    bail!(
                        "Group {} mixes count.per values ({:?} vs {:?})",
                        task_config.group,
                        existing,
                        per_key
                    );
                }
                None => {
                    group_per_key.insert(task_config.group.clone(), per_key.clone());
                }
                _ => {}
            }
            // Collected for every task, not just the second and later members:
            // group size is only known once the whole list has been walked, and
            // the first member's filters count just as much as the rest.
            let constraint_set =
                ConstraintSet::new(&task_config.constraints).with_context(|| {
                    format!("Failed to parse constraints for task {}", task_config.name)
                })?;
            distinct_keys_by_task.insert(*task_id, constraint_set.distinct_keys());
            let permit_set = PermitSet::new(&task_config.permit)
                .with_context(|| format!("Failed to parse permit for task {}", task_config.name))?;
            let filters = Arc::new((constraint_set, permit_set));
            group_filters
                .entry(task_config.group.clone())
                .or_default()
                .push(Arc::clone(&filters));
            parsed_filters.insert(*task_id, filters);
            task_group_map.insert(*task_id, (task_config.group.clone(), per_key));
            task_id_to_group.insert(*task_id, task_config.group.clone());
        }

        let mut group_assignments: HashMap<GroupKey, String> = HashMap::new();
        for (node_id, task_ids) in &own_by_node {
            for task_config_id in task_ids {
                let Some((group_name, per_key)) = task_group_map.get(task_config_id) else {
                    continue;
                };
                if group_sizes.get(group_name).copied().unwrap_or(0) <= 1 {
                    continue;
                }
                // Only allocations on nodes that are still placement
                // candidates pin their group. An allocation left behind on a
                // drained or lost node (a fenced stateful task waits out its
                // `onLost.replaceAfter` there) would otherwise pin the group
                // to a node nothing can be placed on, blocking replacement of
                // its every group-mate. Later passes re-converge the group
                // onto whichever candidate node the survivors landed on.
                let Some(node) = node_lookup.get(node_id.as_str()) else {
                    continue;
                };
                let token = if let Some(per_key) = per_key.as_deref() {
                    let value = node_attribute(node, per_key).with_context(|| {
                        format!("Node {} missing attribute {}", node_id, per_key)
                    })?;
                    value_token(value)
                } else {
                    "default".to_string()
                };
                let key = GroupKey {
                    name: group_name.clone(),
                    token,
                };
                if let Some(existing) = group_assignments.get(&key) {
                    if existing != node_id {
                        bail!(
                            "Group {} already assigned to node {} but allocation exists on {}",
                            key.name,
                            existing,
                            node_id
                        );
                    }
                } else {
                    group_assignments.insert(key, node_id.clone());
                }
            }
        }

        let mut group_candidates: HashMap<GroupKey, Vec<&Node>> = HashMap::new();
        for (group_name, filters) in &group_filters {
            if group_sizes.get(group_name).copied().unwrap_or(0) <= 1 {
                continue;
            }
            let per_key = group_per_key
                .get(group_name)
                .and_then(|value| value.clone());
            let mut grouped: HashMap<String, Vec<&Node>> = HashMap::new();
            for node in nodes.iter().map(|node| &**node) {
                if !filters.iter().all(|filters| {
                    filters.0.is_satisfied(node, None) && filters.1.is_permitted(node)
                }) {
                    continue;
                }
                let token = if let Some(per_key) = per_key.as_deref() {
                    let Some(value) = node_attribute(node, per_key) else {
                        continue;
                    };
                    value_token(value)
                } else {
                    "default".to_string()
                };
                grouped.entry(token).or_default().push(node);
            }

            for (token, group_nodes) in grouped {
                group_candidates.insert(
                    GroupKey {
                        name: group_name.clone(),
                        token,
                    },
                    group_nodes,
                );
            }
        }

        for (task_id, task_config, _desired_count) in tasks {
            let needed = required_counts.get(&task_id).copied().unwrap_or(0);
            let per_key = task_config.count.per.as_deref();
            if needed == 0 && per_key.is_none() {
                continue;
            }

            let in_multi_task_group = group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1;
            let mut plan = self.build_task_plan(
                task_id,
                &task_config,
                nodes,
                resource_policy,
                &own_by_node,
                node_lookup,
                needed,
                index_armed,
                in_multi_task_group,
                &parsed_filters,
            )?;

            let base_eligible = plan.base_eligible.clone();
            if let Some(per_key) = per_key {
                let grouped = group_nodes_by_attr(&base_eligible, per_key);
                if grouped.is_empty() {
                    bail!(
                        "Failed to place task {}: no eligible nodes with attribute {}",
                        task_config.name,
                        per_key
                    );
                }

                // Deterministic order so an explicit target budget below is
                // always spent on the same groups.
                let mut grouped = grouped.into_iter().collect::<Vec<_>>();
                grouped.sort_by(|left, right| left.0.cmp(&right.0));

                // `count.per` normally converges each group to `count.num`
                // independently. An explicit target is a total for the task
                // (rollouts stage placements that way), so it has to cap the
                // sum across groups instead — otherwise a canary step places
                // the whole fleet at once.
                let mut target_budget = task_targets
                    .map(|_| required_counts.get(&task_id).copied().unwrap_or_default());

                for (group_name, group) in grouped {
                    let placed = if matches!(mode, PlanMode::Reschedule) {
                        Self::count_existing_allocations_in_group(
                            &existing_alloc_counts_by_task_node,
                            task_id,
                            &group,
                        )
                    } else {
                        0
                    };
                    let mut needed = task_config.count.num.saturating_sub(placed);
                    if let Some(budget) = target_budget.as_mut() {
                        needed = needed.min(*budget);
                        *budget -= needed;
                    }
                    let group_key = GroupKey {
                        name: task_config.group.clone(),
                        token: group_name.to_string(),
                    };
                    let group_key = if group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1
                    {
                        Some(group_key)
                    } else {
                        None
                    };
                    let candidates_for_group = if let Some(group_key) = group_key.as_ref() {
                        group_candidates
                            .get(group_key)
                            .with_context(|| {
                                format!(
                                    "No eligible nodes for group {} ({})",
                                    group_key.name, group_key.token
                                )
                            })?
                            .as_slice()
                    } else {
                        group.as_slice()
                    };
                    self.place_task_allocations(
                        needed,
                        task_id,
                        &task_config,
                        job_config_id,
                        candidates_for_group,
                        allocations_by_node,
                        &own_by_node,
                        &task_name_to_id,
                        &task_id_to_group,
                        &mut planned_task_allocations,
                        &mut planned,
                        &mut reschedule_rng,
                        nodes_considered_setting.as_ref(),
                        score_sample_setting.as_ref(),
                        &mut nodes_considered_rng,
                        node_lookup,
                        &mut resource_ledger,
                        &mut plan,
                        &mut placements,
                        weights,
                        group_key.as_ref(),
                        &mut group_assignments,
                        candidates_setting,
                        &mut preemption_pass,
                        &mut port_ledger,
                        applying.then_some(&mut unplaced),
                    )?;
                }
            } else {
                let group_key = GroupKey {
                    name: task_config.group.clone(),
                    token: "default".to_string(),
                };
                let group_key = if group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1 {
                    Some(group_key)
                } else {
                    None
                };
                let candidates_for_group = if let Some(group_key) = group_key.as_ref() {
                    group_candidates
                        .get(group_key)
                        .with_context(|| {
                            format!(
                                "No eligible nodes for group {} ({})",
                                group_key.name, group_key.token
                            )
                        })?
                        .as_slice()
                } else {
                    base_eligible.as_slice()
                };
                self.place_task_allocations(
                    needed,
                    task_id,
                    &task_config,
                    job_config_id,
                    candidates_for_group,
                    allocations_by_node,
                    &own_by_node,
                    &task_name_to_id,
                    &task_id_to_group,
                    &mut planned_task_allocations,
                    &mut planned,
                    &mut reschedule_rng,
                    nodes_considered_setting.as_ref(),
                    score_sample_setting.as_ref(),
                    &mut nodes_considered_rng,
                    node_lookup,
                    &mut resource_ledger,
                    &mut plan,
                    &mut placements,
                    weights,
                    group_key.as_ref(),
                    &mut group_assignments,
                    candidates_setting,
                    &mut preemption_pass,
                    &mut port_ledger,
                    applying.then_some(&mut unplaced),
                )?;
            }
            index_report.push(plan.index_usage.clone());
        }

        Self::decouple_plan_components(
            &mut placements,
            &distinct_keys_by_task,
            &task_id_to_group,
            &group_sizes,
        );

        Ok(BuiltPlan {
            placements,
            index_report,
            evictions: preemption_pass.victims,
            observed_preemptions: preemption_pass.observed,
            unplaced,
        })
    }

    /// Partition the plan's placements into coupling components and shrink
    /// every non-singleton component's candidate list to just its chosen
    /// node, so the apply walk can never invalidate the planner's combined
    /// fit math (docs/late-binding-plan-apply.md).
    ///
    /// Coupling edges:
    /// - **a task with a `distinct` constraint** — the tracker couples that
    ///   task's placements even across disjoint node lists (two nodes can
    ///   share a zone);
    /// - **membership in the same multi-task co-placement group** — the
    ///   group is one selection unit.
    ///
    /// **Overlapping candidate lists are NOT an edge.** They were, on the
    /// theory that two independent fallbacks onto a shared node could
    /// oversubscribe it — but the apply walk debits each bind from the
    /// node's in-transaction free pool before the next placement is
    /// considered (`apply_plan_submission_on`), so the second placement
    /// sees the capacity the first spent and skips the node if it no
    /// longer fits; static ports are re-read there too. Keeping the edge
    /// cost far more than it bought: any multi-placement job into one pool
    /// overlaps by construction, so *every* placement of it collapsed to a
    /// one-element list and the plan degenerated to the pre-candidate
    /// all-or-nothing CAS. That is what drove plan rejection to 73.6% on
    /// the 5000-node benchmark
    /// (internal_docs/plans/plan-apply-conflict-reduction-plan.md).
    ///
    /// Singleton components keep their fallback lists and walk
    /// independently at apply; components of size >1 keep the
    /// all-or-nothing epoch CAS on their exact chosen nodes.
    fn decouple_plan_components(
        placements: &mut [PlannedAllocation],
        distinct_keys_by_task: &HashMap<i64, Vec<String>>,
        task_id_to_group: &HashMap<i64, String>,
        group_sizes: &HashMap<String, usize>,
    ) {
        if placements.len() <= 1 {
            return;
        }

        fn find(parent: &mut [usize], mut index: usize) -> usize {
            while parent[index] != index {
                parent[index] = parent[parent[index]];
                index = parent[index];
            }
            index
        }
        fn union(parent: &mut [usize], left: usize, right: usize) {
            let left = find(parent, left);
            let right = find(parent, right);
            if left != right {
                parent[right] = left;
            }
        }

        let mut parent: Vec<usize> = (0..placements.len()).collect();
        {
            let mut by_distinct_task: HashMap<i64, usize> = HashMap::new();
            let mut by_group: HashMap<&str, usize> = HashMap::new();
            for (index, placement) in placements.iter().enumerate() {
                let has_distinct = distinct_keys_by_task
                    .get(&placement.task_config_id)
                    .is_some_and(|keys| !keys.is_empty());
                if has_distinct {
                    match by_distinct_task.entry(placement.task_config_id) {
                        Entry::Occupied(entry) => union(&mut parent, *entry.get(), index),
                        Entry::Vacant(entry) => {
                            entry.insert(index);
                        }
                    }
                }
                if let Some(group) = task_id_to_group.get(&placement.task_config_id) {
                    if group_sizes.get(group).copied().unwrap_or(0) > 1 {
                        match by_group.entry(group.as_str()) {
                            Entry::Occupied(entry) => union(&mut parent, *entry.get(), index),
                            Entry::Vacant(entry) => {
                                entry.insert(index);
                            }
                        }
                    }
                }
            }
        }

        let mut component_sizes: HashMap<usize, usize> = HashMap::new();
        for index in 0..placements.len() {
            let root = find(&mut parent, index);
            *component_sizes.entry(root).or_default() += 1;
        }
        for (index, placement) in placements.iter_mut().enumerate() {
            let root = find(&mut parent, index);
            if component_sizes.get(&root).copied().unwrap_or(0) > 1 {
                placement.candidates = vec![placement.node_id.clone()];
            }
        }
    }

    /// Budget for a truncated (index-served) candidate list: a generous
    /// multiple of what the task still needs, floored so small counts still
    /// see real choice before scoring, and never narrower than the pool the
    /// fallback tail samples from — truncating below that would hand every
    /// concurrent planner the same few nodes again
    /// ([`Self::fallback_pool_width`]).
    fn candidate_budget(&self, needed: u32) -> usize {
        (needed as usize)
            .saturating_mul(8)
            .max(64)
            .max(self.fallback_pool_width())
    }

    /// Today's authoritative candidate filter: constraints, permit, and
    /// volume-driver support over every available node. The permanent
    /// fallback the scheduling index is measured against.
    fn scan_base_eligible<'b>(
        nodes: impl IntoIterator<Item = &'b Node>,
        constraint_set: &ConstraintSet,
        permit_set: &PermitSet,
        required_drivers: &std::collections::BTreeSet<String>,
    ) -> Vec<&'b Node> {
        nodes
            .into_iter()
            .filter(|node| {
                constraint_set.is_satisfied(node, None)
                    && permit_set.is_permitted(node)
                    && node_provides_volume_drivers(node, required_drivers)
            })
            .collect()
    }

    /// Candidate construction through the scheduling index
    /// (docs/scheduling-indexes.md). Every index candidate is re-verified
    /// with exactly the scan's predicate, so a stale or wrong index costs
    /// coverage, never correctness — and empty coverage falls straight back
    /// to the scan. Untruncated results are re-sorted by node id so
    /// index-served planning is byte-identical to scan planning; truncation
    /// keeps the index's nearest-fit-first order.
    fn indexed_base_eligible<'b>(
        &self,
        nodes: &'b [Arc<Node>],
        node_lookup: &'b AvailableNodes,
        constraint_set: &ConstraintSet,
        permit_set: &PermitSet,
        task_request: &ResourceRequest,
        required_drivers: &std::collections::BTreeSet<String>,
        needed: u32,
        allow_truncation: bool,
        usage: &mut TaskIndexUsage,
    ) -> R<Vec<&'b Node>> {
        let metrics = crate::manager::telemetry::metrics();

        // Partition bindings: the task's `=` constraints plus the pool its
        // permit pins. A `pool` partition key always means the node's pool
        // column, so only the permit may bind it.
        let mut bindings = constraint_set.equality_bindings();
        bindings.remove("pool");
        if let Some(pool) = permit_set.pool_binding() {
            bindings.insert("pool".to_string(), pool.clone());
        }
        let request_amounts: HashMap<String, f64> = task_request
            .values
            .iter()
            .filter_map(|(name, value)| match value {
                ResourceValue::Amount(amount) => Some((name.clone(), *amount)),
                ResourceValue::Items(_) => None,
            })
            .collect();

        // Truncatable tasks take the budgeted walk: verification happens
        // inside the index walk and stops one eligible node past the
        // budget, so the cost is O(budget ÷ selectivity) instead of O(shard)
        // — and the shard of the default `fit` index is the whole pool
        // (plan-path-fleet-scaling plan § 6.2). Same output distribution as
        // the verify-everything-then-`truncate_within_boundary_cell` path
        // below, which still serves untruncated tasks and shadow mode.
        if allow_truncation {
            let budget = self.candidate_budget(needed);
            let walked =
                self.store
                    .scheduling_index_walk(&bindings, &request_amounts, budget, |id| {
                        node_lookup.get(id).filter(|node| {
                            constraint_set.is_satisfied(node, None)
                                && permit_set.is_permitted(node)
                                && node_provides_volume_drivers(node, required_drivers)
                        })
                    })?;
            if let Ok(walk) = walked {
                if walk.items.is_empty() {
                    // Same as the full path's empty case below: one scan
                    // tells "nothing matches" from "the index under-covered".
                    let scan = Self::scan_base_eligible(
                        node_refs(nodes),
                        constraint_set,
                        permit_set,
                        required_drivers,
                    );
                    usage.index = Some(walk.index.clone());
                    usage.mode = Some(IndexMode::On.as_str());
                    if scan.is_empty() {
                        metrics
                            .scheduler_index_queries_total
                            .inc(&[walk.index.as_str(), "used"]);
                    } else {
                        usage.fell_back = true;
                        metrics
                            .scheduler_index_queries_total
                            .inc(&[walk.index.as_str(), "fell_back"]);
                        warn!(
                            index = %walk.index,
                            scan_count = scan.len(),
                            "Scheduling index found no eligible candidates but the scan did; using the scan"
                        );
                    }
                    return Ok(scan);
                }
                usage.index = Some(walk.index.clone());
                usage.mode = Some(IndexMode::On.as_str());
                metrics
                    .scheduler_index_queries_total
                    .inc(&[walk.index.as_str(), "used"]);
                let mut verified = walk.items;
                if walk.truncated {
                    usage.truncated = true;
                } else {
                    verified.sort_by(|a, b| a.id.cmp(&b.id));
                }
                metrics
                    .scheduler_index_candidates
                    .observe(&[walk.index.as_str()], verified.len() as f64);
                return Ok(verified);
            }
            // Not walkable (disabled, unusable, empty partition, shadow):
            // the full query below handles every one of those exactly as
            // before.
        }

        // Ordering matters only when the verified list may be truncated to
        // the best-first budget below; the untruncated path re-sorts by
        // node id, so the shard's nearest-fit cell sort would be wasted.
        let candidates = match self.store.scheduling_index_query(
            &bindings,
            &request_amounts,
            None,
            allow_truncation,
        )? {
            IndexQueryOutcome::Disabled => {
                return Ok(Self::scan_base_eligible(
                    node_refs(nodes),
                    constraint_set,
                    permit_set,
                    required_drivers,
                ));
            }
            IndexQueryOutcome::Unusable { reason } => {
                metrics
                    .scheduler_index_queries_total
                    .inc(&["none", "unusable"]);
                usage.reason = Some(reason);
                return Ok(Self::scan_base_eligible(
                    node_refs(nodes),
                    constraint_set,
                    permit_set,
                    required_drivers,
                ));
            }
            IndexQueryOutcome::Candidates(candidates) => candidates,
        };

        // Each candidate keeps the rank of the index cell it came from, so
        // the truncation below can tell "better cell" from "same cell":
        // nodes in one cell are indistinguishable to the index, and a cut
        // that took the first `budget` of them (sorted by id) handed every
        // plan the same lowest-id nodes — the one-slice concentration of
        // aws-phase5-findings.md. An unordered query carries no ranks; every
        // node then shares rank 0, and the untruncated path re-sorts by id.
        let ranked: Vec<(&'b Node, u32)> = candidates
            .node_ids
            .iter()
            .enumerate()
            .filter_map(|(position, id)| {
                node_lookup.get(id.as_str()).map(|node| {
                    (
                        node,
                        candidates.cell_ranks.get(position).copied().unwrap_or(0),
                    )
                })
            })
            .filter(|(node, _)| {
                constraint_set.is_satisfied(node, None)
                    && permit_set.is_permitted(node)
                    && node_provides_volume_drivers(node, required_drivers)
            })
            .collect();
        let mut verified: Vec<&'b Node> = ranked.iter().map(|(node, _)| *node).collect();

        if candidates.mode == IndexMode::Shadow {
            // The scan stays authoritative; the index runs alongside and any
            // node it missed is a disagreement worth shouting about.
            let scan = Self::scan_base_eligible(
                node_refs(nodes),
                constraint_set,
                permit_set,
                required_drivers,
            );
            let scan_ids: HashSet<&str> = scan.iter().map(|node| node.id.as_str()).collect();
            let index_ids: HashSet<&str> = verified.iter().map(|node| node.id.as_str()).collect();
            let agreement = scan_ids == index_ids;
            usage.index = Some(candidates.index.clone());
            usage.mode = Some(IndexMode::Shadow.as_str());
            usage.shadow_agreement = Some(agreement);
            metrics
                .scheduler_index_queries_total
                .inc(&[candidates.index.as_str(), "shadow"]);
            metrics.scheduler_index_shadow_total.inc(&[
                candidates.index.as_str(),
                if agreement { "agree" } else { "disagree" },
            ]);
            if !agreement {
                let missing: Vec<&&str> = scan_ids.difference(&index_ids).take(3).collect();
                warn!(
                    index = %candidates.index,
                    scan_count = scan_ids.len(),
                    index_count = index_ids.len(),
                    missing_sample = ?missing,
                    "Scheduling index shadow disagreement: index candidates diverge from the scan"
                );
            }
            return Ok(scan);
        }

        if verified.is_empty() {
            // Either nothing matches (the scan agrees and this plan fails
            // with the usual diagnostic) or the index under-covered; one
            // scan on the empty path tells them apart and keeps the scan's
            // answer authoritative either way.
            let scan = Self::scan_base_eligible(
                node_refs(nodes),
                constraint_set,
                permit_set,
                required_drivers,
            );
            usage.index = Some(candidates.index.clone());
            usage.mode = Some(IndexMode::On.as_str());
            if scan.is_empty() {
                metrics
                    .scheduler_index_queries_total
                    .inc(&[candidates.index.as_str(), "used"]);
            } else {
                usage.fell_back = true;
                metrics
                    .scheduler_index_queries_total
                    .inc(&[candidates.index.as_str(), "fell_back"]);
                warn!(
                    index = %candidates.index,
                    scan_count = scan.len(),
                    "Scheduling index found no eligible candidates but the scan did; using the scan"
                );
            }
            return Ok(scan);
        }

        usage.index = Some(candidates.index.clone());
        usage.mode = Some(IndexMode::On.as_str());
        metrics
            .scheduler_index_queries_total
            .inc(&[candidates.index.as_str(), "used"]);
        let budget = self.candidate_budget(needed);
        if allow_truncation && verified.len() > budget {
            verified = crate::scheduling_index::truncate_within_boundary_cell(ranked, budget);
            usage.truncated = true;
        } else {
            verified.sort_by(|a, b| a.id.cmp(&b.id));
        }
        metrics
            .scheduler_index_candidates
            .observe(&[candidates.index.as_str()], verified.len() as f64);
        Ok(verified)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_task_plan<'b>(
        &self,
        task_id: i64,
        task_config: &'b TaskConfig,
        nodes: &'b [Arc<Node>],
        resource_policy: &ResourcePolicy,
        own_by_node: &OwnAllocationsByNode,
        node_lookup: &'b AvailableNodes,
        needed: u32,
        index_armed: bool,
        in_multi_task_group: bool,
        parsed_filters: &HashMap<i64, Arc<(ConstraintSet, PermitSet)>>,
    ) -> R<TaskPlan<'b>> {
        let task_resources =
            resource_policy.normalize_task_resources(&task_config.resources, true)?;
        let task_request = ResourceRequest::from_value(&task_resources)?;

        // Parsed once per plan by `build_plan`'s group-filter pass;
        // re-parsing here (the old shape) doubled the per-task JSON walk.
        let filters = match parsed_filters.get(&task_id) {
            Some(filters) => Arc::clone(filters),
            None => Arc::new((
                ConstraintSet::new(&task_config.constraints).with_context(|| {
                    format!("Failed to parse constraints for task {}", task_config.name)
                })?,
                PermitSet::new(&task_config.permit).with_context(|| {
                    format!("Failed to parse permit for task {}", task_config.name)
                })?,
            )),
        };
        let (constraint_set, permit_set) = (&filters.0, &filters.1);
        let affinity_set = AffinitySet::new(&task_config.affinities)
            .with_context(|| format!("Failed to parse affinities for task {}", task_config.name))?;
        let distinct_tracker =
            Self::build_distinct_tracker(constraint_set, task_id, own_by_node, node_lookup);
        // Storage is a hard placement requirement: a node without the task's
        // volume driver (or with it installed-but-unusable) can never run the
        // task, so filter it out here rather than letting the task start and
        // fail at mount time.
        let required_drivers = task_config.required_volume_drivers();

        let mut index_usage = TaskIndexUsage::scan(&task_config.name);
        // Truncating candidates is only safe when nothing downstream needs
        // the full set: `count.per` targets every attribute group, `distinct`
        // constraints need diversity, volume-pinned tasks need their exact
        // node, and multi-task groups place from their own candidate sets.
        // Stateful tasks can carry pins too (a node-local `stateful.storage`
        // volume, or a restore from a `local` snapshot), and a truncated list
        // that leaves out the pinned node fails the placement as if the node
        // were gone.
        let allow_truncation = index_armed
            && task_config.count.per.is_none()
            && constraint_set.distinct_keys().is_empty()
            && task_config.volumes.is_empty()
            && task_config.stateful.is_none()
            && !in_multi_task_group;

        let base_eligible = if index_armed {
            self.indexed_base_eligible(
                nodes,
                node_lookup,
                constraint_set,
                permit_set,
                &task_request,
                &required_drivers,
                needed,
                allow_truncation,
                &mut index_usage,
            )?
        } else {
            Self::scan_base_eligible(
                node_refs(nodes),
                constraint_set,
                permit_set,
                &required_drivers,
            )
        };
        index_usage.candidates = base_eligible.len();
        if base_eligible.is_empty() && !required_drivers.is_empty() {
            let missing = required_drivers
                .iter()
                .filter(|plugin| {
                    !nodes
                        .iter()
                        .any(|node| node_provides_volume_drivers(node, &[(*plugin).clone()].into()))
                })
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                bail!(
                    "Failed to place task {}: no node provides volume driver(s) {}. \
                     Install them under plugins/volumes/<name>/ on a worker, or check \
                     `legion volume drivers` for why an installed one is unusable",
                    task_config.name,
                    missing.join(", ")
                );
            }
        }

        Ok(TaskPlan {
            filters,
            affinity_set,
            task_request,
            base_eligible,
            distinct_tracker,
            index_usage,
        })
    }

    fn place_task_allocations<'b>(
        &self,
        count: u32,
        task_id: i64,
        task_config: &TaskConfig,
        job_config_id: &str,
        candidates: &[&'b Node],
        allocations_by_node: &SharedNodeAllocations,
        own_by_node: &OwnAllocationsByNode,
        task_name_to_id: &HashMap<String, i64>,
        task_id_to_group: &HashMap<i64, String>,
        planned_task_allocations: &mut HashMap<String, Vec<i64>>,
        planned: &mut HashMap<String, usize>,
        reschedule_rng: &mut Option<StdRng>,
        nodes_considered_setting: Option<&NodesConsidered>,
        score_sample_setting: Option<&ScoreSample>,
        nodes_considered_rng: &mut StdRng,
        node_lookup: &'b AvailableNodes,
        resource_ledger: &mut ResourceLedger,
        plan: &mut TaskPlan<'b>,
        placements: &mut Vec<PlannedAllocation>,
        weights: SchedulerWeights,
        group_key: Option<&GroupKey>,
        group_assignments: &mut HashMap<GroupKey, String>,
        candidates_setting: PlanCandidatesSetting,
        preemption: &mut PreemptionPass,
        port_ledger: &mut PortLedger,
        mut unplaced: Option<&mut Vec<UnplacedTask>>,
    ) -> R<()> {
        // Static host ports this task claims on its node. A node already
        // holding one of them (in the snapshot, or claimed earlier in this
        // plan) is not a candidate: the apply transaction would refuse the
        // placement there, and before this check the planner kept choosing
        // it, so the plan was rejected and the eval requeued forever.
        let static_ports = task_static_ports(task_config);
        // A placement no node can take is recorded here (when the plan may
        // place partially and the task is not part of a co-placed group)
        // instead of failing the whole plan.
        let partial = group_key.is_none() && unplaced.is_some();
        // Owned so the truncated-index refill below can widen it mid-task.
        let mut candidates: Vec<&'b Node> = candidates.to_vec();
        let mut scoring_cache = PlanScoringCache::from_plan(
            &candidates,
            task_id,
            task_config,
            &plan.affinity_set,
            allocations_by_node,
            own_by_node,
            task_name_to_id,
            task_id_to_group,
            planned_task_allocations,
            planned,
            resource_ledger,
            &plan.task_request,
        );
        // Content locality: computed once per task, from the in-memory
        // location index. Skipped (no cost beyond walking the task's
        // artifacts) when the weight is 0 or the content set is empty.
        // Instances holding node-local storage can only be replaced on the node
        // their data lives on. Pins are consumed one per placement, in slot
        // order; the allocation binder independently prefers the instance whose
        // volume is pinned to the node an allocation landed on, so the two
        // agree even if their orderings differ (see docs/volumes.md).
        let mut pins: std::collections::VecDeque<(String, String)> = self
            .store
            .pinned_placements_for_task(task_id)
            .unwrap_or_else(|err| {
                warn!(task_id, error = ?err, "Failed loading volume pins; placing unpinned");
                Vec::new()
            })
            .into();

        // Instances restoring from a snapshot (a resume, RestoreInstance, or
        // any instance created with `restore_from`). A `local` snapshot pins
        // the instance to the node holding its bytes and is checked against
        // the manifest's `compat` there. A `published` one has no pin: its
        // `compat` filters every candidate, and the locality term prefers
        // nodes holding its parts. Only keyed tasks can restore, so every
        // other task skips the lookup entirely.
        let restores = if task_config
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Keyed)
        {
            self.store
                .restore_pins_for_task(task_id)
                .unwrap_or_else(|err| {
                    warn!(task_id, error = ?err, "Failed loading restore pins; placing unpinned");
                    Vec::new()
                })
        } else {
            Vec::new()
        };
        // Score locality after the restore lookup, so a restoring instance's
        // manifest parts count toward the content set.
        let locality = self.locality_scores(task_config, &restores, weights);
        scoring_cache.apply_locality(&candidates, locality.as_ref());

        let mut restore_manifests: HashMap<String, crate::storage::store::RestorePin> =
            HashMap::new();
        // Published restores with no volume pin, placed after the hard
        // restore pins and before the volume pins, which is the order the
        // binder pairs them in (restoring instances first).
        let mut floating_restores: std::collections::VecDeque<String> =
            std::collections::VecDeque::new();
        let mut hard_restore_pins = 0usize;
        for restore in restores {
            let volume_pinned = pins
                .iter()
                .any(|(instance, _)| *instance == restore.instance_id);
            if !volume_pinned {
                if let Some(node_id) = restore.hard_pin() {
                    // Restores are rare and recent: place them first, where
                    // the binder (which prefers restoring instances on their
                    // snapshot's node) will pair them up.
                    pins.push_front((restore.instance_id.clone(), node_id.to_string()));
                    hard_restore_pins += 1;
                } else {
                    floating_restores.push_back(restore.instance_id.clone());
                }
            }
            restore_manifests.insert(restore.instance_id.clone(), restore);
        }

        // Disk accounting for node-local volumes, within this plan. Placing 3
        // replicas that each provision a 100G local volume onto a node with
        // 150G free must fail the third placement, not fill the disk at
        // runtime. Deliberately per-plan and best-effort (fingerprinted free
        // space, no cross-plan reservation ledger): the durable guard is the
        // fingerprint itself, which shrinks as volumes fill. Remote volumes
        // (nfs) never count — their bytes live on the storage server, and
        // charging them to the node would block placements for no reason.
        let mut planned_local_bytes: HashMap<String, u64> = HashMap::new();
        // Checkpoint headroom (docs/suspend-and-resume.md, "Capacity for
        // resumes"): a new instance of a task that suspends to memory skips
        // nodes with no room for one more of its checkpoints, whether the
        // `snapshots` content budget or the disk under it is what is short.
        // Read once per task from the in-memory content index, and empty (no
        // per-node work at all) unless a node is short.
        let budget_short: Option<(
            u64,
            HashMap<String, crate::storage::content_index::RoomLimit>,
        )> = crate::structs::suspend::expected_memory_checkpoint_bytes(task_config).and_then(
            |needed| {
                let short = self
                    .store
                    .content_locations()
                    .snapshot_budget_short_nodes(needed);
                (!short.is_empty()).then_some((needed, short))
            },
        );
        // Spread forks (`instance fork --spread`): per fork checkpoint, the
        // nodes its forks run on or were placed on in this plan. Loaded on
        // first use, so a plan with no spread fork never reads it.
        let mut spread_nodes: HashMap<String, HashSet<String>> = HashMap::new();

        for placement_index in 0..count {
            let mut eligible = Self::eligible_nodes(
                &candidates,
                plan.constraint_set(),
                plan.distinct_tracker.as_ref(),
                &scoring_cache,
            );

            // A truncated (index-served) candidate list can run dry while the
            // fleet still has room. Widen once to the authoritative scan and
            // continue — only the single-task path truncates, so group
            // pinning is never disturbed.
            if eligible.is_empty() && plan.index_usage.truncated && group_key.is_none() {
                let required_drivers = task_config.required_volume_drivers();
                let mut widened = Self::scan_base_eligible(
                    node_refs(node_lookup.as_slice()),
                    plan.constraint_set(),
                    plan.permit_set(),
                    &required_drivers,
                );
                widened.sort_by(|a, b| a.id.cmp(&b.id));
                debug!(
                    task = %task_config.name,
                    truncated_count = candidates.len(),
                    widened_count = widened.len(),
                    "Scheduling index candidates exhausted; refilling from full scan"
                );
                plan.index_usage.truncated = false;
                plan.index_usage.refills += 1;
                plan.index_usage.fell_back = true;
                crate::manager::telemetry::metrics()
                    .scheduler_index_queries_total
                    .inc(&[
                        plan.index_usage.index.as_deref().unwrap_or("none"),
                        "refilled",
                    ]);
                candidates = widened;
                scoring_cache = PlanScoringCache::from_plan(
                    &candidates,
                    task_id,
                    task_config,
                    &plan.affinity_set,
                    allocations_by_node,
                    own_by_node,
                    task_name_to_id,
                    task_id_to_group,
                    planned_task_allocations,
                    planned,
                    resource_ledger,
                    &plan.task_request,
                );
                scoring_cache.apply_locality(&candidates, locality.as_ref());
                eligible = Self::eligible_nodes(
                    &candidates,
                    plan.constraint_set(),
                    plan.distinct_tracker.as_ref(),
                    &scoring_cache,
                );
            }

            // Static ports: drop every node where one of this task's static
            // ports is already held. Remember the first port that excluded a
            // node, so a failure below can name it.
            let mut port_conflict: Option<u16> = None;
            if !static_ports.is_empty() {
                eligible.retain(|node| {
                    match port_ledger.conflict(node.id.as_str(), &static_ports, allocations_by_node)
                    {
                        Some(port) => {
                            port_conflict.get_or_insert(port);
                            false
                        }
                        None => true,
                    }
                });
            }

            // Preemption: the fleet has nodes this task is *allowed* on, they
            // are simply full. Ask whether displacing lower-band work would
            // open one. Runs only here — after normal feasibility has already
            // failed — which is what makes the feature free for clusters that
            // never use it.
            let mut preempted_node: Option<String> = None;
            if eligible.is_empty() {
                let mut full_but_allowed = Self::preemption_candidate_nodes(
                    &candidates,
                    plan.constraint_set(),
                    plan.permit_set(),
                    plan.distinct_tracker.as_ref(),
                );
                // Evicting work frees capacity, not a port another job holds:
                // a port-blocked node is no preemption target.
                if !static_ports.is_empty() {
                    full_but_allowed.retain(|node| {
                        port_ledger
                            .conflict(node.id.as_str(), &static_ports, allocations_by_node)
                            .is_none()
                    });
                }
                if let Some(chosen) = self.plan_preemption_for_placement(
                    &full_but_allowed,
                    task_config,
                    &plan.task_request,
                    resource_ledger,
                    group_key
                        .and_then(|key| group_assignments.get(key))
                        .cloned(),
                    // The pin this placement will take: none when a
                    // published restore (which has no node) goes next.
                    if hard_restore_pins == 0 && !floating_restores.is_empty() {
                        None
                    } else {
                        pins.front().map(|(_, node_id)| node_id.clone())
                    },
                    preemption,
                )? {
                    // Book the victims against the ledger so the placement
                    // below sees the capacity they free, and so later
                    // placements in this same plan cannot spend it twice.
                    for victim in &chosen.victims {
                        resource_ledger.release(&chosen.node_id, &victim.request);
                        *preemption
                            .taken_by_task
                            .entry(victim.task_config_id)
                            .or_default() += 1;
                    }
                    // Retire the chosen victims from the index. A task placing
                    // several allocations runs this pass once per placement,
                    // and without this the SAME allocation could be chosen
                    // again for the next one — freeing its capacity twice on
                    // paper and evicting one allocation to pay for two.
                    preemption.retire_victims(&chosen.node_id, &chosen.victims);
                    if let Some(chosen_node) = node_lookup.get(chosen.node_id.as_str()) {
                        scoring_cache.refresh_node(
                            chosen_node,
                            resource_ledger,
                            &plan.task_request,
                        );
                    }
                    preemption
                        .victims
                        .extend(chosen.victims.iter().map(|victim| PlannedPreemption {
                            allocation_id: victim.allocation_id,
                            node_id: victim.node_id.clone(),
                            job_id: victim.job_id.clone(),
                            job_config_id: victim.job_config_id.clone(),
                            task_config_id: victim.task_config_id,
                            task_name: victim.task_name.clone(),
                            band: victim.band,
                            kill_after_secs: victim.kill_after_secs,
                        }));
                    eligible = full_but_allowed
                        .iter()
                        .copied()
                        .filter(|node| node.id == chosen.node_id)
                        .collect();
                    preempted_node = Some(chosen.node_id);
                }
            }
            if let Some(node_id) = preempted_node.as_deref() {
                debug!(
                    task = %task_config.name,
                    node_id,
                    victims = preemption.victims.len(),
                    "Placement will preempt to fit"
                );
            }

            if eligible.is_empty() {
                let reason = match port_conflict {
                    Some(port) => format!(
                        "Failed to place task {}: static port {port} is already in use on every \
                         node it is allowed on that has room for it. A static port fits one \
                         allocation per node: add nodes, lower the count, or use a dynamic port",
                        task_config.name
                    ),
                    None => preemption.no_placement_error(task_config),
                };
                return Self::record_unplaced(
                    partial,
                    &mut unplaced,
                    UnplacedTask {
                        task_config_id: task_id,
                        task_name: task_config.name.clone(),
                        count: count - placement_index,
                        reason,
                        static_port: port_conflict,
                    },
                );
            }

            // Disk fit: drop nodes whose remaining free disk (minus what this
            // plan already put there) cannot hold the task's node-local
            // volumes. Nodes that don't report disk are not excluded — missing
            // data is not evidence of a full disk.
            let eligible: Vec<&Node> = eligible
                .into_iter()
                .filter(|node| {
                    let wanted = task_local_volume_bytes(node, task_config);
                    if wanted == 0 {
                        return true;
                    }
                    match fingerprint_disk_free_bytes(node) {
                        None => true,
                        Some(free) => {
                            let already = planned_local_bytes
                                .get(node.id.as_str())
                                .copied()
                                .unwrap_or(0);
                            free.saturating_sub(already) >= wanted
                        }
                    }
                })
                .collect();
            if eligible.is_empty() {
                bail!(
                    "Failed to place task {}: no node has enough free disk for its node-local \
                     volume(s). Shrink the volume `size`, add disk, or use a network driver \
                     (nfs), whose storage does not consume node disk",
                    task_config.name
                );
            }

            // A pinned instance is not a preference: placing it elsewhere would
            // silently start the workload on empty storage, so failing the eval
            // with a diagnostic is the correct outcome.
            let (pin, floating) = if hard_restore_pins > 0 {
                hard_restore_pins -= 1;
                (pins.pop_front(), None)
            } else if let Some(instance_id) = floating_restores.pop_front() {
                (None, Some(instance_id))
            } else {
                (pins.pop_front(), None)
            };
            // A restore's node was chosen for that instance (its pin, or its
            // `compat` and spread), so the apply must bind that instance.
            let bind_instance = floating.clone().or_else(|| {
                pin.as_ref()
                    .filter(|(instance_id, _)| restore_manifests.contains_key(instance_id))
                    .map(|(instance_id, _)| instance_id.clone())
            });
            if let Some((instance_id, node_id)) = pin.as_ref() {
                if let Some(restore) = restore_manifests.get(instance_id) {
                    // A memory checkpoint resumes a running guest, so the
                    // node must match the CPU and VMM it was captured on.
                    // Failing the placement with the attribute that differs
                    // is the outcome; a corrupt guest never is.
                    check_restore_compat(task_config, restore, node_id, node_lookup)?;
                }
            }
            let floating_restore = floating
                .as_ref()
                .and_then(|instance_id| restore_manifests.get(instance_id));
            let eligible = match floating_restore {
                Some(restore) => filter_restore_compat(task_config, restore, eligible)?,
                None => eligible,
            };
            // A spread fork never shares a node with a sibling. A hard rule,
            // like `distinct`: with no other node left the fork waits.
            let spread_snapshot = floating_restore
                .filter(|restore| restore.spread)
                .map(|restore| restore.snapshot_id.clone());
            let eligible = match spread_snapshot.as_deref() {
                None => eligible,
                Some(snapshot_id) => {
                    let taken = match spread_nodes.entry(snapshot_id.to_string()) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => entry.insert(
                            self.store.nodes_running_forks_of(snapshot_id).unwrap_or_else(
                                |err| {
                                    warn!(
                                        snapshot_id,
                                        error = ?err,
                                        "Failed loading spread fork nodes; spreading within this plan only"
                                    );
                                    HashSet::new()
                                },
                            ),
                        ),
                    };
                    let spread: Vec<&Node> = eligible
                        .into_iter()
                        .filter(|node| !taken.contains(node.id.as_str()))
                        .collect();
                    if spread.is_empty() {
                        // Not an error for the rest of the task: its other
                        // placements go ahead, and this fork stays pending
                        // until a node without a sibling can take it.
                        let mut nodes: Vec<&str> = taken.iter().map(String::as_str).collect();
                        nodes.sort_unstable();
                        info!(
                            task = %task_config.name,
                            instance_id = floating.as_deref().unwrap_or_default(),
                            snapshot_id,
                            siblings_on = %nodes.join(", "),
                            "Spread fork waits: every eligible node runs a sibling"
                        );
                        continue;
                    }
                    spread
                }
            };
            // Only a fresh placement is constrained: a restore already has
            // its checkpoint, and a volume-pinned instance has no other node.
            let eligible = match budget_short.as_ref() {
                Some((needed, short)) if pin.is_none() && floating.is_none() => {
                    let before = eligible.len();
                    let roomy: Vec<&Node> = eligible
                        .iter()
                        .copied()
                        .filter(|node| !short.contains_key(node.id.as_str()))
                        .collect();
                    if roomy.is_empty() {
                        use crate::storage::content_index::RoomLimit;
                        let on_disk = eligible
                            .iter()
                            .filter(|node| short.get(node.id.as_str()) == Some(&RoomLimit::Disk))
                            .count();
                        let on_budget = before - on_disk;
                        bail!(
                            "Failed to place task {}: {before} eligible node(s), but none has \
                             {} MiB of room for this task's memory checkpoint (suspend): \
                             {on_budget} short on the snapshots content budget, {on_disk} short \
                             on disk (free space plus evictable content, less \
                             content.minFreeDisk). Raise content.budgets.snapshots or free disk \
                             on those workers, delete snapshots you no longer need (`legion \
                             snapshot list`), or resume or destroy suspended instances to release \
                             their checkpoints. `legion node usage <node>` shows each node's \
                             checkpoint room",
                            task_config.name,
                            needed / (1024 * 1024)
                        );
                    }
                    roomy
                }
                _ => eligible,
            };
            let eligible = match pin.as_ref() {
                None => eligible,
                Some((instance_id, node_id)) => {
                    let pinned = eligible
                        .iter()
                        .copied()
                        .filter(|node| node.id.as_str() == node_id.as_str())
                        .collect::<Vec<_>>();
                    if pinned.is_empty() {
                        // Say why the pinned node cannot take it. Only a node
                        // that is really gone gets the "abandon the data"
                        // advice; a healthy node that is full, port-blocked,
                        // or excluded from this pass must never prompt anyone
                        // to delete data they have not lost.
                        let why = if node_lookup.get(node_id.as_str()).is_none() {
                            self.pinned_node_absent_reason(node_id)
                        } else if !candidates.iter().any(|node| node.id == *node_id) {
                            "which is healthy but no longer satisfies the task's constraints, \
                             permit, or volume drivers. Its data is intact; restore the node's \
                             attributes to place it there again"
                                .to_string()
                        } else if let Some(port) = (!static_ports.is_empty())
                            .then(|| {
                                port_ledger.conflict(
                                    node_id.as_str(),
                                    &static_ports,
                                    allocations_by_node,
                                )
                            })
                            .flatten()
                        {
                            format!(
                                "which is healthy but has static port {port} in use by another \
                                 allocation. Its data is intact; it is placed there once the \
                                 port is free"
                            )
                        } else {
                            "which is healthy but has no room for it right now (CPU, memory, \
                             disk, or a `distinct` rule). Its data is intact; it is placed \
                             there once the node has room"
                                .to_string()
                        };
                        return Self::record_unplaced(
                            partial,
                            &mut unplaced,
                            UnplacedTask {
                                task_config_id: task_id,
                                task_name: task_config.name.clone(),
                                count: count - placement_index,
                                reason: format!(
                                    "Failed to place task {}: instance {instance_id} holds \
                                     node-local storage on node {node_id}, {why}",
                                    task_config.name
                                ),
                                static_port: None,
                            },
                        );
                    }
                    pinned
                }
            };

            let (node_id, ranked_candidates) = if let Some(group_key) = group_key {
                if let Some(existing) = group_assignments.get(group_key) {
                    let is_eligible = eligible
                        .iter()
                        .any(|node| node.id.as_str() == existing.as_str());
                    if !is_eligible {
                        let port = (!static_ports.is_empty())
                            .then(|| {
                                port_ledger.conflict(
                                    existing.as_str(),
                                    &static_ports,
                                    allocations_by_node,
                                )
                            })
                            .flatten();
                        let reason = match port {
                            Some(port) => format!(
                                "Failed to place task {} in group {}: assigned node {} has \
                                 static port {port} in use",
                                task_config.name, group_key.name, existing
                            ),
                            None => format!(
                                "Failed to place task {} in group {}: assigned node {} is not \
                                 eligible",
                                task_config.name, group_key.name, existing
                            ),
                        };
                        return Self::record_unplaced(
                            partial,
                            &mut unplaced,
                            UnplacedTask {
                                task_config_id: task_id,
                                task_name: task_config.name.clone(),
                                count: count - placement_index,
                                reason,
                                static_port: port,
                            },
                        );
                    }
                    (existing.clone(), vec![existing.clone()])
                } else {
                    // A multi-task group is one selection unit (a coupled
                    // component by definition), so its placements carry
                    // singleton candidate lists — no fallback walk.
                    let selected = self.select_node_candidates(
                        eligible,
                        reschedule_rng,
                        nodes_considered_setting,
                        score_sample_setting,
                        &mut plan.index_usage.score_sampled,
                        nodes_considered_rng,
                        task_id,
                        task_config,
                        &plan.affinity_set,
                        job_config_id,
                        allocations_by_node,
                        task_name_to_id,
                        task_id_to_group,
                        planned_task_allocations,
                        planned,
                        &scoring_cache,
                        weights,
                        PlanCandidatesSetting {
                            count: 1,
                            ..candidates_setting
                        },
                    )?;
                    let node_id = selected[0].clone();
                    group_assignments.insert(group_key.clone(), node_id.clone());
                    (node_id, selected)
                }
            } else {
                let ranked = self.select_node_candidates(
                    eligible,
                    reschedule_rng,
                    nodes_considered_setting,
                    score_sample_setting,
                    &mut plan.index_usage.score_sampled,
                    nodes_considered_rng,
                    task_id,
                    task_config,
                    &plan.affinity_set,
                    job_config_id,
                    allocations_by_node,
                    task_name_to_id,
                    task_id_to_group,
                    planned_task_allocations,
                    planned,
                    &scoring_cache,
                    weights,
                    candidates_setting,
                )?;
                (ranked[0].clone(), ranked)
            };

            let mut ranked_candidates = ranked_candidates;
            if let Some(snapshot_id) = spread_snapshot {
                // No fallback candidates: the apply could bind a fallback node
                // a sibling in this same plan also falls back to.
                ranked_candidates.truncate(1);
                spread_nodes
                    .entry(snapshot_id)
                    .or_default()
                    .insert(node_id.clone());
            }
            if let Some(scores) = locality.as_ref() {
                record_locality_placement(scores.score(&node_id));
            }
            let placed_node = node_lookup.get(node_id.as_str());
            if let Some(node) = placed_node {
                let wanted = task_local_volume_bytes(node, task_config);
                if wanted > 0 {
                    *planned_local_bytes.entry(node_id.clone()).or_default() += wanted;
                }
            }
            *planned.entry(node_id.clone()).or_default() += 1;
            planned_task_allocations
                .entry(node_id.clone())
                .or_default()
                .push(task_id);
            if let Some(tracker) = plan.distinct_tracker.as_mut() {
                if let Some(node) = placed_node {
                    tracker.insert_from_node(node);
                }
            }
            resource_ledger.consume(&node_id, &plan.task_request)?;
            if !static_ports.is_empty() {
                port_ledger.claim(&node_id, &static_ports);
            }
            if let Some(node) = placed_node {
                scoring_cache.update_after_placement(
                    node,
                    task_id,
                    task_id_to_group,
                    resource_ledger,
                    &plan.task_request,
                );
            }
            placements.push(PlannedAllocation {
                node_id,
                task_config_id: task_id,
                candidates: ranked_candidates,
                instance_id: bind_instance,
            });
        }

        Ok(())
    }

    /// Give up on the rest of a task's placements: record them as unplaced
    /// when the plan may place partially, or fail the plan with a typed
    /// [`PlacementBlocked`] when it may not.
    fn record_unplaced(
        partial: bool,
        unplaced: &mut Option<&mut Vec<UnplacedTask>>,
        task: UnplacedTask,
    ) -> R<()> {
        match unplaced.as_deref_mut() {
            Some(list) if partial => {
                list.push(task);
                Ok(())
            }
            _ => Err(anyhow::Error::new(PlacementBlocked {
                unplaced: vec![task],
            })),
        }
    }

    /// Why a stateful instance's pinned node is not in this plan's node set
    /// at all, worded as the tail of "holds node-local storage on node X, …".
    /// Only a node that is lost or gone is called unavailable, and only then
    /// does the message mention abandoning the data.
    fn pinned_node_absent_reason(&self, node_id: &str) -> String {
        match self.store.get_node(node_id) {
            Ok(node) if node.status == "lost" || node.availability == "unavailable" => {
                "which is unavailable. Waiting for that node to return; use `legion volume \
                 delete --force <id>` to abandon the data if it never will"
                    .to_string()
            }
            Ok(node) if node.availability != "available" => format!(
                "which is cordoned or drained (availability {}). Its data is intact; \
                 `legion node uncordon {node_id}` lets the instance return to it",
                node.availability
            ),
            Ok(_) => "which is healthy but was excluded from this placement pass (a \
                      reschedule tries the other nodes first). Its data is intact"
                .to_string(),
            Err(_) => "which is no longer registered. Use `legion volume delete --force <id>` \
                       to abandon the data if the node will not rejoin"
                .to_string(),
        }
    }

    /// Select the ranked candidate list for one placement. The returned
    /// list is never empty; rank 0 is the chosen node, and any further
    /// entries are the fallback candidates the apply walk may bind instead
    /// (docs/late-binding-plan-apply.md).
    #[allow(clippy::too_many_arguments)]
    fn select_node_candidates(
        &self,
        eligible: Vec<&Node>,
        reschedule_rng: &mut Option<StdRng>,
        nodes_considered_setting: Option<&NodesConsidered>,
        score_sample_setting: Option<&ScoreSample>,
        score_sampled: &mut Option<usize>,
        nodes_considered_rng: &mut StdRng,
        task_id: i64,
        task_config: &TaskConfig,
        affinity_set: &AffinitySet,
        job_config_id: &str,
        allocations_by_node: &SharedNodeAllocations,
        task_name_to_id: &HashMap<String, i64>,
        task_id_to_group: &HashMap<i64, String>,
        planned_task_allocations: &HashMap<String, Vec<i64>>,
        planned: &HashMap<String, usize>,
        scoring_cache: &PlanScoringCache,
        weights: SchedulerWeights,
        candidates_setting: PlanCandidatesSetting,
    ) -> R<Vec<String>> {
        let eligible = self.apply_nodes_considered_with_setting(
            eligible,
            nodes_considered_setting,
            nodes_considered_rng,
        )?;
        let eligible = Self::apply_score_sample(
            eligible,
            score_sample_setting,
            nodes_considered_rng,
            score_sampled,
        );

        if matches!(self.policy.name(), "bin_pack" | "spread") {
            // A one-entry list (the `plan_candidates: 0` kill switch) needs
            // only the argmax, so take the fused scoring loop — same pick,
            // without materializing a scored vector per placement.
            if candidates_setting.count.max(1) == 1 {
                let selected = match reschedule_rng.as_mut() {
                    Some(rng) => self.select_cached_node_with_jitter(
                        &eligible,
                        affinity_set,
                        scoring_cache,
                        weights,
                        rng,
                    ),
                    None => {
                        self.select_cached_node(&eligible, affinity_set, scoring_cache, weights)
                    }
                };
                return selected.map(|node| vec![node.id.clone()]).with_context(|| {
                    format!(
                        "Scheduling policy {} failed to place task {}",
                        self.policy.name(),
                        task_config.name
                    )
                });
            }
            let scored = score_nodes_with_cache(&eligible, affinity_set, scoring_cache, weights);
            let fallback_width = self.fallback_pool_width();
            let ranked = if let Some(rng) = reschedule_rng.as_mut() {
                Self::ranked_candidate_list(scored, true, candidates_setting, fallback_width, rng)
            } else {
                Self::ranked_candidate_list(
                    scored,
                    false,
                    candidates_setting,
                    fallback_width,
                    &mut rand::thread_rng(),
                )
            };

            return ranked
                .map(|nodes| {
                    nodes
                        .into_iter()
                        .map(|node| node.id.clone())
                        .collect::<Vec<_>>()
                })
                .with_context(|| {
                    format!(
                        "Scheduling policy {} failed to place task {}",
                        self.policy.name(),
                        task_config.name
                    )
                });
        }

        // Policy plugins select a single node and expose no scored ranking,
        // so their placements carry a singleton candidate list (exact
        // epoch-CAS apply semantics, as before candidate lists existed).
        let ctx = SchedulingContext::new(
            eligible,
            task_config,
            task_id,
            affinity_set,
            job_config_id,
            allocations_by_node,
            task_name_to_id,
            task_id_to_group,
            planned_task_allocations,
            planned,
            weights,
        )
        .with_locality(scoring_cache.locality.as_deref());
        let selected = reschedule_rng
            .as_mut()
            .map(|rng| self.select_policy_node_with_jitter(&ctx, rng))
            .unwrap_or_else(|| self.policy.select_node(ctx));
        selected.map(|node| vec![node.id.clone()]).with_context(|| {
            format!(
                "Scheduling policy {} failed to place task {}",
                self.policy.name(),
                task_config.name
            )
        })
    }

    /// Build the ranked candidate list for one placement out of the scored
    /// eligible set (docs/late-binding-plan-apply.md).
    ///
    /// Rank 0 is today's pick, byte-for-byte: the argmax with exact-tie
    /// shuffle, or — with `jitter_head`, Reschedule mode — the uniform pick
    /// within the 1% jitter band. Ranks 1.. are sampled without
    /// replacement, score-weighted, from the near-tie band (nodes within
    /// `setting.band` of the best score); when the band holds fewer nodes
    /// than the list needs, the remaining slots are sampled — again
    /// without replacement — from the best `fallback_width` nodes below
    /// the band, and kept in score order. All randomness runs here, at
    /// plan time, on the planning manager — the apply walk is
    /// deterministic.
    fn ranked_candidate_list<'b, RNG: rand::Rng + ?Sized>(
        scored: Vec<(&'b Node, f64)>,
        jitter_head: bool,
        setting: PlanCandidatesSetting,
        fallback_width: usize,
        rng: &mut RNG,
    ) -> Option<Vec<&'b Node>> {
        if scored.is_empty() {
            return None;
        }
        let best = scored
            .iter()
            .fold(f64::NEG_INFINITY, |acc, (_, score)| acc.max(*score));

        let head_band = if jitter_head {
            RESCHEDULE_SCORE_JITTER_THRESHOLD
        } else {
            // The exact-tie tolerance the argmax pick has always used.
            1e-9
        };
        let head_pool: Vec<usize> = scored
            .iter()
            .enumerate()
            .filter(|(_, (_, score))| *score >= best - head_band)
            .map(|(index, _)| index)
            .collect();
        let head_index = head_pool[rng.gen_range(0..head_pool.len())];

        let k = setting.count.max(1);
        let mut list = vec![scored[head_index].0];
        if k == 1 {
            return Some(list);
        }

        let band_floor = best - setting.band;
        let mut band: Vec<(&'b Node, f64)> = Vec::new();
        let mut below: Vec<(&'b Node, f64)> = Vec::new();
        for (index, (node, score)) in scored.iter().enumerate() {
            if index == head_index {
                continue;
            }
            if *score >= band_floor {
                band.push((*node, *score));
            } else {
                below.push((*node, *score));
            }
        }

        // Score-weighted sampling without replacement. The weights are the
        // scores shifted to the band floor (plus a small epsilon so exact
        // floor ties stay sampleable): proportional-to-score within the
        // band, and uniform when the whole band ties at the best score.
        let epsilon = setting.band.max(1e-9) * 0.01;
        while list.len() < k && !band.is_empty() {
            let total: f64 = band
                .iter()
                .map(|(_, score)| score - band_floor + epsilon)
                .sum();
            let mut roll = rng.gen_range(0.0..total);
            let mut chosen = band.len() - 1;
            for (index, (_, score)) in band.iter().enumerate() {
                let weight = score - band_floor + epsilon;
                if roll < weight {
                    chosen = index;
                    break;
                }
                roll -= weight;
            }
            list.push(band.swap_remove(chosen).0);
        }

        // Below-band fill, for when the band is thinner than the list (the
        // common bin_pack case). Take the top `width` by score, then sample
        // the slots we still need from that pool without replacement.
        //
        // The sampling is the point. Taking the top `need` deterministically
        // — what this did — hands every concurrent planner the *same*
        // fallback nodes in the same order, so a burst's fallbacks collide
        // exactly as their first choices did and the tail buys nothing.
        // Sampling from a pool sized to the number of placements that can
        // be in flight at once decorrelates them.
        //
        // Still a partial selection, not a full sort: the below-band set is
        // nearly the whole fleet, and O(n + w log w) was measurably cheaper
        // than O(n log n) per placement at 10k nodes.
        let need = k.saturating_sub(list.len()).min(below.len());
        if need > 0 {
            let descending = |left: &(&Node, f64), right: &(&Node, f64)| {
                right
                    .1
                    .partial_cmp(&left.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            };
            let width = fallback_width.max(need).min(below.len());
            below.select_nth_unstable_by(width - 1, descending);
            below.truncate(width);
            // Partial Fisher-Yates: `need` draws without replacement, then
            // back into score order so a lower rank is never a better node.
            for index in 0..need {
                let pick = rng.gen_range(index..below.len());
                below.swap(index, pick);
            }
            below.truncate(need);
            below.sort_by(descending);
            for (node, _) in below {
                list.push(node);
            }
        }

        Some(list)
    }

    /// How wide a score-ordered pool the below-band fallback tail is
    /// sampled from, before it is clamped to the eligible set.
    ///
    /// Sized to the placements that can race for the same scores at once:
    /// one group commit's worth of plans (`plan_apply_batch_max`, default
    /// 16) times a plan's placement count, for which the candidate-list
    /// length (default 8) is the stand-in available here. 128 at the
    /// defaults. Wider costs placement quality (the tail reaches further
    /// down the ranking); narrower re-correlates concurrent planners,
    /// which is the collision this exists to break.
    fn fallback_pool_width(&self) -> usize {
        self.plan_apply_batch_max
            .saturating_mul(self.plan_candidates.count.max(1))
    }

    /// Candidate nodes this task is *allowed* on but does not currently fit
    /// — the input to the preemption pass.
    ///
    /// Identical to [`Self::eligible_nodes`] minus the resource check: the
    /// whole question preemption answers is "would this node fit if lower-band
    /// work were displaced", so the constraint, permit and `distinct` filters
    /// still bind (they are not about capacity and preemption cannot change
    /// them) while capacity does not.
    fn preemption_candidate_nodes<'b>(
        candidates: &[&'b Node],
        constraint_set: &ConstraintSet,
        permit_set: &PermitSet,
        distinct_tracker: Option<&DistinctTracker>,
    ) -> Vec<&'b Node> {
        candidates
            .iter()
            .copied()
            .filter(|node| {
                permit_set.is_permitted(node)
                    && distinct_tracker
                        .map(|tracker| constraint_set.distinct_satisfied(node, tracker))
                        .unwrap_or(true)
            })
            .collect()
    }

    /// Pick the node whose victim set this placement should pay for, or
    /// `None` when nothing can be freed.
    ///
    /// Node choice is the plan's: fewest victims, then the least important
    /// work, then the normal scoring policy as a tiebreak within the winning
    /// group — so packing quality still decides among equally costly options.
    /// `observe` mode runs this entire pass and then returns `None`, which is
    /// what makes the preview honest: the selection an operator sees is the
    /// selection enforce mode would make.
    fn plan_preemption_for_placement(
        &self,
        candidates: &[&Node],
        task_config: &TaskConfig,
        ask: &ResourceRequest,
        resource_ledger: &ResourceLedger,
        pinned_group_node: Option<String>,
        pinned_volume_node: Option<String>,
        pass: &mut PreemptionPass,
    ) -> R<Option<PreemptionChoice>> {
        if !pass.preemptor_band.can_preempt() {
            return Ok(None);
        }
        if candidates.is_empty() {
            return Ok(None);
        }

        // A group already assigned to a node, or a placement pinned to the
        // node its data lives on, has exactly one legal answer. Filtering
        // here rather than after selection keeps the pass from proposing
        // evictions on a node the placement could never use.
        let candidates: Vec<&Node> = candidates
            .iter()
            .copied()
            .filter(|node| {
                pinned_group_node
                    .as_deref()
                    .map(|pinned| pinned == node.id)
                    .unwrap_or(true)
                    && pinned_volume_node
                        .as_deref()
                        .map(|pinned| pinned == node.id)
                        .unwrap_or(true)
            })
            .collect();
        let before_pool_filter = candidates.len();
        let candidates: Vec<&Node> = candidates
            .into_iter()
            .filter(|node| self.preemption.enabled_for_pool(&node.pool))
            .collect();
        if candidates.is_empty() {
            // Distinguish "there was nowhere to look" from "every place to
            // look is exempt": only the second is a config an operator can
            // change, and only the second should be blamed in the error.
            pass.pool_blocked = before_pool_filter > 0;
            return Ok(None);
        }

        // The pass ran. Whether it found anything is a separate question,
        // answered by the tally.
        pass.considered += 1;
        pass.load_candidates(self.store, &self.store.resource_policy, &candidates)?;

        let mut best: Option<(PreemptionChoice, (usize, u32))> = None;
        let mut node_scores: HashMap<String, (usize, u32)> = HashMap::new();
        for node in &candidates {
            let Some(free) = resource_ledger.pools.get(node.id.as_str()) else {
                continue;
            };
            let on_node = pass.candidates_on_node(&node.id);
            let (eligible, tally) = eligible_victims(
                &on_node,
                pass.preemptor_band,
                self.preemption.victim_exempt_age,
                &pass.taken_by_task,
            );
            pass.record_tally(tally);
            let Some(victims) = select_victims(ask, free, &eligible) else {
                continue;
            };
            // A node that needs no victims would have passed the resource
            // check and never reached this pass; treat it as free anyway.
            let cost = preemption_cost(&victims);
            node_scores.insert(node.id.clone(), cost);
            let better = best
                .as_ref()
                .map(|(_, best_cost)| cost < *best_cost)
                .unwrap_or(true);
            if better {
                best = Some((
                    PreemptionChoice {
                        node_id: node.id.clone(),
                        victims,
                    },
                    cost,
                ));
            }
        }

        let Some((mut choice, best_cost)) = best else {
            // The pass ran and could not help. Worth a metric of its own:
            // "preemption is enabled and still not placing this" is a
            // different operational story from "preemption never ran", and
            // only the counter distinguishes them at a glance.
            crate::manager::telemetry::metrics()
                .preemption_plans_total
                .inc(&["no_victims"]);
            return Ok(None);
        };

        // Tiebreak within the cheapest group using the normal policy score,
        // so preemption does not undo the cluster's packing strategy.
        let tied: Vec<&Node> = candidates
            .iter()
            .copied()
            .filter(|node| node_scores.get(&node.id).copied() == Some(best_cost))
            .collect();
        if tied.len() > 1 {
            let mut ids: Vec<&str> = tied.iter().map(|node| node.id.as_str()).collect();
            ids.sort_unstable();
            if let Some(first) = ids.first() {
                if *first != choice.node_id {
                    // Recompute the winner's victims on the deterministic
                    // pick so the returned set matches the returned node.
                    if let Some(node) = tied.iter().find(|node| node.id == *first) {
                        if let Some(free) = resource_ledger.pools.get(node.id.as_str()) {
                            let on_node = pass.candidates_on_node(&node.id);
                            let (eligible, _) = eligible_victims(
                                &on_node,
                                pass.preemptor_band,
                                self.preemption.victim_exempt_age,
                                &pass.taken_by_task,
                            );
                            if let Some(victims) = select_victims(ask, free, &eligible) {
                                choice = PreemptionChoice {
                                    node_id: node.id.clone(),
                                    victims,
                                };
                            }
                        }
                    }
                }
            }
        }

        if matches!(self.preemption.mode, PreemptionMode::Observe) {
            // Same selection, no commitment. The placement fails exactly as
            // it does with preemption off; the operator gets the evidence.
            pass.observed
                .extend(choice.victims.iter().map(|victim| PlannedPreemption {
                    allocation_id: victim.allocation_id,
                    node_id: victim.node_id.clone(),
                    job_id: victim.job_id.clone(),
                    job_config_id: victim.job_config_id.clone(),
                    task_config_id: victim.task_config_id,
                    task_name: victim.task_name.clone(),
                    band: victim.band,
                    kill_after_secs: victim.kill_after_secs,
                }));
            info!(
                task = %task_config.name,
                preemptor_job = %pass.preemptor_job,
                node_id = %choice.node_id,
                victims = choice.victims.len(),
                "Preemption would place this task (observe mode); evicting nothing"
            );
            // Journalled HERE rather than after the plan returns, because
            // observe mode's whole point is that the placement then fails:
            // the plan never reaches its caller, so an event emitted on the
            // way out would never be written.
            if pass.journal_observed {
                let observed = pass.observed.clone();
                self.store.record_observed_preemptions(
                    &pass.preemptor_config_id,
                    &pass.preemptor_job,
                    pass.preemptor_band,
                    &observed,
                );
            }
            return Ok(None);
        }
        Ok(Some(choice))
    }

    /// The subset of `candidates` one placement may still use: the nodes
    /// the `distinct` tracker allows whose remaining capacity fits the
    /// task.
    ///
    /// Constraints, permit, and volume-driver support are deliberately
    /// NOT re-tested here. Every candidate list a placement receives was
    /// built by `scan_base_eligible` / `indexed_base_eligible` (or
    /// filtered down from one), which applied all three once, and none of
    /// them can change during a plan. Re-running the permit per placement
    /// — as this did — was the dominant per-placement cost at fleet
    /// scale, for a question the candidate list already answers.
    fn eligible_nodes<'b>(
        candidates: &[&'b Node],
        constraint_set: &ConstraintSet,
        distinct_tracker: Option<&DistinctTracker>,
        scoring_cache: &PlanScoringCache,
    ) -> Vec<&'b Node> {
        candidates
            .iter()
            .copied()
            .filter(|node| {
                distinct_tracker
                    .map(|tracker| constraint_set.distinct_satisfied(node, tracker))
                    .unwrap_or(true)
                    && scoring_cache.can_fit_task_request(node)
            })
            .collect()
    }

    /// The tasks of a stored config this pass may place, with their desired
    /// counts, and the tasks held back by an unmet `dependsOn`. A `Live`
    /// gate judges dependencies by the statuses workers reported; a
    /// `Preview` gate (a dry run) ignores them and treats every batch
    /// dependency as one that will succeed, so the preview shows the job's
    /// full eventual placement.
    fn tasks_for_plan_from_store(
        &self,
        job_config_id: &str,
        gate: DependencyGate,
    ) -> R<ReadyTasks> {
        let tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .map(|(id, config)| {
                let desired = config.count.num;
                (id, config, desired)
            })
            .collect::<Vec<_>>();
        // A task with no dependencies never needs a status, and most jobs
        // have none: skip the status read entirely for them.
        let has_dependencies = tasks.iter().any(|(_, task, _)| !task.depends_on.is_empty());
        let status_by_id = if has_dependencies && gate == DependencyGate::Live {
            self.store.task_statuses_for_job_config(job_config_id)?
        } else {
            HashMap::new()
        };
        let status_map = tasks
            .iter()
            .map(|(id, task, _)| {
                (
                    task.name.clone(),
                    status_by_id
                        .get(id)
                        .copied()
                        .unwrap_or(TaskStatusState::Pending),
                )
            })
            .collect::<HashMap<_, _>>();

        let mut ready = Self::ready_tasks(tasks, &status_map, gate)?;
        if let Some(job_id) = self.store.job_id_for_job_config(job_config_id)? {
            // Overrides first, keyed counts second: keyed tasks' desired
            // comes from live instances and must win (scaling keyed tasks
            // is rejected at the ScaleTask RPC anyway).
            Self::apply_count_overrides(self.store, &job_id, &mut ready.ready)?;
            Self::apply_keyed_instance_counts(self.store, &job_id, &mut ready.ready)?;
        }
        Ok(ready)
    }

    /// Overlay `legion job scale` count overrides (docs/autoscaling.md):
    /// the effective desired count is the override when one exists, else
    /// the spec count already in the tuple. Keyed tasks (instance-driven)
    /// and `count.per` tasks (group-converging) are skipped — both are
    /// rejected at the ScaleTask RPC.
    fn apply_count_overrides(
        store: &Store,
        job_id: &str,
        tasks: &mut [(i64, TaskConfig, u32)],
    ) -> R<()> {
        for (_task_id, task, desired) in tasks.iter_mut() {
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Keyed);
            if keyed || task.count.per.is_some() {
                continue;
            }
            *desired = store.effective_task_count(job_id, &task.group, &task.name, *desired)?;
        }
        Ok(())
    }

    /// Replace `count.num`-derived desired counts with live keyed-instance
    /// counts for keyed-mode stateful tasks. `count.num` does not drive keyed
    /// tasks: their desired allocation count is exactly the number of
    /// instances in Pending/Active/Rescheduling state, created and destroyed
    /// through the instance management API (zero instances = zero
    /// allocations). Slot-mode and stateless tasks are untouched.
    fn apply_keyed_instance_counts(
        store: &Store,
        job_id: &str,
        tasks: &mut [(i64, TaskConfig, u32)],
    ) -> R<()> {
        for (_task_id, task, desired) in tasks.iter_mut() {
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Keyed);
            if keyed {
                // Live instances, plus suspended ones whose allocation row
                // holds their reservation (`suspend.reserve`), so the
                // scheduler neither culls the hold nor re-places it.
                *desired =
                    store.count_keyed_allocations_desired(job_id, &task.group, &task.name)?;
            }
        }
        Ok(())
    }

    /// Reduce a slot-mode stateful task's desired count by the number of
    /// slots still deliberately occupied by live allocations of OTHER
    /// configs of the same job.
    ///
    /// After (or during) a staged rollout (`rollout.slots.updateFrom`),
    /// slots below the floor keep running the previous config's allocations
    /// on purpose. A plain reschedule of the latest config would otherwise
    /// converge that config to the full `count.num` and double-place those
    /// slots (two live allocations on one instance — the at-most-one
    /// invariant). Only applied to plain (no explicit per-task targets)
    /// `Reschedule` plans: rollouts pass explicit targets and own this math
    /// themselves, and `Fresh` plans deliberately ignore existing
    /// allocations. Keyed and stateless tasks are untouched.
    fn apply_slot_staged_counts(
        store: &Store,
        job_id: &str,
        job_config_id: &str,
        tasks: &mut [(i64, TaskConfig, u32)],
    ) -> R<()> {
        for (_task_id, task, desired) in tasks.iter_mut() {
            let slot_mode = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Slot);
            if slot_mode {
                let other_config_slots = store.count_slot_allocations_excluding_config(
                    job_id,
                    &task.group,
                    job_config_id,
                )?;
                *desired = desired.saturating_sub(other_config_slots);
            }
        }
        Ok(())
    }

    /// The tasks of a not-yet-stored config a dry run places: every task
    /// whose dependencies a preview considers satisfiable.
    fn tasks_for_plan(job_config: &JobConfig) -> R<Vec<(i64, TaskConfig, u32)>> {
        let tasks = job_config
            .tasks
            .iter()
            .enumerate()
            .map(|(index, config)| ((index + 1) as i64, config.clone(), config.count.num))
            .collect::<Vec<_>>();

        Ok(Self::ready_tasks(tasks, &HashMap::new(), DependencyGate::Preview)?.ready)
    }

    fn ready_tasks(
        tasks: Vec<(i64, TaskConfig, u32)>,
        status_map: &HashMap<String, TaskStatusState>,
        gate: DependencyGate,
    ) -> R<ReadyTasks> {
        let mut tasks_by_name: HashMap<String, &TaskConfig> = HashMap::new();
        for (_task_id, task, _count) in &tasks {
            tasks_by_name.insert(task.name.clone(), task);
        }

        let ordered_names = Self::task_names_topologically_sorted(&tasks, &tasks_by_name)?;

        let mut planned = HashSet::new();
        let mut progressed = true;
        while progressed {
            progressed = false;
            for task_name in &ordered_names {
                if planned.contains(task_name) {
                    continue;
                }

                let task = tasks_by_name
                    .get(task_name)
                    .copied()
                    .context("Task missing from task map")?;
                let ready = task.depends_on.iter().all(|dependency| {
                    Self::dependency_satisfied(
                        dependency,
                        status_map,
                        &planned,
                        &tasks_by_name,
                        gate,
                    )
                });
                if ready {
                    planned.insert(task_name.clone());
                    progressed = true;
                }
            }
        }

        let (ready, held): (Vec<_>, Vec<_>) = tasks
            .into_iter()
            .partition(|(_, task, _)| planned.contains(&task.name));
        Ok(ReadyTasks {
            ready,
            held: held.into_iter().map(|(_, task, _)| task.name).collect(),
        })
    }

    fn task_names_topologically_sorted(
        tasks: &[(i64, TaskConfig, u32)],
        tasks_by_name: &HashMap<String, &TaskConfig>,
    ) -> R<Vec<String>> {
        let mut indegree: HashMap<String, usize> = tasks
            .iter()
            .map(|(_, task, _)| (task.name.clone(), 0))
            .collect();
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();

        for (_task_id, task, _count) in tasks {
            for dependency in &task.depends_on {
                let dep_name = dependency.task.as_str();
                if dep_name == task.name {
                    bail!("Task {} cannot depend on itself", task.name);
                }
                if !tasks_by_name.contains_key(dep_name) {
                    bail!("Task {} depends on unknown task {}", task.name, dep_name);
                }

                graph
                    .entry(dep_name.to_string())
                    .or_default()
                    .push(task.name.clone());
                *indegree.entry(task.name.clone()).or_default() += 1;
            }
        }

        let mut zero_degree = indegree
            .iter()
            .filter_map(|(name, degree)| (*degree == 0).then_some(name.clone()))
            .collect::<Vec<_>>();
        zero_degree.sort();
        let mut queue: VecDeque<String> = zero_degree.into_iter().collect();

        let mut ordered = Vec::with_capacity(tasks.len());
        while let Some(name) = queue.pop_front() {
            ordered.push(name.clone());
            if let Some(children) = graph.get(&name) {
                for child in children {
                    if let Some(count) = indegree.get_mut(child) {
                        *count -= 1;
                        if *count == 0 {
                            queue.push_back(child.clone());
                        }
                    }
                }
            }
        }

        if ordered.len() != tasks.len() {
            bail!("Task dependencies contain a cycle");
        }

        Ok(ordered)
    }

    /// Whether one `dependsOn` entry lets its task be placed in this pass.
    ///
    /// - `start`: the dependency has started, or is placed in this pass.
    /// - `complete`: the dependency finished, whether it succeeded or
    ///   failed (its worker reported an exit).
    /// - `success`: the dependency finished successfully (exited 0).
    ///
    /// Under the `Live` gate only a reported status satisfies `complete` and
    /// `success`. Counting a batch dependency that is merely placed in the
    /// same pass — what this did before — started a whole chain of batch
    /// steps at once and let a dependent run after its dependency failed.
    /// The `Preview` gate keeps that optimistic reading for dry runs.
    fn dependency_satisfied(
        dependency: &crate::structs::TaskDependency,
        status_map: &HashMap<String, TaskStatusState>,
        planned: &HashSet<String>,
        tasks_by_name: &HashMap<String, &TaskConfig>,
        gate: DependencyGate,
    ) -> bool {
        let dep_name = dependency.task.as_str();
        let dep_status = status_map
            .get(dep_name)
            .copied()
            .unwrap_or(TaskStatusState::Pending);
        let dep_lifecycle = tasks_by_name
            .get(dep_name)
            .map(|task| task.lifecycle.clone())
            .unwrap_or(TaskLifecycle::Service);
        let planned_dep = planned.contains(dep_name);
        let preview_batch =
            gate == DependencyGate::Preview && planned_dep && dep_lifecycle == TaskLifecycle::Batch;

        match dependency.condition {
            TaskDependencyCondition::Start => dep_status.is_started() || planned_dep,
            TaskDependencyCondition::Complete => {
                matches!(
                    dep_status,
                    TaskStatusState::Complete | TaskStatusState::Success | TaskStatusState::Failed
                ) || preview_batch
            }
            TaskDependencyCondition::Success => {
                dep_status.is_success() || (dep_status != TaskStatusState::Failed && preview_batch)
            }
        }
    }

    fn apply_nodes_considered_with_setting<'b>(
        &self,
        eligible: Vec<&'b Node>,
        setting: Option<&NodesConsidered>,
        rng: &mut StdRng,
    ) -> R<Vec<&'b Node>> {
        if eligible.len() <= 1 {
            return Ok(eligible);
        }

        let Some(setting) = setting else {
            return Ok(eligible);
        };

        let limit = Self::nodes_considered_limit(setting, eligible.len())?;
        let mut eligible = eligible;
        if limit < eligible.len() {
            eligible.shuffle(rng);
        }

        Ok(eligible.into_iter().take(limit).collect())
    }

    fn select_policy_node_with_jitter<'b>(
        &self,
        ctx: &SchedulingContext<'b>,
        rng: &mut StdRng,
    ) -> Option<&'b Node> {
        if !matches!(self.policy.name(), "bin_pack" | "spread") {
            let fallback_ctx = SchedulingContext::new(
                ctx.eligible_nodes.clone(),
                ctx.task,
                ctx.task_id,
                ctx.affinity_set,
                ctx.job_config_id,
                ctx.allocations,
                ctx.task_name_to_id,
                ctx.task_id_to_group,
                ctx.planned_task_allocations,
                ctx.planned,
                ctx.weights,
            )
            .with_locality(ctx.locality);
            return self.policy.select_node(fallback_ctx);
        }

        let scored = score_nodes_for_context(ctx);
        if scored.is_empty() {
            return None;
        }

        let max_score = scored
            .iter()
            .fold(f64::NEG_INFINITY, |best, (_, score)| best.max(*score));
        let mut top = scored
            .into_iter()
            .filter(|(_, score)| *score >= max_score - RESCHEDULE_SCORE_JITTER_THRESHOLD)
            .map(|(node, _)| node)
            .collect::<Vec<_>>();
        top.shuffle(rng);
        top.first().copied()
    }

    /// The per-placement hot loop: pick the best-scoring node, breaking
    /// exact-score ties uniformly at random. Fused into one scoring pass
    /// (after the cheap load-bounds pass) with reservoir tie-breaking —
    /// the same selection distribution as score-all/sort/shuffle, without
    /// allocating a scored vector per placement.
    fn select_cached_node<'b>(
        &self,
        eligible: &[&'b Node],
        affinity_set: &AffinitySet,
        scoring_cache: &PlanScoringCache,
        weights: SchedulerWeights,
    ) -> Option<&'b Node> {
        if eligible.is_empty() {
            return None;
        }
        let bounds = cached_load_bounds(eligible, scoring_cache);
        let mut rng = rand::thread_rng();
        let mut best: Option<(&'b Node, f64)> = None;
        let mut tie_count = 0usize;
        for node in eligible.iter().copied() {
            let score = score_node_with_cache(node, affinity_set, scoring_cache, weights, bounds);
            match best {
                None => {
                    best = Some((node, score));
                    tie_count = 1;
                }
                Some((_, best_score)) if score > best_score + 1e-9 => {
                    best = Some((node, score));
                    tie_count = 1;
                }
                Some((_, best_score)) if (score - best_score).abs() <= 1e-9 => {
                    // Uniform over the tie group: replace the incumbent
                    // with probability 1/n at the n-th tied candidate.
                    tie_count += 1;
                    if rng.gen_range(0..tie_count) == 0 {
                        best = Some((node, best_score));
                    }
                }
                Some(_) => {}
            }
        }
        best.map(|(node, _)| node)
    }

    fn select_cached_node_with_jitter<'b>(
        &self,
        eligible: &[&'b Node],
        affinity_set: &AffinitySet,
        scoring_cache: &PlanScoringCache,
        weights: SchedulerWeights,
        rng: &mut StdRng,
    ) -> Option<&'b Node> {
        let scored = score_nodes_with_cache(eligible, affinity_set, scoring_cache, weights);
        if scored.is_empty() {
            return None;
        }

        let max_score = scored
            .iter()
            .fold(f64::NEG_INFINITY, |best, (_, score)| best.max(*score));
        let mut top = scored
            .into_iter()
            .filter(|(_, score)| *score >= max_score - RESCHEDULE_SCORE_JITTER_THRESHOLD)
            .map(|(node, _)| node)
            .collect::<Vec<_>>();
        top.shuffle(rng);
        top.first().copied()
    }

    /// The content a placement of this task needs on its node: its
    /// digest-pinned artifacts (the node content store keys them by their
    /// `checksum`), plus any restore manifest parts. Image layers join once
    /// the content store holds them by digest (cache plan Phase 1's OCI
    /// per-blob storage). Order and duplicates do not matter to the score.
    pub fn content_set(
        &self,
        task_config: &TaskConfig,
        restores: &[crate::storage::store::RestorePin],
    ) -> Vec<ContentRef> {
        let mut content: Vec<ContentRef> = task_config
            .artifacts
            .iter()
            .filter_map(|artifact| artifact.checksum.as_deref())
            // Normalized the way the node content store keys it, so a
            // checksum written in uppercase still matches the advertisement.
            .filter_map(|checksum| crate::content_store::ContentDigest::parse(checksum).ok())
            .map(|digest| ContentRef::digest(digest.to_string()))
            .collect();
        content.extend(Self::restore_content(restores));
        content
    }

    /// The memory parts of every restore this placement serves (suspend plan
    /// §6.6 and §6.8), sized from their manifests: a placement that resumes
    /// a checkpoint needs those bytes on its node. A `local` snapshot is
    /// already pinned to the node that holds it, so today this only moves the
    /// score in step with the pin. It starts to matter once snapshots can
    /// restore on another node (Phase 7b). The disk half lives in the volume
    /// driver, not the content store, so it is not part of the set. A
    /// manifest that does not parse contributes nothing, and the restore
    /// itself reports the error.
    fn restore_content(restores: &[crate::storage::store::RestorePin]) -> Vec<ContentRef> {
        restores
            .iter()
            .filter_map(|restore| {
                serde_json::from_str::<crate::structs::checkpoint::SnapshotManifest>(
                    &restore.manifest_json,
                )
                .ok()
            })
            .flat_map(|manifest| manifest.memory)
            .map(|part| ContentRef::sized(part.digest, part.size))
            .collect()
    }

    /// This task's per-node locality, or `None` to skip the term.
    fn locality_scores(
        &self,
        task_config: &TaskConfig,
        restores: &[crate::storage::store::RestorePin],
        weights: SchedulerWeights,
    ) -> Option<Arc<LocalityScores>> {
        if weights.locality <= 0.0 {
            return None;
        }
        let content = self.content_set(task_config, restores);
        self.store
            .content_locations()
            .locality_scores(&content)
            .map(Arc::new)
    }

    fn scheduler_weights(&self) -> R<SchedulerWeights> {
        let mut weights = SchedulerWeights::for_policy(self.policy.name());
        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(weights);
        };

        let settings = match settings {
            Value::Null => return Ok(weights),
            Value::Object(map) => map,
            _ => {
                bail!(
                    "scheduler_settings for policy {} must be an object",
                    self.policy.name()
                )
            }
        };

        let Some(weight_values) = settings.get("weights") else {
            return Ok(weights);
        };

        let weight_values = match weight_values {
            Value::Object(map) => map,
            _ => bail!("scheduler_settings weights must be an object"),
        };

        for (key, value) in weight_values {
            let weight = value
                .as_f64()
                .with_context(|| format!("scheduler_settings weights {key} must be a number"))?;

            if weight < 0.0 {
                bail!("scheduler_settings weights {key} cannot be negative");
            }

            match key.as_str() {
                "affinities" => weights.affinity = weight,
                "spread" => weights.spread = weight,
                "bin_pack" => weights.bin_pack = weight,
                "automatic_anti_affinity" => weights.automatic_anti_affinity = weight,
                "locality" => weights.locality = weight,
                _ => {}
            }
        }

        Ok(weights)
    }

    /// Seed a task's `distinct` tracker with the attribute values its
    /// existing allocations already occupy. A task config belongs to
    /// exactly one job config, so the plan's own-allocation index holds
    /// every node the task is on.
    fn build_distinct_tracker(
        constraints: &ConstraintSet,
        task_id: i64,
        own_by_node: &OwnAllocationsByNode,
        node_lookup: &AvailableNodes,
    ) -> Option<DistinctTracker> {
        let distinct_keys = constraints.distinct_keys();
        if distinct_keys.is_empty() {
            return None;
        }

        let mut tracker = DistinctTracker::new(&distinct_keys);
        for (node_id, task_ids) in own_by_node {
            if task_ids.contains(&task_id) {
                if let Some(node) = node_lookup.get(node_id.as_str()) {
                    tracker.insert_from_node(node);
                }
            }
        }

        Some(tracker)
    }

    /// Parse `schedulerSettings.plan_candidates`: an integer shorthand
    /// (`plan_candidates: 8`, the list length) or the full form
    /// (`plan_candidates: { count: 8, band: 0.01 }`). Defaults on at
    /// `count: 8`; `0` disables candidate lists entirely, restoring
    /// single-node (format-1) plan submissions — the mixed-version kill
    /// switch (docs/late-binding-plan-apply.md).
    pub fn plan_candidates_setting(&self) -> R<PlanCandidatesSetting> {
        let default = PlanCandidatesSetting::default();
        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(default);
        };

        let settings = match settings {
            Value::Null => return Ok(default),
            Value::Object(map) => map,
            _ => {
                bail!(
                    "scheduler_settings for policy {} must be an object",
                    self.policy.name()
                )
            }
        };

        let Some(value) = settings.get("plan_candidates") else {
            return Ok(default);
        };

        match value {
            Value::Number(count) => {
                let count = count
                    .as_u64()
                    .context("plan_candidates must be a non-negative integer")?;
                Ok(PlanCandidatesSetting {
                    count: (count as usize).min(PlanCandidatesSetting::MAX_COUNT),
                    ..default
                })
            }
            Value::Object(map) => {
                let count = match map.get("count") {
                    None => default.count,
                    Some(count) => count
                        .as_u64()
                        .context("plan_candidates.count must be a non-negative integer")?
                        as usize,
                };
                let band = match map.get("band") {
                    None => default.band,
                    Some(band) => {
                        let band = band
                            .as_f64()
                            .context("plan_candidates.band must be a number")?;
                        if !(0.0..=1.0).contains(&band) {
                            bail!("plan_candidates.band must be between 0.0 and 1.0");
                        }
                        band
                    }
                };
                Ok(PlanCandidatesSetting {
                    count: count.min(PlanCandidatesSetting::MAX_COUNT),
                    band,
                })
            }
            _ => bail!("plan_candidates must be an integer or an object with count/band"),
        }
    }

    /// Bounded candidate scoring: when armed and the eligible list is at
    /// least `min_nodes`, score `sample` uniformly drawn candidates instead
    /// of all of them (best-of-k). Below the threshold the list is returned
    /// untouched — bit-for-bit today's exact scoring. Records the sample
    /// size in `score_sampled` for the `job plan` explain surface.
    fn apply_score_sample<'b>(
        eligible: Vec<&'b Node>,
        setting: Option<&ScoreSample>,
        rng: &mut StdRng,
        score_sampled: &mut Option<usize>,
    ) -> Vec<&'b Node> {
        let Some(setting) = setting else {
            return eligible;
        };
        if eligible.len() < setting.min_nodes || eligible.len() <= setting.sample {
            return eligible;
        }
        let mut eligible = eligible;
        // Partial Fisher-Yates: the first `sample` slots become a uniform
        // draw without replacement; the tail is never shuffled.
        for index in 0..setting.sample {
            let other = rng.gen_range(index..eligible.len());
            eligible.swap(index, other);
        }
        eligible.truncate(setting.sample);
        *score_sampled = Some(setting.sample);
        eligible
    }

    /// `schedulerSettings.score_sample` — bounded candidate scoring
    /// (`{ min_nodes, sample }`), off unless configured. Only meaningful
    /// for the built-in scoring policies, like `nodes_considered`.
    fn score_sample_setting(&self) -> R<Option<ScoreSample>> {
        if !matches!(self.policy.name(), "bin_pack" | "spread") {
            return Ok(None);
        }
        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(None);
        };
        let Some(raw) = settings.get("score_sample") else {
            return Ok(None);
        };
        let map = raw
            .as_object()
            .context("schedulerSettings.score_sample must be an object { min_nodes, sample }")?;
        let sample =
            map.get("sample")
                .and_then(Value::as_u64)
                .context("score_sample.sample must be a positive integer")? as usize;
        let min_nodes =
            map.get("min_nodes")
                .and_then(Value::as_u64)
                .context("score_sample.min_nodes must be a positive integer")? as usize;
        if sample < 2 {
            bail!("score_sample.sample must be at least 2");
        }
        if min_nodes < sample {
            bail!(
                "score_sample.min_nodes ({min_nodes}) must be at least score_sample.sample \
                 ({sample}): below the threshold scoring is exact"
            );
        }
        Ok(Some(ScoreSample { min_nodes, sample }))
    }

    /// `schedulerSettings.lazy_version_sample`, RETIRED: plans now sample
    /// the fence epochs out of their own capacity snapshot, which has the
    /// eager sample's guarantee at the lazy sample's cost, so there is no
    /// longer a choice to make (plan-path-fleet-scaling plan § 6.1). The key
    /// still parses so existing configs keep loading; `Scheduler::new`
    /// warns once per process when it is set.
    fn retired_lazy_version_sample(&self) -> Option<bool> {
        self.store
            .scheduler_settings
            .as_ref()
            .and_then(|settings| settings.get("lazy_version_sample"))
            .and_then(Value::as_bool)
    }

    fn nodes_considered_setting(&self) -> R<Option<NodesConsidered>> {
        if !matches!(self.policy.name(), "bin_pack" | "spread") {
            return Ok(None);
        }

        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(None);
        };

        let settings = match settings {
            Value::Null => return Ok(None),
            Value::Object(map) => map,
            _ => {
                bail!(
                    "scheduler_settings for policy {} must be an object",
                    self.policy.name()
                )
            }
        };

        let Some(nodes_considered) = settings.get("nodes_considered") else {
            return Ok(None);
        };

        match nodes_considered {
            Value::Number(count) => {
                let count = count
                    .as_u64()
                    .context("nodes_considered must be a positive integer or percentage")?;

                // Matches the floor the percentage form clamps to, and the
                // one `nodes_considered_limit` enforces: a subset of one
                // leaves the policy nothing to compare.
                if count < 2 {
                    bail!("nodes_considered must be at least 2");
                }

                Ok(Some(NodesConsidered::Count(count)))
            }
            Value::String(raw) => {
                let trimmed = raw.trim();
                let percent = trimmed
                    .strip_suffix('%')
                    .context("nodes_considered percentage must end with '%'")?;
                let percent: f64 = percent
                    .trim()
                    .parse()
                    .context("nodes_considered percentage must be a number")?;

                if percent < 0.0 {
                    bail!("nodes_considered percentage cannot be negative");
                }

                Ok(Some(NodesConsidered::Percent(percent)))
            }
            _ => bail!("nodes_considered must be a number or percentage string"),
        }
    }

    fn nodes_considered_limit(setting: &NodesConsidered, eligible_len: usize) -> R<usize> {
        if eligible_len == 0 {
            return Ok(0);
        }

        match setting {
            NodesConsidered::Count(count) => {
                if eligible_len == 1 {
                    return Ok(1);
                }

                if *count < 2 {
                    bail!("nodes_considered must be at least 2");
                }

                Ok(std::cmp::min(*count as usize, eligible_len))
            }
            NodesConsidered::Percent(percent) => {
                if eligible_len == 1 {
                    return Ok(1);
                }

                let percent = *percent / 100.0;
                let mut count = ((eligible_len as f64) * percent).ceil() as usize;
                if count < 2 {
                    count = 2;
                }

                Ok(std::cmp::min(count, eligible_len))
            }
        }
    }

    fn allocations_delta(
        affected_nodes: &HashSet<String>,
        allocations: &[Allocation],
    ) -> Vec<LedgerDelta> {
        let mut added_by_node: HashMap<String, Vec<Allocation>> = HashMap::new();
        for allocation in allocations {
            added_by_node
                .entry(allocation.node_id.clone())
                .or_default()
                .push(allocation.clone());
        }

        let mut ordered_nodes = affected_nodes.iter().collect::<Vec<_>>();
        ordered_nodes.sort();

        let mut deltas = Vec::new();
        for node_id in ordered_nodes {
            if let Some(added) = added_by_node.remove(node_id) {
                if !added.is_empty() {
                    deltas.push(LedgerDelta {
                        node_id: (*node_id).clone(),
                        added,
                        removed: Vec::new(),
                    });
                }
            }
        }

        for (node_id, added) in added_by_node {
            if !added.is_empty() {
                deltas.push(LedgerDelta {
                    node_id,
                    added,
                    removed: Vec::new(),
                });
            }
        }

        deltas
    }
}

/// Whether a node advertises every driver in `required`, and reports each as
/// usable. An installed-but-unavailable driver (no `mount` binary, no declared
/// host volumes) is treated as absent — it cannot serve a mount either way, and
/// skipping the node produces a better error than a failed task start.
fn node_provides_volume_drivers(
    node: &Node,
    required: &std::collections::BTreeSet<String>,
) -> bool {
    if required.is_empty() {
        return true;
    }
    let Some(drivers) = node.fingerprint_attrs.get("volumeDrivers") else {
        // A node that advertises nothing predates plugin fingerprinting (or is
        // a test fixture). Assume the built-ins rather than excluding it, so an
        // upgrade doesn't strand every stateful job on the old workers.
        return required
            .iter()
            .all(|name| crate::volumes::BUILTIN_VOLUME_DRIVERS.contains(&name.as_str()));
    };
    required.iter().all(|name| {
        drivers
            .get(name)
            .map(|caps| {
                // Missing `available` means an older worker that predates the
                // field; assume usable rather than silently excluding it.
                caps.get("available")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
            })
            .unwrap_or(false)
    })
}

/// A restoring instance's parsed manifest, or the placement error for an
/// unreadable one.
fn restore_manifest(
    task_config: &TaskConfig,
    restore: &crate::storage::store::RestorePin,
) -> R<crate::structs::checkpoint::SnapshotManifest> {
    crate::structs::checkpoint::SnapshotManifest::from_json(&restore.manifest_json).with_context(
        || {
            format!(
                "Failed to place task {}: instance {} restores snapshot {}, whose manifest is \
                 unreadable",
                task_config.name, restore.instance_id, restore.snapshot_id
            )
        },
    )
}

/// Whether `node` can restore `manifest` (its `compat` block against the
/// node's fingerprint).
fn node_restore_compat(
    manifest: &crate::structs::checkpoint::SnapshotManifest,
    node: &Node,
) -> Result<(), crate::structs::checkpoint::CompatMismatch> {
    let empty = Map::new();
    let attrs = node.fingerprint_attrs.as_object().unwrap_or(&empty);
    crate::structs::checkpoint::compat_matches(manifest, attrs)
}

/// Check a restoring instance's snapshot `compat` against the node it is
/// pinned to (`node_id`: its local snapshot's node, or its volume's). The
/// error names the instance, the snapshot, the node, and the attribute that
/// differs, with both values.
fn check_restore_compat(
    task_config: &TaskConfig,
    restore: &crate::storage::store::RestorePin,
    node_id: &str,
    node_lookup: &AvailableNodes,
) -> R<()> {
    let manifest = restore_manifest(task_config, restore)?;
    let Some(node) = node_lookup.get(node_id) else {
        // The pin below reports an unavailable node with its own message.
        return Ok(());
    };
    if let Err(mismatch) = node_restore_compat(&manifest, node) {
        bail!(
            "Failed to place task {}: instance {} restores {} snapshot {}, which node {node_id} \
             cannot run: {mismatch}. A memory snapshot resumes only on the CPU and VMM it was \
             taken on; restore an older snapshot, or restart the instance cold",
            task_config.name,
            restore.instance_id,
            manifest.kind.as_str(),
            restore.snapshot_id
        );
    }
    Ok(())
}

/// Keep the candidates that can restore a `published` snapshot, which has
/// no hard pin: its `compat` is checked per candidate (suspend plan §6.8,
/// "Hard versus soft"). A disk snapshot (empty `compat`) passes the compat
/// check everywhere. Either way a candidate's worker must advertise
/// `checkpoint-fetch`, since it may have to fetch the parts first, so a
/// mixed-version fleet never sends a published restore to an older worker.
/// When none fit, the error names up to three candidates and why each one
/// fails.
fn filter_restore_compat<'b>(
    task_config: &TaskConfig,
    restore: &crate::storage::store::RestorePin,
    eligible: Vec<&'b Node>,
) -> R<Vec<&'b Node>> {
    use crate::structs::snapshot::{node_advertises, CHECKPOINT_FETCH_CAPABILITY};
    let manifest = restore_manifest(task_config, restore)?;
    let mut rejected: Vec<String> = Vec::new();
    let mut fitting = Vec::with_capacity(eligible.len());
    for node in eligible {
        if !node_advertises(node, CHECKPOINT_FETCH_CAPABILITY) {
            if rejected.len() < 3 {
                rejected.push(format!(
                    "{}: its worker cannot fetch published snapshots (no \
                     `{CHECKPOINT_FETCH_CAPABILITY}` capability; upgrade it)",
                    node.id
                ));
            }
            continue;
        }
        if manifest.compat.is_empty() {
            fitting.push(node);
            continue;
        }
        match node_restore_compat(&manifest, node) {
            Ok(()) => fitting.push(node),
            Err(mismatch) => {
                if rejected.len() < 3 {
                    rejected.push(format!("{}: {mismatch}", node.id));
                }
            }
        }
    }
    if fitting.is_empty() {
        bail!(
            "Failed to place task {}: instance {} restores published {} snapshot {}, and no \
             eligible node can run it ({}). A memory snapshot resumes only on the CPU and VMM \
             it was taken on, on a worker that can fetch published snapshots; add a matching \
             node, or restart the instance cold",
            task_config.name,
            restore.instance_id,
            manifest.kind.as_str(),
            restore.snapshot_id,
            rejected.join("; ")
        );
    }
    Ok(fitting)
}

/// Bytes of **node-local** volume storage one placement of this task would put
/// on `node`, judged by the node's own advertised driver capabilities.
///
/// The locality question is per node deliberately: `size` on an `nfs` volume
/// consumes the NFS server's disk, not this node's, so remote drivers must not
/// count — that is the difference between "volumes respect node disk" and
/// "volumes double-count storage that lives elsewhere". Drivers the node does
/// not advertise fall back to the built-in set's locality. `tmpfs` is
/// node-local but RAM-backed, so it is excluded from *disk* accounting too.
/// Declarations without a parseable `size` contribute nothing — this is
/// placement sanity, not quota enforcement.
fn task_local_volume_bytes(node: &Node, task_config: &TaskConfig) -> u64 {
    let advertised = node.fingerprint_attrs.get("volumeDrivers");
    task_config
        .volume_size_declarations()
        .into_iter()
        .filter_map(|(driver, size)| {
            let size = size?;
            if driver == "tmpfs" {
                return None;
            }
            let node_local = advertised
                .and_then(|drivers| drivers.get(&driver))
                .and_then(|caps| caps.get("nodeLocal"))
                .and_then(Value::as_bool)
                .unwrap_or_else(|| {
                    // Unadvertised: assume the built-ins' locality (nfs is the
                    // one remote built-in), and treat unknown drivers as local
                    // — over-counting delays a placement; under-counting fills
                    // a disk.
                    driver != "nfs"
                });
            node_local.then_some(size)
        })
        .sum()
}

/// Free disk on a node, from the kitchen-sink fingerprint
/// (`disk_available_space`, bytes as a string). `None` when the node does not
/// report it — the disk check is skipped for such nodes rather than excluding
/// them on missing data.
fn fingerprint_disk_free_bytes(node: &Node) -> Option<u64> {
    match node.fingerprint_attrs.get("disk_available_space")? {
        Value::String(raw) => raw.parse().ok(),
        Value::Number(number) => number.as_u64(),
        _ => None,
    }
}

fn group_nodes_by_attr<'a>(nodes: &'a [&'a Node], key: &str) -> HashMap<String, Vec<&'a Node>> {
    let mut grouped: HashMap<String, Vec<&'a Node>> = HashMap::new();
    for node in nodes {
        if key == "id" {
            grouped.entry(node.id.clone()).or_default().push(*node);
            continue;
        }
        let Some(value) = node_attribute(node, key) else {
            continue;
        };
        let token = value_token(value);
        grouped.entry(token).or_default().push(*node);
    }
    grouped
}

fn score_nodes_for_context<'a>(ctx: &SchedulingContext<'a>) -> Vec<(&'a Node, f64)> {
    if ctx.eligible_nodes.is_empty() {
        return Vec::new();
    }

    let mut min_load: Option<usize> = None;
    let mut max_load: Option<usize> = None;
    let mut loads: HashMap<&'a str, usize> = HashMap::new();

    for node in &ctx.eligible_nodes {
        let load = ctx.total_assigned(&node.id);
        loads.insert(node.id.as_str(), load);
        min_load = Some(min_load.map_or(load, |min| min.min(load)));
        max_load = Some(max_load.map_or(load, |max| max.max(load)));
    }

    let min_load = min_load.unwrap_or_default() as f64;
    let max_load = max_load.unwrap_or_default() as f64;
    let range = (max_load - min_load).max(0.0);

    ctx.eligible_nodes
        .iter()
        .copied()
        .map(|node| {
            let score = score_node_for_context(ctx, node, &loads, min_load, range);
            (node, score)
        })
        .collect()
}

/// Min/max assigned load over `eligible` plus the normalization range —
/// the cheap first pass both the fused selector and the batch scorer share.
#[derive(Debug, Clone, Copy)]
struct CachedLoadBounds {
    min_load: f64,
    max_load: f64,
    range: f64,
}

/// `legion_scheduler_locality_placements_total{outcome}`: how warm the chosen
/// node was for placements that had a content set. `warm` held all of it,
/// `partial` some, `cold` none. The hit ratio is warm over the total.
fn record_locality_placement(score: f64) {
    let outcome = if score >= 1.0 - 1e-9 {
        "warm"
    } else if score > 0.0 {
        "partial"
    } else {
        "cold"
    };
    crate::manager::telemetry::metrics()
        .scheduler_locality_placements_total
        .inc(&[outcome]);
}

fn cached_load_bounds(eligible: &[&Node], scoring_cache: &PlanScoringCache) -> CachedLoadBounds {
    let mut min_load: Option<usize> = None;
    let mut max_load: Option<usize> = None;
    for node in eligible {
        let load = scoring_cache
            .state_of(node)
            .map(|state| state.assigned_count)
            .unwrap_or_default();
        min_load = Some(min_load.map_or(load, |min| min.min(load)));
        max_load = Some(max_load.map_or(load, |max| max.max(load)));
    }
    let min_load = min_load.unwrap_or_default() as f64;
    let max_load = max_load.unwrap_or_default() as f64;
    CachedLoadBounds {
        min_load,
        max_load,
        range: (max_load - min_load).max(0.0),
    }
}

/// One node's score under the cached plan state — the body both the fused
/// single-pass selector and the batch scorer run per candidate.
fn score_node_with_cache(
    node: &Node,
    affinity_set: &AffinitySet,
    scoring_cache: &PlanScoringCache,
    weights: SchedulerWeights,
    bounds: CachedLoadBounds,
) -> f64 {
    let state = scoring_cache.state_of(node);
    let load = state.map(|s| s.assigned_count).unwrap_or_default() as f64;
    let (spread_score, bin_pack_score) = if bounds.range > 0.0 {
        (
            (bounds.max_load - load) / bounds.range,
            (load - bounds.min_load) / bounds.range,
        )
    } else {
        (0.0, 0.0)
    };
    let affinity_score = state
        .map(|s| s.base_affinity + scoring_cache.affinity.colocation_score(s))
        .unwrap_or_else(|| affinity_set.score(node));
    let anti_affinity_penalty = state
        .map(|s| s.same_task_allocations.max(s.same_group_allocations) as f64)
        .unwrap_or_else(|| {
            if scoring_cache.group_name.is_some() {
                1.0
            } else {
                0.0
            }
        });
    let locality_score = match state {
        Some(state) => state.locality,
        None => scoring_cache
            .locality
            .as_ref()
            .map_or(0.0, |scores| scores.score(&node.id)),
    };
    weights.affinity * affinity_score
        + weights.spread * spread_score
        + weights.bin_pack * bin_pack_score
        - weights.automatic_anti_affinity * anti_affinity_penalty
        + weights.locality * locality_score
}

fn score_nodes_with_cache<'a>(
    eligible: &[&'a Node],
    affinity_set: &AffinitySet,
    scoring_cache: &PlanScoringCache,
    weights: SchedulerWeights,
) -> Vec<(&'a Node, f64)> {
    if eligible.is_empty() {
        return Vec::new();
    }
    let bounds = cached_load_bounds(eligible, scoring_cache);
    eligible
        .iter()
        .copied()
        .map(|node| {
            (
                node,
                score_node_with_cache(node, affinity_set, scoring_cache, weights, bounds),
            )
        })
        .collect()
}

fn task_colocation_preferences(affinities: &Value) -> Vec<(&str, f64)> {
    match affinities {
        Value::Object(map) => map
            .get("same_node_as")
            .and_then(Value::as_str)
            .map(|task| vec![(task, 1.0)])
            .unwrap_or_default(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                let obj = item.as_object()?;
                let key = obj.get("key")?.as_str()?;
                if key != "same_node_as" {
                    return None;
                }

                let task = obj.get("value")?.as_str()?;
                let multiplier = obj.get("multiplier").and_then(Value::as_f64).unwrap_or(1.0);
                Some((task, multiplier))
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Borrows through a plan's `Arc`'d node snapshot as plain `&Node`s —
/// everything below candidate construction works on borrows, untouched by
/// how the snapshot is owned.
fn node_refs(nodes: &[Arc<Node>]) -> impl Iterator<Item = &Node> {
    nodes.iter().map(|node| &**node)
}

fn node_attribute<'a>(node: &'a Node, key: &str) -> Option<&'a Value> {
    node.config_attrs
        .as_object()
        .and_then(|attrs| attrs.get(key))
        .or_else(|| {
            node.fingerprint_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
        .or_else(|| {
            node.dynamic_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
}

impl ResourceLedger {
    /// Builds the plan-time ledger from the read model's cached free pools —
    /// no per-node JSON re-parsing. Nodes whose allocation set THIS plan
    /// changed (`freed_nodes`: a Fresh plan drops the job's own allocations
    /// before planning) are recomputed from scratch against the filtered
    /// allocation map, which is exactly the value a full rebuild would
    /// produce; every other node's cached pool already is that value. A
    /// candidate node with no cached pool (removed or drained between the
    /// node and capacity snapshots) simply fits nothing.
    fn from_snapshot(
        mut pools: SharedNodePools,
        freed_nodes: &HashSet<String>,
        node_lookup: &AvailableNodes,
        allocations_by_node: &SharedNodeAllocations,
        policy: &ResourcePolicy,
    ) -> Self {
        for node_id in freed_nodes {
            let Some(node) = node_lookup.get(node_id.as_str()) else {
                continue;
            };
            let allocations = allocations_by_node
                .get(node_id)
                .map(|allocations| allocations.as_slice())
                .unwrap_or(&[]);
            pools.insert(
                node_id.clone(),
                Arc::new(ResourcePool::free_for_node(node, allocations, policy)),
            );
        }
        ResourceLedger { pools }
    }

    fn can_fit(&self, node_id: &str, request: &ResourceRequest) -> bool {
        self.pools
            .get(node_id)
            .map(|pool| pool.can_fit(request))
            .unwrap_or(false)
    }

    fn consume(&mut self, node_id: &str, request: &ResourceRequest) -> R<()> {
        let pool = self
            .pools
            .get_mut(node_id)
            .context("missing resource pool")?;
        if !pool.can_fit(request) {
            bail!("node {node_id} does not have required resources");
        }
        // Copy-on-write: only pools a plan debits are ever deep-cloned.
        Arc::make_mut(pool).consume(request);
        Ok(())
    }

    /// Credit a victim's resources back to its node, so the placements that
    /// paid for the eviction can see the capacity.
    ///
    /// This is bookkeeping *within one plan*. It never lies about the
    /// cluster: the eviction it models commits in the same transaction as the
    /// placement, fenced by the same epoch compare-and-set, so no other plan
    /// can observe the freed capacity without also conflicting.
    fn release(&mut self, node_id: &str, request: &ResourceRequest) {
        if let Some(pool) = self.pools.get_mut(node_id) {
            Arc::make_mut(pool).release(request);
        }
    }
}

fn reschedule_rng() -> StdRng {
    if let Ok(seed) = std::env::var("LEGION_RESCHEDULE_SEED") {
        if let Ok(seed) = seed.parse::<u64>() {
            return StdRng::seed_from_u64(seed);
        }
    }
    if cfg!(test) {
        return StdRng::seed_from_u64(7);
    }
    StdRng::from_entropy()
}

fn value_token(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use std::collections::{HashMap, HashSet};
    use tempfile::Builder;

    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::{
        JobConfig, NodeMsg, Submission, TaskCount, TaskDependencyCondition, TaskLifecycle,
    };

    fn store_path() -> R<String> {
        let store_dir = Builder::new()
            .prefix("khepri-scheduler-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();

        Ok(store_dir
            .to_str()
            .context("Error parsing store path")?
            .to_string())
    }

    fn setup_store() -> R<Store> {
        setup_store_with_scheduler("bin_pack", None)
    }

    fn setup_store_with_scheduler(
        scheduling_policy: &str,
        scheduler_settings: Option<Value>,
    ) -> R<Store> {
        let store_config = ManagerConfigFull {
            store_path: store_path()?,
            scheduling_policy: scheduling_policy.into(),
            scheduler_settings,
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;

        Ok(store)
    }

    fn make_node_msg(id: &str, attrs: Value) -> NodeMsg {
        make_node_msg_with_sources(id, attrs, default_fingerprint(), json!({}))
    }

    fn make_node_msg_with_sources(
        id: &str,
        config_attrs: Value,
        fingerprint_attrs: Value,
        dynamic_attrs: Value,
    ) -> NodeMsg {
        let fingerprint_attrs = merge_fingerprint(fingerprint_attrs);
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: config_attrs.to_string(),
            fingerprint_attrs: fingerprint_attrs.to_string(),
            dynamic_attrs: dynamic_attrs.to_string(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
        }
    }

    fn default_fingerprint() -> Value {
        json!({
            "memory_total": "8589934592",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
    }

    fn merge_fingerprint(extra: Value) -> Value {
        let mut base = default_fingerprint()
            .as_object()
            .cloned()
            .unwrap_or_default();
        if let Value::Object(additional) = extra {
            for (key, value) in additional {
                base.insert(key, value);
            }
        }
        Value::Object(base)
    }

    fn insert_test_nodes(store: &Store) -> R<()> {
        store.insert_node(make_node_msg("node-a", json!({ "tier": "edge" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "db" })))?;

        Ok(())
    }

    /// The scheduling-index equivalence property on the `on` path, asserted
    /// directly: untruncated index-served candidates are the scan's set in
    /// the scan's (by-id) order, across constraint shapes. Shadow mode
    /// asserts the same live comparison in the integration suite; this pins
    /// the mode that actually serves.
    #[test]
    fn indexed_candidates_equal_scan_candidates_untruncated() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({
                "index_min_nodes": 1,
                "indexes": {
                    "fit": false,
                    "topo": ["pool", "region"],
                },
            })),
        )?;
        for idx in 0..40 {
            store.insert_node(make_node_msg(
                &format!("node-{idx:02}"),
                json!({
                    "region": format!("region-{}", idx % 4),
                    "tier": if idx % 2 == 0 { "web" } else { "batch" },
                }),
            ))?;
        }
        let scheduler = Scheduler::new(&store)?;
        let node_set = store.available_nodes()?;
        let nodes = node_set.as_slice();
        let node_lookup: &AvailableNodes = &node_set;
        let no_drivers = std::collections::BTreeSet::new();
        let request = ResourceRequest::from_value(&json!({ "memory": 64 }))?;

        for constraints in [
            json!({ "region": "region-1" }),
            json!({ "region": "region-2", "tier": "web" }),
            json!([
                { "key": "region", "value": "region-3" },
                { "key": "tier", "operator": "matches", "value": "^b" },
            ]),
        ] {
            let constraint_set = ConstraintSet::new(&constraints)?;
            let permit_set = PermitSet::new(&Value::Null)?;
            let scan = Scheduler::scan_base_eligible(
                node_refs(nodes),
                &constraint_set,
                &permit_set,
                &no_drivers,
            );
            let mut usage = TaskIndexUsage::scan("prop");
            let indexed = scheduler.indexed_base_eligible(
                nodes,
                node_lookup,
                &constraint_set,
                &permit_set,
                &request,
                &no_drivers,
                2,
                false,
                &mut usage,
            )?;
            assert_eq!(usage.index.as_deref(), Some("topo"), "{constraints}");
            let scan_ids: Vec<&str> = scan.iter().map(|node| node.id.as_str()).collect();
            let indexed_ids: Vec<&str> = indexed.iter().map(|node| node.id.as_str()).collect();
            assert!(!scan_ids.is_empty(), "test data must match {constraints}");
            assert_eq!(
                indexed_ids, scan_ids,
                "index-served candidates must equal the scan for {constraints}"
            );
        }

        Ok(())
    }

    fn dummy_submission() -> Submission {
        Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }
    }

    fn task_id_for(store: &Store, job_config_id: &str, task_name: &str) -> R<i64> {
        let task = store
            .get_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .find(|(_, cfg)| cfg.name == task_name)
            .context("missing task")?;

        Ok(task.0)
    }

    fn score_with_legacy_context<'a>(
        eligible: Vec<&'a Node>,
        task: &'a TaskConfig,
        task_id: i64,
        affinity_set: &'a AffinitySet,
        job_config_id: &'a str,
        allocations_by_node: &'a SharedNodeAllocations,
        task_name_to_id: &'a HashMap<String, i64>,
        task_id_to_group: &'a HashMap<i64, String>,
        planned_task_allocations: &'a HashMap<String, Vec<i64>>,
        planned: &'a HashMap<String, usize>,
        weights: SchedulerWeights,
    ) -> Vec<(&'a Node, f64)> {
        let ctx = SchedulingContext::new(
            eligible,
            task,
            task_id,
            affinity_set,
            job_config_id,
            allocations_by_node,
            task_name_to_id,
            task_id_to_group,
            planned_task_allocations,
            planned,
            weights,
        );
        score_nodes_for_context(&ctx)
    }

    #[test]
    fn plans_fresh_allocations_and_deltas() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "sched-job".into(),
            tasks: vec![
                TaskConfig {
                    count: TaskCount::new(2),
                    ..TaskConfig::new("web", "noop")
                },
                TaskConfig {
                    constraints: json!({ "role": "db" }),
                    ..TaskConfig::new("db", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 3);

        let web_id = task_id_for(&store, &job_config_id, "web")?;
        let db_id = task_id_for(&store, &job_config_id, "db")?;

        let web_on_node_a = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == web_id && alloc.node_id == "node-a")
            .count();
        let web_on_node_b = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == web_id && alloc.node_id == "node-b")
            .count();
        assert_eq!(web_on_node_a + web_on_node_b, 2);

        let db_on_node_b = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == db_id && alloc.node_id == "node-b")
            .count();
        assert_eq!(db_on_node_b, 1);

        assert!(!outcome.deltas.is_empty());
        let total_added: usize = outcome.deltas.iter().map(|delta| delta.added.len()).sum();
        let total_removed: usize = outcome.deltas.iter().map(|delta| delta.removed.len()).sum();
        assert_eq!(total_added, 3);
        assert_eq!(total_removed, 0);

        Ok(())
    }

    #[test]
    fn reschedule_only_places_missing_allocations() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "resched-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(3),
                ..TaskConfig::new("batch", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let batch_id = task_id_for(&store, &job_config_id, "batch")?;
        store.place_alloc("node-a", batch_id)?;
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        assert_eq!(outcome.allocations.len(), 2);
        assert_eq!(outcome.deltas.len(), 1);

        let node_a_added = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.node_id == "node-a")
            .count();
        let node_b_added = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.node_id == "node-b")
            .count();
        assert_eq!(node_a_added + node_b_added, 2);

        Ok(())
    }

    fn stateful_slot_task(name: &str, count: u32) -> TaskConfig {
        TaskConfig {
            count: TaskCount::new(count),
            stateful: Some(crate::structs::StatefulConfig {
                mode: crate::structs::InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    fn submit_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<String> {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        Ok(store.get_job_config(name)?.id())
    }

    /// Two equal nodes, one already holding the task's pinned artifact: the
    /// placement lands on the warm one, every time. Without locality this
    /// is a coin flip (bin_pack on two empty nodes ties).
    #[test]
    fn placement_prefers_the_node_holding_the_content() -> R<()> {
        use crate::storage::content_index::IngestOutcome;
        use crate::structs::{ArtifactSpec, ContentLocation, ContentLocationReport};
        let digest = format!("sha256:{}", "ab".repeat(32));
        for round in 0..6 {
            let store = setup_store()?;
            store.insert_node(make_node_msg("node-a", json!({})))?;
            store.insert_node(make_node_msg("node-b", json!({})))?;
            let report = |added: Vec<ContentLocation>| ContentLocationReport {
                epoch: 1,
                base_epoch: 0,
                full: true,
                added,
                removed: Vec::new(),
                snapshot_budget: None,
            };
            let now = std::time::Instant::now();
            assert_eq!(
                store.content_locations().ingest(
                    "node-b",
                    &report(vec![ContentLocation {
                        digest: digest.clone(),
                        bytes: 64 * 1024 * 1024,
                    }]),
                    now,
                ),
                IngestOutcome::Replaced
            );
            // node-a reported too, holding nothing.
            store
                .content_locations()
                .ingest("node-a", &report(Vec::new()), now);

            let mut task = TaskConfig::new("agent".to_string(), "noop");
            task.artifacts = vec![ArtifactSpec {
                checksum: Some(digest.clone()),
                ..ArtifactSpec::from_source("https://example.com/agent.bin")
            }];
            let job_config_id = submit_job(&store, &format!("warm-{round}"), vec![task])?;
            let scheduler = Scheduler::new(&store)?;
            assert_eq!(scheduler.weights.locality, DEF_SCHEDULER_LOCALITY_WEIGHT);
            let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
            assert_eq!(outcome.allocations.len(), 1);
            assert_eq!(outcome.allocations[0].node_id, "node-b", "round {round}");
        }
        Ok(())
    }

    /// The content set is the task's digest-pinned artifacts; tasks without
    /// any skip the term entirely.
    #[test]
    fn content_set_is_the_pinned_artifact_digests() -> R<()> {
        use crate::structs::ArtifactSpec;
        let store = setup_store()?;
        let scheduler = Scheduler::new(&store)?;
        let digest = format!("sha256:{}", "cd".repeat(32));
        let mut task = TaskConfig::new("agent".to_string(), "noop");
        assert!(scheduler.content_set(&task, &[]).is_empty());
        assert!(scheduler
            .locality_scores(&task, &[], scheduler.weights)
            .is_none());
        task.artifacts = vec![
            ArtifactSpec {
                checksum: Some(digest.clone()),
                ..ArtifactSpec::from_source("https://example.com/a.bin")
            },
            ArtifactSpec::from_source("https://example.com/unpinned.bin"),
        ];
        assert_eq!(
            scheduler.content_set(&task, &[]),
            vec![ContentRef::digest(digest.clone())]
        );

        // A restoring instance adds its manifest's memory parts, sized.
        let manifest = serde_json::json!({
            "id": "snap-1", "jobId": "j", "task": "agent", "originAllocId": 7,
            "runtime": "sandbox", "kind": "memory", "createdAt": "2026-10-10T00:00:00Z",
            "memory": [{ "role": "memory", "file": "mem", "digest": "sha256:ee", "size": 4096 }],
        });
        let restore = crate::storage::store::RestorePin {
            instance_id: "user-1".into(),
            snapshot_id: "snap-1".into(),
            node_id: Some("node-a".into()),
            manifest_json: manifest.to_string(),
            published: false,
            spread: false,
        };
        assert_eq!(
            scheduler.content_set(&task, std::slice::from_ref(&restore)),
            vec![
                ContentRef::digest(digest),
                ContentRef::sized("sha256:ee", 4096)
            ]
        );
        Ok(())
    }

    /// `weights.locality` parses like every other weight; 0 turns it off.
    #[test]
    fn locality_weight_is_configurable() -> R<()> {
        let store =
            setup_store_with_scheduler("spread", Some(json!({ "weights": { "locality": 0.0 } })))?;
        assert_eq!(Scheduler::new(&store)?.weights.locality, 0.0);
        let store =
            setup_store_with_scheduler("spread", Some(json!({ "weights": { "locality": 2.5 } })))?;
        assert_eq!(Scheduler::new(&store)?.weights.locality, 2.5);
        Ok(())
    }

    #[test]
    fn slot_mode_initial_plan_assigns_slots_zero_through_n_minus_one() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "stateful-initial";
        let job_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 3)])?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 3);

        let mut instance_ids: Vec<String> = outcome
            .allocations
            .iter()
            .map(|alloc| {
                alloc
                    .instance_id
                    .clone()
                    .expect("stateful alloc has instance")
            })
            .collect();
        instance_ids.sort();
        assert_eq!(
            instance_ids,
            vec![
                "stateful-initial-default-0",
                "stateful-initial-default-1",
                "stateful-initial-default-2",
            ]
        );

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3);
        for (slot, instance) in instances.iter().enumerate() {
            assert_eq!(instance.slot_index, Some(slot as u32));
            assert_eq!(instance.state, InstanceState::Active);
            assert!(instance
                .storage_handle
                .as_deref()
                .is_some_and(|handle| handle.starts_with("local-dir/")));
            let bound_alloc = outcome
                .allocations
                .iter()
                .find(|alloc| alloc.instance_id.as_deref() == Some(instance.instance_id.as_str()))
                .expect("instance bound to an allocation");
            assert_eq!(instance.current_alloc_id, Some(bound_alloc.id));
        }
        Ok(())
    }

    #[test]
    fn killed_slot_allocation_replacement_reuses_instance_and_storage() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "stateful-resched";
        let job_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 3)])?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let slot1_alloc = outcome
            .allocations
            .iter()
            .find(|alloc| alloc.instance_id.as_deref() == Some("stateful-resched-default-1"))
            .cloned()
            .expect("slot 1 placed");
        let slot1_instance = store
            .get_instance("stateful-resched-default-1")?
            .expect("slot 1 instance");
        let original_handle = slot1_instance.storage_handle.clone();

        // Kill slot 1's allocation: the instance survives in rescheduling.
        store.remove_allocation_by_id(slot1_alloc.id)?;
        let waiting = store
            .get_instance("stateful-resched-default-1")?
            .expect("instance retained");
        assert_eq!(waiting.state, InstanceState::Rescheduling);

        // The reschedule plan places exactly one replacement that reuses slot
        // 1's instance id and reattaches the same storage handle.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        let replacement = &outcome.allocations[0];
        assert_eq!(
            replacement.instance_id.as_deref(),
            Some("stateful-resched-default-1")
        );
        assert_ne!(replacement.id, slot1_alloc.id);

        let reactivated = store
            .get_instance("stateful-resched-default-1")?
            .expect("instance reactivated");
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.current_alloc_id, Some(replacement.id));
        assert_eq!(reactivated.storage_handle, original_handle);
        Ok(())
    }

    #[test]
    fn slot_reuse_survives_lost_node_reschedule() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "stateful-lost-node";
        let job_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 2)])?;

        let scheduler = Scheduler::new(&store)?;
        scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let handles_before: HashMap<String, Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| (instance.instance_id.clone(), instance.storage_handle))
            .collect();

        store.mark_node_lost("node-a")?;
        store.remove_allocs_for_node("node-a")?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        for alloc in &outcome.allocations {
            assert_eq!(alloc.node_id, "node-b");
            let instance_id = alloc.instance_id.as_deref().expect("instance id");
            let instance = store.get_instance(instance_id)?.expect("instance");
            assert_eq!(
                &instance.storage_handle,
                handles_before.get(instance_id).expect("known instance"),
                "storage handle reattached on the new node"
            );
        }
        Ok(())
    }

    fn stateful_keyed_task(name: &str) -> TaskConfig {
        TaskConfig {
            // count.num is deliberately non-zero to prove it is ignored for
            // keyed tasks: live instances drive the desired count.
            count: TaskCount::new(3),
            stateful: Some(crate::structs::StatefulConfig {
                mode: crate::structs::InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    #[test]
    fn keyed_task_with_zero_instances_plans_zero_allocations() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-zero";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;

        // count.num = 3, but no instances exist: nothing to place.
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert!(outcome.allocations.is_empty(), "count.num must be ignored");
        assert!(store.allocs_for_job(job_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn keyed_instances_drive_desired_count_and_each_binds_a_distinct_instance() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-drive";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;
        for key in ["user-a", "user-b"] {
            store.create_keyed_instance(job_id, "default", "sandbox", Some(key))?;
        }

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2, "one allocation per instance");

        let mut bound: Vec<String> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.instance_id.clone().expect("keyed alloc bound"))
            .collect();
        bound.sort();
        assert_eq!(bound, vec!["user-a", "user-b"]);

        for instance in store.list_instances(job_id)? {
            assert_eq!(instance.state, InstanceState::Active);
            assert_eq!(instance.slot_index, None);
            assert!(instance
                .storage_handle
                .as_deref()
                .is_some_and(|handle| handle.starts_with("local-dir/")));
        }

        // Replanning when everything is placed adds nothing.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert!(outcome.allocations.is_empty());
        Ok(())
    }

    #[test]
    fn killed_keyed_allocation_replacement_reuses_id_and_storage() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-resched-plan";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-b"))?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let victim = outcome
            .allocations
            .iter()
            .find(|alloc| alloc.instance_id.as_deref() == Some("user-a"))
            .cloned()
            .expect("user-a placed");
        let original_handle = store
            .get_instance("user-a")?
            .expect("instance")
            .storage_handle;

        store.remove_allocation_by_id(victim.id)?;
        assert_eq!(
            store.get_instance("user-a")?.expect("retained").state,
            InstanceState::Rescheduling
        );

        // Exactly one replacement, re-bound to the same identity + storage.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(
            outcome.allocations[0].instance_id.as_deref(),
            Some("user-a")
        );
        let reactivated = store.get_instance("user-a")?.expect("reactivated");
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.storage_handle, original_handle);
        Ok(())
    }

    #[test]
    fn destroyed_keyed_instance_is_not_replaced_by_reconciliation() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-destroyed";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-b"))?;

        let scheduler = Scheduler::new(&store)?;
        scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        store.destroy_keyed_instance("user-b")?;

        // The tombstone is not desired: nothing is re-placed for it.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert!(outcome.allocations.is_empty());
        let allocations = store.allocs_for_job(job_id)?;
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].instance_id.as_deref(), Some("user-a"));
        Ok(())
    }

    #[test]
    fn stateless_job_plans_carry_no_instance_metadata() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "plain-job";
        let job_config_id = submit_job(
            &store,
            job_id,
            vec![TaskConfig {
                count: TaskCount::new(2),
                ..TaskConfig::new("web", "noop")
            }],
        )?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2);
        assert!(outcome
            .allocations
            .iter()
            .all(|alloc| alloc.instance_id.is_none()));
        assert!(store.list_instances(job_id)?.is_empty());

        // Reschedule after losing an allocation behaves exactly as today.
        store.remove_allocation_by_id(outcome.allocations[0].id)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        assert!(outcome.allocations[0].instance_id.is_none());
        assert!(store.list_instances(job_id)?.is_empty());
        Ok(())
    }

    /// The staleness half of the storm-drill repro: a fleet that GROWS
    /// after the available-nodes cache is first warmed. On the rig, nodes
    /// register over ~20 seconds while the manager is already serving
    /// planner reads; if registration does not invalidate the cache, every
    /// later plan sees only the early registrants — which is exactly the
    /// observed shape (25,002 allocations on the first-registered slice).
    #[test]
    fn nodes_registered_after_a_plan_are_visible_to_the_next_plan() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        for index in 0..25 {
            store.insert_node(make_node_msg(&format!("n-{index:03}"), json!({})))?;
        }

        // Warm the cache: plan and apply one job against the small fleet.
        let warm = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "warm".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(10),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&warm, &[dummy_submission()])?;
        let warm_id = store.get_job_config(&warm.name)?.id();
        Scheduler::new(&store)?.plan_and_apply(&warm_id, PlanMode::Fresh)?;

        // The fleet quadruples AFTER that plan.
        for index in 25..100 {
            store.insert_node(make_node_msg(&format!("n-{index:03}"), json!({})))?;
        }

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "after-growth".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(50),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_id = store.get_job_config(&job.name)?.id();
        let outcome = Scheduler::new(&store)?.plan_and_apply(&job_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 50);

        let on_new_nodes = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.node_id.as_str() >= "n-025")
            .count();
        println!("on nodes registered after the first plan: {on_new_nodes}/50");
        // 75 of 100 nodes are new and empty; spread should put the great
        // majority there. Zero means the planner never saw them.
        assert!(
            on_new_nodes >= 25,
            "planner is blind to late-registered nodes: {on_new_nodes}/50 landed on them"
        );
        Ok(())
    }

    /// Repro for the 2026-08-06 storm-drill finding: 25,002 spread-policy
    /// allocations all landed on the first-registered quarter of a
    /// 5,000-node fleet, and their replacements packed the NEXT quarter.
    /// Spread on a uniformly empty fleet must not concentrate.
    #[test]
    fn spread_on_an_empty_fleet_does_not_concentrate_on_low_node_ids() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        for index in 0..100 {
            store.insert_node(make_node_msg(&format!("n-{index:03}"), json!({})))?;
        }

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "spread-repro".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(50),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 50);

        // 50 replicas over 100 empty identical nodes: the bottom half of the
        // id ordering should NOT hold materially more than the top half. On
        // the fleet that motivated this test the bottom quarter held 100%.
        let low = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.node_id.as_str() < "n-050")
            .count();
        let distinct: std::collections::HashSet<&str> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        println!("low-half {low}/50, distinct nodes {}", distinct.len());
        assert!(
            (15..=35).contains(&low),
            "spread concentrated: {low}/50 on the low half of the id order"
        );
        assert!(
            distinct.len() >= 40,
            "spread stacked replicas: only {} distinct nodes for 50 allocations",
            distinct.len()
        );
        Ok(())
    }

    #[test]
    fn count_per_attribute_places_allocations_per_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-attr-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 4);

        let mut counts: HashMap<String, usize> = HashMap::new();
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            let region = node
                .config_attrs
                .get("region")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            *counts.entry(region).or_default() += 1;
        }

        assert_eq!(counts.get("east"), Some(&2));
        assert_eq!(counts.get("west"), Some(&2));

        Ok(())
    }

    #[test]
    fn count_per_attribute_reschedules_within_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-attr-resched".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;

        for _ in 0..2 {
            store.place_alloc("node-a", task_id)?;
        }
        for _ in 0..2 {
            store.place_alloc("node-c", task_id)?;
        }

        store.mark_node_lost("node-a")?;
        store.remove_allocs_for_node("node-a")?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        assert_eq!(outcome.allocations.len(), 2);
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            let region = node
                .config_attrs
                .get("region")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            assert_eq!(region, "east");
        }

        Ok(())
    }

    #[test]
    fn count_per_attribute_respects_constraints() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "region": "east", "role": "db" }),
        ))?;
        store.insert_node(make_node_msg(
            "node-b",
            json!({ "region": "east", "role": "web" }),
        ))?;
        store.insert_node(make_node_msg(
            "node-c",
            json!({ "region": "west", "role": "db" }),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-attr-constraints".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(1, "region"),
                constraints: json!({ "role": "db" }),
                ..TaskConfig::new("db", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            let role = node
                .config_attrs
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            assert_eq!(role, "db");
        }

        Ok(())
    }

    #[test]
    fn reschedule_excludes_failed_node() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "resched-exclude".into(),
            tasks: vec![TaskConfig::new("batch", "noop")],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let excluded = HashSet::from(["node-a".to_string()]);
        let outcome = scheduler.plan_and_apply_with_exclusions(
            &job_config_id,
            PlanMode::Reschedule,
            Some(&excluded),
        )?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-b");

        Ok(())
    }

    #[test]
    fn errors_when_constraints_cannot_be_satisfied() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "tier": "edge" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "unschedulable".into(),
            tasks: vec![TaskConfig {
                constraints: json!({ "role": "db" }),
                ..TaskConfig::new("analytics", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected scheduling error");

        assert!(err.to_string().contains("Failed to place task analytics"));

        Ok(())
    }

    #[test]
    fn constraint_matching_across_node_attributes() -> R<()> {
        let node = Node {
            id: "node-mixed".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "role": "db" }),
            fingerprint_attrs: json!({ "arch": "arm64" }),
            dynamic_attrs: json!({ "zone": "us-east" }),
            health_checks: json!({}),
        };

        let constraints = json!({
            "role": "db",
            "arch": "arm64",
            "zone": "us-east",
        });

        let constraint_set = ConstraintSet::new(&constraints)?;
        assert!(constraint_set.is_satisfied(&node, None));

        let mismatched = json!({ "role": "web", "arch": "arm64", "zone": "us-east" });
        let mismatched_set = ConstraintSet::new(&mismatched)?;
        assert!(!mismatched_set.is_satisfied(&node, None));

        Ok(())
    }

    #[test]
    fn scheduler_uses_nodes_matching_constraints_from_any_attribute_source() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;
        store.insert_node(make_node_msg_with_sources(
            "node-b",
            json!({}),
            json!({ "role": "db" }),
            json!({}),
        ))?;
        store.insert_node(make_node_msg_with_sources(
            "node-c",
            json!({}),
            json!({}),
            json!({ "role": "db" }),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "constraint-sources".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                constraints: json!({ "role": "db" }),
                ..TaskConfig::new("db", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            let constraint_set = ConstraintSet::new(&json!({ "role": "db" }))?;
            assert!(constraint_set.is_satisfied(&node, None));
        }

        Ok(())
    }

    #[test]
    fn distinct_constraints_spread_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "zone": "us-east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "zone": "us-west" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "distinct-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                constraints: json!([{ "key": "zone", "operator": "distinct" }]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        let node_ids: HashSet<_> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        assert_eq!(node_ids.len(), 2);

        Ok(())
    }

    #[test]
    fn distinct_constraints_cache_selection_matches_legacy_scoring() -> R<()> {
        let nodes = vec![
            Node {
                id: "node-a".into(),
                status: "active".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat: String::new(),
                heartbeat_source: String::new(),
                config_attrs: json!({"zone": "us-east"}),
                fingerprint_attrs: default_fingerprint(),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            },
            Node {
                id: "node-b".into(),
                status: "active".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat: String::new(),
                heartbeat_source: String::new(),
                config_attrs: json!({"zone": "us-west"}),
                fingerprint_attrs: default_fingerprint(),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            },
        ];
        let candidate_refs = nodes.iter().collect::<Vec<_>>();
        let task = TaskConfig {
            count: TaskCount::new(2),
            constraints: json!([{ "key": "zone", "operator": "distinct" }]),
            ..TaskConfig::new("api", "noop")
        };
        let task_id = 7_i64;
        let allocations_by_node = SharedNodeAllocations::new();
        let planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();
        let planned: HashMap<String, usize> = HashMap::new();
        let task_name_to_id = HashMap::from([(task.name.clone(), task_id)]);
        let task_id_to_group = HashMap::new();
        let affinity_set = AffinitySet::new(&task.affinities)?;
        let constraint_set = ConstraintSet::new(&task.constraints)?;
        let mut distinct_tracker = DistinctTracker::new(&constraint_set.distinct_keys());
        let own_by_node = OwnAllocationsByNode::new();
        let mut pools = HashMap::new();
        for node in &nodes {
            pools.insert(node.id.clone(), Arc::new(ResourcePool::from_node(node)?));
        }
        let mut resource_ledger = ResourceLedger {
            pools: pools.into(),
        };
        let request =
            ResourceRequest::from_value(&json!({"memory": 256, "cpu": 100, "allocations": 1}))?;
        let scoring_cache = PlanScoringCache::from_plan(
            &candidate_refs,
            task_id,
            &task,
            &affinity_set,
            &allocations_by_node,
            &own_by_node,
            &task_name_to_id,
            &task_id_to_group,
            &planned_task_allocations,
            &planned,
            &resource_ledger,
            &request,
        );

        let eligible = Scheduler::eligible_nodes(
            &candidate_refs,
            &constraint_set,
            Some(&distinct_tracker),
            &scoring_cache,
        );
        let legacy_scored = score_with_legacy_context(
            eligible.clone(),
            &task,
            task_id,
            &affinity_set,
            "job-1",
            &allocations_by_node,
            &task_name_to_id,
            &task_id_to_group,
            &planned_task_allocations,
            &planned,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let cached_scored = score_nodes_with_cache(
            &eligible,
            &affinity_set,
            &scoring_cache,
            SchedulerWeights::for_policy("bin_pack"),
        );

        assert_eq!(legacy_scored.len(), cached_scored.len());
        for ((legacy_node, legacy_score), (cached_node, cached_score)) in
            legacy_scored.iter().zip(cached_scored.iter())
        {
            assert_eq!(legacy_node.id, cached_node.id);
            assert!((legacy_score - cached_score).abs() <= 1e-9);
        }

        let chosen = eligible[0];
        distinct_tracker.insert_from_node(chosen);
        resource_ledger.consume(&chosen.id, &request)?;

        Ok(())
    }

    #[test]
    fn apply_time_constraints_are_rechecked() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "apply-constraints".into(),
            tasks: vec![TaskConfig {
                constraints: json!([{ "key": "role", "value": "db" }]),
                ..TaskConfig::new("db", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "db")?;

        let err = store
            .apply_allocation_plan(&[(String::from("node-a"), task_id)])
            .expect_err("expected constraint failure");
        assert!(err.to_string().contains("does not satisfy constraints"));

        Ok(())
    }

    #[test]
    fn permit_allows_single_restriction() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "restriction": { "foo": "bar" } }),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "permit-single".into(),
            tasks: vec![TaskConfig {
                permit: json!([{ "key": "foo", "operator": "equal", "value": "bar" }]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-a");
        Ok(())
    }

    #[test]
    fn permit_handles_multiple_restrictions_pass_and_fail() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "restriction": { "foo": "bar", "number": 4 } }),
        ))?;

        let passing_job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "permit-multi-pass".into(),
            tasks: vec![TaskConfig {
                permit: json!([
                    { "key": "foo", "operator": "equal", "value": "bar" },
                    { "key": "number", "operator": "equal", "value": 4 }
                ]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&passing_job, &[dummy_submission()])?;
        let pass_job_config_id = store.get_job_config(&passing_job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let pass_outcome = scheduler.plan_and_apply(&pass_job_config_id, PlanMode::Fresh)?;
        assert_eq!(pass_outcome.allocations.len(), 1);

        let failing_job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "permit-multi-fail".into(),
            tasks: vec![TaskConfig {
                permit: json!([
                    { "key": "foo", "operator": "equal", "value": "bar" },
                    { "key": "number", "operator": "equal", "value": 5 }
                ]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&failing_job, &[dummy_submission()])?;
        let fail_job_config_id = store.get_job_config(&failing_job.name)?.id();
        let fail_err = scheduler
            .plan_and_apply(&fail_job_config_id, PlanMode::Fresh)
            .expect_err("expected permit failure");
        assert!(fail_err
            .to_string()
            .contains("no available nodes matching constraints, permit, or resources"));

        Ok(())
    }

    #[test]
    fn permit_defaults_to_default_pool_when_no_restrictions() -> R<()> {
        let store = setup_store()?;
        let mut node_default = make_node_msg("node-a", json!({}));
        node_default.pool = "default".into();
        store.insert_node(node_default)?;
        let mut node_other = make_node_msg("node-b", json!({}));
        node_other.pool = "gpu".into();
        store.insert_node(node_other)?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "permit-default".into(),
            tasks: vec![TaskConfig::new("api", "noop")],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-a");

        Ok(())
    }

    #[test]
    fn resource_limits_prevent_overcommit() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({}),
            json!({
                "memory_total": "536870912",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "mem-limited".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(3),
                resources: json!({ "memory": 256 }),
                ..TaskConfig::new("worker", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected resource scheduling failure");

        assert!(err
            .to_string()
            .contains("no available nodes matching constraints, permit, or resources"));

        Ok(())
    }

    #[test]
    fn resource_limit_cache_feasibility_matches_ledger() -> R<()> {
        let node = Node {
            id: "node-a".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({
                "memory_total": "536870912",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        let nodes = [node];
        let candidate_refs = nodes.iter().collect::<Vec<_>>();
        let task = TaskConfig {
            count: TaskCount::new(3),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("worker", "noop")
        };
        let task_id = 11_i64;
        let request =
            ResourceRequest::from_value(&json!({"memory": 256, "cpu": 100, "allocations": 1}))?;
        let mut pools = HashMap::new();
        pools.insert(
            "node-a".to_string(),
            Arc::new(ResourcePool::from_node(&nodes[0])?),
        );
        let mut ledger = ResourceLedger {
            pools: pools.into(),
        };
        let allocations_by_node = SharedNodeAllocations::new();
        let mut planned: HashMap<String, usize> = HashMap::new();
        let mut planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();
        let task_name_to_id = HashMap::from([(task.name.clone(), task_id)]);
        let task_id_to_group = HashMap::new();
        let affinity_set = AffinitySet::new(&task.affinities)?;
        let own_by_node = OwnAllocationsByNode::new();
        let mut cache = PlanScoringCache::from_plan(
            &candidate_refs,
            task_id,
            &task,
            &affinity_set,
            &allocations_by_node,
            &own_by_node,
            &task_name_to_id,
            &task_id_to_group,
            &planned_task_allocations,
            &planned,
            &ledger,
            &request,
        );

        assert!(cache.can_fit_task_request(&nodes[0]));
        ledger.consume("node-a", &request)?;
        *planned.entry("node-a".to_string()).or_default() += 1;
        planned_task_allocations
            .entry("node-a".to_string())
            .or_default()
            .push(task_id);
        cache.update_after_placement(&nodes[0], task_id, &task_id_to_group, &ledger, &request);
        assert!(cache.can_fit_task_request(&nodes[0]));

        ledger.consume("node-a", &request)?;
        *planned.entry("node-a".to_string()).or_default() += 1;
        planned_task_allocations
            .entry("node-a".to_string())
            .or_default()
            .push(task_id);
        cache.update_after_placement(&nodes[0], task_id, &task_id_to_group, &ledger, &request);
        assert!(!cache.can_fit_task_request(&nodes[0]));
        assert_eq!(
            ledger.can_fit("node-a", &request),
            cache.can_fit_task_request(&nodes[0])
        );

        Ok(())
    }

    fn node_with_fingerprint(fingerprint_attrs: Value) -> Node {
        Node {
            id: "node-a".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs,
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    #[test]
    fn default_task_request_fits_an_apple_silicon_node() -> R<()> {
        // Regression: with the frequency summed as MHz this node advertised
        // 40 MHz against the 100 MHz per-task default, so no job could ever
        // be placed on an Apple Silicon worker.
        let cpus = (1..=10)
            .map(|i| (format!("cpu-{i}"), json!({ "frequency": "4" })))
            .collect::<serde_json::Map<_, _>>();
        let node = node_with_fingerprint(json!({
            "memory_total": "34359738368",
            "cpus": cpus,
        }));
        let pool = ResourcePool::from_node(&node)?;
        let request =
            ResourceRequest::from_value(&json!({ "memory": 128, "cpu": 100, "allocations": 1 }))?;

        assert!(pool.can_fit(&request));
        Ok(())
    }

    #[test]
    fn max_allocations_limits_node_capacity() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({ "maxAllocations": 1 }),
            json!({
                "memory_total": "1073741824",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "alloc-limit".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 128 }),
                ..TaskConfig::new("worker", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected max allocations failure");

        assert!(err
            .to_string()
            .contains("no available nodes matching constraints, permit, or resources"));

        Ok(())
    }

    #[test]
    fn spread_limits_nodes_considered_by_count() -> R<()> {
        let mut selected = HashSet::new();
        for _ in 0..20 {
            let store =
                setup_store_with_scheduler("spread", Some(json!({ "nodes_considered": 2 })))?;
            store.insert_node(make_node_msg("node-a", json!({})))?;
            store.insert_node(make_node_msg("node-b", json!({})))?;
            store.insert_node(make_node_msg("node-c", json!({})))?;

            let job = JobConfig {
                priority: Default::default(),
                rollout: None,
                rollout_by_group: Default::default(),
                name: "spread-considered".into(),
                tasks: vec![TaskConfig::new("api", "noop")],
                version: None,
            };

            store.handle_job_submit(&job, &[dummy_submission()])?;
            let job_config_id = store.get_job_config(&job.name)?.id();
            let task_id = task_id_for(&store, &job_config_id, "api")?;

            for _ in 0..3 {
                store.place_alloc("node-a", task_id)?;
            }
            for _ in 0..2 {
                store.place_alloc("node-b", task_id)?;
            }

            let scheduler = Scheduler::new(&store)?;
            let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

            assert_eq!(outcome.allocations.len(), 1);
            selected.insert(outcome.allocations[0].node_id.clone());
        }

        assert!(selected.contains("node-b"));
        assert!(selected.contains("node-c"));

        Ok(())
    }

    #[test]
    fn bin_pack_limits_nodes_considered_by_percentage() -> R<()> {
        let mut selected = HashSet::new();
        for _ in 0..20 {
            let store =
                setup_store_with_scheduler("bin_pack", Some(json!({ "nodes_considered": "50%" })))?;
            store.insert_node(make_node_msg("node-a", json!({})))?;
            store.insert_node(make_node_msg("node-b", json!({})))?;
            store.insert_node(make_node_msg("node-c", json!({})))?;

            let job = JobConfig {
                priority: Default::default(),
                rollout: None,
                rollout_by_group: Default::default(),
                name: "binpack-considered".into(),
                tasks: vec![TaskConfig::new("worker", "noop")],
                version: None,
            };

            store.handle_job_submit(&job, &[dummy_submission()])?;
            let job_config_id = store.get_job_config(&job.name)?.id();
            let task_id = task_id_for(&store, &job_config_id, "worker")?;

            for _ in 0..2 {
                store.place_alloc("node-b", task_id)?;
            }
            for _ in 0..5 {
                store.place_alloc("node-c", task_id)?;
            }

            let scheduler = Scheduler::new(&store)?;
            let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

            assert_eq!(outcome.allocations.len(), 1);
            selected.insert(outcome.allocations[0].node_id.clone());
        }

        assert!(selected.contains("node-b"));
        assert!(selected.contains("node-c"));

        Ok(())
    }

    #[test]
    fn nodes_considered_randomizes_subset_across_runs() -> R<()> {
        let store = setup_store_with_scheduler("spread", Some(json!({ "nodes_considered": 2 })))?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;
        store.insert_node(make_node_msg("node-c", json!({})))?;
        store.insert_node(make_node_msg("node-d", json!({})))?;

        let scheduler = Scheduler::new(&store)?;
        let nodes = scheduler.available_nodes()?;
        let eligible = node_refs(nodes.as_slice()).collect::<Vec<_>>();
        let setting = NodesConsidered::Count(2);

        let mut subsets = HashSet::new();
        let mut rng = StdRng::seed_from_u64(13);
        for _ in 0..20 {
            let subset = scheduler.apply_nodes_considered_with_setting(
                eligible.clone(),
                Some(&setting),
                &mut rng,
            )?;
            let mut ids = subset
                .iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            ids.sort();
            subsets.insert(ids);
        }

        assert!(
            subsets.len() > 1,
            "expected nodes_considered to vary eligible subset"
        );

        Ok(())
    }

    #[test]
    fn group_tasks_co_locate_on_same_node() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "grouped".into(),
            tasks: vec![
                TaskConfig {
                    group: "default".into(),
                    ..TaskConfig::new("api", "noop")
                },
                TaskConfig {
                    group: "default".into(),
                    ..TaskConfig::new("cache", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let node_ids: HashSet<_> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        assert_eq!(node_ids.len(), 1);

        Ok(())
    }

    #[test]
    fn groups_allow_distinct_constraints() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "db" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "group-constraints".into(),
            tasks: vec![
                TaskConfig {
                    group: "web".into(),
                    constraints: json!({ "role": "web" }),
                    ..TaskConfig::new("frontend", "noop")
                },
                TaskConfig {
                    group: "db".into(),
                    constraints: json!({ "role": "db" }),
                    ..TaskConfig::new("database", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let mut allocations_by_task = HashMap::new();
        for allocation in &outcome.allocations {
            allocations_by_task.insert(allocation.task_config_id, allocation.node_id.clone());
        }
        let node_frontend = allocations_by_task
            .get(&task_id_for(&store, &job_config_id, "frontend")?)
            .cloned()
            .unwrap();
        let node_database = allocations_by_task
            .get(&task_id_for(&store, &job_config_id, "database")?)
            .cloned()
            .unwrap();
        assert_ne!(node_frontend, node_database);

        Ok(())
    }

    #[test]
    fn dag_batch_success_allows_service_within_group() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "dag-group".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "app".into(),
                    ..TaskConfig::new("build", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    group: "app".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "build".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        let node_ids: HashSet<_> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        assert_eq!(node_ids.len(), 1);

        Ok(())
    }

    #[test]
    fn dag_batch_success_across_groups() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "prep" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "app" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "dag-cross".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "prep".into(),
                    constraints: json!({ "role": "prep" }),
                    ..TaskConfig::new("seed", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    group: "app".into(),
                    constraints: json!({ "role": "app" }),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "seed".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("web", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        let mut roles = HashSet::new();
        for allocation in &outcome.allocations {
            let node = store.get_node(&allocation.node_id)?;
            let role = node
                .config_attrs
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            roles.insert(role);
        }
        assert!(roles.contains("prep"));
        assert!(roles.contains("app"));

        Ok(())
    }

    #[test]
    fn required_counts_for_mode_reschedule_only_plans_missing() {
        let tasks = vec![
            (1, TaskConfig::new("api", "noop"), 3),
            (2, TaskConfig::new("worker", "noop"), 2),
        ];
        let allocations_by_node = SharedNodeAllocations::from_iter([
            (
                "node-a".to_string(),
                Arc::new(vec![
                    Allocation {
                        id: 1,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 2,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 2,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                ]),
            ),
            (
                "node-b".to_string(),
                Arc::new(vec![Allocation {
                    id: 3,
                    job_id: "other".into(),
                    job_config_id: "cfg-2".into(),
                    task_config_id: 1,
                    node_id: "node-b".into(),
                    status: "running".into(),
                    resources: json!({}),
                    instance_id: None,
                    network: None,
                }]),
            ),
        ]);

        let mut allocations_by_node = allocations_by_node;
        let job_index = job_index_for(&allocations_by_node, "job-1");
        let (own_by_node, freed_nodes, _) = Scheduler::index_own_allocations(
            "cfg-1",
            "job-1",
            &job_index,
            &mut allocations_by_node,
            SnapshotDrop::Nothing,
        );
        assert!(freed_nodes.is_empty());
        let required =
            Scheduler::required_counts_for_mode(&tasks, PlanMode::Reschedule, &own_by_node);

        assert_eq!(required.get(&1), Some(&2));
        assert_eq!(required.get(&2), Some(&1));
    }

    #[test]
    fn existing_alloc_counts_by_task_node_indexes_by_task_and_node() {
        let allocations_by_node = SharedNodeAllocations::from_iter([
            (
                "node-a".to_string(),
                Arc::new(vec![
                    Allocation {
                        id: 1,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 2,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 3,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 2,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                ]),
            ),
            (
                "node-b".to_string(),
                Arc::new(vec![
                    Allocation {
                        id: 4,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-b".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 5,
                        job_id: "other".into(),
                        job_config_id: "cfg-other".into(),
                        task_config_id: 1,
                        node_id: "node-b".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                ]),
            ),
        ]);

        let mut allocations_by_node = allocations_by_node;
        let job_index = job_index_for(&allocations_by_node, "job-1");
        let (own_by_node, _freed_nodes, _) = Scheduler::index_own_allocations(
            "cfg-1",
            "job-1",
            &job_index,
            &mut allocations_by_node,
            SnapshotDrop::Nothing,
        );
        let indexed = Scheduler::existing_alloc_counts_by_task_node(&own_by_node);

        assert_eq!(indexed.get(&(1, "node-a".to_string())), Some(&2));
        assert_eq!(indexed.get(&(1, "node-b".to_string())), Some(&1));
        assert_eq!(indexed.get(&(2, "node-a".to_string())), Some(&1));
        assert!(!indexed.contains_key(&(2, "node-b".to_string())));
    }

    /// The planned job's entry of the read model's job index, as
    /// `Store::plan_capacity_snapshot` would hand it over.
    fn job_index_for(
        allocations_by_node: &SharedNodeAllocations,
        job_id: &str,
    ) -> JobAllocationsByNode {
        let mut model = crate::storage::store::SchedulingReadModel {
            allocations_by_node: allocations_by_node.clone(),
            ..Default::default()
        };
        model.rebuild_job_index();
        model.allocations_by_job.remove(job_id).unwrap_or_default()
    }

    #[test]
    fn index_own_allocations_drops_and_indexes_from_the_job_index() {
        let allocation = |id: i64, job: &str, config: &str, task: i64, node: &str| Allocation {
            id,
            job_id: job.into(),
            job_config_id: config.into(),
            task_config_id: task,
            node_id: node.into(),
            status: "running".into(),
            resources: json!({}),
            instance_id: None,
            network: None,
        };
        let snapshot = || {
            SharedNodeAllocations::from_iter([
                (
                    "node-a".to_string(),
                    Arc::new(vec![
                        allocation(1, "job-1", "cfg-1", 1, "node-a"),
                        allocation(2, "job-1", "cfg-1", 2, "node-a"),
                        allocation(3, "other", "cfg-other", 9, "node-a"),
                    ]),
                ),
                (
                    "node-b".to_string(),
                    // The previous config of the same job: dropped by a
                    // Fresh plan, never part of the planned config's index.
                    Arc::new(vec![allocation(4, "job-1", "cfg-0", 7, "node-b")]),
                ),
                (
                    "node-c".to_string(),
                    Arc::new(vec![allocation(5, "other", "cfg-other", 9, "node-c")]),
                ),
            ])
        };

        // Nothing dropped (Reschedule): the index holds exactly the
        // planned config's rows, by node, and no list is touched.
        let mut untouched = snapshot();
        let job_index = job_index_for(&untouched, "job-1");
        let (own, freed, _) = Scheduler::index_own_allocations(
            "cfg-1",
            "job-1",
            &job_index,
            &mut untouched,
            SnapshotDrop::Nothing,
        );
        assert!(freed.is_empty());
        assert_eq!(own.len(), 1);
        assert_eq!(own.get("node-a").map(Vec::as_slice), Some(&[1, 2][..]));
        assert_eq!(untouched["node-a"].len(), 3);
        assert_eq!(untouched["node-b"].len(), 1);

        // A Fresh plan drops the whole JOB's rows: the freed set names
        // every node that lost one, the surviving lists keep other jobs'
        // rows, and the index is empty because the config's own rows went
        // with the job's.
        let mut dropped = snapshot();
        let job_index = job_index_for(&dropped, "job-1");
        let (own, freed, _) = Scheduler::index_own_allocations(
            "cfg-1",
            "job-1",
            &job_index,
            &mut dropped,
            SnapshotDrop::Job("job-1"),
        );
        assert!(own.is_empty());
        assert_eq!(
            freed,
            HashSet::from(["node-a".to_string(), "node-b".to_string()])
        );
        assert_eq!(dropped["node-a"].len(), 1);
        assert_eq!(dropped["node-a"][0].id, 3);
        assert!(dropped["node-b"].is_empty());
        assert_eq!(dropped["node-c"].len(), 1);

        // The preview drops one CONFIG: the previous config's row on
        // node-b stays (and is counted), so only node-a's list changes.
        let mut previewed = snapshot();
        let job_index = job_index_for(&previewed, "job-1");
        let (own, freed, _) = Scheduler::index_own_allocations(
            "cfg-1",
            "job-1",
            &job_index,
            &mut previewed,
            SnapshotDrop::JobConfig("cfg-1"),
        );
        assert!(own.is_empty());
        assert_eq!(freed, HashSet::from(["node-a".to_string()]));
        assert_eq!(previewed["node-a"].len(), 1);
        assert_eq!(previewed["node-a"][0].id, 3);
        assert_eq!(previewed["node-b"].len(), 1);
        assert_eq!(previewed["node-c"].len(), 1);
    }

    #[test]
    fn ready_tasks_allows_dependent_service_after_batch_chain() -> R<()> {
        let build = TaskConfig {
            lifecycle: TaskLifecycle::Batch,
            ..TaskConfig::new("build", "exec")
        };
        let seed = TaskConfig {
            lifecycle: TaskLifecycle::Batch,
            depends_on: vec![crate::structs::TaskDependency {
                task: "build".into(),
                condition: TaskDependencyCondition::Success,
            }],
            ..TaskConfig::new("seed", "exec")
        };
        let api = TaskConfig {
            depends_on: vec![crate::structs::TaskDependency {
                task: "seed".into(),
                condition: TaskDependencyCondition::Success,
            }],
            ..TaskConfig::new("api", "noop")
        };

        let ready = Scheduler::ready_tasks(
            vec![(1, build, 1), (2, seed, 1), (3, api, 1)],
            &HashMap::new(),
        )?;

        assert_eq!(ready.len(), 3);
        Ok(())
    }

    #[test]
    fn multiple_jobs_schedule_without_resource_oversubscription() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({}),
            json!({
                "memory_total": "2147483648",
                "cpus": { "cpu-1": { "frequency": "2000" } }
            }),
            json!({}),
        ))?;
        store.insert_node(make_node_msg_with_sources(
            "node-b",
            json!({}),
            json!({
                "memory_total": "2147483648",
                "cpus": { "cpu-1": { "frequency": "2000" } }
            }),
            json!({}),
        ))?;

        let job_one = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-one".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 768 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        let job_two = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-two".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "prep".into(),
                    resources: json!({ "memory": 512 }),
                    ..TaskConfig::new("prepare", "exec")
                },
                TaskConfig {
                    group: "app".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "prepare".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    resources: json!({ "memory": 768 }),
                    ..TaskConfig::new("worker", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job_one, &[dummy_submission()])?;
        store.handle_job_submit(&job_two, &[dummy_submission()])?;

        let scheduler = Scheduler::new(&store)?;
        let outcome_one =
            scheduler.plan_and_apply(&store.get_job_config("job-one")?.id(), PlanMode::Fresh)?;
        let outcome_two =
            scheduler.plan_and_apply(&store.get_job_config("job-two")?.id(), PlanMode::Fresh)?;

        assert_eq!(outcome_one.allocations.len(), 2);
        assert_eq!(outcome_two.allocations.len(), 2);

        let ledger = store.get_ledger()?;
        for entry in ledger {
            let total_memory: f64 = entry
                .allocations
                .iter()
                .map(|alloc| {
                    alloc
                        .resources
                        .get("memory")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0)
                })
                .sum();
            assert!(
                total_memory <= 2048.0,
                "node {} exceeded memory budget: {}",
                entry.node.id,
                total_memory
            );
        }

        Ok(())
    }

    #[test]
    fn automatic_anti_affinity_penalizes_existing_group_allocations() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({
                "weights": {
                    "affinities": 0.0,
                    "spread": 0.0,
                    "bin_pack": 0.0,
                    "automatic_anti_affinity": 1.0
                }
            })),
        )?;

        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "anti-affinity-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                group: "service".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;
        store.place_alloc("node-a", task_id)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-b");

        Ok(())
    }

    #[test]
    fn spread_prefers_affinity_over_load_when_weighted() -> R<()> {
        let store = setup_store_with_scheduler(
            "spread",
            Some(json!({
                "weights": {
                    "affinities": 2.0,
                    "spread": 1.0
                }
            })),
        )?;

        store.insert_node(make_node_msg("node-a", json!({ "zone": "us-east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "zone": "us-west" })))?;

        let load_job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "load-job".into(),
            tasks: vec![TaskConfig::new("load", "noop")],
            version: None,
        };
        store.handle_job_submit(&load_job, &[dummy_submission()])?;
        let load_job_config_id = store.get_job_config(&load_job.name)?.id();
        let load_task_id = task_id_for(&store, &load_job_config_id, "load")?;

        for _ in 0..3 {
            store.place_alloc("node-a", load_task_id)?;
        }

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "affinity-job".into(),
            tasks: vec![TaskConfig {
                affinities: json!({ "zone": "us-east" }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-a");

        Ok(())
    }

    #[test]
    fn multi_task_group_honors_constraints_of_every_member() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "db" })))?;

        // The constrained task is listed FIRST in the group: the group's node
        // must still satisfy it.
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "group-first-constraint".into(),
            tasks: vec![
                TaskConfig {
                    group: "app".into(),
                    constraints: json!({ "role": "db" }),
                    ..TaskConfig::new("database", "noop")
                },
                TaskConfig {
                    group: "app".into(),
                    ..TaskConfig::new("sidecar", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        for allocation in &outcome.allocations {
            assert_eq!(allocation.node_id, "node-b");
        }

        Ok(())
    }

    #[test]
    fn distinct_constraint_limits_one_allocation_per_attribute_value() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "zone": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "zone": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "zone": "west" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "distinct-zone".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                constraints: json!([{ "key": "zone", "operator": "distinct" }]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let zones = outcome
            .allocations
            .iter()
            .map(|alloc| {
                let node = store.get_node(&alloc.node_id)?;
                Ok(node
                    .config_attrs
                    .get("zone")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string())
            })
            .collect::<R<HashSet<_>>>()?;
        assert_eq!(zones.len(), 2, "one allocation per distinct zone");

        Ok(())
    }

    #[test]
    fn unavailable_nodes_are_not_placement_candidates() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        let mut draining = make_node_msg("node-b", json!({}));
        draining.availability = "draining".into();
        store.insert_node(draining)?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "avail".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert!(outcome
            .allocations
            .iter()
            .all(|alloc| alloc.node_id == "node-a"));

        Ok(())
    }

    #[test]
    fn custom_item_resources_are_consumed_once() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "resources": { "gpu": ["gpu0", "gpu1"] } }),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "gpu-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 64, "gpu": ["gpu0"] }),
                ..TaskConfig::new("trainer", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let result = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh);
        assert!(
            result.is_err(),
            "second allocation cannot claim an already-consumed gpu item"
        );

        Ok(())
    }

    #[test]
    fn reserved_resources_reduce_schedulable_capacity() -> R<()> {
        let store = setup_store()?;
        // 8 GiB total, 7 GiB reserved -> only 1 GiB schedulable.
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "reservedResources": { "memory": 7168 } }),
        ))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reserved".into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 2048 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        assert!(scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .is_err());

        Ok(())
    }

    #[test]
    fn reschedule_places_only_the_missing_allocations() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "scale-up".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(3),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;
        store.place_alloc("node-a", task_id)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 2, "places only the missing two");

        Ok(())
    }

    #[test]
    fn group_mixing_count_per_values_is_rejected() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "mixed-per".into(),
            tasks: vec![
                TaskConfig {
                    group: "app".into(),
                    count: TaskCount::per(1, "region"),
                    ..TaskConfig::new("api", "noop")
                },
                TaskConfig {
                    group: "app".into(),
                    count: TaskCount::new(1),
                    ..TaskConfig::new("cache", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        assert!(scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .is_err());

        Ok(())
    }

    #[test]
    fn concurrent_plans_cannot_oversubscribe_a_node() -> R<()> {
        let store = setup_store()?;
        // One node with room for exactly one 6000 MiB allocation.
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let mut config_ids = Vec::new();
        for name in ["job-one", "job-two"] {
            let job = JobConfig {
                priority: Default::default(),
                rollout: None,
                rollout_by_group: Default::default(),
                name: name.into(),
                tasks: vec![TaskConfig {
                    resources: json!({ "memory": 6000 }),
                    ..TaskConfig::new("api", "noop")
                }],
                version: None,
            };
            store.handle_job_submit(&job, &[dummy_submission()])?;
            config_ids.push(store.get_job_config(name)?.id());
        }

        let handles = config_ids
            .into_iter()
            .map(|job_config_id| {
                let store = store.clone();
                std::thread::spawn(move || {
                    let scheduler = Scheduler::new(&store)?;
                    scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)
                })
            })
            .collect::<Vec<_>>();

        let mut applied = 0;
        for handle in handles {
            if let Ok(Ok(outcome)) = handle.join() {
                applied += outcome.allocations.len();
            }
        }

        assert_eq!(applied, 1, "only one plan may claim the node");
        assert_eq!(store.allocs_for_node("node-a")?.len(), 1);

        Ok(())
    }

    /// Apply-time re-validation of node resource TOTALS (TODO from the
    /// 2026-07-28 adversarial pass): a worker re-registration that shrinks
    /// capacity between the plan snapshot and the apply bumps no allocation
    /// epoch — the version CAS structurally cannot see it — so the apply
    /// transaction itself must rebuild the node's free pool and reject the
    /// stale placement.
    #[test]
    fn apply_rejects_when_node_capacity_shrank_since_the_plan_snapshot() -> R<()> {
        let store = setup_store()?;
        // 8192 MiB of fingerprinted memory: the 6000 MiB task fits.
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let job_config_id = submit_job(
            &store,
            "shrink-race",
            vec![TaskConfig {
                resources: json!({ "memory": 6000 }),
                ..TaskConfig::new("api", "noop")
            }],
        )?;
        let task_id = task_id_for(&store, &job_config_id, "api")?;

        // The submission a planner snapshotting the 8192 MiB node would
        // produce, with expected versions matching the CURRENT epochs so
        // the compare-and-set passes — proving the rejection below comes
        // from the capacity recheck, not the epoch fence.
        let expected = std::collections::BTreeMap::from([
            (format!("job_config:{job_config_id}"), 0),
            ("node:node-a".to_string(), 0),
        ]);
        let submission = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &expected,
        )?;

        // Node re-registers with 2 GiB before the apply. Registration is a
        // control-lane upsert that bumps no allocation epoch.
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({}),
            json!({ "memory_total": "2147483648" }),
            json!({}),
        ))?;

        match store.apply_plan_submission(&submission)? {
            crate::storage::store::PlanApplyOutcome::Rejected { reason, .. } => {
                assert!(
                    reason.contains("no longer has capacity"),
                    "rejection must name the capacity recheck: {reason}"
                );
            }
            crate::storage::store::PlanApplyOutcome::Applied(allocations) => {
                panic!(
                    "plan applied {} placements onto a node that shrank below the task's ask",
                    allocations.allocations.len()
                )
            }
        }
        assert!(store.allocs_for_node("node-a")?.is_empty());

        Ok(())
    }

    /// The recheck must mirror the planner's ledger exactly for a Fresh
    /// plan: the planned JOB's surviving allocation rows (a job_submit
    /// deletes the previous config's rows only after the apply) are
    /// excluded from the reconstruction, so a same-capacity re-apply is
    /// not spuriously rejected as over capacity.
    /// `fresh_replan_ignores_the_jobs_own_outgoing_allocations` covers the
    /// cross-config (v1 → v2) half of the same property.
    #[test]
    fn apply_capacity_recheck_does_not_double_count_a_fresh_plans_own_config() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        // First plan fills most of the node: 6000 of 8192 MiB.
        let job_config_id = submit_job(
            &store,
            "self-replace",
            vec![TaskConfig {
                resources: json!({ "memory": 6000 }),
                ..TaskConfig::new("api", "noop")
            }],
        )?;
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 1);

        // A second Fresh plan for the SAME config (the planner drops the
        // config's own allocations from its ledger). If the apply-time
        // reconstruction counted the surviving row, 6000 + 6000 > 8192
        // would reject a plan the planner legitimately built.
        let task_id = task_id_for(&store, &job_config_id, "api")?;
        let epochs = store.allocation_version_vector_for_candidates(&job_config_id, &["node-a"])?;
        let submission = store.insert_plan_submission(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &epochs,
        )?;
        match store.apply_plan_submission(&submission)? {
            crate::storage::store::PlanApplyOutcome::Applied(applied) => {
                assert_eq!(applied.allocations.len(), 1);
            }
            crate::storage::store::PlanApplyOutcome::Rejected { reason, .. } => {
                panic!("fresh-mode self-replacement must not be double counted: {reason}")
            }
        }

        Ok(())
    }

    /// The lazy (post-planning) version sample still produces a real
    /// fence: the applied plan bumps its nodes' epochs, so a later
    /// submission carrying pre-apply epochs is visibly stale.
    ///
    /// Asserted on the epoch itself rather than through a rejection: a
    /// stale node epoch is no longer a rejection on its own — the apply
    /// walk revalidates such a candidate and binds it when it still fits
    /// (see `plan_walk_rebinds_an_epoch_dirty_candidate_that_still_fits`).
    /// What the epoch still buys is ordering information, and that is what
    /// this test pins.
    #[test]
    fn snapshot_fence_sample_bumps_node_epochs() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let scheduler = Scheduler::new(&store)?;
        assert_eq!(scheduler.retired_lazy_version_sample(), None);

        let first = submit_job(&store, "job-lazy-a", vec![TaskConfig::new("api", "noop")])?;
        let outcome = scheduler.plan_and_apply(&first, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 1);

        let after = store.allocation_version_vector_for_candidates(&first, &["node-a"])?;
        let node_epoch = after
            .get("node:node-a")
            .copied()
            .expect("the sample covers the placed node");
        assert!(
            node_epoch > 0,
            "the snapshot-sampled apply must have bumped the node epoch"
        );

        Ok(())
    }

    #[test]
    fn score_sample_stays_exact_below_the_threshold() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({ "score_sample": { "min_nodes": 64, "sample": 8 } })),
        )?;
        for index in 0..6 {
            store.insert_node(make_node_msg(&format!("node-{index}"), json!({})))?;
        }
        let job_config_id = submit_job(
            &store,
            "sample-below",
            vec![TaskConfig {
                count: TaskCount::new(4),
                ..TaskConfig::new("api", "noop")
            }],
        )?;
        let scheduler = Scheduler::new(&store)?;
        let (placements, report, _preemptions) =
            scheduler.plan_only_with_report(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(placements.len(), 4);
        assert!(
            report.iter().all(|usage| usage.score_sampled.is_none()),
            "below min_nodes scoring must be exact: {report:?}"
        );
        Ok(())
    }

    #[test]
    fn score_sample_engages_above_the_threshold_and_reports_it() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({ "score_sample": { "min_nodes": 8, "sample": 3 } })),
        )?;
        for index in 0..12 {
            store.insert_node(make_node_msg(&format!("node-{index:02}"), json!({})))?;
        }
        let job_config_id = submit_job(
            &store,
            "sample-above",
            vec![TaskConfig {
                count: TaskCount::new(5),
                ..TaskConfig::new("api", "noop")
            }],
        )?;
        let scheduler = Scheduler::new(&store)?;
        let (placements, report, _preemptions) =
            scheduler.plan_only_with_report(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(placements.len(), 5);
        assert_eq!(
            report.first().and_then(|usage| usage.score_sampled),
            Some(3),
            "sampling must engage and be reported: {report:?}"
        );
        // Every sampled placement is still a real node from the fleet.
        for placement in &placements {
            assert!(placement.node_id.starts_with("node-"));
        }
        Ok(())
    }

    #[test]
    fn score_sample_settings_are_validated() -> R<()> {
        for bad in [
            json!({ "score_sample": { "min_nodes": 8, "sample": 1 } }),
            json!({ "score_sample": { "min_nodes": 4, "sample": 8 } }),
            json!({ "score_sample": { "sample": 8 } }),
            json!({ "score_sample": "128" }),
        ] {
            let store = setup_store_with_scheduler("bin_pack", Some(bad.clone()))?;
            store.insert_node(make_node_msg("node-a", json!({})))?;
            submit_job(&store, "sample-bad", vec![TaskConfig::new("api", "noop")])?;
            // Settings parse once at construction now, so a malformed
            // setting fails Scheduler::new (fail-fast) instead of the
            // first plan.
            assert!(
                Scheduler::new(&store).is_err(),
                "malformed setting must be rejected: {bad}"
            );
        }
        Ok(())
    }

    /// The bounded-candidate-scoring quality gate: best-of-k bin packing
    /// must stay within a bound of exact packing (power-of-k-choices), and
    /// far from uniform-random placement. 100 independent one-GiB tasks
    /// (distinct tasks and groups, so automatic anti-affinity does not
    /// engage) over 40 eight-GiB nodes: exact bin_pack packs onto
    /// ceil(100/8) = 13 nodes; uniform random would touch essentially the
    /// whole fleet (~37 of 40). The sampled run must place everything and
    /// stay ≤ 32 nodes — loose enough to hold with high probability under
    /// an unseeded RNG, tight enough that a regression to unsampled
    /// uniform behavior fails.
    #[test]
    fn score_sample_bin_pack_quality_stays_bounded() -> R<()> {
        let placements_for = |settings: Option<Value>| -> R<usize> {
            let store = setup_store_with_scheduler("bin_pack", settings)?;
            for index in 0..40 {
                store.insert_node(make_node_msg(&format!("node-{index:02}"), json!({})))?;
            }
            let tasks = (0..100)
                .map(|index| TaskConfig {
                    group: format!("group-{index}"),
                    resources: json!({ "memory": 1024 }),
                    ..TaskConfig::new(format!("task-{index}"), "noop")
                })
                .collect();
            let job_config_id = submit_job(&store, "quality", tasks)?;
            let scheduler = Scheduler::new(&store)?;
            let placements = scheduler.plan_only(&job_config_id, PlanMode::Fresh)?;
            assert_eq!(placements.len(), 100, "every allocation must place");
            let nodes_used: HashSet<&str> = placements
                .iter()
                .map(|placement| placement.node_id.as_str())
                .collect();
            Ok(nodes_used.len())
        };

        let exact = placements_for(None)?;
        assert_eq!(exact, 13, "exact bin_pack packs onto ceil(100/8) nodes");

        let sampled = placements_for(Some(
            json!({ "score_sample": { "min_nodes": 8, "sample": 8 } }),
        ))?;
        assert!(
            sampled <= 32,
            "best-of-8 packing must stay within the quality bound: used {sampled} nodes"
        );
        Ok(())
    }

    #[test]
    fn retired_lazy_version_sample_setting_still_parses_and_plans() -> R<()> {
        // Existing configs that set the retired key must keep loading and
        // planning; the key no longer changes how the fence is sampled.
        let store =
            setup_store_with_scheduler("bin_pack", Some(json!({ "lazy_version_sample": false })))?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let scheduler = Scheduler::new(&store)?;
        assert_eq!(scheduler.retired_lazy_version_sample(), Some(false));

        let job_config_id = submit_job(&store, "job-eager", vec![TaskConfig::new("api", "noop")])?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 1);

        Ok(())
    }

    #[test]
    fn failed_batch_dependency_blocks_dependent_task() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "dag-failure".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "build".into(),
                    ..TaskConfig::new("migrate", "noop")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    group: "serve".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "migrate".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let migrate_id = task_id_for(&store, &job_config_id, "migrate")?;
        store.set_task_status(migrate_id, TaskStatusState::Failed)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert!(
            outcome
                .allocations
                .iter()
                .all(|alloc| alloc.task_config_id == migrate_id),
            "api must not be placed while its dependency has failed"
        );

        Ok(())
    }

    #[test]
    fn fresh_replan_ignores_the_jobs_own_outgoing_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "zone": "east" })))?;

        let v1 = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "heavy".into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 6000 }),
                affinities: json!({ "zone": "east" }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[dummy_submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        assert_eq!(
            scheduler
                .plan_and_apply(&v1_id, PlanMode::Fresh)?
                .allocations
                .len(),
            1
        );

        // A schedule-relevant change (affinity) forces a real replan.
        let v2 = JobConfig {
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 6000 }),
                affinities: json!({ "zone": "west" }),
                ..TaskConfig::new("api", "noop")
            }],
            ..v1.clone()
        };
        store.handle_job_submit(&v2, &[dummy_submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        assert_ne!(v1_id, v2_id);

        let outcome = scheduler.plan_and_apply(&v2_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 1);

        Ok(())
    }

    #[test]
    fn lost_node_replacement_respects_node_capacity() -> R<()> {
        let store = setup_store()?;
        // Both nodes fingerprint 8192 MiB, so a 6000 MiB task fits once each.
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "capacity".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 6000 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2);

        // node-a dies. node-b is the only candidate left and has no room, so
        // no replacement may be placed there.
        let _ = store.mark_nodes_as_lost(&["node-a".to_string()]);

        let on_node_b = store
            .allocs_for_job_config(&job_config_id)?
            .into_iter()
            .filter(|alloc| alloc.node_id == "node-b")
            .count();
        assert_eq!(on_node_b, 1, "node-b has room for exactly one allocation");

        Ok(())
    }

    #[test]
    fn count_per_honors_explicit_task_targets() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;
        store.insert_node(make_node_msg("node-d", json!({ "region": "west" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-targets".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;

        // A rollout stages placements by passing explicit per-task targets.
        let targets = HashMap::from([(task_id, 1)]);
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply_with_targets(
            &job_config_id,
            PlanMode::Reschedule,
            &targets,
        )?;
        assert_eq!(outcome.allocations.len(), 1);

        Ok(())
    }

    #[test]
    fn count_per_reschedule_only_refills_the_affected_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;
        store.insert_node(make_node_msg("node-d", json!({ "region": "west" })))?;

        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-refill".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let fresh = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(fresh.allocations.len(), 4);

        store.mark_node_lost("node-c")?;
        store.remove_allocs_for_node("node-c")?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-d");

        Ok(())
    }

    // ── late-binding candidate lists (docs/late-binding-plan-apply.md) ──

    fn task_config(name: &str) -> TaskConfig {
        TaskConfig::new(name, "noop")
    }

    fn plain_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn planned(node: &str, task: i64, candidates: &[&str]) -> PlannedAllocation {
        PlannedAllocation {
            node_id: node.to_string(),
            task_config_id: task,
            candidates: candidates.iter().map(|id| id.to_string()).collect(),
            instance_id: None,
        }
    }

    #[test]
    fn ranked_candidate_list_argmax_head_and_band_tail() {
        let nodes: Vec<Node> = (0..6).map(|i| plain_node(&format!("node-{i}"))).collect();
        let scores = [1.0, 0.995, 0.993, 0.992, 0.9, 0.8];
        let scored: Vec<(&Node, f64)> = nodes.iter().zip(scores).collect();
        let setting = PlanCandidatesSetting {
            count: 4,
            band: 0.01,
        };
        let mut rng = StdRng::seed_from_u64(7);

        let list = Scheduler::ranked_candidate_list(scored, false, setting, 128, &mut rng)
            .expect("non-empty scored list");
        assert_eq!(list.len(), 4);
        assert_eq!(list[0].id, "node-0", "the head must be the true argmax");
        let unique: HashSet<&str> = list.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(
            unique.len(),
            4,
            "no duplicates (sampling without replacement)"
        );
        for node in &list[1..] {
            assert!(
                ["node-1", "node-2", "node-3"].contains(&node.id.as_str()),
                "tail candidate {} must come from the near-tie band",
                node.id
            );
        }
    }

    #[test]
    fn ranked_candidate_list_extends_below_band_in_score_order() {
        let nodes: Vec<Node> = (0..4).map(|i| plain_node(&format!("node-{i}"))).collect();
        let scores = [1.0, 0.995, 0.9, 0.8];
        let scored: Vec<(&Node, f64)> = nodes.iter().zip(scores).collect();
        let setting = PlanCandidatesSetting {
            count: 4,
            band: 0.01,
        };
        let mut rng = StdRng::seed_from_u64(3);

        let list = Scheduler::ranked_candidate_list(scored, false, setting, 128, &mut rng)
            .expect("non-empty scored list");
        let ids: Vec<&str> = list.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["node-0", "node-1", "node-2", "node-3"],
            "a thin band extends straight down the ranking in score order"
        );
    }

    #[test]
    fn ranked_candidate_list_disabled_returns_singleton() {
        let nodes: Vec<Node> = (0..3).map(|i| plain_node(&format!("node-{i}"))).collect();
        let scores = [0.5, 1.0, 0.999_999_999_9];
        let scored: Vec<(&Node, f64)> = nodes.iter().zip(scores).collect();
        let setting = PlanCandidatesSetting {
            count: 0,
            band: 0.01,
        };
        let mut rng = StdRng::seed_from_u64(1);

        let list = Scheduler::ranked_candidate_list(scored, false, setting, 128, &mut rng)
            .expect("non-empty scored list");
        assert_eq!(list.len(), 1, "count 0 restores single-node selection");
        assert_ne!(list[0].id, "node-0", "the singleton is still the argmax");
    }

    /// Overlap is no longer a coupling edge: the apply walk debits each
    /// bind from the shared node's in-transaction free pool, so two
    /// placements landing on it cannot oversubscribe it.
    #[test]
    fn decouple_components_keeps_overlapping_lists() {
        let mut placements = vec![
            planned("node-a", 1, &["node-a", "node-b"]),
            planned("node-c", 2, &["node-c", "node-b"]),
            planned("node-e", 3, &["node-e", "node-f"]),
        ];
        Scheduler::decouple_plan_components(
            &mut placements,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        for placement in &placements {
            assert_eq!(
                placement.candidates.len(),
                2,
                "an overlapping list is not a coupled component"
            );
        }
    }

    /// The same reversal for a transitive chain — the shape that used to
    /// collapse an entire plan because two of its placements shared one
    /// fallback node.
    #[test]
    fn decouple_components_transitive_overlap_is_not_coupled() {
        let mut placements = vec![
            planned("node-a", 1, &["node-a", "node-b"]),
            planned("node-b", 2, &["node-b", "node-c"]),
            planned("node-c", 3, &["node-c", "node-d"]),
        ];
        Scheduler::decouple_plan_components(
            &mut placements,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        for placement in &placements {
            assert_eq!(placement.candidates.len(), 2);
        }
    }

    #[test]
    fn decouple_components_couples_distinct_tasks_across_disjoint_lists() {
        let distinct_keys: HashMap<i64, Vec<String>> =
            HashMap::from([(1, vec!["zone".to_string()])]);
        let mut placements = vec![
            planned("node-a", 1, &["node-a", "node-b"]),
            planned("node-c", 1, &["node-c", "node-d"]),
            planned("node-e", 2, &["node-e", "node-f"]),
        ];
        Scheduler::decouple_plan_components(
            &mut placements,
            &distinct_keys,
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(
            placements[0].candidates.len(),
            1,
            "distinct-constrained placements couple even with disjoint lists"
        );
        assert_eq!(placements[1].candidates.len(), 1);
        assert_eq!(
            placements[2].candidates.len(),
            2,
            "the unrelated task keeps its list"
        );
    }

    #[test]
    fn decouple_components_couples_multi_task_groups() {
        let groups: HashMap<i64, String> =
            HashMap::from([(1, "app".to_string()), (2, "app".to_string())]);
        let sizes: HashMap<String, usize> = HashMap::from([("app".to_string(), 2)]);
        let mut placements = vec![
            planned("node-a", 1, &["node-a", "node-b"]),
            planned("node-c", 2, &["node-c", "node-d"]),
        ];
        Scheduler::decouple_plan_components(&mut placements, &HashMap::new(), &groups, &sizes);
        assert_eq!(placements[0].candidates.len(), 1);
        assert_eq!(placements[1].candidates.len(), 1);
    }

    #[test]
    fn decouple_components_leaves_a_single_placement_alone() {
        let distinct_keys: HashMap<i64, Vec<String>> =
            HashMap::from([(1, vec!["zone".to_string()])]);
        let mut placements = vec![planned("node-a", 1, &["node-a", "node-b"])];
        Scheduler::decouple_plan_components(
            &mut placements,
            &distinct_keys,
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(
            placements[0].candidates.len(),
            2,
            "a lone placement is a singleton component by definition"
        );
    }

    #[test]
    fn plan_candidates_setting_parses_shorthand_object_and_disables() -> R<()> {
        let default_store = setup_store()?;
        let scheduler = Scheduler::new(&default_store)?;
        assert_eq!(
            scheduler.plan_candidates_setting()?,
            PlanCandidatesSetting::default()
        );

        let cases: Vec<(Value, PlanCandidatesSetting)> = vec![
            (
                json!({ "plan_candidates": 0 }),
                PlanCandidatesSetting {
                    count: 0,
                    band: RESCHEDULE_SCORE_JITTER_THRESHOLD,
                },
            ),
            (
                json!({ "plan_candidates": 4 }),
                PlanCandidatesSetting {
                    count: 4,
                    band: RESCHEDULE_SCORE_JITTER_THRESHOLD,
                },
            ),
            (
                json!({ "plan_candidates": 1000 }),
                PlanCandidatesSetting {
                    count: PlanCandidatesSetting::MAX_COUNT,
                    band: RESCHEDULE_SCORE_JITTER_THRESHOLD,
                },
            ),
            (
                json!({ "plan_candidates": { "count": 12, "band": 0.05 } }),
                PlanCandidatesSetting {
                    count: 12,
                    band: 0.05,
                },
            ),
        ];
        for (settings, expected) in cases {
            let store = setup_store_with_scheduler("bin_pack", Some(settings.clone()))?;
            let scheduler = Scheduler::new(&store)?;
            assert_eq!(
                scheduler.plan_candidates_setting()?,
                expected,
                "settings: {settings}"
            );
        }

        // Malformed settings fail at construction, like every other
        // scheduler setting parsed once in `Scheduler::new`.
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({ "plan_candidates": { "band": 2.0 } })),
        )?;
        assert!(Scheduler::new(&store).is_err());
        Ok(())
    }

    #[test]
    fn fresh_single_placement_carries_ranked_candidates() -> R<()> {
        let store = setup_store()?;
        for index in 0..5 {
            store.insert_node(make_node_msg(&format!("node-{index}"), json!({})))?;
        }
        let job_config_id = submit_job(&store, "one-sandbox", vec![task_config("api")])?;

        let scheduler = Scheduler::new(&store)?;
        let placements = scheduler.plan_only(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(placements.len(), 1);
        let placement = &placements[0];
        assert_eq!(placement.candidates[0], placement.node_id);
        assert_eq!(
            placement.candidates.len(),
            5,
            "five equal-score nodes all sit in the near-tie band"
        );
        let unique: HashSet<&str> = placement.candidates.iter().map(String::as_str).collect();
        assert_eq!(unique.len(), 5, "no duplicate candidates");
        Ok(())
    }

    //
    // PREEMPTION (docs/preemption-and-eviction.md)
    //

    /// A node with exactly 512 MiB and one CPU: small enough that two
    /// 256 MiB tasks fill it, which is what makes contention easy to stage.
    fn small_node(store: &Store, id: &str) -> R<()> {
        store.insert_node(make_node_msg_with_sources(
            id,
            json!({}),
            json!({
                "memory_total": "536870912",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;
        Ok(())
    }

    /// Submit a banded job with `count` 256 MiB tasks and place it.
    fn submit_banded(
        store: &Store,
        name: &str,
        band: Band,
        count: u32,
        disruption: Option<crate::structs::DisruptionConfig>,
    ) -> R<String> {
        let job = JobConfig {
            priority: band,
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(count),
                resources: json!({ "memory": 256 }),
                disruption,
                ..TaskConfig::new("worker", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        Ok(store.get_job_config(name)?.id())
    }

    /// Backdate every allocation past `victimExemptAge`, so tests exercise
    /// victim SELECTION rather than the thrash guard. The guard has its own
    /// test below.
    fn age_allocations(store: &Store) -> R<()> {
        let conn = store.write_connection()?;
        conn.execute(
            "UPDATE allocation SET created_at_unix = created_at_unix - 3600;",
            [],
        )?;
        Ok(())
    }

    #[test]
    fn a_higher_band_job_displaces_a_lower_band_one_when_the_cluster_is_full() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;

        // Fill the node with batch work.
        let batch = submit_banded(&store, "transcode", Band::Low, 2, None)?;
        let placed = scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        assert_eq!(placed.allocations.len(), 2, "the node starts full");
        age_allocations(&store)?;

        // The incident: a `high` job needs one slot and there is none.
        let api = submit_banded(&store, "api", Band::High, 1, None)?;
        let outcome = scheduler.plan_and_apply(&api, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1, "the preemptor places");
        assert_eq!(
            outcome.evictions.len(),
            1,
            "and takes exactly the one victim it needs, not both"
        );
        assert_eq!(outcome.evictions[0].band, "low");
        assert_eq!(outcome.evictions[0].job_id, "transcode");

        // The victim is really gone, and the placement really landed.
        let live = store.allocs_for_node("node-a")?;
        assert_eq!(live.len(), 2, "the node is still exactly full");
        assert_eq!(
            live.iter().filter(|a| a.job_id == "api").count(),
            1,
            "the preemptor is running"
        );
        assert_eq!(
            live.iter().filter(|a| a.job_id == "transcode").count(),
            1,
            "one transcode allocation survived"
        );

        // The victim's stop notice names its cause and its preemptor, so the
        // worker can hand the task a notice file it can act on.
        let notices = store.stop_notices_for_node("node-a")?;
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].cause, crate::structs::StopCause::Preempted);
        assert_eq!(notices[0].preemptor_job, "api");
        assert_eq!(notices[0].preemptor_band, "high");
        Ok(())
    }

    #[test]
    fn a_preempted_job_is_requeued_in_the_same_transaction() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Background, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        age_allocations(&store)?;
        let api = submit_banded(&store, "api", Band::Normal, 1, None)?;

        let before = store.get_evals()?.len();
        scheduler.plan_and_apply(&api, PlanMode::Fresh)?;

        // Nothing has to REMEMBER the eviction for the victim to come back:
        // the requeue eval rides the apply transaction and reschedule is
        // driven by desired count. That is why the Nomad #9890 class of bug
        // (preempted batch allocations never replaced) cannot exist here.
        let evals = store.get_evals()?;
        assert!(evals.len() > before, "the victim's job was requeued");
        let requeue = evals
            .iter()
            .find(|eval| eval.kind == "job_reschedule" && eval.job_config_id == batch)
            .expect("a job_reschedule eval for the victim job config");
        assert_eq!(
            requeue.priority,
            Band::Background.eval_priority(),
            "the victim requeues at its OWN band, so it cannot jump ahead of \
             the work that displaced it"
        );
        Ok(())
    }

    #[test]
    fn preemption_never_takes_a_peer_or_a_more_important_job() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        // The node is full of `high` work, and the newcomer is also `high`.
        let incumbent = submit_banded(&store, "incumbent", Band::High, 2, None)?;
        scheduler.plan_and_apply(&incumbent, PlanMode::Fresh)?;
        age_allocations(&store)?;

        let peer = submit_banded(&store, "peer", Band::High, 1, None)?;
        let err = scheduler
            .plan_and_apply(&peer, PlanMode::Fresh)
            .expect_err("a peer must not be displaced");
        assert!(
            err.to_string().contains("same band or higher"),
            "the error explains itself in band terms: {err}"
        );
        assert_eq!(
            store.allocs_for_node("node-a")?.len(),
            2,
            "nothing was evicted"
        );
        Ok(())
    }

    #[test]
    fn a_freshly_placed_allocation_is_exempt_from_preemption() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Low, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        // Deliberately NOT aged: these allocations are seconds old.

        let api = submit_banded(&store, "api", Band::Critical, 1, None)?;
        let err = scheduler
            .plan_and_apply(&api, PlanMode::Fresh)
            .expect_err("victimExemptAge must hold even against `critical`");
        assert!(
            err.to_string().contains("victimExemptAge"),
            "the error names the guard that bound: {err}"
        );
        Ok(())
    }

    #[test]
    fn the_disruption_floor_stops_a_task_being_wiped_out() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        // Two allocations, floor of 50% -> at most one may ever be taken.
        let tool = submit_banded(
            &store,
            "internal-tool",
            Band::Low,
            2,
            Some(crate::structs::DisruptionConfig {
                min_available: Some(json!("50%")),
            }),
        )?;
        scheduler.plan_and_apply(&tool, PlanMode::Fresh)?;
        age_allocations(&store)?;

        // One slot is available by eviction; two are not.
        let api = submit_banded(&store, "api", Band::High, 2, None)?;
        let err = scheduler
            .plan_and_apply(&api, PlanMode::Fresh)
            .expect_err("the floor must bind the second placement");
        assert!(
            err.to_string().contains("disruption.minAvailable"),
            "the error names the floor: {err}"
        );

        // And the floor is a HARD rule, not a score: nothing was evicted at
        // all, because the plan that would have crossed it failed whole.
        assert_eq!(store.allocs_for_node("node-a")?.len(), 2);
        Ok(())
    }

    #[test]
    fn observe_mode_selects_victims_and_evicts_nothing() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({ "preemption": { "mode": "observe" } })),
        )?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Low, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        age_allocations(&store)?;

        let api = submit_banded(&store, "api", Band::High, 1, None)?;
        let err = scheduler
            .plan_and_apply(&api, PlanMode::Fresh)
            .expect_err("observe mode commits nothing, so the placement fails");
        assert!(
            err.to_string().contains("observe mode"),
            "the error says why it did not act: {err}"
        );
        assert_eq!(
            store.allocs_for_node("node-a")?.len(),
            2,
            "observe mode evicted nothing"
        );
        // The evidence an operator turns enforce on for.
        let events =
            store.list_scheduler_events(&crate::storage::events::SchedulerEventFilter {
                kind: Some("allocation.preempt_planned".into()),
                ..Default::default()
            })?;
        assert!(
            !events.is_empty(),
            "observe mode journals what it would have done"
        );
        Ok(())
    }

    /// The reversal that motivated the conflict-reduction work: replicas
    /// of one task into one pool overlap by construction, and coupling
    /// them on that overlap collapsed every placement to a one-element
    /// list — the pre-candidate all-or-nothing CAS, for the single most
    /// common job shape there is. They now keep their lists; the apply
    /// walk's in-transaction pool debit is what keeps two of them off one
    /// node's last slot.
    #[test]
    fn count_two_placements_on_a_shared_pool_keep_their_candidate_lists() -> R<()> {
        let store = setup_store()?;
        for index in 0..5 {
            store.insert_node(make_node_msg(&format!("node-{index}"), json!({})))?;
        }
        let job_config_id = submit_job(
            &store,
            "svc-two",
            vec![TaskConfig {
                count: TaskCount::new(2),
                ..task_config("api")
            }],
        )?;

        let scheduler = Scheduler::new(&store)?;
        let placements = scheduler.plan_only(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(placements.len(), 2);
        for placement in &placements {
            assert!(
                placement.candidates.len() > 1,
                "same-pool replicas keep a fallback list: {:?}",
                placement.candidates
            );
            assert_eq!(
                placement.candidates[0], placement.node_id,
                "rank 0 is still the planner's own choice"
            );
        }
        Ok(())
    }

    #[test]
    fn disjoint_pool_tasks_keep_independent_candidate_lists() -> R<()> {
        let store = setup_store()?;
        for index in 0..3 {
            store.insert_node(make_node_msg(
                &format!("gpu-{index}"),
                json!({ "tier": "gpu" }),
            ))?;
            store.insert_node(make_node_msg(
                &format!("cpu-{index}"),
                json!({ "tier": "cpu" }),
            ))?;
        }
        let gpu_task = TaskConfig {
            group: "gpu".into(),
            constraints: json!({ "tier": "gpu" }),
            ..task_config("model")
        };
        let cpu_task = TaskConfig {
            group: "cpu".into(),
            constraints: json!({ "tier": "cpu" }),
            ..task_config("sidecar")
        };
        let job_config_id = submit_job(&store, "hetero", vec![gpu_task, cpu_task])?;

        let scheduler = Scheduler::new(&store)?;
        let placements = scheduler.plan_only(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(placements.len(), 2);
        for placement in &placements {
            assert_eq!(
                placement.candidates.len(),
                3,
                "disjoint-pool placements each keep their own fallback list"
            );
        }
        Ok(())
    }

    fn latest_placements_json(store: &Store) -> R<String> {
        let conn = store.read_connection()?;
        Ok(conn.query_row(
            "SELECT placements FROM plan_submission ORDER BY id DESC LIMIT 1;",
            [],
            |row| row.get(0),
        )?)
    }

    #[test]
    fn plan_candidates_zero_emits_format1_rows() -> R<()> {
        let store = setup_store_with_scheduler("bin_pack", Some(json!({ "plan_candidates": 0 })))?;
        insert_test_nodes(&store)?;
        let job_config_id = submit_job(&store, "kill-switch", vec![task_config("api")])?;
        let scheduler = Scheduler::new(&store)?;
        scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let placements_json = latest_placements_json(&store)?;
        let value: Value = serde_json::from_str(&placements_json)?;
        assert!(
            value.is_array(),
            "plan_candidates: 0 must emit the pre-candidate array shape, got: {placements_json}"
        );
        Ok(())
    }

    #[test]
    fn preemption_off_leaves_placement_exactly_as_it_was() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({ "preemption": { "mode": "off" } })),
        )?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Low, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        age_allocations(&store)?;

        let api = submit_banded(&store, "api", Band::High, 1, None)?;
        let err = scheduler
            .plan_and_apply(&api, PlanMode::Fresh)
            .expect_err("with preemption off, a full cluster is first-come-first-kept");
        assert!(
            err.to_string().contains("Preemption is off"),
            "the error tells the operator which switch is responsible: {err}"
        );
        assert_eq!(store.allocs_for_node("node-a")?.len(), 2);
        Ok(())
    }

    #[test]
    fn a_disabled_pool_is_never_preempted() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({ "preemption": { "disabledPools": ["default"] } })),
        )?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Background, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        age_allocations(&store)?;

        let api = submit_banded(&store, "api", Band::Critical, 1, None)?;
        let err = scheduler
            .plan_and_apply(&api, PlanMode::Fresh)
            .expect_err("a pool-level exemption outranks any band");
        assert!(
            err.to_string().contains("disabledPools"),
            "the error names the exemption: {err}"
        );
        Ok(())
    }

    #[test]
    fn the_lowest_band_can_never_preempt_anything() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let incumbent = submit_banded(&store, "incumbent", Band::Background, 2, None)?;
        scheduler.plan_and_apply(&incumbent, PlanMode::Fresh)?;
        age_allocations(&store)?;

        let filler = submit_banded(&store, "filler", Band::Background, 1, None)?;
        let err = scheduler
            .plan_and_apply(&filler, PlanMode::Fresh)
            .expect_err("background is the floor");
        assert!(
            err.to_string().contains("lowest priority band"),
            "the error says the band itself is the reason: {err}"
        );
        Ok(())
    }

    #[test]
    fn a_plan_that_never_preempts_carries_no_evictions() -> R<()> {
        // The regression guard for "preemption costs nothing when unused":
        // the pass runs only after normal feasibility fails, so an ordinary
        // plan against an empty cluster must not even reach it.
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let scheduler = Scheduler::new(&store)?;
        let job = submit_banded(&store, "api", Band::Critical, 2, None)?;
        let outcome = scheduler.plan_and_apply(&job, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2);
        assert!(
            outcome.evictions.is_empty(),
            "an unconstrained plan preempts nothing, whatever its band"
        );
        Ok(())
    }

    #[test]
    fn a_multi_placement_plan_never_evicts_the_same_allocation_twice() -> R<()> {
        // Two nodes, each full of two `low` allocations; a `high` job wants
        // three. Each placement runs its own preemption pass against the same
        // plan-wide index, so a victim taken by placement 1 must be invisible
        // to placement 2 — otherwise one eviction is counted as paying for
        // two placements, and the node ends up oversubscribed.
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        small_node(&store, "node-b")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Low, 4, None)?;
        let filled = scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        assert_eq!(filled.allocations.len(), 4, "both nodes start full");
        age_allocations(&store)?;

        let api = submit_banded(&store, "api", Band::High, 3, None)?;
        let outcome = scheduler.plan_and_apply(&api, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 3);

        let mut victim_ids = outcome
            .evictions
            .iter()
            .map(|victim| victim.allocation_id)
            .collect::<Vec<_>>();
        victim_ids.sort_unstable();
        let unique = victim_ids.iter().collect::<HashSet<_>>().len();
        assert_eq!(
            unique,
            victim_ids.len(),
            "each victim is evicted once: {victim_ids:?}"
        );
        assert_eq!(victim_ids.len(), 3, "three placements need three victims");

        // The real invariant behind the count: neither node is over its
        // two-allocation capacity afterwards.
        for node in ["node-a", "node-b"] {
            assert!(
                store.allocs_for_node(node)?.len() <= 2,
                "{node} must not be oversubscribed"
            );
        }
        assert_eq!(
            store.allocs_for_node("node-a")?.len() + store.allocs_for_node("node-b")?.len(),
            4,
            "the cluster is still exactly full"
        );
        Ok(())
    }

    #[test]
    fn a_rejected_plan_evicts_nobody() -> R<()> {
        // The Volcano #5022 regression, at the transaction boundary: apply
        // re-validates every placement against FRESH node rows, and a
        // rejection there COMMITS (it persists the rejected submission row
        // and its event). Without a savepoint around the eviction half, that
        // commit would keep the victims gone and the placements unmade.
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Low, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        age_allocations(&store)?;

        let api = submit_banded(&store, "api", Band::High, 1, None)?;
        let job_config_id = api.clone();
        let victim = store
            .allocs_for_node("node-a")?
            .into_iter()
            .next()
            .expect("a victim to name");

        // Hand-build the plan the scheduler would have produced, then cordon
        // the node before applying it: apply's availability recheck rejects,
        // on a plan that carries an eviction.
        let task_id = store
            .cached_task_configs_for_job_config(&job_config_id)?
            .first()
            .map(|(id, _)| *id)
            .expect("task config");
        let versions = store
            .allocation_version_vector(&job_config_id, &HashSet::from(["node-a".to_string()]))?;
        store.set_node_availability("node-a", "drained")?;

        let (_submission, outcome) = store.submit_and_apply_plan(
            &job_config_id,
            "fresh",
            &[("node-a".to_string(), task_id)],
            &versions,
            &[crate::storage::store::PlanEviction {
                allocation_id: victim.id,
                node_id: "node-a".into(),
                job_id: victim.job_id.clone(),
                job_config_id: victim.job_config_id.clone(),
                task_config_id: victim.task_config_id,
                band: Band::Low.as_str().to_string(),
            }],
        )?;
        assert!(
            matches!(outcome, PlanApplyOutcome::Rejected { .. }),
            "a cordoned node must reject the placement: {outcome:?}"
        );
        assert_eq!(
            store.allocs_for_node("node-a")?.len(),
            2,
            "the victim is still running: evictions cannot outlive the placement they paid for"
        );
        Ok(())
    }

    #[test]
    fn plan_candidates_default_emits_format2_rows() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_config_id = submit_job(&store, "format-two", vec![task_config("api")])?;
        let scheduler = Scheduler::new(&store)?;
        scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let placements_json = latest_placements_json(&store)?;
        let value: Value = serde_json::from_str(&placements_json)?;
        assert_eq!(
            value.get("format").and_then(Value::as_i64),
            Some(2),
            "candidates on must emit format-2 rows, got: {placements_json}"
        );
        Ok(())
    }

    #[test]
    fn job_plan_previews_the_victims_a_submit_would_take() -> R<()> {
        let store = setup_store()?;
        small_node(&store, "node-a")?;
        let scheduler = Scheduler::new(&store)?;
        let batch = submit_banded(&store, "transcode", Band::Low, 2, None)?;
        scheduler.plan_and_apply(&batch, PlanMode::Fresh)?;
        age_allocations(&store)?;

        // A dry run of a job that has not been submitted at all.
        let candidate = JobConfig {
            priority: Band::High,
            rollout: None,
            rollout_by_group: Default::default(),
            name: "api".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(1),
                resources: json!({ "memory": 256 }),
                ..TaskConfig::new("worker", "noop")
            }],
            version: None,
        };
        let preview = scheduler.plan_for_config(&candidate, PlanMode::Fresh, None)?;
        assert_eq!(preview.allocations.len(), 1);
        assert_eq!(preview.preemptions.len(), 1, "the preview names the victim");
        assert_eq!(preview.preemptions[0].job_id, "transcode");
        assert_eq!(preview.preemptions[0].band, Band::Low);
        assert_eq!(
            preview.preemptions[0].kill_after_secs, 30,
            "and how long it would have to shut down"
        );
        // A dry run must not touch anything.
        assert_eq!(store.allocs_for_node("node-a")?.len(), 2);
        Ok(())
    }
}
