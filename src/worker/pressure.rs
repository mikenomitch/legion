//! The memory pressure response: what a worker does, in what order, when its
//! node runs short of memory (docs/resource-enforcement.md, "Memory
//! pressure").
//!
//! A task within its `memory` reservation is left alone (on nodes without
//! oversubscription, always). A task bursting above its reservation gives
//! memory back first and is the first thing stopped if that is not enough.
//! The kernel OOM killer stays the backstop after the last step.
//!
//! The steps are named, never numbered, so an event or a stop reason reads
//! on its own:
//!
//! | Step             | Acts on                                   | Effect                     |
//! | ---------------- | ----------------------------------------- | -------------------------- |
//! | `reclaim`        | allocations above their reservation       | `memory.reclaim` (a sandbox with a balloon: inflate it), nothing stops |
//! | `suspend`        | tasks that set `suspend.onPressure = true`, most idle first | checkpointed and stopped, resumes later |
//! | `evict-bursting` | allocations above their reservation       | stopped, rescheduled       |
//! | `evict-reserved` | within reservation, below `critical`, only with `oversubscribe.memory > 1` | stopped, rescheduled |
//!
//! Within a step, candidates sort by band ascending (least important first,
//! the preemption order), then by bytes freed descending (`suspend`: longest
//! quiet first). Node-pinned storage is never evicted, matching preemption's
//! rule. It can be suspended, which is the point: suspend keeps the storage
//! where it is.
//!
//! The `suspend` step is the one place the worker changes an instance's
//! state without the manager asking: waiting a heartbeat for permission
//! would be wrong under pressure. It reuses the suspend capture
//! (`src/worker/checkpoints.rs`) with a worker-minted snapshot id, and
//! reports the checkpoint as a local suspension the manager applies with the
//! same transaction a SuspendInstance capture uses.
//!
//! The split mirrors `src/worker/usage.rs`: the ordering is a pure function
//! ([`order_for_step`], [`preview`]) so `legion node usage` can show "who
//! goes first" from the same code that acts, and the step runner
//! ([`PressureResponder`]) drives a [`PressureSource`] trait, so every rule
//! here is tested with a fake node on any machine. The only kernel-facing
//! code is [`LivePressureSource`].

use crate::prelude::*;
use crate::structs::{
    Band, MemoryPressureStatus, NodeMemoryUsage, PressureAction, UsageReport,
    WorkerMemoryPressureConfig,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Default `minFree`, as a fraction of node memory.
pub const DEFAULT_FREE_BELOW_FRACTION: f64 = 0.05;
/// Floor for the default `minFree`: below this a small node would act too
/// late to matter.
pub const MIN_DEFAULT_FREE_BELOW: u64 = 256 * MIB;
/// Ceiling for the default `minFree`: on a large node 5% is far more slack
/// than reclaim needs.
pub const MAX_DEFAULT_FREE_BELOW: u64 = 4 * GIB;
/// Default `maxStall`, in percent of PSI `some avg10`.
pub const DEFAULT_STALL_ABOVE_PERCENT: f64 = 10.0;
/// Default `settle`.
pub const DEFAULT_SETTLE: Duration = Duration::from_secs(5);
/// How often the responder checks the node. Cheap: one `/proc/meminfo` and
/// one PSI read per tick, and per-allocation cgroup reads only under
/// pressure.
pub const RESPONDER_TICK: Duration = Duration::from_secs(1);

/// One step of the response. Declaration order is the order steps run in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PressureStep {
    Reclaim,
    /// Checkpoint and stop instances whose task sets
    /// `suspend.onPressure = true`, most idle first. The instance becomes
    /// `suspended` and resumes later with its state.
    Suspend,
    EvictBursting,
    EvictReserved,
}

impl PressureStep {
    pub const ALL: [PressureStep; 4] = [
        PressureStep::Reclaim,
        PressureStep::Suspend,
        PressureStep::EvictBursting,
        PressureStep::EvictReserved,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            PressureStep::Reclaim => "reclaim",
            PressureStep::Suspend => "suspend",
            PressureStep::EvictBursting => "evict-bursting",
            PressureStep::EvictReserved => "evict-reserved",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|step| step.as_str() == value)
    }

    fn evicts(&self) -> bool {
        matches!(
            self,
            PressureStep::EvictBursting | PressureStep::EvictReserved
        )
    }
}

/// The response's settings, resolved against this node.
#[derive(Debug, Clone, PartialEq)]
pub struct PressureSettings {
    pub enabled: bool,
    /// This node's name, for detail strings.
    pub node_name: String,
    pub min_free_bytes: u64,
    pub max_stall_percent: f64,
    pub settle: Duration,
    /// Configured steps, deduplicated, in run order.
    pub steps: Vec<PressureStep>,
    /// `resourceEnforcement.oversubscribe.memory`; `evict-reserved` runs
    /// only above 1.0.
    pub oversubscribe_memory: f64,
}

impl PressureSettings {
    /// 5% of node memory, at least 256 MiB and at most 4 GiB.
    pub fn default_min_free(node_total_bytes: u64) -> u64 {
        ((node_total_bytes as f64 * DEFAULT_FREE_BELOW_FRACTION) as u64)
            .clamp(MIN_DEFAULT_FREE_BELOW, MAX_DEFAULT_FREE_BELOW)
    }

    /// Resolve the worker type's `memoryPressure` block (absent = all
    /// defaults) for a node with `node_total_bytes` of memory.
    pub fn resolve(
        config: Option<&WorkerMemoryPressureConfig>,
        oversubscribe_memory: f64,
        node_total_bytes: u64,
        node_name: &str,
    ) -> R<Self> {
        let default = WorkerMemoryPressureConfig::default();
        let config = config.unwrap_or(&default);
        let min_free_bytes = match config.min_free.as_deref() {
            Some(raw) => crate::structs::parse_byte_size(raw)
                .with_context(|| format!("invalid memoryPressure.minFree {raw:?}"))?,
            None => Self::default_min_free(node_total_bytes),
        };
        let mut steps: Vec<PressureStep> = match config.steps.as_ref() {
            Some(names) => names
                .iter()
                .map(|name| {
                    PressureStep::parse(name)
                        .with_context(|| format!("unknown memoryPressure step {name:?}"))
                })
                .collect::<R<_>>()?,
            None => PressureStep::ALL.to_vec(),
        };
        // Listing order never matters: the order is the point of the design.
        steps.sort();
        steps.dedup();
        Ok(Self {
            enabled: config.enabled.unwrap_or(true),
            node_name: node_name.to_string(),
            min_free_bytes,
            max_stall_percent: config
                .max_stall_percent()?
                .unwrap_or(DEFAULT_STALL_ABOVE_PERCENT),
            settle: config.settle_duration()?.unwrap_or(DEFAULT_SETTLE),
            steps,
            oversubscribe_memory: oversubscribe_memory.max(1.0),
        })
    }

    /// The steps that can act on this node: the configured ones, minus
    /// `evict-reserved` on a node without memory oversubscription, where
    /// every reservation fits in physical memory and a task within its
    /// reservation is never the problem.
    pub fn active_steps(&self) -> Vec<PressureStep> {
        self.steps
            .iter()
            .copied()
            .filter(|step| match step {
                PressureStep::EvictReserved => self.oversubscribe_memory > 1.0,
                _ => true,
            })
            .collect()
    }

    /// Whether `memory` crosses either trigger.
    pub fn triggered(&self, memory: &NodeMemoryUsage) -> bool {
        memory.available_bytes < self.min_free_bytes
            || (memory.psi_available && memory.psi_some_avg10 > self.max_stall_percent)
    }

    /// The wire status reported with usage.
    pub fn status(&self, under_pressure: bool) -> MemoryPressureStatus {
        MemoryPressureStatus {
            enabled: self.enabled,
            under_pressure: self.enabled && under_pressure,
            min_free_bytes: self.min_free_bytes,
            max_stall_percent: self.max_stall_percent,
            settle_seconds: self.settle.as_secs(),
            steps: self
                .steps
                .iter()
                .filter(|step| {
                    **step != PressureStep::EvictReserved || self.oversubscribe_memory > 1.0
                })
                .map(|step| step.as_str().to_string())
                .collect(),
        }
    }
}

/// What the worker knows about an allocation besides its measurements:
/// enough to place it in the order.
#[derive(Debug, Clone, PartialEq)]
pub struct PressureMeta {
    pub job_id: String,
    pub band: Band,
    /// `resources.memory` in bytes. 0 when the task declared none.
    pub reserved_bytes: u64,
    /// True when the allocation's instance storage lives only on this node.
    /// Such an allocation is never evicted: it could only come back here.
    pub node_pinned: bool,
    /// The task sets `suspend.onPressure = true`: the `suspend` step may
    /// checkpoint and stop it.
    pub on_pressure: bool,
}

impl Default for PressureMeta {
    /// Unknown reads as untouchable: node-pinned and not opted into suspend,
    /// so no step ever stops an allocation the worker cannot describe.
    fn default() -> Self {
        Self {
            job_id: String::new(),
            band: Band::default(),
            reserved_bytes: 0,
            node_pinned: true,
            on_pressure: false,
        }
    }
}

/// One allocation as the ordering sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct PressureCandidate {
    pub allocation_id: i64,
    pub meta: PressureMeta,
    /// `memory.current` in bytes.
    pub used_bytes: u64,
    /// Unix seconds since which the allocation has been quiet: its idle
    /// policy's idle-since when it has one, else its last activity. 0 when
    /// unknown, which sorts as the least quiet.
    pub quiet_since_unix: i64,
}

impl PressureCandidate {
    /// Using more than it reserved right now.
    pub fn bursting(&self) -> bool {
        self.used_bytes > self.meta.reserved_bytes
    }

    fn above_reservation(&self) -> u64 {
        self.used_bytes.saturating_sub(self.meta.reserved_bytes)
    }

    /// Sort key for "most idle first": unknown sorts last.
    fn quiet_key(&self) -> i64 {
        if self.quiet_since_unix > 0 {
            self.quiet_since_unix
        } else {
            i64::MAX
        }
    }
}

/// Whether `step` may act on `candidate` at all.
pub fn eligible(
    step: PressureStep,
    candidate: &PressureCandidate,
    settings: &PressureSettings,
) -> bool {
    match step {
        PressureStep::Reclaim => candidate.bursting(),
        // Opted in, in any band and with node-pinned storage too: a suspend
        // keeps the storage where it is and the instance comes back with
        // its state.
        PressureStep::Suspend => candidate.meta.on_pressure && candidate.used_bytes > 0,
        PressureStep::EvictBursting => candidate.bursting() && !candidate.meta.node_pinned,
        PressureStep::EvictReserved => {
            settings.oversubscribe_memory > 1.0
                && !candidate.bursting()
                && !candidate.meta.node_pinned
                && candidate.meta.band != Band::Critical
                && candidate.used_bytes > 0
        }
    }
}

/// What acting on `candidate` with `step` is expected to free: the bytes
/// above its reservation for `reclaim`, everything it holds for a suspend or
/// an eviction.
pub fn freed_estimate(step: PressureStep, candidate: &PressureCandidate) -> u64 {
    if step.evicts() || step == PressureStep::Suspend {
        candidate.used_bytes
    } else {
        candidate.above_reservation()
    }
}

/// The candidates `step` would act on, in the order it acts: band ascending
/// (least important first), then bytes freed descending (`suspend`: quiet
/// longest first), then allocation id so the order is total and stable.
pub fn order_for_step<'a>(
    step: PressureStep,
    candidates: &'a [PressureCandidate],
    settings: &PressureSettings,
) -> Vec<&'a PressureCandidate> {
    let mut ordered: Vec<&PressureCandidate> = candidates
        .iter()
        .filter(|candidate| eligible(step, candidate, settings))
        .collect();
    ordered.sort_by(|a, b| {
        let within_band = if step == PressureStep::Suspend {
            a.quiet_key().cmp(&b.quiet_key())
        } else {
            freed_estimate(step, b).cmp(&freed_estimate(step, a))
        };
        a.meta
            .band
            .rank()
            .cmp(&b.meta.band.rank())
            .then(within_band)
            .then_with(|| a.allocation_id.cmp(&b.allocation_id))
    });
    ordered
}

/// One allocation's place in the node's pressure order.
#[derive(Debug, Clone, PartialEq)]
pub struct PreviewEntry {
    pub allocation_id: i64,
    /// 1-based position of the allocation's first appearance as the steps
    /// run in order.
    pub rank: u32,
    /// Every step that would act on it, in run order.
    pub steps: Vec<PressureStep>,
}

/// "Who goes first": every allocation some active step would act on, in the
/// order the response would reach it if the node ran short now and nothing
/// it did helped. Computed from the same functions the responder acts with.
pub fn preview(candidates: &[PressureCandidate], settings: &PressureSettings) -> Vec<PreviewEntry> {
    let mut entries: Vec<PreviewEntry> = Vec::new();
    let mut index: HashMap<i64, usize> = HashMap::new();
    for step in settings.active_steps() {
        for candidate in order_for_step(step, candidates, settings) {
            match index.get(&candidate.allocation_id) {
                // A suspended allocation is gone: no later step reaches it.
                Some(position) if entries[*position].steps.contains(&PressureStep::Suspend) => {}
                Some(position) => entries[*position].steps.push(step),
                None => {
                    index.insert(candidate.allocation_id, entries.len());
                    entries.push(PreviewEntry {
                        allocation_id: candidate.allocation_id,
                        rank: entries.len() as u32 + 1,
                        steps: vec![step],
                    });
                }
            }
        }
    }
    entries
}

/// Fill a usage report's pressure fields: the node's response status and
/// each allocation's rank and steps. `settings` is `None` when the response
/// is off on this node, which reports `enabled: false` and no order.
pub fn annotate(
    report: &mut UsageReport,
    metas: &HashMap<i64, PressureMeta>,
    settings: Option<&PressureSettings>,
    under_pressure: bool,
) {
    let Some(settings) = settings.filter(|settings| settings.enabled) else {
        report.pressure = Some(MemoryPressureStatus::default());
        return;
    };
    let candidates: Vec<PressureCandidate> = report
        .allocations
        .iter()
        .map(|usage| PressureCandidate {
            allocation_id: usage.allocation_id,
            meta: metas.get(&usage.allocation_id).cloned().unwrap_or_default(),
            used_bytes: usage.memory_current_bytes,
            quiet_since_unix: quiet_since(usage),
        })
        .collect();
    let order: HashMap<i64, PreviewEntry> = preview(&candidates, settings)
        .into_iter()
        .map(|entry| (entry.allocation_id, entry))
        .collect();
    for usage in &mut report.allocations {
        if let Some(entry) = order.get(&usage.allocation_id) {
            usage.pressure_rank = entry.rank;
            usage.pressure_steps = entry.steps.iter().map(|s| s.as_str().to_string()).collect();
        }
    }
    report.pressure = Some(settings.status(under_pressure));
}

/// When an allocation went quiet, for "most idle first": its idle policy's
/// idle-since when it has one, else its last activity.
pub fn quiet_since(usage: &crate::structs::AllocationUsage) -> i64 {
    if usage.idle_since_unix > 0 {
        usage.idle_since_unix
    } else {
        usage.last_activity_unix
    }
}

/// The quiet-since of every allocation in the last usage report, for the
/// live responder, which measures memory itself but not activity.
static QUIET_SINCE: std::sync::LazyLock<std::sync::Mutex<HashMap<i64, i64>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// Record the latest report's quiet-since values (called after each sample).
pub fn publish_quiet_since(report: &UsageReport) {
    *QUIET_SINCE.lock().unwrap_or_else(|p| p.into_inner()) = report
        .allocations
        .iter()
        .map(|usage| (usage.allocation_id, quiet_since(usage)))
        .collect();
}

/// What a `suspend` action did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSuspension {
    /// The worker-minted checkpoint id.
    pub snapshot_id: String,
    /// What the allocation held when it stopped.
    pub freed_bytes: u64,
}

/// The node, as the responder sees it. The live implementation reads
/// `/proc` and cgroups and stops allocations through the task manager; tests
/// use a fake.
pub trait PressureSource {
    /// Node-wide memory now. `None` when it cannot be read (not Linux), which
    /// keeps the responder idle.
    fn node_memory(&mut self) -> Option<NodeMemoryUsage>;
    /// Every governed allocation, measured now.
    fn candidates(&mut self) -> Vec<PressureCandidate>;
    /// Ask the kernel to reclaim up to `bytes` from the allocation's cgroup.
    /// Returns the bytes actually freed.
    fn reclaim(&mut self, allocation_id: i64, bytes: u64) -> R<u64>;
    /// Stop the allocation with `cause = node_pressure`.
    fn evict(&mut self, allocation_id: i64) -> R<()>;
    /// Checkpoint and stop the allocation as a local suspension. On error
    /// the task keeps running (a suspend never stops what it failed to
    /// capture).
    fn suspend(&mut self, allocation_id: i64) -> R<LocalSuspension>;
    /// The episode ended: undo what `reclaim` holds that the guest should
    /// get back (deflate sandbox balloons). Default: nothing to undo,
    /// `memory.reclaim` holds nothing.
    fn relax(&mut self) {}
}

/// The step runner. Holds per-episode state: an episode starts when a
/// trigger fires and ends when neither does.
#[derive(Debug)]
pub struct PressureResponder {
    settings: PressureSettings,
    last_action: Option<Instant>,
    /// `(step, allocation)` pairs already acted on this episode, so a step
    /// never repeats itself on the same allocation and the response moves
    /// on to the next step instead.
    acted: HashSet<(PressureStep, i64)>,
    /// Allocations this responder stopped or suspended. Kept beyond the
    /// episode: a stopped allocation may linger in the candidate list until
    /// its process exits.
    evicted: HashSet<i64>,
    under_pressure: bool,
    /// Whether `reclaim` acted this episode, so its end relaxes it.
    reclaimed: bool,
}

impl PressureResponder {
    pub fn new(settings: PressureSettings) -> Self {
        Self {
            settings,
            last_action: None,
            acted: HashSet::new(),
            evicted: HashSet::new(),
            under_pressure: false,
            reclaimed: false,
        }
    }

    pub fn settings(&self) -> &PressureSettings {
        &self.settings
    }

    /// Whether a trigger fired on the last tick.
    pub fn under_pressure(&self) -> bool {
        self.under_pressure
    }

    /// One check of the node. Returns what it did, for the heartbeat.
    ///
    /// Under pressure, and once `settle` has passed since the last action,
    /// it takes the first step (in run order) that still has a candidate it
    /// has not acted on this episode, and acts on that step's candidates in
    /// order until their expected freed bytes cover the shortfall below
    /// `minFree` (at least one, when only the stall trigger fired). Then
    /// it waits `settle` to see whether that was enough. A step with nothing
    /// left to do hands over to the next, so the response escalates only as
    /// far as it has to. With every step exhausted it does nothing more, and
    /// the kernel OOM killer is the backstop.
    pub fn tick(
        &mut self,
        now: Instant,
        now_unix_ms: i64,
        source: &mut dyn PressureSource,
    ) -> Vec<PressureAction> {
        if !self.settings.enabled {
            return Vec::new();
        }
        let Some(memory) = source.node_memory() else {
            self.under_pressure = false;
            return Vec::new();
        };
        self.under_pressure = self.settings.triggered(&memory);
        if !self.under_pressure {
            self.acted.clear();
            self.last_action = None;
            if std::mem::take(&mut self.reclaimed) {
                source.relax();
            }
            return Vec::new();
        }
        if self
            .last_action
            .is_some_and(|last| now.duration_since(last) < self.settings.settle)
        {
            return Vec::new();
        }

        let candidates: Vec<PressureCandidate> = source
            .candidates()
            .into_iter()
            .filter(|candidate| !self.evicted.contains(&candidate.allocation_id))
            .collect();
        let shortfall = self
            .settings
            .min_free_bytes
            .saturating_sub(memory.available_bytes);

        for step in self.settings.active_steps() {
            let ordered: Vec<PressureCandidate> = order_for_step(step, &candidates, &self.settings)
                .into_iter()
                .filter(|candidate| !self.acted.contains(&(step, candidate.allocation_id)))
                .cloned()
                .collect();
            let mut actions = Vec::new();
            let mut freed_total = 0u64;
            for candidate in ordered {
                if !actions.is_empty() && freed_total >= shortfall {
                    break;
                }
                self.acted.insert((step, candidate.allocation_id));
                let mut snapshot_id = String::new();
                let freed = match step {
                    PressureStep::Reclaim => {
                        match source.reclaim(candidate.allocation_id, candidate.above_reservation())
                        {
                            Ok(freed) => freed,
                            Err(err) => {
                                debug!(
                                    allocation_id = candidate.allocation_id,
                                    error = ?err,
                                    "memory.reclaim failed; moving on"
                                );
                                continue;
                            }
                        }
                    }
                    PressureStep::Suspend => match source.suspend(candidate.allocation_id) {
                        Ok(suspended) => {
                            self.evicted.insert(candidate.allocation_id);
                            snapshot_id = suspended.snapshot_id;
                            suspended.freed_bytes
                        }
                        Err(err) => {
                            warn!(
                                allocation_id = candidate.allocation_id,
                                error = ?err,
                                "Pressure suspend failed; the task keeps running"
                            );
                            continue;
                        }
                    },
                    PressureStep::EvictBursting | PressureStep::EvictReserved => {
                        if let Err(err) = source.evict(candidate.allocation_id) {
                            warn!(
                                allocation_id = candidate.allocation_id,
                                step = step.as_str(),
                                error = ?err,
                                "Pressure eviction failed"
                            );
                            continue;
                        }
                        self.evicted.insert(candidate.allocation_id);
                        candidate.used_bytes
                    }
                };
                if step == PressureStep::Reclaim {
                    self.reclaimed = true;
                }
                freed_total = freed_total.saturating_add(freed);
                let mut action = self.action(step, &candidate, &memory, freed, now_unix_ms);
                action.snapshot_id = snapshot_id;
                actions.push(action);
            }
            if !actions.is_empty() {
                self.last_action = Some(now);
                return actions;
            }
        }
        Vec::new()
    }

    /// Forget allocations that are gone from the node, so the evicted set
    /// stays bounded by what is still stopping.
    pub fn retain_evicted(&mut self, live: &HashSet<i64>) {
        self.evicted.retain(|id| live.contains(id));
    }

    fn action(
        &self,
        step: PressureStep,
        candidate: &PressureCandidate,
        memory: &NodeMemoryUsage,
        freed: u64,
        now_unix_ms: i64,
    ) -> PressureAction {
        let (detail, hint) = describe(step, candidate, memory, &self.settings, freed);
        PressureAction {
            allocation_id: candidate.allocation_id,
            job_id: candidate.meta.job_id.clone(),
            step: step.as_str().to_string(),
            freed_bytes: freed,
            used_bytes: candidate.used_bytes,
            reserved_bytes: candidate.meta.reserved_bytes,
            available_bytes: memory.available_bytes,
            min_free_bytes: self.settings.min_free_bytes,
            stall_percent: if memory.psi_available {
                memory.psi_some_avg10
            } else {
                0.0
            },
            max_stall_percent: self.settings.max_stall_percent,
            detail,
            hint,
            at_unix_ms: now_unix_ms,
            snapshot_id: String::new(),
        }
    }
}

/// Bytes the way a person reads them: "1.9 GiB", "2 GiB", "400 MiB".
pub fn human_bytes(bytes: u64) -> String {
    if bytes >= GIB {
        let gib = bytes as f64 / GIB as f64;
        let rounded = (gib * 10.0).round() / 10.0;
        if rounded.fract() == 0.0 {
            format!("{rounded:.0} GiB")
        } else {
            format!("{rounded:.1} GiB")
        }
    } else {
        format!("{} MiB", (bytes as f64 / MIB as f64).round() as u64)
    }
}

/// The event detail and the author's hint for one action. The detail names
/// the step and the numbers, so it answers "my own limit or the machine?"
/// without a docs lookup.
pub fn describe(
    step: PressureStep,
    candidate: &PressureCandidate,
    memory: &NodeMemoryUsage,
    settings: &PressureSettings,
    freed: u64,
) -> (String, String) {
    let trigger = if memory.available_bytes < settings.min_free_bytes {
        format!(
            "while free memory was {} (threshold {})",
            human_bytes(memory.available_bytes),
            human_bytes(settings.min_free_bytes)
        )
    } else {
        format!(
            "while tasks were stalled on memory {:.1}% of the time (threshold {}%)",
            memory.psi_some_avg10, settings.max_stall_percent
        )
    };
    let used = human_bytes(candidate.used_bytes);
    let reserved = human_bytes(candidate.meta.reserved_bytes);
    let node = &settings.node_name;
    let oversubscribed = settings.oversubscribe_memory > 1.0;
    match step {
        PressureStep::Reclaim => (
            format!(
                "reclaimed {} by reclaim on {node}: using {used} of a {reserved} reservation \
                 {trigger}",
                human_bytes(freed)
            ),
            String::new(),
        ),
        PressureStep::EvictBursting => (
            format!(
                "evicted by evict-bursting on {node}: using {used} of a {reserved} reservation \
                 {trigger}"
            ),
            if oversubscribed {
                "raise resources.memory toward real usage, or move this job to a pool without \
                 oversubscription"
                    .to_string()
            } else {
                "raise resources.memory toward real usage, so the task stops depending on memory \
                 other tasks have reserved"
                    .to_string()
            },
        ),
        PressureStep::EvictReserved => (
            format!(
                "evicted by evict-reserved on {node}: using {used} within a {reserved} \
                 reservation {trigger}, on a node that oversubscribes memory ×{}",
                settings.oversubscribe_memory
            ),
            "this node packs reservations past its physical memory; run this job in a higher \
             band, or in a pool without oversubscription"
                .to_string(),
        ),
        PressureStep::Suspend => (
            format!(
                "suspended by suspend on {node}: using {used} of a {reserved} reservation \
                 {trigger}. Its state is checkpointed and it resumes with `legion instance start`"
            ),
            "the task sets suspend.onPressure = true, so the node suspended it instead of \
             stopping it. Raise resources.memory toward real usage to make it less likely"
                .to_string(),
        ),
    }
}

/// The real node: `/proc` for node memory, the allocation cgroups for usage,
/// `memory.reclaim` (or a sandbox's balloon) for reclaim, the task manager
/// for evictions, and the suspend capture for `suspend`.
pub struct LivePressureSource {
    pub task_manager:
        std::sync::Arc<std::sync::Mutex<crate::worker::task_manager::WorkerTaskManager>>,
    /// Cgroup directories from the last `candidates` call, for `reclaim`.
    cgroups: HashMap<i64, PathBuf>,
    /// Sandboxes whose balloon `reclaim` inflated this episode, deflated by
    /// `relax` when it ends.
    ballooned: HashSet<i64>,
}

impl LivePressureSource {
    pub fn new(
        task_manager: std::sync::Arc<
            std::sync::Mutex<crate::worker::task_manager::WorkerTaskManager>,
        >,
    ) -> Self {
        Self {
            task_manager,
            cgroups: HashMap::new(),
            ballooned: HashSet::new(),
        }
    }
}

impl PressureSource for LivePressureSource {
    fn node_memory(&mut self) -> Option<NodeMemoryUsage> {
        crate::worker::usage::read_node_memory()
    }

    fn candidates(&mut self) -> Vec<PressureCandidate> {
        let targets = self
            .task_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .usage_targets();
        self.cgroups.clear();
        let quiet = QUIET_SINCE
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        targets
            .into_iter()
            .filter_map(|target| {
                // Only cgroup-governed work can be measured and acted on
                // here; an Apple container (macOS dev mode) has no cgroup.
                let dir = target.cgroup_dir()?.to_path_buf();
                let sample = crate::worker::usage::read_cgroup_sample(&dir)?;
                self.cgroups.insert(target.allocation_id, dir);
                Some(PressureCandidate {
                    allocation_id: target.allocation_id,
                    meta: target.pressure,
                    used_bytes: sample.memory_current_bytes,
                    quiet_since_unix: quiet.get(&target.allocation_id).copied().unwrap_or(0),
                })
            })
            .collect()
    }

    fn reclaim(&mut self, allocation_id: i64, bytes: u64) -> R<u64> {
        // A sandbox guest's memory is anonymous VMM memory the kernel cannot
        // reclaim without swap; its balloon is the lever that works.
        let ballooned = self
            .task_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reclaim_guest_memory(allocation_id, bytes)?;
        if let Some(freed) = ballooned {
            self.ballooned.insert(allocation_id);
            return Ok(freed);
        }
        let dir = self
            .cgroups
            .get(&allocation_id)
            .with_context(|| format!("no cgroup known for allocation {allocation_id}"))?;
        let current = |dir: &std::path::Path| {
            crate::worker::usage::read_cgroup_sample(dir)
                .map(|sample| sample.memory_current_bytes)
                .unwrap_or(0)
        };
        let before = current(dir);
        // `memory.reclaim` (Linux 5.19+) reclaims synchronously and returns
        // EAGAIN when it could not reclaim the whole amount, which is a
        // partial success here: whatever it did free is measured below.
        if let Err(err) = std::fs::write(dir.join("memory.reclaim"), bytes.to_string()) {
            if err.raw_os_error() != Some(11) {
                return Err(err).with_context(|| format!("write {}/memory.reclaim", dir.display()));
            }
        }
        Ok(before.saturating_sub(current(dir)))
    }

    fn evict(&mut self, allocation_id: i64) -> R<()> {
        self.task_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .evict_for_pressure(allocation_id)
    }

    fn suspend(&mut self, allocation_id: i64) -> R<LocalSuspension> {
        let used = self
            .cgroups
            .get(&allocation_id)
            .and_then(|dir| crate::worker::usage::read_cgroup_sample(dir))
            .map(|sample| sample.memory_current_bytes)
            .unwrap_or(0);
        let snapshot_id =
            crate::worker::checkpoints::run_local_suspension(&self.task_manager, allocation_id)?;
        Ok(LocalSuspension {
            snapshot_id,
            freed_bytes: used,
        })
    }

    fn relax(&mut self) {
        let mut manager = self.task_manager.lock().unwrap_or_else(|p| p.into_inner());
        for allocation_id in std::mem::take(&mut self.ballooned) {
            if let Err(err) = manager.release_guest_memory(allocation_id) {
                debug!(allocation_id, error = ?err, "Balloon deflate failed; the guest can still reclaim it on OOM");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(oversubscribe: f64) -> PressureSettings {
        PressureSettings::resolve(None, oversubscribe, 16 * GIB, "w-03").expect("defaults resolve")
    }

    fn candidate(id: i64, band: Band, reserved_mib: u64, used_mib: u64) -> PressureCandidate {
        PressureCandidate {
            allocation_id: id,
            meta: PressureMeta {
                job_id: format!("job-{id}"),
                band,
                reserved_bytes: reserved_mib * MIB,
                node_pinned: false,
                on_pressure: false,
            },
            used_bytes: used_mib * MIB,
            quiet_since_unix: 0,
        }
    }

    /// A task that sets `suspend.onPressure = true`, quiet since `quiet`.
    fn suspendable(
        id: i64,
        band: Band,
        reserved_mib: u64,
        used_mib: u64,
        quiet: i64,
    ) -> PressureCandidate {
        let mut candidate = candidate(id, band, reserved_mib, used_mib);
        candidate.meta.on_pressure = true;
        candidate.quiet_since_unix = quiet;
        candidate
    }

    fn ids(ordered: &[&PressureCandidate]) -> Vec<i64> {
        ordered
            .iter()
            .map(|candidate| candidate.allocation_id)
            .collect()
    }

    #[test]
    fn defaults_scale_with_the_node_and_clamp() {
        // 5% of 16 GiB is 819 MiB, inside the clamp.
        assert_eq!(
            PressureSettings::default_min_free(16 * GIB),
            (16.0 * GIB as f64 * 0.05) as u64
        );
        assert_eq!(PressureSettings::default_min_free(2 * GIB), 256 * MIB);
        assert_eq!(PressureSettings::default_min_free(512 * GIB), 4 * GIB);

        let resolved = settings(1.0);
        assert!(resolved.enabled);
        assert_eq!(resolved.max_stall_percent, 10.0);
        assert_eq!(resolved.settle, Duration::from_secs(5));
        assert_eq!(resolved.steps, PressureStep::ALL.to_vec());
    }

    #[test]
    fn configured_values_parse_and_steps_keep_their_fixed_order() -> R<()> {
        let config = WorkerMemoryPressureConfig {
            enabled: Some(true),
            min_free: Some("2GiB".into()),
            max_stall: Some(serde_json::json!("25%")),
            settle: Some("10s".into()),
            steps: Some(vec![
                "evict-bursting".into(),
                "reclaim".into(),
                "reclaim".into(),
            ]),
        };
        let resolved = PressureSettings::resolve(Some(&config), 1.5, 64 * GIB, "w-1")?;
        assert_eq!(resolved.min_free_bytes, 2 * GIB);
        assert_eq!(resolved.max_stall_percent, 25.0);
        assert_eq!(resolved.settle, Duration::from_secs(10));
        assert_eq!(
            resolved.steps,
            vec![PressureStep::Reclaim, PressureStep::EvictBursting]
        );

        let bad = WorkerMemoryPressureConfig {
            steps: Some(vec!["evict-everything".into()]),
            ..Default::default()
        };
        assert!(PressureSettings::resolve(Some(&bad), 1.0, GIB, "w-1").is_err());
        Ok(())
    }

    /// `suspend` acts on every node, and `evict-reserved` is only live on an
    /// oversubscribed node.
    #[test]
    fn suspend_always_acts_and_evict_reserved_needs_oversubscription() {
        assert_eq!(
            settings(1.0).active_steps(),
            vec![
                PressureStep::Reclaim,
                PressureStep::Suspend,
                PressureStep::EvictBursting
            ]
        );
        assert_eq!(
            settings(1.5).active_steps(),
            vec![
                PressureStep::Reclaim,
                PressureStep::Suspend,
                PressureStep::EvictBursting,
                PressureStep::EvictReserved
            ]
        );
        assert_eq!(
            settings(1.0).status(false).steps,
            vec!["reclaim", "suspend", "evict-bursting"]
        );
    }

    /// Band ascending first, then bytes freed descending, then id.
    #[test]
    fn candidates_order_by_band_then_bytes_freed() {
        let settings = settings(1.0);
        let candidates = vec![
            candidate(1, Band::Normal, 512, 1830),    // 1318 MiB over
            candidate(2, Band::Low, 400, 1900),       // 1500 MiB over
            candidate(3, Band::Low, 400, 2900),       // 2500 MiB over
            candidate(4, Band::High, 2048, 1900),     // within reservation
            candidate(5, Band::Background, 100, 200), // 100 MiB over
        ];
        assert_eq!(
            ids(&order_for_step(
                PressureStep::Reclaim,
                &candidates,
                &settings
            )),
            vec![5, 3, 2, 1]
        );
        assert_eq!(
            ids(&order_for_step(
                PressureStep::EvictBursting,
                &candidates,
                &settings
            )),
            vec![5, 3, 2, 1]
        );
        // Eviction frees everything the allocation holds, so within a band
        // the bigger user goes first even if it bursts less.
        let mixed = vec![
            candidate(1, Band::Low, 1000, 1100), // 100 over, 1100 held
            candidate(2, Band::Low, 100, 600),   // 500 over, 600 held
        ];
        assert_eq!(
            ids(&order_for_step(PressureStep::Reclaim, &mixed, &settings)),
            vec![2, 1]
        );
        assert_eq!(
            ids(&order_for_step(
                PressureStep::EvictBursting,
                &mixed,
                &settings
            )),
            vec![1, 2]
        );
    }

    #[test]
    fn node_pinned_storage_is_never_evicted() {
        let mut pinned = candidate(1, Band::Background, 100, 900);
        pinned.meta.node_pinned = true;
        let mut pinned_within = candidate(2, Band::Background, 900, 800);
        pinned_within.meta.node_pinned = true;
        let candidates = vec![pinned, pinned_within];
        let settings = settings(2.0);
        // It still gives memory back.
        assert_eq!(
            ids(&order_for_step(
                PressureStep::Reclaim,
                &candidates,
                &settings
            )),
            vec![1]
        );
        assert!(order_for_step(PressureStep::EvictBursting, &candidates, &settings).is_empty());
        assert!(order_for_step(PressureStep::EvictReserved, &candidates, &settings).is_empty());
    }

    #[test]
    fn evict_reserved_spares_critical_and_never_runs_at_ratio_one() {
        let candidates = vec![
            candidate(1, Band::Critical, 2048, 1900),
            candidate(2, Band::Normal, 2048, 1500),
            candidate(3, Band::Low, 512, 400),
        ];
        assert!(
            order_for_step(PressureStep::EvictReserved, &candidates, &settings(1.0)).is_empty()
        );
        assert_eq!(
            ids(&order_for_step(
                PressureStep::EvictReserved,
                &candidates,
                &settings(1.5)
            )),
            vec![3, 2]
        );
    }

    #[test]
    fn preview_ranks_by_first_step_and_lists_every_step() {
        let candidates = vec![
            candidate(10, Band::Normal, 512, 1830),
            candidate(20, Band::Low, 400, 1900),
            candidate(30, Band::High, 2048, 1900),
            candidate(40, Band::Critical, 1024, 900),
        ];
        let preview = preview(&candidates, &settings(1.5));
        let rows: Vec<(i64, u32, Vec<&str>)> = preview
            .iter()
            .map(|entry| {
                (
                    entry.allocation_id,
                    entry.rank,
                    entry.steps.iter().map(|step| step.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (20, 1, vec!["reclaim", "evict-bursting"]),
                (10, 2, vec!["reclaim", "evict-bursting"]),
                (30, 3, vec!["evict-reserved"]),
            ],
            "critical within its reservation never appears"
        );
    }

    /// A fake node: fixed candidates, a scripted free-memory reading, and a
    /// log of what the responder did to it.
    struct FakeNode {
        memory: NodeMemoryUsage,
        candidates: Vec<PressureCandidate>,
        reclaimed: Vec<(i64, u64)>,
        evicted: Vec<i64>,
        suspended: Vec<i64>,
        failing_suspends: Vec<i64>,
        relaxed: u32,
        /// Fraction of the requested bytes `reclaim` actually frees.
        reclaim_yield: f64,
    }

    impl FakeNode {
        fn new(available_mib: u64, candidates: Vec<PressureCandidate>) -> Self {
            Self {
                memory: NodeMemoryUsage {
                    total_bytes: 16 * GIB,
                    available_bytes: available_mib * MIB,
                    psi_some_avg10: 0.0,
                    psi_available: true,
                },
                candidates,
                reclaimed: Vec::new(),
                evicted: Vec::new(),
                suspended: Vec::new(),
                failing_suspends: Vec::new(),
                relaxed: 0,
                reclaim_yield: 0.0,
            }
        }
    }

    impl PressureSource for FakeNode {
        fn node_memory(&mut self) -> Option<NodeMemoryUsage> {
            Some(self.memory)
        }
        fn candidates(&mut self) -> Vec<PressureCandidate> {
            self.candidates
                .iter()
                .filter(|candidate| !self.evicted.contains(&candidate.allocation_id))
                .cloned()
                .collect()
        }
        fn reclaim(&mut self, allocation_id: i64, bytes: u64) -> R<u64> {
            self.reclaimed.push((allocation_id, bytes));
            Ok((bytes as f64 * self.reclaim_yield) as u64)
        }
        fn evict(&mut self, allocation_id: i64) -> R<()> {
            self.evicted.push(allocation_id);
            Ok(())
        }
        fn suspend(&mut self, allocation_id: i64) -> R<LocalSuspension> {
            if self.failing_suspends.contains(&allocation_id) {
                bail!("content store full");
            }
            self.suspended.push(allocation_id);
            // Gone from the node, like an eviction.
            self.evicted.push(allocation_id);
            let used = self
                .candidates
                .iter()
                .find(|candidate| candidate.allocation_id == allocation_id)
                .map(|candidate| candidate.used_bytes)
                .unwrap_or(0);
            Ok(LocalSuspension {
                snapshot_id: format!("snap-{allocation_id}"),
                freed_bytes: used,
            })
        }
        fn relax(&mut self) {
            self.relaxed += 1;
        }
    }

    /// The Phase 2 acceptance scenario with fakes: two bursting tasks on a
    /// node below its watermark. Reclaim runs first and frees nothing, then
    /// the lower band is evicted and the higher survives.
    #[test]
    fn two_bursting_tasks_lower_band_is_evicted_after_reclaim() {
        let mut node = FakeNode::new(
            500,
            vec![
                candidate(1, Band::High, 400, 1900),
                candidate(2, Band::Low, 400, 1900),
            ],
        );
        let mut responder = PressureResponder::new(settings(1.0));
        let start = Instant::now();

        // Default minFree on 16 GiB is ~819 MiB; 500 MiB free is short by
        // ~319 MiB, so reclaim asks both (nothing is freed) and stops there.
        let first = responder.tick(start, 1_000, &mut node);
        assert!(responder.under_pressure());
        assert_eq!(
            first.iter().map(|a| a.step.as_str()).collect::<Vec<_>>(),
            vec!["reclaim", "reclaim"]
        );
        assert_eq!(node.reclaimed, vec![(2, 1500 * MIB), (1, 1500 * MIB)]);

        // Inside the settle window nothing happens.
        assert!(responder
            .tick(start + Duration::from_secs(2), 3_000, &mut node)
            .is_empty());

        // After settle, still short: evict-bursting takes the low band only,
        // because its 1.9 GiB covers the shortfall.
        let second = responder.tick(start + Duration::from_secs(6), 7_000, &mut node);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].step, "evict-bursting");
        assert_eq!(second[0].allocation_id, 2);
        assert_eq!(second[0].freed_bytes, 1900 * MIB);
        assert_eq!(node.evicted, vec![2]);
        assert!(
            second[0]
                .detail
                .starts_with("evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation while free memory was 500 MiB (threshold"),
            "{}",
            second[0].detail
        );
        assert!(second[0].hint.contains("raise resources.memory"));

        // Pressure clears: the episode ends and nothing further is touched.
        node.memory.available_bytes = 8 * GIB;
        assert!(responder
            .tick(start + Duration::from_secs(12), 13_000, &mut node)
            .is_empty());
        assert!(!responder.under_pressure());
        assert_eq!(node.evicted, vec![2], "the high band survived");
    }

    /// At ratio 1.0 a task within its reservation is never stopped, however
    /// long the pressure lasts.
    #[test]
    fn within_reservation_is_never_evicted_at_ratio_one() {
        let mut node = FakeNode::new(100, vec![candidate(1, Band::Background, 4096, 4000)]);
        let mut responder = PressureResponder::new(settings(1.0));
        let start = Instant::now();
        for second in 0..60 {
            let actions = responder.tick(start + Duration::from_secs(second), 0, &mut node);
            assert!(actions.is_empty(), "{actions:?}");
        }
        assert!(node.evicted.is_empty());

        // The same node oversubscribed may stop it.
        let mut responder = PressureResponder::new(settings(1.5));
        let actions = responder.tick(start, 0, &mut node);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].step, "evict-reserved");
        assert!(actions[0].detail.contains("oversubscribes memory ×1.5"));
    }

    /// The Phase 5 acceptance scenario with fakes: a pressured node suspends
    /// the opted-in instances, most idle first, before evict-bursting stops
    /// anything, and the eviction only comes when suspending was not enough.
    #[test]
    fn opted_in_instances_are_suspended_most_idle_first_before_evict_bursting() {
        let mut node = FakeNode::new(
            100,
            vec![
                // Bursting and opted in, quiet since 500.
                suspendable(1, Band::Normal, 512, 900, 500),
                // Within its reservation and opted in, quiet longer (100):
                // a suspend may still take it, an eviction never would.
                suspendable(2, Band::Normal, 1024, 600, 100),
                // Bursting, not opted in.
                candidate(3, Band::Low, 400, 1900),
            ],
        );
        let mut responder = PressureResponder::new(settings(1.0));
        let start = Instant::now();

        let first = responder.tick(start, 0, &mut node);
        assert!(first.iter().all(|a| a.step == "reclaim"), "{first:?}");

        // Shortfall ~719 MiB: suspending 2 (600 MiB) is not enough, so 1
        // goes too, in idle order. Nothing is evicted.
        let second = responder.tick(start + Duration::from_secs(6), 0, &mut node);
        assert_eq!(
            second
                .iter()
                .map(|a| (a.step.as_str(), a.allocation_id))
                .collect::<Vec<_>>(),
            vec![("suspend", 2), ("suspend", 1)]
        );
        assert_eq!(node.suspended, vec![2, 1]);
        assert_eq!(second[0].snapshot_id, "snap-2");
        assert_eq!(second[0].freed_bytes, 600 * MIB);
        assert!(
            second[0].detail.starts_with("suspended by suspend on w-03"),
            "{}",
            second[0].detail
        );
        assert!(second[0].hint.contains("onPressure"));
        assert!(node.evicted.iter().all(|id| *id != 3));

        // Still short after settle: only now does evict-bursting act, on the
        // one task that did not opt in.
        let third = responder.tick(start + Duration::from_secs(12), 0, &mut node);
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].step, "evict-bursting");
        assert_eq!(third[0].allocation_id, 3);

        // Pressure clears: reclaim's holds are relaxed once.
        node.memory.available_bytes = 8 * GIB;
        responder.tick(start + Duration::from_secs(18), 0, &mut node);
        responder.tick(start + Duration::from_secs(19), 0, &mut node);
        assert_eq!(node.relaxed, 1);
    }

    /// A suspend that fails leaves the task running and the response moves
    /// on, the same as a failed eviction.
    #[test]
    fn a_failed_suspend_moves_on() {
        let mut node = FakeNode::new(
            100,
            vec![
                suspendable(1, Band::Normal, 1024, 600, 100),
                suspendable(2, Band::Normal, 1024, 900, 200),
            ],
        );
        node.failing_suspends = vec![1];
        let mut responder = PressureResponder::new(settings(1.0));
        let actions = responder.tick(Instant::now(), 0, &mut node);
        assert_eq!(
            actions
                .iter()
                .map(|a| (a.step.as_str(), a.allocation_id))
                .collect::<Vec<_>>(),
            vec![("suspend", 2)]
        );
        assert_eq!(node.suspended, vec![2]);
    }

    /// Node-pinned storage is never evicted but can be suspended, and a
    /// `critical` task that opted in is suspended too: the author asked.
    #[test]
    fn suspend_takes_node_pinned_and_critical_tasks_that_opted_in() {
        let mut pinned = suspendable(1, Band::Critical, 100, 900, 50);
        pinned.meta.node_pinned = true;
        let candidates = vec![pinned, candidate(2, Band::Low, 100, 900)];
        let settings = settings(1.0);
        assert_eq!(
            ids(&order_for_step(
                PressureStep::Suspend,
                &candidates,
                &settings
            )),
            vec![1]
        );
        assert_eq!(
            ids(&order_for_step(
                PressureStep::EvictBursting,
                &candidates,
                &settings
            )),
            vec![2]
        );
    }

    /// The preview shows `reclaim, then suspend` for an opted-in bursting
    /// task, and stops there: a suspended allocation is gone.
    #[test]
    fn the_preview_ends_at_suspend_for_opted_in_tasks() {
        let candidates = vec![
            suspendable(1, Band::Normal, 512, 1830, 100),
            candidate(2, Band::Low, 400, 1900),
            suspendable(3, Band::High, 2048, 1000, 100),
        ];
        let rows: Vec<(i64, Vec<&str>)> = preview(&candidates, &settings(1.5))
            .iter()
            .map(|entry| {
                (
                    entry.allocation_id,
                    entry.steps.iter().map(|step| step.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (2, vec!["reclaim", "evict-bursting"]),
                (1, vec!["reclaim", "suspend"]),
                (3, vec!["suspend"]),
            ]
        );
    }

    #[test]
    fn a_step_removed_from_steps_never_runs() {
        let config = WorkerMemoryPressureConfig {
            steps: Some(vec!["reclaim".into()]),
            ..Default::default()
        };
        let settings = PressureSettings::resolve(Some(&config), 2.0, 16 * GIB, "w-1").unwrap();
        let mut node = FakeNode::new(
            10,
            vec![
                candidate(1, Band::Low, 100, 900),
                candidate(2, Band::Low, 900, 800),
            ],
        );
        let mut responder = PressureResponder::new(settings);
        let start = Instant::now();
        for second in (0..60).step_by(6) {
            responder.tick(start + Duration::from_secs(second), 0, &mut node);
        }
        assert_eq!(node.reclaimed.len(), 1, "reclaim acts once per episode");
        assert!(node.evicted.is_empty());
    }

    #[test]
    fn the_stall_trigger_acts_on_one_candidate_at_a_time() {
        let mut node = FakeNode::new(
            8 * 1024,
            vec![
                candidate(1, Band::Low, 100, 900),
                candidate(2, Band::Low, 100, 800),
            ],
        );
        node.memory.psi_some_avg10 = 30.0;
        let mut responder = PressureResponder::new(settings(1.0));
        let actions = responder.tick(Instant::now(), 0, &mut node);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].allocation_id, 1);
        assert!(actions[0].detail.contains("stalled on memory 30.0%"));
    }

    #[test]
    fn a_disabled_response_never_acts() {
        let config = WorkerMemoryPressureConfig {
            enabled: Some(false),
            ..Default::default()
        };
        let settings = PressureSettings::resolve(Some(&config), 1.0, 16 * GIB, "w-1").unwrap();
        let mut node = FakeNode::new(1, vec![candidate(1, Band::Low, 100, 900)]);
        let mut responder = PressureResponder::new(settings);
        assert!(responder.tick(Instant::now(), 0, &mut node).is_empty());
        assert!(node.reclaimed.is_empty() && node.evicted.is_empty());
    }

    #[test]
    fn annotate_fills_rank_steps_and_status() {
        use crate::structs::AllocationUsage;
        let mut report = UsageReport {
            sampled_at_unix_ms: 0,
            allocations: vec![
                AllocationUsage {
                    allocation_id: 1,
                    memory_current_bytes: 900 * MIB,
                    ..Default::default()
                },
                AllocationUsage {
                    allocation_id: 2,
                    memory_current_bytes: 100 * MIB,
                    ..Default::default()
                },
            ],
            node_memory: None,
            pressure: None,
        };
        let metas = HashMap::from([
            (1, candidate(1, Band::Low, 512, 0).meta),
            (2, candidate(2, Band::Low, 512, 0).meta),
        ]);
        annotate(&mut report, &metas, Some(&settings(1.0)), true);
        assert_eq!(report.allocations[0].pressure_rank, 1);
        assert_eq!(
            report.allocations[0].pressure_steps,
            vec!["reclaim", "evict-bursting"]
        );
        assert_eq!(report.allocations[1].pressure_rank, 0);
        let status = report.pressure.clone().expect("status");
        assert!(status.enabled && status.under_pressure);

        let mut off = report.clone();
        off.allocations[0].pressure_rank = 0;
        annotate(&mut off, &metas, None, false);
        assert!(!off.pressure.expect("status").enabled);
    }

    #[test]
    fn bytes_read_like_a_person_wrote_them() {
        assert_eq!(human_bytes(400 * MIB), "400 MiB");
        assert_eq!(human_bytes(2 * GIB), "2 GiB");
        assert_eq!(human_bytes(1900 * MIB), "1.9 GiB");
    }
}
