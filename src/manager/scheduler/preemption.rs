//! Scheduler preemption: choosing which allocations to displace so
//! higher-band work can place.
//!
//! The whole feature rests on one correctness claim, and it is a claim about
//! *transactions*, not about scoring: victims are chosen during planning and
//! committed — deleted, requeued, journalled — in the **same** epoch-CAS
//! transaction that inserts the preemptor's allocations. There is no
//! nomination step, so there is no window in which freed capacity exists
//! un-owned. Kubernetes needed three KEPs to partially close that window and
//! still shipped an 8,000-eviction preemption loop; Volcano shipped evictions
//! whose preemption then failed. Legion's store shape — allocations are rows,
//! plans are compare-and-set — makes the reservation free.
//!
//! Everything in this module is deliberately a **pure function of an eligible
//! set**. Victim selection takes candidates and an ask and returns an ordered
//! victim list; it reads no clock, touches no store, and mutates nothing. That
//! is what keeps it testable, and it is also the seam a future
//! `PreemptionPolicy` plugin would open around once the semantics have proven
//! out (see the judgement calls — the seam is deliberately closed in v1).
//!
//! See `docs/preemption-and-eviction.md`.

use crate::prelude::*;
use crate::resources::{ResourcePool, ResourceRequest, ResourceValue};
use crate::structs::Band;

/// Default minimum age before an allocation may be preempted.
///
/// Slurm's `PreemptExemptTime`, and the cheapest thrash guard in any system:
/// without it, plan A preempts plan B's placement seconds after it lands, and
/// plan B's follow-up eval preempts the replacement right back.
pub const DEFAULT_VICTIM_EXEMPT_AGE: std::time::Duration = std::time::Duration::from_secs(60);

/// What the cluster does when a plan would preempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PreemptionMode {
    /// Select victims and commit them with the placement. The default —
    /// bands make preemption workload-opt-in (nothing is a victim unless its
    /// author chose a lower band), so unlike Nomad there is no hidden switch
    /// between "priority set" and "priority means something".
    #[default]
    Enforce,
    /// Run the identical victim-selection pass, emit
    /// `allocation.preempt_planned` and the metric, and commit nothing. The
    /// placement fails exactly as it does today. YARN's `observe_only`, for
    /// operators who want evidence before enabling.
    Observe,
    /// Never run the pass at all.
    Off,
}

impl PreemptionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            PreemptionMode::Enforce => "enforce",
            PreemptionMode::Observe => "observe",
            PreemptionMode::Off => "off",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "enforce" => Some(PreemptionMode::Enforce),
            "observe" => Some(PreemptionMode::Observe),
            "off" => Some(PreemptionMode::Off),
            _ => None,
        }
    }
}

/// `schedulerSettings.preemption` — the operator's controls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreemptionSettings {
    pub mode: PreemptionMode,
    /// Pools where preemption never runs, whatever the bands say. The TODO's
    /// "eviction toggled at the pool level": a GPU pool whose jobs each hold
    /// hours of un-checkpointed state can opt out wholesale.
    pub disabled_pools: Vec<String>,
    /// Minimum age before an allocation is eligible to be a victim.
    pub victim_exempt_age: std::time::Duration,
}

impl Default for PreemptionSettings {
    fn default() -> Self {
        Self {
            mode: PreemptionMode::default(),
            disabled_pools: Vec::new(),
            victim_exempt_age: DEFAULT_VICTIM_EXEMPT_AGE,
        }
    }
}

impl PreemptionSettings {
    /// Parse `schedulerSettings.preemption`. Unknown values are a **hard
    /// error**: a mistyped `mode: "observ"` silently becoming `enforce` would
    /// evict production work an operator explicitly tried not to evict.
    pub fn from_scheduler_settings(settings: Option<&Value>) -> R<Self> {
        let Some(block) = settings.and_then(|settings| settings.get("preemption")) else {
            return Ok(Self::default());
        };
        if !block.is_object() {
            bail!("schedulerSettings.preemption must be an object");
        }
        let mode = match block.get("mode") {
            None => PreemptionMode::default(),
            Some(Value::String(raw)) => PreemptionMode::parse(raw).ok_or_else(|| {
                anyhow!(
                    "schedulerSettings.preemption.mode {raw:?} must be one of \
                     enforce, observe, off"
                )
            })?,
            Some(_) => bail!("schedulerSettings.preemption.mode must be a string"),
        };
        let disabled_pools = match block.get("disabledPools").or(block.get("disabled_pools")) {
            None => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        anyhow!(
                            "schedulerSettings.preemption.disabledPools entries must be strings"
                        )
                    })
                })
                .collect::<R<Vec<_>>>()?,
            Some(_) => bail!("schedulerSettings.preemption.disabledPools must be a list"),
        };
        let victim_exempt_age = match block
            .get("victimExemptAge")
            .or(block.get("victim_exempt_age"))
        {
            None => DEFAULT_VICTIM_EXEMPT_AGE,
            Some(Value::String(raw)) => humantime::parse_duration(raw).with_context(|| {
                format!("schedulerSettings.preemption.victimExemptAge {raw:?} is not a duration")
            })?,
            Some(Value::Number(seconds)) => std::time::Duration::from_secs_f64(
                seconds
                    .as_f64()
                    .filter(|value| *value >= 0.0)
                    .context("schedulerSettings.preemption.victimExemptAge must be >= 0")?,
            ),
            Some(_) => {
                bail!("schedulerSettings.preemption.victimExemptAge must be a duration string")
            }
        };
        Ok(Self {
            mode,
            disabled_pools,
            victim_exempt_age,
        })
    }

    /// Whether preemption runs for work landing in this pool.
    pub fn enabled_for_pool(&self, pool: &str) -> bool {
        !matches!(self.mode, PreemptionMode::Off)
            && !self.disabled_pools.iter().any(|name| name == pool)
    }
}

/// One allocation a plan proposes to evict, as the planner decided it and as
/// `job plan` previews it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedPreemption {
    pub allocation_id: i64,
    pub node_id: String,
    /// Victim's job name.
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    /// Victim's task name, for the preview and the error text.
    pub task_name: String,
    /// Victim's band. Always strictly below the preemptor's.
    pub band: Band,
    /// The victim's `stop.killAfter`, so the preview can say how long the
    /// displaced work has to shut down.
    pub kill_after_secs: u64,
}

/// An allocation that *could* be a victim, with everything selection needs.
///
/// Built once per plan, lazily — only when normal placement has already
/// failed — so a cluster that never preempts pays nothing for this feature.
#[derive(Debug, Clone)]
pub struct VictimCandidate {
    pub allocation_id: i64,
    pub node_id: String,
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    pub task_name: String,
    pub band: Band,
    /// Resources this allocation would return to the node.
    pub request: ResourceRequest,
    /// Seconds since placement. `None` for rows written before allocation
    /// timestamps existed, which are treated as old enough.
    pub age_secs: Option<i64>,
    /// The task's `disruption.minAvailable`, resolved against its count.
    pub min_available: u32,
    /// Live allocations of the same task across the fleet, which is what the
    /// floor is measured against.
    pub task_running: u32,
    /// True when this allocation's storage lives only on its own node, so
    /// evicting it frees nothing and costs a restart.
    pub node_pinned: bool,
    pub kill_after_secs: u64,
}

/// Why a candidate was not eligible — collected so the "could not place"
/// error can say something better than "no victims".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IneligibleTally {
    pub same_or_higher_band: usize,
    pub too_young: usize,
    pub node_pinned: usize,
    pub floor_bound: usize,
}

impl IneligibleTally {
    pub fn is_empty(&self) -> bool {
        *self == IneligibleTally::default()
    }

    /// A human-readable clause for the placement error, e.g.
    /// `2 allocations protected by disruption.minAvailable, 1 below
    /// victimExemptAge`. Empty when nothing was rejected for a stated reason.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if self.floor_bound > 0 {
            parts.push(format!(
                "{} protected by disruption.minAvailable",
                self.floor_bound
            ));
        }
        if self.too_young > 0 {
            parts.push(format!("{} below victimExemptAge", self.too_young));
        }
        if self.node_pinned > 0 {
            parts.push(format!(
                "{} holding node-local storage (never preemptible)",
                self.node_pinned
            ));
        }
        if self.same_or_higher_band > 0 {
            parts.push(format!(
                "{} in the same band or higher",
                self.same_or_higher_band
            ));
        }
        parts.join(", ")
    }
}

/// Filter a node's allocations down to the ones this preemptor may evict.
///
/// Every rule here is structural rather than tunable, which is the lesson
/// YARN's four interacting dampeners taught:
///
/// - **Strictly cross-band.** Equal bands never preempt each other, so
///   cascades are bounded by the band count and only ever run downward.
/// - **Old enough.** `victimExemptAge` stops a placement being undone
///   seconds after it lands.
/// - **Not node-pinned.** Evicting an instance whose data lives on this node
///   either frees nothing (it can only be recreated here) or abandons the
///   data. Both are indefensible, so the rule is categorical.
/// - **Above the floor.** `disruption.minAvailable` is enforced here, not
///   scored: Nomad's penalty-not-limit budget is documented as violated in
///   practice, and a floor the scheduler *cannot* cross is the one an
///   operator can reason about at 3am.
///
/// `already_taken` counts victims this plan has already chosen from each task,
/// so a floor holds across a multi-placement plan rather than per placement.
pub fn eligible_victims(
    candidates: &[VictimCandidate],
    preemptor: Band,
    victim_exempt_age: std::time::Duration,
    already_taken: &HashMap<i64, u32>,
) -> (Vec<VictimCandidate>, IneligibleTally) {
    let exempt_secs = victim_exempt_age.as_secs() as i64;
    let mut tally = IneligibleTally::default();
    let mut eligible = Vec::new();
    for candidate in candidates {
        if !preemptor.outranks(candidate.band) {
            tally.same_or_higher_band += 1;
            continue;
        }
        // A missing timestamp predates allocation timestamps entirely, so the
        // allocation is older than any exempt window by construction.
        if candidate.age_secs.is_some_and(|age| age < exempt_secs) {
            tally.too_young += 1;
            continue;
        }
        if candidate.node_pinned {
            tally.node_pinned += 1;
            continue;
        }
        let taken = already_taken
            .get(&candidate.task_config_id)
            .copied()
            .unwrap_or(0);
        let remaining = candidate.task_running.saturating_sub(taken);
        if remaining <= candidate.min_available {
            tally.floor_bound += 1;
            continue;
        }
        eligible.push(candidate.clone());
    }
    (eligible, tally)
}

/// Choose the smallest well-shaped victim set on one node, or `None` when the
/// node cannot fit the ask even with every eligible allocation evicted.
///
/// Nomad's shape, kept simple: lowest band first (take from the least
/// important work), then closest resource fit (never kill a 32 GiB allocation
/// to free 1 GiB when a right-sized victim exists), then allocation id for
/// determinism. Take greedily until the ask fits, then run a **reprieve
/// pass** — walk the chosen victims from most expensive to least and drop any
/// the placement turns out not to need.
///
/// Pure: no clock, no store, no mutation. `free` is the node's remaining free
/// capacity as the plan's ledger sees it.
pub fn select_victims(
    ask: &ResourceRequest,
    free: &ResourcePool,
    eligible: &[VictimCandidate],
) -> Option<Vec<VictimCandidate>> {
    if free.can_fit(ask) {
        return Some(Vec::new());
    }

    let mut ordered: Vec<&VictimCandidate> = eligible.iter().collect();
    ordered.sort_by(|left, right| {
        left.band
            .rank()
            .cmp(&right.band.rank())
            .then_with(|| {
                distance(ask, &left.request)
                    .partial_cmp(&distance(ask, &right.request))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.allocation_id.cmp(&right.allocation_id))
    });

    let mut pool = free.clone();
    let mut chosen: Vec<&VictimCandidate> = Vec::new();
    for candidate in ordered {
        if pool.can_fit(ask) {
            break;
        }
        pool.release(&candidate.request);
        chosen.push(candidate);
    }
    if !pool.can_fit(ask) {
        return None;
    }

    // Reprieve pass. Greedy take is band-ordered, so the last victims taken
    // are the most important ones — try to give those back first.
    let mut reprieved: Vec<&VictimCandidate> = Vec::new();
    for index in (0..chosen.len()).rev() {
        let mut without = free.clone();
        for (position, candidate) in chosen.iter().enumerate() {
            if position == index
                || reprieved
                    .iter()
                    .any(|spared| spared.allocation_id == candidate.allocation_id)
            {
                continue;
            }
            without.release(&candidate.request);
        }
        if without.can_fit(ask) {
            reprieved.push(chosen[index]);
        }
    }
    let victims: Vec<VictimCandidate> = chosen
        .into_iter()
        .filter(|candidate| {
            !reprieved
                .iter()
                .any(|spared| spared.allocation_id == candidate.allocation_id)
        })
        .cloned()
        .collect();
    Some(victims)
}

/// One node's victim set for one placement: [`eligible_victims`] then
/// [`select_victims`], with one exception to `victimExemptAge`.
///
/// The age guard must not push a placement *up* the band ladder. When the
/// only old-enough set takes work in a more important band (`normal`) while
/// younger work in a lower band (`low`) would also make room, the young
/// lower-band set wins. Otherwise the guard turns one eviction into two: the
/// displaced `normal` job requeues and, once the `low` work ages past the
/// guard, preempts it to come back (the 2026-10-12 spot check:
/// `band-claim (high)` took `hello-container (normal)`, which then took
/// `band-fill (low)`). The guard never *enables* a preemption: when no
/// old-enough set fits, the young work stays protected and the placement
/// fails as before.
///
/// The second pass runs only when the guard rejected something and the first
/// pass found victims, so the common path costs nothing extra.
pub fn choose_victims(
    ask: &ResourceRequest,
    free: &ResourcePool,
    candidates: &[VictimCandidate],
    preemptor: Band,
    victim_exempt_age: std::time::Duration,
    already_taken: &HashMap<i64, u32>,
) -> (Option<Vec<VictimCandidate>>, IneligibleTally) {
    let (eligible, tally) =
        eligible_victims(candidates, preemptor, victim_exempt_age, already_taken);
    let chosen = select_victims(ask, free, &eligible);
    let highest_band =
        |victims: &[VictimCandidate]| victims.iter().map(|victim| victim.band.rank()).max();
    let Some(highest) = chosen.as_deref().and_then(highest_band) else {
        return (chosen, tally);
    };
    if tally.too_young == 0 {
        return (chosen, tally);
    }
    let (with_young, _) = eligible_victims(
        candidates,
        preemptor,
        std::time::Duration::ZERO,
        already_taken,
    );
    match select_victims(ask, free, &with_young) {
        Some(lower) if highest_band(&lower).is_some_and(|rank| rank < highest) => {
            (Some(lower), tally)
        }
        _ => (chosen, tally),
    }
}

/// How badly a victim's size mismatches the ask, over the amount-typed
/// resources the ask actually names.
///
/// Normalized per resource so memory (MiB, thousands) does not swamp cores
/// (single digits) — the "prefer victims sized like the need" rule is only
/// meaningful if the dimensions are comparable. A victim larger than the ask
/// and one smaller by the same ratio score the same distance; the greedy take
/// then prefers whichever also comes first by band.
fn distance(ask: &ResourceRequest, victim: &ResourceRequest) -> f64 {
    let mut total = 0.0;
    let mut dims = 0.0;
    for (name, demand) in &ask.values {
        let ResourceValue::Amount(wanted) = demand else {
            continue;
        };
        if *wanted <= 0.0 {
            continue;
        }
        let have = match victim.values.get(name) {
            Some(ResourceValue::Amount(amount)) => *amount,
            _ => 0.0,
        };
        total += ((have - wanted) / wanted).abs();
        dims += 1.0;
    }
    if dims == 0.0 {
        // Nothing comparable to sort on (an items-only ask). Treat every
        // victim as equidistant and let band + id decide.
        return 0.0;
    }
    total / dims
}

/// Rank one node's preemption plan against another's. Fewer victims wins;
/// then the plan that takes the least important work; then node id, so a plan
/// is reproducible.
///
/// The caller applies the normal scoring policy only *within* the winning
/// group, so packing quality still decides among equally-costly options — and
/// a placement needing no victims at all never reaches this function, because
/// the preemption pass only runs when normal placement has already failed.
pub fn preemption_cost(victims: &[VictimCandidate]) -> (usize, u32) {
    let band_weight = victims
        .iter()
        .map(|victim| u32::from(victim.band.rank()))
        .sum();
    (victims.len(), band_weight)
}

#[cfg(test)]
mod preemption_test {
    use super::*;
    use serde_json::json;

    fn request(cpu: f64, memory: f64) -> ResourceRequest {
        ResourceRequest::from_value(&json!({ "cpu": cpu, "memory": memory }))
            .expect("resource request parses")
    }

    fn pool(cpu: f64, memory: f64) -> ResourcePool {
        let mut values = HashMap::new();
        values.insert("cpu".to_string(), ResourceValue::Amount(cpu));
        values.insert("memory".to_string(), ResourceValue::Amount(memory));
        ResourcePool { values }
    }

    fn candidate(id: i64, band: Band, cpu: f64, memory: f64) -> VictimCandidate {
        VictimCandidate {
            allocation_id: id,
            node_id: "node-a".into(),
            job_id: format!("job-{id}"),
            job_config_id: format!("job-{id}-1"),
            task_config_id: id,
            task_name: "worker".into(),
            band,
            request: request(cpu, memory),
            age_secs: Some(3600),
            min_available: 0,
            task_running: 4,
            node_pinned: false,
            kill_after_secs: 30,
        }
    }

    #[test]
    fn a_node_with_room_needs_no_victims() {
        let victims = select_victims(&request(100.0, 512.0), &pool(1000.0, 4096.0), &[])
            .expect("fits without preemption");
        assert!(victims.is_empty());
    }

    #[test]
    fn lowest_band_goes_first() {
        let eligible = vec![
            candidate(1, Band::Normal, 1000.0, 1024.0),
            candidate(2, Band::Background, 1000.0, 1024.0),
            candidate(3, Band::Low, 1000.0, 1024.0),
        ];
        let victims = select_victims(&request(1000.0, 1024.0), &pool(0.0, 0.0), &eligible)
            .expect("preemption frees enough");
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].allocation_id, 2, "background is taken first");
    }

    #[test]
    fn a_right_sized_victim_beats_an_oversized_one() {
        let eligible = vec![
            candidate(1, Band::Low, 8000.0, 32768.0),
            candidate(2, Band::Low, 1000.0, 1024.0),
        ];
        let victims = select_victims(&request(1000.0, 1024.0), &pool(0.0, 0.0), &eligible)
            .expect("preemption frees enough");
        assert_eq!(victims.len(), 1);
        assert_eq!(
            victims[0].allocation_id, 2,
            "never kill a 32 GiB allocation to free 1 GiB"
        );
    }

    #[test]
    fn the_reprieve_pass_drops_victims_the_placement_does_not_need() {
        // Two small victims are taken greedily; only one is actually needed
        // once the second (larger) one is counted.
        let eligible = vec![
            candidate(1, Band::Background, 200.0, 256.0),
            candidate(2, Band::Low, 2000.0, 4096.0),
        ];
        let victims = select_victims(&request(2000.0, 4096.0), &pool(0.0, 0.0), &eligible)
            .expect("preemption frees enough");
        assert_eq!(
            victims.iter().map(|v| v.allocation_id).collect::<Vec<_>>(),
            vec![2],
            "the tiny background alloc is reprieved once the big one is taken"
        );
    }

    #[test]
    fn a_node_that_cannot_fit_even_emptied_yields_nothing() {
        let eligible = vec![candidate(1, Band::Low, 100.0, 128.0)];
        assert!(select_victims(&request(9999.0, 99999.0), &pool(0.0, 0.0), &eligible).is_none());
    }

    #[test]
    fn preemption_is_strictly_cross_band() {
        let candidates = vec![
            candidate(1, Band::High, 100.0, 128.0),
            candidate(2, Band::Normal, 100.0, 128.0),
        ];
        let (eligible, tally) = eligible_victims(
            &candidates,
            Band::High,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].allocation_id, 2);
        assert_eq!(tally.same_or_higher_band, 1, "a peer is never a victim");
    }

    #[test]
    fn the_age_guard_never_pushes_a_placement_up_the_band_ladder() {
        let mut young_low = candidate(1, Band::Low, 250.0, 3072.0);
        young_low.age_secs = Some(30);
        let old_normal = candidate(2, Band::Normal, 250.0, 128.0);
        let ask = request(250.0, 3072.0);
        // Either victim alone makes room.
        let free = pool(1000.0, 2944.0);
        let (victims, tally) = choose_victims(
            &ask,
            &free,
            &[young_low.clone(), old_normal.clone()],
            Band::High,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        let victims = victims.expect("preemption frees enough");
        assert_eq!(
            victims.iter().map(|v| v.allocation_id).collect::<Vec<_>>(),
            vec![1],
            "the young low-band allocation goes instead of the normal one"
        );
        assert_eq!(tally.too_young, 1);

        // The guard still holds when the young work is the only way in.
        let (victims, _) = choose_victims(
            &ask,
            &pool(1000.0, 0.0),
            &[young_low.clone(), old_normal.clone()],
            Band::High,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        assert!(victims.is_none(), "old_normal alone cannot make room");

        // And young work in the SAME band as the old set gains nothing.
        let mut young_normal = candidate(3, Band::Normal, 250.0, 3072.0);
        young_normal.age_secs = Some(30);
        let (victims, _) = choose_victims(
            &ask,
            &free,
            &[young_normal, old_normal],
            Band::High,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        assert_eq!(
            victims
                .expect("fits")
                .iter()
                .map(|v| v.allocation_id)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn a_young_allocation_is_exempt_and_a_timestampless_one_is_not() {
        let mut young = candidate(1, Band::Low, 100.0, 128.0);
        young.age_secs = Some(5);
        let mut legacy = candidate(2, Band::Low, 100.0, 128.0);
        legacy.age_secs = None;
        let (eligible, tally) = eligible_victims(
            &[young, legacy],
            Band::High,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].allocation_id, 2);
        assert_eq!(tally.too_young, 1);
    }

    #[test]
    fn node_pinned_stateful_allocations_are_never_victims() {
        let mut pinned = candidate(1, Band::Background, 100.0, 128.0);
        pinned.node_pinned = true;
        let (eligible, tally) = eligible_victims(
            &[pinned],
            Band::Critical,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        assert!(eligible.is_empty());
        assert_eq!(tally.node_pinned, 1);
    }

    #[test]
    fn the_disruption_floor_binds_and_counts_victims_already_chosen() {
        // 4 running, floor 2 -> at most 2 may go.
        let candidates = (1..=4)
            .map(|id| {
                let mut candidate = candidate(id, Band::Low, 100.0, 128.0);
                candidate.task_config_id = 7;
                candidate.min_available = 2;
                candidate.task_running = 4;
                candidate
            })
            .collect::<Vec<_>>();

        let (eligible, _) = eligible_victims(
            &candidates,
            Band::High,
            DEFAULT_VICTIM_EXEMPT_AGE,
            &HashMap::new(),
        );
        assert_eq!(eligible.len(), 4, "with none taken yet, all are candidates");

        let taken = HashMap::from([(7i64, 2u32)]);
        let (eligible, tally) =
            eligible_victims(&candidates, Band::High, DEFAULT_VICTIM_EXEMPT_AGE, &taken);
        assert!(
            eligible.is_empty(),
            "two already chosen exhausts the budget for the whole plan"
        );
        assert_eq!(tally.floor_bound, 4);
        assert!(tally.describe().contains("disruption.minAvailable"));
    }

    #[test]
    fn cheaper_plans_sort_first() {
        let one_low = vec![candidate(1, Band::Low, 100.0, 128.0)];
        let two_background = vec![
            candidate(2, Band::Background, 100.0, 128.0),
            candidate(3, Band::Background, 100.0, 128.0),
        ];
        assert!(
            preemption_cost(&one_low) < preemption_cost(&two_background),
            "fewer victims wins before band weight"
        );
        let one_normal = vec![candidate(4, Band::Normal, 100.0, 128.0)];
        assert!(
            preemption_cost(&one_low) < preemption_cost(&one_normal),
            "at equal victim counts, take the less important work"
        );
    }

    #[test]
    fn settings_parse_and_reject_typos() -> R<()> {
        let settings = json!({
            "preemption": {
                "mode": "observe",
                "disabledPools": ["gpu"],
                "victimExemptAge": "5m",
            }
        });
        let parsed = PreemptionSettings::from_scheduler_settings(Some(&settings))?;
        assert_eq!(parsed.mode, PreemptionMode::Observe);
        assert!(!parsed.enabled_for_pool("gpu"));
        assert!(parsed.enabled_for_pool("default"));
        assert_eq!(
            parsed.victim_exempt_age,
            std::time::Duration::from_secs(300)
        );

        let typo = json!({ "preemption": { "mode": "observ" } });
        assert!(
            PreemptionSettings::from_scheduler_settings(Some(&typo)).is_err(),
            "a mistyped mode must not silently become enforce"
        );

        // Absent settings are the documented default: enforce, no exemptions.
        let default = PreemptionSettings::from_scheduler_settings(None)?;
        assert_eq!(default.mode, PreemptionMode::Enforce);
        assert!(default.enabled_for_pool("anything"));
        Ok(())
    }

    #[test]
    fn off_disables_every_pool() -> R<()> {
        let settings = json!({ "preemption": { "mode": "off" } });
        let parsed = PreemptionSettings::from_scheduler_settings(Some(&settings))?;
        assert!(!parsed.enabled_for_pool("default"));
        Ok(())
    }
}
