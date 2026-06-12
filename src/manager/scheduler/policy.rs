// Scoring helpers take the full placement context by parameter on purpose.
#![allow(clippy::too_many_arguments)]

use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::util::plugin_root_dir;
use std::collections::HashMap;

use crate::constraints::AffinitySet;
use crate::prelude::*;
use crate::structs::config::DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT;
use crate::structs::{Allocation, Node, TaskConfig};
use libloading::{Library, Symbol};
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde_json::Value;

#[derive(Debug, Clone, Copy)]
pub struct SchedulerWeights {
    pub affinity: f64,
    pub spread: f64,
    pub bin_pack: f64,
    pub automatic_anti_affinity: f64,
}

impl SchedulerWeights {
    pub fn for_policy(policy: &str) -> Self {
        match policy {
            "spread" => SchedulerWeights {
                affinity: 1.0,
                spread: 1.0,
                bin_pack: 0.0,
                automatic_anti_affinity: DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT,
            },
            "bin_pack" => SchedulerWeights {
                affinity: 1.0,
                spread: 0.0,
                bin_pack: 1.0,
                automatic_anti_affinity: DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT,
            },
            _ => SchedulerWeights {
                affinity: 1.0,
                spread: 0.0,
                bin_pack: 0.0,
                automatic_anti_affinity: DEF_SCHEDULER_AUTO_ANTI_AFFINITY_WEIGHT,
            },
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SchedulingContext<'a> {
    pub eligible_nodes: Vec<&'a Node>,
    pub task: &'a TaskConfig,
    pub task_id: i64,
    pub affinity_set: &'a AffinitySet,
    pub job_config_id: &'a str,
    pub allocations: &'a HashMap<String, Vec<Allocation>>,
    pub task_name_to_id: &'a HashMap<String, i64>,
    pub task_id_to_group: &'a HashMap<i64, String>,
    pub planned_task_allocations: &'a HashMap<String, Vec<i64>>,
    pub planned: &'a HashMap<String, usize>,
    pub weights: SchedulerWeights,
}

#[allow(dead_code)]
impl<'a> SchedulingContext<'a> {
    pub fn new(
        eligible_nodes: Vec<&'a Node>,
        task: &'a TaskConfig,
        task_id: i64,
        affinity_set: &'a AffinitySet,
        job_config_id: &'a str,
        allocations: &'a HashMap<String, Vec<Allocation>>,
        task_name_to_id: &'a HashMap<String, i64>,
        task_id_to_group: &'a HashMap<i64, String>,
        planned_task_allocations: &'a HashMap<String, Vec<i64>>,
        planned: &'a HashMap<String, usize>,
        weights: SchedulerWeights,
    ) -> Self {
        SchedulingContext {
            eligible_nodes,
            task,
            task_id,
            affinity_set,
            job_config_id,
            allocations,
            task_name_to_id,
            task_id_to_group,
            planned_task_allocations,
            planned,
            weights,
        }
    }

    pub fn total_assigned(&self, node_id: &str) -> usize {
        self.allocations
            .get(node_id)
            .map(|allocs| allocs.len())
            .unwrap_or(0)
            + self.planned.get(node_id).copied().unwrap_or(0)
    }

    fn has_task_on_node(&self, node_id: &str, task_id: i64) -> bool {
        if self.allocations.get(node_id).is_some_and(|allocs| {
            allocs.iter().any(|allocation| {
                allocation.job_config_id == self.job_config_id
                    && allocation.task_config_id == task_id
            })
        }) {
            return true;
        }

        self.planned_task_allocations
            .get(node_id)
            .is_some_and(|task_ids| task_ids.contains(&task_id))
    }

    fn allocations_on_node_for_task(&self, node_id: &str, task_id: i64) -> usize {
        let existing = self
            .allocations
            .get(node_id)
            .map(|allocs| {
                allocs
                    .iter()
                    .filter(|allocation| {
                        allocation.job_config_id == self.job_config_id
                            && allocation.task_config_id == task_id
                    })
                    .count()
            })
            .unwrap_or_default();
        let planned = self
            .planned_task_allocations
            .get(node_id)
            .map(|task_ids| {
                task_ids
                    .iter()
                    .filter(|planned_task_id| **planned_task_id == task_id)
                    .count()
            })
            .unwrap_or_default();

        existing + planned
    }

    fn group_allocations_on_node(&self, node_id: &str) -> usize {
        let Some(group_name) = self.task_id_to_group.get(&self.task_id) else {
            return 0;
        };

        let existing = self
            .allocations
            .get(node_id)
            .map(|allocs| {
                allocs
                    .iter()
                    .filter(|allocation| {
                        allocation.job_config_id == self.job_config_id
                            && self
                                .task_id_to_group
                                .get(&allocation.task_config_id)
                                .is_some_and(|group| group == group_name)
                    })
                    .count()
            })
            .unwrap_or_default();
        let planned = self
            .planned_task_allocations
            .get(node_id)
            .map(|task_ids| {
                task_ids
                    .iter()
                    .filter(|task_id| {
                        self.task_id_to_group
                            .get(task_id)
                            .is_some_and(|group| group == group_name)
                    })
                    .count()
            })
            .unwrap_or_default();

        existing + planned
    }
}

pub trait SchedulingPolicy {
    fn name(&self) -> &'static str;

    fn select_node<'a>(&self, ctx: SchedulingContext<'a>) -> Option<&'a Node>;
}

#[derive(Default)]
pub struct BinPackPolicy;

impl BinPackPolicy {
    fn pick_target<'a>(&self, ctx: &SchedulingContext<'a>) -> Option<&'a Node> {
        select_scored_node(ctx)
    }
}

impl SchedulingPolicy for BinPackPolicy {
    fn name(&self) -> &'static str {
        "bin_pack"
    }

    fn select_node<'a>(&self, ctx: SchedulingContext<'a>) -> Option<&'a Node> {
        self.pick_target(&ctx)
    }
}

#[derive(Default)]
pub struct SpreadPolicy;

impl SpreadPolicy {
    fn select_least_loaded<'a>(&self, ctx: &SchedulingContext<'a>) -> Option<&'a Node> {
        select_scored_node(ctx)
    }
}

impl SchedulingPolicy for SpreadPolicy {
    fn name(&self) -> &'static str {
        "spread"
    }

    fn select_node<'a>(&self, ctx: SchedulingContext<'a>) -> Option<&'a Node> {
        self.select_least_loaded(&ctx)
    }
}

fn select_scored_node<'a>(ctx: &SchedulingContext<'a>) -> Option<&'a Node> {
    if ctx.eligible_nodes.is_empty() {
        return None;
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

    let mut scored_nodes = Vec::with_capacity(ctx.eligible_nodes.len());
    let mut best_score: Option<f64> = None;

    for node in &ctx.eligible_nodes {
        let score = score_node_for_context(ctx, node, &loads, min_load, range);
        best_score = Some(best_score.map_or(score, |best| best.max(score)));
        scored_nodes.push((score, *node));
    }

    let best_score = best_score.unwrap_or_default();
    let mut top_nodes: Vec<&'a Node> = scored_nodes
        .into_iter()
        .filter_map(|(score, node)| {
            if (score - best_score).abs() <= 1e-9 {
                Some(node)
            } else {
                None
            }
        })
        .collect();

    top_nodes.shuffle(&mut thread_rng());
    top_nodes.into_iter().next()
}

pub(crate) fn score_node_for_context(
    ctx: &SchedulingContext<'_>,
    node: &Node,
    loads: &HashMap<&str, usize>,
    min_load: f64,
    range: f64,
) -> f64 {
    let load = loads.get(node.id.as_str()).copied().unwrap_or_default() as f64;
    let (spread_score, bin_pack_score) = if range > 0.0 {
        (
            (max_load_delta(range, load, min_load)),
            (load - min_load) / range,
        )
    } else {
        (0.0, 0.0)
    };

    let affinity_score = affinity_score(ctx.task, ctx.affinity_set, node, ctx);
    let anti_affinity_penalty = automatic_anti_affinity_penalty(ctx, node);

    ctx.weights.affinity * affinity_score
        + ctx.weights.spread * spread_score
        + ctx.weights.bin_pack * bin_pack_score
        - ctx.weights.automatic_anti_affinity * anti_affinity_penalty
}

fn automatic_anti_affinity_penalty(ctx: &SchedulingContext<'_>, node: &Node) -> f64 {
    let same_task = ctx.allocations_on_node_for_task(&node.id, ctx.task_id);
    let same_group = ctx.group_allocations_on_node(&node.id);
    same_task.max(same_group) as f64
}

fn max_load_delta(range: f64, load: f64, min_load: f64) -> f64 {
    let max_load = min_load + range;
    (max_load - load) / range
}

fn affinity_score(
    task: &TaskConfig,
    affinity_set: &AffinitySet,
    node: &Node,
    ctx: &SchedulingContext<'_>,
) -> f64 {
    affinity_set.score(node) + task_colocation_affinity_score(task, node, ctx)
}

fn task_colocation_affinity_score(
    task: &TaskConfig,
    node: &Node,
    ctx: &SchedulingContext<'_>,
) -> f64 {
    let preferences = task_colocation_preferences(&task.affinities);
    if preferences.is_empty() {
        return 0.0;
    }

    let mut matched = 0.0;
    let mut total = 0.0;
    for (task_name, multiplier) in preferences {
        let weight = multiplier.abs();
        total += weight;

        let Some(task_id) = ctx.task_name_to_id.get(task_name) else {
            continue;
        };

        if ctx.has_task_on_node(&node.id, *task_id) {
            matched += multiplier;
        }
    }

    if total == 0.0 {
        0.0
    } else {
        matched / total
    }
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

#[instrument(err, skip_all)]
pub fn load_policy(name: &str) -> R<(Option<Library>, Box<dyn SchedulingPolicy>)> {
    match name {
        "bin_pack" => Ok((None, Box::new(BinPackPolicy))),
        "spread" => Ok((None, Box::new(SpreadPolicy))),
        other => {
            let plugin_root = plugin_root_dir();
            let paths = plugin_paths("scheduling", other, &plugin_root);
            validate_manifest("scheduling", other, &paths.root)
                .with_context(|| format!("invalid scheduling plugin manifest for {}", other))?;
            let path = paths.dylib.to_string_lossy().into_owned();
            unsafe {
                let lib = Library::new(&path)?;
                let policy = {
                    let func: Symbol<unsafe extern "C" fn() -> Box<dyn SchedulingPolicy>> =
                        lib.get(b"get")?;
                    func()
                };
                Ok((Some(lib), policy))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::OnceLock;

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn task(name: &str) -> TaskConfig {
        TaskConfig::new(name, "noop")
    }

    fn empty_task_name_to_id() -> &'static HashMap<String, i64> {
        static EMPTY: OnceLock<HashMap<String, i64>> = OnceLock::new();
        EMPTY.get_or_init(HashMap::new)
    }

    fn empty_task_id_to_group() -> &'static HashMap<i64, String> {
        static EMPTY: OnceLock<HashMap<i64, String>> = OnceLock::new();
        EMPTY.get_or_init(HashMap::new)
    }

    fn empty_planned_task_allocations() -> &'static HashMap<String, Vec<i64>> {
        static EMPTY: OnceLock<HashMap<String, Vec<i64>>> = OnceLock::new();
        EMPTY.get_or_init(HashMap::new)
    }

    fn alloc(node_id: &str, id: i64, job_config_id: &str) -> Allocation {
        alloc_for_task(node_id, id, job_config_id, 1)
    }

    fn alloc_for_task(
        node_id: &str,
        id: i64,
        job_config_id: &str,
        task_config_id: i64,
    ) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources: json!({}),
            node_id: node_id.into(),
            job_config_id: job_config_id.into(),
            task_config_id,
            job_id: "job".into(),
            instance_id: None,
        }
    }

    #[test]
    fn bin_pack_prefers_more_loaded_node() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::from([(
            String::from("node-a"),
            vec![alloc("node-a", 1, "other-job")],
        )]);
        let planned = HashMap::from([(String::from("node-b"), 2_usize)]);

        let task = task("web");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-b");
    }

    #[test]
    fn bin_pack_picks_any_node_on_tie() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned = HashMap::from([
            (String::from("node-a"), 1_usize),
            (String::from("node-b"), 1_usize),
        ]);

        let task = task("web");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert!(["node-a", "node-b"].contains(&choice.id.as_str()));
    }

    #[test]
    fn bin_pack_counts_planned_and_active_allocations() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::from([
            (
                String::from("node-a"),
                vec![
                    alloc("node-a", 1, "other-job"),
                    alloc("node-a", 2, "other-job"),
                ],
            ),
            (
                String::from("node-b"),
                vec![alloc("node-b", 3, "other-job")],
            ),
        ]);
        let planned = HashMap::from([(String::from("node-b"), 2_usize)]);

        let task = task("worker");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-b");
    }

    #[test]
    fn bin_pack_returns_none_without_nodes() {
        let nodes: Vec<Node> = vec![];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned = HashMap::from([(String::from("node-b"), 1_usize)]);

        let task = task("web");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx);
        assert!(choice.is_none());
    }

    #[test]
    fn spread_picks_any_node_on_equal_load() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::from([(
            String::from("node-a"),
            vec![alloc("node-a", 1, "other-job")],
        )]);
        let planned = HashMap::from([(String::from("node-b"), 1_usize)]);

        let task = task("api");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert!(["node-a", "node-b"].contains(&choice.id.as_str()));
    }

    #[test]
    fn spread_picks_any_node_on_tie() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned = HashMap::from([
            (String::from("node-a"), 1_usize),
            (String::from("node-b"), 1_usize),
        ]);

        let task = task("api");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert!(["node-a", "node-b"].contains(&choice.id.as_str()));
    }

    #[test]
    fn bin_pack_treats_near_equal_scores_as_tie() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::from([(
            String::from("node-a"),
            vec![alloc("node-a", 1, "other-job")],
        )]);
        let planned = HashMap::new();

        let task = task("web");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let policy = BinPackPolicy::default();

        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let choice = policy
                .select_node(SchedulingContext::new(
                    nodes.iter().collect(),
                    &task,
                    1,
                    &affinity_set,
                    "job",
                    &allocations,
                    empty_task_name_to_id(),
                    empty_task_id_to_group(),
                    empty_planned_task_allocations(),
                    &planned,
                    SchedulerWeights {
                        affinity: 1.0,
                        spread: 0.0,
                        bin_pack: 5e-10,
                        automatic_anti_affinity: 0.0,
                    },
                ))
                .expect("node expected");
            seen.insert(choice.id.clone());
        }

        assert!(
            seen.contains("node-a") && seen.contains("node-b"),
            "near-equal floating scores should not force deterministic clustering"
        );
    }

    #[test]
    fn spread_prefers_node_with_same_node_as_affinity() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::from([(
            String::from("node-a"),
            vec![alloc_for_task("node-a", 1, "job", 1)],
        )]);
        let planned = HashMap::from([(String::from("node-b"), 1_usize)]);
        let task_name_to_id = HashMap::from([(String::from("api"), 1_i64)]);
        let planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();

        let mut task = task("web");
        task.affinities = json!({ "same_node_as": "api" });
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            &task_name_to_id,
            empty_task_id_to_group(),
            &planned_task_allocations,
            &planned,
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-a");
    }

    #[test]
    fn spread_returns_none_without_nodes() {
        let nodes: Vec<Node> = vec![];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned: HashMap<String, usize> = HashMap::new();

        let task = task("api");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            1,
            &affinity_set,
            "job",
            &allocations,
            empty_task_name_to_id(),
            empty_task_id_to_group(),
            empty_planned_task_allocations(),
            &planned,
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx);
        assert!(choice.is_none());
    }
}
