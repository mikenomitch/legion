use crate::util::plugin_root_dir;
use std::collections::HashMap;

use crate::constraints::AffinitySet;
use crate::prelude::*;
use crate::structs::{Allocation, Node, TaskConfig};
use libloading::{Library, Symbol};

#[derive(Debug, Clone, Copy)]
pub struct SchedulerWeights {
    pub affinity: f64,
    pub spread: f64,
    pub bin_pack: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ResourceUsage {
    pub cpu: Option<f64>,
    pub memory: Option<f64>,
}

impl ResourceUsage {
    fn combined(self) -> Option<f64> {
        let mut total = 0.0;
        let mut count = 0.0;
        if let Some(cpu) = self.cpu {
            total += cpu;
            count += 1.0;
        }
        if let Some(memory) = self.memory {
            total += memory;
            count += 1.0;
        }
        if count > 0.0 {
            Some((total / count).clamp(0.0, 1.0))
        } else {
            None
        }
    }
}

impl SchedulerWeights {
    pub fn for_policy(policy: &str) -> Self {
        match policy {
            "spread" => SchedulerWeights {
                affinity: 1.0,
                spread: 1.0,
                bin_pack: 0.0,
            },
            "bin_pack" => SchedulerWeights {
                affinity: 1.0,
                spread: 0.0,
                bin_pack: 1.0,
            },
            _ => SchedulerWeights {
                affinity: 1.0,
                spread: 0.0,
                bin_pack: 0.0,
            },
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SchedulingContext<'a> {
    pub eligible_nodes: Vec<&'a Node>,
    pub task: &'a TaskConfig,
    pub affinity_set: &'a AffinitySet,
    pub job_config_id: &'a str,
    pub allocations: &'a HashMap<String, Vec<Allocation>>,
    pub planned: &'a HashMap<String, usize>,
    pub resource_usage: HashMap<String, ResourceUsage>,
    pub weights: SchedulerWeights,
}

#[allow(dead_code)]
impl<'a> SchedulingContext<'a> {
    pub fn new(
        eligible_nodes: Vec<&'a Node>,
        task: &'a TaskConfig,
        affinity_set: &'a AffinitySet,
        job_config_id: &'a str,
        allocations: &'a HashMap<String, Vec<Allocation>>,
        planned: &'a HashMap<String, usize>,
        resource_usage: HashMap<String, ResourceUsage>,
        weights: SchedulerWeights,
    ) -> Self {
        SchedulingContext {
            eligible_nodes,
            task,
            affinity_set,
            job_config_id,
            allocations,
            planned,
            resource_usage,
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

    ctx.eligible_nodes.iter().copied().max_by(|a, b| {
        let a_score = score_node(ctx, a, &loads, min_load, range);
        let b_score = score_node(ctx, b, &loads, min_load, range);

        match a_score
            .partial_cmp(&b_score)
            .unwrap_or(std::cmp::Ordering::Equal)
        {
            std::cmp::Ordering::Equal => b.id.cmp(&a.id),
            other => other,
        }
    })
}

fn score_node(
    ctx: &SchedulingContext<'_>,
    node: &Node,
    loads: &HashMap<&str, usize>,
    min_load: f64,
    range: f64,
) -> f64 {
    let load = loads.get(node.id.as_str()).copied().unwrap_or_default() as f64;
    let (mut spread_score, mut bin_pack_score) = if range > 0.0 {
        (
            (max_load_delta(range, load, min_load)),
            (load - min_load) / range,
        )
    } else {
        (0.0, 0.0)
    };
    if let Some(usage) = ctx
        .resource_usage
        .get(node.id.as_str())
        .copied()
        .and_then(ResourceUsage::combined)
    {
        let resource_spread = 1.0 - usage;
        let resource_bin_pack = usage;
        if range > 0.0 {
            spread_score = (spread_score + resource_spread) / 2.0;
            bin_pack_score = (bin_pack_score + resource_bin_pack) / 2.0;
        } else {
            spread_score = resource_spread;
            bin_pack_score = resource_bin_pack;
        }
    }

    let affinity_score = affinity_score(ctx.task, ctx.affinity_set, node);

    ctx.weights.affinity * affinity_score
        + ctx.weights.spread * spread_score
        + ctx.weights.bin_pack * bin_pack_score
}

fn max_load_delta(range: f64, load: f64, min_load: f64) -> f64 {
    let max_load = min_load + range;
    (max_load - load) / range
}

fn affinity_score(_task: &TaskConfig, affinity_set: &AffinitySet, node: &Node) -> f64 {
    affinity_set.score(node)
}

#[instrument(err, skip_all)]
pub fn load_policy(name: &str) -> R<(Option<Library>, Box<dyn SchedulingPolicy>)> {
    match name {
        "bin_pack" => Ok((None, Box::new(BinPackPolicy))),
        "spread" => Ok((None, Box::new(SpreadPolicy))),
        other => {
            let path = policy_plugin_name_to_path(other);
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

fn policy_plugin_name_to_path(name: &str) -> String {
    let mut path = plugin_root_dir();
    path.push("scheduling");
    path.push(name);
    path.push("target");
    path.push("release");
    path.push(format!(
        "{}{}{}",
        std::env::consts::DLL_PREFIX,
        name,
        std::env::consts::DLL_SUFFIX
    ));

    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

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

    fn alloc(node_id: &str, id: i64, job_config_id: &str) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources: json!({}),
            node_id: node_id.into(),
            job_config_id: job_config_id.into(),
            task_config_id: 1,
            job_id: "job".into(),
        }
    }

    fn usage(cpu: f64, memory: f64) -> ResourceUsage {
        ResourceUsage {
            cpu: Some(cpu),
            memory: Some(memory),
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
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-b");
    }

    #[test]
    fn bin_pack_prefers_lower_id_on_tie() {
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
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-a");
    }

    #[test]
    fn bin_pack_prefers_higher_resource_usage_on_tie() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned = HashMap::from([
            (String::from("node-a"), 1_usize),
            (String::from("node-b"), 1_usize),
        ]);
        let resource_usage = HashMap::from([
            (String::from("node-a"), usage(0.85, 0.8)),
            (String::from("node-b"), usage(0.2, 0.3)),
        ]);

        let task = task("web");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            &affinity_set,
            "job",
            &allocations,
            &planned,
            resource_usage,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-a");
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
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
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
        let planned: HashMap<String, usize> = HashMap::new();

        let task = task("web");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
            SchedulerWeights::for_policy("bin_pack"),
        );
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx);
        assert!(choice.is_none());
    }

    #[test]
    fn spread_prefers_least_loaded_node() {
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
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-a");
    }

    #[test]
    fn spread_prefers_lower_resource_usage_on_tie() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned = HashMap::from([
            (String::from("node-a"), 1_usize),
            (String::from("node-b"), 1_usize),
        ]);
        let resource_usage = HashMap::from([
            (String::from("node-a"), usage(0.9, 0.9)),
            (String::from("node-b"), usage(0.1, 0.2)),
        ]);

        let task = task("api");
        let affinity_set = AffinitySet::new(&task.affinities).expect("affinities");
        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task,
            &affinity_set,
            "job",
            &allocations,
            &planned,
            resource_usage,
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-b");
    }

    #[test]
    fn spread_prefers_lower_id_on_tie() {
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
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
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
            &affinity_set,
            "job",
            &allocations,
            &planned,
            HashMap::new(),
            SchedulerWeights::for_policy("spread"),
        );
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx);
        assert!(choice.is_none());
    }
}
