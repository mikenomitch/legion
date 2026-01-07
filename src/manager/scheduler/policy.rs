use std::collections::HashMap;
use std::path::PathBuf;

use libloading::{Library, Symbol};

use crate::prelude::*;
use crate::structs::{Allocation, Node, TaskConfig};

#[allow(dead_code)]
#[derive(Debug)]
pub struct SchedulingContext<'a> {
    pub eligible_nodes: Vec<&'a Node>,
    pub task: &'a TaskConfig,
    pub allocations: &'a HashMap<String, Vec<Allocation>>,
    pub planned: &'a HashMap<String, usize>,
}

#[allow(dead_code)]
impl<'a> SchedulingContext<'a> {
    pub fn new(
        eligible_nodes: Vec<&'a Node>,
        task: &'a TaskConfig,
        allocations: &'a HashMap<String, Vec<Allocation>>,
        planned: &'a HashMap<String, usize>,
    ) -> Self {
        SchedulingContext {
            eligible_nodes,
            task,
            allocations,
            planned,
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
        ctx.eligible_nodes.iter().copied().max_by(|a, b| {
            let a_load = ctx.total_assigned(&a.id);
            let b_load = ctx.total_assigned(&b.id);

            match a_load.cmp(&b_load) {
                std::cmp::Ordering::Equal => b.id.cmp(&a.id),
                other => other,
            }
        })
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
        ctx.eligible_nodes.iter().copied().min_by(|a, b| {
            let a_load = ctx.total_assigned(&a.id);
            let b_load = ctx.total_assigned(&b.id);

            match a_load.cmp(&b_load) {
                std::cmp::Ordering::Equal => a.id.cmp(&b.id),
                other => other,
            }
        })
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

#[instrument(err, skip_all)]
pub fn load_policy(name: &str) -> R<(Option<Library>, Box<dyn SchedulingPolicy>)> {
    match name {
        "bin_pack" => Ok((None, Box::new(BinPackPolicy::default()))),
        "spread" => Ok((None, Box::new(SpreadPolicy::default()))),
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
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("plugins");
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
        TaskConfig {
            name: name.into(),
            count: 1,
            kind: "noop".into(),
            config: json!({}),
            resources: json!({}),
            constraints: json!({}),
        }
    }

    fn alloc(node_id: &str, id: i64) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources: json!({}),
            node_id: node_id.into(),
            job_config_id: "job".into(),
            task_config_id: 1,
            job_id: "job".into(),
        }
    }

    #[test]
    fn bin_pack_prefers_more_loaded_node() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> =
            HashMap::from([(String::from("node-a"), vec![alloc("node-a", 1)])]);
        let planned = HashMap::from([(String::from("node-b"), 2_usize)]);

        let task = task("web");
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
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
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-a");
    }

    #[test]
    fn bin_pack_counts_planned_and_active_allocations() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::from([
            (String::from("node-a"), vec![alloc("node-a", 1), alloc("node-a", 2)]),
            (String::from("node-b"), vec![alloc("node-b", 3)]),
        ]);
        let planned = HashMap::from([(String::from("node-b"), 2_usize)]);

        let task = task("worker");
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
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
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
        let policy = BinPackPolicy::default();

        let choice = policy.select_node(ctx);
        assert!(choice.is_none());
    }

    #[test]
    fn spread_prefers_least_loaded_node() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> =
            HashMap::from([(String::from("node-a"), vec![alloc("node-a", 1)])]);
        let planned = HashMap::from([(String::from("node-b"), 1_usize)]);

        let task = task("api");
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-a");
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
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
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
        let ctx = SchedulingContext::new(nodes.iter().collect(), &task, &allocations, &planned);
        let policy = SpreadPolicy::default();

        let choice = policy.select_node(ctx);
        assert!(choice.is_none());
    }
}
