#![crate_type = "cdylib"]

use legion::manager::scheduler::{SchedulingContext, SchedulingPolicy};
use legion::structs::Node;

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

#[no_mangle]
pub extern "C" fn get() -> Box<dyn SchedulingPolicy> {
    Box::new(BinPackPolicy {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion::structs::{Allocation, TaskConfig};
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
    fn packs_on_most_loaded_node() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> =
            HashMap::from([(String::from("node-a"), vec![alloc("node-a", 1)])]);
        let planned = HashMap::from([(String::from("node-b"), 2_usize)]);

        let ctx = SchedulingContext::new(nodes.iter().collect(), &task("web"), &allocations, &planned);
        let policy = BinPackPolicy {};

        let choice = policy.select_node(ctx).expect("node expected");
        assert_eq!(choice.id, "node-b");
    }
}
