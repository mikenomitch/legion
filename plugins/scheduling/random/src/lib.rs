#![crate_type = "cdylib"]

use legion::manager::scheduler::{SchedulerWeights, SchedulingContext, SchedulingPolicy};
use legion::structs::Node;
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};
use std::cell::RefCell;

pub struct RandomPolicy {
    rng: RefCell<StdRng>,
}

impl RandomPolicy {
    pub fn new() -> Self {
        RandomPolicy {
            rng: RefCell::new(StdRng::from_entropy()),
        }
    }

    #[cfg(test)]
    fn seeded(seed: u64) -> Self {
        RandomPolicy {
            rng: RefCell::new(StdRng::seed_from_u64(seed)),
        }
    }
}

impl SchedulingPolicy for RandomPolicy {
    fn name(&self) -> &'static str {
        "random"
    }

    fn select_node<'a>(&self, ctx: SchedulingContext<'a>) -> Option<&'a Node> {
        let mut rng = self.rng.borrow_mut();
        ctx.eligible_nodes.choose(&mut *rng).copied()
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn SchedulingPolicy> {
    Box::new(RandomPolicy::new())
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
            runtime: "noop".into(),
            config: json!({}),
            resources: json!({}),
            constraints: json!({}),
            affinities: json!({}),
        }
    }

    #[test]
    fn picks_random_eligible_node() {
        let nodes = vec![node("node-a"), node("node-b")];
        let allocations: HashMap<String, Vec<Allocation>> = HashMap::new();
        let planned = HashMap::new();

        let ctx = SchedulingContext::new(
            nodes.iter().collect(),
            &task("job"),
            "job",
            &allocations,
            &planned,
            SchedulerWeights::for_policy("random"),
        );
        let policy = RandomPolicy::seeded(42);

        let first = policy.select_node(ctx).expect("node");
        assert!(first.id == "node-a" || first.id == "node-b");

        let ctx_again = SchedulingContext::new(
            nodes.iter().collect(),
            &task("job"),
            "job",
            &allocations,
            &planned,
            SchedulerWeights::for_policy("random"),
        );
        let second = policy.select_node(ctx_again).expect("node");
        assert_ne!(first.id, second.id, "seeded rng should vary selections");
    }
}
