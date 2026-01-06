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

#[instrument(err, skip_all)]
pub fn load_policy(name: &str) -> R<(Library, Box<dyn SchedulingPolicy>)> {
    let path = policy_plugin_name_to_path(name);
    unsafe {
        let lib = Library::new(&path)?;
        let policy = {
            let func: Symbol<unsafe extern "C" fn() -> Box<dyn SchedulingPolicy>> =
                lib.get(b"get")?;
            func()
        };
        Ok((lib, policy))
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
