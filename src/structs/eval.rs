use rand::{seq::SliceRandom, thread_rng};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::prelude::{bail, json, Context, R};
use crate::storage::store::Store;

use super::config::ManagerConfigFull;
use super::job::{JobConfig, TaskConfig};
use super::ledger::LedgerEntry;
use super::node::Node;
use super::{Eval, Submission};

impl Eval {
    pub fn handle(&self, store: &Store) -> R<String> {
        match self.kind.as_str() {
            "node_join" => self.handle_node_join(store),
            "node_drop" => self.handle_node_drop(store),
            "job_submit" => self.handle_job_submit(store),
            "job_update" => self.handle_job_update(store),
            "job_stop" => self.handle_job_stop(store),
            other => bail!("Unsupported eval kind: {other}"),
        }
    }

    fn handle_node_join(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            bail!("Eval node_join missing node name");
        }

        let node_id = node_id.to_owned();
        let last_heartbeat = Self::current_timestamp();
        let nodes = store.get_nodes()?;

        if nodes.iter().any(|node| node.id == node_id) {
            store.mark_node_ready(&node_id, &last_heartbeat)?;
        } else {
            let node = Node {
                id: node_id.clone(),
                status: "ready".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat,
                config_attrs: json!({}),
                fingerprint_attrs: json!({}),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            };

            store.insert_node(node.into())?;
        }

        Ok(format!("Node {node_id} joined"))
    }

    fn handle_node_drop(&self, store: &Store) -> R<String> {
        let node_id = self.node_name.trim();

        if node_id.is_empty() {
            bail!("Eval node_drop missing node name");
        }

        store.mark_node_lost(node_id)?;

        Ok(format!("Node {node_id} dropped"))
    }

    fn handle_job_submit(&self, store: &Store) -> R<String> {
        self.schedule_job_allocations(store)?;
        Ok(format!("Job {} submitted", self.job_id))
    }

    fn handle_job_update(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_update missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;
        self.schedule_job_allocations(store)?;

        Ok(format!("Job {} updated", self.job_id))
    }

    fn handle_job_stop(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_stop missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;

        Ok(format!("Job {} stopped", self.job_id))
    }

    fn schedule_job_allocations(&self, store: &Store) -> R<()> {
        if self.job_config_id.trim().is_empty() {
            bail!("Eval missing job config id");
        }

        let nodes: Vec<Node> = store
            .get_nodes()?
            .into_iter()
            .filter(|node| node.availability == "available")
            .collect();

        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let task_instances = store.get_task_instances_for_job_config(&self.job_config_id)?;
        if task_instances.is_empty() {
            return Ok(());
        }

        let mut rng = thread_rng();

        for (task_id, count) in task_instances {
            for _ in 0..count {
                let node = nodes
                    .choose(&mut rng)
                    .context("No nodes available to place allocations")?;
                // TODO: Add pluggable scheduling logic here
                store.place_alloc(&node.id, task_id)?;
            }
        }

        Ok(())
    }

    fn current_timestamp() -> String {
        OffsetDateTime::now_utc()
            .format(format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
            ))
            .unwrap_or_else(|_| "1970-01-01 00:00:00 +00:00:00".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::store::Store;

    fn setup_store() -> R<Store> {
        let store_config = ManagerConfigFull {
            store_path: "tests/eval".to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn find_node<'a>(ledger: &'a [LedgerEntry], node_id: &str) -> Option<&'a LedgerEntry> {
        ledger.iter().find(|entry| entry.node.id == node_id)
    }

    #[test]
    fn eval_chain_updates_ledger_state() -> R<()> {
        let store = setup_store()?;

        assert!(store.get_ledger()?.is_empty());

        let node_one_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-1".into(),
        };
        node_one_eval.handle(&store)?;

        let mut ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 1);
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert!(node_one_entry.allocations.is_empty());

        let node_two_eval = Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "node_join".into(),
            job_id: String::new(),
            job_config_id: String::new(),
            node_name: "node-2".into(),
        };
        node_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        assert_eq!(ledger.len(), 2);
        let node_two_entry = find_node(&ledger, "node-2").context("missing node-2")?;
        assert!(node_two_entry.allocations.is_empty());

        let job_one_name = "job-1".to_string();
        let job_one_task = TaskConfig {
            name: "task-one".into(),
            kind: "service".into(),
            count: 1,
            config: json!({ "message": "hi" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({}),
        };
        let job_one_config = JobConfig {
            name: job_one_name.clone(),
            tasks: vec![job_one_task.clone()],
            version: None,
        };
        let job_one_subs = vec![Submission {
            path: "job-one".into(),
            content: "job-one".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_one_config, &job_one_subs)?;
        let job_one_config_id = store.get_job_config(&job_one_name)?.id();

        let job_one_eval = Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id.clone(),
            node_name: String::new(),
        };
        job_one_eval.handle(&store)?;

        let job_one_config_id_v2 = store.get_job_config(&job_one_name)?.id();

        let job_update_eval = Eval {
            id: "5".into(),
            status: "pending".into(),
            kind: "job_update".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
        };
        job_update_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 1);
        assert_eq!(
            node_one_entry.allocations[0].job_config_id,
            job_one_config_id_v2
        );

        let job_two_name = "job-2".to_string();
        let job_two_task = TaskConfig {
            name: "task-two".into(),
            kind: "service".into(),
            count: 1,
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({}),
        };
        let job_two_config = JobConfig {
            name: job_two_name.clone(),
            tasks: vec![job_two_task.clone()],
            version: None,
        };
        let job_two_subs = vec![Submission {
            path: "job-two".into(),
            content: "job-two".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_two_config, &job_two_subs)?;
        let job_two_config_id = store.get_job_config(&job_two_name)?.id();

        let job_two_submit_eval = Eval {
            id: "6".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_two_name.clone(),
            job_config_id: job_two_config_id.clone(),
            node_name: String::new(),
        };
        job_two_submit_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 2);

        let stop_job_one_eval = Eval {
            id: "7".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
        };
        stop_job_one_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert_eq!(node_one_entry.allocations.len(), 1);
        assert!(node_one_entry
            .allocations
            .iter()
            .all(|alloc| alloc.job_config_id == job_two_config_id));

        let stop_job_two_eval = Eval {
            id: "8".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_two_name,
            job_config_id: job_two_config_id,
            node_name: String::new(),
        };
        stop_job_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let node_one_entry = find_node(&ledger, "node-1").context("missing node-1")?;
        assert!(node_one_entry.allocations.is_empty());

        Ok(())
    }
}
