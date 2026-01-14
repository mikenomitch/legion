use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing::info;

use crate::manager::scheduler::{PlanMode, PlanOutcome, Scheduler};
use crate::manager::dispatcher::dispatch_allocations;
#[cfg(test)]
use crate::prelude::Context;
use crate::prelude::{bail, json, warn, R};
use crate::storage::store::Store;

use super::node::Node;
use super::Eval;

#[cfg(test)]
use super::config::ManagerConfigFull;
#[cfg(test)]
use super::job::{JobConfig, TaskConfig};
#[cfg(test)]
use super::ledger::LedgerEntry;
#[cfg(test)]
use super::Submission;

impl Eval {
    pub fn handle(&self, store: &Store) -> R<String> {
        match self.kind.as_str() {
            "node_join" => self.handle_node_join(store),
            "node_drop" => self.handle_node_drop(store),
            "job_submit" => self.handle_job_submit(store),
            "job_update" => self.handle_job_update(store),
            "job_stop" => self.handle_job_stop(store),
            "job_reschedule" => self.handle_job_reschedule(store),
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
                fingerprint_attrs: Self::default_fingerprint_attrs(),
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
        store.reschedule_allocations_for_lost_node(node_id)?;

        Ok(format!("Node {node_id} dropped"))
    }

    fn handle_job_submit(&self, store: &Store) -> R<String> {
        let previous_config_id = store.previous_job_config_id(&self.job_config_id)?;
        if let Some(prev_id) = previous_config_id.as_ref() {
            if store.job_configs_schedule_equivalent(prev_id, &self.job_config_id)? {
                store.migrate_allocations_to_job_config(prev_id, &self.job_config_id)?;
                return Ok(format!("Job {} submitted with 0 placements", self.job_id));
            }
        }

        let outcome = self.schedule_job_allocations(store, PlanMode::Fresh)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job submit allocation deltas");
        }

        if let Some(prev_id) = previous_config_id {
            store.remove_allocs_for_job_config(&prev_id)?;
        }

        Ok(format!(
            "Job {} submitted with {} placements",
            self.job_id,
            outcome.allocations.len()
        ))
    }

    fn handle_job_update(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_update missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;
        let outcome = self.schedule_job_allocations(store, PlanMode::Fresh)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job update allocation deltas");
        }

        Ok(format!(
            "Job {} updated with {} placements",
            self.job_id,
            outcome.allocations.len()
        ))
    }

    fn handle_job_stop(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() {
            bail!("Eval job_stop missing job id");
        }

        store.remove_allocs_for_job(&self.job_id)?;

        Ok(format!("Job {} stopped", self.job_id))
    }

    fn handle_job_reschedule(&self, store: &Store) -> R<String> {
        if self.job_id.trim().is_empty() || self.job_config_id.trim().is_empty() {
            bail!("Eval job_reschedule missing identifiers");
        }

        let outcome = self.schedule_job_allocations(store, PlanMode::Reschedule)?;
        if !outcome.deltas.is_empty() {
            info!(?outcome.deltas, "Job reschedule allocation deltas");
        }

        Ok(format!(
            "Job {} rescheduled with {} placements",
            self.job_id,
            outcome.allocations.len()
        ))
    }

    fn schedule_job_allocations(&self, store: &Store, mode: PlanMode) -> R<PlanOutcome> {
        if self.job_config_id.trim().is_empty() {
            bail!("Eval missing job config id");
        }

        let scheduler = Scheduler::new(store)?;
        let outcome = scheduler.plan_and_apply(&self.job_config_id, mode)?;
        if let Err(err) = dispatch_allocations(store, &outcome.allocations) {
            warn!(error = %err, "Failed to dispatch allocations during eval");
        }
        Ok(outcome)
    }

    fn current_timestamp() -> String {
        OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
    }

    fn default_fingerprint_attrs() -> serde_json::Value {
        json!({
            "memory_total": "8388608",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::store::Store;
    use crate::structs::NodeMsg;
    use tempfile::Builder;

    fn setup_store() -> R<Store> {
        let store_dir = Builder::new()
            .prefix("legion-eval-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    fn find_node<'a>(ledger: &'a [LedgerEntry], node_id: &str) -> Option<&'a LedgerEntry> {
        ledger.iter().find(|entry| entry.node.id == node_id)
    }

    fn make_node(id: &str, zone: &str) -> Node {
        Node {
            id: id.to_string(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            config_attrs: json!({ "node_id": id, "zone": zone }),
            fingerprint_attrs: json!({
                "memory_total": "8388608",
                "cpus": {
                    "cpu-1": { "frequency": "2400" },
                    "cpu-2": { "frequency": "2400" },
                }
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
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
            priority: 50,
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
            priority: 50,
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
            affinities: json!({}),
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
            priority: 50,
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
            priority: 50,
        };
        job_update_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let job_one_allocations: Vec<_> = ledger
            .iter()
            .flat_map(|entry| entry.allocations.iter())
            .filter(|alloc| alloc.job_config_id == job_one_config_id_v2)
            .collect();
        assert_eq!(job_one_allocations.len(), 1);

        let job_two_name = "job-2".to_string();
        let job_two_task = TaskConfig {
            name: "task-two".into(),
            kind: "service".into(),
            count: 1,
            config: json!({ "message": "hola" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({}),
            affinities: json!({}),
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
            priority: 50,
        };
        job_two_submit_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let total_allocations: usize = ledger.iter().map(|entry| entry.allocations.len()).sum();
        assert_eq!(total_allocations, 2);

        let stop_job_one_eval = Eval {
            id: "7".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_one_name.clone(),
            job_config_id: job_one_config_id_v2.clone(),
            node_name: String::new(),
            priority: 50,
        };
        stop_job_one_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let job_two_allocations: Vec<_> = ledger
            .iter()
            .flat_map(|entry| entry.allocations.iter())
            .filter(|alloc| alloc.job_config_id == job_two_config_id)
            .collect();
        assert_eq!(job_two_allocations.len(), 1);

        let stop_job_two_eval = Eval {
            id: "8".into(),
            status: "pending".into(),
            kind: "job_stop".into(),
            job_id: job_two_name,
            job_config_id: job_two_config_id,
            node_name: String::new(),
            priority: 50,
        };
        stop_job_two_eval.handle(&store)?;

        ledger = store.get_ledger()?;
        let total_allocations: usize = ledger.iter().map(|entry| entry.allocations.len()).sum();
        assert_eq!(total_allocations, 0);

        Ok(())
    }

    #[test]
    fn job_submit_update_reconciles_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(NodeMsg::from(make_node("node-a", "east")))?;
        store.insert_node(NodeMsg::from(make_node("node-b", "west")))?;

        let job_name = "update-job".to_string();
        let base_task = TaskConfig {
            name: "web".into(),
            kind: "service".into(),
            count: 1,
            config: json!({ "message": "v1" }),
            resources: json!({ "memory": 128 }),
            constraints: json!({ "node_id": "node-a" }),
            affinities: json!({}),
        };

        let job_config = JobConfig {
            name: job_name.clone(),
            tasks: vec![base_task.clone()],
            version: None,
        };
        let job_subs = vec![Submission {
            path: "job.jsonnet".into(),
            content: "job".into(),
            is_primary: true,
        }];
        store.handle_job_submit(&job_config, &job_subs)?;
        let job_config_id = store.get_job_config(&job_name)?.id();

        let initial_eval = Eval {
            id: "1".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: job_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        initial_eval.handle(&store)?;

        let initial_alloc = store
            .allocs_for_job(&job_name)?
            .into_iter()
            .next()
            .context("missing initial allocation")?;
        assert_eq!(initial_alloc.node_id, "node-a");

        let unchanged_task = TaskConfig {
            config: json!({ "message": "v2" }),
            ..base_task.clone()
        };
        let unchanged_config = JobConfig {
            name: job_name.clone(),
            tasks: vec![unchanged_task.clone()],
            version: None,
        };
        store.handle_job_submit(&unchanged_config, &job_subs)?;
        let unchanged_config_id = store.get_job_config(&job_name)?.id();
        let unchanged_eval = Eval {
            id: "2".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: unchanged_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        unchanged_eval.handle(&store)?;

        let unchanged_allocations = store.allocs_for_job(&job_name)?;
        assert_eq!(unchanged_allocations.len(), 1);
        let unchanged_alloc = &unchanged_allocations[0];
        assert_eq!(unchanged_alloc.id, initial_alloc.id);
        assert_eq!(unchanged_alloc.node_id, "node-a");
        assert_eq!(unchanged_alloc.job_config_id, unchanged_config_id);

        let moved_task = TaskConfig {
            constraints: json!({ "node_id": "node-b" }),
            ..unchanged_task.clone()
        };
        let moved_config = JobConfig {
            name: job_name.clone(),
            tasks: vec![moved_task.clone()],
            version: None,
        };
        store.handle_job_submit(&moved_config, &job_subs)?;
        let moved_config_id = store.get_job_config(&job_name)?.id();
        let moved_eval = Eval {
            id: "3".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name.clone(),
            job_config_id: moved_config_id.clone(),
            node_name: String::new(),
            priority: 50,
        };
        moved_eval.handle(&store)?;

        let moved_allocations = store.allocs_for_job(&job_name)?;
        assert_eq!(moved_allocations.len(), 1);
        let moved_alloc = &moved_allocations[0];
        assert_eq!(moved_alloc.node_id, "node-b");
        assert_ne!(moved_alloc.id, initial_alloc.id);
        assert_eq!(moved_alloc.job_config_id, moved_config_id);

        let removed_task = TaskConfig {
            count: 0,
            ..moved_task
        };
        let removed_config = JobConfig {
            name: job_name.clone(),
            tasks: vec![removed_task],
            version: None,
        };
        store.handle_job_submit(&removed_config, &job_subs)?;
        let removed_config_id = store.get_job_config(&job_name)?.id();
        let removed_eval = Eval {
            id: "4".into(),
            status: "pending".into(),
            kind: "job_submit".into(),
            job_id: job_name,
            job_config_id: removed_config_id,
            node_name: String::new(),
            priority: 50,
        };
        removed_eval.handle(&store)?;

        assert!(store.allocs_for_job("update-job")?.is_empty());

        Ok(())
    }
}
