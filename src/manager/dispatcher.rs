use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{worker_client::WorkerClient, Allocation, TaskAllocation, TaskConfig};
use std::collections::HashMap;
use tonic::transport::{Channel, Endpoint};

#[instrument(err, skip_all)]
pub fn dispatch_allocations(store: &Store, allocations: &[Allocation]) -> R<()> {
    if allocations.is_empty() {
        return Ok(());
    }

    let mut task_cache: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
    let mut node_cache: HashMap<String, crate::structs::Node> = HashMap::new();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let mut clients: HashMap<String, WorkerClient<Channel>> = HashMap::new();
        for allocation in allocations {
            let tasks = task_cache
                .entry(allocation.job_config_id.clone())
                .or_insert_with(|| HashMap::new());
            if tasks.is_empty() {
                let task_configs =
                    store.get_task_configs_for_job_config(&allocation.job_config_id)?;
                for (id, config) in task_configs {
                    tasks.insert(id, config);
                }
            }
            let task = tasks
                .get(&allocation.task_config_id)
                .with_context(|| format!("Missing task config {}", allocation.task_config_id))?;

            if !node_cache.contains_key(&allocation.node_id) {
                let node = store.get_node(&allocation.node_id)?;
                node_cache.insert(allocation.node_id.clone(), node);
            }
            let node = node_cache
                .get(&allocation.node_id)
                .context("Missing node in cache")?;
            let Some(worker_address) = worker_address_for_node(node) else {
                warn!(
                    node_id = %allocation.node_id,
                    "Skipping allocation dispatch; worker address missing"
                );
                continue;
            };

            let task_allocation = build_task_allocation(task, allocation)?;
            let request = crate::structs::PostWorkRequest {
                allocation: Some(task_allocation),
            };

            let client = if let Some(client) = clients.get_mut(&worker_address) {
                client
            } else {
                let endpoint = Endpoint::from_shared(worker_address.clone())?;
                let channel = endpoint.connect_lazy();
                clients.insert(worker_address.clone(), WorkerClient::new(channel));
                clients
                    .get_mut(&worker_address)
                    .context("Failed to build worker client")?
            };

            if let Err(err) = client.post_work(request).await {
                warn!(
                    allocation_id = allocation.id,
                    node_id = %allocation.node_id,
                    error = ?err,
                    "Failed to dispatch allocation to worker"
                );
                continue;
            }
            info!(
                allocation_id = allocation.id,
                node_id = %allocation.node_id,
                "Dispatched allocation to worker"
            );
        }
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn worker_address_for_node(node: &crate::structs::Node) -> Option<String> {
    let attrs = node.config_attrs.as_object()?;
    attrs
        .get("workerAddress")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

pub fn build_task_allocation(task: &TaskConfig, allocation: &Allocation) -> R<TaskAllocation> {
    let task_config = serde_json::to_string(&task.config)?;
    Ok(TaskAllocation {
        allocation_id: allocation.id,
        job_id: allocation.job_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        node_id: allocation.node_id.clone(),
        task_config_id: allocation.task_config_id,
        task_name: task.name.clone(),
        task_kind: task.kind.clone(),
        task_config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn worker_address_from_node_config() {
        let node = crate::structs::Node {
            id: "node-1".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "now".into(),
            config_attrs: json!({"workerAddress": "http://127.0.0.1:4848"}),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };

        assert_eq!(
            worker_address_for_node(&node),
            Some("http://127.0.0.1:4848".to_string())
        );
    }
}
