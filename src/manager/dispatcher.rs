use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{
    worker_client::WorkerClient, Allocation, Instance, TaskAllocation, TaskConfig,
};
use std::collections::HashMap;
use tokio::runtime::RuntimeFlavor;
use tonic::transport::{Channel, Endpoint};

#[instrument(err, skip_all)]
pub fn dispatch_allocations(store: &Store, allocations: &[Allocation]) -> R<()> {
    if allocations.is_empty() {
        return Ok(());
    }

    let mut task_cache: HashMap<String, HashMap<i64, TaskConfig>> = HashMap::new();
    let mut node_cache: HashMap<String, crate::structs::Node> = HashMap::new();

    run_dispatch_future(async {
        let mut clients: HashMap<String, WorkerClient<Channel>> = HashMap::new();
        for allocation in allocations {
            let tasks = task_cache
                .entry(allocation.job_config_id.clone())
                .or_default();
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

            let instance = match allocation.instance_id.as_deref() {
                Some(instance_id) => store.get_instance(instance_id)?,
                None => None,
            };
            let task_allocation = build_task_allocation(task, allocation, instance.as_ref())?;
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

fn run_dispatch_future<F>(future: F) -> R<()>
where
    F: std::future::Future<Output = R<()>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            if handle.runtime_flavor() == RuntimeFlavor::CurrentThread {
                bail!("dispatch_allocations cannot block inside a current-thread runtime");
            }
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Err(_) => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(future)
        }
    }
}

fn worker_address_for_node(node: &crate::structs::Node) -> Option<String> {
    let attrs = node.config_attrs.as_object()?;
    attrs
        .get("workerAddress")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

pub fn build_task_allocation(
    task: &TaskConfig,
    allocation: &Allocation,
    instance: Option<&Instance>,
) -> R<TaskAllocation> {
    let mut config = task.config.clone();
    inject_runtime_env(&mut config, allocation, instance);
    let task_config = serde_json::to_string(&config)?;
    let task_checks = serde_json::to_string(&task.checks)?;
    let task_on_unhealthy = serde_json::to_string(&task.on_unhealthy)?;
    let network = allocation
        .network
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?
        .unwrap_or_default();
    // Identity *specs* only (delivery/refresh instructions) — the tokens
    // themselves are fetched by the worker via IssueWorkloadIdentityToken and
    // never transit the desired-state payload.
    let task_identities = serde_json::to_string(&task.identities)?;
    // Worker self-fence window (the worker half of the at-most-one fencing
    // guarantee, see docs/stateful-workloads.md "Node loss and fencing").
    // Stateful tasks always carry a non-zero window — the 90s default applies
    // even without an `onLost` block. Stateless tasks carry 0 (no fencing;
    // the manager replaces them eagerly on node loss).
    let self_terminate_after_seconds = match task.stateful.as_ref() {
        Some(stateful) => stateful.resolved_on_lost()?.self_terminate_after.as_secs(),
        None => 0,
    };
    // Empty string (not "[]") when no artifacts: jobs without artifacts stay
    // byte-identical on the wire.
    let task_artifacts = if task.artifacts.is_empty() {
        String::new()
    } else {
        serde_json::to_string(&task.artifacts)?
    };
    // Declared upstreams only — the loopback ports they were assigned live on
    // `network.upstreams`. Empty string (not "{}") when the task declares no
    // upstreams, so jobs that do not use connect stay byte-identical.
    let task_connect = match task.connect.as_ref().filter(|connect| !connect.is_empty()) {
        Some(connect) => serde_json::to_string(connect)?,
        None => String::new(),
    };
    Ok(TaskAllocation {
        allocation_id: allocation.id,
        job_id: allocation.job_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        node_id: allocation.node_id.clone(),
        task_config_id: allocation.task_config_id,
        task_name: task.name.clone(),
        task_kind: task.kind.clone(),
        task_config,
        task_checks,
        task_on_unhealthy,
        network,
        task_identities,
        self_terminate_after_seconds,
        task_artifacts,
        task_connect,
    })
}

/// Inject Legion runtime environment variables into the task's kind config
/// `env` map (the map exec/container plugins already read):
///
/// - `LEGION_ALLOC_ID` — always; the ephemeral placement id (churns each move).
/// - `LEGION_INSTANCE_ID` / `LEGION_INSTANCE_MODE` — stateful tasks only; the
///   stable instance id that survives reschedules and its mode (slot/keyed).
/// - `LEGION_INSTANCE_SLOT` — slot-mode only; ordinal `0..count-1`.
///
/// Legion-injected values win over user-declared env keys with the same name:
/// they are authoritative runtime facts, and letting a jobspec shadow them
/// would let a workload misreport its own identity (mirrors Nomad, whose
/// runtime variables always take precedence). Documented in
/// `docs/stateful-workloads.md`.
fn inject_runtime_env(config: &mut Value, allocation: &Allocation, instance: Option<&Instance>) {
    let Some(config) = config.as_object_mut() else {
        // Non-object kind configs cannot carry an env map; plugins that accept
        // them do not read env either, so there is nothing to inject into.
        return;
    };

    let env = config
        .entry("env")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(env) = env.as_object_mut() else {
        warn!(
            allocation_id = allocation.id,
            "Task config env is not an object; skipping runtime env injection"
        );
        return;
    };

    env.insert(
        "LEGION_ALLOC_ID".to_string(),
        Value::String(allocation.id.to_string()),
    );
    if let Some(instance) = instance {
        env.insert(
            "LEGION_INSTANCE_ID".to_string(),
            Value::String(instance.instance_id.clone()),
        );
        env.insert(
            "LEGION_INSTANCE_MODE".to_string(),
            Value::String(instance.mode.to_string()),
        );
        if let Some(slot) = instance.slot_index {
            env.insert(
                "LEGION_INSTANCE_SLOT".to_string(),
                Value::String(slot.to_string()),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{InstanceMode, InstanceState};
    use serde_json::json;

    fn allocation(instance_id: Option<&str>) -> Allocation {
        Allocation {
            id: 42,
            status: "placed".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: instance_id.map(String::from),
            network: None,
        }
    }

    fn slot_instance() -> Instance {
        Instance {
            instance_id: "job-default-1".into(),
            job_id: "job".into(),
            group: "default".into(),
            task_name: "db".into(),
            mode: InstanceMode::Slot,
            state: InstanceState::Active,
            slot_index: Some(1),
            current_alloc_id: Some(42),
            storage_handle: Some("local-dir/abc".into()),
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    fn env_from(task_allocation: &TaskAllocation) -> serde_json::Map<String, Value> {
        let config: Value =
            serde_json::from_str(&task_allocation.task_config).expect("config parses");
        config["env"].as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn injects_alloc_id_for_stateless_tasks_without_instance_vars() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None)?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert!(!env.contains_key("LEGION_INSTANCE_ID"));
        assert!(!env.contains_key("LEGION_INSTANCE_MODE"));
        assert!(!env.contains_key("LEGION_INSTANCE_SLOT"));
        Ok(())
    }

    #[test]
    fn injects_instance_env_for_slot_instances() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
        )?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("job-default-1"));
        assert_eq!(env["LEGION_INSTANCE_MODE"], json!("slot"));
        assert_eq!(env["LEGION_INSTANCE_SLOT"], json!("1"));
        Ok(())
    }

    #[test]
    fn keyed_instances_omit_slot_env_var() -> R<()> {
        let instance = Instance {
            mode: InstanceMode::Keyed,
            slot_index: None,
            instance_id: "sandbox-7f3a".into(),
            ..slot_instance()
        };
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("sandbox", "exec")
        };
        let built =
            build_task_allocation(&task, &allocation(Some("sandbox-7f3a")), Some(&instance))?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("sandbox-7f3a"));
        assert_eq!(env["LEGION_INSTANCE_MODE"], json!("keyed"));
        assert!(!env.contains_key("LEGION_INSTANCE_SLOT"));
        Ok(())
    }

    #[test]
    fn legion_env_wins_over_user_declared_keys() -> R<()> {
        // Legion runtime vars are authoritative facts; a jobspec must not be
        // able to make a workload misreport its own identity.
        let task = TaskConfig {
            config: json!({
                "command": ["true"],
                "env": {
                    "LEGION_INSTANCE_ID": "forged",
                    "LEGION_ALLOC_ID": "forged",
                    "USER_VAR": "kept"
                }
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
        )?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("job-default-1"));
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert_eq!(env["USER_VAR"], json!("kept"));
        Ok(())
    }

    #[test]
    fn stateless_tasks_carry_no_self_fence_window() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None)?;
        assert_eq!(built.self_terminate_after_seconds, 0);
        Ok(())
    }

    #[test]
    fn stateful_tasks_carry_the_default_self_fence_window_without_on_lost() -> R<()> {
        // Even without an onLost block, stateful tasks get the 90s default —
        // fencing is on by default.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: None,
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
        )?;
        assert_eq!(built.self_terminate_after_seconds, 90);
        Ok(())
    }

    #[test]
    fn stateful_tasks_carry_their_declared_self_fence_window() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: Some(crate::structs::StatefulOnLostConfig {
                    self_terminate_after: Some("30s".into()),
                    replace_after: Some("2m".into()),
                }),
                storage: None,
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
        )?;
        assert_eq!(built.self_terminate_after_seconds, 30);
        Ok(())
    }

    #[test]
    fn tasks_without_upstreams_carry_an_empty_connect_payload() -> R<()> {
        // Jobs that do not use connect must stay byte-identical on the wire —
        // an empty string, never "{}" — and an explicitly empty block counts
        // as "no upstreams" so it cannot make the manager do endpoint work.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None)?;
        assert_eq!(built.task_connect, "");

        let task = TaskConfig {
            connect: Some(crate::structs::ConnectConfig::default()),
            ..task
        };
        let built = build_task_allocation(&task, &allocation(None), None)?;
        assert_eq!(built.task_connect, "");
        Ok(())
    }

    #[test]
    fn declared_upstreams_are_serialized_onto_the_allocation() -> R<()> {
        let connect: crate::structs::ConnectConfig = serde_json::from_value(json!({
            "upstreams": [
                { "service": "db" },
                { "service": "cache", "localPort": 6379, "balance": "least_conn" },
            ]
        }))?;
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            connect: Some(connect.clone()),
            ..TaskConfig::new("api", "exec")
        };

        let built = build_task_allocation(&task, &allocation(None), None)?;
        let decoded: crate::structs::ConnectConfig = serde_json::from_str(&built.task_connect)?;
        assert_eq!(decoded, connect);
        // camelCase on the wire, like the rest of the jobspec.
        assert!(built.task_connect.contains("\"localPort\":6379"));
        Ok(())
    }

    #[test]
    fn injection_does_not_mutate_the_stored_task_config() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        build_task_allocation(&task, &allocation(None), None)?;
        assert!(task.config.get("env").is_none());
        Ok(())
    }

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

    #[test]
    fn run_dispatch_future_outside_runtime() -> R<()> {
        run_dispatch_future(async { Ok(()) })?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_dispatch_future_inside_runtime() -> R<()> {
        run_dispatch_future(async { Ok(()) })?;
        Ok(())
    }
}
