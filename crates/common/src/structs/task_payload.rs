//! Building the payload a worker runs: a [`TaskAllocation`] from a task's
//! config, its allocation, and the instance it serves. Pure functions of
//! the domain types, shared by the manager's dispatcher (which sends the
//! payload) and the worker (which builds one itself for an edge-started
//! session).

use crate::prelude::*;
use crate::structs::edge_boot::{Gate, ResolvedSpeculativeStart};
use crate::structs::{Allocation, Instance, ProvisionalAllocation, TaskAllocation, TaskConfig};

/// The payload for `allocation` of `task`: its config with the runtime
/// environment injected (`LEGION_ALLOC_ID`, the instance's identity), its
/// resources, network, volumes, and the lifecycle blocks a worker acts on.
pub fn build_task_allocation(
    task: &TaskConfig,
    allocation: &Allocation,
    instance: Option<&Instance>,
    volumes: &[crate::structs::VolumeMount],
) -> R<TaskAllocation> {
    build_task_allocation_with(task, allocation, instance, volumes, None)
}

/// [`build_task_allocation`] with an optional provisional block.
///
/// When `provisional` is set the payload describes work the store has not
/// recorded yet (docs/edge-boot.md): `allocation_id` is 0 and the node keys
/// the work by the claim id instead. `LEGION_ALLOC_ID` is deliberately
/// **absent** from such a payload rather than being filled with a placeholder
/// — an allocation id does not exist yet, and a workload that reported a fake
/// one would be lying about its own identity. `LEGION_CLAIM_ID` is injected in
/// its place, and `LEGION_INSTANCE_ID` is already correct from the first
/// millisecond, which is the identity a sandbox platform actually addresses.
pub fn build_task_allocation_with(
    task: &TaskConfig,
    allocation: &Allocation,
    instance: Option<&Instance>,
    volumes: &[crate::structs::VolumeMount],
    provisional: Option<ProvisionalAllocation>,
) -> R<TaskAllocation> {
    let mut config = task.config.clone();
    match provisional.as_ref() {
        Some(provisional) => inject_provisional_env(&mut config, &provisional.claim_id, instance),
        None => inject_runtime_env(&mut config, allocation, instance),
    }
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
    // Same rule for volumes: jobs without storage are unchanged on the wire.
    let task_volumes = if volumes.is_empty() {
        String::new()
    } else {
        serde_json::to_string(volumes)?
    };
    // As declared, without the scheduler's defaults folded in, so an isolating
    // runtime can size its sandbox only where the job actually asked for a size.
    let task_resources = match task.resources.as_object() {
        Some(resources) if !resources.is_empty() => {
            // Device requests travel with the ids the store assigned this
            // allocation (`devices[i].assigned`, docs/devices.md): the
            // worker allocates exactly those.
            let mut declared = resources.clone();
            if let Some(devices) = allocation.resources.get(crate::devices::RESOURCES_KEY) {
                if declared.contains_key(crate::devices::RESOURCES_KEY) {
                    declared.insert(crate::devices::RESOURCES_KEY.to_string(), devices.clone());
                }
            }
            serde_json::to_string(&declared)?
        }
        _ => String::new(),
    };
    // The declared stop ladder, carried because the worker rebuilds its
    // TaskConfig from these fields and would otherwise default every stop.
    // Empty means "undeclared", which the container runtime distinguishes
    // from a declared SIGTERM: only the silent case consults the image's own
    // STOPSIGNAL. Jobs that never declare `stop` stay byte-identical on the
    // wire.
    let task_stop = match task.stop.as_ref() {
        Some(stop) => serde_json::to_string(stop).context("Failed to serialize task stop")?,
        None => String::new(),
    };
    // The declared `network` block: mode, labels, and any inline policy. The
    // assigned ports travel separately on `network` (an `AllocNetwork`), so
    // this is only sent when it carries something the datapath reads — a
    // non-default mode, labels, or an inline chain — keeping jobs that only
    // declare ports byte-identical on the wire.
    let task_network = match task.network.as_ref() {
        Some(network)
            if network.mode != "auto"
                || !network.labels.is_empty()
                || network.declares_policy() =>
        {
            serde_json::to_string(network).context("Failed to serialize task network")?
        }
        _ => String::new(),
    };
    // Suspend and snapshots: the node acts on both itself (idle reports,
    // pressure suspends, a checkpoint-ready launch). Empty when undeclared,
    // so jobs without them stay byte-identical on the wire.
    let task_suspend = match task.suspend.as_ref() {
        Some(suspend) => serde_json::to_string(suspend).context("Failed to serialize suspend")?,
        None => String::new(),
    };
    let task_snapshots = match task.snapshots.as_ref() {
        Some(snapshots) => {
            serde_json::to_string(snapshots).context("Failed to serialize snapshots")?
        }
        None => String::new(),
    };
    // Lifecycle and restart decide what the worker's supervisor does when the
    // process exits. Without them every task ran as a service with
    // `restart = always`, so a batch task that exited 0 was relaunched forever
    // and never reported success. The restart block stays empty when
    // undeclared: the worker then derives the policy from the lifecycle.
    let task_restart = match task.restart.as_ref() {
        Some(restart) => serde_json::to_string(restart).context("Failed to serialize restart")?,
        None => String::new(),
    };
    // The node redeems boot tickets against the task config it was
    // dispatched, and the task's own mode caps an edge start's gate.
    let task_speculative_start = match task.speculative_start.as_ref() {
        Some(block) => {
            serde_json::to_string(block).context("Failed to serialize speculativeStart")?
        }
        None => String::new(),
    };
    // The `children` block: the node admits a child started from inside
    // the allocation against it. Empty when undeclared.
    let task_children = match task.children.as_ref() {
        Some(children) => {
            serde_json::to_string(children).context("Failed to serialize children")?
        }
        None => String::new(),
    };
    Ok(TaskAllocation {
        allocation_id: allocation.id,
        task_children,
        job_id: allocation.job_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        node_id: allocation.node_id.clone(),
        task_config_id: allocation.task_config_id,
        task_name: task.name.clone(),
        task_runtime: task.runtime.clone(),
        task_config,
        task_checks,
        task_on_unhealthy,
        network,
        task_identities,
        self_terminate_after_seconds,
        task_artifacts,
        task_connect,
        task_volumes,
        task_resources,
        task_stop,
        task_network,
        provisional,
        // Filled by the heartbeat's desired-set builder, the one path that
        // feeds the worker's pressure response (`build_desired_allocations`).
        priority_band: String::new(),
        // Attached by the heartbeat builder, which knows the instance's
        // restore source (see `snapshot_restore_payload`).
        restore: None,
        task_suspend,
        task_snapshots,
        identity_epoch: instance.map_or(0, |instance| instance.identity_epoch),
        // A node still running the session adopts it as this allocation
        // (docs/session-mode.md, "Promotion"); any other node starts it.
        promoted_from_session: instance
            .and_then(|instance| instance.promoted_from.clone())
            .unwrap_or_default(),
        task_lifecycle: task.lifecycle.to_string(),
        task_restart,
        task_speculative_start,
        task_group: task.group.clone(),
        task_service: match task.service.as_ref() {
            Some(service) => {
                serde_json::to_string(service).context("Failed to serialize service")?
            }
            None => String::new(),
        },
        session_task_spec: String::new(),
        session_task_config_id: 0,
        session_seal_keys: Vec::new(),
        // An envelope's size is its row's, which an elastic envelope's node
        // grows and shrinks (docs/session-mode.md, "Elastic envelopes");
        // `task_resources` stays the declared floor.
        envelope_resources: if crate::structs::session::envelope_serves(task).is_some() {
            allocation.resources.to_string()
        } else {
            String::new()
        },
    })
}

/// On an envelope's payload, carry the session task it serves, so the node
/// can start that task's sessions from a session ticket with no manager on
/// the path (session durability plan §6.4). A no-op for every other task.
pub fn attach_session_task_spec<'a>(
    payload: &mut TaskAllocation,
    task: &TaskConfig,
    job_tasks: impl IntoIterator<Item = (i64, &'a TaskConfig)>,
) -> R<Option<&'a TaskConfig>> {
    let Some(serves) = crate::structs::session::envelope_serves(task) else {
        return Ok(None);
    };
    if let Some((id, session_task)) = job_tasks
        .into_iter()
        .find(|(_, candidate)| candidate.name == serves)
    {
        payload.session_task_spec = serde_json::to_string(session_task)?;
        payload.session_task_config_id = id;
        return Ok(Some(session_task));
    }
    Ok(None)
}

/// Runtime env for a provisional start. Mirrors [`inject_runtime_env`] with
/// `LEGION_ALLOC_ID` replaced by `LEGION_CLAIM_ID`.
fn inject_provisional_env(config: &mut Value, claim_id: &str, instance: Option<&Instance>) {
    let Some(object) = config.as_object_mut() else {
        return;
    };
    let env = object
        .entry("env")
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(env) = env.as_object_mut() else {
        warn!(
            claim_id,
            "Task config env is not an object; skipping runtime env injection"
        );
        return;
    };
    // Never leave a stale or forged value behind: a provisional workload has
    // no allocation id, and must not be able to claim one.
    env.remove("LEGION_ALLOC_ID");
    env.insert(
        "LEGION_CLAIM_ID".to_string(),
        Value::String(claim_id.to_string()),
    );
    // A node builds an edge start from a task config it cached off another
    // allocation's dispatch, whose env names that allocation's instance. The
    // claim's own identity replaces it, and none is left behind without one.
    for key in [
        "LEGION_INSTANCE_ID",
        "LEGION_INSTANCE_KEY",
        "LEGION_INSTANCE_MODE",
        "LEGION_INSTANCE_SLOT",
    ] {
        env.remove(key);
    }
    if let Some(instance) = instance {
        env.insert(
            "LEGION_INSTANCE_ID".to_string(),
            Value::String(instance.instance_id.clone()),
        );
        // The id without its namespace: what a caller named the instance.
        env.insert(
            "LEGION_INSTANCE_KEY".to_string(),
            Value::String(
                crate::structs::namespace::instance_key(&instance.instance_id).to_string(),
            ),
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

/// The gate a task may actually reach, after applying the invariants that are
/// settings for nobody (edge boot plan §6.5).
///
/// A task is **downgraded to `stage`** — never silently, always with the
/// reason — when starting it before the commit would need something the apply
/// transaction has not produced yet:
///
/// - **Assigned ports.** `network.ports` and `connect.upstreams` are assigned
///   inside the apply, so a workload booted before it cannot know them. A
///   `stage`-gated task picks them up from the ratification payload before it
///   starts, which is why `stage` is unaffected.
/// - **Storage that cannot be fenced.** A volume a second node could attach
///   concurrently is mounted only after ratification. Node-local volumes are
///   exempt: the only possible racer is on the same node, where the worker's
///   claim table already fences it.
///
/// Returned alongside the reason so `job validate` can print it before the
/// deploy rather than leaving it to be discovered as a missing speedup.
pub fn effective_gate(task: &TaskConfig, declared: Gate) -> (Gate, Option<String>) {
    if declared <= Gate::Stage {
        return (declared, None);
    }
    if task.gang.is_some() {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` is a gang, whose members' ranks are assigned when the placement \
                 commits; gates above `stage` would boot it without them",
                task.name
            )),
        );
    }
    let declares_ports = task
        .network
        .as_ref()
        .is_some_and(|network| !network.ports.is_empty());
    if declares_ports {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` declares network ports, which are assigned when the placement \
                 commits; gates above `stage` would boot it without them",
                task.name
            )),
        );
    }
    if task
        .connect
        .as_ref()
        .is_some_and(|connect| !connect.is_empty())
    {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` declares connect upstreams, whose loopback ports are assigned \
                 when the placement commits; gates above `stage` would boot it \
                 without them",
                task.name
            )),
        );
    }
    if let Some(unfenceable) = unfenceable_storage(task) {
        return (
            Gate::Stage,
            Some(format!(
                "task `{}` claims volume `{unfenceable}`, whose driver cannot fence a \
                 concurrent attach from another node; it is mounted only after \
                 ratification",
                task.name
            )),
        );
    }
    (declared, None)
}

/// The first volume claim whose driver is not node-local, if any.
///
/// Node-local drivers are the exemption in §6.5: the only possible racer for
/// a node-local volume is on the same node, and the worker's claim table
/// already allows one live claim per instance there.
fn unfenceable_storage(task: &TaskConfig) -> Option<String> {
    const NODE_LOCAL_DRIVERS: &[&str] = &["local-dir", "host", "tmpfs"];
    let is_node_local =
        |driver: Option<&str>| NODE_LOCAL_DRIVERS.contains(&driver.unwrap_or("local-dir"));
    if let Some(storage) = task
        .stateful
        .as_ref()
        .and_then(|stateful| stateful.storage.as_ref())
    {
        if !is_node_local(storage.driver.as_deref()) {
            return Some("stateful.storage".to_string());
        }
    }
    task.volumes
        .iter()
        .find(|volume| !is_node_local(volume.driver.as_deref()))
        .map(|volume| volume.name.clone())
}

/// The resolved `speculativeStart` block for a task, with the invariant
/// downgrades of [`effective_gate`] applied.
pub fn resolved_speculative_start(task: &TaskConfig) -> R<ResolvedSpeculativeStart> {
    let mut resolved = task
        .speculative_start
        .as_ref()
        .map(|block| block.resolve())
        .transpose()?
        .unwrap_or_default();
    let (gate, downgrade) = effective_gate(task, resolved.mode);
    if let Some(reason) = downgrade {
        debug!(task = %task.name, reason, "Downgrading speculative start gate");
    }
    resolved.mode = gate;
    Ok(resolved)
}

/// Inject Legion placement environment variables into the task config’s `env`
/// map (the map exec/container plugins already read):
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
        // Non-object task configs cannot carry an env map; plugins that accept
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
        // The id without its namespace: what a caller named the instance.
        env.insert(
            "LEGION_INSTANCE_KEY".to_string(),
            Value::String(
                crate::structs::namespace::instance_key(&instance.instance_id).to_string(),
            ),
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
            restore_from: None,
            identity_epoch: 0,
            named: true,
            promoted_from: None,
            owner: None,
        }
    }

    fn env_from(task_allocation: &TaskAllocation) -> serde_json::Map<String, Value> {
        let config: Value =
            serde_json::from_str(&task_allocation.task_config).expect("config parses");
        config["env"].as_object().cloned().unwrap_or_default()
    }

    fn speculative(mode: &str) -> Option<crate::structs::edge_boot::SpeculativeStartConfig> {
        Some(crate::structs::edge_boot::SpeculativeStartConfig {
            mode: Some(mode.into()),
            ..Default::default()
        })
    }

    #[test]
    fn injects_alloc_id_for_stateless_tasks_without_instance_vars() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
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
            &[],
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
        let built = build_task_allocation(
            &task,
            &allocation(Some("sandbox-7f3a")),
            Some(&instance),
            &[],
        )?;
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
            &[],
        )?;
        let env = env_from(&built);
        assert_eq!(env["LEGION_INSTANCE_ID"], json!("job-default-1"));
        assert_eq!(env["LEGION_ALLOC_ID"], json!("42"));
        assert_eq!(env["USER_VAR"], json!("kept"));
        Ok(())
    }

    /// An edge start is built from a task config the node cached off another
    /// allocation's dispatch, so its env already names that allocation's
    /// instance. The claim's own identity must replace it.
    #[test]
    fn a_provisional_start_drops_a_cached_instance_identity() -> R<()> {
        let cached = TaskConfig {
            config: json!({
                "command": ["true"],
                "env": { "LEGION_INSTANCE_ID": "someone-else", "LEGION_INSTANCE_MODE": "keyed" }
            }),
            ..TaskConfig::new("box", "exec")
        };
        let provisional = || ProvisionalAllocation {
            claim_id: "clm_a".into(),
            ..Default::default()
        };
        let built = build_task_allocation_with(
            &cached,
            &allocation(Some("r-1")),
            None,
            &[],
            Some(provisional()),
        )?;
        let env = env_from(&built);
        assert!(!env.contains_key("LEGION_INSTANCE_ID"));
        assert_eq!(env["LEGION_CLAIM_ID"], json!("clm_a"));
        let mut instance = slot_instance();
        instance.instance_id = "r-1".into();
        let built = build_task_allocation_with(
            &cached,
            &allocation(Some("r-1")),
            Some(&instance),
            &[],
            Some(provisional()),
        )?;
        assert_eq!(env_from(&built)["LEGION_INSTANCE_ID"], json!("r-1"));
        Ok(())
    }

    #[test]
    fn stateless_tasks_carry_no_self_fence_window() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
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
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
            &[],
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
                require_id: false,
                initial_instances: None,
                on_lost: Some(crate::structs::StatefulOnLostConfig {
                    self_terminate_after: Some("30s".into()),
                    replace_after: Some("2m".into()),
                    unknown_fields: Default::default(),
                }),
                storage: None,
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("db", "exec")
        };
        let built = build_task_allocation(
            &task,
            &allocation(Some("job-default-1")),
            Some(&slot_instance()),
            &[],
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
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert_eq!(built.task_connect, "");

        let task = TaskConfig {
            connect: Some(crate::structs::ConnectConfig::default()),
            ..task
        };
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert_eq!(built.task_connect, "");
        Ok(())
    }

    #[test]
    fn declared_upstreams_are_serialized_onto_the_allocation() -> R<()> {
        let connect: crate::structs::ConnectConfig = serde_json::from_value(json!({
            "upstreams": [
                { "to": "db" },
                { "to": "cache", "port": 6379, "balance": "least_conn" },
            ]
        }))?;
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            connect: Some(connect.clone()),
            ..TaskConfig::new("api", "exec")
        };

        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        let decoded: crate::structs::ConnectConfig = serde_json::from_str(&built.task_connect)?;
        assert_eq!(decoded, connect);
        // camelCase on the wire, like the rest of the jobspec.
        assert!(built.task_connect.contains("\"port\":6379"));
        Ok(())
    }

    #[test]
    fn injection_does_not_mutate_the_stored_task_config() -> R<()> {
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        build_task_allocation(&task, &allocation(None), None, &[])?;
        assert!(task.config.get("env").is_none());
        Ok(())
    }

    // ---------------------------------------------------------------
    // Edge boot (docs/edge-boot.md)
    // ---------------------------------------------------------------

    #[test]
    fn a_task_without_the_block_speculates_nothing_and_changes_no_payload() -> R<()> {
        // The byte-for-byte promise: a job that did not opt in must produce
        // exactly the payload it produced before this feature existed.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("web", "exec")
        };
        assert_eq!(resolved_speculative_start(&task)?.mode, Gate::Off);
        let built = build_task_allocation(&task, &allocation(None), None, &[])?;
        assert!(built.provisional.is_none());
        Ok(())
    }

    #[test]
    fn a_provisional_payload_carries_a_claim_id_and_no_alloc_id() -> R<()> {
        // An allocation id does not exist before the commit, and a workload
        // that reported a fake one would be lying about its own identity.
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            speculative_start: speculative("boot"),
            ..TaskConfig::new("workspace", "exec")
        };
        let mut alloc = allocation(None);
        alloc.id = 0;
        let built = build_task_allocation_with(
            &task,
            &alloc,
            None,
            &[],
            Some(ProvisionalAllocation {
                claim_id: "clm_9f2c".into(),
                gate: Gate::Boot.to_wire(),
                lease_deadline_unix: 1_770_000_005,
                instance_id: "user-42".into(),
                origin: "edge".into(),
                on_refused: "kill".into(),
                ticket_jti: "tkt_1".into(),
                session: None,
            }),
        )?;
        assert_eq!(built.allocation_id, 0);
        let config: Value = serde_json::from_str(&built.task_config)?;
        assert_eq!(config["env"]["LEGION_CLAIM_ID"], json!("clm_9f2c"));
        assert!(config["env"].get("LEGION_ALLOC_ID").is_none());
        assert_eq!(built.provisional.expect("block").gate, Gate::Boot.to_wire());
        Ok(())
    }

    #[test]
    fn a_forged_alloc_id_cannot_survive_into_a_provisional_payload() {
        // A jobspec must not be able to make an unratified workload claim an
        // allocation id it does not have.
        let mut config = json!({
            "command": ["true"],
            "env": { "LEGION_ALLOC_ID": "8123" }
        });
        inject_provisional_env(&mut config, "clm_a", None);
        assert!(config["env"].get("LEGION_ALLOC_ID").is_none());
        assert_eq!(config["env"]["LEGION_CLAIM_ID"], json!("clm_a"));
    }

    #[test]
    fn declared_ports_cap_the_gate_at_stage() -> R<()> {
        // Ports are assigned inside the apply, so a workload booted before it
        // cannot know them. `stage` is unaffected: it picks them up from the
        // ratification payload before it starts.
        let network: crate::structs::network::NetworkConfig =
            serde_json::from_value(json!({ "ports": [{ "name": "http", "to": 8080 }] }))?;
        let task = TaskConfig {
            speculative_start: speculative("serve"),
            network: Some(network),
            ..TaskConfig::new("api", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Serve);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("network ports"));
        assert_eq!(resolved_speculative_start(&task)?.mode, Gate::Stage);
        Ok(())
    }

    #[test]
    fn a_gang_caps_the_gate_at_stage() {
        // A member's rank is assigned with its allocation row, so a member
        // booted before the commit could not know it.
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            gang: Some(crate::structs::GangConfig::default()),
            ..TaskConfig::new("trainer", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Boot);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("is a gang"));
    }

    #[test]
    fn connect_upstreams_cap_the_gate_at_stage() -> R<()> {
        let connect: crate::structs::ConnectConfig =
            serde_json::from_value(json!({ "upstreams": [{ "to": "db" }] }))?;
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            connect: Some(connect),
            ..TaskConfig::new("api", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Boot);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("connect upstreams"));
        Ok(())
    }

    #[test]
    fn unfenceable_storage_caps_the_gate_at_stage() -> R<()> {
        // Storage a second node could attach concurrently is mounted only
        // after ratification, so nothing above `stage` may run against it.
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: Some("nfs".into()),
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                    unknown_fields: Default::default(),
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("workspace", "exec")
        };
        let (gate, reason) = effective_gate(&task, Gate::Boot);
        assert_eq!(gate, Gate::Stage);
        assert!(reason.expect("reason").contains("fence"));
        Ok(())
    }

    #[test]
    fn node_local_storage_is_exempt_from_the_downgrade() -> R<()> {
        // The only possible racer for a node-local volume is on the same
        // node, where the worker's claim table already fences it.
        let task = TaskConfig {
            speculative_start: speculative("boot"),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None, // local-dir, the default
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                    unknown_fields: Default::default(),
                }),
                envelope: None,
                promote: None,
                placement: None,
                fence_after: None,
                unknown_fields: Default::default(),
            }),
            ..TaskConfig::new("workspace", "exec")
        };
        assert_eq!(effective_gate(&task, Gate::Boot).0, Gate::Boot);
        assert_eq!(resolved_speculative_start(&task)?.mode, Gate::Boot);
        Ok(())
    }
}
