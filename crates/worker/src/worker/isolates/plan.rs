//! The arithmetic the supervisor runs before it talks to a host: how many
//! slots one host process may have on this node, how many replicas a cordon
//! needs, and how a task's room is split between them. Pure, so it is
//! tested on its own.

use crate::structs::isolate::{ConfineMode, SlotLayout, WorkerIsolateConfigFull};
use crate::structs::isolate_host::{self as host, HostConfig};
use crate::structs::TaskConfig;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Memory mappings one live instance costs (brief item 19: about 7.5).
pub const MAPPINGS_PER_INSTANCE: u64 = 8;

/// Mappings left for the host process itself: its binary, libraries,
/// thread stacks, the loaded modules.
pub const HOST_MAPPINGS: u64 = 2048;

/// The kernel default for `vm.max_map_count`, used when it cannot be read.
pub const DEFAULT_MAX_MAP_COUNT: u64 = 65_530;

/// The most replicas one cordon runs on a node.
pub const MAX_REPLICAS: u32 = 64;

/// Read `vm.max_map_count`.
pub fn read_max_map_count() -> u64 {
    std::fs::read_to_string("/proc/sys/vm/max_map_count")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(DEFAULT_MAX_MAP_COUNT)
}

/// Slots per host after the clamp, and why it was clamped when it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPlan {
    pub slots: u32,
    pub clamped: Option<String>,
}

/// Clamp the configured slots to what the layout and `vm.max_map_count`
/// allow.
pub fn plan_slots(configured: u32, layout: SlotLayout, max_map_count: u64) -> SlotPlan {
    let by_maps = max_map_count.saturating_sub(HOST_MAPPINGS) / MAPPINGS_PER_INSTANCE;
    let by_maps = u32::try_from(by_maps).unwrap_or(u32::MAX).max(1);
    let wanted = configured.min(layout.max_slots()).max(1);
    if by_maps < wanted {
        return SlotPlan {
            slots: by_maps,
            clamped: Some(format!(
                "isolate.slotsPerHost {configured} clamped to {by_maps}: vm.max_map_count is \
                 {max_map_count} and each live isolate costs about {MAPPINGS_PER_INSTANCE} \
                 mappings; raise vm.max_map_count (sysctl) for denser hosts"
            )),
        };
    }
    SlotPlan {
        slots: wanted,
        clamped: (wanted < configured).then(|| {
            format!(
                "isolate.slotsPerHost {configured} clamped to {wanted}, the most layout {} fits",
                layout_name(layout)
            )
        }),
    }
}

pub fn layout_name(layout: SlotLayout) -> &'static str {
    match layout {
        SlotLayout::GuardPages => "guard-pages",
        SlotLayout::Mpk => "mpk",
        SlotLayout::Small => "small",
    }
}

/// The `legion-isolate` flag value for a layout.
pub fn layout_flag(layout: SlotLayout) -> &'static str {
    match layout {
        SlotLayout::GuardPages => "guard",
        SlotLayout::Mpk => "mpk",
        SlotLayout::Small => "small",
    }
}

/// Replicas a cordon needs for `resident` isolates at `slots` each.
pub fn replicas_needed(resident: u64, slots: u32) -> u32 {
    let slots = u64::from(slots.max(1));
    let needed = resident.div_ceil(slots).max(1);
    u32::try_from(needed).unwrap_or(u32::MAX).min(MAX_REPLICAS)
}

/// Split `total` across `replicas`, the remainder to the first ones.
pub fn split(total: u32, replicas: u32, index: u32) -> u32 {
    let replicas = replicas.max(1);
    total / replicas + u32::from(index < total % replicas)
}

/// What one isolate task may hold on this node, from its envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TaskRoom {
    /// Resident isolates: envelope memory over the reservation.
    pub max_resident: u32,
    /// Invocations at once: envelope CPU over the task's CPU weight.
    pub max_active: u32,
}

/// The room an envelope of `amounts` (MiB, MHz) gives a task.
pub fn task_room(envelope: &BTreeMap<String, f64>, task: &TaskConfig) -> TaskRoom {
    let memory = envelope.get("memory").copied().unwrap_or(0.0).max(0.0);
    let cpu = envelope.get("cpu").copied().unwrap_or(0.0).max(0.0);
    let reservation = crate::structs::isolate::reservation_mib(task).max(1) as f64;
    let weight = task_cpu_mhz(task).max(1) as f64;
    TaskRoom {
        max_resident: (memory / reservation).floor().min(u32::MAX as f64) as u32,
        max_active: ((cpu / weight).floor() as u32).max(1),
    }
}

/// The task's CPU weight in MHz (`resources.cpu`).
pub fn task_cpu_mhz(task: &TaskConfig) -> u64 {
    crate::structs::session::resource_amounts(&task.resources)
        .get("cpu")
        .copied()
        .filter(|cpu| cpu.is_finite() && *cpu > 0.0)
        .map(|cpu| cpu.ceil() as u64)
        .unwrap_or(crate::structs::isolate::DEFAULT_CPU_MHZ)
}

/// The `HostConfig` a cordon replica starts with.
pub fn host_config(
    config: &WorkerIsolateConfigFull,
    cordon: &str,
    slots: u32,
    read_dirs: &[PathBuf],
) -> HostConfig {
    let millis = |raw: crate::prelude::R<std::time::Duration>| {
        raw.map(|d| u32::try_from(d.as_millis()).unwrap_or(u32::MAX))
            .unwrap_or_default()
    };
    HostConfig {
        protocol_version: host::PROTOCOL_VERSION,
        cordon: cordon.to_string(),
        shards: config.shards,
        slots,
        slot_bytes: config.max_memory_bytes().unwrap_or(4 << 30),
        epoch_millis: millis(config.epoch()).max(1),
        slice_millis: millis(config.slice()).max(1),
        page_out_after_millis: millis(config.page_out_after()),
        layout: match config.layout {
            SlotLayout::GuardPages => host::SlotLayout::GuardPages,
            SlotLayout::Mpk => host::SlotLayout::Mpk,
            SlotLayout::Small => host::SlotLayout::Small,
        } as i32,
        confine: match config.confine {
            ConfineMode::Enforce => host::Confine::Enforce,
            ConfineMode::BestEffort => host::Confine::BestEffort,
            ConfineMode::Off => host::Confine::Off,
        } as i32,
        read_dirs: read_dirs
            .iter()
            .map(|dir| dir.display().to_string())
            .collect(),
        max_body_bytes: config.max_body_bytes().unwrap_or(16 << 20),
        async_stack_bytes: 128 << 10,
        require_spectre_mitigations: true,
    }
}

/// The confinement layers a `Hello` reports, by name, and the ones
/// missing.
pub fn confinement_layers(confinement: Option<&host::Confinement>) -> (Vec<String>, Vec<String>) {
    let confinement = confinement.cloned().unwrap_or_default();
    let mut applied = Vec::new();
    let mut missing = Vec::new();
    for (on, name) in [
        (confinement.user_namespace, "userns"),
        (confinement.network_namespace, "netns"),
        (confinement.no_new_privs, "no-new-privs"),
        (confinement.landlock, "landlock"),
        (confinement.seccomp, "seccomp"),
    ] {
        if on {
            applied.push(name.to_string());
        } else {
            missing.push(name.to_string());
        }
    }
    (applied, missing)
}

#[cfg(test)]
mod plan_test {
    use super::*;

    #[test]
    fn slots_are_clamped_by_max_map_count() {
        // The kernel default allows about 7,900 live instances.
        let plan = plan_slots(10_000, SlotLayout::GuardPages, DEFAULT_MAX_MAP_COUNT);
        assert_eq!(
            plan.slots,
            ((DEFAULT_MAX_MAP_COUNT - HOST_MAPPINGS) / 8) as u32
        );
        assert!(plan.clamped.unwrap().contains("vm.max_map_count is 65530"));
        // A raised sysctl leaves the configured value alone.
        let plan = plan_slots(10_000, SlotLayout::GuardPages, 1 << 20);
        assert_eq!(
            plan,
            SlotPlan {
                slots: 10_000,
                clamped: None
            }
        );
        // The layout's own maximum.
        let plan = plan_slots(50_000, SlotLayout::GuardPages, 1 << 22);
        assert_eq!(plan.slots, 16_000);
        assert!(plan.clamped.unwrap().contains("guard-pages"));
        // Never zero, even on an absurd sysctl.
        assert_eq!(plan_slots(10, SlotLayout::Small, 100).slots, 1);
        assert_eq!(plan_slots(0, SlotLayout::Small, 1 << 20).slots, 1);
    }

    #[test]
    fn replicas_and_splits() {
        assert_eq!(replicas_needed(0, 4), 1);
        assert_eq!(replicas_needed(4, 4), 1);
        assert_eq!(replicas_needed(5, 4), 2);
        assert_eq!(replicas_needed(1_000_000, 1), MAX_REPLICAS);
        let parts: Vec<u32> = (0..3).map(|i| split(10, 3, i)).collect();
        assert_eq!(parts, vec![4, 3, 3]);
        assert_eq!(parts.iter().sum::<u32>(), 10);
        assert_eq!(split(2, 3, 2), 0);
        assert_eq!(split(7, 0, 0), 7);
    }

    #[test]
    fn task_room_from_the_envelope() {
        let mut task = TaskConfig::new("run", "isolate");
        task.resources = serde_json::json!({ "cpu": 10, "memory": 16, "memoryMax": 128 });
        let envelope = BTreeMap::from([
            ("memory".to_string(), 32768.0),
            ("cpu".to_string(), 16000.0),
        ]);
        assert_eq!(
            task_room(&envelope, &task),
            TaskRoom {
                max_resident: 2048,
                max_active: 1600
            }
        );
        // A tiny envelope still lets one invocation run.
        let tiny = BTreeMap::from([("memory".to_string(), 8.0), ("cpu".to_string(), 1.0)]);
        assert_eq!(
            task_room(&tiny, &task),
            TaskRoom {
                max_resident: 0,
                max_active: 1
            }
        );
    }

    #[test]
    fn host_config_carries_the_worker_settings() {
        let config = WorkerIsolateConfigFull {
            confine: ConfineMode::BestEffort,
            ..Default::default()
        };
        let built = host_config(&config, "default", 7, &[PathBuf::from("/x/cwasm")]);
        assert_eq!(built.protocol_version, host::PROTOCOL_VERSION);
        assert_eq!(built.slots, 7);
        assert_eq!(built.slot_bytes, 4 << 30);
        assert_eq!(built.epoch_millis, 1);
        assert_eq!(built.slice_millis, 5);
        assert_eq!(built.page_out_after_millis, 2000);
        assert_eq!(built.confine, host::Confine::BestEffort as i32);
        assert_eq!(built.read_dirs, vec!["/x/cwasm"]);
        assert_eq!(built.max_body_bytes, 16 << 20);
    }

    #[test]
    fn confinement_layers_by_name() {
        let (applied, missing) = confinement_layers(Some(&host::Confinement {
            user_namespace: true,
            seccomp: true,
            ..Default::default()
        }));
        assert_eq!(applied, vec!["userns", "seccomp"]);
        assert_eq!(missing, vec!["netns", "no-new-privs", "landlock"]);
    }
}
