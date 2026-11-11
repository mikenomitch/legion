//! Building the node's isolate supervisor at worker start: find and probe
//! the binary, find the images, decide where compiled modules, checkpoints
//! and host cgroups go, and whether the node can page isolates out.

use super::binary::{self, ImageCatalog};
use super::host::ProcessLauncher;
use super::modules::Compiler;
use super::sessions::CheckpointStore;
use super::{IsolateSupervisor, SupervisorParts};
use crate::prelude::*;
use crate::structs::isolate::WorkerIsolateConfigFull;
use crate::worker::task_manager::WorkerTaskManager;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Whether the node can give idle isolates' pages to swap: cgroup v2
/// `memory.reclaim` on the host cgroup's parent and some swap, zswap, or
/// zram. `None` when it can; otherwise why not.
pub fn paging_unavailable(cgroup_dir: Option<&Path>) -> Option<String> {
    let Some(dir) = cgroup_dir else {
        return Some(
            "paging out is not possible on this node: isolate hosts get no cgroup of their own \
             (resource enforcement is off or the node has no cgroup v2)"
                .into(),
        );
    };
    let parent_has_reclaim = dir
        .parent()
        .is_some_and(|parent| parent.join("memory.reclaim").exists());
    if !parent_has_reclaim {
        return Some(
            "paging out is not possible on this node: the kernel has no memory.reclaim".into(),
        );
    }
    let swap = std::fs::read_to_string("/proc/swaps")
        .map(|swaps| swaps.lines().count() > 1)
        .unwrap_or(false);
    let zswap = std::fs::read_to_string("/sys/module/zswap/parameters/enabled")
        .map(|on| on.trim() == "Y")
        .unwrap_or(false);
    if !swap && !zswap {
        return Some(
            "paging out is not possible on this node: it has no swap, zswap, or zram".into(),
        );
    }
    None
}

/// Start the supervisor, or say once why this node runs no isolates.
pub async fn start(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    node_id: &str,
    config: WorkerIsolateConfigFull,
    queue_timeout: Duration,
    enforcement: &crate::structs::WorkerResourceEnforcementConfig,
) -> Option<Arc<IsolateSupervisor>> {
    if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        info!(
            "The isolate runtime runs on x86-64 Linux only; this node does not advertise isolates"
        );
        return None;
    }
    let Some(path) = binary::locate_binary(&config) else {
        info!(
            "No legion-isolate binary (isolate.binary, beside the worker, or \
             LEGION_ISOLATE_BINARY); this node does not run isolates"
        );
        return None;
    };
    let probe_config = config.clone();
    let probed = tokio::task::spawn_blocking(move || binary::probe(&path, &probe_config))
        .await
        .ok()?;
    let found = match probed {
        Ok(found) => found,
        Err(err) => {
            info!(error = %format!("{err:#}"), "legion-isolate is not usable; this node does not run isolates");
            return None;
        }
    };
    let images = ImageCatalog::locate(&config, &found.path).and_then(|dir| {
        ImageCatalog::load(&dir)
            .map_err(|err| warn!(error = %format!("{err:#}"), "Isolate images unreadable; image tasks cannot run here"))
            .ok()
    });
    let (state_dir, store, stager, registry, gate, alloc_root) = {
        let manager = task_manager.lock().ok()?;
        let state_dir = manager
            .state_store()
            .path()
            .parent()
            .map(|dir| dir.join("isolates"))
            .unwrap_or_else(|| PathBuf::from("isolates"));
        (
            state_dir,
            manager.content_store(),
            manager.stager_handle(),
            manager.run_registry(),
            manager.runtime_gate(),
            manager.alloc_root().to_path_buf(),
        )
    };
    let fallback_dir = state_dir.join("cwasm");
    let mut read_dirs = vec![fallback_dir.clone()];
    if let Some(store) = store.as_ref() {
        read_dirs.push(super::modules::derived_dir(store));
    }
    for dir in &read_dirs {
        if let Err(err) = std::fs::create_dir_all(dir) {
            warn!(dir = %dir.display(), error = %err, "Could not create an isolate artifact directory");
        }
    }
    let cgroup_dir = (enforcement.enabled && crate::worker::resources::enforcement_available())
        .then(|| {
            PathBuf::from("/sys/fs/cgroup")
                .join(enforcement.cgroup_parent.trim().trim_matches('/'))
                .join("isolate-hosts")
        });
    let paging = paging_unavailable(cgroup_dir.as_deref());
    info!(
        binary = %found.path.display(),
        engine = %found.engine_version,
        images = images.as_ref().map(|i| i.images.len()).unwrap_or(0),
        "Isolate runtime available on this node"
    );
    let supervisor = IsolateSupervisor::new(SupervisorParts {
        node_id: node_id.to_string(),
        modules: Arc::new(Compiler {
            binary: found.clone(),
            config: config.clone(),
            stager: Some(stager),
            store: store.clone(),
            images: images.clone(),
            fallback_dir,
        }),
        launcher: Arc::new(ProcessLauncher {
            binary: found.path.clone(),
            cgroup_dir,
        }),
        binary: found,
        images,
        read_dirs,
        book_path: Some(state_dir.join("sessions.json")),
        checkpoints: CheckpointStore {
            store,
            dir: state_dir.join("checkpoints"),
        },
        run_registry: registry,
        gate,
        queue_timeout,
        max_map_count: super::plan::read_max_map_count(),
        paging_unavailable: paging,
        config,
    });
    supervisor.attach(task_manager);
    supervisor.set_alloc_root(alloc_root);
    if let Ok(mut manager) = task_manager.lock() {
        manager.set_isolates(Some(supervisor.clone()));
    }
    supervisor.spawn_maintenance();
    Some(supervisor)
}
