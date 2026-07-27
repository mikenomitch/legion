//! The worker half of the volume system: mount before start, unmount on stop,
//! destroy on request, and report every outcome back to the manager.
//!
//! Workers own the drivers; the manager owns the state machine. Nothing here
//! decides *whether* a volume should exist — the manager sends a resolved mount
//! list with every allocation and a reclaim list with every heartbeat reply.
//! See `docs/volumes.md`.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig, VolumeDestroyRequest, VolumeMount, VolumeReport};
use crate::volumes::{
    volume_env_var, VolumeDriverRegistry, VolumeInstanceRef, VolumeMode, VolumeOp, VolumeRef,
    VolumeRequest, VolumeRuntimeConfig,
};

/// A volume this worker currently has mounted for an allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountedVolume {
    pub volume_id: String,
    pub name: String,
    pub driver: String,
    /// How the driver handed it over.
    pub mode: VolumeMode,
    /// Absolute path on this host: the directory in filesystem mode, the
    /// device (or raw disk image) in block mode.
    pub host_path: PathBuf,
    /// Block mode: the filesystem the device carries.
    pub fs: Option<String>,
    /// True when this mount created the storage rather than reattaching it.
    pub fresh: bool,
    /// Where the task should see it — the bind destination for container
    /// tasks. `None` when the job declared no path (exec tasks read the env
    /// var instead).
    pub path: Option<String>,
    pub read_only: bool,
    /// True for the volume that came from `stateful.storage`.
    pub instance_volume: bool,
    /// The request that produced this mount, replayed verbatim for `unmount`.
    request: VolumeRequest,
}

/// Volume drivers plus the mounts this worker holds.
///
/// Every method takes `&self`: the bookkeeping lives behind its own small
/// mutexes, **never held across a driver call**. That structure is
/// load-bearing — driver operations shell out (`mount`, `mkfs`, a hung NFS
/// server) and can stall for minutes, and the caller (the task manager /
/// heartbeat path) must not have its lock chained to ours. A destroy that
/// hangs must cost that destroy, not the node's heartbeats — a worker that
/// cannot heartbeat self-fences its stateful allocations, which would turn one
/// stuck volume into an outage.
pub struct WorkerVolumeManager {
    registry: VolumeDriverRegistry,
    node_id: String,
    mounted: Mutex<HashMap<i64, Vec<MountedVolume>>>,
    /// Outcomes waiting for the next heartbeat.
    reports: Mutex<Vec<VolumeReport>>,
    /// Volume ids already reclaimed this process, so a manager that re-sends a
    /// destroy before its confirmation lands does not re-run the driver.
    destroyed: Mutex<std::collections::HashSet<String>>,
}

impl WorkerVolumeManager {
    pub fn new(node_id: impl Into<String>, config: VolumeRuntimeConfig) -> Self {
        WorkerVolumeManager {
            registry: VolumeDriverRegistry::with_discovery(config),
            node_id: node_id.into(),
            mounted: Mutex::new(HashMap::new()),
            reports: Mutex::new(Vec::new()),
            destroyed: Mutex::new(std::collections::HashSet::new()),
        }
    }

    fn push_report(&self, report: VolumeReport) {
        self.reports.lock().expect("volume reports").push(report);
    }

    /// Capabilities to publish in this node's fingerprint. The manager has no
    /// other source of plugin metadata, so this is what job admission and
    /// placement validate against.
    pub fn fingerprint(&self) -> Value {
        serde_json::to_value(self.registry.capabilities()).unwrap_or_else(|_| json!({}))
    }

    pub fn driver_names(&self) -> Vec<String> {
        self.registry.names()
    }

    /// Mount every volume for an allocation, in order.
    ///
    /// All-or-nothing: if any mount fails the ones already made are released,
    /// so a task never starts with half its storage. The error names the
    /// volume and plugin, because "permission denied" on its own tells an
    /// operator nothing.
    #[instrument(err, skip_all, fields(allocation_id = allocation.id))]
    pub fn mount_for_allocation(
        &self,
        allocation: &Allocation,
        instance: Option<&VolumeInstanceRef>,
        mounts: &[VolumeMount],
    ) -> R<Vec<MountedVolume>> {
        if mounts.is_empty() {
            return Ok(Vec::new());
        }
        let mut mounted: Vec<MountedVolume> = Vec::new();
        for mount in mounts {
            match self.mount_one(allocation, instance, mount) {
                Ok(volume) => mounted.push(volume),
                Err(err) => {
                    let detail = format!("{err:#}");
                    self.push_report(VolumeReport {
                        volume_id: mount.volume_id.clone(),
                        op: VolumeOp::Mount.as_str().to_string(),
                        ok: false,
                        mount_path: String::new(),
                        node_local: false,
                        attrs: String::new(),
                        error: detail.clone(),
                        fs: String::new(),
                        fresh: false,
                    });
                    for done in mounted.iter().rev() {
                        self.unmount_one(done);
                    }
                    bail!(
                        "failed to mount volume `{}` ({}) for allocation {}: {detail}",
                        mount.name,
                        mount.driver,
                        allocation.id
                    );
                }
            }
        }

        self.mounted
            .lock()
            .expect("volume mounts")
            .insert(allocation.id, mounted.clone());
        Ok(mounted)
    }

    fn mount_one(
        &self,
        allocation: &Allocation,
        instance: Option<&VolumeInstanceRef>,
        mount: &VolumeMount,
    ) -> R<MountedVolume> {
        let plugin = self.registry.require(&mount.driver)?;
        let capabilities = plugin.capabilities();
        if !capabilities.available {
            bail!(
                "volume driver `{}` is not usable on this node: {}",
                mount.driver,
                capabilities.detail
            );
        }
        let missing = capabilities.missing_params(&mount.params);
        if !missing.is_empty() {
            bail!(
                "volume driver `{}` requires params: {}",
                mount.driver,
                missing.join(", ")
            );
        }
        // Admission already checked this against the cluster's advertised
        // capabilities; re-checking here catches the case where the node that
        // actually got the allocation is not the one admission looked at.
        if !capabilities.serves_mode(mount.mode) {
            bail!(
                "volume driver `{}` does not serve {} mode on this node (it serves: {})",
                mount.driver,
                mount.mode,
                capabilities
                    .volume_modes
                    .iter()
                    .map(|mode| mode.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let request = VolumeRequest {
            api_version: crate::volumes::VOLUME_API_VERSION,
            op: VolumeOp::Mount,
            mode: mount.mode,
            volume: VolumeRef {
                id: mount.volume_id.clone(),
                name: mount.name.clone(),
                driver: mount.driver.clone(),
                attrs: mount.attrs.clone(),
                read_only: mount.read_only,
            },
            params: mount.params.clone(),
            instance: instance.cloned(),
            alloc_id: allocation.id,
            node: self.registry.node_ref(&self.node_id),
        };

        let result = plugin.mount(&request)?;
        // A driver that answers in the wrong shape (a path for a block
        // request) would fail much later, in the task kind, with a confusing
        // error. Catch it at the boundary.
        result.validate_for(mount.mode, &mount.driver)?;
        let host_path = result.resolved_path().cloned().expect("validated above");
        info!(
            allocation_id = allocation.id,
            volume_id = %mount.volume_id,
            driver = %mount.driver,
            mode = %mount.mode,
            fresh = result.fresh,
            path = %host_path.display(),
            "Mounted volume"
        );
        self.push_report(VolumeReport {
            volume_id: mount.volume_id.clone(),
            op: VolumeOp::Mount.as_str().to_string(),
            ok: true,
            mount_path: host_path.display().to_string(),
            node_local: capabilities.node_local,
            attrs: serde_json::to_string(&result.attrs).unwrap_or_else(|_| "{}".into()),
            error: String::new(),
            fs: result.fs.clone().unwrap_or_default(),
            fresh: result.fresh,
        });

        // Replay the mount request for unmount with only the op swapped, so a
        // driver sees identical inputs on both sides of the pair.
        let mut unmount_request = request.clone();
        unmount_request.op = VolumeOp::Unmount;
        unmount_request.volume.attrs = result.attrs;

        Ok(MountedVolume {
            volume_id: mount.volume_id.clone(),
            name: mount.name.clone(),
            driver: mount.driver.clone(),
            mode: mount.mode,
            host_path,
            fs: result.fs,
            fresh: result.fresh,
            path: mount.path.clone(),
            read_only: mount.read_only,
            instance_volume: mount.instance_volume,
            request: unmount_request,
        })
    }

    /// Release every volume held for an allocation. Best-effort: a failed
    /// unmount is reported and logged but never blocks the allocation from
    /// being torn down, or the worker would wedge on a hung NFS server.
    #[instrument(skip_all, fields(allocation_id))]
    pub fn unmount_for_allocation(&self, allocation_id: i64) {
        let Some(mounts) = self
            .mounted
            .lock()
            .expect("volume mounts")
            .remove(&allocation_id)
        else {
            return;
        };
        for mount in mounts.iter().rev() {
            self.unmount_one(mount);
        }
    }

    fn unmount_one(&self, mount: &MountedVolume) {
        let Some(plugin) = self.registry.get(&mount.driver) else {
            return;
        };
        match plugin.unmount(&mount.request) {
            Ok(()) => {
                info!(
                    volume_id = %mount.volume_id,
                    driver = %mount.driver,
                    "Unmounted volume"
                );
                self.push_report(VolumeReport {
                    volume_id: mount.volume_id.clone(),
                    op: VolumeOp::Unmount.as_str().to_string(),
                    ok: true,
                    mount_path: String::new(),
                    node_local: false,
                    attrs: String::new(),
                    error: String::new(),
                    fs: String::new(),
                    fresh: false,
                });
            }
            Err(err) => {
                warn!(
                    volume_id = %mount.volume_id,
                    driver = %mount.driver,
                    error = ?err,
                    "Failed to unmount volume"
                );
                self.push_report(VolumeReport {
                    volume_id: mount.volume_id.clone(),
                    op: VolumeOp::Unmount.as_str().to_string(),
                    ok: false,
                    mount_path: String::new(),
                    node_local: false,
                    attrs: String::new(),
                    error: format!("{err:#}"),
                    fs: String::new(),
                    fresh: false,
                });
            }
        }
    }

    /// Run the driver's `destroy` for volumes the manager asked to reclaim.
    ///
    /// The manager re-sends until it sees a confirmation, so a failure here is
    /// just a retry next heartbeat — which is what makes reclaim crash-safe.
    ///
    /// Two guards:
    /// - a volume this worker **currently has mounted for a live allocation**
    ///   is refused, not destroyed. `rm -rf` under a running database is the
    ///   one mistake this subsystem must never make; the destroy retries after
    ///   the allocation stops and unmounts.
    /// - the driver call runs with **no lock held**: `mounted` is consulted
    ///   and released before dispatch, so a hung driver stalls this reclaim,
    ///   not mounts, reports, or the caller.
    #[instrument(skip_all)]
    pub fn destroy_requested(&self, requests: &[VolumeDestroyRequest]) {
        // Once the manager stops asking about an id, its confirmation window
        // has closed — forget it rather than accumulating one entry per volume
        // this worker has ever reclaimed.
        {
            let requested: std::collections::HashSet<&str> = requests
                .iter()
                .map(|request| request.volume_id.as_str())
                .collect();
            self.destroyed
                .lock()
                .expect("destroyed volumes")
                .retain(|id| requested.contains(id.as_str()));
        }

        for request in requests {
            if self
                .destroyed
                .lock()
                .expect("destroyed volumes")
                .contains(&request.volume_id)
            {
                continue;
            }
            // In-use guard: never destroy under a live mount.
            let in_use_by = self
                .mounted
                .lock()
                .expect("volume mounts")
                .iter()
                .find(|(_, mounts)| {
                    mounts
                        .iter()
                        .any(|mount| mount.volume_id == request.volume_id)
                })
                .map(|(allocation_id, _)| *allocation_id);
            if let Some(allocation_id) = in_use_by {
                warn!(
                    volume_id = %request.volume_id,
                    allocation_id,
                    "Refusing to destroy a volume mounted by a live allocation; will retry"
                );
                self.push_report(VolumeReport {
                    volume_id: request.volume_id.clone(),
                    op: VolumeOp::Destroy.as_str().to_string(),
                    ok: false,
                    mount_path: String::new(),
                    node_local: false,
                    attrs: String::new(),
                    error: format!("in use by allocation {allocation_id} on this node"),
                    fs: String::new(),
                    fresh: false,
                });
                continue;
            }

            let Some(driver) = self.registry.get(&request.driver) else {
                // The manager filters by advertised drivers, so this only
                // happens mid-reconfiguration. Stay quiet and let it retry.
                debug!(
                    volume_id = %request.volume_id,
                    driver = %request.driver,
                    "Ignoring destroy for a driver this node does not have"
                );
                continue;
            };
            let volume_request = VolumeRequest {
                api_version: crate::volumes::VOLUME_API_VERSION,
                op: VolumeOp::Destroy,
                // Destroy removes the storage whatever shape it was handed out
                // in; a driver that cares reads its own recorded attrs.
                mode: VolumeMode::Filesystem,
                volume: VolumeRef {
                    id: request.volume_id.clone(),
                    name: request.name.clone(),
                    driver: request.driver.clone(),
                    attrs: serde_json::from_str(&request.attrs).unwrap_or_default(),
                    read_only: false,
                },
                params: serde_json::from_str(&request.params).unwrap_or_default(),
                instance: None,
                alloc_id: 0,
                node: self.registry.node_ref(&self.node_id),
            };

            match driver.destroy(&volume_request) {
                Ok(()) => {
                    info!(
                        volume_id = %request.volume_id,
                        driver = %request.driver,
                        "Destroyed volume"
                    );
                    self.destroyed
                        .lock()
                        .expect("destroyed volumes")
                        .insert(request.volume_id.clone());
                    self.push_report(VolumeReport {
                        volume_id: request.volume_id.clone(),
                        op: VolumeOp::Destroy.as_str().to_string(),
                        ok: true,
                        mount_path: String::new(),
                        node_local: false,
                        attrs: String::new(),
                        error: String::new(),
                        fs: String::new(),
                        fresh: false,
                    });
                }
                Err(err) => {
                    warn!(
                        volume_id = %request.volume_id,
                        driver = %request.driver,
                        error = ?err,
                        "Failed to destroy volume; will retry"
                    );
                    self.push_report(VolumeReport {
                        volume_id: request.volume_id.clone(),
                        op: VolumeOp::Destroy.as_str().to_string(),
                        ok: false,
                        mount_path: String::new(),
                        node_local: false,
                        attrs: String::new(),
                        error: format!("{err:#}"),
                        fs: String::new(),
                        fresh: false,
                    });
                }
            }
        }
    }

    /// Take the pending outcomes for the next heartbeat.
    pub fn drain_reports(&self) -> Vec<VolumeReport> {
        std::mem::take(&mut *self.reports.lock().expect("volume reports"))
    }

    /// Put drained outcomes back after a failed heartbeat, ahead of anything
    /// recorded since, so ordering per volume is preserved.
    pub fn requeue_reports(&self, mut reports: Vec<VolumeReport>) {
        if reports.is_empty() {
            return;
        }
        let mut pending = self.reports.lock().expect("volume reports");
        reports.append(&mut pending);
        *pending = reports;
    }

    /// The volumes currently mounted for an allocation (diagnostics/tests).
    pub fn mounts_for(&self, allocation_id: i64) -> Option<Vec<MountedVolume>> {
        self.mounted
            .lock()
            .expect("volume mounts")
            .get(&allocation_id)
            .cloned()
    }
}

/// The instance a volume belongs to, read back from the runtime env the
/// manager injected (`LEGION_INSTANCE_ID` / `_MODE` / `_SLOT`).
///
/// Those variables are authoritative — the manager derives them from the
/// instance binding and they always win over anything a jobspec declares — so
/// reading them here keeps the worker in sync without a second copy of the
/// same facts on the wire. `None` for stateless tasks, which have no instance.
pub fn instance_ref_from_task(
    allocation: &Allocation,
    task: &TaskConfig,
) -> Option<VolumeInstanceRef> {
    let env = task.config.get("env")?.as_object()?;
    let get = |key: &str| env.get(key).and_then(Value::as_str);
    Some(VolumeInstanceRef {
        id: get("LEGION_INSTANCE_ID")?.to_string(),
        job: allocation.job_id.clone(),
        group: task.group.clone(),
        mode: get("LEGION_INSTANCE_MODE").unwrap_or("slot").to_string(),
        slot: get("LEGION_INSTANCE_SLOT").and_then(|slot| slot.parse().ok()),
    })
}

/// Env vars a task sees for its volumes.
///
/// Every volume is exported as `LEGION_VOLUME_<NAME>`; the instance volume is
/// additionally exported as `LEGION_STORAGE_DIR`, which is the variable the
/// Postgres/Redis guides point at.
///
/// `in_task_view` selects which path is exported. Container tasks get the
/// **destination** path, because that is where the volume actually appears
/// inside the container; exec tasks get the **host** path, because they run in
/// the host mount namespace and there is nothing to mount into (see
/// docs/volumes.md, "How `path` works per task kind").
pub fn volume_env(mounts: &[MountedVolume], in_task_view: bool) -> Vec<(String, String)> {
    let mut env = Vec::new();
    for mount in mounts {
        let value = if in_task_view {
            match mount.path.as_deref() {
                Some(path) => path.to_string(),
                None => continue,
            }
        } else {
            mount.host_path.display().to_string()
        };
        env.push((volume_env_var(&mount.name), value.clone()));
        if mount.instance_volume {
            env.push(("LEGION_STORAGE_DIR".to_string(), value));
        }
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::HostVolumeConfig;

    fn temp_root(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("legion-wvol-{label}-{unique}"));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn manager(root: &std::path::Path) -> WorkerVolumeManager {
        WorkerVolumeManager::new(
            "node-1",
            VolumeRuntimeConfig {
                volume_root: root.join("volumes"),
                data_dir: root.to_path_buf(),
                host_volumes: HashMap::new(),
                plugin_dir: root.join("plugins"),
            },
        )
    }

    fn allocation() -> Allocation {
        Allocation {
            id: 7,
            status: "placed".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "cfg".into(),
            task_config_id: 1,
            job_id: "db".into(),
            instance_id: Some("db-default-0".into()),
            network: None,
        }
    }

    fn instance() -> VolumeInstanceRef {
        VolumeInstanceRef {
            id: "db-default-0".into(),
            job: "db".into(),
            group: "default".into(),
            mode: "slot".into(),
            slot: Some(0),
        }
    }

    fn mount_spec(
        name: &str,
        driver: &str,
        path: Option<&str>,
        instance_volume: bool,
    ) -> VolumeMount {
        VolumeMount {
            volume_id: format!("vol-{name}"),
            name: name.to_string(),
            driver: driver.to_string(),
            mode: VolumeMode::Filesystem,
            params: Default::default(),
            attrs: Default::default(),
            path: path.map(str::to_string),
            read_only: false,
            instance_volume,
        }
    }

    #[test]
    fn mounts_persist_across_unmount_and_remount() -> R<()> {
        let root = temp_root("cycle");
        let volumes = manager(&root);
        let mounts = vec![mount_spec("data", "local-dir", Some("/var/lib/db"), true)];

        let mounted = volumes.mount_for_allocation(&allocation(), Some(&instance()), &mounts)?;
        assert_eq!(mounted.len(), 1);
        std::fs::write(mounted[0].host_path.join("x"), "1")?;

        volumes.unmount_for_allocation(7);
        assert!(volumes.mounts_for(7).is_none());

        let remounted = volumes.mount_for_allocation(&allocation(), Some(&instance()), &mounts)?;
        assert_eq!(
            std::fs::read_to_string(remounted[0].host_path.join("x"))?,
            "1",
            "data must survive the unmount/remount cycle"
        );

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn a_failed_mount_releases_the_ones_already_made() -> R<()> {
        let root = temp_root("rollback");
        let volumes = manager(&root);
        // `host` with no declared claims is unusable, so the second mount fails.
        let mounts = vec![
            mount_spec("data", "local-dir", Some("/var/lib/db"), true),
            mount_spec("models", "host", Some("/models"), false),
        ];

        let err = volumes
            .mount_for_allocation(&allocation(), Some(&instance()), &mounts)
            .unwrap_err()
            .to_string();
        assert!(err.contains("models"), "{err}");
        assert!(
            volumes.mounts_for(7).is_none(),
            "a partial mount must not be recorded"
        );

        let reports = volumes.drain_reports();
        assert!(reports
            .iter()
            .any(|report| report.ok && report.op == "mount"));
        assert!(reports
            .iter()
            .any(|report| !report.ok && report.op == "mount"));
        // The successful mount was rolled back, so it also reports an unmount.
        assert!(reports.iter().any(|report| report.op == "unmount"));

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn unknown_plugin_fails_with_the_installed_list() -> R<()> {
        let root = temp_root("unknown");
        let volumes = manager(&root);
        let mounts = vec![mount_spec("data", "ceph", Some("/data"), true)];
        let err = volumes
            .mount_for_allocation(&allocation(), Some(&instance()), &mounts)
            .unwrap_err()
            .to_string();
        assert!(err.contains("ceph"), "{err}");
        assert!(
            err.contains("local-dir"),
            "error should list what IS available: {err}"
        );

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn host_claims_resolve_declared_paths() -> R<()> {
        let root = temp_root("host-claim");
        let models = root.join("models");
        std::fs::create_dir_all(&models)?;
        let mut host_volumes = HashMap::new();
        host_volumes.insert(
            "models".to_string(),
            HostVolumeConfig {
                path: models.display().to_string(),
                device: None,
                fs: None,
                read_only: false,
                create: false,
            },
        );
        let volumes = WorkerVolumeManager::new(
            "node-1",
            VolumeRuntimeConfig {
                volume_root: root.join("volumes"),
                data_dir: root.clone(),
                host_volumes,
                plugin_dir: root.join("plugins"),
            },
        );
        let mut spec = mount_spec("models", "host", Some("/models"), false);
        spec.params.insert("name".into(), json!("models"));

        let mounted = volumes.mount_for_allocation(&allocation(), None, &[spec])?;
        assert_eq!(mounted[0].host_path, models);

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn a_destroy_runs_once_per_request_window_and_is_then_forgotten() -> R<()> {
        let root = temp_root("destroy");
        let volumes = manager(&root);
        let mounts = vec![mount_spec("data", "local-dir", Some("/var/lib/db"), true)];
        let mounted = volumes.mount_for_allocation(&allocation(), Some(&instance()), &mounts)?;
        let dir = mounted[0].host_path.clone();
        volumes.unmount_for_allocation(7);
        let _ = volumes.drain_reports();

        let request = VolumeDestroyRequest {
            volume_id: "vol-data".into(),
            driver: "local-dir".into(),
            name: "data".into(),
            params: "{}".into(),
            attrs: "{}".into(),
        };

        volumes.destroy_requested(std::slice::from_ref(&request));
        assert!(!dir.exists(), "the driver removed the directory");
        let reports = volumes.drain_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].op, "destroy");

        // The manager re-sends until it sees the confirmation; the driver must
        // not run again in the meantime.
        volumes.destroy_requested(std::slice::from_ref(&request));
        assert!(volumes.drain_reports().is_empty(), "no duplicate destroy");

        // Once the manager stops asking, the id is forgotten rather than
        // accumulating for the worker's lifetime.
        volumes.destroy_requested(&[]);
        assert!(volumes.destroyed.lock().expect("destroyed").is_empty());

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn env_uses_host_paths_for_exec_and_destinations_for_containers() {
        let mounts = vec![MountedVolume {
            volume_id: "vol-1".into(),
            name: "data".into(),
            driver: "local-dir".into(),
            mode: VolumeMode::Filesystem,
            host_path: PathBuf::from("/var/lib/legion/volumes/local-dir/vol-1"),
            fs: None,
            fresh: false,
            path: Some("/var/lib/db".into()),
            read_only: false,
            instance_volume: true,
            request: VolumeRequest::new(
                VolumeOp::Unmount,
                VolumeRef {
                    id: "vol-1".into(),
                    name: "data".into(),
                    driver: "local-dir".into(),
                    attrs: Default::default(),
                    read_only: false,
                },
                crate::volumes::VolumeNodeRef {
                    id: "node-1".into(),
                    data_dir: PathBuf::from("/var/lib/legion"),
                    volume_dir: PathBuf::from("/var/lib/legion/volumes"),
                },
            ),
        }];

        let host = volume_env(&mounts, false);
        assert!(host.contains(&(
            "LEGION_VOLUME_DATA".into(),
            "/var/lib/legion/volumes/local-dir/vol-1".into()
        )));
        assert!(host.contains(&(
            "LEGION_STORAGE_DIR".into(),
            "/var/lib/legion/volumes/local-dir/vol-1".into()
        )));

        let in_task = volume_env(&mounts, true);
        assert!(in_task.contains(&("LEGION_VOLUME_DATA".into(), "/var/lib/db".into())));
        assert!(in_task.contains(&("LEGION_STORAGE_DIR".into(), "/var/lib/db".into())));
    }
}
