//! The volume contract test harness.
//!
//! One suite proves a driver — built-in or exec — honors the contract:
//!
//! ```text
//! capabilities → mount → write → unmount → mount → read back → unmount → destroy
//! ```
//!
//! plus a double-call idempotency check on every operation, because "already
//! done is a success" is the rule the whole retry model rests on.
//!
//! The same harness backs `legion volume driver test <name>`, so a driver
//! author runs exactly what CI runs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::prelude::*;
use crate::volumes::{
    VolumeDriver, VolumeMode, VolumeNodeRef, VolumeOp, VolumeRef, VolumeRequest, VOLUME_API_VERSION,
};

/// One checked behavior.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConformanceCheck {
    pub name: String,
    pub passed: bool,
    /// Why it failed, or what it observed when it passed.
    pub detail: String,
    /// True when the check did not apply (an ephemeral driver is not expected
    /// to preserve data across an unmount, for instance).
    #[serde(default)]
    pub skipped: bool,
}

impl ConformanceCheck {
    fn pass(name: &str, detail: impl Into<String>) -> Self {
        ConformanceCheck {
            name: name.to_string(),
            passed: true,
            detail: detail.into(),
            skipped: false,
        }
    }

    fn fail(name: &str, detail: impl Into<String>) -> Self {
        ConformanceCheck {
            name: name.to_string(),
            passed: false,
            detail: detail.into(),
            skipped: false,
        }
    }

    fn skip(name: &str, detail: impl Into<String>) -> Self {
        ConformanceCheck {
            name: name.to_string(),
            passed: true,
            detail: detail.into(),
            skipped: true,
        }
    }
}

/// The outcome of a full run against one plugin.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConformanceReport {
    pub driver: String,
    pub passed: bool,
    pub checks: Vec<ConformanceCheck>,
}

impl ConformanceReport {
    pub fn failures(&self) -> Vec<&ConformanceCheck> {
        self.checks.iter().filter(|check| !check.passed).collect()
    }
}

/// Run the contract suite against `plugin`, using `scratch` as the node data
/// directory and `params` as the jobspec params (`server`/`export` for `nfs`,
/// `name` for `host`, …).
///
/// Every step is recorded rather than short-circuiting on the first failure,
/// so a plugin author sees the whole picture in one run. Steps that cannot run
/// because an earlier one failed are reported as failures with that reason.
pub fn run(
    driver: &dyn VolumeDriver,
    scratch: &Path,
    params: BTreeMap<String, Value>,
    mode: VolumeMode,
) -> ConformanceReport {
    let mut checks = Vec::new();
    let volume_id = format!("conformance-{}", crate::util::random_string(12));

    let caps = driver.capabilities();
    if caps.api_version != VOLUME_API_VERSION {
        checks.push(ConformanceCheck::fail(
            "capabilities.apiVersion",
            format!(
                "driver speaks contract v{}, host speaks v{VOLUME_API_VERSION}",
                caps.api_version
            ),
        ));
    } else {
        checks.push(ConformanceCheck::pass(
            "capabilities.apiVersion",
            format!("v{VOLUME_API_VERSION}"),
        ));
    }
    checks.push(ConformanceCheck::pass(
        "capabilities",
        format!(
            "nodeLocal={} ephemeral={} accessModes=[{}] volumeModes=[{}]",
            caps.node_local,
            caps.ephemeral,
            caps.access_modes.join(", "),
            caps.volume_modes
                .iter()
                .map(|mode| mode.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ));

    if !caps.serves_mode(mode) {
        checks.push(ConformanceCheck::fail(
            "capabilities.volumeModes",
            format!("driver does not serve {mode} mode; run with --mode on a mode it supports"),
        ));
        return finish(driver.name(), checks);
    }

    if !caps.available {
        checks.push(ConformanceCheck::fail(
            "capabilities.available",
            format!(
                "driver reports itself unusable on this node: {}",
                caps.detail
            ),
        ));
        return finish(driver.name(), checks);
    }
    let missing = caps.missing_params(&params);
    if !missing.is_empty() {
        checks.push(ConformanceCheck::fail(
            "params",
            format!(
                "missing required params: {} (pass them with --param key=value)",
                missing.join(", ")
            ),
        ));
        return finish(driver.name(), checks);
    }

    let request = |op: VolumeOp| -> VolumeRequest {
        VolumeRequest {
            api_version: VOLUME_API_VERSION,
            op,
            mode,
            volume: VolumeRef {
                id: volume_id.clone(),
                name: "data".to_string(),
                driver: driver.name().to_string(),
                attrs: BTreeMap::new(),
                read_only: false,
            },
            params: params.clone(),
            instance: None,
            alloc_id: 0,
            node: VolumeNodeRef {
                id: "conformance".to_string(),
                data_dir: scratch.to_path_buf(),
                volume_dir: scratch.join("volumes"),
            },
            snapshot: None,
        }
    };

    // --- mount ---
    let mount_path: PathBuf = match driver.mount(&request(VolumeOp::Mount)) {
        Ok(result) => {
            if let Err(err) = result.validate_for(mode, driver.name()) {
                checks.push(ConformanceCheck::fail("mount", format!("{err:#}")));
                return finish(driver.name(), checks);
            }
            let path = result.resolved_path().cloned().expect("validated above");
            checks.push(ConformanceCheck::pass(
                "mount",
                format!(
                    "{}{}",
                    path.display(),
                    match (mode, result.fs.as_deref()) {
                        (VolumeMode::Block, Some(fs)) => format!(" (fs={fs})"),
                        _ => String::new(),
                    }
                ),
            ));
            // A driver that just created the storage must say so — `fresh` is
            // what lets a consumer tell first-boot-initialize from reattach.
            checks.push(if result.fresh {
                ConformanceCheck::pass("mount.fresh", "first mount reported fresh: true")
            } else {
                ConformanceCheck::fail(
                    "mount.fresh",
                    "first mount of a brand-new volume reported fresh: false",
                )
            });
            path
        }
        Err(err) => {
            checks.push(ConformanceCheck::fail("mount", format!("{err:#}")));
            return finish(driver.name(), checks);
        }
    };

    // --- mount is idempotent ---
    match driver.mount(&request(VolumeOp::Mount)) {
        Ok(again) if again.resolved_path() == Some(&mount_path) => {
            checks.push(ConformanceCheck::pass(
                "mount.idempotent",
                "second mount returned the same path",
            ));
            // For block mode this is the load-bearing one: a driver that
            // reformats on remount destroys the volume it was asked to reattach.
            checks.push(if again.fresh {
                ConformanceCheck::fail(
                    "mount.no_reformat",
                    "second mount reported fresh: true — the driver re-created \
                     (and for block mode, reformatted) an existing volume",
                )
            } else {
                ConformanceCheck::pass("mount.no_reformat", "remount reattached without recreating")
            });
        }
        Ok(again) => checks.push(ConformanceCheck::fail(
            "mount.idempotent",
            format!(
                "second mount returned {:?} instead of {}",
                again.resolved_path(),
                mount_path.display()
            ),
        )),
        Err(err) => checks.push(ConformanceCheck::fail(
            "mount.idempotent",
            format!("second mount failed: {err:#}"),
        )),
    }

    // --- writability + persistence ---
    //
    // The two modes prove the same property — "the bytes I put there are still
    // there after a reattach" — but they cannot prove it the same way.
    //
    // Filesystem mode writes a probe file. Block mode cannot: Legion hands out
    // the device and the *guest* owns the filesystem on it, and mounting it
    // here would need privileges the harness deliberately does not require. So
    // block mode fingerprints the image instead, which tests exactly what the
    // contract promises: the same device, with the same content, comes back.
    let fingerprint_before = match mode {
        VolumeMode::Filesystem => {
            let probe = mount_path.join(".legion-conformance");
            match std::fs::write(&probe, PROBE_PAYLOAD) {
                Ok(()) => {
                    checks.push(ConformanceCheck::pass("write", probe.display().to_string()));
                    None
                }
                Err(err) => {
                    checks.push(ConformanceCheck::fail(
                        "write",
                        format!("could not write to the mount path: {err}"),
                    ));
                    let _ = driver.destroy(&request(VolumeOp::Destroy));
                    return finish(driver.name(), checks);
                }
            }
        }
        VolumeMode::Block => match device_fingerprint(&mount_path) {
            Ok(Some(digest)) => {
                checks.push(ConformanceCheck::pass(
                    "write",
                    "fingerprinted the device image (the guest owns the filesystem on it)",
                ));
                Some(digest)
            }
            Ok(None) => {
                checks.push(ConformanceCheck::skip(
                    "write",
                    "device is not a regular file; skipping the content fingerprint rather than \
                     reading a live block device",
                ));
                None
            }
            Err(err) => {
                checks.push(ConformanceCheck::fail(
                    "write",
                    format!("could not read the device: {err}"),
                ));
                let _ = driver.destroy(&request(VolumeOp::Destroy));
                return finish(driver.name(), checks);
            }
        },
    };

    // --- unmount / remount / data survives ---
    match driver.unmount(&request(VolumeOp::Unmount)) {
        Ok(()) => checks.push(ConformanceCheck::pass("unmount", "released")),
        Err(err) => checks.push(ConformanceCheck::fail("unmount", format!("{err:#}"))),
    }
    match driver.unmount(&request(VolumeOp::Unmount)) {
        Ok(()) => checks.push(ConformanceCheck::pass(
            "unmount.idempotent",
            "second unmount succeeded",
        )),
        Err(err) => checks.push(ConformanceCheck::fail(
            "unmount.idempotent",
            format!("second unmount failed: {err:#}"),
        )),
    }

    match driver.mount(&request(VolumeOp::Mount)) {
        Ok(remounted) => {
            if caps.ephemeral {
                checks.push(ConformanceCheck::skip(
                    "persistence",
                    "driver declares itself ephemeral; data is not expected to survive unmount",
                ));
            } else {
                checks.push(verify_persistence(
                    mode,
                    &remounted,
                    fingerprint_before.as_deref(),
                ));
            }
        }
        Err(err) => checks.push(ConformanceCheck::fail(
            "remount",
            format!("could not remount after unmount: {err:#}"),
        )),
    }

    let _ = driver.unmount(&request(VolumeOp::Unmount));

    // --- snapshot / restore / clone (optional ops) ---
    if caps.ephemeral {
        checks.push(ConformanceCheck::skip(
            "snapshot",
            "driver declares itself ephemeral; there is nothing durable to snapshot",
        ));
    } else if caps.supports(VolumeOp::Snapshot) {
        snapshot_checks(driver, &caps, mode, &request, &mut checks);
    } else {
        checks.push(ConformanceCheck::skip(
            "snapshot",
            "driver does not advertise `snapshot` in capabilities.ops",
        ));
    }

    // --- destroy ---
    match driver.destroy(&request(VolumeOp::Destroy)) {
        Ok(()) => checks.push(ConformanceCheck::pass("destroy", "removed")),
        Err(err) => checks.push(ConformanceCheck::fail("destroy", format!("{err:#}"))),
    }
    match driver.destroy(&request(VolumeOp::Destroy)) {
        Ok(()) => checks.push(ConformanceCheck::pass(
            "destroy.idempotent",
            "second destroy succeeded",
        )),
        Err(err) => checks.push(ConformanceCheck::fail(
            "destroy.idempotent",
            format!("second destroy failed: {err:#}"),
        )),
    }

    finish(driver.name(), checks)
}

const PROBE_PAYLOAD: &str = "legion volume conformance probe\n";

/// Mount, read what the volume holds, unmount. Filesystem mode reads the
/// probe file; block mode fingerprints the image. `None` when the content
/// cannot be compared (a real block device), which callers report as skipped.
fn observe(
    driver: &dyn VolumeDriver,
    mode: VolumeMode,
    request: &VolumeRequest,
) -> R<Option<String>> {
    let mounted = driver.mount(&VolumeRequest {
        op: VolumeOp::Mount,
        ..request.clone()
    })?;
    let path = mounted
        .resolved_path()
        .cloned()
        .context("mount returned no path")?;
    let seen = match mode {
        VolumeMode::Filesystem => Some(
            std::fs::read_to_string(path.join(".legion-conformance"))
                .unwrap_or_else(|_| "<missing>".to_string()),
        ),
        VolumeMode::Block => device_fingerprint(&path)?,
    };
    driver.unmount(&VolumeRequest {
        op: VolumeOp::Unmount,
        ..request.clone()
    })?;
    Ok(seen)
}

/// Mount, overwrite the volume's content with `marker`, unmount. Block mode
/// stamps the first bytes of the image, which ruins the filesystem on it,
/// which is fine for a volume the harness is about to destroy.
fn mutate(
    driver: &dyn VolumeDriver,
    mode: VolumeMode,
    request: &VolumeRequest,
    marker: &str,
) -> R<()> {
    let mounted = driver.mount(&VolumeRequest {
        op: VolumeOp::Mount,
        ..request.clone()
    })?;
    let path = mounted
        .resolved_path()
        .cloned()
        .context("mount returned no path")?;
    match mode {
        VolumeMode::Filesystem => std::fs::write(path.join(".legion-conformance"), marker)?,
        VolumeMode::Block => {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
            file.write_all(marker.as_bytes())?;
        }
    }
    driver.unmount(&VolumeRequest {
        op: VolumeOp::Unmount,
        ..request.clone()
    })?;
    Ok(())
}

/// The optional-op suite: snapshot, mutate, restore, and the contents come
/// back; every op called twice to prove idempotency; clone produces an
/// independent copy; delete removes the snapshot.
fn snapshot_checks(
    driver: &dyn VolumeDriver,
    caps: &crate::volumes::VolumeCapabilities,
    mode: VolumeMode,
    request: &dyn Fn(VolumeOp) -> VolumeRequest,
    checks: &mut Vec<ConformanceCheck>,
) {
    let base = request(VolumeOp::Snapshot);
    let wanted = crate::structs::checkpoint::VolumeSnapshotRef {
        id: format!("conformance-snap-{}", crate::util::random_string(8)),
        volume_id: base.volume.id.clone(),
        driver: driver.name().to_string(),
        mode,
        attrs: BTreeMap::new(),
        size: 0,
    };

    let before = match observe(driver, mode, &base) {
        Ok(Some(seen)) => seen,
        Ok(None) => {
            checks.push(ConformanceCheck::skip(
                "snapshot",
                "device is not a regular file; cannot compare snapshot content",
            ));
            return;
        }
        Err(err) => {
            checks.push(ConformanceCheck::fail(
                "snapshot",
                format!("could not read the volume before snapshotting: {err:#}"),
            ));
            return;
        }
    };

    let taken = match driver.snapshot(&base.clone().with_snapshot(wanted.clone())) {
        Ok(taken) if taken.id == wanted.id => {
            checks.push(ConformanceCheck::pass(
                "snapshot",
                match taken.attrs.get("method").and_then(Value::as_str) {
                    Some(method) => {
                        format!("snapshot {} ({} bytes, {method})", taken.id, taken.size)
                    }
                    None => format!("snapshot {} ({} bytes)", taken.id, taken.size),
                },
            ));
            taken
        }
        Ok(taken) => {
            checks.push(ConformanceCheck::fail(
                "snapshot",
                format!(
                    "asked for snapshot {}, driver returned {}",
                    wanted.id, taken.id
                ),
            ));
            return;
        }
        Err(err) => {
            checks.push(ConformanceCheck::fail("snapshot", format!("{err:#}")));
            return;
        }
    };
    match driver.snapshot(&base.clone().with_snapshot(wanted.clone())) {
        Ok(again) if again == taken => checks.push(ConformanceCheck::pass(
            "snapshot.idempotent",
            "second snapshot to the same id returned the same snapshot",
        )),
        Ok(again) => checks.push(ConformanceCheck::fail(
            "snapshot.idempotent",
            format!("second snapshot returned {again:?}, first returned {taken:?}"),
        )),
        Err(err) => checks.push(ConformanceCheck::fail(
            "snapshot.idempotent",
            format!("second snapshot failed: {err:#}"),
        )),
    }

    if let Err(err) = mutate(driver, mode, &base, "mutated after snapshot\n") {
        checks.push(ConformanceCheck::fail(
            "snapshot.mutate",
            format!("could not change the volume after snapshotting: {err:#}"),
        ));
        return;
    }

    if caps.supports(VolumeOp::Restore) {
        let restore = VolumeRequest {
            op: VolumeOp::Restore,
            ..base.clone()
        }
        .with_snapshot(taken.clone());
        match driver.restore(&restore) {
            Ok(()) => match observe(driver, mode, &base) {
                Ok(Some(after)) if after == before => checks.push(ConformanceCheck::pass(
                    "restore",
                    "the volume came back with its snapshot-time content",
                )),
                Ok(_) => checks.push(ConformanceCheck::fail(
                    "restore",
                    "the volume does not match its snapshot after restore",
                )),
                Err(err) => checks.push(ConformanceCheck::fail("restore", format!("{err:#}"))),
            },
            Err(err) => checks.push(ConformanceCheck::fail("restore", format!("{err:#}"))),
        }
        match driver
            .restore(&restore)
            .and_then(|()| observe(driver, mode, &base))
        {
            Ok(Some(after)) if after == before => checks.push(ConformanceCheck::pass(
                "restore.idempotent",
                "second restore left the same content",
            )),
            Ok(_) => checks.push(ConformanceCheck::fail(
                "restore.idempotent",
                "second restore changed the content",
            )),
            Err(err) => checks.push(ConformanceCheck::fail(
                "restore.idempotent",
                format!("second restore failed: {err:#}"),
            )),
        }
    } else {
        checks.push(ConformanceCheck::skip(
            "restore",
            "driver does not advertise `restore`",
        ));
    }

    if caps.supports(VolumeOp::Clone) {
        let mut clone = VolumeRequest {
            op: VolumeOp::Clone,
            ..base.clone()
        }
        .with_snapshot(taken.clone());
        clone.volume.id = format!("{}-clone", base.volume.id);
        let first = driver.clone_volume(&clone);
        let second = first.as_ref().ok().map(|()| driver.clone_volume(&clone));
        match (first, observe(driver, mode, &clone)) {
            (Ok(()), Ok(Some(seen))) if seen == before => checks.push(ConformanceCheck::pass(
                "clone",
                "the clone holds the snapshot-time content",
            )),
            (Ok(()), Ok(_)) => checks.push(ConformanceCheck::fail(
                "clone",
                "the clone does not match the snapshot",
            )),
            (Ok(()), Err(err)) | (Err(err), _) => {
                checks.push(ConformanceCheck::fail("clone", format!("{err:#}")))
            }
        }
        match second {
            Some(Ok(())) => checks.push(ConformanceCheck::pass(
                "clone.idempotent",
                "second clone into the same volume succeeded",
            )),
            Some(Err(err)) => checks.push(ConformanceCheck::fail(
                "clone.idempotent",
                format!("second clone failed: {err:#}"),
            )),
            None => {}
        }
        let _ = driver.destroy(&VolumeRequest {
            op: VolumeOp::Destroy,
            ..clone
        });
    } else {
        checks.push(ConformanceCheck::skip(
            "clone",
            "driver does not advertise `clone`",
        ));
    }

    if caps.supports(VolumeOp::DeleteSnapshot) {
        let delete = VolumeRequest {
            op: VolumeOp::DeleteSnapshot,
            ..base.clone()
        }
        .with_snapshot(taken.clone());
        for name in ["snapshot-delete", "snapshot-delete.idempotent"] {
            match driver.delete_snapshot(&delete) {
                Ok(()) => checks.push(ConformanceCheck::pass(name, "deleted")),
                Err(err) => checks.push(ConformanceCheck::fail(name, format!("{err:#}"))),
            }
        }
    } else {
        checks.push(ConformanceCheck::skip(
            "snapshot-delete",
            "driver does not advertise `snapshot-delete`; the snapshot is left behind",
        ));
    }
}

/// SHA-256 of a block volume's backing image. `None` when the device is not a
/// regular file (a real `/dev/…` node), which the caller reports as skipped
/// rather than reading a live device.
fn device_fingerprint(device: &Path) -> R<Option<String>> {
    use sha2::{Digest, Sha256};
    if !device
        .metadata()
        .map(|meta| meta.is_file())
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let bytes = std::fs::read(device)
        .with_context(|| format!("failed to read device image {}", device.display()))?;
    Ok(Some(format!("{:x}", Sha256::digest(&bytes))))
}

/// Did the volume come back with the content it had?
fn verify_persistence(
    mode: VolumeMode,
    remounted: &crate::volumes::MountResult,
    fingerprint_before: Option<&str>,
) -> ConformanceCheck {
    let Some(path) = remounted.resolved_path() else {
        return ConformanceCheck::fail("persistence", "remount returned no path");
    };
    match mode {
        VolumeMode::Filesystem => match std::fs::read_to_string(path.join(".legion-conformance")) {
            Ok(read_back) if read_back == PROBE_PAYLOAD => {
                ConformanceCheck::pass("persistence", "data survived unmount/remount")
            }
            Ok(_) => {
                ConformanceCheck::fail("persistence", "probe file came back with different content")
            }
            Err(err) => ConformanceCheck::fail(
                "persistence",
                format!("probe file missing after remount: {err}"),
            ),
        },
        VolumeMode::Block => {
            let Some(before) = fingerprint_before else {
                return ConformanceCheck::skip(
                    "persistence",
                    "no fingerprint was taken (device is not a regular file)",
                );
            };
            match device_fingerprint(path) {
                Ok(Some(after)) if after == before => ConformanceCheck::pass(
                    "persistence",
                    "device image is byte-identical after unmount/remount (no reformat)",
                ),
                Ok(Some(_)) => ConformanceCheck::fail(
                    "persistence",
                    "device image changed across unmount/remount — the driver reformatted or \
                     re-created a volume it was asked to reattach",
                ),
                Ok(None) => ConformanceCheck::skip(
                    "persistence",
                    "device is not a regular file; cannot compare content",
                ),
                Err(err) => ConformanceCheck::fail(
                    "persistence",
                    format!("could not re-read device: {err}"),
                ),
            }
        }
    }
}

fn finish(driver: &str, checks: Vec<ConformanceCheck>) -> ConformanceReport {
    ConformanceReport {
        passed: checks.iter().all(|check| check.passed),
        driver: driver.to_string(),
        checks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::builtins::LocalDirDriver;

    fn temp_root(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("legion-conformance-{label}-{unique}"));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn local_dir_passes_the_contract_suite() {
        let root = temp_root("local-dir");
        let driver = LocalDirDriver::new(root.join("volumes"));
        let report = run(&driver, &root, BTreeMap::new(), VolumeMode::Filesystem);
        assert!(
            report.passed,
            "local-dir failed conformance: {:?}",
            report.failures()
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn reports_missing_required_params_instead_of_failing_at_mount() {
        let root = temp_root("host-params");
        let driver =
            crate::volumes::builtins::HostDriver::new(crate::volumes::VolumeRuntimeConfig {
                volume_root: root.join("volumes"),
                data_dir: root.clone(),
                host_volumes: {
                    let mut map = HashMap::new();
                    map.insert(
                        "models".to_string(),
                        crate::volumes::HostVolumeConfig {
                            path: root.join("models").display().to_string(),
                            device: None,
                            fs: None,
                            read_only: false,
                            create: true,
                        },
                    );
                    map
                },
                plugin_dir: root.join("plugins"),
            });
        let report = run(&driver, &root, BTreeMap::new(), VolumeMode::Filesystem);
        assert!(!report.passed);
        let failure = report.failures()[0];
        assert_eq!(failure.name, "params");
        assert!(failure.detail.contains("name"), "{}", failure.detail);
        std::fs::remove_dir_all(&root).ok();
    }
}
