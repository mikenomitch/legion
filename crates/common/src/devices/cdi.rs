//! Container edits in the shape of CDI (the Container Device Interface,
//! `cncf-tags/container-device-interface`): what a task needs to use a
//! device — env, device nodes, mounts, hooks — and how they land in an OCI
//! bundle.
//!
//! Two sources produce them: a device plugin's `allocate` reply
//! (`containerEdits`), and CDI spec files under the worker's
//! `devices.cdiSpecDirs` (`/etc/cdi`, `/var/run/cdi`), resolved by fully
//! qualified device name (`nvidia.com/gpu=GPU-…`), which an instance's `cdi`
//! attribute or the plugin's `cdiDevices` reply names. Only JSON spec files
//! are read; `nvidia-ctk cdi generate --format=json` writes one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::prelude::*;

/// The spec dirs a worker reads when its config names none: CDI's own
/// defaults.
pub const DEFAULT_SPEC_DIRS: [&str; 2] = ["/etc/cdi", "/var/run/cdi"];

/// No edits, for a bundle built for a task with no devices.
pub static NO_EDITS: ContainerEdits = ContainerEdits {
    env: Vec::new(),
    device_nodes: Vec::new(),
    mounts: Vec::new(),
    hooks: Vec::new(),
};

/// CDI `containerEdits`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerEdits {
    /// `KEY=value` entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub device_nodes: Vec<DeviceNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hooks: Vec<Hook>,
}

/// CDI `deviceNode`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceNode {
    /// Path inside the container.
    pub path: String,
    /// Path on the host; defaults to `path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_path: Option<String>,
    /// `c` (character) or `b` (block); read from the host when absent.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub major: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minor: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_mode: Option<u32>,
    /// cgroup access: any of `r`, `w`, `m`. Default `rwm`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
}

/// CDI `mount`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mount {
    pub host_path: String,
    pub container_path: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

/// CDI `hook`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hook {
    /// The OCI hook point: `prestart`, `createRuntime`, `createContainer`,
    /// `startContainer`, `poststart`, or `poststop`.
    pub hook_name: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i64>,
}

const HOOK_NAMES: [&str; 6] = [
    "prestart",
    "createRuntime",
    "createContainer",
    "startContainer",
    "poststart",
    "poststop",
];

impl ContainerEdits {
    pub fn is_empty(&self) -> bool {
        self.env.is_empty()
            && self.device_nodes.is_empty()
            && self.mounts.is_empty()
            && self.hooks.is_empty()
    }

    /// Append `other`'s edits. A device node or mount already listed (same
    /// container path) is not listed twice; env is merged by key, later
    /// wins.
    pub fn merge(&mut self, other: ContainerEdits) {
        for entry in other.env {
            self.set_env(entry);
        }
        for node in other.device_nodes {
            if !self.device_nodes.iter().any(|n| n.path == node.path) {
                self.device_nodes.push(node);
            }
        }
        for mount in other.mounts {
            if !self
                .mounts
                .iter()
                .any(|m| m.container_path == mount.container_path)
            {
                self.mounts.push(mount);
            }
        }
        for hook in other.hooks {
            if !self.hooks.contains(&hook) {
                self.hooks.push(hook);
            }
        }
    }

    /// Set one `KEY=value` entry, replacing any earlier value for KEY.
    pub fn set_env(&mut self, entry: String) {
        let key = entry.split('=').next().unwrap_or_default().to_string();
        self.env
            .retain(|existing| existing.split('=').next().unwrap_or_default() != key);
        self.env.push(entry);
    }

    /// The env entries as a map (`KEY=value` without `=` is `KEY=""`).
    pub fn env_map(&self) -> BTreeMap<String, String> {
        self.env
            .iter()
            .map(|entry| match entry.split_once('=') {
                Some((key, value)) => (key.to_string(), value.to_string()),
                None => (entry.clone(), String::new()),
            })
            .collect()
    }

    /// Reject what Legion will not apply: relative paths, unknown hook
    /// points, env entries without a key.
    pub fn validate(&self) -> R<()> {
        for entry in &self.env {
            if entry.split('=').next().is_none_or(str::is_empty) {
                bail!("container edit env entry {entry:?} has no name");
            }
        }
        for node in &self.device_nodes {
            if !node.path.starts_with('/') {
                bail!("device node path {:?} must be absolute", node.path);
            }
            if let Some(host) = &node.host_path {
                if !host.starts_with('/') {
                    bail!("device node hostPath {host:?} must be absolute");
                }
            }
            if let Some(kind) = &node.kind {
                if kind != "c" && kind != "b" {
                    bail!(
                        "device node {} type must be c or b, got {kind:?}",
                        node.path
                    );
                }
            }
        }
        for mount in &self.mounts {
            if !mount.container_path.starts_with('/') || !mount.host_path.starts_with('/') {
                bail!(
                    "device mount {} -> {} must use absolute paths",
                    mount.host_path,
                    mount.container_path
                );
            }
        }
        for hook in &self.hooks {
            if !HOOK_NAMES.contains(&hook.hook_name.as_str()) {
                bail!(
                    "device hook {} has unknown hookName {:?} (one of {})",
                    hook.path,
                    hook.hook_name,
                    HOOK_NAMES.join(", ")
                );
            }
            if !hook.path.starts_with('/') {
                bail!("device hook path {:?} must be absolute", hook.path);
            }
        }
        Ok(())
    }

    /// Fill each device node's type, major, and minor from the host node
    /// when the edit left them out (CDI allows that; OCI needs them).
    pub fn resolve_device_nodes(&mut self) -> R<()> {
        for node in &mut self.device_nodes {
            if node.kind.is_some() && node.major.is_some() && node.minor.is_some() {
                continue;
            }
            let host = node.host_path.clone().unwrap_or_else(|| node.path.clone());
            let (kind, major, minor) = stat_device(Path::new(&host))?;
            node.kind.get_or_insert(kind);
            node.major.get_or_insert(major);
            node.minor.get_or_insert(minor);
        }
        Ok(())
    }

    /// Apply the edits to a generated OCI `config.json`: env into
    /// `process.env` (replacing a same-named entry), each device node into
    /// `linux.devices` plus an allow rule in `linux.resources.devices` after
    /// the deny-all rule, mounts into `mounts`, hooks into `hooks`.
    pub fn apply_to_oci_spec(&self, spec: &mut Value) {
        if self.is_empty() {
            return;
        }
        if let Some(env) = spec
            .pointer_mut("/process/env")
            .and_then(Value::as_array_mut)
        {
            for entry in &self.env {
                let key = entry.split('=').next().unwrap_or_default();
                env.retain(|existing| {
                    existing
                        .as_str()
                        .and_then(|e| e.split('=').next())
                        .is_none_or(|k| k != key)
                });
                env.push(Value::String(entry.clone()));
            }
        }
        for node in &self.device_nodes {
            let kind = node.kind.clone().unwrap_or_else(|| "c".to_string());
            let mut device = json!({ "path": node.path, "type": kind });
            if let Some(major) = node.major {
                device["major"] = json!(major);
            }
            if let Some(minor) = node.minor {
                device["minor"] = json!(minor);
            }
            if let Some(mode) = node.file_mode {
                device["fileMode"] = json!(mode);
            }
            if let Some(uid) = node.uid {
                device["uid"] = json!(uid);
            }
            if let Some(gid) = node.gid {
                device["gid"] = json!(gid);
            }
            push_array(spec, &["linux", "devices"], device);
            let mut rule = json!({
                "allow": true,
                "type": kind,
                "access": node.permissions.clone().unwrap_or_else(|| "rwm".to_string()),
            });
            if let Some(major) = node.major {
                rule["major"] = json!(major);
            }
            if let Some(minor) = node.minor {
                rule["minor"] = json!(minor);
            }
            push_array(spec, &["linux", "resources", "devices"], rule);
        }
        for mount in &self.mounts {
            let options = if mount.options.is_empty() {
                vec!["rbind".to_string(), "rprivate".to_string()]
            } else {
                mount.options.clone()
            };
            push_array(
                spec,
                &["mounts"],
                json!({
                    "destination": mount.container_path,
                    "source": mount.host_path,
                    "type": mount.kind.clone().unwrap_or_else(|| "bind".to_string()),
                    "options": options,
                }),
            );
        }
        for hook in &self.hooks {
            let mut entry = json!({ "path": hook.path });
            if !hook.args.is_empty() {
                entry["args"] = json!(hook.args);
            }
            if !hook.env.is_empty() {
                entry["env"] = json!(hook.env);
            }
            if let Some(timeout) = hook.timeout {
                entry["timeout"] = json!(timeout);
            }
            push_array(spec, &["hooks", hook.hook_name.as_str()], entry);
        }
    }
}

/// Append to the array at `path`, creating objects and the array as needed.
fn push_array(spec: &mut Value, path: &[&str], item: Value) {
    let mut cursor = spec;
    for (depth, key) in path.iter().enumerate() {
        if !cursor.is_object() {
            *cursor = json!({});
        }
        let last = depth + 1 == path.len();
        let map = cursor.as_object_mut().expect("object");
        cursor =
            map.entry(key.to_string())
                .or_insert_with(|| if last { json!([]) } else { json!({}) });
    }
    if !cursor.is_array() {
        *cursor = json!([]);
    }
    cursor.as_array_mut().expect("array").push(item);
}

/// `(type, major, minor)` of a device node on this host.
#[cfg(unix)]
fn stat_device(path: &Path) -> R<(String, i64, i64)> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let meta = std::fs::metadata(path)
        .with_context(|| format!("device node {} is not on this node", path.display()))?;
    let kind = if meta.file_type().is_char_device() {
        "c"
    } else if meta.file_type().is_block_device() {
        "b"
    } else {
        bail!("{} is not a device node", path.display());
    };
    let rdev = meta.rdev();
    let major = libc::major(rdev) as i64;
    let minor = libc::minor(rdev) as i64;
    Ok((kind.to_string(), major, minor))
}

#[cfg(not(unix))]
fn stat_device(path: &Path) -> R<(String, i64, i64)> {
    bail!(
        "device node {} cannot be read on this platform",
        path.display()
    )
}

/// One CDI spec file.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    /// `vendor.com/class`.
    kind: String,
    #[serde(default)]
    devices: Vec<SpecDevice>,
    #[serde(default)]
    container_edits: ContainerEdits,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpecDevice {
    name: String,
    #[serde(default)]
    container_edits: ContainerEdits,
}

/// The CDI specs under some dirs, by kind.
#[derive(Debug, Clone, Default)]
pub struct SpecRegistry {
    specs: Vec<(PathBuf, Spec)>,
}

impl SpecRegistry {
    /// Read every `*.json` spec under `dirs` (missing dirs are skipped; a
    /// file that does not parse is logged and skipped, as the CDI reference
    /// does).
    pub fn load(dirs: &[PathBuf]) -> Self {
        let mut specs = Vec::new();
        for dir in dirs {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            let mut paths: Vec<PathBuf> = entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
                .collect();
            paths.sort();
            for path in paths {
                match std::fs::read(&path)
                    .map_err(anyhow::Error::from)
                    .and_then(|bytes| Ok(serde_json::from_slice::<Spec>(&bytes)?))
                {
                    Ok(spec) => specs.push((path, spec)),
                    Err(err) => warn!(
                        path = %path.display(),
                        error = %format!("{err:#}"),
                        "Skipping a CDI spec file that does not parse"
                    ),
                }
            }
        }
        SpecRegistry { specs }
    }

    /// The edits for fully qualified device names (`vendor.com/class=name`):
    /// each device's own edits, plus its spec's spec-level edits once.
    pub fn resolve(&self, names: &[String]) -> R<ContainerEdits> {
        let mut edits = ContainerEdits::default();
        let mut used_specs: Vec<usize> = Vec::new();
        for name in names {
            let (kind, device) = name.split_once('=').with_context(|| {
                format!("CDI device name {name:?} is not of the form vendor.com/class=name")
            })?;
            let found = self
                .specs
                .iter()
                .enumerate()
                .find_map(|(index, (_, spec))| {
                    (spec.kind == kind)
                        .then(|| spec.devices.iter().find(|d| d.name == device))
                        .flatten()
                        .map(|d| (index, d))
                });
            let Some((index, found)) = found else {
                bail!(
                    "CDI device {name} is not in any spec under the worker's CDI spec dirs; \
                     generate one (for NVIDIA: `nvidia-ctk cdi generate --format=json \
                     --output=/etc/cdi/nvidia.json`)"
                );
            };
            if !used_specs.contains(&index) {
                used_specs.push(index);
                edits.merge(self.specs[index].1.container_edits.clone());
            }
            edits.merge(found.container_edits.clone());
        }
        Ok(edits)
    }
}

#[cfg(test)]
mod cdi_tests {
    use super::*;

    fn edits() -> ContainerEdits {
        ContainerEdits {
            env: vec!["NVIDIA_VISIBLE_DEVICES=GPU-a,GPU-b".into()],
            device_nodes: vec![DeviceNode {
                path: "/dev/nvidia0".into(),
                kind: Some("c".into()),
                major: Some(195),
                minor: Some(0),
                ..Default::default()
            }],
            mounts: vec![Mount {
                host_path: "/usr/lib/libcuda.so.1".into(),
                container_path: "/usr/lib/libcuda.so.1".into(),
                kind: None,
                options: vec!["ro".into(), "nosuid".into(), "nodev".into(), "bind".into()],
            }],
            hooks: vec![Hook {
                hook_name: "createContainer".into(),
                path: "/usr/bin/nvidia-ctk".into(),
                args: vec!["nvidia-ctk".into(), "hook".into(), "update-ldcache".into()],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn edits_land_in_the_oci_spec_test() {
        let mut spec = json!({
            "process": { "env": ["PATH=/bin", "NVIDIA_VISIBLE_DEVICES=void"] },
            "mounts": [{ "destination": "/proc" }],
            "linux": { "resources": { "devices": [{ "allow": false, "access": "rwm" }] } },
        });
        edits().apply_to_oci_spec(&mut spec);
        let env: Vec<&str> = spec["process"]["env"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(env, vec!["PATH=/bin", "NVIDIA_VISIBLE_DEVICES=GPU-a,GPU-b"]);
        assert_eq!(
            spec["linux"]["devices"],
            json!([{ "path": "/dev/nvidia0", "type": "c", "major": 195, "minor": 0 }])
        );
        // The deny-all rule stays first; the allow rule follows it.
        assert_eq!(
            spec["linux"]["resources"]["devices"],
            json!([
                { "allow": false, "access": "rwm" },
                { "allow": true, "type": "c", "major": 195, "minor": 0, "access": "rwm" },
            ])
        );
        let mounts = spec["mounts"].as_array().unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1]["destination"], "/usr/lib/libcuda.so.1");
        assert_eq!(mounts[1]["type"], "bind");
        assert_eq!(
            spec["hooks"]["createContainer"][0]["path"],
            "/usr/bin/nvidia-ctk"
        );
    }

    #[test]
    fn empty_edits_leave_the_spec_alone_test() {
        let mut spec = json!({ "process": { "env": [] } });
        let before = spec.clone();
        ContainerEdits::default().apply_to_oci_spec(&mut spec);
        assert_eq!(spec, before);
    }

    #[test]
    fn merge_dedups_and_env_later_wins_test() {
        let mut a = edits();
        let mut b = edits();
        b.env = vec!["NVIDIA_VISIBLE_DEVICES=GPU-c".into(), "X=1".into()];
        a.merge(b);
        assert_eq!(a.device_nodes.len(), 1);
        assert_eq!(a.mounts.len(), 1);
        assert_eq!(a.hooks.len(), 1);
        let env = a.env_map();
        assert_eq!(env["NVIDIA_VISIBLE_DEVICES"], "GPU-c");
        assert_eq!(env["X"], "1");
    }

    #[test]
    fn validation_refuses_relative_paths_and_unknown_hooks_test() {
        let mut bad = edits();
        bad.device_nodes[0].path = "dev/nvidia0".into();
        assert!(bad.validate().is_err());
        let mut bad = edits();
        bad.hooks[0].hook_name = "whenever".into();
        assert!(bad.validate().unwrap_err().to_string().contains("hookName"));
        assert!(edits().validate().is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn device_nodes_are_read_from_the_host_when_left_out_test() -> R<()> {
        let mut edits = ContainerEdits {
            device_nodes: vec![DeviceNode {
                path: "/dev/null".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        edits.resolve_device_nodes()?;
        let node = &edits.device_nodes[0];
        assert_eq!(node.kind.as_deref(), Some("c"));
        assert_eq!((node.major, node.minor), (Some(1), Some(3)));
        let mut missing = ContainerEdits {
            device_nodes: vec![DeviceNode {
                path: "/dev/legion-no-such-device".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(missing.resolve_device_nodes().is_err());
        Ok(())
    }

    #[test]
    fn cdi_specs_resolve_by_qualified_name_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("vendor.json"),
            serde_json::to_vec(&json!({
                "cdiVersion": "0.6.0",
                "kind": "example.com/fpga",
                "containerEdits": { "env": ["FPGA_DRIVER=1"] },
                "devices": [
                    { "name": "0", "containerEdits": {
                        "deviceNodes": [{ "path": "/dev/fpga0", "type": "c", "major": 240, "minor": 0 }] } },
                    { "name": "1", "containerEdits": {
                        "deviceNodes": [{ "path": "/dev/fpga1", "type": "c", "major": 240, "minor": 1 }] } },
                ],
            }))?,
        )?;
        std::fs::write(dir.path().join("broken.json"), b"not json")?;
        std::fs::write(dir.path().join("ignored.yaml"), b"kind: x")?;
        let registry = SpecRegistry::load(&[dir.path().to_path_buf(), "/nonexistent".into()]);
        let edits =
            registry.resolve(&["example.com/fpga=0".into(), "example.com/fpga=1".into()])?;
        assert_eq!(edits.env, vec!["FPGA_DRIVER=1".to_string()]);
        assert_eq!(edits.device_nodes.len(), 2);
        let err = registry
            .resolve(&["example.com/fpga=9".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("not in any spec"), "{err}");
        assert!(registry.resolve(&["no-equals".into()]).is_err());
        Ok(())
    }
}
