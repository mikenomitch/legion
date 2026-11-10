//! Device plugins: the worker half of the device model (docs/devices.md,
//! "Device plugins").
//!
//! A device plugin is any command. Legion runs it once per operation with
//! one JSON request on stdin and reads one JSON reply from stdout; a
//! non-zero exit, a timeout, or a reply that does not parse is an error
//! (the convention the keyring and cluster-CA `exec` providers use, through
//! [`crate::util::run_json_command`]). Every request carries `protocol`
//! ([`super::PROTOCOL`]), `op`, `plugin` (its name in the worker type), and
//! `config` (its `config` record):
//!
//! ```json
//! {"protocol":"legion-device/1","op":"fingerprint","plugin":"nvidia","config":{}}
//! → {"devices":[{"vendor":"nvidia","type":"gpu","model":"A100-SXM4-80GB",
//!                "attributes":{"memoryMiB":81920},
//!                "instances":[{"id":"GPU-6f2c…","healthy":true,
//!                              "attributes":{"numaNode":0,"peerGroup":"nvlink-0"}}]}]}
//!
//! {"protocol":"legion-device/1","op":"allocate","plugin":"nvidia","config":{},
//!  "device":"nvidia/gpu/A100-SXM4-80GB","ids":["GPU-6f2c…"]}
//! → {"containerEdits":{"env":["CUDA_VISIBLE_DEVICES=GPU-6f2c…"],"deviceNodes":[…]},
//!    "cdiDevices":["nvidia.com/gpu=GPU-6f2c…"]}
//! ```
//!
//! Why exec rather than the dylib ABI or a gRPC socket: a fingerprint runs
//! every 30 seconds and an allocate once per task start, both shell out to
//! vendor tools (`nvidia-smi`), and a crashing plugin must not take the
//! worker down. A process per call costs nothing at that rate and lets a
//! plugin be a 50-line script in any language.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::cdi::{ContainerEdits, SpecRegistry, DEFAULT_SPEC_DIRS};
use super::{DeviceAssignment, DeviceGroup, DeviceReport, PluginStatus, PROTOCOL, STATIC_PLUGIN};
use crate::prelude::*;

/// How long one plugin call may run when the worker type sets no `timeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The worker type's `devices` block. It rides to the worker in the
/// artifacts payload ([`crate::structs::WorkerArtifactsPayload`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerDevicesConfig {
    /// Device plugins, by name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub plugins: BTreeMap<String, DevicePluginConfig>,
    /// Device groups declared by hand, for hardware no plugin reports. Their
    /// `plugin` is `static`, and their instances are always healthy unless
    /// declared otherwise.
    #[serde(default, rename = "static", skip_serializing_if = "Vec::is_empty")]
    pub static_groups: Vec<DeviceGroup>,
    /// Where CDI spec files are read from. Default `["/etc/cdi",
    /// "/var/run/cdi"]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdi_spec_dirs: Option<Vec<String>>,
}

/// One device plugin.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DevicePluginConfig {
    /// The command and its arguments; the request goes on stdin.
    pub command: Vec<String>,
    /// Passed through to the plugin as `config` on every request.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub config: Value,
    /// How long one call may run (humantime). Default `10s`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

impl DevicePluginConfig {
    fn timeout(&self) -> Duration {
        self.timeout
            .as_deref()
            .and_then(|raw| humantime::parse_duration(raw).ok())
            .unwrap_or(DEFAULT_TIMEOUT)
    }
}

impl WorkerDevicesConfig {
    /// Validated at manager startup, so a bad block fails there rather than
    /// on every node.
    pub fn validate(&self) -> R<()> {
        for (name, plugin) in &self.plugins {
            if name.is_empty()
                || name == STATIC_PLUGIN
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                bail!(
                    "devices.plugins.{name}: a plugin name is letters, digits, '-' and '_', and \
                     `{STATIC_PLUGIN}` is reserved"
                );
            }
            if plugin
                .command
                .first()
                .is_none_or(|program| program.is_empty())
            {
                bail!("devices.plugins.{name}.command must name a program");
            }
            if let Some(raw) = &plugin.timeout {
                let timeout = humantime::parse_duration(raw)
                    .with_context(|| format!("devices.plugins.{name}.timeout {raw:?}"))?;
                if timeout.is_zero() {
                    bail!("devices.plugins.{name}.timeout must be above zero");
                }
            }
        }
        let mut ids = HashSet::new();
        for group in &self.static_groups {
            group
                .validate()
                .with_context(|| format!("devices.static {}", group.id()))?;
            if !ids.insert(group.id()) {
                bail!("devices.static lists group {} twice", group.id());
            }
        }
        if let Some(dirs) = &self.cdi_spec_dirs {
            for dir in dirs {
                if !dir.starts_with('/') {
                    bail!("devices.cdiSpecDirs entry {dir:?} must be absolute");
                }
            }
        }
        Ok(())
    }

    fn spec_dirs(&self) -> Vec<PathBuf> {
        match &self.cdi_spec_dirs {
            Some(dirs) => dirs.iter().map(PathBuf::from).collect(),
            None => DEFAULT_SPEC_DIRS.iter().map(PathBuf::from).collect(),
        }
    }
}

/// The `fingerprint` reply.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FingerprintReply {
    #[serde(default)]
    devices: Vec<DeviceGroup>,
}

/// The `allocate` reply.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocateReply {
    #[serde(default)]
    pub container_edits: ContainerEdits,
    #[serde(default)]
    pub cdi_devices: Vec<String>,
}

fn request(op: &str, name: &str, plugin: &DevicePluginConfig) -> Value {
    json!({
        "protocol": PROTOCOL,
        "op": op,
        "plugin": name,
        "config": plugin.config,
    })
}

/// Run `fingerprint` against one plugin: its groups, validated, each stamped
/// with the plugin's name.
pub fn fingerprint(name: &str, plugin: &DevicePluginConfig) -> R<Vec<DeviceGroup>> {
    let what = format!("device plugin {name}");
    let reply = crate::util::run_json_command(
        &what,
        &plugin.command,
        &request("fingerprint", name, plugin),
        plugin.timeout(),
    )?;
    let reply: FingerprintReply = serde_json::from_value(reply)
        .with_context(|| format!("{what}: the fingerprint reply is not a device list"))?;
    let mut groups = reply.devices;
    let mut seen = HashSet::new();
    for group in &mut groups {
        group.validate().with_context(|| what.clone())?;
        if !seen.insert(group.id()) {
            bail!("{what} reported group {} twice", group.id());
        }
        group.plugin = name.to_string();
    }
    Ok(groups)
}

/// Run `allocate` for one assignment.
pub fn allocate(
    name: &str,
    plugin: &DevicePluginConfig,
    assignment: &DeviceAssignment,
) -> R<AllocateReply> {
    let what = format!("device plugin {name}");
    let mut body = request("allocate", name, plugin);
    body["device"] = json!(assignment.device);
    body["ids"] = json!(assignment.ids);
    let reply = crate::util::run_json_command(&what, &plugin.command, &body, plugin.timeout())?;
    let reply: AllocateReply = serde_json::from_value(reply)
        .with_context(|| format!("{what}: the allocate reply is not container edits"))?;
    reply
        .container_edits
        .validate()
        .with_context(|| format!("{what} allocate"))?;
    Ok(reply)
}

/// What a task gets for its devices: env for every runtime, and the
/// container edits an isolating runtime applies.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeviceGrant {
    /// `LEGION_DEVICE_<NAME>_IDS` plus every env entry the edits carry.
    pub env: BTreeMap<String, String>,
    /// Everything, env included, for the container runtime.
    pub edits: ContainerEdits,
}

/// The worker's device side: its plugins, its static groups, and its last
/// report.
#[derive(Debug, Default)]
pub struct DeviceManager {
    config: WorkerDevicesConfig,
    last: Mutex<DeviceReport>,
}

impl DeviceManager {
    pub fn new(config: WorkerDevicesConfig) -> Self {
        DeviceManager {
            config,
            last: Mutex::new(DeviceReport::default()),
        }
    }

    pub fn config(&self) -> &WorkerDevicesConfig {
        &self.config
    }

    /// Whether this worker reports devices at all. A worker with neither
    /// plugins nor static groups sends no report, so its health checks stay
    /// as they were.
    pub fn is_configured(&self) -> bool {
        !self.config.plugins.is_empty() || !self.config.static_groups.is_empty()
    }

    /// Run every plugin's fingerprint (blocking: call off the async
    /// runtime) and merge in the static groups. A failing plugin reports no
    /// groups and its error; the rest are unaffected. Groups are sorted by id
    /// so an unchanged node sends a byte-identical report.
    pub fn fingerprint(&self) -> DeviceReport {
        let mut report = DeviceReport::default();
        for group in &self.config.static_groups {
            let mut group = group.clone();
            group.plugin = STATIC_PLUGIN.to_string();
            report.groups.push(group);
        }
        for (name, plugin) in &self.config.plugins {
            match fingerprint(name, plugin) {
                Ok(groups) => {
                    report.plugins.insert(
                        name.clone(),
                        PluginStatus {
                            status: "ok".into(),
                            error: String::new(),
                            groups: groups.len(),
                        },
                    );
                    for group in groups {
                        if report.groups.iter().any(|g| g.id() == group.id()) {
                            warn!(
                                plugin = %name,
                                group = %group.id(),
                                "Two device sources report the same group; keeping the first"
                            );
                            continue;
                        }
                        report.groups.push(group);
                    }
                }
                Err(err) => {
                    let message = format!("{err:#}");
                    let previous = self
                        .last
                        .lock()
                        .ok()
                        .and_then(|last| last.plugins.get(name).cloned());
                    if previous.is_none_or(|p| p.error != message) {
                        warn!(plugin = %name, error = %message, "Device plugin fingerprint failed");
                    }
                    report.plugins.insert(
                        name.clone(),
                        PluginStatus {
                            status: "error".into(),
                            error: message,
                            groups: 0,
                        },
                    );
                }
            }
        }
        report.groups.sort_by_key(DeviceGroup::id);
        {
            let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
            let unhealthy = |r: &DeviceReport| -> Vec<String> {
                r.groups
                    .iter()
                    .flat_map(|g| {
                        g.instances
                            .iter()
                            .filter(|i| !i.healthy)
                            .map(move |i| format!("{}:{}", g.id(), i.id))
                    })
                    .collect()
            };
            let (before, after) = (unhealthy(&last), unhealthy(&report));
            for gone_bad in after.iter().filter(|id| !before.contains(id)) {
                warn!(device = %gone_bad, "Device instance reported unhealthy");
            }
            for recovered in before.iter().filter(|id| !after.contains(id)) {
                info!(device = %recovered, "Device instance healthy again");
            }
            *last = report.clone();
        }
        report
    }

    /// The last report [`Self::fingerprint`] produced.
    pub fn last_report(&self) -> DeviceReport {
        self.last
            .lock()
            .map(|last| last.clone())
            .unwrap_or_default()
    }

    /// Allocate the devices recorded on an allocation's `resources`
    /// (blocking): the generic env, each plugin's `allocate` edits, and the
    /// edits of every CDI device the instances or the plugin name. A request
    /// with no assignment fails: the scheduler assigns every request it
    /// places, so one without is work that did not come through it (an edge
    /// start), and running it would hand the task no devices silently.
    pub fn grant(&self, resources: &Value) -> R<DeviceGrant> {
        let specs = super::request_specs(resources)?;
        if specs.is_empty() {
            return Ok(DeviceGrant::default());
        }
        let report = self.last_report();
        let mut grant = DeviceGrant::default();
        let mut cdi_names: Vec<String> = Vec::new();
        for spec in &specs {
            let Some(assignment) = spec.assigned.as_ref() else {
                bail!(
                    "device request {:?} has no assignment: devices are assigned by the \
                     scheduler, so a task that asks for them cannot be started any other way",
                    spec.name
                );
            };
            let var = super::ids_env_var(&spec.name);
            let value = assignment.ids.join(",");
            grant.env.insert(var.clone(), value.clone());
            grant.edits.set_env(format!("{var}={value}"));
            let group = report.groups.iter().find(|g| g.id() == assignment.device);
            if let Some(group) = group {
                for instance in &group.instances {
                    if assignment.ids.contains(&instance.id) {
                        if let Some(name) = &instance.cdi {
                            cdi_names.push(name.clone());
                        }
                    }
                }
            }
            match self.config.plugins.get(&assignment.plugin) {
                Some(plugin) => {
                    let reply = allocate(&assignment.plugin, plugin, assignment)?;
                    cdi_names.extend(reply.cdi_devices);
                    grant.edits.merge(reply.container_edits);
                }
                None if assignment.plugin == STATIC_PLUGIN || assignment.plugin.is_empty() => {}
                None => bail!(
                    "device {} was assigned from plugin {}, which this worker no longer runs",
                    assignment.device,
                    assignment.plugin
                ),
            }
        }
        if !cdi_names.is_empty() {
            cdi_names.dedup();
            let registry = SpecRegistry::load(&self.config.spec_dirs());
            grant.edits.merge(registry.resolve(&cdi_names)?);
        }
        grant.edits.validate()?;
        // OCI needs each node's type and numbers; a node missing from the
        // host means the device is not usable here.
        grant.edits.resolve_device_nodes()?;
        for (key, value) in grant.edits.env_map() {
            grant.env.insert(key, value);
        }
        Ok(grant)
    }
}

/// One step of [`conformance`].
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConformanceStep {
    pub check: String,
    pub passed: bool,
    pub detail: String,
}

/// The conformance harness behind `legion device plugin test`: fingerprint
/// twice (the reply must be valid and stable), then allocate the first
/// healthy instance of each group and check the edits are valid.
pub fn conformance(name: &str, plugin: &DevicePluginConfig) -> Vec<ConformanceStep> {
    let mut steps = Vec::new();
    let mut step = |check: &str, result: R<String>| {
        let (passed, detail) = match result {
            Ok(detail) => (true, detail),
            Err(err) => (false, format!("{err:#}")),
        };
        steps.push(ConformanceStep {
            check: check.to_string(),
            passed,
            detail,
        });
        passed
    };
    let first = fingerprint(name, plugin);
    let groups = match first {
        Ok(groups) => {
            step(
                "fingerprint",
                Ok(format!(
                    "{} group(s), {} instance(s)",
                    groups.len(),
                    groups.iter().map(|g| g.instances.len()).sum::<usize>()
                )),
            );
            groups
        }
        Err(err) => {
            step("fingerprint", Err(err));
            return steps;
        }
    };
    let second = fingerprint(name, plugin).and_then(|again| {
        if again == groups {
            Ok("two fingerprints agree".to_string())
        } else {
            bail!("two fingerprints in a row disagree; Legion would rewrite the node each pass")
        }
    });
    step("fingerprint is stable", second);
    for group in &groups {
        let Some(instance) = group.instances.iter().find(|i| i.healthy) else {
            step(
                &format!("allocate {}", group.id()),
                Ok("skipped: no healthy instance".to_string()),
            );
            continue;
        };
        let assignment = DeviceAssignment {
            device: group.id(),
            plugin: name.to_string(),
            ids: vec![instance.id.clone()],
        };
        let result = allocate(name, plugin, &assignment).map(|reply| {
            format!(
                "{} env, {} device node(s), {} mount(s), {} hook(s), {} CDI device(s)",
                reply.container_edits.env.len(),
                reply.container_edits.device_nodes.len(),
                reply.container_edits.mounts.len(),
                reply.container_edits.hooks.len(),
                reply.cdi_devices.len()
            )
        });
        step(&format!("allocate {}", group.id()), result);
    }
    steps
}

#[cfg(test)]
pub(crate) mod plugin_tests {
    use super::*;

    /// A shell device plugin reading its fake inventory from `inventory`
    /// (a JSON array of groups) and answering `allocate` with an env entry
    /// and a `/dev/null` device node.
    pub(crate) fn fake_plugin(dir: &std::path::Path, inventory: &Value) -> DevicePluginConfig {
        let inventory_path = dir.join("inventory.json");
        std::fs::write(&inventory_path, inventory.to_string()).expect("inventory");
        let script = dir.join("fake-device-plugin");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 req=$(cat)\n\
                 case \"$req\" in\n\
                 *'\"op\":\"fingerprint\"'*) printf '{{\"devices\":%s}}' \"$(cat {inv})\" ;;\n\
                 *'\"op\":\"allocate\"'*) ids=$(printf '%s' \"$req\" | sed 's/.*\"ids\":\\[\\([^]]*\\)\\].*/\\1/' | tr -d '\"');\n\
                   printf '{{\"containerEdits\":{{\"env\":[\"FAKE_VISIBLE_DEVICES=%s\"],\"deviceNodes\":[{{\"path\":\"/dev/null\"}}]}}}}' \"$ids\" ;;\n\
                 *) echo 'unknown op' >&2; exit 2 ;;\n\
                 esac\n",
                inv = inventory_path.display()
            ),
        )
        .expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
                .expect("chmod");
        }
        DevicePluginConfig {
            command: vec![script.display().to_string()],
            config: Value::Null,
            timeout: Some("5s".into()),
        }
    }

    fn inventory(healthy: bool) -> Value {
        json!([{
            "vendor": "fake", "type": "gpu", "model": "f1",
            "attributes": { "memoryMiB": 16384 },
            "instances": [
                { "id": "fake-0", "attributes": { "peerGroup": "a" } },
                { "id": "fake-1", "healthy": healthy, "healthDetail": "xid 79",
                  "attributes": { "peerGroup": "a" } },
            ],
        }])
    }

    #[test]
    fn fingerprint_merges_plugins_and_static_groups_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut config = WorkerDevicesConfig::default();
        config
            .plugins
            .insert("fake".into(), fake_plugin(dir.path(), &inventory(true)));
        config.plugins.insert(
            "broken".into(),
            DevicePluginConfig {
                command: vec!["/nonexistent/legion-device-plugin".into()],
                ..Default::default()
            },
        );
        config.static_groups.push(serde_json::from_value(json!({
            "vendor": "xilinx", "type": "fpga", "model": "u250",
            "instances": [{ "id": "0", "cdi": "xilinx.com/fpga=0" }],
        }))?);
        config.validate()?;
        let manager = DeviceManager::new(config);
        let report = manager.fingerprint();
        let ids: Vec<String> = report.groups.iter().map(DeviceGroup::id).collect();
        assert_eq!(ids, vec!["fake/gpu/f1", "xilinx/fpga/u250"]);
        assert_eq!(report.groups[0].plugin, "fake");
        assert_eq!(report.groups[1].plugin, STATIC_PLUGIN);
        assert_eq!(report.plugins["fake"].status, "ok");
        assert_eq!(report.plugins["broken"].status, "error");
        assert!(!report.plugins["broken"].error.is_empty());
        // Deterministic: the same inventory is the same value.
        assert_eq!(manager.fingerprint().to_value(), report.to_value());
        Ok(())
    }

    #[test]
    fn grant_allocates_through_the_plugin_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut config = WorkerDevicesConfig::default();
        config
            .plugins
            .insert("fake".into(), fake_plugin(dir.path(), &inventory(true)));
        let manager = DeviceManager::new(config);
        manager.fingerprint();
        let resources = json!({ "memory": 64, "devices": [{
            "name": "fake/gpu", "count": 2,
            "assigned": { "device": "fake/gpu/f1", "plugin": "fake", "ids": ["fake-0", "fake-1"] },
        }]});
        let grant = manager.grant(&resources)?;
        assert_eq!(grant.env["LEGION_DEVICE_FAKE_GPU_IDS"], "fake-0,fake-1");
        assert_eq!(grant.env["FAKE_VISIBLE_DEVICES"], "fake-0,fake-1");
        assert_eq!(grant.edits.device_nodes[0].path, "/dev/null");

        let unassigned = json!({ "devices": [{ "name": "fake/gpu" }] });
        assert!(manager
            .grant(&unassigned)
            .unwrap_err()
            .to_string()
            .contains("no assignment"));
        assert_eq!(
            manager.grant(&json!({ "memory": 1 }))?,
            DeviceGrant::default()
        );
        Ok(())
    }

    #[test]
    fn conformance_passes_the_fake_plugin_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let plugin = fake_plugin(dir.path(), &inventory(false));
        let steps = conformance("fake", &plugin);
        assert!(steps.iter().all(|s| s.passed), "{steps:?}");
        assert_eq!(steps.len(), 3, "{steps:?}");
        Ok(())
    }

    /// The shipped reference plugin against a stub `nvidia-smi` that prints
    /// four A100s in two NVLink pairs, one with ECC errors. Exercises the
    /// parsing only: it has not run against real GPUs.
    #[cfg(unix)]
    #[test]
    fn the_reference_nvidia_plugin_parses_nvidia_smi_test() -> R<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let stub = dir.path().join("nvidia-smi");
        std::fs::write(
            &stub,
            "#!/bin/sh\n\
             case \"$1\" in\n\
             topo) printf '\\tGPU0\\tGPU1\\tGPU2\\tGPU3\\tCPU Affinity\\tNUMA Affinity\\n\
GPU0\\t X \\tNV12\\tSYS\\tSYS\\t0-31\\t0\\n\
GPU1\\tNV12\\t X \\tSYS\\tSYS\\t0-31\\t0\\n\
GPU2\\tSYS\\tSYS\\t X \\tNV12\\t32-63\\t1\\n\
GPU3\\tSYS\\tSYS\\tNV12\\t X \\t32-63\\t1\\n' ;;\n\
             *) printf '0, GPU-aaa, NVIDIA A100-SXM4-80GB, 81920, 550.54.15, 00000000:07:00.0, 8.0, 0\\n\
1, GPU-bbb, NVIDIA A100-SXM4-80GB, 81920, 550.54.15, 00000000:0F:00.0, 8.0, 0\\n\
2, GPU-ccc, NVIDIA A100-SXM4-80GB, 81920, 550.54.15, 00000000:47:00.0, 8.0, 3\\n\
3, GPU-ddd, NVIDIA A100-SXM4-80GB, 81920, 550.54.15, 00000000:4E:00.0, 8.0, [N/A]\\n' ;;\n\
             esac\n",
        )?;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))?;
        let plugin_path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
            .join("plugins/devices/nvidia/legion-device-nvidia");
        let plugin = DevicePluginConfig {
            command: vec![
                "env".into(),
                format!("PATH={}:/usr/bin:/bin", dir.path().display()),
                plugin_path.display().to_string(),
                "--cdi=uuid".into(),
            ],
            config: Value::Null,
            timeout: Some("20s".into()),
        };
        let groups = fingerprint("nvidia", &plugin)?;
        assert_eq!(groups.len(), 1, "{groups:?}");
        let group = &groups[0];
        assert_eq!(group.id(), "nvidia/gpu/A100-SXM4-80GB");
        assert_eq!(group.attributes["memoryMiB"], json!(81920));
        assert_eq!(group.attributes["driver"], json!("550.54.15"));
        let ids: Vec<&str> = group.instances.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, vec!["GPU-aaa", "GPU-bbb", "GPU-ccc", "GPU-ddd"]);
        assert!(group.instances[0].healthy);
        assert!(!group.instances[2].healthy, "ECC errors make it unhealthy");
        assert!(group.instances[3].healthy, "N/A ECC is not an error");
        assert_eq!(
            group.instances[0].attributes["peerGroup"],
            json!("nvlink-0")
        );
        assert_eq!(
            group.instances[1].attributes["peerGroup"],
            json!("nvlink-0")
        );
        assert_eq!(
            group.instances[3].attributes["peerGroup"],
            json!("nvlink-2")
        );
        assert_eq!(group.instances[1].attributes["index"], json!(1));

        let reply = allocate(
            "nvidia",
            &plugin,
            &DeviceAssignment {
                device: group.id(),
                plugin: "nvidia".into(),
                ids: vec!["GPU-aaa".into(), "GPU-bbb".into()],
            },
        )?;
        let env = reply.container_edits.env_map();
        assert_eq!(env["CUDA_VISIBLE_DEVICES"], "GPU-aaa,GPU-bbb");
        assert_eq!(env["NVIDIA_VISIBLE_DEVICES"], "GPU-aaa,GPU-bbb");
        assert_eq!(reply.container_edits.device_nodes[0].path, "/dev/nvidia0");
        assert_eq!(reply.container_edits.device_nodes[1].path, "/dev/nvidia1");
        assert_eq!(
            reply.cdi_devices,
            vec!["nvidia.com/gpu=GPU-aaa", "nvidia.com/gpu=GPU-bbb"]
        );
        Ok(())
    }

    #[test]
    fn config_validation_test() {
        let mut config = WorkerDevicesConfig::default();
        config.plugins.insert(
            STATIC_PLUGIN.into(),
            DevicePluginConfig {
                command: vec!["/bin/true".into()],
                ..Default::default()
            },
        );
        assert!(config.validate().is_err());
        let mut config = WorkerDevicesConfig::default();
        config
            .plugins
            .insert("nvidia".into(), DevicePluginConfig::default());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("command"));
        let config = WorkerDevicesConfig {
            cdi_spec_dirs: Some(vec!["relative".into()]),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
