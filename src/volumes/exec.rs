//! The exec plugin ABI — how out-of-tree volume drivers plug in.
//!
//! A plugin is any executable at `plugins/volumes/<name>/<name>` beside a
//! `plugin.json` manifest. Legion runs it with the operation as `argv[1]` and a
//! JSON request on stdin; the plugin writes a JSON response on stdout and exits
//! 0.
//!
//! ```console
//! $ echo '{"apiVersion":1,"op":"capabilities",...}' | plugins/volumes/zfs/zfs capabilities
//! {"apiVersion":1,"name":"zfs","nodeLocal":true,"accessModes":["single-writer"]}
//! ```
//!
//! Why exec rather than the `cdylib` ABI the task-kind plugins use: volume
//! operations are low-frequency (task start/stop), usually shell out to system
//! tools (`mount`, `zfs`, a cloud CLI), and must not be able to take the worker
//! down when they crash. Process isolation buys all three, and a fork per task
//! start costs nothing at this rate.
//!
//! `plugins/volumes/example/` is a ~60-line shell implementation of the whole
//! contract; `legion volume plugin test example` runs the conformance harness
//! against it.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde::Deserialize;

use crate::prelude::*;
use crate::volumes::{MountResult, VolumeCapabilities, VolumeOp, VolumePlugin, VolumeRequest};

/// Manifest key/value contract for `plugins/volumes/<name>/plugin.json`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumePluginManifest {
    pub kind: String,
    pub name: String,
    pub legion_volume_api_version: u32,
}

/// How long a single plugin invocation may run before Legion gives up. Mounts
/// can legitimately be slow (a cold NFS server), so this is generous; it exists
/// to stop a wedged driver from hanging a worker's task-start path forever.
const PLUGIN_TIMEOUT: Duration = Duration::from_secs(120);

/// An out-of-tree driver invoked as a subprocess.
#[derive(Debug)]
pub struct ExecVolumePlugin {
    name: String,
    binary: PathBuf,
}

impl ExecVolumePlugin {
    /// Validate `<root>/plugin.json` and locate the executable.
    pub fn load(root: &Path, name: &str) -> R<Self> {
        let manifest_path = root.join("plugin.json");
        let bytes = std::fs::read(&manifest_path)
            .with_context(|| format!("missing plugin manifest at {}", manifest_path.display()))?;
        let manifest: VolumePluginManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid plugin manifest at {}", manifest_path.display()))?;

        if manifest.kind != "volumes" {
            bail!(
                "volume plugin manifest kind mismatch for {name}: expected volumes, got {}",
                manifest.kind
            );
        }
        if manifest.name != name {
            bail!(
                "volume plugin manifest name mismatch: directory {name}, manifest {}",
                manifest.name
            );
        }
        if manifest.legion_volume_api_version != super::VOLUME_API_VERSION {
            bail!(
                "volume plugin {name} declares legionVolumeApiVersion {} (host supports {})",
                manifest.legion_volume_api_version,
                super::VOLUME_API_VERSION
            );
        }

        let binary = root.join(name);
        if !binary.is_file() {
            bail!(
                "volume plugin {name} has a manifest but no executable at {}",
                binary.display()
            );
        }
        Ok(ExecVolumePlugin {
            name: name.to_string(),
            binary,
        })
    }

    pub fn binary(&self) -> &Path {
        &self.binary
    }

    /// Run one operation and parse the response. `T` is the op's reply shape;
    /// ops with no payload use [`Empty`].
    fn invoke<T: serde::de::DeserializeOwned>(&self, request: &VolumeRequest) -> R<T> {
        let payload = serde_json::to_vec(request)?;
        let mut child = Command::new(&self.binary)
            .arg(request.op.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to run volume plugin {} at {}",
                    self.name,
                    self.binary.display()
                )
            })?;

        child
            .stdin
            .as_mut()
            .context("volume plugin stdin unavailable")?
            .write_all(&payload)
            .with_context(|| format!("failed to write request to volume plugin {}", self.name))?;
        // Close stdin so the plugin's `cat` (or read loop) terminates.
        drop(child.stdin.take());

        let output = wait_with_timeout(child, PLUGIN_TIMEOUT).with_context(|| {
            format!(
                "volume plugin {} timed out running `{}`",
                self.name, request.op
            )
        })?;
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

        if !output.status.success() {
            bail!("{}", plugin_error(&self.name, request.op, &stdout, &stderr));
        }
        // A plugin that exits 0 but reports an error object is still a
        // failure — being lenient here would let silent no-ops look like
        // successful mounts.
        if let Some(message) = parse_error_payload(&stdout) {
            bail!(
                "volume plugin {} failed running `{}`: {message}",
                self.name,
                request.op
            );
        }
        if stdout.is_empty() {
            // Ops with no payload (`unmount`, `destroy`) may print nothing.
            return serde_json::from_str::<T>("{}").with_context(|| {
                format!(
                    "volume plugin {} returned no output for `{}`",
                    self.name, request.op
                )
            });
        }
        serde_json::from_str::<T>(&stdout).with_context(|| {
            format!(
                "volume plugin {} returned invalid JSON for `{}`: {stdout}",
                self.name, request.op
            )
        })
    }
}

#[derive(Deserialize)]
struct Empty {}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Deserialize)]
struct ErrorBody {
    message: String,
    #[serde(default)]
    code: Option<String>,
}

/// Pull `{"error": {...}}` out of a plugin's stdout, if it wrote one.
fn parse_error_payload(stdout: &str) -> Option<String> {
    let envelope: ErrorEnvelope = serde_json::from_str(stdout).ok()?;
    Some(match envelope.error.code {
        Some(code) => format!("{} ({code})", envelope.error.message),
        None => envelope.error.message,
    })
}

/// Compose the most useful message we can from a failed invocation: the
/// plugin's structured error if it wrote one, else its stderr, else the exit
/// status.
fn plugin_error(name: &str, op: VolumeOp, stdout: &str, stderr: &str) -> String {
    if let Some(message) = parse_error_payload(stdout) {
        return format!("volume plugin {name} failed running `{op}`: {message}");
    }
    if !stderr.is_empty() {
        return format!("volume plugin {name} failed running `{op}`: {stderr}");
    }
    format!("volume plugin {name} exited non-zero running `{op}`")
}

/// `Child::wait_with_output` with a deadline. Polls rather than reading
/// concurrently, which is fine for the small payloads this ABI carries.
fn wait_with_timeout(mut child: std::process::Child, timeout: Duration) -> R<std::process::Output> {
    let start = std::time::Instant::now();
    loop {
        if let Some(_status) = child.try_wait()? {
            return Ok(child.wait_with_output()?);
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            bail!("plugin exceeded the {}s timeout", timeout.as_secs());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl VolumePlugin for ExecVolumePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> VolumeCapabilities {
        // A probe that fails must not be fatal: report the plugin as installed
        // but unavailable so operators see *why* in `legion volume plugins`
        // instead of the plugin silently vanishing from the list.
        let request = VolumeRequest::new(
            VolumeOp::Capabilities,
            super::VolumeRef {
                id: String::new(),
                name: String::new(),
                plugin: self.name.clone(),
                attrs: Default::default(),
                read_only: false,
            },
            super::VolumeNodeRef {
                id: String::new(),
                data_dir: PathBuf::new(),
                volume_dir: PathBuf::new(),
            },
        );
        match self.invoke::<VolumeCapabilities>(&request) {
            Ok(mut caps) => {
                // The registry key is authoritative: a plugin that renames
                // itself in its own reply would break every lookup.
                caps.name = self.name.clone();
                caps
            }
            Err(err) => VolumeCapabilities::new(&self.name, true)
                .unavailable(format!("capability probe failed: {err}")),
        }
    }

    fn mount(&self, request: &VolumeRequest) -> R<MountResult> {
        self.invoke::<MountResult>(request)
    }

    fn unmount(&self, request: &VolumeRequest) -> R<()> {
        self.invoke::<Empty>(request).map(|_| ())
    }

    fn destroy(&self, request: &VolumeRequest) -> R<()> {
        self.invoke::<Empty>(request).map(|_| ())
    }
}

/// Every valid exec plugin under `root` (`<root>/<name>/plugin.json`).
/// Directories that fail validation are logged and skipped: one broken plugin
/// must not stop a worker from starting.
pub fn discover(root: &Path) -> R<Vec<ExecVolumePlugin>> {
    let entries = std::fs::read_dir(root)
        .with_context(|| format!("no volume plugin directory at {}", root.display()))?;
    let mut plugins = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        match ExecVolumePlugin::load(&entry.path(), &name) {
            Ok(plugin) => plugins.push(plugin),
            Err(err) => warn!(
                plugin = %name,
                error = ?err,
                "Skipping invalid volume plugin directory"
            ),
        }
    }
    plugins.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(plugins)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volumes::{VolumeNodeRef, VolumeRef};

    fn temp_root(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("legion-exec-vol-{label}-{unique}"));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// Writes a plugin directory with `script` as the executable.
    fn write_plugin(root: &Path, name: &str, api_version: u32, script: &str) -> R<PathBuf> {
        let plugin_root = root.join(name);
        std::fs::create_dir_all(&plugin_root)?;
        std::fs::write(
            plugin_root.join("plugin.json"),
            format!(
                r#"{{"kind":"volumes","name":"{name}","legionVolumeApiVersion":{api_version}}}"#
            ),
        )?;
        let binary = plugin_root.join(name);
        std::fs::write(&binary, script)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))?;
        }
        Ok(plugin_root)
    }

    fn request(op: VolumeOp) -> VolumeRequest {
        VolumeRequest::new(
            op,
            VolumeRef {
                id: "vol-1".into(),
                name: "data".into(),
                plugin: "demo".into(),
                attrs: Default::default(),
                read_only: false,
            },
            VolumeNodeRef {
                id: "node-1".into(),
                data_dir: PathBuf::from("/tmp"),
                volume_dir: PathBuf::from("/tmp/volumes"),
            },
        )
    }

    #[test]
    fn loads_and_invokes_a_shell_plugin() -> R<()> {
        let root = temp_root("ok");
        write_plugin(
            &root,
            "demo",
            1,
            "#!/bin/sh\n\
             cat > /dev/null\n\
             case \"$1\" in\n\
             capabilities) echo '{\"apiVersion\":1,\"name\":\"demo\",\"nodeLocal\":true}';;\n\
             mount) echo '{\"mountPath\":\"/tmp/demo-vol\"}';;\n\
             *) echo '{}';;\n\
             esac\n",
        )?;
        let plugin = ExecVolumePlugin::load(&root.join("demo"), "demo")?;

        let caps = plugin.capabilities();
        assert!(caps.available, "{}", caps.detail);
        assert!(caps.node_local);

        let mounted = plugin.mount(&request(VolumeOp::Mount))?;
        assert_eq!(mounted.mount_path, Some(PathBuf::from("/tmp/demo-vol")));
        plugin.unmount(&request(VolumeOp::Unmount))?;
        plugin.destroy(&request(VolumeOp::Destroy))?;

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn surfaces_structured_plugin_errors() -> R<()> {
        let root = temp_root("err");
        write_plugin(
            &root,
            "demo",
            1,
            "#!/bin/sh\n\
             cat > /dev/null\n\
             echo '{\"error\":{\"code\":\"ENOSPC\",\"message\":\"disk full\"}}'\n\
             exit 1\n",
        )?;
        let plugin = ExecVolumePlugin::load(&root.join("demo"), "demo")?;
        let err = plugin
            .mount(&request(VolumeOp::Mount))
            .unwrap_err()
            .to_string();
        assert!(err.contains("disk full"), "{err}");
        assert!(err.contains("ENOSPC"), "{err}");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn zero_exit_with_error_payload_still_fails() -> R<()> {
        let root = temp_root("soft-err");
        write_plugin(
            &root,
            "demo",
            1,
            "#!/bin/sh\ncat > /dev/null\necho '{\"error\":{\"message\":\"nope\"}}'\n",
        )?;
        let plugin = ExecVolumePlugin::load(&root.join("demo"), "demo")?;
        let err = plugin
            .mount(&request(VolumeOp::Mount))
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "{err}");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn rejects_wrong_api_version_and_missing_binary() -> R<()> {
        let root = temp_root("bad");
        write_plugin(&root, "old", 99, "#!/bin/sh\nexit 0\n")?;
        let err = ExecVolumePlugin::load(&root.join("old"), "old")
            .unwrap_err()
            .to_string();
        assert!(err.contains("legionVolumeApiVersion"), "{err}");

        let no_binary = root.join("ghost");
        std::fs::create_dir_all(&no_binary)?;
        std::fs::write(
            no_binary.join("plugin.json"),
            r#"{"kind":"volumes","name":"ghost","legionVolumeApiVersion":1}"#,
        )?;
        let err = ExecVolumePlugin::load(&no_binary, "ghost")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no executable"), "{err}");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn invalid_json_response_is_reported_with_the_payload() -> R<()> {
        let root = temp_root("badjson");
        write_plugin(
            &root,
            "demo",
            1,
            "#!/bin/sh\ncat > /dev/null\necho 'not json at all'\n",
        )?;
        let plugin = ExecVolumePlugin::load(&root.join("demo"), "demo")?;
        let err = plugin
            .mount(&request(VolumeOp::Mount))
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid JSON"), "{err}");
        assert!(err.contains("not json at all"), "{err}");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }

    #[test]
    fn discovery_skips_broken_directories() -> R<()> {
        let root = temp_root("discover");
        write_plugin(
            &root,
            "good",
            1,
            "#!/bin/sh\ncat > /dev/null\necho '{\"apiVersion\":1,\"name\":\"good\",\"nodeLocal\":true}'\n",
        )?;
        write_plugin(&root, "stale", 42, "#!/bin/sh\nexit 0\n")?;
        std::fs::create_dir_all(root.join("empty"))?;

        let found = discover(&root)?;
        assert_eq!(found.len(), 1, "only the valid plugin should load");
        assert_eq!(found[0].name(), "good");

        std::fs::remove_dir_all(&root).ok();
        Ok(())
    }
}
