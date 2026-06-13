//! OCI image layout → OCI runtime bundle conversion.
//!
//! Low-level OCI runtimes (youki, runc, crun) do not pull or run images by
//! reference; they run a *bundle*: an extracted root filesystem plus a
//! `config.json` runtime spec. This module turns the on-disk OCI **image**
//! layout produced by the `oci://` artifact fetcher into that bundle so the
//! container task kind can run it on Linux.
//!
//! The conversion is deliberately self-contained (no external `umoci`/`skopeo`
//! dependency): read the image manifest + config from the layout, extract each
//! layer tarball into `rootfs/` (honoring overlayfs whiteouts), and synthesize
//! a runtime spec that merges the image config (env, working dir, user) with
//! the task's command and env.

use crate::prelude::*;
use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::path::{Component, Path, PathBuf};

/// Image config fields we carry from the OCI layout into the runtime spec,
/// plus the ordered layer blob paths to extract.
struct ImageConfig {
    env: Vec<String>,
    working_dir: Option<String>,
    user: Option<String>,
    layers: Vec<PathBuf>,
}

/// Builds an OCI runtime bundle at `bundle_dir` from the image `oci_layout`.
///
/// `command` is the container's argv (Legion requires it on the task), `env`
/// the task's env (merged over the image's, task wins), and `hostname` the
/// container's hostname. The bundle is rebuilt from scratch each call.
pub(crate) fn build_bundle(
    oci_layout: &Path,
    bundle_dir: &Path,
    command: &[String],
    env: &std::collections::HashMap<String, String>,
    hostname: &str,
) -> R<()> {
    if command.is_empty() {
        bail!("container command must include at least one entry");
    }
    let image = read_image_config(oci_layout)?;

    if bundle_dir.exists() {
        std::fs::remove_dir_all(bundle_dir)
            .with_context(|| format!("clear bundle dir {}", bundle_dir.display()))?;
    }
    let rootfs = bundle_dir.join("rootfs");
    std::fs::create_dir_all(&rootfs)
        .with_context(|| format!("create rootfs {}", rootfs.display()))?;

    for layer in &image.layers {
        extract_layer(layer, &rootfs)
            .with_context(|| format!("extract layer {}", layer.display()))?;
    }

    let spec = build_spec(command, env, &image, hostname);
    let config_path = bundle_dir.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&spec)?)
        .with_context(|| format!("write {}", config_path.display()))?;
    Ok(())
}

fn read_json(path: &Path) -> R<Value> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

/// `blobs/<algorithm>/<hex>` for a `algorithm:hex` digest, with hex validated
/// so a malformed digest cannot escape the layout directory.
fn blob_path(layout: &Path, digest: &str) -> R<PathBuf> {
    let (algorithm, hex) = digest
        .split_once(':')
        .with_context(|| format!("invalid blob digest {digest}"))?;
    if hex.is_empty() || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid blob digest {digest}");
    }
    Ok(layout.join("blobs").join(algorithm).join(hex))
}

fn read_image_config(layout: &Path) -> R<ImageConfig> {
    let index = read_json(&layout.join("index.json"))?;
    let manifest_digest = index["manifests"][0]["digest"]
        .as_str()
        .context("OCI index has no manifest digest")?;
    let manifest = read_json(&blob_path(layout, manifest_digest)?)?;

    let config_digest = manifest["config"]["digest"]
        .as_str()
        .context("image manifest has no config digest")?;
    let config = read_json(&blob_path(layout, config_digest)?)?;
    let cfg = &config["config"];

    let env = cfg["Env"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let working_dir = cfg["WorkingDir"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(String::from);
    let user = cfg["User"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(String::from);

    let layers = manifest["layers"]
        .as_array()
        .context("image manifest has no layers")?
        .iter()
        .map(|layer| {
            let digest = layer["digest"].as_str().context("layer has no digest")?;
            blob_path(layout, digest)
        })
        .collect::<R<Vec<_>>>()?;

    Ok(ImageConfig {
        env,
        working_dir,
        user,
        layers,
    })
}

/// Normalizes a tar entry path to a safe relative path within the rootfs.
/// Rejects absolute paths and `..` traversal; returns `None` for entries that
/// normalize to nothing (e.g. `.`).
fn safe_relative(path: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            // Absolute roots, prefixes, and `..` are all unsafe in a layer.
            Component::RootDir | Component::Prefix(_) | Component::ParentDir => return None,
        }
    }
    if out.as_os_str().is_empty() {
        None
    } else {
        Some(out)
    }
}

/// Extracts one image layer over `rootfs`, applying overlayfs whiteouts:
/// `.wh.<name>` deletes `<name>`, and `.wh..wh..opq` clears the directory's
/// existing contents (an opaque directory) before later entries land.
fn extract_layer(blob: &Path, rootfs: &Path) -> R<()> {
    let mut file = std::fs::File::open(blob)?;
    let mut magic = [0u8; 2];
    let read = file.read(&mut magic)?;
    file.rewind()?;
    let gzipped = read == 2 && magic == [0x1f, 0x8b];

    let reader: Box<dyn Read> = if gzipped {
        Box::new(flate2::read::GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut archive = tar::Archive::new(reader);
    archive.set_preserve_permissions(true);
    archive.set_overwrite(true);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let Some(rel) = safe_relative(&path) else {
            continue;
        };
        let file_name = rel
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();

        if let Some(whiteout) = file_name.strip_prefix(".wh.") {
            let parent = rel
                .parent()
                .map(|p| rootfs.join(p))
                .unwrap_or_else(|| rootfs.to_path_buf());
            if whiteout == ".wh..opq" {
                clear_dir(&parent)?;
            } else {
                let target = parent.join(whiteout);
                if target.is_dir() {
                    let _ = std::fs::remove_dir_all(&target);
                } else {
                    let _ = std::fs::remove_file(&target);
                }
            }
            continue;
        }

        // `unpack_in` re-validates the path against the rootfs and resolves
        // hardlink/symlink targets safely.
        entry.unpack_in(rootfs)?;
    }
    Ok(())
}

/// Removes the contents of a directory (used for opaque whiteouts) without
/// removing the directory itself.
fn clear_dir(dir: &Path) -> R<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() && !path.is_symlink() {
            let _ = std::fs::remove_dir_all(&path);
        } else {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

/// Merges image env with task env (task wins) and guarantees a `PATH`.
fn merge_env(
    image_env: &[String],
    task_env: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    let mut merged: BTreeMap<String, String> = BTreeMap::new();
    for entry in image_env {
        if let Some((key, value)) = entry.split_once('=') {
            merged.insert(key.to_string(), value.to_string());
        }
    }
    for (key, value) in task_env {
        merged.insert(key.clone(), value.clone());
    }
    merged.entry("PATH".to_string()).or_insert_with(|| {
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string()
    });
    merged
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

/// Best-effort numeric user parse (`uid`, `uid:gid`). Non-numeric users
/// (names needing `/etc/passwd` resolution) fall back to root.
fn parse_user(user: Option<&str>) -> (u32, u32) {
    let Some(user) = user else {
        return (0, 0);
    };
    let (uid_str, gid_str) = match user.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (user, None),
    };
    let uid = uid_str.parse().unwrap_or(0);
    let gid = gid_str.and_then(|g| g.parse().ok()).unwrap_or(uid);
    (uid, gid)
}

/// Builds an OCI runtime spec. Based on the standard `runc spec` default, with:
/// the task command as `process.args`, merged env, the image's working dir and
/// user, no private network namespace (the container shares host networking
/// until network orchestration lands), and a writable root.
fn build_spec(
    command: &[String],
    task_env: &std::collections::HashMap<String, String>,
    image: &ImageConfig,
    hostname: &str,
) -> Value {
    let (uid, gid) = parse_user(image.user.as_deref());
    let env = merge_env(&image.env, task_env);
    let cwd = image.working_dir.clone().unwrap_or_else(|| "/".to_string());

    json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": { "uid": uid, "gid": gid },
            "args": command,
            "env": env,
            "cwd": cwd,
            "capabilities": {
                "bounding": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
                "effective": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
                "permitted": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"]
            },
            "rlimits": [{ "type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024 }],
            "noNewPrivileges": true
        },
        "root": { "path": "rootfs", "readonly": false },
        "hostname": hostname,
        "mounts": [
            { "destination": "/proc", "type": "proc", "source": "proc" },
            { "destination": "/dev", "type": "tmpfs", "source": "tmpfs",
              "options": ["nosuid", "strictatime", "mode=755", "size=65536k"] },
            { "destination": "/dev/pts", "type": "devpts", "source": "devpts",
              "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"] },
            { "destination": "/dev/shm", "type": "tmpfs", "source": "shm",
              "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"] },
            { "destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue",
              "options": ["nosuid", "noexec", "nodev"] },
            { "destination": "/sys", "type": "sysfs", "source": "sysfs",
              "options": ["nosuid", "noexec", "nodev", "ro"] }
        ],
        "linux": {
            "resources": { "devices": [{ "allow": false, "access": "rwm" }] },
            "namespaces": [
                { "type": "pid" },
                { "type": "ipc" },
                { "type": "uts" },
                { "type": "mount" }
            ],
            "maskedPaths": [
                "/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys",
                "/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats",
                "/proc/sched_debug", "/sys/firmware", "/proc/scsi"
            ],
            "readonlyPaths": [
                "/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"
            ]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn safe_relative_rejects_traversal_and_absolute() {
        assert_eq!(
            safe_relative(Path::new("etc/hosts")),
            Some(PathBuf::from("etc/hosts"))
        );
        assert_eq!(
            safe_relative(Path::new("./a/b")),
            Some(PathBuf::from("a/b"))
        );
        assert_eq!(safe_relative(Path::new("/abs")), None);
        assert_eq!(safe_relative(Path::new("../escape")), None);
        assert_eq!(safe_relative(Path::new("a/../../escape")), None);
        assert_eq!(safe_relative(Path::new(".")), None);
    }

    #[test]
    fn merge_env_task_overrides_image_and_path_defaulted() {
        let image = vec!["PATH=/img".to_string(), "FOO=image".to_string()];
        let mut task = HashMap::new();
        task.insert("FOO".to_string(), "task".to_string());
        task.insert("BAR".to_string(), "1".to_string());
        let merged = merge_env(&image, &task);
        assert!(merged.contains(&"FOO=task".to_string()));
        assert!(merged.contains(&"BAR=1".to_string()));
        assert!(merged.contains(&"PATH=/img".to_string()));

        // PATH is synthesized when the image omits it.
        let merged = merge_env(&[], &HashMap::new());
        assert!(merged.iter().any(|e| e.starts_with("PATH=/usr/local/sbin")));
    }

    #[test]
    fn parse_user_handles_numeric_and_names() {
        assert_eq!(parse_user(None), (0, 0));
        assert_eq!(parse_user(Some("1000")), (1000, 1000));
        assert_eq!(parse_user(Some("1000:2000")), (1000, 2000));
        // Names need passwd resolution we don't do; fall back to root.
        assert_eq!(parse_user(Some("nobody")), (0, 0));
    }

    #[test]
    fn build_spec_sets_command_env_and_host_networking() {
        let image = ImageConfig {
            env: vec!["FROM_IMAGE=1".to_string()],
            working_dir: Some("/app".to_string()),
            user: Some("1000:1000".to_string()),
            layers: vec![],
        };
        let mut task_env = HashMap::new();
        task_env.insert("MODE".to_string(), "test".to_string());
        let spec = build_spec(
            &["/bin/echo".to_string(), "hi".to_string()],
            &task_env,
            &image,
            "legion-7",
        );

        assert_eq!(spec["process"]["args"], json!(["/bin/echo", "hi"]));
        assert_eq!(spec["process"]["cwd"], "/app");
        assert_eq!(spec["process"]["user"]["uid"], 1000);
        assert_eq!(spec["process"]["terminal"], false);
        assert_eq!(spec["root"]["readonly"], false);
        assert_eq!(spec["hostname"], "legion-7");
        let env = spec["process"]["env"].as_array().unwrap();
        assert!(env.iter().any(|e| e == "MODE=test"));
        assert!(env.iter().any(|e| e == "FROM_IMAGE=1"));
        // No private network namespace: container uses host networking.
        let namespaces = spec["linux"]["namespaces"].as_array().unwrap();
        assert!(!namespaces.iter().any(|n| n["type"] == "network"));
        assert!(namespaces.iter().any(|n| n["type"] == "pid"));
    }
}
