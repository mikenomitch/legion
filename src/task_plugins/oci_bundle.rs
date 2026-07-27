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
    volumes: &[crate::task_plugins::RuntimeVolume],
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

    // Resolve the image user against the *extracted* rootfs so named users
    // (`postgres`, `nobody:nogroup`) map to their real uid/gid instead of
    // silently running as root.
    let user = resolve_user(&rootfs, image.user.as_deref());
    let spec = build_spec(command, env, &image, &user, hostname, volumes);
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
    let mut magic = [0u8; 4];
    let read = file.read(&mut magic)?;
    file.rewind()?;
    if read == 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        bail!("zstd-compressed image layers are not yet supported");
    }
    let gzipped = read >= 2 && magic[..2] == [0x1f, 0x8b];

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

/// The image `User` resolved to concrete ids, plus the home directory when it
/// came from a passwd entry (images expect `HOME` set, as Docker does).
#[derive(Debug, PartialEq, Eq)]
struct ResolvedUser {
    uid: u32,
    gid: u32,
    home: Option<String>,
}

/// Resolves the image config's `User` (`uid`, `uid:gid`, `name`, `name:group`)
/// against the extracted rootfs. Names look up `etc/passwd` / `etc/group`
/// inside the rootfs; a name that resolves nowhere falls back to root with a
/// warning (matching the previous behavior, but no longer silently).
fn resolve_user(rootfs: &Path, user: Option<&str>) -> ResolvedUser {
    let root = ResolvedUser {
        uid: 0,
        gid: 0,
        home: None,
    };
    let Some(user) = user else {
        return root;
    };
    let (user_part, group_part) = match user.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (user, None),
    };

    let passwd_entry = match user_part.parse::<u32>() {
        Ok(uid) => lookup_passwd(rootfs, |entry| entry.uid == uid),
        Err(_) => lookup_passwd(rootfs, |entry| entry.name == user_part),
    };
    let (uid, passwd_gid, home) = match (&passwd_entry, user_part.parse::<u32>()) {
        (Some(entry), _) => (entry.uid, Some(entry.gid), Some(entry.home.clone())),
        // Numeric uid without a passwd entry is fine — kernels don't need one.
        (None, Ok(uid)) => (uid, None, None),
        (None, Err(_)) => {
            warn!(
                user,
                "image user not found in rootfs /etc/passwd; running as root"
            );
            return root;
        }
    };

    let gid = match group_part {
        Some(group) => match group.parse::<u32>() {
            Ok(gid) => gid,
            Err(_) => match lookup_group(rootfs, group) {
                Some(gid) => gid,
                None => {
                    warn!(
                        group,
                        "image group not found in rootfs /etc/group; using uid as gid"
                    );
                    uid
                }
            },
        },
        None => passwd_gid.unwrap_or(uid),
    };

    ResolvedUser { uid, gid, home }
}

struct PasswdEntry {
    name: String,
    uid: u32,
    gid: u32,
    home: String,
}

/// Finds the first `etc/passwd` entry matching `predicate`
/// (`name:pw:uid:gid:gecos:home:shell`). Malformed lines are skipped.
fn lookup_passwd(rootfs: &Path, predicate: impl Fn(&PasswdEntry) -> bool) -> Option<PasswdEntry> {
    let content = std::fs::read_to_string(rootfs.join("etc/passwd")).ok()?;
    content.lines().find_map(|line| {
        let fields: Vec<&str> = line.split(':').collect();
        let entry = PasswdEntry {
            name: (*fields.first()?).to_string(),
            uid: fields.get(2)?.parse().ok()?,
            gid: fields.get(3)?.parse().ok()?,
            home: (*fields.get(5)?).to_string(),
        };
        predicate(&entry).then_some(entry)
    })
}

/// Resolves a group name to its gid via `etc/group` (`name:pw:gid:members`).
fn lookup_group(rootfs: &Path, group: &str) -> Option<u32> {
    let content = std::fs::read_to_string(rootfs.join("etc/group")).ok()?;
    content.lines().find_map(|line| {
        let fields: Vec<&str> = line.split(':').collect();
        (*fields.first()? == group).then(|| fields.get(2)?.parse().ok())?
    })
}

/// Read-only bind mounts of the host's DNS/hosts files, when they exist. The
/// container shares host networking, so without these the rootfs has no
/// resolver config and every name lookup fails.
fn dns_mounts() -> Vec<Value> {
    ["/etc/resolv.conf", "/etc/hosts"]
        .iter()
        .filter(|path| Path::new(path).is_file())
        .map(|path| {
            json!({
                "destination": path,
                "type": "bind",
                "source": path,
                "options": ["bind", "ro"],
            })
        })
        .collect()
}

/// Builds an OCI runtime spec. Based on the standard `runc spec` default, with:
/// the task command as `process.args`, merged env (plus `HOME` from the
/// resolved user's passwd entry when unset), the image's working dir and user,
/// no private network namespace (the container shares host networking until
/// network orchestration lands; host DNS files are bind-mounted in), and a
/// writable root.
/// Bind mounts for the allocation's volumes, in declaration order.
///
/// This is where `stateful.storage.path` and `volumes[].path` become real for a
/// container: the driver produced a host path, and the container sees it at the
/// declared destination. Volumes with no `path` are skipped — admission rejects
/// that combination for container tasks, so reaching here means a jobspec
/// changed kind after the fact.
fn volume_mounts(volumes: &[crate::task_plugins::RuntimeVolume]) -> Vec<Value> {
    volumes
        .iter()
        .filter_map(|volume| {
            let destination = volume.path.as_deref()?;
            // `rbind` (not plain `bind`) so a volume that is itself a mount
            // point — an NFS export, a tmpfs — carries its submounts in.
            let mut options = vec!["rbind".to_string(), "rprivate".to_string()];
            options.push(if volume.read_only { "ro" } else { "rw" }.to_string());
            Some(json!({
                "destination": destination,
                "type": "bind",
                "source": volume.host_path.display().to_string(),
                "options": options,
            }))
        })
        .collect()
}

fn build_spec(
    command: &[String],
    task_env: &std::collections::HashMap<String, String>,
    image: &ImageConfig,
    user: &ResolvedUser,
    hostname: &str,
    volumes: &[crate::task_plugins::RuntimeVolume],
) -> Value {
    let mut env = merge_env(&image.env, task_env);
    if let Some(home) = &user.home {
        if !env.iter().any(|entry| entry.starts_with("HOME=")) {
            env.push(format!("HOME={home}"));
        }
    }
    let cwd = image.working_dir.clone().unwrap_or_else(|| "/".to_string());

    let mut mounts = vec![
        json!({ "destination": "/proc", "type": "proc", "source": "proc" }),
        json!({ "destination": "/dev", "type": "tmpfs", "source": "tmpfs",
              "options": ["nosuid", "strictatime", "mode=755", "size=65536k"] }),
        json!({ "destination": "/dev/pts", "type": "devpts", "source": "devpts",
              "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"] }),
        json!({ "destination": "/dev/shm", "type": "tmpfs", "source": "shm",
              "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"] }),
        json!({ "destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue",
              "options": ["nosuid", "noexec", "nodev"] }),
        json!({ "destination": "/sys", "type": "sysfs", "source": "sysfs",
              "options": ["nosuid", "noexec", "nodev", "ro"] }),
    ];
    mounts.extend(dns_mounts());
    // Volumes last so a job can deliberately shadow a default mount point.
    mounts.extend(volume_mounts(volumes));

    json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": { "uid": user.uid, "gid": user.gid },
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
        "mounts": mounts,
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

    /// A synthetic rootfs with passwd/group files for user resolution tests.
    fn fake_rootfs() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("etc")).expect("etc");
        std::fs::write(
            dir.path().join("etc/passwd"),
            "root:x:0:0:root:/root:/bin/sh\n\
             postgres:x:70:70:PostgreSQL:/var/lib/postgresql:/bin/sh\n\
             malformed-line\n",
        )
        .expect("passwd");
        std::fs::write(
            dir.path().join("etc/group"),
            "root:x:0:\npostgres:x:70:\nstaff:x:50:\n",
        )
        .expect("group");
        dir
    }

    #[test]
    fn resolve_user_handles_numeric_named_and_missing() {
        let rootfs = fake_rootfs();
        let root = rootfs.path();

        // No user -> root.
        assert_eq!(
            resolve_user(root, None),
            ResolvedUser {
                uid: 0,
                gid: 0,
                home: None
            }
        );
        // Named user resolves uid/gid/home from passwd.
        assert_eq!(
            resolve_user(root, Some("postgres")),
            ResolvedUser {
                uid: 70,
                gid: 70,
                home: Some("/var/lib/postgresql".into())
            }
        );
        // Numeric uid with a passwd entry picks up its gid and home.
        assert_eq!(
            resolve_user(root, Some("70")),
            ResolvedUser {
                uid: 70,
                gid: 70,
                home: Some("/var/lib/postgresql".into())
            }
        );
        // Numeric uid without a passwd entry is used as-is.
        assert_eq!(
            resolve_user(root, Some("1234")),
            ResolvedUser {
                uid: 1234,
                gid: 1234,
                home: None
            }
        );
        // user:group with a named group resolves via /etc/group.
        assert_eq!(
            resolve_user(root, Some("postgres:staff")),
            ResolvedUser {
                uid: 70,
                gid: 50,
                home: Some("/var/lib/postgresql".into())
            }
        );
        // uid:gid stays numeric.
        assert_eq!(
            resolve_user(root, Some("1000:2000")),
            ResolvedUser {
                uid: 1000,
                gid: 2000,
                home: None
            }
        );
        // Unknown name falls back to root.
        assert_eq!(
            resolve_user(root, Some("ghost")),
            ResolvedUser {
                uid: 0,
                gid: 0,
                home: None
            }
        );
    }

    #[test]
    fn build_spec_sets_command_env_user_and_host_networking() {
        let image = ImageConfig {
            env: vec!["FROM_IMAGE=1".to_string()],
            working_dir: Some("/app".to_string()),
            user: Some("1000:1000".to_string()),
            layers: vec![],
        };
        let user = ResolvedUser {
            uid: 1000,
            gid: 1000,
            home: Some("/home/app".to_string()),
        };
        let mut task_env = HashMap::new();
        task_env.insert("MODE".to_string(), "test".to_string());
        let spec = build_spec(
            &["/bin/echo".to_string(), "hi".to_string()],
            &task_env,
            &image,
            &user,
            "legion-7",
            &[],
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
        // HOME comes from the resolved user when the env doesn't set it.
        assert!(env.iter().any(|e| e == "HOME=/home/app"));
        // No private network namespace: container uses host networking.
        let namespaces = spec["linux"]["namespaces"].as_array().unwrap();
        assert!(!namespaces.iter().any(|n| n["type"] == "network"));
        assert!(namespaces.iter().any(|n| n["type"] == "pid"));
        // Host DNS files are bind-mounted in when they exist on this host.
        let mounts = spec["mounts"].as_array().unwrap();
        for path in ["/etc/resolv.conf", "/etc/hosts"] {
            assert_eq!(
                mounts.iter().any(|m| m["destination"] == path),
                Path::new(path).is_file(),
                "mount presence for {path} should match host file presence"
            );
        }
    }

    #[test]
    fn env_home_not_overridden_when_set() {
        let image = ImageConfig {
            env: vec!["HOME=/from-image".to_string()],
            working_dir: None,
            user: None,
            layers: vec![],
        };
        let user = ResolvedUser {
            uid: 70,
            gid: 70,
            home: Some("/var/lib/postgresql".to_string()),
        };
        let spec = build_spec(
            &["true".to_string()],
            &HashMap::new(),
            &image,
            &user,
            "h",
            &[],
        );
        let env = spec["process"]["env"].as_array().unwrap();
        assert!(env.iter().any(|e| e == "HOME=/from-image"));
        assert!(!env.iter().any(|e| e == "HOME=/var/lib/postgresql"));
    }
}
