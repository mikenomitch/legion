//! OCI image layout → OCI runtime bundle conversion.
//!
//! Low-level OCI runtimes (youki, runc, crun) do not pull or run images by
//! reference; they run a *bundle*: an extracted root filesystem plus a
//! `config.json` runtime spec. This module turns the on-disk OCI **image**
//! layout produced by the `oci://` artifact fetcher into that bundle so the
//! container task runtime can run it on Linux.
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
#[derive(Default)]
pub(crate) struct ImageConfig {
    pub(crate) env: Vec<String>,
    pub(crate) working_dir: Option<String>,
    pub(crate) user: Option<String>,
    /// The image's declared `StopSignal`, when it has one. Docker honors it
    /// on `stop` unless the caller overrides; Legion uses it as the middle
    /// layer of the same default chain (jobspec → image → `SIGTERM`).
    pub(crate) stop_signal: Option<String>,
    layers: Vec<PathBuf>,
}

/// Materializes an image's root filesystem at `rootfs` by extracting every
/// layer in order, returning the image config so callers can honor its env,
/// working dir, and user. Shared by the bundle path (container kind) and the
/// microVM disk-image path (sandbox kind) — both need exactly this.
///
/// The directory is cleared first: a rootfs assembled over stale content
/// would silently resurrect files a newer image deleted.
pub(crate) fn materialize_rootfs(oci_layout: &Path, rootfs: &Path) -> R<ImageConfig> {
    materialize_rootfs_shifted(oci_layout, rootfs, None)
}

/// [`materialize_rootfs`], writing every file's owner already shifted into
/// `shift` (a user-namespaced container's host id range): the layer's
/// `uid:gid` becomes `base+uid:base+gid`, so container root owns its rootfs
/// without a separate chown pass.
pub(crate) fn materialize_rootfs_shifted(
    oci_layout: &Path,
    rootfs: &Path,
    shift: Option<crate::task_plugins::userns::IdRange>,
) -> R<ImageConfig> {
    let image = read_image_config(oci_layout)?;
    if rootfs.exists() {
        std::fs::remove_dir_all(rootfs)
            .with_context(|| format!("clear rootfs {}", rootfs.display()))?;
    }
    std::fs::create_dir_all(rootfs)
        .with_context(|| format!("create rootfs {}", rootfs.display()))?;
    for layer in &image.layers {
        extract_layer(layer, rootfs, shift)
            .with_context(|| format!("extract layer {}", layer.display()))?;
    }
    #[cfg(unix)]
    if let Some(range) = shift {
        // The root directory itself, which no layer entry names.
        std::os::unix::fs::lchown(rootfs, Some(range.host_base), Some(range.host_base))
            .with_context(|| format!("chown {}", rootfs.display()))?;
    }
    Ok(image)
}

/// Builds an OCI runtime bundle at `bundle_dir` from the image `oci_layout`.
///
/// `command` is the container's argv (Legion requires it on the task), `env`
/// the task's env (merged over the image's, task wins), and `ctx` everything
/// about the allocation itself — hostname, mounted volumes, the task
/// directory to expose, and the node's resource decision (see
/// [`BundleContext`]). Shared with the sandbox kind's dev mode, which runs an
/// OCI bundle too. The bundle is rebuilt from scratch each call.
///
/// Returns the image's declared `StopSignal`, when it has one, for the
/// container runtime's stop-signal default chain.
pub(crate) fn build_bundle(
    oci_layout: &Path,
    bundle_dir: &Path,
    command: &[String],
    env: &std::collections::HashMap<String, String>,
    ctx: &BundleContext<'_>,
) -> R<Option<String>> {
    if command.is_empty() {
        bail!("container command must include at least one entry");
    }

    if bundle_dir.exists() {
        std::fs::remove_dir_all(bundle_dir)
            .with_context(|| format!("clear bundle dir {}", bundle_dir.display()))?;
    }
    let rootfs = bundle_dir.join("rootfs");
    let image = materialize_rootfs_shifted(oci_layout, &rootfs, ctx.security.id_map)?;

    // Resolve the image user against the *extracted* rootfs so named users
    // (`postgres`, `nobody:nogroup`) map to their real uid/gid instead of
    // silently running as root.
    let user = resolve_user(&rootfs, image.user.as_deref());
    // A per-allocation resolver gets its own resolv.conf in the bundle, bind
    // mounted over the image's. Written here, next to config.json, so it
    // lives exactly as long as the bundle.
    let resolv_conf = match ctx.resolver {
        Some(resolver) => {
            let path = bundle_dir.join("resolv.conf");
            std::fs::write(&path, resolv_conf_contents(resolver))
                .with_context(|| format!("write {}", path.display()))?;
            Some(path)
        }
        None => None,
    };
    let ctx = BundleContext {
        hostname: ctx.hostname,
        volumes: ctx.volumes,
        task_dir: ctx.task_dir,
        assignment: ctx.assignment,
        resolver: ctx.resolver,
        resolv_conf: resolv_conf.as_deref(),
        netns: ctx.netns,
        security: ctx.security,
    };
    let spec = build_spec(command, env, &image, &user, &ctx);
    let config_path = bundle_dir.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&spec)?)
        .with_context(|| format!("write {}", config_path.display()))?;
    Ok(image.stop_signal)
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

pub(crate) fn read_image_config(layout: &Path) -> R<ImageConfig> {
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
    let stop_signal = cfg["StopSignal"]
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
        stop_signal,
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
fn extract_layer(
    blob: &Path,
    rootfs: &Path,
    shift: Option<crate::task_plugins::userns::IdRange>,
) -> R<()> {
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
        let unpacked = entry.unpack_in(rootfs)?;
        #[cfg(unix)]
        if let (true, Some(range)) = (unpacked, shift) {
            let header = entry.header();
            let uid = range.shift(header.uid().unwrap_or(0));
            let gid = range.shift(header.gid().unwrap_or(0));
            let (Some(uid), Some(gid)) = (uid, gid) else {
                bail!(
                    "{} is owned by an id outside the container's 0-{} range",
                    rel.display(),
                    range.size - 1
                );
            };
            // `lchown`: a symlink is re-owned, never followed.
            std::os::unix::fs::lchown(rootfs.join(&rel), Some(uid), Some(gid))
                .with_context(|| format!("chown {}", rel.display()))?;
        }
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
/// Shared by the bundle spec and the sandbox kind's guest env file.
pub(crate) fn merge_env_map(
    image_env: &[String],
    task_env: &std::collections::HashMap<String, String>,
) -> BTreeMap<String, String> {
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
}

fn merge_env(
    image_env: &[String],
    task_env: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    merge_env_map(image_env, task_env)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

/// The image `User` resolved to concrete ids, plus the home directory when it
/// came from a passwd entry (images expect `HOME` set, as Docker does).
#[derive(Debug, PartialEq, Eq, Default)]
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
fn dns_mounts(resolv_conf: Option<&Path>) -> Vec<Value> {
    let mut mounts: Vec<Value> = Vec::new();
    match resolv_conf {
        // The allocation's own resolver (name-is-config): its resolv.conf,
        // generated into the bundle, over the image's.
        Some(path) => mounts.push(json!({
            "destination": "/etc/resolv.conf",
            "type": "bind",
            "source": path,
            "options": ["bind", "ro"],
        })),
        None => {
            if Path::new("/etc/resolv.conf").is_file() {
                mounts.push(json!({
                    "destination": "/etc/resolv.conf",
                    "type": "bind",
                    "source": "/etc/resolv.conf",
                    "options": ["bind", "ro"],
                }));
            }
        }
    }
    if Path::new("/etc/hosts").is_file() {
        mounts.push(json!({
            "destination": "/etc/hosts",
            "type": "bind",
            "source": "/etc/hosts",
            "options": ["bind", "ro"],
        }));
    }
    mounts
}

/// `resolv.conf` for a task whose resolver is the allocation's own: one
/// nameserver, the `svc.legion` search domain so `db` resolves as
/// `db.svc.legion`, and `ndots:1` so external names never pay a failed
/// search-domain probe (the K8s `ndots:5` tax, avoided on purpose).
pub(crate) fn resolv_conf_contents(resolver: &str) -> String {
    format!(
        "nameserver {resolver}\nsearch {}\noptions ndots:1\n",
        crate::structs::SVC_ZONE
    )
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
fn volume_mounts(volumes: &[crate::task_plugins::MountedVolume]) -> Vec<Value> {
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

/// The `linux.resources` block for a task, and the `cgroupsPath` the runtime
/// should create its cgroup at.
///
/// This is the `RuntimeApplied` half of the enforcement contract: worker core
/// decided the numbers (see `src/worker/resources.rs`), and the OCI runtime
/// binary creates the cgroup at the given path — which places it inside the
/// worker's `legion/share` or `legion/reserve` parent, so the share mask
/// governs it exactly like a worker-created one.
///
/// Declared-only, mirroring the macOS guest-sizing rule: a job that declares
/// nothing gets no `memory`/`cpu` entries and stays unconstrained.
fn resource_limits(assignment: &crate::task_plugins::ResourceAssignment) -> Value {
    let mut resources = serde_json::Map::new();
    if let Some(bytes) = assignment.memory_limit_bytes {
        // Swap set equal to the limit means the cgroup may use `limit` bytes
        // of memory+swap combined — i.e. swap cannot be used to escape it
        // (the K8s semantic). Docker's default of 2x-limit-in-swap makes
        // limits soft in practice, which defeats scheduling against them.
        let mut memory = json!({ "limit": bytes, "swap": bytes });
        // `reservation` is cgroup v2 `memory.low` in runc and crun: the
        // reservation the kernel protects from reclaim while a bursting
        // neighbour gives memory back first.
        if let Some(low) = assignment.memory_low_bytes {
            memory["reservation"] = json!(low);
        }
        resources.insert("memory".into(), memory);
    }
    let mut cpu = serde_json::Map::new();
    if let Some(cores) = assignment.cores.as_ref().filter(|cores| !cores.is_empty()) {
        cpu.insert(
            "cpus".into(),
            Value::String(crate::worker::resources::format_cpu_list(cores)),
        );
    }
    if let Some(shares) = assignment.cpu_weight {
        // OCI `cpu.shares`; cgroup-v2 runtimes convert it to `cpu.weight`
        // with the standard mapping. Nomad's convention is shares == MHz.
        cpu.insert("shares".into(), json!(shares));
    }
    if !cpu.is_empty() {
        resources.insert("cpu".into(), Value::Object(cpu));
    }
    Value::Object(resources)
}

/// Everything about *this allocation* that shapes its bundle, as opposed to
/// the image and the command: what the container is called, what is mounted
/// into it, and how the node decided to confine it.
pub(crate) struct BundleContext<'a> {
    /// Container hostname (and OCI container id): `legion-<alloc>`.
    pub(crate) hostname: &'a str,
    /// Volumes the worker mounted for this allocation.
    pub(crate) volumes: &'a [crate::task_plugins::MountedVolume],
    /// Host task directory to mount at [`crate::constants::CONTAINER_TASK_DIR`],
    /// when the runtime exposes one (the `container` runtime does; the
    /// sandbox runtime's dev mode does not).
    pub(crate) task_dir: Option<&'a Path>,
    /// Worker-computed CPU/memory placement, for a `RuntimeApplied` runtime.
    pub(crate) assignment: Option<&'a crate::task_plugins::ResourceAssignment>,
    /// The allocation's own resolver (`LEGION_DNS`, docs/network-handlers.md
    /// "Name-is-config"). When set, the container gets a generated
    /// `/etc/resolv.conf` pointing at it instead of the host's, so
    /// `<key>.svc.legion` resolves and egress policy applies at resolution.
    pub(crate) resolver: Option<&'a str>,
    /// Filled in by `build_bundle`: the generated resolv.conf's host path.
    pub(crate) resolv_conf: Option<&'a Path>,
    /// A gated network namespace (`network.mode = "gated"`) the container
    /// joins instead of the host's: `/var/run/netns/legion-<allocation>`,
    /// created by the worker before start and holding loopback plus the
    /// datapath's listeners only.
    pub(crate) netns: Option<&'a Path>,
    /// The privilege floor (docs/task-runtime-container.md, "Security").
    pub(crate) security: BundleSecurity,
}

/// What confines a container beyond its namespaces and capabilities
/// (worker-plane-auth plan §6.14). `Default` is none of it, which is what
/// the sandbox runtime's dev mode and the bundle tests ask for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BundleSecurity {
    /// A user namespace mapping container ids `0-65535` to this host range.
    /// The rootfs is extracted with shifted owners, and the task directory
    /// and volumes are idmapped mounts, so files keep their host owners.
    pub(crate) id_map: Option<crate::task_plugins::userns::IdRange>,
    /// The default seccomp profile (`crate::task_plugins::seccomp`).
    pub(crate) seccomp: bool,
    /// A cgroup namespace: the container sees its own cgroup as the root.
    pub(crate) cgroup_namespace: bool,
}

fn build_spec(
    command: &[String],
    task_env: &std::collections::HashMap<String, String>,
    image: &ImageConfig,
    user: &ResolvedUser,
    ctx: &BundleContext<'_>,
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
    ];
    // Inside a user namespace, `sysfs` can only be mounted when the network
    // namespace belongs to that user namespace too, and a gated or host
    // network namespace does not. A read-only recursive bind of the host's
    // `/sys` works on every runtime (and is what runc falls back to).
    if ctx.security.id_map.is_some() {
        mounts.push(
            json!({ "destination": "/sys", "type": "bind", "source": "/sys",
              "options": ["rbind", "nosuid", "noexec", "nodev", "ro"] }),
        );
    } else {
        mounts.push(
            json!({ "destination": "/sys", "type": "sysfs", "source": "sysfs",
              "options": ["nosuid", "noexec", "nodev", "ro"] }),
        );
    }
    mounts.extend(dns_mounts(ctx.resolv_conf));
    // The allocation's task directory, read-write, at the canonical mount
    // point. The *directory* is mounted, never individual files: the worker
    // replaces rendered files atomically by rename, which stays visible
    // through a directory bind mount and silently breaks through a per-file
    // one (the well-known docker pitfall).
    if let Some(task_dir) = ctx.task_dir {
        mounts.push(idmapped(
            json!({
                "destination": crate::constants::CONTAINER_TASK_DIR,
                "type": "bind",
                "source": task_dir.display().to_string(),
                "options": ["rbind", "rprivate", "rw"],
            }),
            ctx.security.id_map,
        ));
    }
    // Volumes last so a job can deliberately shadow a default mount point —
    // the task-dir mount included.
    mounts.extend(
        volume_mounts(ctx.volumes)
            .into_iter()
            .map(|mount| idmapped(mount, ctx.security.id_map)),
    );

    // Namespaces: pid/ipc/uts/mount as before, plus — for a gated
    // allocation — the worker-created network namespace, joined by path. A
    // task without one shares the host's network, exactly as before.
    let mut namespaces = vec![
        json!({ "type": "pid" }),
        json!({ "type": "ipc" }),
        json!({ "type": "uts" }),
        json!({ "type": "mount" }),
    ];
    if let Some(netns) = ctx.netns {
        namespaces.push(json!({ "type": "network", "path": netns.display().to_string() }));
    }
    if ctx.security.id_map.is_some() {
        namespaces.push(json!({ "type": "user" }));
    }
    if ctx.security.cgroup_namespace {
        namespaces.push(json!({ "type": "cgroup" }));
    }

    // The device-cgroup deny rule is unconditional; declared limits are
    // layered over it, so a task that declares nothing keeps exactly the
    // spec it had before enforcement existed.
    let mut linux_resources = json!({ "devices": [{ "allow": false, "access": "rwm" }] });
    if let Some(assignment) = ctx.assignment {
        if let (Some(target), Some(limits)) = (
            linux_resources.as_object_mut(),
            resource_limits(assignment).as_object(),
        ) {
            for (key, value) in limits {
                target.insert(key.clone(), value.clone());
            }
        }
    }

    let mut spec = json!({
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
        "hostname": ctx.hostname,
        "mounts": mounts,
        "linux": {
            "resources": linux_resources,
            "namespaces": namespaces,
            "maskedPaths": [
                "/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys",
                "/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats",
                "/proc/sched_debug", "/sys/firmware", "/proc/scsi"
            ],
            "readonlyPaths": [
                "/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"
            ]
        }
    });

    // Explicit cgroupfs form (`legion/share/legion-<alloc>`), so an operator
    // can find a task's cgroup by allocation id and the share/reserve split
    // is visible in the hierarchy. Omitted entirely when nothing governs this
    // task, leaving the runtime's own default placement.
    if let Some(assignment) = ctx.assignment {
        spec["linux"]["cgroupsPath"] = Value::String(assignment.cgroup_path.display().to_string());
    }
    if let Some(range) = ctx.security.id_map {
        spec["linux"]["uidMappings"] = range.oci_mapping();
        spec["linux"]["gidMappings"] = range.oci_mapping();
    }
    if ctx.security.seccomp {
        spec["linux"]["seccomp"] = crate::task_plugins::seccomp::default_profile();
    }
    spec
}

/// A bind mount, made an idmapped mount under a user namespace: files keep
/// their host owners on disk, and appear inside with the container's ids
/// (a host-root-owned file is the container root's). So a volume reattached
/// to an allocation with a different range keeps working, and files the
/// worker writes into the task directory later stay readable inside.
fn idmapped(mut mount: Value, id_map: Option<crate::task_plugins::userns::IdRange>) -> Value {
    if let Some(range) = id_map {
        mount["uidMappings"] = range.oci_mapping();
        mount["gidMappings"] = range.oci_mapping();
    }
    mount
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
    fn gated_context_joins_the_network_namespace_by_path() {
        let image = ImageConfig::default();
        let user = ResolvedUser::default();
        let netns = Path::new("/var/run/netns/legion-7");
        let ctx = BundleContext {
            hostname: "legion-7",
            volumes: &[],
            task_dir: None,
            assignment: None,
            resolver: Some("127.42.0.1"),
            resolv_conf: None,
            netns: Some(netns),
            security: BundleSecurity::default(),
        };
        let spec = build_spec(
            &["true".to_string()],
            &Default::default(),
            &image,
            &user,
            &ctx,
        );
        let namespaces = spec["linux"]["namespaces"].as_array().unwrap();
        let network = namespaces
            .iter()
            .find(|n| n["type"] == "network")
            .expect("network namespace");
        assert_eq!(network["path"], "/var/run/netns/legion-7");
        // Not gated: no network namespace, as before.
        let plain = BundleContext { netns: None, ..ctx };
        let spec = build_spec(
            &["true".to_string()],
            &Default::default(),
            &image,
            &user,
            &plain,
        );
        assert!(!spec["linux"]["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["type"] == "network"));
    }

    #[test]
    fn the_privilege_floor_shapes_the_spec_test() {
        let image = ImageConfig::default();
        let user = ResolvedUser::default();
        let range = crate::task_plugins::userns::IdRange::for_slot(3);
        let task_dir = Path::new("/srv/legion/allocs/7/task");
        let ctx = BundleContext {
            hostname: "legion-7",
            volumes: &[],
            task_dir: Some(task_dir),
            assignment: None,
            resolver: None,
            resolv_conf: None,
            netns: None,
            security: BundleSecurity {
                id_map: Some(range),
                seccomp: true,
                cgroup_namespace: true,
            },
        };
        let spec = build_spec(
            &["true".to_string()],
            &Default::default(),
            &image,
            &user,
            &ctx,
        );
        let namespaces: Vec<&str> = spec["linux"]["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|n| n["type"].as_str())
            .collect();
        assert!(namespaces.contains(&"user") && namespaces.contains(&"cgroup"));
        let mapping = json!([{ "containerID": 0, "hostID": range.host_base, "size": 65536 }]);
        assert_eq!(spec["linux"]["uidMappings"], mapping);
        assert_eq!(spec["linux"]["gidMappings"], mapping);
        assert_eq!(spec["linux"]["seccomp"]["defaultAction"], "SCMP_ACT_ALLOW");
        let mounts = spec["mounts"].as_array().unwrap();
        let sys = mounts.iter().find(|m| m["destination"] == "/sys").unwrap();
        assert_eq!(
            sys["type"], "bind",
            "a user namespace binds /sys instead of mounting sysfs"
        );
        let task = mounts
            .iter()
            .find(|m| m["destination"] == crate::constants::CONTAINER_TASK_DIR)
            .unwrap();
        assert_eq!(
            task["uidMappings"], mapping,
            "the task directory is idmapped"
        );

        // With the floor off, none of it.
        let plain = BundleContext {
            security: BundleSecurity::default(),
            ..ctx
        };
        let spec = build_spec(
            &["true".to_string()],
            &Default::default(),
            &image,
            &user,
            &plain,
        );
        assert!(spec["linux"].get("uidMappings").is_none());
        assert!(spec["linux"].get("seccomp").is_none());
        let sys = spec["mounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["destination"] == "/sys")
            .unwrap()
            .clone();
        assert_eq!(sys["type"], "sysfs");
    }

    #[test]
    fn allocation_resolver_replaces_the_host_resolv_conf() {
        let generated = Path::new("/bundle/resolv.conf");
        let mounts = dns_mounts(Some(generated));
        let resolv = mounts
            .iter()
            .find(|m| m["destination"] == "/etc/resolv.conf")
            .expect("resolv.conf mount");
        assert_eq!(resolv["source"], "/bundle/resolv.conf");
        assert_eq!(resolv["options"][1], "ro");
        // Without a resolver the host file is used, as before.
        let mounts = dns_mounts(None);
        if Path::new("/etc/resolv.conf").is_file() {
            let resolv = mounts
                .iter()
                .find(|m| m["destination"] == "/etc/resolv.conf")
                .unwrap();
            assert_eq!(resolv["source"], "/etc/resolv.conf");
        }
        let contents = resolv_conf_contents("127.42.3.1");
        assert_eq!(
            contents,
            "nameserver 127.42.3.1\nsearch svc.legion\noptions ndots:1\n"
        );
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
            stop_signal: None,
            layers: vec![],
        };
        let user = ResolvedUser {
            uid: 1000,
            gid: 1000,
            home: Some("/home/app".to_string()),
        };
        let mut task_env = HashMap::new();
        task_env.insert("MODE".to_string(), "test".to_string());
        let volumes = [crate::task_plugins::MountedVolume {
            name: "state".to_string(),
            mode: crate::volumes::VolumeMode::Filesystem,
            host_path: PathBuf::from("/var/legion/state"),
            path: Some("/workspace".to_string()),
            fs: None,
            fresh: false,
            read_only: false,
        }];
        let spec = build_spec(
            &["/bin/echo".to_string(), "hi".to_string()],
            &task_env,
            &image,
            &user,
            &BundleContext {
                hostname: "legion-7",
                volumes: &volumes,
                task_dir: None,
                assignment: None,
                resolver: None,
                resolv_conf: None,
                netns: None,
                security: BundleSecurity::default(),
            },
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
        // Volumes land as read-write rbind mounts at their declared path.
        let state = mounts
            .iter()
            .find(|mount| mount["destination"] == "/workspace")
            .expect("volume mount present");
        assert_eq!(state["source"], "/var/legion/state");
        assert_eq!(state["type"], "bind");
        assert_eq!(state["options"], json!(["rbind", "rprivate", "rw"]));
        for path in ["/etc/resolv.conf", "/etc/hosts"] {
            assert_eq!(
                mounts.iter().any(|m| m["destination"] == path),
                Path::new(path).is_file(),
                "mount presence for {path} should match host file presence"
            );
        }
    }

    fn bare_image() -> ImageConfig {
        ImageConfig {
            env: vec![],
            working_dir: None,
            user: None,
            stop_signal: None,
            layers: vec![],
        }
    }

    fn spec_with(assignment: Option<&crate::task_plugins::ResourceAssignment>) -> Value {
        build_spec(
            &["true".to_string()],
            &HashMap::new(),
            &bare_image(),
            &ResolvedUser {
                uid: 0,
                gid: 0,
                home: None,
            },
            &BundleContext {
                hostname: "legion-42",
                volumes: &[],
                task_dir: None,
                assignment,
                resolver: None,
                resolv_conf: None,
                netns: None,
                security: BundleSecurity::default(),
            },
        )
    }

    /// A pinned task: exclusive cpuset, hard memory limit with swap equal to
    /// it, and a `cgroupsPath` under the reserve slice.
    #[test]
    fn a_reserve_slice_assignment_lands_in_the_spec() {
        let spec = spec_with(Some(&crate::task_plugins::ResourceAssignment {
            slice: crate::task_plugins::Slice::Reserve,
            cgroup_path: PathBuf::from("legion/reserve/legion-42"),
            cores: Some(vec![3, 7]),
            cpu_weight: None,
            memory_limit_bytes: Some(268_435_456),
            memory_low_bytes: None,
        }));

        assert_eq!(spec["linux"]["cgroupsPath"], "legion/reserve/legion-42");
        assert_eq!(spec["linux"]["resources"]["cpu"]["cpus"], "3,7");
        assert_eq!(
            spec["linux"]["resources"]["memory"]["limit"],
            268_435_456u64
        );
        assert_eq!(
            spec["linux"]["resources"]["memory"]["swap"], 268_435_456u64,
            "swap equals the limit so it cannot be used to escape it"
        );
        // The device deny rule survives untouched.
        assert_eq!(
            spec["linux"]["resources"]["devices"],
            json!([{ "allow": false, "access": "rwm" }])
        );
    }

    /// A share-slice task: proportional weight, no cpuset.
    #[test]
    fn a_share_slice_assignment_carries_shares_not_cpus() {
        let spec = spec_with(Some(&crate::task_plugins::ResourceAssignment {
            slice: crate::task_plugins::Slice::Share,
            cgroup_path: PathBuf::from("legion/share/legion-42"),
            cores: None,
            cpu_weight: Some(500),
            memory_limit_bytes: Some(134_217_728),
            memory_low_bytes: None,
        }));

        assert_eq!(spec["linux"]["cgroupsPath"], "legion/share/legion-42");
        assert_eq!(spec["linux"]["resources"]["cpu"]["shares"], 500);
        assert!(spec["linux"]["resources"]["cpu"]["cpus"].is_null());
        assert_eq!(
            spec["linux"]["resources"]["memory"]["limit"],
            134_217_728u64
        );
    }

    /// Declared-only, mirroring the macOS guest-sizing rule: a job that
    /// declares nothing is left unconstrained, and an ungoverned node's spec
    /// is byte-identical to what it was before enforcement existed.
    #[test]
    fn an_undeclared_task_gets_no_limits() {
        let governed_but_empty = spec_with(Some(&crate::task_plugins::ResourceAssignment {
            slice: crate::task_plugins::Slice::Share,
            cgroup_path: PathBuf::from("legion/share/legion-42"),
            cores: None,
            cpu_weight: None,
            memory_limit_bytes: None,
            memory_low_bytes: None,
        }));
        assert!(governed_but_empty["linux"]["resources"]["memory"].is_null());
        assert!(governed_but_empty["linux"]["resources"]["cpu"].is_null());
        // It still gets a cgroup path: that is what makes reserved cores
        // off-limits to it.
        assert_eq!(
            governed_but_empty["linux"]["cgroupsPath"],
            "legion/share/legion-42"
        );

        let ungoverned = spec_with(None);
        assert!(ungoverned["linux"]["cgroupsPath"].is_null());
        assert_eq!(
            ungoverned["linux"]["resources"],
            json!({ "devices": [{ "allow": false, "access": "rwm" }] })
        );
    }

    /// The task dir is mounted read-write at the canonical path, as a
    /// *directory* (so host-side atomic renames stay visible), and before
    /// declared volumes so a job can still shadow it.
    #[test]
    fn the_task_dir_mounts_at_the_canonical_path() {
        let spec = build_spec(
            &["true".to_string()],
            &HashMap::new(),
            &bare_image(),
            &ResolvedUser {
                uid: 0,
                gid: 0,
                home: None,
            },
            &BundleContext {
                hostname: "legion-42",
                volumes: &[crate::task_plugins::MountedVolume {
                    name: "data".to_string(),
                    mode: crate::volumes::VolumeMode::Filesystem,
                    host_path: PathBuf::from("/var/legion/data"),
                    path: Some("/data".to_string()),
                    fs: None,
                    fresh: false,
                    read_only: false,
                }],
                task_dir: Some(Path::new("/var/legion/allocs/42/web")),
                assignment: None,
                resolver: None,
                resolv_conf: None,
                netns: None,
                security: BundleSecurity::default(),
            },
        );

        let mounts = spec["mounts"].as_array().expect("mounts");
        let task_mount = mounts
            .iter()
            .position(|mount| mount["destination"] == "/legion/task")
            .expect("task dir mounted");
        assert_eq!(mounts[task_mount]["source"], "/var/legion/allocs/42/web");
        assert_eq!(mounts[task_mount]["type"], "bind");
        assert_eq!(
            mounts[task_mount]["options"],
            json!(["rbind", "rprivate", "rw"])
        );

        let volume = mounts
            .iter()
            .position(|mount| mount["destination"] == "/data")
            .expect("declared volume mounted");
        assert!(
            task_mount < volume,
            "the task dir mounts before declared volumes so one can shadow it"
        );

        // Without a task dir there is no mount at all — the pre-existing
        // spec, unchanged.
        let bare = spec_with(None);
        assert!(!bare["mounts"]
            .as_array()
            .expect("mounts")
            .iter()
            .any(|mount| mount["destination"] == "/legion/task"));
    }

    #[test]
    fn env_home_not_overridden_when_set() {
        let image = ImageConfig {
            env: vec!["HOME=/from-image".to_string()],
            working_dir: None,
            user: None,
            stop_signal: None,
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
            &BundleContext {
                hostname: "h",
                volumes: &[],
                task_dir: None,
                assignment: None,
                resolver: None,
                resolv_conf: None,
                netns: None,
                security: BundleSecurity::default(),
            },
        );
        let env = spec["process"]["env"].as_array().unwrap();
        assert!(env.iter().any(|e| e == "HOME=/from-image"));
        assert!(!env.iter().any(|e| e == "HOME=/var/lib/postgresql"));
    }
}
