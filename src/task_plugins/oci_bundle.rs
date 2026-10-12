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
//! layer tarball into `rootfs/` (honoring overlayfs whiteouts, and keeping each
//! file's owner, mode, and file capabilities), and synthesize a runtime spec
//! that merges the image config (env, working dir, user) with the task's
//! command and env.
//!
//! The spec's process privileges follow Docker's defaults, because stock
//! images are written against them: Docker's capability set (less
//! `CAP_NET_RAW`, see [`DEFAULT_CAPABILITIES`]) and a high `RLIMIT_NOFILE`.
//! A container task adjusts either through its config
//! (`security.capabilities`, `ulimits`).

use crate::prelude::*;
use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::path::{Component, Path, PathBuf};

/// The capabilities a container gets when its jobspec adjusts nothing:
/// Docker's default set (moby `oci/caps/defaults.go`) without `CAP_NET_RAW`.
///
/// Stock images assume these. The official nginx and Postgres entrypoints
/// `chown` their directories and drop to an unprivileged user with
/// `setuid`/`setgid` (`gosu`, `su-exec`), which needs `CAP_CHOWN`,
/// `CAP_SETUID`, and `CAP_SETGID`.
///
/// `CAP_NET_RAW` is the one deliberate difference. A Docker container has its
/// own network namespace, so raw sockets there see only its own traffic. A
/// Legion container shares the node's network namespace unless it is gated,
/// and `CAP_NET_RAW` there means sniffing and spoofing on the node's
/// interfaces. Nomad's Docker driver drops it from its default for the same
/// reason. A task that needs it (`ping`, `traceroute`) adds it back with
/// `security.capabilities.add = ["NET_RAW"]`.
pub(crate) const DEFAULT_CAPABILITIES: &[&str] = &[
    "CAP_AUDIT_WRITE",
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FOWNER",
    "CAP_FSETID",
    "CAP_KILL",
    "CAP_MKNOD",
    "CAP_NET_BIND_SERVICE",
    "CAP_SETFCAP",
    "CAP_SETGID",
    "CAP_SETPCAP",
    "CAP_SETUID",
    "CAP_SYS_CHROOT",
];

/// Every capability Linux defines, through 5.9's `CAP_CHECKPOINT_RESTORE`.
/// The vocabulary `security.capabilities` is validated against, and what
/// `add = ["ALL"]` expands to.
pub(crate) const ALL_CAPABILITIES: &[&str] = &[
    "CAP_AUDIT_CONTROL",
    "CAP_AUDIT_READ",
    "CAP_AUDIT_WRITE",
    "CAP_BLOCK_SUSPEND",
    "CAP_BPF",
    "CAP_CHECKPOINT_RESTORE",
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_DAC_READ_SEARCH",
    "CAP_FOWNER",
    "CAP_FSETID",
    "CAP_IPC_LOCK",
    "CAP_IPC_OWNER",
    "CAP_KILL",
    "CAP_LEASE",
    "CAP_LINUX_IMMUTABLE",
    "CAP_MAC_ADMIN",
    "CAP_MAC_OVERRIDE",
    "CAP_MKNOD",
    "CAP_NET_ADMIN",
    "CAP_NET_BIND_SERVICE",
    "CAP_NET_BROADCAST",
    "CAP_NET_RAW",
    "CAP_PERFMON",
    "CAP_SETFCAP",
    "CAP_SETGID",
    "CAP_SETPCAP",
    "CAP_SETUID",
    "CAP_SYSLOG",
    "CAP_SYS_ADMIN",
    "CAP_SYS_BOOT",
    "CAP_SYS_CHROOT",
    "CAP_SYS_MODULE",
    "CAP_SYS_NICE",
    "CAP_SYS_PACCT",
    "CAP_SYS_PTRACE",
    "CAP_SYS_RAWIO",
    "CAP_SYS_RESOURCE",
    "CAP_SYS_TIME",
    "CAP_SYS_TTY_CONFIG",
    "CAP_WAKE_ALARM",
];

/// Normalizes one capability name from a jobspec: case-insensitive, with or
/// without the `CAP_` prefix (`net_admin`, `NET_ADMIN`, and `CAP_NET_ADMIN`
/// are the same capability), plus the `ALL` keyword. `field` names the list
/// for the error.
fn normalize_capability(raw: &str, field: &str) -> R<String> {
    let upper = raw.trim().to_ascii_uppercase();
    if upper == "ALL" {
        return Ok(upper);
    }
    let name = if upper.starts_with("CAP_") {
        upper
    } else {
        format!("CAP_{upper}")
    };
    if ALL_CAPABILITIES.contains(&name.as_str()) {
        Ok(name)
    } else {
        bail!(
            "unknown capability {raw:?} in security.capabilities.{field}: use a Linux capability \
             name such as NET_ADMIN (the CAP_ prefix is optional), or ALL"
        )
    }
}

/// The container's capability set from the jobspec's adjustments, with
/// Docker's `--cap-add` / `--cap-drop` semantics:
///
/// - `add` containing `ALL`: every capability except those `drop` names;
/// - `drop` containing `ALL`: exactly the capabilities `add` names;
/// - otherwise: [`DEFAULT_CAPABILITIES`], less `drop`, plus `add` (a name in
///   both lists ends up granted, as in Docker).
///
/// Names are validated, so a typo fails at submit instead of starting a
/// container that quietly lacks the capability it needs. `ALL` in both lists
/// is rejected as contradictory.
pub(crate) fn resolve_capabilities(add: &[String], drop: &[String]) -> R<Vec<String>> {
    let add = add
        .iter()
        .map(|raw| normalize_capability(raw, "add"))
        .collect::<R<Vec<_>>>()?;
    let drop = drop
        .iter()
        .map(|raw| normalize_capability(raw, "drop"))
        .collect::<R<Vec<_>>>()?;
    let add_all = add.iter().any(|cap| cap == "ALL");
    let drop_all = drop.iter().any(|cap| cap == "ALL");
    let mut caps: Vec<String> = match (add_all, drop_all) {
        (true, true) => bail!(
            "security.capabilities lists ALL in both add and drop; keep ALL in one list and name \
             the exceptions in the other"
        ),
        (true, false) => ALL_CAPABILITIES
            .iter()
            .filter(|cap| !drop.iter().any(|dropped| dropped == *cap))
            .map(|cap| cap.to_string())
            .collect(),
        (false, true) => add,
        (false, false) => DEFAULT_CAPABILITIES
            .iter()
            .filter(|cap| !drop.iter().any(|dropped| dropped == *cap))
            .map(|cap| cap.to_string())
            .chain(add)
            .collect(),
    };
    caps.sort();
    caps.dedup();
    Ok(caps)
}

/// The default capability set, as owned strings.
pub(crate) fn default_capabilities() -> Vec<String> {
    DEFAULT_CAPABILITIES
        .iter()
        .map(|cap| cap.to_string())
        .collect()
}

/// One `process.rlimits` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rlimit {
    /// `RLIMIT_NOFILE` and friends.
    pub(crate) kind: String,
    pub(crate) soft: u64,
    pub(crate) hard: u64,
}

/// `RLIM_INFINITY`, which is what `"unlimited"` in a jobspec means.
const RLIM_INFINITY: u64 = u64::MAX;

/// The open-file limit a container gets by default: 1048576 (2^20), the
/// value Docker containers ran with for years (containerd's systemd unit set
/// `LimitNOFILE=infinity`, which the kernel caps at `fs.nr_open`), and what
/// CRI-O sets. The OCI runtime's own default is a hard 1024, which servers
/// like Redis warn about at start and which a busy proxy exhausts.
pub(crate) const DEFAULT_NOFILE: u64 = 1_048_576;

/// The rlimit names `ulimits` accepts, as `(jobspec name, OCI type)`. The
/// same names `docker run --ulimit` and Compose's `ulimits` take.
const RLIMIT_NAMES: &[(&str, &str)] = &[
    ("as", "RLIMIT_AS"),
    ("core", "RLIMIT_CORE"),
    ("cpu", "RLIMIT_CPU"),
    ("data", "RLIMIT_DATA"),
    ("fsize", "RLIMIT_FSIZE"),
    ("locks", "RLIMIT_LOCKS"),
    ("memlock", "RLIMIT_MEMLOCK"),
    ("msgqueue", "RLIMIT_MSGQUEUE"),
    ("nice", "RLIMIT_NICE"),
    ("nofile", "RLIMIT_NOFILE"),
    ("nproc", "RLIMIT_NPROC"),
    ("rss", "RLIMIT_RSS"),
    ("rtprio", "RLIMIT_RTPRIO"),
    ("rttime", "RLIMIT_RTTIME"),
    ("sigpending", "RLIMIT_SIGPENDING"),
    ("stack", "RLIMIT_STACK"),
];

/// The default open-file limit for this host: [`DEFAULT_NOFILE`], lowered to
/// the kernel's `fs.nr_open` when that is smaller (a hard limit above it
/// fails `setrlimit` and the container with it), and to this process's own
/// hard limit when the worker is not root (raising a hard limit needs
/// `CAP_SYS_RESOURCE`).
fn default_nofile() -> u64 {
    let mut limit = DEFAULT_NOFILE;
    if let Some(nr_open) = std::fs::read_to_string("/proc/sys/fs/nr_open")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
    {
        limit = limit.min(nr_open);
    }
    #[cfg(unix)]
    if !running_as_root() {
        let mut current = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit only writes the struct it is handed.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) } == 0 {
            limit = limit.min(current.rlim_max as u64);
        }
    }
    limit
}

/// The rlimits a container gets when its jobspec sets no `ulimits`.
pub(crate) fn default_rlimits() -> Vec<Rlimit> {
    let nofile = default_nofile();
    vec![Rlimit {
        kind: "RLIMIT_NOFILE".to_string(),
        soft: nofile,
        hard: nofile,
    }]
}

/// One `ulimits` value: a number or `"unlimited"`.
fn rlimit_amount(name: &str, which: &str, value: &Value) -> R<u64> {
    match value {
        Value::Number(number) => number.as_u64().with_context(|| {
            format!("ulimits.{name}{which} must be a non-negative whole number or \"unlimited\"")
        }),
        Value::String(word) if word == "unlimited" => Ok(RLIM_INFINITY),
        _ => bail!("ulimits.{name}{which} must be a non-negative whole number or \"unlimited\""),
    }
}

/// The container's rlimits: the defaults, with each `ulimits` entry the
/// jobspec declares replacing the default of the same kind.
///
/// An entry is a number (soft and hard both), `"unlimited"`, or
/// `{ soft, hard }`, with `soft <= hard`. Names are the lowercase rlimit
/// names (`nofile`, `nproc`, `memlock`, ...), validated so a typo fails at
/// submit.
pub(crate) fn resolve_rlimits(ulimits: &BTreeMap<String, Value>) -> R<Vec<Rlimit>> {
    let mut declared: Vec<Rlimit> = Vec::new();
    for (name, value) in ulimits {
        let Some((_, kind)) = RLIMIT_NAMES.iter().find(|(known, _)| known == name) else {
            let known: Vec<&str> = RLIMIT_NAMES.iter().map(|(known, _)| *known).collect();
            bail!(
                "unknown ulimit {name:?}; expected one of {}",
                known.join(", ")
            );
        };
        let (soft, hard) = match value {
            Value::Object(pair) => {
                if let Some(extra) = pair.keys().find(|key| *key != "soft" && *key != "hard") {
                    bail!("ulimits.{name} has an unknown field {extra:?}; use {{ soft, hard }}");
                }
                let soft = pair
                    .get("soft")
                    .with_context(|| format!("ulimits.{name} needs both soft and hard"))?;
                let hard = pair
                    .get("hard")
                    .with_context(|| format!("ulimits.{name} needs both soft and hard"))?;
                (
                    rlimit_amount(name, ".soft", soft)?,
                    rlimit_amount(name, ".hard", hard)?,
                )
            }
            single => {
                let amount = rlimit_amount(name, "", single)?;
                (amount, amount)
            }
        };
        if soft > hard {
            bail!("ulimits.{name}: soft ({soft}) must not exceed hard ({hard})");
        }
        declared.push(Rlimit {
            kind: kind.to_string(),
            soft,
            hard,
        });
    }
    let mut rlimits: Vec<Rlimit> = default_rlimits()
        .into_iter()
        .filter(|default| !declared.iter().any(|set| set.kind == default.kind))
        .collect();
    rlimits.extend(declared);
    Ok(rlimits)
}

/// The privileges and limits of the container's process: its capability set
/// and its rlimits. [`Default`] is what a task that adjusts nothing gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessPolicy {
    /// Granted as the bounding, effective, and permitted sets, as Docker
    /// does. The inheritable and ambient sets stay empty, so a non-root
    /// process keeps none of them across `execve`.
    pub(crate) capabilities: Vec<String>,
    pub(crate) rlimits: Vec<Rlimit>,
}

impl Default for ProcessPolicy {
    fn default() -> Self {
        Self {
            capabilities: default_capabilities(),
            rlimits: default_rlimits(),
        }
    }
}

/// Whether this process runs as root, which decides whether extraction can
/// keep a layer's owners and whether the task's log files can be handed to
/// the container's user.
fn running_as_root() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and cannot fail.
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Hands the task's captured `stdout.log` / `stderr.log` to the container's
/// user, so a non-root process can reopen its own output by path.
///
/// The container's stdout and stderr are these files, opened by the worker.
/// A process that writes to them through its inherited descriptors needs
/// nothing more, but many images reopen them: nginx symlinks
/// `/var/log/nginx/access.log` to `/dev/stdout`, and `/dev/stdout` is
/// `/proc/self/fd/1`. Reopening goes through the file's own permission check,
/// so a root-owned file refuses a non-root user with `EACCES`, and nginx (or
/// any image with a `USER`) fails to start. Docker avoids this by chowning the
/// stdio pipes it hands the container to the container's user (moby's
/// `IoUid`/`IoGid`).
///
/// The log directory itself stays owner-only (0700, root): reopening through
/// `/proc/self/fd` does not traverse it, so the container gains access to its
/// own two files and nothing else. Re-run at every start, because an
/// in-place restart may run a different image user. A no-op when the worker
/// is not root (it cannot chown), or when the task has no log directory.
pub(crate) fn hand_task_logs_to(log_dir: Option<&Path>, uid: u32, gid: u32) {
    let Some(log_dir) = log_dir else {
        return;
    };
    if !running_as_root() {
        return;
    }
    for file in [
        crate::task_plugins::STDOUT_LOG_FILE,
        crate::task_plugins::STDERR_LOG_FILE,
    ] {
        let path = log_dir.join(file);
        #[cfg(unix)]
        if let Err(err) = std::os::unix::fs::chown(&path, Some(uid), Some(gid)) {
            warn!(
                path = %path.display(),
                uid,
                gid,
                error = %err,
                "Could not hand the task log to the container user; a non-root container cannot \
                 reopen /dev/stdout or /dev/stderr"
            );
        }
    }
}

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
    let image = read_image_config(oci_layout)?;
    if rootfs.exists() {
        std::fs::remove_dir_all(rootfs)
            .with_context(|| format!("clear rootfs {}", rootfs.display()))?;
    }
    std::fs::create_dir_all(rootfs)
        .with_context(|| format!("create rootfs {}", rootfs.display()))?;
    for layer in &image.layers {
        extract_layer(layer, rootfs)
            .with_context(|| format!("extract layer {}", layer.display()))?;
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
/// Returns what the caller needs from the image: its declared `StopSignal`
/// (for the container runtime's stop-signal default chain) and the uid/gid
/// the process runs as (to hand it the task's log files).
pub(crate) fn build_bundle(
    oci_layout: &Path,
    bundle_dir: &Path,
    command: &[String],
    env: &std::collections::HashMap<String, String>,
    ctx: &BundleContext<'_>,
) -> R<BuiltBundle> {
    if command.is_empty() {
        bail!("container command must include at least one entry");
    }

    if bundle_dir.exists() {
        std::fs::remove_dir_all(bundle_dir)
            .with_context(|| format!("clear bundle dir {}", bundle_dir.display()))?;
    }
    let rootfs = bundle_dir.join("rootfs");
    let image = materialize_rootfs(oci_layout, &rootfs)?;

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
        policy: ctx.policy,
    };
    let spec = build_spec(command, env, &image, &user, &ctx);
    let config_path = bundle_dir.join("config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&spec)?)
        .with_context(|| format!("write {}", config_path.display()))?;
    Ok(BuiltBundle {
        stop_signal: image.stop_signal,
        uid: user.uid,
        gid: user.gid,
    })
}

/// What [`build_bundle`] learned from the image that its caller acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuiltBundle {
    /// The image's declared `StopSignal`, when it has one.
    pub(crate) stop_signal: Option<String>,
    /// The uid the container's process runs as (the image's `User`,
    /// resolved against its rootfs).
    pub(crate) uid: u32,
    /// The gid the container's process runs as.
    pub(crate) gid: u32,
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
///
/// Each entry keeps what the image recorded for it, the way Docker's
/// extraction does:
///
/// - **Owner (uid/gid)**, when the worker runs as root. An image that runs as
///   a non-root `USER` relies on it: `RUN chown nginx /var/cache/nginx` is how
///   nginx's image lets its workers write there, and a rootfs where every
///   file is `0:0` breaks it. A non-root worker cannot chown, so there every
///   file belongs to the worker's user (and the container runtime runs
///   rootless anyway).
/// - **Mode, including setuid/setgid/sticky.** Ownership is applied before
///   the mode, because `chown` clears the setuid bits.
/// - **File capabilities and other extended attributes** (PAX
///   `SCHILY.xattr.*` records), best effort and only as root. `security.capability`
///   is what lets an image grant `cap_net_bind_service` to one binary.
///   `security.selinux` and `trusted.*` are skipped: labels belong to the
///   host's policy, and `trusted.overlay.*` is overlayfs bookkeeping, not
///   image content. A filesystem that refuses an attribute logs and moves on.
fn extract_layer(blob: &Path, rootfs: &Path) -> R<()> {
    extract_layer_as(blob, rootfs, running_as_root())
}

/// [`extract_layer`] with the ownership decision made by the caller, so tests
/// can exercise both paths.
fn extract_layer_as(blob: &Path, rootfs: &Path, preserve_owners: bool) -> R<()> {
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
    archive.set_preserve_ownerships(preserve_owners);
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

        // Read before unpacking: the PAX records sit in the header, and the
        // entry's data is consumed by the unpack.
        let xattrs = if preserve_owners {
            entry_xattrs(&mut entry)
        } else {
            Vec::new()
        };
        // `unpack_in` re-validates the path against the rootfs and resolves
        // hardlink/symlink targets safely. It applies the owner, then the
        // mode, so setuid bits survive.
        if entry.unpack_in(rootfs)? && !xattrs.is_empty() {
            // After the chown, which would clear `security.capability`.
            apply_xattrs(&rootfs.join(&rel), &xattrs);
        }
    }
    Ok(())
}

/// The PAX `SCHILY.xattr.<name>` records on a tar entry, minus the ones that
/// must not be copied onto the host: `security.selinux` (the host's policy
/// labels files, not the image) and `trusted.*` (overlayfs bookkeeping).
fn entry_xattrs<T: Read>(entry: &mut tar::Entry<'_, T>) -> Vec<(String, Vec<u8>)> {
    let Ok(Some(extensions)) = entry.pax_extensions() else {
        return Vec::new();
    };
    extensions
        .filter_map(Result::ok)
        .filter_map(|extension| {
            let name = extension.key().ok()?.strip_prefix("SCHILY.xattr.")?;
            if name == "security.selinux" || name.starts_with("trusted.") {
                return None;
            }
            Some((name.to_string(), extension.value_bytes().to_vec()))
        })
        .collect()
}

/// Sets extended attributes on an extracted path, best effort: a filesystem
/// that does not support one (or a kernel that refuses it) costs the image
/// that attribute, not the whole start.
fn apply_xattrs(path: &Path, xattrs: &[(String, Vec<u8>)]) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
            return;
        };
        for (name, value) in xattrs {
            let Ok(c_name) = std::ffi::CString::new(name.as_bytes()) else {
                continue;
            };
            // SAFETY: both strings are NUL-terminated and outlive the call;
            // `value` is a valid buffer of `value.len()` bytes.
            let rc = unsafe {
                libc::lsetxattr(
                    c_path.as_ptr(),
                    c_name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            };
            if rc != 0 {
                debug!(
                    path = %path.display(),
                    xattr = %name,
                    error = %std::io::Error::last_os_error(),
                    "Skipping an image layer's extended attribute this host refuses"
                );
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, xattrs);
    }
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
    /// The process's capability set and rlimits: the defaults, or the
    /// container task's `security.capabilities` / `ulimits`.
    pub(crate) policy: &'a ProcessPolicy,
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
        json!({ "destination": "/sys", "type": "sysfs", "source": "sysfs",
              "options": ["nosuid", "noexec", "nodev", "ro"] }),
    ];
    mounts.extend(dns_mounts(ctx.resolv_conf));
    // The allocation's task directory, read-write, at the canonical mount
    // point. The *directory* is mounted, never individual files: the worker
    // replaces rendered files atomically by rename, which stays visible
    // through a directory bind mount and silently breaks through a per-file
    // one (the well-known docker pitfall).
    if let Some(task_dir) = ctx.task_dir {
        mounts.push(json!({
            "destination": crate::constants::CONTAINER_TASK_DIR,
            "type": "bind",
            "source": task_dir.display().to_string(),
            "options": ["rbind", "rprivate", "rw"],
        }));
    }
    // Volumes last so a job can deliberately shadow a default mount point —
    // the task-dir mount included.
    mounts.extend(volume_mounts(ctx.volumes));

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

    // Docker's model: one set as bounding, effective, and permitted; nothing
    // inheritable or ambient, so a process running as a non-root user keeps
    // no capabilities across `execve`. `noNewPrivileges` stays on: it stops
    // setuid binaries and file capabilities from *raising* privileges at
    // exec, and does not stop a root entrypoint from dropping to another
    // user with setuid(2)/setgid(2) — which is all `gosu` and `su-exec` do.
    let capabilities = &ctx.policy.capabilities;
    let rlimits: Vec<Value> = ctx
        .policy
        .rlimits
        .iter()
        .map(|limit| json!({ "type": limit.kind, "soft": limit.soft, "hard": limit.hard }))
        .collect();
    let mut spec = json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": { "uid": user.uid, "gid": user.gid },
            "args": command,
            "env": env,
            "cwd": cwd,
            "capabilities": {
                "bounding": capabilities,
                "effective": capabilities,
                "permitted": capabilities
            },
            "rlimits": rlimits,
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
    spec
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
            policy: &ProcessPolicy::default(),
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
                policy: &ProcessPolicy::default(),
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
                policy: &ProcessPolicy::default(),
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
                policy: &ProcessPolicy::default(),
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
                policy: &ProcessPolicy::default(),
            },
        );
        let env = spec["process"]["env"].as_array().unwrap();
        assert!(env.iter().any(|e| e == "HOME=/from-image"));
        assert!(!env.iter().any(|e| e == "HOME=/var/lib/postgresql"));
    }

    fn caps(spec: &Value, set: &str) -> Vec<String> {
        spec["process"]["capabilities"][set]
            .as_array()
            .expect("capability set")
            .iter()
            .map(|cap| cap.as_str().expect("name").to_string())
            .collect()
    }

    /// Stock images chown their directories and drop to a service user at
    /// start (nginx, the Postgres entrypoint's `su-exec`/`gosu`). The default
    /// spec must grant what that takes, as Docker does — and withhold
    /// `CAP_NET_RAW`, because a Legion container shares the node's network.
    #[test]
    fn the_default_spec_grants_dockers_capabilities_without_net_raw() {
        let spec = spec_with(None);
        let bounding = caps(&spec, "bounding");
        for needed in [
            "CAP_CHOWN",
            "CAP_DAC_OVERRIDE",
            "CAP_FOWNER",
            "CAP_FSETID",
            "CAP_SETUID",
            "CAP_SETGID",
            "CAP_KILL",
            "CAP_NET_BIND_SERVICE",
            "CAP_AUDIT_WRITE",
        ] {
            assert!(
                bounding.iter().any(|cap| cap == needed),
                "{needed} in {bounding:?}"
            );
        }
        assert!(!bounding.iter().any(|cap| cap == "CAP_NET_RAW"));
        assert!(!bounding.iter().any(|cap| cap == "CAP_SYS_ADMIN"));
        assert_eq!(bounding.len(), DEFAULT_CAPABILITIES.len());
        // One set, applied to bounding, effective, and permitted alike;
        // nothing inheritable or ambient.
        assert_eq!(caps(&spec, "effective"), bounding);
        assert_eq!(caps(&spec, "permitted"), bounding);
        assert!(spec["process"]["capabilities"]["inheritable"].is_null());
        assert!(spec["process"]["capabilities"]["ambient"].is_null());
        assert_eq!(spec["process"]["noNewPrivileges"], true);
    }

    /// The open-file limit is no longer the OCI runtime's hard 1024.
    #[test]
    fn the_default_spec_raises_the_open_file_limit() {
        let spec = spec_with(None);
        let rlimits = spec["process"]["rlimits"].as_array().expect("rlimits");
        let nofile = rlimits
            .iter()
            .find(|limit| limit["type"] == "RLIMIT_NOFILE")
            .expect("RLIMIT_NOFILE set");
        let soft = nofile["soft"].as_u64().unwrap();
        assert_eq!(soft, nofile["hard"].as_u64().unwrap());
        assert!(soft <= DEFAULT_NOFILE);
        // Only a host whose own limits are lower than that clamps it.
        if running_as_root() {
            let nr_open = std::fs::read_to_string("/proc/sys/fs/nr_open")
                .ok()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
                .unwrap_or(DEFAULT_NOFILE);
            assert_eq!(soft, DEFAULT_NOFILE.min(nr_open));
        }
    }

    #[test]
    fn capability_adjustments_follow_docker_semantics() -> R<()> {
        let names = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        // Nothing adjusted: the default set.
        assert_eq!(resolve_capabilities(&[], &[])?, default_capabilities());

        // Names are case-insensitive and the CAP_ prefix is optional.
        let caps =
            resolve_capabilities(&names(&["net_raw", "CAP_SYS_PTRACE"]), &names(&["mknod"]))?;
        assert!(caps.contains(&"CAP_NET_RAW".to_string()));
        assert!(caps.contains(&"CAP_SYS_PTRACE".to_string()));
        assert!(!caps.contains(&"CAP_MKNOD".to_string()));
        assert!(caps.contains(&"CAP_CHOWN".to_string()), "defaults kept");

        // drop = ALL starts from nothing.
        assert_eq!(
            resolve_capabilities(&names(&["NET_BIND_SERVICE"]), &names(&["all"]))?,
            vec!["CAP_NET_BIND_SERVICE".to_string()]
        );
        assert!(resolve_capabilities(&[], &names(&["ALL"]))?.is_empty());

        // add = ALL is every capability, less what drop names.
        let everything = resolve_capabilities(&names(&["ALL"]), &names(&["SYS_ADMIN"]))?;
        assert_eq!(everything.len(), ALL_CAPABILITIES.len() - 1);
        assert!(!everything.contains(&"CAP_SYS_ADMIN".to_string()));

        // A name in both lists ends up granted, as in Docker.
        assert!(
            resolve_capabilities(&names(&["NET_RAW"]), &names(&["NET_RAW"]))?
                .contains(&"CAP_NET_RAW".to_string())
        );

        // Typos fail loudly, naming the list.
        let err = resolve_capabilities(&names(&["NET_ADMINN"]), &[]).unwrap_err();
        assert!(
            err.to_string().contains("security.capabilities.add"),
            "{err}"
        );
        assert!(err.to_string().contains("NET_ADMINN"), "{err}");
        let err = resolve_capabilities(&names(&["ALL"]), &names(&["ALL"])).unwrap_err();
        assert!(err.to_string().contains("both add and drop"), "{err}");
        Ok(())
    }

    #[test]
    fn ulimits_replace_the_matching_default_and_validate() -> R<()> {
        let parse = |value: Value| -> R<Vec<Rlimit>> {
            let map: BTreeMap<String, Value> = serde_json::from_value(value)?;
            resolve_rlimits(&map)
        };

        // Nothing declared: the defaults.
        assert_eq!(parse(json!({}))?, default_rlimits());

        // A bare number sets soft and hard; the declared nofile replaces the
        // default one instead of adding a second entry.
        let limits = parse(json!({ "nofile": 65536, "nproc": { "soft": 1024, "hard": 4096 } }))?;
        let nofile: Vec<&Rlimit> = limits
            .iter()
            .filter(|limit| limit.kind == "RLIMIT_NOFILE")
            .collect();
        assert_eq!(nofile.len(), 1);
        assert_eq!((nofile[0].soft, nofile[0].hard), (65536, 65536));
        let nproc = limits
            .iter()
            .find(|limit| limit.kind == "RLIMIT_NPROC")
            .expect("nproc");
        assert_eq!((nproc.soft, nproc.hard), (1024, 4096));

        // "unlimited" is RLIM_INFINITY.
        let limits = parse(json!({ "memlock": "unlimited" }))?;
        let memlock = limits
            .iter()
            .find(|limit| limit.kind == "RLIMIT_MEMLOCK")
            .expect("memlock");
        assert_eq!(memlock.hard, u64::MAX);

        for (bad, needle) in [
            (json!({ "nofiles": 10 }), "unknown ulimit"),
            (
                json!({ "nofile": { "soft": 10, "hard": 5 } }),
                "must not exceed",
            ),
            (json!({ "nofile": { "soft": 10 } }), "both soft and hard"),
            (json!({ "nofile": -1 }), "non-negative"),
            (json!({ "nofile": "lots" }), "unlimited"),
            (
                json!({ "nofile": { "soft": 1, "hard": 2, "max": 3 } }),
                "unknown field",
            ),
        ] {
            let err = parse(bad.clone()).expect_err("rejected");
            assert!(err.to_string().contains(needle), "{bad}: {err}");
        }
        Ok(())
    }

    /// A jobspec's capability set and rlimits reach the generated spec.
    #[test]
    fn a_task_policy_lands_in_the_spec() -> R<()> {
        let policy = ProcessPolicy {
            capabilities: resolve_capabilities(&["NET_RAW".to_string()], &["ALL".to_string()])?,
            rlimits: vec![Rlimit {
                kind: "RLIMIT_NOFILE".into(),
                soft: 4096,
                hard: 8192,
            }],
        };
        let spec = build_spec(
            &["true".to_string()],
            &HashMap::new(),
            &bare_image(),
            &ResolvedUser::default(),
            &BundleContext {
                hostname: "h",
                volumes: &[],
                task_dir: None,
                assignment: None,
                resolver: None,
                resolv_conf: None,
                netns: None,
                policy: &policy,
            },
        );
        assert_eq!(caps(&spec, "bounding"), vec!["CAP_NET_RAW".to_string()]);
        assert_eq!(
            spec["process"]["rlimits"],
            json!([{ "type": "RLIMIT_NOFILE", "soft": 4096, "hard": 8192 }])
        );
        Ok(())
    }

    /// One PAX extended-header record: `"<len> <key>=<value>\n"`, where the
    /// length counts the whole record including its own digits.
    fn pax_record(key: &str, value: &[u8]) -> Vec<u8> {
        let body_len = 1 + key.len() + 1 + value.len() + 1;
        let mut len = body_len + 1;
        while len.to_string().len() + body_len != len {
            len = len.to_string().len() + body_len;
        }
        let mut record = format!("{len} {key}=").into_bytes();
        record.extend_from_slice(value);
        record.push(b'\n');
        record
    }

    /// A synthetic image layer the way `RUN chown`/`chmod u+s`/`setcap`
    /// leave one: a directory owned by a service user, a setuid binary, and a
    /// file carrying an extended attribute.
    fn ownership_layer(dir: &Path) -> R<PathBuf> {
        let blob = dir.join("layer.tar");
        let mut builder = tar::Builder::new(std::fs::File::create(&blob)?);

        let mut header = tar::Header::new_ustar();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_path("var/cache/nginx/")?;
        header.set_mode(0o755);
        header.set_uid(101);
        header.set_gid(102);
        header.set_size(0);
        header.set_cksum();
        builder.append(&header, std::io::empty())?;

        let mut header = tar::Header::new_ustar();
        header.set_path("usr/bin/helper")?;
        header.set_mode(0o4755);
        header.set_uid(0);
        header.set_gid(0);
        header.set_size(2);
        header.set_cksum();
        builder.append(&header, &b"#!"[..])?;

        // A PAX header carrying an xattr for the entry that follows.
        let records = [
            pax_record("SCHILY.xattr.user.legion-test", b"kept"),
            pax_record("SCHILY.xattr.trusted.overlay.opaque", b"y"),
        ]
        .concat();
        let mut pax = tar::Header::new_ustar();
        pax.set_entry_type(tar::EntryType::XHeader);
        pax.set_path("PaxHeaders/data")?;
        pax.set_size(records.len() as u64);
        pax.set_mode(0o644);
        pax.set_cksum();
        builder.append(&pax, records.as_slice())?;
        let mut header = tar::Header::new_ustar();
        header.set_path("var/cache/nginx/data")?;
        header.set_mode(0o640);
        header.set_uid(101);
        header.set_gid(102);
        header.set_size(2);
        header.set_cksum();
        builder.append(&header, &b"ok"[..])?;

        builder.finish()?;
        Ok(blob)
    }

    /// The root cause of "a `chown nginx` in the image shows up as 0:0":
    /// extraction dropped every owner. As root it now keeps owners, the
    /// setuid bit (applied after the chown that would clear it), and the
    /// layer's extended attributes.
    #[test]
    fn extraction_as_root_keeps_owners_setuid_bits_and_xattrs() -> R<()> {
        use std::os::unix::fs::MetadataExt;
        if !running_as_root() {
            // Only root can chown to another user; the non-root path is
            // covered below.
            return Ok(());
        }
        let dir = tempfile::tempdir()?;
        let blob = ownership_layer(dir.path())?;
        let rootfs = dir.path().join("rootfs");
        std::fs::create_dir_all(&rootfs)?;
        extract_layer_as(&blob, &rootfs, true)?;

        let cache = std::fs::metadata(rootfs.join("var/cache/nginx"))?;
        assert_eq!((cache.uid(), cache.gid()), (101, 102));
        let data = std::fs::metadata(rootfs.join("var/cache/nginx/data"))?;
        assert_eq!((data.uid(), data.gid()), (101, 102));
        assert_eq!(data.mode() & 0o7777, 0o640);
        let helper = std::fs::metadata(rootfs.join("usr/bin/helper"))?;
        assert_eq!(helper.mode() & 0o7777, 0o4755, "setuid bit kept");

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::ffi::OsStrExt;
            let read = |name: &str| -> Option<Vec<u8>> {
                let path = std::ffi::CString::new(
                    rootfs.join("var/cache/nginx/data").as_os_str().as_bytes(),
                )
                .unwrap();
                let name = std::ffi::CString::new(name).unwrap();
                let mut buf = [0u8; 64];
                // SAFETY: valid NUL-terminated strings and a sized buffer.
                let len = unsafe {
                    libc::lgetxattr(
                        path.as_ptr(),
                        name.as_ptr(),
                        buf.as_mut_ptr().cast(),
                        buf.len(),
                    )
                };
                (len >= 0).then(|| buf[..len as usize].to_vec())
            };
            // Filesystems without user xattrs (some tmpfs) skip it; where the
            // host supports it, the value arrives intact.
            if let Some(value) = read("user.legion-test") {
                assert_eq!(value, b"kept");
            }
            assert_eq!(
                read("trusted.overlay.opaque"),
                None,
                "overlay bookkeeping skipped"
            );
        }
        Ok(())
    }

    /// A worker that is not root cannot chown, so extraction must not try:
    /// everything belongs to the worker's own user and the start succeeds.
    #[test]
    fn extraction_without_root_keeps_modes_and_skips_owners() -> R<()> {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir()?;
        let blob = ownership_layer(dir.path())?;
        let rootfs = dir.path().join("rootfs");
        std::fs::create_dir_all(&rootfs)?;
        extract_layer_as(&blob, &rootfs, false)?;
        // SAFETY: geteuid cannot fail.
        let me = unsafe { libc::geteuid() };
        let data = std::fs::metadata(rootfs.join("var/cache/nginx/data"))?;
        assert_eq!(data.uid(), me);
        assert_eq!(data.mode() & 0o777, 0o640);
        Ok(())
    }

    /// Non-root containers reopen `/dev/stdout` (`/proc/self/fd/1`), which
    /// checks the log file's own permissions. The files are handed to the
    /// container's user; the directory stays owner-only.
    #[test]
    fn task_logs_are_handed_to_the_container_user() -> R<()> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if !running_as_root() {
            return Ok(());
        }
        let dir = tempfile::tempdir()?;
        let log_dir = dir.path().join("logs");
        std::fs::create_dir_all(&log_dir)?;
        std::fs::set_permissions(&log_dir, std::fs::Permissions::from_mode(0o700))?;
        for file in ["stdout.log", "stderr.log"] {
            std::fs::write(log_dir.join(file), b"")?;
        }

        hand_task_logs_to(Some(&log_dir), 101, 101);
        for file in ["stdout.log", "stderr.log"] {
            let meta = std::fs::metadata(log_dir.join(file))?;
            assert_eq!((meta.uid(), meta.gid()), (101, 101), "{file}");
        }
        let dir_meta = std::fs::metadata(&log_dir)?;
        assert_eq!(dir_meta.uid(), 0, "the directory stays the worker's");
        assert_eq!(dir_meta.mode() & 0o777, 0o700);

        // A later start as root hands them back.
        hand_task_logs_to(Some(&log_dir), 0, 0);
        assert_eq!(std::fs::metadata(log_dir.join("stdout.log"))?.uid(), 0);
        // No log dir: nothing to do, and no panic.
        hand_task_logs_to(None, 101, 101);
        Ok(())
    }
}
