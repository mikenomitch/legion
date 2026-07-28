use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, reap_task_process, spawn_pipe, spawn_pty,
    spawn_task_process, ExecOptions, ExecSession, ExecSupport, TaskInstance, TaskPlugin,
};
#[cfg(target_os = "linux")]
use landlock::{AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI};
use schemars::JsonSchema;
use serde::Deserialize;
#[cfg(unix)]
use std::ffi::CString;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(unix)]
use std::path::Path;
use std::process::Command;

fn default_exec_env() -> HashMap<String, String> {
    HashMap::new()
}

fn default_landlock_paths() -> Vec<String> {
    Vec::new()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct ExecTaskConfig {
    /// Command and arguments to execute. The first entry is the program binary.
    command: Vec<String>,

    /// Environment variables injected into the spawned process.
    #[serde(default = "default_exec_env")]
    #[schemars(default = "default_exec_env")]
    env: HashMap<String, String>,

    /// Optional working directory for the process.
    #[serde(default)]
    cwd: Option<String>,

    /// Optional username or UID to run as (Unix only).
    #[serde(default)]
    user: Option<String>,

    /// Legacy alias for Linux `oom_score_adj`.
    #[serde(default)]
    #[schemars(range(min = -1000, max = 1000))]
    oom_adj: Option<i32>,

    /// Linux OOM score adjustment in the range `[-1000, 1000]`.
    #[serde(default)]
    #[schemars(range(min = -1000, max = 1000))]
    oom_score_adj: Option<i32>,

    /// Optional Linux cgroup v2 subpath (for example `workloads/demo`).
    #[serde(default)]
    cgroup: Option<String>,

    /// Optional Landlock path restrictions (Linux only).
    #[serde(default)]
    landlock: Option<ExecLandlockConfig>,

    /// Files to materialize on the host before task start.
    #[serde(default)]
    files: Vec<ExecInjectedFile>,

    /// Additional plugin-specific fields reserved for forward compatibility.
    #[serde(default, flatten)]
    #[schemars(skip)]
    _extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct ExecInjectedFile {
    path: String,
    #[serde(default)]
    mode: Option<String>,
    content: String,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ExecLandlockConfig {
    /// Read-only paths accessible to the task process.
    #[serde(default = "default_landlock_paths")]
    #[schemars(default = "default_landlock_paths")]
    read_only_paths: Vec<String>,

    /// Read-write paths accessible to the task process.
    #[serde(default = "default_landlock_paths")]
    #[schemars(default = "default_landlock_paths")]
    read_write_paths: Vec<String>,
}

impl ExecLandlockConfig {
    fn to_runtime(&self) -> R<LandlockOptions> {
        if self.read_only_paths.is_empty() && self.read_write_paths.is_empty() {
            bail!("exec task landlock must specify at least one path");
        }

        Ok(LandlockOptions {
            read_only_paths: self.read_only_paths.clone(),
            read_write_paths: self.read_write_paths.clone(),
        })
    }
}

impl ExecTaskConfig {
    fn from_value(config: &Value) -> R<Self> {
        let parsed: Self =
            serde_json::from_value(config.clone()).context("exec task config must be an object")?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> R<()> {
        if self.command.is_empty() {
            bail!("exec task command must include at least one entry");
        }

        if self.oom_adj.is_some() && self.oom_score_adj.is_some() {
            bail!("exec task config may specify only one of oom_adj or oom_score_adj");
        }

        if let Some(score) = self.oom_adj.or(self.oom_score_adj) {
            if !(-1000..=1000).contains(&score) {
                bail!("exec task oom_adj must be between -1000 and 1000");
            }
        }

        if let Some(value) = self.cgroup.as_deref() {
            let cgroup = value.trim_matches('/');
            if cgroup.is_empty() {
                bail!("exec task cgroup must not be empty");
            }
        }

        if let Some(landlock) = self.landlock.as_ref() {
            let _ = landlock.to_runtime()?;
        }

        Ok(())
    }

    #[cfg(unix)]
    fn user_spec(&self) -> R<Option<UserSpec>> {
        let Some(user) = self.user.as_deref() else {
            return Ok(None);
        };
        lookup_user(user).map(Some)
    }

    fn linux_options(&self) -> R<ExecLinuxOptions> {
        let landlock = self
            .landlock
            .as_ref()
            .map(ExecLandlockConfig::to_runtime)
            .transpose()?;

        let oom_score_adj = self.oom_adj.or(self.oom_score_adj);
        let cgroup = self
            .cgroup
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.trim_matches('/').to_string());

        Ok(ExecLinuxOptions {
            landlock,
            oom_score_adj,
            cgroup,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct ExecLinuxOptions {
    landlock: Option<LandlockOptions>,
    oom_score_adj: Option<i32>,
    cgroup: Option<String>,
}

/// Parsed on every platform so a landlock jobspec validates the same way for a
/// macOS developer as it does on a worker, but the paths are only ever consumed
/// by `apply_landlock`, which is Linux-only. The fields are therefore dead
/// everywhere else; dead-code analysis still applies on Linux, where a truly
/// unused field is a real bug.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug)]
struct LandlockOptions {
    read_only_paths: Vec<String>,
    read_write_paths: Vec<String>,
}

#[cfg(unix)]
#[derive(Clone, Debug)]
struct UserSpec {
    uid: u32,
    gid: u32,
}

#[derive(Default)]
pub struct ExecTaskPlugin;

impl ExecTaskPlugin {
    fn parse_config(&self, config: &Value) -> R<ExecTaskConfig> {
        ExecTaskConfig::from_value(config)
    }

    #[cfg(test)]
    fn config_linux_options(&self, config: &Value) -> R<ExecLinuxOptions> {
        self.parse_config(config)?.linux_options()
    }
}

impl TaskPlugin for ExecTaskPlugin {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn schema(&self) -> Value {
        derived_schema::<ExecTaskConfig>()
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("exec".into(), Value::String("pass".into()));
        map
    }

    fn supports_kv_env_injection(&self) -> bool {
        true
    }

    fn supports_kv_file_injection(&self) -> bool {
        true
    }

    fn supports_identity_env(&self) -> bool {
        true
    }

    fn supports_identity_file(&self) -> bool {
        true
    }

    fn supports_signal(&self) -> bool {
        true
    }

    fn supports_artifacts(&self) -> bool {
        true
    }

    fn exec_support(&self) -> ExecSupport {
        // A sibling host process, even with landlock: still host-level, so it
        // carries the `host` ACL flag (Nomad's alloc-node-exec lesson).
        ExecSupport {
            exec: true,
            pty: true,
            host_level: true,
        }
    }

    fn exec(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        mut opts: ExecOptions,
    ) -> R<Box<dyn ExecSession>> {
        // Run the exec command in the same *context* the task got: its env
        // (worker-injected first, then task env), and its working directory.
        // It is a sibling host process — the running task has no namespace to
        // join — which is the honest semantic for the `exec` runtime.
        let config = self.parse_config(&task.config)?;

        let mut env = instance.extra_env.clone();
        for (k, v) in &config.env {
            env.insert(k.clone(), v.clone());
        }
        // Preserve any env the caller set (e.g. TERM) without clobbering task env.
        for (k, v) in std::mem::take(&mut opts.env) {
            env.entry(k).or_insert(v);
        }
        opts.env = env;

        if opts.cwd.is_none() {
            opts.cwd = config
                .cwd
                .as_deref()
                .map(std::path::PathBuf::from)
                .or_else(|| instance.task_dir.clone());
        }

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            tty = opts.tty,
            "Opening exec session for exec task"
        );

        if opts.tty {
            spawn_pty(&opts)
        } else {
            spawn_pipe(&opts)
        }
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing exec task"
        );
        Ok(())
    }

    fn start(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let config = self.parse_config(&task.config)?;
        let (program, args) = config
            .command
            .split_first()
            .context("exec task command must include at least one entry")?;

        let mut linux_options = config.linux_options()?;
        // Artifacts stage into the task directory; a landlocked task must be
        // able to read them, so the task dir rides along automatically.
        if let (Some(landlock), Some(task_dir)) =
            (linux_options.landlock.as_mut(), instance.task_dir.as_ref())
        {
            landlock
                .read_write_paths
                .push(task_dir.display().to_string());
        }
        for file in &config.files {
            let path = std::path::PathBuf::from(&file.path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&path, file.content.as_bytes())?;
            #[cfg(unix)]
            if let Some(mode) = file.mode.as_deref() {
                if let Ok(mode_bits) = u32::from_str_radix(mode.trim_start_matches('0'), 8) {
                    fs::set_permissions(&path, fs::Permissions::from_mode(mode_bits))?;
                }
            }
        }

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            command = program,
            "Starting exec task"
        );

        let mut cmd = Command::new(program);
        cmd.args(args);
        // cwd defaults to the private task directory when the spec sets none.
        match config.cwd.as_deref() {
            Some(cwd) => {
                cmd.current_dir(cwd);
            }
            None => {
                if let Some(task_dir) = instance.task_dir.as_ref() {
                    cmd.current_dir(task_dir);
                }
            }
        }
        #[cfg(unix)]
        if let Some(user) = config.user_spec()? {
            cmd.uid(user.uid);
            cmd.gid(user.gid);
            // The worker creates alloc/task dirs owner-only (its own user) as
            // the isolation boundary between allocations on a node. Hand this
            // allocation's tree to the task user, or the dropped-privilege
            // process could not enter its own cwd or read staged artifacts —
            // while every other allocation stays unreachable to it.
            if let Some(alloc_dir) = instance.alloc_dir.as_ref() {
                chown_tree(alloc_dir, user.uid, user.gid).with_context(|| {
                    format!(
                        "failed to hand allocation dir {} to exec task user",
                        alloc_dir.display()
                    )
                })?;
            }
        }

        // Legion-provided env first (allocation identity/ports, then
        // worker-injected LEGION_TASK_DIR/LEGION_ARTIFACT_*), so explicit
        // task env can override on collision.
        cmd.envs(crate::task_plugins::allocation_env(allocation, task));
        if !instance.extra_env.is_empty() {
            cmd.envs(&instance.extra_env);
        }
        if !config.env.is_empty() {
            cmd.envs(&config.env);
        }

        configure_linux_process_options(&mut cmd, &linux_options, &allocation.id, &task.name)?;
        configure_task_log_capture(&mut cmd, instance)?;

        let child = spawn_task_process(&mut cmd).with_context(|| {
            format!(
                "Failed to spawn exec task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        instance.pid = Some(child.id());
        instance.child = Some(child);
        Ok(())
    }

    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        reap_task_process(allocation.id, &task.name, instance, "exec");
        Ok(())
    }

    fn destroy(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Destroying exec task"
        );
        self.stop(allocation, task, instance)
    }
}

#[cfg(target_os = "linux")]
fn configure_linux_process_options(
    cmd: &mut Command,
    options: &ExecLinuxOptions,
    allocation_id: &i64,
    task_name: &str,
) -> R<()> {
    let has_landlock = options.landlock.is_some();
    let oom_score_adj = options.oom_score_adj;
    let cgroup = options.cgroup.clone();
    let options = options.clone();
    let task_name = task_name.to_string();
    let allocation_id = *allocation_id;
    unsafe {
        cmd.pre_exec(move || {
            if let Some(score) = options.oom_score_adj {
                std::fs::write("/proc/self/oom_score_adj", score.to_string())
                    .map_err(std::io::Error::other)?;
            }

            if let Some(ref cgroup) = options.cgroup {
                let unified = Path::new("/sys/fs/cgroup/cgroup.controllers");
                if !unified.exists() {
                    return Err(std::io::Error::other("exec task cgroup requires cgroup v2"));
                }
                let cgroup_procs = Path::new("/sys/fs/cgroup")
                    .join(cgroup)
                    .join("cgroup.procs");
                std::fs::write(cgroup_procs, std::process::id().to_string())
                    .map_err(std::io::Error::other)?;
            }

            if let Some(ref landlock) = options.landlock {
                apply_landlock(landlock).map_err(std::io::Error::other)?;
            }
            Ok(())
        });
    }

    debug!(
        allocation_id,
        task_name = %task_name,
        has_landlock,
        oom_score_adj,
        cgroup = cgroup.as_deref().unwrap_or(""),
        "Configured Linux process options for exec task"
    );
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn configure_linux_process_options(
    _cmd: &mut Command,
    options: &ExecLinuxOptions,
    allocation_id: &i64,
    task_name: &str,
) -> R<()> {
    if options.landlock.is_some() {
        warn!(allocation_id, task_name = %task_name, "landlock is only supported on Linux and will be ignored");
    }
    if options.oom_score_adj.is_some() {
        warn!(allocation_id, task_name = %task_name, "oom_score_adj is only supported on Linux and will be ignored");
    }
    if options.cgroup.is_some() {
        warn!(allocation_id, task_name = %task_name, "cgroup assignment is only supported on Linux and will be ignored");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_landlock(options: &LandlockOptions) -> R<()> {
    let abi = ABI::V4;
    let read_access = AccessFs::from_read(abi);
    let write_access = AccessFs::from_write(abi);
    let all_access = read_access | write_access;

    let mut ruleset = Ruleset::default()
        .handle_access(all_access)
        .context("failed to declare landlock access")?
        .create()
        .context("failed to create landlock ruleset")?;

    for path in &options.read_only_paths {
        let fd =
            PathFd::new(path).with_context(|| format!("failed to open landlock path {path}"))?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, read_access))
            .with_context(|| format!("failed to add landlock read-only path {path}"))?;
    }

    for path in &options.read_write_paths {
        let fd =
            PathFd::new(path).with_context(|| format!("failed to open landlock path {path}"))?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, all_access))
            .with_context(|| format!("failed to add landlock read-write path {path}"))?;
    }

    ruleset
        .restrict_self()
        .context("failed to restrict process with landlock")?;

    Ok(())
}

/// Chown a directory tree without following symlinks: a symlink staged into
/// the task dir (e.g. inside a tree artifact) must not hand ownership of a
/// path outside the allocation tree to the task user.
#[cfg(unix)]
fn chown_tree(path: &Path, uid: u32, gid: u32) -> R<()> {
    std::os::unix::fs::lchown(path, Some(uid), Some(gid))
        .with_context(|| format!("failed to chown {}", path.display()))?;
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path)
            .with_context(|| format!("failed to read dir {}", path.display()))?
        {
            chown_tree(&entry?.path(), uid, gid)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn lookup_user(value: &str) -> R<UserSpec> {
    let cstr = CString::new(value).context("exec task user must not contain null bytes")?;
    // SAFETY: libc returns pointers to static storage for getpwnam/getpwuid and we only read fields.
    let passwd = unsafe {
        if let Ok(uid) = value.parse::<u32>() {
            libc::getpwuid(uid)
        } else {
            libc::getpwnam(cstr.as_ptr())
        }
    };
    if passwd.is_null() {
        bail!("exec task user {value} not found");
    }
    // SAFETY: passwd has been checked as non-null.
    let passwd = unsafe { *passwd };
    Ok(UserSpec {
        uid: passwd.pw_uid,
        gid: passwd.pw_gid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_options_parse_from_exec_config() -> R<()> {
        let plugin = ExecTaskPlugin;
        let config = json!({
            "command": ["echo", "hello"],
            "oom_adj": 250,
            "cgroup": "workloads/demo",
            "landlock": {
                "read_only_paths": ["/usr"],
                "read_write_paths": ["/tmp"],
            }
        });

        let options = plugin.config_linux_options(&config)?;
        assert_eq!(options.oom_score_adj, Some(250));
        assert_eq!(options.cgroup.as_deref(), Some("workloads/demo"));
        assert!(options.landlock.is_some());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn chown_tree_walks_dirs_without_following_symlinks() -> R<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let root = temp_dir.path().join("alloc");
        std::fs::create_dir_all(root.join("task/nested"))?;
        std::fs::write(root.join("task/nested/artifact"), b"data")?;
        // A dangling symlink errors when followed (chown) but not when
        // operated on directly (lchown) — success proves no-follow.
        std::os::unix::fs::symlink("/nonexistent-target", root.join("task/dangling"))?;

        // Chown to our own uid/gid: a no-op ownership-wise, but exercises the
        // full walk without requiring root.
        // SAFETY: trivial libc getters with no preconditions.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        chown_tree(&root, uid, gid)?;
        Ok(())
    }

    #[test]
    fn linux_options_reject_invalid_oom_adj() {
        let plugin = ExecTaskPlugin;
        let config = json!({
            "command": ["echo"],
            "oom_adj": 1001,
        });

        let err = plugin.config_linux_options(&config).unwrap_err();
        assert!(err.to_string().contains("between -1000 and 1000"));
    }

    #[test]
    fn linux_options_rejects_legacy_and_new_oom_fields_together() {
        let plugin = ExecTaskPlugin;
        let config = json!({
            "command": ["echo"],
            "oom_adj": 100,
            "oom_score_adj": 200,
        });

        let err = plugin.config_linux_options(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("only one of oom_adj or oom_score_adj"));
    }
}
