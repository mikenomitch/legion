use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::exec_isolation::{
    enforcement_enabled, remove_managed_cgroup, ExecHardeningConfig,
};
// Linux-only: these are called from the pre-exec hardening block, which is
// itself `cfg(target_os = "linux")` because it goes through `libc::prctl`
// and `PR_CAPBSET_DROP`. Importing them unconditionally made every macOS
// build warn, and `cargo clippy -- -D warnings` fails on it — green on CI,
// which is Linux, and broken for anyone developing on a Mac.
#[cfg(target_os = "linux")]
use crate::task_plugins::exec_isolation::{cap_last_cap, capabilities_to_drop, parse_umask};
use crate::task_plugins::{
    configure_task_log_capture, derived_schema, spawn_pipe, spawn_pty, spawn_task_process,
    ExecOptions, ExecSession, ExecSupport, TaskInstance, TaskPlugin,
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
    ///
    /// This *joins* a cgroup somebody else created and limited. It also opts
    /// this task out of worker-owned enforcement: an explicit cgroup wins over
    /// `resourceEnforcement` (see docs/task-supervision.md).
    #[serde(default)]
    cgroup: Option<String>,

    /// Optional process hardening (linux): no-new-privs, capability drops, a
    /// private /tmp, umask, nice. See docs/task-supervision.md.
    #[serde(default)]
    hardening: Option<ExecHardeningConfig>,

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

        if let Some(hardening) = self.hardening.as_ref() {
            hardening.validate()?;
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
            managed_cgroup: None,
            hardening: self.hardening.clone(),
            user: None,
            netns: None,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct ExecLinuxOptions {
    landlock: Option<LandlockOptions>,
    oom_score_adj: Option<i32>,
    /// Explicit `config.cgroup`: join a cgroup the operator already made.
    cgroup: Option<String>,
    /// Worker-owned cgroup created for this allocation, relative to
    /// `/sys/fs/cgroup`. Only set when the worker enforces resources and the
    /// task did not name a cgroup of its own.
    managed_cgroup: Option<std::path::PathBuf>,
    hardening: Option<ExecHardeningConfig>,
    /// A gated network namespace (`network.mode = "gated"`) to enter before
    /// exec: `/var/run/netns/legion-<allocation>`, created by the worker
    /// ahead of start. Entered before the user switch (it needs the
    /// worker's privileges) and before landlock.
    netns: Option<std::path::PathBuf>,
    /// User to switch to inside the child, when the plugin owns the switch
    /// (privileged hardening steps must run before dropping privileges, and
    /// `Command::uid` drops them before pre-exec closures run).
    #[cfg_attr(not(unix), allow(dead_code))]
    user: Option<UserSpec>,
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
    /// A gated allocation's namespace is a real boundary: nothing leaves it
    /// except through the datapath, which holds its policy until ratified.
    fn network_gate_for(&self, allocation: &Allocation) -> crate::task_plugins::NetworkGate {
        if crate::task_plugins::allocation_is_gated(allocation) {
            crate::task_plugins::NetworkGate::Enforced
        } else {
            crate::task_plugins::NetworkGate::Advisory
        }
    }

    fn set_network_gate(
        &self,
        allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
        open: bool,
    ) -> R<()> {
        crate::task_plugins::set_gated_namespace_gate(allocation, open)
    }

    fn name(&self) -> &'static str {
        "exec"
    }

    fn schema(&self) -> Value {
        derived_schema::<ExecTaskConfig>()
    }

    /// Runs the typed parse, not just the derived JSON schema — the same
    /// argument the sandbox runtime makes: the config carries a `#[serde(flatten)]`
    /// catch-all for forward compatibility, so a schema-only check accepts a
    /// bad umask, an unknown capability, or both `oom_adj` and `oom_score_adj`,
    /// and the job only fails when a task tries to start on some node. Parsing
    /// here moves every check in [`ExecTaskConfig::validate`] to submit time,
    /// where `legion job validate` and an agent can still act on it.
    fn validate_config(&self, config: &Value) -> R<()> {
        crate::task_plugins::validate_schema(&self.schema(), config)?;
        ExecTaskConfig::from_value(config)?;
        Ok(())
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

    /// An exec task *is* a host process, so worker core can create its cgroup
    /// and the plugin simply spawns into it. That puts exec tasks in the same
    /// hierarchy as container tasks, which is what makes a reserved core
    /// exclusive against *every* governed workload on the node rather than
    /// only against other containers.
    ///
    /// An explicit `config.cgroup` still wins and opts the task out — it
    /// keeps meaning what it has always meant: join the cgroup the job named.
    fn resource_enforcement(&self) -> crate::task_plugins::ResourceEnforcement {
        crate::task_plugins::ResourceEnforcement::WorkerCgroup
    }

    /// `exec` is the canonical "one host process we spawned" runtime.
    fn supervises_process(&self) -> bool {
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
        // A gated allocation runs inside the namespace the worker created
        // for it (docs/network-handlers.md). Its absence is a hard stop, not
        // a silent fall back to host networking.
        if crate::task_plugins::allocation_is_gated(allocation) {
            let path = crate::worker::netd::netns::netns_path(allocation.id);
            if !path.exists() {
                bail!(
                    "gated network namespace {} is not ready; the worker creates it before start \
                     and retries on its next heartbeat",
                    path.display()
                );
            }
            linux_options.netns = Some(path);
        }
        // Worker-owned resource enforcement: worker core already created this
        // allocation's cgroup and wrote its limits (see
        // `src/worker/resources.rs`); the plugin only has to spawn into it.
        // An explicit `config.cgroup` opts out — the operator named a cgroup
        // they manage, and admission already rejected combining that with
        // `cores`, which Legion could not fence inside it.
        //
        // The join rides the existing pre_exec sequence rather than
        // `join_assignment_cgroup`, because ordering matters here: it is
        // privileged work and must happen before the switch to the task user.
        // A plugin without that constraint gets the same effect from the
        // one-call helper.
        if linux_options.cgroup.is_none() {
            linux_options.managed_cgroup = instance
                .resource_assignment
                .as_ref()
                .map(|assignment| assignment.cgroup_path.clone());
        }
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
            // Privileged hardening (mount namespaces, capability drops) must
            // run *before* the process drops to the task user, and std applies
            // `Command::uid`/`gid` before pre-exec closures. So when hardening
            // needs privilege, the plugin performs the switch itself at the
            // right point in its own pre-exec sequence.
            // The managed cgroup is created by the worker and is not writable
            // by the task user, so joining it is privileged work too.
            let plugin_owns_user_switch = linux_options.managed_cgroup.is_some()
                || linux_options
                    .hardening
                    .as_ref()
                    .is_some_and(ExecHardeningConfig::needs_privileged_setup);
            if plugin_owns_user_switch {
                linux_options.user = Some(user.clone());
            } else {
                cmd.uid(user.uid);
                cmd.gid(user.gid);
            }
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

    /// The hard path. The graceful notice is the trait default
    /// (`signal_task_process` to the tracked process group); the worker's
    /// reaper calls this once the task has exited or its window has closed.
    fn stop(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        crate::task_plugins::reap_task_process(allocation.id, &task.name, instance, "exec");
        // Whole-tree teardown, the second thing `WorkerCgroup` buys: a
        // double-forked descendant that reparented away from the tracked pid
        // still lives in this cgroup, so `cgroup.kill` reaps it where
        // `kill(pid)` would have orphaned it. Strictly after the reap, so a
        // task that drained cleanly inside its window is never SIGKILLed for
        // the privilege.
        if let Some(assignment) = instance.resource_assignment.as_ref() {
            crate::worker::resources::kill_cgroup(assignment);
        }
        // The cgroup directory itself is worker core's to remove (it owns the
        // allocator and the slice masks); this only cleans up the legacy
        // per-allocation cgroup from before the hierarchy existed.
        if enforcement_enabled() && instance.resource_assignment.is_none() {
            remove_managed_cgroup(allocation.id, &task.name);
        }
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
    let managed_cgroup = options.managed_cgroup.clone();
    let options = options.clone();
    let task_name = task_name.to_string();
    let allocation_id = *allocation_id;
    unsafe {
        cmd.pre_exec(move || {
            // Ordered by privilege: everything that needs the worker's
            // capabilities runs before the process drops to the task user, and
            // the unprivileged restrictions go on last so they cover the exec.
            if let Some(score) = options.oom_score_adj {
                std::fs::write("/proc/self/oom_score_adj", score.to_string())
                    .map_err(std::io::Error::other)?;
            }

            // 1. Join a cgroup — explicit `config.cgroup` first, else the one
            //    the worker created for this allocation.
            let cgroup_target = options
                .cgroup
                .as_ref()
                .map(std::path::PathBuf::from)
                .or_else(|| options.managed_cgroup.clone());
            if let Some(cgroup) = cgroup_target {
                let unified = Path::new("/sys/fs/cgroup/cgroup.controllers");
                if !unified.exists() {
                    return Err(std::io::Error::other("exec task cgroup requires cgroup v2"));
                }
                let cgroup_procs = Path::new(crate::task_plugins::exec_isolation::CGROUP_ROOT)
                    .join(cgroup)
                    .join("cgroup.procs");
                std::fs::write(cgroup_procs, std::process::id().to_string())
                    .map_err(std::io::Error::other)?;
            }

            // 1.5 Gated networking: enter the allocation's namespace, whose
            //     only way out is the datapath's listeners. Before the user
            //     switch (setns needs CAP_SYS_ADMIN) and before landlock.
            if let Some(ref netns) = options.netns {
                let file = std::fs::File::open(netns)?;
                use std::os::fd::AsRawFd;
                if libc::setns(file.as_raw_fd(), libc::CLONE_NEWNET) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }

            if let Some(ref hardening) = options.hardening {
                // 2. Private /tmp: unshare the mount namespace, make it
                //    private so nothing propagates back to the host, then
                //    mount a fresh tmpfs over /tmp.
                if hardening.private_tmp {
                    apply_private_tmp()?;
                }

                // 3. Drop capabilities from the bounding set (needs
                //    CAP_SETPCAP, so before the user switch).
                if let Some(ref capabilities) = hardening.capabilities {
                    let last = cap_last_cap();
                    for bit in capabilities_to_drop(capabilities, last) {
                        // PR_CAPBSET_DROP. A capability this kernel does not
                        // have is not an error worth failing a task over.
                        libc::prctl(libc::PR_CAPBSET_DROP, bit as libc::c_ulong, 0, 0, 0);
                    }
                }
            }

            // 4. Become the task user, now that the privileged work is done.
            if let Some(ref user) = options.user {
                if libc::setgid(user.gid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::setuid(user.uid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }

            if let Some(ref hardening) = options.hardening {
                // 5. No new privileges, then the cosmetic knobs.
                if hardening.no_new_privileges
                    && libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                if let Some(ref umask) = hardening.umask {
                    let mode = parse_umask(umask).map_err(std::io::Error::other)?;
                    libc::umask(mode as libc::mode_t);
                }
                if let Some(nice) = hardening.nice {
                    libc::setpriority(libc::PRIO_PROCESS, 0, nice);
                }
            }

            // 6. Landlock last: it restricts this process for good, including
            //    paths the steps above needed.
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
        managed_cgroup = managed_cgroup
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        "Configured Linux process options for exec task"
    );
    Ok(())
}

/// Give the child its own `/tmp`: a private mount namespace with a fresh
/// tmpfs over it. Runs in the forked child, before the privilege drop.
#[cfg(target_os = "linux")]
fn apply_private_tmp() -> std::io::Result<()> {
    use std::ffi::CString;

    let none = CString::new("none").expect("static cstr");
    let root = CString::new("/").expect("static cstr");
    let tmpfs = CString::new("tmpfs").expect("static cstr");
    let tmp = CString::new("/tmp").expect("static cstr");
    let empty = CString::new("").expect("static cstr");

    if unsafe { libc::unshare(libc::CLONE_NEWNS) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // Nothing this task does to its mounts may propagate back to the host.
    if unsafe {
        libc::mount(
            none.as_ptr(),
            root.as_ptr(),
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe {
        libc::mount(
            tmpfs.as_ptr(),
            tmp.as_ptr(),
            tmpfs.as_ptr(),
            libc::MS_NOSUID | libc::MS_NODEV,
            empty.as_ptr() as *const libc::c_void,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
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
    if options.hardening.is_some() {
        warn!(allocation_id, task_name = %task_name, "process hardening is only supported on Linux and will be ignored");
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

    #[test]
    fn hardening_parses_and_validates_from_the_jobspec() -> R<()> {
        let plugin = ExecTaskPlugin;
        let config = json!({
            "command": ["echo", "hello"],
            "user": "nobody",
            "hardening": {
                "noNewPrivileges": true,
                "privateTmp": true,
                "capabilities": { "drop": ["all"] },
                "umask": "0027",
                "nice": 5,
            }
        });

        plugin.validate_config(&config)?;
        let options = plugin.config_linux_options(&config)?;
        let hardening = options.hardening.expect("hardening parsed");
        assert!(hardening.no_new_privileges);
        assert!(hardening.private_tmp);
        assert_eq!(hardening.umask.as_deref(), Some("0027"));
        assert_eq!(hardening.nice, Some(5));
        // Namespaces and capability drops need privilege, so the plugin owns
        // the user switch for this task rather than letting std drop first.
        assert!(hardening.needs_privileged_setup());
        Ok(())
    }

    #[test]
    fn hardening_rejects_values_the_kernel_would() {
        let plugin = ExecTaskPlugin;
        let config = json!({
            "command": ["echo"],
            "hardening": { "capabilities": { "drop": ["cap_teleport"] } }
        });
        let err = plugin
            .validate_config(&config)
            .expect_err("unknown capability");
        assert!(
            err.to_string().contains("cap_teleport"),
            "error should name the offending capability, got: {err}"
        );
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

    /// The stop **notice** is delivered and returns immediately: `stop_with`
    /// runs under the worker's task-manager lock, so a runtime that blocked
    /// here for the grace window would stall heartbeats. A trapping process
    /// then exits on its own, inside the window, without anyone killing it.
    #[cfg(unix)]
    #[test]
    fn stop_with_signals_promptly_and_lets_a_trapping_process_drain() -> R<()> {
        let plugin = ExecTaskPlugin;
        let allocation = crate::structs::Allocation {
            id: 4242,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        };
        let task = TaskConfig {
            config: json!({
                "command": ["/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 0.1; done"],
            }),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: std::time::Duration::from_secs(30),
                drain: None,
            }),
            ..TaskConfig::new("drain", "exec")
        };

        let mut instance = TaskInstance::default();
        plugin.start(&allocation, &task, &mut instance)?;
        assert!(instance.pid.is_some(), "task process started");
        // Let the shell reach its `trap` before signalling it, or the test
        // measures a startup race instead of the stop path.
        std::thread::sleep(std::time::Duration::from_millis(300));

        let ctx = crate::task_plugins::StopContext::with_ladder(
            crate::structs::StopCause::Unknown,
            "SIGTERM",
            std::time::Duration::from_secs(30),
        );
        let started = std::time::Instant::now();
        let signalled = plugin.stop_with(&allocation, &task, &mut instance, &ctx)?;
        let elapsed = started.elapsed();

        assert!(signalled, "a tracked host process can be signalled");
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "the notice must not wait out the window, took {elapsed:?}"
        );
        // Still tracked: the notice is not the kill, and the worker's reaper
        // owns the handle until the process actually goes away.
        assert!(
            instance.pid.is_some(),
            "the process is draining, not reaped"
        );

        // It drains on its own, well inside the 30s window. Polled through
        // the child handle, not `kill(pid, 0)`: an exited-but-unreaped child
        // is a zombie, and a liveness probe would call it alive forever.
        let mut exited = false;
        for _ in 0..200 {
            if !matches!(
                instance.child.as_mut().expect("child handle").try_wait(),
                Ok(None)
            ) {
                exited = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(exited, "a trapping process exits on its stop signal");
        plugin.stop(&allocation, &task, &mut instance)?;
        Ok(())
    }

    /// And `stop` is the hard step: a process that ignored the notice is
    /// killed, promptly. The window belongs to the worker's reaper, so this
    /// call must not wait one out either.
    #[cfg(unix)]
    #[test]
    fn stop_hard_kills_a_process_that_ignored_the_notice() -> R<()> {
        let plugin = ExecTaskPlugin;
        let allocation = crate::structs::Allocation {
            id: 4243,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        };
        let task = TaskConfig {
            config: json!({
                "command": ["/bin/sh", "-c", "trap '' TERM; while :; do sleep 0.1; done"],
            }),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: std::time::Duration::from_secs(1),
                drain: None,
            }),
            ..TaskConfig::new("stubborn", "exec")
        };

        let mut instance = TaskInstance::default();
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_millis(300));
        let pid = instance.pid.expect("pid");

        let ctx = crate::task_plugins::StopContext::with_ladder(
            crate::structs::StopCause::Unknown,
            "SIGTERM",
            std::time::Duration::from_secs(1),
        );
        plugin.stop_with(&allocation, &task, &mut instance, &ctx)?;
        // It traps TERM, so it is still alive when the reaper gives up.
        assert!(
            crate::signals::pid_alive(pid),
            "the task ignored its notice"
        );

        let started = std::time::Instant::now();
        plugin.stop(&allocation, &task, &mut instance)?;
        let elapsed = started.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "the hard kill must not wait out a window, took {elapsed:?}"
        );
        assert!(instance.pid.is_none(), "the process was reaped");
        Ok(())
    }

    /// The `WorkerCgroup` adoption, asserted at the seam a unit test can
    /// reach: the assignment becomes the cgroup the child is spawned into,
    /// and an explicit `config.cgroup` still wins (back-compat).
    #[test]
    fn the_assignment_supplies_the_cgroup_unless_the_job_named_one() -> R<()> {
        let plugin = ExecTaskPlugin;
        let assignment = crate::task_plugins::ResourceAssignment {
            slice: crate::task_plugins::Slice::Share,
            cgroup_path: std::path::PathBuf::from("legion/share/legion-3"),
            cores: None,
            cpu_weight: Some(500),
            memory_limit_bytes: Some(64 * 1024 * 1024),
        };

        // No explicit cgroup: the worker's assignment is what the task joins.
        let mut options = plugin.config_linux_options(&json!({ "command": ["true"] }))?;
        let instance = TaskInstance {
            resource_assignment: Some(assignment.clone()),
            ..TaskInstance::default()
        };
        if options.cgroup.is_none() {
            options.managed_cgroup = instance
                .resource_assignment
                .as_ref()
                .map(|assignment| assignment.cgroup_path.clone());
        }
        assert_eq!(
            options.managed_cgroup.as_deref(),
            Some(std::path::Path::new("legion/share/legion-3"))
        );

        // An explicit cgroup opts the task out of the Legion hierarchy: it
        // keeps meaning what it has always meant.
        let mut named = plugin.config_linux_options(&json!({
            "command": ["true"],
            "cgroup": "workloads/demo",
        }))?;
        if named.cgroup.is_none() {
            named.managed_cgroup = Some(assignment.cgroup_path.clone());
        }
        assert_eq!(named.cgroup.as_deref(), Some("workloads/demo"));
        assert_eq!(named.managed_cgroup, None);
        Ok(())
    }

    /// End-to-end Linux: an exec task's declared `memory` becomes a real
    /// `memory.max` on the cgroup worker core created for it, and a
    /// double-forked descendant is reaped by `cgroup.kill` where the
    /// tracked-pid path would have orphaned it.
    ///
    /// Run on a node with cgroup v2 and a root worker.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires cgroup v2 + root"]
    fn exec_tasks_join_their_worker_created_cgroup() -> R<()> {
        let plugin = ExecTaskPlugin;
        let allocation = crate::structs::Allocation {
            id: 9101,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        };
        let task = TaskConfig {
            config: json!({
                // A double-forked grandchild that outlives its parent: the
                // case cgroup.kill exists to catch.
                "command": ["/bin/sh", "-c", "(sleep 300 &) ; sleep 300"],
            }),
            resources: json!({ "memory": 64 }),
            ..TaskConfig::new("cgrouptest", "exec")
        };

        let mut governor = crate::worker::resources::NodeResourceGovernor::from_env();
        assert!(
            governor.enabled(),
            "run this test with resourceEnforcement enabled"
        );
        let assignment = governor
            .assign(
                allocation.id,
                &task,
                crate::task_plugins::ResourceEnforcement::WorkerCgroup,
            )?
            .expect("governed");

        let mut instance = TaskInstance {
            resource_assignment: Some(assignment.clone()),
            ..TaskInstance::default()
        };
        plugin.start(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_millis(500));

        let dir = assignment.absolute_cgroup_path();
        assert_eq!(
            std::fs::read_to_string(dir.join("memory.max"))?.trim(),
            (64u64 * 1024 * 1024).to_string(),
            "the declared memory limit is a real cgroup limit"
        );
        let procs = std::fs::read_to_string(dir.join("cgroup.procs"))?;
        assert!(
            procs.lines().count() >= 2,
            "the task and its double-forked grandchild are both in the cgroup: {procs:?}"
        );

        plugin.stop(&allocation, &task, &mut instance)?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        let remaining = std::fs::read_to_string(dir.join("cgroup.procs")).unwrap_or_default();
        assert!(
            remaining.trim().is_empty(),
            "cgroup.kill reaped the whole tree: {remaining:?}"
        );

        governor.release(allocation.id, Some(&assignment));
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
