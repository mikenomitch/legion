use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use crate::task_plugins::{spawn_task_process, TaskPlugin, TaskRuntime};
#[cfg(target_os = "linux")]
use landlock::{AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::Command;
#[cfg(unix)]
use std::{ffi::CString, path::Path};

#[derive(Clone, Debug, Default)]
struct ExecLinuxOptions {
    landlock: Option<LandlockOptions>,
    oom_score_adj: Option<i32>,
    cgroup: Option<String>,
}

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
    fn config_command(&self, config: &Value) -> R<Vec<String>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let command = obj
            .get("command")
            .context("exec task config requires command")?;
        let command = command
            .as_array()
            .context("exec task command must be a list of strings")?;
        let command = command
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(|val| val.to_string())
                    .context("exec task command values must be strings")
            })
            .collect::<R<Vec<_>>>()?;
        if command.is_empty() {
            bail!("exec task command must include at least one entry");
        }
        Ok(command)
    }

    fn config_env(&self, config: &Value) -> R<HashMap<String, String>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let Some(env) = obj.get("env") else {
            return Ok(HashMap::new());
        };
        let env = env.as_object().context("exec task env must be an object")?;
        env.iter()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .context("exec task env values must be strings")?;
                Ok((key.clone(), value.to_string()))
            })
            .collect::<R<HashMap<_, _>>>()
    }

    fn config_cwd(&self, config: &Value) -> R<Option<String>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let cwd = obj.get("cwd");
        match cwd {
            None => Ok(None),
            Some(value) => Ok(Some(
                value
                    .as_str()
                    .context("exec task cwd must be a string")?
                    .to_string(),
            )),
        }
    }

    #[cfg(unix)]
    fn config_user(&self, config: &Value) -> R<Option<UserSpec>> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;
        let Some(value) = obj.get("user") else {
            return Ok(None);
        };
        let user = value.as_str().context("exec task user must be a string")?;
        lookup_user(user).map(Some)
    }

    fn config_linux_options(&self, config: &Value) -> R<ExecLinuxOptions> {
        let obj = config
            .as_object()
            .context("exec task config must be an object")?;

        let landlock = match obj.get("landlock") {
            None => None,
            Some(value) => Some(parse_landlock_config(value)?),
        };

        let oom_adj = obj.get("oom_adj");
        let oom_score_adj = obj.get("oom_score_adj");
        if oom_adj.is_some() && oom_score_adj.is_some() {
            bail!("exec task config may specify only one of oom_adj or oom_score_adj");
        }
        let oom_score_adj = match oom_adj.or(oom_score_adj) {
            None => None,
            Some(value) => Some(parse_oom_score_adj(value)?),
        };

        let cgroup = match obj.get("cgroup") {
            None => None,
            Some(value) => {
                let cgroup = value
                    .as_str()
                    .context("exec task cgroup must be a string")?
                    .trim_matches('/')
                    .to_string();
                if cgroup.is_empty() {
                    bail!("exec task cgroup must not be empty");
                }
                Some(cgroup)
            }
        };

        Ok(ExecLinuxOptions {
            landlock,
            oom_score_adj,
            cgroup,
        })
    }
}

impl TaskPlugin for ExecTaskPlugin {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string" }
                },
                "env": {
                    "type": "object",
                    "additionalProperties": { "type": "string" }
                },
                "cwd": { "type": "string" },
                "user": { "type": "string" },
                "oom_adj": { "type": "integer", "minimum": -1000, "maximum": 1000 },
                "oom_score_adj": { "type": "integer", "minimum": -1000, "maximum": 1000 },
                "cgroup": { "type": "string" },
                "landlock": {
                    "type": "object",
                    "properties": {
                        "read_only_paths": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "read_write_paths": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": true
        })
    }

    fn readiness(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("exec".into(), Value::String("pass".into()));
        map
    }

    fn prepare(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        _runtime: &mut TaskRuntime,
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
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        let command = self.config_command(&task.config)?;
        let env = self.config_env(&task.config)?;
        let cwd = self.config_cwd(&task.config)?;
        #[cfg(unix)]
        let user = self.config_user(&task.config)?;
        let linux_options = self.config_linux_options(&task.config)?;
        let (program, args) = command
            .split_first()
            .context("exec task command must include at least one entry")?;

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            command = program,
            "Starting exec task"
        );

        let mut cmd = Command::new(program);
        cmd.args(args);
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }
        if !env.is_empty() {
            cmd.envs(env);
        }

        #[cfg(unix)]
        if let Some(user) = user {
            cmd.uid(user.uid);
            cmd.gid(user.gid);
        }

        configure_linux_process_options(&mut cmd, &linux_options, &allocation.id, &task.name)?;

        let child = spawn_task_process(&mut cmd).with_context(|| {
            format!(
                "Failed to spawn exec task {} for allocation {}",
                task.name, allocation.id
            )
        })?;
        runtime.pid = Some(child.id());
        runtime.child = Some(child);
        Ok(())
    }

    fn stop(&self, allocation: &Allocation, task: &TaskConfig, runtime: &mut TaskRuntime) -> R<()> {
        if let Some(mut child) = runtime.child.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                "Stopping exec task"
            );
            let _ = child.kill();
            let _ = child.wait();
            runtime.pid = None;
        } else if let Some(pid) = runtime.pid.take() {
            info!(
                allocation_id = allocation.id,
                task_name = %task.name,
                pid,
                "Stopping exec task by pid"
            );
            let mut system = sysinfo::System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
                process.kill();
            }
        }
        Ok(())
    }

    fn destroy(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
    ) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Destroying exec task"
        );
        self.stop(allocation, task, runtime)
    }
}

fn parse_oom_score_adj(value: &Value) -> R<i32> {
    let score = value
        .as_i64()
        .context("exec task oom_adj must be an integer")?;
    if !(-1000..=1000).contains(&score) {
        bail!("exec task oom_adj must be between -1000 and 1000");
    }
    Ok(score as i32)
}

fn parse_landlock_config(value: &Value) -> R<LandlockOptions> {
    let obj = value
        .as_object()
        .context("exec task landlock must be an object")?;
    let read_only_paths = parse_path_list(obj.get("read_only_paths"), "read_only_paths")?;
    let read_write_paths = parse_path_list(obj.get("read_write_paths"), "read_write_paths")?;
    if read_only_paths.is_empty() && read_write_paths.is_empty() {
        bail!("exec task landlock must specify at least one path");
    }
    Ok(LandlockOptions {
        read_only_paths,
        read_write_paths,
    })
}

fn parse_path_list(value: Option<&Value>, key: &str) -> R<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .with_context(|| format!("exec task landlock {key} must be a list of strings"))?;
    values
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(ToOwned::to_owned)
                .with_context(|| format!("exec task landlock {key} must contain only strings"))
        })
        .collect::<R<Vec<_>>>()
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
