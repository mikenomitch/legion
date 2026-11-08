//! Resource enforcement and process hardening for the `exec` runtime.
//!
//! Two things live here, both Linux-only in effect and both parsed
//! everywhere so a jobspec validates identically on a macOS laptop:
//!
//! - **Legacy cgroup cleanup.** Worker-owned cgroups moved to worker core
//!   (`src/worker/resources.rs`), which now places `exec` *and* `container`
//!   tasks in one node-wide `legion/share` + `legion/reserve` hierarchy — see
//!   `docs/resource-enforcement.md`. What remains here is
//!   [`remove_managed_cgroup`], which tears down the per-allocation cgroups a
//!   *previous* worker version created (`legion/alloc-<id>-<task>`), so an
//!   upgrade does not orphan them.
//! - **The `hardening` block.** `noNewPrivileges`, capability bounding-set
//!   drops, a private `/tmp`, `umask`, and `nice` — the isolation floor
//!   containers already had.
//!
//! See `internal_docs/plans/host-process-supervision-plan.md` §§6.5–6.6 and
//! `docs/task-supervision.md`.

use crate::prelude::*;
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;

/// Mount point of the cgroup v2 hierarchy.
pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The `hardening` block on an `exec` task config.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecHardeningConfig {
    /// `prctl(PR_SET_NO_NEW_PRIVS)`: the process and its children can never
    /// gain privileges through setuid binaries or file capabilities.
    #[serde(default)]
    pub no_new_privileges: bool,
    /// Give the task its own `/tmp` (a private mount namespace plus a tmpfs),
    /// invisible to every other task on the node. Requires a worker with
    /// `CAP_SYS_ADMIN`.
    #[serde(default)]
    pub private_tmp: bool,
    /// Capabilities to drop from the bounding set before the task starts.
    /// `["all"]` drops everything. Requires `CAP_SETPCAP` on the worker.
    #[serde(default)]
    pub capabilities: Option<ExecCapabilities>,
    /// File-mode creation mask, octal (e.g. `"0027"`).
    #[serde(default)]
    pub umask: Option<String>,
    /// Scheduling niceness, `-20..=19`.
    #[serde(default)]
    pub nice: Option<i32>,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecCapabilities {
    /// Capability names (`CAP_NET_RAW`, case-insensitive, `CAP_` optional) or
    /// the single entry `all`.
    #[serde(default)]
    pub drop: Vec<String>,
    /// Capabilities a task that runs as another `user` keeps after the
    /// switch, as ambient capabilities that survive `exec`. Only
    /// `net_bind_service` may be named: it lets the task bind ports below
    /// 1024 without root, and every other capability is a path to root.
    /// Linux only; elsewhere it is accepted and does nothing (macOS lets any
    /// user bind those ports).
    #[serde(default)]
    pub add: Vec<String>,
}

/// The one capability `capabilities.add` may name.
pub const ADDABLE_CAPABILITY: &str = "net_bind_service";

impl ExecHardeningConfig {
    /// Reject what the kernel would only reject at start: a bad umask, an
    /// out-of-range nice value, an unknown capability name. Runs at submit
    /// (`legion job validate`) on every platform.
    pub fn validate(&self) -> R<()> {
        if let Some(umask) = self.umask.as_deref() {
            parse_umask(umask)?;
        }
        if let Some(nice) = self.nice {
            if !(-20..=19).contains(&nice) {
                bail!("exec hardening nice must be in -20..=19, got {nice}");
            }
        }
        if let Some(capabilities) = self.capabilities.as_ref() {
            for name in &capabilities.drop {
                if name.eq_ignore_ascii_case("all") {
                    continue;
                }
                if capability_bit(name).is_none() {
                    bail!("exec hardening: unknown capability `{name}`");
                }
            }
            let addable = capability_bit(ADDABLE_CAPABILITY);
            for name in &capabilities.add {
                let Some(bit) = capability_bit(name) else {
                    bail!("exec hardening: unknown capability `{name}`");
                };
                if Some(bit) != addable {
                    bail!(
                        "exec hardening: capabilities.add may name only {ADDABLE_CAPABILITY}, \
                         which lets a task bind ports below 1024 without root; {name:?} would \
                         make it root-equivalent"
                    );
                }
                if capabilities_to_drop(capabilities, cap_last_cap()).contains(&bit) {
                    bail!(
                        "exec hardening: capabilities {name:?} is both dropped and added; \
                         a dropped capability cannot be kept"
                    );
                }
            }
        }
        Ok(())
    }

    /// The capability bits the task keeps after its user switch
    /// (`capabilities.add`), validated: only [`ADDABLE_CAPABILITY`].
    pub fn capabilities_to_keep(&self) -> Vec<i32> {
        let addable = capability_bit(ADDABLE_CAPABILITY);
        let mut bits: Vec<i32> = self
            .capabilities
            .iter()
            .flat_map(|capabilities| capabilities.add.iter())
            .filter_map(|name| capability_bit(name))
            .filter(|bit| Some(*bit) == addable)
            .collect();
        bits.sort_unstable();
        bits.dedup();
        bits
    }

    /// Whether anything here needs privileges the task itself must not have —
    /// which is what decides whether the plugin takes over the `user` switch
    /// so it happens *after* the privileged steps.
    pub fn needs_privileged_setup(&self) -> bool {
        self.private_tmp
            || self.capabilities.as_ref().is_some_and(|capabilities| {
                !capabilities.drop.is_empty() || !capabilities.add.is_empty()
            })
    }
}

/// Octal umask string -> mode bits.
pub fn parse_umask(raw: &str) -> R<u32> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("exec hardening umask must not be empty");
    }
    u32::from_str_radix(trimmed.trim_start_matches("0o"), 8)
        .ok()
        .filter(|mode| *mode <= 0o777)
        .with_context(|| format!("exec hardening umask must be octal 0..=0777, got `{raw}`"))
}

/// Capability name -> bounding-set bit. Accepts `CAP_NET_RAW`, `net_raw`, and
/// anything in between; the table is the Linux capability set as of 6.x.
pub fn capability_bit(name: &str) -> Option<i32> {
    const CAPABILITIES: [&str; 41] = [
        "chown",
        "dac_override",
        "dac_read_search",
        "fowner",
        "fsetid",
        "kill",
        "setgid",
        "setuid",
        "setpcap",
        "linux_immutable",
        "net_bind_service",
        "net_broadcast",
        "net_admin",
        "net_raw",
        "ipc_lock",
        "ipc_owner",
        "sys_module",
        "sys_rawio",
        "sys_chroot",
        "sys_ptrace",
        "sys_pacct",
        "sys_admin",
        "sys_boot",
        "sys_nice",
        "sys_resource",
        "sys_time",
        "sys_tty_config",
        "mknod",
        "lease",
        "audit_write",
        "audit_control",
        "setfcap",
        "mac_override",
        "mac_admin",
        "syslog",
        "wake_alarm",
        "block_suspend",
        "audit_read",
        "perfmon",
        "bpf",
        "checkpoint_restore",
    ];

    let needle = name.trim().to_ascii_lowercase();
    let needle = needle.strip_prefix("cap_").unwrap_or(&needle);
    CAPABILITIES
        .iter()
        .position(|candidate| *candidate == needle)
        .map(|index| index as i32)
}

/// The capability bits a `drop` list resolves to, expanded for `all`.
pub fn capabilities_to_drop(capabilities: &ExecCapabilities, cap_last_cap: i32) -> Vec<i32> {
    if capabilities
        .drop
        .iter()
        .any(|name| name.eq_ignore_ascii_case("all"))
    {
        return (0..=cap_last_cap).collect();
    }
    capabilities
        .drop
        .iter()
        .filter_map(|name| capability_bit(name))
        .collect()
}

/// Highest capability number this kernel knows. Falls back to the last entry
/// in our own table, which is the conservative direction: dropping a bit the
/// kernel does not have is an error we ignore, missing one is a hole.
pub fn cap_last_cap() -> i32 {
    std::fs::read_to_string("/proc/sys/kernel/cap_last_cap")
        .ok()
        .and_then(|raw| raw.trim().parse::<i32>().ok())
        .unwrap_or(40)
}

/// Whether this worker was started with `resourceEnforcement.enabled`.
pub fn enforcement_enabled() -> bool {
    std::env::var(crate::constants::ENV_RESOURCE_ENFORCEMENT)
        .map(|value| value == "1")
        .unwrap_or(false)
}

/// The configured cgroup parent (relative to [`CGROUP_ROOT`]).
pub fn cgroup_parent() -> String {
    std::env::var(crate::constants::ENV_CGROUP_PARENT)
        .ok()
        .map(|value| value.trim().trim_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "legion".to_string())
}

/// Per-allocation cgroup path, relative to [`CGROUP_ROOT`]. Deterministic so
/// teardown can find it again without carrying state through the instance.
pub fn managed_cgroup_path(parent: &str, allocation_id: i64, task_name: &str) -> PathBuf {
    let task = task_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    PathBuf::from(parent).join(format!("alloc-{allocation_id}-{task}"))
}

/// Remove an allocation's cgroup. Best-effort: an rmdir that fails because a
/// process is still in the group is normal during teardown, and the next
/// start of the same allocation id reuses the directory anyway.
#[cfg(target_os = "linux")]
pub fn remove_managed_cgroup(allocation_id: i64, task_name: &str) {
    let relative = managed_cgroup_path(&cgroup_parent(), allocation_id, task_name);
    let absolute = std::path::Path::new(CGROUP_ROOT).join(relative);
    if absolute.exists() {
        if let Err(err) = std::fs::remove_dir(&absolute) {
            debug!(
                allocation_id,
                task_name,
                error = ?err,
                path = %absolute.display(),
                "Could not remove task cgroup"
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn remove_managed_cgroup(_allocation_id: i64, _task_name: &str) {}

#[cfg(test)]
mod exec_isolation_test {
    use super::*;

    #[test]
    fn managed_cgroup_paths_are_deterministic_and_sanitized() {
        assert_eq!(
            managed_cgroup_path("legion", 42, "web"),
            PathBuf::from("legion/alloc-42-web")
        );
        assert_eq!(
            managed_cgroup_path("legion/prod", 7, "api/v2"),
            PathBuf::from("legion/prod/alloc-7-api-v2")
        );
    }

    #[test]
    fn capability_names_resolve_in_any_spelling() {
        assert_eq!(capability_bit("CAP_NET_RAW"), Some(13));
        assert_eq!(capability_bit("net_raw"), Some(13));
        assert_eq!(capability_bit("cap_chown"), Some(0));
        assert_eq!(capability_bit("not_a_capability"), None);
    }

    #[test]
    fn drop_all_expands_to_every_capability() {
        let all = ExecCapabilities {
            drop: vec!["all".into()],
            ..Default::default()
        };
        assert_eq!(capabilities_to_drop(&all, 40).len(), 41);

        let some = ExecCapabilities {
            drop: vec!["CAP_NET_RAW".into(), "sys_admin".into()],
            ..Default::default()
        };
        assert_eq!(capabilities_to_drop(&some, 40), vec![13, 21]);
    }

    #[test]
    fn hardening_validation_catches_what_the_kernel_would() {
        let bad_umask = ExecHardeningConfig {
            umask: Some("999".into()),
            ..Default::default()
        };
        assert!(bad_umask.validate().is_err());

        let bad_nice = ExecHardeningConfig {
            nice: Some(42),
            ..Default::default()
        };
        assert!(bad_nice.validate().is_err());

        let bad_cap = ExecHardeningConfig {
            capabilities: Some(ExecCapabilities {
                drop: vec!["cap_teleport".into()],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(bad_cap.validate().is_err());

        let good = ExecHardeningConfig {
            no_new_privileges: true,
            private_tmp: true,
            capabilities: Some(ExecCapabilities {
                drop: vec!["all".into()],
                ..Default::default()
            }),
            umask: Some("0027".into()),
            nice: Some(5),
        };
        assert!(good.validate().is_ok());
        assert_eq!(parse_umask("0027").expect("umask"), 0o27);
    }

    /// `add` takes `net_bind_service` alone, in any spelling, and never
    /// together with a drop that removes it.
    #[test]
    fn only_net_bind_service_can_be_added() {
        let adding = |add: &[&str], drop: &[&str]| ExecHardeningConfig {
            capabilities: Some(ExecCapabilities {
                add: add.iter().map(|name| name.to_string()).collect(),
                drop: drop.iter().map(|name| name.to_string()).collect(),
            }),
            ..Default::default()
        };
        let ok = adding(&["CAP_NET_BIND_SERVICE"], &["net_raw"]);
        ok.validate().expect("net_bind_service may be added");
        assert_eq!(ok.capabilities_to_keep(), vec![10]);
        assert!(ok.needs_privileged_setup());

        let err = adding(&["sys_admin"], &[]).validate().unwrap_err();
        assert!(
            err.to_string().contains("may name only net_bind_service"),
            "{err}"
        );
        assert!(adding(&["cap_teleport"], &[]).validate().is_err());
        let err = adding(&["net_bind_service"], &["all"])
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("both dropped and added"), "{err}");
        assert!(adding(&[], &[]).capabilities_to_keep().is_empty());
    }

    #[test]
    fn only_privilege_needing_hardening_takes_over_the_user_switch() {
        let plain = ExecHardeningConfig {
            no_new_privileges: true,
            umask: Some("0022".into()),
            ..Default::default()
        };
        assert!(!plain.needs_privileged_setup());

        let namespaced = ExecHardeningConfig {
            private_tmp: true,
            ..Default::default()
        };
        assert!(namespaced.needs_privileged_setup());
    }
}
