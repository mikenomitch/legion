//! The default seccomp profile for Linux containers (worker-plane-auth plan
//! §6.14; docs/task-runtime-container.md, "Security").
//!
//! It refuses, with `EPERM`, the syscalls Docker's default profile keeps away
//! from containers: kernel modules and `kexec`, `mount` and the new mount
//! API, `unshare` and namespace-creating `clone` flags (`setns` too), `bpf`,
//! `perf_event_open`, the kernel keyring (`keyctl`, `add_key`,
//! `request_key`), clock and time setting, `reboot`, swap, `acct`,
//! `quotactl`, `pivot_root`, `ptrace`-adjacent cross-process memory access,
//! `userfaultfd`, NUMA memory policy, handle-based opens, and the legacy
//! syscalls (`uselib`, `ustat`, `sysfs`, `_sysctl`, `vm86`, ...). `clone3` answers
//! `ENOSYS`, so libc falls back to `clone`, whose flags the filter can see.
//!
//! The list follows Docker's documented blocked set ("Significant syscalls
//! blocked by the default profile", moby/moby `profiles/seccomp`, Apache-2.0).
//! Docker's profile is an allowlist; this one is the equivalent denylist over
//! the same syscalls, which is the difference the public doc states: a
//! syscall a future kernel adds is allowed here until the list names it.

use crate::prelude::*;

/// `EPERM`, as the kernel and Docker's profile return it.
const EPERM: u32 = 1;
/// `ENOSYS`: "not implemented", so callers fall back.
const ENOSYS: u32 = 38;

/// The syscalls the default profile refuses outright.
pub const BLOCKED: &[&str] = &[
    // Accounting, clocks, and the machine itself.
    "acct",
    "adjtimex",
    "clock_adjtime",
    "clock_settime",
    "settimeofday",
    "stime",
    "reboot",
    "swapon",
    "swapoff",
    // Kernel modules and kexec.
    "create_module",
    "delete_module",
    "finit_module",
    "init_module",
    "get_kernel_syms",
    "query_module",
    "kexec_file_load",
    "kexec_load",
    // Mounts and namespaces.
    "mount",
    "umount",
    "umount2",
    "pivot_root",
    "fsconfig",
    "fsmount",
    "fsopen",
    "fspick",
    "move_mount",
    "open_tree",
    "mount_setattr",
    "unshare",
    "setns",
    // Kernel attack surface.
    "bpf",
    "perf_event_open",
    "userfaultfd",
    "lookup_dcookie",
    "nfsservctl",
    "quotactl",
    "quotactl_fd",
    // The kernel keyring.
    "add_key",
    "keyctl",
    "request_key",
    // Other processes' memory and handles.
    "kcmp",
    "process_vm_readv",
    "process_vm_writev",
    "name_to_handle_at",
    "open_by_handle_at",
    // NUMA policy (needs CAP_SYS_NICE in Docker's profile).
    "get_mempolicy",
    "mbind",
    "move_pages",
    "set_mempolicy",
    // Port I/O and legacy syscalls.
    "ioperm",
    "iopl",
    "sysfs",
    "_sysctl",
    "uselib",
    "ustat",
    "vm86",
    "vm86old",
];

/// The namespace flags a `clone` may not carry.
pub const CLONE_NAMESPACE_FLAGS: &[(&str, u64)] = &[
    ("CLONE_NEWNS", 0x0002_0000),
    ("CLONE_NEWCGROUP", 0x0200_0000),
    ("CLONE_NEWUTS", 0x0400_0000),
    ("CLONE_NEWIPC", 0x0800_0000),
    ("CLONE_NEWUSER", 0x1000_0000),
    ("CLONE_NEWPID", 0x2000_0000),
    ("CLONE_NEWNET", 0x4000_0000),
];

/// The profile as the OCI `linux.seccomp` object.
pub fn default_profile() -> Value {
    let mut syscalls = vec![json!({
        "names": BLOCKED,
        "action": "SCMP_ACT_ERRNO",
        "errnoRet": EPERM,
    })];
    // `clone(flags, ...)`: refuse a namespace flag (one rule per flag, since
    // a filter compares one masked value at a time).
    for (_, flag) in CLONE_NAMESPACE_FLAGS {
        syscalls.push(json!({
            "names": ["clone"],
            "action": "SCMP_ACT_ERRNO",
            "errnoRet": EPERM,
            "args": [{ "index": 0, "value": flag, "valueTwo": flag, "op": "SCMP_CMP_MASKED_EQ" }],
        }));
    }
    syscalls.push(json!({
        "names": ["clone3"],
        "action": "SCMP_ACT_ERRNO",
        "errnoRet": ENOSYS,
    }));
    json!({
        "defaultAction": "SCMP_ACT_ALLOW",
        "architectures": ["SCMP_ARCH_X86_64", "SCMP_ARCH_X86", "SCMP_ARCH_X32", "SCMP_ARCH_AARCH64", "SCMP_ARCH_ARM"],
        "syscalls": syscalls,
    })
}

#[cfg(test)]
mod seccomp_test {
    use super::*;

    #[test]
    fn the_profile_refuses_namespaces_mounts_and_the_keyring_test() {
        let profile = default_profile();
        assert_eq!(profile["defaultAction"], "SCMP_ACT_ALLOW");
        let syscalls = profile["syscalls"].as_array().unwrap();
        let blocked: Vec<&str> = syscalls[0]["names"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        for name in [
            "unshare",
            "setns",
            "mount",
            "bpf",
            "keyctl",
            "kexec_load",
            "init_module",
        ] {
            assert!(blocked.contains(&name), "{name} is blocked");
        }
        let clone_rules = syscalls
            .iter()
            .filter(|rule| rule["names"][0] == "clone")
            .count();
        assert_eq!(clone_rules, CLONE_NAMESPACE_FLAGS.len());
        assert!(syscalls
            .iter()
            .any(|rule| rule["names"][0] == "clone3" && rule["errnoRet"] == ENOSYS));
        // No duplicates: runtimes refuse a profile that names a syscall twice
        // under the same action.
        let mut unique = blocked.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), blocked.len());
    }
}
