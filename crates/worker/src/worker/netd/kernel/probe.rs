//! Can this node run the kernel classifier? Probed once at start, by
//! feature, not by kernel version (distribution kernels backport): the
//! object must load, every program must pass the verifier, and the programs
//! must attach to a cgroup. The answer is the `netKernel` node attribute.

/// The probe's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    /// Every program loads and attaches.
    Active,
    Unavailable(String),
}

/// What the attribute reads when the classifier runs.
pub const ACTIVE_FEATURES: &str =
    "ebpf: cgroup sock_addr, sock_create, sockops, cgroup_skb, ringbuf";

impl Availability {
    /// The `netKernel` node attribute value.
    pub fn attribute(&self) -> String {
        match self {
            Availability::Active => format!("active ({ACTIVE_FEATURES})"),
            Availability::Unavailable(reason) => format!("unavailable ({reason})"),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Availability::Active)
    }
}

/// Probe under `cgroup_root` (the cgroup v2 mount). Creates, attaches to,
/// and removes one throwaway cgroup.
pub fn probe(cgroup_root: &std::path::Path) -> Availability {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cgroup_root;
        Availability::Unavailable("not Linux".into())
    }
    #[cfg(target_os = "linux")]
    {
        linux::probe(cgroup_root)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::Availability;
    use crate::worker::netd::kernel::runtime::{KernelNet, KernelSettings};

    pub fn probe(cgroup_root: &std::path::Path) -> Availability {
        if !cgroup_root.join("cgroup.controllers").exists() {
            return Availability::Unavailable(format!(
                "no cgroup v2 unified hierarchy at {}",
                cgroup_root.display()
            ));
        }
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } != 0 {
            return Availability::Unavailable("the worker is not root".into());
        }
        let scratch = format!("legion-net-probe-{}", std::process::id());
        let settings = KernelSettings {
            cgroup_root: cgroup_root.to_path_buf(),
            cgroup_parent: scratch.clone(),
            pin_dir: None,
        };
        let result = KernelNet::open(settings);
        let outcome = match result {
            Ok(net) => {
                net.shutdown();
                Availability::Active
            }
            Err(err) => Availability::Unavailable(short(&format!("{err:#}"))),
        };
        let _ = std::fs::remove_dir(cgroup_root.join(&scratch));
        outcome
    }

    /// The first line of a loader error: verifier logs are long.
    fn short(error: &str) -> String {
        let line = error.lines().next().unwrap_or(error);
        if line.len() > 200 {
            format!("{}…", &line[..200])
        } else {
            line.to_string()
        }
    }
}
