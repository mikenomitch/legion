//! The datapath's persisted inputs (docs/network-handlers.md, "The datapath
//! across worker restarts").
//!
//! Tasks survive a worker restart; until this file existed, their network
//! did not: every listener, VIP, resolver, egress proxy and ingress port was
//! rebuilt only on the new worker's first *successful* heartbeat, which
//! could be seconds — or, with the manager unreachable, indefinitely — of
//! refused connections for workloads that were still running. The
//! namespaces themselves already survive (they are bind-mounted under
//! `/var/run/netns`), so the gap was purely the listeners inside them.
//!
//! The worker writes what the datapath was last told — the desired set,
//! the endpoint feed, the policy assignments and their bodies — after every
//! successful heartbeat that changed it, and a restarting worker replays it
//! before its first heartbeat. Fail-static, on disk: a node that restarts
//! while the manager is down comes back with exactly the datapath it had.
//!
//! Secrets never live here: the desired set carries task configs (the same
//! ones `worker_state.json` already persists, with the same owner-only
//! mode), never tokens or keys.

use crate::prelude::*;
use crate::structs::netpolicy::EffectivePolicy;
use crate::structs::{AllocNetPolicy, TaskAllocation, UpstreamEndpoints};
use serde::{Deserialize, Serialize};

use std::path::{Path, PathBuf};

/// What a heartbeat reply handed the datapath, minus everything that is
/// not the datapath's (stop notices, volume reclaim, cadence).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetdState {
    #[serde(default)]
    pub desired: Vec<TaskAllocation>,
    #[serde(default)]
    pub upstream_endpoints: Vec<UpstreamEndpoints>,
    #[serde(default)]
    pub net_policies: Vec<AllocNetPolicy>,
    /// Policy bodies by hash, so no fetch is needed to restore.
    #[serde(default)]
    pub policy_bodies: Vec<(String, EffectivePolicy)>,
}

impl NetdState {
    pub fn is_empty(&self) -> bool {
        self.desired.is_empty()
    }
}

/// `netd_state.json`, beside the worker state file.
pub struct NetdStateStore {
    path: PathBuf,
}

impl NetdStateStore {
    pub fn beside(worker_state_path: &Path) -> Self {
        NetdStateStore {
            path: worker_state_path.with_file_name("netd_state.json"),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> R<NetdState> {
        if !self.path.exists() {
            return Ok(NetdState::default());
        }
        let text = std::fs::read_to_string(&self.path)
            .with_context(|| format!("reading {}", self.path.display()))?;
        if text.trim().is_empty() {
            return Ok(NetdState::default());
        }
        serde_json::from_str(&text).with_context(|| format!("parsing {}", self.path.display()))
    }

    /// Atomic, owner-only write.
    pub fn save(&self, state: &NetdState) -> R<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_string(state)?;
        let tmp = self.path.with_extension("json.tmp");
        // Created 0600, not chmodded after the write: the worker state dir
        // is traverse-only, so a file at the umask is readable by path for
        // as long as it exists.
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        #[cfg(unix)]
        {
            // A leftover temp file keeps its old mode through `open`.
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        std::io::Write::write_all(&mut file, text.as_bytes())?;
        drop(file);
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    pub fn clear(&self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_tolerates_absence() -> R<()> {
        let dir = tempfile::TempDir::new()?;
        let store = NetdStateStore::beside(&dir.path().join("worker_state.json"));
        assert!(store.load()?.is_empty());
        let state = NetdState {
            desired: vec![TaskAllocation {
                allocation_id: 7,
                job_id: "web".into(),
                task_name: "server".into(),
                network: "{\"mode\":\"gated\"}".into(),
                ..Default::default()
            }],
            upstream_endpoints: vec![UpstreamEndpoints {
                allocation_id: 7,
                service: "db".into(),
                endpoints: Vec::new(),
                for_handler: false,
            }],
            net_policies: vec![AllocNetPolicy {
                allocation_id: 7,
                policy_hash: "abc".into(),
            }],
            policy_bodies: vec![("abc".into(), EffectivePolicy::default())],
        };
        store.save(&state)?;
        assert_eq!(store.path(), dir.path().join("netd_state.json"));
        assert_eq!(store.load()?, state);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(store.path())?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        store.clear();
        assert!(store.load()?.is_empty());
        Ok(())
    }
}
