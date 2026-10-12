//! The manager's side of the authenticated worker plane
//! (worker-plane-auth plan §6.2, §6.3).
//!
//! Every manager-to-worker call (dispatch, exec, actions, logs, restart, the
//! reachability probe) goes through [`WorkerPlane::client`], which:
//!
//! - reads the node's advertised `workerAddress` from its record (one place,
//!   not a copy per call site);
//! - refuses a worker that does not speak the mTLS worker plane (protocol
//!   version below 3, or a plaintext `http://` address), naming its version,
//!   with no plaintext fallback (calls entry 16);
//! - dials it over TLS with the cluster CA bundle as the only root,
//!   presenting this manager's client certificate
//!   (`legion://manager/<id>`);
//! - verifies the server certificate against the name of the node it means
//!   to reach (`<node>.node.legion`, or the simulator's fleet name), so a
//!   worker that registered a stolen or stale address can't receive another
//!   node's work;
//! - caches one channel per (CA, address, name), like the dispatcher's
//!   per-address cache before it.
//!
//! The manager mints its own certificates from the CA key it already holds
//! through the store, re-minting before they reach a third of their life.
//! The same materials back the cluster TLS the manager serves on its own
//! port to workers (`crate::manager::listener`).

use crate::cluster_tls::{
    certified_key, client_config, tls_channel, NamedServerVerifier, SwappableIdentity,
    SwappableRoots,
};
use crate::manager::mesh_ca::{
    fleet_dns_name, issue_manager_cert, node_dns_name, ManagerCertKind, NODE_CERT_REFRESH_AFTER,
};
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::worker_client::WorkerClient;
use crate::structs::Node;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::Instant;
use tonic::transport::Channel;
use tonic::Status;

/// The worker protocol version that serves the mTLS worker plane and
/// advertises a reachable `https://` address. Managers dial nothing older.
pub const MIN_WORKER_PLANE_PROTOCOL: u64 = 3;

/// The node config attribute the simulator sets so the manager verifies its
/// shared fleet certificate instead of a per-node one.
pub const WORKER_FLEET_ATTR: &str = "workerFleet";

/// Where and how to reach one node's worker plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTarget {
    pub node_id: String,
    /// The advertised address (`https://10.0.1.7:4848`).
    pub address: String,
    /// The DNS name its certificate must carry.
    pub server_name: String,
}

impl WorkerTarget {
    /// The target for a node record, or the reason it can't be dialed.
    pub fn for_node(node: &Node) -> Result<WorkerTarget, Status> {
        let address = worker_address(node).ok_or_else(|| {
            Status::failed_precondition(format!(
                "node {} has not advertised a worker address",
                node.id
            ))
        })?;
        let protocol = worker_protocol_version(node);
        if protocol < MIN_WORKER_PLANE_PROTOCOL || address.starts_with("http://") {
            return Err(Status::failed_precondition(format!(
                "node {} runs worker protocol {protocol} at {address}, which serves the worker \
                 plane without authentication; this manager dials only protocol \
                 {MIN_WORKER_PLANE_PROTOCOL} workers over mutual TLS\n\
                 Hint: upgrade the worker on node {} (docs/upgrades.md); work still reaches it \
                 through heartbeats meanwhile",
                node.id, node.id
            )));
        }
        let server_name = match worker_fleet(node) {
            Some(fleet) => fleet_dns_name(&fleet),
            None => node_dns_name(&node.id),
        };
        Ok(WorkerTarget {
            node_id: node.id.clone(),
            address,
            server_name,
        })
    }
}

/// The node's advertised worker-plane address, from its registration.
pub fn worker_address(node: &Node) -> Option<String> {
    node.config_attrs
        .as_object()?
        .get("workerAddress")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The simulator fleet a node belongs to, when it registered as one.
pub fn worker_fleet(node: &Node) -> Option<String> {
    node.config_attrs
        .as_object()?
        .get(WORKER_FLEET_ATTR)
        .and_then(Value::as_str)
        .filter(|fleet| !fleet.is_empty())
        .map(str::to_string)
}

/// The protocol version the node reported at registration (0 when it
/// predates the field).
pub fn worker_protocol_version(node: &Node) -> u64 {
    node.dynamic_attrs
        .get("protocolVersion")
        .or_else(|| node.config_attrs.get("protocolVersion"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

/// One cluster CA's worth of manager TLS material. Keyed by the signing
/// CA's pin, so several in-process clusters (tests) and a rotation each get
/// their own.
pub struct ManagerMaterials {
    pub client_identity: Arc<SwappableIdentity>,
    pub server_identity: Arc<SwappableIdentity>,
    pub roots: Arc<SwappableRoots>,
    minted_at: Mutex<Instant>,
}

impl std::fmt::Debug for ManagerMaterials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagerMaterials").finish_non_exhaustive()
    }
}

/// The process-wide worker plane client factory.
pub struct WorkerPlane {
    manager_id: RwLock<String>,
    materials: Mutex<HashMap<String, Arc<ManagerMaterials>>>,
    channels: Mutex<HashMap<(String, String, String), WorkerClient<Channel>>>,
}

impl WorkerPlane {
    /// The factory every call site shares. A manager names itself with
    /// [`WorkerPlane::set_manager_id`] at start; until then (and in tests)
    /// it is `local`.
    pub fn global() -> &'static WorkerPlane {
        static PLANE: OnceLock<WorkerPlane> = OnceLock::new();
        PLANE.get_or_init(|| WorkerPlane {
            manager_id: RwLock::new("local".into()),
            materials: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
        })
    }

    /// The id this manager puts in its certificates' URI SAN: the raft
    /// `nodeId`, or `local` for a single manager.
    pub fn set_manager_id(&self, id: &str) {
        *self.manager_id.write().unwrap_or_else(|p| p.into_inner()) = id.to_string();
        // Re-mint under the new name on next use.
        self.materials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }

    pub fn manager_id(&self) -> String {
        self.manager_id
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// This manager's TLS material for the cluster `store` belongs to,
    /// minted on first use and re-minted after a third of its life. Creates
    /// the cluster CA when none exists yet (a replicated write, so only a
    /// control-lane leader can).
    pub fn materials(&self, store: &Store) -> R<Arc<ManagerMaterials>> {
        let ca = match store.mesh_ca()? {
            Some(ca) => ca,
            None => store
                .ensure_mesh_ca()
                .context("the cluster CA does not exist yet and could not be created")?,
        };
        let pin = ca.pin()?;
        let existing = self
            .materials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&pin)
            .cloned();
        if let Some(materials) = existing {
            let stale = materials
                .minted_at
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .elapsed()
                >= NODE_CERT_REFRESH_AFTER;
            if stale {
                self.mint_into(&materials, &ca)?;
            }
            return Ok(materials);
        }
        let materials = Arc::new(ManagerMaterials {
            client_identity: SwappableIdentity::new(),
            server_identity: SwappableIdentity::new(),
            roots: SwappableRoots::from_pem(&ca.bundle_pem())?,
            minted_at: Mutex::new(Instant::now()),
        });
        self.mint_into(&materials, &ca)?;
        self.materials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(pin, materials.clone());
        Ok(materials)
    }

    fn mint_into(
        &self,
        materials: &ManagerMaterials,
        ca: &crate::storage::netpolicy_store::MeshCa,
    ) -> R<()> {
        let manager_id = self.manager_id();
        let (client_pem, client_key, _) = issue_manager_cert(
            &ca.cert_pem,
            &ca.key_pem,
            &manager_id,
            ManagerCertKind::Client,
        )?;
        let (server_pem, server_key, _) = issue_manager_cert(
            &ca.cert_pem,
            &ca.key_pem,
            &manager_id,
            ManagerCertKind::Server,
        )?;
        materials.client_identity.set(certified_key(
            &format!("{client_pem}{}", ca.chain_suffix()),
            &client_key,
        )?);
        materials.server_identity.set(certified_key(
            &format!("{server_pem}{}", ca.server_chain_suffix()),
            &server_key,
        )?);
        materials.roots.set_pem(&ca.bundle_pem())?;
        *materials
            .minted_at
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Instant::now();
        debug!(manager_id = %manager_id, "Minted manager cluster certificates");
        Ok(())
    }

    /// A worker client for `node`, dialing its advertised address over mTLS
    /// and verifying its certificate names it. Lazily connecting: a dead
    /// worker fails the call, not this function.
    pub fn client(&self, store: &Store, node: &Node) -> Result<WorkerClient<Channel>, Status> {
        let target = WorkerTarget::for_node(node)?;
        self.client_for(store, &target)
    }

    /// [`WorkerPlane::client`] for an already-resolved target.
    pub fn client_for(
        &self,
        store: &Store,
        target: &WorkerTarget,
    ) -> Result<WorkerClient<Channel>, Status> {
        let materials = self.materials(store).map_err(|err| {
            error!(error = ?err, "Cluster TLS material unavailable");
            Status::unavailable(format!(
                "this manager has no cluster certificate to dial workers with: {err:#}"
            ))
        })?;
        let pin = store
            .mesh_ca()
            .ok()
            .flatten()
            .and_then(|ca| ca.pin().ok())
            .unwrap_or_default();
        let key = (pin, target.address.clone(), target.server_name.clone());
        if let Some(client) = self
            .channels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
        {
            return Ok(client.clone());
        }
        let build = || -> R<WorkerClient<Channel>> {
            let verifier = NamedServerVerifier::new(materials.roots.clone(), &target.server_name)?;
            let config = client_config(verifier, Some(materials.client_identity.clone()))?;
            let channel = tls_channel(&target.address, config, &target.server_name)?;
            Ok(WorkerClient::new(channel))
        };
        let client = build().map_err(|err| {
            Status::failed_precondition(format!(
                "invalid worker address {} for node {}: {err:#}",
                target.address, target.node_id
            ))
        })?;
        self.channels
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, client.clone());
        Ok(client)
    }
}

#[cfg(test)]
mod worker_plane_test {
    use super::*;

    fn node(id: &str, config: Value, dynamic: Value) -> Node {
        Node {
            id: id.into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: config,
            fingerprint_attrs: json!({}),
            dynamic_attrs: dynamic,
            health_checks: json!({}),
        }
    }

    #[test]
    fn a_protocol_3_node_is_dialed_by_its_own_name_test() -> R<()> {
        let target = WorkerTarget::for_node(&node(
            "wkr-1",
            json!({"workerAddress": "https://10.0.1.7:4848"}),
            json!({"protocolVersion": 3}),
        ))?;
        assert_eq!(target.address, "https://10.0.1.7:4848");
        assert_eq!(target.server_name, "wkr-1.node.legion");
        Ok(())
    }

    #[test]
    fn a_fleet_node_is_dialed_by_its_fleet_name_test() -> R<()> {
        let target = WorkerTarget::for_node(&node(
            "sim-default-3",
            json!({"workerAddress": "https://10.0.9.9:41000", "workerFleet": "sim"}),
            json!({"protocolVersion": 3}),
        ))?;
        assert_eq!(target.server_name, "sim.fleet.legion");
        Ok(())
    }

    #[test]
    fn plaintext_and_old_workers_are_refused_by_version_test() {
        let old = WorkerTarget::for_node(&node(
            "old",
            json!({"workerAddress": "http://127.0.0.1:4848"}),
            json!({"protocolVersion": 2}),
        ))
        .expect_err("protocol 2 refused");
        assert_eq!(old.code(), tonic::Code::FailedPrecondition);
        assert!(old.message().contains("protocol 2"), "{}", old.message());
        assert!(old.message().contains("upgrade"), "{}", old.message());

        let plaintext = WorkerTarget::for_node(&node(
            "odd",
            json!({"workerAddress": "http://10.0.0.1:4848"}),
            json!({"protocolVersion": 3}),
        ))
        .expect_err("http refused");
        assert_eq!(plaintext.code(), tonic::Code::FailedPrecondition);

        let missing =
            WorkerTarget::for_node(&node("none", json!({}), json!({"protocolVersion": 3})))
                .expect_err("no address");
        assert!(missing.message().contains("worker address"));
    }
}
