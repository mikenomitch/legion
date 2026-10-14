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
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tonic::Status;

/// How long a dial reuses the cluster CA's pin and this manager's material
/// for it before reading the CA row again. A session start dials a worker
/// per call; reading and parsing the CA twice per call was a fifth of the
/// manager's share of a start. A rotated CA is picked up within this long,
/// and the rotation's bundle still trusts the previous CA meanwhile.
const CURRENT_CA_TTL: Duration = Duration::from_secs(1);

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
    /// Per store (several clusters share a test process), the CA pin and
    /// material a dial uses, read at most every [`CURRENT_CA_TTL`].
    current: Mutex<HashMap<usize, CurrentCa>>,
}

/// One store's CA pin and this manager's material for it, as last read.
struct CurrentCa {
    /// The store it was read from: a dropped store's address can be reused
    /// by the next one, which must not inherit this entry.
    store: Weak<crate::storage::session_index::SessionIndex>,
    read_at: Instant,
    pin: String,
    materials: Arc<ManagerMaterials>,
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
            current: Mutex::new(HashMap::new()),
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
        self.current
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
    /// control-lane leader can). A non-leader without a CA returns an error
    /// without proposing anything: its callers retry until the leader's CA
    /// replicates, and a doomed proposal would log an ERROR per attempt.
    pub fn materials(&self, store: &Store) -> R<Arc<ManagerMaterials>> {
        Ok(self.materials_with_pin(store)?.1)
    }

    /// [`Self::materials`], with the pin of the CA they were minted from.
    fn materials_with_pin(&self, store: &Store) -> R<(String, Arc<ManagerMaterials>)> {
        let ca = match store.mesh_ca()? {
            Some(ca) => ca,
            None if !store.is_raft_leader(crate::storage::Lane::Control) => {
                bail!("the cluster CA has not replicated to this manager yet")
            }
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
            return Ok((pin, materials));
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
            .insert(pin.clone(), materials.clone());
        Ok((pin, materials))
    }

    /// The CA pin and material a dial uses, read from the store at most
    /// every [`CURRENT_CA_TTL`].
    fn current_ca(&self, store: &Store) -> R<(String, Arc<ManagerMaterials>)> {
        let index = store.sessions();
        let key = Arc::as_ptr(index) as usize;
        if let Some(current) = self
            .current
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .filter(|current| {
                current.read_at.elapsed() < CURRENT_CA_TTL
                    && current
                        .store
                        .upgrade()
                        .is_some_and(|live| Arc::ptr_eq(&live, index))
            })
        {
            return Ok((current.pin.clone(), current.materials.clone()));
        }
        let (pin, materials) = self.materials_with_pin(store)?;
        self.current
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                key,
                CurrentCa {
                    store: Arc::downgrade(index),
                    read_at: Instant::now(),
                    pin: pin.clone(),
                    materials: materials.clone(),
                },
            );
        Ok((pin, materials))
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
        let (pin, materials) = self.current_ca(store).map_err(|err| {
            error!(error = ?err, "Cluster TLS material unavailable");
            Status::unavailable(format!(
                "this manager has no cluster certificate to dial workers with: {err:#}"
            ))
        })?;
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

/// The metadata key a status carries its stable error code in. The CLI and
/// MCP error envelopes use it as `error.code` instead of the gRPC code name.
pub const ERROR_CODE_METADATA: &str = "legion-error-code";

/// The error code for "a manager could not reach this worker".
pub const WORKER_UNREACHABLE: &str = "WORKER_UNREACHABLE";

/// The node attribute (in its dynamic attributes) a probe's outcome lands
/// in.
pub const WORKER_PLANE_ATTR: &str = "workerPlane";

/// Whether a failed worker-plane call means the worker was never reached
/// (connection refused or timed out, TLS failed) rather than that it
/// answered with an error.
pub fn is_unreachable(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
        || (status.code() == tonic::Code::Unknown && status.message().contains("transport error"))
}

/// The port of a worker address, for the hint (`4848` when it has none).
fn worker_port(address: &str) -> String {
    crate::cluster_tls::host_port(address)
        .ok()
        .and_then(|host_port| host_port.rsplit(':').next().map(str::to_string))
        .unwrap_or_else(|| "4848".to_string())
}

/// The error exec, logs, and restart return when this manager cannot reach
/// a worker (worker-plane-auth plan §6.7): `Unavailable`, tagged
/// [`WORKER_UNREACHABLE`], naming the node, its address, and the cause, with
/// the fix as a hint.
pub fn unreachable_status(node_id: &str, address: &str, cause: &str) -> Status {
    let cause = if cause.trim().is_empty() {
        "connection failed"
    } else {
        cause.trim()
    };
    let mut status = Status::unavailable(format!(
        "cannot reach node {node_id} at {address} ({cause})\nHint: managers dial workers on \
         port {}; see `legion node info {node_id}`",
        worker_port(address)
    ));
    status.metadata_mut().insert(
        ERROR_CODE_METADATA,
        tonic::metadata::MetadataValue::from_static(WORKER_UNREACHABLE),
    );
    status
}

/// [`unreachable_status`] when `status` says the worker was never reached;
/// the worker's own answer otherwise.
pub fn worker_call_error(node_id: &str, address: &str, status: Status) -> Status {
    if is_unreachable(&status) {
        let cause = match std::error::Error::source(&status) {
            Some(source) => format!("{}: {source}", status.message()),
            None => status.message().to_string(),
        };
        unreachable_status(node_id, address, &cause)
    } else {
        status
    }
}

/// Probes one node's worker plane from this manager and returns what lands
/// on the node record: `{reachable, address, from, checkedAt, error?}`. A
/// reply naming another node counts as unreachable: the address is stale.
pub async fn probe_node(store: &Store, node: &Node) -> Value {
    let manager = WorkerPlane::global().manager_id();
    let address = worker_address(node).unwrap_or_default();
    let checked_at = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    let outcome: Result<(), String> = async {
        let mut client = WorkerPlane::global()
            .client(store, node)
            .map_err(|status| status.message().to_string())?;
        let reply = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.probe(tonic::Request::new(crate::structs::ProbeRequest {})),
        )
        .await
        .map_err(|_| "connection timed out".to_string())?
        .map_err(|status| {
            worker_call_error(&node.id, &address, status)
                .message()
                .split("\nHint:")
                .next()
                .unwrap_or_default()
                .to_string()
        })?
        .into_inner();
        let fleet_reply = worker_fleet(node).is_some_and(|fleet| fleet == reply.node_id);
        if reply.node_id != node.id && !fleet_reply {
            return Err(format!(
                "{address} answers as node {:?}, not {:?}; the advertised address is stale",
                reply.node_id, node.id
            ));
        }
        Ok(())
    }
    .await;
    let mut record = serde_json::json!({
        "reachable": outcome.is_ok(),
        "address": address,
        "from": manager,
        "checkedAt": checked_at,
    });
    if let Err(error) = outcome {
        record["error"] = Value::String(error);
    }
    record
}

/// After a registration: waits for the worker's plane to come up (it starts
/// once the node certificate is issued, a moment after registering), probes
/// it, retrying a few times, and records the outcome on the node.
pub fn spawn_probe_after_registration(store: Store, node_id: String) {
    tokio::spawn(async move {
        let mut record = Value::Null;
        for delay in [2u64, 5, 10, 20] {
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            let Ok(node) = store.get_node(&node_id) else {
                return;
            };
            record = probe_node(&store, &node).await;
            if record["reachable"] == true {
                break;
            }
        }
        if record["reachable"] != true {
            warn!(
                node_id = %node_id,
                address = %record["address"].as_str().unwrap_or_default(),
                error = %record["error"].as_str().unwrap_or_default(),
                "This manager cannot reach the worker plane; exec, logs, and restart will fail \
                 for this node (see `legion node info`)"
            );
        }
        let mut diff = serde_json::Map::new();
        diff.insert(WORKER_PLANE_ATTR.to_string(), record);
        if let Err(err) = store.update_node_dyamic(&node_id, &diff) {
            debug!(node_id = %node_id, error = ?err, "Could not record the worker plane probe");
        }
    });
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

    /// A manager that may not write the control lane (a raft follower, here
    /// a streaming standby) waits for the leader's CA instead of proposing
    /// one: each doomed proposal used to log an ERROR at startup.
    #[test]
    fn a_non_leader_without_a_ca_waits_instead_of_creating_one_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let mut config = crate::structs::ManagerConfigFull {
            store_path: dir.path().display().to_string(),
            ..Default::default()
        };
        config.persistence.r#type = crate::structs::PersistenceType::Streaming;
        config.persistence.streaming.lease.provider =
            crate::structs::StreamingLeaseProvider::ObjectStorage;
        let store = Store::start_storage(&config)?;

        let err = WorkerPlane::global()
            .materials(&store)
            .expect_err("no CA on a non-leader");
        assert!(format!("{err:#}").contains("not replicated"), "{err:#}");
        assert!(store.mesh_ca()?.is_none());
        Ok(())
    }
}
