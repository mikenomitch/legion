//! The worker's view of the manager set, and failover between managers.
//!
//! A worker used to dial exactly one manager — the address in its join
//! token — forever. Every worker joined through that manager went offline
//! with it, even though any other manager could have served it (spot-check
//! P0-5). Now:
//!
//! - every registration and heartbeat reply carries the cluster's manager
//!   addresses (`HeartbeatReply.manager_addresses`, from the replicated raft
//!   membership), which the worker learns and persists beside its state
//!   (`managers.json`);
//! - the worker talks to ONE manager at a time (the *active* one) and, after
//!   [`FAILOVER_AFTER_FAILURES`] consecutive transport failures, moves to the
//!   next known address, wrapping around — so it comes back to a recovered
//!   manager the same way it left it;
//! - a restarted worker starts from the persisted set, so it comes up even
//!   when the manager its token names is down.
//!
//! Any manager accepts any worker-plane call: a manager that does not lead
//! the owning raft lane forwards the write to the leader (`ForwardLaneWrite`)
//! or the whole RPC (the leader-forward layer), so which manager a worker
//! talks to is a reachability choice, never a correctness one.
//!
//! The switch is done underneath one tonic [`Channel`]: a balance channel
//! whose endpoint set always holds exactly the active manager. Every
//! [`ManagerClient`](crate::structs::manager_client::ManagerClient) cloned
//! from it — heartbeats, health checks, the identity issuer, edge-boot
//! ratification — follows a failover without being rebuilt.

use crate::prelude::*;
use crate::worker::state::WorkerStateStore;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tower::discover::Change;

/// Consecutive transport failures to the active manager before the worker
/// moves to the next one. Two rather than one so a single dropped request
/// (a manager restarting its listener, a blip) does not bounce a whole fleet
/// across managers; with the 500ms-10s heartbeat retry backoff that is well
/// inside the leader's peer-digest grace (30s) and failover grace (60s).
pub const FAILOVER_AFTER_FAILURES: u32 = 2;

/// How long a dial to a manager may take before it counts as a failure.
/// Without it a manager whose host vanished (rather than refusing the
/// connection) would hang a heartbeat on the OS connect timeout, minutes.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on any one worker-plane RPC. Every call a worker makes to a
/// manager is unary and small; a reply that takes longer than this is a
/// manager in trouble, and failing over is the right response.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP/2 keepalive, so a connection to a manager that disappeared without
/// closing it (host stopped, network partition) is noticed in seconds.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether a failed call means "this manager is unreachable", which is what
/// failover is for. A manager that answered — with any error at all,
/// `Unauthenticated` and not-the-leader included — is reachable, and another
/// manager would answer the same way.
///
/// tonic reports a refused or failed dial as `Unavailable`, and the
/// [`REQUEST_TIMEOUT`] expiring as `Cancelled` ("Timeout expired"); the
/// worker never cancels its own manager calls, so both mean the manager did
/// not answer.
pub fn is_unreachable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

/// Canonical form of a manager address: trimmed, no trailing slash, and an
/// `http://` scheme when none was given.
fn normalize(addr: &str) -> Option<String> {
    let addr = addr.trim().trim_end_matches('/');
    if addr.is_empty() {
        return None;
    }
    Some(if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    })
}

#[derive(Debug)]
struct State {
    /// The address from the join token. Always kept as a last resort: it is
    /// the one address the operator vouched for.
    seed: String,
    /// Every known manager address, in failover order, deduplicated.
    known: Vec<String>,
    /// The manager every call currently goes to.
    active: String,
    consecutive_failures: u32,
    /// Where the known set is persisted, once the worker knows its state
    /// directory.
    store: Option<WorkerStateStore>,
}

/// The worker's manager set. Cheap to share (`Arc`); every method is
/// synchronous and non-blocking.
#[derive(Debug)]
pub struct ManagerEndpoints {
    channel: Channel,
    changes: tokio::sync::mpsc::Sender<Change<String, Endpoint>>,
    state: StdMutex<State>,
}

impl ManagerEndpoints {
    /// Build the set from the token's address plus any addresses a previous
    /// worker process persisted, with the token's address active. Must be
    /// called inside a tokio runtime (the channel spawns its worker task).
    pub fn new(seed: &str, persisted: Vec<String>) -> R<Arc<Self>> {
        let seed = normalize(seed).context("manager address is empty")?;
        let mut known = vec![seed.clone()];
        for addr in persisted.iter().filter_map(|addr| normalize(addr)) {
            if !known.contains(&addr) {
                known.push(addr);
            }
        }
        let (channel, changes) = Channel::balance_channel::<String>(16);
        let endpoints = ManagerEndpoints {
            channel,
            changes,
            state: StdMutex::new(State {
                seed: seed.clone(),
                known,
                active: seed.clone(),
                consecutive_failures: 0,
                store: None,
            }),
        };
        endpoints.insert(&seed)?;
        Ok(Arc::new(endpoints))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn endpoint(addr: &str) -> R<Endpoint> {
        Ok(Endpoint::from_shared(addr.to_string())
            .with_context(|| format!("invalid manager address {addr:?}"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true))
    }

    fn insert(&self, addr: &str) -> R<()> {
        let endpoint = Self::endpoint(addr)?;
        self.changes
            .try_send(Change::Insert(addr.to_string(), endpoint))
            .map_err(|err| anyhow!("failed switching the manager channel to {addr}: {err}"))
    }

    /// The channel every manager client is built from. Follows failovers.
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// The manager calls currently go to.
    pub fn active(&self) -> String {
        self.lock().active.clone()
    }

    /// Every known manager address, in failover order.
    pub fn known(&self) -> Vec<String> {
        self.lock().known.clone()
    }

    /// Start persisting the known set into this worker's state directory,
    /// writing it once now.
    pub fn persist_to(&self, store: WorkerStateStore) {
        let known = {
            let mut state = self.lock();
            state.store = Some(store.clone());
            state.known.clone()
        };
        if let Err(err) = store.save_known_managers(&known) {
            debug!(error = ?err, "Failed to persist the known manager set");
        }
    }

    /// Adopt the manager set a manager reported. An empty report (a manager
    /// outside raft mode, or one that predates the field) changes nothing.
    /// The token's address and the active manager are always kept: the first
    /// is the operator's, the second is answering right now even if the
    /// cluster advertises it under another name.
    pub fn learn(&self, reported: &[String]) {
        if reported.is_empty() {
            return;
        }
        let (changed, known, store) = {
            let mut state = self.lock();
            let mut known: Vec<String> = Vec::new();
            for addr in reported.iter().filter_map(|addr| normalize(addr)) {
                if !known.contains(&addr) {
                    known.push(addr);
                }
            }
            for keep in [state.active.clone(), state.seed.clone()] {
                if !known.contains(&keep) {
                    known.push(keep);
                }
            }
            let changed = known != state.known;
            if changed {
                info!(managers = ?known, "Learned the cluster's manager set");
                state.known = known.clone();
            }
            (changed, known, state.store.clone())
        };
        if changed {
            if let Some(store) = store {
                if let Err(err) = store.save_known_managers(&known) {
                    debug!(error = ?err, "Failed to persist the known manager set");
                }
            }
        }
    }

    /// The active manager answered: reset the failure count.
    pub fn record_success(&self) {
        self.lock().consecutive_failures = 0;
    }

    /// Count one failed call. Returns the `(from, to)` pair when this
    /// failure moved the worker to another manager.
    pub fn record_failure(&self, status: &tonic::Status) -> Option<(String, String)> {
        if !is_unreachable(status) {
            self.record_success();
            return None;
        }
        let failures = {
            let mut state = self.lock();
            state.consecutive_failures += 1;
            state.consecutive_failures
        };
        (failures >= FAILOVER_AFTER_FAILURES)
            .then(|| self.fail_over())
            .flatten()
    }

    /// Move to the next known manager now. `None` when there is no other
    /// manager to move to.
    pub fn fail_over(&self) -> Option<(String, String)> {
        let (from, to) = {
            let mut state = self.lock();
            state.consecutive_failures = 0;
            if state.known.len() < 2 {
                return None;
            }
            let position = state
                .known
                .iter()
                .position(|addr| *addr == state.active)
                .unwrap_or(0);
            let next = state.known[(position + 1) % state.known.len()].clone();
            let from = std::mem::replace(&mut state.active, next.clone());
            (from, next)
        };
        // Insert before remove, so the channel is never without an endpoint
        // (a request in between would otherwise wait for one).
        if let Err(err) = self.insert(&to) {
            warn!(error = ?err, "Could not switch managers");
            let mut state = self.lock();
            state.active = from;
            return None;
        }
        let _ = self.changes.try_send(Change::Remove(from.clone()));
        warn!(
            from = %from,
            to = %to,
            "Manager unreachable; failing over to the next known manager"
        );
        Some((from, to))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unavailable() -> tonic::Status {
        tonic::Status::unavailable("tcp connect error")
    }

    #[tokio::test]
    async fn failover_rotates_through_known_managers_and_wraps() -> R<()> {
        let managers = ManagerEndpoints::new(
            "http://10.0.0.1:4747",
            vec!["10.0.0.2:4747".into(), "http://10.0.0.1:4747/".into()],
        )?;
        assert_eq!(
            managers.known(),
            vec!["http://10.0.0.1:4747", "http://10.0.0.2:4747"],
            "persisted addresses are normalized and deduplicated"
        );
        assert_eq!(managers.active(), "http://10.0.0.1:4747");

        // One failure is a blip; the second moves.
        assert_eq!(managers.record_failure(&unavailable()), None);
        assert_eq!(
            managers.record_failure(&unavailable()),
            Some((
                "http://10.0.0.1:4747".to_string(),
                "http://10.0.0.2:4747".to_string()
            ))
        );
        assert_eq!(managers.active(), "http://10.0.0.2:4747");
        // ...and back again, the same way.
        managers.record_failure(&unavailable());
        managers.record_failure(&unavailable());
        assert_eq!(managers.active(), "http://10.0.0.1:4747");
        Ok(())
    }

    #[tokio::test]
    async fn an_answering_manager_is_never_failed_away_from() -> R<()> {
        let managers =
            ManagerEndpoints::new("http://10.0.0.1:4747", vec!["http://10.0.0.2:4747".into()])?;
        managers.record_failure(&unavailable());
        // The manager answered (with an error): the streak resets.
        managers.record_failure(&tonic::Status::unauthenticated("bad token"));
        managers.record_failure(&tonic::Status::failed_precondition("not the raft leader"));
        managers.record_failure(&unavailable());
        assert_eq!(managers.active(), "http://10.0.0.1:4747");
        Ok(())
    }

    #[tokio::test]
    async fn a_lone_manager_has_nowhere_to_go() -> R<()> {
        let managers = ManagerEndpoints::new("http://10.0.0.1:4747", Vec::new())?;
        assert_eq!(managers.fail_over(), None);
        assert_eq!(managers.active(), "http://10.0.0.1:4747");
        Ok(())
    }

    #[tokio::test]
    async fn learning_keeps_the_seed_and_the_active_manager_and_persists() -> R<()> {
        let dir = tempfile::tempdir()?;
        let store = WorkerStateStore::new(dir.path().join("worker_state.json"));
        let managers = ManagerEndpoints::new("http://public.example:4747", Vec::new())?;
        managers.persist_to(store.clone());
        // Nothing reported: nothing changes.
        managers.learn(&[]);
        assert_eq!(managers.known(), vec!["http://public.example:4747"]);

        managers.learn(&["http://10.0.0.1:4747".into(), "http://10.0.0.2:4747".into()]);
        assert_eq!(
            managers.known(),
            vec![
                "http://10.0.0.1:4747",
                "http://10.0.0.2:4747",
                "http://public.example:4747"
            ]
        );
        assert_eq!(store.load_known_managers(), managers.known());

        // A restarted worker starts from what was persisted.
        let restarted =
            ManagerEndpoints::new("http://public.example:4747", store.load_known_managers())?;
        assert_eq!(restarted.known().len(), 3);
        Ok(())
    }

    /// The channel really follows a failover: a call through a client built
    /// BEFORE the switch reaches the manager that is active AFTER it.
    #[tokio::test]
    async fn clients_follow_the_active_manager() -> R<()> {
        use crate::structs::manager_client::ManagerClient;
        use crate::structs::manager_server::ManagerServer;

        // Manager B: answers WhoAmI. Manager A: an address nothing listens on.
        let store = crate::storage::store::Store::start_storage(
            &crate::structs::config::ManagerConfigFull {
                store_path: tempfile::tempdir()?.keep().display().to_string(),
                ..Default::default()
            },
        )?;
        let service = crate::manager::server::ManagerDaemonService::new(store);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let live = format!("http://{}", listener.local_addr()?);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ManagerServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let dead = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
            format!("http://{}", probe.local_addr()?)
        };

        let managers = ManagerEndpoints::new(&dead, vec![live.clone()])?;
        let mut client = ManagerClient::new(managers.channel());
        let first = client
            .who_am_i(crate::structs::WhoAmIRequest::default())
            .await
            .expect_err("the token's manager is down");
        assert!(is_unreachable(&first), "{first:?}");
        managers.record_failure(&first);
        managers.record_failure(&first);
        assert_eq!(managers.active(), live);
        client
            .who_am_i(crate::structs::WhoAmIRequest::default())
            .await
            .expect("the same client now reaches the live manager");
        Ok(())
    }
}
