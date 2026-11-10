//! The manager's telemetry listener, over the process metrics registry in
//! [`crate::telemetry`] (re-exported here, so `manager::telemetry::metrics()`
//! and friends keep working for manager code).
//!
//! The listener (same hyper http/1.1 pattern as `exec_ws`) serves:
//! - `GET /metrics`  — Prometheus text format (`text/plain; version=0.0.4`)
//! - `GET /healthz`  — liveness: `200 ok` whenever the listener is serving
//! - `GET /readyz`   — readiness: `200` only when a store read probe
//!   (`SELECT 1`) succeeds and, under raft persistence, a leader is known;
//!   otherwise `503` with the reason in the body.
//!
//! By convention the listener binds `server.port + 2` (the exec WebSocket
//! holds `+ 1`); `telemetry.port` overrides it. See docs/metrics.md.

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use time::OffsetDateTime;

use crate::prelude::*;
use crate::storage::store::Store;

pub use crate::telemetry::*;

// ---------------------------------------------------------------------------
// HTTP listener (/metrics, /healthz, /readyz)
// ---------------------------------------------------------------------------

/// Shared state for the telemetry listener.
#[derive(Clone)]
pub struct TelemetryState {
    pub store: Store,
    /// True when persistence runs in raft mode: readiness then additionally
    /// requires `raft_status()` to report a known leader.
    pub raft_required: bool,
    /// When this node first observed the current lane-leadership divergence
    /// (unix ms), or `0` while converged. Owned by the readiness probe: the
    /// divergence grace window is measured from this timestamp, so it is
    /// "how long has THIS split stood continuously" — immune to an
    /// unrelated lane flapping its leader (which used to refresh the old
    /// `election_epoch_ms`-based grace forever and mask a standing split as
    /// ready), and it grants a full grace window after a manager restart
    /// (where `election_epoch_ms` started at 0 and denied any grace).
    pub diverged_since_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

/// Start the telemetry listener on its own port (`server.port + 2` by
/// convention; `telemetry.port` overrides) and serve it in the background
/// until the process exits.
///
/// The port is bound before this returns, so a port conflict fails the
/// manager's startup instead of leaving a manager up with no `/metrics` and
/// no health probes, which an orchestrator reads as a dead manager.
pub async fn start_telemetry(addr: SocketAddr, state: TelemetryState) -> R<()> {
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| {
        format!(
            "failed to bind the telemetry listener on {addr}; free the port, set \
             telemetry.port, or set telemetry.enabled = false"
        )
    })?;
    info!(%addr, "Telemetry listener started (/metrics, /healthz, /readyz)");
    tokio::spawn(async move {
        if let Err(err) = serve_telemetry_on(listener, state).await {
            tracing::error!(error = ?err, "telemetry listener exited");
        }
    });
    Ok(())
}

/// Serve telemetry on an already-bound listener (tests bind port 0 and read
/// back the local address before calling this).
pub async fn serve_telemetry_on(listener: tokio::net::TcpListener, state: TelemetryState) -> R<()> {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = ?err, "telemetry accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| handle(req, state.clone()));
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                debug!(error = ?err, "telemetry connection closed");
            }
        });
    }
}

async fn handle(
    req: Request<Incoming>,
    state: TelemetryState,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() != hyper::Method::GET {
        return Ok(text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "method not allowed\n".into(),
        ));
    }
    let resp = match req.uri().path() {
        "/metrics" => text_response(StatusCode::OK, METRICS_CONTENT_TYPE, render()),
        "/healthz" => text_response(StatusCode::OK, "text/plain", "ok\n".into()),
        "/readyz" => match readiness(&state) {
            Ok(notes) => {
                // Advisory notes (e.g. even raft voter parity) do NOT break
                // readiness — the manager serves fine today; the note names
                // the availability risk and the fix (plan §7-C3, JC-29).
                let mut body = String::from("ok\n");
                for note in notes {
                    body.push_str(&format!("note: {note}\n"));
                }
                text_response(StatusCode::OK, "text/plain", body)
            }
            Err(reason) => {
                debug!(%reason, "readiness probe failing");
                text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "text/plain",
                    format!("not ready: {reason}\n"),
                )
            }
        },
        _ => text_response(StatusCode::NOT_FOUND, "text/plain", "not found\n".into()),
    };
    Ok(resp)
}

/// Readiness: the store must answer a trivial read (`SELECT 1` on a read
/// connection) and, under raft persistence, a leader must be known.
///
/// `Ok` carries advisory note lines that do NOT affect the 200 status: when
/// a leader is known but a lane's voter count is unhealthy (even parity, or
/// below `raft.expectedVoters`), the lane's plain-language advisory is
/// surfaced in the body — readiness is about serving traffic today, and an
/// even cluster serves fine; it just survives fewer failures than it could.
fn readiness(state: &TelemetryState) -> Result<Vec<String>, String> {
    let probe = state.store.read_connection().and_then(|conn| {
        conn.query_row("SELECT 1;", [], |row| row.get::<_, i64>(0))
            .map_err(anyhow::Error::from)
    });
    match probe {
        Ok(1) => {}
        Ok(other) => return Err(format!("store probe returned unexpected value {other}")),
        Err(err) => return Err(format!("store probe failed: {err}")),
    }
    let mut notes = Vec::new();
    if state.raft_required {
        match state.store.raft_status() {
            Some(status) => {
                let now_ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
                let diverged_for_ms =
                    observe_lane_divergence(&state.diverged_since_ms, &status, now_ms);
                if let Some(reason) = raft_readiness_failure(&status, diverged_for_ms) {
                    return Err(with_lane_detail(reason, &status));
                }
                // Per-lane detail in the body on the healthy path too: an
                // operator debugging a lanes cluster should not have to
                // reach for `legion raft status` to see which node leads
                // which lane (JC-40).
                notes.extend(lane_detail_lines(&status));
                for lane in &status.lanes {
                    if let Some(warning) = &lane.warning {
                        let note = if status.lanes.len() > 1 {
                            format!("lane {}: {warning}", lane.lane)
                        } else {
                            warning.clone()
                        };
                        if !notes.contains(&note) {
                            notes.push(note);
                        }
                    }
                }
            }
            None => return Err("raft status unavailable".to_string()),
        }
    }
    Ok(notes)
}

/// How long per-lane leaders may DISAGREE before this manager reports
/// itself not ready (JC-40). Elections are independent per lane, so a
/// failover legitimately leaves the lanes split for a moment while the
/// anchor leader's convergence loop reacquires them; that window must stay
/// 200 or every failover would flap every load balancer. Beyond it, the
/// split is standing, which is an abnormal leadership shape an operator
/// should look at — even though the write path now degrades rather than
/// dies (plan §8: submits are accepted by the control leader and completed
/// by lane reconcilers, worker liveness is forwarded to the status leader).
///
/// Note that convergence is BEST EFFORT (JC-40 correction, 2026-07-28): the
/// anchor leader's campaign is a race raft's election safety can refuse
/// forever, so this probe can legitimately stay `503` indefinitely. See
/// `docs/multi-raft.md`, "Lane leadership: co-located, best effort".
const LANE_LEADER_DIVERGENCE_GRACE_MS: i64 = 10_000;

/// Track how long the current lane-leadership divergence has stood, from
/// this probe's own observations. Returns `Some(ms)` while diverged, `None`
/// while converged (resetting the tracker).
///
/// "Diverged" is any not-fully-converged multi-lane shape: lanes led by
/// different managers, or some lane leaderless while others are led. Both
/// keep the timer running so a lane that flaps its leader (through elected
/// and leaderless states) cannot re-grant the grace window to a standing
/// split of the OTHER lanes — the defect in the previous
/// `election_epoch_ms`-based grace, which any leader change refreshed. Only
/// genuine convergence resets the clock. A restarted manager starts
/// converged (tracker 0), so its first observed divergence gets the full
/// grace window — the old scheme started at "no grace" after every restart
/// and failed rolling restarts straight into 503.
fn observe_lane_divergence(
    diverged_since_ms: &std::sync::atomic::AtomicI64,
    status: &crate::storage::persistence::RaftStatus,
    now_ms: i64,
) -> Option<i64> {
    use std::sync::atomic::Ordering;
    let converged = status.lanes.len() < 2 || {
        let mut leaders = std::collections::BTreeSet::new();
        let mut all_led = true;
        for lane in &status.lanes {
            match lane.leader_id.as_deref() {
                Some(leader) => {
                    leaders.insert(leader);
                }
                None => all_led = false,
            }
        }
        all_led && leaders.len() == 1
    };
    if converged {
        diverged_since_ms.store(0, Ordering::Relaxed);
        return None;
    }
    let mut since = diverged_since_ms.load(Ordering::Relaxed);
    if since == 0 {
        // First observation of this divergence: the clock starts now. On a
        // race between two probes the earlier store wins; either way the
        // value is within one probe interval of the truth.
        let _ = diverged_since_ms.compare_exchange(0, now_ms, Ordering::Relaxed, Ordering::Relaxed);
        since = diverged_since_ms.load(Ordering::Relaxed);
    }
    Some(now_ms.saturating_sub(since))
}

/// Why raft state makes this manager NOT ready, if anything.
///
/// Three failure modes, all lanes-aware:
///
/// 1. no leader at all;
/// 2. some lane has no leader — the aggregate `leader_id` becomes a
///    `control=…,scheduler=-,…` summary string whenever per-lane leaders
///    differ, INCLUDING a lane with no leader, so gating on
///    `leader_id.is_some()` alone reported 200 while, say, the scheduler
///    lane's quorum (and with it all placement) was down (adversarial
///    finding, 2026-07-24);
/// 3. every lane has a leader but they are DIFFERENT nodes, and have been
///    for longer than [`LANE_LEADER_DIVERGENCE_GRACE_MS`] (JC-40).
///
/// `diverged_for_ms` comes from [`observe_lane_divergence`]: how long the
/// current divergence has stood continuously, by this probe's own clock.
fn raft_readiness_failure(
    status: &crate::storage::persistence::RaftStatus,
    diverged_for_ms: Option<i64>,
) -> Option<String> {
    if status.leader_id.is_none() {
        return Some("raft leader unknown".to_string());
    }
    let leaderless: Vec<&str> = status
        .lanes
        .iter()
        .filter(|lane| lane.leader_id.is_none())
        .map(|lane| lane.lane.as_str())
        .collect();
    if !leaderless.is_empty() {
        return Some(format!(
            "raft leader unknown for lane(s): {}",
            leaderless.join(", ")
        ));
    }
    let distinct: std::collections::BTreeSet<&str> = status
        .lanes
        .iter()
        .filter_map(|lane| lane.leader_id.as_deref())
        .collect();
    if status.lanes.len() > 1 && distinct.len() > 1 {
        let split_for_ms = diverged_for_ms.unwrap_or(0);
        if split_for_ms < LANE_LEADER_DIVERGENCE_GRACE_MS {
            // Brief post-election convergence: still ready.
            return None;
        }
        return Some(format!(
            "raft lane leadership has not converged for {}s: lanes are led by different \
             managers ({}). Single-lane operations and reads are unaffected; job submits \
             are accepted by the control leader and completed by lane reconcilers; worker \
             liveness is forwarded — but scheduling latency is degraded and state backup \
             is unavailable until leadership converges. Check `legion raft status` and \
             restart the stray lane's leader if it is unhealthy",
            split_for_ms.max(0) / 1000,
            lane_leader_summary(status)
        ));
    }
    None
}

/// `control=mgr-a,scheduler=mgr-b,status=-` — the same shape `raft status`
/// uses for its aggregate leader column when lanes disagree.
fn lane_leader_summary(status: &crate::storage::persistence::RaftStatus) -> String {
    status
        .lanes
        .iter()
        .map(|lane| format!("{}={}", lane.lane, lane.leader_id.as_deref().unwrap_or("-")))
        .collect::<Vec<_>>()
        .join(",")
}

/// One body line per lane (multi-lane topologies only): who leads it and
/// what this node's role on it is.
fn lane_detail_lines(status: &crate::storage::persistence::RaftStatus) -> Vec<String> {
    if status.lanes.len() < 2 {
        return Vec::new();
    }
    status
        .lanes
        .iter()
        .map(|lane| {
            format!(
                "lane {}: leader {} (this node: {})",
                lane.lane,
                lane.leader_id.as_deref().unwrap_or("-"),
                lane.role.as_str()
            )
        })
        .collect()
}

/// Appends the per-lane detail lines to a readiness failure reason so the
/// non-200 body names which lane is wrong, not just that something is.
fn with_lane_detail(reason: String, status: &crate::storage::persistence::RaftStatus) -> String {
    let detail = lane_detail_lines(status);
    if detail.is_empty() {
        return reason;
    }
    format!("{reason}\n{}", detail.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Adversarial regression (readyz vs. partial lane leadership): a lane
    /// with NO leader must fail readiness even while the aggregate
    /// `leader_id` carries the non-empty split summary string.
    #[test]
    fn readiness_requires_a_leader_on_every_lane() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        const NOW_MS: i64 = 1_000_000;
        let lane = |name: &str, leader: Option<&str>| RaftLaneStatus {
            lane: name.to_string(),
            role: PersistenceRole::Standby,
            leader_id: leader.map(str::to_string),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            voters: 3,
            quorum: 2,
            survives: 1,
            warning: None,
        };
        let mut status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("control=mgr-a,scheduler=-,status=mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![
                lane("control", Some("mgr-a")),
                lane("scheduler", None),
                lane("status", Some("mgr-a")),
            ],
        };
        let reason =
            raft_readiness_failure(&status, Some(NOW_MS)).expect("a leaderless lane must fail");
        assert!(reason.contains("scheduler"), "{reason}");

        // Every lane led by the SAME node (JC-40 steady state): ready.
        status.lanes[1].leader_id = Some("mgr-a".into());
        status.leader_id = Some("mgr-a".into());
        assert_eq!(raft_readiness_failure(&status, None), None);

        status.leader_id = None;
        assert!(raft_readiness_failure(&status, None).is_some());
    }

    /// JC-40: lanes led by DIFFERENT managers are ready during the brief
    /// post-election convergence window and NOT ready once the split has
    /// stood still past the grace period — the aggregate leader summary is
    /// non-empty in both states, so it must not be what readiness reads.
    #[test]
    fn readiness_fails_when_lane_leadership_stays_diverged() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        let lane = |name: &str, leader: &str| RaftLaneStatus {
            lane: name.to_string(),
            role: PersistenceRole::Standby,
            leader_id: Some(leader.to_string()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            voters: 3,
            quorum: 2,
            survives: 1,
            warning: None,
        };
        let mut status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("control=mgr-a,scheduler=mgr-b,status=mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![
                lane("control", "mgr-a"),
                lane("scheduler", "mgr-b"),
                lane("status", "mgr-a"),
            ],
        };
        // A split observed one second ago: convergence is still in flight.
        assert_eq!(
            raft_readiness_failure(&status, Some(1_000)),
            None,
            "a split within the grace window must stay ready"
        );

        // The same split, standing continuously past the grace window.
        let reason = raft_readiness_failure(&status, Some(LANE_LEADER_DIVERGENCE_GRACE_MS + 5_000))
            .expect("a persistent lane-leadership split must fail readiness");
        assert!(
            reason.contains("scheduler=mgr-b") && reason.contains("control=mgr-a"),
            "the reason must name the per-lane leaders: {reason}"
        );

        // Converged onto one manager: ready again, regardless of age.
        status.lanes[1].leader_id = Some("mgr-a".into());
        assert_eq!(raft_readiness_failure(&status, None), None);

        // Per-lane detail rides the body in both directions.
        let detail = lane_detail_lines(&status);
        assert_eq!(detail.len(), 3);
        assert!(
            detail[0].contains("lane control: leader mgr-a"),
            "{detail:?}"
        );
    }

    /// The divergence tracker measures how long the CURRENT divergence has
    /// stood continuously, by this probe's own observations:
    ///
    /// - a fresh (restarted) manager grants the full grace window to the
    ///   first divergence it sees (the old `election_epoch_ms` scheme
    ///   started at "no grace" and failed rolling restarts straight into
    ///   503);
    /// - an unrelated lane flapping its leader — including through
    ///   leaderless moments — must NOT reset the clock on a standing split
    ///   (the old scheme refreshed the grace on any leader change, masking
    ///   the split forever);
    /// - genuine convergence resets it.
    #[test]
    fn divergence_tracker_measures_continuous_divergence() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        use std::sync::atomic::AtomicI64;
        let lane = |name: &str, leader: Option<&str>| RaftLaneStatus {
            lane: name.to_string(),
            role: PersistenceRole::Standby,
            leader_id: leader.map(str::to_string),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            voters: 3,
            quorum: 2,
            survives: 1,
            warning: None,
        };
        let mut status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("control=mgr-a,scheduler=mgr-b,status=mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![
                lane("control", Some("mgr-a")),
                lane("scheduler", Some("mgr-b")),
                lane("status", Some("mgr-a")),
            ],
        };
        let tracker = AtomicI64::new(0);
        // Restart-fresh tracker: the first observation starts the clock at 0.
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 100_000),
            Some(0),
            "the first observed divergence gets the full grace window"
        );
        // The split stands: the clock runs.
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 108_000),
            Some(8_000)
        );
        // The status lane flaps through a leaderless moment: still diverged,
        // clock keeps running.
        status.lanes[2].leader_id = None;
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 111_000),
            Some(11_000)
        );
        // It re-elects a DIFFERENT leader: still diverged, clock still runs.
        status.lanes[2].leader_id = Some("mgr-c".into());
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 115_000),
            Some(15_000)
        );
        // Genuine convergence resets.
        status.lanes[1].leader_id = Some("mgr-a".into());
        status.lanes[2].leader_id = Some("mgr-a".into());
        assert_eq!(observe_lane_divergence(&tracker, &status, 120_000), None);
        // A later, new split starts a fresh clock.
        status.lanes[1].leader_id = Some("mgr-b".into());
        assert_eq!(observe_lane_divergence(&tracker, &status, 130_000), Some(0));
    }

    /// `single` topology reports one lane; a co-location check must never
    /// fire there (there is nothing to diverge from).
    #[test]
    fn readiness_ignores_lane_divergence_in_single_topology() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        let status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![RaftLaneStatus {
                lane: "control".into(),
                role: PersistenceRole::Primary,
                leader_id: Some("mgr-a".into()),
                term: 1,
                commit_index: 0,
                applied_index: 0,
                voters: 3,
                quorum: 2,
                survives: 1,
                warning: None,
            }],
        };
        assert_eq!(raft_readiness_failure(&status, None), None);
        assert!(lane_detail_lines(&status).is_empty());
    }
}
