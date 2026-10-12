//! Reporting this node's sessions to the manager (docs/session-mode.md).
//!
//! The same epoch protocol as the content advertisement
//! ([`crate::worker::content_report`]): the worker keeps the set the manager
//! last acknowledged and its epoch, and each heartbeat sends the difference.
//! Nothing advances until the manager acknowledges, so a failed heartbeat
//! needs no restore step: the next diff carries what the lost one did. A
//! manager that cannot apply a delta (it just started, it took over the
//! node, or a reply was lost) asks for a full report.
//!
//! A full report also goes out every [`FULL_REPORT_EVERY`] beats, as a
//! backstop against an index that drifted for any reason the epochs cannot
//! see.

use crate::structs::{SessionReport, SessionStatus};
use std::collections::HashMap;

/// Beats between unconditional full reports.
pub const FULL_REPORT_EVERY: u32 = 30;

/// One report in flight, with the set it describes, so it can be
/// acknowledged without recomputing.
#[derive(Debug, Clone)]
pub struct PendingSessionReport {
    pub report: SessionReport,
    snapshot: HashMap<String, SessionStatus>,
}

/// The worker half of the session report's epoch protocol.
#[derive(Debug, Clone)]
pub struct SessionAdvertiser {
    acked: HashMap<String, SessionStatus>,
    acked_epoch: u64,
    needs_full: bool,
    beats_since_full: u32,
}

impl Default for SessionAdvertiser {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdvertiser {
    /// A fresh worker process opens with a full report.
    pub fn new() -> Self {
        Self {
            acked: HashMap::new(),
            acked_epoch: 0,
            needs_full: true,
            beats_since_full: 0,
        }
    }

    /// The report for this heartbeat, given the sessions running now.
    pub fn prepare(&self, current: Vec<SessionStatus>) -> PendingSessionReport {
        let current: HashMap<String, SessionStatus> = current
            .into_iter()
            .map(|status| (status.session_id.clone(), status))
            .collect();
        let next = self.acked_epoch + 1;
        if self.needs_full || self.beats_since_full + 1 >= FULL_REPORT_EVERY {
            let mut upserts: Vec<SessionStatus> = current.values().cloned().collect();
            upserts.sort_by(|a, b| a.session_id.cmp(&b.session_id));
            return PendingSessionReport {
                report: SessionReport {
                    epoch: next,
                    base_epoch: 0,
                    full: true,
                    upserts,
                    removed: Vec::new(),
                },
                snapshot: current,
            };
        }
        let mut upserts: Vec<SessionStatus> = current
            .iter()
            .filter(|(id, status)| self.acked.get(*id) != Some(*status))
            .map(|(_, status)| status.clone())
            .collect();
        upserts.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        let mut removed: Vec<String> = self
            .acked
            .keys()
            .filter(|id| !current.contains_key(*id))
            .cloned()
            .collect();
        removed.sort();
        // Unchanged: the probe that lets a manager with no state notice.
        let epoch = if upserts.is_empty() && removed.is_empty() {
            self.acked_epoch
        } else {
            next
        };
        PendingSessionReport {
            report: SessionReport {
                epoch,
                base_epoch: self.acked_epoch,
                full: false,
                upserts,
                removed,
            },
            snapshot: current,
        }
    }

    /// The manager answered. `resync` means it could not apply the report
    /// and wants a full one next.
    pub fn acknowledge(&mut self, pending: PendingSessionReport, resync: bool) {
        if resync {
            self.needs_full = true;
            return;
        }
        self.beats_since_full = if pending.report.full {
            0
        } else {
            self.beats_since_full + 1
        };
        self.acked = pending.snapshot;
        self.acked_epoch = pending.report.epoch;
        self.needs_full = false;
    }
}

#[cfg(test)]
mod session_report_test {
    use super::*;

    fn status(id: &str, state: &str) -> SessionStatus {
        SessionStatus {
            session_id: id.into(),
            job_id: "agents".into(),
            job_config_id: "agents-1".into(),
            task_name: "workspace".into(),
            envelope_allocation_id: 7,
            state: state.into(),
            lease_issued_at_unix_ms: 1,
            resources: "{}".into(),
        }
    }

    #[test]
    fn opens_full_then_sends_deltas_and_probes_test() {
        let mut advertiser = SessionAdvertiser::new();
        let first = advertiser.prepare(vec![status("a", "starting")]);
        assert!(first.report.full);
        assert_eq!(first.report.epoch, 1);
        advertiser.acknowledge(first, false);

        let probe = advertiser.prepare(vec![status("a", "starting")]);
        assert!(!probe.report.full);
        assert_eq!((probe.report.base_epoch, probe.report.epoch), (1, 1));
        assert!(probe.report.upserts.is_empty() && probe.report.removed.is_empty());
        advertiser.acknowledge(probe, false);

        let delta = advertiser.prepare(vec![status("a", "running"), status("b", "starting")]);
        assert_eq!((delta.report.base_epoch, delta.report.epoch), (1, 2));
        assert_eq!(delta.report.upserts.len(), 2);
        advertiser.acknowledge(delta, false);

        let ended = advertiser.prepare(vec![status("b", "starting")]);
        assert_eq!(ended.report.removed, vec!["a".to_string()]);
    }

    #[test]
    fn a_lost_reply_resends_and_a_resync_goes_full_test() {
        let mut advertiser = SessionAdvertiser::new();
        let first = advertiser.prepare(vec![]);
        advertiser.acknowledge(first, false);
        // Lost: never acknowledged, so the next diff carries the same change.
        let lost = advertiser.prepare(vec![status("a", "running")]);
        assert_eq!(lost.report.upserts.len(), 1);
        let again = advertiser.prepare(vec![status("a", "running")]);
        assert_eq!(again.report.upserts.len(), 1);
        advertiser.acknowledge(again, true);
        assert!(advertiser.prepare(vec![]).report.full);
    }

    #[test]
    fn a_full_report_goes_out_every_thirty_beats_test() {
        let mut advertiser = SessionAdvertiser::new();
        let mut fulls = 0;
        for _ in 0..61 {
            let pending = advertiser.prepare(vec![status("a", "running")]);
            fulls += usize::from(pending.report.full);
            advertiser.acknowledge(pending, false);
        }
        assert_eq!(fulls, 3);
    }
}
