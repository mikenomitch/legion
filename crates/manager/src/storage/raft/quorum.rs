//! Quorum arithmetic + the plain-language voter-count advisories (phase C-4
//! of `docs/plans/durability-and-ops-execution-plan.md` §7-C3, JC-29).
//!
//! Every surface that talks about voter counts — config validation,
//! `legion raft status`, membership commands, join promotion, `/readyz` —
//! builds its wording HERE, so the explanation an operator sees is identical
//! everywhere: what the count means for commit availability, and the exact
//! fix. Per JC-29 the advisories warn loudly and block rarely: the only hard
//! refusals are removals that would leave zero voters or a quorum the
//! currently-known-alive members cannot form (see [`removal_guard`]).

use std::sync::Once;

use crate::prelude::*;
use crate::structs::RaftPersistenceConfigFull;

/// Majority size for `voters` voting members: the number that must be alive
/// (and reachable) for the group to commit. `0` for an empty voter set.
pub(crate) fn quorum_size(voters: u64) -> u64 {
    if voters == 0 {
        0
    } else {
        voters / 2 + 1
    }
}

/// Voter failures the group survives while still committing:
/// `voters - quorum_size(voters)`, i.e. `(voters - 1) / 2`.
pub(crate) fn failures_survivable(voters: u64) -> u64 {
    voters.saturating_sub(quorum_size(voters))
}

/// Whether `voters` is an even (parity-wasteful) voter count. `0` is not a
/// parity problem — it is "no cluster", handled by the removal guard.
pub(crate) fn is_even_parity(voters: u64) -> bool {
    voters >= 2 && voters.is_multiple_of(2)
}

/// The plain-language advisory for an even voter count — the single source
/// of truth for the wording used by config validation, status, membership
/// commands, metrics docs, and readyz. `None` when the count is fine (odd,
/// or fewer than two voters).
pub(crate) fn parity_advisory(voters: u64) -> Option<String> {
    if !is_even_parity(voters) {
        return None;
    }
    let quorum = quorum_size(voters);
    let survives = failures_survivable(voters);
    Some(if voters == 2 {
        "2 voters need both alive to commit — this cluster survives zero failures. \
         Use 1, 3, or 5 voters; add one more manager or remove one."
            .to_string()
    } else {
        format!(
            "{voters} voters need {quorum} alive to commit — this cluster survives \
             {survives} failure(s), no more than {fewer} voters would. Use an odd voter \
             count (1, 3, or 5); add one more manager or remove one.",
            fewer = voters - 1
        )
    })
}

/// The plain-language advisory for a lane whose membership change is still
/// JOINT — openraft's two-step change has appended the joint configuration
/// but has not yet committed the uniform one.
///
/// This matters because a joint configuration needs a quorum of **each**
/// config to commit or elect: growing `{a,b}` to `{a,b,c}` leaves a lane
/// that still needs BOTH `a` and `b`, even though its voter list already
/// reads as three. Losing `a` there costs the lane its write path
/// permanently, so the state is worth naming on every status surface until
/// it clears.
pub(crate) fn joint_membership_advisory(configs: &[Vec<String>]) -> Option<String> {
    if configs.len() < 2 {
        return None;
    }
    let rendered: Vec<String> = configs
        .iter()
        .map(|config| format!("[{}]", config.join(", ")))
        .collect();
    Some(format!(
        "membership change in flight: this lane is in a JOINT configuration {} and needs a \
         majority of EVERY listed config to commit or elect — it is less fault-tolerant than \
         its voter count suggests until the change settles. Re-issue the join (membership \
         changes are idempotent) if it does not clear.",
        rendered.join(" + ")
    ))
}

/// Advisory for a voter count below the operator's declared
/// `raft.expectedVoters`. `None` when no expectation is set or it is met.
pub(crate) fn below_expected_advisory(voters: u64, expected: Option<u32>) -> Option<String> {
    let expected = expected?;
    (voters < u64::from(expected)).then(|| {
        format!(
            "{voters} voter(s) present but raft.expectedVoters is {expected} — managers \
             may be down, not yet joined, or still learners. Check `legion raft status` \
             on each manager and re-issue a join token or promote as needed."
        )
    })
}

/// The combined status-surface warning for a lane's current voter count:
/// even parity and/or below `raft.expectedVoters`. `None` when healthy.
pub(crate) fn voter_count_warning(voters: u64, expected: Option<u32>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(parity) = parity_advisory(voters) {
        parts.push(parity);
    }
    if let Some(below) = below_expected_advisory(voters, expected) {
        parts.push(below);
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

/// The refusal advisory for removing the last voter.
pub(crate) fn removal_would_zero_advisory(target: &str) -> String {
    format!(
        "refusing to remove {target}: it is the last raft voter, and a cluster with zero \
         voters can never commit anything again (quorum of 0 does not exist). Add another \
         voter first (`legion raft command add-voter --target name@host:port`), or retire \
         the cluster instead of emptying it."
    )
}

/// The refusal advisory for a removal that would leave a quorum the
/// currently-known-alive members cannot form.
pub(crate) fn removal_infeasible_advisory(
    target: &str,
    resulting_voters: u64,
    alive_resulting: u64,
) -> String {
    format!(
        "refusing to remove {target}: the remaining {resulting_voters} voter(s) need \
         {quorum} alive to commit, but only {alive_resulting} of them currently appear \
         alive from this leader — the cluster would be unable to commit (including any \
         further membership fix). Recover the dead manager(s) or remove a dead member \
         first.",
        quorum = quorum_size(resulting_voters)
    )
}

/// Hard block for `remove-member` (JC-29: the ONLY hard block): refuse when
/// the removal would leave zero voters, or when the resulting quorum could
/// not be formed by the members currently known to be alive. Warn-level
/// parity is NOT refused here — that is [`parity_advisory`]'s job.
///
/// `resulting_voters` is the voter count AFTER the removal;
/// `alive_resulting` is how many of those resulting voters are currently
/// known alive (best-effort: the leader itself plus followers with an acked
/// replication stream).
pub(crate) fn removal_guard(
    target: &str,
    resulting_voters: u64,
    alive_resulting: u64,
) -> Result<(), String> {
    if resulting_voters == 0 {
        return Err(removal_would_zero_advisory(target));
    }
    if alive_resulting < quorum_size(resulting_voters) {
        return Err(removal_infeasible_advisory(
            target,
            resulting_voters,
            alive_resulting,
        ));
    }
    Ok(())
}

/// Guards the per-process once-ness of the config-load voter warnings
/// (production loads config once; tests re-enter `start_storage` freely and
/// must not spam the log).
static CONFIG_VOTER_WARNINGS: Once = Once::new();

/// How many heartbeat intervals must fit inside the SHORTEST election
/// timeout before the arrangement is considered stable.
///
/// openraft itself only rejects `election_timeout_min <= heartbeat_interval`
/// — a single missed heartbeat away from a campaign, which flaps under any
/// real load. Requiring the minimum to cover at least this many heartbeats
/// means a follower tolerates a couple of merely slow or dropped heartbeats
/// before concluding the leader is gone. The shipped defaults sit at 2.4x
/// (250 / 600), comfortably clear of the floor.
const MIN_HEARTBEATS_PER_ELECTION_TIMEOUT: u64 = 2;

/// Config-load validation for the raft knobs (plan §7-C3):
/// `expectedVoters = 0` is a hard error; the election-timing knobs must
/// describe a stable arrangement (see
/// [`MIN_HEARTBEATS_PER_ELECTION_TIMEOUT`] and `min < max`); an even
/// `expectedVoters`, or a `peers` list describing an even cluster, logs the
/// parity advisory once per process and proceeds.
pub(crate) fn validate_raft_config(cfg: &RaftPersistenceConfigFull) -> R<()> {
    validate_election_timing(cfg)?;
    if cfg.expected_voters == Some(0) {
        bail!(
            "persistence.raft.expectedVoters must be at least 1 — a raft cluster needs at \
             least one voter to commit anything. Leave it unset to skip the expected-size \
             check, or set it to the number of managers you run (1, 3, or 5)."
        );
    }
    let mut warnings: Vec<String> = Vec::new();
    if let Some(expected) = cfg.expected_voters {
        if let Some(advisory) = parity_advisory(u64::from(expected)) {
            warnings.push(format!(
                "persistence.raft.expectedVoters = {expected} is an even voter count: {advisory}"
            ));
        }
    }
    // The `peers` convention lists EVERY manager of the cluster (self
    // included), so an even list length describes an even cluster.
    let peer_count = cfg.peers.len() as u64;
    if let Some(advisory) = parity_advisory(peer_count) {
        warnings.push(format!(
            "persistence.raft.peers lists {peer_count} managers — an even cluster: {advisory}"
        ));
    }
    if !warnings.is_empty() {
        CONFIG_VOTER_WARNINGS.call_once(|| {
            for warning in &warnings {
                warn!("{warning}");
            }
        });
    }
    Ok(())
}

/// Rejects election-timing knobs that would make the cluster flap.
///
/// Two relationships have to hold, and both are cheap to get wrong by hand:
/// the election minimum must cover several heartbeats (otherwise one slow
/// round trip unseats a healthy leader), and the randomized election window
/// must be non-empty (otherwise every follower campaigns at the same
/// instant and splits the vote). openraft's own `Config::validate` catches
/// only the degenerate forms of these at engine start; catching them at
/// config load lets the message name the offending values and say what the
/// safe relationship is.
fn validate_election_timing(cfg: &RaftPersistenceConfigFull) -> R<()> {
    let heartbeat = cfg.heartbeat_interval_ms;
    let min = cfg.election_timeout_min_ms;
    let max = cfg.election_timeout_max_ms;

    if heartbeat == 0 {
        bail!(
            "persistence.raft.heartbeatIntervalMs must be greater than 0 — a leader that never \
             heartbeats is indistinguishable from a dead one."
        );
    }
    if heartbeat.saturating_mul(MIN_HEARTBEATS_PER_ELECTION_TIMEOUT) > min {
        let safe = heartbeat.saturating_mul(MIN_HEARTBEATS_PER_ELECTION_TIMEOUT);
        bail!(
            "persistence.raft.electionTimeoutMinMs = {min} is too small for \
             heartbeatIntervalMs = {heartbeat}: a follower would start an election after \
             fewer than {MIN_HEARTBEATS_PER_ELECTION_TIMEOUT} missed heartbeats, so ordinary \
             load makes the cluster flap between leaders instead of committing. Require \
             electionTimeoutMinMs >= {MIN_HEARTBEATS_PER_ELECTION_TIMEOUT} * \
             heartbeatIntervalMs (>= {safe} here) — either raise electionTimeoutMinMs to at \
             least {safe}, or lower heartbeatIntervalMs to at most \
             {}.",
            min / MIN_HEARTBEATS_PER_ELECTION_TIMEOUT
        );
    }
    if min >= max {
        bail!(
            "persistence.raft.electionTimeoutMinMs = {min} must be strictly less than \
             electionTimeoutMaxMs = {max}. The election timeout is drawn at random from \
             [min, max) so that candidates wake at different times and one wins outright; an \
             empty window makes every follower campaign at the same instant and split the \
             vote. Leave a real gap — the shipped defaults use max = 2 * min."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full 1..=7 voter table: quorum, failures survivable, parity.
    #[test]
    fn quorum_table_one_through_seven() {
        // (voters, quorum, survives, even_parity)
        let table = [
            (1, 1, 0, false),
            (2, 2, 0, true),
            (3, 2, 1, false),
            (4, 3, 1, true),
            (5, 3, 2, false),
            (6, 4, 2, true),
            (7, 4, 3, false),
        ];
        for (voters, quorum, survives, even) in table {
            assert_eq!(quorum_size(voters), quorum, "quorum for {voters} voters");
            assert_eq!(
                failures_survivable(voters),
                survives,
                "survivable failures for {voters} voters"
            );
            assert_eq!(is_even_parity(voters), even, "parity for {voters} voters");
            assert_eq!(
                parity_advisory(voters).is_some(),
                even,
                "advisory presence for {voters} voters"
            );
        }
        // Degenerate zero: no quorum exists, no parity advisory (the removal
        // guard owns the zero case).
        assert_eq!(quorum_size(0), 0);
        assert_eq!(failures_survivable(0), 0);
        assert!(parity_advisory(0).is_none());
    }

    #[test]
    fn parity_advisory_wording_names_the_fix() {
        let two = parity_advisory(2).expect("2 voters must warn");
        assert!(two.contains("need both alive to commit"), "{two}");
        assert!(two.contains("survives zero failures"), "{two}");
        assert!(two.contains("Use 1, 3, or 5 voters"), "{two}");
        assert!(two.contains("add one more manager or remove one"), "{two}");

        let four = parity_advisory(4).expect("4 voters must warn");
        assert!(four.contains("4 voters need 3 alive"), "{four}");
        assert!(four.contains("survives 1 failure(s)"), "{four}");
        assert!(four.contains("no more than 3 voters would"), "{four}");
        assert!(
            four.contains("add one more manager or remove one"),
            "{four}"
        );

        assert!(parity_advisory(1).is_none());
        assert!(parity_advisory(3).is_none());
        assert!(parity_advisory(5).is_none());
    }

    #[test]
    fn below_expected_advisory_fires_only_under_expectation() {
        assert!(below_expected_advisory(3, None).is_none());
        assert!(below_expected_advisory(3, Some(3)).is_none());
        assert!(below_expected_advisory(4, Some(3)).is_none());
        let short = below_expected_advisory(1, Some(3)).expect("1 of 3 must warn");
        assert!(short.contains("raft.expectedVoters is 3"), "{short}");
        assert!(short.contains("1 voter(s) present"), "{short}");
    }

    #[test]
    fn voter_count_warning_combines_parity_and_expectation() {
        assert!(voter_count_warning(3, Some(3)).is_none());
        assert!(voter_count_warning(1, None).is_none());
        // Even AND below expectation: both advisories, in one string.
        let both = voter_count_warning(2, Some(3)).expect("2 of 3 must warn");
        assert!(both.contains("need both alive to commit"), "{both}");
        assert!(both.contains("raft.expectedVoters is 3"), "{both}");
        // Odd but below expectation: only the expectation half.
        let short = voter_count_warning(1, Some(3)).expect("1 of 3 must warn");
        assert!(!short.contains("alive to commit"), "{short}");
        assert!(short.contains("raft.expectedVoters is 3"), "{short}");
    }

    #[test]
    fn removal_guard_refuses_zero_and_infeasible_quorum() {
        // Removing the last voter: refused with the zero-voters advisory.
        let zero = removal_guard("mgr-a", 0, 0).expect_err("zero voters must refuse");
        assert!(zero.contains("last raft voter"), "{zero}");
        assert!(zero.contains("zero voters"), "{zero}");

        // 3 voters, one dead, removing a LIVE one: remaining {leader, dead}
        // needs quorum 2 with only 1 alive — refused.
        let wedged = removal_guard("mgr-b", 2, 1).expect_err("infeasible quorum must refuse");
        assert!(wedged.contains("refusing to remove mgr-b"), "{wedged}");
        assert!(wedged.contains("need 2 alive"), "{wedged}");

        // Healthy removals proceed (even when the RESULT is even — that is a
        // warning, not a block: JC-29).
        assert!(removal_guard("mgr-c", 2, 2).is_ok());
        assert!(removal_guard("mgr-c", 1, 1).is_ok());
    }

    #[test]
    fn validate_raft_config_errors_on_zero_expected_voters() {
        let mut cfg = RaftPersistenceConfigFull {
            expected_voters: Some(0),
            ..Default::default()
        };
        let err = validate_raft_config(&cfg).expect_err("expectedVoters=0 must fail load");
        assert!(format!("{err:#}").contains("at least 1"), "{err:#}");

        // 1/3/5 and unset all pass; even values pass with a (once) warning.
        for ok in [None, Some(1), Some(2), Some(3), Some(4), Some(5)] {
            cfg.expected_voters = ok;
            validate_raft_config(&cfg).expect("non-zero expectedVoters must load");
        }
        cfg.expected_voters = None;
        cfg.peers = vec!["mgr-a=10.0.0.1:4747".into(), "mgr-b=10.0.0.2:4747".into()];
        validate_raft_config(&cfg).expect("even peers list warns but loads");
    }

    /// The shipped defaults, and any sanely-spaced override, load cleanly —
    /// and the values that reach openraft agree with what was configured.
    #[test]
    fn validate_raft_config_accepts_sane_election_timing() {
        let defaults = RaftPersistenceConfigFull::default();
        validate_raft_config(&defaults).expect("shipped defaults must load");
        // Guard against a future default edit that quietly violates the
        // very relationship this validation exists to enforce.
        assert!(
            defaults.heartbeat_interval_ms * MIN_HEARTBEATS_PER_ELECTION_TIMEOUT
                <= defaults.election_timeout_min_ms,
            "default heartbeat {} vs election min {}",
            defaults.heartbeat_interval_ms,
            defaults.election_timeout_min_ms
        );
        assert!(defaults.election_timeout_min_ms < defaults.election_timeout_max_ms);

        // Relaxed timings of the kind the multi-node test harnesses and
        // loaded/WAN deployments use.
        for (heartbeat, min, max) in [(250, 600, 1200), (500, 1500, 3000), (1000, 4000, 8000)] {
            let cfg = RaftPersistenceConfigFull {
                heartbeat_interval_ms: heartbeat,
                election_timeout_min_ms: min,
                election_timeout_max_ms: max,
                ..Default::default()
            };
            validate_raft_config(&cfg)
                .unwrap_or_else(|e| panic!("{heartbeat}/{min}/{max} must load: {e:#}"));
        }
    }

    #[test]
    fn validate_raft_config_rejects_heartbeat_too_large_for_election_timeout() {
        // 400ms heartbeat under a 600ms election minimum: 1.5 heartbeats,
        // below the 2x floor — one slow round trip unseats the leader.
        let cfg = RaftPersistenceConfigFull {
            heartbeat_interval_ms: 400,
            election_timeout_min_ms: 600,
            election_timeout_max_ms: 1200,
            ..Default::default()
        };
        let err = validate_raft_config(&cfg).expect_err("heartbeat >= min/2 must fail load");
        let msg = format!("{err:#}");
        // The message must name both offending values and the remedy.
        assert!(msg.contains("electionTimeoutMinMs = 600"), "{msg}");
        assert!(msg.contains("heartbeatIntervalMs = 400"), "{msg}");
        assert!(
            msg.contains("raise electionTimeoutMinMs to at least 800"),
            "{msg}"
        );
        assert!(
            msg.contains("lower heartbeatIntervalMs to at most 300"),
            "{msg}"
        );

        // A zero heartbeat is its own degenerate case.
        let zero = RaftPersistenceConfigFull {
            heartbeat_interval_ms: 0,
            ..Default::default()
        };
        let err = validate_raft_config(&zero).expect_err("zero heartbeat must fail load");
        assert!(
            format!("{err:#}").contains("heartbeatIntervalMs must be greater than 0"),
            "{err:#}"
        );
    }

    #[test]
    fn validate_raft_config_rejects_election_min_at_or_above_max() {
        for (min, max) in [(1200u64, 1200u64), (1600, 1200)] {
            let cfg = RaftPersistenceConfigFull {
                heartbeat_interval_ms: 250,
                election_timeout_min_ms: min,
                election_timeout_max_ms: max,
                ..Default::default()
            };
            let err = validate_raft_config(&cfg)
                .expect_err(&format!("min={min} max={max} must fail load"));
            let msg = format!("{err:#}");
            assert!(
                msg.contains(&format!("electionTimeoutMinMs = {min}")),
                "{msg}"
            );
            assert!(
                msg.contains(&format!("electionTimeoutMaxMs = {max}")),
                "{msg}"
            );
            assert!(msg.contains("strictly less than"), "{msg}");
        }
    }
}
