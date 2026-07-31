//! Job priority as a small set of named bands.
//!
//! Legion has one importance axis and it is a closed vocabulary, not an
//! integer. Every mature system converged here after pain: Borg collapsed
//! fine-grained priorities into bands *because* of preemption cascades,
//! Nomad's "victims must be ≥10 points lower" rule is a band rule wearing an
//! integer costume, and Kubernetes' 2^31 space produced the
//! `priority: 1000000000` arms race that platforms then patch with quota and
//! admission webhooks.
//!
//! Bands buy three things an integer cannot:
//!
//! - **A structural anti-cascade rule.** Preemption is strictly *cross-band*:
//!   an allocation is only ever a victim of a job in a higher band, never its
//!   own. [`Band::Critical`] therefore never preempts `critical` (Borg's
//!   production-band rule), and displaced work can only ever displace work
//!   below it — so a cascade is bounded by the number of bands and only ever
//!   runs downward.
//! - **Governance as a vocabulary question.** "Who may use `critical`" is an
//!   ACL selector over six known strings (`bands` on `job` rules), not an
//!   audit of arbitrary integers.
//! - **One field, both meanings.** The band sets the job's default eval-queue
//!   priority *and* governs preemption, which is the single most-requested fix
//!   to Nomad's model.
//!
//! See `docs/preemption-and-eviction.md` and
//! `internal_docs/plans/preemption-and-eviction-plan.md` § Job priority.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A job's priority band. Ordered most important first; the `Ord` derive
/// follows declaration order, so **`Critical < Background`** as an enum
/// value. Never compare bands with `<`/`>` directly — use [`Band::rank`],
/// where a larger number means more important, or the intent-named
/// [`Band::outranks`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Band {
    /// Cluster-critical work: the control plane's own jobs, incident-response
    /// capacity, anything whose loss is an outage. Never preempted, because
    /// nothing sits above it. Gate this band with an ACL `bands` selector —
    /// an ungoverned top band means nothing.
    Critical,
    /// Production work that pages someone when it stops.
    High,
    /// Production-adjacent work that matters more than the default but is not
    /// on the paging path: async workers behind a user-visible queue,
    /// near-real-time pipelines, staging environments that gate a release.
    Elevated,
    /// The default. Everything that does not say otherwise lands here.
    #[default]
    Normal,
    /// Work that may be displaced when the cluster is full: batch backfills,
    /// nightly reports, CI fleets. Choosing this band **is** the consent to
    /// be preempted.
    Low,
    /// Fill-the-gaps work with no completion expectation at all: speculative
    /// jobs, opportunistic re-encodes, cache warmers. First to go, and
    /// preemptible by every band above it.
    Background,
}

impl Band {
    /// Every band, most important first. The order the CLI, the docs and the
    /// error messages all render.
    pub const ALL: &'static [Band] = &[
        Band::Critical,
        Band::High,
        Band::Elevated,
        Band::Normal,
        Band::Low,
        Band::Background,
    ];

    /// The stable wire/jobspec spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Band::Critical => "critical",
            Band::High => "high",
            Band::Elevated => "elevated",
            Band::Normal => "normal",
            Band::Low => "low",
            Band::Background => "background",
        }
    }

    /// Parse a jobspec/wire spelling. Unlike [`crate::structs::StopCause`],
    /// an unrecognized band is an **error**: a typo'd band silently becoming
    /// `normal` would mean a job the author marked `critical` quietly loses
    /// every contention fight, which is exactly the class of bug the closed
    /// vocabulary exists to prevent.
    pub fn parse(value: &str) -> Result<Self, String> {
        Band::ALL
            .iter()
            .copied()
            .find(|band| band.as_str() == value)
            .ok_or_else(|| {
                format!(
                    "unknown priority band {value:?}; expected one of {}",
                    Band::ALL
                        .iter()
                        .map(|band| band.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }

    /// Parse a stored value, falling back to the default for anything
    /// unrecognized. Used on the read path only: a row written by a newer
    /// manager naming a band this build has never heard of must not fail
    /// every plan that touches the job.
    pub fn from_stored(value: &str) -> Self {
        Band::parse(value).unwrap_or_default()
    }

    /// Importance rank — **larger is more important**. The number is
    /// deliberately not the eval priority: bands are a closed ordering, and
    /// tying the two would make a change to queue weights a change to
    /// preemption semantics.
    pub fn rank(&self) -> u8 {
        match self {
            Band::Critical => 50,
            Band::High => 40,
            Band::Elevated => 30,
            Band::Normal => 20,
            Band::Low => 10,
            Band::Background => 0,
        }
    }

    /// Whether `self` may preempt work in `other`. Strictly cross-band and
    /// strictly downward: equal bands never preempt each other, which is what
    /// bounds cascades.
    pub fn outranks(&self, other: Band) -> bool {
        self.rank() > other.rank()
    }

    /// Default eval-queue priority for jobs in this band. `legion job run
    /// --priority N` and `--asap` still override it for one submission.
    ///
    /// The values keep `normal` at the historical default of 50 so an
    /// un-banded cluster's queue behaves exactly as it did.
    pub fn eval_priority(&self) -> i64 {
        match self {
            Band::Critical => 90,
            Band::High => 70,
            Band::Elevated => 60,
            Band::Normal => 50,
            Band::Low => 30,
            Band::Background => 15,
        }
    }

    /// Whether anything could preempt this band's work. `critical` is the top
    /// band, so it is never a victim.
    pub fn is_preemptible(&self) -> bool {
        Band::ALL.iter().any(|band| band.outranks(*self))
    }

    /// Whether this band could ever displace anything. `background` is the
    /// floor, so it never preempts.
    pub fn can_preempt(&self) -> bool {
        Band::ALL.iter().any(|band| self.outranks(*band))
    }
}

impl fmt::Display for Band {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Band {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Band::parse(value)
    }
}

#[cfg(test)]
mod band_test {
    use super::*;

    #[test]
    fn wire_forms_round_trip() {
        for band in Band::ALL {
            assert_eq!(Band::parse(band.as_str()), Ok(*band));
        }
    }

    #[test]
    fn an_unknown_band_is_an_error_not_a_silent_normal() {
        let err = Band::parse("urgent").expect_err("unknown band should not parse");
        assert!(err.contains("urgent"), "{err}");
        assert!(
            err.contains("critical"),
            "message lists the vocabulary: {err}"
        );
        // The read path is the one place it degrades instead.
        assert_eq!(Band::from_stored("urgent"), Band::Normal);
    }

    #[test]
    fn ranks_are_strictly_ordered_most_important_first() {
        for window in Band::ALL.windows(2) {
            assert!(
                window[0].outranks(window[1]),
                "{} should outrank {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn preemption_is_never_within_a_band() {
        for band in Band::ALL {
            assert!(!band.outranks(*band), "{band} must not preempt itself");
        }
    }

    #[test]
    fn the_top_band_is_never_a_victim_and_the_floor_never_preempts() {
        assert!(!Band::Critical.is_preemptible());
        assert!(Band::High.is_preemptible());
        assert!(!Band::Background.can_preempt());
        assert!(Band::Low.can_preempt());
    }

    #[test]
    fn eval_priorities_keep_normal_at_the_historical_default() {
        assert_eq!(Band::Normal.eval_priority(), 50);
        // And stay strictly ordered, so the queue agrees with the bands.
        for window in Band::ALL.windows(2) {
            assert!(
                window[0].eval_priority() > window[1].eval_priority(),
                "{} should dequeue before {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn serde_uses_the_wire_spelling() -> anyhow::Result<()> {
        assert_eq!(serde_json::to_string(&Band::Background)?, "\"background\"");
        assert_eq!(
            serde_json::from_str::<Band>("\"elevated\"")?,
            Band::Elevated
        );
        Ok(())
    }
}
