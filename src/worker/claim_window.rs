//! The node's window on unratified edge claims: admission control that
//! sheds edge starts before they boot when ratification falls behind
//! (docs/edge-boot.md, "Overload"; edge admission control plan §5).
//!
//! An edge start boots while its ratification is in flight, and dies at its
//! lease (`speculativeStart.ratifyTimeout`) if no verdict arrives. When the
//! managers fall behind, every start a node admits boots, waits out its
//! lease, and is killed, while the router retries it and adds more load. The
//! window stops that: a node holds at most `window` unratified edge claims,
//! and a start that finds the window full waits up to `edgeBoot.queueTimeout`
//! for one to settle, then is refused `OVERLOADED` (retryable) before it is
//! admitted, booted, or spends its ticket.
//!
//! The window is additive-increase, multiplicative-decrease, the control law
//! TCP uses for the same problem (many senders, one shared bottleneck, and
//! only delay to go on):
//!
//! - it starts at, and never exceeds, `edgeBoot.maxProvisionalPerNode`;
//! - a verdict that arrives within `edgeBoot.overloadThreshold` of its
//!   claim's lease grows it by `1 / window` (one slot per window of timely
//!   verdicts);
//! - a verdict that arrives later, a ratify call that fails, or a lease that
//!   runs out halves it, never below one. A claim still waiting past its
//!   threshold counts as late the moment an admission looks, so shedding
//!   starts within the threshold rather than a lease later. Only claims
//!   admitted after the last cut can cut again: the claims that were already
//!   in flight belong to the congestion the node has just reacted to.
//!
//! The floor of one keeps every node probing, so admission resumes on its
//! own once verdicts come back fast. Claims already admitted are never
//! touched: the window only decides what is admitted next.
//!
//! Only edge claims (`Worker.StartClaim`) count. A manager-originated
//! speculative dispatch was placed by the scheduler, which already checked
//! the node's capacity, and a session is never ratified at all.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// What the end of a claim says about the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feedback {
    /// The manager answered, confirmed or refused. Timely or late by how
    /// long it took relative to the claim's lease.
    Verdict,
    /// No answer: the ratify call failed, or the lease ran out first.
    Failed,
    /// The claim ended for a reason of the node's own (its launch failed,
    /// it was withdrawn). Says nothing about the control plane.
    Neutral,
}

/// Why an edge start was refused before admission.
#[derive(Debug, Clone, PartialEq)]
pub struct Overloaded {
    /// Unratified edge claims (and reservations) the node holds.
    pub held: usize,
    /// The window in force.
    pub window: usize,
    /// `edgeBoot.maxProvisionalPerNode`.
    pub ceiling: usize,
    /// How long the start waited for room (`edgeBoot.queueTimeout`).
    pub waited: Duration,
}

impl Overloaded {
    /// Whether the window is below its ceiling because ratifications were
    /// slow, rather than full at the configured cap.
    pub fn shedding(&self) -> bool {
        self.window < self.ceiling
    }
}

impl std::fmt::Display for Overloaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let waited = humantime::format_duration(self.waited);
        if self.shedding() {
            write!(
                f,
                "node is overloaded: recent ratifications were slow, so it holds at most {} \
                 unratified edge claims (edgeBoot.maxProvisionalPerNode is {}), and all {} \
                 stayed unratified for {waited}\nHint: the managers are behind on \
                 ratification, so retry with backoff (other nodes share the same managers)",
                self.window, self.ceiling, self.held
            )
        } else {
            write!(
                f,
                "node is overloaded: all {} unratified-claim slots \
                 (edgeBoot.maxProvisionalPerNode) stayed held for {waited}\nHint: retry with \
                 backoff or on another node, or raise edgeBoot.maxProvisionalPerNode",
                self.ceiling
            )
        }
    }
}

impl std::error::Error for Overloaded {}

/// The window's current numbers, for the dry run and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowSnapshot {
    pub held: usize,
    pub window: usize,
    pub ceiling: usize,
}

/// One admitted edge claim the window is waiting on.
#[derive(Debug, Clone)]
struct Entry {
    admitted_at: Instant,
    lease: Duration,
    /// Already counted as late, by [`WindowState::note_overdue`].
    signalled: bool,
}

/// The control law, with time passed in so it is testable without a clock.
#[derive(Debug)]
pub(crate) struct WindowState {
    ceiling: usize,
    /// `edgeBoot.overloadThreshold`; `0` turns adaptation off.
    threshold: f64,
    limit: f64,
    /// Reservations not yet bound to a claim, plus bound claims.
    held: usize,
    last_cut: Option<Instant>,
    claims: HashMap<String, Entry>,
}

impl WindowState {
    pub(crate) fn new(ceiling: usize, threshold: f64) -> Self {
        let ceiling = ceiling.max(1);
        WindowState {
            ceiling,
            threshold: threshold.clamp(0.0, 1.0),
            limit: ceiling as f64,
            held: 0,
            last_cut: None,
            claims: HashMap::new(),
        }
    }

    pub(crate) fn window(&self) -> usize {
        (self.limit.floor() as usize).clamp(1, self.ceiling)
    }

    fn adaptive(&self) -> bool {
        self.threshold > 0.0
    }

    pub(crate) fn snapshot(&self) -> WindowSnapshot {
        WindowSnapshot {
            held: self.held,
            window: self.window(),
            ceiling: self.ceiling,
        }
    }

    /// Take a slot if there is room, after counting overdue claims.
    pub(crate) fn try_reserve(&mut self, now: Instant) -> bool {
        self.note_overdue(now);
        if self.held < self.window() {
            self.held += 1;
            true
        } else {
            false
        }
    }

    /// Give back a slot that was never bound to a claim.
    pub(crate) fn unreserve(&mut self) {
        self.held = self.held.saturating_sub(1);
    }

    /// Bind a reserved slot to the claim it admitted.
    pub(crate) fn bind(&mut self, claim_id: &str, lease: Duration, now: Instant) {
        self.claims.insert(
            claim_id.to_string(),
            Entry {
                admitted_at: now,
                lease,
                signalled: false,
            },
        );
    }

    /// Count every claim past its threshold without a verdict as late now,
    /// so the window shrinks within the threshold of trouble starting, not
    /// a whole lease later.
    pub(crate) fn note_overdue(&mut self, now: Instant) {
        if !self.adaptive() {
            return;
        }
        let threshold = self.threshold;
        let overdue: Vec<Instant> = self
            .claims
            .values_mut()
            .filter(|entry| {
                !entry.signalled
                    && now.saturating_duration_since(entry.admitted_at)
                        > entry.lease.mul_f64(threshold)
            })
            .map(|entry| {
                entry.signalled = true;
                entry.admitted_at
            })
            .collect();
        for admitted_at in overdue {
            self.late(admitted_at, now);
        }
    }

    fn late(&mut self, admitted_at: Instant, now: Instant) {
        // One cut per congestion episode: a claim admitted before the last
        // cut was sent into the congestion that cut already answered.
        if self.last_cut.is_none_or(|cut| admitted_at > cut) {
            self.limit = (self.limit / 2.0).max(1.0);
            self.last_cut = Some(now);
        }
    }

    fn timely(&mut self) {
        self.limit = (self.limit + 1.0 / self.limit).min(self.ceiling as f64);
    }

    /// The claim stopped being unratified. Returns whether the window was
    /// tracking it.
    pub(crate) fn settle(&mut self, claim_id: &str, feedback: Feedback, now: Instant) -> bool {
        let Some(entry) = self.claims.remove(claim_id) else {
            return false;
        };
        self.held = self.held.saturating_sub(1);
        if !self.adaptive() || entry.signalled {
            return true;
        }
        match feedback {
            Feedback::Verdict => {
                let elapsed = now.saturating_duration_since(entry.admitted_at);
                if elapsed > entry.lease.mul_f64(self.threshold) {
                    self.late(entry.admitted_at, now);
                } else {
                    self.timely();
                }
            }
            Feedback::Failed => self.late(entry.admitted_at, now),
            Feedback::Neutral => {}
        }
        true
    }

    fn overloaded(&self, waited: Duration) -> Overloaded {
        Overloaded {
            held: self.held,
            window: self.window(),
            ceiling: self.ceiling,
            waited,
        }
    }
}

/// The node's window, shared by the RPC handlers that admit edge starts and
/// the task manager that settles them.
#[derive(Debug)]
pub struct ClaimWindow {
    state: Mutex<WindowState>,
    /// Woken whenever a slot frees or the window grows.
    room: Notify,
    queue_timeout: Duration,
}

impl ClaimWindow {
    pub fn new(ceiling: usize, threshold: f64, queue_timeout: Duration) -> Self {
        let state = WindowState::new(ceiling, threshold);
        crate::manager::telemetry::worker_metrics()
            .claim_window
            .set(state.window() as i64);
        ClaimWindow {
            state: Mutex::new(state),
            room: Notify::new(),
            queue_timeout,
        }
    }

    /// The window a worker runs with, from its resolved `edgeBoot` block.
    pub fn from_config(config: &crate::structs::WorkerEdgeBootConfigFull) -> Self {
        let queue_timeout =
            humantime::parse_duration(&config.queue_timeout).unwrap_or(Duration::from_secs(1));
        ClaimWindow::new(
            config.max_provisional_per_node as usize,
            config.overload_threshold,
            queue_timeout,
        )
    }

    fn lock(&self) -> MutexGuard<'_, WindowState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Run `f` on the state and publish the window if it moved, waking
    /// waiters when there may be room.
    fn update<T>(&self, f: impl FnOnce(&mut WindowState) -> T) -> T {
        let mut state = self.lock();
        let (held, window) = (state.held, state.window());
        let out = f(&mut state);
        let (held_after, window_after, ceiling) = (state.held, state.window(), state.ceiling);
        drop(state);
        if window_after != window {
            crate::manager::telemetry::worker_metrics()
                .claim_window
                .set(window_after as i64);
            // Lifecycle only: a cut happens at most once per slow patch, and
            // growth is logged when it is complete, never per verdict.
            if window_after < window {
                tracing::info!(
                    from = window,
                    to = window_after,
                    ceiling,
                    "Ratification is slow, shrinking the edge claim window"
                );
            } else if window_after == ceiling {
                tracing::info!(ceiling, "Edge claim window is back at its ceiling");
            }
        }
        if held_after < held || window_after > window {
            self.room.notify_waiters();
        }
        out
    }

    /// Reserve room for one edge claim, waiting up to `edgeBoot.queueTimeout`
    /// for a slot to settle. The refusal is the `OVERLOADED` answer.
    pub async fn acquire(self: &Arc<Self>) -> Result<Reservation, Overloaded> {
        let deadline = tokio::time::Instant::now() + self.queue_timeout;
        loop {
            // Registered before the check, so a slot that frees between the
            // check and the wait still wakes this start.
            let notified = self.room.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.update(|state| state.try_reserve(Instant::now())) {
                return Ok(Reservation {
                    window: Some(self.clone()),
                });
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                // One last look: a slot may have freed at the deadline.
                if self.update(|state| state.try_reserve(Instant::now())) {
                    return Ok(Reservation {
                        window: Some(self.clone()),
                    });
                }
                crate::manager::telemetry::worker_metrics()
                    .claim_overload_refusals_total
                    .inc();
                return Err(self.lock().overloaded(self.queue_timeout));
            }
        }
    }

    /// The claim stopped being unratified: feed what its end says back into
    /// the window. A claim the window is not tracking (a session, a
    /// manager-originated claim, one already settled) is ignored.
    pub fn settle(&self, claim_id: &str, feedback: Feedback) {
        self.update(|state| state.settle(claim_id, feedback, Instant::now()));
    }

    /// Free a claim's slot without feedback. The backstop every claim exit
    /// calls, so a slot can never leak and wedge the node shut.
    pub fn release(&self, claim_id: &str) {
        self.settle(claim_id, Feedback::Neutral);
    }

    pub fn snapshot(&self) -> WindowSnapshot {
        self.lock().snapshot()
    }
}

impl Default for ClaimWindow {
    fn default() -> Self {
        ClaimWindow::from_config(&crate::structs::WorkerEdgeBootConfigFull::default())
    }
}

/// A slot reserved for an edge start that is not admitted yet. Binding it
/// hands the slot to the admitted claim; dropping it unbound (the start was
/// refused for another reason) gives the slot back.
#[derive(Debug)]
pub struct Reservation {
    window: Option<Arc<ClaimWindow>>,
}

impl Reservation {
    /// The admitted claim now holds the slot until it settles.
    pub fn bind(mut self, claim_id: &str, lease: Duration) {
        if let Some(window) = self.window.take() {
            window.update(|state| state.bind(claim_id, lease, Instant::now()));
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(window) = self.window.take() {
            window.update(WindowState::unreserve);
        }
    }
}

#[cfg(test)]
mod claim_window_test {
    use super::*;

    const LEASE: Duration = Duration::from_secs(5);

    fn admit(state: &mut WindowState, claim_id: &str, now: Instant) -> bool {
        if !state.try_reserve(now) {
            return false;
        }
        state.bind(claim_id, LEASE, now);
        true
    }

    #[test]
    fn the_window_starts_at_the_ceiling_and_caps_admission_test() {
        let now = Instant::now();
        let mut state = WindowState::new(2, 0.5);
        assert!(admit(&mut state, "a", now));
        assert!(admit(&mut state, "b", now));
        assert!(!admit(&mut state, "c", now), "the ceiling is a hard cap");
        assert!(state.settle("a", Feedback::Verdict, now + Duration::from_millis(20)));
        assert!(
            admit(&mut state, "c", now),
            "a settled claim frees its slot"
        );
    }

    #[test]
    fn a_slow_verdict_halves_the_window_and_fast_ones_grow_it_back_test() {
        let t0 = Instant::now();
        let mut state = WindowState::new(32, 0.5);
        assert!(admit(&mut state, "slow", t0));
        // 3s of a 5s lease is past the 50% threshold.
        state.settle("slow", Feedback::Verdict, t0 + Duration::from_secs(3));
        assert_eq!(state.window(), 16);

        // Additive increase: about one slot per window's worth of timely
        // verdicts, so 17 of them take it from 16 to 17.
        let t1 = t0 + Duration::from_secs(4);
        for i in 0..17 {
            assert_eq!(state.window(), 16, "after {i} timely verdicts");
            let id = format!("fast-{i}");
            assert!(admit(&mut state, &id, t1));
            state.settle(&id, Feedback::Verdict, t1 + Duration::from_millis(30));
        }
        assert_eq!(state.window(), 17);
    }

    #[test]
    fn claims_already_in_flight_cut_the_window_only_once_test() {
        // Ten claims sent into one slow patch are one congestion episode,
        // not ten: halving per claim would collapse the window to one on a
        // single blip.
        let t0 = Instant::now();
        let mut state = WindowState::new(32, 0.5);
        for i in 0..10 {
            assert!(admit(&mut state, &format!("c{i}"), t0));
        }
        let late = t0 + Duration::from_secs(4);
        for i in 0..10 {
            state.settle(&format!("c{i}"), Feedback::Verdict, late);
        }
        assert_eq!(state.window(), 16);
        // A claim admitted after that cut is new evidence and cuts again.
        let t1 = late + Duration::from_millis(1);
        assert!(admit(&mut state, "d", t1));
        state.settle("d", Feedback::Failed, t1 + Duration::from_millis(5));
        assert_eq!(state.window(), 8);
    }

    #[test]
    fn an_overdue_claim_cuts_before_its_verdict_arrives_test() {
        let t0 = Instant::now();
        let mut state = WindowState::new(2, 0.5);
        assert!(admit(&mut state, "a", t0));
        // Before the threshold the second slot is free.
        assert!(state.try_reserve(t0 + Duration::from_secs(1)));
        state.unreserve();
        // Past it, "a" counts as late without waiting for its verdict, and
        // the window drops to one, which "a" already holds.
        assert!(!state.try_reserve(t0 + Duration::from_secs(3)));
        assert_eq!(state.window(), 1);
        // Its verdict, when it lands, is not counted twice.
        state.settle("a", Feedback::Verdict, t0 + Duration::from_millis(4500));
        assert_eq!(state.window(), 1);
        assert_eq!(state.snapshot().held, 0);
    }

    #[test]
    fn admission_resumes_through_the_floor_of_one_test() {
        // However slow things got, one claim is always admitted, so a node
        // keeps probing and recovers on its own when verdicts are fast again.
        let mut now = Instant::now();
        let mut state = WindowState::new(32, 0.5);
        for round in 0..10 {
            let id = format!("slow-{round}");
            assert!(admit(&mut state, &id, now), "round {round}");
            now += Duration::from_secs(5);
            state.settle(&id, Feedback::Failed, now);
            now += Duration::from_millis(1);
        }
        assert_eq!(state.window(), 1);
        assert!(admit(&mut state, "probe", now));
        state.settle("probe", Feedback::Verdict, now + Duration::from_millis(20));
        assert_eq!(state.window(), 2);
    }

    #[test]
    fn a_failed_call_counts_as_late_and_a_local_failure_counts_as_nothing_test() {
        let t0 = Instant::now();
        let mut state = WindowState::new(8, 0.5);
        assert!(admit(&mut state, "local", t0));
        state.settle("local", Feedback::Neutral, t0 + Duration::from_millis(5));
        assert_eq!(state.window(), 8);
        assert!(admit(&mut state, "unreachable", t0));
        state.settle(
            "unreachable",
            Feedback::Failed,
            t0 + Duration::from_millis(5),
        );
        assert_eq!(state.window(), 4);
    }

    #[test]
    fn a_zero_threshold_keeps_a_static_cap_test() {
        let t0 = Instant::now();
        let mut state = WindowState::new(4, 0.0);
        assert!(admit(&mut state, "a", t0));
        // Neither an overdue claim nor a failed one moves the window.
        assert!(state.try_reserve(t0 + Duration::from_secs(60)));
        state.unreserve();
        state.settle("a", Feedback::Failed, t0 + Duration::from_secs(5));
        assert_eq!(state.window(), 4);
    }

    #[test]
    fn an_unbound_reservation_gives_its_slot_back_test() {
        let window = Arc::new(ClaimWindow::new(1, 0.5, Duration::from_millis(10)));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let reservation = window.acquire().await.expect("room");
            assert_eq!(window.snapshot().held, 1);
            drop(reservation);
            assert_eq!(window.snapshot().held, 0);
            window.acquire().await.expect("room again").bind("a", LEASE);
            let refused = window.acquire().await.expect_err("full");
            assert_eq!(refused.held, 1);
            assert!(!refused.shedding());
            assert!(refused.to_string().contains("maxProvisionalPerNode"));
            window.release("a");
            assert!(window.acquire().await.is_ok(), "released");
        });
    }

    #[test]
    fn the_window_keeps_goodput_when_ratification_cannot_keep_up_test() {
        // 100 edge nodes, managers that ratify 500 claims a second, a 5s
        // lease. At twice that offered load, a node with no window boots
        // everything and nearly all of it dies at the lease; the window
        // keeps ratified starts near what the managers can do.
        let none = overload_sim::run(&overload_sim::Scenario {
            load: 2.0,
            ..overload_sim::Scenario::unwindowed()
        });
        let window = overload_sim::run(&overload_sim::Scenario {
            load: 2.0,
            ..overload_sim::Scenario::default()
        });
        assert!(none.goodput < 0.4 * overload_sim::MU, "{none:?}");
        assert!(none.killed_after_boot > overload_sim::MU, "{none:?}");
        assert!(window.goodput > 0.85 * overload_sim::MU, "{window:?}");
        assert!(
            window.killed_after_boot < 0.1 * overload_sim::MU,
            "{window:?}"
        );

        // Below capacity the window changes nothing.
        let calm = overload_sim::run(&overload_sim::Scenario {
            load: 0.5,
            ..overload_sim::Scenario::default()
        });
        assert_eq!(calm.refused_before_boot, 0.0, "{calm:?}");
        assert_eq!(calm.killed_after_boot, 0.0, "{calm:?}");
    }

    /// The table in the edge admission control plan. Run with
    /// `cargo test --lib overload_table -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn overload_table() {
        println!(
            "{:>5} {:>5} {:>10} {:>8} {:>8} {:>8} {:>8} {:>6} {:>6}",
            "nodes", "load", "mode", "offered", "goodput", "killed", "refused", "p50", "p99"
        );
        for nodes in [50, 100, 200] {
            for load in [0.5, 0.9, 1.2, 2.0, 3.0] {
                for (mode, base) in [
                    ("none", overload_sim::Scenario::unwindowed()),
                    ("static32", overload_sim::Scenario::static_cap()),
                    ("window32", overload_sim::Scenario::default()),
                    (
                        "window@0.5",
                        overload_sim::Scenario {
                            threshold: 0.5,
                            ..overload_sim::Scenario::default()
                        },
                    ),
                ] {
                    let r = overload_sim::run(&overload_sim::Scenario {
                        nodes,
                        load,
                        ..base
                    });
                    println!(
                        "{nodes:>5} {load:>5.1} {mode:>10} {:>8.0} {:>8.0} {:>8.0} {:>8.0} {:>6.2} {:>6.2}",
                        r.offered, r.goodput, r.killed_after_boot, r.refused_before_boot, r.p50, r.p99
                    );
                }
            }
        }
    }

    /// A discrete-event model of a pool of edge nodes in front of managers
    /// that ratify first come, first served at a fixed rate. Each node runs
    /// the real [`WindowState`]; only time and the managers are simulated.
    /// The managers process every ratification they receive, including ones
    /// whose node already gave up, as the synchronous `RatifyClaim` handler
    /// does.
    mod overload_sim {
        use super::super::*;
        use std::cmp::Reverse;
        use std::collections::{BinaryHeap, HashSet, VecDeque};

        /// Ratifications per second the managers sustain.
        pub const MU: f64 = 500.0;

        #[derive(Debug, Clone, Copy)]
        pub struct Scenario {
            pub nodes: usize,
            /// Offered starts as a multiple of [`MU`].
            pub load: f64,
            pub ceiling: usize,
            pub threshold: f64,
            pub lease: f64,
            pub queue_timeout: f64,
            pub seconds: f64,
        }

        impl Default for Scenario {
            fn default() -> Self {
                Scenario {
                    nodes: 100,
                    load: 1.0,
                    ceiling: crate::structs::config::DEF_MAX_PROVISIONAL_PER_NODE as usize,
                    threshold: crate::structs::config::DEF_OVERLOAD_THRESHOLD,
                    lease: 5.0,
                    queue_timeout: 1.0,
                    seconds: 30.0,
                }
            }
        }

        impl Scenario {
            /// Before this change: nothing bounds unratified claims.
            pub fn unwindowed() -> Self {
                Scenario {
                    ceiling: usize::MAX / 2,
                    threshold: 0.0,
                    ..Scenario::default()
                }
            }

            /// `maxProvisionalPerNode` enforced, adaptation off.
            pub fn static_cap() -> Self {
                Scenario {
                    threshold: 0.0,
                    ..Scenario::default()
                }
            }
        }

        /// Rates are per second of offered load.
        #[derive(Debug)]
        pub struct Outcome {
            pub offered: f64,
            pub goodput: f64,
            pub killed_after_boot: f64,
            pub refused_before_boot: f64,
            pub p50: f64,
            pub p99: f64,
        }

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum Event {
            Arrive(usize),
            WaitTimeout(u64),
            Verdict(usize, u64),
            Expire(usize, u64),
        }

        /// A small deterministic generator, so the model needs no new
        /// dependency and every run is the same.
        struct Lcg(u64);

        impl Lcg {
            fn unit(&mut self) -> f64 {
                self.0 = self
                    .0
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((self.0 >> 11) as f64 + 0.5) / (1u64 << 53) as f64
            }
        }

        struct Node {
            state: WindowState,
            waiters: VecDeque<u64>,
        }

        struct Sim {
            base: Instant,
            now: f64,
            seq: u64,
            events: BinaryHeap<Reverse<(u64, u64, Event)>>,
            nodes: Vec<Node>,
            waiting: HashSet<u64>,
            admitted_at: std::collections::HashMap<u64, f64>,
            manager_free: f64,
            lease: f64,
            latencies: Vec<f64>,
            ratified: u64,
            killed: u64,
            refused: u64,
        }

        impl Sim {
            fn at(&self, t: f64) -> Instant {
                self.base + Duration::from_secs_f64(t)
            }

            fn push(&mut self, t: f64, event: Event) {
                self.seq += 1;
                let nanos = (t * 1e9) as u64;
                self.events.push(Reverse((nanos, self.seq, event)));
            }

            fn admit(&mut self, node: usize) {
                self.seq += 1;
                let claim = self.seq;
                let now = self.at(self.now);
                self.nodes[node].state.bind(
                    &claim.to_string(),
                    Duration::from_secs_f64(self.lease),
                    now,
                );
                self.admitted_at.insert(claim, self.now);
                let start = self.manager_free.max(self.now);
                let done = start + 1.0 / MU;
                self.manager_free = done;
                if done - self.now <= self.lease {
                    self.push(done, Event::Verdict(node, claim));
                } else {
                    self.push(self.now + self.lease, Event::Expire(node, claim));
                }
            }

            fn drain(&mut self, node: usize) {
                while let Some(&waiter) = self.nodes[node].waiters.front() {
                    if !self.waiting.contains(&waiter) {
                        self.nodes[node].waiters.pop_front();
                        continue;
                    }
                    let now = self.at(self.now);
                    if !self.nodes[node].state.try_reserve(now) {
                        break;
                    }
                    self.nodes[node].waiters.pop_front();
                    self.waiting.remove(&waiter);
                    self.admit(node);
                }
            }

            fn settle(&mut self, node: usize, claim: u64, feedback: Feedback) {
                let now = self.at(self.now);
                self.nodes[node]
                    .state
                    .settle(&claim.to_string(), feedback, now);
                self.drain(node);
            }
        }

        pub fn run(scenario: &Scenario) -> Outcome {
            let mut rng = Lcg(0x5eed);
            let mut sim = Sim {
                base: Instant::now(),
                now: 0.0,
                seq: 0,
                events: BinaryHeap::new(),
                nodes: (0..scenario.nodes)
                    .map(|_| Node {
                        state: WindowState::new(scenario.ceiling, scenario.threshold),
                        waiters: VecDeque::new(),
                    })
                    .collect(),
                waiting: HashSet::new(),
                admitted_at: std::collections::HashMap::new(),
                manager_free: 0.0,
                lease: scenario.lease,
                latencies: Vec::new(),
                ratified: 0,
                killed: 0,
                refused: 0,
            };
            let rate = scenario.load * MU;
            let mut t = 0.0;
            let mut offered = 0u64;
            loop {
                t += -rng.unit().ln() / rate;
                if t >= scenario.seconds {
                    break;
                }
                let node = (rng.unit() * scenario.nodes as f64) as usize % scenario.nodes;
                sim.push(t, Event::Arrive(node));
                offered += 1;
            }
            while let Some(Reverse((nanos, _, event))) = sim.events.pop() {
                sim.now = nanos as f64 / 1e9;
                // Rates cover the offered interval only, not the managers
                // draining their backlog after arrivals stop.
                let counted = sim.now <= scenario.seconds;
                match event {
                    Event::Arrive(node) => {
                        let now = sim.at(sim.now);
                        if sim.nodes[node].waiters.is_empty()
                            && sim.nodes[node].state.try_reserve(now)
                        {
                            sim.admit(node);
                        } else {
                            sim.seq += 1;
                            let waiter = sim.seq;
                            sim.waiting.insert(waiter);
                            sim.nodes[node].waiters.push_back(waiter);
                            sim.push(sim.now + scenario.queue_timeout, Event::WaitTimeout(waiter));
                        }
                    }
                    Event::WaitTimeout(waiter) => {
                        if sim.waiting.remove(&waiter) && counted {
                            sim.refused += 1;
                        }
                    }
                    Event::Verdict(node, claim) => {
                        let admitted = sim.admitted_at.remove(&claim).unwrap_or(sim.now);
                        if counted {
                            sim.latencies.push(sim.now - admitted);
                            sim.ratified += 1;
                        }
                        sim.settle(node, claim, Feedback::Verdict);
                    }
                    Event::Expire(node, claim) => {
                        sim.admitted_at.remove(&claim);
                        if counted {
                            sim.killed += 1;
                        }
                        sim.settle(node, claim, Feedback::Failed);
                    }
                }
            }
            sim.latencies.sort_by(f64::total_cmp);
            let quantile = |q: f64| {
                sim.latencies
                    .get(((sim.latencies.len().max(1) - 1) as f64 * q) as usize)
                    .copied()
                    .unwrap_or(f64::NAN)
            };
            let per_second = |count: u64| count as f64 / scenario.seconds;
            Outcome {
                offered: per_second(offered),
                goodput: per_second(sim.ratified),
                killed_after_boot: per_second(sim.killed),
                refused_before_boot: per_second(sim.refused),
                p50: quantile(0.5),
                p99: quantile(0.99),
            }
        }
    }

    #[tokio::test]
    async fn a_waiting_start_takes_the_slot_a_verdict_frees_test() {
        let window = Arc::new(ClaimWindow::new(1, 0.5, Duration::from_secs(5)));
        window.acquire().await.expect("room").bind("a", LEASE);
        let waiter = {
            let window = window.clone();
            tokio::spawn(async move { window.acquire().await.map(|r| r.bind("b", LEASE)) })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        window.settle("a", Feedback::Verdict);
        assert!(
            waiter.await.expect("waiter").is_ok(),
            "admitted on the verdict"
        );
        assert_eq!(window.snapshot().held, 1);
    }
}
