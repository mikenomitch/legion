//! Elastic session envelopes, the node's half (elastic-session-envelopes
//! plan §6.2 to §6.4; docs/session-mode.md, "Elastic envelopes").
//!
//! An envelope whose session task sets `stateful.envelope.max` sizes itself
//! between its floor (`envelope.resources`) and that ceiling. The node
//! decides when, because it alone knows what its sessions hold at every
//! instant: at each heartbeat [`EnvelopeSize::tick`] reads the active
//! sessions' usage and returns at most one resize to ask the manager for.
//! The manager applies it as one scheduler-lane write and answers on the
//! same heartbeat ([`EnvelopeSize::answer`]).
//!
//! - **Grow** when the sessions fill more than [`GROW_AT_FILL`] of the
//!   envelope on any key, or one more session would not fit.
//! - **Shrink** when they fill less than [`SHRINK_BELOW_FILL`] on every key
//!   for `envelope.shrinkAfter`.
//! - Either way the new size is the one the sessions would fill to
//!   [`TARGET_FILL`], with room for one more session, clamped to the bounds.
//!   A grow never lowers a key, and a shrink never raises one.
//!
//! **A shrink never strands a session.** While a shrink is in flight,
//! [`EnvelopeSize::room`] is capped at its target, so admission cannot pass
//! it, and the target was computed from usage the caller read under the
//! same lock that admits. When the shrink commits, usage is at or below it.
//!
//! Usage here is **active sessions only**. Suspended sessions hold no room,
//! and anything reclaimable the node keeps inside an envelope (a warm pool)
//! must not count, or it would grow the envelope that makes room for more
//! of itself.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::structs::session::ElasticBounds;

/// Grow when the sessions fill more than this of the envelope, on any key.
pub const GROW_AT_FILL: f64 = 0.9;
/// A resize sizes the envelope so its sessions fill this much of it.
pub const TARGET_FILL: f64 = 0.75;
/// Shrink once the sessions fill less than this, on every key, for
/// `envelope.shrinkAfter`.
pub const SHRINK_BELOW_FILL: f64 = 0.5;
/// After a refused grow (the node is full, or not taking work), wait this
/// long before asking again. A refusal writes nothing, but each ask is a
/// scheduler-lane op on the leader.
pub const GROW_REFUSED_BACKOFF: Duration = Duration::from_secs(30);
/// After any answer, the node trusts its own record of the size over a
/// desired-set payload's for this long: a follower can answer from a
/// replica that has not applied the resize yet, and its payload would carry
/// the old size.
pub const PAYLOAD_TRUST_WINDOW: Duration = Duration::from_secs(60);

/// Resource amounts by key, as `resources` objects hold them.
pub type Amounts = BTreeMap<String, f64>;

/// Which way a resize goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Grow,
    Shrink,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Grow => "grow",
            Direction::Shrink => "shrink",
        }
    }
}

/// A resize the node has asked for and the manager has not answered.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingResize {
    pub from: Amounts,
    pub to: Amounts,
    pub direction: Direction,
}

/// One envelope's size as this node knows it, and its controller state.
#[derive(Debug, Clone)]
pub struct EnvelopeSize {
    /// The committed size, as of the last payload or answer.
    size: Amounts,
    /// The resize in flight, re-sent on every heartbeat until answered.
    pending: Option<PendingResize>,
    /// Since when the sessions have filled less than [`SHRINK_BELOW_FILL`].
    low_since: Option<Instant>,
    /// No grow is asked for before this.
    grow_backoff_until: Option<Instant>,
    /// Payload sizes are ignored before this.
    trust_payload_after: Option<Instant>,
}

impl EnvelopeSize {
    pub fn new(size: Amounts) -> Self {
        EnvelopeSize {
            size,
            pending: None,
            low_since: None,
            grow_backoff_until: None,
            trust_payload_after: None,
        }
    }

    /// The committed size.
    pub fn size(&self) -> &Amounts {
        &self.size
    }

    /// The resize in flight, if any.
    pub fn pending(&self) -> Option<&PendingResize> {
        self.pending.as_ref()
    }

    /// The room sessions are admitted against: the committed size, capped
    /// on every key by a shrink in flight.
    pub fn room(&self) -> Amounts {
        let mut room = self.size.clone();
        if let Some(PendingResize {
            to,
            direction: Direction::Shrink,
            ..
        }) = &self.pending
        {
            for (key, amount) in room.iter_mut() {
                if let Some(cap) = to.get(key) {
                    *amount = amount.min(*cap);
                }
            }
        }
        room
    }

    /// The size a desired-set payload carries. Taken unless a resize is in
    /// flight (its answer is authoritative) or one was answered within
    /// [`PAYLOAD_TRUST_WINDOW`] (the payload may predate it).
    pub fn observe_payload(&mut self, size: Amounts, now: Instant) {
        if size.is_empty() || self.pending.is_some() {
            return;
        }
        if self.trust_payload_after.is_some_and(|after| now < after) {
            return;
        }
        self.size = size;
    }

    /// This heartbeat's resize: the one in flight again, a new one, or none.
    ///
    /// `used` is what the envelope's **active** sessions hold, and `session`
    /// one session's resources (keys the envelope does not size are
    /// ignored). Call it under the lock that admits sessions, so a shrink's
    /// cap is in place before another admission can read the room.
    pub fn tick(
        &mut self,
        bounds: &ElasticBounds,
        used: &Amounts,
        session: &Amounts,
        now: Instant,
    ) -> Option<PendingResize> {
        if let Some(pending) = &self.pending {
            return Some(pending.clone());
        }
        let keys: Vec<&String> = bounds.floor.keys().collect();
        let used_of = |key: &str| used.get(key).copied().unwrap_or(0.0);
        let session_of = |key: &str| session.get(key).copied().unwrap_or(0.0);
        let size_of = |key: &str| self.size.get(key).copied().unwrap_or(0.0);
        let fill = keys
            .iter()
            .map(|key| {
                let (used, size) = (used_of(key), size_of(key));
                if size > 0.0 {
                    used / size
                } else if used > 0.0 {
                    f64::INFINITY
                } else {
                    0.0
                }
            })
            .fold(0.0, f64::max);
        let fits_one_more = keys
            .iter()
            .all(|key| used_of(key) + session_of(key) <= size_of(key) + 1e-9);
        // Where a resize lands: the sessions at TARGET_FILL, with room for
        // one more, within the bounds. Whole units, rounded up.
        let target: Amounts = keys
            .iter()
            .map(|key| {
                let floor = bounds.floor[*key];
                let ceiling = bounds.max.get(*key).copied().unwrap_or(floor);
                let wanted = (used_of(key) / TARGET_FILL)
                    .max(used_of(key) + session_of(key))
                    .ceil();
                ((*key).clone(), wanted.clamp(floor, ceiling))
            })
            .collect();

        if fill > GROW_AT_FILL || !fits_one_more {
            self.low_since = None;
            if self.grow_backoff_until.is_some_and(|until| now < until) {
                return None;
            }
            let to: Amounts = target
                .iter()
                .map(|(key, amount)| (key.clone(), amount.max(size_of(key))))
                .collect();
            return self.ask(to, Direction::Grow);
        }
        if fill < SHRINK_BELOW_FILL {
            let since = *self.low_since.get_or_insert(now);
            if now.duration_since(since) < bounds.shrink_after {
                return None;
            }
            self.low_since = None;
            let to: Amounts = target
                .iter()
                .map(|(key, amount)| (key.clone(), amount.min(size_of(key))))
                .collect();
            return self.ask(to, Direction::Shrink);
        }
        self.low_since = None;
        None
    }

    fn ask(&mut self, to: Amounts, direction: Direction) -> Option<PendingResize> {
        let unchanged = to.iter().all(|(key, amount)| {
            (self.size.get(key).copied().unwrap_or(0.0) - amount).abs() < 1e-9
        });
        if unchanged {
            return None;
        }
        let pending = PendingResize {
            from: self.size.clone(),
            to,
            direction,
        };
        self.pending = Some(pending.clone());
        Some(pending)
    }

    /// The manager's answer to the resize in flight
    /// (`EnvelopeResizeResult.outcome`, with its committed `size` when it
    /// carried one). An `error` keeps the request in flight, so it is asked
    /// again on the next heartbeat; every other answer settles it.
    pub fn answer(&mut self, outcome: &str, size: Option<Amounts>, now: Instant) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        match outcome {
            "error" => {
                self.pending = Some(pending);
                return;
            }
            "applied" => self.size = size.unwrap_or_else(|| pending.to.clone()),
            "partial" | "stale" => {
                if let Some(size) = size {
                    self.size = size;
                }
            }
            "refused" => {
                if let Some(size) = size {
                    self.size = size;
                }
                self.grow_backoff_until = Some(now + GROW_REFUSED_BACKOFF);
            }
            // `gone`, `invalid`: nothing to adopt. Back off so a node whose
            // spec disagrees with the manager's does not ask every beat.
            _ => self.grow_backoff_until = Some(now + GROW_REFUSED_BACKOFF),
        }
        self.trust_payload_after = Some(now + PAYLOAD_TRUST_WINDOW);
    }
}

#[cfg(test)]
mod envelope_elastic_test {
    use super::*;

    fn amounts(cpu: f64, memory: f64) -> Amounts {
        Amounts::from([("cpu".to_string(), cpu), ("memory".to_string(), memory)])
    }

    /// Floor 2 sessions, ceiling 20, of 500 MHz / 512 MiB each.
    fn bounds() -> ElasticBounds {
        ElasticBounds {
            floor: amounts(1000.0, 1024.0),
            max: amounts(10000.0, 10240.0),
            shrink_after: Duration::from_secs(600),
        }
    }

    fn session() -> Amounts {
        amounts(500.0, 512.0)
    }

    fn sessions(n: f64) -> Amounts {
        amounts(500.0 * n, 512.0 * n)
    }

    #[test]
    fn a_full_envelope_asks_to_grow_to_three_quarters_full_test() {
        let now = Instant::now();
        let mut envelope = EnvelopeSize::new(amounts(4000.0, 4096.0));
        // 6 of 8 sessions: 75% full, nothing to do.
        assert_eq!(
            envelope.tick(&bounds(), &sessions(6.0), &session(), now),
            None
        );
        // 8 of 8: one more does not fit. 8 sessions at 75% is 10.67.
        let grow = envelope
            .tick(&bounds(), &sessions(8.0), &session(), now)
            .expect("grow");
        assert_eq!(grow.direction, Direction::Grow);
        assert_eq!(grow.from, amounts(4000.0, 4096.0));
        assert_eq!(grow.to, amounts(5334.0, 5462.0));
        // In flight: asked again, unchanged, until answered.
        assert_eq!(
            envelope.tick(&bounds(), &sessions(1.0), &session(), now),
            Some(grow.clone())
        );
        envelope.answer("applied", Some(grow.to.clone()), now);
        assert_eq!(envelope.size(), &grow.to);
        assert_eq!(envelope.pending(), None);
    }

    #[test]
    fn a_grow_stops_at_the_ceiling_and_keeps_room_for_one_session_test() {
        let now = Instant::now();
        // 1000 MHz holds 2; 2 running fill it.
        let mut envelope = EnvelopeSize::new(amounts(1000.0, 1024.0));
        let grow = envelope
            .tick(&bounds(), &sessions(2.0), &session(), now)
            .expect("grow");
        // 2 / 0.75 = 2.67 sessions, and at least 3.
        assert_eq!(grow.to, amounts(1500.0, 1536.0));
        envelope.answer("applied", None, now);

        let mut envelope = EnvelopeSize::new(amounts(9500.0, 9728.0));
        let grow = envelope
            .tick(&bounds(), &sessions(19.0), &session(), now)
            .expect("grow");
        assert_eq!(grow.to, bounds().max, "clamped to the ceiling");
        envelope.answer("applied", None, now);
        assert_eq!(
            envelope.tick(&bounds(), &sessions(20.0), &session(), now),
            None,
            "at the ceiling, nothing to ask"
        );
    }

    #[test]
    fn a_shrink_waits_for_shrink_after_and_caps_admission_test() {
        let start = Instant::now();
        let mut envelope = EnvelopeSize::new(amounts(10000.0, 10240.0));
        // 4 of 20: 20% full, but not for long enough.
        assert_eq!(
            envelope.tick(&bounds(), &sessions(4.0), &session(), start),
            None
        );
        let later = start + Duration::from_secs(599);
        assert_eq!(
            envelope.tick(&bounds(), &sessions(4.0), &session(), later),
            None
        );
        let due = start + Duration::from_secs(600);
        // A moment above half resets the clock.
        let blip = start + Duration::from_secs(300);
        let mut reset = envelope.clone();
        assert_eq!(
            reset.tick(&bounds(), &sessions(12.0), &session(), blip),
            None
        );
        assert_eq!(
            reset.tick(&bounds(), &sessions(4.0), &session(), due),
            None,
            "the low period restarted"
        );

        let shrink = envelope
            .tick(&bounds(), &sessions(4.0), &session(), due)
            .expect("shrink");
        assert_eq!(shrink.direction, Direction::Shrink);
        // 4 / 0.75 = 5.33 sessions.
        assert_eq!(shrink.to, amounts(2667.0, 2731.0));
        // Admission sees the target while the shrink is in flight.
        assert_eq!(envelope.room(), shrink.to);
        assert_eq!(envelope.size(), &amounts(10000.0, 10240.0));
        envelope.answer("applied", None, due);
        assert_eq!(envelope.room(), shrink.to);
    }

    #[test]
    fn a_shrink_never_goes_below_the_floor_or_the_sessions_test() {
        let start = Instant::now();
        let mut envelope = EnvelopeSize::new(amounts(3000.0, 3072.0));
        let due = start + Duration::from_secs(600);
        assert_eq!(
            envelope.tick(&bounds(), &sessions(0.0), &session(), start),
            None
        );
        let shrink = envelope
            .tick(&bounds(), &sessions(0.0), &session(), due)
            .expect("shrink");
        assert_eq!(shrink.to, bounds().floor);
        envelope.answer("applied", None, due);
        // At the floor and empty: nothing more to give back.
        let much_later = due + Duration::from_secs(3600);
        assert_eq!(
            envelope.tick(&bounds(), &sessions(0.0), &session(), due),
            None
        );
        assert_eq!(
            envelope.tick(&bounds(), &sessions(0.0), &session(), much_later),
            None
        );
    }

    #[test]
    fn a_refused_grow_backs_off_and_a_stale_answer_adopts_the_size_test() {
        let now = Instant::now();
        let mut envelope = EnvelopeSize::new(amounts(1000.0, 1024.0));
        envelope
            .tick(&bounds(), &sessions(2.0), &session(), now)
            .expect("grow");
        envelope.answer("refused", Some(amounts(1000.0, 1024.0)), now);
        assert_eq!(
            envelope.tick(&bounds(), &sessions(2.0), &session(), now),
            None
        );
        let after = now + GROW_REFUSED_BACKOFF;
        let again = envelope
            .tick(&bounds(), &sessions(2.0), &session(), after)
            .expect("asks again after the backoff");
        // The row held something else: adopt it, and the next tick decides
        // from there.
        envelope.answer("stale", Some(amounts(4000.0, 4096.0)), after);
        assert_eq!(envelope.size(), &amounts(4000.0, 4096.0));
        assert_ne!(envelope.pending(), Some(&again));
        assert_eq!(
            envelope.tick(&bounds(), &sessions(2.0), &session(), after),
            None
        );
    }

    #[test]
    fn an_error_keeps_the_resize_and_its_cap_in_flight_test() {
        let start = Instant::now();
        let due = start + Duration::from_secs(600);
        let mut envelope = EnvelopeSize::new(amounts(10000.0, 10240.0));
        envelope.tick(&bounds(), &sessions(1.0), &session(), start);
        let shrink = envelope
            .tick(&bounds(), &sessions(1.0), &session(), due)
            .expect("shrink");
        envelope.answer("error", None, due);
        assert_eq!(envelope.pending(), Some(&shrink));
        assert_eq!(envelope.room(), shrink.to, "the cap holds until answered");
        // Gone lifts it.
        envelope.answer("gone", None, due);
        assert_eq!(envelope.pending(), None);
        assert_eq!(envelope.room(), amounts(10000.0, 10240.0));
    }

    #[test]
    fn a_payload_inside_the_trust_window_is_ignored_test() {
        let now = Instant::now();
        let mut envelope = EnvelopeSize::new(amounts(4000.0, 4096.0));
        envelope.observe_payload(amounts(5000.0, 5120.0), now);
        assert_eq!(envelope.size(), &amounts(5000.0, 5120.0), "no resize yet");
        let shrink_to = amounts(2000.0, 2048.0);
        envelope.pending = Some(PendingResize {
            from: amounts(5000.0, 5120.0),
            to: shrink_to.clone(),
            direction: Direction::Shrink,
        });
        envelope.observe_payload(amounts(5000.0, 5120.0), now);
        envelope.answer("applied", None, now);
        // A lagging replica's desired set still says 5000.
        envelope.observe_payload(amounts(5000.0, 5120.0), now + Duration::from_secs(5));
        assert_eq!(envelope.size(), &shrink_to);
        envelope.observe_payload(amounts(3000.0, 3072.0), now + PAYLOAD_TRUST_WINDOW);
        assert_eq!(envelope.size(), &amounts(3000.0, 3072.0));
    }

    /// What one node's envelope did over a simulated day.
    #[derive(Debug, Default)]
    struct DayReport {
        /// Mean envelope size, in sessions.
        reserved: f64,
        /// Mean running sessions.
        used: f64,
        /// Starts refused for room.
        refused: u64,
        admitted: u64,
        grows: u64,
        shrinks: u64,
    }

    /// Knuth's Poisson sampler: fine for the small means here.
    fn poisson(rng: &mut impl rand::Rng, mean: f64) -> u64 {
        let limit = (-mean).exp();
        let mut product = 1.0;
        let mut count = 0;
        loop {
            product *= rng.gen::<f64>();
            if product <= limit {
                return count;
            }
            count += 1;
        }
    }

    /// One node under a diurnal load for two days, measured on the second,
    /// with the shipped controller deciding at every 2-second heartbeat and
    /// the manager answering at once (every grow fits). Sessions are 500 MHz
    /// / 512 MiB; the load peaks at 54 concurrent sessions in the afternoon
    /// and bottoms out at a quarter of that, each session living 10 minutes
    /// on average. `elastic: None` is a fixed envelope at the ceiling.
    fn simulate_day(
        elastic: Option<ElasticBounds>,
        burst: Option<(u64, u64)>,
        seed: u64,
    ) -> DayReport {
        use rand::{Rng, SeedableRng};
        const DAY: u64 = 86_400;
        const PEAK: f64 = 54.0;
        const TROUGH_RATIO: f64 = 0.25;
        const MEAN_LIFE_S: f64 = 600.0;
        const CEILING: f64 = 60.0;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let session = session();
        let start = Instant::now();
        let mut envelope = EnvelopeSize::new(match &elastic {
            Some(bounds) => bounds.floor.clone(),
            None => sessions(CEILING),
        });
        let mut running = 0u64;
        let mut report = DayReport::default();
        let (mut reserved_sum, mut used_sum) = (0.0, 0.0);
        for second in 0..2 * DAY {
            let measured = second >= DAY;
            let phase = (2.0 * std::f64::consts::PI * second as f64 / DAY as f64
                - std::f64::consts::FRAC_PI_2)
                .sin();
            let concurrency = PEAK * (TROUGH_RATIO + (1.0 - TROUGH_RATIO) * (phase + 1.0) / 2.0);
            let mut arrivals = poisson(&mut rng, concurrency / MEAN_LIFE_S);
            if let Some((at, extra)) = burst {
                if second % DAY == at {
                    arrivals += extra;
                }
            }
            let ended = (0..running)
                .filter(|_| rng.gen::<f64>() < 1.0 / MEAN_LIFE_S)
                .count() as u64;
            running -= ended;
            let capacity = envelope.room()["cpu"] / session["cpu"];
            for _ in 0..arrivals {
                if (running + 1) as f64 <= capacity + 1e-9 {
                    running += 1;
                    report.admitted += u64::from(measured);
                } else {
                    report.refused += u64::from(measured);
                }
            }
            if measured {
                reserved_sum += envelope.size()["cpu"] / session["cpu"];
                used_sum += running as f64;
            }
            let Some(bounds) = &elastic else {
                continue;
            };
            if second % 2 == 0 {
                let now = start + Duration::from_secs(second);
                if let Some(resize) =
                    envelope.tick(bounds, &sessions(running as f64), &session, now)
                {
                    if measured {
                        match resize.direction {
                            Direction::Grow => report.grows += 1,
                            Direction::Shrink => report.shrinks += 1,
                        }
                    }
                    envelope.answer("applied", Some(resize.to), now);
                }
            }
        }
        report.reserved = reserved_sum / DAY as f64;
        report.used = used_sum / DAY as f64;
        report
    }

    /// Floor 12 sessions, ceiling 60, the default `shrinkAfter`.
    fn diurnal_bounds() -> ElasticBounds {
        ElasticBounds {
            floor: sessions(12.0),
            max: sessions(60.0),
            shrink_after: crate::structs::session::DEFAULT_SHRINK_AFTER,
        }
    }

    /// Elastic-session-envelopes plan §9: under a smooth diurnal load the
    /// elastic envelope strands far less room than a fixed one at the
    /// ceiling, refuses no more starts, and resizes a few dozen times a day.
    #[test]
    fn an_elastic_envelope_strands_less_room_than_a_fixed_one_over_a_day_test() {
        let fixed = simulate_day(None, None, 7);
        let elastic = simulate_day(Some(diurnal_bounds()), None, 7);
        let stranded = |day: &DayReport| day.reserved - day.used;
        assert!(
            stranded(&elastic) < 0.7 * stranded(&fixed),
            "fixed {fixed:?}, elastic {elastic:?}"
        );
        assert!(
            elastic.refused <= fixed.refused + fixed.refused / 10 + 5,
            "fixed {fixed:?}, elastic {elastic:?}"
        );
        assert!(
            elastic.grows + elastic.shrinks < 100,
            "resizes a day: {elastic:?}"
        );
    }

    /// The numbers the plan and the PR quote, over several seeds and with
    /// an off-peak burst. `cargo test --lib -- --ignored
    /// elastic_envelope_diurnal_measure --nocapture`.
    #[test]
    #[ignore]
    fn elastic_envelope_diurnal_measure() {
        for (label, burst) in [
            ("smooth", None),
            ("burst of 30 at 03:00", Some((3 * 3600, 30))),
        ] {
            for seed in 1..=5 {
                let fixed = simulate_day(None, burst, seed);
                let elastic = simulate_day(Some(diurnal_bounds()), burst, seed);
                for (name, day) in [("fixed at 60", &fixed), ("elastic 12..60", &elastic)] {
                    println!(
                        "{label:<22} seed {seed} {name:<15} reserved {:>5.1} used {:>5.1} \
                         stranded {:>5.1} refused {:>4} of {:>5} grows {:>3} shrinks {:>3}",
                        day.reserved,
                        day.used,
                        day.reserved - day.used,
                        day.refused,
                        day.refused + day.admitted,
                        day.grows,
                        day.shrinks
                    );
                }
            }
        }
        for minutes in [2, 5, 10, 30] {
            let bounds = ElasticBounds {
                shrink_after: Duration::from_secs(60 * minutes),
                ..diurnal_bounds()
            };
            let day = simulate_day(Some(bounds), None, 1);
            println!(
                "shrinkAfter {minutes:>2}m: reserved {:>5.1} stranded {:>5.1} refused {:>4} \
                 resizes {:>3}",
                day.reserved,
                day.reserved - day.used,
                day.refused,
                day.grows + day.shrinks
            );
        }
    }
}
