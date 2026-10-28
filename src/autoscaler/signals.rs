//! How raw inputs become an [`Observation`](super::Observation)'s numbers:
//! the warmup count and the averaging windows (calls entries 39 and 50).
//!
//! The live autoscaler and `legion scaling simulate` over the manager's
//! history both run these, so a simulation of the same inputs sees the
//! same ready count and the same window averages the live engine saw.
//!
//! - [`Readiness`]: a replica is *warming* for `warmup` after the ready
//!   count rises; replicas that go away take the newest rises with them.
//! - [`Windows`]: a check's reading is the mean of the readings taken inside
//!   its `window`, by the engine's clock.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

fn ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX / 4)
}

/// Warmup bookkeeping for one target.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Readiness {
    last_ready: Option<u32>,
    rises: VecDeque<(i64, u32)>,
}

impl Readiness {
    /// Record `ready` (healthy replicas) at `now_ms`; returns how many of
    /// them are still warming.
    pub fn observe(&mut self, ready: u32, now_ms: i64, warmup: Duration) -> u32 {
        if let Some(last) = self.last_ready {
            if ready > last {
                self.rises.push_back((now_ms, ready - last));
            } else if ready < last {
                // Replicas went away: the newest rises go first.
                let mut gone = last - ready;
                while gone > 0 {
                    let Some(back) = self.rises.back_mut() else {
                        break;
                    };
                    let take = back.1.min(gone);
                    back.1 -= take;
                    gone -= take;
                    if back.1 == 0 {
                        self.rises.pop_back();
                    }
                }
            }
        }
        self.last_ready = Some(ready);
        let since = now_ms - ms(warmup);
        while self.rises.front().is_some_and(|(at, _)| *at <= since) {
            self.rises.pop_front();
        }
        self.rises.iter().map(|(_, n)| *n).sum::<u32>().min(ready)
    }
}

/// Each check's readings inside its window.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Windows {
    samples: BTreeMap<String, VecDeque<(i64, f64)>>,
}

impl Windows {
    /// Add `value` for `check` at `now_ms` and return the mean over
    /// `window`.
    pub fn add(&mut self, check: &str, value: f64, now_ms: i64, window: Duration) -> f64 {
        let ring = self.samples.entry(check.to_string()).or_default();
        ring.push_back((now_ms, value));
        let since = now_ms - ms(window);
        while ring.front().is_some_and(|(at, _)| *at <= since) {
            ring.pop_front();
        }
        let (sum, n) = ring
            .iter()
            .fold((0.0, 0usize), |(sum, n), (_, value)| (sum + value, n + 1));
        if n == 0 {
            value
        } else {
            sum / n as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Replicas that became ready within `warmup` are warming; replicas
    /// that go away take the newest rises with them.
    #[test]
    fn readiness_counts_warming_replicas_test() {
        let warmup = Duration::from_secs(30);
        let mut readiness = Readiness::default();
        assert_eq!(
            readiness.observe(2, 0, warmup),
            0,
            "the first sight is not a rise"
        );
        assert_eq!(readiness.observe(4, 10_000, warmup), 2);
        assert_eq!(readiness.observe(5, 20_000, warmup), 3);
        assert_eq!(
            readiness.observe(5, 40_001, warmup),
            1,
            "the first rise warmed up"
        );
        assert_eq!(
            readiness.observe(4, 41_000, warmup),
            0,
            "the newest rise left"
        );
        assert_eq!(readiness.observe(4, 60_000, warmup), 0);
    }

    #[test]
    fn windows_average_by_the_engine_clock_test() {
        let mut windows = Windows::default();
        let window = Duration::from_secs(60);
        assert_eq!(windows.add("cpu", 0.1, 0, window), 0.1);
        assert_eq!(windows.add("cpu", 0.5, 30_000, window), 0.3);
        // The first sample left the window.
        assert_eq!(windows.add("cpu", 0.9, 60_000, window), 0.7);
        // Checks keep their own windows.
        assert_eq!(windows.add("lag", 10.0, 60_000, window), 10.0);
    }
}
