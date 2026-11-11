//! What the shard executor tells the code it polls about the current turn.
//!
//! A turn is one poll of one invocation on a shard thread. Before each poll
//! the executor records the thread's CPU clock and the time, so that:
//! - an invocation's CPU use can be read mid-turn (the epoch callback checks
//!   the CPU budget against `charged + (now - turn start)`);
//! - the guest's clocks are frozen within a turn: both read the turn's
//!   start, coarsened to 1 ms, and advance only when the guest gives control
//!   back (an await, or an epoch yield). Plan §6.9, §6.10 layer 4.

use std::cell::Cell;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Clock resolution offered to guests.
pub const CLOCK_RESOLUTION: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug)]
struct TurnState {
    cpu_start_ns: u64,
    wall: Duration,
    mono_ns: u64,
}

thread_local! {
    static TURN: Cell<Option<TurnState>> = const { Cell::new(None) };
}

/// The calling thread's CPU time, in nanoseconds.
pub fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid pointer to a timespec; CLOCK_THREAD_CPUTIME_ID always
    // exists on Linux.
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn coarse(d: Duration) -> Duration {
    Duration::from_millis(d.as_millis() as u64)
}

fn mono_base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

fn real_wall() -> Duration {
    coarse(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default())
}

fn real_mono_ns() -> u64 {
    // Offset by one second so a guest never sees 0.
    let d = coarse(mono_base().elapsed()) + Duration::from_secs(1);
    d.as_nanos() as u64
}

/// Start a turn on this thread: returns the CPU clock at its start.
pub fn begin() -> u64 {
    let cpu = thread_cpu_ns();
    TURN.with(|t| {
        t.set(Some(TurnState {
            cpu_start_ns: cpu,
            wall: real_wall(),
            mono_ns: real_mono_ns(),
        }))
    });
    cpu
}

/// End the turn: returns the CPU it used.
pub fn end(cpu_start: u64) -> u64 {
    TURN.with(|t| t.set(None));
    thread_cpu_ns().saturating_sub(cpu_start)
}

/// CPU used so far in the current turn (0 off a shard).
pub fn cpu_in_turn_ns() -> u64 {
    TURN.with(|t| t.get())
        .map(|s| thread_cpu_ns().saturating_sub(s.cpu_start_ns))
        .unwrap_or(0)
}

/// The guest's wall clock: frozen in a turn, 1 ms resolution.
pub struct FrozenWallClock;

impl wasmtime_wasi::HostWallClock for FrozenWallClock {
    fn resolution(&self) -> Duration {
        CLOCK_RESOLUTION
    }
    fn now(&self) -> Duration {
        TURN.with(|t| t.get()).map(|s| s.wall).unwrap_or_else(real_wall)
    }
}

/// The guest's monotonic clock: frozen in a turn, 1 ms resolution.
pub struct FrozenMonotonicClock;

impl wasmtime_wasi::HostMonotonicClock for FrozenMonotonicClock {
    fn resolution(&self) -> u64 {
        CLOCK_RESOLUTION.as_nanos() as u64
    }
    fn now(&self) -> u64 {
        TURN.with(|t| t.get())
            .map(|s| s.mono_ns)
            .unwrap_or_else(real_mono_ns)
    }
}

#[cfg(test)]
mod turn_test {
    use super::*;
    use wasmtime_wasi::{HostMonotonicClock, HostWallClock};

    #[test]
    fn clocks_are_frozen_within_a_turn_and_coarse_test() {
        let cpu = begin();
        let w1 = FrozenWallClock.now();
        let m1 = FrozenMonotonicClock.now();
        std::thread::sleep(Duration::from_millis(3));
        assert_eq!(w1, FrozenWallClock.now());
        assert_eq!(m1, FrozenMonotonicClock.now());
        assert_eq!(w1.subsec_nanos() % 1_000_000, 0);
        assert_eq!(m1 % 1_000_000, 0);
        end(cpu);
        let cpu = begin();
        assert!(FrozenMonotonicClock.now() >= m1 + 3_000_000);
        end(cpu);
    }
}
