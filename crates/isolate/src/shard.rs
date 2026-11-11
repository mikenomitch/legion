//! Shards: one pinned thread each, running a single-threaded executor that
//! serves runnable invocations by deficit round robin across tenants
//! (plan §6.3, §6.6).
//!
//! - **Flows.** Every task belongs to a flow keyed by (job, task, tenant
//!   key). Runnable tasks wait in their flow's FIFO; the shard visits active
//!   flows round robin.
//! - **Deficits.** A flow's visit adds `quantum × weight` to its deficit;
//!   the flow then runs turns (one poll of one task each) while its deficit
//!   is positive, and each turn is charged the CPU it actually used,
//!   measured with the thread's CPU clock. A flow that overdraws carries the
//!   debt into its next visit, also when its queue empties, so a tenant
//!   cannot reset its debt by going idle for a moment. A tenant with 10,000
//!   runnable invocations gets the same share as one with ten.
//! - **Turns.** Long guest turns are cut by the epoch deadline, which yields
//!   (`UpdateDeadline::Yield`) back here; the task wakes itself and goes to
//!   the back of its flow's queue.
//! - **Timers and I/O** belong to the host's tokio runtime: shard threads
//!   enter its handle, so `tokio::time` and channels work in tasks, and a
//!   wake from any thread puts the task back on its shard.
//!
//! Nothing on a shard thread blocks except the wait for work.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::guest::Invocation;
use crate::turn;

/// (job id, task name, tenant key).
pub type FlowKey = (String, String, String);

type BoxFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Flows idle with neither debt nor credit are forgotten; past this many
/// remembered flows, debts are forgiven to bound memory.
const MAX_REMEMBERED_FLOWS: usize = 100_000;

struct Task {
    flow: FlowKey,
    weight: u32,
    fut: Mutex<Option<BoxFut>>,
    queued: AtomicBool,
    meter: Option<Arc<Invocation>>,
    shard: Weak<ShardInner>,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            if let Some(s) = self.shard.upgrade() {
                s.push(self.clone());
            }
        }
    }
}

struct ShardInner {
    id: usize,
    incoming: Mutex<Vec<Arc<Task>>>,
    cv: Condvar,
    /// Tasks spawned and not finished.
    live: AtomicUsize,
    stop: AtomicBool,
    quantum_ns: u64,
    /// Counters for stats.
    turns: AtomicU64,
    cpu_ns: AtomicU64,
}

impl ShardInner {
    fn push(&self, t: Arc<Task>) {
        self.incoming.lock().unwrap().push(t);
        self.cv.notify_one();
    }
}

/// A handle to one shard.
#[derive(Clone)]
pub struct Shard {
    inner: Arc<ShardInner>,
}

/// What to know about a shard from outside.
#[derive(Debug, Clone, Copy)]
pub struct ShardStats {
    pub live: usize,
    pub turns: u64,
    pub cpu_ns: u64,
}

impl Shard {
    /// Start a shard thread. `cpu` pins it; `rt` is entered for timers.
    pub fn start(
        id: usize,
        cpu: Option<usize>,
        quantum: Duration,
        rt: tokio::runtime::Handle,
    ) -> std::io::Result<Self> {
        let inner = Arc::new(ShardInner {
            id,
            incoming: Mutex::new(Vec::new()),
            cv: Condvar::new(),
            live: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            quantum_ns: quantum.as_nanos().max(1) as u64,
            turns: AtomicU64::new(0),
            cpu_ns: AtomicU64::new(0),
        });
        let run = inner.clone();
        std::thread::Builder::new()
            .name(format!("isolate-shard-{id}"))
            .stack_size(1 << 20)
            .spawn(move || {
                if let Some(cpu) = cpu {
                    pin_to(cpu);
                }
                let _rt = rt.enter();
                run_loop(&run);
            })?;
        Ok(Self { inner })
    }

    pub fn id(&self) -> usize {
        self.inner.id
    }

    pub fn stats(&self) -> ShardStats {
        ShardStats {
            live: self.inner.live.load(Ordering::Relaxed),
            turns: self.inner.turns.load(Ordering::Relaxed),
            cpu_ns: self.inner.cpu_ns.load(Ordering::Relaxed),
        }
    }

    /// Run `fut` on this shard in `flow`, charging CPU to `meter` when
    /// given. The receiver gets the output; dropping it does not cancel.
    pub fn spawn<F, T>(
        &self,
        flow: FlowKey,
        weight: u32,
        meter: Option<Arc<Invocation>>,
        fut: F,
    ) -> oneshot::Receiver<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let fut: BoxFut = Box::pin(async move {
            let _ = tx.send(fut.await);
        });
        let task = Arc::new(Task {
            flow,
            weight: weight.max(1),
            fut: Mutex::new(Some(fut)),
            queued: AtomicBool::new(true),
            meter,
            shard: Arc::downgrade(&self.inner),
        });
        self.inner.live.fetch_add(1, Ordering::Relaxed);
        self.inner.push(task);
        rx
    }

    /// Stop the thread once it has no runnable work. Tasks still waiting
    /// are dropped.
    pub fn shutdown(&self) {
        self.inner.stop.store(true, Ordering::Release);
        self.inner.cv.notify_all();
    }
}

impl Drop for ShardInner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

fn pin_to(cpu: usize) {
    // SAFETY: a zeroed cpu_set_t is valid; CPU_SET bounds-checks nothing,
    // so the index is checked against the set's size first.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if cpu < 8 * std::mem::size_of::<libc::cpu_set_t>() {
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
}

/// The CPUs this process may run on.
pub fn allowed_cpus() -> Vec<usize> {
    // SAFETY: as above; sched_getaffinity fills the set.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return (0..std::thread::available_parallelism().map_or(1, |n| n.get())).collect();
        }
        (0..8 * std::mem::size_of::<libc::cpu_set_t>())
            .filter(|&c| libc::CPU_ISSET(c, &set))
            .collect()
    }
}

#[derive(Default)]
struct Flow {
    queue: VecDeque<Arc<Task>>,
    deficit: i64,
    weight: u32,
    active: bool,
}

struct Sched {
    flows: HashMap<FlowKey, Flow>,
    active: VecDeque<FlowKey>,
}

impl Sched {
    fn enqueue(&mut self, t: Arc<Task>) {
        let flow = self.flows.entry(t.flow.clone()).or_default();
        flow.weight = t.weight;
        flow.queue.push_back(t.clone());
        if !flow.active {
            flow.active = true;
            self.active.push_back(t.flow.clone());
        }
    }
}

fn run_loop(inner: &Arc<ShardInner>) {
    let mut s = Sched {
        flows: HashMap::new(),
        active: VecDeque::new(),
    };
    loop {
        // Take what arrived; wait when there is nothing to run.
        {
            let mut inc = inner.incoming.lock().unwrap();
            while inc.is_empty() && s.active.is_empty() {
                if inner.stop.load(Ordering::Acquire) {
                    return;
                }
                inc = inner.cv.wait(inc).unwrap();
            }
            for t in inc.drain(..) {
                s.enqueue(t);
            }
        }
        if inner.stop.load(Ordering::Acquire) && s.active.is_empty() {
            return;
        }
        let Some(key) = s.active.pop_front() else {
            continue;
        };
        let Some(mut flow) = s.flows.remove(&key) else {
            continue;
        };
        flow.deficit += (inner.quantum_ns * flow.weight as u64) as i64;
        while flow.deficit > 0 {
            let Some(task) = flow.queue.pop_front() else {
                break;
            };
            let used = poll_task(inner, &task);
            flow.deficit -= used as i64;
            // Wakes during the turn (a yielding guest wakes itself) go to
            // the back of their flow's queue.
            let arrived: Vec<_> = inner.incoming.lock().unwrap().drain(..).collect();
            for t in arrived {
                if t.flow == key {
                    flow.queue.push_back(t);
                } else {
                    s.enqueue(t);
                }
            }
        }
        if flow.queue.is_empty() {
            flow.active = false;
            // Keep debt, forget credit.
            flow.deficit = flow.deficit.min(0);
            if flow.deficit < 0 && s.flows.len() < MAX_REMEMBERED_FLOWS {
                s.flows.insert(key, flow);
            }
        } else {
            s.active.push_back(key.clone());
            s.flows.insert(key, flow);
        }
    }
}

/// Run one turn of `task`; returns the CPU it used.
fn poll_task(inner: &Arc<ShardInner>, task: &Arc<Task>) -> u64 {
    task.queued.store(false, Ordering::Release);
    let mut slot = task.fut.lock().unwrap();
    let Some(fut) = slot.as_mut() else {
        return 0;
    };
    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    let cpu0 = turn::begin();
    let done = fut.as_mut().poll(&mut cx).is_ready();
    let used = turn::end(cpu0);
    if let Some(m) = &task.meter {
        m.cpu_ns.fetch_add(used, Ordering::Relaxed);
        m.turns.fetch_add(1, Ordering::Relaxed);
    }
    inner.turns.fetch_add(1, Ordering::Relaxed);
    inner.cpu_ns.fetch_add(used, Ordering::Relaxed);
    if done {
        *slot = None;
        inner.live.fetch_sub(1, Ordering::Relaxed);
    }
    used
}

/// Give the shard back once (a cooperative yield for host-side loops).
pub async fn yield_now() {
    struct Y(bool);
    impl Future for Y {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    Y(false).await
}

#[cfg(test)]
mod shard_test {
    use super::*;
    use std::time::Instant;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spin_for(d: Duration) {
        let start = turn::thread_cpu_ns();
        while turn::thread_cpu_ns() - start < d.as_nanos() as u64 {
            std::hint::spin_loop();
        }
    }

    /// Run `n` tasks per flow, each spinning 200µs per turn until `stop`;
    /// returns CPU per flow.
    fn share(flows: &[(&str, u32, usize)]) -> Vec<f64> {
        let rt = rt();
        let shard = Shard::start(0, None, Duration::from_millis(2), rt.handle().clone()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut meters = Vec::new();
        let mut done = Vec::new();
        for (tenant, weight, n) in flows {
            let used = Arc::new(AtomicU64::new(0));
            for _ in 0..*n {
                let stop = stop.clone();
                let used = used.clone();
                done.push(shard.spawn(
                    ("j".into(), "t".into(), tenant.to_string()),
                    *weight,
                    None,
                    async move {
                        while !stop.load(Ordering::Relaxed) {
                            let t0 = turn::thread_cpu_ns();
                            spin_for(Duration::from_micros(200));
                            used.fetch_add(turn::thread_cpu_ns() - t0, Ordering::Relaxed);
                            yield_now().await;
                        }
                    },
                ));
            }
            meters.push(used);
        }
        std::thread::sleep(Duration::from_millis(1500));
        stop.store(true, Ordering::Relaxed);
        for d in done {
            rt.block_on(d).unwrap();
        }
        shard.shutdown();
        let total: u64 = meters.iter().map(|m| m.load(Ordering::Relaxed)).sum();
        meters
            .iter()
            .map(|m| m.load(Ordering::Relaxed) as f64 / total as f64)
            .collect()
    }

    #[test]
    fn drr_shares_cpu_per_tenant_not_per_invocation_test() {
        // 200 runnable invocations against 2: equal weights, equal shares.
        let s = share(&[("many", 1, 200), ("few", 1, 2)]);
        assert!((s[0] - 0.5).abs() < 0.05, "shares {s:?}");
    }

    #[test]
    fn drr_shares_follow_weights_test() {
        let s = share(&[("heavy", 2, 3), ("light", 1, 30)]);
        assert!((s[0] - 2.0 / 3.0).abs() < 0.05, "shares {s:?}");
    }

    #[test]
    fn a_spinning_tenant_does_not_starve_a_short_one_test() {
        let rt = rt();
        let shard = Shard::start(0, None, Duration::from_millis(2), rt.handle().clone()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        for _ in 0..50 {
            let stop = stop.clone();
            let _ = shard.spawn(("j".into(), "t".into(), "hog".into()), 1, None, async move {
                while !stop.load(Ordering::Relaxed) {
                    spin_for(Duration::from_micros(500));
                    yield_now().await;
                }
            });
        }
        std::thread::sleep(Duration::from_millis(50));
        let t0 = Instant::now();
        let rx = shard.spawn(("j".into(), "t".into(), "small".into()), 1, None, async {});
        rt.block_on(rx).unwrap();
        let waited = t0.elapsed();
        stop.store(true, Ordering::Relaxed);
        shard.shutdown();
        // One quantum of the hog's flow at most, plus one turn.
        assert!(waited < Duration::from_millis(20), "waited {waited:?}");
    }
}
