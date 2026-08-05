//! Leader-elect request coalescer (plan-path scale levers, D5).
//!
//! Batches concurrent submitters of the same kind of store write into one
//! commit: submitters enqueue a request and race for the commit slot; the
//! winner (the *batch leader*) drains whatever is queued, runs the whole
//! batch as one operation, and hands each submitter its own result.
//! Requests arriving while a commit is running form the next batch —
//! natural batching with no timer, and a lone submitter pays nothing
//! (a batch of one takes whatever fast path the batch runner provides).
//!
//! The primitive is deliberately generic (request in, result out, batch
//! function supplied by the caller); path-specific behavior — per-plan
//! savepoint isolation, re-run-singly on batch failure, deduplication —
//! belongs in the batch functions, not here.
//!
//! ## Why one lock and a condvar
//!
//! A submitter that loses the election is waiting for whichever of two
//! things happens first: its result arrives (some leader drained it), or
//! the commit slot falls free (it may lead the next batch itself). The
//! first shape of this file put those two facts behind two different
//! primitives — a reply channel and a `Mutex` — and nothing can block on
//! both at once, so a loser had to *poll*: sleep a millisecond on the
//! channel, wake, re-contend for the lock, repeat. That cost real time at
//! every commit boundary (the lock sat free until some waiter's next tick)
//! and burned a wake-up per waiter per millisecond of every commit.
//!
//! Both facts are now one state transition — a leader publishes results
//! and releases the slot in a single critical section — so both are
//! covered by a single `Mutex` + `Condvar`. A loser blocks once and is
//! woken exactly when the commit it is queued behind ends. **The wait it
//! already pays now buys it a place in the next batch instead of a ticket
//! in a lottery it usually loses.** The AWS 5,000-node run this was
//! written against measured 347 ms of follower wait against a 138 ms
//! commit, i.e. the coalescer was the plan path, and 66 ms of *leader*
//! wait — the time a submitter spent winning a lock that was already free.
//!
//! Service order is queue order regardless of who leads: the queue is
//! FIFO and a leader drains from the front, so leadership is only about
//! which thread runs the batch, never about who gets in it.

use anyhow::{anyhow, Result as R};
use std::collections::HashMap;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

/// Everything the coalescer shares, under one lock so that "am I served
/// already?" and "is the commit slot free?" are answered from the same
/// snapshot — and so that a waiter can block on both at once.
struct Pending<Req, Res> {
    /// Unserved requests in arrival order. A leader drains from the front.
    queue: Vec<(u64, Req)>,
    /// Results a leader published, each held for the one submitter whose
    /// ticket it is. Every entry is removed by its owner, so this does not
    /// accumulate; it exists only to cover the window between a leader
    /// publishing and a woken submitter re-acquiring the lock.
    served: HashMap<u64, R<Res>>,
    /// Set while a leader is running a batch outside this lock. This flag
    /// *is* the election: a submitter that finds it clear may lead, and
    /// because it is set and cleared in the same critical sections that
    /// move the queue, a clear flag also means every unserved request is
    /// in the queue.
    committing: bool,
    /// Ticket source. Wrapping needs 2^64 store writes on one coalescer.
    next_ticket: u64,
}

pub(crate) struct Coalescer<Req, Res> {
    /// Metric label for this coalescer's path (`heartbeat`, `plan_apply`).
    path: &'static str,
    pending: Mutex<Pending<Req, Res>>,
    /// Signalled when a leader publishes. That one event both hands out
    /// results and frees the commit slot, which is why a single condvar
    /// covers both things a waiting submitter waits for.
    published: Condvar,
    max_batch: usize,
}

impl<Req, Res> std::fmt::Debug for Coalescer<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coalescer")
            .field("path", &self.path)
            .field("max_batch", &self.max_batch)
            .finish_non_exhaustive()
    }
}

impl<Req, Res> Coalescer<Req, Res> {
    /// `path` labels every metric this coalescer emits; keep it a short
    /// stable identifier for the write path (`heartbeat`, `plan_apply`).
    pub(crate) fn new(path: &'static str, max_batch: usize) -> Self {
        Coalescer {
            path,
            pending: Mutex::new(Pending {
                queue: Vec::new(),
                served: HashMap::new(),
                committing: false,
                next_ticket: 0,
            }),
            published: Condvar::new(),
            max_batch: max_batch.max(1),
        }
    }

    /// Submit one request and block until a batch containing it has run.
    ///
    /// `run` executes a drained batch and returns exactly one result per
    /// request, in request order. The caller may be elected leader (and
    /// then runs `run` for its own and others' requests) or served by
    /// another leader — either way it returns its own request's result.
    pub(crate) fn submit<F>(&self, req: Req, run: F) -> R<Res>
    where
        F: Fn(Vec<Req>) -> Vec<R<Res>>,
    {
        // Instrumentation (docs/metrics.md, "Group commit"): whether
        // batching engages here at all, and whether a submitter's latency is
        // waiting or committing, is not observable from the RPC handler's
        // duration — it was the missing half of a manager-throughput
        // investigation. Cost is a handful of atomics per submit.
        let telemetry = crate::manager::telemetry::metrics();
        // Taken by whichever branch stops this submitter's wait, so the
        // wait is recorded exactly once even when the loop runs again to
        // lead a second batch.
        let mut waited = Some(Instant::now());

        let ticket = {
            let mut pending = self.lock_pending()?;
            let ticket = pending.next_ticket;
            pending.next_ticket = pending.next_ticket.wrapping_add(1);
            pending.queue.push((ticket, req));
            ticket
        };

        loop {
            let batch = {
                let mut pending = self.lock_pending()?;
                // Waits so far. The first one is this submitter waiting out
                // the commit it arrived behind, which is the commit's cost,
                // not the coalescer's; only the ones after it are rounds
                // that bought nothing.
                let mut waits = 0u64;
                loop {
                    // Checked before anything can block, so a submitter
                    // whose result already landed never waits on the
                    // commit slot it no longer needs.
                    if let Some(result) = pending.served.remove(&ticket) {
                        self.observe_wait(&mut waited, "follower");
                        return result;
                    }
                    if !pending.committing {
                        break;
                    }
                    if waits > 0 {
                        // The previous wake found yet another commit
                        // already in flight: this submitter is sitting
                        // through a commit that is not its own.
                        telemetry.store_coalesce_wait_rounds_total.inc(&[self.path]);
                    }
                    waits += 1;
                    pending = self
                        .published
                        .wait(pending)
                        .map_err(|_| anyhow!("coalescer queue lock poisoned"))?;
                }

                // Leader. No batch is in flight, so every unserved request
                // — ours included — is in the queue; an empty drain here
                // would mean the request was lost, so say so rather than
                // spin.
                let take = pending.queue.len().min(self.max_batch);
                if take == 0 {
                    return Err(anyhow!("coalescer lost the request before it ran"));
                }
                pending.committing = true;
                pending.queue.drain(..take).collect::<Vec<(u64, Req)>>()
            };

            // Holding the commit slot ends this submitter's wait: what
            // follows is its own commit, timed separately.
            self.observe_wait(&mut waited, "leader");
            let (tickets, requests): (Vec<u64>, Vec<Req>) = batch.into_iter().unzip();
            let expected = requests.len();
            telemetry
                .store_coalesce_batch_size
                .observe(&[self.path], expected as f64);

            // From here the slot is held, and every way out of it —
            // including a panic inside `run` — has to publish and release,
            // or `committing` stays set and every waiter wedges forever.
            // That is the guard's whole job.
            let commit = Commit {
                coalescer: self,
                tickets,
            };
            let commit_started = Instant::now();
            let mut results = run(requests);
            telemetry
                .store_coalesce_commit_seconds
                .observe(&[self.path], commit_started.elapsed().as_secs_f64());
            if results.len() != expected {
                let msg = format!(
                    "coalescer batch runner returned {} results for {} requests",
                    results.len(),
                    expected
                );
                results = (0..expected).map(|_| Err(anyhow!(msg.clone()))).collect();
            }
            commit.publish(results);
            // Our request was either in this batch (result waiting) or is
            // still queued beyond max_batch (loop again and lead the next
            // batch).
        }
    }

    fn lock_pending(&self) -> R<MutexGuard<'_, Pending<Req, Res>>> {
        self.pending
            .lock()
            .map_err(|_| anyhow!("coalescer queue lock poisoned"))
    }

    /// Hand a finished batch's results to their owners and free the commit
    /// slot, then wake everyone waiting. Both halves happen in one critical
    /// section and the notify follows it, so a waiter re-checking the state
    /// under the lock cannot miss the transition it was woken for.
    ///
    /// `notify_all`, not `notify_one`: the waiters are not
    /// interchangeable. Most are waiting for a specific result and only one
    /// (or none) will lead next, and waking the wrong single thread is a
    /// lost wakeup.
    fn release(&self, results: Vec<(u64, R<Res>)>) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.committing = false;
            pending.served.extend(results);
        }
        // A poisoned lock is already fatal for every submitter — each one
        // errors out of `lock_pending` — so there is nothing better to do
        // than let them.
        self.published.notify_all();
    }

    /// Record this submitter's wait once, under the role that ended it:
    /// `leader` (won the commit slot; its own commit has not started yet)
    /// or `follower` (another submitter's batch handed back its result).
    fn observe_wait(&self, waited: &mut Option<Instant>, role: &'static str) {
        if let Some(started) = waited.take() {
            crate::manager::telemetry::metrics()
                .store_coalesce_wait_seconds
                .observe(&[self.path, role], started.elapsed().as_secs_f64());
        }
    }
}

/// Holds the commit slot for one batch. `publish` is the normal exit;
/// dropping with tickets still owed means the leader died mid-commit
/// (a panic in `run`, or the thread unwinding), and the batch's other
/// members must still be told and the slot must still be released.
struct Commit<'a, Req, Res> {
    coalescer: &'a Coalescer<Req, Res>,
    /// Tickets this leader owes a result to, in request order. Emptied by
    /// `publish`, which is also how `drop` tells the two exits apart.
    tickets: Vec<u64>,
}

impl<Req, Res> Commit<'_, Req, Res> {
    /// Exactly one result per ticket, in request order — the caller has
    /// already replaced a wrong-length `run` return with one error each,
    /// so this zip cannot drop or duplicate a submitter's result.
    fn publish(mut self, results: Vec<R<Res>>) {
        let tickets = std::mem::take(&mut self.tickets);
        self.coalescer
            .release(tickets.into_iter().zip(results).collect());
    }
}

impl<Req, Res> Drop for Commit<'_, Req, Res> {
    fn drop(&mut self) {
        let tickets = std::mem::take(&mut self.tickets);
        if tickets.is_empty() {
            // Published normally. The slot is already released, and it may
            // by now belong to a different leader, so do nothing.
            return;
        }
        let owed = tickets
            .into_iter()
            .map(|ticket| {
                (
                    ticket,
                    Err(anyhow!("coalescer batch leader dropped the request")),
                )
            })
            .collect();
        self.coalescer.release(owed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn every_submitter_gets_its_own_result() -> R<()> {
        let coalescer: Arc<Coalescer<usize, usize>> = Arc::new(Coalescer::new("test", 64));
        let batches = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..32)
            .map(|i| {
                let coalescer = Arc::clone(&coalescer);
                let batches = Arc::clone(&batches);
                std::thread::spawn(move || {
                    coalescer.submit(i, |batch| {
                        batches.fetch_add(1, Ordering::SeqCst);
                        // Simulate commit latency so followers queue up.
                        std::thread::sleep(Duration::from_millis(2));
                        batch.into_iter().map(|req| Ok(req * 10)).collect()
                    })
                })
            })
            .collect();

        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.join().expect("submitter panicked")?;
            assert_eq!(result, i * 10, "submitter {i} got someone else's result");
        }
        assert!(
            batches.load(Ordering::SeqCst) <= 32,
            "batching must never run more batches than requests"
        );
        Ok(())
    }

    #[test]
    fn max_batch_caps_a_drain_but_everyone_is_served() -> R<()> {
        let coalescer: Arc<Coalescer<usize, usize>> = Arc::new(Coalescer::new("test_max_batch", 2));
        let handles: Vec<_> = (0..9)
            .map(|i| {
                let coalescer = Arc::clone(&coalescer);
                std::thread::spawn(move || {
                    coalescer.submit(i, |batch| {
                        assert!(batch.len() <= 2, "drain exceeded max_batch");
                        batch.into_iter().map(Ok).collect()
                    })
                })
            })
            .collect();
        for (i, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.join().expect("submitter panicked")?, i);
        }
        Ok(())
    }

    #[test]
    fn instrumentation_records_batch_size_and_the_leader_wait() -> R<()> {
        // A unique path label keeps this independent of every other test
        // sharing the process-global registry.
        let coalescer: Coalescer<usize, usize> = Coalescer::new("test_instrumentation", 8);
        coalescer.submit(7, |batch| batch.into_iter().map(Ok).collect())?;

        let rendered = crate::manager::telemetry::render();
        // An uncontended submitter leads a batch of exactly one — the shape
        // that says "batching never engaged".
        assert!(
            rendered.contains(
                "legion_store_coalesce_batch_size_bucket{path=\"test_instrumentation\",le=\"1\"} 1"
            ),
            "batch size 1 not recorded:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "legion_store_coalesce_commit_seconds_count{path=\"test_instrumentation\"} 1"
            ),
            "commit duration not recorded:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "legion_store_coalesce_wait_seconds_count{path=\"test_instrumentation\",role=\"leader\"} 1"
            ),
            "leader wait not recorded:\n{rendered}"
        );
        Ok(())
    }

    #[test]
    fn wrong_result_count_errors_every_batch_member() {
        let coalescer: Coalescer<usize, usize> = Coalescer::new("test_result_count", 8);
        let result = coalescer.submit(1, |_batch| Vec::new());
        assert!(result.is_err());
    }

    #[test]
    fn a_panicking_leader_does_not_wedge_its_batch_mates() -> R<()> {
        // The leader holds the commit slot while `run` executes. If a panic
        // could carry it away without releasing, every other submitter
        // would block forever instead of getting the error the old reply
        // channel gave them by hanging up.
        let coalescer: Arc<Coalescer<usize, usize>> =
            Arc::new(Coalescer::new("test_leader_panic", 64));
        let committing = Arc::new(AtomicBool::new(false));

        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let committing = Arc::clone(&committing);
            std::thread::spawn(move || {
                coalescer.submit(0, |_batch| {
                    committing.store(true, Ordering::SeqCst);
                    // Long enough for the follower below to queue behind
                    // this commit before it dies.
                    std::thread::sleep(Duration::from_millis(50));
                    panic!("batch runner died mid-commit");
                })
            })
        };

        while !committing.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
        // Queued during the doomed commit, so it is not in that batch: it
        // must be able to lead its own once the slot is released.
        let follower = coalescer.submit(1, |batch| batch.into_iter().map(Ok).collect())?;
        assert_eq!(follower, 1);
        assert!(leader.join().is_err(), "the leader was supposed to panic");
        Ok(())
    }

    /// A submitter that arrives while a commit is in flight has lost the
    /// election and must wait for it. The point of this test is *what the
    /// wait is spent on*: it must be spent enrolled in the next batch, not
    /// re-contending for a lock on a poll interval.
    ///
    /// The poll-based version of this file fails here. Six losers queued
    /// behind a 150 ms commit each woke every millisecond to retry a
    /// `try_lock` that could not succeed until the leader was done, which
    /// is ~900 wasted rounds; the assertion below caps them at 20. (The
    /// batch shape `[1, 6]` passes either way — it is the drain that was
    /// always FIFO, and the wait that was not.)
    #[test]
    fn losing_the_election_enrolls_a_submitter_instead_of_making_it_poll() -> R<()> {
        const FOLLOWERS: usize = 6;
        const PATH: &str = "test_inflight_join";

        let coalescer: Arc<Coalescer<usize, usize>> = Arc::new(Coalescer::new(PATH, 64));
        let sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let commits = Arc::new(AtomicUsize::new(0));
        let committing = Arc::new(AtomicBool::new(false));
        let arrived = Arc::new(AtomicUsize::new(0));

        let run = {
            let sizes = Arc::clone(&sizes);
            let commits = Arc::clone(&commits);
            let committing = Arc::clone(&committing);
            let arrived = Arc::clone(&arrived);
            move |batch: Vec<usize>| {
                sizes.lock().expect("sizes").push(batch.len());
                if commits.fetch_add(1, Ordering::SeqCst) == 0 {
                    // Hold the first commit open until every follower has
                    // reached `submit`, so all six are provably queued
                    // *during* it, then keep holding it long enough that a
                    // 1 ms poll loop would run visibly many rounds.
                    committing.store(true, Ordering::SeqCst);
                    while arrived.load(Ordering::SeqCst) < FOLLOWERS {
                        std::thread::yield_now();
                    }
                    std::thread::sleep(Duration::from_millis(150));
                }
                batch.into_iter().map(|req| Ok(req * 10)).collect()
            }
        };

        let leader = {
            let coalescer = Arc::clone(&coalescer);
            let run = run.clone();
            std::thread::spawn(move || coalescer.submit(0, run))
        };
        while !committing.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }

        let followers: Vec<_> = (1..=FOLLOWERS)
            .map(|i| {
                let coalescer = Arc::clone(&coalescer);
                let run = run.clone();
                let arrived = Arc::clone(&arrived);
                std::thread::spawn(move || {
                    arrived.fetch_add(1, Ordering::SeqCst);
                    coalescer.submit(i, run)
                })
            })
            .collect();

        assert_eq!(leader.join().expect("leader panicked")?, 0);
        for (i, handle) in followers.into_iter().enumerate() {
            let expected = i + 1;
            assert_eq!(
                handle.join().expect("follower panicked")?,
                expected * 10,
                "follower {expected} got someone else's result"
            );
        }

        // The leader committed alone; every follower that queued behind it
        // rode the one batch that followed.
        assert_eq!(
            *sizes.lock().expect("sizes"),
            vec![1, FOLLOWERS],
            "the six losers did not ride a single batch"
        );
        let rounds = wait_rounds(PATH);
        assert!(
            rounds < 20,
            "losers burned {rounds} re-contention rounds waiting out one commit; \
             their wait should have enrolled them, not spun"
        );
        Ok(())
    }

    /// `legion_store_coalesce_wait_rounds_total` for one path, or 0 when
    /// the counter never fired and so has no series.
    fn wait_rounds(path: &str) -> u64 {
        let needle = format!("legion_store_coalesce_wait_rounds_total{{path=\"{path}\"}} ");
        crate::manager::telemetry::render()
            .lines()
            .find_map(|line| line.strip_prefix(&needle)?.trim().parse().ok())
            .unwrap_or(0)
    }
}
