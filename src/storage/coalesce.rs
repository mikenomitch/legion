//! Leader-elect request coalescer (plan-path scale levers, D5).
//!
//! Batches concurrent submitters of the same kind of store write into one
//! commit: submitters enqueue a request and race for the commit lock; the
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

use anyhow::{anyhow, Result as R};
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a non-leader waits on its reply channel before re-contending
/// for the commit lock. Only reached in the window where the previous
/// leader drained the queue *before* this request was enqueued — the
/// common case is woken by the reply itself.
const LEADER_RECHECK_INTERVAL: Duration = Duration::from_millis(1);

pub(crate) struct Coalescer<Req, Res> {
    /// Metric label for this coalescer's path (`heartbeat`, `plan_apply`).
    path: &'static str,
    queue: Mutex<Vec<(Req, mpsc::Sender<R<Res>>)>>,
    commit_lock: Mutex<()>,
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
            queue: Mutex::new(Vec::new()),
            commit_lock: Mutex::new(()),
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

        let (reply_tx, reply_rx) = mpsc::channel();
        self.queue
            .lock()
            .map_err(|_| anyhow!("coalescer queue lock poisoned"))?
            .push((req, reply_tx));

        loop {
            // Served by a previous leader while we were contending?
            if let Ok(result) = reply_rx.try_recv() {
                self.observe_wait(&mut waited, "follower");
                return result;
            }
            match self.commit_lock.try_lock() {
                Ok(_leader) => {
                    // Any batch that contained our request has fully
                    // distributed before its leader released the lock, so
                    // re-check before draining.
                    if let Ok(result) = reply_rx.try_recv() {
                        self.observe_wait(&mut waited, "follower");
                        return result;
                    }
                    // Holding the commit lock ends this submitter's wait:
                    // what follows is its own commit, timed separately.
                    self.observe_wait(&mut waited, "leader");
                    let batch: Vec<(Req, mpsc::Sender<R<Res>>)> = {
                        let mut queue = self
                            .queue
                            .lock()
                            .map_err(|_| anyhow!("coalescer queue lock poisoned"))?;
                        let take = queue.len().min(self.max_batch);
                        queue.drain(..take).collect()
                    };
                    if batch.is_empty() {
                        // Unreachable in practice (our own request must be
                        // queued to get here), but never spin on a bug.
                        std::thread::sleep(LEADER_RECHECK_INTERVAL);
                        continue;
                    }
                    let (requests, senders): (Vec<Req>, Vec<mpsc::Sender<R<Res>>>) =
                        batch.into_iter().unzip();
                    let expected = requests.len();
                    telemetry
                        .store_coalesce_batch_size
                        .observe(&[self.path], expected as f64);
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
                    for (sender, result) in senders.into_iter().zip(results) {
                        // A receiver that gave up (caller unwound) is not
                        // this batch's problem.
                        let _ = sender.send(result);
                    }
                    // Our request was either in this batch (reply waiting)
                    // or is still queued beyond max_batch (loop again and
                    // lead the next batch).
                }
                Err(_busy) => match reply_rx.recv_timeout(LEADER_RECHECK_INTERVAL) {
                    Ok(result) => {
                        self.observe_wait(&mut waited, "follower");
                        return result;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Each round is a full LEADER_RECHECK_INTERVAL of
                        // latency that belongs to the coalescer's polling
                        // rather than to any commit — counted so it can be
                        // told apart from a slow commit.
                        telemetry.store_coalesce_wait_rounds_total.inc(&[self.path]);
                        continue;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(anyhow!("coalescer batch leader dropped the request"));
                    }
                },
            }
        }
    }

    /// Record this submitter's wait once, under the role that ended it:
    /// `leader` (won the commit lock; its own commit has not started yet)
    /// or `follower` (another submitter's batch returned its result).
    fn observe_wait(&self, waited: &mut Option<Instant>, role: &'static str) {
        if let Some(started) = waited.take() {
            crate::manager::telemetry::metrics()
                .store_coalesce_wait_seconds
                .observe(&[self.path, role], started.elapsed().as_secs_f64());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
}
