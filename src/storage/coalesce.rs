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
use std::time::Duration;

/// How long a non-leader waits on its reply channel before re-contending
/// for the commit lock. Only reached in the window where the previous
/// leader drained the queue *before* this request was enqueued — the
/// common case is woken by the reply itself.
const LEADER_RECHECK_INTERVAL: Duration = Duration::from_millis(1);

pub(crate) struct Coalescer<Req, Res> {
    queue: Mutex<Vec<(Req, mpsc::Sender<R<Res>>)>>,
    commit_lock: Mutex<()>,
    max_batch: usize,
}

impl<Req, Res> std::fmt::Debug for Coalescer<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coalescer")
            .field("max_batch", &self.max_batch)
            .finish_non_exhaustive()
    }
}

impl<Req, Res> Coalescer<Req, Res> {
    pub(crate) fn new(max_batch: usize) -> Self {
        Coalescer {
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
        let (reply_tx, reply_rx) = mpsc::channel();
        self.queue
            .lock()
            .map_err(|_| anyhow!("coalescer queue lock poisoned"))?
            .push((req, reply_tx));

        loop {
            // Served by a previous leader while we were contending?
            if let Ok(result) = reply_rx.try_recv() {
                return result;
            }
            match self.commit_lock.try_lock() {
                Ok(_leader) => {
                    // Any batch that contained our request has fully
                    // distributed before its leader released the lock, so
                    // re-check before draining.
                    if let Ok(result) = reply_rx.try_recv() {
                        return result;
                    }
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
                    let mut results = run(requests);
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
                    Ok(result) => return result,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(anyhow!("coalescer batch leader dropped the request"));
                    }
                },
            }
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
        let coalescer: Arc<Coalescer<usize, usize>> = Arc::new(Coalescer::new(64));
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
        let coalescer: Arc<Coalescer<usize, usize>> = Arc::new(Coalescer::new(2));
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
    fn wrong_result_count_errors_every_batch_member() {
        let coalescer: Coalescer<usize, usize> = Coalescer::new(8);
        let result = coalescer.submit(1, |_batch| Vec::new());
        assert!(result.is_err());
    }
}
