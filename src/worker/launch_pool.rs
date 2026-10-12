//! The node's bounded pool of provisional launches (session durability plan
//! §6.1).
//!
//! A session or claim start holds one slot from admission until its runtime
//! call returns. A node therefore runs at most `edgeBoot.launchConcurrency`
//! starts through its runtime at once; a start that cannot get a slot within
//! `edgeBoot.queueTimeout` is refused with `RESOURCE_EXHAUSTED` and marked
//! retryable, so the picker (a manager's session router, or a load balancer
//! in front of the edge) tries the next node instead of piling threads up
//! behind a busy one.
//!
//! The slot is taken *before* admission, so a refused start held nothing:
//! no envelope share, no claim, no ticket use.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A held launch slot. Dropping it frees the slot.
pub type LaunchSlot = OwnedSemaphorePermit;

#[derive(Debug)]
pub struct LaunchPool {
    slots: Arc<Semaphore>,
    size: usize,
    queue_timeout: Duration,
}

/// Why a start was refused before admission: every slot stayed busy for the
/// whole queue timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchPoolFull {
    pub size: usize,
    pub waited: Duration,
}

impl std::fmt::Display for LaunchPoolFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node is busy: all {} launch slots stayed in use for {:?}; retry on another node",
            self.size, self.waited
        )
    }
}

impl std::error::Error for LaunchPoolFull {}

impl LaunchPool {
    pub fn new(size: usize, queue_timeout: Duration) -> Self {
        let size = size.max(1);
        LaunchPool {
            slots: Arc::new(Semaphore::new(size)),
            size,
            queue_timeout,
        }
    }

    /// The pool a worker runs with, from its resolved `edgeBoot` block.
    pub fn from_config(config: &crate::structs::WorkerEdgeBootConfigFull) -> Self {
        let queue_timeout =
            humantime::parse_duration(&config.queue_timeout).unwrap_or(Duration::from_secs(1));
        LaunchPool::new(config.resolved_launch_concurrency(), queue_timeout)
    }

    /// Wait up to the queue timeout for a slot.
    pub async fn acquire(&self) -> Result<LaunchSlot, LaunchPoolFull> {
        let full = LaunchPoolFull {
            size: self.size,
            waited: self.queue_timeout,
        };
        match tokio::time::timeout(self.queue_timeout, self.slots.clone().acquire_owned()).await {
            Ok(Ok(slot)) => Ok(slot),
            // The semaphore is never closed; a timeout is the only refusal.
            _ => {
                crate::manager::telemetry::worker_metrics()
                    .launch_pool_refusals_total
                    .inc();
                Err(full)
            }
        }
    }

    /// A slot if one is free right now, for callers that cannot wait.
    pub fn try_acquire(&self) -> Option<LaunchSlot> {
        self.slots.clone().try_acquire_owned().ok()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Slots currently held.
    pub fn in_use(&self) -> usize {
        self.size - self.slots.available_permits()
    }
}

impl Default for LaunchPool {
    fn default() -> Self {
        LaunchPool::from_config(&crate::structs::WorkerEdgeBootConfigFull::default())
    }
}

#[cfg(test)]
mod launch_pool_test {
    use super::*;

    #[tokio::test]
    async fn a_full_pool_refuses_after_the_queue_timeout() {
        let pool = LaunchPool::new(2, Duration::from_millis(20));
        let first = pool.acquire().await.expect("a free slot");
        let _second = pool.acquire().await.expect("a free slot");
        assert_eq!(pool.in_use(), 2);
        let refused = pool.acquire().await.expect_err("both slots are held");
        assert_eq!(refused.size, 2);
        assert!(refused.to_string().contains("retry on another node"));
        drop(first);
        assert!(pool.acquire().await.is_ok(), "a freed slot is reused");
    }

    #[tokio::test]
    async fn a_waiting_start_gets_the_next_freed_slot() {
        let pool = Arc::new(LaunchPool::new(1, Duration::from_secs(5)));
        let held = pool.acquire().await.expect("a free slot");
        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.acquire().await.is_ok() })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(waiter.await.expect("waiter"), "the queued start launched");
    }
}
