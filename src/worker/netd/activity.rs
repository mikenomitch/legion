//! Datapath activity per allocation, for the idle policy
//! (docs/suspend-and-resume.md, "Idle policy").
//!
//! The service graph ([`super::graph`]) records a flow's bytes when it
//! ends, which is too late for idle detection: a long model stream would be
//! seen at its start and its end, never during. And the `connections`
//! signal keeps an allocation awake for as long as any connection is open,
//! even a silent one: a client holding an interactive session, or a harness
//! waiting on a quiet model call.
//!
//! So every connection the datapath fronts is wrapped in a
//! [`TrackedStream`]: each ingress connection, and each connection through a
//! gated allocation's egress proxy. It counts as open from accept to close,
//! and stamps the allocation's last-bytes time whenever data moves either
//! way. Both are atomics on a per-allocation slot fetched once per
//! connection, so the hot path is one relaxed store per read or write and
//! never takes a lock.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// One allocation's datapath counters.
#[derive(Debug, Default)]
pub struct AllocActivity {
    open: AtomicU32,
    /// Unix seconds bytes last moved through any tracked connection. 0 never.
    last_bytes_unix: AtomicI64,
}

impl AllocActivity {
    fn touch(&self) {
        self.last_bytes_unix
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
    }
}

/// What the sampler reads for one allocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DatapathActivity {
    /// Ingress and egress connections open through the datapath right now.
    pub open_connections: u32,
    /// Unix seconds bytes last moved, 0 if never.
    pub last_bytes_unix: i64,
}

/// Every allocation's datapath counters on this node.
#[derive(Debug, Default)]
pub struct ActivityLedger {
    slots: Mutex<HashMap<i64, Arc<AllocActivity>>>,
}

impl ActivityLedger {
    pub fn new() -> Self {
        Self::default()
    }

    fn slot(&self, allocation_id: i64) -> Arc<AllocActivity> {
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(allocation_id)
            .or_default()
            .clone()
    }

    /// Wrap an accepted connection (ingress, or into a gated allocation's
    /// egress proxy). It counts as open until the returned stream is dropped.
    pub fn track<S>(&self, allocation_id: i64, stream: S) -> TrackedStream<S> {
        let slot = self.slot(allocation_id);
        slot.open.fetch_add(1, Ordering::Relaxed);
        slot.touch();
        TrackedStream {
            inner: stream,
            slot,
        }
    }

    /// The counters of every allocation the ledger knows.
    pub fn snapshot(&self) -> HashMap<i64, DatapathActivity> {
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(id, slot)| {
                (
                    *id,
                    DatapathActivity {
                        open_connections: slot.open.load(Ordering::Relaxed),
                        last_bytes_unix: slot.last_bytes_unix.load(Ordering::Relaxed),
                    },
                )
            })
            .collect()
    }

    /// Move an allocation's last-bytes stamp back, so a test can see the
    /// next byte move it forward again.
    #[cfg(test)]
    pub(crate) fn rewind(&self, allocation_id: i64, to_unix: i64) {
        self.slot(allocation_id)
            .last_bytes_unix
            .store(to_unix, Ordering::Relaxed);
    }

    /// Forget allocations no longer on the node, unless a connection to one
    /// is still open (it drains on its own and is forgotten next time).
    pub fn retain(&self, live: &HashSet<i64>) {
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|id, slot| live.contains(id) || slot.open.load(Ordering::Relaxed) > 0);
    }
}

/// A datapath connection that reports its activity. Transparent otherwise.
pub struct TrackedStream<S> {
    inner: S,
    slot: Arc<AllocActivity>,
}

impl<S> Drop for TrackedStream<S> {
    fn drop(&mut self) {
        self.slot.open.fetch_sub(1, Ordering::Relaxed);
        self.slot.touch();
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TrackedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.slot.touch();
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TrackedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, data);
        if matches!(polled, Poll::Ready(Ok(written)) if written > 0) {
            self.slot.touch();
        }
        polled
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S: super::splice::SpliceStream> super::splice::SpliceStream for TrackedStream<S> {
    fn socket(&self) -> Option<super::splice::Socket<'_>> {
        self.inner.socket()
    }

    fn spliced(&self, bytes: usize) {
        if bytes > 0 {
            self.slot.touch();
        }
        self.inner.spliced(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A connection counts as open from accept to drop, and bytes in either
    /// direction stamp the last-bytes time. A quiet open connection keeps
    /// the old stamp but still counts as open.
    #[tokio::test]
    async fn connections_count_while_open_and_bytes_stamp_activity() {
        let ledger = ActivityLedger::new();
        let (client, server) = tokio::io::duplex(64);
        let mut tracked = ledger.track(7, server);
        let seen = ledger.snapshot()[&7];
        assert_eq!(seen.open_connections, 1);
        assert!(seen.last_bytes_unix > 0, "accept counts as activity");

        // Rewind the stamp to show that bytes move it forward again.
        ledger.slot(7).last_bytes_unix.store(1, Ordering::Relaxed);
        let mut client = client;
        client.write_all(b"hi").await.unwrap();
        let mut buf = [0u8; 2];
        tracked.read_exact(&mut buf).await.unwrap();
        assert!(ledger.snapshot()[&7].last_bytes_unix > 1);

        // Quiet: the stamp stays put, the connection stays open.
        ledger.slot(7).last_bytes_unix.store(1, Ordering::Relaxed);
        let quiet = ledger.snapshot()[&7];
        assert_eq!(quiet.open_connections, 1);
        assert_eq!(quiet.last_bytes_unix, 1);

        drop(tracked);
        assert_eq!(ledger.snapshot()[&7].open_connections, 0);
        ledger.retain(&HashSet::new());
        assert!(ledger.snapshot().is_empty());
    }
}
