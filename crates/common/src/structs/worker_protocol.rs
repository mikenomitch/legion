//! What a manager and a worker must compute the same way: the digests each
//! side keeps of the other's view, the constants both read off the wire,
//! and the cadence rules both apply. One definition, in the crate both
//! depend on, so a worker and a manager built from one tree cannot disagree.

use crate::constants::{MAX_WORKER_HEARTBEAT_SECONDS, WORKER_HEARTBEAT_DURATION_SEC};

/// The node config attribute the simulator sets so the manager verifies its
/// shared fleet certificate instead of a per-node one.
pub const WORKER_FLEET_ATTR: &str = "workerFleet";

/// The metadata key a status carries its stable error code in. The CLI and
/// MCP error envelopes use it as `error.code` instead of the gRPC code name.
pub const ERROR_CODE_METADATA: &str = "legion-error-code";

/// The error code for "this node is shedding edge starts": its window of
/// unratified edge claims stayed full (docs/edge-boot.md, "Overload").
/// Carried on a retryable `RESOURCE_EXHAUSTED`.
pub const OVERLOADED: &str = "OVERLOADED";

/// A worker type's `heartbeatSeconds`, clamped to the legal range.
/// `None` means the built-in default.
pub fn clamp_heartbeat_seconds(configured: Option<u32>) -> u64 {
    configured
        .map(|seconds| u64::from(seconds).clamp(1, u64::from(MAX_WORKER_HEARTBEAT_SECONDS)))
        .unwrap_or(WORKER_HEARTBEAT_DURATION_SEC)
}

/// Only content at least this large is advertised. Small blobs download in
/// well under a heartbeat, so steering placement for them buys nothing and
/// costs index memory. Workers filter before sending, and the manager filters
/// again so a misbehaving worker cannot flood the index.
pub const MIN_ADVERTISED_BYTES: u64 = 1024 * 1024;

/// FNV-1a, 64-bit: stable across processes, builds, and platforms, which
/// the digests need (a worker reports a digest to whichever manager it
/// heartbeats next).
#[derive(Debug, Clone, Copy)]
pub struct StableHash(u64);

impl Default for StableHash {
    fn default() -> Self {
        StableHash(0xcbf2_9ce4_8422_2325)
    }
}

impl StableHash {
    pub fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self
    }

    /// A length-prefixed string, so `("ab","c")` and `("a","bc")` differ.
    pub fn str(&mut self, value: &str) -> &mut Self {
        self.u64(value.len() as u64).bytes(value.as_bytes())
    }

    pub fn u64(&mut self, value: u64) -> &mut Self {
        self.bytes(&value.to_le_bytes())
    }

    pub fn i64(&mut self, value: i64) -> &mut Self {
        self.bytes(&value.to_le_bytes())
    }

    pub fn bool(&mut self, value: bool) -> &mut Self {
        self.bytes(&[u8::from(value)])
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}

/// The order-independent digest of a set of allocation ids:
/// `<count>:<xor of splitmix64(id)>` in lowercase hex. Both sides update it
/// per id, so neither has to walk the set to compare.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkDigest {
    count: u64,
    xor: u64,
}

impl WorkDigest {
    fn mix(id: i64) -> u64 {
        // splitmix64: a cheap, well-distributed bijection, so distinct id sets
        // collide only by accident of the xor.
        let mut z = (id as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn insert(&mut self, id: i64) {
        self.count += 1;
        self.xor ^= Self::mix(id);
    }

    pub fn remove(&mut self, id: i64) {
        self.count = self.count.saturating_sub(1);
        self.xor ^= Self::mix(id);
    }

    pub fn of(ids: impl IntoIterator<Item = i64>) -> Self {
        let mut digest = Self::default();
        for id in ids {
            digest.insert(id);
        }
        digest
    }

    pub fn render(&self) -> String {
        format!("{:x}:{:x}", self.count, self.xor)
    }
}
