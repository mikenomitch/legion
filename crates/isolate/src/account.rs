//! One isolate's memory account: its linear memory and its scratch files
//! are charged to the same ceiling (`resources.memoryMax`), so a guest
//! cannot sidestep its cap by writing files instead of growing its heap.
//!
//! The store's resource limiter charges linear memory, the scratch
//! filesystem charges file bytes, and both refuse a charge that would take
//! the total over the limit. A refusal is remembered, so a guest that traps
//! right after one is classified as stopped by its memory limit rather than
//! as crashed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared between a store's limiter and its scratch filesystem.
#[derive(Debug)]
pub struct MemoryAccount {
    limit: AtomicU64,
    linear: AtomicU64,
    scratch: AtomicU64,
    peak: AtomicU64,
    refused: AtomicBool,
}

impl MemoryAccount {
    /// An account with a ceiling of `limit` bytes.
    pub fn new(limit: u64) -> Self {
        Self {
            limit: AtomicU64::new(limit),
            linear: AtomicU64::new(0),
            scratch: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            refused: AtomicBool::new(false),
        }
    }

    /// The ceiling, in bytes.
    pub fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Change the ceiling (a resize). A lower ceiling never takes back
    /// memory already charged; it only refuses further growth.
    pub fn set_limit(&self, limit: u64) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    /// Bytes of linear memory charged.
    pub fn linear(&self) -> u64 {
        self.linear.load(Ordering::Relaxed)
    }

    /// Bytes of scratch files charged.
    pub fn scratch(&self) -> u64 {
        self.scratch.load(Ordering::Relaxed)
    }

    /// Linear memory plus scratch files.
    pub fn total(&self) -> u64 {
        self.linear() + self.scratch()
    }

    /// The highest total this account has reached.
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Whether a charge was ever refused.
    pub fn refused(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// Set linear memory to `bytes` (a memory grew to this size). Returns
    /// false, and remembers the refusal, when that would exceed the limit.
    pub fn set_linear(&self, bytes: u64) -> bool {
        if bytes.saturating_add(self.scratch()) > self.limit() {
            self.refused.store(true, Ordering::Relaxed);
            return false;
        }
        self.linear.store(bytes, Ordering::Relaxed);
        self.note_peak();
        true
    }

    /// Charge `bytes` more of scratch files. Returns false, and remembers
    /// the refusal, when that would exceed the limit.
    pub fn grow_scratch(&self, bytes: u64) -> bool {
        let scratch = self.scratch();
        if scratch.saturating_add(bytes).saturating_add(self.linear()) > self.limit() {
            self.refused.store(true, Ordering::Relaxed);
            return false;
        }
        self.scratch.store(scratch + bytes, Ordering::Relaxed);
        self.note_peak();
        true
    }

    /// Give back `bytes` of scratch files (a file shrank or was removed).
    pub fn shrink_scratch(&self, bytes: u64) {
        let scratch = self.scratch();
        self.scratch
            .store(scratch.saturating_sub(bytes), Ordering::Relaxed);
    }

    fn note_peak(&self) {
        self.peak.fetch_max(self.total(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod account_test {
    use super::*;

    #[test]
    fn linear_and_scratch_share_one_ceiling_test() {
        let account = MemoryAccount::new(100);
        assert!(account.set_linear(60));
        assert!(account.grow_scratch(40));
        assert!(!account.refused());
        // One more byte of either is over the ceiling.
        assert!(!account.grow_scratch(1));
        assert!(account.refused());
        assert!(!account.set_linear(61));
        assert_eq!(account.total(), 100);
        assert_eq!(account.peak(), 100);
    }

    #[test]
    fn freed_scratch_can_be_charged_again_test() {
        let account = MemoryAccount::new(100);
        assert!(account.grow_scratch(80));
        account.shrink_scratch(50);
        assert_eq!(account.scratch(), 30);
        assert!(account.set_linear(70));
        // The peak is the high-water mark, not the current total.
        assert_eq!(account.peak(), 100);
    }

    #[test]
    fn a_lower_ceiling_refuses_growth_but_keeps_what_is_charged_test() {
        let account = MemoryAccount::new(100);
        assert!(account.set_linear(90));
        account.set_limit(50);
        assert_eq!(account.linear(), 90);
        assert!(!account.set_linear(91));
        assert!(account.set_linear(40));
    }
}
