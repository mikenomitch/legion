//! The guest side of identity refresh after a restore (suspend plan §6.9,
//! §6.12; docs/suspend-and-resume.md, "Identity after a resume").
//!
//! The handler runs inside the guest, so it lives in the `legion-guest`
//! crate the static guest binary is built from. This module re-exports it
//! for the worker, which shares its constants (the token directory) and its
//! restore-detection rules.

pub use legion_guest::resume::*;
