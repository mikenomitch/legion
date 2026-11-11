//! Legion's wire types, generated from `legion.proto` by `tonic-build`.
//!
//! Every protobuf message and enum, and the tonic client and server stubs for
//! the `Manager` service, live here. The main `legion` crate re-exports all
//! of it from `legion::structs`, which is where Legion's own code names
//! them; depend on this crate directly only to talk to a manager without
//! linking the rest of Legion.
//!
//! The crate holds generated code and the impls the orphan rule pins to these
//! types (the redacting `Debug`s in [`redacted`]). Behavior on these types
//! that needs the rest of Legion is an extension trait in the main crate.

// `tonic::Status` is 176 bytes, so every generated service trait trips
// clippy's `result_large_err`; the type is tonic's.
#![allow(clippy::result_large_err)]
// Tonic's generated service traits mark boxed-future methods `#[must_use]`,
// which clippy's `double_must_use` flags. The attribute comes from the macro.
#![allow(clippy::double_must_use)]
// The generated code carries the proto's doc comments, whose list formatting
// newer clippy flags; generated output is not hand-maintained.
#![allow(clippy::doc_overindented_list_items)]

tonic::include_proto!("legion");

/// The node-local protocol between a worker and the `legion-isolate` host
/// processes it supervises, generated from `isolate_host.proto`. Internal:
/// both ends run on one node, and the two are versioned together by
/// [`isolate_host::PROTOCOL_VERSION`], not by wire-compatibility rules.
pub mod isolate_host {
    tonic::include_proto!("legion.isolate.host");

    /// Bumped on any incompatible change to `isolate_host.proto`. A host
    /// refuses a `HostConfig` carrying another version, and a worker refuses
    /// a host whose `Hello` reports another.
    pub const PROTOCOL_VERSION: u32 = 1;
}

mod redacted;

/// The text of `legion.proto` this crate was generated from. Tests and the
/// RPC access tables parse it to check that every RPC is classified.
pub const PROTO_SOURCE: &str = include_str!("../legion.proto");
