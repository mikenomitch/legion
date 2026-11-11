//! Legion's shared layer: what the worker and the manager both stand on.
//!
//! The domain types and the jobspec ([`structs`], which re-exports the wire
//! types generated in `legion-proto`), the config language
//! ([`config_lang`]), autoscaling policy ([`autoscaler`]), the bundled apps'
//! manifests ([`bundled_apps`]), cluster TLS and the mesh CA helpers
//! ([`cluster_tls`], [`mesh_ca`]), and the process metrics registry
//! ([`telemetry`]). Nothing here knows about the store, the scheduler, or
//! a task runtime: this crate depends on neither `legion-worker` nor
//! `legion-manager`, and both depend on it
//! (docs/codebase-conventions.md, "Crates").

// `tonic::Status` is 176 bytes, so every gRPC handler (including the
// generated service traits) trips clippy's `result_large_err` on Rust 1.98+.
// The type is tonic's; boxing it at every boundary would fight the framework.
#![allow(clippy::result_large_err)]
// `#[tonic::async_trait]` and tonic's generated service traits mark methods
// returning `Pin<Box<dyn Future>>` as `#[must_use]`, which clippy's
// `double_must_use` flags on Rust 1.99+. The attribute comes from the macro.
#![allow(clippy::double_must_use)]

pub mod autoscaler;
pub mod bundled_apps;
pub mod certs;
pub mod checkpoint_seal;
pub mod cli_paths;
pub mod cluster_tls;
pub mod config_lang;
pub mod constants;
pub mod constraints;
pub mod content_store;
pub mod devices;
pub mod mesh_ca;
pub mod otel;
pub mod prelude;
pub mod resources;
pub mod scheduling_index;
pub mod signals;
pub mod structs;
pub mod telemetry;
pub mod test_scratch;
pub mod tracing_util;
pub mod util;
pub mod volumes;
