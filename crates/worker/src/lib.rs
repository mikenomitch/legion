//! Legion's worker: the node agent ([`worker`]), the task runtimes and job
//! validation that reads them ([`task_plugins`]), artifacts and the content
//! store's publishing side ([`artifacts`], [`content_publish`]), the
//! object-store client ([`objects`]), and the dylib plugin loader
//! ([`plugin_runtime`]).
//!
//! It stands on `legion-common` and knows nothing of the manager or the
//! store. The manager is the crate above this one: it validates jobs
//! against the runtimes here and can run a worker in its own process
//! (docs/codebase-conventions.md, "Crates").

// `tonic::Status` is 176 bytes, so every gRPC handler (including the
// generated service traits) trips clippy's `result_large_err` on Rust 1.98+.
// The type is tonic's; boxing it at every boundary would fight the framework.
#![allow(clippy::result_large_err)]
// `#[tonic::async_trait]` and tonic's generated service traits mark methods
// returning `Pin<Box<dyn Future>>` as `#[must_use]`, which clippy's
// `double_must_use` flags on Rust 1.99+. The attribute comes from the macro.
#![allow(clippy::double_must_use)]

// The layer below, at the paths this crate's code has always used
// (`crate::structs::Job`, `crate::prelude::*`).
#[cfg(test)]
pub(crate) use legion_common::test_scratch;
pub use legion_common::{
    autoscaler, bundled_apps, certs, checkpoint_seal, cli_paths, cluster_tls, config_lang,
    constants, constraints, content_store, devices, mesh_ca, otel, prelude, resources,
    scheduling_index, signals, structs, telemetry, tracing_util, util, volumes,
};

pub mod artifacts;
pub mod checkpoint_chunks;
pub mod content_publish;
pub mod objects;
pub mod plugin_runtime;
pub mod rollout_strategy;
pub mod session_manifest;
pub mod task_plugins;
pub mod wasm;
pub mod worker;
