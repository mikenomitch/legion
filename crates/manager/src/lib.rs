//! Legion's manager: the gRPC server, eval loop, scheduler, and rollouts
//! ([`manager`]), over the replicated state store ([`storage`]).
//!
//! It stands on `legion-worker` (job validation reads the task runtimes,
//! and a single-process cluster runs a worker in the manager's process) and
//! `legion-common`. The `legion` binary crate, with the CLI and the bundled
//! apps, is the one above (docs/codebase-conventions.md, "Crates").

// `tonic::Status` is 176 bytes, so every gRPC handler (including the
// generated service traits) trips clippy's `result_large_err` on Rust 1.98+.
// The type is tonic's; boxing it at every boundary would fight the framework.
#![allow(clippy::result_large_err)]
// `#[tonic::async_trait]` and tonic's generated service traits mark methods
// returning `Pin<Box<dyn Future>>` as `#[must_use]`, which clippy's
// `double_must_use` flags on Rust 1.99+. The attribute comes from the macro.
#![allow(clippy::double_must_use)]

// The layers below, at the paths this crate's code has always used
// (`crate::structs::Job`, `crate::worker::netd`, `crate::prelude::*`).
#[cfg(test)]
pub(crate) use legion_common::test_scratch;
pub use legion_common::{
    autoscaler, bundled_apps, certs, checkpoint_seal, cli_paths, cluster_tls, config_lang,
    constants, constraints, content_store, devices, mesh_ca, otel, prelude, resources,
    scheduling_index, signals, structs, telemetry, tracing_util, util, volumes,
};
pub use legion_worker::{
    artifacts, checkpoint_chunks, content_publish, objects, plugin_runtime, rollout_strategy,
    session_manifest, task_plugins, wasm, worker,
};
// The plugin macros a scheduling-policy plugin's `export_*!` expands to.
pub use legion_worker::declare_plugin_abi;

pub mod manager;
pub mod storage;
