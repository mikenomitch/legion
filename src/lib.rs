//! The `legion` crate: the CLI and MCP server ([`cmd`]), the bundled apps
//! ([`apps`]), and the gateway, on top of the three library crates Legion is
//! split into. Their modules are re-exported here, so `legion::manager`,
//! `legion::worker`, `legion::structs` and the rest are the paths they have
//! always been for the binary, the integration tests, and plugins
//! (docs/codebase-conventions.md, "Crates").

// `tonic::Status` is 176 bytes, so every gRPC handler (including the
// generated service traits) trips clippy's `result_large_err` on Rust 1.98+.
// The type is tonic's; boxing it at every boundary would fight the framework.
#![allow(clippy::result_large_err)]
// `#[tonic::async_trait]` and tonic's generated service traits mark methods
// returning `Pin<Box<dyn Future>>` as `#[must_use]`, which clippy's
// `double_must_use` flags on Rust 1.99+. The attribute comes from the macro.
#![allow(clippy::double_must_use)]

#[cfg(test)]
pub(crate) use legion_common::test_scratch;
pub use legion_common::{
    autoscaler, bundled_apps, certs, checkpoint_seal, cli_paths, cluster_tls, config_lang,
    constants, constraints, content_store, devices, mesh_ca, otel, prelude, resources,
    scheduling_index, signals, structs, telemetry, tracing_util, util, volumes,
};
pub use legion_manager::{manager, storage};
pub use legion_worker::{
    artifacts, checkpoint_chunks, content_publish, objects, plugin_runtime, rollout_strategy,
    session_manifest, task_plugins, wasm, worker,
};
// The macros a plugin invokes as `legion::export_*!` / `legion::declare_*!`.
pub use legion_manager::export_scheduling_policy;
pub use legion_worker::{
    declare_plugin_abi, declare_task_plugin_abi, export_artifact_fetcher, export_node_checker,
    export_rollout_strategy,
};

pub mod apps;
pub mod cmd;
pub mod gateway;
