// `tonic::Status` is 176 bytes, so every gRPC handler (including the
// generated service traits) trips clippy's `result_large_err` on Rust 1.98+.
// The type is tonic's; boxing it at every boundary would fight the framework.
#![allow(clippy::result_large_err)]
// `#[tonic::async_trait]` and tonic's generated service traits mark methods
// returning `Pin<Box<dyn Future>>` as `#[must_use]`, which clippy's
// `double_must_use` flags on Rust 1.99+. The attribute comes from the macro.
#![allow(clippy::double_must_use)]

pub mod artifacts;
pub mod certs;
pub mod checkpoint_chunks;
pub mod checkpoint_seal;
pub mod cluster_tls;
pub mod cmd;
pub mod config_lang;
pub mod constants;
pub mod constraints;
pub mod content_publish;
pub mod content_store;
pub mod gateway;
pub mod manager;
pub mod otel;
pub mod plugin_runtime;
pub mod prelude;
pub mod resources;
pub mod scheduling_index;
pub mod session_manifest;
pub mod signals;
pub mod storage;
pub mod structs;
pub mod task_plugins;
#[cfg(test)]
pub(crate) mod test_scratch;
pub mod tracing_util;
pub mod util;
pub mod volumes;
pub mod wasm;
pub mod worker;
