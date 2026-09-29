// `tonic::Status` is 176 bytes, so every gRPC handler (including the
// generated service traits) trips clippy's `result_large_err` on Rust 1.98+.
// The type is tonic's; boxing it at every boundary would fight the framework.
#![allow(clippy::result_large_err)]

pub mod artifacts;
pub mod certs;
pub mod cmd;
pub mod config_lang;
pub mod constants;
pub mod constraints;
pub mod manager;
pub mod otel;
pub mod plugin_runtime;
pub mod prelude;
pub mod resources;
pub mod scheduling_index;
pub mod signals;
pub mod storage;
pub mod structs;
pub mod task_plugins;
pub mod tracing_util;
pub mod util;
pub mod volumes;
pub mod worker;
