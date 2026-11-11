//! `legion-isolate`: the isolate host and the module compiler
//! (docs/task-runtime-isolate.md).
//!
//! A worker runs this binary in two roles. As a **host** (`legion-isolate
//! host`) it is a confined, long-lived process that holds many tenants'
//! WebAssembly isolates and speaks `legion_proto::isolate_host` to its
//! worker over two inherited sockets. As a **compiler** (`legion-isolate
//! compile`) it is a short-lived process that validates one component and
//! writes the artifact a host later maps. The host role never compiles.
//!
//! The library is the host without its sockets: [`host::Host`] takes a
//! `HostConfig` and a [`worker_api::WorkerApi`], and [`service`] is the
//! thin gRPC shell the binary puts around it.

pub mod account;
pub mod checkpoint;
pub mod compile;
pub mod confine;
pub mod engine;
pub mod guest;
pub mod host;
pub mod module;
pub mod run_host;
pub mod scratch;
pub mod service;
pub mod shard;
pub mod turn;
pub mod worker_api;
pub mod world;
