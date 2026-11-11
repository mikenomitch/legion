//! `legion-isolate`: the isolate host and the module compiler
//! (docs/task-runtime-isolate.md).
//!
//! A worker runs this binary in two roles. As a **host** (`legion-isolate
//! host`) it is a confined, long-lived process that holds many tenants'
//! WebAssembly isolates and speaks `legion_proto::isolate_host` to its
//! worker over two inherited sockets. As a **compiler** (`legion-isolate
//! compile`) it is a short-lived process that validates one component and
//! writes the artifact a host later maps. The host role never compiles.

pub mod account;
pub mod scratch;
