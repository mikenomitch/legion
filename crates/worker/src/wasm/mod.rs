//! Running WASM modules inside Legion: the parts every embedder shares.
//!
//! Legion embeds [wasmi](https://github.com/wasmi-labs/wasmi), an
//! interpreter, so a module runs in-process with no compiler, no JIT memory,
//! and about 1.8 MB of binary (mesh interfaces plan calls entry 22). An
//! embedder is the code that decides what a module is *for*: the network
//! datapath's proxy-wasm host (`worker::netd::plugins`) today, and a `wasm`
//! task runtime that runs a WASI command module as a task tomorrow. Each
//! embedder brings its own ABI; this module gives them one way to do the
//! rest:
//!
//! - [`engine`]: the node's one engine (fuel metering on), the
//!   [`MemoryCap`] limiter that remembers a refusal, and [`describe_failure`],
//!   which names a trap, exhausted fuel, a memory limit, or `proc_exit` the
//!   same way for every embedder.
//! - [`memory`]: reading and writing guest memory from a host function.
//! - [`wasi`]: WASI preview1 (`wasi_snapshot_preview1`) for modules built
//!   with `wasm32-wasip1`: arguments, environment, clocks, randomness,
//!   standard output and error, and `proc_exit`, driven by a [`wasi::WasiHost`]
//!   the embedder implements. No filesystem and no sockets: every other WASI
//!   call answers `ENOSYS`. [`wasi::stub_unknown_imports`] lets a module
//!   that imports more than an embedder provides still instantiate.
//! - [`module`]: fetching a module's bytes by URL and `sha256:` checksum,
//!   through the artifact stager into the node content store, so a module is
//!   downloaded once per node whichever embedder asked for it.
//!
//! What stays with the embedder: its ABI's exports and host functions, how
//! instances are pooled or scheduled, and what the limits are (a plugin
//! callback gets fuel and a deadline per call; a task would get a CPU and
//! memory budget for its whole run).

pub mod engine;
pub mod memory;
pub mod module;
pub mod wasi;

pub use engine::{describe_failure, engine, humanize, MemoryCap};
