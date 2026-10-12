// The generated protobuf code (included below) carries doc comments whose
// list formatting newer clippy flags; we don't hand-maintain generated output.
#![allow(clippy::doc_overindented_list_items)]

tonic::include_proto!("legion");

pub mod acl;
pub mod archive;
pub mod artifact;
pub mod band;
pub mod checkpoint;
pub mod config;
pub mod edge_boot;
pub mod eval;
pub mod exec_ticket;
pub mod fork;
pub mod host_guard;
pub mod instance;
pub mod job;
pub mod ledger;
pub mod netpolicy;
pub mod network;
pub mod node;
pub mod session;
pub mod snapshot;
pub mod stop;
pub mod suspend;
pub mod token;
pub mod volume;
pub mod workload_identity;

pub use acl::*;
pub use artifact::*;
pub use band::*;
pub use config::*;
pub use edge_boot::*;
pub use instance::*;
pub use job::*;
pub use ledger::*;
pub use network::*;
pub use node::*;
pub use stop::*;
pub use token::*;
pub use volume::*;
pub use workload_identity::*;
