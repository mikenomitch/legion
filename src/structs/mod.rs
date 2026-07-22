// The generated protobuf code (included below) carries doc comments whose
// list formatting newer clippy flags; we don't hand-maintain generated output.
#![allow(clippy::doc_overindented_list_items)]

tonic::include_proto!("legion");

pub mod acl;
pub mod artifact;
pub mod config;
pub mod eval;
pub mod instance;
pub mod job;
pub mod ledger;
pub mod network;
pub mod node;
pub mod token;
pub mod workload_identity;

pub use acl::*;
pub use artifact::*;
pub use config::*;
pub use instance::*;
pub use job::*;
pub use ledger::*;
pub use network::*;
pub use node::*;
pub use token::*;
pub use workload_identity::*;
