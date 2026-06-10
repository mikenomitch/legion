tonic::include_proto!("legion");

pub mod config;
pub mod eval;
pub mod job;
pub mod ledger;
pub mod node;
pub mod token;
pub mod workload_identity;

pub use config::*;
pub use job::*;
pub use ledger::*;
pub use node::*;
pub use token::*;
pub use workload_identity::*;
