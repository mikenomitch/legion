tonic::include_proto!("legion");

pub mod config;
pub mod eval;
pub mod job;
pub mod ledger;
pub mod network;
pub mod node;
pub mod token;

pub use config::*;
pub use job::*;
pub use ledger::*;
pub use network::*;
pub use node::*;
pub use token::*;
