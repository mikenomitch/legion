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
pub mod grant;
pub mod hops;
pub mod host_guard;
pub mod instance;
pub mod job;
pub mod ledger;
pub mod namespace;
pub mod namespace_policy;
pub mod netplugin;
pub mod netpolicy;
pub mod network;
pub mod node;
pub mod session;
pub mod snapshot;
pub mod source;
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

#[cfg(test)]
mod persisted_json_tests {
    use super::*;

    /// A field added to a message must not make JSON written before it
    /// unreadable. Worker state files hold proto messages as JSON, and a
    /// worker upgraded past `TaskAllocation.envelope_resources` could not
    /// read its `netd_state.json` (`missing field envelopeResources`).
    #[test]
    fn a_message_written_before_a_newer_field_still_parses_test() {
        let mut older = serde_json::to_value(TaskAllocation {
            allocation_id: 7,
            job_id: "web".into(),
            envelope_resources: "{}".into(),
            ..Default::default()
        })
        .expect("serialize");
        older
            .as_object_mut()
            .expect("an object")
            .remove("envelopeResources");

        let parsed: TaskAllocation = serde_json::from_value(older).expect("older JSON parses");
        assert_eq!(parsed.allocation_id, 7);
        assert_eq!(parsed.job_id, "web");
        assert_eq!(parsed.envelope_resources, "");

        // Every field missing is every field at its proto3 default.
        let empty: TaskAllocation = serde_json::from_str("{}").expect("empty object parses");
        assert_eq!(empty, TaskAllocation::default());
    }
}
