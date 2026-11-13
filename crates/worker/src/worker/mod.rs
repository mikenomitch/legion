pub mod api_socket;
pub mod app_forward;
pub mod artifact_credentials;
pub mod balance;
pub mod boot_ticket;
pub mod checkpoints;
pub mod claim_window;
pub mod claims;
pub mod content_report;
pub mod daemon;
pub mod direct_exec;
pub mod edge;
pub mod envelope_elastic;
pub mod exec;
pub mod forwarder;
pub mod handoff;
pub mod identity;
pub mod idle;
pub mod interruption;
pub mod isolates;
pub mod launch_pool;
pub mod log_sink;
pub mod logs;
pub mod managers;
pub mod netd;
pub mod node_check;
pub mod park;
pub mod plane;
pub mod pressure;
pub mod resources;
pub mod runtime_state;
pub mod server;
pub mod session_log;
pub mod session_report;
pub mod simulate;
pub mod start_timing;
pub mod state;
pub mod task_health;
pub mod task_manager;
pub mod task_supervisor;
pub mod templates;
pub mod usage;
pub mod volumes;
pub mod work_stream;

#[cfg(test)]
mod node_attr_test {
    /// The attributes a worker advertises about its own structure are the
    /// ones the types crate treats as structural
    /// (`structs::node::is_structural_node_attr`), which keeps its own list
    /// because it cannot see the constants defined up here.
    #[test]
    fn structural_attrs_match_the_constants_they_mirror_test() {
        for key in [
            crate::constraints::NODE_NAMESPACES_ATTR,
            crate::resources::CAPACITY_ATTR,
            crate::structs::worker_protocol::WORKER_FLEET_ATTR,
            crate::worker::netd::kernel_guard::NODE_ATTR,
            crate::worker::netd::kernel::NODE_ATTR,
            crate::task_plugins::sandbox_addr::BRIDGES_ATTR,
            crate::task_plugins::runtime_class::NODE_ATTR,
            crate::task_plugins::IDMAPPED_MOUNTS_ATTR,
        ] {
            assert!(crate::structs::node::is_structural_node_attr(key), "{key}");
        }
    }
}
