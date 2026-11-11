//! The isolate supervisor: cordon hosts, modules, tasks and session isolates
//! on this node. The datapath side of an isolate (egress, ingress, child VMs)
//! lives in [`egress`], [`ingress`] and [`vm`].

pub mod binary;
pub mod egress;
pub mod host;
pub mod ingress;
pub mod modules;
pub mod plan;
pub mod preflight;
pub mod service;
pub mod sessions;
pub mod stats;
pub mod supervisor;
pub mod supervisor_setup;
pub mod tier;
pub mod vm;

#[cfg(test)]
mod supervisor_test;

pub use supervisor::{IsolateSupervisor, StartedIsolate, SupervisorParts};

/// One live envelope of an isolate task on this node, as the task manager
/// holds it: what the supervisor prepares the task from.
#[derive(Debug, Clone)]
pub struct IsolateEnvelope {
    pub job_id: String,
    /// The envelope allocation's id (its local key).
    pub allocation_id: i64,
    pub spec: crate::worker::task_manager::SessionSpec,
    /// The envelope's room now: `memory` in MiB, `cpu` in MHz.
    pub room: std::collections::BTreeMap<String, f64>,
}

/// The egress backend until the datapath's is wired: every request is
/// refused as `EGRESS_UNAVAILABLE`.
#[derive(Debug, Default)]
pub struct NoEgress;

#[tonic::async_trait]
impl EgressBackend for NoEgress {
    async fn fetch(
        &self,
        _task: &IsolateTaskRef,
        _request: crate::structs::isolate_host::EgressRequest,
    ) -> crate::structs::isolate_host::EgressReply {
        crate::structs::isolate_host::EgressReply {
            error: Some(crate::structs::isolate_host::EgressError {
                kind: crate::structs::isolate_host::EgressErrorKind::EgressUnavailable as i32,
                message: "this node has no datapath for the task".into(),
            }),
            ..Default::default()
        }
    }
}

/// The VM backend until the datapath's is wired: `VM_NOT_CONFIGURED`.
#[derive(Debug, Default)]
pub struct NoVm;

#[tonic::async_trait]
impl VmBackend for NoVm {
    async fn call(
        &self,
        _task: &IsolateTaskRef,
        _vm_task: Option<&str>,
        _request: crate::structs::isolate_host::VmRequest,
    ) -> crate::structs::isolate_host::VmReply {
        crate::structs::isolate_host::VmReply {
            error: Some(crate::structs::isolate_host::VmError {
                kind: crate::structs::isolate_host::VmErrorKind::VmNotConfigured as i32,
                message: "child VMs are not available to isolates on this node".into(),
            }),
            ..Default::default()
        }
    }

    async fn owner_ended(&self, _task: &IsolateTaskRef, _reason: &str) {}

    fn child_count(&self, _job_id: &str, _task_name: &str) -> u32 {
        0
    }
}

/// Identifies the isolate task a host-to-worker request is made for, after the supervisor has
/// checked the request's Origin against the cordon the socket belongs to.
#[derive(Clone, Debug)]
pub struct IsolateTaskRef {
    pub job_id: String,
    pub task_name: String,
    /// The task's envelope allocation on this node: the datapath identity.
    pub envelope_allocation_id: i64,
    pub isolate_id: String,
    pub tenant: String,
    pub session: bool,
}

#[tonic::async_trait]
pub trait EgressBackend: Send + Sync + 'static {
    /// One outbound HTTP request under the task's connect upstreams, egress policy, handlers and credential injection.
    async fn fetch(
        &self,
        task: &IsolateTaskRef,
        request: crate::structs::isolate_host::EgressRequest,
    ) -> crate::structs::isolate_host::EgressReply;
}

#[tonic::async_trait]
pub trait VmBackend: Send + Sync + 'static {
    /// One `legion:isolate/vm` operation: child sessions of the task's VM task, owned by the isolate.
    async fn call(
        &self,
        task: &IsolateTaskRef,
        vm_task: Option<&str>,
        request: crate::structs::isolate_host::VmRequest,
    ) -> crate::structs::isolate_host::VmReply;
    /// The isolate ended (its request finished, its session was closed): end the children it owns.
    async fn owner_ended(&self, task: &IsolateTaskRef, reason: &str);
    /// Child VMs currently owned by this task's isolates on the node (for stats).
    fn child_count(&self, job_id: &str, task_name: &str) -> u32;
}

#[tonic::async_trait]
pub trait IngressTarget: Send + Sync + 'static {
    /// One inbound HTTP request for an isolate task (the datapath's ingress listener calls this).
    /// `tenant` / `session_id` come from the Legion-Tenant / Legion-Session request headers.
    async fn handle(
        &self,
        job_id: &str,
        task_name: &str,
        tenant: &str,
        session_id: &str,
        request: crate::structs::isolate_host::HttpCall,
    ) -> Result<crate::structs::isolate_host::HttpOutcome, tonic::Status>;
}
