//! The isolate supervisor: cordon hosts, modules, tasks and session isolates
//! on this node. The datapath side of an isolate (egress, ingress, child VMs)
//! lives in [`egress`], [`ingress`] and [`vm`].

pub mod egress;
pub mod ingress;
pub mod vm;

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
