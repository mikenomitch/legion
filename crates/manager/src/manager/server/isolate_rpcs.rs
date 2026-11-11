//! The isolate RPCs (docs/task-runtime-isolate.md, "Observing"):
//! `GetIsolateStats` from the heartbeat aggregates this manager keeps
//! ([`crate::manager::isolates`]), and `GetIsolate`, a read forwarded to the
//! node a session isolate's id names. Managers keep no record of isolates
//! (isolate-runtime calls entry 3).

use super::*;
use crate::manager::{isolates, sessions};
use crate::structs::{
    GetIsolateReply, GetIsolateRequest, GetIsolateStatsReply, GetIsolateStatsRequest,
    IsolateNodeStats,
};

/// Bound on one forwarded isolate read.
const ISOLATE_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on asking a node directly for its isolate stats, when this
/// manager holds no heartbeat of it (it heartbeats to a peer).
const STATS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

impl ManagerDaemonService {
    /// GetIsolateStats: the latest aggregate of a job's isolate tasks from
    /// every node with an envelope for them.
    pub(super) async fn get_isolate_stats_rpc(
        &self,
        request: Request<GetIsolateStatsRequest>,
    ) -> Result<Response<GetIsolateStatsReply>, Status> {
        // The scope comes from the body: authenticate, then enforce before
        // the job is read, so a caller without access cannot tell which
        // jobs exist.
        let ident = self.authenticate_acl(&request)?;
        let request = request.into_inner();
        let job_id = request.job_id.trim();
        if job_id.is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }
        acl::enforce(
            &ident,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job(job_id),
        )?;
        let config = self
            .store
            .get_job_config(job_id)
            .map_err(|_| Status::not_found(format!("job {job_id} not found")))?;
        let tasks = isolates::stats_tasks(&config.name, &config.tasks, &request.task_name)
            .map_err(Status::failed_precondition)?;
        let mut nodes = {
            let table = self
                .isolate_stats
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            table.for_job(&config.name, &tasks, std::time::Instant::now())
        };
        // A node that heartbeats to a peer manager is asked directly, so
        // the reply covers every node with an envelope whichever manager
        // answers. A node that does not answer in time is left out.
        let mut missing: Vec<String> = Vec::new();
        for task in &tasks {
            for node_id in sessions::envelope_nodes(&self.store, &config.name, task)
                .unwrap_or_default()
            {
                if !nodes.iter().any(|node| node.node_id == node_id) && !missing.contains(&node_id)
                {
                    missing.push(node_id);
                }
            }
        }
        let probes = missing.into_iter().map(|node_id| {
            let store = self.store.clone();
            let job_id = config.name.clone();
            let tasks = tasks.clone();
            async move { probe_node_stats(&store, &node_id, &job_id, &tasks).await }
        });
        nodes.extend(
            futures::future::join_all(probes)
                .await
                .into_iter()
                .flatten(),
        );
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(Response::new(GetIsolateStatsReply { nodes }))
    }

    /// GetIsolate: describe one session isolate by asking its node.
    pub(super) async fn get_isolate_rpc(
        &self,
        request: Request<GetIsolateRequest>,
    ) -> Result<Response<GetIsolateReply>, Status> {
        // Coarse first: the isolate's job is known only once its node
        // answers.
        let ident = self.check_acl(
            &request,
            AclResource::Job,
            AclAction::Read,
            AclScope::any(),
        )?;
        let request = request.into_inner();
        let session_id = request.session_id.trim().to_string();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id is required"));
        }
        let nodes = isolate_nodes(&self.store, &session_id, &request.node_id)?;
        let mut unreachable: Option<Status> = None;
        for node_id in &nodes {
            match get_isolate_on_node(&self.store, node_id, &session_id).await {
                Ok(mut reply) => {
                    acl::enforce(
                        &ident,
                        AclResource::Job,
                        AclAction::Read,
                        &AclScope::job(&reply.job_id),
                    )?;
                    if reply.node_id.is_empty() {
                        reply.node_id = node_id.clone();
                    }
                    return Ok(Response::new(reply));
                }
                Err(status) if status.code() == tonic::Code::NotFound => {}
                Err(status) => {
                    unreachable.get_or_insert(status);
                }
            }
        }
        Err(match unreachable {
            Some(status) => status,
            None => Status::not_found(format!(
                "node {} holds no session isolate {session_id}",
                nodes.join(", ")
            )),
        })
    }
}

/// The nodes to ask about a session isolate: `node_id` when the caller
/// named one, else the nodes whose tag the id carries.
pub(super) fn isolate_nodes(
    store: &Store,
    session_id: &str,
    node_id: &str,
) -> Result<Vec<String>, Status> {
    let node_id = node_id.trim();
    if !node_id.is_empty() {
        store
            .get_node(node_id)
            .map_err(|_| Status::not_found(format!("node {node_id} not found")))?;
        return Ok(vec![node_id.to_string()]);
    }
    let Some(tag) = crate::structs::session_node_tag(session_id) else {
        return Err(Status::invalid_argument(format!(
            "session id {session_id} carries no node tag (an id derived from an idempotency \
             key); name its node with node_id (--on-node)"
        )));
    };
    let nodes = sessions::nodes_for_tag(store, tag);
    if nodes.is_empty() {
        return Err(Status::not_found(format!(
            "no node matches the tag of session isolate {session_id} (the node left the \
             cluster, or the id is mistyped)"
        )));
    }
    Ok(nodes)
}

/// Ask one node for a session isolate (`Worker.GetIsolate`). The node's
/// NotFound passes through; a node that cannot be reached answers
/// UNAVAILABLE naming it.
async fn get_isolate_on_node(
    store: &Store,
    node_id: &str,
    session_id: &str,
) -> Result<GetIsolateReply, Status> {
    let (mut client, target) = sessions::worker_client(store, node_id)?;
    let mut call = tonic::Request::new(GetIsolateRequest {
        session_id: session_id.to_string(),
        node_id: String::new(),
        ticket: String::new(),
    });
    call.set_timeout(ISOLATE_CALL_TIMEOUT);
    match client.get_isolate(call).await {
        Ok(reply) => Ok(reply.into_inner()),
        Err(status) if status.code() == tonic::Code::NotFound => Err(status),
        Err(status) => {
            let status = crate::manager::worker_plane::worker_call_error(
                &target.node_id,
                &target.address,
                status,
            );
            Err(if crate::manager::worker_plane::is_unreachable(&status) {
                Status::unavailable(format!(
                    "node {node_id} holds session isolate {session_id} and could not be \
                     reached ({})\nHint: retry when the node is back (`legion node info \
                     {node_id}`)",
                    status.message().split("\nHint:").next().unwrap_or_default()
                ))
            } else {
                status
            })
        }
    }
}

/// Ask a node this manager holds no heartbeat of for its isolate stats
/// (`Worker.GetIsolateStats`), filtered to `tasks` of `job_id`. `None` when
/// it does not answer in time or reports none of them.
async fn probe_node_stats(
    store: &Store,
    node_id: &str,
    job_id: &str,
    tasks: &[String],
) -> Option<IsolateNodeStats> {
    let (mut client, _) = sessions::worker_client(store, node_id).ok()?;
    let mut call = tonic::Request::new(GetIsolateStatsRequest {
        job_id: job_id.to_string(),
        task_name: String::new(),
    });
    call.set_timeout(STATS_PROBE_TIMEOUT);
    let reply = match client.get_isolate_stats(call).await {
        Ok(reply) => reply.into_inner(),
        Err(status) => {
            debug!(node_id, error = %status, "Isolate stats probe failed");
            return None;
        }
    };
    let node = reply.nodes.into_iter().next()?;
    let found = isolates::filter_tasks(&node.tasks, job_id, tasks);
    if found.is_empty() {
        return None;
    }
    let hosts = isolates::hosts_serving(&node.hosts, &found);
    Some(IsolateNodeStats {
        node_id: node_id.to_string(),
        tasks: found,
        hosts,
        age_ms: 0,
    })
}
