//! Server-side leader forwarding for Manager RPCs (spot-check P2-5).
//!
//! In raft mode only the leader commits writes. A write RPC that reaches a
//! follower used to fail with `FAILED_PRECONDITION` naming the leader's raft
//! advertise address — often a private address the caller (a laptop, a CI
//! runner, a worker whose token names a follower) cannot reach, and the CLI
//! never followed the hint anyway. Now the follower proxies the request to
//! the leader itself, over the same manager-to-manager path the raft
//! transport and lane-write forwarding already use, and returns the leader's
//! reply. Any manager accepts any request.
//!
//! **How it decides.** The layer sits inside the gRPC-Web translation, so it
//! sees native gRPC. For a unary Manager request on a manager that does not
//! lead every raft lane, it buffers the request, runs it locally, and looks
//! at the reply: only the typed not-the-leader rejection
//! (`FAILED_PRECONDITION` carrying the `x-legion-raft-leader` metadata that
//! `map_store_error` attaches) triggers forwarding, and the request is then
//! replayed verbatim — headers, bearer credential, idempotency key, body —
//! to the leader named there. Reads, and writes that succeeded, never leave
//! this manager. Replaying a rejected request is safe for the same reason a
//! client retry against the leader is: the rejection happens before any
//! capture runs, so nothing was written here (the composite case under
//! `lanes` topology is the documented client-retry shape, which converges).
//!
//! **What it never does.** It never forwards a request twice (the leader
//! sees `x-legion-forwarded-by` and answers itself), never forwards the one
//! client-streaming RPC (`ExecAllocation`, which is not a store write), and
//! never forwards the manager-plane `RaftTransport` service. When the leader
//! cannot be reached, the caller gets the original rejection with an
//! explanation of what was tried, which names the leader's advertise address
//! as exactly that.
//!
//! **Authorization** is the leader's: the caller's bearer credential travels
//! with the request, and the leader runs the same ACL check the follower did.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use http::{HeaderValue, Request, Response};
use http_body_util::BodyExt;
use tonic::body::BoxBody;
use tower::ServiceExt;

use crate::prelude::*;
use crate::storage::store::Store;

/// Status metadata naming the leader (`host:port`) a not-the-leader
/// rejection should be retried against. Attached by `map_store_error`; read
/// here to decide whether, and where, to forward.
pub const RAFT_LEADER_METADATA_KEY: &str = "x-legion-raft-leader";

/// Request header a forwarding manager stamps with its own raft node id.
/// A request carrying it is never forwarded again: one hop, no loops.
pub const FORWARDED_BY_HEADER: &str = "x-legion-forwarded-by";

/// Path prefix of the public Manager service.
const MANAGER_SERVICE_PREFIX: &str = "/legion.Manager/";

/// Manager RPCs with a streaming REQUEST, which cannot be buffered and
/// replayed. Kept in sync with `proto/legion.proto` by
/// `streaming_rpcs_match_the_proto`.
const CLIENT_STREAMING_RPCS: &[&str] = &["ExecAllocation"];

/// Largest request body buffered for a possible replay. Above tonic's 4 MiB
/// default decode limit, so anything the handler would accept fits.
const MAX_BUFFERED_REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// How long a forwarded request may take end to end.
const FORWARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Tower layer that forwards not-the-leader rejections to the leader.
#[derive(Clone)]
pub struct LeaderForwardLayer {
    store: Store,
    /// This manager's raft node id; `None` outside raft mode, where the layer
    /// is a pure passthrough.
    node_id: Option<String>,
}

impl LeaderForwardLayer {
    pub fn new(store: Store) -> Self {
        let node_id = store.raft_node_id();
        Self { store, node_id }
    }
}

impl<S> tower::Layer<S> for LeaderForwardLayer {
    type Service = LeaderForwardService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LeaderForwardService {
            inner,
            store: self.store.clone(),
            node_id: self.node_id.clone(),
        }
    }
}

/// Service produced by [`LeaderForwardLayer`].
#[derive(Clone)]
pub struct LeaderForwardService<S> {
    inner: S,
    store: Store,
    node_id: Option<String>,
}

/// Whether `path` is a Manager RPC whose request can be buffered.
fn forwardable_path(path: &str) -> bool {
    path.strip_prefix(MANAGER_SERVICE_PREFIX)
        .is_some_and(|method| !CLIENT_STREAMING_RPCS.contains(&method))
}

/// The leader a reply says to retry against, when the reply is the typed
/// not-the-leader rejection. Errors are trailers-only responses, so the
/// status and its metadata are in the headers.
fn leader_from_rejection(headers: &http::HeaderMap) -> Option<String> {
    let status = tonic::Status::from_header_map(headers)?;
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    headers
        .get(RAFT_LEADER_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(str::to_string)
}

impl<S> tower::Service<Request<BoxBody>> for LeaderForwardService<S>
where
    S: tower::Service<Request<BoxBody>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let Some(node_id) = self.node_id.clone() else {
            return Box::pin(self.inner.call(req));
        };
        if !forwardable_path(req.uri().path())
            || req.headers().contains_key(FORWARDED_BY_HEADER)
            || self.store.leads_every_raft_lane()
        {
            return Box::pin(self.inner.call(req));
        }
        // The readied service runs this request; a fresh clone takes its
        // place for the next one (the standard tower clone-and-swap).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let store = self.store.clone();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let bytes = match http_body_util::Limited::new(body, MAX_BUFFERED_REQUEST_BYTES)
                .collect()
                .await
            {
                Ok(collected) => collected.to_bytes(),
                Err(err) => {
                    return Ok(tonic::Status::resource_exhausted(format!(
                        "request body could not be read for leader forwarding: {err}"
                    ))
                    .into_http());
                }
            };
            let replay_head = (
                parts.method.clone(),
                parts.uri.clone(),
                parts.version,
                parts.headers.clone(),
            );
            let local = inner
                .call(Request::from_parts(parts, full_body(bytes.clone())))
                .await?;
            let Some(leader) = leader_from_rejection(local.headers()) else {
                return Ok(local);
            };
            let method = replay_head.1.path().to_string();
            match forward(&store, &node_id, &leader, replay_head, bytes).await {
                Ok(response) => {
                    debug!(%method, %leader, "Forwarded a write to the raft leader");
                    Ok(response)
                }
                Err(err) => {
                    warn!(
                        %method,
                        %leader,
                        error = %err,
                        "Could not forward a write to the raft leader; returning the rejection"
                    );
                    let original = tonic::Status::from_header_map(local.headers())
                        .map(|status| status.message().to_string())
                        .unwrap_or_default();
                    let mut status = tonic::Status::failed_precondition(format!(
                        "{original} (this manager tried to forward the request to the leader at \
                         {leader}, its raft advertise address, and failed: {err}. Retry against \
                         any manager once a leader is reachable from it, or point LEGION_ADDR at \
                         the leader's API address)"
                    ));
                    if let Ok(value) = leader.parse() {
                        status
                            .metadata_mut()
                            .insert(RAFT_LEADER_METADATA_KEY, value);
                    }
                    Ok(status.into_http())
                }
            }
        })
    }
}

fn full_body(bytes: Bytes) -> BoxBody {
    tonic::body::boxed(http_body_util::Full::new(bytes))
}

/// Replay one buffered request against the leader.
async fn forward(
    store: &Store,
    node_id: &str,
    leader: &str,
    (method, uri, version, mut headers): (http::Method, http::Uri, http::Version, http::HeaderMap),
    body: Bytes,
) -> R<Response<BoxBody>> {
    let channel = store
        .raft_peer_channel(leader)
        .with_context(|| format!("no channel to the leader at {leader}"))?;
    // Connection-scoped headers belong to the caller's hop (a browser's
    // HTTP/1.1 gRPC-Web request arrives with some); HTTP/2 refuses them.
    for hop in [
        http::header::CONNECTION,
        http::header::HOST,
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
        http::header::CONTENT_LENGTH,
    ] {
        headers.remove(hop);
    }
    headers.remove("keep-alive");
    headers.remove("proxy-connection");
    headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
    headers.insert(
        FORWARDED_BY_HEADER,
        HeaderValue::from_str(node_id).unwrap_or_else(|_| HeaderValue::from_static("manager")),
    );
    let mut request = Request::builder()
        .method(method)
        .uri(
            uri.path_and_query()
                .map(|pq| pq.as_str().to_string())
                .unwrap_or_else(|| uri.path().to_string()),
        )
        .version(version)
        .body(full_body(body))?;
    *request.headers_mut() = headers;
    let response = tokio::time::timeout(FORWARD_TIMEOUT, channel.oneshot(request))
        .await
        .map_err(|_| anyhow!("timed out after {FORWARD_TIMEOUT:?}"))?
        .map_err(|err| anyhow!("{err}"))?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_unary_manager_rpcs_are_forwardable() {
        assert!(forwardable_path("/legion.Manager/RunJob"));
        assert!(forwardable_path("/legion.Manager/WorkerRegister"));
        assert!(!forwardable_path("/legion.Manager/ExecAllocation"));
        assert!(!forwardable_path("/legion.RaftTransport/AppendEntries"));
        assert!(!forwardable_path("/legion.Worker/PostWork"));
        assert!(!forwardable_path("/healthz"));
    }

    /// Every Manager RPC with a streaming request must be on the skip list:
    /// buffering one would hang until the client closes its stream.
    #[test]
    fn streaming_rpcs_match_the_proto() {
        let proto = include_str!("../../proto/legion.proto");
        let manager = proto
            .split("service Manager {")
            .nth(1)
            .and_then(|rest| rest.split("\nservice ").next())
            .expect("Manager service in proto");
        let mut streaming: Vec<&str> = manager
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("rpc ") && line.contains("(stream "))
            .filter_map(|line| line.strip_prefix("rpc ")?.split_whitespace().next())
            .map(|name| name.trim_end_matches('('))
            .collect();
        streaming.sort();
        let mut expected = CLIENT_STREAMING_RPCS.to_vec();
        expected.sort();
        assert_eq!(streaming, expected);
    }

    #[test]
    fn only_the_typed_rejection_names_a_leader() {
        let mut headers = http::HeaderMap::new();
        let mut status = tonic::Status::failed_precondition("this manager is not the raft leader");
        status
            .metadata_mut()
            .insert(RAFT_LEADER_METADATA_KEY, "10.0.0.2:4747".parse().unwrap());
        status.add_header(&mut headers).unwrap();
        assert_eq!(
            leader_from_rejection(&headers).as_deref(),
            Some("10.0.0.2:4747")
        );

        // A precondition failure WITHOUT the marker is a real refusal.
        let mut plain = http::HeaderMap::new();
        tonic::Status::failed_precondition("job must be stopped first")
            .add_header(&mut plain)
            .unwrap();
        assert_eq!(leader_from_rejection(&plain), None);

        // Any other code is answered locally, marker or not.
        let mut other = http::HeaderMap::new();
        let mut status = tonic::Status::not_found("no such job");
        status
            .metadata_mut()
            .insert(RAFT_LEADER_METADATA_KEY, "10.0.0.2:4747".parse().unwrap());
        status.add_header(&mut other).unwrap();
        assert_eq!(leader_from_rejection(&other), None);
    }
}
