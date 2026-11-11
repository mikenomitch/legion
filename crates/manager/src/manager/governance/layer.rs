//! The governance tower layer: wraps every `/legion.Manager/*` request with a
//! [`RequestAudit`] context and journals it on completion.
//!
//! It sits innermost in the manager's gRPC stack, inside leader forwarding
//! (`crates/manager/src/manager/daemon.rs`): a follower that rejects a write as
//! not-the-leader sees that rejection here and skips it (the leader journals
//! the replay), so each request is journaled once, by the manager that
//! executed it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use http::{Request, Response};
use http_body_util::BodyExt;
use tonic::body::BoxBody;
use tonic::Status;

use super::{digest, Governance, RequestAudit, APPROVAL_HEADER};

/// Path prefix of the public Manager service.
const MANAGER_PREFIX: &str = "/legion.Manager/";

/// Largest request body buffered for the digest (above tonic's decode
/// limit, so anything a handler would accept fits).
const MAX_DIGEST_BODY: usize = 64 * 1024 * 1024;

/// Tower layer installing the governance context on Manager RPCs.
#[derive(Clone)]
pub struct GovernanceLayer {
    governance: Arc<Governance>,
}

impl GovernanceLayer {
    pub fn new(governance: Arc<Governance>) -> Self {
        GovernanceLayer { governance }
    }
}

impl<S> tower::Layer<S> for GovernanceLayer {
    type Service = GovernanceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GovernanceService {
            inner,
            governance: self.governance.clone(),
        }
    }
}

/// The service [`GovernanceLayer`] produces.
#[derive(Clone)]
pub struct GovernanceService<S> {
    inner: S,
    governance: Arc<Governance>,
}

impl<S> tower::Service<Request<BoxBody>> for GovernanceService<S>
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
        let Some(operation) = req
            .uri()
            .path()
            .strip_prefix(MANAGER_PREFIX)
            .map(str::to_string)
        else {
            return Box::pin(self.inner.call(req));
        };
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let governance = self.governance.clone();
        Box::pin(async move {
            let approval_id = req
                .headers()
                .get(APPROVAL_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let streaming =
                crate::manager::leader_forward::CLIENT_STREAMING_RPCS.contains(&operation.as_str());
            let (req, request_digest) = if streaming {
                (req, String::new())
            } else {
                let (parts, body) = req.into_parts();
                let bytes = match http_body_util::Limited::new(body, MAX_DIGEST_BODY)
                    .collect()
                    .await
                {
                    Ok(collected) => collected.to_bytes(),
                    Err(err) => {
                        return Ok(Status::resource_exhausted(format!(
                            "request body could not be read: {err}"
                        ))
                        .into_http());
                    }
                };
                let request_digest = digest::request_digest(&bytes);
                (
                    Request::from_parts(
                        parts,
                        tonic::body::boxed(http_body_util::Full::new(bytes)),
                    ),
                    request_digest,
                )
            };
            let context = Arc::new(RequestAudit::new(
                governance.clone(),
                &operation,
                request_digest,
                approval_id,
            ));
            let response = context
                .clone()
                .scope(async move { inner.call(req).await })
                .await?;
            finish_from_headers(&governance, &context, response.headers());
            Ok(response)
        })
    }
}

/// Journals `context` from a response's headers. A trailers-only response
/// (every unary error) carries `grpc-status` in its headers; a response
/// without one is a success whose status rides the trailers.
fn finish_from_headers(governance: &Governance, context: &RequestAudit, headers: &http::HeaderMap) {
    let status = Status::from_header_map(headers);
    if let Some(status) = &status {
        // A not-the-leader rejection the leader-forward layer will replay:
        // the leader journals the replay.
        if status.code() != tonic::Code::Ok
            && headers.contains_key(crate::manager::leader_forward::RAFT_LEADER_METADATA_KEY)
        {
            return;
        }
    }
    let (outcome, message) = match &status {
        None => ("ok".to_string(), String::new()),
        Some(status) if status.code() == tonic::Code::Ok => ("ok".to_string(), String::new()),
        Some(status) => (
            headers
                .get(crate::manager::worker_plane::ERROR_CODE_METADATA)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{:?}", status.code())),
            status.message().to_string(),
        ),
    };
    if let Some(record) = context.record(&outcome, &message) {
        governance.submit(record);
    }
}

/// The journal outcome for a handler result: `ok`, the typed Legion code,
/// or the gRPC code name.
pub fn outcome_of<T>(result: &Result<T, Status>) -> (String, String) {
    match result {
        Ok(_) => ("ok".into(), String::new()),
        Err(status) => (
            status
                .metadata()
                .get(crate::manager::worker_plane::ERROR_CODE_METADATA)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{:?}", status.code())),
            status.message().to_string(),
        ),
    }
}

/// Runs a handler call under a governance context and journals it, as the
/// layer would: for callers that invoke the service in process (tests, and
/// embedders that do not serve gRPC). `request_digest` is the caller's
/// (see [`digest::message_digest`]).
pub async fn audited<T, F>(
    governance: &Arc<Governance>,
    operation: &str,
    request_digest: String,
    approval_id: Option<String>,
    call: F,
) -> Result<T, Status>
where
    F: Future<Output = Result<T, Status>>,
{
    let context = Arc::new(RequestAudit::new(
        governance.clone(),
        operation,
        request_digest,
        approval_id,
    ));
    let result = context.clone().scope(call).await;
    let (outcome, message) = outcome_of(&result);
    if let Some(record) = context.record(&outcome, &message) {
        governance.submit(record);
    }
    result
}

impl Governance {
    /// Hands a record to the journal writer, or writes it synchronously when
    /// no writer runs (in-process callers, tests).
    pub fn submit(&self, record: crate::storage::governance_store::AuditRecord) {
        if !self.config.audit_enabled() {
            return;
        }
        match &self.journal {
            Some(journal) => journal.submit(record),
            None => {
                if let Err(err) = self.store.append_audit_events(vec![record]) {
                    tracing::warn!(target: "audit", error = %err, "audit record could not be journaled");
                }
            }
        }
    }
}
