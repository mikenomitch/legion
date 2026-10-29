//! The worker's half of app view and action delivery (docs/apps.md, "Views
//! and actions"; apps plan §6.5): `Worker.ForwardAppRequest`, the unary
//! subset of a worker-plane tunnel that app views need until a general
//! tunnel exists (manager-reverse-proxy plan §6.5).
//!
//! The caller is a manager (the plane's `manager` class), which already
//! authorized the viewer and signed the viewer assertion the request
//! carries. The worker checks only what keeps this from being a general
//! proxy:
//!
//! - the allocation runs here now, its job is the one the manager named,
//!   and that job is in the reserved `legion-system` namespace (an app's);
//! - the allocation declares the port label;
//! - GET or POST, a path under `/legion/v1/`, three headers, a bounded
//!   body.
//!
//! It dials the port the way the ingress datapath does
//! ([`crate::worker::netd::ingress::TaskTarget::connect`]): loopback, inside
//! the task's network namespace when it is gated, or a sandbox guest over
//! vsock or its bridge address. Never an address the caller names. The UI
//! port is not a catalog service, and nothing else on the node routes to it
//! on the app's behalf.

use crate::prelude::*;
use crate::structs::namespace::{split_job_id, SYSTEM_NAMESPACE};
use crate::structs::{AppHttpHeader, ForwardAppRequestReply, ForwardAppRequestRequest};
use crate::worker::netd::ingress::TaskTarget;
use std::time::Duration;
use tonic::Status;

/// Largest request body (an action's context and inputs).
pub const MAX_REQUEST_BODY: usize = 256 * 1024;
/// Largest answer the worker reads.
pub const MAX_RESPONSE_BODY: usize = 1024 * 1024;
/// How long the app has to answer, from the dial.
pub const APP_ANSWER_TIMEOUT: Duration = Duration::from_secs(10);
/// The only path prefix the worker delivers.
pub const PATH_PREFIX: &str = "/legion/v1/";
/// The headers that pass; everything else the manager sent is dropped.
const PASSED_HEADERS: &[&str] = &["legion-viewer", "content-type", "accept"];

/// Check a request against the limits, before anything is looked up.
pub fn check_request(request: &ForwardAppRequestRequest) -> Result<(), Status> {
    if !matches!(request.method.as_str(), "GET" | "POST") {
        return Err(Status::invalid_argument(format!(
            "ForwardAppRequest delivers GET and POST only, not {:?}",
            request.method
        )));
    }
    let path_ok = request.path.starts_with(PATH_PREFIX)
        && !request.path.contains("..")
        && request.path.len() <= 4096
        && request
            .path
            .chars()
            .all(|c| c.is_ascii_graphic() && c != '#');
    if !path_ok {
        return Err(Status::invalid_argument(format!(
            "ForwardAppRequest delivers paths under {PATH_PREFIX} only"
        )));
    }
    if request.body.len() > MAX_REQUEST_BODY {
        return Err(Status::invalid_argument(format!(
            "the request body is {} bytes; the limit is {MAX_REQUEST_BODY}",
            request.body.len()
        )));
    }
    if request.port.trim().is_empty() {
        return Err(Status::invalid_argument("a port label is required"));
    }
    let (namespace, _) = split_job_id(&request.job_id);
    if namespace != SYSTEM_NAMESPACE {
        return Err(Status::failed_precondition(format!(
            "ForwardAppRequest delivers to apps only (jobs in {SYSTEM_NAMESPACE}), not to {:?}",
            request.job_id
        )));
    }
    Ok(())
}

/// Send `request` to the task behind `target` and read its answer.
pub async fn deliver(
    target: &TaskTarget,
    request: &ForwardAppRequestRequest,
) -> Result<ForwardAppRequestReply, Status> {
    let exchange = async {
        let stream = target.connect().await.map_err(|err| {
            Status::unavailable(format!(
                "could not reach port {:?} of allocation {}: {err:#}",
                request.port, request.allocation_id
            ))
        })?;
        exchange(stream, request).await
    };
    match tokio::time::timeout(APP_ANSWER_TIMEOUT, exchange).await {
        Ok(result) => result,
        Err(_) => Err(Status::deadline_exceeded(format!(
            "allocation {} did not answer {} within {}s",
            request.allocation_id,
            request.path.split('?').next().unwrap_or_default(),
            APP_ANSWER_TIMEOUT.as_secs()
        ))),
    }
}

/// One HTTP/1.1 exchange over an open connection.
pub async fn exchange<S>(
    stream: S,
    request: &ForwardAppRequestRequest,
) -> Result<ForwardAppRequestReply, Status>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use http_body_util::{BodyExt, Full, Limited};
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(|err| Status::unavailable(format!("HTTP handshake with the app: {err}")))?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            debug!(error = %err, "App connection ended");
        }
    });
    let mut builder = http::Request::builder()
        .method(request.method.as_str())
        .uri(request.path.as_str())
        .header(http::header::HOST, "legion-app")
        .header(http::header::CONNECTION, "close");
    for AppHttpHeader { name, value } in &request.headers {
        let name = name.trim().to_ascii_lowercase();
        if PASSED_HEADERS.contains(&name.as_str()) {
            builder = builder.header(name, value.as_str());
        }
    }
    let http_request = builder
        .body(Full::new(bytes::Bytes::from(request.body.clone())))
        .map_err(|err| Status::invalid_argument(format!("building the request: {err}")))?;
    // From here on the request may have reached the app: a failure is not
    // Unavailable, so the manager never sends it to a second allocation.
    let response = sender
        .send_request(http_request)
        .await
        .map_err(|err| Status::aborted(format!("the app's connection failed: {err}")))?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = Limited::new(response.into_body(), MAX_RESPONSE_BODY)
        .collect()
        .await
        .map_err(|err| {
            Status::resource_exhausted(format!(
                "reading the app's answer (at most {MAX_RESPONSE_BODY} bytes): {err}"
            ))
        })?
        .to_bytes();
    Ok(ForwardAppRequestReply {
        status: u32::from(status),
        content_type,
        body: body.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::netd::ingress::TaskDial;

    fn request(path: &str) -> ForwardAppRequestRequest {
        ForwardAppRequestRequest {
            allocation_id: 7,
            job_id: "legion-system/fake".into(),
            port: "api".into(),
            method: "GET".into(),
            path: path.into(),
            headers: vec![
                AppHttpHeader {
                    name: "legion-viewer".into(),
                    value: "assertion".into(),
                },
                AppHttpHeader {
                    name: "authorization".into(),
                    value: "Bearer secret".into(),
                },
            ],
            body: Vec::new(),
        }
    }

    #[test]
    fn only_app_requests_under_the_prefix_pass() {
        check_request(&request("/legion/v1/views/job?slot=job")).expect("valid");
        assert!(check_request(&request("/admin")).is_err());
        assert!(check_request(&request("/legion/v1/../admin")).is_err());
        let mut other_job = request("/legion/v1/views/job");
        other_job.job_id = "default/web".into();
        assert_eq!(
            check_request(&other_job).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        let mut delete = request("/legion/v1/views/job");
        delete.method = "DELETE".into();
        assert!(check_request(&delete).is_err());
        let mut big = request("/legion/v1/actions/go");
        big.method = "POST".into();
        big.body = vec![0; MAX_REQUEST_BODY + 1];
        assert!(check_request(&big).is_err());
    }

    /// The worker dials the task's own port on loopback, passes only the
    /// allowed headers, and returns the answer.
    #[tokio::test]
    async fn delivers_to_the_port_and_drops_other_headers() -> R<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut seen = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let read = stream.read(&mut buf).await?;
                seen.extend_from_slice(&buf[..read]);
                if read == 0 || seen.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
                )
                .await?;
            Ok::<_, anyhow::Error>(String::from_utf8_lossy(&seen).to_string())
        });
        let target = TaskTarget {
            dial: TaskDial::Loopback { netns: None },
            label: "api".into(),
            port,
        };
        let reply = deliver(&target, &request("/legion/v1/views/job?slot=job")).await?;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.content_type, "application/json");
        assert_eq!(reply.body, b"{\"ok\":true}");
        let seen = server.await??.to_ascii_lowercase();
        assert!(
            seen.starts_with("get /legion/v1/views/job?slot=job http/1.1"),
            "{seen}"
        );
        assert!(seen.contains("legion-viewer: assertion"), "{seen}");
        assert!(!seen.contains("authorization"), "{seen}");

        // Nothing listening: Unavailable, so the manager may try another
        // allocation.
        let closed = TaskTarget {
            dial: TaskDial::Loopback { netns: None },
            label: "api".into(),
            port: free_port()?,
        };
        let err = deliver(&closed, &request("/legion/v1/views/job"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable, "{err}");
        Ok(())
    }

    fn free_port() -> R<u16> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    }
}
