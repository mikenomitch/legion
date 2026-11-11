//! HTTP ingress to isolates.
//!
//! An isolate task's advertised ports (`network.ports`) ride its envelope
//! allocation, so the node's ingress listener for each port is bound for the
//! envelope like any other (TLS termination, the ingress chain, fail-closed
//! while policy is pending). What it dials is `TaskDial::Isolate`: a Unix
//! socket per port label in the envelope's socket dir
//! (`netd::attach::isolate_ingress_socket`), which [`serve`] binds and
//! answers. Each HTTP/1.1 request on it becomes one `HttpCall` for the
//! supervisor's [`IngressTarget`]:
//!
//! - `Legion-Tenant` names the tenant (checked with
//!   `validate_tenant_key`; absent means the task's anonymous tenant) and
//!   `Legion-Session` the session isolate; both are removed before the guest
//!   sees the request, with the hop-by-hop headers.
//! - The body is buffered up to the worker's `isolate.maxBodyBytes`; more is
//!   a `413`.
//! - A refusal from the supervisor maps to an HTTP status:
//!   `RESOURCE_EXHAUSTED` → `503` with `Retry-After: 1`, `NOT_FOUND` →
//!   `404`, `FAILED_PRECONDITION` → `502` with the message, `UNAVAILABLE` →
//!   `503`, `DEADLINE_EXCEEDED` → `504`, `INVALID_ARGUMENT` → `400`,
//!   `PERMISSION_DENIED` → `403`, anything else → `500`.

use crate::prelude::*;
use crate::structs::isolate::{validate_tenant_key, SESSION_HEADER, TENANT_HEADER};
use crate::structs::isolate_host::{Header, HttpCall, HttpOutcome};
use crate::worker::isolates::egress::{
    collect_limited, connection_tokens, inbound_headers, is_hop_by_hop, BodyError,
};
use crate::worker::isolates::IngressTarget;
use crate::worker::netd::listen::{spawn_accept_loop, ListenAddr};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{header, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

/// One served ingress socket. Dropping it stops serving and removes the
/// socket, so ingress answers "not reachable" instead of hanging.
pub struct IngressServer {
    path: PathBuf,
    task: JoinHandle<()>,
}

impl IngressServer {
    /// The socket this server answers on.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for IngressServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

struct Served {
    job_id: String,
    task_name: String,
    max_body_bytes: u64,
    target: Arc<dyn IngressTarget>,
}

/// Serve isolate ingress for one port on `socket` (replacing a stale socket
/// left behind, creating its directory): every request on it goes to
/// `target` for `job_id`/`task_name`. Stops when the returned server is
/// dropped.
pub async fn serve(
    socket: PathBuf,
    job_id: &str,
    task_name: &str,
    max_body_bytes: u64,
    target: Arc<dyn IngressTarget>,
) -> R<IngressServer> {
    let listener = ListenAddr::Unix(socket.clone())
        .bind()
        .await
        .with_context(|| format!("bind isolate ingress socket {}", socket.display()))?;
    let served = Arc::new(Served {
        job_id: job_id.to_string(),
        task_name: task_name.to_string(),
        max_body_bytes,
        target,
    });
    let task = spawn_accept_loop(listener, "isolate ingress", move |stream, _| {
        let served = served.clone();
        async move {
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let served = served.clone();
                async move { Ok::<_, std::convert::Infallible>(answer(&served, request).await) }
            });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!(error = ?err, "Isolate ingress connection ended with an error");
            }
        }
    });
    debug!(job_id, task = task_name, socket = %socket.display(), "Serving isolate ingress");
    Ok(IngressServer { path: socket, task })
}

/// [`serve`] for every advertised port of an isolate task: one socket per
/// port label in its envelope's socket dir (`<alloc_root>/<envelope>/.isolate`),
/// which is where the node's ingress listener for the envelope dials.
pub async fn serve_ports(
    alloc_root: &Path,
    envelope_allocation_id: i64,
    labels: &[String],
    job_id: &str,
    task_name: &str,
    max_body_bytes: u64,
    target: Arc<dyn IngressTarget>,
) -> R<Vec<IngressServer>> {
    let dir = crate::worker::netd::attach::isolate_dir(alloc_root, envelope_allocation_id);
    let mut servers = Vec::with_capacity(labels.len());
    for label in labels {
        let socket = crate::worker::netd::attach::isolate_ingress_socket(&dir, label);
        servers.push(serve(socket, job_id, task_name, max_body_bytes, target.clone()).await?);
    }
    Ok(servers)
}

async fn answer(served: &Served, request: Request<Incoming>) -> Response<Full<Bytes>> {
    let (parts, body) = request.into_parts();
    // A routing header given twice is ambiguous (which tenant? which
    // session?): refused rather than first-wins.
    for name in [TENANT_HEADER, SESSION_HEADER] {
        if parts.headers.get_all(name).iter().count() > 1 {
            return plain(
                StatusCode::BAD_REQUEST,
                &format!("{name} may appear at most once in a request"),
            );
        }
    }
    let header_text = |name: &str| {
        parts
            .headers
            .get(name)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).trim().to_string())
            .unwrap_or_default()
    };
    let tenant = header_text(TENANT_HEADER);
    if !tenant.is_empty() {
        if let Err(err) = validate_tenant_key(&tenant) {
            return plain(StatusCode::BAD_REQUEST, &format!("{TENANT_HEADER}: {err}"));
        }
    }
    let session_id = header_text(SESSION_HEADER);
    let body = match collect_limited(body, served.max_body_bytes).await {
        Ok(body) => body,
        Err(BodyError::TooLarge) => {
            return plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!(
                    "the request body is over this node's isolate.maxBodyBytes ({} bytes)",
                    served.max_body_bytes
                ),
            )
        }
        Err(BodyError::Read(err)) => {
            return plain(StatusCode::BAD_REQUEST, &format!("reading the body: {err}"))
        }
    };
    let call = http_call(&parts, body);
    match served
        .target
        .handle(
            &served.job_id,
            &served.task_name,
            &tenant,
            &session_id,
            call,
        )
        .await
    {
        Ok(outcome) => respond(outcome),
        Err(status) => refusal(&status),
    }
}

/// The guest's view of a request: hop-by-hop and the isolate routing
/// headers removed.
fn http_call(parts: &http::request::Parts, body: Bytes) -> HttpCall {
    let connection = connection_tokens(
        parts
            .headers
            .get_all(header::CONNECTION)
            .iter()
            .map(|value| value.as_bytes()),
    );
    let headers = parts
        .headers
        .iter()
        .filter(|(name, _)| {
            !is_hop_by_hop(name.as_str(), &connection)
                && !name.as_str().eq_ignore_ascii_case(TENANT_HEADER)
                && !name.as_str().eq_ignore_ascii_case(SESSION_HEADER)
        })
        .map(|(name, value)| Header {
            name: name.as_str().to_string(),
            value: value.as_bytes().to_vec(),
        })
        .collect();
    let authority = parts
        .uri
        .authority()
        .map(|authority| authority.as_str().to_string())
        .or_else(|| {
            parts
                .headers
                .get(header::HOST)
                .and_then(|host| host.to_str().ok())
                .map(str::to_string)
        })
        .unwrap_or_default();
    HttpCall {
        method: parts.method.as_str().to_string(),
        path: parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".into()),
        authority,
        // TLS, when the port has it, ends at the node's ingress listener.
        scheme: "http".into(),
        headers,
        body: body.to_vec(),
    }
}

fn respond(outcome: HttpOutcome) -> Response<Full<Bytes>> {
    let Some(status) = u16::try_from(outcome.status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok())
    else {
        return plain(
            StatusCode::BAD_GATEWAY,
            &format!("the isolate answered with status {}", outcome.status),
        );
    };
    let mut response = Response::new(Full::new(Bytes::from(outcome.body)));
    *response.status_mut() = status;
    let mut headers = header::HeaderMap::new();
    for h in &outcome.headers {
        let (Ok(name), Ok(value)) = (
            header::HeaderName::from_bytes(h.name.as_bytes()),
            header::HeaderValue::from_bytes(&h.value),
        ) else {
            continue;
        };
        headers.append(name, value);
    }
    // Hop-by-hop headers are this connection's, and the body is whole.
    for h in inbound_headers(&headers) {
        if let (Ok(name), Ok(value)) = (
            header::HeaderName::from_bytes(h.name.as_bytes()),
            header::HeaderValue::from_bytes(&h.value),
        ) {
            response.headers_mut().append(name, value);
        }
    }
    response
}

/// The HTTP answer for a refusal from the supervisor.
pub fn refusal(status: &tonic::Status) -> Response<Full<Bytes>> {
    let code = refusal_status(status.code());
    let mut response = plain(code, status.message());
    if status.code() == tonic::Code::ResourceExhausted {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
    }
    response
}

/// The HTTP status for a gRPC code from the supervisor.
pub fn refusal_status(code: tonic::Code) -> StatusCode {
    match code {
        tonic::Code::ResourceExhausted | tonic::Code::Unavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::FailedPrecondition => StatusCode::BAD_GATEWAY,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn plain(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(format!("{message}\n"))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    /// job, task, tenant, session, and the call.
    type Seen = (String, String, String, String, HttpCall);

    /// Records what it was called with and answers by the path.
    #[derive(Default)]
    struct FakeTarget {
        calls: Mutex<Vec<Seen>>,
    }

    #[tonic::async_trait]
    impl IngressTarget for FakeTarget {
        async fn handle(
            &self,
            job_id: &str,
            task_name: &str,
            tenant: &str,
            session_id: &str,
            request: HttpCall,
        ) -> Result<HttpOutcome, tonic::Status> {
            let path = request.path.clone();
            self.calls.lock().unwrap().push((
                job_id.into(),
                task_name.into(),
                tenant.into(),
                session_id.into(),
                request,
            ));
            match path.as_str() {
                "/full" => Err(tonic::Status::resource_exhausted(
                    "the task is at max_active",
                )),
                "/gone" => Err(tonic::Status::not_found("no session s-9")),
                "/broken" => Err(tonic::Status::failed_precondition(
                    "module failed to compile",
                )),
                "/down" => Err(tonic::Status::unavailable("host restarting")),
                "/slow" => Err(tonic::Status::deadline_exceeded("wall limit")),
                "/weird" => Ok(HttpOutcome {
                    status: 1000,
                    headers: vec![],
                    body: vec![],
                }),
                _ => Ok(HttpOutcome {
                    status: 201,
                    headers: vec![
                        Header {
                            name: "x-guest".into(),
                            value: b"1".to_vec(),
                        },
                        Header {
                            name: "connection".into(),
                            value: b"keep-alive".to_vec(),
                        },
                    ],
                    body: b"made".to_vec(),
                }),
            }
        }
    }

    async fn send(path: &Path, raw: &str) -> String {
        let mut stream = UnixStream::connect(path).await.unwrap();
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut out),
        )
        .await
        .expect("answered in time")
        .unwrap();
        String::from_utf8_lossy(&out).to_string()
    }

    #[tokio::test]
    async fn requests_become_http_calls_for_the_target_test() {
        let dir = tempfile::tempdir().unwrap();
        let target = Arc::new(FakeTarget::default());
        let servers = serve_ports(
            dir.path(),
            41,
            &["http".to_string()],
            "agents",
            "run",
            16,
            target.clone(),
        )
        .await
        .unwrap();
        let socket = dir.path().join("41/.isolate/ingress.sock_http");
        assert_eq!(servers[0].path(), socket);

        let response = send(
            &socket,
            "POST /make?x=1 HTTP/1.1\r\nHost: api.example\r\nLegion-Tenant: acme\r\nLegion-Session: s-1\r\nConnection: close, X-Hop\r\nX-Hop: 1\r\nX-Keep: 2\r\nContent-Length: 5\r\n\r\nhello",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 201"), "{response}");
        assert!(
            response.to_ascii_lowercase().contains("x-guest: 1"),
            "{response}"
        );
        assert!(response.ends_with("made"), "{response}");
        let (job, task, tenant, session, call) = target.calls.lock().unwrap()[0].clone();
        assert_eq!(
            (
                job.as_str(),
                task.as_str(),
                tenant.as_str(),
                session.as_str()
            ),
            ("agents", "run", "acme", "s-1")
        );
        assert_eq!(call.method, "POST");
        assert_eq!(call.path, "/make?x=1");
        assert_eq!(call.authority, "api.example");
        assert_eq!(call.scheme, "http");
        assert_eq!(call.body, b"hello");
        let names: Vec<&str> = call.headers.iter().map(|h| h.name.as_str()).collect();
        assert!(names.contains(&"x-keep"), "{names:?}");
        for gone in ["legion-tenant", "legion-session", "connection", "x-hop"] {
            assert!(
                !names.contains(&gone),
                "{gone} reached the guest: {names:?}"
            );
        }

        // Over the body cap: 413, and the target never sees it.
        let response = send(
            &socket,
            "POST / HTTP/1.1\r\nHost: h\r\nConnection: close\r\nContent-Length: 17\r\n\r\n12345678901234567",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 413"), "{response}");
        // A bad tenant key: 400 naming the header.
        let response = send(
            &socket,
            "GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\nLegion-Tenant: a b\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(response.contains("Legion-Tenant"), "{response}");
        // A routing header twice: 400, never first-wins.
        for twice in [
            "Legion-Tenant: a\r\nLegion-Tenant: b",
            "Legion-Session: s-1\r\nlegion-session: s-2",
        ] {
            let response = send(
                &socket,
                &format!("GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n{twice}\r\n\r\n"),
            )
            .await;
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(response.contains("at most once"), "{response}");
        }
        assert_eq!(target.calls.lock().unwrap().len(), 1);

        // Dropping the server removes its socket.
        drop(servers);
        assert!(!socket.exists());
    }

    #[tokio::test]
    async fn supervisor_refusals_map_to_http_statuses_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("ingress.sock_web");
        let _server = serve(
            socket.clone(),
            "agents",
            "run",
            1024,
            Arc::new(FakeTarget::default()),
        )
        .await
        .unwrap();
        for (path, status) in [
            ("/full", "503"),
            ("/gone", "404"),
            ("/broken", "502"),
            ("/down", "503"),
            ("/slow", "504"),
            ("/weird", "502"),
        ] {
            let response = send(
                &socket,
                &format!("GET {path} HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"),
            )
            .await;
            assert!(
                response.starts_with(&format!("HTTP/1.1 {status}")),
                "{path}: {response}"
            );
            if path == "/full" {
                assert!(
                    response.to_ascii_lowercase().contains("retry-after: 1"),
                    "{response}"
                );
            }
            if path == "/broken" {
                assert!(response.contains("module failed to compile"), "{response}");
            }
        }
    }

    /// The node's ingress listener for an isolate envelope reaches the
    /// supervisor's socket through `TaskDial::Isolate`, by port label.
    #[tokio::test]
    async fn the_ingress_dial_reaches_the_served_socket_test() {
        let dir = tempfile::tempdir().unwrap();
        let _server = serve(
            crate::worker::netd::attach::isolate_ingress_socket(dir.path(), "web"),
            "agents",
            "run",
            1024,
            Arc::new(FakeTarget::default()),
        )
        .await
        .unwrap();
        let target = crate::worker::netd::ingress::TaskTarget {
            dial: crate::worker::netd::ingress::TaskDial::Isolate {
                dir: dir.path().to_path_buf(),
            },
            label: "web".into(),
            port: 8080,
        };
        let mut stream = target.connect().await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert!(String::from_utf8_lossy(&out).starts_with("HTTP/1.1 201"));
        // A label nobody serves: a clear error, not a hang.
        let missing = crate::worker::netd::ingress::TaskTarget {
            label: "admin".into(),
            ..target
        };
        let err = missing.connect().await.err().unwrap();
        assert!(format!("{err:#}").contains("not prepared"), "{err:#}");
    }
}
