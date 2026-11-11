//! Isolate egress through the node datapath.
//!
//! An isolate has no sockets (its `wasi:sockets` deny everything). Its
//! outbound HTTP is one `IsolateWorker.Egress` call per request, which the
//! worker sends on the isolate's behalf through the listeners the datapath
//! bound for the task's envelope allocation (`netd::attach::Attachment::Isolate`),
//! exactly the listeners a gated workload's traffic goes through:
//!
//! - **A `connect` upstream** — `http://<key>/...` or `http://<key>.svc.legion/...`
//!   (any port; the scheme may be `https`, the upstream's own `to` decides how
//!   the last hop is encrypted) — goes to that upstream's listener socket, so
//!   its handlers and credential injection apply. The isolate never sees the
//!   injected credential: the listener adds it on the way out.
//! - **Anything else over `http://`** goes to the envelope's egress proxy as
//!   an absolute-URI proxy request, judged with its head.
//! - **Anything else over `https://`** is a `CONNECT host:port` to the egress
//!   proxy (judged by host and port), then TLS that the worker originates
//!   with the public roots the datapath uses everywhere else
//!   (`ForwarderContext::tls_connector`), then HTTP/1.1 over the tunnel.
//!
//! Bodies are buffered both ways and bounded by the worker's
//! `isolate.maxBodyBytes`. A refusal by the datapath (its `403` carrying
//! `Legion-Denied-By`) comes back as `EGRESS_DENIED` with the rule and the
//! reason, so a guest can tell a policy denial from a server's own `403`.
//! The datapath records the denial against the envelope allocation, as it
//! does for any gated workload; the isolate and tenant it was for are in
//! this module's debug log line (the listeners read no PROXY header that
//! could carry them).

use crate::prelude::*;
use crate::structs::isolate_host::{
    EgressError, EgressErrorKind, EgressReply, EgressRequest, Header,
};
use crate::worker::forwarder::UpstreamForwarder;
use crate::worker::isolates::{EgressBackend, IsolateTaskRef};
use crate::worker::netd::listen::{ListenAddr, Stream};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{header, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::time::Duration;

/// The timeout a request gets when the guest names none.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// The longest any one egress request may take, whatever the guest asks.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(300);
/// The suffix a `connect` upstream answers to by name, as in a container
/// (`<key>.svc.legion`; docs/network-handlers.md, "Name-is-config").
const SVC_SUFFIX: &str = ".svc.legion";
/// How long dialing one of the envelope's listener sockets may take.
const LISTENER_DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// `EgressBackend` over the node datapath: the envelope allocation's
/// upstream listeners and egress proxy.
pub struct NetdEgress {
    forwarder: Arc<UpstreamForwarder>,
    max_body_bytes: u64,
    tls: tokio_rustls::TlsConnector,
}

impl NetdEgress {
    /// Isolate egress through `forwarder` (`Netd::forwarder`), with bodies
    /// bounded by `max_body_bytes` (`isolate.maxBodyBytes`).
    pub fn new(forwarder: Arc<UpstreamForwarder>, max_body_bytes: u64) -> Self {
        let tls = forwarder.context().tls_connector();
        NetdEgress {
            forwarder,
            max_body_bytes,
            tls,
        }
    }

    /// Originate `https://` with this connector instead of the public roots
    /// (tests, with a local CA).
    pub fn with_tls_connector(mut self, tls: tokio_rustls::TlsConnector) -> Self {
        self.tls = tls;
        self
    }

    async fn route(&self, task: &IsolateTaskRef, request: EgressRequest) -> EgressReply {
        let envelope = task.envelope_allocation_id;
        let target = match Target::parse(&request.url) {
            Ok(target) => target,
            Err(message) => return failure(EgressErrorKind::EgressProtocol, message),
        };
        let method = match Method::from_bytes(request.method.trim().as_bytes()) {
            Ok(method) if !request.method.trim().is_empty() => method,
            _ => {
                return failure(
                    EgressErrorKind::EgressProtocol,
                    format!("{:?} is not an HTTP method", request.method),
                )
            }
        };
        if method == Method::CONNECT {
            return failure(
                EgressErrorKind::EgressProtocol,
                "CONNECT is not an outbound request an isolate can send",
            );
        }
        if request.body.len() as u64 > self.max_body_bytes {
            return failure(
                EgressErrorKind::EgressTooLarge,
                format!(
                    "the request body is {} bytes, over the node's isolate.maxBodyBytes ({})",
                    request.body.len(),
                    self.max_body_bytes
                ),
            );
        }
        let headers = match outbound_headers(&request.headers) {
            Ok(headers) => headers,
            Err(message) => return failure(EgressErrorKind::EgressProtocol, message),
        };
        let body = Bytes::from(request.body);

        // A `connect` upstream by name.
        if let Some(upstream) = target.upstream_key() {
            if let Some(listen) = self
                .forwarder
                .serving_upstreams(envelope)
                .into_iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(upstream))
                .map(|(_, listen)| listen)
            {
                debug!(
                    job_id = %task.job_id,
                    task = %task.task_name,
                    isolate_id = %task.isolate_id,
                    tenant = %task.tenant,
                    allocation_id = envelope,
                    upstream,
                    method = %method,
                    "Isolate egress through a connect upstream"
                );
                let stream = match dial_listener(&listen).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        return failure(
                            EgressErrorKind::EgressUnavailable,
                            format!(
                                "the listener of upstream {upstream} is not reachable: {err:#}"
                            ),
                        )
                    }
                };
                let uri = target.origin_form();
                return self
                    .exchange(stream, method, uri, &target.authority, headers, body)
                    .await;
            }
        }

        let Some(proxy) = self
            .forwarder
            .proxies()
            .and_then(|proxies| proxies.serving_addr(envelope))
        else {
            return failure(
                EgressErrorKind::EgressUnavailable,
                "this node has no egress datapath for the task (it declares no `network` or `connect` block, or the node's datapath is not serving it yet)",
            );
        };
        debug!(
            job_id = %task.job_id,
            task = %task.task_name,
            isolate_id = %task.isolate_id,
            tenant = %task.tenant,
            allocation_id = envelope,
            host = %target.host,
            port = target.port,
            https = target.https,
            method = %method,
            "Isolate egress through the egress proxy"
        );
        let stream = match dial_listener(&proxy).await {
            Ok(stream) => stream,
            Err(err) => {
                return failure(
                    EgressErrorKind::EgressUnavailable,
                    format!("the task's egress proxy is not reachable: {err:#}"),
                )
            }
        };
        if !target.https {
            let uri = target.absolute_form();
            return self
                .exchange(stream, method, uri, &target.authority, headers, body)
                .await;
        }
        let tunnel = match self.connect_tunnel(stream, &target).await {
            Ok(tunnel) => tunnel,
            Err(reply) => return reply,
        };
        let server_name = match rustls::pki_types::ServerName::try_from(target.host.clone()) {
            Ok(name) => name,
            Err(_) => {
                return failure(
                    EgressErrorKind::EgressProtocol,
                    format!("{} is not a name TLS can verify", target.host),
                )
            }
        };
        let tls = match self.tls.connect(server_name, TokioIo::new(tunnel)).await {
            Ok(tls) => tls,
            Err(err) => {
                return failure(
                    EgressErrorKind::EgressConnect,
                    format!("TLS with {}: {err}", target.authority),
                )
            }
        };
        self.exchange(
            tls,
            method,
            target.origin_form(),
            &target.authority,
            headers,
            body,
        )
        .await
    }

    /// `CONNECT host:port` through the egress proxy; the upgraded stream on
    /// success, the typed failure otherwise.
    async fn connect_tunnel(
        &self,
        stream: Stream,
        target: &Target,
    ) -> Result<hyper::upgrade::Upgraded, EgressReply> {
        let protocol = |err: hyper::Error| {
            failure(
                EgressErrorKind::EgressProtocol,
                format!("the egress proxy: {err}"),
            )
        };
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(protocol)?;
        tokio::spawn(async move {
            let _ = conn.with_upgrades().await;
        });
        let authority = format!("{}:{}", bracket(&target.host), target.port);
        let request = Request::builder()
            .method(Method::CONNECT)
            .uri(authority.as_str())
            .header(header::HOST, authority.as_str())
            .body(Full::new(Bytes::new()))
            .map_err(|err| failure(EgressErrorKind::EgressProtocol, err.to_string()))?;
        let mut response = sender.send_request(request).await.map_err(protocol)?;
        if response.status() == StatusCode::OK {
            return hyper::upgrade::on(&mut response).await.map_err(protocol);
        }
        if let Some(denied) = denial(&response) {
            return Err(denied);
        }
        if response.status() == StatusCode::BAD_GATEWAY {
            // The proxy allowed the destination but could not dial it: tell
            // a name that does not resolve from one that would not connect.
            let resolved = self
                .forwarder
                .context()
                .resolver()
                .resolve(&target.host, target.port)
                .await;
            return Err(match resolved {
                Ok(addrs) if !addrs.is_empty() => failure(
                    EgressErrorKind::EgressConnect,
                    format!("could not connect to {authority}"),
                ),
                Ok(_) => failure(
                    EgressErrorKind::EgressDns,
                    format!("{} resolved to no address", target.host),
                ),
                Err(err) => failure(
                    EgressErrorKind::EgressDns,
                    format!("{} does not resolve: {err}", target.host),
                ),
            });
        }
        Err(failure(
            EgressErrorKind::EgressProtocol,
            format!(
                "the egress proxy answered CONNECT {authority} with {}",
                response.status()
            ),
        ))
    }

    /// One HTTP/1.1 request on `io`, the response buffered up to the cap.
    async fn exchange<I>(
        &self,
        io: I,
        method: Method,
        uri: String,
        authority: &str,
        headers: Vec<(header::HeaderName, header::HeaderValue)>,
        body: Bytes,
    ) -> EgressReply
    where
        I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sender, conn) = match hyper::client::conn::http1::handshake(TokioIo::new(io)).await
        {
            Ok(pair) => pair,
            Err(err) => return failure(EgressErrorKind::EgressProtocol, err.to_string()),
        };
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut builder = Request::builder().method(method).uri(uri.as_str());
        let Some(request_headers) = builder.headers_mut() else {
            return failure(
                EgressErrorKind::EgressProtocol,
                format!("{uri} is not a request target"),
            );
        };
        for (name, value) in headers {
            request_headers.append(name, value);
        }
        match header::HeaderValue::from_str(authority) {
            Ok(host) => {
                request_headers.insert(header::HOST, host);
            }
            Err(_) => {
                return failure(
                    EgressErrorKind::EgressProtocol,
                    format!("{authority:?} is not a valid Host"),
                )
            }
        }
        if !body.is_empty() {
            request_headers.insert(
                header::CONTENT_LENGTH,
                header::HeaderValue::from(body.len()),
            );
        }
        let request = match builder.body(Full::new(body)) {
            Ok(request) => request,
            Err(err) => return failure(EgressErrorKind::EgressProtocol, err.to_string()),
        };
        let response = match sender.send_request(request).await {
            Ok(response) => response,
            Err(err) => {
                // A listener that closes without answering: a `tcp`
                // upstream whose policy refused the connection, or one with
                // no endpoint to dial.
                return failure(
                    EgressErrorKind::EgressConnect,
                    format!("the connection closed without a response: {err}"),
                );
            }
        };
        if let Some(denied) = denial(&response) {
            return denied;
        }
        let status = response.status().as_u16();
        let headers = inbound_headers(response.headers());
        let body = match collect_limited(response.into_body(), self.max_body_bytes).await {
            Ok(body) => body,
            Err(BodyError::TooLarge) => {
                return failure(
                    EgressErrorKind::EgressTooLarge,
                    format!(
                        "the response body is over the node's isolate.maxBodyBytes ({})",
                        self.max_body_bytes
                    ),
                )
            }
            Err(BodyError::Read(err)) => {
                return failure(
                    EgressErrorKind::EgressProtocol,
                    format!("reading the response body: {err}"),
                )
            }
        };
        EgressReply {
            status: u32::from(status),
            headers,
            body: body.to_vec(),
            error: None,
        }
    }
}

#[tonic::async_trait]
impl EgressBackend for NetdEgress {
    async fn fetch(&self, task: &IsolateTaskRef, request: EgressRequest) -> EgressReply {
        let timeout = request_timeout(request.timeout_millis);
        let url = request.url.clone();
        match tokio::time::timeout(timeout, self.route(task, request)).await {
            Ok(reply) => {
                if let Some(error) = reply.error.as_ref() {
                    debug!(
                        job_id = %task.job_id,
                        task = %task.task_name,
                        isolate_id = %task.isolate_id,
                        tenant = %task.tenant,
                        allocation_id = task.envelope_allocation_id,
                        kind = ?EgressErrorKind::try_from(error.kind).unwrap_or_default(),
                        message = %error.message,
                        "Isolate egress failed"
                    );
                }
                reply
            }
            Err(_) => failure(
                EgressErrorKind::EgressTimeout,
                format!(
                    "{} did not answer within {}ms",
                    redact_url(&url),
                    timeout.as_millis()
                ),
            ),
        }
    }
}

/// The guest's timeout, bounded: none means [`DEFAULT_TIMEOUT`], and
/// nothing goes past [`MAX_TIMEOUT`].
pub fn request_timeout(timeout_millis: u32) -> Duration {
    if timeout_millis == 0 {
        return DEFAULT_TIMEOUT;
    }
    Duration::from_millis(u64::from(timeout_millis)).min(MAX_TIMEOUT)
}

fn failure(kind: EgressErrorKind, message: impl Into<String>) -> EgressReply {
    EgressReply {
        status: 0,
        headers: Vec::new(),
        body: Vec::new(),
        error: Some(EgressError {
            kind: kind as i32,
            message: message.into(),
        }),
    }
}

/// A URL with any query string left out, for a log line or an error a
/// guest reads back (a query can carry a key).
fn redact_url(url: &str) -> String {
    match url.split_once('?') {
        Some((head, _)) => format!("{head}?…"),
        None => url.to_string(),
    }
}

/// A refusal the datapath answered itself: a `403` naming the rule in
/// `Legion-Denied-By` (`netd::proxy::denied_response`, and the upstream
/// listeners' HTTP mode).
fn denial<B>(response: &Response<B>) -> Option<EgressReply> {
    if response.status() != StatusCode::FORBIDDEN {
        return None;
    }
    let source = response
        .headers()
        .get("legion-denied-by")?
        .to_str()
        .ok()?
        .to_string();
    let reason = response
        .headers()
        .get("legion-denied-reason")
        .and_then(|value| value.to_str().ok())
        .filter(|reason| !reason.is_empty());
    Some(failure(
        EgressErrorKind::EgressDenied,
        match reason {
            Some(reason) => format!("denied by {source}: {reason}"),
            None => format!("denied by {source}"),
        },
    ))
}

/// Where an isolate's request goes, parsed from its absolute URL.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    https: bool,
    /// Lower-cased, without brackets.
    host: String,
    port: u16,
    /// `host[:port]` as the guest wrote it (lower-cased host): the `Host`.
    authority: String,
    /// Path and query, `/` when empty.
    path_and_query: String,
}

impl Target {
    fn parse(raw: &str) -> Result<Self, String> {
        let url = url::Url::parse(raw.trim())
            .map_err(|err| format!("{raw:?} is not an absolute URL: {err}"))?;
        let https = match url.scheme() {
            "http" => false,
            "https" => true,
            other => {
                return Err(format!(
                    "the {other}:// scheme is not supported; isolate egress is http:// or https://"
                ))
            }
        };
        if !url.username().is_empty() || url.password().is_some() {
            return Err(
                "a URL with credentials in it is not supported; send an Authorization header instead"
                    .into(),
            );
        }
        let host = match url.host() {
            Some(url::Host::Domain(domain)) => domain.to_ascii_lowercase(),
            Some(url::Host::Ipv4(ip)) => ip.to_string(),
            Some(url::Host::Ipv6(ip)) => ip.to_string(),
            None => return Err(format!("{raw:?} names no host")),
        };
        let port = url
            .port_or_known_default()
            .unwrap_or(if https { 443 } else { 80 });
        let authority = match url.port() {
            Some(port) => format!("{}:{port}", bracket(&host)),
            None => bracket(&host),
        };
        let mut path_and_query = url.path().to_string();
        if path_and_query.is_empty() {
            path_and_query.push('/');
        }
        if let Some(query) = url.query() {
            path_and_query.push('?');
            path_and_query.push_str(query);
        }
        Ok(Target {
            https,
            host,
            port,
            authority,
            path_and_query,
        })
    }

    /// The `connect` upstream key this host names, if it is one: the bare
    /// key, or `<key>.svc.legion`. Whether the task has that upstream is the
    /// caller's to check; a name that is not one goes to the egress proxy.
    fn upstream_key(&self) -> Option<&str> {
        let name = self.host.strip_suffix(SVC_SUFFIX).unwrap_or(&self.host);
        (!name.is_empty() && !name.contains(':')).then_some(name)
    }

    fn origin_form(&self) -> String {
        self.path_and_query.clone()
    }

    fn absolute_form(&self) -> String {
        format!("http://{}{}", self.authority, self.path_and_query)
    }
}

fn bracket(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

/// Headers that describe one hop of a connection, never forwarded
/// (RFC 9110 §7.6.1), plus the ones the worker sets itself.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// Request headers a caller never gets to forward: the routing headers of
/// isolate ingress (`Legion-Tenant`, `Legion-Session`), which name who the
/// isolate serves, not who it is calling.
const ISOLATE_ROUTING: &[&str] = &["legion-tenant", "legion-session"];

/// Whether a header is hop-by-hop: listed, or named by `Connection`.
pub(crate) fn is_hop_by_hop(name: &str, connection: &[String]) -> bool {
    let name = name.to_ascii_lowercase();
    HOP_BY_HOP.contains(&name.as_str()) || connection.contains(&name)
}

/// The header names a `Connection` header lists.
pub(crate) fn connection_tokens<'a>(values: impl Iterator<Item = &'a [u8]>) -> Vec<String> {
    values
        .flat_map(|value| {
            String::from_utf8_lossy(value)
                .split(',')
                .map(|token| token.trim().to_ascii_lowercase())
                .filter(|token| !token.is_empty())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The guest's headers as they go out: hop-by-hop, `Host`,
/// `Content-Length` (both set from the request itself) and the isolate
/// routing headers removed.
fn outbound_headers(
    headers: &[Header],
) -> Result<Vec<(header::HeaderName, header::HeaderValue)>, String> {
    let connection = connection_tokens(
        headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case("connection"))
            .map(|h| h.value.as_slice()),
    );
    let mut out = Vec::with_capacity(headers.len());
    for h in headers {
        let lower = h.name.to_ascii_lowercase();
        if is_hop_by_hop(&lower, &connection)
            || lower == "host"
            || lower == "content-length"
            || ISOLATE_ROUTING.contains(&lower.as_str())
        {
            continue;
        }
        let name = header::HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|_| format!("{:?} is not a valid header name", h.name))?;
        let value = header::HeaderValue::from_bytes(&h.value)
            .map_err(|_| format!("the value of header {} is not valid", h.name))?;
        out.push((name, value));
    }
    Ok(out)
}

/// A response's headers for the guest: hop-by-hop and `Content-Length`
/// removed (the body is whole).
pub(crate) fn inbound_headers(headers: &header::HeaderMap) -> Vec<Header> {
    let connection = connection_tokens(
        headers
            .get_all(header::CONNECTION)
            .iter()
            .map(|value| value.as_bytes()),
    );
    headers
        .iter()
        .filter(|(name, _)| {
            !is_hop_by_hop(name.as_str(), &connection) && *name != header::CONTENT_LENGTH
        })
        .map(|(name, value)| Header {
            name: name.as_str().to_string(),
            value: value.as_bytes().to_vec(),
        })
        .collect()
}

pub(crate) enum BodyError {
    TooLarge,
    Read(String),
}

/// Buffer a body, refusing one over `limit` bytes.
pub(crate) async fn collect_limited<B>(body: B, limit: u64) -> Result<Bytes, BodyError>
where
    B: hyper::body::Body,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    match Limited::new(body, limit).collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(err) => {
            if err
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                Err(BodyError::TooLarge)
            } else {
                Err(BodyError::Read(err.to_string()))
            }
        }
    }
}

/// Connect to one of the envelope's listeners.
async fn dial_listener(listen: &ListenAddr) -> R<Stream> {
    match listen {
        ListenAddr::Unix(path) => {
            let stream =
                tokio::time::timeout(LISTENER_DIAL_TIMEOUT, tokio::net::UnixStream::connect(path))
                    .await
                    .map_err(|_| anyhow!("connect {}: timed out", path.display()))?
                    .with_context(|| format!("connect {}", path.display()))?;
            Ok(Stream::Unix(stream))
        }
        ListenAddr::Tcp { netns, addr } => {
            let stream = crate::worker::netd::netns::connect_tcp(netns.clone(), *addr).await?;
            let _ = stream.set_nodelay(true);
            Ok(Stream::Tcp(stream))
        }
        ListenAddr::AnyAddress(port) => {
            let addr = std::net::SocketAddr::from(([127, 0, 0, 1], *port));
            let stream = crate::worker::netd::listen::dial_tcp(addr, LISTENER_DIAL_TIMEOUT)
                .await
                .with_context(|| format!("connect {addr}"))?;
            Ok(Stream::Tcp(stream))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::AllocNetPolicy;
    use crate::worker::forwarder::{ForwarderContext, SecretValue, UpstreamSpec};
    use crate::worker::netd::attach::Attachment;
    use crate::worker::netd::proxy::{EgressProxies, ProxySpec};
    use crate::worker::netd::{Denials, NetPolicyCache};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    const ENVELOPE: i64 = 70;

    fn task() -> IsolateTaskRef {
        IsolateTaskRef {
            job_id: "agents".into(),
            task_name: "run".into(),
            envelope_allocation_id: ENVELOPE,
            isolate_id: "inv-1".into(),
            tenant: "acme".into(),
            session: false,
        }
    }

    fn request(method: &str, url: &str, headers: &[(&str, &str)], body: &[u8]) -> EgressRequest {
        EgressRequest {
            origin: None,
            method: method.into(),
            url: url.into(),
            headers: headers
                .iter()
                .map(|(name, value)| Header {
                    name: name.to_string(),
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
            body: body.to_vec(),
            timeout_millis: 5_000,
        }
    }

    fn kind(reply: &EgressReply) -> Option<EgressErrorKind> {
        reply
            .error
            .as_ref()
            .map(|error| EgressErrorKind::try_from(error.kind).unwrap())
    }

    /// Read one HTTP/1.1 request (head and a `Content-Length` body) from a
    /// stream; the head as text, lower-cased.
    async fn read_request<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                let length = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                while buf.len() < end + 4 + length {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                return String::from_utf8_lossy(&buf).to_string();
            }
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    const ORIGIN_REPLY: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Origin: yes\r\nKeep-Alive: timeout=5\r\nConnection: close\r\n\r\nok";

    /// A plaintext origin that reports every request it reads (head and
    /// body, as received) on the channel and answers `ok`.
    async fn origin() -> (u16, mpsc::UnboundedReceiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let seen = read_request(&mut stream).await;
                    let _ = tx.send(seen);
                    let _ = stream.write_all(ORIGIN_REPLY).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (port, rx)
    }

    /// A TLS origin for `secure.test` with a fresh self-signed certificate,
    /// and a connector that trusts only that certificate.
    async fn tls_origin() -> (
        u16,
        mpsc::UnboundedReceiver<String>,
        tokio_rustls::TlsConnector,
    ) {
        let certified =
            rcgen::generate_simple_self_signed(vec!["secure.test".to_string()]).unwrap();
        let cert = certified.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()),
        );
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let connector =
            tokio_rustls::TlsConnector::from(crate::worker::netd::tls::pinned_client_config(roots));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = tx.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut stream) = acceptor.accept(stream).await else {
                        return;
                    };
                    let seen = read_request(&mut stream).await;
                    let _ = tx.send(seen);
                    let _ = stream.write_all(ORIGIN_REPLY).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (port, rx, connector)
    }

    /// The datapath for one isolate envelope (allocation [`ENVELOPE`]), as
    /// the heartbeat would bind it: its egress proxy on its socket dir, a
    /// policy allowing `allowed.test`, `secure.test` and `origin.test`, every
    /// `.test` name resolving to loopback, and the host guard opening only
    /// the given origin ports.
    async fn datapath(
        dir: &std::path::Path,
        open_ports: &[u16],
    ) -> (Arc<UpstreamForwarder>, Arc<Denials>) {
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require",
            "egress": { "protocol": "http", "allow": ["allowed.test", "secure.test", "origin.test"] }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: ENVELOPE,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let resolver = crate::worker::netd::guard::test_support::FakeResolver::new();
        for name in ["allowed.test", "denied.test", "secure.test", "origin.test"] {
            resolver.answer(name, vec![vec!["127.0.0.1".parse().unwrap()]]);
        }
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: open_ports
                    .iter()
                    .map(|port| format!("127.0.0.1:{port}"))
                    .collect(),
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, denials.clone())
                .with_resolver(resolver)
                .with_host_guard(Arc::new(guard)),
        );
        let proxies = Arc::new(EgressProxies::new(context.clone()));
        let forwarder = Arc::new(UpstreamForwarder::with_context(context).with_proxies(proxies));
        let attachment = Attachment::Isolate {
            dir: dir.to_path_buf(),
        };
        forwarder
            .proxies()
            .unwrap()
            .sync(&[ProxySpec {
                allocation_id: ENVELOPE,
                attachment,
                listen: None,
            }])
            .await;
        (forwarder, denials)
    }

    #[test]
    fn urls_parse_into_targets_test() {
        let target = Target::parse("http://API.example.com/v1/x?y=1#frag").unwrap();
        assert!(!target.https);
        assert_eq!(target.host, "api.example.com");
        assert_eq!(target.port, 80);
        assert_eq!(target.authority, "api.example.com");
        assert_eq!(target.path_and_query, "/v1/x?y=1");
        assert_eq!(target.absolute_form(), "http://api.example.com/v1/x?y=1");
        let target = Target::parse("https://h.test:8443").unwrap();
        assert_eq!((target.https, target.port), (true, 8443));
        assert_eq!(target.authority, "h.test:8443");
        assert_eq!(target.origin_form(), "/");
        let v6 = Target::parse("http://[::1]:81/").unwrap();
        assert_eq!(v6.authority, "[::1]:81");
        assert_eq!(v6.upstream_key(), None);
        assert!(Target::parse("ftp://h/").unwrap_err().contains("ftp://"));
        assert!(Target::parse("/relative").is_err());
        assert!(Target::parse("http://u:p@h/")
            .unwrap_err()
            .contains("Authorization"));
    }

    /// An upstream is addressed by its key, or `<key>.svc.legion` as in a
    /// container.
    #[test]
    fn an_upstream_is_named_by_key_or_svc_name_test() {
        assert_eq!(
            Target::parse("http://openai/v1").unwrap().upstream_key(),
            Some("openai")
        );
        assert_eq!(
            Target::parse("http://openai.svc.legion:8080/v1")
                .unwrap()
                .upstream_key(),
            Some("openai")
        );
        assert_eq!(
            Target::parse("http://api.openai.com/")
                .unwrap()
                .upstream_key(),
            Some("api.openai.com"),
            "a candidate name; the task's upstream list decides"
        );
    }

    #[test]
    fn timeouts_default_and_are_capped_test() {
        assert_eq!(request_timeout(0), DEFAULT_TIMEOUT);
        assert_eq!(request_timeout(1500), Duration::from_millis(1500));
        assert_eq!(request_timeout(u32::MAX), MAX_TIMEOUT);
    }

    #[test]
    fn hop_by_hop_and_routing_headers_never_go_out_test() {
        let headers: Vec<Header> = [
            ("Connection", "keep-alive, X-Secret-Hop"),
            ("X-Secret-Hop", "1"),
            ("Transfer-Encoding", "chunked"),
            ("Host", "spoofed"),
            ("Content-Length", "99"),
            ("Legion-Tenant", "acme"),
            ("legion-session", "s-1"),
            ("Proxy-Authorization", "Basic x"),
            ("Accept", "application/json"),
        ]
        .iter()
        .map(|(name, value)| Header {
            name: name.to_string(),
            value: value.as_bytes().to_vec(),
        })
        .collect();
        let out = outbound_headers(&headers).unwrap();
        let names: Vec<&str> = out.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["accept"]);
        let bad = vec![Header {
            name: "bad name".into(),
            value: b"x".to_vec(),
        }];
        assert!(outbound_headers(&bad).is_err());
    }

    /// No datapath bound for the envelope (netd off, or no network block):
    /// the guest gets a typed `EGRESS_UNAVAILABLE`, not a hang.
    #[tokio::test]
    async fn an_envelope_with_no_datapath_is_unavailable_test() {
        let forwarder = Arc::new(UpstreamForwarder::new());
        let egress = NetdEgress::new(forwarder, 1024);
        let reply = egress
            .fetch(&task(), request("GET", "http://example.com/", &[], b""))
            .await;
        assert_eq!(kind(&reply), Some(EgressErrorKind::EgressUnavailable));
        let reply = egress
            .fetch(&task(), request("GET", "gopher://x/", &[], b""))
            .await;
        assert_eq!(kind(&reply), Some(EgressErrorKind::EgressProtocol));
        let reply = egress
            .fetch(&task(), request("POST", "http://x/", &[], &[0u8; 2048]))
            .await;
        assert_eq!(kind(&reply), Some(EgressErrorKind::EgressTooLarge));
    }

    /// A request to a `connect` upstream goes through that upstream's
    /// listener on the envelope's socket, which injects the credential: the
    /// origin sees it, the guest's request did not carry it, and nothing in
    /// the reply contains it.
    #[tokio::test]
    async fn an_upstream_request_gets_the_injected_credential_test() {
        let (port, mut seen) = origin().await;
        let dir = tempfile::tempdir().unwrap();
        let (forwarder, _denials) = datapath(dir.path(), &[port]).await;
        let credentials: Vec<crate::structs::UpstreamCredential> = serde_json::from_value(json!([
            { "header": "authorization", "prefix": "Bearer ", "from": { "identity": "api" } },
        ]))
        .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: ENVELOPE,
                service: "api".into(),
                local_port: Some(20020),
                http_protocol: true,
                balance: "round_robin".into(),
                to: Some(format!("http://origin.test:{port}")),
                http: None,
                handlers: Vec::new(),
                credentials,
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: Attachment::Isolate {
                    dir: dir.path().to_path_buf(),
                },
            }])
            .await;
        assert!(
            dir.path().join("net.sock_20020").exists(),
            "the upstream binds on the envelope's socket dir"
        );
        forwarder.context().set_credential(
            ENVELOPE,
            "api",
            "authorization",
            "identity api",
            1_900_000_000,
            SecretValue::new("eyJ.secret-token"),
        );
        let egress = NetdEgress::new(forwarder.clone(), 1 << 20);
        let guest = request(
            "POST",
            "http://api/v1/chat?x=1",
            &[
                ("Content-Type", "application/json"),
                ("Legion-Tenant", "acme"),
            ],
            b"{\"q\":1}",
        );
        let guest_text = format!("{guest:?}");
        let reply = egress.fetch(&task(), guest).await;
        assert!(reply.error.is_none(), "{reply:?}");
        assert_eq!(reply.status, 200);
        assert_eq!(reply.body, b"ok");
        let names: Vec<String> = reply.headers.iter().map(|h| h.name.clone()).collect();
        assert!(names.contains(&"x-origin".to_string()), "{names:?}");
        assert!(
            !names.iter().any(|n| n == "connection" || n == "keep-alive"),
            "{names:?}"
        );
        let received = seen.recv().await.unwrap();
        assert!(
            received.starts_with("POST /v1/chat?x=1 HTTP/1.1"),
            "{received}"
        );
        assert!(
            received
                .to_ascii_lowercase()
                .contains("authorization: bearer eyj.secret-token"),
            "{received}"
        );
        assert!(received.ends_with("{\"q\":1}"), "{received}");
        assert!(
            !received.to_ascii_lowercase().contains("legion-tenant"),
            "{received}"
        );
        assert!(!guest_text.contains("secret-token"));
        assert!(!format!("{reply:?}").contains("secret-token"), "{reply:?}");

        // The `<key>.svc.legion` form reaches the same listener.
        let reply = egress
            .fetch(
                &task(),
                request("GET", "http://api.svc.legion/v2", &[], b""),
            )
            .await;
        assert_eq!(reply.status, 200, "{reply:?}");
        assert!(seen.recv().await.unwrap().starts_with("GET /v2 HTTP/1.1"));
    }

    /// Plain `http://` goes through the envelope's egress proxy: an allowed
    /// host works, a host the policy does not allow is `EGRESS_DENIED` with
    /// the rule, recorded against the envelope allocation.
    #[tokio::test]
    async fn http_through_the_proxy_is_judged_by_policy_test() {
        let (port, mut seen) = origin().await;
        let dir = tempfile::tempdir().unwrap();
        let (forwarder, denials) = datapath(dir.path(), &[port]).await;
        assert!(dir.path().join("net.sock_1080").exists());
        let egress = NetdEgress::new(forwarder, 1 << 20);

        let reply = egress
            .fetch(
                &task(),
                request("GET", &format!("http://allowed.test:{port}/fine"), &[], b""),
            )
            .await;
        assert!(reply.error.is_none(), "{reply:?}");
        assert_eq!((reply.status, reply.body.as_slice()), (200, &b"ok"[..]));
        let received = seen.recv().await.unwrap();
        assert!(received.starts_with("GET /fine HTTP/1.1"), "{received}");
        assert!(
            received
                .to_ascii_lowercase()
                .contains(&format!("host: allowed.test:{port}")),
            "{received}"
        );

        let reply = egress
            .fetch(
                &task(),
                request("GET", &format!("http://denied.test:{port}/x"), &[], b""),
            )
            .await;
        assert_eq!(
            kind(&reply),
            Some(EgressErrorKind::EgressDenied),
            "{reply:?}"
        );
        assert!(
            reply
                .error
                .as_ref()
                .unwrap()
                .message
                .starts_with("denied by "),
            "{reply:?}"
        );
        let recorded = denials.drain();
        assert!(
            recorded
                .iter()
                .any(|d| d.allocation_id == ENVELOPE && d.destination.contains("denied.test")),
            "{recorded:?}"
        );

        // A body over the cap on the way back.
        let small = NetdEgress::new(egress.forwarder.clone(), 1);
        let reply = small
            .fetch(
                &task(),
                request("GET", &format!("http://allowed.test:{port}/big"), &[], b""),
            )
            .await;
        assert_eq!(
            kind(&reply),
            Some(EgressErrorKind::EgressTooLarge),
            "{reply:?}"
        );
    }

    /// `https://` is a CONNECT to the proxy, judged by host and port, then
    /// TLS the worker originates; here the connector trusts a local CA.
    #[tokio::test]
    async fn https_is_tunnelled_and_tls_is_originated_by_the_worker_test() {
        let (port, mut seen, connector) = tls_origin().await;
        let dir = tempfile::tempdir().unwrap();
        let (forwarder, _denials) = datapath(dir.path(), &[port]).await;
        let egress = NetdEgress::new(forwarder.clone(), 1 << 20).with_tls_connector(connector);
        let reply = egress
            .fetch(
                &task(),
                request(
                    "PUT",
                    &format!("https://secure.test:{port}/s?k=v"),
                    &[("X-Req", "1")],
                    b"hello",
                ),
            )
            .await;
        assert!(reply.error.is_none(), "{reply:?}");
        assert_eq!(reply.status, 200);
        let received = seen.recv().await.unwrap();
        assert!(received.starts_with("PUT /s?k=v HTTP/1.1"), "{received}");
        assert!(received.ends_with("hello"), "{received}");

        // The public roots do not trust the local CA: a TLS failure, typed.
        let public = NetdEgress::new(forwarder.clone(), 1 << 20);
        let reply = public
            .fetch(
                &task(),
                request("GET", &format!("https://secure.test:{port}/"), &[], b""),
            )
            .await;
        assert_eq!(
            kind(&reply),
            Some(EgressErrorKind::EgressConnect),
            "{reply:?}"
        );

        // A CONNECT the policy refuses is a denial, before any TLS.
        let reply = egress
            .fetch(
                &task(),
                request("GET", &format!("https://denied.test:{port}/"), &[], b""),
            )
            .await;
        assert_eq!(
            kind(&reply),
            Some(EgressErrorKind::EgressDenied),
            "{reply:?}"
        );
    }
}
