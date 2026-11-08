//! The request path (docs/ingress.md, "What a request goes through").
//!
//! Each accepted connection is served by hyper's auto server: HTTP/1.1, and
//! HTTP/2 by ALPN on TLS or by prior knowledge on plaintext. Each request is
//! routed on its own: the host and path pick a route from the table (an
//! `Arc` the request clones, never waiting on a writer), the route's backend
//! picks an endpoint, and the request goes out on a pooled HTTP/1.1
//! connection to it with hop-by-hop headers stripped and `X-Forwarded-*`
//! set. Bodies stream both ways. A dial failure ejects the endpoint and the
//! request, not yet sent, goes to another; so does a pooled connection the
//! backend closed before the request left. WebSocket (and any HTTP/1.1)
//! upgrades take a connection of their own and are copied both ways after
//! the `101`.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::{Request, Response, StatusCode, Uri, Version};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};

use super::backend::{Endpoint, InFlight, PooledBody, UpstreamBody};
use super::routes::{strip_prefix, Route};
use super::State;
use crate::prelude::*;

/// One dial to an endpoint.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(3);
/// How long a backend has to send its response headers.
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a client has to send a request head.
pub const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
/// Endpoints one request may try (a dial failure, or a pooled connection
/// the backend closed, moves it on).
pub const MAX_ATTEMPTS: usize = 3;
/// The header that names the ingress's own errors.
pub const ERROR_HEADER: &str = "legion-ingress-error";
/// Where ACME HTTP-01 challenges are answered.
pub const CHALLENGE_PREFIX: &str = crate::apps::certs::http01::CHALLENGE_PREFIX;

/// Request headers that describe one hop, never forwarded (RFC 9110 §7.6.1).
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
];

/// What a response body is: the backend's, streamed, or the ingress's own.
pub enum ProxyBody {
    Upstream(PooledBody),
    Local(Full<Bytes>),
}

impl hyper::body::Body for ProxyBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        match self.get_mut() {
            ProxyBody::Upstream(body) => std::pin::Pin::new(body).poll_frame(cx),
            ProxyBody::Local(body) => std::pin::Pin::new(body)
                .poll_frame(cx)
                .map_err(|never| match never {}),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            ProxyBody::Upstream(body) => body.is_end_stream(),
            ProxyBody::Local(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match self {
            ProxyBody::Upstream(body) => body.size_hint(),
            ProxyBody::Local(body) => body.size_hint(),
        }
    }
}

/// The ingress's own error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    NoRoute,
    NoEndpoint,
    BadGateway,
    Timeout,
    BadRequest,
}

impl Refusal {
    fn status(self) -> StatusCode {
        match self {
            Refusal::NoRoute => StatusCode::NOT_FOUND,
            Refusal::NoEndpoint => StatusCode::SERVICE_UNAVAILABLE,
            Refusal::BadGateway => StatusCode::BAD_GATEWAY,
            Refusal::Timeout => StatusCode::GATEWAY_TIMEOUT,
            Refusal::BadRequest => StatusCode::BAD_REQUEST,
        }
    }

    pub fn code(self) -> &'static str {
        match self {
            Refusal::NoRoute => "no_route",
            Refusal::NoEndpoint => "no_endpoint",
            Refusal::BadGateway => "bad_gateway",
            Refusal::Timeout => "upstream_timeout",
            Refusal::BadRequest => "bad_request",
        }
    }

    fn retryable(self) -> bool {
        matches!(
            self,
            Refusal::NoEndpoint | Refusal::BadGateway | Refusal::Timeout
        )
    }
}

/// An error answer in the standard envelope.
pub fn refusal(refusal: Refusal, message: String, hint: &str) -> Response<ProxyBody> {
    let body = json!({
        "ok": false,
        "error": {
            "code": refusal.code(),
            "message": message,
            "hint": hint,
            "retryable": refusal.retryable(),
        }
    });
    let mut response = Response::new(ProxyBody::Local(Full::new(Bytes::from(body.to_string()))));
    *response.status_mut() = refusal.status();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(ERROR_HEADER, HeaderValue::from_static(refusal.code()));
    if refusal.retryable() {
        headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

fn plain(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<ProxyBody> {
    let mut response = Response::new(ProxyBody::Local(Full::new(body.into())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

/// The host a request names, lower-case without a port: the URI's
/// authority (HTTP/2, absolute-form HTTP/1.1), else `Host`.
pub fn request_host<B>(request: &Request<B>) -> Option<String> {
    let raw = match request.uri().host() {
        Some(host) => host.to_string(),
        None => {
            let value = request.headers().get(header::HOST)?.to_str().ok()?;
            let value = value.trim();
            // `[v6]:port`, `name:port`, or bare.
            if let Some(rest) = value.strip_prefix('[') {
                format!("[{}]", rest.split(']').next()?)
            } else {
                value.split(':').next()?.to_string()
            }
        }
    };
    let host = raw.trim_end_matches('.').to_ascii_lowercase();
    (!host.is_empty()).then_some(host)
}

/// Whether a request asks to switch protocols (an HTTP/1.1 `Upgrade`).
fn wants_upgrade<B>(request: &Request<B>) -> bool {
    request.version() == Version::HTTP_11
        && request.headers().contains_key(header::UPGRADE)
        && connection_tokens(request.headers()).any(|token| token.eq_ignore_ascii_case("upgrade"))
}

fn connection_tokens(headers: &HeaderMap) -> impl Iterator<Item = &str> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

/// Drop hop-by-hop headers, including any `Connection` names. With
/// `keep_upgrade`, `Upgrade` and `Connection: upgrade` stay.
pub fn strip_hop_by_hop(headers: &mut HeaderMap, keep_upgrade: bool) {
    let named: Vec<HeaderName> = connection_tokens(headers)
        .filter(|token| !(keep_upgrade && token.eq_ignore_ascii_case("upgrade")))
        .filter_map(|token| HeaderName::from_bytes(token.as_bytes()).ok())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
    if keep_upgrade {
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    } else {
        headers.remove(header::UPGRADE);
    }
}

/// Set `X-Forwarded-For` (appending the peer), `-Proto`, and `-Host`.
pub fn set_forwarded(headers: &mut HeaderMap, peer: SocketAddr, tls: bool, host: &str) {
    let peer_ip = peer.ip().to_canonical().to_string();
    let forwarded_for = match headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
    {
        Some(existing) if !existing.trim().is_empty() => format!("{}, {peer_ip}", existing.trim()),
        _ => peer_ip,
    };
    if let Ok(value) = HeaderValue::from_str(&forwarded_for) {
        headers.insert("x-forwarded-for", value);
    }
    headers.insert(
        "x-forwarded-proto",
        HeaderValue::from_static(if tls { "https" } else { "http" }),
    );
    if let Ok(value) = HeaderValue::from_str(host) {
        headers.insert("x-forwarded-host", value);
    }
}

/// The origin-form URI the backend gets: the path (without the route's
/// prefix when it strips) and the query.
fn upstream_uri(route: &Route, uri: &Uri) -> Result<Uri, http::Error> {
    let path = uri.path();
    let path = if route.strip_path {
        strip_prefix(&route.path, path)
    } else {
        std::borrow::Cow::Borrowed(path)
    };
    let path = if path.is_empty() { "/" } else { path.as_ref() };
    let path_and_query = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    Uri::builder().path_and_query(path_and_query).build()
}

/// Serve one request.
pub async fn handle(
    state: Arc<State>,
    mut request: Request<Incoming>,
    peer: SocketAddr,
    tls: bool,
) -> Response<ProxyBody> {
    let path = request.uri().path().to_string();
    if !tls && path.starts_with(CHALLENGE_PREFIX) {
        return answer_challenge(&state, request.method().as_str(), &path);
    }
    let Some(host) = request_host(&request) else {
        return refusal(
            Refusal::BadRequest,
            "the request names no host".into(),
            "send a Host header",
        );
    };
    let table = state.table();
    let Some(route) = table.find(&host, &path).cloned() else {
        return refusal(
            Refusal::NoRoute,
            format!("no route for {host}{path}"),
            "add `ingress = { host = \"...\" }` to the service that should answer, and check `legion app view ingress nav`",
        );
    };
    drop(table);
    if !tls && state.redirect_https() && state.certs.serves(&host) {
        let target = match request.uri().query() {
            Some(query) => format!("https://{host}{path}?{query}"),
            None => format!("https://{host}{path}"),
        };
        let mut response = plain(StatusCode::PERMANENT_REDIRECT, "text/plain", "");
        if let Ok(location) = HeaderValue::from_str(&target) {
            response.headers_mut().insert(header::LOCATION, location);
        }
        return response;
    }

    let upgrade = wants_upgrade(&request);
    let client_upgrade = upgrade.then(|| hyper::upgrade::on(&mut request));
    let (mut parts, body) = request.into_parts();
    // HTTP/2 carries the host in `:authority`; the service gets it as
    // `Host`, port and all, as an HTTP/1.1 client would have sent it.
    let authority = parts.uri.authority().map(|authority| authority.to_string());
    parts.uri = match upstream_uri(&route, &parts.uri) {
        Ok(uri) => uri,
        Err(_) => {
            return refusal(
                Refusal::BadRequest,
                "the request path cannot be forwarded".into(),
                "check the URL",
            )
        }
    };
    if parts.version == Version::HTTP_2 && !parts.headers.contains_key(header::HOST) {
        if let Ok(value) = HeaderValue::from_str(authority.as_deref().unwrap_or(&host)) {
            parts.headers.insert(header::HOST, value);
        }
    }
    parts.version = Version::HTTP_11;
    strip_hop_by_hop(&mut parts.headers, upgrade);
    set_forwarded(&mut parts.headers, peer, tls, &host);
    let mut outgoing = Request::from_parts(parts, body);

    let mut tried: Vec<Arc<Endpoint>> = Vec::new();
    for _ in 0..MAX_ATTEMPTS {
        let Some(endpoint) = route.backend.pick(&tried) else {
            break;
        };
        let guard = endpoint.begin();
        match send(&endpoint, outgoing, upgrade).await {
            Sent::Answered(response, sender) => {
                return respond(response, sender, guard, client_upgrade);
            }
            Sent::Unsent(request, why) => {
                debug!(endpoint = %endpoint.authority(), "Trying another endpoint: {why}");
                endpoint.eject();
                tried.push(endpoint);
                outgoing = request;
            }
            Sent::Failed(Refusal::Timeout) => {
                return refusal(
                    Refusal::Timeout,
                    format!(
                        "{} did not answer within {}s",
                        route.backend.target.display(),
                        RESPONSE_TIMEOUT.as_secs()
                    ),
                    "check the service's logs",
                )
            }
            Sent::Failed(_) => {
                return refusal(
                    Refusal::BadGateway,
                    format!("{} broke the connection", route.backend.target.display()),
                    "check the service's logs",
                )
            }
        }
    }
    if tried.is_empty() {
        refusal(
            Refusal::NoEndpoint,
            format!("{} has no healthy endpoint", route.backend.target.display()),
            "check `legion service resolve` and the job's health checks",
        )
    } else {
        refusal(
            Refusal::BadGateway,
            format!(
                "no endpoint of {} accepted a connection ({} tried)",
                route.backend.target.display(),
                tried.len()
            ),
            "check that the service listens on its advertised port and that the ingress's nodes can reach it",
        )
    }
}

/// What sending a request did.
enum Sent {
    Answered(
        Response<Incoming>,
        Option<hyper::client::conn::http1::SendRequest<UpstreamBody>>,
    ),
    /// The request never left: try another endpoint with it.
    Unsent(Request<UpstreamBody>, String),
    Failed(Refusal),
}

/// Send on a pooled connection, else a new one. An upgrade always takes a
/// new connection, which never goes back to the pool.
async fn send(endpoint: &Arc<Endpoint>, mut request: Request<UpstreamBody>, upgrade: bool) -> Sent {
    if !upgrade {
        while let Some(mut sender) = endpoint.checkout() {
            if sender.ready().await.is_err() {
                continue;
            }
            match exchange(&mut sender, request).await {
                Exchange::Done(response) => return Sent::Answered(response, Some(sender)),
                Exchange::Returned(back) => request = back,
                Exchange::Failed(refusal) => return Sent::Failed(refusal),
            }
        }
    }
    let stream = match tokio::time::timeout(
        DIAL_TIMEOUT,
        tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port)),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => return Sent::Unsent(request, format!("dial: {err}")),
        Err(_) => return Sent::Unsent(request, "dial timed out".into()),
    };
    let _ = stream.set_nodelay(true);
    let (mut sender, connection) = match hyper::client::conn::http1::Builder::new()
        .handshake::<_, UpstreamBody>(TokioIo::new(stream))
        .await
    {
        Ok(pair) => pair,
        Err(err) => return Sent::Unsent(request, format!("handshake: {err}")),
    };
    tokio::spawn(async move {
        if let Err(err) = connection.with_upgrades().await {
            debug!("upstream connection closed: {err}");
        }
    });
    match exchange(&mut sender, request).await {
        Exchange::Done(response) => Sent::Answered(response, (!upgrade).then_some(sender)),
        Exchange::Returned(back) => Sent::Unsent(back, "the new connection closed".into()),
        Exchange::Failed(refusal) => Sent::Failed(refusal),
    }
}

enum Exchange {
    Done(Response<Incoming>),
    /// Not sent: the connection was closed before the request went out.
    Returned(Request<UpstreamBody>),
    Failed(Refusal),
}

async fn exchange(
    sender: &mut hyper::client::conn::http1::SendRequest<UpstreamBody>,
    request: Request<UpstreamBody>,
) -> Exchange {
    match tokio::time::timeout(RESPONSE_TIMEOUT, sender.try_send_request(request)).await {
        Ok(Ok(response)) => Exchange::Done(response),
        Ok(Err(mut err)) => match err.take_message() {
            Some(request) => Exchange::Returned(request),
            None => {
                debug!("upstream request failed: {}", err.into_error());
                Exchange::Failed(Refusal::BadGateway)
            }
        },
        Err(_) => Exchange::Failed(Refusal::Timeout),
    }
}

/// Turn the backend's response into the client's: hop-by-hop headers
/// stripped, the body streamed (and its connection pooled when it ends),
/// or, for a `101`, the two upgraded connections joined.
fn respond(
    mut response: Response<Incoming>,
    sender: Option<hyper::client::conn::http1::SendRequest<UpstreamBody>>,
    guard: InFlight,
    client_upgrade: Option<hyper::upgrade::OnUpgrade>,
) -> Response<ProxyBody> {
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        let Some(client_upgrade) = client_upgrade else {
            // A 101 nobody asked for cannot be relayed.
            return refusal(
                Refusal::BadGateway,
                "the service switched protocols unasked".into(),
                "check the service",
            );
        };
        let upstream_upgrade = hyper::upgrade::on(&mut response);
        let (mut parts, _) = response.into_parts();
        tokio::spawn(async move {
            let _guard = guard;
            match tokio::join!(client_upgrade, upstream_upgrade) {
                (Ok(client), Ok(upstream)) => {
                    let mut client = TokioIo::new(client);
                    let mut upstream = TokioIo::new(upstream);
                    if let Err(err) =
                        tokio::io::copy_bidirectional(&mut client, &mut upstream).await
                    {
                        debug!("upgraded connection closed: {err}");
                    }
                }
                (client, upstream) => debug!(
                    client_ok = client.is_ok(),
                    upstream_ok = upstream.is_ok(),
                    "an upgrade did not complete"
                ),
            }
        });
        strip_hop_by_hop(&mut parts.headers, true);
        return Response::from_parts(parts, ProxyBody::Local(Full::new(Bytes::new())));
    }
    let (mut parts, body) = response.into_parts();
    let reusable = sender.filter(|_| {
        !connection_tokens(&parts.headers).any(|token| token.eq_ignore_ascii_case("close"))
    });
    strip_hop_by_hop(&mut parts.headers, false);
    Response::from_parts(
        parts,
        ProxyBody::Upstream(PooledBody::new(body, reusable, guard)),
    )
}

/// Answer an ACME HTTP-01 challenge for the Issuer the ingress uses:
/// `<token>.<thumbprint>`, from the account's public thumbprint, as the
/// cert manager's own responder does. Every replica answers every token
/// with no shared state; until the thumbprint is known, `503`.
fn answer_challenge(state: &State, method: &str, path: &str) -> Response<ProxyBody> {
    let thumbprint = state.thumbprint();
    let answer =
        crate::apps::certs::http01::answer(method, path, None, thumbprint.as_deref(), false);
    let status = StatusCode::from_u16(answer.status).unwrap_or(StatusCode::NOT_FOUND);
    plain(status, "text/plain", answer.body)
}

/// How each listener serves connections.
#[derive(Clone, Copy, Debug)]
pub enum Listener {
    Http,
    Https,
}

/// Accept on `listener` until `stop` fires, then let open connections
/// finish their requests (HTTP/1.1 keep-alive ends, HTTP/2 sends GOAWAY)
/// for up to `grace`.
pub async fn serve(
    state: Arc<State>,
    listener: tokio::net::TcpListener,
    kind: Listener,
    tls: Option<tokio_rustls::TlsAcceptor>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    grace: Duration,
) {
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT)
        .keep_alive(true);
    builder.http2().timer(TokioTimer::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = stop.changed() => break,
        };
        let (stream, peer) = match accepted {
            Ok(pair) => pair,
            Err(err) => {
                debug!("accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let state = state.clone();
        let tls_enabled = matches!(kind, Listener::Https);
        let service = hyper::service::service_fn(move |request| {
            let state = state.clone();
            async move { Ok::<_, Infallible>(handle(state, request, peer, tls_enabled).await) }
        });
        match &tls {
            Some(acceptor) => {
                let acceptor = acceptor.clone();
                let builder = builder.clone();
                let watcher = graceful.watcher();
                tokio::spawn(async move {
                    let stream =
                        match tokio::time::timeout(HEADER_TIMEOUT, acceptor.accept(stream)).await {
                            Ok(Ok(stream)) => stream,
                            Ok(Err(err)) => {
                                debug!(%peer, "TLS handshake failed: {err}");
                                return;
                            }
                            Err(_) => return,
                        };
                    let connection = builder
                        .serve_connection_with_upgrades(TokioIo::new(stream), service)
                        .into_owned();
                    if let Err(err) = watcher.watch(connection).await {
                        debug!(%peer, "connection ended: {err}");
                    }
                });
            }
            None => {
                let connection = builder
                    .serve_connection_with_upgrades(TokioIo::new(stream), service)
                    .into_owned();
                let watched = graceful.watch(connection);
                tokio::spawn(async move {
                    if let Err(err) = watched.await {
                        debug!(%peer, "connection ended: {err}");
                    }
                });
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(grace, graceful.shutdown())
        .await
        .is_err()
    {
        debug!(
            ?kind,
            "Connections still open at the end of the grace period"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_host_comes_from_the_authority_or_the_host_header() {
        let request = Request::builder()
            .uri("/x")
            .header(header::HOST, "Shop.Example.com:8080")
            .body(())
            .unwrap();
        assert_eq!(request_host(&request).as_deref(), Some("shop.example.com"));
        let absolute = Request::builder()
            .uri("http://api.example.com/x")
            .header(header::HOST, "ignored.example.com")
            .body(())
            .unwrap();
        assert_eq!(request_host(&absolute).as_deref(), Some("api.example.com"));
        let v6 = Request::builder()
            .uri("/")
            .header(header::HOST, "[fd00::1]:80")
            .body(())
            .unwrap();
        assert_eq!(request_host(&v6).as_deref(), Some("[fd00::1]"));
        let none = Request::builder().uri("/").body(()).unwrap();
        assert_eq!(request_host(&none), None);
    }

    #[test]
    fn hop_by_hop_headers_go_and_upgrades_keep_theirs() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, X-Secret, Upgrade"),
        );
        headers.insert("x-secret", HeaderValue::from_static("1"));
        headers.insert(header::TE, HeaderValue::from_static("trailers"));
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert("x-app", HeaderValue::from_static("kept"));
        let mut plain = headers.clone();
        strip_hop_by_hop(&mut plain, false);
        assert!(plain.get("x-secret").is_none());
        assert!(plain.get(header::TE).is_none());
        assert!(plain.get(header::UPGRADE).is_none());
        assert!(plain.get(header::CONNECTION).is_none());
        assert_eq!(plain.get("x-app").unwrap(), "kept");

        strip_hop_by_hop(&mut headers, true);
        assert_eq!(headers.get(header::UPGRADE).unwrap(), "websocket");
        assert_eq!(headers.get(header::CONNECTION).unwrap(), "upgrade");
        assert!(headers.get("x-secret").is_none());
    }

    #[test]
    fn forwarded_headers_append_the_peer() {
        let mut headers = HeaderMap::new();
        let peer: SocketAddr = "203.0.113.9:5555".parse().unwrap();
        set_forwarded(&mut headers, peer, true, "shop.example.com");
        assert_eq!(headers.get("x-forwarded-for").unwrap(), "203.0.113.9");
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "https");
        assert_eq!(headers.get("x-forwarded-host").unwrap(), "shop.example.com");
        set_forwarded(
            &mut headers,
            "[::ffff:198.51.100.1]:1".parse().unwrap(),
            false,
            "h",
        );
        assert_eq!(
            headers.get("x-forwarded-for").unwrap(),
            "203.0.113.9, 198.51.100.1",
            "a v4-mapped peer reads as v4"
        );
        assert_eq!(headers.get("x-forwarded-proto").unwrap(), "http");
    }

    #[test]
    fn refusals_use_the_standard_envelope() {
        let response = refusal(Refusal::NoEndpoint, "no".into(), "hint");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(ERROR_HEADER).unwrap(), "no_endpoint");
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
        assert_eq!(
            refusal(Refusal::NoRoute, "x".into(), "y").status(),
            StatusCode::NOT_FOUND
        );
    }
}
