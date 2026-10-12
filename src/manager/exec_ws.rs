//! Browser exec transport: a dedicated WebSocket listener.
//!
//! The manager's gRPC listener (tonic + hyper-util `auto`) does not enable
//! HTTP upgrades, and browsers cannot do client/bidirectional streaming over
//! gRPC-Web, so interactive exec from the UI needs a WebSocket. This module
//! runs a small hyper HTTP/1.1 listener on its own port that upgrades
//! `GET /v1/exec/ws` to a WebSocket and bridges it onto the same
//! [`run_exec_session`] core the gRPC `ExecAllocation` handler uses.
//!
//! Wire format: each binary WebSocket message is a protobuf-encoded
//! [`ExecClientFrame`] (client → manager) or [`ExecServerFrame`]
//! (manager → client) — the identical frames the native path uses. The first
//! client message must carry an `ExecStart`; its `bearer_token` field
//! authenticates the session (browsers cannot set WebSocket request headers).
//!
//! WebSockets are exempt from CORS, so the `Origin` header is checked here
//! against the same policy as gRPC-Web (`crate::manager::origin`): any page
//! could otherwise open a session, and with ACLs off it would be management.
//!
//! The listener follows `server.tls`: when the main gRPC listener serves TLS,
//! this one terminates TLS with the same certificate and key, so the UI's
//! `wss://` URL (derived from an `https://` manager endpoint) handshakes.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prost::Message as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;

use crate::manager::acl::{Authenticator, StreamCredential};
use crate::manager::exec::run_exec_session;
use crate::manager::origin::OriginPolicy;
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{exec_client_frame, ExecClientFrame, ExecServerFrame, TlsConfigFull};

/// Shared state for the exec WebSocket listener.
#[derive(Clone)]
pub struct ExecWsState {
    pub store: Store,
    pub auth: Arc<Authenticator>,
    /// Browser origins allowed to open a session (`server.allowedOrigins`).
    pub origins: Arc<OriginPolicy>,
}

/// The path the UI connects to.
pub const EXEC_WS_PATH: &str = "/v1/exec/ws";

/// TLS for the exec WebSocket listener, built from `server.tls` — the same
/// certificate and key the gRPC listener serves. `None` when TLS is off.
pub fn tls_acceptor(tls: &TlsConfigFull) -> R<Option<TlsAcceptor>> {
    if !tls.enabled {
        return Ok(None);
    }
    if tls.cert_path.is_empty() || tls.key_path.is_empty() {
        bail!("server.tls.enabled requires certPath and keyPath");
    }
    let cert_pem = std::fs::read(&tls.cert_path)
        .with_context(|| format!("failed to read TLS certificate {}", tls.cert_path))?;
    let key_pem = std::fs::read(&tls.key_path)
        .with_context(|| format!("failed to read TLS key {}", tls.key_path))?;
    let chain = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("invalid certificate PEM in {}", tls.cert_path))?;
    if chain.is_empty() {
        bail!("{} holds no certificate", tls.cert_path);
    }
    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .with_context(|| format!("invalid key PEM in {}", tls.key_path))?
        .ok_or_else(|| anyhow!("{} holds no private key", tls.key_path))?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .context("server.tls certificate and key do not match")?;
    // The listener only speaks HTTP/1.1 (WebSocket upgrades).
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

/// Run the exec WebSocket listener until the process exits. Bound on its own
/// port (`server.port + 1` by convention). With `tls`, every connection is a
/// TLS connection (`wss://`); without it, plaintext (`ws://`).
pub async fn serve_exec_ws(
    addr: SocketAddr,
    state: ExecWsState,
    tls: Option<TlsAcceptor>,
) -> R<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind exec ws listener on {addr}"))?;
    info!(%addr, tls = tls.is_some(), "Exec WebSocket listener started");
    serve_exec_ws_on(listener, state, tls).await
}

/// Accept loop over an already-bound listener.
async fn serve_exec_ws_on(
    listener: TcpListener,
    state: ExecWsState,
    tls: Option<TlsAcceptor>,
) -> R<()> {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = ?err, "exec ws accept failed");
                continue;
            }
        };
        let state = state.clone();
        let tls = tls.clone();
        // The TLS handshake runs on the connection's own task so a slow
        // client cannot stall the accept loop.
        tokio::spawn(async move {
            match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => serve_connection(stream, state).await,
                    Err(err) => debug!(%peer, error = ?err, "exec ws TLS handshake failed"),
                },
                None => serve_connection(stream, state).await,
            }
        });
    }
}

/// Serve HTTP/1.1 (with upgrades) on one accepted connection, plaintext or TLS.
async fn serve_connection<I>(io: I, state: ExecWsState)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |req| handle(req, state.clone()));
    if let Err(err) = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades()
        .await
    {
        debug!(error = ?err, "exec ws connection closed");
    }
}

/// CORS headers for an allowed request: the request's own origin is echoed
/// (never `*`), and nothing is added when it carried none.
fn cors_headers(
    mut resp: Response<Full<Bytes>>,
    origin: Option<&hyper::header::HeaderValue>,
) -> Response<Full<Bytes>> {
    let headers = resp.headers_mut();
    if let Some(origin) = origin {
        headers.insert("access-control-allow-origin", origin.clone());
        headers.insert("vary", "origin".parse().unwrap());
    }
    headers.insert(
        "access-control-allow-methods",
        "GET, OPTIONS".parse().unwrap(),
    );
    headers.insert("access-control-allow-headers", "*".parse().unwrap());
    resp
}

fn simple(
    status: StatusCode,
    body: &str,
    origin: Option<&hyper::header::HeaderValue>,
) -> Response<Full<Bytes>> {
    cors_headers(
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from(body.to_string())))
            .expect("static response is valid"),
        origin,
    )
}

async fn handle(
    mut req: Request<Incoming>,
    state: ExecWsState,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let origin = req.headers().get(hyper::header::ORIGIN).cloned();
    if !state.origins.allows_request(req.headers()) {
        warn!(
            origin = origin
                .as_ref()
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-"),
            "Refused an exec WebSocket from a disallowed origin \
             (add it to server.allowedOrigins to allow it)"
        );
        return Ok(simple(
            StatusCode::FORBIDDEN,
            "origin not allowed; add it to server.allowedOrigins",
            None,
        ));
    }
    let origin = origin.as_ref();
    if req.method() == hyper::Method::OPTIONS {
        return Ok(simple(StatusCode::NO_CONTENT, "", origin));
    }
    if req.uri().path() != EXEC_WS_PATH {
        return Ok(simple(StatusCode::NOT_FOUND, "not found", origin));
    }
    if !hyper_tungstenite::is_upgrade_request(&req) {
        return Ok(simple(
            StatusCode::BAD_REQUEST,
            "expected a websocket upgrade",
            origin,
        ));
    }

    match hyper_tungstenite::upgrade(&mut req, None) {
        Ok((response, websocket)) => {
            tokio::spawn(async move {
                if let Err(err) = bridge(websocket, state).await {
                    debug!(error = ?err, "exec ws session ended");
                }
            });
            // hyper-tungstenite hands back a 101 response with the accept key.
            Ok(cors_headers(response, origin))
        }
        Err(err) => Ok(simple(
            StatusCode::BAD_REQUEST,
            &format!("websocket upgrade failed: {err}"),
            origin,
        )),
    }
}

/// Bridge one upgraded WebSocket onto an exec session.
async fn bridge(websocket: hyper_tungstenite::HyperWebsocket, state: ExecWsState) -> R<()> {
    let ws = websocket.await.context("websocket handshake failed")?;
    let (mut ws_out, mut ws_in) = ws.split();

    // First message must be an ExecStart.
    let first = loop {
        match ws_in.next().await {
            Some(Ok(Message::Binary(data))) => break data,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | None => return Ok(()),
            Some(Ok(_)) => continue,
            Some(Err(err)) => return Err(anyhow!("websocket read error: {err}")),
        }
    };
    let start_frame =
        ExecClientFrame::decode(&first[..]).context("first frame was not an ExecClientFrame")?;
    let start = match start_frame.frame {
        Some(exec_client_frame::Frame::Start(start)) => start,
        _ => {
            send_ws_error(&mut ws_out, "first frame must be ExecStart").await;
            return Ok(());
        }
    };

    // Authenticate from the in-frame bearer token (browsers can't set headers).
    let mut metadata = tonic::metadata::MetadataMap::new();
    if !start.bearer_token.is_empty() {
        if let Ok(value) = format!("Bearer {}", start.bearer_token).parse() {
            metadata.insert("authorization", value);
        }
    }
    let identity = match state.auth.authenticate(&metadata, &state.store) {
        Ok(identity) => identity,
        Err(status) => {
            send_ws_error(&mut ws_out, status.message()).await;
            return Ok(());
        }
    };

    // Channel of subsequent client frames feeding the shared core.
    let (in_tx, in_rx) = mpsc::channel::<ExecClientFrame>(128);
    let credential = StreamCredential::new(state.auth.clone(), &metadata);
    let out_rx = match run_exec_session(&state.store, &identity, start, in_rx, credential).await {
        Ok(rx) => rx,
        Err(status) => {
            send_ws_error(&mut ws_out, status.message()).await;
            return Ok(());
        }
    };

    // WS -> core: forward client frames.
    let reader = tokio::spawn(async move {
        while let Some(msg) = ws_in.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if let Ok(frame) = ExecClientFrame::decode(&data[..]) {
                        if in_tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    // core -> WS: forward server frames until the session ends.
    let mut out_rx = out_rx;
    while let Some(item) = out_rx.recv().await {
        match item {
            Ok(frame) => {
                let is_exit = matches!(
                    frame.frame,
                    Some(crate::structs::exec_server_frame::Frame::Exit(_))
                );
                let bytes = frame.encode_to_vec();
                if ws_out.send(Message::Binary(bytes)).await.is_err() {
                    break;
                }
                if is_exit {
                    break;
                }
            }
            Err(status) => {
                send_ws_error(&mut ws_out, status.message()).await;
                break;
            }
        }
    }

    let _ = ws_out.send(Message::Close(None)).await;
    reader.abort();
    Ok(())
}

/// Send a synthetic exit frame carrying an error message, then close.
async fn send_ws_error<S>(ws_out: &mut S, message: &str)
where
    S: SinkExt<Message> + Unpin,
{
    let frame = ExecServerFrame {
        frame: Some(crate::structs::exec_server_frame::Frame::Exit(
            crate::structs::ExecExit {
                code: -1,
                message: message.to_string(),
            },
        )),
    };
    let _ = ws_out.send(Message::Binary(frame.encode_to_vec())).await;
    let _ = ws_out.send(Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::acl::Authenticator;
    use crate::structs::ExecStart;
    use tokio_tungstenite::client_async;

    const LISTENER_HOST: &str = "localhost";
    const MISSING_ALLOCATION: i64 = 424_242;

    fn fixture(path: &str) -> String {
        format!("{}/configs/tls/{path}", env!("CARGO_MANIFEST_DIR"))
    }

    fn dev_tls() -> TlsConfigFull {
        TlsConfigFull {
            enabled: true,
            cert_path: fixture("server/server.pem"),
            key_path: fixture("server/server.key"),
        }
    }

    /// Start the listener on an ephemeral loopback port. The returned temp
    /// dir holds the store and must outlive the test.
    async fn start(tls: Option<TlsAcceptor>) -> (u16, tempfile::TempDir) {
        let dir = tempfile::Builder::new()
            .prefix("legion-exec-ws-")
            .tempdir()
            .expect("tempdir");
        let store = Store::start_storage(&crate::structs::ManagerConfigFull {
            store_path: dir.path().to_str().expect("path").to_string(),
            ..Default::default()
        })
        .expect("store");
        let state = ExecWsState {
            store,
            auth: Arc::new(Authenticator::disabled()),
            origins: Arc::new(OriginPolicy::new(&[])),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(serve_exec_ws_on(listener, state, tls));
        (port, dir)
    }

    fn dev_client_tls() -> tokio_rustls::TlsConnector {
        let mut roots = rustls::RootCertStore::empty();
        let ca_pem = std::fs::read(fixture("rootca.pem")).expect("read root CA");
        for cert in rustls_pemfile::certs(&mut &ca_pem[..]) {
            roots.add(cert.expect("root CA PEM")).expect("add root CA");
        }
        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        // What a browser offers for a WebSocket.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    fn start_frame() -> Vec<u8> {
        ExecClientFrame {
            frame: Some(exec_client_frame::Frame::Start(ExecStart {
                allocation_id: MISSING_ALLOCATION,
                command: vec!["/bin/sh".to_string()],
                ..Default::default()
            })),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn exec_ws_serves_wss_when_server_tls_is_enabled() {
        let acceptor = tls_acceptor(&dev_tls())
            .expect("acceptor from dev certs")
            .expect("tls enabled");
        let (port, _dir) = start(Some(acceptor)).await;

        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let server_name =
            rustls::pki_types::ServerName::try_from(LISTENER_HOST).expect("server name");
        let tls_stream = dev_client_tls()
            .connect(server_name, tcp)
            .await
            .expect("TLS handshake against the exec ws listener");
        assert_eq!(
            tls_stream.get_ref().1.alpn_protocol(),
            Some(&b"http/1.1"[..])
        );

        let url = format!("wss://{LISTENER_HOST}:{port}{EXEC_WS_PATH}");
        let (mut ws, response) = client_async(url, tls_stream)
            .await
            .expect("websocket upgrade over TLS");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

        // The bridge runs behind the TLS stream: an ExecStart for a missing
        // allocation comes back as the synthetic error exit frame.
        ws.send(Message::Binary(start_frame()))
            .await
            .expect("send ExecStart");
        let reply = loop {
            match ws.next().await.expect("server frame").expect("ws read") {
                Message::Binary(data) => break data,
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected message {other:?}"),
            }
        };
        let frame = ExecServerFrame::decode(&reply[..]).expect("ExecServerFrame");
        match frame.frame {
            Some(crate::structs::exec_server_frame::Frame::Exit(exit)) => {
                assert_eq!(exit.code, -1);
                assert!(
                    exit.message.contains("not found"),
                    "unexpected exit message: {}",
                    exit.message
                );
            }
            other => panic!("expected an exit frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exec_ws_refuses_plaintext_when_server_tls_is_enabled() {
        let acceptor = tls_acceptor(&dev_tls())
            .expect("acceptor from dev certs")
            .expect("tls enabled");
        let (port, _dir) = start(Some(acceptor)).await;

        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let url = format!("ws://127.0.0.1:{port}{EXEC_WS_PATH}");
        assert!(client_async(url, tcp).await.is_err());
    }

    #[tokio::test]
    async fn exec_ws_stays_plaintext_when_server_tls_is_off() {
        let tls = TlsConfigFull {
            enabled: false,
            ..dev_tls()
        };
        let acceptor = tls_acceptor(&tls).expect("tls off");
        assert!(acceptor.is_none());
        let (port, _dir) = start(acceptor).await;

        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let url = format!("ws://127.0.0.1:{port}{EXEC_WS_PATH}");
        let (_ws, response) = client_async(url, tcp)
            .await
            .expect("plaintext websocket upgrade");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    }

    #[test]
    fn tls_acceptor_requires_cert_and_key_paths() {
        let err = tls_acceptor(&TlsConfigFull {
            enabled: true,
            cert_path: String::new(),
            key_path: String::new(),
        })
        .err()
        .expect("missing paths must fail");
        assert!(err.to_string().contains("certPath and keyPath"));
    }
}
