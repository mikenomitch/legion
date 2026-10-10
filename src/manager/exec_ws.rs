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
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::manager::acl::{Authenticator, StreamCredential};
use crate::manager::exec::run_exec_session;
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{exec_client_frame, ExecClientFrame, ExecServerFrame};

/// Shared state for the exec WebSocket listener.
#[derive(Clone)]
pub struct ExecWsState {
    pub store: Store,
    pub auth: Arc<Authenticator>,
}

/// The path the UI connects to.
pub const EXEC_WS_PATH: &str = "/v1/exec/ws";

/// Run the exec WebSocket listener until the process exits. Bound on its own
/// port (`server.port + 1` by convention).
pub async fn serve_exec_ws(addr: SocketAddr, state: ExecWsState) -> R<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind exec ws listener on {addr}"))?;
    info!(%addr, "Exec WebSocket listener started");

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = ?err, "exec ws accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| handle(req, state.clone()));
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades()
                .await
            {
                debug!(error = ?err, "exec ws connection closed");
            }
        });
    }
}

fn cors_headers(mut resp: Response<Full<Bytes>>) -> Response<Full<Bytes>> {
    let headers = resp.headers_mut();
    headers.insert("access-control-allow-origin", "*".parse().unwrap());
    headers.insert(
        "access-control-allow-methods",
        "GET, OPTIONS".parse().unwrap(),
    );
    headers.insert("access-control-allow-headers", "*".parse().unwrap());
    resp
}

fn simple(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    cors_headers(
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from(body.to_string())))
            .expect("static response is valid"),
    )
}

async fn handle(
    mut req: Request<Incoming>,
    state: ExecWsState,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() == hyper::Method::OPTIONS {
        return Ok(simple(StatusCode::NO_CONTENT, ""));
    }
    if req.uri().path() != EXEC_WS_PATH {
        return Ok(simple(StatusCode::NOT_FOUND, "not found"));
    }
    if !hyper_tungstenite::is_upgrade_request(&req) {
        return Ok(simple(
            StatusCode::BAD_REQUEST,
            "expected a websocket upgrade",
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
            Ok(cors_headers(response))
        }
        Err(err) => Ok(simple(
            StatusCode::BAD_REQUEST,
            &format!("websocket upgrade failed: {err}"),
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
