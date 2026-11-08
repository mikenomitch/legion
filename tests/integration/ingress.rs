//! The bundled ingress (docs/ingress.md; ingress plan §9): the real proxy,
//! certificate store, and issuer, driven through a fake [`Control`] so no
//! cluster is needed, against real HTTP backends on loopback.
//!
//! - routing by host and by path prefix, `stripPath`, `X-Forwarded-*`, and
//!   the standard error envelope for an unknown host or a route with no
//!   endpoint;
//! - HTTP/2 from the client (prior knowledge) to an HTTP/1.1 backend;
//! - balancing across endpoints, and retrying past one that refuses;
//! - a WebSocket-style upgrade relayed both ways;
//! - HTTPS from an operator bundle in KV, chosen by SNI, and the redirect
//!   from HTTP once a certificate is served;
//! - HTTP-01 answers read from the shared board;
//! - automatic certificates against an in-process ACME CA that verifies
//!   every signature, nonce, and URL, refuses one nonce, and validates
//!   HTTP-01 by fetching the answer from the ingress: registered, ordered,
//!   stored in KV, served by SNI, and loaded by a second replica without a
//!   second order;
//! - against Pebble (Let's Encrypt's test CA) when `LEGION_TEST_PEBBLE_DIR`
//!   names a directory holding `pebble` and `pebble-challtestsrv`: a
//!   certificate ordered over HTTP-01 and served.
//!
//! The manager side (the key registered by seeding, checked at submit, and
//! delivered to the app's identity) is in `app_extensions.rs`.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use http_body_util::{BodyExt, Full};
use legion::apps::ingress::backend::CatalogEndpoint;
use legion::apps::ingress::certs::{KvVersion, OPERATOR_PREFIX};
use legion::apps::ingress::control::Control;
use legion::apps::ingress::{Ingress, IngressConfig, Listen, State};
use legion::structs::{AppExtensionEvent, AppExtensionValue, ExtensionPath};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type R<T> = anyhow::Result<T>;

/// Routes, endpoints, KV, and the issuer lock, in memory.
#[derive(Default)]
struct FakeControl {
    events: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<R<AppExtensionEvent>>>>,
    /// Service ref -> (job\0task, endpoint).
    endpoints: Mutex<HashMap<String, Vec<(String, CatalogEndpoint)>>>,
    kv: Mutex<BTreeMap<String, (String, i64)>>,
    version: std::sync::atomic::AtomicI64,
}

impl FakeControl {
    fn new() -> (
        Arc<FakeControl>,
        tokio::sync::mpsc::UnboundedSender<R<AppExtensionEvent>>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let control = FakeControl {
            events: Mutex::new(Some(rx)),
            ..Default::default()
        };
        (Arc::new(control), tx)
    }

    fn set_endpoints(&self, service: &str, job: &str, task: &str, addrs: &[SocketAddr]) {
        let entries = addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                (
                    format!("{job}\u{0}{task}"),
                    CatalogEndpoint {
                        host: addr.ip().to_string(),
                        port: addr.port(),
                        allocation_id: i as i64 + 1,
                        node_id: "node-1".into(),
                        healthy: true,
                        mesh_tls: false,
                    },
                )
            })
            .collect();
        self.endpoints
            .lock()
            .unwrap()
            .insert(service.to_string(), entries);
    }

    fn put(&self, key: &str, value: &str) {
        let version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.kv
            .lock()
            .unwrap()
            .insert(key.to_string(), (value.to_string(), version));
    }
}

#[async_trait::async_trait]
impl Control for FakeControl {
    async fn watch_routes(&self) -> R<BoxStream<'static, R<AppExtensionEvent>>> {
        match self.events.lock().unwrap().take() {
            Some(rx) => Ok(tokio_stream::wrappers::UnboundedReceiverStream::new(rx).boxed()),
            None => Ok(futures::stream::pending().boxed()),
        }
    }

    async fn resolve(&self, service: &str) -> R<Vec<(String, CatalogEndpoint)>> {
        Ok(self
            .endpoints
            .lock()
            .unwrap()
            .get(service)
            .cloned()
            .unwrap_or_default())
    }

    async fn kv_list(&self, prefix: &str) -> R<Vec<KvVersion>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, (_, version))| KvVersion {
                key: key.clone(),
                version: *version,
            })
            .collect())
    }

    async fn kv_get(&self, key: &str) -> R<Option<(String, i64)>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn kv_put(&self, key: &str, value: &str) -> R<()> {
        self.put(key, value);
        Ok(())
    }

    async fn kv_delete(&self, key: &str) -> R<()> {
        self.kv.lock().unwrap().remove(key);
        Ok(())
    }

    async fn lock(&self, _key: &str, _lock_id: &str, _owner: &str, _ttl: Duration) -> R<bool> {
        Ok(true)
    }

    async fn unlock(&self, _key: &str, _lock_id: &str) {}
}

/// A route value as `WatchAppExtensions` delivers it.
fn route(job: &str, service: &str, effective: Value) -> AppExtensionValue {
    AppExtensionValue {
        job_id: job.into(),
        namespace: legion::structs::namespace::job_namespace(job).into(),
        config_version: 1,
        job_status: "running".into(),
        point: "service".into(),
        path: Some(ExtensionPath {
            task: "web".into(),
            service: service.into(),
            ..Default::default()
        }),
        key: "ingress".into(),
        value_json: effective.to_string(),
        effective_json: effective.to_string(),
        state: "valid".into(),
        ..Default::default()
    }
}

fn snapshot(values: Vec<AppExtensionValue>) -> R<AppExtensionEvent> {
    Ok(AppExtensionEvent {
        snapshot: true,
        upserts: values,
        deletes: Vec::new(),
        generation: 1,
    })
}

/// An HTTP backend that answers with what it saw, as JSON.
async fn backend(name: &'static str) -> R<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| async move {
                        let header = |name: &str| {
                            request
                                .headers()
                                .get(name)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string()
                        };
                        let body = json!({
                            "backend": name,
                            "path": request.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/"),
                            "host": header("host"),
                            "xff": header("x-forwarded-for"),
                            "proto": header("x-forwarded-proto"),
                            "forwardedHost": header("x-forwarded-host"),
                            "connection": header("connection"),
                            "version": format!("{:?}", request.version()),
                        });
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(
                            Bytes::from(body.to_string()),
                        )))
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok(addr)
}

/// A backend that switches protocols on any request and echoes bytes.
async fn echo_upgrade_backend() -> R<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if stream.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    head.push(byte[0]);
                }
                let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
                if !text.contains("upgrade: echo") {
                    let _ = stream
                        .write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n")
                        .await;
                    return;
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 101 Switching Protocols\r\nupgrade: echo\r\nconnection: upgrade\r\n\r\n")
                    .await;
                let (mut read, mut write) = stream.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });
    Ok(addr)
}

/// An address nothing listens on.
async fn dead_address() -> R<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

async fn start(config: Value, control: Arc<FakeControl>, https: bool, issue: bool) -> R<Ingress> {
    let state = State::new(IngressConfig::from_value(&config)?);
    let mut ingress = Ingress::start(
        state,
        Listen {
            http: "127.0.0.1:0".parse()?,
            https: https.then(|| "127.0.0.1:0".parse()).transpose()?,
        },
    )
    .await?;
    ingress.connect(control, "test".into(), None, issue);
    Ok(ingress)
}

/// Wait until `check` holds, polling.
async fn eventually(what: &str, mut check: impl FnMut() -> bool) -> R<()> {
    for _ in 0..200 {
        if check() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("timed out waiting for {what}")
}

/// A client that resolves every name to the ingress and never uses a proxy.
fn client(ingress: &Ingress, hosts: &[&str], roots: &[&str]) -> R<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10));
    for host in hosts {
        builder = builder.resolve(host, ingress.http);
    }
    for pem in roots {
        builder = builder.add_root_certificate(reqwest::Certificate::from_pem(pem.as_bytes())?);
    }
    Ok(builder.build()?)
}

async fn get_json(client: &reqwest::Client, url: &str) -> R<(u16, Value)> {
    let response = client.get(url).send().await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    Ok((
        status,
        serde_json::from_str(&body).unwrap_or(Value::String(body)),
    ))
}

fn no_acme() -> Value {
    json!({ "acme": { "enabled": false } })
}

#[tokio::test(flavor = "multi_thread")]
async fn routes_by_host_and_path_with_forwarded_headers_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    let api = backend("api").await?;
    control.set_endpoints("web", "shop", "web", &[site]);
    control.set_endpoints("team-a/api", "team-a/shop-api", "web", &[api]);
    control.set_endpoints("empty", "dark", "web", &[]);
    events.send(snapshot(vec![
        route(
            "shop",
            "web",
            json!({ "host": "shop.example.com", "path": "/", "stripPath": false }),
        ),
        route(
            "team-a/shop-api",
            "api",
            json!({ "host": "shop.example.com", "path": "/api", "stripPath": true }),
        ),
        route(
            "dark",
            "empty",
            json!({ "host": "dark.example.com", "path": "/", "stripPath": false }),
        ),
    ]))?;
    let ingress = start(no_acme(), control.clone(), false, false).await?;
    eventually("the routes", || ingress.state.table().served() == 3).await?;
    let client = client(
        &ingress,
        &["shop.example.com", "dark.example.com", "nope.example.com"],
        &[],
    )?;
    let port = ingress.http.port();

    // The site, with the forwarding headers and the original host.
    let mut site_answer = Value::Null;
    for _ in 0..50 {
        let (status, body) =
            get_json(&client, &format!("http://shop.example.com:{port}/cart?x=1")).await?;
        if status == 200 {
            site_answer = body;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(site_answer["backend"], "site", "{site_answer}");
    assert_eq!(site_answer["path"], "/cart?x=1");
    assert_eq!(site_answer["host"], format!("shop.example.com:{port}"));
    assert_eq!(site_answer["xff"], "127.0.0.1");
    assert_eq!(site_answer["proto"], "http");
    assert_eq!(site_answer["forwardedHost"], "shop.example.com");
    assert_eq!(site_answer["version"], "HTTP/1.1");

    // The API, under its prefix, stripped; a near miss goes to the site.
    let (status, body) = get_json(
        &client,
        &format!("http://shop.example.com:{port}/api/users?page=2"),
    )
    .await?;
    assert_eq!((status, &body["backend"]), (200, &json!("api")), "{body}");
    assert_eq!(body["path"], "/users?page=2");
    let (_, body) = get_json(&client, &format!("http://shop.example.com:{port}/apiary")).await?;
    assert_eq!(body["backend"], "site");

    // An unknown host, and a route with no endpoint.
    let response = client
        .get(format!("http://nope.example.com:{port}/"))
        .send()
        .await?;
    assert_eq!(response.status().as_u16(), 404);
    assert_eq!(response.headers()["legion-ingress-error"], "no_route");
    let body: Value = serde_json::from_str(&response.text().await?)?;
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"]["code"], "no_route");
    let response = client
        .get(format!("http://dark.example.com:{port}/"))
        .send()
        .await?;
    assert_eq!(response.status().as_u16(), 503);
    assert_eq!(response.headers()["legion-ingress-error"], "no_endpoint");

    // HTTP/2 from the client, by prior knowledge, to the HTTP/1.1 backend.
    let stream = tokio::net::TcpStream::connect(ingress.http).await?;
    let (mut sender, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(stream),
    )
    .await?;
    tokio::spawn(connection);
    let request = hyper::Request::builder()
        .uri(format!("http://shop.example.com:{port}/h2"))
        .body(Full::new(Bytes::new()))?;
    let response = sender.send_request(request).await?;
    assert_eq!(response.status(), 200);
    let body: Value = serde_json::from_slice(&response.into_body().collect().await?.to_bytes())?;
    assert_eq!(body["backend"], "site");
    assert_eq!(body["path"], "/h2");
    assert_eq!(body["host"], format!("shop.example.com:{port}"));
    assert_eq!(body["version"], "HTTP/1.1");

    // A route that goes away stops answering.
    events.send(snapshot(vec![route(
        "shop",
        "web",
        json!({ "host": "shop.example.com", "path": "/", "stripPath": false }),
    )]))?;
    eventually("the API route to go", || {
        ingress.state.table().served() == 1
    })
    .await?;
    let (_, body) = get_json(
        &client,
        &format!("http://shop.example.com:{port}/api/users"),
    )
    .await?;
    assert_eq!(body["backend"], "site");
    ingress.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn balances_and_retries_past_a_dead_endpoint_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let one = backend("one").await?;
    let two = backend("two").await?;
    let dead = dead_address().await?;
    control.set_endpoints("web", "shop", "web", &[one, dead, two]);
    events.send(snapshot(vec![route(
        "shop",
        "web",
        json!({ "host": "shop.example.com", "path": "/", "stripPath": false }),
    )]))?;
    let ingress = start(no_acme(), control.clone(), false, false).await?;
    eventually("the endpoints", || {
        ingress
            .state
            .table()
            .backends()
            .next()
            .is_some_and(|backend| backend.endpoints().len() == 3)
    })
    .await?;
    let client = client(&ingress, &["shop.example.com"], &[])?;
    let mut seen: HashMap<String, usize> = HashMap::new();
    for _ in 0..40 {
        let (status, body) = get_json(
            &client,
            &format!("http://shop.example.com:{}/", ingress.http.port()),
        )
        .await?;
        assert_eq!(
            status, 200,
            "every request lands, the dead endpoint retried past: {body}"
        );
        *seen
            .entry(body["backend"].as_str().unwrap_or_default().to_string())
            .or_default() += 1;
    }
    assert_eq!(seen.len(), 2, "both live endpoints served: {seen:?}");
    ingress.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn upgrades_are_relayed_both_ways_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let echo = echo_upgrade_backend().await?;
    control.set_endpoints("ws", "chat", "web", &[echo]);
    events.send(snapshot(vec![route(
        "chat",
        "ws",
        json!({ "host": "chat.example.com", "path": "/", "stripPath": false }),
    )]))?;
    let ingress = start(no_acme(), control.clone(), false, false).await?;
    eventually("the endpoint", || {
        ingress
            .state
            .table()
            .backends()
            .next()
            .is_some_and(|backend| !backend.endpoints().is_empty())
    })
    .await?;
    let mut stream = tokio::net::TcpStream::connect(ingress.http).await?;
    stream
        .write_all(b"GET /socket HTTP/1.1\r\nHost: chat.example.com\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
        .await?;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await?;
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 101"), "{head}");
    assert!(head.contains("upgrade: echo"), "{head}");
    stream.write_all(b"ping through the ingress").await?;
    let mut echoed = [0u8; 24];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut echoed)).await??;
    assert_eq!(&echoed, b"ping through the ingress");
    ingress.stop().await;
    Ok(())
}

/// A self-signed bundle for `names`, and its certificate alone.
fn self_signed(names: &[&str]) -> R<(String, String)> {
    let key = rcgen::KeyPair::generate()?;
    let params = rcgen::CertificateParams::new(
        names
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>(),
    )?;
    let cert = params.self_signed(&key)?;
    Ok((format!("{}{}", cert.pem(), key.serialize_pem()), cert.pem()))
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_operator_bundles_by_sni_and_redirects_http_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    control.set_endpoints("web", "shop", "web", &[site]);
    let (bundle, cert) = self_signed(&["shop.example.com"])?;
    control.put(&format!("{OPERATOR_PREFIX}shop"), &bundle);
    control.put(&format!("{OPERATOR_PREFIX}broken"), "not a bundle");
    events.send(snapshot(vec![
        route(
            "shop",
            "web",
            json!({ "host": "shop.example.com", "path": "/", "stripPath": false }),
        ),
        route(
            "shop",
            "plain",
            json!({ "host": "plain.example.com", "path": "/", "stripPath": false }),
        ),
    ]))?;
    control.set_endpoints("plain", "shop", "web", &[site]);
    let ingress = start(json!({}), control.clone(), true, false).await?;
    eventually("the bundle", || {
        ingress.state.certs.serves("shop.example.com")
    })
    .await?;
    let (_, broken) = ingress.state.certs.snapshot();
    assert_eq!(broken.len(), 1, "a bad bundle is reported, not served");
    let https = ingress.https.context("https listens")?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve("shop.example.com", https)
        .add_root_certificate(reqwest::Certificate::from_pem(cert.as_bytes())?)
        .build()?;
    let mut answer = Value::Null;
    for _ in 0..50 {
        let (status, body) = get_json(
            &client,
            &format!("https://shop.example.com:{}/secure", https.port()),
        )
        .await?;
        if status == 200 {
            answer = body;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(answer["backend"], "site", "{answer}");
    assert_eq!(answer["proto"], "https");
    assert_eq!(answer["path"], "/secure");

    // Plain HTTP for a host with a certificate redirects; one without
    // serves over HTTP.
    let plain = client_for_http(&ingress)?;
    let response = plain
        .get(format!(
            "http://shop.example.com:{}/cart?id=7",
            ingress.http.port()
        ))
        .send()
        .await?;
    assert_eq!(response.status().as_u16(), 308);
    assert_eq!(
        response.headers()["location"],
        "https://shop.example.com/cart?id=7"
    );
    let (status, body) = get_json(
        &plain,
        &format!("http://plain.example.com:{}/", ingress.http.port()),
    )
    .await?;
    assert_eq!((status, &body["proto"]), (200, &json!("http")), "{body}");

    // A name with no certificate fails the handshake rather than serve the
    // wrong one.
    let other = reqwest::Client::builder()
        .no_proxy()
        .resolve("plain.example.com", https)
        .add_root_certificate(reqwest::Certificate::from_pem(cert.as_bytes())?)
        .build()?;
    assert!(other
        .get(format!("https://plain.example.com:{}/", https.port()))
        .send()
        .await
        .is_err());
    ingress.stop().await;
    Ok(())
}

fn client_for_http(ingress: &Ingress) -> R<reqwest::Client> {
    client(ingress, &["shop.example.com", "plain.example.com"], &[])
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_http01_challenges_from_the_shared_board_test() -> R<()> {
    let (control, _events) = FakeControl::new();
    control.put(
        "legion/apps/ingress/acme/challenges/tok_123-abc",
        "tok_123-abc.thumb",
    );
    let ingress = start(no_acme(), control.clone(), false, false).await?;
    let client = client(&ingress, &["any.example.com"], &[])?;
    let port = ingress.http.port();
    let mut found = None;
    for _ in 0..50 {
        let response = client
            .get(format!(
                "http://any.example.com:{port}/.well-known/acme-challenge/tok_123-abc"
            ))
            .send()
            .await?;
        if response.status().as_u16() == 200 {
            found = Some(response.text().await?);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(found.as_deref(), Some("tok_123-abc.thumb"));
    let missing = client
        .get(format!(
            "http://any.example.com:{port}/.well-known/acme-challenge/unknown"
        ))
        .send()
        .await?;
    assert_eq!(missing.status().as_u16(), 404);
    let bad = client
        .get(format!(
            "http://any.example.com:{port}/.well-known/acme-challenge/..%2Fx"
        ))
        .send()
        .await?;
    assert_eq!(bad.status().as_u16(), 404);
    ingress.stop().await;
    Ok(())
}

/// Against Pebble: the issuer registers, orders over HTTP-01 (Pebble's
/// validator fetching the answer from the ingress's HTTP port), stores the
/// bundle in KV, and the HTTPS port serves it. Needs `pebble` and
/// `pebble-challtestsrv` in `LEGION_TEST_PEBBLE_DIR`; skipped otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn orders_a_certificate_from_pebble_test() -> R<()> {
    let Ok(dir) = std::env::var("LEGION_TEST_PEBBLE_DIR") else {
        eprintln!("skipped: set LEGION_TEST_PEBBLE_DIR to a directory with pebble and pebble-challtestsrv");
        return Ok(());
    };
    let dir = std::path::PathBuf::from(dir);
    let work = tempfile::Builder::new()
        .prefix("legion-ingress-pebble-")
        .tempdir_in(crate::support::scratch_root())?;
    // Pebble's own listener certificate, trusted by the issuer.
    let key = rcgen::KeyPair::generate()?;
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])?;
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse()?));
    let pebble_cert = params.self_signed(&key)?;
    std::fs::write(work.path().join("cert.pem"), pebble_cert.pem())?;
    std::fs::write(work.path().join("key.pem"), key.serialize_pem())?;

    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    control.set_endpoints("web", "shop", "web", &[site]);
    let host = "shop.legion-ingress.dev";
    events.send(snapshot(vec![route(
        "shop",
        "web",
        json!({ "host": host, "path": "/", "stripPath": false }),
    )]))?;
    let free_port = |_: ()| -> R<u16> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    };
    let (acme_port, management_port, dns_port) = (free_port(())?, free_port(())?, free_port(())?);
    let state = State::new(IngressConfig::from_value(&json!({
        "acme": {
            "directory": format!("https://127.0.0.1:{acme_port}/dir"),
            "trustRoots": pebble_cert.pem(),
            "email": "ops@legion-ingress.dev",
        }
    }))?);
    let mut ingress = Ingress::start(
        state,
        Listen {
            http: "127.0.0.1:0".parse()?,
            https: Some("127.0.0.1:0".parse()?),
        },
    )
    .await?;
    std::fs::write(
        work.path().join("pebble.json"),
        json!({
            "pebble": {
                "listenAddress": format!("127.0.0.1:{acme_port}"),
                "managementListenAddress": format!("127.0.0.1:{management_port}"),
                "certificate": work.path().join("cert.pem"),
                "privateKey": work.path().join("key.pem"),
                "httpPort": ingress.http.port(),
                "tlsPort": 5001,
                "ocspResponderURL": "",
                "externalAccountBindingRequired": false,
                "retryAfter": { "authz": 1, "order": 1 },
            }
        })
        .to_string(),
    )?;
    let mut challtestsrv = tokio::process::Command::new(dir.join("pebble-challtestsrv"))
        .args([
            "-defaultIPv4",
            "127.0.0.1",
            "-defaultIPv6",
            "",
            "-dns01",
            &format!("127.0.0.1:{dns_port}"),
            "-http01",
            "",
            "-https01",
            "",
            "-tlsalpn01",
            "",
            "-doh",
            "",
            "-management",
            &format!("127.0.0.1:{}", free_port(())?),
        ])
        .kill_on_drop(true)
        .spawn()
        .context("starting pebble-challtestsrv")?;
    let mut pebble = tokio::process::Command::new(dir.join("pebble"))
        .args([
            "-config",
            &work.path().join("pebble.json").to_string_lossy(),
            "-dnsserver",
            &format!("127.0.0.1:{dns_port}"),
        ])
        .env("PEBBLE_VA_NOSLEEP", "1")
        .kill_on_drop(true)
        .spawn()
        .context("starting pebble")?;
    // Wait for Pebble to answer.
    let probe = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(reqwest::Certificate::from_pem(
            pebble_cert.pem().as_bytes(),
        )?)
        .build()?;
    let mut up = false;
    for _ in 0..100 {
        if probe
            .get(format!("https://127.0.0.1:{acme_port}/dir"))
            .send()
            .await
            .is_ok()
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::ensure!(up, "pebble did not start");

    ingress.connect(control.clone(), "test".into(), None, true);
    for _ in 0..600 {
        if ingress.state.certs.serves(host) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let status = ingress.state.issuer_status();
    anyhow::ensure!(
        ingress.state.certs.serves(host),
        "no certificate was ordered: {status:?}"
    );
    assert!(control
        .kv
        .lock()
        .unwrap()
        .contains_key(&format!("legion/apps/ingress/acme/certs/{host}")));
    assert!(
        !control
            .kv
            .lock()
            .unwrap()
            .keys()
            .any(|key| key.contains("/acme/challenges/")),
        "answers are taken down after the order"
    );

    // The chain Pebble issued verifies against Pebble's root.
    let root = probe
        .get(format!("https://127.0.0.1:{management_port}/roots/0"))
        .send()
        .await?
        .text()
        .await?;
    let https = ingress.https.context("https")?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .resolve(host, https)
        .add_root_certificate(reqwest::Certificate::from_pem(root.as_bytes())?)
        .build()?;
    let (status, body) = get_json(&client, &format!("https://{host}:{}/", https.port())).await?;
    assert_eq!((status, &body["backend"]), (200, &json!("site")), "{body}");
    ingress.stop().await;
    let _ = pebble.kill().await;
    let _ = challtestsrv.kill().await;
    Ok(())
}

/// A small ACME CA (RFC 8555), in process, for
/// [`orders_a_certificate_from_an_acme_ca_test`]: it serves a directory,
/// verifies every request's ES256 JWS against the account's key and its
/// nonce and URL, refuses the first order once with `badNonce`, validates
/// HTTP-01 by fetching the answer from the ingress's HTTP port, and signs
/// the CSR with its own root.
struct FakeCa {
    base: String,
    /// The ingress's HTTP port, where HTTP-01 answers are fetched.
    http_port: u16,
    root: rcgen::Certificate,
    root_key: rcgen::KeyPair,
    state: Mutex<CaState>,
}

#[derive(Default)]
struct CaState {
    nonces: std::collections::HashSet<String>,
    jwk: Option<Value>,
    refused_once: bool,
    host: String,
    token: String,
    authorized: Option<bool>,
    chain: Option<String>,
    seen: Vec<String>,
    /// Orders created (a refused attempt is not one).
    orders: u32,
}

fn b64url_decode(text: &str) -> R<Vec<u8>> {
    use base64::Engine as _;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(text)?)
}

fn b64url(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

impl FakeCa {
    fn nonce(&self) -> String {
        let nonce = format!("n{:016x}", rand::random::<u64>());
        self.state.lock().unwrap().nonces.insert(nonce.clone());
        nonce
    }

    fn thumbprint(jwk: &Value) -> String {
        let canonical = format!(
            r#"{{"crv":"{}","kty":"{}","x":"{}","y":"{}"}}"#,
            jwk["crv"].as_str().unwrap_or_default(),
            jwk["kty"].as_str().unwrap_or_default(),
            jwk["x"].as_str().unwrap_or_default(),
            jwk["y"].as_str().unwrap_or_default()
        );
        b64url(ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes()).as_ref())
    }

    /// Check a JWS as RFC 8555 §6.2-6.5 asks: a known, unused nonce, the
    /// request's URL, ES256 over the account's key. Returns the payload.
    fn verify(&self, url: &str, body: &[u8]) -> Result<Value, (u16, &'static str)> {
        let jws: Value = serde_json::from_slice(body).map_err(|_| (400, "malformed"))?;
        let field = |name: &str| jws[name].as_str().unwrap_or_default().to_string();
        let protected: Value = serde_json::from_slice(
            &b64url_decode(&field("protected")).map_err(|_| (400, "malformed"))?,
        )
        .map_err(|_| (400, "malformed"))?;
        let mut state = self.state.lock().unwrap();
        let nonce = protected["nonce"].as_str().unwrap_or_default();
        if !state.nonces.remove(nonce) {
            return Err((400, "badNonce"));
        }
        if protected["url"] != url || protected["alg"] != "ES256" {
            return Err((401, "unauthorized"));
        }
        let jwk = match protected.get("jwk") {
            Some(jwk) => {
                state.jwk = Some(jwk.clone());
                jwk.clone()
            }
            None => {
                if protected["kid"] != format!("{}/acct/1", self.base) {
                    return Err((400, "accountDoesNotExist"));
                }
                state.jwk.clone().ok_or((400, "accountDoesNotExist"))?
            }
        };
        let mut public = vec![0x04];
        public.extend(
            b64url_decode(jwk["x"].as_str().unwrap_or_default()).map_err(|_| (400, "malformed"))?,
        );
        public.extend(
            b64url_decode(jwk["y"].as_str().unwrap_or_default()).map_err(|_| (400, "malformed"))?,
        );
        let message = format!("{}.{}", field("protected"), field("payload"));
        ring::signature::UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_FIXED, public)
            .verify(
                message.as_bytes(),
                &b64url_decode(&field("signature")).map_err(|_| (400, "malformed"))?,
            )
            .map_err(|_| (401, "unauthorized"))?;
        state.seen.push(url.to_string());
        let payload = field("payload");
        if payload.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&b64url_decode(&payload).map_err(|_| (400, "malformed"))?)
            .map_err(|_| (400, "malformed"))
    }

    async fn answer(
        self: Arc<Self>,
        request: hyper::Request<hyper::body::Incoming>,
    ) -> hyper::Response<Full<Bytes>> {
        let path = request.uri().path().to_string();
        let url = format!("{}{path}", self.base);
        let method = request.method().clone();
        let body = request
            .into_body()
            .collect()
            .await
            .map(|body| body.to_bytes())
            .unwrap_or_default();
        let reply = |status: u16, body: Value, location: Option<String>| {
            let mut response = hyper::Response::new(Full::new(Bytes::from(body.to_string())));
            *response.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
            let headers = response.headers_mut();
            headers.insert("replay-nonce", self.nonce().parse().unwrap());
            headers.insert("content-type", "application/json".parse().unwrap());
            if let Some(location) = location {
                headers.insert("location", location.parse().unwrap());
            }
            response
        };
        if path == "/dir" {
            return reply(
                200,
                json!({
                    "newNonce": format!("{}/nonce", self.base),
                    "newAccount": format!("{}/acct", self.base),
                    "newOrder": format!("{}/order", self.base),
                }),
                None,
            );
        }
        if path == "/nonce" && method == hyper::Method::HEAD {
            return reply(200, json!({}), None);
        }
        let payload = match self.verify(&url, &body) {
            Ok(payload) => payload,
            Err((status, kind)) => {
                return reply(
                    status,
                    json!({ "type": format!("urn:ietf:params:acme:error:{kind}"), "detail": kind }),
                    None,
                )
            }
        };
        let order = |state: &CaState, base: &str| {
            let status = match (&state.chain, state.authorized) {
                (Some(_), _) => "valid",
                (None, Some(true)) => "ready",
                (None, Some(false)) => "invalid",
                (None, None) => "pending",
            };
            let mut order = json!({
                "status": status,
                "identifiers": [{ "type": "dns", "value": state.host }],
                "authorizations": [format!("{base}/authz/1")],
                "finalize": format!("{base}/finalize/1"),
            });
            if state.chain.is_some() {
                order["certificate"] = json!(format!("{base}/cert/1"));
            }
            order
        };
        match path.as_str() {
            "/acct" => {
                assert_eq!(payload["termsOfServiceAgreed"], true);
                reply(
                    201,
                    json!({ "status": "valid" }),
                    Some(format!("{}/acct/1", self.base)),
                )
            }
            "/order" => {
                let mut state = self.state.lock().unwrap();
                if !state.refused_once {
                    // A CA may refuse any nonce; the client must retry.
                    state.refused_once = true;
                    drop(state);
                    return reply(
                        400,
                        json!({ "type": "urn:ietf:params:acme:error:badNonce", "detail": "try again" }),
                        None,
                    );
                }
                state.orders += 1;
                state.host = payload["identifiers"][0]["value"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                state.token = format!("tok{:016x}", rand::random::<u64>());
                let body = order(&state, &self.base);
                drop(state);
                reply(201, body, Some(format!("{}/order/1", self.base)))
            }
            "/order/1" => {
                let state = self.state.lock().unwrap();
                let body = order(&state, &self.base);
                drop(state);
                reply(200, body, None)
            }
            "/authz/1" => {
                let state = self.state.lock().unwrap();
                let status = match state.authorized {
                    Some(true) => "valid",
                    Some(false) => "invalid",
                    None => "pending",
                };
                let body = json!({
                    "status": status,
                    "identifier": { "type": "dns", "value": state.host },
                    "challenges": [{
                        "type": "http-01",
                        "url": format!("{}/chall/1", self.base),
                        "token": state.token,
                        "status": status,
                    }],
                });
                drop(state);
                reply(200, body, None)
            }
            "/chall/1" => {
                // Validate as a CA does: fetch the answer over plain HTTP,
                // naming the host, from the address DNS gives (here, the
                // ingress).
                let (host, token, jwk) = {
                    let state = self.state.lock().unwrap();
                    (
                        state.host.clone(),
                        state.token.clone(),
                        state.jwk.clone().unwrap_or_default(),
                    )
                };
                let fetched = reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .unwrap()
                    .get(format!(
                        "http://127.0.0.1:{}/.well-known/acme-challenge/{token}",
                        self.http_port
                    ))
                    .header("host", host)
                    .send()
                    .await;
                let answer = match fetched {
                    Ok(response) if response.status().is_success() => {
                        response.text().await.unwrap_or_default()
                    }
                    _ => String::new(),
                };
                let valid = answer == format!("{token}.{}", Self::thumbprint(&jwk));
                self.state.lock().unwrap().authorized = Some(valid);
                reply(200, json!({ "status": "processing" }), None)
            }
            "/finalize/1" => {
                let der =
                    b64url_decode(payload["csr"].as_str().unwrap_or_default()).unwrap_or_default();
                let csr = rcgen::CertificateSigningRequestParams::from_der(
                    &rustls_pki_types::CertificateSigningRequestDer::from(der),
                )
                .expect("the CSR parses");
                let host = self.state.lock().unwrap().host.clone();
                assert_eq!(
                    csr.params.subject_alt_names,
                    vec![rcgen::SanType::DnsName(host.clone().try_into().unwrap())],
                    "the CSR names the ordered host"
                );
                let leaf = csr.signed_by(&self.root, &self.root_key).expect("signed");
                let mut state = self.state.lock().unwrap();
                state.chain = Some(format!("{}{}", leaf.pem(), self.root.pem()));
                let body = order(&state, &self.base);
                drop(state);
                reply(200, body, None)
            }
            "/cert/1" => {
                let chain = self.state.lock().unwrap().chain.clone().unwrap_or_default();
                let mut response = hyper::Response::new(Full::new(Bytes::from(chain)));
                response.headers_mut().insert(
                    "content-type",
                    "application/pem-certificate-chain".parse().unwrap(),
                );
                response
                    .headers_mut()
                    .insert("replay-nonce", self.nonce().parse().unwrap());
                response
            }
            _ => reply(404, json!({ "detail": "no such resource" }), None),
        }
    }

    /// Serve over TLS on loopback; returns the CA and its TLS root (PEM).
    async fn start(http_port: u16) -> R<(Arc<FakeCa>, String)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
        let tls_key = rcgen::KeyPair::generate()?;
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress("127.0.0.1".parse()?));
        let tls_cert = params.self_signed(&tls_key)?;
        let root_key = rcgen::KeyPair::generate()?;
        let mut root_params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        root_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        root_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Legion test ACME root");
        let root = root_params.self_signed(&root_key)?;
        let ca = Arc::new(FakeCa {
            base,
            http_port,
            root,
            root_key,
            state: Mutex::new(CaState::default()),
        });
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            vec![tls_cert.der().clone()],
            rustls_pki_types::PrivateKeyDer::try_from(tls_key.serialize_der())
                .map_err(|err| anyhow::anyhow!(err))?,
        )?;
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let serving = ca.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let ca = serving.clone();
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(stream).await else {
                        return;
                    };
                    let service = hyper::service::service_fn(move |request| {
                        let ca = ca.clone();
                        async move { Ok::<_, std::convert::Infallible>(ca.answer(request).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Ok((ca, tls_cert.pem()))
    }
}

/// The issuer, against an ACME CA in process: it registers, orders over
/// HTTP-01 (the CA fetching the answer from the ingress's HTTP port by way
/// of the shared board), retries a `badNonce`, finalizes with a CSR for the
/// host, stores the bundle in KV, takes the answer down, and serves the
/// certificate by SNI. A second replica sharing the KV loads it and does not
/// order again.
#[tokio::test(flavor = "multi_thread")]
async fn orders_a_certificate_from_an_acme_ca_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    control.set_endpoints("web", "shop", "web", &[site]);
    let host = "shop.legion-ingress.dev";
    events.send(snapshot(vec![route(
        "shop",
        "web",
        json!({ "host": host, "path": "/", "stripPath": false }),
    )]))?;
    // The CA must know the ingress's HTTP port, and the ingress the CA's
    // directory: pick the port first.
    let http_port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let (ca, tls_root) = FakeCa::start(http_port).await?;
    let state = State::new(IngressConfig::from_value(&json!({
        "acme": {
            "directory": format!("{}/dir", ca.base),
            "trustRoots": tls_root,
            "email": "ops@legion-ingress.dev",
        }
    }))?);
    let http: SocketAddr = format!("127.0.0.1:{}", ca.http_port).parse()?;
    let mut ingress = Ingress::start(
        state,
        Listen {
            http,
            https: Some("127.0.0.1:0".parse()?),
        },
    )
    .await?;
    ingress.connect(control.clone(), "replica-a".into(), None, true);
    for _ in 0..300 {
        if ingress.state.certs.serves(host) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status = ingress.state.issuer_status();
    anyhow::ensure!(
        ingress.state.certs.serves(host),
        "no certificate: {status:?}; the CA saw {:?}",
        ca.state.lock().unwrap().seen
    );
    assert_eq!(
        ca.state.lock().unwrap().authorized,
        Some(true),
        "HTTP-01 answered"
    );
    {
        let kv = control.kv.lock().unwrap();
        assert!(kv.contains_key(&format!("legion/apps/ingress/acme/certs/{host}")));
        assert!(
            kv.keys()
                .any(|key| key.starts_with("legion/apps/ingress/acme/accounts/")),
            "the account is kept for the next issuer"
        );
        assert!(
            !kv.keys().any(|key| key.contains("/acme/challenges/")),
            "answers are taken down after the order"
        );
    }

    // HTTPS, verified against the CA's root.
    let https = ingress.https.context("https")?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .resolve(host, https)
        .add_root_certificate(reqwest::Certificate::from_pem(ca.root.pem().as_bytes())?)
        .build()?;
    let (status, body) = get_json(&client, &format!("https://{host}:{}/", https.port())).await?;
    assert_eq!((status, &body["backend"]), (200, &json!("site")), "{body}");
    assert_eq!(body["proto"], "https");

    // Another replica on the same KV serves it without ordering.
    let (other_control, other_events) = FakeControl::new();
    *other_control.kv.lock().unwrap() = control.kv.lock().unwrap().clone();
    other_events.send(snapshot(vec![route(
        "shop",
        "web",
        json!({ "host": host, "path": "/", "stripPath": false }),
    )]))?;
    let other = start(
        json!({ "acme": { "enabled": false } }),
        other_control,
        true,
        false,
    )
    .await?;
    eventually("the second replica to load the certificate", || {
        other.state.certs.serves(host)
    })
    .await?;
    let (orders, refused_once) = {
        let state = ca.state.lock().unwrap();
        (state.orders, state.refused_once)
    };
    assert!(refused_once, "the badNonce was retried");
    assert_eq!(orders, 1, "one order, however many replicas");
    other.stop().await;
    ingress.stop().await;
    Ok(())
}

/// A backend like [`backend`] that counts the TCP connections it accepts.
async fn counting_backend() -> R<(SocketAddr, Arc<std::sync::atomic::AtomicUsize>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    |request: hyper::Request<hyper::body::Incoming>| async move {
                        let mut response =
                            hyper::Response::new(Full::new(Bytes::from_static(b"pooled")));
                        if request.uri().path() == "/empty" {
                            *response.status_mut() = hyper::StatusCode::NO_CONTENT;
                            *response.body_mut() = Full::new(Bytes::new());
                        }
                        Ok::<_, std::convert::Infallible>(response)
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok((addr, accepted))
}

/// Requests reuse the ingress's connection to a service: GETs, HEADs, and
/// bodiless `204`s, from separate client connections, all ride one.
#[tokio::test(flavor = "multi_thread")]
async fn connections_to_a_service_are_reused_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let (addr, accepted) = counting_backend().await?;
    control.set_endpoints("web", "shop", "web", &[addr]);
    events.send(snapshot(vec![route(
        "shop",
        "web",
        json!({ "host": "shop.example.com", "path": "/", "stripPath": false }),
    )]))?;
    let ingress = start(no_acme(), control.clone(), false, false).await?;
    eventually("the endpoint", || {
        ingress
            .state
            .table()
            .backends()
            .next()
            .is_some_and(|backend| !backend.endpoints().is_empty())
    })
    .await?;
    let port = ingress.http.port();
    for i in 0..12 {
        // A new client connection each time: only the ingress's side pools.
        let client = client(&ingress, &["shop.example.com"], &[])?;
        let url = format!(
            "http://shop.example.com:{port}/{}",
            ["page", "empty"][i % 2]
        );
        let response = if i % 3 == 0 {
            client.head(&url).send().await?
        } else {
            client.get(&url).send().await?
        };
        assert!(response.status().is_success(), "{}", response.status());
        response.bytes().await?;
    }
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "every request rode the first connection"
    );
    ingress.stop().await;
    Ok(())
}
