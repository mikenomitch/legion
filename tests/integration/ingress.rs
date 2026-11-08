//! The bundled ingress (docs/ingress.md; ingress plan §9): the real proxy,
//! certificate store, and certificates loop, driven through a fake
//! [`Control`] so no cluster is needed, against real HTTP backends on
//! loopback.
//!
//! - routing by host and by path prefix, `stripPath`, `X-Forwarded-*`, and
//!   the standard error envelope for an unknown host or a route with no
//!   endpoint;
//! - HTTP/2 from the client (prior knowledge) to an HTTP/1.1 backend;
//! - balancing across endpoints, and retrying past one that refuses;
//! - a WebSocket-style upgrade relayed both ways;
//! - connections to a service reused across requests;
//! - HTTPS from an operator bundle in KV, chosen by SNI, and the redirect
//!   from HTTP once a certificate is served;
//! - one `certs/Certificate` kept per routed host the Issuer can prove,
//!   dropped with its route, and its bundle served once the cert manager
//!   writes it; the default Issuer kept from the config;
//! - HTTP-01 answered from the Issuer's thumbprint, with no shared state;
//! - where to point DNS, from the ingress's own catalog service, and each
//!   host's DNS checked against it.
//!
//! The cert manager issuing for the ingress, end to end against a fake ACME
//! CA that validates HTTP-01 through the ingress, is in `certs.rs`. The
//! manager side (the key registered by seeding, checked at submit, and
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
use legion::apps::ingress::certificates::{
    certificate_name, issued_key, CertState, DEFAULT_ISSUER, HOST_LABEL,
    NAMESPACE as CERT_NAMESPACE,
};
use legion::apps::ingress::certs::{KvVersion, OPERATOR_PREFIX};
use legion::apps::ingress::control::{Control, ObjectView};
use legion::apps::ingress::edge::{self, DnsCheck};
use legion::apps::ingress::{Ingress, IngressConfig, Listen, State};
use legion::structs::{AppExtensionEvent, AppExtensionValue, ExtensionPath};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type R<T> = anyhow::Result<T>;

/// Routes, endpoints, KV, the certificates lock, and objects, in memory.
/// `certs.rs` borrows its routes and endpoints for the end-to-end test.
#[derive(Default)]
pub(crate) struct FakeControl {
    events: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<R<AppExtensionEvent>>>>,
    /// Service ref -> (job\0task, endpoint).
    endpoints: Mutex<HashMap<String, Vec<(String, CatalogEndpoint)>>>,
    kv: Mutex<BTreeMap<String, (String, i64)>>,
    version: std::sync::atomic::AtomicI64,
    /// (kind, namespace, name) to the object.
    objects: Mutex<BTreeMap<(String, String, String), ObjectView>>,
}

impl FakeControl {
    pub(crate) fn new() -> (
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

    pub(crate) fn set_endpoints(&self, service: &str, job: &str, task: &str, addrs: &[SocketAddr]) {
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
                        public_address: String::new(),
                    },
                )
            })
            .collect();
        self.endpoints
            .lock()
            .unwrap()
            .insert(service.to_string(), entries);
    }

    /// The ingress's own replicas: (node, cluster address, public address).
    fn set_edge(&self, replicas: &[(&str, &str, &str)]) {
        let entries = replicas
            .iter()
            .enumerate()
            .map(|(i, (node, host, public))| {
                (
                    "legion-system/ingress\u{0}proxy".to_string(),
                    CatalogEndpoint {
                        host: host.to_string(),
                        port: 80,
                        allocation_id: i as i64 + 1,
                        node_id: node.to_string(),
                        healthy: true,
                        mesh_tls: false,
                        public_address: public.to_string(),
                    },
                )
            })
            .collect();
        self.endpoints
            .lock()
            .unwrap()
            .insert(edge::SERVICE.to_string(), entries);
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

    async fn apply_object(&self, document: &Value) -> R<()> {
        let text = |field: &str| document[field].as_str().unwrap_or_default().to_string();
        let key = (text("kind"), text("namespace"), text("name"));
        let mut objects = self.objects.lock().unwrap();
        let previous = objects.get(&key).cloned().unwrap_or_default();
        objects.insert(
            key,
            ObjectView {
                name: text("name"),
                namespace: text("namespace"),
                labels: serde_json::from_value(document["labels"].clone()).unwrap_or_default(),
                spec: document["spec"].clone(),
                ..previous
            },
        );
        Ok(())
    }

    async fn get_object(&self, kind: &str, name: &str, namespace: &str) -> R<Option<ObjectView>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(&(kind.to_string(), namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn list_objects(
        &self,
        kind: &str,
        namespace: &str,
        labels: &BTreeMap<String, String>,
    ) -> R<Vec<ObjectView>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|((k, ns, _), _)| k == kind && ns == namespace)
            .filter(|(_, object)| {
                labels
                    .iter()
                    .all(|(key, value)| object.labels.get(key) == Some(value))
            })
            .map(|(_, object)| object.clone())
            .collect())
    }

    async fn delete_object(&self, kind: &str, name: &str, namespace: &str) -> R<()> {
        self.objects.lock().unwrap().remove(&(
            kind.to_string(),
            namespace.to_string(),
            name.to_string(),
        ));
        Ok(())
    }
}

impl FakeControl {
    /// Report a status on an object, as its app would.
    fn report(&self, kind: &str, namespace: &str, name: &str, ready: (&str, &str), fields: Value) {
        let mut objects = self.objects.lock().unwrap();
        let object = objects
            .get_mut(&(kind.to_string(), namespace.to_string(), name.to_string()))
            .expect("the object exists");
        object.ready = Some((ready.0.to_string(), ready.1.to_string(), String::new()));
        object.fields = fields;
        object.reported = true;
    }

    fn object(&self, kind: &str, namespace: &str, name: &str) -> Option<ObjectView> {
        self.objects
            .lock()
            .unwrap()
            .get(&(kind.to_string(), namespace.to_string(), name.to_string()))
            .cloned()
    }

    fn certificates(&self) -> Vec<String> {
        self.objects
            .lock()
            .unwrap()
            .keys()
            .filter(|(kind, _, _)| kind == "certs/Certificate")
            .map(|(_, _, name)| name.clone())
            .collect()
    }
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

pub(crate) fn snapshot(values: Vec<AppExtensionValue>) -> R<AppExtensionEvent> {
    Ok(AppExtensionEvent {
        snapshot: true,
        upserts: values,
        deletes: Vec::new(),
        generation: 1,
    })
}

/// An HTTP backend that answers with what it saw, as JSON.
pub(crate) async fn backend(name: &'static str) -> R<SocketAddr> {
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
    start_with_edge(config, control, https, issue, None).await
}

/// [`start`], following the ingress's own replicas at `edge_service`.
async fn start_with_edge(
    config: Value,
    control: Arc<FakeControl>,
    https: bool,
    issue: bool,
    edge_service: Option<&str>,
) -> R<Ingress> {
    let state = State::new(IngressConfig::from_value(&config)?);
    let mut ingress = Ingress::start(
        state,
        Listen {
            http: "127.0.0.1:0".parse()?,
            https: https.then(|| "127.0.0.1:0".parse()).transpose()?,
        },
    )
    .await?;
    ingress.connect(
        control,
        "test".into(),
        edge_service.map(str::to_string),
        issue,
    );
    Ok(ingress)
}

/// Wait until `check` holds, polling.
async fn eventually(what: &str, check: impl FnMut() -> bool) -> R<()> {
    eventually_within(what, Duration::from_secs(10), check).await
}

async fn eventually_within(what: &str, within: Duration, mut check: impl FnMut() -> bool) -> R<()> {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
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

fn no_certificates() -> Value {
    json!({ "certificates": false })
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
    let ingress = start(no_certificates(), control.clone(), false, false).await?;
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
    let ingress = start(no_certificates(), control.clone(), false, false).await?;
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
    let ingress = start(no_certificates(), control.clone(), false, false).await?;
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
    let ingress = start(no_certificates(), control.clone(), false, false).await?;
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

pub(crate) fn web_route(service: &str, host: &str) -> AppExtensionValue {
    route(
        "shop",
        service,
        json!({ "host": host, "path": "/", "stripPath": false }),
    )
}

fn skipped(states: &BTreeMap<String, CertState>, host: &str) -> String {
    match states.get(host) {
        Some(CertState::Skipped(why)) => why.clone(),
        other => panic!("{host} is not skipped: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn keeps_one_certificate_per_routed_host_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    for service in ["web", "wild", "intra", "blog"] {
        control.set_endpoints(service, "shop", "web", &[site]);
    }
    // What the cert manager writes for shop.example.com, at the key the
    // ingress's Certificate names (stable per host, so it can be first).
    let (bundle, cert) = self_signed(&["shop.example.com"])?;
    control.put(&issued_key("shop.example.com"), &bundle);
    events.send(snapshot(vec![
        web_route("web", "shop.example.com"),
        web_route("wild", "*.apps.example.com"),
        web_route("intra", "db.internal"),
    ]))?;
    let ingress = start(
        json!({ "acme": { "email": "ops@example.com" } }),
        control.clone(),
        true,
        true,
    )
    .await?;
    eventually("the certificate states", || {
        ingress.state.cert_states().len() == 3
    })
    .await?;
    // Nothing is ordered until the cert manager has registered the
    // Issuer's account: its thumbprint answers the HTTP-01 challenge.
    assert_eq!(
        ingress.state.cert_states()["shop.example.com"],
        CertState::AwaitingIssuer
    );
    assert!(control.certificates().is_empty());
    control.report(
        "certs/Issuer",
        "",
        DEFAULT_ISSUER,
        ("true", "Registered"),
        json!({ "thumbprint": "thumb-print" }),
    );
    eventually_within("the Certificate", Duration::from_secs(15), || {
        ingress.state.cert_states().get("shop.example.com") == Some(&CertState::Waiting)
    })
    .await?;

    // The default Issuer, from the config: HTTP-01 against Let's Encrypt,
    // for the ingress's namespace only.
    let issuer = control
        .object("certs/Issuer", "", DEFAULT_ISSUER)
        .context("the default Issuer")?;
    assert_eq!(
        issuer.spec,
        json!({
            "namespaces": ["legion-system"],
            "acme": { "server": "letsencrypt", "email": "ops@example.com", "http01": {} },
        })
    );

    // One Certificate, for the one host an HTTP-01 Issuer can prove.
    let shop = certificate_name("shop.example.com");
    assert_eq!(control.certificates(), vec![shop.clone()]);
    let certificate = control
        .object("certs/Certificate", CERT_NAMESPACE, &shop)
        .context("shop's Certificate")?;
    assert_eq!(
        certificate.spec,
        json!({
            "issuer": "ingress",
            "dnsNames": ["shop.example.com"],
            "kv": issued_key("shop.example.com"),
        })
    );
    assert_eq!(certificate.labels[HOST_LABEL], "shop.example.com");
    let states = ingress.state.cert_states();
    assert_eq!(states["shop.example.com"], CertState::Waiting);
    let why = skipped(&states, "*.apps.example.com");
    assert!(why.contains("DNS-01"), "{why}");
    let why = skipped(&states, "db.internal");
    assert!(why.contains("no public CA"), "{why}");

    // The bundle at the key it names is served by SNI.
    eventually("the issued bundle", || {
        ingress.state.certs.serves("shop.example.com")
    })
    .await?;
    let https = ingress.https.context("https listens")?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .resolve("shop.example.com", https)
        .add_root_certificate(reqwest::Certificate::from_pem(cert.as_bytes())?)
        .build()?;
    let (status, body) = get_json(
        &client,
        &format!("https://shop.example.com:{}/", https.port()),
    )
    .await?;
    assert_eq!((status, &body["proto"]), (200, &json!("https")), "{body}");

    // The cert manager reports it issued; a new route gets its own
    // Certificate, and routes that leave take theirs with them.
    control.report(
        "certs/Certificate",
        CERT_NAMESPACE,
        &shop,
        ("true", "Issued"),
        json!({}),
    );
    events.send(snapshot(vec![
        web_route("web", "shop.example.com"),
        web_route("blog", "blog.example.com"),
    ]))?;
    let blog = certificate_name("blog.example.com");
    eventually("blog's Certificate and shop's status", || {
        let states = ingress.state.cert_states();
        control.certificates() == vec![blog.clone(), shop.clone()]
            && matches!(
                states.get("shop.example.com"),
                Some(CertState::Requested { ready: true, reason, .. }) if reason == "Issued"
            )
            && states.len() == 2
    })
    .await?;
    events.send(snapshot(vec![web_route("blog", "blog.example.com")]))?;
    eventually("shop's Certificate dropped", || {
        control.certificates() == vec![blog.clone()]
    })
    .await?;
    assert!(
        !control
            .kv
            .lock()
            .unwrap()
            .contains_key(&issued_key("shop.example.com")),
        "its bundle goes with it"
    );
    ingress.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_named_issuer_covers_wildcards_and_private_names_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    for service in ["web", "wild", "intra"] {
        control.set_endpoints(service, "shop", "web", &[site]);
    }
    control
        .apply_object(&json!({
            "kind": "certs/Issuer",
            "name": "corp-ca",
            "spec": {
                "namespaces": ["legion-system"],
                "ca": { "kv": "certs/ca/corp" },
            },
        }))
        .await?;
    events.send(snapshot(vec![
        web_route("web", "shop.example.com"),
        web_route("wild", "*.apps.example.com"),
        web_route("intra", "db.internal"),
    ]))?;
    let ingress = start(json!({ "issuer": "corp-ca" }), control.clone(), false, true).await?;
    eventually("a Certificate per host", || {
        control.certificates().len() == 3
    })
    .await?;
    for host in ["shop.example.com", "*.apps.example.com", "db.internal"] {
        let certificate = control
            .object("certs/Certificate", CERT_NAMESPACE, &certificate_name(host))
            .with_context(|| format!("{host}'s Certificate"))?;
        assert_eq!(certificate.spec["issuer"], "corp-ca");
        assert_eq!(certificate.spec["dnsNames"], json!([host]));
    }
    assert!(
        control.object("certs/Issuer", "", DEFAULT_ISSUER).is_none(),
        "the ingress keeps no Issuer of its own when the config names one"
    );
    ingress.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_http01_with_the_issuer_thumbprint_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    control.set_endpoints("web", "shop", "web", &[site]);
    events.send(snapshot(vec![web_route("web", "shop.example.com")]))?;
    let ingress = start(json!({}), control.clone(), false, true).await?;
    eventually("the default Issuer", || {
        control.object("certs/Issuer", "", DEFAULT_ISSUER).is_some()
    })
    .await?;
    let http = client(&ingress, &["shop.example.com"], &[])?;
    let challenge = format!(
        "http://shop.example.com:{}/.well-known/acme-challenge/tok_123-abc",
        ingress.http.port()
    );
    // Until the cert manager registers the account there is nothing to
    // answer with.
    let response = http.get(&challenge).send().await?;
    assert_eq!(response.status().as_u16(), 503);

    control.report(
        "certs/Issuer",
        "",
        DEFAULT_ISSUER,
        ("true", "Registered"),
        json!({ "thumbprint": "thumb-print" }),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let answer = loop {
        let response = http.get(&challenge).send().await?;
        if response.status().as_u16() == 200 {
            break response.text().await?;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the challenge is never answered"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(answer, "tok_123-abc.thumb-print");
    // Every other path on the host is the app's.
    let (status, body) = get_json(
        &http,
        &format!("http://shop.example.com:{}/cart", ingress.http.port()),
    )
    .await?;
    assert_eq!((status, &body["backend"]), (200, &json!("site")), "{body}");
    ingress.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn says_where_to_point_dns_test() -> R<()> {
    let (control, events) = FakeControl::new();
    let site = backend("site").await?;
    for service in ["here", "elsewhere", "missing", "wild"] {
        control.set_endpoints(service, "shop", "web", &[site]);
    }
    control.set_edge(&[
        ("mgr-1", "127.0.0.1", ""),
        ("mgr-2", "10.0.0.2", "203.0.113.7"),
    ]);
    events.send(snapshot(vec![
        web_route("here", "localhost"),
        // An address literal resolves to itself, and no replica has it.
        web_route("elsewhere", "127.0.0.2"),
        web_route("missing", "nothing.invalid"),
        web_route("wild", "*.apps.example.com"),
    ]))?;
    let ingress = start_with_edge(
        no_certificates(),
        control.clone(),
        false,
        false,
        Some(edge::SERVICE),
    )
    .await?;
    eventually("every exact host checked", || {
        ingress.state.dns().len() == 3
    })
    .await?;
    let targets: Vec<String> = ingress
        .state
        .edge()
        .iter()
        .map(|replica| replica.target().to_string())
        .collect();
    assert_eq!(targets, ["127.0.0.1", "203.0.113.7"], "public first");
    let dns = ingress.state.dns();
    assert_eq!(dns["localhost"], DnsCheck::Here);
    assert_eq!(
        dns["127.0.0.2"],
        DnsCheck::Elsewhere(vec!["127.0.0.2".parse()?])
    );
    assert_eq!(dns["nothing.invalid"], DnsCheck::Missing);

    // The Ingress page says it in words.
    let viewer = legion::apps::ui::Viewer {
        principal: "user:alice".into(),
        accessor: None,
        permission: Some("job:read".into()),
        scope: legion::structs::app_ui::AssertionScope::Any,
        namespaces: vec!["*".into()],
        delegation: None,
    };
    let page = serde_json::to_string(&legion::apps::ingress::views::routes_view(
        &ingress.state,
        &viewer,
    ))?;
    for expected in [
        "Point DNS here",
        "203.0.113.7",
        "LEGION_WORKER_PUBLIC_ADDRESS",
        "points here",
        "points to 127.0.0.2",
        "no record",
    ] {
        assert!(page.contains(expected), "{expected:?} not in {page}");
    }
    ingress.stop().await;
    Ok(())
}
