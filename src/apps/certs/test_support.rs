//! Stand-ins for the outside world the cert manager talks to, for its unit
//! and integration tests: a small HTTP server, and a fake ACME CA
//! ([`FakeAcme`]) that speaks enough of RFC 8555 to issue a certificate.
//!
//! The fake CA checks what a real one checks: every JWS signature (ES256
//! against the account's JWK), single-use nonces (and can refuse one with
//! `badNonce` to exercise the retry), the `url` in each protected header,
//! external account binding when asked, and the CSR's names against the
//! order. It validates HTTP-01 by fetching
//! `/.well-known/acme-challenge/<token>` from a responder it is pointed at
//! (with the name in `Host`), and DNS-01 by reading a fake DNS that
//! [`FakeAcme::dns_webhook`] fills through the webhook provider's protocol.

use crate::apps::certs::acme::{dns01_name, dns01_value, key_authorization, verify_es256};
use crate::prelude::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine as _;
use futures::future::BoxFuture;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex as StdMutex;
use std::time::Duration;

/// One request a test server received.
#[derive(Debug, Clone)]
pub struct Exchange {
    pub method: String,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Exchange {
    pub fn path(&self) -> String {
        self.path_and_query
            .split('?')
            .next()
            .unwrap_or("")
            .to_string()
    }

    pub fn query(&self, name: &str) -> Option<String> {
        let query = self.path_and_query.split_once('?')?.1;
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.into_owned())
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    }
}

/// A test server's answer.
#[derive(Debug, Clone, Default)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl From<(u16, String)> for Reply {
    fn from((status, body): (u16, String)) -> Reply {
        Reply {
            status,
            headers: vec![],
            body: body.into_bytes(),
        }
    }
}

type AsyncHandler = Arc<dyn Fn(Exchange) -> BoxFuture<'static, Reply> + Send + Sync>;

/// Serve `handler` on a fresh loopback port; returns `http://127.0.0.1:<port>`.
pub async fn serve_http<F, A>(handler: F) -> R<String>
where
    F: Fn(Exchange) -> A + Send + Sync + 'static,
    A: Into<Reply> + 'static,
{
    let handler = Arc::new(handler);
    serve_http_async(Arc::new(move |exchange| {
        let reply = handler(exchange).into();
        Box::pin(async move { reply })
    }))
    .await
}

async fn serve_http_async(handler: AsyncHandler) -> R<String> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: hyper::Request<hyper::body::Incoming>| {
                        let handler = handler.clone();
                        async move {
                            let method = request.method().to_string();
                            let path_and_query = request
                                .uri()
                                .path_and_query()
                                .map(|value| value.as_str().to_string())
                                .unwrap_or_default();
                            let headers = request
                                .headers()
                                .iter()
                                .map(|(key, value)| {
                                    (key.to_string(), value.to_str().unwrap_or("").to_string())
                                })
                                .collect();
                            let body = request
                                .into_body()
                                .collect()
                                .await
                                .map(|collected| collected.to_bytes().to_vec())
                                .unwrap_or_default();
                            let reply = handler(Exchange {
                                method,
                                path_and_query,
                                headers,
                                body,
                            })
                            .await;
                            let mut response = hyper::Response::builder().status(reply.status);
                            for (key, value) in &reply.headers {
                                response = response.header(key, value);
                            }
                            Ok::<_, std::convert::Infallible>(
                                response
                                    .body(Full::new(Bytes::from(reply.body)))
                                    .expect("a test response"),
                            )
                        }
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok(format!("http://{addr}"))
}

/// How a [`FakeAcme`] behaves.
#[derive(Debug, Clone)]
pub struct FakeAcmeOptions {
    /// Where HTTP-01 challenges are fetched from (`host:port`); the name
    /// being validated goes in `Host`.
    pub http01_target: Option<String>,
    /// Require external account binding with this key id and MAC key.
    pub eab: Option<(String, Vec<u8>)>,
    /// How long issued certificates last.
    pub lifetime: Duration,
    /// Refuse the first signed request with `badNonce`.
    pub bad_nonce_once: bool,
}

impl Default for FakeAcmeOptions {
    fn default() -> Self {
        FakeAcmeOptions {
            http01_target: None,
            eab: None,
            lifetime: Duration::from_secs(90 * 86400),
            bad_nonce_once: false,
        }
    }
}

#[derive(Debug, Clone)]
struct FakeOrder {
    account: usize,
    identifiers: Vec<(String, String)>,
    authzs: Vec<usize>,
    status: String,
    certificate: Option<String>,
}

#[derive(Debug, Clone)]
struct FakeAuthz {
    account: usize,
    kind: String,
    value: String,
    wildcard: bool,
    token: String,
    status: String,
    error: Option<String>,
    validated_with: Option<String>,
}

struct AcmeState {
    base: String,
    options: FakeAcmeOptions,
    nonces: HashSet<String>,
    next_nonce: u64,
    bad_nonce_pending: bool,
    accounts: Vec<Value>,
    orders: Vec<FakeOrder>,
    authzs: Vec<FakeAuthz>,
    root_cert: rcgen::Certificate,
    root_key: rcgen::KeyPair,
    log: Vec<String>,
}

/// A fake ACME CA on a loopback port.
#[derive(Clone)]
pub struct FakeAcme {
    base: String,
    state: Arc<StdMutex<AcmeState>>,
    dns: Arc<StdMutex<BTreeMap<String, Vec<String>>>>,
}

fn problem(status: u16, kind: &str, detail: &str) -> Reply {
    Reply {
        status,
        headers: vec![(
            "content-type".into(),
            "application/problem+json".into(),
        )],
        body: json!({ "type": format!("urn:ietf:params:acme:error:{kind}"), "detail": detail, "status": status })
            .to_string()
            .into_bytes(),
    }
}

fn json_reply(status: u16, body: Value, location: Option<String>) -> Reply {
    let mut headers = vec![("content-type".to_string(), "application/json".to_string())];
    if let Some(location) = location {
        headers.push(("location".into(), location));
    }
    Reply {
        status,
        headers,
        body: body.to_string().into_bytes(),
    }
}

fn thumbprint_of(jwk: &Value) -> String {
    let canonical = format!(
        r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
        jwk["x"].as_str().unwrap_or(""),
        jwk["y"].as_str().unwrap_or("")
    );
    B64.encode(Sha256::digest(canonical.as_bytes()))
}

impl FakeAcme {
    /// Start a fake CA.
    pub async fn start(options: FakeAcmeOptions) -> R<FakeAcme> {
        let root_key = rcgen::KeyPair::generate()?;
        let mut params = rcgen::CertificateParams::default();
        params.distinguished_name = {
            let mut dn = rcgen::DistinguishedName::new();
            dn.push(rcgen::DnType::CommonName, "Fake ACME Root");
            dn
        };
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let root_cert = params.self_signed(&root_key)?;
        let state = Arc::new(StdMutex::new(AcmeState {
            base: String::new(),
            bad_nonce_pending: options.bad_nonce_once,
            options,
            nonces: HashSet::new(),
            next_nonce: 0,
            accounts: vec![],
            orders: vec![],
            authzs: vec![],
            root_cert,
            root_key,
            log: vec![],
        }));
        let dns: Arc<StdMutex<BTreeMap<String, Vec<String>>>> = Arc::default();
        let fake = FakeAcme {
            base: String::new(),
            state: state.clone(),
            dns: dns.clone(),
        };
        let handler_fake = fake.clone();
        let base = serve_http_async(Arc::new(move |exchange| {
            let fake = handler_fake.clone();
            Box::pin(async move { fake.handle(exchange).await })
        }))
        .await?;
        state.lock().unwrap().base = base.clone();
        Ok(FakeAcme { base, ..fake })
    }

    /// The directory URL to give an Issuer as `acme.server`.
    pub fn directory_url(&self) -> String {
        format!("{}/directory", self.base)
    }

    /// The CA's root, which every certificate it issues chains to.
    pub fn root_pem(&self) -> String {
        self.state.lock().unwrap().root_cert.pem()
    }

    /// Point HTTP-01 validation at a responder.
    pub fn set_http01_target(&self, target: &str) {
        self.state.lock().unwrap().options.http01_target = Some(target.to_string());
    }

    /// How many orders were created.
    pub fn orders(&self) -> usize {
        self.state.lock().unwrap().orders.len()
    }

    /// How many accounts were created.
    pub fn accounts(&self) -> usize {
        self.state.lock().unwrap().accounts.len()
    }

    /// Which challenge type validated each name (`name -> dns-01`).
    pub fn validations(&self) -> BTreeMap<String, String> {
        self.state
            .lock()
            .unwrap()
            .authzs
            .iter()
            .filter_map(|authz| {
                let name = if authz.wildcard {
                    format!("*.{}", authz.value)
                } else {
                    authz.value.clone()
                };
                Some((name, authz.validated_with.clone()?))
            })
            .collect()
    }

    /// What happened, one line per signed request.
    pub fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }

    /// The fake DNS: TXT values by name (with a trailing dot).
    pub fn dns_records(&self) -> BTreeMap<String, Vec<String>> {
        self.dns.lock().unwrap().clone()
    }

    /// A DNS webhook (lego `httpreq` shape) that writes into this CA's
    /// fake DNS; give its URL to an Issuer's `dns01.webhook.url`.
    pub async fn dns_webhook(&self) -> R<String> {
        let dns = self.dns.clone();
        serve_http(move |exchange: Exchange| {
            let body: Value = serde_json::from_slice(&exchange.body).unwrap_or(Value::Null);
            let (Some(fqdn), Some(value)) = (body["fqdn"].as_str(), body["value"].as_str()) else {
                return (400, "fqdn and value are required".to_string());
            };
            let mut dns = dns.lock().unwrap();
            match exchange.path().as_str() {
                "/present" => {
                    dns.entry(fqdn.to_string())
                        .or_default()
                        .push(value.to_string());
                    (200, String::new())
                }
                "/cleanup" => {
                    if let Some(values) = dns.get_mut(fqdn) {
                        values.retain(|v| v != value);
                        if values.is_empty() {
                            dns.remove(fqdn);
                        }
                    }
                    (200, String::new())
                }
                _ => (404, String::new()),
            }
        })
        .await
    }

    fn nonce(state: &mut AcmeState) -> String {
        state.next_nonce += 1;
        let nonce = B64.encode(format!("nonce-{}", state.next_nonce));
        state.nonces.insert(nonce.clone());
        nonce
    }

    async fn handle(&self, exchange: Exchange) -> Reply {
        let mut reply = self.route(&exchange).await;
        let nonce = Self::nonce(&mut self.state.lock().unwrap());
        reply.headers.push(("replay-nonce".into(), nonce));
        reply
    }

    /// Check a JWS: its nonce, url, and signature. Returns the payload
    /// (`None` for POST-as-GET) and the account index (`None` for a
    /// `jwk`-signed newAccount, with the JWK).
    fn verify(&self, exchange: &Exchange) -> Result<(Option<Value>, Result<usize, Value>), Reply> {
        let body: Value = serde_json::from_slice(&exchange.body)
            .map_err(|_| problem(400, "malformed", "the body is not a JWS"))?;
        let (Some(protected_b64), Some(payload_b64), Some(signature_b64)) = (
            body["protected"].as_str(),
            body["payload"].as_str(),
            body["signature"].as_str(),
        ) else {
            return Err(problem(400, "malformed", "not a flattened JWS"));
        };
        let protected: Value = B64
            .decode(protected_b64)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| problem(400, "malformed", "protected header"))?;
        let mut state = self.state.lock().unwrap();
        let nonce = protected["nonce"].as_str().unwrap_or("");
        if state.bad_nonce_pending {
            state.bad_nonce_pending = false;
            state.nonces.remove(nonce);
            return Err(problem(400, "badNonce", "try again"));
        }
        if !state.nonces.remove(nonce) {
            return Err(problem(400, "badNonce", "unknown or reused nonce"));
        }
        let url = format!("{}{}", state.base, exchange.path());
        if protected["url"].as_str() != Some(url.as_str()) {
            return Err(problem(
                400,
                "unauthorized",
                "the url in the JWS is not this one",
            ));
        }
        if protected["alg"] != "ES256" {
            return Err(problem(400, "badSignatureAlgorithm", "ES256 only"));
        }
        let account = match (protected.get("jwk"), protected["kid"].as_str()) {
            (Some(jwk), None) => Err(jwk.clone()),
            (None, Some(kid)) => {
                let index = kid
                    .rsplit('/')
                    .next()
                    .and_then(|n| n.parse::<usize>().ok())
                    .filter(|n| *n < state.accounts.len())
                    .ok_or_else(|| problem(400, "accountDoesNotExist", kid))?;
                Ok(index)
            }
            _ => return Err(problem(400, "malformed", "exactly one of jwk and kid")),
        };
        let jwk = match &account {
            Ok(index) => state.accounts[*index].clone(),
            Err(jwk) => jwk.clone(),
        };
        let signature = B64
            .decode(signature_b64)
            .map_err(|_| problem(400, "malformed", "signature"))?;
        if !verify_es256(
            &jwk,
            format!("{protected_b64}.{payload_b64}").as_bytes(),
            &signature,
        ) {
            return Err(problem(
                403,
                "unauthorized",
                "the signature does not verify",
            ));
        }
        let payload = if payload_b64.is_empty() {
            None
        } else {
            Some(
                B64.decode(payload_b64)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                    .ok_or_else(|| problem(400, "malformed", "payload"))?,
            )
        };
        state
            .log
            .push(format!("{} {}", exchange.method, exchange.path()));
        Ok((payload, account))
    }

    fn order_json(state: &AcmeState, index: usize) -> Value {
        let order = &state.orders[index];
        json!({
            "status": order.status,
            "identifiers": order.identifiers.iter().map(|(t, v)| json!({ "type": t, "value": v })).collect::<Vec<_>>(),
            "authorizations": order.authzs.iter().map(|a| format!("{}/authz/{a}", state.base)).collect::<Vec<_>>(),
            "finalize": format!("{}/order/{index}/finalize", state.base),
            "certificate": order.certificate,
        })
    }

    fn authz_json(state: &AcmeState, index: usize) -> Value {
        let authz = &state.authzs[index];
        let mut challenges = vec![json!({
            "type": "dns-01", "url": format!("{}/chall/{index}/dns-01", state.base),
            "token": authz.token, "status": "pending",
        })];
        if !authz.wildcard {
            challenges.push(json!({
                "type": "http-01", "url": format!("{}/chall/{index}/http-01", state.base),
                "token": authz.token, "status": "pending",
            }));
        }
        if authz.kind == "ip" {
            challenges.retain(|c| c["type"] == "http-01");
        }
        if let Some(error) = &authz.error {
            for challenge in &mut challenges {
                challenge["error"] =
                    json!({ "type": "urn:ietf:params:acme:error:unauthorized", "detail": error });
            }
        }
        json!({
            "identifier": { "type": authz.kind, "value": authz.value },
            "status": authz.status,
            "wildcard": authz.wildcard,
            "challenges": challenges,
        })
    }

    async fn route(&self, exchange: &Exchange) -> Reply {
        let path = exchange.path();
        // The handler's clone was made before the port was known.
        let base = self.state.lock().unwrap().base.clone();
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match (exchange.method.as_str(), segments.as_slice()) {
            ("GET", ["directory"]) => {
                let eab = self.state.lock().unwrap().options.eab.is_some();
                json_reply(
                    200,
                    json!({
                        "newNonce": format!("{base}/nonce"),
                        "newAccount": format!("{base}/account"),
                        "newOrder": format!("{base}/order"),
                        "meta": { "externalAccountRequired": eab },
                    }),
                    None,
                )
            }
            ("HEAD" | "GET", ["nonce"]) => Reply {
                status: 200,
                ..Default::default()
            },
            ("POST", ["account"]) => {
                let (payload, account) = match self.verify(exchange) {
                    Ok(verified) => verified,
                    Err(reply) => return reply,
                };
                let Err(jwk) = account else {
                    return problem(400, "malformed", "newAccount is signed with jwk");
                };
                let payload = payload.unwrap_or_default();
                if payload["termsOfServiceAgreed"] != true {
                    return problem(400, "userActionRequired", "agree to the terms");
                }
                let mut state = self.state.lock().unwrap();
                if let Some((kid, key)) = state.options.eab.clone() {
                    let binding = &payload["externalAccountBinding"];
                    let protected: Value = binding["protected"]
                        .as_str()
                        .and_then(|p| B64.decode(p).ok())
                        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                        .unwrap_or_default();
                    let mac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &key);
                    let input = format!(
                        "{}.{}",
                        binding["protected"].as_str().unwrap_or(""),
                        binding["payload"].as_str().unwrap_or("")
                    );
                    let signature = binding["signature"]
                        .as_str()
                        .and_then(|s| B64.decode(s).ok())
                        .unwrap_or_default();
                    let bound: Value = binding["payload"]
                        .as_str()
                        .and_then(|p| B64.decode(p).ok())
                        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                        .unwrap_or_default();
                    if protected["kid"].as_str() != Some(kid.as_str())
                        || ring::hmac::verify(&mac, input.as_bytes(), &signature).is_err()
                        || bound != jwk
                    {
                        return problem(
                            400,
                            "externalAccountRequired",
                            "the external account binding does not verify",
                        );
                    }
                }
                let existing = state.accounts.iter().position(|known| known == &jwk);
                let (index, status) = match existing {
                    Some(index) => (index, 200),
                    None => {
                        state.accounts.push(jwk);
                        (state.accounts.len() - 1, 201)
                    }
                };
                json_reply(
                    status,
                    json!({ "status": "valid" }),
                    Some(format!("{base}/acct/{index}")),
                )
            }
            ("POST", ["order"]) => {
                let (payload, account) = match self.verify(exchange) {
                    Ok(verified) => verified,
                    Err(reply) => return reply,
                };
                let Ok(account) = account else {
                    return problem(400, "malformed", "newOrder is signed with kid");
                };
                let identifiers: Vec<(String, String)> = payload.unwrap_or_default()["identifiers"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|id| {
                        (
                            id["type"].as_str().unwrap_or("").to_string(),
                            id["value"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect();
                if identifiers.is_empty() {
                    return problem(400, "malformed", "no identifiers");
                }
                if let Some((_, bad)) = identifiers.iter().find(|(_, v)| v.ends_with(".invalid")) {
                    return problem(400, "rejectedIdentifier", &format!("{bad} is not allowed"));
                }
                let mut state = self.state.lock().unwrap();
                let mut authzs = vec![];
                for (kind, value) in &identifiers {
                    let wildcard = value.starts_with("*.");
                    let token = B64.encode(format!("token-{}-{}", state.authzs.len(), value));
                    state.authzs.push(FakeAuthz {
                        account,
                        kind: kind.clone(),
                        value: value.trim_start_matches("*.").to_string(),
                        wildcard,
                        token,
                        status: "pending".into(),
                        error: None,
                        validated_with: None,
                    });
                    authzs.push(state.authzs.len() - 1);
                }
                state.orders.push(FakeOrder {
                    account,
                    identifiers,
                    authzs,
                    status: "pending".into(),
                    certificate: None,
                });
                let index = state.orders.len() - 1;
                json_reply(
                    201,
                    Self::order_json(&state, index),
                    Some(format!("{base}/order/{index}")),
                )
            }
            ("POST", ["authz", index]) => {
                if let Err(reply) = self.verify(exchange) {
                    return reply;
                }
                let state = self.state.lock().unwrap();
                match index
                    .parse::<usize>()
                    .ok()
                    .filter(|i| *i < state.authzs.len())
                {
                    Some(index) => json_reply(200, Self::authz_json(&state, index), None),
                    None => problem(404, "malformed", "no such authorization"),
                }
            }
            ("POST", ["chall", index, kind]) => {
                let account = match self.verify(exchange) {
                    Ok((_, Ok(account))) => account,
                    Ok(_) => return problem(400, "malformed", "kid"),
                    Err(reply) => return reply,
                };
                let Some(index) = index.parse::<usize>().ok() else {
                    return problem(404, "malformed", "no such challenge");
                };
                let (authz, target, thumbprint) = {
                    let mut state = self.state.lock().unwrap();
                    let Some(authz) = state.authzs.get_mut(index) else {
                        return problem(404, "malformed", "no such challenge");
                    };
                    if authz.account != account {
                        return problem(403, "unauthorized", "not your authorization");
                    }
                    authz.status = "processing".into();
                    let authz = authz.clone();
                    let thumbprint = thumbprint_of(&state.accounts[account]);
                    (authz, state.options.http01_target.clone(), thumbprint)
                };
                let kind = kind.to_string();
                let answered = kind.clone();
                let fake = self.clone();
                // Validate in the background, as a CA does; the client polls.
                tokio::spawn(async move {
                    let expected = key_authorization(&authz.token, &thumbprint);
                    let outcome = match kind.as_str() {
                        "dns-01" => {
                            let name = dns01_name(&authz.value);
                            let values = fake.dns.lock().unwrap().get(&name).cloned();
                            if values.unwrap_or_default().contains(&dns01_value(&expected)) {
                                Ok(())
                            } else {
                                Err(format!("no TXT record with the expected value at {name}"))
                            }
                        }
                        "http-01" => match target {
                            None => Err("nothing answers port 80 for this name".to_string()),
                            Some(target) => {
                                let url = format!(
                                    "http://{target}/.well-known/acme-challenge/{}",
                                    authz.token
                                );
                                match reqwest::Client::new()
                                    .get(&url)
                                    .header("host", authz.value.clone())
                                    .send()
                                    .await
                                {
                                    Ok(response) if response.status().is_success() => {
                                        let body = response.text().await.unwrap_or_default();
                                        if body.trim() == expected {
                                            Ok(())
                                        } else {
                                            Err(format!("the responder answered {body:?}"))
                                        }
                                    }
                                    Ok(response) => {
                                        Err(format!("the responder answered {}", response.status()))
                                    }
                                    Err(err) => Err(format!("could not reach {target}: {err}")),
                                }
                            }
                        },
                        other => Err(format!("unknown challenge {other}")),
                    };
                    let mut state = fake.state.lock().unwrap();
                    let entry = &mut state.authzs[index];
                    match outcome {
                        Ok(()) => {
                            entry.status = "valid".into();
                            entry.validated_with = Some(kind);
                        }
                        Err(error) => {
                            entry.status = "invalid".into();
                            entry.error = Some(error);
                        }
                    }
                });
                json_reply(
                    200,
                    json!({ "type": answered, "status": "processing" }),
                    None,
                )
            }
            ("POST", ["order", index, "finalize"]) => {
                let (payload, account) = match self.verify(exchange) {
                    Ok(verified) => verified,
                    Err(reply) => return reply,
                };
                let mut state = self.state.lock().unwrap();
                let Some(index) = index
                    .parse::<usize>()
                    .ok()
                    .filter(|i| *i < state.orders.len())
                else {
                    return problem(404, "malformed", "no such order");
                };
                let order = state.orders[index].clone();
                if Ok(order.account) != account {
                    return problem(403, "unauthorized", "not your order");
                }
                if !order
                    .authzs
                    .iter()
                    .all(|a| state.authzs[*a].status == "valid")
                {
                    return problem(403, "orderNotReady", "authorizations are not valid");
                }
                let csr = payload.unwrap_or_default()["csr"]
                    .as_str()
                    .and_then(|csr| B64.decode(csr).ok())
                    .unwrap_or_default();
                let Ok(mut request) = rcgen::CertificateSigningRequestParams::from_der(&csr.into())
                else {
                    return problem(400, "badCSR", "the CSR does not parse");
                };
                let mut wanted: Vec<String> =
                    order.identifiers.iter().map(|(_, v)| v.clone()).collect();
                wanted.sort();
                let mut named: Vec<String> = request
                    .params
                    .subject_alt_names
                    .iter()
                    .map(|san| match san {
                        rcgen::SanType::DnsName(name) => name.as_str().to_string(),
                        rcgen::SanType::IpAddress(ip) => ip.to_string(),
                        other => format!("{other:?}"),
                    })
                    .collect();
                named.sort();
                if named != wanted {
                    return problem(400, "badCSR", "the CSR's names are not the order's");
                }
                let now = time::OffsetDateTime::now_utc();
                request.params.not_before = now - time::Duration::hours(1);
                request.params.not_after = now
                    + time::Duration::seconds(state.options.lifetime.as_secs() as i64)
                    - time::Duration::hours(1);
                request.params.extended_key_usages =
                    vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
                request.params.use_authority_key_identifier_extension = true;
                let leaf = match request.signed_by(&state.root_cert, &state.root_key) {
                    Ok(leaf) => leaf,
                    Err(err) => return problem(500, "serverInternal", &err.to_string()),
                };
                // As Let's Encrypt sends it: the leaf, then the issuer.
                let chain = format!("{}{}", leaf.pem(), state.root_cert.pem());
                state.orders[index].status = "valid".into();
                state.orders[index].certificate = Some(format!("{base}/cert/{index}"));
                state.log.push(format!("issued {}", wanted.join(",")));
                CERTS
                    .lock()
                    .unwrap()
                    .insert(format!("{base}/cert/{index}"), chain);
                json_reply(200, Self::order_json(&state, index), None)
            }
            ("POST", ["order", index]) => {
                if let Err(reply) = self.verify(exchange) {
                    return reply;
                }
                let state = self.state.lock().unwrap();
                match index
                    .parse::<usize>()
                    .ok()
                    .filter(|i| *i < state.orders.len())
                {
                    Some(index) => json_reply(200, Self::order_json(&state, index), None),
                    None => problem(404, "malformed", "no such order"),
                }
            }
            ("POST", ["cert", _]) => {
                if let Err(reply) = self.verify(exchange) {
                    return reply;
                }
                let url = format!("{base}{path}");
                match CERTS.lock().unwrap().get(&url).cloned() {
                    Some(chain) => Reply {
                        status: 200,
                        headers: vec![(
                            "content-type".into(),
                            "application/pem-certificate-chain".into(),
                        )],
                        body: chain.into_bytes(),
                    },
                    None => problem(404, "malformed", "no such certificate"),
                }
            }
            _ => problem(404, "malformed", &format!("no route for {path}")),
        }
    }
}

/// Issued chains by certificate URL (URLs carry the server's port, so
/// servers never collide).
static CERTS: std::sync::LazyLock<StdMutex<HashMap<String, String>>> =
    std::sync::LazyLock::new(Default::default);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::certs::acme::{AccountKey, AcmeClient, Proof};
    use crate::apps::certs::dns01::Webhook;
    use crate::apps::certs::issue::{acme_bundle, csr, verify_chain};
    use crate::apps::certs::spec::{CertificateSpec, Usage};

    fn desired(names: &[&str]) -> crate::apps::certs::spec::Desired {
        serde_json::from_value::<CertificateSpec>(json!({
            "issuer": "le", "kv": "certs/x", "dnsNames": names,
        }))
        .unwrap()
        .desired()
        .unwrap()
    }

    /// The whole ACME flow against the fake CA: an account (after a
    /// `badNonce`), DNS-01 through the webhook for a wildcard, the record
    /// removed afterwards, and a chain that verifies.
    #[tokio::test]
    async fn dns01_issues_a_wildcard_through_the_webhook_test() -> R<()> {
        let ca = FakeAcme::start(FakeAcmeOptions {
            bad_nonce_once: true,
            ..Default::default()
        })
        .await?;
        let http = reqwest::Client::new();
        let key = Arc::new(AccountKey::generate()?);
        let client = AcmeClient::connect(http.clone(), &ca.directory_url(), key).await?;
        let account = client.register(Some("ops@example.com"), None).await?;
        assert!(account.ends_with("/acct/0"), "{account}");
        assert_eq!(ca.accounts(), 1);
        // The same key registers as the same account.
        client.register(None, None).await?;
        assert_eq!(ca.accounts(), 1);

        let hook = ca.dns_webhook().await?;
        let want = desired(&["example.com", "*.example.com"]);
        let (leaf_key, csr_der) = csr(&want)?;
        let proof = Proof {
            dns01: Some((Arc::new(Webhook::new(http, &hook, None)), Duration::ZERO)),
            http01: false,
        };
        let chain = client
            .issue(&want.names(), &csr_der, &proof, Duration::from_secs(30))
            .await?;
        let bundle = acme_bundle(&chain, &leaf_key)?;
        assert!(bundle.leaf.names_match(&want));
        verify_chain(
            &bundle.chain_pem,
            &ca.root_pem(),
            "www.example.com",
            Usage::Server,
        )?;
        assert_eq!(
            ca.validations().get("*.example.com").map(String::as_str),
            Some("dns-01")
        );
        assert!(ca.dns_records().is_empty(), "the records were cleaned up");
        Ok(())
    }

    #[tokio::test]
    async fn http01_is_answered_by_the_stateless_responder_test() -> R<()> {
        let ca = FakeAcme::start(FakeAcmeOptions::default()).await?;
        let key = Arc::new(AccountKey::generate()?);
        // The responder, told only the thumbprint.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        ca.set_http01_target(&listener.local_addr()?.to_string());
        let thumbprint = Arc::new(std::sync::RwLock::new(Some(key.thumbprint().to_string())));
        tokio::spawn(crate::apps::certs::http01::serve(
            listener, thumbprint, true,
        ));

        let client = AcmeClient::connect(reqwest::Client::new(), &ca.directory_url(), key).await?;
        client.register(None, None).await?;
        let want = desired(&["shop.example.com"]);
        let (leaf_key, csr_der) = csr(&want)?;
        let proof = Proof {
            dns01: None,
            http01: true,
        };
        let chain = client
            .issue(&want.names(), &csr_der, &proof, Duration::from_secs(30))
            .await?;
        acme_bundle(&chain, &leaf_key)?;
        assert_eq!(
            ca.validations().get("shop.example.com").map(String::as_str),
            Some("http-01")
        );

        // A wildcard cannot be proven over HTTP-01.
        let wild = desired(&["*.example.com"]);
        let (_, csr_der) = csr(&wild)?;
        let err = client
            .issue(&wild.names(), &csr_der, &proof, Duration::from_secs(30))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no challenge this Issuer can answer"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_validation_names_the_reason_test() -> R<()> {
        let ca = FakeAcme::start(FakeAcmeOptions::default()).await?;
        let key = Arc::new(AccountKey::generate()?);
        let client = AcmeClient::connect(reqwest::Client::new(), &ca.directory_url(), key).await?;
        client.register(None, None).await?;
        let want = desired(&["shop.example.com"]);
        let (_, csr_der) = csr(&want)?;
        let err = client
            .issue(
                &want.names(),
                &csr_der,
                &Proof {
                    dns01: None,
                    http01: true,
                },
                Duration::from_secs(30),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("could not verify control") && err.contains("nothing answers port 80"),
            "{err}"
        );
        // A rejected name fails at the order.
        let bad = desired(&["a.invalid"]);
        let (_, csr_der) = csr(&bad)?;
        let err = client
            .issue(
                &bad.names(),
                &csr_der,
                &Proof {
                    dns01: None,
                    http01: true,
                },
                Duration::from_secs(30),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("rejectedIdentifier"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn external_account_binding_is_required_when_the_ca_says_test() -> R<()> {
        let mac = b"a-shared-mac-key".to_vec();
        let ca = FakeAcme::start(FakeAcmeOptions {
            eab: Some(("kid-7".into(), mac.clone())),
            ..Default::default()
        })
        .await?;
        let key = Arc::new(AccountKey::generate()?);
        let client = AcmeClient::connect(reqwest::Client::new(), &ca.directory_url(), key).await?;
        let err = client.register(None, None).await.unwrap_err().to_string();
        assert!(err.contains("requires external account binding"), "{err}");
        let err = client
            .register(None, Some(("kid-7", b"wrong")))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not verify"), "{err}");
        client.register(None, Some(("kid-7", &mac))).await?;
        assert_eq!(ca.accounts(), 1);
        Ok(())
    }
}
