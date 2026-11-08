//! A small ACME client (RFC 8555) for the ingress's certificates
//! (docs/ingress.md, "Automatic certificates"; ingress plan calls entry 5).
//!
//! It does what the ingress needs and no more: register an account, order
//! one certificate for one host, answer its HTTP-01 challenge through a
//! [`ChallengeBoard`] every replica reads, finalize with a fresh P-256 key,
//! and download the chain. Requests are JWS-signed with ES256 (`ring`),
//! sent with async `reqwest`, and retried once on a `badNonce`.

use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, KeyPair as _, ECDSA_P256_SHA256_FIXED_SIGNING};
use serde::Deserialize;

use crate::prelude::*;

/// Let's Encrypt's production directory, the default.
pub const LETS_ENCRYPT: &str = "https://acme-v02.api.letsencrypt.org/directory";
/// Let's Encrypt's staging directory: untrusted certificates, generous
/// limits.
pub const LETS_ENCRYPT_STAGING: &str = "https://acme-staging-v02.api.letsencrypt.org/directory";
/// How long one order may take from new order to downloaded chain.
pub const ORDER_TIMEOUT: Duration = Duration::from_secs(120);
/// One HTTP exchange with the CA.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The directory URL a config value names: an alias or a URL.
pub fn directory_url(configured: Option<&str>) -> R<String> {
    match configured.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("letsencrypt") => Ok(LETS_ENCRYPT.to_string()),
        Some("letsencrypt-staging") => Ok(LETS_ENCRYPT_STAGING.to_string()),
        Some(url) if url.starts_with("https://") => Ok(url.to_string()),
        Some(other) => bail!(
            "acme.directory {other:?} is not `letsencrypt`, `letsencrypt-staging`, or an https:// URL"
        ),
    }
}

fn b64(bytes: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// An HTTP client for the CA, trusting `trust_roots` (PEM) beside the
/// public roots.
pub fn http_client(trust_roots: Option<&str>) -> R<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .user_agent(concat!("legion-ingress/", env!("CARGO_PKG_VERSION")))
        .timeout(REQUEST_TIMEOUT)
        .use_rustls_tls();
    if let Some(pem) = trust_roots.filter(|pem| !pem.trim().is_empty()) {
        let roots = reqwest::Certificate::from_pem_bundle(pem.as_bytes())
            .context("acme.trustRoots holds no PEM certificate")?;
        if roots.is_empty() {
            bail!("acme.trustRoots holds no PEM certificate");
        }
        for root in roots {
            builder = builder.add_root_certificate(root);
        }
    }
    builder.build().context("building the ACME HTTP client")
}

/// The directory's endpoints.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Directory {
    pub new_nonce: String,
    pub new_account: String,
    pub new_order: String,
}

/// An ACME problem document, or what stood in for one.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Problem {
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub detail: String,
}

impl Problem {
    fn parse(status: reqwest::StatusCode, body: &[u8]) -> Problem {
        serde_json::from_slice(body).unwrap_or_else(|_| Problem {
            kind: String::new(),
            detail: format!(
                "HTTP {status}: {}",
                String::from_utf8_lossy(&body[..body.len().min(200)])
            ),
        })
    }
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = self
            .kind
            .strip_prefix("urn:ietf:params:acme:error:")
            .unwrap_or(&self.kind);
        match (self.detail.is_empty(), kind.is_empty()) {
            (false, false) => write!(f, "{} ({kind})", self.detail),
            (false, true) => write!(f, "{}", self.detail),
            (true, false) => write!(f, "{kind}"),
            (true, true) => write!(f, "the CA gave no reason"),
        }
    }
}

/// Where HTTP-01 answers are published, so whichever replica the CA
/// reaches can answer.
#[async_trait::async_trait]
pub trait ChallengeBoard: Send + Sync {
    /// Publish `key_authorization` for `token` before the CA is told to
    /// look.
    async fn put(&self, token: &str, key_authorization: &str) -> R<()>;
    /// Take it down once the order is done, either way.
    async fn remove(&self, token: &str);
}

/// A certificate and its key, as PEM.
#[derive(Debug, Clone)]
pub struct Issued {
    pub chain_pem: String,
    pub key_pem: String,
}

impl Issued {
    /// The bundle the ingress stores and serves: the chain, then the key.
    pub fn bundle(&self) -> String {
        let mut bundle = self.chain_pem.trim_end().to_string();
        bundle.push('\n');
        bundle.push_str(&self.key_pem);
        bundle
    }
}

/// One reply, read whole.
struct Reply {
    status: reqwest::StatusCode,
    location: Option<String>,
    retry_after: Option<Duration>,
    body: Bytes,
}

impl Reply {
    fn json<T: serde::de::DeserializeOwned>(&self, what: &str) -> R<T> {
        serde_json::from_slice(&self.body)
            .with_context(|| format!("the CA's {what} is not valid JSON"))
    }
}

#[derive(Debug, Deserialize)]
struct Order {
    status: String,
    #[serde(default)]
    authorizations: Vec<String>,
    #[serde(default)]
    finalize: String,
    #[serde(default)]
    certificate: Option<String>,
    #[serde(default)]
    error: Option<Problem>,
}

#[derive(Debug, Deserialize)]
struct Authorization {
    status: String,
    #[serde(default)]
    challenges: Vec<Challenge>,
}

#[derive(Debug, Deserialize)]
struct Challenge {
    #[serde(rename = "type")]
    kind: String,
    url: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    error: Option<Problem>,
}

/// A client bound to one directory and one account key.
pub struct AcmeClient {
    http: reqwest::Client,
    directory: Directory,
    key: EcdsaKeyPair,
    rng: SystemRandom,
    jwk: Value,
    thumbprint: String,
    /// The account URL (`kid`), once registered.
    kid: Option<String>,
    nonce: tokio::sync::Mutex<Option<String>>,
}

impl AcmeClient {
    /// A new P-256 account key, PKCS#8.
    pub fn generate_key() -> R<Vec<u8>> {
        let rng = SystemRandom::new();
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .map(|document| document.as_ref().to_vec())
            .map_err(|_| anyhow!("generating an ACME account key failed"))
    }

    /// Read the directory at `url` and bind `key_pkcs8` (from
    /// [`Self::generate_key`]).
    pub async fn connect(http: reqwest::Client, url: &str, key_pkcs8: &[u8]) -> R<AcmeClient> {
        let response = http
            .get(url)
            .send()
            .await
            .with_context(|| format!("reaching the ACME directory {url}"))?;
        let status = response.status();
        let body = response.bytes().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "the ACME directory {url} answered {}",
                Problem::parse(status, &body)
            );
        }
        let directory: Directory = serde_json::from_slice(&body)
            .with_context(|| format!("{url} is not an ACME directory"))?;
        Self::with_directory(http, directory, key_pkcs8)
    }

    fn with_directory(
        http: reqwest::Client,
        directory: Directory,
        key_pkcs8: &[u8],
    ) -> R<AcmeClient> {
        let rng = SystemRandom::new();
        let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, key_pkcs8, &rng)
            .map_err(|err| anyhow!("the ACME account key is not a P-256 PKCS#8 key: {err}"))?;
        let (jwk, thumbprint) = jwk_and_thumbprint(key.public_key().as_ref())?;
        Ok(AcmeClient {
            http,
            directory,
            key,
            rng,
            jwk,
            thumbprint,
            kid: None,
            nonce: tokio::sync::Mutex::new(None),
        })
    }

    /// Use account `url` (`kid`) without registering again.
    pub fn set_account(&mut self, url: String) {
        self.kid = Some(url);
    }

    pub fn account(&self) -> Option<&str> {
        self.kid.as_deref()
    }

    /// Register (or find) the account for this key, agreeing to the CA's
    /// terms, and return its URL.
    pub async fn register(&mut self, email: Option<&str>) -> R<String> {
        let mut payload = json!({ "termsOfServiceAgreed": true });
        if let Some(email) = email.map(str::trim).filter(|email| !email.is_empty()) {
            payload["contact"] = json!([format!("mailto:{email}")]);
        }
        self.kid = None;
        let url = self.directory.new_account.clone();
        let reply = self.post(&url, Some(&payload), false).await?;
        let kid = reply
            .location
            .context("the CA registered the account without saying where it is")?;
        self.kid = Some(kid.clone());
        Ok(kid)
    }

    /// `token.thumbprint`: the HTTP-01 answer for `token`.
    pub fn key_authorization(&self, token: &str) -> String {
        format!("{token}.{}", self.thumbprint)
    }

    /// Order a certificate for `host`, answering its HTTP-01 challenge on
    /// `board`, within [`ORDER_TIMEOUT`].
    pub async fn issue(&self, host: &str, board: &dyn ChallengeBoard) -> R<Issued> {
        let mut published = Vec::new();
        let outcome = tokio::time::timeout(ORDER_TIMEOUT, self.order(host, board, &mut published))
            .await
            .unwrap_or_else(|_| {
                Err(anyhow!(
                    "the order for {host} did not finish within {}s",
                    ORDER_TIMEOUT.as_secs()
                ))
            });
        for token in published {
            board.remove(&token).await;
        }
        outcome
    }

    async fn order(
        &self,
        host: &str,
        board: &dyn ChallengeBoard,
        published: &mut Vec<String>,
    ) -> R<Issued> {
        let new_order = self.directory.new_order.clone();
        let reply = self
            .post(
                &new_order,
                Some(&json!({ "identifiers": [{ "type": "dns", "value": host }] })),
                false,
            )
            .await?;
        let order_url = reply
            .location
            .clone()
            .context("the CA created the order without saying where it is")?;
        let order: Order = reply.json("order")?;
        for url in &order.authorizations {
            let authorization: Authorization =
                self.post(url, None, false).await?.json("authorization")?;
            if authorization.status == "valid" {
                continue;
            }
            let challenge = authorization
                .challenges
                .iter()
                .find(|challenge| challenge.kind == "http-01")
                .with_context(|| format!("the CA offers no http-01 challenge for {host}"))?;
            if !valid_token(&challenge.token) {
                bail!("the CA's http-01 token for {host} is not a base64url token");
            }
            board
                .put(&challenge.token, &self.key_authorization(&challenge.token))
                .await
                .context("publishing the http-01 answer")?;
            published.push(challenge.token.clone());
            self.post(&challenge.url, Some(&json!({})), false).await?;
            self.wait_for_authorization(url, host).await?;
        }
        let order = self.wait_for_order(&order_url, &["ready", "valid"]).await?;
        let key = rcgen::KeyPair::generate().context("generating the certificate key")?;
        let mut params = rcgen::CertificateParams::new(vec![host.to_string()])
            .with_context(|| format!("{host} is not a name a certificate can carry"))?;
        params.distinguished_name = rcgen::DistinguishedName::new();
        let csr = params
            .serialize_request(&key)
            .context("building the certificate request")?;
        if order.status == "ready" {
            self.post(
                &order.finalize,
                Some(&json!({ "csr": b64(csr.der()) })),
                false,
            )
            .await?;
        }
        let order = self.wait_for_order(&order_url, &["valid"]).await?;
        let certificate = order
            .certificate
            .context("the CA finished the order without a certificate")?;
        let chain = self.post(&certificate, None, true).await?;
        let chain_pem =
            String::from_utf8(chain.body.to_vec()).context("the certificate chain is not text")?;
        if !chain_pem.contains("-----BEGIN CERTIFICATE-----") {
            bail!("the CA's certificate download is not a PEM chain");
        }
        Ok(Issued {
            chain_pem,
            key_pem: key.serialize_pem(),
        })
    }

    async fn wait_for_authorization(&self, url: &str, host: &str) -> R<()> {
        let deadline = Instant::now() + ORDER_TIMEOUT;
        loop {
            let reply = self.post(url, None, false).await?;
            let authorization: Authorization = reply.json("authorization")?;
            match authorization.status.as_str() {
                "valid" => return Ok(()),
                "pending" | "processing" => {}
                status => {
                    let why = authorization
                        .challenges
                        .iter()
                        .find_map(|challenge| challenge.error.as_ref())
                        .map(ToString::to_string)
                        .unwrap_or_else(|| format!("the authorization is {status}"));
                    bail!("validating {host} failed: {why}");
                }
            }
            if Instant::now() >= deadline {
                bail!("validating {host} took too long");
            }
            tokio::time::sleep(poll_delay(reply.retry_after)).await;
        }
    }

    async fn wait_for_order(&self, url: &str, until: &[&str]) -> R<Order> {
        let deadline = Instant::now() + ORDER_TIMEOUT;
        loop {
            let reply = self.post(url, None, false).await?;
            let order: Order = reply.json("order")?;
            if until.contains(&order.status.as_str()) {
                return Ok(order);
            }
            if order.status == "invalid" {
                let why = order
                    .error
                    .map(|problem| problem.to_string())
                    .unwrap_or_else(|| "the order is invalid".into());
                bail!("{why}");
            }
            if Instant::now() >= deadline {
                bail!(
                    "the order is still {} after {}s",
                    order.status,
                    ORDER_TIMEOUT.as_secs()
                );
            }
            tokio::time::sleep(poll_delay(reply.retry_after)).await;
        }
    }

    /// A signed POST (a POST-as-GET when `payload` is `None`), retried on a
    /// `badNonce`.
    async fn post(&self, url: &str, payload: Option<&Value>, pem: bool) -> R<Reply> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let nonce = self.take_nonce().await?;
            let body = self.sign(url, &nonce, payload)?;
            let mut request = self
                .http
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/jose+json")
                .body(body);
            if pem {
                request =
                    request.header(reqwest::header::ACCEPT, "application/pem-certificate-chain");
            }
            let response = request
                .send()
                .await
                .with_context(|| format!("reaching the CA at {url}"))?;
            let reply = self.read(response).await?;
            if reply.status.is_success() {
                return Ok(reply);
            }
            let problem = Problem::parse(reply.status, &reply.body);
            if problem.kind.ends_with(":badNonce") && attempt < 3 {
                continue;
            }
            bail!("{problem}");
        }
    }

    async fn read(&self, response: reqwest::Response) -> R<Reply> {
        let headers = response.headers();
        if let Some(nonce) = headers
            .get("replay-nonce")
            .and_then(|value| value.to_str().ok())
        {
            *self.nonce.lock().await = Some(nonce.to_string());
        }
        let location = headers
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let retry_after = headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let status = response.status();
        let body = response.bytes().await.context("reading the CA's reply")?;
        Ok(Reply {
            status,
            location,
            retry_after,
            body,
        })
    }

    async fn take_nonce(&self) -> R<String> {
        if let Some(nonce) = self.nonce.lock().await.take() {
            return Ok(nonce);
        }
        let response = self
            .http
            .head(&self.directory.new_nonce)
            .send()
            .await
            .context("asking the CA for a nonce")?;
        response
            .headers()
            .get("replay-nonce")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .context("the CA's newNonce answered without a Replay-Nonce")
    }

    /// The flattened JWS for `url`.
    fn sign(&self, url: &str, nonce: &str, payload: Option<&Value>) -> R<String> {
        let mut protected = json!({ "alg": "ES256", "nonce": nonce, "url": url });
        match &self.kid {
            Some(kid) => protected["kid"] = json!(kid),
            None => protected["jwk"] = self.jwk.clone(),
        }
        let protected = b64(protected.to_string());
        let payload = payload
            .map(|payload| b64(payload.to_string()))
            .unwrap_or_default();
        let signature = self
            .key
            .sign(&self.rng, format!("{protected}.{payload}").as_bytes())
            .map_err(|_| anyhow!("signing an ACME request failed"))?;
        Ok(json!({
            "protected": protected,
            "payload": payload,
            "signature": b64(signature.as_ref()),
        })
        .to_string())
    }
}

/// The JWK of an uncompressed P-256 public key, and its RFC 7638
/// thumbprint.
fn jwk_and_thumbprint(public: &[u8]) -> R<(Value, String)> {
    if public.len() != 65 || public[0] != 0x04 {
        bail!("the ACME account key's public half is not an uncompressed P-256 point");
    }
    let (x, y) = (b64(&public[1..33]), b64(&public[33..65]));
    // Members in lexicographic order, no whitespace (RFC 7638 §3).
    let canonical = format!(r#"{{"crv":"P-256","kty":"EC","x":"{x}","y":"{y}"}}"#);
    let digest = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes());
    Ok((
        json!({ "crv": "P-256", "kty": "EC", "x": x, "y": y }),
        b64(digest.as_ref()),
    ))
}

/// An HTTP-01 token as RFC 8555 §8.3 shapes it: base64url, nothing else.
pub fn valid_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 256
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn poll_delay(retry_after: Option<Duration>) -> Duration {
    retry_after
        .unwrap_or(Duration::from_secs(1))
        .clamp(Duration::from_millis(500), Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> AcmeClient {
        let key = AcmeClient::generate_key().unwrap();
        AcmeClient::with_directory(
            reqwest::Client::new(),
            Directory {
                new_nonce: "https://ca.test/nonce".into(),
                new_account: "https://ca.test/account".into(),
                new_order: "https://ca.test/order".into(),
            },
            &key,
        )
        .unwrap()
    }

    #[test]
    fn directory_aliases_resolve() {
        assert_eq!(directory_url(None).unwrap(), LETS_ENCRYPT);
        assert_eq!(directory_url(Some("letsencrypt")).unwrap(), LETS_ENCRYPT);
        assert_eq!(
            directory_url(Some("letsencrypt-staging")).unwrap(),
            LETS_ENCRYPT_STAGING
        );
        assert_eq!(
            directory_url(Some("https://ca.internal/dir")).unwrap(),
            "https://ca.internal/dir"
        );
        assert!(directory_url(Some("http://plain")).is_err());
    }

    /// The JWS verifies against the JWK it carries, and its header is what
    /// RFC 8555 §6.2 asks for: `jwk` before an account exists, `kid` after.
    #[test]
    fn requests_are_signed_es256_jws() {
        let mut client = client();
        let jws: Value = serde_json::from_str(
            &client
                .sign("https://ca.test/order", "n1", Some(&json!({ "a": 1 })))
                .unwrap(),
        )
        .unwrap();
        let decode = |field: &str| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(jws[field].as_str().unwrap())
                .unwrap()
        };
        let protected: Value = serde_json::from_slice(&decode("protected")).unwrap();
        assert_eq!(protected["alg"], "ES256");
        assert_eq!(protected["nonce"], "n1");
        assert_eq!(protected["url"], "https://ca.test/order");
        assert_eq!(protected["jwk"]["kty"], "EC");
        assert!(protected.get("kid").is_none());
        assert_eq!(
            serde_json::from_slice::<Value>(&decode("payload")).unwrap(),
            json!({ "a": 1 })
        );
        let message = format!(
            "{}.{}",
            jws["protected"].as_str().unwrap(),
            jws["payload"].as_str().unwrap()
        );
        let public = ring::signature::UnparsedPublicKey::new(
            &ring::signature::ECDSA_P256_SHA256_FIXED,
            client.key.public_key().as_ref().to_vec(),
        );
        public
            .verify(message.as_bytes(), &decode("signature"))
            .expect("the signature verifies");

        client.set_account("https://ca.test/acct/1".into());
        let jws: Value =
            serde_json::from_str(&client.sign("https://ca.test/x", "n2", None).unwrap()).unwrap();
        let protected: Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(jws["protected"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(protected["kid"], "https://ca.test/acct/1");
        assert!(protected.get("jwk").is_none());
        assert_eq!(jws["payload"], "", "POST-as-GET has an empty payload");
    }

    /// The thumbprint is SHA-256 over the JWK's required members in order,
    /// and the key authorization is `token.thumbprint`.
    #[test]
    fn thumbprints_and_key_authorizations_follow_rfc_7638() {
        let client = client();
        let canonical = format!(
            r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
            client.jwk["x"].as_str().unwrap(),
            client.jwk["y"].as_str().unwrap()
        );
        let digest = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes());
        assert_eq!(client.thumbprint, b64(digest.as_ref()));
        assert_eq!(client.thumbprint.len(), 43);
        assert_eq!(
            client.key_authorization("tok-1"),
            format!("tok-1.{}", client.thumbprint)
        );
        assert!(jwk_and_thumbprint(&[0u8; 10]).is_err());
    }

    #[test]
    fn tokens_and_problems_read_as_rfc_8555_shapes_them() {
        assert!(valid_token("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ-PCt92wr-oA"));
        assert!(!valid_token("../etc/passwd"));
        assert!(!valid_token(""));
        let problem = Problem::parse(
            reqwest::StatusCode::FORBIDDEN,
            br#"{"type":"urn:ietf:params:acme:error:unauthorized","detail":"no such host"}"#,
        );
        assert_eq!(problem.to_string(), "no such host (unauthorized)");
        let raw = Problem::parse(reqwest::StatusCode::BAD_GATEWAY, b"upstream down");
        assert!(raw.to_string().contains("upstream down"));
        assert_eq!(
            poll_delay(Some(Duration::from_secs(60))),
            Duration::from_secs(10)
        );
        assert_eq!(poll_delay(None), Duration::from_secs(1));
    }

    #[test]
    fn a_bundle_is_the_chain_then_the_key() {
        let issued = Issued {
            chain_pem: "-----BEGIN CERTIFICATE-----\nA\n-----END CERTIFICATE-----\n\n".into(),
            key_pem: "-----BEGIN PRIVATE KEY-----\nB\n-----END PRIVATE KEY-----\n".into(),
        };
        let bundle = issued.bundle();
        assert!(bundle.starts_with("-----BEGIN CERTIFICATE"));
        assert!(bundle.ends_with("-----END PRIVATE KEY-----\n"));
        assert!(!bundle.contains("\n\n"));
    }
}
