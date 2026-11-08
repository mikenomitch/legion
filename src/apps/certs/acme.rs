//! An ACME client (RFC 8555): enough of the protocol to get a certificate
//! from Let's Encrypt or any other ACME CA.
//!
//! - **Account keys** are ECDSA P-256, signing ES256 JWS with `ring`. One
//!   per Issuer, stored PKCS#8 in the app's own KV; its JWK thumbprint
//!   (RFC 7638) is public and is what HTTP-01 responders answer with.
//! - **Orders** cover every name of a Certificate at once. Each
//!   authorization is proven with `dns-01` (a TXT record through a
//!   [`DnsProvider`]) when the Issuer has DNS-01, and `http-01` otherwise,
//!   answered by a stateless responder (`legion cert http01`) somewhere on
//!   the name's port 80: the key authorization is `<token>.<thumbprint>`,
//!   so a responder needs no state from this client.
//! - **Nonces** come from each response's `Replay-Nonce`; a `badNonce`
//!   error is retried with a fresh one, as RFC 8555 §6.5 expects.
//! - **External account binding** (RFC 8555 §7.3.4) for CAs that need one.

use super::dns01::DnsProvider;
use crate::prelude::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, KeyPair as _, ECDSA_P256_SHA256_FIXED_SIGNING};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::time::Instant;

/// The content type of every ACME POST.
const JOSE_JSON: &str = "application/jose+json";
/// How many times a `badNonce` is retried before giving up.
const BAD_NONCE_RETRIES: usize = 3;
/// How long to wait between polls when the CA sends no `Retry-After`.
const POLL_DEFAULT: Duration = Duration::from_secs(2);
/// The longest `Retry-After` honored while polling.
const POLL_MAX: Duration = Duration::from_secs(30);

/// An account key: ECDSA P-256, for ES256.
pub struct AccountKey {
    key: EcdsaKeyPair,
    pkcs8_pem: String,
    jwk: Value,
    thumbprint: String,
}

impl std::fmt::Debug for AccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountKey")
            .field("thumbprint", &self.thumbprint)
            .finish()
    }
}

impl AccountKey {
    /// A new key.
    pub fn generate() -> R<AccountKey> {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .map_err(|_| anyhow!("generating an ACME account key failed"))?;
        let pem = pkcs8_pem(pkcs8.as_ref());
        AccountKey::from_pem(&pem)
    }

    /// A key stored as PKCS#8 PEM (what [`Self::to_pem`] writes).
    pub fn from_pem(pem: &str) -> R<AccountKey> {
        let der = crate::worker::netd::tls::key_from_pem(pem.as_bytes())
            .context("the ACME account key")?;
        let rng = SystemRandom::new();
        let key =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, der.secret_der(), &rng)
                .map_err(|err| anyhow!("the ACME account key is not ECDSA P-256 PKCS#8: {err}"))?;
        let point = key.public_key().as_ref();
        if point.len() != 65 || point[0] != 4 {
            bail!("the ACME account key's public point is not uncompressed P-256");
        }
        let x = B64.encode(&point[1..33]);
        let y = B64.encode(&point[33..65]);
        // RFC 7638: the required members, lexicographic, no whitespace.
        let canonical = format!(r#"{{"crv":"P-256","kty":"EC","x":"{x}","y":"{y}"}}"#);
        let thumbprint = B64.encode(Sha256::digest(canonical.as_bytes()));
        Ok(AccountKey {
            key,
            pkcs8_pem: pem.to_string(),
            jwk: json!({ "crv": "P-256", "kty": "EC", "x": x, "y": y }),
            thumbprint,
        })
    }

    /// The key as PKCS#8 PEM, for KV.
    pub fn to_pem(&self) -> &str {
        &self.pkcs8_pem
    }

    /// The JWK thumbprint (RFC 7638), base64url: public, and the second half
    /// of every key authorization.
    pub fn thumbprint(&self) -> &str {
        &self.thumbprint
    }

    /// The public key as a JWK.
    pub fn jwk(&self) -> &Value {
        &self.jwk
    }

    fn sign(&self, message: &[u8]) -> R<Vec<u8>> {
        let rng = SystemRandom::new();
        Ok(self
            .key
            .sign(&rng, message)
            .map_err(|_| anyhow!("signing with the ACME account key failed"))?
            .as_ref()
            .to_vec())
    }
}

/// PKCS#8 DER as PEM.
fn pkcs8_pem(der: &[u8]) -> String {
    use base64::engine::general_purpose::STANDARD;
    let body = STANDARD.encode(der);
    let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
    for line in body.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(line).expect("base64 is ascii"));
        pem.push('\n');
    }
    pem.push_str("-----END PRIVATE KEY-----\n");
    pem
}

/// `<token>.<thumbprint>`: what a challenge proves (RFC 8555 §8.1).
pub fn key_authorization(token: &str, thumbprint: &str) -> String {
    format!("{token}.{thumbprint}")
}

/// The TXT record value for a DNS-01 challenge: base64url SHA-256 of the
/// key authorization (RFC 8555 §8.4).
pub fn dns01_value(key_authorization: &str) -> String {
    B64.encode(Sha256::digest(key_authorization.as_bytes()))
}

/// The record name a DNS-01 challenge for `domain` is answered at.
pub fn dns01_name(domain: &str) -> String {
    format!(
        "_acme-challenge.{}.",
        domain.trim_start_matches("*.").trim_end_matches('.')
    )
}

/// An RFC 7807 problem document an ACME server answers errors with.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Problem {
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub subproblems: Vec<Problem>,
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = self
            .kind
            .strip_prefix("urn:ietf:params:acme:error:")
            .unwrap_or(&self.kind);
        write!(f, "{kind}: {}", self.detail)?;
        for sub in &self.subproblems {
            write!(f, "; {}", sub.detail)?;
        }
        Ok(())
    }
}

/// The directory (RFC 8555 §7.1.1).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Directory {
    pub new_nonce: String,
    pub new_account: String,
    pub new_order: String,
    #[serde(default)]
    pub meta: Option<DirectoryMeta>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DirectoryMeta {
    #[serde(default)]
    pub external_account_required: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    status: String,
    #[serde(default)]
    authorizations: Vec<String>,
    finalize: String,
    #[serde(default)]
    certificate: Option<String>,
    #[serde(default)]
    error: Option<Problem>,
}

#[derive(Debug, Clone, Deserialize)]
struct Identifier {
    #[serde(rename = "type")]
    kind: String,
    value: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Authorization {
    identifier: Identifier,
    status: String,
    #[serde(default)]
    challenges: Vec<Challenge>,
    #[serde(default)]
    wildcard: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct Challenge {
    #[serde(rename = "type")]
    kind: String,
    url: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    error: Option<Problem>,
}

/// How an order's names are proven.
#[derive(Clone)]
pub struct Proof {
    /// DNS-01 through this provider, waiting this long after publishing.
    pub dns01: Option<(Arc<dyn DnsProvider>, Duration)>,
    /// HTTP-01 through responders that answer with the thumbprint.
    pub http01: bool,
}

/// A response: status, headers we use, and the body.
struct Response {
    location: Option<String>,
    retry_after: Option<Duration>,
    body: Vec<u8>,
}

impl Response {
    fn json<T: serde::de::DeserializeOwned>(&self, what: &str) -> R<T> {
        serde_json::from_slice(&self.body).with_context(|| {
            format!(
                "the ACME server's {what} did not parse: {}",
                String::from_utf8_lossy(&self.body[..self.body.len().min(300)])
            )
        })
    }
}

/// A client for one ACME server and one account key.
pub struct AcmeClient {
    http: reqwest::Client,
    directory: Directory,
    key: Arc<AccountKey>,
    account_url: tokio::sync::Mutex<Option<String>>,
    nonce: tokio::sync::Mutex<Option<String>>,
}

impl std::fmt::Debug for AcmeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcmeClient")
            .field("new_account", &self.directory.new_account)
            .field("key", &self.key)
            .finish()
    }
}

impl AcmeClient {
    /// Read the directory at `directory_url`.
    pub async fn connect(
        http: reqwest::Client,
        directory_url: &str,
        key: Arc<AccountKey>,
    ) -> R<AcmeClient> {
        let response = http
            .get(directory_url)
            .send()
            .await
            .with_context(|| format!("reading the ACME directory {directory_url}"))?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            bail!("the ACME directory {directory_url} answered {status}");
        }
        let directory: Directory = serde_json::from_slice(&body)
            .with_context(|| format!("the ACME directory {directory_url} did not parse"))?;
        Ok(AcmeClient {
            http,
            directory,
            key,
            account_url: tokio::sync::Mutex::new(None),
            nonce: tokio::sync::Mutex::new(None),
        })
    }

    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    pub fn thumbprint(&self) -> &str {
        self.key.thumbprint()
    }

    async fn fresh_nonce(&self) -> R<String> {
        if let Some(nonce) = self.nonce.lock().await.take() {
            return Ok(nonce);
        }
        let response = self
            .http
            .head(&self.directory.new_nonce)
            .send()
            .await
            .context("asking the ACME server for a nonce")?;
        response
            .headers()
            .get("replay-nonce")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("the ACME server's newNonce sent no Replay-Nonce"))
    }

    /// The JWS for `payload` (`None`: POST-as-GET) to `url`.
    async fn jws(&self, url: &str, payload: Option<&Value>, use_jwk: bool) -> R<Value> {
        let nonce = self.fresh_nonce().await?;
        let mut protected = json!({ "alg": "ES256", "nonce": nonce, "url": url });
        let account = self.account_url.lock().await.clone();
        match (use_jwk, account) {
            (false, Some(kid)) => protected["kid"] = json!(kid),
            (true, _) => protected["jwk"] = self.key.jwk().clone(),
            (false, None) => bail!("this ACME client has no account yet"),
        }
        let protected = B64.encode(protected.to_string());
        let payload = match payload {
            Some(payload) => B64.encode(payload.to_string()),
            None => String::new(),
        };
        let signature = self.key.sign(format!("{protected}.{payload}").as_bytes())?;
        Ok(json!({
            "protected": protected,
            "payload": payload,
            "signature": B64.encode(signature),
        }))
    }

    /// POST a signed request, retrying `badNonce`, and fail on a problem.
    async fn post(
        &self,
        url: &str,
        payload: Option<&Value>,
        use_jwk: bool,
        accept: Option<&str>,
    ) -> R<Response> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let body = self.jws(url, payload, use_jwk).await?;
            let mut request = self
                .http
                .post(url)
                .header("content-type", JOSE_JSON)
                .body(body.to_string());
            if let Some(accept) = accept {
                request = request.header("accept", accept);
            }
            let response = request
                .send()
                .await
                .with_context(|| format!("calling the ACME server at {url}"))?;
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            if let Some(nonce) = headers.get("replay-nonce").and_then(|v| v.to_str().ok()) {
                *self.nonce.lock().await = Some(nonce.to_string());
            }
            let body = response.bytes().await?.to_vec();
            let response = Response {
                location: headers
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string),
                retry_after: headers
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map(Duration::from_secs),
                body,
            };
            if status < 400 {
                return Ok(response);
            }
            let problem: Problem = serde_json::from_slice(&response.body).unwrap_or(Problem {
                kind: String::new(),
                detail: format!(
                    "HTTP {status}: {}",
                    String::from_utf8_lossy(&response.body[..response.body.len().min(300)])
                ),
                status: Some(status),
                subproblems: vec![],
            });
            if problem.kind.ends_with(":badNonce") && attempt <= BAD_NONCE_RETRIES {
                debug!(url, "ACME badNonce; retrying with a fresh nonce");
                continue;
            }
            bail!("the ACME server refused {url}: {problem}");
        }
    }

    /// Find or create the account for this key (RFC 8555 §7.3), agreeing
    /// to the CA's terms. Returns the account URL.
    pub async fn register(&self, email: Option<&str>, eab: Option<(&str, &[u8])>) -> R<String> {
        let mut payload = json!({ "termsOfServiceAgreed": true });
        if let Some(email) = email {
            payload["contact"] = json!([format!("mailto:{email}")]);
        }
        let external_required = self
            .directory
            .meta
            .as_ref()
            .is_some_and(|meta| meta.external_account_required);
        match eab {
            Some((kid, hmac_key)) => {
                payload["externalAccountBinding"] =
                    eab_jws(kid, hmac_key, &self.directory.new_account, self.key.jwk())?;
            }
            None if external_required => bail!(
                "this ACME server requires external account binding: set acme.eab = {{ kid, \
                 hmacKey = {{ kv }} }} with the credentials it gave you"
            ),
            None => {}
        }
        let response = self
            .post(&self.directory.new_account, Some(&payload), true, None)
            .await?;
        let url = response
            .location
            .ok_or_else(|| anyhow!("the ACME server's newAccount sent no account URL"))?;
        *self.account_url.lock().await = Some(url.clone());
        Ok(url)
    }

    /// Use an account URL learned before (skips the newAccount call).
    pub async fn set_account_url(&self, url: String) {
        *self.account_url.lock().await = Some(url);
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str, what: &str) -> R<(T, Response)> {
        let response = self.post(url, None, false, None).await?;
        Ok((response.json(what)?, response))
    }

    /// Order a certificate for `names` (DNS names and IP addresses), prove
    /// each, finalize with `csr_der`, and return the chain as PEM. DNS
    /// records published for the order are removed before it returns,
    /// whatever happened. Gives up at `deadline`.
    pub async fn issue(
        &self,
        names: &[String],
        csr_der: &[u8],
        proof: &Proof,
        timeout: Duration,
    ) -> R<String> {
        let deadline = Instant::now() + timeout;
        let identifiers: Vec<Value> = names
            .iter()
            .map(|name| {
                if name.parse::<std::net::IpAddr>().is_ok() {
                    json!({ "type": "ip", "value": name })
                } else {
                    json!({ "type": "dns", "value": name })
                }
            })
            .collect();
        let response = self
            .post(
                &self.directory.new_order,
                Some(&json!({ "identifiers": identifiers })),
                false,
                None,
            )
            .await?;
        let order_url = response
            .location
            .clone()
            .ok_or_else(|| anyhow!("the ACME server's newOrder sent no order URL"))?;
        let order: Order = response.json("order")?;
        if order.status == "invalid" {
            bail!(
                "the ACME server refused the order: {}",
                order.error.map(|p| p.to_string()).unwrap_or_default()
            );
        }

        let mut published: Vec<(Arc<dyn DnsProvider>, String, String)> = Vec::new();
        let outcome = self
            .prove_and_finalize(&order, &order_url, csr_der, proof, deadline, &mut published)
            .await;
        for (provider, fqdn, value) in published {
            if let Err(err) = provider.cleanup(&fqdn, &value).await {
                warn!(fqdn, error = %format!("{err:#}"), "Could not remove an ACME DNS-01 record");
            }
        }
        outcome
    }

    async fn prove_and_finalize(
        &self,
        order: &Order,
        order_url: &str,
        csr_der: &[u8],
        proof: &Proof,
        deadline: Instant,
        published: &mut Vec<(Arc<dyn DnsProvider>, String, String)>,
    ) -> R<String> {
        // Choose and publish every challenge first, so one propagation
        // wait covers them all.
        let mut pending: Vec<(String, String, String)> = Vec::new(); // (authz url, challenge url, name)
        for authz_url in &order.authorizations {
            let (authz, _) = self
                .get::<Authorization>(authz_url, "authorization")
                .await?;
            if authz.status == "valid" {
                continue;
            }
            let name = if authz.wildcard {
                format!("*.{}", authz.identifier.value)
            } else {
                authz.identifier.value.clone()
            };
            let find = |kind: &str| authz.challenges.iter().find(|c| c.kind == kind);
            let dns_ok = authz.identifier.kind == "dns";
            let chosen = match (&proof.dns01, find("dns-01"), find("http-01")) {
                (Some((provider, _)), Some(challenge), _) if dns_ok => {
                    let value =
                        dns01_value(&key_authorization(&challenge.token, self.key.thumbprint()));
                    let fqdn = dns01_name(&authz.identifier.value);
                    provider
                        .present(&fqdn, &value)
                        .await
                        .with_context(|| format!("publishing the DNS-01 record {fqdn}"))?;
                    published.push((provider.clone(), fqdn, value));
                    challenge
                }
                (_, _, Some(challenge)) if proof.http01 && !authz.wildcard => challenge,
                _ => {
                    let offered: Vec<&str> =
                        authz.challenges.iter().map(|c| c.kind.as_str()).collect();
                    bail!(
                        "{name}: no challenge this Issuer can answer (the CA offers {}; the \
                         Issuer has {}{})",
                        offered.join(", "),
                        if proof.dns01.is_some() { "dns01" } else { "" },
                        if proof.http01 { " http01" } else { "" }
                    );
                }
            };
            pending.push((authz_url.clone(), chosen.url.clone(), name));
        }
        if let Some((_, delay)) = &proof.dns01 {
            if !published.is_empty() && !delay.is_zero() {
                tokio::time::sleep(*delay).await;
            }
        }
        for (_, challenge_url, _) in &pending {
            self.post(challenge_url, Some(&json!({})), false, None)
                .await?;
        }
        for (authz_url, _, name) in &pending {
            loop {
                let (authz, response) = self
                    .get::<Authorization>(authz_url, "authorization")
                    .await?;
                match authz.status.as_str() {
                    "valid" => break,
                    "pending" | "processing" => {}
                    other => {
                        let detail = authz
                            .challenges
                            .iter()
                            .filter_map(|c| c.error.as_ref())
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join("; ");
                        bail!("{name}: the CA could not verify control ({other}): {detail}");
                    }
                }
                wait(response.retry_after, deadline, name).await?;
            }
        }

        // Finalize, and wait for the certificate.
        let response = self
            .post(
                &order.finalize,
                Some(&json!({ "csr": B64.encode(csr_der) })),
                false,
                None,
            )
            .await?;
        let mut current: Order = response.json("order")?;
        let mut retry = response.retry_after;
        loop {
            match current.status.as_str() {
                "valid" => break,
                "invalid" => bail!(
                    "the CA refused to issue: {}",
                    current.error.map(|p| p.to_string()).unwrap_or_default()
                ),
                _ => {}
            }
            wait(retry, deadline, "the order").await?;
            let (next, response) = self.get::<Order>(order_url, "order").await?;
            current = next;
            retry = response.retry_after;
        }
        let certificate_url = current
            .certificate
            .ok_or_else(|| anyhow!("the CA's valid order has no certificate URL"))?;
        let response = self
            .post(
                &certificate_url,
                None,
                false,
                Some("application/pem-certificate-chain"),
            )
            .await?;
        String::from_utf8(response.body).context("the certificate chain is not text")
    }
}

/// Sleep before the next poll, or fail at the deadline.
async fn wait(retry_after: Option<Duration>, deadline: Instant, what: &str) -> R<()> {
    let pause = retry_after.unwrap_or(POLL_DEFAULT).min(POLL_MAX);
    if Instant::now() + pause > deadline {
        bail!("{what}: the CA did not finish in time (still pending)");
    }
    tokio::time::sleep(pause).await;
    Ok(())
}

/// The `externalAccountBinding` JWS: the account's JWK, signed HS256 with
/// the CA's MAC key under its key id.
pub fn eab_jws(kid: &str, hmac_key: &[u8], new_account_url: &str, jwk: &Value) -> R<Value> {
    let protected =
        B64.encode(json!({ "alg": "HS256", "kid": kid, "url": new_account_url }).to_string());
    let payload = B64.encode(jwk.to_string());
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, hmac_key);
    let signature = ring::hmac::sign(&key, format!("{protected}.{payload}").as_bytes());
    Ok(json!({
        "protected": protected,
        "payload": payload,
        "signature": B64.encode(signature.as_ref()),
    }))
}

/// Decode an EAB MAC key as CAs hand it out (base64url, sometimes padded or
/// in standard base64).
pub fn decode_hmac_key(text: &str) -> R<Vec<u8>> {
    let text = text.trim().trim_end_matches('=');
    B64.decode(text)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(text))
        .map_err(|_| anyhow!("the EAB hmacKey is not base64url"))
}

/// Verify an ES256 JWS signature against a JWK (the server's side; used by
/// the test server and the unit tests).
pub fn verify_es256(jwk: &Value, signing_input: &[u8], signature: &[u8]) -> bool {
    let (Some(x), Some(y)) = (
        jwk["x"].as_str().and_then(|x| B64.decode(x).ok()),
        jwk["y"].as_str().and_then(|y| B64.decode(y).ok()),
    ) else {
        return false;
    };
    let mut point = vec![4u8];
    point.extend(x);
    point.extend(y);
    ring::signature::UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_FIXED, point)
        .verify(signing_input, signature)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_keys_round_trip_and_sign_verifiably_test() -> R<()> {
        let key = AccountKey::generate()?;
        let again = AccountKey::from_pem(key.to_pem())?;
        assert_eq!(key.thumbprint(), again.thumbprint());
        assert_eq!(key.jwk(), again.jwk());
        let signature = key.sign(b"hello")?;
        assert!(verify_es256(key.jwk(), b"hello", &signature));
        assert!(!verify_es256(key.jwk(), b"hellO", &signature));
        // 43 characters: base64url of a SHA-256.
        assert_eq!(key.thumbprint().len(), 43);
        Ok(())
    }

    /// RFC 7638 §3.1 computes a thumbprint over an RSA key; the same rules
    /// (required members, sorted, no whitespace) give this for a P-256 key.
    #[test]
    fn the_thumbprint_is_over_the_canonical_jwk_test() -> R<()> {
        let key = AccountKey::generate()?;
        let jwk = key.jwk();
        let canonical = format!(
            r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
            jwk["x"].as_str().unwrap(),
            jwk["y"].as_str().unwrap()
        );
        assert_eq!(
            key.thumbprint(),
            B64.encode(Sha256::digest(canonical.as_bytes()))
        );
        Ok(())
    }

    #[test]
    fn challenge_values_follow_rfc_8555_test() {
        // RFC 8555 §8.4's example key authorization digest shape.
        let auth = key_authorization("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ-PCt92wr-oA", "thumb");
        assert_eq!(auth, "evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ-PCt92wr-oA.thumb");
        assert_eq!(dns01_value(&auth).len(), 43);
        assert_eq!(dns01_name("*.example.com"), "_acme-challenge.example.com.");
        assert_eq!(
            dns01_name("www.example.com."),
            "_acme-challenge.www.example.com."
        );
    }

    #[test]
    fn eab_binds_the_account_key_with_the_mac_test() -> R<()> {
        let key = AccountKey::generate()?;
        let mac = decode_hmac_key("c2VjcmV0LWtleS1mb3ItdGVzdHM")?;
        let jws = eab_jws("kid-1", &mac, "https://ca/new-account", key.jwk())?;
        let protected: Value =
            serde_json::from_slice(&B64.decode(jws["protected"].as_str().unwrap())?)?;
        assert_eq!(protected["alg"], "HS256");
        assert_eq!(protected["kid"], "kid-1");
        let payload: Value =
            serde_json::from_slice(&B64.decode(jws["payload"].as_str().unwrap())?)?;
        assert_eq!(&payload, key.jwk());
        let input = format!(
            "{}.{}",
            jws["protected"].as_str().unwrap(),
            jws["payload"].as_str().unwrap()
        );
        let mac_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &mac);
        ring::hmac::verify(
            &mac_key,
            input.as_bytes(),
            &B64.decode(jws["signature"].as_str().unwrap())?,
        )
        .map_err(|_| anyhow!("bad mac"))?;
        // Padded and standard base64 decode too.
        assert_eq!(decode_hmac_key("c2VjcmV0LWtleS1mb3ItdGVzdHM=")?, mac);
        Ok(())
    }

    #[test]
    fn problems_read_well_test() {
        let problem: Problem = serde_json::from_value(json!({
            "type": "urn:ietf:params:acme:error:rejectedIdentifier",
            "detail": "Error creating new order",
            "subproblems": [{ "type": "x", "detail": "\"a.invalid\" is not a valid domain" }],
        }))
        .unwrap();
        assert_eq!(
            problem.to_string(),
            "rejectedIdentifier: Error creating new order; \"a.invalid\" is not a valid domain"
        );
    }
}
