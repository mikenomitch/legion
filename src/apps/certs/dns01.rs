//! DNS-01 providers: publish and remove the `_acme-challenge` TXT record
//! an ACME CA looks up.
//!
//! - **Cloudflare**: the zone is found by walking up the name
//!   (`_acme-challenge.www.example.com` tries `www.example.com`, then
//!   `example.com`) with a token that may edit DNS in it.
//! - **Webhook**: lego's `httpreq` shape, so an existing bridge to any DNS
//!   API works unchanged: `POST <url>/present` and `POST <url>/cleanup`,
//!   each with `{ "fqdn": "_acme-challenge.example.com.", "value": "..." }`,
//!   and an optional bearer token.

use crate::prelude::*;

/// Cloudflare's API.
pub const CLOUDFLARE_API: &str = "https://api.cloudflare.com/client/v4";

/// Publishes and removes TXT records.
#[async_trait::async_trait]
pub trait DnsProvider: Send + Sync {
    /// Publish `value` as a TXT record at `fqdn` (with a trailing dot).
    async fn present(&self, fqdn: &str, value: &str) -> R<()>;
    /// Remove that record. Removing one that is not there is not an error.
    async fn cleanup(&self, fqdn: &str, value: &str) -> R<()>;
}

async fn read_json(response: reqwest::Response, what: &str) -> R<(u16, Value)> {
    let status = response.status().as_u16();
    let body = response.bytes().await?;
    let value = serde_json::from_slice(&body).unwrap_or_else(
        |_| json!({ "raw": String::from_utf8_lossy(&body[..body.len().min(300)]) }),
    );
    if !(200..300).contains(&status) {
        let detail = value["errors"]
            .as_array()
            .map(|errors| {
                errors
                    .iter()
                    .map(|e| {
                        format!(
                            "{} {}",
                            e["code"].as_i64().unwrap_or(0),
                            e["message"].as_str().unwrap_or("")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .filter(|detail| !detail.is_empty())
            .unwrap_or_else(|| value.to_string());
        bail!("{what} answered {status}: {detail}");
    }
    Ok((status, value))
}

/// Cloudflare DNS, with an API token.
pub struct Cloudflare {
    http: reqwest::Client,
    api: String,
    token: String,
}

impl Cloudflare {
    pub fn new(http: reqwest::Client, api: &str, token: &str) -> Cloudflare {
        Cloudflare {
            http,
            api: api.trim_end_matches('/').to_string(),
            token: token.trim().to_string(),
        }
    }

    /// The id of the most specific zone that holds `fqdn`.
    async fn zone_id(&self, fqdn: &str) -> R<String> {
        let name = fqdn.trim_end_matches('.');
        let labels: Vec<&str> = name.split('.').collect();
        // Never ask about the bare TLD; start one below the record.
        for start in 1..labels.len().saturating_sub(1) {
            let candidate = labels[start..].join(".");
            let response = self
                .http
                .get(format!("{}/zones", self.api))
                .query(&[("name", candidate.as_str())])
                .bearer_auth(&self.token)
                .send()
                .await
                .context("calling Cloudflare")?;
            let (_, body) = read_json(response, "cloudflare: listing zones").await?;
            if let Some(id) = body["result"]
                .as_array()
                .and_then(|zones| zones.first())
                .and_then(|zone| zone["id"].as_str())
            {
                return Ok(id.to_string());
            }
        }
        bail!(
            "cloudflare: no zone this token can see holds {name} (the token needs Zone:Read and \
             DNS:Edit on the zone)"
        )
    }
}

#[async_trait::async_trait]
impl DnsProvider for Cloudflare {
    async fn present(&self, fqdn: &str, value: &str) -> R<()> {
        let zone = self.zone_id(fqdn).await?;
        let response = self
            .http
            .post(format!("{}/zones/{zone}/dns_records", self.api))
            .bearer_auth(&self.token)
            .json_body(&json!({
                "type": "TXT",
                "name": fqdn.trim_end_matches('.'),
                "content": value,
                "ttl": 60,
            }))
            .send()
            .await
            .context("calling Cloudflare")?;
        read_json(response, "cloudflare: creating the TXT record").await?;
        Ok(())
    }

    async fn cleanup(&self, fqdn: &str, value: &str) -> R<()> {
        let zone = self.zone_id(fqdn).await?;
        let name = fqdn.trim_end_matches('.');
        let response = self
            .http
            .get(format!("{}/zones/{zone}/dns_records", self.api))
            .query(&[("type", "TXT"), ("name", name)])
            .bearer_auth(&self.token)
            .send()
            .await
            .context("calling Cloudflare")?;
        let (_, body) = read_json(response, "cloudflare: listing TXT records").await?;
        for record in body["result"].as_array().cloned().unwrap_or_default() {
            let content = record["content"].as_str().unwrap_or("").trim_matches('"');
            if content != value {
                continue;
            }
            let Some(id) = record["id"].as_str() else {
                continue;
            };
            let response = self
                .http
                .delete(format!("{}/zones/{zone}/dns_records/{id}", self.api))
                .bearer_auth(&self.token)
                .send()
                .await
                .context("calling Cloudflare")?;
            read_json(response, "cloudflare: deleting the TXT record").await?;
        }
        Ok(())
    }
}

/// A webhook in lego's `httpreq` shape.
pub struct Webhook {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
}

impl Webhook {
    pub fn new(http: reqwest::Client, url: &str, token: Option<String>) -> Webhook {
        Webhook {
            http,
            url: url.trim_end_matches('/').to_string(),
            token: token.map(|token| token.trim().to_string()),
        }
    }

    async fn call(&self, op: &str, fqdn: &str, value: &str) -> R<()> {
        let mut request = self
            .http
            .post(format!("{}/{op}", self.url))
            .json_body(&json!({ "fqdn": fqdn, "value": value }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("calling the DNS webhook {}/{op}", self.url))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "the DNS webhook {}/{op} answered {status}: {}",
                self.url,
                body.chars().take(300).collect::<String>()
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl DnsProvider for Webhook {
    async fn present(&self, fqdn: &str, value: &str) -> R<()> {
        self.call("present", fqdn, value).await
    }

    async fn cleanup(&self, fqdn: &str, value: &str) -> R<()> {
        self.call("cleanup", fqdn, value).await
    }
}

/// `reqwest` without its `json` feature: a JSON body by hand.
trait JsonBody {
    fn json_body(self, body: &Value) -> Self;
}

impl JsonBody for reqwest::RequestBuilder {
    fn json_body(self, body: &Value) -> Self {
        self.header("content-type", "application/json")
            .body(body.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::certs::test_support::{serve_http, Exchange};
    use std::sync::Mutex as StdMutex;

    /// A Cloudflare stand-in: one zone, `example.com`, and its records.
    #[tokio::test]
    async fn cloudflare_finds_the_zone_and_manages_the_record_test() -> R<()> {
        let records: Arc<StdMutex<Vec<Value>>> = Arc::default();
        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let (records_in, seen_in) = (records.clone(), seen.clone());
        let api = serve_http(move |exchange: Exchange| {
            let mut records = records_in.lock().unwrap();
            seen_in
                .lock()
                .unwrap()
                .push(format!("{} {}", exchange.method, exchange.path_and_query));
            assert_eq!(exchange.header("authorization"), Some("Bearer tok".into()));
            let path = exchange.path();
            match (exchange.method.as_str(), path.as_str()) {
                ("GET", "/zones") => {
                    let name = exchange.query("name").unwrap_or_default();
                    let result = if name == "example.com" {
                        json!([{ "id": "z1", "name": "example.com" }])
                    } else {
                        json!([])
                    };
                    (200, json!({ "success": true, "result": result }).to_string())
                }
                ("POST", "/zones/z1/dns_records") => {
                    let mut body: Value = serde_json::from_slice(&exchange.body).unwrap();
                    body["id"] = json!(format!("r{}", records.len()));
                    records.push(body);
                    (200, json!({ "success": true }).to_string())
                }
                ("GET", "/zones/z1/dns_records") => (
                    200,
                    json!({ "success": true, "result": records.clone() }).to_string(),
                ),
                ("DELETE", record) => {
                    let id = record.rsplit('/').next().unwrap().to_string();
                    records.retain(|r| r["id"] != json!(id));
                    (200, json!({ "success": true }).to_string())
                }
                _ => (404, json!({ "success": false, "errors": [{ "code": 7003, "message": "no route" }] }).to_string()),
            }
        })
        .await?;
        let provider = Cloudflare::new(reqwest::Client::new(), &api, "tok");
        provider
            .present("_acme-challenge.www.example.com.", "v1")
            .await?;
        {
            let records = records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["name"], "_acme-challenge.www.example.com");
            assert_eq!(records[0]["content"], "v1");
            assert_eq!(records[0]["type"], "TXT");
        }
        // It walked www.example.com, then example.com; never the TLD.
        let lookups: Vec<String> = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.starts_with("GET /zones?"))
            .cloned()
            .collect();
        assert_eq!(
            lookups,
            vec![
                "GET /zones?name=www.example.com",
                "GET /zones?name=example.com"
            ]
        );
        provider
            .cleanup("_acme-challenge.www.example.com.", "v1")
            .await?;
        assert!(records.lock().unwrap().is_empty());
        let err = provider
            .present("_acme-challenge.other.org.", "v")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no zone"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn the_webhook_speaks_httpreq_test() -> R<()> {
        let calls: Arc<StdMutex<Vec<(String, Value, Option<String>)>>> = Arc::default();
        let calls_in = calls.clone();
        let url = serve_http(move |exchange: Exchange| {
            calls_in.lock().unwrap().push((
                exchange.path(),
                serde_json::from_slice(&exchange.body).unwrap_or(Value::Null),
                exchange.header("authorization"),
            ));
            if exchange.path() == "/dns/cleanup" {
                (500, "boom".to_string())
            } else {
                (200, String::new())
            }
        })
        .await?;
        let hook = Webhook::new(
            reqwest::Client::new(),
            &format!("{url}/dns/"),
            Some("t0k".into()),
        );
        hook.present("_acme-challenge.a.test.", "val").await?;
        let err = hook
            .cleanup("_acme-challenge.a.test.", "val")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("500") && err.contains("boom"), "{err}");
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, "/dns/present");
        assert_eq!(
            calls[0].1,
            json!({ "fqdn": "_acme-challenge.a.test.", "value": "val" })
        );
        assert_eq!(calls[0].2.as_deref(), Some("Bearer t0k"));
        Ok(())
    }
}
