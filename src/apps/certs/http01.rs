//! The HTTP-01 responder (`legion cert http01`): answers every ACME
//! HTTP-01 challenge for one Issuer on port 80, statelessly, and redirects
//! everything else to HTTPS.
//!
//! An HTTP-01 key authorization is `<token>.<thumbprint>`, where the
//! thumbprint is the Issuer's ACME account key's, which is public. So the
//! responder needs no list of pending challenges and no secret: it answers
//! `GET /.well-known/acme-challenge/<token>` with `<token>.<thumbprint>`
//! for any token. Answering a token the Issuer never asked about gives
//! nothing away: only the holder of the account key can use it.

use crate::prelude::*;
use std::sync::RwLock;

/// The challenge path prefix (RFC 8555 §8.3).
pub const CHALLENGE_PREFIX: &str = "/.well-known/acme-challenge/";
/// The responder's own health path, outside anything an app serves.
pub const HEALTH_PATH: &str = "/.well-known/legion-certs-http01";

/// What the responder answers one request with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    pub location: Option<String>,
    pub body: String,
}

/// Whether `token` looks like an ACME token: base64url, 1 to 128
/// characters.
fn is_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 128
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Answer one request: a challenge, the health path, a redirect to HTTPS
/// (when `redirect`), or 404.
pub fn answer(
    method: &str,
    path_and_query: &str,
    host: Option<&str>,
    thumbprint: Option<&str>,
    redirect: bool,
) -> Answer {
    let path = path_and_query.split('?').next().unwrap_or("");
    if path == HEALTH_PATH {
        return match thumbprint {
            Some(_) => Answer {
                status: 200,
                location: None,
                body: "ok\n".into(),
            },
            None => Answer {
                status: 503,
                location: None,
                body: "waiting for the issuer's account thumbprint\n".into(),
            },
        };
    }
    if let Some(token) = path.strip_prefix(CHALLENGE_PREFIX) {
        if !matches!(method, "GET" | "HEAD") || !is_token(token) {
            return Answer {
                status: 404,
                location: None,
                body: "not found\n".into(),
            };
        }
        return match thumbprint {
            Some(thumbprint) => Answer {
                status: 200,
                location: None,
                body: super::acme::key_authorization(token, thumbprint),
            },
            None => Answer {
                status: 503,
                location: None,
                body: "the responder does not know its issuer's thumbprint yet\n".into(),
            },
        };
    }
    let host = host
        .map(|host| host.trim())
        .filter(|host| !host.is_empty())
        .map(strip_port);
    match (redirect, host) {
        (true, Some(host)) if !host.contains(['/', ' ', '\\']) => Answer {
            status: 308,
            location: Some(format!("https://{host}{path_and_query}")),
            body: String::new(),
        },
        _ => Answer {
            status: 404,
            location: None,
            body: "not found\n".into(),
        },
    }
}

/// A `Host` without its port (the HTTPS side listens on 443):
/// `a.test:80` and `[2001:db8::1]:80` lose it, `[2001:db8::1]` keeps its
/// brackets.
fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return match host.find(']') {
            Some(end) => &host[..=end],
            None => host,
        };
    }
    match host.rsplit_once(':') {
        Some((name, port)) if !name.contains(':') && port.chars().all(|c| c.is_ascii_digit()) => {
            name
        }
        _ => host,
    }
}

/// The thumbprint a responder answers with, which a refresher may change.
pub type SharedThumbprint = Arc<RwLock<Option<String>>>;

/// Serve the responder on `listener` until the future is dropped.
pub async fn serve(
    listener: tokio::net::TcpListener,
    thumbprint: SharedThumbprint,
    redirect: bool,
) -> R<()> {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::Response;
    use hyper_util::rt::TokioIo;
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                debug!(error = ?err, "http01 responder accept failed");
                continue;
            }
        };
        let thumbprint = thumbprint.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request: hyper::Request<_>| {
                let thumbprint = thumbprint.read().ok().and_then(|value| value.clone());
                async move {
                    let host = request
                        .headers()
                        .get(hyper::header::HOST)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string)
                        .or_else(|| request.uri().host().map(str::to_string));
                    let path_and_query = request
                        .uri()
                        .path_and_query()
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| "/".to_string());
                    let answer = answer(
                        request.method().as_str(),
                        &path_and_query,
                        host.as_deref(),
                        thumbprint.as_deref(),
                        redirect,
                    );
                    let mut response = Response::builder()
                        .status(answer.status)
                        .header("content-type", "text/plain; charset=utf-8")
                        .header("cache-control", "no-store");
                    if let Some(location) = &answer.location {
                        response = response.header("location", location);
                    }
                    let response = response
                        .body(Full::new(Bytes::from(answer.body)))
                        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())));
                    Ok::<_, std::convert::Infallible>(response)
                }
            });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!(error = ?err, "http01 responder connection closed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenges_are_answered_statelessly_test() {
        let ok = answer(
            "GET",
            "/.well-known/acme-challenge/abc-DEF_123",
            Some("shop.example.com"),
            Some("THUMB"),
            true,
        );
        assert_eq!(ok.status, 200);
        assert_eq!(ok.body, "abc-DEF_123.THUMB");
        // Not a token: never echoed back.
        for path in [
            "/.well-known/acme-challenge/",
            "/.well-known/acme-challenge/a.b",
            "/.well-known/acme-challenge/<script>",
        ] {
            assert_eq!(
                answer("GET", path, None, Some("T"), true).status,
                404,
                "{path}"
            );
        }
        assert_eq!(
            answer(
                "POST",
                "/.well-known/acme-challenge/abc",
                None,
                Some("T"),
                true
            )
            .status,
            404
        );
        // Before the thumbprint is known: unavailable, so a CA retry or a
        // health check can tell.
        assert_eq!(
            answer("GET", "/.well-known/acme-challenge/abc", None, None, true).status,
            503
        );
        assert_eq!(answer("GET", HEALTH_PATH, None, None, true).status, 503);
        assert_eq!(
            answer("GET", HEALTH_PATH, None, Some("T"), true).status,
            200
        );
    }

    #[test]
    fn everything_else_goes_to_https_test() {
        let moved = answer(
            "GET",
            "/cart?id=7",
            Some("shop.example.com:80"),
            Some("T"),
            true,
        );
        assert_eq!(moved.status, 308);
        assert_eq!(
            moved.location.as_deref(),
            Some("https://shop.example.com/cart?id=7")
        );
        let v6 = answer("GET", "/", Some("[2001:db8::1]:80"), Some("T"), true);
        assert_eq!(v6.location.as_deref(), Some("https://[2001:db8::1]/"));
        assert_eq!(answer("GET", "/", None, Some("T"), true).status, 404);
        assert_eq!(
            answer("GET", "/", Some("evil/x"), Some("T"), true).status,
            404
        );
        assert_eq!(
            answer("GET", "/", Some("a.test"), Some("T"), false).status,
            404
        );
    }

    #[tokio::test]
    async fn the_server_answers_over_http_test() -> R<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let thumbprint: SharedThumbprint = Arc::new(RwLock::new(None));
        tokio::spawn(serve(listener, thumbprint.clone(), true));
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let url = format!("http://{addr}/.well-known/acme-challenge/tok");
        assert_eq!(client.get(&url).send().await?.status().as_u16(), 503);
        *thumbprint.write().unwrap() = Some("THUMB".into());
        let response = client
            .get(&url)
            .header("host", "a.example.com")
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.text().await?, "tok.THUMB");
        let response = client
            .get(format!("http://{addr}/x"))
            .header("host", "a.example.com")
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), 308);
        assert_eq!(
            response.headers()["location"].to_str()?,
            "https://a.example.com/x"
        );
        Ok(())
    }
}
