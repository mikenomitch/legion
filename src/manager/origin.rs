//! Browser-origin policy for the manager's HTTP surfaces (gRPC-Web on the
//! main listener and the exec WebSocket).
//!
//! Browsers attach ambient network reach to every page: a page on any website
//! can send requests to `127.0.0.1:4747`, and DNS rebinding lets it do so
//! under its own hostname. With ACLs off (the default dev posture) every
//! caller is management, so an allow-all CORS policy let any website run jobs
//! on the cluster. This module closes that:
//!
//! - Loopback origins (`localhost`, `127.0.0.0/8`, `[::1]`, any port) are
//!   always allowed. They are the local UI dev server, and anything running
//!   locally can already reach the manager directly.
//! - `server.allowedOrigins` adds more, exactly (`https://legion.corp`), or
//!   `"*"` for every origin.
//! - Requests that carry no `Origin` header are never affected. Browsers
//!   always send one on cross-origin requests, on POSTs, and on WebSocket
//!   handshakes, and non-browser clients (CLI, workers, SDKs) send none.
//!
//! A DNS-rebinding page is caught by the same check: its requests carry the
//! attacker's own origin, which is neither loopback nor configured. Checking
//! `Host` as well would break legitimate clients, which reach the manager by
//! whatever name or address they were given.
//!
//! Enforcement is server-side ([`OriginGuardLayer`] rejects with 403 before
//! the request reaches any handler), not only through CORS response headers,
//! which merely stop a browser from reading a response it already caused.

use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::Either;
use tonic::body::BoxBody;
use tower::{Layer, Service};

use crate::prelude::*;

/// Which browser origins may call the manager.
#[derive(Debug, Clone, Default)]
pub struct OriginPolicy {
    any: bool,
    allowed: Vec<String>,
}

impl OriginPolicy {
    /// Builds the policy from `server.allowedOrigins`.
    pub fn new(allowed_origins: &[String]) -> Self {
        let any = allowed_origins.iter().any(|origin| origin.trim() == "*");
        let allowed = allowed_origins
            .iter()
            .map(|origin| normalize(origin))
            .filter(|origin| !origin.is_empty() && origin != "*")
            .collect();
        OriginPolicy { any, allowed }
    }

    /// Whether a request carrying this `Origin` header value may proceed.
    pub fn allows(&self, origin: &str) -> bool {
        if self.any {
            return true;
        }
        let origin = normalize(origin);
        is_loopback_origin(&origin) || self.allowed.contains(&origin)
    }

    /// Whether a request may proceed: no `Origin` header (a non-browser
    /// client) or an allowed one. A header that is not valid text is refused.
    pub fn allows_request(&self, headers: &http::HeaderMap) -> bool {
        match headers.get(http::header::ORIGIN) {
            None => true,
            Some(value) => value.to_str().is_ok_and(|origin| self.allows(origin)),
        }
    }
}

fn normalize(origin: &str) -> String {
    origin.trim().trim_end_matches('/').to_ascii_lowercase()
}

/// `http(s)://` plus a loopback host, with any port. `null` (sandboxed
/// iframes, `file://` pages) is not loopback.
fn is_loopback_origin(origin: &str) -> bool {
    let Some(rest) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };
    // Strip the port. IPv6 literals are bracketed, so their colons are
    // inside the brackets.
    let host = match rest.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((host, port)) if port.is_empty() || port.starts_with(':') => host,
            _ => return false,
        },
        None => rest.split(':').next().unwrap_or_default(),
    };
    if host == "localhost" {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// Rejects requests from disallowed browser origins with 403 before they
/// reach CORS, gRPC-Web translation, or any handler.
#[derive(Clone)]
pub struct OriginGuardLayer {
    policy: Arc<OriginPolicy>,
}

impl OriginGuardLayer {
    pub fn new(policy: Arc<OriginPolicy>) -> Self {
        Self { policy }
    }
}

impl<S> Layer<S> for OriginGuardLayer {
    type Service = OriginGuard<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OriginGuard {
            inner,
            policy: self.policy.clone(),
        }
    }
}

/// The service produced by [`OriginGuardLayer`].
#[derive(Clone)]
pub struct OriginGuard<S> {
    inner: S,
    policy: Arc<OriginPolicy>,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for OriginGuard<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>>,
{
    type Response = http::Response<BoxBody>;
    type Error = S::Error;
    type Future = Either<std::future::Ready<Result<Self::Response, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        if self.policy.allows_request(req.headers()) {
            return Either::Right(self.inner.call(req));
        }
        warn!(
            origin = req
                .headers()
                .get(http::header::ORIGIN)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-"),
            path = req.uri().path(),
            "Refused a browser request from a disallowed origin \
             (add it to server.allowedOrigins to allow it)"
        );
        Either::Left(std::future::ready(Ok(http::Response::builder()
            .status(http::StatusCode::FORBIDDEN)
            .body(tonic::body::empty_body())
            .expect("static response parts are valid"))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_origins_are_always_allowed() {
        let policy = OriginPolicy::new(&[]);
        for origin in [
            "http://localhost:5173",
            "http://127.0.0.1:4747",
            "http://127.1.2.3",
            "https://localhost",
            "http://[::1]:5173",
            "HTTP://LOCALHOST:5173/",
        ] {
            assert!(policy.allows(origin), "{origin} should be allowed");
        }
    }

    #[test]
    fn other_origins_need_configuration() {
        let policy = OriginPolicy::new(&[]);
        for origin in [
            "https://evil.example",
            "http://localhost.evil.example",
            "http://127.0.0.1.evil.example",
            "http://[::1].evil.example",
            "null",
            "",
        ] {
            assert!(!policy.allows(origin), "{origin} should be refused");
        }

        let policy = OriginPolicy::new(&["https://legion.corp/".to_string()]);
        assert!(policy.allows("https://legion.corp"));
        assert!(!policy.allows("http://legion.corp"));
        assert!(!policy.allows("https://legion.corp:8443"));

        assert!(OriginPolicy::new(&["*".to_string()]).allows("https://anything.example"));
    }

    #[test]
    fn requests_without_an_origin_are_unaffected() {
        let policy = OriginPolicy::new(&[]);
        assert!(policy.allows_request(&http::HeaderMap::new()));

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        assert!(!policy.allows_request(&headers));
    }

    #[tokio::test]
    async fn guard_refuses_disallowed_origins_before_the_handler_runs() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::ServiceExt;

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let inner = tower::service_fn(move |_req: http::Request<()>| {
            counter.fetch_add(1, Ordering::SeqCst);
            async {
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(http::StatusCode::OK)
                        .body(tonic::body::empty_body())
                        .unwrap(),
                )
            }
        });
        let guard = OriginGuardLayer::new(Arc::new(OriginPolicy::new(&[]))).layer(inner);
        let request = |origin: Option<&str>| {
            let mut builder = http::Request::builder()
                .method("POST")
                .uri("/legion.Manager/RunJob");
            if let Some(origin) = origin {
                builder = builder.header(http::header::ORIGIN, origin);
            }
            builder.body(()).unwrap()
        };

        let refused = guard
            .clone()
            .oneshot(request(Some("https://evil.example")))
            .await
            .unwrap();
        assert_eq!(refused.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the handler must not run");

        for origin in [None, Some("http://localhost:5173")] {
            let allowed = guard.clone().oneshot(request(origin)).await.unwrap();
            assert_eq!(allowed.status(), http::StatusCode::OK);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
