//! Public OIDC HTTP endpoints: the discovery document and the JWKS.
//!
//! External relying parties (AWS STS, GCP Workload Identity Federation,
//! Vault) fetch `/.well-known/openid-configuration` and the JWKS with
//! **generic HTTP clients** — they cannot speak gRPC or grpc-web — so these
//! two read-only routes must be plain HTTP/1.1 JSON responses.
//!
//! Rather than binding a second listener, [`OidcHttpLayer`] is a small tower
//! layer mounted **outermost** on the manager's main listener (which already
//! has `accept_http1(true)`, see `crate::manager::agent`). It intercepts GET
//! requests for exactly two paths — [`DISCOVERY_PATH`] and the configured
//! JWKS path — and answers them directly; every other request falls through
//! to CORS → grpc-web → the gRPC `Manager` service untouched. This keeps the
//! approach dependency-free (no axum) and version-proof against tonic's
//! internal router.
//!
//! The routes are only mounted when `workloadIdentity.enabled` is true: the
//! layer is constructed with `None` otherwise and becomes a pure passthrough.
//! Unknown paths under the reserved `/.well-known/` namespace return 404 so
//! probes get a sane HTTP answer instead of a gRPC error frame.
//!
//! The JSON bodies advertise `issuer`/`jwks_uri` from the **configured**
//! issuer URL (`workloadIdentity.issuer`), not from the local bind address,
//! so the published URLs stay correct behind proxies and load balancers.

use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::Either;
use http_body_util::Full;
use tonic::body::BoxBody;
use tower::{Layer, Service};

use crate::manager::workload_identity::{WorkloadIdentityState, DISCOVERY_PATH};
use crate::prelude::*;

/// Mounts the OIDC discovery + JWKS routes in front of an HTTP service.
/// Constructed with `None` (workload identity disabled) it passes every
/// request through unchanged.
#[derive(Clone)]
pub struct OidcHttpLayer {
    state: Option<Arc<WorkloadIdentityState>>,
}

impl OidcHttpLayer {
    pub fn new(state: Option<Arc<WorkloadIdentityState>>) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for OidcHttpLayer {
    type Service = OidcHttp<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OidcHttp {
            inner,
            state: self.state.clone(),
        }
    }
}

/// The service produced by [`OidcHttpLayer`].
#[derive(Clone)]
pub struct OidcHttp<S> {
    inner: S,
    state: Option<Arc<WorkloadIdentityState>>,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for OidcHttp<S>
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
        let Some(state) = self.state.as_ref() else {
            return Either::Right(self.inner.call(req));
        };
        if req.method() != http::Method::GET {
            return Either::Right(self.inner.call(req));
        }

        let path = req.uri().path();
        let body = if path == DISCOVERY_PATH {
            Some(state.discovery_document())
        } else if path == state.jwks_path() {
            Some(state.jwks())
        } else if path.starts_with("/.well-known/") {
            // The well-known namespace is owned by this surface: answer
            // probes with a plain 404 instead of a gRPC error frame.
            return Either::Left(std::future::ready(Ok(status_response(
                http::StatusCode::NOT_FOUND,
            ))));
        } else {
            None
        };

        match body {
            Some(Ok(document)) => Either::Left(std::future::ready(Ok(json_response(&document)))),
            Some(Err(err)) => {
                error!(error = ?err, path, "Failed to render OIDC document");
                Either::Left(std::future::ready(Ok(status_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                ))))
            }
            None => Either::Right(self.inner.call(req)),
        }
    }
}

fn json_response(value: &Value) -> http::Response<BoxBody> {
    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::CACHE_CONTROL, "no-store")
        .body(tonic::body::boxed(Full::new(Bytes::from(
            value.to_string(),
        ))))
        .expect("static response parts are valid")
}

fn status_response(status: http::StatusCode) -> http::Response<BoxBody> {
    http::Response::builder()
        .status(status)
        .body(tonic::body::empty_body())
        .expect("static response parts are valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::server::ManagerAgentService;
    use crate::storage::store::Store;
    use crate::structs::manager_server::ManagerServer;
    use crate::structs::workload_identity::{WorkloadIdentity, WorkloadIdentityFile};
    use crate::structs::{ManagerConfigFull, WorkloadIdentityConfigFull};

    use anyhow::Context as _;
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    use std::time::{Duration, SystemTime};
    use tokio::sync::oneshot;
    use tonic::transport::Server;
    use tonic_web::GrpcWebLayer;
    use tower_http::cors::{Any, CorsLayer};

    fn setup_store() -> R<Store> {
        let store_dir = tempfile::Builder::new()
            .prefix("legion-oidc-http-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();
        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            ..Default::default()
        };
        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;
        Ok(store)
    }

    /// Boot the manager's real listener stack (HTTP/1.1 + CORS + grpc-web +
    /// the gRPC service) with the OIDC layer mounted, exactly as
    /// `run_manager` does. Returns the base URL and a shutdown handle.
    async fn boot_manager(
        store: Store,
        state: Option<Arc<WorkloadIdentityState>>,
    ) -> R<(String, oneshot::Sender<()>)> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);

        let cors = CorsLayer::new().allow_origin(Any).allow_headers(Any);
        let service = ManagerServer::new(ManagerAgentService::new(store));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .accept_http1(true)
                .layer(OidcHttpLayer::new(state))
                .layer(cors)
                .layer(GrpcWebLayer::new())
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });
        // Give the listener a moment to bind.
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok((format!("http://{addr}"), shutdown_tx))
    }

    fn enabled_state(store: &Store, issuer_url: &str) -> R<Arc<WorkloadIdentityState>> {
        let config = WorkloadIdentityConfigFull {
            enabled: true,
            issuer: Some(issuer_url.to_string()),
            ..Default::default()
        };
        Ok(Arc::new(WorkloadIdentityState::initialize(
            store,
            &config,
            issuer_url,
            SystemTime::now(),
        )?))
    }

    #[tokio::test]
    async fn discovery_and_jwks_are_fetchable_and_verify_a_token() -> R<()> {
        let store = setup_store()?;
        // The issuer URL is the externally-published one (config-driven, not
        // derived from the bind address); point it at the test listener so
        // the advertised jwks_uri is fetchable in this test.
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);
        let base = format!("http://{addr}");
        let state = enabled_state(&store, &base)?;

        let cors = CorsLayer::new().allow_origin(Any).allow_headers(Any);
        let service = ManagerServer::new(ManagerAgentService::new(store.clone()));
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let state_for_server = state.clone();
        tokio::spawn(async move {
            Server::builder()
                .accept_http1(true)
                .layer(OidcHttpLayer::new(Some(state_for_server)))
                .layer(cors)
                .layer(GrpcWebLayer::new())
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 1. Discovery document: issuer + jwks_uri come from the configured
        //    issuer URL.
        let discovery: Value = reqwest::get(format!("{base}{DISCOVERY_PATH}"))
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(discovery["issuer"], json!(base));
        assert_eq!(
            discovery["jwks_uri"],
            json!(format!("{base}/.well-known/jwks.json"))
        );
        assert_eq!(
            discovery["id_token_signing_alg_values_supported"],
            json!(["ES256"])
        );

        // 2. Issue a token, then verify it the way a third party would: using
        //    ONLY the JWKS fetched over HTTP (kid → JWK → ES256 verify).
        let claims = crate::structs::workload_identity::WorkloadIdentityClaims {
            region: "auto".into(),
            job: "api".into(),
            group: "web".into(),
            task: "server".into(),
            node_id: "node-1".into(),
            alloc_id: "7".into(),
            instance: None,
            slot: None,
        };
        let identity = WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            file: Some(WorkloadIdentityFile {
                path: "/tmp/token".into(),
                mode: None,
            }),
            ..Default::default()
        };
        let issued = state.issue(&claims, &identity, SystemTime::now())?;

        let jwks: Value = reqwest::get(discovery["jwks_uri"].as_str().unwrap())
            .await?
            .error_for_status()?
            .json()
            .await?;
        let header = jsonwebtoken::decode_header(&issued.token)?;
        let kid = header.kid.context("token missing kid")?;
        let jwk = jwks["keys"]
            .as_array()
            .context("jwks keys")?
            .iter()
            .find(|k| k["kid"] == json!(kid))
            .context("kid not present in fetched JWKS")?;
        assert_eq!(jwk["kty"], json!("EC"));
        assert_eq!(jwk["alg"], json!("ES256"));
        let decoding = DecodingKey::from_ec_components(
            jwk["x"].as_str().context("jwk x")?,
            jwk["y"].as_str().context("jwk y")?,
        )?;
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[&base]);
        validation.set_audience(&["sts.amazonaws.com"]);
        let decoded = decode::<Map<String, Value>>(&issued.token, &decoding, &validation)?.claims;
        assert_eq!(decoded["sub"], json!("auto:api:web:server"));
        assert_eq!(decoded["legion.io/alloc"], json!("7"));

        // 3. Unknown well-known paths are plain 404s.
        let resp = reqwest::get(format!("{base}/.well-known/does-not-exist")).await?;
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn endpoints_absent_when_workload_identity_disabled() -> R<()> {
        let store = setup_store()?;
        let (base, shutdown) = boot_manager(store, None).await?;

        let resp = reqwest::get(format!("{base}{DISCOVERY_PATH}")).await?;
        // With the feature disabled the request falls through to the gRPC
        // stack, which does not serve a discovery document: either a non-200
        // status or a non-JSON / non-discovery body.
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let parsed: Result<Value, _> = serde_json::from_str(&body);
        let looks_like_discovery = parsed.map(|v| v.get("issuer").is_some()).unwrap_or(false);
        assert!(
            !status.is_success() || !looks_like_discovery,
            "discovery document must not be served when disabled (status {status}, body {body:?})"
        );

        let _ = shutdown.send(());
        Ok(())
    }
}
