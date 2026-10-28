//! The server-side half of a sandboxed page's RPC allowlist (docs/apps.md,
//! "Sandboxed pages"; apps plan §6.5, tier 2).
//!
//! A sandboxed page never calls the manager itself: it runs in an opaque
//! origin with `connect-src 'none'` and no token. It asks the SPA, over a
//! `postMessage` bridge, and the SPA makes the call as the viewer, but only
//! for an RPC in the page's `rpcs` (`ui.pages[].rpcs`). The SPA stamps every
//! such call with `legion-app-page: <app>/<page>`, and this layer checks it
//! again against the page's current manifest before the handler runs:
//!
//! - the app exists and is enabled, and declares the page;
//! - the call is a Manager RPC the page lists, and pages may call it at all
//!   ([`crate::structs::app_ui::page_rpc_kind`]).
//!
//! Anything else is refused with `PERMISSION_DENIED`. A call without the
//! header passes untouched: this is not what authorizes the viewer (the
//! handler's ACL check does that, as for any call), and the header is the
//! SPA's own claim. What it adds is that an SPA bug cannot widen a page's
//! allowlist, that a page still open after an upgrade narrowed its `rpcs`
//! is held to the new list, and that bridged calls are attributable in the
//! manager's logs.

use std::task::{Context, Poll};

use futures::future::Either;

use tonic::body::BoxBody;
use tonic::Status;
use tower::{Layer, Service};

use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::app_ui::{page_rpc_refusal, APP_PAGE_HEADER};

/// Path prefix of the public Manager service.
const MANAGER_SERVICE_PREFIX: &str = "/legion.Manager/";

/// Tower layer checking calls the SPA makes for sandboxed pages.
#[derive(Clone)]
pub struct AppPageGuardLayer {
    store: Store,
}

impl AppPageGuardLayer {
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for AppPageGuardLayer {
    type Service = AppPageGuard<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AppPageGuard {
            inner,
            store: self.store.clone(),
        }
    }
}

/// The service produced by [`AppPageGuardLayer`].
#[derive(Clone)]
pub struct AppPageGuard<S> {
    inner: S,
    store: Store,
}

/// Check one bridged call (`page_header` is the `legion-app-page` value,
/// `path` the gRPC route): `Ok` when it may proceed.
pub fn check_page_call(store: &Store, page_header: &str, path: &str) -> Result<(), Status> {
    let Some((app, page)) = page_header.trim().split_once('/') else {
        return Err(Status::permission_denied(format!(
            "{APP_PAGE_HEADER} must name `<app>/<page>`"
        )));
    };
    let Some(method) = path.strip_prefix(MANAGER_SERVICE_PREFIX) else {
        return Err(Status::permission_denied(
            "a sandboxed page calls Manager RPCs only",
        ));
    };
    let record = store
        .get_app(app)
        .map_err(|err| Status::unavailable(format!("loading app {app}: {err:#}")))?
        .ok_or_else(|| {
            Status::permission_denied(format!(
                "app {app:?} is not installed, so its page {page:?} may call nothing"
            ))
        })?;
    if !record.enabled {
        return Err(Status::permission_denied(format!(
            "app {app} is disabled, so its page {page:?} may call nothing"
        )));
    }
    let ui = record.manifest.ui_spec().map_err(|err| {
        Status::permission_denied(format!("app {app}'s ui block is invalid: {err}"))
    })?;
    let Some(declared) = ui.page(page) else {
        return Err(Status::permission_denied(format!(
            "app {app} declares no page {page:?}"
        )));
    };
    if declared.allows_rpc(method) {
        return Ok(());
    }
    if declared.rpcs.iter().any(|rpc| rpc == method) {
        return Err(Status::permission_denied(format!(
            "page {app}/{page}: {}",
            page_rpc_refusal(method)
        )));
    }
    Err(Status::permission_denied(format!(
        "page {app}/{page} may not call {method}: it is not in the page's `rpcs` ({})",
        if declared.rpcs.is_empty() {
            "none".to_string()
        } else {
            declared.rpcs.join(", ")
        }
    )))
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for AppPageGuard<S>
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
        let Some(header) = req.headers().get(APP_PAGE_HEADER) else {
            return Either::Right(self.inner.call(req));
        };
        let header = header.to_str().unwrap_or_default().to_string();
        let path = req.uri().path().to_string();
        match check_page_call(&self.store, &header, &path) {
            Ok(()) => {
                debug!(page = %header, path = %path, "A sandboxed page's call");
                Either::Right(self.inner.call(req))
            }
            Err(status) => {
                warn!(
                    page = %header,
                    path = %path,
                    reason = %status.message(),
                    "Refused a sandboxed page's call outside its allowlist"
                );
                Either::Left(std::future::ready(Ok(status.into_http())))
            }
        }
    }
}
