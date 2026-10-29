//! An app's UI views and actions, from the inside (docs/apps.md, "Views and
//! actions" and "Writing views"; apps plan §6.5, tier 1).
//!
//! An app registers **views** (functions from a context to a
//! `legion-view/1` document) and **actions** (functions from a context and
//! typed inputs to a message), and the framework serves them on the app's
//! UI port:
//!
//! - `GET /legion/v1/views/<view>?<context>`;
//! - `POST /legion/v1/actions/<action>` with `{ "context", "inputs" }`;
//! - `GET /legion/v1/pages/<page>`: a sandboxed page's single-file HTML
//!   bundle (tier 2), which the SPA mounts in a sandboxed iframe.
//!
//! Only the manager calls these, over the worker plane, after it has
//! authorized the viewer. Every request carries a **viewer assertion** in
//! the `Legion-Viewer` header, and the framework checks it before any
//! handler runs ([`AssertionVerifier`]): the signature against the
//! cluster's workload-identity keys, the issuer, `aud = app:<name>`, the
//! expiry, the `legion.io/viewer` claim, and that the claim describes *this*
//! request (the same kind, view or action, context, and inputs digest). An
//! action's assertion is also single-use. A request that fails any check is
//! refused (401 for a missing or invalid assertion, 403 for one that
//! describes another request), so a workload that can reach the port
//! directly can do nothing with it.
//!
//! Views a handler returns are validated against the view schema before they
//! leave, so a handler bug shows up as a clear error here rather than a
//! refusal at the manager.
//!
//! **Acting as the viewer.** An action's assertion also carries a
//! delegation: the action's declared permission on the context.
//! [`ActionRequest::client_as_viewer`] returns a Legion client that presents
//! the assertion back in `legion-on-behalf-of` beside the app's own
//! credential, so the manager runs its calls as `<viewer> via app <app>`,
//! bounded by the delegation and by the app's grant (docs/apps.md, "Acting
//! as the viewer").
//!
//! The keys come from the cluster: the framework fetches the issuer and the
//! JWKS through `LEGION_ADDR`, the allocation's API socket, which needs
//! workload identity on (`workloadIdentity.enabled`). Without it the app
//! serves `/healthz` but refuses every view with 503.

use crate::cmd::helpers::{AuthedManagerClient, ManagerConnection};
use crate::prelude::*;
use crate::structs::app_ui::{
    assertion_audience, assertion_mismatch, page_context, validate_view_value, AppView,
    AssertionKind, AssertionScope, Delegation, ViewContext, ViewerAssertion, ACTION_PATH_PREFIX,
    ASSERTION_CLAIM, ASSERTION_TTL_SECS, MAX_PAGE_BYTES, PAGE_PATH_PREFIX, VIEW_PATH_PREFIX,
};
use crate::structs::extensions::{
    validate_context, ValidateAnswer, ValidateCall, ValidateResult, VALIDATE_PATH,
};
use futures::future::BoxFuture;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Tolerance for clock disagreement between the manager that signed an
/// assertion and the app.
pub const CLOCK_SKEW: Duration = Duration::from_secs(30);
/// How long fetched keys are trusted before a refresh.
pub const KEY_REFRESH: Duration = Duration::from_secs(300);
/// The soonest an unknown `kid` triggers another fetch.
const KEY_MISS_BACKOFF: Duration = Duration::from_secs(10);
/// Largest action request body the framework reads.
pub const MAX_ACTION_BODY: usize = 256 * 1024;

/// Who is looking, as the manager vouches for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Viewer {
    /// `user:alice`, a token's name, a JWT subject.
    pub principal: String,
    /// The viewer's ACL token accessor, when they used one.
    pub accessor: Option<String>,
    /// The permission the manager checked (`scale:read`), if the entry
    /// declared one.
    pub permission: Option<String>,
    /// Where the permission was checked: on the context, on any scope
    /// (`nav`, `overview`), or not at all (no permission declared).
    pub scope: AssertionScope,
    /// For a namespaced permission checked on any scope: the namespace
    /// globs where the viewer holds it. Filter cluster-wide rows by
    /// [`Viewer::sees_namespace`].
    pub namespaces: Vec<String>,
    /// For an action: what [`ActionRequest::client_as_viewer`] may do as the
    /// viewer (before the app's own grant bounds it).
    pub delegation: Option<Delegation>,
}

impl Viewer {
    fn from_assertion(assertion: &ViewerAssertion) -> Viewer {
        Viewer {
            principal: assertion.principal.clone(),
            accessor: assertion.accessor.clone(),
            permission: assertion.permission.clone(),
            scope: assertion.scope,
            namespaces: assertion.namespaces.clone(),
            delegation: assertion.delegation.clone(),
        }
    }

    /// Whether a row in `namespace` belongs on a page shown to this viewer.
    /// True for anything checked on the context or with no permission
    /// declared; for an `any`-scope check, true where the viewer holds the
    /// permission (with the reserved-prefix rule: `*` does not reach
    /// `legion-system`).
    pub fn sees_namespace(&self, namespace: &str) -> bool {
        match self.scope {
            AssertionScope::Context | AssertionScope::None => true,
            AssertionScope::Any => self
                .namespaces
                .iter()
                .any(|pattern| crate::structs::acl::namespace_glob_match(pattern, namespace)),
        }
    }
}

/// A view request, verified.
#[derive(Debug, Clone)]
pub struct ViewRequest {
    pub view: String,
    pub context: ViewContext,
    pub viewer: Viewer,
}

/// An action request, verified. `inputs` are the action's declared inputs,
/// checked and defaulted by the manager.
#[derive(Clone)]
pub struct ActionRequest {
    pub action: String,
    pub context: ViewContext,
    pub inputs: Value,
    pub viewer: Viewer,
    /// The viewer assertion the request carried, which the app presents
    /// back to act as the viewer. Never logged.
    assertion: String,
}

impl std::fmt::Debug for ActionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionRequest")
            .field("action", &self.action)
            .field("context", &self.context)
            .field("inputs", &self.inputs)
            .field("viewer", &self.viewer)
            .field("assertion", &"<redacted>")
            .finish()
    }
}

impl ActionRequest {
    /// A Legion client that acts as the viewer (docs/apps.md, "Acting as the
    /// viewer"): it dials `LEGION_ADDR` like [`crate::apps::AppContext::client`],
    /// the API socket still attaches the app's own token, and every call
    /// also carries this action's viewer assertion in `legion-on-behalf-of`.
    /// The manager runs those calls as `<viewer> via app <app>`, holding
    /// only [`Viewer::delegation`] (the action's declared permission on its
    /// context, and `read` there when the viewer holds it), within the
    /// app's own grant, for the assertion's 60 seconds. Anything else is
    /// `PermissionDenied`, whatever the app's own grant would allow.
    pub async fn client_as_viewer(
        &self,
        context: &crate::apps::AppContext,
    ) -> R<AuthedManagerClient> {
        self.client_as_viewer_over(&context.connection()?)
    }

    /// [`Self::client_as_viewer`] over a connection the app already holds
    /// (its own [`ManagerConnection`], which must present the app's own
    /// identity: the manager refuses an assertion from anyone else).
    pub fn client_as_viewer_over(&self, connection: &ManagerConnection) -> R<AuthedManagerClient> {
        if self.viewer.delegation.is_none() {
            bail!(
                "this action's viewer assertion carries no delegation (the manager that signed \
                 it predates acting as the viewer)"
            );
        }
        connection.client_on_behalf_of(&self.assertion)
    }

    /// The raw viewer assertion, for an app that calls Legion its own way:
    /// send it in `legion-on-behalf-of` with the app's own credential.
    pub fn assertion(&self) -> &str {
        &self.assertion
    }
}

/// A submit's request to check this app's jobspec extension values
/// (`POST /legion/v1/extensions/validate`; docs/apps.md, "Jobspec
/// extensions"), verified: the job, its values for the app's keys with
/// their context, and the submitter as the viewer. The handler answers one
/// [`ValidateResult`] per value, in order: errors reject the submit,
/// warnings pass through to its reply.
#[derive(Debug, Clone)]
pub struct ValidateRequest {
    pub call: ValidateCall,
    pub viewer: Viewer,
}

/// A page request, verified: the page's bundle, for this viewer.
#[derive(Debug, Clone)]
pub struct PageRequest {
    pub page: String,
    pub viewer: Viewer,
}

/// What an action answers: a message for the viewer, and optionally a
/// refreshed view.
#[derive(Debug, Clone, Default)]
pub struct ActionOutcome {
    pub message: String,
    pub view: Option<AppView>,
}

impl ActionOutcome {
    pub fn done(message: impl Into<String>) -> ActionOutcome {
        ActionOutcome {
            message: message.into(),
            view: None,
        }
    }

    pub fn with_view(mut self, view: AppView) -> ActionOutcome {
        self.view = Some(view);
        self
    }
}

/// Return this from an action (as the error) to refuse it with a message
/// the viewer sees, rather than as a failure: "nothing to pause, this job
/// has no scaling policy". Anything else an action returns as an error is a
/// failure (HTTP 500).
#[derive(Debug, Clone)]
pub struct Refused(pub String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

type ViewHandler = Arc<dyn Fn(ViewRequest) -> BoxFuture<'static, R<AppView>> + Send + Sync>;
type ActionHandler =
    Arc<dyn Fn(ActionRequest) -> BoxFuture<'static, R<ActionOutcome>> + Send + Sync>;
type PageHandler = Arc<dyn Fn(PageRequest) -> BoxFuture<'static, R<String>> + Send + Sync>;
type ValidateHandler =
    Arc<dyn Fn(ValidateRequest) -> BoxFuture<'static, R<Vec<ValidateResult>>> + Send + Sync>;

/// The views and actions an app serves. Registrations may happen at any
/// time, also after the port is up.
#[derive(Default)]
pub struct UiRegistry {
    views: std::sync::RwLock<HashMap<String, ViewHandler>>,
    actions: std::sync::RwLock<HashMap<String, ActionHandler>>,
    pages: std::sync::RwLock<HashMap<String, PageHandler>>,
    validate: std::sync::RwLock<Option<ValidateHandler>>,
}

impl UiRegistry {
    /// Serve view `name` from `handler`. A second registration replaces the
    /// first.
    pub fn view<F, Fut>(&self, name: &str, handler: F)
    where
        F: Fn(ViewRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<AppView>> + Send + 'static,
    {
        let handler: ViewHandler = Arc::new(move |request| Box::pin(handler(request)));
        if let Ok(mut views) = self.views.write() {
            views.insert(name.to_string(), handler);
        }
    }

    /// Serve action `name` from `handler`.
    pub fn action<F, Fut>(&self, name: &str, handler: F)
    where
        F: Fn(ActionRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<ActionOutcome>> + Send + 'static,
    {
        let handler: ActionHandler = Arc::new(move |request| Box::pin(handler(request)));
        if let Ok(mut actions) = self.actions.write() {
            actions.insert(name.to_string(), handler);
        }
    }

    /// Serve sandboxed page `name` (`ui.pages`, at its default bundle path
    /// `/legion/v1/pages/<name>`) from `handler`, which returns the page's
    /// single-file HTML. The SPA mounts it in a sandboxed iframe under a
    /// strict CSP: everything it needs (scripts, styles, images as `data:`
    /// URLs) is inline, and it reaches Legion only through the bridge.
    pub fn page<F, Fut>(&self, name: &str, handler: F)
    where
        F: Fn(PageRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<String>> + Send + 'static,
    {
        let handler: PageHandler = Arc::new(move |request| Box::pin(handler(request)));
        if let Ok(mut pages) = self.pages.write() {
            pages.insert(name.to_string(), handler);
        }
    }

    /// Answer the app's validate call (the keys its manifest declares with
    /// `validate = true`) from `handler`. Without one, the framework answers
    /// that every value passes.
    pub fn validate<F, Fut>(&self, handler: F)
    where
        F: Fn(ValidateRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R<Vec<ValidateResult>>> + Send + 'static,
    {
        let handler: ValidateHandler = Arc::new(move |request| Box::pin(handler(request)));
        if let Ok(mut validate) = self.validate.write() {
            *validate = Some(handler);
        }
    }

    fn validate_handler(&self) -> Option<ValidateHandler> {
        self.validate.read().ok()?.clone()
    }

    fn page_handler(&self, name: &str) -> Option<PageHandler> {
        self.pages.read().ok()?.get(name).cloned()
    }

    fn view_handler(&self, name: &str) -> Option<ViewHandler> {
        self.views.read().ok()?.get(name).cloned()
    }

    fn action_handler(&self, name: &str) -> Option<ActionHandler> {
        self.actions.read().ok()?.get(name).cloned()
    }

    /// The registered view names, sorted.
    pub fn view_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .views
            .read()
            .map(|views| views.keys().cloned().collect())
            .unwrap_or_default();
        names.sort();
        names
    }
}

/// The cluster's verification material: its issuer and its JWKS.
#[derive(Debug, Clone)]
pub struct ClusterKeys {
    pub issuer: String,
    pub jwks: Value,
}

/// Where the framework gets the cluster's keys.
#[tonic::async_trait]
pub trait KeySource: Send + Sync {
    async fn fetch(&self) -> R<ClusterKeys>;
}

/// Keys known up front (tests, and apps that pin them).
pub struct StaticKeys(pub ClusterKeys);

#[tonic::async_trait]
impl KeySource for StaticKeys {
    async fn fetch(&self) -> R<ClusterKeys> {
        Ok(self.0.clone())
    }
}

/// The cluster's keys through `LEGION_ADDR`: the discovery document
/// (`/.well-known/openid-configuration`, for the issuer and the JWKS path)
/// and the JWKS, fetched over the allocation's API socket like any other
/// call the app makes. The worker forwards them to a manager, which serves
/// both whenever workload identity is on.
pub struct ManagerKeys {
    address: String,
}

impl ManagerKeys {
    pub fn new(address: impl Into<String>) -> ManagerKeys {
        ManagerKeys {
            address: address.into(),
        }
    }

    async fn get_json(&self, channel: &mut tonic::transport::Channel, path: &str) -> R<Value> {
        use http_body_util::{BodyExt, Limited};
        use tower::{Service, ServiceExt};
        let request = http::Request::get(path)
            .header(http::header::ACCEPT, "application/json")
            .body(tonic::body::empty_body())?;
        let ready = channel
            .ready()
            .await
            .with_context(|| format!("connect to {}", self.address))?;
        let response = ready
            .call(request)
            .await
            .with_context(|| format!("GET {path} through {}", self.address))?;
        if !response.status().is_success() {
            bail!(
                "GET {path} through {} answered {} (is workloadIdentity.enabled on the managers?)",
                self.address,
                response.status()
            );
        }
        let body = Limited::new(response.into_body(), 256 * 1024)
            .collect()
            .await
            .map_err(|err| anyhow!("reading {path}: {err}"))?
            .to_bytes();
        serde_json::from_slice(&body).with_context(|| format!("{path} is not JSON"))
    }
}

#[tonic::async_trait]
impl KeySource for ManagerKeys {
    async fn fetch(&self) -> R<ClusterKeys> {
        let mut channel = if let Some(path) = crate::cmd::helpers::unix_socket_path(&self.address) {
            crate::cmd::helpers::unix_socket_channel(path, Some(Duration::from_secs(5)))
        } else if self.address.starts_with("http://") {
            tonic::transport::Endpoint::from_shared(self.address.clone())?
                .connect_timeout(Duration::from_secs(5))
                .connect_lazy()
        } else {
            bail!(
                "LEGION_ADDR {:?} is neither the allocation's API socket (unix://) nor plain \
                 http://; the app cannot fetch the cluster's keys to verify viewer assertions",
                self.address
            );
        };
        let discovery = self
            .get_json(
                &mut channel,
                crate::manager::workload_identity::DISCOVERY_PATH,
            )
            .await?;
        let issuer = discovery
            .get("issuer")
            .and_then(Value::as_str)
            .context("the discovery document names no issuer")?
            .trim_end_matches('/')
            .to_string();
        let jwks_path = discovery
            .get("jwks_uri")
            .and_then(Value::as_str)
            .and_then(|uri| url::Url::parse(uri).ok())
            .map(|uri| uri.path().to_string())
            .unwrap_or_else(|| crate::manager::workload_identity::DEFAULT_JWKS_PATH.to_string());
        let jwks = self.get_json(&mut channel, &jwks_path).await?;
        Ok(ClusterKeys { issuer, jwks })
    }
}

#[derive(Default)]
struct KeyCache {
    issuer: String,
    keys: HashMap<String, DecodingKey>,
    fetched_at: Option<Instant>,
    last_attempt: Option<Instant>,
}

/// Why a request was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// 401: no assertion, or one that does not verify (signature, issuer,
    /// audience, expiry, not a viewer assertion).
    Unauthenticated(String),
    /// 403: a valid assertion for another request (view, action, context,
    /// inputs), or a replayed action assertion.
    Mismatch(String),
    /// 503: the framework cannot verify anything right now (no keys).
    Unavailable(String),
}

impl Refusal {
    fn status(&self) -> u16 {
        match self {
            Refusal::Unauthenticated(_) => 401,
            Refusal::Mismatch(_) => 403,
            Refusal::Unavailable(_) => 503,
        }
    }

    fn message(&self) -> &str {
        match self {
            Refusal::Unauthenticated(message)
            | Refusal::Mismatch(message)
            | Refusal::Unavailable(message) => message,
        }
    }
}

/// Verifies viewer assertions for one app (docs/apps.md, "The viewer
/// assertion").
pub struct AssertionVerifier {
    app: String,
    source: Arc<dyn KeySource>,
    cache: tokio::sync::Mutex<KeyCache>,
    /// Spent action assertions (`jti` to `exp`), forgotten once expired.
    spent: std::sync::Mutex<HashMap<String, i64>>,
}

impl AssertionVerifier {
    pub fn new(app: impl Into<String>, source: Arc<dyn KeySource>) -> AssertionVerifier {
        AssertionVerifier {
            app: app.into(),
            source,
            cache: tokio::sync::Mutex::new(KeyCache::default()),
            spent: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// A verifier that fetches keys through `LEGION_ADDR`, or `None` when it
    /// is unset (workload identity is off).
    pub fn from_env(app: &str) -> Option<AssertionVerifier> {
        let address = std::env::var("LEGION_ADDR")
            .ok()
            .filter(|addr| !addr.trim().is_empty())?;
        Some(AssertionVerifier::new(
            app,
            Arc::new(ManagerKeys::new(address.trim())),
        ))
    }

    /// The app this verifier checks assertions for.
    pub fn app(&self) -> &str {
        &self.app
    }

    fn install(cache: &mut KeyCache, keys: ClusterKeys) -> R<()> {
        let entries = keys
            .jwks
            .get("keys")
            .and_then(Value::as_array)
            .context("the JWKS has no `keys`")?;
        let mut parsed = HashMap::new();
        for entry in entries {
            let (Some(kid), Some(x), Some(y)) = (
                entry.get("kid").and_then(Value::as_str),
                entry.get("x").and_then(Value::as_str),
                entry.get("y").and_then(Value::as_str),
            ) else {
                continue;
            };
            if let Ok(key) = DecodingKey::from_ec_components(x, y) {
                parsed.insert(kid.to_string(), key);
            }
        }
        if parsed.is_empty() {
            bail!("the JWKS holds no usable ES256 key");
        }
        cache.issuer = keys.issuer.trim_end_matches('/').to_string();
        cache.keys = parsed;
        cache.fetched_at = Some(Instant::now());
        Ok(())
    }

    /// The key for `kid` and the issuer, fetching when the cache is empty,
    /// stale, or misses (at most every [`KEY_MISS_BACKOFF`] on misses). A
    /// failed fetch keeps the keys already held.
    async fn key(&self, kid: &str) -> Result<(DecodingKey, String), Refusal> {
        let mut cache = self.cache.lock().await;
        let stale = cache
            .fetched_at
            .is_none_or(|fetched| fetched.elapsed() >= KEY_REFRESH);
        let missing = !cache.keys.contains_key(kid);
        let may_retry = cache
            .last_attempt
            .is_none_or(|attempt| attempt.elapsed() >= KEY_MISS_BACKOFF);
        if (stale || missing) && (may_retry || cache.fetched_at.is_none()) {
            cache.last_attempt = Some(Instant::now());
            match self.source.fetch().await {
                Ok(keys) => {
                    if let Err(err) = Self::install(&mut cache, keys) {
                        warn!(app = %self.app, error = %err, "Unusable cluster keys");
                    }
                }
                Err(err) => warn!(
                    app = %self.app,
                    error = %format!("{err:#}"),
                    "Could not fetch the cluster's keys; keeping the ones held"
                ),
            }
        }
        if cache.keys.is_empty() {
            return Err(Refusal::Unavailable(
                "this app could not fetch the cluster's keys, so it cannot verify viewer \
                 assertions yet"
                    .into(),
            ));
        }
        let key = cache.keys.get(kid).cloned().ok_or_else(|| {
            Refusal::Unauthenticated(format!(
                "the viewer assertion is signed by key {kid:?}, which is not one of the cluster's"
            ))
        })?;
        Ok((key, cache.issuer.clone()))
    }

    /// Verify `token` at `now` (unix seconds): signature, issuer, audience,
    /// lifetime, and the `legion.io/viewer` claim. The caller then checks
    /// that the claim matches its request.
    pub async fn verify(
        &self,
        token: &str,
        now: i64,
    ) -> Result<(ViewerAssertion, Map<String, Value>), Refusal> {
        let header = decode_header(token)
            .map_err(|err| Refusal::Unauthenticated(format!("not a JWT: {err}")))?;
        if header.alg != Algorithm::ES256 {
            return Err(Refusal::Unauthenticated(
                "the viewer assertion is not ES256".into(),
            ));
        }
        let kid = header
            .kid
            .ok_or_else(|| Refusal::Unauthenticated("the viewer assertion has no kid".into()))?;
        let (key, issuer) = self.key(&kid).await?;
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_audience(&[assertion_audience(&self.app)]);
        validation.set_issuer(&[issuer.as_str()]);
        // Lifetime is checked below against `now`, with the skew.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.required_spec_claims.clear();
        let claims = decode::<Map<String, Value>>(token, &key, &validation)
            .map_err(|err| {
                Refusal::Unauthenticated(format!(
                    "the viewer assertion does not verify for app {} (issuer {issuer}): {err}",
                    self.app
                ))
            })?
            .claims;
        let skew = CLOCK_SKEW.as_secs() as i64;
        let number = |name: &str| claims.get(name).and_then(Value::as_i64);
        let (Some(issued), Some(expires)) = (number("iat"), number("exp")) else {
            return Err(Refusal::Unauthenticated(
                "the viewer assertion has no iat or exp".into(),
            ));
        };
        if now - skew > expires {
            return Err(Refusal::Unauthenticated(format!(
                "the viewer assertion expired at {expires} (app clock {now})"
            )));
        }
        if issued > now + skew {
            return Err(Refusal::Unauthenticated(format!(
                "the viewer assertion was issued at {issued}, after the app's clock ({now})"
            )));
        }
        if expires - issued > (ASSERTION_TTL_SECS as i64) * 2 {
            return Err(Refusal::Unauthenticated(
                "the token lives longer than a viewer assertion does".into(),
            ));
        }
        let claim = claims.get(ASSERTION_CLAIM).ok_or_else(|| {
            Refusal::Unauthenticated(format!(
                "the token carries no {ASSERTION_CLAIM} claim, so it is not a viewer assertion"
            ))
        })?;
        let assertion: ViewerAssertion = serde_json::from_value(claim.clone()).map_err(|err| {
            Refusal::Unauthenticated(format!("the {ASSERTION_CLAIM} claim is malformed: {err}"))
        })?;
        Ok((assertion, claims))
    }

    /// Mark an action assertion spent; a second use within its life is
    /// refused.
    fn spend(&self, claims: &Map<String, Value>, now: i64) -> Result<(), Refusal> {
        let jti = claims
            .get("jti")
            .and_then(Value::as_str)
            .ok_or_else(|| Refusal::Unauthenticated("the viewer assertion has no jti".into()))?;
        let expires = claims.get("exp").and_then(Value::as_i64).unwrap_or(now);
        let mut spent = self.spent.lock().unwrap_or_else(|p| p.into_inner());
        let skew = CLOCK_SKEW.as_secs() as i64;
        spent.retain(|_, until| *until + skew >= now);
        if spent.contains_key(jti) {
            return Err(Refusal::Mismatch(
                "this action's viewer assertion was already used".into(),
            ));
        }
        spent.insert(jti.to_string(), expires);
        Ok(())
    }
}

/// An app's UI port: the registry, the verifier, and a count of the
/// requests that reached it.
pub struct UiServer {
    registry: Arc<UiRegistry>,
    verifier: Option<Arc<AssertionVerifier>>,
    requests: AtomicU64,
}

/// One answer: an HTTP status and a JSON body, or, for a page, HTML.
#[derive(Debug, Clone)]
pub struct UiAnswer {
    pub status: u16,
    pub body: Value,
    /// A page's bundle, served as `text/html` instead of `body`.
    pub html: Option<String>,
}

impl UiAnswer {
    fn error(status: u16, message: impl Into<String>) -> UiAnswer {
        UiAnswer {
            status,
            body: json!({ "error": message.into() }),
            html: None,
        }
    }

    fn json(body: Value) -> UiAnswer {
        UiAnswer {
            status: 200,
            body,
            html: None,
        }
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

impl UiServer {
    /// `verifier` `None` refuses every view and action with 503: the app has
    /// no way to check who is asking.
    pub fn new(registry: Arc<UiRegistry>, verifier: Option<Arc<AssertionVerifier>>) -> UiServer {
        UiServer {
            registry,
            verifier,
            requests: AtomicU64::new(0),
        }
    }

    /// How many `/legion/v1/` requests reached this port, verified or not.
    pub fn requests_seen(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Answer one `/legion/v1/` request.
    pub async fn handle(
        &self,
        method: &str,
        path_and_query: &str,
        assertion: Option<&str>,
        body: &[u8],
    ) -> UiAnswer {
        self.handle_at(method, path_and_query, assertion, body, now_unix())
            .await
    }

    /// [`Self::handle`] at a given clock.
    pub async fn handle_at(
        &self,
        method: &str,
        path_and_query: &str,
        assertion: Option<&str>,
        body: &[u8],
        now: i64,
    ) -> UiAnswer {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let (path, query) = path_and_query
            .split_once('?')
            .unwrap_or((path_and_query, ""));
        let (kind, name) = if let Some(name) = path.strip_prefix(VIEW_PATH_PREFIX) {
            if method != "GET" {
                return UiAnswer::error(405, "views are fetched with GET");
            }
            (AssertionKind::View, name)
        } else if let Some(name) = path.strip_prefix(ACTION_PATH_PREFIX) {
            if method != "POST" {
                return UiAnswer::error(405, "actions are invoked with POST");
            }
            (AssertionKind::Action, name)
        } else if let Some(name) = path.strip_prefix(PAGE_PATH_PREFIX) {
            if method != "GET" {
                return UiAnswer::error(405, "pages are fetched with GET");
            }
            (AssertionKind::Page, name)
        } else if path == VALIDATE_PATH {
            if method != "POST" {
                return UiAnswer::error(405, "extension values are validated with POST");
            }
            (AssertionKind::Validate, "validate")
        } else {
            return UiAnswer::error(404, "not found");
        };
        let Some(verifier) = self.verifier.as_ref() else {
            return UiAnswer::error(
                503,
                "this app cannot verify viewer assertions: it has no LEGION_ADDR (workload \
                 identity is off on the managers), so it serves no views",
            );
        };
        let Some(token) = assertion.map(str::trim).filter(|token| !token.is_empty()) else {
            return UiAnswer::error(
                401,
                "no viewer assertion: views and actions are requested through the manager \
                 (GetAppView, InvokeAppAction), never directly",
            );
        };
        let (claimed, claims) = match verifier.verify(token, now).await {
            Ok(verified) => verified,
            Err(refusal) => return UiAnswer::error(refusal.status(), refusal.message()),
        };
        let mut validate_call = None;
        let (context, inputs) = match kind {
            AssertionKind::Validate => {
                if body.len() > MAX_ACTION_BODY {
                    return UiAnswer::error(413, "the validate request is too large");
                }
                let raw: Value = match serde_json::from_slice(body) {
                    Ok(raw) => raw,
                    Err(err) => {
                        return UiAnswer::error(400, format!("the validate request: {err}"))
                    }
                };
                match serde_json::from_value::<ValidateCall>(raw.clone()) {
                    Ok(call) => {
                        let context = validate_context(&call.job);
                        validate_call = Some(call);
                        (context, Some(raw))
                    }
                    Err(err) => {
                        return UiAnswer::error(400, format!("the validate request: {err}"))
                    }
                }
            }
            AssertionKind::View => match ViewContext::from_query(query) {
                Ok(context) => (context, None),
                Err(err) => return UiAnswer::error(400, err),
            },
            AssertionKind::Page if !query.is_empty() => {
                return UiAnswer::error(400, "a page request takes no query")
            }
            AssertionKind::Page => (page_context(), None),
            AssertionKind::Action => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Body {
                    context: ViewContext,
                    #[serde(default)]
                    inputs: Value,
                }
                if body.len() > MAX_ACTION_BODY {
                    return UiAnswer::error(413, "the action request is too large");
                }
                match serde_json::from_slice::<Body>(body) {
                    Ok(parsed) => {
                        let inputs = match parsed.inputs {
                            Value::Null => json!({}),
                            inputs => inputs,
                        };
                        (parsed.context, Some(inputs))
                    }
                    Err(err) => return UiAnswer::error(400, format!("the action request: {err}")),
                }
            }
        };
        if let Some(mismatch) = assertion_mismatch(
            &claimed,
            verifier.app(),
            kind,
            name,
            &context,
            inputs.as_ref(),
        ) {
            return UiAnswer::error(403, mismatch);
        }
        let viewer = Viewer::from_assertion(&claimed);
        let slot = context.slot().ok();
        match kind {
            AssertionKind::Validate => {
                let call = validate_call.expect("parsed above");
                let count = call.values.len();
                let Some(handler) = self.registry.validate_handler() else {
                    return UiAnswer::json(
                        serde_json::to_value(ValidateAnswer {
                            results: vec![ValidateResult::default(); count],
                        })
                        .unwrap_or_default(),
                    );
                };
                match handler(ValidateRequest { call, viewer }).await {
                    Ok(mut results) => {
                        results.resize(count, ValidateResult::default());
                        UiAnswer::json(
                            serde_json::to_value(ValidateAnswer { results }).unwrap_or_default(),
                        )
                    }
                    Err(err) => {
                        warn!(error = %format!("{err:#}"), "Validating extension values failed");
                        UiAnswer::error(500, format!("{err:#}"))
                    }
                }
            }
            AssertionKind::View => {
                let Some(handler) = self.registry.view_handler(name) else {
                    return UiAnswer::error(404, format!("this app serves no view {name:?}"));
                };
                let view = match handler(ViewRequest {
                    view: name.to_string(),
                    context,
                    viewer,
                })
                .await
                {
                    Ok(view) => view,
                    Err(err) => {
                        warn!(view = name, error = %format!("{err:#}"), "A view failed");
                        return UiAnswer::error(500, format!("{err:#}"));
                    }
                };
                match checked_view(&view, slot) {
                    Ok(body) => UiAnswer::json(body),
                    Err(err) => UiAnswer::error(500, err),
                }
            }
            AssertionKind::Page => {
                let Some(handler) = self.registry.page_handler(name) else {
                    return UiAnswer::error(404, format!("this app serves no page {name:?}"));
                };
                match handler(PageRequest {
                    page: name.to_string(),
                    viewer,
                })
                .await
                {
                    Ok(html) if html.len() > MAX_PAGE_BYTES => UiAnswer::error(
                        500,
                        format!(
                            "page {name:?} is {} bytes; a page is at most {MAX_PAGE_BYTES}",
                            html.len()
                        ),
                    ),
                    Ok(html) => UiAnswer {
                        status: 200,
                        body: Value::Null,
                        html: Some(html),
                    },
                    Err(err) => {
                        warn!(page = name, error = %format!("{err:#}"), "A page failed");
                        UiAnswer::error(500, format!("{err:#}"))
                    }
                }
            }
            AssertionKind::Action => {
                if let Err(refusal) = verifier.spend(&claims, now) {
                    return UiAnswer::error(refusal.status(), refusal.message());
                }
                let Some(handler) = self.registry.action_handler(name) else {
                    return UiAnswer::error(404, format!("this app serves no action {name:?}"));
                };
                info!(
                    action = name,
                    principal = %viewer.principal,
                    slot = %context.slot,
                    job = context.job_id().as_deref().unwrap_or("-"),
                    "Running an action"
                );
                let outcome = match handler(ActionRequest {
                    action: name.to_string(),
                    context,
                    inputs: inputs.unwrap_or_default(),
                    viewer,
                    assertion: token.to_string(),
                })
                .await
                {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        if let Some(refused) = err.downcast_ref::<Refused>() {
                            return UiAnswer::error(422, refused.0.clone());
                        }
                        warn!(action = name, error = %format!("{err:#}"), "An action failed");
                        return UiAnswer::error(500, format!("{err:#}"));
                    }
                };
                let mut body = json!({ "message": outcome.message });
                if let Some(view) = &outcome.view {
                    match checked_view(view, slot) {
                        Ok(view) => body["view"] = view,
                        Err(err) => return UiAnswer::error(500, err),
                    }
                }
                UiAnswer::json(body)
            }
        }
    }
}

/// A handler's view, validated as the manager will validate it.
fn checked_view(
    view: &AppView,
    slot: Option<crate::structs::app_ui::Slot>,
) -> Result<Value, String> {
    let value = serde_json::to_value(view).map_err(|err| err.to_string())?;
    validate_view_value(&value, None, slot)
        .map_err(|err| format!("the view this app built is invalid: {err}"))?;
    Ok(value)
}

/// Serve `server` (and `/healthz`, always 200) on `listener` until the task
/// is dropped: the UI port of an app built on this framework outside
/// `legion app run` (tests, embedding).
pub async fn serve(listener: tokio::net::TcpListener, server: Arc<UiServer>) -> R<()> {
    crate::apps::serve_port(listener, None, Some(server)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::workload_identity::Issuer;

    #[test]
    fn a_viewer_on_any_scope_sees_only_its_namespaces() {
        let viewer = Viewer {
            principal: "alice".into(),
            accessor: None,
            permission: Some("scale:read".into()),
            scope: AssertionScope::Any,
            namespaces: vec!["team-*".into()],
            delegation: None,
        };
        assert!(viewer.sees_namespace("team-a"));
        assert!(!viewer.sees_namespace("payments"));
        let everywhere = Viewer {
            namespaces: vec!["*".into()],
            ..viewer.clone()
        };
        assert!(everywhere.sees_namespace("payments"));
        assert!(!everywhere.sees_namespace("legion-system"));
        let on_context = Viewer {
            scope: AssertionScope::Context,
            namespaces: Vec::new(),
            ..viewer
        };
        assert!(on_context.sees_namespace("anything"));
    }

    const ISSUER: &str = "https://manager.example.com:4747";
    const NOW: i64 = 1_000_100;

    fn issuer() -> Issuer {
        Issuer::new(
            ISSUER,
            "/.well-known/jwks.json",
            Duration::from_secs(3600),
            None,
            std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000),
        )
        .expect("issuer")
    }

    fn context() -> ViewContext {
        ViewContext {
            slot: "job".into(),
            namespace: "default".into(),
            job: "web".into(),
            ..Default::default()
        }
    }

    fn assertion(kind: AssertionKind, name: &str, inputs: Option<&Value>) -> ViewerAssertion {
        ViewerAssertion {
            kind,
            app: "fake".into(),
            name: name.into(),
            principal: "user:alice".into(),
            accessor: Some("acc-1".into()),
            permission: Some("job:read".into()),
            scope: AssertionScope::Context,
            namespaces: Vec::new(),
            context: context(),
            inputs: inputs.map(crate::structs::app_ui::inputs_digest),
            delegation: None,
        }
    }

    fn sign(
        issuer: &Issuer,
        audience: &str,
        claim: Option<&ViewerAssertion>,
        jti: &str,
        iat: u64,
        exp: u64,
    ) -> String {
        let mut claims = Map::new();
        if let Some(claim) = claim {
            claims.insert(ASSERTION_CLAIM.into(), serde_json::to_value(claim).unwrap());
        }
        issuer
            .issue_ticket(audience, jti, "accessor:acc-1", claims, iat, exp)
            .expect("sign")
            .token
    }

    fn server(issuer: &Issuer) -> UiServer {
        let registry = Arc::new(UiRegistry::default());
        registry.view("job", |request: ViewRequest| async move {
            Ok(AppView::new(vec![crate::structs::app_ui::Block::Notice(
                crate::structs::app_ui::NoticeBlock {
                    tone: Default::default(),
                    title: None,
                    text: format!("hello {}", request.viewer.principal),
                },
            )]))
        });
        registry.action("go", |request: ActionRequest| async move {
            Ok(ActionOutcome::done(format!(
                "went {}",
                request.inputs["for"]
            )))
        });
        let keys = ClusterKeys {
            issuer: ISSUER.into(),
            jwks: issuer.jwks(),
        };
        UiServer::new(
            registry,
            Some(Arc::new(AssertionVerifier::new(
                "fake",
                Arc::new(StaticKeys(keys)),
            ))),
        )
    }

    const VIEW: &str = "/legion/v1/views/job?slot=job&namespace=default&job=web";

    /// The framework refuses a missing, expired, wrong-audience, or
    /// context-mismatched assertion before any handler runs (plan §9, A4).
    #[tokio::test]
    async fn assertions_are_checked_before_handlers() {
        let issuer = issuer();
        let server = server(&issuer);
        let view = assertion(AssertionKind::View, "job", None);
        let good = sign(
            &issuer,
            "app:fake",
            Some(&view),
            "va_1",
            1_000_090,
            1_000_150,
        );
        let ok = server.handle_at("GET", VIEW, Some(&good), b"", NOW).await;
        assert_eq!(ok.status, 200, "{:?}", ok.body);
        assert_eq!(ok.body["blocks"][0]["text"], "hello user:alice");

        let missing = server.handle_at("GET", VIEW, None, b"", NOW).await;
        assert_eq!(missing.status, 401, "{:?}", missing.body);
        let expired = sign(&issuer, "app:fake", Some(&view), "va_2", 999_000, 999_060);
        assert_eq!(
            server
                .handle_at("GET", VIEW, Some(&expired), b"", NOW)
                .await
                .status,
            401
        );
        let other_app = sign(
            &issuer,
            "app:other",
            Some(&view),
            "va_3",
            1_000_090,
            1_000_150,
        );
        assert_eq!(
            server
                .handle_at("GET", VIEW, Some(&other_app), b"", NOW)
                .await
                .status,
            401
        );
        // A token for this audience that is not a viewer assertion (a job's
        // identity token could carry `aud = app:fake`).
        let plain = sign(&issuer, "app:fake", None, "va_4", 1_000_090, 1_000_150);
        let refused = server.handle_at("GET", VIEW, Some(&plain), b"", NOW).await;
        assert_eq!(refused.status, 401);
        assert!(refused.body["error"]
            .as_str()
            .unwrap()
            .contains("not a viewer assertion"));
        // Another cluster's key.
        let foreign = sign(
            &Issuer::new(
                ISSUER,
                "/.well-known/jwks.json",
                Duration::from_secs(3600),
                None,
                std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000),
            )
            .unwrap(),
            "app:fake",
            Some(&view),
            "va_5",
            1_000_090,
            1_000_150,
        );
        assert_eq!(
            server
                .handle_at("GET", VIEW, Some(&foreign), b"", NOW)
                .await
                .status,
            401
        );
        // The same assertion on another job's context.
        let mismatch = server
            .handle_at(
                "GET",
                "/legion/v1/views/job?slot=job&namespace=default&job=db",
                Some(&good),
                b"",
                NOW,
            )
            .await;
        assert_eq!(mismatch.status, 403, "{:?}", mismatch.body);
        // ... or for another view.
        let other_view = server
            .handle_at(
                "GET",
                "/legion/v1/views/admin?slot=job&namespace=default&job=web",
                Some(&good),
                b"",
                NOW,
            )
            .await;
        assert_eq!(other_view.status, 403);
        assert_eq!(server.requests_seen(), 8);
    }

    /// An action's handler gets the raw assertion it ran on (to present
    /// back as the viewer) and the delegation it carries; the request's
    /// `Debug` never prints the token, and an assertion without a
    /// delegation makes no viewer client.
    #[tokio::test]
    async fn an_action_gets_its_assertion_and_delegation_test() {
        let issuer = issuer();
        let registry = Arc::new(UiRegistry::default());
        registry.action("go", |request: ActionRequest| async move {
            assert!(!format!("{request:?}").contains(request.assertion()));
            let connection = crate::cmd::helpers::manager_connection_with_token(
                "http://127.0.0.1:1",
                "app-token",
                crate::cmd::helpers::ClientTimeouts {
                    connect: None,
                    request: None,
                },
            )?;
            let viewer_client = request.client_as_viewer_over(&connection).is_ok();
            Ok(ActionOutcome::done(format!(
                "{} {} {viewer_client}",
                request.assertion().len(),
                request
                    .viewer
                    .delegation
                    .as_ref()
                    .map(Delegation::label)
                    .unwrap_or_default()
            )))
        });
        let keys = ClusterKeys {
            issuer: ISSUER.into(),
            jwks: issuer.jwks(),
        };
        let server = UiServer::new(
            registry,
            Some(Arc::new(AssertionVerifier::new(
                "fake",
                Arc::new(StaticKeys(keys)),
            ))),
        );
        let inputs = json!({});
        let mut claim = assertion(AssertionKind::Action, "go", Some(&inputs));
        let body = json!({ "context": context(), "inputs": inputs }).to_string();
        let run = |token: String| {
            let server = &server;
            let body = body.clone();
            async move {
                server
                    .handle_at(
                        "POST",
                        "/legion/v1/actions/go",
                        Some(&token),
                        body.as_bytes(),
                        NOW,
                    )
                    .await
            }
        };
        let token = sign(
            &issuer,
            "app:fake",
            Some(&claim),
            "va_d0",
            1_000_090,
            1_000_150,
        );
        let ran = run(token.clone()).await;
        assert_eq!(ran.status, 200, "{:?}", ran.body);
        assert_eq!(ran.body["message"], format!("{}  false", token.len()));

        claim.delegation = Some(
            Delegation::new(
                crate::structs::acl::AclResource::Job,
                &[crate::structs::acl::AclAction::Write],
                &crate::structs::acl::AclScope::job("web"),
            )
            .unwrap(),
        );
        let token = sign(
            &issuer,
            "app:fake",
            Some(&claim),
            "va_d1",
            1_000_090,
            1_000_150,
        );
        let ran = run(token.clone()).await;
        assert_eq!(ran.status, 200, "{:?}", ran.body);
        assert_eq!(
            ran.body["message"],
            format!("{} job:write on default/web true", token.len())
        );
    }

    /// An action's assertion binds its inputs and is single-use.
    #[tokio::test]
    async fn action_assertions_bind_inputs_and_are_single_use() {
        let issuer = issuer();
        let server = server(&issuer);
        let inputs = json!({ "for": "1h" });
        let claim = assertion(AssertionKind::Action, "go", Some(&inputs));
        let token = sign(
            &issuer,
            "app:fake",
            Some(&claim),
            "va_a",
            1_000_090,
            1_000_150,
        );
        let body = json!({ "context": context(), "inputs": inputs }).to_string();
        let ran = server
            .handle_at(
                "POST",
                "/legion/v1/actions/go",
                Some(&token),
                body.as_bytes(),
                NOW,
            )
            .await;
        assert_eq!(ran.status, 200, "{:?}", ran.body);
        assert_eq!(ran.body["message"], "went \"1h\"");
        let replay = server
            .handle_at(
                "POST",
                "/legion/v1/actions/go",
                Some(&token),
                body.as_bytes(),
                NOW,
            )
            .await;
        assert_eq!(replay.status, 403, "{:?}", replay.body);

        let fresh = sign(
            &issuer,
            "app:fake",
            Some(&claim),
            "va_b",
            1_000_090,
            1_000_150,
        );
        let other_inputs = json!({ "context": context(), "inputs": { "for": "9h" } }).to_string();
        let tampered = server
            .handle_at(
                "POST",
                "/legion/v1/actions/go",
                Some(&fresh),
                other_inputs.as_bytes(),
                NOW,
            )
            .await;
        assert_eq!(tampered.status, 403, "{:?}", tampered.body);
        // A view assertion does not run an action.
        let view = sign(
            &issuer,
            "app:fake",
            Some(&assertion(AssertionKind::View, "go", None)),
            "va_c",
            1_000_090,
            1_000_150,
        );
        assert_eq!(
            server
                .handle_at(
                    "POST",
                    "/legion/v1/actions/go",
                    Some(&view),
                    body.as_bytes(),
                    NOW
                )
                .await
                .status,
            403
        );
    }

    /// A page is served as HTML only to an assertion for that page; a view
    /// assertion, another page's, or a query is refused.
    #[tokio::test]
    async fn pages_need_a_page_assertion() {
        let issuer = issuer();
        let server = server(&issuer);
        server
            .registry
            .page("console", |request: PageRequest| async move {
                Ok(format!("<p>hello {}</p>", request.viewer.principal))
            });
        let mut claim = assertion(AssertionKind::Page, "console", None);
        claim.context = page_context();
        let token = sign(
            &issuer,
            "app:fake",
            Some(&claim),
            "va_p",
            1_000_090,
            1_000_150,
        );
        let page = server
            .handle_at("GET", "/legion/v1/pages/console", Some(&token), b"", NOW)
            .await;
        assert_eq!(page.status, 200, "{:?}", page.body);
        assert_eq!(page.html.as_deref(), Some("<p>hello user:alice</p>"));
        // The same assertion for another page, or with a query.
        let other = server
            .handle_at("GET", "/legion/v1/pages/admin", Some(&token), b"", NOW)
            .await;
        assert_eq!(other.status, 403, "{:?}", other.body);
        let query = server
            .handle_at(
                "GET",
                "/legion/v1/pages/console?slot=job",
                Some(&token),
                b"",
                NOW,
            )
            .await;
        assert_eq!(query.status, 400, "{:?}", query.body);
        // A view assertion does not fetch a page.
        let view = sign(
            &issuer,
            "app:fake",
            Some(&assertion(AssertionKind::View, "console", None)),
            "va_q",
            1_000_090,
            1_000_150,
        );
        let refused = server
            .handle_at("GET", "/legion/v1/pages/console", Some(&view), b"", NOW)
            .await;
        assert_eq!(refused.status, 403, "{:?}", refused.body);
        assert!(refused.html.is_none());
        let missing = server
            .handle_at("GET", "/legion/v1/pages/console", None, b"", NOW)
            .await;
        assert_eq!(missing.status, 401);
    }

    /// Without a way to verify, nothing is served.
    #[tokio::test]
    async fn no_verifier_serves_nothing() {
        let server = UiServer::new(Arc::new(UiRegistry::default()), None);
        assert_eq!(server.handle("GET", VIEW, Some("x"), b"").await.status, 503);
    }
}
