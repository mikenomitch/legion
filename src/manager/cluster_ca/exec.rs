//! `exec`: an external signer over JSON on stdin/stdout (docs/cluster-ca.md,
//! "The exec provider"), with the conventions of the keyring's `exec` root
//! key provider: one JSON document on stdin per invocation, one JSON
//! document on stdout, and a non-zero exit, a timeout, or malformed output
//! is a provider error.
//!
//! ```json
//! {"op":"describe"}
//! → {"bundle_pem":"<every CA holders trust>","chain_pem":"<the signing CA, then intermediates toward the bundle>"}
//!
//! {"op":"sign","role":"node","csr_pem":"…","common_name":"node-1.node.legion",
//!  "dns_sans":["node-1.node.legion"],"uri_sans":["legion://node/node-1"],
//!  "ttl_seconds":86400,"usages":["server_auth","client_auth"]}
//! → {"cert_pem":"<the leaf, optionally followed by its intermediates>"}
//! ```
//!
//! `describe` is cached for `refreshInterval` and re-run in the background
//! when stale (the stale answer keeps serving meanwhile), so after the
//! first call no request waits on it. A failed refresh keeps the last good
//! answer and reports the problem. `sign` runs per certificate: on a node's
//! issuance (every 8 hours per node) and on a manager's own re-mint, never
//! on a heartbeat.

use super::{verify, CaError, CaView, ClusterCaProvider};
use crate::manager::mesh_ca::LeafSpec;
use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::config::CLUSTER_CA_EXEC;
use std::time::{Duration, Instant};

/// How often a provider that has never answered is asked again, on demand.
const FIRST_DESCRIBE_RETRY: Duration = Duration::from_secs(5);

#[derive(Default)]
struct State {
    /// The last good `describe` answer and when it was fetched.
    view: Option<(CaView, Instant)>,
    /// A refresh is running.
    refreshing: bool,
    /// When the last `describe` started.
    last_attempt: Option<Instant>,
    /// Why the last `describe` failed, while the last good one is kept.
    problem: Option<String>,
    /// Whether that failure needs an operator (a bad answer) rather than a
    /// retry (a signer that did not run or answer).
    problem_needs_operator: bool,
}

struct Inner {
    command: Vec<String>,
    timeout: Duration,
    refresh: Duration,
    state: Mutex<State>,
}

/// The `exec` provider. Clones share the cache.
#[derive(Clone)]
pub struct ExecCa {
    inner: Arc<Inner>,
}

/// Run one request against `command`.
pub fn call(command: &[String], request: &Value, timeout: Duration) -> R<Value> {
    crate::util::run_json_command("cluster CA provider", command, request, timeout)
}

fn reply_field<'a>(reply: &'a Value, field: &str, op: &str) -> R<&'a str> {
    reply
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("cluster CA provider `{op}` reply is missing `{field}`"))
}

/// The `describe` request and its reply as a view. A helper that cannot be
/// run is `Unavailable`; one that answers with something that is not a
/// usable CA is `FailedPrecondition`.
pub fn describe(command: &[String], timeout: Duration) -> Result<CaView, CaError> {
    let reply = call(command, &json!({ "op": "describe" }), timeout)
        .map_err(|err| CaError::Unavailable(format!("{err:#}")))?;
    let bundle =
        reply_field(&reply, "bundle_pem", "describe").map_err(|err| bad_reply(command, err))?;
    let chain =
        reply_field(&reply, "chain_pem", "describe").map_err(|err| bad_reply(command, err))?;
    verify::assemble_view(CLUSTER_CA_EXEC, command.join(" "), chain, bundle).map_err(|err| {
        CaError::FailedPrecondition(format!(
            "cluster CA provider `{}` describe: {err}",
            command.join(" ")
        ))
    })
}

fn bad_reply(command: &[String], err: anyhow::Error) -> CaError {
    CaError::FailedPrecondition(format!("{err:#} (provider `{}`)", command.join(" ")))
}

/// The `sign` request for `spec`.
pub fn sign_request(csr_pem: &str, spec: &LeafSpec) -> Value {
    json!({
        "op": "sign",
        "role": spec.role.as_str(),
        "csr_pem": csr_pem,
        "common_name": spec.common_name,
        "dns_sans": spec.dns_names,
        "uri_sans": spec.uris,
        "ttl_seconds": spec.ttl.as_secs(),
        "usages": spec.usages(),
    })
}

/// Run `sign` and return the provider's `cert_pem`.
pub fn sign(
    command: &[String],
    timeout: Duration,
    csr_pem: &str,
    spec: &LeafSpec,
) -> Result<String, CaError> {
    let reply = call(command, &sign_request(csr_pem, spec), timeout)
        .map_err(|err| CaError::Unavailable(format!("{err:#}")))?;
    reply_field(&reply, "cert_pem", "sign")
        .map(str::to_string)
        .map_err(|err| bad_reply(command, err))
}

impl ExecCa {
    pub fn new(command: Vec<String>, timeout: Duration, refresh: Duration) -> ExecCa {
        ExecCa {
            inner: Arc::new(Inner {
                command,
                timeout,
                refresh,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// `describe` now, updating the cache: the answer, or the error with
    /// the last good answer kept.
    fn refresh_now(&self) -> Result<CaView, CaError> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_attempt = Some(Instant::now());
        let result = describe(&self.inner.command, self.inner.timeout);
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        state.refreshing = false;
        match result {
            Ok(view) => {
                super::count_call(CLUSTER_CA_EXEC, "describe", "ok");
                let changed = state
                    .view
                    .as_ref()
                    .is_none_or(|(old, _)| old.fingerprint() != view.fingerprint());
                if changed {
                    info!(
                        command = %self.inner.command.join(" "),
                        anchor = %crate::cluster_tls::short_pin(&view.pin),
                        bundle = view.bundle_pins.len(),
                        "Read the cluster CA from the exec provider"
                    );
                }
                if state.problem.take().is_some() {
                    info!("The cluster CA provider answers again");
                }
                state.view = Some((view.clone(), Instant::now()));
                Ok(view)
            }
            Err(err) => {
                super::count_call(CLUSTER_CA_EXEC, "describe", "error");
                let message = err.to_string();
                if state.problem.as_deref() != Some(message.as_str()) {
                    warn!(error = %message, "Cluster CA provider describe failed");
                }
                state.problem_needs_operator = matches!(err, CaError::FailedPrecondition(_));
                state.problem = Some(match state.view {
                    Some(_) => format!(
                        "the provider's describe failed, so this manager keeps the CA it read \
                         last: {message}"
                    ),
                    None => message,
                });
                Err(err)
            }
        }
    }

    /// The cached view; stale ones are refreshed in the background while
    /// they keep serving. Before the first answer, only the very first call
    /// in the process waits for `describe` (a manager makes that call in the
    /// background at start); every later one gets `Unavailable` at once and
    /// at most every few seconds triggers a background retry, so request
    /// paths never pile up behind a signer that is down.
    fn cached(&self) -> Result<CaView, CaError> {
        let spawn = {
            let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
            match state.view.as_ref() {
                Some((view, fetched)) => {
                    let view = view.clone();
                    let stale = fetched.elapsed() >= self.inner.refresh;
                    let spawn = stale && !state.refreshing;
                    if spawn {
                        state.refreshing = true;
                    }
                    Some((view, spawn))
                }
                None if state.last_attempt.is_none() && !state.refreshing => {
                    state.refreshing = true;
                    None
                }
                None => {
                    let retry = !state.refreshing
                        && state
                            .last_attempt
                            .is_none_or(|at| at.elapsed() >= FIRST_DESCRIBE_RETRY);
                    if retry {
                        state.refreshing = true;
                    }
                    let error = match &state.problem {
                        Some(problem) if state.problem_needs_operator => {
                            CaError::FailedPrecondition(problem.clone())
                        }
                        Some(problem) => CaError::Unavailable(problem.clone()),
                        None => CaError::Unavailable(format!(
                            "the cluster CA provider `{}` has not answered its first describe \
                             yet",
                            self.inner.command.join(" ")
                        )),
                    };
                    drop(state);
                    if retry {
                        self.refresh_in_background();
                    }
                    return Err(error);
                }
            }
        };
        match spawn {
            Some((view, spawn)) => {
                if spawn {
                    self.refresh_in_background();
                }
                Ok(view)
            }
            None => self.refresh_now(),
        }
    }

    /// Run `describe` on a thread of its own; the caller has set
    /// `refreshing`.
    fn refresh_in_background(&self) {
        let this = self.clone();
        let spawned = std::thread::Builder::new()
            .name("cluster-ca-describe".into())
            .spawn(move || {
                if let Err(err) = this.refresh_now() {
                    debug!(error = %err, "Background cluster CA describe failed");
                }
            });
        if spawned.is_err() {
            self.inner
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .refreshing = false;
        }
    }
}

impl ClusterCaProvider for ExecCa {
    fn kind(&self) -> &'static str {
        CLUSTER_CA_EXEC
    }

    fn location(&self) -> String {
        self.inner.command.join(" ")
    }

    fn view(&self, _store: &Store) -> Result<Option<CaView>, CaError> {
        Ok(Some(self.cached()?))
    }

    fn ensure(&self, _store: &Store) -> Result<CaView, CaError> {
        self.cached()
    }

    fn sign(
        &self,
        _store: &Store,
        csr_pem: &str,
        spec: &LeafSpec,
    ) -> Result<(String, CaView), CaError> {
        let view = self.cached()?;
        let pem = sign(&self.inner.command, self.inner.timeout, csr_pem, spec)?;
        Ok((pem, view))
    }

    fn rotate(&self, _store: &Store) -> Result<CaView, CaError> {
        Err(CaError::FailedPrecondition(format!(
            "the cluster CA comes from the `exec` provider (`{}`); Legion cannot rotate it. \
             Rotate it in the external CA: make the new CA part of the trust bundle the \
             provider's describe reports, wait one heartbeat, switch the CA it signs with, \
             then drop the old CA from the bundle a day later. With the Vault PKI helper that \
             is: create a new issuer in the mount, make it the default, and delete the old \
             issuer a day later (docs/cluster-ca.md#rotation)",
            self.location()
        )))
    }

    fn reload(&self, _store: &Store) -> Result<Option<CaView>, CaError> {
        self.refresh_now().map(Some)
    }

    fn problem(&self) -> Option<String> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .problem
            .clone()
    }

    fn signs_locally(&self) -> bool {
        false
    }

    fn start(&self) -> R<()> {
        // Never fails the start: a signer that is down at boot is retried
        // on use, and reported meanwhile (`refresh_now` logs the failure).
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.view.is_some() || state.refreshing {
                return Ok(());
            }
            state.refreshing = true;
            // Counts as the first attempt: requests from now on never wait.
            state.last_attempt = Some(Instant::now());
        }
        self.refresh_in_background();
        Ok(())
    }
}
