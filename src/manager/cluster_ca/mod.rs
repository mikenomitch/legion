//! The cluster CA provider seam (docs/cluster-ca.md).
//!
//! One CA anchors everything Legion's processes present to each other: node
//! and fleet certificates (the worker plane and the mesh), each manager's
//! own client and server certificates, and the pin every join token
//! carries. Which CA that is, and where its signing key lives, is the
//! manager's `clusterCa.provider`:
//!
//! - **`builtin`** (default, [`builtin`]): generated on first use and
//!   stored, key included, in the replicated `net_mesh_ca` row; rotated by
//!   `legion net ca rotate`.
//! - **`file`** ([`file`]): an operator-provided CA certificate and key on
//!   each manager's disk, typically an intermediate under their root, plus
//!   an optional trust bundle. The key never enters the store.
//! - **`exec`** ([`exec`]): an external signer (Vault PKI, step-ca, ...)
//!   spoken to over JSON on stdin/stdout, like the keyring's `exec` root
//!   key provider. Legion never holds the key.
//!
//! **Legion decides the names; the provider only signs.** Every request
//! carries a [`LeafSpec`] built from the authenticated caller, and whatever a
//! non-builtin provider returns is checked against it before it is handed
//! out ([`verify`]). Verification on every *holder* was already
//! issuer-agnostic (`crate::cluster_tls`: a trust bundle, roles in URI
//! SANs), so nothing there changes.
//!
//! [`ClusterCa`] wraps the configured provider and is held by the `Store`.
//! Every reader of the trust bundle, the pin, or the chain goes through it,
//! never through `net_mesh_ca` directly. It also owns the multi-manager
//! agreement check: the control-lane leader records its provider's view in
//! `cluster_ca_record`, and every other manager refuses to sign while its
//! own view and that record would not trust each other.

pub mod builtin;
pub mod conformance;
pub mod exec;
pub mod file;
pub mod verify;

use crate::manager::mesh_ca::{generate_csr, CsrSubject, LeafSpec, ManagerCertKind};
use crate::prelude::*;
use crate::storage::netpolicy_store::{CaRotationPhase, ClusterCaRecord};
use crate::storage::store::Store;
use crate::structs::config::{
    ClusterCaConfigFull, CLUSTER_CA_BUILTIN, CLUSTER_CA_EXEC, CLUSTER_CA_FILE,
};
use std::time::{Duration, Instant};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// How far ahead of this manager's clock a returned certificate's
/// `notBefore` may be.
pub const MAX_NOT_BEFORE_SKEW: Duration = Duration::from_secs(5 * 60);
/// How much longer than requested a provider may make a certificate.
pub const MAX_LIFETIME_SLACK: Duration = Duration::from_secs(10 * 60);
/// The shortest certificate Legion accepts from a provider. Holders
/// re-issue with under an hour left, so anything shorter would re-issue on
/// every heartbeat.
pub const MIN_LEAF_LIFETIME: Duration = Duration::from_secs(2 * 60 * 60);
/// How long an agreement verdict is reused before the record is read again.
const AGREEMENT_RECHECK: Duration = Duration::from_secs(5);

/// The cluster CA as one provider sees it right now: everything a reader
/// needs, with no key material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaView {
    /// `builtin` | `file` | `exec`.
    pub provider: &'static str,
    /// Where the CA comes from, for messages: the paths, the command, or
    /// empty for `builtin`.
    pub location: String,
    /// The CA certificate that signs leaves, PEM.
    pub signing_cert_pem: String,
    /// Every CA a holder trusts, PEM, the anchor first.
    pub bundle_pem: String,
    /// The certificates a leaf carries after itself, PEM: the intermediates
    /// between it and the anchor (`builtin`: the cross-signed CA during a
    /// rotation's overlap). Empty when the signing CA is itself in the
    /// bundle.
    pub chain_pem: String,
    /// The pin (`sha256:<hex>`) join tokens carry: the bundle certificate
    /// the signing CA chains to (the signing CA itself for `builtin`).
    pub pin: String,
    /// The pin of every certificate in `bundle_pem`, in order.
    pub bundle_pins: Vec<String>,
    /// When the signing CA was created (`builtin`) or its `notBefore`
    /// (others), RFC 3339.
    pub created_at: String,
    /// Where a built-in rotation stands; always `Stable` for other
    /// providers, which rotate outside Legion.
    pub phase: CaRotationPhase,
    /// `builtin` during a rotation's overlap: the previous CA.
    pub previous_cert_pem: Option<String>,
}

impl CaView {
    /// What a manager sends after its server certificate: the chain, then
    /// every trusted CA, so a worker holding only its join token's pin finds
    /// the pinned CA in the handshake (`cluster_tls::PinnedManagerVerifier`).
    pub fn server_chain_suffix(&self) -> String {
        format!("{}{}", self.chain_pem, self.bundle_pem)
    }

    /// Identifies this view for caches: the signing CA, the chain, and the
    /// bundle. Two views with the same fingerprint issue interchangeable
    /// certificates.
    pub fn fingerprint(&self) -> String {
        let signing = crate::cluster_tls::ca_pin_pem(&self.signing_cert_pem).unwrap_or_default();
        let chain = crate::cluster_tls::bundle_pins(&self.chain_pem).join(",");
        format!("{signing}|{chain}|{}", self.bundle_pins.join(","))
    }

    /// This view as a [`ClusterCaRecord`] written by `manager_id`.
    pub fn record(&self, manager_id: &str) -> ClusterCaRecord {
        ClusterCaRecord {
            provider: self.provider.to_string(),
            anchor_pin: self.pin.clone(),
            bundle_pins: self.bundle_pins.clone(),
            recorded_by: manager_id.to_string(),
            recorded_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_default(),
        }
    }

    /// `"<provider> (<location>)"`, or `builtin`.
    pub fn describe_source(&self) -> String {
        describe_source(self.provider, &self.location)
    }
}

fn describe_source(provider: &str, location: &str) -> String {
    if location.is_empty() {
        provider.to_string()
    } else {
        format!("{provider} ({location})")
    }
}

/// Why the cluster CA could not do what was asked. Each kind maps to one
/// gRPC code, so callers see a stable, actionable error.
#[derive(Debug)]
pub enum CaError {
    /// The caller's request is invalid (a malformed CSR).
    InvalidRequest(String),
    /// Not available on this manager yet, or the provider could not be
    /// reached: retrying may succeed (`Unavailable`).
    Unavailable(String),
    /// An operator has to fix something: a provider returned a certificate
    /// Legion refuses, CA files are invalid, this manager disagrees with the
    /// cluster, or the provider cannot be rotated by Legion
    /// (`FailedPrecondition`).
    FailedPrecondition(String),
    /// The state store or a local signature failed.
    Internal(anyhow::Error),
}

impl std::fmt::Display for CaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CaError::InvalidRequest(message)
            | CaError::Unavailable(message)
            | CaError::FailedPrecondition(message) => f.write_str(message),
            CaError::Internal(err) => write!(f, "{err:#}"),
        }
    }
}

impl std::error::Error for CaError {}

impl CaError {
    /// The gRPC status for this error.
    pub fn to_status(&self) -> tonic::Status {
        match self {
            CaError::InvalidRequest(message) => tonic::Status::invalid_argument(message.clone()),
            CaError::Unavailable(message) => tonic::Status::unavailable(message.clone()),
            CaError::FailedPrecondition(message) => {
                tonic::Status::failed_precondition(message.clone())
            }
            CaError::Internal(err) => tonic::Status::internal(format!("{err:#}")),
        }
    }
}

/// A leaf the cluster CA issued, as a holder installs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issued {
    /// The leaf certificate, PEM.
    pub leaf_pem: String,
    /// What follows the leaf in a chain, PEM (see [`CaView::chain_pem`]).
    pub chain_pem: String,
    /// The trust bundle, PEM, anchor first.
    pub bundle_pem: String,
    /// Expiry as Unix seconds.
    pub expires_at: i64,
    /// `notBefore` as Unix seconds.
    pub not_before: i64,
}

impl Issued {
    /// The leaf followed by its chain: what a holder presents.
    pub fn chain(&self) -> String {
        format!("{}{}", self.leaf_pem, self.chain_pem)
    }

    /// The leaf, its chain, and every trusted CA: what a manager presents
    /// to a worker that may hold only its join token's pin.
    pub fn server_chain(&self) -> String {
        format!("{}{}{}", self.leaf_pem, self.chain_pem, self.bundle_pem)
    }

    /// The certificate's lifetime.
    pub fn lifetime(&self) -> Duration {
        Duration::from_secs((self.expires_at - self.not_before).max(0) as u64)
    }
}

/// Where a cluster CA's signing key lives. Implementations hold no store
/// state of their own: `builtin` reads the replicated row, `file` reads the
/// manager's disk, `exec` asks an external command.
pub trait ClusterCaProvider: Send + Sync {
    /// `builtin` | `file` | `exec`.
    fn kind(&self) -> &'static str;
    /// Where the CA comes from, for status output and messages: the paths,
    /// the command, or empty for `builtin`.
    fn location(&self) -> String;
    /// The CA as of now. `Ok(None)` only from `builtin` before its CA
    /// exists.
    fn view(&self, store: &Store) -> Result<Option<CaView>, CaError>;
    /// The CA, created first when this provider can create one (`builtin`,
    /// on the control-lane leader).
    fn ensure(&self, store: &Store) -> Result<CaView, CaError>;
    /// Sign `csr_pem` (already parsed and proven) as `spec`. Returns the
    /// provider's PEM (the leaf, optionally followed by intermediates) and
    /// the view it signed under.
    fn sign(
        &self,
        store: &Store,
        csr_pem: &str,
        spec: &LeafSpec,
    ) -> Result<(String, CaView), CaError>;
    /// Rotate the CA (`legion net ca rotate`). Providers that rotate outside
    /// Legion refuse with the procedure.
    fn rotate(&self, store: &Store) -> Result<CaView, CaError>;
    /// Re-read the CA now, ignoring caches. `Ok(None)` when there is
    /// nothing to re-read.
    fn reload(&self, _store: &Store) -> Result<Option<CaView>, CaError> {
        Ok(None)
    }
    /// A problem the provider knows about while still serving a view (a
    /// failed reload or `describe` while the last good CA is kept).
    fn problem(&self) -> Option<String> {
        None
    }
    /// Whether signing happens in this process, cheaply. `exec` signs
    /// through a command and is kept off latency-sensitive paths.
    fn signs_locally(&self) -> bool {
        true
    }
    /// Whether Legion must verify what [`Self::sign`] returns before
    /// handing it out. Only the built-in signer, which builds the
    /// certificate from the spec itself, is exempt.
    fn verifies_output(&self) -> bool {
        true
    }
    /// Load or warm up at manager start. An error stops the manager.
    fn start(&self) -> R<()> {
        Ok(())
    }
}

/// What `legion net ca show` reports: the view, if any, and anything wrong.
#[derive(Debug, Clone)]
pub struct CaStatus {
    pub provider: &'static str,
    pub location: String,
    pub view: Option<CaView>,
    /// Why this manager will not sign right now, or what is degraded.
    pub problem: Option<String>,
}

/// One agreement verdict, reused for [`AGREEMENT_RECHECK`].
struct AgreementCheck {
    at: Instant,
    fingerprint: String,
    outcome: Result<(), String>,
}

struct Inner {
    provider: Arc<dyn ClusterCaProvider>,
    agreement: Mutex<Option<AgreementCheck>>,
}

/// The configured cluster CA provider plus what every provider shares:
/// the agreement check, CSR-based manager certificates, and verification
/// of whatever a provider returns. Clones share state.
#[derive(Clone)]
pub struct ClusterCa {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ClusterCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCa")
            .field("provider", &self.kind())
            .field("location", &self.location())
            .finish()
    }
}

impl ClusterCa {
    /// The provider named by the resolved `clusterCa` block. Validates the
    /// block; reads nothing ([`Self::start`] does).
    pub fn from_config(config: &ClusterCaConfigFull) -> R<ClusterCa> {
        config.validate()?;
        let provider: Arc<dyn ClusterCaProvider> = match config.provider.as_str() {
            CLUSTER_CA_BUILTIN => Arc::new(builtin::BuiltinCa),
            CLUSTER_CA_FILE => Arc::new(file::FileCa::new(
                config.cert_path.clone().into(),
                config.key_path.clone().into(),
                (!config.bundle_path.is_empty()).then(|| config.bundle_path.clone().into()),
            )),
            CLUSTER_CA_EXEC => Arc::new(exec::ExecCa::new(
                config.command.clone(),
                config.timeout_duration(),
                config.refresh_duration(),
            )),
            other => bail!("clusterCa.provider `{other}` is not one of builtin, file, exec"),
        };
        Ok(Self::with_provider(provider))
    }

    /// A cluster CA backed by `provider`.
    pub fn with_provider(provider: Arc<dyn ClusterCaProvider>) -> ClusterCa {
        ClusterCa {
            inner: Arc::new(Inner {
                provider,
                agreement: Mutex::new(None),
            }),
        }
    }

    pub fn kind(&self) -> &'static str {
        self.inner.provider.kind()
    }

    pub fn location(&self) -> String {
        self.inner.provider.location()
    }

    pub fn is_builtin(&self) -> bool {
        self.kind() == CLUSTER_CA_BUILTIN
    }

    /// Whether signing is local and cheap (`builtin`, `file`).
    pub fn signs_locally(&self) -> bool {
        self.inner.provider.signs_locally()
    }

    /// Load the provider at manager start: `file` reads and self-tests its
    /// files and fails the start when they are unusable; `exec` warms its
    /// `describe` cache in the background and never fails the start.
    pub fn start(&self) -> R<()> {
        self.inner.provider.start()
    }

    /// The CA as this manager sees it, checked against the cluster's
    /// record. `Ok(None)` when the built-in CA does not exist yet.
    pub fn current(&self, store: &Store) -> Result<Option<CaView>, CaError> {
        let Some(view) = self.inner.provider.view(store)? else {
            return Ok(None);
        };
        self.agree(store, &view)?;
        Ok(Some(view))
    }

    /// The CA, created first when the provider can (`builtin` on the
    /// control-lane leader), checked against the cluster's record.
    pub fn ensure(&self, store: &Store) -> Result<CaView, CaError> {
        let view = self.inner.provider.ensure(store)?;
        self.agree(store, &view)?;
        Ok(view)
    }

    /// [`Self::ensure`], waiting up to `timeout` while the provider is not
    /// available yet (an `exec` signer's first `describe` still running at
    /// manager start). Blocking; call it off the async runtime.
    pub fn ensure_waiting(&self, store: &Store, timeout: Duration) -> Result<CaView, CaError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.ensure(store) {
                Err(CaError::Unavailable(_)) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(200));
                }
                other => return other,
            }
        }
    }

    /// The view and anything wrong with it, for `GetMeshCa`. Never fails:
    /// a problem is reported, not returned.
    pub fn status(&self, store: &Store) -> CaStatus {
        let provider = self.inner.provider.kind();
        let location = self.inner.provider.location();
        match self.inner.provider.view(store) {
            Ok(Some(view)) => {
                let problem = self
                    .agree(store, &view)
                    .err()
                    .map(|err| err.to_string())
                    .or_else(|| self.inner.provider.problem());
                CaStatus {
                    provider,
                    location,
                    view: Some(view),
                    problem,
                }
            }
            Ok(None) => CaStatus {
                provider,
                location,
                view: None,
                problem: self.inner.provider.problem(),
            },
            Err(err) => CaStatus {
                provider,
                location,
                view: None,
                problem: Some(err.to_string()),
            },
        }
    }

    /// Sign a node's or fleet's CSR. The names come from `subject` (the
    /// authenticated caller), never from the CSR.
    pub fn sign_csr(
        &self,
        store: &Store,
        csr_pem: &str,
        subject: &CsrSubject,
    ) -> Result<Issued, CaError> {
        self.sign(store, csr_pem, &LeafSpec::for_subject(subject))
    }

    /// A certificate this manager mints for itself: a fresh key and CSR,
    /// signed through the provider. Returns the issued chain and the key
    /// PEM, which never leaves this process.
    pub fn issue_manager_cert(
        &self,
        store: &Store,
        manager_id: &str,
        kind: ManagerCertKind,
    ) -> Result<(Issued, String), CaError> {
        let spec = LeafSpec::for_manager(manager_id, kind);
        let (key_pem, csr_pem) = generate_csr(&spec.common_name)
            .map_err(|err| CaError::Internal(err.context("generating a manager key")))?;
        let issued = self.sign(store, &csr_pem, &spec)?;
        Ok((issued, key_pem))
    }

    /// Sign `csr_pem` as `spec` and, for every provider but `builtin`,
    /// verify what came back before handing it out.
    pub fn sign(&self, store: &Store, csr_pem: &str, spec: &LeafSpec) -> Result<Issued, CaError> {
        verify::check_csr(csr_pem).map_err(CaError::InvalidRequest)?;
        let before = self.ensure(store)?;
        let provider = &self.inner.provider;
        let kind = provider.kind();
        let (returned, view) = match provider.sign(store, csr_pem, spec) {
            Ok(signed) => signed,
            Err(err) => {
                count_call(kind, "sign", "error");
                return Err(err);
            }
        };
        // The provider may have reloaded between the check above and the
        // signature: the view it signed under must agree too.
        if view.fingerprint() != before.fingerprint() {
            self.agree(store, &view)?;
        }
        if !provider.verifies_output() {
            count_call(kind, "sign", "ok");
            return verify::assemble_trusted(&returned, &view)
                .map_err(|err| CaError::Internal(anyhow!("{err}")));
        }
        let now = OffsetDateTime::now_utc();
        let verified = match verify::verify_issued(spec, csr_pem, &returned, &view, now) {
            Err(refusal) if refusal.is_chain_failure() => {
                // An issuer rotated inside the external CA, or CA files
                // replaced a moment ago: re-read once before refusing.
                match provider.reload(store)? {
                    Some(fresh) if fresh.fingerprint() != view.fingerprint() => {
                        self.agree(store, &fresh)?;
                        verify::verify_issued(spec, csr_pem, &returned, &fresh, now)
                    }
                    _ => Err(refusal),
                }
            }
            other => other,
        };
        match verified {
            Ok(issued) => {
                count_call(kind, "sign", "ok");
                Ok(issued)
            }
            Err(refusal) => {
                count_call(kind, "sign", "refused");
                let message = format!(
                    "the {} cluster CA provider returned a certificate Legion will not hand \
                     out: {refusal}",
                    view.describe_source()
                );
                warn!(
                    provider = kind,
                    role = spec.role.as_str(),
                    reason = %refusal,
                    "Refused a certificate from the cluster CA provider"
                );
                Err(CaError::FailedPrecondition(message))
            }
        }
    }

    /// Rotate the CA (`legion net ca rotate`). Only `builtin` can; the
    /// others refuse with the procedure for that provider.
    pub fn rotate(&self, store: &Store) -> Result<CaView, CaError> {
        let view = self.inner.provider.rotate(store)?;
        // A rotation is a view change on the leader: record it now rather
        // than on the next background pass.
        self.record(store);
        Ok(view)
    }

    /// On the control-lane leader, make the cluster's record follow this
    /// manager's view (docs/cluster-ca.md, "Every manager must agree"). A
    /// replicated write when the record is missing or stale, so it runs off
    /// the request path: the manager's certificate refresher calls it at
    /// start and every minute, and a rotation calls it once. A follower, or
    /// a CA that does not exist yet, does nothing.
    pub fn record(&self, store: &Store) {
        if !store.is_raft_leader(crate::storage::Lane::Control) {
            return;
        }
        if let Ok(Some(view)) = self.inner.provider.view(store) {
            let _ = self.agree_as(store, &view, true, true);
        }
    }

    /// Check `view` against the cluster's record, caching the verdict for
    /// a few seconds. Read-only: on the control-lane leader this manager's
    /// view is the cluster's, and [`Self::record`] writes it down.
    fn agree(&self, store: &Store, view: &CaView) -> Result<(), CaError> {
        let fingerprint = view.fingerprint();
        {
            let cached = self
                .inner
                .agreement
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(check) = cached.as_ref().filter(|check| {
                check.fingerprint == fingerprint && check.at.elapsed() < AGREEMENT_RECHECK
            }) {
                return check.outcome.clone().map_err(CaError::FailedPrecondition);
            }
        }
        let leader = store.is_raft_leader(crate::storage::Lane::Control);
        let outcome = self.agree_as(store, view, leader, false);
        *self
            .inner
            .agreement
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(AgreementCheck {
            at: Instant::now(),
            fingerprint,
            outcome: outcome.clone(),
        });
        outcome.map_err(CaError::FailedPrecondition)
    }

    /// [`Self::agree`] without the cache, with leadership given. A leader
    /// agrees with itself, and writes its view as the cluster's record when
    /// `may_record` is set and the record differs ([`Self::record`]).
    pub(crate) fn agree_as(
        &self,
        store: &Store,
        view: &CaView,
        leader: bool,
        may_record: bool,
    ) -> Result<(), String> {
        let manager_id = crate::manager::worker_plane::WorkerPlane::global().manager_id();
        let mine = view.record(&manager_id);
        let recorded = match store.cluster_ca_record() {
            Ok(recorded) => recorded,
            Err(err) => {
                debug!(error = ?err, "Could not read the cluster CA record");
                return Ok(());
            }
        };
        if leader {
            if may_record
                && !recorded
                    .as_ref()
                    .is_some_and(|record| record.same_ca(&mine))
            {
                match store.record_cluster_ca(&mine) {
                    Ok(()) => info!(
                        provider = %mine.provider,
                        anchor = %crate::cluster_tls::short_pin(&mine.anchor_pin),
                        bundle = mine.bundle_pins.len(),
                        "Recorded this manager's cluster CA as the cluster's"
                    ),
                    Err(err) => {
                        debug!(error = ?err, "Could not record the cluster CA view")
                    }
                }
            }
            return Ok(());
        }
        match recorded {
            None => Ok(()),
            Some(record) => {
                let verdict = check_agreement(&mine, &record);
                if let Err(reason) = &verdict {
                    warn!(%reason, "This manager's cluster CA disagrees with the cluster's");
                }
                verdict
            }
        }
    }
}

/// Whether this manager's view (`mine`) and the cluster's record agree
/// (docs/cluster-ca.md, "Every manager must agree"):
///
/// - a `builtin` manager agrees only with a `builtin` record: both read the
///   same replicated row, so nothing else needs comparing;
/// - a `file`/`exec` manager never agrees with a `builtin` record;
/// - two `file`/`exec` views agree when they trust each other: each one's
///   anchor is in the other's bundle. That admits every step of a rotation
///   (add the new CA to the bundle, switch the signing CA, drop the old
///   one) and refuses a manager whose certificates the rest would reject.
pub fn check_agreement(mine: &ClusterCaRecord, cluster: &ClusterCaRecord) -> Result<(), String> {
    let by = format!(
        "recorded by manager {} at {}",
        cluster.recorded_by, cluster.recorded_at
    );
    let mine_builtin = mine.provider == CLUSTER_CA_BUILTIN;
    let cluster_builtin = cluster.provider == CLUSTER_CA_BUILTIN;
    match (mine_builtin, cluster_builtin) {
        (true, true) => Ok(()),
        (true, false) => Err(format!(
            "this manager uses the built-in cluster CA, but the cluster's CA comes from the \
             `{}` provider ({by}); give this manager the same `clusterCa` block as the others \
             (docs/cluster-ca.md#every-manager-must-agree)",
            cluster.provider
        )),
        (false, true) => Err(format!(
            "this manager's cluster CA comes from the `{}` provider, but the cluster uses the \
             built-in CA ({by}); give every manager the same `clusterCa` block \
             (docs/cluster-ca.md#every-manager-must-agree)",
            mine.provider
        )),
        (false, false) => {
            let they_trust_mine = cluster.bundle_pins.contains(&mine.anchor_pin);
            let i_trust_theirs = mine.bundle_pins.contains(&cluster.anchor_pin);
            if they_trust_mine && i_trust_theirs {
                return Ok(());
            }
            let short = crate::cluster_tls::short_pin;
            let mut gaps = Vec::new();
            if !they_trust_mine {
                gaps.push(format!(
                    "the cluster's trust bundle lacks this manager's CA {}",
                    short(&mine.anchor_pin)
                ));
            }
            if !i_trust_theirs {
                gaps.push(format!(
                    "this manager's trust bundle lacks the cluster's CA {}",
                    short(&cluster.anchor_pin)
                ));
            }
            Err(format!(
                "this manager's cluster CA ({}, anchor {}) and the cluster's ({}, anchor {}, \
                 {by}) do not trust each other: {}. Certificates this manager signed would be \
                 refused by the rest of the cluster, so it signs nothing until they agree. Put \
                 the same CA on every manager, or keep both CAs in every manager's trust bundle \
                 while you rotate (docs/cluster-ca.md#rotation)",
                mine.provider,
                short(&mine.anchor_pin),
                cluster.provider,
                short(&cluster.anchor_pin),
                gaps.join("; ")
            ))
        }
    }
}

/// Count one provider call for `legion_cluster_ca_provider_calls_total`.
pub(crate) fn count_call(provider: &str, op: &str, outcome: &str) {
    crate::manager::telemetry::metrics()
        .cluster_ca_provider_calls_total
        .inc(&[provider, op, outcome]);
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
