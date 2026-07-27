//! Workload identity spec types, claims, and the `sub` template renderer.
//!
//! See `internal_docs/plans/workload-identity-oidc-plan.md`. This module owns the
//! *spec-level* and *claims-level* primitives:
//!
//! - [`WorkloadIdentity`] — one named identity declared on a task.
//! - [`WorkloadIdentityClaims`] — the scheduling facts the manager turns into a
//!   JWT (the manager derives these; they are never trusted from a worker).
//! - The dumb `{field}` `sub` template renderer + its admission validation,
//!   which is the single most security-sensitive decision in the design: the
//!   rendered `sub` is the only fine-grained routing key AWS IAM can match.
//!
//! The crypto/issuer side (signing, JWKS, discovery doc) lives in
//! `crate::manager::workload_identity`.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::prelude::*;

/// The reserved (built-in) identity name that, when present, the manager issues
/// with `aud` set to the cluster's own issuer so a workload can authenticate
/// back to Legion itself. Reserved here so the spec/issuance path is
/// audience-agnostic and the control-plane identity (a later phase) is a
/// config/authz change rather than a re-architecture.
pub const CONTROL_PLANE_IDENTITY_NAME: &str = "legion";

/// JWT claim namespace for Legion-specific scheduling facts.
pub const LEGION_CLAIM_PREFIX: &str = "legion.io/";

/// Registered JWT claims that operators may never override via `extra_claims`
/// or a `sub` template — overriding them would forge the trust-critical fields.
pub const RESERVED_CLAIMS: &[&str] = &["iss", "sub", "aud", "exp", "iat", "nbf", "jti", "kid"];

/// Signal names the worker can actually deliver for `changeMode: signal`. Kept
/// in sync with `parse_signal` in `crate::worker::task_manager`; admission
/// validates `changeSignal` against this set so a typo fails `job validate`
/// rather than silently going stale in the refresh loop hours later.
pub const SUPPORTED_CHANGE_SIGNALS: &[&str] = &[
    "HUP", "INT", "QUIT", "TERM", "USR1", "USR2", "CONT", "WINCH",
];

/// Normalize a `changeSignal` to its canonical short name (uppercased, optional
/// `SIG` prefix stripped), erroring if it is not one the worker can deliver.
pub fn normalize_change_signal(raw: &str) -> R<&'static str> {
    let trimmed = raw.trim().to_ascii_uppercase();
    let bare = trimmed.strip_prefix("SIG").unwrap_or(&trimmed);
    SUPPORTED_CHANGE_SIGNALS
        .iter()
        .find(|s| **s == bare)
        .copied()
        .with_context(|| {
            format!(
                "unsupported changeSignal {raw:?}; supported signals are {}",
                SUPPORTED_CHANGE_SIGNALS.join(", ")
            )
        })
}

/// Parse an octal file mode string like "0400" (accepts an optional `0o`
/// prefix). Shared by admission validation and worker-side delivery so a bad
/// mode fails `job validate`, not first launch on a node.
pub fn parse_file_mode(raw: &str) -> R<u32> {
    let trimmed = raw.trim().trim_start_matches("0o");
    u32::from_str_radix(trimmed, 8)
        .with_context(|| format!("invalid file mode {raw:?} (octal, e.g. \"0400\")"))
}

/// Placeholders allowed in a `sub` template that are present on every workload.
const STABLE_SUB_FIELDS: &[&str] = &["region", "job", "group", "task"];

/// Placeholders allowed in a `sub` template only for stateful tasks.
const STATEFUL_SUB_FIELDS: &[&str] = &["instance", "slot"];

/// The cluster-default `sub` template. Mirrors Nomad's composed subject minus
/// the namespace segment Legion lacks: stable across reschedules, per-task
/// least privilege.
pub const DEFAULT_SUB_TEMPLATE: &str = "{region}:{job}:{group}:{task}";

/// What happens when an identity's token is re-issued before expiry. Mirrors
/// the existing reconcile-action vocabulary rather than inventing a new one.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ChangeMode {
    /// Just rewrite the delivered token value (atomic file rewrite for files).
    /// Most cloud SDKs re-read the token file on each call, so no reload needed.
    #[default]
    Noop,
    /// Rewrite, then send `change_signal` so a long-running process reloads the
    /// token without restarting. The only way to refresh an env-delivered token.
    Signal,
    /// Rewrite, then restart the allocation/task in place.
    Restart,
}

/// File delivery target for an identity token.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentityFile {
    pub path: String,
    /// Octal file mode string, e.g. "0400". Defaults applied at delivery time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

/// One named workload identity declared on a task. Mirrors Nomad's `identity`
/// blocks: a task may declare several so one workload can federate into AWS
/// *and* Vault with correctly-scoped tokens.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentity {
    /// Unique within the task; the selector used in the issuance RPC and in
    /// delivery paths.
    pub name: String,
    /// One or more audiences. Manager-enforced, never empty: an audience-less
    /// token is replayable against any trusting provider.
    #[serde(default)]
    pub audience: Vec<String>,
    /// Token lifetime (humantime, e.g. "15m"). Falls back to the cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// Deliver the token as this env var.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Deliver the token as a file (recommended; refreshable in place).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<WorkloadIdentityFile>,
    #[serde(default)]
    pub change_mode: ChangeMode,
    /// Signal to send on refresh when `change_mode` is `signal`, e.g. "SIGHUP".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_signal: Option<String>,
    /// Operator-defined static claims merged into the JWT. Reserved keys and the
    /// `legion.io/*` namespace are rejected at validation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_claims: BTreeMap<String, String>,
    /// Per-identity `sub` template override (gated; multi-cloud workloads
    /// sometimes need different subject shapes per provider).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_template: Option<String>,
}

impl WorkloadIdentity {
    /// Whether this identity declares file delivery.
    pub fn delivers_file(&self) -> bool {
        self.file
            .as_ref()
            .is_some_and(|f| !f.path.trim().is_empty())
    }

    /// Whether this identity declares env delivery.
    pub fn delivers_env(&self) -> bool {
        self.env.as_deref().is_some_and(|e| !e.trim().is_empty())
    }

    /// Parse the configured TTL, if any.
    pub fn parsed_ttl(&self) -> R<Option<Duration>> {
        match self.ttl.as_deref() {
            None => Ok(None),
            Some(raw) => {
                let parsed: Duration = raw
                    .parse::<humantime::Duration>()
                    .with_context(|| format!("invalid ttl {raw:?} (use e.g. \"15m\")"))?
                    .into();
                Ok(Some(parsed))
            }
        }
    }

    /// Validate a single identity for a task. `stateful` gates the
    /// `{instance}`/`{slot}` placeholders in a per-identity `sub_template`.
    pub fn validate(&self, task_name: &str, stateful: bool) -> R<()> {
        let scope = format!("task {task_name} identity {:?}", self.name);

        // The name is used as the issuance-RPC selector, in env-file fallback
        // paths (`.legion/identity/<alloc>/<name>/token`), and in delivery
        // directories. A permissive name (`.`/`/`/`..`) would let a jobspec
        // escape the task directory via path traversal, so enforce a strict
        // charset at admission (the worker re-checks before any path join as
        // defense in depth — see `validate_identity_name`).
        validate_identity_name(&self.name)
            .with_context(|| format!("task {task_name} declares an invalid identity name"))?;

        // The reserved control-plane identity `legion` authenticates back to
        // Legion itself, so its audience is always the cluster issuer and the
        // manager owns it — an operator must not set one. Every other identity
        // must declare a non-empty audience (an audience-less token is
        // replayable against any trusting provider).
        if self.name == CONTROL_PLANE_IDENTITY_NAME {
            if !self.audience.is_empty() {
                bail!(
                    "{scope} is the reserved control-plane identity {CONTROL_PLANE_IDENTITY_NAME:?}; \
                     its audience is always the cluster issuer, so leave `audience` unset"
                );
            }
        } else {
            if self.audience.is_empty() {
                bail!("{scope} must declare at least one audience");
            }
            if self.audience.iter().any(|a| a.trim().is_empty()) {
                bail!("{scope} audience entries cannot be empty");
            }
        }

        if !self.delivers_env() && !self.delivers_file() {
            bail!("{scope} must set at least one of env or file delivery");
        }

        // Parse TTL eagerly so a bad value fails admission, not first issuance.
        self.parsed_ttl()
            .with_context(|| format!("{scope} ttl is invalid"))?;

        // Parse the file mode eagerly too, so a bad value fails admission
        // rather than task launch on the node.
        if let Some(mode) = self.file.as_ref().and_then(|f| f.mode.as_deref()) {
            parse_file_mode(mode).with_context(|| format!("{scope} file mode is invalid"))?;
        }

        match self.change_mode {
            ChangeMode::Noop => {
                // env can never refresh in place — fixed at exec time.
                if self.delivers_env() {
                    bail!(
                        "{scope} delivers env with changeMode noop, which can never \
                         refresh; use signal or restart for env delivery"
                    );
                }
            }
            ChangeMode::Signal => {
                match self.change_signal.as_deref() {
                    None => bail!("{scope} changeMode signal requires a non-empty changeSignal"),
                    Some(s) if s.trim().is_empty() => {
                        bail!("{scope} changeMode signal requires a non-empty changeSignal")
                    }
                    // Reject a signal the worker can't deliver at admission,
                    // not silently in the refresh loop.
                    Some(s) => {
                        normalize_change_signal(s)
                            .with_context(|| format!("{scope} changeSignal is invalid"))?;
                    }
                }
            }
            ChangeMode::Restart => {}
        }

        for key in self.extra_claims.keys() {
            validate_extra_claim_key(&scope, key)?;
        }

        if let Some(template) = self.sub_template.as_deref() {
            validate_sub_template(template, stateful)
                .with_context(|| format!("{scope} subTemplate is invalid"))?;
        }

        Ok(())
    }
}

/// Maximum length of an identity `name`.
pub const MAX_IDENTITY_NAME_LEN: usize = 64;

/// Validate an identity `name` against a strict charset: `[A-Za-z0-9_-]{1,64}`.
///
/// The name flows into a filesystem path (the env-delivery backing file at
/// `.legion/identity/<alloc>/<name>/token` and per-identity delivery
/// directories) and into the issuance-RPC selector. Allowing `.`, `/`, or `..`
/// would permit path traversal out of the task directory, so reject anything
/// outside the allow-list. Enforced at admission and re-checked on the worker
/// before any path join (defense in depth).
pub fn validate_identity_name(name: &str) -> R<()> {
    if name.is_empty() {
        bail!("identity name must not be empty");
    }
    if name.len() > MAX_IDENTITY_NAME_LEN {
        bail!("identity name {name:?} is too long (max {MAX_IDENTITY_NAME_LEN} characters)");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!(
            "identity name {name:?} is invalid; names must match [A-Za-z0-9_-] \
             (no '.', '/', '..', whitespace, or other characters) to avoid \
             filesystem path traversal"
        );
    }
    Ok(())
}

/// Reject a `sub`-component value (job, group, or task name) that contains a
/// character which could act as a template separator, collapsing two distinct
/// workloads onto one identity. Enforced at admission only for tasks that
/// declare identities, so jobs that never mint tokens keep their existing
/// naming freedom. Mirrors the keyed-instance-id charset (`[A-Za-z0-9._-]`).
pub fn validate_subject_field(label: &str, value: &str) -> R<()> {
    if value.is_empty() {
        bail!("{label} must not be empty for a task that declares identities");
    }
    if let Some(bad) = value
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        bail!(
            "{label} {value:?} contains {bad:?}, which is not allowed for a task that declares \
             workload identities (allowed: [A-Za-z0-9._-]); the value becomes part of the OIDC \
             `sub`, and a separator character there could collide with another workload's subject"
        );
    }
    Ok(())
}

/// Whether a claim key is one the signer must never let `extra_claims`
/// override: a registered/reserved JWT claim or a key in the `legion.io/*`
/// namespace. Admission already rejects these, but the signing sink re-checks
/// so a derived claim can never be forged even if admission were bypassed.
pub fn is_protected_claim_key(key: &str) -> bool {
    RESERVED_CLAIMS.contains(&key) || key.starts_with(LEGION_CLAIM_PREFIX)
}

/// Reject `extra_claims` keys that would forge a trust-critical claim.
fn validate_extra_claim_key(scope: &str, key: &str) -> R<()> {
    if RESERVED_CLAIMS.contains(&key) {
        bail!("{scope} extra claim {key:?} is a reserved JWT claim and cannot be overridden");
    }
    if key.starts_with(LEGION_CLAIM_PREFIX) {
        bail!("{scope} extra claim {key:?} uses the reserved {LEGION_CLAIM_PREFIX:?} namespace");
    }
    Ok(())
}

/// Validate every identity declared on a task: unique names plus per-identity
/// rules. `stateful` gates the stateful-only `sub` template placeholders.
pub fn validate_identities(
    task_name: &str,
    identities: &[WorkloadIdentity],
    stateful: bool,
) -> R<()> {
    let mut seen = std::collections::HashSet::new();
    // Delivery targets must be unique across the task's identities: two
    // identities writing the same file path (each on its own refresh schedule)
    // would clobber each other with tokens of different audiences, and two
    // sharing an env key is silent last-write-wins. Map target → first
    // identity name so the error can name both sides.
    let mut env_targets: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut file_targets: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for identity in identities {
        identity.validate(task_name, stateful)?;
        if !seen.insert(identity.name.as_str()) {
            bail!(
                "task {task_name} declares duplicate identity name {:?}",
                identity.name
            );
        }
        if identity.delivers_env() {
            let key = identity
                .env
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_string();
            // The `<ENV>_FILE` companion this identity injects must not collide
            // with another identity's env var either.
            let companion = format!("{key}_FILE");
            if let Some(other) = env_targets.insert(key.clone(), identity.name.clone()) {
                bail!(
                    "task {task_name} identities {other:?} and {:?} both deliver env var {key:?}",
                    identity.name
                );
            }
            if let Some(other) = env_targets.insert(companion.clone(), identity.name.clone()) {
                bail!(
                    "task {task_name} env var {companion:?} (the refresh companion of \
                     identity {:?}) collides with identity {other:?}",
                    identity.name
                );
            }
        }
        if identity.delivers_file() {
            let path = identity
                .file
                .as_ref()
                .map(|f| f.path.trim().to_string())
                .unwrap_or_default();
            if let Some(other) = file_targets.insert(path.clone(), identity.name.clone()) {
                bail!(
                    "task {task_name} identities {other:?} and {:?} both deliver to file \
                     path {path:?}",
                    identity.name
                );
            }
        }
    }
    Ok(())
}

/// Validate a `sub` template at admission time.
///
/// `sub` is a security boundary, so a template can *break* the two invariants
/// the system otherwise guarantees structurally (stability across reschedules,
/// uniqueness across workloads). This enforces:
/// - allow-list only (unknown placeholder rejected; `{alloc}`/`{node}` rejected
///   explicitly because they churn);
/// - `{instance}`/`{slot}` only on stateful tasks;
/// - a uniqueness floor (job + group + task must all survive);
/// - at least one placeholder (a literal-only template renders the same `sub`
///   for every workload).
pub fn validate_sub_template(template: &str, stateful: bool) -> R<()> {
    let placeholders = parse_placeholders(template)?;

    if placeholders.is_empty() {
        bail!(
            "sub template {template:?} has no placeholders; it would render the same \
             subject for every workload"
        );
    }

    for field in &placeholders {
        if field == "alloc" || field == "node" {
            bail!(
                "sub template {template:?} uses {{{field}}}, which churns across reschedules \
                 and would destroy reschedule-stable identity; it is available only as a \
                 legion.io/{field} claim"
            );
        }
        let is_stable = STABLE_SUB_FIELDS.contains(&field.as_str());
        let is_stateful = STATEFUL_SUB_FIELDS.contains(&field.as_str());
        if is_stateful && !stateful {
            bail!(
                "sub template {template:?} uses {{{field}}}, which is only valid on tasks \
                 that declare a stateful block"
            );
        }
        if !is_stable && !is_stateful {
            bail!("sub template {template:?} uses unknown placeholder {{{field}}}");
        }
    }

    // Uniqueness floor: dropping below job+group+task granularity collapses
    // distinct workloads into one shared identity. Hard reject (see plan open
    // question #1 — we chose the strict rail).
    for required in ["job", "group", "task"] {
        if !placeholders.iter().any(|p| p == required) {
            bail!(
                "sub template {template:?} drops {{{required}}}; templates must keep at least \
                 job+group+task granularity so distinct workloads stay distinct"
            );
        }
    }

    Ok(())
}

/// The set of characters that act as *separators between two adjacent
/// placeholders* in a template. For the default `{region}:{job}:{group}:{task}`
/// this is `{':'}`; for `legion/{job}/{task}` it is `{'/'}`. A prefix/suffix
/// literal (a placeholder on only one side) is a fixed constant and cannot
/// cause a collision, so its characters are excluded.
///
/// This is the charset a rendered field value must not contain: if `task`
/// could contain `:`, then `(job=a, group=b, task="c:d")` and
/// `(job=a, group="b:c", task=d)` would both render `a:b:c:d`, silently merging
/// two distinct workloads' identities — and `sub` is the only fine-grained key
/// AWS IAM can match, so that collision is a real privilege boundary.
fn separator_chars(template: &str) -> R<std::collections::HashSet<char>> {
    let mut seps = std::collections::HashSet::new();
    let mut rest = template;
    let mut prev_was_field = false;
    while let Some(open) = rest.find('{') {
        let literal = &rest[..open];
        // A literal sits *between* two placeholders only if a field preceded it
        // and one follows it (the `{` we just found).
        if prev_was_field {
            seps.extend(literal.chars());
        }
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .with_context(|| format!("sub template {template:?} has an unclosed '{{'"))?;
        prev_was_field = true;
        rest = &after[close + 1..];
    }
    // Trailing literal after the last field is a suffix constant — ignored.
    Ok(seps)
}

/// Extract the `{field}` placeholder names from a template, in order. Errors on
/// an unclosed brace so a typo fails loud rather than rendering garbage.
fn parse_placeholders(template: &str) -> R<Vec<String>> {
    let mut fields = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .with_context(|| format!("sub template {template:?} has an unclosed '{{'"))?;
        let field = &after[..close];
        if field.is_empty() {
            bail!("sub template {template:?} contains an empty placeholder '{{}}'");
        }
        fields.push(field.to_string());
        rest = &after[close + 1..];
    }
    Ok(fields)
}

/// Persisted form of an OIDC signing key (the `signing_key` table).
///
/// The private key PEM is stored in the manager's SQLite — the same trust
/// domain as the CA material: whoever can read the store can mint identities.
/// Persistence is what keeps externally-held tokens verifiable across manager
/// restarts (same JWKS, same active signer). `state` is one of
/// `active`/`retiring`/`expired`; expired rows are retained for audit but
/// never loaded into the issuer or published in the JWKS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningKeyRecord {
    pub kid: String,
    /// JWS algorithm name, e.g. "ES256".
    pub alg: String,
    /// PKCS#8 PEM of the private key. Never serialized into tokens or RPCs.
    pub private_key_pem: String,
    /// base64url(P-256 affine X coordinate) — the public JWK `x`.
    pub public_jwk_x: String,
    /// base64url(P-256 affine Y coordinate) — the public JWK `y`.
    pub public_jwk_y: String,
    /// Unix seconds the key was generated.
    pub created_at: u64,
    /// Last instant a token signed by this key can still be valid; `None`
    /// while the key is active.
    pub not_after: Option<u64>,
    /// `active` | `retiring` | `expired`.
    pub state: String,
}

/// Claims describing a workload's verifiable identity. The manager derives
/// these from the scheduling record; it never trusts claims sent by a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadIdentityClaims {
    pub region: String,
    pub job: String,
    pub group: String,
    pub task: String,
    pub node_id: String,
    pub alloc_id: String,
    /// Stable per-instance key for stateful workloads (see the stateful plan).
    pub instance: Option<String>,
    /// Slot index for slot-mode stateful workloads.
    pub slot: Option<u32>,
}

impl WorkloadIdentityClaims {
    /// Build the default OIDC `sub` claim. Mirrors Nomad's composed subject
    /// (minus the namespace segment Legion lacks): stable across reschedules,
    /// per-task least privilege. Deliberately omits alloc_id/node_id (they
    /// churn → no stable policy); those ride along as `legion.io/*` claims.
    pub fn subject(&self) -> String {
        format!("{}:{}:{}:{}", self.region, self.job, self.group, self.task)
    }

    /// Whether these claims describe a stateful instance.
    pub fn is_stateful(&self) -> bool {
        self.instance.is_some()
    }

    /// Look up a single template field value. `None` means the placeholder is
    /// not available for this workload (e.g. `{instance}` on a stateless task).
    fn field(&self, name: &str) -> Option<String> {
        match name {
            "region" => Some(self.region.clone()),
            "job" => Some(self.job.clone()),
            "group" => Some(self.group.clone()),
            "task" => Some(self.task.clone()),
            "instance" => self.instance.clone(),
            "slot" => self.slot.map(|s| s.to_string()),
            _ => None,
        }
    }

    /// Render a `sub` from a template (dumb `{field}` substitution over the
    /// stable-field allow-list). Callers should have validated the template
    /// with [`validate_sub_template`] first; this still rejects unknown fields
    /// and an empty render as a defense-in-depth check.
    pub fn render_subject(&self, template: &str) -> R<String> {
        let separators = separator_chars(template)?;
        let mut out = String::with_capacity(template.len());
        let mut rest = template;
        while let Some(open) = rest.find('{') {
            out.push_str(&rest[..open]);
            let after = &rest[open + 1..];
            let close = after
                .find('}')
                .with_context(|| format!("sub template {template:?} has an unclosed '{{'"))?;
            let field = &after[..close];
            let value = self
                .field(field)
                .with_context(|| format!("sub template {template:?} field {{{field}}} is not available for this workload"))?;
            // A field value must not contain a template separator, or two
            // distinct workloads could render an identical `sub` (see
            // `separator_chars`). Admission also rejects such values, but the
            // signer refuses to mint an ambiguous identity even if that were
            // bypassed.
            if let Some(bad) = value.chars().find(|c| separators.contains(c)) {
                bail!(
                    "sub template {template:?} field {{{field}}} value {value:?} contains the \
                     separator {bad:?}, which would let it collide with another workload's subject"
                );
            }
            out.push_str(&value);
            rest = &after[close + 1..];
        }
        out.push_str(rest);

        if out.is_empty() {
            bail!("sub template {template:?} rendered an empty subject");
        }
        Ok(out)
    }

    /// Render the `sub` using `template` if provided, else the built-in
    /// default. Both paths go through [`Self::render_subject`], so the
    /// separator-collision guard applies to the default template too.
    pub fn subject_for(&self, template: Option<&str>) -> R<String> {
        self.render_subject(template.unwrap_or(DEFAULT_SUB_TEMPLATE))
    }

    /// The discrete `legion.io/*` claims, so providers that can match arbitrary
    /// claims (GCP, Vault) don't have to parse `sub`.
    pub fn legion_claims(&self) -> BTreeMap<String, String> {
        let mut claims = BTreeMap::new();
        claims.insert(format!("{LEGION_CLAIM_PREFIX}region"), self.region.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}job"), self.job.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}group"), self.group.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}task"), self.task.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}node"), self.node_id.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}alloc"), self.alloc_id.clone());
        if let Some(instance) = &self.instance {
            claims.insert(format!("{LEGION_CLAIM_PREFIX}instance"), instance.clone());
        }
        if let Some(slot) = self.slot {
            claims.insert(format!("{LEGION_CLAIM_PREFIX}slot"), slot.to_string());
        }
        claims
    }
}

/// Default `sub` template for node identities. Same stability rule as task
/// identities: policy-friendly fields only — the churning node id is
/// claims-only (`legion.io/node`), exactly like `{alloc}` for tasks.
pub const DEFAULT_NODE_SUB_TEMPLATE: &str = "{region}:node:{pool}";

/// Placeholders allowed in a node-identity `sub` template.
const NODE_SUB_FIELDS: &[&str] = &["region", "pool"];

/// One cluster-level identity issued to *worker nodes* rather than tasks
/// (artifact system plan §6). Node identities let store policies say "any
/// Legion worker node in region X, pool Y may read" — the cluster-grain
/// credential that cluster artifact-auth matchers and pre-warming use.
/// Declared under `workloadIdentity.nodeIdentities` in manager config.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeIdentityConfig {
    /// Unique cluster-wide; the selector used in `auth.nodeIdentity` and in
    /// cluster artifact-auth matchers.
    pub name: String,
    /// One or more audiences. Never empty, like task identities.
    #[serde(default)]
    pub audience: Vec<String>,
    /// Token lifetime (humantime). Falls back to the cluster default TTL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// `sub` template override; defaults to [`DEFAULT_NODE_SUB_TEMPLATE`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_template: Option<String>,
}

impl NodeIdentityConfig {
    pub fn parsed_ttl(&self) -> R<Option<Duration>> {
        match self.ttl.as_deref() {
            None => Ok(None),
            Some(raw) => {
                let parsed: Duration = raw
                    .parse::<humantime::Duration>()
                    .with_context(|| format!("invalid ttl {raw:?} (use e.g. \"15m\")"))?
                    .into();
                Ok(Some(parsed))
            }
        }
    }

    pub fn validate(&self) -> R<()> {
        let scope = format!("nodeIdentity {:?}", self.name);
        if self.name.trim().is_empty() {
            bail!("nodeIdentities entries must have a non-empty name");
        }
        if self.audience.is_empty() {
            bail!("{scope} must declare at least one audience");
        }
        if self.audience.iter().any(|a| a.trim().is_empty()) {
            bail!("{scope} audience entries cannot be empty");
        }
        self.parsed_ttl()
            .with_context(|| format!("{scope} ttl is invalid"))?;
        if let Some(template) = self.sub_template.as_deref() {
            validate_node_sub_template(template)
                .with_context(|| format!("{scope} subTemplate is invalid"))?;
        }
        Ok(())
    }
}

/// Validate the cluster's node-identity list: unique names plus per-entry
/// rules.
pub fn validate_node_identities(identities: &[NodeIdentityConfig]) -> R<()> {
    let mut seen = std::collections::HashSet::new();
    for identity in identities {
        identity.validate()?;
        if !seen.insert(identity.name.as_str()) {
            bail!("duplicate nodeIdentity name {:?}", identity.name);
        }
    }
    Ok(())
}

/// Validate a node-identity `sub` template: allow-list only (`{region}` and
/// `{pool}`); `{node}` rejected explicitly because node ids churn — the node
/// id rides along as a `legion.io/node` claim, never in `sub`.
pub fn validate_node_sub_template(template: &str) -> R<()> {
    let placeholders = parse_placeholders(template)?;
    if placeholders.is_empty() {
        bail!(
            "node sub template {template:?} has no placeholders; it would render the same \
             subject for every cluster"
        );
    }
    for field in &placeholders {
        if field == "node" {
            bail!(
                "node sub template {template:?} uses {{node}}, which churns as nodes are \
                 replaced; it is available only as a legion.io/node claim"
            );
        }
        if !NODE_SUB_FIELDS.contains(&field.as_str()) {
            bail!("node sub template {template:?} uses unknown placeholder {{{field}}}");
        }
    }
    Ok(())
}

/// Claims describing a worker node's verifiable identity. The manager derives
/// these from the node's registration record at issuance time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIdentityClaims {
    pub region: String,
    pub pool: String,
    pub node_id: String,
}

impl NodeIdentityClaims {
    /// Render the `sub` from a template, defaulting to
    /// [`DEFAULT_NODE_SUB_TEMPLATE`].
    pub fn subject_for(&self, template: Option<&str>) -> R<String> {
        let template = template.unwrap_or(DEFAULT_NODE_SUB_TEMPLATE);
        let mut out = String::with_capacity(template.len());
        let mut rest = template;
        while let Some(open) = rest.find('{') {
            out.push_str(&rest[..open]);
            let after = &rest[open + 1..];
            let close = after
                .find('}')
                .with_context(|| format!("node sub template {template:?} has an unclosed '{{'"))?;
            let value = match &after[..close] {
                "region" => self.region.clone(),
                "pool" => self.pool.clone(),
                field => bail!(
                    "node sub template {template:?} field {{{field}}} is not available for \
                     node identities"
                ),
            };
            out.push_str(&value);
            rest = &after[close + 1..];
        }
        out.push_str(rest);
        if out.is_empty() {
            bail!("node sub template {template:?} rendered an empty subject");
        }
        Ok(out)
    }

    /// The discrete `legion.io/*` claims for a node token. `legion.io/scope`
    /// is `"node"` so relying parties can distinguish node tokens from task
    /// tokens issued by the same cluster; `legion.io/node` is audit-only.
    pub fn legion_claims(&self) -> BTreeMap<String, String> {
        let mut claims = BTreeMap::new();
        claims.insert(format!("{LEGION_CLAIM_PREFIX}scope"), "node".to_string());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}region"), self.region.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}pool"), self.pool.clone());
        claims.insert(format!("{LEGION_CLAIM_PREFIX}node"), self.node_id.clone());
        claims
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> WorkloadIdentityClaims {
        WorkloadIdentityClaims {
            region: "auto".into(),
            job: "api".into(),
            group: "web".into(),
            task: "server".into(),
            node_id: "node-1".into(),
            alloc_id: "alloc-42".into(),
            instance: None,
            slot: None,
        }
    }

    fn identity(name: &str) -> WorkloadIdentity {
        WorkloadIdentity {
            name: name.into(),
            audience: vec!["sts.amazonaws.com".into()],
            file: Some(WorkloadIdentityFile {
                // Per-identity path so distinct identities don't collide on a
                // shared delivery target (validate_identities rejects that).
                path: format!("/var/run/legion/identity/{name}/token"),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn default_subject_matches_nomad_shape() {
        assert_eq!(claims().subject(), "auto:api:web:server");
    }

    #[test]
    fn legion_claims_expose_discrete_facts() {
        let c = claims().legion_claims();
        assert_eq!(c["legion.io/region"], "auto");
        assert_eq!(c["legion.io/alloc"], "alloc-42");
        assert_eq!(c["legion.io/node"], "node-1");
        assert!(!c.contains_key("legion.io/instance"));
    }

    #[test]
    fn stateful_claims_carry_instance_and_slot() {
        let c = WorkloadIdentityClaims {
            instance: Some("api-web-0".into()),
            slot: Some(0),
            ..claims()
        };
        assert!(c.is_stateful());
        let lc = c.legion_claims();
        assert_eq!(lc["legion.io/instance"], "api-web-0");
        assert_eq!(lc["legion.io/slot"], "0");
    }

    #[test]
    fn template_renders_known_fields() {
        let s = claims().render_subject("legion/{job}/{task}").unwrap();
        assert_eq!(s, "legion/api/server");
    }

    #[test]
    fn default_template_equals_builtin_subject() {
        let s = claims().render_subject(DEFAULT_SUB_TEMPLATE).unwrap();
        assert_eq!(s, claims().subject());
    }

    #[test]
    fn render_rejects_unknown_field() {
        let err = claims().render_subject("{job}:{bogus}").unwrap_err();
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn render_rejects_unavailable_stateful_field() {
        // instance is unset on a stateless workload.
        let err = claims()
            .render_subject("{job}:{group}:{task}:{instance}")
            .unwrap_err();
        assert!(err.to_string().contains("instance"));
    }

    #[test]
    fn subject_for_falls_back_to_default() {
        assert_eq!(claims().subject_for(None).unwrap(), "auto:api:web:server");
    }

    // --- sub template admission validation ---

    #[test]
    fn validate_template_accepts_default() {
        validate_sub_template(DEFAULT_SUB_TEMPLATE, false).unwrap();
    }

    #[test]
    fn validate_template_rejects_alloc_and_node() {
        for field in ["{alloc}", "{node}"] {
            let t = format!("{{job}}:{{group}}:{{task}}:{field}");
            let err = validate_sub_template(&t, false).unwrap_err();
            assert!(err.to_string().contains("churns"), "{}", err);
        }
    }

    #[test]
    fn validate_template_rejects_unknown_placeholder() {
        let err = validate_sub_template("{job}:{group}:{task}:{tenant}", false).unwrap_err();
        assert!(err.to_string().contains("unknown placeholder"));
    }

    #[test]
    fn validate_template_rejects_below_uniqueness_floor() {
        let err = validate_sub_template("legion/{job}", false).unwrap_err();
        assert!(err.to_string().contains("granularity"));
    }

    #[test]
    fn validate_template_rejects_literal_only() {
        let err = validate_sub_template("static-subject", false).unwrap_err();
        assert!(err.to_string().contains("no placeholders"));
    }

    #[test]
    fn validate_template_rejects_unclosed_brace() {
        let err = validate_sub_template("{job}:{group}:{task", false).unwrap_err();
        assert!(err.to_string().contains("unclosed"));
    }

    #[test]
    fn validate_template_gates_instance_on_stateful() {
        let t = "{job}:{group}:{task}:{instance}";
        assert!(validate_sub_template(t, false).is_err());
        validate_sub_template(t, true).unwrap();
    }

    // --- identity validation ---

    #[test]
    fn identity_requires_audience() {
        let mut id = identity("aws");
        id.audience.clear();
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("audience"));
    }

    #[test]
    fn identity_requires_a_delivery_mode() {
        let mut id = identity("aws");
        id.file = None;
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("delivery"));
    }

    #[test]
    fn env_with_noop_change_mode_is_rejected() {
        let id = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Noop,
            ..Default::default()
        };
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("can never"));
    }

    #[test]
    fn signal_change_mode_requires_change_signal() {
        let id = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Signal,
            change_signal: None,
            ..Default::default()
        };
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("changeSignal"));
    }

    #[test]
    fn env_with_signal_change_mode_is_allowed() {
        let id = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Signal,
            change_signal: Some("SIGHUP".into()),
            ..Default::default()
        };
        id.validate("api", false).unwrap();
    }

    #[test]
    fn extra_claims_reject_reserved_and_namespaced_keys() {
        for key in ["sub", "aud", "legion.io/region"] {
            let mut id = identity("aws");
            id.extra_claims.insert(key.into(), "x".into());
            let err = id.validate("api", false).unwrap_err();
            assert!(err.to_string().contains(key), "{}", err);
        }
    }

    #[test]
    fn extra_claims_allow_custom_namespaced_keys() {
        let mut id = identity("aws");
        id.extra_claims
            .insert("app.example.com/tier".into(), "backend".into());
        id.validate("api", false).unwrap();
    }

    #[test]
    fn bad_ttl_is_rejected_at_validation() {
        let mut id = identity("aws");
        id.ttl = Some("15".into()); // missing unit
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("ttl"));
    }

    #[test]
    fn identity_name_rejects_path_traversal_and_bad_charset() {
        for bad in [
            "../evil",
            "a/b",
            "..",
            "",
            &"x".repeat(65),
            "has space",
            "a.b",
        ] {
            assert!(
                validate_identity_name(bad).is_err(),
                "expected {bad:?} to be rejected"
            );
            let mut id = identity("aws");
            id.name = bad.to_string();
            assert!(
                id.validate("api", false).is_err(),
                "expected identity name {bad:?} to be rejected at admission"
            );
        }
    }

    #[test]
    fn identity_name_accepts_valid_charset() {
        for good in [
            "aws",
            "vault-prod",
            "gcp_west",
            "a",
            &"x".repeat(64),
            "legion",
        ] {
            validate_identity_name(good).unwrap_or_else(|e| panic!("{good:?}: {e}"));
            let mut id = identity("aws");
            id.name = good.to_string();
            // The reserved `legion` name is control-plane-shaped: no operator
            // audience (the manager owns it).
            if good == CONTROL_PLANE_IDENTITY_NAME {
                id.audience.clear();
            }
            id.validate("api", false)
                .unwrap_or_else(|e| panic!("{good:?}: {e}"));
        }
    }

    #[test]
    fn duplicate_identity_names_rejected() {
        let ids = vec![identity("aws"), identity("aws")];
        let err = validate_identities("api", &ids, false).unwrap_err();
        assert!(err.to_string().contains("duplicate identity"));
    }

    #[test]
    fn distinct_identities_validate() {
        let ids = vec![identity("aws"), identity("gcp")];
        validate_identities("api", &ids, false).unwrap();
    }

    // --- reserved control-plane identity `legion` (Phase 7) ---

    #[test]
    fn control_plane_identity_rejects_operator_audience() {
        // The reserved `legion` identity's audience is manager-owned (the
        // cluster issuer); setting one is rejected.
        let mut id = identity(CONTROL_PLANE_IDENTITY_NAME);
        id.audience = vec!["sts.amazonaws.com".into()];
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("control-plane"), "{err}");
    }

    #[test]
    fn control_plane_identity_validates_without_audience() {
        let id = WorkloadIdentity {
            name: CONTROL_PLANE_IDENTITY_NAME.into(),
            audience: Vec::new(),
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/legion/token".into(),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        };
        id.validate("api", false).unwrap();
    }

    #[test]
    fn duplicate_file_delivery_target_rejected() {
        let mut a = identity("aws");
        let mut b = identity("gcp");
        a.file.as_mut().unwrap().path = "/run/legion/token".into();
        b.file.as_mut().unwrap().path = "/run/legion/token".into();
        let err = validate_identities("api", &[a, b], false).unwrap_err();
        assert!(err.to_string().contains("file path"), "{err}");
    }

    #[test]
    fn duplicate_env_delivery_target_rejected() {
        let env_id = |name: &str| WorkloadIdentity {
            name: name.into(),
            audience: vec!["a".into()],
            env: Some("TOKEN".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        };
        let err = validate_identities("api", &[env_id("aws"), env_id("gcp")], false).unwrap_err();
        assert!(err.to_string().contains("env var"), "{err}");
    }

    #[test]
    fn env_target_colliding_with_companion_rejected() {
        // Identity A injects TOKEN + TOKEN_FILE; identity B uses TOKEN_FILE.
        let a = WorkloadIdentity {
            name: "a".into(),
            audience: vec!["x".into()],
            env: Some("TOKEN".into()),
            change_mode: ChangeMode::Signal,
            change_signal: Some("SIGHUP".into()),
            ..Default::default()
        };
        let b = WorkloadIdentity {
            name: "b".into(),
            audience: vec!["x".into()],
            env: Some("TOKEN_FILE".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        };
        // B's env key collides with A's `<ENV>_FILE` refresh companion, so the
        // pair is rejected (the companion would otherwise clobber B's token).
        let err = validate_identities("api", &[a, b], false).unwrap_err();
        assert!(err.to_string().contains("TOKEN_FILE"), "{err}");
    }

    #[test]
    fn invalid_change_signal_rejected_at_admission() {
        for bad in ["SIGUSR3", "SIGHUPP", "not-a-signal"] {
            let id = WorkloadIdentity {
                name: "vault".into(),
                audience: vec!["vault.internal".into()],
                env: Some("VAULT_TOKEN".into()),
                change_mode: ChangeMode::Signal,
                change_signal: Some(bad.into()),
                ..Default::default()
            };
            let err = id.validate("api", false).unwrap_err();
            assert!(err.to_string().contains("changeSignal"), "{bad}: {err}");
        }
    }

    #[test]
    fn supported_change_signals_accepted() {
        for good in ["SIGHUP", "hup", "sigterm", "USR1"] {
            let id = WorkloadIdentity {
                name: "vault".into(),
                audience: vec!["vault.internal".into()],
                env: Some("VAULT_TOKEN".into()),
                change_mode: ChangeMode::Signal,
                change_signal: Some(good.into()),
                ..Default::default()
            };
            id.validate("api", false)
                .unwrap_or_else(|e| panic!("{good}: {e}"));
        }
    }

    #[test]
    fn bad_file_mode_rejected_at_validation() {
        let mut id = identity("aws");
        id.file.as_mut().unwrap().mode = Some("rw-".into());
        let err = id.validate("api", false).unwrap_err();
        assert!(err.to_string().contains("file mode"), "{err}");
        // Valid octal modes still pass (with and without the 0o prefix).
        for good in ["0400", "0o600", "640"] {
            let mut ok = identity("aws");
            ok.file.as_mut().unwrap().mode = Some(good.into());
            ok.validate("api", false)
                .unwrap_or_else(|e| panic!("{good}: {e}"));
        }
    }

    // --- sub delimiter-injection defense (C20) ---

    #[test]
    fn render_rejects_value_containing_separator() {
        // task value carrying the ':' separator would collide with another
        // (job, group, task) tuple.
        let c = WorkloadIdentityClaims {
            task: "server:extra".into(),
            ..claims()
        };
        let err = c.subject_for(None).unwrap_err();
        assert!(err.to_string().contains("separator"), "{err}");
    }

    #[test]
    fn render_allows_prefix_literal_chars_in_values() {
        // "legion/" is a prefix constant; a value containing its letters is
        // fine — only true between-placeholder separators ('/') are rejected.
        let s = claims()
            .render_subject("legion/{job}/{group}/{task}")
            .unwrap();
        assert_eq!(s, "legion/api/web/server");
        let c = WorkloadIdentityClaims {
            job: "a/b".into(),
            ..claims()
        };
        let err = c.render_subject("legion/{job}/{group}/{task}").unwrap_err();
        assert!(err.to_string().contains("separator"), "{err}");
    }

    #[test]
    fn subject_field_charset_rejects_delimiters() {
        for bad in ["a:b", "a/b", "a b"] {
            assert!(
                validate_subject_field("job name", bad).is_err(),
                "expected {bad:?} rejected"
            );
        }
        for good in ["api", "web-1", "team.svc", "a_b"] {
            validate_subject_field("job name", good).unwrap_or_else(|e| panic!("{good}: {e}"));
        }
    }

    // --- node identities (artifact system plan §6) ---

    fn node_identity(name: &str) -> NodeIdentityConfig {
        NodeIdentityConfig {
            name: name.into(),
            audience: vec!["sts.amazonaws.com".into()],
            ttl: Some("15m".into()),
            sub_template: None,
        }
    }

    #[test]
    fn node_identity_validates_with_defaults() {
        validate_node_identities(&[node_identity("artifact-pull")]).unwrap();
    }

    #[test]
    fn node_identity_requires_audience() {
        let mut id = node_identity("pull");
        id.audience.clear();
        let err = id.validate().unwrap_err();
        assert!(err.to_string().contains("audience"), "{err}");
    }

    #[test]
    fn duplicate_node_identity_names_rejected() {
        let err =
            validate_node_identities(&[node_identity("pull"), node_identity("pull")]).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn node_sub_template_allows_region_and_pool_only() {
        validate_node_sub_template("{region}:node:{pool}").unwrap();
        validate_node_sub_template("legion-{region}-{pool}").unwrap();

        // {node} churns and is claims-only, exactly like {alloc} for tasks.
        let err = validate_node_sub_template("{region}:{node}").unwrap_err();
        assert!(err.to_string().contains("legion.io/node"), "{err}");

        // Task placeholders are not valid in node scope.
        let err = validate_node_sub_template("{region}:{job}").unwrap_err();
        assert!(err.to_string().contains("unknown placeholder"), "{err}");

        // Literal-only templates render the same subject everywhere.
        let err = validate_node_sub_template("static").unwrap_err();
        assert!(err.to_string().contains("no placeholders"), "{err}");
    }

    #[test]
    fn node_claims_render_subject_and_scope() -> crate::prelude::R<()> {
        let claims = NodeIdentityClaims {
            region: "prod".into(),
            pool: "default".into(),
            node_id: "node-abc".into(),
        };
        assert_eq!(claims.subject_for(None)?, "prod:node:default");
        assert_eq!(
            claims.subject_for(Some("legion-{region}-{pool}"))?,
            "legion-prod-default"
        );

        let legion_claims = claims.legion_claims();
        assert_eq!(
            legion_claims.get("legion.io/scope").map(String::as_str),
            Some("node")
        );
        assert_eq!(
            legion_claims.get("legion.io/node").map(String::as_str),
            Some("node-abc")
        );
        assert_eq!(
            legion_claims.get("legion.io/pool").map(String::as_str),
            Some("default")
        );
        Ok(())
    }
}
