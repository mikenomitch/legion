//! Artifact spec types shared by every task kind.
//!
//! See `docs/plans/artifact-system-plan.md` and the user-facing
//! `docs/artifacts.md`. This module owns the *spec-level* primitives:
//!
//! - [`ArtifactSpec`] — one declared artifact on a task (string shorthand or
//!   full object form).
//! - [`ArtifactAuth`] — the per-artifact auth override (KV secret, task
//!   workload identity, or node workload identity; exactly one).
//! - Source parsing/redaction, checksum/dest/name validation, and the
//!   `${artifact.<name>}` interpolation token helpers used both at admission
//!   (reference validation) and on the worker (substitution).
//!
//! Fetching/staging lives in `crate::artifacts`; these types are kept in
//! `structs` so the job spec, store, and proto plumbing stay self-contained.

use std::collections::BTreeSet;
use std::path::{Component, Path};

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use crate::prelude::*;
use crate::structs::workload_identity::WorkloadIdentity;

lazy_static! {
    /// Matches `${artifact.<name>}` interpolation tokens inside string config
    /// fields. Names share the artifact-name charset.
    static ref ARTIFACT_TOKEN_RE: Regex =
        Regex::new(r"\$\{artifact\.([a-z0-9_-]+)\}").expect("artifact token regex");
}

/// Whether (and how) a fetched artifact is unpacked into the task directory.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactUnpack {
    /// Unpack when the source basename has a recognized archive extension.
    #[default]
    Auto,
    /// Force unpacking; fail when the payload is not a readable archive.
    Archive,
    /// Never unpack; stage the raw file.
    File,
}

/// When a mutable reference (OCI tag, git ref) is resolved to a digest.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ArtifactResolve {
    /// Resolve at submit; every allocation of the job config fetches the
    /// pinned digest. The default for mutable references.
    #[default]
    Pin,
    /// Re-resolve at every staging: allocations track the tag, at the
    /// documented cost that one job config may run different bytes.
    Always,
}

/// Auth override for one artifact. Exactly one of `kv` / `identity` /
/// `nodeIdentity` must be set; `assumeRole` may only accompany `identity`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactAuth {
    /// KV path holding a credential JSON object (see docs/artifacts.md for
    /// the credential shapes). Read by the worker at fetch time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv: Option<String>,
    /// Name of an entry in the task's `identities` list; the worker presents
    /// that workload-identity token (or exchanges it) when fetching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    /// Optional AWS role ARN exchanged via `AssumeRoleWithWebIdentity` when
    /// `identity` is used against `s3://` sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role: Option<String>,
    /// Name of a cluster-level node identity (`workloadIdentity.nodeIdentities`);
    /// the fetch authenticates as the cluster's worker nodes, not as the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_identity: Option<String>,
}

impl ArtifactAuth {
    /// Admission validation: exactly one auth mechanism, `assumeRole` only
    /// alongside `identity`.
    pub fn validate(&self) -> R<()> {
        let set = [
            self.kv.is_some(),
            self.identity.is_some(),
            self.node_identity.is_some(),
        ]
        .iter()
        .filter(|present| **present)
        .count();
        if set != 1 {
            bail!("artifact auth must set exactly one of kv, identity, or nodeIdentity");
        }
        if self.assume_role.is_some() && self.identity.is_none() {
            bail!("artifact auth assumeRole requires identity");
        }
        for (field, value) in [
            ("kv", &self.kv),
            ("identity", &self.identity),
            ("nodeIdentity", &self.node_identity),
        ] {
            if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                bail!("artifact auth {field} cannot be empty");
            }
        }
        Ok(())
    }

    /// Human-readable auth *source* for status surfaces and errors. Never
    /// includes credential values — only the KV path or identity name.
    pub fn describe(&self) -> String {
        if let Some(kv) = self.kv.as_deref() {
            return format!("kv: {kv}");
        }
        if let Some(identity) = self.identity.as_deref() {
            return format!("identity: {identity}");
        }
        if let Some(node_identity) = self.node_identity.as_deref() {
            return format!("nodeIdentity: {node_identity}");
        }
        "anonymous".to_string()
    }
}

/// One declared artifact on a task.
///
/// Deserializes from either a bare source string
/// (`"https://example.com/tool.tar.gz"`) or the full object form. The
/// `pinned_from` / `auth_origin` fields are normalization annotations written
/// by the manager at admission; user specs never need to set them.
#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactSpec {
    /// Source URL; the scheme picks the fetcher (`https://`, `http://`,
    /// `file://`, `git://` or `git::https://…`, `s3://`, `oci://`).
    pub source: String,
    /// Optional handle for `${artifact.<name>}` interpolation and the
    /// `LEGION_ARTIFACT_<NAME>` env var. Lowercase `[a-z0-9_-]+`, unique
    /// within the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Destination relative to the task directory. Defaults to `.` for
    /// archives/trees and the source basename for single files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dest: Option<String>,
    /// `sha256:<64 hex>`. Presence makes the artifact immutable and
    /// verification mandatory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "is_default_unpack")]
    pub unpack: ArtifactUnpack,
    /// Pin-vs-track for mutable references. Unset means `pin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolve: Option<ArtifactResolve>,
    /// Auth override; omitted means cluster `artifacts.auth` matchers apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ArtifactAuth>,
    /// Octal chmod (for example "0755") applied to single-file artifacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// For ORAS multi-file artifacts: select layers by media type. Omit to
    /// take the whole artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Set by admission normalization when `resolve: pin` rewrote `source`
    /// to a digest: the original mutable reference, for UI provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_from: Option<String>,
    /// Set by admission normalization: where the effective `auth` came from
    /// (`"task"` or `"cluster"`). Unset means anonymous.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_origin: Option<String>,
}

fn is_default_unpack(unpack: &ArtifactUnpack) -> bool {
    *unpack == ArtifactUnpack::Auto
}

/// Mirror of [`ArtifactSpec`] used for object-form deserialization.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ArtifactSpecObject {
    source: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    dest: Option<String>,
    #[serde(default)]
    checksum: Option<String>,
    #[serde(default)]
    unpack: ArtifactUnpack,
    #[serde(default)]
    resolve: Option<ArtifactResolve>,
    #[serde(default)]
    auth: Option<ArtifactAuth>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    pinned_from: Option<String>,
    #[serde(default)]
    auth_origin: Option<String>,
}

impl From<ArtifactSpecObject> for ArtifactSpec {
    fn from(input: ArtifactSpecObject) -> Self {
        ArtifactSpec {
            source: input.source,
            name: input.name,
            dest: input.dest,
            checksum: input.checksum,
            unpack: input.unpack,
            resolve: input.resolve,
            auth: input.auth,
            mode: input.mode,
            media_type: input.media_type,
            pinned_from: input.pinned_from,
            auth_origin: input.auth_origin,
        }
    }
}

impl<'de> Deserialize<'de> for ArtifactSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Input {
            Source(String),
            Full(Box<ArtifactSpecObject>),
        }

        match Input::deserialize(deserializer)? {
            Input::Source(source) => Ok(ArtifactSpec {
                source,
                ..ArtifactSpec::default()
            }),
            Input::Full(full) => Ok((*full).into()),
        }
    }
}

/// Source after scheme normalization: `git::https://…` becomes scheme `git`
/// with the inner transport URL preserved.
#[derive(Debug, Clone)]
pub struct ParsedSource {
    pub scheme: String,
    pub url: Url,
}

/// Parses an artifact source, normalizing the go-getter style `git::` prefix.
pub fn parse_artifact_source(source: &str) -> R<ParsedSource> {
    if let Some(inner) = source.strip_prefix("git::") {
        let url = Url::parse(inner)
            .with_context(|| format!("invalid git artifact source {}", redact_source(source)))?;
        return Ok(ParsedSource {
            scheme: "git".into(),
            url,
        });
    }
    let url = Url::parse(source)
        .with_context(|| format!("invalid artifact source {}", redact_source(source)))?;
    Ok(ParsedSource {
        scheme: url.scheme().to_string(),
        url,
    })
}

/// Redacts a source for logs, errors, and status surfaces: strips userinfo
/// and the entire query/fragment (presigned URLs carry secrets in query
/// values). Unparseable sources are passed through scheme-only.
pub fn redact_source(source: &str) -> String {
    let (prefix, raw) = match source.strip_prefix("git::") {
        Some(inner) => ("git::", inner),
        None => ("", source),
    };
    let Ok(mut url) = Url::parse(raw) else {
        return format!(
            "{}{}",
            prefix,
            raw.split(['?', '@']).next().unwrap_or("<unparseable>")
        );
    };
    let had_query = url.query().is_some();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    if had_query {
        format!("{prefix}{url}?…")
    } else {
        format!("{prefix}{url}")
    }
}

/// Validates an artifact name: lowercase `[a-z0-9_-]+`.
pub fn validate_artifact_name(name: &str) -> R<()> {
    if name.is_empty() {
        bail!("artifact name cannot be empty");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        bail!("artifact name {name} must match [a-z0-9_-]+");
    }
    Ok(())
}

/// Validates a checksum reference: `sha256:<64 lowercase hex>`.
pub fn validate_artifact_checksum(checksum: &str) -> R<()> {
    let Some(hex) = checksum.strip_prefix("sha256:") else {
        bail!("artifact checksum must be sha256:<hex> (got {checksum})");
    };
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("artifact checksum must carry 64 hex digits after sha256:");
    }
    Ok(())
}

/// Validates a `dest`: relative, normalizes to something inside the task
/// directory (`..` may never escape it).
pub fn validate_artifact_dest(dest: &str) -> R<()> {
    let path = Path::new(dest);
    if path.is_absolute() {
        bail!("artifact dest must be relative to the task directory (got {dest})");
    }
    let mut depth: i64 = 0;
    for component in path.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    bail!("artifact dest {dest} escapes the task directory");
                }
            }
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => {
                bail!("artifact dest must be relative to the task directory (got {dest})");
            }
        }
    }
    Ok(())
}

impl ArtifactSpec {
    /// Convenience constructor for kind-implicit artifacts (for example the
    /// container `image` desugar).
    pub fn from_source(source: impl Into<String>) -> Self {
        ArtifactSpec {
            source: source.into(),
            ..ArtifactSpec::default()
        }
    }

    /// The implicit OCI image artifact a container task's `image` field
    /// desugars into. Docker Hub shorthand (`alpine:latest`,
    /// `acme/api:v1`) expands to a fully-qualified `docker.io` reference so
    /// the `oci://` source always carries a registry host.
    pub fn oci_image(image: &str) -> Self {
        let source = if image.contains("://") {
            image.to_string()
        } else {
            let first_segment = image.split('/').next().unwrap_or("");
            // A registry host is only present before a `/`; a colon in a
            // single-segment image (`alpine:latest`) is the tag, not a port.
            let has_registry = image.contains('/')
                && (first_segment.contains('.')
                    || first_segment.contains(':')
                    || first_segment == "localhost");
            if has_registry {
                format!("oci://{image}")
            } else if image.contains('/') {
                format!("oci://docker.io/{image}")
            } else {
                format!("oci://docker.io/library/{image}")
            }
        };
        ArtifactSpec {
            source,
            name: Some("image".into()),
            ..ArtifactSpec::default()
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Effective resolve mode (`pin` unless explicitly set).
    pub fn effective_resolve(&self) -> ArtifactResolve {
        self.resolve.unwrap_or_default()
    }

    /// Whether the source is a mutable reference that `resolve: pin` should
    /// turn into a digest at submit. Checksummed artifacts are already
    /// immutable; OCI digests and git full-SHA refs need no resolution.
    pub fn is_mutable_reference(&self) -> R<bool> {
        if self.checksum.is_some() {
            return Ok(false);
        }
        let parsed = parse_artifact_source(&self.source)?;
        match parsed.scheme.as_str() {
            "oci" => Ok(!self.source.contains("@sha256:")),
            "git" => {
                let pinned = parsed
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "ref")
                    .map(|(_, value)| {
                        value.len() == 40 && value.chars().all(|c| c.is_ascii_hexdigit())
                    })
                    .unwrap_or(false);
                Ok(!pinned)
            }
            _ => Ok(false),
        }
    }

    /// Redacted source for status surfaces.
    pub fn redacted_source(&self) -> String {
        redact_source(&self.source)
    }

    /// `LEGION_ARTIFACT_<NAME>` env var name for a named artifact.
    pub fn env_var_name(&self) -> Option<String> {
        self.name
            .as_deref()
            .map(|name| format!("LEGION_ARTIFACT_{}", name.to_uppercase().replace('-', "_")))
    }

    /// Auth description for status surfaces (names only, never values).
    pub fn auth_description(&self) -> String {
        match (&self.auth, self.auth_origin.as_deref()) {
            (Some(auth), Some("cluster")) => format!("{} (cluster default)", auth.describe()),
            (Some(auth), _) => auth.describe(),
            (None, _) => "anonymous".to_string(),
        }
    }

    /// Spec-shape validation that needs no cluster config: source, name,
    /// dest, checksum, mode, and auth one-of rules.
    pub fn validate(&self) -> R<()> {
        let parsed = parse_artifact_source(&self.source)?;
        if parsed.scheme == "oci" && parsed.url.host_str().is_none() {
            bail!(
                "artifact source {} must include a registry host",
                self.redacted_source()
            );
        }
        if let Some(name) = self.name.as_deref() {
            validate_artifact_name(name)?;
        }
        if let Some(dest) = self.dest.as_deref() {
            validate_artifact_dest(dest)?;
        }
        if let Some(checksum) = self.checksum.as_deref() {
            validate_artifact_checksum(checksum)?;
        }
        if let Some(mode) = self.mode.as_deref() {
            u32::from_str_radix(mode.trim_start_matches('0'), 8)
                .ok()
                .with_context(|| format!("artifact mode {mode} must be an octal file mode"))?;
        }
        if let Some(auth) = self.auth.as_ref() {
            auth.validate()?;
        }
        if let Some(media_type) = self.media_type.as_deref() {
            if media_type.trim().is_empty() {
                bail!("artifact mediaType cannot be empty");
            }
            if parsed.scheme != "oci" {
                bail!("artifact mediaType is only valid for oci:// sources");
            }
        }
        Ok(())
    }
}

/// Collects every `${artifact.<name>}` token referenced anywhere in a config
/// value (recursing through objects and arrays).
pub fn artifact_tokens_in_value(value: &Value) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    collect_tokens(value, &mut tokens);
    tokens
}

fn collect_tokens(value: &Value, tokens: &mut BTreeSet<String>) {
    match value {
        Value::String(s) => {
            for capture in ARTIFACT_TOKEN_RE.captures_iter(s) {
                tokens.insert(capture[1].to_string());
            }
        }
        Value::Array(items) => items.iter().for_each(|item| collect_tokens(item, tokens)),
        Value::Object(map) => map.values().for_each(|item| collect_tokens(item, tokens)),
        _ => {}
    }
}

/// Substitutes `${artifact.<name>}` tokens in every string field of a config
/// value. Only names present in `paths` are replaced; admission has already
/// guaranteed every token in the config names a declared artifact.
pub fn substitute_artifact_tokens(value: &mut Value, paths: &HashMap<String, String>) {
    match value {
        Value::String(s) => {
            if s.contains("${artifact.") {
                let replaced = ARTIFACT_TOKEN_RE.replace_all(s, |caps: &regex::Captures<'_>| {
                    paths
                        .get(&caps[1])
                        .cloned()
                        .unwrap_or_else(|| caps[0].to_string())
                });
                *s = replaced.into_owned();
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| substitute_artifact_tokens(item, paths)),
        Value::Object(map) => map
            .values_mut()
            .for_each(|item| substitute_artifact_tokens(item, paths)),
        _ => {}
    }
}

/// Cross-field validation for a task's artifact list: per-spec shape rules,
/// name uniqueness, `${artifact.<name>}` references in the kind config, and
/// `auth.identity` references into the task's `identities` list.
///
/// Cluster-dependent rules (scheme allowlist, `auth.nodeIdentity` existence)
/// are enforced separately at admission where the manager config is in scope.
pub fn validate_task_artifacts(
    task_name: &str,
    artifacts: &[ArtifactSpec],
    config: &Value,
    identities: &[WorkloadIdentity],
) -> R<()> {
    let mut names = BTreeSet::new();
    for artifact in artifacts {
        artifact
            .validate()
            .with_context(|| format!("task {task_name} artifact {}", artifact.redacted_source()))?;
        if let Some(name) = artifact.name.as_deref() {
            if !names.insert(name.to_string()) {
                bail!("task {task_name} declares duplicate artifact name {name}");
            }
        }
        if let Some(auth) = artifact.auth.as_ref() {
            if let Some(identity) = auth.identity.as_deref() {
                if !identities.iter().any(|i| i.name == identity) {
                    bail!(
                        "task {task_name} artifact auth references unknown identity {identity} \
                         (declare it in the task's identities list)"
                    );
                }
            }
        }
    }

    for token in artifact_tokens_in_value(config) {
        if !names.contains(&token) {
            bail!(
                "task {task_name} config references ${{artifact.{token}}} but declares no \
                 artifact named {token}"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(value: Value) -> R<ArtifactSpec> {
        Ok(serde_json::from_value(value)?)
    }

    #[test]
    fn string_shorthand_deserializes_to_full_spec() -> R<()> {
        let artifact = spec(json!("https://example.com/tool.tar.gz"))?;
        assert_eq!(artifact.source, "https://example.com/tool.tar.gz");
        assert_eq!(artifact.name, None);
        assert_eq!(artifact.unpack, ArtifactUnpack::Auto);
        Ok(())
    }

    #[test]
    fn full_form_deserializes_camel_case_fields() -> R<()> {
        let artifact = spec(json!({
            "source": "s3://bucket/key.tar.gz",
            "name": "build",
            "dest": "build/",
            "checksum": format!("sha256:{}", "a".repeat(64)),
            "unpack": "archive",
            "resolve": "always",
            "auth": {"kv": "secrets/build"},
            "mode": "0755",
        }))?;
        assert_eq!(artifact.name.as_deref(), Some("build"));
        assert_eq!(artifact.unpack, ArtifactUnpack::Archive);
        assert_eq!(artifact.effective_resolve(), ArtifactResolve::Always);
        assert_eq!(
            artifact.auth.as_ref().and_then(|a| a.kv.as_deref()),
            Some("secrets/build")
        );
        artifact.validate()
    }

    #[test]
    fn unknown_fields_rejected_in_object_form() {
        assert!(spec(json!({"source": "https://x/y", "nope": 1})).is_err());
    }

    #[test]
    fn round_trips_through_serialization() -> R<()> {
        let artifact = spec(json!({
            "source": "oci://registry/app:v1",
            "name": "app",
            "mediaType": "application/vnd.acme.model",
        }))?;
        let serialized = serde_json::to_value(&artifact)?;
        assert_eq!(serialized["mediaType"], "application/vnd.acme.model");
        let back: ArtifactSpec = serde_json::from_value(serialized)?;
        assert_eq!(back, artifact);
        Ok(())
    }

    #[test]
    fn rejects_bad_names() {
        for name in ["Tool", "a b", "", "UPPER", "x.y"] {
            assert!(validate_artifact_name(name).is_err(), "{name}");
        }
        for name in ["tool", "a-b_c", "x9"] {
            assert!(validate_artifact_name(name).is_ok(), "{name}");
        }
    }

    #[test]
    fn rejects_bad_checksums() {
        assert!(validate_artifact_checksum("md5:abc").is_err());
        assert!(validate_artifact_checksum("sha256:short").is_err());
        assert!(validate_artifact_checksum(&format!("sha256:{}", "g".repeat(64))).is_err());
        assert!(validate_artifact_checksum(&format!("sha256:{}", "a".repeat(64))).is_ok());
    }

    #[test]
    fn rejects_escaping_dest() {
        assert!(validate_artifact_dest("/abs").is_err());
        assert!(validate_artifact_dest("../up").is_err());
        assert!(validate_artifact_dest("a/../../up").is_err());
        assert!(validate_artifact_dest("a/../b").is_ok());
        assert!(validate_artifact_dest("nested/dir/").is_ok());
    }

    #[test]
    fn auth_requires_exactly_one_mechanism() {
        let none = ArtifactAuth::default();
        assert!(none.validate().is_err());

        let two = ArtifactAuth {
            kv: Some("secrets/a".into()),
            identity: Some("aws".into()),
            ..Default::default()
        };
        assert!(two.validate().is_err());

        let assume_role_without_identity = ArtifactAuth {
            kv: Some("secrets/a".into()),
            assume_role: Some("arn:aws:iam::1:role/x".into()),
            ..Default::default()
        };
        assert!(assume_role_without_identity.validate().is_err());

        let identity = ArtifactAuth {
            identity: Some("aws".into()),
            assume_role: Some("arn:aws:iam::1:role/x".into()),
            ..Default::default()
        };
        assert!(identity.validate().is_ok());
    }

    #[test]
    fn git_prefix_normalizes_scheme() -> R<()> {
        let parsed = parse_artifact_source("git::https://github.com/acme/repo?ref=v1.2.0")?;
        assert_eq!(parsed.scheme, "git");
        assert_eq!(parsed.url.host_str(), Some("github.com"));
        Ok(())
    }

    #[test]
    fn redaction_strips_userinfo_and_query() {
        assert_eq!(
            redact_source("https://user:pass@example.com/path?X-Amz-Signature=secret"),
            "https://example.com/path?…"
        );
        assert_eq!(
            redact_source("s3://bucket/key.tar.gz"),
            "s3://bucket/key.tar.gz"
        );
        assert!(!redact_source("not a url ?secret=x@host").contains("secret=x"));
    }

    #[test]
    fn mutability_detection() -> R<()> {
        let tag = ArtifactSpec::from_source("oci://registry/app:v1");
        assert!(tag.is_mutable_reference()?);

        let digest =
            ArtifactSpec::from_source(format!("oci://registry/app@sha256:{}", "a".repeat(64)));
        assert!(!digest.is_mutable_reference()?);

        let git_branch = ArtifactSpec::from_source("git::https://github.com/acme/repo?ref=main");
        assert!(git_branch.is_mutable_reference()?);

        let git_sha = ArtifactSpec::from_source(format!(
            "git::https://github.com/acme/repo?ref={}",
            "a".repeat(40)
        ));
        assert!(!git_sha.is_mutable_reference()?);

        let mut checksummed = ArtifactSpec::from_source("https://example.com/x.tar.gz");
        checksummed.checksum = Some(format!("sha256:{}", "a".repeat(64)));
        assert!(!checksummed.is_mutable_reference()?);
        Ok(())
    }

    #[test]
    fn token_collection_and_substitution() {
        let mut config = json!({
            "command": ["${artifact.tool}/bin/run", "--weights", "${artifact.model}/w.bin"],
            "env": {"MODEL_DIR": "${artifact.model}"},
            "count": 3,
        });
        let tokens = artifact_tokens_in_value(&config);
        assert_eq!(
            tokens.into_iter().collect::<Vec<_>>(),
            vec!["model".to_string(), "tool".to_string()]
        );

        let mut paths = HashMap::new();
        paths.insert("tool".to_string(), "/task/tool".to_string());
        paths.insert("model".to_string(), "/task/models".to_string());
        substitute_artifact_tokens(&mut config, &paths);
        assert_eq!(config["command"][0], "/task/tool/bin/run");
        assert_eq!(config["env"]["MODEL_DIR"], "/task/models");
        assert_eq!(config["count"], 3);
    }

    #[test]
    fn validate_task_artifacts_full_matrix() {
        let identities = vec![WorkloadIdentity {
            name: "aws".into(),
            audience: vec!["sts.amazonaws.com".into()],
            env: Some("AWS_TOKEN".into()),
            ..Default::default()
        }];

        // Duplicate names rejected.
        let duplicate = vec![
            ArtifactSpec::from_source("https://x/a").named("tool"),
            ArtifactSpec::from_source("https://x/b").named("tool"),
        ];
        let err = validate_task_artifacts("t", &duplicate, &json!({}), &identities).unwrap_err();
        assert!(err.to_string().contains("duplicate artifact name"), "{err}");

        // Unknown interpolation reference rejected.
        let artifacts = vec![ArtifactSpec::from_source("https://x/a").named("tool")];
        let config = json!({"command": ["${artifact.missing}/x"]});
        let err = validate_task_artifacts("t", &artifacts, &config, &identities).unwrap_err();
        assert!(err.to_string().contains("artifact.missing"), "{err}");

        // Unknown identity reference rejected.
        let mut with_auth = ArtifactSpec::from_source("https://x/a");
        with_auth.auth = Some(ArtifactAuth {
            identity: Some("gcp".into()),
            ..Default::default()
        });
        let err = validate_task_artifacts("t", &[with_auth], &json!({}), &identities).unwrap_err();
        assert!(err.to_string().contains("unknown identity gcp"), "{err}");

        // Valid spec passes.
        let mut good = ArtifactSpec::from_source("https://x/a.tar.gz").named("tool");
        good.auth = Some(ArtifactAuth {
            identity: Some("aws".into()),
            ..Default::default()
        });
        let config = json!({"command": ["${artifact.tool}/run"]});
        validate_task_artifacts("t", &[good], &config, &identities).expect("valid artifacts");
    }

    #[test]
    fn media_type_only_for_oci() {
        let mut artifact = ArtifactSpec::from_source("https://x/a");
        artifact.media_type = Some("application/wasm".into());
        assert!(artifact.validate().is_err());

        let mut oci = ArtifactSpec::from_source("oci://registry/app:v1");
        oci.media_type = Some("application/wasm".into());
        assert!(oci.validate().is_ok());
    }

    #[test]
    fn env_var_name_uppercases_and_replaces_dashes() {
        let artifact = ArtifactSpec::from_source("https://x/a").named("my-tool_2");
        assert_eq!(
            artifact.env_var_name().as_deref(),
            Some("LEGION_ARTIFACT_MY_TOOL_2")
        );
    }
}
