//! Cluster artifact-auth matchers and effective-auth normalization
//! (artifact system plan §6).
//!
//! Resolution order: task artifact `auth` → first matching cluster
//! `artifacts.auth` entry → anonymous. The manager applies matchers at
//! admission and persists the *effective* auth on the normalized artifact
//! list, so workers never re-evaluate cluster config (changing matchers
//! affects the next submit, not in-flight jobs — documented in
//! docs/artifacts.md). Credentials themselves are still resolved late, on
//! the worker, at fetch time.

use crate::prelude::*;
use crate::structs::{
    parse_artifact_source, ArtifactAuth, ArtifactAuthMatcher, ArtifactSpec,
    ArtifactsManagerConfigFull,
};

/// Returns the first cluster matcher whose glob matches the source
/// (first-match-wins, like the config order).
pub fn match_cluster_auth<'a>(
    matchers: &'a [ArtifactAuthMatcher],
    source: &str,
) -> Option<&'a ArtifactAuthMatcher> {
    let normalized = normalize_for_match(source)?;
    matchers
        .iter()
        .find(|matcher| glob_match(&matcher.r#match, &normalized))
}

/// Normalizes a source for matching: `scheme://host/path` with the
/// go-getter `git::` prefix folded into a `git://` scheme and query/userinfo
/// stripped.
fn normalize_for_match(source: &str) -> Option<String> {
    let parsed = parse_artifact_source(source).ok()?;
    let host = parsed.url.host_str().unwrap_or("");
    let path = parsed.url.path();
    Some(format!("{}://{}{}", parsed.scheme, host, path))
}

/// Glob semantics: `**` crosses `/` boundaries, `*` matches within one path
/// segment, everything else is literal.
fn glob_match(pattern: &str, value: &str) -> bool {
    let mut regex = String::with_capacity(pattern.len() * 2);
    regex.push('^');
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => {
                if chars.peek() == Some(&'*') {
                    chars.next();
                    regex.push_str(".*");
                } else {
                    regex.push_str("[^/]*");
                }
            }
            '?' => regex.push_str("[^/]"),
            c if "\\.+()[]{}^$|".contains(c) => {
                regex.push('\\');
                regex.push(c);
            }
            c => regex.push(c),
        }
    }
    regex.push('$');
    regex::Regex::new(&regex)
        .map(|re| re.is_match(value))
        .unwrap_or(false)
}

/// Admission normalization: fills in the effective auth for every artifact
/// that carries none, from the cluster matchers. Artifacts with task-level
/// auth are annotated `auth_origin: "task"`; matched ones get the matcher's
/// auth and `auth_origin: "cluster"`; the rest stay anonymous.
pub fn apply_cluster_auth(artifacts: &mut [ArtifactSpec], cluster: &ArtifactsManagerConfigFull) {
    for artifact in artifacts {
        if artifact.auth.is_some() {
            artifact.auth_origin = Some("task".to_string());
            continue;
        }
        if let Some(matcher) = match_cluster_auth(&cluster.auth, &artifact.source) {
            artifact.auth = Some(ArtifactAuth {
                kv: matcher.kv.clone(),
                node_identity: matcher.node_identity.clone(),
                ..ArtifactAuth::default()
            });
            artifact.auth_origin = Some("cluster".to_string());
        }
    }
}

/// Cluster-scoped admission checks that need manager config: scheme
/// allowlist, matcher shape, and `auth.nodeIdentity` references.
pub fn validate_artifacts_against_cluster(
    task_name: &str,
    artifacts: &[ArtifactSpec],
    cluster: &ArtifactsManagerConfigFull,
    node_identity_names: &[String],
) -> R<()> {
    for artifact in artifacts {
        let parsed = parse_artifact_source(&artifact.source)
            .with_context(|| format!("task {task_name} artifact source"))?;
        if !cluster
            .allowed_schemes
            .iter()
            .any(|scheme| scheme == &parsed.scheme)
        {
            bail!(
                "task {task_name} artifact {} uses scheme {}, which is not in the cluster's \
                 artifacts.allowedSchemes {:?}",
                artifact.redacted_source(),
                parsed.scheme,
                cluster.allowed_schemes
            );
        }
        if let Some(node_identity) = artifact
            .auth
            .as_ref()
            .and_then(|auth| auth.node_identity.as_deref())
        {
            if !node_identity_names.iter().any(|n| n == node_identity) {
                bail!(
                    "task {task_name} artifact {} references unknown nodeIdentity \
                     {node_identity} (declare it under workloadIdentity.nodeIdentities)",
                    artifact.redacted_source()
                );
            }
        }
    }
    Ok(())
}

/// Validates the cluster `artifacts` config block itself (matcher shape and
/// nodeIdentity references). Called at manager startup and before use.
pub fn validate_cluster_artifacts_config(
    cluster: &ArtifactsManagerConfigFull,
    node_identity_names: &[String],
) -> R<()> {
    for matcher in &cluster.auth {
        matcher.validate()?;
        if let Some(node_identity) = matcher.node_identity.as_deref() {
            if !node_identity_names.iter().any(|n| n == node_identity) {
                bail!(
                    "artifacts.auth matcher {:?} references unknown nodeIdentity \
                     {node_identity} (declare it under workloadIdentity.nodeIdentities)",
                    matcher.r#match
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher(
        pattern: &str,
        kv: Option<&str>,
        node_identity: Option<&str>,
    ) -> ArtifactAuthMatcher {
        ArtifactAuthMatcher {
            r#match: pattern.into(),
            kv: kv.map(str::to_string),
            node_identity: node_identity.map(str::to_string),
        }
    }

    #[test]
    fn glob_semantics() {
        assert!(glob_match(
            "oci://ghcr.io/acme/**",
            "oci://ghcr.io/acme/api"
        ));
        assert!(glob_match(
            "oci://ghcr.io/acme/**",
            "oci://ghcr.io/acme/deep/nested"
        ));
        assert!(!glob_match(
            "oci://ghcr.io/acme/**",
            "oci://ghcr.io/other/api"
        ));
        // Single * stays within a segment.
        assert!(glob_match("s3://acme-*/tool", "s3://acme-builds/tool"));
        assert!(!glob_match("s3://acme-*/tool", "s3://acme-builds/sub/tool"));
        // Literal dots are not regex wildcards.
        assert!(!glob_match("oci://ghcrxio/**", "oci://ghcr.io/x"));
    }

    #[test]
    fn first_match_wins() {
        let matchers = vec![
            matcher("oci://ghcr.io/acme/**", Some("secrets/acme"), None),
            matcher("oci://ghcr.io/**", Some("secrets/fallback"), None),
        ];
        let matched = match_cluster_auth(&matchers, "oci://ghcr.io/acme/api:v1").expect("match");
        assert_eq!(matched.kv.as_deref(), Some("secrets/acme"));

        let fallback = match_cluster_auth(&matchers, "oci://ghcr.io/other/api:v1").expect("match");
        assert_eq!(fallback.kv.as_deref(), Some("secrets/fallback"));

        assert!(match_cluster_auth(&matchers, "https://example.com/x").is_none());
    }

    #[test]
    fn git_prefix_sources_match_git_patterns() {
        let matchers = vec![matcher(
            "git://github.com/acme/**",
            Some("secrets/git"),
            None,
        )];
        assert!(
            match_cluster_auth(&matchers, "git::https://github.com/acme/repo?ref=v1").is_some()
        );
    }

    #[test]
    fn apply_cluster_auth_annotates_origins() {
        let cluster = ArtifactsManagerConfigFull {
            auth: vec![matcher("s3://acme-tooling/**", None, Some("artifact-pull"))],
            ..Default::default()
        };

        let mut artifacts = vec![
            // Task-level auth wins untouched.
            {
                let mut spec = ArtifactSpec::from_source("s3://acme-tooling/tool.tar.gz");
                spec.auth = Some(ArtifactAuth {
                    kv: Some("secrets/own".into()),
                    ..Default::default()
                });
                spec
            },
            // Matched by cluster.
            ArtifactSpec::from_source("s3://acme-tooling/shared.tar.gz"),
            // Unmatched stays anonymous.
            ArtifactSpec::from_source("https://example.com/x.tar.gz"),
        ];
        apply_cluster_auth(&mut artifacts, &cluster);

        assert_eq!(artifacts[0].auth_origin.as_deref(), Some("task"));
        assert_eq!(
            artifacts[0].auth.as_ref().unwrap().kv.as_deref(),
            Some("secrets/own")
        );

        assert_eq!(artifacts[1].auth_origin.as_deref(), Some("cluster"));
        assert_eq!(
            artifacts[1].auth.as_ref().unwrap().node_identity.as_deref(),
            Some("artifact-pull")
        );
        assert_eq!(
            artifacts[1].auth_description(),
            "nodeIdentity: artifact-pull (cluster default)"
        );

        assert!(artifacts[2].auth.is_none());
        assert!(artifacts[2].auth_origin.is_none());
        assert_eq!(artifacts[2].auth_description(), "anonymous");
    }

    #[test]
    fn cluster_validation_rejects_disallowed_scheme_and_unknown_node_identity() {
        let cluster = ArtifactsManagerConfigFull::default();
        let artifacts = vec![ArtifactSpec::from_source("http://insecure.example.com/x")];
        let err = validate_artifacts_against_cluster("t", &artifacts, &cluster, &[]).unwrap_err();
        assert!(err.to_string().contains("allowedSchemes"), "{err}");

        let mut with_node_identity = ArtifactSpec::from_source("s3://bucket/key");
        with_node_identity.auth = Some(ArtifactAuth {
            node_identity: Some("missing".into()),
            ..Default::default()
        });
        let err = validate_artifacts_against_cluster("t", &[with_node_identity], &cluster, &[])
            .unwrap_err();
        assert!(err.to_string().contains("unknown nodeIdentity"), "{err}");
    }

    #[test]
    fn cluster_config_validation_checks_matchers() {
        let bad_matcher = ArtifactsManagerConfigFull {
            auth: vec![matcher("oci://x/**", Some("kv"), Some("node"))],
            ..Default::default()
        };
        assert!(validate_cluster_artifacts_config(&bad_matcher, &["node".into()]).is_err());

        let unknown_identity = ArtifactsManagerConfigFull {
            auth: vec![matcher("oci://x/**", None, Some("ghost"))],
            ..Default::default()
        };
        assert!(validate_cluster_artifacts_config(&unknown_identity, &[]).is_err());

        let valid = ArtifactsManagerConfigFull {
            auth: vec![
                matcher("oci://x/**", Some("secrets/x"), None),
                matcher("s3://y/**", None, Some("pull")),
            ],
            ..Default::default()
        };
        validate_cluster_artifacts_config(&valid, &["pull".into()]).expect("valid config");
    }
}
