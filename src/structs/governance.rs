//! Manager configuration for agent governance (`governance { … }` in the
//! manager config; docs/audit-journal.md, docs/approvals-and-rate-limits.md,
//! docs/usage-accounting.md), and the pure rule logic the gates evaluate.
//!
//! Every field is optional; the accessors resolve the defaults, so the same
//! type serves as the parsed and the resolved form.

use crate::structs::acl::{glob_match, AclAction, AclResource, AclScope};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default number of audit records each manager keeps.
pub const DEF_AUDIT_RETAIN_EVENTS: u64 = 200_000;
/// Default age after which audit records are pruned.
pub const DEF_AUDIT_RETAIN_AGE: &str = "90d";
/// Default flush interval of the batched audit writer.
pub const DEF_AUDIT_FLUSH_INTERVAL: &str = "250ms";
/// Default lifetime of an approval request (and of a granted approval).
pub const DEF_APPROVAL_TTL: &str = "1h";
/// Default sustained write rate per token.
pub const DEF_RATE_WRITES_PER_SECOND: f64 = 20.0;
/// Default write burst per token.
pub const DEF_RATE_BURST: u32 = 100;
/// Default usage accumulation interval.
pub const DEF_USAGE_INTERVAL: &str = "60s";
/// Default usage retention.
pub const DEF_USAGE_RETENTION: &str = "90d";

fn parse_duration(raw: &str, fallback: &str) -> Duration {
    raw.parse::<humantime::Duration>()
        .map(Into::into)
        .unwrap_or_else(|_| {
            fallback
                .parse::<humantime::Duration>()
                .map(Into::into)
                .unwrap_or(Duration::from_secs(60))
        })
}

/// The `governance` block of the manager config.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GovernanceConfig {
    #[serde(default)]
    pub audit: Option<AuditConfig>,
    #[serde(default)]
    pub approvals: Option<ApprovalsConfig>,
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    #[serde(default)]
    pub usage: Option<UsageConfig>,
}

/// `governance.audit`: the durable audit journal.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuditConfig {
    /// Default true. `false` stops journaling (denials still reach the
    /// `acl_audit` tracing target).
    pub enabled: Option<bool>,
    /// Records each manager keeps. Default 200,000.
    pub retain_events: Option<u64>,
    /// Records older than this are pruned. Default `90d`.
    pub retain_age: Option<String>,
    /// How often the batched writer flushes. Default `250ms`.
    pub flush_interval: Option<String>,
}

/// `governance.approvals`: human-approval gates.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalsConfig {
    /// How long a request waits for a decision, and how long a granted
    /// approval stays usable. Default `1h`.
    pub ttl: Option<String>,
    /// The gates. Default: none.
    #[serde(default)]
    pub rules: Vec<ApprovalRule>,
}

/// One approval gate. Every non-empty field must match; empty fields match
/// anything. A rule with no `operations` and no `permissions` is rejected.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalRule {
    /// Names the rule in errors, approval requests and the journal.
    pub name: String,
    /// Globs over Manager RPC names (`DestroyJob`, `*Acl*`).
    #[serde(default)]
    pub operations: Vec<String>,
    /// Globs over the write being checked, `resource:action`
    /// (`acl:write`, `node:*`).
    #[serde(default)]
    pub permissions: Vec<String>,
    /// Globs over the target's namespace. A target without one never matches
    /// a rule that names namespaces.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Globs over the target's name (job, app, object, …).
    #[serde(default)]
    pub names: Vec<String>,
    /// Globs over the target's node pool.
    #[serde(default)]
    pub pools: Vec<String>,
    /// Globs over the target's KV path.
    #[serde(default)]
    pub paths: Vec<String>,
    /// Globs over the caller's principal. Default: everyone.
    #[serde(default)]
    pub principals: Vec<String>,
    /// The caller holds at least one of these policies.
    #[serde(default)]
    pub policies: Vec<String>,
    /// Whether management tokens are gated too. Default true: a management
    /// token in an agent's hands is the case this exists for.
    pub include_management: Option<bool>,
}

/// `governance.rateLimit`: per-token write rate limits.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Default true.
    pub enabled: Option<bool>,
    /// Sustained writes per second per token. Default 20.
    pub writes_per_second: Option<f64>,
    /// Bucket size. Default 100.
    pub burst: Option<u32>,
    /// Exempt management tokens. Default true: they are break-glass
    /// credentials; give agents client tokens.
    pub exempt_management: Option<bool>,
    /// Per-caller overrides; the first match wins.
    #[serde(default)]
    pub overrides: Vec<RateLimitOverride>,
}

/// One rate-limit override, matched on the caller.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RateLimitOverride {
    #[serde(default)]
    pub principals: Vec<String>,
    #[serde(default)]
    pub policies: Vec<String>,
    #[serde(default)]
    pub accessors: Vec<String>,
    pub writes_per_second: f64,
    pub burst: u32,
}

/// `governance.usage`: time-integrated usage accounting.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UsageConfig {
    /// Default true.
    pub enabled: Option<bool>,
    /// How often reservations are integrated. Default `60s`.
    pub interval: Option<String>,
    /// How long hourly buckets are kept. Default `90d`.
    pub retention: Option<String>,
}

impl GovernanceConfig {
    pub fn audit_enabled(&self) -> bool {
        self.audit.as_ref().and_then(|a| a.enabled).unwrap_or(true)
    }
    pub fn audit_retain_events(&self) -> u64 {
        self.audit
            .as_ref()
            .and_then(|a| a.retain_events)
            .unwrap_or(DEF_AUDIT_RETAIN_EVENTS)
    }
    pub fn audit_retain_age(&self) -> Duration {
        let raw = self.audit.as_ref().and_then(|a| a.retain_age.clone());
        parse_duration(
            raw.as_deref().unwrap_or(DEF_AUDIT_RETAIN_AGE),
            DEF_AUDIT_RETAIN_AGE,
        )
    }
    pub fn audit_flush_interval(&self) -> Duration {
        let raw = self.audit.as_ref().and_then(|a| a.flush_interval.clone());
        parse_duration(
            raw.as_deref().unwrap_or(DEF_AUDIT_FLUSH_INTERVAL),
            DEF_AUDIT_FLUSH_INTERVAL,
        )
    }
    pub fn approval_ttl(&self) -> Duration {
        let raw = self.approvals.as_ref().and_then(|a| a.ttl.clone());
        parse_duration(raw.as_deref().unwrap_or(DEF_APPROVAL_TTL), DEF_APPROVAL_TTL)
    }
    pub fn approval_rules(&self) -> &[ApprovalRule] {
        self.approvals
            .as_ref()
            .map(|a| a.rules.as_slice())
            .unwrap_or_default()
    }
    pub fn rate_limit_enabled(&self) -> bool {
        self.rate_limit
            .as_ref()
            .and_then(|r| r.enabled)
            .unwrap_or(true)
    }
    pub fn usage_enabled(&self) -> bool {
        self.usage.as_ref().and_then(|u| u.enabled).unwrap_or(true)
    }
    pub fn usage_interval(&self) -> Duration {
        let raw = self.usage.as_ref().and_then(|u| u.interval.clone());
        parse_duration(
            raw.as_deref().unwrap_or(DEF_USAGE_INTERVAL),
            DEF_USAGE_INTERVAL,
        )
    }
    pub fn usage_retention(&self) -> Duration {
        let raw = self.usage.as_ref().and_then(|u| u.retention.clone());
        parse_duration(
            raw.as_deref().unwrap_or(DEF_USAGE_RETENTION),
            DEF_USAGE_RETENTION,
        )
    }

    /// Rejects malformed durations and approval rules that would gate
    /// nothing or everything by accident. Run at manager start.
    pub fn validate(&self) -> Result<(), String> {
        let durations = [
            (
                "governance.audit.retainAge",
                self.audit.as_ref().and_then(|a| a.retain_age.clone()),
            ),
            (
                "governance.audit.flushInterval",
                self.audit.as_ref().and_then(|a| a.flush_interval.clone()),
            ),
            (
                "governance.approvals.ttl",
                self.approvals.as_ref().and_then(|a| a.ttl.clone()),
            ),
            (
                "governance.usage.interval",
                self.usage.as_ref().and_then(|u| u.interval.clone()),
            ),
            (
                "governance.usage.retention",
                self.usage.as_ref().and_then(|u| u.retention.clone()),
            ),
        ];
        for (field, raw) in durations {
            if let Some(raw) = raw {
                raw.parse::<humantime::Duration>()
                    .map_err(|e| format!("{field}: invalid duration `{raw}`: {e}"))?;
            }
        }
        for (index, rule) in self.approval_rules().iter().enumerate() {
            if rule.name.trim().is_empty() {
                return Err(format!(
                    "governance.approvals.rules[{index}]: name is required"
                ));
            }
            if rule.operations.is_empty() && rule.permissions.is_empty() {
                return Err(format!(
                    "governance.approvals.rules[{index}] (`{}`): name at least one of \
                     `operations` or `permissions`; a rule matching every write would gate \
                     the whole API",
                    rule.name
                ));
            }
        }
        if let Some(rate) = &self.rate_limit {
            if rate.writes_per_second.is_some_and(|r| r <= 0.0) {
                return Err("governance.rateLimit.writesPerSecond must be positive".into());
            }
            for (index, o) in rate.overrides.iter().enumerate() {
                if o.writes_per_second <= 0.0 || o.burst == 0 {
                    return Err(format!(
                        "governance.rateLimit.overrides[{index}]: writesPerSecond and burst \
                         must be positive"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The `(writes per second, burst)` limit for a caller, or `None` when
    /// it is exempt. `token_limit` is a limit stored on the token itself
    /// (`legion acl token create --write-rate`), which wins over config.
    pub fn rate_limit_for(
        &self,
        caller: &GateCaller<'_>,
        token_limit: Option<(f64, u32)>,
    ) -> Option<(f64, u32)> {
        if !self.rate_limit_enabled() {
            return None;
        }
        if let Some(limit) = token_limit {
            return Some(limit);
        }
        let config = self.rate_limit.clone().unwrap_or_default();
        if caller.is_management && config.exempt_management.unwrap_or(true) {
            return None;
        }
        for o in &config.overrides {
            let principal_ok = o.principals.is_empty()
                || o.principals.iter().any(|p| glob_match(p, caller.principal));
            let policy_ok = o.policies.is_empty()
                || o.policies
                    .iter()
                    .any(|p| caller.policies.iter().any(|held| glob_match(p, held)));
            let accessor_ok = o.accessors.is_empty()
                || o.accessors
                    .iter()
                    .any(|a| caller.accessor_id.is_some_and(|held| glob_match(a, held)));
            if principal_ok && policy_ok && accessor_ok {
                return Some((o.writes_per_second, o.burst));
            }
        }
        Some((
            config
                .writes_per_second
                .unwrap_or(DEF_RATE_WRITES_PER_SECOND),
            config.burst.unwrap_or(DEF_RATE_BURST),
        ))
    }
}

/// The caller as the gates see it.
#[derive(Debug, Clone, Copy)]
pub struct GateCaller<'a> {
    pub principal: &'a str,
    pub accessor_id: Option<&'a str>,
    pub policies: &'a [String],
    pub is_management: bool,
}

impl ApprovalRule {
    /// Whether this rule gates `operation`, a write of `resource:action` on
    /// `scope`, by `caller`.
    pub fn matches(
        &self,
        operation: &str,
        resource: AclResource,
        action: AclAction,
        scope: &AclScope,
        caller: &GateCaller<'_>,
    ) -> bool {
        if caller.is_management && !self.include_management.unwrap_or(true) {
            return false;
        }
        if !self.operations.is_empty() && !self.operations.iter().any(|g| glob_match(g, operation))
        {
            return false;
        }
        let permission = format!("{}:{}", resource.as_str(), action.as_str());
        if !self.permissions.is_empty()
            && !self.permissions.iter().any(|g| glob_match(g, &permission))
        {
            return false;
        }
        let selector = |globs: &[String], value: &Option<String>| -> bool {
            globs.is_empty()
                || value
                    .as_deref()
                    .is_some_and(|value| globs.iter().any(|g| glob_match(g, value)))
        };
        if !selector(&self.namespaces, &scope.namespace)
            || !selector(&self.names, &scope.name)
            || !selector(&self.pools, &scope.pool)
            || !selector(&self.paths, &scope.path)
        {
            return false;
        }
        if !self.principals.is_empty()
            && !self
                .principals
                .iter()
                .any(|g| glob_match(g, caller.principal))
        {
            return false;
        }
        if !self.policies.is_empty()
            && !self
                .policies
                .iter()
                .any(|g| caller.policies.iter().any(|held| glob_match(g, held)))
        {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod governance_config_test {
    use super::*;

    fn caller<'a>(principal: &'a str, policies: &'a [String]) -> GateCaller<'a> {
        GateCaller {
            principal,
            accessor_id: Some("acc"),
            policies,
            is_management: false,
        }
    }

    #[test]
    fn approval_rules_match_operation_target_and_caller_test() {
        let rule = ApprovalRule {
            name: "prod-destroys".into(),
            operations: vec!["DestroyJob".into(), "StopJob".into()],
            namespaces: vec!["prod".into()],
            ..Default::default()
        };
        let none: Vec<String> = Vec::new();
        let prod = AclScope {
            namespace: Some("prod".into()),
            name: Some("web".into()),
            ..Default::default()
        };
        let dev = AclScope {
            namespace: Some("dev".into()),
            ..Default::default()
        };
        let who = caller("agent", &none);
        assert!(rule.matches(
            "DestroyJob",
            AclResource::Job,
            AclAction::Write,
            &prod,
            &who
        ));
        assert!(!rule.matches("DestroyJob", AclResource::Job, AclAction::Write, &dev, &who));
        assert!(!rule.matches("RunJob", AclResource::Job, AclAction::Write, &prod, &who));
        // The coarse `any()` check carries no namespace, so it never matches
        // a namespace-scoped rule; the precise check after the lookup does.
        assert!(!rule.matches(
            "DestroyJob",
            AclResource::Job,
            AclAction::Write,
            &AclScope::any(),
            &who
        ));

        let acl_writes = ApprovalRule {
            name: "acl".into(),
            permissions: vec!["acl:write".into()],
            principals: vec!["agent-*".into()],
            ..Default::default()
        };
        let agent = caller("agent-7", &none);
        let human = caller("alice", &none);
        assert!(acl_writes.matches(
            "CreateAclToken",
            AclResource::Acl,
            AclAction::Write,
            &AclScope::none(),
            &agent
        ));
        assert!(!acl_writes.matches(
            "CreateAclToken",
            AclResource::Acl,
            AclAction::Write,
            &AclScope::none(),
            &human
        ));
        let mut management = agent;
        management.is_management = true;
        let exempting = ApprovalRule {
            include_management: Some(false),
            ..acl_writes.clone()
        };
        assert!(!exempting.matches(
            "CreateAclToken",
            AclResource::Acl,
            AclAction::Write,
            &AclScope::none(),
            &management
        ));
    }

    #[test]
    fn validate_rejects_rules_that_gate_everything_test() {
        let config = GovernanceConfig {
            approvals: Some(ApprovalsConfig {
                ttl: None,
                rules: vec![ApprovalRule {
                    name: "all".into(),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        assert!(config.validate().unwrap_err().contains("operations"));
        let bad_ttl = GovernanceConfig {
            approvals: Some(ApprovalsConfig {
                ttl: Some("soon".into()),
                rules: Vec::new(),
            }),
            ..Default::default()
        };
        assert!(bad_ttl.validate().is_err());
        assert!(GovernanceConfig::default().validate().is_ok());
    }

    #[test]
    fn rate_limits_resolve_defaults_overrides_and_exemptions_test() {
        let config = GovernanceConfig {
            rate_limit: Some(RateLimitConfig {
                overrides: vec![RateLimitOverride {
                    policies: vec!["ci-*".into()],
                    writes_per_second: 2.0,
                    burst: 10,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let ci = vec!["ci-deployer".to_string()];
        let none: Vec<String> = Vec::new();
        assert_eq!(
            config.rate_limit_for(&caller("bot", &ci), None),
            Some((2.0, 10))
        );
        assert_eq!(
            config.rate_limit_for(&caller("alice", &none), None),
            Some((DEF_RATE_WRITES_PER_SECOND, DEF_RATE_BURST))
        );
        let mut management = caller("root", &none);
        management.is_management = true;
        assert_eq!(config.rate_limit_for(&management, None), None);
        assert_eq!(
            config.rate_limit_for(&caller("alice", &none), Some((1.0, 1))),
            Some((1.0, 1))
        );
    }
}
