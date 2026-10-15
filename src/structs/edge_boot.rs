//! Edge boot: gate levels, the task-level `speculativeStart` block, claim
//! identifiers, and boot-ticket claims.
//!
//! This module is pure logic — no storage, no transport, no clocks it does not
//! take as an argument — so the two decisions that carry the whole safety
//! argument can be unit tested exhaustively:
//!
//! 1. **The lower gate always wins.** A task's `speculativeStart.mode`, a boot
//!    ticket's `legion.io/gate` claim, and a caller's requested gate are
//!    intersected with [`Gate::min`]. Neither a jobspec nor a ticket can
//!    raise the other's ceiling.
//! 2. **A ticket is verified against facts the node already holds** — its own
//!    pool and region, the clock, the task config — never against anything the
//!    caller supplies alongside it ([`BootTicketClaims::authorizes`]).
//!
//! See `docs/edge-boot.md` and
//! `internal_docs/plans/edge-boot-plan.md`.

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// Audience every boot ticket carries. Reserved: it is not a task audience,
/// so a workload identity token can never be replayed as a boot ticket and
/// vice versa.
pub const BOOT_TICKET_AUDIENCE: &str = "legion:node";

/// Claim namespace for Legion-specific boot ticket claims.
pub const CLAIM_SCOPE: &str = "legion.io/scope";
pub const CLAIM_JOB: &str = "legion.io/job";
pub const CLAIM_JOB_CONFIG: &str = "legion.io/job_config";
pub const CLAIM_TASK: &str = "legion.io/task";
pub const CLAIM_INSTANCE: &str = "legion.io/instance";
pub const CLAIM_INSTANCE_PREFIX: &str = "legion.io/instance_prefix";
pub const CLAIM_POOL: &str = "legion.io/pool";
pub const CLAIM_GATE: &str = "legion.io/gate";
pub const CLAIM_USES: &str = "legion.io/uses";

/// Value of `legion.io/scope` on a boot ticket.
pub const BOOT_TICKET_SCOPE: &str = "boot";

/// Value of `legion.io/scope` on a session ticket (session durability plan
/// §6.4): it lets its holder start, read, and stop sessions of one session
/// task on any node of one pool that holds the task's envelope, until it
/// expires.
pub const SESSION_TICKET_SCOPE: &str = "session";

/// Default lease: how long an unratified start survives before the node kills
/// it unconditionally. Long enough to ride out a raft election, short enough
/// that a partitioned node's wasted microVM is bounded.
pub const DEFAULT_RATIFY_TIMEOUT: Duration = Duration::from_secs(5);

/// Hard ceiling on `speculativeStart.ratifyTimeout`. A lease is a promise
/// that a loser dies *soon*; a jobspec must not be able to turn it into a
/// promise that a loser dies eventually.
pub const MAX_RATIFY_TIMEOUT: Duration = Duration::from_secs(120);

/// How far a node may get on a start before the placement is durable.
///
/// Ordered: `Off < Stage < Boot < Serve`. The ordering is the whole point —
/// intersecting two ceilings is [`Ord::min`].
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "camelCase")]
pub enum Gate {
    /// No speculation: the node is told after the commit. Today's behavior,
    /// and the default for every task.
    #[default]
    Off,
    /// Fetch artifacts, pull images, build root disks, provision (not attach)
    /// node-local volumes. Do not start the workload.
    Stage,
    /// All of `Stage`, plus boot the guest with its network held shut.
    Boot,
    /// All of `Boot`, plus open the network immediately.
    Serve,
}

impl Gate {
    /// Stable wire/string form, as it appears in the jobspec, in events, and
    /// in `--json` output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Gate::Off => "off",
            Gate::Stage => "stage",
            Gate::Boot => "boot",
            Gate::Serve => "serve",
        }
    }

    /// Parse the string form. Unknown values are an error rather than a
    /// silent downgrade: a typo in a jobspec that quietly disabled the
    /// feature would be discovered as a latency regression in production.
    pub fn parse(raw: &str) -> R<Self> {
        match raw.trim() {
            "off" => Ok(Gate::Off),
            "stage" => Ok(Gate::Stage),
            "boot" => Ok(Gate::Boot),
            "serve" => Ok(Gate::Serve),
            other => bail!("invalid speculativeStart mode {other:?} (off|stage|boot|serve)"),
        }
    }

    /// The proto enum value.
    pub fn to_wire(self) -> i32 {
        match self {
            Gate::Off => crate::structs::GateLevel::GateOff as i32,
            Gate::Stage => crate::structs::GateLevel::GateStage as i32,
            Gate::Boot => crate::structs::GateLevel::GateBoot as i32,
            Gate::Serve => crate::structs::GateLevel::GateServe as i32,
        }
    }

    /// From the proto enum value. An unrecognized value (a newer manager
    /// talking to an older worker) reads as [`Gate::Off`] — fail closed,
    /// which for a gate means "speculate nothing".
    pub fn from_wire(raw: i32) -> Self {
        match crate::structs::GateLevel::try_from(raw) {
            Ok(crate::structs::GateLevel::GateStage) => Gate::Stage,
            Ok(crate::structs::GateLevel::GateBoot) => Gate::Boot,
            Ok(crate::structs::GateLevel::GateServe) => Gate::Serve,
            _ => Gate::Off,
        }
    }

    /// Whether this gate permits starting the workload at all.
    pub fn may_start(self) -> bool {
        self >= Gate::Boot
    }

    /// Whether this gate permits the workload's network to be open before
    /// ratification.
    pub fn may_open_network(self) -> bool {
        self >= Gate::Serve
    }
}

impl fmt::Display for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the node does with work whose ratification was refused.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum OnRefused {
    /// Hard destroy. The default, because the common refusal is "someone else
    /// owns this id" and a loser lingering is exactly what the at-most-one
    /// invariant forbids.
    #[default]
    Kill,
    /// Run the task's normal graceful-stop ladder (docs/graceful-stop.md),
    /// then remove it. For workloads that want to flush before disappearing.
    Stop,
}

impl OnRefused {
    pub fn as_str(&self) -> &'static str {
        match self {
            OnRefused::Kill => "kill",
            OnRefused::Stop => "stop",
        }
    }

    pub fn parse(raw: &str) -> R<Self> {
        match raw.trim() {
            "kill" => Ok(OnRefused::Kill),
            "stop" => Ok(OnRefused::Stop),
            other => bail!("invalid speculativeStart onRefused {other:?} (kill|stop)"),
        }
    }
}

/// Task-level `speculativeStart` block, as declared in a jobspec.
///
/// ```nickel
/// speculativeStart: {
///   mode: 'boot',        // 'off' (default) | 'stage' | 'boot' | 'serve'
///   ratifyTimeout: '5s', // lease; unratified work is killed at expiry
///   onRefused: 'kill',   // 'kill' (default) | 'stop'
/// }
/// ```
///
/// Absent from every jobspec by default; an absent block means `off`, which
/// costs nothing and changes no wire payload.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SpeculativeStartConfig {
    /// How far a node may get before the placement is durable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Lease: unratified work is killed at expiry. Default `5s`, capped at
    /// `2m`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratify_timeout: Option<String>,
    /// What to do when ratification is refused. Default `kill`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_refused: Option<String>,
}

/// A resolved `speculativeStart` block: every default applied, every value
/// validated. Built once at admission so no runtime path parses strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedSpeculativeStart {
    pub mode: Gate,
    pub ratify_timeout: Duration,
    pub on_refused: OnRefused,
}

impl Default for ResolvedSpeculativeStart {
    fn default() -> Self {
        ResolvedSpeculativeStart {
            mode: Gate::Off,
            ratify_timeout: DEFAULT_RATIFY_TIMEOUT,
            on_refused: OnRefused::Kill,
        }
    }
}

impl SpeculativeStartConfig {
    /// Resolve and validate. Errors are admission errors: a jobspec that
    /// declares nonsense is rejected at `job run` rather than discovered when
    /// a sandbox fails to start.
    pub fn resolve(&self) -> R<ResolvedSpeculativeStart> {
        let mode = match self.mode.as_deref() {
            Some(raw) => Gate::parse(raw)?,
            None => Gate::Off,
        };
        let ratify_timeout = match self.ratify_timeout.as_deref() {
            Some(raw) => {
                let parsed = humantime::parse_duration(raw.trim())
                    .with_context(|| format!("invalid speculativeStart.ratifyTimeout {raw:?}"))?;
                // The lease travels as a unix deadline in whole seconds, so a
                // sub-second lease would land on the node already expired.
                if parsed < Duration::from_secs(1) {
                    bail!(
                        "speculativeStart.ratifyTimeout {} is shorter than 1s, the shortest \
                         lease a node can hold",
                        humantime::format_duration(parsed)
                    );
                }
                if parsed > MAX_RATIFY_TIMEOUT {
                    bail!(
                        "speculativeStart.ratifyTimeout {} exceeds the {} maximum",
                        humantime::format_duration(parsed),
                        humantime::format_duration(MAX_RATIFY_TIMEOUT)
                    );
                }
                parsed
            }
            None => DEFAULT_RATIFY_TIMEOUT,
        };
        let on_refused = match self.on_refused.as_deref() {
            Some(raw) => OnRefused::parse(raw)?,
            None => OnRefused::Kill,
        };
        Ok(ResolvedSpeculativeStart {
            mode,
            ratify_timeout,
            on_refused,
        })
    }
}

/// How far a claim has actually progressed on the node.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(rename_all = "camelCase")]
pub enum ClaimStage {
    /// Admitted locally (ticket verified, resources reserved) — nothing
    /// fetched yet.
    #[default]
    Claimed,
    /// Artifacts fetched, image pulled, root disk built. Nothing executing.
    Staged,
    /// The workload is running with its network held shut.
    Booted,
    /// The workload is running with its network open.
    Serving,
}

impl ClaimStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClaimStage::Claimed => "claimed",
            ClaimStage::Staged => "staged",
            ClaimStage::Booted => "booted",
            ClaimStage::Serving => "serving",
        }
    }
}

impl fmt::Display for ClaimStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a ratification was refused. Mirrors the `ClaimRefusalReason` proto
/// enum; kept as a Rust enum so the manager's refusal sites are exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RefusalReason {
    #[default]
    Unspecified,
    /// Another allocation already owns this instance id — the expected
    /// outcome of a lost race.
    InstanceClaimed,
    NoCapacity,
    Ineligible,
    Expired,
    TicketInvalid,
    UnknownTarget,
    Disabled,
    Internal,
}

impl RefusalReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefusalReason::Unspecified => "unspecified",
            RefusalReason::InstanceClaimed => "instance_claimed",
            RefusalReason::NoCapacity => "no_capacity",
            RefusalReason::Ineligible => "ineligible",
            RefusalReason::Expired => "expired",
            RefusalReason::TicketInvalid => "ticket_invalid",
            RefusalReason::UnknownTarget => "unknown_target",
            RefusalReason::Disabled => "disabled",
            RefusalReason::Internal => "internal",
        }
    }

    pub fn to_wire(self) -> i32 {
        use crate::structs::ClaimRefusalReason as W;
        let wire = match self {
            RefusalReason::Unspecified => W::RefusalUnspecified,
            RefusalReason::InstanceClaimed => W::RefusalInstanceClaimed,
            RefusalReason::NoCapacity => W::RefusalNoCapacity,
            RefusalReason::Ineligible => W::RefusalIneligible,
            RefusalReason::Expired => W::RefusalExpired,
            RefusalReason::TicketInvalid => W::RefusalTicketInvalid,
            RefusalReason::UnknownTarget => W::RefusalUnknownTarget,
            RefusalReason::Disabled => W::RefusalDisabled,
            RefusalReason::Internal => W::RefusalInternal,
        };
        wire as i32
    }

    pub fn from_wire(raw: i32) -> Self {
        use crate::structs::ClaimRefusalReason as W;
        match W::try_from(raw) {
            Ok(W::RefusalInstanceClaimed) => RefusalReason::InstanceClaimed,
            Ok(W::RefusalNoCapacity) => RefusalReason::NoCapacity,
            Ok(W::RefusalIneligible) => RefusalReason::Ineligible,
            Ok(W::RefusalExpired) => RefusalReason::Expired,
            Ok(W::RefusalTicketInvalid) => RefusalReason::TicketInvalid,
            Ok(W::RefusalUnknownTarget) => RefusalReason::UnknownTarget,
            Ok(W::RefusalDisabled) => RefusalReason::Disabled,
            Ok(W::RefusalInternal) => RefusalReason::Internal,
            _ => RefusalReason::Unspecified,
        }
    }

    /// Whether the node may retry ratification inside its lease rather than
    /// killing the work immediately. Only transient manager-side conditions
    /// qualify — a refusal that is *about the claim* is final.
    pub fn retryable(&self) -> bool {
        matches!(self, RefusalReason::Internal)
    }
}

impl fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Mint a claim id. `clm_` prefixed so it is never confused with an
/// allocation id, an instance id, or a ticket id in a log line.
pub fn new_claim_id() -> String {
    format!("clm_{}", crate::util::random_string(24).to_lowercase())
}

/// Mint a boot ticket id (the JWT's `jti`).
pub fn new_ticket_id() -> String {
    format!("tkt_{}", crate::util::random_string(24).to_lowercase())
}

/// The Legion-specific claims carried by a boot ticket, extracted from a
/// verified JWT.
///
/// Extraction is deliberately separate from signature verification: the
/// signature says the manager minted it, and *this* says what it authorizes.
/// Both must pass, and the second is where every scoping mistake would live,
/// so it is pure and exhaustively tested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootTicketClaims {
    pub jti: String,
    pub job: String,
    pub job_config_id: String,
    pub task: String,
    /// Exactly one instance, when the ticket is narrowed to one.
    pub instance_id: Option<String>,
    /// Any instance with this (non-empty) prefix.
    pub instance_prefix: Option<String>,
    pub pool: String,
    pub gate: Gate,
    pub uses: u32,
    pub expires_at_unix: i64,
}

impl BootTicketClaims {
    /// Read the Legion claims out of a verified JWT claim set.
    ///
    /// Every field is required except the instance narrowing, of which
    /// exactly one form must be present — a ticket that names neither would
    /// authorize every instance of the task, which is not a capability
    /// Legion mints.
    pub fn from_claims(claims: &serde_json::Map<String, Value>) -> R<Self> {
        let string = |key: &str| -> R<String> {
            claims
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("boot ticket is missing {key}"))
        };

        let scope = string(CLAIM_SCOPE)?;
        if scope != BOOT_TICKET_SCOPE {
            bail!("token scope {scope:?} is not a boot ticket");
        }

        let instance_id = claims
            .get(CLAIM_INSTANCE)
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty());
        let instance_prefix = claims
            .get(CLAIM_INSTANCE_PREFIX)
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty());
        if instance_id.is_some() && instance_prefix.is_some() {
            bail!("boot ticket names both an instance and an instance prefix");
        }

        let gate = Gate::parse(&string(CLAIM_GATE)?)?;
        if gate == Gate::Off {
            bail!("boot ticket gate is `off`; it authorizes nothing");
        }

        let uses = claims
            .get(CLAIM_USES)
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .min(u64::from(u16::MAX)) as u32;
        let expires_at_unix = claims
            .get("exp")
            .and_then(Value::as_i64)
            .context("boot ticket is missing exp")?;

        Ok(BootTicketClaims {
            jti: string("jti")?,
            job: string(CLAIM_JOB)?,
            job_config_id: string(CLAIM_JOB_CONFIG)?,
            task: string(CLAIM_TASK)?,
            instance_id,
            instance_prefix,
            pool: string(CLAIM_POOL)?,
            gate,
            uses,
            expires_at_unix,
        })
    }

    /// Whether this ticket authorizes starting `instance_id` in `pool`.
    ///
    /// Checked against facts the verifier already holds — the node's own pool
    /// from its registration, the instance id the caller asked for — never
    /// against anything else the caller supplies. A stateless task passes an
    /// empty `instance_id`, which only a prefix-less, instance-less ticket
    /// could authorize, and those do not exist.
    pub fn authorizes(&self, instance_id: &str, pool: &str) -> R<()> {
        if self.pool != pool {
            bail!(
                "boot ticket is scoped to pool {} but this node is in pool {pool}",
                self.pool
            );
        }
        match (&self.instance_id, &self.instance_prefix) {
            (Some(exact), _) if exact == instance_id => Ok(()),
            (Some(exact), _) => {
                bail!("boot ticket is scoped to instance {exact}, not {instance_id}")
            }
            (None, Some(prefix)) if instance_id.starts_with(prefix.as_str()) => Ok(()),
            (None, Some(prefix)) => {
                bail!("instance {instance_id} does not match ticket prefix {prefix}")
            }
            (None, None) => bail!("boot ticket names no instance and no instance prefix"),
        }
    }
}

/// The Legion claims of a session ticket, extracted from a verified JWT.
///
/// A session ticket names no instance, prefix, or job config: the node mints
/// the session id and starts the spec of the envelope it holds. It is not
/// spent per session; the envelopes' room bounds what it can start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTicketClaims {
    pub jti: String,
    pub job: String,
    pub task: String,
    pub pool: String,
    pub expires_at_unix: i64,
}

impl SessionTicketClaims {
    pub fn from_claims(claims: &serde_json::Map<String, Value>) -> R<Self> {
        let string = |key: &str| -> R<String> {
            claims
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("session ticket is missing {key}"))
        };
        let scope = string(CLAIM_SCOPE)?;
        if scope != SESSION_TICKET_SCOPE {
            bail!(
                "ticket scope {scope:?} is not a session ticket; mint one with `legion \
                 boot-ticket create <job> --task <task> --scope session`"
            );
        }
        Ok(SessionTicketClaims {
            jti: string("jti")?,
            job: string(CLAIM_JOB)?,
            task: string(CLAIM_TASK)?,
            pool: string(CLAIM_POOL)?,
            expires_at_unix: claims
                .get("exp")
                .and_then(Value::as_i64)
                .context("session ticket is missing exp")?,
        })
    }

    /// Whether this ticket covers sessions of `job`/`task`.
    pub fn covers(&self, job: &str, task: &str) -> R<()> {
        if self.job != job || self.task != task {
            bail!(
                "session ticket {} covers sessions of {}/{}, not {job}/{task}",
                self.jti,
                self.job,
                self.task
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod edge_boot_test {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_session_ticket_needs_its_scope_and_names_one_task_test() -> R<()> {
        let claims = json!({
            "jti": "bt_1",
            "exp": 100,
            CLAIM_SCOPE: "session",
            CLAIM_JOB: "agents",
            CLAIM_TASK: "workspace",
            CLAIM_POOL: "agents",
        });
        let parsed = SessionTicketClaims::from_claims(claims.as_object().unwrap())?;
        assert!(parsed.covers("agents", "workspace").is_ok());
        assert!(parsed.covers("agents", "other").is_err());
        let mut boot = claims.clone();
        boot[CLAIM_SCOPE] = json!("boot");
        let refused = SessionTicketClaims::from_claims(boot.as_object().unwrap()).unwrap_err();
        assert!(refused.to_string().contains("--scope session"), "{refused}");
        Ok(())
    }

    #[test]
    fn gate_levels_order_from_off_to_serve() {
        assert!(Gate::Off < Gate::Stage);
        assert!(Gate::Stage < Gate::Boot);
        assert!(Gate::Boot < Gate::Serve);
    }

    #[test]
    fn the_lower_gate_always_wins() {
        // The safety claim the whole feature rests on: intersecting a task's
        // mode with a ticket's ceiling can only ever narrow.
        assert_eq!(Gate::Serve.min(Gate::Stage), Gate::Stage);
        assert_eq!(Gate::Stage.min(Gate::Serve), Gate::Stage);
        assert_eq!(Gate::Off.min(Gate::Serve), Gate::Off);
        assert_eq!(Gate::Boot.min(Gate::Boot), Gate::Boot);
    }

    #[test]
    fn only_boot_and_above_may_start_and_only_serve_may_open_the_network() {
        assert!(!Gate::Off.may_start());
        assert!(!Gate::Stage.may_start());
        assert!(Gate::Boot.may_start());
        assert!(Gate::Serve.may_start());

        assert!(!Gate::Boot.may_open_network());
        assert!(Gate::Serve.may_open_network());
    }

    #[test]
    fn unknown_wire_gate_reads_as_off() {
        // Fail closed under version skew: an unrecognized gate speculates
        // nothing rather than defaulting to the fastest thing it saw.
        assert_eq!(Gate::from_wire(9999), Gate::Off);
        assert_eq!(Gate::from_wire(Gate::Serve.to_wire()), Gate::Serve);
    }

    #[test]
    fn absent_block_resolves_to_off_with_the_default_lease() {
        let resolved = SpeculativeStartConfig::default()
            .resolve()
            .expect("resolve");
        assert_eq!(resolved.mode, Gate::Off);
        assert_eq!(resolved.ratify_timeout, DEFAULT_RATIFY_TIMEOUT);
        assert_eq!(resolved.on_refused, OnRefused::Kill);
    }

    #[test]
    fn a_bad_mode_is_an_admission_error_not_a_silent_downgrade() {
        let config = SpeculativeStartConfig {
            mode: Some("boott".into()),
            ..Default::default()
        };
        assert!(config.resolve().is_err());
    }

    #[test]
    fn ratify_timeout_is_bounded() {
        let too_long = SpeculativeStartConfig {
            mode: Some("boot".into()),
            ratify_timeout: Some("10m".into()),
            ..Default::default()
        };
        assert!(too_long.resolve().is_err());

        let zero = SpeculativeStartConfig {
            ratify_timeout: Some("0s".into()),
            ..Default::default()
        };
        assert!(zero.resolve().is_err());

        let sub_second = SpeculativeStartConfig {
            ratify_timeout: Some("500ms".into()),
            ..Default::default()
        };
        let err = sub_second.resolve().expect_err("sub-second lease");
        assert!(err.to_string().contains("shorter than 1s"), "{err}");

        let ok = SpeculativeStartConfig {
            mode: Some("serve".into()),
            ratify_timeout: Some("30s".into()),
            on_refused: Some("stop".into()),
        }
        .resolve()
        .expect("resolve");
        assert_eq!(ok.ratify_timeout, Duration::from_secs(30));
        assert_eq!(ok.on_refused, OnRefused::Stop);
    }

    fn ticket_claims(extra: Value) -> serde_json::Map<String, Value> {
        let mut base = json!({
            "jti": "tkt_abc",
            "exp": 1_770_000_300i64,
            CLAIM_SCOPE: "boot",
            CLAIM_JOB: "agent-sandboxes",
            CLAIM_JOB_CONFIG: "jc-7f3a91",
            CLAIM_TASK: "workspace",
            CLAIM_POOL: "edge-eu",
            CLAIM_GATE: "boot",
        });
        let base_map = base.as_object_mut().expect("object");
        for (key, value) in extra.as_object().expect("object") {
            base_map.insert(key.clone(), value.clone());
        }
        base_map.clone()
    }

    #[test]
    fn claims_parse_and_authorize_an_exact_instance() -> R<()> {
        let claims =
            BootTicketClaims::from_claims(&ticket_claims(json!({ CLAIM_INSTANCE: "user-42" })))?;
        assert_eq!(claims.gate, Gate::Boot);
        assert_eq!(claims.uses, 1, "tickets are single-use by default");
        claims.authorizes("user-42", "edge-eu")?;
        assert!(claims.authorizes("user-43", "edge-eu").is_err());
        assert!(claims.authorizes("user-42", "edge-us").is_err());
        Ok(())
    }

    #[test]
    fn a_prefix_ticket_covers_its_cohort_and_nothing_else() -> R<()> {
        let claims = BootTicketClaims::from_claims(&ticket_claims(
            json!({ CLAIM_INSTANCE_PREFIX: "user-" }),
        ))?;
        claims.authorizes("user-42", "edge-eu")?;
        claims.authorizes("user-", "edge-eu")?;
        assert!(claims.authorizes("admin-1", "edge-eu").is_err());
        Ok(())
    }

    #[test]
    fn a_ticket_naming_no_instance_authorizes_nothing() {
        let claims = BootTicketClaims::from_claims(&ticket_claims(json!({})));
        // Parsing succeeds (the claim set is well formed) but it can never
        // authorize a start — an empty prefix would mean "every instance".
        let claims = claims.expect("parses");
        assert!(claims.authorizes("user-42", "edge-eu").is_err());
        assert!(claims.authorizes("", "edge-eu").is_err());
    }

    #[test]
    fn a_ticket_with_both_narrowings_is_rejected() {
        let result = BootTicketClaims::from_claims(&ticket_claims(json!({
            CLAIM_INSTANCE: "user-42",
            CLAIM_INSTANCE_PREFIX: "user-",
        })));
        assert!(result.is_err());
    }

    #[test]
    fn a_workload_identity_token_is_not_a_boot_ticket() {
        // The `scope` claim is what stops a task's own identity token being
        // replayed at a worker's StartClaim endpoint.
        let mut claims = ticket_claims(json!({ CLAIM_INSTANCE: "user-42" }));
        claims.insert(CLAIM_SCOPE.into(), json!("task"));
        assert!(BootTicketClaims::from_claims(&claims).is_err());
    }

    #[test]
    fn a_gate_off_ticket_is_rejected_at_parse() {
        let mut claims = ticket_claims(json!({ CLAIM_INSTANCE: "user-42" }));
        claims.insert(CLAIM_GATE.into(), json!("off"));
        assert!(BootTicketClaims::from_claims(&claims).is_err());
    }

    #[test]
    fn refusal_reasons_round_trip_and_only_internal_is_retryable() {
        for reason in [
            RefusalReason::Unspecified,
            RefusalReason::InstanceClaimed,
            RefusalReason::NoCapacity,
            RefusalReason::Ineligible,
            RefusalReason::Expired,
            RefusalReason::TicketInvalid,
            RefusalReason::UnknownTarget,
            RefusalReason::Disabled,
            RefusalReason::Internal,
        ] {
            assert_eq!(RefusalReason::from_wire(reason.to_wire()), reason);
        }
        assert!(RefusalReason::Internal.retryable());
        assert!(!RefusalReason::InstanceClaimed.retryable());
        // A reason from a newer manager kills the work rather than retrying.
        assert!(!RefusalReason::from_wire(9999).retryable());
    }

    #[test]
    fn claim_ids_are_prefixed_and_unique() {
        let a = new_claim_id();
        let b = new_claim_id();
        assert!(a.starts_with("clm_"));
        assert_ne!(a, b);
        assert!(new_ticket_id().starts_with("tkt_"));
    }
}
