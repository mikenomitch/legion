//! Resize: changing a running unit's `memory`, `memoryMax`, and `cpu` in
//! place (docs/resize.md).
//!
//! A resize is an override over the spec, never a new job version. The types
//! here are shared by the manager (which commits a top-level allocation's
//! resize as a compare-and-set on its row), the worker (which admits a
//! session's resize against its envelope and tells the runtime), and the
//! runtimes (which say what they can change live).

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The registration capability of a worker that answers `Worker.ResizeSession`.
/// A manager never sends a resize to a node without it.
pub const RESIZE_CAPABILITY: &str = "resize";

/// The three resources a resize can change. `memory` and `memoryMax` are in
/// MiB, `cpu` in MHz, as in a task's `resources` block.
pub const RESIZABLE_KEYS: [&str; 3] = ["memory", crate::resources::MEMORY_MAX_KEY, "cpu"];

/// A size: the resizable part of a `resources` block. An unset field is
/// "not declared" when it describes a unit, and "leave as is" when it is a
/// request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSize {
    /// The reservation, in MiB: what the scheduler packs on and the
    /// cgroup's `memory.low`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<f64>,
    /// The ceiling, in MiB: the cgroup's `memory.max`, or a guest's usable
    /// memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_max: Option<f64>,
    /// CPU in MHz: `cpu.weight`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
}

impl ResourceSize {
    /// The resizable amounts a `resources` object declares (`cpus` is read
    /// as `cpu`).
    pub fn of(resources: &Value) -> Self {
        let amount = |key: &str| {
            resources
                .get(key)
                .and_then(Value::as_f64)
                .filter(|amount| amount.is_finite())
        };
        ResourceSize {
            memory: amount("memory"),
            memory_max: amount(crate::resources::MEMORY_MAX_KEY),
            cpu: amount("cpu").or_else(|| amount("cpus")),
        }
    }

    /// Whether this request changes nothing.
    pub fn is_empty(&self) -> bool {
        self.memory.is_none() && self.memory_max.is_none() && self.cpu.is_none()
    }

    /// The same three amounts as a JSON object, unset ones left out.
    pub fn to_json(&self) -> Value {
        let mut map = Map::new();
        for (key, amount) in [
            ("memory", self.memory),
            (crate::resources::MEMORY_MAX_KEY, self.memory_max),
            ("cpu", self.cpu),
        ] {
            if let Some(amount) = amount {
                map.insert(key.to_string(), json_amount(amount));
            }
        }
        Value::Object(map)
    }

    /// Check a request's amounts on their own: positive and finite.
    pub fn validate_request(&self) -> R<()> {
        if self.is_empty() {
            bail!("a resize names at least one of memory, memoryMax, and cpu");
        }
        for (name, amount) in [
            ("memory", self.memory),
            ("memoryMax", self.memory_max),
            ("cpu", self.cpu),
        ] {
            if let Some(amount) = amount {
                if !amount.is_finite() || amount <= 0.0 {
                    bail!("{name} must be a positive amount, got {amount}");
                }
            }
        }
        Ok(())
    }

    /// `resources` with this request laid over it: the unit's effective
    /// resources after the resize. Every other key (devices, custom
    /// resources, `cores`) is kept as it was.
    ///
    /// Refused when the result would put the ceiling below the reservation.
    /// A unit that declares no `memoryMax` keeps none unless the request
    /// sets one, so its ceiling stays its reservation.
    pub fn apply_to(&self, resources: &Value) -> R<Value> {
        let mut map = resources.as_object().cloned().unwrap_or_default();
        if let Some(memory) = self.memory {
            map.insert("memory".to_string(), json_amount(memory));
        }
        if let Some(memory_max) = self.memory_max {
            map.insert(
                crate::resources::MEMORY_MAX_KEY.to_string(),
                json_amount(memory_max),
            );
        }
        if let Some(cpu) = self.cpu {
            // A unit declared with the `cpus` alias keeps one cpu key.
            map.remove("cpus");
            map.insert("cpu".to_string(), json_amount(cpu));
        }
        let after = Value::Object(map);
        let size = ResourceSize::of(&after);
        if let (Some(memory), Some(memory_max)) = (size.memory, size.memory_max) {
            if memory_max < memory {
                bail!(
                    "memoryMax ({}) would be below memory ({}): the ceiling cannot be under the \
                     reservation. Hint: pass both --memory and --memory-max",
                    json_amount(memory_max),
                    json_amount(memory)
                );
            }
        }
        Ok(after)
    }

    /// Whether moving from `self` to `to` raises a reservation the node or
    /// envelope has to have free (`memory` or `cpu`). The ceiling is never
    /// reserved, so raising only `memoryMax` is not a grow.
    pub fn grows_to(&self, to: &ResourceSize) -> bool {
        let up =
            |from: Option<f64>, to: Option<f64>| to.unwrap_or(0.0) > from.unwrap_or(0.0) + 1e-9;
        up(self.memory, to.memory) || up(self.cpu, to.cpu)
    }
}

/// An amount as JSON: an integer when it is one, so `2048` does not become
/// `2048.0` in a stored row or a reply.
pub fn json_amount(amount: f64) -> Value {
    if amount.fract() == 0.0 && amount.abs() < 9.0e15 {
        serde_json::json!(amount as i64)
    } else {
        serde_json::json!(amount)
    }
}

/// What a resize did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResizeOutcome {
    /// The unit runs at the new size.
    Applied,
    /// The unit's elastic envelope is growing to make room; ask again.
    Deferred,
    /// The new size does not fit, or a shrink would go below current use.
    Refused,
    /// The runtime cannot make this change while the unit runs.
    NeedsRestart,
    /// The runtime (or the node's worker) cannot resize at all.
    NotSupported,
}

impl ResizeOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            ResizeOutcome::Applied => "applied",
            ResizeOutcome::Deferred => "deferred",
            ResizeOutcome::Refused => "refused",
            ResizeOutcome::NeedsRestart => "needs_restart",
            ResizeOutcome::NotSupported => "not_supported",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "applied" => ResizeOutcome::Applied,
            "deferred" => ResizeOutcome::Deferred,
            "refused" => ResizeOutcome::Refused,
            "needs_restart" => ResizeOutcome::NeedsRestart,
            "not_supported" => ResizeOutcome::NotSupported,
            _ => return None,
        })
    }
}

/// What a runtime can change on a running unit. The default is nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResizeSupport {
    /// The reservation (`memory.low`).
    pub memory: bool,
    /// The ceiling (`memory.max`, or a guest's balloon).
    pub memory_max: bool,
    /// The CPU weight.
    pub cpu: bool,
}

impl ResizeSupport {
    /// Every resizable resource, live.
    pub const ALL: ResizeSupport = ResizeSupport {
        memory: true,
        memory_max: true,
        cpu: true,
    };

    pub fn any(&self) -> bool {
        self.memory || self.memory_max || self.cpu
    }

    /// The first resource `request` changes that this runtime cannot.
    pub fn first_unsupported(&self, request: &ResourceSize) -> Option<&'static str> {
        [
            ("memory", request.memory.is_some(), self.memory),
            ("memoryMax", request.memory_max.is_some(), self.memory_max),
            ("cpu", request.cpu.is_some(), self.cpu),
        ]
        .into_iter()
        .find(|(_, asked, supported)| *asked && !supported)
        .map(|(name, _, _)| name)
    }
}

/// A runtime's answer to one resize. `NeedsRestart` and `NotSupported` are
/// answers, never actions: a runtime never restarts a unit to resize it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResizeAnswer {
    Applied,
    /// A shrink below current use without `force`, with the reason.
    Refused(String),
    NeedsRestart(String),
    NotSupported(String),
}

impl ResizeAnswer {
    pub fn outcome(&self) -> ResizeOutcome {
        match self {
            ResizeAnswer::Applied => ResizeOutcome::Applied,
            ResizeAnswer::Refused(_) => ResizeOutcome::Refused,
            ResizeAnswer::NeedsRestart(_) => ResizeOutcome::NeedsRestart,
            ResizeAnswer::NotSupported(_) => ResizeOutcome::NotSupported,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            ResizeAnswer::Applied => "",
            ResizeAnswer::Refused(message)
            | ResizeAnswer::NeedsRestart(message)
            | ResizeAnswer::NotSupported(message) => message,
        }
    }
}

/// What a runtime is asked to apply: the unit's full resources before and
/// after, so it never has to work out what changed from a partial request.
#[derive(Debug, Clone, PartialEq)]
pub struct ResizeChange {
    /// The unit's effective `resources` now.
    pub from: Value,
    /// The unit's effective `resources` after the resize.
    pub to: Value,
    /// Allow a shrink below current use: the runtime may reclaim or kill.
    pub force: bool,
    /// Decide and answer without changing anything.
    pub dry_run: bool,
}

impl ResizeChange {
    pub fn from_size(&self) -> ResourceSize {
        ResourceSize::of(&self.from)
    }

    pub fn to_size(&self) -> ResourceSize {
        ResourceSize::of(&self.to)
    }
}

/// The override a resize leaves on a unit: what was set, by whom, and why.
/// Stored on a top-level allocation's row (`allocation.resources_override`);
/// a task-level override (`legion job resize`) uses the same shape.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesOverride {
    /// The amounts that differ from the spec.
    #[serde(flatten)]
    pub size: ResourceSize,
    /// The spec's amounts when the override was written, so a reader can
    /// show "spec 1000" without the job config.
    #[serde(default)]
    pub spec: ResourceSize,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// Who resized: an ACL accessor id or principal, never a secret.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by: String,
    /// RFC 3339.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub at: String,
}

impl ResourcesOverride {
    /// The override that takes a unit from `spec` to `effective`: only the
    /// amounts that differ. `None` when they are the same, which is how a
    /// resize back to the spec removes the override.
    pub fn between(
        spec: &Value,
        effective: &Value,
        reason: &str,
        by: &str,
        at: &str,
    ) -> Option<Self> {
        let (spec, effective) = (ResourceSize::of(spec), ResourceSize::of(effective));
        let differs = |a: Option<f64>, b: Option<f64>| match (a, b) {
            (Some(a), Some(b)) => (a - b).abs() > 1e-9,
            (None, None) => false,
            _ => true,
        };
        let size = ResourceSize {
            memory: effective
                .memory
                .filter(|_| differs(spec.memory, effective.memory)),
            memory_max: effective
                .memory_max
                .filter(|_| differs(spec.memory_max, effective.memory_max)),
            cpu: effective.cpu.filter(|_| differs(spec.cpu, effective.cpu)),
        };
        if size.is_empty() {
            return None;
        }
        Some(ResourcesOverride {
            size,
            spec,
            reason: reason.to_string(),
            by: by.to_string(),
            at: at.to_string(),
        })
    }
}

/// `1024 -> 2048` style rendering of one amount for messages.
pub fn mib(amount: f64) -> String {
    if amount >= 1024.0 && (amount / 1024.0 * 10.0).fract() == 0.0 {
        format!("{}GiB", json_amount(amount / 1024.0))
    } else {
        format!("{}MiB", json_amount(amount))
    }
}

/// Parse a memory amount as a user types it: a bare number is MiB, and
/// `MiB`, `GiB`, `M`, `G` suffixes are accepted (`2GiB`, `512MiB`, `1.5G`).
pub fn parse_memory_mib(raw: &str) -> R<f64> {
    let text = raw.trim();
    let lower = text.to_ascii_lowercase();
    let (number, scale) = if let Some(n) = lower.strip_suffix("gib").or(lower.strip_suffix("gb")) {
        (n, 1024.0)
    } else if let Some(n) = lower.strip_suffix("mib").or(lower.strip_suffix("mb")) {
        (n, 1.0)
    } else if let Some(n) = lower.strip_suffix('g') {
        (n, 1024.0)
    } else if let Some(n) = lower.strip_suffix('m') {
        (n, 1.0)
    } else {
        (lower.as_str(), 1.0)
    };
    let amount: f64 = number
        .trim()
        .parse()
        .with_context(|| format!("'{raw}' is not a memory amount (use 512, 512MiB, or 2GiB)"))?;
    if !amount.is_finite() || amount <= 0.0 {
        bail!("'{raw}' is not a positive memory amount");
    }
    Ok(amount * scale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_request_lays_over_the_units_resources_and_keeps_the_rest() -> R<()> {
        let resources = json!({ "cpus": 500, "memory": 1024, "gpu": 1 });
        let after = ResourceSize {
            memory: Some(2048.0),
            cpu: Some(1000.0),
            ..Default::default()
        }
        .apply_to(&resources)?;
        assert_eq!(after, json!({ "cpu": 1000, "memory": 2048, "gpu": 1 }));
        Ok(())
    }

    #[test]
    fn a_ceiling_below_the_reservation_is_refused() {
        let resources = json!({ "memory": 1024, "memoryMax": 2048 });
        let err = ResourceSize {
            memory: Some(4096.0),
            ..Default::default()
        }
        .apply_to(&resources)
        .unwrap_err();
        assert!(format!("{err}").contains("memoryMax (2048) would be below memory (4096)"));
        // Without a declared ceiling the reservation is the ceiling.
        assert!(ResourceSize {
            memory: Some(4096.0),
            ..Default::default()
        }
        .apply_to(&json!({ "memory": 1024 }))
        .is_ok());
    }

    #[test]
    fn only_a_raised_reservation_is_a_grow() {
        let from = ResourceSize::of(&json!({ "memory": 1024, "memoryMax": 2048, "cpu": 500 }));
        let ceiling_only = ResourceSize {
            memory_max: Some(4096.0),
            ..from
        };
        assert!(!from.grows_to(&ceiling_only));
        assert!(from.grows_to(&ResourceSize {
            cpu: Some(501.0),
            ..from
        }));
        assert!(!from.grows_to(&ResourceSize {
            memory: Some(512.0),
            ..from
        }));
    }

    #[test]
    fn an_override_holds_only_what_differs_from_the_spec() {
        let spec = json!({ "memory": 1024, "cpu": 1000 });
        let effective = json!({ "memory": 1024, "cpu": 2000 });
        let record = ResourcesOverride::between(&spec, &effective, "batch week", "accessor:1", "t")
            .expect("differs");
        assert_eq!(record.size.cpu, Some(2000.0));
        assert_eq!(record.size.memory, None);
        assert_eq!(record.spec.cpu, Some(1000.0));
        assert!(ResourcesOverride::between(&spec, &spec, "", "", "").is_none());
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json["cpu"], json!(2000.0));
        assert_eq!(json["reason"], json!("batch week"));
    }

    #[test]
    fn memory_amounts_parse_with_and_without_units() -> R<()> {
        assert_eq!(parse_memory_mib("512")?, 512.0);
        assert_eq!(parse_memory_mib("2GiB")?, 2048.0);
        assert_eq!(parse_memory_mib("1.5g")?, 1536.0);
        assert_eq!(parse_memory_mib("256MiB")?, 256.0);
        assert!(parse_memory_mib("lots").is_err());
        assert!(parse_memory_mib("0").is_err());
        assert_eq!(mib(2048.0), "2GiB");
        assert_eq!(mib(1500.0), "1500MiB");
        Ok(())
    }

    #[test]
    fn support_names_the_first_resource_it_cannot_change() {
        let support = ResizeSupport {
            memory: true,
            memory_max: false,
            cpu: true,
        };
        let request = ResourceSize {
            memory_max: Some(1.0),
            ..Default::default()
        };
        assert_eq!(support.first_unsupported(&request), Some("memoryMax"));
        assert_eq!(ResizeSupport::ALL.first_unsupported(&request), None);
        assert!(!ResizeSupport::default().any());
    }
}
