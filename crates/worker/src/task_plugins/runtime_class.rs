//! OCI runtime classes for `container` tasks (review finding PLG3;
//! docs/task-runtime-container.md, "Runtime classes").
//!
//! A worker type names extra OCI runtimes, such as gVisor (`runsc`) or Kata:
//!
//! ```nickel
//! container = { runtimeClasses = {
//!   gvisor = { path = "/usr/local/bin/runsc", userNamespace = false, isolatesRoot = true },
//!   kata   = { path = "/usr/bin/kata-runtime", userNamespace = false, isolatesRoot = true },
//! } }
//! ```
//!
//! and a task picks one with `config.runtimeClass = "gvisor"`. The worker
//! probes each class at start (`<path> --version`) and advertises the ones
//! that answer as the node attribute [`NODE_ATTR`]; the task carries an
//! implicit `container.runtimeClasses contains "<class>"` constraint, so it
//! only places where the class exists. Without `runtimeClass` a task runs on
//! the node's default runtime (runc or crun), exactly as before.
//!
//! **The privilege floor under a class.** Legion's floor is a user namespace
//! with idmapped mounts and its seccomp profile (docs/task-runtime-
//! container.md, "Security"). gVisor and Kata implement neither the way runc
//! does, but they do not need them for the floor's purpose: container root
//! is root in gVisor's user-space kernel or in Kata's VM, not on the host.
//! A class says what it supports (`userNamespace`, `seccomp`) and whether it
//! keeps root off the host by itself (`isolatesRoot`). Legion then:
//!
//! - applies a part the class supports, as usual;
//! - drops a part the class does not support when the class
//!   `isolatesRoot` (logged at start: the class's own isolation stands in);
//! - **refuses the start** when the class neither supports the part nor
//!   isolates root, naming the class and the task field, rather than run the
//!   task below the floor it asked for.

use std::collections::BTreeMap;

use crate::prelude::*;

/// The node attribute listing the runtime classes a node can run.
pub const NODE_ATTR: &str = "container.runtimeClasses";

/// How the worker hands its configured classes to the container runtime
/// (the same channel as the capability allowlist).
pub const CLASSES_ENV: &str = "LEGION_CONTAINER_RUNTIME_CLASSES";

pub use crate::structs::config::RuntimeClassConfig;

/// A class name: letters, digits, `-` and `_`, at most 63 characters.
pub fn validate_name(name: &str) -> R<()> {
    if name.is_empty()
        || name.len() > 63
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!(
            "runtime class name {name:?} must be 1-63 letters, digits, '-' or '_' \
             (for example `gvisor`)"
        );
    }
    Ok(())
}

/// Validate a worker type's classes at manager startup.
pub fn validate_classes(classes: &BTreeMap<String, RuntimeClassConfig>) -> R<()> {
    for (name, class) in classes {
        validate_name(name).with_context(|| format!("container.runtimeClasses.{name}"))?;
        if !class.path.starts_with('/') {
            bail!(
                "container.runtimeClasses.{name}.path must be absolute, got {:?}",
                class.path
            );
        }
    }
    Ok(())
}

/// Publish the worker's classes to the container runtime (worker start).
pub fn publish(classes: &BTreeMap<String, RuntimeClassConfig>) {
    if classes.is_empty() {
        std::env::remove_var(CLASSES_ENV);
        return;
    }
    match serde_json::to_string(classes) {
        Ok(json) => std::env::set_var(CLASSES_ENV, json),
        Err(err) => warn!(error = %err, "Failed to publish container runtime classes"),
    }
}

/// The classes the worker published.
pub fn configured() -> BTreeMap<String, RuntimeClassConfig> {
    std::env::var(CLASSES_ENV)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

/// The classes whose runtime answers `--version`: the ones the node
/// advertises. A class that does not is logged and left out, so no task that
/// needs it places here.
pub fn probe(classes: &BTreeMap<String, RuntimeClassConfig>) -> Vec<String> {
    classes
        .iter()
        .filter_map(|(name, class)| match preflight(name, class) {
            Ok(()) => Some(name.clone()),
            Err(err) => {
                warn!(class = %name, error = %format!("{err:#}"), "Container runtime class unavailable; not advertised");
                None
            }
        })
        .collect()
}

/// Confirm a class's runtime is installed and runs.
pub fn preflight(name: &str, class: &RuntimeClassConfig) -> R<()> {
    let output = crate::util::output_with_timeout(
        std::process::Command::new(&class.path).arg("--version"),
        crate::util::RUNTIME_CLI_TIMEOUT,
    )
    .with_context(|| {
        format!(
            "runtime class {name}: `{} --version` could not run (is the runtime installed?)",
            class.path
        )
    })?;
    if !output.status.success() {
        bail!(
            "runtime class {name}: `{} --version` exited with {}: {}",
            class.path,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// The privilege floor a container actually gets under `class`: whether to
/// give it a user namespace and Legion's seccomp profile. `wants_*` are the
/// task's own `security.userNamespace` and `security.seccomp = "default"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveFloor {
    pub user_namespace: bool,
    pub seccomp: bool,
}

/// See the module docs: apply, drop under `isolatesRoot`, or refuse.
pub fn effective_floor(
    class_name: &str,
    class: &RuntimeClassConfig,
    wants_user_namespace: bool,
    wants_seccomp: bool,
) -> R<EffectiveFloor> {
    let part = |wanted: bool, supported: bool, field: &str, what: &str| -> R<bool> {
        if !wanted || supported {
            return Ok(wanted);
        }
        if class.isolates_root {
            return Ok(false);
        }
        bail!(
            "runtime class {class_name} cannot apply {what}, which this task asks for \
             ({field}, on by default), and the class does not isolate root by itself \
             (isolatesRoot is false). Set {field} to opt out on the task (which makes it \
             host-level), or run it on a class that supports it"
        )
    };
    Ok(EffectiveFloor {
        user_namespace: part(
            wants_user_namespace,
            class.user_namespace,
            "security.userNamespace",
            "a user namespace with idmapped mounts",
        )?,
        seccomp: part(
            wants_seccomp,
            class.seccomp,
            "security.seccomp",
            "Legion's seccomp profile",
        )?,
    })
}

/// The task's `config.runtimeClass`, if it names one.
pub fn task_class(config: &Value) -> Option<String> {
    config
        .get("runtimeClass")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}

/// Give a task that names a runtime class the implicit constraint that its
/// node advertises that class. Idempotent.
pub fn constrain_to_class(task: &mut crate::structs::TaskConfig) {
    let Some(class) = task_class(&task.config) else {
        return;
    };
    let rule = json!({ "key": NODE_ATTR, "operator": "contains", "value": class });
    let mut rules = match std::mem::take(&mut task.constraints) {
        Value::Array(rules) => rules,
        Value::Object(map) => map
            .into_iter()
            .map(|(key, value)| json!({ "key": key, "value": value }))
            .collect(),
        Value::Null => Vec::new(),
        other => {
            task.constraints = other;
            return;
        }
    };
    if !rules.contains(&rule) {
        rules.push(rule);
    }
    task.constraints = Value::Array(rules);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(user_namespace: bool, seccomp: bool, isolates_root: bool) -> RuntimeClassConfig {
        RuntimeClassConfig {
            path: "/usr/local/bin/runsc".into(),
            user_namespace,
            seccomp,
            isolates_root,
        }
    }

    #[test]
    fn the_floor_is_applied_dropped_or_refused_per_class_test() {
        // A class that supports everything keeps the task's floor.
        assert_eq!(
            effective_floor("full", &class(true, true, false), true, true).unwrap(),
            EffectiveFloor {
                user_namespace: true,
                seccomp: true
            }
        );
        // gVisor: no userns, isolates root -> dropped, not refused.
        assert_eq!(
            effective_floor("gvisor", &class(false, true, true), true, true).unwrap(),
            EffectiveFloor {
                user_namespace: false,
                seccomp: true
            }
        );
        // A runc wrapper with no userns that isolates nothing -> refused.
        let err = effective_floor("plain", &class(false, true, false), true, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("runtime class plain") && err.contains("security.userNamespace"));
        // ... unless the task opted out itself.
        assert!(effective_floor("plain", &class(false, true, false), false, true).is_ok());
        let err = effective_floor("noseccomp", &class(true, false, false), true, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("security.seccomp"), "{err}");
    }

    #[test]
    fn a_task_naming_a_class_is_constrained_to_nodes_that_have_it_test() {
        let mut task = crate::structs::TaskConfig::new("sandboxed", "container");
        task.config = json!({ "image": "x", "command": ["true"], "runtimeClass": "gvisor" });
        task.constraints = json!({ "region": "eu" });
        constrain_to_class(&mut task);
        constrain_to_class(&mut task);
        let rules = task.constraints.as_array().unwrap();
        assert_eq!(rules.len(), 2, "idempotent: {rules:?}");
        assert_eq!(rules[1]["key"], NODE_ATTR);
        assert_eq!(rules[1]["value"], "gvisor");
        let node = crate::structs::Node {
            id: "n".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "region": "eu", NODE_ATTR: ["gvisor", "kata"] }),
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        let set = crate::constraints::ConstraintSet::new(&task.constraints).unwrap();
        assert!(set.is_satisfied(&node, None));
        let mut without = node.clone();
        without.config_attrs = json!({ "region": "eu", NODE_ATTR: ["kata"] });
        assert!(!set.is_satisfied(&without, None));
    }

    #[test]
    fn classes_validate_and_probe_test() -> R<()> {
        let mut classes = BTreeMap::new();
        classes.insert("ok".to_string(), class(true, true, false));
        validate_classes(&classes)?;
        classes.insert(
            "rel".to_string(),
            RuntimeClassConfig {
                path: "runsc".into(),
                ..class(true, true, false)
            },
        );
        assert!(validate_classes(&classes).is_err());
        assert!(validate_name("bad name").is_err());
        // `/bin/sh --version` answers on most systems; a missing binary
        // never does.
        let mut probe_me = BTreeMap::new();
        probe_me.insert(
            "missing".to_string(),
            RuntimeClassConfig {
                path: "/nonexistent/legion-runtime".into(),
                ..class(true, true, false)
            },
        );
        assert!(probe(&probe_me).is_empty());
        Ok(())
    }
}
