//! Shared plumbing for Apple's `container` CLI (macOS): the `container`
//! runtime and the `sandbox` runtime's dev mode both run on it.
//!
//! Two rules live here so both runtimes follow them:
//!
//! - **Names are scoped to the node.** Container names are global to the Mac,
//!   and two local clusters (or one cluster reset with `rm -rf .legion`) both
//!   number allocations from 1. A bare `legion-<alloc>` let one cluster's
//!   launch replace another's running container of the same number.
//! - **Every CLI call has a deadline.** A wedged `container` service makes
//!   the CLI hang indefinitely, and a hung call used to hang whatever was
//!   waiting on it: a launch, the heartbeat's usage sample, a teardown.

use crate::prelude::*;
use crate::structs::Allocation;
use crate::util::RUNTIME_CLI_TIMEOUT;
use std::process::{Command, Output};
use std::time::Duration;

/// Apple's container CLI. Installed via `brew install container`.
pub(crate) const BIN: &str = "container";

/// Label naming the node that launched a container: what stale-container
/// cleanup checks before it force-removes anything.
pub(crate) const NODE_LABEL: &str = "legion.node";

/// Label naming the allocation a container belongs to, for an operator
/// reading `container list`.
pub(crate) const ALLOCATION_LABEL: &str = "legion.allocation";

/// `container stop` signals, then waits out its own `--time` (5s by default)
/// before killing, so it gets more room than a query.
pub(crate) const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// `container image load` imports a whole image archive, which can take
/// minutes for a large image on a slow disk.
pub(crate) const IMAGE_LOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// `container stats --no-stream` runs on the worker's heartbeat, so it is
/// held to less than a query.
pub(crate) const STATS_TIMEOUT: Duration = Duration::from_secs(5);

/// The allocation's container name, `<prefix>-<node>-<alloc>`, with the node
/// id lowercased and reduced to `[a-z0-9-]`. Falls back to
/// `<prefix>-<alloc>` for an allocation with no node id.
pub(crate) fn node_scoped_name(prefix: &str, allocation: &Allocation) -> String {
    let node: String = allocation
        .node_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let node = node.trim_matches('-');
    if node.is_empty() {
        format!("{prefix}-{}", allocation.id)
    } else {
        format!("{prefix}-{node}-{}", allocation.id)
    }
}

/// `run` flags that mark a container as this node's allocation.
pub(crate) fn ownership_labels(allocation: &Allocation) -> Vec<String> {
    vec![
        "--label".to_string(),
        format!("{NODE_LABEL}={}", allocation.node_id),
        "--label".to_string(),
        format!("{ALLOCATION_LABEL}={}", allocation.id),
    ]
}

/// Run `container <args>` to completion, killed past `timeout`.
pub(crate) fn run(args: &[&str], timeout: Duration) -> R<Output> {
    crate::util::output_with_timeout(Command::new(BIN).args(args), timeout)
}

/// Who a container found under an allocation's name belongs to.
#[derive(Debug, PartialEq, Eq)]
enum Owner {
    /// `container inspect` found nothing.
    Absent,
    /// Labeled as this node's.
    ThisNode,
    /// Anything else, described for the error.
    Other(String),
}

/// Read the owner out of `container inspect` output: a JSON array of
/// container snapshots whose `configuration.labels` holds what `run
/// --label` set. The labels object is searched for rather than addressed
/// by path, so a reshuffle of the surrounding snapshot does not turn every
/// container into a foreign one.
fn owner_from_inspect(stdout: &[u8], node_id: &str) -> Owner {
    let Ok(parsed) = serde_json::from_slice::<Value>(stdout) else {
        return Owner::Other("its `container inspect` output was unreadable".into());
    };
    let first = match &parsed {
        Value::Array(items) => match items.first() {
            Some(item) => item,
            None => return Owner::Absent,
        },
        other => other,
    };
    fn find_labels(value: &Value) -> Option<&Map<String, Value>> {
        match value {
            Value::Object(map) => map
                .get("labels")
                .and_then(Value::as_object)
                .or_else(|| map.values().find_map(find_labels)),
            Value::Array(items) => items.iter().find_map(find_labels),
            _ => None,
        }
    }
    match find_labels(first)
        .and_then(|labels| labels.get(NODE_LABEL))
        .and_then(Value::as_str)
    {
        Some(owner) if owner == node_id => Owner::ThisNode,
        Some(owner) => Owner::Other(format!("it is labeled for node {owner:?}")),
        None => Owner::Other(format!("it carries no {NODE_LABEL} label")),
    }
}

/// Remove a container left behind under `name` by an earlier start of this
/// allocation on this node. `run --name` fails outright when the name is
/// taken, which happens whenever a start was not torn down cleanly (a worker
/// crash, or a `--rm` run killed before the runtime could reap it).
///
/// Only a container labeled as this node's is removed. Anything else under
/// the name fails the start with the command that would clear it: Legion
/// does not force-remove a container it cannot show it launched. A missing
/// container is the normal case and succeeds; a CLI that hangs fails after
/// [`RUNTIME_CLI_TIMEOUT`] instead of stalling the launch.
pub(crate) fn remove_stale(name: &str, allocation: &Allocation) -> R<()> {
    remove_stale_with(BIN, RUNTIME_CLI_TIMEOUT, name, allocation)
}

/// [`remove_stale`] against any `container`-shaped CLI, for tests.
fn remove_stale_with(
    program: &str,
    timeout: Duration,
    name: &str,
    allocation: &Allocation,
) -> R<()> {
    let run =
        |args: &[&str]| crate::util::output_with_timeout(Command::new(program).args(args), timeout);
    let inspect = run(&["inspect", name])?;
    if !inspect.status.success() {
        // "not found": nothing is holding the name.
        return Ok(());
    }
    match owner_from_inspect(&inspect.stdout, &allocation.node_id) {
        Owner::Absent => return Ok(()),
        Owner::ThisNode => {}
        Owner::Other(why) => bail!(
            "an Apple container named {name} already exists and is not this node's ({why}), \
             so Legion will not remove it; if it is stale, remove it with \
             `container rm --force {name}`"
        ),
    }
    let removed = run(&["rm", "--force", name])?;
    if !removed.status.success() {
        // A `--rm` container can be reaped between the inspect and the rm;
        // if it is still there, `run` reports the name conflict itself.
        debug!(
            container = name,
            stderr = %String::from_utf8_lossy(&removed.stderr).trim(),
            "container rm of a stale container failed"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocation(id: i64, node_id: &str) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources: json!({}),
            node_id: node_id.into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        }
    }

    #[test]
    fn names_carry_the_node_test() {
        assert_eq!(
            node_scoped_name("legion", &allocation(5, "Mike's Mac.local")),
            "legion-mike-s-mac-local-5"
        );
        assert_eq!(node_scoped_name("legion", &allocation(5, "")), "legion-5");
        // Two clusters' allocation 5 no longer share a name.
        assert_ne!(
            node_scoped_name("legion", &allocation(5, "dev-a1b2c3d4")),
            node_scoped_name("legion", &allocation(5, "dev-e5f6g7h8"))
        );
    }

    #[test]
    fn ownership_labels_name_node_and_allocation_test() {
        assert_eq!(
            ownership_labels(&allocation(3, "dev-a1b2c3d4")),
            vec![
                "--label",
                "legion.node=dev-a1b2c3d4",
                "--label",
                "legion.allocation=3"
            ]
        );
    }

    /// A fake `container` CLI: `inspect` prints `inspect_json` after
    /// sleeping `inspect_sleep` seconds (or fails when `inspect_json` is
    /// empty), and every call is appended to `calls.log`.
    fn fake_cli(dir: &std::path::Path, inspect_json: &str, inspect_sleep: u32) -> String {
        use std::os::unix::fs::PermissionsExt;
        let log = dir.join("calls.log");
        let script = dir.join("container");
        let inspect = if inspect_json.is_empty() {
            "echo 'container not found' >&2; exit 1".to_string()
        } else {
            format!("sleep {inspect_sleep}; printf '%s' '{inspect_json}'")
        };
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho \"$@\" >> {}\ncase \"$1\" in\n  inspect) {inspect} ;;\nesac\n",
                log.display()
            ),
        )
        .expect("write fake cli");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake cli");
        script.display().to_string()
    }

    fn calls(dir: &std::path::Path) -> String {
        std::fs::read_to_string(dir.join("calls.log")).unwrap_or_default()
    }

    #[test]
    fn stale_cleanup_removes_only_this_nodes_container_test() -> R<()> {
        let allocation = allocation(3, "n1");
        let quick = Duration::from_secs(10);

        // Nothing under the name: no rm.
        let dir = tempfile::tempdir()?;
        remove_stale_with(
            &fake_cli(dir.path(), "", 0),
            quick,
            "legion-n1-3",
            &allocation,
        )?;
        assert_eq!(calls(dir.path()), "inspect legion-n1-3\n");

        // Ours, from an earlier start on this node: removed.
        let dir = tempfile::tempdir()?;
        let ours = r#"[{"configuration":{"labels":{"legion.node":"n1"}}}]"#;
        remove_stale_with(
            &fake_cli(dir.path(), ours, 0),
            quick,
            "legion-n1-3",
            &allocation,
        )?;
        assert_eq!(
            calls(dir.path()),
            "inspect legion-n1-3\nrm --force legion-n1-3\n"
        );

        // The finding: another cluster's container under the name is left
        // alone, and the error says how to clear it by hand.
        let dir = tempfile::tempdir()?;
        let theirs = r#"[{"configuration":{"labels":{}}}]"#;
        let err = remove_stale_with(
            &fake_cli(dir.path(), theirs, 0),
            quick,
            "legion-n1-3",
            &allocation,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(calls(dir.path()), "inspect legion-n1-3\n");
        assert!(err.contains("not this node's"), "{err}");
        assert!(err.contains("container rm --force legion-n1-3"), "{err}");
        Ok(())
    }

    /// The other finding: a CLI that hangs fails the cleanup, naming the
    /// command, instead of blocking the launch indefinitely.
    #[test]
    fn stale_cleanup_gives_up_on_a_hung_cli_test() -> R<()> {
        let dir = tempfile::tempdir()?;
        let cli = fake_cli(dir.path(), "[]", 30);
        let started = std::time::Instant::now();
        let err = remove_stale_with(
            &cli,
            Duration::from_millis(300),
            "legion-n1-3",
            &allocation(3, "n1"),
        )
        .unwrap_err()
        .to_string();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(err.contains("inspect legion-n1-3"), "{err}");
        assert!(err.contains("did not finish"), "{err}");
        Ok(())
    }

    #[test]
    fn inspect_output_decides_the_owner_test() {
        let ours = br#"[{"configuration":{"id":"legion-n1-3","labels":{"legion.node":"n1","legion.allocation":"3"}},"status":"running"}]"#;
        assert_eq!(owner_from_inspect(ours, "n1"), Owner::ThisNode);
        assert!(
            matches!(owner_from_inspect(ours, "n2"), Owner::Other(why) if why.contains("\"n1\""))
        );

        let unlabeled = br#"[{"configuration":{"id":"legion-3","labels":{}},"status":"running"}]"#;
        assert!(
            matches!(owner_from_inspect(unlabeled, "n1"), Owner::Other(why) if why.contains("no legion.node"))
        );
        assert_eq!(owner_from_inspect(b"[]", "n1"), Owner::Absent);
        assert!(matches!(
            owner_from_inspect(b"not json", "n1"),
            Owner::Other(_)
        ));
    }
}
