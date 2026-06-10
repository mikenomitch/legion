//! `legion capabilities` — a machine-readable catalog of what Legion can do.
//!
//! Agents need to *discover* a tool's surface before they can use it safely.
//! This command emits a stable catalog of Legion's operations, each annotated
//! with whether it mutates cluster state and whether it is safe to call
//! without human confirmation. The same catalog backs the MCP server's tool
//! list, so the CLI, gRPC, and MCP surfaces stay in agreement.

use crate::cmd::helpers::json_flag;
use crate::cmd::output::{self, OutputFormat};
use crate::prelude::*;
use serde::Serialize;
use tabled::Tabled;

/// One discoverable Legion operation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capability {
    /// Stable dotted identifier, e.g. `job.run`.
    pub name: &'static str,
    /// Representative CLI invocation.
    pub cli: &'static str,
    /// Backing gRPC method (`service/Method`), when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<&'static str>,
    /// One-line description of the operation.
    pub summary: &'static str,
    /// Whether the operation changes cluster state.
    pub mutates: bool,
    /// Whether an agent can call this without explicit human confirmation.
    /// Read-only operations are safe; mutating ones default to unsafe.
    pub read_only_safe: bool,
}

/// The canonical catalog of agent-relevant Legion operations.
///
/// Keep this in sync with `proto/legion.proto` and the CLI subcommands. It is
/// the single source of truth shared by `legion capabilities` and the MCP
/// server's tool list.
pub fn catalog() -> Vec<Capability> {
    vec![
        // --- Jobs: read ---
        Capability {
            name: "job.list",
            cli: "legion job list",
            grpc: Some("Manager/ListJobs"),
            summary: "List all jobs and their status.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.info",
            cli: "legion job info <NAME>",
            grpc: Some("Manager/GetJobInfo"),
            summary: "Inspect a single job's current state.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.spec",
            cli: "legion job spec <NAME>",
            grpc: Some("Manager/GetJobSpec"),
            summary: "Fetch the source submissions that define a job.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.plan",
            cli: "legion job plan <PATH>",
            grpc: Some("Manager/PlanJob"),
            summary: "Dry-run: preview which nodes allocations would land on.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.validate",
            cli: "legion job validate <PATH>",
            grpc: None,
            summary: "Render and validate a jobspec locally without submitting.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.schema",
            cli: "legion job schema",
            grpc: None,
            summary: "Emit the JSON Schema for jobspecs and built-in task kinds.",
            mutates: false,
            read_only_safe: true,
        },
        // --- Jobs: write ---
        Capability {
            name: "job.run",
            cli: "legion job run <PATH>",
            grpc: Some("Manager/RunJob"),
            summary: "Submit or update a job. Supports --wait and --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "job.wait",
            cli: "legion job wait <NAME>",
            grpc: Some("Manager/GetRolloutStatus"),
            summary: "Block until a job's active rollout converges or times out.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.logs",
            cli: "legion job logs [<NAME>] [--allocation <ID>] [--follow]",
            grpc: Some("Manager/GetAllocationLogs"),
            summary: "Fetch captured stdout/stderr for a job's allocations.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.stop",
            cli: "legion job stop <NAME>",
            grpc: Some("Manager/StopJob"),
            summary: "Stop a running job. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "job.destroy",
            cli: "legion job destroy <NAME>",
            grpc: Some("Manager/DestroyJob"),
            summary: "Destroy a job and its allocations. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Rollouts ---
        Capability {
            name: "rollout.list",
            cli: "legion job rollout list",
            grpc: Some("Manager/ListRollouts"),
            summary: "List rollouts, optionally filtered by job.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "rollout.status",
            cli: "legion job rollout status --id <ID>",
            grpc: Some("Manager/GetRolloutStatus"),
            summary: "Inspect rollout progress and allocation health.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "rollout.events",
            cli: "legion job rollout events --id <ID>",
            grpc: Some("Manager/ListRolloutEvents"),
            summary: "Show a rollout's event history.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "rollout.advance",
            cli: "legion job rollout advance --id <ID>",
            grpc: Some("Manager/AdvanceRollout"),
            summary: "Manually advance a paused rollout.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Nodes ---
        Capability {
            name: "node.list",
            cli: "legion node list",
            grpc: Some("Manager/GetNodes"),
            summary: "List worker nodes and their health.",
            mutates: false,
            read_only_safe: true,
        },
        // --- State store (the agent's introspection workhorse) ---
        Capability {
            name: "state.query",
            cli: "legion state query <SQL>",
            grpc: Some("Manager/QueryStateStore"),
            summary: "Run read-only SQL against the manager state store.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "state.schema",
            cli: "legion state schema",
            grpc: Some("Manager/GetStateStoreSchema"),
            summary: "Describe the state store's tables and columns.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "state.seed-queries",
            cli: "legion state seed-queries",
            grpc: Some("Manager/UpsertSavedStateQuery"),
            summary: "Install the curated library of agent introspection queries.",
            mutates: true,
            read_only_safe: false,
        },
        // --- KV ---
        Capability {
            name: "kv.get",
            cli: "legion kv get <KEY>",
            grpc: Some("Manager/GetKv"),
            summary: "Read a value from the KV store.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "kv.list",
            cli: "legion kv list",
            grpc: Some("Manager/ListKv"),
            summary: "List KV entries under a prefix.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "kv.put",
            cli: "legion kv put <KEY> <VALUE>",
            grpc: Some("Manager/PutKv"),
            summary: "Write a value to the KV store.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Discovery ---
        Capability {
            name: "capabilities",
            cli: "legion capabilities",
            grpc: None,
            summary: "List the operations described by this catalog.",
            mutates: false,
            read_only_safe: true,
        },
    ]
}

#[derive(Tabled)]
struct CapabilityRow {
    name: String,
    mutates: String,
    #[tabled(rename = "agent-safe")]
    agent_safe: String,
    summary: String,
}

/// Build the clap configuration for `legion capabilities`.
pub fn config() -> Command {
    Command::new("capabilities")
        .about("List Legion's agent-callable operations (machine-readable with --json)")
        .arg(json_flag())
}

/// Handle `legion capabilities`.
#[instrument(err, skip_all)]
pub async fn handle(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let caps = catalog();

    if format.is_json() {
        output::print_success(&caps);
        return Ok(());
    }

    let rows = caps
        .into_iter()
        .map(|cap| CapabilityRow {
            name: cap.name.to_string(),
            mutates: if cap.mutates { "yes" } else { "no" }.to_string(),
            agent_safe: if cap.read_only_safe { "yes" } else { "confirm" }.to_string(),
            summary: cap.summary.to_string(),
        })
        .collect::<Vec<_>>();
    crate::cmd::helpers::print_table(rows);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_non_empty_and_consistent() {
        let caps = catalog();
        assert!(!caps.is_empty());
        for cap in &caps {
            assert!(!cap.name.is_empty());
            assert!(!cap.summary.is_empty());
            // Mutating operations must never be marked agent-safe.
            if cap.mutates {
                assert!(!cap.read_only_safe, "{} must not be agent-safe", cap.name);
            }
        }
    }

    #[test]
    fn capability_names_are_unique() {
        let caps = catalog();
        let mut names: Vec<&str> = caps.iter().map(|c| c.name).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(count, names.len(), "capability names must be unique");
    }

    #[test]
    fn config_parses() {
        let m = config()
            .try_get_matches_from(["capabilities", "--json"])
            .unwrap();
        assert!(m.get_flag("json"));
    }
}
