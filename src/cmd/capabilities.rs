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
            cli: "legion job list [--status STATUS]",
            grpc: Some("Manager/ListJobs"),
            summary: "List all jobs and their status, optionally filtered to one status.",
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
            summary: "Dry-run: preview which nodes allocations would land on, and which running allocations the submit would preempt.",
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
            summary: "Emit the JSON Schema for jobspecs and built-in task runtimes.",
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
            name: "job.scale",
            cli: "legion job scale <NAME> [--count N | --reset]",
            grpc: Some("Manager/ScaleTask"),
            summary: "Change a task's count without a new job version or rollout (docs/autoscaling.md). Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "job.scale-status",
            cli: "legion job scale <NAME>",
            grpc: Some("Manager/GetJobScaleStatus"),
            summary: "Per-task spec vs override vs effective counts for a job.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.destroy",
            cli: "legion job destroy <NAME>",
            grpc: Some("Manager/DestroyJob"),
            summary: "Destroy a job and its allocations. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Job templates (see docs/job-templates.md) ---
        Capability {
            name: "template.list",
            cli: "legion template list [-q <TEXT>]",
            grpc: Some("Manager/ListJobTemplates"),
            summary: "List cluster-resident job templates (latest-version summaries).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "template.get",
            cli: "legion template get <NAME> [--version N] [--source]",
            grpc: Some("Manager/GetJobTemplate"),
            summary: "Show one template version: variables, source, and version history.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "template.render",
            cli: "legion template render <NAME> [--var K=V]...",
            grpc: Some("Manager/RenderJobTemplate"),
            summary: "Dry-run a template: validate variables and print the rendered jobspec.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "template.create",
            cli: "legion template create <NAME> -f <PATH>",
            grpc: Some("Manager/UpsertJobTemplate"),
            summary: "Upload a template file as the next immutable version of NAME.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "template.run",
            cli: "legion template run <NAME> [--var K=V]... [--wait]",
            grpc: Some("Manager/RunJobTemplate"),
            summary:
                "Render a template with variables and submit the job. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "template.delete",
            cli: "legion template delete <NAME>",
            grpc: Some("Manager/DeleteJobTemplate"),
            summary: "Delete a template and all of its versions (running jobs keep running).",
            mutates: true,
            read_only_safe: false,
        },
        // --- Stateful instances (keyed mode; see docs/stateful-workloads.md) ---
        Capability {
            name: "instance.list",
            cli: "legion instance list <JOB>",
            grpc: Some("Manager/ListInstances"),
            summary: "List a job's stateful instances (slot and keyed, incl. tombstones).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "instance.get",
            cli: "legion instance get <ID>",
            grpc: Some("Manager/GetInstance"),
            summary: "Inspect one stateful instance by id.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "instance.create",
            cli: "legion instance create <JOB> [--id <KEY>]",
            grpc: Some("Manager/CreateInstance"),
            summary:
                "Create a keyed-mode instance (never-recycled id). Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.create-batch",
            cli: "legion instance create <JOB> --id <KEY>... --count <N>",
            grpc: Some("Manager/CreateInstances"),
            summary:
                "Create several keyed instances atomically (all-or-nothing: explicit ids plus N \
                 server-minted). Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.stop",
            cli: "legion instance stop <ID>",
            grpc: Some("Manager/StopInstance"),
            summary: "Pause a keyed instance: stop its allocation, retain row + storage handle \
                 (reversible via instance.start). Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.start",
            cli: "legion instance start <ID>",
            grpc: Some("Manager/StartInstance"),
            summary: "Resume a stopped keyed instance: re-place it with the same id and retained \
                 storage handle. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.destroy",
            cli: "legion instance destroy <ID>",
            grpc: Some("Manager/DestroyInstance"),
            summary:
                "Destroy a keyed-mode instance and drop its storage. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.replace",
            cli: "legion instance replace <ID>",
            grpc: Some("Manager/ReplaceInstance"),
            summary: "Operator override: force-replace an instance's allocation stuck on a LOST \
                 node without waiting for onLost.replaceAfter (split-brain risk if the node is \
                 only partitioned). Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Volumes (see docs/volumes.md) ---
        Capability {
            name: "volume.list",
            cli: "legion volume list [--job <J>] [--node <N>] [--driver <D>]",
            grpc: Some("Manager/ListVolumes"),
            summary: "List volumes (storage behind stateful instances), with state and node pin.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "volume.get",
            cli: "legion volume get <ID>",
            grpc: Some("Manager/GetVolume"),
            summary: "Inspect one volume: driver, params, node pin, mount path, last error.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "volume.delete",
            cli: "legion volume delete <ID> [--force]",
            grpc: Some("Manager/DeleteVolume"),
            summary: "Delete a volume and reclaim its storage (DESTROYS DATA). --force \
                 tombstones without driver confirmation for a node that is gone for good. \
                 Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "volume.drivers",
            cli: "legion volume drivers",
            grpc: Some("Manager/ListVolumeDrivers"),
            summary: "List volume drivers installed across the cluster, with capabilities and \
                 which nodes can actually use them.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "volume.driver.test",
            cli: "legion volume driver test <NAME>",
            grpc: None,
            summary: "Run the volume contract conformance suite against a driver locally \
                 (mount/write/unmount/remount/read/destroy plus idempotency). No manager needed.",
            mutates: false,
            read_only_safe: true,
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
        Capability {
            name: "node.cadence",
            cli: "legion node cadence",
            grpc: Some("Manager/GetHeartbeatCadence"),
            summary: "Report whether the fleet is sustaining its heartbeat cadence: freshness \
                 ratio, lost window, sustainable fleet size, and the heartbeatSeconds value to \
                 set if it is falling behind.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "node.cordon",
            cli: "legion node cordon <ID>",
            grpc: Some("Manager/SetNodeAvailability"),
            summary: "Mark a node unschedulable; existing allocations keep running. \
                 Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.drain",
            cli: "legion node drain <ID>",
            grpc: Some("Manager/SetNodeAvailability"),
            summary: "Mark a node unschedulable and evict its allocations (stateless move \
                 immediately; stateful stay protected by their onLost policy). Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.uncordon",
            cli: "legion node uncordon <ID>",
            grpc: Some("Manager/SetNodeAvailability"),
            summary: "Restore a cordoned/drained node to the scheduling pool. \
                 Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.join-token.create",
            cli: "legion join-token create",
            grpc: Some("Manager/CreateJoinToken"),
            summary: "Create a join token that introduces a worker to the cluster.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.join-token.list",
            cli: "legion join-token list",
            grpc: Some("Manager/ListJoinTokens"),
            summary: "List worker join tokens (metadata only).",
            mutates: false,
            read_only_safe: true,
        },
        // --- Scheduler events ---
        Capability {
            name: "events.list",
            cli: "legion events list",
            grpc: Some("Manager/ListEvents"),
            summary: "List recent scheduler events with filters and an after-id cursor.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "node.join-token.delete",
            cli: "legion join-token delete <ID>",
            grpc: Some("Manager/DeleteJoinToken"),
            summary: "Revoke a worker join token.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Edge boot (docs/edge-boot.md) ---
        Capability {
            name: "boot-ticket.create",
            cli: "legion boot-ticket create <JOB> --task <T> --pool <P>",
            grpc: Some("Manager/IssueBootTicket"),
            summary: "Mint a short-lived capability letting an edge node start one workload.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "boot-ticket.list",
            cli: "legion boot-ticket list",
            grpc: Some("Manager/ListBootTickets"),
            summary: "List outstanding boot tickets (metadata only; token bytes are never shown).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "sandbox.start",
            cli: "legion sandbox start <JOB> --id <ID> --node <ADDR> --ticket <T>",
            grpc: Some("Worker/StartClaim"),
            summary: "Start a workload directly on an edge node from a boot ticket.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.claims",
            cli: "legion node claims <NODE>",
            grpc: Some("Manager/GetNode"),
            summary: "List a node's live provisional (unratified) allocations.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "events.follow",
            cli: "legion events follow",
            grpc: Some("Manager/StreamEvents"),
            summary: "Stream scheduler events live (NDJSON envelopes in --json mode).",
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
        Capability {
            name: "state.backup",
            cli: "legion state backup [--out <PATH>]",
            grpc: Some("Manager/GetStateBackup"),
            summary: "Write a consistent server-side backup file of the state store: a \
                 point-in-time image of that node's applied state (VACUUM INTO; runs only on \
                 the node leading every raft lane, not a cluster-wide snapshot at one index). \
                 Restore via `legion start manager --restore-from <path>`.",
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
        // --- KV encryption keyring (docs/kv-encryption.md) ---
        Capability {
            name: "keyring.status",
            cli: "legion keyring status",
            grpc: Some("Manager/GetKeyringStatus"),
            summary: "Show KV encryption on this manager: root key, data keys, plaintext rows, and the next step.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "keyring.rotate",
            cli: "legion keyring rotate",
            grpc: Some("Manager/RotateDataKey"),
            summary: "Rotate the KV data key now; values are re-encrypted in the background.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "keyring.rewrap",
            cli: "legion keyring rewrap [--plaintext]",
            grpc: Some("Manager/RewrapKv"),
            summary: "Re-encrypt every KV value under the active data key (or decrypt all with --plaintext).",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "keyring.root_key_rewrap",
            cli: "legion keyring root-key rewrap",
            grpc: Some("Manager/RewrapRootKey"),
            summary: "Re-wrap every data key under the current root key (completes a root key rotation).",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "keyring.reset",
            cli: "legion keyring reset --i-understand-encrypted-values-will-be-lost",
            grpc: Some("Manager/ResetKeyring"),
            summary: "Break glass after losing the root key: delete undecryptable values and start a fresh keyring.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Auth & ACL ---
        Capability {
            name: "acl.whoami",
            cli: "legion acl whoami",
            grpc: Some("Manager/WhoAmI"),
            summary: "Show whether ACLs are enabled and what access the caller has.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.auth-methods",
            cli: "legion login --method <METHOD>",
            grpc: Some("Manager/GetAuthMethods"),
            summary: "List the auth methods available for login (public metadata only).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.login",
            cli: "legion login --method <METHOD>",
            grpc: Some("Manager/Login"),
            summary: "Exchange username/password or an external JWT for an ACL token.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.bootstrap",
            cli: "legion acl bootstrap",
            grpc: Some("Manager/BootstrapAcl"),
            summary: "One-time creation of the initial management token.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.policy.list",
            cli: "legion acl policy list",
            grpc: Some("Manager/ListAclPolicies"),
            summary: "List ACL policies.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.policy.get",
            cli: "legion acl policy get <NAME>",
            grpc: Some("Manager/GetAclPolicy"),
            summary: "Show one ACL policy and its rules.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.policy.apply",
            cli: "legion acl policy apply <NAME> <FILE>",
            grpc: Some("Manager/UpsertAclPolicy"),
            summary: "Create or replace an ACL policy.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.policy.delete",
            cli: "legion acl policy delete <NAME>",
            grpc: Some("Manager/DeleteAclPolicy"),
            summary: "Delete an ACL policy.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.token.list",
            cli: "legion acl token list",
            grpc: Some("Manager/ListAclTokens"),
            summary: "List ACL tokens (metadata only; secrets are never shown).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.token.create",
            cli: "legion acl token create --name <NAME> --policy <POLICY>",
            grpc: Some("Manager/CreateAclToken"),
            summary: "Create an ACL token attached to policies.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.token.delete",
            cli: "legion acl token delete <ACCESSOR_ID>",
            grpc: Some("Manager/DeleteAclToken"),
            summary: "Revoke an ACL token.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.user.list",
            cli: "legion acl user list",
            grpc: Some("Manager/ListAclUsers"),
            summary: "List username/password ACL users.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.user.upsert",
            cli: "legion acl user upsert <USERNAME> --password <PASSWORD>",
            grpc: Some("Manager/UpsertAclUser"),
            summary: "Create or update a username/password ACL user.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "acl.user.delete",
            cli: "legion acl user delete <USERNAME>",
            grpc: Some("Manager/DeleteAclUser"),
            summary: "Delete a username/password ACL user.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Service discovery ---
        Capability {
            name: "service.list",
            cli: "legion service list",
            grpc: Some("Manager/ListServices"),
            summary: "List declared services with healthy/total endpoint counts.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "service.resolve",
            cli: "legion service resolve <SERVICE> [--one] [--all]",
            grpc: Some("Manager/ResolveService"),
            summary: "Resolve a service to its healthy endpoint addresses; --one prints a single \
                 random address:port for scripts.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "service.doctor",
            cli: "legion service doctor <SERVICE>",
            grpc: None,
            summary: "Walk the discovery layers (declared, placed, node, port, health) and name \
                 the failing one; also reports the service's connect intentions.",
            mutates: false,
            read_only_safe: true,
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

    #[test]
    fn service_discovery_commands_are_catalogued_read_only() {
        let caps = catalog();
        for name in ["service.list", "service.resolve", "service.doctor"] {
            let cap = caps
                .iter()
                .find(|cap| cap.name == name)
                .unwrap_or_else(|| panic!("{name} missing from the capability catalog"));
            assert!(!cap.mutates, "{name} must not mutate");
            assert!(cap.read_only_safe, "{name} must be agent-safe");
            assert!(cap.cli.starts_with("legion service "), "{name} cli");
        }
    }
}
