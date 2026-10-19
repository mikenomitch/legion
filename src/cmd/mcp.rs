//! `legion mcp serve` — a Model Context Protocol (MCP) server for Legion.
//!
//! MCP is the emerging standard for exposing tools to LLM agents. This server
//! speaks MCP over stdio (newline-delimited JSON-RPC 2.0) and exposes Legion's
//! operations as MCP tools, so any MCP-capable agent can discover and drive a
//! Legion cluster without hand-rolling a gRPC client or scraping CLI text.
//!
//! ## Transport
//!
//! One JSON-RPC message per line on stdin; one response line on stdout. This
//! is the MCP stdio transport contract (messages must not contain embedded
//! newlines).
//!
//! ## Safety
//!
//! By default only read-only / introspection tools are exposed. Mutating tools
//! (`legion_job_run`, `legion_job_stop`, `legion_job_scale`,
//! `legion_job_destroy`, `legion_instance_create`, `legion_instance_stop`,
//! `legion_instance_start`, `legion_instance_promote`,
//! `legion_instance_destroy`) are gated behind `--allow-write`, mirroring the
//! `read_only_safe` flag in the capabilities catalog. Even with
//! `--allow-write`, `legion_state_query` always runs in read-only mode (no SQL
//! writes).

use crate::cmd::helpers::connect_manager_client;
use crate::cmd::output::CliError;
use crate::prelude::*;
use crate::structs::{
    exec_client_frame, exec_server_frame, CreateInstanceRequest, CreateInstancesRequest,
    DeleteJobTemplateRequest, DeleteVolumeRequest, DestroyInstanceRequest, DestroyJobRequest,
    ExecClientFrame, ExecInput, ExecStart, GetAllocationLogsRequest, GetInstanceRequest,
    GetJobInfoRequest, GetJobSpecRequest, GetJobTemplateRequest, GetLedgerRequest,
    GetMeshCaRequest, GetNodesRequest, GetRolloutStatusRequest, GetStateStoreSchemaRequest,
    GetVolumeRequest, JobConfig, ListEventsRequest, ListInstancesRequest, ListJobActionsRequest,
    ListJobTemplatesRequest, ListJobsRequest, ListNetPoliciesRequest, ListRolloutsRequest,
    ListServiceEdgesRequest, ListServicesRequest, ListVolumeDriversRequest, ListVolumesRequest,
    PlanJobRequest, QueryStateStoreRequest, RenderJobTemplateRequest, ReplaceInstanceRequest,
    ResolveServiceRequest, RotateMeshCaRequest, RunJobRequest, RunJobTemplateRequest,
    SetNodeAvailabilityRequest, ShowNetPolicyRequest, StartInstanceRequest, StopInstanceRequest,
    StopJobRequest, SuspendInstanceRequest, TestNetPolicyRequest, UpsertJobTemplateRequest,
    UpsertNetPolicyRequest,
};
// Phase 6 (fork and named snapshots).
use crate::structs::{ForkInstanceRequest, SetSnapshotAliasRequest};
// Sandbox-scaling Phase 6 (session mode).
use crate::structs::PromoteInstanceRequest;
use crate::task_plugins::validate_job_config;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// MCP protocol revision this server implements.
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// Build the clap configuration for `legion mcp`.
pub fn config() -> Command {
    let serve_cmd = Command::new("serve")
        .about("Run an MCP server over stdio for agent integrations")
        .arg(
            Arg::new("allow-write")
                .long("allow-write")
                .help("Expose mutating tools (run/stop/destroy). Off by default.")
                .action(ArgAction::SetTrue),
        );

    Command::new("mcp")
        .about("Model Context Protocol server for AI agents")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(serve_cmd)
}

/// Handle `legion mcp ...`.
pub async fn handle(sub_matches: &ArgMatches) -> R<()> {
    match sub_matches.subcommand() {
        Some(("serve", args)) => serve(args.get_flag("allow-write")).await,
        _ => unreachable!(),
    }
}

/// A single MCP tool definition.
struct ToolDef {
    name: &'static str,
    description: &'static str,
    /// JSON Schema for the tool's arguments.
    input_schema: fn() -> Value,
    /// Whether the tool mutates cluster state (and so requires `--allow-write`).
    mutates: bool,
}

fn empty_schema() -> Value {
    json!({ "type": "object", "properties": {}, "additionalProperties": false })
}

fn job_list_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "description": "Only jobs with this exact status (case-insensitive), one of running, pending, failed, scheduled, complete, suspended, idle, stopped. Omit for all."
            }
        },
        "additionalProperties": false
    })
}

fn namespace_name_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "name": { "type": "string", "description": "Namespace name." } },
        "required": ["name"],
        "additionalProperties": false
    })
}

fn namespace_apply_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Namespace name (DNS label)." },
            "description": { "type": "string" },
            "meta": { "type": "object", "additionalProperties": { "type": "string" } },
            "dry_run": { "type": "boolean", "description": "Validate and report reaching ACL rules without writing." }
        },
        "required": ["name"],
        "additionalProperties": false
    })
}

fn namespace_delete_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Namespace name." },
            "cascade": { "type": "boolean", "description": "Stop and destroy everything in it first." },
            "dry_run": { "type": "boolean", "description": "List what it holds without deleting." },
            "idempotency_key": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })
}

fn name_arg_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "name": { "type": "string", "description": "Job name." } },
        "required": ["name"],
        "additionalProperties": false
    })
}

/// The full tool catalog. Read-only tools are always available; mutating tools
/// are filtered out unless `--allow-write` is set.
fn tool_defs() -> Vec<ToolDef> {
    vec![
        ToolDef {
            name: "legion_capabilities",
            description: "List Legion's agent-callable operations and whether each mutates state.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_list",
            description: "List jobs and their status, optionally only those in one status. The reply's statusCounts gives the per-status totals.",
            input_schema: job_list_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_namespace_list",
            description: "List the namespaces this token can see, with job counts. A namespace is a name and permission scope, not an isolation boundary.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_namespace_get",
            description: "Show one namespace: what it holds by kind and, for tokens with acl:read, the ACL rules that reach it.",
            input_schema: namespace_name_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_namespace_apply",
            description: "Create a namespace, or update its description and meta. Names are DNS labels (lowercase letters, digits, '-'; at most 63). Set dry_run to see the ACL rules that would reach it without creating it.",
            input_schema: namespace_apply_schema,
            mutates: true,
        },
        ToolDef {
            name: "legion_namespace_delete",
            description: "Delete a namespace. Refused while it holds jobs, volumes or snapshots unless cascade is set, which stops and destroys them first (synchronously). dry_run lists the contents without deleting.",
            input_schema: namespace_delete_schema,
            mutates: true,
        },
        ToolDef {
            name: "legion_job_info",
            description: "Inspect a single job's current state. A job whose placement found no room has status `blocked` and a `blocked` block naming the key it waits on, the task, and its per-placement shortfall; it places on its own when capacity appears. For a gang task, `blocked.gang` says how many replicas fit at the last attempt (`placeable`, `running`) against the quorum it must place together (`min`), and `deadline` (Unix seconds, 0 for none) when its `placementTimeout` fails the job if it is still short. A gang job with status `failed` (timed out, or failed by its `onMemberLoss`) places nothing until it is run again (docs/gang-scheduling.md).",
            input_schema: name_arg_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_spec",
            description: "Fetch the source submissions that define a job.",
            input_schema: name_arg_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_schema",
            description: "Get the JSON Schema for jobspecs and built-in task runtimes. Use this before authoring a jobspec.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_validate",
            description: "Validate a rendered jobspec (JSON) without submitting it.",
            input_schema: || json!({
                "type": "object",
                "properties": { "spec_json": { "type": "string", "description": "Rendered jobspec as a JSON string." } },
                "required": ["spec_json"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_job_plan",
            description: "Dry-run a rendered jobspec: which nodes its allocations would land on, and — the reason to run this before submitting — which RUNNING allocations the submit would preempt, with their job, task, priority band and grace window. A non-empty `preemptions` list means the submit costs somebody else something; treat it as requiring confirmation. `preemptionMode` says whether an empty list means \"nothing would be displaced\" or \"preemption is off\". `gangReports` has one entry per task that sets `gang`: `wouldPlace: false` means the job would place NOTHING and wait (blocked, holding nothing) until `min - running` replicas fit together, with `placeable`, `eligibleNodes` and `reason` saying how far it got — resize the gang, wait, or add capacity (docs/gang-scheduling.md).",
            input_schema: || json!({
                "type": "object",
                "properties": { "spec_json": { "type": "string", "description": "Rendered jobspec as a JSON string." } },
                "required": ["spec_json"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_rollout_list",
            description: "List rollouts, optionally filtered by job id.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Filter by job id." },
                    "active_only": { "type": "boolean", "description": "Only active rollouts." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_rollout_status",
            description: "Inspect rollout progress and allocation health by rollout id.",
            input_schema: || json!({
                "type": "object",
                "properties": { "id": { "type": "integer", "description": "Rollout id." } },
                "required": ["id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_instance_list",
            description: "List a job's stateful instances (slot and keyed, including terminated tombstones). An instance with `coldBoot` set lost its running processes: its last resume or restore failed and it booted fresh on its volume (`coldBoot.reason` says why). An `archived` instance is suspended with its checkpoint moved to an archive manifest (`manifestUrl`); start it as usual. With `include_idle`, archived instances that have no row (suspend.archive = \"manifestOnly\") are listed from the bucket after the rows, one page at a time: pass `nextIdlePageToken` back as `idle_page_token`. A session task's running sessions are listed too, with `durable: false`, a page at a time (session_limit, default 5000; pass nextSessionPageToken back as session_page_token), with sessionCount and sessionsByNode totals; `complete: false` means some node's sessions could not be read, so the list may be missing some.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Job name." },
                    "group": { "type": "string", "description": "Optional task-group filter." },
                    "include_idle": { "type": "boolean", "description": "Also list archived instances with no row, from the contentStore. Default false." },
                    "idle_page_token": { "type": "string", "description": "With include_idle: the previous reply's nextIdlePageToken." },
                    "idle_page_size": { "type": "integer", "minimum": 1, "maximum": 10000, "description": "With include_idle: the most bucket-listed instances to return (default 1000)." },
                    "session_limit": { "type": "integer", "minimum": 0, "maximum": 50000, "description": "The most running sessions to list, in id order (default 5000). 0 lists none and returns only sessionCount and sessionsByNode, which is the cheap way to count sessions at any scale." },
                    "session_page_token": { "type": "string", "description": "The previous reply's nextSessionPageToken, for the next page of sessions." }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_instance_get",
            description: "Inspect one stateful instance by id (state, slot, current allocation, storage handle). For suspend: `checkpointId`, `suspendReason` (manual, idle, or pressure), and, while the task's suspend.idle policy is watching a running instance, `idleSince` and `idleAfterSeconds`. `coldBoot` (snapshotId, reason, at) is set when the last resume or restore failed and the instance booted fresh on its volume under onRestoreFailure = \"cold\": its running processes and memory are gone, so do not assume a resumed session survived. Also reports a live PROVISIONAL claim when a node has started this instance but the placement is not durable yet (docs/edge-boot.md) — do not build on a provisional instance, it may still be killed.",
            input_schema: || json!({
                "type": "object",
                "properties": { "instance_id": { "type": "string", "description": "Instance id." } },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_volume_list",
            description: "List volumes — the persistent storage behind stateful instances (see docs/volumes.md). Every filter is optional. `nodeLocal: true` with a `nodeId` means the owning instance can ONLY be replaced on that node; `lastError` explains a volume stuck in pending or destroying.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Filter by owning job." },
                    "node_id": { "type": "string", "description": "Filter by the node the volume lives on." },
                    "driver": { "type": "string", "description": "Filter by volume driver (local-dir, host, nfs, tmpfs, ...)." },
                    "instance_id": { "type": "string", "description": "Filter by owning stateful instance." },
                    "state": { "type": "string", "description": "Filter by state: pending, created, destroying, destroyed." },
                    "include_destroyed": { "type": "boolean", "description": "Also return destroyed tombstones (reclaimed storage). Default false; filtering on state=destroyed returns them regardless." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_volume_get",
            description: "Inspect one volume: driver, params, driver metadata, node pin, mount path, and last driver error.",
            input_schema: || json!({
                "type": "object",
                "properties": { "volume_id": { "type": "string", "description": "Volume id (the second half of an instance's driver/volume_id storage handle)." } },
                "required": ["volume_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_volume_drivers",
            description: "List the volume drivers installed across the cluster with their capabilities and which nodes can actually use them. Use this to answer why a job was rejected for an unknown or unusable volume driver.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_volume_delete",
            description: "Delete a volume and reclaim its storage. DESTROYS DATA: the owning instance mints a fresh, empty volume on its next placement. Reclaim is confirmed asynchronously by the owning worker; `force: true` tombstones it immediately WITHOUT driver confirmation, leaving any bytes behind — only for a node that is gone for good. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "volume_id": { "type": "string", "description": "Volume id." },
                    "force": { "type": "boolean", "description": "Skip driver confirmation (node is permanently gone). Default false." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the first result." }
                },
                "required": ["volume_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_snapshot_list",
            description: "List snapshots of instances, newest first (docs/suspend-and-resume.md). Every filter is optional. `state` is capturing, local (captured; bytes on `nodeId`), published (in the content store; restores on any compatible node and survives its node), failed (`error` says why), deleting, or lost (its node was removed).",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Filter by job." },
                    "instance_id": { "type": "string", "description": "Filter by the instance it was taken from." },
                    "state": { "type": "string", "description": "Filter by state: capturing, local, published, failed, deleting, lost." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_snapshot_get",
            description: "Inspect one snapshot by id (or by name with job_id), including its manifest: memory parts, volume snapshot refs, and the compat block (VMM and CPU) that a restore is placed against.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "snapshot": { "type": "string", "description": "Snapshot id, or a snapshot name when job_id is given." },
                    "job_id": { "type": "string", "description": "Job the named snapshot belongs to." }
                },
                "required": ["snapshot"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_snapshot_create",
            description: "Snapshot a running instance. kind `disk` (default) copies its stateful.storage volume; kind `memory` also captures guest memory so a restore resumes running processes (sandbox tasks on Firecracker or cloud-hypervisor with a `snapshots` block only). Returns at once in state `capturing`; poll legion_snapshot_get until `local` or `failed`. A memory snapshot contains the workload's in-memory secrets. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance to snapshot." },
                    "kind": { "type": "string", "enum": ["disk", "memory"], "description": "Default disk." },
                    "name": { "type": "string", "description": "Optional name, unique within the job." },
                    "publish": { "type": "boolean", "description": "Publish it to the node's contentStore once captured (state local -> published), so it restores on any compatible node and survives the node. Memory snapshots of sandboxes without stateful.storage only. Default false." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the first snapshot." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_snapshot_delete",
            description: "Delete a snapshot. Its node reclaims the bytes on its next heartbeat. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "snapshot_id": { "type": "string", "description": "Snapshot id." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the first result." }
                },
                "required": ["snapshot_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_snapshot_alias_set",
            description: "Point a snapshot name at a local or published manual snapshot of the same job, creating or moving the name (a golden snapshot is used by name: legion_instance_create with from_snapshot = the name). Repointing never touches the old snapshot. Mutating; needs snapshot:write.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Job the name belongs to." },
                    "name": { "type": "string", "description": "The name ([A-Za-z0-9._-]{1,64}, not starting with snap-)." },
                    "snapshot": { "type": "string", "description": "Snapshot id, or another name of the job." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key." }
                },
                "required": ["job_id", "name", "snapshot"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_snapshot_restore",
            description: "Roll a keyed instance back to one of its own snapshots, in place: its allocation is replaced by one started from the snapshot on the node that holds it. Placement fails with a named reason when the node's CPU or VMM no longer matches a memory snapshot. The outcome arrives as a `snapshot.restored` or `snapshot.restore_failed` event; a failed restore boots the instance cold on its retained storage. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Keyed instance to roll back." },
                    "snapshot": { "type": "string", "description": "Snapshot id or name." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the first result." }
                },
                "required": ["instance_id", "snapshot"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_snapshot_warm",
            description: "Pre-fetch a published snapshot's parts onto every available node that matches the constraints and could restore it, so a later resume, restore, or fork there starts from local bytes and placement prefers those nodes. Best effort and idempotent: nodes that already hold the parts are skipped, and outcomes arrive as `snapshot.warmed` / `snapshot.warm_failed` events. Only a `published` snapshot can be warmed. Triggers downloads on the selected nodes, so it is gated like a mutating tool.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "snapshot": { "type": "string", "description": "Snapshot id, or a snapshot name when job_id is given." },
                    "job_id": { "type": "string", "description": "Job the named snapshot belongs to." },
                    "constraints": { "type": "array", "items": { "type": "string" }, "description": "Node attribute filters, each key=value (for example region=us-west). Empty warms every compatible node." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the first reply." }
                },
                "required": ["snapshot"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_node_list",
            description: "List worker nodes and their health. Each node carries `heartbeatSource`: `live` means the answering manager observed the beat itself, `checkpoint` means the timestamp comes from a periodic replicated checkpoint and is up to `liveness.checkpointInterval` (30s) behind, empty means none has been observed. The node's `status` is exact either way (docs/node-liveness.md).",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_node_cadence",
            description: "Report whether the fleet is sustaining its heartbeat cadence. Returns the freshness ratio (1.0 = every node checked in on time; false node loss begins at served_interval/lost_window, 0.20 at the defaults), the per-fleet lost window, the sustainable node count for this control plane, and the `heartbeatSeconds` value to set if the fleet is falling behind. Read this before concluding that nodes have died: a low ratio means the control plane is behind, not that the machines are gone (docs/heartbeat-cadence-and-scale.md).",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_node_usage",
            description: "Show one node's memory and CPU: schedulable capacity, the sum of reservations placed on it, and what its allocations actually use, per allocation (memoryCurrentBytes, memoryPeakBytes, memoryAnonBytes, cpuMillicores, idleSeconds). Usage comes from cgroup reports that workers with resourceEnforcement.enabled send every 10 seconds; `usageReported: false` means none reached this manager, and `reportAgeSeconds` well above 10 means the node stopped reporting. Each allocation also carries `band`, `memoryMaxMib`, and its place in the node's memory pressure order (`pressureRank`, 1 = acted on first, 0 = never; `pressureSteps`, e.g. [\"reclaim\", \"evict-bursting\"], or [\"reclaim\", \"suspend\"] for a task that sets suspend.onPressure, which is suspended with its state instead of stopped), and `memory` carries `oversubscribeRatio` and `pressureResponse` (whether the node is under pressure now). Use it to compare reservations with real use before resizing a job, and to see which allocations a short node would stop first (docs/resource-enforcement.md).",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node to inspect." }
                },
                "required": ["node_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_node_usage_list",
            description: "Show memory and CPU for many nodes in one call: per node, schedulable capacity (with `oversubscribeRatio`), the sum of reservations, measured use, `usageReported`, and `memory.pressureResponse` (whether the node is under memory pressure now). CPU carries `cores` and `usedMillicores`, which are filled even on nodes that report no clock speed. Per-allocation rows are omitted unless `include_allocations` is true. Empty `node_ids` means every node (up to 500; `truncated` says when there were more). Use it to find tight or pressured nodes before drilling in with legion_node_usage (docs/resource-enforcement.md).",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "node_ids": { "type": "array", "items": { "type": "string" }, "description": "Nodes to report on; omit for every node." },
                    "include_allocations": { "type": "boolean", "description": "Also return each node's per-allocation rows. Default false." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_node_content",
            description: "Show what one node holds for content locality: the content its worker advertises to the manager's location index (digest, bytes, how many nodes hold it, and the snapshot and part role it belongs to when known), whether the worker can publish and checkpoint, and the node's snapshots (captured there, or published elsewhere with parts fetched or warmed there) with `partsHere`/`partsTotal`. `indexReported: false` means no content report reached this manager. Use it to see why placement preferred a node, or whether a node is warm for a snapshot before a resume or fork (docs/artifacts.md, \"Content locality\").",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node to inspect." }
                },
                "required": ["node_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_manager_list",
            description: "List every manager in the control plane from the replicated raft membership: node_id, role (leader|follower|learner, or standalone outside raft), advertiseAddr, voter, and whether the answering manager reached it just now (reachable, probeError, probeLatencyMs), plus each reachable member's term and appliedIndex. Any manager answers with the full list. Use it to find the leader, spot a manager that is down or partitioned (reachable=false), or one lagging on appliedIndex (docs/raft-operations.md).",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_scheduler_concurrency",
            description: "Report how many evals the manager will PLAN at once, and why. Returns the current limit against its floor/ceiling (`evalThreads`), the smoothed plan-conflict rate, the fallback rate (races absorbed by candidate lists — these are successes, not failures), goodput in placements/sec, and a `state` of steady|throttled|probing|saturated. Read this before concluding that a cluster is slow or full: `throttled` means the scheduler is deliberately admitting fewer planners and work will drain, while `saturated` means backing off did NOT help and the conflicts are capacity-driven — the answer there is more nodes, not waiting or resubmitting (docs/adaptive-scheduler-concurrency.md).",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_boot_ticket_create",
            description: "Mint a boot ticket: a short-lived capability that lets one edge node start one workload without calling a manager (docs/edge-boot.md). Names an exact job config, one task, one instance (or a non-empty instance prefix), one node pool, and a gate ceiling. Single-use by default and revocable only by expiry, so prefer short TTLs. The token is returned ONCE. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Job the ticket may start work for." },
                    "task": { "type": "string", "description": "Task within that job." },
                    "pool": { "type": "string", "description": "Node pool the ticket may be redeemed in." },
                    "instance_id": { "type": "string", "description": "Exact instance the ticket may start. Use this or instance_prefix." },
                    "instance_prefix": { "type": "string", "description": "Any instance with this prefix. Wider capability than instance_id." },
                    "gate": { "type": "string", "enum": ["stage", "boot", "serve"], "description": "Ceiling on how far the node may get before ratification. Capped by the task's own speculativeStart.mode." },
                    "job_config_id": { "type": "string", "description": "Pin to an exact job config. Default: the job's current config." },
                    "ttl": { "type": "string", "description": "Lifetime, e.g. 5m. Default: edgeBoot.defaultTicketTtl." },
                    "uses": { "type": "integer", "description": "Redemptions allowed. Default 1." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the ticket already minted." },
                    "scope": { "type": "string", "enum": ["boot", "session"], "description": "boot (default): start one instance. session: a reusable ticket a front-end presents to Worker.StartSession/StopSession/GetSession on any node of the pool to start sessions of a session-mode task with no manager call (docs/session-mode.md). Session tickets take no instance, prefix, gate, or job_config_id, and default to unlimited uses." }
                },
                "required": ["job", "task", "pool"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_boot_ticket_list",
            description: "List outstanding boot tickets (metadata only — token bytes are never stored or returned). Read-only.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Restrict to one job." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_node_claims",
            description: "List a node's live provisional (unratified) allocations — work it has started that the store has not recorded yet (docs/edge-boot.md). Use this to see started-but-unconfirmed capacity, and to tell 'still booting' from 'already confirmed'. Read-only.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node ID." }
                },
                "required": ["node_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_keyring_status",
            description: "Show KV encryption at rest on the answering manager (docs/kv-encryption.md): whether it is enabled, the root key provider and location, whether every data key is unwrapped (state unlocked|locked|pending_upgrade|uninitialized|disabled), the data keys with their value counts, how many rows are still plaintext, rewrap progress, and a one-line `hint` with the next step when something needs doing.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_keyring_rotate",
            description: "Rotate the KV data key now: a new active key is created, the old one is retired but kept, and every value is re-encrypted under the new key in the background (no job restarts). Mutating; retry-safe with idempotency_key.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "idempotency_key": { "type": "string", "description": "Optional key to deduplicate retries." }
                },
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_node_drain",
            description: "Cordon, drain, or uncordon a worker node. cordon = stop new placements (existing allocations keep running); drain = cordon + evict allocations (stateless move immediately, stateful stay protected by their stateful.onLost policy); uncordon = make the node schedulable again. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node ID." },
                    "mode": { "type": "string", "enum": ["cordon", "drain", "uncordon"], "description": "cordon: unschedulable, keep allocations. drain: unschedulable + evict. uncordon: schedulable again." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["node_id", "mode"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_acl_whoami",
            description: "Show whether ACLs are enabled and what access the current credentials grant. Use this to diagnose PermissionDenied errors.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_events",
            description: "List recent scheduler events (evals, placements, node/job lifecycle, rollouts). Poll with after_id set to the returned latestId to tail the stream incrementally.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "after_id": { "type": "integer", "description": "Only events with ID greater than this cursor." },
                    "limit": { "type": "integer", "description": "Max events to return (default 100, max 1000)." },
                    "kind": { "type": "string", "description": "Kind filter, exact or category prefix (e.g. 'eval' matches eval.*)." },
                    "job_id": { "type": "string", "description": "Filter by exact job ID." },
                    "node_id": { "type": "string", "description": "Filter by exact node ID." },
                    "level": { "type": "string", "enum": ["info", "warn", "error"], "description": "Filter by severity." },
                    "instance_id": { "type": "string", "description": "Only events about this stateful instance: suspends, resumes, forks, snapshots, and its allocations' placements and failures." },
                    "allocation_id": { "type": "integer", "description": "Filter by exact allocation ID." },
                    "namespaces": { "type": "array", "items": { "type": "string" }, "description": "Only events of these namespaces (names or `*`-globs); omit for every event this token can see, cluster events included." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_job_logs",
            description: "Fetch captured stdout/stderr for a job's allocations (or one allocation). Use this to diagnose task failures. Allocations removed in the last 24 hours (failed and rescheduled, stopped, the job destroyed) are still readable: a job with no live allocation returns its most recently removed ones, each marked `removed` with its `removalCause`.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name to gather allocation logs for." },
                    "allocation_id": { "type": "integer", "description": "Optional: read a single allocation id (live, or removed in the last 24 hours) instead of a whole job." },
                    "stream": { "type": "string", "enum": ["stdout", "stderr", "all"], "description": "Which stream to read (default all)." },
                    "max_bytes": { "type": "integer", "description": "Max bytes per stream (0 = worker default cap)." },
                    "tail": { "type": "boolean", "description": "Return trailing bytes instead of leading bytes." },
                    "include_removed": { "type": "boolean", "description": "Also return the job's recently removed allocations when it has live ones (default false)." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_service_list",
            description: "List declared services with their healthy/total endpoint counts. Use this to discover what a job can connect to.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_service_resolve",
            description: "Resolve a service to its endpoint addresses (address, port, allocation, node, health). Healthy endpoints only unless include_unhealthy is set.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "service": { "type": "string", "description": "Service name (service.name, defaulting to the task name)." },
                    "include_unhealthy": { "type": "boolean", "description": "Also return endpoints failing health checks (default false)." }
                },
                "required": ["service"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_service_edges",
            description: "The service graph: who calls whom through the datapath, with requests per second, error rate, denials, latency and bytes per (caller job, caller task, service, tag) over the window; declared-but-silent upstreams; and whether the service's allow list admits each caller. Read-only.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "service": { "type": "string", "description": "Only edges into or out of this service (optional)." },
                    "window_secs": { "type": "integer", "description": "Window in seconds, 60..900 (default 300)." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_net_policy_list",
            description: "List stored network policies (name, priority, enforcement, selector, rules) in merge order.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_net_policy_show",
            description: "Show a task's effective network policy: every matching stored policy merged with the task's own network.egress/ingress, each rule naming the policy it came from, plus the resolved network mode.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Job id." },
                    "task": { "type": "string", "description": "Task name (optional when the job has one task)." },
                    "node_pool": { "type": "string", "description": "Node pool to match selectors against (optional)." }
                },
                "required": ["job"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_net_policy_test",
            description: "Dry-run a network policy verdict for a task: would it be allowed to reach `to` (egress) or be reached from `from` (ingress)? Returns allow/deny/defer, the deciding rule, and its reason. Sends nothing. Use before retrying a connection that hangs.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Job id." },
                    "task": { "type": "string", "description": "Task name (optional when the job has one task)." },
                    "to": { "type": "string", "description": "Egress destination: host[:port], IP, or URL (a URL's path is tested when the chain is HTTP)." },
                    "from": { "type": "string", "description": "Ingress peer: host[:port] or IP." },
                    "identity": { "type": "string", "description": "Peer workload identity for ingress tests (optional)." },
                    "method": { "type": "string", "description": "HTTP method for request-time rules (optional)." },
                    "path": { "type": "string", "description": "HTTP path for request-time rules (optional)." },
                    "node_pool": { "type": "string", "description": "Node pool to match selectors against (optional)." }
                },
                "required": ["job"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_net_doctor",
            description: "Walk a task's network from policy to datapath and say where it breaks: the resolved policy, whether the mode enforces it (gated) or relies on cooperation (host), and per running allocation whether the node's datapath is ready (namespace, listeners, resolver, proxy), enforcing the current policy hash, or fail-closed (policy pending, boot gate shut). Returns a verdict (ok|warn|fail) and the steps with fix hints. Read-only.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Job id." },
                    "task": { "type": "string", "description": "Task name (optional when the job has one task)." },
                    "node_pool": { "type": "string", "description": "Node pool to match selectors against (optional)." }
                },
                "required": ["job"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_net_handler_test",
            description: "Conformance-probe an external network handler the way the datapath would use it: an authz webhook URL (http[s]://...) gets a sample facts payload and must answer {verdict: allow|deny}; a proxy at host:port must accept a PROXY protocol v2 header. Sends one synthetic request to the handler; never touches the cluster. Use before wiring a handler into a policy.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": "Webhook URL or proxy host:port." },
                    "timeout": { "type": "string", "description": "Per-probe timeout (default 2s)." }
                },
                "required": ["target"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_net_ca_show",
            description: "The cluster CA as the answering manager sees it (PEM, never a key): the provider (builtin, file, or exec) and where it comes from, the trust bundle (both CAs during a built-in rotation), the chain leaves carry, the pin join tokens carry, the rotation phase, and `problem` when this manager will not sign (an unusable provider, or a CA that disagrees with the cluster's). What a non-Legion client trusts to reach a mesh-TLS port or a worker plane. Empty until the first join token or node certificate.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_net_ca_rotate",
            description: "Rotate the built-in cluster CA in two steps: the new CA (cross-signed by the old one) signs at once, the old one stays trusted for 24 hours, and nodes re-issue within one heartbeat. No connection breaks. Fails with FailedPrecondition, naming the procedure, when the CA comes from a `file` or `exec` provider, which rotate outside Legion. Mutating.",
            input_schema: empty_schema,
            mutates: true,
        },
        ToolDef {
            name: "legion_net_policy_apply",
            description: "Create or replace a network policy object from its JSON (a NetPolicyObject: name, selector, enforcement, egress/ingress). Takes effect on matching running allocations within one heartbeat; no task restarts. Set dry_run to validate and list affected allocations without writing.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "object": { "type": "object", "description": "The policy object (see docs/network-handlers.md)." },
                    "dry_run": { "type": "boolean", "description": "Validate and report affected allocations only (default false)." }
                },
                "required": ["object"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_state_query",
            description: "Run a read-only SQL query against the manager state store. Use legion_state_schema first to learn the tables.",
            input_schema: || json!({
                "type": "object",
                "properties": { "sql": { "type": "string", "description": "Read-only SQL statement." } },
                "required": ["sql"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_state_schema",
            description: "Describe the manager state store's tables and columns.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_template_list",
            description: "List cluster-resident job templates (name, latest version, variables).",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Optional name/description substring filter." }
                },
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_template_get",
            description: "Show one job template version: declared variables, source, and version history.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Template name." },
                    "version": { "type": "integer", "description": "Version to fetch (default: latest)." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_template_render",
            description: "Dry-run a job template: validate variables and return the rendered jobspec without submitting.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Template name." },
                    "version": { "type": "integer", "description": "Version to render (default: latest)." },
                    "variables": { "type": "object", "description": "Variable values keyed by variable key." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_job_run",
            description: "Submit or update a job from a rendered jobspec (JSON). Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "spec_json": { "type": "string", "description": "Rendered jobspec as a JSON string." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." },
                    "force_reschedule": { "type": "boolean", "description": "Reschedule even if unchanged." }
                },
                "required": ["spec_json"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_job_stop",
            description: "Stop a running job by name. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_job_scale",
            description: "Change a task's count without a new job version or rollout (count override; docs/autoscaling.md). Pass count for a target, or reset=true to return ownership to the spec. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name." },
                    "count": { "type": "integer", "minimum": 0, "description": "Desired replica count. Omit with reset=true." },
                    "reset": { "type": "boolean", "description": "Drop the override; the spec owns the count again." },
                    "group": { "type": "string", "description": "Task group, when the job has more than one scalable task." },
                    "task": { "type": "string", "description": "Task name, when the job has more than one scalable task." },
                    "reason": { "type": "string", "description": "Audit note recorded with the override." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_job_scale_status",
            description: "Per-task spec vs override vs effective counts for a job (read-only).",
            input_schema: name_arg_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_job_destroy",
            description: "Destroy a job and its allocations by name. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Job name." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_template_run",
            description: "Render a job template with variables and submit the resulting job. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Template name." },
                    "version": { "type": "integer", "description": "Version to render (default: latest)." },
                    "variables": { "type": "object", "description": "Variable values keyed by variable key." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." },
                    "force_reschedule": { "type": "boolean", "description": "Reschedule even if unchanged." }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_template_upsert",
            description: "Store Nickel source as the next immutable version of a job template. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Template name (lowercase, digits, dashes)." },
                    "source_nickel": { "type": "string", "description": "Self-contained template source with a `job | not_exported = fun vars => ...` function." }
                },
                "required": ["name", "source_nickel"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_template_delete",
            description: "Delete a job template and all of its versions. Mutating.",
            input_schema: name_arg_schema,
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_create",
            description: "Create keyed-mode stateful instance(s) for a job's keyed task (never-recycled ids). A single instance_id (or neither instance_ids nor count) uses CreateInstance; instance_ids and/or count use the atomic all-or-nothing batch CreateInstances. On a session task (stateful.mode = \"session\"), omitting instance_id starts an unnamed session on a node with envelope room and no raft write: the reply has mode=session, durable=false, and nodeId; pass idempotency_key so a retry finds the same session, and call legion_instance_promote to keep it. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "Job name." },
                    "group": { "type": "string", "description": "Task group, when the keyed task is ambiguous." },
                    "task": { "type": "string", "description": "Task name, when the keyed task is ambiguous." },
                    "instance_id": { "type": "string", "description": "Explicit instance key ([A-Za-z0-9._-]{1,128}); server-minted when omitted." },
                    "instance_ids": { "type": "array", "items": { "type": "string" }, "description": "Batch of explicit instance keys, created atomically (all-or-nothing). Mutually exclusive with instance_id." },
                    "count": { "type": "integer", "minimum": 1, "description": "Create this many additional server-minted instances (atomic with instance_ids)." },
                    "from_snapshot": { "type": "string", "description": "Restore the new instance(s) from this snapshot of the job (an id or a name) instead of booting empty. Each gets its own copy of the snapshot's volume and its own identity, on the snapshot's node; at most 64 per call. Needs snapshot:read." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_session_nodes",
            description: "List the nodes that hold an envelope for a session task (stateful.mode = \"session\"): each node's worker address, its node tag (the 8 characters a session id minted there carries before its final '.'), whether it accepts edge starts and why not (lost, down, draining, ineligible, no-address, no-edge-sessions, full), the room left, a weight in sessions of the task's size, and its session count. Use it to build a picker's ring (accepting nodes) and its tag-to-address map (all nodes) for starting sessions at the edge with a session ticket. Answered from the manager's own state; advisory. Read-only.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string", "description": "The job." },
                    "task": { "type": "string", "description": "The session task. Optional when the job has one." }
                },
                "required": ["job_id"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_instance_promote",
            description: "Promote a running session (mode=session, durable=false) to a keyed instance in place: one raft entry writes its row and an allocation the node adopts it as, so nothing restarts. From then on it is listed exactly, rescheduled on node loss, and can be suspended or forked. new_id names it (refused when the task sets stateful.promote.onName = false); omit it to keep the session id. A repeat reports the same instance with deduplicated=true. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Id of the running session to promote." },
                    "new_id": { "type": "string", "description": "Name for the promoted instance ([A-Za-z0-9._-]{1,128}, never used before). Keeps the session id when omitted." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_stop",
            description: "Stop (pause) a keyed-mode instance: stop its allocation, retain the row and storage handle, keep the id owned. Reversible via legion_instance_start. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to stop." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_start",
            description: "Start a stopped keyed-mode instance (re-place it with the same id and retained storage handle), or resume a suspended one from its checkpoint (cold=true discards the checkpoint and boots fresh). The owning job must be running. Returns at once; poll legion_instance_get for state=active and no pendingOp, then check `coldBoot`: when set, the restore failed and the instance booted fresh (onRestoreFailure = \"cold\", the default), losing its running processes. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to start." },
                    "cold": { "type": "boolean", "description": "Suspended instances only: discard the checkpoint and boot fresh. Default false." },
                    "force_move": { "type": "boolean", "description": "Sessions with suspend.durability = \"store\" only: move the session to another node even when its old node did not answer the probe. Use only once that node is known to be down. Default false." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_suspend",
            description: "Suspend a keyed-mode instance: the node checkpoints the running allocation (memory for a sandbox launched for checkpoints, else disk), stops it, and frees its reservation. The instance becomes `suspended`; legion_instance_start resumes it. A failed capture leaves it running (see detail on legion_instance_get). Returns at once; poll legion_instance_get for state=suspended. Mutating; a memory checkpoint also needs snapshot:write.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to suspend." },
                    "checkpoint": { "type": "string", "enum": ["memory", "disk"], "description": "Checkpoint kind; default the task's suspend.checkpoint, else the best the instance can take." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_fork",
            description: "Fork a running keyed-mode instance into `count` new instances (docs/suspend-and-resume.md, \"Fork\"). The origin is checkpointed live and keeps running (a memory checkpoint pauses it for `pauseMs`). Each fork is a new instance `<origin>-f<n>` with its own cloned volume and its own identity (allocation id, hostname, token, entropy, MAC/TAP), pinned to the origin's node, or with spread=true each on its own node once the checkpoint is published. Returns the fork ids at once; poll legion_snapshot_get on `snapshot` for state=local (forks released for placement; published for a spread fork) or failed (origin keeps running, forks terminated). Retry-safe with idempotency_key. Mutating; needs job:write and snapshot:write.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Active keyed instance to fork." },
                    "count": { "type": "integer", "minimum": 1, "maximum": 64, "description": "How many forks (default 1)." },
                    "checkpoint": { "type": "string", "enum": ["memory", "disk"], "description": "Default memory when the origin can take one, else disk." },
                    "spread": { "type": "boolean", "description": "Place each fork on its own node. Takes a memory checkpoint, publishes it to the origin node's contentStore, and places the forks once it is published. Needs an instance with no stateful.storage volume." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the first call's forks and creates nothing." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_destroy",
            description: "Destroy a keyed-mode instance: stop its allocation, drop its storage, tombstone the id forever. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id to destroy." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_instance_replace",
            description: "Operator override for the node-loss confirmation gate: force-replace a stateful instance's allocation stuck on a LOST node without waiting for stateful.onLost.replaceAfter (K8s analog: force-delete a pod). DANGER: split-brain risk if the node is only partitioned and has not self-fenced yet. Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "instance_id": { "type": "string", "description": "Instance id whose lost-node allocation to force-replace." },
                    "idempotency_key": { "type": "string", "description": "Optional key to dedupe retries." }
                },
                "required": ["instance_id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_action_list",
            description: "List the declared actions (pre-baked, parameterized commands) on a job's tasks, including each action's parameters.",
            input_schema: || json!({
                "type": "object",
                "properties": { "job": { "type": "string", "description": "Job name." } },
                "required": ["job"],
                "additionalProperties": false
            }),
            mutates: false,
        },
        ToolDef {
            name: "legion_action_run",
            description: "Run a declared action against a running allocation and return `exitCode`, `stdout`, `stderr`, and `message`. One-shot (no interactive tty). Mutating.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "job": { "type": "string", "description": "Job name (used to pick the newest running allocation when allocation_id is omitted)." },
                    "action": { "type": "string", "description": "Action name." },
                    "allocation_id": { "type": "integer", "description": "Target allocation id; defaults to the newest running allocation of the job." },
                    "instance_id": { "type": "string", "description": "Target an instance instead: the manager resolves it to its current allocation, across reschedules and resumes." },
                    "params": { "type": "object", "description": "Action parameters as a string->string map.", "additionalProperties": { "type": "string" } }
                },
                "required": ["job", "action"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_exec_run",
            description: "Run a one-shot command inside a running allocation and return `exitCode`, `stdout`, `stderr`, and `message`. No interactive tty. Mutating; requires exec:write.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "allocation_id": { "type": "integer", "description": "Target allocation id. Use this or instance_id." },
                    "instance_id": { "type": "string", "description": "Target an instance: the manager resolves it to its current allocation, across reschedules and resumes." },
                    "command": { "type": "array", "items": { "type": "string" }, "description": "Command argv; first element is the program." },
                    "task": { "type": "string", "description": "Task name, for multi-task allocations." }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_exec_ticket_mint",
            description: "Mint an exec ticket for direct exec (docs/exec-and-actions.md, \"Direct exec\"): a short-lived, single-use credential that lets a client open ONE session on the worker running the target, with no manager on the data path. Authorized exactly like legion_exec_run (exec:write on the job, host for host-level runtimes) or legion_action_run (action:write) for an action ticket. Returns the ticket, the worker address, the node name its certificate carries, the cluster CA to verify it with, and when the ticket stops being redeemable. The ticket opens a shell, so treat it as a secret. Mutating (it grants a session); writes nothing to the cluster.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "allocation_id": { "type": "integer", "description": "Target allocation id. Use this or instance_id." },
                    "instance_id": { "type": "string", "description": "Target instance, resolved to its current allocation." },
                    "task": { "type": "string", "description": "Task name, for multi-task allocations." },
                    "command": { "type": "array", "items": { "type": "string" }, "description": "The argv the ticket allows. Omit (with no action) to allow any command." },
                    "action": { "type": "string", "description": "Mint for a declared action instead; the manager renders its argv." },
                    "params": { "type": "object", "description": "Action parameters as a string->string map.", "additionalProperties": { "type": "string" } },
                    "tty": { "type": "boolean", "description": "Whether the session is interactive." },
                    "ttl": { "type": "string", "description": "Redeem window, e.g. 30s. Default 60s, at most exec.tickets.maxTtl (10m)." },
                    "max_session": { "type": "string", "description": "How long the session may run, e.g. 15m. Default and ceiling exec.tickets.maxSession (1h)." }
                },
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_exec_key_list",
            description: "List delegated exec minting keys (docs/exec-and-actions.md, \"Delegated minting keys\"): each key's id, job glob, and ticket ceilings. Public keys only. Read-only; needs acl:read.",
            input_schema: empty_schema,
            mutates: false,
        },
        ToolDef {
            name: "legion_exec_key_create",
            description: "Register a platform's P-256 public key so workers accept exec tickets it signs for jobs matching `jobs`, with no manager call per session. Its tickets never reach a host-level runtime (`exec`), and are bounded by `max_ttl` and `max_session`. A key is a shell capability for every matching job until deleted. Mutating; needs acl:write. Supports `idempotency_key`.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "jobs": { "type": "string", "description": "Glob over job names, e.g. agents-*." },
                    "public_key_pem": { "type": "string", "description": "P-256 public key, SPKI PEM." },
                    "name": { "type": "string", "description": "A label for people." },
                    "max_ttl": { "type": "string", "description": "Ceiling on a ticket's redeem window. Default exec.tickets.maxTtl." },
                    "max_session": { "type": "string", "description": "Ceiling on a ticket's session length. Default exec.tickets.maxSession." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key; a repeat returns the key already created." }
                },
                "required": ["jobs", "public_key_pem"],
                "additionalProperties": false
            }),
            mutates: true,
        },
        ToolDef {
            name: "legion_exec_key_delete",
            description: "Delete a delegated exec minting key. Workers stop accepting its tickets, and end the sessions it minted, on their next heartbeat. Mutating; needs acl:write. Supports `idempotency_key`.",
            input_schema: || json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Key id (xk_...)." },
                    "idempotency_key": { "type": "string", "description": "Retry-safe key." }
                },
                "required": ["id"],
                "additionalProperties": false
            }),
            mutates: true,
        },
    ]
}

/// Tools available given the write policy.
fn available_tools(allow_write: bool) -> Vec<ToolDef> {
    tool_defs()
        .into_iter()
        .filter(|tool| allow_write || !tool.mutates)
        .collect()
}

/// Run the stdio MCP server loop until stdin closes.
async fn serve(allow_write: bool) -> R<()> {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    info!(allow_write, "legion mcp server started");

    while let Some(line) = reader.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let response = handle_message(line, allow_write).await;
        if let Some(response) = response {
            let mut text = serde_json::to_string(&response)?;
            text.push('\n');
            stdout.write_all(text.as_bytes()).await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

/// Parse and dispatch one JSON-RPC message. Returns `None` for notifications
/// (no `id`), which must not be answered.
async fn handle_message(line: &str, allow_write: bool) -> Option<Value> {
    let parsed: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(err) => {
            return Some(json_rpc_error(
                Value::Null,
                -32700,
                &format!("parse error: {err}"),
            ));
        }
    };

    let id = parsed.get("id").cloned();
    let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");
    let params = parsed.get("params").cloned().unwrap_or(Value::Null);

    // Notifications carry no id and expect no response.
    let is_notification = id.is_none();

    match method {
        "initialize" => Some(json_rpc_result(id, initialize_result())),
        "tools/list" => Some(json_rpc_result(id, tools_list_result(allow_write))),
        "tools/call" => Some(json_rpc_result(
            id,
            tools_call_result(params, allow_write).await,
        )),
        "ping" => Some(json_rpc_result(id, json!({}))),
        _ if is_notification => None,
        _ => Some(json_rpc_error(
            id.unwrap_or(Value::Null),
            -32601,
            &format!("method not found: {method}"),
        )),
    }
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "legion", "version": env!("CARGO_PKG_VERSION") }
    })
}

fn tools_list_result(allow_write: bool) -> Value {
    let tools = available_tools(allow_write)
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool_schema(&tool),
            })
        })
        .collect::<Vec<_>>();
    json!({ "tools": tools })
}

/// Execute a `tools/call`. Tool-level failures are reported inside the result
/// with `isError: true` (per MCP convention), not as JSON-RPC errors.
async fn tools_call_result(params: Value, allow_write: bool) -> Value {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    // Enforce the write policy regardless of what tools/list advertised.
    if !allow_write && tool_defs().iter().any(|t| t.name == name && t.mutates) {
        return tool_error(&format!(
            "tool {name} is mutating and requires the server to be started with --allow-write"
        ));
    }

    match dispatch_tool(name, arguments).await {
        Ok(value) => tool_success(&value),
        Err(err) => tool_error(&err),
    }
}

/// Run a tool and return its data payload as JSON, or a human-readable error
/// string on failure.
async fn dispatch_tool(name: &str, args: Value) -> Result<Value, String> {
    match name {
        "legion_capabilities" => {
            Ok(serde_json::to_value(crate::cmd::capabilities::catalog())
                .map_err(|e| e.to_string())?)
        }
        "legion_job_schema" => Ok(crate::cmd::job::build_jobspec_schema()),
        "legion_job_validate" => validate_tool(args),
        "legion_job_plan" => {
            let parsed_jobspec = str_arg(&args, "spec_json")?;
            let mut client = connect().await?;
            grpc(client.plan_job(PlanJobRequest {
                parsed_jobspec,
                namespace: opt_str_arg(&args, "namespace"),
            }))
            .await
        }
        "legion_job_list" => {
            let status = opt_str_arg(&args, "status");
            let namespaces = args
                .get("namespaces")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let mut client = connect().await?;
            grpc(client.list_jobs(ListJobsRequest {
                status,
                namespaces,
                ..Default::default()
            }))
            .await
        }
        "legion_namespace_list" => {
            let mut client = connect().await?;
            grpc(client.list_namespaces(crate::structs::ListNamespacesRequest {})).await
        }
        "legion_namespace_get" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_namespace(crate::structs::GetNamespaceRequest { name })).await
        }
        "legion_namespace_apply" => {
            let name = str_arg(&args, "name")?;
            let meta = args
                .get("meta")
                .and_then(Value::as_object)
                .map(|meta| {
                    meta.iter()
                        .filter_map(|(key, value)| {
                            value.as_str().map(|value| (key.clone(), value.to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let mut client = connect().await?;
            grpc(
                client.apply_namespace(crate::structs::ApplyNamespaceRequest {
                    namespace: Some(crate::structs::Namespace {
                        name,
                        description: opt_str_arg(&args, "description").unwrap_or_default(),
                        meta,
                        ..Default::default()
                    }),
                    dry_run: args
                        .get("dry_run")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                }),
            )
            .await
        }
        "legion_namespace_delete" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(
                client.delete_namespace(crate::structs::DeleteNamespaceRequest {
                    name,
                    cascade: args
                        .get("cascade")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    dry_run: args
                        .get("dry_run")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                }),
            )
            .await
        }
        "legion_job_info" => {
            let name = job_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_job_info(GetJobInfoRequest { name })).await
        }
        "legion_job_spec" => {
            let name = job_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_job_spec(GetJobSpecRequest { name })).await
        }
        "legion_rollout_list" => {
            let job_id = opt_job_arg(&args, "job_id")?;
            let active_only = args
                .get("active_only")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.list_rollouts(ListRolloutsRequest {
                job_id,
                active_only,
                ..Default::default()
            }))
            .await
        }
        "legion_rollout_status" => {
            let id = args
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| "missing integer argument: id".to_string())?;
            let mut client = connect().await?;
            grpc(client.get_rollout_status(GetRolloutStatusRequest { id })).await
        }
        "legion_node_list" => {
            let mut client = connect().await?;
            grpc(client.get_nodes(GetNodesRequest::default()))
                .await
                .map(decode_node_attrs)
        }
        "legion_node_cadence" => {
            let mut client = connect().await?;
            grpc(client.get_heartbeat_cadence(crate::structs::GetHeartbeatCadenceRequest {})).await
        }
        "legion_node_usage" => {
            let node_id = args
                .get("node_id")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing string argument: node_id".to_string())?
                .to_string();
            let mut client = connect().await?;
            grpc(client.get_node_usage(crate::structs::GetNodeUsageRequest { node_id })).await
        }
        "legion_node_usage_list" => {
            let node_ids = args
                .get("node_ids")
                .and_then(Value::as_array)
                .map(|ids| {
                    ids.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let include_allocations = args
                .get("include_allocations")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(
                client.list_node_usage(crate::structs::ListNodeUsageRequest {
                    node_ids,
                    include_allocations,
                }),
            )
            .await
        }
        "legion_node_content" => {
            let node_id = args
                .get("node_id")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing string argument: node_id".to_string())?
                .to_string();
            let mut client = connect().await?;
            grpc(client.get_node_content(crate::structs::GetNodeContentRequest { node_id })).await
        }
        "legion_keyring_status" => {
            let mut client = connect().await?;
            grpc(client.get_keyring_status(crate::structs::GetKeyringStatusRequest {})).await
        }
        "legion_keyring_rotate" => {
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.rotate_data_key(crate::structs::RotateDataKeyRequest { idempotency_key }))
                .await
        }
        "legion_manager_list" => {
            let mut client = connect().await?;
            grpc(client.list_managers(crate::structs::ListManagersRequest {})).await
        }
        "legion_scheduler_concurrency" => {
            let mut client = connect().await?;
            grpc(
                client.get_scheduler_concurrency(crate::structs::GetSchedulerConcurrencyRequest {}),
            )
            .await
        }
        "legion_node_drain" => {
            let node_id = str_arg(&args, "node_id")?;
            let mode = str_arg(&args, "mode")?;
            let (availability, evict) = match mode.as_str() {
                "cordon" => ("drained", false),
                "drain" => ("drained", true),
                "uncordon" => ("available", false),
                other => {
                    return Err(format!(
                        "invalid mode {other:?}: expected cordon, drain, or uncordon"
                    ))
                }
            };
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.set_node_availability(SetNodeAvailabilityRequest {
                node_id,
                availability: availability.to_string(),
                evict,
                idempotency_key,
            }))
            .await
            .map(decode_node_attrs)
        }
        "legion_boot_ticket_create" => {
            let scope = opt_str_arg(&args, "scope").unwrap_or_else(|| "boot".to_string());
            let session = scope == "session";
            let gate = match opt_str_arg(&args, "gate") {
                Some(raw) => crate::structs::edge_boot::Gate::parse(&raw)
                    .map_err(|err| format!("{err:#}"))?,
                None if session => crate::structs::edge_boot::Gate::Off,
                None => crate::structs::edge_boot::Gate::Boot,
            };
            let uses = args
                .get("uses")
                .and_then(|value| value.as_u64())
                .unwrap_or(if session { 0 } else { 1 }) as u32;
            let mut client = connect().await?;
            grpc(
                client.issue_boot_ticket(crate::structs::IssueBootTicketRequest {
                    job: job_arg(&args, "job")?,
                    task: str_arg(&args, "task")?,
                    pool: str_arg(&args, "pool")?,
                    instance_id: opt_str_arg(&args, "instance_id").unwrap_or_default(),
                    instance_prefix: opt_str_arg(&args, "instance_prefix").unwrap_or_default(),
                    job_config_id: opt_str_arg(&args, "job_config_id").unwrap_or_default(),
                    gate: gate.to_wire(),
                    ttl: opt_str_arg(&args, "ttl").unwrap_or_default(),
                    uses,
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                    scope,
                }),
            )
            .await
        }
        "legion_boot_ticket_list" => {
            let mut client = connect().await?;
            grpc(
                client.list_boot_tickets(crate::structs::ListBootTicketsRequest {
                    job: opt_job_arg(&args, "job")?.unwrap_or_default(),
                }),
            )
            .await
        }
        "legion_node_claims" => {
            let id = str_arg(&args, "node_id")?;
            let mut client = connect().await?;
            grpc(client.get_node(crate::structs::GetNodeRequest { id }))
                .await
                .map(decode_node_attrs)
        }
        "legion_acl_whoami" => {
            let mut client = connect().await?;
            grpc(client.who_am_i(crate::structs::WhoAmIRequest {})).await
        }
        "legion_action_list" => {
            let job = job_arg(&args, "job")?;
            let mut client = connect().await?;
            grpc(client.list_job_actions(ListJobActionsRequest { job })).await
        }
        "legion_action_run" => {
            let job = job_arg(&args, "job")?;
            let action = str_arg(&args, "action")?;
            let params: std::collections::HashMap<String, String> = args
                .get("params")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let mut client = connect().await?;
            let instance_id = opt_str_arg(&args, "instance_id").unwrap_or_default();
            let allocation_id = match args.get("allocation_id").and_then(Value::as_i64) {
                Some(id) => id,
                None if !instance_id.is_empty() => 0,
                None => newest_alloc_for_job(&mut client, &job).await?,
            };
            let start = ExecStart {
                allocation_id,
                task_name: String::new(),
                command: Vec::new(),
                action,
                action_params: params,
                tty: false,
                winsize: None,
                bearer_token: String::new(),
                instance_id,
                exec_ticket: String::new(),
            };
            run_exec_oneshot(&mut client, start).await
        }
        "legion_exec_ticket_mint" => {
            let params: std::collections::HashMap<String, String> = args
                .get("params")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let command: Vec<String> = args
                .get("command")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let instance_id = opt_str_arg(&args, "instance_id").unwrap_or_default();
            let allocation_id = args
                .get("allocation_id")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            if allocation_id == 0 && instance_id.is_empty() {
                return Err("name the target: allocation_id or instance_id".to_string());
            }
            let mut client = connect().await?;
            grpc(
                client.mint_exec_ticket(crate::structs::MintExecTicketRequest {
                    allocation_id,
                    instance_id,
                    task_name: opt_str_arg(&args, "task").unwrap_or_default(),
                    command,
                    action: opt_str_arg(&args, "action").unwrap_or_default(),
                    action_params: params,
                    tty: args.get("tty").and_then(Value::as_bool).unwrap_or(false),
                    ttl: opt_str_arg(&args, "ttl").unwrap_or_default(),
                    max_session: opt_str_arg(&args, "max_session").unwrap_or_default(),
                }),
            )
            .await
        }
        "legion_exec_key_list" => {
            let mut client = connect().await?;
            grpc(client.list_exec_keys(crate::structs::ListExecKeysRequest {})).await
        }
        "legion_exec_key_create" => {
            let mut client = connect().await?;
            grpc(
                client.create_exec_key(crate::structs::CreateExecKeyRequest {
                    jobs: str_arg(&args, "jobs")?,
                    public_key_pem: str_arg(&args, "public_key_pem")?,
                    name: opt_str_arg(&args, "name").unwrap_or_default(),
                    max_ttl: opt_str_arg(&args, "max_ttl").unwrap_or_default(),
                    max_session: opt_str_arg(&args, "max_session").unwrap_or_default(),
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                }),
            )
            .await
        }
        "legion_exec_key_delete" => {
            let mut client = connect().await?;
            grpc(
                client.delete_exec_key(crate::structs::DeleteExecKeyRequest {
                    id: str_arg(&args, "id")?,
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                }),
            )
            .await
        }
        "legion_exec_run" => {
            let instance_id = opt_str_arg(&args, "instance_id").unwrap_or_default();
            let allocation_id = match args.get("allocation_id").and_then(Value::as_i64) {
                Some(id) => id,
                None if !instance_id.is_empty() => 0,
                None => return Err("missing argument: allocation_id or instance_id".to_string()),
            };
            let command: Vec<String> = args
                .get("command")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            if command.is_empty() {
                return Err("command must be a non-empty array".to_string());
            }
            let start = ExecStart {
                allocation_id,
                task_name: opt_str_arg(&args, "task").unwrap_or_default(),
                command,
                action: String::new(),
                action_params: std::collections::HashMap::new(),
                tty: false,
                winsize: None,
                bearer_token: String::new(),
                instance_id,
                exec_ticket: String::new(),
            };
            let mut client = connect().await?;
            run_exec_oneshot(&mut client, start).await
        }
        "legion_instance_list" => {
            let job_id = job_arg(&args, "job_id")?;
            let group = opt_str_arg(&args, "group");
            let include_idle = args
                .get("include_idle")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let idle_page_token = opt_str_arg(&args, "idle_page_token").unwrap_or_default();
            let idle_page_size = args
                .get("idle_page_size")
                .and_then(Value::as_u64)
                .map(|size| size.min(u64::from(u32::MAX)) as u32)
                .unwrap_or(0);
            let session_limit = args
                .get("session_limit")
                .and_then(Value::as_u64)
                .map(|limit| limit.min(u64::from(u32::MAX)) as u32);
            let session_page_token = opt_str_arg(&args, "session_page_token").unwrap_or_default();
            let mut client = connect().await?;
            grpc(client.list_instances(ListInstancesRequest {
                job_id,
                group,
                include_idle,
                idle_page_token,
                idle_page_size,
                session_limit,
                session_page_token,
            }))
            .await
        }
        "legion_instance_get" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let mut client = connect().await?;
            grpc(client.get_instance(GetInstanceRequest { instance_id })).await
        }
        "legion_volume_list" => {
            let mut client = connect().await?;
            grpc(
                client.list_volumes(ListVolumesRequest {
                    job_id: opt_job_arg(&args, "job_id")?,
                    node_id: opt_str_arg(&args, "node_id"),
                    driver: opt_str_arg(&args, "driver"),
                    instance_id: opt_str_arg(&args, "instance_id"),
                    state: opt_str_arg(&args, "state"),
                    include_destroyed: args
                        .get("include_destroyed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                }),
            )
            .await
        }
        "legion_volume_get" => {
            let volume_id = str_arg(&args, "volume_id")?;
            let mut client = connect().await?;
            grpc(client.get_volume(GetVolumeRequest { volume_id })).await
        }
        "legion_volume_drivers" => {
            let mut client = connect().await?;
            grpc(client.list_volume_drivers(ListVolumeDriversRequest {})).await
        }
        "legion_volume_delete" => {
            let volume_id = str_arg(&args, "volume_id")?;
            let force = args.get("force").and_then(Value::as_bool).unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.delete_volume(DeleteVolumeRequest {
                volume_id,
                force,
                idempotency_key: opt_str_arg(&args, "idempotency_key"),
            }))
            .await
        }
        "legion_snapshot_list" => {
            let mut client = connect().await?;
            grpc(client.list_snapshots(crate::structs::ListSnapshotsRequest {
                job_id: opt_job_arg(&args, "job_id")?,
                instance_id: opt_str_arg(&args, "instance_id"),
                state: opt_str_arg(&args, "state"),
            }))
            .await
        }
        "legion_snapshot_get" => {
            let snapshot = str_arg(&args, "snapshot")?;
            let mut client = connect().await?;
            grpc(client.get_snapshot(crate::structs::GetSnapshotRequest {
                snapshot,
                job_id: opt_job_arg(&args, "job_id")?,
            }))
            .await
        }
        "legion_snapshot_create" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let mut client = connect().await?;
            grpc(
                client.create_snapshot(crate::structs::CreateSnapshotRequest {
                    instance_id,
                    kind: opt_str_arg(&args, "kind").unwrap_or_else(|| "disk".into()),
                    name: opt_str_arg(&args, "name"),
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                    publish: args
                        .get("publish")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                }),
            )
            .await
        }
        "legion_snapshot_warm" => {
            let snapshot = str_arg(&args, "snapshot")?;
            let constraints: Vec<String> = args
                .get("constraints")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let mut client = connect().await?;
            grpc(client.warm_snapshot(crate::structs::WarmSnapshotRequest {
                snapshot,
                job_id: opt_job_arg(&args, "job_id")?,
                constraints,
                idempotency_key: opt_str_arg(&args, "idempotency_key"),
            }))
            .await
        }
        "legion_snapshot_delete" => {
            let snapshot_id = str_arg(&args, "snapshot_id")?;
            let mut client = connect().await?;
            grpc(
                client.delete_snapshot(crate::structs::DeleteSnapshotRequest {
                    snapshot_id,
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                }),
            )
            .await
        }
        "legion_snapshot_restore" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let snapshot = str_arg(&args, "snapshot")?;
            let mut client = connect().await?;
            grpc(
                client.restore_instance(crate::structs::RestoreInstanceRequest {
                    instance_id,
                    snapshot,
                    idempotency_key: opt_str_arg(&args, "idempotency_key"),
                }),
            )
            .await
        }
        "legion_instance_create" => {
            let job_id = job_arg(&args, "job_id")?;
            let group = opt_str_arg(&args, "group");
            let task = opt_str_arg(&args, "task");
            let instance_id = opt_str_arg(&args, "instance_id");
            let instance_ids: Vec<String> = args
                .get("instance_ids")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let count = args.get("count").and_then(Value::as_u64).unwrap_or(0) as u32;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let from_snapshot = opt_str_arg(&args, "from_snapshot");
            let mut client = connect().await?;
            // instance_ids/count select the atomic batch RPC; a lone
            // instance_id keeps the single-create semantics (incl. server
            // minting when absent).
            if !instance_ids.is_empty() || count > 0 {
                if instance_id.is_some() {
                    return Err(
                        "pass either instance_id (single create) or instance_ids/count \
                         (batch create), not both"
                            .into(),
                    );
                }
                grpc(client.create_instances(CreateInstancesRequest {
                    job_id,
                    group,
                    task,
                    instance_ids,
                    count,
                    idempotency_key,
                    from_snapshot,
                }))
                .await
            } else {
                grpc(client.create_instance(CreateInstanceRequest {
                    job_id,
                    group,
                    task,
                    instance_id,
                    idempotency_key,
                    from_snapshot,
                }))
                .await
            }
        }
        "legion_session_nodes" => {
            let job_id = job_arg(&args, "job_id")?;
            let task_name = opt_str_arg(&args, "task").unwrap_or_default();
            let mut client = connect().await?;
            grpc(
                client.list_session_nodes(crate::structs::ListSessionNodesRequest {
                    job_id,
                    task_name,
                }),
            )
            .await
        }
        "legion_instance_promote" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let new_id = opt_str_arg(&args, "new_id").unwrap_or_default();
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.promote_instance(PromoteInstanceRequest {
                instance_id,
                new_id,
                idempotency_key,
            }))
            .await
        }
        "legion_instance_stop" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.stop_instance(StopInstanceRequest {
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_instance_start" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let cold = args
                .get("cold")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let force_move = args
                .get("force_move")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.start_instance(StartInstanceRequest {
                instance_id,
                idempotency_key,
                cold,
                force_move,
            }))
            .await
        }
        "legion_instance_suspend" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let checkpoint = opt_str_arg(&args, "checkpoint");
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.suspend_instance(SuspendInstanceRequest {
                instance_id,
                checkpoint,
                idempotency_key,
            }))
            .await
        }
        "legion_instance_fork" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let count = args.get("count").and_then(Value::as_u64).unwrap_or(1) as u32;
            let spread = args.get("spread").and_then(Value::as_bool).unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.fork_instance(ForkInstanceRequest {
                instance_id,
                count,
                spread,
                checkpoint: opt_str_arg(&args, "checkpoint"),
                idempotency_key: opt_str_arg(&args, "idempotency_key"),
            }))
            .await
        }
        "legion_snapshot_alias_set" => {
            let job_id = job_arg(&args, "job_id")?;
            let name = str_arg(&args, "name")?;
            let snapshot = str_arg(&args, "snapshot")?;
            let mut client = connect().await?;
            grpc(client.set_snapshot_alias(SetSnapshotAliasRequest {
                job_id,
                name,
                snapshot,
                idempotency_key: opt_str_arg(&args, "idempotency_key"),
            }))
            .await
        }
        "legion_instance_destroy" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.destroy_instance(DestroyInstanceRequest {
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_instance_replace" => {
            let instance_id = str_arg(&args, "instance_id")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.replace_instance(ReplaceInstanceRequest {
                instance_id,
                idempotency_key,
            }))
            .await
        }
        "legion_events" => {
            let request = ListEventsRequest {
                after_id: args.get("after_id").and_then(Value::as_i64),
                limit: args
                    .get("limit")
                    .and_then(Value::as_u64)
                    .map(|limit| limit.min(u32::MAX as u64) as u32),
                kind: opt_str_arg(&args, "kind"),
                job_id: opt_job_arg(&args, "job_id")?,
                node_id: opt_str_arg(&args, "node_id"),
                level: opt_str_arg(&args, "level"),
                instance_id: opt_str_arg(&args, "instance_id"),
                allocation_id: args.get("allocation_id").and_then(Value::as_i64),
                namespaces: args
                    .get("namespaces")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            let mut client = connect().await?;
            grpc(client.list_events(request)).await
        }
        "legion_job_logs" => {
            let job = opt_job_arg(&args, "name")?.unwrap_or_default();
            let allocation_id = args.get("allocation_id").and_then(Value::as_i64);
            if job.is_empty() && allocation_id.is_none() {
                return Err("provide a job name or allocation_id".to_string());
            }
            let stream = opt_str_arg(&args, "stream").unwrap_or_else(|| "all".to_string());
            let max_bytes = args.get("max_bytes").and_then(Value::as_i64).unwrap_or(0);
            let tail = args.get("tail").and_then(Value::as_bool).unwrap_or(false);
            let include_removed = args
                .get("include_removed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.get_allocation_logs(GetAllocationLogsRequest {
                job,
                allocation_id,
                stream,
                max_bytes,
                tail,
                offset: 0,
                include_removed,
            }))
            .await
        }
        "legion_service_list" => {
            let mut client = connect().await?;
            let namespaces = args
                .get("namespaces")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_else(|| vec!["*".to_string()]);
            grpc(client.list_services(ListServicesRequest { namespaces })).await
        }
        "legion_service_edges" => {
            let service = args
                .get("service")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let window_secs = args
                .get("window_secs")
                .and_then(Value::as_u64)
                .unwrap_or(300) as u32;
            let mut client = connect().await?;
            grpc(client.list_service_edges(ListServiceEdgesRequest {
                service,
                window_secs,
            }))
            .await
        }
        "legion_net_policy_list" => {
            let mut client = connect().await?;
            grpc(client.list_net_policies(ListNetPoliciesRequest {})).await
        }
        "legion_net_policy_show" => {
            let job_id = job_arg(&args, "job")?;
            let task_name = args
                .get("task")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let node_pool = args
                .get("node_pool")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let mut client = connect().await?;
            grpc(client.show_net_policy(ShowNetPolicyRequest {
                job_id,
                task_name,
                node_pool,
            }))
            .await
        }
        "legion_net_policy_test" => {
            let job_id = job_arg(&args, "job")?;
            let get = |key: &str| {
                args.get(key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            let mut client = connect().await?;
            grpc(client.test_net_policy(TestNetPolicyRequest {
                job_id,
                task_name: get("task"),
                node_pool: get("node_pool"),
                to: get("to"),
                from: get("from"),
                identity: get("identity"),
                method: get("method"),
                path: get("path"),
            }))
            .await
        }
        "legion_net_doctor" => {
            let job_id = job_arg(&args, "job")?;
            let get = |key: &str| {
                args.get(key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            let task_name = get("task");
            let mut client = connect().await?;
            let reply = client
                .show_net_policy(ShowNetPolicyRequest {
                    job_id: job_id.clone(),
                    task_name: task_name.clone(),
                    node_pool: get("node_pool"),
                })
                .await
                .map_err(|status| format!("{}: {}", status.code(), status.message()))?
                .into_inner();
            let steps = crate::cmd::net::diagnose_network(&job_id, &task_name, &reply);
            Ok(json!({
                "job": job_id,
                "task": task_name,
                "verdict": crate::cmd::net::doctor_verdict(&steps),
                "steps": steps,
            }))
        }
        "legion_net_handler_test" => {
            let target = str_arg(&args, "target")?;
            let timeout = args
                .get("timeout")
                .and_then(Value::as_str)
                .and_then(|t| crate::structs::netpolicy::parse_duration(t).ok())
                .unwrap_or(std::time::Duration::from_secs(2));
            let probe = crate::cmd::net::probe_handler(&target, timeout).await;
            serde_json::to_value(probe).map_err(|e| e.to_string())
        }
        "legion_net_ca_show" => {
            let mut client = connect().await?;
            grpc(client.get_mesh_ca(GetMeshCaRequest {})).await
        }
        "legion_net_ca_rotate" => {
            let mut client = connect().await?;
            grpc(client.rotate_mesh_ca(RotateMeshCaRequest {})).await
        }
        "legion_net_policy_apply" => {
            let object = args
                .get("object")
                .cloned()
                .ok_or_else(|| "missing required argument: object".to_string())?;
            let dry_run = args
                .get("dry_run")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.upsert_net_policy(UpsertNetPolicyRequest {
                object_json: object.to_string(),
                dry_run,
            }))
            .await
        }
        "legion_service_resolve" => {
            let service = str_arg(&args, "service")?;
            let include_unhealthy = args
                .get("include_unhealthy")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.resolve_service(ResolveServiceRequest {
                service,
                include_unhealthy,
            }))
            .await
        }
        "legion_state_query" => {
            let sql = str_arg(&args, "sql")?;
            let mut client = connect().await?;
            // Always read-only over MCP.
            grpc(client.query_state_store(QueryStateStoreRequest {
                sql,
                allow_write: false,
            }))
            .await
        }
        "legion_state_schema" => {
            let mut client = connect().await?;
            grpc(client.get_state_store_schema(GetStateStoreSchemaRequest {})).await
        }
        "legion_template_list" => {
            let mut client = connect().await?;
            grpc(client.list_job_templates(ListJobTemplatesRequest {
                query: opt_str_arg(&args, "query"),
            }))
            .await
        }
        "legion_template_get" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_job_template(GetJobTemplateRequest {
                name,
                version: opt_u32_arg(&args, "version"),
            }))
            .await
        }
        "legion_template_render" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.render_job_template(RenderJobTemplateRequest {
                name,
                version: opt_u32_arg(&args, "version"),
                variables_json: variables_arg(&args),
                namespace: opt_str_arg(&args, "namespace"),
            }))
            .await
        }
        "legion_template_run" => {
            let name = str_arg(&args, "name")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let force_reschedule = args
                .get("force_reschedule")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.run_job_template(RunJobTemplateRequest {
                name,
                version: opt_u32_arg(&args, "version"),
                variables_json: variables_arg(&args),
                priority: None,
                force_reschedule,
                idempotency_key,
                namespace: opt_str_arg(&args, "namespace"),
            }))
            .await
        }
        "legion_template_upsert" => {
            let name = str_arg(&args, "name")?;
            let source_nickel = str_arg(&args, "source_nickel")?;
            let mut client = connect().await?;
            grpc(client.upsert_job_template(UpsertJobTemplateRequest {
                name,
                source_nickel,
            }))
            .await
        }
        "legion_template_delete" => {
            let name = str_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.delete_job_template(DeleteJobTemplateRequest { name })).await
        }
        "legion_job_run" => {
            let spec_json = str_arg(&args, "spec_json")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let force_reschedule = args
                .get("force_reschedule")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.run_job(RunJobRequest {
                parsed_jobspec: spec_json,
                submissions: vec![],
                priority: None,
                force_reschedule,
                idempotency_key,
                namespace: opt_str_arg(&args, "namespace"),
            }))
            .await
        }
        "legion_job_stop" => {
            let name = job_arg(&args, "name")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.stop_job(StopJobRequest {
                name,
                priority: None,
                idempotency_key,
            }))
            .await
        }
        "legion_job_scale" => {
            let name = job_arg(&args, "name")?;
            let count = args
                .get("count")
                .and_then(|value| value.as_u64())
                .map(|count| count as u32);
            let reset = args
                .get("reset")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let mut client = connect().await?;
            grpc(client.scale_task(crate::structs::ScaleTaskRequest {
                name,
                task_group: opt_str_arg(&args, "group"),
                task_name: opt_str_arg(&args, "task"),
                count,
                reset,
                reason: opt_str_arg(&args, "reason"),
                idempotency_key: opt_str_arg(&args, "idempotency_key"),
            }))
            .await
        }
        "legion_job_scale_status" => {
            let name = job_arg(&args, "name")?;
            let mut client = connect().await?;
            grpc(client.get_job_scale_status(crate::structs::GetJobScaleStatusRequest { name }))
                .await
        }
        "legion_job_destroy" => {
            let name = job_arg(&args, "name")?;
            let idempotency_key = opt_str_arg(&args, "idempotency_key");
            let mut client = connect().await?;
            grpc(client.destroy_job(DestroyJobRequest {
                name,
                priority: None,
                idempotency_key,
            }))
            .await
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

/// Validate a rendered jobspec locally (no manager required).
fn validate_tool(args: Value) -> Result<Value, String> {
    let spec_json = str_arg(&args, "spec_json")?;
    let config: JobConfig = serde_json::from_str(&spec_json)
        .map_err(|e| format!("jobspec is not a valid job configuration: {e}"))?;
    validate_job_config(&config).map_err(|e| format!("jobspec failed validation: {e}"))?;
    Ok(json!({
        "valid": true,
        "job": config.name,
        "tasks": config.tasks.len(),
    }))
}

/// Connect to the manager, mapping connection failures to a tool error string.
async fn connect() -> Result<crate::cmd::helpers::AuthedManagerClient, String> {
    connect_manager_client().await.map_err(|e| e.to_string())
}

/// Pick the newest running allocation for a job (for exec/action tools that
/// default the target).
async fn newest_alloc_for_job(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job: &str,
) -> Result<i64, String> {
    let ledger = client
        .get_ledger(GetLedgerRequest {})
        .await
        .map_err(|s| s.message().to_string())?
        .into_inner();
    let mut best: Option<i64> = None;
    for node in ledger.nodes {
        for alloc in node.allocations {
            if alloc.job_id == job
                && matches!(alloc.status.as_str(), "placed" | "active" | "running")
            {
                best = Some(best.map_or(alloc.id, |b| b.max(alloc.id)));
            }
        }
    }
    best.ok_or_else(|| format!("no running allocation found for job `{job}`"))
}

/// Drive a one-shot exec/action session over `ExecAllocation`, collecting
/// stdout/stderr and the exit code. Interactive PTY is not supported here.
async fn run_exec_oneshot(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    start: ExecStart,
) -> Result<Value, String> {
    let (tx, rx) = tokio::sync::mpsc::channel::<ExecClientFrame>(8);
    tx.send(ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Start(start)),
    })
    .await
    .ok();
    // No stdin; signal EOF immediately.
    tx.send(ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Stdin(ExecInput {
            data: Vec::new(),
            eof: true,
        })),
    })
    .await
    .ok();
    drop(tx);

    let mut inbound = client
        .exec_allocation(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .map_err(|s| s.message().to_string())?
        .into_inner();

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_code = -1i32;
    let mut exit_message = String::new();
    while let Some(frame) = inbound
        .message()
        .await
        .map_err(|s| s.message().to_string())?
    {
        match frame.frame {
            Some(exec_server_frame::Frame::Stdout(o)) => stdout.extend_from_slice(&o.data),
            Some(exec_server_frame::Frame::Stderr(o)) => stderr.extend_from_slice(&o.data),
            Some(exec_server_frame::Frame::Exit(e)) => {
                exit_code = e.code;
                exit_message = e.message;
                break;
            }
            _ => {}
        }
    }

    Ok(json!({
        "exitCode": exit_code,
        "stdout": String::from_utf8_lossy(&stdout),
        "stderr": String::from_utf8_lossy(&stderr),
        "message": exit_message,
    }))
}

/// Execute a gRPC call and serialize the reply to JSON, mapping a gRPC
/// [`tonic::Status`] into a structured error string for the agent.
async fn grpc<T, F>(call: F) -> Result<Value, String>
where
    T: Serialize,
    F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match call.await {
        Ok(response) => serde_json::to_value(response.into_inner()).map_err(|e| e.to_string()),
        Err(status) => {
            let err = CliError::from_status("request failed", &status);
            Err(serde_json::to_string(&err).unwrap_or_else(|_| status.message().to_string()))
        }
    }
}

/// `NodeMsg` fields that carry JSON as text on the wire.
const NODE_JSON_FIELDS: [&str; 4] = [
    "configAttrs",
    "fingerprintAttrs",
    "dynamicAttrs",
    "healthChecks",
];

/// Decode the JSON-text attribute fields of the node(s) in a reply's `node`
/// or `nodes`, so an agent reads them as objects, as `legion node list
/// --json` prints them, instead of a JSON string inside JSON. A field that
/// does not parse is left as the string it was.
fn decode_node_attrs(mut reply: Value) -> Value {
    fn decode(node: &mut Value) {
        for key in NODE_JSON_FIELDS {
            let parsed = node
                .get(key)
                .and_then(Value::as_str)
                .and_then(|text| serde_json::from_str::<Value>(text).ok());
            if let Some(parsed) = parsed {
                node[key] = parsed;
            }
        }
    }
    if let Some(node) = reply.get_mut("node").filter(|node| node.is_object()) {
        decode(node);
    }
    if let Some(Value::Array(nodes)) = reply.get_mut("nodes") {
        nodes.iter_mut().for_each(decode);
    }
    reply
}

fn str_arg(args: &Value, key: &str) -> Result<String, String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing string argument: {key}"))
}

fn opt_str_arg(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// A job argument resolved against the tool's optional `namespace`
/// argument: `web` (in `namespace`, else `default`) or `payments-prod/web`.
fn job_arg(args: &Value, key: &str) -> Result<String, String> {
    let raw = str_arg(args, key)?;
    crate::structs::namespace::resolve_job_ref(&raw, opt_str_arg(args, "namespace").as_deref())
}

fn opt_job_arg(args: &Value, key: &str) -> Result<Option<String>, String> {
    match opt_str_arg(args, key) {
        Some(raw) => crate::structs::namespace::resolve_job_ref(
            &raw,
            opt_str_arg(args, "namespace").as_deref(),
        )
        .map(Some),
        None => Ok(None),
    }
}

/// Tools whose `name` argument is a job (the rest use `job` / `job_id`).
const JOB_NAME_TOOLS: &[&str] = &[
    "legion_job_info",
    "legion_job_logs",
    "legion_job_spec",
    "legion_job_stop",
    "legion_job_scale",
    "legion_job_scale_status",
    "legion_job_destroy",
];

/// Tools that submit a spec into a namespace.
const SPEC_NAMESPACE_TOOLS: &[&str] = &[
    "legion_job_run",
    "legion_job_plan",
    "legion_template_render",
    "legion_template_run",
];

/// A tool's input schema, with the namespace arguments every job-addressing
/// tool takes (docs/namespaces.md): `namespace` on tools that name one job
/// or submit one spec, `namespaces` on the job list.
fn tool_schema(tool: &ToolDef) -> Value {
    let mut schema = (tool.input_schema)();
    let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) else {
        return schema;
    };
    if tool.name == "legion_job_list" {
        properties.insert(
            "namespaces".into(),
            json!({
                "type": "array",
                "items": { "type": "string" },
                "description": "Namespaces to list: names or `*`-globs. Omit for `default`; [\"*\"] for every namespace this token can read."
            }),
        );
        return schema;
    }
    let names_a_job = JOB_NAME_TOOLS.contains(&tool.name)
        || SPEC_NAMESPACE_TOOLS.contains(&tool.name)
        || properties.contains_key("job")
        || properties.contains_key("job_id");
    if names_a_job {
        properties.insert(
            "namespace".into(),
            json!({
                "type": "string",
                "description": "Namespace the job lives in (default `default`). A job argument may instead be qualified, `namespace/name`; the two must agree."
            }),
        );
    }
    schema
}

fn opt_u32_arg(args: &Value, key: &str) -> Option<u32> {
    args.get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

/// The `variables` object argument serialized for `variables_json` fields.
fn variables_arg(args: &Value) -> String {
    args.get("variables")
        .filter(|value| value.is_object())
        .map(Value::to_string)
        .unwrap_or_else(|| "{}".to_string())
}

fn tool_success(data: &Value) -> Value {
    let text = serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string());
    json!({ "content": [{ "type": "text", "text": text }], "isError": false })
}

fn tool_error(message: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
}

fn json_rpc_result(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id.unwrap_or(Value::Null), "result": result })
}

fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS spot-check re-run: `legion_node_list` returned `configAttrs` and
    /// `dynamicAttrs` as JSON strings inside the JSON reply. Both MCP and
    /// `legion node list --json` must give objects.
    #[test]
    fn node_attrs_are_objects_not_json_strings_test() {
        let node = crate::structs::NodeMsg {
            id: "w-1".into(),
            config_attrs: r#"{"workerAddress":"https://10.0.0.5:4848"}"#.into(),
            fingerprint_attrs: r#"{"memory":1024}"#.into(),
            dynamic_attrs: r#"{"rack":"r1"}"#.into(),
            health_checks: "{}".into(),
            ..Default::default()
        };
        let reply = crate::structs::GetNodesReply {
            nodes: vec![node.clone()],
            total_count: 1,
            ..Default::default()
        };
        let decoded = decode_node_attrs(serde_json::to_value(reply).expect("serialize"));
        let listed = &decoded["nodes"][0];
        assert_eq!(
            listed["configAttrs"]["workerAddress"],
            "https://10.0.0.5:4848"
        );
        assert_eq!(listed["dynamicAttrs"]["rack"], "r1");
        assert_eq!(listed["fingerprintAttrs"]["memory"], 1024);
        assert_eq!(decoded["totalCount"], 1);

        let single = decode_node_attrs(json!({ "node": serde_json::to_value(&node).unwrap() }));
        assert_eq!(single["node"]["dynamicAttrs"]["rack"], "r1");

        // The CLI's `--json` shape is the same.
        let cli = serde_json::to_value(crate::structs::Node::from(node)).expect("serialize");
        assert_eq!(cli["configAttrs"]["workerAddress"], "https://10.0.0.5:4848");
        assert_eq!(cli["dynamicAttrs"]["rack"], "r1");
    }

    #[test]
    fn config_parses_serve() {
        let m = config()
            .try_get_matches_from(["mcp", "serve", "--allow-write"])
            .unwrap();
        let (name, sub) = m.subcommand().unwrap();
        assert_eq!(name, "serve");
        assert!(sub.get_flag("allow-write"));
    }

    #[test]
    fn read_only_hides_mutating_tools() {
        let names: Vec<&str> = available_tools(false).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_list"));
        assert!(names.contains(&"legion_instance_list"));
        assert!(names.contains(&"legion_instance_get"));
        assert!(!names.contains(&"legion_job_run"));
        assert!(!names.contains(&"legion_job_destroy"));
        assert!(!names.contains(&"legion_instance_create"));
        assert!(!names.contains(&"legion_instance_stop"));
        assert!(!names.contains(&"legion_instance_start"));
        assert!(!names.contains(&"legion_instance_promote"));
        assert!(!names.contains(&"legion_instance_destroy"));
    }

    #[test]
    fn job_logs_tool_is_read_only() {
        let names: Vec<&str> = available_tools(false).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_logs"));
    }

    #[test]
    fn service_discovery_tools_are_read_only() {
        let names: Vec<&str> = available_tools(false).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_service_list"));
        assert!(names.contains(&"legion_service_resolve"));
    }

    #[test]
    fn service_resolve_schema_requires_the_service_name() {
        let tool = tool_defs()
            .into_iter()
            .find(|tool| tool.name == "legion_service_resolve")
            .expect("legion_service_resolve");
        let schema = (tool.input_schema)();
        assert_eq!(schema["required"], json!(["service"]));
        assert!(schema["properties"]["include_unhealthy"].is_object());
    }

    #[tokio::test]
    async fn service_resolve_rejects_a_missing_service_argument() {
        let req = json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": { "name": "legion_service_resolve", "arguments": {} }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("service"), "{text}");
    }

    #[test]
    fn allow_write_exposes_mutating_tools() {
        let names: Vec<&str> = available_tools(true).iter().map(|t| t.name).collect();
        assert!(names.contains(&"legion_job_run"));
        assert!(names.contains(&"legion_job_stop"));
        assert!(names.contains(&"legion_job_scale"));
        assert!(names.contains(&"legion_instance_create"));
        assert!(names.contains(&"legion_instance_stop"));
        assert!(names.contains(&"legion_instance_start"));
        assert!(names.contains(&"legion_instance_promote"));
        assert!(names.contains(&"legion_instance_destroy"));
    }

    #[tokio::test]
    async fn initialize_returns_protocol_version() {
        let req = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" }).to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(resp["result"]["serverInfo"]["name"], "legion");
    }

    #[tokio::test]
    async fn tools_list_respects_write_policy() {
        let req = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }).to_string();
        let resp = handle_message(&req, false).await.expect("response");
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(names.contains(&"legion_capabilities"));
        assert!(!names.contains(&"legion_job_run"));
    }

    #[tokio::test]
    async fn notifications_get_no_response() {
        let req = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string();
        assert!(handle_message(&req, false).await.is_none());
    }

    #[tokio::test]
    async fn job_schema_tool_runs_without_manager() {
        let req = json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "legion_job_schema", "arguments": {} }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], false);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("Legion Jobspec"));
    }

    #[tokio::test]
    async fn validate_tool_reports_invalid_spec() {
        let req = json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "legion_job_validate", "arguments": { "spec_json": "{not json}" } }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn mutating_tool_blocked_without_allow_write() {
        let req = json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": { "name": "legion_job_destroy", "arguments": { "name": "demo" } }
        })
        .to_string();
        let resp = handle_message(&req, false).await.expect("response");
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("--allow-write"));
    }
}
