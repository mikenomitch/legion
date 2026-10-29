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
            cli: "legion job list [--status STATUS] [-n NAMESPACES | -A]",
            grpc: Some("Manager/ListJobs"),
            summary: "List jobs and their status in the active namespace (or -n globs, or -A for every namespace you can read), optionally filtered to one status.",
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
            summary: "Dry-run: preview which nodes allocations would land on, which running allocations the submit would preempt, and whether each gang would reach its quorum. Supports --allow-unknown-fields.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.validate",
            cli: "legion job validate <PATH>",
            grpc: Some("Manager/GetJobSchema"),
            summary: "Render and validate a jobspec without submitting. Unknown keys are errors with a suggestion; installed apps' keys are checked against their schemas when a manager is reachable (offline, they are reported as not checked); --allow-unknown-fields reports what a submit would keep and drop.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.schema",
            cli: "legion job schema",
            grpc: Some("Manager/GetJobSchema"),
            summary: "Emit this cluster's jobspec JSON Schema: the core fields and built-in task runtimes, plus each key installed apps add at its point (the compiled-in schema, saying so, when no manager is reachable).",
            mutates: false,
            read_only_safe: true,
        },
        // --- Namespaces (docs/namespaces.md) ---
        Capability {
            name: "namespace.list",
            cli: "legion namespace list",
            grpc: Some("Manager/ListNamespaces"),
            summary: "List the namespaces this token can see, with job counts.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "namespace.get",
            cli: "legion namespace get <NAME>",
            grpc: Some("Manager/GetNamespace"),
            summary: "Show one namespace, what it holds by kind, and (with acl:read) the ACL rules that reach it.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "namespace.apply",
            cli: "legion namespace create <NAME> | legion namespace apply -f <FILE>",
            grpc: Some("Manager/ApplyNamespace"),
            summary: "Create a namespace or update its description and meta. --dry-run reports the ACL rules that would reach it.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "namespace.delete",
            cli: "legion namespace delete <NAME> [--cascade] [--dry-run]",
            grpc: Some("Manager/DeleteNamespace"),
            summary: "Delete a namespace. Refused while it holds anything; --cascade stops and destroys its contents first. --dry-run lists them.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "namespace.policy.list",
            cli: "legion namespace policy list",
            grpc: Some("Manager/ListNamespacePolicies"),
            summary: "List namespace policies: quota, counts, pools, runtimes and host volumes over namespace globs. Needs acl:read.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "namespace.policy.get",
            cli: "legion namespace policy get <POLICY>",
            grpc: Some("Manager/GetNamespacePolicy"),
            summary: "Show one namespace policy with its limits and charged usage across every namespace it matches. Needs acl:read.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "namespace.policy.apply",
            cli: "legion namespace policy apply -f <FILE> [--dry-run]",
            grpc: Some("Manager/ApplyNamespacePolicy"),
            summary: "Create or replace a namespace policy. --dry-run lists what it would have refused among existing jobs and counts. Needs acl:write.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "namespace.policy.delete",
            cli: "legion namespace policy delete <POLICY>",
            grpc: Some("Manager/DeleteNamespacePolicy"),
            summary: "Delete a namespace policy. Needs acl:write.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Jobs: write ---
        Capability {
            name: "job.run",
            cli: "legion job run <PATH>",
            grpc: Some("Manager/RunJob"),
            summary: "Submit or update a job. Supports --wait, --idempotency-key, and --allow-unknown-fields.",
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
            cli: "legion job logs [<NAME>] [--allocation <ID>] [--include-removed] [--follow]",
            grpc: Some("Manager/GetAllocationLogs"),
            summary: "Fetch captured stdout/stderr for a job's allocations. Allocations removed \
                 in the last 24 hours stay readable: a job with none live returns its newest \
                 removed ones, each marked `removed` with its `removalCause`.",
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
            cli: "legion job scale <NAME> [--count N [--for D] | --min N --for D | --max N --for D | --release | --reset]",
            grpc: Some("Manager/ScaleTask"),
            summary: "Change a task's count without a new job version or rollout (docs/autoscaling.md). On an autoscaled task a count is a time-boxed pin and needs --for; --min/--max pin a floor or ceiling; --release ends a pin. Supports --idempotency-key.",
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
        // --- Task autoscaling (docs/autoscaling.md) ---
        Capability {
            name: "scaling.status",
            cli: "legion scaling status [JOB] [-n NAMESPACES | -A]",
            grpc: Some("Manager/GetScalingStatus"),
            summary: "Every autoscaled task's count, wanted count, bounds, mode, and state (ok, held, limited, pinned, paused, dry-run, stale, ...), and whether the autoscaler app is running.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.explain",
            cli: "legion scaling explain <JOB | --standalone NAME> [--group G] [--task T] [--policy] [-n NAMESPACE]",
            grpc: Some("Manager/GetScalingStatus"),
            summary: "Why an autoscaled task (or, with --standalone, a standalone policy's target) is at its count: each check's reading and every safeguard stage of the last evaluation; with --policy, the effective policy and where each field came from.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.history",
            cli: "legion scaling history <JOB | --standalone NAME> [--group G] [--task T] [--limit N] [-n NAMESPACE]",
            grpc: Some("Manager/GetScaleHistory"),
            summary: "The durable record of a job's (or a standalone policy's) count changes, newest first: people's and the autoscaler's, pins, dry-run \"would\" rows, and failed target calls.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.pause",
            cli: "legion scaling pause <JOB | --standalone NAME | --all> [--group G] [--task T] [--for D] [--reason R] [-n NAMESPACE]",
            grpc: Some("Manager/PauseScaling"),
            summary: "Freeze autoscaling for a task, a job, a standalone policy, or the cluster (--all, needs operator:write); the server refuses autoscaler writes while paused. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "scaling.resume",
            cli: "legion scaling resume <JOB | --standalone NAME | --all> [--group G] [--task T] [-n NAMESPACE]",
            grpc: Some("Manager/PauseScaling"),
            summary: "Lift a pause set at the same scope.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "scaling.defaults.get",
            cli: "legion scaling defaults get [-n NAMESPACE]",
            grpc: Some("Manager/GetScalingDefaults"),
            summary: "The cluster's scaling defaults, and the effective defaults for a namespace with the level each field came from.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.defaults.apply",
            cli: "legion scaling defaults apply <FILE> [--dry-run] [--force]",
            grpc: Some("Manager/ApplyScalingDefaults"),
            summary: "Set the cluster's scaling defaults (needs operator:write); every inheriting task follows live. --dry-run lists the policies that would change; a change that invalidates some is refused unless --force. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "scaling.simulate",
            cli: "legion scaling simulate <JOB | FILE> [--since D] [--series CSV] [--replay | --follow] [--cold] [--all]",
            grpc: Some("Manager/GetScalingInputs"),
            summary: "Run a policy (a running job's, a jobspec's, or a standalone policy file's) over up to an hour of recorded history or a CSV series and list the decisions it makes. The live policy over its own history makes the decisions the autoscaler made; an edited one shows what it would have done. Writes nothing.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.apply",
            cli: "legion scaling apply <FILE> [--dry-run] [--wait] [-n NAMESPACE]",
            grpc: Some("Manager/ApplyScalingPolicy"),
            summary: "Create or update a standalone scaling policy (an external target scaled through a target plugin) or a node group (machines a node target plugin starts, drained and terminated by Legion). --dry-run validates and shows the field-by-field diff without writing; --wait blocks until the autoscaler has evaluated it. Needs scale:write on the policy's namespace and name; a node group needs operator:write and node:write. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "scaling.list",
            cli: "legion scaling list [-n NAMESPACES | -A] [--detail]",
            grpc: Some("Manager/ListScalingPolicies"),
            summary: "Every scaling policy the caller can read: autoscaled tasks, standalone policies, and node groups (with node:read), with kind, bounds, mode and version.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.delete",
            cli: "legion scaling delete <NAME> [-n NAMESPACE]",
            grpc: Some("Manager/DeleteScalingPolicy"),
            summary: "Delete a standalone scaling policy or a node group (and its pause and action row); the target keeps its current size. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "scaling.plugin.test",
            cli: "legion scaling plugin test <COMMAND... | URL> [--config JSON] [--policy-config JSON] [--query Q]",
            grpc: None,
            summary: "Run the scaling-plugin conformance suite against a plugin program or URL: the handshake, schema, every method its kinds declare, unknown and malformed requests, crash and restart, and timeouts. Local; talks to no manager.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "job.scaling-inputs",
            cli: "legion job info <NAME>",
            grpc: Some("Manager/GetScalingInputs"),
            summary: "Cluster-wide request vs usage per task, batched: ready/placed/desired/unplaced replicas, measured CPU and memory against reservations, service request rate/errors/latency (internal callers), rollout state, and data freshness, with up to an hour of 10-second history. Needs scale:read (docs/autoscaling.md, \"Scaling inputs\").",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "scaling.demand",
            cli: "legion scaling demand [--pool P --cpu MHZ --memory MIB ... | -f nodes.ncl]",
            grpc: Some("Manager/GetCapacityDemand"),
            summary: "How many nodes of a node-group template the pending work needs, packed \
                 with the scheduler's own rules, plus the replicas no template can host and \
                 the rule that fails. Read-only; needs node:read (docs/autoscaling.md, \
                 \"Capacity demand\").",
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
                "Render a template with variables and submit the job. Supports --idempotency-key and --allow-unknown-fields.",
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
            summary: "List a job's stateful instances (slot and keyed, incl. tombstones), then \
                 its running sessions from the session index, with `complete` and `asOf`.",
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
            cli: "legion instance create <JOB> [--id <KEY>] [--from-snapshot <NAME|ID>]",
            grpc: Some("Manager/CreateInstance"),
            summary: "Create a keyed-mode instance (never-recycled id), optionally restored from \
                 a snapshot of the job (needs snapshot:read). On a session task without --id, \
                 start an unnamed session with no raft write (durable false). Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.create-batch",
            cli: "legion instance create <JOB> --id <KEY>... --count <N> [--from-snapshot <NAME|ID>]",
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
            cli: "legion instance start <ID> [--cold] [--wait]",
            grpc: Some("Manager/StartInstance"),
            summary: "Start a stopped keyed instance (same id, retained storage), or resume a \
                 suspended one from its checkpoint (--cold boots fresh instead). Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "instance.suspend",
            cli: "legion instance suspend <ID> [--checkpoint memory|disk] [--wait]",
            grpc: Some("Manager/SuspendInstance"),
            summary: "Checkpoint a keyed instance, stop it, and free its reservation; a failed \
                 capture leaves it running. Memory checkpoints need snapshot:write. Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        // Fork.
        Capability {
            name: "instance.fork",
            cli: "legion instance fork <ID> [--count N] [--checkpoint memory|disk] [--spread] [--wait]",
            grpc: Some("Manager/ForkInstance"),
            summary: "Fork a running keyed instance into N new instances (<id>-f<n>), each with \
                 its own cloned volume and identity, on the origin's node (or, with --spread, \
                 each on its own node from a published checkpoint); the origin keeps running. \
                 Needs job:write and snapshot:write. Supports --idempotency-key.",
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
            name: "instance.promote",
            cli: "legion instance promote <SESSION> [--id <KEY>]",
            grpc: Some("Manager/PromoteInstance"),
            summary: "Turn a running session into a keyed instance in place (one raft entry; \
                 nothing restarts), keeping its id or naming it with --id. Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "session.nodes",
            cli: "legion session nodes <JOB> [--task <TASK>] [--format envoy-eds|haproxy]",
            grpc: Some("Manager/ListSessionNodes"),
            summary: "List the nodes holding an envelope for a session task: address, node tag, \
                 whether each accepts edge starts (and why not), room left, and a weight in \
                 sessions. Renders Envoy EDS or HAProxy server lines for a picker.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "session.start",
            cli: "legion session start <JOB> --task <TASK> --node <ADDR> --ticket <T>",
            grpc: Some("Worker/StartSession"),
            summary: "Start a session on one node with a session ticket, with no manager call \
                 (the edge form, for testing and scripts). Supports --idempotency-key \
                 (node-local).",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "session.get",
            cli: "legion session get <SESSION> --node <ADDR> --ticket <T>",
            grpc: Some("Worker/GetSession"),
            summary: "Read a session from its node with a session ticket.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "session.stop",
            cli: "legion session stop <SESSION> --node <ADDR> --ticket <T>",
            grpc: Some("Worker/StopSession"),
            summary: "End a session on its node with a session ticket. Idempotent.",
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
        // --- Snapshots (see docs/suspend-and-resume.md) ---
        Capability {
            name: "snapshot.list",
            cli: "legion snapshot list [--job <J>] [--instance <I>] [--state <S>]",
            grpc: Some("Manager/ListSnapshots"),
            summary: "List instance snapshots, newest first, with kind, state, node, and size.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "snapshot.get",
            cli: "legion snapshot get <ID|NAME> [--job <J>]",
            grpc: Some("Manager/GetSnapshot"),
            summary: "Inspect one snapshot, including its manifest and why it failed, if it did.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "snapshot.create",
            cli: "legion snapshot create <INSTANCE> [--memory|--disk] [--name <N>] [--publish] \
                 [--wait]",
            grpc: Some("Manager/CreateSnapshot"),
            summary: "Snapshot a running instance's volume (disk) or its guest memory too \
                 (memory, sandbox tasks). --publish uploads it to the node's contentStore so it \
                 restores on any compatible node. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "snapshot.delete",
            cli: "legion snapshot delete <ID>",
            grpc: Some("Manager/DeleteSnapshot"),
            summary: "Delete a snapshot; its node reclaims the bytes. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        // Phase 6: named snapshots.
        Capability {
            name: "snapshot.alias.set",
            cli: "legion snapshot alias set <JOB> <NAME> <SNAPSHOT>",
            grpc: Some("Manager/SetSnapshotAlias"),
            summary: "Point a snapshot name at a local or published manual snapshot of the \
                 job, creating or moving it (instance create --from-snapshot <NAME> uses it). \
                 Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "snapshot.alias.delete",
            cli: "legion snapshot alias delete <JOB> <NAME>",
            grpc: Some("Manager/DeleteSnapshotAlias"),
            summary: "Remove a snapshot name; the snapshot is untouched. Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "snapshot.restore",
            cli: "legion snapshot restore <INSTANCE> <SNAPSHOT> [--wait]",
            grpc: Some("Manager/RestoreInstance"),
            summary: "Roll a keyed instance back to one of its snapshots in place, on the node \
                 that holds it (any compatible node for a published snapshot). Supports \
                 --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "snapshot.warm",
            cli: "legion snapshot warm <ID|NAME> [--job <J>] [--constraint <K=V>]...",
            grpc: Some("Manager/WarmSnapshot"),
            summary: "Pre-fetch a published snapshot's parts onto matching nodes so a restore \
                 there starts from local bytes. Best effort and idempotent; triggers downloads, \
                 so it counts as mutating. Supports --idempotency-key.",
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
            name: "node.usage",
            cli: "legion node usage <ID>",
            grpc: Some("Manager/GetNodeUsage"),
            summary: "Show a node's memory and CPU capacity, reservations, and measured usage per \
                 allocation (current, peak, CPU, idle time), from cgroup reports workers with \
                 resource enforcement send every 10 seconds.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "node.usage.list",
            cli: "legion node usage",
            grpc: Some("Manager/ListNodeUsage"),
            summary: "Show memory and CPU capacity, reservations, measured use, and memory \
                 pressure status for every node in one call (the batch form of node.usage, \
                 per-node totals only unless include_allocations is set).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "node.content",
            cli: "legion node content <ID>",
            grpc: Some("Manager/GetNodeContent"),
            summary: "Show what a node holds for content locality: the content it advertises to \
                 the manager's location index, which snapshot each entry belongs to, and its \
                 snapshots with how many of their parts it holds.",
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
            name: "node.forget",
            cli: "legion node forget <ID> [--force]",
            grpc: Some("Manager/ForgetNode"),
            summary: "Remove a dead node's record and revoke its node token in one write. \
                 Refused without --force while it has allocations, is live and not cordoned, \
                 or holds node-local volumes. Supports --idempotency-key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.join-token.create",
            cli: "legion join-token create [--max-uses N] [--attr K=V]...",
            grpc: Some("Manager/CreateJoinToken"),
            summary: "Create a join token that introduces a worker to the cluster, optionally \
                 admitting at most N new nodes and stamping attributes on each.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "node.join-token.list",
            cli: "legion join-token list",
            grpc: Some("Manager/ListJoinTokens"),
            summary: "List worker join tokens (metadata only), with uses against max_uses and \
                 the attributes each stamps.",
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
        // --- Exec and actions (docs/exec-and-actions.md) ---
        Capability {
            name: "exec",
            cli: "legion exec <ALLOC>|--job <JOB>|--instance <ID> [-it] [--direct] -- <CMD>",
            grpc: Some("Manager/ExecAllocation"),
            summary: "Run a command inside a running allocation, through the manager or, with \
                      --direct, against its worker with an exec ticket.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "exec-ticket.mint",
            cli: "legion exec-ticket mint <ALLOC>|--instance <ID> [--action A] [-- <CMD>]",
            grpc: Some("Manager/MintExecTicket"),
            summary: "Mint a short-lived, single-use ticket for one direct exec session on the \
                      worker running the target (no raft write).",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "exec-ticket.sign",
            cli: "legion exec-ticket sign --key <PEM> --key-id <ID> --node <N> --alloc <A> --job <J> --task <T>",
            grpc: None,
            summary: "Sign an exec ticket offline with a delegated exec key's private key (no \
                      manager call).",
            mutates: false,
            read_only_safe: false,
        },
        Capability {
            name: "exec-key.create",
            cli: "legion exec-key create --jobs <GLOB> --public-key <PEM>",
            grpc: Some("Manager/CreateExecKey"),
            summary: "Register a platform's public key for minting exec tickets for matching \
                      jobs on non-host runtimes.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "exec-key.list",
            cli: "legion exec-key list",
            grpc: Some("Manager/ListExecKeys"),
            summary: "List delegated exec minting keys and their ceilings.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "exec-key.delete",
            cli: "legion exec-key delete <ID>",
            grpc: Some("Manager/DeleteExecKey"),
            summary: "Delete a delegated exec key; workers drop it and end its sessions on \
                      their next heartbeat.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "action.list",
            cli: "legion action list <JOB>",
            grpc: Some("Manager/ListJobActions"),
            summary: "List the actions declared on a job's tasks, with their parameters.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "action.run",
            cli: "legion action run <JOB> <ACTION> [--alloc <ID>|--instance <ID>] [--param k=v] \
                  [--json]",
            grpc: Some("Manager/ExecAllocation"),
            summary: "Run a declared action in a running allocation, rendered by the manager; \
                      `--json` returns its exit code and output.",
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
        // --- Scheduler diagnostics ---
        Capability {
            name: "scheduler.concurrency",
            cli: "legion scheduler concurrency",
            grpc: Some("Manager/GetSchedulerConcurrency"),
            summary: "Report how many evals the manager will plan at once and why: the current \
                 limit against evalThreads, the plan-conflict and fallback rates, goodput, and \
                 whether conflicts are concurrency-driven (wait) or capacity-driven (add nodes).",
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
            summary: "Read a value from the KV store. A missing key is a `NotFound` error \
                 (non-zero exit), so `kv get || kv put` works.",
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
            name: "keyring.root_key_generate",
            cli: "legion keyring root-key generate",
            grpc: None,
            summary: "Print a new base64 root key line for the file or env provider. Runs \
                 locally; nothing on the cluster changes until the key is installed and \
                 `keyring root-key rewrap` runs.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "keyring.provider.test",
            cli: "legion keyring provider test [--timeout D] -- <COMMAND...>",
            grpc: None,
            summary: "Conformance-test an exec root-key provider before pointing a manager at \
                 it: describe, a wrap/unwrap round trip, a tampered ciphertext refused, and \
                 randomized wrapping. Runs locally; never touches the cluster, but the \
                 command really calls its KMS.",
            mutates: false,
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
        Capability {
            name: "acl.grants.list",
            cli: "legion acl grants list [--job JOB] [--all]",
            grpc: Some("Manager/ListGrants"),
            summary: "List job grants: what each job version's `legion` identity was granted at submit, and by whom.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "acl.grants.revoke",
            cli: "legion acl grants revoke <JOB> [--version N]",
            grpc: Some("Manager/RevokeGrant"),
            summary: "Revoke a job's grants; its workload tokens lose them at their next request.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Apps (docs/apps.md) ---
        Capability {
            name: "app.list",
            cli: "legion app list",
            grpc: Some("Manager/ListApps"),
            summary: "List installed apps (Legion's add-ons): version, source, status, placement, replicas running, and UI slots.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.status",
            cli: "legion app status <APP>",
            grpc: Some("Manager/GetApp"),
            summary: "Show one app: its record, grant, job status, the reconciler's last action and error, and its build marker.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.logs",
            cli: "legion app logs <APP> [-f]",
            grpc: Some("Manager/GetAllocationLogs"),
            summary: "Fetch an app's task logs (the logs of its job, legion-system/<app>).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.config.get",
            cli: "legion app config get <APP>",
            grpc: Some("Manager/GetApp"),
            summary: "Print an app's config as JSON.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.config.schema",
            cli: "legion app config schema <APP>",
            grpc: Some("Manager/GetApp"),
            summary: "Print an app's config schema (JSON Schema, draft 7).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.config.set",
            cli: "legion app config set <APP> -f <FILE> [--dry-run]",
            grpc: Some("Manager/SetAppConfig"),
            summary: "Replace an app's config, validated against its schema and the job's admission; the app's job rolls. Changing a code field needs the app's whole grant. --dry-run checks without changing.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.set",
            cli: "legion app set <APP> [--replicas N] [--placement managers|workers|any] [--pool POOL] [--cpu MHZ] [--memory MIB] [--dry-run]",
            grpc: Some("Manager/UpdateAppSettings"),
            summary: "Change an app's settings (the only way to change its job's count or placement: direct writes to an app's job are refused); its job rolls with them. --dry-run checks without changing.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.enable",
            cli: "legion app enable <APP> [--dry-run]",
            grpc: Some("Manager/SetAppEnabled"),
            summary: "Enable an app: its job runs again. On an enabled app, resubmits a job that does not run healthy at its current version (the retry for a failed upgrade).",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.disable",
            cli: "legion app disable <APP> [--dry-run]",
            grpc: Some("Manager/SetAppEnabled"),
            summary: "Disable an app: its job stops; its record, config, and grant are kept.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.install",
            cli: "legion app install <FILE|URL> [--yes] [--dry-run] [--key POINT.KEY=NEW]",
            grpc: Some("Manager/InstallApp"),
            summary: "Install an app from a manifest. The caller must hold every permission it asks for; --dry-run shows the prompt and whether it does, and which stored jobspec values the app's keys would adopt. --key installs a key under another name when another app owns it.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.upgrade",
            cli: "legion app upgrade <APP> <FILE|URL> [--yes] [--force] [--dry-run]",
            grpc: Some("Manager/UpgradeApp"),
            summary: "Replace an installed app's manifest; shows the permission diff, and new permissions need approval. --dry-run reports `uncovered` (a permission the token lacks) and `orphanedJobs`; dropping a jobspec key jobs use needs --force.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.uninstall",
            cli: "legion app uninstall <APP> [--force] [--purge] [--dry-run]",
            grpc: Some("Manager/UninstallApp"),
            summary: "Uninstall an app: stop its job, revoke its grant, and drop its record (a corrupt record too). Needs --force when jobs set its jobspec keys; their values stay stored, flagged orphaned. --purge also deletes its KV under legion/apps/<app>/.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.extensions",
            cli: "legion app extensions [APP]",
            grpc: Some("Manager/ListApps"),
            summary: "List the jobspec keys installed apps add: each key's point (job, group, task, volume, port, service), owner, description, schema, examples, and whether setting it needs an extra permission.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.views",
            cli: "legion app view <APP>",
            grpc: Some("Manager/ListUiExtensions"),
            summary: "List what an app shows you in the UI, and where: sidebar pages, overview cards, job and node panels, task badges, filtered by each one's declared permission.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.view",
            cli: "legion app view <APP> <nav|overview|job|task|node> [CTX...] [--view VIEW]",
            grpc: Some("Manager/GetAppView"),
            summary: "Render an app's declarative view (what its UI panel shows) as text, or its validated legion-view/1 JSON with -j; checked as you against the view's declared permission before the app is asked.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "app.action",
            cli: "legion app view <APP> <job|task|node> <CTX...> (lists the view's actions; they run from the UI)",
            grpc: Some("Manager/InvokeAppAction"),
            summary: "Run an action an app declares (a button in its view) on a job, task, or node, checked as you against the action's declared permission there; audited as an app.action event and retry-safe with an idempotency key.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "app.page",
            cli: "legion app view <APP> (lists the app's sandboxed pages; they open in the UI)",
            grpc: Some("Manager/GetAppPage"),
            summary: "Fetch an app's sandboxed page (ui.pages): its single-file HTML bundle and the RPCs it may call through the UI's bridge as you, checked against the page's declared permission before the app is asked. For the UI, which mounts it in a sandboxed iframe.",
            mutates: false,
            read_only_safe: true,
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
            name: "net.policy.apply",
            cli: "legion net policy apply -f <FILE> [--dry-run]",
            grpc: Some("Manager/UpsertNetPolicy"),
            summary: "Create or replace a network policy object (selector-scoped egress/ingress \
                 rules and handlers); --dry-run validates and lists affected allocations \
                 without writing.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "net.policy.list",
            cli: "legion net policy list",
            grpc: Some("Manager/ListNetPolicies"),
            summary: "List stored network policies in merge order (require first, then priority).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.policy.describe",
            cli: "legion net policy describe <NAME>",
            grpc: Some("Manager/GetNetPolicyObject"),
            summary: "Fetch one stored network policy object.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.policy.delete",
            cli: "legion net policy delete <NAME>",
            grpc: Some("Manager/DeleteNetPolicy"),
            summary: "Delete a stored network policy; matching allocations lose it on their next \
                 heartbeat.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "net.plugin.new",
            cli: "legion net plugin new <NAME> [--dir <DIR>]",
            grpc: None,
            summary: "Scaffold a WASM network plugin crate (proxy-wasm Rust SDK) and its object \
                 file. Local only.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.plugin.test",
            cli: "legion net plugin test <MODULE|NAME> --request '<METHOD> <URL>' [--label k=v] \
                 [--config <JSON>]",
            grpc: None,
            summary: "Run a .wasm module or a stored plugin against one synthetic request, \
                 locally: per stage what it did, what it changed, its time and fuel.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.plugin.apply",
            cli: "legion net plugin apply -f <FILE> [--dry-run] [--idempotency-key <KEY>]",
            grpc: Some("Manager/UpsertNetPlugin"),
            summary: "Create or replace a network plugin object; --dry-run validates it and \
                 every policy that uses it without writing.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "net.plugin.list",
            cli: "legion net plugin list",
            grpc: Some("Manager/ListNetPlugins"),
            summary: "List stored network plugin objects.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.plugin.describe",
            cli: "legion net plugin describe <NAME>",
            grpc: Some("Manager/GetNetPlugin"),
            summary: "Fetch one stored plugin object and the policies that use it.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.plugin.status",
            cli: "legion net plugin status <NAME>",
            grpc: Some("Manager/GetNetPluginStatus"),
            summary: "Per node: the revision held, module state, calls, errors, last error, \
                 and p99 callback time.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.plugin.delete",
            cli: "legion net plugin delete <NAME>",
            grpc: Some("Manager/DeleteNetPlugin"),
            summary: "Delete a stored plugin object (refused while a policy uses it).",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "net.policy.show",
            cli: "legion net policy show <JOB> [--task <TASK>] [--pool <POOL>]",
            grpc: Some("Manager/ShowNetPolicy"),
            summary: "Show a task's effective network policy: every matching stored policy merged \
                 with the task's own block, each rule naming its source.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.doctor",
            cli: "legion net doctor <JOB> [--task <TASK>]",
            grpc: Some("Manager/ShowNetPolicy"),
            summary: "Walk a task's network layers — policy resolved, mode and whether it is \
                 enforced, datapath readiness per allocation, the edge-boot gate — and name \
                 the first one that is wrong with a fix hint.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "sandbox.guest-agent",
            cli: "legion guest-agent",
            grpc: None,
            summary: "Internal: the gated sandbox guest agent, started by the generated guest \
                 init inside a microVM to relay the guest's loopback proxy and DNS ports, and \
                 each declared upstream's loopback port and VIP, to the node datapath over \
                 vsock, and the datapath's inbound connections to the workload's advertised \
                 ports. On a sandbox launched for memory checkpoints it also runs the resume \
                 watch, closes its relays on a restore, and retargets its upstreams. Not for \
                 operators or agents to run.",
            mutates: false,
            read_only_safe: false,
        },
        Capability {
            name: "sandbox.guest-resume",
            cli: "legion guest-resume",
            grpc: None,
            summary: "Internal: the resume handler inside a sandbox launched for memory \
                 checkpoints, started by the generated guest init. Refreshes the guest's \
                 identity, RNG, and clock and runs suspend.onResume after a restore. Not for \
                 operators or agents to run.",
            mutates: false,
            read_only_safe: false,
        },
        Capability {
            name: "net.handler.test",
            cli: "legion net handler test <URL|HOST:PORT>",
            grpc: None,
            summary: "Conformance-probe an external handler the way the datapath uses it: an \
                 authz webhook must answer a verdict to a sample facts payload; a proxy must \
                 accept a PROXY v2 header. Sends one synthetic request to the handler; never \
                 touches the cluster.",
            mutates: false,
            read_only_safe: false,
        },
        Capability {
            name: "net.ca.show",
            cli: "legion net ca show",
            grpc: Some("Manager/GetMeshCa"),
            summary: "Print the cluster CA trust bundle (never a key), its pin, the rotation \
                 phase, which provider supplies it (builtin, file, exec), and any problem that \
                 stops this manager signing: what an outside client trusts to reach a mesh-TLS \
                 port or a worker plane, or verifies a node certificate against.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "net.ca.rotate",
            cli: "legion net ca rotate",
            grpc: Some("Manager/RotateMeshCa"),
            summary: "Rotate the built-in cluster CA in two steps: the new CA, cross-signed by \
                 the old one, signs at once and the old one stays trusted for 24h, so no \
                 connection breaks. Nodes re-issue within one heartbeat. `file` and `exec` \
                 CAs rotate outside Legion; this refuses with the procedure.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "net.ca.provider.test",
            cli: "legion net ca provider test (-- <COMMAND...> | --cert-path P --key-path P [--bundle-path P])",
            grpc: None,
            summary: "Conformance-test a cluster CA provider before pointing a manager at it: \
                 an exec signer command or file CA material. Reads the trust bundle, has node \
                 and manager certificates signed, and verifies each one as a manager would. \
                 Runs locally; never touches the cluster, but an exec signer really issues the \
                 test certificates.",
            mutates: false,
            read_only_safe: false,
        },
        Capability {
            name: "net.policy.test",
            cli: "legion net policy test <JOB> --to <DEST> | --from <PEER> [--method M --path P]",
            grpc: Some("Manager/TestNetPolicy"),
            summary: "Dry-run a verdict: would this task be allowed to reach a destination (or be \
                 reached from a peer)? Names the deciding rule; sends nothing.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "service.edges",
            cli: "legion service edges [SERVICE] [--window 5m]",
            grpc: Some("Manager/ListServiceEdges"),
            summary: "The service graph: who calls whom through the datapath, with request \
                 rate, error rate, denials and latency per (caller, service, tag) over the \
                 window, plus declared-but-silent upstreams and the allow verdict.",
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
        // --- Exec & actions ---
        Capability {
            name: "exec.run",
            cli: "legion exec <ALLOC> --json -- <CMD> [ARGS...]",
            grpc: Some("Manager/ExecAllocation"),
            summary: "Run a command inside a running allocation. `--json` collects the output \
                 into one envelope (`exitCode`, `stdout`, `stderr`); the process exits with \
                 the remote exit code.",
            mutates: true,
            read_only_safe: false,
        },
        // --- Raft (manager control plane) ---
        Capability {
            name: "raft.status",
            cli: "legion raft status",
            grpc: Some("Manager/GetRaftStatus"),
            summary: "The answering manager's raft status: role, leader, term, indexes, \
                 voters, and per-lane quorum health with any advisory.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "raft.members",
            cli: "legion raft members",
            grpc: Some("Manager/ListManagers"),
            summary: "Every manager in the cluster from the replicated raft membership: role \
                 (leader/follower/learner, or standalone outside raft), advertised address, \
                 and whether the answering manager can reach it, with each reachable \
                 member's term and applied index. Any manager answers with the full list.",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "raft.history",
            cli: "legion raft history [--limit <N>]",
            grpc: Some("Manager/ListRaftHistory"),
            summary: "Recent raft events (elections, membership changes, operator commands).",
            mutates: false,
            read_only_safe: true,
        },
        Capability {
            name: "raft.command",
            cli: "legion raft command <COMMAND> [--target <NODE_ID>]",
            grpc: Some("Manager/IssueRaftCommand"),
            summary: "Issue an operator raft command such as trigger-election.",
            mutates: true,
            read_only_safe: false,
        },
        Capability {
            name: "raft.join-token",
            cli: "legion raft join-token",
            grpc: Some("Manager/GetRaftJoinToken"),
            summary: "Mint a join token for adding a manager to the raft cluster.",
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
#[instrument(skip_all)]
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
