# Agent Readiness

Legion is built to be driven by autonomous AI agents as well as by humans.
This document describes the agent-facing surfaces, the conventions they rely on,
and the recommended workflow for operating a cluster safely from an agent.

If you are an agent operating Legion, start here. Then call
`legion capabilities --json` (or the `legion_capabilities` MCP tool) to discover
the live operation catalog.

## Why these features exist

An agent-ready tool needs four things:

- **Discoverability:** the agent can list what it can do.
- **Structured I/O:** one parseable shape for both success and failure.
- **Safety under retries:** idempotency, read/write separation, and dry-runs.
- **A closed loop:** the agent can observe the effect of an action.

The features below map onto those needs.

| Pillar | Legion feature |
| --- | --- |
| Discoverability | `legion capabilities`, MCP `tools/list`, `legion job schema`, `GetStateStoreSchema` |
| Structured I/O | Universal `--json` envelope, typed errors with `code`/`hint`/`retryable` |
| Safety under retries | Idempotency keys, read-only-by-default SQL, `job plan`, `job validate` |
| Durable intent | A submit either places or stays visibly pending. Plan conflicts requeue the eval instead of failing it silently ([late-binding-plan-apply.md](late-binding-plan-apply.md)), so one `job run` needs no poll-and-resubmit loop on the agent side. |
| Closing the loop | `job run --wait` / `job wait`, rollout events, `legion state query`, `job logs` |

## 1. MCP server (`legion mcp serve`)

[MCP](https://modelcontextprotocol.io) is the standard way to expose tools to LLM
agents. Legion ships its own MCP server, which speaks JSON-RPC 2.0 over stdio.

```bash
# Read-only tools only (default — safe for exploration):
legion mcp serve

# Also expose mutating tools (run / stop / destroy):
legion mcp serve --allow-write
```

To configure it in an MCP client (for example Claude Desktop or Claude Code), add
a stdio server with command `legion` and args `["mcp", "serve"]`. The server
reads `LEGION_ADDR` and the TLS environment variables (see
[Connecting](#connecting)).

### Tools

Read-only tools are always available. Mutating tools require `--allow-write`.
Even with `--allow-write`, `legion_state_query` stays read-only.

| Tool | Mutates | Purpose |
| --- | --- | --- |
| `legion_capabilities` | no | List the operation catalog. |
| `legion_job_list` | no | List jobs, optionally only those in one `status`. The reply's `statusCounts` gives per-status totals. |
| `legion_job_info` | no | Inspect one job. |
| `legion_job_spec` | no | Fetch a job's source submissions. |
| `legion_job_schema` | no | JSON Schema for jobspecs + task runtimes. |
| `legion_job_validate` | no | Validate a rendered jobspec (JSON). |
| `legion_rollout_list` | no | List rollouts. |
| `legion_rollout_status` | no | Rollout progress + allocation health. |
| `legion_instance_list` | no | List a job's stateful instances (slot + keyed). |
| `legion_instance_get` | no | Inspect one stateful instance, including a live unratified claim when one exists ([edge-boot.md](edge-boot.md)). |
| `legion_volume_list` | no | List volumes, the storage behind stateful instances. Filters by job/node/plugin/instance/state. |
| `legion_volume_get` | no | Inspect one volume: plugin, params, node pin, mount path, last driver error. |
| `legion_volume_drivers` | no | Volume plugins installed across the cluster and which nodes can use them. Use it to find out why a job was rejected for plugin X. |
| `legion_snapshot_list` | no | List instance [snapshots](suspend-and-resume.md), newest first. Filters by job, instance, and state. |
| `legion_snapshot_get` | no | Inspect one snapshot by id (or name plus job), including its manifest and, for a failed one, why. |
| `legion_node_list` | no | List worker nodes. Each carries `heartbeatSource` (see the freshness note below). |
| `legion_node_cadence` | no | Check whether the fleet is keeping up with its heartbeat cadence. Returns the freshness ratio, lost window, sustainable fleet size, and the `heartbeatSeconds` to set if it is falling behind ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)). |
| `legion_node_usage` | no | One node's memory and CPU: capacity, reservations, and measured usage per allocation (current and peak memory, CPU, idle seconds). Also the node's oversubscription ratio and memory pressure status, and each allocation's place in the pressure order (`pressureRank`, `pressureSteps`): which allocations a short node would stop first. Use it to compare reservations with real use before resizing a job ([resource-enforcement.md](resource-enforcement.md#memory-pressure)). |
| `legion_node_usage_list` | no | Memory and CPU for many nodes in one call (`ListNodeUsage`): per node, capacity with the oversubscription ratio, reservations, measured use, and memory pressure status, plus CPU `cores` and `usedMillicores` (filled even where the platform reports no clock speed). Per-allocation rows only with `include_allocations`. Empty `node_ids` means every node, up to 500. Use it to find tight or pressured nodes before drilling in with `legion_node_usage` ([resource-enforcement.md](resource-enforcement.md#measuring-usage)). |
| `legion_node_content` | no | What one node holds for content locality: the content it advertises to the manager's location index (digest, bytes, holder count, and the snapshot and part role it belongs to), whether its worker can publish and checkpoint, and its snapshots with `partsHere` of `partsTotal`. `indexReported: false` means no content report reached this manager. Use it to see why placement preferred a node, or whether a node is warm for a snapshot before a resume or fork ([artifacts.md](artifacts.md)). |
| `legion_keyring_status` | no | KV encryption at rest on the answering manager ([kv-encryption.md](kv-encryption.md)): `state` (`unlocked`, `locked`, `pending_upgrade`, …), the root key provider and location, each data key with how many values it protects, plaintext rows, rewrap progress, and a one-line `hint` with the next step when something needs doing. |
| `legion_keyring_rotate` | **yes** | Rotate the KV data key now. Values are re-encrypted in the background with no job restarts. Takes an optional `idempotency_key`. |
| `legion_node_claims` | no | A node's live provisional (unratified) allocations, meaning work that started but is not yet confirmed ([edge-boot.md](edge-boot.md)). Tells "still booting" apart from "already confirmed". |
| `legion_boot_ticket_list` | no | Outstanding boot tickets (metadata only. Token bytes are never stored or returned). |
| `legion_service_list` | no | List declared services with healthy/total endpoint counts, which is what a job can connect to. |
| `legion_service_resolve` | no | Resolve a service to its endpoints (address, port, allocation, node, health). |
| `legion_net_policy_list` | no | List stored network policies (selector, enforcement, rules) in merge order. |
| `legion_net_policy_show` | no | A task's effective network policy and its resolved network mode. The policy merges every matching stored policy with the task's own block, and each rule names its source. |
| `legion_net_policy_test` | no | Dry-run a verdict: may this task reach `to` or be reached from `from`? Returns allow/deny/defer with the deciding rule and reason. Sends nothing. |
| `legion_net_doctor` | no | Walk a task's network from policy to datapath and report where it breaks: the resolved policy, whether the mode enforces it, and, per allocation, whether the node's datapath is ready, enforcing the current policy, or fail-closed. Returns ok/warn/fail with fix hints. |
| `legion_net_handler_test` | no | Probe an external handler the way the datapath would use it. An authz webhook must answer `{verdict}` to a sample facts payload. A proxy must accept a PROXY v2 header. Sends one synthetic request and never touches the cluster. |
| `legion_service_edges` | no | The service graph: who calls whom through the datapath, with request rate, errors, denials, latency and bytes per (caller, service) over the window. Also lists declared-but-silent upstreams and whether each caller is on the service's allow list. |
| `legion_net_ca_show` | no | The mesh CA certificate (PEM, never the key) that non-Legion clients trust to reach a mesh-TLS port. |
| `legion_net_ca_rotate` | **yes** | Replace the mesh CA. Every node re-issues its certificate on its next heartbeat, and peers on different CAs refuse each other until then. |
| `legion_net_policy_apply` | **yes** | Create or replace a network policy object. `dry_run` validates and lists affected allocations without writing. |
| `legion_acl_whoami` | no | ACL status + caller identity (diagnose PermissionDenied). |
| `legion_events` | no | List scheduler events. Poll with `after_id` to tail. |
| `legion_job_logs` | no | Fetch captured task stdout/stderr for a job, including allocations removed in the last 24 hours (`removed`, `removalCause`). |
| `legion_state_query` | no | Read-only SQL against the state store. |
| `legion_state_schema` | no | Describe state store tables. |
| `legion_template_list` | no | List job templates (name, latest version, variable count). |
| `legion_template_get` | no | One template version: declared variables, source, version history. |
| `legion_template_render` | no | Dry-run a template: validate variables and return the rendered jobspec without submitting. |
| `legion_job_run` | **yes** | Submit/update a job from rendered JSON. |
| `legion_job_stop` | **yes** | Stop a job. |
| `legion_job_scale` | **yes** | Change a task's count without a new job version or rollout ([autoscaling.md](autoscaling.md)). Supports `idempotency_key`. |
| `legion_job_scale_status` | no | Per-task spec vs override vs effective counts. |
| `legion_job_destroy` | **yes** | Destroy a job. |
| `legion_template_run` | **yes** | Render a template with variables and submit the job (supports `idempotency_key`). |
| `legion_template_upsert` | **yes** | Store Nickel as the next immutable version of a template. |
| `legion_template_delete` | **yes** | Delete a template and all of its versions (running jobs are unaffected). |
| `legion_instance_create` | **yes** | Create a keyed-mode stateful instance (never-recycled id, supports `idempotency_key`). `from_snapshot` restores the new instance(s) from a snapshot of the job (an id or a name), on the snapshot's node, at most 64 per call. Needs `snapshot:read` too. |
| `legion_instance_destroy` | **yes** | Destroy a keyed instance, stop its allocation, and drop its storage (supports `idempotency_key`). |
| `legion_instance_suspend` | **yes** | [Suspend](suspend-and-resume.md) a keyed instance: its node checkpoints it (`checkpoint = "memory"` or `"disk"`, default the best it can take), stops it, and frees its reservation. Returns at once, so poll `legion_instance_get` until `state` is `suspended`, or until `pendingOp` clears with the reason in `detail` (a failed capture leaves the instance running). A memory checkpoint needs `snapshot:write`. Supports `idempotency_key`. |
| `legion_instance_start` | **yes** | Start a stopped keyed instance, or resume a suspended one from its checkpoint (`cold = true` boots fresh instead). Returns at once with `resumingFrom`. Poll `legion_instance_get` until `state` is `active` with no `pendingOp`, or read `instance.resumed` / `instance.restore_failed`. Then check `coldBoot`: when set, the restore failed and the instance booted fresh on its volume (`onRestoreFailure = "cold"`), so its running processes are gone. Supports `idempotency_key`. |
| `legion_instance_fork` | **yes** | [Fork](suspend-and-resume.md#fork) a running keyed instance into `count` new instances (`<id>-f<n>`), each with its own cloned volume and identity, on the origin's node, or with `spread: true` each on its own node from a published checkpoint. The origin keeps running. Returns the fork ids at once. Poll `legion_snapshot_get` on the returned `snapshot` until `local` (forks being placed, or `published` for a spread fork) or `failed` (origin unaffected, forks terminated). Needs `job:write` and `snapshot:write`. Supports `idempotency_key`, and a retry returns the same forks. |
| `legion_instance_replace` | **yes** | Force-replace an instance's allocation that is stuck on a LOST node. Skips the `onLost.replaceAfter` loss-confirmation gate, so it risks split-brain if the node is only partitioned. Supports `idempotency_key`. |
| `legion_volume_delete` | **yes** | Delete a volume and reclaim its storage. DESTROYS DATA. `force = true` tombstones without driver confirmation, for a node that is gone for good. Supports `idempotency_key`. |
| `legion_snapshot_create` | **yes** | Snapshot a running instance: `kind = "disk"` (default) or `"memory"` (sandbox tasks with a `snapshots` block). `publish = true` uploads it to the node's `contentStore` once captured, so it restores on any compatible node. Returns in state `capturing`, so poll `legion_snapshot_get` until `local` (or `published`) or `failed`. A memory snapshot contains the workload's in-memory secrets. Supports `idempotency_key`. |
| `legion_snapshot_delete` | **yes** | Delete a snapshot. Its node reclaims the bytes on its next heartbeat. Supports `idempotency_key`. |
| `legion_snapshot_restore` | **yes** | Roll a keyed instance back to one of its own snapshots, in place. The outcome arrives as a `snapshot.restored` or `snapshot.restore_failed` event. Supports `idempotency_key`. |
| `legion_snapshot_alias_set` | **yes** | Point a snapshot name at a local or published manual snapshot of the job, creating or moving the name, so `legion_instance_create` with `from_snapshot` = the name picks up the new snapshot. Supports `idempotency_key`. |
| `legion_snapshot_warm` | **yes** | Pre-fetch a `published` snapshot's parts onto available nodes matching `constraints` (`key=value` node attributes) that could restore it, so a later resume there starts from local bytes. Best effort and idempotent. Only needs `snapshot:read`, but it starts downloads, so it is gated behind `--allow-write`. The reply lists the nodes asked, the ones already warm, and the incompatible ones with the reason. Supports `idempotency_key`. |
| `legion_boot_ticket_create` | **yes** | Mint a boot ticket: a short-lived capability that lets one edge node start one workload without a manager call ([edge-boot.md](edge-boot.md)). It can only be revoked by expiry, so prefer short TTLs. Supports `idempotency_key`. |
| `legion_action_list` | no | List the actions declared on a job's tasks, with their parameters. |
| `legion_exec_run` | **yes** | Run a one-shot command inside a running allocation; returns `exitCode`, `stdout`, `stderr`, `message`. Needs `exec:write`. |
| `legion_action_run` | **yes** | Run a declared action against a running allocation (the newest of the job when `allocation_id` is omitted); same result shape. |

Tool errors come back in-band (`result.isError = true` with a text payload),
following MCP convention. Transport and protocol errors use JSON-RPC `error`.

The transport sends one JSON message per line. In MCP mode Legion routes all
console logs to stderr, so stdout carries only the protocol stream.

### Reading a node's last heartbeat honestly

A node's `lastHeartbeat` is not always a live clock. An agent that treats it as
one will draw wrong conclusions. Heartbeats live in the memory of the manager
that owns node status, and only status transitions are replicated. So every
node carries `heartbeatSource`:

- `live`: the answering manager observed the beat itself. It is current.
- `checkpoint`: from a periodic replicated checkpoint, up to
  `liveness.checkpointInterval` (30s by default) behind.
- `""`: no beat observed, or checkpoints are disabled.

The node's `status` is accurate on every manager, so branch on `status`. Use
`lastHeartbeat` for display and ordering, and don't treat it as evidence that
a node just died. When many nodes look stale at once, call
`legion_node_cadence` before concluding anything. A low freshness ratio means
the control plane is behind, and the machines are probably still there. See
[node-liveness.md](node-liveness.md).

## 2. Structured output envelope

Every command accepts `--json` / `-j` and emits one of two shapes, `raft`,
`exec`, `action`, and `docs` included. The only exceptions are the commands
that are not a request with a result: the daemons (`start manager`,
`start worker`, `join`) and `mcp serve`, whose stdout is the JSON-RPC stream
itself. A unit test walks the whole command tree and fails the build when a
command lacks the flag (`every_command_accepts_json` in
[`src/cmd/mod.rs`](../src/cmd/mod.rs)).

```json
{ "ok": true,  "data": <command-specific value> }
{ "ok": false, "error": { "code": "...", "message": "...", "hint": "...", "retryable": true|false } }
```

An agent checks `ok` once and never has to scrape prose. The error `code`
mirrors the gRPC status code (`NotFound`, `Unavailable`, `InvalidArgument`, …).
`retryable` tells the agent whether retrying the same request may succeed. It is
true for `Unavailable`, `DeadlineExceeded`, `Aborted`, and `ResourceExhausted`.
Commands exit non-zero on failure.

The `message` carries the manager's own reason. A request the cluster's state
refuses is `FailedPrecondition` with that reason, for example `job destroy` on
a running job: `Job web must be stopped before it can be destroyed (it is
running); run legion job stop web first`. An unexpected server failure is
`Internal` and still names its cause. An error a worker returned (exec, logs)
keeps the worker's code and message, and an unreachable worker is
`Unavailable` naming the node, its address, and the port. `hint` is the next
step: the server's own `Hint:` line when it sent one, else a per-code default
(`legion acl whoami` for `PermissionDenied`, `legion login` for
`Unauthenticated`, LEGION_ADDR for a manager that cannot be reached). Human
output prints the same message and hint, ending in `[code=FailedPrecondition]`.

A lookup that finds nothing is a failure too: `legion kv get` on a missing key
returns `NotFound` and exits 1 rather than `{ "ok": true, "data": null }`, so
`legion kv get k || legion kv put k v` works.

**Every key is camelCase**, in every command's `data` and in the MCP tools'
results alike: `currentAllocId`, `heartbeatSource`, `statusCounts`,
`instanceIds`, `exitCode`. The API messages and the CLI's own envelopes use
the same rule. Map keys that are data, such as node attributes or environment
variable names, are printed as they are. MCP tool *arguments* keep their
snake_case names (`allocation_id`, `idempotency_key`).

`legion exec … --json -- <cmd>` (and `legion action run … --json`) collects a
one-shot command's output into `{ allocationId, exitCode, stdout, stderr,
message }`. The process still exits with the remote exit code, so a non-zero
command means `ok: true` with a non-zero `exitCode` and a non-zero exit.
`--json` cannot be combined with `--tty` or `--stdin`.

Implemented in [`src/cmd/output.rs`](../src/cmd/output.rs).

## 3. Capability discovery (`legion capabilities`)

```bash
legion capabilities --json
```

Returns the catalog of agent-callable operations, each marked with `mutates`
and `readOnlySafe`. The CLI and the MCP `tools/list` share this one catalog.
Keep it in sync when you add operations
([`src/cmd/capabilities.rs`](../src/cmd/capabilities.rs)).

## 4. Authoring jobs: schema + validate

Agents write jobspecs far more reliably against a schema than against free-form
Nickel.

```bash
# Get the jobspec JSON Schema, including the live per-runtime task config schemas:
legion job schema --json

# Render and deeply validate a jobspec WITHOUT submitting it:
legion job validate ./configs/jobs/demo.ncl --json
```

`job validate` runs the same validation the manager applies on submit
(`validate_job_config`), so an agent can fix mistakes before touching the
cluster. The schema includes the built-in task-runtime config schemas (`exec`,
`container`, …) under `taskRuntimes`, derived from the typed Rust structs.

**Write the spec as `.json`, not `.ncl`.** The JSON path exists for
machine-generated specs like these. `legion job schema` describes the JSON, an
agent already emits JSON, and a `.json` file is accepted anywhere a `.ncl` one
is. Going through Nickel means generating a second syntax whose records use
`=`. A JSON object handed to the Nickel parser fails with
`invalid contract expression`, which is a hard error to self-correct from.
Nickel is for the humans who read the spec later. If the job needs to be both
generated and readable, emit a `.json` data file and import it from a short
hand-written `.ncl`. See
[Configuration Language](configuration-language.md#json-for-machine-emitted-config).

Runtime configs are checked against their typed structs as well as their JSON
schema. That catches mistakes an agent could not otherwise see, such as an
unknown capability name in `exec`'s `hardening` block, a non-octal `umask`, or
both `oom_adj` and `oom_score_adj` set. These come back from `job validate`
instead of from a task that fails to start on some node ten seconds later.

### Job templates: authoring by parameter instead of by spec

When a cluster publishes [job templates](job-templates.md), an agent doesn't
need to write a jobspec at all. It discovers the declared variables and
supplies values:

```bash
legion template list --json
legion template get render --json      # declared variables, types, defaults, options
legion template render render --var scene=demo --json   # dry run: the exact spec that would be submitted
legion template run render --var scene=demo --idempotency-key run-1 --wait --json
```

The manager type-checks every value against its declaration and rejects
unknown keys. A mistyped variable fails with a specific message
(`variable 'frames': expected a number, got "many"`) instead of producing a
subtly wrong job. Use `template render` to self-correct: it is read-only and
returns what `template run` would submit.

## 4a. Wiring workloads together (service discovery + connect)

An agent building a multi-task system should never have to invent, inject, or
reconcile addresses. Declare the producer's port once and have the consumer name
the service ([Service Discovery](service-discovery.md)):

```nickel
# producer
service = { port = 8080, allow = ["api"] },      # allow is optional; omit = any job
# consumer
connect = { upstreams = [{ service = "db", balance = "least_conn" }] },
```

The consumer's task then dials `$LEGION_UPSTREAM_DB` (`127.0.0.1:<port>`), and
the worker load-balances that connection across `db`'s healthy endpoints. Two
properties matter for agents:

- **Order doesn't matter.** An upstream that names a service that doesn't exist
  yet is not a validation error. An agent can submit producer and consumer in
  either order, or in separate jobs, without a two-phase dance. The upstream
  has no endpoints until the service appears.
- **You can self-correct before mutating.** `legion job validate <spec>` runs
  the same connect validation the manager applies on submit. It catches an
  unknown `balance` strategy (the error lists the valid names), a duplicate
  upstream service, a `localPort` that collides with a declared static port,
  and an empty `service.allow` entry. Validate, fix, then submit.

To observe the result (all read-only, all `-j`, all on the standard envelope):

```bash
legion service list -j                # every declared service + healthy/total counts
legion service resolve web -j         # healthy endpoints
legion service resolve web --all -j   # include unhealthy, with per-endpoint status
legion service resolve web --one      # one random address:port, no jq needed
legion service doctor web -j          # layered diagnosis: checks[] + verdict + hints
```

`doctor -j` returns every check as data (`checks[]` with
`layer`/`status`/`summary`/`details`, plus `ok` and the resolved `endpoints`).
An agent can branch on the first failing layer instead of parsing prose. It
also reports the service's `allow` list, which lets an agent tell "no healthy
endpoints" apart from "this consumer is not permitted". That list comes back on
`ListServices` (`ServiceSummary.allow`) instead of from the state store, so the
whole report needs only `job:read`.

`service list -j` and `service resolve -j` used to print a bare JSON array.
They now use the envelope like everything else, so the array lives under
`.data`. Scripts pinned to `jq '.[0]'` need `jq '.data[0]'`, or `--one`.

Over MCP the same catalog is `legion_service_list` and `legion_service_resolve`
(both read-only, always listed). Over gRPC it is `ListServices` /
`ResolveService`.

> **Scope note for agents reasoning about isolation:** `service.allow` is
> enforced on the connect path only. The manager leaves denied endpoints out of
> the consumer's feed. It is not cryptographic identity or a network policy, so
> don't treat it as a security boundary. See
> [Service Discovery](service-discovery.md#intentions-serviceallow).

## 4b. Network policy: asking before dialing

When a policy denies a connection, it is closed before it is dialed, and the
client sees a reset or a hang. An agent shouldn't have to infer policy from
timeouts. Two read-only surfaces answer the question directly
([Network Handlers](network-handlers.md)):

```bash
legion net policy show agent -j                         # the merged policy, rules with sources
legion net policy test agent --to https://api.github.com/user -j
# → {"ok":true,"data":{"verdict":"deny","source":"policy/sandbox-egress#3","reason":"…"}}
```

Over MCP these are `legion_net_policy_show` and `legion_net_policy_test`.
When a request is denied in HTTP mode, the response says why: a `403` with
`Legion-Denied-By: <policy>/<rule>` and a JSON body naming the rule and reason.
An agent inside a sandbox can then change its approach instead of retrying.

`legion net policy apply` (and `legion_net_policy_apply`) mutates and is gated
behind `--allow-write`. `--dry-run` / `dry_run` validates the object and lists
the running allocations whose effective policy would change, without writing.

## 5. Idempotency keys

Mutating job, instance, volume, and snapshot operations accept an optional
idempotency key, so an agent can retry after a timeout without applying the
change twice:

```bash
legion job run demo.ncl --idempotency-key deploy-2026-05-29-001
legion job stop demo        --idempotency-key stop-demo-001
legion job destroy demo     --idempotency-key destroy-demo-001
legion instance create sandbox-demo --id user-42 --idempotency-key sbx-user-42
legion instance destroy user-42     --idempotency-key destroy-user-42
legion snapshot create user-42 --name before-refactor --idempotency-key snap-user-42-001
```

The manager records the first outcome for a key and replays it for any later
request with the same key. The reply's `deduplicated` field reports whether a
result was replayed. Results are stored in the `idempotency_record` table
([migration](../src/storage/migrations.rs), store methods in
[`src/storage/store.rs`](../src/storage/store.rs)). An empty or absent key
disables deduplication.

A key is scoped to one operation and one caller:

- The same key on a different operation is a new request. Reusing a
  `job stop` key on `job destroy` runs the destroy.
- The same key from a different caller is a new request. The caller is the
  ACL token (by accessor ID), or the principal for JWT and anonymous callers.
  One caller can never read another's recorded reply. With ACLs disabled,
  every caller is the same caller.
- The key does not cover the request body. Retrying a key with different
  arguments (another job name, say) replays the first reply. Use a fresh key
  per intended change.

Records are kept for 24 hours, then pruned by the manager (the control-lane
leader in raft mode) within the following hour. A retry after that runs the
operation again. Keys recorded by a manager older than this scoping no longer
match after an upgrade, so a retry that spans the upgrade runs again.

Concurrent requests with the same key are a different case from a retry. Both
miss the recorded result, so the record decides which one wins. `job run`
handles this: one caller is told it was first, and the other gets the winner's
message with `deduplicated: true`. Known limitation: the other mutating RPCs
(`job stop`, `job destroy`, `job scale`, the `instance` verbs) still report
`deduplicated: false` to both sides of such a race. Sequential retries, the main
use for the key, work correctly on all of them.

## 6. Closing the loop: waiting for convergence

```bash
# Submit and block until the resulting rollout converges:
legion job run demo.ncl --wait --timeout 180 --json

# Or wait on an existing job's latest rollout:
legion job wait demo --json
```

`wait` polls the job's most recent rollout until it reaches a settled state
(`completed`, `failed`, `cancelled`, `completed_incomplete`, or `paused`) or the
timeout runs out. The result reports `converged`, `success`, `status`, and the
tracked `rolloutId`. A wait that doesn't converge exits non-zero.

> **Scope note:** `wait` tracks rollouts, which signal convergence for updates
> to existing jobs. A brand-new job with no prior version has no rollout to
> track and reports `status = "no_rollout"`. For deeper post-deploy inspection,
> use rollout events and the state store (below).

Suspend and resume wait the same way: `legion instance suspend … --wait`
blocks until the instance is `suspended` (non-zero with `SuspendFailed` and
the node's reason when the capture failed, in which case the instance keeps
running), and `legion instance start … --wait` blocks until it is `active`
and the restore has reported (non-zero with `RestoreFailed` when
`onRestoreFailure = "fail"` left it suspended). Under the default `"cold"`, a
restore that fails still ends `active`, so check the envelope's `coldBoot`:
when it is set, the instance booted fresh on its volume and lost its running
processes.

An instance can also become `suspended` without any call: through its task's
[idle policy](suspend-and-resume.md#idle-policy), or the node's `suspend`
memory pressure step. `legion_instance_get` says which in `suspendReason`
(`manual`, `idle`, `pressure`), and while the idle policy is watching a
running instance it reports `idleSince` and `idleAfterSeconds`, so an
agent can see a suspend coming and keep a workspace awake by using it.

Fork waits on its checkpoint: `legion instance fork … --wait` blocks until
the origin's capture lands and the forks are released for placement, and
fills in `pauseMs`. With `--spread` it also waits for the publish, through
the manager's retries. It exits non-zero with `ForkFailed` and the node's
reason when the capture (or a spread fork's publish) failed, in which case
the origin keeps running and the forks are terminated. The envelope's
`durability` says where the forks run: `local` on the origin's node, `store`
each on its own node.

Snapshots have their own waits. `legion snapshot create … --wait` blocks
until the capture is `local` or `failed` (with `--publish`, until it is
`published` or the upload failed, which exits `PublishFailed`), and `legion
snapshot restore … --wait` blocks until the node reports the restore's
outcome. Both exit non-zero on failure, with the node's reason in the error
envelope ([suspend-and-resume.md](suspend-and-resume.md)). A suspended
instance's `checkpointState` (`local` or `published`) in
`legion_instance_get` tells an agent whether a resume can leave the node.

Other signals an agent can use to close the loop:

- `legion events list --json` / `legion events follow --json`: the scheduler
  event stream (see below).
- `legion job rollout events --id <ID> --json`: the rollout's event history.
- `legion job rollout status --id <ID> --json`: per-allocation health, including
  per-artifact staging states (`artifactStates`) for tasks that declare
  [artifacts](artifacts.md).
- `legion job logs <NAME> --json`: captured task stdout/stderr (see below).
- `legion state query "<SQL>" --json`: inspect `task_status`, `allocation_health`,
  `eval`, and so on (read-only by default).

### The scheduler event stream

The manager records an event for everything the scheduler does: evals
enqueued, completed, or failed, allocations placed, removed, or failed, node and
job lifecycle transitions, and rollout activity. It exposes these events three
ways (see [Event Stream](event-stream.md)):

```bash
# A window of recent history (oldest first), with filters:
legion events list --kind eval --job demo --level warn --json

# Live tail over the StreamEvents gRPC stream; in JSON mode each line is a
# complete { "ok": true, "data": <event> } envelope (NDJSON):
legion events follow --job demo --json
```

Every event has a monotonically increasing `id`, so a polling agent never misses
or double-counts an event. Call `legion events list --json` once, remember
`latestId`, then poll with `--after-id <latestId>`. The `legion_events` MCP tool
takes the same `after_id` cursor. After a dropped `follow`, resume with
`--after-id <last seen id>`, and the manager replays the gap before tailing.

## 6a. Task log retrieval (`legion job logs`)

Rollout events and the state store tell an agent that a task is unhealthy.
Task logs tell it why. Workers capture each task's stdout and stderr to
per-allocation files, and the manager proxies reads back to the agent:

```bash
# All allocations of a job, both streams (first 256 KiB per stream by default;
# add --tail for the most recent output, or --follow to live-tail):
legion job logs demo --json

# A single allocation, stderr only, last 4 KiB:
legion job logs demo --allocation 42 --stream stderr --max-bytes 4096 --tail
```

Flags:

- `--allocation <ID>` targets one allocation instead of the whole job.
- `--stream stdout|stderr|all` picks the stream (default `all`).
- `--max-bytes <N>` caps each stream (`0` uses the worker's built-in 256 KiB cap).
- `--tail` returns the trailing bytes instead of the leading bytes.
- `--follow`/`-f` live-tails until interrupted.

The MCP tool is `legion_job_logs`, and the web UI has a live-tailing **Logs**
tab on jobs and tasks.

Workers rotate and prune these files (size cap + retention), so a chatty task
can't fill a node's disk. The files are also ready to ship to an external
collector. See [Logging](logging.md) for capture, the `LogSink` seam, rotation
config, tailing, external shipping, and exporting Legion's own traces over
OpenTelemetry.

Each returned allocation carries its `allocationId`, `nodeId`, `taskName`,
`status`, the captured `streams` (with `sizeBytes` and a `truncated` flag),
and a per-allocation `error` string. One unreachable node returns an error for
its allocations without failing the whole request.

Removed allocations stay readable for 24 hours, which is when an agent most
needs them: the task failed and was rescheduled, the job was stopped, or it
was destroyed. A job with no live allocation returns its most recently
removed ones (newest first, at most 10), `--include-removed` (MCP:
`include_removed`) adds them to a running job's, and `--allocation <ID>`
reaches any one of them. Each carries `removed: true`, `removalCause` (`stop`,
`update`, `reschedule`, `node_lost`, …), and `removedAtUnix`. See
[Logging](logging.md#logs-of-removed-allocations).

How it is wired (read-only, safe for agents):

1. Workers redirect task stdout/stderr into `stdout.log` / `stderr.log` under a
   per-allocation log directory passed through the `TaskPlugin` runtime
   (`TaskInstance.log_dir`). Built-in `exec` and `container` tasks capture
   automatically.
2. `Worker.GetTaskLogs` reads those files on the node (with the byte cap / tail).
3. `Manager.GetAllocationLogs` resolves a job (or allocation id) to its
   allocations, finds each host node, and proxies the read to that worker.
4. `legion job logs <name>` and the `legion_job_logs` MCP tool expose it.

## 6b. Starting work at the edge (`legion sandbox start`)

An agent that owns its own routing can start a workload directly on a node,
without a manager on the request path, using a boot ticket
([edge-boot.md](edge-boot.md)):

```bash
# Ahead of time (needs boot:write), from wherever the agent's control plane runs:
legion boot-ticket create agent-sandboxes \
    --task workspace --id user-42 --pool edge-eu --gate boot --ttl 5m -j

# Once: the cluster CA the node's certificate must chain to.
legion net ca show > ca.pem

# On the request path, against the node itself (mTLS; no manager call):
legion sandbox start agent-sandboxes \
    --id user-42 --node https://10.0.1.7:4848 --ca-cert ca.pem --ticket "$TICKET" -j
```

The command streams one frame per transition (`admitted`, `staged`/`booted`,
then `ratified` or `refused`). Two properties matter to an agent:

- **A `booted` frame is not a promise.** The workload exists, but its placement
  isn't durable yet. A `refused` frame may still arrive, and the node then
  kills the workload. Build on `ratified`, which carries the allocation id.
- **`--dry-run` checks the ticket and the node's admission without holding or
  booting anything.** Use it to self-correct before mutating a cluster, as with
  `job validate` and `job plan`.

`legion instance get` reports a sandbox as `provisional` until it is ratified.
`legion node claims <NODE>` lists everything a node has started but not yet
had confirmed.

## 7. State store introspection + saved query library

The SQL state store is the agent's main introspection tool. It is read-only by
default. Writes require the `--allow-write` flag on `legion state query`
(the `allow_write` field on the RPC).

```bash
legion state schema --json                 # discover tables/columns first
legion state query "SELECT * FROM node" -j # read-only by default
legion state seed-queries                  # install the curated agent query library
```

`seed-queries` installs a curated set of named introspection queries (prefixed
`agent:`), such as `agent:unhealthy-allocations`, `agent:active-rollouts`, and
`agent:pending-evals`. They then show up in the saved-query APIs and the UI State
Explorer. See `AGENT_SAVED_QUERIES` in [`src/cmd/state.rs`](../src/cmd/state.rs).

## Connecting

All client commands and the MCP server read:

- `LEGION_ADDR`: manager address (default `http://127.0.0.1:4747`).
- `LEGION_TLS_CA_CERT`: CA certificate path for `https`/`grpcs` addresses.
- `LEGION_TOKEN`: ACL token sent as a bearer credential when the cluster has
  ACLs enabled. Falls back to the `legion login` token file, `~/.legion/token`.
  On a `PermissionDenied`, call `legion acl whoami` (or the `legion_acl_whoami`
  MCP tool) to see the caller's policies. See [Auth & ACLs](auth-and-acls.md).

## Recommended agent workflow

1. **Discover**: `legion capabilities --json` (or MCP `tools/list`).
2. **Observe (read-only)**: `legion state schema`, then `legion state query`,
   `legion job list`, `legion node list`.
3. **Author + check**: build a jobspec against `legion job schema`, then run
   `legion job validate` on it. If a template covers the task, use
   `legion template get <name>` and fill in its declared variables instead.
4. **Preview**: run `legion job plan` to see placements (dry-run), or
   `legion template render` to see the spec a template would submit. The
   `job plan --json` envelope also carries:
   - `indexUsage`: per task, which scheduling index generated the candidates,
     or why none did ([scheduling-indexes.md](scheduling-indexes.md)).
   - `preemptions`: every allocation the submit would displace, with its job,
     task, band, and grace window
     ([preemption-and-eviction.md](preemption-and-eviction.md)).

   `preemptionMode` tells you whether that list is empty because nothing would
   be displaced or because preemption is off. Ask for confirmation when
   `preemptions` is non-empty, since it is the one part of a plan that costs
   another workload something.
5. **Apply with a retry-safe key**: `legion job run … --idempotency-key … --wait`
   (or `legion template run … --idempotency-key … --wait`).
6. **Verify**: inspect the `wait` result, `rollout events`, and `state query`.

Prefer read-only operations. Treat anything with `mutates = true` or
`readOnlySafe = false` as requiring confirmation.

**Reacting to being stopped.** Legion tells an agent-run task when it is about
to shut the task down. `LEGION_STOP_NOTICE_PATH` names a JSON file written just
before the stop signal. It carries the cause (`preempted`, `drain`, `stop`, …),
the SIGKILL deadline, the grace window in seconds, and, for preemption, the job
that took the capacity. That is enough for the task to checkpoint, hand off, or
exit cleanly without guessing from a bare SIGTERM. See
[graceful-stop.md](graceful-stop.md).

## Status / roadmap

Implemented: MCP server, structured envelope + typed errors, capabilities catalog,
`job schema`/`job validate`, idempotency keys, `--wait`/`job wait`, saved-query
library, the scheduler event stream (`events list`/`events follow`,
`legion_events`), task log retrieval (`legion job logs` /
`legion_job_logs`), and the service catalog (`legion service
list|resolve|doctor`, `legion_service_list` / `legion_service_resolve`) with the
declarative wiring described in [§4a](#4a-wiring-workloads-together-service-discovery--connect).
For how task logs are captured and served, see
[§6a](#6a-task-log-retrieval-legion-job-logs).
