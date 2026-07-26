# Agent Readiness

Legion is designed to be driven not just by humans but by autonomous AI agents.
This document describes the agent-facing surfaces, the conventions they rely on,
and the recommended workflow for operating a cluster safely from an agent.

If you are an agent operating Legion, start here, then call
`legion capabilities --json` (or the `legion_capabilities` MCP tool) to discover
the live operation catalog.

## Why these features exist

An agent-ready tool must be **discoverable** (the agent can enumerate what it can
do), produce **structured I/O** (one parseable shape for success and failure),
be **safe under retries** (idempotency, read/write separation, dry-runs), and
**close the loop** (the agent can observe the effect of an action). The features
below map directly onto those pillars.

| Pillar | Legion feature |
| --- | --- |
| Discoverability | `legion capabilities`, MCP `tools/list`, `legion job schema`, `GetStateStoreSchema` |
| Structured I/O | Universal `--json` envelope, typed errors with `code`/`hint`/`retryable` |
| Safety under retries | Idempotency keys, read-only-by-default SQL, `job plan`, `job validate` |
| Closing the loop | `job run --wait` / `job wait`, rollout events, `legion state query`, `job logs` |

## 1. MCP server (`legion mcp serve`)

[MCP](https://modelcontextprotocol.io) is the standard way to expose tools to LLM
agents. Legion ships a first-party MCP server that speaks JSON-RPC 2.0 over stdio.

```bash
# Read-only tools only (default — safe for exploration):
legion mcp serve

# Also expose mutating tools (run / stop / destroy):
legion mcp serve --allow-write
```

Configure it in an MCP client (e.g. Claude Desktop / Claude Code) as a stdio
server with command `legion` and args `["mcp", "serve"]`. The server honours
`LEGION_ADDR` and the TLS environment variables (see
[Connecting](#connecting)).

### Tools

Read-only tools are always available; mutating tools require `--allow-write`.
Even with `--allow-write`, `legion_state_query` is forced to read-only.

| Tool | Mutates | Purpose |
| --- | --- | --- |
| `legion_capabilities` | no | List the operation catalog. |
| `legion_job_list` | no | List jobs. |
| `legion_job_info` | no | Inspect one job. |
| `legion_job_spec` | no | Fetch a job's source submissions. |
| `legion_job_schema` | no | JSON Schema for jobspecs + task kinds. |
| `legion_job_validate` | no | Validate a rendered jobspec (JSON). |
| `legion_rollout_list` | no | List rollouts. |
| `legion_rollout_status` | no | Rollout progress + allocation health. |
| `legion_instance_list` | no | List a job's stateful instances (slot + keyed). |
| `legion_instance_get` | no | Inspect one stateful instance. |
| `legion_node_list` | no | List worker nodes. |
| `legion_service_list` | no | List declared services with healthy/total endpoint counts — what a job can connect to. |
| `legion_service_resolve` | no | Resolve a service to its endpoints (address, port, allocation, node, health). |
| `legion_acl_whoami` | no | ACL status + caller identity (diagnose PermissionDenied). |
| `legion_events` | no | List scheduler events; poll with `after_id` to tail. |
| `legion_job_logs` | no | Fetch captured task stdout/stderr for a job. |
| `legion_state_query` | no | Read-only SQL against the state store. |
| `legion_state_schema` | no | Describe state store tables. |
| `legion_job_run` | **yes** | Submit/update a job from rendered JSON. |
| `legion_job_stop` | **yes** | Stop a job. |
| `legion_job_destroy` | **yes** | Destroy a job. |
| `legion_instance_create` | **yes** | Create a keyed-mode stateful instance (never-recycled id; supports `idempotency_key`). |
| `legion_instance_destroy` | **yes** | Destroy a keyed instance, stop its allocation, drop its storage (supports `idempotency_key`). |
| `legion_instance_replace` | **yes** | Force-replace an instance's allocation stuck on a LOST node (skips the `onLost.replaceAfter` loss-confirmation gate; split-brain risk if the node is only partitioned; supports `idempotency_key`). |

Tool errors are returned in-band (`result.isError = true` with a text payload),
following MCP convention; transport/protocol errors use JSON-RPC `error`.

The transport contract is one JSON message per line. In MCP mode Legion routes
all console logs to **stderr** so stdout stays a clean protocol stream.

## 2. Structured output envelope

The agent-facing commands (`job`, `instance`, `state`, `node`, `kv`,
`capabilities`) accept
`--json` / `-j` and emit exactly one of two shapes (bootstrap/cluster-admin
commands such as `start`, `join`, and `raft` are operator-oriented and are not
part of the agent surface):

```json
{ "ok": true,  "data": <command-specific value> }
{ "ok": false, "error": { "code": "...", "message": "...", "hint": "...", "retryable": true|false } }
```

An agent branches on `ok` once and never scrapes prose. The error `code` mirrors
the gRPC status code (`NotFound`, `Unavailable`, `InvalidArgument`, …); `retryable`
tells the agent whether retrying the *same* request may succeed (true for
`Unavailable`, `DeadlineExceeded`, `Aborted`, `ResourceExhausted`). Commands exit
non-zero on failure.

Implemented in [`src/cmd/output.rs`](../src/cmd/output.rs).

## 3. Capability discovery (`legion capabilities`)

```bash
legion capabilities --json
```

Returns the catalog of agent-callable operations, each annotated with `mutates`
and `readOnlySafe`. This catalog is the single source of truth shared by the CLI
and the MCP `tools/list`; keep it in sync when adding operations
([`src/cmd/capabilities.rs`](../src/cmd/capabilities.rs)).

## 4. Authoring jobs: schema + validate

Agents author far more reliably against a schema than against free-form Jsonnet.

```bash
# Get the jobspec JSON Schema, including the live per-kind task config schemas:
legion job schema --json

# Render and deeply validate a jobspec WITHOUT submitting it:
legion job validate ./configs/jobs/demo.jsonnet --json
```

`job validate` runs the exact validation the manager applies on submit
(`validate_job_config`), so an agent can self-correct before touching the cluster.
The schema embeds the built-in task-kind config schemas (`exec`, `container`, …)
under `taskKinds`, derived from the typed Rust structs.

## 4a. Wiring workloads together (service discovery + connect)

An agent authoring a multi-task system should never have to invent, inject, or
reconcile addresses. Declare the producer's port once and let the consumer name
the service ([Service Discovery](service-discovery.md)):

```jsonnet
// producer
service: { port: 8080, allow: ["api"] },      // allow is optional; omit = any job
// consumer
connect: { upstreams: [{ service: "db", balance: "least_conn" }] },
```

The consumer's task then dials `$LEGION_UPSTREAM_DB` (`127.0.0.1:<port>`) and
the worker load-balances that connection across `db`'s healthy endpoints. Two
properties matter for agents specifically:

- **Order-independent.** An upstream naming a service that does not exist yet is
  *not* a validation error, so an agent may submit producer and consumer in
  either order (or in separate jobs) without a two-phase dance. An unresolvable
  upstream simply has no endpoints until the service appears.
- **Self-correctable before mutating.** `legion job validate <spec>` runs the
  same connect validation the manager applies on submit — unknown `balance`
  strategy (the error lists the valid names), duplicate upstream service,
  `localPort` colliding with a declared static port, empty `service.allow`
  entry. Validate, fix, then submit.

To observe the result — all read-only, all `-j`, all on the standard envelope:

```bash
legion service list -j                # every declared service + healthy/total counts
legion service resolve web -j         # healthy endpoints
legion service resolve web --all -j   # include unhealthy, with per-endpoint status
legion service resolve web --one      # one random address:port, no jq needed
legion service doctor web -j          # layered diagnosis: checks[] + verdict + hints
```

`doctor -j` returns the whole ladder as data (`checks[]` with
`layer`/`status`/`summary`/`details`, plus `ok` and the resolved `endpoints`),
so an agent can branch on the first failing layer instead of parsing prose. It
also reports the service's `allow` list, which is how an agent tells "no healthy
endpoints" apart from "this consumer is not permitted" — and because that list
comes back on `ListServices` (`ServiceSummary.allow`) rather than out of the
state store, the whole report needs only `job:read`.

`service list -j` and `service resolve -j` used to print a bare JSON array;
they now use the envelope like everything else, so the array lives under
`.data`. Scripts pinned to `jq '.[0]'` need `jq '.data[0]'`, or `--one`.

Over MCP the same catalog is `legion_service_list` and `legion_service_resolve`
(both read-only, always listed); over gRPC it is `ListServices` /
`ResolveService`.

> **Scope note for agents reasoning about isolation:** `service.allow` is
> enforced on the connect path only — the manager omits denied endpoints from
> the consumer's feed. It is not cryptographic identity and not a network
> policy; do not treat it as a security boundary. See
> [Service Discovery](service-discovery.md#intentions-serviceallow).

## 5. Idempotency keys

Mutating job operations accept an optional idempotency key so an agent can retry
after a timeout without risking a double-apply:

```bash
legion job run demo.jsonnet --idempotency-key deploy-2026-05-29-001
legion job stop demo        --idempotency-key stop-demo-001
legion job destroy demo     --idempotency-key destroy-demo-001
legion instance create sandbox-demo --id user-42 --idempotency-key sbx-user-42
legion instance destroy user-42     --idempotency-key destroy-user-42
```

The manager records the first outcome for a key and replays it for any later
request carrying the same key; the reply's `deduplicated` field reports whether a
result was replayed. Backed by the `idempotency_record` table
([migration](../src/storage/migrations.rs), store methods in
[`src/storage/store.rs`](../src/storage/store.rs)). An empty/absent key disables
deduplication.

## 6. Closing the loop: waiting for convergence

```bash
# Submit and block until the resulting rollout converges:
legion job run demo.jsonnet --wait --timeout 180 --json

# Or wait on an existing job's latest rollout:
legion job wait demo --json
```

`wait` polls the job's most recent rollout until it reaches a settled state
(`completed`, `failed`, `cancelled`, `completed_incomplete`, or `paused`) or the
timeout elapses. The result reports `converged`, `success`, `status`, and the
tracked `rolloutId`. A non-converged wait exits non-zero.

> **Scope note:** `wait` tracks **rollouts**, which are the convergence signal for
> updates to existing jobs. A brand-new job with no prior version has no rollout
> to track and reports `status: "no_rollout"`. For deeper post-deploy inspection,
> use rollout events and the state store (below).

Other loop-closing signals available to agents today:

- `legion events list --json` / `legion events follow --json` — the scheduler
  event stream (see below).
- `legion job rollout events --id <ID> --json` — the rollout's event history.
- `legion job rollout status --id <ID> --json` — per-allocation health, including
  per-artifact staging states (`artifact_states`) for tasks that declare
  [artifacts](artifacts.md).
- `legion job logs <NAME> --json` — captured task stdout/stderr (see below).
- `legion state query "<SQL>" --json` — inspect `task_status`, `allocation_health`,
  `eval`, etc. (read-only by default).

### The scheduler event stream

The manager journals an event for everything the scheduler does — evals
enqueued/completed/failed, allocations placed/removed/failed, node and job
lifecycle transitions, rollout activity — and exposes it three ways (see
[Event Stream](event-stream.md)):

```bash
# A window of recent history (oldest first), with filters:
legion events list --kind eval --job demo --level warn --json

# Live tail over the StreamEvents gRPC stream; in JSON mode each line is a
# complete { "ok": true, "data": <event> } envelope (NDJSON):
legion events follow --job demo --json
```

Every event has a monotonically increasing `id`, so polling agents never miss
or double-count: call `legion events list --json` once, remember `latestId`,
then poll with `--after-id <latestId>` (the `legion_events` MCP tool takes the
same `after_id` cursor). After a dropped `follow`, resume with
`--after-id <last seen id>` and the manager replays the gap before tailing.

## 6a. Task log retrieval (`legion job logs`)

Rollout events and the state store tell an agent *that* a task is unhealthy;
task logs tell it *why*. Workers capture each task's stdout and stderr to
per-allocation files, and the manager proxies reads back to the agent:

```bash
# All allocations of a job, both streams (first 256 KiB per stream by default;
# add --tail for the most recent output, or --follow to live-tail):
legion job logs demo --json

# A single allocation, stderr only, last 4 KiB:
legion job logs demo --allocation 42 --stream stderr --max-bytes 4096 --tail
```

Flags: `--allocation <ID>` targets one allocation instead of the whole job;
`--stream stdout|stderr|all` (default `all`); `--max-bytes <N>` caps each stream
(`0` uses the worker's built-in 256 KiB cap); `--tail` returns the trailing
bytes rather than the leading bytes; `--follow`/`-f` live-tails until interrupted.
The MCP tool is `legion_job_logs`, and the web UI exposes a live-tailing **Logs**
tab on jobs and tasks.

Workers rotate and prune these files (size cap + retention) so a chatty task
cannot fill a node's disk, and the files are ready to ship to an external
collector. See **[Logging](logging.md)** for the full picture: capture, the
`LogSink` seam, rotation config, tailing, external shipping, and exporting
Legion's own traces over OpenTelemetry.

Each returned allocation carries its `allocationId`, `nodeId`, `taskName`, the
captured `streams` (with `sizeBytes` and a `truncated` flag), and a per-allocation
`error` string — so one unreachable node degrades gracefully instead of failing
the whole request.

How it is wired (read-only, agent-safe):

1. Workers redirect task stdout/stderr into `stdout.log` / `stderr.log` under a
   per-allocation log directory threaded through the `TaskPlugin` runtime
   (`TaskRuntime.log_dir`); built-in `exec` and `container` tasks capture
   automatically.
2. `Worker.GetTaskLogs` reads those files on the node (with the byte cap / tail).
3. `Manager.GetAllocationLogs` resolves a job (or allocation id) to its
   allocations, locates each host node, and proxies the read to that worker.
4. `legion job logs <name>` and the `legion_job_logs` MCP tool surface it.

## 7. State store introspection + saved query library

The SQL state store is the agent's main introspection tool. It is read-only by
default and gated behind `allow_write`.

```bash
legion state schema --json                 # discover tables/columns first
legion state query "SELECT * FROM node" -j # read-only by default
legion state seed-queries                  # install the curated agent query library
```

`seed-queries` installs a curated set of named introspection queries (prefixed
`agent:`) such as `agent:unhealthy-allocations`, `agent:active-rollouts`, and
`agent:pending-evals`. They then appear via the saved-query APIs and the UI State
Explorer. See `AGENT_SAVED_QUERIES` in [`src/cmd/state.rs`](../src/cmd/state.rs).

## Connecting

All client commands and the MCP server honour:

- `LEGION_ADDR` — manager address (default `http://127.0.0.1:4747`).
- `LEGION_TLS_CA_CERT` — CA certificate path for `https`/`grpcs` addresses.
- `LEGION_TOKEN` — ACL token attached as a bearer credential when the cluster
  has ACLs enabled (falls back to the `legion login` token file,
  `~/.legion/token`). On a `PermissionDenied`, call `legion acl whoami`
  (or the `legion_acl_whoami` MCP tool) to see the caller's policies. See
  [Auth & ACLs](auth-and-acls.md).

## Recommended agent workflow

1. **Discover**: `legion capabilities --json` (or MCP `tools/list`).
2. **Observe (read-only)**: `legion state schema`, then `legion state query`,
   `legion job list`, `legion node list`.
3. **Author + check**: build a jobspec against `legion job schema`, then
   `legion job validate` it.
4. **Preview**: `legion job plan` to see placements (dry-run).
5. **Apply with a retry-safe key**: `legion job run … --idempotency-key … --wait`.
6. **Verify**: inspect the `wait` result, `rollout events`, and `state query`.

Prefer read-only operations; treat anything with `mutates: true` /
`readOnlySafe: false` as requiring confirmation.

## Status / roadmap

Implemented: MCP server, structured envelope + typed errors, capabilities catalog,
`job schema`/`job validate`, idempotency keys, `--wait`/`job wait`, saved-query
library, the scheduler event stream (`events list`/`events follow`,
`legion_events`), **task log retrieval** (`legion job logs` /
`legion_job_logs`), and the **service catalog** (`legion service
list|resolve|doctor`, `legion_service_list` / `legion_service_resolve`) with the
declarative wiring described in [§4a](#4a-wiring-workloads-together-service-discovery--connect).

The task-log path captures task stdout/stderr to per-allocation files via the
`TaskPlugin` runtime (`TaskRuntime.log_dir`), reads them on the node with
`Worker.GetTaskLogs`, and proxies through `Manager.GetAllocationLogs`. See
[§6a](#6a-task-log-retrieval-legion-job-logs).
