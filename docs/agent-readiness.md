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
| Closing the loop | `job run --wait` / `job wait`, rollout events, `legion state query` |

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
| `legion_node_list` | no | List worker nodes. |
| `legion_acl_whoami` | no | ACL status + caller identity (diagnose PermissionDenied). |
| `legion_state_query` | no | Read-only SQL against the state store. |
| `legion_state_schema` | no | Describe state store tables. |
| `legion_job_run` | **yes** | Submit/update a job from rendered JSON. |
| `legion_job_stop` | **yes** | Stop a job. |
| `legion_job_destroy` | **yes** | Destroy a job. |

Tool errors are returned in-band (`result.isError = true` with a text payload),
following MCP convention; transport/protocol errors use JSON-RPC `error`.

The transport contract is one JSON message per line. In MCP mode Legion routes
all console logs to **stderr** so stdout stays a clean protocol stream.

## 2. Structured output envelope

The agent-facing commands (`job`, `state`, `node`, `kv`, `capabilities`) accept
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

## 5. Idempotency keys

Mutating job operations accept an optional idempotency key so an agent can retry
after a timeout without risking a double-apply:

```bash
legion job run demo.jsonnet --idempotency-key deploy-2026-05-29-001
legion job stop demo        --idempotency-key stop-demo-001
legion job destroy demo     --idempotency-key destroy-demo-001
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

- `legion job rollout events --id <ID> --json` — the rollout's event history.
- `legion job rollout status --id <ID> --json` — per-allocation health.
- `legion state query "<SQL>" --json` — inspect `task_status`, `allocation_health`,
  `eval`, etc. (read-only by default).

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
library.

Recommended next step — **task log retrieval**. Today an agent diagnoses failures
via rollout events and the state store. Direct stdout/stderr retrieval would
tighten the loop further. The intended design:

1. Workers capture task stdout/stderr to per-allocation files (requires threading
   an allocation log directory through the `TaskPlugin` runtime, which affects the
   plugin ABI — hence staged separately).
2. A `Worker.GetTaskLogs` RPC reads those files on the node.
3. A `Manager.GetAllocationLogs` RPC locates the node and proxies the request.
4. `legion job logs <name>` and a `legion_job_logs` MCP tool surface it.

This was deliberately not bundled with the changes above to keep the plugin ABI
stable and avoid shipping cluster-coupled code that cannot be verified without a
live multi-node cluster.
