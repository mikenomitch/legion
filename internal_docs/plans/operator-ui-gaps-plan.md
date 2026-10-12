# Operator UI gaps: instance and allocation pages, honest task data, KV reveal, public addresses

> **Judgement calls:** [operator-ui-gaps-plan_calls.md](../calls/operator-ui-gaps-plan_calls.md)

**Status: SHIPPED** (2026-10-12), except the items under
[Deferred](#deferred). Written alongside the fixes: the work started as a batch
of bug reports from an operator driving a real cluster through the UI, and the
reports that turned out to need new surfaces are planned here.

## 1. Problem statement

An operator running agent sandboxes and a handful of services on an AWS
cluster hit these in one sitting:

1. **Tasks look missing.** The job page's Tasks card says "No task information
   available in the current job spec" for every job submitted from the CLI,
   while the Allocations card lists running work. The task page's "Runtime
   allocations" and Logs cards are empty for any job deployed without a
   rollout. The UI reconstructed tasks by `JSON.parse`-ing the job's source
   submissions, which are Nickel, and read allocations from the latest
   rollout's status, which a first deploy does not have.
2. **No way into an instance or an allocation.** The Instances table is a dead
   end, and nothing in the UI answers "what happened to this sandbox" — the
   event journal knows, but cannot be filtered to an instance or an
   allocation. There is no allocation page at all.
3. **Exec collapses on its own.** The task page polls, rebuilds every
   allocation object, and remounts every per-allocation exec panel, closing
   whatever terminal was open.
4. **KV shows every secret by default.** `ListKv` decrypts and returns every
   value under the prefix; the KV page renders the selected value in
   plaintext. Nothing says which jobs read a key.
5. **Links to services do not open.** Endpoint links point at nodes' private
   VPC addresses. Legion has no notion of a node's public address.
6. **Scale silently undone.** After a KV change re-rendered `kv-demo`, an old
   rollout kept ticking, treated the newer version's allocations as "old",
   retired them, and could place nothing — 0 allocations, rollout RUNNING
   forever, and every scale-out removed within two seconds.
7. **The manager page is a JSON dump** of a double-encoded config string, with
   "updated —" because the timestamp format did not parse.

## 2. Prior art

- **Nomad UI** has allocation pages (task states, events, resource
  utilization, logs, exec, file browser) and an allocation is the unit you
  click into from everywhere. Its "recent events" per task are the closest
  analogue to the instance/allocation timelines here; ours come from the
  cluster event journal rather than a per-task event list.
- **Kubernetes** answers "what happened to this pod" with `kubectl describe` —
  events filtered by `involvedObject`. The instance filter is the same idea:
  an event matches when it names the object, plus the objects that fulfilled
  it (the instance's allocations).
- **Vault / 1Password / AWS Secrets Manager consoles** mask secret values and
  require an explicit reveal; listing never returns values.
- **Nomad** links endpoints to the client's advertised address and leaves
  reachability to the operator; **Kubernetes** `Node.status.addresses` carries
  `InternalIP` and `ExternalIP` side by side. The public address follows K8s:
  a second, display-only address next to the one the cluster dials.

## 3. Jobs to be done

- **Operator debugging a sandbox** — "Open this instance, see its state,
  storage, checkpoint, and every event about it, including the allocations
  that ran it, then suspend/resume/fork it from the same page."
- **Operator on call** — "Open allocation #98 from anywhere it is mentioned
  and see what it is, whether it is healthy, how to reach it, what it is
  using, what happened to it, its logs, and a shell — and still see its
  history after it is gone."
- **Operator handling secrets** — "Browse keys without anyone over my
  shoulder seeing values; reveal one deliberately; know which jobs break if I
  delete or change it."
- **Developer checking a deployed UI** — "Click the endpoint and have it
  open."
- **Agent** (MCP / CLI) — "Fetch the events for one instance or allocation
  without paging the whole journal."

## 4. Goals and non-goals

Goals: the pages above; server data that makes them honest without
`state:read` workarounds; no value decryption on browse paths; a working
public link when the operator declares a public address; stale rollouts that
cannot destroy newer work.

Non-goals: a reverse proxy through the manager (planned separately in
[manager-reverse-proxy-plan.md](manager-reverse-proxy-plan.md)); auto-detecting
public addresses from cloud metadata; an allocations index page; a
`GetAllocation` RPC (the ledger serves the page — see calls).

## 5. What existed

- `GetJobSpec` returned only `submissions` (source as typed).
  `extractJobSpecData` in `ui/src/utils/jobspec.ts` JSON-parsed them.
- `LedgerAllocation` carried `task_config_id` but no task name; the UI joined
  through `queryStateStore` SQL (needs `state:read`).
- `SchedulerEventFilter` (`src/storage/events.rs`) filtered by kind, job,
  node, level. Instance events carry `instanceId` in metadata
  (`instanceId`/`instance_id` in `node.pressure`); allocation events carry
  `allocation_id`; allocation rows carry `instance_id`.
- `job_kv_reference` already recorded `(job, config, key, json_pointer)` for
  every `$path` reference and `kvInjections` entry at submit
  (`Store::replace_job_kv_references`).
- Workers register `workerAddress` (always the private bind address); service
  endpoints derive `address` from it.
- `handle_rollout_tick` (`src/manager/rollout.rs`) never checked that its
  target was still the job's latest config; `reconcile_kv_changes`
  (`src/manager/server.rs`) creates new configs on KV restart/update without
  cancelling an active rollout.

## 6. Design

### 6.1 Rendered jobspec and task names (bug fixes with wire additions)

- `GetJobSpecReply.rendered_jobspec`: the stored `JobConfig` as JSON, the
  shape `RunJobRequest.parsed_jobspec` accepts. Both `GetJobSpec` and
  `GetJobSpecByConfig` fill it. The UI's `extractJobSpecFromReply` prefers it
  and falls back to submissions.
- `LedgerAllocation.task_name`, filled by `GetLedger` from one
  `task_config` lookup per call (`Store::task_names_for_configs`).
- The task page lists the task's allocations from the ledger (all statuses),
  enriched with the rollout's role when a rollout lists them; logs cover the
  union.

### 6.2 Event filters

```
legion events list --instance box-KPI6u4fi8GmwSWCo
legion events list --allocation 98
legion events follow --instance moose
```

`ListEventsRequest.instance_id` / `StreamEventsRequest.instance_id` match an
event whose metadata names the instance (`instanceId`, `instance_id`, or a
member of `instances` / `suspending`) **or** whose `allocation_id` is an
allocation that fulfilled the instance. `allocation_id` is an exact column
match. Same filters on the `legion_events` MCP tool. In SQL the instance match
is `json_extract`/`json_each` plus `allocation_id IN (SELECT id FROM
allocation WHERE instance_id = ?)`; the live stream resolves the allocation
half per event, so allocations placed after the stream opened match.

### 6.3 Instance and allocation pages

- `/jobs/:job/instances/:id` — state, mode, slot, current allocation and node,
  storage, checkpoint, idle countdown, fork lineage, last stop and cold boot,
  usage, the same Suspend / Resume / Fork controls as the job page, the
  instance's snapshots, and the instance event timeline (§6.2).
- `/allocations/:id` — what and where (job, task, instance, node, config,
  rollout), health with the declared checks, ports and endpoints, reserved vs
  measured resources, artifacts, the allocation event timeline, logs, and the
  exec console. When the ledger no longer has the row, the page says the
  allocation has ended and still shows its events.
- Every allocation id in the UI links to its page; instance ids link to theirs.

### 6.4 Exec

*Superseded on rebase.* This branch built a per-task exec console
(`TaskExecConsole`) to fix the panel that remounted on every poll. Main
meanwhile shipped the task page's **Actions** tab
([ui-task-and-template-redesign-plan.md](ui-task-and-template-redesign-plan.md)),
which fixes the same remount with one panel behind a target picker. The
Actions tab won, and `TaskExecConsole` was dropped. The allocation page embeds
the same `TaskActions` panel aimed at its one allocation
([calls 6](../calls/operator-ui-gaps-plan_calls.md#6-reversed-exec-uses-the-task-pages-actions-panel-not-a-second-console)).

### 6.5 KV values are hidden until revealed

- `ListKvRequest.include_values` (default **false**): listing never decrypts.
  `legion kv list --values` opts in for `--json`.
- `GetKvRequest.metadata_only`: the entry without its value, no decryption.
- `GetKvReply.references`: jobs reading the key, from `job_kv_reference`,
  filtered to jobs the caller may read.
- The KV page loads metadata only, masks the value, and reveals on click
  (Hide drops the plaintext; switching keys re-hides). "Replace value" writes
  a new value without revealing the old one. A "Used by" list names the jobs;
  delete warns when any read the key. `legion kv get` prints `used by:`.

### 6.6 Worker public address

```
LEGION_WORKER_PUBLIC_ADDRESS=203.0.113.7 legion join <token>
# on EC2:
LEGION_WORKER_PUBLIC_ADDRESS=$(curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT \
  http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" \
  http://169.254.169.254/latest/meta-data/public-ipv4) legion join <token>
```

The worker registers it as the `publicAddress` config attribute.
`ServiceEndpoint.public_address` carries it; `legion service resolve` shows a
PUBLIC column; the UI links the public address and shows the in-cluster one
beside it. Without one, private (RFC 1918, CGNAT, loopback, link-local, ULA)
addresses are marked "private" with how to fix it. Nothing in the cluster
dials the public address.

### 6.7 Stale rollouts end themselves

A rollout tick whose target is not the job's latest config cancels the
rollout ("Rollout superseded: job config X replaced its target Y"), releases
deferred scale-ins, and enqueues a `job_reschedule` for the latest config. One
guard covers every path that creates a config without RunJob's supersede
step, and recovers jobs already stuck.

### 6.8 Manager pages

The manager detail page renders the parsed config as summary, feature tiles,
and per-area cards, with secrets redacted and unknown keys under "Other
settings". The KV encryption card moved from the Managers list to the KV page.
Manager timestamps in the store's `YYYY-MM-DD HH:MM:SS +HH:MM:SS` format
parse.

## 7. Proto and format changes

All additive except one default: `ListKv` no longer returns values unless
asked. New fields: `GetJobSpecReply.rendered_jobspec`,
`LedgerAllocation.task_name`, `ListEventsRequest.instance_id`/`allocation_id`,
`StreamEventsRequest.instance_id`/`allocation_id`, `ListKvRequest.include_values`,
`GetKvRequest.metadata_only`, `GetKvReply.references` + `KvJobReference`,
`ServiceEndpoint.public_address`. No migrations; no new job-config fields.

## 8. Testing

- `structs::eval::tests::stale_rollout_cancels_instead_of_retiring_newer_allocations`
- `storage::store::tests::event_filter_by_instance_matches_metadata_and_allocations`
  (SQL and the stream's `matches_with` agree)
- `storage::store::tests::list_kv_keys_omits_values`
- `storage::store::tests::service_endpoints_carry_the_node_public_address`
- `tests/acl_endpoints.rs` covers the changed KV requests.
- UI: `tsc`, and Playwright renders against mocked RPCs for the new pages.

## 9. Rollout

No installed base: the `ListKv` default flips outright. Clients that need
values pass `include_values` (the CLI's `--values`). Workers without
`LEGION_WORKER_PUBLIC_ADDRESS` behave exactly as before, with the private
marker in the UI.

## Deferred

- Auto-detecting the public address from cloud metadata
  (`LEGION_WORKER_PUBLIC_ADDRESS=auto`).
- A manager reverse proxy for nodes with no reachable ports
  ([plan](manager-reverse-proxy-plan.md)).
- A `GetAllocation` RPC so the allocation page works for job-scoped tokens
  without `node:read`.
- `legion kv get --json` does not include `references` (text output does).
- The Jobs index still shows `done` for running jobs (existing open item).
