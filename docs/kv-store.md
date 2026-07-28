# KV Store

This document explains manager KV behavior and job integration.

## Data Model

Primary objects:

- `kv_entry`:
  - `key` (string, primary key)
  - `value` (text)
  - `value_type` (`string`, `number`, `json`)
  - `version` (int64, optimistic concurrency)
  - timestamps
- `kv_lock`:
  - `key`, `lock_id`, optional owner
  - acquired/expiry timestamps

## CRUD Operations

Manager API and CLI support:

- put
- get
- list by prefix
- delete
- json patch (for JSON values only)

Value normalization rules:

- `string`: stored as-is
- `number`: must parse as JSON number
- `json`: must parse as valid JSON

## Versioned Writes

`expected_version` semantics:

- write/update succeeds only when expected equals current version
- create can require `expected_version=0`
- mismatch returns conflict

This is used for optimistic concurrency.

## Locking Model

Optional lock enforcement for mutating operations:

- Acquire lock with TTL (`acquire_kv_lock`).
- Renew/release lock by `lock_id`.
- Expired locks are cleaned lazily during lock checks.
- Mutations that provide wrong/missing lock ID conflict when lock is active.

`force` on acquire can take over an existing lock.

## JSON Patch

`patch_kv` requires entry type `json`.

- Patch payload must be RFC6902-style JSON patch array.
- Patch is applied server-side and bumps version.

## Job KV References

Job submit path resolves KV references inside jobspec JSON:

- String values starting with `$` and containing `/` are treated as KV references.
- Referenced KV value replaces that string in jobspec.
- Resolved references are tracked in `job_kv_reference` with JSON pointers.

Legacy `$` references are implicitly watched and required:

- A put to the key re-materializes the job config and rolls it out
  (historical behavior, unchanged).
- A delete marks the job failed with reason `missing_kv_dependency`
  (the reference can no longer be rendered). See "Fail semantics" below.

## Reactive updates (KV-driven task updates)

Tasks can opt into KV-backed env and file injections with explicit
per-reference update policies. See `configs/jobs/kv-reactive-demo.jsonnet`
for a minimal runnable example and `configs/jobs/maximal.jsonnet` for the
full surface.

### Task spec API

```jsonnet
task: {
  runtime: "exec",
  // Task-level policy defaults for all injections below.
  kv: { watch: true, onChange: "restart", onDelete: "fail" },
  env: [
    {
      name: "FEATURE_FLAGS",
      value: {
        // Exactly one of:
        template: '{{ kv "flags/global" }}',  // inline template
        templateKv: "templates/feature-flags",  // KV key holding the template
      },
      // Optional per-entry policy override (field-by-field).
      kv: { onChange: "rollout" },
    },
  ],
  files: [
    {
      path: "/etc/app/runtime.conf",
      mode: "0640",
      file: { template: 'tenant={{ kv "tenant/id" }}\n' },
    },
  ],
}
```

Templates substitute `{{ kv "some/key" }}` (or bare `kv "some/key"`) with the
current KV value. Rendering happens server-side at submit and on every
reconciliation; the rendered result lands in the task config (`env` map /
`files[]` array) the worker receives. A `templateKv` source tracks both the
template key itself *and* every key the template text looks up (transitive
dependencies), so changing either triggers the entry's policy.

If a required key is missing at submit time, the submit fails with
`kv entry <key> not found` — jobs are never admitted with unresolvable
references.

### Policy semantics

Per-reference policy fields (all optional):

| Field      | Values                                | Hard default |
|------------|---------------------------------------|--------------|
| `watch`    | `true` / `false`                      | `false`      |
| `onChange` | `none`, `restart`, `rollout`, `fail`  | `none`       |
| `onDelete` | `none`, `restart`, `rollout`, `fail`  | `fail`       |

Precedence is field-by-field: an entry-level `kv` block overrides only the
fields it sets; unset fields inherit the task-level `kv` defaults block, and
fields unset in both places use the hard defaults above. (So an entry can,
for example, override `onChange` while inheriting `watch: true` from the
defaults — or set `watch: false` to opt out entirely.)

Event handling: a put resolves through `onChange`, a delete through
`onDelete`. Edges with `watch: false` (the default) never act — though a put
still refreshes the rendered value in the stored desired config, which is
picked up on the next natural restart/update. When one KV change matches
several edges of a job, the strongest action wins:
`none < restart < rollout < fail`.

Actions:

- `none`: desired state is re-rendered and stored; nothing is scheduled.
- `restart`: re-render, then enqueue a `job_update` eval. This re-evaluates the
  **whole job's** allocations against the new config — not just the task that
  owns the changed key. A watched-key restart therefore replaces every task's
  allocations in the job (in place, without a staged rollout), so co-located
  tasks pick up the change together. Use `rollout` if you need staged
  progression instead.
- `rollout`: re-render, then create a rollout for the new config revision
  (manual progression is honored) and enqueue `rollout_tick`.
- `fail`: mark the job failed (see below). Typically used as `onDelete`.

On delete, the deleted key obviously cannot be re-rendered: entries that
depend on it keep their last rendered value, and `onDelete: restart/rollout`
still fires even though the rendered config is unchanged.

### Debounce / coalescing

Bursts of KV writes are coalesced so jobs do not see an eval/rollout storm.
Controlled by `kvDebounceMs` in the manager config (default `500`, `0`
disables debouncing and reconciles synchronously inside the KV RPC):

- The first event opens a window; every KV event within the window joins the
  pending batch. The window is not extended by later events, so
  reconciliation latency is bounded by one window.
- Within a batch the *last* event per key wins for policy selection: a put
  followed by a delete resolves through `onDelete`; a delete followed by a
  put resolves through `onChange`.
- The flush re-renders from the KV state at flush time, so the final
  materialized config always reflects the latest values, and at most one
  action fires per job per window (the strongest across all matched edges).
- Events are only ever coalesced, never dropped. With debouncing enabled,
  reconciliation errors are logged on the manager (there is no RPC caller to
  surface them to); with `kvDebounceMs: 0` they surface on the KV RPC.

### Fail semantics and recovery

When the resolved action is `fail` (usually `onDelete: fail` and the key was
deleted):

- The job status becomes `failed` (visible in `job list` / `job info`).
- The **impacted** allocations have their status stamped
  `failed:missing_kv_dependency` (visible in the ledger, the UI, and
  state-store queries). Scope is precise: a missing *injection* key only stamps
  the allocations of the task(s) that depend on it — sibling tasks in the same
  job are left untouched. A missing *legacy `$key`* reference is not scoped to
  one task (it makes the whole rendered config unresolvable), so it stamps
  every allocation of the job. Either way the allocations are *not* torn down
  and nothing is rescheduled — the job sits failed with the stale config; no
  restart or rollout storm.
- The KV dependency edges are retained.

Recovery paths:

- Put the missing key back: the put resolves through the edge's `onChange`;
  a `restart` or `rollout` action re-renders the config, clears the
  `failed:missing_kv_dependency` stamps, flips the job status back to `done`,
  and replaces the failed allocations.
- `onChange: none` does **not** auto-recover: a put re-renders and stores the
  desired config but schedules nothing, so the job stays `failed` with its
  stamped allocations until an explicit action.
- A manual `job run` (resubmitting the jobspec) is the recovery path for the
  `onChange: none` case — and works generally: it clears the failed status and
  re-runs the job through the normal submit/eval flow with the latest KV state.

### Capability matrix

Each task runtime reports whether it supports KV injections; submits that
require an unsupported capability are rejected with an explicit error
(`Task <name> runtime <runtime> does not support kv env/file injection`).

| Kind        | env injection | file injection |
|-------------|---------------|----------------|
| `exec`      | yes           | yes            |
| `container` | yes           | no             |
| `noop`      | no            | no             |
| plugins     | per `supports_kv_env_injection` / `supports_kv_file_injection` |

### Env precedence

Injected env vars are rendered into the task config `env` map and **override
a static `env` entry of the same name** declared in the task config. Prefer
distinct names if you need both.

### Testing this locally in a basic way

Start a manager with an in-process worker:

```sh
cargo run -- start manager -c configs/manager.jsonnet
```

Seed the watched key, then run the demo job (one exec task; watched env +
file injection with `onChange: restart`, `onDelete: fail`):

```sh
cargo run -- kv put demo/banner hello
cargo run -- job run configs/jobs/kv-reactive-demo.jsonnet
cargo run -- job list -j        # job kv-reactive-demo running
```

Trigger a restart by changing the key, then watch the allocation be
replaced (within the debounce window, ~500ms by default):

```sh
cargo run -- kv put demo/banner world
cargo run -- ledger             # allocation id changed; task re-ran with BANNER=world
cat /tmp/legion/kv-demo.conf    # banner=world
```

Delete the key to see `onDelete: fail`:

```sh
cargo run -- kv delete demo/banner
cargo run -- job list -j        # status: failed
cargo run -- ledger -j          # allocation status: failed:missing_kv_dependency
```

Recover by putting the key back:

```sh
cargo run -- kv put demo/banner back
cargo run -- job list -j        # status: done; task restarted with BANNER=back
```
