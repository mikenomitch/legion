# Plan: KV-Driven Task Updates, Injection, and Capabilities

## Problem Statement

Legion has a KV store and job/task specs, but there is no unified, user-facing way to:

1. Trigger task/job updates when referenced KV values change (including deletes).
2. Opt in/out per KV reference in job spec.
3. Inject KV-backed data into task files and environment variables with predictable update behavior.
4. Respect task-kind support constraints via explicit capability reporting.
5. Test this end-to-end with clear coverage.

This plan proposes a simple, explicit API in job spec and a runtime model that is generic across task kinds.

---

## Goals

- **KV deletion awareness:** Deleting a watched KV key must generate a reconciliation event that can update affected tasks/jobs.
- **Spec-level opt in/out:** Each KV-derived value can independently declare whether Legion auto-updates when the KV value changes.
- **Simple injection API:** Allow `file` and `env` injections from inline template or KV template reference.
- **Explicit rollout behavior:** Per injection/reference update mode supports `restart`, `rollout`, or `none`.
- **Capability-gated behavior:** Each task kind reports whether it supports KV env injection, KV file injection, and restart semantics.
- **Deterministic behavior:** If a configured action is unsupported by task kind, fail fast in planning/validation with actionable error.

## Non-Goals

- Secret management redesign (this is KV-driven config behavior, not a full secret store).
- Arbitrary dynamic reconfiguration protocols inside running processes.
- Expanding plugin ABI beyond capability reporting and prepared input contract changes needed for KV injection.

---

## Proposed Job Spec Additions

### 1) Unified KV Reference Policy Block

Introduce a reusable policy object for any KV-backed field:

```json
{
  "on_change": "none | restart | rollout",
  "watch": true,
  "on_delete": "none | restart | rollout | fail"
}
```

Semantics:
- `watch` (default `false`): when `true`, Legion subscribes/reconciles on KV create/update/delete events for this key.
- `on_change` (default `none`): action when resolved value changes.
- `on_delete` (default `fail`): action when key is deleted.
  - `fail`: mark allocation/task as failed with clear diagnostic because required input vanished.
  - Other values apply normal behavior.

### 2) File Injection API

Add `kv_injections.files[]` at task spec level:

```json
{
  "path": "/etc/myapp/config.yaml",
  "mode": "0640",
  "file": {
    "template": "port={{ kv \"app/port\" }}\n",
    "template_kv": "templates/myapp-config"
  },
  "kv": {
    "watch": true,
    "on_change": "rollout",
    "on_delete": "fail"
  }
}
```

Rules:
- `file.template` = inline string template source.
- `file.template_kv` = KV key that stores template text.
- Exactly one of `template` or `template_kv` must be set.
- Template rendering can reference KV values via existing/extended template functions.
- Result is materialized at `path` before task start.

### 3) Env Injection API

Add `kv_injections.env[]` at task spec level:

```json
{
  "name": "FEATURE_FLAGS",
  "value": {
    "template": "{{ kv \"flags/global\" }}",
    "template_kv": "templates/feature-flags"
  },
  "kv": {
    "watch": true,
    "on_change": "restart",
    "on_delete": "none"
  }
}
```

Rules:
- Exactly one of `value.template` or `value.template_kv`.
- Rendered string is injected as environment variable before task launch.
- Normal env override precedence should be documented (recommended: explicit static env in task spec wins unless `allow_override=true` is later introduced).

### 4) Optional Top-Level Defaults (Simple Inheritance)

To reduce repetition, allow optional task-level defaults:

```json
{
  "kv_injections": {
    "defaults": {
      "kv": {
        "watch": true,
        "on_change": "restart",
        "on_delete": "fail"
      }
    },
    "env": [ ... ],
    "files": [ ... ]
  }
}
```

Per-entry policy overrides defaults.

---

## Proposed Runtime / Control-Plane Behavior

### A) Dependency Tracking and Watch Graph

- During job parse/plan, compute `KVDependencySet` per task:
  - Explicit keys: `template_kv` pointers and direct `kv("...")` references found during template analysis.
  - Mark dependency edge with policy (`watch`, `on_change`, `on_delete`) and injection target metadata.
- Persist dependency graph in state store for manager reconciliation.

### B) KV Event Handling (Create/Update/Delete)

- KV subsystem emits normalized events: `{key, revision, event_type}` where `event_type ∈ {put, delete}`.
- Reconciler resolves impacted tasks/jobs via dependency graph.
- For each impacted edge:
  - If `watch=false`, ignore.
  - If `put`, apply `on_change`.
  - If `delete`, apply `on_delete`.
- Coalesce rapid events by key/job into debounce window to avoid stormy restarts/rollouts.

### C) Action Semantics

- `none`: update desired rendered inputs in state only; no immediate restart/rollout.
- `restart`: restart allocation/task in place where supported.
- `rollout`: create new deployment/eval and perform configured rollout strategy.
- `fail`: set task desired state to failed with reason `missing_kv_dependency`.

### D) KV Delete Requirement

Deletion must be treated as first-class event (not “missing update”).
- Prior behavior likely ignored missing keys; new behavior must propagate explicit delete signals.
- Delete-induced render resolution should differentiate:
  - optional reference (future extension) vs required reference (default required).

---

## Task Kind Capability Reporting

### Capability Model

Introduce capability flags reported by each task kind plugin/implementation:

- `supports_kv_env_injection`
- `supports_kv_file_injection`
- `supports_live_restart`

Optional future flags:
- `supports_in_place_reload_signal`
- `supports_atomic_file_swap`

### Validation Rules

At job admission/planning:
- If spec requires file injection but kind lacks `supports_kv_file_injection` → validation error.
- If `on_change=restart` but kind lacks `supports_live_restart` → validation error.
- Same checks for `on_delete` actions.

### API / Protocol Surface

- Extend task-kind registration/handshake response with capability set.
- Persist capability set on worker/node/task-kind record.
- Scheduler filters placements that cannot satisfy required capabilities.

---

## Proto and Documentation Updates

1. Update `proto/legion.proto` comments for:
   - KV injection structures.
   - Policy enums and defaults.
   - Delete behavior and failure modes.
   - Capability fields and validation behavior.
2. Regenerate `structs.rs` via build.
3. Add deep docs updates:
   - `docs/kv-store.md` (watch/delete semantics).
   - `docs/jobs-tasks-and-scheduling.md` (job spec API).
   - `docs/task-kind-*.md` (capability expectations per kind).
   - `docs/plugin-system.md` (capability handshake changes).

---

## Proposed Spec (Consolidated Example)

```jsonnet
{
  job_name: 'api',
  tasks: [
    {
      name: 'api',
      kind: 'container',

      env: {
        STATIC_MODE: 'prod',
      },

      kv_injections: {
        defaults: {
          kv: {
            watch: true,
            on_change: 'restart',
            on_delete: 'fail',
          },
        },

        env: [
          {
            name: 'FEATURE_FLAGS',
            value: {
              template_kv: 'templates/feature-flags-env',
            },
            kv: {
              on_change: 'rollout',
            },
          },
          {
            name: 'WELCOME_BANNER',
            value: {
              template: '{{ kv "ui/banner" }}',
            },
            kv: {
              watch: false,
              on_change: 'none',
              on_delete: 'none',
            },
          },
        ],

        files: [
          {
            path: '/etc/api/runtime.yaml',
            mode: '0640',
            file: {
              template_kv: 'templates/api-runtime-config',
            },
            kv: {
              on_change: 'rollout',
              on_delete: 'fail',
            },
          },
          {
            path: '/etc/api/tenant.cfg',
            file: {
              template: 'tenant={{ kv "tenant/id" }}\n',
            },
            kv: {
              on_change: 'restart',
              on_delete: 'restart',
            },
          },
        ],
      },
    },
  ],
}
```

---

## Implementation Plan

### Phase 1: Schema + Validation

- Add proto messages/enums for KV injection and policy.
- Add task capability fields in proto/plugin handshake types.
- Implement admission validation for:
  - mutually exclusive template sources,
  - capability compatibility,
  - required fields and defaults.

### Phase 2: Dependency Extraction + State

- Build parser/extractor for KV dependencies from `template` and `template_kv`.
- Persist dependency graph keyed by `{job_id, task_name, key}` plus action policy.
- Ensure updates on job revisions clean stale dependency edges.

### Phase 3: Eventing + Reconciliation

- Emit KV delete events in same stream as puts.
- Add reconciler path mapping key events to impacted tasks/evals.
- Implement debounce/coalescing and idempotent action scheduling.

### Phase 4: Runtime Materialization

- Extend task launch pipeline to render env and file injections.
- Ensure deterministic ordering, atomic file write semantics, and clear error messages.
- Wire action modes (`none`, `restart`, `rollout`, `fail`) into eval/rollout system.

### Phase 5: Capability Plumbing

- Extend task-kind capability reporting and persistence.
- Update scheduler checks and placement constraints.
- Document per-kind support matrix (`exec`, `container`, `noop`, plugins).

### Phase 6: Documentation + Maximal Config

- Add new API examples to `configs/jobs/maximal.jsonnet`.
- Update docs listed above with user-facing guidance and edge-case behavior.

**Note:** We still need a future design for injecting KV values directly into job spec fields (outside task env/file injections); this plan intentionally does not implement that capability.

---

## Complete Testing Plan

### Unit Tests

1. **Spec validation tests**
   - Reject both `template` + `template_kv` set.
   - Reject neither set.
   - Default policy assignment correctness.
   - Capability mismatch failures for each action.

2. **Dependency extraction tests**
   - Extract keys from inline templates.
   - Extract template pointer keys.
   - Deduplicate repeated references.

3. **Policy resolution tests**
   - Entry override vs defaults precedence.
   - Delete event selects `on_delete`, put event selects `on_change`.

4. **Materialization tests**
   - Env rendering success/failure paths.
   - File write path, mode handling, atomic swap behavior.

### Integration Tests

1. **KV put triggers restart**
   - Start job with watched env reference and `on_change=restart`.
   - Mutate key, assert task restart count increments.

2. **KV put triggers rollout**
   - Watched file reference with `on_change=rollout`.
   - Mutate key, assert new eval/deployment revision created.

3. **KV delete behavior matrix**
   - `on_delete=none`: no rollout/restart.
   - `on_delete=restart`: restart occurs.
   - `on_delete=rollout`: rollout occurs.
   - `on_delete=fail`: task/allocation enters failed with reason.

4. **watch=false behavior**
   - Mutate/delete key; assert no automatic action.

5. **Capability-gated placement**
   - Submit spec requiring file injection to unsupported task kind.
   - Assert admission or scheduling failure with explicit capability message.

6. **Debounce/idempotence**
   - Burst updates to same key; assert single bounded action per debounce window.

### End-to-End / CLI Tests

- Job run with sample config in `configs/jobs/maximal.jsonnet`.
- CLI/API surfaces expose capability and validation errors clearly.
- Documentation snippets are executable or covered by doc tests where feasible.

### Regression Tests

- Existing non-KV jobs unchanged.
- Existing KV reads without watches preserve prior behavior.
- Multi-task job where only subset depends on changed key updates only impacted tasks.

---

## Rollout & Backward Compatibility

- Feature flag gate initially (`kv_reactive_updates`).
- Backward-compatible defaults (`watch=false`, `on_change=none`, `on_delete=fail` for explicit KV injections).
- Migration note: users must opt in to automatic updates; no surprise restarts for existing jobs.

---

## Open Questions (To Resolve During Implementation)

1. Should `on_delete=fail` be default for env/file injections, or should it differ by optional marker?
2. Should template rendering support explicit optional lookup helper like `kv_or("key", "default")` in v1?
3. For `none`, should changed rendered data be visible in desired-state only or deferred until next normal restart?
4. Exact precedence between static `env` and injected env when names collide.
5. Whether restart vs rollout should be promoted automatically when deployment strategy requires immutable updates.

---

## Acceptance Criteria

- User can define KV-based env/file injections in job spec with clear per-reference watch/action policy.
- KV delete events reliably trigger configured behavior.
- Unsupported task kinds are rejected with precise capability errors.
- Docs and maximal config demonstrate full API.
- Test suite covers validation, reconciliation, capabilities, and delete semantics.
