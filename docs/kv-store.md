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

## KV Change -> Job Update Flow

On `put_kv` and `patch_kv`:

1. Manager finds jobs referencing that key.
2. Re-materializes job configs with updated KV values.
3. Submits updated job config.
4. Creates rollout for config change.
5. Enqueues `rollout_tick` (unless manual rollout progression).

This gives automatic rollout on KV update for linked jobs.

## Important Current Behavior

`delete_kv` does not trigger the KV-change job-update flow today (no `handle_kv_change` call on delete path).

