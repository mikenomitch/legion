# JSONB → TEXT Column Affinity (stop parsing JSON blobs as numbers)

**Status: SHIPPED** (2026-08-06, single phase) — fixes the TODO item
"JSONB columns carry NUMERIC affinity, so every JSON write parses as a
number first". Scope is deliberately the JSON columns only; `STRING` and
`TIMESTAMP` columns share the defect and are recorded as an explicit
non-goal (see below and the calls doc).

> **Judgement calls:** [`../calls/jsonb-text-affinity-plan_calls.md`](../calls/jsonb-text-affinity-plan_calls.md)

## Problem

SQLite assigns column *affinity* from the declared type name by substring
rules ([datatype3.html §3.1](https://www.sqlite.org/datatype3.html)):
`INT` → INTEGER, `CHAR`/`CLOB`/`TEXT` → TEXT, `BLOB` → BLOB,
`REAL`/`FLOA`/`DOUB` → REAL, **anything else → NUMERIC**. `JSONB`
contains none of the keywords, so every one of Legion's 46 JSON columns
across 17 tables has NUMERIC affinity. On every INSERT/UPDATE, SQLite
runs `applyNumericAffinity`/`sqlite3AtoF` over the bound text trying to
convert it to a number first; a sampled manager showed ~4-5% of CPU in
that path. The conversion always fails for a JSON object/array, so the
work is pure waste — and it sits inside the serialized plan-apply commit
(`allocation.resources`, `plan_submission.placements`,
`node_alloc_usage.usage_json`), on the job-intake path (21 JSON columns
on `task_config`), and on the event stream (`scheduler_event.metadata_json`).

A local A/B (in-memory SQLite, 200k-row inserts of a realistic
`resources` blob into 3 JSON columns + 1 timestamp) measures the
NUMERIC-affinity table ~5-10% slower than the TEXT twin on the raw
insert path, consistent with the profiled share.

There is a correctness edge as well, not just waste: NUMERIC affinity
*succeeds* on numeric-looking text. A JSON column storing a bare JSON
number (`42`) would be silently stored as an INTEGER and fail a
`column_str` read-back. No current writer produces bare JSON scalars, so
this is latent — but TEXT affinity removes the class.

## Prior art

- SQLite itself: `JSONB` as a declared type is a footgun the SQLite docs
  warn about indirectly — their own JSONB (3.45+) is a *storage format*
  used via `jsonb()` functions, not a column type; declared types are
  affinity hints only. The documented fix for a wrong declared type is a
  table rebuild ([lang_altertable.html §"Making Other Kinds Of Table
  Schema Changes"](https://www.sqlite.org/lang_altertable.html)).
- This repo: the C-0/C-1/C-2 lane-ownership migrations
  (`src/storage/migrations.rs`) established the rebuild pattern —
  create twin, copy, drop, rename, re-create indexes — including the
  rule that rebuild order must account for `foreign_keys = ON` (a
  `DROP TABLE` performs an implicit `DELETE FROM`, firing CASCADEs).

## Jobs to be done

**Cluster operator**
- When my manager is saturated, I want its CPU spent planning and
  committing placements, not re-parsing JSON blobs as numbers, so the
  same hardware schedules more.
- When I upgrade a manager, I want the storage migration to be invisible
  — same data, same indexes, same rollback story — so an upgrade stays
  boring.

**Legion maintainer**
- When I profile the write path, I want `sqlite3AtoF` gone from the
  flame graph, so real costs are visible instead of noise I have to
  explain away every time.

## Goals

1. Every JSON column in the live schema declares a TEXT-affinity type.
2. The migration preserves data, column order, column count, constraints,
   indexes, and rowids exactly — byte-for-byte content, only declared
   types change.
3. Mixed-version raft clusters stay safe during a rolling manager
   upgrade (see Rollout).

## Non-goals

- **`STRING` and `TIMESTAMP` columns keep NUMERIC affinity.** They share
  the wasted-parse defect, but unlike JSON columns their conversion can
  *succeed*: `STRING` ids that look numeric and unix-seconds timestamps
  are stored as INTEGERs today. Re-typing them changes what is stored
  and breaks comparisons against existing INTEGER-stored rows; that is a
  value-migration project with its own risk budget, not a type-rename.
  Recorded in TODO.md as a known cost.
- No new-cluster-only shortcut: the initial migrations are append-only
  history and are not edited (editing them would fork fresh and upgraded
  clusters onto different schemas).

## What exists today

- 17 tables with 46 `JSONB` columns (`src/storage/migrations.rs`), the
  hot ones being `allocation` (resources, network), `plan_submission`
  (placements, expected_version_vector, evictions), `node_alloc_usage`
  (usage_json), `task_config` (21 columns), `node`/`node_health`
  (attrs, health_checks), `scheduler_event` (metadata_json).
- `eval`, `task_status`, `instance`, `job` carry no JSON columns —
  the eval loop's own tables are unaffected.
- Migrations run via `rusqlite_migration` inside a transaction with
  `foreign_keys = ON` (`Store::configure_connection`,
  `src/storage/store.rs:1767`), so `PRAGMA foreign_keys = OFF` is not
  available to a migration and CASCADE hazards must be handled by
  ordering.
- `job_config` is the one rebuilt table that is a CASCADE *parent*
  (children: `task_config`, `job_config_submission`, `job_kv_reference`,
  `job_update`) — dropping it naively would fire its children's
  `ON DELETE CASCADE` via the implicit `DELETE FROM`.
- Raft replication captures per-table changesets via the SQLite session
  extension, which identifies columns **by position**, and lane
  snapshots install rows **by name** (`src/storage/raft/state_machine.rs`).

## Proposed design

One migration, appended, that rebuilds every JSONB-carrying table with
the same schema text except ` JSONB` → ` TEXT`:

1. `ALTER TABLE job_config RENAME TO job_config__jsonb` (children's FK
   clauses follow the rename), create the new `job_config` with TEXT
   affinity, copy rows.
2. Rebuild the four children (`task_config` with its JSONB→TEXT changes;
   `job_config_submission`, `job_kv_reference`, `job_update` with
   identical schemas) — their fresh `REFERENCES job_config` clauses bind
   to the new parent, and their copies FK-check against it.
3. `DROP TABLE job_config__jsonb` — nothing references it anymore, so
   the implicit DELETE fires no cascades.
4. Rebuild the remaining 15 JSONB tables with the twin-copy-drop-rename
   pattern, re-creating each table's indexes (including the
   `idx_task_config_service_name` expression index).

The migration SQL is generated mechanically from the live schema (replay
migrations, rewrite declared types, emit rebuilds) rather than typed by
hand, then embedded as the usual static `M::up` string.

**Column order and count are preserved exactly** — this is the
load-bearing property, not a nicety: session changesets apply by column
position, so a rebuild that reordered columns would let a mixed-version
manager pair silently write values into the wrong columns during a
rolling upgrade. The twin-table rebuild keeps the positional layout
identical to a pre-migration peer; the only cross-version difference is
declared type names, which changesets never see.

## Persistence changes

- One appended migration in `src/storage/migrations.rs` (schema version
  +1). No wire change, no config surface, no struct change.
- Rowids are preserved (explicit-id copies), so `node_alloc_usage`
  fingerprints (alloc epoch + count + id sum) remain valid — per the
  NOTE on that table's migration, a rebuild that renumbered ids would
  have had to clear the cache table.
- `sqlite_sequence` rows for the three AUTOINCREMENT tables
  (`plan_submission`, `rollout`, `rollout_event`) are re-seeded to
  max(id) by the explicit-id copies. If the largest-id row was deleted
  before the migration, an id below the old high-water mark can be
  reused — same acceptance as the C-2 rebuild (ids are never treated as
  a total order across deletes anywhere in the code).

## Implementation phases

Single phase: migration + tests + benchmark record. There is nothing to
gate — the change is invisible except to a profiler.

## Testing plan

- Migration unit tests (in `migrations.rs`): after `to_latest`, **no
  column in any table declares JSONB**; a seeded pre-migration database
  (rows across the job_config cluster, allocations, plan submissions,
  node usage rows) comes out with identical values, identical column
  order, identical index sets, `PRAGMA foreign_key_check` clean, FKs
  still enforcing (bogus child insert fails, parent delete cascades),
  and AUTOINCREMENT continuing above copied ids.
- Full suite (`cargo test --all --locked`) — the raft suites cover
  changeset capture/apply over the rebuilt tables.
- Before/after Criterion runs of `scheduler_speed` (bin_pack, 1k nodes)
  and `eval_claim_apply_complete_latency`, recorded in
  `benchmarks/scheduler/README.md`.

## Rollout and backward compatibility

- The migration runs once at manager startup, per replica, before raft
  mode arms. Data is untouched (all stored JSON was already TEXT —
  NUMERIC conversion never succeeded on it); only future writes skip the
  numeric parse.
- **Rolling manager upgrade:** safe. Column order/count are preserved,
  so mixed-version changeset exchange is positionally identical; lane
  snapshot install matches columns by name and both sides have the same
  names. A downgraded manager reads the TEXT-affinity schema exactly as
  before (affinity only changes conversion behavior on write, and old
  binaries write the same TEXT values).
- Failure mode: the migration is one transaction — any error rolls the
  whole rebuild back and the manager refuses to start on the old
  schema version, which is the standard migration failure story.

## Acceptance criteria

- `grep JSONB` finds no live table (only history in earlier migrations).
- Migration tests above pass; full suite green.
- Before/after benchmark cells recorded with host + profile noted.
