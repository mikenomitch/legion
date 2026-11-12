# Judgement Calls — JSONB → TEXT Column Affinity

For [`../plans/jsonb-text-affinity-plan.md`](../plans/jsonb-text-affinity-plan.md).
Entries are ranked by project impact, most impactful first; the number is
the rank.

## 1. Twin-table rebuild, not per-column surgery — column position is a wire format

**The call.** Every JSONB-carrying table is rebuilt as a twin with the
identical schema text (` JSONB` → ` TEXT`), copied row-for-row, dropped,
and renamed — preserving column order, column count, and rowids exactly.

**The alternative.** Per-column surgery: `ADD COLUMN <col>__text TEXT` →
`UPDATE` copy → `DROP COLUMN` → `RENAME COLUMN`. Far less SQL (four short
statements per column instead of re-declaring 20 tables), no
`DROP TABLE` anywhere, so the `job_config` CASCADE hazard (entry 3)
never arises. A competent engineer would reach for it first; the first
draft of this migration did.

**The trade-off.** The rebuild is ~300 lines of generated SQL and one
full row-copy per table at upgrade time, versus surgery's near-nothing.

**Why.** Surgery moves every rebuilt column to the end of the table, and
**raft session changesets bind columns by position**. During a rolling
manager upgrade, a migrated replica exchanging changesets with an
unmigrated one would apply values into the wrong columns — silently,
because the affected columns are all dynamically-typed TEXT. Column
order is effectively part of the replication wire format, so the
migration must be positionally invisible. Performance-goal work must not
spend the operability budget: an upgrade that can corrupt a follower is
not a perf fix. (The additive-column migrations this repo already ships
are safe on that path for the opposite reason — a column-count mismatch
makes `changeset_apply` fail loudly rather than misbind.)

## 2. `STRING` and `TIMESTAMP` columns keep NUMERIC affinity

> **Superseded 2026-11-12** by entries 7–9: the columns are re-typed (TEXT,
> and INTEGER for `job.created_at`) by a later migration with its own
> audit and tests. The reasoning below is what shaped that migration.

**The call.** Only JSON columns are re-typed. The schema's `STRING` ids
and `TIMESTAMP` columns share the same wasted-parse defect (neither name
matches an affinity keyword, so both are NUMERIC) and they are left
alone, recorded as an open TODO item.

**The alternative.** Fix all three classes in one rebuild — it is the
same mechanical change, and the heartbeat path writes `TIMESTAMP`
columns far more often than anything writes `acl_policy.rules`.

**The trade-off.** The waste on those columns remains, and the schema is
now inconsistent about it (JSON columns are honest TEXT, ids are not).

**Why.** For JSON objects/arrays the numeric conversion always *fails*,
so stored bytes are identical before and after — the re-type is pure
declaration. For `STRING`/`TIMESTAMP` the conversion can *succeed*:
numeric-looking ids and the unix-seconds timestamps `submit_job_config`
writes are stored as INTEGERs today. Re-typing those columns changes
what future writes store while old rows stay INTEGER, and
INTEGER-vs-TEXT never compare equal in SQLite — index lookups and range
scans against existing rows would silently miss. That is a
value-rewriting migration with its own testing budget, not a
type-rename, and bundling it here would have put the safe 90% behind
the risky 10%. Simplicity of the *upgrade* won over completeness.

## 3. `job_config` gets a rename dance, and three tables are rebuilt purely for their FKs

**The call.** `job_config` — the one rebuilt table that is a CASCADE
parent — is renamed aside first; its four children are rebuilt so their
fresh `REFERENCES job_config` clauses bind to the new parent
(`job_config_submission`, `job_kv_reference`, and `job_update` carry no
JSONB and are rebuilt with byte-identical schemas purely for this); the
renamed original is dropped only when nothing references it.

**The alternative.** `PRAGMA writable_schema=ON` and a one-line
`UPDATE sqlite_schema SET sql = replace(sql, ' JSONB', ' TEXT')` — the
O(1) in-place rewrite SQLite's own ALTER TABLE docs describe for
content-preserving type changes. No copies, no FK hazards, no dance.

**The trade-off.** ~80 extra lines of migration SQL and a full copy of
three tables that gain nothing from being copied.

**Why.** Migrations here run inside `rusqlite_migration`'s transaction
with `foreign_keys = ON` (so `DROP TABLE` on a CASCADE parent performs
an implicit `DELETE FROM` that would wipe its children — the C-2 rule),
and the schema-cookie bump the writable_schema procedure requires is
awkward to sequence inside that framework transaction. More decisively:
every statement in the dance is ordinary SQL that either works or rolls
the transaction back, while a writable_schema mistake *commits* and
manifests later as an unreadable schema. For a one-shot migration on
the cluster's source of truth, the loud-failure shape won over the
elegant one.

## 4. The migration SQL is generated from the live schema, then embedded as a static string

**The call.** The rebuild SQL was produced by a script that replays the
migration history, rewrites declared types, emits the rebuilds, and
validates the result (no JSONB left, column order preserved, index sets
preserved, `foreign_key_check` clean) — and the validated output is
pasted into `migrations.rs` as the usual static `M::up` string.

**The alternative.** Hand-writing the SQL (the C-2 precedent), or a
`rusqlite_migration` hook that performs the rebuild programmatically at
runtime from `sqlite_master`.

**The trade-off.** The generator is throwaway; if the SQL ever needs
editing it must be edited as SQL. A runtime hook would have adapted to
whatever schema it found instead of assuming this exact history.

**Why.** Twenty tables × up to 31 columns of hand-copied identifiers is
exactly where a transposed column name would slip through review — and
a transposed pair of same-typed TEXT columns would pass every type
check while swapping data. Generation makes the copy list mechanical;
embedding the output keeps the migration inspectable and identical on
every replica, which a runtime-adaptive hook cannot promise
(deterministic bytes are what make replicated migrations reviewable).

## 5. AUTOINCREMENT high-water marks are allowed to regress across deletes

**The call.** `plan_submission`, `rollout`, and `rollout_event` lose
their `sqlite_sequence` rows when the old tables drop; the explicit-id
copies re-seed each to max(id). An id below the pre-migration high-water
mark can be reused iff the largest-id row had already been deleted.

**The alternative.** Restore the exact counters with
`INSERT INTO sqlite_sequence` rows carried across the rebuild.

**The trade-off.** A pruned-then-migrated table can re-mint an id that
existed historically; anything treating those ids as a total order
across deletions would be confused.

**Why.** Nothing in the code does — plan submissions are consumed by id
lookup, rollout ids are foreign-keyed not ordered across pruning — and
the C-2 rebuild already accepted precisely this for the same tables, so
matching the precedent keeps the migration's behavior explainable by
one rule instead of two.

## 6. The eval benchmark stops timing store creation

**The call.** Fixing the benchmark harnesses was folded into this work:
both micro benches leaked one store directory per iteration
(`tempdir().keep()`), and `eval_critical_path_bench` built its store —
running the full migration suite twice — inside the timed iteration.
The stores now ride their `TempDir` guard (dropped outside the timed
section) and the eval bench builds its store in `iter_batched` setup.

**The alternative.** Leave the harness alone for comparability with
every previously recorded number, and accept that this change's
"before/after" would include ~99 more migration statements in the
"after" timing.

**The trade-off.** Absolute eval-bench numbers stop being comparable
with earlier records (they no longer include store setup). Every future
reader comparing across records has to know this seam.

**Why.** The leak filled the disk over a long run — the first after-run
measured filesystem pressure and then died of ENOSPC — and the timed
setup charged every appended migration to a benchmark named
claim/apply/complete latency, which would have indicted this migration
(and every future one) for a cost no production eval ever pays:
migrations run once at startup, not per eval. A benchmark that punishes
schema migrations in perpetuity is measuring the wrong thing;
performance work needs instruments that measure what their names claim.
The seam is recorded in `benchmarks/scheduler/README.md`.

## 7. `STRING`/`TIMESTAMP` re-type: types chosen per column from an audit of what is stored

**The call.** Every `STRING` and `TIMESTAMP` column becomes `TEXT`, except
`job.created_at`, which becomes `INTEGER`. The choice came from running
the whole manager unit suite with its databases kept and recording
`typeof()` of every value in every such column (732 databases): only
`job.created_at` held numbers (unix seconds, read back as `i64`, the same
as its `INTEGER` sibling `job.updated_at`); every other column held text or
NULL.

**The alternatives.** (a) `TEXT` for all, as entry 2 feared. (b) Infer the
type from each column's Rust readers and writers by hand, across ~300
columns. (c) `ANY`/no declared type.

**The trade-off.** The audit sees only what the tests write. A column
production fills with numbers the tests never do would be re-typed TEXT and
its numbers rewritten to text, and a reader taking `i64` would then fail.
The audit found no such column, and a reader of that kind already fails
today on any text value.

**Why (correctness, cheaply proven).** (a) would have broken every job read
(`job_from_row` takes `created_at` as `i64`). (b) is the same answer at a
hundred times the cost and with more room for a miss. (c) keeps whatever
type the writer bound, which is the inconsistency this removes.

## 8. Declared types are rewritten in place (`writable_schema`), not by rebuilding tables

**The call.** One migration hook edits each table's `CREATE TABLE` text in
`sqlite_schema` (a quote-aware scanner replaces only the type token after a
column name), bumps the schema cookie, then rewrites stored numbers to text
in those columns, `REINDEX`es the tables it touched, and runs
`foreign_key_check`, all in one transaction.

**The alternatives.** (a) Twin-table rebuilds of every affected table, as
entry 1 did for JSON columns (about 60 tables, with the `job_config`
cascade dance of entry 3 repeated for every parent). (b) Add/copy/drop
column surgery.

**The trade-off.** `writable_schema` is a sharp tool: a malformed rewrite
corrupts the schema. SQLite documents it for changes that do not alter
stored records, which a type change is not quite (index entries carry the
old values), hence the `REINDEX` of every table whose values changed and
the integrity, foreign-key and index-lookup assertions in
`string_and_timestamp_columns_become_text_with_their_values`.

**Why (simplicity, and the wire format).** Column order and rowids are
untouched, so raft changesets (positional, entry 1) are unaffected, and no
foreign key is dropped and recreated, so no cascade can fire. (a) is
hundreds of lines of generated SQL whose own risk entry 3 describes; (b)
reorders columns.

## 9. The re-type migration runs with foreign-key enforcement off, and checks keys itself

**The call.** `migrate_to_latest` runs every migration with enforcement on
except the re-type, which it runs with enforcement off; the hook then
checks every key with `foreign_key_check` and fails the transaction on any
violation. Called with enforcement on and numbers to rewrite, the hook
refuses.

**The alternatives.** (a) Enforcement on with `defer_foreign_keys`. (b)
Rewrite values after the migration commits, on a second connection with
enforcement off.

**The trade-off.** One migration is special-cased in the store's startup.

**Why (correctness).** (a) fails: SQLite's deferred-constraint counter
miscounts a key whose comparison changes mid-transaction (a parent rewritten
from `2026` to `'2026'` while a child still holds either form), and the
commit fails on a database `foreign_key_check` finds consistent; which
statement orders survive depends on the table graph (reproduced on a
three-table replica). (b) leaves a window where a crash after the type
change and before the value rewrite strands numbers that text lookups
cannot find. Off-then-check keeps the change atomic and the guarantee the
same.

