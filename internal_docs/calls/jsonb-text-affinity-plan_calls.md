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
