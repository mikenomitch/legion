# Plan: Slop-Fork Litestream into a Rust HA Library (`rustyriver`)

## Status and Goal

**Goal:** Port [Litestream](https://github.com/benbjohnson/litestream) (Go) into a
standalone, embeddable **Rust library** — **`rustyriver`** — so Legion can do
streaming SQLite replication and failover **in-process**, without shelling out
to the external `litestream` binary.

**Why now:** Legion already specs streaming persistence in
[`docs/persistence-streaming.md`](../persistence-streaming.md) and
[`docs/persistence-plan.md`](../persistence-plan.md), but both assume an external
`litestreamBinary` on the host `PATH`. That is operationally awkward for a
"single binary that runs on edge devices" (see `README.md`). An embedded Rust
crate removes the runtime dependency, lets the manager control replication
lifecycle directly, and lets us surface replication state through the existing
`Store` / status APIs and MCP surface.

**Where this lives:** This *plan* lives in the Legion repo (`docs/plans/`). The
*code* lives in a **new repository**, `rustyriver` (crate name `rustyriver`),
consumed by Legion as a path/git/crates.io dependency.

> Naming note: "slop-fork" / "vibe-fork" is the
> [Cloudflare-`vinext` style](https://mbleigh.dev/posts/slop-forks/) practice of
> using AI agents to re-implement an existing project in a new language/runtime
> while preserving behavior and using the original test suite as the
> correctness oracle. This plan applies those practices deliberately.

---

## 1. Slop-Fork Best Practices (vinext-derived)

From the Cloudflare `vinext` (Vite-powered Next.js) effort and the surrounding
commentary, the practices that map onto this fork:

1. **Compatibility over elegance — first.** The objective is behavioral parity
   (pass the ported tests, read/write the LTX on-disk + object-store format),
   *not* idiomatic-Rust beauty on the first pass. Clean up later. The Cloudflare
   lead was explicit that the goal was "compatibility, pass tests, verify
   feasibility," not perfect engineering.
2. **The original test suite is the spec.** Litestream's `*_test.go` files +
   `tests/` integration suite are the oracle. Port them *before/along with* the
   code they exercise so every Rust module lands with its Go test translated.
3. **Refactor generated code into typed, linted modules.** Don't leave
   machine-translated blobs. Each ported file must pass `cargo fmt` +
   `cargo clippy -D warnings` (Legion's bar per `AGENTS.md`) before it's "done."
4. **Rich agent context wins.** Give porting agents the Go source, the Go test,
   the LTX format/byte-layout docs, and a Rust style guide in-context. `vinext`
   reported ~20% quality lift from good context (Context7/Exa-style retrieval);
   we replicate that by pinning reference material into each subagent prompt.
5. **The last 6% is the real work.** API/format coverage gets to ~94% fast; the
   remaining edge cases (checksum mismatches, partial-LTX recovery, retention
   races, crash consistency) are where years of hardening live. Budget the
   majority of *review* effort there, and lean on fuzz/property tests.

Sources:
[Welcome to the age of the Slop Fork (mbleigh.dev)](https://mbleigh.dev/posts/slop-forks/) ·
[Are AI Slop Forks Killing Software? (builder.io)](https://www.builder.io/blog/ai-slop-forks) ·
[Cloudflare's Next.js Slop Fork (Syntax #988)](https://syntax.fm/show/988/cloudflare-s-next-js-slop-fork) ·
[36kr writeup](https://eu.36kr.com/en/p/3749387382522631)

---

## 2. Litestream Has Two Architectures — We Target Only the Modern One

Litestream has two fundamentally different designs. The public "how it works"
page documents the **old** one; the current `main` branch is the **new** one.

### v0.3.x — "classic" (shadow WAL + generations) — **NOT ported**
- **Shadow WAL:** raw SQLite WAL frames copied into a staged file sequence
  (`00000000.wal`, …).
- **Generations:** a 16-char hex ID names one contiguous snapshot+WAL chain; any
  continuity break starts a brand-new generation with a fresh snapshot.
- **Restore:** replay a generation's snapshot + contiguous WAL; point-in-time is
  awkward because you reason across generations.
- **No compaction hierarchy** — WAL segments accumulate until retention GC.

### v0.5.x — "modern" (LTX + compaction levels) — **the port target**
- **LTX (Litestream Transaction) files** replace raw WAL segments. Each carries a
  **monotonic transaction ID (TXID)**. (Same format family as LiteFS / the `ltx`
  crate.)
- **Generations are gone.** On a continuity break, Litestream re-snapshots as the
  next LTX file. TXIDs are globally ordered, so "DB state at txid X" is a direct
  lookup — no cross-generation hunting.
- **Compaction levels:** L1 = 30s windows, L2 = 5min, L3 = hourly. Small frequent
  files roll up into fewer larger ones, bounding restore cost and enabling fast
  point-in-time recovery.
- **Live read replicas via a SQLite VFS** (a standby serves reads streamed from
  the primary).
- **Not backward compatible:** v0.5 cannot restore v0.3 WAL segments. v0.5.7+
  added a separate `ReplicaClientV3` shim *only* to read legacy v0.3 backups.

### Decision: skip v0.3 entirely, build on LTX from day one

We are **greenfield** — there are no existing Litestream backups to restore — so
the entire v0.3 shadow-WAL/generation machinery and the `ReplicaClientV3`
migration shim are dead weight. `main` *is* v0.5, so the Go source and
`*_test.go` files we port are already the LTX implementation; porting v0.3 first
would mean translating an older codebase we'd immediately throw away, with tests
that don't match upstream. The LTX model (monotonic TXIDs, no generation
rollover state machine) is also arguably *simpler to implement correctly*.

Sources:
[Litestream v0.5.0 is Here (Fly blog)](https://fly.io/blog/litestream-v050-is-here/) ·
[Simon Willison's notes](https://simonwillison.net/2025/Oct/3/litestream/) ·
[Litestream VFS (Fly blog)](https://fly.io/blog/litestream-vfs/) ·
[Migration Guide](https://litestream.io/docs/migration/)

---

## 3. Legion's Actual HA Use Case

Legion's manager keeps cluster state in a single SQLite file in WAL mode
(`docs/state-store.md`): one write connection, a read pool, an in-memory
scheduling cache. HA requirements:

- **Single-primary writer**, async LTX shipping to object storage (R2/S3) with
  small RPO. (Matches `persistence-streaming.md` guarantees.)
- **Crash/host-loss recovery:** restore the manager DB from object storage on a
  fresh node.
- **Failover fencing:** an object-storage lease so only one manager is primary
  (already specced; Litestream's `leaser` is the reference implementation).
- **Edge-friendly:** no extra binary, no CGo, cross-platform (Mac/Linux/Win).

This determines what we keep vs. drop.

---

## 4. Scope: Keep / Defer / Drop

### KEEP — required for Legion HA (Milestone 1–2)

| Area | Go source | Why |
|------|-----------|-----|
| Core DB lifecycle & WAL capture | `db.go`, `litestream.go` | Checkpoint takeover, frame capture |
| WAL frame parsing + checksums | `wal_reader.go` | Correctness-critical; must be byte-exact |
| **LTX read/write + TXID handling** | `v3.go`, `ltx` usage | The modern on-disk/object format — the core |
| Replica coordination (single replica) | `replica.go`, `replica_client.go` | Drives the sync loop |
| Replica URL parsing | `replica_url.go` | Config: `s3://…`, `file://…` |
| **S3-compatible client** | `s3/` | R2/S3/MinIO — Legion's primary target |
| **File client** | `file/` | Local/NFS replica + dev fixtures |
| Store / snapshot bookkeeping | `store.go` | TXID/snapshot tracking, retention |
| Restore path | `db.go`/`replica.go` + `restore` | Failover recovery |
| Lease-based fencing | `leaser.go` | Maps to Legion lease config |
| Log/util glue | `log.go`, `internal/` | Support |

### DEFER — valuable, not Milestone 1 (behind cargo features)

| Area | Go source | Note |
|------|-----------|------|
| **Compaction levels (L1/L2/L3)** | `compactor.go`, `compaction_level.go` | **MVP ships L0-only** (every txn = one small LTX file, no roll-up): replicates and restores correctly, just slower to restore on a long-lived DB. Add compaction in M2 to bound restore cost. |
| Live read replicas (VFS + server) | `vfs.go`, `server.go` | Lets a standby serve reads — useful for read-scaling later, **not needed for DR/failover**. |

### DROP — not needed for Legion (re-addable behind cargo features later)

| Area | Go source | Reason |
|------|-----------|--------|
| **All v0.3 shadow-WAL + generations machinery** | (v0.3 paths) | Greenfield; nothing to be compatible with |
| **`ReplicaClientV3` v0.3 restore shim** | `v3` client path | Only reads legacy v0.3 backups we don't have |
| GCS client | `gs/` | R2/S3 only in Legion's targets |
| Azure Blob client | `abs/` | Same |
| Alibaba OSS client | `oss/` | Same |
| SFTP client | `sftp/` | Same |
| WebDAV client | `webdav/` | Same |
| NATS client | `nats/` | Same |
| **Multi-replica fan-out** | replica orchestration | Litestream ships to N replicas at once; Legion needs **one** object-store target. Model a single `ReplicaClient`, skip the fan-out. |
| **Built-in Prometheus `/metrics` + subscriber plumbing** | metrics/subscriber | Legion has OTEL; emit `tracing` spans instead. |
| Standalone CLI | `cmd/litestream/*` | Legion drives it as a library. A thin debug CLI is optional, not a product. |

> Dropped clients are cheap to re-add behind cargo features (`feature = "gcs"`,
> …) — the `ReplicaClient` trait keeps the seam clean. The `object_store` crate
> (see §5) actually gives several of them back for near-free if demand appears.

---

## 5. Target Crate Design (`rustyriver`)

Map Go packages → Rust modules, async on **Tokio**, object IO via the
**`object_store`** crate (S3/R2/file behind one trait; other clouds re-enable by
feature flag for near-free).

```
rustyriver/
├── Cargo.toml                # tokio, object_store, bytes, crc, thiserror, tracing
├── src/
│   ├── lib.rs                # public API: Db, Replica, Config, restore()
│   ├── db.rs                 # <- db.go: checkpoint takeover, WAL capture loop
│   ├── wal.rs                # <- wal_reader.go: frame parse + checksum
│   ├── ltx.rs                # <- v3.go + ltx: LTX read/write, TXID handling (CORE)
│   ├── replica.rs            # <- replica.go: single-replica sync loop, restore
│   ├── client/
│   │   ├── mod.rs            # ReplicaClient trait <- replica_client.go
│   │   ├── object_store.rs   # S3/R2 via `object_store` <- s3/
│   │   └── file.rs           # local fs <- file/
│   ├── store.rs              # <- store.go: snapshot/TXID bookkeeping, retention
│   ├── replica_url.rs        # <- replica_url.go
│   ├── leaser.rs             # <- leaser.go: object-storage lease/fencing
│   ├── compactor.rs          # (DEFER, M2) <- compactor.go + compaction_level.go
│   ├── vfs.rs / server.rs    # (DEFER, M4) live read replicas
│   └── error.rs              # thiserror error model
└── tests/                    # ported integration tests + fixtures
```

**Public API sketch (what Legion calls):**

```rust
let db = rustyriver::Db::open("/var/lib/legion/store.sqlite", cfg).await?;
db.start_replication().await?;            // checkpoint takeover + LTX sync loop
// failover on a fresh node:
rustyriver::restore(&replica_cfg, "/var/lib/legion/store.sqlite").await?;
// fencing:
let lease = rustyriver::Leaser::new(obj_cfg).acquire().await?;
```

**Legion integration:** implement the existing `PersistenceDriver` trait
(`docs/persistence-plan.md`, `src/storage/persistence.rs`) backed by `rustyriver`
instead of spawning `litestreamBinary`. The `streaming` config block in
`persistence-streaming.md` stays the same minus `litestreamBinary`.

**Hard correctness constraints:**
- WAL frame + page checksums must be **byte-identical** to SQLite/Litestream
  (big/little-endian salt handling). This is the #1 source of subtle bugs.
- LTX files must be **format-valid** (TXID ordering, page framing, checksums) so
  the upstream `litestream` CLI can restore them as a break-glass tool — *if* we
  commit to that (decide in M1, see Open Questions).

---

## 6. Test Porting Plan (the spec)

Every Go test becomes a Rust test, in this order (correctness foundation first).
Note: because we target v0.5, the LTX tests are first-class, and v0.3-only tests
are skipped.

### Tier 1 — Foundation (port first, blocks everything)

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `wal_reader_test.go` | `wal.rs` unit tests | WAL header/frame parse, checksum algorithm, salt rotation, partial-frame handling. **Highest-value port.** |
| `v3_test.go` | `ltx.rs` tests | LTX file read/write, TXID ordering, framing, checksums |
| `replica_url_test.go` | `replica_url.rs` tests | `s3://`, `file://`, path parsing edge cases |
| `litestream_test.go` | `lib.rs`/`db.rs` tests | Top-level helpers, position/offset math |

### Tier 2 — Core DB + replication

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `db_test.go` | `db.rs` tests | Open, checkpoint takeover, LTX capture, snapshot-on-continuity-break |
| `db_internal_test.go` | `db.rs` `#[cfg(test)]` | Internal invariants (offsets, sync state) |
| `db_shutdown_test.go` | `db.rs` tests | Clean shutdown releases read-tx, no corruption, resume after restart |
| `store_test.go` | `store.rs` tests | Snapshot/TXID tracking, retention selection |
| `replica_test.go` | `replica.rs` tests | Sync loop, snapshot creation, end-to-end replicate→restore round trip |
| `replica_internal_test.go` | `replica.rs` tests | Internal sync-position bookkeeping |

### Tier 3 — Replica client conformance

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `replica_client_test.go` | `client/mod.rs` test harness | **Client conformance suite** run against *every* client impl (write/read/list/delete LTX+snapshot objects, ordering, pagination). Port as a generic `async fn run_client_suite(client)` and run it for `file` and `object_store`(MinIO). |
| `s3/` package tests | `client/object_store.rs` tests | S3 key layout, multipart, error mapping — run against **MinIO** in CI |
| `file/` package tests | `client/file.rs` tests | Local fs layout, atomic rename, missing-dir handling |

### Tier 4 — Recovery hardening (the critical 6%)

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `restore_fuzz_test.go` | `tests/restore_fuzz.rs` via `cargo-fuzz`/`proptest` | Randomized LTX sequences restore to a valid, expected DB. **Port as property tests; this catches the edge cases.** |
| `tests/` integration suite | `tests/integration_*.rs` | Full replicate-then-restore against file + MinIO, crash-in-the-middle, snapshot-on-continuity-break, retention GC |

### Tier 5 — Deferred features (port with the feature)

| Go test | Ports to | Note |
|---------|----------|------|
| `compactor_test.go`, `store_compaction_remote_test.go` | `compactor.rs` tests | M2, when L1/L2/L3 compaction is implemented |
| `vfs_test.go`, `vfs_write_test.go`, `vfs_compaction_test.go` | `vfs.rs` tests | M4, with read-replica VFS |
| `server_test.go` | `server.rs` tests | M4, with VFS |
| `heartbeat_test.go` | `replica.rs`/`leaser.rs` tests | Liveness; pair with leaser |

**Skipped Go tests:** any v0.3-only shadow-WAL/generation tests and
`ReplicaClientV3` tests — out of scope by the §2 decision.

**Additional Rust-native tests to add (not in Go but needed):**
- LTX/checksum **golden vectors**: capture real Litestream-produced LTX bytes as
  fixtures under `tests/fixtures/` and assert our parser/serializer matches.
- **Cross-tool compat test** (if wire-compat is chosen): replicate with
  `rustyriver`, restore with the real `litestream` binary, and vice-versa.
- Concurrency/loom-style test for checkpoint-takeover read-tx lifecycle.

---

## 7. Port Execution Plan — Claude Agents + Dynamic Workflows

This is a textbook **dynamic-workflow** job: a large, mostly-parallel migration
where the orchestration is worth codifying and rerunning, and where adversarial
review of each port materially improves trust.
([Claude Code: Orchestrate subagents at scale](https://code.claude.com/docs/en/workflows))

### Why a workflow (not turn-by-turn subagents)

- Dozens of files to port, each an independent unit → fan-out across up to **16
  concurrent agents** (1,000 total/run cap).
- We want the **plan in code**: the same script reruns as upstream evolves.
- Intermediate results (per-file ports) stay in script variables, not in the
  orchestrator's context — only the final report lands back.
- We can encode the slop-fork quality gate: **a porting agent and an independent
  adversarial reviewer agent per file**, so a port isn't "done" until a second
  agent fails to refute it.

### Workflow shape (the script Claude writes when you say "run a workflow to…")

```
Phase 0 — Bootstrap (1 agent)
  • Scaffold the rustyriver repo: Cargo.toml, module skeleton, error.rs,
    ReplicaClient trait, empty test files mirroring the Go suite (LTX-only).
  • Vendor the Go v0.5 source + upstream tests into a reference dir for context.

Phase 1 — Foundation port (sequential, gated)   [Tier 1 tests]
  • wal.rs + ltx.rs MUST be green before fan-out (everything depends on correct
    frame/checksum + LTX framing).
  • replica_url.rs, lib.rs helpers.
  • Gate: cargo test on foundation passes, fixtures match golden vectors.

Phase 2 — Core fan-out (parallel, ≤16 agents)    [Tier 2 + 3]
  For each unit { db, store, replica, client/file, client/object_store }:
    a) Porter agent: translate Go file + its _test.go to Rust, make it compile
       and pass in isolation (mock the rest behind the trait).
    b) Adversarial reviewer agent (fresh context): given the Go original + the
       Rust port, hunts a behavioral divergence (checksum endianness, off-by-one
       offsets, TXID ordering, error mapping, retention boundaries). Writes a
       failing test if it finds one.
    c) Fixer agent: resolves reviewer findings. Loop a/b until reviewer can't
       refute (cap N iterations, else escalate to report).

Phase 3 — Integration assembly (sequential)      [Tier 4]
  • Wire modules together (L0-only, no compaction); port tests/ integration
    suite + restore fuzz/property tests; stand up MinIO in CI; run
    replicate→restore round trips and crash-in-the-middle cases.

Phase 4 — Adversarial recovery sweep (parallel, the critical 6%)
  • Several agents generate hostile scenarios (truncated LTX, missed frames →
    snapshot-on-continuity-break, clock skew on lease, partial multipart upload,
    retention GC racing a restore) and assert recovery correctness.

Phase 5 — Legion integration (sequential)
  • Implement PersistenceDriver in Legion against rustyriver; swap out
    litestreamBinary; run Legion's own cargo test --all --locked.

Phase 6 — Synthesis (1 agent)
  • Coverage report: every Go test → Rust test status (ported/deferred/skipped).
  • List unported edge cases + risk notes. Returns one report.
```

### Operational notes for running it

- Trigger with the literal word **`workflow`** in the prompt, or `/effort
  ultracode` to let Claude orchestrate automatically per task.
- Pre-add the agents' needed commands (`cargo`, `git`, MinIO/docker) to the
  **tool allowlist** before launch — workflow subagents run in `acceptEdits` but
  un-allowlisted shell/MCP calls still prompt mid-run.
- Route mechanical translation stages to a **smaller model** (Haiku/Sonnet) and
  reserve Opus for the adversarial recovery sweep (Phase 4) to manage cost.
- **Save the run** (`/workflows` → `s`) as `/port-litestream` so re-syncing with
  upstream Litestream changes is one command.
- Use a **git worktree per porter agent** (isolation) so parallel ports don't
  collide; the orchestrator merges green units.

### Quality gate per ported file (the slop-fork bar)

1. Compiles, `cargo fmt`, `cargo clippy -D warnings` clean.
2. Its ported Go test passes.
3. An independent reviewer agent could not produce a failing divergence test.
4. For byte-format code: matches captured golden vectors.

---

## 8. Milestones

| M | Deliverable | Scope |
|---|-------------|-------|
| **M0** | `rustyriver` repo scaffold + v0.5 reference vendoring + workflow script saved | Phase 0–1 |
| **M1** | DR-capable crate: S3/R2 + file replicate & restore on **LTX, L0-only (no compaction)**, single replica, Tier 1–4 tests green | Phases 1–4 |
| **M2** | **Compaction levels (L1/L2/L3)** to bound restore cost; Tier 5 compaction tests. Legion uses the crate via `PersistenceDriver`; `litestreamBinary` removed; lease fencing wired | Phase 5 + compaction |
| **M3** | Live read replicas (VFS + server) for standby read-scaling | DEFER set |
| **M4** | Re-enable dropped clients behind cargo features as demand appears | gcs/abs/oss/sftp/webdav |

Legion gets real HA value at **M1** (DR) and full operational parity at **M2**
(bounded restore + binary removed). M3+ are upside.

---

## 9. Risks & Mitigations

- **Checksum / endianness bugs (the classic Litestream footgun).** → Golden
  vectors + cross-tool compat tests + dedicate the adversarial sweep here.
- **LTX format fidelity:** TXID ordering, page framing, and checksums must match
  so the format is valid (and CLI-restorable if we choose wire-compat). → Tier 1
  `ltx.rs` tests + golden vectors gate the whole fan-out.
- **CGo/SQLite interop:** Go uses mattn/SQLite; Rust uses `rusqlite`
  (bundled SQLite) — checkpoint-takeover semantics (holding a read tx, manual
  `PRAGMA wal_checkpoint`) must be reproduced exactly. → Tier 2 `db` tests +
  loom-style concurrency test.
- **L0-only restore cost in M1:** a long-lived DB replays many small LTX files
  on restore. → Acceptable for M1 DR; M2 compaction fixes it. Document the
  tradeoff so operators don't deploy M1 to huge DBs.
- **Wire-compat decision drift:** if we *don't* commit to Litestream's LTX
  object layout we lose the `litestream restore` break-glass tool. → Decide in
  M1 (Open Questions) and write it down.
- **AI-translation "looks right, isn't":** the whole point of the per-file
  adversarial reviewer + fuzz/property tests. Don't merge a unit on porter
  confidence alone.
- **Upstream moves (v0.5 actively developed):** pin a specific upstream v0.5
  commit as the port target; the saved workflow re-runs against new commits.

---

## 10. Open Questions (decide before/at M1)

1. **Wire compatibility:** Must `rustyriver`'s LTX object layout be restorable by
   the upstream `litestream` CLI (break-glass restore)? If yes, match the LTX
   directory/key layout exactly; if no, we get design freedom but lose the
   external safety net. (Recommended: yes — it's a cheap insurance policy.)
2. **SQLite binding:** `rusqlite` (bundled, simplest) vs `libsqlite3-sys` shared
   — affects the "single static binary on edge" goal. Lean `rusqlite` bundled.
3. **Object IO:** confirm `object_store` crate (recommended — free multi-cloud)
   vs hand-rolled `aws-sdk-s3` (more control, more code).
4. **LTX crate vs hand-roll:** is there a usable Rust `ltx` crate to depend on,
   or do we port the format ourselves? Porting ourselves keeps zero-dependency
   control over the most correctness-critical code.
5. **Crate license:** Litestream is Apache-2.0; `rustyriver` must carry
   Apache-2.0 + NOTICE attribution. Confirm acceptable for Legion's licensing.
```
