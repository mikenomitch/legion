# Plan: Slop-Fork Litestream into a Rust HA Library (`litestream-rs`)

## Status and Goal

**Goal:** Port [Litestream](https://github.com/benbjohnson/litestream) (Go) into a
standalone, embeddable **Rust library** so Legion can do streaming SQLite
replication and failover **in-process**, without shelling out to the external
`litestream` binary.

**Why now:** Legion already specs streaming persistence in
[`docs/persistence-streaming.md`](../persistence-streaming.md) and
[`docs/persistence-plan.md`](../persistence-plan.md), but both assume an external
`litestreamBinary` on the host `PATH`. That is operationally awkward for a
"single binary that runs on edge devices" (see `README.md`). An embedded Rust
crate removes the runtime dependency, lets the manager control replication
lifecycle directly, and lets us surface replication state through the existing
`Store` / status APIs and MCP surface.

**Where this lives:** This *plan* lives in the Legion repo (`docs/plans/`). The
*code* lives in a **new repository** (proposed name `litestream-rs`, crate
`litestream`), consumed by Legion as a path/git/crates.io dependency.

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
   (pass the ported tests, read/write a wire-compatible on-disk + object-store
   format), *not* idiomatic-Rust beauty on the first pass. Clean up later. The
   Cloudflare lead was explicit that the goal was "compatibility, pass tests,
   verify feasibility," not perfect engineering.
2. **The original test suite is the spec.** Litestream's `*_test.go` files +
   `tests/` integration suite are the oracle. Port them *before/along with* the
   code they exercise so every Rust module lands with its Go test translated.
3. **Refactor generated code into typed, linted modules.** Don't leave
   machine-translated blobs. Each ported file must pass `cargo fmt` +
   `cargo clippy -D warnings` (Legion's bar per `AGENTS.md`) before it's "done."
4. **Rich agent context wins.** Give porting agents the Go source, the Go test,
   the format/byte-layout docs, and a Rust style guide in-context. `vinext`
   reported ~20% quality lift from good context (Context7/Exa-style retrieval);
   we replicate that by pinning reference material into each subagent prompt.
5. **The last 6% is the real work.** API/format coverage gets to ~94% fast; the
   remaining edge cases (checksum mismatches, partial-WAL recovery, retention
   races, crash consistency) are where years of hardening live. Budget the
   majority of *review* effort there, and lean on fuzz/property tests.

Sources:
[Welcome to the age of the Slop Fork (mbleigh.dev)](https://mbleigh.dev/posts/slop-forks/) ·
[Are AI Slop Forks Killing Software? (builder.io)](https://www.builder.io/blog/ai-slop-forks) ·
[Cloudflare's Next.js Slop Fork (Syntax #988)](https://syntax.fm/show/988/cloudflare-s-next-js-slop-fork) ·
[36kr writeup](https://eu.36kr.com/en/p/3749387382522631)

---

## 2. Litestream Architecture Primer

Litestream runs beside a SQLite DB and continuously ships WAL frames to a
replica (object store / file). Two generations of the design exist:

### Classic model (v0.3.x) — well-documented
([litestream.io/how-it-works](https://litestream.io/how-it-works/))

- **Checkpoint takeover:** holds a long-running read transaction so no other
  process can checkpoint/restart the WAL.
- **Shadow WAL:** copies new WAL pages into a staged sequence
  (`00000000.wal`, `00000001.wal`, …) preserving frames + checksums.
- **Generations:** a 16-char hex id naming a contiguous snapshot+WAL set; a new
  generation starts whenever continuity is lost (restart, missed frames).
- **Snapshots + retention/compaction:** periodic full snapshots; old
  snapshots/WAL past the retention window (default 24h) are GC'd.
- **Restore:** replay snapshot + contiguous WAL files to a point in time.
- **Async replication:** default ~1s sync interval → small non-zero RPO.

### Modern model (v0.5.x) — what the current `main` actually is

The current repo (files: `compactor.go`, `compaction_level.go`, `v3.go`,
`vfs.go`, `server.go`, `leaser.go`, `wal_reader.go`) is the newer architecture:

- **LTX transaction files** replace the raw shadow-WAL sequence (Litestream
  shares the LTX format with the `ltx` library).
- **Compaction levels** roll many small LTX files into fewer larger ones
  (L0→L1→L2…), bounding restore cost.
- **Live read replicas via a SQLite VFS + HTTP server** (`vfs.go`,
  `server.go`) — a standby can serve reads streamed from the primary.
- **Leaser** (`leaser.go`) — lease-based single-primary coordination, which maps
  exactly onto Legion's `lease.provider = "object_storage"` fencing model.

**Decision point (see §4):** target the **modern LTX model** for the long term,
but the *first milestone* implements the **classic snapshot+WAL restore path**
because it is the minimal slice that gives Legion disaster recovery, and it's
the best-documented for correctness checking.

---

## 3. Legion's Actual HA Use Case

Legion's manager keeps cluster state in a single SQLite file in WAL mode
(`docs/state-store.md`): one write connection, a read pool, an in-memory
scheduling cache. HA requirements:

- **Single-primary writer**, async WAL shipping to object storage (R2/S3) with
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
| Core DB lifecycle & WAL capture | `db.go`, `litestream.go` | The heart: checkpoint takeover, frame capture |
| WAL frame parsing + checksums | `wal_reader.go` | Correctness-critical; must be byte-exact |
| Replica coordination | `replica.go`, `replica_client.go` | Drives sync loop |
| Replica URL parsing | `replica_url.go` | Config: `s3://…` etc. |
| **S3-compatible client** | `s3/` | R2/S3/MinIO — Legion's primary target |
| **File client** | `file/` | Local/NFS replica + tests + dev fixtures |
| Store / generations / snapshots | `store.go` | Generation + snapshot bookkeeping |
| Restore path | (in `db.go`/`replica.go` + `cmd/.../restore.go`) | Failover recovery |
| Lease-based fencing | `leaser.go` | Maps to Legion lease config |
| Log/util glue | `log.go`, `internal/` | Support |

### DEFER — modern features, valuable but not Milestone 1

| Area | Go source | Note |
|------|-----------|------|
| LTX format + compaction levels | `compactor.go`, `compaction_level.go`, `v3.go` | Adopt for v2 of the crate; reduces restore cost |
| Live read replicas (VFS) | `vfs.go`, `server.go` | Lets a standby serve reads — nice for Legion read-scaling later, not needed for DR |

### DROP — not needed for Legion

| Area | Go source | Reason |
|------|-----------|--------|
| GCS client | `gs/` | Not in Legion's target deployment (R2/S3 only) |
| Azure Blob client | `abs/` | Same |
| Alibaba OSS client | `oss/` | Same |
| SFTP client | `sftp/` | Same |
| WebDAV client | `webdav/` | Same |
| NATS client | `nats/` | Same |
| Standalone CLI | `cmd/litestream/*` | Legion drives it as a library; we may keep a thin debug CLI but it's not a product |

> Dropped clients are cheap to re-add later behind cargo features
> (`feature = "gcs"`, …) — the `ReplicaClient` trait keeps the seam clean.

---

## 5. Target Crate Design (`litestream-rs`)

Map Go packages → Rust modules, async on **Tokio**, object IO via the
**`object_store`** crate (gives S3/R2/file/GCS/Azure behind one trait — lets us
keep only S3+file enabled and re-enable others by feature flag for near-free).

```
litestream-rs/
├── Cargo.toml                # tokio, object_store, bytes, crc, thiserror, tracing
├── src/
│   ├── lib.rs                # public API: Db, Replica, Config, restore()
│   ├── db.rs                 # <- db.go: checkpoint takeover, WAL capture loop
│   ├── wal.rs                # <- wal_reader.go: frame parse + checksum (no_std-able core)
│   ├── replica.rs            # <- replica.go: sync loop, snapshot/restore orchestration
│   ├── client/
│   │   ├── mod.rs            # ReplicaClient trait <- replica_client.go
│   │   ├── object_store.rs   # S3/R2 via `object_store` <- s3/
│   │   └── file.rs           # local fs <- file/
│   ├── store.rs              # <- store.go: generations, snapshots, retention
│   ├── replica_url.rs        # <- replica_url.go
│   ├── leaser.rs             # <- leaser.go: object-storage lease/fencing
│   ├── compactor.rs          # (DEFER) <- compactor.go + compaction_level.go
│   ├── vfs.rs / server.rs    # (DEFER) live read replicas
│   └── error.rs              # thiserror error model
└── tests/                    # ported integration tests + fixtures
```

**Public API sketch (what Legion calls):**

```rust
let db = litestream::Db::open("/var/lib/legion/store.sqlite", cfg).await?;
db.start_replication().await?;            // checkpoint takeover + sync loop
// failover on a fresh node:
litestream::restore(&replica_cfg, "/var/lib/legion/store.sqlite").await?;
// fencing:
let lease = litestream::Leaser::new(obj_cfg).acquire().await?;
```

**Legion integration:** implement the existing `PersistenceDriver` trait
(`docs/persistence-plan.md`, `src/storage/persistence.rs`) backed by this crate
instead of spawning `litestreamBinary`. The `streaming` config block in
`persistence-streaming.md` stays the same minus `litestreamBinary`.

**Hard correctness constraints:**
- WAL frame + page checksums must be **byte-identical** to SQLite/Litestream
  (big/little-endian salt handling). This is the #1 source of subtle bugs.
- On-object-store **layout must be Litestream-readable** if we want the existing
  `litestream restore` CLI to work as a break-glass tool. (Decide explicitly in
  M1 — see Open Questions. If we don't need wire-compat, we get freedom but lose
  the external safety net.)

---

## 6. Test Porting Plan (the spec)

Every Go test becomes a Rust test. Litestream tests fall into unit, golden/byte,
integration, and fuzz buckets. Port them in this order (correctness foundation
first):

### Tier 1 — Foundation (port first, blocks everything)

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `wal_reader_test.go` | `wal.rs` unit tests | WAL header/frame parse, checksum algorithm, salt rotation, partial-frame handling. **Highest-value port.** |
| `replica_url_test.go` | `replica_url.rs` tests | `s3://`, `file://`, path parsing edge cases |
| `litestream_test.go` | `lib.rs`/`db.rs` tests | Top-level helpers, position/offset math |

### Tier 2 — Core DB + replication

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `db_test.go` | `db.rs` tests | Open, checkpoint takeover, shadow-WAL/LTX capture, generation creation |
| `db_internal_test.go` | `db.rs` `#[cfg(test)]` | Internal invariants (offsets, sync state) |
| `db_shutdown_test.go` | `db.rs` tests | Clean shutdown releases read-tx, no corruption, resume after restart |
| `store_test.go` | `store.rs` tests | Generation/snapshot tracking, retention selection |
| `replica_test.go` | `replica.rs` tests | Sync loop, snapshot creation, end-to-end replicate→restore round trip |
| `replica_internal_test.go` | `replica.rs` tests | Internal sync-position bookkeeping |

### Tier 3 — Replica client conformance

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `replica_client_test.go` | `client/mod.rs` test harness | **Client conformance suite** run against *every* client impl (write/read/list/delete WAL+snapshot objects, ordering, pagination). Port as a generic `async fn run_client_suite(client)` and run it for `file` and `object_store`(MinIO). |
| `s3/` package tests | `client/object_store.rs` tests | S3 key layout, multipart, error mapping — run against **MinIO** in CI |
| `file/` package tests | `client/file.rs` tests | Local fs layout, atomic rename, missing-dir handling |

### Tier 4 — Recovery hardening (the critical 6%)

| Go test | Ports to | What it asserts |
|---------|----------|-----------------|
| `restore_fuzz_test.go` | `tests/restore_fuzz.rs` via `cargo-fuzz`/`proptest` | Randomized snapshot+WAL sequences restore to a valid, expected DB. **Port as property tests; this catches the edge cases.** |
| `tests/` integration suite | `tests/integration_*.rs` | Full replicate-then-restore against file + MinIO, crash-in-the-middle, generation rollover, retention GC |

### Tier 5 — Deferred features (port with the feature)

| Go test | Ports to | Note |
|---------|----------|------|
| `compactor_test.go`, `store_compaction_remote_test.go` | `compactor.rs` tests | Only when LTX/compaction is implemented |
| `vfs_test.go`, `vfs_write_test.go`, `vfs_compaction_test.go` | `vfs.rs` tests | Only when read-replica VFS is implemented |
| `server_test.go` | `server.rs` tests | With VFS |
| `heartbeat_test.go` | `replica.rs`/`leaser.rs` tests | Liveness; pair with leaser |
| `v3_test.go` | `v3.rs` tests | LTX format |

**Additional Rust-native tests to add (not in Go but needed):**
- Checksum **golden vectors**: capture real Litestream-produced WAL/LTX bytes as
  fixtures under `tests/fixtures/` and assert our parser/serializer matches.
- **Cross-tool compat test** (if wire-compat is chosen): replicate with the Rust
  crate, restore with the real `litestream` binary, and vice-versa.
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
- We want the **plan in code**: the same script reruns as the upstream evolves.
- Intermediate results (per-file ports) stay in script variables, not in the
  orchestrator's context — only the final report lands back.
- We can encode the slop-fork quality gate: **a porting agent and an independent
  adversarial reviewer agent per file**, so a port isn't "done" until a second
  agent fails to refute it.

### Workflow shape (the script Claude writes when you say "run a workflow to…")

```
Phase 0 — Bootstrap (1 agent)
  • Scaffold litestream-rs repo: Cargo.toml, module skeleton, error.rs,
    ReplicaClient trait, empty test files mirroring the Go suite.
  • Vendor the Go source + the upstream tests into a reference dir for context.

Phase 1 — Foundation port (sequential, gated)   [Tier 1 tests]
  • wal.rs + wal_reader_test  → MUST be green before fan-out (everything
    depends on correct frame/checksum handling).
  • replica_url.rs, lib.rs helpers.
  • Gate: cargo test on foundation passes, fixtures match golden vectors.

Phase 2 — Core fan-out (parallel, ≤16 agents)    [Tier 2 + 3]
  For each unit { db, store, replica, client/file, client/object_store }:
    a) Porter agent: translate Go file + its _test.go to Rust, make it compile
       and pass in isolation (mock the rest behind the trait).
    b) Adversarial reviewer agent (fresh context): given the Go original + the
       Rust port, tries to find a behavioral divergence (checksum endianness,
       off-by-one offsets, error mapping, retention boundaries). Writes a
       failing test if it finds one.
    c) Fixer agent: resolves reviewer findings. Loop a/b until reviewer can't
       refute (cap N iterations, else escalate to report).

Phase 3 — Integration assembly (sequential)      [Tier 4]
  • Wire modules together; port tests/ integration suite + restore fuzz/property
    tests; stand up MinIO in CI; run replicate→restore round trips and
    crash-in-the-middle cases.

Phase 4 — Adversarial recovery sweep (parallel, the critical 6%)
  • Several agents generate hostile scenarios (truncated WAL, missed frames →
    generation rollover, clock skew on lease, partial multipart upload,
    retention GC racing a restore) and assert recovery correctness.

Phase 5 — Legion integration (sequential)
  • Implement PersistenceDriver in Legion against the crate; swap out
    litestreamBinary; run Legion's own cargo test --all --locked.

Phase 6 — Synthesis (1 agent)
  • Coverage report: every Go test → Rust test status (ported/deferred/dropped).
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
| **M0** | Repo scaffold + reference vendoring + workflow script saved | Phase 0–1 |
| **M1** | DR-capable crate: S3/R2 + file replicate & restore, classic snapshot+WAL, Tier 1–4 tests green | Phases 1–4 (classic model) |
| **M2** | Legion uses the crate via `PersistenceDriver`; `litestreamBinary` removed; lease fencing wired | Phase 5 |
| **M3** | Modern LTX + compaction levels (lower restore cost), Tier 5 compaction tests | DEFER set, part 1 |
| **M4** | Live read replicas (VFS + server) for standby read-scaling | DEFER set, part 2 |
| **M5** | Re-enable dropped clients behind cargo features as demand appears | gcs/abs/oss/sftp/webdav |

Legion gets real HA value at **M2**. M3+ are upside.

---

## 9. Risks & Mitigations

- **Checksum / endianness bugs (the classic Litestream footgun).** → Golden
  vectors + cross-tool compat tests + dedicate the adversarial sweep here.
- **CGo/SQLite interop:** Go uses mattn/SQLite; Rust uses `rusqlite`
  (bundled SQLite) — checkpoint-takeover semantics (holding a read tx, manual
  `PRAGMA wal_checkpoint`) must be reproduced exactly. → Tier 2 `db` tests +
  loom-style concurrency test.
- **Wire-compat decision drift:** if we *don't* commit to Litestream's
  object-store layout we lose the `litestream restore` break-glass tool. →
  Decide in M1 (Open Questions) and write it down.
- **AI-translation "looks right, isn't":** the whole point of the per-file
  adversarial reviewer + fuzz/property tests. Don't merge a unit on porter
  confidence alone.
- **Upstream moves (v0.5 actively developed):** pin a specific upstream commit
  as the port target; the saved workflow re-runs against new commits later.
- **Scope creep into deferred features:** features are behind the M3+ line and
  cargo flags; M1/M2 must not block on them.

---

## 10. Open Questions (decide before/at M1)

1. **Wire compatibility:** Must the Rust crate's object-store layout be readable
   by the upstream `litestream` CLI (break-glass restore)? If yes, classic
   shadow-WAL+generation layout in M1; if no, we can jump straight to a clean
   LTX-only design.
2. **SQLite binding:** `rusqlite` (bundled, simplest) vs `libsqlite3-sys` shared
   — affects the "single static binary on edge" goal. Lean `rusqlite` bundled.
3. **Object IO:** confirm `object_store` crate (recommended — free multi-cloud)
   vs hand-rolled `aws-sdk-s3` (more control, more code).
4. **Crate license:** Litestream is Apache-2.0; the fork must carry Apache-2.0 +
   NOTICE attribution. Confirm this is acceptable for Legion's licensing.
5. **Repo name:** `litestream-rs` (clear lineage) vs a neutral name. Lineage
   name is friendlier for attribution and discoverability.
```
