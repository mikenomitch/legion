# Plan: Embed-and-Exec Litestream (replace the PATH-resolved binary)

## Status and Goal

**Goal:** Change the streaming-persistence integration so the `litestream`
binary is **shipped inside the Legion binary** and **extracted + executed at
runtime**, instead of being resolved from `$PATH` / a config path. This delivers
the "single binary on edge devices" goal (README) without reimplementing
Litestream — the cheap alternative to the Rust fork
([`litestream-rust-fork-plan.md`](./litestream-rust-fork-plan.md)).

**Why this over the fork:** keeps the real, battle-tested Litestream as the
replication engine (zero data-loss risk from a reimplementation), removes the
external-dependency friction, and is days of work rather than weeks. See the
trade-off discussion that motivated this plan.

## Current State (what we're changing)

- `src/storage/persistence.rs`
  - `StreamingPersistenceDriver` (line ~132): holds `litestream_process: Arc<Mutex<Option<Child>>>`.
  - `start_litestream()` (line ~170): `Command::new(self.cfg.litestream_binary)` then `replicate -exec "sleep infinity" -url <replica_url> -interval <sync_interval> <db_path>`; stores the `Child`.
  - `object_storage_lease_loop` (native Rust) — **unchanged by this plan**.
  - `status()` reports `backend/role/lease_held`.
  - `build_persistence_driver()` (line ~692): factory selecting driver by `PersistenceType`.
  - **Gaps today:** no child shutdown (orphan on exit), no restore-on-failover.
- `src/structs/config.rs`
  - `StreamingPersistenceConfig.litestream_binary: Option<String>` (line ~147), default `"litestream"` (lines ~329/340), plus `replica_url`, `sync_interval`, `lease`.
- `docs/persistence-streaming.md` documents `litestreamBinary` on PATH.
- `docker/` does not install litestream (only a worker dev image), so today a real deployment would have to add it — exactly the friction we remove.

---

## Decisions to Make Before Implementing

| # | Topic | Recommendation | Notes |
|---|-------|----------------|-------|
| **E-1** | How to obtain the binary at build time | **Vendor via a checked-in fetch script into a gitignored `vendor/litestream/<target>/`, verified by SHA-256; `build.rs` reads it (or `LITESTREAM_BINARY_PATH`).** | **Avoids network at build time** — important under this repo's remote-execution network policy. Downloading inside `build.rs` would couple builds to GitHub and the policy. |
| **E-2** | Embed all platforms or just the build target | **Build-target only**, chosen by `build.rs` from `TARGET`/`CARGO_CFG_TARGET_*`. | Embedding every platform bloats the binary by ~25 MB each. One target = one litestream. |
| **E-3** | Compress the embedded bytes | **Yes — zstd, decompress on extract** (~25 MB → ~8 MB). | Optional but cheap; behind a const so it can be toggled. |
| **E-4** | Keep a PATH/override escape hatch | **Yes.** Feature `embedded-litestream` (default on). `litestream_binary` config, when set, overrides the embedded one. Building without the feature falls back to PATH (dev/unsupported targets). | Backward compatible; lets CI/dev skip the vendored binary. |
| **E-5** | Add restore-on-failover now | **Yes, behind `restore_if_missing: bool` (default `false`).** | This is the real payoff of having the binary in-process. Off by default because it changes failover semantics — opt-in. |
| **E-6** | Process lifecycle | **Kill child on `stop()`/`Drop` now; full crash-restart supervisor is optional/later.** | Fixes the current orphan-process gap with minimal scope. |
| **E-7** | Pinned Litestream version | **`v0.5.11`** (latest stable, Apr 2026) | Single constant; checksum pinned alongside. |
| **E-8** | Supported target matrix | linux amd64/arm64, darwin amd64/arm64, windows amd64 | Map Rust triples → Litestream release asset names; unsupported target → PATH fallback (E-4). |

---

## Target Design

### New module: `src/storage/litestream_bin.rs`
Owns sourcing + extraction of the binary. Public surface:
```rust
/// Pinned upstream version this build embeds/expects.
pub const LITESTREAM_VERSION: &str = "0.5.11";

/// Resolve a runnable litestream executable, in priority order:
///  1. cfg override (`litestream_binary`) if set,
///  2. the embedded binary (feature `embedded-litestream`), extracted+cached,
///  3. `litestream` on $PATH (fallback).
/// Returns the path to an executable and how it was resolved (for logging/status).
pub fn resolve_litestream(cfg_override: Option<&str>) -> R<ResolvedBinary>;
```
- **Embedded bytes:** `build.rs` emits `OUT_DIR/litestream_embedded.rs` containing either `pub static BYTES: &[u8] = include_bytes!("…vendor path…");` (+ a compressed flag + expected SHA-256), or, when the feature is off / no vendored binary, an empty sentinel that forces PATH fallback.
- **Extraction:** decompress (if E-3) → write atomically to a version+hash-named path under the OS cache dir (e.g. `dirs::cache_dir()/legion/litestream-<version>-<sha8>[.exe]`) via temp-file + rename; set `0o755` on unix; **idempotent** (reuse if present and checksum matches); safe under concurrent managers (rename is atomic; losers reuse the winner's file).
- **Integrity:** verify SHA-256 of the extracted file against the pinned constant before returning; mismatch is a hard error.

### `build.rs` additions
- Read `TARGET` → map to the expected vendored path `vendor/litestream/<rust-target>/litestream[.exe]` (override with `LITESTREAM_BINARY_PATH`).
- If present: compute SHA-256, optionally zstd-compress into `OUT_DIR`, generate `litestream_embedded.rs` pointing at it, emit `cargo:rerun-if-changed` + `cargo:rerun-if-env-changed=LITESTREAM_BINARY_PATH`.
- If absent **and** feature on: emit a clear `cargo:warning` + a sentinel that compiles but forces PATH fallback at runtime (don't fail the build — keeps dev ergonomic). If the feature is off: always sentinel.

### `StreamingPersistenceDriver` changes (`persistence.rs`)
- In `start_litestream()`, replace `Command::new(self.cfg.litestream_binary)` with `litestream_bin::resolve_litestream(self.cfg.litestream_binary_override())?` and log the resolution mode (`embedded` | `override` | `path`).
- Add `restore_if_missing` handling: before `replicate`, if the DB file is absent and config opts in, run `litestream restore -url <replica_url> -if-replica-exists <db_path>` with the resolved binary; surface failures.
- Add `fn stop(&self)` + `impl Drop`: take the `Child` and `kill()` + `wait()` (graceful: send terminate, then kill after a short grace).
- `status()`: include the resolved-binary mode (extend `PersistenceStatus` with an optional `engine` string, or log only — see E-9 below).

### Config changes (`config.rs`) — backward compatible
- Keep `litestream_binary: Option<String>` but **redefine its meaning** to "explicit override path"; default is now `None` (embedded), not `"litestream"`. Update `with_defaults()` / `Default` accordingly.
- Add `restore_if_missing: Option<bool>` (full: `bool`, default `false`).
- `configs/jobs/maximal.jsonnet` + any persistence example configs updated (per AGENTS.md: new config keys must appear in maximal).

---

## Work Breakdown

| ID | Task | Files | Done when |
|----|------|-------|-----------|
| **L1** | `scripts/fetch-litestream.sh`: download pinned assets for the target matrix into `vendor/litestream/<target>/`, verify SHA-256 (checksums committed in the script). Add `vendor/litestream/` to `.gitignore`. | `scripts/`, `.gitignore` | Running it populates the host target's binary; checksums enforced. |
| **L2** | `build.rs` embed logic (E-1/E-2/E-3/E-8) + generated `litestream_embedded.rs`. | `build.rs`, `Cargo.toml` (feature `embedded-litestream`, `zstd`, `sha2` build-deps) | Build with vendored binary embeds it; without it, warns + sentinel. |
| **L3** | `src/storage/litestream_bin.rs`: `resolve_litestream`, extraction, integrity, idempotency, concurrency-safety. | new file | Unit tests (below) pass. |
| **L4** | Wire driver to resolver; add `stop()`/`Drop`; logging of resolution mode. | `persistence.rs` | Replication starts via embedded binary; child is killed on shutdown. |
| **L5** | `restore_if_missing` path (E-5) + config field. | `persistence.rs`, `config.rs`, `maximal.jsonnet` | Fresh host with empty DB + opt-in restores from replica before replicating. |
| **L6** | Config redefinition + defaults + back-compat. | `config.rs` | Old configs (`litestreamBinary` set) still work as override; default is embedded. |
| **L7** | Docs: rewrite `docs/persistence-streaming.md` (no more "install litestream on PATH"; document embedded behavior, override, restore, version). | `docs/` | Doc matches behavior. |
| **L8** | Packaging: note in Dockerfiles/release that litestream no longer needs separate install; add the `fetch-litestream.sh` step to the release/build pipeline; ship `LICENSE`/`NOTICE` for the embedded Apache-2.0 binary. | `docker/`, CI/release, `NOTICE` | Release build embeds without a manual step; attribution shipped. |

---

## Tests

- **Unit (`litestream_bin.rs`):**
  - extraction writes an executable file, sets the exec bit (unix), and is idempotent (second call reuses, no rewrite);
  - SHA-256 mismatch is rejected;
  - concurrent `resolve_litestream` from N threads yields one valid file (no torn writes);
  - resolution priority: override > embedded > PATH;
  - feature-off build compiles and falls back to PATH.
- **Behavioral:** extracted binary runs `litestream version` and reports the pinned `LITESTREAM_VERSION` (skipped automatically when no embedded binary and none on PATH).
- **Integration (gated on a test replica — file:// or MinIO):**
  - `replicate` then `restore` round-trips a small DB using the embedded binary;
  - `restore_if_missing` restores a fresh DB on startup;
  - `Drop`/`stop()` leaves no orphan `litestream` process.
- **Existing suite:** `cargo test --all --locked` stays green; lease-loop tests unchanged.

> CI note: CI runners must run `scripts/fetch-litestream.sh` (or set
> `LITESTREAM_BINARY_PATH`) before building if the embedded path is exercised;
> otherwise tests fall back to PATH and the binary-dependent integration tests
> self-skip.

---

## Open Questions

- **E-9 — surface the engine/resolution in status APIs?** Add `engine: "litestream <ver> (embedded|override|path)"` to `PersistenceStatus` (and the gRPC/MCP status surface), or keep it to logs? Recommend adding it — it's useful for operators/agents and cheap.
- **E-10 — binary-size budget.** With zstd the manager binary grows ~8 MB per build. Acceptable for edge? If not, gate embedding to release builds only and keep dev/PATH.
- **E-11 — build network policy.** Confirm CI/release can run `fetch-litestream.sh` (reach GitHub releases) at least once to populate `vendor/`. If outbound is blocked there too, we need the binaries mirrored to an allowed artifact store.
- **E-12 — cross-compilation.** If release builds cross-compile (e.g. build linux/arm64 on amd64), `fetch-litestream.sh` must fetch the *target* asset, and `build.rs` must key off `TARGET` (not the host). Confirm the release pipeline's target list.

---

## Risks & Mitigations

- **R-1 Stale/corrupt extracted binary** → version+sha-named cache path + integrity check on every resolve; never trust an existing file without checksum.
- **R-2 Orphaned child processes** (exists today) → L4 `Drop`/`stop()` kill+wait.
- **R-3 Binary bloat** → zstd (E-3) + build-target-only (E-2) + optional release-only gate (E-10).
- **R-4 Redistribution/licensing** → ship Litestream `LICENSE` + `NOTICE` (Apache-2.0) in the image/release (L8).
- **R-5 Read-only / locked filesystems can't extract** → allow `litestream_binary` override to point at a pre-placed binary, and honor a configurable extraction dir; fall back to PATH.
- **R-6 Version skew between embedded binary and replica data** → single pinned `LITESTREAM_VERSION`; restore uses the same embedded binary, so writer/reader versions always match.
```
