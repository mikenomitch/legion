# Porting streaming persistence from Litestream to rustyriver

This documents the migration of Legion's `streaming` persistence backend from the
out-of-process **Litestream** binary to the embeddable
[**rustyriver**](https://crates.io/crates/rustyriver) crate (a Rust
reimplementation of Litestream v0.5), and records the one optional upstream change
that would improve config parity.

## What changed in Legion

- **Dependency:** added `rustyriver = "0.1.0"` (default `s3` feature, which pulls
  `object_store/aws` + `reqwest`). Versions line up with Legion's existing tree
  (`object_store` 0.11, `rusqlite` 0.32, `reqwest` 0.12, `tokio` 1), so the
  shared `object_store::ObjectStore` trait unifies to a single crate version —
  this is what lets Legion hand an `Arc<dyn ObjectStore>` to `S3Leaser::new`.
- **WAL shipping** (`src/storage/persistence.rs`): the old `litestream replicate`
  subprocess (`std::process::Command`) is replaced by an in-process loop:
  - `rustyriver::Db::open(db_path)` takes over checkpointing (WAL mode,
    auto-checkpoint off, long-running read lock).
  - `rustyriver::Replica::new(db, client)` connects it to a replica client.
  - Each cycle calls `Db::sync()` (WAL → local LTX) then `Replica::sync()`
    (upload new LTX files), sleeping `syncInterval` between cycles.
  - `replicaUrl` scheme selects the client: `s3://…` →
    `ObjectStoreClient` (built from `parse_replica_url_with_query` +
    `ObjectStoreConfig::from_url`); `file://…` or a bare path →
    `FileReplicaClient`.
- **Lease fencing:** the hand-rolled `object_store` put/get lease loop (custom
  `LeaseDocument`, last-write-wins overwrite) is replaced by
  `rustyriver::S3Leaser`, which uses ETag compare-and-swap (`PutMode::Create` /
  `Update`) for correct single-primary fencing. The acquire/renew/release
  decision is isolated in `lease_step` for unit testing.
- **Config:** the now-meaningless `streaming.litestreamBinary` field was removed
  from `StreamingPersistenceConfig(Full)`, `configs/manager.jsonnet`, and the
  docs. Removal is backward-compatible: the structs do not set
  `deny_unknown_fields`, so old configs that still carry `litestreamBinary`
  deserialize fine (the field is ignored).
- **Tests** (`src/storage/persistence.rs`): added an end-to-end replicate →
  simulate-host-loss → `rustyriver::restore` → verify test over a
  `FileReplicaClient`, a lease acquire/renew/fence test using `S3Leaser` over an
  in-memory object store, and unit tests for the URL/interval/key helpers.

## Required rustyriver changes

**None.** The port works against the published `rustyriver 0.1.0` as-is.

## Optional rustyriver enhancement (nice-to-have)

There is exactly one cosmetic mismatch worth an upstream change.

**Configurable lock-object key/filename on `S3Leaser`.**

- **Today:** `S3Leaser` always writes its lock object as `lock.json`, under a
  configurable prefix set via `with_path(prefix)` (key = `<prefix>/lock.json`,
  hardcoded in `lockKey()` / `DEFAULT_LEASE_PATH`).
- **Legion's historical config** treated the lease `key` as a *full* object key
  (default `leases/default/primary.json`). To map onto `with_path`, Legion passes
  the **directory** of `key` as the prefix, so the lock object lands at
  `<dir(key)>/lock.json` (e.g. `leases/default/lock.json`). The filename changes
  from `primary.json` to `lock.json`; this is harmless (the lease is ephemeral
  runtime coordination state and all nodes agree on the same key), but it is not a
  byte-for-byte match of the old object name.
- **Suggested API** to allow exact parity:

  ```rust
  impl S3Leaser {
      /// Override the lock-object filename (default "lock.json").
      pub fn with_lock_filename(mut self, name: impl Into<String>) -> Self { /* … */ }

      // …or, to set the full key directly:
      /// Use an exact lock-object key instead of "<path>/lock.json".
      pub fn with_lock_key(mut self, key: impl Into<String>) -> Self { /* … */ }
  }
  ```

  With either method, Legion could pass the configured `key` verbatim and drop the
  `lease_path_prefix` directory-derivation helper. Until then, the directory
  mapping is the workaround and requires no upstream change.

### Notes / non-issues found during the port

- **Replica credentials.** `ObjectStoreConfig::from_url` sources S3 credentials
  from the environment (`AWS_*` / `LITESTREAM_*`) only. Legion preserves
  Litestream's URL-credential behavior for `replicaUrl`. If passing explicit
  credentials for the replica destination is ever needed, `ObjectStoreConfig`'s
  fields are all public, so they can be set after `from_url` without an upstream
  change.
- **Module path.** `parse_replica_url_with_query` / `ParsedReplicaUrl` are reached
  via `rustyriver::replica_url::…` (the module is public but these are not
  re-exported at the crate root). A crate-root re-export would be a minor
  ergonomic improvement but is not required.
