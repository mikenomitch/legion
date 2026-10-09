# Streaming Persistence (rustyriver + Object-Storage Lease)

Legion supports a streaming persistence mode for manager state using:

- SQLite as the local primary store.
- [rustyriver](https://crates.io/crates/rustyriver) for WAL shipping to object
  storage. rustyriver is an embeddable Rust reimplementation of Litestream v0.5,
  so replication runs **in-process** — there is no `litestream` sidecar binary to
  install or supervise.
- An optional object-storage lease for single-primary failover fencing, provided
  by rustyriver's `S3Leaser`.

## Guarantees

- **Write model:** single primary writer.
- **Replication model:** asynchronous WAL shipping (small, non-zero RPO).
- **Failover model:**
  - `lease.provider = "none"`: manual failover only.
  - `lease.provider = "object_storage"`: automatic primary election/fencing via an
    S3-compatible lease object (ETag compare-and-swap).

## Configuration

Nickel deliberately has no access to the process environment, so secrets are
kept in a separate, uncommitted file and imported:

```nickel
# secrets.ncl (gitignored)
{ accessKeyId = "…", secretAccessKey = "…" }
```

```nickel
let secrets = import "secrets.ncl" in
{
  persistence = {
    type = "streaming",
    streaming = {
      replicaUrl = "s3://my-bucket/legion/prod",
      syncInterval = "1s",
      lease = {
        provider = "object_storage", # none | object_storage
        key = "leases/prod/primary.json",
        ttlSeconds = 15,
        renewIntervalSeconds = 5,
        objectStorage = {
          bucket = "my-bucket",
          region = "auto", # R2 commonly uses auto
          endpoint = "https://<account-id>.r2.cloudflarestorage.com",
          accessKeyId = secrets.accessKeyId,
          secretAccessKey = secrets.secretAccessKey,
        },
      },
    },
  },
}
```

### `replicaUrl`

- `s3://<bucket>/<prefix>` — replicate to S3/R2/MinIO via rustyriver's
  `ObjectStoreClient`. Credentials for this destination are read from the ambient
  environment (`AWS_*`, falling back to `LITESTREAM_*`), matching Litestream's URL
  semantics.
- `file://<path>` or a bare local path — replicate to a local directory via
  rustyriver's `FileReplicaClient` (useful for tests and single-host backups).
- empty string — replication is disabled (local SQLite only).

KV values inside the shipped WAL frames are ciphertext
([KV Encryption at Rest](kv-encryption.md)), and the root key that decrypts
them is never part of the replica. Segments written *before* encryption was
enabled keep whatever plaintext they held until the replica's retention
removes them — rotate retention after enabling if that history matters.

## Runtime Behavior

On manager startup in `streaming` mode:

1. Legion opens the SQLite store as a rustyriver-managed `Db` (WAL mode,
   auto-checkpoint disabled, long-running read lock held to take over
   checkpointing), then runs the capture/upload loop every `syncInterval`:
   `Db::sync()` captures new WAL frames into a local LTX file and
   `Replica::sync()` uploads new LTX files to `replicaUrl`. This is skipped when
   `replicaUrl` is empty.
2. If `lease.provider = "none"`, the node is primary immediately.
3. If `lease.provider = "object_storage"`, the node participates in an
   `S3Leaser` loop:
   - Acquires the lock object if no live lease exists.
   - Renews the lease (ETag CAS) while it holds it.
   - Stands by while another node holds a live lease.

The manager exposes persistence status through existing manager status APIs
(`backend`, `role`, `leaseHeld`).

## Operational Notes

- **No external binary is required** — replication is embedded via the
  `rustyriver` crate. (The former `litestreamBinary` config field has been
  removed; it is ignored if still present in old configs.)
- For Cloudflare R2, use the S3-compatible endpoint + credentials.
- Keep `ttlSeconds` comfortably above `renewIntervalSeconds` (default 15s / 5s).
- For manual failover (`none`), operators must ensure only one writer is active.
- The lease lock object is written at `<dir(key)>/lock.json` — see
  [`rustyriver-migration.md`](../internal_docs/rustyriver-migration.md) for the lock-key naming
  detail and the one optional upstream enhancement.
- Disaster recovery (point-in-time restore from the replica) uses
  `rustyriver::restore`.
