# Streaming Persistence (rustyriver + Object-Storage Lease)

Legion supports a streaming persistence mode for manager state using:

- SQLite as the local primary store.
- [rustyriver](https://crates.io/crates/rustyriver) for WAL shipping to object
  storage. rustyriver is an embeddable Rust reimplementation of Litestream v0.5,
  so replication runs in-process. There is no `litestream` sidecar binary to
  install or supervise.
- An optional object-storage lease for single-primary failover fencing, provided
  by rustyriver's `S3Leaser`.

## Guarantees

- **Write model:** single primary writer.
- **Replication model:** asynchronous WAL shipping (small, non-zero RPO).
- **Failover model:**
  - `lease.provider = "none"`: manual failover only. Every manager configured
    this way acts as a primary, so you must run exactly one.
  - `lease.provider = "object_storage"`: automatic takeover via an
    S3-compatible lease object (ETag compare-and-swap). Only the lease holder
    writes and ships WAL. A standby that takes the lease restores the replica
    before it serves as primary. A primary that loses the lease is fenced
    until it is restarted. See [Failover with the lease](#failover-with-the-lease).

## Configuration

Nickel has no access to the process environment. Keep secrets in a separate,
uncommitted file and import them:

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

- `s3://<bucket>/<prefix>`: replicate to S3/R2/MinIO via rustyriver's
  `ObjectStoreClient`. Credentials for this destination are read from the ambient
  environment (`AWS_*`, falling back to `LITESTREAM_*`), matching Litestream's URL
  semantics.
- `file://<path>` or a bare local path: replicate to a local directory via
  rustyriver's `FileReplicaClient` (useful for tests and single-host backups).
- Empty string: replication is disabled (local SQLite only).

KV values inside the shipped WAL frames are ciphertext
([KV Encryption at Rest](kv-encryption.md)). The root key that decrypts them
is never part of the replica. Segments written before you enabled encryption
keep whatever plaintext they held until the replica's retention removes them.
If that history matters, rotate retention after enabling encryption.

## Runtime Behavior

Shipping WAL works the same way on any primary. Legion opens the SQLite
store as a rustyriver-managed `Db`. It uses WAL mode, disables
auto-checkpoint, and holds a long-running read lock so it can take over
checkpointing. It then runs the capture/upload loop every `syncInterval`.
`Db::sync()` captures new WAL frames into a local LTX file, and
`Replica::sync()` uploads new LTX files to `replicaUrl`. Legion skips this
when `replicaUrl` is empty.

If `lease.provider = "none"`, the manager is primary from startup and ships
immediately.

If `lease.provider = "object_storage"`, the manager starts as a **standby**
and runs an `S3Leaser` loop. It acquires the lock object if no live lease
exists, renews it (ETag CAS) while it holds it, and stands by while another
manager holds a live lease.

The manager exposes persistence status through existing manager status APIs
(`backend`, `role`, `leaseHeld`).

## Failover with the lease

What a manager does depends on its role:

- **Standby.** It serves reads from its local database, which is whatever it
  had when it last ran and can be far behind the primary. It does not run
  the scheduler, the node checker, or any other writer loop. Every write is
  refused with an error that says it is a standby, so send writes and point
  workers at the primary. It does not ship WAL.
- **Taking over.** When the standby acquires the lease, it first restores
  the replica's latest state (`rustyriver::restore`), migrates it to its own
  schema, and swaps it in as its store. Then it starts shipping, continuing
  the replica's history, and only then reports itself `primary` and starts
  its writer loops. An empty replica (a brand-new deployment) leaves the
  local database as it is. If the restore fails, the manager gives the lease
  back and stays a standby.
- **Losing the lease.** A primary that sees another manager take the lease,
  or cannot renew it for a whole `ttlSeconds`, is **fenced**. It stops
  shipping, refuses writes, idles its writer loops, and does not compete for
  the lease again. Restart it to bring it back as a standby.
- **Stopping cleanly.** On SIGTERM or SIGINT, a primary runs one final
  capture/upload cycle before it releases the lease, so a clean stop leaves
  the replica as current as the local database. A crash or SIGKILL skips that
  step, and the replica can lag by up to one `syncInterval`. A fenced
  manager never runs it, because the replica belongs to the new primary. See
  [Stopping a manager](upgrades.md#stopping-a-manager).

Known limitations:

- Replication is asynchronous. Writes the old primary made in its last
  `syncInterval` before it died, and had not shipped yet, are not in the
  replica, so the new primary does not have them.
- Fencing is time-based. An old primary that is paused (a long process
  stall, a frozen VM) for longer than the lease TTL can still accept a write
  and ship one more sync cycle before its lease loop notices. Keep
  `ttlSeconds` well above `syncInterval` and `renewIntervalSeconds`.
- A standby serves stale reads. Treat only the primary as the source of
  truth.

## Operational Notes

- No external binary is required. Replication is embedded via the
  `rustyriver` crate. The former `litestreamBinary` config field has been
  removed, and Legion ignores it if it is still present in old configs.
- For Cloudflare R2, use the S3-compatible endpoint and credentials.
- Keep `ttlSeconds` comfortably above `renewIntervalSeconds` (default 15s / 5s).
- For manual failover (`none`), you must make sure only one writer is active.
- The lease lock object is written at `<dir(key)>/lock.json`.
- Disaster recovery (point-in-time restore from the replica) uses
  `rustyriver::restore`.
