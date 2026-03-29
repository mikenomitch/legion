# Streaming Persistence (Litestream + Object-Storage Lease)

Legion supports a streaming persistence mode for manager state using:

- SQLite as the local primary store.
- Litestream for WAL shipping to object storage.
- An optional object-storage lease for single-primary failover fencing.

## Guarantees

- **Write model:** single primary writer.
- **Replication model:** asynchronous WAL shipping (small, non-zero RPO).
- **Failover model:**
  - `lease.provider = "none"`: manual failover only.
  - `lease.provider = "object_storage"`: automatic primary election/fencing via an S3-compatible lease object.

## Configuration

```jsonnet
{
  persistence: {
    type: "streaming",
    streaming: {
      litestreamBinary: "litestream",
      replicaUrl: "s3://my-bucket/legion/prod",
      syncInterval: "1s",
      lease: {
        provider: "object_storage", // none | object_storage
        key: "leases/prod/primary.json",
        ttlSeconds: 15,
        renewIntervalSeconds: 5,
        objectStorage: {
          bucket: "my-bucket",
          region: "auto", // R2 commonly uses auto
          endpoint: "https://<account-id>.r2.cloudflarestorage.com",
          accessKeyId: std.extVar("R2_ACCESS_KEY_ID"),
          secretAccessKey: std.extVar("R2_SECRET_ACCESS_KEY"),
        },
      },
    },
  },
}
```

## Runtime Behavior

On manager startup in `streaming` mode:

1. Legion starts Litestream replication if `replicaUrl` is set.
2. If `lease.provider = "none"`, the node is primary immediately.
3. If `lease.provider = "object_storage"`, the node participates in a lease loop:
   - Creates the lease key if missing.
   - Renews lease while holder.
   - Becomes standby while another healthy holder exists.

The manager exposes persistence status through existing manager status APIs (`backend`, `role`, `leaseHeld`).

## Operational Notes

- Ensure `litestream` binary is available on the manager host PATH, or set `litestreamBinary`.
- For Cloudflare R2, use S3-compatible endpoint + credentials.
- Keep `ttlSeconds` comfortably above `renewIntervalSeconds` (default 15s / 5s).
- For manual failover (`none`), operators must ensure only one writer is active.
