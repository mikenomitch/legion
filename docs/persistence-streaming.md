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

```jsonnet
{
  persistence: {
    type: "streaming",
    streaming: {
      replicaUrl: "s3://my-bucket/legion/prod",
      syncInterval: "1s",
      credentials: {
        provider: "environment", // environment | static | webIdentity
      },
      lease: {
        provider: "object_storage", // none | object_storage
        key: "leases/prod/primary.json",
        ttlSeconds: 15,
        renewIntervalSeconds: 5,
        objectStorage: {
          bucket: "my-bucket",
          region: "auto", // R2 commonly uses auto
          endpoint: "https://<account-id>.r2.cloudflarestorage.com",
        },
      },
    },
  },
}
```

### `replicaUrl`

- `s3://<bucket>/<prefix>` — replicate to S3/R2/MinIO via rustyriver's
  `ObjectStoreClient`.
- `file://<path>` or a bare local path — replicate to a local directory via
  rustyriver's `FileReplicaClient` (useful for tests and single-host backups).
- empty string — replication is disabled (local SQLite only).

## Credentials

`persistence.streaming.credentials` is the **single** credential source for both
the WAL uploads and the lease object. Three providers:

| `provider` | Where credentials come from |
| --- | --- |
| `environment` (default) | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from the manager's process environment, falling back to `LITESTREAM_ACCESS_KEY_ID` / `LITESTREAM_SECRET_ACCESS_KEY` (and `LITESTREAM_S3_ENDPOINT`), matching Litestream's URL semantics. |
| `static` | Literal keys from the config file. |
| `webIdentity` | A platform-issued OIDC token read from a file and exchanged for short-lived session credentials via STS `AssumeRoleWithWebIdentity`. |

`environment` is the default, so a config written before this block existed
behaves exactly as it did.

> **Note.** `environment` is *only* the static env vars listed above — it is not
> the full AWS credential chain. An EC2 instance profile, an ECS task role, or
> `~/.aws/credentials` will **not** be picked up. Use `webIdentity` for federated
> credentials, or `static` for literal keys.

### `webIdentity` — the manager's workload identity

This is Legion's [workload identity](workload-identity.md) model applied to the
manager: a short-lived, federated credential instead of a permanent access key
on the most privileged host in the cluster.

```jsonnet
credentials: {
  provider: 'webIdentity',
  webIdentity: {
    // Re-read on every refresh, so a rotating projected token is picked up.
    tokenFile: '/var/run/secrets/eks.amazonaws.com/serviceaccount/token',
    roleArn: 'arn:aws:iam::123456789012:role/legion-manager-state',
    sessionName: 'legion-manager',   // optional; appears in CloudTrail
    durationSeconds: 3600,           // optional; 900..43200, default 3600
    stsRegion: 'us-east-1',          // optional; picks the regional STS endpoint
    // stsEndpoint: 'https://sts.us-east-1.amazonaws.com',  // optional override
  },
},
```

Legion reads the token, POSTs an unsigned `AssumeRoleWithWebIdentity` request to
STS, and caches the returned session credentials until five minutes before they
expire. Refresh happens in-process; no manager restart is involved, and the WAL
sync loop does not pause for it.

**Where the token comes from is not Legion's business** — it needs to be a file
containing a JWT that your provider's IAM trusts:

- **EKS (IRSA) / EKS Pod Identity** — the projected service-account token at
  `/var/run/secrets/eks.amazonaws.com/serviceaccount/token`.
- **GKE Workload Identity Federation** — the projected token, with an AWS role
  configured to trust GCP's issuer.
- **SPIFFE / SPIRE** — a JWT-SVID written to disk by the SPIRE agent. This is
  the answer for bare metal: `webIdentity` takes a *file*, not a cloud.
- **Vault** — the identity token backend (`/identity/oidc/token/<role>`) written
  to a file by a sidecar or an agent template.

#### Why the manager does not use Legion's own OIDC issuer

Legion is itself an OIDC provider — it mints tokens for tasks and worker nodes.
It deliberately does **not** mint its own state-replication credentials, and the
reason matters for how you set this up:

- The signing key lives in the SQLite store that streaming persistence is
  backing up. A credential minted from it would mean that anyone who reads the
  store can also delete its backups — and surviving control-plane compromise is
  the entire point of a backup.
- Disaster recovery runs before any store exists, so there would be no key and
  no token. A self-issued identity can never serve a restore.
- AWS validates a token by fetching the issuer's JWKS over HTTPS from the
  manager. Backing up the manager would then depend on the manager being
  reachable — which, during the incidents where backups matter, it is not.

So the identity has to come from outside Legion. In one line: **an identity may
not be issued by the system it is used to protect.** Worker nodes satisfy this
(the manager issues theirs, and it is a separate trust root); the manager's own
backups cannot, against the manager's own issuer.

### Credentials for the lease

The lease uses the same `credentials` block. `lease.objectStorage.accessKeyId` /
`secretAccessKey` still work and still win *for the lease only*, so existing
configs are unchanged — but combining them with `provider: 'webIdentity'` is
rejected at startup:

```
persistence.streaming.credentials.provider is 'webIdentity' but
persistence.streaming.lease.objectStorage still sets accessKeyId/secretAccessKey;
remove the lease keys so the lease uses the same federated credential as the replica
```

That combination is almost always a half-finished migration, and its symptom —
a manager that replicates fine but silently cannot renew its lease, so failover
never happens — is not one you want to discover during a failover.

### An IAM policy for the role

The manager needs read/write on the replica prefix, and on the lease object when
`lease.provider` is `object_storage`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-bucket/legion/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::my-bucket/leases/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::my-bucket"
    }
  ]
}
```

Consider putting the backup prefix in a **separate account or bucket** with
object-lock or versioning, and granting the manager's role no `DeleteObject` on
it — the point of a federated credential is to bound what a compromised manager
can reach, and delete is the permission that matters.

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

## Failure modes and diagnostics

| Symptom | Cause | What to check |
| --- | --- | --- |
| Manager refuses to boot naming `credentials.webIdentity` | Missing `roleArn`/`tokenFile`, or `durationSeconds` outside 900–43200 | The error names the field. |
| Manager refuses to boot naming both `credentials` and `lease.objectStorage` | Federated replica next to a static-keyed lease | Delete the lease's static keys. |
| `rustyriver replica sync cycle failed … InvalidIdentityToken` | The token is not trusted by the role, or the OIDC provider is not registered in IAM | The IAM OIDC provider's issuer must match the `iss` in the token; the role's trust policy must match its `sub`/`aud`. |
| `… AccessDenied` on the exchange | The role's trust policy rejects this token's claims | Decode the token file and compare `sub`/`aud` against the trust policy. |
| `… ExpiredTokenException` | The token file went stale — usually a token *not* being rotated by the platform | Legion re-reads the file on every refresh, so this points outside Legion. |
| Replication is silent but the lease never renews | Only the lease has credentials, or only the replica does | The boot line `Started rustyriver streaming replication` reports `credentials_provider`. |

A failed credential exchange fails that one operation, not the manager: the sync
loop retries on its next tick, and a lease renewal error keeps the lease rather
than dropping it, so a brief STS outage does not cause a failover.

## Operational Notes

- **No external binary is required** — replication is embedded via the
  `rustyriver` crate. (The former `litestreamBinary` config field has been
  removed; it is ignored if still present in old configs.)
- For Cloudflare R2, use the S3-compatible endpoint + credentials. R2 has no STS,
  so `webIdentity` does not apply there — use `static` or `environment`.
- Keep `ttlSeconds` comfortably above `renewIntervalSeconds` (default 15s / 5s).
- For manual failover (`none`), operators must ensure only one writer is active.
- The lease lock object is written at `<dir(key)>/lock.json` — see
  [`rustyriver-migration.md`](../internal_docs/rustyriver-migration.md) for the lock-key naming
  detail and the one optional upstream enhancement.
- Disaster recovery (point-in-time restore from the replica) uses
  `rustyriver::restore`.

### Known limitations

- **Restore does not yet use this credential block.** `legion start manager
  --restore-from <path>` takes a local file; pulling a replica down from object
  storage is a separate, manual step today, authenticated by whatever tool you
  use for it. Teaching `--restore-from` to accept a replica URL and reuse the
  `credentials` block is planned but not shipped.
- **No credential introspection surface.** The effective provider appears in the
  startup log line; there is no CLI or RPC that reports "which provider, last
  refreshed when, expiring when". Planned, not shipped.
- **`environment` is not the full AWS chain.** Instance profiles, ECS task
  roles, and `~/.aws/credentials` are not consulted by any provider today.
- **AWS-shaped auth only.** Every supported target (S3, R2, MinIO, Backblaze,
  Filebase, Supabase) speaks SigV4; there is no GCS-native or Azure SAS path.
