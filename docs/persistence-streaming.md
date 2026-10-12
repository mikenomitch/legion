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

On AWS, give the manager a role and leave credentials out of the config
entirely (see [Credentials](#credentials)):

```nickel
{
  persistence = {
    type = "streaming",
    streaming = {
      replicaUrl = "s3://acme-legion-state/prod",
      syncInterval = "1s",
      lease = {
        provider = "object_storage", # none | object_storage
        key = "leases/prod/primary.json",
        ttlSeconds = 15,
        renewIntervalSeconds = 5,
        objectStorage = {
          bucket = "acme-legion-state",
          region = "us-west-2",
        },
      },
    },
  },
}
```

Object stores that only issue keys (Cloudflare R2, MinIO, Backblaze) take them
in `credentials`. Nickel has no access to the process environment, so keep the
keys in a separate, uncommitted file and import them:

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
      replicaUrl = "s3://my-bucket/legion/prod?endpoint=https://<account-id>.r2.cloudflarestorage.com&region=auto",
      credentials = {
        accessKeyId = secrets.accessKeyId,
        secretAccessKey = secrets.secretAccessKey,
      },
      lease = {
        provider = "object_storage",
        key = "leases/prod/primary.json",
        objectStorage = {
          bucket = "my-bucket",
          region = "auto", # R2 uses auto
          endpoint = "https://<account-id>.r2.cloudflarestorage.com",
        },
      },
    },
  },
}
```

### `replicaUrl`

- `s3://<bucket>/<prefix>`: replicate to S3/R2/MinIO via rustyriver's
  `ObjectStoreClient`. Query parameters set the `endpoint`, `region`,
  `forcePathStyle` and `skipVerify`; `LITESTREAM_S3_ENDPOINT` supplies the
  endpoint when the URL has none. Credentials come from
  [`credentials`](#credentials), never from the URL.
- `file://<path>` or a bare local path: replicate to a local directory via
  rustyriver's `FileReplicaClient` (useful for tests and single-host backups).
- Empty string: replication is disabled (local SQLite only).

## Credentials

Three things talk to object storage: the WAL uploads, the restore a standby
runs when it takes the lease, and the lease object itself. All three use the
same credentials, resolved once at boot in this order. The first source that
is configured wins:

| # | Source | What sets it |
| --- | --- | --- |
| 1 | `persistence.streaming.credentials` | `accessKeyId` + `secretAccessKey` (+ `sessionToken` for temporary keys) in the manager config. |
| 2 | Environment keys | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`). |
| 3 | Web identity | `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` (+ `AWS_ROLE_SESSION_NAME`). EKS IRSA injects these into the pod. |
| 4 | ECS task role | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, which ECS sets. |
| 5 | EC2 instance metadata | Nothing: this is what happens when none of the above is set. |

Sources 3 to 5 are short-lived and refresh themselves in-process: the manager
never restarts for a rotation, and the WAL sync loop does not pause for it.
With web identity, the token file is re-read on every exchange, so a rotating
projected token is picked up. Session credentials are refreshed five minutes
before they expire.

The manager logs the source it resolved at boot:

```
INFO Streaming persistence authenticates to object storage credential_source="web_identity" role_arn=arn:aws:iam::123456789012:role/legion-manager-state token_file=/var/run/secrets/eks.amazonaws.com/serviceaccount/token
```

`credential_source` is one of `config`, `environment`, `web_identity`,
`container_role` or `instance_metadata`. Keys never appear in logs, and the
`credentials` block prints as `<redacted>` wherever the config is debug-printed.

**Prefer a platform identity over keys.** The manager holds the one
credential that can overwrite the cluster's backups, so it should be a
short-lived, federated one. Leave `credentials` unset and give the manager an
identity through its platform:

- **EKS (IRSA).** Annotate the manager's service account with the role. The
  EKS webhook sets `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`, and the
  manager picks them up with no Legion config at all.
- **Bare metal or any other platform.** Anything that writes a JWT to a file
  works: a SPIFFE/SPIRE agent writing a JWT-SVID, Vault's identity token
  backend rendered by Vault Agent, GKE workload identity federation. Register
  its issuer as an IAM OIDC provider, trust it from a role, and set the two
  variables in the manager's service unit:

  ```ini
  # /etc/systemd/system/legion-manager.service.d/identity.conf
  [Service]
  Environment=AWS_ROLE_ARN=arn:aws:iam::123456789012:role/legion-manager-state
  Environment=AWS_WEB_IDENTITY_TOKEN_FILE=/run/spire/svid/jwt
  ```

- **EC2.** Attach an instance profile. **ECS.** Give the task a task role.

### Why the manager does not use Legion's own OIDC issuer

Legion is itself an OIDC provider: it mints [workload identity](workload-identity.md)
tokens for tasks and worker nodes. It deliberately does **not** mint the
manager's own storage credentials, and there is no `managerIdentities` block:

- The signing key lives in the SQLite store that streaming persistence is
  backing up. A credential minted from it would mean that anyone who can read
  the store can also overwrite its backups, and surviving a control-plane
  compromise is the point of a backup.
- A restore runs before any store exists, so there would be no key and no
  token. A self-issued identity can never authenticate disaster recovery.
- AWS validates a token by fetching the issuer's JWKS over HTTPS, from the
  manager. Backing up the manager would then depend on the manager being
  reachable, which during the incidents where backups matter it often is not.

In one line: **an identity may not be issued by the system it is used to
protect.** Worker nodes satisfy that rule (the manager is a separate trust
root from the worker); the manager's own backups cannot, against the
manager's own issuer.

### An IAM policy for the manager's role

The manager reads and writes the replica prefix, and the lease object when
`lease.provider = "object_storage"`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::acme-legion-state/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::acme-legion-state/leases/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::acme-legion-state"
    }
  ]
}
```

`DeleteObject` on the replica prefix is what retention uses to prune old
segments. Turn on bucket versioning (or Object Lock) so that a compromised
manager that overwrites or deletes segments cannot destroy the history.

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

## Failure modes and diagnostics

Credential problems that can be caught before anything runs stop the boot,
with an error naming the setting:

| Boot error | Cause |
| --- | --- |
| `persistence.streaming.credentials has no secret access key` (or `no access key id`) | Half a key pair in the config. |
| `the AWS_* environment has no secret access key` (or `no access key id`) | Half a key pair in the environment. |
| `AWS_WEB_IDENTITY_TOKEN_FILE is set but AWS_ROLE_ARN is not` (or the reverse) | Half a web identity. Without both, the manager would quietly fall through to instance metadata, a different principal. |
| `AWS_CONTAINER_CREDENTIALS_FULL_URI is set (EKS Pod Identity), which streaming persistence does not support` | EKS Pod Identity. It would otherwise fall through to the node's instance role. Use IRSA instead. |
| `lease.objectStorage.region is 'auto', but the manager authenticates with web identity` | The STS endpoint is derived from the lease's region, and `auto` is not an AWS region. Set the bucket's region. |
| `unknown field 'accessKeyId'` under `lease.objectStorage` | The lease's own keys were removed; move them to `persistence.streaming.credentials`. |

Problems only object storage can report show up at runtime, in the replica
sync or lease loop's warning, and fail that one operation rather than the
manager. The sync loop retries on its next tick, and a lease renewal error
keeps the lease until the TTL runs out, so a short STS outage does not cause
a failover.

| Runtime error contains | Cause |
| --- | --- |
| `InvalidIdentityToken` | IAM does not trust the token's issuer. The IAM OIDC provider's URL must match the token's `iss`. |
| `AccessDenied` on `AssumeRoleWithWebIdentity` | The role's trust policy rejects the token's `sub` or `aud`. Decode the token file and compare. |
| `ExpiredTokenException` | The token file is stale. The manager re-reads it on every exchange, so whatever writes it has stopped rotating it. |
| Requests to `169.254.169.254` timing out | Nothing configured a credential and the manager is not on EC2. The boot line says `credential_source="instance_metadata"`. |

## Operational Notes

- No external binary is required. Replication is embedded via the
  `rustyriver` crate. The former `litestreamBinary` config field has been
  removed, and Legion ignores it if it is still present in old configs.
- For Cloudflare R2, use the S3-compatible endpoint and `credentials`. R2 has
  no STS, so web identity does not apply there.
- Keep `ttlSeconds` comfortably above `renewIntervalSeconds` (default 15s / 5s).
- For manual failover (`none`), you must make sure only one writer is active.
- The lease lock object is written at `<dir(key)>/lock.json`.
- Disaster recovery (point-in-time restore from the replica) uses
  `rustyriver::restore`.

### Known limitations

- **EKS Pod Identity is not supported**, nor are `~/.aws/credentials`
  profiles or AWS SSO. The underlying `object_store` crate's AWS chain stops at
  the five sources above. On EKS use IRSA.
- **`legion start manager --restore-from` takes a local file.** Pulling a
  replica down from object storage onto a fresh box is a separate step today,
  authenticated by whatever tool you use for it. The restore a standby runs
  when it takes the lease does use these credentials.
- **No introspection beyond the boot log.** No CLI or RPC reports which
  credential source is in effect or when it next refreshes.
- **AWS-shaped auth only.** Every supported target (S3, R2, MinIO, Backblaze,
  Filebase, Supabase) speaks SigV4; there is no GCS-native or Azure path.
