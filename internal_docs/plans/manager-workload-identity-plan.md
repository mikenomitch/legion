# Manager Workload Identity for State Replication

> **Judgement calls:** [`../calls/manager-workload-identity-plan_calls.md`](../calls/manager-workload-identity-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 (credential providers for streaming
persistence: `environment`, `static`, `webIdentity`) is implemented. Phase 2
(the same provider for `--restore-from` / disaster recovery) and Phase 3 (a
`legion operator persistence credentials` introspection verb) are deferred and
named in [§9](#9-implementation-phases).

**Deliberately not built:** a Legion-*issued* manager identity — the manager
signing its own S3 credentials with its own OIDC key. [§6](#6-why-the-manager-does-not-use-legions-own-idp)
is the argument, and it is the central decision of this plan.

---

## 1. Problem statement

Legion's streaming persistence mode ships the manager's SQLite WAL to object
storage (`docs/persistence-streaming.md`). It is the durability story for a
single-manager cluster: lose the manager's disk, restore the replica.

Every credential on that path is a long-lived static secret today.

- The **replica** (`persistence.streaming.replicaUrl`) gets its credentials
  from rustyriver's URL semantics — `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY`, falling back to `LITESTREAM_*`, read from the
  manager process's ambient environment
  (`client/object_store.rs` `ObjectStoreConfig::from_url`).
- The **lease** (`persistence.streaming.lease.objectStorage`) takes
  `accessKeyId` / `secretAccessKey` as literal strings in `manager.jsonnet`
  (`src/structs/config.rs:1005`, consumed at
  `src/storage/persistence.rs:451`).

So an operator running streaming persistence today does one of:

```jsonnet
// manager.jsonnet — a permanent IAM user's keys, in config
lease: {
  provider: 'object_storage',
  objectStorage: {
    accessKeyId: std.extVar('R2_ACCESS_KEY_ID'),
    secretAccessKey: std.extVar('R2_SECRET_ACCESS_KEY'),
  },
},
```

```sh
# …or the same secret in the unit file / container env
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... legion start manager
```

Three things are wrong with this, in order of how much they matter:

1. **It is the exact thing workload identity exists to delete.** Legion tells
   job authors "no long-lived secret ever touches the job spec"
   (`docs/workload-identity.md:7`) and hands tasks 15-minute OIDC tokens
   instead. The manager — the *most* privileged process in the cluster, holding
   a credential that can write and delete the cluster's only backup — is the
   one component still on a static key. The story has a hole in the middle of
   it.
2. **The two credentials are configured in two different places, two different
   ways.** The replica reads process env; the lease reads config literals.
   Nothing checks they agree. An operator who rotates one and not the other
   gets a manager that replicates fine and silently stops being able to renew
   its lease, or vice versa.
3. **The path that most needs short-lived credentials cannot use them.** Even
   an operator who *has* a modern credential source — EKS IRSA, GKE workload
   identity, a SPIRE agent minting JWT-SVIDs — cannot use it, because
   rustyriver builds its store with `AmazonS3Builder::new()` rather than
   `from_env()`, so `object_store`'s built-in web-identity chain
   (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`) is never consulted. The
   credential exists on the host and Legion structurally cannot reach it.

The question this plan answers is the one an operator actually asks: *"Legion
gives my tasks federated identity — why is my control plane still holding an
access key?"*

## 2. Prior art

**Nomad.** Nomad's servers are not workload-identity clients. Server-side
snapshot shipping (`snapshot agent`, an Enterprise feature) authenticates to S3
with `aws_access_key_id`/`aws_secret_access_key` in the agent config, or — the
recommended path — with **no credentials at all**, falling through to the AWS
SDK's default chain so the EC2 instance profile answers. Nomad's own workload
identity is strictly for *allocations*; the servers use the platform's identity.
That split is the model we take.

**Consul.** Same shape: Consul's snapshot agent takes explicit keys or falls
through to the instance role. Consul's Vault integration for the servers uses
`auth_method` against an *external* Vault, never against Consul itself.

**Kubernetes.** etcd backup tooling (velero, etcd-backup-operator) runs as a
pod with a service account, and IRSA / GKE workload identity turns that service
account into cloud credentials. The identity is issued by the *cluster's* API
server — which is the thing being backed up. K8s gets away with this only
because the projected token is validated by AWS against the cluster's public
OIDC issuer, which is hosted on **S3 itself**, not on the API server. That
detail is the tell: even K8s does not let the backed-up component be the
reachability dependency for its own backup credentials. We cannot copy the
"host your JWKS on S3" trick, because Legion's JWKS is served by the manager
process.

**Litestream.** Static keys or the ambient AWS chain. No identity model.

**SPIFFE/SPIRE.** The generic answer for the no-cloud case: an agent on the host
writes a JWT-SVID to a file, and `AssumeRoleWithWebIdentity` exchanges it. This
is what makes the "platform identity" answer available on bare metal and not
just on EKS — worth stating explicitly, because it is the objection this design
has to survive.

**What we take:** Nomad's and Consul's split — control-plane processes
authenticate with the *platform's* identity, not the orchestrator's own. **What
we reject:** minting the manager's storage credentials from Legion's own IdP
(§6).

## 3. Jobs to be done, by persona

**Cluster operator**

- When I turn on state replication, I want the manager to pick up whatever
  credential my platform already gives it, so I do not have to create an IAM
  user and paste its key into a config file that lives in git.
- When my security team asks "what has standing write access to the backup
  bucket", I want the answer to be a role with a trust policy, not a key that
  was issued in 2024 and has never been rotated.
- When I already run EKS with IRSA (or GKE, or SPIRE), I want Legion to use it
  the same way every other pod on the cluster does, so the manager is not the
  one special case my credential-rotation runbook has to carve out.

**Security / platform admin**

- When someone compromises a manager, I want them to *not* automatically be
  able to erase the backups, so there is still something to restore from.
- When I audit which principals can write to the state bucket, I want each
  manager's access to be attributable and time-bounded, so a leaked credential
  stops working on its own.

**On-call responder**

- When replication stops, I want the log line to tell me whether it was the
  credential or the bucket, so I am not guessing at 3am whether the key
  expired.
- When I am restoring from the replica after losing every manager, I want the
  credentials to work while Legion is completely down, so restore does not
  depend on the thing I am restoring.

*Not in scope:* the **job author** persona — nothing here is visible in a job
spec. And the **autonomous agent** persona is only touched by phase 3 (a
structured way to read back which provider is in effect); phases 1–2 add no CLI
verb and no RPC.

### User scenarios

1. *EKS.* The operator annotates the manager's service account for IRSA. The
   projected token appears at
   `/var/run/secrets/eks.amazonaws.com/serviceaccount/token`. They set
   `credentials.provider: 'webIdentity'` with that path and a role ARN, delete
   the two static keys from `manager.jsonnet`, and restart. Replication and the
   lease both now use 1-hour session credentials that refresh themselves.
2. *Bare metal with SPIRE.* Same config, `tokenFile` pointing at the SVID the
   SPIRE agent writes. No cloud metadata service involved.
3. *Disaster recovery.* Every manager is gone. The operator runs the restore on
   a fresh box with the same role and token file. It works, because nothing on
   that path needs a running Legion.

## 4. Goals and non-goals

**Goals**

- Streaming persistence can authenticate to object storage with a short-lived,
  federated credential rather than a static key.
- The replica and the lease share **one** credential configuration. Configuring
  them separately becomes impossible rather than merely discouraged.
- The default behavior of an existing config does not change.
- The credential path does not depend on any Legion process being up, so it
  works during restore.

**Non-goals**

- **A Legion-issued manager identity.** Explicitly rejected — §6.
- Rotating or provisioning the *platform* credential. Legion consumes a token
  file; something else produces it.
- Non-AWS-shaped object storage auth (GCS HMAC-less, Azure SAS). The S3 path is
  what streaming persistence speaks, and every target it supports (S3, R2,
  MinIO, Backblaze, Filebase, Supabase) speaks SigV4.
- Extending this to the raft backend. Raft managers replicate to each other, not
  to object storage; there is no credential there to fix.
- Reworking artifact credentials. The provider built here is deliberately
  general enough to be reused by `artifacts` later (§10), but wiring it there is
  separate work.

## 5. Background — what exists today

- `src/storage/persistence.rs:382` `run_replica_loop` builds
  `ObjectStoreClient::new(ObjectStoreConfig::from_url(&parsed))`. rustyriver's
  `from_url` reads `AWS_*` / `LITESTREAM_*` env vars into `access_key_id` /
  `secret_access_key`, and `build_store` calls `AmazonS3Builder::new()` — never
  `from_env()`. **Consequence:** no ambient chain at all. No instance profile,
  no IRSA, no `~/.aws/credentials`. Only literal static keys from env.
- `src/storage/persistence.rs:445` `build_lease_store` builds a *separate*
  `AmazonS3Builder` from `lease.objectStorage`, and hands the result to
  `S3Leaser::new(store)` (`src/storage/persistence.rs:534`).
- rustyriver exposes `ObjectStoreClient::with_store(config, store)`
  (`client/object_store.rs:478`) — so we can build the store ourselves and hand
  it in, which is what makes a shared provider possible without forking
  rustyriver.
- `object_store` 0.11.2 exposes
  `AmazonS3Builder::with_credentials(Arc<dyn CredentialProvider<Credential = AwsCredential>>)`
  (`aws/builder.rs:689`). Its own `WebIdentityProvider` is `pub(crate)` and only
  reachable via `from_env()`, so we implement the exchange ourselves rather than
  contort the config into env vars.
- `src/manager/workload_identity.rs` is the manager's OIDC IdP: ES256 signer,
  JWKS, rotation, keys persisted in the `signing_key` table of the store. Node
  identities (`IssueNodeIdentityToken`) are the closest existing analog to what
  a "manager identity" would be.
- `ArtifactAuth::assume_role` (`src/structs/artifact.rs:74`) is declared and
  validated but **never consumed** — the worker passes a node-identity token as
  a `Bearer` credential (`src/artifacts/stage.rs:670`), which the S3 fetcher
  then rejects (`src/artifacts/fetchers/s3.rs:93`). There is no working
  `AssumeRoleWithWebIdentity` anywhere in the codebase yet. The provider built
  here is the first, and is written to be lifted into the artifact path later.

## 6. Why the manager does not use Legion's own IdP

This is the question the plan exists to answer, so it gets its own section
rather than a bullet in the non-goals.

The tempting design is a `managerIdentities` block that mirrors
`nodeIdentities` exactly: the manager mints itself an ES256 JWT with
`legion.io/scope = "manager"`, AWS validates it against Legion's published
JWKS, `AssumeRoleWithWebIdentity` returns session credentials, replication uses
them. It is a small amount of code — the signer, the JWKS, the discovery
document, and the rotation loop all already exist. It would make the identity
story uniform: *every* Legion component federates through Legion.

It is the wrong design, for three reasons that all sharpen to the same point.

**1. The credential's trust root is inside the thing it protects.** The ES256
private key lives in the `signing_key` table of the manager's SQLite store —
the store being backed up. Anyone who can read that store can mint a token that
grants write access to the backup bucket, which means *store compromise implies
backup destruction*. Backups exist precisely to survive control-plane
compromise; a credential minted by the control plane cannot provide that.
A platform-issued credential (IRSA, an instance profile, a SPIRE SVID) is
rooted outside Legion, so an attacker who owns the manager's database still has
to separately own the platform's identity to reach the backups.

**2. Restore is structurally impossible.** Disaster recovery reads the replica
to rebuild the store. Before the store exists there is no signing key, so there
is no token, so there are no credentials. A Legion-issued identity can *never*
serve `--restore-from`. The operator therefore has to keep a second, static
credential path for exactly the scenario the whole feature is for — which means
the feature deletes no secrets, it just adds a second mechanism next to the one
it failed to replace.

**3. Verification depends on the manager being reachable.** AWS STS validates
the token by fetching the issuer's JWKS over public HTTPS. That endpoint is
served by the manager (`src/manager/oidc_http.rs`). So the manager's ability to
write its own backup depends on the manager being externally reachable — and
during the incidents where the backup matters most, it is not. K8s dodges this
by hosting its cluster OIDC discovery document on S3 rather than on the API
server; Legion cannot, because the JWKS is generated by the running process.

The rule that falls out is worth stating in general form, because it is the
reusable part:

> **An identity may not be issued by the system it is used to protect.**

Workers satisfy this: a worker's node identity is issued by the manager, a
separate trust root, and a compromised worker cannot mint another worker's
token. The manager's *own* state backup cannot satisfy it against the manager's
own IdP, by construction.

So the answer to "should workload identity be extended to managers?" is **yes —
but 'manager identity' means the manager *presenting* an identity issued by the
platform, not one it signs for itself.** That is what this plan builds: the
consuming half of workload identity (exchange a signed JWT for provider
credentials, refresh it, never persist it) applied to the manager, with the
issuing half deliberately left to whatever already owns the host.

The consolation for the bare-metal operator who has no platform IdP is real and
should be said out loud in the public doc: `webIdentity` takes a *file*, not a
cloud. Anything that writes a JWT to disk — SPIRE, Vault's identity token
backend, a cron job with a signing key — satisfies it, and none of them are
inside Legion's own failure domain.

## 7. Proposed design

### 7.1 One credentials block, two consumers

```jsonnet
// manager.jsonnet
persistence: {
  type: 'streaming',
  streaming: {
    replicaUrl: 's3://acme-legion-state/prod',
    syncInterval: '1s',

    // NEW — one credential source for the replica *and* the lease.
    credentials: {
      provider: 'webIdentity',        // environment | static | webIdentity
      webIdentity: {
        tokenFile: '/var/run/secrets/eks.amazonaws.com/serviceaccount/token',
        roleArn: 'arn:aws:iam::123456789012:role/legion-manager-state',
        sessionName: 'legion-manager',      // optional
        durationSeconds: 3600,              // optional, 900..43200
        stsRegion: 'us-east-1',             // optional; regional STS endpoint
        stsEndpoint: 'https://sts.us-east-1.amazonaws.com',  // optional override
      },
    },

    lease: {
      provider: 'object_storage',
      key: 'leases/prod/primary.json',
      objectStorage: {
        bucket: 'acme-legion-state',
        region: 'us-east-1',
        // no accessKeyId / secretAccessKey — the credentials block answers
      },
    },
  },
},
```

The three providers:

| `provider` | Meaning |
| --- | --- |
| `environment` | **Default.** Today's behavior: `AWS_*`, falling back to `LITESTREAM_*`, from the manager's process environment. |
| `static` | Literal `accessKeyId` / `secretAccessKey` / optional `sessionToken` in config. What `lease.objectStorage` does today, promoted to cover both consumers. |
| `webIdentity` | Read an OIDC token from `tokenFile`, exchange it via `AssumeRoleWithWebIdentity`, cache and refresh the session credentials. |

### 7.2 Back-compatibility with `lease.objectStorage`

`lease.objectStorage.accessKeyId` / `secretAccessKey` keep working exactly as
they do today, and keep applying to the lease only. When `credentials` is
absent, nothing changes for any existing config. When both are set, the plan
takes the narrower one for the lease (explicit beats inherited) and warns once
at boot, because an operator who set both probably meant to delete one. When
`credentials.provider` is `webIdentity` **and** the lease carries static keys,
that is a hard boot failure rather than a warning: silently federating the
replica while the lease stays on a static key is precisely the split-brain
credential state this plan exists to remove.

### 7.3 The web-identity exchange

A new `src/storage/s3_credentials.rs`:

- `WebIdentityCredentials` implements
  `object_store::CredentialProvider<Credential = AwsCredential>`.
- `get_credential()` returns a cached `Arc<AwsCredential>` while it is fresh,
  and otherwise performs the exchange under a single-flight `tokio::sync::Mutex`
  so a burst of concurrent object-store calls produces one STS request.
- Freshness is `now < expiry - refresh_skew`, with a 5-minute skew — so
  credentials are replaced well before any in-flight multipart upload can be
  signed with an expiring key.
- **The token file is re-read on every exchange**, never cached. Projected
  service-account tokens rotate on their own schedule; caching the token is the
  classic IRSA bug.
- The exchange is a `POST` of a form-encoded body to the STS endpoint with
  `Action=AssumeRoleWithWebIdentity&Version=2011-06-15`. It is unsigned — the
  JWT *is* the authentication — which is why this needs no bootstrap
  credential.
- The XML response is parsed with `quick-xml`'s event reader (already in the
  lock file via `object_store`) for `AccessKeyId`, `SecretAccessKey`,
  `SessionToken`, `Expiration`. An `<Error><Code>` response is surfaced with its
  code and message.
- **Redaction is a type-level property**, not a discipline: `Debug` for the
  provider prints the role ARN and token *path* and nothing else, and the
  errors carry STS's code/message but never the token or the secret.

### 7.4 Failure behavior

A failed exchange fails the *operation*, not the process. The replica sync loop
already logs and retries on error (`src/storage/persistence.rs:409`), and the
lease loop treats a renew error as `Retain` (keep the lease, retry next tick) —
so an STS blip degrades to a delayed sync and a delayed renewal rather than a
spurious failover. Boot-time validation catches the misconfigurations that can
be caught statically (missing `roleArn`, unreadable `tokenFile`, out-of-range
`durationSeconds`) so those fail fast instead of at the first sync.

## 8. Config and struct changes

New in `src/structs/config.rs`:

- `StreamingCredentialsProvider` — `Environment` (default) | `Static` |
  `WebIdentity`, `#[serde(rename_all = "camelCase")]`.
- `StreamingCredentialsConfig` / `…Full` with `provider`, `static`,
  `webIdentity`.
- `StaticS3CredentialsConfig` / `…Full` — `accessKeyId`, `secretAccessKey`,
  `sessionToken`.
- `WebIdentityCredentialsConfig` / `…Full` — `tokenFile`, `roleArn`,
  `sessionName`, `durationSeconds`, `stsRegion`, `stsEndpoint`.
- `StreamingPersistenceConfig{,Full}` gains `credentials`.
- `StreamingCredentialsConfigFull::validate()` called from the manager's
  existing startup config validation.

No proto changes, no new RPC, no new CLI verb, no job-config field — so
`configs/jobs/maximal.jsonnet`, `src/cmd/capabilities.rs`, and
`tests/acl_endpoints.rs` are all untouched. `configs/manager.jsonnet` gains the
block, commented, with the defaults spelled out.

## 9. Implementation phases

**Phase 1 — credential providers for streaming persistence.** *(implemented)*
The config block, `s3_credentials.rs`, one shared `build_s3_store()` used by
both the replica (`ObjectStoreClient::with_store`) and the lease
(`S3Leaser::new`), boot validation, and the log line naming the effective
provider. Independently valuable: it deletes static keys from the replication
path and unifies the two credential sites even for operators who stay on
`environment`.

**Phase 2 — the same credentials for restore.** *(deferred)*
`legion start manager --restore-from` currently takes a local path
(`src/cmd/start.rs:152`). Teach it to accept the replica URL and pull through
`rustyriver::restore` with the phase-1 provider, so disaster recovery uses the
same federated credential as replication. Deferred because it is a separate
user-facing surface (a CLI argument that now accepts a URL) with its own
failure modes, and phase 1 stands alone without it.

**Phase 3 — introspection.** *(deferred)*
Surface the effective provider, the last successful exchange, and the current
credential expiry through the persistence status RPC, so an operator can see
"webIdentity, refreshed 4m ago, expires in 51m" instead of reading logs. Adds a
proto field, so it carries proto-comment and agent-surface obligations phase 1
does not.

## 10. Testing plan

Unit, in `src/structs/config.rs`:

- defaults — an absent `credentials` block yields `provider: environment` and
  changes nothing else;
- round-trip `Full → Config → Full`;
- validation — `webIdentity` without `roleArn`/`tokenFile` rejected;
  `durationSeconds` outside 900..=43200 rejected; `static` without both keys
  rejected; `webIdentity` alongside `lease.objectStorage` static keys rejected.

Unit, in `src/storage/s3_credentials.rs`:

- STS XML response parsing — the happy path, and an `<ErrorResponse>` surfacing
  code + message;
- expiry math — a credential inside the skew window is treated as stale;
- redaction — `Debug` output and error strings contain neither the token nor
  the secret (asserted on a fixture with recognizable sentinel values);
- token file re-read — two exchanges against a file whose contents changed in
  between send the second token, not the first.

Unit, in `src/cmd/start.rs`:

- the shipped `configs/manager.jsonnet` evaluates, deserializes, and validates.
  There was no guard on the sample manager config before this change — the job
  side has `configs/jobs/maximal.jsonnet` and the repo rule around it, the
  manager side had nothing — so a jsonnet typo or a renamed field would have
  been found by the next operator to run the sample.

Not covered by tests: a live STS exchange. It needs a real IdP and a real AWS
account; the parsing, expiry, and single-flight logic are what can break in
Legion's code and they are covered.

## 11. Rollout and backward compatibility

Default-off in the sense that matters: an existing `manager.jsonnet` with no
`credentials` block behaves byte-identically, because `environment` reproduces
today's `AWS_*` / `LITESTREAM_*` lookup. `lease.objectStorage` static keys keep
working. No migration, no state format change, no version skew concern — the
change is entirely inside one manager process's startup.

The one intentional new failure: `credentials.provider: 'webIdentity'` plus
lease static keys refuses to boot (§7.2), with an error naming both sites. That
config cannot exist today, so nothing breaks.

## 12. Open questions

- **Should `environment` become a real default-chain (instance profile, config
  file) rather than only static env vars?** Current lean: no, not in this plan.
  `AmazonS3Builder::from_env()` would pull in IMDS and the shared config file,
  which is *more* useful but silently changes what an existing deployment
  authenticates as — the opposite of the phase's back-compat promise. A fourth
  provider (`default`) is the honest way to add it later.
- **Should the artifact system's `auth.assumeRole` be wired to this provider?**
  Lean: yes, eventually, and the module is shaped for it — but the worker needs
  the fetching allocation's id threaded through first
  (`docs/workload-identity.md:452`), which is that plan's work, not this one's.

## 13. Acceptance criteria

Phase 1:

- A `manager.jsonnet` with no `credentials` block produces exactly today's
  behavior, verified by the defaults test.
- With `provider: 'webIdentity'`, both the replica sync and the lease renewal
  sign with STS session credentials obtained from the token file, and the boot
  log names the provider once.
- A credential nearing expiry is replaced without a manager restart and without
  a sync-loop error.
- Neither the token nor the secret key appears in any log line, error string, or
  `Debug` rendering — asserted, not reviewed.
- `cargo fmt --all`, `cargo clippy --all -- -D warnings`, and
  `cargo test --all --locked` pass.
