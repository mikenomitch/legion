# Manager Workload Identity for State Replication

> **Judgement calls:** [`../calls/manager-workload-identity-plan_calls.md`](../calls/manager-workload-identity-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 (one credential path for every streaming
store, the platform-identity chain made explicit and checked at boot, and the
lease's missing conditional puts) is implemented. Phase 2 (`--restore-from`
pulling from the replica URL) and Phase 3 (credential introspection on the
persistence status RPC) are deferred; see [§9](#9-implementation-phases).

**Deliberately not built:** a Legion-*issued* manager identity, where the
manager signs its own S3 credentials with its own OIDC key.
[§6](#6-why-the-manager-does-not-use-legions-own-idp) is the argument, and it
is the central decision of this plan. Also not built: a config-driven
`webIdentity` provider with its own STS client
([calls #2](../calls/manager-workload-identity-plan_calls.md#2-web-identity-comes-from-the-standard-aws-variables-not-a-config-provider-with-its-own-sts-client)).

---

## 1. Problem statement

The question that started this: *should workload identity be extended to
managers so they can stream their state to S3, Litestream-style?*

Streaming persistence talks to object storage from three places: the WAL
replica, the restore a standby runs when it takes the lease, and the lease
object. Before this work:

1. **The three were configured in two different places, two different
   ways.** The replica and the restore read `AWS_*`/`LITESTREAM_*` static keys
   from the process environment (rustyriver's URL semantics). The lease took
   literal keys from `lease.objectStorage` in the config. Nothing checked
   they agreed. The documented setup needed the same keys in both places, and
   an operator who rotated one and not the other got a manager that
   replicated fine and could not renew its lease, or the reverse.
2. **Federated credentials worked by accident, undocumented.** With no static
   keys set, both stores fell through to `object_store`'s own chain, which
   does read `AWS_WEB_IDENTITY_TOKEN_FILE`/`AWS_ROLE_ARN` and EC2 instance
   metadata. Nobody had written that down, so the public doc told operators
   to paste a key, and nothing told them which principal a manager was
   actually using. A half-configured web identity silently fell through to
   the instance role, a different and usually broader principal.
3. **The lease could not be acquired on real object storage.** The lease
   store was built without `conditional_put`, so `object_store` answered
   `S3Leaser`'s `PutMode::Create` with `NotImplemented`. Every manager logged
   `failed to acquire persistence lease` and stayed a standby. Only the
   in-memory store the tests use honoured the precondition.

Legion tells job authors "no long-lived secret ever touches the job spec" and
hands tasks 15-minute OIDC tokens. The manager, which holds the one credential
that can overwrite the cluster's backups, was the component pushed hardest
towards a static key.

## 2. Prior art

**Nomad.** Servers are not workload-identity clients. Snapshot shipping (the
Enterprise snapshot agent) takes `aws_access_key_id`/`aws_secret_access_key`,
or, the recommended path, no credentials at all, falling through to the AWS
SDK's default chain so the instance profile answers. Nomad's own workload
identity is strictly for allocations; the servers use the platform's identity.

**Consul.** Same shape: the snapshot agent takes keys or the instance role.
Consul's Vault integration for servers authenticates against an *external*
Vault, never against Consul itself.

**Kubernetes.** etcd backup tooling (velero, etcd-backup-operator) runs as a
pod with a service account, and IRSA or GKE workload identity turns that into
cloud credentials. The identity is issued by the cluster's API server, the
thing being backed up. K8s gets away with it only because AWS validates the
projected token against the cluster's OIDC discovery document, which EKS
hosts on S3 rather than on the API server. Even K8s does not make the
backed-up component the reachability dependency for its own backup
credentials.

**Litestream.** Static keys or the ambient AWS chain. No identity model.

**SPIFFE/SPIRE.** The generic answer off-cloud: an agent writes a JWT-SVID to
a file, and `AssumeRoleWithWebIdentity` exchanges it. That is what makes the
platform-identity answer available on bare metal, not just on EKS.

**What we take:** the Nomad/Consul split. Control-plane processes authenticate
with the platform's identity, through the standard AWS chain. **What we
reject:** minting the manager's storage credentials from Legion's own IdP
(§6).

## 3. Jobs to be done, by persona

**Cluster operator**

- When I turn on state replication, I want the manager to pick up whatever
  credential my platform already gives it, so I do not create an IAM user and
  paste its key into a config file.
- When I run EKS with IRSA (or SPIRE, or Vault), I want the manager to use it
  the way every other AWS client on the box does, so it is not the special
  case in my rotation runbook.
- When I do have to use keys (R2, MinIO), I want to set them once, so the
  replica and the lease cannot drift apart.
- When I set up the lease, I want it to actually work against my bucket.

**Security / platform admin**

- When someone compromises a manager, I want them to *not* automatically be
  able to destroy the backups, so there is still something to restore from.
- When I audit who can write the state bucket, I want the answer to be a role
  with a trust policy, not a key from two years ago.

**On-call responder**

- When replication stops, I want the boot log to say which credential source
  the manager is using, so I am not guessing whether it is a key, a role or
  the node's instance profile.
- When a credential is half-configured, I want the manager to refuse to boot
  and name the setting, not quietly authenticate as something else.

*Not in scope:* the **job author** (nothing here is in a job spec) and the
**autonomous agent** (phase 3 is where a structured readback would live;
phase 1 adds no CLI verb or RPC).

### User scenarios

1. *EKS.* The operator annotates the manager's service account for IRSA.
   EKS injects `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`. The manager
   boots with `credential_source="web_identity"` and the role ARN in the log,
   with no Legion config change. Replica, restore and lease all use
   one-hour session credentials that refresh themselves.
2. *Bare metal with SPIRE.* The SPIRE agent writes a JWT-SVID to
   `/run/spire/svid/jwt`. The operator registers the SPIRE issuer as an IAM
   OIDC provider and sets the same two variables in the systemd unit.
3. *R2.* The operator imports keys from `secrets.ncl` into
   `persistence.streaming.credentials`, once.

## 4. Goals and non-goals

**Goals**

- Every streaming store (replica, takeover restore, lease) authenticates the
  same way, through one function, from one config block or one environment.
- The platform-identity chain is the default, documented, and visible in the
  boot log.
- Misconfigurations that would silently change the principal fail the boot.
- The lease works against real S3-compatible storage.

**Non-goals**

- **A Legion-issued manager identity.** Rejected, §6.
- Rotating or provisioning the platform credential. Legion consumes it.
- EKS Pod Identity, `~/.aws/credentials` profiles and AWS SSO. The
  `object_store` 0.11 chain does not have them; Pod Identity is refused at
  boot rather than downgraded to the node's role.
- Non-SigV4 storage (GCS-native, Azure).
- The raft backend, which replicates between managers, not to object storage.
- Artifact credentials. `ArtifactAuth::assume_role` has its own gap and its
  own plan.

## 5. Background: what exists today

- `src/storage/persistence.rs` `run_replica_loop` and `restore_replica_to`
  built `ObjectStoreClient::new(ObjectStoreConfig::from_url(..))`.
  rustyriver's `from_url` reads `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
  (falling back to `LITESTREAM_*`) and its `build_store` uses
  `AmazonS3Builder::new()`. `AWS_SESSION_TOKEN` is never read, so temporary
  keys in the environment do not work.
- `object_store` 0.11.2's `AmazonS3Builder::build` (`aws/builder.rs:873`)
  resolves credentials itself whenever no keys were set on the builder:
  `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` from the process
  environment (even on `new()`), then a container credentials URI if one was
  set *on the builder*, then EC2 instance metadata. Verified by building a
  store with and without the IRSA variables and inspecting the provider.
  Its `WebIdentityProvider` re-reads the token file on every exchange and
  its token cache refreshes five minutes before expiry.
- `build_lease_store` built a separate `AmazonS3Builder::new()` from
  `lease.objectStorage` with optional literal keys, no `conditional_put` and
  no `allow_http`. `S3Leaser` acquires with `PutMode::Create` and renews with
  `PutMode::Update`; `object_store` maps either to `NotImplemented` when
  `conditional_put` is unset (`aws/mod.rs:172`).
- rustyriver exposes `ObjectStoreClient::with_store(config, store)`, so Legion
  can build the replica store itself without forking rustyriver.
- `src/manager/workload_identity.rs` is the manager's OIDC IdP; its keys live
  in the `signing_key` table of the store.

## 6. Why the manager does not use Legion's own IdP

The tempting design is a `managerIdentities` block that mirrors
`nodeIdentities`: the manager mints itself an ES256 JWT, AWS validates it
against Legion's JWKS, `AssumeRoleWithWebIdentity` returns session
credentials. The signer, JWKS, discovery document and rotation loop already
exist, so it is a small amount of code, and it would make the identity story
uniform.

It is the wrong design, for three reasons that sharpen to one point.

**1. The trust root is inside the thing it protects.** The signing key lives
in the store being backed up. Anyone who can read that store can mint a token
that writes the backup bucket: store compromise implies backup destruction.
Backups exist to survive control-plane compromise. A platform-issued
credential is rooted outside Legion, so an attacker who owns the manager's
database still has to separately own the platform identity.

**2. Restore is structurally impossible.** Before the store exists there is no
signing key, so no token, so no credentials. A Legion-issued identity can
never serve a restore, so the operator keeps a second static credential for
exactly the scenario the feature is for.

**3. Verification depends on the manager being reachable.** STS fetches the
issuer's JWKS over public HTTPS, from the manager. During the incidents where
the backup matters most, the manager is often not reachable. K8s dodges this
by hosting discovery on S3; Legion cannot, because the JWKS is generated by
the running process.

> **An identity may not be issued by the system it is used to protect.**

Node identities satisfy it: the manager is a separate trust root from the
worker. The manager's own backups cannot, against its own issuer. So "manager
workload identity" means the manager *presenting* a platform identity, and the
issuing half belongs to whatever already owns the host.

## 7. Design

### 7.1 One credential path, three consumers

```nickel
persistence.streaming = {
  replicaUrl = "s3://acme-legion-state/prod",
  # Optional. Unset = the platform chain below.
  credentials = {
    accessKeyId = secrets.accessKeyId,
    secretAccessKey = secrets.secretAccessKey,
    sessionToken = secrets.sessionToken, # optional, temporary keys
  },
  lease = {
    provider = "object_storage",
    objectStorage = { bucket = "acme-legion-state", region = "us-west-2" },
  },
}
```

`src/storage/s3_credentials.rs` resolves one `CredentialSource`, in order:

1. `persistence.streaming.credentials`.
2. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`).
3. `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` (object_store's provider).
4. `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` (passed to the builder, since
   `new()` does not read it).
5. EC2 instance metadata (object_store's fallback).

`build_s3_store(location, source)` builds every store. The replica and
restore use rustyriver's URL parse for bucket, region, endpoint and
path-style, then hand Legion's store to `ObjectStoreClient::with_store`, with
rustyriver's parsed keys cleared. The lease uses `lease.objectStorage` for
the location and sets `conditional_put = ETagMatch`.

`lease.objectStorage.accessKeyId`/`secretAccessKey` are removed, and the block
denies unknown fields, so a config that still has them fails to load instead
of authenticating the lease as whatever the ambient chain finds.
`LITESTREAM_ACCESS_KEY_ID`/`LITESTREAM_SECRET_ACCESS_KEY` are no longer read.

### 7.2 Boot-time checks

`StreamingPersistenceDriver::start` resolves the source when the replica is
`s3://` or the lease is `object_storage`, and fails the boot on:

- half a key pair, in the config or the environment;
- half a web identity (one of the two variables);
- `AWS_CONTAINER_CREDENTIALS_FULL_URI` (EKS Pod Identity), which object_store
  does not support and would otherwise downgrade to the node's instance role;
- web identity with the lease region left at `auto`, since object_store
  derives the STS endpoint from the store's region.

It then logs `credential_source` (and, for web identity, the role ARN and
token path). Keys are never logged; `StreamingCredentialsConfig` has a
redacting `Debug`.

### 7.3 Failure behavior

Runtime credential failures fail the operation, not the process: the sync
loop logs and retries on its next tick, and a lease renewal error keeps the
lease until a whole TTL passes without a successful renewal (the existing
`LeaseTransition::Retain` path).

## 8. Config and struct changes

- `StreamingPersistenceConfig{,Full}.credentials: Option<StreamingCredentialsConfig>`.
- `StreamingCredentialsConfig { access_key_id, secret_access_key, session_token }`,
  `deny_unknown_fields`, redacting `Debug`.
- `StreamingObjectStorageLeaseConfig{,Full}`: `access_key_id` and
  `secret_access_key` removed; `deny_unknown_fields` on the partial.
- `configs/manager.ncl` documents the block (commented out).

## 9. Implementation phases

1. **One credential path (shipped).** §7. Unit tests for resolution order,
   boot refusals, redaction, provider selection and the lease's conditional
   put.
2. **`--restore-from` from the replica URL (deferred).** Today it takes a
   local file. Teaching it a replica URL is a new CLI surface with its own
   failure modes (partial download, which TXID, overwrite rules); it would
   reuse `build_s3_store` and the same resolution.
3. **Credential introspection (deferred).** A `credentialSource` field on the
   persistence status RPC, with proto comments, the capability catalog and an
   MCP readback. The boot log is the only readback until then.

## 10. Testing plan

- `s3_credentials` unit tests: config outranks the environment; the ambient
  order; empty variables count as unset; half pairs and half web identities
  fail; Pod Identity is refused; secrets never reach `Debug`; the built store
  uses the static, task-role or instance-metadata provider as resolved;
  conditional put is only on when asked.
- `persistence` unit tests: credentials are resolved only when object storage
  is used; a web-identity lease needs a real region; the replica client holds
  no keys and uses Legion's prebuilt store; the lease store sends ETag
  preconditions.
- `config` unit test: one block for both consumers; lease keys and unknown
  credential fields are rejected.
- End to end against a local S3 emulator: a manager with an object-storage
  lease and an `s3://` replica acquires the lease, becomes primary and ships
  WAL, using keys from `credentials`.

Web identity against real STS is not exercised in CI: object_store hard-codes
the `sts.<region>.amazonaws.com` endpoint, so it cannot be pointed at an
emulator.

## 11. Rollout

Breaking, by design (AGENTS.md: no installed base):

- `lease.objectStorage.accessKeyId`/`secretAccessKey` fail to load. Move them
  to `persistence.streaming.credentials`.
- `LITESTREAM_ACCESS_KEY_ID`/`LITESTREAM_SECRET_ACCESS_KEY` are ignored. Use
  the `AWS_*` names.
- A config that previously relied on the lease and replica using *different*
  credentials cannot express that any more.
- New boot failures for half-configured credentials and Pod Identity.

## 12. Acceptance criteria

- On EKS with IRSA, a streaming manager replicates and holds the lease with
  no credentials in its config, and logs `credential_source="web_identity"`.
- The replica, the takeover restore and the lease can never use different
  credentials.
- The object-storage lease is acquired against S3-compatible storage that
  supports conditional writes.
- Every half-configuration in §7.2 stops the boot with a message naming it.

## History

A first draft of this work (an unmerged branch from 2026-07-31) concluded that
`AmazonS3Builder::new()` never consults the web-identity chain and built a
config-driven `webIdentity` provider with a hand-written STS client and XML
parser (~640 lines, two new dependencies). The premise was wrong (§5), and the
redesign keeps that draft's §6 argument and its single-block shape while
leaning on `object_store`'s own providers. The draft also did not touch the
lease's missing conditional puts. See
[calls #2](../calls/manager-workload-identity-plan_calls.md#2-web-identity-comes-from-the-standard-aws-variables-not-a-config-provider-with-its-own-sts-client).
