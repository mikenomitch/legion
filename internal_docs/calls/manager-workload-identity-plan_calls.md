# Judgement calls: manager workload identity for state replication

Plan: [`../plans/manager-workload-identity-plan.md`](../plans/manager-workload-identity-plan.md)

Entries are ranked by project impact, highest first; the number is the rank.

## 1. The manager presents a platform identity and never issues one for itself

**The call.** Streaming persistence authenticates with an identity the
platform issues (IRSA, SPIRE, Vault, an instance profile, an ECS task role).
There is no `managerIdentities` block, and the manager never signs a token for
its own storage access.

**The alternative.** Mirror `nodeIdentities`: the manager mints an ES256 JWT
from its own issuer and exchanges it with `AssumeRoleWithWebIdentity`. The
signer, JWKS, discovery and rotation already exist, so it is little code, and
"every Legion component federates through Legion" is a tidy story.

**The trade-off.** An operator with no platform IdP (bare metal, no SPIRE, no
Vault) gets no federated option from Legion alone and is left with keys. The
identity story is less uniform: tasks and nodes receive, the manager presents.

**Why.** Security, and it is not close
([plan §6](../plans/manager-workload-identity-plan.md#6-why-the-manager-does-not-use-legions-own-idp)).
The signing key lives in the store being backed up, so a self-issued
credential makes store compromise imply backup destruction. A restore has no
store to sign from. STS validates by fetching the manager's own JWKS, so a
backup would depend on the manager being reachable. *An identity may not be
issued by the system it is used to protect.* The public docs say this in
[persistence-streaming.md](../../docs/persistence-streaming.md#why-the-manager-does-not-use-legions-own-oidc-issuer)
and [workload-identity.md](../../docs/workload-identity.md#the-managers-own-identity-and-why-legion-does-not-issue-it).

## 2. Web identity comes from the standard AWS variables, not a config provider with its own STS client

**The call.** Web identity is `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`
in the manager's environment, exchanged by `object_store`'s own
`WebIdentityProvider`. There is no `credentials.webIdentity { tokenFile,
roleArn, stsEndpoint, durationSeconds }` block.

```ini
Environment=AWS_ROLE_ARN=arn:aws:iam::123456789012:role/legion-manager-state
Environment=AWS_WEB_IDENTITY_TOKEN_FILE=/run/spire/svid/jwt
```

**The alternative.** A first draft of this work built exactly that block, with
a hand-written `AssumeRoleWithWebIdentity` client, a quick-xml response
parser and its own single-flight cache: ~640 lines and two new dependencies.
It argued `object_store`'s provider was unreachable because rustyriver uses
`AmazonS3Builder::new()` rather than `from_env()`. That was wrong:
`AmazonS3Builder::build` reads the two variables from the process environment
whichever constructor was used (`aws/builder.rs:901`), which a probe build
confirmed. A config block would still have real advantages: it lives in
`manager.ncl` next to everything else, can set a custom STS endpoint
(GovCloud, China partitions) and a session duration, and does not depend on
how the process was launched.

**The trade-off.** No STS endpoint override: `object_store` uses
`sts.<region>.amazonaws.com` for the store's region, so the China partitions
are out, and the lease's region has to be a real AWS region (a boot check
enforces it, call #5). Sessions are always one hour. Web identity is
configured outside `manager.ncl`. It cannot be exercised against an emulator
in CI, because the endpoint is fixed.

**Why.** Simplicity and correctness. On EKS, IRSA injects these variables
already, so the common case is zero Legion config. Every AWS SDK and CLI on
the box reads the same two variables, so an operator's runbook carries over.
And the provider that refreshes credentials is code we do not own, test or
patch: the token re-read, the five-minute refresh skew, retries and
`sensitive` request marking come with `object_store`. Revisit if someone needs
GovCloud/China or a custom STS endpoint; a config block can then set the
endpoint on an `object_store` provider rather than reimplement STS.

## 3. One credentials block for the replica, the restore and the lease, and the lease's own keys are gone

**The call.** `persistence.streaming.credentials { accessKeyId,
secretAccessKey, sessionToken }` is the only place keys go, and every store is
built by one function from one resolved source.
`lease.objectStorage.accessKeyId`/`secretAccessKey` are removed, and that
block now denies unknown fields:

```
unknown field `accessKeyId`, expected one of `bucket`, `region`, `endpoint`
```

**The alternative.** Keep the lease's keys and let them win for the lease
only, so existing configs keep working (the first draft's choice), and fail
the boot only on the one split that looked like a half-done migration.

**The trade-off.** Configs with lease keys stop loading and must move them.
A deployment that genuinely wanted the lease and the replica in different
accounts with different principals cannot express that any more.

**Why.** Simplicity, and AGENTS.md's "no compatibility shims". Two credential
sites that must agree is the bug: the old documented setup needed the same
keys in the environment (replica) and in `secrets.ncl` (lease), and drift
between them fails exactly when it matters, at failover. Denying unknown
fields matters as much as the removal. Silently ignoring a stale
`accessKeyId` would authenticate the lease as whatever the ambient chain finds.

## 4. The lease store sends ETag preconditions

**The call.** The lease's store is built with `conditional_put = ETagMatch`,
so `S3Leaser`'s `PutMode::Create` and `PutMode::Update` go out as
`If-None-Match: *` and `If-Match: <etag>`.

**The alternative.** Leave the lease alone in a credentials change, and file
the bug separately.

**The trade-off.** Requires object storage that implements conditional
writes: AWS S3 (since late 2024), R2 and MinIO do; an older S3-compatible
store gets `PreconditionFailed` or `NotImplemented` and the manager stays a
standby, as before.

**Why.** Without it the lease could never be acquired on real object storage.
`object_store` answers both put modes with `NotImplemented` when
`conditional_put` is unset, so every manager logged
`failed to acquire persistence lease … Operation not yet implemented` and
stayed a standby forever. Reproduced against a local S3 emulator before the
fix; after it, a manager acquires the lease, becomes primary, ships WAL, and a
second manager restores from the replica on takeover. The unit tests never
caught it because they use an in-memory store that honours the precondition
without being asked. It is a one-line fix in a function this change rewrites
anyway, and a credential change whose test is "the lease works" cannot ship
around it.

## 5. Misconfigurations that would change the principal fail the boot

**The call.** At startup, when the replica is `s3://` or the lease is
`object_storage`, the manager refuses to boot on: half a key pair (config or
environment), one web-identity variable without the other,
`AWS_CONTAINER_CREDENTIALS_FULL_URI` (EKS Pod Identity), and web identity with
the lease region left at `auto`.

```
AWS_CONTAINER_CREDENTIALS_FULL_URI is set (EKS Pod Identity), which streaming
persistence does not support: it would fall back to the node's instance role.
Use IRSA (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN) or persistence.streaming.credentials
```

**The alternative.** Do what `object_store` does: use whatever resolves, and
let a half-configured web identity or Pod Identity fall through to instance
metadata.

**The trade-off.** These checks mirror `object_store` 0.11's resolution order
by hand, so an upgrade that changes its chain (adding Pod Identity, say) needs
this code updated too. A deployment that set a stray `AWS_ROLE_ARN` and
relied on the instance role now fails to boot.

**Why.** Security. Falling through to instance metadata means authenticating
as a different, usually broader principal (on EKS, the node's role), and the
symptom is a manager that works, as the wrong identity. A boot failure naming
the variable is a two-minute fix; a silent principal switch is an audit
finding. The region check is cheaper still: `object_store` builds the STS URL
from the store's region, and `sts.auto.amazonaws.com` fails as a DNS error
far from its cause.

## 6. The resolved source is logged at boot; there is no introspection RPC yet

**The call.** One boot line names the source
(`credential_source="web_identity"`, plus the role ARN and token path for web
identity). Keys never appear, and `StreamingCredentialsConfig` has a redacting
`Debug`. No status field, CLI verb or MCP tool reports it.

**The alternative.** A `credentialSource` field on the persistence status RPC
now, with the proto comments, capability catalog entry and MCP readback that
an agent surface requires.

**The trade-off.** An agent or a responder without log access cannot ask the
cluster which principal it is using, or when the credential next refreshes.

**Why.** Scope. The log line answers the on-call question ("key, role or
instance profile?"); the RPC is a new public surface with its own obligations
and belongs in its own change (plan phase 3, tracked in TODO.md).

## 7. `LITESTREAM_*` key variables are no longer read

**The call.** Environment keys are `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
(+ `AWS_SESSION_TOKEN`) only. rustyriver's parsed keys, which fall back to
`LITESTREAM_ACCESS_KEY_ID`/`LITESTREAM_SECRET_ACCESS_KEY`, are cleared
before its client is built. `LITESTREAM_S3_ENDPOINT` still supplies an
endpoint, since that is URL parsing, not credentials.

**The alternative.** Keep the `LITESTREAM_*` fallback for operators coming
from Litestream.

**The trade-off.** A Litestream migrant who sets only `LITESTREAM_*` falls
through to instance metadata, which the boot line shows as
`credential_source="instance_metadata"`.

**Why.** Simplicity: one set of names, the ones every AWS tool reads, and the
lease and replica now share one resolver that would otherwise need two name
families. Legion has no installed base to migrate. Clearing rustyriver's
copies also keeps keys out of its `ObjectStoreClient` `Debug` output.

## 8. `--restore-from` stays a local path

**The call.** Disaster recovery onto a fresh box still takes a local file; the
replica is pulled down out of band. Only the takeover restore (a standby
taking the lease) uses the shared credentials.

**The alternative.** Teach `--restore-from` to accept the replica URL and pull
through `rustyriver::restore` with `build_s3_store`.

**The trade-off.** The scenario where a non-Legion-dependent credential
matters most (every manager gone) still needs a separate tool.

**Why.** It is a new CLI surface with its own failure modes (which TXID,
partial downloads, overwrite rules, `--force`), and doing it properly is its
own change. Deferred as plan phase 2 and tracked in TODO.md; the
public doc says so under Known limitations.
