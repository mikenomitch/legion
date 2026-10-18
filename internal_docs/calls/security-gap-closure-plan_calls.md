# Judgement calls: closing the known security gaps (October 2026)

Plan: [security-gap-closure-plan.md](../plans/security-gap-closure-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Raft peers authenticate a key derived from the cluster secret, not a CA-issued certificate

**Call.** Every manager derives one Ed25519 key from the cluster secret
(HKDF-SHA256, salt `legion-raft-transport`) and a self-signed certificate for
`raft.legion`. The API port presents it to a client asking for that name;
peers dial TLS with SNI `raft.legion` and accept exactly that public key. The
secret in request metadata stays the client's half of the authentication.

**Alternatives.** (a) Issue each manager a `manager.legion` certificate from
the cluster CA and verify peers against the CA, as workers do. (b) A separate
raft port with its own operator-supplied certificates, like Consul's
`verify_incoming`.

**Trade-off.** Every manager holds the same private key, so the transport does
not tell managers apart; anyone with the secret is a peer. That was already
true of the plaintext transport, where the secret alone authenticated. A
leaked secret is now a TLS key leak too, and rotating the secret is the only
rotation.

**Why (simplicity).** The cluster CA lives in the replicated store, so it
cannot exist before raft elects a leader, and a joining manager has no store
when it first dials. The cluster secret is the one thing every manager holds
from its first moment (minted at bootstrap, carried in the raft join token).
Deriving from it needs no new config, no ordering, and no new port; (b) would
have added a second certificate story operators must run.

## 2. Naming a KV key in a job needs `kv:read` on it, everywhere a job can name one

**Call.** Admission (run, template run and render, plan) checks `kv:read` on
every key a spec resolves (`$path` references, `kvInjections` env and files,
the `{{ kv "…" }}` lookups in their templates, a `templateKv` pointer) and on
every artifact `auth.kv` the spec names. Cluster `artifacts.auth` matchers are
not checked: they are the operator's.

**Alternatives.** Keep the documented trust model ("job submit implies reading
any KV a job names") and recommend separate clusters for tenants, as the
artifacts doc did.

**Trade-off.** Jobs whose submitters lack `kv:read` on keys they reference now
fail at submit. A `kv:write` holder on a `templateKv` key can still make that
template name a key the original submitter could not read; the re-render does
not re-check (TODO). Accepted: catching it needs the submitter's identity at
re-render time.

**Why (simplicity).** One rule (naming a key is reading it) already covered
`certFrom` and `connect` credentials. Applying it everywhere is easier to
explain than a list of exceptions, and without it `kv` path rules protected
nothing from anyone who could deploy.

## 3. `GetArtifactCredential` serves only what the node's work names

**Call.** The manager derives the paths a node may read: `auth.kv` on
artifacts of allocations placed on it, on the task of every live boot ticket
for its pool (speculative staging happens before any allocation exists), and
its pool's `contentStore.auth.kv`. Anything else is refused before the read,
with `NotFound`:

```text
NotFound: kv team/config is not an artifact credential of node edge-7: no allocation placed on it, live boot ticket for its pool, or contentStore of its pool names it
```

**Alternatives.** Have the worker present the boot ticket (or allocation id)
with each request, and check exactly that one. Separately, `PermissionDenied`
for the refusal: it reads as an ACL failure, but the node is already
authenticated and bound, and every other "your node has no such thing" answer
on the worker plane is `NotFound`.

**Trade-off.** A compromised node in a pool can read the artifact credentials
of every job with a live boot ticket for that pool, not just the one it is
starting. Those are jobs it may legitimately be asked to start.

**Why (simplicity).** The stager's credential source has no allocation
context, and threading a ticket through staging would change the artifact
plugin seam for one check. The node-scoped set matches how
`GetIngressCertificates` and `GetUpstreamCredentials` already bind.

## 4. Host commands, node files, and the node's identity are host-level

**Call.** A task is host-level (needs `host = true` on `job:write`) when it
declares an `exec` health check (the worker runs it on the host, as root,
whatever the task's runtime), an artifact or `image` is `file://`, a git
source uses the `file` transport, or an artifact names `auth.nodeIdentity`.

```console
error: permission denied: job "build" has a host-level task ("fetch" fetches artifact file:///etc/legion/ from the node's own filesystem). Submitting it needs a job:write rule with host = true
```

**Alternatives.** (a) Drop `file` from the default `allowedSchemes`, like
plain `http`. (b) Allow `auth.nodeIdentity` only through cluster matchers.
(c) Run `exec` checks inside the task's runtime (`runc exec`, the sandbox
guest agent), as Nomad runs script checks in the task's context.

**Trade-off.** Single-tenant clusters where every deployer is trusted see no
change (management tokens hold `host`); multi-tenant ones need a host grant
for local-file sources and for `exec` checks on containers and sandboxes,
which is the right answer only until (c) exists (TODO).

**Why (simplicity).** "Host-level" is already the line between deploy and
root on every node, with one grant and one error message. (a) would have
broken dev flows on a management token for no gain there; (b) keeps the
operator path and only moves the spec path behind the grant that already
means "may act as the node".

## 5. A job's `subTemplate` must be on the operator's allowlist

**Call.** `workloadIdentity.allowedSubTemplates` lists the subject shapes a
job may name; the cluster default is always allowed. Checked at admission and
again when a token is minted.

```nickel
workloadIdentity = {
  allowedSubTemplates = ["{region}:{job}:{group}:{task}:{instance}"],
},
```

**Alternatives.** (a) Remove per-identity templates. (b) Accept any template
that extends the cluster default with more `{field}` segments, which cannot
collide with it.

**Trade-off.** A job that needs a new subject shape needs an operator first.

**Why (simplicity).** Any two templates may render the same subject for
different workloads, and whether that matters depends on the relying party's
policies, which only the operator knows. (b) is sound but needs a rule users
must learn and that `{region}`-first clusters would still trip; (a) drops a
documented multi-provider use.

## 6. Artifact HTTP fetches follow redirects themselves, judged by the host guard

**Call.** The HTTP fetcher follows up to five redirects itself. Each hop's
host is resolved, every address judged by the worker's host guard, and the
client pinned to the addresses that pass. Credentials go to the source's own
origin only, and an `https` source may not redirect to `http`. An S3
`endpoint` is judged but not pinned; git and OCI fetches are not judged.

**Alternatives.** A reqwest custom DNS resolver plus a redirect policy, which
keeps reqwest's redirect handling.

**Trade-off.** A fetch that relied on an https→http redirect now fails.
Loopback artifact servers need a `hostGuard.allow` entry. Git and OCI remain
unjudged (TODO).

**Why (simplicity).** reqwest's resolver does not see the port, and the host
guard's `allow` entries are per port, so a resolver would have had to guess.
A manual loop also gives one place to drop credentials on a cross-origin hop,
which reqwest does for `Authorization` but not for custom headers.

## 7. Writes into task directories never follow a symlink

**Call.** `util::write_file_beneath` opens each directory below the task
directory with `O_NOFOLLOW`, creates the temp file `O_EXCL | O_NOFOLLOW`, and
renames inside the opened directory. Templates, identity tokens, KV files,
and container declared files use it. Absolute `exec` destinations (chosen by
a `host` submitter) are written as given.

**Alternatives.** `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)`.

**Trade-off.** A link anywhere on the path fails the write, even one the task
made for its own convenience inside its directory.

**Why (simplicity).** `openat2` is Linux 5.6+ only and Legion workers also run
on macOS; the component walk is portable and has one behavior everywhere.

## 8. A join token cannot take over a live node; a lost one can be rejoined

**Call.** A join-token registration that names an existing node is refused
while that node is not `lost`, unless the request also carries the node's
own token (a restart).

**Alternatives.** (a) Per-node join tokens. (b) Refuse every existing name.

**Trade-off.** A compromised worker can still claim the name of a node that
is down. A worker restarted without its state directory inside the
lost-node window is refused (and exits; its service manager restarts it)
until the old node is marked lost. A running worker whose node token was
revoked keeps retrying from its heartbeat loop and rejoins the same way,
about one lost-node window later.

**Why (simplicity).** (b) breaks the "worker lost its disk and rejoins" path
(architecture-audit calls 3); (a) changes how every worker bootstraps. A down
node has no allocations or traffic to impersonate.

## 9. Certificate DNS labels are injective

**Call.** A node, fleet, or manager name that is already a plain label is its
own label; any other name becomes `head--<16 hex digits of its SHA-256>`, a
form no plain label takes.

**Alternatives.** (a) Scan for a colliding label at registration. (b) Restrict
node names to plain labels.

**Trade-off.** Names with dots, capitals, or underscores get less readable
certificate names, and those names changed.

**Why (simplicity).** (a) is a race and a full scan on every registration;
(b) would refuse hostnames like `ip-10-0-1-7.ec2.internal` that workers use
as names today.

## 10. Exec tasks get an allowlisted environment

**Call.** An `exec` task and a session into one start from `PATH`, `LANG`,
`LANGUAGE`, `TZ`, and `LC_*` from the worker, plus `HOME`, `USER`, `LOGNAME`,
and `SHELL` for the user it runs as (the worker's own when it sets no
`user`).

**Alternatives.** Nomad's denylist (`env.denylist`).

**Trade-off.** A task that read some other worker variable (`http_proxy`,
`SSL_CERT_FILE`) must set it in its own `env`.

**Why (simplicity).** A denylist has to know every credential name a worker's
environment might hold; an allowlist fails closed.

## 11. Object names are restricted at admission

**Call.** Job, group, and task names are `[A-Za-z0-9._-]`, 1–128 characters,
never starting with `.`. The worker re-checks before creating a directory.

**Alternatives.** Sanitize names on the worker only, like the sandbox's
`path_safe`.

**Trade-off.** Names with spaces or slashes are refused at submit.

**Why (simplicity).** A name that means two things (one in the API, another on
disk) is a debugging trap; one charset everywhere is explainable.

## 12. A reused idempotency key with another request fails

**Call.** The stored key carries a SHA-256 fingerprint of the request (its
JSON form without the key). A lookup that finds the same key under another
fingerprint fails with `INVALID_ARGUMENT`.

**Alternatives.** (a) A `request_hash` column and migration. (b) Treat a
different request as a new key and run it.

**Trade-off.** A client that changes a request field on retry (a timestamp,
say) gets an error instead of a replay.

**Why (simplicity).** (a) needed a migration for one comparison; (b) silently
runs twice what the client believes is one operation.

## 13. `RatifyClaim` requires the ticket and that it matches the claim

**Call.** A claim without a ticket is refused, and the ticket must name the
claim's job config and task, at a gate no lower than requested.
`IssueBootTicket` refuses a `job_config_id` of another job.

**Alternatives.** Record every admitted claim on the manager and accept
ticketless claims that match a record.

**Trade-off.** None found: every claim a worker admits carries its ticket.

**Why (simplicity).** The worker never ratifies without one, so the check
costs nothing and removes a path to placing any config on any node.

## 14. JWT and OIDC methods must name an audience

**Call.** A method verified by keys (JWKS or OIDC discovery) must set
`boundAudiences`; an OIDC method's `oidcClientId` counts. `jwksUrl` and
`issuer` must be `https` unless they point at loopback. HMAC methods are
exempt.

**Trade-off.** Existing configs without an audience fail manager startup.

**Why (simplicity).** An issuer mints tokens for many services; without an
audience every one of them logs in here.

## 15. A workload-identity login ends with its identity token

**Call.** An ACL token minted by the `legion` method expires no later than
the identity token presented for it.

**Alternatives.** Record the instance and identity epoch on the token and
check them on every request.

**Trade-off.** After a fork, a copied login token stays valid until the
identity token it came from would have expired (minutes, by default), not
until the epoch moves.

**Why (performance).** The epoch check would put a lookup on the ACL hot path
for one flow; clamping bounds the window with no per-request cost.

## 16. An ingress peer is a node only on a cluster-verified chain

**Call.** A client certificate with a `legion://node/` URI is treated as a
Legion node (trusted to name the workload it forwards for) only when the
chain was verified against the cluster CA (`clientCa = "legion"`, or a mesh
certificate). Otherwise the URI is just that client's identity.

**Trade-off.** None for mesh traffic; an operator CA can no longer mint node
identities.

## 17. Tar extraction refuses hard links

**Call.** Entries are unpacked with the tar crate's `unpack_in`, and hard-link
entries are refused.

**Alternatives.** Resolve hard links inside the destination and keep them.

**Trade-off.** Archives that use hard links (some language toolchains ship
them) must be repacked.

**Why (simplicity).** A hard link names an existing inode; there is no way to
check one that is as simple as refusing it.

## 18. Direct exec revocation still waits for a heartbeat

**Call.** Not changed. A revoked direct exec session ends on the worker's next
heartbeat (about 2 seconds).

**Alternatives.** A manager-to-worker `RevokeExecSessions` push when a token,
policy, or exec key is deleted.

**Trade-off.** A revoked session keeps running for up to one heartbeat.

**Why (simplicity).** The manager does not keep which nodes hold sessions
(workers report them per heartbeat), and with several managers the deleting
one is often not the one that heard them. The push needs that state; the
window is already short. TODO.

## 19. Recorded idempotency replies read as NULL through raw SQL

**Call.** `idempotency_record.response_message` joins the redacted columns,
because a keyed `IssueBootTicket` records the whole ticket.

**Alternatives.** Encrypt that reply under the keyring.

**Why (simplicity).** No operator reads replay payloads through SQL; hiding
the column is one line.

## 20. The rest

The credential pruner runs with ACLs off too, expired boot tickets are
pruned hourly on the scheduler-lane leader, secret files are `0600` from
creation, delegated exec tickets may not be future-dated, the `authz` cache
key includes the allocation and webhook URL, git fetches are pinned to their
URL's transport, and template renders and runs don't reveal whether a
template the caller can't read exists. Each is the obvious fix with no real
alternative.
