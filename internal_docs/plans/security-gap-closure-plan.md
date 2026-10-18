# Closing the known security gaps (October 2026)

> **Judgement calls:** [security-gap-closure-plan_calls.md](../calls/security-gap-closure-plan_calls.md)

**Status: SHIPPED**, except what is deferred below. Deferred, and tracked in
[TODO.md](../TODO.md) (Auth, TLS & security):

- Pushing a direct exec revocation to the worker at once. Revocation still
  lands on the next heartbeat (about 2 seconds).
- ~~Fleet certificates for the worker simulator are granted from the
  registering node's own `workerFleet` attribute.~~ Closed 2026-10-18: the
  join token's worker type must grant the fleet (calls 20).
- Git and OCI artifact fetches are not judged by the host guard (HTTP is, and
  an S3 `endpoint` is checked but not pinned).
- ~~A `templateKv` template edited in KV is re-rendered without re-checking
  the submitter's `kv:read` on any new key it names (calls 2).~~ Closed
  2026-10-18: a re-render may not read a key the job did not already name
  (calls 21).
- `exec` health checks run on the host, so they make a task host-level;
  running them inside the task's runtime would lift that (calls 4).
- A join secret can still claim the name of a `lost` node (calls 8).
- Every manager shares one raft transport key, so the transport cannot tell
  managers apart or revoke one (calls 1).
- An `exec` task without a `user` still runs as root on the host. That is the
  runtime's design, gated by the `host` grant, not a gap this plan closes.

## 1. Problem

The README listed three known security issues and `TODO.md` about ten more.
A review of the manager's authorization layer, the worker's file handling,
the network surfaces, and credential handling found more. In order of how
much a tenant with only `job:write` on their own job could do:

1. **Any job could read any KV secret.** `$path` references, `kvInjections`
   (env, files, the `{{ kv }}` lookups in their templates, `templateKv`), and
   artifact `auth.kv` were all resolved without checking the submitter's
   `kv:read`, and `GetArtifactCredential` served any path to any node.
2. **The root worker could be steered outside the allocation directory.** Task
   names were never validated (`allocs/<alloc>/<task>` with a `..` or absolute
   name), an `exec` health check on any task ran its command on the host as
   root, tar extraction used the crate's unchecked `unpack` and allowed hard
   links, `file://` artifacts copied any host file (the node's TLS key
   included), and the worker's rewrites of templates, identity tokens, and KV
   files followed symlinks the running task could plant in its own directory.
3. **Node credentials could leave the node.** A task-level
   `auth.nodeIdentity` sent the node's OIDC token to a source the job chose;
   HTTP artifact fetches followed redirects to the cloud metadata service and
   carried custom credential headers along.
4. **Identities could be forged.** A job's own `subTemplate` could render
   another workload's `sub`; node names that fold to the same DNS label
   (`NODE-1`, `node.1`, `node_1`) got certificates valid for each other; a
   client certificate from an operator's `clientCa` could claim to be a
   Legion node; a join secret could take over a live node's name; a boot
   ticket could be minted for another job's config, and `RatifyClaim` never
   checked that the claim was the one its ticket authorized.
5. **Manager-to-manager traffic was plaintext**, carrying the raft log, the
   cluster secret, and (through leader forwarding) callers' bearer tokens.
6. **Smaller gaps:** `exec` tasks inherited the worker's whole environment
   (`LEGION_TOKEN`, cloud credentials); idempotency keys replayed the first
   reply for a different request; live boot tickets were readable with
   `state:read`; JWT/OIDC methods skipped the audience check when
   `boundAudiences` was unset; delegated exec tickets could be future-dated past
   their key's ceiling; secret files were created at the umask and tightened
   afterwards; the `authz` verdict cache was shared across allocations;
   expired boot tickets were never pruned.

## 2. Prior art

- **Nomad** gates `raw_exec` behind a client option and filters the task
  environment with a denylist (`env.denylist`, which includes
  `VAULT_TOKEN` and the AWS keys). Legion already gates host-level tasks
  with the `host` grant; the environment uses an allowlist instead.
- **Kubernetes** rejects path-unsafe object names (DNS-1123) at the API, uses
  `secureJoin`/`openat2(RESOLVE_BENEATH)` for volume subpaths after
  CVE-2017-1002101, and requires `get secrets` RBAC for anything that mounts
  a secret by name. Legion now validates names at admission and requires
  `kv:read` for every KV key a spec names.
- **Consul and etcd** run peer traffic over TLS. Both expect an operator CA;
  Legion derives the peer identity from the cluster secret instead, because
  a joining manager holds that secret before any CA exists (calls 1).
- **Stripe's idempotency keys** refuse a reused key whose parameters differ,
  rather than replaying. Legion does the same.
- **Docker/Moby** and **containerd** archive extraction refuse escaping hard
  links and resolve every entry inside the destination; the tar crate's
  `unpack_in` does that check.

## 3. Jobs to be done

**Security / platform admin**
- When I give a team the right to deploy their own jobs, I want that to stop
  at their jobs, so a deploy right is not a way to read every secret, become
  root on every node, or borrow the nodes' cloud identity.
- When my AWS trust policy matches a subject, I want only that workload to
  be able to present it.

**Cluster operator**
- When I run three managers across a network I don't fully control, I want
  their traffic encrypted and authenticated, so the raft log and the tokens
  forwarded between them can't be read or altered.
- When a worker has a problem, I want it unable to impersonate a healthy
  peer, so one bad node doesn't become two.

**Job author**
- When my job is refused, I want the error to name the grant I'm missing, so
  I can ask for it instead of guessing.

**Autonomous agent**
- When I retry a request with an idempotency key, I want a reused key with a
  different request to fail, so I never believe a change happened when it
  didn't.

*Not in scope:* the on-call responder; nothing here changes how an incident
is worked.

## 4. Goals and non-goals

**Goals.** Close every finding above that a tenant without `host` can
exploit, keep each refusal specific (it names the missing grant or rule), and
change defaults where the safe one is right for a new cluster.

**Non-goals.** Namespaces (their own plan), a push channel for exec
revocation, judging git and OCI fetches, and making `exec` safe for untrusted
code. `exec` stays a host-level runtime.

## 5. Design (as shipped)

### 5.1 KV values a job consumes need `kv:read`

Admission (`RunJob`, `RunJobTemplate`, `RenderJobTemplate`, `PlanJob`)
enforces `kv:read` on every key the spec resolves, and on every artifact
`auth.kv` the spec itself names (before cluster `artifacts.auth` matchers
apply theirs):

```console
$ legion job run web.ncl
error: permission denied: ... (the jobspec reads kv secrets/db-password; the submitter needs kv read on it)
```

`GetArtifactCredential` serves a node only the paths named by an
allocation placed on it, by the task of a live boot ticket for its pool, or by
its pool's `contentStore.auth.kv`, and refuses anything else before reading.

### 5.2 The root worker stays inside the allocation

- Job, group, and task names: `[A-Za-z0-9._-]`, 1–128 characters, not
  starting with `.`. The worker re-checks before it creates a task directory.
- Tar: `unpack_in`, hard links refused.
- `exec` health checks, `file://` artifacts, `git::file://` repositories,
  and `file://` images are host-level (`host = true` on `job:write`); an
  `exec` check's command gets the allowlisted environment.
- Templates, identity tokens, and KV files below the task directory are
  written by `util::write_file_beneath`: every directory opened `O_NOFOLLOW`,
  the temp file `O_EXCL | O_NOFOLLOW`, the rename inside the opened
  directory. A relative `exec` file path is now below the task directory
  instead of the worker's working directory.
- A staged-artifact manifest's paths and a snapshot manifest's part names are
  checked before reuse.

### 5.3 Node credentials stay with the node

- A task-level `auth.nodeIdentity` is host-level. Cluster matchers keep
  working: they are the operator's choice of source.
- HTTP artifact fetches follow redirects themselves: each hop's addresses are
  judged by the worker's host guard and the connection pinned to them;
  credentials go to the source's own origin only; `https` never redirects to
  `http`. An S3 `endpoint` is judged too.

### 5.4 Identities

- `workloadIdentity.allowedSubTemplates` lists the subject shapes a job may
  name; anything else is refused at admission and at mint.
- Certificate DNS labels are injective: a plain label is itself, anything
  else is `head--<sha256 prefix>`.
- An ingress peer is a Legion node only on a chain verified against the
  cluster CA.
- A join token may not register a live node's name (a `lost` one is fine),
  unless the node's own token rides along.
- `IssueBootTicket` refuses a `job_config_id` of another job; `RatifyClaim`
  requires a ticket and that it names the claim's config, task, and a gate at
  least as high.
- JWT/OIDC methods need `boundAudiences` (an OIDC method's `oidcClientId`
  counts) and `https` URLs off loopback. A workload-identity login lasts no
  longer than the identity token it came from.

### 5.5 Raft transport TLS

Every manager derives an Ed25519 key from the cluster secret (HKDF-SHA256) and
a self-signed certificate for `raft.legion`. The API port presents it to a
client asking for that name (also when `server.tls` makes the port TLS-only);
peers dial TLS and accept only that public key. The raft log, snapshots,
joins, lane-write and leader forwarding, and member probes all ride it.

### 5.6 The rest

`exec` tasks and sessions get an allowlisted environment; idempotency keys
carry a request fingerprint and a reuse with another request fails;
`idempotency_record.response_message` reads as `NULL` through raw SQL;
delegated exec tickets may not be future-dated; secret files are `0600` from
creation; the `authz` cache key includes the allocation and webhook; expired
boot tickets are pruned, and the credential pruner runs with ACLs off too;
`RenderJobTemplate`/`RunJobTemplate` don't reveal whether a template the
caller can't read exists.

## 6. Proto, config, and persistence changes

- Manager config: `workloadIdentity.allowedSubTemplates` (new).
- No new RPCs, proto fields, job-config fields, or migrations. The
  idempotency fingerprint is part of the stored key.
- Wire: manager peers now speak TLS; a manager from before this change
  cannot talk to one after it. Upgrade all managers together.

## 7. Phases

Each section of §5 shipped on its own; they share a branch only because they
came out of one review.

## 8. Testing

Unit tests sit beside each change (names end in `_test`). The raft TLS
change is covered end to end by the raft integration suites, which now serve
the transport over TLS (`RaftTransportTls::incoming`), and by
`raft_peers_handshake_only_with_their_own_cluster_test`.

## 9. Rollout

Breaking, on purpose (AGENTS.md, "Upgrades and Breaking Changes"):

- Jobs that name KV keys or artifact credentials their submitter can't read,
  that use `file://` sources or a task-level `auth.nodeIdentity` without
  `host`, or whose names fall outside the charset, are refused at submit.
- A per-identity `subTemplate` needs an `allowedSubTemplates` entry.
- JWT/OIDC methods without an audience fail manager startup.
- Node certificate names change for names that are not plain labels.
- Managers must be upgraded together (raft TLS).
