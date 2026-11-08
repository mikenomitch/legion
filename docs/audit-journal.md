# Audit Journal

Every manager keeps a durable, replicated journal of **who changed what**:
one record for every mutating API call, for every request refused by
authentication or authorization, and for the calls that mint credentials or
decide approvals. It answers the question an operator asks the morning after
an agent ran: *which credential, acting for whom, did this, and did it
work?*

```console
$ legion audit list --since 1h --operation DestroyJob
TIME                  OPERATION   PRINCIPAL                  ACCESSOR   TARGET         OUTCOME
2026-11-08T10:02:11Z  DestroyJob  sub-agent (child of 9f2c…)  41d0a7e1…  job:prod/web   ok approval=apr_7c1…
2026-11-08T10:01:40Z  DestroyJob  sub-agent (child of 9f2c…)  41d0a7e1…  job:prod/web   APPROVAL_REQUIRED
2026-11-08T09:58:02Z  DestroyJob  anonymous                            job:prod/web   Unauthenticated
```

## What a record holds

| Field | Meaning |
| --- | --- |
| `time`, `tsMs` | When the call completed on the manager that handled it. |
| `manager` | That manager's raft node id (empty outside raft mode). |
| `operation` | The Manager RPC (`DestroyJob`, `PutKv`, `CreateChildToken`, …). |
| `kind` | `write` (a write permission was checked), `denied` (authentication or authorization failed), or `audited` (an always-audited call that checks no write, such as `Login`). |
| `accessorId` | The presented token's accessor ID. Never the secret. Empty for JWT-bearer, anonymous and ACL-disabled callers. |
| `principal`, `source` | Who the caller is (`ci-deployer`, `userpass:alice`, `anonymous`) and how it authenticated (`token`, `jwt`, `anonymous`, `disabled`, `delegated`, `for-object`). |
| `onBehalfOf` | The on-behalf-of chain: `alice via app autoscaler` when an app acts for a viewer ([delegated identities](auth-and-acls.md#delegated-identities)), `gitops for Repo team/web` when an app acts for an object. |
| `parentAccessorId` | For a [child token](auth-and-acls.md#child-tokens), the token it was minted from. |
| `resource`, `action`, `target`, `namespace` | The permission that decided the call and what it was scoped to: `job:prod/web`, `kv:team/db`, `node:pool/gpu`, `acl`. |
| `requestDigest` | SHA-256 over the canonical request message: the same arguments give the same digest. The body itself is never stored. |
| `idempotencyKey` | The request's idempotency key, when it carried one. |
| `approvalId` | The [approval](approvals-and-rate-limits.md) the call consumed, when a gate required one. |
| `outcome`, `message` | `ok`, a typed code (`APPROVAL_REQUIRED`, `RATE_LIMITED`, `WORKER_UNREACHABLE`, …) or the gRPC code name (`PermissionDenied`, `NotFound`, …), and the error message truncated to 512 bytes. |
| `detailJson` | Operation-specific JSON with no secrets, for example the minted token's accessor on `CreateAclToken`/`CreateChildToken`. |
| `durationMs` | Handler time. |

Reads that pass their checks are not journaled; the journal is a write
journal. A refused read is journaled as a denial.

## Querying it

Reading the journal needs `audit:read`, a cluster-scoped resource of its
own: the journal shows every caller's activity, so neither `operator:read`
nor `acl:read` implies it.

```nickel
{ resources = ["audit"], actions = ["read"] }
```

```bash
legion audit list                                   # last 24h, newest first
legion audit list --accessor 41d0a7e1… --since 7d   # one token's week
legion audit list --target job:prod/ --kind write   # writes under prod
legion audit list --outcome PermissionDenied        # who is probing
legion audit list --follow --json                   # NDJSON tail, one {"ok":true,"data":<event>} per line
```

Filters: `--accessor`, `--principal`, `--operation`, `--target` (a prefix),
`--in-namespace`, `--outcome`, `--kind`, `--since`/`--until` (RFC3339 or a
duration back from now). Over gRPC it is `ListAuditEvents`, paginated
newest-first with `page_token`/`next_page_token`, or followed oldest-first
with `after_cursor`/`last_cursor`. Agents use the MCP tool
`legion_audit_list`. The table is `audit_event`, so `legion state query`
reaches it too:

```bash
legion state query "SELECT operation, principal, target, outcome FROM audit_event
                    WHERE ts_ms > strftime('%s','now','-1 hour')*1000 ORDER BY ts_ms DESC"
```

## How coverage works

The journal is not written by each handler. A layer in front of every
Manager RPC installs a per-request context; the ACL check every handler
already makes (`acl::enforce`) reports the permission, scope and decision
into it, and the layer writes the record when the reply leaves. Any handler
that checks a write permission is therefore journaled without code of its
own, and a test walks the ACL endpoint table to prove every write RPC leaves
a record (`tests/integration/acl_endpoints.rs`,
`every_write_rpc_is_journaled`).

Calls that never reach the gRPC listener are not journaled: the manager's
own reconcilers (app reconcilers, gitops, the autoscaler's in-process
writes) act as themselves inside the manager, and their effects appear in
the [event stream](event-stream.md) instead.

## Durability

Records are written **asynchronously and in batches**: the layer queues the
record and replies; a writer task flushes the queue every 250 ms
(`governance.audit.flushInterval`) or every 256 records as **one**
replicated entry. Journaling therefore adds no consensus round trip to the
call it records. The trade-offs:

- A manager that crashes loses the records it had queued but not flushed —
  at most one flush interval's worth.
- A full queue (16,384 records) drops records rather than slow the API;
  drops are counted on `legion_governance_events_total{event="audit_dropped"}`.
- In raft mode the journal is a commutative table: whichever lane the
  handling manager leads carries the batch, and every manager serves the
  journal. A follower that leads **no** lane (every follower in the default
  `single` topology) cannot write it itself. Writes never end on such a
  follower (they are forwarded to the leader, which journals them), but the
  requests it refuses itself — denials, validation errors — do. The
  follower forwards those batches to the status-lane leader, which
  journals them like its own, so `legion audit list` shows a denial
  whichever manager answered it. Forwarded batches are counted on
  `legion_leader_forwards_total{op="append_audit_events"}`.
- When that forward cannot complete — no lane leader is known (an
  election, a lost quorum) or it cannot be reached — the batch is not
  retried: its records go to the `audit` tracing target on that manager
  instead, counted as `audit_unjournaled`. Ship that target with your logs
  if a quorum loss must not leave gaps.

## Retention

Each manager prunes its own copy (like the scheduler event journal):
records older than `retainAge` (default 90 days), then everything beyond the
newest `retainEvents` (default 200,000). Pruning runs every five minutes.

```nickel
governance = {
  audit = {
    enabled = true,           # false stops journaling (denials still reach the acl_audit log target)
    retainEvents = 200000,
    retainAge = "90d",
    flushInterval = "250ms",
  },
}
```

## Limitations

- No external sink (syslog, webhook, S3). `legion audit list --follow
  --json` is the integration point.
- The record names the request by digest, not by content; correlate with
  the [event stream](event-stream.md) and job history for what changed.
- Sessions' in-guest activity is not journaled ([session mode](session-mode.md));
  creating, forking and stopping sessions are API writes and are.
