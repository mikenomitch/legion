# Approvals and Rate Limits

Two guardrails for clusters that agents operate: **approval gates**, which
hold chosen writes until a person says yes, and **per-token rate limits**,
which stop a runaway loop before it saturates the API. Both run on every
manager's gRPC listener, after the ACL check allowed the write, and both
answer with typed errors an agent can act on. Every step lands in the
[audit journal](audit-journal.md).

Both need ACLs on: with `acl.enabled = false` every caller is the same
`acl-disabled` identity, so there is no one to rate-limit apart and no one
else to approve.

## Approval gates

### Configuring a gate

```nickel
governance = {
  approvals = {
    ttl = "1h",   # how long a request waits for a decision, and how long a granted approval stays usable
    rules = [
      # Destroying or stopping anything in prod.
      { name = "prod-destroys", operations = ["DestroyJob", "StopJob"], namespaces = ["prod"] },
      # Every ACL change.
      { name = "acl-writes", permissions = ["acl:write"] },
      # Draining nodes, but only for agents.
      { name = "agent-drains", operations = ["SetNodeAvailability"], principals = ["agent-*"] },
    ],
  },
}
```

A rule gates a write when every field it sets matches; fields left out
match anything:

| Field | Matches |
| --- | --- |
| `operations` | Globs over the Manager RPC name (`DestroyJob`, `*Kv`). |
| `permissions` | Globs over the write being checked, `resource:action` (`acl:write`, `node:*`). |
| `namespaces`, `names`, `pools`, `paths` | Globs over the target's namespace, name, node pool, or KV path. A target without that attribute does not match. |
| `principals` | Globs over the caller's principal (token name, `userpass:alice`). |
| `policies` | The caller holds at least one of these policies. |
| `includeManagement` | Whether management tokens are gated too. Default `true`. |

A rule must name `operations` or `permissions`; the manager refuses to
start with one that would gate every write. `DecideApproval` itself is
never gated.

### What the caller sees

```console
$ legion job destroy web -n prod
Error: approval required: DestroyJob on job:prod/web matches approval rule `prod-destroys` (request apr_7c1e…)
Hint: ask an approver to run `legion approval approve apr_7c1e…`, then retry the same command with --approval apr_7c1e…

$ legion job destroy web -n prod --json
{"ok":false,"error":{"code":"APPROVAL_REQUIRED","message":"…","retryable":false,
 "details":{"approvalId":"apr_7c1e…","requestDigest":"5b0f…","rule":"prod-destroys","expiresAt":"2026-11-08T11:02:11Z"}}}
```

Nothing was changed. Over gRPC the status is `FAILED_PRECONDITION` with
metadata `legion-error-code: APPROVAL_REQUIRED` and
`legion-error-details` carrying the JSON above. Repeating the call before a
decision returns the same request id rather than filing another.

### Deciding

An approver — any caller with `approval:write` who is **not** the requester
— lists and decides:

```bash
legion approval list --status pending
legion approval get apr_7c1e…
legion approval approve apr_7c1e… --reason "change ticket 42"
legion approval deny apr_7c1e… --reason "not during the freeze"
```

The approver may not be the requesting token, a token with the same
principal, or a token in the requester's lineage (its parent, its children,
or siblings under the same root): an agent cannot approve its own request
through a child token. `approval:read` lists everyone's requests; without
it, `legion approval list` and `get` show only the caller's own, which is
how an agent polls for the decision.

### Retrying

The requester retries the **identical** command with the approval:

```bash
legion job destroy web -n prod --approval apr_7c1e…     # or LEGION_APPROVAL=apr_7c1e…
```

Over gRPC the approval travels in the `legion-approval-id` request metadata.
MCP tools take an `approval_id` argument.

### What an approval binds

An approval is usable:

- **once** — the first retry that passes the gate consumes it (a replicated
  compare-and-set), whether or not the operation then succeeds;
- **by the requester only** — same principal and accessor;
- **for the same request** — same RPC and the same request digest, the
  SHA-256 of the canonical request message. A retry with different
  arguments (or a different idempotency key) is refused with
  `APPROVAL_INVALID` and "approval … was granted for a different request";
  file a new request instead. The digest sorts map entries and message
  fields, so the CLI producing a map in a different order does not matter;
  reordering a list field of the same request does not change the digest
  either;
- **until it expires** — a pending request lapses `ttl` after it was filed;
  an approved one is usable for `ttl` after the decision.

`APPROVAL_INVALID` (also `FAILED_PRECONDITION`) names why a presented
approval was refused: not approved yet, denied, expired, already used, a
different request, or another caller's.

### Agents and MCP elicitation

An MCP client that declares the `elicitation` capability (MCP 2025-06-18)
sees the gate as a prompt: the server shows the user the pending request
and the command an approver runs, and once they choose "Retry now" it
retries the call with the approval. The server never asks for a credential;
the approval is still an approver's own authorized action. Clients without
elicitation, and the HTTP transport, get the typed error and retry with
`approval_id` themselves.

## Rate limits

```nickel
governance = {
  rateLimit = {
    enabled = true,
    writesPerSecond = 20,     # sustained, per token
    burst = 100,              # bucket size
    exemptManagement = true,  # management tokens are break-glass; give agents client tokens
    overrides = [
      { policies = ["ci-agent"], writesPerSecond = 2, burst = 10 },
      { principals = ["batch-*"], writesPerSecond = 100, burst = 500 },
      { accessors = ["41d0a7e1…"], writesPerSecond = 1, burst = 5 },
    ],
  },
}
```

Each token (by accessor; JWT callers by principal) has a token bucket. Every
mutating call that passes its ACL check takes one token; reads are never
limited. The first matching override wins; a [child
token](auth-and-acls.md#child-tokens) minted with `--write-rate 5/s` (or
`5/s,burst=20`) carries its own limit, which wins over config and may not
exceed its parent's own limit.

```console
$ legion kv put team/a 1 --json
{"ok":false,"error":{"code":"RATE_LIMITED","message":"rate limited: ci-agent exceeded its write limit of 2/s (burst 10)…",
 "hint":"…retry after 480 ms…","retryable":true,"details":{"retryAfterMs":480,"limitPerSecond":2.0,"burst":10}}}
```

Over gRPC: `RESOURCE_EXHAUSTED`, `legion-error-code: RATE_LIMITED`, details
in `legion-error-details`. Nothing was changed, so retrying after
`retryAfterMs` is safe.

**Limits are per manager.** Buckets live in each manager's memory, so a
caller spreading writes across three managers gets up to three times the
rate, and a manager restart refills every bucket. That keeps the check off
the consensus path; cluster-wide budgets build on [usage
accounting](usage-accounting.md) instead.

## Metrics

`legion_governance_events_total{event}` counts `rate_limited`,
`approval_required`, `approval_invalid`, `approval_used`, and the journal's
`audit_written`, `audit_dropped` and `audit_unjournaled` (see
[metrics](metrics.md)).

## Limitations

- One approver per request; multi-party approval is not implemented.
- Gates and limits apply to calls through the gRPC listener (CLI, UI, MCP,
  SDKs). The manager's in-process reconcilers are not gated.
- A gate consumes its approval before the operation runs: if the operation
  then fails, file a new request.
