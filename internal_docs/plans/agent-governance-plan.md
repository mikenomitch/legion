# Agent Governance Plan: audit journal, scoped credentials, approvals, rate limits, usage, and MCP

> **Judgement calls:** [`calls/agent-governance-plan_calls.md`](../calls/agent-governance-plan_calls.md)

Status: **IN EXECUTION** — written 2026-11-08 to close findings AI2, AI3, AI4
and AI9 of [`architecture-review-2026-11-07.md`](../architecture-review-2026-11-07.md)
("AI readiness"). Phases 1–8 shipped together. Deliberately deferred (open in
`internal_docs/TODO.md`, "Agent surfaces" and "Observability"): journaling
on a follower that leads no raft lane (needs a raft forward op), an external
audit sink, inline narrowing for child tokens, multi-party approvals,
cluster-wide limits and enforcing budgets, MCP HTTP SSE/sessions/auth, and
the `legion_job_run` wait (owned by the AI5 work).

Public docs: [`docs/audit-journal.md`](../../docs/audit-journal.md),
[`docs/approvals-and-rate-limits.md`](../../docs/approvals-and-rate-limits.md),
[`docs/usage-accounting.md`](../../docs/usage-accounting.md),
[`docs/auth-and-acls.md`](../../docs/auth-and-acls.md) (token lifetimes, child
tokens), [`docs/agent-readiness.md`](../../docs/agent-readiness.md) (MCP).

---

## 1. Problem

Legion is driven by people and, increasingly, by autonomous agents holding a
token. The review found that the cluster cannot answer the questions an
operator asks the morning after an agent ran:

- **Who did what?** Only ACL *denials* are logged, to the `acl_audit` tracing
  target; nothing durable records a successful `job destroy`, which token sent
  it, or whom that token acted for (AI2).
- **Can I hand an agent a safe credential?** Tokens never expire unless the
  operator sets `acl.tokenMaxTtl`; minting any token needs `acl:write`, so an
  agent cannot give a sub-agent a narrower token; there is no human-approval
  gate for destructive operations and no rate limit, so a runaway loop is
  stopped only by revoking its token (AI3).
- **Can the agent see what it may do?** `legion capabilities` lists every
  operation whatever the caller holds (AI3).
- **Is the MCP server current?** It speaks MCP `2024-11-05` over stdio, has no
  tool annotations, structured results or elicitation, and about 60
  user-facing RPCs have no tool (AI4).
- **What did a tenant (or an agent) use this week?** Only current charged
  usage exists; there is no time-integrated accounting, so there is no basis
  for budgets (AI9).

## 2. Prior art

- **Kubernetes audit** — a policy-driven event stream of every API request
  (stage, user, impersonated user, verb, object ref, response code) to a log
  or webhook backend. Take: one record per request decided centrally in the
  API server, the impersonation chain, outcome codes. Reject: request/response
  bodies in the record (secrets leak into audit backends regularly).
- **Nomad Enterprise audit logging** — HTTP-layer audit with filters; stage
  `OperationReceived`/`OperationComplete`. Take: central layer, not per
  handler. **CloudTrail** — queryable history with a lookup API by
  principal/event name/resource and time range. Take: the query shape.
- **Vault tokens** — child tokens, TTL and max TTL, revoking a parent revokes
  its children, `-orphan` to escape. Take: parent/child lineage, cascade
  revocation, child expiry bounded by the parent. **Macaroons / Biscuit** —
  attenuation by caveats. Reject for now: a second credential format and a
  caveat language; Legion's policies already are the vocabulary.
- **Kubernetes/OPA approval tooling, Teleport access requests, GitHub
  environment protection rules** — a gated action creates a request a
  different human approves; the approval is single-use and bound to what was
  requested. Take all three properties.
- **API gateways (Envoy local rate limit, GitHub API)** — token buckets per
  credential, `429`/`retry-after`. Take: token bucket per accessor, typed
  retryable error with a retry-after hint.
- **Kubernetes `kubectl auth can-i --list`** — the caller-aware view of what
  it may do. Take: a server RPC that evaluates the caller's rules against the
  RPC catalog.
- **Cloud billing / Kubecost / Nomad Enterprise quotas** — usage accumulated
  into hourly buckets by namespace and workload; budgets on top. Take: the
  hourly bucket, reservation-based core-seconds and GiB-hours.
- **MCP 2025-06-18** — version negotiation, tool `annotations`
  (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`),
  `outputSchema` + `structuredContent`, server-initiated `elicitation/create`,
  and the Streamable HTTP transport.

## 3. Jobs to be done

**Security / platform admin**
- When something changed in production overnight, I want to look up who did
  it, with which credential, acting for whom, and whether it worked, so I can
  answer for it without grepping manager logs.
- When I hand an agent a credential, I want it to expire on its own and to be
  unable to do more than I meant, so a leaked or forgotten token is not a
  permanent liability.
- When an agent or a teammate is about to destroy something in production, I
  want a person to say yes first, so one bad prompt cannot take the service
  down.

**Autonomous agent**
- When I delegate part of my task to a helper agent, I want to give it a
  token that can do only part of what I can, for less time, so its mistakes
  stay small.
- When I plan my next step, I want to know which operations my token allows,
  so I do not learn it by failing.
- When an operation needs a human's approval, I want a typed answer that says
  which approval to wait for, so I can pause and retry instead of guessing.
- When I am told to slow down, I want to know for how long.

**Cluster operator**
- When a runaway loop hammers the API, I want it throttled per token before
  it saturates the cluster, without hurting other callers.
- When a tenant asks why the bill went up, I want CPU and memory used over
  time per namespace and job.

**MCP client user (a human using an agent IDE)**
- When the agent wants to do something that needs approval, I want my client
  to show me, so I can get it approved and let the agent continue.

*Not in scope:* the job author — nothing in a jobspec changes.

**Scenarios.** (1) An agent with a 30-day client token mints a 1-hour child
token holding only `ci-deployer` for a sub-agent; the sub-agent's attempt to
`job destroy` in `prod` returns `APPROVAL_REQUIRED` with an approval id; the
on-call approves with `legion approval approve <id>`; the sub-agent retries
with `--approval <id>` and the destroy succeeds once; `legion audit list
--accessor <child>` shows all three steps. (2) A loop retries `job run` 500
times a second; after the burst it gets `RATE_LIMITED` with `retryAfterMs`.
(3) `legion usage --since 7d -n team-a --by job` answers "how much".

## 4. Goals and non-goals

Goals: a durable, replicated, queryable write journal covering every mutating
RPC centrally; token expiry by default; child tokens that are provably never
wider than their parent; configurable approval gates with single-use,
digest-bound approvals; per-token write rate limits; a caller-aware catalog;
MCP 2025-06-18 with annotations, structured results, elicitation and full
RPC coverage; hourly usage rollups per namespace and job.

Non-goals: request/response bodies in the audit record; a webhook/syslog
audit backend (the journal plus `--follow` is the seam); biscuit/macaroon
credentials; budgets that *enforce* (usage is the basis, enforcement is a
follow-up); cluster-wide (cross-manager) rate limits; metered (measured)
usage — usage integrates reservations.

## 5. What exists today

- ACL checks: `ManagerDaemonService::check_acl`/`authenticate_acl`
  (`src/manager/server.rs:2364`) and `acl::enforce` (`src/manager/acl.rs:857`),
  which logs denials to `acl_audit` and nothing else.
- Delegation: `with_delegation` resolves `legion-on-behalf-of` /
  `legion-for-object` into `AclSource::Delegated` / `ForObject` identities
  (`src/manager/server/delegation.rs`).
- Idempotency: `scoped_idempotency_key` (`server.rs:13207`) sees every
  idempotency key a handler honours.
- The gRPC stack in `src/manager/daemon.rs:760`: OIDC, origin guard, CORS,
  gRPC-Web, metrics, app-page guard, leader forwarding (innermost).
- Commutative raft tables (`scheduler_event`, `idempotency_record`): insert
  from any lane, pruned locally (`src/storage/raft/ownership.rs`).
- Tokens: `acl_token` rows, `CreateAclToken` needs `acl:write`
  (`server.rs:12128`), `acl.tokenMaxTtl` unbounded by default
  (`src/structs/config.rs:1602`).
- MCP: `src/cmd/mcp.rs`, stdio, `2024-11-05`, 107 tools.
- Namespace charged usage: `namespace_charges` (current only).

## 6. Design

### 6.1 The governance layer (AI2, AI3 gates)

A tower layer, `GovernanceLayer` (`src/manager/governance/`), sits innermost
in the manager's gRPC stack, inside leader forwarding, so only the manager
that actually executes a request journals it. For each `/legion.Manager/*`
request it:

1. buffers a unary request body and computes the **request digest**: SHA-256
   over a canonical form of the protobuf message (fields sorted, recursively,
   so map order does not matter);
2. installs a per-request context in a tokio task-local and runs the handler;
3. the handler's existing ACL calls feed the context: `authenticate_acl`
   (identity, on-behalf-of chain), `acl::enforce` (resource, action, scope,
   decision), `scoped_idempotency_key` (the key);
4. on the response, journals the request when a `write` check ran, when the
   outcome is an auth failure, or when the RPC is always audited (`Login`,
   token minting, approval decisions).

Because the hooks are in `enforce`, coverage is structural: any handler that
checks a `write` permission is journaled. A test walks the ACL endpoint table
and fails if a `:write` endpoint produces no journal record.

The same context carries the **gates** that run inside `enforce` once a write
is allowed: the rate limiter (§6.4) and the approval gate (§6.3). In-process
callers that bypass gRPC (the reconcilers inside the manager) are not gated.

### 6.2 The audit journal

```
$ legion audit list --since 1h --operation DestroyJob
TIME                  OPERATION   PRINCIPAL               ACCESSOR   TARGET              OUTCOME
2026-11-08T10:02:11Z  DestroyJob  ci-agent (child of ops)  9f2c…     job:prod/web        ok  approval=apr_7c1…
2026-11-08T10:01:40Z  DestroyJob  ci-agent (child of ops)  9f2c…     job:prod/web        APPROVAL_REQUIRED
```

Record: id, time, manager, operation, kind (`write`/`denied`/`audited`),
accessor, principal, source, on-behalf-of chain (`alice via app autoscaler`,
`for gitops/Repo team/web`), parent accessor for child tokens, resource,
action, target (`job:prod/web`, `kv:team/db`, `pool:gpu`), namespace, request
digest, idempotency key, approval id, outcome (`ok` or the typed/gRPC code),
a truncated error message, and duration. Never the request body or a secret.

Storage: table `audit_event`, `Owner::Commutative` (random 128-bit ids, any
lane inserts), replicated so every manager serves it. Writes are **batched
and asynchronous**: the layer hands records to a bounded queue; a writer task
flushes every 250 ms (or 256 records) as one replicated entry. Retention:
`governance.audit.retainEvents` (default 200,000) and `retainAge` (default
90 days), pruned locally like `scheduler_event`.

Surface: `ListAuditEvents` (filters: accessor, principal, operation, target
prefix, namespace, outcome, kind, since/until; cursor pagination, newest
first), `legion audit list [--follow]`, MCP `legion_audit_list`, and `state
query` over `audit_event`. ACL: a new `audit` resource (`audit:read`) — the
journal shows every caller's activity, which neither `operator:read` nor
`acl:read` holders should get implicitly.

### 6.3 Approval gates

```nickel
governance.approvals = {
  ttl = "1h",
  rules = [
    { name = "prod-destroys", operations = ["DestroyJob", "StopJob"], namespaces = ["prod"] },
    { name = "acl-writes", permissions = ["acl:write"] },
    { name = "drains", operations = ["SetNodeAvailability"], principals = ["agent-*"] },
  ],
}
```

A rule matches on RPC name globs, `resource:action` globs, scope selectors
(namespaces, names, pools, paths), and the caller (principal globs, policies
held). A matching write without an approval fails:

```
$ legion job destroy web -n prod
Error: approval required: DestroyJob on job:prod/web matches approval rule `prod-destroys`
Hint: ask an approver to run `legion approval approve apr_7c1…`, then retry with --approval apr_7c1…
{"ok":false,"error":{"code":"APPROVAL_REQUIRED","retryable":false,
 "details":{"approvalId":"apr_7c1…","requestDigest":"…","rule":"prod-destroys"}}}
```

An approver (`approval:write`, never the requester or a token in the
requester's lineage) runs `legion approval approve|deny <id>`; the caller
retries with `--approval <id>` (header `legion-approval-id`). The approval is
bound to the operation, the request digest and the requester, expires after
`ttl`, and is consumed by the first retry that passes the gate (a replicated
compare-and-set). Every step is journaled. MCP clients that advertise
`elicitation` are shown the pending approval and asked whether to retry.

### 6.4 Rate limits

```nickel
governance.rateLimit = {
  writesPerSecond = 20, burst = 100, exemptManagement = true,
  overrides = [ { policies = ["ci-agent"], writesPerSecond = 2, burst = 10 } ],
}
```

A token bucket per accessor (or principal for JWT callers) on writes,
evaluated in `enforce`. Exceeding it returns `RESOURCE_EXHAUSTED` with code
`RATE_LIMITED`, `retryable: true`, and `details.retryAfterMs`. A token can
carry its own limit (`legion acl token create --child --write-rate 5/s`);
other tokens are named in `overrides` by accessor. Limits are per manager
(in memory); documented.

### 6.5 Token lifetimes and child tokens

- `acl.tokenDefaultTtl` (default `720h`) applies to every token created
  without `--ttl`; `acl.tokenMaxTtl` now defaults to `2160h` for client
  tokens (`"none"` disables the bound). Management tokens created through the
  API default to the same TTL and may ask for `--ttl never`. The bootstrap
  management token never expires: it is the break-glass credential, and
  rotating it is documented.
- `CreateChildToken` / `legion acl token create --child`: any caller holding
  a Legion token mints a child whose policies are a subset of its own, whose
  expiry is at most the parent's and at most `acl.childTokenMaxTtl` (default
  `24h`, default TTL `1h`), and that records its parent. Revoking a parent
  revokes its descendants. At request time a child is evaluated as its own
  policies **plus every deny rule of its root's policies**, and its whole
  lineage must still exist and be unexpired — so it is never wider than its
  parent, even after policies are edited.

### 6.6 Caller-aware catalog

`GetCallerCapabilities` evaluates the caller against every Manager RPC's
required permission (a table in `src/manager/rpc_access.rs`, kept equal to
the ACL endpoint table by a test) and answers `allowed` / `partial` (some
namespaces) / `denied` / `open`. `legion capabilities --mine` and the MCP
`legion_capabilities` tool merge it into the catalog.

### 6.7 MCP

- Protocol `2025-06-18`, negotiated: a client asking for `2025-03-26` or
  `2024-11-05` gets that version and only the features it defines.
- Annotations derived from each tool's metadata (`mutates`, destructive
  verbs, idempotency key support).
- `outputSchema` (Legion's envelope) and `structuredContent` on every result,
  errors included (`{ok:false,error:{code,…}}`).
- Elicitation for `APPROVAL_REQUIRED` when the client supports it.
- Streamable HTTP: `legion mcp serve --http 127.0.0.1:PORT` (JSON responses,
  no SSE), loopback by default, Origin-checked.
- Coverage: a tool for every user-facing Manager RPC or an explicit exclusion
  with a reason; a unit test parses the proto and fails otherwise.

### 6.8 Usage accounting (AI9)

The manager leading the status lane integrates reservations every
`governance.usage.interval` (60 s): for each allocation holding capacity, it
adds `cpu MHz × seconds`, `memory MiB × seconds`, `cores × seconds`,
`allocation × seconds`, and any other numeric resource dimension
(`resource:<name>`, where devices land) into the hourly bucket row for
(namespace, job, submitter). Submitter is the accessor of the job's last
successful `RunJob` in the audit journal, when known. Retention
`governance.usage.retention` (default 90 days).

```
$ legion usage --since 7d --by namespace
NAMESPACE  CPU CORE-HOURS  MEMORY GIB-HOURS  CORE-HOURS  ALLOC-HOURS
team-a     412.3           1,630.2           0.0         168.0
```

`GetUsage` (filters: namespaces, job, submitter, since/until; group by
namespace, job, submitter, or bucket), `legion usage`, `legion namespace
usage <ns>`, MCP `legion_usage`. Budgets are the follow-up: a namespace
policy `budget` checked against these rollups.

## 7. Proto, config and persistence

- Proto: `ListAuditEvents`, `ListApprovals`, `GetApproval`, `DecideApproval`,
  `CreateChildToken`, `GetCallerCapabilities`, `GetUsage` in one block.
  `AclTokenInfo` gains `parent_accessor_id`. (`CreateAclTokenRequest` was
  to gain `write_rate`; dropped, see calls #7.)
- Config: `acl.tokenDefaultTtl`, `acl.childTokenDefaultTtl`,
  `acl.childTokenMaxTtl`; a new `governance` block (`audit`, `approvals`,
  `rateLimit`, `usage`).
- Migrations (DDL only): `audit_event` (commutative), `approval_request`
  (control lane), `acl_token_meta` (control lane: lineage and per-token
  limits), `usage_rollup` (status lane).

## 8. Phases

1. **Audit journal** — layer, context, journal table, writer, RPC, CLI,
   MCP tool, coverage test.
2. **Token lifetimes** — default TTL and bounded max; `--ttl` and expiry
   shown everywhere.
3. **Child tokens** — RPC, lineage, cascade, intersection at request time.
4. **Approvals** — rules, gate, RPCs, CLI, journal entries.
5. **Rate limits** — bucket, config, per-token override, metric.
6. **Caller-aware catalog**.
7. **MCP** — protocol, annotations, structured results, elicitation,
   coverage, HTTP transport.
8. **Usage rollups**.

Each phase ships alone: the journal is useful without gates, gates without
MCP, usage without budgets.

## 9. Testing

Unit: digest canonicalization, approval rule matching, token bucket,
child-token rule composition, MCP negotiation/annotations/structured
content, the MCP coverage test, usage integration arithmetic.
Integration (`tests/integration/agent_governance.rs`): journal coverage over
the endpoint table, content (accessor, on-behalf-of, outcome), retention;
default TTL and expiry; child token bounds, expiry and revocation; approval
flow end to end (gated, approve, single use, deny, expiry, digest mismatch)
through a real gRPC server with the layer; rate limit trips and recovers;
caller-aware catalog; usage accumulation over simulated time. Existing
suites: `acl_endpoints::`, `acl_default_on::`, `grants::`,
`app_action_delegation::`, `namespace_restrictions::`.

## 10. Rollout and breaking changes

Legion has no installed base; defaults are chosen for a new cluster.
Breaking: client tokens created without `--ttl` now expire after 30 days (and
cannot exceed 90 days); management tokens created through the API expire
after 30 days unless `--ttl never`; client write RPCs are rate limited
(20/s, burst 100) by default; the MCP server answers `2025-06-18` to clients
that ask for it; `policies = ["*"]` style rules now also reach the new
`audit` and `approval` resources.

## 11. Open questions

- Should approvals be able to require N approvers? Lean: later, as a rule
  field `approvers = 2`.
- Should the journal stream to an external sink? Lean: a webhook app
  consuming `--follow`.

## 12. Acceptance

Per phase, the integration tests in §9 pass; `legion audit list`, `legion
approval …`, `legion usage`, `legion acl token create --child` work as shown;
the MCP coverage test passes with no unexplained RPC.
