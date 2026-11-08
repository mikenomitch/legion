# Agent Governance: Judgement Calls

Plan: [`plans/agent-governance-plan.md`](../plans/agent-governance-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Journaling and gating hook the existing ACL check through a request layer, not each handler

**Call.** A tower layer (`src/manager/governance/layer.rs`), innermost in the
manager's gRPC stack and inside leader forwarding, installs a per-request
context in a tokio task-local. `acl::enforce` (which every handler already
calls), `authenticate_acl`, and `scoped_idempotency_key` report into it; the
layer journals the request from the reply's status. The rate limit and the
approval gate run inside `enforce` once a write is allowed, so they see the
real target scope (`job:prod/web`) the handler computed.

**Alternatives.** (a) Edit ~100 write handlers to call a journal helper —
the obvious way, and what K8s-style per-resource audit looks like.
(b) A tonic interceptor: sees requests, never outcomes. (c) A pure tower
layer with no handler hooks: knows the RPC name and the status, not the
caller's resolved identity or the target. (d) A generated wrapper around the
whole `Manager` trait in `build.rs`.

**Trade-off.** Calls that bypass the gRPC listener — in-process reconcilers,
tests calling the service directly — are neither journaled nor gated; a
handler that performed its write check on a spawned task would escape too.
The coverage test (`every_write_rpc_is_journaled` in
`tests/integration/acl_endpoints.rs`) exists to catch the latter, and it runs
handlers under the same context the layer installs (`layer::audited`).

**Why (simplicity, extensibility).** One hook in the one function every
endpoint must already call makes coverage structural: a new write RPC is
journaled, rate-limited and gateable the day it ships, with no line of
governance code. Gating at `enforce` instead of the layer is what lets a rule
say "destroy in namespace prod" without the layer parsing every request type.

## 2. Child tokens narrow by policy subset plus the root's deny rules, re-evaluated per request

**Call.** `CreateChildToken` mints a client token whose policies are a subset
of its parent's (a child of a management token names any existing policies).
At request time the child's rules are its own policies' rules **plus every
deny rule of its root ancestor's policies**, and every ancestor must still
exist and be unexpired. Lineage lives in `acl_token_meta`; deleting a token
deletes its descendants. Expiry ≤ parent and ≤ `acl.childTokenMaxTtl`.

```bash
legion acl token create --child --name sub-agent --policy kv-team --ttl 30m --write-rate 5/s
```

**Alternatives.** (a) Biscuit/macaroon caveats — true attenuation of any
shape, offline-verifiable. (b) Inline restriction rules on the child
evaluated as an intersection (`child_rules ∧ parent_rules`), which needs a
second rule set on `AclIdentity`. (c) Copying the parent's rules into the
child at mint time.

**Trade-off.** A child can only be narrowed along policy boundaries: an
agent holding one broad policy cannot hand out "only namespace X" unless a
policy for X exists. Deferred as an open TODO (inline restriction rules).
Every child authentication costs one extra point read per ancestor.

**Why (simplicity, security).** Subset-of-policies plus the root's denies is
provably never wider than the parent — every allow the child uses is one the
root holds, every deny the root is bound by binds the child — and it stays
true when someone later edits a policy, because both sides resolve the same
current rules. (b) would have added a field to `AclIdentity`, which 28
struct literals across the tree (and other branches) construct; (c) goes
stale on policy edits; (a) is a second credential format. The test proves
the post-mint policy edit case explicitly.

## 3. Tokens expire by default: 30 days (client and API-made management), 90-day client bound; the bootstrap token never expires

**Call.** `acl.tokenDefaultTtl = "720h"` applies to every `CreateAclToken`
token without `--ttl`; `acl.tokenMaxTtl` now defaults to `"2160h"` for client
tokens (`"none"` lifts it). Management tokens made through the API default
to 30 days but may ask for `--ttl never`. The bootstrap management token and
node tokens never expire.

```text
$ legion acl token create --name ci --policy ci --ttl 2400h
Error: requested ttl exceeds acl.tokenMaxTtl (90days)
```

**Alternatives.** (a) Expire the bootstrap token too (Nomad's critics ask
for exactly this). (b) Keep management tokens exempt from any default.
(c) A stricter client default (24h, like logins).

**Trade-off.** The break-glass token is a permanent credential; operators
must rotate it themselves (documented). A 30-day client default still means
a leaked token lives up to a month.

**Why (simplicity).** The manager re-mints the bootstrap token only when no
management token exists; an expired one still exists, so expiring it by
default would lock a forgotten cluster out with no recovery path short of
editing the store. 30/90 days matches common CI-credential rotation and
does not break every automation the day after setup; agents get short lives
through child tokens (1h default).

## 4. An approval binds the operation, a canonical digest of the request, and the requester, and is consumed at the gate

**Call.** The layer digests the request body (SHA-256 over a canonical
protobuf form: fields sorted recursively, so `map` order does not matter).
An approval is usable once, by the requester, for the same RPC and digest,
until `ttl` after the decision. The gate consumes it with a replicated
compare-and-set *before* the handler runs. The approver may not be the
requester, share its principal, or share its token lineage root.

**Alternatives.** (a) Digest the decoded message's JSON in each handler —
exact, but needs a `Serialize` bound on every ACL call site (streaming
requests cannot satisfy it). (b) Raw-bytes digest — prost encodes `HashMap`
fields in per-process random order, so a CLI retry would mismatch.
(c) Consume on success only, so a failed operation keeps its approval.

**Trade-off.** Being type-blind, the canonicalizer treats any
length-delimited value that parses as a message as one, so reordering a
repeated field (or permuting such a value's fields) yields the same digest:
the requester could reorder its own approved list arguments. An operation
that fails after the gate burns its approval; the requester files a new one.

**Why (security over convenience).** Consume-before-run makes double use
impossible under concurrent retries; consume-on-success would need a
reservation protocol. The digest's residual ambiguity is bounded to the
requester's own approved request, which the other two bindings already pin.

## 5. The journal is written asynchronously in batches to a commutative table

**Call.** The layer queues records; a writer flushes every 250 ms or 256
records as one replicated entry (`audit_event`, `Owner::Commutative`, random
128-bit ids, pruned locally per manager: 200,000 rows / 90 days). A follower
that leads no raft lane logs its own records (denials it answered itself) to
the `audit` tracing target and counts them as `audit_unjournaled`.

**Alternatives.** (a) Write synchronously inside each request's capture, so a
write and its record commit atomically. (b) A control-lane table written via
the leader. (c) Add a `ForwardableLaneWrite::AppendAudit` so followers
forward their batches.

**Trade-off.** A crash loses up to one flush interval of records; a full
queue drops records (counted). Follower-local denials are not in the
replicated journal.

**Why (performance).** (a) adds a raft round trip or capture work to every
write in the cluster for an observability feature. (c) needs a change in
`src/storage/raft/`, outside this change's remit; recorded as a TODO.
Writes themselves always end on a leader and are journaled there, so the gap
is denials on followers, which still reach a log.

## 6. New `audit` and `approval` ACL resources instead of reusing `operator` or `acl`

**Call.** `audit:read` reads the journal; `approval:read` lists everyone's
approval requests and `approval:write` decides them. A requester reads its
own requests without either.

**Alternatives.** `operator:read` for the journal and `acl:write` for
approvals (no new vocabulary).

**Trade-off.** Two more resources in the rule vocabulary; existing
`resources = ["*"]` rules reach them.

**Why (least privilege).** The journal shows every caller's activity; an
on-call approver should not need the right to rewrite policies, and a
policy author should not implicitly approve production destroys.

## 7. Rate limits are in-memory per manager, management tokens exempt by default, and per-token limits ride child tokens and config overrides

**Call.** A token bucket per accessor (principal for JWTs) on writes, 20/s
burst 100 by default, `exemptManagement = true`. Overrides match by policy,
principal, or accessor in `governance.rateLimit.overrides`; a child token can
carry `--write-rate`, capped by its parent's.

**Alternatives.** (a) Replicated buckets for a cluster-wide limit. (b) Limit
management tokens too. (c) A `write_rate` field on `CreateAclTokenRequest`.

**Trade-off.** A caller spreading load across N managers gets N× the rate;
a restart refills buckets. A management token in an agent's hands is not
throttled unless an override names it.

**Why (performance, compatibility).** The check sits on every write; a
mutex over a map is the only acceptable cost. Exempting management keeps
benchmarks and break-glass operations unthrottled; agents are meant to hold
client or child tokens. (c) was built and backed out: every
`CreateAclTokenRequest { .. }` literal in the integration suite (15 files,
other branches too) would have stopped compiling, and config overrides by
accessor cover the same need.

## 8. MCP 2025-06-18 with negotiation; elicitation asks the user to get approval, never for a credential; HTTP is JSON-only on loopback

**Call.** The server implements 2025-06-18 and serves 2025-03-26 and
2024-11-05 clients with only the features those define. On
`APPROVAL_REQUIRED`, a client with `elicitation` is asked (enum "Retry now" /
"Cancel") after being shown the `legion approval approve <id>` command; on
retry the server presents the approval. Streamable HTTP (`--http`) answers
JSON, opens no SSE stream, keeps no sessions, binds loopback only, and
checks `Origin`.

**Alternatives.** (a) 2025-11-25. (b) Let the elicitation approve directly by
asking for an approver's token. (c) Full Streamable HTTP with SSE, sessions
and MCP's OAuth authorization.

**Trade-off.** No elicitation over HTTP; HTTP clients cannot authenticate
separately — they all act as the server's token, hence loopback-only.

**Why (security, simplicity).** The spec forbids eliciting secrets, and an
approval must stay an approver's own authorized act; asking the human to
"get it approved, then retry" keeps that. 2025-06-18 is the revision whose
details we are sure of. JSON-only HTTP is spec-compliant and ~150 lines;
SSE plus OAuth is a project of its own (open TODO).

## 9. One-to-one MCP tools decode snake_case arguments straight into the proto request

**Call.** `src/cmd/mcp_tools.rs` adds 65 tools through one macro: each row
names the tool, RPC, request type, `mutates`, and a compact schema;
arguments are camel-cased and deserialized into the request message. A test
parses the proto and fails when a user-facing RPC has neither a tool (found
by scanning the MCP sources for client calls, or in the table) nor an
`EXCLUDED` entry with a reason; another checks `mutates` against the RPC's
access table.

**Alternatives.** Hand-written dispatch per tool, like the original 107.

**Trade-off.** Schemas are hand-listed and shallow; nested messages
(`AppViewContext`, scaling selectors) take the proto's camelCase JSON.

**Why (extensibility).** Coverage stays complete as RPCs are added, at one
line per tool; the hand-written style had left 65 RPCs behind.

## 10. Usage integrates reservations on the status-lane leader, attributed to the submitter from the journal

**Call.** Every 60 s the status-lane leader adds each capacity-holding
allocation's reservation × elapsed seconds into hourly `usage_rollup` rows
per (namespace, job, submitter, dimension). Unknown numeric resource keys
become `resource:<name>`. The submitter is the accessor of the job's last
successful `RunJob` in the audit journal.

**Alternatives.** (a) Measured usage from heartbeats. (b) Stamp the
submitter on `job_config` at submit.

**Trade-off.** Idle reservations are charged; one interval per failover is
lost; submitters vanish when the journal prunes their submit.

**Why (simplicity).** Reservations are what quotas charge and what capacity
costs, and they are already in the store; measured usage is lossy and
node-local. (b) touches the submit path and the job schema for an
attribution the journal already holds.

## 11. `CreateChildToken` is not idempotent

**Call.** No `idempotency_key`: a retry mints a second child.

**Alternatives.** Store the secret encrypted for replay.

**Trade-off.** A retried call leaves an unused child, which expires on its
own (1h default).

**Why (security).** Replaying a mint means storing a usable secret; the
hash-only invariant for tokens is worth more than retry-safety here.

## 12. The caller-aware catalog is a server RPC over a source-side access table

**Call.** `GetCallerCapabilities` evaluates the caller against
`src/manager/rpc_access.rs` (a restatement of the ACL endpoint table, held
equal by `rpc_access_table_matches_the_endpoint_table`) and the CLI/MCP merge
it into the catalog by `grpc` method.

**Alternatives.** Annotate each catalog entry with its permission in the
CLI.

**Trade-off.** A second table to keep in step with the endpoint table (the
test enforces it).

**Why.** The server owns the caller's rules (including child-token denies);
the CLI cannot evaluate them, and the same table drives the MCP coverage
test.

## 13. Gates skip ACL-disabled clusters; approval rules must name operations or permissions

**Call.** With ACLs off there is one identity, so gates and limits do
nothing; the manager refuses a rule with neither `operations` nor
`permissions`. `DecideApproval` is never gated.

**Why (simplicity).** Self-approval is forbidden, so a gate on an
ACL-disabled cluster could never be satisfied; a rule matching every write
would gate the approval RPC's own prerequisites and lock the cluster.
