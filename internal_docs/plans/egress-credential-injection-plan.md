# Egress Credential Injection: Secrets the Workload Never Sees

> **Judgement calls:** [egress-credential-injection-plan_calls.md](../calls/egress-credential-injection-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 (KV-backed header credentials on external
`https://` upstreams) and Phase 2 (the workload-identity provider) are
shipped. Phases 3 and 4 (a webhook provider with non-header placements,
per-request audit events) are planned. Gated sandboxes reach their
upstreams (and so injection) over vsock since the gated plan's upstream
relays (calls #11). Deliberately deferred and tracked in
[TODO.md](../TODO.md): upstreams, and so credentials, from TAP-wired and
loopback-only sandboxes.

Public reference: [docs/network-handlers.md, "Credential injection"](../../docs/network-handlers.md#credential-injection).

## 1. Problem

An agent workload that calls a model API needs the API key. Today the only
ways to give it one put the key inside the workload: an env var, a KV
`files` entry, a template. Once it is there:

- Anything running in the task can read it, including code the agent wrote
  or downloaded a minute ago. A prompt-injected agent can print it, send it
  anywhere its egress allows, or write it into a file a user later reads.
- A `sandbox` with memory checkpoints captures it. Every suspend checkpoint,
  manual snapshot, fork, and published part holds the key in guest memory
  (docs/suspend-and-resume.md, "Security"). Forking an agent ten times
  copies the key ten times. Rotating it means every checkpoint taken before
  the rotation is a valid credential until the provider revokes it.
- Rotation means re-rendering and restarting (or live-updating) every task.

Other substrates solve this by never letting the secret in. agent-substrate's
gateway terminates the sandbox's HTTPS with its own CA and swaps a
placeholder header for a real secret from a pluggable provider. Legion has
publicly committed never to intercept TLS (docs/network-handlers.md,
"Current scope and limitations"), so that design is not available to us.

It turns out we do not need it. Legion already has **external upstreams**:
`connect.upstreams = [{ name = "openai", to = "https://api.openai.com" }]`
gives the task a loopback port where it speaks plaintext HTTP, and the node's
forwarder opens TLS to the destination. The node already reads every request
head on that path. Injecting a credential is setting one more header on the
request the node builds for the origin.

## 2. Prior art

- **agent-substrate gateway.** Terminates sandbox HTTPS with a private CA
  trusted inside the sandbox, matches the destination, swaps a placeholder
  (`Authorization: Bearer PLACEHOLDER`) for the provider's value, and
  re-originates TLS. Providers are pluggable (static, vault-ish, token
  exchange). *Take:* the outcome (the secret never enters the sandbox or its
  snapshots), the provider seam, header-level injection. *Reject:* TLS
  interception and the in-sandbox CA. Our client points at a plaintext
  loopback upstream instead, which needs no trust-store changes and cannot
  be used to read arbitrary HTTPS.
- **Fly.io `tokenizer`.** An HTTP proxy that receives a *sealed* secret
  (encrypted to the proxy's key) in a request header plus an injection
  instruction, unseals it, and injects it on the way to the origin. The
  application only ever holds ciphertext. *Take:* "the client holds nothing
  usable", injection as a header processor with a prefix (`Bearer `).
  *Reject:* sealed secrets travelling with the request; in Legion the
  declaration lives in the jobspec and the value in KV, so the task holds
  nothing at all, not even ciphertext.
- **Cloudflare AI Gateway / Workers secret bindings.** The gateway holds
  provider keys ("BYOK stored keys") and adds them to requests from a Worker
  that only names the gateway. *Take:* the client sets a base URL and
  nothing else; the key is an operator-managed object. *Reject:* a
  provider-specific product surface; ours is a plain header on any HTTPS
  origin.
- **Envoy `credential_injector` HTTP filter.** An upstream filter that adds
  a credential (generic secret via SDS, or OAuth2 client-credentials) to a
  header, with `overwrite` controlling whether an existing header is
  replaced. *Take:* per-upstream declaration, the `overwrite` question (we
  always overwrite, see calls #4), provider types as the extension seam
  (`generic`, `oauth2` map to our KV and later identity providers).
- **Vault Agent / Nomad `template`.** Render secrets into files or env for
  the workload. This is what Legion already does with KV files. *Reject* for
  this problem: the secret ends up in the workload, which is exactly what we
  are removing.
- **Kubernetes.** No built-in equivalent; service meshes (Istio) can add
  headers via `EnvoyFilter`, which is the Envoy filter above with no secret
  delivery story of its own.

## 3. Jobs to be done

**Platform builder running agents (job author)**
- When my agent calls a model API, I want the API key added on the way out,
  so the agent (and anything it runs) can never read, print, or leak it.
- When I fork or snapshot an agent sandbox, I want no API key in any copy,
  so a leaked checkpoint is not a leaked key.
- When my SDK refuses to start without an API key, I want to give it a fake
  one and have the real one used anyway, so I do not have to patch the SDK.

**Security / platform admin**
- When I hand a team the right to deploy jobs, I want them able to use only
  the keys I let them read, so deploy access is not key access.
- When a key leaks or expires, I want to rotate it in one place and have
  every running workload use the new one within seconds, without restarts.
- When I revoke a key, I want requests to stop, not to keep going with the
  old value or go out unauthenticated.

**On-call responder**
- When an agent's API calls start failing, I want the failure to name the
  missing key, so I can tell a Legion configuration problem from a provider
  outage.

**Autonomous agent (the workload itself)**
- When a call fails because of configuration, I want a response that says
  so in a form I can read, so I stop retrying and report it.

*Not in scope:* the agent persona as an operator of this feature. It
declares nothing new over MCP; it consumes the existing `job run` surface.

### User scenarios

1. A job declares `credentials = [{ header = "x-api-key", from = { kv =
   "secrets/anthropic" } }]` on its `anthropic` upstream and sets
   `ANTHROPIC_BASE_URL=http://$LEGION_UPSTREAM_ANTHROPIC`,
   `ANTHROPIC_API_KEY=placeholder`. The SDK works; the key is in no env var,
   file, or memory image.
2. The admin runs `legion kv put secrets/anthropic sk-new`. Within one
   heartbeat every node injects the new value.
3. The admin deletes the key. The next requests get a local `503` naming
   `kv secrets/anthropic not found`; `legion net doctor` says the same.

## 4. Goals and non-goals

**Goals**
- The secret never enters the workload: not its env, files, memory, or any
  checkpoint of it.
- No TLS interception, ever, and no CA the workload must trust.
- One declaration per header, next to the upstream it belongs to.
- Live rotation and revocation through KV, with no restarts.
- Fail closed: no request leaves without its declared credential.
- A provider seam that later phases fill without reshaping the jobspec.

**Non-goals**
- Injecting into `CONNECT` tunnels, plaintext `http://` upstreams, or
  `tcp://`/`tls://` upstreams.
- Injecting into traffic that does not go through a declared upstream (the
  gated egress proxy, raw sockets).
- Making secrets invisible to the node, the manager, or operators with
  `kv:read`. This moves the trust boundary from the workload to the node.
- HTTP/2 to the origin (HTTP mode is HTTP/1.1).

## 5. Background: what exists today

- External upstreams: `UpstreamConfig.to` (`src/structs/network.rs`),
  parsed by `parse_upstream_target`; the forwarder opens TLS for
  `https://` in `attempt_request` (`src/worker/netd/http.rs`) and rewrites
  `Host`. The task speaks plaintext to `LEGION_UPSTREAM_<NAME>`.
- HTTP mode per request: policy, external handlers (`authz` sees headers
  minus `authorization`/`cookie`/`proxy-authorization`), `dlp`, retries,
  `proxy` hand-off in absolute form (`handle` in `http.rs`).
- The precedent to mirror: `certFrom = { kv }` ingress certificates
  (kv-cert-delivery-plan). Worker-plane `GetIngressCertificates` names
  allocations, the manager derives the path, the worker polls per heartbeat
  with the version it holds, keeps material in memory
  (`ForwarderContext.kv_certs`), reports problems as `tls_error` /
  `problems`; RunJob enforces `kv:read` on the path (`enforce_kv_cert_read`).
- The desired allocation set carries the task's `connect` declaration as
  JSON (`TaskAllocation.task_connect`); the heartbeat loop fills it from the
  `StreamWork` mirror on streaming workers, then applies the datapath.

## 6. Proposed design

### 6.1 Surface

```nickel
connect = {
  upstreams = [{
    name = "openai",
    to = "https://api.openai.com",
    credentials = [
      { header = "Authorization", prefix = "Bearer ", from = { kv = "secrets/openai" } },
    ],
  }],
},
```

`credentials[]` is `{ header, prefix?, from }`. `from` names exactly one
provider; Phase 1 has `kv`. Later providers are sibling fields of `from`
(Phase 2 shipped `identity = "<task identity name>"`; `handler = "<name>"`
is next), so the shape does not change.

### 6.2 Validation at submit

- Only on an external upstream whose `to` is `https://`. `http://` would put
  the secret on the wire in clear; `tcp://`/`tls://` have no request head;
  a catalog service is not an external destination. Each refusal names why
  and what to do instead.
- `header` is a valid token, not hop-by-hop, `host`, `content-length`, or
  `transfer-encoding`; at most once per upstream (case-insensitive; stored
  lower-cased).
- `prefix` has no CR/LF/NUL; `from.kv` is non-empty.
- RunJob and template runs require `kv:read` on every path
  (`enforce_kv_credential_read`), with the message
  `(connect.upstreams[openai].credentials names kv secrets/openai; the submitter needs kv read on it)`.

### 6.3 Delivery

Worker-plane RPC `GetUpstreamCredentials(node_id, known[{allocation_id,
upstream, header, version}])` returns per entry `{kv_path, version,
unchanged, value, error, absent}`. The manager derives the path from the
allocation's own `task_connect` for allocations placed on the caller's node
(`build_desired_allocations`), so a node can read only what a job on it
declares. `absent` marks authoritative problems (key deleted, declaration
gone) that drop the held value; anything else is transient and keeps it.
One trailing line ending is trimmed; a value with an inner line break or
NUL is refused (it would split the request).

The worker calls it once per heartbeat, after the datapath is applied,
only while some desired allocation declares credentials, with the versions
it holds. Values live in `ForwarderContext.credentials` (a redacting
`SecretValue`), keyed (allocation, upstream, header); they are never
persisted (`netd_state.json` carries only the declaration) and dropped when
the allocation leaves or stops declaring the header.

### 6.4 Datapath

An upstream with credentials always runs in HTTP mode. Per request, after
policy and external handlers and before reading the body, the forwarder
reads every declared value once. A missing one answers `503` locally with
`Legion-Denied-By: credential-unavailable` and a body naming the upstream,
header, and key, without dialing. Otherwise every attempt (retries, and a
`proxy` hand-off) drops the task's headers of those names and sets
`prefix + value`, marked sensitive. `authz` hops run before injection and
never see the value.

### 6.5 Status

`NetAllocStatus.problems` carries
`upstream <key> credential <header>: <error> (requests on this upstream get 503)`
or `...: injecting the previous value; <error>`. `legion net doctor` shows
problems on the datapath layer. No proto change to the status message.

## 7. Proto, struct, config, persistence

- `proto/legion.proto`: `GetUpstreamCredentials` RPC; messages
  `UpstreamCredentialKnown`, `GetUpstreamCredentialsRequest`,
  `UpstreamCredentialValue`, `GetUpstreamCredentialsReply`.
- `src/structs/network.rs`: `UpstreamConfig.credentials`,
  `UpstreamCredential`, `CredentialFrom`.
- `configs/jobs/maximal.ncl`: `credentials` on the `openai` upstream.
- No storage change: the declaration rides the existing job config and
  `task_connect`; values are read from KV on demand.

## 8. Phases

1. **KV provider, header placement, `https://` upstreams.** *Shipped.*
   Everything in §6.
2. **Workload-identity provider.** *Shipped,* in a different shape than
   first sketched: `from = { identity = "<name>" }` names one of the task's
   `identities` (which owns audience, TTL, and claims) instead of an inline
   `{ audience }`, and an identity named this way needs no `env`/`file`
   delivery. The manager mints the token inside `GetUpstreamCredentials`
   rather than the node calling `IssueWorkloadIdentityToken`: the token's
   expiry is its version, and the manager re-mints once less than half the
   TTL is left. Useful for origins that accept OIDC (cloud STS-fronted APIs,
   internal services). See calls #6 to #8.
3. **Webhook provider and non-header placements.** `from = { handler =
   "vault-exchange" }` calls an operator webhook with the allocation's
   facts and caches the returned value for its TTL (token exchange, short-
   lived keys). Placements beyond a header: `query = "api_key"` and
   `basic = { user, password }`.
4. **Per-request audit events.** An opt-in `audit = true` emits a bounded
   `net.credential_used` event (allocation, upstream, header, KV version,
   status) so a security team can answer "which workloads used key X, and
   when", without per-request logs elsewhere.

Separately (not a phase of this plan, a dependency for sandboxes): reach
upstreams from a `sandbox` guest, e.g. guest-agent relays from guest
loopback ports to host vsock ports bound as upstream listeners. *Built for
gated sandboxes* in the same PR, under the gated plan (§6.10, "As built"),
so injection serves `exec`, `container`, and gated `sandbox` tasks. TAP-wired
and loopback-only sandboxes remain open.

## 9. Testing

- Unit (`src/structs/network.rs`): accepts `https://` external; rejects
  `http://`, `tcp://`, `tls://`, service upstreams, hop-by-hop and `host`,
  duplicates, empty paths, missing and unknown providers, CR/LF prefixes.
- Datapath (`src/worker/netd/http.rs`): a plaintext origin sees the injected
  headers and none of the task's own values; a never-fetched value answers
  `503` without dialing; rotation is picked up; a transient error keeps the
  value; `absent` revokes it; leaving allocations drop their values; Debug
  redacts.
- Manager (`src/manager/server.rs`): path derived from the declaration,
  `unchanged` carries no value, a bad value is refused but not `absent`, a
  deleted key and a foreign allocation are `absent`.
- Worker (`src/worker/daemon.rs`): declared keys and upstream specs follow
  the desired set.
- ACL (`tests/integration/acl_endpoints.rs`): the RPC is classified worker-plane and
  refuses a foreign node token; RunJob needs `kv:read` on the path.

## 10. Rollout

Additive: a job without `credentials` is unchanged, and a worker with no
declaring allocation makes no extra call. A worker built before this change
ignores the field and forwards the task's own headers (so a placeholder
reaches the origin and fails with a `401`); upgrade workers before relying
on it. Per the repository's policy there is no installed base to protect.

## 11. Open questions

- Should the `503` also be a `net.denied`-style event? Lean: no, it is a
  configuration error surfaced in status and doctor; revisit with Phase 4.
- Should credentials be declarable on policy objects (one key for every
  sandbox on a pool)? Lean: yes, later; it needs the policy `kv:read` check
  `UpsertNetPolicy` already does for `certFrom`.

## 12. Acceptance criteria (Phase 1)

- A job with a credential on an `https://` upstream submits only with
  `kv:read` on the path, and its requests reach the origin with the header
  set and the task's value gone.
- Deleting the key yields `503 credential-unavailable` on the next request
  after the next heartbeat, and `net doctor` names the key.
- `legion kv put` on the key changes the injected value within one
  heartbeat with no restart.
- No file the worker writes contains the value.
