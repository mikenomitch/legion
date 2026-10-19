# Network Handlers: Egress and Ingress Policy, Upstreams, and Middleware

Legion's node datapath is the loopback forwarder behind `connect.upstreams`.
It can decide, observe, and (when you ask) transform the traffic that crosses
a task's boundary. This page is the reference for the three concepts behind
that, and the surfaces you use them through:

- **Policy**: declarative rules that say what a task may reach and who may
  reach it. You write it inline on a task, or store it as a **policy object**
  that applies to every task a selector matches (a node pool, a runtime, a
  job glob, a label). Policy objects change live, without restarting
  anything.
- **Upstreams**: where a task's egress goes. That is a catalog service, or an
  **external destination** that Legion opens TLS to. The task speaks
  plaintext to a loopback port, so policy can see the request body without
  intercepting anyone's TLS.
- **Handlers**: code on the path, for when policy is not enough. A handler is
  a webhook that returns a verdict, or any proxy in any language.

This page describes what ships today and states what does not. See
[Current scope](#current-scope-and-limitations).

## Quick start

Deny everything except two registries, on the connect datapath, for one task:

```nickel
{
    name = "agent",
    tasks = [{
        name = "sandbox",
        runtime = "exec",
        config = { command = ["sh", "-c", "echo $LEGION_UPSTREAM_PYPI; sleep 3600"] },
        network = {
            egress = { allow = ["pypi.org", "files.pythonhosted.org"] },
        },
        connect = {
            upstreams = [
                { name = "pypi", to = "https://pypi.org" },
                { name = "files", to = "https://files.pythonhosted.org" },
            ],
        },
    }],
}
```

Or apply it once for every sandbox on a pool, with nothing in the job:

```bash
legion net policy apply -f configs/policies/sandbox-egress.ncl
legion net policy show agent              # what the task actually gets, with sources
legion net policy test agent --to https://api.github.com/user   # would this be allowed?
```

## What the datapath does today

Every connection a task makes through an upstream (`LEGION_UPSTREAM_<KEY>`,
an address the node owns) passes through the node's forwarder. The forwarder
runs in one of two modes, and the upstream's `to` scheme alone decides which.

**L4 (the default).** Before dialing, the forwarder evaluates the task's
**effective egress policy** using what is known at connect time: the
destination host (the service name, or an external upstream's host), the
port, and, for service upstreams, the endpoint about to be dialed. A denial
closes the connection without dialing, increments `legion_net_denied_total`,
and is reported on the next heartbeat as a `net.denied` event that names the
rule. An allowed connection is spliced through without parsing. The only
per-connection cost of a connect-time policy is one pass over the rules. No
bytes are inspected.

On Linux, when both ends of a flow are plain sockets, bulk transfers move
with `splice(2)` and never enter the worker's memory. Each direction starts
as an ordinary copy and switches to `splice` once it is clearly carrying
bulk data (a read fills the 16 KiB copy buffer), because for small requests
and replies a copy is cheaper. That covers upstream and ingress flows over
TCP, and a sandbox's flows over its vsock relay. A flow that has to be
decrypted or framed (TLS, a mesh hop, a PROXY header read off the
connection) is copied in user space, as is every flow on macOS. A kernel
that refuses `splice` (some sandboxed kernels) is detected once at first
use, and every flow is then copied. The behavior is the same either way.
Only the CPU cost differs.

**A policy never changes how a stream is parsed.** A chain's `protocol =
"http"` says which rules it may hold (path, method, header); it does not put
an upstream in HTTP mode. Under an HTTP chain, an upstream whose `to` does not
speak HTTP (`db`, `tcp://…`, `tls://…`) is judged with the connect-time facts.
When the first rule that could match it needs the request head, the
connection is refused, and the `net.denied` event names both:

```
net.denied  policy/prod-web#1  upstream 'db' is tcp://, and the first rule that
could match it needs a path, method, or header. Hint: allow 'db' by host ahead
of path rules, or write to = "http://db" if it speaks HTTP
```

`legion job validate`, `job plan`, and `job run` warn about such an upstream
before it is placed, and `legion net policy apply --dry-run` warns about the
running tasks a new policy would do it to.

**HTTP mode.** An upstream is parsed as HTTP/1.1 when its `to` is `http://` or
`https://`. For each request:

1. The chain is evaluated with the request head (method, path, headers) in
   addition to the connect-time facts. A denial returns a `403`. Its body and
   its `Legion-Denied-By` / `Legion-Denied-Reason` headers name the rule, so
   a client (such as an agent in a sandbox) can read why it was denied
   instead of retrying.
2. `dlp` handlers buffer the body up to `maxBytes` and reject a match.
3. The upstream's `http` block applies traffic management:
   - a per-request `timeout`
   - `retries` on connect failures and 5xx, for idempotent methods only (by
     default), under a `budget` (default 20% of requests per minute)
   - a weighted `split` by service tag
   - outlier ejection, which has no setting: an endpoint that answers 5xx five
     times in a row is skipped for 30 seconds, whatever the balancer
4. Upstream connections are pooled per listener and reused across
   requests, so a task's calls pay the TCP, TLS, and mesh handshakes once
   rather than per request. TLS is opened for `https://`, and the `Host`
   header is rewritten to an external target's host. See
   [Connection pooling](#connection-pooling).

`accessLog = true` on the chain writes one NDJSON line
per request to `<worker logs>/<allocation>/net-access.log`. Each line has
the task's labels, the verdict, the deciding rule, status, bytes, and
duration.

HTTP mode supports HTTP/1.1 only, with no HTTP/2 and no WebSocket upgrades.
A task that needs those should use a `tcp://`/`tls://` upstream and an L4
policy.

### Connection pooling

In HTTP mode, the datapath keeps idle upstream connections and reuses them.
This applies to service endpoints, external targets, mesh endpoints, and the
task behind an HTTP-mode ingress chain.

- **Per listener.** Each upstream of each allocation has its own pool. Two
  allocations never share a connection, so a mesh connection's PROXY header,
  sent once when it opens, always names the workload making the request.
- **When a connection comes back.** After the response body has been read,
  if the upstream kept the connection alive. A response with
  `Connection: close` ends the connection instead.
- **Limits.** A connection is closed after 30 seconds idle, and each
  destination keeps at most 32 idle connections.
- **A connection the upstream closed.** It is skipped when picked from the
  pool. If the upstream closes it just as a request goes out, the request is
  sent again on a new connection when it never left the node, or when its
  method is idempotent (`GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`, `TRACE`).
  Otherwise the task gets a `502`. This retry is separate from the `retries`
  policy and does not count against its budget.
- **Not pooled.** A request handed to a [`proxy` handler](#proxy--bring-your-own-proxy).

L4 flows are not pooled. Each connection the task opens is one connection
upstream, and on a mesh endpoint, one mTLS handshake.

### What the datapath costs

These numbers come from `net_bench`
([Testing and Benchmarks](testing-and-benchmarks.md)), which runs the real
forwarder against backends on loopback. That isolates the cost of the hop
itself: there is no NIC, no network round trip, and no TLS. The runs used
Linux 7.0 in a 10-vCPU VM on an Apple M5, on a host shared with other work.
Each figure is the range across runs, not a single best case.

| One hop through the forwarder (L4) | Direct | Through the forwarder | The hop adds |
| --- | --- | --- | --- |
| Request and reply, 100 bytes, kept-open connection | 4–7 µs | 8–13 µs | 4–7 µs |
| New connection, one request and reply | 19–37 µs | 63–120 µs | 45–80 µs |
| Bulk transfer, one stream | 5.3–6.0 GiB/s | 4.1–4.9 GiB/s | |
| CPU per GiB moved, the hop alone | | 0.09–0.11 CPU-s | |

For comparison, a bare userspace proxy (accept, dial, copy) in the same
harness moved about 2 GiB/s at 0.27–0.37 CPU-seconds per GiB. The forwarder
beats it on bulk transfers because it splices them in the kernel on Linux.
On macOS every flow is copied, so expect numbers closer to that baseline.

What this means in practice:

- **Kept-open connections pay a few microseconds per round trip.** A
  request between two nodes in one datacenter usually takes a few hundred
  microseconds on the network, so one hop is a small share of it. A call
  through `connect` to a port another node fronts crosses two hops, one on
  each node.
- **New connections pay the most.** The forwarder accepts one connection and
  dials another, which roughly doubles the setup cost on loopback. A client
  that keeps its connections open avoids this.
- **Mesh mTLS adds a handshake per new L4 connection.** That is one extra
  network round trip plus the handshake's CPU, and it is not in the table.
  HTTP mode pools its upstream connections, so a task making many requests
  pays the handshake once per pooled connection, not once per request.
- **HTTP mode parses every request and response.** On loopback, a request
  through HTTP mode on a reused upstream connection measured within about
  20 µs of a direct one. That figure is noisy: a direct HTTP request in the
  same harness averaged anywhere from about 30 to over 100 µs across runs.

Balancing matters more than the hop when a backend is slow. With three
backends and one of them deliberately slow, strategies that spread traffic
evenly averaged 1.8–1.9 ms per request. `least_latency` averaged 62–73 µs in
HTTP mode, and 98–114 µs over L4 with a new connection per request, close to
the fast backends alone
([Balancing strategies](service-discovery.md#balancing-strategies)). A pick
costs 90–145 ns across 16 endpoints, against 2–5 ns for `round_robin` and
110–180 ns for `least_conn`.

To reproduce these on your own hardware:

```console
$ cargo bench --bench net_bench
```

## Policy

### The inline form

Set `network.egress` and `network.ingress` on a task. `allow` is a shorthand.
The full form has `rules`, `handlers`, and `tls`.

```nickel
network = {
    labels = { user = "alice", tier = "free" },   # attribution; selectors can match on these
    egress = {
        protocol = "tcp",       # tcp (default) | http — never inferred from the rules
        policy = "deny",        # fallthrough: allow | deny (deny when `allow` is used)
        accessLog = false,
        allow = ["*.pypi.org", "api.github.com:443", "10.0.0.0/8:5432"],
        rules = [
            { when = { host = "telemetry.example.com" }, action = "allow" },
            { when = { cidr = "169.254.0.0/16" }, action = "deny", reason = "no metadata service" },
        ],
        handlers = [],
    },
},
```

**Rules match on** these fields:

- `host`: a glob. `*` matches one or more labels at a label boundary, so
  `*.pypi.org` matches `files.pypi.org` but never `pypi.org`. A bare `*`
  matches everything.
- `port`
- `cidr`: the resolved destination, or the peer on ingress.
- `identity`: the verified peer's workload identity (ingress).
- With `protocol = "http"` only: `path` (`*` within a segment, `**` across
  segments), `method`, and `header` (name → value glob).

Every field present in a rule must match. The first matching rule wins, and
`policy` applies when nothing matches. A `deny` rule's `reason` is returned
to the workload and recorded on the event.

**`allow` entries** are `host`, `host:port`, `cidr`, `cidr:port`, an IP, or
`host/path` (only with `protocol = "http"`). A path in a `tcp` chain is
rejected at submit with a fix:

```
egress.allow[1] "api.github.com/repos/**" matches on a path, but
egress.protocol is "tcp". Set protocol = "http" to match on paths, or
drop the path to match the host only.
```

`protocol` is never inferred. If typing a slash silently turned a rule set
into L7, you could not review the policy just by reading it, and it would
cost about twice as much per byte.

### Policy objects

A stored policy is the same block plus a **selector** and an
**enforcement** mode. It lives in the cluster store (raft-replicated) and you
edit it with `legion net policy`. It applies to every matching task on the
task's next heartbeat, with no restart, rollout, or job edit.

```nickel
# configs/policies/sandbox-egress.ncl
{
    name = "sandbox-egress",
    "priority" = 100,
    selector = {
        nodePools = ["untrusted"],       # node.pool globs
        namespaces = ["agents-*"],       # namespace globs (docs/namespaces.md)
        runtimes = ["sandbox"],
        jobs = ["agent-*"],              # globs over the job's bare name
        labels = { tier = "free" },      # task network.labels that must all match
    },
    enforcement = "require",             # require | default
    mode = "gated",                      # optional network.mode to impose
    egress = { protocol = "http", policy = "deny", allow = ["*.pypi.org", "api.github.com/repos/**"] },
}
```

`selector = {}` matches every task on the cluster. Values within a list are
ORed, and the fields are ANDed.

| `enforcement` | Applied when | A job author may |
| --- | --- | --- |
| `default` | the task declares no chain of its own for that direction | replace it entirely by declaring one |
| `require` | always, **ahead of** the task's own block | append rules and handlers and add denials, but never remove or widen |

**The merge is deterministic, and you can inspect it.** Every matching
`require` policy applies in `priority` order (ties broken by name). Then
exactly one of these applies: the task's own block, or the highest-priority
matching `default` policy. Nothing after a required `deny` fallthrough can
widen it. Two `default` policies that would tie for the same task (equal
priority, overlapping selectors) are rejected at `apply`, so the winner is
never decided at runtime. In the merged result, every rule and handler
carries a `source`: `policy/<name>#<index>` or `task#<index>`.

`legion net policy show <job>` prints that merged result. The node datapath
runs the same function on the same inputs.

### How policy reaches a node

The heartbeat reply carries one content hash per allocation. A node fetches
each body it has not seen once and caches it, so a thousand identical
sandboxes cost one fetch. Bodies are swapped only after a successful
heartbeat, and the last known good body is kept through a manager outage.
This is the same fail-static behavior as the endpoint feed. A cluster with no
stored policy and no task that declares one pays nothing extra per heartbeat.

## Upstreams

Each `connect.upstreams` entry names its destination with `to`: a catalog
**service** by its bare name, or an **external destination** by URL
([Service Discovery](service-discovery.md#connecting-to-services-the-connect-block)
has the full rule):

```nickel
connect = {
    upstreams = [
        { to = "db" },                                       # a catalog service, L4
        { to = "http://search" },                            # a catalog service, HTTP
        { name = "openai", to = "https://api.openai.com" },  # external
    ],
},
```

An external upstream has a `name` and a `to`. The task gets
`LEGION_UPSTREAM_<NAME>`, the same as for a service upstream. The scheme of
`to` decides two things:

| `to` | Legion originates TLS | HTTP framing |
| --- | --- | --- |
| `tcp://host:port` | no | no |
| `tls://host:port` | yes | no |
| `http://host[:port]` | no | yes |
| `https://host[:port]` | yes | yes |

A dotless external host carries its port (`http://wiki:80`); without one, a
bare name is a catalog service.

With `https://`, the task speaks **plaintext HTTP to its upstream address**, and the
node's forwarder dials the destination over TLS (verified against the Mozilla
roots, or against the bundle the upstream's `ca` pins,
[TLS on ports](#tls-on-ports)). This lets policy and handlers see an LLM API call's body **without
intercepting anyone's TLS**. Point the client at the upstream:

```sh
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
```

Legion never holds a certificate authority that can impersonate the public
internet. `to` must not include a path or query. The task chooses paths.

The `http` block on an upstream (`timeout`, `retries`, `split`) adds traffic
management to an upstream that speaks HTTP. Per-upstream `handlers` run
there too. See [What the datapath does today](#what-the-datapath-does-today).

### Credential injection

An external `https://` upstream can carry the API key itself, so the task
never holds it. The node's forwarder sets the header on every request it
sends to the destination, with a value from one of two providers:

| Provider | `from` | The value |
| --- | --- | --- |
| KV | `{ kv = "secrets/openai" }` | the KV value at that path |
| Workload identity | `{ identity = "api" }` | a signed token for the task's identity named `api`, minted for this allocation and refreshed before it expires |

A KV-backed key looks like this:

```nickel
connect = {
  upstreams = [{
    name = "anthropic",
    to = "https://api.anthropic.com",
    credentials = [
      { header = "x-api-key", from = { kv = "secrets/anthropic" } },
    ],
  }, {
    name = "openai",
    to = "https://api.openai.com",
    credentials = [
      { header = "Authorization", prefix = "Bearer ", from = { kv = "secrets/openai" } },
    ],
  }],
},
```

```bash
legion kv put secrets/openai "sk-..."
```

Point the client at the upstream, as for any external upstream. An SDK that
refuses to start without a key gets a placeholder:

```sh
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
export OPENAI_API_KEY=placeholder          # replaced on the way out
export ANTHROPIC_BASE_URL="http://$LEGION_UPSTREAM_ANTHROPIC"
export ANTHROPIC_API_KEY=placeholder
```

An origin that trusts Legion's OIDC issuer (a cloud STS, an internal API
that verifies JWTs) can take a [workload identity](workload-identity.md)
token instead of a stored key. Declare the identity with the origin's
audience and name it in `from`. It needs no `env` or `file` delivery, and
without one the token never enters the task:

```nickel
identities = [{ name = "api", audience = ["https://api.internal.example"], ttl = "15m" }],
connect = {
  upstreams = [{
    name = "api",
    to = "https://api.internal.example",
    credentials = [
      { header = "Authorization", prefix = "Bearer ", from = { identity = "api" } },
    ],
  }],
},
```

The manager mints the token from the scheduling record, exactly like a
delivered one (the identity's audience, TTL, `sub` template, and extra
claims; see [Workload identity](workload-identity.md)), and the node injects
it. The node asks for a new one once less than half the TTL is left, so the
origin always receives an unexpired token and nothing in the task refreshes
anything. An identity that also sets `env` or `file` is delivered to the
task as well, as usual.

**How it works without intercepting TLS.** With an `https://` upstream the
task already speaks plain HTTP to its loopback port, and the node opens TLS to
the destination (see the table above). So the node reads the request head
anyway; injecting a credential is setting one more header on the request it
builds for the origin. Legion holds no certificate authority, terminates no
TLS the task opened, and never sees inside a `CONNECT` tunnel.

**Where the secret is, and is not.** The value lives in the worker's memory
and on the TLS connection from the node to the origin. It is never in the
task's environment, files, or memory, so it is never in a `sandbox`'s
memory checkpoints, forks, or published snapshots either. The worker never
writes it to disk (including `netd_state.json`) or to a log line; the access
log records no headers. After a worker restart, the node fetches it again on
its first heartbeat, and until then requests on that upstream get a `503`.

**Semantics.**

- **Overwrite.** Whatever the task sent for the header (any case, any number
  of times) is dropped, and the header is set to `prefix` + the value. A
  placeholder is harmless; so is no header at all.
- **Every request.** The value is read once per request and sent on every
  attempt of it: retries, and a [`proxy` handler](#proxy--bring-your-own-proxy)
  hand-off. A proxy hop sees the credential, because it has to reach the
  origin with it. The hand-off is plain HTTP to the proxy (it opens TLS to
  the origin), so a proxy on another node receives the key over the network
  unencrypted: prefer a `node`- or `port`-local proxy target, and run only
  proxies you would trust with the key.
- **`authz` hops never see it.** They run before injection, and they receive
  the task's own headers (minus `authorization`, `cookie`, and
  `proxy-authorization`, as always), so a placeholder at most.
- **Fail closed.** A request whose credential the node does not hold (never
  fetched, or the key was deleted) is answered locally, without dialing:

  ```http
  HTTP/1.1 503 Service Unavailable
  Legion-Denied-By: credential-unavailable
  Legion-Denied-Reason: credential-unavailable: upstream openai header authorization: kv secrets/openai not found

  {"legion":{"error":"credential-unavailable: upstream openai header authorization: kv secrets/openai not found","source":"credential-unavailable","upstream":"openai"}}
  ```

  An unauthenticated request never leaves, so a missing key cannot turn
  into a burst of `401`s, or into requests that succeed against an origin
  that allows anonymous access.
- **Rotation.** `legion kv put secrets/openai ...` reaches every node on its
  next heartbeat (`FetchDatapathMaterial`, asked only for versions newer
  than the one held). Requests already in flight keep the old value. Nothing
  restarts. Deleting the key revokes it on the next heartbeat. An identity
  token is replaced on the first heartbeat after half its TTL has passed.
- **Fail-static.** An unreachable manager, a KV read error, or a failed
  token mint keeps the last good value. A value no header can carry (a line
  break or NUL inside it; one trailing newline is trimmed) is rejected and
  the previous value stays. An identity token held through a long manager
  outage still expires at the origin, which then answers `401`.
- **Status.** Problems are reported per allocation, and `legion net doctor`
  shows them on the datapath layer:
  `upstream openai credential authorization: kv secrets/openai not found (requests on this upstream get 503)`,
  or `... injecting the previous value; kv secrets/openai could not be read`.

**Validation.** Submit refuses credentials that could not work, or would
leak:

| Declaration | Result |
| --- | --- |
| `to = "http://..."` | refused: the secret would cross the network in the clear. Use `https://` |
| `to = "tcp://..."` or `"tls://..."` | refused: there is no request head to put a header in. Deliver the secret to the task instead |
| a catalog `service` upstream | refused: credentials are for external destinations |
| `header` = `host`, `content-length`, `transfer-encoding`, or a hop-by-hop header | refused: the datapath owns these |
| the same header twice on one upstream | refused |
| `from = { kv = "" }`, no provider, or both `kv` and `identity` | refused |
| `from = { identity = "api" }` with no identity named `api` on the task | refused, naming the identities it does declare |
| `from = { identity = "legion" }` | refused: the control-plane identity authenticates to Legion itself and must not be handed to another origin |
| an identity with neither `env` nor `file`, that no credential names | refused: nothing would receive it |
| `changeMode` or `changeSignal` on an identity with neither `env` nor `file` | refused: only the node holds that token, so there is nothing to signal or restart |

Header names are case-insensitive and stored lower-cased.

**Access.** Naming a key in `credentials` sends its value to a host the job
author chose, so it needs the right to read it: `RunJob` and template runs
require `kv:read` on every credential path, in addition to `job:write`.
Without it the submit fails with `PermissionDenied`:
`... (connect.upstreams[openai].credentials names kv secrets/openai; the submitter needs kv read on it)`.
An identity credential needs no extra right: declaring an identity is
already part of submitting the job. A node can only fetch a credential that
an allocation placed on it declares. It names (allocation, upstream,
header), and the manager derives each KV path or identity from the
allocation's own declaration.

**Identity provider prerequisites.** The manager must run with
`workloadIdentity.enabled: true`. When it does not, the node holds no token
and requests on that upstream get the `503` above, with the reason
`workload identity is not enabled on this manager`.

**Gated tasks.** A gated `container` or `exec` task reaches its upstreams
inside its namespace, and declared upstreams are an explicit allowance, so
[default deny](#default-deny) does not block them. HTTPS a client sends
through the egress proxy (`HTTPS_PROXY`, `CONNECT`) is an opaque tunnel, so
**no credential is ever injected on proxy traffic**. Set the client's base
URL to the upstream instead, as above.

**Sandboxes.** A gated `sandbox` reaches its upstreams over vsock, through
the same listener (see [Sandboxes](#sandboxes)), so injection works there
unchanged: the guest calls `http://$LEGION_UPSTREAM_OPENAI/v1` with a
placeholder, and the key is added on the node. It is never in the guest's
memory, so never in its memory checkpoints, forks, or published snapshots.

**Limits.**

- The providers are `kv` and `identity`. There is no webhook provider (a
  token exchange, for example), and a credential can only be a request
  header: not a query parameter or basic-auth pair.
- HTTP/1.1 only, like the rest of HTTP mode. A client that needs HTTP/2 to
  the origin cannot use an upstream.
- A TAP-wired or loopback-only `sandbox` cannot reach its upstreams, so
  injection reaches `exec`, `container`, and gated `sandbox` tasks only.
- There is no per-request audit event for an injected credential; the
  access log (if on) records the request, not which credential it carried.

## Name-is-config: `<key>.svc.legion`

On a Linux node, every allocation that declares upstreams gets its own
loopback /24 at placement (`127.42.<b>`, unique per node). All of 127/8 is
loopback on Linux with no configuration. Placement goes by the node's `os`
fingerprint, so a manager on macOS assigns blocks to its Linux nodes and none
to its macOS ones. In that block:

- **`.1` is the allocation's resolver**, exported as `LEGION_DNS`. It answers
  `<key>.svc.legion` (and bare `<key>`, through the `svc.legion` search
  domain) with that allocation's upstream VIP. An upstream that crosses into
  another [namespace](namespaces.md#services-and-the-mesh) is keyed
  `<service>.<namespace>`, so it answers `auth.shared.svc.legion` (and the SRV
  form `_auth._tcp.shared.svc.legion`). SRV records carry the port.
  Other names are forwarded to the host's resolvers after the egress policy
  has checked the name. A host the policy would deny gets `NXDOMAIN` in one
  round trip and a `net.denied` event (`protocol: dns`), instead of a
  hanging connect. Names are not checked when the chain matches on CIDRs,
  because the rule needs the address.
- **`.2` onward are the upstream VIPs**, one per `connect.upstreams` entry in
  declaration order. Each listens on the upstream's **canonical port**,
  which is the first of these that applies: a pinned `port`, the port the
  service declared on its own task (`network.ports[].to`, or its static
  port), an external target's port, or a port from the dynamic range.
  `db.svc.legion:5432` works because the `db` service declared 5432.

The VIP is the upstream's address: `LEGION_UPSTREAM_<KEY>` is
`<vip>:<canonical port>`, and the upstream takes no port from the node's
dynamic range. A pinned `port` also binds `127.0.0.1:<port>` (the same
listener), for a config file that wants a literal loopback address. A
sandbox's upstreams keep a loopback port too, because the guest's relay is
keyed by it.

| Env var | Value |
| --- | --- |
| `LEGION_DNS` | `127.42.<b>.1`, the allocation's resolver (`127.0.0.53` for a gated allocation with no upstreams) |
| `LEGION_UPSTREAM_<KEY>` | `<vip>:<canonical port>` |
| `LEGION_UPSTREAM_HOST_<KEY>` | `<vip>` |
| `LEGION_UPSTREAM_PORT_<KEY>` | `<canonical port>` |

The variables carry the address rather than the name because an `exec` task
keeps the host's `/etc/resolv.conf` (below), where `<key>.svc.legion` does
not resolve.

**Who gets the resolver automatically.** Every allocation with upstreams,
and every gated `container` or `exec` allocation, upstreams or not. A gated
allocation without a VIP block gets its resolver at the fixed address
`127.0.0.53` inside its namespace, which spends no VIP block. A `container`
task's `/etc/resolv.conf` is generated to point at it (`search svc.legion`,
`ndots:1`). An `exec` task gets `LEGION_DNS` and keeps the host's
`/etc/resolv.conf`, in gated mode too. Legion does not generate a
resolv.conf for `exec` tasks, so point your tool at `LEGION_DNS` to use the
allocation's resolver; on a host whose `resolv.conf` names systemd-resolved's
`127.0.0.53`, a gated `exec` task reaches its own resolver without that. A gated `sandbox` uses the guest
agent's relay as its resolver (`nameserver 127.0.0.1` in the guest's
`resolv.conf`), and the agent also listens on each upstream's VIP in the
guest, so `<key>.svc.legion` works there too (see "Sandboxes").

**Forwarding.** The host's resolvers are the `nameserver` lines of the
node's `/etc/resolv.conf`, read when the worker starts. A forwarded query
goes to them over UDP, first answer wins. When a client asked over TCP and
the answer came back truncated, the resolver asks the same nameserver again
over TCP and returns the full answer. A UDP client gets the truncated answer
and asks again over TCP itself; the resolver serves TCP on the same address.
A node whose `resolv.conf` names no nameserver answers every name outside
`svc.legion` with `SERVFAIL` at once, and the worker logs a warning when it
starts. No query goes to a public resolver the operator did not name.

**macOS** configures only `127.0.0.1` on `lo0`, so no VIP block is assigned
there. Each upstream gets a loopback port from the dynamic range, which is
what `LEGION_UPSTREAM_<KEY>` names, and there is no per-allocation resolver.
The same applies on a Linux node that has run out of VIP blocks.

## Handlers

Handlers are code on the path. They only run when declared, either on a
chain (`egress.handlers`, `ingress.handlers`, on a task or a policy object)
or on one upstream (`connect.upstreams[].handlers`). They run in order after
the rules have allowed the flow, with the chain's handlers first, then the
upstream's. Every path runs the same list: an L4 upstream, an HTTP-mode
upstream, the gated proxy (`CONNECT` and absolute-URI requests), and
ingress.

A handler is written as its kind, with the kind's settings inside it:

| Kind | What it does | Stage | Runs |
| --- | --- | --- | --- |
| `authz` | POSTs the flow's facts to `url` and reads back a verdict, cached per (hop, identity, destination[, method, path]) for `cache` (default `10s`, at most `5m`) | connect, request | external |
| `proxy` | hands the flow to any proxy (a catalog `service`, with `local = true` for its endpoints on this node only, or a sidecar's `port` label in the same job) with a PROXY protocol v2 header (TCP) or `Legion-*` headers (HTTP) | connect, request | external |
| `dlp` | rejects HTTP request bodies matching `patterns`, buffering up to `maxBytes` (default 1 MiB) | body | netd |
| `plugin` | runs a stored [network plugin](network-plugins.md) object's WASM module (proxy-wasm ABI) with this reference's `config`: it may answer, rewrite, meter, or sign | the stages its object declares: connect, request, body, response, done | netd (wasm) |

**Stage** is where in a flow the kind acts. An L4 path runs a hop at
`connect`, with the connect-time facts; an HTTP path runs it at `request`,
with the request head, and then at `body`. A kind whose stages all need
HTTP, such as `dlp`, is refused at submit on a chain whose `protocol` is
`tcp` or on an upstream that does not parse HTTP:

```
network.egress.handlers[0] dlp acts at stage "body", which needs HTTP, but
network.egress.protocol is "tcp". Set protocol = "http", or put it on an
upstream whose `to` is http:// or https://
```

**Runs** is where the work happens: in the node's datapath (`netd`), in
the datapath inside a WASM sandbox (`netd (wasm)`), or outside it
(`external`, a webhook or another proxy). `legion net policy
show` prints both for every hop.

Every hop has a `timeout` (default `2s`) and an `onError` (default `deny`).
If a hop cannot be reached (a webhook that times out, a proxy with no
endpoint) or a `dlp` body is larger than `maxBytes`, the flow fails
**closed** unless the hop sets `onError = "allow"`. `onError` belongs to the
hop: a chain has no `onError` of its own, and a chain that sets one is
refused at submit.

```nickel
egress = {
    protocol = "http",
    allow = ["api.github.com/**"],
    accessLog = true,
    handlers = [
        { authz = { url = "http://policy.internal:8080/check", cache = "30s" } },
        { proxy = { service = "egress-filter" }, onError = "deny" },
        { dlp = { patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"], maxBytes = "1Mi" } },
    ],
},
```

A setting in the wrong place is an error that names the kind:

```
handler 'proxy' has an unexpected field 'url' beside it; a handler is one
kind (authz, proxy, dlp, plugin) plus timeout and onError, with the kind's
settings inside it
```

There is no `log` handler. The chain's `accessLog = true` writes the access
log for every HTTP request the chain governs, upstreams and the gated proxy
alike.

### `authz` — ask your service

Most custom logic fits a webhook that answers "may this workload talk to
that destination?" Legion asks it once per new (identity, destination) pair
and caches the answer.

**Request** (`POST`, `application/json`, header `Legion-Handler: authz`):

```json
{
  "allocation": 41, "job": "agent-7", "task": "sandbox",
  "identity": "auto:agent-7:default:sandbox",
  "labels": { "user": "alice", "tier": "free" }, "node": "node-3",
  "direction": "egress", "protocol": "http",
  "destination": "api.github.com:443", "host": "api.github.com", "port": 443,
  "peerIdentity": null,
  "method": "GET", "path": "/repos/x", "headers": [["host", "api.github.com"]]
}
```

`method`, `path`, and `headers` are present in HTTP mode only.
`authorization`, `cookie`, and `proxy-authorization` are never forwarded.
`peerIdentity` is the verified client on ingress.

**Reply** (`200`): `{ "verdict": "allow" | "deny", "reason": "...", "ttl": "1m" }`.
Anything else is a handler error and follows `onError`. That includes a
non-2xx status, a body that is not JSON, a verdict other than
`allow`/`deny`, and a timeout. A `deny` works like any other policy denial.
The connection closes (or the request gets a `403` that names the hop in
`Legion-Denied-By`), a `net.denied` event carries the reason, and
`legion net policy show` names the hop.

**Where it runs.** On L4 paths (an upstream splice, the gated egress proxy's
`CONNECT`, an ingress splice), the webhook is called at connect time
with the connect-time facts. In HTTP mode it is called per request with the
request head: once per new (identity, destination, method, path), then from
the cache.

### `proxy` — bring your own proxy

A `proxy` hop takes over the flow. Legion dials the proxy, hands it the
connection along with everything it needs to decide or filter, and then
splices:

- **TCP** (an L4 upstream, `CONNECT`, an ingress splice): the
  connection starts with a **PROXY protocol v2** header. It carries the
  source and destination addresses, plus these Legion TLVs: `0xE0`
  identity, `0xE1` the original destination as `host:port` (or the task's
  private port on ingress), `0xE2` allocation id, `0xE3` labels
  (`k=v;...`), `0xE4` direction, `0xE5` job, `0xE6` task. HAProxy, Envoy,
  nginx, and Caddy read the addresses and ignore TLVs they do not know. Set
  `proxyProtocol = false` to leave out the header for a proxy that cannot
  accept one.
- **HTTP** (an HTTP-mode upstream, an absolute-URI request through the
  gated proxy): the request reaches the proxy in **absolute form**
  (`GET http://api.example/x HTTP/1.1`) with headers `Legion-Identity`,
  `Legion-Allocation`, `Legion-Job`, `Legion-Task`, `Legion-Labels`,
  `Legion-Direction`, `Legion-Destination`, and `Legion-Peer-Identity` on
  ingress. The proxy answers for the origin. A `4xx` from the proxy is the
  denial.

Targets:

- `{ proxy = { service = "x" } }`: any healthy endpoint of catalog service
  `x`, round-robin.
- `{ proxy = { service = "x", local = true } }`: only endpoints of `x` on the
  same node, such as a per-node filter run as a system job.
- `{ proxy = { port = "filter" } }`: a sidecar task in the same job with a
  port of that label, on this node.

Exactly one of `service` and `port` is set. `proxyProtocol = false` leaves
out the PROXY v2 header.

The manager sends the endpoints on the heartbeat, as it does for an
upstream. A target with no endpoint is a handler error (`onError`).

An ingress `proxy` hop hands the inbound connection to the proxy, which
reaches the task on the private port named in the header. This works for a
task in host mode, where the proxy can dial the port. A gated task's private
port is inside its namespace, which an external proxy cannot enter.

### Health, and `legion net handler test`

Every hop the datapath has run is reported on the heartbeat
(`NetAllocStatus.handlers`: source, kind, target, healthy, calls, errors,
last error). `legion net doctor` shows it, and so does the **Datapath** area
of the UI's task Network section. Test a handler before you wire it in:

```bash
legion net handler test http://policy.internal:8080/check   # authz: sample facts → verdict?
legion net handler test 10.0.0.9:9000                       # proxy: accepts a PROXY v2 header?
```

The probe sends one synthetic request the way the datapath would. It prints
each check (`reachable`, `status_2xx`, `verdict`, `ttl`, or
`proxy_protocol`) with a fix, and exits non-zero when a check fails. It is
also available as an MCP tool (`legion_net_handler_test`).

## The mesh

One switch puts a service on the mesh:

```nickel
service = { port = "http", mesh = true, allow = ["web", "agent-*"] },
```

With `mesh = true`:

- **The service's port terminates node-to-node mTLS** with the node's
  certificate, and verifies peers against the cluster CA. The datapath owns
  the advertised port and the task binds a private twin, as with any
  [ingress](#ingress) chain, so the task never holds a certificate.
- **The catalog marks the endpoints mesh** (`ServiceEndpoint.mesh_tls`).
  Each consumer's datapath dials them with its own node certificate,
  verifies the peer node, and names the consuming **workload**
  (`<region>:<job>:<group>:<task>`) in a PROXY v2 TLV inside the session.
  The receiving node trusts that attribution because the peer proved it is a
  node, and passes the identity to `identity` rules and `authz`.
- **`service.allow` is enforced on the wire.** Its entries compile into the
  service's ingress chain as a check that runs before any rule, with the
  source `service/allow`. A verified caller outside the list is refused, and
  a `net.denied` event names it. `legion net policy show` prints the check
  as rule `0.` Without the mesh, `allow` is enforced by the endpoint feed
  only ([Service Discovery](service-discovery.md#intentions-serviceallow)).
- **A plain client that dials the port directly is refused at the
  handshake.** A non-Legion client that must reach the port has two options:
  present a certificate from the cluster CA (`legion net ca show` prints its
  trust bundle, but Legion does not issue certificates for outside clients),
  or go through a Legion task. With a `file` or `exec` CA, remember that
  anything your PKI signs under that CA with a `legion://` URI SAN is a node
  or a manager to every peer: keep that intermediate (or Vault role) for
  Legion alone.
- **Workloads never hold certificates.** The node attributes identity,
  which keeps a per-user sandbox's identity out of the sandbox.

`mesh` is resolved **at placement**, like [`network.mode`](#networkmode),
because the catalog's flag and the listener must agree. A policy object can
set it for the tasks it matches:

```nickel
{ name = "mesh-prod", enforcement = "require", selector = { nodePools = ["prod"] }, mesh = true }
```

Under `require` it wins over the task; under `default` it applies only to a
task that does not set `mesh` itself, so a task's own `mesh = false` beats a
default. A port that carries its own `tls` (below) is never made the mesh
port by a policy, and a task that sets both on one port is rejected at
submit. A policy change reaches an allocation at its next placement.

**The node certificate.** The mesh uses the **cluster CA**. By default that
is a CA the manager creates on first use and replicates with the control
lane; it can instead be an intermediate from your own PKI or an external
signer such as Vault PKI ([The Cluster CA](cluster-ca.md)). Every worker
node holds a short-lived **node certificate** from it: the worker generates
its own key, sends a CSR (`IssueNodeCertificate`, authorized by its node
token) right after registration, and the manager has it signed with the SANs
`<node>.node.legion` and `legion://node/<node id>`, valid for 24 hours. The
manager picks those names itself and checks what an external CA returns
before handing it out. The worker refreshes it after 8 hours, or within one
heartbeat when the cluster's trust bundle changes. The same certificate
serves the node's worker plane
([auth-and-acls.md](auth-and-acls.md#the-worker-plane)), and the key and
certificate persist under `<state-dir>/identity/` (mode `0600`) so a
restarted node has its mesh identity before it reaches a manager. The
private key never leaves the node.

`legion net ca show` prints the CA trust bundle (never a key), its pin, which
provider supplies it, and where a rotation stands. `legion net ca rotate`
rotates the built-in CA in two steps that break no mesh connection: the new
CA, cross-signed by the old one, signs at once, and the old one stays in
every trust bundle for one certificate lifetime (24 hours). Every node
re-issues on its next heartbeat. A `file` or `exec` CA rotates outside
Legion with the same add-switch-remove steps
([The Cluster CA](cluster-ca.md#rotation)). Peers never fall back to
plaintext ([Rotating the cluster
CA](auth-and-acls.md#rotating-the-cluster-ca)).

## TLS on ports

TLS that is not the mesh goes on the port that serves it. The node
terminates it, and the task speaks plaintext on its private port:

```nickel
network = {
    ports = [
        { name = "https", static = 443, tls = { cert = { kv = "certs/app.example.com" } } },
        { name = "admin", to = 9443,
          tls = { cert = { file = { cert = "/etc/app/tls.crt", key = "/etc/app/tls.key" } },
                  clients = { ca = { kv = "certs/admin-clients" } } } },   # present = mutual
    ],
},
```

| Field | Meaning |
| --- | --- |
| `tls.cert` | The certificate chain and its private key. `{ kv = "<path>" }`: one PEM bundle held in KV, swapped live when the key changes ([below](#certificates-and-cas-from-kv)). `{ file = { cert, key } }`: two files on the node (or staged artifacts); `{ file = "<path>" }`: one file holding both. |
| `tls.clients.ca` | Present: the port is **mutual**, and a client must present a certificate that verifies against this CA bundle, `{ kv = "<path>" }` or `{ file = "<path>" }`. |

- **A mutual port's peer identity** is its certificate's first URI SAN, or
  else its first DNS SAN, or else its CN. `identity` rules and `authz` see
  that string. A port's own CA vouches only for names: a `legion://node/…`
  SAN it verifies is just a name, never a node whose PROXY v2 attribution is
  trusted. Only the mesh CA vouches for nodes.
- **A port is either the service's mesh port or carries its own `tls`,
  never both.** Submit says so.
- **An identity is not a certificate.** `cert = { identity = … }` is a
  submit error, and so is a relative file path.
- **Chains carry policy only.** An ingress chain or policy object has no
  `tls`; a policy object can impose `mesh = true` but cannot push
  certificate material onto the tasks it matches.

**Originating TLS to an external upstream** needs no block: a `tls://` or
`https://` [upstream](#upstreams) already originates. Its certificate is
verified against the public (Mozilla) roots, or against the bundle the
upstream's `ca` pins:

```nickel
{ name = "corp", to = "https://api.corp.example", ca = { kv = "certs/corp-ca" } }
```

`ca` takes `{ kv }` or `{ file }` and applies to external `tls://` and
`https://` upstreams only (a catalog service on the mesh is verified
against the cluster CA). A pinned bundle that is missing or does not parse
fails the upstream's connections; it never falls back to the public roots,
and the allocation's `problems` say why.

### Certificates and CAs from KV

A public certificate (from Let's Encrypt or your own CA), a client CA, or an
upstream's trust bundle can live in KV instead of on node disks. The node
serves it and swaps it when the key changes, so renewal is a single
`kv put`:

```bash
legion kv put certs/app.example.com "$(cat fullchain.pem privkey.pem)"
legion kv put certs/admin-clients "$(cat clients-ca.pem)"
```

- **One value, one bundle.** A certificate's KV value is PEM: the chain,
  leaf first, then the private key. Keeping both in one key means a reader
  never pairs a new certificate with an old key. A CA's value is one or more
  PEM certificates.
- **The task never holds the key.** The node terminates TLS.
- **Swaps are live.** Each heartbeat, a node asks the manager for material
  newer than the version it holds (`FetchDatapathMaterial`). New connections
  get the new certificate or CA, and connections already open keep theirs.
  Nothing restarts.
- **Fail-static.** The manager checks a value before sending it: a
  certificate bundle must hold a certificate and a private key that matches
  the leaf, and a CA bundle at least one certificate. A bad value, a deleted
  key, or an unreachable manager leaves the node serving the last good
  material. The allocation reports the problem: `tls_error` names the key,
  and `problems` reads
  `ingress TLS: serving the previous certificate; kv certs/app.example.com holds no private key`.
  A port that has never had good material is not ready, and its handshakes
  fail.
- **Access.** Naming a key hands its value to the datapath, so it needs the
  right to read it. `RunJob` (and template runs) require `kv:read` on every
  path a task names in `ports[].tls.cert`, `ports[].tls.clients.ca`, or
  `connect.upstreams[].ca`, in addition to their own permissions. Without it
  the submit fails with `PermissionDenied`, naming the field and the key. A
  node can only fetch what an allocation placed on it declares: it names an
  allocation and a port label (or upstream key), and the manager derives each
  path itself.
- **In memory only.** The node never writes the material to disk, including
  its `netd_state.json`. After a worker restart it fetches it again on its
  first heartbeat.

To renew automatically, run a scheduled batch job that puts the new bundle.
[`configs/jobs/cert-renewer.ncl`](../configs/jobs/cert-renewer.ncl) does it
with lego and DNS-01, and
[`configs/jobs/tls-edge.ncl`](../configs/jobs/tls-edge.ncl) serves the result.
For a task that terminates TLS itself, such as nginx, deliver the same key as
a file with `onChange = "update"` instead
([Live file updates](kv-store.md#live-file-updates)).

Limits: a renewal reaches the node on its next heartbeat, so expect up to
one heartbeat interval of delay. Legion does not run an ACME client itself.

**Doctor.** `legion net doctor` reports `tls` as a layer. It flags files
that do not parse, a KV value that was rejected (naming the key and the
`legion kv put` that fixes it), and a mesh certificate the node has not
received yet. Until it arrives, mesh peers are refused, and the
allocation's `problems` say so.

Limits: there is no kTLS/`splice` fast path, so a terminated connection is
copied in user space. TLS 1.2 and 1.3 are supported. There is no
certificate revocation; the mesh's 24-hour lifetime takes its place.

## Sources

Everywhere a value comes from somewhere, it is the same record:

| Source | Read by | Allowed in |
| --- | --- | --- |
| `{ kv = "<path>" }` | the manager, for the node; the submitter needs `kv:read` on it | `ports[].tls.cert`, `ports[].tls.clients.ca`, `connect.upstreams[].ca`, `credentials[].from` |
| `{ identity = "<name>" }` | the manager mints the task's workload identity of that name for the allocation | `credentials[].from` |
| `{ file = "<path>" }` or `{ file = { cert, key } }` | the node itself; never sent over the wire | `ports[].tls.cert`, `ports[].tls.clients.ca`, `connect.upstreams[].ca` |

Each field allows the kinds that make sense for it, and anything else is a
submit error that names the field. Credentials take no `file`, so a node's
disk never becomes a secret store, and every credential's access is checked
at submit against `kv:read`. Exactly one key per source: `{ kv = …,
identity = … }` does not parse.

A node fetches everything `kv` and `identity` sources hold, plus the policy
bodies its allocations are assigned, in **one** worker-plane call per
heartbeat, `FetchDatapathMaterial`. Each request entry names a kind
(`policy`, `port_cert`, `client_ca`, `upstream_ca`, `credential`) and what
it is for, with the version the node holds, so an unchanged value costs
nothing on the wire. Values stay in memory only.

## `network.mode`

| `mode` | Meaning | Default for |
| --- | --- | --- |
| `host` | host networking. Policy is cooperative (proxy env vars, upstreams, the resolver) | `exec`, `sandbox`, `container` on macOS |
| `gated` | the workload's network namespace holds loopback and the datapath only, so a `container` or non-root `exec` task can't open a socket around it. Every destination the egress proxy dials is judged by the [host guard](#the-host-guard) and then by policy, by the address a name resolves to. A root `exec` task can leave the namespace. For a `sandbox`, it is a microVM with no NIC whose only way out is a vsock to the datapath (see "Sandboxes") | `container` on Linux |
| `auto` (the default) | `gated` for a `container` on a Linux node, `host` for everything else | — |

The effective mode is resolved **at placement**, with the node in hand:
`auto` reads the node's `os` fingerprint, so one `container` job runs gated
on its Linux nodes and on host networking on its macOS nodes. It is the
task's own value, or the one imposed by the highest-priority matching
`require` policy (`mode` on a policy object). An explicit `gated` is never
downgraded. A namespace is set when the task starts, so a policy
that changes `mode` affects the next placement, not a running allocation.

### Gated mode

With `mode = "gated"`, the worker creates an empty network namespace for the
allocation before it starts: `/var/run/netns/legion-<allocation>`, with
loopback up, no other interface, and no route. It binds every datapath
listener inside that namespace: the upstream loopback ports, the VIPs, the
allocation's resolver (at `127.42.<b>.1:53` with a VIP block, else at
`127.0.0.53:53`), an **ingress listener** for every declared port, and an
**egress proxy** at `127.0.0.1:1080`. A `container` joins the namespace by path (an OCI
`network` namespace entry). An `exec` task enters it with `setns` before
exec, before dropping privileges. The worker dials out from the host
namespace, so the listeners are the only way out.

The worker's heartbeat creates the namespace ahead of the reconcile that
starts the task. An allocation the manager pushes straight to the worker can
start before any heartbeat lists it (at once, or as soon as its artifacts are
staged), so the start creates the namespace itself when it is missing; the two
paths are serialized and idempotent. The datapath listeners inside it are
bound on the worker's next heartbeat, so for up to one heartbeat interval a
pushed task has a namespace with loopback only: its egress and upstream
connections fail until the listeners exist. A worker that cannot
create namespaces (no `CAP_SYS_ADMIN`, or not Linux) fails the start with
`allocation <id> is network.mode = "gated" but its network namespace could not
be created`, and never falls back to host networking.

Inside, the task gets:

| Env var | Value |
| --- | --- |
| `LEGION_NETWORK_MODE` | `gated` |
| `LEGION_EGRESS_PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and lower-case `http_proxy`, `https_proxy`, `all_proxy` | `http://127.0.0.1:1080` |
| `NO_PROXY`, `no_proxy` | `localhost,127.0.0.0/8,svc.legion` |
| `NODE_USE_ENV_PROXY` | `1` |

Both cases are set because tools disagree: curl ignores upper-case
`HTTP_PROXY` for plain HTTP, and apt reads only lower case.
`NODE_USE_ENV_PROXY=1` makes Node's built-in `fetch` and `http` honor the
variables on Node 22.21 and 24.5 or later. Older Node ignores it, and so has
no route out.

The proxy is an HTTP proxy, and every proxy variable names it as
`http://`. It takes both forms:

- HTTP `CONNECT`, for TLS and other opaque traffic, checked by host and port.
- Absolute-URI plain HTTP, also checked by the request head and forwarded in
  origin form on an upstream connection of its own. A client may keep its
  connection to the proxy open or pipeline requests on it: each request is
  checked on its own, so a request behind an allowed one is never let
  through unchecked.

There is no SOCKS listener: Legion never sets a `socks5://` proxy, and a
client configured for one gets no answer.

Nothing is added to traffic through the proxy. In particular, a
[credential](#credential-injection) is only injected on a declared
`https://` upstream, never into a `CONNECT` tunnel, which the node cannot
read. Point a client that needs one at its upstream, not the proxy.

Every denial is a `403` with `Legion-Denied-By` and a `net.denied` event.
**On a node without the [kernel classifier](#the-kernel-classifier), a tool
that ignores the proxy variables has no route and fails.** That failure is
how the policy is enforced. With the classifier, its `connect()` is captured
transparently and judged the same way.

Three fail-closed rules apply only to gated allocations. Every listener
denies while the allocation's policy has not arrived (`policy-pending`), and
while its [edge-boot](edge-boot.md) `boot` gate is shut (`gate`). The gate
check runs before the policy, so a shut gate denies every destination, even
ones the policy allows. So for gated `container` and `exec` tasks, the `boot`
gate is enforced, unlike the advisory gate in host networking. And a gated
allocation that nothing allows is denied, below.

### Default deny

**Gated means nothing leaves unless a rule says so.** A gated allocation
whose egress no task block and no policy object covers (nothing matched and
nothing inline) denies every destination, on every runtime. The verdict
names its own source, `gated-default`, so the `403`, the `net.denied` event,
and `legion net policy test` all say why:

```console
$ curl -sS https://example.com          # inside the gated task
curl: (56) CONNECT tunnel failed, response 403

$ legion net policy test agents --to https://example.com
✗ deny https://example.com: denied by gated-default — task workspace is gated and nothing allows example.com
  Hint: add "example.com" to network.egress.allow, or apply a policy object; network.egress = { policy = "allow" } allows everything
```

A plain-HTTP request gets the JSON `403` body with
`Legion-Denied-By: gated-default`. The allocation's resolver answers a name
nothing allows with `NXDOMAIN` in one round trip and records the same
denial, while its own `*.svc.legion` names still resolve. Declared
`connect.upstreams` are an explicit allowance and are not affected.
`legion net doctor` warns on the policy layer when a gated task has nothing
covering its egress.

Opening it is one line in the task:

```nickel
network = { mode = "gated", egress = { allow = ["pypi.org", "files.pythonhosted.org"] } },
```

To observe everything and block nothing, which is what a gated allocation
with no policy did before, write it:

```nickel
network = { mode = "gated", egress = { policy = "allow", accessLog = true } },
```

For agent sandboxes, `configs/policies/agent-sandbox-egress.ncl` is a
ready-made `default` policy for `runtimes = ["sandbox"]` that opens the
common read paths: PyPI's index and files, npm's registry, crates.io, Go's
module proxy, and GitHub's clone and download hosts. Read it before you
apply it. Behind HTTPS `CONNECT` the proxy sees a host and a port, not a
method, so a registry it opens for downloads is open for uploads too (npm's
registry takes `npm publish`, `github.com` takes `git push`), and each line
says so. It never applies unless you apply it, and it never lists a model
API: which one your platform calls is yours to add.

```console
legion net policy apply -f configs/policies/agent-sandbox-egress.ncl
```

### Sandboxes

A `sandbox` task with `network.mode = "gated"` boots a microVM with **no
network interface at all**. Its only way out is a virtio-vsock device whose
host end is the node datapath. Before the guest boots, the worker binds the
allocation's egress proxy on the VMM's host socket for guest port 1080, and
its resolver on the socket for port 53. The sockets are
`<alloc dir>/.sandbox/vsock.sock_1080` and `_53`, the naming both
Firecracker and cloud-hypervisor use. Every gated sandbox gets the resolver,
whether or not it declares upstreams, so names resolve in the guest from the
first boot.

Inside the guest, the generated init starts `legion guest-agent` before the
task's command. It relays `127.0.0.1:1080` to host vsock port 1080 and
`127.0.0.1:53/udp` to host port 53 (using DNS-over-TCP framing). So the same
proxy variables and the same `resolv.conf` a gated container gets work
unchanged. The node checks every connection (the [host guard](#the-host-guard)
first, then policy), and the guest has no other way out.

The agent also carries the other direction. For every port the task
advertises, it listens on that vsock port in the guest and relays each
connection to the workload's `127.0.0.1:<port>`. The node datapath opens
those connections after its ingress chain has run (see
[Ingress into a sandbox](#ingress-into-a-sandbox)).

**Upstreams.** A gated sandbox's `connect.upstreams` work as they do in a
container, through the same listener. For each upstream, the agent listens
on `127.0.0.1:<local port>` in the guest (what `LEGION_UPSTREAM_<KEY>`
names) and relays every connection to host vsock port `<local port>`. The
worker binds that upstream's listener on
`<alloc dir>/.sandbox/vsock.sock_<local port>` instead of on the node's
loopback. So balancing, HTTP mode, the egress chain, and the upstream's
`handlers` all apply unchanged, and the listener fails closed while the
allocation's policy is pending. Names work too. The
resolver on vsock port 53 answers `<key>.svc.legion` with the upstream's
VIP, and the agent also listens on `<vip>:<canonical port>` in the guest
(the guest's `lo` holds all of 127/8), relaying to the same vsock port.
In the guest, `LEGION_UPSTREAM_<KEY>` is the guest-side loopback address the
agent relays, `127.0.0.1:<port>`, with `LEGION_UPSTREAM_HOST_<KEY>` and
`LEGION_UPSTREAM_PORT_<KEY>` as its parts, and the VIP name works as for a
container.

```nickel
connect = {
    upstreams = [{ to = "db" }],
},
network = { mode = "gated" },
```

In the guest, `psql -h db.svc.legion -p 5432` and
`psql "postgres://$LEGION_UPSTREAM_DB"` reach the same listener on the
node.

**The guest agent binary.** The worker copies `legion-guest`, a statically
linked build of the agent and the resume handler, into the rootfs as
`/legion/legion`, so it runs on any image, `alpine:latest` and busybox
included. Install it next to the worker binary (or point
`LEGION_GUEST_BINARY` at it in the worker's environment). Without it, the
worker copies itself and logs a warning at start. That fallback needs a
compatible glibc in the image: on a musl image the agent cannot run, init
says so on the console, and the guest has no network, which fails closed.

What is and is not enforced:

- The guest is untrusted throughout. Policy, handlers, identity, and the
  service graph all run on the host side of the vsock. The agent decides
  nothing.
- A gated guest that no policy covers denies everything
  ([Default deny](#default-deny)).
- A gated sandbox cannot also have a `config.network` (a TAP, or `none`):
  submit refuses the pair, `task "workspace" sets network.mode = "gated" and
  config.network = { mode = "tap" }. A gated guest has no NIC`, with a hint
  to drop one of them. It also needs a microVM backend. With `hypervisor =
  "dev"` (a container standing in for a VM), the start fails with that
  reason instead of running unprotected.
- Advertised ports work. The datapath owns each one on the node and dials
  into the guest over the VMM's vsock socket, so every inbound connection
  passes the ingress chain first. A gated guest fails closed on ingress like
  any gated allocation (see
  [Ingress into a sandbox](#ingress-into-a-sandbox)).
- The edge-boot `boot` gate uses the same policy-withheld marker the vsock
  listeners check, so a provisional sandbox has no network until it is
  ratified.
- `legion net doctor` and the UI report the sandbox like any gated
  allocation. `capture_ready` means every host socket is bound: the egress
  proxy, the resolver, and one per declared upstream. An unbound one is
  named: `egress proxy is not bound on the guest's vsock socket <path>`, or
  `upstream db is not bound on the guest's vsock socket <path>`.
- The agent holds three ports in a gated guest. `127.0.0.1:1080` is its
  egress proxy relay, guest vsock port 1100 is its exec listener (each
  advertised port is served on the guest vsock port of the same number),
  and host vsock ports 1080 and 53 are the egress proxy and the resolver.
  Submit refuses an advertised port the guest would bind on 1080 (`task
  "workspace" binds network.ports "socks" on port 1080 in its guest, but in
  a gated sandbox guest 127.0.0.1:1080 is the guest agent's egress proxy`)
  or on 1100 (`... but in a gated sandbox guest the node serves each port on
  the guest vsock port of the same number, and vsock port 1100 is the guest
  agent's exec listener`), and an upstream `port` of 1080 or 53 (`task
  "workspace" connect.upstreams entry "db" sets port 53, but a gated
  sandbox's upstreams are relayed to the node on vsock ports of the same
  number, and 1080 and 53 are the guest's egress proxy and resolver`).
  Dynamic ports never collide with any of them.
- Upstreams reach the guest only on gated networking. A TAP-wired or
  loopback-only sandbox that declares `connect.upstreams` is placed and
  gets the env vars, but they name the guest's own loopback, where nothing
  listens, so every dial is refused.
- Every gated guest uses vsock CID 3. Both VMMs implement vsock over Unix
  sockets in user space, so a CID is private to its guest, and the datapath
  attributes a connection by the socket it arrived on, never by anything the
  guest says.

**Memory checkpoints.** A gated sandbox suspends, resumes, snapshots, and
forks with memory. The host socket path is derived from the allocation, and
every restore moves the guest onto the restoring allocation's own socket
(Firecracker's `vsock_override`, needing 1.16.1 or later, or
cloud-hypervisor's restored `config.json`). The new allocation's listeners
are bound before the restore, so a restored or forked guest's traffic is
attributed to the new allocation and judged by the policy in force at
restore. A checkpoint closes the guest's open connections: the guest agent
closes the relays it holds when it sees the restore, so clients get a prompt
close rather than a hang. Upstreams keep working across a restore. The
restored workload still dials the loopback ports it was captured with, and
the restore tells the agent which local port the new allocation was
assigned for each, so the agent relays to the new allocation's listener
from the first connection. It also starts listening on the new VIPs. The
old VIPs keep relaying too, for a resolver cache that still holds them. See
[suspend-and-resume.md](suspend-and-resume.md#gated-sandboxes).

Current status: the host side (VMM configuration, listeners, the ingress
dial and its handshake, upstream listeners on vsock sockets, status, the
restore-time socket move) and the agent (egress, ingress, and upstream
relays) are implemented and unit-tested over Unix sockets with the VMMs'
naming, and against fake VMMs. This repository's CI has no KVM, so it does
not run an end-to-end boot, restore, inbound connection, or upstream dial
with a real VMM. `auto` resolves to `host` for sandboxes, as it does for
every runtime.
If the guest binary cannot run on the image (the glibc fallback on a musl
image), init reports it on the console and the guest has no network. This
fails closed, and `net doctor` shows the proxy never connecting. UDP inside
the guest and transparent
capture are not implemented.

Requirements and limits:

- Linux only. macOS has no network namespaces, so `gated` is rejected at
  start there with that reason.
- The worker needs `CAP_SYS_ADMIN`. It already does, for `exec` hardening's
  mount namespace and for running the OCI runtime.
- IPv4 loopback only inside the namespace.
- A gated `container` or `exec` task that declares no `connect.upstreams`
  has no resolver in its namespace. Proxy-aware tools work, because the
  proxy resolves the names they send it, but a tool that resolves a name
  itself fails. (A gated `sandbox` always gets its resolver.)
- No raw UDP and no ICMP.
- No inbound traffic except through the datapath's ingress listeners (below).

## The host guard

The host guard is a fixed set of destinations no allocation may reach
through the datapath: its own node, the cloud's link-local services, and
other allocations' listeners. It sits **below** policy. A job's rules and
stored policy objects can't widen it; only the node's operator can, in the
worker config.

| Class | Addresses | Why |
| --- | --- | --- |
| loopback | `127.0.0.0/8`, `::1` | the node's own services: Legion's ports, other allocations' upstreams and VIPs |
| unspecified | `0.0.0.0/8`, `::` | the same services by another name |
| this node | every address on the node's interfaces, re-read every heartbeat | the same services by the node's IP |
| link-local | `169.254.0.0/16`, `fe80::/10` | AWS, GCP, Azure, OCI, and DigitalOcean metadata, ECS task metadata (`169.254.170.2`), the VPC resolver |
| cloud metadata | `fd00:ec2::254`, `100.100.100.200` | AWS IPv6 metadata, Alibaba Cloud metadata |
| `hostGuard.deny` | operator CIDRs | anything else the operator wants treated like metadata |

IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`) are judged as the IPv4
address they map.

**Where it applies.** Every connection the datapath dials to a destination
the allocation named:

- the gated egress proxy, on both paths (HTTP `CONNECT` and absolute-URI
  HTTP);
- external upstreams (`to = "tcp://…"`, `tls://`, `http://`, `https://`), in
  L4 and HTTP mode;
- `proxy` handler targets, on egress and ingress. A handler that points at
  a node-local proxy (`target = "127.0.0.1:15001"`) needs a
  `hostGuard.allow` entry for that port; until then its health reports
  `host-guard: 127.0.0.1 is loopback; allocations may not reach it`.

Service upstreams are not judged: their endpoints come from the service
catalog, which Legion fills, not from the allocation. Neither are `authz`
handler calls, which the worker makes itself with request facts, never task
bytes, and whose answer is a verdict the task never sees.

**Resolve once, judge, dial what was judged.** A hostname is resolved once,
every address it resolves to is judged, and the dial goes only to an address
that passed. Nothing resolves the name again between the verdict and the
connect, so `localhost`, `2130706433`, `0177.0.0.1`, a public name that
resolves to `169.254.169.254`, and a name that rebinds between two lookups
are all refused. A name that resolves to a mix of addresses dials only the
ones that passed. CIDR rules in policy are matched against the same resolved
addresses, so a hostname can't reach a range a CIDR rule denies.

**Order of checks** on every gated listener: the edge-boot `boot` gate, then
the host guard, then `policy-pending`, then the egress chain. A host-guard
denial is final; no policy can turn it into an allow.

**What a task sees** is the ordinary denial shape with source `host-guard`:

```console
$ curl -sS -x $HTTP_PROXY http://169.254.169.254/latest/meta-data/   # inside a gated task
curl: (56) CONNECT tunnel failed, response 403

HTTP/1.1 403 Forbidden
Legion-Denied-By: host-guard
Legion-Denied-Reason: 169.254.169.254 is link-local (cloud metadata); allocations may not reach it
```

An L4 upstream closes the connection instead. Either way the denial is
reported on the next heartbeat as a `net.denied` event with
`source = "host-guard"`, and `legion net policy test` explains it before the
chain:

```console
$ legion net policy test web --to http://instance-data.ec2.internal/
✗ deny host-guard: instance-data.ec2.internal resolves to 169.254.169.254 (link-local); allocations may not reach it
  Hint: the host guard is set on the worker (hostGuard.allow), not in job policy
```

`net policy test` runs on the manager, so it judges the built-in classes
(and resolves names from the manager). A node's own addresses and its
`hostGuard` entries are judged on the worker; a `net.denied` event from the
node is the authority. `legion net doctor` lists the guard as an
informational step for every allocation whose egress proxy is up.

**Operator overrides**, in the worker-type config, for the rare host service
an allocation should reach on purpose:

```nickel
hostGuard = {
  # Opened on purpose: the node exporter, for a metrics-scraping job.
  allow = ["10.0.1.7:9100"],
  # Extra destinations to treat like metadata.
  deny = ["192.0.2.10/32"],
},
```

`allow` takes `ip:port` or `cidr:port` (`[fd00::7]:9100` for IPv6) and
nothing broader, so an override opens one service and not the node. The
built-in classes can't be removed, only punched through one port at a time.
An invalid entry fails manager startup with the entry named. A bare address
in `allow` is refused for the same reason.

**Limits.**

- The datapath guard judges what the datapath dials. A `host`-mode task dials
  the network directly; the [kernel guard](#the-host-guard-in-the-kernel)
  below covers it on Linux nodes that can run it.
- A root `exec` task in `gated` mode can `setns` back into the host
  namespace.
- The guard judges what allocations reach, and the artifact fetches a job
  points the worker at: `http(s)` artifact sources and their redirects are
  judged and pinned, and an `s3://` `endpoint` is judged
  ([Artifacts](artifacts.md#what-the-host-guard-checks)). Git and OCI
  artifact fetches, `authz` webhooks (the operator's), and the worker's own
  calls to the manager are not judged.

### The host guard in the kernel

A `host`-mode task (an `exec` task, a container with `network.mode =
"host"`) never passes through the datapath, so on Linux the worker also
applies a floor to every socket an allocation opens. On a node that can run
the [kernel classifier](#the-kernel-classifier), the classifier's eBPF
programs apply it (`hostGuard = active (ebpf)`), with the same classes,
`EPERM`, and `net.denied` events described here. Elsewhere the worker
installs one `nftables` table, `inet legion_host_guard`. It matches sockets by the
allocation's cgroup leaf (`<cgroupParent>/{share,reserve}/legion-<alloc>`)
in the output hook and drops:

| Destination | Why |
| --- | --- |
| `169.254.0.0/16`, `fe80::/10`, `fd00:ec2::254`, `100.100.100.200` | the cloud metadata service |
| `hostGuard.deny` entries | operator additions |
| Legion's ports on any local address: the worker plane, and the manager's API, exec, and telemetry ports when the manager shares the node | the node's control plane |
| `127.42.0.0/16` and every allocation's upstream loopback ports | other allocations' datapath listeners |

An allocation keeps its own VIP block and upstream ports, and
`hostGuard.allow` entries are accepted first. A dropped `connect()` fails at
once with `EPERM` ("Operation not permitted"), not a timeout. Each allocation
has its own chain with its own counters. The worker reads them every
heartbeat and reports new drops as `net.denied` events with source
`host-guard`, the class as the destination (`cloud metadata`, `a Legion
control port`, `another allocation's datapath`), and `dropped in the kernel`
in the reason, so `legion net doctor` and the event stream name the cause.

The worker owns the table: it replaces it atomically whenever the set of
allocations changes (`nft -f` with an add-delete-add transaction) and needs
no new dependency, only the `nft` binary. Every task gets a cgroup leaf
where the guard runs, even with `resourceEnforcement.enabled = false`: the
leaf then carries no limits, only the label the guard matches.

Whether a node runs it is the `hostGuard` node attribute, shown by
`legion node info`:

```console
$ legion node info wkr-1
Node ID: wkr-1
Worker plane   https://10.0.1.7:4848
Host guard     active (nftables)
```

A node that cannot run it says why, logs a `WARN` at start, and still runs
host-mode tasks, because host mode is the explicit trust opt-in:

```text
Host guard     unavailable (no cgroup v2 unified hierarchy at /sys/fs/cgroup)
```

**What it does not cover.**

- A task that is root on the host with `CAP_NET_ADMIN`, which is any `exec`
  task without a `user`, can delete the table.
- Other daemons on the node's addresses, except Legion's own ports. Host
  mode is host networking; the guard keeps Legion's control plane, the
  metadata service, and other allocations' listeners out of reach, not
  every service an operator chose to run.
- Nodes without the cgroup v2 unified hierarchy (hybrid or v1-only hosts),
  without `nft`, or where the worker is not root.
- macOS. Apple `container` runs each container in its own VM, and `exec`
  tasks there are host processes.

### Cloud metadata

The cloud instance metadata service (`169.254.169.254`, `fd00:ec2::254`,
`100.100.100.200`) serves the instance's role credentials to anyone who can
reach it. The host guard refuses it for every connection the datapath
dials, so a gated task cannot reach it through Legion. A `host`-mode task's
traffic does not pass through Legion; on Linux nodes running the
[kernel guard](#the-host-guard-in-the-kernel) it is dropped there instead.

As defense in depth on AWS, require IMDSv2 and set the hop limit to 1, so a
token request from inside a container's network namespace never gets an
answer even if something routes it:

```console
$ aws ec2 modify-instance-metadata-options --instance-id i-0123456789abcdef0 \
    --http-tokens required --http-put-response-hop-limit 1
```

GCP and Azure require a `Metadata-Flavor: Google` or `Metadata: true`
header, which a browser-style SSRF can't set but a task can. Grant each
node's instance role only what the worker itself needs.

## The kernel classifier

On a Linux node that can run it, one fixed set of Legion-owned eBPF
programs decides every flow a host-mode task opens, at `connect()`. Users
write the same rules they write today; nothing in a job changes.

```
task: connect(151.101.0.223:443)
  │
  ▼  cgroup programs on the worker's cgroup parent
  ├─ its own datapath (VIP block, upstream ports) ───► pass
  ├─ host guard ─────────────────────────────────────► EPERM (final)
  ├─ port 53 ────────────────────────────────────────► the allocation's resolver
  ├─ the compiled egress chain ──────────────────────► pass | EPERM | redirect
  └─ redirect ─► capture listener: SNI/Host, then the egress proxy's judge
```

- **Allowed flows that nothing reads connect directly.** No proxy hop: a
  few map lookups at `connect()`. Denied flows fail at once with `EPERM`
  ("Operation not permitted"), and nothing is sent.
- **Host-mode egress chains are enforced.** In host mode, a chain used to
  apply only to traffic that went through an upstream or the allocation's
  resolver. On a node with the classifier, it applies to every TCP
  `connect()`, every connected or unconnected UDP send, TCP Fast Open, and
  `io_uring` connects. A task that dialed past its own chain now gets
  `EPERM`.
- **Hostnames are learned from the resolver.** The kernel sees addresses,
  and rules name hosts. Every port-53 query a governed task sends goes to
  its allocation's own resolver, whatever `resolv.conf` says, and the reply
  reads as if it came from the nameserver the task asked. When the resolver
  answers a name the chain allows, the addresses it returns satisfy that
  name's host rules in the kernel for the record's TTL (at least five
  minutes, so a client that caches longer is not cut off mid-use). A
  host-mode task with an egress chain gets a VIP block on Linux nodes for
  that resolver, upstreams or not.
- **What only netd can decide is redirected**, to a capture listener on the
  node's loopback: an address no answer taught the kernel under a chain with
  host rules (a cached answer, `/etc/hosts`, a hard-coded IP), a rule that
  needs the request head, and a chain with handlers or an HTTP access log.
  The listener recovers the original destination, reads the TLS SNI or the
  HTTP `Host`, and allows a named flow only if the address the task dialed is
  one of that name's addresses, so a client that lies in its SNI gets
  nothing (`Legion-Denied-By: capture/name` in the event). Then the flow is
  judged, its hops run, and it is dialed and spliced exactly as a `CONNECT`
  to the gated proxy is; plaintext HTTP under an HTTP chain is served request
  by request. A flow with no name (a protocol where the server speaks first)
  is judged by its address alone.
- **Gated tasks never pass.** Their namespace is the boundary. The
  classifier only makes capture **transparent**: a connect from a gated
  `exec` or `container` task is redirected to a capture listener inside its
  namespace (`127.0.0.1:1081`, beside its proxy), so a tool that ignores
  `HTTPS_PROXY` still gets out under policy. The proxy variables stay set.
- **Raw and packet sockets are refused** to governed tasks, so raw IP cannot
  slip past the rest.
- **Fail-static across restarts.** With bpffs mounted at `/sys/fs/bpf`, the
  programs and maps are pinned under `/sys/fs/bpf/legion/`. A worker that
  restarts re-opens them, attaches fresh programs, and drops the old ones;
  in between, the kernel keeps enforcing the last policy it was given, and
  allowed flows that never entered netd are untouched. Without bpffs, the
  classifier stops enforcing while the worker is down, and the worker logs a
  `WARN` at start.

Whether a node runs it is the `netKernel` node attribute, and `hostGuard`
says which mechanism guards the host:

```console
$ legion node info wkr-1
Host guard     active (ebpf)
Net kernel     active (ebpf: cgroup sock_addr, sock_create, sockops, cgroup_skb, ringbuf)
```

The probe runs once at start and checks features, not kernel versions
(distribution kernels backport): the programs must load and attach to a
cgroup. A node that cannot run the classifier says why, keeps the
`nftables` guard, and treats host-mode egress as cooperative, exactly as
before:

```text
Net kernel     unavailable (no cgroup v2 unified hierarchy at /sys/fs/cgroup)
```

Each allocation's datapath status (`legion net doctor`, `NetAllocStatus.kernel`)
reports whether the kernel enforces its chain, whether capture is on, its
compiled rule count, and how many connections the kernel passed,
redirected, and refused since the last heartbeat. Kernel counters feed the
`legion_net_*` connection metrics, and bytes the kernel moved count as
activity for the [idle policy](suspend-and-resume.md#idle-policy),
so pass flows are not invisible.

**Limits.**

- Up to 64 connect-time rules and 64 distinct host names per chain. A chain
  beyond either keeps today's behavior (netd judges what goes through it),
  and the allocation's status says why.
- A root `exec` task with `CAP_SYS_ADMIN` or `CAP_BPF` can detach the
  programs from its cgroup, as it can delete the `nftables` table. Governed
  tasks must not hold `CAP_NET_RAW`.
- DNS over HTTPS skips the resolver. Its addresses are never learned, so
  those flows are redirected and judged by their SNI, which the policy has to
  allow. A CDN address that an allowed name and a denied name share can be
  reached through the allowed name, as with any egress control based on SNI
  or address.
- The resolver and the capture listener are IPv4: an IPv6 flow is passed or
  refused in the kernel, never redirected, and an IPv6 address a resolver
  returned is learned like an IPv4 one.
- A sandbox's sockets live in its guest kernel, which the classifier never
  sees: a gated sandbox's egress is judged by netd per vsock stream, as
  before. macOS has no classifier.
- UDP other than DNS is passed or refused in the kernel, never captured.
- A body-reading hop (`dlp`) on a captured flow runs only on plaintext HTTP
  under an HTTP chain; a TLS flow it cannot read is spliced after its SNI is
  judged.

## The datapath across worker restarts

Tasks survive a worker restart, because the worker re-adopts their processes
from its state file. Their network survives too. After every successful
heartbeat that changed it, the worker writes what the datapath was last told
to `netd_state.json`, next to `worker_state.json`. That includes the desired
set, the endpoint feed, the policy assignments, and their bodies. A
restarting worker replays this file before its first heartbeat.

Namespaces persist on their own (bind mounts under `/var/run/netns`). For
the allocations that survived, the replay re-binds every listener inside and
next to them: upstream ports, VIPs, resolvers, egress proxies, and ingress
ports, plus identities and policy bodies. Because this state is on disk, a
node that restarts while the manager is down comes back with the same
datapath it had.

Connections in flight during the restart drop, because the listeners are new
sockets. There is also a gap while the process starts. A separate
`legion netd` process with listener hand-off (`SCM_RIGHTS`) and zero dropped
connections is not implemented. This replay fixes the main problem: a gated
sandbox whose worker is upgraded no longer loses its network until the
manager is reachable again.

A gated allocation whose capture is not ready (a missing namespace or
listener, or an unbound vsock socket) is reported **unhealthy** with
`net_capture_unavailable: <problems>` on the heartbeat. A rollout does not
count it as up, and `legion net doctor` names the cause.

## Ingress

Declare an `ingress` chain on a task, impose one with a `require` policy
object, put the service on the [mesh](#the-mesh), or give a port its own
[`tls`](#tls-on-ports), and the datapath fronts every port the task
advertises:

```nickel
network = {
    ports = [{ name = "http", static = 8080 }],
    ingress = {
        protocol = "http",
        policy = "deny",
        rules = [
            { when = { identity = "auto:web:*" }, action = "allow" },     # <region>:<job>:<group>:<task>
            { when = { cidr = "127.0.0.0/8" }, action = "allow", reason = "health checks" },
        ],
        accessLog = true,
    },
},
```

- **The datapath owns the advertised port.** The node's datapath binds
  `LEGION_PORT_HTTP` as consumers see it (the port in the catalog and in
  `LEGION_UPSTREAM_*` feeds) on the host. The task gets a **private twin**
  as its own `LEGION_PORT_HTTP` (a container's port publish uses the twin
  too), so nothing in the task changes. On the host the twin comes from the
  same dynamic range. Inside a gated namespace, which has its own ports, the
  twin is the port's `to`, or the advertised port number itself when `to` is
  unset, so an image that listens on a fixed port (`{ name = "pg", to =
  5432 }`) needs no change. The task cannot take the advertised port back, because the bind
  fails with the address already in use. This makes ingress enforceable on
  every runtime and platform, not only on gated Linux.
- **Every accepted connection is checked** against the ingress chain using
  the peer's address (`cidr`, `port`) and, on the mesh port or a mutual
  port, the peer's verified identity (`identity`). In HTTP mode, the request head is
  checked too, with the same request rules, `dlp`, structured `403`s, and
  access log as egress. A denial closes the connection and records
  `net.denied` with `direction = ingress`.
- **Allowed connections are spliced** to the twin port (inside the
  allocation's namespace when it is gated, on the host otherwise), or
  proxied per request in HTTP mode.
- **Gated allocations fail closed** on ingress the same way as on egress.
  While the policy is pending or the boot gate is shut, nothing is accepted.
- **Every gated allocation gets ingress listeners**, chain or not, because
  its namespace has no other way in. Without a chain the listener accepts
  every peer, so a gated container's service is reachable from other nodes,
  from its consumers, and from its health checks exactly as on host
  networking.
- **Health checks go through the chain**, because the checker dials the
  advertised port. A chain that denies everything also denies the checker.
  Allow `cidr = "127.0.0.0/8"` for a node-local checker. `legion net doctor`
  warns when a chain would deny its own checks.

Limits: TCP only. The task sees the datapath as the peer. Legion does not
pass the original peer to the task (no PROXY header and no forwarding
headers). The original peer appears in the access log and in `net.denied`
events, and an ingress `proxy` hop receives it in the PROXY v2 header. There
is one listener per advertised port per allocation. The cluster edge and per-tag routing on ingress are not part of
this feature.

### Ingress into a sandbox

A sandbox guest has its own network stack, so a port it listens on is not
on the node's loopback. The datapath fronts **every** port a sandbox
advertises, with or without an ingress chain: it owns `<address>:<host
port>` on the node, as it does for any fronted port, and dials into the
guest for each connection it allows.

| The sandbox's network | How the datapath reaches the guest |
| --- | --- |
| `network.mode = "gated"` (no NIC) | Through the VMM's vsock socket for the allocation, `<alloc dir>/.sandbox/vsock.sock`, with the host-initiated handshake both Firecracker and cloud-hypervisor implement: it writes `CONNECT <port>\n`, expects `OK <n>\n`, then splices. Anything else (a closed stream, an error line, silence for two seconds) is a failed dial. In the guest, the agent listens on vsock port `<port>` and relays to `127.0.0.1:<port>`. |
| `config.network = { mode = "tap", ... }` | TCP to the guest's leased bridge address, `<guest address>:<port>`. |
| Neither (`config.network = { mode = "none" }`, or no network config while `auto` is `host`) | Nothing can reach the guest's loopback, so submit refuses `network.ports`. |

```nickel
network = {
    mode = "gated",
    ports = [{ name = "http" }],
    egress = { allow = ["pypi.org"] },
},
```

The workload binds `$LEGION_PORT_HTTP` as on every runtime: a gated guest
on loopback or every address (`0.0.0.0`), a TAP guest on its bridge address
or every address. That is the advertised port's own number when there is no
ingress chain (the guest's stack is its own, so it does not collide with
the datapath's listener on the node), and the private twin when there is
one. The vsock port is the same number, so one number names both ends.

Everything else is unchanged: the ingress chain, TLS termination, the
access log, `net.denied`, the activity ledger behind the `connections`
idle signal, and `legion net doctor`. A gated guest fails closed on
ingress like any gated allocation: while its policy is pending or its
boot gate is shut, nothing is accepted.

**Across a memory checkpoint.** A suspended, resumed, or forked sandbox
keeps serving its ports through the restoring allocation's listeners. The
restored workload still listens on the ports it was captured with, even
when the new placement assigned different numbers. The checkpoint records
those ports, and the datapath dials them. A port the new placement adds is
added to the guest agent by the restore's identity. A TAP guest is dialed
at the address the restore leased it. Connections open at capture are
closed, as on egress.

Failure modes:

- **Nothing listens in the guest yet.** The agent closes the stream at
  once (a gated guest), or the dial is refused (a TAP guest). The client
  sees its connection close, and a health check through the port fails
  until the workload is up.
- **The guest agent is not running** (the glibc fallback on a musl image).
  The VMM closes the handshake, so every inbound connection closes. The
  guest has no egress either, so this fails closed.
- **A TAP guest has no lease yet.** The dial fails with `the sandbox guest
  has no address leased in <dir> yet`.

Limits:

- An ingress `proxy` hop hands the connection to your proxy, which cannot
  reach a port inside a sandbox guest, as with a gated task's private port.
- In a gated guest, `127.0.0.1:1080` belongs to the agent's egress proxy
  relay and guest vsock port 1100 to its exec listener, so the workload
  cannot serve on port 1080 or 1100, and submit refuses an advertised port
  that would bind either.
- Firecracker 1.17 fixes host-initiated vsock connections that stop
  receiving after a VM is paused and resumed in place (Firecracker #6100),
  which a live `snapshot create --memory` or a fork does to the origin.
  Legion does not require 1.17 yet. Run 1.17 or later for gated sandboxes
  that serve ports.
- CI has no KVM, so the vsock path is tested against a fake VMM and a fake
  guest, not a real microVM.

**Rollouts.** A task with a `service` block drains before it stops. It
leaves the catalog, keeps serving for one heartbeat interval (or
[`stop.drain`](graceful-stop.md)), and only then begins its normal stop. A
consumer dialing it through an upstream sees no `connection refused` while a
rollout replaces it.

## Service graph

The service graph shows who calls whom, with RED metrics (rate, errors,
duration). It is a main reason to adopt a mesh, and the first thing on the
UI's Network page.

Every calling datapath (an upstream listener, the gated egress proxy, an
HTTP-mode request) records what it saw per (upstream, endpoint tag) since
the last heartbeat: connections or requests, errors (5xx, or no endpoint
reached), denials, latency, and bytes. The samples ride on the heartbeat
(`HeartbeatRequest.net_edges`, restored on a failed heartbeat). The manager
rolls them into per-minute buckets per (caller job, caller task, service,
tag) and keeps 15 minutes in memory. An edge appears within one heartbeat of
the first call.

```bash
legion service edges                 # the whole graph, last 5 minutes
legion service edges api --window 15m
```

```
CALLER          SERVICE   TAG     STATE                 RPS    ERRORS     DENIED  LATENCY avg/max  LAST SEEN
web/server      api       -       healthy (declared)    12.40  0 (0.0%)   0       18ms/240ms       3s ago
web/server      api       canary  some errors           1.30   4 (1.0%)   0       22ms/910ms       3s ago
agent-7/sandbox api       -       denials               0.00   0 (0.0%)   17      -                9s ago
batch/loader    db        -       declared, silent      0.00   0 (0.0%)   0       -                never
```

Each edge shows whether the caller's latest job config **declares** the
upstream, whether the service's `allow` intentions **admit** the caller, and
what was **observed**: requests per second, error rate, denials, average and
max latency (HTTP mode), and bytes.

- A declared upstream that has never been called shows as
  `declared, silent`.
- A caller that calls without declaring (through the gated proxy, for
  example) shows without the `declared` mark.
- An edge into a service whose `allow` list excludes the caller shows as
  `not allowed`.

Tags come from the endpoint the call was balanced to, so a `split` canary
gets its own row, with per-tag RED side by side.

The UI's **Network** page draws the graph above the same table. Callers are
on the left and services on the right. Line width shows request rate. Lines
are dashed for silent edges and red for denials, errors above 5%, or a
disallowed caller. A **Service** page lists that service's callers.
`ListServiceEdges` and the MCP tool `legion_service_edges` return the same
edges (`job:read`, like the catalog).

The graph does not include:

- Ingress-side attribution. The caller records edges, so a client outside
  Legion dialing an advertised port is not an edge. Its denials are still
  events.
- Traffic that bypasses the datapath in `host` mode.
- Anything older than 15 minutes. The graph lives in memory on the manager.
  A failover starts it empty, and the next heartbeats refill it.

Through the gated proxy, an external host appears as a node of the graph
(the service is the destination host). This is capped at 4,096 distinct
edges per node between heartbeats.

## CLI

```bash
legion net policy apply -f <file.ncl|file.json> [--dry-run]   # create or replace
legion net policy list                                          # merge order
legion net policy describe <name>
legion net policy delete <name>
legion net policy show <job> [--task <task>] [--pool <pool>]    # the effective policy
legion net policy test <job> --to <dest> [--method M --path P]  # dry-run a verdict
legion net policy test <job> --from <peer> [--identity <sub>]
legion net doctor <job> [--task <task>]                         # policy → mode → datapath → handlers → tls
legion net handler test <url|host:port>                         # probe an authz webhook or a proxy
legion net plugin new|test|apply|list|describe|status|delete     # WASM plugins (docs/network-plugins.md)
legion net ca show | rotate                                     # the mesh CA
```

`net doctor` walks through a task's network the way `service doctor` walks
through a service. It shows:

- the resolved policy and where each chain came from
- whether the effective mode enforces it (`gated`) or relies on cooperation
  (`host`, with a hint to gate it)
- for each running allocation, what the node reports: the namespace and
  every listener bound, whether the resolver and egress proxy are serving,
  the policy hash the datapath enforces compared to the one the manager
  computes, `policy-pending` or a shut boot gate (fail-closed while gated),
  and any problem the node has (for example, an ingress listener that could
  not bind)

It stops at the first failure and gives a hint for fixing it. Nodes report
this status on every heartbeat, and the same lines appear in the UI's task
Network section.

Every subcommand takes `--json`/`-j` and emits the standard envelope.
`apply --dry-run` validates the policy and lists the running allocations
whose effective policy would change. `test` answers `allow`, `deny` (with
the deciding rule and its reason), or `defer`, which means a request-time
rule would decide and no method or path was given.

`show`, `test`, `doctor`, `handler test`, and `ca show` are also MCP tools
(`legion_net_policy_show`, `legion_net_policy_test`, `legion_net_doctor`,
`legion_net_handler_test`, `legion_net_ca_show`). `apply` and `ca rotate`
are mutating MCP tools (`legion_net_policy_apply`, `legion_net_ca_rotate`).
An agent can ask "is that host blocked?" instead of watching a connection
hang.

## Observability

- **Metrics** (worker registry, bounded labels): `legion_net_conns_total{direction,protocol,verdict}`,
  `legion_net_denied_total{direction}`, `legion_net_bytes_total{direction}`,
  `legion_net_handler_total{handler,outcome}` (authz/proxy calls by
  allow|deny|proxy|error), `legion_net_handler_seconds` (webhook latency,
  cache misses only), and for [plugins](network-plugins.md#status-and-diagnostics)
  `legion_net_plugin_calls_total{plugin,stage,outcome}`,
  `legion_net_plugin_callback_seconds`, and
  `legion_net_plugin_metric{plugin,scope,name}`.
  Attribution is never a metric label. The registry caps label
  combinations, and a per-user label would silently break the series.
- **Events**: `net.denied` (warn) with the destination, the deciding rule,
  the reason, the peer identity, and the task's `network.labels`.
  `net.policy_applied` and `net.policy_deleted` (info), and
  `net.plugin_applied` and `net.plugin_deleted` (info).
- **Access log** (`accessLog = true` on the chain): one NDJSON line
  per request in HTTP mode, at `<worker logs>/<allocation>/net-access.log`.
  L4 connections are counted in metrics, not logged one by one.
- **Datapath status**: every heartbeat carries the node's view of each
  allocation's datapath: mode, capture readiness, the enforced policy hash,
  bound upstream and ingress listeners, resolver and proxy state, the boot
  gate, and problems. `legion net policy show` returns it as
  `node_status_json`, `legion net doctor` interprets it, and the UI's task
  Network section shows it under **Datapath**.

## ACLs

The `net` resource controls access:

- `read` lists policies, shows and tests a job's effective policy (scoped by
  job name), and reads the mesh CA certificate.
- `write` applies and deletes policies. It is scoped by `pools`, which is
  matched against the policy's own `selector.nodePools`, so a pool's admin
  can set policy for their pool without cluster-wide write access.

A cluster-wide policy (no pools in its selector) and `ca rotate` need
unscoped `net:write`. [Plugin objects](network-plugins.md#acls) follow the
same rule on their own `nodePools`, and a `usableBy = "jobs"` plugin needs
unscoped `net:write`. `IssueNodeCertificate` is worker-plane, and a node may
only mint its own certificate.

Naming a KV key in a job needs `kv:read` on it, because the datapath hands
the value out: `ports[].tls.cert`, `ports[].tls.clients.ca`, and
`connect.upstreams[].ca`
([Certificates and CAs from KV](#certificates-and-cas-from-kv)), and
`connect.upstreams[].credentials` ([Credential injection](#credential-injection)).
The same rule covers every other place a job names a key
([KV references in a job](auth-and-acls.md#kv-references-in-a-job)).
`FetchDatapathMaterial`, the one call a node's datapath fetches everything
with, is worker-plane, and a node only receives values that allocations
placed on it declare: the request names an allocation and what it needs
(an upstream and a header, say), never a KV path, and the manager derives the
path from the allocation's own declaration.

## Current scope and limitations

What ships now is the **policy and upstream layer on the connect datapath**.
This section lists what that covers and what it does not.

- **Rules and handlers are enforced on the datapath.** That includes
  connect-time rules on every dial, request-time rules (path, method,
  header), `dlp`, the `http` traffic block, and the external `authz` and
  `proxy` hops on every path (upstream splices, HTTP mode, the gated egress
  proxy, ingress).
- **Egress in `host` mode is enforced only where the kernel classifier
  runs, and `gated` mode is not yet a full boundary.** On a Linux node with
  the [kernel classifier](#the-kernel-classifier), a host-mode `exec` or
  `container` task's egress chain is enforced at `connect()`. Elsewhere, in
  `host` mode, policy applies to traffic that goes through an upstream and
  to names resolved through the allocation's resolver, and a process that
  dials a raw address bypasses it. `mode = "gated"` (Linux,
  `container` and `exec`) closes that gap, because the namespace has no
  route except Legion's listeners, and every destination those listeners
  dial is judged by the [host guard](#the-host-guard) before policy. A root
  `exec` task can still leave the namespace. Without the classifier, a
  host-mode task's policy stays cooperative, but the
  [kernel guard](#the-host-guard-in-the-kernel) keeps it off the node's
  Legion ports, the metadata service, and other allocations' listeners where
  it runs. On macOS, egress stays cooperative, and
  there are no VIPs, per-allocation resolvers, or gated namespaces. A gated
  `sandbox` is captured over vsock, upstreams included. That path is
  implemented and unit-tested but not exercised on a real VMM in CI. A
  TAP-wired or loopback-only sandbox cannot reach its upstreams. `auto`
  resolves to `gated` for containers on Linux nodes and to `host` for
  everything else, so set `gated` explicitly for an `exec` task.
- **Ingress is enforced everywhere it applies.** The datapath owns the
  advertised port and the task binds a private twin, on every runtime and
  platform. A gated or TAP-wired sandbox's ports are always fronted, over
  vsock or at the guest's bridge address. TCP only. The task sees the
  datapath as the peer, not the original client.
- **TLS**: a port terminates TLS (mutual with `clients`) with your files or
  a certificate held in KV (swapped live on renewal), and a mesh service's
  port with the node certificate.
  Legion stores and serves certificates but does not issue public ones: a
  renewal job (or any ACME client) writes them to KV. The mesh is node-to-node mTLS with attributed
  workload identity, never per-workload certificates. There is no kTLS fast
  path (a TLS connection, mesh hops included, is copied in user space rather
  than spliced), no revocation
  beyond the 24-hour lifetime, and an ingress `proxy` hop cannot reach a
  gated task's private port or a port inside a sandbox guest.
- **No HTTP/2 or HTTP/3** (`protocol = "h2"` is rejected with that reason),
  no UDP, and no TLS interception. Custom per-request logic runs as
  [WASM plugins](network-plugins.md); their own limits are listed there. Legion will not add TLS
  interception. Use external upstreams when policy needs to see the body,
  or when the node should add a credential the task never holds
  ([Credential injection](#credential-injection)): both work because the
  task speaks plaintext to the node and the node opens TLS itself.
- **Net policies are cluster objects.** `selector.namespaces` matches each
  task's real [namespace](namespaces.md), but writing a policy needs a
  cluster-wide `net:write`: a namespace's tenants cannot author policy for
  their own namespace.
