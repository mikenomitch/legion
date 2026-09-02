# Network Handlers: Egress and Ingress Policy, Upstreams, and Middleware

Legion's node datapath — the loopback forwarder behind `connect.upstreams` —
can decide, observe, and (where you ask) transform the traffic that crosses a
task's boundary. This page is the reference for the three concepts that make
that possible and the surfaces they are used through:

- **Policy** — declarative rules that say what a task may reach and who may
  reach it. Written inline on a task, or stored as a **policy object** that
  applies to every task a selector matches (a node pool, a runtime, a job
  glob, a label) and changes live, without restarting anything.
- **Upstreams** — where a task's egress goes. A catalog service, as before,
  or an **external destination** to which Legion originates TLS, so the task
  speaks plaintext to a loopback port and the body is visible to policy
  without anyone's TLS being intercepted.
- **Handlers** — code on the path, only when policy is not enough: a webhook
  that returns a verdict, or any proxy in any language.

The design and the phases are in
[`internal_docs/plans/service-mesh-plan.md`](../internal_docs/plans/service-mesh-plan.md)
and [`network-handlers-plan.md`](../internal_docs/plans/network-handlers-plan.md).
This page describes **what ships today** and says plainly what does not
yet; see [Current scope](#current-scope-and-limitations).

## Quick start

Deny everything but two registries, on the connect datapath, for one task:

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

Or once, for every sandbox on a pool, with nothing in the job:

```bash
legion net policy apply -f configs/policies/sandbox-egress.ncl
legion net policy show agent              # what the task actually gets, with sources
legion net policy test agent --to https://api.github.com/user   # would this be allowed?
```

## What the datapath does today

Every connection a task makes through an upstream — `LEGION_UPSTREAM_<KEY>`,
a loopback port the node owns — passes through the node's forwarder. Before
it dials, the forwarder evaluates the task's **effective egress policy** with
what is known at connect time: the destination host (the service name, or
an external upstream's host), the port, and for service upstreams the
endpoint about to be dialed. A denial closes the connection without dialing,
increments `legion_net_denied_total`, and is reported on the next heartbeat
as a `net.denied` event naming the rule. An allowed connection is spliced;
nothing is parsed.

That is the whole per-connection cost of a connect-time policy: one rule
walk, no bytes inspected. Rules that need the HTTP request (a path, a
method, a header) are the HTTP phase's work and are validated today, run
tomorrow — see [Current scope](#current-scope-and-limitations).

## Policy

### The inline form

`network.egress` and `network.ingress` on a task. `allow` is sugar; the full
form has `rules`, `handlers`, and `tls`.

```nickel
network = {
    labels = { user = "alice", tier = "free" },   # attribution; selectors can match on these
    egress = {
        protocol = "tcp",       # tcp (default) | http — never inferred from the rules
        policy = "deny",        # fallthrough: allow | deny (deny when `allow` is used)
        onError = "deny",       # what a failed handler means: deny | allow
        accessLog = false,
        allow = ["*.pypi.org", "api.github.com:443", "10.0.0.0/8:5432"],
        rules = [
            { match = { host = "telemetry.example.com" }, action = "allow" },
            { match = { cidr = "169.254.0.0/16" }, action = "deny", reason = "no metadata service" },
        ],
        handlers = [],
    },
},
```

**Rules match on** `host` (a glob: `*` matches one or more labels at a label
boundary, so `*.pypi.org` matches `files.pypi.org` and never `pypi.org`;
bare `*` matches everything), `port`, `cidr` (the resolved destination or the
ingress peer), `identity` (the verified peer's workload identity, ingress),
and — with `protocol = "http"` — `path` (`*` within a segment, `**` across),
`method`, and `header` (name → value glob). Every present field must match;
first match wins; `policy` is the fallthrough. A `deny` rule's `reason` is
returned to the workload and recorded on the event.

**`allow` entries** are `host`, `host:port`, `cidr`, `cidr:port`, an IP, or —
only with `protocol = "http"` — `host/path`. A path in a `tcp` chain is
rejected at submit with the fix:

```
egress.allow[1] "api.github.com/repos/**" matches on a path, but
egress.protocol is "tcp". Set protocol = "http" to match on paths, or
drop the path to match the host only.
```

`protocol` is never inferred. A rule set that silently became L7 because
someone typed a slash would be a policy you cannot review by reading, and
about twice as expensive per byte.

### Policy objects

A stored policy is the same block plus a **selector** and an **enforcement**
mode. It lives in the cluster store (raft-replicated), is edited with
`legion net policy`, and applies to every matching task on its next
heartbeat — no restart, no rollout, no job edit.

```nickel
# configs/policies/sandbox-egress.ncl
{
    name = "sandbox-egress",
    priority = 100,
    selector = {
        nodePools = ["untrusted"],       # node.pool globs
        namespaces = ["default"],        # every job is in `default` until namespaces land
        runtimes = ["sandbox"],
        jobs = ["agent-*"],              # job id globs
        labels = { tier = "free" },      # task network.labels that must all match
    },
    enforcement = "require",             # require | default
    mode = "gated",                      # optional network.mode to impose
    egress = { protocol = "http", policy = "deny", allow = ["*.pypi.org", "api.github.com/repos/**"] },
}
```

`selector = {}` matches every task on the cluster. Every list is an OR, the
fields are an AND.

| `enforcement` | Applied when | A job author may |
| --- | --- | --- |
| `default` | the task declares no chain of its own for that direction | replace it entirely by declaring one |
| `require` | always, **ahead of** the task's own block | append rules and handlers, add denials; never remove or widen |

**The merge is deterministic and shows its work.** Every matching `require`
policy applies in `priority` order (ties by name), then exactly one of: the
task's own block, or the highest-priority matching `default` policy. A
required `deny` fallthrough is never widened by anything after it. Two
`default` policies that would tie for the same task — equal priority,
overlapping selectors — are rejected at `apply`, so the winner is never
decided at runtime. In the merged result every rule and handler carries a
`source`: `policy/<name>#<index>` or `task#<index>`.

`legion net policy show <job>` prints exactly that merged result; the node
datapath runs the same function on the same inputs.

### How policy reaches a node

The heartbeat reply carries one content hash per allocation. A node fetches
a body it has not seen once and caches it, so a thousand identical sandboxes
cost one fetch. Bodies are swapped only after a successful heartbeat and the
last known good body is kept through a manager outage — the same fail-static
contract as the endpoint feed. A cluster with no stored policy and no task
declaring one pays nothing per heartbeat.

## Upstreams

`connect.upstreams` entries are either a catalog **service** or an
**external destination**:

```nickel
connect = {
    upstreams = [
        { service = "db" },                                   # as before
        { name = "openai", to = "https://api.openai.com" },  # external
    ],
},
```

An external upstream is `name` + `to`; the task gets `LEGION_UPSTREAM_<NAME>`
(`127.0.0.1:<port>`) exactly like a service upstream. The scheme of `to`
decides two things:

| `to` | Legion originates TLS | HTTP framing |
| --- | --- | --- |
| `tcp://host:port` | no | no |
| `tls://host:port` | yes | no |
| `http://host[:port]` | no | yes (HTTP phase) |
| `https://host[:port]` | yes | yes (HTTP phase) |

With `https://`, the task speaks **plaintext HTTP to loopback** and the
node's forwarder dials the destination over TLS (verified against the
Mozilla roots). That is how an LLM API call's body becomes visible to policy
and handlers **without anyone's TLS being intercepted**: point the client at
the upstream —

```sh
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
```

— and Legion never holds a certificate authority that can impersonate the
public internet. `to` must not carry a path or query; the task chooses paths.

The `http` block on an upstream (`timeout`, `retries`, `split`, `outlier`)
and per-upstream `handlers` are validated at submit today and run in the
HTTP phase; see below.

## Handlers

Handlers are code on the path and appear only when declared. Their names are
validated at submit; the datapath runs them from the HTTP phase onward:

| `handler` | What it does | Sees bytes |
| --- | --- | --- |
| `authz` | POSTs the connection's facts to `url` and caches the verdict for `cache` (at most 5m) | no |
| `proxy` | hands the flow to any proxy — a catalog `service`, a sidecar's `port` label, or a `node`-local service — with a PROXY protocol v2 header (TCP) or `Legion-*` headers (HTTP) | yes |
| `log` | access log for this hop | as declared |
| `dlp` | rejects HTTP bodies matching `config.patterns` up to `config.maxBytes` (needs `protocol = "http"`) | yes |

Every hop has a `timeout` (default `2s`) and an `onError` (`deny` by
default). There is no syntax for an unbounded handler.

## `network.mode`

| `mode` | Meaning | Default for |
| --- | --- | --- |
| `host` | host networking; policy is cooperative (proxy env vars) | `container`, `exec` |
| `gated` | the namespace holds loopback and the datapath only — unbypassable (gated phase) | `sandbox` |
| `auto` | `gated` where the runtime and platform can, else `host` | — |
| `bridge`, `cni/<name>` | reserved for the datapath plugin; rejected at submit | — |

The value is validated today and read from the gated phase on. A stored
`require` policy can impose it for a pool.

## CLI

```bash
legion net policy apply -f <file.ncl|file.json> [--dry-run]   # create or replace
legion net policy list                                          # merge order
legion net policy describe <name>
legion net policy delete <name>
legion net policy show <job> [--task <task>] [--pool <pool>]    # the effective policy
legion net policy test <job> --to <dest> [--method M --path P]  # dry-run a verdict
legion net policy test <job> --from <peer> [--identity <sub>]
```

Every subcommand takes `--json`/`-j` and emits the standard envelope. `apply
--dry-run` validates and lists the running allocations whose effective policy
would change. `test` answers `allow`, `deny` (with the deciding rule and its
reason), or `defer` — a request-time rule would decide and no method/path
was given.

`show` and `test` are also MCP tools (`legion_net_policy_show`,
`legion_net_policy_test`, read-only) and `apply` is a mutating one
(`legion_net_policy_apply`), so an agent can ask "is that host blocked?"
instead of watching a connection hang.

## Observability

- **Metrics** (worker registry, bounded labels): `legion_net_conns_total{direction,protocol,verdict}`,
  `legion_net_denied_total{direction}`, `legion_net_bytes_total{direction}`.
  Attribution is never a metric label — the registry caps label combinations
  and a per-user label would silently destroy the series.
- **Events**: `net.denied` (warn) with the destination, the deciding rule,
  the reason, the peer identity, and the task's `network.labels`;
  `net.policy_applied` and `net.policy_deleted` (info).
- **Access log** (`accessLog = true`): from the HTTP phase, one NDJSON line
  per connection or request in the allocation's log directory.

## ACLs

A new resource, `net`: `read` lists policies and shows/tests a job's
effective policy (scoped by job name); `write` applies and deletes policies,
scoped by `pools`, which is matched against the policy's own
`selector.nodePools` — so a pool's admin can set posture for their pool
without cluster-wide write. A cluster-wide policy (no pools in its
selector) needs unscoped `net:write`.

## Current scope and limitations

What ships now is the **policy and upstream layer on the connect datapath**;
the phases below are laid out in the plan and named here so nothing reads as
more than it is.

- **Connect-time rules only are enforced today** (host, port, cidr,
  identity) — on every `connect.upstreams` dial, service or external. Rules
  on path, method, and header, the `dlp` handler, the `http` traffic block,
  and per-upstream `handlers` are **validated at submit and run from the HTTP
  phase**.
- **Egress is cooperative for `container` and `exec`.** Policy applies to
  traffic that goes through an upstream. A process that dials a raw address
  bypasses it; the gated phase closes that on Linux with `mode = "gated"`
  (an empty network namespace holding loopback and the datapath). On macOS
  egress stays cooperative.
- **Ingress chains are validated, not yet served.** Nothing binds a task's
  advertised port yet; that is the ingress phase.
- **`tls` on chains is validated, not yet terminated**; external upstreams
  originate TLS today. Bring-your-own certificates and the cluster CA are the
  mTLS phase.
- **No HTTP/2 or HTTP/3** (`protocol = "h2"` is rejected with that reason),
  no UDP, no WASM handlers, no TLS interception — the last by design:
  external upstreams are the body-visible route.
- **`selector.namespaces`** matches every job today (all jobs are in
  `default`); write it now and it starts scoping when namespaces land.
