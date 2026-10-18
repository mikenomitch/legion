# Service Discovery

Legion has built-in service discovery. Declare a service on a task, and other
workloads (and you) can find its healthy endpoints. You don't need Consul,
sidecars, or any extra components. Add a `connect` block and the workload gets
a fixed local address for each upstream, with load balancing behind it. This
works on Linux and macOS. Upstream names and VIPs are Linux-only (see
[Platform notes](#platform-notes)).

## Quick start

In the simplest form, a bare port number declares and advertises the port:

```nickel
{
    name = "redis-job",
    tasks = [{
        name = "redis",
        runtime = "exec",
        config = { command = ["redis-server", "--port", "6379"] },
        service = { port = 6379 },   # -> service "redis", static port 6379
    }],
}
```

Then:

```bash
legion service list             # NAME | JOB | TASK | HEALTHY | TAGS
legion service resolve redis    # healthy endpoint addresses
legion service doctor redis     # layer-by-layer diagnosis when something's off
```

To use it from another task, add one block and read one env var:

```nickel
connect = { upstreams = [{ to = "redis" }] },   # -> $LEGION_UPSTREAM_REDIS
```

See [Connecting to services](#connecting-to-services-the-connect-block).

## Declaring ports: the `network` block

```nickel
network = {
    mode = "auto",                        # auto | host
    ports = [
        { name = "http", to = 8080 },      # no `static` -> dynamic host port (20000-32000)
        { name = "metrics", static = 9090 }, # fixed host port
    ],
},
```

- **Dynamic ports** are assigned by the scheduler at placement and are unique
  per node. The task reads its assigned port from the environment.
- **Static ports** are reserved as declared. A conflicting placement on the
  same node fails with an error naming the port and node.
- `to` records the in-task target port for datapaths that remap. On the host
  datapath it is advisory.

## Advertising: the `service` block

```nickel
service = {
    name = "web",          # defaults to the task name
    port = "http",         # a network.ports label...
    # port = 6379,        # ...or a number  -> declares a static port inline
    # port = "dynamic",   # ...or "dynamic" -> declares a dynamic port inline
    tags = ["v1"],
    meta = { protocol = "http" },
    # allow = ["gateway"],   # optional connect intentions; see below
},
```

The manager derives the service catalog from allocations and health-check
state. There is no registration API to call, so nothing can drift. When the
allocation dies or its node is lost, the endpoint is gone.

`resolve` returns only endpoints whose latest health snapshot is healthy and
whose node is alive. Tasks without checks count as healthy once running. Add
checks to gate registration on actual readiness.

## Checks against dynamic ports

A check's `port` may name a port label. It resolves to the assigned host port
at runtime:

```nickel
checks = [
    { type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" },
],
```

## Environment variables

> On Linux an allocation with upstreams also gets `LEGION_DNS` (its own
> resolver), so `<key>.svc.legion:<port>` works in a URL or config file with
> no env plumbing. See
> [Network Handlers: Name-is-config](network-handlers.md#name-is-config-keysvclegion).

Every task gets its identity and assigned ports injected into its environment.
If you declare the same variable yourself, your value wins:

| Variable | Meaning |
| --- | --- |
| `LEGION_ALLOC_ID` | Allocation ID (churns on each placement). |
| `LEGION_JOB_ID` | Job ID. |
| `LEGION_TASK_NAME` | Task name. |
| `LEGION_PORT_<LABEL>` | Assigned host port for the named port. |
| `LEGION_PORT_TO_<LABEL>` | Declared in-task target port (when `to` is set). |
| `LEGION_IP_<LABEL>` | Node address serving the port (when known). |
| `LEGION_ADDR_<LABEL>` | `ip:port` convenience form. |
| `LEGION_UPSTREAM_<KEY>` | The one `host:port` to dial for a declared upstream ([below](#connecting-to-services-the-connect-block)). |
| `LEGION_UPSTREAM_HOST_<KEY>` | Its host part. |
| `LEGION_UPSTREAM_PORT_<KEY>` | Its port part. |

Labels are uppercased, with non-alphanumeric characters mapped to `_` (`http-2`
becomes `HTTP_2`). Upstream keys follow the same rule (`user-db` becomes
`LEGION_UPSTREAM_USER_DB`). A server binds its dynamic port with, for
example, `python3 -m http.server $LEGION_PORT_HTTP`.

A task only receives endpoints for services it asked for. Its environment
grows with its own ports and upstreams, not with the number of services in the
cluster.

## Connecting to services: the `connect` block

> **External destinations, HTTP mode, and policy.** An upstream can also be
> an external destination (`{ name = "openai", to = "https://api.openai.com" }`),
> speak HTTP with an `http` block (timeouts, budgeted retries, canary split by
> tag), and be governed by egress policy. All of this runs on the same node
> forwarder and is documented in [Network Handlers](network-handlers.md).


Discovery tells you where a service is. `connect` gets your task there. The
task gets one fixed address per upstream, and the worker load-balances every
connection to that address across the service's healthy endpoints. You don't
need a client library, a sidecar, DNS, or service-specific code.

```nickel
{
    name = "api",
    runtime = "exec",
    config = { command = ["sh", "-c", "exec ./api"] },
    connect = {
        upstreams = [
            { to = "db" },                              # a catalog service, L4
            { to = "http://search", balance = "least_conn" },  # parsed as HTTP; strategy override
            { to = "cache", port = 6379 },              # pinned port, also on 127.0.0.1:6379
        ],
    },
}
```

`to` names the upstream, and its scheme alone decides the protocol:

| `to` | Destination | Protocol | Key |
| --- | --- | --- | --- |
| `db`, `tcp://db`, `db.svc.legion` | catalog service `db` | L4 | `name`, default `db` |
| `http://search` | catalog service `search` | HTTP | `name`, default `search` |
| `tcp://10.0.0.5:5432`, `tls://ledger.example:7000`, `http://intranet.example`, `https://api.openai.com` | an external destination | by scheme | `name`, required |

A host that is a bare service name (letters, digits, `-`, `_`, with no dot and
no port) or ends in `.svc.legion` is a catalog service. Anything else is an
external destination and needs a scheme. A dotless external host is written
with its port (`http://wiki:80`). Catalog services take `tcp` (the default)
or `http`; they are reached over the mesh when they are on it, so `tls://` and
`https://` name external destinations only.

The task dials `$LEGION_UPSTREAM_DB`, and the worker forwards that connection
to one of `db`'s healthy endpoints:

```sh
psql "host=$LEGION_UPSTREAM_HOST_DB port=$LEGION_UPSTREAM_PORT_DB"
curl "http://$LEGION_UPSTREAM_SEARCH/_search"
```

**One address per upstream.** `LEGION_UPSTREAM_<KEY>` is what the task dials,
and `LEGION_UPSTREAM_HOST_<KEY>` and `LEGION_UPSTREAM_PORT_<KEY>` are its parts.
`{{ service "<key>" }}` in a [template](#templates-config-files-from-the-feed)
renders the same address.

| Node | `LEGION_UPSTREAM_DB` |
| --- | --- |
| Linux | `127.42.7.2:5432`: the upstream's VIP on its canonical port ([Name-is-config](network-handlers.md#name-is-config-keysvclegion)) |
| macOS, or a node with no VIP block left | `127.0.0.1:20003`: a loopback port from the dynamic range |

On Linux an upstream takes no port from the node's range: its VIP is unique to
the allocation. `port` pins the canonical port and also binds
`127.0.0.1:<port>`, for a client config that wants a literal loopback address
such as `127.0.0.1:6379`. A sandbox's upstreams also get a loopback port,
because the guest's relay is keyed by it. A loopback port never collides with
a host port on that node. `balance` overrides the strategy for that one
upstream.

Upstream addresses are per allocation, like host ports. A `count = 3` client
task gets three independent listeners.

An `http` block (timeouts, budgeted retries, a split by tag) needs an upstream
that speaks HTTP, and the scheme is what says so:

```
task api connect.upstreams entry 'db' to = "db" is reached over L4, but it has
an http block. Hint: write to = "http://db" if it speaks HTTP
```

### Balancing strategies

| `balance` | Behavior |
| --- | --- |
| `least_latency` *(default)* | Latency-aware. Each pick compares two random endpoints and takes the one with the lower latency × (in-flight + 1). A slow-but-alive backend sheds traffic within a few requests, and gets it back once it recovers. |
| `round_robin` | Hand out endpoints in order, wrapping around. Even spread, no state to warm up. Each listener starts at a random point in the list, so many clients started together do not all send their first connection to the same endpoint. |
| `least_conn` | Fewest in-flight connections wins, and ties break round-robin from a random start. Keeps connection counts even whatever the distance, which is what long-lived connections (database pools, gRPC channels, websockets) need: `least_latency` deliberately favors nearby endpoints, so it would pile them onto whichever replica answered first. |
| `local_first` | Round-robin over the endpoints on the same node as the caller. Widens to every endpoint when none is local, so a caller never waits for a local backend that is not there. |

Names are normalized before lookup (trimmed, lowercased, `-` mapped to `_`),
so `Least-Conn` and `least_conn` are the same strategy. An unknown name is
rejected at submit, and the error lists the valid ones.

All state is per listener: one allocation's view of one upstream on one
node. There is no cluster-wide load view. A connection (or, in HTTP mode, a
request) counts as in flight from the moment its dial starts until it closes.

#### How `least_latency` learns

`least_latency` is the balancer Linkerd and Finagle use: power of two choices
over a peak-EWMA latency estimate. What it measures depends on the mode:

- **L4 (the default).** The time to connect, then the time from the first
  byte the task sends to the first byte the endpoint answers. Nothing is
  parsed. A protocol where the server speaks first (a database greeting, SMTP)
  only gives the connect time, which still tells a nearby endpoint from a
  far one.
- **HTTP mode.** The time to each response's head.

In a benchmark with three backends, one of them deliberately slow, strategies
that spread evenly averaged 1.8–1.9 ms per request and `least_latency`
averaged 62–114 µs, close to the fast backends alone
([What the datapath costs](network-handlers.md#what-the-datapath-costs)).

A slow answer raises an endpoint's estimate at once. Estimates fall back with
a 10-second time constant, so one fast answer doesn't erase a run of slow
ones, and an endpoint that was avoided is tried again once it cools down. A
failed dial, a failed mesh handshake, or (HTTP mode) a 5xx counts as a
1-second answer. An endpoint with no samples yet starts at the average of the
others, so a new backend gets a fair share rather than a flood.

Two consequences to know about:

- **It favors nearby endpoints.** An endpoint on the caller's node connects
  faster than one across the network, so it takes more of the traffic until
  its load evens out the difference. Use `round_robin` when you want an even
  spread regardless of distance.
- **Long-lived connections learn slowly on L4.** A connection is measured
  when it opens. A pool of long-lived database connections is balanced by
  connect time and load, not by query latency. HTTP mode measures every
  request.

#### Adding a strategy

A strategy is a trait plus a registry entry. Adding one takes two steps, both
in one file:

1. Implement `Balancer` in [`src/worker/balance.rs`](../src/worker/balance.rs)
   (`name()` plus `pick(&[Endpoint], &HashMap<String, usize>) -> Option<usize>`,
   where the map is in-flight connections per endpoint address). A strategy
   that learns from outcomes also implements `observe(addr, Outcome)`, which
   the datapath calls with every latency sample and failure it sees.
2. Add one row to the `BALANCERS` table in the same file:
   `("first_healthy", || Arc::new(FirstHealthy))`.

`BALANCERS` is the only list of strategy names in the tree. Config validation
reads it too (`is_known_balance`, and the "valid strategies are …" error
message), so you can't ship a name the worker cannot build. Nothing else
changes. The wire carries the strategy name as a string, so a new strategy
needs no proto change and no migration.

### How it works

1. **Placement.** The manager assigns each upstream its address and stores it
   on the allocation next to its host ports: a VIP and canonical port on
   Linux, a loopback port elsewhere (and for a pinned `port`).
2. **Listeners.** The worker runs one TCP listener per
   `(allocation, upstream)` on that address.
3. **Endpoint feed.** On every heartbeat (the worker type's
   `heartbeatSeconds`, 2s by default), the manager sends the current endpoint set for each upstream this
   node's allocations declare. The set holds healthy endpoints only, already
   filtered by intentions. That interval is how quickly a client sees a backend
   come up, go unhealthy, or move. Nodes running no `connect` jobs pay nothing,
   because the manager skips the catalog work entirely.
4. **Per connection.** The worker asks the strategy for an endpoint and dials
   it with a 2s timeout. On refusal or timeout it drops that candidate and
   picks again, up to 3 endpoints. Then it splices the two sockets together.
   On Linux the bytes move with `splice(2)` and never leave the kernel.
   Elsewhere, and whenever TLS is involved, they are copied in user space
   ([Network Handlers](network-handlers.md#what-the-datapath-does-today)).
5. **No endpoints.** The connection is accepted and closed immediately. A
   scaled-to-zero, all-unhealthy, or intention-denied upstream looks like a
   fast close to the client, rather than a hang.

Only the first hop is loopback. The forwarder dials the endpoint's advertised
node address, so cross-node traffic goes over the regular network as plain TCP.

**Fail-static.** Endpoints are replaced only after a successful heartbeat. A
manager outage leaves the last known good set in place, and existing clients
keep working. An explicit empty list is authoritative, so a killed backend
stops receiving traffic.

**Never fatal.** If an upstream port won't bind because something else on the
node holds it, the worker logs one warning and retries on the next heartbeat.
The task's dials fail in the meantime. A backend that refuses connections, or
an upstream that disappears, affects only that listener. It never affects the
heartbeat loop or the worker. When an upstream leaves the desired set, its
listener stops accepting and its port is released. Connections already being
forwarded drain on their own. Per-connection logging stays at `debug`.

### Intentions: `service.allow`

A service can name the jobs allowed to reach it through connect:

```nickel
service = {
    port = "http",
    allow = ["gateway", "api"],   # job ids; absent or ["*"] = every job
},
```

- Entries are job ids (a job's `name`), matched against the consuming
  allocation's job.
- Absent or `["*"]` means every job may connect. This is the default.
- Enforcement happens in the endpoint feed. A denied consumer's forwarder gets
  an empty list, so its connections are accepted and closed. There is no error
  code and no partial list. When a client sees only immediate closes, run
  `legion service doctor <svc>`. It prints the service's `allow` list, so you
  can tell "no healthy endpoints" apart from "your job is not on the list". The
  list comes back on `ListServices` (`ServiceSummary.allow`), so `doctor` needs
  only `job:read`, the same scope as the rest of the report.
- Intentions don't affect the catalog. `legion service list|resolve` and the
  `ListServices`/`ResolveService` RPCs still show the service to anyone whose
  ACLs permit the read. Intentions gate the connect datapath, not who can see
  the service.

**Scope.** Intentions are enforced by the control plane and the forwarder.
They don't use cryptographic identity. They govern what Legion's own connect
path hands out. They don't stop a task that already knows a backend's
`address:port` from dialing it directly, and nothing on that wire is
authenticated or encrypted by default. Intentions do not use mTLS identity.
For cryptographic identity, turn on mesh mTLS on the service's ingress chain
and restrict callers with `requireIdentity`. See
[network-handlers.md](network-handlers.md#mtls).

### Validation

These are checked at submit, and by `legion job validate <spec>` before you submit:

- Upstream `to` is a catalog service name or an external URL (see the table
  above), an external one has a `name`, and no key is declared twice in one
  task.
- An `http` block, a body-reading handler, or credentials need an upstream
  whose `to` speaks HTTP.
- `port` is greater than 0, with no duplicates and no collision with a
  static port declared in the same task's `network.ports` (including one
  declared by the `service.port` number shorthand).
- `balance` is a known strategy. The error lists the valid names.
- `service.allow` entries are non-empty.

The upstream service doesn't have to exist yet. It may be submitted later, or
by someone else, and Legion won't fail a submission because of ordering. An
upstream whose service has no healthy endpoints gets an empty list at runtime,
and `legion service doctor <svc>` will explain why.

### Templates: config files from the feed

Some clients, such as HAProxy, nginx, or a database driver, read backends from
a file instead of an env var. `connect.templates` renders those files from the
task's upstreams and their live endpoint feed. It renders before the task
starts and again on every heartbeat that changes the result:

```nickel
connect = {
    upstreams = [{ to = "db" }, { to = "api" }],
    templates = [
        {
            name = "backends",
            destination = "local/backends.cfg",   # under the task directory
            source = m%"
                db = {{ service "db" }}
                {{ range endpoints "api" }}
                server {{ .address }}:{{ .port }} # {{ .node }} {{ .tags }}
                {{ end }}
                alloc = {{ env "LEGION_ALLOC_ID" }}
            "%,
            changeMode = "signal",     # noop (default) | signal | restart
            changeSignal = "SIGHUP",
            coalesce = "2s",           # let the feed settle before acting
            splay = "5s",              # random delay so a fleet does not reload at once
        },
    ],
},
```

Templates support three functions and one loop:

| Action | Renders |
| --- | --- |
| `{{ service "db" }}` | the address the task dials for the upstream: `<vip>:<canonical port>` on Linux, `127.0.0.1:<local port>` elsewhere. Pass `"host"` or `"port"` as a second argument to select a part. Every connection still goes through the node's datapath, so policy, handlers and the graph apply. |
| `{{ range endpoints "db" }} … {{ end }}` | one copy of the body per healthy endpoint the manager last fed, with `.address`, `.port`, `.node`, `.allocation`, `.tags` |
| `{{ env "NAME" }}` | the Legion-injected environment (`LEGION_*`), empty when unset |

When the content changes, the file is written atomically (temp file plus
rename). `changeMode` then sets what the task receives: nothing (most clients
re-read on their own), `changeSignal`, or a restart. `coalesce` holds the
signal until the feed has been quiet for that long, though the file itself is
always current. `splay` adds a random delay of up to that long.

Templates are fail-static: a manager outage leaves the feed and the file as
they were. A template is rejected at submit if it names an upstream the task
doesn't declare, uses an unknown action, or points outside the task directory.
A render problem at runtime shows in `legion net doctor` and the task's Network
section. Templates are intentionally small, with no conditionals or
arithmetic.

### What connect is not

- **Balancing is per connection** in L4 mode. A long-lived keep-alive or
  gRPC channel stays on the backend it first landed on. HTTP mode balances,
  retries and splits per request. See
  [network-handlers.md](network-handlers.md).
- **No mid-stream retries** in L4 mode. Failover happens while dialing.
- **No UDP.** Names (`<service>.svc.legion`), namespaces (`mode = "gated"`),
  mTLS, handlers and the service graph all live in
  [network-handlers.md](network-handlers.md).

### Platform behavior

Connect is plain userspace loopback TCP, so `LEGION_UPSTREAM_*` behaves the
same on macOS and Linux: it is the address to dial on either. On Linux it is
an upstream VIP, and the names (`LEGION_DNS`, `<key>.svc.legion`) come with
it; on macOS it is a loopback port. See [Platform notes](#platform-notes).

## Querying: CLI and API

```bash
legion service list                  # all services + healthy/total counts
legion service resolve web           # healthy endpoints (table)
legion service resolve web -j        # JSON (scriptable)
legion service resolve web --all     # include unhealthy, with status detail
legion service resolve web --one     # a single random address:port, for scripts
legion service doctor web            # walk the layers, name the failing one
```

Every subcommand takes `--json`/`-j` and emits the standard
`{ "ok": true, "data": … }` envelope, so agents don't have to parse prose.
`--one` skips `jq` entirely:

```bash
curl "http://$(legion service resolve web --one)/health"
```

> **Output change (connect release).** `service list -j` and `service resolve -j`
> used to print a bare JSON array. They now print the standard envelope like
> every other command, so the endpoint array moved from `.` to `.data`. Change a
> script doing `jq -r '.[0]…'` to `jq -r '.data[0]…'`, or use
> `legion service resolve <svc> --one`, which needs no `jq` at all. Failures on
> these commands now return the typed `{ "ok": false, "error": … }` envelope
> instead of prose on stderr. `service list -j` entries also have a new `allow`
> field with the service's connect intentions (empty means every job may
> connect).

The same data is available over gRPC through `ListServices` and
`ResolveService` on the `Manager` service (see `proto/legion.proto`), and over
MCP as `legion_service_list` and `legion_service_resolve`. Both RPCs carry the
service's `meta` map. By convention, clients use `meta.protocol` (`http`,
`https`, …) to turn `address:port` into a URL.

The UI has a Services page with the same catalog, per-endpoint health, and a
visual version of `doctor`. Endpoints link straight to the address when the
service speaks HTTP (see [Public addresses](#public-addresses) for nodes on a
private network). Job and task pages show their own services, assigned ports,
declared checks, and per-allocation health. In the Network page's service
graph, each service box opens its service page and each caller box opens its
job.

### Public addresses

An endpoint's `address` is where the *cluster* reaches it: the node's
in-cluster address, usually private (`10.x`, `172.16–31.x`, `192.168.x`). A
browser outside that network can't open it. To give the UI and CLI a link
that works, start each worker with its public host name or IP:

```bash
LEGION_WORKER_PUBLIC_ADDRESS=203.0.113.7 legion join <token>

# EC2: read the instance's public IPv4 from the metadata service (IMDSv2)
TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token \
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
LEGION_WORKER_PUBLIC_ADDRESS=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/public-ipv4) legion join <token>
```

The value must be a bare host name or IP (no scheme, no port). Anything else
is ignored with a warning. The worker registers it as the `publicAddress`
node attribute, and every endpoint on that node carries it as
`ServiceEndpoint.public_address`:

- the UI links the public address (badged `public`) and shows the in-cluster
  one beside it. Allocation port chips on the job, task, worker, and
  allocation pages do the same;
- `legion service resolve` adds a `PUBLIC` column, and `-j` adds
  `publicAddress`.

It is display only. Nothing in the cluster dials the public address, and
setting it does not open the port. The node's firewall or security group must
still allow inbound traffic to the task's host port. Without a public address,
private endpoint addresses are still linked (a VPN or tunnel may reach them)
but dimmed and badged `private`, with a tooltip naming this variable.

Not done yet: detecting the public address automatically from cloud
metadata, and reaching nodes that have no open ports at all through the
manager.

`doctor` checks these layers in order: service declared, allocations placed,
nodes alive, port assigned, health checks passing. At the first failing layer
it prints a verdict, the cause, and a fix hint. When everything passes, it
prints the resolved endpoints. It also reports the service's connect
intentions when it declares any, so a consumer that resolves to nothing can
tell "unhealthy" apart from "not allowed".

## Try it end to end

```bash
# terminal 1                          # terminal 2
cargo run -- start manager \          cargo run -- join <TOKEN>
  -c configs/manager.ncl

# terminal 3
cargo run -- job run configs/jobs/service-demo.ncl
cargo run -- service resolve web -j
curl "http://$(cargo run -q -- service resolve web --one)"

# kill the process behind the service (not via legion), watch it leave the catalog:
pkill -f http.server; sleep 12
cargo run -- service resolve web      # no healthy endpoints
cargo run -- service doctor web       # names the failing layer
# after on_unhealthy restarts it, it re-registers (with a fresh dynamic port).
```

### ...and the connect path

[`configs/jobs/connect-demo.ncl`](../configs/jobs/connect-demo.ncl) runs
two `web` backends plus a `client` task that curls `$LEGION_UPSTREAM_WEB` in a
loop. Each backend serves its own allocation id, so the balancing is visible in
the client's logs. The demo sets `balance = "round_robin"` so the client
visibly alternates:

```bash
cargo run -- job run configs/jobs/connect-demo.ncl
cargo run -- service resolve web              # two healthy endpoints
cargo run -- job logs connect-demo --tail     # client alternates between them

# kill ONE backend's process (not via legion): the client keeps serving from the
# survivor, and returns to alternating once on_unhealthy restarts the dead one.
pkill -n -f http.server
cargo run -- job logs connect-demo --tail
```

## Platform notes

Discovery runs entirely in userspace. Ports, env vars, the catalog, and the
CLI behave the same on Linux and macOS. Connect's loopback ports do too. The
forwarder is loopback TCP with no network namespaces, no bridge, no iptables
and no `SO_REUSEPORT`.

Upstream names and VIPs need the whole `127.0.0.0/8` range routed to
loopback. Linux does this by default, and macOS only has `127.0.0.1`. So on
macOS an allocation gets no VIP block: no `LEGION_DNS`, and
`LEGION_UPSTREAM_<KEY>` and `{{ service }}` are `127.0.0.1:<local port>`. On
Linux, if a node runs out of VIP blocks, the allocation still places and its
upstreams get loopback ports instead. macOS has no network-namespace isolation, so all tasks share
the host port space. Dynamic ports (both task ports and upstream ports) avoid
collisions. Static ports are first come, first served per node.

## Beyond discovery

The mesh adds more on top of the catalog: names (`<service>.svc.legion` from a
node-local resolver), HTTP mode with retries and canary splits, gated
namespaces, mTLS on node identity, external handlers, templates, and the
service graph. These are documented in [network-handlers.md](network-handlers.md).
UDP, transparent capture, and WASM handlers are not supported yet.

## Who calls a service

`legion service edges [<service>]` and the Network page in the UI show the
service graph. It lists every caller that reaches a service through its
datapath, with request rate, errors, denials and latency over the last few
minutes. It also shows declared upstreams with no traffic, and whether the
service's `allow` intentions admit each caller. See
[network-handlers.md](network-handlers.md#service-graph).
