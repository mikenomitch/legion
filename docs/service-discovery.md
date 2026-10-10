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
connect = { upstreams = [{ service = "redis" }] },   # -> $LEGION_UPSTREAM_REDIS
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
> resolver) and, per upstream, `LEGION_UPSTREAM_HOST_<KEY>` /
> `LEGION_UPSTREAM_VIP_<KEY>`, so `<key>.svc.legion:<port>` works in a URL or
> config file with no env plumbing. See
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
| `LEGION_UPSTREAM_<SERVICE>` | `127.0.0.1:<port>` to dial a declared upstream ([below](#connecting-to-services-the-connect-block)). |
| `LEGION_UPSTREAM_PORT_<SERVICE>` | Just the local port of that upstream. |

Labels are uppercased, with non-alphanumeric characters mapped to `_` (`http-2`
becomes `HTTP_2`). Upstream service names follow the same rule (`user-db`
becomes `LEGION_UPSTREAM_USER_DB`). A server binds its dynamic port with, for
example, `python3 -m http.server $LEGION_PORT_HTTP`.

A task only receives endpoints for services it asked for. Its environment
grows with its own ports and upstreams, not with the number of services in the
cluster.

## Connecting to services: the `connect` block

> **External destinations, HTTP mode, and policy.** An upstream can also be
> an external destination (`{ name = "openai", to = "https://api.openai.com" }`),
> carry an `http` block (timeouts, budgeted retries, canary split by tag,
> outlier ejection), and be governed by egress policy. All of this runs on the
> same loopback forwarder and is documented in
> [Network Handlers](network-handlers.md).


Discovery tells you where a service is. `connect` gets your task there. The
task gets one fixed local address per upstream, and the worker load-balances
every connection to that address across the service's healthy endpoints. You
don't need a client library, a sidecar, DNS, or service-specific code.

```nickel
{
    name = "api",
    runtime = "exec",
    config = { command = ["sh", "-c", "exec ./api"] },
    connect = {
        upstreams = [
            { service = "db" },                             # dynamic local port
            { service = "cache", localPort = 6379 },         # pinned local port
            { service = "search", balance = "least_conn" },  # strategy override
        ],
    },
}
```

The task dials `$LEGION_UPSTREAM_DB` (say, `127.0.0.1:20003`), and the worker
forwards that connection to one of `db`'s healthy endpoints:

```sh
psql "host=127.0.0.1 port=$LEGION_UPSTREAM_PORT_DB"
curl "http://$LEGION_UPSTREAM_SEARCH/_search"
```

Only `service` is required. `localPort` pins the loopback port for clients that
can't read environment variables, such as a config file that wants a literal
`127.0.0.1:6379`. Without it, the port is assigned at placement from the same
per-node dynamic range (20000-32000) and used-port set as task ports. An
upstream port can never collide with a host port on that node. `balance`
overrides the strategy for that one upstream.

Upstream ports are per allocation, like host ports. A `count = 3` client task
gets three independent listeners on three distinct ports.

### Balancing strategies

| `balance` | Behavior |
| --- | --- |
| `round_robin` *(default)* | Hand out endpoints in order, wrapping around. Even spread, no state to warm up. |
| `random` | Uniform random pick. Best when many clients share one backend set, since there are no cursors to fall into sync. |
| `least_conn` | Fewest in-flight connections wins, and ties break round-robin. For long-lived or uneven connections. |
| `local_first` | Round-robin over the endpoints on the same node as the caller. Widens to every endpoint when none is local, so a caller never waits for a local backend that is not there. |

Names are normalized before lookup (trimmed, lowercased, `-` mapped to `_`),
so `Least-Conn` and `least_conn` are the same strategy. An unknown name is
rejected at submit, and the error lists the valid ones.

`least_conn` counts are per listener: one allocation's view of one upstream on
one node. A connection counts from the moment its dial starts until it closes.
There is no cluster-wide load view, and round-robin cursors are per listener
too.

#### Adding a strategy

A strategy is a trait plus a registry entry. Adding one takes two steps, both
in one file:

1. Implement `Balancer` in [`src/worker/balance.rs`](../src/worker/balance.rs)
   (`name()` plus `pick(&[Endpoint], &HashMap<String, usize>) -> Option<usize>`,
   where the map is in-flight connections per endpoint address).
2. Add one row to the `BALANCERS` table in the same file:
   `("first_healthy", || Arc::new(FirstHealthy))`.

`BALANCERS` is the only list of strategy names in the tree. Config validation
reads it too (`is_known_balance`, and the "valid strategies are …" error
message), so you can't ship a name the worker cannot build. Nothing else
changes. The wire carries the strategy name as a string, so a new strategy
needs no proto change and no migration.

### How it works

1. **Placement.** The manager assigns each upstream a loopback port and stores
   it on the allocation next to its host ports.
2. **Listeners.** The worker runs one TCP listener per
   `(allocation, upstream service)` bound on `127.0.0.1:<local port>`.
3. **Endpoint feed.** On every heartbeat (the worker type's
   `heartbeatSeconds`, 2s by default), the manager sends the current endpoint set for each upstream this
   node's allocations declare. The set holds healthy endpoints only, already
   filtered by intentions. That interval is how quickly a client sees a backend
   come up, go unhealthy, or move. Nodes running no `connect` jobs pay nothing,
   because the manager skips the catalog work entirely.
4. **Per connection.** The worker asks the strategy for an endpoint and dials
   it with a 2s timeout. On refusal or timeout it drops that candidate and
   picks again, up to 3 endpoints. Then it splices the two sockets together
   (`copy_bidirectional`).
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

- Upstream `service` is non-empty, with no duplicate upstream services in one
  task.
- `localPort` is greater than 0, with no duplicates and no collision with a
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
    upstreams = [{ service = "db" }, { service = "api" }],
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

Connect is plain userspace loopback TCP, so the loopback ports and
`LEGION_UPSTREAM_*` addresses behave the same on macOS and Linux. Upstream
names and VIPs (`LEGION_DNS`, `LEGION_UPSTREAM_HOST_*`,
`LEGION_UPSTREAM_VIP_*`) are Linux-only. See [Platform notes](#platform-notes).

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
service speaks HTTP. Job and task pages show their own services, assigned
ports, declared checks, and per-allocation health.

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
the client's logs:

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
macOS an allocation gets no VIP block: no `LEGION_DNS`, no
`LEGION_UPSTREAM_HOST_*` or `LEGION_UPSTREAM_VIP_*`, and `{{ service }}`
renders `127.0.0.1:<local port>`. On Linux, if a node runs out of VIP blocks,
the allocation still places and gets loopback ports only. macOS has no network-namespace isolation, so all tasks share
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
