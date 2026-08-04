# Service Discovery

Legion has built-in service discovery: declare a service on a task and other
workloads (and you) can find its healthy endpoints — no Consul, no sidecars, no
extra components. Then declare a `connect` block and the workload gets a fixed
local address per upstream with load balancing behind it. Works the same on
Linux and macOS.

Design background lives in [internal_docs/plans/service-discovery-final-plan.md](../internal_docs/plans/service-discovery-final-plan.md)
(and the docs it consolidates). This page documents what is implemented today.

## Quick start

The one-liner — a bare port number declares and advertises the port:

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

And to *use* it from another task — one block, one env var:

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

- **Dynamic ports** are assigned by the scheduler at placement, guaranteed
  unique per node. The task reads its assigned port from the environment.
- **Static ports** are reserved as-declared; a conflicting placement on the
  same node fails with a clear error naming the port and node.
- `to` records the in-task target port for datapaths that remap; on the host
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

Registration is a **projection**: the manager derives the catalog from
allocations plus health-check state. There is no registration API to call and
nothing that can drift — when the allocation dies or its node is lost, the
endpoint is gone.

**Only-healthy:** `resolve` returns endpoints whose latest health snapshot is
healthy *and* whose node is alive. Tasks without checks count as healthy once
running; add checks to gate registration on real readiness.

## Checks against dynamic ports

A check's `port` may name a port label — it resolves to the assigned host port
at runtime:

```nickel
checks = [
    { type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" },
],
```

## Environment variables

Every task gets its identity and assigned ports injected (user-declared env
wins on conflicts):

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

Labels are uppercased with non-alphanumerics mapped to `_` (`http-2` →
`HTTP_2`); upstream service names use the same rule (`user-db` →
`LEGION_UPSTREAM_USER_DB`). A server binds its dynamic port with e.g.
`python3 -m http.server $LEGION_PORT_HTTP`.

A task is never handed the endpoints of services it did not ask for — the
environment is O(its own ports + its own upstreams), never O(cluster services).

## Connecting to services: the `connect` block

Discovery tells you *where* a service is. `connect` closes the last mile: the
task gets one fixed local address per upstream, and the worker load-balances
every connection made to it across that service's healthy endpoints. No client
library, no sidecar, no DNS, no service-specific code.

```nickel
{
    name = "api",
    kind = "exec",
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

The task dials `$LEGION_UPSTREAM_DB` — say `127.0.0.1:20003` — and the worker
forwards that connection to one of `db`'s healthy endpoints:

```sh
psql "host=127.0.0.1 port=$LEGION_UPSTREAM_PORT_DB"
curl "http://$LEGION_UPSTREAM_SEARCH/_search"
```

Only `service` is required. `localPort` pins the loopback port for clients that
cannot read environment variables (a config file that wants a literal
`127.0.0.1:6379`); without it, the port is assigned at placement from the same
per-node dynamic range (20000–32000) and the same used-port set as task ports,
so an upstream port can never collide with a host port on that node. `balance`
overrides the strategy for that one upstream.

Upstream ports are per *allocation*, exactly like host ports: a `count = 3`
client task gets three independent listeners on three distinct ports.

### Balancing strategies

| `balance` | Behavior |
| --- | --- |
| `round_robin` *(default)* | Hand out endpoints in order, wrapping around. Even spread, no state to warm up. |
| `random` | Uniform random pick. Best when many clients share one backend set — no synchronized cursors. |
| `least_conn` | Fewest in-flight connections wins; ties break round-robin. For long-lived or uneven connections. |

Names are normalized before lookup — trimmed, lowercased, `-` mapped to `_` —
so `Least-Conn` and `least_conn` are the same strategy. An unknown name is
rejected at submit, and the error lists the valid ones.

`least_conn`'s counts are honest about their scope: they are per listener (one
allocation's view of one upstream on one node), and a connection counts from
the moment its dial starts until it closes. There is no cluster-wide load view,
and round-robin cursors are per listener too.

#### Adding a strategy

The strategy is a trait plus a registry, which is the whole extension story —
two steps, both in one file:

1. Implement `Balancer` in [`src/worker/balance.rs`](../src/worker/balance.rs)
   (`name()` plus `pick(&[Endpoint], &HashMap<String, usize>) -> Option<usize>`,
   where the map is in-flight connections per endpoint address).
2. Add one row to the `BALANCERS` table in the same file:
   `("first_healthy", || Arc::new(FirstHealthy))`.

`BALANCERS` is the only list of strategy names in the tree — config validation
reads it too (`is_known_balance`, and the "valid strategies are …" error
message), so there is no second place to forget and no way to ship a name the
worker cannot build. Nothing else changes: the wire carries the strategy *name*,
not an enum, so a new strategy needs no proto change and no migration.

### How it works

1. **Placement.** The manager assigns each upstream a loopback port and stores
   it on the allocation next to its host ports.
2. **Listeners.** The worker runs one TCP listener per
   `(allocation, upstream service)` bound on `127.0.0.1:<local port>`.
3. **Endpoint feed.** Every heartbeat — `WORKER_HEARTBEAT_DURATION_SEC`, i.e.
   every 2s — the manager sends the current endpoint set for each upstream this
   node's allocations declare: healthy endpoints only, already filtered by
   intentions. That interval is the refresh cadence a client observes after a
   backend comes up, goes unhealthy, or moves. Nodes running no `connect` jobs
   pay nothing: the manager skips the catalog work entirely.
4. **Per connection.** Ask the strategy for an endpoint → dial with a 2s
   timeout → on refusal or timeout drop that candidate and pick again, up to 3
   endpoints → then splice the two sockets together (`copy_bidirectional`).
5. **No endpoints.** The connection is accepted and closed immediately. That is
   what a scaled-to-zero, all-unhealthy, or intention-denied upstream looks like
   to a client — a fast close, not a hang.

Only the *first* hop is loopback. The forwarder dials the endpoint's advertised
node address, so cross-node traffic goes over the regular network as plain TCP.

**Fail-static.** Endpoints are replaced only after a heartbeat that actually
succeeded, so a manager outage leaves the last known good set in place and
existing clients keep working. An explicit *empty* list is authoritative — a
killed backend stops receiving traffic.

**Never fatal.** An upstream port that will not bind (something else on the node
holds it) logs one warning and is retried on the next heartbeat; the task's
dials fail meanwhile. A backend that refuses connections, or an upstream that
disappears, degrades that listener only — never the heartbeat loop or the
worker. When an upstream leaves the desired set its listener stops accepting and
its port is released; connections already being forwarded drain on their own.
Per-connection logging stays at `debug`.

### Intentions: `service.allow`

A service can name the jobs allowed to reach it through connect:

```nickel
service = {
    port = "http",
    allow = ["gateway", "api"],   # job ids; absent or ["*"] = every job
},
```

- Entries are **job ids** (a job's `name`), matched against the *consuming*
  allocation's job.
- Absent or `["*"]` means every job may connect — the default, and exactly the
  behavior services had before connect existed.
- Enforcement happens in the endpoint feed: a denied consumer's forwarder is
  handed an empty list, so its connections are accepted and closed. There is no
  error code and no partial list — when a client sees only immediate closes,
  `legion service doctor <svc>` prints the service's `allow` list so you can
  tell "no healthy endpoints" apart from "your job is not on the list". The list
  rides along on `ListServices` (`ServiceSummary.allow`), so `doctor` needs only
  `job:read` — the same scope as the rest of the report.
- Intentions do **not** affect the catalog. `legion service list|resolve` and
  the `ListServices`/`ResolveService` RPCs still show the service to anyone
  whose ACLs permit the read — intentions gate the connect datapath, not who may
  look at it.

**Honest scope.** This is control-plane plus forwarder enforcement, not
cryptographic identity. It governs what Legion's own connect path hands out; it
does not stop a task that already knows a backend's `address:port` from dialing
it directly, and nothing on that wire is authenticated or encrypted.
Identity-backed enforcement (mTLS between forwarders, with `service.allow` as
the policy input) is the mesh follow-on — see
[the final plan](../internal_docs/plans/service-discovery-final-plan.md) §8.

### Validation

Checked at submit — and by `legion job validate <spec>` before you submit:

- upstream `service` non-empty; no duplicate upstream services in one task
- `localPort` greater than 0; no duplicates; no collision with a static port
  declared in the same task's `network.ports` (including one declared by the
  `service.port` number shorthand)
- `balance` must be a known strategy; the error lists the valid names
- `service.allow` entries non-empty

The upstream service is deliberately **not** required to exist. It may be
submitted later, or by someone else — failing submission on ordering is exactly
the config-ordering footgun this design avoids. An upstream whose service has no
healthy endpoints simply gets an empty list at runtime, which
`legion service doctor <svc>` will explain.

### What connect is not (v1)

- **L4 only.** TCP, and balancing is per *connection*, not per request. A
  long-lived HTTP keep-alive or gRPC channel stays on the backend it first
  landed on; open a new connection to be rebalanced.
- **No mid-stream retries.** Failover happens while dialing; once bytes have
  flowed, a broken backend is the client's error to handle.
- **No encryption or peer identity** — see the intentions scope note above.
- **No DNS names, no sidecar, no netns, no UDP.**

### Platform behavior

Pure userspace loopback TCP — nothing here is platform-conditional, so macOS and
Linux behave identically (see [Platform notes](#platform-notes)).

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
`{ "ok": true, "data": … }` envelope, so agents never scrape prose. `--one`
skips `jq` entirely:

```bash
curl "http://$(legion service resolve web --one)/health"
```

> **Output change (connect release).** `service list -j` and `service resolve -j`
> previously printed a bare JSON array. They now print the standard envelope
> like every other command, so the endpoint array moved from `.` to `.data`:
> a script doing `jq -r '.[0]…'` must become `jq -r '.data[0]…'`, or — better —
> `legion service resolve <svc> --one`, which needs no `jq` at all. Failures on
> these commands are now the typed `{ "ok": false, "error": … }` envelope rather
> than prose on stderr. `service list -j` entries also gained an `allow` field
> (the service's connect intentions; empty means every job may connect).

The same data is available over gRPC: `ListServices` and `ResolveService` on
the `Manager` service (see `proto/legion.proto`), and over MCP as
`legion_service_list` / `legion_service_resolve`. Both RPCs carry the service's
`meta` map; `meta.protocol` (`http`, `https`, …) is the convention clients use
to turn `address:port` into a URL.

The UI has a **Services** page with the same catalog, per-endpoint health, and
a visual version of `doctor`. Endpoints link straight to the address when the
service speaks HTTP, and job/task pages show their own services, assigned
ports, declared checks, and per-allocation health.

`doctor` checks, in order: service declared → allocations placed → nodes
alive → port assigned → health checks passing — and prints a verdict, the
cause, and a fix hint at the first failing layer. When everything passes it
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

Discovery is pure userspace: ports + env vars + catalog + CLI behave
identically on Linux and macOS. So is connect — the forwarder is loopback TCP
with no network namespaces, no bridge, no iptables and no `SO_REUSEPORT`.
macOS has no network-namespace isolation, so all tasks share the host port
space — dynamic ports (task ports *and* upstream ports) avoid collisions; static
ports are first-come-first-served per node.

## What's next (planned)

Shipped since v1: `connect.upstreams`, the per-node L4 forwarder, pluggable
balancing, and connect-path intentions (`service.allow`) — all described above.

Still planned: DNS (`<service>.svc.legion` from a node-local resolver), template
functions (`{{ range endpoints "db" }}`), the `datapath` plugin ABI
(bridge/CNI), **mTLS** so intentions rest on cryptographic workload identity
rather than the control plane alone, the opt-in L7 layer with WASM middleware,
and the observability service graph. See the
[final plan](../internal_docs/plans/service-discovery-final-plan.md) §8.
