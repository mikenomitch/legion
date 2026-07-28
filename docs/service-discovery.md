# Service Discovery

Legion has built-in service discovery: declare a service on a task and other
workloads (and you) can find its healthy endpoints — no Consul, no sidecars, no
extra components. Works the same on Linux and macOS.

Design background lives in [internal_docs/plans/service-discovery-final-plan.md](../internal_docs/plans/service-discovery-final-plan.md)
(and the docs it consolidates). This page documents what is implemented today.

## Quick start

The one-liner — a bare port number declares and advertises the port:

```jsonnet
{
    name: "redis-job",
    tasks: [{
        name: "redis",
        runtime: "exec",
        config: { command: ["redis-server", "--port", "6379"] },
        service: { port: 6379 },   // -> service "redis", static port 6379
    }],
}
```

Then:

```bash
legion service list             # NAME | JOB | TASK | HEALTHY | TAGS
legion service resolve redis    # healthy endpoint addresses
legion service doctor redis     # layer-by-layer diagnosis when something's off
```

## Declaring ports: the `network` block

```jsonnet
network: {
    mode: "auto",                        // auto | host
    ports: [
        { name: "http", to: 8080 },      // no `static` -> dynamic host port (20000-32000)
        { name: "metrics", static: 9090 }, // fixed host port
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

```jsonnet
service: {
    name: "web",          // defaults to the task name
    port: "http",         // a network.ports label...
    // port: 6379,        // ...or a number  -> declares a static port inline
    // port: "dynamic",   // ...or 'dynamic' -> declares a dynamic port inline
    tags: ["v1"],
    meta: { protocol: "http" },
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

```jsonnet
checks: [
    { type: "health", kind: "tcp", port: "http", every: "5s", timeout: "2s" },
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

Labels are uppercased with non-alphanumerics mapped to `_` (`http-2` →
`HTTP_2`). A server binds its dynamic port with e.g.
`python3 -m http.server $LEGION_PORT_HTTP`.

## Querying: CLI and API

```bash
legion service list                  # all services + healthy/total counts
legion service resolve web           # healthy endpoints (table)
legion service resolve web -j        # JSON (scriptable)
legion service resolve web --all     # include unhealthy, with status detail
legion service doctor web            # walk the layers, name the failing one
```

The same data is available over gRPC: `ListServices` and `ResolveService` on
the `Manager` service (see `proto/legion.proto`). Both carry the service's
`meta` map; `meta.protocol` (`http`, `https`, …) is the convention clients use
to turn `address:port` into a URL.

The UI has a **Services** page with the same catalog, per-endpoint health, and
a visual version of `doctor`. Endpoints link straight to the address when the
service speaks HTTP, and job/task pages show their own services, assigned
ports, declared checks, and per-allocation health.

`doctor` checks, in order: service declared → allocations placed → nodes
alive → port assigned → health checks passing — and prints a verdict, the
cause, and a fix hint at the first failing layer. When everything passes it
prints the resolved endpoints.

## Try it end to end

```bash
# terminal 1                          # terminal 2
cargo run -- start manager \          cargo run -- join <TOKEN>
  -c configs/manager.jsonnet

# terminal 3
cargo run -- job run configs/jobs/service-demo.jsonnet
cargo run -- service resolve web -j
curl "$(cargo run -q -- service resolve web -j | jq -r '.[0] | "\(.address):\(.port)"')"

# kill the process behind the service (not via legion), watch it leave the catalog:
pkill -f http.server; sleep 12
cargo run -- service resolve web      # no healthy endpoints
cargo run -- service doctor web       # names the failing layer
# after on_unhealthy restarts it, it re-registers (with a fresh dynamic port).
```

## Platform notes

Discovery is pure userspace: ports + env vars + catalog + CLI behave
identically on Linux and macOS. macOS has no network-namespace isolation, so
all tasks share the host port space — dynamic ports avoid collisions; static
ports are first-come-first-served per node.

## What's next (planned)

DNS (`<service>.svc.legion` from a node-local resolver), template functions
(`{{ range endpoints "db" }}`), the `datapath` plugin ABI (bridge/CNI), the
per-node L4 forwarder with mTLS + intentions (`service.allow`), and the opt-in
L7 layer. See the [final plan](../internal_docs/plans/service-discovery-final-plan.md) §8.
