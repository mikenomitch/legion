# Plan: Wake on Request

> **Judgement calls:** [`calls/wake-on-request-plan_calls.md`](../calls/wake-on-request-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-12). Phase 1 shipped: the
`suspend.wake` block, `legion start gateway` with host, header, `CONNECT`,
and webhook routing, held requests with single-flight resumes, optional TLS,
and the wake fields on `GetInstance`. It works for every runtime whose ports
the node can reach, and for sandboxes through the datapath's sandbox ingress
([gated plan](gated-sandbox-checkpoints-plan.md) Phase 5). Deliberately
deferred and named: gateway metrics (Phase 2), waking `session`-mode
sessions from bucket manifests (Phase 3, after
[sandbox-scaling](sandbox-scaling-plan.md) Phase 6), node-local wake on
connect without a gateway (Phase 4, the suspend plan's §6.10 sketch), and
HTTP/2 to the gateway.

**Relationship to existing plans.** This plan executes the wake half of
[sandbox-scaling-plan.md](sandbox-scaling-plan.md) §6.14 (its Phase 7)
early, for **keyed instances**, which exist today, instead of waiting for
session mode. The gateway, its addressing, the webhook, and the held
request are the shapes §6.14 drew. What §6.14 adds for sessions (the
session index, resume from a manifest on any node) becomes Phase 3 here and
reuses this gateway unchanged. The suspend plan's node-local wake on connect
(§6.10 there) stays a separate, later mechanism (Phase 4).

---

## 1. Problem statement

A keyed sandbox that suspends when idle costs nothing while it sleeps, but
today it only wakes when something calls `legion instance start`. A platform
that serves a preview URL, a harness API, or a webhook receiver from inside
each sandbox has to run its own router: look the instance up, call start,
poll `instance get` until it is active, find the node and port, and only
then forward the request. Every team building agent sandboxes on Legion
writes that router, and each one gets the races wrong in its own way (two
requests start two resumes, a request is forwarded before the port binds, a
timeout leaves the client hanging).

Agent Substrate's router does this for its actors, Fly's proxy does it for
suspended Machines, and Cloudflare keeps WebSockets open while a Durable
Object sleeps. Legion has the suspend, the resume, and the idle policy, but
not the piece that turns an inbound request into a resume.

## 2. Prior art

- **Agent Substrate** ([repo](https://github.com/agent-substrate/substrate)).
  Its router resumes a suspended actor on a request, holds the request while
  the restore runs, and forwards it. When the pool is full it holds the
  request for up to 5 s instead of answering `503`. Other ports work through
  HTTP `CONNECT`. *Take:* hold-then-forward, `CONNECT` for non-HTTP ports.
  *Reject:* a fixed 5 s hold; the right hold depends on the workload, so it
  is a jobspec field.
- **Fly.io autostart** ([docs](https://fly.io/docs/reference/fly-proxy-autostop-autostart/)).
  `auto_stop_machines = "suspend"` and `auto_start_machines = true`: the
  proxy starts a stopped or suspended Machine on a request. *Take:* the proxy
  is the waker, and opting in is per service. *Reject:* waking *stopped*
  machines; in Legion `stop` is an operator's explicit act (calls entry 4).
- **E2B** ([docs](https://e2b.dev/docs)). Sandboxes are addressed as
  `https://<port>-<sandbox id>.e2b.app`, and paused sandboxes resume through
  the SDK. *Take:* a wildcard host per instance. *Reject:* the port in the
  host; Legion ports have labels, so the default port comes from the jobspec
  and `Legion-Port` or `CONNECT` names another (calls entry 3).
- **Cloudflare Durable Objects hibernation**
  ([docs](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)).
  The edge keeps the client connection while the object sleeps. *Take:* the
  gateway, not the workload, owns the waiting client.
- **Knative activator** ([docs](https://knative.dev/docs/serving/load-balancing/target-burst-capacity/)).
  Scale-from-zero requests buffer in the activator until a pod is ready.
  *Take:* readiness means "accepts a connection on the port", and buffering
  happens in a stateless tier. *Reject:* putting the activator in the data
  path once the target is up; the gateway splices and gets out of the way.
- **Nomad and Kubernetes** have no request-driven wake for a single
  stateful workload. Kubernetes `agent-sandbox` leaves it to the platform.

## 3. Jobs to be done, by persona

**Platform builder (job author)**
- When a user opens their agent's preview link after it went to sleep, I
  want the page to load a moment later rather than fail, so I can let
  sandboxes sleep aggressively without users noticing.
- When an external system calls a webhook my sandbox serves, I want the call
  to wake the sandbox and be answered, so I don't run a poller per sandbox.
- When I need a non-HTTP port (a database, a language server), I want the
  same wake to work, so I don't build a second path for it.

**Cluster operator**
- When I run the front door for thousands of sleeping sandboxes, I want it
  stateless and replaceable, so I can run several behind a load balancer and
  restart any of them without losing anything.
- When the manager is briefly unavailable, I want running sandboxes to stay
  reachable, so a control-plane blip is not an outage.

**Security / platform admin**
- When I expose a gateway to the internet, I want it to reach only instances
  whose authors opted in, and only their declared ports, so it is not a
  route into the cluster.

**Autonomous agent**
- When I hand a sandbox a task, I want one call that wakes it and tells me
  where it is, so I don't poll.

*Not in scope:* the on-call responder; nothing here changes how a broken
instance is diagnosed, and the gateway's errors name the instance and what
to run.

### User scenarios

1. A user returns to `https://ws-7.sandboxes.example.com` an hour later.
   The instance suspended after three idle minutes. The gateway resumes it
   from its memory checkpoint and the page loads about a second later.
2. Three browser tabs reload at once. One resume runs, and all three
   requests are answered by it.
3. A resume cannot be placed for longer than `wake.timeout`. The client gets
   `503` with `Retry-After: 5` and a JSON body that names the instance.

## 4. Goals and non-goals

**Goals**
- Opt-in per task, in the jobspec, next to the idle policy that puts the
  instance to sleep.
- A stateless gateway that holds requests during a resume, shares one resume
  across concurrent requests, and needs only an ordinary ACL token.
- HTTP/1.1 by host or header, any TCP port by `CONNECT`, and a webhook that
  wakes without a held request.
- Running instances stay reachable through a short manager outage.

**Non-goals**
- A public ingress controller (path routing, rewrites, rate limits, auth).
  Put one in front of the gateway if you need it.
- Waking stopped instances. `stop` is deliberate.
- Load balancing. A keyed instance is one allocation.
- HTTP/2 or HTTP/3 to the gateway.

## 5. Background: what exists today

- `StartInstance` on a `suspended` instance is a resume
  (`src/manager/server.rs`, `start_instance`), idempotent on a live
  instance, so any number of callers can send it.
- `suspend.idle` suspends a quiet instance (`docs/suspend-and-resume.md`).
- An allocation's `network` row carries the node's advertised host
  (`workerAddress`) and each port's advertised number
  (`src/storage/store.rs`, `worker_address_host`, `AllocNetwork.ports`).
- `GetInstance` had no endpoint or wake information, so a router had to read
  the allocation and the job spec separately.
- Sandboxes: a gated sandbox could not declare ports, and a TAP sandbox's
  ports were not mapped (gated plan Phase 5 closes both).

## 6. Proposed design

### 6.1 The jobspec

```nickel
suspend = {
  checkpoint = "memory",
  idle = { after = "3m" },
  wake = { port = "http", timeout = "30s" },   # new
},
network = { ports = [{ name = "http" }] },
```

- `wake.port` names a `network.ports` label. Submit refuses one that is not
  declared:

  ```text
  task "workspace" sets suspend.wake.port = "admin", but it declares http
  Hint: add { name = "admin" } to network.ports, or point wake.port at a declared port
  ```

- `wake.timeout` (default `30s`, from 1 s to 5 m) is how long the gateway
  holds a request. It matches `resumeTimeout`'s default, so a resume that
  falls back to a cold boot still answers.
- Without `wake`, the gateway answers `404` for the instance, whatever its
  state. Opting in is the security boundary (calls entry 2).

### 6.2 The gateway

```console
$ export LEGION_ADDR=https://manager:4747 LEGION_TOKEN=<gateway token>
$ legion start gateway --listen 0.0.0.0:443 --domain sandboxes.example.com \
    --tls-cert wildcard.pem --tls-key wildcard-key.pem
```

Routing reads only the first request head on a connection:

| Request | Goes to |
| --- | --- |
| `Host: <instance>.<domain>` | the instance's `wake.port` |
| `Legion-Instance: <instance>` (any host) | the same; wins over the host |
| `Legion-Port: <label or number>` | that declared port instead |
| `CONNECT <instance>[.<domain>]:<label or number>` | `200`, then a raw tunnel to that declared port |
| `POST /v1/wake/<instance>` (a host that names no instance) | resume, wait, answer JSON with the address |
| `GET /v1/health` (a host that names no instance) | `200` |

An instance host always wins over the gateway's own paths, so an app's own
`/v1/health` is never shadowed.

### 6.3 The hold

1. `GetInstance`. Not found, or no `wake`: `404`.
2. `active` with an endpoint for the port: dial it, splice.
3. `suspended`: `StartInstance`, at most once per instance per 10 s from
   this gateway. Concurrent requests on one gateway share that call; across
   gateways the manager treats a second start as a no-op.
4. `pending` or `rescheduling`: a resume is in flight (anyone's). Poll
   `GetInstance` every 250 ms.
5. Once `active`, dial the port until it accepts. A memory restore accepts at
   once; a cold boot takes as long as the app takes to bind.
6. Past `wake.timeout`: `503`, `Retry-After: 5`.
7. `stopped` or `terminated`: `503` without `Retry-After`, naming
   `legion instance start`.

Errors use the standard envelope:

```json
{"ok":false,"error":{"code":"wake_timeout","message":"instance \"ws-7\" was still resuming after 30012ms","hint":"retry after Retry-After seconds; raise suspend.wake.timeout if resumes routinely take longer","retryable":true}}
```

with `Legion-Gateway-Error: <code>`.

### 6.4 Endpoints, caching, and manager outages

`GetInstance` gains `endpoints` (port label, node address, advertised port)
and `wake` (port, timeout), set by `GetInstance` only so `ListInstances` pays
nothing. The gateway caches a running instance's endpoints for 5 s, and when
the manager cannot be reached it still dials a cached endpoint of any age.
Nothing can be woken while the manager is down.

### 6.5 After the splice

The gateway does not parse anything after the first head. Keep-alive
requests, bodies, and WebSocket upgrades pass through. The connection is
pinned to the instance its first request named. Traffic reaches the
instance's advertised port like any other client's, so the idle policy
counts it the same way (a datapath-fronted port counts as `network` and
`connections`).

### 6.6 Sandboxes

A gated sandbox's advertised port is fronted by the datapath, which dials
the guest over host-initiated vsock; a TAP sandbox's is fronted and dialed
on the bridge (gated plan §6.10). The gateway sees an ordinary
`address:port` either way.

## 7. Proto, struct, config, and persistence changes

- Jobspec: `suspend.wake = { port, timeout }` (`WakeConfig`), in
  `configs/jobs/maximal.ncl`.
- Proto: `InstanceInfo.endpoints` (`InstanceEndpoint { port, address,
  number }`) and `InstanceInfo.wake` (`InstanceWake { port, timeout_ms }`).
  No new RPC.
- CLI: `legion start gateway [--listen] [--domain] [--tls-cert --tls-key]`.
- ACL: none new. The gateway's token needs `job:read` and `job:write` on the
  jobs it serves.
- Persistence: none. The `suspend` column already stores the block.

## 8. Implementation phases

- **Phase 1 (shipped): the gateway.** Everything in §6, with unit tests over
  a fake manager and an end-to-end test against a real manager.
- **Phase 2: observability.** `legion_gateway_requests_total{outcome}`,
  `legion_gateway_wake_seconds`, held-request gauge, on a `/metrics` listener.
- **Phase 3: sessions.** When sandbox-scaling Phase 6 ships, resolve a
  session id through the session index and resume from its manifest. Same
  gateway, same jobspec block.
- **Phase 4: node-local wake on connect.** The suspend plan's parked
  listener, for clusters that do not want a gateway tier.

## 9. Testing plan

- `src/structs/suspend.rs`: `wake` resolves with defaults, refuses an
  undeclared port, an empty port, and out-of-range timeouts.
- `src/gateway.rs`: routing (host, header, `CONNECT`, webhook, health,
  nested hosts), one resume for three concurrent requests, the timeout
  answer, `404` for an instance without `wake` and an unknown one, a stopped
  instance not woken, the webhook body, `CONNECT` to a declared port and
  `404` for an undeclared one, TLS termination.
- `tests/wake_integration.rs`: a real manager, a suspended exec instance,
  the gateway resumes it through `StartInstance`, and the held request is
  answered by the resumed port.
- Not in CI: a real microVM resume behind the gateway (no KVM in CI).

## 10. Rollout and backward compatibility

New and opt-in. A task without `wake` is unaffected, and nothing runs until
an operator starts a gateway.

## 11. Open questions

- **Should the gateway pass the client address?** Lean: add
  `Forwarded` on the first request only once there is a way to apply it to
  every request on a kept-alive connection; until then the access log on an
  ingress chain records the gateway as the peer.

## 12. Acceptance criteria

- **Phase 1:** a request to a suspended instance with `wake` is answered by
  the resumed instance; three concurrent requests cause one
  `StartInstance`; an instance without `wake` is `404`; a resume longer than
  `wake.timeout` is `503` with `Retry-After`.
