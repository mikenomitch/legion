# Gateway: Wake on Request

`legion start gateway` is a front door for keyed instances. A request names
an instance, and the gateway sends it to the instance's port. When the
instance is suspended, the gateway resumes it, holds the request until the
instance accepts connections, and then forwards it. A sandbox that sleeps
after three idle minutes still answers its preview link a moment later, and
nobody has to call `legion instance start` first.

The gateway keeps no state of its own. It asks the manager where an instance
is and resumes it with `StartInstance`, so you can run as many gateways as
you like behind a load balancer and restart any of them at any time.

## Quick start

Opt the task in with `suspend.wake`, next to the idle policy that puts it to
sleep:

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    config = { image = "ghcr.io/acme/agent:latest", command = ["/start.sh"] },
    stateful = { mode = "keyed", storage = { path = "/workspace" } },
    network = { ports = [{ name = "http" }] },
    suspend = {
      checkpoint = "memory",
      idle = { after = "3m" },
      wake = { port = "http", timeout = "30s" },
    },
  }],
}
```

Start a gateway with a token that can read and start the job's instances
(`LEGION_ADDR` and `LEGION_TOKEN` work as they do for the CLI):

```console
$ legion acl policy apply gateway --rules '[{"resources": ["job"], "actions": ["read", "write"], "names": ["agents"]}]'
$ legion acl token create --name gateway --policy gateway
$ export LEGION_ADDR=https://manager.internal:4747 LEGION_TOKEN=<secret>
$ legion start gateway --listen 0.0.0.0:443 --domain sandboxes.example.com \
    --tls-cert wildcard.pem --tls-key wildcard-key.pem
```

Point `*.sandboxes.example.com` at the gateway. A request for instance
`ws-7` now goes to `https://ws-7.sandboxes.example.com`:

```console
$ legion instance get ws-7 | grep state
state: suspended
$ curl -s https://ws-7.sandboxes.example.com/
<html>... answered by ws-7, resumed from its checkpoint ...
$ legion instance get ws-7 | grep state
state: active
```

## The `suspend.wake` block

| Field | Default | Meaning |
| --- | --- | --- |
| `port` | required | The `network.ports` label requests go to. It must be declared. |
| `timeout` | `30s` | How long the gateway holds a request while the instance resumes. From `1s` to `5m`. |

Only a keyed task (`stateful.mode = "keyed"`) has a `suspend` block. The
gateway refuses every instance whose task does not set `wake`, running or
suspended, with the same `404` it gives an id that does not exist. Setting
`wake` is how an author decides an instance is reachable through the
gateway, and the gateway only ever dials ports the task declares.

Submit checks the port:

```text
Error: jobspec failed validation: task "workspace" sets suspend.wake.port = "admin", but it declares http
Hint: add { name = "admin" } to network.ports, or point wake.port at a declared port
```

The default timeout matches `suspend.resumeTimeout`, so a resume that cannot
use its checkpoint and boots cold instead still answers the held request.

## Starting a gateway

```text
legion start gateway [--listen ADDR] [--domain DOMAIN] [--tls-cert PEM --tls-key PEM]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `0.0.0.0:4780` | Where the gateway listens. |
| `--domain` | none | The wildcard domain. `<instance>.<domain>` names an instance. Without it, requests name the instance with the `Legion-Instance` header only. |
| `--tls-cert`, `--tls-key` | none | Terminate TLS with this chain (use a wildcard certificate for `--domain`) and key. Without them the gateway speaks plain HTTP, for use behind a load balancer that terminates TLS. |

The gateway reaches the manager at `LEGION_ADDR` with `LEGION_TOKEN`, and it
dials instances at their node's advertised address (the worker's
`workerAddress` host) and port, so it needs a route to the workers' network.

The token needs `job:read` (for `GetInstance`) and `job:write` (for
`StartInstance`) on the jobs it serves. There is no gateway-specific ACL.

## Routing

The gateway reads the first request head on a connection and picks an
instance:

| Request | Goes to |
| --- | --- |
| `Host: <instance>.<domain>` | the instance's `wake.port` |
| `Legion-Instance: <instance>` (any host) | the same. The header wins over the host. |
| `Legion-Port: <label or number>` added to either | that declared port instead of `wake.port` |
| `CONNECT <instance>:<label or number>` (or `<instance>.<domain>:<port>`) | `200 Connection Established`, then a raw TCP tunnel to that declared port |
| `POST /v1/wake/<instance>` | resume and wait, then answer with the address (see below) |
| `GET /v1/health` | `200`, for load balancer checks |

The two gateway paths apply only when the host does not name an instance, so
an app's own `/v1/health` is never shadowed. Only one label is taken from
the host: `a.b.<domain>` names nothing.

Hostnames are case-insensitive, so the host form suits instance ids that are
valid lower-case DNS labels. Use the `Legion-Instance` header for any other
id.

After choosing, the gateway forwards the head and copies bytes in both
directions until either side closes. Bodies, keep-alive requests, and
WebSocket upgrades pass through untouched. Every request on that connection
goes to the same instance.

`CONNECT` reaches any declared port, HTTP or not. A port can be named by its
label or by its advertised number, which is the `static` port when the task
pins one. With `network.ports = [{ name = "db", static = 5432 }]`:

```console
$ curl -p -x http://gateway:4780 telnet://ws-7:5432     # curl's tunnel through CONNECT
```

Most tools cannot send a label in `CONNECT`, so pin a static port for
non-HTTP ports you reach this way, or use a client that lets you write the
`CONNECT` line yourself.

### The webhook

`POST /v1/wake/<instance>` resumes an instance and returns once it accepts
connections, without forwarding anything. An agent or platform that talks to
the instance some other way uses it to wake it in one call:

```console
$ curl -s -X POST http://gateway:4780/v1/wake/ws-7
{"ok":true,"data":{"instance":"ws-7","address":"10.0.3.17:24312","port":"http","woke":true,"waitedMs":842}}
```

`woke` is false when the instance was already running.

## What happens on a request

1. The gateway asks the manager for the instance (`GetInstance`).
2. When it is `active`, the gateway dials its port and forwards.
3. When it is `suspended`, the gateway calls `StartInstance`. A gateway
   sends at most one start per instance every 10 seconds, so a burst of
   requests shares one resume. Another gateway's start is a no-op on the
   manager.
4. While the resume runs (`pending`), the gateway polls every 250 ms.
5. Once the instance is `active`, the gateway dials its port until it
   accepts. A memory restore accepts at once. A cold boot accepts when the
   app binds.
6. The held request is forwarded on that connection.

A running instance's endpoints are cached for 5 seconds, so a page that
opens many connections costs one manager call. A connection to a cached
endpoint that fails drops the entry and asks again.

**When the manager is unreachable**, the gateway still dials a running
instance it has seen before, so running instances stay reachable through a
short manager outage. It cannot wake anything until the manager answers.

## Errors

Every refusal closes the connection with a JSON body in the standard
envelope (see [agent readiness](agent-readiness.md)) and a
`Legion-Gateway-Error` header:

| Status | Code | When | `Retry-After` |
| --- | --- | --- | --- |
| `404` | `not_found` | No such instance, or its task does not set `suspend.wake` | no |
| `404` | `unknown_port` | `Legion-Port` or `CONNECT` names a port the task does not declare | no |
| `503` | `wake_timeout` | Still resuming when `wake.timeout` ran out | `5` |
| `503` | `not_runnable` | The instance is `stopped` or `terminated`, or the resume was refused (`onRestoreFailure = "fail"`, the job is stopped) | no |
| `502` | `manager_unavailable` | The manager could not be asked, and nothing was cached | `5` |
| `400`, `431` | `bad_request` | Not HTTP/1.x, or a head over 16 KiB | no |

```json
{"ok":false,"error":{"code":"wake_timeout","message":"instance \"ws-7\" was still resuming after 30012ms","hint":"retry after Retry-After seconds; raise suspend.wake.timeout if resumes routinely take longer","retryable":true}}
```

A **stopped** instance is never woken by a request. Stopping is an explicit
act, and stopping a suspended instance discards its checkpoint. The error
names `legion instance start <id>`.

## Idle and wake together

Traffic through the gateway reaches the instance's port like any other
client's, so the idle policy sees it the same way. A port the node datapath
fronts (an `ingress` chain, and every port of a sandbox) counts as `network`
and `connections` activity. For an `exec` or `container` task without an
ingress chain, the task binds the port itself and only `cpu` sees the
request, so add an ingress chain (even `ingress = { policy = "allow" }`) if
requests should keep it awake. See
[Idle policy](suspend-and-resume.md#idle-policy).

## Sandboxes

The gateway dials an instance's advertised port, and for a sandbox the node
datapath owns that port and carries connections into the microVM: over the
host-initiated vsock for a gated sandbox, and over the bridge for a TAP
sandbox. See [Ingress](network-handlers.md#ingress). Nothing in the gateway
is sandbox-specific. A memory checkpoint closes the guest's open
connections, so a client connected when the instance suspends sees its
connection close, and its next request wakes the instance.

## Limits

- HTTP/1.x only. HTTP/2 clients must fall back (the gateway's TLS offers
  only `http/1.1` in ALPN).
- The instance sees the gateway as the client. No `Forwarded` or
  `X-Forwarded-For` header is added.
- The gateway wakes keyed instances. It does not load-balance, and it does
  not route to slot-mode instances or services.
- No metrics endpoint yet. The gateway logs each wake (`Gateway woke
  instance`, with `waited_ms`) and each refusal at debug level.
- Certificates are read at start. Restart the gateway to rotate them.

## Reference

- Jobspec: `suspend.wake` in [suspend-and-resume.md](suspend-and-resume.md)
  and `configs/jobs/maximal.ncl`.
- API: `InstanceInfo.wake` and `InstanceInfo.endpoints` on `GetInstance`
  (`proto/legion.proto`).
- Design: `internal_docs/plans/wake-on-request-plan.md`.
