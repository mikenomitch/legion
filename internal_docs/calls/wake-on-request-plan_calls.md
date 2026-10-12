# Judgement calls: Wake on Request

Calls made while building [wake-on-request-plan.md](../plans/wake-on-request-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Wake keyed instances now, through a separate gateway process, instead of waiting for session mode

The sandbox-scaling plan put wake on request at Phase 7, after session mode
(Phase 6), because it was designed to wake sessions stored as bucket
manifests. This work ships it for keyed instances, which exist today, as a
new daemon: `legion start gateway`.

- **Alternatives.** (a) Wait for session mode, as the plan sequenced it.
  (b) Put the waker in the manager, next to `StartInstance`, so there is no
  new process. (c) Node-local wake on connect (the suspend plan's parked
  listener), so there is no new tier at all.
- **Trade-off.** One more process to run, with a token, and one more network
  hop on every request. When sessions ship, the gateway needs a second
  lookup path (the session index), which Phase 3 adds.
- **Why.** Keyed instances already suspend, resume, and idle out, and the
  missing piece for every agent platform is the request-to-resume step.
  Waiting buys nothing. In the manager, held requests would put client
  sockets, TLS for a public wildcard domain, and request bursts on the
  control plane's critical path (performance), and a manager is not
  something you put on the internet. A parked listener only works while the
  checkpoint is on the node that holds the port, which `durability =
  "store"` and node loss break. A stateless gateway scales by running more
  of it (simplicity), and is the tier §6.14 already named.

## 2. Opting in per task is the security boundary: no `wake`, no route

```nickel
suspend = { idle = { after = "3m" }, wake = { port = "http" } },
```

The gateway answers `404` for any instance whose task does not declare
`suspend.wake`, running or not, and dials only ports the task declares.

- **Alternatives.** Route to any keyed instance the gateway's token can read,
  and let ACLs decide.
- **Trade-off.** An author who wants a running, never-suspending instance
  behind the gateway still writes a `suspend.wake` block.
- **Why.** A gateway is reachable by whoever can reach it; its ACL token is
  the operator's, not the client's. Tying reachability to a line in the
  jobspec makes exposure a reviewed decision by the author, not a side effect
  of an operator's broad token. The same `404` for "no such instance" and
  "not opted in" also keeps the gateway from confirming which ids exist.

## 3. Name the instance by host or header, and the port by label, not `<port>-<id>` in the host

```text
Host: ws-7.sandboxes.example.com          → wake.port
Legion-Instance: ws-7  /  Legion-Port: admin
CONNECT ws-7:5432 HTTP/1.1
```

- **Alternatives.** E2B's `<port>-<id>.<domain>`, which sandbox-scaling
  §6.14 sketched.
- **Trade-off.** Reaching a second port by browser needs `Legion-Port`,
  which a browser cannot set; non-default ports are for programmatic clients
  and `CONNECT`.
- **Why.** Legion ports have labels and dynamic numbers, so `<port>-` would
  mean a label, and labels and instance ids can both contain `-`, making the
  split ambiguous. The default port lives in the jobspec, so the common case
  (one web port) is a bare instance host. Instance ids are case-sensitive and
  hosts are not, so the header form exists for ids that are not valid DNS
  labels.

## 4. Never wake a stopped instance

A `stopped` instance gets `503` naming `legion instance start <id>`, without
`Retry-After`.

- **Alternatives.** Fly's autostart, which starts stopped machines too.
- **Trade-off.** A platform that stops instances to save checkpoint storage
  must start them itself.
- **Why.** In Legion `stop` is an explicit act, and stopping a suspended
  instance discards its checkpoint. A request from the internet should not undo an
  operator's decision. Suspend is the sleep state; stop is off.

## 5. Splice after the first head instead of proxying each request

The gateway reads one request head, picks the instance, and then copies
bytes both ways until either side closes.

- **Alternatives.** A full HTTP proxy (hyper) that routes each request and
  can add `Forwarded` headers.
- **Trade-off.** All requests on a kept-alive connection go to the instance
  the first one named, and the instance sees the gateway as the client's
  address (open question in the plan).
- **Why.** Splicing gives WebSockets, streaming bodies, and any HTTP/1.1
  client for free, the same path serves `CONNECT` tunnels, and once the
  instance is up the gateway costs a copy loop, not a parser (performance).
  Browsers keep one connection per host, and the host is the instance, so the
  pinning matches how clients behave.

## 6. Readiness is "the port accepts a connection", not a health check

After the instance is `active`, the gateway dials its port until it accepts
(within the remaining timeout) and hands that connection to the client.

- **Alternatives.** Wait for the allocation's health checks to pass, or a
  wakeup probe like Substrate's that waits for HTTP 200.
- **Trade-off.** An app that binds before it is ready to answer may get the
  first request early.
- **Why.** A memory restore resumes a process that was already serving, so
  it accepts at once and a probe adds latency to the case wake is for. The
  dial needs no new jobspec field and works for any protocol. A task with
  health checks still gets them for its catalog entry.

## 7. Hold for `wake.timeout`, defaulting to 30 s, not a fixed short hold

- **Alternatives.** Substrate's 5 s hold when the pool is full; the
  sandbox-scaling sketch's `10s`.
- **Trade-off.** A client can wait up to 30 s by default before a `503`.
- **Why.** A resume that cannot use its checkpoint boots cold after
  `resumeTimeout` (30 s by default), and a blocked resume waits for capacity
  under the blocked-eval machinery. A shorter hold would return `503` for
  resumes that are about to succeed. Authors who prefer to fail fast set
  `timeout = "5s"`.

## 8. Cache running endpoints for 5 s, and dial stale ones while the manager is unreachable

- **Alternatives.** Ask the manager on every connection; or cache longer.
- **Trade-off.** For up to 5 s after an instance moves, a new connection may
  dial the old address once before the gateway asks again.
- **Why.** A browser opening a page makes many connections, and each would
  otherwise be a `GetInstance` on the manager (performance). The failed dial
  evicts the entry, so a move costs one extra dial. Serving a stale endpoint
  only when the manager cannot answer keeps running instances reachable
  through a control-plane blip, which is the operator job in the plan.

## 9. `legion start gateway` is not in the capability catalog and has no `--json`

- **Alternatives.** List it in `catalog()` like other commands.
- **Trade-off.** An agent does not discover the gateway from `legion
  capabilities`.
- **Why.** It is a daemon like `start manager` and `start worker`, which are
  not listed either and sit with it in the `NO_ENVELOPE` exemptions of the
  `every_command_accepts_json` test; it produces no output to structure. The agent-facing
  surface is the HTTP webhook, documented in `docs/agent-readiness.md`, and
  its errors use the standard envelope.
