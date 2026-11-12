# Launch blockers, round 2 (2026-11-12)

> **Judgement calls:** [launch-blockers-round-2-plan_calls.md](../calls/launch-blockers-round-2-plan_calls.md)

**Status: SHIPPED** on branch `claude/bold-bohr-c13jno`, except what §9 names
as deferred. Covered by unit and integration tests only; nothing here has run
on the AWS rig.

Written after the fact, like
[launch-blockers-and-security-gaps-plan.md](launch-blockers-and-security-gaps-plan.md),
for one batch of the open "Launch blockers" items in [TODO.md](../TODO.md):
the ones fixable and testable without real hardware. Several change a
security boundary, a default, or an on-disk file, which is why they get a plan
and a calls record.

## 1. Problem

1. **OCI and S3 fetches were judged but not pinned.** oci-client and
   object_store build their own reqwest clients, re-resolve the host, and
   follow redirects (a blob CDN, the token `realm`, an S3 `307`) unjudged. A
   registry that passes the host guard could redirect a fetch to cloud
   metadata.
2. **The manager's pin lookups had no `hostGuard.allow`**, so a dev registry
   on the manager's host could not be pinned at submit, and the OCI `HEAD`
   was unpinned like a worker's pull.
3. **`legion net policy test` judged only the built-in classes**, never a
   node's own addresses or its `hostGuard` entries.
4. **A worker under a new `--name` in the same data dir only warned** that
   another identity left tasks running; they ran twice (P1-5).
5. **Unratified provisional claims were never persisted**, so a worker crash
   left them running with nothing to stop them.
6. **A task that adds a capability placed anywhere** and failed at start on a
   node whose `container.allowCaps` refused it.
7. **Workers failed over to raft advertise addresses**, which may sit on a
   peer network workers cannot reach.
8. **Removed-allocation history (24h) and worker log retention (7 days)
   disagreed**, so logs the worker still had were unreachable through the
   manager.
9. **A destructive static-port rollout raced the old process's graceful
   stop**: the new process failed to bind and went through its restart
   policy.
10. **Image pulls**: a hung registry held every other start of the image (no
    read timeout); native roots for a TLS-intercepting proxy were believed
    missing.

## 2. Prior art

- **Forward proxies as an egress pin.** Squid/Envoy egress gateways and
  Kubernetes' `HTTPS_PROXY` convention: clients that do their own DNS can be
  held to a policy by routing them through a proxy that resolves and judges.
  Smokescreen (Stripe) is exactly this for SSRF: a CONNECT proxy that refuses
  private and metadata addresses after resolution.
- **Nomad** advertises driver capabilities as node attributes
  (`driver.docker.privileged.enabled`) and lets jobs constrain on them;
  `allow_caps` is enforced at start. We take the attribute and add the
  constraint implicitly.
- **Kubernetes** assumes one kubelet per node and takes no lock of its own;
  containerd and etcd take `flock`s on their data dirs, which is the model for
  the identity lock.
- **Nomad** client advertises `advertise.http` for servers separately from
  `advertise.rpc`/`serf`; workers dialing an API address distinct from the
  peer address is the same split.

## 3. Jobs to be done

- **Operator:** "A job cannot reach cloud metadata through a registry
  redirect." "A dev registry on my manager host can be pinned at submit."
  "Re-running the installer under a new name does not leave the old tasks
  running twice." "A crashed worker does not leak sandboxes." "My managers
  peer on a private network; workers still fail over."
- **Job author:** "A task that needs `NET_ADMIN` lands where it can start."
  "A rollout of my static-port service does not crash-loop the new version."
  "Yesterday's failed allocation's logs are still readable this week."
- **Agent:** "`net policy test` tells me when a target is a node's own
  address before I deploy."

## 4. Goals and non-goals

Goals: fix each item above with a regression test that fails without the fix.

Non-goals: the microVM HTTP ingress relay, a vendored seccomp allowlist,
cross-store content reads, pre-vote, folding partial placement into blocked
evals, unifying `command`/`ENTRYPOINT` across OSes, caching extracted
container rootfs, and the scheduler benchmark run for reschedule holds (all
left open in TODO.md).

## 5. Design, per item

1. **Guarded proxy** (`crates/worker/src/artifacts/guard_proxy.rs`). One
   loopback listener per `HostGuard`, on a two-thread runtime of its own,
   speaking `CONNECT` and absolute-form HTTP. Each connection is resolved,
   judged, and dialed only to a judged address; a refusal is `403` with
   `Legion-Denied-By: host-guard` and is filed under the client's
   `Proxy-Authorization` user, so `guard_proxy::explain` adds the guard's
   reason to a failed fetch. OCI clients (cached per credential) get
   `https_proxy`/`http_proxy` and a 60s `read_timeout`; S3 stores get
   `with_proxy_url` with the credential endpoints excluded. Skipped when the
   environment names its own proxy.
2. **Manager guard**: `artifacts.hostGuard` (`HostGuardConfig`) in the manager
   config, validated at boot (`counterInterval` refused), built once per
   service and shared so its proxy and clients are reused.
3. **Node guard rules**: workers advertise `hostGuardRules` (allow, deny,
   non-loopback interface addresses). `TestNetPolicy` judges the target with
   each advertising node's rules in the pool: all refuse → `host-guard` deny;
   some refuse → a note on the explanation.
4. **Identity lock**: `workers/<name>/worker.lock`, `flock`ed for the
   process's life (`hold_identity_lock`); a second process under the same
   identity fails to start. At startup, other identities with live tasks and
   a free lock are stopped (`stop_abandoned_identity`) and their state set
   aside as `worker_state.json.abandoned-<ts>`.
5. **Provisional record**: `provisional_claims.json` beside the state file,
   rewritten (no fsync) when the set of unratified non-session claims with a
   pid changes; `restore_allocations` stops each recorded process first.
6. **Capability placement**: workers advertise `container.allowCaps`
   (expanded; every name on macOS); the container plugin's `normalize_task`
   adds `container.allowCaps contains "CAP_X"` per capability outside the
   default set.
7. **Failover addresses**: `worker_manager_addresses` maps each raft member
   to its published config row's `server.advertiseAddr` when set; cached 30s.
8. **History retention**: `ALLOCATION_HISTORY_RETENTION_SECS` follows
   `DEF_TASK_LOG_RETENTION_DAYS` (7 days); each job keeps its newest 1000.
9. **Static ports**: `begin_task_allocation_gated` returns `InFlight` while
   another allocation on the node (running, draining, or terminating) holds
   one of the payload's static ports; the next reconcile starts it.
10. **Image pulls**: the read timeout above; the reqwest
    `rustls-tls-native-roots` feature, already on through object_store, is
    named in the workspace.

## 6. Format and surface changes

- Manager config: `artifacts.hostGuard`.
- Node attributes (structural): `hostGuardRules`, `container.allowCaps`.
- Worker data dir: `workers/<name>/worker.lock`,
  `worker_state.json.abandoned-<ts>`, `provisional_claims.json`.
- `TestNetPolicyReply.explanation` may end with the nodes that refuse.
- `HeartbeatReply`/`WorkerRegisterReply.manager_addresses` prefer
  `server.advertiseAddr`.
- Removed-allocation history: 7 days, 1000 per job.
- No proto shape changes; comments updated.

## 7. Testing

Unit: `guard_proxy` (refusal, explanation, forwarding, redirect judged on its
own, unlabeled requests), OCI redirect to a guarded registry, host-guard rule
round trip, `TestNetPolicy` over advertised rules, identity lock and
abandoned-identity stop, provisional crash and kill, capability constraints,
manager address mapping, history cap, static-port hold. Integration:
`manager_host_guard_allow_opens_a_pin_lookup`.

## 8. Rollout

No installed base. What breaks, on purpose:

- A second worker process on the same identity fails to start.
- A worker under a new name stops another (unlocked) identity's tasks.
- A container task that adds capabilities no longer places on nodes that
  would refuse it (it shows unplaced instead of failing).
- OCI/S3 fetches on a guarded worker go through a loopback proxy unless the
  environment names a proxy.

## 9. Deferred

- Scheduler benchmark numbers for reschedule holds (needs two release builds;
  not run in this change).
- A plain-`http` cross-origin redirect's refusal carries no label, so the
  fetch error lacks the guard's reason.
- A killed claim still inside its graceful stop when the worker crashes is
  not recorded.
- Node addresses in `hostGuardRules` are as of registration.
- Everything here needs the AWS rig re-run.
