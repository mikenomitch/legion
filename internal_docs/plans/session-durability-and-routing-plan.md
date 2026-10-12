# Plan: Session Durability and Routing Back

> **Judgement calls:** [`calls/session-durability-and-routing-plan_calls.md`](../calls/session-durability-and-routing-plan_calls.md)

**Status: PLANNED** (written 2026-10-12). Builds on session mode from
[sandbox-scaling-plan.md](sandbox-scaling-plan.md) Phase 6, which is in
flight in [#30](https://github.com/mikenomitch/legion/pull/30); every
`src/` citation below is at that PR's head (`86e0d24`), not `main`. This
plan replaces the parts of sandbox-scaling §6.4 and §6.5 that its Phase 6
deferred (calls entries 18 and 19 there) with a narrower order: survive a
worker restart, route by id without a lookup, then sleep on the node.
Deliberately deferred and named: a replicated session directory in raft
(Phase 4 here, built only past a trigger), workload identity for sessions
(sandbox-scaling §6.8), edge-started sessions (sandbox-scaling §6.3), and
waking sessions from a gateway, which
[wake-on-request-plan.md](wake-on-request-plan.md) Phase 3 owns and which
this plan feeds.

---

## 1. Problem statement

Session mode buys a start with no raft entry by keeping nothing about a
session anywhere but the process that runs it. That makes a session less
durable than it needs to be, in three ways a platform builder hits on day
one:

- **A worker restart ends every unpromoted session on the node**, even
  though the node, its envelope, and the processes themselves are fine.
  The worker holds sessions in memory only (sandbox-scaling calls entry 19;
  `docs/session-mode.md:371`), while ordinary tasks already outlive a worker
  restart and are re-adopted by pid (`docs/architecture-managers-workers.md:234`).
  A rolling worker upgrade is therefore a rolling session massacre.
- **A session cannot sleep.** Suspend, fork, and snapshot are refused on
  an unpromoted session (`docs/session-mode.md:139`), and a session task
  may not use `suspend.durability = "local"` at all
  (`src/structs/session.rs:408`). An agent sandbox that is idle for an
  hour either burns its envelope share the whole hour or is thrown away.
  "Come back to the same sandbox later" means promoting it, which is the
  raft write session mode exists to avoid.
- **Finding a session costs a cluster-wide fan-out.** Each manager indexes
  only the sessions of nodes that heartbeat to it
  (`src/storage/session_index.rs:1-14`), so `get`, `stop`, `start`, and an
  idempotent create on an id this manager does not index ask every peer
  (`src/manager/sessions.rs:71`, `:173`). A node silent for 120 s drops out
  of the index (`src/storage/session_index.rs:45`), and its sessions answer
  `NOT_FOUND`, indistinguishable from an id that never existed.

The question that prompted this plan was whether to write each session's
location to raft in batches, eventually consistent, so sessions get basic
durability and can be woken and routed back to their node if it is still
there. The answer this plan gives: the location is the cheap part, and it
can be carried in the id for free. What is missing is a session that still
exists to be routed back to, and that has to live on the node.

## 2. Prior art

- **Nomad client state.** The Nomad client keeps a local BoltDB of its
  allocations and task handles, and a restarted client reattaches to
  running tasks instead of restarting them. Legion's worker already does
  this for allocations through its JSON `WorkerStateStore`.
  - **Take:** a node-local record is what makes work survive the agent's
    restart. Raft is not involved.
- **Kubernetes kubelet.** Containers keep running across a kubelet
  restart, and the kubelet rebuilds its view from the runtime (CRI) and its
  checkpoint files.
  - **Take:** rebuild from what the runtime still holds, verify, adopt.
- **Fly Machines and `fly-proxy`.** A stopped or suspended Machine stays on
  its host and starts there again on the next request; a Machine on a dead
  host is gone unless it had a volume snapshot. Fly's routing state is
  replicated by gossip into SQLite on every proxy (Corrosion), eventually
  consistent, rather than through a consensus store.
  - **Take:** sleep on the host, wake on the host, and accept that the
    host's loss is the session's loss for the cheap tier.
  - **Take, for Phase 4 only:** if a cluster-wide directory is ever needed,
    it is a routing hint that tolerates staleness, not a record of truth.
- **Cloudflare Durable Objects.** An object takes a location hint when it
  is first created and lives in one place from then on; requests are
  routed to that home rather than to wherever the caller happens to be.
  - **Take:** decide the home at creation and make it cheap to find from
    the id. This plan goes one step further and puts the home in the id.
- **Borg and Omega.** Cells keep task state on the Borglet, and the master
  rebuilds from Borglet reports after failover.
  - **Take:** the node is the authority for what it runs; the control plane
    holds a rebuildable view. Session mode already follows this
    (sandbox-scaling calls entry 1).

## 3. Jobs to be done, by persona

**Platform builder (job author)**
- When a user comes back to an agent sandbox after a coffee break, I want
  them to land in the same sandbox with its files and processes, so the
  agent does not start over.
- When a sandbox has been idle for a while, I want it to stop costing CPU
  and memory without losing it, so I can afford to keep thousands of them
  around per node.
- When I upgrade the Legion worker, I want running sandboxes to keep
  running, so an upgrade is not an outage for my users.
- When a node dies, I want to be told clearly that its sandboxes are gone,
  so I can start fresh ones instead of retrying forever.

**Cluster operator**
- When session counts grow, I want the managers' disk and replication load
  to stay flat, so sessions cannot take the control plane down.
- When I roll workers, I want sessions to survive the same way tasks do,
  so the runbook has one rule.

**Autonomous agent**
- When I hold a session id from an earlier call, I want one call that
  either resumes it or tells me exactly why it cannot (`evicted`,
  `node lost`), so I can decide whether to retry or recreate without
  guessing.

*Not in scope:* the security admin persona. Nothing here changes who may
start, stop, or wake a session; the ACL scopes of session mode stay as
they are.

### User scenarios

1. **Back after lunch.** An agent sandbox goes idle for 3 minutes and the
   node suspends it to local disk, freeing its envelope share. Forty minutes
   later the platform calls `legion instance start <id>`. The manager reads
   the node from the id, the node restores from its local checkpoint, and
   the sandbox is back with its memory intact. No raft entry was written at
   any step.
2. **A worker upgrade.** The operator restarts the worker on `w-17`, which
   runs 800 sessions. The worker comes back, reads its session log, finds
   every process still alive, and re-adopts them. The managers see the
   sessions again on the first full report.
3. **A dead node.** `w-17` loses power. A platform calls `start` on one of
   its idle sessions. The manager answers `UNAVAILABLE` with "node w-17 is
   down; sessions held only on it are lost unless it returns". With
   `durability = "store"`, the manager instead finds the session's
   manifest in the bucket and resumes it on another node.

## 4. Goals and non-goals

**Goals**
- Unpromoted sessions survive a worker restart (Phase 1).
- Any manager routes a session id to its node with no index lookup and no
  peer fan-out in the common case (Phase 2).
- Unpromoted sessions can suspend on their node and resume there, with
  `durability = "local"` (node-held, lost with the node or on eviction) or
  `"store"` (also published, resumable anywhere) (Phase 3).
- Every answer about an unreachable session says why: `evicted`,
  `node lost`, `node unreachable`, or `not found`.
- Zero raft entries per session for start, suspend, resume, and stop,
  unchanged from session mode.

**Non-goals**
- **A raft row per session**, batched or not, in the default path. Phase 4
  describes one, gated behind a measured trigger (§6.5).
- **Surviving node loss without a published checkpoint.** A session held
  only on a node dies with it; that is what makes it cheap.
- **At-most-one copy.** Converge-to-one stays (sandbox-scaling calls
  entry 25). Promote for exactness.
- **Gateway wake on request.** Owned by wake-on-request Phase 3; this plan
  gives it a node to route to.

## 5. Background: what exists today

- **Starts go through a manager.** The router picks the envelope with the
  most room from a one-second view built from raft allocation rows and the
  merged session index, then calls the worker's `PostWork`
  (`src/manager/sessions.rs:368-406`, `:476-605`). Edge starts are refused
  (`docs/session-mode.md:365`).
- **The session index is soft, per manager.** Nodes report deltas against
  an epoch with a full report every 30 beats
  (`src/storage/session_index.rs:16-24`). Reads merge this manager's index
  with every peer's (`src/manager/sessions.rs:71-150`).
- **Ids are minted with no record.** `{task}-{26 base32}`, 130 random bits
  (`src/structs/instance.rs:296`); `is_minted_instance_id`
  (`:310`) gates every session lookup, so a caller id never fans out.
  Idempotent creates derive the id from the key
  (`src/structs/session.rs:135`).
- **Start on a running session is a no-op**, and on an unknown id is
  `NOT_FOUND` (`src/manager/server.rs:5721-5735`).
- **The worker holds sessions in memory.** Ending one is routine
  (`src/worker/task_manager.rs:2992`); a session whose envelope leaves the
  node is ended after 30 s (`:355`). Nothing is written to disk.
- **Allocations survive a worker restart** through the JSON
  `WorkerStateStore`, re-adopted by pid and start time
  (`docs/architecture-managers-workers.md:234-256`).
- **Published checkpoints already do "same node if cached, else any
  node".** After upload the local parts become evictable cache entries,
  and a resume prefers the node that still holds them through the
  `locality` score (`docs/suspend-and-resume.md:733-775`).
- **Session suspend is refused** at submit, both `durability = "local"`
  and a `reserve` (`src/structs/session.rs:404-420`).

## 6. Proposed design

### 6.1 The session log on the node (Phase 1)

The worker writes each session's lifecycle to `sessions.db`, a SQLite
database beside its state directory, as sandbox-scaling calls entry 37
proposed: one row per live or sleeping session (id, job, job config, task,
envelope allocation, state, pid and start time, lease `iat`, checkpoint
key if suspended), written in WAL mode and fsynced in groups every 50 ms,
so a start costs no synchronous disk flush on the start path. A start the
log has not flushed when the worker dies is re-adopted anyway if its
process is alive: the worker scans its runtime's children on boot and
adopts any session-shaped claim it can verify against the envelope.

On restart the worker:
1. Reads `sessions.db` and verifies each running session's pid and start
   time, as it does for allocations.
2. Adopts the live ones, ends the rows whose process is gone, and keeps
   suspended rows as they are.
3. Sends a full session report on its first heartbeat; managers already
   ask for one from a node they have not heard from.

A session whose envelope is gone after the restart follows the existing
30 s orphan rule.

### 6.2 The node is in the id (Phase 2)

A minted session id carries a short tag of the node it started on:

```text
workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h
          └ node ┘ └──── 130 random bits ───┘
```

- The tag is 8 base32 characters (40 bits) of a keyed hash of the node id,
  so it does not spell the node's name and 10^4 nodes collide with
  probability about 5 × 10^-5. A collision costs one extra probe, never a
  wrong answer, because the node is asked and answers for itself.
- Every manager resolves a tag from the `nodes` table it already holds
  (a local read of replicated rows, cached).
- `get`, `stop`, `start`, and `promote` on a session id go to the tagged
  node first, over the worker plane. The node's answer is authoritative.
  The index fan-out stays as the fallback for a tag that resolves to no
  node, a node that answers "not here", and listings.
- Derived ids (idempotency keys) cannot know their node before placement.
  They keep the untagged shape and the fan-out path, which is the price of
  a retry-safe create; a platform that wants O(1) routing reads the
  returned id and uses it.

This answers "can I route back to the same node while it is alive" with
no write anywhere.

### 6.3 Sleeping on the node (Phase 3)

Session tasks may suspend. The jobspec surface is today's `suspend` block,
with the session refusals at `src/structs/session.rs:404-420` relaxed:

```nickel
stateful = { mode = "session", envelope = { resources = { cpu = 30000, memory = 61440 } } },
suspend = {
  checkpoint = "memory",
  durability = "local",          # or "store": also publish, resume anywhere
  idle = { after = "3m" },
  keepFor = "24h",               # new: how long a node holds a local-only checkpoint
},
```

- **Suspend is decided and done on the node.** The node's idle policy (or
  `legion instance suspend <id>`, routed by tag) captures the checkpoint,
  frees the session's envelope share, records `suspended` in `sessions.db`,
  and reports it in the next delta. The checkpoint is charged to the node's
  `snapshots` content budget, as a keyed instance's local checkpoint is.
- **Eviction.** A local-only checkpoint is pinned until `keepFor` expires,
  the job stops, or `minFreeDisk` pressure forces it out, oldest first.
  The node keeps the evicted id in `sessions.db` for `keepFor` again, so
  the answer is `evicted`, not `not found`. With `durability = "store"` the
  checkpoint is also published to
  `<contentStore.url>/sessions/<job>/<id-prefix>/<id>.json`
  (sandbox-scaling §6.5); after publish the local copy is an ordinary cache
  entry, and eviction just means the next resume fetches the parts.
- **Resume.** `legion instance start <id>`:

```console
$ legion instance start workspace-q4n7c2rd.7k2q... -j
{"ok":true,"data":{"message":"Instance workspace-q4n7c2rd.7k2q... resumed",
 "node":"w-17","resumingFrom":"local","deduplicated":false}}

$ legion instance start workspace-q4n7c2rd.9m3x... -j   # evicted, local only
{"ok":false,"error":{"code":"NOT_FOUND",
 "message":"session workspace-q4n7c2rd.9m3x... was evicted from w-17 at 2026-11-02T18:04:11Z (disk pressure); a local-only session cannot be restored",
 "hint":"start a new session, or set suspend.durability = \"store\" to keep idle sessions in the content store","retryable":false}}

$ legion instance start workspace-r8p2k5ta.4h6w... -j   # its node is down
{"ok":false,"error":{"code":"UNAVAILABLE",
 "message":"node w-23 holds session workspace-r8p2k5ta.4h6w... and has not been heard from for 3m12s",
 "hint":"the session is lost if w-23 does not return; with durability = \"store\" it would resume elsewhere","retryable":true}}
```

  The manager routes by tag (§6.2). The node restores from its local
  checkpoint if its envelope has room, and refuses with `ENVELOPE_FULL`
  otherwise. On a full envelope, an eviction, or a lost node, a `store`
  session falls back to its manifest and the router places it like a new
  start, preferring nodes that hold its parts. A resumed session keeps its
  id; when it lands on another node the tag is stale, and §6.2's fallback
  finds it.
- **Duplicates.** A resume that raced to two nodes converges to one by the
  existing rule (earliest lease wins); the resume refreshes the lease.

### 6.4 What each failure costs

| Event | `durability = "local"` session | `durability = "store"` session |
| --- | --- | --- |
| Worker restart | Running and sleeping sessions kept (Phase 1) | Same |
| Manager failover | Nothing lost; routing by tag needs no index | Same |
| Node silent, comes back | Sessions kept; answers `UNAVAILABLE` meanwhile | Resumes elsewhere if asked meanwhile, and the copy on the returning node is the later duplicate and is stopped |
| Node lost | Lost, answered as `node lost` | Idle ones resume elsewhere from the manifest; running ones are lost |
| Eviction | Lost, answered as `evicted` for `keepFor` | Parts fetched back on the next resume |

### 6.5 The replicated directory, and why it is Phase 4 (conditional)

The proposal on the table: managers fold session reports into a `session`
table in raft, flushed in batches, eventually consistent, so any manager
knows where every session lives and that it existed.

**The write path that would work.** Each manager buffers the deltas of its
own nodes and flushes one status-lane entry per second or per 5,000
changes, whichever comes first, as an upsert/delete changeset. Status-lane
writes do not contend with the scheduler lane that bounds keyed
lifecycles (`docs/multi-raft.md`, lanes). A node's full report reconciles
its rows. A node marked lost has its rows flipped to `lost` in one entry.

**What it costs.** At 1,000 starts and 1,000 ends a second, that is one
entry a second per manager carrying about 2,000 row changes (roughly
150 KB of changeset), which raft handles easily. The cost is the rows:
every manager stores every node-held session, and every snapshot carries
them. At 10^6 sessions that is about 150 MB per manager; at the plan's
10^8 idle target (sandbox-scaling calls entry 35) it is 15 GB, past the
4 GiB store trigger in sandbox-scaling §6.11. Idle sessions at that count
must stay in the bucket, so the directory could only ever cover node-held
sessions.

**What it would buy, once Phases 1 to 3 exist.**
- Routing: nothing over the id tag, which is exact at mint and needs no
  write. A resumed-elsewhere session is the only stale-tag case, and the
  fan-out covers it.
- Durability: nothing. The row points at a session; if the node lost it,
  the row is a pointer to nothing.
- "It existed, and its node is gone" for a session on a silent node: the
  tag already says which node, and the node table says it is down.
- An exact listing of sleeping sessions while their node is unreachable:
  the one real gain.

**Trigger.** Build Phase 4 if a platform needs complete listings across
node outages, or if fan-out reads measurably load managers (more than 10%
of a manager's CPU at the target rate in the three-manager run). Until
then, every session-sized write stays off raft.

## 7. Proto, struct, config, and persistence changes

- **Worker disk:** `sessions.db` (new, SQLite, WAL), owned by the worker.
- **Ids:** the minted session id gains the `<tag>.` segment; derived ids
  are unchanged. `is_minted_instance_id` accepts both shapes, and caller
  ids may not take either (breaking, per `AGENTS.md`).
- **Proto:** `SessionStatus.state` gains `suspended`;
  `SessionStatus.checkpoint` (durability, bytes, published URL) is new;
  `Worker.ResumeSession(ResumeSessionRequest) → ResumeSessionReply` and
  `Worker.SuspendSessions` are new worker-plane RPCs, manager-only like
  `StopSessions`; `Worker.GetSession` answers a routed `get` and reports
  `evicted` with a time and reason.
- **Jobspec:** `suspend.keepFor` (new, default `"24h"`, session tasks
  only), in `configs/jobs/maximal.ncl`. `durability = "local"` becomes
  valid on session tasks.
- **No raft schema change** in Phases 1 to 3.

## 8. Implementation phases

1. **Sessions survive a worker restart.** `sessions.db`, adopt on boot,
   full report after. Valuable alone: worker upgrades stop killing
   sessions.
2. **The node in the id.** Tagged mint, tag resolution, tag-first routing
   for `get`/`stop`/`start`/`promote`, fallback to fan-out. Valuable alone:
   per-request cost no longer grows with the manager count, and a silent
   node's sessions answer `UNAVAILABLE` naming the node instead of
   `NOT_FOUND`.
3. **Sleeping on the node.** Session suspend (`local` and `store`), the
   node's idle policy for sessions, `keepFor` and eviction answers, resume
   by tag with manifest fallback. Valuable alone: idle sessions stop
   costing envelope room. Unblocks wake-on-request Phase 3.
4. **Replicated directory (conditional, §6.5).** Only past its trigger.

## 9. Testing plan

- **Phase 1:** a worker-restart integration test in
  `tests/session_integration.rs` (fake node over the real worker plane,
  restarted mid-run): sessions adopted, ended processes dropped, full
  report on first beat. Unit tests for the log's group commit and recovery
  from a truncated WAL.
- **Phase 2:** routing tests asserting zero `QuerySessions` peer calls for
  a tagged id in a three-manager cluster; tag collision forced in a unit
  test; stale tag after a resume falls back.
- **Phase 3:** suspend and resume round trip on the same node (`exec`
  runtime with a test checkpoint; KVM memory checkpoints in the KVM suite);
  eviction under `minFreeDisk` answers `evicted`; `store` fallback when the
  node is fenced; zero raft entries across start, suspend, resume, stop
  (applied index summed over lanes, as sandbox-scaling §16.3 measures).
- **Benchmarks:** the start-rate measure of sandbox-scaling §16.3 rerun
  with Phase 1's log enabled, base and branch, to show the log stays off
  the start path.

## 10. Rollout

No installed base: tagged ids replace the current shape outright, and
existing sessions (memory only) do not survive the worker upgrade that
installs Phase 1, which is the last time that happens. Managers before
workers, as always. Public docs: `docs/session-mode.md` gains "Worker
restarts", "Routing", and "Idle sessions" sections, and its "What you give
up" table and Limitations change with each phase.

## 11. Open questions

- **Checkpoint seal keys without raft.** Published keyed checkpoints are
  sealed with a key the manager sends and records. A session has no row
  to record it in. Lean: derive it, `HMAC(cluster key, session id ‖
  checkpoint seq)`, so any manager can re-derive it for a resume.
- **Should the tag be the envelope allocation id instead of a node hash?**
  It is already a replicated row, so no new mapping is needed, but a job
  update replaces envelopes in place and would stale every tag on the
  node. Lean: node hash.
- **`keepFor` default.** Lean: 24 h, matching the gateway's typical
  "come back later" window; operators shorten it under disk pressure.

## 12. Acceptance criteria

- **Phase 1:** restarting a worker with 1,000 sessions leaves all 1,000
  running and listed within one full-report period; the §16.3 start rate
  is within 5% of the base.
- **Phase 2:** a tagged `get` in a three-manager cluster makes no peer
  call; a session on a silent node answers `UNAVAILABLE` naming its node.
- **Phase 3:** an idle session suspends on its node, frees its envelope
  share (`legion node usage`), and resumes on the same node from local
  disk; an evicted one answers `evicted`; a `store` session on a fenced
  node resumes elsewhere; zero raft entries for the whole cycle.
