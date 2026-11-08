# Watches, Revisions, and Blocking Queries

Every read of a job, an allocation, or a KV key tells you how new it is (a
**revision**), and you can ask Legion to tell you when it changes instead of
polling for it:

- a **blocking read** waits until what it returns has changed after a
  revision you pass, like Nomad's `?index=` queries;
- a **watch** streams each change as it commits, and resumes after a
  revision when it reconnects, like a Kubernetes list-and-watch.

The same revisions make a write and a read on any manager agree, give
`legion job run` a compare-and-set, and let a worker resume its work stream
on another manager after a failover without being sent everything again.

## Revisions

A revision is a manager's **applied position**: everything committed at or
below it is in what that manager reads.

- In `local_disk` mode it is a counter the store moves on every committed
  write to a job, allocation, or KV key. It is persisted in blocks, so it
  never goes backwards across a restart.
- In raft mode it is the raft log's applied index. Under the default
  `single` topology that is one number, e.g. `4812`. Under
  `topology = "lanes"` there are three raft groups, and a revision is their
  three applied indexes, Control, Scheduler, and Status:
  `4812.90211.120034`.

Because it is the replicated log's position, a revision means the same thing
on every manager, and it never goes backwards across a leader failover.

**Treat a revision as opaque.** Carry it, pass it back exactly as you got it,
and compare two for equality. Do not add to it or parse it: its shape depends
on the topology.

Replies that carry `revision`:

| RPC | CLI | What the revision covers |
| --- | --- | --- |
| `GetJobInfo` | `legion job info --json` | the job: row, versions, allocations, task outcomes |
| `ListJobs` | | every job |
| `ListInstances` | | the job's allocations and instances |
| `GetLedger` | | every allocation |
| `GetKv` | `legion kv get --json` | the key |
| `ListKv` | | the keys under the prefix |
| `RunJob` | `legion job run --json` | the revision at which the submit is visible on the manager that answered |
| `PutKv`, `DeleteKv` | | the same, for the write |

## Blocking reads

Pass a revision back to wait for the next change:

```console
$ legion job info web --json | jq -r .revision
4812
$ legion job info web --index 4812 --wait 60 --json   # returns when web changes, or after 60s
$ legion kv get app/config --index 4812 --json
```

Over gRPC, the six reads in the table above (all but the writes) take three
optional request headers:

| Header | Meaning |
| --- | --- |
| `x-legion-min-revision: <revision>` | Wait until an object the read would return changed after this revision, then answer with the current state and its `revision`. |
| `x-legion-wait: <seconds>` | How long to wait. Default and maximum 300 (5 minutes, Nomad's default). |
| `x-legion-consistency: linearizable` | See [Consistency](#consistency). `default` is the same as no header. |

How a blocking read behaves:

- It returns as soon as something it would return changed after the
  revision: for `GetJobInfo`, the job's row, its versions, its allocations,
  or its task outcomes (everything its derived status reads); for
  `ListJobs`, any job; for `ListInstances`, the job's allocations; for
  `GetLedger`, any allocation; for `GetKv`, the key; for `ListKv`, any key
  under the prefix.
- When nothing changes within the wait, it answers anyway with the current
  state and revision. Compare the revisions to tell a change from a timeout.
- A revision older than the manager's journal (below) answers at once: the
  manager cannot prove nothing changed, so it lets you re-read.
- A revision *ahead* of this manager (you read it from another, newer
  manager) waits for this manager to catch up first.
- A malformed revision is `INVALID_ARGUMENT`.

The read itself is the normal read: same ACL checks, same filters. Waking is
what the revision buys you, not a different answer.

## Watches

```console
$ legion job watch web
Watching (Ctrl-C to stop)...
4813  job web  changed  status=pending version=7
4820  job web  changed  status=running version=7
$ legion job watch web --allocations --node node-3
$ legion kv watch app/ --from 4812 --json
{"ok":true,"data":{"revision":"4812","progress":true,...}}
{"ok":true,"data":{"revision":"4831","changes":[{"kind":"WATCH_KIND_KV","key":"app/config",...}],...}}
```

`Manager.Watch` is a server-streaming RPC. A `WatchRequest` names the
`kind` (job, allocation, or KV) and optional filters: `job` and `namespace`
for jobs and allocations, `node_id` for allocations, `prefix` for KV, and
`from_revision` to resume.

- The **first message** is always a progress event (`progress = true`) with
  the revision the stream starts from.
- Each later message carries the **changes** since the last one, oldest
  first, one per object, and the `revision` to resume from. Each change
  names the object, says whether it was `deleted`, and carries the object as
  it reads when the message is sent: the job (with its derived status and
  `version`), the KV entry (value included), or the allocation (status, node,
  job, task). Several changes to one object between two messages are sent
  once, at its newest state.
- With no change for 30 seconds, a progress event carries the current
  revision, so a client always has a recent place to resume from.
- A job counts as changed when anything its `GetJobInfo` reads changed: its
  row, its versions, its allocations, or its task outcomes. Watching a job is
  how you see its status move.

### Resuming

Reconnect with `from_revision` set to the last `revision` you received and
you get every change after it, and nothing at or before it, on any manager.

When `from_revision` is older than the manager can serve, the first and only
message has `resync_required = true` and `oldest_revision`, and the stream
ends. List again (the list's reply carries `revision`), then watch from that
revision: the same contract as a Kubernetes watch answered `410 Gone`. The
CLI reports this as the typed error `ResyncRequired`.

### The journal and its window

Each manager keeps a **journal** of the newest 65,536 changes to jobs,
allocations, and KV keys, in memory, fed by the same projection that keeps
its read model current. It starts empty, at the manager's applied revision,
when the process starts. So:

- A watch or blocking read can reach back as far as the window: at heavy
  churn (thousands of allocation changes a second) that can be under a
  minute; on a quiet cluster it covers everything since the manager started.
- After a manager restarts, resumes from before the restart get
  `resync_required` *from that manager*. Another manager that kept running
  can still serve them.
- A raft snapshot install on a manager (a follower too far behind to catch
  up from the log) empties its journal and ends every open watch on it with
  `resync_required`.
- Under `lanes` the window is shared by all three groups, so heavy status
  churn can roll a KV watch's window too.

### ACLs

Opening a job or allocation watch needs `job:read`, and a KV watch
`kv:read`, with any scope. Each change is then filtered to what you can read
(the job's scope, or the key's path), so a narrow token sees only its own
objects, and an object you cannot read never appears.

## Consistency

By default a read answers from the replica of the manager you asked, which
can trail the leader by the replication delay (normally milliseconds).

- **Your own writes are always visible.** A write sent to a follower is
  forwarded to the leader, and the follower holds the reply until its own
  replica has applied the write (up to 5 seconds), so a write followed by a
  read on the same manager sees the write. A follower more than 5 seconds
  behind replies anyway; the write committed, but its next read may not show
  it yet.
- **Linearizable reads** (`x-legion-consistency: linearizable`, `--consistent`
  on the CLI) include every write committed before the read began,
  whichever manager took it. The leader confirms its leadership with a
  quorum round and catches up before reading; a follower forwards the read
  to the leader. It costs one quorum round trip per read.
- `local_disk` managers are always linearizable.

## Compare-and-set on jobs

`Job.version` is a job's spec version: the version of its latest config,
raised by every submit that changes the spec. `RunJobRequest.expected_version`
(`legion job run --expected-version N`, MCP `legion_job_run` with
`expected_version`) submits only when the job is at exactly that version;
`0` means the job must not exist:

```console
$ legion job info web --json | jq .version
6
$ legion job run web.ncl --expected-version 6 --json | jq .data.version
7
$ legion job run web.ncl --expected-version 6
Error: job web is at version 7, not 6: it changed since you read it
Hint: read it again (legion job info web) and resubmit on top of it with --expected-version 7
```

- The check runs inside the same replicated write as the new version, so two
  writers racing on one version cannot both win.
- A mismatch writes nothing and fails with `ABORTED` (the same code
  `ApplyObject`'s compare-and-set uses). Retrying the same request fails
  again: read the job, reapply your change, and submit with the new version.
- An unchanged resubmit at the right version succeeds without a new version.
- A retry with the same `idempotency_key` returns the recorded reply without
  checking again.
- Only a person's or agent's own submit takes `expected_version`; a template
  run, an app's submit, or a submit acting for an object refuses it with
  `INVALID_ARGUMENT`.
- The version guards the spec, not the job's runtime state: a status or
  allocation change does not move it.

`RunJobReply.version` is the version after the submit, ready for the next
compare-and-set.

## Plan diffs

`legion job plan` (and `PlanJobReply.diff`) shows what a submit would change
in the stored spec, field by field:

```console
$ legion job plan web.ncl
Job config changed; showing updated placements
Changes to the stored spec:
  ~ priority: "normal" → "high"
  ~ tasks.api.resources.memory: 256 → 512
  + tasks.api.env.MODE: "fast"
  - tasks.old-sidecar: {"name":"old-sidecar",…}
```

Paths name tasks (and any list whose entries carry a `name`) by name, other
lists by index. Values are JSON. A value the stored spec rendered from a KV
key you cannot read is reported as changed with its values left out. The
diff is empty for a new job and for an unchanged spec (`same_content`).

## Worker streams resume too

Each `StreamWork` message carries the revision its contents reflect, and a
worker that reconnects to a different manager (after a failover, say) sends
the revision of the last message it applied. When that manager's journal
still holds every allocation change on the node since then, it sends one
resume message with just those changes instead of the node's whole desired
set. See [Managers and Workers](architecture-managers-workers.md#delivery).

## Event ids

Scheduler event ids are not revisions, but they share the guarantee that
matters to a cursor: they never go backwards across a failover. See
[Event Stream](event-stream.md).

## For agents

- MCP `legion_watch` waits (bounded, default 30 s, at most 300 s) for the
  next changes to jobs, allocations, or KV keys after a revision and returns
  them with the revision to pass next time; with no change it returns an
  empty list and the current revision.
- MCP `legion_job_run` takes `expected_version` and `wait_seconds`: it waits,
  with blocking reads, until the job's rollout (if any) finishes and its
  status reads running, complete, scheduled, or idle, or reads failed or
  stopped.
- See [Agent Readiness](agent-readiness.md).

## Known limitations

- The journal is in memory and bounded (above). A manager restart empties
  it.
- Watches cover jobs, allocations, and KV keys only. App objects keep their
  own `WatchObjects` stream, which polls and resends a full snapshot on
  reconnect; nodes, rollouts, and evals have no watch.
- `kv list`, `job list`, and `ledger` on the CLI do not take `--index` yet
  (their `--json` shape is a bare list); the RPCs do.
- In `local_disk` mode, an instance-only change (no allocation row moved)
  does not wake a blocking `ListInstances`.
