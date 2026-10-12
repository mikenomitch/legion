# Sessions in the UI, and a UI that survives a million of them

> **Judgement calls:** [session-ui-plan_calls.md](../calls/session-ui-plan_calls.md)

**Status: SHIPPED** (UI only; no manager, proto, or CLI change). Deferred,
because the API lacks it: server-side paging, a count-only reply, and node
and envelope filters on `ListInstances` (§8). Not verified in a browser
against a live session cluster yet (§9).

## 1. Problem

Session mode (`stateful.mode = "session"`, [docs](../../docs/session-mode.md))
shipped in the manager, CLI, and MCP, and the UI never learned it:

- The job page's Instances card listed sessions as rows with mode `session`,
  no allocation, no node, no actions, and nothing saying what a session is.
  Stop and Promote existed only in the CLI.
- An envelope allocation (`<task>-envelope`) read like a broken service:
  health "no report", no ports, Logs and Exec offered for a placeholder that
  runs nothing.
- A session task's card on the job page said "not allocated", because
  sessions are not allocations.
- At scale it falls over. `ListInstances` returns every running session in
  one reply, with no paging, no limit, and no filter. The job page polled it
  every 2 seconds and rendered every row (each with its own 1-second timer for
  the idle column), and the instance page polled it every 5 seconds just to
  find forks. A job with a million sessions would decode a multi-hundred-MB
  reply every 2 seconds and try to mount a million rows.

## 2. Prior art

- **Kubernetes dashboard / Lens**: list views page through `limit` and
  `continue` tokens; the dashboard shows "1-10 of N" and never renders a full
  namespace of pods. We take the shape (a page at a time, a count of the rest)
  but not the paging, because `ListInstances` has none.
- **Nomad UI**: allocation lists on the job page are paginated client side
  (25 per page) over a full list, and the "allocations" summary is a count
  bar, not a list. We take "counts beat lists".
- **Consul UI**: service instance lists filter client side over a full
  blocking query. That is the trap at a million rows; we bound it by not
  fetching automatically past a threshold.

## 3. Jobs to be done

**Platform engineer running an agent-sandbox service**
- When my job runs thousands of sessions, I want to see how many are running
  and where, so I can tell whether the envelopes are sized right.
- When one session turns out to matter, I want to keep it from the page I am
  looking at, so I do not have to switch to a terminal mid-investigation.
- When a session misbehaves, I want to end it from the UI.

**On-call responder**
- When a node looks hot, I want to jump from the node to the sessions running
  on it, so I can see what a node loss would take with it.
- When I open an envelope allocation, I want to understand that it is a
  reservation, not a crashed process, so I do not chase a non-problem.

**Cluster operator**
- When a job has a very large number of sessions, I want the UI to stay
  responsive and not hammer the managers, so a dashboard left open is not an
  incident of its own.

**Autonomous agent**: out of scope; agents use `legion_instance_*` over MCP.

## 4. Goals and non-goals

Goals: sessions are visible and labelled everywhere instances appear, with
Promote and End where `job:write` allows; envelopes read as reservations;
no page fetches, polls, or renders without a bound because of sessions.

Non-goals: creating sessions from the UI (create stays in the CLI, like keyed
create); new RPCs or fields (UI-only change); showing every session.

## 5. What exists today

- `ui/src/pages/details/JobDetail.tsx`: `listInstances` polled at 2 s, every
  row mapped and rendered by `InstancesPanel`/`InstancesTable`.
- `ui/src/pages/details/InstanceDetail.tsx`: `GetInstance` at 2 s (works for
  sessions), `listInstances` at 5 s for fork lineage.
- `ui/src/pages/details/AllocationDetail.tsx`, `NodeDeepView.tsx`: allocation
  views with no idea of envelopes.
- Generated types already carry `InstanceInfo.node_id`, `envelope_alloc_id`,
  `lease_issued_at`, `promoted_from`, `durable`, `ListInstancesReply.complete`
  and `as_of`, and `PromoteInstance`.

## 6. Design

### 6.1 Job page

- Instances card: one pass counts the listing (states, modes, sessions by
  node, envelope, and task). The table renders 100 rows, "Show 100 more" up
  to 1,000, then "narrow the filter or `legion instance list <job> -j`".
  Filters: state chips, mode chips, a text filter (id, node, task), and
  node / envelope / task filters that live in the URL
  (`/jobs/agents?mode=session&node=w-17#instances`) so other pages can link
  to them.
- A session summary: "12,345 running sessions on 87 nodes", whether every
  node has reported (`complete`), when the index was read (`as_of`), and the
  eight busiest nodes as filter chips.
- Session rows: a `session` badge in Mode, `envelope #N on <node>` in the
  Allocation column, and Promote (optional new name, hidden when
  `promote.onName = false`) and End, each two-step. Promoted rows say
  "promoted from <id>".
- Polling: a listing of 5,000 or more is not re-polled; the card says so and
  offers Refresh. Actions refresh it once.
- Not fetching: when placed envelopes times sessions-per-envelope (from the
  spec) exceeds 50,000, or cannot be computed (no `node:read`, or the spec
  does not size both), the card explains and offers "Load instances and
  sessions" instead of fetching. `?load=1` skips the gate.
- Task cards: a session task shows its envelopes' status and "N running";
  the Allocations card labels envelope rows and renders 200 at a time.
- Header: "N sessions" badge.

### 6.2 Instance page

`Session` eyebrow, a "session · not durable" badge, a note on what that
means, the node and envelope from `GetInstance`, Promote and End in the
header; promotion to a new name navigates to it. Snapshots are hidden;
Resources and Checkpoint say why they are empty. After a session ends the
not-found state reads "Session … has ended". Fork lineage is not looked up
for a session or in a job with session tasks; elsewhere a listing of 5,000+
is kept rather than re-polled.

### 6.3 Allocation page (envelopes)

An envelope is recognized by task name `<task>-envelope` where `<task>` is a
session task in the spec (name alone when the spec is unknown). It gets a
"session envelope" badge and a card: what it reserves, room for N sessions
(from the spec), "Count its sessions" (one `ListInstances` on click, first 10
ids listed), and a link to the job's Instances card filtered to it. Health,
Network, Logs, and Exec are hidden.

### 6.4 Node and task pages

Node page: envelope rows get an "envelope · sessions" badge linking to the
job's sessions on this node. No fetch. Task page: a note for a session task
(its sessions are not allocations; link to them) and for an envelope task
(generated, runs nothing).

## 7. Proto, config, persistence

None.

## 8. What the API lacks (deferred)

- `ListInstances` paging (`page_size`, `page_token`) and a session limit.
- A count-only mode, or session counts by node/envelope on `GetJobInfo`.
- `ListInstances` filters for `node_id`, `envelope_alloc_id`, `mode`, and
  `task`, so the node, envelope, and filtered views fetch only what they show.

With those, the deferral gate and the 5,000-row polling pause can go.

## 9. Testing and rollout

`npm run typecheck` and `npm run build` (the UI has no unit-test runner).
`/testing` renders a mixed session listing, a 250,000-session listing built
in the browser, both deferred-card states, and the envelope card. The pure
helpers in `ui/src/utils/sessions.ts` were exercised at 1,000,000 sessions
outside the browser (count ~0.3 s, a node filter ~55 ms, a text filter
~0.5 s). Rollout: none; the UI reads fields every session-capable manager
sends, and against an older manager there are no session rows.

Acceptance: a session job's page shows counts and the first 100 rows; Promote
and End work and refresh; an envelope page explains itself; the 250k fixture
stays responsive.
