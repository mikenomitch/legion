# UI Architecture

The UI is a SolidJS single-page app in `ui/`. It talks directly to the manager's gRPC APIs over gRPC-Web.

## Stack

- SolidJS + Solid Router
- ConnectRPC (`@connectrpc/connect-web`) generated from `proto/legion.proto`
- TailwindCSS
- Vite build, split by route. The API Explorer (which embeds the generated
  proto sources as text for its field docs), the State Explorer, the
  Observability page and the xterm.js terminal load on first use, so the
  first page load carries none of them.
- Self-hosted fonts via `@fontsource-variable` (Space Grotesk for display,
  Inter for text, Fira Code for data), so there are no CDN requests. The
  `font-mono` utility disables Fira Code's `liga`/`calt` ligatures so IDs,
  hashes and log lines render literally.

## Design System

The visual language ("Legion command post": warm stone neutrals, brass
accent, dark-first with a full light theme) lives in a few central places:

- **Tokens**: theme colors are CSS variables in `ui/src/index.css`
  (`--c-canvas`, `--c-surface`, `--c-line`, `--c-ink`, `--c-muted`,
  `--c-brass`, …) mapped into Tailwind color names in
  `ui/tailwind.config.js` (`bg-surface`, `text-muted`, `border-line`,
  `text-brass`, …). Light and dark themes switch by swapping variables on
  `.dark`. Pages never write `dark:` variants for theme colors.
- **Primitives**: `ui/src/components/ui.tsx` exports the shared components
  every page builds on: `Icon`, `Badge`/`StatusBadge`/`MonoBadge`,
  `Card`/`SectionCard`/`PageHeader`/`StatCard`, `Tabs`, `Note`,
  `EmptyState`, `Spinner`/`LoadingRows`/`PageSkeleton`,
  `Button`/`ConfirmButton` (two-step destructive confirm)/`CopyButton`,
  `CodeBlock`, `DefList`, `RelativeTime` (relative text, exact time as the
  tooltip, kept fresh by a shared 30s clock), `Kbd`, `FilterChips`, form
  classes (`inputClass`, `selectClass`, …) and table classes (`tableClass`,
  `thClass`, `tdClass`, …). Add new UI here first.
- **Status vocabulary**: `ui/src/utils/status.ts` maps every status string
  the manager emits to one of six tones (`ok`/`warn`/`danger`/`info`/
  `neutral`/`accent`) and exposes the badge, dot and text classes for each.
  The same word gets the same color everywhere. `StatusBadge` uses it
  automatically.
- **Favicon and tab title**: `ui/public/favicon.svg` is the brand mark.
  `document.title` reads innermost-first (`web · api · Jobs · Legion`), so
  you can tell open tabs apart by their first word.

## Entry Points

- App root: `ui/src/index.tsx`
- Main routing/layout: `ui/src/App.tsx`
- gRPC client setup: `ui/src/grpcClient.ts`

Manager endpoint:

- `VITE_MANAGER_ENDPOINT`
- default: `http://127.0.0.1:4747`

## Route Structure

Routes in `App.tsx`:

- `/` overview dashboard (KPI stat cards, active rollouts, workers, recent
  scheduler activity, allocations)
- `/jobs` (+ `/jobs/:id`, `/jobs/:id/tasks/:taskName`, `/jobs/:id/specs/:configId`)
- `/templates` (+ `/templates/:name`)
- `/services` (+ `/services/:name`)
- `/rollouts` (+ `/rollouts/:id`). `/deployments` redirects here.
- `/nodes` (+ `/nodes/:id`)
- `/manager` (+ `/manager/:id`)
- `/raft`, `/events`, `/kv`, `/access`, `/api`, `/state-explorer`,
  `/observability` (shown only when the manager reports o11y enabled),
  `/testing` (unlisted component fixtures, see below)
- anything else renders a not-found card naming the path

A shared layout provides the grouped sidebar nav, a slim topbar
(breadcrumbs, ⌘K search, the shortcut sheet, theme toggle), and page headers
for top-level routes. Detail pages render their own `PageHeader`. The sidebar
footer shows the current identity (principal and source, or "ACLs off" / "Not
logged in") with a link to Access. It also shows the manager address with a
dot that pulses red while contact is lost.

Heavy routes load through `lazyPage()` in `ui/src/components/LazyPage.tsx`.
This is a resource-backed loader used instead of Solid's `lazy()`. A
`Suspense` boundary around a page also catches every resource inside it, so a
polled page under Suspense would flash its fallback on every refetch.
`lazyComponent()` does the same for the terminal.

## Keyboard Navigation

You can use the UI without a mouse. Everything below is global, with no
setting or mode to turn on. Press `?` anywhere for a sheet that lists it all.

| Keys | What it does |
| --- | --- |
| `⌘K` / `Ctrl+K` | Command palette: recent pages, every page, every row of the index you are on, and actions (theme, shortcuts) |
| `?` | The keyboard shortcut sheet |
| `j` / `k`, `↑` / `↓` | Move the selection down/up the current index |
| `o` / `Enter` | Open the selected row |
| `u` | Go up one level (`/jobs/api/tasks/web` → `/jobs/api`) |
| *hold* `⌘` / `Ctrl` | **Jump hints**: label the rows and the sidebar with the key that opens them |
| `⌘1` … `⌘9`, `⌘0` | Open the 1st … 10th row of the index |
| `⌘⇧` + letter | Open the labelled sidebar item |
| `⌘↵` / `Ctrl+Enter` | Run the query in the State Explorer |

`j`/`k`/`o`, `u` and `?` are ignored while you are typing in a field, and
while a link or button has keyboard focus. Tab focus takes priority over the
virtual selection. A dialog (the palette, the sheet) owns the keyboard while
it is open.

The palette's Recent group shows the last eight detail pages this browser
opened. The layout records them from the URL (`ui/src/utils/recents.ts`,
localStorage). "Clear recent pages" is one of the palette's actions.

### Jump hints

Hold `⌘` (`Ctrl` on Windows and Linux) and the UI labels what you can jump to:
a digit on each of the first ten rows of the index, and a letter on each sidebar
item. Press the labelled key with the modifier still held. Release it and the
labels disappear.

```
Jobs                        ⇧B          ⌘⇧B  → Jobs
Services                    ⇧S          ⌘⇧S  → Services

1  svc-api      DONE                    ⌘1   → svc-api
2  svc-web      DONE                    ⌘2   → svc-web
```

Sidebar hints include `⇧` because plain `⌘`+letter belongs to the browser and
the OS (`⌘S`, `⌘W`, `⌘R`). The badge always spells out the full chord, so what
you read is what you press.

Known limitations:

- **Ten rows.** Digits run out after the tenth row. On a 20- or 50-row page
  the rest have no hints. Use `⌘K` or the search box for those.
- **Index pages only.** Jobs, Workers, Rollouts, Managers, Services and Volumes
  label their rows. The Overview dashboard and the tables inside detail pages
  do not, and neither do secondary tables (the driver table on Volumes).
- **Sidebar letters are positional.** They are assigned from the sidebar as
  rendered, so an item's letter can change when the nav does. This is most
  visible when the manager has observability enabled and an extra item appears.
- **Collapsed sidebar shows no letters** (there is no room for the badge), but
  the chords still work.
- **A browser may keep a chord.** The letters skip the combinations browsers
  reserve for tabs, windows and devtools. A browser or extension with its own
  bindings can still claim one, and that chord will do the browser's action
  instead.

Implementation: `ui/src/utils/jumpHints.ts` holds the visibility signal, the
letter assignment, and the registry of the current index's rows.
`ui/src/App.tsx` owns the key handling. Pages opt in with
`useIndexJumpTargets` and the `<JumpHint>` primitive. The sheet's rows in
`ui/src/components/ShortcutsHelp.tsx` are written by hand from these
bindings, so change both together.

## Data Fetch Pattern

Every page is live. The pattern is `createResource` fed by a poll tick from
`ui/src/utils/polling.ts`:

- `usePoll(intervalMs)` returns a counter that advances on a timer while the
  tab is visible. Ticks stop when the tab is hidden and resume (with an
  immediate tick) when it is visible again. There are two cadences:
  `POLL_INTERVAL_MS` (2s) for things being watched (allocations, health, a
  rollout in progress) and `INDEX_POLL_INTERVAL_MS` (5s) for index pages and
  the detail pages that fan out into many RPCs per refresh.
- Solid keeps a resource's previous value on screen while the next fetch is in
  flight, so nothing flashes on a poll. Detail pages check that the data
  matches the entity in the URL (`data()?.name === jobName()`), never
  `loading`. That way, navigating from one job to another shows a spinner
  instead of the previous job.
- `retainOnError(fetcher, key)` wraps a polled fetcher. Once it has succeeded
  for a source, a later failure for the same source returns the last good
  value instead of rejecting. A restarting manager does not turn a page into
  an error card, and the connection banner (below) reports the outage. The
  first fetch, and the first after the source changes to a different entity,
  still reject, so a missing job is still reported as missing. **Known
  limitation:** if a single RPC starts failing permanently while the manager
  is otherwise up, that page keeps showing its last data with no local hint.
- Secondary data that may be denied (the ledger on job pages, the o11y probe
  in the layout, the overview stats, the scale panel) swallows its own
  rejection and degrades to empty.
- Lists whose rows carry inputs use `<Index>` instead of `<For>`. Each poll
  returns fresh objects, so a keyed list would remount every row.

Exception: the Events page (`Events.tsx`) consumes the `StreamEvents`
server-streaming RPC over gRPC-Web (an async iterable from ConnectRPC) instead
of polling. It seeds with `ListEvents`, tails the stream, and reconnects with
an `afterId` cursor so no events are missed across disconnects.

## Connection Watchdog

`ui/src/utils/connection.ts` probes `WhoAmI` once to gate the app
(`ConnectionGate`). A wrong address or a manager that is down gets a card
with the address and the error. The card retries on its own every 5 seconds
and has a button for an immediate retry. After that first success, a
watchdog keeps probing: every 10s, every 3s once contact is lost, and never
while the tab is hidden.

When contact is lost, a red banner appears under the ACL banner with the
address, how long ago contact was lost, and a Retry button. Pages keep their
last data and say nothing themselves. Recovery clears the banner and re-reads
the identity, in case ACLs changed across the restart. A manager that
answers but refuses the call (a stale token) is not a connection problem, and
the Access page reports it instead.

## Page Responsibilities

- `Homepage.tsx`: stat cards, active rollouts, the worker table (status,
  health, availability), the last eight scheduler events (needs
  `operator:read`, hidden when denied), and the allocation table.
- `Nodes.tsx`: the worker index. Status is the node's liveness
  (`active`/`lost`/…). The health-check summary has its own column, a badge
  marks any availability other than `available`, and the last column is
  labelled Heartbeat because that is what it shows. A "Memory used /
  reserved" column shows two thin bars per node from `GetNodeUsage`, the
  oversubscription ratio, and a red dot while the node is under memory
  pressure (see [Usage, pressure, and snapshots](#usage-pressure-and-snapshots)).
- `Jobs.tsx`: job list and on-demand spec retrieval, with status chips above
  the table. The chips come from `ListJobsReply.status_counts`, which the
  server counts over the whole query-matched set before the status filter.
  That way the other chips keep their numbers while one is selected. Picking
  a chip sends `ListJobsRequest.status` (exact, case-insensitive). The shared
  "Updated" column shows `Job.updated_at`, the job record's mtime. It moves
  on spec submissions and status transitions but not on allocation churn (see
  [Job Timestamps](jobs-tasks-and-scheduling.md#job-timestamps-created_at-and-updated_at)).
  The list is sorted newest-change-first by default, matching Rollouts,
  Templates, and Manager. The job detail view (`pages/details/JobDetail.tsx`):
  - shows created/updated under the title, and a tooltip on the `done`
    badge. The tooltip explains that `done` is the manager's word for an
    accepted, scheduled job, not a finished one.
  - offers **Stop job** (two-step confirm, hidden once the job is stopped,
    failed or destroyed) beside **Delete job**, both gated by `job:write`.
    Destroy needs a stopped job first, which is why Stop lives there.
  - has a **Scale** card (`components/ScalePanel.tsx`). Per task, it shows
    the spec count, the override with its source and age, the effective count
    and any pending scale-in. It also has a Desired field with Scale / Reset,
    backed by the same `ScaleTask` RPC as `legion job scale`
    ([Scaling Jobs](autoscaling.md)). The card is gated by `scale:write`.
    Keyed stateful tasks are read-only, with the reason in the tooltip. The
    whole card is hidden when `GetJobScaleStatus` is denied.
  - marks stateful tasks with a "stateful · slot/keyed" badge (plus storage
    path). For jobs with stateful tasks or live `ListInstances` rows, it
    renders a polled Instances panel (`components/InstancesTable.tsx`: id,
    mode, color-coded state, slot, current allocation, memory, CPU, idle,
    storage handle, created) and a Snapshots card
    (`components/SnapshotsPanel.tsx`). Keyed instance create/destroy is
    CLI-only (`legion instance ...`). The only instance mutation in the UI is
    restoring one from a snapshot.
  - shows a "Memory pressure" card, beside "Preemption history", when a
    node's pressure response acted on this job's allocations, and Memory and
    Idle columns on the allocation table.
- `pages/details/TaskDetail.tsx`: definition, volumes, artifacts, discovery,
  health, runtime allocations, exec, logs. The header badge is the same live
  per-task status the job page's task cards show
  (`utils/allocations.ts` `fetchTaskStatusByName`), so the two pages agree.
- `pages/details/NodeDetail.tsx` → `components/NodeDeepView.tsx`: the worker
  page, polled. The header has **Cordon** / **Drain** (two-step confirm)
  while the node is available and **Uncordon** while it is drained. These
  call the `SetNodeAvailability` RPC behind `legion node cordon|drain|uncordon`
  ([node availability](jobs-tasks-and-scheduling.md)) and are gated by
  `node:write` on the node's pool. The RPC's reply shows under the header,
  along with a note explaining a drained node. The Usage card and the node's
  "Memory pressure" card are described below.
- `Services.tsx`: the discovery catalog (`ListServices`) with healthy/total
  counts. `pages/details/ServiceDetail.tsx` resolves endpoints
  (`ResolveService`, unhealthy included) and renders the same layered
  diagnosis as `legion service doctor` (`utils/services.ts`). Endpoint
  addresses link out when the service's `meta.protocol` is web-reachable.
- `Volumes.tsx`: the volume index (`ListVolumes`) plus a plugin table
  (`ListVolumeDrivers`). Node-local volumes are marked with `*`, because that
  property decides whether a stateful workload can move at all.
  `pages/details/VolumeDetail.tsx` shows one volume's plugin params, driver
  metadata, node pin, and last driver error. Deletion is CLI-only
  (`legion volume delete`). It destroys data, and a two-click confirm in a
  dashboard is not a strong enough gate for that.
- `Rollouts.tsx`: rollout list, per-group status, event logs, manual advance
  action. The detail page polls at the live cadence.
- `Events.tsx`: live scheduler event feed with filtering and pause/resume.
  The job and node chips on each row link to their pages. Expanded
  `node.pressure` rows show the detail and hint as labeled lines, and
  `snapshot.*` rows show snapshot, instance, kind, size, and error, above
  the raw JSON.
- `KeyValue.tsx`: key tree browser and CRUD/patch editor.
- `Manager.tsx`: manager config snapshots.
- `Raft.tsx`: under Cluster navigation. Polled consensus status, recent raft
  history, and operator commands (election, failover, membership, and join
  tokens).
- `components/LogViewer.tsx`: the live task log tail, with stream and
  allocation filters and **Find in logs** (case-insensitive, hits
  highlighted, "N of M lines match").
- `TestingUtils.tsx`: unlisted (`/testing`), lazy-loaded. Renders the
  usage, pressure, and snapshot components from fixtures in
  `ui/src/fixtures/`, for shapes a dev cluster cannot produce. Nothing on it
  calls the manager.

## Service Discovery

Discovery shows up in three places, all reading the same projection:

- **Services** (`/services`, `/services/:name`): the catalog, per-endpoint
  health, and the doctor-style diagnosis.
- **Job detail**: a Services card with each declared service's endpoints, and
  Health/Ports columns on the allocation table.
- **Task detail**: the declared `service`/`network.ports` blocks, live
  endpoints for that task, the declared `checks` table, and the latest
  per-allocation health snapshot.

Allocation ports and health come from the state store
(`utils/allocations.ts`). The ledger carries the assigned `network` blob but
not the owning task name or the health snapshot. So one SQL query per job
joins `allocation` → `task_config` → `allocation_health`.

## Volumes

Storage shows up wherever you'd ask "where is my data"
(`utils/volumes.ts` holds the shared state/scope vocabulary):

- **Volumes** (`/volumes`, `/volumes/:id`): the index, with counts for pinned,
  reclaiming, and driver-erroring volumes. A warning banner appears when a
  plugin is installed but unusable everywhere, which is why jobs get rejected
  at submit. Destroyed tombstones are filtered out by default.
- **Job detail**: each instance's storage handle links to the volume holding
  that replica's data.
- **Task detail**: a Volumes card listing this task's live volumes with state,
  owning instance, and node.
- **Worker detail**: volumes materialized on that host, with a note that
  draining or losing the node strands the node-local ones.

## Usage, pressure, and snapshots

Measured usage, the memory pressure response
([resource-enforcement.md](resource-enforcement.md)), and snapshots
([suspend-and-resume.md](suspend-and-resume.md)) appear on the pages where
their questions get asked. `utils/usage.ts`, `utils/snapshots.ts`, and
`utils/events.ts` hold the pure helpers, so the pages and the fixtures share
one vocabulary.

- **Worker page, Usage card** (`components/NodeUsagePanel.tsx`), from
  `GetNodeUsage`: memory and CPU capacity with the `×ratio` when
  oversubscribed, reserved (percent of what the scheduler packs against) and
  used (percent of physical) bars, the pressure status (`ok`, `under
  pressure`, `response off`, `status unknown`, `not reported`), every
  allocation's res/max, used, peak, CPU, and idle, and the "Under pressure,
  in order" list with a chip per step. It matches `legion node usage`. The
  old reservation-only card stays for custom resources, and for the whole
  page when the manager has no `GetNodeUsage` or the caller lacks
  `node:read`.
- **Memory pressure cards** (`components/PressureHistory.tsx`) read
  `node.pressure` events (job page by job, worker page by node) and put each
  action's detail and hint on their own lines. They need `operator:read`,
  like every event view, and are hidden when empty.
- **Snapshots card** on the job page: kind, purpose, state, node, size,
  created, and expiry per snapshot, with a row expansion that loads the
  manifest (`GetSnapshot`). Take snapshot (memory is offered only for a
  `sandbox` task with a `snapshots` block and no gated networking, and is a
  two-step confirm because it pauses the guest), Restore (two-step,
  `local` snapshots of keyed, running instances), and Delete (two-step). An
  instance restoring from a snapshot carries a "restoring from" badge.
- **Tolerance.** Every field the manager may not send renders as `-` or a
  named state rather than an error. Unknown snapshot states and purposes
  render as plain badges.

Known limitations:

- The workers index makes one `GetNodeUsage` call per visible row per poll.
  There is no batch RPC yet.
- Pressure history comes from the bounded event journal and needs
  `operator:read`. A job-scoped token sees no Memory pressure card.
- The memory snapshot option is derived from the job spec. A stored network
  policy that gates the task is invisible to it, so the manager can still
  refuse with the reason.

## Job Templates

`/templates` lists the cluster's [job templates](job-templates.md).
`/templates/:name` is where you use a template:

- The run form is generated from the template's declared variables: text
  inputs, textareas, number fields, toggles, `<select>`s, option chips for
  `multi_select`, and a JSON editor for `json` variables. Form state is a
  single `Record<string, unknown>` seeded from the declared defaults. It is
  converted to typed JSON only at submit.
- **Preview rendered spec** calls `RenderJobTemplate`, the same render the
  submit does, so what you see is what gets submitted. Editing any value
  invalidates a stale preview.
- Submitting links straight to the created job. That job's page shows a
  `template: <name> v<N>` badge linking back to the template version that
  made it.
- The source editor (new template, new version) renders only for identities
  holding `template:write`.

## ACL-Aware Rendering

`WhoAmI` returns the caller's effective rules (`rulesJson`).
`utils/acl.ts` mirrors the manager's evaluation behind
`allows(resource, action, scope)`: same globbing, same default-deny, and the
same rule that a selector never matches a request without that attribute.

- **Nav**: an authenticated identity only sees destinations it can read
  (`ROUTE_RESOURCE` in `utils/acl.ts` maps each top-level route to the
  resource it needs). An anonymous caller keeps the full sidebar. They have
  not had a chance to authenticate yet, and an app that hides everything
  looks broken instead of locked.
- **Denied pages**: every protected page wraps its own content in
  `AccessGate`. It swaps in a card naming the missing permission, with a
  login link (anonymous) or a switch-identity link (authenticated). The gate
  lives at the page level because wrapping the router's outlet in any
  conditional captures it once, which freezes navigation while the URL keeps
  changing.
- **Mutating controls** (stop/destroy job, scale, cordon/drain/uncordon,
  rollout advance/stop, KV save/delete/patch, raft commands, saved-query
  writes, exec and actions, template publish/delete, snapshot
  create/delete/restore) render disabled with a title explaining why. The
  Snapshots card is hidden without `snapshot:read` on the job. Take snapshot
  and Delete need `snapshot:write`, and Restore needs `snapshot:read` and
  `job:write`.
- **Access page** shows admin tabs only with `acl:read` (worker join needs
  `node:read`).

Two rules keep restricted identities from breaking the app:

- **A resource that may be denied must swallow its own rejection.** Reading
  an errored Solid resource throws at the read site. A single unguarded
  `operator:read` probe in the layout once killed the whole reactive graph,
  leaving the URL changing and the page frozen. Secondary RPCs (the ledger on
  job pages, the o11y probe in the layout, the overview stats) degrade to
  empty on denial.
- **`RouteErrorBoundary`** wraps the page area as a last line of defense. A
  page that still throws gets an error card and a retry, and navigating
  elsewhere resets it. It always renders its children and is not a
  conditional around the outlet.

All of this is cosmetic. The manager is the only enforcement point.

## Stateful Workloads

Allocation views (node detail allocation summaries, Homepage ledger cards,
job detail allocation table) show a compact mono instance-id pill when
`LedgerAllocation.instance_id` is set. This makes a rescheduled stateful
allocation visibly the same instance. Stateless allocations are unchanged.

## Backend Compatibility Requirements

Manager server enables:

- gRPC-Web layer (`tonic_web::GrpcWebLayer`)
- CORS for allowed browser origins

Without these, browser gRPC-Web calls would fail.

### Allowed browser origins

The manager only answers browsers from allowed origins. Any web page can send
requests to `127.0.0.1:4747`, and with ACLs off every caller has full access,
so an allow-all policy would let any website you visit run jobs on your
cluster. The same applies to DNS rebinding, where a page reaches the manager
under its own hostname.

- Loopback origins are always allowed: `localhost`, `127.0.0.0/8`, and
  `[::1]`, on any port and scheme. This covers `npm run dev` on
  `http://localhost:5173`, with or without the Vite proxy.
- `server.allowedOrigins` in the manager config adds more. Entries are exact
  origins (`scheme://host[:port]`, no path). `["*"]` allows every origin.

  ```nickel
  server = {
    port = 4747,
    allowedOrigins = ["https://legion-ui.example.com"],
  },
  ```

- Requests with no `Origin` header are unaffected. The CLI, workers, SDKs, and
  `curl` never send one.

A request from any other origin gets HTTP 403 before it reaches a handler,
and the manager logs `Refused a browser request from a disallowed origin`.
In the browser this shows up as a failed fetch or a CORS error on every call.
The exec WebSocket applies the same check to its handshake. A UI served from
a tunnel or forwarded-port domain (even through the Vite proxy, which forwards
the browser's `Origin`) needs that domain in `allowedOrigins`.

The UI tolerates a manager older than its own client. It sends
`ListJobsRequest.status` only when a chip other than All is selected. An
older manager then returns every status, which is visible but not wrong. A
missing `status_counts` collapses the chip row to All.

## Generated API Clients

The generated client stubs in `ui/src/gen/` come from `npm run gen` (the
protoc invocation in `ui/README.md`, which needs `protoc` on the PATH). CI's
`ui-build` job regenerates them on every UI or proto change and fails when the
committed files differ, so a proto change cannot merge without its client.

## Exec & Actions (interactive terminal)

The task detail page (`pages/details/JobDetail.tsx` → `TaskDetail.tsx`) has an
**Exec & Actions** section listing this task's running allocations. These are
the job's allocations from `GetLedger`, narrowed to the ones that run the
page's task. The ledger carries only `task_config_id`, so the task name comes
from the state-store lookup that also carries allocation health. A token
without `state:read` cannot make that mapping. In that case the section lists
every running allocation of the job (the manager still rejects an exec whose
allocation runs a different task). Each listed allocation gets an
`AllocationExecPanel` with a command launcher (interactive-tty toggle). For
tasks that declare actions, it also shows action forms generated from the
parameter schema (`ListJobActions`, filtered to this task).

Exec is the one part of the UI that does not use gRPC-Web, because browsers
cannot do bidirectional streaming over it. `ui/src/lib/execSocket.ts` opens a
WebSocket to the manager's exec bridge (`ws://<host>:<port+1>/v1/exec/ws`).
It exchanges protobuf `ExecClientFrame`/`ExecServerFrame` messages (the same
frames the CLI uses), one per binary WS frame. The bearer token rides in the
first frame.

`components/ExecTerminal.tsx` binds those frames to an xterm.js terminal
(`@xterm/xterm` + `@xterm/addon-fit`) and sends resize frames when the
container resizes. It loads on the first session instead of with the app.
The `<Terminal>` contract (open/write/onData/resize) sticks to the xterm.js
surface, so the renderer can later be swapped for a libghostty-vt wrapper
without touching the transport.

## Development

- `npm run dev`: the Vite dev server (see `ui/README.md` for the seeded
  preview mode and the same-origin proxy).
- `npm run typecheck`: `tsc --noEmit`. `npm run build` typechecks and
  bundles.
- `npm run gen`: regenerate `src/gen` from `proto/legion.proto`.
- CI (`.github/workflows/ci.yml`, job `ui-build`) runs the drift check, the
  typecheck and the build whenever `ui/**` or the proto changes.
