# UI Architecture

The UI is a SolidJS single-page app in `ui/` and talks directly to manager gRPC APIs over gRPC-Web.

## Stack

- SolidJS + Solid Router
- ConnectRPC (`@connectrpc/connect-web`) generated from `proto/legion.proto`
- TailwindCSS
- Vite build, split by route: the API Explorer (which embeds the generated
  proto sources as text for its field docs), the State Explorer, the
  Observability page and the xterm.js terminal load on first use, so the
  first page load carries none of them.
- Self-hosted fonts via `@fontsource-variable` (Space Grotesk for display,
  Inter for text, Fira Code for data) — no CDN requests. The `font-mono`
  utility disables Fira Code's `liga`/`calt` ligatures so IDs, hashes and log
  lines render literally.

## Design System

The visual language ("Legion command post": warm stone neutrals, brass
accent, dark-first with a full light theme) is centralized:

- **Tokens**: theme colors are CSS variables in `ui/src/index.css`
  (`--c-canvas`, `--c-surface`, `--c-line`, `--c-ink`, `--c-muted`,
  `--c-brass`, …) mapped into Tailwind color names in
  `ui/tailwind.config.js` (`bg-surface`, `text-muted`, `border-line`,
  `text-brass`, …). Light/dark switch by swapping variables on `.dark`;
  pages never write `dark:` variants for theme colors.
- **Primitives**: `ui/src/components/ui.tsx` exports the shared components
  every page composes — `Icon`, `Badge`/`StatusBadge`/`MonoBadge`,
  `Card`/`SectionCard`/`PageHeader`/`StatCard`, `Tabs`, `Note`,
  `EmptyState`, `Spinner`/`LoadingRows`/`PageSkeleton`,
  `Button`/`ConfirmButton` (two-step destructive confirm)/`CopyButton`,
  `CodeBlock`, `DefList`, `RelativeTime` (relative text, exact time as the
  tooltip, kept fresh by a shared 30s clock), `Kbd`, `FilterChips`, form
  classes (`inputClass`, `selectClass`, …) and table classes (`tableClass`,
  `thClass`, `tdClass`, …). Add new UI here first.
- **Status vocabulary**: `ui/src/utils/status.ts` maps every status string
  the manager emits to one of six tones (`ok`/`warn`/`danger`/`info`/
  `neutral`/`accent`) and exposes the badge/dot/text classes for each, so
  the same word gets the same color everywhere. `StatusBadge` uses it
  automatically.
- **Favicon and tab title**: `ui/public/favicon.svg` is the brand mark;
  `document.title` reads innermost-first (`web · api · Jobs · Legion`) so
  open tabs are tellable apart by their first word.

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
- `/rollouts` (+ `/rollouts/:id`); `/deployments` redirects here
- `/nodes` (+ `/nodes/:id`)
- `/manager` (+ `/manager/:id`)
- `/raft`, `/events`, `/kv`, `/access`, `/api`, `/state-explorer`,
  `/observability` (shown only when the manager reports o11y enabled),
  `/testing`
- anything else renders a not-found card naming the path (it used to land
  silently on the overview)

A shared layout provides the grouped sidebar nav, a slim topbar
(breadcrumbs, ⌘K search, the shortcut sheet, theme toggle), and page headers
for top-level routes; detail pages render their own `PageHeader`. The sidebar
footer shows the current identity (principal and source, or "ACLs off" / "Not
logged in") linking to Access, and the manager address with a dot that
pulses red while contact is lost.

Heavy routes are loaded through `lazyPage()` in
`ui/src/components/LazyPage.tsx` — a resource-backed loader rather than
Solid's `lazy()`, because a `Suspense` boundary around a page also catches
every resource *inside* it, and a polled page under Suspense would flash its
fallback on every refetch. `lazyComponent()` does the same for the terminal.

## Keyboard Navigation

The UI is usable without a mouse. Everything below is global — no setting, no
mode to enter. Press `?` anywhere for the sheet that lists all of it.

| Keys | What it does |
| --- | --- |
| `⌘K` / `Ctrl+K` | Command palette: recent pages, every page, every row of the index you are on, and actions (theme, shortcuts) |
| `?` | The keyboard shortcut sheet |
| `j` / `k`, `↑` / `↓` | Move the selection down/up the current index |
| `o` / `Enter` | Open the selected row |
| `u` | Go up one level (`/jobs/api/tasks/web` → `/jobs/api`) |
| *hold* `⌘` / `Ctrl` | **Jump hints** — label the rows and the sidebar with the key that opens them |
| `⌘1` … `⌘9`, `⌘0` | Open the 1st … 10th row of the index |
| `⌘⇧` + letter | Open the labelled sidebar item |
| `⌘↵` / `Ctrl+Enter` | Run the query in the State Explorer |

`j`/`k`/`o`, `u` and `?` are ignored while you are typing in a field, and
while a link or button has keyboard focus — Tab focus wins over the virtual
selection. A dialog (the palette, the sheet) owns the keyboard while open.

The palette's **Recent** group is the last eight detail pages this browser
opened, recorded from the URL in the layout (`ui/src/utils/recents.ts`,
localStorage); "Clear recent pages" is one of its actions.

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

Sidebar hints carry `⇧` because plain `⌘`+letter belongs to the browser and the
OS (`⌘S`, `⌘W`, `⌘R`). The badge always spells the full chord, so what you read
is what you press.

Known limitations:

- **Ten rows.** Digits run out after the tenth row; on a 20- or 50-row page the
  rest carry no hints. Use `⌘K` or the search box for those.
- **Index pages only.** Jobs, Workers, Rollouts, Managers, Services and Volumes
  label their rows. The Overview dashboard and the tables inside detail pages
  do not, and neither do secondary tables (the driver table on Volumes).
- **Sidebar letters are positional.** They are assigned from the sidebar as
  rendered, so an item's letter can change when the nav does — most visibly when
  the manager has observability enabled and an extra item appears.
- **Collapsed sidebar shows no letters** (there is no room for the badge), but
  the chords still work.
- **A browser may keep a chord.** The letters skip the combinations browsers
  reserve for tabs, windows and devtools, but a browser or extension with its
  own bindings can still win one; that chord will do the browser's thing
  instead.

Implementation: `ui/src/utils/jumpHints.ts` holds the visibility signal, the
letter assignment, and the registry of the current index's rows;
`ui/src/App.tsx` owns the key handling; pages opt in with `useIndexJumpTargets`
and the `<JumpHint>` primitive. The sheet's rows in
`ui/src/components/ShortcutsHelp.tsx` are written by hand from these
bindings; change both together.

## Data Fetch Pattern

Every page is live. The pattern is `createResource` fed by a poll tick from
`ui/src/utils/polling.ts`:

- `usePoll(intervalMs)` returns a counter that advances on a timer **while the
  tab is visible**; ticks stop when the tab is hidden and resume (with an
  immediate tick) when it is looked at again. Two cadences:
  `POLL_INTERVAL_MS` (2s) for things being watched — allocations, health, a
  rollout in progress — and `INDEX_POLL_INTERVAL_MS` (5s) for index pages and
  the detail pages that fan out into many RPCs per refresh.
- Solid keeps a resource's previous value on screen while the next fetch is in
  flight, so nothing flashes on a poll. Detail pages guard on "this is the
  entity in the URL" (`data()?.name === jobName()`), never on `loading`, so a
  job→job navigation shows a spinner instead of the previous job.
- `retainOnError(fetcher, key)` wraps a polled fetcher so that once it has
  succeeded for a source, a later failure for the *same* source returns the
  last good value instead of rejecting. A restarting manager does not turn a
  page into an error card; the connection banner (below) is what reports the
  outage. The first fetch, and the first after the source changes to a
  different entity, still reject, so a missing job is still reported as
  missing. **Known limitation:** a single RPC that starts failing permanently
  while the manager is otherwise up leaves that page showing its last data
  with no local hint.
- Garnish that may be denied (the ledger on job pages, the o11y probe in the
  layout, the overview stats, the scale panel) swallows its own rejection and
  degrades to empty.
- Lists whose rows carry inputs use `<Index>` rather than `<For>`, because
  each poll returns fresh objects and a keyed list would remount every row.

Exception: the Events page (`Events.tsx`) consumes the `StreamEvents`
server-streaming RPC over gRPC-Web (an async iterable from ConnectRPC) instead
of polling. It seeds with `ListEvents`, tails the stream, and reconnects with
an `afterId` cursor so no events are missed across disconnects.

## Connection Watchdog

`ui/src/utils/connection.ts` probes `WhoAmI` once to gate the app
(`ConnectionGate`: a wrong address or a manager that is down gets a card with
the address and the error, which retries on its own every 5 seconds and has a
button for an immediate retry). After that first success a watchdog keeps
probing —
every 10s, every 3s once contact is lost, never while the tab is hidden. Loss
shows a red banner under the ACL banner with the address, how long ago, and a
Retry button; pages keep their last data and say nothing themselves. Recovery
clears the banner and re-reads the identity, in case ACLs changed across the
restart. A manager that answers but refuses the call (a stale token) is not a
connection problem and is reported on the Access page instead.

## Page Responsibilities

- `Homepage.tsx`: stat cards, active rollouts, the worker table (status,
  health, availability), the last eight scheduler events (operator:read
  garnish — hidden when denied), and the allocation table.
- `Nodes.tsx`: the worker index. Status is the node's liveness
  (`active`/`lost`/…); the health-check summary is its own column, a badge
  marks any availability other than `available`, and the last column is
  labelled **Heartbeat** because that is what it shows.
- `Jobs.tsx`: job list + on-demand spec retrieval, with **status chips**
  above the table. The chips come from `ListJobsReply.status_counts` — counted
  server-side over the whole query-matched set before the status filter — so
  the other chips keep their numbers while one is selected; picking one sends
  `ListJobsRequest.status` (exact, case-insensitive). The shared "Updated"
  column shows `Job.updated_at` — the job record's mtime, which moves on spec
  submissions and status transitions but *not* on allocation churn (see
  [Job Timestamps](jobs-tasks-and-scheduling.md#job-timestamps-created_at-and-updated_at)).
  Sorted newest-change-first by default, matching Rollouts, Templates, and
  Manager. The job detail view (`pages/details/JobDetail.tsx`):
  - shows created/updated under the title, and a tooltip on the `done`
    badge explaining that `done` is the manager's word for an accepted,
    scheduled job rather than a finished one;
  - offers **Stop job** (two-step confirm, hidden once the job is stopped,
    failed or destroyed) beside **Delete job**, both gated by `job:write` —
    destroy needs a stopped job first, which is why Stop lives there;
  - carries a **Scale** card (`components/ScalePanel.tsx`): per task the spec
    count, the override with its source and age, the effective count and any
    pending scale-in, plus a Desired field with Scale / Reset — the same
    `ScaleTask` behind `legion job scale` ([Scaling Jobs](autoscaling.md)).
    Gated by `scale:write`; keyed stateful tasks are read-only with the reason
    in the tooltip; the whole card is hidden when `GetJobScaleStatus` is
    denied;
  - marks stateful tasks with a "stateful · slot/keyed" badge (plus storage
    path) and, for jobs with stateful tasks or live `ListInstances` rows,
    renders a polled read-only Instances panel (id, mode, color-coded state,
    slot, current allocation, storage handle, created). Keyed instance
    create/destroy is CLI-only (`legion instance ...`); the UI does not
    mutate instances.
- `pages/details/TaskDetail.tsx`: definition, volumes, artifacts, discovery,
  health, runtime allocations, exec, logs. The header badge is the same live
  per-task status the job page's task cards show
  (`utils/allocations.ts` `fetchTaskStatusByName`), so the two pages agree.
- `pages/details/NodeDetail.tsx` → `components/NodeDeepView.tsx`: the worker
  page, polled. The header carries **Cordon** / **Drain** (two-step confirm)
  while the node is available and **Uncordon** while it is drained — the
  `SetNodeAvailability` RPC behind `legion node cordon|drain|uncordon`
  ([node availability](jobs-tasks-and-scheduling.md)) — gated by
  `node:write` on the node's pool, with the RPC's reply shown under the
  header and a note explaining a drained node.
- `Services.tsx`: the discovery catalog (`ListServices`) with healthy/total
  counts; `pages/details/ServiceDetail.tsx` resolves endpoints
  (`ResolveService`, unhealthy included) and renders the same layered
  diagnosis as `legion service doctor` (`utils/services.ts`). Endpoint
  addresses link out when the service's `meta.protocol` is web-reachable.
- `Volumes.tsx`: the volume index (`ListVolumes`) plus a plugin table
  (`ListVolumeDrivers`). Node-local volumes are marked with `*` because that is
  the property deciding whether a stateful workload can move at all;
  `pages/details/VolumeDetail.tsx` shows one volume's plugin params, driver
  metadata, node pin, and last driver error. Deletion is CLI-only
  (`legion volume delete`) — it destroys data, and a two-click confirm in a
  dashboard is not the right gate for that.
- `Rollouts.tsx`: rollout list, per-group status, event logs, manual advance
  action; the detail page polls at the live cadence.
- `Events.tsx`: live scheduler event feed with filtering and pause/resume;
  the job and node chips on each row link to their pages.
- `KeyValue.tsx`: key tree browser and CRUD/patch editor.
- `Manager.tsx`: manager config snapshots.
- `Raft.tsx`: polled consensus status, operator commands, history.
- `components/LogViewer.tsx`: the live task log tail, with stream and
  allocation filters and **Find in logs** (case-insensitive, hits
  highlighted, "N of M lines match").
- `TestingUtils.tsx`: placeholder (not implemented yet).

## Service Discovery

Discovery is surfaced in three places, all reading the same projection:

- **Services** (`/services`, `/services/:name`): the catalog, per-endpoint
  health, and the doctor-style diagnosis.
- **Job detail**: a Services card with each declared service's endpoints, and
  Health/Ports columns on the allocation table.
- **Task detail**: the declared `service`/`network.ports` blocks, live
  endpoints for that task, the declared `checks` table, and the latest
  per-allocation health snapshot.

Allocation ports and health come from the state store
(`utils/allocations.ts`): the ledger carries the assigned `network` blob but
not the owning task name or the health snapshot, so one SQL query joins
`allocation` → `task_config` → `allocation_health` per job.

## Volumes

Storage is surfaced wherever the question "where is my data" gets asked
(`utils/volumes.ts` holds the shared state/scope vocabulary):

- **Volumes** (`/volumes`, `/volumes/:id`): the index, with counts for pinned,
  reclaiming, and driver-erroring volumes, plus a warning banner when a plugin
  is installed but unusable everywhere (the reason jobs get rejected at submit).
  Destroyed tombstones are filtered out by default.
- **Job detail**: each instance's storage handle links to the volume holding
  that replica's data.
- **Task detail**: a Volumes card listing this task's live volumes with state,
  owning instance, and node.
- **Worker detail**: volumes materialized on that host, with an explicit note
  that draining or losing the node strands the node-local ones.

## Job Templates

`/templates` lists the cluster's [job templates](job-templates.md);
`/templates/:name` is where a template is actually used:

- The **run form is generated from the template's declared variables** —
  text inputs, textareas, number fields, toggles, `<select>`s, option chips
  for `multi_select`, and a JSON editor for `json` variables. Form state is
  a single `Record<string, unknown>` seeded from the declared defaults, and
  it is converted to typed JSON only at submit.
- **Preview rendered spec** calls `RenderJobTemplate` — the same render the
  submit would do, so what the user sees is what gets submitted. Editing any
  value invalidates a stale preview.
- Submitting links straight to the created job; that job's page shows a
  `template: <name> v<N>` badge back to the template version that made it.
- The source editor (new template, new version) renders only for identities
  holding `template:write`.

## ACL-Aware Rendering

`WhoAmI` returns the caller's effective rules (`rulesJson`), and
`utils/acl.ts` mirrors the manager's evaluation — same globbing, same
default-deny, same "a rule with a selector never matches a request without
that attribute" — behind `allows(resource, action, scope)`.

- **Nav**: an authenticated identity only sees destinations it can read
  (`ROUTE_RESOURCE` in `utils/acl.ts` maps each top-level route to the
  resource it needs). An **anonymous** caller keeps the full sidebar — they
  have not had a chance to authenticate, and an app that hides everything
  looks broken rather than locked.
- **Denied pages**: every protected page wraps its own content in
  `AccessGate`, which swaps in a card naming the missing permission with a
  login link (anonymous) or a switch-identity link (authenticated). The gate
  lives at the page level on purpose: wrapping the router's *outlet* in any
  conditional captures it once and freezes navigation while the URL keeps
  changing.
- **Mutating controls** (stop/destroy job, scale, cordon/drain/uncordon,
  rollout advance/stop, KV save/delete/patch, raft commands, saved-query
  writes, exec and actions, template publish/delete) render disabled with a
  title explaining why.
- **Access page** shows admin tabs only with `acl:read` (worker join needs
  `node:read`).

Two hard-won rules keep restricted identities from breaking the app:

- **A resource that may be denied must swallow its own rejection.** Reading
  an errored Solid resource *throws at the read site*; one unguarded
  `operator:read` probe in the layout used to kill the whole reactive graph
  — URL changing, page frozen. Garnish RPCs (the ledger on job pages, the
  o11y probe in the layout, the overview stats) degrade to empty on denial.
- **`RouteErrorBoundary`** wraps the page area as a last line of defense: a
  page that still throws gets an error card and a retry, and navigating
  elsewhere resets it. It renders children unconditionally — it is not a
  conditional around the outlet.

As everywhere else in the UI, all of this is cosmetic; the manager is the
only enforcement point.

## Stateful Workloads

Allocation views (node detail allocation summaries, Homepage ledger cards,
job detail allocation table) show a compact mono instance-id pill when
`LedgerAllocation.instance_id` is set, so a rescheduled stateful allocation
is visibly the same instance. Stateless allocations are unchanged.

## Backend Compatibility Requirements

Manager server enables:

- gRPC-Web layer (`tonic_web::GrpcWebLayer`)
- CORS allow-all setup

Without these, browser gRPC-Web calls would fail.

The UI tolerates a manager older than its own client: `ListJobsRequest.status`
is only sent when a chip other than All is selected (an older manager returns
every status — visible, not wrong), and a missing `status_counts` collapses
the chip row to All.

## Generated API Clients

The generated client stubs in `ui/src/gen/` come from `npm run gen` (the
protoc invocation in `ui/README.md`; needs `protoc` on the PATH). CI's
`ui-build` job regenerates them on every UI or proto change and fails when the
committed files differ, so a proto change cannot merge without its client.

## Raft page

The UI includes a dedicated **Raft** page under Cluster navigation for manager raft status, recent raft history, and operator commands (election/failover/membership/join-token flows).

## Exec & Actions (interactive terminal)

The task detail page (`pages/details/JobDetail.tsx` → `TaskDetail.tsx`) has an
**Exec & Actions** section listing **this task's** running allocations: the
job's allocations from `GetLedger`, narrowed to the ones whose task is the page's
task. The ledger carries only `task_config_id`, so the task name comes from the
state-store lookup that also carries allocation health; a token without
`state:read` cannot make that mapping, and the section then falls back to listing
every running allocation of the job (the manager still rejects an exec whose
allocation runs a different task). Each listed allocation gets an
`AllocationExecPanel` with a command launcher (interactive-tty toggle) and, for
tasks that declare them, action forms generated from the parameter schema
(`ListJobActions`, filtered to this task).

Unlike the rest of the UI, exec does **not** use gRPC-Web — browsers cannot do
bidirectional streaming over it. `src/lib/execSocket.ts` opens a **WebSocket**
to the manager's exec bridge (`ws://<host>:<port+1>/v1/exec/ws`), exchanging
protobuf `ExecClientFrame`/`ExecServerFrame` messages (the same frames the CLI
uses) one per binary WS frame; the bearer token rides in the first frame.
`components/ExecTerminal.tsx` binds those frames to an **xterm.js** terminal
(`@xterm/xterm` + `@xterm/addon-fit`), driving resize frames on container
resize; it is loaded on the first session rather than with the app. The
`<Terminal>` contract (open/write/onData/resize) is kept to the xterm.js
surface so the renderer can later be swapped for a libghostty-vt wrapper
without touching the transport (see the plan's Q5).

## Development

- `npm run dev` — the Vite dev server (see `ui/README.md` for the seeded
  preview mode and the same-origin proxy).
- `npm run typecheck` — `tsc --noEmit`; `npm run build` typechecks and
  bundles.
- `npm run gen` — regenerate `src/gen` from `proto/legion.proto`.
- CI (`.github/workflows/ci.yml`, job `ui-build`) runs the drift check, the
  typecheck and the build whenever `ui/**` or the proto changes.
