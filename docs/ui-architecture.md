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
- `/jobs` (+ `/jobs/:namespace/:name`, `/jobs/:namespace/:name/tasks/:taskName`,
  `/jobs/:namespace/:name/specs/:configId`, `/jobs/:namespace/:name/instances/:instanceId`;
  the namespace is always a path segment, `default` included, and lists take `?ns=`)
- `/allocations/:id` (no index page; every allocation id in the UI links here)
- `/templates` (+ `/templates/:name`)
- `/services` (+ `/services/:namespace/:name`)
- `/namespaces` (+ `/namespaces/:name`)
- `/rollouts` (+ `/rollouts/:id`). `/deployments` redirects here.
- `/nodes` (+ `/nodes/:id`)
- `/manager` (+ `/manager/:id`)
- `/raft`, `/events`, `/kv`, `/access`, `/api`, `/state-explorer`,
  `/observability` (shown only when the manager reports o11y enabled),
  `/testing` (unlisted component fixtures, see below)
- `/apps/:app/:view` (an installed app's page; no index, see
  [App extensions](#app-extensions)) and `/apps/:app/pages/:page` (an app's
  sandboxed page, see [Sandboxed pages](#sandboxed-pages))
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
  - shows created/updated under the title, and a tooltip on the status
    badge that says what the derived status means (for example, `pending`:
    a task is waiting to be placed or for a dependency).
  - offers **Stop job** (two-step confirm, hidden once the job is stopped
    or destroyed) beside **Delete job**, both gated by `job:write`.
    Destroy needs a stopped job first, which is why Stop lives there.
  - has a **Scale** card (`components/ScalePanel.tsx`). Per task, it shows
    the spec count, the override with its source and age, the effective count
    and any pending scale-in. It also has a Desired field with Scale / Reset,
    backed by the same `ScaleTask` RPC as `legion job scale`
    ([Autoscaling](autoscaling.md)). The card is gated by `scale:write`.
    Keyed stateful tasks are read-only, with the reason in the tooltip. The
    whole card is hidden when `GetJobScaleStatus` is denied.
  - has a **Request vs usage** card (`components/UsageInputsPanel.tsx`), the
    `GetScalingInputs` reply `legion job info` prints. Per task, it shows
    ready/placed replicas with the desired count, an unplaced badge and a
    rollout badge, measured CPU and memory against the reservation with a
    utilization meter, requests per second, error ratio, and mean latency
    into the service the task backs, and the data state (`current` with its
    age, `missing`, `resyncing`). It is cluster-wide whichever manager the UI
    talks to, polled with the page, and hidden when `scale:read` is denied
    ([Scaling inputs](autoscaling.md#scaling-inputs)).
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
- `pages/details/TaskDetail.tsx`: four tabs, polled as one resource. See
  [Task page](#task-page). The header badge is the same live per-task status
  the job page's task cards show (`utils/allocations.ts`
  `fetchTaskStatusByName`), so the two pages agree.
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
- `KeyValue.tsx`: key tree browser and CRUD/patch editor. Values are never
  listed (`ListKv` without `include_values`). A selected key loads with
  `GetKv { metadata_only }` and stays masked until **Reveal** (re-masked
  after 60s, on Hide, on switching keys, refresh, or a hidden tab).
  **Copy value** copies without showing it, and **Replace value** writes
  without revealing. **Used by** lists `GetKvReply.references`, and delete
  warns when any job reads the key. **+ New** only creates. The KV
  encryption-at-rest strip (`components/KeyringStrip.tsx`, `operator:read`)
  sits at the top. See [KV Store](kv-store.md#in-the-ui).
- `Manager.tsx`: every manager in the cluster, from `ListManagers`. The
  roster is the replicated raft membership, so the page lists all members
  whether the UI points at the leader or a follower, and a manager that is
  down stays listed. Each row shows the role (`leader`, `follower`,
  `learner`, or `standalone` outside raft), the advertised address, live
  reachability from the answering manager (latency, or `unreachable` with
  the probe error on hover), the member's term and applied index with its
  lag behind the furthest member, and when its config snapshot was written.
  A warning names how many members did not answer. Outside raft the table
  has one row. The manager name links to its config page when it has a
  snapshot. Every manager writes one at startup, a follower through the
  control lane's leader, so `—` means the manager has not reached a leader
  since it started (or its row predates the member-name key). The page used
  to list the config table itself, which showed only managers that had led.
  `pages/details/ManagerNodeDetail.tsx`
  renders the published config as an at-a-glance summary, feature tiles
  (exec, telemetry, ACLs, workload identity, KV encryption, edge boot,
  auto-suspend, adaptive heartbeats), and per-area cards. Unknown keys go
  under "Other settings" as dotted paths, secret-looking values are redacted
  everywhere (including **Copy JSON** and the raw view), and the store's
  `YYYY-MM-DD HH:MM:SS +HH:MM:SS` timestamps parse
  (`utils/managerTime.ts`).
- `pages/details/InstanceDetail.tsx` (`/jobs/:namespace/:name/instances/:instanceId`): one
  stateful instance from `GetInstance`. It shows state, current allocation
  and node, storage, fork lineage, last stop and cold boot, usage against the
  reservation, checkpoint and what it can checkpoint now, the shared
  instance controls (`components/InstanceActions.tsx`, also used by the job
  page's Instances card, plus Stop/Start here), its snapshots, and an event
  timeline (`components/EventTimeline.tsx`) from
  `ListEvents { instance_id }`.
- `pages/details/AllocationDetail.tsx` (`/allocations/:id`): one allocation.
  It covers placement (job, task, group, instance, worker, spec version and
  whether it is current, the rollout that produced it), health with the
  declared checks and why it last stopped, network (public and cluster
  address, ports, the endpoint it registered), resources (memory gauge
  against reservation and ceiling, CPU, idle, pressure order), an event
  timeline from `ListEvents { allocation_id }`, logs, the task's
  **Actions** panel aimed at this allocation, and storage. The row comes from
  the ledger, so without `node:read` the page falls back to the job's
  rollout (a `?job=` hint from the linking page helps) and to events. An
  allocation the ledger no longer has is shown as ended, with its events.
  `components/AllocationLink.tsx` renders the `#id` links used across the
  UI.
- `Raft.tsx`: under Cluster navigation. Polled consensus status, recent raft
  history, and operator commands (election, failover, membership, and join
  tokens). Commands that act on another manager (Transfer Leader, Add Voter,
  Add Learner, Remove Member) open an inline target form, with known peer
  ids suggested, instead of sharing one always-visible field. Transfer Leader
  is disabled while this manager is the only settled voter.
- `components/LogViewer.tsx`: the live task log tail, with stream and
  allocation filters and **Find in logs** (case-insensitive, hits
  highlighted, "N of M lines match").
- `TestingUtils.tsx`: unlisted (`/testing`), lazy-loaded. Renders the
  usage, pressure, instance (suspended, idle countdown, last stop, forks),
  snapshot (published, named, fork and suspend checkpoints), and content
  locality components from fixtures in `ui/src/fixtures/`, for shapes a dev
  cluster cannot produce. Nothing on it calls the manager.

## Service Discovery

Discovery shows up in three places, all reading the same projection:

- **Services** (`/services`, `/services/:namespace/:name`): the catalog, per-endpoint
  health, and the doctor-style diagnosis.
- **Job detail**: a Services card with each declared service's endpoints, and
  Health/Ports columns on the allocation table.
- **Task detail**: the declared `service`/`network.ports` blocks, live
  endpoints for that task, the declared `checks` table, and the latest
  per-allocation health snapshot.

The ledger carries each allocation's assigned `network` blob and its task
name (`LedgerAllocation.task_name`), so pages group a job's allocations by
task without another lookup. Health snapshots still come from the state store
(`utils/allocations.ts`): one SQL query per job joins `allocation` →
`task_config` → `allocation_health`.

Endpoint and port links use the node's public address
(`ServiceEndpoint.public_address`, or `publicAddress` in the node's
`configAttrs`) when its worker was started with
`LEGION_WORKER_PUBLIC_ADDRESS`, with the in-cluster address beside it.
Without one, private addresses (RFC 1918, CGNAT, loopback, link-local, ULA)
are still linked but dimmed and badged `private`, with a tooltip naming the
variable (`utils/services.ts`; see
[Public addresses](service-discovery.md#public-addresses)). In the Network
page's service graph, service boxes open the service and caller boxes open
the job.

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

## Task Page

`/jobs/:namespace/:name/tasks/:taskName` answers "is this task healthy, and if not, which
instance and why" first, and keeps configuration on its own tab. The header
carries the live status, an `N/M healthy` badge, and **Logs** / **Open shell**
buttons.

| Tab | Contents |
| --- | --- |
| **Overview** | Tiles for instances (running, or running / desired when the task has a plain count — a `job scale` override wins over the spec), health, endpoints (when the task declares a service), and failures in the last 24h. One **Instances** table: allocation, node, status, health with the reported reason, ports, artifact progress, and per-row **Logs** / **Shell** buttons; unhealthy rows sort first. **Health checks** (declared) beside **Recent events**. |
| **Logs** | The live log viewer for this task's allocations. With more than one allocation in view it has an allocation picker. |
| **Actions** | The task's declared actions as a list, the chosen one as a typed form with the argv it will run, a target (one instance, all running, or only the unhealthy ones), and an exit code and output per instance. **Open a shell** is the last list entry. The tab stays mounted once opened, so a shell or a running action survives switching to Logs and back. See [Exec & Actions](#exec--actions-interactive-terminal). |
| **Spec** | Definition (including declared resources: cpus, memory reservation, memoryMax), service & ports with live endpoints, volumes, declared artifacts, and the effective network policy. |

The tab is in the URL: `?tab=overview|logs|actions|spec` (default
`overview`; an unknown value falls back to it, and `exec`, the tab's former
id, opens Actions). `?alloc=<allocation id>` picks the allocation the Logs and
Actions tabs start on, and `?action=<name>|shell` picks the Actions list
entry. A row's **Logs** / **Shell** buttons set them, so a link can land on
one allocation's logs (`/jobs/default/web/tasks/api?tab=logs&alloc=4219`) or a shell
in it (`?tab=actions&action=shell&alloc=4219`). Tab changes replace the history
entry rather than adding one.

The page degrades per permission rather than failing: events (and the
failures tile) need `operator:read`, which `ListEvents` requires; the desired
count needs `scale:read`; matching events to this task needs `state:read`;
allocation placement needs `node:read`. Without one, that piece says it is
unavailable or falls back.

Known limits:

- **Health is one verdict per allocation.** The store keeps one healthy flag
  and status string per allocation, not each check's last result, so the
  Overview lists the declared checks and each instance's overall health.
- **Failures are counted from events.** There is no restart counter; the tile
  counts `allocation.failed` events for this task's task configs among the
  job's last 200 events, and shows `N+` when that window is shorter than a
  day.

## Usage, pressure, and snapshots

Measured usage, the memory pressure response
([resource-enforcement.md](resource-enforcement.md)), suspend, resume, fork,
and snapshots ([suspend-and-resume.md](suspend-and-resume.md)), and content
locality ([artifacts.md](artifacts.md)) appear on the pages where their
questions get asked. `utils/usage.ts`, `utils/instances.ts`,
`utils/snapshots.ts`, and `utils/events.ts` hold the pure helpers, so the
pages and the fixtures share one vocabulary.

- **Worker page, Usage card** (`components/NodeUsagePanel.tsx`), from
  `GetNodeUsage`: memory and CPU capacity with the `×ratio` when
  oversubscribed, reserved (percent of what the scheduler packs against) and
  used (percent of physical) bars, the pressure status (`ok`, `under
  pressure`, `response off`, `status unknown`, `not reported`), every
  allocation's res/max, used, peak, CPU, and idle, and the "Under pressure,
  in order" list with a chip per step (`suspend` included). It matches
  `legion node usage`. A node that reports cores but no clock speed (Apple
  Silicon) shows `10 cores` and measures CPU use in cores. The old
  reservation-only card stays for custom resources, and for the whole page
  when the manager has no `GetNodeUsage` or the caller lacks `node:read`.
- **Worker page, Content and locality card**
  (`components/NodeContentCard.tsx`), from `GetNodeContent`: whether the
  worker can checkpoint and publish, the snapshots captured here or fetched
  or warmed here with their parts held, and every advertised digest with the
  snapshot and role it belongs to and how many nodes hold it. When the
  manager holds no content report for the node, the card says why.
- **Workers index, Memory column**, from one `ListNodeUsage` call per poll
  (one `GetNodeUsage` per row against a manager without it).
- **Memory pressure cards** (`components/PressureHistory.tsx`) read
  `node.pressure` events (job page by job, worker page by node) and put each
  action's detail and hint on their own lines, with the instance when the
  event names one. They need `operator:read`, like every event view, and are
  hidden when empty.
- **Instances card** on the job page (`components/InstancesPanel.tsx` and
  `components/InstancesTable.tsx`): a state filter, and per instance its
  state (`suspended` has its own accent tone, with the suspend reason and
  any operation in flight), memory and CPU, idle time with a live countdown
  to the idle suspend, the checkpoint (kind, local or published, size, node,
  age) with the last suspend and restore durations, fork lineage, storage,
  and why the platform last stopped it (`last_stop`, with the hint). Keyed
  instances get Suspend (with a kind menu when both kinds are possible),
  Resume, Resume cold, and Fork (count 1 to 10, and spread only when the
  manager allows it), each a two-step confirm. Rows are keyed by instance
  id and cells read their row through an accessor, so an armed confirm
  survives every poll.
- **Snapshots card** on the job page: kind, purpose, state, durability,
  node, size, created, and expiry per snapshot, with names shown and
  removable, and a row expansion that loads the manifest, pause time, fork
  lineage, and which nodes hold the parts (`GetSnapshot`). Take snapshot
  (with publish when the manager allows it; memory is a two-step confirm
  because it pauses the guest), Restore (two-step, `local` or `published`
  manual snapshots of keyed, running instances), Name, Warm (published
  snapshots), and Delete (two-step).
- **Eligibility.** Every Suspend, Fork, spread, and memory snapshot control
  reads `InstanceInfo.checkpoint_support`, which the manager computes with
  the same checks the RPCs apply, and shows its blocker message as the
  tooltip. Against a manager without the field the UI falls back to the job
  spec.
- **Job status.** The Jobs index and the job page both show the status the
  manager derives from the workload (`running`, `pending`, `complete`,
  `scheduled`, `failed`, `suspended`, `idle`, `stopped`; see
  [Job Status](jobs-tasks-and-scheduling.md#job-status)). The status chips
  filter on it.
- **Events.** `snapshot` and `instance` are filter categories, and their
  expanded rows show the snapshot, instance, kind, size, durations, forks,
  and error above the raw JSON.
- **Tolerance.** Every field the manager may not send renders as `-` or a
  named state rather than an error. Unknown snapshot states and purposes
  render as plain badges. Each new RPC treats `Unimplemented` and a denied
  permission as "no card".

Known limitations:

- Pressure history comes from the bounded event journal and needs
  `operator:read`. A job-scoped token sees no Memory pressure card, though
  each instance row still shows its own last stop.
- Instances, including from a snapshot (`legion instance create
  --from-snapshot`), are created and destroyed from the CLI only.
- The content card shows no content store budgets: `GetNodeContent` reads
  the location index, which carries digests and sizes but not classes.
- A macOS worker reports no usage, so the idle countdown and populated usage
  are only visible on Linux workers with enforcement on, and on `/testing`.

## Sessions

Sessions ([session-mode.md](session-mode.md)) appear wherever instances do,
labelled as sessions, and every page that touches them is bounded so a job
with a million of them stays usable. `utils/sessions.ts` holds the
vocabulary, the limits, and the session pager.

- **Job page, Instances card.** A `session` badge in the Mode column, the
  envelope allocation and node in the Allocation column, and **Promote**
  (with an optional new name, hidden when `stateful.promote.onName = false`)
  and **End** per row, each a two-step confirm that needs `job:write`.
  Promoted instances say which session they came from. Above the table: how
  many sessions run on how many nodes and the busiest nodes as filter chips
  (all from the reply's `session_count` and `sessions_by_node`, not from the
  page), whether every node has reported to the session index (`complete`),
  when it was read (`asOf`), and, once there is more than one page,
  `Sessions 501–1,000 of 250,000, in id order` with **First**, **Prev**, and
  **Next**. State and mode chips and a text filter (id, node, or task,
  case-sensitive) narrow the rows and the page loaded; the mode, node,
  envelope, and task filters are in the URL, so
  `/jobs/agents?mode=session&node=w-17#instances` opens one node's sessions.
- **Job page, Edge nodes card.** One per session task, from
  `ListSessionNodes`: every node holding an envelope for the task, its tag
  (the 8 characters a session id minted there carries before its final `.`,
  as in `workspace-q4n7c2rd.01J…`), whether it takes new edge starts and why
  not (`lost`, `down`, `draining`, `ineligible`, `no-address`,
  `no-edge-sessions`, `full`), its weight, its sessions (linking to them on
  the Instances card), and its envelope's room left. 100 nodes at a time.
- **Job page, elsewhere.** The header counts sessions (`session_count`); a
  session task's card shows its envelopes' status and how many sessions run
  (the sum of its `ListSessionNodes` entries); envelope rows in Allocations
  are badged with their node's session count for the task and link to their
  sessions.
- **Instance page.** A session reads as `Session`, `session · not durable`,
  with its node and envelope, Promote and End in the header, and a note on
  what it gives up. Promoting under a new name opens the new instance's
  page. After a session ends, the page says it ended. Snapshots, suspend,
  and fork say to promote first. Fork lineage lists the job's rows with
  `session_limit: 0`, so it works in a session job too.
- **Allocation page, envelopes.** An allocation of `<task>-envelope` gets a
  **Session envelope** card: what it reserves, how many sessions fit (from
  the spec), and, read on open from the node's `ListSessionNodes` entry, how
  many run in it, whether the node takes edge starts, its room left, and its
  tag. A manager that refuses `ListSessionNodes` falls back to
  `ListInstances` with `session_limit: 0`, whose per-node total counts every
  session task the job has on that node; the card says so. A link opens the
  sessions on the job page. Health, Network, Logs, and Exec are hidden,
  since an envelope runs nothing.
- **Worker and task pages.** Envelope allocations on a worker are badged and
  link to the job's sessions on that worker. A session task's page says its
  sessions are not allocations; an envelope task's page says it was
  generated and runs nothing.

### Limits at scale

`ListInstances` returns the job's rows in full and its running sessions a
page at a time, in session id order, with the totals counted for free. The
UI never asks for an unbounded list:

| What | Request | What happens |
| --- | --- | --- |
| Instances card | `session_limit: 500`, the pager's token | One page re-read every 2 seconds; **Next** passes the reply's `next_session_page_token`, **Prev** and **First** replay earlier tokens the page kept |
| Rows rendered | | 100 at a time, at most 1,000: "Showing the first N of M", **Show more**, then the filter or `legion instance list <job> -j` |
| Counts (header, summary, busiest nodes) | from the same reply | `session_count` and `sessions_by_node`, whatever the page holds |
| Task cards, envelope rows, Edge nodes | `ListSessionNodes` per session task | Read every 5 seconds; one entry per node, never per session |
| Envelope card | `ListSessionNodes`, else `session_limit: 0` | Read on open and on **Count again** |
| Fork lineage | `session_limit: 0` | Rows only |
| Re-polling | listings under 5,000 | Only a job with that many keyed or slot rows reaches it; the card then stops refreshing every 2 seconds and offers **Refresh** |
| Job allocations | | 200 at a time, **Show more** |

Known limitations:

- `ListInstances` has no node or envelope filter, so the node, envelope,
  state, and text filters narrow the page loaded, not every session. With a
  node filter on, the card says how many sessions that node runs in all and
  to page on for the rest. The Edge nodes card has each node's count.
- Page offsets are counted by the page: if sessions end between pages, the
  `501–1,000` range is approximate, and a page past the last session says
  so and offers **First**.
- `origin` (`manager` or `edge`), `generation`, and `ticket_jti` are on the
  worker's `SessionStatus` but not on `InstanceInfo`, so the UI does not
  show them.
- On the worker page an envelope is recognized by its task name alone, so a
  task named `<x>-envelope` in a job without sessions is badged too.
- Sessions are created from the CLI or the API, not the UI.
- `/testing` renders a mixed session listing and a 250,000-session listing
  built in the browser, both answered the way the manager pages them
  (`session_limit`, the page token, `session_count`, `sessions_by_node`), an
  Edge nodes card for the large one, and the envelope card.

## Job Templates

`/templates` lists the cluster's [job templates](job-templates.md).
`/templates/:name` has two tabs, **Launch** and **Source**.

**Launch** is a three-step guided launch:

1. **Variables.** A form generated from the template's declared variables:
   text inputs, textareas, number fields, toggles, `<select>`s, option chips
   for `multi_select`, and a JSON editor for `json` variables, plus the version
   picker. Form state is a single `Record<string, unknown>` seeded from the
   declared defaults, converted to typed JSON only when sent. **Next: review**
   calls `RenderJobTemplate`; a render error stays on this step.
2. **Review.** Says whether the launch **creates a new job** or **updates the
   running job** (naming the template and version it was last launched from).
   A values table shows what will be sent, and — when the job's current config
   came from this same template — marks values that changed since that launch
   (`was 1`) and keys that are new. Below it, a unified diff of the job's
   recorded rendered spec (`job.rendered.json`) against the new render, keys
   sorted so ordering is not a change; a new job, or a job with no JSON spec
   recorded, gets the full rendered spec instead. Editing any value or
   switching version sends you back to step 1.
3. **Launch.** Calls `RunJobTemplate` with exactly the reviewed values and
   version, then follows the rollout it started (matched on the new config
   id) with per-group progress until it settles. **Copy as CLI** copies the
   equivalent `legion template run <name> --version <N> --var …/--var-json …`
   command, version pinned.

The created job's page shows a `template: <name> v<N>` badge linking back to
the template version that made it.

**Source** shows the selected version's Nickel source and the version list.
The publish editor (new version) renders only for identities holding
`template:write`; the header's **New version** button opens it.

Known limits:

- The Review diff is of rendered specs. It does not say how many allocations
  a change replaces or whether it fits; use `legion job plan` for that.
- A token that may only launch through templates is denied `GetJobInfo` even
  for a job that does not exist yet, so its Review says it cannot tell whether
  the job exists, and it cannot follow the rollout.
- The diff is skipped (full spec shown) above 20,000 lines.

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
  changing. The Overview spans every resource, so it gates differently: with
  ACLs enforced, an anonymous visitor gets the same log-in card (`LoginGate`)
  and a logged-in identity sees each card it can read.
- **Mutating controls** (stop/destroy job, scale, cordon/drain/uncordon,
  rollout advance/stop, KV save/delete/patch, raft commands, saved-query
  writes, exec and actions, template publish/delete, snapshot
  create/delete/restore) render disabled with a title explaining why. The
  Snapshots card is hidden without `snapshot:read` on the job. Take snapshot
  and Delete need `snapshot:write`, and Restore needs `snapshot:read` and
  `job:write`.
- **Access page** shows admin tabs only with `acl:read` (worker join needs
  `node:read`).
- **Logging in.** ACLs are on by default, so a fresh cluster's UI opens on the
  log-in card. Paste the initial management token from
  `<dataDir>/bootstrap-token` (or `~/.legion/tokens/<host>_<port>` on the
  manager's machine),
  or log in with any configured auth method. The UI does not log in
  automatically on the manager's machine: loopback is exactly where workloads
  live, so being local proves nothing.

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

## App extensions

Installed apps add to the UI without shipping code into it
([Apps: Views and actions](apps.md#views-and-actions)). An app registers
**slots** in its manifest, and answers each with a **declarative view**: a
`legion-view/1` JSON document built from a closed set of blocks, which the
SPA draws with its own components. No app code runs in the SPA's origin,
where the operator's token lives.

| Slot | Outlet | Component |
| --- | --- | --- |
| `nav` | an item in the sidebar's **Apps** section, opening `/apps/:app/:view` | `pages/AppPage.tsx` |
| `overview` | a card on the overview, under the stat cards | `AppPanels slot="overview"` |
| `job` | a panel on the job page, under the scale panel | `AppPanels slot="job"` |
| `task` | a badge on each task tile of the job page, and a panel on the task page's overview tab | `AppTaskBadges`, `AppPanels slot="task"` |
| `node` | a panel on the node page | `AppPanels slot="node"` |

The pieces, in `ui/src`:

- `utils/appViews.ts`: the view types, link checks, and the **registry**:
  `ListUiExtensions` without a context, fetched once per login (and again
  when the stored token changes). The sidebar and the overview read it.
- `components/apps/AppSlot.tsx`: the outlets. A job, task, or node outlet
  asks `ListUiExtensions` again **with the page's context**, which the
  manager filters by each entry's declared permission as the viewer, so a
  panel appears only to viewers who may see it there. Each entry then
  fetches `GetAppView` and re-fetches at the app's `ui.refresh` (at least
  5 seconds; `usePoll`, so it pauses in a hidden tab). A view the app marks
  `hidden` draws nothing; a denied one draws nothing in a panel and says so
  on a page.
- `components/apps/ViewBlocks.tsx`: the block renderer: stat tiles, an SVG
  line chart (four lines, a dashed right axis), tables with in-app and
  external links, a timeline, key-values, notices, markdown, and action
  buttons.
- `components/apps/SafeMarkdown.tsx`: a CommonMark subset (headings,
  paragraphs, lists, quotes, code, emphasis, links) drawn as DOM nodes,
  never `innerHTML`.

**Links.** An in-app `route` (one leading slash) becomes a router link; an
`http(s)` `href` opens in a new tab with `noopener noreferrer`; anything
else renders as text. The manager already refuses other schemes, raw HTML,
and images; the renderer would not draw them either.

**Actions.** A button names an action the manifest declares. `GetAppView`
returns each referenced action's title, confirmation (context filled in),
typed inputs, and whether the viewer holds its permission here: a button the
viewer may not press is disabled, titled with the permission it needs. A
`write` action always opens an inline confirmation, with fields for the
inputs the button does not preset (text, number, duration, a select for an
enum, a checkbox for a boolean). It runs through `InvokeAppAction` with a
fresh idempotency key per press. The app's message shows above the view,
and a view it returns replaces the drawn one; the panel then re-fetches.

The browser never talks to an app: every view and action is a gRPC-Web call
to the manager, which authorizes as the viewer, forwards with a signed viewer
assertion, and validates the answer.

The bundled autoscaler's UI is built this way, with no page of its own in
the SPA: its sidebar page, job panel, task badges, and node-group panel are
its views ([autoscaling.md, "In the UI"](autoscaling.md#in-the-ui)).

### Sandboxed pages

An app's sandboxed page (tier 2, `ui.pages`;
[Apps: Sandboxed pages](apps.md#sandboxed-pages)) is the one place app code
runs in the browser, and it runs outside the SPA's origin.

| Piece | Where |
| --- | --- |
| the sidebar item (a `nav` entry with `page` set) and the route `/apps/:app/pages/:page` | `App.tsx`, `pages/AppSandboxPage.tsx` |
| the frame, the bridge's message handling, and the confirmation dialog | `components/apps/SandboxedPage.tsx` |
| the CSP, the sandbox flags, the bridge script, and the request helpers | `utils/appBridge.ts` |

- **Fetch.** `GetAppPage` (authorized as the viewer like a view) returns the
  bundle and the page's RPC allowlist, each RPC marked read or write.
- **Mount.** `<iframe sandbox="allow-scripts" srcdoc=… referrerpolicy=
  "no-referrer">`. The `srcdoc` is the bundle with the CSP `<meta>` and the
  bridge `<script>` inserted first (after a leading doctype):
  `default-src 'none'; script-src 'unsafe-inline'; style-src
  'unsafe-inline'; img-src data:; connect-src 'none'; form-action 'none';
  base-uri 'none'`. The opaque origin keeps the page away from
  `localStorage`, where the token lives, and from the SPA's DOM.
- **Bridge.** The page posts `{ legion: "legion-bridge/1", id, op, ... }` to
  its parent; the SPA answers `{ legion, id, ok, result | error }` and posts
  `{ legion, event: "theme", theme }` when the theme changes. A message is
  heard only if its `source` is the page's own frame window and its origin is
  `"null"` (opaque), and only while the frame holds its first document: the
  frame's second `load` (the page navigated away) closes the bridge for good.
  At most 16 calls run at once, and 4 confirmations wait.
- **Calls.** `call` is refused unless the RPC is in the allowlist and unary.
  The request is parsed from proto3 JSON with the generated message type
  (`Manager.methods`), a write waits for the dialog, and the call goes out
  through `managerClient` (so it carries the viewer's token) with the
  `legion-app-page: <app>/<page>` header, which the manager checks against
  the page's current manifest. The reply goes back as proto3 JSON. `view` and
  `action` use `GetAppView` and `InvokeAppAction` for the page's own app;
  every action is confirmed. `navigate` takes in-app routes only.
- **Confirmation.** A modal the SPA draws over everything, outside the frame,
  naming the app, the RPC (or action), and the target taken from the request
  (`job`, `name`, `node`, `path`, ...), with the request itself below.

## Namespaces

[Namespaces](namespaces.md) reach the UI three ways, all in
`utils/namespaces.ts` and `components/NamespaceSwitcher.tsx`:

- **Paths and links.** `jobPath(id)` and `servicePath(namespace, name)` build
  every job and service link, always with the namespace segment
  (`/jobs/default/web`), and `jobIdFromParams` turns a route back into the
  canonical id the RPCs take. No component formats a job URL by hand.
- **The selection.** `useNamespaceSelection()` reads `?ns=` (names or globs,
  comma-separated), defaulting to `*` for an identity whose
  `WhoAmI.readable_namespaces` holds several and to its one namespace
  otherwise. Jobs, Services, Rollouts and Volumes send it as the list RPC's
  `namespaces`; Events filters its stream by `event.namespace`, showing
  cluster events only under All.
- **The header.** The layout's `PageHeader` renders `HeaderNamespaceControl`
  for the current top-level path: the switcher on namespaced list pages, a
  `cluster-scoped` tag on cluster pages, and nothing at all, beyond a badge,
  for a single-namespace identity. Detail pages render
  `DetailNamespaceSwitcher`, which navigates to the chosen namespace's list.

The ACL mirror's `jobScope(id)` splits the canonical id into `namespace` and
`name`, matching the manager's `AclScope::job`, and an allow rule over
namespaced resources without `namespaces` matches `default` only, as on the
manager.

## Stateful Workloads

Allocation views (node detail allocation summaries, Homepage ledger cards,
job detail allocation table, the task page's Instances table) show the
instance id, linked to its instance page, when
`LedgerAllocation.instance_id` is set. This makes a rescheduled stateful
allocation visibly the same instance. Stateless allocations are unchanged.

## Backend Compatibility Requirements

Manager server enables:

- gRPC-Web layer (`tonic_web::GrpcWebLayer`)
- CORS for allowed browser origins

Without these, browser gRPC-Web calls would fail.

### Allowed browser origins

The manager only answers browsers from allowed origins. Any web page can send
requests to `127.0.0.1:4747`, and with ACLs turned off every caller has full
access, so an allow-all policy would let any website you visit run jobs on
your cluster. The same applies to DNS rebinding, where a page reaches the manager
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
**Actions** tab (`components/TaskActions.tsx`). Its targets are the job's
allocations from `GetLedger`, narrowed to the ones that run the page's task.
The ledger carries only `task_config_id`, so the task name comes from the
state-store lookup that also carries allocation health. A token without
`state:read` cannot make that mapping. In that case every running allocation
of the job is offered (the manager still rejects an exec whose allocation runs
a different task).

The left list is the task's actions (`ListJobActions`, filtered to this task)
plus **Open a shell**. The chosen action shows:

- **Parameters**, one control per declared type: an `enum` of up to four
  values is a segmented control, five or more a chip group; `int` and
  `string` are text inputs checked as you type against `min`/`max` and
  `pattern` (the same rules as `ActionParam::validate_value`); a `sensitive`
  string is masked; `bool` is a switch. A required empty value, or any value
  that fails its check, disables **Run** with the reason under the field.
  Empty optional values are left out, so the manager applies the default.
- **Runs**: the action's `command` from the jobspec with the current values
  substituted for `${param.<name>}` (sensitive values masked). The manager
  still renders the real argv; this is a preview.
- **Run on**: one instance (chips with health dots; default the URL's
  `alloc`, else the first unhealthy, else the first), all running, or only the
  unhealthy ones. A `tty` action runs in one instance only.
- **Run**, and under it the equivalent `legion action run <job> <action>
  --alloc <id> --param k=v` line (`***` for sensitive values).
- **Result**: one row per instance with its exit code, duration, last line,
  and output (stderr tinted), opened automatically for a failure or a single
  target.

Running on several instances opens one exec session per allocation, all at
once; there is no multi-allocation RPC. A `tty` action, and the shell entry
(command plus an *Interactive (tty)* toggle), open an xterm.js terminal. The
action list, form values, and results are kept while you switch actions, and
open sessions close when you leave the page. There is no run history: results
live in the page.

Exec is the one part of the UI that does not use gRPC-Web, because browsers
cannot do bidirectional streaming over it. `ui/src/lib/execSocket.ts` opens a
WebSocket to the manager's exec bridge at `<host>:<port+1>/v1/exec/ws`.
It exchanges protobuf `ExecClientFrame`/`ExecServerFrame` messages (the same
frames the CLI uses), one per binary WS frame. The bearer token rides in the
first frame.

The exec WebSocket follows `server.tls`. The UI uses `wss://` when the
manager endpoint is `https://`, and `ws://` otherwise. When `server.tls` is
enabled, the exec listener serves TLS with the same certificate and key as
the gRPC listener. It accepts only TLS, and a plaintext `ws://` connection
fails its handshake. When TLS is off, the listener stays plaintext. The
browser has to trust the certificate on the exec port as well as on the
manager port. A certificate-warning exception you click through for the
manager port may not cover `port+1`, because some browsers (Firefox, for
example) record exceptions per host and port. If the terminal fails to
connect while the rest of the UI works, check that first. Trusting the CA
that signed the certificate covers both ports.

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
- `npm run test:smoke` (after `npm run build`): the Playwright smoke tests
  in `ui/tests/smoke`, against a fake manager in the browser
  ([Testing: UI smoke tests](testing-and-benchmarks.md#ui-smoke-tests)).
- CI (`.github/workflows/ci.yml`, job `ui-build`) runs the drift check, the
  typecheck and the build whenever `ui/**` or the proto changes.
