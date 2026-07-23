# UI Architecture

The UI is a SolidJS single-page app in `ui/` and talks directly to manager gRPC APIs over gRPC-Web.

## Stack

- SolidJS + Solid Router
- ConnectRPC (`@connectrpc/connect-web`) generated from `proto/legion.proto`
- TailwindCSS
- Vite build
- Self-hosted fonts via `@fontsource-variable` (Space Grotesk for display,
  Inter for text, JetBrains Mono for data) — no CDN requests

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
  `EmptyState`, `Spinner`/`LoadingRows`, `Button`/`ConfirmButton` (two-step
  destructive confirm)/`CopyButton`, `CodeBlock`, `DefList`, form classes
  (`inputClass`, `selectClass`, …) and table classes (`tableClass`,
  `thClass`, `tdClass`, …). Add new UI here first.
- **Status vocabulary**: `ui/src/utils/status.ts` maps every status string
  the manager emits to one of six tones (`ok`/`warn`/`danger`/`info`/
  `neutral`/`accent`) and exposes the badge/dot/text classes for each, so
  the same word gets the same color everywhere. `StatusBadge` uses it
  automatically.

## Entry Points

- App root: `ui/src/index.tsx`
- Main routing/layout: `ui/src/App.tsx`
- gRPC client setup: `ui/src/grpcClient.ts`

Manager endpoint:

- `VITE_MANAGER_ENDPOINT`
- default: `http://127.0.0.1:4747`

## Route Structure

Routes in `App.tsx`:

- `/` overview dashboard (KPI stat cards, active rollouts, workers and
  allocations tables)
- `/jobs` (+ `/jobs/:id`, `/jobs/:id/tasks/:taskName`, `/jobs/:id/specs/:configId`)
- `/rollouts` (+ `/rollouts/:id`); `/deployments` redirects here
- `/nodes` (+ `/nodes/:id`)
- `/manager` (+ `/manager/:id`)
- `/raft`, `/events`, `/kv`, `/access`, `/api`, `/state-explorer`,
  `/observability` (shown only when the manager reports o11y enabled),
  `/testing`

A shared layout provides the grouped sidebar nav, a slim topbar
(breadcrumbs, ⌘K search, theme toggle), and page headers for top-level
routes; detail pages render their own `PageHeader`.

## Data Fetch Pattern

The app uses `createResource` + polling.

- Poll interval: `POLL_INTERVAL_MS` (2s)
- Route-scoped polling logic in `PollingController`
- Mutation avoids unnecessary rerenders by equality checks before updating resource state

Exception: the Events page (`Events.tsx`) consumes the `StreamEvents`
server-streaming RPC over gRPC-Web (an async iterable from ConnectRPC) instead
of polling. It seeds with `ListEvents`, tails the stream, and reconnects with
an `afterId` cursor so no events are missed across disconnects.

## Page Responsibilities

- `Homepage.tsx`: ledger cards and active rollout summary.
- `Nodes.tsx`: node inventory, attributes, resource usage visualization, allocation summaries.
- `Jobs.tsx`: job list + on-demand spec retrieval. The job detail view
  (`pages/details/JobDetail.tsx`) marks stateful tasks with a
  "stateful · slot/keyed" badge (plus storage path) and, for jobs with
  stateful tasks or live `ListInstances` rows, renders a polled read-only
  Instances panel (id, mode, color-coded state, slot, current allocation,
  storage handle, created). Keyed instance create/destroy is CLI-only
  (`legion instance ...`); the UI does not mutate instances.
- `Rollouts.tsx`: rollout list, per-group status, event logs, manual advance action.
- `Events.tsx`: live scheduler event feed with filtering and pause/resume.
- `KeyValue.tsx`: key tree browser and CRUD/patch editor.
- `Manager.tsx`: manager config snapshots.
- `TestingUtils.tsx`: placeholder (not implemented yet).

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

## Generated API Clients

Proto compilation command is documented in `ui/README.md` and writes generated files to `ui/src/gen/`.



## Raft page

The UI includes a dedicated **Raft** page under Cluster navigation for manager raft status, recent raft history, and operator commands (election/failover/membership/join-token flows).

## Exec & Actions (interactive terminal)

The task detail page (`pages/details/JobDetail.tsx` → `TaskDetail.tsx`) has an
**Exec & Actions** section listing the job's running allocations (from
`GetLedger`). Each allocation gets an `AllocationExecPanel` with a command
launcher (interactive-tty toggle) and, for tasks that declare them, action
forms generated from the parameter schema (`ListJobActions`).

Unlike the rest of the UI, exec does **not** use gRPC-Web — browsers cannot do
bidirectional streaming over it. `src/lib/execSocket.ts` opens a **WebSocket**
to the manager's exec bridge (`ws://<host>:<port+1>/v1/exec/ws`), exchanging
protobuf `ExecClientFrame`/`ExecServerFrame` messages (the same frames the CLI
uses) one per binary WS frame; the bearer token rides in the first frame.
`components/ExecTerminal.tsx` binds those frames to an **xterm.js** terminal
(`@xterm/xterm` + `@xterm/addon-fit`), driving resize frames on container
resize. The `<Terminal>` contract (open/write/onData/resize) is kept to the
xterm.js surface so the renderer can later be swapped for a libghostty-vt
wrapper without touching the transport (see the plan's Q5).
