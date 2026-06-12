# UI Architecture

The UI is a SolidJS single-page app in `ui/` and talks directly to manager gRPC APIs over gRPC-Web.

## Stack

- SolidJS + Solid Router
- ConnectRPC (`@connectrpc/connect-web`) generated from `proto/legion.proto`
- TailwindCSS
- Vite build

## Entry Points

- App root: `ui/src/index.tsx`
- Main routing/layout: `ui/src/App.tsx`
- gRPC client setup: `ui/src/grpcClient.ts`

Manager endpoint:

- `VITE_MANAGER_ENDPOINT`
- default: `http://127.0.0.1:4747`

## Route Structure

Routes in `App.tsx`:

- `/` overview dashboard (ledger + active rollouts)
- `/nodes`
- `/jobs`
- `/rollouts`
- `/events`
- `/kv`
- `/manager`
- `/testing`

A shared layout provides nav, theme toggle, and page headers.

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
