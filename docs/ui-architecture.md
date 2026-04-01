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
- `/kv`
- `/manager`
- `/testing`

A shared layout provides nav, theme toggle, and page headers.

## Data Fetch Pattern

The app uses `createResource` + polling.

- Poll interval: `POLL_INTERVAL_MS` (2s)
- Route-scoped polling logic in `PollingController`
- Mutation avoids unnecessary rerenders by equality checks before updating resource state

## Page Responsibilities

- `Homepage.tsx`: ledger cards and active rollout summary.
- `Nodes.tsx`: node inventory, attributes, resource usage visualization, allocation summaries.
- `Jobs.tsx`: job list + on-demand spec retrieval.
- `Rollouts.tsx`: rollout list, per-group status, event logs, manual advance action.
- `KeyValue.tsx`: key tree browser and CRUD/patch editor.
- `Manager.tsx`: manager config snapshots.
- `TestingUtils.tsx`: placeholder (not implemented yet).

## Backend Compatibility Requirements

Manager server enables:

- gRPC-Web layer (`tonic_web::GrpcWebLayer`)
- CORS allow-all setup

Without these, browser gRPC-Web calls would fail.

## Generated API Clients

Proto compilation command is documented in `ui/README.md` and writes generated files to `ui/src/gen/`.



## Raft page

The UI includes a dedicated **Raft** page under Cluster navigation for manager raft status, recent raft history, and operator commands (election/failover/membership/join-token flows).
