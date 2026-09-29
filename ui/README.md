# Legion UI

A minimal Solid.js + Tailwind front-end for interacting with the Legion manager over gRPC.

## Getting started

```bash
npm install
npm run dev
```

The UI defaults to talking to the manager at `http://127.0.0.1:4747`. Set `VITE_MANAGER_ENDPOINT` to override:

```bash
VITE_MANAGER_ENDPOINT="http://192.168.1.25:4747" npm run dev
```


### Seeded preview mode (for frontend work)

If you are doing UI work and want predictable data without manually creating jobs/workers, start the manager with `LEGION_SEED_UI_DATA=1`.

```bash
LEGION_SEED_UI_DATA=1 cargo run -- start manager --run-worker -c configs/manager.ncl
```

This only seeds when the store is empty and creates demo workers, a demo job, and a couple of KV entries so the UI has real backend data to render.

## Generating gRPC clients

The generated client stubs live in `src/gen`. Regenerate them after any change
to `proto/legion.proto` (needs `protoc` on the PATH; the two plugins are npm
dev dependencies):

```bash
npm run gen
```

CI runs the same command on every UI or proto change and fails if the
committed files differ, so a proto change cannot merge without its client.

## Available scripts

- `npm run dev` – start the dev server
- `npm run typecheck` – type-check without building
- `npm run build` – type-check and produce a production build
- `npm run gen` – regenerate `src/gen` from the proto
- `npm run preview` – preview the production build locally
