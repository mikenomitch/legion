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

The manager only answers browsers from loopback origins (`localhost`,
`127.0.0.1`, `[::1]`) unless you list the UI's origin in the manager config's
`server.allowedOrigins`. If you open the UI from another host or a tunnel
domain, add that origin there. See `docs/ui-architecture.md`.


### Seeded preview mode (for frontend work)

If you are doing UI work and want predictable data without manually creating jobs/workers, start the manager with `LEGION_SEED_UI_DATA=1`.

```bash
LEGION_SEED_UI_DATA=1 cargo run -- start manager --run-worker -c configs/manager.ncl
```

This only seeds when the store is empty and creates demo workers, a demo job, and a couple of KV entries so the UI has real backend data to render.

## Appearance

The look is Swiss, with seven palettes and six tweaks a viewer picks from the
palette button in the top bar (saved per browser). Palettes live in
`src/appearance/palettes.ts`; after changing one, run `npm run gen:appearance`
to regenerate `src/appearance/palettes.css` (CI checks it with
`npm run check:appearance`) and `npm run lab:contrast` to audit it. The
structure and tweak rules are `src/appearance/swiss.css`. Details:
[UI architecture: Appearance](../docs/ui-architecture.md#appearance-palettes-and-tweaks).

## Design lab and theme studio

To work on the look of the UI, start the dev server in lab mode:

```bash
npm run lab
```

A theme studio docks on the right. Pick one of the candidate themes in
`src/themes/catalog.ts` (or the shipped look) and adjust colors, status hues,
fonts, sizes, corners, shadows and the sidebar while the real pages update.
Contrast against WCAG AA is shown next to every text color. Copy the result
as CSS tokens for `src/index.css` or as JSON for the catalog.

Data comes from a manager at `LEGION_MANAGER_PROXY` (default
`http://127.0.0.1:4747`) when one is running, and from the recorded responses
in `dev/fixtures/` when not. The studio's **Live / Fake / Auto** switch picks
explicitly. To refresh the recording from a running cluster:

```bash
npm run lab:record          # terminal 1: proxy + record
npm run lab:crawl           # terminal 2: visit every page (needs Playwright)
```

Other lab scripts: `npm run lab:shots -- --out <dir>` screenshots every
preset on the Workers, Job and Task pages; `npm run lab:contrast` audits every
preset. None of this is included in `npm run build`. See
`docs/ui-architecture.md` for details and limits.

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
- `npm run lab` – dev server with the theme studio and live/recorded data
- `npm run lab:record` / `npm run lab:crawl` – record fixtures from a live manager
- `npm run lab:shots` / `npm run lab:contrast` – screenshot or contrast-audit every theme
- `npm run gen:appearance` / `npm run check:appearance` – regenerate or check `src/appearance/palettes.css`
- `npm run typecheck` – type-check without building
- `npm run build` – type-check and produce a production build
- `npm run gen` – regenerate `src/gen` from the proto
- `npm run preview` – preview the production build locally
