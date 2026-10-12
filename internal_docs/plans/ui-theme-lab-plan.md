# UI theme lab: pick a new look without touching the UX

> **Judgement calls:** [ui-theme-lab-plan_calls.md](../calls/ui-theme-lab-plan_calls.md)

**Status: SHIPPED.** Phases 1–3 are built. Phase 1–2 (the tokenized design
system, twenty candidate themes, the dev-only theme studio and recorded fake
data) and rounds two and three of the exploration landed on
`claude/ui-design-variations-ibx1qw`; phase 3 shipped Swiss as the default
look with an appearance picker over its seven palettes and six tweaks (see
[Phase 3](#phase-3-ship-swiss-and-the-picker)). Deliberately deferred: a
per-user (server-side) appearance preference, opposite-mode palettes for the
non-Swiss riffs, and deciding whether the lab stays.

## Problem

The UI's UX is in good shape (polish plan, jump hints), but its look — "command
post": warm stone, brass, Space Grotesk/Inter/Fira Code — was never compared
against alternatives. Comparing alternatives today means editing
`index.css`, rebuilding, and clicking through pages by hand, one look at a
time, against whatever cluster happens to be running. Only colors were
variables; type, corner radius, shadows and status hues were hard-coded
utility classes, so most of a "different look" could not be expressed without
touching components.

## Prior art

- **Nomad UI / Consul UI** ship one look with a light/dark switch; their design
  system (HashiCorp Helios) is token-driven, which is what makes product-wide
  reskins tractable.
- **Grafana, GitHub, Linear** expose a small set of curated themes rather than
  free-form editing to end users, and keep the editing tools internal.
- **Storybook-style theme editors** and Radix Themes' playground let designers
  change tokens live against real components — the model for the studio.

Taken: tokens for everything themeable, curated presets, an internal editor.
Rejected for now: a theme picker for end users (see calls doc, entry 1). Round
three reversed this for Swiss only; see phase 3.

## Jobs to be done

**Maintainer choosing the look**
- When I am deciding how Legion should look, I want to see several genuinely
  different directions on the same real pages and data, so I can compare them
  side by side instead of imagining them.
- When a direction is close but not right, I want to change its colors, fonts
  and shape on the live UI and see the result immediately, so I can converge
  without an edit-rebuild loop.
- When I have settled, I want the result as code I can paste, so shipping it is
  a small, reviewable change.

**Frontend contributor**
- When I work on the UI without a cluster, I want realistic data on every page,
  so I can see layouts and states that only appear with real workers and jobs.

**Cluster operator / on-call responder** (the eventual audience of the chosen
theme, not users of the lab): long sessions, dense tables, status that must be
readable at a glance, often in a dim room or a bright one. Every candidate is
held to WCAG AA for text and status badges for them.

*Not in scope:* the autonomous agent persona — nothing here changes the API or
CLI.

## Goals and non-goals

Goals: every themeable property is a CSS variable or a stable structural hook;
twenty distinct, accessible candidates; a live editor; fake data good enough
to judge a theme; nothing in production builds.

Non-goals: changing any page's UX or layout; a production theme picker; per-user
theme storage on the manager.

## Background — what existed

`ui/src/index.css` defined color tokens on `:root`/`.dark`;
`ui/tailwind.config.js` mapped them to color names. Fonts, radius and shadows
were Tailwind config values; status colors were Tailwind's `emerald`/`amber`/
`red`/`sky` in `ui/src/utils/status.ts` and a few components.

## Design

1. **Tokens.** Tailwind's `fontFamily`, `borderRadius`, `boxShadow`, and the
   four status palettes read CSS variables whose defaults are today's values
   (no visual change). Primitives carry `data-ui` hooks for structural theme
   CSS. (`docs/ui-architecture.md` → Design System.)
2. **Catalog.** `ui/src/themes/catalog.ts`: each theme is data — mode, ten
   colors, four status hues, optional sidebar overrides, three font stacks,
   radius scale, shadows, and a rationale and audience line.
   `ui/src/themes/themes.css` holds per-theme structure keyed on
   `html[data-design]`.
3. **Studio.** `ui/src/themes/studio.tsx`, rendered in a shadow root, edits a
   working copy of a preset and applies it through `engine.ts`; exports CSS
   tokens and catalog JSON; saves named presets in the browser.
4. **Data.** `ui/dev/fixtures-plugin.ts`: `npm run lab:record` saves every
   unary manager response as proto3 JSON; `npm run lab` serves live or
   recorded per a cookie, defaulting to live-when-reachable.

The surface, as typed:

```bash
npm run lab                       # studio + Auto/Live/Fake data
open 'http://localhost:5173/jobs/checkout?design=swiss'
npm run lab:record & npm run lab:crawl
npm run lab:shots -- --out ../.lab-shots
npm run lab:contrast
```

## Round two: finalists, opposite modes, riffs

The maintainer shortlisted Command Post, Forge and Swiss. Each now has:

- **Its opposite mode.** Command Post's is the shipped dark theme, unchanged;
  Forge gets Forge Light; Swiss gets Swiss Dark.
- **Five riffs**, each changing one or two things on purpose so a reaction
  points at a choice: Command Post → Vexillum (red accent, gold structure),
  Marble (cool neutrals, soft depth), Brass Rail (dark rail on light), Field
  Desk (compact density), Lamplight (warm-brown dark). Forge → Foundry (blue
  steel), Crucible (glow), Oxide (rust), Anvil (neutral graphite, Hubot Sans),
  Shipyard (Big Shoulders, amber). Swiss → Neue Grafik (visible grid), Zurich
  Blue (cobalt signal), Poster (red rail, 64px titles), Lausanne (warm paper),
  Basel Mono (lowercase titles, mono tables).

Riffs are named `<family>-<riff>` and inherit their family's structural CSS
through `[data-design|='<family>']`; Swiss's structure was rewritten in terms
of `--c-ink`, `--c-canvas` and a new `--c-signal` token so its dark mode and
riffs get it for free. All thirty-nine presets are in the studio, grouped by
family.

## Round three: Swiss as palettes × tweaks

The maintainer picked Swiss and said a theme picker saved to localStorage is
coming, so the variations are split into two independent kinds:

- **Palettes** — colors only, each designed as a light/dark pair: Swiss Red
  (default), Zurich Blue, Signal Yellow, Duotone, Concrete, Paper, Mono.
- **Tweaks** — structural axes, each with a default: typeface (Inter Tight ·
  Akzidenz · Geist · Mono), titles (Bold · Hairline), density (Comfortable ·
  Compact), cards (Rules · Boxed · Soft), page header (Plain · Masthead), nav
  rail (Plain · Black).

The picker contract is three things on `<html>`: `data-palette`, the `dark`
class, and `data-tweak-<axis>` for each non-default tweak; the stored value is
`{ palette, mode, tweaks }`. All tweak and palette rules live in
`ui/src/themes/themes.css` under "Swiss picker" and are written against
tokens, so they combine freely in both modes. The studio's Picker section
drives exactly these attributes, and the app's own light/dark toggle swaps a
palette's colors while keeping every tweak.

## Phases

1. **Tokens + catalog + screenshots** (built). Behaviour-preserving
   tokenization, twenty themes, `lab:shots`, `lab:contrast`.
2. **Studio + fake data** (built). Live editing, presets, export, record/replay.
3. **Ship Swiss and a picker** (shipped 2026-10-12). See below.

## Phase 3: ship Swiss and the picker

**Jobs to be done.**

- *An operator* opening the UI gets Swiss in the mode their OS uses, with no
  setup.
- *An operator who stares at it all day* picks a palette that suits their
  screen and eyes (Paper's warm off-white, Concrete's grey) and a density
  that fits their fleet (Compact), and it stays that way on reload.
- *A contributor* changes a palette's colors in one file and CI tells them if
  they forgot to regenerate the CSS; the contrast audit tells them if it
  fails AA.

**Surface.** A palette icon in the top bar beside the sun/moon, and
**Appearance** in the ⌘K palette, open a dialog with Mode (Light · Dark ·
System), Palette (seven swatches) and one segmented control per tweak, plus
**Reset to defaults**. Changes apply live and save to
`localStorage['legion-appearance']` as
`{"palette":"concrete","mode":"dark","tweaks":{"density":"compact"}}`;
defaults are never stored. The page reads `data-palette`, `.dark` and
`data-tweak-<axis>` on `<html>`.

**Design.**

- `ui/src/appearance/palettes.ts` owns the palettes and tweaks (moved out of
  the lab; the lab's `swiss.ts` now imports them).
- `scripts/gen-appearance-css.mjs` generates `ui/src/appearance/palettes.css`
  (all color tokens per palette and mode). Swiss Red is `:root`/`.dark`; the
  rest key on `data-palette` and step aside under `[data-design]` for the lab.
  `npm run check:appearance` runs in CI.
- `ui/src/appearance/swiss.css` holds the Swiss structure and every palette
  and tweak rule, moved from the lab's `themes.css`.
- `index.css` carries Swiss's type, radius and shadow tokens and imports the
  offered typefaces (Inter Tight, Roboto Mono, Archivo, Geist, Geist Mono).
- An inline script in `index.html` applies the stored choice before first
  paint; `ui/src/appearance/store.ts` owns it at runtime (sanitizing, System
  mode, cross-tab sync). `App.tsx`'s old `ui-theme` signal is gone.
- The lab keeps working: its engine routes palette, tweaks and mode through
  the store, and Command Post becomes a reference preset.

**Testing.** Typecheck and build (no lab code or Command Post font in
`dist/`), `lab:contrast` (every Swiss palette passes AA in both modes),
`check:appearance`, and a browser run: pick a palette and tweaks, switch mode,
reload (attributes present before the app script runs), sun/moon keeps
tweaks, reset clears the key, phone width.

**Rollout.** A visible change to every page in one PR. No migration: the old
`ui-theme` key is ignored, so returning browsers start on System.

## Testing

`npm run typecheck` and `npm run build` (the build must not contain lab code:
check that no `themes/` chunk or lab font is emitted). `npm run lab:contrast`
gates every candidate at AA. `lab:shots` against a live cluster and against
the recording is the visual check.

## Rollout

Phases 1–2 change nothing a user sees: defaults equal the shipped values and
the lab is dev-only. Phase 3 is a visible change to every page and goes out as
its own PR with before/after screenshots.

## Acceptance criteria

- Phase 1: production build visually identical to before; twenty presets pass
  `lab:contrast`.
- Phase 2: `npm run lab` with no manager running renders the Workers, Job and
  Task pages with data; studio edits apply live and survive a reload.
- Phase 3: Swiss is the default in both modes; every palette passes AA in
  both modes; a saved choice survives a reload with no flash.
