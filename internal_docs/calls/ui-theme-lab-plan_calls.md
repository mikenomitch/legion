# UI theme lab — judgement calls

Calls made while building the theme lab
([plan](../plans/ui-theme-lab-plan.md)). Ordered by project impact, highest
first; the number is the rank.

## 1. The lab is a dev-server tool, not a theme picker in the product

**Call.** Everything under `ui/src/themes/` loads only behind
`import.meta.env.DEV`, and the fake-data server only in `npm run lab` /
`lab:record`. A production build has no theme switcher, no candidate themes and
none of the forty extra fonts.

**Alternative.** Ship the twenty themes as a user-facing picker (Settings →
Appearance), as Grafana and GitHub do.

**Trade-off.** Operators cannot choose a look; the exploration has to end in a
decision rather than in configuration. The fonts would also have cost every
user a larger install and a heavier `@font-face` set.

**Why.** The request was to pick one direction, not to offer twenty. A picker
is a permanent surface to keep accessible and consistent across every future
page (simplicity/ease). Keeping it dev-only makes the whole lab free to leave
in the repo or delete after the pick.

*Revisited in round three:* the maintainer now plans a picker saved to
localStorage. The lab itself stays dev-only; entry 3 is the shape it hands the
picker, and entry 2 records that it shipped as the only look.

## 2. Swiss replaces Command Post outright; the picker offers Swiss only

**Call.** Phase 3 makes Swiss Red the default in both modes and the picker's
seven palettes and six tweaks are all Swiss. Command Post, the previous look,
is not offered; it survives only as a reference preset in the dev-only lab.

**Alternatives.** Offer Command Post (and perhaps Forge) as extra "themes"
beside the Swiss palettes; or keep Command Post as the default and make Swiss
opt-in.

**Trade-off.** Anyone who liked the warm brass look loses it with no setting
to bring it back, and every page changes at once. A second family would also
have had to be written against the tweaks (most of them are Swiss-specific:
rules, masthead, signal square).

**Why.** The maintainer picked Swiss ("Swiss is my favorite") and asked for
variations "in color (for the default)", which reads as Swiss being the
default and the picker varying it. One family keeps every tweak meaningful
with every palette (simplicity/ease), and AGENTS.md says to pick the right
default for a new cluster rather than preserve an old look. The user-facing
surface is a palette button in the top bar and an **Appearance** entry in ⌘K.

## 3. Swiss is a palette × tweaks system, applied as attributes on `<html>`

**Call.** Round three splits Swiss into **palettes** (colors only, each a
light/dark pair: Red, Zurich Blue, Signal Yellow, Duotone, Concrete, Paper,
Mono) and **tweaks** (independent structural axes: typeface, titles, density,
cards, page header, nav rail). A picker sets `data-palette`, the `dark` class
and `data-tweak-<axis>` on `<html>` and stores
`{"palette":"concrete","mode":"dark","tweaks":{"density":"compact"}}`.

**Alternatives.** Ten fixed light/dark variations (what round three started
as, before the maintainer said a picker is coming); one `data-theme` naming a
combination; or tweak classes on components.

**Trade-off.** Combinations are not individually designed — each tweak is
written against tokens and has to hold up with every palette in both modes,
and some pairings are weaker (the black rail barely contrasts with Swiss
Dark's near-black page). Seven palettes × 4 × 2 × 2 × 3 × 2 × 2 tweak options
is 1,344 looks per mode; only defaults, each tweak alone and three
combinations are screenshotted.

**Why.** A picker needs orthogonal choices, not a list of finished themes:
"Concrete, compact, black rail" should be three independent settings, each
undoable on its own (extensibility: a new palette or tweak is one block of
CSS). Plain attributes keep the production picker free of lab code and let it
apply the stored choice before first paint.

## 4. The choice is per browser (localStorage), not per user

**Call.** The picker stores `{"palette":…,"mode":…,"tweaks":{…}}` under the
`legion-appearance` localStorage key. Nothing reaches the manager.

**Alternatives.** A per-identity preference on the manager (a KV path or a
new RPC), so the look follows a person across machines; or a cluster-wide
default set by the operator.

**Trade-off.** The look does not follow a person to another browser, and an
operator cannot set a house style for everyone.

**Why.** It is what the maintainer described ("a theme picker that can save to
local storage"). It needs no RPC, no ACL resource, no idempotency and no
migration, and it works before sign-in and with ACLs off (simplicity/ease). A
server-side preference can be added later on top of the same stored shape
(extensibility); it is logged in TODO.md.

## 5. Palette colors are generated, checked-in static CSS keyed on attributes

**Call.** `scripts/gen-appearance-css.mjs` turns `palettes.ts` into
`src/appearance/palettes.css` (every `--c-*` triplet, the status ramps,
`--c-signal`, `--signal-ink`), keyed on `data-palette` and `.dark`. An inline
script in `index.html` sets the attributes from localStorage before first
paint; `npm run check:appearance` in CI fails when the CSS is stale.

**Alternatives.** Compute the variables in JavaScript at startup from
`palettes.ts` (no generated file); or generate the CSS at build time with a
Vite plugin (no checked-in file).

**Trade-off.** A generated file in the repo and a step to remember when
editing a palette (CI catches a miss). The boot script duplicates a few lines
of `store.ts`.

**Why.** Module scripts run after the first paint can happen, so runtime
variables would flash Swiss Red before a saved palette applied. Static CSS
paints correctly with no theme code at all (performance), and a checked-in
file is reviewable in a diff and needs no build-tool plugin (simplicity).

## 6. Mode has a third value, System, and it is the default

**Call.** Mode is Light, Dark or System. With nothing stored the page follows
`prefers-color-scheme` and keeps following it; the sun/moon button sets an
explicit Light or Dark.

**Alternatives.** Keep the old two-state toggle (an explicit choice was saved
on first toggle and the OS was read only when nothing was stored).

**Trade-off.** One more choice in the picker, and the sun/moon button now
leaves System silently.

**Why.** "Follow my OS" is the expectation set by every OS-level dark mode,
and a picker is the natural place for it. The old key (`ui-theme`) is dropped
without migration, per AGENTS.md, so a returning browser starts on System.

## 7. Type, shape, depth and status became CSS variables

**Call.** Tailwind's `font-*`, `rounded-*`, `shadow-card/pop` and the
`emerald/amber/red/sky` 200–800 shades now read CSS variables whose defaults
are the old values; primitives gained `data-ui` hooks.

**Alternative.** Per-theme Tailwind configs and a rebuild per theme, or
theme-aware component props.

**Trade-off.** One indirection on every themed utility, and the `data-ui`
hooks become a contract: renaming one silently drops a theme's structural CSS.
`docs/ui-architecture.md` lists them so refactors keep them.

**Why.** It is the only option that lets a theme change live without touching
components, and it is behaviour-preserving for the shipped look
(extensibility: the next reskin is a token change).

## 8. "Notes page" read as the Workers page; "allocation page" as the task page

**Call.** The request named the Notes page, the allocation page and the job
page. The screenshots show Workers (`/nodes`), the task detail page
(`/jobs/:id/tasks/:task`, labeled "Allocation" in the gallery), and Job.

**Alternative.** Ask before shooting; or shoot the worker *detail* page.

**Trade-off.** If "Notes" meant something else, the user sees the wrong page
for one of three. There is no page called Notes, and the task page is the one
that lists a task's allocations (endpoints, health, runtime allocations, exec,
logs).

**Why.** `/nodes` is literally "nodes"; the index shows every theme's table,
status and badge treatment at once, which is what a look is judged on.

## 9. Each candidate theme is designed for one mode

**Call.** Every catalog theme is either light or dark (11 dark, 9 light), and
the lab pins the app to that mode.

**Alternative.** Forty palettes: a light and a dark version of each.

**Trade-off.** The chosen theme still needs its second mode designed in phase
3, and the app's sun/moon toggle mismatches colors while a lab theme is on.

**Why.** Twenty strong directions beat twenty pairs of half-considered ones;
the second mode is cheap once the direction is fixed.

## 10. "A dark/light mode for each" means the three finalists, and Command Post's is the shipped dark

**Call.** Round two adds the opposite mode for Command Post, Forge and Swiss —
not for each of the fifteen riffs. Command Post's opposite (dark) already
ships, so it is shown unchanged; Forge Light and Swiss Dark are new.

**Alternatives.** Opposite modes for all fifteen riffs (thirty more
palettes); or a redesigned Command Post dark.

**Trade-off.** A riff picked in round three still needs its other mode
designed, and the shipped dark is not re-examined.

**Why.** "Each" in the request most naturally refers to "these" three. Forty
more palettes before the direction is chosen would spend effort on themes that
will be discarded (simplicity/ease). Showing the shipped dark untouched keeps it
an honest baseline.

## 11. Each riff changes one or two things on purpose

**Call.** Every riff keeps its family's structure and moves one or two
variables: Command Post's accent, temperature, frame, density, darkness;
Forge's metal, heat and type; Swiss's signal, grid, volume and type. The
gallery names what changed on every card.

**Alternative.** Five free reinterpretations per family, as in round one.

**Trade-off.** Some riffs are subtle side by side (Marble vs Command Post),
and combinations (Forge's type with Oxide's palette) are not shown — the
studio exists for those.

**Why.** Isolated changes make the feedback attributable: liking Lausanne
over Swiss says "softer paper", not just "this one". It also makes the pick
cheap to compose in the studio.

## 12. Fake data is recorded real traffic, stored as proto3 JSON

**Call.** The dev server records the manager's unary gRPC-web responses into
`ui/dev/fixtures/<Method>.json` (decoded with the generated descriptors) and
replays them, shifting timestamps by the time since recording.

**Alternatives.** A hand-written mock transport in `grpcClient.ts`; binary
fixtures; the manager's `LEGION_SEED_UI_DATA` seed.

**Trade-off.** Fixtures are one cluster at one moment, they miss proto fields
added later until re-recorded, mutations are not replayed (an unrecorded
mutating call fails with an explicit message), and streams are not recorded.

**Why.** Recording keeps every page's data internally consistent for free;
JSON keeps it editable; doing it in the dev server keeps app code untouched.
The seed has two workers and no running tasks, too thin to judge a theme.

## 13. Structural theme CSS hangs off `data-ui` hooks with attribute selectors

**Call.** `themes.css` uses `:root[data-design='x'] [data-ui='…']` rules; the
studio's tweaks use `:root[data-design]` and win by source order.

**Alternative.** Theme-aware components (a `variant` prop per theme).

**Trade-off.** Specificity is the mechanism: a later utility with `!important`
or a higher-specificity rule could override a theme.

**Why.** It keeps every component unaware of themes, which is what makes the
lab deletable.

## 14. Riffs inherit by id prefix, and Swiss's structure is drawn from tokens

**Call.** Riff ids are `<family>-<riff>` and family rules match
`:root[data-design|='<family>']`. Swiss's rules, which hard-coded black,
white and red, now use `--c-ink`, `--c-canvas` and a new optional
`--c-signal`; Vexillum and Zurich Blue vary the signal the same way.

**Alternative.** Copy the family's rules per riff, or add a second
`data-design-family` attribute.

**Trade-off.** Naming is now load-bearing: a round-one theme whose id began
with `forge-` or `swiss-` would silently pick up those rules.

**Why.** One rule set per family keeps riffs to a palette plus their single
structural move, and the token rewrite is what makes Swiss Dark free.

## 15. Text on a signal fill is computed, not chosen per palette

**Call.** The engine sets `--signal-ink` to whichever of white or near-black
has more contrast on the signal color, and every signal-filled element (the
active nav item in Yellow, Blue and the black rail) uses it.

**Alternative.** White everywhere, with per-palette overrides.

**Trade-off.** Bright dark-mode reds get black text on the active item, which
is less conventional than white.

**Why.** White on the brighter dark-mode reds is about 3.7:1, under AA; the
computed choice is never below 4.77:1 across all palettes, and new palettes
get it for free.

## 16. Screenshot data came from a real local cluster, with two local workarounds

**Call.** The gallery shots come from a manager plus seven named workers
(three pools; one drained, one lost) and four jobs, all on one host. Two
workarounds were applied locally and **not committed**: the worker disk health
check was loosened, because the sandbox's read-only mounts and sparse quota
failed it on every worker; and the edge proxy got a static port, because
"nodes" on one host collided on dynamic ports.

**Alternative.** Fake data from the start, or keep the all-red health column.

**Trade-off.** The Workers page shows healthy workers where the unmodified
binary in that sandbox would show every worker unhealthy, which would look like
a real cluster problem in every screenshot.

**Why.** The shots are for judging color and type on realistic states, and an
artifact of the build machine would have dominated every Workers screenshot.
An attempt at a persistently unhealthy allocation on the Job page was dropped:
Legion correctly reaps it within two minutes, so it could not stay consistent
across a 2.5-minute shoot.

Round two was shot on the recorded data (`npm run lab` with the Fake source)
rather than the live cluster, so every shot of all twenty-one themes shows
byte-identical data; the recording was taken from the same cluster right
after round one.

## 17. The studio renders in a shadow root

**Call.** The panel is a Solid tree inside a shadow root with its own CSS.

**Alternative.** Render it in the app's DOM with Tailwind classes.

**Trade-off.** A second small stylesheet that does not use the design system.

**Why.** A theme editor must not be restyled by the theme it edits; Brutalist's
2px borders or Phosphor's scanlines would otherwise reach the panel.

## 18. The build ships every offered typeface; the browser fetches one

**Call.** `index.css` imports Inter Tight, Roboto Mono, Archivo, Geist and
Geist Mono. Inter, Space Grotesk and Fira Code (Command Post) moved to the
lab.

**Alternatives.** Load a typeface's CSS on demand when the Typeface tweak
changes; or offer only Inter Tight.

**Trade-off.** The `dist/` folder carries about two dozen more woff2 files.

**Why.** `@font-face` downloads nothing until text renders in the family, so a
viewer pays only for the face they use, and static imports keep the
no-flash-on-reload property that on-demand loading would break.

## 19. The picker is a dialog from the top bar, not a Settings page

**Call.** A palette icon beside the sun/moon (and **Appearance** in ⌘K) opens
a modal like the keyboard cheat sheet. Changes apply live; there is no Save.

**Alternatives.** A `/settings` route; a popover anchored to the button.

**Trade-off.** No URL to link to the picker, and the dialog covers the page it
is restyling (it is translucent at the edges only).

**Why.** Legion has no Settings page and appearance is its only per-viewer
setting; a route for one panel is overhead (simplicity). The dialog reuses the
existing modal pattern, Escape handling and focus rules.

## 20. The shipped Swiss rules coexist with the lab through selectors

**Call.** `swiss.css` scopes the Swiss structure to
`:root:is(:not([data-design]), [data-design|='swiss'])` and repeats the
attribute on token-setting tweak rules (`[data-tweak-type='geist']
[data-tweak-type]`); palette colors step aside under `[data-design]`. The lab
engine applies palette, tweaks and mode through the app's appearance store.

**Alternatives.** Delete the lab now that Swiss shipped; or put the lab's
token sheet in a CSS cascade layer.

**Trade-off.** Selectors that need a comment to explain their specificity.

**Why.** The lab is still how palettes are audited and screenshotted, and it
costs production nothing. `:is()` keeps the specificity every lab rule was
written against, and routing the lab through the store keeps the app's
sun/moon toggle and picker in step with a preset instead of fighting it.
