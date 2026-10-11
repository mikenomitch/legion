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

## 2. Type, shape, depth and status became CSS variables

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

## 3. "Notes page" read as the Workers page; "allocation page" as the task page

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

## 4. Each candidate theme is designed for one mode

**Call.** Every catalog theme is either light or dark (11 dark, 9 light), and
the lab pins the app to that mode.

**Alternative.** Forty palettes: a light and a dark version of each.

**Trade-off.** The chosen theme still needs its second mode designed in phase
3, and the app's sun/moon toggle mismatches colors while a lab theme is on.

**Why.** Twenty strong directions beat twenty pairs of half-considered ones;
the second mode is cheap once the direction is fixed.

## 5. "A dark/light mode for each" means the three finalists, and Command Post's is the shipped dark

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

## 6. Each riff changes one or two things on purpose

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

## 7. Fake data is recorded real traffic, stored as proto3 JSON

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

## 8. Structural theme CSS hangs off `data-ui` hooks with attribute selectors

**Call.** `themes.css` uses `:root[data-design='x'] [data-ui='…']` rules; the
studio's tweaks use `:root[data-design]` and win by source order.

**Alternative.** Theme-aware components (a `variant` prop per theme).

**Trade-off.** Specificity is the mechanism: a later utility with `!important`
or a higher-specificity rule could override a theme.

**Why.** It keeps every component unaware of themes, which is what makes the
lab deletable.

## 9. Riffs inherit by id prefix, and Swiss's structure is drawn from tokens

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

## 10. Screenshot data came from a real local cluster, with two local workarounds

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

## 11. The studio renders in a shadow root

**Call.** The panel is a Solid tree inside a shadow root with its own CSS.

**Alternative.** Render it in the app's DOM with Tailwind classes.

**Trade-off.** A second small stylesheet that does not use the design system.

**Why.** A theme editor must not be restyled by the theme it edits; Brutalist's
2px borders or Phosphor's scanlines would otherwise reach the panel.
