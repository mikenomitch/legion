# UI Jump Hints

> **Judgement calls:** [`../calls/ui-jump-hints-plan_calls.md`](../calls/ui-jump-hints-plan_calls.md)

**Status: SHIPPED.** Digit hints on the four `SharedIndex` pages plus Services
and Volumes; letter hints on the expanded sidebar. Deliberately deferred:
hints on Homepage and detail-page tables, hints past the tenth row, and a
user-remappable keymap.

## Problem statement

The UI already has a keyboard story, and it is a two-speed one. `⌘K` gets you
anywhere by name, and `j`/`k`/`o` walk the current index one row at a time.
Neither serves the most common motion in an operator's day: *this list is on
screen, and I want the third thing on it.*

Today that costs either a mouse trip or `j j j o` — four keystrokes whose count
depends on where the virtual cursor happened to be left. `⌘K` is worse for this
case, not better: it makes you type a name you can already see.

The same gap exists in the sidebar. Moving from Jobs to Workers is a mouse
click or a `⌘K` round trip through a search box, for a target that is
permanently on screen in a fixed position.

## Prior art

- **Vimium / Link Hints** (browser extensions) label every link with a letter
  sequence on a keypress. The idea we take: *the affordance teaches itself* —
  you do not memorize a mapping, you look at the label. What we reject: the
  two- and three-letter sequences and the modeful "hint mode", which are
  necessary when labelling hundreds of arbitrary links and overkill for a ten
  row table.
- **macOS / Windows menu accelerators** underline a letter *while the modifier
  is held*. This is the interaction we copy almost exactly: hold-to-reveal, no
  mode to enter or leave, and releasing the key puts everything back.
- **Slack, Discord, and most tab-based apps** bind `⌘1`…`⌘9` to "the Nth thing
  in the rail". This is why a digit on a row needs no explanation.
- **Linear** uses `g`-prefixed sequences (`g` then `i`) for navigation. Rejected:
  a sequence is invisible until you already know it, which is exactly the
  property we are trying to avoid.

## User scenarios

1. An operator is on `/jobs` after a deploy, sees `svc-api` third in the list,
   holds `⌘`, and presses `3`.
2. The same operator, now on a job detail page, holds `⌘` and glances at the
   sidebar to jump to Workers without reaching for the mouse.
3. A keyboard user who has never seen the feature holds `⌘` on the way to `⌘K`,
   notices the labels, and stops needing `⌘K` for things already on screen.

## Goals and non-goals

**Goals**

- Zero-memorization: the label is the documentation, and appears only when
  relevant.
- No mode. Nothing to enter, nothing to escape, no state left behind.
- No permanent cost to the layout when the feature is not in use.
- One mechanism, reused by every index, rather than per-page key handling.

**Non-goals**

- Labelling *every* link on the page (that is Vimium, and it needs Vimium's
  complexity).
- Hints for rows past the tenth. Ten single keystrokes is the budget; beyond
  that, `⌘K` and search are the right tools.
- A remappable keymap. Worth doing if the fixed chords prove wrong, not before.
- Replacing anything. `j`/`k`/`o`, `u`, and `⌘K` all stay exactly as they are.

## Background — what exists today

- `ui/src/App.tsx` owns the layout, the sidebar model (`navItems`, a flat list
  of sections and items), and one global `keydown` listener that implements
  `⌘K` and `u`.
- `ui/src/components/SharedIndex.tsx` is the index table used by Jobs, Workers,
  Rollouts and Managers. It already runs a global `keydown` listener for
  `j`/`k`/`o` over a `selectedIndex` signal, and publishes its rows to `⌘K`
  through `ui/src/utils/commandPalette.ts` — a module-level Solid signal that
  the mounted index writes and clears.
- `ui/src/pages/Services.tsx` and `ui/src/pages/Volumes.tsx` render their own
  tables rather than `SharedIndex`, and feed `⌘K` the same way.
- `ui/src/components/ui.tsx` holds every shared primitive.

The `commandPalette.ts` pattern — a global signal, written by whichever index is
mounted — is the shape to copy for a second registry.

## Proposed design

### 1. The surface

Hold the platform's command modifier (`⌘` on macOS, `Ctrl` elsewhere):

- every row of the page's index shows a digit, `1`–`9` then `0` for the tenth;
- every sidebar item shows a letter, prefixed with `⇧`.

Press the labelled key with the modifier still down:

```
⌘1        open the first row of the index
⌘0        open the tenth
⌘⇧O       Overview          ⌘⇧K   Workers
⌘⇧B       Jobs              ⌘⇧M   Managers
⌘⇧S       Services          ⌘⇧A   Raft
⌘⇧R       Rollouts          ⌘⇧E   Events
⌘⇧V       Volumes           ⌘⇧D   KV Store
```

Release the modifier and every label disappears. Nothing else changes.

### 2. Why the sidebar needs Shift and the rows do not

`⌘`+digit is a browser tab shortcut, but a cancelable one — pages override it
routinely (Slack, Discord). `⌘`+letter is not available at any price: `⌘S`,
`⌘W`, `⌘R`, `⌘O` and friends belong to the browser and the OS. So sidebar items
take `⌘⇧`+letter, and the badge spells `⇧B` rather than `B` so the chord on
screen is the whole chord.

Letters are assigned from the label — word initials first, then the remaining
letters, then any free letter — skipping `c i j n q t w`, which the browser
keeps for tab and window lifecycle, macOS log-out, and devtools.

### 3. Where it lives

- `ui/src/utils/jumpHints.ts` — the platform check, the visibility signal, the
  letter-assignment function, `event.code` parsing, and an owner-scoped registry
  of the current index's hrefs (`useIndexJumpTargets`).
- `ui/src/components/ui.tsx` — `<JumpHint keys="3" />`, which renders nothing
  unless the modifier is held.
- `ui/src/App.tsx` — one `keydown`/`keyup`/`blur` trio that drives the
  visibility signal, and one `keydown` handler that resolves a chord to an href
  and navigates.

Pages opt in with one hook call and one `<JumpHint>`; nothing else changes.

## Implementation phases

Single phase — the mechanism is worthless split, and it is one reviewable diff.
Adding a page later is two lines.

## Testing plan

Typecheck and build (`npm run build`), then drive a real browser against a
throwaway cluster and verify: labels appear on modifier-down and clear on
modifier-up, `blur`, and `⌘K`; `⌘1` and `⌘⇧R` navigate; `Ctrl+1` is inert on
macOS; an unassigned letter and a bare digit are inert; the collapsed sidebar
shows no letters but still answers its chords; both themes are legible.

## Rollout and backward compatibility

Front-end only, no manager involvement, no persisted state, no config. The
existing `j`/`k`/`o`, `u`, and `⌘K` bindings are untouched.

## Open questions

- Does a real Chrome/Safari/Firefox let the page win `⌘`+digit and
  `⌘⇧`+letter in every case? Chromium's non-reserved accelerators are
  cancelable and the reserved set is avoided, but this is asserted from the
  reserved-key list rather than measured on every browser. Current lean: ship,
  and treat a browser that steals a specific chord as a letter-assignment bug.

## Acceptance criteria

- Holding the modifier on `/jobs` labels the first ten rows and the sidebar.
- `⌘3` opens the third row on every index page listed above.
- `⌘⇧<letter>` opens the matching sidebar item from anywhere in the app.
- Releasing the modifier restores the page exactly, including row padding.
