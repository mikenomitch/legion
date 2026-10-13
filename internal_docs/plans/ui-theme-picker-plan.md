# UI Theme Picker

> **Judgement calls:** [ui-theme-picker-plan_calls.md](../calls/ui-theme-picker-plan_calls.md)

**Status: SHIPPED** (all four phases). Deliberately deferred: per-user themes
stored on the manager, importing or exporting a palette, theming the exec
terminal and status colors, and typeface or layout options.

## Problem

The UI has one look, Command Post, and one control over it: a sun/moon button
that flips light and dark. Once flipped, the choice is pinned in
`localStorage['ui-theme']` and the page stops following the OS. There is no
way back to "follow my system", and someone who finds warm stone and brass
wrong for a wall display, a long night shift or their own taste has no
alternative.

An earlier attempt (PR #34) made a different look, Swiss, the new default and
added a picker with seven palettes and six layout tweaks. It was closed
without merging. This plan keeps Command Post as the default, offers a small
set of alternative color themes, and adds the System mode.

## Prior art

- **Grafana**: Light, Dark, System, plus a handful of experimental themes,
  chosen per user in preferences. Server-stored, because Grafana has user
  profiles.
- **GitHub**: light/dark/system mode, a separate theme per mode, stored on the
  account. Picking a theme shows a live preview tile.
- **Nomad UI / Kubernetes Dashboard**: no theme choice beyond following the
  OS. Consul's UI likewise.
- **Linear, Vercel**: mode plus a short list of named themes, applied at once,
  no Save button.

What we take: three-way mode with System as the default, a short list of
named themes with a preview swatch, instant apply. What we reject for now:
server-side storage (Legion has tokens, not user profiles) and separate
light/dark theme choices (one theme defines both).

## Jobs to be done

- **Operator on call at night**: "Let me pick a darker, calmer look for
  tonight without losing my place, and have the UI go back to light in the
  morning on its own." Needs System mode and instant apply.
- **Team putting the UI on a wall screen**: "Pick a look that reads from
  across the room and matches our other dashboards." Needs a few distinct
  themes, chosen per browser.
- **Someone who just likes it the way it is**: "Don't change anything." Needs
  the default unchanged and nothing to dismiss.

Scenarios the design has to serve:

1. Ana opens the palette icon in the top bar, clicks Nord, then Dark. The page
   changes as she clicks. She reloads and it is still Nord dark, with no flash
   of brass.
2. Ana clicks the sun/moon button. The mode flips; the theme stays Nord.
3. Ana opens Customize and sets the accent to her team's yellow. Only Nord
   dark changes; Nord light and the other themes keep their colors. The
   picker warns her that button labels are now hard to read, and she darkens
   the page background until the warning clears.
4. Ana clicks Reset to defaults. The UI is Command Post following her OS, and
   nothing is left in storage.

## Goals and non-goals

Goals: Light/Dark/System mode; six built-in color themes, Command Post the
default; every color settable by hand, per theme and mode; instant apply;
saved per browser; no flash on reload; every built-in alternative meets a
stated contrast bar in both modes, and hand-set colors that miss it get a
warning.

Non-goals: changing typefaces, spacing, radii or layout; theming status
colors; sharing or importing palettes; syncing the choice across browsers.

## What exists today

`ui/src/index.css` defines ten RGB-triplet tokens (`--c-canvas` …
`--c-brass-strong`) on `:root` and `.dark`; `tailwind.config.js` maps them to
utility names. `App.tsx` owns a `theme` signal, toggles `.dark` on `<html>`
and writes `ui-theme`. Nothing else reads the key.

## Design

The surface:

- A **palette icon** in the top bar, next to the sun/moon, and an
  **Appearance** entry in ⌘K. Both open a dialog with a Mode control (Light,
  Dark, System) and a grid of theme tiles. Each tile is a swatch painted in
  that theme for the current mode.
- A **Colors** section, collapsed behind Customize, with one row per token:
  a native color input, a hex field, and a × that returns the token to the
  theme's value. Rows show the value on screen, read back from `<html>`.
  Below them, a warning names any of body text, secondary text, links and
  button labels under 4.5:1.
- Storage: `localStorage['legion-appearance'] =
  {"theme":"nord","mode":"dark","colors":{"nord":{"dark":{"brass":"#ffcc00"}}}}`.
  The default (`command-post`, `system`, no colors) is never written; Reset
  removes the key.
- DOM: `<html class="dark" data-theme="nord" style="--c-brass: 255 204 0">`.
  No `data-theme` means Command Post; hand-set colors for the theme and mode
  on screen are inline custom properties.

Themes: Command Post, Forge, Nord, Sage, Porcelain, Tomorrow Night Bright. Each is a light and a dark
block of the same ten tokens in `ui/src/appearance/themes.css`. The blocks
match `[data-theme=…]` on any element, which is what lets a swatch render in
a theme other than the active one.

An inline script in `index.html` applies the saved choice before first paint.
`ui/src/appearance/store.ts` owns it once the app runs: it sanitizes the
stored value, follows the OS while in System mode, and follows other tabs
through the `storage` event.

Contrast bar for the five alternatives: ink 7:1 on surface; muted and faint
4.5:1 on surface and canvas; links 4.5:1 on surface; button text (canvas on
brass) 4.5:1. Command Post's values are unchanged.

## Phases

1. **Store and mode.** `store.ts`, the boot script, System mode, the sun/moon
   wired through the store. Ships alone: the toggle can now return to System.
2. **Themes.** `themes.ts`, `themes.css`, the alternatives, checked
   against the contrast bar.
3. **Picker.** The dialog, the top-bar button, the ⌘K entry, Escape and
   shortcut handling while it is open, docs.
4. **Hand-set colors.** The Colors section, per-theme-per-mode storage, the
   boot script applying them, the contrast warning. Also adds Tomorrow Night
   Bright.

## Testing

- `npm run build` and `tsc --noEmit`.
- Browser run against a live manager: pick a theme and mode, check the
  attributes and the stored value, reload, flip with sun/moon, reset, and load
  a page with the app script blocked to prove the boot script alone applies
  the choice. Phone width. No console errors.
- Hand-set colors: set two by hex and one by the color input, reload, block
  the app script (boot script alone applies them), flip to light (none apply)
  and back, clear one with ×, clear all, reset.
- Contrast computed from `themes.css` for every theme in both modes.

## Rollout

Ships in the UI bundle; no manager change. The old `ui-theme` key is no longer
read, so a browser that had pinned light or dark starts on System once
(AGENTS.md: no compatibility shims). Stated in the PR.
