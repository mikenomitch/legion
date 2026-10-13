# UI Theme Picker: Judgement Calls

Calls made building [ui-theme-picker-plan.md](../plans/ui-theme-picker-plan.md).
Ordered by project impact, highest first; the number is the rank.

## 1. Command Post stays the default; Swiss is not offered

**Call.** The default look is unchanged. The picker offers Command Post plus
four color-only alternatives, and none of them is Swiss.

**Alternatives.** (a) Ship PR #34 as it was: Swiss as the default with seven
palettes and six layout tweaks. (b) Keep Command Post as the default but
offer Swiss as one choice.

**Trade-off.** The picker can only recolor. Anyone who wanted a structurally
different look (square corners, oversized titles, denser tables) gets nothing
here.

**Why (simplicity).** The request was a built-in picker without Swiss, after
#34 was closed. Keeping the default means nobody's UI changes unless they ask,
and color-only themes need no structural CSS.

## 2. Themes recolor the ten existing tokens and nothing else

**Call.** A theme is a light and a dark block of the same ten `--c-*` tokens
`index.css` already defines. Typefaces, radii, shadows, status colors and the
exec terminal are shared by every theme.

**Alternatives.** Port the lab's full theme model: per-theme fonts, radius
scale, shadows, status hues and structural CSS keyed on the theme.

**Trade-off.** Themes are less distinctive. Status greens and reds are the same
in Nord as in Forge, and the terminal cursor stays brass.

**Why (simplicity).** No component changes, no new font downloads, and the
whole feature is two small files plus a dialog. A new theme is one CSS block
and one line in `themes.ts`.

## 3. Theme blocks match any element, so swatches need no duplicate data

**Call.** `[data-theme='x']` and `.dark [data-theme='x']` set the tokens on
whatever element carries the attribute. A swatch is ordinary markup with
`data-theme` on it and `bg-canvas`/`bg-surface`/`bg-brass` inside. Command Post
is repeated in `themes.css` (it is also `:root`) so its swatch renders while
another theme is active.

**Alternatives.** Keep swatch colors as hex values in `themes.ts`, or generate
`themes.css` from a TS source as #34 did.

**Trade-off.** Command Post's values live in two places in CSS
(`index.css` and `themes.css`); changing one without the other makes its
swatch disagree with the page.

**Why (simplicity).** One source of truth per theme and no build step. A
comment at both sites names the duplication.

## 4. The old `ui-theme` key is dropped, not migrated

**Call.** The store reads only `legion-appearance`. A browser that had pinned
light or dark through the old toggle starts on System once.

**Alternatives.** Read `ui-theme` once and carry it over as the mode.

**Trade-off.** A returning user who chose the opposite of their OS sees the
mode flip back once and has to click sun/moon again.

**Why.** AGENTS.md: no installed base yet, no compatibility shims. Called out
in the PR as a break.

## 5. Contrast bar applies to new themes only

**Call.** The four alternatives meet ink 7:1, muted and faint 4.5:1, links
4.5:1 and button text 4.5:1. Command Post is left exactly as it was, although
its light faint text measures 4.26:1 on surface and 3.94:1 on canvas.

**Alternatives.** Darken Command Post's light faint token as part of this
change.

**Trade-off.** The default keeps a known contrast shortfall on faint
(secondary, never sole-meaning) text.

**Why.** The request was to add a picker without changing the default look.
The shortfall is recorded in TODO as its own item.

## 6. One theme defines both modes

**Call.** Picking a theme picks its light and dark versions together, and the
mode control chooses between them. Three of the alternatives were dark-only
or light-only in the design lab; their other mode was authored for this
picker.

**Alternatives.** GitHub's model: a separate theme choice for light and for
dark.

**Trade-off.** No mixing, for example Nord at night and Sage by day.

**Why (simplicity).** One grid and one choice. System mode then just works.
