/*
 * Swiss as a system for a theme picker: palettes × tweaks.
 *
 * - A **palette** is colors only, designed as a light/dark pair. Red is the
 *   default; the others swap the signal color or the neutrals.
 * - A **tweak** is one structural axis (typeface, titles, density, cards,
 *   page header, nav rail) with a default and one or more alternatives.
 *   Tweaks are independent of each other and of the palette, and work in
 *   both modes.
 *
 * The palettes and tweaks themselves now ship: they live in
 * src/appearance/palettes.ts and the app's appearance picker offers them.
 *
 * The presets below exist so the lab, the gallery and the studio can show
 * every palette and every tweak (on the default palette) in both modes, plus
 * a few combinations. Ids all start `swiss-`, so the Swiss family rules apply.
 */
import type { DesignTheme, Tweaks } from './catalog.ts'
import { PALETTES, TWEAKS, type Palette } from '../appearance/palettes.ts'
import { monoStack, stack } from './stacks.ts'

export const SWISS_TYPE = {
  sans: stack('Inter Tight Variable'),
  display: stack('Inter Tight Variable'),
  mono: monoStack('Roboto Mono Variable'),
  label: 'Inter Tight · Roboto Mono',
}

export type { Palette as SwissPalette, Tweak as SwissTweak, TweakOption as SwissTweakOption } from '../appearance/palettes.ts'
export { PALETTES as SWISS_PALETTES, TWEAKS as SWISS_TWEAKS }

type Meta = { name: string; tagline: string; audience: string; rationale: string }

const pair = (id: string, meta: Meta, palette: Palette, tweaks: Tweaks = {}): DesignTheme[] => {
  const shared = {
    family: 'swiss' as const,
    role: 'variation' as const,
    type: SWISS_TYPE,
    radius: 'none' as const,
    palette: palette.id,
    tweaks,
    tagline: meta.tagline,
    audience: meta.audience,
    rationale: meta.rationale,
  }
  const darkId = `${id}-dark`
  return [
    {
      ...shared, ...palette.light, id, pair: darkId, name: meta.name, mode: 'light',
      shadow: { card: 'none', pop: '0 0 0 1px #0a0a0a, 0 16px 32px -16px rgb(0 0 0 / 0.35)' },
    },
    {
      ...shared, ...palette.dark, id: darkId, pair: id, name: `${meta.name} Dark`, mode: 'dark',
      shadow: { card: 'none', pop: '0 0 0 1px #f5f5f5, 0 16px 32px -16px rgb(0 0 0 / 0.8)' },
    },
  ]
}

const paletteById = (id: string) => PALETTES.find((palette) => palette.id === id)!
const RED = paletteById('red')

// Every palette except red (that is the `swiss` / `swiss-dark` pair in
// catalog.ts and riffs.ts) on default tweaks.
const PALETTE_PRESETS = PALETTES.filter((palette) => palette.id !== 'red').flatMap((palette) =>
  pair(`swiss-${palette.id}`, {
    name: palette.name,
    tagline: palette.description,
    audience: 'A color choice for the default Swiss layout.',
    rationale: `Palette only: ${palette.description} Every tweak works with it in both modes.`,
  }, palette),
)

// Every non-default tweak option, alone, on the default red palette.
const TWEAK_PRESETS = TWEAKS.flatMap((tweak) =>
  tweak.options.slice(1).map((option) =>
    pair(`swiss-tweak-${tweak.axis}-${option.value}`, {
      name: `${tweak.label}: ${option.label}`,
      tagline: option.description,
      audience: 'A tweak that combines with any palette and any other tweak.',
      rationale: `Shown on Swiss Red with every other tweak at its default. ${option.description}`,
    }, RED, { [tweak.axis]: option.value }),
  ).flat(),
)

const COMBO_PRESETS = [
  ...pair('swiss-combo-ops', {
    name: 'Combo: Ops Desk',
    tagline: 'Concrete + Compact + Boxed cards + Black rail.',
    audience: 'Operators of big fleets: dense, ruled and framed.',
    rationale: 'An example of tweaks stacking: every choice here is independent, and each can be undone on its own.',
  }, paletteById('concrete'), { density: 'compact', cards: 'boxed', rail: 'black' }),
  ...pair('swiss-combo-gallery', {
    name: 'Combo: Gallery',
    tagline: 'Paper + Akzidenz + Hairline titles + Masthead.',
    audience: 'Readers more than operators: editorial and calm.',
    rationale: 'An example of tweaks stacking: every choice here is independent, and each can be undone on its own.',
  }, paletteById('paper'), { type: 'akzidenz', titles: 'hairline', header: 'masthead' }),
  ...pair('swiss-combo-product', {
    name: 'Combo: Product',
    tagline: 'Zurich Blue + Geist + Soft cards.',
    audience: 'A contemporary SaaS feel on the Swiss bones.',
    rationale: 'An example of tweaks stacking: every choice here is independent, and each can be undone on its own.',
  }, paletteById('blue'), { type: 'geist', cards: 'soft' }),
]

export const SWISS_PALETTE_PRESETS = PALETTE_PRESETS
export const SWISS_TWEAK_PRESETS = TWEAK_PRESETS
export const SWISS_COMBO_PRESETS = COMBO_PRESETS
export const SWISS_PRESETS: DesignTheme[] = [...PALETTE_PRESETS, ...TWEAK_PRESETS, ...COMBO_PRESETS]
