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
 * The contract a picker needs is three things on <html>: `data-palette`, the
 * `dark` class, and `data-tweak-<axis>` for each non-default tweak. Store
 * `{ palette, mode, tweaks }` in localStorage and apply it before first paint.
 * themes.css holds the tweak and palette rules; engine.ts sets the
 * attributes for the lab.
 *
 * The presets below exist so the lab, the gallery and the studio can show
 * every palette and every tweak (on the default palette) in both modes, plus
 * a few combinations. Ids all start `swiss-`, so the Swiss family rules apply.
 */
import type { DesignTheme, Hex, StatusColors, ThemeColors, TweakAxis, Tweaks } from './catalog.ts'
import { monoStack, stack } from './stacks.ts'

export const SWISS_TYPE = {
  sans: stack('Inter Tight Variable'),
  display: stack('Inter Tight Variable'),
  mono: monoStack('Roboto Mono Variable'),
  label: 'Inter Tight · Roboto Mono',
}

type ModeSpec = { colors: ThemeColors; status: StatusColors; signal: Hex }

export type SwissPalette = {
  id: string
  name: string
  description: string
  light: ModeSpec
  dark: ModeSpec
}

const LIGHT: ThemeColors = {
  canvas: '#ffffff', surface: '#ffffff', surface2: '#f2f2f2', line: '#e2e2e2', lineStrong: '#8f8f8f',
  ink: '#0a0a0a', muted: '#474747', faint: '#666666', accent: '#0a0a0a', accentStrong: '#0a0a0a',
}
const DARK: ThemeColors = {
  canvas: '#0b0b0b', surface: '#0b0b0b', surface2: '#1a1a1a', line: '#2b2b2b', lineStrong: '#7a7a7a',
  ink: '#f5f5f5', muted: '#bdbdbd', faint: '#9e9e9e', accent: '#f5f5f5', accentStrong: '#f5f5f5',
}
const LIGHT_STATUS: StatusColors = { ok: '#00a651', warn: '#d68b00', danger: '#e10600', info: '#0062ff' }
const DARK_STATUS: StatusColors = { ok: '#2fd27a', warn: '#ffb020', danger: '#ff4a3d', info: '#5c9dff' }

export const SWISS_PALETTES: SwissPalette[] = [
  {
    id: 'red',
    name: 'Swiss Red',
    description: 'The default: black and white with one red square. Actions are black.',
    light: { colors: LIGHT, status: LIGHT_STATUS, signal: '#e10600' },
    dark: { colors: DARK, status: DARK_STATUS, signal: '#ff2d1f' },
  },
  {
    id: 'blue',
    name: 'Zurich Blue',
    description: 'Cobalt is the signal and the action color, so red only ever means an error.',
    light: { colors: { ...LIGHT, accent: '#0038e0', accentStrong: '#0030c2' }, status: { ...LIGHT_STATUS, info: '#0a8fa8' }, signal: '#0038e0' },
    dark: { colors: { ...DARK, accent: '#7aa2ff', accentStrong: '#9bbaff' }, status: { ...DARK_STATUS, info: '#3cc7d9' }, signal: '#2b59ff' },
  },
  {
    id: 'yellow',
    name: 'Signal Yellow',
    description: 'Yellow marks where you are (the square and the active nav item), always with black on it.',
    light: { colors: LIGHT, status: LIGHT_STATUS, signal: '#ffd400' },
    dark: { colors: DARK, status: DARK_STATUS, signal: '#ffd400' },
  },
  {
    id: 'duotone',
    name: 'Duotone',
    description: 'Red marks structure, blue marks what you can click. Info moves to teal.',
    light: { colors: { ...LIGHT, accent: '#0047bb', accentStrong: '#003da3' }, status: { ...LIGHT_STATUS, info: '#0a8fa8' }, signal: '#e10600' },
    dark: { colors: { ...DARK, accent: '#6f9bff', accentStrong: '#93b4ff' }, status: { ...DARK_STATUS, info: '#3cc7d9' }, signal: '#ff2d1f' },
  },
  {
    id: 'concrete',
    name: 'Concrete',
    description: 'Cool grey-blue neutrals and graphite ink instead of pure black and white.',
    light: {
      colors: {
        canvas: '#eceeef', surface: '#f6f7f8', surface2: '#e3e6e8', line: '#d3d7da', lineStrong: '#8a9096',
        ink: '#1b1f22', muted: '#464c52', faint: '#5c6268', accent: '#1b1f22', accentStrong: '#1b1f22',
      },
      status: { ...LIGHT_STATUS, warn: '#c27f00' },
      signal: '#e3120b',
    },
    dark: {
      colors: {
        canvas: '#16191b', surface: '#1c1f22', surface2: '#262a2e', line: '#33383d', lineStrong: '#737a81',
        ink: '#e8ebed', muted: '#b4bac0', faint: '#959ca3', accent: '#e8ebed', accentStrong: '#e8ebed',
      },
      status: DARK_STATUS,
      signal: '#ff3b30',
    },
  },
  {
    id: 'paper',
    name: 'Paper',
    description: 'Warm off-white and warm charcoal with a softer vermilion, for all-day reading.',
    light: {
      colors: {
        canvas: '#f5f3ee', surface: '#faf9f6', surface2: '#ece9e2', line: '#dedad1', lineStrong: '#96918a',
        ink: '#141311', muted: '#4a4741', faint: '#605c55', accent: '#141311', accentStrong: '#141311',
      },
      status: { ok: '#12924b', warn: '#c28100', danger: '#c81e1e', info: '#2563c9' },
      signal: '#d6361f',
    },
    dark: {
      colors: {
        canvas: '#151412', surface: '#1a1917', surface2: '#24221f', line: '#33302b', lineStrong: '#7d776f',
        ink: '#f1ede6', muted: '#c0b9ae', faint: '#a19a8f', accent: '#f1ede6', accentStrong: '#f1ede6',
      },
      status: { ok: '#3fcf7f', warn: '#f0b13a', danger: '#ff5a48', info: '#6aa3f0' },
      signal: '#ff5a3c',
    },
  },
  {
    id: 'mono',
    name: 'Mono',
    description: 'No signal color at all: the square is ink. Status keeps its hues, so it is the only color on screen.',
    light: { colors: LIGHT, status: LIGHT_STATUS, signal: '#0a0a0a' },
    dark: { colors: DARK, status: DARK_STATUS, signal: '#f5f5f5' },
  },
]

export type SwissTweakOption = { value: string; label: string; description: string }
export type SwissTweak = { axis: TweakAxis; label: string; options: SwissTweakOption[] }

// The first option of each axis is the default (no attribute set).
export const SWISS_TWEAKS: SwissTweak[] = [
  {
    axis: 'type',
    label: 'Typeface',
    options: [
      { value: 'default', label: 'Inter Tight', description: 'Neutral neo-grotesk; the default.' },
      { value: 'akzidenz', label: 'Akzidenz', description: 'Archivo, an Akzidenz-style grotesk; titles condense to 87% at weight 800 like a printed poster.' },
      { value: 'geist', label: 'Geist', description: 'A contemporary grotesk designed as a pair with Geist Mono.' },
      { value: 'mono', label: 'Mono', description: 'Geist with lowercase titles and labels, and monospaced tables, so columns of IDs line up.' },
    ],
  },
  {
    axis: 'titles',
    label: 'Titles',
    options: [
      { value: 'default', label: 'Bold', description: 'Oversized and bold; the default.' },
      { value: 'hairline', label: 'Hairline', description: 'Larger but weight 300, with 1px section rules and a quiet active nav item.' },
    ],
  },
  {
    axis: 'density',
    label: 'Density',
    options: [
      { value: 'default', label: 'Comfortable', description: 'The default spacing.' },
      { value: 'compact', label: 'Compact', description: '14px base, tighter rows and smaller titles: about a quarter more rows per screen.' },
    ],
  },
  {
    axis: 'cards',
    label: 'Cards',
    options: [
      { value: 'default', label: 'Rules', description: 'Sections separated by a heavy top rule, no boxes; the default.' },
      { value: 'boxed', label: 'Boxed', description: 'A timetable: every section boxed in ink, table heads inverted, columns ruled.' },
      { value: 'soft', label: 'Soft', description: 'Rounded boxes with a light shadow, pill badges and a red dot instead of a square.' },
    ],
  },
  {
    axis: 'header',
    label: 'Page header',
    options: [
      { value: 'default', label: 'Plain', description: 'Title on the page; the default.' },
      { value: 'masthead', label: 'Masthead', description: 'Each page opens on an inverse ink band carrying its title, status and actions.' },
    ],
  },
  {
    axis: 'rail',
    label: 'Nav rail',
    options: [
      { value: 'default', label: 'Plain', description: 'The rail matches the page; the default.' },
      { value: 'black', label: 'Black', description: 'A solid black rail with the active item in the signal color.' },
    ],
  },
]

type Meta = { name: string; tagline: string; audience: string; rationale: string }

const pair = (id: string, meta: Meta, palette: SwissPalette, tweaks: Tweaks = {}): DesignTheme[] => {
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

const paletteById = (id: string) => SWISS_PALETTES.find((palette) => palette.id === id)!
const RED = paletteById('red')

// Every palette except red (that is the `swiss` / `swiss-dark` pair in
// catalog.ts and riffs.ts) on default tweaks.
const PALETTE_PRESETS = SWISS_PALETTES.filter((palette) => palette.id !== 'red').flatMap((palette) =>
  pair(`swiss-${palette.id}`, {
    name: palette.name,
    tagline: palette.description,
    audience: 'A color choice for the default Swiss layout.',
    rationale: `Palette only: ${palette.description} Every tweak works with it in both modes.`,
  }, palette),
)

// Every non-default tweak option, alone, on the default red palette.
const TWEAK_PRESETS = SWISS_TWEAKS.flatMap((tweak) =>
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
