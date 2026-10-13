/*
 * The built-in color themes the appearance picker offers. Each id has a
 * light and a dark block in themes.css; the first entry is the default and
 * is the look index.css gives :root, so it needs no attribute at all.
 */

export type Theme = {
  id: string
  name: string
  description: string
}

export const THEMES: readonly Theme[] = [
  { id: 'command-post', name: 'Command Post', description: 'Warm stone neutrals with a brass accent. The default.' },
  { id: 'forge', name: 'Forge', description: 'Steel grey and burnt orange; charcoal and molten orange in the dark.' },
  { id: 'nord', name: 'Nord', description: 'Snow-storm greys and frost blue, after the Nord editor palette.' },
  { id: 'sage', name: 'Sage', description: 'Calm greens with a forest accent; dark moss at night.' },
  { id: 'porcelain', name: 'Porcelain', description: 'Cool white and indigo; deep ink blue at night.' },
  {
    id: 'tomorrow-night-bright',
    name: 'Tomorrow Night Bright',
    description: 'Pure black and bright blue, after the Tomorrow Night Bright editor theme; Tomorrow in light mode.',
  },
]

export const DEFAULT_THEME = THEMES[0].id

/*
 * The color tokens a theme sets, in the order the picker lists them. Each is
 * an `--c-<id>` RGB triplet (index.css) that Tailwind maps to a utility name.
 */
export const COLOR_TOKENS = [
  { id: 'canvas', label: 'Page background' },
  { id: 'surface', label: 'Cards and panels' },
  { id: 'surface2', label: 'Raised and hover' },
  { id: 'line', label: 'Borders' },
  { id: 'line-strong', label: 'Strong borders' },
  { id: 'ink', label: 'Text' },
  { id: 'muted', label: 'Secondary text' },
  { id: 'faint', label: 'Faint text' },
  { id: 'brass', label: 'Accent and buttons' },
  { id: 'brass-strong', label: 'Links' },
] as const

export type ColorToken = (typeof COLOR_TOKENS)[number]['id']
