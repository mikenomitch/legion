// Typefaces the theme studio offers. Every family here is loaded by fonts.ts
// (or index.css for the shipped three); `stack` is the exact value the
// catalog uses, so a preset's fonts show up selected.
export type FontOption = { label: string; stack: string; group: 'Sans' | 'Serif & display' | 'Mono' }

const sans = (family: string) => `'${family}', system-ui, -apple-system, sans-serif`
const serif = (family: string) => `'${family}', Georgia, serif`
const mono = (family: string) => `'${family}', ui-monospace, SFMono-Regular, Menlo, monospace`

export const FONT_OPTIONS: FontOption[] = [
  { label: 'Inter (shipped)', stack: sans('Inter Variable'), group: 'Sans' },
  { label: 'Geist', stack: sans('Geist Variable'), group: 'Sans' },
  { label: 'IBM Plex Sans', stack: sans('IBM Plex Sans Variable'), group: 'Sans' },
  { label: 'IBM Plex Sans Condensed', stack: sans('IBM Plex Sans Condensed'), group: 'Sans' },
  { label: 'Barlow', stack: sans('Barlow'), group: 'Sans' },
  { label: 'Outfit', stack: sans('Outfit Variable'), group: 'Sans' },
  { label: 'Archivo', stack: sans('Archivo Variable'), group: 'Sans' },
  { label: 'Manrope', stack: sans('Manrope Variable'), group: 'Sans' },
  { label: 'Hanken Grotesk', stack: sans('Hanken Grotesk Variable'), group: 'Sans' },
  { label: 'Atkinson Hyperlegible Next', stack: sans('Atkinson Hyperlegible Next Variable'), group: 'Sans' },
  { label: 'Schibsted Grotesk', stack: sans('Schibsted Grotesk Variable'), group: 'Sans' },
  { label: 'Libre Franklin', stack: sans('Libre Franklin Variable'), group: 'Sans' },
  { label: 'Inter Tight', stack: sans('Inter Tight Variable'), group: 'Sans' },
  { label: 'Space Grotesk (shipped display)', stack: sans('Space Grotesk Variable'), group: 'Sans' },
  { label: 'Plus Jakarta Sans', stack: sans('Plus Jakarta Sans Variable'), group: 'Sans' },
  { label: 'DM Sans', stack: sans('DM Sans Variable'), group: 'Sans' },
  { label: 'Figtree', stack: sans('Figtree Variable'), group: 'Sans' },
  { label: 'Recursive', stack: sans('Recursive Variable'), group: 'Sans' },
  { label: 'Instrument Sans', stack: sans('Instrument Sans Variable'), group: 'Sans' },
  { label: 'Public Sans', stack: sans('Public Sans Variable'), group: 'Sans' },
  { label: 'Saira', stack: sans('Saira Variable'), group: 'Sans' },
  { label: 'Hubot Sans', stack: sans('Hubot Sans Variable'), group: 'Sans' },
  { label: 'System UI', stack: 'system-ui, -apple-system, sans-serif', group: 'Sans' },
  { label: 'Barlow Condensed', stack: sans('Barlow Condensed'), group: 'Serif & display' },
  { label: 'Bricolage Grotesque', stack: sans('Bricolage Grotesque Variable'), group: 'Serif & display' },
  { label: 'Big Shoulders Display', stack: sans('Big Shoulders Display Variable'), group: 'Serif & display' },
  { label: 'Cinzel', stack: serif('Cinzel Variable'), group: 'Serif & display' },
  { label: 'Newsreader', stack: serif('Newsreader Variable'), group: 'Serif & display' },
  { label: 'Fraunces', stack: serif('Fraunces Variable'), group: 'Serif & display' },
  { label: 'Instrument Serif', stack: serif('Instrument Serif'), group: 'Serif & display' },
  { label: 'VT323 (pixel)', stack: `'VT323', monospace`, group: 'Serif & display' },
  { label: 'Fira Code (shipped)', stack: mono('Fira Code Variable'), group: 'Mono' },
  { label: 'Geist Mono', stack: mono('Geist Mono Variable'), group: 'Mono' },
  { label: 'IBM Plex Mono', stack: mono('IBM Plex Mono'), group: 'Mono' },
  { label: 'JetBrains Mono', stack: mono('JetBrains Mono Variable'), group: 'Mono' },
  { label: 'Spline Sans Mono', stack: mono('Spline Sans Mono Variable'), group: 'Mono' },
  { label: 'Chivo Mono', stack: mono('Chivo Mono Variable'), group: 'Mono' },
  { label: 'Red Hat Mono', stack: mono('Red Hat Mono Variable'), group: 'Mono' },
  { label: 'Atkinson Hyperlegible Mono', stack: mono('Atkinson Hyperlegible Mono Variable'), group: 'Mono' },
  { label: 'Fragment Mono', stack: mono('Fragment Mono'), group: 'Mono' },
  { label: 'Roboto Mono', stack: mono('Roboto Mono Variable'), group: 'Mono' },
  { label: 'Martian Mono', stack: mono('Martian Mono Variable'), group: 'Mono' },
  { label: 'Space Mono', stack: mono('Space Mono'), group: 'Mono' },
  { label: 'Source Code Pro', stack: mono('Source Code Pro Variable'), group: 'Mono' },
  { label: 'DM Mono', stack: mono('DM Mono'), group: 'Mono' },
  { label: 'Azeret Mono', stack: mono('Azeret Mono Variable'), group: 'Mono' },
  { label: 'Recursive Mono', stack: mono('Recursive Variable'), group: 'Mono' },
]

const family = (stack: string) => stack.split(',')[0].trim().replace(/^['"]|['"]$/g, '')

// The option a stored stack corresponds to (matched on its first family).
export const fontOptionFor = (stack: string) => FONT_OPTIONS.find((option) => family(option.stack) === family(stack))
