/*
 * Design-lab theme catalog: twenty candidate looks for the Legion UI.
 *
 * Each theme is data — colors, type, shape, depth — applied at runtime by
 * `lab.ts` as CSS variables, plus optional structural flourishes in
 * `themes.css` keyed on `html[data-design="<id>"]`. Nothing here changes a
 * component; the UX stays exactly as it is.
 *
 * Shared constraints every theme was held to:
 * - Body text (ink) >= 7:1 and secondary text (muted) >= 4.5:1 on surfaces;
 *   faint text >= 4.5:1 on surfaces where it carries meaning.
 * - Links (accentStrong) >= 4.5:1 on surfaces.
 * - Status keeps its four hues (ok / warn / danger / info) and its text
 *   label, so state never rides on color alone.
 * - Mono stays a true monospace with ligatures off for IDs and logs.
 * `npm run lab:contrast` (scripts/lab-contrast.mjs) checks the numbers.
 */

import { RIFF_THEMES } from './riffs.ts'
import { monoStack, stack } from './stacks.ts'

export type Hex = `#${string}`

export type ThemeColors = {
  canvas: Hex
  surface: Hex
  surface2: Hex
  line: Hex
  lineStrong: Hex
  ink: Hex
  muted: Hex
  faint: Hex
  accent: Hex
  accentStrong: Hex
}

export type StatusColors = { ok: Hex; warn: Hex; danger: Hex; info: Hex }

export type RadiusScale = 'none' | 'sharp' | 'default' | 'soft' | 'round'

export type DesignTheme = {
  id: string
  name: string
  mode: 'light' | 'dark'
  tagline: string
  audience: string
  rationale: string
  type: { sans: string; display: string; mono: string; label: string }
  radius: RadiusScale
  shadow?: { card: string; pop: string }
  colors: ThemeColors
  status?: StatusColors
  // Scoped overrides for the sidebar, for themes with an inverse nav rail.
  sidebar?: Partial<ThemeColors>
  // Fine-tuning the theme studio exposes; unset means "leave the theme's own".
  baseSize?: number // root font size, px
  title?: { size?: number; weight?: number; transform?: 'none' | 'uppercase' | 'lowercase'; tracking?: number } // rem, 100-900, em
  cardBorder?: number // px
  // Whose structural CSS (themes.css) to use; defaults to the theme's own id.
  // Riffs are named `<family>-<riff>`, and family rules in themes.css match
  // `[data-design|='<family>']`, so a riff inherits its family's structure.
  flourishes?: string
  // A structural accent separate from the interactive one (Swiss's red
  // square, Vexillum's banner red), as `--c-signal`. Never used for text.
  signal?: Hex
  // Round-two grouping: which family a theme belongs to and its role in it.
  family?: 'command-post' | 'forge' | 'swiss'
  role?: 'base' | 'counterpart' | 'riff'
}


export const RADIUS_SCALES: Record<RadiusScale, Record<string, string>> = {
  none: { sm: '0', r: '0', md: '0', lg: '0', xl: '0', '2xl': '0', full: '0' },
  sharp: { sm: '1px', r: '2px', md: '2px', lg: '3px', xl: '4px', '2xl': '6px', full: '9999px' },
  default: { sm: '0.125rem', r: '0.25rem', md: '0.375rem', lg: '0.5rem', xl: '0.75rem', '2xl': '1rem', full: '9999px' },
  soft: { sm: '3px', r: '5px', md: '8px', lg: '12px', xl: '16px', '2xl': '20px', full: '9999px' },
  round: { sm: '4px', r: '6px', md: '10px', lg: '16px', xl: '20px', '2xl': '24px', full: '9999px' },
}

export const DESIGN_THEMES: DesignTheme[] = [
  // -------------------------------------------------------------------------
  // Dark
  // -------------------------------------------------------------------------
  {
    id: 'obsidian',
    name: 'Obsidian',
    mode: 'dark',
    tagline: 'Quiet near-black with a single violet signal.',
    audience: 'Engineers who live in Linear, Vercel and Raycast and expect a tool to feel that finished.',
    rationale:
      'Neutral greys keep the status hues the loudest thing on screen; one cool accent marks selection and action. Geist is tuned for UI at small sizes and Geist Mono pairs with it metrically.',
    type: { sans: stack('Geist Variable'), display: stack('Geist Variable'), mono: monoStack('Geist Mono Variable'), label: 'Geist · Geist Mono' },
    radius: 'soft',
    shadow: { card: '0 0 0 1px rgb(255 255 255 / 0.015), 0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.7)' },
    colors: {
      canvas: '#09090b', surface: '#111113', surface2: '#19191c', line: '#232327', lineStrong: '#36363c',
      ink: '#ededef', muted: '#a1a1aa', faint: '#8a8a93', accent: '#8b7cf6', accentStrong: '#b0a5ff',
    },
  },
  {
    id: 'phosphor',
    name: 'Phosphor',
    mode: 'dark',
    tagline: 'A green-screen terminal, with the legibility of a modern one.',
    audience: 'Terminal-native SREs who would rather be in tmux — the UI should feel like the CLI they already trust.',
    rationale:
      'Monospace everywhere makes columns of IDs, ports and counts line up for free. The glow and scanlines are kept faint enough that body text still clears AA; amber and red stay distinct from the green field.',
    type: { sans: monoStack('IBM Plex Mono'), display: stack('VT323', 'monospace'), mono: monoStack('IBM Plex Mono'), label: 'IBM Plex Mono · VT323' },
    radius: 'none',
    shadow: { card: '0 0 0 1px rgb(57 255 136 / 0.04), 0 0 28px -14px rgb(57 255 136 / 0.35)', pop: '0 0 0 1px rgb(57 255 136 / 0.3), 0 0 40px -10px rgb(57 255 136 / 0.35)' },
    colors: {
      canvas: '#030805', surface: '#06100a', surface2: '#0b1b11', line: '#14301f', lineStrong: '#21503a',
      ink: '#c2f8d2', muted: '#7fd49a', faint: '#5fae7a', accent: '#39ff88', accentStrong: '#8dffbd',
    },
    status: { ok: '#39ff88', warn: '#ffc53d', danger: '#ff5f56', info: '#5ce1ff' },
  },
  {
    id: 'mission-control',
    name: 'Mission Control',
    mode: 'dark',
    tagline: 'Deep navy NOC console built to be read from across the room.',
    audience: 'On-call teams and NOC wall displays — glanceable state, dense tables, long shifts in dim rooms.',
    rationale:
      'Condensed uppercase headings buy horizontal room for wide tables; navy reads calmer than black over a long shift. Cyan is the accent, so info status moves to a distinct periwinkle.',
    type: { sans: stack('Barlow'), display: stack('Barlow Condensed'), mono: monoStack('JetBrains Mono Variable'), label: 'Barlow · Barlow Condensed · JetBrains Mono' },
    radius: 'sharp',
    shadow: { card: '0 1px 0 rgb(255 255 255 / 0.03) inset, 0 1px 2px rgb(0 0 0 / 0.4)', pop: '0 20px 40px -16px rgb(0 0 0 / 0.7)' },
    colors: {
      canvas: '#070b14', surface: '#0c1322', surface2: '#121c30', line: '#1c2a43', lineStrong: '#2c3e60',
      ink: '#e4eaf6', muted: '#9aaac6', faint: '#7a8cab', accent: '#22d3ee', accentStrong: '#7ae8f8',
    },
    status: { ok: '#34d399', warn: '#fbbf24', danger: '#f43f5e', info: '#818cf8' },
  },
  {
    id: 'neon-dusk',
    name: 'Neon Dusk',
    mode: 'dark',
    tagline: 'Synthwave purple with a hot-magenta pulse.',
    audience: 'Small teams and homelabs that want their control plane to have a personality.',
    rationale:
      'Saturated color lives in the chrome (titles, nav, focus) and never in the data, so the tables stay as readable as any other theme. Outfit is geometric and friendly; Spline Sans Mono keeps IDs crisp.',
    type: { sans: stack('Outfit Variable'), display: stack('Outfit Variable'), mono: monoStack('Spline Sans Mono Variable'), label: 'Outfit · Spline Sans Mono' },
    radius: 'round',
    shadow: { card: '0 0 0 1px rgb(255 79 216 / 0.04), 0 8px 24px -12px rgb(0 0 0 / 0.6)', pop: '0 0 0 1px rgb(255 79 216 / 0.25), 0 24px 60px -20px rgb(255 79 216 / 0.35)' },
    colors: {
      canvas: '#0f0a1a', surface: '#170f26', surface2: '#201634', line: '#2e2148', lineStrong: '#463262',
      ink: '#f4edff', muted: '#bcaddb', faint: '#9e8cc0', accent: '#ff4fd8', accentStrong: '#ff92e8',
    },
    status: { ok: '#3df2b0', warn: '#ffc857', danger: '#ff5470', info: '#4dd7ff' },
  },
  {
    id: 'forge',
    family: 'forge',
    role: 'base',
    name: 'Forge',
    mode: 'dark',
    tagline: 'Charcoal and molten orange — heavy machinery, condensed type.',
    audience: 'Infra teams running bare metal and big batch fleets; an industrial tool for industrial work.',
    rationale:
      'Archivo’s width axis gives compressed, stamped-steel headings without a second family. Orange is reserved for action; warn shifts to yellow so it is never mistaken for the accent.',
    type: { sans: stack('Archivo Variable'), display: stack('Archivo Variable'), mono: monoStack('Chivo Mono Variable'), label: 'Archivo (+ condensed) · Chivo Mono' },
    radius: 'sharp',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.75)' },
    colors: {
      canvas: '#0e0d0c', surface: '#171513', surface2: '#211e1b', line: '#2f2a26', lineStrong: '#4a413a',
      ink: '#f3ede7', muted: '#b8ab9f', faint: '#988a7e', accent: '#ff7a1a', accentStrong: '#ffa766',
    },
    status: { ok: '#4ade80', warn: '#facc15', danger: '#f43f3f', info: '#38bdf8' },
  },
  {
    id: 'nord',
    name: 'Nord',
    mode: 'dark',
    tagline: 'Arctic slate and frost — low contrast fatigue, high clarity.',
    audience: 'People who keep the UI open all day next to an editor themed the same way.',
    rationale:
      'The Nord palette is familiar from editors and terminals, so the UI disappears into the desktop. Muted aurora status colors keep alerts visible without shouting; Manrope’s open shapes stay legible on mid-grey.',
    type: { sans: stack('Manrope Variable'), display: stack('Manrope Variable'), mono: monoStack('JetBrains Mono Variable'), label: 'Manrope · JetBrains Mono' },
    radius: 'soft',
    shadow: { card: '0 1px 3px rgb(0 0 0 / 0.18)', pop: '0 20px 40px -16px rgb(0 0 0 / 0.5)' },
    colors: {
      canvas: '#242933', surface: '#2e3440', surface2: '#363d4b', line: '#3f4757', lineStrong: '#4f5a6e',
      ink: '#eceff4', muted: '#c5cddb', faint: '#a6b0c2', accent: '#88c0d0', accentStrong: '#a8d8e5',
    },
    status: { ok: '#a3be8c', warn: '#ebcb8b', danger: '#d0707a', info: '#81a1c1' },
  },
  {
    id: 'carbon',
    name: 'Carbon',
    mode: 'dark',
    tagline: 'Enterprise-grade: flat, square, systematic.',
    audience: 'Platform teams in large orgs — familiar to anyone who has used IBM Cloud or a bank’s internal tools.',
    rationale:
      'Plex Sans at a light weight for page titles and semibold for everything that must be read fast. Zero radius and no shadows make the grid the structure. Blue is the one interactive color.',
    type: { sans: stack('IBM Plex Sans Variable'), display: stack('IBM Plex Sans Variable'), mono: monoStack('IBM Plex Mono'), label: 'IBM Plex Sans · IBM Plex Mono' },
    radius: 'none',
    shadow: { card: 'none', pop: '0 8px 24px rgb(0 0 0 / 0.5)' },
    colors: {
      canvas: '#161616', surface: '#1f1f1f', surface2: '#2a2a2a', line: '#393939', lineStrong: '#555555',
      ink: '#f4f4f4', muted: '#c6c6c6', faint: '#a8a8a8', accent: '#78a9ff', accentStrong: '#a6c8ff',
    },
    status: { ok: '#42be65', warn: '#f1c21b', danger: '#fa4d56', info: '#4589ff' },
  },
  {
    id: 'imperium',
    name: 'Imperium',
    mode: 'dark',
    tagline: 'Tyrian purple and gold — the legion standard, literally.',
    audience: 'A brand-forward default: it makes Legion look like Legion in screenshots, docs and demos.',
    rationale:
      'Cinzel is used only for page and section titles, where its inscriptional capitals carry the brand; everything that is read at speed is Hanken Grotesk. Parchment ink on wine-black keeps contrast high without pure white.',
    type: { sans: stack('Hanken Grotesk Variable'), display: stack('Cinzel Variable', 'serif'), mono: monoStack('Red Hat Mono Variable'), label: 'Cinzel · Hanken Grotesk · Red Hat Mono' },
    radius: 'sharp',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 0 0 1px rgb(212 175 55 / 0.25), 0 24px 48px -16px rgb(0 0 0 / 0.8)' },
    colors: {
      canvas: '#120a10', surface: '#1b1018', surface2: '#251722', line: '#3a2534', lineStrong: '#58384b',
      ink: '#f5ebdd', muted: '#ccb8a4', faint: '#a99481', accent: '#d4af37', accentStrong: '#ecd07a',
    },
    status: { ok: '#7bc47f', warn: '#e8a33d', danger: '#e45d66', info: '#86acdc' },
    sidebar: { surface: '#0d070b', surface2: '#1b1018', line: '#2e1d29' },
  },
  {
    id: 'graphite',
    name: 'Graphite AAA',
    mode: 'dark',
    tagline: 'Maximum-contrast monochrome with a signal-yellow accent.',
    audience: 'Low-vision operators, glare-heavy rooms, and anyone who wants WCAG AAA rather than AA.',
    rationale:
      'Atkinson Hyperlegible was designed for low-vision readers: unambiguous l/1/I and 0/O matter in IDs. Larger base size, white ink on black, and a yellow accent that no status color competes with (warn moves to orange).',
    type: {
      sans: stack('Atkinson Hyperlegible Next Variable'),
      display: stack('Atkinson Hyperlegible Next Variable'),
      mono: monoStack('Atkinson Hyperlegible Mono Variable'),
      label: 'Atkinson Hyperlegible Next · Mono',
    },
    radius: 'default',
    shadow: { card: 'none', pop: '0 0 0 2px #ffffff' },
    colors: {
      canvas: '#000000', surface: '#0b0b0b', surface2: '#1c1c1c', line: '#4a4a4a', lineStrong: '#8a8a8a',
      ink: '#ffffff', muted: '#dedede', faint: '#c2c2c2', accent: '#ffd60a', accentStrong: '#ffe45c',
    },
    status: { ok: '#3ddc84', warn: '#ff9f1c', danger: '#ff6b6b', info: '#7cc4ff' },
  },
  {
    id: 'verdigris',
    name: 'Verdigris',
    mode: 'dark',
    tagline: 'Patinated bronze — teal-green field with copper details.',
    audience: 'Teams that want a dark theme that is not another grey-and-blue clone, without novelty.',
    rationale:
      'A tinted dark (green-slate) is easier on the eye than neutral black and gives the brand a color without an accent shouting. Bricolage Grotesque gives titles character; Schibsted Grotesk stays sober for data.',
    type: { sans: stack('Schibsted Grotesk Variable'), display: stack('Bricolage Grotesque Variable'), mono: monoStack('Fragment Mono'), label: 'Bricolage Grotesque · Schibsted Grotesk · Fragment Mono' },
    radius: 'soft',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.4)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.7)' },
    colors: {
      canvas: '#0b1413', surface: '#101d1b', surface2: '#172824', line: '#213732', lineStrong: '#31504a',
      ink: '#e7f2ee', muted: '#a6c4bc', faint: '#87a79e', accent: '#4fd1b0', accentStrong: '#8ae5ce',
    },
    status: { ok: '#9bd65a', warn: '#f2b84b', danger: '#f26d6d', info: '#6cb4f5' },
  },
  {
    id: 'blueprint',
    name: 'Blueprint',
    mode: 'dark',
    tagline: 'Engineering drawing: cyanotype blue, white linework, red-pencil accent.',
    audience: 'Architects and platform designers — the cluster as a technical drawing you can inspect.',
    rationale:
      'The grid background and dashed rules make structure visible without heavier boxes. Martian Mono labels read like drawing annotations; Plex Sans Condensed keeps body text compact.',
    type: { sans: stack('IBM Plex Sans Condensed'), display: monoStack('Martian Mono Variable'), mono: monoStack('Martian Mono Variable'), label: 'IBM Plex Sans Condensed · Martian Mono' },
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px rgb(255 255 255 / 0.6), 0 24px 48px -16px rgb(0 0 0 / 0.6)' },
    colors: {
      canvas: '#0b3a6b', surface: '#0d4277', surface2: '#124c87', line: '#3a6fa3', lineStrong: '#6b97c5',
      ink: '#f3f8ff', muted: '#cddff3', faint: '#aac6e6', accent: '#ffe066', accentStrong: '#fff0a8',
    },
    status: { ok: '#7cf0b4', warn: '#ffc46b', danger: '#ff9a9a', info: '#b5e2ff' },
  },

  // -------------------------------------------------------------------------
  // Light
  // -------------------------------------------------------------------------
  {
    id: 'field-manual',
    name: 'Field Manual',
    mode: 'light',
    tagline: 'A printed operations manual: paper, ink, and a signal-red rule.',
    audience: 'Readers more than clickers — people who use the UI to understand a system, often alongside the docs.',
    rationale:
      'A serif display for titles gives hierarchy through typeface instead of size; Libre Franklin is a sturdy news sans for the data. Warm paper reduces glare versus pure white. Red marks action and nothing else.',
    type: { sans: stack('Libre Franklin Variable'), display: stack('Newsreader Variable', 'Georgia, serif'), mono: monoStack('IBM Plex Mono'), label: 'Newsreader · Libre Franklin · IBM Plex Mono' },
    radius: 'sharp',
    shadow: { card: 'none', pop: '0 16px 40px -16px rgb(40 30 10 / 0.35)' },
    colors: {
      canvas: '#f3efe4', surface: '#fbf9f3', surface2: '#efe9da', line: '#ddd5c2', lineStrong: '#bfb399',
      ink: '#1d1b16', muted: '#55503f', faint: '#6c6553', accent: '#b3361e', accentStrong: '#8e2814',
    },
    status: { ok: '#2e8b57', warn: '#c98a12', danger: '#c0392b', info: '#2d6cb5' },
  },
  {
    id: 'swiss',
    family: 'swiss',
    role: 'base',
    name: 'Swiss',
    mode: 'light',
    tagline: 'International Typographic Style: black, white, one red square.',
    audience: 'Design-literate teams who want maximum clarity and zero decoration.',
    rationale:
      'Hierarchy comes from scale and weight alone: very large titles, bold black section heads, hairline rules. Black is the action color, so red can be a pure accent that never collides with danger state in buttons.',
    type: { sans: stack('Inter Tight Variable'), display: stack('Inter Tight Variable'), mono: monoStack('Roboto Mono Variable'), label: 'Inter Tight · Roboto Mono' },
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #0a0a0a, 0 16px 32px -16px rgb(0 0 0 / 0.35)' },
    colors: {
      canvas: '#ffffff', surface: '#ffffff', surface2: '#f2f2f2', line: '#e2e2e2', lineStrong: '#8f8f8f',
      ink: '#0a0a0a', muted: '#474747', faint: '#666666', accent: '#0a0a0a', accentStrong: '#0a0a0a',
    },
    status: { ok: '#00a651', warn: '#d68b00', danger: '#e10600', info: '#0062ff' },
    signal: '#e10600',
  },
  {
    id: 'brutalist',
    name: 'Neo-Brutalist',
    mode: 'light',
    tagline: 'Thick black outlines, hard shadows, highlighter yellow.',
    audience: 'Startups and dev-tool fans who want a UI with attitude that is still easy to scan.',
    rationale:
      'Every container has the same 2px outline and offset shadow, so the structure is unmistakable. Yellow highlights “you are here”; electric blue is the action color and passes AA with white text.',
    type: { sans: stack('Space Grotesk Variable'), display: stack('Space Grotesk Variable'), mono: monoStack('Space Mono'), label: 'Space Grotesk · Space Mono' },
    radius: 'none',
    shadow: { card: '4px 4px 0 0 #111111', pop: '6px 6px 0 0 #111111' },
    colors: {
      canvas: '#fff8e7', surface: '#ffffff', surface2: '#fff1c9', line: '#111111', lineStrong: '#111111',
      ink: '#111111', muted: '#383838', faint: '#555555', accent: '#3d5afe', accentStrong: '#2236c9',
    },
    status: { ok: '#00a862', warn: '#cc8f00', danger: '#ff3b30', info: '#3d9bff' },
  },
  {
    id: 'porcelain',
    name: 'Porcelain',
    mode: 'light',
    tagline: 'Polished SaaS: cool white, soft depth, indigo.',
    audience: 'The broadest audience — new users, managers, and evaluators who judge a tool in the first minute.',
    rationale:
      'Layered soft shadows and generous radius read as approachable; indigo is the most widely understood “interactive” color. Plus Jakarta Sans is friendly but has tabular numerals for the counts.',
    type: { sans: stack('Plus Jakarta Sans Variable'), display: stack('Plus Jakarta Sans Variable'), mono: monoStack('Source Code Pro Variable'), label: 'Plus Jakarta Sans · Source Code Pro' },
    radius: 'round',
    shadow: {
      card: '0 1px 2px rgb(16 24 40 / 0.04), 0 4px 12px -2px rgb(16 24 40 / 0.06)',
      pop: '0 24px 48px -12px rgb(16 24 40 / 0.25)',
    },
    colors: {
      canvas: '#f5f6fa', surface: '#ffffff', surface2: '#f1f3f9', line: '#e4e7f0', lineStrong: '#cbd1e0',
      ink: '#0f1222', muted: '#4a5068', faint: '#666c84', accent: '#4f46e5', accentStrong: '#3730a3',
    },
  },
  {
    id: 'terracotta',
    name: 'Terracotta',
    mode: 'light',
    tagline: 'Sun-baked clay and plaster — warm, rounded, human.',
    audience: 'Teams who want an infrastructure tool that feels welcoming rather than clinical.',
    rationale:
      'Warm neutrals lower the harshness of a white UI; a soft-serif display (Fraunces) adds warmth to titles while DM Sans keeps tables neutral. Olive “ok” sits naturally in the palette.',
    type: { sans: stack('DM Sans Variable'), display: stack('Fraunces Variable', 'Georgia, serif'), mono: monoStack('DM Mono'), label: 'Fraunces · DM Sans · DM Mono' },
    radius: 'round',
    shadow: { card: '0 1px 2px rgb(80 40 10 / 0.05), 0 6px 16px -8px rgb(80 40 10 / 0.12)', pop: '0 24px 48px -12px rgb(80 40 10 / 0.3)' },
    colors: {
      canvas: '#f6f0e8', surface: '#fffcf8', surface2: '#f2e9dd', line: '#e7dbcb', lineStrong: '#cdb9a0',
      ink: '#2a1d15', muted: '#654f40', faint: '#7f6a5a', accent: '#c0532d', accentStrong: '#9a3f1f',
    },
    status: { ok: '#5e8c3a', warn: '#c98a12', danger: '#c62f2f', info: '#3f7cab' },
  },
  {
    id: 'sage',
    name: 'Sage',
    mode: 'light',
    tagline: 'Calm greens with a deep-forest nav rail.',
    audience: 'Ops teams who want a light theme with a strong frame — the dark rail anchors navigation.',
    rationale:
      'An inverse sidebar separates “where am I” from “what am I looking at” at a glance. Low-saturation sage keeps the content area restful so status hues stand out.',
    type: { sans: stack('Figtree Variable'), display: stack('Figtree Variable'), mono: monoStack('Red Hat Mono Variable'), label: 'Figtree · Red Hat Mono' },
    radius: 'soft',
    shadow: { card: '0 1px 2px rgb(20 40 25 / 0.05)', pop: '0 24px 48px -12px rgb(20 40 25 / 0.25)' },
    colors: {
      canvas: '#f1f4ee', surface: '#ffffff', surface2: '#eaefe6', line: '#dbe2d4', lineStrong: '#bfc9b6',
      ink: '#16201a', muted: '#4a574d', faint: '#627064', accent: '#3a7a54', accentStrong: '#2a5a3d',
    },
    status: { ok: '#2f9e5b', warn: '#d0901a', danger: '#d2493f', info: '#3b82c4' },
    sidebar: {
      surface: '#15271d', surface2: '#1f382a', line: '#264434', lineStrong: '#36604a',
      ink: '#e8f1ea', muted: '#b1c7b8', faint: '#93ad9c', accent: '#8fd3a8', accentStrong: '#b5e6c6',
    },
  },
  {
    id: 'solar',
    name: 'Solar',
    mode: 'light',
    tagline: 'Solarized Light, the developer classic, with Recursive type.',
    audience: 'Developers whose editor and terminal are already Solarized; zero adjustment cost.',
    rationale:
      'Solarized’s base tones were chosen for consistent lightness contrast; the UI reuses its accents for status so “ok/warn/danger/info” match what users see in their terminal. Recursive gives sans and mono from one family.',
    type: { sans: stack('Recursive Variable'), display: stack('Recursive Variable'), mono: monoStack('Recursive Variable'), label: 'Recursive Sans · Recursive Mono' },
    radius: 'default',
    shadow: { card: '0 1px 2px rgb(7 54 66 / 0.05)', pop: '0 20px 40px -12px rgb(7 54 66 / 0.25)' },
    colors: {
      canvas: '#fdf6e3', surface: '#fffbf0', surface2: '#f3ecd8', line: '#e6dec6', lineStrong: '#cfc5a6',
      ink: '#073642', muted: '#4b6168', faint: '#5d737a', accent: '#268bd2', accentStrong: '#1a659c',
    },
    status: { ok: '#859900', warn: '#b58900', danger: '#dc322f', info: '#2aa198' },
  },
  {
    id: 'quiet',
    name: 'Quiet',
    mode: 'light',
    tagline: 'Almost nothing: white space, hairlines, a serif headline.',
    audience: 'People who find dashboards noisy — writers of runbooks, staff engineers, managers skimming state.',
    rationale:
      'No accent color at all: interaction is ink, so the only color on screen is status. Cards lose their boxes and keep a hairline; an Instrument Serif headline gives the page a calm, editorial anchor.',
    type: { sans: stack('Instrument Sans Variable'), display: stack('Instrument Serif', 'Georgia, serif'), mono: monoStack('Geist Mono Variable'), label: 'Instrument Serif · Instrument Sans · Geist Mono' },
    radius: 'soft',
    shadow: { card: 'none', pop: '0 0 0 1px rgb(0 0 0 / 0.06), 0 24px 48px -16px rgb(0 0 0 / 0.2)' },
    colors: {
      canvas: '#ffffff', surface: '#ffffff', surface2: '#f6f6f4', line: '#ebebe8', lineStrong: '#d2d2ce',
      ink: '#1f1f1e', muted: '#5c5c59', faint: '#71716d', accent: '#1f1f1e', accentStrong: '#1f1f1e',
    },
    status: { ok: '#3e9b6c', warn: '#c98b1a', danger: '#d0453b', info: '#3a7bc8' },
  },
  {
    id: 'hi-vis',
    name: 'Hi-Vis',
    mode: 'light',
    tagline: 'Safety orange on concrete, with a caution-striped header.',
    audience: 'Field and edge operators — people managing fleets of physical boxes, often on a laptop in a bright room.',
    rationale:
      'High-visibility orange is used as a fill with black text (7:1), never as text on white. A black nav rail and condensed uppercase titles give an equipment-panel feel that holds up in glare.',
    type: { sans: stack('Public Sans Variable'), display: stack('Saira Variable'), mono: monoStack('Azeret Mono Variable'), label: 'Saira (condensed) · Public Sans · Azeret Mono' },
    radius: 'none',
    shadow: { card: '0 1px 0 rgb(0 0 0 / 0.08)', pop: '0 0 0 2px #121212, 0 20px 40px -12px rgb(0 0 0 / 0.35)' },
    colors: {
      canvas: '#ebebe6', surface: '#ffffff', surface2: '#f3f3ee', line: '#d4d4cc', lineStrong: '#a9a99f',
      ink: '#121212', muted: '#42423c', faint: '#5a5a53', accent: '#ff5a00', accentStrong: '#b03b00',
    },
    status: { ok: '#1e9e4a', warn: '#c99700', danger: '#d62828', info: '#1f6fd1' },
    sidebar: {
      surface: '#121212', surface2: '#232323', line: '#2c2c2c', lineStrong: '#444444',
      ink: '#f5f5f0', muted: '#c4c4bc', faint: '#a3a39b', accent: '#ff6a14', accentStrong: '#ff8c4a',
    },
  },
]

// The shipped look (index.css), as editable presets. Not one of the twenty.
const COMMAND_POST_TYPE = {
  sans: stack('Inter Variable'),
  display: stack('Space Grotesk Variable'),
  mono: monoStack('Fira Code Variable'),
  label: 'Space Grotesk · Inter · Fira Code',
}

export const BASELINE_THEMES: DesignTheme[] = [
  {
    id: 'command-post',
    family: 'command-post',
    role: 'base',
    name: 'Command Post (shipped, light)',
    mode: 'light',
    tagline: 'The current look: warm stone neutrals with a brass accent.',
    audience: 'Reference point — what Legion ships today.',
    rationale: 'Included so every candidate can be compared against today’s UI, and as a starting point for edits.',
    type: COMMAND_POST_TYPE,
    radius: 'default',
    colors: {
      canvas: '#f7f6f3', surface: '#ffffff', surface2: '#f4f2ed', line: '#e4e1d8', lineStrong: '#cbc6b9',
      ink: '#1e1c17', muted: '#645f54', faint: '#807a6e', accent: '#8f6a15', accentStrong: '#6e5110',
    },
  },
  {
    id: 'command-post-dark',
    family: 'command-post',
    role: 'counterpart',
    name: 'Command Post (shipped, dark)',
    mode: 'dark',
    tagline: 'The current dark look: near-black stone with a gold accent.',
    audience: 'Reference point — what Legion ships today.',
    rationale: 'Included so every candidate can be compared against today’s UI, and as a starting point for edits.',
    type: COMMAND_POST_TYPE,
    radius: 'default',
    colors: {
      canvas: '#0c0b09', surface: '#171511', surface2: '#1f1c16', line: '#2d2920', lineStrong: '#443d2e',
      ink: '#ebe6db', muted: '#a8a090', faint: '#8c8472', accent: '#d6b45a', accentStrong: '#e7c97a',
    },
  },
]

export const ALL_PRESETS = [...BASELINE_THEMES, ...DESIGN_THEMES, ...RIFF_THEMES]

export const findTheme = (id: string | null | undefined) => ALL_PRESETS.find((theme) => theme.id === id)
