/*
 * Round two: the three finalists (Command Post, Forge, Swiss), each with its
 * opposite mode and five riffs. A riff keeps its family's structure and
 * changes one or two things on purpose — palette temperature, accent, type,
 * density, or one structural move — so a reaction to it says something
 * specific about the family.
 *
 * Ids are `<family>-<riff>`; themes.css matches family rules with
 * `[data-design|='<family>']`, so every riff inherits its family's structure.
 * Command Post's counterpart is the shipped dark theme (catalog.ts).
 */
import type { DesignTheme } from './catalog.ts'
import { monoStack, stack } from './stacks.ts'

const COMMAND_POST_TYPE = {
  sans: stack('Inter Variable'),
  display: stack('Space Grotesk Variable'),
  mono: monoStack('Fira Code Variable'),
  label: 'Space Grotesk · Inter · Fira Code',
}

const FORGE_TYPE = {
  sans: stack('Archivo Variable'),
  display: stack('Archivo Variable'),
  mono: monoStack('Chivo Mono Variable'),
  label: 'Archivo (+ condensed) · Chivo Mono',
}

const SWISS_TYPE = {
  sans: stack('Inter Tight Variable'),
  display: stack('Inter Tight Variable'),
  mono: monoStack('Roboto Mono Variable'),
  label: 'Inter Tight · Roboto Mono',
}

const SWISS_COLORS = {
  canvas: '#ffffff', surface: '#ffffff', surface2: '#f2f2f2', line: '#e2e2e2', lineStrong: '#8f8f8f',
  ink: '#0a0a0a', muted: '#474747', faint: '#666666', accent: '#0a0a0a', accentStrong: '#0a0a0a',
} as const

const SWISS_STATUS = { ok: '#00a651', warn: '#d68b00', danger: '#e10600', info: '#0062ff' } as const

export const RIFF_THEMES: DesignTheme[] = [
  // -------------------------------------------------------------------------
  // Command Post — base and counterpart are the shipped light/dark themes.
  // -------------------------------------------------------------------------
  {
    id: 'command-post-vexillum',
    family: 'command-post',
    role: 'riff',
    name: 'Vexillum',
    mode: 'light',
    tagline: 'Command Post under the legion’s red banner, trimmed in gold.',
    audience: 'If the brand should carry more weight: red is the standard’s own color, so the UI reads as Legion at a glance.',
    rationale:
      'Swaps brass for vexillum red as the action color and demotes gold to structure (title rule, nav marker, card tops). Titles go uppercase and tracked, extending the inscription eyebrows to the page name.',
    type: COMMAND_POST_TYPE,
    radius: 'default',
    colors: {
      canvas: '#f6f4ef', surface: '#fffefb', surface2: '#f2eee6', line: '#e3ddd0', lineStrong: '#c9c0ad',
      ink: '#1f1a16', muted: '#5e564b', faint: '#6e665a', accent: '#9b2a1c', accentStrong: '#7f2015',
    },
    status: { ok: '#059669', warn: '#d97706', danger: '#dc2626', info: '#0284c7' },
    signal: '#b8902a',
    title: { weight: 700, transform: 'uppercase', tracking: 0.06 },
  },
  {
    id: 'command-post-marble',
    family: 'command-post',
    role: 'riff',
    name: 'Marble',
    mode: 'light',
    tagline: 'Command Post in daylight: cooler stone, softer corners, more air.',
    audience: 'Teams who like Command Post’s calm but find the warm cream a little dated on a bright screen.',
    rationale:
      'Moves the neutrals from warm stone to cool marble so brass is the only warm thing on screen. Softer corners and layered shadows read as more polished; titles step up a size.',
    type: COMMAND_POST_TYPE,
    radius: 'soft',
    shadow: {
      card: '0 1px 2px rgb(20 26 34 / 0.04), 0 4px 14px -4px rgb(20 26 34 / 0.07)',
      pop: '0 24px 48px -12px rgb(20 26 34 / 0.25)',
    },
    colors: {
      canvas: '#f3f4f6', surface: '#ffffff', surface2: '#eff1f4', line: '#e0e3e8', lineStrong: '#c3c9d1',
      ink: '#15181c', muted: '#4e5660', faint: '#636b75', accent: '#8d6b12', accentStrong: '#6a500d',
    },
    title: { size: 1.85, weight: 600 },
  },
  {
    id: 'command-post-brass-rail',
    family: 'command-post',
    role: 'riff',
    name: 'Brass Rail',
    mode: 'light',
    tagline: 'Light Command Post with a dark bronze navigation rail.',
    audience: 'People who want the light theme for content but a strong frame: the rail separates “where am I” from the page.',
    rationale:
      'Keeps every light-mode token (with faint text darkened to pass AA) and moves the shipped dark palette onto the sidebar only, so the two modes meet in one screen.',
    type: COMMAND_POST_TYPE,
    radius: 'default',
    colors: {
      canvas: '#f7f6f3', surface: '#ffffff', surface2: '#f4f2ed', line: '#e4e1d8', lineStrong: '#cbc6b9',
      ink: '#1e1c17', muted: '#645f54', faint: '#6f695d', accent: '#8f6a15', accentStrong: '#6e5110',
    },
    sidebar: {
      surface: '#1c1913', surface2: '#2a251c', line: '#332d22', lineStrong: '#4a4131',
      ink: '#efe9dc', muted: '#c3b9a5', faint: '#a59a85', accent: '#d6b45a', accentStrong: '#e7c97a',
    },
  },
  {
    id: 'command-post-field-desk',
    family: 'command-post',
    role: 'riff',
    name: 'Field Desk',
    mode: 'light',
    tagline: 'Compact Command Post: more rows per screen, square edges, mono labels.',
    audience: 'Operators of big clusters who live in the tables and want density over breathing room.',
    rationale:
      'A 14.5px base, tighter rows and sharp corners fit roughly a fifth more on screen. IBM Plex Sans stays legible small; labels move to Plex Mono so they read as data fields.',
    type: {
      sans: stack('IBM Plex Sans Variable'),
      display: stack('Space Grotesk Variable'),
      mono: monoStack('IBM Plex Mono'),
      label: 'Space Grotesk · IBM Plex Sans · IBM Plex Mono',
    },
    radius: 'sharp',
    baseSize: 14.5,
    colors: {
      canvas: '#f2f1ed', surface: '#ffffff', surface2: '#f1efe9', line: '#e1ddd3', lineStrong: '#c6bfb0',
      ink: '#1c1a15', muted: '#5a554b', faint: '#6c665a', accent: '#8a6513', accentStrong: '#6b4e0f',
    },
  },
  {
    id: 'command-post-lamplight',
    family: 'command-post',
    role: 'riff',
    name: 'Lamplight',
    mode: 'dark',
    tagline: 'A softer Command Post dark: warm brown instead of near-black.',
    audience: 'Night-shift readers who find the shipped dark too stark; this one feels like a lit tent, not a void.',
    rationale:
      'Raises the dark surfaces into warm browns and lightens the brass toward amber, which lowers the contrast swing between page and text without dropping below AA.',
    type: COMMAND_POST_TYPE,
    radius: 'default',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.35)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.6)' },
    colors: {
      canvas: '#1a1612', surface: '#221d18', surface2: '#2b251e', line: '#3a3229', lineStrong: '#554a3c',
      ink: '#f0e8da', muted: '#c2b49f', faint: '#a39580', accent: '#e0b45c', accentStrong: '#f0cd85',
    },
    status: { ok: '#6fcf8a', warn: '#f0a742', danger: '#ef6b5b', info: '#6cb3e6' },
  },

  // -------------------------------------------------------------------------
  // Forge — base is round one's Forge (catalog.ts).
  // -------------------------------------------------------------------------
  {
    id: 'forge-light',
    family: 'forge',
    role: 'counterpart',
    name: 'Forge Light',
    mode: 'light',
    tagline: 'Forge in daylight: steel-grey paper, burnt orange, the same stamped titles.',
    audience: 'Forge’s audience on a bright floor or a laptop by a window.',
    rationale:
      'Orange darkens to burnt orange so white button text and orange links both pass AA on light surfaces. Warn stays yellow-gold so it never reads as the accent; the card header band and the orange rule carry over unchanged.',
    type: FORGE_TYPE,
    radius: 'sharp',
    shadow: { card: '0 1px 2px rgb(30 20 10 / 0.06)', pop: '0 20px 40px -12px rgb(30 20 10 / 0.3)' },
    colors: {
      canvas: '#efede9', surface: '#ffffff', surface2: '#ebe7e1', line: '#dcd6cd', lineStrong: '#b9b0a3',
      ink: '#17140f', muted: '#4f4840', faint: '#655d53', accent: '#c2410c', accentStrong: '#a3360a',
    },
    status: { ok: '#1f9a52', warn: '#b98d00', danger: '#d02b2b', info: '#1d6fc4' },
  },
  {
    id: 'forge-foundry',
    family: 'forge',
    role: 'riff',
    name: 'Foundry',
    mode: 'dark',
    tagline: 'Blue steel and fire: Forge on cold grey-blue instead of charcoal.',
    audience: 'Teams who like Forge’s weight but want the dark to feel cooler and more like a terminal.',
    rationale:
      'Only the neutrals change: a blue-grey steel ramp makes the orange read hotter by contrast, and info moves to a periwinkle so it never blends into the background.',
    type: FORGE_TYPE,
    radius: 'sharp',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.75)' },
    colors: {
      canvas: '#0c0f13', surface: '#131820', surface2: '#1b222c', line: '#263040', lineStrong: '#3a4759',
      ink: '#e9eef5', muted: '#a6b1c2', faint: '#8693a6', accent: '#ff7a1a', accentStrong: '#ffa766',
    },
    status: { ok: '#4ade80', warn: '#facc15', danger: '#f4505a', info: '#8fa8ff' },
  },
  {
    id: 'forge-crucible',
    family: 'forge',
    role: 'riff',
    name: 'Crucible',
    mode: 'dark',
    tagline: 'Forge running hot: deeper black, a hotter orange, and a glow.',
    audience: 'Demo screens and dashboards on a wall, where a little drama helps the state read from across the room.',
    rationale:
      'The glow is confined to chrome (card edges, the title, the header rule) and never sits behind data. Accent shifts toward red-orange; warn stays yellow so the two never meet.',
    type: FORGE_TYPE,
    radius: 'sharp',
    shadow: {
      card: '0 0 0 1px rgb(255 90 31 / 0.07), 0 12px 32px -20px rgb(255 90 31 / 0.5)',
      pop: '0 0 0 1px rgb(255 90 31 / 0.3), 0 24px 60px -20px rgb(255 90 31 / 0.45)',
    },
    colors: {
      canvas: '#0a0807', surface: '#130f0d', surface2: '#1d1714', line: '#2d231e', lineStrong: '#4b3a31',
      ink: '#fbefe6', muted: '#c4ad9d', faint: '#a38c7c', accent: '#ff5a1f', accentStrong: '#ff9466',
    },
    status: { ok: '#5be38f', warn: '#ffd23f', danger: '#ff4d4d', info: '#4cc3ff' },
  },
  {
    id: 'forge-oxide',
    family: 'forge',
    role: 'riff',
    name: 'Oxide',
    mode: 'dark',
    tagline: 'Weathered iron: brown-black neutrals and a rust accent.',
    audience: 'People who like Forge’s structure but find bright orange tiring; rust is warmer and quieter.',
    rationale:
      'Desaturates the accent to rust and tints every neutral brown, so the whole screen sits in one material. IBM Plex Mono replaces Chivo Mono for slightly wider, calmer IDs.',
    type: { ...FORGE_TYPE, mono: monoStack('IBM Plex Mono'), label: 'Archivo (+ condensed) · IBM Plex Mono' },
    radius: 'sharp',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.75)' },
    colors: {
      canvas: '#120d0b', surface: '#1b1411', surface2: '#251b17', line: '#36271f', lineStrong: '#533d31',
      ink: '#f4e9e2', muted: '#c6ab9c', faint: '#a68b7c', accent: '#e0663a', accentStrong: '#f2956f',
    },
    status: { ok: '#8bcf6a', warn: '#f2c14e', danger: '#ff5d5d', info: '#73b7e8' },
  },
  {
    id: 'forge-anvil',
    family: 'forge',
    role: 'riff',
    name: 'Anvil',
    mode: 'dark',
    tagline: 'Forge stripped to neutral graphite, flat and square, orange only where it counts.',
    audience: 'Teams who want Forge’s type and structure without its warmth — closer to a classic ops console.',
    rationale:
      'No tint in the greys, no shadows, no rounding. Hubot Sans has its own width axis, so titles condense like Archivo’s but with a more technical, engineered feel.',
    type: {
      sans: stack('Hubot Sans Variable'),
      display: stack('Hubot Sans Variable'),
      mono: monoStack('JetBrains Mono Variable'),
      label: 'Hubot Sans (+ condensed) · JetBrains Mono',
    },
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #4a4a4a, 0 24px 48px -16px rgb(0 0 0 / 0.8)' },
    colors: {
      canvas: '#111111', surface: '#1a1a1a', surface2: '#232323', line: '#303030', lineStrong: '#4a4a4a',
      ink: '#f2f2f2', muted: '#b3b3b3', faint: '#969696', accent: '#ff7a1a', accentStrong: '#ffa766',
    },
    status: { ok: '#4ade80', warn: '#facc15', danger: '#f43f3f', info: '#38bdf8' },
  },
  {
    id: 'forge-shipyard',
    family: 'forge',
    role: 'riff',
    name: 'Shipyard',
    mode: 'dark',
    tagline: 'Industrial signage: navy-black, safety amber, Big Shoulders titles.',
    audience: 'Fleets of physical machines — the UI as the sign over the loading dock.',
    rationale:
      'Big Shoulders is drawn from Chicago’s industrial signage and is condensed by design, so titles get taller rather than wider. Amber replaces orange; warn moves to orange to stay distinct.',
    type: {
      sans: stack('IBM Plex Sans Variable'),
      display: stack('Big Shoulders Display Variable'),
      mono: monoStack('IBM Plex Mono'),
      label: 'Big Shoulders Display · IBM Plex Sans · IBM Plex Mono',
    },
    radius: 'sharp',
    shadow: { card: '0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.75)' },
    colors: {
      canvas: '#0d1014', surface: '#141920', surface2: '#1c232c', line: '#29323e', lineStrong: '#3e4a5a',
      ink: '#eef1f5', muted: '#a9b3c0', faint: '#8a95a4', accent: '#ffb000', accentStrong: '#ffc94d',
    },
    status: { ok: '#4ade80', warn: '#ff8a3d', danger: '#ff4d5e', info: '#5cb8ff' },
    title: { size: 2.6, weight: 800 },
  },

  // -------------------------------------------------------------------------
  // Swiss — base is round one's Swiss (catalog.ts).
  // -------------------------------------------------------------------------
  {
    id: 'swiss-dark',
    family: 'swiss',
    role: 'counterpart',
    name: 'Swiss Dark',
    mode: 'dark',
    tagline: 'Swiss inverted: black field, white type, the red square kept.',
    audience: 'Swiss’s audience after dark; the structure is identical, so switching modes changes nothing but the light.',
    rationale:
      'Ink and canvas swap; the rules, the inverse nav state and the oversized titles follow automatically because Swiss’s structure is drawn from tokens. The red brightens slightly to hold its weight on black.',
    type: SWISS_TYPE,
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #f5f5f5, 0 16px 32px -16px rgb(0 0 0 / 0.8)' },
    colors: {
      canvas: '#0b0b0b', surface: '#0b0b0b', surface2: '#1a1a1a', line: '#2b2b2b', lineStrong: '#7a7a7a',
      ink: '#f5f5f5', muted: '#bdbdbd', faint: '#9e9e9e', accent: '#f5f5f5', accentStrong: '#f5f5f5',
    },
    status: { ok: '#2fd27a', warn: '#ffb020', danger: '#ff4a3d', info: '#5c9dff' },
    signal: '#ff2d1f',
  },
  {
    id: 'swiss-neue-grafik',
    family: 'swiss',
    role: 'riff',
    name: 'Neue Grafik',
    mode: 'light',
    tagline: 'The grid made visible: column lines, red rules, even bigger type.',
    audience: 'Fans of the original who want the system to show — the page as a designed sheet.',
    rationale:
      'Faint column lines under the content and red top rules on cards expose the grid that Swiss only implied. Section titles stay black so red remains structural, never informational.',
    type: SWISS_TYPE,
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #0a0a0a, 0 16px 32px -16px rgb(0 0 0 / 0.35)' },
    colors: SWISS_COLORS,
    status: SWISS_STATUS,
    signal: '#e10600',
    title: { size: 3.4 },
  },
  {
    id: 'swiss-zurich',
    family: 'swiss',
    role: 'riff',
    name: 'Zurich Blue',
    mode: 'light',
    tagline: 'Swiss with cobalt in place of red: calmer, and red is left to errors.',
    audience: 'Teams who love Swiss but do not want the brand color to be the same red as a failure.',
    rationale:
      'Cobalt becomes both the signal and the action color (white on it is 7:1), so red appears only when something is wrong. Info moves to teal to stay distinct from the accent.',
    type: SWISS_TYPE,
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #0a0a0a, 0 16px 32px -16px rgb(0 0 0 / 0.35)' },
    colors: { ...SWISS_COLORS, accent: '#0038e0', accentStrong: '#0030c2' },
    status: { ok: '#00a651', warn: '#d68b00', danger: '#e10600', info: '#0a8fa8' },
    signal: '#0038e0',
  },
  {
    id: 'swiss-poster',
    family: 'swiss',
    role: 'riff',
    name: 'Poster',
    mode: 'light',
    tagline: 'Swiss at full volume: a red rail and 64px titles.',
    audience: 'A brand-forward default for demos and docs screenshots, where the product should be recognizable from a thumbnail.',
    rationale:
      'The red moves from a square to the whole navigation rail, deepened so white text on it clears 7:1, and page titles double in size. The content area is unchanged Swiss, so tables stay quiet.',
    type: SWISS_TYPE,
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #0a0a0a, 0 16px 32px -16px rgb(0 0 0 / 0.35)' },
    colors: SWISS_COLORS,
    status: SWISS_STATUS,
    signal: '#b30500',
    sidebar: {
      surface: '#b30500', surface2: '#960400', line: '#c8332b', lineStrong: '#ff8a83',
      ink: '#ffffff', muted: '#ffffff', faint: '#ffeaea', accent: '#ffffff', accentStrong: '#ffffff',
    },
    title: { size: 4, tracking: -0.05 },
  },
  {
    id: 'swiss-lausanne',
    family: 'swiss',
    role: 'riff',
    name: 'Lausanne',
    mode: 'light',
    tagline: 'Swiss on warm paper with a softer vermilion, for all-day reading.',
    audience: 'People who like Swiss’s clarity but find pure white and pure red harsh over a long session.',
    rationale:
      'Off-white paper and warm greys cut glare; the signal softens to vermilion. Everything structural is unchanged, so it is the same system at a lower volume.',
    type: SWISS_TYPE,
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #141311, 0 16px 32px -16px rgb(0 0 0 / 0.3)' },
    colors: {
      canvas: '#f5f3ee', surface: '#faf9f6', surface2: '#ece9e2', line: '#dedad1', lineStrong: '#96918a',
      ink: '#141311', muted: '#4a4741', faint: '#605c55', accent: '#141311', accentStrong: '#141311',
    },
    status: { ok: '#12924b', warn: '#c28100', danger: '#c81e1e', info: '#2563c9' },
    signal: '#d6361f',
  },
  {
    id: 'swiss-basel',
    family: 'swiss',
    role: 'riff',
    name: 'Basel Mono',
    mode: 'light',
    tagline: 'Technical Swiss: lowercase titles, monospaced tables.',
    audience: 'Engineers who read the UI like a terminal — every column of IDs and counts lines up.',
    rationale:
      'Geist and Geist Mono are designed as a pair, so mono tables sit cleanly under sans headings. Lowercase titles are a Swiss tradition and read less shouty at this size.',
    type: {
      sans: stack('Geist Variable'),
      display: stack('Geist Variable'),
      mono: monoStack('Geist Mono Variable'),
      label: 'Geist · Geist Mono',
    },
    radius: 'none',
    shadow: { card: 'none', pop: '0 0 0 1px #0a0a0a, 0 16px 32px -16px rgb(0 0 0 / 0.35)' },
    colors: SWISS_COLORS,
    status: SWISS_STATUS,
    signal: '#e10600',
    title: { weight: 600, transform: 'lowercase', tracking: -0.045 },
  },
]
