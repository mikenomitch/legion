/*
 * The viewer's appearance choice, saved per browser in localStorage and
 * applied to <html>: a color theme, a mode, and any colors the viewer set by
 * hand, kept per theme and per mode.
 *
 *   localStorage['legion-appearance'] = {
 *     "theme": "nord", "mode": "dark",
 *     "colors": { "nord": { "dark": { "brass": "#ffcc00" } } }
 *   }
 *   <html class="dark" data-theme="nord" style="--c-brass: 255 204 0">
 *
 * Defaults are never written: no data-theme means Command Post, mode
 * "system" follows the OS, and a color nobody set comes from the theme.
 * index.html runs the same logic inline before first paint (keep the two in
 * step); this module owns it once the app is running.
 */
import { createSignal } from 'solid-js'
import { COLOR_TOKENS, DEFAULT_THEME, THEMES, type ColorToken } from './themes'

export const APPEARANCE_KEY = 'legion-appearance'

export type AppearanceMode = 'light' | 'dark' | 'system'

/** Hand-set colors as `#rrggbb`, by theme id, then mode, then token. */
export type ColorOverrides = Record<string, Partial<Record<'light' | 'dark', Partial<Record<ColorToken, string>>>>>

export type Appearance = {
  theme: string
  mode: AppearanceMode
  colors: ColorOverrides
}

export const DEFAULT_APPEARANCE: Appearance = { theme: DEFAULT_THEME, mode: 'system', colors: {} }

const HEX = /^#[0-9a-f]{6}$/i

// Drops anything this build does not know (a removed theme or token, a
// malformed color), so a stale or hand-edited value degrades to the defaults.
const sanitizeColors = (value: unknown): ColorOverrides => {
  const colors: ColorOverrides = {}
  if (!value || typeof value !== 'object') return colors
  for (const theme of THEMES) {
    const byMode = (value as Record<string, unknown>)[theme.id]
    if (!byMode || typeof byMode !== 'object') continue
    for (const mode of ['light', 'dark'] as const) {
      const byToken = (byMode as Record<string, unknown>)[mode]
      if (!byToken || typeof byToken !== 'object') continue
      for (const token of COLOR_TOKENS) {
        const hex = (byToken as Record<string, unknown>)[token.id]
        if (typeof hex !== 'string' || !HEX.test(hex)) continue
        ;((colors[theme.id] ??= {})[mode] ??= {})[token.id] = hex.toLowerCase()
      }
    }
  }
  return colors
}

export const sanitizeAppearance = (value: unknown): Appearance => {
  if (!value || typeof value !== 'object') return DEFAULT_APPEARANCE
  const raw = value as Record<string, unknown>
  const theme = THEMES.some((entry) => entry.id === raw.theme) ? (raw.theme as string) : DEFAULT_THEME
  const mode: AppearanceMode = raw.mode === 'light' || raw.mode === 'dark' ? raw.mode : 'system'
  return { theme, mode, colors: sanitizeColors(raw.colors) }
}

export const isDefault = (value: Appearance) =>
  value.theme === DEFAULT_THEME && value.mode === 'system' && Object.keys(value.colors).length === 0

const readStored = (): Appearance => {
  try {
    const raw = localStorage.getItem(APPEARANCE_KEY)
    return raw ? sanitizeAppearance(JSON.parse(raw)) : DEFAULT_APPEARANCE
  } catch {
    return DEFAULT_APPEARANCE
  }
}

const writeStored = (value: Appearance) => {
  try {
    if (isDefault(value)) localStorage.removeItem(APPEARANCE_KEY)
    else localStorage.setItem(APPEARANCE_KEY, JSON.stringify(value))
  } catch {
    // Storage unavailable (private window, blocked site data): the choice
    // still applies for this page view.
  }
}

const prefersDark = () => typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches

const [systemDark, setSystemDark] = createSignal(prefersDark())
const [appearance, setAppearanceSignal] = createSignal<Appearance>(readStored())
// Bumped after every write to <html>, so readers of computed colors (the
// picker's inputs) re-read once the new values are actually in place.
const [appliedVersion, setAppliedVersion] = createSignal(0)

export { appearance, appliedVersion }

/** The mode actually on screen, with "system" resolved against the OS. */
export const resolvedMode = (): 'light' | 'dark' => {
  const mode = appearance().mode
  return mode === 'system' ? (systemDark() ? 'dark' : 'light') : mode
}

/** The hand-set colors for the theme and mode on screen. */
export const activeOverrides = (): Partial<Record<ColorToken, string>> =>
  appearance().colors[appearance().theme]?.[resolvedMode()] ?? {}

export const hexToTriplet = (hex: string) =>
  [1, 3, 5].map((start) => parseInt(hex.slice(start, start + 2), 16)).join(' ')

const applyToDocument = () => {
  const root = document.documentElement
  const value = appearance()
  if (value.theme === DEFAULT_THEME) delete root.dataset.theme
  else root.dataset.theme = value.theme
  root.classList.toggle('dark', resolvedMode() === 'dark')
  const overrides = activeOverrides()
  for (const token of COLOR_TOKENS) {
    const hex = overrides[token.id]
    if (hex) root.style.setProperty(`--c-${token.id}`, hexToTriplet(hex))
    else root.style.removeProperty(`--c-${token.id}`)
  }
  setAppliedVersion((version) => version + 1)
}

/** Replaces the whole choice, saves it and applies it. */
export const setAppearance = (next: Appearance) => {
  const value = sanitizeAppearance(next)
  setAppearanceSignal(value)
  writeStored(value)
  applyToDocument()
}

export const setTheme = (theme: string) => setAppearance({ ...appearance(), theme })
export const setMode = (mode: AppearanceMode) => setAppearance({ ...appearance(), mode })
export const resetAppearance = () => setAppearance(DEFAULT_APPEARANCE)

/**
 * Sets one color for the theme and mode on screen, or clears it back to the
 * theme's own value when `hex` is null.
 */
export const setColor = (token: ColorToken, hex: string | null) => {
  const { theme, colors } = appearance()
  const mode = resolvedMode()
  const forMode = { ...colors[theme]?.[mode] }
  if (hex) forMode[token] = hex
  else delete forMode[token]
  const forTheme = { ...colors[theme], [mode]: forMode }
  if (Object.keys(forMode).length === 0) delete forTheme[mode]
  const next = { ...colors, [theme]: forTheme }
  if (Object.keys(forTheme).length === 0) delete next[theme]
  setAppearance({ ...appearance(), colors: next })
}

/** Clears every hand-set color for the theme and mode on screen. */
export const resetColors = () => {
  const { theme, colors } = appearance()
  const forTheme = { ...colors[theme] }
  delete forTheme[resolvedMode()]
  const next = { ...colors, [theme]: forTheme }
  if (Object.keys(forTheme).length === 0) delete next[theme]
  setAppearance({ ...appearance(), colors: next })
}

/** Flips between light and dark (the header's sun/moon), leaving "system". */
export const toggleMode = () => setMode(resolvedMode() === 'dark' ? 'light' : 'dark')

if (typeof window !== 'undefined') {
  // Re-apply on load in case the inline boot script could not run, then
  // follow the OS while the mode is "system".
  applyToDocument()
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (event) => {
    setSystemDark(event.matches)
    if (appearance().mode === 'system') applyToDocument()
  })
  // Another tab changed the choice: follow it.
  window.addEventListener('storage', (event) => {
    if (event.key !== APPEARANCE_KEY) return
    setAppearanceSignal(readStored())
    applyToDocument()
  })
}
