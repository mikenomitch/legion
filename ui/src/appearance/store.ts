/*
 * The viewer's appearance choice, a color theme and a mode, saved per browser
 * in localStorage and applied as attributes on <html>.
 *
 *   localStorage['legion-appearance'] = {"theme":"nord","mode":"dark"}
 *   <html class="dark" data-theme="nord">
 *
 * Defaults are never written: no data-theme means Command Post, and mode
 * "system" follows the OS. index.html runs the same logic inline before first
 * paint (keep the two in step); this module owns it once the app is running.
 */
import { createSignal } from 'solid-js'
import { DEFAULT_THEME, THEMES } from './themes'

export const APPEARANCE_KEY = 'legion-appearance'

export type AppearanceMode = 'light' | 'dark' | 'system'

export type Appearance = {
  theme: string
  mode: AppearanceMode
}

export const DEFAULT_APPEARANCE: Appearance = { theme: DEFAULT_THEME, mode: 'system' }

// Drops anything this build does not know (a removed theme id), so a stale or
// hand-edited value degrades to the defaults.
export const sanitizeAppearance = (value: unknown): Appearance => {
  if (!value || typeof value !== 'object') return DEFAULT_APPEARANCE
  const raw = value as Record<string, unknown>
  const theme = THEMES.some((entry) => entry.id === raw.theme) ? (raw.theme as string) : DEFAULT_THEME
  const mode: AppearanceMode = raw.mode === 'light' || raw.mode === 'dark' ? raw.mode : 'system'
  return { theme, mode }
}

export const isDefault = (value: Appearance) => value.theme === DEFAULT_THEME && value.mode === 'system'

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

export { appearance }

/** The mode actually on screen, with "system" resolved against the OS. */
export const resolvedMode = (): 'light' | 'dark' => {
  const mode = appearance().mode
  return mode === 'system' ? (systemDark() ? 'dark' : 'light') : mode
}

const applyToDocument = () => {
  const root = document.documentElement
  const value = appearance()
  if (value.theme === DEFAULT_THEME) delete root.dataset.theme
  else root.dataset.theme = value.theme
  root.classList.toggle('dark', resolvedMode() === 'dark')
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
