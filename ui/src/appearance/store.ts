/*
 * The viewer's appearance choice: a palette, a mode and a set of tweaks,
 * saved per browser in localStorage and applied as attributes on <html>.
 *
 *   localStorage['legion-appearance'] = {"palette":"concrete","mode":"dark","tweaks":{"density":"compact"}}
 *   <html class="dark" data-palette="concrete" data-tweak-density="compact">
 *
 * Defaults are never written as attributes: no data-palette means Swiss Red,
 * no data-tweak-<axis> means that axis's first option, and mode "system"
 * follows the OS. index.html runs the same logic inline before first paint
 * (keep the two in step); this module owns it once the app is running.
 */
import { createSignal } from 'solid-js'
import { DEFAULT_PALETTE, PALETTES, TWEAKS, TWEAK_AXES, type TweakAxis, type Tweaks } from './palettes'

export const APPEARANCE_KEY = 'legion-appearance'

export type AppearanceMode = 'light' | 'dark' | 'system'

export type Appearance = {
  palette: string
  mode: AppearanceMode
  tweaks: Tweaks
}

export const DEFAULT_APPEARANCE: Appearance = { palette: DEFAULT_PALETTE, mode: 'system', tweaks: {} }

// Drops anything this build does not know (an old palette id, a removed
// tweak), so a stale or hand-edited value degrades to the defaults.
export const sanitizeAppearance = (value: unknown): Appearance => {
  if (!value || typeof value !== 'object') return DEFAULT_APPEARANCE
  const raw = value as Record<string, unknown>
  const palette = PALETTES.some((entry) => entry.id === raw.palette) ? (raw.palette as string) : DEFAULT_PALETTE
  const mode: AppearanceMode = raw.mode === 'light' || raw.mode === 'dark' ? raw.mode : 'system'
  const tweaks: Tweaks = {}
  const rawTweaks = raw.tweaks && typeof raw.tweaks === 'object' ? (raw.tweaks as Record<string, unknown>) : {}
  for (const tweak of TWEAKS) {
    const chosen = rawTweaks[tweak.axis]
    // The first option is the default and is never stored.
    if (tweak.options.slice(1).some((option) => option.value === chosen)) tweaks[tweak.axis] = chosen as string
  }
  return { palette, mode, tweaks }
}

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

export const isDefault = (value: Appearance) =>
  value.palette === DEFAULT_PALETTE && value.mode === 'system' && Object.keys(value.tweaks).length === 0

const prefersDark = () => typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches

const [systemDark, setSystemDark] = createSignal(prefersDark())
const [appearance, setAppearanceSignal] = createSignal<Appearance>(readStored())

export { appearance }

/** The mode actually on screen, with "system" resolved against the OS. */
export const resolvedMode = (): 'light' | 'dark' => {
  const mode = appearance().mode
  return mode === 'system' ? (systemDark() ? 'dark' : 'light') : mode
}

const tweakAttribute = (axis: TweakAxis) => `tweak${axis[0].toUpperCase()}${axis.slice(1)}`

const applyToDocument = () => {
  const root = document.documentElement
  const value = appearance()
  if (value.palette === DEFAULT_PALETTE) delete root.dataset.palette
  else root.dataset.palette = value.palette
  for (const axis of TWEAK_AXES) {
    const chosen = value.tweaks[axis]
    if (chosen) root.dataset[tweakAttribute(axis)] = chosen
    else delete root.dataset[tweakAttribute(axis)]
  }
  root.classList.toggle('dark', resolvedMode() === 'dark')
}

/** Replaces the whole choice, saves it and applies it. */
export const setAppearance = (next: Appearance) => {
  const value = sanitizeAppearance(next)
  setAppearanceSignal(value)
  writeStored(value)
  applyToDocument()
}

export const setPalette = (palette: string) => setAppearance({ ...appearance(), palette })
export const setMode = (mode: AppearanceMode) => setAppearance({ ...appearance(), mode })
export const setTweak = (axis: TweakAxis, value: string) => {
  const tweaks = { ...appearance().tweaks }
  const tweak = TWEAKS.find((entry) => entry.axis === axis)
  if (!tweak || value === tweak.options[0].value) delete tweaks[axis]
  else tweaks[axis] = value
  setAppearance({ ...appearance(), tweaks })
}
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
