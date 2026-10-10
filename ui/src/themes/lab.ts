/*
 * Design lab entry (dev server only). Applies a theme before the app mounts
 * and docks the theme studio.
 *
 *   npm run lab                                 studio + live/fake data switch
 *   http://localhost:5173/jobs?design=nord      open with a preset
 *   http://localhost:5173/?design=current       back to the shipped look
 *   &shot=1                                     no studio (screenshots)
 *
 * index.tsx imports this only on the dev server (npm run lab, a ?design=
 * URL, or a browser that has used the studio), so production builds contain
 * none of it — no theme data, no fonts, no studio.
 */
import './fonts'
import './themes.css'
import { findTheme, type DesignTheme } from './catalog'
import { applyTheme } from './engine'
import { SHIPPED, STATE_KEY, mountStudio } from './studio'

type Stored = { presetId: string; theme?: DesignTheme }

const readStored = (): Stored | undefined => {
  try {
    const raw = localStorage.getItem(STATE_KEY)
    return raw ? (JSON.parse(raw) as Stored) : undefined
  } catch {
    return undefined
  }
}

const writeStored = (value: Stored) => {
  try {
    localStorage.setItem(STATE_KEY, JSON.stringify(value))
  } catch {
    // storage unavailable
  }
}

export function initDesignLab() {
  const params = new URLSearchParams(window.location.search)
  const requested = params.get('design')
  let presetId = SHIPPED
  let theme: DesignTheme | undefined

  if (requested) {
    // A link names a preset and starts it fresh, discarding unsaved edits.
    const preset = findTheme(requested)
    if (preset) {
      presetId = preset.id
      theme = JSON.parse(JSON.stringify(preset)) as DesignTheme
    }
    writeStored(theme ? { presetId, theme } : { presetId: SHIPPED })
  } else {
    const stored = readStored()
    if (stored?.theme && stored.presetId !== SHIPPED) {
      presetId = stored.presetId
      theme = stored.theme
    }
  }

  applyTheme(theme)
  if (!params.has('shot')) {
    // The studio needs <body>; index.tsx runs this before the app renders.
    mountStudio(theme, presetId)
  }
}
