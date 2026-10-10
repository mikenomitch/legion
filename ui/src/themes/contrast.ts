// WCAG contrast math for the design lab, shared by the theme studio's live
// readouts and `npm run lab:contrast`. No imports, so Node can load it
// directly from the audit script.

export type RGB = [number, number, number]

export const hexToRgb = (hex: string): RGB => {
  const value = hex.replace('#', '')
  const full = value.length === 3 ? value.split('').map((c) => c + c).join('') : value
  return [0, 2, 4].map((offset) => parseInt(full.slice(offset, offset + 2), 16) || 0) as RGB
}

export const rgbToHex = (rgb: RGB) => `#${rgb.map((v) => Math.round(v).toString(16).padStart(2, '0')).join('')}`

const luminance = (rgb: RGB) => {
  const [r, g, b] = rgb.map((v) => {
    const c = v / 255
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

export const contrast = (a: RGB | string, b: RGB | string) => {
  const [x, y] = [luminance(typeof a === 'string' ? hexToRgb(a) : a), luminance(typeof b === 'string' ? hexToRgb(b) : b)].sort((m, n) => n - m)
  return (x + 0.05) / (y + 0.05)
}

export const mix = (rgb: RGB, target: number, amount: number): RGB => rgb.map((v) => Math.round(v + (target - v) * amount)) as RGB
export const over = (fg: RGB, bg: RGB, alpha: number): RGB => fg.map((v, i) => Math.round(v * alpha + bg[i] * (1 - alpha))) as RGB

// The 200..800 ramp the lab derives from one status hue (treated as 500), so
// the components' `text-emerald-700` / `dark:text-emerald-300` land on tints.
export const statusRamp = (base: string) => {
  const rgb = hexToRgb(base)
  return {
    200: mix(rgb, 255, 0.7),
    300: mix(rgb, 255, 0.48),
    400: mix(rgb, 255, 0.24),
    500: rgb,
    600: mix(rgb, 0, 0.14),
    700: mix(rgb, 0, 0.3),
    800: mix(rgb, 0, 0.45),
  } as Record<200 | 300 | 400 | 500 | 600 | 700 | 800, RGB>
}

// Tailwind's own shades, which themes without status colors keep.
export const TAILWIND_STATUS = {
  ok: { 300: '#6ee7b7', 400: '#34d399', 500: '#10b981', 700: '#047857' },
  warn: { 300: '#fcd34d', 400: '#fbbf24', 500: '#f59e0b', 700: '#b45309' },
  danger: { 300: '#fca5a5', 400: '#f87171', 500: '#ef4444', 700: '#b91c1c' },
  info: { 300: '#7dd3fc', 400: '#38bdf8', 500: '#0ea5e9', 700: '#0369a1' },
} as const

export type StatusName = keyof typeof TAILWIND_STATUS

// Badge text on its own tinted wash, exactly as the Badge component draws it.
export const badgeContrast = (mode: 'light' | 'dark', surface: string, status: StatusName, hue?: string) => {
  const shades = hue
    ? statusRamp(hue)
    : (Object.fromEntries(Object.entries(TAILWIND_STATUS[status]).map(([k, v]) => [k, hexToRgb(v)])) as Record<number, RGB>)
  const bg = hexToRgb(surface)
  const [text, wash] = mode === 'dark' ? [shades[300], over(shades[400], bg, 0.1)] : [shades[700], over(shades[500], bg, 0.1)]
  return contrast(text, wash)
}

export type ContrastCheck = { id: string; label: string; ratio: number; min: number }

type AuditInput = {
  mode: 'light' | 'dark'
  colors: Record<'canvas' | 'surface' | 'ink' | 'muted' | 'faint' | 'accent' | 'accentStrong', string>
  status?: Record<StatusName, string>
  sidebar?: Partial<Record<'surface' | 'ink' | 'muted' | 'faint', string>>
}

export const auditTheme = (theme: AuditInput): ContrastCheck[] => {
  const c = theme.colors
  const checks: ContrastCheck[] = [
    { id: 'ink', label: 'Body text on surface', ratio: contrast(c.ink, c.surface), min: 7 },
    { id: 'ink-canvas', label: 'Body text on canvas', ratio: contrast(c.ink, c.canvas), min: 7 },
    { id: 'muted', label: 'Secondary text on surface', ratio: contrast(c.muted, c.surface), min: 4.5 },
    { id: 'muted-canvas', label: 'Secondary text on canvas', ratio: contrast(c.muted, c.canvas), min: 4.5 },
    { id: 'faint', label: 'Faint text on surface', ratio: contrast(c.faint, c.surface), min: 4.5 },
    { id: 'faint-canvas', label: 'Faint text on canvas', ratio: contrast(c.faint, c.canvas), min: 4.5 },
    { id: 'link', label: 'Links on surface', ratio: contrast(c.accentStrong, c.surface), min: 4.5 },
  ]
  for (const name of Object.keys(TAILWIND_STATUS) as StatusName[]) {
    checks.push({ id: `badge-${name}`, label: `${name} badge`, ratio: badgeContrast(theme.mode, c.surface, name, theme.status?.[name]), min: 4.5 })
  }
  if (theme.sidebar) {
    const s = { ...c, ...theme.sidebar }
    checks.push({ id: 'sidebar-ink', label: 'Sidebar text', ratio: contrast(s.ink, s.surface), min: 7 })
    checks.push({ id: 'sidebar-muted', label: 'Sidebar secondary text', ratio: contrast(s.muted, s.surface), min: 4.5 })
    checks.push({ id: 'sidebar-faint', label: 'Sidebar faint text', ratio: contrast(s.faint, s.surface), min: 4.5 })
  }
  return checks
}
