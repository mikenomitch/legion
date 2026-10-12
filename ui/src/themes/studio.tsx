/*
 * Theme studio (dev server only): a docked panel for tuning the UI's look
 * live — colors, status hues, type, shape, depth, sidebar and title — with
 * every catalog theme as a preset, live WCAG contrast, and CSS/JSON export.
 *
 * Rendered into a shadow root so the themes it edits never restyle it.
 */
import { For, Show, createEffect, createMemo, createResource, createSignal, on, onCleanup } from 'solid-js'
import { createStore, reconcile, unwrap } from 'solid-js/store'
import { render } from 'solid-js/web'
import { ALL_PRESETS, BASELINE_THEMES, findTheme, DESIGN_THEMES, type DesignTheme, type RadiusScale, type StatusColors, type ThemeColors, type TweakAxis } from './catalog'
import { RIFF_THEMES } from './riffs'
import { SWISS_COMBO_PRESETS, SWISS_PALETTES, SWISS_PALETTE_PRESETS, SWISS_TWEAKS, SWISS_TWEAK_PRESETS } from './swiss'
import { auditTheme, badgeContrast, contrast, type StatusName } from './contrast'
import { applyTheme, exportCss, normalizeHex } from './engine'
import { FONT_OPTIONS, type FontOption } from './fontOptions'
import studioCss from './studio.css?inline'

const STATE_KEY = 'ui-design-studio'
const SAVED_KEY = 'ui-design-studio-presets'
const OPEN_KEY = 'ui-design-studio-open'
const SHIPPED = 'shipped'
const PANEL_WIDTH = 360

const read = <T,>(key: string, fallback: T): T => {
  try {
    const raw = localStorage.getItem(key)
    return raw === null ? fallback : (JSON.parse(raw) as T)
  } catch {
    return fallback
  }
}
const write = (key: string, value: unknown) => {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // storage unavailable; the studio still works for this page load
  }
}

const clone = <T,>(value: T): T => JSON.parse(JSON.stringify(value)) as T

const COLOR_FIELDS: Array<{ key: keyof ThemeColors; label: string; on?: keyof ThemeColors; min?: number }> = [
  { key: 'canvas', label: 'Canvas (page)' },
  { key: 'surface', label: 'Surface (cards)' },
  { key: 'surface2', label: 'Surface 2 (headers, hover)' },
  { key: 'line', label: 'Line' },
  { key: 'lineStrong', label: 'Line strong' },
  { key: 'ink', label: 'Ink (body text)', on: 'surface', min: 7 },
  { key: 'muted', label: 'Muted text', on: 'surface', min: 4.5 },
  { key: 'faint', label: 'Faint text', on: 'surface', min: 4.5 },
  { key: 'accent', label: 'Accent (fills, icons)' },
  { key: 'accentStrong', label: 'Accent strong (links)', on: 'surface', min: 4.5 },
]

const SIDEBAR_FIELDS: Array<keyof ThemeColors> = ['surface', 'surface2', 'line', 'lineStrong', 'ink', 'muted', 'faint', 'accent', 'accentStrong']

const STATUS_FIELDS: Array<{ key: StatusName; label: string }> = [
  { key: 'ok', label: 'OK (running, healthy)' },
  { key: 'warn', label: 'Warn (pending, drained)' },
  { key: 'danger', label: 'Danger (failed, lost)' },
  { key: 'info', label: 'Info (rolling, starting)' },
]

const DEFAULT_STATUS: StatusColors = { ok: '#10b981', warn: '#f59e0b', danger: '#ef4444', info: '#0ea5e9' }

const SHADOWS: Array<{ label: string; card: string; pop: string }> = [
  { label: 'None', card: 'none', pop: '0 16px 40px -16px rgb(0 0 0 / 0.4)' },
  { label: 'Hairline (shipped)', card: '0 1px 2px 0 rgb(0 0 0 / 0.04)', pop: '0 12px 32px -12px rgb(0 0 0 / 0.35)' },
  { label: 'Soft layered', card: '0 1px 2px rgb(16 24 40 / 0.04), 0 4px 12px -2px rgb(16 24 40 / 0.06)', pop: '0 24px 48px -12px rgb(16 24 40 / 0.25)' },
  { label: 'Deep (dark UIs)', card: '0 1px 2px rgb(0 0 0 / 0.5)', pop: '0 24px 48px -16px rgb(0 0 0 / 0.75)' },
  { label: 'Hard offset', card: '4px 4px 0 0 #111111', pop: '6px 6px 0 0 #111111' },
  { label: 'Accent glow', card: '0 0 0 1px rgb(var(--c-brass) / 0.08), 0 0 28px -14px rgb(var(--c-brass) / 0.5)', pop: '0 0 0 1px rgb(var(--c-brass) / 0.3), 0 0 40px -10px rgb(var(--c-brass) / 0.4)' },
]

const RADII: Array<{ value: RadiusScale; label: string }> = [
  { value: 'none', label: 'None (square, incl. pills)' },
  { value: 'sharp', label: 'Sharp (2px)' },
  { value: 'default', label: 'Default (shipped)' },
  { value: 'soft', label: 'Soft (8–12px)' },
  { value: 'round', label: 'Round (10–16px)' },
]

type DataStatus = { mode: string; preference: string; target: string; reachable: boolean; fixtures: number; recordedAt?: string }

const fontLabel = (stack: string, group: FontOption['group']) => {
  const first = stack.split(',')[0].trim().replace(/^['"]|['"]$/g, '')
  const matches = FONT_OPTIONS.filter((option) => option.stack.split(',')[0].trim().replace(/^['"]|['"]$/g, '') === first)
  return (matches.find((option) => option.group === group) ?? matches[0])?.stack
}

function Studio(props: { initial: DesignTheme | undefined; initialPreset: string }) {
  const [saved, setSaved] = createSignal<DesignTheme[]>(read(SAVED_KEY, []))
  const presets = () => [...ALL_PRESETS, ...saved()]
  const presetById = (id: string) => presets().find((preset) => preset.id === id)

  const [presetId, setPresetId] = createSignal(props.initialPreset)
  const [shipped, setShipped] = createSignal(!props.initial)
  // The shipped look is Swiss Red in whichever mode is on screen.
  const fallback = () => findTheme(document.documentElement.classList.contains('dark') ? 'swiss-dark' : 'swiss')!
  const [theme, setTheme] = createStore<DesignTheme>(clone(props.initial ?? fallback()))
  const [open, setOpen] = createSignal(read(OPEN_KEY, import.meta.env.MODE === 'lab'))
  const [toast, setToast] = createSignal('')
  const [importText, setImportText] = createSignal('')
  const [saveName, setSaveName] = createSignal('')
  const [exportText, setExportText] = createSignal('')

  let toastTimer: ReturnType<typeof setTimeout> | undefined
  const flash = (message: string) => {
    setToast(message)
    clearTimeout(toastTimer)
    toastTimer = setTimeout(() => setToast(''), 1800)
  }

  // Any edit means "show my working theme", even when starting from the
  // shipped look.
  const edit = (fn: () => void) => {
    if (shipped()) {
      setShipped(false)
      setPresetId(theme.id)
    }
    fn()
  }

  // One place applies and persists whatever the panel shows.
  createEffect(
    on([() => JSON.stringify(theme), shipped], ([snapshot, isShipped]) => {
      if (isShipped) {
        applyTheme(undefined)
        write(STATE_KEY, { presetId: SHIPPED })
        return
      }
      const current = JSON.parse(snapshot) as DesignTheme
      applyTheme(current)
      write(STATE_KEY, { presetId: presetId(), theme: current })
    }),
  )

  // Dock: push the app over instead of covering its right edge.
  createEffect(() => {
    document.body.style.marginRight = open() ? `${PANEL_WIDTH}px` : ''
    write(OPEN_KEY, open())
  })
  onCleanup(() => (document.body.style.marginRight = ''))

  const edited = createMemo(() => {
    if (shipped()) return false
    const base = presetById(presetId())
    return !base || JSON.stringify(base) !== JSON.stringify(theme)
  })

  const loadPreset = (id: string) => {
    if (id === SHIPPED) {
      setPresetId(SHIPPED)
      setTheme(reconcile(clone(fallback())))
      setShipped(true)
      return
    }
    const preset = presetById(id)
    if (!preset) return
    setPresetId(id)
    setShipped(false)
    setTheme(reconcile(clone(preset)))
  }

  // --- Swiss picker: palette × mode × tweaks (the contract a production
  // picker stores in localStorage) -----------------------------------------
  const applyPalette = (paletteId: string, mode: 'light' | 'dark' = theme.mode) => {
    const palette = SWISS_PALETTES.find((entry) => entry.id === paletteId)
    if (!palette) return
    const spec = palette[mode]
    edit(() => {
      if (!(theme.flourishes ?? theme.id).startsWith('swiss')) setTheme('flourishes', 'swiss')
      setTheme('mode', mode)
      setTheme('palette', palette.id)
      setTheme('colors', clone(spec.colors))
      setTheme('status', clone(spec.status))
      setTheme('signal', spec.signal)
      setTheme('sidebar', undefined)
    })
  }
  const setTweak = (axis: TweakAxis, value: string) =>
    edit(() => setTheme('tweaks', (current) => ({ ...(current ?? {}), [axis]: value })))
  const pickerState = () =>
    JSON.stringify({ palette: theme.palette ?? 'red', mode: theme.mode, tweaks: Object.fromEntries(Object.entries(theme.tweaks ?? {}).filter(([, v]) => v && v !== 'default')) })

  // The app's own sun/moon toggle flips the `dark` class. For a preset with a
  // twin in the other mode, follow it there instead of leaving the palette
  // mismatched; edited themes and unpaired presets are left alone.
  const followAppToggle = new MutationObserver(() => {
    const isDark = document.documentElement.classList.contains('dark')
    if (shipped() || isDark === (theme.mode === 'dark')) return
    // A Swiss palette knows both modes, so keep every tweak and swap colors.
    if (theme.palette) return applyPalette(theme.palette, isDark ? 'dark' : 'light')
    if (theme.pair && !edited()) loadPreset(theme.pair)
  })
  followAppToggle.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
  onCleanup(() => followAppToggle.disconnect())

  const order = () => [SHIPPED, ...presets().map((preset) => preset.id)]
  const step = (delta: number) => {
    const ids = order()
    const index = Math.max(0, ids.indexOf(shipped() ? SHIPPED : presetId()))
    loadPreset(ids[(index + delta + ids.length) % ids.length])
  }

  const checks = createMemo(() => auditTheme({ ...theme, status: theme.status, sidebar: theme.sidebar }))
  const failing = createMemo(() => checks().filter((check) => check.ratio < check.min))
  const buttonText = createMemo(() => contrast(theme.colors.canvas, theme.colors.accent))

  // --- data source (npm run lab) -------------------------------------------
  const [dataStatus, { refetch }] = createResource<DataStatus | null>(async () => {
    try {
      const response = await fetch('/__legion-lab/status')
      return response.ok ? ((await response.json()) as DataStatus) : null
    } catch {
      return null
    }
  })
  const setData = (preference: 'auto' | 'live' | 'fake') => {
    document.cookie = `legion-data=${preference}; path=/; SameSite=Lax; max-age=31536000`
    window.location.reload()
  }
  const dataLabel = () => {
    const status = dataStatus()
    if (status === undefined) return 'Checking…'
    if (status === null) return 'Live manager (direct)'
    if (status.mode === 'record') return 'Recording from live manager'
    if (status.preference === 'fake' || (status.preference === 'auto' && !status.reachable)) return 'Fake data (recorded)'
    return 'Live manager'
  }

  // --- export / import -------------------------------------------------------
  const copy = async (text: string, label: string) => {
    setExportText(text)
    try {
      await navigator.clipboard.writeText(text)
      flash(`${label} copied`)
    } catch {
      flash('Clipboard blocked — copy from the box below')
    }
  }
  const themeJson = () => JSON.stringify(unwrap(theme), null, 2)
  const applyImport = () => {
    try {
      const parsed = JSON.parse(importText()) as DesignTheme
      if (!parsed.colors || !parsed.type || !parsed.mode) throw new Error('needs mode, colors and type')
      edit(() => setTheme(reconcile({ ...clone(fallback()), ...parsed })))
      flash('Theme imported')
    } catch (error) {
      flash(`Not a theme: ${(error as Error).message}`)
    }
  }
  const saveAs = () => {
    const name = saveName().trim()
    if (!name) return flash('Name the preset first')
    const id = `my-${name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')}`
    const entry: DesignTheme = { ...clone(unwrap(theme)), id, name, flourishes: theme.flourishes ?? theme.id }
    const next = [...saved().filter((preset) => preset.id !== id), entry]
    setSaved(next)
    write(SAVED_KEY, next)
    setPresetId(id)
    setTheme(reconcile(clone(entry)))
    setSaveName('')
    flash(`Saved “${name}”`)
  }
  const deleteSaved = () => {
    const id = presetId()
    const next = saved().filter((preset) => preset.id !== id)
    setSaved(next)
    write(SAVED_KEY, next)
    loadPreset(DESIGN_THEMES[0].id)
    flash('Preset deleted')
  }
  const shareLink = () => {
    const url = new URL(window.location.href)
    url.searchParams.set('design', shipped() ? 'current' : presetId())
    void copy(url.toString(), 'Link')
  }

  // --- editors -----------------------------------------------------------------
  const setColor = (key: keyof ThemeColors, hex: string) => edit(() => setTheme('colors', key, hex as `#${string}`))
  const setStatus = (key: StatusName, hex: string) =>
    edit(() => setTheme('status', (current) => ({ ...(current ?? DEFAULT_STATUS), [key]: hex })))
  const setSidebar = (key: keyof ThemeColors, hex: string) =>
    edit(() => setTheme('sidebar', (current) => ({ ...(current ?? {}), [key]: hex })))
  const toggleSidebar = (enabled: boolean) =>
    edit(() =>
      setTheme(
        'sidebar',
        enabled ? Object.fromEntries(SIDEBAR_FIELDS.map((key) => [key, theme.colors[key]])) : undefined,
      ),
    )
  const setTitle = (patch: NonNullable<DesignTheme['title']>) => edit(() => setTheme('title', (current) => ({ ...(current ?? {}), ...patch })))

  const ColorRow = (rowProps: { label: string; value: string; onChange: (hex: string) => void; ratio?: number; min?: number; ratioTitle?: string }) => {
    const [draft, setDraft] = createSignal(rowProps.value)
    createEffect(on(() => rowProps.value, (value) => setDraft(value)))
    const valid = () => Boolean(normalizeHex(draft()))
    const id = `c-${Math.random().toString(36).slice(2, 8)}`
    return (
      <div class="color-row">
        <input type="color" id={id} value={rowProps.value} onInput={(event) => rowProps.onChange(event.currentTarget.value)} aria-label={rowProps.label} />
        <label for={id} title={rowProps.label}>{rowProps.label}</label>
        <input
          type="text"
          value={draft()}
          aria-label={`${rowProps.label} hex`}
          aria-invalid={!valid()}
          spellcheck={false}
          onInput={(event) => {
            setDraft(event.currentTarget.value)
            const hex = normalizeHex(event.currentTarget.value)
            if (hex) rowProps.onChange(hex)
          }}
        />
        <span
          class={`ratio ${rowProps.ratio === undefined ? '' : rowProps.ratio >= (rowProps.min ?? 4.5) ? 'ok' : 'bad'}`}
          title={rowProps.ratioTitle ?? (rowProps.min ? `Contrast; needs ${rowProps.min}:1` : undefined)}
        >
          {rowProps.ratio === undefined ? '' : `${rowProps.ratio.toFixed(1)}${rowProps.ratio >= (rowProps.min ?? 4.5) ? ' ✓' : ' ✕'}`}
        </span>
      </div>
    )
  }

  const FontSelect = (selectProps: { label: string; value: string; group: FontOption['group']; onChange: (stack: string) => void }) => (
    <label class="field">
      <span>{selectProps.label}</span>
      <select value={fontLabel(selectProps.value, selectProps.group) ?? selectProps.value} onChange={(event) => selectProps.onChange(event.currentTarget.value)}>
        <Show when={!fontLabel(selectProps.value, selectProps.group)}>
          <option value={selectProps.value}>Custom: {selectProps.value.split(',')[0]}</option>
        </Show>
        <For each={['Sans', 'Serif & display', 'Mono'] as const}>
          {(group) => (
            <optgroup label={group}>
              <For each={FONT_OPTIONS.filter((option) => option.group === group)}>
                {(option) => <option value={option.stack}>{option.label}</option>}
              </For>
            </optgroup>
          )}
        </For>
      </select>
    </label>
  )

  const Slider = (sliderProps: { label: string; min: number; max: number; step: number; value: number | undefined; fallback: number; unit: string; onInput: (value: number) => void }) => (
    <label class="field">
      <span>{sliderProps.label}</span>
      <span class="slider">
        <input
          type="range"
          min={sliderProps.min}
          max={sliderProps.max}
          step={sliderProps.step}
          value={sliderProps.value ?? sliderProps.fallback}
          onInput={(event) => sliderProps.onInput(Number(event.currentTarget.value))}
        />
        <output>{sliderProps.value === undefined ? 'theme' : `${sliderProps.value}${sliderProps.unit}`}</output>
      </span>
    </label>
  )

  // Round two first, by family (base, other mode, riffs), then round one.
  const family = (name: NonNullable<DesignTheme['family']>) =>
    [...BASELINE_THEMES, ...DESIGN_THEMES, ...RIFF_THEMES]
      .filter((preset) => preset.family === name)
      .sort((a, b) => ['base', 'counterpart', 'riff'].indexOf(a.role ?? 'riff') - ['base', 'counterpart', 'riff'].indexOf(b.role ?? 'riff'))
  const groups = createMemo(() => [
    { label: 'Swiss palettes · light and dark', items: [presetById('swiss')!, presetById('swiss-dark')!, ...SWISS_PALETTE_PRESETS] },
    { label: 'Swiss tweaks · on Swiss Red', items: SWISS_TWEAK_PRESETS },
    { label: 'Swiss combinations', items: SWISS_COMBO_PRESETS },
    { label: 'Command Post family', items: family('command-post') },
    { label: 'Forge family', items: family('forge') },
    { label: 'Swiss family', items: family('swiss') },
    { label: 'Round one · dark', items: DESIGN_THEMES.filter((preset) => preset.mode === 'dark' && !preset.family) },
    { label: 'Round one · light', items: DESIGN_THEMES.filter((preset) => preset.mode === 'light' && !preset.family) },
    { label: 'My presets', items: saved() },
  ])
  const number = (preset: DesignTheme) => {
    const index = DESIGN_THEMES.indexOf(preset)
    return index >= 0 ? `${String(index + 1).padStart(2, '0')}. ` : ''
  }

  return (
    <>
      <style>{studioCss}</style>
      <Show
        when={open()}
        fallback={
          <button type="button" class="launcher" onClick={() => setOpen(true)} aria-label="Open theme studio">
            <i />
            Theme studio
          </button>
        }
      >
        <aside class="panel" aria-label="Theme studio">
          <header class="top">
            <div class="title-row">
              <h1>Theme studio</h1>
              <span class={`pill ${failing().length ? 'bad' : 'ok'}`} title="WCAG contrast checks for this theme">
                {failing().length ? `${failing().length} contrast issue${failing().length > 1 ? 's' : ''}` : 'AA ✓'}
              </span>
              <span class="spacer" />
              <button type="button" class="icon-btn" onClick={() => setOpen(false)} aria-label="Hide theme studio" title="Hide">
                Hide
              </button>
            </div>
            <div class="preset-row">
              <button type="button" class="icon-btn" onClick={() => step(-1)} aria-label="Previous preset">‹</button>
              <select
                aria-label="Preset"
                value={shipped() ? SHIPPED : presetId()}
                onChange={(event) => loadPreset(event.currentTarget.value)}
              >
                <option value={SHIPPED}>Shipped look (no overrides)</option>
                <For each={groups().filter((group) => group.items.length)}>
                  {(group) => (
                    <optgroup label={group.label}>
                      <For each={group.items}>{(preset) => <option value={preset.id}>{number(preset)}{preset.name}</option>}</For>
                    </optgroup>
                  )}
                </For>
              </select>
              <button type="button" class="icon-btn" onClick={() => step(1)} aria-label="Next preset">›</button>
            </div>
            <Show when={!shipped() && !theme.palette && presetById(presetId())?.pair}>
              {(pairId) => (
                <div class="seg" role="group" aria-label="Light or dark version of this preset">
                  <For each={['light', 'dark'] as const}>
                    {(mode) => (
                      <button
                        type="button"
                        aria-pressed={theme.mode === mode}
                        onClick={() => mode !== theme.mode && loadPreset(pairId())}
                      >
                        {mode === 'light' ? 'Light version' : 'Dark version'}
                      </button>
                    )}
                  </For>
                </div>
              )}
            </Show>
            <div class="edited">
              <Show when={edited()} fallback={<span>{shipped() ? 'Showing the shipped CSS, untouched.' : presetById(presetId())?.tagline}</span>}>
                <b>● Edited</b>
                <span>from {presetById(presetId())?.name ?? 'preset'}</span>
                <span class="spacer" />
                <button type="button" class="icon-btn" onClick={() => loadPreset(presetId())}>Reset</button>
              </Show>
            </div>
          </header>

          <div class="body">
            <details open>
              <summary>Picker <span class="pill">Swiss · palette × tweaks</span></summary>
              <div class="section">
                <p class="hint">
                  The app's Appearance picker, applied to this lab theme. It sets <code>data-palette</code>, the{' '}
                  <code>dark</code> class and <code>data-tweak-*</code> on &lt;html&gt;; the saved value is shown below.
                </p>
                <label class="field">
                  <span>Palette</span>
                  <select value={theme.palette ?? ''} onChange={(event) => applyPalette(event.currentTarget.value)}>
                    <Show when={!theme.palette}>
                      <option value="">Not a Swiss palette</option>
                    </Show>
                    <For each={SWISS_PALETTES}>{(palette) => <option value={palette.id}>{palette.name}</option>}</For>
                  </select>
                </label>
                <label class="field">
                  <span>Mode</span>
                  <span class="seg" role="group" aria-label="Picker mode">
                    <For each={['light', 'dark'] as const}>
                      {(mode) => (
                        <button
                          type="button"
                          aria-pressed={theme.mode === mode}
                          onClick={() => (theme.palette ? applyPalette(theme.palette, mode) : edit(() => setTheme('mode', mode)))}
                        >
                          {mode === 'light' ? 'Light' : 'Dark'}
                        </button>
                      )}
                    </For>
                  </span>
                </label>
                <For each={SWISS_TWEAKS}>
                  {(tweak) => (
                    <label class="field">
                      <span>{tweak.label}</span>
                      <span class="seg" role="group" aria-label={tweak.label}>
                        <For each={tweak.options}>
                          {(option) => (
                            <button
                              type="button"
                              title={option.description}
                              aria-pressed={(theme.tweaks?.[tweak.axis] ?? 'default') === option.value}
                              onClick={() => setTweak(tweak.axis, option.value)}
                            >
                              {option.label}
                            </button>
                          )}
                        </For>
                      </span>
                    </label>
                  )}
                </For>
                <code class="stored">{pickerState()}</code>
              </div>
            </details>

            <details open>
              <summary>Data <span class="pill">{dataLabel()}</span></summary>
              <div class="section">
                <div class="seg" role="group" aria-label="Data source">
                  <For each={['auto', 'live', 'fake'] as const}>
                    {(preference) => (
                      <button
                        type="button"
                        aria-pressed={dataStatus()?.preference === preference}
                        disabled={!dataStatus() || dataStatus()?.mode === 'record'}
                        onClick={() => setData(preference)}
                      >
                        {preference === 'auto' ? 'Auto' : preference === 'live' ? 'Live' : 'Fake'}
                      </button>
                    )}
                  </For>
                  <button type="button" onClick={() => void refetch()} title="Re-check the manager">↻</button>
                </div>
                <Show
                  when={dataStatus()}
                  fallback={
                    <p class="hint">
                      Talking to the manager directly. Run <code>npm run lab</code> to switch between a live manager and recorded
                      fake data from here.
                    </p>
                  }
                >
                  {(status) => (
                    <p class="hint">
                      Manager <code>{status().target}</code> {status().reachable ? 'is reachable' : 'is not reachable'}.{' '}
                      {status().fixtures} recorded RPCs
                      {status().recordedAt ? ` from ${new Date(status().recordedAt!).toLocaleDateString()}` : ''}. Auto uses the
                      manager when it answers and the recording otherwise.
                    </p>
                  )}
                </Show>
              </div>
            </details>

            <details open>
              <summary>Mode & surfaces</summary>
              <div class="section">
                <div class="seg" role="group" aria-label="Mode">
                  <For each={['light', 'dark'] as const}>
                    {(mode) => (
                      <button type="button" aria-pressed={theme.mode === mode} onClick={() => edit(() => setTheme('mode', mode))}>
                        {mode === 'light' ? 'Light' : 'Dark'}
                      </button>
                    )}
                  </For>
                </div>
                <p class="hint">Mode picks the app's light/dark status shades; set the colors below to match.</p>
                <For each={COLOR_FIELDS}>
                  {(field) => (
                    <ColorRow
                      label={field.label}
                      value={theme.colors[field.key]}
                      onChange={(hex) => setColor(field.key, hex)}
                      ratio={field.on ? contrast(theme.colors[field.key], theme.colors[field.on]) : undefined}
                      min={field.min}
                    />
                  )}
                </For>
                <ColorRow
                  label="Signal (structural red/gold)"
                  value={theme.signal ?? '#e10600'}
                  onChange={(hex) => edit(() => setTheme('signal', hex as `#${string}`))}
                />
                <div class="checks">
                  <div>
                    <span>Primary button text on accent</span>
                    <span class={`ratio ${buttonText() >= 4.5 ? 'ok' : 'bad'}`}>{buttonText().toFixed(1)}:1</span>
                  </div>
                </div>
              </div>
            </details>

            <details open>
              <summary>Status colors</summary>
              <div class="section">
                <For each={STATUS_FIELDS}>
                  {(field) => (
                    <ColorRow
                      label={field.label}
                      value={(theme.status ?? DEFAULT_STATUS)[field.key]}
                      onChange={(hex) => setStatus(field.key, hex)}
                      ratio={badgeContrast(theme.mode, theme.colors.surface, field.key, theme.status?.[field.key])}
                      min={4.5}
                      ratioTitle="Badge text on its tinted background; needs 4.5:1"
                    />
                  )}
                </For>
                <Show when={theme.status}>
                  <button type="button" class="btn" onClick={() => edit(() => setTheme('status', undefined))}>
                    Use Tailwind defaults
                  </button>
                </Show>
              </div>
            </details>

            <details>
              <summary>Sidebar</summary>
              <div class="section">
                <label class="check">
                  <input type="checkbox" checked={Boolean(theme.sidebar)} onChange={(event) => toggleSidebar(event.currentTarget.checked)} />
                  Give the nav rail its own colors (e.g. a dark rail on a light app)
                </label>
                <Show when={theme.sidebar}>
                  <For each={SIDEBAR_FIELDS}>
                    {(key) => (
                      <ColorRow
                        label={COLOR_FIELDS.find((field) => field.key === key)!.label}
                        value={theme.sidebar?.[key] ?? theme.colors[key]}
                        onChange={(hex) => setSidebar(key, hex)}
                        ratio={['ink', 'muted', 'faint'].includes(key) ? contrast(theme.sidebar?.[key] ?? theme.colors[key], theme.sidebar?.surface ?? theme.colors.surface) : undefined}
                        min={key === 'ink' ? 7 : 4.5}
                      />
                    )}
                  </For>
                </Show>
              </div>
            </details>

            <details open>
              <summary>Typography</summary>
              <div class="section">
                <FontSelect label="Body" group="Sans" value={theme.type.sans} onChange={(stack) => edit(() => setTheme('type', 'sans', stack))} />
                <FontSelect label="Titles" group="Serif & display" value={theme.type.display} onChange={(stack) => edit(() => setTheme('type', 'display', stack))} />
                <FontSelect label="Mono (IDs, logs)" group="Mono" value={theme.type.mono} onChange={(stack) => edit(() => setTheme('type', 'mono', stack))} />
                <Slider label="Base size" min={13} max={19} step={0.5} unit="px" value={theme.baseSize} fallback={16} onInput={(value) => edit(() => setTheme('baseSize', value))} />
                <Slider label="Title size" min={1.25} max={3.5} step={0.05} unit="rem" value={theme.title?.size} fallback={1.5} onInput={(value) => setTitle({ size: value })} />
                <Slider label="Title weight" min={300} max={900} step={50} unit="" value={theme.title?.weight} fallback={600} onInput={(value) => setTitle({ weight: value })} />
                <Slider label="Title tracking" min={-0.05} max={0.15} step={0.005} unit="em" value={theme.title?.tracking} fallback={0} onInput={(value) => setTitle({ tracking: value })} />
                <label class="field">
                  <span>Title case</span>
                  <span class="seg" role="group" aria-label="Title case">
                    <For each={['none', 'uppercase', 'lowercase'] as const}>
                      {(transform) => (
                        <button type="button" aria-pressed={(theme.title?.transform ?? 'none') === transform} onClick={() => setTitle({ transform })}>
                          {transform === 'none' ? 'As written' : transform === 'uppercase' ? 'UPPER' : 'lower'}
                        </button>
                      )}
                    </For>
                  </span>
                </label>
                <Show when={theme.baseSize !== undefined || theme.title}>
                  <button type="button" class="btn" onClick={() => edit(() => (setTheme('baseSize', undefined), setTheme('title', undefined)))}>
                    Reset size & title to the theme's own
                  </button>
                </Show>
              </div>
            </details>

            <details open>
              <summary>Shape & depth</summary>
              <div class="section">
                <label class="field">
                  <span>Corners</span>
                  <select value={theme.radius} onChange={(event) => edit(() => setTheme('radius', event.currentTarget.value as RadiusScale))}>
                    <For each={RADII}>{(radius) => <option value={radius.value}>{radius.label}</option>}</For>
                  </select>
                </label>
                <label class="field">
                  <span>Card shadow</span>
                  <select
                    value={SHADOWS.findIndex((shadow) => shadow.card === (theme.shadow?.card ?? SHADOWS[1].card)).toString()}
                    onChange={(event) => {
                      const index = Number(event.currentTarget.value)
                      if (index >= 0) edit(() => setTheme('shadow', { card: SHADOWS[index].card, pop: SHADOWS[index].pop }))
                    }}
                  >
                    <Show when={SHADOWS.every((shadow) => shadow.card !== (theme.shadow?.card ?? SHADOWS[1].card))}>
                      <option value="-1">Theme's own</option>
                    </Show>
                    <For each={SHADOWS}>{(shadow, index) => <option value={index().toString()}>{shadow.label}</option>}</For>
                  </select>
                </label>
                <Slider label="Card border" min={0} max={3} step={0.5} unit="px" value={theme.cardBorder} fallback={1} onInput={(value) => edit(() => setTheme('cardBorder', value))} />
                <label class="field">
                  <span>Flourishes</span>
                  <select
                    value={theme.flourishes ?? theme.id}
                    onChange={(event) => edit(() => setTheme('flourishes', event.currentTarget.value))}
                    title="Structural CSS from src/themes/themes.css: title treatments, card headers, backgrounds, nav states"
                  >
                    <option value="custom">None (tokens only)</option>
                    <For each={[...DESIGN_THEMES, ...RIFF_THEMES]}>{(preset) => <option value={preset.id}>From {preset.name}</option>}</For>
                  </select>
                </label>
                <p class="hint">Flourishes are each preset's structural extras (title treatment, card headers, backgrounds). Mix them with any palette.</p>
              </div>
            </details>

            <details>
              <summary>
                Accessibility <span class={`pill ${failing().length ? 'bad' : 'ok'}`}>{failing().length ? `${failing().length} failing` : 'all pass'}</span>
              </summary>
              <div class="section checks">
                <For each={checks()}>
                  {(check) => (
                    <div>
                      <span>{check.label}</span>
                      <span class={`ratio ${check.ratio >= check.min ? 'ok' : 'bad'}`}>
                        {check.ratio.toFixed(2)} / {check.min}
                      </span>
                    </div>
                  )}
                </For>
              </div>
            </details>

            <details>
              <summary>Save, export & import</summary>
              <div class="section">
                <div class="row">
                  <input type="text" placeholder="Preset name" value={saveName()} onInput={(event) => setSaveName(event.currentTarget.value)} aria-label="Preset name" style="flex:1" />
                  <button type="button" class="btn primary" onClick={saveAs}>Save preset</button>
                </div>
                <Show when={presetId().startsWith('my-')}>
                  <button type="button" class="btn danger" onClick={deleteSaved}>Delete “{presetById(presetId())?.name}”</button>
                </Show>
                <div class="row">
                  <button type="button" class="btn" onClick={() => void copy(exportCss(unwrap(theme)), 'CSS tokens')}>Copy CSS tokens</button>
                  <button type="button" class="btn" onClick={() => void copy(themeJson(), 'Theme JSON')}>Copy theme JSON</button>
                  <button type="button" class="btn" onClick={shareLink} disabled={edited()} title={edited() ? 'Links name a preset; save or export your edits' : undefined}>
                    Copy link
                  </button>
                </div>
                <Show when={exportText()}>
                  <textarea rows={8} readOnly value={exportText()} aria-label="Exported text" onFocus={(event) => event.currentTarget.select()} />
                </Show>
                <textarea rows={4} placeholder="Paste theme JSON here" value={importText()} onInput={(event) => setImportText(event.currentTarget.value)} aria-label="Theme JSON to import" />
                <button type="button" class="btn" onClick={applyImport} disabled={!importText().trim()}>Apply pasted JSON</button>
              </div>
            </details>
          </div>

          <footer class="bottom">
            <span class="hint">Saved in this browser · {presets().length} presets</span>
            <span class="spacer" />
            <Show when={toast()}>
              <span class="toast" role="status">{toast()}</span>
            </Show>
          </footer>
        </aside>
      </Show>
    </>
  )
}

export function mountStudio(initial: DesignTheme | undefined, initialPreset: string) {
  const host = document.createElement('div')
  host.setAttribute('data-theme-studio', '')
  document.body.appendChild(host)
  const shadow = host.attachShadow({ mode: 'open' })
  const mount = document.createElement('div')
  shadow.appendChild(mount)
  render(() => <Studio initial={initial} initialPreset={initialPreset} />, mount)
}

export { SHIPPED, STATE_KEY }
