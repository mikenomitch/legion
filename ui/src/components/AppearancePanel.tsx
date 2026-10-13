import { For, Show, createEffect, createMemo, createSignal } from 'solid-js'
import { COLOR_TOKENS, THEMES, type ColorToken } from '../appearance/themes'
import {
  activeOverrides,
  appearance,
  appliedVersion,
  isDefault,
  resetAppearance,
  resetColors,
  resolvedMode,
  setColor,
  setMode,
  setTheme,
  type AppearanceMode,
} from '../appearance/store'
import { Icon, cx, inputClass } from './ui'

/*
 * The appearance picker: light/dark/system, the color theme, and every color
 * of that theme by hand. Every change applies at once and is saved in this
 * browser only (localStorage), so there is no Save button and nothing reaches
 * the manager.
 */

const MODES: Array<{ value: AppearanceMode; label: string }> = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'system', label: 'System' },
]

// A theme as it looks in the current mode. `data-theme` on the swatch scopes
// that theme's tokens to it (themes.css), so the bg-* classes below paint in
// the swatch's theme whatever the page's own theme is.
function Swatch(props: { theme: string }) {
  return (
    <span aria-hidden="true" data-theme={props.theme} class="flex h-12 w-full items-end gap-1.5 rounded-md border border-line bg-canvas p-1.5">
      <span class="flex h-full flex-1 flex-col justify-between rounded-sm border border-line bg-surface p-1.5">
        <span class="block h-1 w-3/4 rounded-full bg-ink" />
        <span class="block h-1 w-1/2 rounded-full bg-muted/60" />
      </span>
      <span class="block h-4 w-5 rounded-sm bg-brass" />
    </span>
  )
}

// The colors on screen right now, read back from <html> as #rrggbb, so the
// inputs show the theme's value until the viewer overrides it.
const readColors = (): Record<ColorToken, string> => {
  const style = getComputedStyle(document.documentElement)
  const entries = COLOR_TOKENS.map((token) => {
    const [r, g, b] = style.getPropertyValue(`--c-${token.id}`).trim().split(/\s+/).map(Number)
    const hex = '#' + [r, g, b].map((channel) => (channel || 0).toString(16).padStart(2, '0')).join('')
    return [token.id, hex] as const
  })
  return Object.fromEntries(entries) as Record<ColorToken, string>
}

const luminance = (hex: string) => {
  const channel = (start: number) => {
    const value = parseInt(hex.slice(start, start + 2), 16) / 255
    return value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4
  }
  return 0.2126 * channel(1) + 0.7152 * channel(3) + 0.0722 * channel(5)
}

const contrast = (a: string, b: string) => {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (hi + 0.05) / (lo + 0.05)
}

// Pairs that carry reading; below 4.5:1 the picker says so (WCAG AA).
const CONTRAST_PAIRS: Array<{ label: string; fg: ColorToken; bg: ColorToken }> = [
  { label: 'Text on cards', fg: 'ink', bg: 'surface' },
  { label: 'Secondary text on cards', fg: 'muted', bg: 'surface' },
  { label: 'Links on cards', fg: 'brass-strong', bg: 'surface' },
  { label: 'Button labels', fg: 'canvas', bg: 'brass' },
]

// Accepts "abc123" or "#abc123"; anything else is ignored.
const parseHex = (text: string) => {
  const value = text.trim().replace(/^#?/, '#').toLowerCase()
  return /^#[0-9a-f]{6}$/.test(value) ? value : null
}

function ColorEditor() {
  const colors = createMemo(() => {
    appliedVersion()
    return readColors()
  })
  const overrides = () => activeOverrides()
  const lowContrast = () =>
    CONTRAST_PAIRS.map((pair) => ({ ...pair, ratio: contrast(colors()[pair.fg], colors()[pair.bg]) })).filter(
      (pair) => pair.ratio < 4.5,
    )

  return (
    <div class="mt-3 space-y-3">
      <p class="text-xs text-muted">
        Editing <span class="font-medium text-ink">{THEMES.find((theme) => theme.id === appearance().theme)?.name}</span> in{' '}
        {resolvedMode()} mode. Each theme keeps its own colors for light and for dark.
      </p>
      <div class="grid gap-x-4 gap-y-1.5 sm:grid-cols-2">
        <For each={COLOR_TOKENS}>
          {(token) => (
            <div class="flex items-center gap-2">
              <input
                type="color"
                id={`appearance-color-${token.id}`}
                class="h-7 w-9 shrink-0 cursor-pointer rounded border border-line bg-surface p-0.5"
                value={colors()[token.id]}
                onInput={(event) => setColor(token.id, event.currentTarget.value)}
              />
              <label for={`appearance-color-${token.id}`} class="min-w-0 flex-1 truncate text-xs text-ink">
                {token.label}
              </label>
              <input
                type="text"
                aria-label={`${token.label} hex value`}
                spellcheck={false}
                class={cx(inputClass, '!w-[5.5rem] !px-2 !py-1 font-mono !text-xs')}
                value={colors()[token.id]}
                onChange={(event) => {
                  const hex = parseHex(event.currentTarget.value)
                  if (hex) setColor(token.id, hex)
                  else event.currentTarget.value = colors()[token.id]
                }}
              />
              <button
                type="button"
                class="rounded p-1 text-faint transition-colors hover:bg-surface2 hover:text-ink disabled:invisible"
                disabled={!overrides()[token.id]}
                onClick={() => setColor(token.id, null)}
                aria-label={`Reset ${token.label} to the theme's color`}
                title="Use the theme's color"
              >
                <Icon name="x" size={12} />
              </button>
            </div>
          )}
        </For>
      </div>
      <Show when={lowContrast().length > 0}>
        <p class="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-ink" role="status">
          Hard to read:{' '}
          {lowContrast()
            .map((pair) => `${pair.label.toLowerCase()} ${pair.ratio.toFixed(1)}:1`)
            .join(', ')}
          . Aim for at least 4.5:1.
        </p>
      </Show>
      <button
        type="button"
        class="text-xs font-medium text-brass-strong underline-offset-2 hover:underline disabled:cursor-default disabled:text-faint disabled:no-underline"
        disabled={Object.keys(overrides()).length === 0}
        onClick={resetColors}
      >
        Use the theme's colors
      </button>
    </div>
  )
}

export function AppearancePanel(props: { open: boolean; onClose: () => void }) {
  let closeButton: HTMLButtonElement | undefined
  const [customizing, setCustomizing] = createSignal(false)
  const customCount = () => Object.keys(activeOverrides()).length
  createEffect(() => {
    if (props.open) requestAnimationFrame(() => closeButton?.focus())
  })

  return (
    <Show when={props.open}>
      <div
        class="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/50 p-4 pt-[10vh] backdrop-blur-[2px]"
        data-modal
        onClick={props.onClose}
      >
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="appearance-title"
          class="w-full max-w-2xl overflow-hidden rounded-xl border border-line bg-surface shadow-pop"
          onClick={(event) => event.stopPropagation()}
        >
          <div class="flex items-center justify-between gap-3 border-b border-line px-5 py-3">
            <h2 id="appearance-title" class="flex items-center gap-2 font-display text-base font-semibold text-ink">
              <Icon name="palette" size={17} class="text-brass" />
              Appearance
            </h2>
            <button
              ref={closeButton}
              type="button"
              class="rounded-md p-1.5 text-muted transition-colors hover:bg-surface2 hover:text-ink"
              onClick={props.onClose}
              aria-label="Close"
            >
              <Icon name="x" size={16} />
            </button>
          </div>

          <div class="space-y-6 px-5 py-4">
            <section>
              <p class="eyebrow mb-2">Mode</p>
              <div role="radiogroup" aria-label="Mode" class="inline-flex rounded-md border border-line bg-surface2 p-0.5">
                <For each={MODES}>
                  {(mode) => (
                    <button
                      type="button"
                      role="radio"
                      aria-checked={appearance().mode === mode.value}
                      class={cx(
                        'rounded px-3 py-1 text-xs font-medium transition-colors',
                        appearance().mode === mode.value ? 'bg-surface text-ink shadow-card' : 'text-muted hover:text-ink',
                      )}
                      onClick={() => setMode(mode.value)}
                    >
                      {mode.label}
                    </button>
                  )}
                </For>
              </div>
            </section>

            <section>
              <p class="eyebrow mb-2">Theme</p>
              <div role="radiogroup" aria-label="Theme" class="grid grid-cols-2 gap-2 sm:grid-cols-3">
                <For each={THEMES}>
                  {(theme) => (
                    <button
                      type="button"
                      role="radio"
                      aria-checked={appearance().theme === theme.id}
                      title={theme.description}
                      class={cx(
                        'flex flex-col gap-1.5 rounded-lg border p-1.5 text-left text-xs font-medium transition-colors',
                        appearance().theme === theme.id
                          ? 'border-brass text-ink ring-1 ring-brass'
                          : 'border-line text-muted hover:border-line-strong hover:text-ink',
                      )}
                      onClick={() => setTheme(theme.id)}
                    >
                      <Swatch theme={theme.id} />
                      <span class="px-0.5">{theme.name}</span>
                    </button>
                  )}
                </For>
              </div>
              <p class="mt-2 text-xs text-muted">{THEMES.find((theme) => theme.id === appearance().theme)?.description}</p>
            </section>

            <section>
              <button
                type="button"
                class="flex w-full items-center justify-between gap-2 text-left"
                aria-expanded={customizing()}
                onClick={() => setCustomizing((open) => !open)}
              >
                <span class="eyebrow">Colors</span>
                <span class="flex items-center gap-1.5 text-xs text-muted">
                  {customCount() > 0 ? `${customCount()} set by hand` : 'Customize'}
                  <Icon name="chevronRight" size={14} class={cx('transition-transform', customizing() && 'rotate-90')} />
                </span>
              </button>
              <Show when={customizing()}>
                <ColorEditor />
              </Show>
            </section>
          </div>

          <div class="flex items-center justify-between gap-3 border-t border-line px-5 py-2.5">
            <p class="text-2xs text-faint">Saved in this browser only. Changes apply as you pick.</p>
            <button
              type="button"
              class="text-xs font-medium text-brass-strong underline-offset-2 hover:underline disabled:cursor-default disabled:text-faint disabled:no-underline"
              disabled={isDefault(appearance())}
              onClick={resetAppearance}
            >
              Reset to defaults
            </button>
          </div>
        </div>
      </div>
    </Show>
  )
}
