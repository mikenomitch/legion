import { For, Show, createEffect } from 'solid-js'
import { THEMES } from '../appearance/themes'
import { appearance, isDefault, resetAppearance, setMode, setTheme, type AppearanceMode } from '../appearance/store'
import { Icon, cx } from './ui'

/*
 * The appearance picker: light/dark/system and the color theme. Every change
 * applies at once and is saved in this browser only (localStorage), so there
 * is no Save button and nothing reaches the manager.
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

export function AppearancePanel(props: { open: boolean; onClose: () => void }) {
  let closeButton: HTMLButtonElement | undefined
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
          class="w-full max-w-xl overflow-hidden rounded-xl border border-line bg-surface shadow-pop"
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
