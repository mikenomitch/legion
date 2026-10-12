import { For, Show, createEffect } from 'solid-js'
import { PALETTES, TWEAKS, type Palette } from '../appearance/palettes'
import {
  appearance,
  isDefault,
  resetAppearance,
  resolvedMode,
  setMode,
  setPalette,
  setTweak,
  type AppearanceMode,
} from '../appearance/store'
import { Icon, cx } from './ui'

/*
 * The appearance picker: palette, light/dark/system, and the tweaks. Every
 * change applies at once and is saved in this browser only (localStorage),
 * so there is no Save button and nothing reaches the manager.
 */

const MODES: Array<{ value: AppearanceMode; label: string }> = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'system', label: 'System' },
]

// One segmented control; the choices are radio buttons for assistive tech.
function Segmented(props: {
  label: string
  options: Array<{ value: string; label: string }>
  value: string
  onChange: (value: string) => void
}) {
  return (
    <div role="radiogroup" aria-label={props.label} class="inline-flex flex-wrap border border-line-strong" data-ui="segmented">
      <For each={props.options}>
        {(option) => (
          <button
            type="button"
            role="radio"
            aria-checked={props.value === option.value}
            class={cx(
              'px-3 py-1.5 text-xs font-medium transition-colors',
              props.value === option.value ? 'bg-ink text-canvas' : 'text-muted hover:bg-surface2 hover:text-ink',
            )}
            onClick={() => props.onChange(option.value)}
          >
            {option.label}
          </button>
        )}
      </For>
    </div>
  )
}

// A palette as it looks in the current mode: page, rule, ink, signal.
function Swatch(props: { palette: Palette }) {
  const spec = () => props.palette[resolvedMode()]
  return (
    <span
      aria-hidden="true"
      class="relative block h-10 w-full shrink-0 border border-line"
      style={{ background: spec().colors.canvas }}
    >
      <span class="absolute left-2 top-2 block h-3 w-3" style={{ background: spec().signal }} />
      <span class="absolute bottom-2 left-2 right-8 block h-[3px]" style={{ background: spec().colors.ink }} />
      <span class="absolute bottom-2 right-2 block h-3 w-4" style={{ background: spec().colors.accent }} />
    </span>
  )
}

export function AppearancePanel(props: { open: boolean; onClose: () => void }) {
  let closeButton: HTMLButtonElement | undefined
  createEffect(() => {
    if (props.open) requestAnimationFrame(() => closeButton?.focus())
  })

  const tweakValue = (axis: (typeof TWEAKS)[number]['axis']) =>
    appearance().tweaks[axis] ?? TWEAKS.find((tweak) => tweak.axis === axis)!.options[0].value

  return (
    <Show when={props.open}>
      <div
        class="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/50 p-4 pt-[8vh] backdrop-blur-[2px]"
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
              <Segmented label="Mode" options={MODES} value={appearance().mode} onChange={(value) => setMode(value as AppearanceMode)} />
            </section>

            <section>
              <p class="eyebrow mb-2">Palette</p>
              <div role="radiogroup" aria-label="Palette" class="grid grid-cols-2 gap-2 sm:grid-cols-4">
                <For each={PALETTES}>
                  {(palette) => (
                    <button
                      type="button"
                      role="radio"
                      aria-checked={appearance().palette === palette.id}
                      title={palette.description}
                      class={cx(
                        'flex flex-col gap-1.5 border p-1.5 text-left text-xs font-medium transition-colors',
                        appearance().palette === palette.id
                          ? 'border-ink text-ink outline outline-2 outline-offset-0 outline-ink'
                          : 'border-line text-muted hover:border-line-strong hover:text-ink',
                      )}
                      onClick={() => setPalette(palette.id)}
                    >
                      <Swatch palette={palette} />
                      {palette.name}
                    </button>
                  )}
                </For>
              </div>
              <p class="mt-2 text-xs text-muted">{PALETTES.find((palette) => palette.id === appearance().palette)?.description}</p>
            </section>

            <section class="space-y-4">
              <p class="eyebrow">Layout and type</p>
              <For each={TWEAKS}>
                {(tweak) => (
                  <div class="grid gap-1.5 sm:grid-cols-[8rem_1fr] sm:items-start sm:gap-4">
                    <span class="pt-1.5 text-sm font-medium text-ink">{tweak.label}</span>
                    <div class="min-w-0">
                      <Segmented
                        label={tweak.label}
                        options={tweak.options}
                        value={tweakValue(tweak.axis)}
                        onChange={(value) => setTweak(tweak.axis, value)}
                      />
                      <p class="mt-1 text-xs text-faint">
                        {tweak.options.find((option) => option.value === tweakValue(tweak.axis))?.description}
                      </p>
                    </div>
                  </div>
                )}
              </For>
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
