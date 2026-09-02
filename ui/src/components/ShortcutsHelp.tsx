import { For, Show, createEffect } from 'solid-js'
import { JUMP_MODIFIER_LABEL } from '../utils/jumpHints'
import { Icon, Kbd } from './ui'

/*
 * The keyboard cheat sheet, opened with `?` (the GitHub / Linear / Gmail
 * convention). Every binding below is a real one from App.tsx or
 * SharedIndex.tsx; keep the two in step when adding a key.
 */

type Shortcut = { keys: string[]; label: string; note?: string }
type ShortcutGroup = { heading: string; items: Shortcut[] }

const mod = JUMP_MODIFIER_LABEL

const GROUPS: ShortcutGroup[] = [
  {
    heading: 'Go anywhere',
    items: [
      { keys: [`${mod}K`], label: 'Search pages, jobs, workers, and recent pages' },
      { keys: [`hold ${mod}`], label: 'Reveal jump hints on the index rows and the sidebar' },
      { keys: [`${mod}1`, '…', `${mod}0`], label: 'Open the 1st … 10th row of the index' },
      { keys: [`${mod}⇧`, 'letter'], label: 'Open the labelled sidebar item' },
      { keys: ['u'], label: 'Go up one level', note: 'a task page goes to its job, a job to the list' },
      { keys: ['?'], label: 'This cheat sheet' },
    ],
  },
  {
    heading: 'On an index',
    items: [
      { keys: ['j', '↓'], label: 'Select the next row' },
      { keys: ['k', '↑'], label: 'Select the previous row' },
      { keys: ['o', '↵'], label: 'Open the selected row' },
    ],
  },
  {
    heading: 'In editors',
    items: [
      { keys: [`${mod}↵`], label: 'Run the query in the State Explorer' },
      { keys: ['↵'], label: 'Launch the command in an exec panel' },
      { keys: ['esc'], label: 'Close a dialog, or disarm a confirm button' },
    ],
  },
]

export function ShortcutsHelp(props: { open: boolean; onClose: () => void }) {
  let closeButton: HTMLButtonElement | undefined
  createEffect(() => {
    if (props.open) requestAnimationFrame(() => closeButton?.focus())
  })

  return (
    <Show when={props.open}>
      <div
        class="fixed inset-0 z-50 flex items-start justify-center bg-black/50 p-4 pt-[10vh] backdrop-blur-[2px]"
        data-modal
        onClick={props.onClose}
      >
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="shortcuts-title"
          class="w-full max-w-2xl overflow-hidden rounded-xl border border-line bg-surface shadow-pop"
          onClick={(event) => event.stopPropagation()}
        >
          <div class="flex items-center justify-between gap-3 border-b border-line px-5 py-3">
            <h2 id="shortcuts-title" class="flex items-center gap-2 font-display text-base font-semibold text-ink">
              <Icon name="keyboard" size={17} class="text-brass" />
              Keyboard shortcuts
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
          <div class="grid gap-x-8 gap-y-5 px-5 py-4 sm:grid-cols-2">
            <For each={GROUPS}>
              {(group) => (
                <section>
                  <p class="eyebrow mb-2">{group.heading}</p>
                  <ul class="space-y-1.5">
                    <For each={group.items}>
                      {(shortcut) => (
                        <li class="flex items-start justify-between gap-4 text-sm">
                          <span class="min-w-0 text-ink">
                            {shortcut.label}
                            <Show when={shortcut.note}>
                              <span class="block text-xs text-faint">{shortcut.note}</span>
                            </Show>
                          </span>
                          <span class="flex shrink-0 items-center gap-1">
                            <For each={shortcut.keys}>
                              {(key) => (key === '…' ? <span class="text-faint">…</span> : <Kbd>{key}</Kbd>)}
                            </For>
                          </span>
                        </li>
                      )}
                    </For>
                  </ul>
                </section>
              )}
            </For>
          </div>
          <p class="border-t border-line px-5 py-2.5 text-2xs text-faint">
            Single-key shortcuts stay out of your way while you type in a field or have a link focused.
          </p>
        </div>
      </div>
    </Show>
  )
}
