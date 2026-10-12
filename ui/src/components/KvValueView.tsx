import { Show, createSignal, onCleanup, type JSX } from 'solid-js'
import { copyFetchedText } from '../utils/clipboard'
import { Button, cx } from './ui'

/*
 * Pieces of the KV page's masked value (pages/KeyValue.tsx): a value is
 * hidden until someone asks for it, shown read-only once revealed, and can be
 * copied without being shown at all.
 */

// Lucide "eye-off"; the kit's icon set has only `eye`.
export function EyeOffIcon(props: { size?: number; class?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={props.size ?? 13}
      height={props.size ?? 13}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width={1.75}
      stroke-linecap="round"
      stroke-linejoin="round"
      class={cx('shrink-0', props.class)}
      aria-hidden="true"
    >
      <path d="M10.73 5.08A10.4 10.4 0 0 1 12 5c6.5 0 10 7 10 7a13.2 13.2 0 0 1-1.67 2.68 M6.61 6.61A13.5 13.5 0 0 0 2 12s3.5 7 10 7a9.7 9.7 0 0 0 5.39-1.61 M14.12 14.12a3 3 0 1 1-4.24-4.24 M2 2l20 20" />
    </svg>
  )
}

/** The value's place-holder while it is hidden. Holds no part of the value. */
export function HiddenValue(props: { children?: JSX.Element; class?: string }) {
  return (
    <div
      class={cx(
        'flex min-h-[88px] flex-col items-center justify-center gap-1 rounded-md border border-dashed border-line bg-surface2/40 px-3 py-4 text-center',
        props.class,
      )}
    >
      <p class="flex items-center gap-2 text-faint">
        <EyeOffIcon size={14} />
        <span class="select-none font-mono text-base leading-none tracking-[0.2em]" aria-hidden="true">
          ••••••••
        </span>
        <span class="text-xs font-medium uppercase tracking-wider">hidden</span>
      </p>
      {props.children}
    </div>
  )
}

/** A revealed value, read-only. */
export function RevealedValue(props: { value: string; class?: string }) {
  return (
    <Show
      when={props.value !== ''}
      fallback={
        <div class={cx('rounded-md border border-line bg-surface2/70 px-3 py-3 text-xs italic text-faint', props.class)}>
          (empty string)
        </div>
      }
    >
      <pre
        class={cx(
          'max-h-80 min-h-[88px] overflow-auto whitespace-pre-wrap break-all rounded-md border border-line bg-surface2/70 p-3 font-mono text-xs leading-relaxed text-ink',
          props.class,
        )}
      >
        {props.value}
      </pre>
    </Show>
  )
}

/**
 * Fetches a value and puts it on the clipboard without rendering it: the
 * plaintext lives only inside the copy call. Errors go to `onError`.
 */
export function FetchCopyButton(props: {
  fetchText: () => Promise<string>
  onError: (err: unknown) => void
  label?: string
  disabled?: boolean
  title?: string
}) {
  const [state, setState] = createSignal<'idle' | 'copying' | 'copied'>('idle')
  let timer: ReturnType<typeof setTimeout> | undefined
  onCleanup(() => {
    if (timer) clearTimeout(timer)
  })
  const copy = async () => {
    setState('copying')
    try {
      await copyFetchedText(props.fetchText)
      setState('copied')
      timer = setTimeout(() => setState('idle'), 1500)
    } catch (err) {
      setState('idle')
      props.onError(err)
    }
  }
  return (
    <Button
      size="sm"
      icon={state() === 'copied' ? 'check' : 'copy'}
      disabled={props.disabled || state() === 'copying'}
      title={props.title}
      onClick={() => void copy()}
    >
      {state() === 'copied' ? 'Copied' : state() === 'copying' ? 'Copying…' : (props.label ?? 'Copy value')}
    </Button>
  )
}
