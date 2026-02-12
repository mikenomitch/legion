import { Show, type JSX } from 'solid-js'

type HoverInfoProps = {
  content?: string
  children: JSX.Element
  class?: string
  tooltipClass?: string
}

const hasText = (value?: string) => Boolean(value && value.trim().length > 0)

export default function HoverInfo(props: HoverInfoProps) {
  const content = () => props.content?.trim() ?? ''

  return (
    <Show when={hasText(content())} fallback={props.children}>
      <span class={`group/hover-info relative inline-flex ${props.class ?? ''}`.trim()} tabindex={0}>
        {props.children}
        <span
          class={`pointer-events-none absolute bottom-full left-1/2 z-30 mb-2 hidden w-[26rem] max-w-[calc(100vw-2rem)] -translate-x-1/2 rounded border border-neutral-300 bg-white/95 px-3 py-2 text-left text-xs leading-relaxed text-neutral-800 shadow-[4px_4px_0_#d4d4d8] group-hover/hover-info:block group-focus-within/hover-info:block dark:border-neutral-700 dark:bg-neutral-900/95 dark:text-neutral-100 dark:shadow-[4px_4px_0_#0b0d12] ${props.tooltipClass ?? ''}`.trim()}
        >
          <span class="block whitespace-pre-wrap">{content()}</span>
        </span>
      </span>
    </Show>
  )
}
