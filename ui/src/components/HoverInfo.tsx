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
          class={`pointer-events-none absolute bottom-full left-1/2 z-[9999] mb-2 hidden w-[26rem] max-w-[calc(100vw-2rem)] -translate-x-1/2 rounded-md border border-line bg-surface/95 px-3 py-2 text-left text-xs leading-relaxed text-ink shadow-pop backdrop-blur group-hover/hover-info:block group-focus-within/hover-info:block ${props.tooltipClass ?? ''}`.trim()}
        >
          <span class="block whitespace-pre-wrap">{content()}</span>
        </span>
      </span>
    </Show>
  )
}
