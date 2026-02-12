import { Show, type JSX } from 'solid-js'

type DetailSectionProps = {
  title?: string
  actions?: JSX.Element
  class?: string
  children: JSX.Element
}

const sectionBaseClass = 'rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60'

export function DetailSection(props: DetailSectionProps) {
  return (
    <section class={`${sectionBaseClass} ${props.class ?? ''}`.trim()}>
      <Show when={props.title || props.actions}>
        <div class="mb-3 flex flex-wrap items-center justify-between gap-2">
          <Show when={props.title}>
            <h3 class="font-semibold">{props.title}</h3>
          </Show>
          {props.actions}
        </div>
      </Show>
      {props.children}
    </section>
  )
}

type DetailBadgeTone = 'neutral' | 'success' | 'warning' | 'danger'

const badgeToneClasses: Record<DetailBadgeTone, string> = {
  neutral: 'border-neutral-300 text-neutral-700 dark:border-neutral-700 dark:text-neutral-200',
  success: 'border-emerald-400 text-emerald-700 dark:border-emerald-700 dark:text-emerald-300',
  warning: 'border-amber-400 text-amber-700 dark:border-amber-700 dark:text-amber-300',
  danger: 'border-rose-400 text-rose-700 dark:border-rose-700 dark:text-rose-300',
}

type DetailBadgeProps = {
  tone?: DetailBadgeTone
  children: JSX.Element
}

export function DetailBadge(props: DetailBadgeProps) {
  return (
    <span class={`rounded border px-2 py-0.5 text-[11px] uppercase tracking-wide ${badgeToneClasses[props.tone ?? 'neutral']}`}>
      {props.children}
    </span>
  )
}

export const detailPrimaryButtonClass = 'rounded border px-3 py-2 text-sm font-semibold'
export const detailDangerButtonClass = 'rounded border border-rose-300 px-3 py-2 text-sm font-semibold text-rose-700 dark:border-rose-700 dark:text-rose-300'

export const detailTabButtonClass = (active: boolean) =>
  `rounded border px-3 py-1.5 text-sm ${
    active
      ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-100 dark:bg-neutral-100 dark:text-neutral-900'
      : 'border-neutral-300'
  }`
