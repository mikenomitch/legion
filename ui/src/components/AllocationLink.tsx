import { A } from '@solidjs/router'
import type { JSX } from 'solid-js'
import { cx } from './ui'

/*
 * The one way to point at an allocation: `#1042` in mono, linking to its page
 * at /allocations/:id. Every table that lists allocations uses it, so an id
 * reads (and clicks) the same everywhere.
 */

/**
 * The allocation page's URL. `job` is an optional hint the page uses when the
 * reader's token cannot read placements (no `node:read`): with the job known,
 * it can still find the allocation through the job's own rollouts. Pass it
 * whenever the caller knows the job; the page ignores it when the ledger
 * answers.
 */
export const allocationHref = (id: bigint | number | string, job?: string): string =>
  `/allocations/${encodeURIComponent(String(id))}${job ? `?job=${encodeURIComponent(job)}` : ''}`

const VARIANTS = {
  // Table cells and headings: the id is the row's identity.
  strong: 'font-semibold text-ink hover:text-brass-strong',
  // Secondary mentions (an event's subject, a "now served by" note).
  muted: 'text-muted hover:text-brass-strong',
  // The caller's class sets the color (e.g. inside the dark log pane).
  inherit: '',
} as const

export type AllocationLinkProps = {
  id: bigint | number | string
  /** The owning job, when known; see `allocationHref`. */
  job?: string
  variant?: keyof typeof VARIANTS
  class?: string
  title?: string
  /** Replaces the default `#<id>` label. */
  children?: JSX.Element
  onClick?: (event: MouseEvent) => void
}

export function AllocationLink(props: AllocationLinkProps) {
  return (
    <A
      href={allocationHref(props.id, props.job)}
      class={cx('font-mono text-xs underline-offset-2 hover:underline', VARIANTS[props.variant ?? 'strong'], props.class)}
      title={props.title ?? `Allocation ${String(props.id)}`}
      onClick={(event: MouseEvent) => props.onClick?.(event)}
    >
      {props.children ?? `#${String(props.id)}`}
    </A>
  )
}

export default AllocationLink
