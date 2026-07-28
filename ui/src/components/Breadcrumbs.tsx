import { A, useLocation } from '@solidjs/router'
import { For, Show, createMemo } from 'solid-js'
import { Icon } from './ui'

export type Breadcrumb = {
  label: string
  href?: string
}

const sectionLabels: Record<string, string> = {
  nodes: 'Workers',
  manager: 'Managers',
  jobs: 'Jobs',
  templates: 'Templates',
  rollouts: 'Rollouts',
  raft: 'Raft',
  events: 'Events',
  kv: 'KV Store',
  access: 'Access',
  api: 'API Explorer',
  'state-explorer': 'State Explorer',
  observability: 'Observability',
  testing: 'Testing',
}

const decodeSegment = (value: string) => {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

const titleCase = (value: string) =>
  value
    .replace(/[-_]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())

const labelForSegment = (segments: readonly string[], index: number): string => {
  const current = segments[index]
  const previous = index > 0 ? segments[index - 1] : undefined
  const decoded = decodeSegment(current)

  if (index === 0 && sectionLabels[current]) return sectionLabels[current]

  if (previous === 'jobs') return decoded
  if (previous === 'templates') return decoded
  if (previous === 'nodes') return decoded
  if (previous === 'manager') return decoded
  if (previous === 'rollouts') return decoded
  if (previous === 'specs') return decoded
  if (previous === 'tasks') return decoded

  if (current === 'specs') return 'Specs'
  if (current === 'tasks') return 'Tasks'
  if (sectionLabels[current]) return sectionLabels[current]

  return titleCase(decoded)
}

export const buildCrumbs = (pathname: string): Breadcrumb[] => {
  const normalized = pathname.replace(/\/+$/, '') || '/'
  const crumbs: Breadcrumb[] = [{ label: 'Legion', href: '/' }]
  if (normalized === '/') return crumbs

  const segments = normalized.split('/').filter(Boolean)

  let href = ''
  segments.forEach((segment, index) => {
    href += `/${segment}`
    crumbs.push({
      label: labelForSegment(segments, index),
      href: index === segments.length - 1 ? undefined : href,
    })
  })

  return crumbs
}

export default function Breadcrumbs() {
  const location = useLocation()
  const crumbs = createMemo(() => buildCrumbs(location.pathname))

  return (
    <Show when={crumbs().length > 0}>
      <nav aria-label="Breadcrumb" class="min-w-0">
        <ol class="flex items-center gap-1 overflow-hidden text-xs text-faint">
          <For each={crumbs()}>
            {(crumb, index) => (
              <>
                <Show when={index() > 0}>
                  <li aria-hidden="true" class="shrink-0">
                    <Icon name="chevronRight" size={12} class="text-faint/70" />
                  </li>
                </Show>
                <li class="min-w-0">
                  <Show
                    when={crumb.href}
                    fallback={<span class="block max-w-[16rem] truncate font-medium text-ink">{crumb.label}</span>}
                  >
                    {(href) => (
                      <A
                        href={href()}
                        class="block max-w-[12rem] truncate rounded px-0.5 py-0.5 text-muted transition-colors hover:text-ink"
                      >
                        {crumb.label}
                      </A>
                    )}
                  </Show>
                </li>
              </>
            )}
          </For>
        </ol>
      </nav>
    </Show>
  )
}
