import { A, useLocation } from '@solidjs/router'
import { For, Show, createMemo } from 'solid-js'

export type Breadcrumb = {
  label: string
  href?: string
}

const sectionLabels: Record<string, string> = {
  nodes: 'Workers',
  manager: 'Managers',
  jobs: 'Jobs',
  deployments: 'Deployments',
  rollouts: 'Rollouts',
  kv: 'Key/Value',
  api: 'API',
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
  if (previous === 'nodes') return decoded
  if (previous === 'manager') return decoded
  if (previous === 'rollouts') return decoded
  if (previous === 'deployments') return decoded
  if (previous === 'specs') return decoded

  if (current === 'specs') return 'Specs'
  if (sectionLabels[current]) return sectionLabels[current]

  return titleCase(decoded)
}

export const buildCrumbs = (pathname: string): Breadcrumb[] => {
  const normalized = pathname.replace(/\/+$/, '') || '/'
  if (normalized === '/') return []

  const segments = normalized.split('/').filter(Boolean)
  const crumbs: Breadcrumb[] = []

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
      <nav aria-label="Breadcrumb" class="mb-4">
        <ol class="flex flex-wrap items-center gap-2 text-xs text-neutral-500 dark:text-neutral-400">
          <For each={crumbs()}>
            {(crumb, index) => (
              <>
                <Show when={index() > 0}>
                  <li aria-hidden="true" class="text-neutral-400 dark:text-neutral-600">/</li>
                </Show>
                <li>
                  <Show
                    when={crumb.href}
                    fallback={<span class="font-medium text-neutral-700 dark:text-neutral-200">{crumb.label}</span>}
                  >
                    {(href) => (
                      <A
                        href={href()}
                        class="rounded px-1 py-0.5 text-neutral-600 underline decoration-dotted underline-offset-4 hover:text-neutral-900 dark:text-neutral-300 dark:hover:text-neutral-100"
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
