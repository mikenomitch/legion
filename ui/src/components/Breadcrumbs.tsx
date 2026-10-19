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
  services: 'Services',
  network: 'Network',
  volumes: 'Volumes',
  raft: 'Raft',
  events: 'Events',
  kv: 'KV Store',
  access: 'Access',
  api: 'API Explorer',
  'state-explorer': 'State Explorer',
  observability: 'Observability',
  testing: 'Testing',
  namespaces: 'Namespaces',
  oidc: 'Single sign-on',
  deployments: 'Rollouts',
}

// Path segments with no route of their own and no better target: the
// crumb is a label rather than a link to a 404 (see crumbHref).
const unlinkedSegments = new Set(['oidc'])

// Collections inside a job that are sections of the job page rather than
// pages of their own: their crumb goes back to the job.
const JOB_SECTIONS = new Map([
  ['tasks', 'Tasks'],
  ['specs', 'Specs'],
  ['instances', 'Instances'],
])

// Sections with detail pages but no index page: `/allocations/7` is a page,
// `/allocations` is not, so the section crumb is a label, not a link, and the
// bare path stays unknown.
const DETAIL_ONLY_SECTIONS = new Map([['allocations', 'Allocations']])

/** True when the first path segment is a section the app has a page for. */
export const isKnownSection = (segment: string | undefined): boolean =>
  segment === undefined || Object.prototype.hasOwnProperty.call(sectionLabels, segment)

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
  if (index === 0 && DETAIL_ONLY_SECTIONS.has(current)) return DETAIL_ONLY_SECTIONS.get(current)!

  if (previous === 'jobs') return decoded
  if (previous === 'templates') return decoded
  if (previous === 'nodes') return decoded
  if (previous === 'manager') return decoded
  if (previous === 'rollouts') return decoded
  if (previous === 'services') return decoded
  if (previous === 'volumes') return decoded
  if (previous === 'specs') return decoded
  if (previous === 'tasks') return decoded
  if (previous === 'instances') return decoded
  if (previous === 'allocations') return `#${decoded}`

  // `/jobs/<namespace>/<name>/<section>/<item>`: the namespace, the job,
  // then the job's sections.
  if (segments[0] === 'jobs' && index === 3 && JOB_SECTIONS.has(current)) return JOB_SECTIONS.get(current)!
  if (segments[0] === 'jobs' && index === 2) return decoded
  if (sectionLabels[current]) return sectionLabels[current]

  return titleCase(decoded)
}

// Where an intermediate crumb leads: its own path, the namespace's job
// list for a job's namespace segment, the job page for a job's tasks,
// specs, or instances (`/jobs/default/web/instances` has no page), and
// nowhere for a section without an index (`/allocations`).
const crumbHref = (segments: readonly string[], index: number, path: string): string | undefined => {
  if (unlinkedSegments.has(segments[index])) return undefined
  if (index === 0 && DETAIL_ONLY_SECTIONS.has(segments[0])) return undefined
  if (segments[0] === 'jobs' && index === 1) return `/jobs?ns=${segments[1]}`
  if (segments[0] === 'jobs' && index === 3 && JOB_SECTIONS.has(segments[3])) return `/jobs/${segments[1]}/${segments[2]}`
  return path
}

export const buildCrumbs = (pathname: string): Breadcrumb[] => {
  const normalized = pathname.replace(/\/+$/, '') || '/'
  const crumbs: Breadcrumb[] = [{ label: 'Legion', href: '/' }]
  if (normalized === '/') return crumbs

  const segments = normalized.split('/').filter(Boolean)
  const detailOnly = DETAIL_ONLY_SECTIONS.has(segments[0]) && segments.length > 1
  if (!isKnownSection(segments[0]) && !detailOnly) return [...crumbs, { label: 'Not found' }]

  let href = ''
  segments.forEach((segment, index) => {
    href += `/${segment}`
    crumbs.push({
      label: labelForSegment(segments, index),
      href: index === segments.length - 1 ? undefined : crumbHref(segments, index, href),
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
