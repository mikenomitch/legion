import { A, Route, Router, useLocation, useNavigate, type RouteSectionProps } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import type { LedgerNode, Rollout } from './gen/legion_pb'
import Breadcrumbs from './components/Breadcrumbs'
import { AclBanner } from './components/AclBanner'
import { managerClient, managerEndpoint } from './grpcClient'
import { commandPaletteIndexItems } from './utils/commandPalette'
import Access from './pages/Access'
import OidcCallback from './pages/OidcCallback'
import Deployments from './pages/Deployments'
import JobDetail from './pages/details/JobDetail'
import JobSpecDetail from './pages/details/JobSpecDetail'
import ManagerNodeDetail from './pages/details/ManagerNodeDetail'
import NodeDetail from './pages/details/NodeDetail'
import RolloutDetail from './pages/details/RolloutDetail'
import TaskDetail from './pages/details/TaskDetail'
import Homepage from './pages/Homepage'
import Jobs from './pages/Jobs'
import KeyValue from './pages/KeyValue'
import Manager from './pages/Manager'
import Nodes from './pages/Nodes'
import Rollouts from './pages/Rollouts'
import ApiExplorer from './pages/ApiExplorer'
import Observability from './pages/Observability'
import TestingUtils from './pages/TestingUtils'
import StateExplorer from './pages/StateExplorer'
import Raft from './pages/Raft'

type SidebarItem = {
  kind: 'item'
  label: string
  path: string
  title: string
  copy: string
}

type SidebarSection = {
  kind: 'section'
  heading: string
  items: SidebarItem[]
}

type CommandPaletteItem = {
  id: string
  label: string
  description: string
  searchText: string
  href: string
  group: string
}

type SidebarNode = SidebarItem | SidebarSection

const baseNavItems: SidebarItem[] = [
  { kind: 'item', label: 'Overview', path: '/', title: 'Overview', copy: 'Jobs running on the cluster.' },
]

const detailRoutes = new Set(['jobs', 'nodes', 'manager', 'rollouts', 'deployments'])

const decodePathSegment = (value: string) => {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

const resolveInitialTheme = (): 'light' | 'dark' => {
  if (typeof window === 'undefined') return 'dark'
  const stored = localStorage.getItem('ui-theme')
  if (stored === 'light' || stored === 'dark') return stored
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

const Layout = (props: RouteSectionProps) => {
  const location = useLocation()
  const navigate = useNavigate()
  const [isNavOpen, setIsNavOpen] = createSignal(false)
  const [theme, setTheme] = createSignal<'light' | 'dark'>(resolveInitialTheme())
  const [o11yStatus] = createResource(async () => managerClient.getObservability({ maxLines: 1 }))
  const [commandPaletteOpen, setCommandPaletteOpen] = createSignal(false)
  const [commandPaletteQuery, setCommandPaletteQuery] = createSignal('')
  const [commandPaletteSelected, setCommandPaletteSelected] = createSignal(0)
  let commandPaletteInput: HTMLInputElement | undefined
  const isActiveNavPath = (path: string) =>
    path === '/' ? location.pathname === '/' : location.pathname === path || location.pathname.startsWith(`${path}/`)

  const navItems = createMemo(() => {
    const base: SidebarNode[] = [
      ...baseNavItems,
      {
        kind: 'section',
        heading: 'Workloads',
        items: [
          { kind: 'item', label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Jobs and their health status.' },
          { kind: 'item', label: 'Rollouts', path: '/rollouts', title: 'Rollouts', copy: 'Rollout execution details.' },
        ],
      },
      {
        kind: 'section',
        heading: 'Cluster',
        items: [
          { kind: 'item', label: 'Worker Nodes', path: '/nodes', title: 'Worker Nodes', copy: 'Inventory of worker nodes.' },
          { kind: 'item', label: 'Manager Nodes', path: '/manager', title: 'Manager Nodes', copy: 'Manager config snapshots.' },
          { kind: 'item', label: 'Raft', path: '/raft', title: 'Raft', copy: 'Raft elections, failover, and history.' },
          { kind: 'item', label: 'KV', path: '/kv', title: 'KV', copy: 'Browse and edit key/value entries.' },
          { kind: 'item', label: 'Access', path: '/access', title: 'Access', copy: 'ACL status, tokens, and login.' },
        ],
      },
      {
        kind: 'section',
        heading: 'Development',
        items: [
          { kind: 'item', label: 'API', path: '/api', title: 'API', copy: 'Interactive gRPC explorer.' },
          { kind: 'item', label: 'State Explorer', path: '/state-explorer', title: 'State Explorer', copy: 'Explore and query manager state SQLite data.' },
        ],
      },
    ]

    if (o11yStatus()?.o11yEnabled) {
      const developmentIndex = base.findIndex((item) => item.kind === 'section' && item.heading === 'Development')
      const observabilityItem: SidebarItem = {
        kind: 'item',
        label: 'Observability',
        path: '/observability',
        title: 'Observability',
        copy: 'Manager logs and operational telemetry.',
      }

      if (developmentIndex >= 0) {
        ;(base[developmentIndex] as SidebarSection).items.push(observabilityItem)
      } else {
        base.push(observabilityItem)
      }
    }

    return base
  })

  const flattenedNavItems = createMemo(() =>
    navItems().flatMap((entry) => (entry.kind === 'item' ? [entry] : entry.items)),
  )

  const commandPaletteEntries = createMemo(() => {
    const query = commandPaletteQuery().trim().toLowerCase()
    const deduped = new Map<string, CommandPaletteItem>()
    const entries: CommandPaletteItem[] = [
      ...flattenedNavItems().map((entry) => ({
        id: `nav:${entry.path}`,
        label: entry.label,
        description: entry.copy,
        searchText: entry.label.toLowerCase(),
        href: entry.path,
        group: 'Navigation',
      })),
      ...commandPaletteIndexItems().map((entry) => ({
        id: `index:${entry.id}`,
        label: entry.label,
        description: [entry.subtitle, entry.source].filter(Boolean).join(' · '),
        searchText: `${entry.label} ${entry.source}`.toLowerCase(),
        href: entry.href,
        group: entry.source,
      })),
    ]

    for (const entry of entries) {
      deduped.set(`${entry.group}:${entry.href}`, entry)
    }

    if (!query) {
      return Array.from(deduped.values())
    }

    const filtered = new Map<string, CommandPaletteItem>()
    for (const entry of deduped.values()) {
      if (entry.searchText.includes(query) && !filtered.has(`${entry.group}:${entry.href}`)) {
        filtered.set(`${entry.group}:${entry.href}`, entry)
      }
    }
    return Array.from(filtered.values())
  })

  const totalCommandPaletteEntries = createMemo(() => commandPaletteEntries().length)
  const commandPaletteInstruction = () => (totalCommandPaletteEntries() === 0 ? 'No matches' : `Navigate to ${totalCommandPaletteEntries()} destinations`)

  const openCommandPalette = () => {
    setCommandPaletteQuery('')
    setCommandPaletteOpen(true)
    setCommandPaletteSelected(0)
    requestAnimationFrame(() => commandPaletteInput?.focus())
  }

  const closeCommandPalette = () => {
    setCommandPaletteOpen(false)
  }

  const executeCommand = (href: string) => {
    closeCommandPalette()
    navigate(href)
  }

  const handleCommandPaletteNavigation = (event: KeyboardEvent) => {
    const key = event.key.toLowerCase()

    if (key === 'escape') {
      event.preventDefault()
      closeCommandPalette()
      return true
    }
    if (key === 'j' || event.key === 'ArrowDown') {
      event.preventDefault()
      setCommandPaletteSelected((value) => {
        const total = totalCommandPaletteEntries()
        return total === 0 ? 0 : Math.min(value + 1, total - 1)
      })
      return true
    }
    if (key === 'k' || event.key === 'ArrowUp') {
      event.preventDefault()
      setCommandPaletteSelected((value) => {
        const total = totalCommandPaletteEntries()
        return total === 0 ? 0 : Math.max(value - 1, 0)
      })
      return true
    }
    if (key === 'o' || key === 'enter' || event.key === 'Enter') {
      event.preventDefault()
      const entry = commandPaletteEntries()[commandPaletteSelected()]
      if (entry) {
        executeCommand(entry.href)
      }
      return true
    }
    if (key === 'u') {
      event.preventDefault()
      const normalized = location.pathname.replace(/\/+$/, '') || '/'
      const segments = normalized.split('/').filter(Boolean)
      if (segments.length > 1) {
        segments.pop()
        executeCommand(`/${segments.join('/')}`)
      }
      return true
    }
    return false
  }

  const navigateUpOneLevel = () => {
    const normalized = location.pathname.replace(/\/+$/, '') || '/'
    const segments = normalized.split('/').filter(Boolean)
    if (segments.length <= 1) {
      if (location.pathname !== '/') {
        executeCommand('/')
      }
      return
    }

    segments.pop()
    executeCommand(`/${segments.join('/')}`)
  }

  const isPaletteSearchTarget = (event: Event): boolean => {
    const target = event.target as HTMLElement | null
    if (!target || !target.closest) return false

    if (!target.closest('[data-command-palette]')) return false

    return target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.tagName === 'SELECT' || target.isContentEditable
  }

  const isPaletteVimCommandKey = (key: string) => key === 'j' || key === 'k' || key === 'o' || key === 'u'
  const isEditableTarget = (event: Event): boolean => {
    const target = event.target as HTMLElement | null
    if (!target || !target.closest) return false
    return target.closest('input, textarea, select, [contenteditable="true"]') !== null
  }

  const onGlobalCommandPaletteKeydown = (event: KeyboardEvent) => {
    if (commandPaletteOpen()) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        closeCommandPalette()
        return
      }

      if (isPaletteSearchTarget(event)) return

      if (handleCommandPaletteNavigation(event)) return
      return
    }

    if ((event.key.toLowerCase() === 'u') && !event.metaKey && !event.ctrlKey && !event.altKey && !isEditableTarget(event)) {
      event.preventDefault()
      navigateUpOneLevel()
      return
    }

    if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== 'k') return

    event.preventDefault()
    openCommandPalette()
  }

  const onCommandPaletteKeydown = (event: KeyboardEvent) => {
    if (isPaletteSearchTarget(event) && isPaletteVimCommandKey(event.key.toLowerCase())) {
      return
    }

    if (event.key === 'Escape') {
      handleCommandPaletteNavigation(event)
      return
    }
    handleCommandPaletteNavigation(event)
  }

  onMount(() => {
    window.addEventListener('keydown', onGlobalCommandPaletteKeydown)
    onCleanup(() => {
      window.removeEventListener('keydown', onGlobalCommandPaletteKeydown)
    })
  })

  createEffect(() => {
    if (!commandPaletteOpen()) return
    setCommandPaletteSelected(0)
  })

  createEffect(() => {
    commandPaletteQuery()
    if (!commandPaletteOpen()) return
    setCommandPaletteSelected(0)
  })

  createEffect(() => {
    if (!commandPaletteOpen()) return
    const total = totalCommandPaletteEntries()
    if (commandPaletteSelected() >= total) {
      setCommandPaletteSelected(Math.max(total - 1, 0))
    }
  })

  createEffect(() => {
    location.pathname
    setIsNavOpen(false)
    closeCommandPalette()
  })

  createEffect(() => {
    const current = theme()
    document.documentElement.classList.toggle('dark', current === 'dark')
    localStorage.setItem('ui-theme', current)
  })

  const pageMeta = createMemo(() => {
    const normalized = location.pathname.replace(/\/+$/, '') || '/'
    const segments = normalized.split('/').filter(Boolean)
    const topLevelPath = segments.length > 0 ? `/${segments[0]}` : '/'
    const visibleNavItems = navItems().flatMap((entry) => (entry.kind === 'item' ? [entry] : entry.items))
    const topLevel = visibleNavItems.find((item) => item.path === topLevelPath) ?? visibleNavItems[0]
    const title =
      segments.length <= 1
        ? topLevel.title
        : decodePathSegment(segments[segments.length - 1])

    return {
      title,
      subtitle: topLevel.copy,
    }
  })

  const isDetailRoute = () => {
    const normalized = location.pathname.replace(/\/+$/, '') || '/'
    const segments = normalized.split('/').filter(Boolean)
    return segments.length > 1 && detailRoutes.has(segments[0] ?? '')
  }

  return (
    <div class="min-h-screen bg-neutral-50 text-neutral-900 dark:bg-neutral-950 dark:text-neutral-100">
      <div class="flex min-h-screen">
        <aside class={`fixed inset-y-0 left-0 z-40 w-64 shrink-0 transform border-r border-neutral-200 bg-white/95 px-5 py-6 transition md:sticky md:top-0 md:h-screen md:translate-x-0 dark:border-neutral-800 dark:bg-neutral-950 ${isNavOpen() ? 'translate-x-0' : '-translate-x-full'}`}>
          <div class="mb-6 flex items-center justify-between">
            <A href="/" class="block text-lg font-semibold uppercase tracking-[0.18em]">Legion</A>
            <button
              type="button"
              class="rounded border px-2 py-1 text-sm md:hidden"
              onClick={() => setIsNavOpen(false)}
              aria-label="Close menu"
            >
              ✕
            </button>
          </div>
            <nav class="space-y-2">
              <For each={navItems()}>
                {(entry) => (
                  <Show
                  when={entry.kind === 'section'}
                  fallback={
                    <A
                      href={(entry as SidebarItem).path}
                      class={`block rounded border px-3 py-2 text-sm font-semibold ${isActiveNavPath((entry as SidebarItem).path) ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-200 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300'}`}
                    >
                      {(entry as SidebarItem).label}
                    </A>
                  }
                >
                  <p class="px-3 pb-1 pt-4 text-[11px] font-medium uppercase tracking-[0.2em] text-neutral-500">{(entry as SidebarSection).heading}</p>
                  <For each={(entry as SidebarSection).items}>
                    {(item) => (
                      <A
                        href={item.path}
                        class={`mb-1 block rounded border px-3 py-2 text-sm font-semibold ${isActiveNavPath(item.path) ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-200 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300'}`}
                      >
                        {item.label}
                      </A>
                    )}
                  </For>
                </Show>
              )}
            </For>
          </nav>
          <p class="mt-auto pt-6 text-xs text-neutral-500">Manager endpoint {managerEndpoint}</p>
        </aside>
          <Show when={isNavOpen()}>
          <button
            type="button"
            class="fixed inset-0 z-30 bg-black/30 backdrop-blur-[1px] md:hidden"
            onClick={() => setIsNavOpen(false)}
            aria-label="Close menu overlay"
          />
        </Show>
          <div class="relative flex min-h-screen min-w-0 flex-1 flex-col md:ml-0">
            <AclBanner />
            <button
              type="button"
              class="absolute right-4 top-4 z-20 flex h-9 w-9 items-center justify-center rounded border bg-white/90 text-base dark:border-neutral-700 dark:bg-neutral-900/80"
              onClick={() => setTheme((value) => (value === 'dark' ? 'light' : 'dark'))}
            aria-label={theme() === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
            >
              {theme() === 'dark' ? '☀' : '☾'}
            </button>
            <button
              type="button"
              class="absolute right-14 top-4 z-20 rounded border bg-white/90 px-2 py-1 text-xs dark:border-neutral-700 dark:bg-neutral-900/80"
              onClick={openCommandPalette}
              title="Open command palette"
              aria-label="Open command palette"
            >
              ⌘K
            </button>
            <Show when={commandPaletteOpen()}>
              <div
                class="fixed inset-0 z-50 flex items-start justify-center bg-black/40 p-4 backdrop-blur-[1px]"
                data-command-palette
                onClick={closeCommandPalette}
              >
                <div
                  class="w-full max-w-xl rounded border border-neutral-200 bg-white shadow-2xl dark:border-neutral-700 dark:bg-neutral-900"
                  onClick={(event) => event.stopPropagation()}
                >
                  <div class="border-b border-neutral-200 px-3 py-2 dark:border-neutral-700">
                    <input
                      ref={commandPaletteInput}
                      value={commandPaletteQuery()}
                      onInput={(event) => setCommandPaletteQuery(event.currentTarget.value)}
                      onKeyDown={onCommandPaletteKeydown}
                      placeholder="Search pages and current index items"
                      class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm outline-none dark:border-neutral-700 dark:bg-neutral-950"
                      aria-label="Search command palette"
                    />
                  </div>
                  <div class="max-h-[24rem] overflow-auto px-1 py-2 text-sm">
                    <p class="px-3 pb-2 text-xs uppercase tracking-wide text-neutral-500">{commandPaletteInstruction()}</p>
                    <For each={commandPaletteEntries()}>
                      {(entry, index) => {
                        const isActive = () => index() === commandPaletteSelected()
                        return (
                          <button
                            type="button"
                            onMouseEnter={() => setCommandPaletteSelected(index())}
                            onClick={() => executeCommand(entry.href)}
                            class={`w-full rounded px-3 py-2 text-left ${isActive() ? 'bg-neutral-900 text-white dark:bg-neutral-700' : 'hover:bg-neutral-100 dark:hover:bg-neutral-800'}`}
                          >
                            <div class="flex justify-between gap-2">
                              <span class="font-medium">{entry.label}</span>
                              <span class={`text-[11px] uppercase ${isActive() ? 'text-neutral-200' : 'text-neutral-500'}`}>{entry.group}</span>
                            </div>
                            <p class={`text-xs ${isActive() ? 'text-neutral-200' : 'text-neutral-500'}`}>{entry.description}</p>
                          </button>
                        )
                      }}
                    </For>
                  </div>
                </div>
              </div>
            </Show>
            <main class="flex-1 min-w-0 px-4 pt-8 pb-6 md:px-8 md:pt-10">
            <div class="mb-3 md:hidden">
              <button
                type="button"
                class="rounded border px-3 py-2"
                onClick={() => setIsNavOpen((value) => !value)}
              >
                ☰ Menu
              </button>
            </div>
            <Breadcrumbs />
            <Show when={!isDetailRoute()}>
              <section class="mb-5 rounded border border-neutral-200 bg-white/90 px-4 py-3 dark:border-neutral-800 dark:bg-neutral-900/60">
                <h1 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-100">{pageMeta().title}</h1>
                <p class="mt-1 text-sm text-neutral-600 dark:text-neutral-300">{pageMeta().subtitle}</p>
              </section>
            </Show>
            {props.children}
          </main>
        </div>
      </div>
    </div>
  )
}

const AppRoutes = () => {
  const [ledger] = createResource(async (): Promise<readonly LedgerNode[]> => (await managerClient.getLedger({})).nodes)
  const [rollouts] = createResource(async (): Promise<readonly Rollout[]> => (await managerClient.listRollouts({ activeOnly: true, page: 1, pageSize: 20 })).rollouts)

  return (
    <Route path="/" component={(props) => <Layout {...props}>{props.children}</Layout>}>
      <Route path="/" component={() => <Homepage ledger={ledger} rollouts={rollouts} />} />
      <Route path="/nodes" component={Nodes} />
      <Route path="/nodes/:id" component={NodeDetail} />
        <Route path="/manager" component={Manager} />
        <Route path="/raft" component={Raft} />
      <Route path="/manager/:id" component={ManagerNodeDetail} />
      <Route path="/jobs" component={Jobs} />
      <Route path="/jobs/:id" component={JobDetail} />
      <Route path="/jobs/:id/tasks/:taskName" component={TaskDetail} />
      <Route path="/jobs/:id/specs/:configId" component={JobSpecDetail} />
      <Route path="/deployments" component={Deployments} />
      <Route path="/rollouts" component={Rollouts} />
      <Route path="/rollouts/:id" component={RolloutDetail} />
      <Route path="/kv" component={KeyValue} />
      <Route path="/access" component={Access} />
      <Route path="/oidc/callback" component={OidcCallback} />
      <Route path="/api" component={ApiExplorer} />
      <Route path="/observability" component={Observability} />
      <Route path="/state-explorer" component={StateExplorer} />
      <Route path="/testing" component={TestingUtils} />
      <Route path="*" component={() => <Homepage ledger={ledger} rollouts={rollouts} />} />
    </Route>
  )
}

export default function App() {
  return <Router><AppRoutes /></Router>
}
