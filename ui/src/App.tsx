import { A, Navigate, Route, Router, useLocation, useNavigate, type RouteSectionProps } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal, on, onCleanup, onMount } from 'solid-js'
import Breadcrumbs from './components/Breadcrumbs'
import { AclBanner } from './components/AclBanner'
import { ConnectionGate } from './components/ConnectionGate'
import { Icon, PageHeader, cx, type IconName } from './components/ui'
import { managerClient } from './grpcClient'
import { commandPaletteIndexItems } from './utils/commandPalette'
import { connectionStatus, managerAddress } from './utils/connection'
import Access from './pages/Access'
import OidcCallback from './pages/OidcCallback'
import Events from './pages/Events'
import JobDetail from './pages/details/JobDetail'
import JobSpecDetail from './pages/details/JobSpecDetail'
import ManagerNodeDetail from './pages/details/ManagerNodeDetail'
import NodeDetail from './pages/details/NodeDetail'
import RolloutDetail from './pages/details/RolloutDetail'
import ServiceDetail from './pages/details/ServiceDetail'
import TaskDetail from './pages/details/TaskDetail'
import Homepage from './pages/Homepage'
import Jobs from './pages/Jobs'
import KeyValue from './pages/KeyValue'
import Manager from './pages/Manager'
import Nodes from './pages/Nodes'
import Rollouts from './pages/Rollouts'
import Services from './pages/Services'
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
  icon: IconName
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

const detailRoutes = new Set(['jobs', 'nodes', 'manager', 'rollouts', 'services'])

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

const NAV_COLLAPSED_KEY = 'ui-sidebar-collapsed'

const resolveInitialNavCollapsed = (): boolean =>
  typeof window !== 'undefined' && localStorage.getItem(NAV_COLLAPSED_KEY) === 'true'

// Geometric vexillum (legion standard): finial, crossbar, hanging banner.
const BrandMark = (props: { class?: string }) => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" class={props.class} aria-hidden="true">
    <path d="M12 4.5v17" />
    <circle cx="12" cy="2.8" r="1.1" fill="currentColor" stroke="none" />
    <path d="M5 6.5h14" />
    <path d="M7 6.5v7l2.5-1.5L12 13.5l2.5-1.5L17 13.5v-7" />
  </svg>
)

const Layout = (props: RouteSectionProps) => {
  const location = useLocation()
  const navigate = useNavigate()
  const [isNavOpen, setIsNavOpen] = createSignal(false)
  const [navCollapsePref, setNavCollapsePref] = createSignal(resolveInitialNavCollapsed())
  const [navTooltip, setNavTooltip] = createSignal<{ label: string; top: number } | null>(null)
  // Collapsing is a desktop affordance. The mobile drawer is already an
  // overlay with room for labels, so it ignores the preference while open.
  const isNavCollapsed = () => navCollapsePref() && !isNavOpen()
  const [theme, setTheme] = createSignal<'light' | 'dark'>(resolveInitialTheme())
  // Waits for the connection probe: firing this before we know the manager is
  // reachable just produces a second failure to ignore.
  const [o11yStatus] = createResource(
    () => connectionStatus() === 'connected',
    async () => managerClient.getObservability({ maxLines: 1 }),
  )
  const [commandPaletteOpen, setCommandPaletteOpen] = createSignal(false)
  const [commandPaletteQuery, setCommandPaletteQuery] = createSignal('')
  const [commandPaletteSelected, setCommandPaletteSelected] = createSignal(0)
  let commandPaletteInput: HTMLInputElement | undefined
  const isActiveNavPath = (path: string) =>
    path === '/' ? location.pathname === '/' : location.pathname === path || location.pathname.startsWith(`${path}/`)

  const navItems = createMemo(() => {
    const base: SidebarNode[] = [
      { kind: 'item', label: 'Overview', path: '/', title: 'Overview', copy: 'Cluster health at a glance.', icon: 'gauge' },
      {
        kind: 'section',
        heading: 'Workloads',
        items: [
          { kind: 'item', label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Every job on the cluster and its health.', icon: 'layers' },
          { kind: 'item', label: 'Services', path: '/services', title: 'Services', copy: 'Discoverable endpoints and their health.', icon: 'radio' },
          { kind: 'item', label: 'Rollouts', path: '/rollouts', title: 'Rollouts', copy: 'Version rollouts and their progress.', icon: 'rocket' },
        ],
      },
      {
        kind: 'section',
        heading: 'Cluster',
        items: [
          { kind: 'item', label: 'Workers', path: '/nodes', title: 'Workers', copy: 'Worker nodes running your allocations.', icon: 'server' },
          { kind: 'item', label: 'Managers', path: '/manager', title: 'Managers', copy: 'Manager nodes and their configuration.', icon: 'network' },
          { kind: 'item', label: 'Raft', path: '/raft', title: 'Raft', copy: 'Consensus status, elections, and membership.', icon: 'gitBranch' },
          { kind: 'item', label: 'Events', path: '/events', title: 'Events', copy: 'Live stream of scheduler activity.', icon: 'zap' },
        ],
      },
      {
        kind: 'section',
        heading: 'System',
        items: [
          { kind: 'item', label: 'KV Store', path: '/kv', title: 'KV Store', copy: 'Browse and edit key/value entries.', icon: 'database' },
          { kind: 'item', label: 'Access', path: '/access', title: 'Access', copy: 'Identity, tokens, policies, and login.', icon: 'shieldCheck' },
        ],
      },
      {
        kind: 'section',
        heading: 'Developer',
        items: [
          { kind: 'item', label: 'API Explorer', path: '/api', title: 'API Explorer', copy: 'Call any manager RPC interactively.', icon: 'terminal' },
          { kind: 'item', label: 'State Explorer', path: '/state-explorer', title: 'State Explorer', copy: 'Query the manager state store with SQL.', icon: 'boxes' },
        ],
      },
    ]

    if (o11yStatus()?.o11yEnabled) {
      const developer = base.find((item) => item.kind === 'section' && item.heading === 'Developer') as SidebarSection | undefined
      developer?.items.push({
        kind: 'item',
        label: 'Observability',
        path: '/observability',
        title: 'Observability',
        copy: 'Manager logs and operational telemetry.',
        icon: 'eye',
      })
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

  // Remember what was focused before the palette opened so closing it
  // returns focus there instead of dropping it on <body>.
  let paletteReturnFocus: HTMLElement | null = null

  const openCommandPalette = () => {
    paletteReturnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
    setCommandPaletteQuery('')
    setCommandPaletteOpen(true)
    setCommandPaletteSelected(0)
    requestAnimationFrame(() => commandPaletteInput?.focus())
  }

  const closeCommandPalette = () => {
    if (!commandPaletteOpen()) return
    setCommandPaletteOpen(false)
    paletteReturnFocus?.focus()
    paletteReturnFocus = null
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
    if (event.key === 'Escape' && isNavOpen()) {
      setIsNavOpen(false)
      return
    }
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
      // Don't hijack the key while a link/button has focus (keyboard users).
      const active = document.activeElement as HTMLElement | null
      if (active && active !== document.body && active.closest('a, button, [role="button"]')) return
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

  // `on` keeps the dependency list to the pathname. A bare createEffect would
  // also track commandPaletteOpen() (read inside closeCommandPalette), so
  // opening the palette re-ran this effect and closed it again immediately.
  createEffect(
    on(
      () => location.pathname,
      () => {
        setIsNavOpen(false)
        closeCommandPalette()
      },
    ),
  )

  createEffect(() => {
    const current = theme()
    document.documentElement.classList.toggle('dark', current === 'dark')
    localStorage.setItem('ui-theme', current)
  })

  createEffect(() => {
    localStorage.setItem(NAV_COLLAPSED_KEY, String(navCollapsePref()))
  })

  // Collapsed nav items are icons only, so a tooltip names them. It is
  // position-fixed rather than absolute because the nav is a scroll container
  // and would clip anything overflowing its right edge.
  const showNavTooltip = (target: HTMLElement, label: string) => {
    if (!isNavCollapsed()) return
    const rect = target.getBoundingClientRect()
    setNavTooltip({ label, top: rect.top + rect.height / 2 })
  }

  const hideNavTooltip = () => setNavTooltip(null)

  const pageMeta = createMemo(() => {
    const normalized = location.pathname.replace(/\/+$/, '') || '/'
    const segments = normalized.split('/').filter(Boolean)
    const topLevelPath = segments.length > 0 ? `/${segments[0]}` : '/'
    const visibleNavItems = flattenedNavItems()
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

  const NavLink = (itemProps: { item: SidebarItem }) => (
    <A
      href={itemProps.item.path}
      aria-label={isNavCollapsed() ? itemProps.item.label : undefined}
      onMouseEnter={(event) => showNavTooltip(event.currentTarget, itemProps.item.label)}
      onMouseLeave={hideNavTooltip}
      onFocus={(event) => showNavTooltip(event.currentTarget, itemProps.item.label)}
      onBlur={hideNavTooltip}
      class={cx(
        'group relative flex items-center rounded-md py-1.5 text-sm transition-colors',
        isNavCollapsed() ? 'justify-center px-2' : 'gap-2.5 px-2.5',
        isActiveNavPath(itemProps.item.path)
          ? 'bg-brass/10 font-medium text-ink'
          : 'text-muted hover:bg-surface2 hover:text-ink',
      )}
    >
      <span
        class={cx(
          'absolute -left-2.5 top-1/2 h-4 w-0.5 -translate-y-1/2 rounded-full bg-brass transition-opacity',
          isActiveNavPath(itemProps.item.path) ? 'opacity-100' : 'opacity-0',
        )}
      />
      <Icon name={itemProps.item.icon} size={15} class={cx(isActiveNavPath(itemProps.item.path) ? 'text-brass' : 'text-faint group-hover:text-muted')} />
      <Show when={!isNavCollapsed()}>{itemProps.item.label}</Show>
    </A>
  )

  return (
    <div class="min-h-screen">
      <div class="flex min-h-screen">
        <aside
          id="app-sidebar"
          class={cx(
            'fixed inset-y-0 left-0 z-40 flex shrink-0 transform flex-col border-r border-line bg-surface transition-all md:sticky md:top-0 md:h-screen md:translate-x-0',
            isNavCollapsed() ? 'w-16' : 'w-60',
            isNavOpen() ? 'translate-x-0' : '-translate-x-full',
          )}
        >
          <div
            class={cx(
              'flex pb-4 pt-5',
              isNavCollapsed() ? 'flex-col items-center gap-3 px-2' : 'items-center justify-between px-4',
            )}
          >
            <A href="/" class="flex items-center gap-2.5" aria-label="Legion home">
              <span class="flex h-7 w-7 items-center justify-center rounded-md bg-brass/15 text-brass">
                <BrandMark class="h-[18px] w-[18px]" />
              </span>
              <Show when={!isNavCollapsed()}>
                <span class="font-display text-sm font-bold uppercase tracking-[0.28em] text-ink">Legion</span>
              </Show>
            </A>
            <button
              type="button"
              class="hidden rounded-md p-1.5 text-faint transition-colors hover:bg-surface2 hover:text-ink md:inline-flex"
              onClick={() => {
                hideNavTooltip()
                setNavCollapsePref((value) => !value)
              }}
              aria-label={navCollapsePref() ? 'Expand sidebar' : 'Collapse sidebar'}
              aria-expanded={!navCollapsePref()}
              aria-controls="app-sidebar"
            >
              <Icon name={navCollapsePref() ? 'chevronRight' : 'chevronLeft'} size={15} />
            </button>
            <button
              type="button"
              class="rounded-md p-1.5 text-muted hover:bg-surface2 md:hidden"
              onClick={() => setIsNavOpen(false)}
              aria-label="Close menu"
            >
              <Icon name="x" size={16} />
            </button>
          </div>
          <nav class={cx('flex-1 space-y-4 overflow-y-auto pb-4', isNavCollapsed() ? 'px-2' : 'px-2.5')}>
            <For each={navItems()}>
              {(entry) => (
                <Show
                  when={entry.kind === 'section'}
                  fallback={<NavLink item={entry as SidebarItem} />}
                >
                  <div>
                    {/* Collapsed there is no room for the heading, but the
                        grouping still reads as a rule between icon clusters. */}
                    <Show
                      when={!isNavCollapsed()}
                      fallback={<div class="mx-auto mb-2 h-px w-6 bg-line" />}
                    >
                      <p class="eyebrow px-2.5 pb-1.5">{(entry as SidebarSection).heading}</p>
                    </Show>
                    <div class="space-y-0.5">
                      <For each={(entry as SidebarSection).items}>
                        {(item) => <NavLink item={item} />}
                      </For>
                    </div>
                  </div>
                </Show>
              )}
            </For>
          </nav>
          <div
            class={cx('border-t border-line py-3', isNavCollapsed() ? 'px-2' : 'px-4')}
            onMouseEnter={(event) => showNavTooltip(event.currentTarget, `Manager · ${managerAddress}`)}
            onMouseLeave={hideNavTooltip}
          >
            <Show
              when={!isNavCollapsed()}
              fallback={
                <div class="flex justify-center text-faint">
                  <Icon name="network" size={15} />
                </div>
              }
            >
              <p class="eyebrow mb-1">Manager</p>
              <p class="truncate font-mono text-2xs text-muted" title={managerAddress}>
                {managerAddress.replace(/^https?:\/\//, '')}
              </p>
            </Show>
          </div>
        </aside>
        <Show when={navTooltip()}>
          {(tip) => (
            <div
              role="tooltip"
              class="tooltip-pop pointer-events-none fixed left-[4.5rem] z-50 -translate-y-1/2 whitespace-nowrap rounded-md border border-line bg-surface px-2 py-1 text-xs font-medium text-ink shadow-pop"
              style={{ top: `${tip().top}px` }}
            >
              {tip().label}
            </div>
          )}
        </Show>
        <Show when={isNavOpen()}>
          {/* Click-to-close scrim; keyboard users close via Escape or the ✕ button. */}
          <div
            class="fixed inset-0 z-30 bg-black/40 backdrop-blur-[1px] md:hidden"
            onClick={() => setIsNavOpen(false)}
            aria-hidden="true"
          />
        </Show>
        <div class="relative flex min-h-screen min-w-0 flex-1 flex-col">
          <AclBanner />
          <div class="sticky top-0 z-20 flex h-12 items-center gap-3 border-b border-line bg-canvas/85 px-4 backdrop-blur md:px-6">
            <button
              type="button"
              class="rounded-md p-1.5 text-muted hover:bg-surface2 md:hidden"
              onClick={() => setIsNavOpen((value) => !value)}
              aria-label="Open menu"
              aria-expanded={isNavOpen()}
              aria-controls="app-sidebar"
            >
              <Icon name="menu" size={17} />
            </button>
            <Breadcrumbs />
            <div class="flex-1" />
            <button
              type="button"
              class="hidden items-center gap-2 rounded-md border border-line bg-surface px-2.5 py-1.5 text-xs text-faint transition-colors hover:border-line-strong hover:text-muted sm:flex"
              onClick={openCommandPalette}
              aria-label="Open command palette"
            >
              <Icon name="search" size={13} />
              <span>Search…</span>
              <kbd class="rounded border border-line bg-surface2 px-1 font-mono text-2xs">⌘K</kbd>
            </button>
            <button
              type="button"
              class="rounded-md p-1.5 text-muted transition-colors hover:bg-surface2 hover:text-ink sm:hidden"
              onClick={openCommandPalette}
              aria-label="Open command palette"
            >
              <Icon name="search" size={16} />
            </button>
            <button
              type="button"
              class="rounded-md p-1.5 text-muted transition-colors hover:bg-surface2 hover:text-ink"
              onClick={() => setTheme((value) => (value === 'dark' ? 'light' : 'dark'))}
              aria-label={theme() === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
            >
              <Icon name={theme() === 'dark' ? 'sun' : 'moon'} size={16} />
            </button>
          </div>
          <Show when={commandPaletteOpen()}>
            <div
              class="fixed inset-0 z-50 flex items-start justify-center bg-black/50 p-4 pt-[12vh] backdrop-blur-[2px]"
              data-command-palette
              onClick={closeCommandPalette}
              onKeyDown={(event) => {
                // Keep focus on the search input — the palette is keyboard-driven.
                if (event.key === 'Tab') {
                  event.preventDefault()
                  commandPaletteInput?.focus()
                }
              }}
            >
              <div
                role="dialog"
                aria-modal="true"
                aria-label="Command palette"
                class="w-full max-w-xl overflow-hidden rounded-xl border border-line bg-surface shadow-pop"
                onClick={(event) => event.stopPropagation()}
              >
                <div class="flex items-center gap-2.5 border-b border-line px-4 py-3">
                  <Icon name="search" size={16} class="text-faint" />
                  <input
                    ref={commandPaletteInput}
                    value={commandPaletteQuery()}
                    onInput={(event) => setCommandPaletteQuery(event.currentTarget.value)}
                    onKeyDown={onCommandPaletteKeydown}
                    placeholder="Jump to a page, job, node…"
                    class="w-full border-none bg-transparent p-0 text-sm text-ink placeholder:text-faint focus:outline-none focus:ring-0"
                    role="combobox"
                    aria-expanded="true"
                    aria-controls="command-palette-listbox"
                    aria-activedescendant={`command-palette-option-${commandPaletteSelected()}`}
                    aria-label="Search command palette"
                  />
                  <kbd class="rounded border border-line bg-surface2 px-1.5 py-0.5 font-mono text-2xs text-faint">esc</kbd>
                </div>
                <div id="command-palette-listbox" role="listbox" aria-label="Destinations" class="max-h-[22rem] overflow-auto p-1.5 text-sm">
                  <Show
                    when={totalCommandPaletteEntries() > 0}
                    fallback={<p class="px-3 py-6 text-center text-sm text-faint">No matches.</p>}
                  >
                    <For each={commandPaletteEntries()}>
                      {(entry, index) => {
                        const isActive = () => index() === commandPaletteSelected()
                        return (
                          <button
                            type="button"
                            role="option"
                            id={`command-palette-option-${index()}`}
                            aria-selected={isActive()}
                            tabindex={-1}
                            onMouseEnter={() => setCommandPaletteSelected(index())}
                            onClick={() => executeCommand(entry.href)}
                            class={cx(
                              'flex w-full items-center justify-between gap-3 rounded-md px-3 py-2 text-left transition-colors',
                              isActive() ? 'bg-brass/10' : 'hover:bg-surface2',
                            )}
                          >
                            <span class="min-w-0">
                              <span class={cx('block truncate font-medium', isActive() ? 'text-ink' : 'text-ink')}>{entry.label}</span>
                              <span class="block truncate text-xs text-faint">{entry.description}</span>
                            </span>
                            <span class={cx('shrink-0 text-2xs uppercase tracking-wide', isActive() ? 'text-brass' : 'text-faint')}>{entry.group}</span>
                          </button>
                        )
                      }}
                    </For>
                  </Show>
                </div>
                <div class="flex items-center gap-3 border-t border-line px-4 py-2 text-2xs text-faint">
                  <span><kbd class="font-mono">↑↓</kbd> navigate</span>
                  <span><kbd class="font-mono">↵</kbd> open</span>
                  <span><kbd class="font-mono">esc</kbd> close</span>
                </div>
              </div>
            </div>
          </Show>
          <main class="flex-1 min-w-0 px-4 py-6 md:px-8">
            <Show when={!isDetailRoute()}>
              <PageHeader title={pageMeta().title} subtitle={pageMeta().subtitle} />
            </Show>
            <ConnectionGate>{props.children}</ConnectionGate>
          </main>
        </div>
      </div>
    </div>
  )
}

const AppRoutes = () => {
  return (
    <Route path="/" component={(props) => <Layout {...props}>{props.children}</Layout>}>
      <Route path="/" component={Homepage} />
      <Route path="/nodes" component={Nodes} />
      <Route path="/nodes/:id" component={NodeDetail} />
      <Route path="/manager" component={Manager} />
      <Route path="/raft" component={Raft} />
      <Route path="/manager/:id" component={ManagerNodeDetail} />
      <Route path="/jobs" component={Jobs} />
      <Route path="/jobs/:id" component={JobDetail} />
      <Route path="/jobs/:id/tasks/:taskName" component={TaskDetail} />
      <Route path="/jobs/:id/specs/:configId" component={JobSpecDetail} />
      <Route path="/services" component={Services} />
      <Route path="/services/:name" component={ServiceDetail} />
      <Route path="/deployments" component={() => <Navigate href="/rollouts" />} />
      <Route path="/rollouts" component={Rollouts} />
      <Route path="/rollouts/:id" component={RolloutDetail} />
      <Route path="/kv" component={KeyValue} />
      <Route path="/events" component={Events} />
      <Route path="/access" component={Access} />
      <Route path="/oidc/callback" component={OidcCallback} />
      <Route path="/api" component={ApiExplorer} />
      <Route path="/observability" component={Observability} />
      <Route path="/state-explorer" component={StateExplorer} />
      <Route path="/testing" component={TestingUtils} />
      <Route path="*" component={Homepage} />
    </Route>
  )
}

export default function App() {
  return <Router><AppRoutes /></Router>
}
