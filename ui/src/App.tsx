import { A, Navigate, Route, Router, useLocation, useNavigate, type RouteSectionProps } from '@solidjs/router'
import { ErrorBoundary, For, Show, createEffect, createMemo, createResource, createSignal, on, onCleanup, onMount, type JSX } from 'solid-js'
import Breadcrumbs, { buildCrumbs, isKnownSection } from './components/Breadcrumbs'
import { AclBanner } from './components/AclBanner'
import { ConnectionGate } from './components/ConnectionGate'
import { lazyPage } from './components/LazyPage'
import { ShortcutsHelp } from './components/ShortcutsHelp'
import { Button, Dot, Icon, JumpHint, Kbd, PageHeader, RelativeTime, cx, type IconName } from './components/ui'
import { managerClient } from './grpcClient'
import { ROUTE_RESOURCE, canRead, isAnonymous } from './utils/acl'
import { commandPaletteIndexItems } from './utils/commandPalette'
import {
  connectionStatus,
  identityError,
  lostSince,
  managerAddress,
  managerIdentity,
  managerLink,
  probeManagerLink,
  startConnectionWatchdog,
} from './utils/connection'
import {
  assignNavHintLetters,
  hasJumpModifier,
  indexHintPosition,
  indexJumpTargets,
  jumpDigitFromEvent,
  jumpLetterFromEvent,
  setJumpHintsVisible,
} from './utils/jumpHints'
import { clearRecents, recents, recordRecent } from './utils/recents'
import type { Tone } from './utils/status'
import Access from './pages/Access'
import OidcCallback from './pages/OidcCallback'
import Events from './pages/Events'
import JobDetail from './pages/details/JobDetail'
import JobSpecDetail from './pages/details/JobSpecDetail'
import ManagerNodeDetail from './pages/details/ManagerNodeDetail'
import NodeDetail from './pages/details/NodeDetail'
import RolloutDetail from './pages/details/RolloutDetail'
import ServiceDetail from './pages/details/ServiceDetail'
import VolumeDetail from './pages/details/VolumeDetail'
import TaskDetail from './pages/details/TaskDetail'
import Homepage from './pages/Homepage'
import Jobs from './pages/Jobs'
import KeyValue from './pages/KeyValue'
import Manager from './pages/Manager'
import Nodes from './pages/Nodes'
import NotFound from './pages/NotFound'
import Rollouts from './pages/Rollouts'
import Services from './pages/Services'
import Network from './pages/Network'
import Volumes from './pages/Volumes'
import TestingUtils from './pages/TestingUtils'
import Raft from './pages/Raft'
import Templates from './pages/Templates'
import TemplateDetail from './pages/details/TemplateDetail'

// The developer tools carry the heaviest code (the API Explorer ships the
// generated proto sources as text for its docs), so they load on first
// visit rather than on every visit to the overview.
const ApiExplorer = lazyPage(() => import('./pages/ApiExplorer'))
const StateExplorer = lazyPage(() => import('./pages/StateExplorer'))
const Observability = lazyPage(() => import('./pages/Observability'))

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
  // An action rather than a destination: run it instead of navigating.
  run?: () => void
  icon?: IconName
}

type SidebarNode = SidebarItem | SidebarSection

const detailRoutes = new Set(['jobs', 'nodes', 'manager', 'rollouts', 'services', 'templates', 'volumes'])

// The section a detail URL belongs to, as the ⌘K "Recent" group names it.
const RECENT_KINDS: Record<string, string> = {
  jobs: 'Jobs',
  nodes: 'Workers',
  manager: 'Managers',
  rollouts: 'Rollouts',
  services: 'Services',
  templates: 'Templates',
  volumes: 'Volumes',
}

// Anonymous callers keep the whole sidebar: they have not had a chance to
// authenticate, and an app that hides every destination looks broken rather
// than locked. Clicking through lands on AccessDenied, which says why and
// links to login. A caller who IS authenticated only sees what their rules
// allow. Cosmetic — the manager enforces regardless.
const isNavVisible = (path: string): boolean => {
  const resource = ROUTE_RESOURCE[path]
  if (!resource) return true
  return canRead(resource) || isAnonymous()
}

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

// Last line of defense for the page area: a page that throws (most often an
// unguarded read of an errored resource) must never take the app down with
// it — before this existed, one rejected RPC could freeze navigation while
// the URL kept changing. Renders its children normally (no conditional
// capture of the outlet) and swaps to the card only when something throws;
// navigating elsewhere resets it.
const RouteErrorBoundary = (props: { children: JSX.Element }) => {
  const location = useLocation()
  return (
    <ErrorBoundary
      fallback={(err, reset) => {
        createEffect(on(() => location.pathname, reset, { defer: true }))
        return (
          <div class="flex justify-center py-12">
            <div class="w-full max-w-md rounded-lg border border-line bg-surface p-6 text-center shadow-card">
              <span class="mx-auto flex h-10 w-10 items-center justify-center rounded-full bg-red-500/10 text-red-600 dark:text-red-400">
                <Icon name="alertTriangle" size={19} />
              </span>
              <h2 class="mt-3 font-display text-lg font-semibold text-ink">This page hit an error</h2>
              <p class="mx-auto mt-1.5 max-w-sm break-words font-mono text-xs text-muted">
                {err instanceof Error ? err.message : String(err)}
              </p>
              <button
                type="button"
                class="mt-5 inline-flex items-center gap-2 rounded-md border border-transparent bg-brass px-3.5 py-2 text-sm font-semibold text-canvas transition-colors hover:bg-brass-strong"
                onClick={reset}
              >
                <Icon name="refresh" size={15} />
                Try again
              </button>
            </div>
          </div>
        )
      }}
    >
      {props.children}
    </ErrorBoundary>
  )
}

// Who the manager thinks this browser is, for the sidebar footer. One line,
// because it is glanced at, not read: the Access page has the detail.
const identitySummary = (): { label: string; detail: string; tone: Tone } => {
  const identity = managerIdentity()
  if (!identity) {
    return identityError()
      ? { label: 'Credential rejected', detail: 'The stored token was refused — open Access to fix it', tone: 'danger' }
      : { label: 'Resolving identity…', detail: '', tone: 'neutral' }
  }
  if (!identity.aclEnabled) {
    return { label: 'ACLs off · full access', detail: 'Every caller has full access until acl.enabled is set', tone: 'warn' }
  }
  if (identity.source === 'anonymous') {
    return { label: 'Not logged in', detail: 'ACLs are enforced — log in to see your cluster', tone: 'warn' }
  }
  return {
    label: identity.principal || identity.tokenType || 'authenticated',
    detail: `${identity.tokenType || 'token'} via ${identity.source}${identity.auditOnly ? ' · audit-only' : ''}`,
    tone: identity.auditOnly ? 'info' : 'ok',
  }
}

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
  const [shortcutsOpen, setShortcutsOpen] = createSignal(false)
  // Waits for the connection probe: firing this before we know the manager is
  // reachable just produces a second failure to ignore.
  // The probe MUST swallow its own failure. GetObservability needs
  // operator:read; for any identity without it the RPC rejects, and reading
  // an errored resource THROWS at the read site — which is the navItems()
  // memo. An exception there takes down the layout's whole reactive graph:
  // the URL keeps changing while the nav, header, and page freeze. (This is
  // exactly what restricted tokens hit before ACL-aware rendering existed;
  // treat "can't ask" the same as "o11y off".)
  const [o11yStatus] = createResource(
    () => connectionStatus() === 'connected',
    async () => {
      try {
        return await managerClient.getObservability({ maxLines: 1 })
      } catch {
        return undefined
      }
    },
  )
  const [commandPaletteOpen, setCommandPaletteOpen] = createSignal(false)
  const [commandPaletteQuery, setCommandPaletteQuery] = createSignal('')
  const [commandPaletteSelected, setCommandPaletteSelected] = createSignal(0)
  let commandPaletteInput: HTMLInputElement | undefined
  const isActiveNavPath = (path: string) =>
    path === '/' ? location.pathname === '/' : location.pathname === path || location.pathname.startsWith(`${path}/`)

  // Once the first connect succeeds, keep checking that the manager is still
  // there; the banner below the topbar reports when it is not.
  createEffect(() => {
    if (connectionStatus() === 'connected') startConnectionWatchdog()
  })

  const navItems = createMemo(() => {
    const base: SidebarNode[] = [
      { kind: 'item', label: 'Overview', path: '/', title: 'Overview', copy: 'Cluster health at a glance.', icon: 'gauge' },
      {
        kind: 'section',
        heading: 'Workloads',
        items: [
          { kind: 'item', label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Every job on the cluster and its health.', icon: 'layers' },
          { kind: 'item', label: 'Templates', path: '/templates', title: 'Templates', copy: 'Reusable job templates with guided parameters.', icon: 'layoutTemplate' },
          { kind: 'item', label: 'Services', path: '/services', title: 'Services', copy: 'Discoverable endpoints and their health.', icon: 'radio' },
          { kind: 'item', label: 'Network', path: '/network', title: 'Network', copy: 'Egress and ingress policy, upstreams, and handlers.', icon: 'shieldCheck' },
          { kind: 'item', label: 'Rollouts', path: '/rollouts', title: 'Rollouts', copy: 'Version rollouts and their progress.', icon: 'rocket' },
          { kind: 'item', label: 'Volumes', path: '/volumes', title: 'Volumes', copy: 'Persistent storage behind stateful tasks.', icon: 'hardDrive' },
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

    // Hide destinations the identity's ACL rules cannot read; drop sections
    // that end up empty. Direct navigation still works (and fails loudly) —
    // this only trims what the UI offers.
    const isVisible = (item: SidebarItem) => isNavVisible(item.path)
    return base.flatMap<SidebarNode>((entry) => {
      if (entry.kind === 'item') return isVisible(entry) ? [entry] : []
      const items = entry.items.filter(isVisible)
      return items.length > 0 ? [{ ...entry, items }] : []
    })
  })

  const flattenedNavItems = createMemo(() =>
    navItems().flatMap((entry) => (entry.kind === 'item' ? [entry] : entry.items)),
  )

  // Letters are assigned over the flattened nav in render order, so the hints
  // stay stable as long as the sidebar does — and shift by one item when
  // Observability appears, which is the honest cost of assigning them from the
  // live list rather than hard-coding a letter per route.
  const navHintLetters = createMemo(() => {
    const items = flattenedNavItems()
    const letters = assignNavHintLetters(items.map((item) => item.label))
    return items.map((item, index) => ({ path: item.path, letter: letters[index] }))
  })

  // The badge spells the whole chord minus the modifier already under the
  // user's thumb — "⇧O", not "O", so nobody reaches for ⌘O and gets the
  // browser's file picker.
  const navHintKeysFor = createMemo(() => {
    const byPath = new Map(navHintLetters().map((entry) => [entry.path, entry.letter]))
    return (path: string) => {
      const letter = byPath.get(path)
      return letter ? `⇧${letter.toUpperCase()}` : undefined
    }
  })

  const navHintTargets = createMemo(
    () => new Map(navHintLetters().filter((entry) => entry.letter).map((entry) => [entry.letter, entry.path])),
  )

  const toggleTheme = () => setTheme((value) => (value === 'dark' ? 'light' : 'dark'))

  const commandPaletteEntries = createMemo(() => {
    const query = commandPaletteQuery().trim().toLowerCase()
    const deduped = new Map<string, CommandPaletteItem>()
    const entries: CommandPaletteItem[] = [
      // Recent pages come first: with no query they are the most likely
      // destination, and with one they still match by name.
      ...recents().map((entry) => ({
        id: `recent:${entry.href}`,
        label: entry.label,
        description: entry.kind,
        searchText: `${entry.label} ${entry.kind}`.toLowerCase(),
        href: entry.href,
        group: 'Recent',
        icon: 'history' as const,
      })),
      ...flattenedNavItems().map((entry) => ({
        id: `nav:${entry.path}`,
        label: entry.label,
        description: entry.copy,
        searchText: entry.label.toLowerCase(),
        href: entry.path,
        group: 'Navigation',
        icon: entry.icon,
      })),
      ...commandPaletteIndexItems().map((entry) => ({
        id: `index:${entry.id}`,
        label: entry.label,
        description: [entry.subtitle, entry.source].filter(Boolean).join(' · '),
        searchText: `${entry.label} ${entry.source}`.toLowerCase(),
        href: entry.href,
        group: entry.source,
      })),
      {
        id: 'action:theme',
        label: theme() === 'dark' ? 'Switch to light mode' : 'Switch to dark mode',
        description: 'Theme',
        searchText: 'theme light dark mode switch toggle',
        href: '#theme',
        group: 'Actions',
        run: toggleTheme,
        icon: theme() === 'dark' ? ('sun' as const) : ('moon' as const),
      },
      {
        id: 'action:shortcuts',
        label: 'Keyboard shortcuts',
        description: 'Every key the UI answers to',
        searchText: 'keyboard shortcuts help keys cheat sheet',
        href: '#shortcuts',
        group: 'Actions',
        run: () => setShortcutsOpen(true),
        icon: 'keyboard' as const,
      },
      ...(recents().length > 0
        ? [
            {
              id: 'action:clear-recents',
              label: 'Clear recent pages',
              description: 'Forget the pages listed under Recent',
              searchText: 'clear recent pages history forget',
              href: '#clear-recents',
              group: 'Actions',
              run: clearRecents,
              icon: 'trash' as const,
            },
          ]
        : []),
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

  const executeCommand = (entry: Pick<CommandPaletteItem, 'href' | 'run'>) => {
    closeCommandPalette()
    if (entry.run) {
      entry.run()
      return
    }
    navigate(entry.href)
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
        executeCommand(entry)
      }
      return true
    }
    if (key === 'u') {
      event.preventDefault()
      const normalized = location.pathname.replace(/\/+$/, '') || '/'
      const segments = normalized.split('/').filter(Boolean)
      if (segments.length > 1) {
        segments.pop()
        executeCommand({ href: `/${segments.join('/')}` })
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
        executeCommand({ href: '/' })
      }
      return
    }

    segments.pop()
    executeCommand({ href: `/${segments.join('/')}` })
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
    if (event.key === 'Escape' && shortcutsOpen()) {
      event.preventDefault()
      setShortcutsOpen(false)
      return
    }
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

    // `?` opens the cheat sheet, the way it does on GitHub and Linear. It is
    // Shift+/ on most layouts, so match the produced character, not the code.
    if (event.key === '?' && !event.metaKey && !event.ctrlKey && !event.altKey && !isEditableTarget(event)) {
      event.preventDefault()
      setShortcutsOpen((value) => !value)
      return
    }

    if (shortcutsOpen()) return

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
    setShortcutsOpen(false)
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

  // Hold ⌘ (Ctrl off macOS) to label every index row and sidebar item with the
  // key that opens it. Both keydown and keyup carry the modifier flags, so one
  // handler on each covers press and release.
  const syncJumpHints = (event: KeyboardEvent) => {
    setJumpHintsVisible(hasJumpModifier(event) && !commandPaletteOpen() && !shortcutsOpen())
  }

  // ⌘Tab and friends take focus without ever delivering the keyup, which would
  // otherwise leave the hints stuck on.
  const hideJumpHints = () => setJumpHintsVisible(false)

  // Visibility stays a pure function of the modifier, so keeping it held after a
  // jump simply labels the page you landed on.
  const onJumpHintKeydown = (event: KeyboardEvent) => {
    if (commandPaletteOpen() || shortcutsOpen() || !hasJumpModifier(event) || event.altKey) return

    // Sidebar items carry Shift because plain ⌘+letter belongs to the browser.
    if (event.shiftKey) {
      const letter = jumpLetterFromEvent(event)
      const path = letter ? navHintTargets().get(letter) : undefined
      if (!path) return
      event.preventDefault()
      navigate(path)
      return
    }

    const digit = jumpDigitFromEvent(event)
    const position = digit ? indexHintPosition(digit) : -1
    const href = position >= 0 ? indexJumpTargets()[position] : undefined
    if (!href) return
    event.preventDefault()
    navigate(href)
  }

  onMount(() => {
    window.addEventListener('keydown', onGlobalCommandPaletteKeydown)
    window.addEventListener('keydown', onJumpHintKeydown)
    window.addEventListener('keydown', syncJumpHints)
    window.addEventListener('keyup', syncJumpHints)
    window.addEventListener('blur', hideJumpHints)
    onCleanup(() => {
      window.removeEventListener('keydown', onGlobalCommandPaletteKeydown)
      window.removeEventListener('keydown', onJumpHintKeydown)
      window.removeEventListener('keydown', syncJumpHints)
      window.removeEventListener('keyup', syncJumpHints)
      window.removeEventListener('blur', hideJumpHints)
    })
  })

  // ⌘K reveals the hints on its way to opening the palette; drop them once it is.
  createEffect(() => {
    if (commandPaletteOpen() || shortcutsOpen()) hideJumpHints()
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
        setShortcutsOpen(false)
      },
    ),
  )

  // Every detail page you open becomes a ⌘K "Recent" entry, from the URL
  // alone — no page has to know. Task pages name both the task and its job.
  createEffect(
    on(
      () => location.pathname,
      (pathname) => {
        const segments = (pathname.replace(/\/+$/, '') || '/').split('/').filter(Boolean)
        const kind = segments.length >= 2 ? RECENT_KINDS[segments[0]] : undefined
        if (!kind) return
        const id = decodePathSegment(segments[1])
        if (segments[0] === 'jobs' && segments[2] === 'specs') return
        if (segments[0] === 'jobs' && segments[2] === 'tasks' && segments[3]) {
          recordRecent({ href: pathname, label: `${decodePathSegment(segments[3])} · ${id}`, kind: 'Tasks' })
          return
        }
        if (segments.length > 2) return
        recordRecent({ href: pathname, label: segments[0] === 'rollouts' ? `Rollout #${id}` : id, kind })
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

  // The tab title reads innermost-first ("web · api · Jobs · Legion") so
  // twenty open tabs are tellable apart by their first word.
  createEffect(() => {
    const labels = buildCrumbs(location.pathname)
      .slice(1)
      .map((crumb) => crumb.label)
      .filter((label) => label !== 'Tasks' && label !== 'Specs')
    document.title = [...(labels.length > 0 ? labels.reverse() : ['Overview']), 'Legion'].join(' · ')
  })

  const isDetailRoute = () => {
    const normalized = location.pathname.replace(/\/+$/, '') || '/'
    const segments = normalized.split('/').filter(Boolean)
    return segments.length > 1 && detailRoutes.has(segments[0] ?? '')
  }

  // An unknown URL renders the not-found card, which carries its own
  // heading; the generic header would name the nearest nav item instead.
  const isUnknownRoute = () => {
    const segments = (location.pathname.replace(/\/+$/, '') || '/').split('/').filter(Boolean)
    return !isKnownSection(segments[0])
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
      <Show when={!isNavCollapsed()}>
        <span class="truncate">{itemProps.item.label}</span>
        {/* Collapsed to icons there is nowhere to put the letter, so the hints
            are an expanded-sidebar affordance; the chord still works. */}
        <JumpHint keys={navHintKeysFor()(itemProps.item.path)} class="ml-auto" />
      </Show>
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
          {/* Who you are, and where you are connected. Both link to the page
              that can change them. */}
          <div class={cx('space-y-1 border-t border-line py-2.5', isNavCollapsed() ? 'px-2' : 'px-2.5')}>
            <A
              href="/access"
              class={cx(
                'flex items-center rounded-md py-1.5 text-xs text-muted transition-colors hover:bg-surface2 hover:text-ink',
                isNavCollapsed() ? 'justify-center px-2' : 'gap-2.5 px-2.5',
              )}
              aria-label={`Identity: ${identitySummary().label}`}
              title={identitySummary().detail || undefined}
              onMouseEnter={(event) => showNavTooltip(event.currentTarget, identitySummary().label)}
              onMouseLeave={hideNavTooltip}
              onFocus={(event) => showNavTooltip(event.currentTarget, identitySummary().label)}
              onBlur={hideNavTooltip}
            >
              <span class="relative flex shrink-0 items-center justify-center text-faint">
                <Icon name="user" size={15} />
                <Dot tone={identitySummary().tone} class="absolute -bottom-0.5 -right-1 ring-2 ring-surface" />
              </span>
              <Show when={!isNavCollapsed()}>
                <span class="min-w-0 flex-1">
                  <span class="block truncate font-medium text-ink">{identitySummary().label}</span>
                  <Show when={identitySummary().detail}>
                    <span class="block truncate text-2xs text-faint">{identitySummary().detail}</span>
                  </Show>
                </span>
              </Show>
            </A>
            <div
              class={cx('flex items-center py-1 text-2xs text-faint', isNavCollapsed() ? 'justify-center px-2' : 'gap-2.5 px-2.5')}
              onMouseEnter={(event) => showNavTooltip(event.currentTarget, `Manager · ${managerAddress}`)}
              onMouseLeave={hideNavTooltip}
            >
              <span class="relative flex shrink-0 items-center justify-center">
                <Icon name="network" size={15} />
                <Dot
                  tone={managerLink() === 'ok' ? 'ok' : 'danger'}
                  pulse={managerLink() !== 'ok'}
                  class="absolute -bottom-0.5 -right-1 ring-2 ring-surface"
                />
              </span>
              <Show when={!isNavCollapsed()}>
                <span class="min-w-0 flex-1">
                  <span class="eyebrow block">Manager</span>
                  <span class="block truncate font-mono text-2xs text-muted" title={managerAddress}>
                    {managerAddress.replace(/^https?:\/\//, '')}
                  </span>
                </span>
              </Show>
            </div>
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
          <Show when={managerLink() === 'lost'}>
            <div
              role="alert"
              class="flex items-center justify-between gap-3 border-b border-red-500/25 bg-red-500/10 px-4 py-2 text-sm text-red-800 dark:text-red-200 md:px-6"
            >
              <p class="flex min-w-0 items-center gap-2">
                <Icon name="wifiOff" size={15} class="shrink-0" />
                <span class="min-w-0">
                  <span class="font-semibold">Lost contact with the manager</span> at{' '}
                  <span class="font-mono text-xs">{managerAddress.replace(/^https?:\/\//, '')}</span>
                  <Show when={lostSince()}>
                    {(since) => (
                      <>
                        {' '}
                        <RelativeTime value={new Date(since()).toISOString()} />
                      </>
                    )}
                  </Show>
                  . Retrying every few seconds — what is on screen may be stale.
                </span>
              </p>
              <Button size="xs" variant="ghost" icon="refresh" class="shrink-0 text-inherit" onClick={() => void probeManagerLink()}>
                Retry now
              </Button>
            </div>
          </Show>
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
              <Kbd>⌘K</Kbd>
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
              class="hidden rounded-md p-1.5 text-muted transition-colors hover:bg-surface2 hover:text-ink sm:inline-flex"
              onClick={() => setShortcutsOpen((value) => !value)}
              aria-label="Keyboard shortcuts"
              title="Keyboard shortcuts (?)"
            >
              <Icon name="keyboard" size={16} />
            </button>
            <button
              type="button"
              class="rounded-md p-1.5 text-muted transition-colors hover:bg-surface2 hover:text-ink"
              onClick={toggleTheme}
              aria-label={theme() === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
            >
              <Icon name={theme() === 'dark' ? 'sun' : 'moon'} size={16} />
            </button>
          </div>
          <ShortcutsHelp open={shortcutsOpen()} onClose={() => setShortcutsOpen(false)} />
          <Show when={commandPaletteOpen()}>
            <div
              class="fixed inset-0 z-50 flex items-start justify-center bg-black/50 p-4 pt-[12vh] backdrop-blur-[2px]"
              data-command-palette
              data-modal
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
                    placeholder="Jump to a page, job, worker, or recent page…"
                    class="w-full border-none bg-transparent p-0 text-sm text-ink placeholder:text-faint focus:outline-none focus:ring-0"
                    role="combobox"
                    aria-expanded="true"
                    aria-controls="command-palette-listbox"
                    aria-activedescendant={`command-palette-option-${commandPaletteSelected()}`}
                    aria-label="Search command palette"
                  />
                  <Kbd>esc</Kbd>
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
                            onClick={() => executeCommand(entry)}
                            class={cx(
                              'flex w-full items-center justify-between gap-3 rounded-md px-3 py-2 text-left transition-colors',
                              isActive() ? 'bg-brass/10' : 'hover:bg-surface2',
                            )}
                          >
                            <span class="flex min-w-0 items-center gap-2.5">
                              <Show when={entry.icon}>
                                <Icon name={entry.icon!} size={14} class={cx('shrink-0', isActive() ? 'text-brass' : 'text-faint')} />
                              </Show>
                              <span class="min-w-0">
                                <span class="block truncate font-medium text-ink">{entry.label}</span>
                                <span class="block truncate text-xs text-faint">{entry.description}</span>
                              </span>
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
                  <span class="ml-auto"><kbd class="font-mono">?</kbd> all shortcuts</span>
                </div>
              </div>
            </div>
          </Show>
          <main class="flex-1 min-w-0 px-4 py-6 md:px-8">
            <Show when={!isDetailRoute() && !isUnknownRoute()}>
              <PageHeader title={pageMeta().title} subtitle={pageMeta().subtitle} />
            </Show>
            <ConnectionGate>
              <RouteErrorBoundary>{props.children}</RouteErrorBoundary>
            </ConnectionGate>
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
      <Route path="/templates" component={Templates} />
      <Route path="/templates/:name" component={TemplateDetail} />
      <Route path="/jobs/:id" component={JobDetail} />
      <Route path="/jobs/:id/tasks/:taskName" component={TaskDetail} />
      <Route path="/jobs/:id/specs/:configId" component={JobSpecDetail} />
      <Route path="/services" component={Services} />
      <Route path="/services/:name" component={ServiceDetail} />
      <Route path="/network" component={Network} />
      <Route path="/volumes" component={Volumes} />
      <Route path="/volumes/:id" component={VolumeDetail} />
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
      <Route path="*" component={NotFound} />
    </Route>
  )
}

export default function App() {
  return <Router><AppRoutes /></Router>
}
