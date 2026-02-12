import { A, Route, Router, useLocation, type RouteSectionProps } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import type { LedgerNode, Rollout } from './gen/legion_pb'
import Breadcrumbs from './components/Breadcrumbs'
import { managerClient, managerEndpoint } from './grpcClient'
import Deployments from './pages/Deployments'
import JobDetail from './pages/details/JobDetail'
import JobSpecDetail from './pages/details/JobSpecDetail'
import ManagerNodeDetail from './pages/details/ManagerNodeDetail'
import NodeDetail from './pages/details/NodeDetail'
import RolloutDetail from './pages/details/RolloutDetail'
import Homepage from './pages/Homepage'
import Jobs from './pages/Jobs'
import KeyValue from './pages/KeyValue'
import Manager from './pages/Manager'
import Nodes from './pages/Nodes'
import Rollouts from './pages/Rollouts'
import ApiExplorer from './pages/ApiExplorer'
import TestingUtils from './pages/TestingUtils'

const navItems = [
  { label: 'Overview', path: '/', title: 'Overview', copy: 'Jobs running on the cluster.' },
  { label: 'Workers', path: '/nodes', title: 'Workers', copy: 'Inventory of workers.' },
  { label: 'Managers', path: '/manager', title: 'Managers', copy: 'Manager config snapshots.' },
  { label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Jobs and their health status.' },
  { label: 'Rollouts', path: '/rollouts', title: 'Rollouts', copy: 'Rollout execution details.' },
  { label: 'Key/Value', path: '/kv', title: 'Key/Value', copy: 'Browse and edit key/value entries.' },
  { label: 'API', path: '/api', title: 'API', copy: 'Interactive gRPC explorer.' },
] as const

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
  const [isNavOpen, setIsNavOpen] = createSignal(false)
  const [theme, setTheme] = createSignal<'light' | 'dark'>(resolveInitialTheme())

  createEffect(() => {
    location.pathname
    setIsNavOpen(false)
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
    const topLevel = navItems.find((item) => item.path === topLevelPath) ?? navItems[0]
    const title =
      segments.length <= 1
        ? topLevel.title
        : decodePathSegment(segments[segments.length - 1])

    return {
      title,
      subtitle: topLevel.copy,
    }
  })

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
            <For each={navItems}>{(item) => <A href={item.path} class={`block rounded border px-3 py-2 text-sm font-semibold ${location.pathname === item.path || (item.path !== '/' && location.pathname.startsWith(item.path)) ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-200 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300'}`}>{item.label}</A>}</For>
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
          <button
            type="button"
            class="absolute right-4 top-4 z-20 flex h-9 w-9 items-center justify-center rounded border bg-white/90 text-base dark:border-neutral-700 dark:bg-neutral-900/80"
            onClick={() => setTheme((value) => (value === 'dark' ? 'light' : 'dark'))}
            aria-label={theme() === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
          >
            {theme() === 'dark' ? '☀' : '☾'}
          </button>
          <main class="flex-1 min-w-0 px-4 py-6 md:px-8">
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
            <section class="mb-5 rounded border border-neutral-200 bg-white/90 px-4 py-3 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h1 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-100">{pageMeta().title}</h1>
              <p class="mt-1 text-sm text-neutral-600 dark:text-neutral-300">{pageMeta().subtitle}</p>
            </section>
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
      <Route path="/manager/:id" component={ManagerNodeDetail} />
      <Route path="/jobs" component={Jobs} />
      <Route path="/jobs/:id" component={JobDetail} />
      <Route path="/jobs/:id/specs/:configId" component={JobSpecDetail} />
      <Route path="/deployments" component={Deployments} />
      <Route path="/rollouts" component={Rollouts} />
      <Route path="/rollouts/:id" component={RolloutDetail} />
      <Route path="/kv" component={KeyValue} />
      <Route path="/api" component={ApiExplorer} />
      <Route path="/testing" component={TestingUtils} />
      <Route path="*" component={() => <Homepage ledger={ledger} rollouts={rollouts} />} />
    </Route>
  )
}

export default function App() {
  return <Router><AppRoutes /></Router>
}
