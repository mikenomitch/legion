import { A, Route, Router, useLocation, type RouteSectionProps } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import { managerClient, managerEndpoint } from './grpcClient'
import type { Job, LedgerNode, NodeMsg } from './gen/legion_pb'
import Homepage from './pages/Homepage'
import Jobs from './pages/Jobs'
import Manager from './pages/Manager'
import Nodes from './pages/Nodes'
import TestingUtils from './pages/TestingUtils'

const navItems = [
  { key: 'overview', label: 'Overview', path: '/', title: 'Ledger', copy: 'Live allocations flowing through the cluster ledger.' },
  { key: 'nodes', label: 'Nodes', path: '/nodes', title: 'Nodes', copy: 'Every node the manager knows about.' },
  { key: 'jobs', label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Jobs reported by the manager in real time.' },
  { key: 'manager', label: 'Manager', path: '/manager', title: 'Manager', copy: 'Manager config and controls.' },
  { key: 'testing', label: 'Testing Utils', path: '/testing', title: 'Testing Utilities', copy: 'Sandboxes and utilities for experiments.' },
] as const

const resolveInitialTheme = (): 'light' | 'dark' => {
  if (typeof window === 'undefined') return 'dark'
  const stored = localStorage.getItem('ui-theme')
  if (stored === 'light' || stored === 'dark') return stored
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

const baseContainer =
  'bg-neutral-50 text-neutral-900 dark:bg-gradient-to-br dark:from-neutral-950 dark:via-neutral-900 dark:to-neutral-950 dark:text-neutral-100'

const navButton = (active: boolean) =>
  `flex w-full items-center justify-between rounded border px-3 py-2 text-left text-sm font-semibold transition hover:-translate-y-0.5 ` +
  (active
    ? 'border-neutral-900 bg-neutral-900 text-white shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-300 dark:bg-neutral-100 dark:text-neutral-900 dark:shadow-[4px_4px_0_#0b0d12]'
    : 'border-neutral-200 bg-transparent text-neutral-700 shadow-[4px_4px_0_#d4d4d8] hover:border-neutral-400 hover:text-neutral-900 dark:border-neutral-800 dark:text-neutral-300 dark:shadow-[4px_4px_0_#0b0d12] dark:hover:border-neutral-500 dark:hover:text-white')

const Layout = (props: RouteSectionProps) => {
  const location = useLocation()
  const [isNavOpen, setIsNavOpen] = createSignal(false)
  const [theme, setTheme] = createSignal<'light' | 'dark'>(resolveInitialTheme())

  createEffect(() => {
    const root = document.documentElement
    const current = theme()
    root.classList.toggle('dark', current === 'dark')
    localStorage.setItem('ui-theme', current)
  })

  createEffect(() => {
    location.pathname
    setIsNavOpen(false)
  })

  const isActivePath = (path: string) => {
    const current = location.pathname.replace(/\/+$/, '') || '/'
    const target = path.replace(/\/+$/, '') || '/'
    return current === target || (target !== '/' && current.startsWith(target))
  }

  const activeNav = createMemo(() => navItems.find((item) => isActivePath(item.path)) ?? navItems[0])

  const toggleTheme = () => setTheme((prev) => (prev === 'dark' ? 'light' : 'dark'))

  return (
    <div class={`min-h-screen ${baseContainer}`}>
      <div class="flex min-h-screen">
        <aside
          class={`fixed inset-y-0 left-0 z-40 w-64 transform border-r border-neutral-200 bg-white/90 px-5 py-6 backdrop-blur-sm transition md:static md:translate-x-0 dark:border-neutral-800 dark:bg-neutral-950/80 ${
            isNavOpen() ? 'translate-x-0' : '-translate-x-full'
          } md:flex md:flex-col`}
          aria-label="Sidebar"
        >
          <div class="mb-8 flex items-center justify-between">
            <div class="text-lg font-semibold uppercase tracking-[0.18em]">Legion</div>
            <button
              type="button"
              class="rounded border border-neutral-200 p-2 text-neutral-700 shadow-sm transition hover:bg-neutral-100 md:hidden dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-900"
              onClick={() => setIsNavOpen(false)}
              aria-label="Close navigation"
            >
              ✕
            </button>
          </div>
          <nav class="flex flex-col gap-2">
            <For each={navItems}>
              {(item) => (
                <A href={item.path} class={navButton(isActivePath(item.path))} aria-current={isActivePath(item.path) ? 'page' : 'false'}>
                  <span>{item.label}</span>
                </A>
              )}
            </For>
          </nav>
          <div class="mt-auto pt-8 text-xs text-neutral-500 dark:text-neutral-400">
            <p class="uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-500">Manager</p>
            <p class="break-words font-mono text-neutral-700 dark:text-neutral-300">{managerEndpoint}</p>
          </div>
        </aside>

        <Show when={isNavOpen()}>
          <button
            type="button"
            class="fixed inset-0 z-30 bg-black/40 backdrop-blur-sm md:hidden"
            onClick={() => setIsNavOpen(false)}
            aria-label="Close navigation overlay"
          />
        </Show>

        <div class="flex flex-1 flex-col">
          <header class="flex flex-wrap items-center justify-between gap-3 border-b border-neutral-200 px-4 pb-4 pt-6 md:px-8 dark:border-neutral-800">
            <div class="flex items-center gap-3">
              <button
                type="button"
                class="flex h-10 w-10 items-center justify-center rounded border border-neutral-300 bg-white text-lg shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 md:hidden dark:border-neutral-700 dark:bg-neutral-900 dark:shadow-[3px_3px_0_#0b0d12]"
                onClick={() => setIsNavOpen(true)}
                aria-label="Open navigation"
              >
                ☰
              </button>
              <div>
                <p class="text-xs uppercase tracking-[0.16em] text-neutral-500 dark:text-neutral-400">{activeNav().title}</p>
                <h1 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-50">{activeNav().copy}</h1>
              </div>
            </div>
            <div class="flex flex-wrap items-center gap-3">
              <button
                type="button"
                class="flex h-10 w-10 items-center justify-center rounded border border-neutral-300 bg-white text-lg font-medium text-neutral-800 shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100 dark:shadow-[3px_3px_0_#0b0d12]"
                onClick={toggleTheme}
                aria-label={theme() === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
              >
                <span aria-hidden="true" class="inline-block grayscale">💡</span>
              </button>
            </div>
          </header>

          <main class="flex-1 px-4 py-6 md:px-8">{props.children ?? null}</main>
        </div>
      </div>
    </div>
  )
}

function App() {
  const [ledger] = createResource(async (): Promise<readonly LedgerNode[]> => {
    const response = await managerClient.getLedger({})
    return response.nodes
  })

  const [nodes] = createResource(async (): Promise<readonly NodeMsg[]> => {
    const response = await managerClient.getNodes({})
    return response.nodes
  })

  const [jobs] = createResource(async (): Promise<readonly Job[]> => {
    const response = await managerClient.listJobs({})
    return response.jobs
  })

  return (
    <Router>
      <Route path="/" component={Layout}>
        <Route path="/" component={() => <Homepage ledger={ledger} />} />
        <Route path="/nodes" component={() => <Nodes nodes={nodes} />} />
        <Route path="/jobs" component={() => <Jobs jobs={jobs} />} />
        <Route path="/manager" component={Manager} />
        <Route path="/testing" component={TestingUtils} />
        <Route path="*" component={() => <Homepage ledger={ledger} />} />
      </Route>
    </Router>
  )
}

export default App
