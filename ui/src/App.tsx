import { A, Route, Router, useLocation, type RouteSectionProps } from '@solidjs/router'
import { For, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import type { LedgerNode, Rollout } from './gen/legion_pb'
import { managerClient, managerEndpoint } from './grpcClient'
import DeploymentDetail from './pages/details/DeploymentDetail'
import JobDetail from './pages/details/JobDetail'
import ManagerNodeDetail from './pages/details/ManagerNodeDetail'
import NodeDetail from './pages/details/NodeDetail'
import RolloutDetail from './pages/details/RolloutDetail'
import Deployments from './pages/Deployments'
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
  { label: 'Nodes', path: '/nodes', title: 'Nodes', copy: 'Inventory of worker nodes.' },
  { label: 'Manager Nodes', path: '/manager', title: 'Manager Nodes', copy: 'Manager config snapshots.' },
  { label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Jobs and their health status.' },
  { label: 'Deployments', path: '/deployments', title: 'Deployments', copy: 'Deployment records from rollout history.' },
  { label: 'Rollouts', path: '/rollouts', title: 'Rollouts', copy: 'Rollout execution details.' },
  { label: 'Key/Value', path: '/kv', title: 'Key/Value', copy: 'Browse and edit key/value entries.' },
  { label: 'API', path: '/api', title: 'API', copy: 'Interactive gRPC explorer.' },
  { label: 'Testing', path: '/testing', title: 'Testing', copy: 'Testing utilities and sandboxes.' },
] as const

const baseContainer =
  'bg-neutral-50 text-neutral-900 dark:bg-gradient-to-br dark:from-neutral-950 dark:via-neutral-900 dark:to-neutral-950 dark:text-neutral-100'

const navButton = (active: boolean) =>
  `flex w-full items-center justify-between rounded border px-3 py-2 text-left text-sm font-semibold transition ${
    active
      ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-300 dark:bg-neutral-100 dark:text-neutral-900'
      : 'border-neutral-200 bg-transparent text-neutral-700 hover:border-neutral-400 dark:border-neutral-800 dark:text-neutral-300'
  }`

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
    const current = theme()
    document.documentElement.classList.toggle('dark', current === 'dark')
    localStorage.setItem('ui-theme', current)
  })

  const activeNav = createMemo(() => {
    const current = location.pathname.replace(/\/+$/, '') || '/'
    return navItems.find((item) => {
      const target = item.path.replace(/\/+$/, '') || '/'
      return current === target || (target !== '/' && current.startsWith(target))
    }) || navItems[0]
  })

  return (
    <div class={`min-h-screen ${baseContainer}`}>
      <div class="flex min-h-screen">
        <aside
          class={`fixed inset-y-0 left-0 z-40 flex w-64 transform flex-col border-r border-neutral-200 bg-white/90 px-5 py-6 transition md:static md:translate-x-0 dark:border-neutral-800 dark:bg-neutral-950/80 ${
            isNavOpen() ? 'translate-x-0' : '-translate-x-full'
          }`}
        >
          <A href="/" class="mb-6 text-lg font-semibold uppercase tracking-[0.18em]">
            Legion
          </A>
          <nav class="space-y-2">
            <For each={navItems}>
              {(item) => (
                <A href={item.path} class={navButton(location.pathname === item.path || (item.path !== '/' && location.pathname.startsWith(item.path)))}>
                  {item.label}
                </A>
              )}
            </For>
          </nav>
          <p class="mt-auto pt-6 text-xs text-neutral-500">Manager endpoint {managerEndpoint}</p>
        </aside>
        <div class="flex min-h-screen flex-1 flex-col">
          <header class="flex items-center justify-between border-b border-neutral-200 px-4 py-4 dark:border-neutral-800">
            <button type="button" class="rounded border px-3 py-2 md:hidden" onClick={() => setIsNavOpen((v) => !v)}>
              ☰
            </button>
            <div>
              <p class="text-xs uppercase tracking-[0.16em] text-neutral-500">{activeNav().title}</p>
              <h1 class="text-xl font-semibold">{activeNav().copy}</h1>
            </div>
            <button type="button" class="rounded border px-3 py-2" onClick={() => setTheme((v) => (v === 'dark' ? 'light' : 'dark'))}>
              {theme() === 'dark' ? '☾' : '☀'}
            </button>
          </header>
          <main class="flex-1 px-4 py-6 md:px-8">{props.children}</main>
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
      <Route path="/deployments" component={Deployments} />
      <Route path="/deployments/:id" component={DeploymentDetail} />
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
  return (
    <Router>
      <AppRoutes />
    </Router>
  )
}
