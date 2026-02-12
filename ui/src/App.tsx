import { A, Route, Router, useLocation, type RouteSectionProps } from '@solidjs/router'
import { For, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import type { LedgerNode, Rollout } from './gen/legion_pb'
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
import TestingUtils from './pages/TestingUtils'

const navItems = [
  { label: 'Overview', path: '/', title: 'Overview', copy: 'Jobs running on the cluster.' },
  { label: 'Nodes', path: '/nodes', title: 'Nodes', copy: 'Inventory of worker nodes.' },
  { label: 'Manager Nodes', path: '/manager', title: 'Manager Nodes', copy: 'Manager config snapshots.' },
  { label: 'Jobs', path: '/jobs', title: 'Jobs', copy: 'Jobs and their health status.' },
  { label: 'Rollouts', path: '/rollouts', title: 'Rollouts', copy: 'Rollout execution details.' },
  { label: 'Key/Value', path: '/kv', title: 'Key/Value', copy: 'Browse and edit key/value entries.' },
  { label: 'Testing', path: '/testing', title: 'Testing', copy: 'Testing utilities and sandboxes.' },
] as const

const Layout = (props: RouteSectionProps) => {
  const location = useLocation()
  const [isNavOpen, setIsNavOpen] = createSignal(false)

  createEffect(() => {
    location.pathname
    setIsNavOpen(false)
  })

  const activeNav = createMemo(() => {
    const current = location.pathname.replace(/\/+$/, '') || '/'
    return navItems.find((item) => current === item.path || (item.path !== '/' && current.startsWith(item.path))) ?? navItems[0]
  })

  return (
    <div class="min-h-screen bg-neutral-50 text-neutral-900 dark:bg-neutral-950 dark:text-neutral-100">
      <div class="flex min-h-screen">
        <aside class={`fixed inset-y-0 left-0 z-40 w-64 shrink-0 transform border-r border-neutral-200 bg-white/95 px-5 py-6 transition md:sticky md:top-0 md:h-screen md:translate-x-0 dark:border-neutral-800 dark:bg-neutral-950 ${isNavOpen() ? 'translate-x-0' : '-translate-x-full'}`}>
          <A href="/" class="mb-6 block text-lg font-semibold uppercase tracking-[0.18em]">Legion</A>
          <nav class="space-y-2">
            <For each={navItems}>{(item) => <A href={item.path} class={`block rounded border px-3 py-2 text-sm font-semibold ${location.pathname === item.path || (item.path !== '/' && location.pathname.startsWith(item.path)) ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-200 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300'}`}>{item.label}</A>}</For>
          </nav>
          <p class="mt-auto pt-6 text-xs text-neutral-500">Manager endpoint {managerEndpoint}</p>
        </aside>
        <div class="flex min-h-screen min-w-0 flex-1 flex-col md:ml-0">
          <header class="flex items-center justify-between border-b border-neutral-200 px-4 py-4 dark:border-neutral-800">
            <button type="button" class="rounded border px-3 py-2 md:hidden" onClick={() => setIsNavOpen((value) => !value)}>☰</button>
            <div class="min-w-0">
              <p class="text-xs uppercase tracking-[0.16em] text-neutral-500">{activeNav().title}</p>
              <h1 class="truncate text-xl font-semibold">{activeNav().copy}</h1>
            </div>
          </header>
          <main class="flex-1 min-w-0 px-4 py-6 md:px-8">{props.children}</main>
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
      <Route path="/testing" component={TestingUtils} />
      <Route path="*" component={() => <Homepage ledger={ledger} rollouts={rollouts} />} />
    </Route>
  )
}

export default function App() {
  return <Router><AppRoutes /></Router>
}
