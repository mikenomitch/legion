import { For, Show, createEffect, createResource, createSignal } from 'solid-js'
import { managerClient, managerEndpoint } from './grpcClient'
import type { Job, LedgerNode, NodeMsg } from './gen/legion_pb'
import Homepage from './pages/Homepage'
import Jobs from './pages/Jobs'
import Manager from './pages/Manager'
import Nodes from './pages/Nodes'
import TestingUtils from './pages/TestingUtils'

type Theme = 'light' | 'dark'

const navItems = [
  { key: 'home', label: 'Homepage' },
  { key: 'nodes', label: 'Nodes' },
  { key: 'jobs', label: 'Jobs' },
  { key: 'manager', label: 'Manager' },
  { key: 'testing', label: 'Testing Utils' },
] as const

type Section = (typeof navItems)[number]['key']

const sectionTitle: Record<Section, string> = {
  home: 'Ledger',
  nodes: 'Nodes',
  jobs: 'Jobs',
  manager: 'Manager',
  testing: 'Testing Utilities',
}

const sectionCopy: Record<Section, string> = {
  home: 'Live allocations flowing through the cluster ledger.',
  nodes: 'Every node the manager knows about.',
  jobs: 'Jobs reported by the manager in real time.',
  manager: 'Reserved for manager-specific controls.',
  testing: 'Sandboxes and utilities for experiments.',
}

const resolveInitialTheme = (): Theme => {
  if (typeof window === 'undefined') return 'dark'
  const stored = localStorage.getItem('ui-theme') as Theme | null
  if (stored === 'light' || stored === 'dark') return stored
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

function App() {
  const [section, setSection] = createSignal<Section>('home')
  const [theme, setTheme] = createSignal<Theme>(resolveInitialTheme())

  createEffect(() => {
    const root = document.documentElement
    const current = theme()
    root.classList.toggle('dark', current === 'dark')
    localStorage.setItem('ui-theme', current)
  })

  const toggleTheme = () => setTheme((prev) => (prev === 'dark' ? 'light' : 'dark'))

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

  const baseContainer =
    'flex min-h-screen bg-neutral-50 text-neutral-900 dark:bg-gradient-to-br dark:from-neutral-950 dark:via-neutral-900 dark:to-neutral-950 dark:text-neutral-100'

  const navButton = (active: boolean) =>
    `flex w-full items-center justify-between rounded border px-3 py-2 text-left text-sm font-semibold transition hover:-translate-y-0.5 ` +
    (active
      ? 'border-neutral-900 bg-neutral-900 text-white shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-300 dark:bg-neutral-100 dark:text-neutral-900 dark:shadow-[4px_4px_0_#0b0d12]'
      : 'border-neutral-200 bg-transparent text-neutral-700 shadow-[4px_4px_0_#d4d4d8] hover:border-neutral-400 hover:text-neutral-900 dark:border-neutral-800 dark:text-neutral-300 dark:shadow-[4px_4px_0_#0b0d12] dark:hover:border-neutral-500 dark:hover:text-white')

  return (
    <div class={baseContainer}>
      <aside class="flex w-60 flex-col border-r border-neutral-200 bg-white/90 px-5 py-6 backdrop-blur-sm dark:border-neutral-800 dark:bg-neutral-950/80">
        <div class="mb-8">
          <div class="text-lg font-semibold uppercase tracking-[0.18em]">Legion</div>
        </div>
        <nav class="flex flex-col gap-2">
          <For each={navItems}>
            {(item) => (
              <button type="button" class={navButton(section() === item.key)} onClick={() => setSection(item.key)}>
                <span>{item.label}</span>
              </button>
            )}
          </For>
        </nav>
        <div class="mt-auto pt-8 text-xs text-neutral-500 dark:text-neutral-400">
          <p class="uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-500">Manager</p>
          <p class="break-words font-mono text-neutral-700 dark:text-neutral-300">{managerEndpoint}</p>
        </div>
      </aside>

      <main class="flex-1 px-8 py-8">
        <header class="mb-6 flex flex-wrap items-center justify-between gap-3 border-b border-neutral-200 pb-4 dark:border-neutral-800">
          <div>
            <p class="text-xs uppercase tracking-[0.16em] text-neutral-500 dark:text-neutral-400">{sectionTitle[section()]}</p>
            <h1 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-50">{sectionCopy[section()]}</h1>
          </div>
          <div class="flex flex-wrap items-center gap-3">
            <button
              type="button"
              class="flex items-center gap-2 rounded border border-neutral-300 bg-white px-3 py-2 text-xs font-medium text-neutral-800 shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100 dark:shadow-[3px_3px_0_#0b0d12]"
              onClick={toggleTheme}
            >
              <span aria-hidden="true">{theme() === 'dark' ? '🌙' : '☀️'}</span>
              <span class="uppercase tracking-[0.08em]">{theme() === 'dark' ? 'Dark mode' : 'Light mode'}</span>
            </button>
            <div class="rounded border border-neutral-300 bg-white px-3 py-2 text-xs text-neutral-700 shadow-[3px_3px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-300 dark:shadow-[3px_3px_0_#0b0d12]">
              Live gRPC over {managerEndpoint.replace(/^https?:\/\//, '')}
            </div>
          </div>
        </header>

        <Show when={section() === 'home'}>
          <Homepage ledger={ledger} />
        </Show>

        <Show when={section() === 'nodes'}>
          <Nodes nodes={nodes} />
        </Show>

        <Show when={section() === 'jobs'}>
          <Jobs jobs={jobs} />
        </Show>

        <Show when={section() === 'manager'}>
          <Manager />
        </Show>

        <Show when={section() === 'testing'}>
          <TestingUtils />
        </Show>
      </main>
    </div>
  )
}

export default App
