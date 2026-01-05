import { For, Show, createResource, createSignal } from 'solid-js'
import { managerClient, managerEndpoint } from './grpcClient'
import type { Job, LedgerNode, NodeMsg } from './gen/legion_pb'

const navItems = [
  { key: 'home', label: 'Homepage' },
  { key: 'nodes', label: 'Nodes' },
  { key: 'jobs', label: 'Jobs' },
  { key: 'manager', label: 'Manager' },
  { key: 'testing', label: 'Testing Utils' },
] as const

type Section = (typeof navItems)[number]['key']

const formatDate = (timestamp?: bigint): string => {
  if (!timestamp) return '—'
  const millis = Number(timestamp) * 1000
  return new Intl.DateTimeFormat('en', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(millis))
}

const asDisplay = (value?: string | bigint): string => {
  if (value === undefined || value === null) return '—'
  if (typeof value === 'bigint') return value.toString()
  return value === '' ? '—' : value
}

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

function App() {
  const [section, setSection] = createSignal<Section>('home')

  const [ledger] = createResource(async () => {
    const response = await managerClient.getLedger({})
    return response.nodes
  })

  const [nodes] = createResource(async () => {
    const response = await managerClient.getNodes({})
    return response.nodes
  })

  const [jobs] = createResource(async () => {
    const response = await managerClient.listJobs({})
    return response.jobs
  })

  const renderLedger = (entries: readonly LedgerNode[]) => (
    <div class="space-y-4">
      <For each={entries}>
        {(item) => (
          <article class="rounded border border-neutral-800 bg-neutral-900/60 p-4 shadow-[6px_6px_0_#0b0d12]">
            <header class="flex items-center justify-between gap-4">
              <div>
                <p class="text-xs uppercase tracking-[0.12em] text-neutral-500">Node</p>
                <div class="font-semibold text-neutral-100">{item.node?.id ?? 'Unknown node'}</div>
              </div>
              <span class="rounded border border-neutral-700 px-3 py-1 text-xs uppercase tracking-wide text-neutral-200">
                {item.node?.status ?? 'N/A'}
              </span>
            </header>
            <div class="mt-3 grid grid-cols-2 gap-2 text-sm text-neutral-300 md:grid-cols-3">
              <span class="rounded border border-neutral-800 bg-neutral-900 px-3 py-2">Pool: {item.node?.pool ?? '—'}</span>
              <span class="rounded border border-neutral-800 bg-neutral-900 px-3 py-2">Availability: {item.node?.availability ?? '—'}</span>
              <span class="rounded border border-neutral-800 bg-neutral-900 px-3 py-2">Last heartbeat: {item.node?.lastHeartbeat ?? '—'}</span>
            </div>
            <div class="mt-4">
              <p class="text-xs uppercase tracking-[0.12em] text-neutral-500">Allocations</p>
              <div class="mt-2 space-y-2">
                <Show when={item.allocations.length} fallback={<p class="rounded border border-dashed border-neutral-800 px-3 py-2 text-sm text-neutral-500">No allocations recorded.</p>}>
                  <For each={item.allocations}>
                    {(allocation) => (
                      <div class="rounded border border-neutral-800 bg-neutral-950/60 p-3">
                        <div class="flex flex-wrap items-center justify-between gap-2 text-sm text-neutral-100">
                          <span class="font-semibold">Allocation #{asDisplay(allocation.id)}</span>
                          <span class="rounded border border-neutral-700 px-2 py-1 text-[11px] uppercase tracking-wide">{allocation.status}</span>
                        </div>
                        <div class="mt-2 grid gap-2 text-xs text-neutral-400 md:grid-cols-3">
                          <span class="rounded border border-neutral-800 bg-neutral-900 px-2 py-1">Resources: {asDisplay(allocation.resources)}</span>
                          <span class="rounded border border-neutral-800 bg-neutral-900 px-2 py-1">Job Config: {asDisplay(allocation.jobConfigId)}</span>
                          <span class="rounded border border-neutral-800 bg-neutral-900 px-2 py-1">Task Config: {asDisplay(allocation.taskConfigId)}</span>
                        </div>
                      </div>
                    )}
                  </For>
                </Show>
              </div>
            </div>
          </article>
        )}
      </For>
    </div>
  )

  const renderNodes = (items: readonly NodeMsg[]) => (
    <div class="grid gap-3 md:grid-cols-2">
      <For each={items}>
        {(node) => (
          <article class="rounded border border-neutral-800 bg-neutral-900/60 p-4 shadow-[5px_5px_0_#0b0d12]">
            <div class="flex items-start justify-between">
              <div>
                <p class="text-xs uppercase tracking-[0.12em] text-neutral-500">Node</p>
                <p class="text-lg font-semibold text-neutral-100">{node.id}</p>
                <p class="text-sm text-neutral-400">Pool: {node.pool || '—'}</p>
              </div>
              <span class="rounded border border-neutral-700 px-2 py-1 text-[11px] uppercase tracking-wide">{node.status}</span>
            </div>
            <div class="mt-3 space-y-1 text-sm text-neutral-300">
              <p class="rounded border border-neutral-800 bg-neutral-950/60 px-2 py-1">Availability: {node.availability || '—'}</p>
              <p class="rounded border border-neutral-800 bg-neutral-950/60 px-2 py-1">Config: {node.configAttrs || '—'}</p>
              <p class="rounded border border-neutral-800 bg-neutral-950/60 px-2 py-1">Fingerprint: {node.fingerprintAttrs || '—'}</p>
              <p class="rounded border border-neutral-800 bg-neutral-950/60 px-2 py-1">Dynamic: {node.dynamicAttrs || '—'}</p>
              <p class="rounded border border-neutral-800 bg-neutral-950/60 px-2 py-1">Last heartbeat: {node.lastHeartbeat || '—'}</p>
            </div>
          </article>
        )}
      </For>
    </div>
  )

  const renderJobs = (items: readonly Job[]) => (
    <div class="space-y-3">
      <For each={items}>
        {(job) => (
          <article class="rounded border border-neutral-800 bg-neutral-900/60 p-4 shadow-[5px_5px_0_#0b0d12]">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p class="text-xs uppercase tracking-[0.12em] text-neutral-500">Job</p>
                <p class="text-lg font-semibold text-neutral-50">{job.name || job.id}</p>
                <p class="text-sm text-neutral-400">Created {formatDate(job.createdAt)}</p>
              </div>
              <span class="rounded border border-neutral-700 px-3 py-1 text-[11px] uppercase tracking-wide">{job.status || 'Unknown'}</span>
            </div>
          </article>
        )}
      </For>
    </div>
  )

  return (
    <div class="flex min-h-screen bg-gradient-to-br from-neutral-950 via-neutral-900 to-neutral-950 text-neutral-100">
      <aside class="flex w-60 flex-col border-r border-neutral-800 bg-neutral-950/80 px-5 py-6">
        <div class="mb-8 space-y-1">
          <div class="text-lg font-semibold uppercase tracking-[0.18em]">Khepri</div>
          <p class="text-xs text-neutral-500">Minimal control surface for the manager.</p>
        </div>
        <nav class="flex flex-col gap-2">
          <For each={navItems}>
            {(item) => (
              <button
                type="button"
                class={`flex w-full items-center justify-between rounded border px-3 py-2 text-left text-sm transition hover:-translate-y-0.5 hover:border-neutral-500 hover:text-white ${
                  section() === item.key
                    ? 'border-neutral-300 bg-neutral-100 text-neutral-900 shadow-[4px_4px_0_#0b0d12]'
                    : 'border-neutral-800 bg-transparent text-neutral-300 shadow-[4px_4px_0_#0b0d12]'
                }`}
                onClick={() => setSection(item.key)}
              >
                <span class="font-semibold">{item.label}</span>
                <span class="text-[10px] uppercase tracking-wide text-neutral-500">{sectionTitle[item.key]}</span>
              </button>
            )}
          </For>
        </nav>
        <div class="mt-auto pt-8 text-xs text-neutral-500">
          <p class="uppercase tracking-[0.12em] text-neutral-600">Manager</p>
          <p class="break-words font-mono text-neutral-300">{managerEndpoint}</p>
        </div>
      </aside>

      <main class="flex-1 px-8 py-8">
        <header class="mb-6 flex flex-wrap items-center justify-between gap-3 border-b border-neutral-800 pb-4">
          <div>
            <p class="text-xs uppercase tracking-[0.16em] text-neutral-500">{sectionTitle[section()]}</p>
            <h1 class="text-2xl font-semibold text-neutral-50">{sectionCopy[section()]}</h1>
          </div>
          <div class="rounded border border-neutral-800 bg-neutral-900 px-3 py-2 text-xs text-neutral-400">
            Live gRPC over {managerEndpoint.replace(/^https?:\/\//, '')}
          </div>
        </header>

        <Show when={section() === 'home'}>
          <Show when={!ledger.loading} fallback={<p class="text-neutral-500">Loading ledger…</p>}>
            <Show when={!ledger.error} fallback={<p class="text-red-400">Failed to load ledger.</p>}>
              {ledger() && ledger()!.length > 0 ? (
                renderLedger(ledger()!)
              ) : (
                <p class="text-neutral-500">No ledger entries available.</p>
              )}
            </Show>
          </Show>
        </Show>

        <Show when={section() === 'nodes'}>
          <Show when={!nodes.loading} fallback={<p class="text-neutral-500">Loading nodes…</p>}>
            <Show when={!nodes.error} fallback={<p class="text-red-400">Failed to load nodes.</p>}>
              {nodes() && nodes()!.length > 0 ? (
                renderNodes(nodes()!)
              ) : (
                <p class="text-neutral-500">No nodes discovered yet.</p>
              )}
            </Show>
          </Show>
        </Show>

        <Show when={section() === 'jobs'}>
          <Show when={!jobs.loading} fallback={<p class="text-neutral-500">Loading jobs…</p>}>
            <Show when={!jobs.error} fallback={<p class="text-red-400">Failed to load jobs.</p>}>
              {jobs() && jobs()!.length > 0 ? (
                renderJobs(jobs()!)
              ) : (
                <p class="text-neutral-500">No jobs reported.</p>
              )}
            </Show>
          </Show>
        </Show>

        <Show when={section() === 'manager'}>
          <div class="rounded border border-dashed border-neutral-800 bg-neutral-900/40 p-6 text-neutral-400">
            Manager-specific controls will live here.
          </div>
        </Show>

        <Show when={section() === 'testing'}>
          <div class="rounded border border-dashed border-neutral-800 bg-neutral-900/40 p-6 text-neutral-400">
            Testing utilities coming soon.
          </div>
        </Show>
      </main>
    </div>
  )
}

export default App
