import { useParams } from '@solidjs/router'
import { Show, createMemo, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'
import { fetchNode } from './indexData'

const parseJson = (value: string): string => {
  try {
    return JSON.stringify(JSON.parse(value), null, 2)
  } catch {
    return value
  }
}

export default function NodeDetail() {
  const params = useParams<{ id: string }>()
  const nodeId = createMemo(() => decodeURIComponent(params.id || ''))

  const [node] = createResource(nodeId, async (id) => fetchNode(id))
  const [ledger] = createResource(nodeId, async () => {
    const response = await managerClient.getLedger({})
    return response.nodes.find((entry) => entry.node?.id === nodeId())
  })

  return (
    <Show when={!node.loading} fallback={<p class="text-neutral-500">Loading node…</p>}>
      <Show when={node()} fallback={<p class="text-neutral-500">Node not found.</p>}>
        {(entry) => (
          <section class="space-y-4">
            <header class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h2 class="text-xl font-semibold">{entry().id}</h2>
              <p class="text-sm text-neutral-500">Status {entry().status || 'unknown'} · Updated {entry().lastHeartbeat || '—'}</p>
            </header>
            <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Allocations</h3>
              <p class="text-sm text-neutral-500">{ledger()?.allocations.length ?? 0} allocations on this node.</p>
            </article>
            <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Details</h3>
              <pre class="mt-2 max-h-96 overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{parseJson(entry().configAttrs)}</pre>
            </article>
          </section>
        )}
      </Show>
    </Show>
  )
}
