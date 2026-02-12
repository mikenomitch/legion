import { For, Show, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'

export default function Homepage() {
  const [ledger] = createResource(async () => {
    const response = await managerClient.getLedger({})
    return response.nodes
  })

  const [rollouts] = createResource(async () => {
    const response = await managerClient.listRollouts({ activeOnly: true })
    return response.rollouts
  })

  return (
    <Show when={!ledger.loading} fallback={<p class="text-neutral-500">Loading overview…</p>}>
      <section class="space-y-4">
        <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-lg font-semibold">Cluster overview</h2>
          <p class="text-sm text-neutral-500">{ledger()?.length ?? 0} nodes · {rollouts()?.length ?? 0} active rollouts</p>
        </article>
        <For each={ledger() ?? []}>
          {(entry) => (
            <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <p class="font-semibold">{entry.node?.id || 'unknown node'}</p>
              <p class="text-sm text-neutral-500">{entry.node?.status || 'unknown'} · {entry.allocations.length} allocations</p>
            </article>
          )}
        </For>
      </section>
    </Show>
  )
}
