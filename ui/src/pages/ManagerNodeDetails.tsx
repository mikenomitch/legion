import { useParams } from '@solidjs/router'
import { Show, createMemo, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'

export default function ManagerNodeDetails() {
  const params = useParams()
  const key = () => decodeURIComponent(params.id || '')

  const [configs] = createResource(async () => {
    const response = await managerClient.getManagerConfigs({})
    return response.configs
  })

  const config = createMemo(() => (configs() ?? []).find((entry) => entry.nodeId === key()))

  return (
    <section class="space-y-4">
      <h2 class="text-xl font-semibold">Manager Node: {key()}</h2>
      <Show when={!configs.loading} fallback={<p>Loading manager node…</p>}>
        <Show when={!configs.error && config()} fallback={<p class="text-red-500">Manager node not found.</p>}>
          <pre class="overflow-auto rounded border border-neutral-200 bg-white/90 p-4 text-xs dark:border-neutral-800 dark:bg-neutral-900/60">{config()?.configJson}</pre>
        </Show>
      </Show>
    </section>
  )
}
