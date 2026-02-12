import { useParams } from '@solidjs/router'
import { Show, createResource } from 'solid-js'
import { managerClient } from '../../grpcClient'

export default function ManagerNodeDetail() {
  const params = useParams()
  const [data] = createResource(async () => {
    const response = await managerClient.getManagerConfigs({ query: decodeURIComponent(params.id), page: 1, pageSize: 1 })
    return response.configs[0]
  })

  return (
    <Show when={!data.loading} fallback={<p>Loading manager…</p>}>
      <Show when={!data.error && data()} fallback={<p class="text-rose-500">Failed to load manager.</p>}>
        <article class="space-y-3 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-2xl font-semibold">{data()!.nodeId}</h2>
          <pre class="max-h-[520px] overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{JSON.stringify(data(), null, 2)}</pre>
        </article>
      </Show>
    </Show>
  )
}
