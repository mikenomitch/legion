import { useParams } from '@solidjs/router'
import { Show, createResource } from 'solid-js'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'

export default function NodeDetail() {
  const params = useParams()
  const [node] = createResource(async () => {
    const response = await managerClient.getNode({ id: decodeURIComponent(params.id) })
    return response.node
  })

  return (
    <Show when={!node.loading} fallback={<p>Loading node…</p>}>
      <Show when={!node.error && node()} fallback={<p class="text-rose-500">Failed to load node.</p>}>
        <article class="space-y-3 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-xl font-semibold">{node()!.id}</h2>
          <p class="text-sm text-neutral-500">Last heartbeat {formatDate(node()!.lastHeartbeat)}</p>
          <pre class="max-h-[520px] overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{JSON.stringify(node(), null, 2)}</pre>
        </article>
      </Show>
    </Show>
  )
}
