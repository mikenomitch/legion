import { useParams } from '@solidjs/router'
import { Show, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'

export default function NodeDetails() {
  const params = useParams()
  const nodeId = () => decodeURIComponent(params.id || '')
  const [node] = createResource(nodeId, async (id) => managerClient.getNode({ id }))

  return (
    <section class="space-y-4">
      <h2 class="text-xl font-semibold">Node: {nodeId()}</h2>
      <Show when={!node.loading} fallback={<p>Loading node details…</p>}>
        <Show when={!node.error && node()?.node} fallback={<p class="text-red-500">Failed to load node details.</p>}>
          <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <p><strong>Status:</strong> {node()?.node?.status || 'unknown'}</p>
            <p><strong>Pool:</strong> {node()?.node?.pool || '—'}</p>
            <p><strong>Availability:</strong> {node()?.node?.availability || '—'}</p>
            <p><strong>Last heartbeat:</strong> {formatRelativeTime(node()?.node?.lastHeartbeat)}</p>
          </article>
        </Show>
      </Show>
    </section>
  )
}
