import { useParams } from '@solidjs/router'
import { Show, createMemo, createResource } from 'solid-js'
import { fetchManagerNodeConfig } from './indexData'

const prettify = (value: string): string => {
  try {
    return JSON.stringify(JSON.parse(value), null, 2)
  } catch {
    return value
  }
}

export default function ManagerNodeDetail() {
  const params = useParams<{ id: string }>()
  const nodeId = createMemo(() => decodeURIComponent(params.id || ''))
  const [config] = createResource(nodeId, fetchManagerNodeConfig)

  return (
    <Show when={!config.loading} fallback={<p class="text-neutral-500">Loading manager node…</p>}>
      <Show when={config()} fallback={<p class="text-neutral-500">Manager node not found.</p>}>
        {(entry) => (
          <section class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <h2 class="text-xl font-semibold">{entry().nodeId}</h2>
            <p class="text-sm text-neutral-500">Updated {entry().updatedAt || '—'}</p>
            <pre class="max-h-96 overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{prettify(entry().configJson)}</pre>
          </section>
        )}
      </Show>
    </Show>
  )
}
