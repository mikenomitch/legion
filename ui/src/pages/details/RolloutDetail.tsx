import { useParams } from '@solidjs/router'
import { Show, createResource } from 'solid-js'
import { managerClient } from '../../grpcClient'

export default function RolloutDetail() {
  const params = useParams()
  const [data] = createResource(async () => {
    const id = BigInt(params.id)
    const [rollout, events] = await Promise.all([
      managerClient.getRolloutStatus({ id }),
      managerClient.listRolloutEvents({ rolloutId: id, limit: 200 }),
    ])
    return { rollout, events: events.events }
  })

  return (
    <Show when={!data.loading} fallback={<p>Loading rollout…</p>}>
      <Show when={!data.error && data()} fallback={<p class="text-rose-500">Failed to load rollout.</p>}>
        <article class="space-y-3 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-xl font-semibold">Rollout #{params.id}</h2>
          <pre class="max-h-[520px] overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{JSON.stringify(data(), null, 2)}</pre>
        </article>
      </Show>
    </Show>
  )
}
