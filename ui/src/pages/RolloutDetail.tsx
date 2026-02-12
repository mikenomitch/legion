import { useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import { fetchRollout } from './indexData'

export default function RolloutDetail() {
  const params = useParams<{ id: string }>()
  const rolloutId = createMemo(() => {
    try {
      return BigInt(params.id || '0')
    } catch {
      return BigInt(0)
    }
  })
  const [advanceBusy, setAdvanceBusy] = createSignal(false)
  const [message, setMessage] = createSignal('')

  const [rollout, { refetch }] = createResource(rolloutId, fetchRollout)
  const [status] = createResource(rolloutId, (id) => managerClient.getRolloutStatus({ id }))
  const [events] = createResource(rolloutId, (id) => managerClient.listRolloutEvents({ rolloutId: id, limit: 200 }))

  const advance = async () => {
    if (!rollout()) return
    setAdvanceBusy(true)
    setMessage('')
    try {
      const response = await managerClient.advanceRollout({ id: rolloutId(), groups: [] })
      setMessage(response.message || 'Advance queued')
      await refetch()
    } catch {
      setMessage('Failed to advance rollout')
    } finally {
      setAdvanceBusy(false)
    }
  }

  return (
    <Show when={!rollout.loading} fallback={<p class="text-neutral-500">Loading rollout…</p>}>
      <Show when={rollout()} fallback={<p class="text-neutral-500">Rollout not found.</p>}>
        {(entry) => (
          <section class="space-y-4">
            <header class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h2 class="text-xl font-semibold">Rollout #{String(entry().id)}</h2>
              <p class="text-sm text-neutral-500">Job {entry().jobId} · Status {entry().status}</p>
              <button type="button" onClick={advance} disabled={advanceBusy()} class="mt-2 rounded border border-neutral-300 px-3 py-2 text-xs dark:border-neutral-700">
                {advanceBusy() ? 'Advancing…' : 'Advance rollout'}
              </button>
              <Show when={message()}><p class="mt-1 text-sm text-neutral-500">{message()}</p></Show>
            </header>
            <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Groups</h3>
              <For each={status()?.groups ?? []}>{(group) => <p class="text-sm text-neutral-700 dark:text-neutral-300">{group.group} · step {group.stepIndex}/{group.totalSteps}</p>}</For>
            </article>
            <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Recent events</h3>
              <For each={events()?.events ?? []}>{(event) => <p class="text-xs text-neutral-700 dark:text-neutral-300">{event.level}: {event.message}</p>}</For>
            </article>
          </section>
        )}
      </Show>
    </Show>
  )
}
