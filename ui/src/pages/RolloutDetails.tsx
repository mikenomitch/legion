import { useParams } from '@solidjs/router'
import { For, Show, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'
import { asDisplay, formatDate, formatRelativeTime } from '../utils/format'

export default function RolloutDetails() {
  const params = useParams()
  const rolloutId = () => BigInt(params.id || '0')

  const [status] = createResource(rolloutId, async (id) => managerClient.getRolloutStatus({ id }))
  const [events] = createResource(rolloutId, async (id) => managerClient.listRolloutEvents({ rolloutId: id, limit: 100 }))

  return (
    <section class="space-y-4">
      <h2 class="text-xl font-semibold">Rollout #{params.id}</h2>
      <Show when={!status.loading && !events.loading} fallback={<p>Loading rollout details…</p>}>
        <Show when={!status.error && !events.error && status()?.rollout} fallback={<p class="text-red-500">Failed to load rollout details.</p>}>
          <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <p><strong>Job:</strong> {asDisplay(status()?.rollout?.jobId)}</p>
            <p><strong>Status:</strong> {asDisplay(status()?.rollout?.status)}</p>
            <p><strong>Created:</strong> {formatDate(status()?.rollout?.createdAt)}</p>
            <p><strong>Updated:</strong> {formatRelativeTime(status()?.rollout?.updatedAt)}</p>
          </article>
          <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <h3 class="font-semibold">Recent Events</h3>
            <ul class="mt-2 space-y-2 text-sm">
              <For each={events()?.events ?? []}>
                {(event) => (
                  <li class="rounded border border-neutral-200 p-2 dark:border-neutral-700">
                    <div class="font-medium">{event.level}: {event.message}</div>
                    <div class="text-xs text-neutral-500">{formatDate(event.createdAt)} · {event.groupName || 'all groups'}</div>
                  </li>
                )}
              </For>
            </ul>
          </article>
        </Show>
      </Show>
    </section>
  )
}
