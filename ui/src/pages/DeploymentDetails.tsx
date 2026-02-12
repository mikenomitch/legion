import { useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'
import { formatDate } from '../utils/format'

export default function DeploymentDetails() {
  const params = useParams()
  const [jobId, configId] = decodeURIComponent(params.id || '').split(':')

  const [rollouts] = createResource(async () => {
    const response = await managerClient.listRollouts({ activeOnly: false, jobId })
    return response.rollouts
  })

  const matching = createMemo(() => (rollouts() ?? []).filter((rollout) => rollout.toJobConfigId === configId))

  return (
    <section class="space-y-4">
      <h2 class="text-xl font-semibold">Deployment: {jobId} / {configId}</h2>
      <Show when={!rollouts.loading} fallback={<p>Loading deployment…</p>}>
        <Show when={!rollouts.error} fallback={<p class="text-red-500">Failed to load deployment details.</p>}>
          <For each={matching()}>
            {(rollout) => (
              <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
                <p><strong>Rollout ID:</strong> {rollout.id.toString()}</p>
                <p><strong>Status:</strong> {rollout.status}</p>
                <p><strong>Updated:</strong> {formatDate(rollout.updatedAt)}</p>
              </article>
            )}
          </For>
        </Show>
      </Show>
    </section>
  )
}
