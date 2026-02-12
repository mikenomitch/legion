import { useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'

export default function DeploymentDetail() {
  const params = useParams<{ id: string }>()
  const deploymentId = createMemo(() => decodeURIComponent(params.id || ''))

  const [rollouts] = createResource(deploymentId, async (id) => {
    const response = await managerClient.listRollouts({ activeOnly: false })
    return response.rollouts.filter((rollout) => rollout.toJobConfigId === id)
  })

  return (
    <Show when={!rollouts.loading} fallback={<p class="text-neutral-500">Loading deployment…</p>}>
      <Show when={(rollouts()?.length ?? 0) > 0} fallback={<p class="text-neutral-500">Deployment not found.</p>}>
        <section class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-xl font-semibold">Deployment {deploymentId()}</h2>
          <For each={rollouts() ?? []}>
            {(rollout) => (
              <article class="rounded border border-neutral-200 bg-neutral-50 p-3 dark:border-neutral-800 dark:bg-neutral-950/50">
                <p class="text-sm font-semibold">Rollout #{String(rollout.id)}</p>
                <p class="text-xs text-neutral-500">{rollout.jobId} · {rollout.status} · Updated {rollout.updatedAt}</p>
              </article>
            )}
          </For>
        </section>
      </Show>
    </Show>
  )
}
