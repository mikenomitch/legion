import { useParams } from '@solidjs/router'
import { For, Show, createResource } from 'solid-js'
import { managerClient } from '../../grpcClient'

export default function JobSpecDetail() {
  const params = useParams()
  const [data] = createResource(async () => {
    const id = decodeURIComponent(params.configId)
    const response = await managerClient.getJobSpecByConfig({ configId: id })
    return { configId: id, submissions: response.submissions }
  })

  return (
    <Show when={!data.loading} fallback={<p>Loading spec…</p>}>
      <Show when={!data.error && data()} fallback={<p class="text-rose-500">Failed to load spec.</p>}>
        <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-xl font-semibold">Job Spec {data()!.configId}</h2>
          <For each={data()!.submissions}>{(submission) => (
            <div class="mt-3 rounded border border-neutral-200 p-3 dark:border-neutral-700">
              <div class="text-sm font-semibold">{submission.path}</div>
              <pre class="mt-2 max-h-[520px] overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{submission.content}</pre>
            </div>
          )}</For>
        </section>
      </Show>
    </Show>
  )
}
