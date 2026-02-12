import { useParams } from '@solidjs/router'
import { Show, createResource } from 'solid-js'
import { managerClient } from '../../grpcClient'

export default function JobDetail() {
  const params = useParams()
  const [data] = createResource(async () => {
    const name = decodeURIComponent(params.id)
    const [job, spec] = await Promise.all([
      managerClient.getJobInfo({ name }),
      managerClient.getJobSpec({ name }),
    ])
    return { job: job.job, submissions: spec.submissions }
  })

  return (
    <Show when={!data.loading} fallback={<p>Loading job…</p>}>
      <Show when={!data.error && data()} fallback={<p class="text-rose-500">Failed to load job.</p>}>
        <article class="space-y-3 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-xl font-semibold">{data()!.job?.name || data()!.job?.id}</h2>
          <p class="text-sm text-neutral-500">Status {data()!.job?.status || 'unknown'}</p>
          <pre class="max-h-[520px] overflow-auto rounded bg-neutral-100 p-3 text-xs dark:bg-neutral-950">{JSON.stringify(data(), null, 2)}</pre>
        </article>
      </Show>
    </Show>
  )
}
