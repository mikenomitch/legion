import { useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'
import { fetchJob } from './indexData'

export default function JobDetail() {
  const params = useParams<{ id: string }>()
  const jobId = createMemo(() => decodeURIComponent(params.id || ''))

  const [job] = createResource(jobId, fetchJob)
  const [spec] = createResource(jobId, (id) => managerClient.getJobSpec({ name: id }))

  return (
    <Show when={!job.loading} fallback={<p class="text-neutral-500">Loading job…</p>}>
      <Show when={job()} fallback={<p class="text-neutral-500">Job not found.</p>}>
        {(entry) => (
          <section class="space-y-4">
            <header class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h2 class="text-xl font-semibold">{entry().name || entry().id}</h2>
              <p class="text-sm text-neutral-500">Status {entry().status || 'unknown'}</p>
            </header>
            <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Submissions</h3>
              <Show when={!spec.loading} fallback={<p class="text-sm text-neutral-500">Loading spec…</p>}>
                <Show when={(spec()?.submissions.length ?? 0) > 0} fallback={<p class="text-sm text-neutral-500">No submissions for this job.</p>}>
                  <div class="space-y-2">
                    <For each={spec()?.submissions ?? []}>
                      {(submission) => <p class="rounded bg-neutral-100 px-2 py-1 text-xs dark:bg-neutral-950">{submission.path || 'unknown path'}</p>}
                    </For>
                  </div>
                </Show>
              </Show>
            </article>
          </section>
        )}
      </Show>
    </Show>
  )
}
