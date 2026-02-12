import { useParams } from '@solidjs/router'
import { For, Show, createResource } from 'solid-js'
import { managerClient } from '../grpcClient'

export default function JobDetails() {
  const params = useParams()
  const key = () => decodeURIComponent(params.id || '')

  const [info] = createResource(key, async (name) => managerClient.getJobInfo({ name }))
  const [spec] = createResource(key, async (name) => managerClient.getJobSpec({ name }))

  return (
    <section class="space-y-4">
      <h2 class="text-xl font-semibold">Job: {key()}</h2>
      <Show when={!info.loading && !spec.loading} fallback={<p>Loading job details…</p>}>
        <Show when={!info.error && !spec.error} fallback={<p class="text-red-500">Failed to load job details.</p>}>
          <div class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <p><strong>Name:</strong> {info()?.job?.name || info()?.job?.id || key()}</p>
            <p><strong>Status:</strong> {info()?.job?.status || 'unknown'}</p>
            <p><strong>Submissions:</strong> {spec()?.submissions.length ?? 0}</p>
          </div>
          <For each={spec()?.submissions ?? []}>
            {(submission) => (
              <article class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
                <p class="font-semibold">{submission.path || 'Unknown path'}</p>
                <pre class="mt-2 overflow-auto rounded bg-neutral-100 p-2 text-xs dark:bg-neutral-950">{submission.content}</pre>
              </article>
            )}
          </For>
        </Show>
      </Show>
    </section>
  )
}
