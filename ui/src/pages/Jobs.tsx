import { For, Show, type Resource } from 'solid-js'
import type { Job } from '../gen/legion_pb'
import { formatDate } from '../utils/format'

interface JobsProps {
  jobs: Resource<readonly Job[] | undefined>
}

export default function Jobs(props: JobsProps) {
  return (
    <Show when={!props.jobs.loading} fallback={<p class="text-neutral-500">Loading jobs…</p>}>
      <Show when={!props.jobs.error} fallback={<p class="text-red-500">Failed to load jobs.</p>}>
        {props.jobs() && props.jobs()!.length > 0 ? (
          <div class="space-y-3">
            <For each={props.jobs()!}>
              {(job) => (
                <article class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] transition dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
                  <div class="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Job</p>
                      <p class="text-lg font-semibold text-neutral-900 dark:text-neutral-50">{job.name || job.id}</p>
                      <p class="text-sm text-neutral-500 dark:text-neutral-400">Created {formatDate(job.createdAt)}</p>
                    </div>
                    <span class="rounded border border-neutral-300 px-3 py-1 text-[11px] uppercase tracking-wide text-neutral-800 dark:border-neutral-700 dark:text-neutral-200">
                      {job.status || 'Unknown'}
                    </span>
                  </div>
                </article>
              )}
            </For>
          </div>
        ) : (
          <p class="text-neutral-500">No jobs reported.</p>
        )}
      </Show>
    </Show>
  )
}
