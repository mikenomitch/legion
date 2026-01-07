import { For, Show, createEffect, createMemo, onCleanup, type Resource } from 'solid-js'
import { createStore } from 'solid-js/store'
import type { Job, Submission } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { POLL_INTERVAL_MS } from '../utils/polling'
import { formatDate } from '../utils/format'

interface JobsProps {
  jobs: Resource<readonly Job[] | undefined>
}

export default function Jobs(props: JobsProps) {
  const [specs, setSpecs] =
    createStore<Record<string, { loading: boolean; error?: string; submissions?: Submission[]; expanded?: boolean }>>({})

  const jobList = createMemo(() => props.jobs() ?? [])

  const loadSpec = async (job: Job) => {
    const key = job.name || job.id
    if (!key) return

    setSpecs(key, (current) => ({ ...current, loading: true, error: undefined, expanded: true }))
    try {
      const response = await managerClient.getJobSpec({ name: key })
      setSpecs(key, { loading: false, submissions: response.submissions, expanded: true })
    } catch (error) {
      console.error('Failed to load job spec', error)
      setSpecs(key, { loading: false, error: 'Unable to load job spec', expanded: true })
    }
  }

  const refreshSpec = async (key: string) => {
    const current = specs[key]
    if (!current) return

    setSpecs(key, (prev) => ({ ...prev, loading: true, error: undefined }))
    try {
      const response = await managerClient.getJobSpec({ name: key })
      setSpecs(key, (prev) => ({ ...prev, loading: false, submissions: response.submissions }))
    } catch (error) {
      console.error('Failed to refresh job spec', error)
      setSpecs(key, (prev) => ({ ...prev, loading: false, error: 'Unable to load job spec' }))
    }
  }

  const toggleSpecVisibility = (job: Job) => {
    const key = job.name || job.id
    if (!key) return

    setSpecs(key, (current) => (current ? { ...current, expanded: !current.expanded } : current))
  }

  createEffect(() => {
    const interval = window.setInterval(() => {
      Object.entries(specs).forEach(([key, value]) => {
        if (value?.expanded) {
          void refreshSpec(key)
        }
      })
    }, POLL_INTERVAL_MS)

    onCleanup(() => window.clearInterval(interval))
  })

  return (
    <Show when={!props.jobs.loading} fallback={<p class="text-neutral-500">Loading jobs…</p>}>
      <Show when={!props.jobs.error} fallback={<p class="text-red-500">Failed to load jobs.</p>}>
        {jobList().length > 0 ? (
          <div class="space-y-3">
            <For each={jobList()}>
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

                  <div class="mt-3 flex flex-wrap items-center gap-2">
                    <button
                      type="button"
                      class="rounded border border-neutral-300 bg-white px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-neutral-800 shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100 dark:shadow-[3px_3px_0_#0b0d12]"
                      onClick={() => loadSpec(job)}
                      disabled={specs[job.name || job.id]?.loading}
                    >
                      See Spec
                    </button>
                  </div>

                  <Show when={specs[job.name || job.id]}>
                    <div class="mt-3 space-y-2 rounded border border-neutral-200 bg-neutral-50 p-3 text-sm text-neutral-800 dark:border-neutral-800 dark:bg-neutral-950/60 dark:text-neutral-200">
                      <button
                        type="button"
                        class="flex w-full items-center justify-between rounded border border-neutral-200 bg-white px-3 py-2 text-left font-semibold uppercase tracking-[0.08em] text-neutral-800 shadow-sm transition hover:-translate-y-0.5 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
                        onClick={() => toggleSpecVisibility(job)}
                      >
                        <span class="flex items-center gap-2">
                          <span
                            class={`inline-block text-lg leading-none transition-transform ${
                              specs[job.name || job.id]?.expanded ? 'rotate-90' : 'rotate-0'
                            }`}
                            aria-hidden="true"
                          >
                            ›
                          </span>
                          Job Spec
                        </span>
                        <Show when={specs[job.name || job.id]?.loading}>
                          <span class="text-xs font-normal uppercase tracking-[0.1em] text-neutral-500">Loading…</span>
                        </Show>
                      </button>

                      <Show when={specs[job.name || job.id]?.expanded}>
                        <div class="space-y-2">
                          <Show when={specs[job.name || job.id]?.loading}>
                            <p>Loading job spec…</p>
                          </Show>

                          <Show when={specs[job.name || job.id]?.error}>
                            <p class="text-red-500">{specs[job.name || job.id]!.error}</p>
                          </Show>

                          <Show
                            when={!specs[job.name || job.id]?.loading && !specs[job.name || job.id]?.error}
                            fallback={null}
                          >
                            <Show
                              when={(specs[job.name || job.id]?.submissions?.length ?? 0) > 0}
                              fallback={<p class="text-neutral-500">No spec returned for this job.</p>}
                            >
                              <div class="space-y-2">
                                <For each={specs[job.name || job.id]?.submissions ?? []}>
                                  {(submission) => (
                                    <div class="rounded border border-neutral-200 bg-white px-3 py-2 shadow-sm dark:border-neutral-700 dark:bg-neutral-900">
                                      <div class="flex flex-wrap items-center justify-between gap-2">
                                        <div class="text-sm font-semibold">{submission.path || 'Unknown path'}</div>
                                        <Show when={submission.isPrimary}>
                                          <span class="rounded border border-emerald-400 bg-emerald-50 px-2 py-0.5 text-[11px] uppercase tracking-wide text-emerald-700 dark:border-emerald-600 dark:bg-emerald-950 dark:text-emerald-100">
                                            Primary
                                          </span>
                                        </Show>
                                      </div>
                                      <pre class="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded bg-neutral-100 p-2 font-mono text-xs text-neutral-800 dark:bg-neutral-950 dark:text-neutral-100">
{submission.content || 'No content provided'}
                                      </pre>
                                    </div>
                                  )}
                                </For>
                              </div>
                            </Show>
                          </Show>
                        </div>
                      </Show>
                    </div>
                  </Show>
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
