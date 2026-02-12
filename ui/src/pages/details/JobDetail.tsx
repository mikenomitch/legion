import { A, useNavigate, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { managerClient } from '../../grpcClient'
import { formatDate } from '../../utils/format'

export default function JobDetail() {
  const params = useParams()
  const navigate = useNavigate()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const [activeTab, setActiveTab] = createSignal<'overview' | 'spec'>('overview')
  const [actionMessage, setActionMessage] = createSignal('')

  const [jobData] = createResource(async () => {
    const [jobInfo, ledger, rollouts, spec, specHistory] = await Promise.all([
      managerClient.getJobInfo({ name: jobName() }),
      managerClient.getLedger({}),
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
      managerClient.listJobSpecHistory({ name: jobName() }),
    ])

    const allocations = ledger.nodes.flatMap((node) =>
      node.allocations.filter((allocation) => allocation.jobId === (jobInfo.job?.id ?? jobName())),
    )

    return {
      job: jobInfo.job,
      allocations,
      rollouts: rollouts.rollouts,
      activeRollout: rollouts.rollouts.find((rollout) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(rollout.status.toLowerCase())),
      spec: spec.submissions,
      specHistory: specHistory.items,
    }
  })

  const destroyJob = async () => {
    try {
      const response = await managerClient.destroyJob({ name: jobName() })
      setActionMessage(response.message || 'Job destroyed')
      setTimeout(() => navigate('/jobs'), 900)
    } catch (error) {
      console.error(error)
      setActionMessage('Destroy failed: stop the job first, then retry.')
    }
  }

  return (
    <Show when={!jobData.loading} fallback={<p>Loading job…</p>}>
      <Show when={!jobData.error && jobData()} fallback={<p class="text-rose-500">Failed to load job.</p>}>
        <div class="space-y-4">
          <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 class="text-2xl font-semibold">{jobData()!.job?.name || jobData()!.job?.id}</h2>
                <p class="text-sm text-neutral-500">Status: {jobData()!.job?.status || 'unknown'} · Allocations: {jobData()!.allocations.length}</p>
              </div>
              <button type="button" onClick={destroyJob} class="rounded border border-rose-300 px-3 py-2 text-sm font-semibold text-rose-700 dark:border-rose-700 dark:text-rose-300">
                Delete Job
              </button>
            </div>
            <Show when={actionMessage()}><p class="mt-2 text-sm text-neutral-600 dark:text-neutral-300">{actionMessage()}</p></Show>

            <div class="mt-4 flex gap-2">
              <button type="button" onClick={() => setActiveTab('overview')} class={`rounded border px-3 py-1.5 text-sm ${activeTab() === 'overview' ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-100 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-300'}`}>Overview</button>
              <button type="button" onClick={() => setActiveTab('spec')} class={`rounded border px-3 py-1.5 text-sm ${activeTab() === 'spec' ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-100 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-300'}`}>Job Spec</button>
            </div>
          </section>

          <Show when={activeTab() === 'overview'}>
            <Show when={jobData()!.activeRollout}>
              <section class="rounded border border-amber-300 bg-amber-50/70 p-4 dark:border-amber-700 dark:bg-amber-950/30">
                <h3 class="font-semibold">Active rollout</h3>
                <p class="text-sm">Rollout #{String(jobData()!.activeRollout!.id)} · {jobData()!.activeRollout!.status}</p>
                <A href={`/rollouts/${jobData()!.activeRollout!.id}`} class="text-sm underline">Open rollout</A>
              </section>
            </Show>


            <Show when={jobData()!.activeRollout}>
              <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
                <h3 class="font-semibold">Allocations in active rollout target</h3>
                <div class="mt-2 space-y-1 text-sm">
                  <For each={jobData()!.allocations.filter((allocation) => allocation.jobConfigId === jobData()!.activeRollout!.toJobConfigId)}>
                    {(allocation) => <p>#{String(allocation.id)} on <A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A> · {allocation.status}</p>}
                  </For>
                </div>
              </section>
            </Show>

            <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="mb-2 font-semibold">Allocations</h3>
              <div class="overflow-x-auto">
                <table class="min-w-full text-sm">
                  <thead><tr class="text-left"><th class="py-2 pr-4">ID</th><th class="py-2 pr-4">Status</th><th class="py-2 pr-4">Node</th><th class="py-2 pr-4">Job Config</th></tr></thead>
                  <tbody>
                    <For each={jobData()!.allocations}>{(allocation) => (
                      <tr class="border-t border-neutral-200 dark:border-neutral-800"><td class="py-2 pr-4">{String(allocation.id)}</td><td class="py-2 pr-4">{allocation.status}</td><td class="py-2 pr-4"><A class="underline" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A></td><td class="py-2 pr-4"><A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(allocation.jobConfigId)}`}>{allocation.jobConfigId}</A></td></tr>
                    )}</For>
                  </tbody>
                </table>
              </div>
            </section>

            <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="mb-2 font-semibold">Associated rollouts</h3>
              <div class="space-y-2">
                <For each={jobData()!.rollouts}>{(rollout) => (
                  <div class="rounded border border-neutral-200 p-3 dark:border-neutral-700">
                    <div class="flex items-center justify-between gap-2"><A href={`/rollouts/${rollout.id}`} class="font-semibold underline">Rollout #{String(rollout.id)}</A><span>{rollout.status}</span></div>
                    <p class="mt-1 text-xs text-neutral-500">Updated {formatDate(rollout.updatedAt)}</p>
                    <p class="mt-1 text-xs">from <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(rollout.fromJobConfigId || '')}`}>{rollout.fromJobConfigId || '—'}</A> → to <A class="underline" href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(rollout.toJobConfigId)}`}>{rollout.toJobConfigId}</A></p>
                  </div>
                )}</For>
              </div>
            </section>
          </Show>

          <Show when={activeTab() === 'spec'}>
            <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Current spec submissions</h3>
              <For each={jobData()!.spec}>{(submission) => (
                <div class="mt-3 rounded border border-neutral-200 p-3 dark:border-neutral-700">
                  <div class="text-sm font-semibold">{submission.path}</div>
                  <pre class="mt-2 max-h-80 overflow-auto rounded bg-neutral-100 p-2 text-xs dark:bg-neutral-950">{submission.content}</pre>
                </div>
              )}</For>
            </section>

            <section class="rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
              <h3 class="font-semibold">Spec history</h3>
              <div class="mt-2 space-y-2">
                <For each={jobData()!.specHistory}>{(item) => (
                  <A href={`/jobs/${encodeURIComponent(jobName())}/specs/${encodeURIComponent(item.configId)}`} class="block rounded border border-neutral-200 p-3 text-sm hover:border-neutral-400 dark:border-neutral-700">
                    {item.configId} · version {item.version !== undefined ? String(item.version) : '—'} · {formatDate(item.createdAt)}
                  </A>
                )}</For>
              </div>
            </section>
          </Show>
        </div>
      </Show>
    </Show>
  )
}
