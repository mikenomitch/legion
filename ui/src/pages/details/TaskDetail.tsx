import { A, useParams } from '@solidjs/router'
import { GetLedgerReply } from '../../gen/legion_pb'
import { AccessGate } from '../../components/AccessGate'
import { For, Show, createMemo, createResource } from 'solid-js'
import { Badge, DefList, EmptyState, Note, PageHeader, SectionCard, Spinner, StatusBadge, TableShell, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../../components/ui'
import LogViewer from '../../components/LogViewer'
import { managerClient } from '../../grpcClient'
import AllocationExecPanel from '../../components/AllocationExecPanel'
import { AllocationPorts, ServiceEndpointsTable, ServiceTags } from '../../components/ServiceEndpoints'
import { allocationHealthLabel, fetchAllocationRuntime, parseAllocNetwork } from '../../utils/allocations'
import {
  artifactProgressPercent,
  buildTaskInstancesByName,
  extractJobSpecData,
  parseArtifactStates,
  shortenArtifactDigest,
  type ArtifactStateRow,
  type JobTaskArtifact,
} from '../../utils/jobspec'
import { shortVolumeId, volumePinsPlacement, volumeStateTone } from '../../utils/volumes'

// Checksum column value: pinned sources carry their digest in the source
// (`…@sha256:…`); otherwise fall back to the declared checksum.
const declaredArtifactDigest = (artifact: JobTaskArtifact): string | undefined => {
  if (artifact.pinnedFrom) {
    const separator = artifact.source.lastIndexOf('@')
    if (separator >= 0) return artifact.source.slice(separator + 1)
  }
  return artifact.checksum
}

const formatMb = (bytes: number): string => `${(bytes / (1024 * 1024)).toFixed(1)} MB`

const artifactProgressLabel = (row: ArtifactStateRow): string | undefined => {
  if (row.bytesFetched === undefined || row.bytesTotal === undefined) return undefined
  const percent = artifactProgressPercent(row)
  const prefix = percent === undefined ? '' : `${percent}% · `
  return `${prefix}${formatMb(row.bytesFetched)} / ${formatMb(row.bytesTotal)}`
}

const formatDurationMs = (ms: number): string => (ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`)

function TaskDetailContent() {
  const params = useParams()
  const jobName = createMemo(() => decodeURIComponent(params.id))
  const taskName = createMemo(() => decodeURIComponent(params.taskName))

  // Keyed on the route params so task→task navigation refetches.
  const [data] = createResource(() => `${jobName()}/${taskName()}`, async () => {
    const [jobInfo, rollouts, spec, ledger, allocationHealth, volumes] = await Promise.all([
      managerClient.getJobInfo({ name: jobName() }),
      managerClient.listRollouts({ jobId: jobName(), activeOnly: false, page: 1, pageSize: 200 }),
      managerClient.getJobSpec({ name: jobName() }),
      // node:read garnish (allocation placement); job-scoped tokens
      // still get the rest of the page.
      managerClient.getLedger({}).catch(() => new GetLedgerReply()),
      fetchAllocationRuntime(jobName()),
      // Actual volumes rather than the declaration: this page should answer
      // "where is my data and is it healthy", which the spec cannot.
      managerClient.listVolumes({ jobId: jobName() }).catch(() => ({ volumes: [] })),
    ])

    const latestRollout = rollouts.rollouts[0]
    const rolloutStatus = latestRollout ? await managerClient.getRolloutStatus({ id: latestRollout.id }) : undefined

    const taskSpec = extractJobSpecData(spec.submissions)?.tasks.find((task) => task.name === taskName())
    const instancesByTask = buildTaskInstancesByName(rolloutStatus?.groups ?? [])
    const taskInstances = instancesByTask.get(taskName())

    // All running allocations for this job (from the ledger), regardless of
    // whether a rollout produced them. The exec panel targets these; the
    // manager rejects exec into an allocation whose task != this page's task.
    const runnable = new Set(['placed', 'active', 'running'])
    const jobAllocations = ledger.nodes
      .flatMap((node) => node.allocations)
      .filter((alloc) => alloc.jobId === jobName() && runnable.has(alloc.status))

    // Endpoints for the service this task declares (if any), unhealthy
    // included so a failing endpoint is visible rather than absent.
    const serviceName = taskSpec?.service ? (taskSpec.service.name ?? taskName()) : undefined
    const endpoints = serviceName
      ? await managerClient
          .resolveService({ service: serviceName, includeUnhealthy: true })
          .then((reply) => reply.endpoints.filter((endpoint) => endpoint.taskName === taskName()))
          .catch(() => [])
      : []

    return {
      job: jobInfo.job,
      taskSpec,
      taskInstances,
      jobAllocations,
      latestRollout,
      groups: rolloutStatus?.groups ?? [],
      allocationHealth,
      serviceName,
      endpoints,
      // Tombstones belong on the Volumes page (which has a filter for them);
      // here they are just noise between the reader and their live storage.
      volumes: volumes.volumes.filter(
        (volume) => volume.taskName === taskName() && volume.state !== 'destroyed',
      ),
    }
  })

  // The ledger identifies allocations by task_config_id only, so the task name
  // comes from the same state-store lookup that carries health.
  const taskAllocations = createMemo(() =>
    (data()?.jobAllocations ?? []).filter(
      (alloc) => data()?.allocationHealth.get(Number(alloc.id))?.taskName === taskName(),
    ),
  )

  // Allocations the exec panel may target. The manager rejects exec into an
  // allocation whose task is not this page's task, so a sibling task's
  // allocation is a dead panel — and on a multi-task job it renders this
  // task's whole action form set once per sibling allocation. Filter to this
  // task whenever the task↔allocation mapping is available; a job-scoped token
  // without `state:read` gets an empty map, and there we still show the job's
  // allocations rather than an empty section (the manager is the real gate).
  const execAllocations = createMemo(() => {
    const runtime = data()?.allocationHealth
    const allocations = data()?.jobAllocations ?? []
    if (!runtime || runtime.size === 0) return allocations
    return allocations.filter((alloc) => {
      const known = runtime.get(Number(alloc.id))
      return known === undefined || known.taskName === taskName()
    })
  })

  return (
    <Show when={!data.loading} fallback={<div class="py-6"><Spinner label="Loading task…" /></div>}>
      <Show when={!data.error && data()} fallback={<Note tone="danger">Failed to load task details.</Note>}>
        <div>
          <PageHeader
            eyebrow="Task"
            title={taskName()}
            subtitle={
              <>
                In job{' '}
                <A class={linkClass} href={`/jobs/${encodeURIComponent(jobName())}`}>
                  {data()!.job?.name ?? jobName()}
                </A>
              </>
            }
            badges={<StatusBadge status={data()!.taskInstances?.statuses[0] ?? 'unknown'} />}
          />

          <div class="space-y-4">
            <Show when={data()!.taskSpec} fallback={<Note tone="warn">Task not present in the current job spec.</Note>}>
              <SectionCard title="Definition">
                <DefList
                  cols={3}
                  items={[
                    ['Group', data()!.taskSpec!.group],
                    ['Runtime', data()!.taskSpec!.runtime],
                    [
                      'Lifecycle',
                      <span class="inline-flex items-center gap-2">
                        {data()!.taskSpec!.lifecycle}
                        <Show when={data()!.taskSpec!.lifecycle === 'sidecar'}>
                          <Badge tone="info">runs alongside its group</Badge>
                        </Show>
                      </span>,
                    ],
                    ...(data()!.taskSpec!.stateful
                      ? ([[
                          'Stateful',
                          <span class="inline-flex flex-wrap items-center gap-2">
                            <Badge tone="accent">stateful · {data()!.taskSpec!.stateful!.mode}</Badge>
                            <Show when={data()!.taskSpec!.stateful!.storagePath}>
                              <span class="font-mono text-xs text-muted" title="Where the task sees its instance storage">{data()!.taskSpec!.stateful!.storagePath}</span>
                            </Show>
                            <Show when={data()!.volumes.find((volume) => volume.instanceId)}>
                              {(volume) => <Badge tone={volume().nodeLocal ? 'neutral' : 'ok'}>{volume().driver}</Badge>}
                            </Show>
                          </span>,
                        ]] as Array<[string, any]>)
                      : []),
                    [
                      'Depends on',
                      data()!.taskSpec!.dependsOn.length > 0 ? (
                        <span class="inline-flex flex-wrap gap-x-2 gap-y-1">
                          <For each={data()!.taskSpec!.dependsOn}>
                            {(dependency) => (
                              <A class={linkClass} href={`/jobs/${encodeURIComponent(jobName())}/tasks/${encodeURIComponent(dependency.task)}`}>
                                {dependency.task}
                                <Show when={dependency.condition}>
                                  <span class="text-faint"> ({dependency.condition})</span>
                                </Show>
                              </A>
                            )}
                          </For>
                        </span>
                      ) : (
                        <span class="text-muted">none</span>
                      ),
                    ],
                  ]}
                />
              </SectionCard>

              <Show when={(data()!.volumes ?? []).length > 0}>
                <SectionCard
                  title="Volumes"
                  description="Persistent storage for this task. A node-local volume pins its instance to the node holding the data."
                  flush
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>Volume</th>
                          <th class={thClass}>Driver</th>
                          <th class={thClass}>State</th>
                          <th class={thClass}>Instance</th>
                          <th class={thClass}>Node</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={data()!.volumes}>
                          {(volume) => (
                            <tr class={trHoverClass}>
                              <td class={tdClass}>
                                <A
                                  href={`/volumes/${encodeURIComponent(volume.volumeId)}`}
                                  class="font-mono text-xs text-ink hover:text-brass-strong"
                                  title={volume.volumeId}
                                >
                                  {shortVolumeId(volume.volumeId)}
                                </A>
                                <div class="text-2xs text-faint">{volume.name}</div>
                              </td>
                              <td class={tdClass}>
                                <Badge tone={volume.nodeLocal ? 'neutral' : 'ok'}>{volume.driver}</Badge>
                              </td>
                              <td class={tdClass}>
                                <Badge tone={volumeStateTone(volume)} dot class="uppercase tracking-wide">
                                  {volume.state}
                                </Badge>
                                <Show when={volume.lastError}>
                                  <div class="mt-0.5 max-w-[16rem] truncate text-2xs text-red-500" title={volume.lastError}>
                                    {volume.lastError}
                                  </div>
                                </Show>
                              </td>
                              <td class={tdClass + ' font-mono text-xs text-muted'}>{volume.instanceId ?? '—'}</td>
                              <td class={tdClass}>
                                <Show when={volume.nodeId} fallback={<span class="text-faint">—</span>}>
                                  <A href={`/nodes/${encodeURIComponent(volume.nodeId!)}`} class="text-muted hover:text-brass-strong">
                                    {volume.nodeId}
                                  </A>
                                  <Show when={volumePinsPlacement(volume)}>
                                    <span class="ml-1 text-brass-strong" title="Node-local: this instance can only be replaced here">*</span>
                                  </Show>
                                </Show>
                              </td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                </SectionCard>
              </Show>

              <Show when={data()!.taskSpec!.artifacts.length > 0}>
                <SectionCard title="Artifacts" flush>
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>Name</th>
                          <th class={thClass}>Source</th>
                          <th class={thClass}>Checksum / digest</th>
                          <th class={thClass}>Resolve</th>
                          <th class={thClass}>Auth</th>
                          <th class={thClass}>Dest</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={data()!.taskSpec!.artifacts}>
                          {(artifact) => {
                            const digest = declaredArtifactDigest(artifact)
                            return (
                              <tr class={trHoverClass}>
                                <td class={tdClass}>{artifact.name ?? '—'}</td>
                                <td class={tdClass}>
                                  <span class="block max-w-[320px] truncate font-mono text-xs text-muted" title={artifact.source}>{artifact.source}</span>
                                </td>
                                <td class={tdClass}>
                                  <Show when={digest} fallback={<span class="text-faint">—</span>}>
                                    <span class="font-mono text-xs" title={digest}>{shortenArtifactDigest(digest!)}</span>
                                  </Show>
                                  <Show when={artifact.pinnedFrom}>
                                    <p class="text-2xs text-faint">pinned from <span class="font-mono">{artifact.pinnedFrom}</span></p>
                                  </Show>
                                </td>
                                <td class={tdClass + ' text-muted'}>{artifact.resolve ?? '—'}</td>
                                <td class={tdClass + ' text-muted'}>{artifact.authLabel}</td>
                                <td class={tdClass + ' text-muted'}>{artifact.dest ?? '—'}</td>
                              </tr>
                            )
                          }}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                </SectionCard>
              </Show>

              {/* Discovery: what this task advertises, on which port, and
                  where it is reachable right now. */}
              <Show when={data()!.taskSpec!.service || data()!.taskSpec!.ports.length > 0}>
                <SectionCard
                  title="Service & ports"
                  description="What this task advertises to discovery, and the ports it declares."
                  actions={
                    <Show when={data()!.serviceName}>
                      <A class={linkClass + ' text-sm'} href={`/services/${encodeURIComponent(data()!.serviceName!)}`}>
                        Open service
                      </A>
                    </Show>
                  }
                >
                  <div class="space-y-4">
                    <Show when={data()!.taskSpec!.service}>
                      <DefList
                        cols={3}
                        items={[
                          ['Service', <span class="font-medium text-ink">{data()!.serviceName}</span>],
                          ['Advertised port', <span class="font-mono text-xs">{data()!.taskSpec!.service!.port}</span>],
                          [
                            'Tags & meta',
                            <ServiceTags tags={data()!.taskSpec!.service!.tags} meta={data()!.taskSpec!.service!.meta} />,
                          ],
                        ]}
                      />
                    </Show>

                    <Show when={data()!.taskSpec!.ports.length > 0}>
                      <div>
                        <p class="eyebrow mb-1.5">
                          Declared ports
                          <Show when={data()!.taskSpec!.networkMode}>
                            <span class="ml-2 normal-case tracking-normal text-faint">mode {data()!.taskSpec!.networkMode}</span>
                          </Show>
                        </p>
                        <div class="flex flex-wrap gap-1.5">
                          <For each={data()!.taskSpec!.ports}>
                            {(port) => (
                              <Badge tone="neutral" mono>
                                {port.name}
                                {port.staticPort === undefined ? ' · dynamic' : ` · ${port.staticPort}`}
                                {port.to === undefined ? '' : ` → ${port.to}`}
                              </Badge>
                            )}
                          </For>
                        </div>
                      </div>
                    </Show>

                    <Show when={data()!.serviceName}>
                      <div>
                        <p class="eyebrow mb-1.5">Live endpoints</p>
                        <ServiceEndpointsTable
                          endpoints={data()!.endpoints}
                          emptyTitle="No endpoints yet"
                          emptyBody="An endpoint appears once an allocation is placed, gets its port, and passes its checks."
                        />
                      </div>
                    </Show>
                  </div>
                </SectionCard>
              </Show>

              <SectionCard
                title="Health"
                description="Declared checks, and what the workers last reported for each allocation."
              >
                <div class="space-y-4">
                  <div>
                    <p class="eyebrow mb-1.5">Declared checks</p>
                    <Show
                      when={data()!.taskSpec!.checks.length > 0}
                      fallback={
                        <p class="text-sm text-muted">
                          No checks declared — the task counts as healthy as soon as it is running.
                        </p>
                      }
                    >
                      <TableShell>
                        <table class={tableClass}>
                          <thead class={theadClass}>
                            <tr>
                              <th class={thClass}>Check</th>
                              <th class={thClass}>Kind</th>
                              <th class={thClass}>Target</th>
                              <th class={thClass}>Interval</th>
                              <th class={thClass}>Timeout</th>
                              <th class={thClass}>Unhealthy after</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-line">
                            <For each={data()!.taskSpec!.checks}>
                              {(check) => (
                                <tr class={trHoverClass}>
                                  <td class={tdClass}>{check.name ?? check.type}</td>
                                  <td class={tdClass + ' text-muted'}>{check.kind}</td>
                                  <td class={tdClass + ' font-mono text-xs text-muted'}>
                                    {[check.port ? `port ${check.port}` : undefined, check.path].filter(Boolean).join(' ') || '—'}
                                  </td>
                                  <td class={tdClass + ' text-muted'}>{check.every ?? '—'}</td>
                                  <td class={tdClass + ' text-muted'}>{check.timeout ?? '—'}</td>
                                  <td class={tdClass + ' tabular-nums text-muted'}>
                                    {check.unhealthyAfterCount === undefined ? '—' : `${check.unhealthyAfterCount} failures`}
                                  </td>
                                </tr>
                              )}
                            </For>
                          </tbody>
                        </table>
                      </TableShell>
                    </Show>
                  </div>

                  <div>
                    <p class="eyebrow mb-1.5">Reported health</p>
                    <Show
                      when={taskAllocations().length > 0}
                      fallback={<p class="text-sm text-muted">No allocations running for this task.</p>}
                    >
                      <ul class="space-y-1.5">
                        <For each={taskAllocations()}>
                          {(alloc) => {
                            const health = () => data()!.allocationHealth.get(Number(alloc.id))
                            return (
                              <li class="flex flex-wrap items-center gap-2 rounded-md border border-line bg-surface2/40 px-2.5 py-1.5 text-sm">
                                <span class="font-mono text-xs font-semibold">#{String(alloc.id)}</span>
                                <StatusBadge status={allocationHealthLabel(health())} />
                                <span class="text-xs text-faint">on {alloc.nodeId}</span>
                                <Show when={health()?.status}>
                                  <span class="text-xs text-muted">· {health()!.status}</span>
                                </Show>
                                <span class="flex-1" />
                                <AllocationPorts network={parseAllocNetwork(alloc.network)} />
                              </li>
                            )
                          }}
                        </For>
                      </ul>
                    </Show>
                  </div>
                </div>
              </SectionCard>

              <SectionCard
                title="Runtime allocations"
                actions={
                  <Show when={data()!.latestRollout}>
                    <A class={linkClass + ' text-sm'} href={`/rollouts/${data()!.latestRollout!.id}`}>Open rollout</A>
                  </Show>
                }
              >
                <Show when={data()!.taskInstances?.allocations.length} fallback={<p class="text-sm text-muted">No runtime allocations available.</p>}>
                  <div class="space-y-2.5">
                    <For each={data()!.taskInstances!.allocations}>
                      {(allocation) => {
                        const artifactRows = parseArtifactStates(allocation.artifactStates)
                        return (
                          <div class="rounded-lg border border-line bg-surface2/40 p-3">
                            <div class="flex flex-wrap items-center justify-between gap-2">
                              <p class="font-mono text-sm font-semibold">#{String(allocation.allocationId)}</p>
                              <StatusBadge status={allocation.status} />
                            </div>
                            <p class="mt-1 text-xs text-faint">
                              Worker <A class="text-muted hover:text-brass-strong" href={`/nodes/${encodeURIComponent(allocation.nodeId)}`}>{allocation.nodeId}</A>
                              {' · '}group {allocation.taskGroup} · {allocation.lifecycle} · role {allocation.role}
                            </p>
                            <Show when={artifactRows.length > 0}>
                              <div class="mt-2 space-y-1.5 border-t border-line pt-2">
                                <p class="eyebrow">Artifacts</p>
                                <For each={artifactRows}>
                                  {(artifact) => (
                                    <div class="text-xs">
                                      <div class="flex flex-wrap items-center gap-2">
                                        <StatusBadge status={artifact.state} />
                                        <span class="max-w-[280px] truncate font-mono text-muted" title={artifact.source}>
                                          {artifact.name ?? artifact.source}
                                        </span>
                                        <Show when={artifactProgressLabel(artifact)}>
                                          <span class="text-faint">{artifactProgressLabel(artifact)}</span>
                                        </Show>
                                        <Show when={artifact.digest}>
                                          <span class="font-mono text-faint" title={artifact.digest}>{shortenArtifactDigest(artifact.digest!)}</span>
                                        </Show>
                                        <Show when={artifact.durationMs !== undefined}>
                                          <span class="text-faint">{formatDurationMs(artifact.durationMs!)}</span>
                                        </Show>
                                      </div>
                                      <Show when={artifact.state === 'failed' && artifact.error}>
                                        <p class="mt-0.5 text-red-600 dark:text-red-400">{artifact.error}</p>
                                      </Show>
                                    </div>
                                  )}
                                </For>
                              </div>
                            </Show>
                          </div>
                        )
                      }}
                    </For>
                  </div>
                </Show>
              </SectionCard>
            </Show>

            <SectionCard title="Exec & actions" description="Run commands or declared actions inside this task's running allocations.">
              <Show
                when={execAllocations().length > 0}
                fallback={<EmptyState icon="terminal" title="No running allocations" body="Once an allocation is running you can open a shell or trigger actions here." />}
              >
                <div class="space-y-2.5">
                  <For each={execAllocations()}>
                    {(alloc) => (
                      <div class="rounded-lg border border-line bg-surface2/40 p-3">
                        <div class="flex flex-wrap items-center justify-between gap-2">
                          <p class="font-mono text-sm font-semibold">#{String(alloc.id)}</p>
                          <div class="flex items-center gap-2">
                            <span class="text-xs text-faint">on {alloc.nodeId}</span>
                            <StatusBadge status={alloc.status} />
                          </div>
                        </div>
                        <AllocationExecPanel
                          allocationId={alloc.id}
                          jobName={jobName()}
                          taskName={taskName()}
                        />
                      </div>
                    )}
                  </For>
                </div>
              </Show>
            </SectionCard>

            <SectionCard title="Logs">
              <Show
                when={(data()!.taskInstances?.allocations.length ?? 0) > 0}
                fallback={<p class="text-sm text-muted">No allocations are running for this task yet.</p>}
              >
                <LogViewer
                  job={jobName()}
                  allocationIds={data()!.taskInstances!.allocations.map((allocation) => allocation.allocationId)}
                />
              </Show>
            </SectionCard>
          </div>
        </div>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function TaskDetail() {
  return (
    <AccessGate resource="job">
      <TaskDetailContent />
    </AccessGate>
  )
}
