import { A, useParams, useSearchParams } from '@solidjs/router'
import { For, Index, Show, createMemo, createResource, createSignal, type JSX, type Resource } from 'solid-js'
import type {
  InstanceInfo,
  JobSpecHistoryItem,
  LedgerAllocation,
  NodeAllocationUsage,
  NodeMsg,
  Rollout,
  RolloutAllocationStatus,
  SchedulerEvent,
  ServiceEndpoint,
  VolumeInfo,
} from '../../gen/legion_pb'
import { AccessGate } from '../../components/AccessGate'
import { AllocationLink } from '../../components/AllocationLink'
import EnvelopeCard from '../../components/EnvelopeCard'
import LogViewer from '../../components/LogViewer'
import { PressureSteps } from '../../components/NodeUsagePanel'
import { InstanceEventSummary, PressureExplanation, SnapshotEventSummary } from '../../components/PressureHistory'
import { EndpointAddress, ServiceTags } from '../../components/ServiceEndpoints'
import TaskActions, { SHELL_ENTRY, type ActionTarget } from '../../components/TaskActions'
import type { JobAction } from '../../gen/legion_pb'
import {
  Badge,
  Button,
  CopyButton,
  Dot,
  EmptyState,
  Icon,
  Meter,
  MonoBadge,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  Spinner,
  StatusBadge,
  TableShell,
  cx,
  linkClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
  trHoverClass,
} from '../../components/ui'
import { managerClient } from '../../grpcClient'
import { parseAllocNetwork } from '../../utils/allocations'
import {
  checkReport,
  deriveStopReason,
  factsFromEvents,
  eventAllocationEntry,
  fetchAllocationState,
  fetchTaskConfigInfo,
  healthView,
  isAclDenied,
  isRunningStatus,
  orAbsent,
  parseAllocationId,
  parseAllocationResources,
  splitStatus,
  useAllocationEvents,
  type AllocationStateLookup,
  type HealthView,
  type StopReason,
} from '../../utils/allocationDetail'
import { parseEventMeta } from '../../utils/events'
import { formatBytes, formatDateTime, pluralize } from '../../utils/format'
import { artifactProgressPercent, extractJobSpecFromReply, parseArtifactStates, shortenArtifactDigest, type JobSpecData } from '../../utils/jobspec'
import { INDEX_POLL_INTERVAL_MS, POLL_INTERVAL_MS, retainOnError, usePoll } from '../../utils/polling'
import { PUBLIC_ADDRESS_HINT, isPrivateHost, nodePublicAddress } from '../../utils/services'
import { dotToneClasses, statusTone, type Tone } from '../../utils/status'
import {
  UNREPORTED_REASONS,
  bytesToMiB,
  fetchNodeUsage,
  formatCores,
  formatIdle,
  formatMiB,
  physicalCapacity,
  pressureSummary,
} from '../../utils/usage'
import { shortVolumeId, volumePinsPlacement, volumeStateTone } from '../../utils/volumes'
import { ENVELOPE_COPY, envelopeServes, sessionsPerEnvelope } from '../../utils/sessions'

/*
 * One allocation: one placement of one task of one job version on one worker.
 *
 * The page answers an operator's questions in the order they are asked —
 * what and where is it, is it OK, how do I reach it, what is it using, what
 * happened to it, let me look inside, where is its data, what can I do — and
 * each section reads from whichever RPC the caller's token can see
 * (utils/allocationDetail.ts). An allocation the manager no longer has still
 * gets a page: its events outlive it and say what it was and why it ended.
 *
 * An envelope allocation (docs/session-mode.md) runs nothing: it reserves a
 * node's room for a session task's sessions. Its page says so, counts the
 * sessions inside it (ListSessionNodes), and leaves out health, network, logs,
 * and exec, which have nothing to show for a placeholder.
 */

const isRolloutDone = (status: string) =>
  ['completed', 'failed', 'done', 'cancelled', 'completed_incomplete'].includes(status.toLowerCase())

const levelTone = (level: string): Tone => (level === 'error' ? 'danger' : level === 'warn' ? 'warn' : 'neutral')

/**
 * A resource's value, or undefined while it holds an error. Reading an errored
 * resource throws to the route's error boundary; the sections here are
 * garnish and must degrade to "not available" instead. (Once a fetch has
 * succeeded, `retainOnError` keeps later failures from erroring at all.)
 */
const valueOf = <T,>(resource: Resource<T>): T | undefined => (resource.error === undefined ? resource() : undefined)

const scrollToSection = (id: string) => document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' })

const jobHref = (job: string) => `/jobs/${encodeURIComponent(job)}`
const taskHref = (job: string, task: string) => `${jobHref(job)}/tasks/${encodeURIComponent(task)}`
const instanceHref = (job: string, instance: string) => `${jobHref(job)}/instances/${encodeURIComponent(instance)}`
const nodeHref = (node: string) => `/nodes/${encodeURIComponent(node)}`

// ---------------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------------

type Located = {
  key: string
  ledgerDenied: boolean
  entry?: LedgerAllocation
  nodes: Map<string, NodeMsg>
  state: AllocationStateLookup
}

// The allocation's own row: the ledger (node:read) and the state store
// (state:read), side by side. Either answering without the id means the
// manager no longer has it.
const locate = async ([id]: readonly [bigint, number]): Promise<Located> => {
  const [ledger, state] = await Promise.all([
    managerClient.getLedger({}).then(
      (reply) => ({ reply, denied: false }),
      (error: unknown) => {
        if (isAclDenied(error)) return { reply: undefined, denied: true }
        throw error
      },
    ),
    fetchAllocationState(id),
  ])
  const nodes = new Map<string, NodeMsg>()
  let entry: LedgerAllocation | undefined
  for (const ledgerNode of ledger.reply?.nodes ?? []) {
    if (ledgerNode.node?.id) nodes.set(ledgerNode.node.id, ledgerNode.node)
    entry ??= ledgerNode.allocations.find((allocation) => allocation.id === id)
  }
  return { key: String(id), ledgerDenied: ledger.denied, entry, nodes, state }
}

type JobFacts = {
  key: string
  producing?: Rollout
  replacing?: Rollout
  rolloutEntry?: RolloutAllocationStatus
  inferredConfigId?: string
  versions: JobSpecHistoryItem[]
  volumes: VolumeInfo[]
  instance?: InstanceInfo
}

type JobFactsSource = readonly [job: string, configId: string, id: string, instanceId: string, tick: number]
const jobFactsKey = ([job, configId, id, instanceId]: JobFactsSource) => `${job}|${configId}|${id}|${instanceId}`

// Everything job:read can say about the allocation. The rollout status is the
// one job-scoped read that lists allocations, so it doubles as the way to find
// one when the ledger is denied (configId unknown): the latest rollout covers
// the allocations of its from and to versions.
const loadJobFacts = async (source: JobFactsSource): Promise<JobFacts> => {
  const [job, baseConfigId, idText, instanceId] = source
  const id = BigInt(idText)
  const [rollouts, history, volumes, instance] = await Promise.all([
    orAbsent(managerClient.listRollouts({ jobId: job, activeOnly: false, page: 1, pageSize: 50 }), undefined),
    orAbsent(managerClient.listJobSpecHistory({ name: job }), undefined),
    orAbsent(managerClient.listVolumes({ jobId: job }), undefined),
    instanceId ? orAbsent(managerClient.getInstance({ instanceId }), undefined) : Promise.resolve(undefined),
  ])
  const list = rollouts?.rollouts ?? []
  const statusTarget = baseConfigId
    ? (list.find((rollout) => rollout.toJobConfigId === baseConfigId) ?? list.find((rollout) => rollout.fromJobConfigId === baseConfigId))
    : list[0]
  const status = statusTarget ? await orAbsent(managerClient.getRolloutStatus({ id: statusTarget.id }), undefined) : undefined
  const rolloutEntry = status?.groups.flatMap((group) => group.allocations).find((allocation) => allocation.allocationId === id)
  let configId = baseConfigId || undefined
  if (!configId && rolloutEntry && statusTarget) {
    configId = rolloutEntry.role === 'replacee' ? statusTarget.fromJobConfigId : statusTarget.toJobConfigId
  }
  return {
    key: jobFactsKey(source),
    producing: configId ? list.find((rollout) => rollout.toJobConfigId === configId) : undefined,
    replacing: configId ? list.find((rollout) => rollout.fromJobConfigId === configId && !isRolloutDone(rollout.status)) : undefined,
    rolloutEntry,
    inferredConfigId: baseConfigId ? undefined : configId,
    versions: history?.items ?? [],
    volumes: volumes?.volumes ?? [],
    instance: instance?.instance,
  }
}

// The job version the allocation runs, not the job's latest: an allocation
// mid-rollout runs the old spec, and its checks and ports are the old ones.
const loadSpec = async ([job, configId]: readonly [string, string]): Promise<{ key: string; spec: JobSpecData | null }> => {
  const reply = await orAbsent(configId ? managerClient.getJobSpecByConfig({ configId }) : managerClient.getJobSpec({ name: job }), undefined)
  return { key: `${job}|${configId}`, spec: reply ? extractJobSpecFromReply(reply) : null }
}

// ---------------------------------------------------------------------------
// Small pieces
// ---------------------------------------------------------------------------

/** A label/value pair, laid out like DefList but with stable children. */
function Fact(props: { label: JSX.Element; children: JSX.Element; title?: string }) {
  return (
    <div class="min-w-0" title={props.title}>
      <dt class="text-xs text-faint">{props.label}</dt>
      <dd class="mt-0.5 break-words font-medium text-ink">{props.children}</dd>
    </div>
  )
}

const Faint = (props: { children: JSX.Element }) => <span class="font-normal text-faint">{props.children}</span>

function HealthBadge(props: { view: HealthView; class?: string }) {
  return (
    <StatusBadge
      status={props.view.label}
      tone={props.view.tone}
      class={props.class}
      title={props.view.status ? `Latest report: ${props.view.status}` : 'No health report yet'}
    />
  )
}

/** A command an operator (or an agent) can paste, with a copy button. */
function CliLine(props: { command: string; note?: string }) {
  return (
    <div class="flex min-w-0 items-center gap-2 rounded-md border border-line bg-surface2/60 py-1 pl-2.5 pr-1">
      <code class="min-w-0 flex-1 overflow-x-auto whitespace-nowrap font-mono text-2xs text-ink" title={props.note}>
        {props.command}
      </code>
      <CopyButton text={props.command} label="" />
    </div>
  )
}

function StopCallout(props: { stop: StopReason; ended?: boolean }) {
  return (
    <Note tone={props.stop.tone === 'ok' ? 'ok' : props.stop.tone === 'neutral' ? 'neutral' : props.stop.tone}>
      <p class="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span class="font-semibold">{props.ended ? 'Ended' : 'Last stop'}: {props.stop.label}</span>
        <Show when={props.stop.at}>
          <RelativeTime value={props.stop.at} class="text-xs opacity-80" title={`From ${props.stop.source}`} />
        </Show>
        <Show when={props.stop.preemptorJob}>
          {(job) => (
            <span class="text-xs">
              by{' '}
              <A href={jobHref(job())} class="font-medium underline-offset-2 hover:underline">
                {job()}
              </A>
            </span>
          )}
        </Show>
      </p>
      <Show when={props.stop.detail}>
        <p class="mt-1 break-words text-xs">{props.stop.detail}</p>
      </Show>
      <Show when={props.stop.hint}>
        <p class="mt-1 text-xs">
          <span class="font-semibold">Hint </span>
          {props.stop.hint}
        </p>
      </Show>
    </Note>
  )
}

/**
 * Measured memory against the reservation (the marker) and the ceiling (the
 * bar's end when `memoryMax` is set; otherwise the reservation is the limit).
 * The fill is green within the reservation, amber while bursting past it, red
 * within 5% of the hard limit.
 */
function MemoryGauge(props: { usedMiB: number; peakMiB?: number; reservedMiB: number; ceilingMiB?: number }) {
  const limit = () => (props.ceilingMiB && props.ceilingMiB > 0 ? props.ceilingMiB : props.reservedMiB)
  const scale = () => Math.max(limit(), props.usedMiB, props.peakMiB ?? 0, 1)
  const at = (value: number) => `${Math.max(0, Math.min(100, (value / scale()) * 100))}%`
  const tone = (): Tone => {
    if (limit() > 0 && props.usedMiB >= limit() * 0.95) return 'danger'
    if (props.reservedMiB > 0 && props.usedMiB > props.reservedMiB) return 'warn'
    return 'ok'
  }
  return (
    <div
      role="meter"
      aria-label="Memory used"
      aria-valuemin={0}
      aria-valuemax={Math.round(limit())}
      aria-valuenow={Math.round(props.usedMiB)}
      class="relative h-2 w-full rounded-full bg-line/70"
    >
      <div class={cx('absolute inset-y-0 left-0 rounded-full transition-all', dotToneClasses[tone()])} style={{ width: at(props.usedMiB) }} />
      <Show when={props.peakMiB !== undefined && props.peakMiB > props.usedMiB}>
        <div class="absolute -inset-y-0.5 w-px bg-muted/70" style={{ left: at(props.peakMiB!) }} title={`Peak ${formatMiB(props.peakMiB!)} MiB`} />
      </Show>
      <Show when={props.reservedMiB > 0 && props.reservedMiB < scale()}>
        <div
          class="absolute -inset-y-1 w-0.5 -translate-x-1/2 rounded-full bg-ink"
          style={{ left: at(props.reservedMiB) }}
          title={`Reservation ${formatMiB(props.reservedMiB)} MiB`}
        />
      </Show>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Event timeline
// ---------------------------------------------------------------------------

function EventRow(props: { event: SchedulerEvent; allocationId: bigint }) {
  const [open, setOpen] = createSignal(false)
  const meta = createMemo(() => parseEventMeta(props.event))
  // Batch events cover several allocations; say so, and show this one's entry.
  const entry = createMemo(() => (props.event.allocationId === props.allocationId ? undefined : eventAllocationEntry(props.event, props.allocationId)))
  const batchCount = () => {
    const count = meta().count
    return typeof count === 'number' && count > 1 ? count : undefined
  }
  const category = () => props.event.kind.split('.')[0]
  const metadataText = createMemo(() => {
    try {
      return JSON.stringify(JSON.parse(props.event.metadataJson || '{}'), null, 2)
    } catch {
      return props.event.metadataJson
    }
  })
  return (
    <li>
      <button
        type="button"
        class="flex w-full items-start gap-3 px-4 py-2.5 text-left transition-colors hover:bg-surface2/50"
        aria-expanded={open()}
        onClick={() => setOpen((value) => !value)}
      >
        <Badge tone={levelTone(props.event.level)} class="mt-0.5 shrink-0 uppercase">
          {props.event.level || 'info'}
        </Badge>
        <div class="min-w-0 flex-1">
          <div class="flex flex-wrap items-center gap-x-2 gap-y-0.5">
            <span class="font-mono text-xs text-muted">{props.event.kind}</span>
            <Show when={batchCount()}>
              {(count) => <span class="text-2xs text-faint" title="This event covers several allocations; this is one of them">one of {count()}</span>}
            </Show>
          </div>
          <p class="mt-0.5 break-words text-sm text-ink">{props.event.message}</p>
        </div>
        <div class="shrink-0 text-right">
          <RelativeTime value={props.event.createdAt} class="block whitespace-nowrap text-xs text-muted" />
          <span class="hidden whitespace-nowrap font-mono text-2xs text-faint sm:block">{formatDateTime(props.event.createdAt)}</span>
        </div>
        <Icon name="chevronRight" size={14} class={cx('mt-0.5 text-faint transition-transform', open() && 'rotate-90')} />
      </button>
      <Show when={open()}>
        <div class="space-y-2 border-t border-line bg-surface2/40 px-4 py-2.5 text-xs">
          <p class="flex flex-wrap gap-x-4 gap-y-1 text-muted">
            <span>
              <span class="font-semibold text-faint">id</span> <span class="font-mono">{String(props.event.id)}</span>
            </span>
            <Show when={props.event.jobId}>
              {(job) => (
                <span>
                  <span class="font-semibold text-faint">job</span>{' '}
                  <A href={jobHref(job())} class="hover:text-brass-strong">
                    {job()}
                  </A>
                </span>
              )}
            </Show>
            <Show when={props.event.nodeId}>
              {(node) => (
                <span>
                  <span class="font-semibold text-faint">node</span>{' '}
                  <A href={nodeHref(node())} class="hover:text-brass-strong">
                    {node()}
                  </A>
                </span>
              )}
            </Show>
            <Show when={props.event.rolloutId !== undefined}>
              <span>
                <span class="font-semibold text-faint">rollout</span>{' '}
                <A href={`/rollouts/${String(props.event.rolloutId)}`} class="hover:text-brass-strong">
                  #{String(props.event.rolloutId)}
                </A>
              </span>
            </Show>
            <Show when={props.event.evalId}>
              <span>
                <span class="font-semibold text-faint">eval</span> <span class="font-mono">{props.event.evalId}</span>
              </span>
            </Show>
          </p>
          <Show when={props.event.kind === 'node.pressure'}>
            <PressureExplanation event={props.event} />
          </Show>
          <Show when={category() === 'snapshot'}>
            <SnapshotEventSummary event={props.event} />
          </Show>
          <Show when={category() === 'instance'}>
            <InstanceEventSummary event={props.event} />
          </Show>
          <Show when={entry()}>
            {(own) => (
              <p class="text-muted">
                <span class="font-semibold text-faint">this allocation</span>{' '}
                <span class="font-mono">{JSON.stringify(own())}</span>
              </p>
            )}
          </Show>
          <pre class="max-h-64 overflow-auto rounded-md border border-line bg-surface p-2 font-mono text-2xs leading-relaxed text-muted">{metadataText()}</pre>
        </div>
      </Show>
    </li>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

function AllocationDetailContent() {
  const params = useParams()
  const [search] = useSearchParams()
  const allocationId = createMemo(() => parseAllocationId(params.id))
  const idLabel = () => `#${String(allocationId() ?? params.id)}`
  const jobHint = createMemo(() => {
    const value = search.job
    return typeof value === 'string' && value.trim() ? value.trim() : undefined
  })

  // Facts that fan out into several RPCs refresh at the slow cadence; the
  // event feed is incremental and cheap, so it runs at the live one.
  const slowTick = usePoll(INDEX_POLL_INTERVAL_MS)
  const liveTick = usePoll(POLL_INTERVAL_MS)

  const [located] = createResource(
    () => {
      const id = allocationId()
      return id === undefined ? undefined : ([id, slowTick()] as const)
    },
    retainOnError(locate, ([id]) => String(id)),
  )
  const current = () => {
    const value = valueOf(located)
    return value && value.key === String(allocationId()) ? value : undefined
  }
  const ledgerEntry = () => current()?.entry
  const stateRow = () => current()?.state.row

  // --- Identity: ledger, then state store, then the job's rollout, then events.
  const feed = useAllocationEvents(allocationId, liveTick)
  const eventFacts = createMemo(() => {
    const id = allocationId()
    return id === undefined ? {} : factsFromEvents(feed.events(), id)
  })

  const jobId = createMemo(() => ledgerEntry()?.jobId || stateRow()?.jobId || eventFacts().jobId || jobHint())
  const baseConfigId = createMemo(() => ledgerEntry()?.jobConfigId || stateRow()?.jobConfigId || eventFacts().jobConfigId)
  const instanceId = createMemo(() => ledgerEntry()?.instanceId || stateRow()?.instanceId || eventFacts().instanceId)

  const [jobFacts] = createResource(
    () => {
      const job = jobId()
      const id = allocationId()
      // Wait for the placement read: it names the job version, without which
      // the first fetch would go to the wrong rollout and be thrown away.
      if (!job || id === undefined || !current()) return undefined
      return [job, baseConfigId() ?? '', String(id), instanceId() ?? '', slowTick()] as const
    },
    retainOnError(loadJobFacts, jobFactsKey),
  )
  const facts = () => {
    const value = valueOf(jobFacts)
    const id = allocationId()
    const job = jobId()
    if (!value || !job || id === undefined) return undefined
    return value.key === jobFactsKey([job, baseConfigId() ?? '', String(id), instanceId() ?? '', 0]) ? value : undefined
  }
  const rolloutEntry = () => facts()?.rolloutEntry
  const configId = createMemo(() => baseConfigId() || facts()?.inferredConfigId)

  const taskConfigId = createMemo(() => ledgerEntry()?.taskConfigId ?? stateRow()?.taskConfigId ?? rolloutEntry()?.taskConfigId ?? eventFacts().taskConfigId)
  const directTaskName = createMemo(() => ledgerEntry()?.taskName || stateRow()?.taskName || rolloutEntry()?.taskName)
  // A gone allocation's events name its task config, not its task.
  const [taskConfigInfo] = createResource(
    () => (!directTaskName() && taskConfigId() ? String(taskConfigId()) : undefined),
    (id: string) => fetchTaskConfigInfo(BigInt(id)),
  )
  const taskName = createMemo(() => directTaskName() || valueOf(taskConfigInfo)?.name)

  const [specResult] = createResource(
    () => {
      const job = jobId()
      return job && current() ? ([job, configId() ?? ''] as const) : undefined
    },
    loadSpec,
  )
  const specTasks = createMemo(() => {
    const job = jobId()
    const result = valueOf(specResult)
    if (!result || result.key !== `${job}|${configId() ?? ''}`) return undefined
    return result.spec?.tasks
  })
  const taskSpec = createMemo(() => {
    const name = taskName()
    return name ? specTasks()?.find((task) => task.name === name) : undefined
  })
  // The session task this allocation is the envelope for, if it is one. The
  // rendered spec lists the session task, never its generated envelope.
  const envelopeFor = createMemo(() => envelopeServes(taskName(), specTasks()))
  const servedSpec = createMemo(() => {
    const served = envelopeFor()
    return served ? specTasks()?.find((task) => task.name === served) : undefined
  })

  const nodeId = createMemo(() => ledgerEntry()?.nodeId || stateRow()?.nodeId || rolloutEntry()?.nodeId || eventFacts().nodeId)
  const node = createMemo(() => {
    const id = nodeId()
    return id ? current()?.nodes.get(id) : undefined
  })
  const status = createMemo(() => ledgerEntry()?.status || stateRow()?.status || rolloutEntry()?.status || '')
  const group = createMemo(() => stateRow()?.group || rolloutEntry()?.taskGroup || valueOf(taskConfigInfo)?.group || taskSpec()?.group)
  const lifecycle = createMemo(() => taskSpec()?.lifecycle || rolloutEntry()?.lifecycle || stateRow()?.lifecycle || valueOf(taskConfigInfo)?.lifecycle)

  // --- Existence. The ledger and the state store each list every live
  // allocation, so either one answering without this id means it is gone.
  const exists = createMemo(() => Boolean(ledgerEntry() || stateRow() || rolloutEntry()))
  const authoritative = createMemo(() => {
    const value = current()
    return Boolean(value && (!value.ledgerDenied || !value.state.denied))
  })
  const gone = createMemo(() => current() !== undefined && !exists() && authoritative())
  const unconfirmed = createMemo(() => current() !== undefined && !exists() && !authoritative() && (!jobId() || facts() !== undefined || Boolean(jobFacts.error)))
  const running = createMemo(() => exists() && isRunningStatus(status()))

  // --- Health: state store (has a timestamp), then rollout status, then the
  // service endpoint. None of them is "unhealthy" by default.
  const [endpointsResult] = createResource(
    () => {
      const spec = taskSpec()
      const service = spec?.service ? (spec.service.name ?? spec.name) : undefined
      return service && exists() ? ([service, slowTick()] as const) : undefined
    },
    retainOnError(
      async ([service]: readonly [string, number]) => ({
        service,
        endpoints: (await orAbsent(managerClient.resolveService({ service, includeUnhealthy: true }), undefined))?.endpoints ?? [],
      }),
      ([service]) => service,
    ),
  )
  const endpoints = createMemo<ServiceEndpoint[]>(() => (valueOf(endpointsResult)?.endpoints ?? []).filter((endpoint) => endpoint.allocationId === allocationId()))

  const health = createMemo<HealthView>(() => {
    const row = stateRow()
    if (row?.healthy !== undefined) return healthView(row.healthy, row.healthStatus)
    const entry = rolloutEntry()
    if (entry?.allocationHealthy !== undefined) return healthView(entry.allocationHealthy, entry.allocationHealth)
    const endpoint = endpoints()[0]
    if (endpoint?.healthStatus) return healthView(endpoint.healthy, endpoint.healthStatus)
    return healthView(undefined, undefined)
  })
  const artifacts = createMemo(() => parseArtifactStates(ledgerEntry()?.artifactStates ?? rolloutEntry()?.artifactStates))

  const stopReason = createMemo(() => {
    const id = allocationId()
    if (id === undefined) return undefined
    return deriveStopReason(feed.events(), id, current()?.state.stop, facts()?.instance?.lastStop)
  })

  // --- Network
  const network = createMemo(() => parseAllocNetwork(ledgerEntry()?.network || stateRow()?.network))
  const publicAddress = createMemo(() => nodePublicAddress(node()?.configAttrs))
  const reachHost = () => publicAddress() ?? network()?.address
  const privateOnly = () => !publicAddress() && Boolean(network()?.address) && isPrivateHost(network()!.address!)

  // --- Usage
  const [usageResult] = createResource(
    () => {
      const id = nodeId()
      return id && exists() ? ([id, slowTick()] as const) : undefined
    },
    retainOnError(async ([id]: readonly [string, number]) => ({ nodeId: id, reply: await fetchNodeUsage(id) }), ([id]) => id),
  )
  const usageReply = () => {
    const result = valueOf(usageResult)
    return result?.nodeId === nodeId() ? result?.reply : undefined
  }
  const usageEntry = createMemo<NodeAllocationUsage | undefined>(() => usageReply()?.allocations.find((entry) => entry.allocationId === allocationId()))
  const reservedResources = createMemo(() => parseAllocationResources(ledgerEntry()?.resources || stateRow()?.resources))

  // --- Versions, rollouts, storage
  const version = createMemo(() => facts()?.versions.find((item) => item.configId === configId()))
  const latestVersion = () => facts()?.versions[0]
  const volumes = createMemo(() =>
    (facts()?.volumes ?? []).filter(
      (volume) =>
        volume.state !== 'destroyed' &&
        ((instanceId() && volume.instanceId === instanceId()) || (!volume.instanceId && taskName() && volume.taskName === taskName())),
    ),
  )
  const movedTo = createMemo(() => {
    const instance = facts()?.instance
    if (!instance || instance.currentAllocId === undefined || instance.currentAllocId === allocationId()) return undefined
    return instance.currentAllocId
  })

  const canExec = createMemo(() => running() && Boolean(jobId()) && Boolean(taskName()) && !envelopeFor())
  // The task page's Actions panel, aimed at this one allocation. Targets and
  // actions are compared by content so a poll does not rebuild the panel.
  const actionTargets = createMemo(
    (): ActionTarget[] => {
      const id = allocationId()
      if (id === undefined) return []
      const healthy = health().label === 'healthy' ? true : health().label === 'unhealthy' ? false : undefined
      return [{ id: String(id), nodeId: nodeId(), healthy }]
    },
    undefined,
    { equals: (a, b) => JSON.stringify(a) === JSON.stringify(b) },
  )
  const [jobActions] = createResource(
    () => (canExec() ? ([jobId()!, taskName()!] as const) : undefined),
    async ([job, task]): Promise<JobAction[]> => {
      try {
        const reply = await managerClient.listJobActions({ job })
        return reply.actions.filter((action) => action.taskName === task || action.taskName === '')
      } catch {
        return []
      }
    },
  )
  const [actionEntry, setActionEntry] = createSignal<string>()
  const selectedAction = () => actionEntry() ?? jobActions()?.[0]?.name ?? SHELL_ENTRY
  const canTailLogs = createMemo(() => exists() && Boolean(jobId()) && !envelopeFor())

  const statusParts = createMemo(() => splitStatus(status()))

  return (
    <Show
      when={allocationId() !== undefined}
      fallback={
        <EmptyState icon="search" title={`"${params.id}" is not an allocation id`} body="Allocation ids are whole numbers, such as #1042." />
      }
    >
      <Show
        when={current() || !located.error}
        fallback={<Note tone="danger">Failed to load allocation {idLabel()}: {String((located.error as Error)?.message ?? located.error)}</Note>}
      >
        <Show when={current()} fallback={<div class="py-6"><Spinner label={`Loading allocation ${idLabel()}…`} /></div>}>
          <PageHeader
            eyebrow="Allocation"
            title={<span class="font-mono">{idLabel()}</span>}
            badges={
              <span class="inline-flex flex-wrap items-center gap-1.5">
                <Show
                  when={exists()}
                  fallback={
                    <Show when={gone() || unconfirmed()}>
                      <Badge tone="neutral" dot class="uppercase tracking-wide">{gone() ? 'ended' : 'unconfirmed'}</Badge>
                    </Show>
                  }
                >
                  <StatusBadge status={statusParts().word} title={statusParts().detail} />
                  <HealthBadge view={health()} />
                </Show>
                <Show when={lifecycle()}>{(value) => <Badge tone="neutral">{value()}</Badge>}</Show>
                <Show when={instanceId()}>
                  <Badge tone="accent" title="Fulfils a stable instance of a stateful task">stateful</Badge>
                </Show>
                <Show when={envelopeFor()}>
                  <Badge tone="info" title={ENVELOPE_COPY}>
                    session envelope
                  </Badge>
                </Show>
                <Show when={usageEntry()?.usage && usageEntry()!.reservedMemoryMib > 0 && bytesToMiB(usageEntry()!.usage!.memoryCurrentBytes) > usageEntry()!.reservedMemoryMib}>
                  <Badge tone="warn" title="Using more memory than it reserved: first to give memory back under node pressure">over reservation</Badge>
                </Show>
              </span>
            }
            subtitle={
              <span class="inline-flex flex-wrap items-center gap-x-1.5 gap-y-0.5">
                <Show when={taskName()} fallback={<span>Task unknown</span>}>
                  <span>
                    Task{' '}
                    <Show when={jobId()} fallback={<span class="font-medium text-ink">{taskName()}</span>}>
                      <A class={linkClass} href={taskHref(jobId()!, taskName()!)}>
                        {taskName()}
                      </A>
                    </Show>
                  </span>
                </Show>
                <Show when={jobId()}>
                  <span>
                    of job{' '}
                    <A class={linkClass} href={jobHref(jobId()!)}>
                      {jobId()}
                    </A>
                  </span>
                </Show>
                <Show when={nodeId()}>
                  <span class="text-faint">·</span>
                  <span>
                    {gone() ? 'ran on' : 'on'}{' '}
                    <A class={linkClass} href={nodeHref(nodeId()!)}>
                      {nodeId()}
                    </A>
                  </span>
                </Show>
              </span>
            }
            actions={
              <>
                <Show when={exists()}>
                  <Button size="xs" variant="ghost" icon="activity" onClick={() => scrollToSection('allocation-health')}>
                    Health
                  </Button>
                </Show>
                <Button size="xs" variant="ghost" icon="history" onClick={() => scrollToSection('allocation-events')}>
                  Events
                </Button>
                <Show when={canTailLogs()}>
                  <Button size="xs" variant="ghost" icon="file" onClick={() => scrollToSection('allocation-logs')}>
                    Logs
                  </Button>
                </Show>
                <Show when={canExec()}>
                  <Button size="xs" variant="ghost" icon="terminal" onClick={() => scrollToSection('allocation-exec')}>
                    Exec
                  </Button>
                </Show>
                <CopyButton text={String(allocationId())} label="Copy ID" />
              </>
            }
          />

          <div class="space-y-4">
            {/* --- What state is it in, in one sentence --------------------- */}
            <Show when={gone()}>
              <Note tone="warn">
                <p class="font-medium">Allocation {idLabel()} has ended (or never existed).</p>
                <p class="mt-1 text-xs">
                  The manager removes an allocation's record once it stops, so there is no live state to show. What remains is its
                  event history below, which names its job, worker, and how it ended.
                  <Show when={movedTo()}>
                    {(next) => (
                      <>
                        {' '}Its instance <span class="font-mono">{instanceId()}</span> is now served by{' '}
                        <AllocationLink id={next()} job={jobId()} variant="inherit" class="font-semibold" />.
                      </>
                    )}
                  </Show>
                </p>
              </Note>
            </Show>
            <Show when={unconfirmed()}>
              <Note tone="neutral">
                <p class="font-medium">Can't confirm whether allocation {idLabel()} is still running.</p>
                <p class="mt-1 text-xs">
                  Your token can't read placements (<code class="font-mono">node:read</code>) or the state store
                  (<code class="font-mono">state:read</code>)
                  <Show when={jobId()} fallback=", and the job is not known, so its rollouts can't be searched either.">
                    , and the allocation is not in job <span class="font-mono">{jobId()}</span>'s latest rollout.
                  </Show>{' '}
                  Its events, if your token can read them, are below.
                </p>
              </Note>
            </Show>
            <Show when={exists() && node() && node()!.status !== 'ready'}>
              <Note tone={statusTone(node()!.status) === 'danger' ? 'danger' : 'warn'}>
                Worker <A href={nodeHref(node()!.id)} class="font-medium underline-offset-2 hover:underline">{node()!.id}</A> is{' '}
                <span class="font-semibold">{node()!.status}</span>. The manager replaces allocations on a lost worker; stateful ones
                wait under their <code class="font-mono text-xs">stateful.onLost</code> policy.
              </Note>
            </Show>
            <Show when={gone() && stopReason()}>{(stop) => <StopCallout stop={stop()} ended />}</Show>
            <Show when={facts()?.replacing && exists()}>
              <Note tone="info">
                Being replaced by rollout{' '}
                <A href={`/rollouts/${String(facts()!.replacing!.id)}`} class="font-semibold underline-offset-2 hover:underline">
                  #{String(facts()!.replacing!.id)}
                </A>{' '}
                ({facts()!.replacing!.status}): it runs an older version of the job, which the rollout is moving off.
              </Note>
            </Show>

            {/* --- 1. What and where --------------------------------------- */}
            <SectionCard
              title="Placement"
              description={
                current()?.ledgerDenied && exists()
                  ? ledgerEntry() || stateRow()
                    ? undefined
                    : "Read from the job's rollout: your token can't read placements (node:read), so ports and reservations are not shown."
                  : undefined
              }
            >
              <dl class="grid gap-x-8 gap-y-3 text-sm sm:grid-cols-2 lg:grid-cols-3">
                <Fact label="Job">
                  <Show when={jobId()} fallback={<Faint>unknown</Faint>}>
                    <A class={linkClass} href={jobHref(jobId()!)}>
                      {jobId()}
                    </A>
                  </Show>
                </Fact>
                <Fact label="Task">
                  <Show when={taskName()} fallback={<Faint>{taskConfigId() ? `task config ${String(taskConfigId())}` : 'unknown'}</Faint>}>
                    <Show when={jobId()} fallback={<span>{taskName()}</span>}>
                      <A class={linkClass} href={taskHref(jobId()!, taskName()!)}>
                        {taskName()}
                      </A>
                    </Show>
                    <Show when={group()}>
                      <span class="ml-1.5 text-xs font-normal text-faint">group {group()}</span>
                    </Show>
                  </Show>
                </Fact>
                <Fact label="Instance">
                  <Show when={instanceId()} fallback={<Faint>stateless</Faint>}>
                    <Show when={jobId()} fallback={<MonoBadge>{instanceId()}</MonoBadge>}>
                      <A href={instanceHref(jobId()!, instanceId()!)} title={`Stateful instance ${instanceId()}`}>
                        <MonoBadge class="hover:border-brass/50">{instanceId()}</MonoBadge>
                      </A>
                    </Show>
                    <Show when={facts()?.instance}>
                      {(instance) => <StatusBadge status={instance().state} class="ml-1.5" title="The instance's lifecycle state" />}
                    </Show>
                  </Show>
                </Fact>
                <Fact label="Worker">
                  <Show when={nodeId()} fallback={<Faint>unknown</Faint>}>
                    <A class={linkClass} href={nodeHref(nodeId()!)}>
                      {nodeId()}
                    </A>
                    <Show when={node()}>
                      {(value) => (
                        <span class="ml-1.5 inline-flex gap-1 align-middle">
                          <Show when={value().status !== 'ready'}>
                            <StatusBadge status={value().status} />
                          </Show>
                          <Show when={value().availability && value().availability !== 'eligible'}>
                            <Badge tone="warn">{value().availability}</Badge>
                          </Show>
                          <Show when={value().pool && value().pool !== 'default'}>
                            <Badge tone="neutral">pool {value().pool}</Badge>
                          </Show>
                        </span>
                      )}
                    </Show>
                    <Show when={rolloutEntry()?.nodeHealthy === false}>
                      <Badge tone="danger" class="ml-1.5" title="The worker's own node health checks are failing">worker unhealthy</Badge>
                    </Show>
                  </Show>
                </Fact>
                <Fact label="Job version" title="The job config this allocation was placed from">
                  <Show when={configId() && jobId()} fallback={<Faint>unknown</Faint>}>
                    <A class="font-mono text-xs text-ink hover:text-brass-strong" href={`${jobHref(jobId()!)}/specs/${encodeURIComponent(configId()!)}`}>
                      <Show when={version()?.version !== undefined} fallback={configId()!.slice(0, 12)}>
                        v{String(version()!.version)}
                      </Show>
                    </A>
                    <Show when={latestVersion()}>
                      {(latest) => (
                        <Show
                          when={latest().configId === configId()}
                          fallback={
                            <Badge tone="warn" class="ml-1.5" title={`The job's current version is ${latest().version !== undefined ? `v${String(latest().version)}` : latest().configId}`}>
                              not current
                            </Badge>
                          }
                        >
                          <Badge tone="ok" class="ml-1.5">current</Badge>
                        </Show>
                      )}
                    </Show>
                  </Show>
                </Fact>
                <Fact label="Rollout" title="The rollout whose target version this allocation runs">
                  <Show
                    when={facts()?.producing}
                    fallback={<Faint>{facts() ? 'none (placed outside a rollout)' : '—'}</Faint>}
                  >
                    {(rollout) => (
                      <>
                        <A class="font-mono text-xs text-ink hover:text-brass-strong" href={`/rollouts/${String(rollout().id)}`}>
                          #{String(rollout().id)}
                        </A>
                        <StatusBadge status={rollout().status} class="ml-1.5" />
                        <Show when={rolloutEntry()?.role}>
                          <span class="ml-1.5 text-xs font-normal text-faint">{rolloutEntry()!.role}</span>
                        </Show>
                      </>
                    )}
                  </Show>
                </Fact>
                <Fact label="Runtime">
                  <Show when={taskSpec()?.runtime} fallback={<Faint>—</Faint>}>
                    <span class="font-mono text-xs">{taskSpec()!.runtime}</span>
                  </Show>
                </Fact>
                <Fact label="Placed" title="When the scheduler placed it (needs state:read, or its placement event)">
                  <Show when={stateRow()?.placedAt ?? eventFacts().placedAt} fallback={<Faint>—</Faint>}>
                    {(at) => <RelativeTime value={at()} />}
                  </Show>
                </Fact>
                <Fact label="Task config">
                  <Show when={taskConfigId()} fallback={<Faint>—</Faint>}>
                    <span class="font-mono text-xs text-muted">{String(taskConfigId())}</span>
                  </Show>
                </Fact>
              </dl>
            </SectionCard>

            <Show when={envelopeFor()}>
              {(served) => (
                <EnvelopeCard
                  jobId={jobId()}
                  allocationId={allocationId()!}
                  nodeId={nodeId()}
                  servedTask={served()}
                  perEnvelope={sessionsPerEnvelope(servedSpec())}
                  reserved={reservedResources()}
                />
              )}
            </Show>

            {/* --- 2. Is it OK ---------------------------------------------- */}
            <Show when={exists() && !envelopeFor()}>
              <div id="allocation-health" class="scroll-mt-20">
                <SectionCard
                  title="Health"
                  description="What the worker last reported, against the checks this version of the task declares."
                >
                  <div class="space-y-4">
                    <div class="flex flex-wrap items-start gap-x-4 gap-y-2">
                      <HealthBadge view={health()} class="text-xs" />
                      <div class="min-w-0 flex-1 text-sm">
                        <Show
                          when={health().status}
                          fallback={
                            <p class="text-muted">
                              No health report yet. Workers send one with each heartbeat once the task starts; until then its health is
                              unknown, which is not the same as unhealthy.
                            </p>
                          }
                        >
                          <p class="break-words font-mono text-xs text-ink">{health().status}</p>
                        </Show>
                        <Show when={stateRow()?.healthAt}>
                          {(at) => (
                            <p class="mt-0.5 text-xs text-faint">
                              reported <RelativeTime value={at()} />
                            </p>
                          )}
                        </Show>
                      </div>
                    </div>

                    <Show when={stopReason()}>{(stop) => <StopCallout stop={stop()} />}</Show>

                    <div>
                      <p class="eyebrow mb-1.5">Declared checks</p>
                      <Show
                        when={taskSpec()}
                        fallback={<p class="text-sm text-muted">The task's spec could not be read, so its checks are not shown.</p>}
                      >
                        <Show
                          when={taskSpec()!.checks.length > 0}
                          fallback={<p class="text-sm text-muted">None — the allocation counts as healthy once it is running.</p>}
                        >
                          <TableShell class="-mx-4">
                            <table class={tableClass}>
                              <thead class={theadClass}>
                                <tr>
                                  <th class={thClass}>Check</th>
                                  <th class={thClass}>Type</th>
                                  <th class={thClass}>Target</th>
                                  <th class={thClass}>Every</th>
                                  <th class={thClass} title="Workers report the result of whichever check ran last, so at most one check shows a result">
                                    Latest result
                                  </th>
                                </tr>
                              </thead>
                              <tbody class="divide-y divide-line">
                                <Index each={taskSpec()!.checks}>
                                  {(check) => {
                                    const report = () => checkReport(check(), health().status)
                                    return (
                                      <tr class={trHoverClass}>
                                        <td class={tdClass}>
                                          {check().name ?? <span class="text-muted">{check().kind}</span>}
                                        </td>
                                        <td class={tdClass + ' text-muted'}>
                                          {check().type} · {check().kind}
                                        </td>
                                        <td class={tdClass + ' font-mono text-xs text-muted'}>
                                          {[check().port ? `port ${check().port}` : undefined, check().path].filter(Boolean).join(' ') || '—'}
                                        </td>
                                        <td class={tdClass + ' text-muted'}>{check().every ?? '—'}</td>
                                        <td class={tdClass}>
                                          <Show
                                            when={report()}
                                            fallback={
                                              <span class="text-xs text-faint">
                                                {check().type === 'readiness' ? 'not reported (readiness)' : '—'}
                                              </span>
                                            }
                                          >
                                            {(result) => (
                                              <span class="inline-flex flex-wrap items-center gap-1.5">
                                                <StatusBadge status={result().ok ? 'passed' : 'failed'} />
                                                <Show when={result().reason}>
                                                  <span class="text-xs text-muted">{result().reason}</span>
                                                </Show>
                                              </span>
                                            )}
                                          </Show>
                                        </td>
                                      </tr>
                                    )
                                  }}
                                </Index>
                              </tbody>
                            </table>
                          </TableShell>
                        </Show>
                      </Show>
                    </div>

                    <Show when={artifacts().length > 0}>
                      <div>
                        <p class="eyebrow mb-1.5">
                          Artifacts · {artifacts().filter((row) => row.state === 'staged').length}/{artifacts().length} staged
                        </p>
                        <ul class="space-y-2">
                          <Index each={artifacts()}>
                            {(artifact) => (
                              <li class="rounded-md border border-line bg-surface2/40 px-3 py-2 text-xs">
                                <div class="flex flex-wrap items-center gap-2">
                                  <StatusBadge status={artifact().state} />
                                  <span class="min-w-0 max-w-full truncate font-mono text-muted" title={artifact().source}>
                                    {artifact().name ?? artifact().source}
                                  </span>
                                  <span class="flex-1" />
                                  <Show when={artifact().digest}>
                                    <span class="font-mono text-faint" title={artifact().digest}>{shortenArtifactDigest(artifact().digest!)}</span>
                                  </Show>
                                  <Show when={artifact().durationMs !== undefined}>
                                    <span class="text-faint">{artifact().durationMs! >= 1000 ? `${(artifact().durationMs! / 1000).toFixed(1)}s` : `${artifact().durationMs}ms`}</span>
                                  </Show>
                                  <Show when={(artifact().attempts ?? 0) > 1}>
                                    <span class="text-faint">{artifact().attempts} attempts</span>
                                  </Show>
                                </div>
                                <Show when={artifactProgressPercent(artifact()) !== undefined && artifact().state !== 'staged'}>
                                  <div class="mt-1.5 flex items-center gap-2">
                                    <Meter percent={artifactProgressPercent(artifact())!} tone="info" size="xs" label="Fetch progress" />
                                    <span class="shrink-0 font-mono tabular-nums text-faint">
                                      {formatBytes(artifact().bytesFetched)} / {formatBytes(artifact().bytesTotal)}
                                    </span>
                                  </div>
                                </Show>
                                <Show when={artifact().error}>
                                  <p class="mt-1 break-words text-red-600 dark:text-red-400">{artifact().error}</p>
                                </Show>
                              </li>
                            )}
                          </Index>
                        </ul>
                      </div>
                    </Show>
                  </div>
                </SectionCard>
              </div>
            </Show>

            <Show when={exists()}>
              <div class="grid items-start gap-4 lg:grid-cols-2">
                {/* --- 3. How do I reach it ---------------------------------- */}
                <Show when={!envelopeFor()}>
                <SectionCard title="Network" description="The address and ports assigned at placement, and the endpoints it registered.">
                  <div class="space-y-4">
                    <Show
                      when={network()}
                      fallback={
                        <p class="text-sm text-muted">
                          <Show
                            when={ledgerEntry() || stateRow()}
                            fallback="Assigned ports are not visible without node:read or state:read. A service endpoint below, if any, shows the advertised one."
                          >
                            <Show
                              when={(taskSpec()?.ports.length ?? 0) > 0}
                              fallback="No ports: the task declares no network block, so nothing is published on the worker for it."
                            >
                              The task declares ports, but none are assigned to this allocation yet.
                            </Show>
                          </Show>
                        </p>
                      }
                    >
                      {(net) => (
                        <>
                          <div class="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
                            <Show when={publicAddress()}>
                              <div class="min-w-0">
                                <p class="text-xs text-faint">Public address</p>
                                <p class="flex items-center gap-1 font-mono text-xs text-ink">
                                  {publicAddress()} <CopyButton text={publicAddress()!} label="" />
                                </p>
                              </div>
                            </Show>
                            <Show when={net().address}>
                              <div class="min-w-0">
                                <p class="text-xs text-faint">{publicAddress() ? 'Cluster address' : 'Address'}</p>
                                <p class="flex items-center gap-1 font-mono text-xs text-ink">
                                  {net().address} <CopyButton text={net().address!} label="" />
                                </p>
                              </div>
                            </Show>
                            <Show when={taskSpec()?.networkMode}>
                              <div>
                                <p class="text-xs text-faint">Mode</p>
                                <p class="font-mono text-xs text-ink">{taskSpec()!.networkMode}</p>
                              </div>
                            </Show>
                          </div>
                          <Show when={privateOnly()}>
                            <p class="text-xs text-faint">{PUBLIC_ADDRESS_HINT}</p>
                          </Show>
                          <Show when={net().ports.length > 0}>
                            <TableShell class="-mx-4">
                              <table class={tableClass}>
                                <thead class={theadClass}>
                                  <tr>
                                    <th class={thClass}>Port</th>
                                    <th class={thClass}>Host</th>
                                    <th class={thClass} title="The port inside the task, when it differs (network.ports[].to)">To</th>
                                    <th class={thClass}>Reach it at</th>
                                  </tr>
                                </thead>
                                <tbody class="divide-y divide-line">
                                  <Index each={net().ports}>
                                    {(port) => {
                                      const address = () => (reachHost() ? `${reachHost()}:${port().hostPort}` : undefined)
                                      return (
                                        <tr class={trHoverClass}>
                                          <td class={tdClass + ' font-mono text-xs'}>{port().name}</td>
                                          <td class={tdClass + ' font-mono text-xs tabular-nums'}>{port().hostPort}</td>
                                          <td class={tdClass + ' font-mono text-xs tabular-nums text-muted'}>{port().to ?? '—'}</td>
                                          <td class={tdClass}>
                                            <Show when={address()} fallback={<span class="text-faint">—</span>}>
                                              <span class="inline-flex items-center gap-1">
                                                <a
                                                  href={`http://${address()}`}
                                                  target="_blank"
                                                  rel="noreferrer"
                                                  class={cx('font-mono text-xs underline-offset-2 hover:underline', privateOnly() ? 'text-muted' : 'text-brass-strong')}
                                                  title={privateOnly() ? `http://${address()}\n\n${PUBLIC_ADDRESS_HINT}` : `Open http://${address()}`}
                                                >
                                                  {address()}
                                                </a>
                                                <CopyButton text={address()!} label="" />
                                              </span>
                                            </Show>
                                          </td>
                                        </tr>
                                      )
                                    }}
                                  </Index>
                                </tbody>
                              </table>
                            </TableShell>
                          </Show>
                        </>
                      )}
                    </Show>

                    <Show when={taskSpec()?.service}>
                      <div>
                        <p class="eyebrow mb-1.5">
                          Service{' '}
                          <A
                            class="normal-case tracking-normal text-brass-strong hover:underline"
                            href={`/services/${encodeURIComponent(taskSpec()!.service!.name ?? taskSpec()!.name)}`}
                          >
                            {taskSpec()!.service!.name ?? taskSpec()!.name}
                          </A>
                        </p>
                        <Show
                          when={endpoints().length > 0}
                          fallback={
                            <p class="text-sm text-muted">
                              Not registered yet. An endpoint appears once the allocation has its port and the manager has a health
                              report for it.
                            </p>
                          }
                        >
                          <ul class="space-y-1.5">
                            <Index each={endpoints()}>
                              {(endpoint) => (
                                <li class="flex flex-wrap items-center gap-2 rounded-md border border-line bg-surface2/40 px-2.5 py-1.5 text-sm">
                                  <EndpointAddress endpoint={endpoint()} />
                                  <Show when={endpoint().portLabel}>
                                    <span class="text-2xs text-faint">port {endpoint().portLabel}</span>
                                  </Show>
                                  <StatusBadge status={endpoint().healthy ? 'healthy' : 'unhealthy'} title={endpoint().healthStatus || 'No health report yet'} />
                                  <Show when={endpoint().meshTls}>
                                    <Badge tone="info" title="The advertised port terminates mesh TLS; plain clients are refused at the handshake">mesh TLS</Badge>
                                  </Show>
                                  <ServiceTags tags={endpoint().tags} meta={endpoint().meta} />
                                </li>
                              )}
                            </Index>
                          </ul>
                        </Show>
                      </div>
                    </Show>
                  </div>
                </SectionCard>
                </Show>

                {/* --- 4. What is it using ----------------------------------- */}
                <SectionCard
                  title="Resources"
                  description="What it reserved, and what its worker last measured."
                  actions={
                    <Show when={usageReply()?.usageReported && usageReply()!.sampledAtUnixMs > 0n}>
                      <span class="text-xs text-faint" title={`Reports arrive every 10 seconds; this one is ${String(usageReply()!.reportAgeSeconds)}s old`}>
                        sampled <RelativeTime value={new Date(Number(usageReply()!.sampledAtUnixMs)).toISOString()} />
                        <Show when={usageReply()!.reportAgeSeconds > 30n}>
                          <Badge tone="warn" class="ml-1.5">stale</Badge>
                        </Show>
                      </span>
                    </Show>
                  }
                >
                  <div class="space-y-4">
                    <Show
                      when={usageEntry()?.usage}
                      fallback={
                        <p class="text-sm text-muted">
                          <Show
                            when={usageReply()}
                            fallback={
                              <>
                                Measured usage is not available
                                {current()?.ledgerDenied ? ': reading it needs node:read.' : ' for this worker.'}
                              </>
                            }
                          >
                            <Show
                              when={usageReply()!.usageReported}
                              fallback={
                                <>
                                  Worker {nodeId()} has not reported usage. Possible reasons: {UNREPORTED_REASONS.join('; ')}.
                                </>
                              }
                            >
                              Not measured: the worker reports usage only for runtimes it governs with a cgroup, and this allocation has
                              no sample yet.
                            </Show>
                          </Show>
                        </p>
                      }
                    >
                      {(usage) => {
                        const entry = () => usageEntry()!
                        const used = () => bytesToMiB(usage().memoryCurrentBytes)
                        const peak = () => (usage().memoryPeakBytes > 0n ? bytesToMiB(usage().memoryPeakBytes) : undefined)
                        const cpu = () => usageReply()?.cpu
                        // MHz per physical core, when the node reports a clock.
                        const mhzPerCore = () => {
                          const value = cpu()
                          return value && value.capacity > 0 && value.cores > 0 ? physicalCapacity(value) / value.cores : undefined
                        }
                        return (
                          <>
                            <div>
                              <div class="mb-1.5 flex flex-wrap items-baseline justify-between gap-x-3 text-sm">
                                <p class="font-medium text-ink">Memory</p>
                                <p class="font-mono text-xs tabular-nums text-muted">
                                  <span class="text-ink">{formatBytes(Number(usage().memoryCurrentBytes))}</span>
                                  <Show when={entry().reservedMemoryMib > 0}> of {formatMiB(entry().reservedMemoryMib)} MiB reserved</Show>
                                </p>
                              </div>
                              <MemoryGauge
                                usedMiB={used()}
                                peakMiB={peak()}
                                reservedMiB={entry().reservedMemoryMib}
                                ceilingMiB={entry().memoryMaxMib || undefined}
                              />
                              <p class="mt-1.5 flex flex-wrap gap-x-3 gap-y-0.5 font-mono text-2xs tabular-nums text-faint">
                                <Show when={peak() !== undefined}>
                                  <span title="Highest use since the allocation started">peak {formatMiB(peak()!)} MiB</span>
                                </Show>
                                <Show when={usage().memoryAnonBytes > 0n}>
                                  <span title="Heap, stack, guest RAM: what only stopping or checkpointing gives back">anon {formatMiB(bytesToMiB(usage().memoryAnonBytes))} MiB</span>
                                </Show>
                                <span title={entry().memoryMaxMib > 0 ? 'resources.memoryMax: the hard limit' : 'No memoryMax: the reservation is the hard limit'}>
                                  limit {formatMiB(entry().memoryMaxMib > 0 ? entry().memoryMaxMib : entry().reservedMemoryMib)} MiB
                                  {entry().memoryMaxMib > 0 ? ' (memoryMax)' : ' (= reservation)'}
                                </span>
                                <Show when={usage().psiMemorySomeAvg10 > 0}>
                                  <span title="Share of the last 10s some task in the allocation stalled waiting on memory">
                                    stall {usage().psiMemorySomeAvg10.toFixed(1)}%
                                  </span>
                                </Show>
                              </p>
                            </div>

                            <dl class="grid grid-cols-2 gap-x-6 gap-y-3 text-sm">
                              <Fact label="CPU">
                                <span class="font-mono text-xs tabular-nums">{formatCores(usage().cpuMillicores)} cores</span>
                                <Show when={mhzPerCore() && entry().reservedCpuMhz > 0}>
                                  <span class="ml-1 font-mono text-2xs font-normal text-faint" title="cpu is a proportional share that only matters under contention, not a cap">
                                    ≈{Math.round((Number(usage().cpuMillicores) / 1000) * mhzPerCore()!)} of {Math.round(entry().reservedCpuMhz)} MHz share
                                  </span>
                                </Show>
                              </Fact>
                              <Fact label="Idle" title="Time since it last used 1% of a core, moved traffic, or had an exec session open">
                                <span class="font-mono text-xs tabular-nums">{entry().idleSeconds > 0n ? formatIdle(entry().idleSeconds) : 'active'}</span>
                                <Show when={usage().idleAfterSeconds > 0n}>
                                  <span class="ml-1 text-2xs font-normal text-faint">suspends after {formatIdle(usage().idleAfterSeconds)} idle</span>
                                </Show>
                              </Fact>
                              <Fact label="Under pressure" title="Where it stands in the node's memory pressure order: 1 is acted on first">
                                <Show when={entry().pressureRank > 0} fallback={<Faint>not in the order</Faint>}>
                                  <span class="inline-flex flex-wrap items-center gap-1.5">
                                    <span class="font-mono text-xs">#{entry().pressureRank}</span>
                                    <PressureSteps steps={entry().pressureSteps} />
                                  </span>
                                </Show>
                              </Fact>
                              <Fact label="Worker memory">
                                {(() => {
                                  const summary = () => pressureSummary(usageReply()?.memory, Boolean(usageReply()?.usageReported))
                                  return (
                                    <span class="inline-flex flex-wrap items-center gap-1.5" title={summary().title}>
                                      <Badge tone={summary().tone} dot>{summary().label}</Badge>
                                      <Show when={summary().detail}>
                                        <span class="text-2xs font-normal text-faint">{summary().detail}</span>
                                      </Show>
                                    </span>
                                  )
                                })()}
                              </Fact>
                              <Show when={entry().band}>
                                <Fact label="Priority band" title="Lower bands are acted on first under pressure and can be preempted by higher ones">
                                  <span class="font-mono text-xs">{entry().band}</span>
                                </Fact>
                              </Show>
                            </dl>
                          </>
                        )
                      }}
                    </Show>

                    <div>
                      <p class="eyebrow mb-1.5">Reserved (as declared)</p>
                      <Show
                        when={reservedResources().length > 0}
                        fallback={
                          <p class="text-sm text-muted">
                            <Show when={ledgerEntry() || stateRow()} fallback="Not visible without node:read or state:read.">
                              Nothing declared: the scheduler reserves its defaults, and nothing is enforced.
                            </Show>
                          </p>
                        }
                      >
                        <dl class="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:grid-cols-3">
                          <Index each={reservedResources()}>
                            {(resource) => (
                              <Fact label={resource().label} title={resource().title}>
                                <span class="font-mono text-xs">{resource().value}</span>
                              </Fact>
                            )}
                          </Index>
                        </dl>
                      </Show>
                    </div>
                  </div>
                </SectionCard>
              </div>
            </Show>

            {/* --- 5. What happened ---------------------------------------- */}
            <div id="allocation-events" class="scroll-mt-20">
              <SectionCard
                title="Events"
                description="Scheduler events about this allocation, newest first. The manager's event journal is bounded, so older history may have been pruned."
                actions={
                  <Show when={feed.status() === 'ready'}>
                    <span class="flex items-center gap-1.5 text-xs text-faint" role="status">
                      <Dot tone="ok" pulse={exists()} />
                      {pluralize(feed.events().length, 'event')}
                    </span>
                  </Show>
                }
                flush
              >
                <Show when={feed.status() !== 'denied'} fallback={<p class="px-4 py-4 text-sm text-muted">Event history needs <code class="font-mono text-xs">operator:read</code>, which your token doesn't have.</p>}>
                  <Show when={feed.status() !== 'error'} fallback={<p class="px-4 py-4 text-sm text-red-600 dark:text-red-400">Could not load events: {feed.error()}</p>}>
                    <Show when={feed.events().length > 0} fallback={
                      <Show when={feed.status() === 'ready'} fallback={<div class="px-4 py-4"><Spinner label="Loading events…" /></div>}>
                        <EmptyState icon="history" title="No events" body="Nothing in the journal names this allocation. Placement events arrive once its job is known; old events may have been pruned." />
                      </Show>
                    }>
                      <ul class="divide-y divide-line">
                        <For each={[...feed.events()].reverse()}>{(event) => <EventRow event={event} allocationId={allocationId()!} />}</For>
                      </ul>
                    </Show>
                  </Show>
                </Show>
              </SectionCard>
            </div>

            {/* --- 6. Look inside ------------------------------------------ */}
            <Show when={!envelopeFor()}>
            <div id="allocation-logs" class="scroll-mt-20">
              <SectionCard title="Logs" description="stdout and stderr captured by the worker, tailed live.">
                <Show
                  when={canTailLogs()}
                  fallback={
                    <p class="text-sm text-muted">
                      <Show
                        when={gone()}
                        fallback="Logs are read through the allocation's job, which is not known yet."
                      >
                        The manager reads logs through the allocation's record, which is gone. The worker may still hold them on disk at{' '}
                        <code class="font-mono text-xs">&lt;worker state dir&gt;/logs/{String(allocationId())}/</code>
                        <Show when={nodeId()}> on {nodeId()}</Show> until its log retention removes them.
                      </Show>
                    </p>
                  }
                >
                  <LogViewer job={jobId()!} allocationIds={[allocationId()!]} />
                </Show>
              </SectionCard>
            </div>
            </Show>

            <Show when={exists() && !envelopeFor()}>
              <div id="allocation-exec" class="scroll-mt-20">
                <SectionCard title="Exec & actions" description="Run a command, open a shell, or trigger a declared action inside this allocation.">
                  <Show
                    when={canExec()}
                    fallback={
                      <EmptyState
                        icon="terminal"
                        title="Not running"
                        body={`Exec needs a running allocation; this one is ${statusParts().word || 'not running'}.`}
                      />
                    }
                  >
                    <TaskActions
                      jobName={jobId()!}
                      taskName={taskName()!}
                      actions={jobActions() ?? []}
                      specActions={taskSpec()?.actions ?? []}
                      targets={actionTargets()}
                      selected={selectedAction()}
                      onSelect={setActionEntry}
                      preferredTarget={actionTargets()[0]?.id}
                    />
                  </Show>
                </SectionCard>
              </div>
            </Show>

            <div class="grid items-start gap-4 lg:grid-cols-2">
              {/* --- 7. Storage --------------------------------------------- */}
              <SectionCard
                title="Storage"
                description={
                  gone()
                    ? "Volumes it mounted: its instance's own, which outlive any one allocation, and claims shared by its task."
                    : "Volumes this allocation mounts: its instance's own, and claims shared by its task."
                }
                flush
              >
                <Show
                  when={volumes().length > 0}
                  fallback={
                    <p class="px-4 py-4 text-sm text-muted">
                      <Show
                        when={instanceId()}
                        fallback="No volumes. A stateless allocation keeps nothing when it stops."
                      >
                        No volumes recorded for instance <span class="font-mono">{instanceId()}</span>.
                      </Show>
                      <Show when={taskSpec()?.stateful?.storagePath}>
                        {' '}The task sees its instance storage at <code class="font-mono text-xs">{taskSpec()!.stateful!.storagePath}</code>.
                      </Show>
                    </p>
                  }
                >
                  <ul class="divide-y divide-line">
                    <Index each={volumes()}>
                      {(volume) => (
                        <li class="px-4 py-2.5 text-sm">
                          <div class="flex flex-wrap items-center gap-2">
                            <A
                              href={`/volumes/${encodeURIComponent(volume().volumeId)}`}
                              class="font-mono text-xs font-semibold text-ink hover:text-brass-strong"
                              title={volume().volumeId}
                            >
                              {shortVolumeId(volume().volumeId)}
                            </A>
                            <span class="text-xs text-muted">{volume().name}</span>
                            <Badge tone={volume().nodeLocal ? 'neutral' : 'ok'}>{volume().driver}</Badge>
                            <Badge tone={volumeStateTone(volume())} dot class="uppercase tracking-wide">
                              {volume().state}
                            </Badge>
                            <Show when={!volume().instanceId}>
                              <Badge tone="neutral" title="A task volume claim: shared by every allocation of the task">shared</Badge>
                            </Show>
                            <Show when={volume().readOnly}>
                              <Badge tone="warn">read-only</Badge>
                            </Show>
                          </div>
                          <Show when={volumePinsPlacement(volume())}>
                            <p class="mt-1 text-2xs text-faint">Node-local on {volume().nodeId}: its instance can only be replaced there.</p>
                          </Show>
                          <Show when={volume().mountPath}>
                            <p class="mt-1 truncate font-mono text-2xs text-faint" title={volume().mountPath}>
                              {volume().mountPath}
                            </p>
                          </Show>
                          <Show when={volume().lastError}>
                            <p class="mt-1 break-words text-2xs text-red-600 dark:text-red-400">{volume().lastError}</p>
                          </Show>
                        </li>
                      )}
                    </Index>
                  </ul>
                </Show>
              </SectionCard>

              {/* --- 8. Act ------------------------------------------------- */}
              <SectionCard title="Act on it">
                <div class="space-y-4 text-sm">
                  <p class="text-muted">
                    Legion has no per-allocation restart or stop: an allocation changes when its job, task, or instance does.
                  </p>
                  <ul class="space-y-1.5">
                    <Show when={instanceId() && jobId()}>
                      <li>
                        <A class={linkClass} href={instanceHref(jobId()!, instanceId()!)}>Instance {instanceId()}</A>
                        <span class="text-muted"> — stop, start, suspend, or replace the stable identity it serves</span>
                      </li>
                    </Show>
                    <Show when={taskName() && jobId()}>
                      <li>
                        <A class={linkClass} href={taskHref(jobId()!, taskName()!)}>Task {taskName()}</A>
                        <span class="text-muted"> — scale it, or exec into any of its allocations</span>
                      </li>
                    </Show>
                    <Show when={jobId()}>
                      <li>
                        <A class={linkClass} href={jobHref(jobId()!)}>Job {jobId()}</A>
                        <span class="text-muted"> — update, stop, or roll it back</span>
                      </li>
                    </Show>
                    <Show when={nodeId() && exists()}>
                      <li>
                        <A class={linkClass} href={nodeHref(nodeId()!)}>Worker {nodeId()}</A>
                        <span class="text-muted"> — drain it to move this allocation elsewhere</span>
                      </li>
                    </Show>
                  </ul>
                  <div class="space-y-1.5">
                    <p class="eyebrow">From the CLI</p>
                    <Show when={running() && !envelopeFor()}>
                      <CliLine command={`legion exec ${String(allocationId())} -it -- /bin/sh`} />
                    </Show>
                    <Show when={envelopeFor() && jobId()}>
                      <CliLine command={`legion session nodes ${jobId()} --task ${envelopeFor()}`} note="Each node's envelope for the task: its sessions, room left, and whether it takes edge starts" />
                    </Show>
                    <Show when={jobId() && exists() && !envelopeFor()}>
                      <CliLine command={`legion job logs ${jobId()} --allocation ${String(allocationId())} --follow`} />
                    </Show>
                    <CliLine command={`legion events list --allocation ${String(allocationId())}`} />
                    <Show when={nodeId() && exists()}>
                      <CliLine command={`legion node usage ${nodeId()}`} note="Shows this allocation's row with its node's other allocations" />
                    </Show>
                  </div>
                </div>
              </SectionCard>
            </div>
          </div>
        </Show>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate). An allocation is part of a job, and
// everything beyond its placement row (spec, logs, rollouts, volumes) is
// job-scoped, so `job:read` is the page's gate; node, state, and operator
// reads are garnish that degrade on their own.
export default function AllocationDetail() {
  return (
    <AccessGate resource="job">
      <AllocationDetailContent />
    </AccessGate>
  )
}
