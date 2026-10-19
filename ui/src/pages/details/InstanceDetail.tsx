import { Code, ConnectError } from '@connectrpc/connect'
import { A, useNavigate, useParams } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal, type JSX } from 'solid-js'
import { AccessGate } from '../../components/AccessGate'
import { EventTimeline, createEventFeed, eventCategory } from '../../components/EventTimeline'
import { InstanceActionButtons, createInstanceActions } from '../../components/InstanceActions'
import { CheckpointCell, IdleCell, InstanceStateCell, instanceMemory, type InstanceRow } from '../../components/InstancesTable'
import { SnapshotsTable } from '../../components/SnapshotsPanel'
import {
  Badge,
  ConfirmButton,
  CopyButton,
  DefList,
  Dot,
  EmptyState,
  FilterChips,
  Icon,
  LoadingRows,
  Meter,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  Spinner,
  StatusBadge,
  cx,
  linkClass,
} from '../../components/ui'
import { GateLevel, InstanceInfo, ProvisionalClaimReport, SnapshotInfo, type CheckpointSupport, type NodeAllocationUsage } from '../../gen/legion_pb'
import { managerClient } from '../../grpcClient'
import { formatDateTime } from '../../utils/format'
import {
  formatMs,
  instanceHref,
  pendingOpLabel,
  stableList,
  stopCauseLabel,
  stopCauseTone,
  suspendReasonLabel,
  volumeIdFromHandle,
} from '../../utils/instances'
import { extractJobSpecData, type JobTask } from '../../utils/jobspec'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../../utils/polling'
import { LARGE_LISTING, SESSION_COPY, isSession, jobInstancesHref } from '../../utils/sessions'
import { mayReadSnapshots, restoreBlocker } from '../../utils/snapshots'
import { statusTone } from '../../utils/status'
import { fetchNodeUsage, formatCores, formatIdle, formatMiB, heldLabel, percentOf, reservationLabel } from '../../utils/usage'
import { jobIdFromParams, jobPath } from '../../utils/namespaces'

/*
 * One stateful instance (docs/stateful-workloads.md,
 * docs/suspend-and-resume.md): where it runs, what it holds, what it can
 * checkpoint, the controls for its lifecycle, its snapshots, and the events
 * about it.
 *
 * GetInstance is polled at the live cadence and is the page's spine: it
 * carries the state, last stop, checkpoint, and usage, so a job-scoped
 * token sees nearly everything. The rest is garnish fetched at the slow
 * cadence, each part degrading on its own: the node (GetLedger, node:read),
 * the reservation (GetNodeUsage, node:read; the task spec otherwise), forks
 * (ListInstances), snapshots (snapshot:read), and events (operator:read).
 *
 * A session (docs/session-mode.md) gets the same page in its own words:
 * its node and envelope come from GetInstance itself, it offers Promote and
 * End, and the cards about allocations, checkpoints, and snapshots say why
 * they are empty. Forks are not looked up for a session (it cannot have
 * any); for anything else the listing asks for no sessions
 * (`session_limit: 0`), since a fork is always a row.
 * Unchanged objects are kept across polls, so an armed confirm or an open
 * fork form survives every refresh.
 */

type Loaded = {
  kind: 'ok'
  id: string
  instance: InstanceInfo
  provisional?: ProvisionalClaimReport
  provisionalNodeId: string
}
type Loading = Loaded | { kind: 'missing'; id: string } | { kind: 'failed'; id: string; message: string }

const isDenied = (error: unknown) =>
  error instanceof ConnectError && [Code.PermissionDenied, Code.Unauthenticated].includes(error.code)

/**
 * A garnish fetcher that never rejects: the last good value for the same
 * source through an outage (`retainOnError`), `fallback` before there is
 * one. A resource in error throws when read, and garnish must never take
 * the page down with it.
 */
const garnish = <S, T>(fetcher: (source: S) => Promise<T>, key: (source: S) => string, fallback: T) => {
  const retained = retainOnError(fetcher, key)
  return (source: S): Promise<T> => retained(source).catch(() => fallback)
}

// Event categories in the order an instance's history is usually read.
const CATEGORY_ORDER = ['instance', 'allocation', 'snapshot', 'node', 'volume']

const MODE_COPY: Record<string, string> = {
  keyed: 'Keyed: created on demand through the API, never recycled while its job exists.',
  slot: 'Slot: one of the ordinal slots 0..count.num-1. A replacement for the slot reuses this id and its storage.',
  session: SESSION_COPY,
}

const STATE_COPY: Record<string, string> = {
  pending: 'Waiting to be placed. Its id and storage are bound already.',
  active: 'A live allocation fulfills it.',
  rescheduling: 'Its allocation died. The replacement reuses this id and storage.',
  stopped: 'Stopped: no allocation and not scheduled, but its id and storage are kept. Start brings the same instance back.',
  suspended: 'Checkpointed and stopped. It holds no allocation, and a resume restores it from the checkpoint.',
  terminated: 'No longer desired. The row is kept so the id is never reused while the job exists.',
}

// A session reports `pending` while its node admits it and `active` once it runs.
const SESSION_STATE_COPY: Record<string, string> = {
  pending: 'Starting: its node admitted it into the envelope and is starting it.',
  active: 'Running on its node, inside the envelope allocation that reserves its room.',
}

function InstanceDetailContent() {
  const params = useParams()
  const navigate = useNavigate()
  const instanceId = createMemo(() => decodeURIComponent(params.instanceId))
  const routeJob = createMemo(() => jobIdFromParams({ namespace: decodeURIComponent(params.namespace), name: decodeURIComponent(params.name) }))
  const tick = usePoll()
  const slowTick = usePoll(INDEX_POLL_INTERVAL_MS)

  // GetInstance, keeping the previous object when nothing changed so the
  // controls under it do not re-render. NotFound is an answer (the id is
  // wrong, or the job was destroyed), not an error, and it wins even after
  // a success; any other failure keeps the last good instance.
  let last: Loaded | undefined
  const [data, { refetch }] = createResource(
    () => [instanceId(), tick()] as const,
    async ([id]): Promise<Loading> => {
      try {
        const reply = await managerClient.getInstance({ instanceId: id })
        if (!reply.instance) return { kind: 'missing', id }
        const held = last?.id === id ? last : undefined
        const instance = held && InstanceInfo.equals(held.instance, reply.instance) ? held.instance : reply.instance
        const provisional =
          held?.provisional && reply.provisional && ProvisionalClaimReport.equals(held.provisional, reply.provisional) ? held.provisional : reply.provisional
        last = { kind: 'ok', id, instance, provisional, provisionalNodeId: reply.provisionalNodeId }
        return last
      } catch (error) {
        if (error instanceof ConnectError && error.code === Code.NotFound) return { kind: 'missing', id }
        if (last?.id === id) return last
        return { kind: 'failed', id, message: error instanceof Error ? error.message : String(error) }
      }
    },
  )
  // Only this route's answer: navigating instance to instance shows the
  // spinner, never the previous instance's page.
  const current = () => {
    const value = data()
    return value?.id === instanceId() ? value : undefined
  }
  const loaded = () => {
    const value = current()
    return value?.kind === 'ok' ? value : undefined
  }
  const instance = createMemo(() => loaded()?.instance)
  const jobId = createMemo(() => instance()?.jobId ?? routeJob())
  const allocId = createMemo(() => instance()?.currentAllocId)

  // Forks of this instance, and the task spec (for the reservation and the
  // suspend plan when the node cannot be read).
  //
  // Forks come from ListInstances with `session_limit: 0`: forks are rows,
  // so the job's sessions are never read here, however many it runs. A
  // session (which has no forks) skips it, and a listing of LARGE_LISTING or
  // more rows is kept rather than re-read every few seconds; an action here
  // re-reads it.
  const mergeInstances = stableList<InstanceInfo>((entry) => entry.instanceId, (a, b) => InstanceInfo.equals(a, b))
  const session = createMemo(() => (instance() ? isSession(instance()!) : undefined))
  let forkListing: { job: string; instances: InstanceInfo[] } | undefined
  let forceForks = false
  const [jobContext, { refetch: refetchContext }] = createResource(
    () => (session() === undefined ? undefined : ([jobId(), session()!, slowTick()] as const)),
    garnish(
      async ([job, isSessionInstance]: readonly [string, boolean, number]) => {
        const tasks = await managerClient
          .getJobSpec({ name: job })
          .then((reply) => extractJobSpecData(reply.submissions)?.tasks ?? [])
          .catch((): JobTask[] => [])
        const forced = forceForks
        forceForks = false
        if (isSessionInstance) return { job, instances: undefined as InstanceInfo[] | undefined, tasks }
        const kept = forkListing?.job === job && forkListing.instances.length >= LARGE_LISTING && !forced ? forkListing : undefined
        const instances = kept?.instances ?? mergeInstances((await managerClient.listInstances({ jobId: job, sessionLimit: 0 })).instances)
        forkListing = { job, instances }
        return { job, instances: instances as InstanceInfo[] | undefined, tasks }
      },
      ([job, isSessionInstance]) => `${job}/${String(isSessionInstance)}`,
      undefined,
    ),
  )
  const refetchJob = () => {
    forceForks = true
    void refetchContext()
  }
  const context = () => (jobContext()?.job === jobId() ? jobContext() : undefined)
  const task = createMemo(() => context()?.tasks.find((entry) => entry.name === instance()?.task))
  // Undefined when forks are not looked up (see above).
  const forks = createMemo(() => context()?.instances?.filter((entry) => entry.forkedFrom === instanceId()))

  // The node running the current allocation, from the ledger (node:read).
  const [placement] = createResource(
    () => {
      const alloc = allocId()
      return alloc === undefined ? undefined : ([String(alloc), slowTick()] as const)
    },
    garnish(
      async ([alloc]: readonly [string, number]) => {
        try {
          const ledger = await managerClient.getLedger({})
          for (const entry of ledger.nodes) {
            const match = entry.allocations.find((allocation) => String(allocation.id) === alloc)
            if (match) return { alloc, denied: false, nodeId: match.nodeId || entry.node?.id, status: match.status }
          }
          return { alloc, denied: false, nodeId: undefined, status: undefined }
        } catch (error) {
          if (isDenied(error)) return { alloc, denied: true, nodeId: undefined, status: undefined }
          throw error
        }
      },
      ([alloc]) => alloc,
      undefined,
    ),
  )
  const currentPlacement = () => {
    const value = placement()
    return value && allocId() !== undefined && value.alloc === String(allocId()) ? value : undefined
  }
  const nodeId = createMemo(() => currentPlacement()?.nodeId)

  // The node's view of the allocation: reservation, ceiling, reserved CPU.
  const [nodeAllocation] = createResource(
    () => {
      const node = nodeId()
      const alloc = allocId()
      return node && alloc !== undefined ? ([node, String(alloc), slowTick()] as const) : undefined
    },
    garnish(
      async ([node, alloc]: readonly [string, string, number]) => {
        const reply = await fetchNodeUsage(node)
        return { alloc, usage: reply?.allocations.find((allocation) => String(allocation.allocationId) === alloc) }
      },
      ([node, alloc]) => `${node}/${alloc}`,
      undefined,
    ),
  )

  const row = createMemo<InstanceRow | undefined>(() => {
    const inst = instance()
    if (!inst) return undefined
    const usage = nodeAllocation()
    const allocation: NodeAllocationUsage | undefined =
      usage && inst.currentAllocId !== undefined && usage.alloc === String(inst.currentAllocId) ? usage.usage : undefined
    return { instance: inst, task: task(), allocation }
  })

  // This instance's snapshots. The manager drops rows the caller cannot
  // read; without snapshot:read the card is hidden, as on the job page.
  const mergeSnapshots = stableList<SnapshotInfo>((snapshot) => snapshot.snapshotId, (a, b) => SnapshotInfo.equals(a, b))
  const [snapshots, { refetch: refetchSnapshots }] = createResource(
    () => (instance() && mayReadSnapshots(jobId()) ? ([jobId(), instanceId(), tick()] as const) : undefined),
    garnish(
      async ([job, id]: readonly [string, string, number]) =>
        mergeSnapshots((await managerClient.listSnapshots({ jobId: job, instanceId: id })).snapshots),
      ([job, id]) => `${job}/${id}`,
      [] as SnapshotInfo[],
    ),
  )

  const actions = createInstanceActions({
    onChanged: () => {
      void refetch()
      void refetchSnapshots()
      // A fork adds instances this page lists under Fork lineage.
      refetchJob()
    },
    // A promotion that renamed the session lives at its new id.
    onPromoted: (promoted) => {
      if (promoted !== instanceId()) navigate(instanceHref(jobId(), promoted))
    },
  })
  // What this id was when it last answered, so a session that ends reads as
  // ended rather than as a mistyped id.
  const [lastMode, setLastMode] = createSignal<{ id: string; mode: string }>()
  createEffect(() => {
    const current = instance()
    if (current) setLastMode({ id: current.instanceId, mode: current.mode })
  })
  const endedSession = () => lastMode()?.id === instanceId() && lastMode()?.mode === 'session'

  // Events about this instance: its own (metadata names it) and its
  // allocations'. Starts with the route, so a missing instance still shows
  // what happened to it while the journal remembers.
  const feed = createEventFeed(() => ({ instanceId: instanceId() }), tick, { limit: 200 })

  return (
    <Show
      when={current()}
      fallback={
        <div class="py-6">
          <Spinner label="Loading instance…" />
        </div>
      }
    >
      {(state) => (
        <Show when={state().kind === 'ok'} fallback={<NotLoaded state={state()} job={routeJob()} feed={feed} session={endedSession()} />}>
          <div class="space-y-4">
            <Header
              instance={instance()!}
              row={() => row()!}
              actions={actions}
            />

            <Show when={actions.outcome()}>
              {(result) => (
                <div aria-live="polite">
                  <Note tone={result().failed ? 'danger' : 'ok'} onDismiss={actions.dismiss}>
                    <span class="whitespace-pre-line">{result().message}</span>
                    <For each={result().forks ?? []}>
                      {(id) => (
                        <A href={instanceHref(jobId(), id)} class="ml-1.5 font-mono underline-offset-2 hover:underline">
                          {id}
                        </A>
                      )}
                    </For>
                  </Note>
                </div>
              )}
            </Show>

            <Callouts
              instance={instance()!}
              routeJob={routeJob()}
              provisional={loaded()?.provisional}
              provisionalNodeId={loaded()?.provisionalNodeId ?? ''}
            />

            <SectionCard title="Overview" description={(session() ? SESSION_STATE_COPY : STATE_COPY)[instance()!.state]}>
              <DefList
                cols={3}
                items={[
                  ['State', <InstanceStateCell instance={instance()!} />],
                  [
                    'Mode',
                    <span title={MODE_COPY[instance()!.mode]}>
                      {instance()!.mode}
                      <Show when={session() && !instance()!.durable}>
                        <span class="text-faint"> · not durable</span>
                      </Show>
                      <Show when={instance()!.slotIndex !== undefined}>
                        <span class="text-faint"> · slot {String(instance()!.slotIndex)}</span>
                      </Show>
                    </span>,
                  ],
                  [
                    'Task',
                    <A href={`${jobPath(jobId())}/tasks/${encodeURIComponent(instance()!.task)}`} class={linkClass}>
                      {instance()!.task}
                    </A>,
                  ],
                  [
                    'Job',
                    <A href={`${jobPath(jobId())}`} class={linkClass}>
                      {jobId()}
                    </A>,
                  ],
                  ['Group', <span class="font-mono text-xs">{instance()!.group || '—'}</span>],
                  [
                    'Allocation',
                    <Show
                      when={allocId() !== undefined}
                      fallback={
                        <Show when={session() && instance()!.envelopeAllocId !== undefined} fallback={<span class="text-faint">{noAllocationCopy(instance()!)}</span>}>
                          <span class="text-faint" title="A session has no allocation of its own: it is admitted into its node's envelope, which reserves the room">
                            none: runs in envelope{' '}
                            <A href={`/allocations/${String(instance()!.envelopeAllocId)}`} class={cx(linkClass, 'font-mono text-xs')}>
                              #{String(instance()!.envelopeAllocId)}
                            </A>
                          </span>
                        </Show>
                      }
                    >
                      <span class="inline-flex flex-wrap items-center gap-2">
                        <A href={`/allocations/${String(allocId())}`} class={cx(linkClass, 'font-mono text-xs')}>
                          #{String(allocId())}
                        </A>
                        <Show when={currentPlacement()?.status}>{(status) => <StatusBadge status={status()} />}</Show>
                      </span>
                    </Show>,
                  ],
                  [
                    'Node',
                    <Show
                      when={nodeId() ?? instance()!.nodeId}
                      fallback={
                        <Show
                          when={allocId() === undefined && instance()!.checkpointNodeId}
                          fallback={
                            <span class="text-faint" title={currentPlacement()?.denied ? 'Placement comes from the ledger, which needs node:read' : undefined}>
                              {allocId() === undefined ? '—' : currentPlacement()?.denied ? 'hidden (needs node:read)' : placement.loading ? '…' : '—'}
                            </span>
                          }
                        >
                          {(node) => (
                            <span class="text-faint" title="No allocation; this node holds the checkpoint a resume restores from">
                              checkpoint on{' '}
                              <A href={`/nodes/${encodeURIComponent(node())}`} class={linkClass}>
                                {node()}
                              </A>
                            </span>
                          )}
                        </Show>
                      }
                    >
                      {(node) => (
                        <A href={`/nodes/${encodeURIComponent(node())}`} class={linkClass}>
                          {node()}
                        </A>
                      )}
                    </Show>,
                  ],
                  [
                    'Storage',
                    <Show
                      when={instance()!.storageHandle}
                      fallback={<span class="text-faint" title="The task declares no stateful.storage volume">none</span>}
                    >
                      {(handle) => (
                        <A
                          href={`/volumes/${encodeURIComponent(volumeIdFromHandle(handle()))}`}
                          class={cx(linkClass, 'break-all font-mono text-xs')}
                          title="The volume holding this instance's data. It survives reschedules, stops, and suspends."
                        >
                          {handle()}
                        </A>
                      )}
                    </Show>,
                  ],
                  ['Created', <RelativeTime value={instance()!.createdAt} />],
                  ['Updated', <RelativeTime value={instance()!.updatedAt} title="Last state or binding change" />],
                  [
                    'Fork lineage',
                    <ForkLineage
                      jobId={jobId()}
                      origin={instance()!.forkedFrom}
                      forks={forks()}
                      loading={context() === undefined && jobContext.loading}
                      session={session()}
                    />,
                  ],
                ]}
              />
              <Show when={instance()!.lastStop}>
                {(stop) => (
                  <div class="mt-4 rounded-md border border-line bg-surface2/40 p-3 text-sm">
                    <div class="flex flex-wrap items-center gap-x-2 gap-y-1">
                      <span class="eyebrow">Last stop</span>
                      <Badge tone={stopCauseTone(stop().cause)}>{stopCauseLabel(stop().cause)}</Badge>
                      <RelativeTime value={stop().at} class="text-xs text-faint" />
                      <Show when={stop().allocationId > 0n}>
                        <A href={`/allocations/${String(stop().allocationId)}`} class="font-mono text-xs text-muted hover:text-brass-strong">
                          #{String(stop().allocationId)}
                        </A>
                      </Show>
                      <Show when={stop().nodeId}>
                        <span class="text-xs text-faint">
                          on{' '}
                          <A href={`/nodes/${encodeURIComponent(stop().nodeId)}`} class="text-muted hover:text-brass-strong">
                            {stop().nodeId}
                          </A>
                        </span>
                      </Show>
                      <Show when={stop().preemptorJob}>
                        <span class="text-xs text-faint">
                          for{' '}
                          <A href={`${jobPath(stop().preemptorJob)}`} class="text-muted hover:text-brass-strong">
                            {stop().preemptorJob}
                          </A>
                        </span>
                      </Show>
                    </div>
                    <Show when={stop().detail}>
                      <p class="mt-1 break-words text-muted">{stop().detail}</p>
                    </Show>
                    <Show when={stop().hint}>
                      <p class="mt-1 text-amber-700 dark:text-amber-300">
                        <span class="mr-1.5 font-semibold">Hint</span>
                        {stop().hint}
                      </p>
                    </Show>
                  </div>
                )}
              </Show>
              <Show when={session()}>
                <p class="mt-4 text-xs text-faint">
                  Issued <RelativeTime value={instance()!.leaseIssuedAt} /> by the manager that routed it.{' '}
                  <Show when={instance()!.nodeId}>
                    {(node) => (
                      <A href={jobInstancesHref(jobId(), { mode: 'session', node: node() })} class={linkClass}>
                        This job's other sessions on {node()}
                      </A>
                    )}
                  </Show>
                </p>
              </Show>
              <Show when={instance()!.mode === 'keyed' && instance()!.state !== 'terminated'}>
                <p class="mt-4 text-xs text-faint">
                  Destroy from the CLI with{' '}
                  <code class="rounded bg-surface2 px-1 font-mono">legion instance destroy {instance()!.instanceId}</code>: it drops the
                  storage and tombstones the id for good.
                </p>
              </Show>
            </SectionCard>

            <div class="grid gap-4 lg:grid-cols-2">
              <ResourcesCard row={row()!} />
              <CheckpointCard instance={instance()!} />
            </div>

            <EventsCard feed={feed} instanceId={instanceId()} jobId={jobId()} />

            <Show when={mayReadSnapshots(jobId()) && !session()}>
              <SectionCard
                title="Snapshots"
                description="Point-in-time captures of this instance, newest first. Restore rolls it back in place: the running allocation is replaced by one started from the snapshot."
                flush
              >
                <Show
                  when={(snapshots() ?? []).length > 0}
                  fallback={
                    <EmptyState
                      icon="camera"
                      title={snapshots() === undefined ? 'Loading snapshots…' : 'No snapshots of this instance'}
                      body={
                        snapshots() === undefined ? undefined : (
                          <>
                            Take one on the job page, or with <code class="font-mono">legion snapshot create {instanceId()}</code>.
                          </>
                        )
                      }
                    />
                  }
                >
                  {/* `relative` keeps the table's sr-only Actions header
                      (absolutely positioned) inside the card on a phone. */}
                  <div class="relative">
                    <SnapshotsTable
                      snapshots={snapshots() ?? []}
                      actions={(snapshot) => {
                        const blocker = () => restoreBlocker(snapshot, instance(), jobId())
                        return (
                          <Show when={snapshot.purpose === 'manual' && (snapshot.state === 'local' || snapshot.state === 'published')}>
                            <span title={blocker()}>
                              <ConfirmButton
                                size="xs"
                                variant="subtle"
                                icon="history"
                                label="Restore"
                                confirmLabel="Replace the running allocation?"
                                disabled={actions.busy() || Boolean(blocker())}
                                onConfirm={() => void actions.restore(instance()!, snapshot.snapshotId)}
                              />
                            </span>
                          </Show>
                        )
                      }}
                    />
                  </div>
                </Show>
                <p class="border-t border-line px-4 py-2.5 text-xs text-faint">
                  Take, name, warm, and delete snapshots in the{' '}
                  <A href={`${jobPath(jobId())}`} class={linkClass}>
                    job's Snapshots card
                  </A>{' '}
                  or with <code class="rounded bg-surface2 px-1 font-mono">legion snapshot</code>.
                </p>
              </SectionCard>
            </Show>
          </div>
        </Show>
      )}
    </Show>
  )
}

/** Why there is no allocation, in the words of the state. */
const noAllocationCopy = (instance: InstanceInfo): string => {
  switch (instance.state) {
    case 'pending':
      return instance.pendingOp === 'forking' ? 'none yet: waiting for the checkpoint' : 'none yet: waiting for placement'
    case 'rescheduling':
      return 'none: a replacement is coming'
    case 'suspended':
      return 'none while suspended'
    case 'stopped':
      return 'none while stopped'
    case 'terminated':
      return 'none'
    default:
      return 'none'
  }
}

function Header(props: { instance: InstanceInfo; row: () => InstanceRow; actions: ReturnType<typeof createInstanceActions> }) {
  return (
    <PageHeader
      class="!mb-0"
      eyebrow={props.instance.mode === 'slot' ? 'Slot instance' : props.instance.mode === 'session' ? 'Session' : 'Instance'}
      title={<span class="font-mono text-lg md:text-xl">{props.instance.instanceId}</span>}
      badges={
        <span class="inline-flex flex-wrap items-center gap-1.5">
          <StatusBadge
            status={props.instance.state}
            title={[props.instance.detail, props.instance.state === 'suspended' ? suspendReasonLabel(props.instance.suspendReason) : undefined].filter(Boolean).join('\n') || undefined}
          />
          <Show when={props.instance.pendingOp}>
            {(op) => (
              <Badge tone="info" title={pendingOpLabel(op())}>
                {op()}
              </Badge>
            )}
          </Show>
          <Show when={props.instance.forkedFrom}>
            <Badge tone="neutral" title={`Forked from ${props.instance.forkedFrom}`}>
              fork
            </Badge>
          </Show>
          <Show when={props.instance.mode === 'session'}>
            <Badge tone="info" title={SESSION_COPY}>
              session · not durable
            </Badge>
          </Show>
          <Show when={props.instance.promotedFrom}>
            {(origin) => (
              <Badge tone="neutral" title={`Promoted in place from session ${origin()} (PromoteInstance)`}>
                promoted
              </Badge>
            )}
          </Show>
          <CopyButton text={props.instance.instanceId} label="" />
        </span>
      }
      subtitle={
        <>
          Task{' '}
          <A href={`${jobPath(props.instance.jobId)}/tasks/${encodeURIComponent(props.instance.task)}`} class={linkClass}>
            {props.instance.task}
          </A>{' '}
          of job{' '}
          <A href={`${jobPath(props.instance.jobId)}`} class={linkClass}>
            {props.instance.jobId}
          </A>
          <Show when={props.instance.group}>
            <span class="text-faint"> · group {props.instance.group}</span>
          </Show>
          <span class="text-faint">
            {' · '}created <RelativeTime value={props.instance.createdAt} />
            <Show when={props.instance.updatedAt && props.instance.updatedAt !== props.instance.createdAt}>
              {' · '}updated <RelativeTime value={props.instance.updatedAt} />
            </Show>
          </span>
        </>
      }
      actions={<InstanceActionButtons row={props.row} jobName={props.instance.jobId} actions={props.actions} size="sm" lifecycle />}
    />
  )
}

/** The notes that explain a non-obvious situation, loudest first. */
function Callouts(props: { instance: InstanceInfo; routeJob: string; provisional?: ProvisionalClaimReport; provisionalNodeId: string }) {
  const cold = () => props.instance.coldBoot
  // The cold boot is shown on its own; its detail line repeats it.
  const detail = () => {
    const text = props.instance.detail
    if (!text) return undefined
    const boot = cold()
    return boot && text.includes(boot.reason) ? undefined : text
  }
  const gated = () => detail()?.startsWith('pending-loss-confirmation')
  return (
    <>
      <Show when={props.instance.mode === 'session'}>
        <Note tone="info">
          <span class="font-medium">A session, not a keyed instance.</span> Nothing about it is in the replicated store: its node holds it and
          the managers list it from an in-memory index that can lag the node by a heartbeat. It ends when it exits, when you end it, or when its
          node is lost, and is not rescheduled. Promote it to give it a row in place, without a restart: it then reschedules on node loss and
          can be suspended, forked, and snapshotted.
        </Note>
      </Show>
      <Show when={props.instance.promotedFrom}>
        {(origin) => (
          <Note tone="neutral">
            Promoted from session <span class="font-mono text-xs">{origin()}</span>. Its node adopted the running session as this instance's
            allocation, so nothing restarted; the workload keeps the <code class="font-mono text-xs">LEGION_INSTANCE_ID</code> it started
            with until it restarts.
          </Note>
        )}
      </Show>
      <Show when={props.instance.jobId !== props.routeJob}>
        <Note tone="info">
          This instance belongs to job{' '}
          <A href={instanceHref(props.instance.jobId, props.instance.instanceId)} class={linkClass}>
            {props.instance.jobId}
          </A>
          , not {props.routeJob}.
        </Note>
      </Show>
      <Show when={cold()}>
        {(boot) => (
          <Note tone="warn">
            <span class="font-medium">Resumed cold</span> <RelativeTime value={boot().at} class="opacity-80" />: the restore from{' '}
            <span class="font-mono text-xs">{boot().snapshotId}</span> failed, so it booted fresh on its volume. Its running processes and memory
            were lost. <span class="opacity-80">{boot().reason}</span>
          </Note>
        )}
      </Show>
      <Show when={detail()}>
        {(text) => (
          <Note tone={gated() ? 'warn' : 'neutral'}>
            <span class="break-words">{text()}</span>
            <Show when={gated()}>
              <span class="mt-1 block text-xs opacity-80">
                If the node is confirmed dead,{' '}
                <code class="rounded bg-surface2 px-1 font-mono">legion instance replace {props.instance.instanceId}</code> frees it now. On a
                node that is only partitioned this can run the instance twice.
              </span>
            </Show>
          </Note>
        )}
      </Show>
      <Show when={props.provisional}>
        {(claim) => (
          <Note tone="info">
            <span class="font-medium">Started, not yet confirmed.</span>{' '}
            <Show when={props.provisionalNodeId} fallback="A node">
              <A href={`/nodes/${encodeURIComponent(props.provisionalNodeId)}`} class={linkClass}>
                {props.provisionalNodeId}
              </A>
            </Show>{' '}
            started it ahead of the placement (claim <span class="font-mono text-xs">{claim().claimId}</span>, {claim().stage || 'claimed'}, gate{' '}
            {(GateLevel[claim().gate] ?? String(claim().gate)).replace(/^GATE_/, '').toLowerCase()}). The node stops it at{' '}
            {formatDateTime(Number(claim().leaseDeadlineUnix))} if the manager has not ratified it by then.
          </Note>
        )}
      </Show>
      <Show when={props.instance.state === 'terminated'}>
        <Note tone="neutral">
          <Show
            when={props.instance.mode === 'keyed'}
            fallback="This slot is no longer desired (a scale-in or a job stop). Its row is kept, so a scale-out brings this id back."
          >
            Destroyed. The row is kept as a tombstone so the id is never reused while the job exists, and its storage was dropped.
          </Show>
        </Note>
      </Show>
    </>
  )
}

function ForkLineage(props: { jobId: string; origin?: string; forks?: readonly InstanceInfo[]; loading: boolean; session?: boolean }) {
  const forks = () => props.forks ?? []
  const fallback = () => {
    if (props.session) return 'none: a session cannot be forked'
    if (props.loading) return '…'
    if (props.forks === undefined) return 'not looked up'
    return 'not a fork, no forks'
  }
  return (
    <Show
      when={props.origin || forks().length > 0}
      fallback={<span class="text-faint">{fallback()}</span>}
    >
      <div class="space-y-1 text-xs">
        <Show when={props.origin}>
          {(origin) => (
            <div>
              <span class="text-faint">forked from </span>
              <A href={instanceHref(props.jobId, origin())} class={cx(linkClass, 'font-mono')}>
                {origin()}
              </A>
            </div>
          )}
        </Show>
        <Show when={forks().length > 0}>
          <div class="flex flex-wrap items-center gap-x-2 gap-y-1">
            <span class="text-faint">forks</span>
            <For each={forks()}>
              {(fork) => (
                <A href={instanceHref(props.jobId, fork.instanceId)} class="inline-flex items-center gap-1 font-mono text-ink hover:text-brass-strong" title={fork.state}>
                  <Dot tone={statusTone(fork.state)} />
                  {fork.instanceId}
                </A>
              )}
            </For>
          </div>
        </Show>
      </div>
    </Show>
  )
}

function Tile(props: { label: string; value: JSX.Element; valueClass?: string; title?: string; children?: JSX.Element }) {
  return (
    <div class="min-w-0 rounded-md border border-line bg-surface2/30 p-3" title={props.title}>
      <p class="eyebrow">{props.label}</p>
      <div class={cx('mt-1 font-display text-xl font-semibold tabular-nums tracking-tight', props.valueClass ?? 'text-ink')}>{props.value}</div>
      <div class="mt-1.5 space-y-1 text-2xs text-faint">{props.children}</div>
    </div>
  )
}

/** Memory, CPU, and idle of the current allocation, with what it reserved. */
function ResourcesCard(props: { row: InstanceRow }) {
  const instance = () => props.row.instance
  const memory = () => instanceMemory(props.row)
  const usage = () => instance().usage ?? props.row.allocation?.usage
  // The bar's scale is the burst ceiling when there is one, with a tick at
  // the reservation; otherwise the reservation.
  const scale = () => memory().max ?? memory().reserved
  const reservedCpu = () => {
    const mhz = props.row.allocation?.reservedCpuMhz
    if (mhz) return `${Math.round(mhz)} MHz reserved`
    const cpus = props.row.task?.resources?.cpus
    return cpus !== undefined ? `${cpus} cpus reserved` : undefined
  }
  const idleAfter = () => instance().idleAfterSeconds
  return (
    <SectionCard title="Resources" description="What the current allocation uses, measured by its node, against what it reserved.">
      <Show
        when={instance().currentAllocId !== undefined}
        fallback={
          <div class="space-y-1 text-sm text-muted">
            <p>
              {instance().mode === 'session'
                ? "A session has no allocation of its own: it runs inside its node's envelope, which reserves its room. Per-session usage is not reported; the envelope allocation's page shows the room it holds."
                : instance().state === 'suspended'
                  ? 'Suspended: it holds no allocation, so nothing runs or is measured.'
                  : 'No allocation right now, so nothing is reserved or measured.'}
            </p>
            <Show when={heldLabel(instance().heldMemoryMib, instance().heldCpuMhz)}>
              {(held) => (
                <p class="text-xs text-faint">
                  {held()} on {instance().checkpointNodeId ?? 'its node'} (suspend.reserve), so the resume finds room.
                </p>
              )}
            </Show>
          </div>
        }
      >
        <div class="grid gap-3 sm:grid-cols-3">
          <Tile
            label="Memory"
            value={memory().used === undefined ? '—' : `${formatMiB(memory().used!)} MiB`}
            valueClass={memory().bursting ? 'text-amber-600 dark:text-amber-400' : undefined}
            title={memory().bursting ? 'Using more than its reservation: the first to give memory back under node pressure' : undefined}
          >
            <Show when={memory().used !== undefined && scale()}>
              <div class="relative py-0.5">
                <Meter percent={percentOf(memory().used!, scale()!)} tone={memory().bursting ? 'warn' : 'ok'} label="Memory used" />
                <Show when={memory().max && memory().reserved && memory().max! > memory().reserved!}>
                  <span
                    aria-hidden="true"
                    class="absolute top-0 h-2.5 w-px bg-ink/50"
                    style={{ left: `${percentOf(memory().reserved!, memory().max!)}%` }}
                    title="Reservation"
                  />
                </Show>
              </div>
            </Show>
            <p class="font-mono" title="Reservation / burst ceiling (resources.memory / resources.memoryMax), MiB">
              res/max {reservationLabel(memory().reserved, memory().max)}
            </p>
            <Show when={memory().peak}>{(peak) => <p class="font-mono">peak {formatMiB(peak())} MiB</p>}</Show>
          </Tile>
          <Tile label="CPU" value={usage() ? `${formatCores(usage()!.cpuMillicores)}` : '—'} title="Average cores over the last report interval">
            <p>{usage() ? 'cores in use' : 'not measured'}</p>
            <Show when={reservedCpu()}>{(reserved) => <p class="font-mono">{reserved()}</p>}</Show>
          </Tile>
          <Tile
            label="Idle"
            value={<IdleCell row={props.row} />}
            title="Time since the allocation last used 1% of a core or moved traffic"
          >
            <p>
              {idleAfter() !== undefined
                ? `Suspends after ${formatIdle(idleAfter()!)} idle (suspend.idle)`
                : 'No idle policy'}
            </p>
          </Tile>
        </div>
        <Show when={!usage()}>
          <p class="mt-3 text-xs text-faint">
            Not measured: its worker sends no usage reports (resourceEnforcement off, always so on macOS), or none since it started.
          </p>
        </Show>
      </Show>
    </SectionCard>
  )
}

const SUPPORT_ITEMS: Array<{ label: string; ok: (s: CheckpointSupport) => boolean; why: (s: CheckpointSupport) => string }> = [
  { label: 'memory snapshot', ok: (s) => s.snapshotMemory, why: (s) => s.snapshotMemoryBlocker },
  { label: 'disk snapshot', ok: (s) => s.snapshotDisk, why: (s) => s.snapshotDiskBlocker },
  { label: 'memory suspend', ok: (s) => s.suspendMemory, why: (s) => s.suspendMemoryBlocker },
  { label: 'memory fork', ok: (s) => s.forkMemory, why: (s) => s.forkMemoryBlocker },
  { label: 'disk fork', ok: (s) => s.forkDisk, why: (s) => s.forkDiskBlocker },
  { label: 'spread fork', ok: (s) => s.spread, why: (s) => s.spreadBlocker },
  { label: 'publish', ok: (s) => s.publish, why: (s) => s.publishBlocker },
]

/** The checkpoint a suspended instance holds, restore history, and what it can checkpoint now. */
function CheckpointCard(props: { instance: InstanceInfo }) {
  const instance = () => props.instance
  return (
    <SectionCard title="Suspend and restore" description="Its checkpoint while suspended, the last suspend and restore, and what it can checkpoint right now.">
      <DefList
        items={[
          [
            'Checkpoint',
            <Show when={instance().checkpointId} fallback={<span class="text-faint">none</span>}>
              <CheckpointCell instance={instance()} />
            </Show>,
          ],
          [
            'Suspend reason',
            <span title={suspendReasonLabel(instance().suspendReason)}>
              {instance().suspendReason ?? <span class="text-faint">never suspended</span>}
            </span>,
          ],
          [
            'Restoring from',
            <Show when={instance().restoreSnapshotId} fallback={<span class="text-faint">—</span>}>
              {(snapshot) => <span class="break-all font-mono text-xs">{snapshot()}</span>}
            </Show>,
          ],
          [
            'Last restored from',
            <Show when={instance().restoredFromSnapshotId} fallback={<span class="text-faint">—</span>}>
              {(snapshot) => <span class="break-all font-mono text-xs">{snapshot()}</span>}
            </Show>,
          ],
          ['Last suspend took', <span class="font-mono text-xs">{formatMs(instance().lastSuspendMs)}</span>],
          ['Last restore took', <span class="font-mono text-xs">{formatMs(instance().lastRestoreMs)}</span>],
        ]}
      />
      <div class="mt-4">
        <p class="mb-1.5 text-xs text-faint">Can checkpoint now</p>
        <Show
          when={instance().checkpointSupport}
          fallback={
            <p class="text-sm text-faint">
              {instance().mode === 'session'
                ? 'Nothing: a session cannot be suspended, forked, or snapshotted. Promote it first.'
                : instance().state === 'active'
                  ? 'This manager does not report checkpoint support.'
                  : 'Only an active instance with a running allocation can be checkpointed.'}
            </p>
          }
        >
          {(support) => (
            <div class="space-y-1.5">
              <ul class="flex flex-wrap gap-1.5">
                <For each={SUPPORT_ITEMS}>
                  {(item) => (
                    <li>
                      <Badge tone={item.ok(support()) ? 'ok' : 'neutral'} title={item.ok(support()) ? undefined : item.why(support()) || 'Not available'}>
                        <Icon name={item.ok(support()) ? 'check' : 'minus'} size={11} />
                        {item.label}
                      </Badge>
                    </li>
                  )}
                </For>
              </ul>
              <p class="text-2xs text-faint">
                <Show when={support().suspendDefault} fallback={<>Suspend: {support().suspendBlocker || 'not possible'}</>}>
                  A plain suspend takes a {support().suspendDefault} checkpoint.
                </Show>{' '}
                Hover a grey item for the manager's reason.
              </p>
            </div>
          )}
        </Show>
      </div>
    </SectionCard>
  )
}

function EventsCard(props: { feed: ReturnType<typeof createEventFeed>; instanceId: string; jobId: string }) {
  const [category, setCategory] = createSignal('all')
  const counts = createMemo(() => {
    const byCategory = new Map<string, number>()
    for (const event of props.feed.events()) {
      const key = eventCategory(event.kind)
      byCategory.set(key, (byCategory.get(key) ?? 0) + 1)
    }
    return byCategory
  })
  const chips = createMemo(() => {
    const present = [...counts().keys()]
    const ordered = [...CATEGORY_ORDER.filter((key) => present.includes(key)), ...present.filter((key) => !CATEGORY_ORDER.includes(key)).sort()]
    return [{ id: 'all', label: 'all', count: props.feed.events().length }, ...ordered.map((key) => ({ id: key, label: `${key}.*`, count: counts().get(key) }))]
  })
  const visible = createMemo(() => {
    const active = category()
    return active === 'all' ? props.feed.events() : props.feed.events().filter((event) => eventCategory(event.kind) === active)
  })
  return (
    <SectionCard
      title="Events"
      description="What happened to this instance and its allocations, newest first. Updates live."
      actions={
        <>
          <Show when={chips().length > 2}>
            <FilterChips label="Filter events by kind" chips={chips()} active={category()} onChange={setCategory} />
          </Show>
          <Show when={props.feed.status() === 'ready'}>
            <span class="inline-flex items-center gap-1.5 text-2xs font-medium text-muted" title="Polling the event journal every 2 seconds">
              <Dot tone={props.feed.error() ? 'warn' : 'ok'} pulse={!props.feed.error()} />
              {props.feed.error() ? 'Retrying' : 'Live'}
            </span>
          </Show>
        </>
      }
      flush
    >
      <Show when={props.feed.status() !== 'loading'} fallback={<LoadingRows rows={4} />}>
        <Show
          when={props.feed.status() !== 'denied'}
          fallback={
            <div class="p-4">
              <Note tone="neutral">
                Your token can't read the event journal (<code class="font-mono text-xs">operator:read</code>). The state, last stop, and
                checkpoint above come from the instance itself and need only <code class="font-mono text-xs">job:read</code>.
              </Note>
            </div>
          }
        >
          <Show
            when={props.feed.status() !== 'error'}
            fallback={
              <div class="p-4">
                <Note tone="danger">Could not load events: {props.feed.error()}</Note>
              </div>
            }
          >
            <Show
              when={props.feed.events().length > 0}
              fallback={
                <EmptyState
                  icon="radio"
                  title="No events about this instance"
                  body="Nothing in the journal names it or its allocations yet, or its events have aged out."
                />
              }
            >
              <Show when={visible().length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No {category()}.* events.</p>}>
                <div class="max-h-[36rem] overflow-y-auto overscroll-contain">
                  <EventTimeline events={visible()} instanceId={props.instanceId} jobId={props.jobId} />
                </div>
              </Show>
            </Show>
          </Show>
        </Show>
      </Show>
      <Show when={props.feed.status() !== 'denied'}>
        <p class="border-t border-line px-4 py-2.5 text-xs text-faint">
          From the manager's event journal, which is bounded: the oldest events age out, so a long-lived instance's early history may be
          gone.
          <Show when={props.feed.truncated()}> Showing the latest 200 at load, plus everything since.</Show> Allocation events match
          through allocations the manager still has a row for. The CLI shows the same with{' '}
          <code class="rounded bg-surface2 px-1 font-mono">legion events list --instance {props.instanceId}</code>.
        </p>
      </Show>
    </SectionCard>
  )
}

/** NotFound, or a first load that failed. Events still show for a gone instance while the journal remembers it. */
function NotLoaded(props: { state: Loading; job: string; feed: ReturnType<typeof createEventFeed>; session?: boolean }) {
  return (
    <div class="space-y-4">
      <Show
        when={props.state.kind === 'missing'}
        fallback={<Note tone="danger">Failed to load instance {props.state.id}: {props.state.kind === 'failed' ? props.state.message : ''}</Note>}
      >
        <div class="rounded-lg border border-line bg-surface shadow-card">
          <EmptyState
            icon="boxes"
            title={
              <Show
                when={props.session}
                fallback={
                  <>
                    No instance <span class="font-mono">{props.state.id}</span>
                  </>
                }
              >
                Session <span class="font-mono">{props.state.id}</span> has ended
              </Show>
            }
            body={
              props.session
                ? 'It exited, was ended, or its node or envelope went away. A session leaves nothing behind: no row, no tombstone, no events of its own.'
                : 'It may have been destroyed along with its job, or the id is mistyped. Keyed ids are never reused while their job exists.'
            }
          >
            <A href={`${jobPath(props.job)}`} class={cx(linkClass, 'mt-2 text-sm')}>
              Back to {props.job}
            </A>
          </EmptyState>
        </div>
        <Show when={props.feed.events().length > 0}>
          <SectionCard title="Events" description="What the journal still remembers about this id, newest first." flush>
            <div class="max-h-[36rem] overflow-y-auto overscroll-contain">
              <EventTimeline events={props.feed.events()} instanceId={props.state.id} jobId={props.job} />
            </div>
          </SectionCard>
        </Show>
      </Show>
    </div>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function InstanceDetail() {
  return (
    <AccessGate resource="job">
      <InstanceDetailContent />
    </AccessGate>
  )
}
