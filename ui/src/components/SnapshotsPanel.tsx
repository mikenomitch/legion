import { For, Show, createMemo, createResource, createSignal, type JSX } from 'solid-js'
import { SnapshotInfo, type InstanceInfo } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { formatBytes } from '../utils/format'
import { memorySnapshotBlocker, type JobTask } from '../utils/jobspec'
import { POLL_INTERVAL_MS, retainOnError, swallowDenied, usePoll } from '../utils/polling'
import {
  deleteBlocker,
  mayCreateSnapshot,
  mayReadSnapshots,
  restoreBlocker,
  snapshotLabel,
  snapshotNameError,
  snapshotStateTone,
} from '../utils/snapshots'
import {
  Badge,
  Button,
  CodeBlock,
  ConfirmButton,
  DefList,
  EmptyState,
  Icon,
  MonoBadge,
  Note,
  RelativeTime,
  SectionCard,
  Spinner,
  TableShell,
  cx,
  inputClass,
  selectClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
  trHoverClass,
} from './ui'

/*
 * A job's snapshots (docs/suspend-and-resume.md): the list, a form to take
 * one, and restore and delete on each row. Every action is gated by the
 * snapshot ACL through utils/snapshots.ts, and every action that stops,
 * replaces, or pauses something is a two-step confirm.
 *
 * `SnapshotsTable` is presentational, so /testing renders it from fixtures
 * and later phases add columns (durability, alias) and actions (warm) in one
 * place.
 */

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error))

const formatDuration = (ms: bigint): string => {
  const value = Number(ms)
  if (!value) return '-'
  return value >= 1000 ? `${(value / 1000).toFixed(1)}s` : `${value} ms`
}

export function SnapshotsTable(props: {
  snapshots: readonly SnapshotInfo[]
  actions?: (snapshot: SnapshotInfo) => JSX.Element
  expandedId?: string
  onToggle?: (snapshotId: string) => void
  expanded?: (snapshot: SnapshotInfo) => JSX.Element
}) {
  const columns = () => (props.actions ? 10 : 9)
  return (
    <TableShell>
      <table class={tableClass}>
        <thead class={theadClass}>
          <tr>
            <th class={thClass}>Snapshot</th>
            <th class={thClass}>Instance</th>
            <th class={thClass}>Kind</th>
            <th class={thClass}>Purpose</th>
            <th class={thClass}>State</th>
            <th class={thClass}>Node</th>
            <th class={thClass + ' text-right'}>Size</th>
            <th class={thClass + ' text-right'}>Created</th>
            <th class={thClass + ' text-right'}>Expires</th>
            <Show when={props.actions}>
              <th class={thClass + ' text-right'}>
                <span class="sr-only">Actions</span>
              </th>
            </Show>
          </tr>
        </thead>
        <tbody class="divide-y divide-line">
          <For each={props.snapshots}>
            {(snapshot) => {
              const open = () => props.expandedId === snapshot.snapshotId
              return (
                <>
                  <tr class={trHoverClass}>
                    <td class={tdClass}>
                      <button
                        type="button"
                        class="inline-flex items-center gap-1.5 text-left hover:text-brass-strong disabled:cursor-default"
                        onClick={() => props.onToggle?.(snapshot.snapshotId)}
                        disabled={!props.onToggle}
                        aria-expanded={props.onToggle ? open() : undefined}
                        title={snapshot.snapshotId}
                      >
                        <Show when={props.onToggle}>
                          <Icon name="chevronRight" size={12} class={cx('text-faint transition-transform', open() && 'rotate-90')} />
                        </Show>
                        <span class={cx('text-xs', snapshot.name ? 'font-medium' : 'font-mono')}>{snapshotLabel(snapshot)}</span>
                      </button>
                    </td>
                    <td class={tdClass}>
                      <Show when={snapshot.instanceId} fallback={<span class="text-faint">-</span>}>
                        <MonoBadge>{snapshot.instanceId}</MonoBadge>
                      </Show>
                    </td>
                    <td class={tdClass}>
                      <Badge tone={snapshot.kind === 'memory' ? 'accent' : 'neutral'}>{snapshot.kind}</Badge>
                    </td>
                    <td class={tdClass + ' text-xs text-muted'}>{snapshot.purpose || '-'}</td>
                    <td class={tdClass}>
                      <Badge tone={snapshotStateTone(snapshot.state)} dot class="uppercase tracking-wide" title={snapshot.error || undefined}>
                        {snapshot.state}
                      </Badge>
                    </td>
                    <td class={tdClass + ' text-xs text-muted'}>{snapshot.nodeId || '-'}</td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>
                      {snapshot.totalBytes > 0n ? formatBytes(Number(snapshot.totalBytes)) : '-'}
                    </td>
                    <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>
                      <RelativeTime value={snapshot.createdAt} />
                    </td>
                    <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>
                      <Show when={snapshot.expiresAt} fallback="never">
                        <RelativeTime value={snapshot.expiresAt} />
                      </Show>
                    </td>
                    <Show when={props.actions}>
                      <td class={tdClass + ' text-right'}>
                        <div class="flex justify-end gap-1.5">{props.actions!(snapshot)}</div>
                      </td>
                    </Show>
                  </tr>
                  <Show when={snapshot.error && !open()}>
                    <tr>
                      <td colSpan={columns()} class="px-4 pb-2 pt-0 text-xs text-red-600 dark:text-red-400">
                        {snapshot.error}
                      </td>
                    </tr>
                  </Show>
                  <Show when={open() && props.expanded}>
                    <tr>
                      <td colSpan={columns()} class="border-t border-line bg-surface2/40 px-4 py-3">
                        {props.expanded!(snapshot)}
                      </td>
                    </tr>
                  </Show>
                </>
              )
            }}
          </For>
        </tbody>
      </table>
    </TableShell>
  )
}

/** One snapshot's full record, including the manifest only GetSnapshot returns. */
function SnapshotDetail(props: { snapshotId: string }) {
  const [detail] = createResource(
    () => props.snapshotId,
    async (snapshot) => (await managerClient.getSnapshot({ snapshot })).snapshot,
  )
  return (
    <Show when={!detail.error} fallback={<Note tone="danger">Could not load the snapshot: {errorText(detail.error)}</Note>}>
      <Show when={detail()} fallback={<Spinner label="Loading snapshot…" />}>
        {(snapshot) => (
          <div class="space-y-3">
            <DefList
              cols={3}
              items={[
                ['Id', <span class="font-mono text-xs">{snapshot().snapshotId}</span>],
                ['Task', snapshot().task || '-'],
                ['Allocation', snapshot().allocationId ? `#${String(snapshot().allocationId)}` : '-'],
                ['Capture took', formatDuration(snapshot().durationMs)],
                ['Size', snapshot().totalBytes > 0n ? formatBytes(Number(snapshot().totalBytes)) : '-'],
                ['Expires', snapshot().expiresAt ? <RelativeTime value={snapshot().expiresAt} /> : 'never'],
              ]}
            />
            <Show when={snapshot().error}>
              <Note tone="danger">{snapshot().error}</Note>
            </Show>
            <Show when={snapshot().manifestJson}>
              <CodeBlock code={formatJson(snapshot().manifestJson)} label="manifest" maxHClass="max-h-72" />
            </Show>
          </div>
        )}
      </Show>
    </Show>
  )
}

const formatJson = (raw: string): string => {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

type Outcome = { message: string; failed: boolean }

export default function SnapshotsPanel(props: {
  jobName: string
  instances: readonly InstanceInfo[]
  tasks: readonly JobTask[]
  // Called after a restore so the owner can refetch instances right away.
  onChanged?: () => void
}) {
  const tick = usePoll(POLL_INTERVAL_MS)
  const [outcome, setOutcome] = createSignal<Outcome | null>(null)
  const [busy, setBusy] = createSignal(false)
  const [expandedId, setExpandedId] = createSignal<string | undefined>()
  const [instanceFilter, setInstanceFilter] = createSignal('')
  const [createInstance, setCreateInstance] = createSignal('')
  const [createKind, setCreateKind] = createSignal<'disk' | 'memory'>('disk')
  const [createName, setCreateName] = createSignal('')

  // Rows the caller cannot read are filtered out by the manager; a caller
  // with no snapshot:read at all gets an empty list rather than an error.
  //
  // Each poll returns fresh objects, and a keyed <For> would remount every
  // row, disarming a half-confirmed Restore or Delete. So an unchanged row
  // keeps the object it had, and only rows that changed re-render.
  let previous = new Map<string, SnapshotInfo>()
  const [snapshots, { refetch }] = createResource(
    () => [props.jobName, tick()] as const,
    retainOnError(
      swallowDenied(async ([jobId]: readonly [string, number]) => {
        const next = (await managerClient.listSnapshots({ jobId })).snapshots.map((snapshot) => {
          const before = previous.get(snapshot.snapshotId)
          return before && SnapshotInfo.equals(before, snapshot) ? before : snapshot
        })
        previous = new Map(next.map((snapshot) => [snapshot.snapshotId, snapshot]))
        return next
      }, []),
      ([jobId]) => jobId,
    ),
  )

  const instancesById = createMemo(() => new Map(props.instances.map((instance) => [instance.instanceId, instance])))
  const tasksByName = createMemo(() => new Map(props.tasks.map((task) => [task.name, task])))

  const visible = createMemo(() => {
    const filter = instanceFilter()
    return (snapshots() ?? []).filter((snapshot) => !filter || snapshot.instanceId === filter)
  })

  // A snapshot is taken of a running allocation, so only instances with one
  // are offered.
  const snapshottable = createMemo(() =>
    props.instances.filter((instance) => instance.state === 'active' && instance.currentAllocId !== undefined),
  )
  const selectedInstance = () => instancesById().get(createInstance()) ?? snapshottable()[0]
  const selectedTask = () => tasksByName().get(selectedInstance()?.task ?? '')
  const memoryBlocker = () => memorySnapshotBlocker(selectedTask())
  const diskBlocker = () =>
    selectedTask() && !selectedTask()!.stateful?.storagePath ? 'Disk snapshots need a stateful.storage volume' : undefined
  const kindBlocker = () => (createKind() === 'memory' ? memoryBlocker() : diskBlocker())
  const nameError = () => snapshotNameError(createName().trim())
  const createBlocker = () => {
    if (!mayCreateSnapshot(props.jobName)) return 'Taking a snapshot needs snapshot:write on this job'
    if (!selectedInstance()) return 'No instance has a running allocation to snapshot'
    return kindBlocker() ?? nameError()
  }

  const run = async (action: () => Promise<string>, afterwards?: () => void) => {
    setBusy(true)
    try {
      setOutcome({ message: await action(), failed: false })
      void refetch()
      afterwards?.()
    } catch (error) {
      setOutcome({ message: errorText(error), failed: true })
    } finally {
      setBusy(false)
    }
  }

  const create = () =>
    run(async () => {
      const instance = selectedInstance()!
      const name = createName().trim()
      const reply = await managerClient.createSnapshot({
        instanceId: instance.instanceId,
        kind: createKind(),
        name: name || undefined,
      })
      setCreateName('')
      return `Capturing ${reply.snapshot?.snapshotId ?? 'snapshot'} of ${instance.instanceId}. Its node takes it on the next heartbeat.`
    })

  const restore = (snapshot: SnapshotInfo) =>
    run(async () => {
      const reply = await managerClient.restoreInstance({ instanceId: snapshot.instanceId, snapshot: snapshot.snapshotId })
      return reply.message || `Restoring ${snapshot.instanceId} from ${snapshot.snapshotId}`
    }, props.onChanged)

  const remove = (snapshot: SnapshotInfo) =>
    run(async () => (await managerClient.deleteSnapshot({ snapshotId: snapshot.snapshotId })).message || `Deleting ${snapshot.snapshotId}`)

  const instanceIds = createMemo(() => [...new Set((snapshots() ?? []).map((snapshot) => snapshot.instanceId).filter(Boolean))].sort())

  return (
    <Show when={mayReadSnapshots(props.jobName)}>
      <SectionCard
        title="Snapshots"
        description="Point-in-time captures of instances. A disk snapshot copies the instance volume, a memory snapshot also saves a sandbox's running guest. Restore rolls an instance back in place."
        actions={
          <Show when={instanceIds().length > 1}>
            <select
              class={cx(selectClass, 'py-1 text-xs')}
              value={instanceFilter()}
              onChange={(event) => setInstanceFilter(event.currentTarget.value)}
              aria-label="Filter snapshots by instance"
            >
              <option value="">All instances</option>
              <For each={instanceIds()}>{(id) => <option value={id}>{id}</option>}</For>
            </select>
          </Show>
        }
        flush
      >
        <Show when={snapshottable().length > 0}>
          <form
            class="flex flex-wrap items-end gap-2 border-b border-line px-4 py-3"
            onSubmit={(event) => event.preventDefault()}
            title={createBlocker()}
          >
            <label class="block">
              <span class="mb-1 block text-2xs font-medium text-muted">Instance</span>
              <select
                class={cx(selectClass, 'py-1 text-xs')}
                value={selectedInstance()?.instanceId ?? ''}
                onChange={(event) => setCreateInstance(event.currentTarget.value)}
              >
                <For each={snapshottable()}>{(instance) => <option value={instance.instanceId}>{instance.instanceId}</option>}</For>
              </select>
            </label>
            <label class="block">
              <span class="mb-1 block text-2xs font-medium text-muted">Kind</span>
              <select
                class={cx(selectClass, 'py-1 text-xs')}
                value={createKind()}
                onChange={(event) => setCreateKind(event.currentTarget.value as 'disk' | 'memory')}
              >
                <option value="disk" disabled={Boolean(diskBlocker())} title={diskBlocker()}>
                  disk
                </option>
                <option value="memory" disabled={Boolean(memoryBlocker())} title={memoryBlocker()}>
                  memory{memoryBlocker() ? ' (unavailable)' : ''}
                </option>
              </select>
            </label>
            <label class="block">
              <span class="mb-1 block text-2xs font-medium text-muted">Name (optional)</span>
              <input
                class={cx(inputClass, 'w-44 py-1 text-xs', nameError() && 'border-red-500')}
                placeholder="before-refactor"
                value={createName()}
                onInput={(event) => setCreateName(event.currentTarget.value)}
                aria-invalid={Boolean(nameError())}
              />
            </label>
            {/* A memory snapshot pauses the guest while its memory is
                written, so it gets the two-step confirm. A disk snapshot is
                taken live and is one click. */}
            <Show
              when={createKind() === 'memory'}
              fallback={
                <Button icon="camera" disabled={busy() || Boolean(createBlocker())} title={createBlocker()} onClick={() => void create()}>
                  Take snapshot
                </Button>
              }
            >
              <ConfirmButton
                variant="subtle"
                icon="camera"
                label="Take snapshot"
                confirmLabel="Pause the guest and capture?"
                disabled={busy() || Boolean(createBlocker())}
                title={createBlocker()}
                onConfirm={() => void create()}
              />
            </Show>
            <Show when={createBlocker() && mayCreateSnapshot(props.jobName)}>
              <span class="pb-1.5 text-2xs text-faint">{createBlocker()}</span>
            </Show>
          </form>
        </Show>

        <Show when={outcome()}>
          {(result) => (
            <div aria-live="polite" class="px-4 pt-3">
              <Note tone={result().failed ? 'danger' : 'ok'} onDismiss={() => setOutcome(null)}>
                {result().message}
              </Note>
            </div>
          )}
        </Show>

        <Show
          when={visible().length > 0}
          fallback={
            <EmptyState
              icon="camera"
              title={snapshots() === undefined ? 'Loading snapshots…' : 'No snapshots'}
              body={
                snapshots() === undefined ? undefined : (
                  <>
                    Take one above, or with <code class="font-mono">legion snapshot create &lt;instance&gt;</code>.
                  </>
                )
              }
            />
          }
        >
          <SnapshotsTable
            snapshots={visible()}
            expandedId={expandedId()}
            onToggle={(id) => setExpandedId((current) => (current === id ? undefined : id))}
            expanded={(snapshot) => <SnapshotDetail snapshotId={snapshot.snapshotId} />}
            actions={(snapshot) => {
              const restoreReason = () => restoreBlocker(snapshot, instancesById().get(snapshot.instanceId), props.jobName)
              const deleteReason = () => deleteBlocker(snapshot, props.jobName)
              return (
                <>
                  <Show when={snapshot.state === 'local'}>
                    <span title={restoreReason()}>
                      <ConfirmButton
                        size="xs"
                        variant="subtle"
                        icon="history"
                        label="Restore"
                        confirmLabel="Replace the running allocation?"
                        disabled={busy() || Boolean(restoreReason())}
                        onConfirm={() => void restore(snapshot)}
                      />
                    </span>
                  </Show>
                  <Show when={snapshot.state !== 'capturing' && snapshot.state !== 'deleting'}>
                    <span title={deleteReason()}>
                      <ConfirmButton
                        size="xs"
                        label="Delete"
                        disabled={busy() || Boolean(deleteReason())}
                        onConfirm={() => void remove(snapshot)}
                      />
                    </span>
                  </Show>
                </>
              )
            }}
          />
        </Show>
      </SectionCard>
    </Show>
  )
}
