import { A } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, type JSX } from 'solid-js'
import { SnapshotInfo, type InstanceInfo } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { formatBytes } from '../utils/format'
import { diskSnapshotReason, firstLine, formatMs, memorySnapshotReason, publishReason, stableList } from '../utils/instances'
import type { JobTask } from '../utils/jobspec'
import { POLL_INTERVAL_MS, retainOnError, swallowDenied, usePoll } from '../utils/polling'
import {
  aliasBlocker,
  deleteBlocker,
  forksOf,
  mayCreateSnapshot,
  mayReadSnapshots,
  restoreBlocker,
  snapshotLabel,
  snapshotNameError,
  snapshotSizeLabel,
  snapshotStateTone,
  warmBlocker,
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
  checkboxClass,
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
 * one, and restore, name, warm, and delete on each row. Every action is
 * gated by the snapshot ACL through utils/snapshots.ts, and every action
 * that stops, replaces, pauses, or deletes something is a two-step confirm.
 *
 * `SnapshotsTable` is presentational, so /testing renders it from fixtures.
 */

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error))

/** Durability as one word: where the bytes live and whether they moved yet. */
const durabilityLabel = (snapshot: SnapshotInfo): { label: string; title: string } => {
  if (snapshot.state === 'published') {
    return { label: 'published', title: 'In the content store: restores on any compatible node and survives its node' }
  }
  if (snapshot.durability === 'store') {
    return {
      label: snapshot.state === 'capturing' ? 'store' : 'publishing',
      title: snapshot.error || 'Published to the content store once captured; local on its node until the upload finishes',
    }
  }
  return { label: 'local', title: `On ${snapshot.nodeId || 'its node'} only: restores there, and is lost with the node` }
}

export function SnapshotsTable(props: {
  snapshots: readonly SnapshotInfo[]
  actions?: (snapshot: SnapshotInfo) => JSX.Element
  expandedId?: string
  onToggle?: (snapshotId: string) => void
  expanded?: (snapshot: SnapshotInfo) => JSX.Element
  // Names with a remove control, when the caller can change them.
  onRemoveAlias?: (snapshot: SnapshotInfo, name: string) => void
}) {
  const columns = () => (props.actions ? 11 : 10)
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
            <th class={thClass} title="local: on its node only. store: published to the content store once captured.">
              Durability
            </th>
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
              // The row's own name is the first alias; the others are extra
              // names pointing at the same snapshot.
              const extraNames = () => snapshot.aliases.filter((alias) => alias !== snapshot.name)
              const durability = () => durabilityLabel(snapshot)
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
                      <Show when={snapshot.name && props.onRemoveAlias}>
                        <button
                          type="button"
                          class="ml-1 align-middle text-faint hover:text-red-600"
                          aria-label={`Remove name ${snapshot.name}`}
                          title="Remove this name (the snapshot is untouched)"
                          onClick={() => props.onRemoveAlias!(snapshot, snapshot.name)}
                        >
                          <Icon name="x" size={10} />
                        </button>
                      </Show>
                      <Show when={extraNames().length > 0}>
                        <div class="mt-1 flex flex-wrap gap-1">
                          <For each={extraNames()}>
                            {(alias) => (
                              <span class="inline-flex items-center gap-0.5 rounded bg-surface2 px-1 font-mono text-2xs text-muted" title="Another name for this snapshot: `legion instance create --from-snapshot` resolves it">
                                {alias}
                                <Show when={props.onRemoveAlias}>
                                  <button
                                    type="button"
                                    class="text-faint hover:text-red-600"
                                    aria-label={`Remove name ${alias}`}
                                    title="Remove this name (the snapshot is untouched)"
                                    onClick={() => props.onRemoveAlias!(snapshot, alias)}
                                  >
                                    <Icon name="x" size={10} />
                                  </button>
                                </Show>
                              </span>
                            )}
                          </For>
                        </div>
                      </Show>
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
                    <td class={tdClass}>
                      <Badge tone={durability().label === 'published' ? 'ok' : durability().label === 'local' ? 'neutral' : 'info'} title={durability().title}>
                        {durability().label}
                      </Badge>
                    </td>
                    <td class={tdClass + ' text-xs text-muted'}>
                      <Show when={snapshot.nodeId} fallback="-">
                        <A href={`/nodes/${encodeURIComponent(snapshot.nodeId)}`} class="hover:text-brass-strong">
                          {snapshot.nodeId}
                        </A>
                      </Show>
                    </td>
                    <td class={tdClass + ' text-right font-mono text-xs tabular-nums text-muted'}>{snapshotSizeLabel(snapshot, formatBytes)}</td>
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
                        <div class="flex flex-wrap justify-end gap-1.5">{props.actions!(snapshot)}</div>
                      </td>
                    </Show>
                  </tr>
                  <Show when={snapshot.error && !open()}>
                    <tr>
                      <td colSpan={columns()} class={cx('px-4 pb-2 pt-0 text-xs', snapshot.state === 'failed' ? 'text-red-600 dark:text-red-400' : 'text-amber-700 dark:text-amber-300')}>
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

/** Which nodes hold a snapshot's memory parts (GetSnapshot `holders`). */
export function SnapshotHolders(props: { snapshot: SnapshotInfo }) {
  return (
    <Show
      when={props.snapshot.holders.length > 0}
      fallback={
        <p class="text-xs text-faint">
          {props.snapshot.kind === 'disk'
            ? 'A disk snapshot lives in its volume driver on its node, not in the content store.'
            : 'No node advertises its parts to this manager right now.'}
        </p>
      }
    >
      <ul class="flex flex-wrap gap-1.5">
        <For each={props.snapshot.holders}>
          {(holder) => (
            <li>
              <A
                href={`/nodes/${encodeURIComponent(holder.nodeId)}`}
                class="inline-flex items-center gap-1 rounded border border-line bg-surface px-1.5 py-0.5 text-2xs hover:border-brass/50"
                title={`${holder.partsHere} of ${holder.partsTotal} parts, ${formatBytes(Number(holder.bytesHere))}${holder.capturedHere ? ', captured here' : ''}`}
              >
                <span class="font-medium text-ink">{holder.nodeId}</span>
                <span class="font-mono text-faint">
                  {holder.partsHere}/{holder.partsTotal}
                </span>
                <Show when={holder.capturedHere}>
                  <Badge tone="neutral">origin</Badge>
                </Show>
              </A>
            </li>
          )}
        </For>
      </ul>
    </Show>
  )
}

/** One snapshot's full record, including the manifest and holders only GetSnapshot returns. */
function SnapshotDetail(props: { snapshotId: string; instances: readonly InstanceInfo[] }) {
  const [detail] = createResource(
    () => props.snapshotId,
    async (snapshot) => (await managerClient.getSnapshot({ snapshot })).snapshot,
  )
  return (
    <Show when={!detail.error} fallback={<Note tone="danger">Could not load the snapshot: {errorText(detail.error)}</Note>}>
      <Show when={detail()} fallback={<Spinner label="Loading snapshot…" />}>
        {(snapshot) => {
          const forks = () => forksOf(snapshot(), props.instances)
          return (
            <div class="space-y-3">
              <DefList
                cols={3}
                items={[
                  ['Id', <span class="font-mono text-xs">{snapshot().snapshotId}</span>],
                  ['Task', snapshot().task || '-'],
                  ['Allocation', snapshot().allocationId ? `#${String(snapshot().allocationId)}` : '-'],
                  ['Capture took', formatMs(snapshot().durationMs)],
                  ['Paused', snapshot().pauseMs > 0n ? formatMs(snapshot().pauseMs) : snapshot().kind === 'disk' ? 'never (taken live)' : '-'],
                  ['Size', snapshotSizeLabel(snapshot(), formatBytes)],
                  ['Durability', snapshot().durability || 'local'],
                  ['Published', snapshot().publishedAt ? <RelativeTime value={snapshot().publishedAt} /> : '-'],
                  ['Expires', snapshot().expiresAt ? <RelativeTime value={snapshot().expiresAt} /> : 'never'],
                ]}
              />
              <Show when={forks().length > 0}>
                <p class="text-xs text-muted">
                  <span class="mr-1.5 font-semibold text-faint">Fork lineage</span>
                  <span class="font-mono">
                    {snapshot().instanceId} → {forks().map((instance) => instance.instanceId).join(', ')}
                  </span>
                </p>
              </Show>
              <div>
                <p class="eyebrow mb-1.5">Held on</p>
                <SnapshotHolders snapshot={snapshot()} />
              </div>
              <Show when={snapshot().error}>
                <Note tone={snapshot().state === 'failed' ? 'danger' : 'warn'}>{snapshot().error}</Note>
              </Show>
              <Show when={snapshot().manifestJson}>
                <CodeBlock code={formatJson(snapshot().manifestJson)} label="manifest" maxHClass="max-h-72" />
              </Show>
            </div>
          )
        }}
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

/** Set a name on one row: an inline input that opens from the Name button. */
function NameControl(props: { snapshot: SnapshotInfo; blocker?: string; busy: boolean; onName: (name: string) => Promise<boolean> }) {
  const [open, setOpen] = createSignal(false)
  const [name, setName] = createSignal('')
  const error = () => (name().trim() ? snapshotNameError(name().trim()) : 'Enter a name')
  return (
    <Show
      when={open()}
      fallback={
        <span title={props.blocker ?? 'Point a name at this snapshot (moves the name if it exists)'}>
          <Button size="xs" icon="plus" disabled={props.busy || Boolean(props.blocker)} onClick={() => setOpen(true)}>
            Name
          </Button>
        </span>
      }
    >
      <form
        class="inline-flex items-center gap-1"
        onSubmit={(event) => {
          event.preventDefault()
          if (error()) return
          void props.onName(name().trim()).then((ok) => {
            if (ok) {
              setOpen(false)
              setName('')
            }
          })
        }}
      >
        <input
          class={cx(inputClass, 'w-32 py-0.5 text-2xs', name().trim() && error() && 'border-red-500')}
          placeholder="golden"
          value={name()}
          onInput={(event) => setName(event.currentTarget.value)}
          aria-label={`Name for ${props.snapshot.snapshotId}`}
          title={error()}
        />
        <Button size="xs" type="submit" disabled={props.busy || Boolean(error())}>
          Set
        </Button>
        <Button size="xs" variant="ghost" icon="x" aria-label="Cancel naming" onClick={() => setOpen(false)} />
      </form>
    </Show>
  )
}

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
  const [createPublish, setCreatePublish] = createSignal(false)

  // Rows the caller cannot read are filtered out by the manager; a caller
  // with no snapshot:read at all gets an empty list rather than an error.
  //
  // Each poll returns fresh objects, and a keyed <For> would remount every
  // row, disarming a half-confirmed Restore or Delete. So an unchanged row
  // keeps the object it had, and only rows that changed re-render.
  const merge = stableList<SnapshotInfo>((snapshot) => snapshot.snapshotId, (a, b) => SnapshotInfo.equals(a, b))
  const [snapshots, { refetch }] = createResource(
    () => [props.jobName, tick()] as const,
    retainOnError(
      swallowDenied(async ([jobId]: readonly [string, number]) => merge((await managerClient.listSnapshots({ jobId })).snapshots), []),
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
  // The manager's own answer (checkpoint_support) when it sends one, which
  // sees the node's VMM and any stored network policy; the spec otherwise.
  const memoryBlocker = () => memorySnapshotReason(selectedInstance(), selectedTask())
  const diskBlocker = () => diskSnapshotReason(selectedInstance(), selectedTask())
  const publishBlocker = () => (createKind() === 'memory' ? publishReason(selectedInstance()) : 'Only a memory snapshot can be published')
  const kindBlocker = () => (createKind() === 'memory' ? memoryBlocker() : diskBlocker())
  const nameError = () => snapshotNameError(createName().trim())
  const createBlocker = () => {
    if (!mayCreateSnapshot(props.jobName)) return 'Taking a snapshot needs snapshot:write on this job'
    if (!selectedInstance()) return 'No instance has a running allocation to snapshot'
    return kindBlocker() ?? nameError()
  }

  const run = async (action: () => Promise<string>, afterwards?: () => void): Promise<boolean> => {
    setBusy(true)
    try {
      setOutcome({ message: await action(), failed: false })
      void refetch()
      afterwards?.()
      return true
    } catch (error) {
      setOutcome({ message: errorText(error), failed: true })
      return false
    } finally {
      setBusy(false)
    }
  }

  const create = () =>
    run(async () => {
      const instance = selectedInstance()!
      const name = createName().trim()
      const publish = createPublish() && !publishBlocker()
      const reply = await managerClient.createSnapshot({
        instanceId: instance.instanceId,
        kind: createKind(),
        name: name || undefined,
        publish,
      })
      setCreateName('')
      return `Capturing ${reply.snapshot?.snapshotId ?? 'snapshot'} of ${instance.instanceId}${publish ? ', then publishing it' : ''}. Its node takes it on the next heartbeat.`
    })

  const restore = (snapshot: SnapshotInfo) =>
    run(async () => {
      const reply = await managerClient.restoreInstance({ instanceId: snapshot.instanceId, snapshot: snapshot.snapshotId })
      return firstLine(reply.message) || `Restoring ${snapshot.instanceId} from ${snapshot.snapshotId}`
    }, props.onChanged)

  const remove = (snapshot: SnapshotInfo) =>
    run(async () => firstLine((await managerClient.deleteSnapshot({ snapshotId: snapshot.snapshotId })).message) || `Deleting ${snapshot.snapshotId}`)

  const warm = (snapshot: SnapshotInfo) =>
    run(async () => {
      const reply = await managerClient.warmSnapshot({ snapshot: snapshot.snapshotId, jobId: snapshot.jobId })
      return (
        firstLine(reply.message) ||
        `Warming ${snapshot.snapshotId} on ${reply.nodes.length} node(s)${reply.alreadyWarm.length ? `, ${reply.alreadyWarm.length} already warm` : ''}`
      )
    })

  const setAlias = (snapshot: SnapshotInfo, name: string) =>
    run(async () => firstLine((await managerClient.setSnapshotAlias({ jobId: snapshot.jobId, name, snapshot: snapshot.snapshotId })).message) || `${name} now points at ${snapshot.snapshotId}`)

  const removeAlias = (snapshot: SnapshotInfo, name: string) =>
    void run(async () => firstLine((await managerClient.deleteSnapshotAlias({ jobId: snapshot.jobId, name })).message) || `Removed the name ${name}`)

  const instanceIds = createMemo(() => [...new Set((snapshots() ?? []).map((snapshot) => snapshot.instanceId).filter(Boolean))].sort())

  return (
    <Show when={mayReadSnapshots(props.jobName)}>
      <SectionCard
        title="Snapshots"
        description="Point-in-time captures of instances. A disk snapshot copies the instance volume, a memory snapshot also saves a sandbox's running guest. Restore rolls an instance back in place. A name lets `legion instance create --from-snapshot` start new instances from it."
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
                  disk{diskBlocker() ? ' (unavailable)' : ''}
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
            <label class="flex items-center gap-1.5 pb-1.5 text-xs text-muted" title={publishBlocker() ?? 'Upload to the content store once captured, so it restores on any compatible node'}>
              <input
                type="checkbox"
                class={checkboxClass}
                checked={createPublish() && !publishBlocker()}
                disabled={Boolean(publishBlocker())}
                onChange={(event) => setCreatePublish(event.currentTarget.checked)}
              />
              publish
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
              <span class="max-w-md whitespace-pre-line pb-1.5 text-2xs text-faint">{createBlocker()}</span>
            </Show>
          </form>
        </Show>

        <Show when={outcome()}>
          {(result) => (
            <div aria-live="polite" class="px-4 pt-3">
              <Note tone={result().failed ? 'danger' : 'ok'} onDismiss={() => setOutcome(null)}>
                <span class="whitespace-pre-line">{result().message}</span>
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
            expanded={(snapshot) => <SnapshotDetail snapshotId={snapshot.snapshotId} instances={props.instances} />}
            onRemoveAlias={mayCreateSnapshot(props.jobName) ? removeAlias : undefined}
            actions={(snapshot) => {
              const restoreReason = () => restoreBlocker(snapshot, instancesById().get(snapshot.instanceId), props.jobName)
              const deleteReason = () => deleteBlocker(snapshot, props.jobName)
              return (
                <>
                  <Show when={snapshot.purpose === 'manual' && (snapshot.state === 'local' || snapshot.state === 'published')}>
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
                    <NameControl snapshot={snapshot} blocker={aliasBlocker(snapshot, props.jobName)} busy={busy()} onName={(name) => setAlias(snapshot, name)} />
                  </Show>
                  <Show when={snapshot.state === 'published'}>
                    <span title={warmBlocker(snapshot, props.jobName) ?? 'Pre-fetch its parts onto every compatible node'}>
                      <Button size="xs" icon="zap" disabled={busy() || Boolean(warmBlocker(snapshot, props.jobName))} onClick={() => void warm(snapshot)}>
                        Warm
                      </Button>
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
