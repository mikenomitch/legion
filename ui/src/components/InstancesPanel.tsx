import { useSearchParams } from '@solidjs/router'
import { For, Show, createMemo, createSignal } from 'solid-js'
import type { InstanceInfo } from '../gen/legion_pb'
import { debounced } from '../utils/debounce'
import { forksByOrigin } from '../utils/instances'
import {
  INSTANCE_PAGE,
  INSTANCE_RENDER_MAX,
  SESSION_COPY,
  countInstances,
  formatCount,
  listCommand,
  matchesFilter,
  takeMatching,
  topCounts,
  type InstanceFilter,
} from '../utils/sessions'
import { statusTone } from '../utils/status'
import { InstanceActionButtons, createInstanceActions } from './InstanceActions'
import InstancesTable, { INSTANCE_COLUMNS, type InstanceColumn, type InstanceRow } from './InstancesTable'
import { Badge, Button, CopyButton, FilterChips, Note, RelativeTime, SectionCard, cx, inputClass } from './ui'

/*
 * The job page's Instances card (docs/suspend-and-resume.md,
 * docs/session-mode.md): the table, state and mode filters, and per-row
 * Suspend, Resume, Resume cold, and Fork for keyed instances, or Promote and
 * End for sessions (the shared controls in InstanceActions.tsx). Every
 * action is gated through utils/instances.ts and utils/sessions.ts, and every
 * action that stops or replaces something is a two-step confirm. Each id
 * links to the instance's own page, which adds Stop, Start, and the
 * instance's events. Instance create and destroy stay in the CLI.
 *
 * Built for a job with a million sessions: counts come from one pass over
 * the listing, filters scan it without copying it, and the table renders at
 * most INSTANCE_PAGE rows at a time (INSTANCE_RENDER_MAX in all). Rows are
 * built only for what is shown. The mode, node, envelope, and task filters
 * live in the URL, so the node and allocation pages can link straight to
 * "this node's sessions".
 */

// The states worth a filter chip, in the order an operator acts on them.
const STATE_ORDER = ['active', 'suspended', 'pending', 'rescheduling', 'stopped', 'terminated']
const MODE_ORDER = ['keyed', 'slot', 'session']
// Busiest nodes listed in the session summary.
const TOP_NODES = 8
// Fork lineage entries listed before "and N more".
const LINEAGE_MAX = 20

/** How the listing was read, for the summary line. */
export type InstanceListing = {
  /** ListInstancesReply.complete: every node that can run sessions has reported. */
  complete: boolean
  /** ListInstancesReply.as_of: when the session part was read. */
  asOf?: string
  /** Large listings are not re-polled; the owner refreshes on request. */
  paused: boolean
  refreshing: boolean
  onRefresh: () => void
}

const param = (value: string | string[] | undefined): string | undefined => {
  const first = Array.isArray(value) ? value[0] : value
  return first && first.trim() ? first.trim() : undefined
}

export default function InstancesPanel(props: {
  jobName: string
  instances: readonly InstanceInfo[]
  // Builds the table row for one shown instance (task spec, node usage).
  toRow: (instance: InstanceInfo) => InstanceRow
  // Called after an action so the owner refetches instances right away.
  onChanged?: () => void
  // Whether rows get the actions column (keyed or session tasks).
  actions: boolean
  listing?: InstanceListing
}) {
  const [search, setSearch] = useSearchParams()
  const [stateFilter, setStateFilter] = createSignal('all')
  const [text, setText] = createSignal('')
  const textFilter = debounced(() => text().trim(), 200)
  const [limit, setLimit] = createSignal(INSTANCE_PAGE)
  const [highlight, setHighlight] = createSignal<string>()

  const filter = createMemo<InstanceFilter>(() => ({
    state: stateFilter() === 'all' ? undefined : stateFilter(),
    mode: param(search.mode),
    node: param(search.node),
    envelope: param(search.envelope),
    task: param(search.task),
    text: textFilter() || undefined,
  }))
  const setParam = (key: 'mode' | 'node' | 'envelope' | 'task', value: string | undefined) => {
    setLimit(INSTANCE_PAGE)
    setSearch({ [key]: value }, { replace: true })
  }

  const counts = createMemo(() => countInstances(props.instances))
  const chips = createMemo(() => {
    const byState = counts().byState
    const present = [...byState.keys()]
    const ordered = [...STATE_ORDER.filter((state) => present.includes(state)), ...present.filter((state) => !STATE_ORDER.includes(state)).sort()]
    return [
      { id: 'all', label: 'all', count: counts().total },
      ...ordered.map((state) => ({ id: state, label: state, count: byState.get(state), tone: statusTone(state) })),
    ]
  })
  const modeChips = createMemo(() => {
    const byMode = counts().byMode
    const present = [...byMode.keys()]
    const ordered = [...MODE_ORDER.filter((mode) => present.includes(mode)), ...present.filter((mode) => !MODE_ORDER.includes(mode)).sort()]
    return [{ id: 'all', label: 'every mode', count: counts().total }, ...ordered.map((mode) => ({ id: mode, label: mode, count: byMode.get(mode) }))]
  })

  // One scan: the first `limit` matches and how many match in all.
  const page = createMemo(() => {
    const active = filter()
    return takeMatching(props.instances, (instance) => matchesFilter(instance, active), limit())
  })
  const rows = createMemo<InstanceRow[]>(() => page().shown.map((instance) => props.toRow(instance)))
  const filtered = () => Object.values(filter()).some(Boolean)

  const lineage = createMemo(() => forksByOrigin(props.instances))
  const lineageEntries = createMemo(() => [...lineage().entries()].slice(0, LINEAGE_MAX))

  // One runner for every row's controls, so the card shows one result for
  // whichever action ran. A fork or promotion highlights the new row.
  const actions = createInstanceActions({
    onChanged: () => props.onChanged?.(),
    onForked: (forks) => setHighlight(forks[0]),
    onPromoted: (id) => setHighlight(id),
  })

  const actionsColumn: InstanceColumn = {
    key: 'actions',
    header: <span class="sr-only">Actions</span>,
    class: 'text-right',
    cell: (row) => <InstanceActionButtons row={row} jobName={props.jobName} actions={actions} />,
  }

  const columns = createMemo(() => (props.actions ? [...INSTANCE_COLUMNS, actionsColumn] : INSTANCE_COLUMNS))

  return (
    <SectionCard
      title="Instances"
      description="Stable identities for stateful tasks, and the job's running sessions. Instance ids and storage survive reschedules, suspends, and restores; sessions live on their node until they end or are promoted."
      actions={<Show when={counts().total > 1}>{<FilterChips label="Filter instances by state" chips={chips()} active={stateFilter()} onChange={(id) => { setLimit(INSTANCE_PAGE); setStateFilter(id) }} />}</Show>}
      flush
    >
      <Show when={actions.outcome()}>
        {(result) => (
          <div aria-live="polite" class="px-4 pt-3">
            <Note tone={result().failed ? 'danger' : 'ok'} onDismiss={actions.dismiss}>
              <span class="whitespace-pre-line">{result().message}</span>
              <For each={result().forks ?? []}>
                {(id) => (
                  <a href={`#instance-${encodeURIComponent(id)}`} class="ml-1.5 font-mono underline-offset-2 hover:underline" onClick={() => setHighlight(id)}>
                    {id}
                  </a>
                )}
              </For>
            </Note>
          </div>
        )}
      </Show>

      <Show when={counts().sessions > 0 || props.listing?.paused || props.listing?.complete === false}>
        <SessionSummary
          sessions={counts().sessions}
          sessionsByNode={counts().sessionsByNode}
          listing={props.listing}
          total={counts().total}
          activeNode={filter().node}
          onNode={(node) => setParam('node', node === filter().node ? undefined : node)}
        />
      </Show>

      <Show when={lineage().size > 0}>
        <div class="flex flex-wrap gap-x-4 gap-y-1 border-b border-line px-4 py-2 text-2xs text-muted">
          <span class="font-semibold uppercase tracking-wide text-faint">Fork lineage</span>
          <For each={lineageEntries()}>
            {([origin, forks]) => (
              <span class="font-mono">
                <a href={`#instance-${encodeURIComponent(origin)}`} class="hover:text-brass-strong" onClick={() => setHighlight(origin)}>
                  {origin}
                </a>{' '}
                → {forks.length > 5 ? `${forks.slice(0, 5).join(', ')} and ${forks.length - 5} more` : forks.join(', ')}
              </span>
            )}
          </For>
          <Show when={lineage().size > LINEAGE_MAX}>
            <span>and {formatCount(lineage().size - LINEAGE_MAX)} more origins</span>
          </Show>
        </div>
      </Show>

      <Show when={counts().total > 10 || modeChips().length > 2 || filtered()}>
        <div class="flex flex-wrap items-center gap-2 border-b border-line px-4 py-2">
          <Show when={modeChips().length > 2}>
            <FilterChips
              label="Filter instances by mode"
              chips={modeChips()}
              active={filter().mode ?? 'all'}
              onChange={(id) => setParam('mode', id === 'all' ? undefined : id)}
            />
          </Show>
          <input
            type="search"
            class={cx(inputClass, 'w-56 py-1 text-xs')}
            placeholder="Filter by id, node, or task"
            aria-label="Filter instances by id, node, or task"
            value={text()}
            onInput={(event) => {
              setLimit(INSTANCE_PAGE)
              setText(event.currentTarget.value)
            }}
          />
          <For each={(['node', 'envelope', 'task'] as const).filter((key) => filter()[key])}>
            {(key) => (
              <Badge tone="accent" class="gap-1">
                {key === 'envelope' ? `envelope #${filter()[key]}` : `${key} ${filter()[key]}`}
                <button type="button" class="ml-0.5 opacity-70 hover:opacity-100" aria-label={`Clear the ${key} filter`} onClick={() => setParam(key, undefined)}>
                  ×
                </button>
              </Badge>
            )}
          </For>
        </div>
      </Show>

      <Show
        when={rows().length > 0}
        fallback={
          <p class="px-4 py-4 text-sm text-muted">
            {counts().total === 0 ? 'No instances or running sessions yet.' : 'Nothing matches the filter.'}
          </p>
        }
      >
        <InstancesTable rows={rows()} columns={columns()} highlight={highlight()} />
      </Show>

      <Show when={page().matching > rows().length}>
        <div class="flex flex-wrap items-center gap-x-3 gap-y-1.5 border-t border-line px-4 py-2.5 text-xs text-muted">
          <span>
            Showing the first {formatCount(rows().length)} of {formatCount(page().matching)}
            {filtered() ? ' matching' : ''}.
          </span>
          <Show
            when={limit() < INSTANCE_RENDER_MAX}
            fallback={<span class="text-faint">Narrow the filter to see others, or list them all from the CLI:</span>}
          >
            <Button size="xs" onClick={() => setLimit((value) => Math.min(value + INSTANCE_PAGE, INSTANCE_RENDER_MAX))}>
              Show {formatCount(Math.min(INSTANCE_PAGE, page().matching - rows().length))} more
            </Button>
            <span class="text-faint">or list them all from the CLI:</span>
          </Show>
          <code class="rounded bg-surface2 px-1 font-mono text-2xs">{listCommand(props.jobName)}</code>
          <CopyButton text={listCommand(props.jobName)} label="" size="xs" />
        </div>
      </Show>

      <Show when={props.actions}>
        <p class="border-t border-line px-4 py-2.5 text-xs text-faint">
          Create and destroy keyed instances from the CLI with{' '}
          <code class="rounded bg-surface2 px-1 font-mono">legion instance create</code> (add{' '}
          <code class="rounded bg-surface2 px-1 font-mono">--from-snapshot &lt;name&gt;</code> to start from a snapshot) and{' '}
          <code class="rounded bg-surface2 px-1 font-mono">legion instance destroy</code>. Without{' '}
          <code class="rounded bg-surface2 px-1 font-mono">--id</code>, a create on a session task starts a session.
        </p>
      </Show>
    </SectionCard>
  )
}

/**
 * Sessions at a glance: how many, on how many nodes, the busiest nodes, and
 * whether the index the listing came from has heard from every node.
 */
function SessionSummary(props: {
  sessions: number
  total: number
  sessionsByNode: ReadonlyMap<string, number>
  listing?: InstanceListing
  activeNode?: string
  onNode: (node: string) => void
}) {
  const busiest = createMemo(() => topCounts(props.sessionsByNode, TOP_NODES))
  return (
    <div class="space-y-2 border-b border-line px-4 py-2.5 text-xs text-muted">
      <div class="flex flex-wrap items-center gap-x-3 gap-y-1">
        <Show when={props.sessions > 0}>
          <span title={SESSION_COPY}>
            <span class="font-semibold text-ink">{formatCount(props.sessions)}</span> running {props.sessions === 1 ? 'session' : 'sessions'} on{' '}
            {formatCount(props.sessionsByNode.size)} {props.sessionsByNode.size === 1 ? 'node' : 'nodes'}
          </span>
        </Show>
        <Show when={props.listing}>
          {(listing) => (
            <>
              <Show
                when={listing().complete}
                fallback={
                  <Badge tone="warn" title="Some nodes that can run sessions have not reported to any manager yet (a manager just started, or a node is unreachable). Some sessions may be missing.">
                    partial: some nodes not reported
                  </Badge>
                }
              >
                <Show when={props.sessions > 0}>
                  <Badge tone="ok" title="Every live node that can run sessions has reported its sessions to some manager">
                    every node reported
                  </Badge>
                </Show>
              </Show>
              <Show when={listing().asOf}>
                <span class="text-faint">
                  read <RelativeTime value={listing().asOf} />
                </span>
              </Show>
              <Show when={listing().paused}>
                <span class="text-faint">
                  · {formatCount(props.total)} instances is too many to re-read every few seconds, so this list does not refresh on its own.
                </span>
                <Button size="xs" icon="refresh" disabled={listing().refreshing} onClick={listing().onRefresh}>
                  {listing().refreshing ? 'Refreshing…' : 'Refresh'}
                </Button>
              </Show>
            </>
          )}
        </Show>
      </div>
      <Show when={busiest().length > 1}>
        <div class="flex flex-wrap items-center gap-1.5">
          <span class="text-2xs font-semibold uppercase tracking-wide text-faint">Busiest nodes</span>
          <For each={busiest()}>
            {([node, count]) => (
              <button
                type="button"
                aria-pressed={props.activeNode === node}
                class={cx(
                  'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 font-mono text-2xs transition-colors',
                  props.activeNode === node ? 'border-brass/50 bg-brass/10 text-ink' : 'border-line bg-surface hover:border-line-strong hover:text-ink',
                )}
                title={`Show only ${node}'s sessions`}
                onClick={() => props.onNode(node)}
              >
                {node || 'unknown node'} <span class="tabular-nums text-faint">{formatCount(count)}</span>
              </button>
            )}
          </For>
          <Show when={props.sessionsByNode.size > TOP_NODES}>
            <span class="text-2xs text-faint">and {formatCount(props.sessionsByNode.size - TOP_NODES)} more nodes</span>
          </Show>
        </div>
      </Show>
    </div>
  )
}

/**
 * The Instances card before the listing is fetched: the job's envelopes
 * could hold more sessions than a browser should decode in one reply
 * (ListInstances has no paging), so the page waits to be asked.
 */
export function DeferredInstancesCard(props: { jobName: string; capacity?: number; envelopes: number; onLoad: () => void; loading: boolean }) {
  return (
    <SectionCard title="Instances" description="Stable identities for stateful tasks, and the job's running sessions.">
      <div class="space-y-3 text-sm text-muted">
        <p>
          <Show
            when={props.capacity !== undefined}
            fallback={
              <>
                This job runs sessions, and how many its envelopes can hold is not known here (counting envelopes needs{' '}
                <code class="font-mono text-xs">node:read</code>, and the spec must size both the envelope and the session).
              </>
            }
          >
            This job's {formatCount(props.envelopes)} {props.envelopes === 1 ? 'envelope' : 'envelopes'} can hold up to{' '}
            <span class="font-semibold text-ink">{formatCount(props.capacity!)}</span> sessions.
          </Show>{' '}
          The manager lists instances and sessions in a single reply with no paging, so the list is not loaded automatically. Loading it reads every running session once; the page
          then shows the first {formatCount(INSTANCE_PAGE)} and counts the rest.
        </p>
        <div class="flex flex-wrap items-center gap-2">
          <Button size="sm" icon="boxes" disabled={props.loading} onClick={props.onLoad}>
            {props.loading ? 'Loading…' : 'Load instances and sessions'}
          </Button>
          <span class="text-xs text-faint">or from the CLI:</span>
          <code class="rounded bg-surface2 px-1 font-mono text-xs">{listCommand(props.jobName)}</code>
          <CopyButton text={listCommand(props.jobName)} label="" size="xs" />
        </div>
      </div>
    </SectionCard>
  )
}
