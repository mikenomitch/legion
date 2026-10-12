import { For, Show, createMemo, createSignal } from 'solid-js'
import { forksByOrigin } from '../utils/instances'
import { statusTone } from '../utils/status'
import { InstanceActionButtons, createInstanceActions } from './InstanceActions'
import InstancesTable, { INSTANCE_COLUMNS, type InstanceColumn, type InstanceRow } from './InstancesTable'
import { FilterChips, Note, SectionCard } from './ui'

/*
 * The job page's Instances card (docs/suspend-and-resume.md): the table, a
 * state filter, and per-row Suspend, Resume, Resume cold, and Fork (the
 * shared controls in InstanceActions.tsx). Every action is gated through
 * utils/instances.ts, which reads the manager's own checkpoint eligibility,
 * and every action that stops or replaces something is a two-step confirm.
 * Each id links to the instance's own page, which adds Stop, Start, and the
 * instance's events. Instance create and destroy stay in the CLI.
 */

// The states worth a filter chip, in the order an operator acts on them.
const STATE_ORDER = ['active', 'suspended', 'pending', 'rescheduling', 'stopped', 'terminated']

export default function InstancesPanel(props: {
  jobName: string
  rows: readonly InstanceRow[]
  // Called after an action so the owner refetches instances right away.
  onChanged?: () => void
  keyed: boolean
}) {
  const [filter, setFilter] = createSignal('all')
  const [highlight, setHighlight] = createSignal<string>()

  const instances = createMemo(() => props.rows.map((row) => row.instance))
  const counts = createMemo(() => {
    const byState = new Map<string, number>()
    for (const instance of instances()) byState.set(instance.state, (byState.get(instance.state) ?? 0) + 1)
    return byState
  })
  const chips = createMemo(() => {
    const present = [...counts().keys()]
    const ordered = [...STATE_ORDER.filter((state) => present.includes(state)), ...present.filter((state) => !STATE_ORDER.includes(state)).sort()]
    return [
      { id: 'all', label: 'all', count: instances().length },
      ...ordered.map((state) => ({ id: state, label: state, count: counts().get(state), tone: statusTone(state) })),
    ]
  })
  const visible = createMemo(() => (filter() === 'all' ? props.rows : props.rows.filter((row) => row.instance.state === filter())))
  const lineage = createMemo(() => forksByOrigin(instances()))

  // One runner for every row's controls, so the card shows one result for
  // whichever action ran. A fork highlights its first new row.
  const actions = createInstanceActions({
    onChanged: () => props.onChanged?.(),
    onForked: (forks) => setHighlight(forks[0]),
  })

  const actionsColumn: InstanceColumn = {
    key: 'actions',
    header: <span class="sr-only">Actions</span>,
    class: 'text-right',
    cell: (row) => <InstanceActionButtons row={row} jobName={props.jobName} actions={actions} />,
  }

  const columns = createMemo(() => (props.keyed ? [...INSTANCE_COLUMNS, actionsColumn] : INSTANCE_COLUMNS))

  return (
    <SectionCard
      title="Instances"
      description="Stable identities for stateful tasks. Instance ids and storage survive reschedules, suspends, and restores."
      actions={<Show when={instances().length > 1}>{<FilterChips label="Filter instances by state" chips={chips()} active={filter()} onChange={setFilter} />}</Show>}
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
      <Show when={lineage().size > 0}>
        <div class="flex flex-wrap gap-x-4 gap-y-1 border-b border-line px-4 py-2 text-2xs text-muted">
          <span class="font-semibold uppercase tracking-wide text-faint">Fork lineage</span>
          <For each={[...lineage().entries()]}>
            {([origin, forks]) => (
              <span class="font-mono">
                <a href={`#instance-${encodeURIComponent(origin)}`} class="hover:text-brass-strong" onClick={() => setHighlight(origin)}>
                  {origin}
                </a>{' '}
                → {forks.join(', ')}
              </span>
            )}
          </For>
        </div>
      </Show>
      <Show
        when={visible().length > 0}
        fallback={<p class="px-4 py-4 text-sm text-muted">{props.rows.length === 0 ? 'No instances yet.' : `No ${filter()} instances.`}</p>}
      >
        <InstancesTable rows={visible()} columns={columns()} highlight={highlight()} />
      </Show>
      <Show when={props.keyed}>
        <p class="border-t border-line px-4 py-2.5 text-xs text-faint">
          Create and destroy keyed instances from the CLI with{' '}
          <code class="rounded bg-surface2 px-1 font-mono">legion instance create</code> (add{' '}
          <code class="rounded bg-surface2 px-1 font-mono">--from-snapshot &lt;name&gt;</code> to start from a snapshot) and{' '}
          <code class="rounded bg-surface2 px-1 font-mono">legion instance destroy</code>.
        </p>
      </Show>
    </SectionCard>
  )
}
