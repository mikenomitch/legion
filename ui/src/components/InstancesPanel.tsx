import { For, Show, createMemo, createSignal, type Accessor } from 'solid-js'
import type { InstanceInfo } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { firstLine, forkPlan, forksByOrigin, resumeBlocker, suspendPlan } from '../utils/instances'
import { statusTone } from '../utils/status'
import InstancesTable, { INSTANCE_COLUMNS, type InstanceColumn, type InstanceRow } from './InstancesTable'
import { Button, ConfirmButton, FilterChips, Note, SectionCard, checkboxClass, cx, inputClass, selectClass } from './ui'

/*
 * The job page's Instances card (docs/suspend-and-resume.md): the table, a
 * state filter, and per-row Suspend, Resume, Resume cold, and Fork. Every
 * action is gated through utils/instances.ts, which reads the manager's own
 * checkpoint eligibility, and every action that stops or replaces something
 * is a two-step confirm. Instance create and destroy stay in the CLI.
 */

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error))

// Plain data, rendered in the component: JSX built after an await would be
// created outside the component's reactive root.
type Outcome = { message: string; failed: boolean; forks?: string[] }

// The states worth a filter chip, in the order an operator acts on them.
const STATE_ORDER = ['active', 'suspended', 'pending', 'rescheduling', 'stopped', 'terminated']

function SuspendControl(props: { row: Accessor<InstanceRow>; jobName: string; busy: boolean; onSuspend: (kind: 'memory' | 'disk') => void }) {
  const plan = () => suspendPlan(props.row().instance, props.jobName, props.row().task)
  const [chosen, setChosen] = createSignal<'memory' | 'disk' | undefined>()
  const kind = () => chosen() ?? plan().defaultKind ?? 'disk'
  const kindBlocker = () => plan().choices.find((choice) => choice.kind === kind())?.blocker
  const offersBoth = () => plan().choices.filter((choice) => !choice.blocker).length > 1
  return (
    <span class="inline-flex items-center gap-1" title={plan().blocker ?? kindBlocker()}>
      <Show when={offersBoth()}>
        <select
          class={cx(selectClass, 'py-0.5 text-2xs')}
          value={kind()}
          onChange={(event) => setChosen(event.currentTarget.value === 'memory' ? 'memory' : 'disk')}
          aria-label={`Checkpoint kind for ${props.row().instance.instanceId}`}
        >
          <For each={plan().choices}>
            {(choice) => (
              <option value={choice.kind} disabled={Boolean(choice.blocker)} title={choice.blocker}>
                {choice.kind}
              </option>
            )}
          </For>
        </select>
      </Show>
      <ConfirmButton
        size="xs"
        variant="subtle"
        icon="pause"
        label="Suspend"
        confirmLabel={kind() === 'memory' ? 'Pause, capture, and stop?' : 'Stop and keep the volume?'}
        disabled={props.busy || Boolean(plan().blocker) || Boolean(kindBlocker())}
        title={plan().blocker ?? kindBlocker() ?? `Suspend with a ${kind()} checkpoint`}
        onConfirm={() => props.onSuspend(kind())}
      />
    </span>
  )
}

function ForkControl(props: {
  row: Accessor<InstanceRow>
  jobName: string
  busy: boolean
  onFork: (count: number, spread: boolean) => Promise<boolean>
}) {
  const plan = () => forkPlan(props.row().instance, props.jobName)
  const [open, setOpen] = createSignal(false)
  const [count, setCount] = createSignal(1)
  const [spread, setSpread] = createSignal(false)
  const countValid = () => Number.isInteger(count()) && count() >= 1 && count() <= 10
  return (
    <Show
      when={open()}
      fallback={
        <span title={plan().blocker}>
          <Button size="xs" icon="copy" disabled={props.busy || Boolean(plan().blocker)} onClick={() => setOpen(true)}>
            Fork
          </Button>
        </span>
      }
    >
      <span class="inline-flex flex-wrap items-center justify-end gap-1.5 rounded-md border border-line bg-surface2/60 px-1.5 py-1">
        <label class="inline-flex items-center gap-1 text-2xs text-muted">
          count
          <input
            type="number"
            min="1"
            max="10"
            class={cx(inputClass, 'w-12 py-0.5 text-2xs', !countValid() && 'border-red-500')}
            value={count()}
            onInput={(event) => setCount(Number(event.currentTarget.value))}
          />
        </label>
        <label class="inline-flex items-center gap-1 text-2xs text-muted" title={plan().spreadBlocker ?? 'Place each fork on its own node, from a published checkpoint'}>
          <input
            type="checkbox"
            class={checkboxClass}
            checked={spread()}
            disabled={Boolean(plan().spreadBlocker)}
            onChange={(event) => setSpread(event.currentTarget.checked)}
          />
          spread
        </label>
        <ConfirmButton
          size="xs"
          variant="subtle"
          icon="copy"
          label="Fork"
          confirmLabel={`Create ${count()} fork${count() === 1 ? '' : 's'}?`}
          disabled={props.busy || !countValid() || Boolean(plan().blocker)}
          onConfirm={() =>
            void props.onFork(count(), spread()).then((ok) => {
              if (ok) setOpen(false)
            })
          }
        />
        <Button size="xs" variant="ghost" icon="x" aria-label="Cancel fork" onClick={() => setOpen(false)} />
      </span>
    </Show>
  )
}

export default function InstancesPanel(props: {
  jobName: string
  rows: readonly InstanceRow[]
  // Called after an action so the owner refetches instances right away.
  onChanged?: () => void
  keyed: boolean
}) {
  const [filter, setFilter] = createSignal('all')
  const [outcome, setOutcome] = createSignal<Outcome | null>(null)
  const [busy, setBusy] = createSignal(false)
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

  const run = async (action: () => Promise<Outcome>): Promise<boolean> => {
    setBusy(true)
    try {
      setOutcome(await action())
      props.onChanged?.()
      return true
    } catch (error) {
      setOutcome({ message: errorText(error), failed: true })
      return false
    } finally {
      setBusy(false)
    }
  }

  const done = (message: string): Outcome => ({ message, failed: false })

  const suspend = (instance: InstanceInfo, kind: 'memory' | 'disk') =>
    run(async () => {
      const reply = await managerClient.suspendInstance({ instanceId: instance.instanceId, checkpoint: kind })
      return done(firstLine(reply.message) || `Suspending ${instance.instanceId}`)
    })

  const resume = (instance: InstanceInfo, cold: boolean) =>
    run(async () => {
      const reply = await managerClient.startInstance({ instanceId: instance.instanceId, cold })
      return done(firstLine(reply.message) || `Starting ${instance.instanceId}`)
    })

  const fork = (instance: InstanceInfo, count: number, spread: boolean) =>
    run(async () => {
      const reply = await managerClient.forkInstance({ instanceId: instance.instanceId, count, spread })
      setHighlight(reply.instances[0])
      const where = reply.durability === 'store' ? 'published, one node each' : `on ${reply.nodeId}`
      return {
        message: `Forking ${instance.instanceId} (${reply.kind} checkpoint ${reply.snapshot}, ${where}). The forks wait for the checkpoint, then start:`,
        failed: false,
        forks: reply.instances,
      }
    })

  const actionsColumn: InstanceColumn = {
    key: 'actions',
    header: <span class="sr-only">Actions</span>,
    class: 'text-right',
    cell: (row) => (
      <div class="flex flex-wrap justify-end gap-1.5">
        <Show when={row().instance.mode === 'keyed' && row().instance.state === 'active'}>
          <SuspendControl row={row} jobName={props.jobName} busy={busy()} onSuspend={(kind) => void suspend(row().instance, kind)} />
          <ForkControl row={row} jobName={props.jobName} busy={busy()} onFork={(count, spread) => fork(row().instance, count, spread)} />
        </Show>
        <Show when={row().instance.state === 'suspended'}>
          <span title={resumeBlocker(row().instance, props.jobName)}>
            <ConfirmButton
              size="xs"
              variant="subtle"
              icon="play"
              label="Resume"
              confirmLabel="Restore from the checkpoint?"
              disabled={busy() || Boolean(resumeBlocker(row().instance, props.jobName))}
              onConfirm={() => void resume(row().instance, false)}
            />
          </span>
          <span title={resumeBlocker(row().instance, props.jobName) ?? 'Discard the checkpoint and boot fresh on the retained storage'}>
            <ConfirmButton
              size="xs"
              icon="refresh"
              label="Resume cold"
              confirmLabel="Discard the checkpoint?"
              disabled={busy() || Boolean(resumeBlocker(row().instance, props.jobName))}
              onConfirm={() => void resume(row().instance, true)}
            />
          </span>
        </Show>
      </div>
    ),
  }

  const columns = createMemo(() => (props.keyed ? [...INSTANCE_COLUMNS, actionsColumn] : INSTANCE_COLUMNS))

  return (
    <SectionCard
      title="Instances"
      description="Stable identities for stateful tasks. Instance ids and storage survive reschedules, suspends, and restores."
      actions={<Show when={instances().length > 1}>{<FilterChips label="Filter instances by state" chips={chips()} active={filter()} onChange={setFilter} />}</Show>}
      flush
    >
      <Show when={outcome()}>
        {(result) => (
          <div aria-live="polite" class="px-4 pt-3">
            <Note tone={result().failed ? 'danger' : 'ok'} onDismiss={() => setOutcome(null)}>
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
