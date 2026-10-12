import { For, Show, createSignal, type Accessor } from 'solid-js'
import type { InstanceInfo } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { firstLine, forkPlan, resumeBlocker, startBlocker, stopBlocker, suspendPlan } from '../utils/instances'
import { isSession, promoteBlocker, promoteNameError } from '../utils/sessions'
import type { InstanceRow } from './InstancesTable'
import { Button, ConfirmButton, checkboxClass, cx, inputClass, selectClass } from './ui'

/*
 * The instance lifecycle controls (docs/suspend-and-resume.md): Suspend,
 * Resume, Resume cold, and Fork on the job page's Instances card, plus Stop
 * and Start on the instance's own page. A session (docs/session-mode.md)
 * offers Promote and Stop instead, on both: it cannot be suspended or
 * forked until it is promoted, and stopping one leaves nothing behind. Every control is gated through
 * utils/instances.ts, which reads the manager's own checkpoint eligibility,
 * and every action that stops or replaces something is a two-step confirm.
 * Create and destroy stay in the CLI.
 *
 * `createInstanceActions` owns the RPCs, the busy flag, and the outcome, so
 * one owner (a card or a page) shows one result for whichever control ran.
 */

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error))

type Size = 'xs' | 'sm'

/**
 * What the last action said. Plain data, rendered by the owner: JSX built
 * after an await would be created outside the component's reactive root.
 */
export type InstanceActionOutcome = { message: string; failed: boolean; forks?: string[]; promoted?: string }

export type InstanceActions = {
  busy: Accessor<boolean>
  outcome: Accessor<InstanceActionOutcome | null>
  dismiss: () => void
  suspend: (instance: InstanceInfo, kind: 'memory' | 'disk') => Promise<boolean>
  resume: (instance: InstanceInfo, cold: boolean) => Promise<boolean>
  fork: (instance: InstanceInfo, count: number, spread: boolean) => Promise<boolean>
  stop: (instance: InstanceInfo) => Promise<boolean>
  start: (instance: InstanceInfo) => Promise<boolean>
  /** Roll the instance back to one of its own snapshots (RestoreInstance). */
  restore: (instance: InstanceInfo, snapshotId: string) => Promise<boolean>
  /** Give a running session a row, in place (PromoteInstance). An empty name keeps the session id. */
  promote: (instance: InstanceInfo, newId: string) => Promise<boolean>
}

export function createInstanceActions(options: {
  // Called after a successful action so the owner refetches right away.
  onChanged?: () => void
  // The instances a fork created, for the owner to point at.
  onForked?: (forks: string[]) => void
  // The keyed instance a promotion created, for the owner to point at.
  onPromoted?: (instanceId: string) => void
} = {}): InstanceActions {
  const [outcome, setOutcome] = createSignal<InstanceActionOutcome | null>(null)
  const [busy, setBusy] = createSignal(false)

  const run = async (action: () => Promise<InstanceActionOutcome>): Promise<boolean> => {
    setBusy(true)
    try {
      setOutcome(await action())
      options.onChanged?.()
      return true
    } catch (error) {
      setOutcome({ message: errorText(error), failed: true })
      return false
    } finally {
      setBusy(false)
    }
  }

  const done = (message: string): InstanceActionOutcome => ({ message, failed: false })

  return {
    busy,
    outcome,
    dismiss: () => setOutcome(null),
    suspend: (instance, kind) =>
      run(async () => {
        const reply = await managerClient.suspendInstance({ instanceId: instance.instanceId, checkpoint: kind })
        return done(firstLine(reply.message) || `Suspending ${instance.instanceId}`)
      }),
    resume: (instance, cold) =>
      run(async () => {
        const reply = await managerClient.startInstance({ instanceId: instance.instanceId, cold })
        return done(firstLine(reply.message) || `Starting ${instance.instanceId}`)
      }),
    fork: (instance, count, spread) =>
      run(async () => {
        const reply = await managerClient.forkInstance({ instanceId: instance.instanceId, count, spread })
        options.onForked?.(reply.instances)
        const where = reply.durability === 'store' ? 'published, one node each' : `on ${reply.nodeId}`
        return {
          message: `Forking ${instance.instanceId} (${reply.kind} checkpoint ${reply.snapshot}, ${where}). The forks wait for the checkpoint, then start:`,
          failed: false,
          forks: reply.instances,
        }
      }),
    stop: (instance) =>
      run(async () => {
        const reply = await managerClient.stopInstance({ instanceId: instance.instanceId })
        return done(firstLine(reply.message) || (isSession(instance) ? `Ended session ${instance.instanceId}` : `Stopping ${instance.instanceId}`))
      }),
    start: (instance) =>
      run(async () => {
        const reply = await managerClient.startInstance({ instanceId: instance.instanceId })
        return done(firstLine(reply.message) || `Starting ${instance.instanceId}`)
      }),
    restore: (instance, snapshotId) =>
      run(async () => {
        const reply = await managerClient.restoreInstance({ instanceId: instance.instanceId, snapshot: snapshotId })
        return done(firstLine(reply.message) || `Restoring ${instance.instanceId} from ${snapshotId}`)
      }),
    promote: (instance, newId) =>
      run(async () => {
        const reply = await managerClient.promoteInstance({ instanceId: instance.instanceId, newId: newId || undefined })
        options.onPromoted?.(reply.instanceId)
        const renamed = reply.instanceId !== reply.promotedFrom ? ` as ${reply.instanceId}` : ''
        return {
          message: `${reply.deduplicated ? 'Already promoted' : 'Promoted'} session ${reply.promotedFrom}${renamed}: a keyed instance now, adopted in place as allocation #${String(reply.allocationId)} on ${reply.nodeId}.`,
          failed: false,
          promoted: reply.instanceId,
        }
      }),
  }
}

// The compact row controls and the page header's larger ones share markup;
// only the select and input sizes differ.
const fieldSize = (size: Size | undefined) => (size === 'sm' ? 'py-1 text-xs' : 'py-0.5 text-2xs')

export function SuspendControl(props: {
  row: Accessor<InstanceRow>
  jobName: string
  busy: boolean
  onSuspend: (kind: 'memory' | 'disk') => void
  size?: Size
}) {
  const plan = () => suspendPlan(props.row().instance, props.jobName, props.row().task)
  const [chosen, setChosen] = createSignal<'memory' | 'disk' | undefined>()
  const kind = () => chosen() ?? plan().defaultKind ?? 'disk'
  const kindBlocker = () => plan().choices.find((choice) => choice.kind === kind())?.blocker
  const offersBoth = () => plan().choices.filter((choice) => !choice.blocker).length > 1
  return (
    <span class="inline-flex items-center gap-1" title={plan().blocker ?? kindBlocker()}>
      <Show when={offersBoth()}>
        <select
          class={cx(selectClass, fieldSize(props.size))}
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
        size={props.size ?? 'xs'}
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

export function ForkControl(props: {
  row: Accessor<InstanceRow>
  jobName: string
  busy: boolean
  onFork: (count: number, spread: boolean) => Promise<boolean>
  size?: Size
}) {
  const plan = () => forkPlan(props.row().instance, props.jobName)
  const [open, setOpen] = createSignal(false)
  const [count, setCount] = createSignal(1)
  const [spread, setSpread] = createSignal(false)
  const countValid = () => Number.isInteger(count()) && count() >= 1 && count() <= 10
  const textSize = () => (props.size === 'sm' ? 'text-xs' : 'text-2xs')
  return (
    <Show
      when={open()}
      fallback={
        <span title={plan().blocker}>
          <Button size={props.size ?? 'xs'} icon="copy" disabled={props.busy || Boolean(plan().blocker)} onClick={() => setOpen(true)}>
            Fork
          </Button>
        </span>
      }
    >
      <span class="inline-flex flex-wrap items-center justify-end gap-1.5 rounded-md border border-line bg-surface2/60 px-1.5 py-1">
        <label class={cx('inline-flex items-center gap-1 text-muted', textSize())}>
          count
          <input
            type="number"
            min="1"
            max="10"
            class={cx(inputClass, 'w-12', fieldSize(props.size), !countValid() && 'border-red-500')}
            value={count()}
            onInput={(event) => setCount(Number(event.currentTarget.value))}
          />
        </label>
        <label
          class={cx('inline-flex items-center gap-1 text-muted', textSize())}
          title={plan().spreadBlocker ?? 'Place each fork on its own node, from a published checkpoint'}
        >
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
          size={props.size ?? 'xs'}
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
        <Button size={props.size ?? 'xs'} variant="ghost" icon="x" aria-label="Cancel fork" onClick={() => setOpen(false)} />
      </span>
    </Show>
  )
}

/** Resume from the checkpoint, or discard it and boot fresh on the retained storage. */
export function ResumeControls(props: { instance: InstanceInfo; jobName: string; busy: boolean; onResume: (cold: boolean) => void; size?: Size }) {
  const blocker = () => resumeBlocker(props.instance, props.jobName)
  return (
    <>
      <span title={blocker()}>
        <ConfirmButton
          size={props.size ?? 'xs'}
          variant="subtle"
          icon="play"
          label="Resume"
          confirmLabel="Restore from the checkpoint?"
          disabled={props.busy || Boolean(blocker())}
          onConfirm={() => props.onResume(false)}
        />
      </span>
      <span title={blocker() ?? 'Discard the checkpoint and boot fresh on the retained storage'}>
        <ConfirmButton
          size={props.size ?? 'xs'}
          icon="refresh"
          label="Resume cold"
          confirmLabel="Discard the checkpoint?"
          disabled={props.busy || Boolean(blocker())}
          onConfirm={() => props.onResume(true)}
        />
      </span>
    </>
  )
}

/**
 * Stop keeps the row, id, and storage, so Start brings the same instance
 * back. Stopping a suspended instance discards its checkpoint, which the
 * confirm says.
 */
export function StopControl(props: { instance: InstanceInfo; jobName: string; busy: boolean; onStop: () => void; size?: Size }) {
  const blocker = () => stopBlocker(props.instance, props.jobName)
  const session = () => isSession(props.instance)
  return (
    <span
      title={
        blocker() ??
        (session()
          ? 'End the session on its node. Nothing is kept: a session has no row, id, or storage to come back to.'
          : 'Stop the allocation and keep the id and storage. Start brings it back.')
      }
    >
      <ConfirmButton
        size={props.size ?? 'xs'}
        icon="square"
        label={session() ? 'End' : 'Stop'}
        confirmLabel={session() ? 'End the session? Nothing is kept.' : props.instance.state === 'suspended' ? 'Discard the checkpoint and stop?' : 'Stop and keep the storage?'}
        disabled={props.busy || Boolean(blocker())}
        onConfirm={props.onStop}
      />
    </span>
  )
}

/** Start a stopped instance: it is placed again, booting cold on its retained storage. */
export function StartControl(props: { instance: InstanceInfo; jobName: string; busy: boolean; onStart: () => void; size?: Size }) {
  const blocker = () => startBlocker(props.instance, props.jobName)
  return (
    <span title={blocker() ?? 'Place it again on its retained storage (boots cold)'}>
      <Button size={props.size ?? 'xs'} icon="play" disabled={props.busy || Boolean(blocker())} onClick={props.onStart}>
        Start
      </Button>
    </span>
  )
}

/**
 * Promote a session to a keyed instance in place (PromoteInstance): one
 * replicated write, nothing restarts. The optional name is refused when the
 * task sets `stateful.promote.onName = false`, so the field is hidden then.
 */
export function PromoteControl(props: {
  row: Accessor<InstanceRow>
  jobName: string
  busy: boolean
  onPromote: (newId: string) => Promise<boolean>
  size?: Size
}) {
  const blocker = () => promoteBlocker(props.row().instance, props.jobName)
  const mayName = () => props.row().task?.stateful?.promoteOnName !== false
  const [open, setOpen] = createSignal(false)
  const [name, setName] = createSignal('')
  const nameError = () => promoteNameError(name().trim())
  const textSize = () => (props.size === 'sm' ? 'text-xs' : 'text-2xs')
  return (
    <Show
      when={open()}
      fallback={
        <span title={blocker() ?? 'Give this session a row without restarting it: durable, rescheduled on node loss, suspendable, forkable'}>
          <Button size={props.size ?? 'xs'} icon="plus" disabled={props.busy || Boolean(blocker())} onClick={() => setOpen(true)}>
            Promote
          </Button>
        </span>
      }
    >
      <span class="inline-flex flex-wrap items-center justify-end gap-1.5 rounded-md border border-line bg-surface2/60 px-1.5 py-1">
        <Show
          when={mayName()}
          fallback={
            <span class={cx('text-muted', textSize())} title="The task sets stateful.promote.onName = false">
              keeps its id
            </span>
          }
        >
          <label class={cx('inline-flex items-center gap-1 text-muted', textSize())} title={nameError() ?? 'Empty keeps the session id, which stays unnamed'}>
            name
            <input
              class={cx(inputClass, 'w-32', fieldSize(props.size), nameError() && 'border-red-500')}
              placeholder="optional"
              value={name()}
              aria-invalid={Boolean(nameError())}
              aria-label={`New id for ${props.row().instance.instanceId}`}
              onInput={(event) => setName(event.currentTarget.value)}
            />
          </label>
        </Show>
        <ConfirmButton
          size={props.size ?? 'xs'}
          variant="subtle"
          icon="plus"
          label="Promote"
          confirmLabel={name().trim() ? `Promote as ${name().trim()}?` : 'Promote, keeping its id?'}
          disabled={props.busy || Boolean(blocker()) || Boolean(nameError())}
          onConfirm={() =>
            void props.onPromote(mayName() ? name().trim() : '').then((ok) => {
              if (ok) {
                setOpen(false)
                setName('')
              }
            })
          }
        />
        <Button size={props.size ?? 'xs'} variant="ghost" icon="x" aria-label="Cancel promote" onClick={() => setOpen(false)} />
      </span>
    </Show>
  )
}

/**
 * The controls an instance offers in its current state. `lifecycle` adds
 * Stop and Start, which the instance page offers and the job page's rows
 * leave to the CLI.
 */
export function InstanceActionButtons(props: {
  row: Accessor<InstanceRow>
  jobName: string
  actions: InstanceActions
  size?: Size
  lifecycle?: boolean
  class?: string
}) {
  const instance = () => props.row().instance
  const keyed = () => instance().mode === 'keyed'
  return (
    <div class={cx('flex flex-wrap items-center justify-end gap-1.5', props.class)}>
      {/* A session's whole lifecycle is Promote or End, on rows and pages alike. */}
      <Show when={isSession(instance())}>
        <PromoteControl
          row={props.row}
          jobName={props.jobName}
          busy={props.actions.busy()}
          size={props.size}
          onPromote={(newId) => props.actions.promote(instance(), newId)}
        />
        <StopControl instance={instance()} jobName={props.jobName} busy={props.actions.busy()} size={props.size} onStop={() => void props.actions.stop(instance())} />
      </Show>
      <Show when={keyed() && instance().state === 'active'}>
        <SuspendControl
          row={props.row}
          jobName={props.jobName}
          busy={props.actions.busy()}
          size={props.size}
          onSuspend={(kind) => void props.actions.suspend(instance(), kind)}
        />
        <ForkControl
          row={props.row}
          jobName={props.jobName}
          busy={props.actions.busy()}
          size={props.size}
          onFork={(count, spread) => props.actions.fork(instance(), count, spread)}
        />
      </Show>
      <Show when={instance().state === 'suspended'}>
        <ResumeControls
          instance={instance()}
          jobName={props.jobName}
          busy={props.actions.busy()}
          size={props.size}
          onResume={(cold) => void props.actions.resume(instance(), cold)}
        />
      </Show>
      <Show when={props.lifecycle && keyed()}>
        <Show
          when={instance().state === 'stopped'}
          fallback={
            <Show when={instance().state !== 'terminated'}>
              <StopControl instance={instance()} jobName={props.jobName} busy={props.actions.busy()} size={props.size} onStop={() => void props.actions.stop(instance())} />
            </Show>
          }
        >
          <StartControl instance={instance()} jobName={props.jobName} busy={props.actions.busy()} size={props.size} onStart={() => void props.actions.start(instance())} />
        </Show>
      </Show>
    </div>
  )
}
