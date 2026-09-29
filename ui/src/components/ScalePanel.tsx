import { Index, Show, createEffect, createResource, createSignal, on, type Accessor } from 'solid-js'
import type { TaskScaleStatus } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { allows, jobScope } from '../utils/acl'
import { Badge, Button, Icon, Note, RelativeTime, SectionCard, TableShell, cx, inputClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

/*
 * Scale a job's tasks from its page — the same `ScaleTask` the CLI's
 * `legion job scale` uses: a count override laid over the spec, no new job
 * version and no rollout. The status columns come from GetJobScaleStatus,
 * polled with the page so an applied change (or a deferred scale-in
 * completing) shows up without a reload.
 */

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error))

function ScaleRow(props: {
  jobName: string
  task: TaskScaleStatus
  keyed: boolean
  mayWrite: boolean
  onDone: (message: string, deferred: boolean) => void
  onError: (message: string) => void
}) {
  const [draft, setDraft] = createSignal<string>(String(props.task.effectiveCount))
  const [touched, setTouched] = createSignal(false)
  const [busy, setBusy] = createSignal(false)

  // The panel polls. A count that changed underneath (an applied scale, a
  // teammate's) refreshes the field unless the user is mid-edit.
  createEffect(
    on(
      () => props.task.effectiveCount,
      (count) => {
        if (!touched()) setDraft(String(count))
      },
      { defer: true },
    ),
  )

  const parsedDraft = () => {
    const value = Number.parseInt(draft(), 10)
    return Number.isInteger(value) && value >= 0 ? value : undefined
  }
  const dirty = () => parsedDraft() !== undefined && parsedDraft() !== props.task.effectiveCount
  const disabledReason = () => {
    if (props.keyed) return 'Keyed stateful tasks have no count — manage them with legion instance create/destroy'
    if (!props.mayWrite) return 'Your token cannot scale this job (needs scale:write)'
    return undefined
  }

  const apply = async (request: { count?: number; reset?: boolean }) => {
    setBusy(true)
    try {
      const reply = await managerClient.scaleTask({
        name: props.jobName,
        taskGroup: props.task.taskGroup,
        taskName: props.task.taskName,
        count: request.count,
        reset: request.reset ?? false,
        reason: 'ui',
      })
      setTouched(false)
      props.onDone(reply.message, reply.deferred)
    } catch (error) {
      props.onError(errorText(error))
    } finally {
      setBusy(false)
    }
  }

  return (
    <tr class={trHoverClass}>
      <td class={tdClass}>
        <span class="font-medium text-ink">{props.task.taskName}</span>
        <span class="ml-1.5 text-2xs text-faint">group {props.task.taskGroup}</span>
      </td>
      <td class={cx(tdClass, 'tabular-nums text-muted')}>{props.task.specCount}</td>
      <td class={tdClass}>
        <Show when={props.task.overrideCount !== undefined} fallback={<span class="text-faint">—</span>}>
          <span class="inline-flex flex-wrap items-center gap-1.5">
            <span class="tabular-nums text-ink">{props.task.overrideCount}</span>
            <Show when={props.task.source}>
              <Badge tone="accent" title={props.task.reason ? `Reason: ${props.task.reason}` : undefined}>
                by {props.task.source}
              </Badge>
            </Show>
            <Show when={props.task.updatedAt}>
              <RelativeTime value={props.task.updatedAt} class="text-2xs text-faint" />
            </Show>
          </span>
        </Show>
      </td>
      <td class={cx(tdClass, 'font-semibold tabular-nums text-ink')}>
        {props.task.effectiveCount}
        <Show when={props.task.pendingCount !== undefined}>
          <Badge tone="warn" class="ml-1.5" title="A scale-in waits for the in-flight rollout to finish">
            → {props.task.pendingCount} pending
          </Badge>
        </Show>
      </td>
      <td class={cx(tdClass, 'text-right')}>
        <div class="inline-flex items-center gap-1.5" title={disabledReason()}>
          <input
            type="number"
            min="0"
            step="1"
            inputmode="numeric"
            class={cx(inputClass, 'w-20 py-1 text-right tabular-nums')}
            value={draft()}
            disabled={busy() || disabledReason() !== undefined}
            aria-label={`Desired count for ${props.task.taskName}`}
            onInput={(event) => {
              setTouched(true)
              setDraft(event.currentTarget.value)
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && dirty() && !busy()) void apply({ count: parsedDraft() })
            }}
          />
          <Button
            size="xs"
            variant="primary"
            icon="sliders"
            disabled={busy() || !dirty() || disabledReason() !== undefined}
            onClick={() => void apply({ count: parsedDraft() })}
          >
            Scale
          </Button>
          <Show when={props.task.overrideCount !== undefined}>
            <Button
              size="xs"
              variant="ghost"
              disabled={busy() || disabledReason() !== undefined}
              title="Drop the override and return to the spec count"
              onClick={() => {
                setDraft(String(props.task.specCount))
                setTouched(false)
                void apply({ reset: true })
              }}
            >
              Reset
            </Button>
          </Show>
        </div>
      </td>
    </tr>
  )
}

export default function ScalePanel(props: { jobName: string; tick: Accessor<number>; keyedTasks: ReadonlySet<string> }) {
  const [status, { refetch }] = createResource(
    () => [props.jobName, props.tick()] as const,
    async ([name]) => {
      try {
        return (await managerClient.getJobScaleStatus({ name })).tasks
      } catch {
        // Denied (`scale:read` missing) or an older manager: the panel simply
        // does not render rather than explaining itself on every job page.
        return null
      }
    },
  )
  const [notice, setNotice] = createSignal<{ text: string; tone: 'ok' | 'warn' | 'danger' } | null>(null)
  const mayWrite = () => allows('scale', 'write', jobScope(props.jobName))

  return (
    <Show when={(status() ?? []).length > 0}>
      <SectionCard
        title="Scale"
        description="Change how many copies of a task run — no new job version, no rollout. Overrides survive resubmits until reset."
        flush
        actions={
          <Show when={!mayWrite()}>
            <Badge tone="neutral" title="Scaling has its own ACL resource: scale:write, scoped by job name">read-only</Badge>
          </Show>
        }
      >
        <Show when={notice()}>
          {(current) => (
            <div class="px-4 pt-3">
              <Note tone={current().tone} onDismiss={() => setNotice(null)}>{current().text}</Note>
            </div>
          )}
        </Show>
        <TableShell>
          <table class={tableClass}>
            <thead class={theadClass}>
              <tr>
                <th class={thClass}>Task</th>
                <th class={thClass}>Spec</th>
                <th class={thClass}>Override</th>
                <th class={thClass}>Effective</th>
                <th class={cx(thClass, 'text-right')}>Desired</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-line">
              {/* Index, not For: each poll returns fresh objects, and a
                  keyed list would remount every row (and its input) each time. */}
              <Index each={status() ?? []}>
                {(task) => (
                  <ScaleRow
                    jobName={props.jobName}
                    task={task()}
                    keyed={props.keyedTasks.has(task().taskName)}
                    mayWrite={mayWrite()}
                    onDone={(message, deferred) => {
                      setNotice({ text: message, tone: deferred ? 'warn' : 'ok' })
                      void refetch()
                    }}
                    onError={(message) => setNotice({ text: message, tone: 'danger' })}
                  />
                )}
              </Index>
            </tbody>
          </table>
        </TableShell>
        <p class="flex items-center gap-1.5 border-t border-line px-4 py-2 text-2xs text-faint">
          <Icon name="info" size={12} />
          Scale-out applies immediately; a scale-in during a rollout waits for it to finish. Same as <code class="font-mono">legion job scale</code>.
        </p>
      </SectionCard>
    </Show>
  )
}
