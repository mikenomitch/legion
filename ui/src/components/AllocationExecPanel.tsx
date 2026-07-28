// Per-allocation exec + actions UI. Renders:
//   - an Exec launcher (command + interactive toggle) that opens a terminal;
//   - an Actions list (from ListJobActions) with generated param forms.
// Both run over the same WebSocket exec bridge.

import { For, Show, createResource, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import { allows, jobScope } from '../utils/acl'
import type { JobAction } from '../gen/legion_pb'
import ExecTerminal from './ExecTerminal'
import type { ExecStartOptions } from '../lib/execSocket'
import { Badge, Button, Icon, checkboxClass, cx, inputClass, selectClass } from './ui'

interface Props {
  allocationId: bigint
  jobName: string
  taskName: string
}

export default function AllocationExecPanel(props: Props) {
  const [open, setOpen] = createSignal(false)
  const [command, setCommand] = createSignal('/bin/sh')
  const [interactive, setInteractive] = createSignal(true)
  // A monotonically-bumped key remounts the terminal for each run.
  const [session, setSession] = createSignal<{ start: ExecStartOptions; interactive: boolean; key: number } | null>(
    null,
  )
  let runKey = 0

  const [actions] = createResource(
    () => props.jobName,
    async (job) => {
      try {
        const reply = await managerClient.listJobActions({ job })
        return reply.actions.filter((a) => a.taskName === props.taskName || a.taskName === '')
      } catch {
        return [] as JobAction[]
      }
    },
  )

  const launchExec = () => {
    const argv = command().trim().split(/\s+/).filter(Boolean)
    if (argv.length === 0) return
    runKey += 1
    setSession({
      start: {
        allocationId: props.allocationId,
        taskName: props.taskName,
        command: argv,
        tty: interactive(),
      },
      interactive: interactive(),
      key: runKey,
    })
  }

  const launchAction = (action: JobAction, params: Record<string, string>) => {
    runKey += 1
    setSession({
      start: {
        allocationId: props.allocationId,
        taskName: props.taskName,
        action: action.name,
        actionParams: params,
        tty: action.tty,
      },
      interactive: action.tty,
      key: runKey,
    })
  }

  return (
    <div class="mt-3 border-t border-line pt-3">
      <button
        type="button"
        class="flex items-center gap-1.5 text-sm font-medium text-brass-strong transition-colors hover:text-brass"
        onClick={() => setOpen(!open())}
        aria-expanded={open()}
      >
        <Icon name="chevronRight" size={13} class={cx('transition-transform', open() && 'rotate-90')} />
        <Icon name="terminal" size={14} />
        Exec &amp; actions
      </button>

      <Show when={open()}>
        <div class="mt-3 space-y-3">
          {/* Exec launcher */}
          <div class="rounded-md border border-line bg-surface p-3">
            <p class="eyebrow mb-2">Run a command</p>
            <div class="flex flex-wrap items-center gap-2">
              <input
                class={cx(inputClass, 'flex-1 font-mono')}
                value={command()}
                onInput={(e) => setCommand(e.currentTarget.value)}
                placeholder="/bin/sh"
                onKeyDown={(e) => {
                  if (e.key === 'Enter') launchExec()
                }}
              />
              <label class="flex items-center gap-1.5 text-xs text-muted">
                <input
                  type="checkbox"
                  class={checkboxClass}
                  checked={interactive()}
                  onChange={(e) => setInteractive(e.currentTarget.checked)}
                />
                Interactive (tty)
              </label>
              <Button
                variant="primary"
                icon="terminal"
                onClick={launchExec}
                disabled={!allows('exec', 'write', jobScope(props.jobName))}
                title={allows('exec', 'write', jobScope(props.jobName)) ? undefined : 'Your token cannot exec into allocations'}
              >
                Exec
              </Button>
            </div>
          </div>

          {/* Actions */}
          <Show when={(actions()?.length ?? 0) > 0}>
            <div class="rounded-md border border-line bg-surface p-3">
              <p class="eyebrow mb-2">Actions</p>
              <div class="space-y-2.5">
                <For each={actions()}>
                  {(action) => (
                    <ActionForm
                      action={action}
                      onRun={launchAction}
                      disabled={
                        // The server allows an action run via action:write on
                        // (job, action) OR exec:write on the job.
                        !allows('action', 'write', { ...jobScope(props.jobName), actionName: action.name }) &&
                        !allows('exec', 'write', jobScope(props.jobName))
                      }
                    />
                  )}
                </For>
              </div>
            </div>
          </Show>

          {/* Output terminal */}
          <Show when={session()} keyed>
            {(s) => (
              <div>
                <ExecTerminal start={s.start} interactive={s.interactive} />
              </div>
            )}
          </Show>
        </div>
      </Show>
    </div>
  )
}

function ActionForm(props: {
  action: JobAction
  onRun: (action: JobAction, params: Record<string, string>) => void
  disabled?: boolean
}) {
  const initial: Record<string, string> = {}
  for (const p of props.action.params) initial[p.name] = p.default ?? ''
  const [values, setValues] = createSignal<Record<string, string>>(initial)

  const setParam = (name: string, value: string) =>
    setValues({ ...values(), [name]: value })

  return (
    <div class="rounded-md border border-line bg-surface2/40 p-2.5">
      <div class="flex items-center justify-between gap-2">
        <div class="min-w-0">
          <span class="font-mono text-sm font-semibold">{props.action.name}</span>
          <Show when={props.action.tty}>
            <Badge tone="neutral" class="ml-2">tty</Badge>
          </Show>
          <Show when={props.action.description}>
            <p class="mt-0.5 text-xs text-faint">{props.action.description}</p>
          </Show>
        </div>
        <Button
          variant="accent"
          icon="play"
          onClick={() => props.onRun(props.action, values())}
          disabled={props.disabled}
          title={props.disabled ? 'Your token cannot run this action' : undefined}
        >
          Run
        </Button>
      </div>
      <Show when={props.action.params.length > 0}>
        <div class="mt-2 grid gap-2 sm:grid-cols-2">
          <For each={props.action.params}>
            {(param) => (
              <label class="text-xs">
                <span class="text-muted">
                  {param.name}
                  {param.required ? ' *' : ''}
                </span>
                <Show
                  when={param.type === 'enum'}
                  fallback={
                    <input
                      type={param.type === 'int' ? 'number' : 'text'}
                      class={cx(inputClass, 'mt-0.5 font-mono')}
                      value={values()[param.name] ?? ''}
                      placeholder={param.sensitive ? '••••' : param.default}
                      onInput={(e) => setParam(param.name, e.currentTarget.value)}
                    />
                  }
                >
                  <select
                    class={cx(selectClass, 'mt-0.5 w-full')}
                    value={values()[param.name] ?? ''}
                    onChange={(e) => setParam(param.name, e.currentTarget.value)}
                  >
                    <For each={param.values}>{(v) => <option value={v}>{v}</option>}</For>
                  </select>
                </Show>
              </label>
            )}
          </For>
        </div>
      </Show>
    </div>
  )
}
