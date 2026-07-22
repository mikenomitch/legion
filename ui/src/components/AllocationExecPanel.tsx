// Per-allocation exec + actions UI. Renders:
//   - an Exec launcher (command + interactive toggle) that opens a terminal;
//   - an Actions list (from ListJobActions) with generated param forms.
// Both run over the same WebSocket exec bridge.

import { For, Show, createResource, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import type { JobAction } from '../gen/legion_pb'
import ExecTerminal from './ExecTerminal'
import type { ExecStartOptions } from '../lib/execSocket'

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
    <div class="mt-3 border-t border-neutral-200 pt-3 dark:border-neutral-700">
      <button
        class="text-sm font-medium text-blue-600 hover:underline dark:text-blue-400"
        onClick={() => setOpen(!open())}
      >
        {open() ? '▾' : '▸'} Exec &amp; actions
      </button>

      <Show when={open()}>
        <div class="mt-3 space-y-4">
          {/* Exec launcher */}
          <div class="rounded-md border border-neutral-200 p-3 dark:border-neutral-700">
            <p class="mb-2 text-xs font-semibold uppercase tracking-wide text-neutral-500">
              Run a command
            </p>
            <div class="flex flex-wrap items-center gap-2">
              <input
                class="flex-1 rounded border border-neutral-300 px-2 py-1 font-mono text-sm dark:border-neutral-600 dark:bg-neutral-800"
                value={command()}
                onInput={(e) => setCommand(e.currentTarget.value)}
                placeholder="/bin/sh"
              />
              <label class="flex items-center gap-1 text-sm">
                <input
                  type="checkbox"
                  checked={interactive()}
                  onChange={(e) => setInteractive(e.currentTarget.checked)}
                />
                Interactive (tty)
              </label>
              <button
                class="rounded bg-blue-600 px-3 py-1 text-sm font-medium text-white hover:bg-blue-700"
                onClick={launchExec}
              >
                Exec
              </button>
            </div>
          </div>

          {/* Actions */}
          <Show when={(actions()?.length ?? 0) > 0}>
            <div class="rounded-md border border-neutral-200 p-3 dark:border-neutral-700">
              <p class="mb-2 text-xs font-semibold uppercase tracking-wide text-neutral-500">
                Actions
              </p>
              <div class="space-y-3">
                <For each={actions()}>
                  {(action) => <ActionForm action={action} onRun={launchAction} />}
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
}) {
  const initial: Record<string, string> = {}
  for (const p of props.action.params) initial[p.name] = p.default ?? ''
  const [values, setValues] = createSignal<Record<string, string>>(initial)

  const setParam = (name: string, value: string) =>
    setValues({ ...values(), [name]: value })

  return (
    <div class="rounded border border-neutral-200 p-2 dark:border-neutral-700">
      <div class="flex items-center justify-between">
        <div>
          <span class="font-mono text-sm font-semibold">{props.action.name}</span>
          <Show when={props.action.tty}>
            <span class="ml-2 rounded bg-neutral-200 px-1 text-xs dark:bg-neutral-700">tty</span>
          </Show>
          <Show when={props.action.description}>
            <p class="text-xs text-neutral-500">{props.action.description}</p>
          </Show>
        </div>
        <button
          class="rounded bg-emerald-600 px-3 py-1 text-sm font-medium text-white hover:bg-emerald-700"
          onClick={() => props.onRun(props.action, values())}
        >
          Run
        </button>
      </div>
      <Show when={props.action.params.length > 0}>
        <div class="mt-2 grid gap-2 sm:grid-cols-2">
          <For each={props.action.params}>
            {(param) => (
              <label class="text-xs">
                <span class="text-neutral-600 dark:text-neutral-300">
                  {param.name}
                  {param.required ? ' *' : ''}
                </span>
                <Show
                  when={param.type === 'enum'}
                  fallback={
                    <input
                      type={param.type === 'int' ? 'number' : 'text'}
                      class="mt-0.5 w-full rounded border border-neutral-300 px-2 py-1 font-mono text-sm dark:border-neutral-600 dark:bg-neutral-800"
                      value={values()[param.name] ?? ''}
                      placeholder={param.sensitive ? '••••' : param.default}
                      onInput={(e) => setParam(param.name, e.currentTarget.value)}
                    />
                  }
                >
                  <select
                    class="mt-0.5 w-full rounded border border-neutral-300 px-2 py-1 text-sm dark:border-neutral-600 dark:bg-neutral-800"
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
