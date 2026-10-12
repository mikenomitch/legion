// The task page's Actions tab: the task's declared actions as a list, and the
// chosen one as a typed form, the exact argv it renders to, a target (one
// instance, all running, or only the unhealthy ones) and a result per
// instance. A raw shell is the last entry in the list.
//
// Everything runs over the exec WebSocket bridge (lib/execSocket.ts). Running
// on several instances is one session per allocation, opened together; the
// manager renders and validates each one, so the checks here only save a
// round trip.

import { For, Index, Show, createMemo, createSignal, onCleanup } from 'solid-js'
import { createStore, produce } from 'solid-js/store'
import type { JobAction, JobActionParam } from '../gen/legion_pb'
import { ExecSession, type ExecStartOptions } from '../lib/execSocket'
import { allows, jobScope } from '../utils/acl'
import type { JobTaskAction } from '../utils/jobspec'
import { lazyComponent } from './LazyPage'
import { Badge, Button, Dot, EmptyState, Icon, Spinner, cx, inputClass } from './ui'

// xterm.js is the heaviest dependency in the app and only tty sessions need
// it, so it arrives with the first one.
const ExecTerminal = lazyComponent(() => import('./ExecTerminal'), <Spinner label="Loading terminal…" />)

/** The `?action=` value that selects the raw shell entry. */
export const SHELL_ENTRY = 'shell'

export type ActionTarget = {
  id: string
  nodeId?: string
  /** `false` only when the instance reported unhealthy; undefined = not reported. */
  healthy?: boolean
}

type Props = {
  jobName: string
  taskName: string
  actions: readonly JobAction[]
  /** The same actions as declared in the jobspec (argv template, timeout). */
  specActions: readonly JobTaskAction[]
  targets: readonly ActionTarget[]
  /** Selected list entry: an action name or SHELL_ENTRY. */
  selected: string
  onSelect: (entry: string) => void
  /** The `?alloc=` pick, used as the one-instance target when it is running. */
  preferredTarget?: string
}

type TargetMode = 'one' | 'all' | 'unhealthy'

type OutputLine = { stream: 'stdout' | 'stderr' | 'meta'; text: string }

type RunRow = {
  id: string
  nodeId?: string
  state: 'running' | 'exited' | 'error'
  code?: number
  message?: string
  lines: OutputLine[]
  startedAt: number
  endedAt?: number
  open?: boolean
}

type ActionRun = { order: string[]; rows: Record<string, RunRow> }

// Bounded per instance so a chatty action cannot grow the page without limit.
const MAX_OUTPUT_LINES = 2000

const SEGMENTED_MAX = 4

const formatDuration = (ms: number): string => (ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`)

const shellQuote = (value: string): string =>
  /^[A-Za-z0-9_@%+=:,./-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`

/**
 * The value's problem, if any, mirroring `ActionParam::validate_value` in
 * src/structs/job.rs. A pattern the browser's regex engine cannot compile is
 * left to the manager.
 */
const paramError = (param: JobActionParam, value: string): string | undefined => {
  if (value === '') return param.required ? 'Required' : undefined
  switch (param.type) {
    case 'int': {
      if (!/^-?\d+$/.test(value.trim())) return 'Whole numbers only'
      const n = Number(value)
      const min = param.min === '' ? undefined : Number(param.min)
      const max = param.max === '' ? undefined : Number(param.max)
      if (min !== undefined && n < min) return max !== undefined ? `Between ${min} and ${max}` : `At least ${min}`
      if (max !== undefined && n > max) return min !== undefined ? `Between ${min} and ${max}` : `At most ${max}`
      return undefined
    }
    case 'bool':
      return value === 'true' || value === 'false' ? undefined : 'true or false'
    case 'enum':
      return param.values.includes(value) ? undefined : `One of ${param.values.join(', ')}`
    default: {
      if (!param.pattern) return undefined
      try {
        return new RegExp(param.pattern).test(value) ? undefined : `Must match ${param.pattern}`
      } catch {
        return undefined
      }
    }
  }
}

const typeLabel = (param: JobActionParam): string => {
  switch (param.type) {
    case 'enum':
      return `enum · ${param.values.length} values`
    case 'int':
      return param.min !== '' || param.max !== '' ? `int · ${param.min || '…'}–${param.max || '…'}` : 'int'
    case 'bool':
      return 'bool'
    default:
      return param.sensitive ? 'string · sensitive' : param.pattern ? `string · ${param.pattern}` : 'string'
  }
}

const initialValue = (param: JobActionParam): string => {
  if (param.default !== '') return param.default
  return param.type === 'bool' ? 'false' : ''
}

export default function TaskActions(props: Props) {
  const [filter, setFilter] = createSignal('')
  const [values, setValues] = createStore<Record<string, Record<string, string>>>({})
  const [runs, setRuns] = createStore<Record<string, ActionRun | undefined>>({})
  const [mode, setMode] = createSignal<TargetMode>('one')
  const [chosenOne, setChosenOne] = createSignal<string | undefined>()
  // A tty action or a shell runs in a terminal; the key remounts it per run.
  const [terminal, setTerminal] = createSignal<{ owner: string; start: ExecStartOptions; key: number } | null>(null)
  let runKey = 0

  // Live sessions, closed when their action is re-run and when the tab goes.
  const sessions = new Map<string, ExecSession[]>()
  const closeSessions = (owner: string) => {
    for (const session of sessions.get(owner) ?? []) session.close()
    sessions.delete(owner)
  }
  onCleanup(() => {
    for (const owner of [...sessions.keys()]) closeSessions(owner)
  })

  const mayExec = () => allows('exec', 'write', jobScope(props.jobName))
  // The manager allows an action run via action:write on (job, action) or
  // exec:write on the job — anyone who can open a shell can type the command.
  const mayRun = (action: string) =>
    allows('action', 'write', { ...jobScope(props.jobName), actionName: action }) || mayExec()

  const visibleActions = createMemo(() => {
    const query = filter().trim().toLowerCase()
    if (!query) return props.actions
    return props.actions.filter(
      (action) => action.name.toLowerCase().includes(query) || action.description.toLowerCase().includes(query),
    )
  })

  const current = createMemo(() => props.actions.find((action) => action.name === props.selected))
  const currentSpec = createMemo(() => props.specActions.find((action) => action.name === props.selected))

  const valueOf = (action: JobAction, param: JobActionParam): string =>
    values[action.name]?.[param.name] ?? initialValue(param)
  const setValue = (action: string, param: string, value: string) =>
    setValues(produce((all) => {
      all[action] = { ...(all[action] ?? {}), [param]: value }
    }))

  // The one-instance target: an explicit pick, else the URL's, else the first
  // unhealthy instance (the likeliest reason to be here), else the first.
  const oneTarget = createMemo(() => {
    const ids = props.targets.map((target) => target.id)
    const picked = chosenOne()
    if (picked && ids.includes(picked)) return picked
    if (props.preferredTarget && ids.includes(props.preferredTarget)) return props.preferredTarget
    return props.targets.find((target) => target.healthy === false)?.id ?? ids[0]
  })
  const unhealthyTargets = createMemo(() => props.targets.filter((target) => target.healthy === false))
  const effectiveMode = (): TargetMode => {
    if (current()?.tty) return 'one'
    if (mode() === 'unhealthy' && unhealthyTargets().length === 0) return 'one'
    return mode()
  }
  const targetIds = (): string[] => {
    const one = oneTarget()
    switch (effectiveMode()) {
      case 'all':
        return props.targets.map((target) => target.id)
      case 'unhealthy':
        return unhealthyTargets().map((target) => target.id)
      default:
        return one ? [one] : []
    }
  }

  const errors = createMemo(() => {
    const action = current()
    if (!action) return []
    return action.params
      .map((param) => ({ name: param.name, error: paramError(param, valueOf(action, param)) }))
      .filter((entry): entry is { name: string; error: string } => Boolean(entry.error))
  })

  // The argv the manager will render, with the current values substituted.
  const argvPreview = createMemo(() => {
    const action = current()
    const spec = currentSpec()
    if (!action || !spec) return undefined
    return spec.command.map((part) => {
      let isParam = false
      const text = part.replace(/\$\{param\.([A-Za-z0-9_-]+)\}/g, (_, name: string) => {
        isParam = true
        const param = action.params.find((candidate) => candidate.name === name)
        const value = param ? valueOf(action, param) : ''
        if (param?.sensitive) return value ? '••••••' : `‹${name}›`
        return value === '' ? `‹${name}›` : value
      })
      return { text, isParam }
    })
  })

  const cliLine = createMemo(() => {
    const action = current()
    if (!action) return ''
    const parts = [`legion action run ${shellQuote(props.jobName)} ${action.name}`]
    if (effectiveMode() === 'one' && oneTarget()) parts.push(`--alloc ${oneTarget()}`)
    for (const param of action.params) {
      const value = valueOf(action, param)
      if (value === '') continue
      parts.push(`--param ${param.name}=${param.sensitive ? '***' : shellQuote(value)}`)
    }
    if (action.tty) parts.push('-it')
    const line = parts.join(' ')
    return effectiveMode() === 'one' ? line : `${line}   # once per instance, with --alloc`
  })

  const runLabel = () => {
    const action = current()
    if (!action) return 'Run'
    if (action.tty) return `Open ${action.name} in #${oneTarget() ?? '?'}`
    const ids = targetIds()
    return ids.length === 1 ? `Run ${action.name} on #${ids[0]}` : `Run ${action.name} on ${ids.length} instances`
  }

  const sentParams = (action: JobAction): Record<string, string> => {
    const sent: Record<string, string> = {}
    for (const param of action.params) {
      const value = valueOf(action, param)
      // Empty optional values are left out: the manager applies the default.
      if (value !== '') sent[param.name] = value
    }
    return sent
  }

  const run = () => {
    const action = current()
    if (!action || errors().length > 0 || !mayRun(action.name)) return
    const params = sentParams(action)
    if (action.tty) {
      const id = oneTarget()
      if (!id) return
      runKey += 1
      setTerminal({
        owner: action.name,
        key: runKey,
        start: { allocationId: BigInt(id), taskName: props.taskName, action: action.name, actionParams: params, tty: true },
      })
      return
    }

    closeSessions(action.name)
    const ids = targetIds()
    const nodes = new Map(props.targets.map((target) => [target.id, target.nodeId]))
    const startedAt = Date.now()
    setRuns(action.name, {
      order: ids,
      rows: Object.fromEntries(
        ids.map((id) => [id, { id, nodeId: nodes.get(id), state: 'running', lines: [], startedAt } satisfies RunRow]),
      ),
    })
    const decoder = new TextDecoder()
    const opened: ExecSession[] = []
    for (const id of ids) {
      // Split on newlines per stream; a partial last line waits for its end.
      const partial = { stdout: '', stderr: '' }
      const append = (stream: 'stdout' | 'stderr', chunk: string) => {
        const parts = (partial[stream] + chunk).split('\n')
        partial[stream] = parts.pop() ?? ''
        if (parts.length === 0) return
        setRuns(action.name, 'rows', id, 'lines', (lines) => {
          const next = lines.concat(parts.map((text) => ({ stream, text })))
          return next.length > MAX_OUTPUT_LINES ? next.slice(next.length - MAX_OUTPUT_LINES) : next
        })
      }
      const flush = () => {
        for (const stream of ['stdout', 'stderr'] as const) {
          if (partial[stream]) append(stream, '\n')
        }
      }
      const session = new ExecSession(
        { allocationId: BigInt(id), taskName: props.taskName, action: action.name, actionParams: params, tty: false },
        {
          onStdout: (data) => append('stdout', decoder.decode(data)),
          onStderr: (data) => append('stderr', decoder.decode(data)),
          onExit: (code, message) => {
            flush()
            // A negative code is the bridge's own (socket closed before an exit).
            setRuns(action.name, 'rows', id, { state: code < 0 ? 'error' : 'exited', code, message, endedAt: Date.now() })
          },
          onError: (message) => {
            setRuns(action.name, 'rows', id, { state: 'error', message, endedAt: Date.now() })
          },
        },
      )
      // One-shot: no input, so the process's stdin closes right away.
      session.closeStdin()
      opened.push(session)
    }
    sessions.set(action.name, opened)
  }

  const currentRun = () => runs[props.selected]
  const runSummary = () => {
    const record = currentRun()
    if (!record) return ''
    const rows = record.order.map((id) => record.rows[id])
    const running = rows.filter((row) => row.state === 'running').length
    const ok = rows.filter((row) => row.state === 'exited' && row.code === 0).length
    const failed = rows.length - running - ok
    return [ok && `${ok} succeeded`, failed && `${failed} failed`, running && `${running} running`].filter(Boolean).join(' · ')
  }
  const isOpen = (row: RunRow, total: number) =>
    row.open ?? (total === 1 || (row.state !== 'running' && row.code !== 0))

  // Shell entry state.
  const [command, setCommand] = createSignal('/bin/sh')
  const [interactive, setInteractive] = createSignal(true)
  const openShell = () => {
    const argv = command().trim().split(/\s+/).filter(Boolean)
    const id = oneTarget()
    if (argv.length === 0 || !id || !mayExec()) return
    runKey += 1
    setTerminal({
      owner: SHELL_ENTRY,
      key: runKey,
      start: { allocationId: BigInt(id), taskName: props.taskName, command: argv, tty: interactive() },
    })
  }

  const listEntryClass = (active: boolean) =>
    cx(
      'flex w-full flex-col items-start gap-0.5 rounded-md border px-3 py-2.5 text-left transition-colors',
      active ? 'border-brass/45 bg-brass/10' : 'border-transparent hover:bg-surface2',
    )

  const targetPicker = (allowFanOut: boolean) => (
    <fieldset class="flex flex-col gap-2">
      <legend class="eyebrow mb-2">Run on</legend>
      <div class="grid gap-2 sm:grid-cols-3">
        <For
          each={[
            { id: 'one' as const, label: 'One instance', sub: oneTarget() ? `#${oneTarget()}` : 'none running', disabled: false },
            { id: 'all' as const, label: 'All running', sub: `${props.targets.length} instances`, disabled: !allowFanOut },
            {
              id: 'unhealthy' as const,
              label: 'Unhealthy only',
              sub: unhealthyTargets().length ? unhealthyTargets().map((target) => `#${target.id}`).join(' ') : 'none unhealthy',
              disabled: !allowFanOut || unhealthyTargets().length === 0,
            },
          ]}
        >
          {(option) => (
            <label
              class={cx(
                'flex min-h-[44px] cursor-pointer items-start gap-2.5 rounded-lg border px-3 py-2',
                effectiveMode() === option.id ? 'border-brass/50 bg-brass/5' : 'border-line bg-surface',
                option.disabled && 'cursor-not-allowed opacity-45',
              )}
            >
              <input
                type="radio"
                name="action-target"
                class="mt-1 accent-[rgb(var(--c-brass))]"
                checked={effectiveMode() === option.id}
                disabled={option.disabled}
                onChange={() => setMode(option.id)}
              />
              <span class="flex flex-col">
                <span class="text-sm font-medium text-ink">{option.label}</span>
                <span class="font-mono text-xs text-faint">{option.sub}</span>
              </span>
            </label>
          )}
        </For>
      </div>
      <div class="flex flex-wrap items-center gap-1.5">
        <span class="mr-1 text-xs text-faint">Instance</span>
        <For each={props.targets}>
          {(target) => (
            <button
              type="button"
              class={cx(
                'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 font-mono text-xs transition-colors',
                effectiveMode() === 'one' && oneTarget() === target.id
                  ? 'border-brass/50 bg-brass/15 font-semibold text-brass-strong'
                  : 'border-line text-muted hover:border-line-strong',
              )}
              aria-pressed={effectiveMode() === 'one' && oneTarget() === target.id}
              title={target.nodeId ? `on ${target.nodeId}` : undefined}
              onClick={() => {
                setChosenOne(target.id)
                setMode('one')
              }}
            >
              <Dot tone={target.healthy === false ? 'danger' : target.healthy ? 'ok' : 'neutral'} />#{target.id}
            </button>
          )}
        </For>
      </div>
    </fieldset>
  )

  const terminalFor = (owner: string) => (
    <Show when={terminal()?.owner === owner ? terminal() : null} keyed>
      {(session) => <ExecTerminal start={session.start} interactive={session.start.tty ?? false} />}
    </Show>
  )

  return (
    <div class="flex flex-wrap items-start gap-4">
      <nav aria-label="Actions" class="min-w-0 flex-[1_1_280px] overflow-hidden rounded-lg border border-line bg-surface shadow-card lg:max-w-[360px]">
        <Show when={props.actions.length > 3}>
          <div class="border-b border-line p-2.5">
            <input
              class={inputClass}
              placeholder="Filter actions…"
              aria-label="Filter actions"
              value={filter()}
              onInput={(event) => setFilter(event.currentTarget.value)}
            />
          </div>
        </Show>
        <div class="flex flex-col gap-0.5 p-1.5">
          <For
            each={visibleActions()}
            fallback={
              <p class="px-3 py-4 text-sm text-muted">
                {props.actions.length === 0 ? 'This task declares no actions.' : 'No action matches.'}
              </p>
            }
          >
            {(action) => (
              <button
                type="button"
                class={listEntryClass(props.selected === action.name)}
                aria-current={props.selected === action.name ? 'true' : undefined}
                onClick={() => props.onSelect(action.name)}
              >
                <span class="flex items-center gap-2">
                  <span class="font-mono text-sm font-semibold text-ink">{action.name}</span>
                  <Show when={action.tty}>
                    <Badge tone="neutral">tty</Badge>
                  </Show>
                </span>
                <Show when={action.description}>
                  <span class="text-[13px] text-muted">{action.description}</span>
                </Show>
                <span class="font-mono text-2xs text-faint">
                  {action.params.length === 0
                    ? action.tty
                      ? 'interactive · no parameters'
                      : 'no parameters'
                    : action.params.map((param) => (param.sensitive ? `${param.name} (secret)` : param.name)).join(' · ')}
                </span>
              </button>
            )}
          </For>
        </div>
        <div class="border-t border-line p-1.5">
          <button
            type="button"
            class={cx(listEntryClass(props.selected === SHELL_ENTRY), props.selected !== SHELL_ENTRY && 'border-dashed !border-line-strong')}
            aria-current={props.selected === SHELL_ENTRY ? 'true' : undefined}
            onClick={() => props.onSelect(SHELL_ENTRY)}
          >
            <span class="flex items-center gap-2 text-sm font-semibold text-ink">
              <Icon name="terminal" size={14} /> Open a shell
            </span>
            <span class="text-xs text-faint">
              {mayExec() ? 'A command or /bin/sh in one instance' : 'Your token cannot exec into this job'}
            </span>
          </button>
        </div>
      </nav>

      <section class="min-w-0 flex-[999_1_560px] overflow-hidden rounded-lg border border-line bg-surface shadow-card">
        <Show
          when={props.targets.length > 0}
          fallback={<EmptyState icon="terminal" title="No running allocations" body="Once an instance is running you can run actions or open a shell here." />}
        >
          <Show when={props.selected === SHELL_ENTRY}>
            <header class="border-b border-line px-5 py-4">
              <h3 class="font-display text-lg font-semibold text-ink">Open a shell</h3>
              <p class="mt-0.5 text-sm text-muted">Runs a command inside one instance of this task. Sessions are audited.</p>
            </header>
            <div class="flex flex-col gap-5 px-5 py-4">
              {targetPicker(false)}
              <div class="flex flex-wrap items-center gap-2">
                <input
                  class={cx(inputClass, 'flex-1 font-mono')}
                  value={command()}
                  aria-label="Command"
                  placeholder="/bin/sh"
                  onInput={(event) => setCommand(event.currentTarget.value)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') openShell()
                  }}
                />
                <label class="flex items-center gap-1.5 text-xs text-muted">
                  <input
                    type="checkbox"
                    class="form-checkbox h-3.5 w-3.5 rounded border-line-strong bg-surface text-brass focus:ring-brass/50"
                    checked={interactive()}
                    onChange={(event) => setInteractive(event.currentTarget.checked)}
                  />
                  Interactive (tty)
                </label>
                <Button
                  variant="primary"
                  icon="terminal"
                  disabled={!mayExec() || !oneTarget()}
                  title={mayExec() ? undefined : 'Your token cannot exec into allocations of this job'}
                  onClick={openShell}
                >
                  Open in #{oneTarget() ?? '?'}
                </Button>
              </div>
              {terminalFor(SHELL_ENTRY)}
            </div>
          </Show>

          <Show when={props.selected !== SHELL_ENTRY}>
            <Show when={current()} fallback={<EmptyState icon="zap" title="Pick an action" body="Choose an action from the list to fill in its parameters and run it." />}>
              {(action) => (
                <>
                  <header class="border-b border-line px-5 py-4">
                    <div class="flex flex-wrap items-center gap-2">
                      <h3 class="font-mono text-lg font-semibold text-ink">{action().name}</h3>
                      <Show when={action().tty}>
                        <Badge tone="neutral">interactive · tty</Badge>
                      </Show>
                      <Show when={currentSpec()?.timeout}>
                        <Badge tone="neutral">timeout {currentSpec()!.timeout}</Badge>
                      </Show>
                    </div>
                    <Show when={action().description}>
                      <p class="mt-1 text-sm text-muted">{action().description}</p>
                    </Show>
                  </header>

                  <div class="flex flex-col gap-5 px-5 pb-5 pt-1">
                    <div>
                      <Show when={action().params.length === 0}>
                        <p class="pt-3 text-sm text-faint">No parameters.</p>
                      </Show>
                      <Index each={action().params}>
                        {(param) => {
                          const value = () => valueOf(action(), param())
                          const error = () => paramError(param(), value())
                          const set = (next: string) => setValue(action().name, param().name, next)
                          const badge = () =>
                            param().required ? 'required' : param().default !== '' ? `default ${param().default}` : 'optional'
                          return (
                            <div class="flex flex-wrap gap-x-4 gap-y-1.5 border-b border-line py-3 last:border-b-0">
                              <div class="flex flex-[0_0_190px] flex-col gap-0.5">
                                <span class="flex flex-wrap items-center gap-1.5">
                                  <span class="font-mono text-sm font-semibold text-ink">{param().name}</span>
                                  <Badge tone={param().required ? 'warn' : 'neutral'}>{badge()}</Badge>
                                </span>
                                <span class="text-xs text-faint">{typeLabel(param())}</span>
                              </div>
                              <div class="flex min-w-0 flex-[1_1_260px] flex-col items-start gap-1.5">
                                <Show when={param().type === 'enum' && param().values.length <= SEGMENTED_MAX}>
                                  <div role="radiogroup" aria-label={param().name} class="inline-flex flex-wrap overflow-hidden rounded-md border border-line">
                                    <For each={param().values}>
                                      {(option) => (
                                        <button
                                          type="button"
                                          role="radio"
                                          aria-checked={value() === option}
                                          class={cx(
                                            'min-h-[32px] border-r border-line px-3 font-mono text-[12.5px] last:border-r-0',
                                            value() === option ? 'bg-brass/15 font-semibold text-brass-strong' : 'text-muted hover:bg-surface2',
                                          )}
                                          onClick={() => set(option)}
                                        >
                                          {option}
                                        </button>
                                      )}
                                    </For>
                                  </div>
                                </Show>
                                <Show when={param().type === 'enum' && param().values.length > SEGMENTED_MAX}>
                                  <div role="radiogroup" aria-label={param().name} class="flex flex-wrap gap-1.5">
                                    <For each={param().values}>
                                      {(option) => (
                                        <button
                                          type="button"
                                          role="radio"
                                          aria-checked={value() === option}
                                          class={cx(
                                            'min-h-[32px] rounded-full border px-3 font-mono text-[12.5px]',
                                            value() === option
                                              ? 'border-brass/50 bg-brass/15 font-semibold text-brass-strong'
                                              : 'border-line text-muted hover:border-line-strong',
                                          )}
                                          onClick={() => set(option)}
                                        >
                                          {option}
                                        </button>
                                      )}
                                    </For>
                                  </div>
                                </Show>
                                <Show when={param().type === 'bool'}>
                                  <button
                                    type="button"
                                    role="switch"
                                    aria-checked={value() === 'true'}
                                    aria-label={param().name}
                                    class="inline-flex min-h-[32px] items-center gap-2 text-sm text-muted"
                                    onClick={() => set(value() === 'true' ? 'false' : 'true')}
                                  >
                                    <span class={cx('relative inline-block h-5 w-9 rounded-full transition-colors', value() === 'true' ? 'bg-brass' : 'bg-line-strong')}>
                                      <span class={cx('absolute top-0.5 h-4 w-4 rounded-full bg-white transition-[left]', value() === 'true' ? 'left-[18px]' : 'left-0.5')} />
                                    </span>
                                    <span class="font-mono">{value()}</span>
                                  </button>
                                </Show>
                                <Show when={param().type !== 'enum' && param().type !== 'bool'}>
                                  <input
                                    class={cx(inputClass, 'max-w-[360px] font-mono', error() && error() !== 'Required' && '!border-red-500/70')}
                                    type={param().sensitive ? 'password' : 'text'}
                                    inputMode={param().type === 'int' ? 'numeric' : undefined}
                                    autocomplete={param().sensitive ? 'off' : undefined}
                                    aria-label={param().name}
                                    aria-invalid={Boolean(error()) || undefined}
                                    placeholder={param().sensitive ? 'secret' : undefined}
                                    value={value()}
                                    onInput={(event) => set(event.currentTarget.value)}
                                  />
                                </Show>
                                <Show when={param().description}>
                                  <span class="text-xs text-faint">{param().description}</span>
                                </Show>
                                <Show when={param().sensitive}>
                                  <span class="text-xs text-faint">Redacted in audit events and the CLI echo.</span>
                                </Show>
                                <Show when={error()}>
                                  <span class={cx('text-xs', error() === 'Required' ? 'text-amber-600 dark:text-amber-400' : 'text-red-600 dark:text-red-400')}>
                                    {error()}
                                  </span>
                                </Show>
                              </div>
                            </div>
                          )
                        }}
                      </Index>
                    </div>

                    <Show when={argvPreview()}>
                      {(argv) => (
                        <div class="flex flex-col gap-1.5">
                          <div class="flex flex-wrap items-center justify-between gap-2">
                            <p class="eyebrow">Runs</p>
                            <span class="text-xs text-faint">From the jobspec, rendered by the manager · no shell</span>
                          </div>
                          <div class="flex flex-wrap items-center gap-x-1.5 gap-y-1 rounded-md border border-line bg-surface2 px-3 py-2.5 font-mono text-[12.5px]">
                            <For each={argv()}>
                              {(part) => (
                                <span class={part.isParam ? 'rounded bg-brass/15 px-1 font-semibold text-brass-strong' : 'text-muted'}>{part.text}</span>
                              )}
                            </For>
                          </div>
                        </div>
                      )}
                    </Show>

                    {targetPicker(!action().tty)}
                    <Show when={action().tty}>
                      <p class="-mt-3 text-xs text-faint">Interactive actions open a terminal, so they run in one instance.</p>
                    </Show>

                    <div class="flex flex-col gap-2">
                      <div class="flex flex-wrap items-center gap-3">
                        <Button
                          variant="primary"
                          size="md"
                          icon={action().tty ? 'terminal' : 'play'}
                          disabled={errors().length > 0 || !mayRun(action().name) || targetIds().length === 0}
                          title={mayRun(action().name) ? undefined : 'Your token cannot run this action'}
                          onClick={run}
                        >
                          {runLabel()}
                        </Button>
                        <Show when={errors().length > 0}>
                          <span class="text-xs text-amber-600 dark:text-amber-400">
                            {errors().length === 1 ? '1 field needs attention' : `${errors().length} fields need attention`}
                          </span>
                        </Show>
                      </div>
                      <code class="overflow-x-auto whitespace-nowrap font-mono text-xs text-faint">$ {cliLine()}</code>
                    </div>

                    <Show when={action().tty}>{terminalFor(action().name)}</Show>

                    <Show when={!action().tty && currentRun()}>
                      {(record) => (
                        <div class="overflow-hidden rounded-lg border border-line">
                          <div class="flex flex-wrap items-center justify-between gap-2 bg-surface2/60 px-3.5 py-2">
                            <p class="eyebrow">Result</p>
                            <span class="text-xs text-muted">{runSummary()}</span>
                          </div>
                          <For each={record().order}>
                            {(id) => {
                              const row = () => record().rows[id]
                              const open = () => isOpen(row(), record().order.length)
                              const lastLine = () => row().lines[row().lines.length - 1]?.text ?? row().message ?? ''
                              return (
                                <div class="border-t border-line">
                                  <div class="flex flex-wrap items-center gap-x-3 gap-y-1 px-3.5 py-2 text-sm">
                                    <Show
                                      when={row().state !== 'running'}
                                      fallback={<Spinner label="running" class="text-xs" />}
                                    >
                                      <Badge tone={row().state === 'exited' && row().code === 0 ? 'ok' : 'danger'}>
                                        {row().state === 'error' ? 'error' : `exit ${row().code}`}
                                      </Badge>
                                    </Show>
                                    <span class="font-mono font-semibold text-ink">#{id}</span>
                                    <Show when={row().nodeId}>
                                      <span class="text-muted">{row().nodeId}</span>
                                    </Show>
                                    <Show when={row().endedAt}>
                                      <span class="text-xs text-faint">{formatDuration(row().endedAt! - row().startedAt)}</span>
                                    </Show>
                                    <span class="flex-1" />
                                    <span class="max-w-[320px] truncate text-xs text-faint" title={lastLine()}>{lastLine()}</span>
                                    <Button
                                      variant="ghost"
                                      size="xs"
                                      onClick={() => setRuns(props.selected, 'rows', id, 'open', !open())}
                                    >
                                      {open() ? 'Hide' : 'Output'}
                                    </Button>
                                  </div>
                                  <Show when={open()}>
                                    <pre class="mx-3.5 mb-3 max-h-72 overflow-auto rounded-md bg-[#0d0c0a] px-3 py-2 font-mono text-xs leading-relaxed text-[#e6e0d2]">
                                      <For each={row().lines} fallback={<span class="text-[#8c8472]">{row().state === 'running' ? 'waiting for output…' : 'no output'}</span>}>
                                        {(line) => <div class={line.stream === 'stderr' ? 'text-[#f2a296]' : undefined}>{line.text}</div>}
                                      </For>
                                      <Show when={row().state !== 'running'}>
                                        <div class="text-[#8c8472]">
                                          [{row().state === 'error' ? 'error' : `exited ${row().code}`}
                                          {row().message ? `: ${row().message}` : ''}]
                                        </div>
                                      </Show>
                                    </pre>
                                  </Show>
                                </div>
                              )
                            }}
                          </For>
                        </div>
                      )}
                    </Show>
                  </div>
                </>
              )}
            </Show>
          </Show>
        </Show>
      </section>
    </div>
  )
}
