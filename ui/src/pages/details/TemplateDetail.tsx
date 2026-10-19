import { A, useNavigate, useParams } from '@solidjs/router'
import { Code, ConnectError } from '@connectrpc/connect'
import { AccessGate } from '../../components/AccessGate'
import {
  For,
  Index,
  Match,
  Show,
  Switch,
  createEffect,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
  untrack,
  type JSX,
} from 'solid-js'
import {
  Badge,
  Button,
  CodeBlock,
  ConfirmButton,
  Field,
  Icon,
  MonoBadge,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  Spinner,
  StatusBadge,
  Tabs,
  checkboxClass,
  cx,
  inputClass,
  linkClass,
  selectClass,
  tableClass,
  tdClass,
  textareaClass,
  thClass,
  theadClass,
} from '../../components/ui'
import type {
  GetJobInfoReply,
  GetJobSpecReply,
  JobTemplate,
  JobTemplateVariable,
  RolloutGroupStatus,
  Submission,
} from '../../gen/legion_pb'
import { managerClient } from '../../grpcClient'
import { allows, canWriteAny } from '../../utils/acl'
import { canonicalJson, diffLines, type DiffLine, type LineDiff } from '../../utils/diff'
import { formatDate } from '../../utils/format'
import { retainOnError, usePoll } from '../../utils/polling'
import { dotToneClasses, isRolloutDone, statusTone } from '../../utils/status'
import { isMultiNamespace, jobPath, readableNamespaces } from '../../utils/namespaces'

// A ConnectError's `message` carries a `[code]` prefix; the raw message is
// what the manager actually said, which is what belongs in a form error.
const errorText = (error: unknown): string => {
  if (error instanceof ConnectError) return error.rawMessage || error.message
  return error instanceof Error ? error.message : String(error)
}

const hasCode = (error: unknown, code: Code): boolean => error instanceof ConnectError && error.code === code

const hasOwn = (record: Record<string, unknown>, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(record, key)

const parseDefault = (variable: JobTemplateVariable): unknown => {
  if (variable.defaultJson === undefined) return undefined
  try {
    return JSON.parse(variable.defaultJson)
  } catch {
    return undefined
  }
}

const parseObject = (text: string): Record<string, unknown> | undefined => {
  try {
    const parsed: unknown = JSON.parse(text)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : undefined
  } catch {
    return undefined
  }
}

/**
 * Form state is a map of variable key -> current value in a shape convenient
 * for inputs (strings for text-ish types, booleans, string arrays). It is
 * converted to the typed variables object right before render/run.
 */
type FormValues = Record<string, unknown>

const seedValues = (variables: readonly JobTemplateVariable[]): FormValues => {
  const seeded: FormValues = {}
  for (const variable of variables) {
    const fallback = parseDefault(variable)
    if (fallback !== undefined) {
      seeded[variable.key] = variable.type === 'json' ? JSON.stringify(fallback, null, 2) : fallback
    } else if (variable.type === 'multi_select') {
      seeded[variable.key] = []
    } else if (variable.type === 'boolean') {
      seeded[variable.key] = false
    } else {
      seeded[variable.key] = ''
    }
  }
  return seeded
}

/** Builds the request's variables object; throws on unparseable json vars. */
const toVariablesObject = (
  variables: readonly JobTemplateVariable[],
  values: FormValues,
): Record<string, unknown> => {
  const out: Record<string, unknown> = {}
  for (const variable of variables) {
    const value = values[variable.key]
    switch (variable.type) {
      case 'number': {
        if (value === '' || value === undefined) break
        const parsed = Number(value)
        if (Number.isNaN(parsed)) throw new Error(`variable '${variable.key}' expects a number`)
        out[variable.key] = parsed
        break
      }
      case 'boolean':
        out[variable.key] = Boolean(value)
        break
      case 'multi_select':
        if (Array.isArray(value) && value.length > 0) out[variable.key] = value
        break
      case 'json': {
        const text = typeof value === 'string' ? value.trim() : ''
        if (!text) break
        try {
          out[variable.key] = JSON.parse(text)
        } catch (error) {
          throw new Error(`variable '${variable.key}' is not valid JSON: ${errorText(error)}`)
        }
        break
      }
      default: {
        const text = typeof value === 'string' ? value : String(value ?? '')
        if (text !== '') out[variable.key] = text
      }
    }
  }
  return out
}

const variableSummary = (variables: readonly JobTemplateVariable[]): string => {
  if (variables.length === 0) return 'No variables'
  const required = variables.filter((variable) => variable.required).length
  return `${variables.length} value${variables.length === 1 ? '' : 's'} · ${required} required`
}

const formatValue = (value: unknown): string => {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'string') return value === '' ? '""' : value
  if (Array.isArray(value) && value.every((entry) => typeof entry === 'string')) {
    return value.length > 0 ? value.join(', ') : '[]'
  }
  return JSON.stringify(value)
}

const truncate = (text: string, max: number): string => (text.length > max ? `${text.slice(0, max - 1)}…` : text)

// ---------------------------------------------------------------------------
// What a template run records (RunJobTemplate in src/manager/server.rs)
// ---------------------------------------------------------------------------

/** The resolved variable values (defaults merged in) of a template run. */
const VARIABLES_SUBMISSION = 'template.variables.json'
/** The rendered jobspec JSON, recorded with every submission. */
const RENDERED_SUBMISSION = 'job.rendered.json'

/**
 * The job's current spec as a JSON object. Submissions carry the rendered
 * spec as `job.rendered.json`; a config without one may still hold the spec
 * as a plain JSON submission (a `job run` of a .json file). A Nickel-only
 * config has nothing comparable.
 */
const currentSpecFrom = (submissions: readonly Submission[]): Record<string, unknown> | undefined => {
  const rendered = submissions.find((submission) => submission.path === RENDERED_SUBMISSION)
  const parsed = rendered ? parseObject(rendered.content) : undefined
  if (parsed) return parsed
  for (const submission of submissions) {
    if (submission.path === VARIABLES_SUBMISSION) continue
    const candidate = parseObject(submission.content)
    if (candidate && Array.isArray(candidate.tasks)) return candidate
  }
  return undefined
}

// ---------------------------------------------------------------------------
// Copy as CLI
// ---------------------------------------------------------------------------

// POSIX single-quoting: safe for any value, newlines included (text vars).
const shellQuote = (value: string): string =>
  /^[A-Za-z0-9_@%+=:,./-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`

/**
 * The `legion template run` equivalent of a launch (src/cmd/template.rs).
 * Strings go through `--var` verbatim; everything else (numbers, booleans,
 * lists, JSON) through `--var-json`, so the value arrives with its type
 * rather than relying on the manager's string coercion. The version is
 * pinned: the command reproduces this launch even after a newer version is
 * published. Keys are identifiers (the manager enforces it), so only values
 * need quoting.
 */
const cliCommand = (templateName: string, version: number, sent: Record<string, unknown>): string => {
  const parts = [`legion template run ${shellQuote(templateName)} --version ${version}`]
  for (const [key, value] of Object.entries(sent)) {
    parts.push(
      typeof value === 'string'
        ? `--var ${key}=${shellQuote(value)}`
        : `--var-json ${key}=${shellQuote(JSON.stringify(value))}`,
    )
  }
  return parts.join(' \\\n  ')
}

// ---------------------------------------------------------------------------
// Review model
// ---------------------------------------------------------------------------

type Step = 1 | 2 | 3

type JobTarget =
  | { kind: 'new' }
  | { kind: 'existing'; status: string; provenance?: { name: string; version: number } }
  | { kind: 'unknown'; reason: string }

type ValueRow = {
  key: string
  label: string
  value: unknown
  // An empty field is left out of the request and the manager fills in the
  // declared default (or null) — the row shows what it will actually use.
  origin: 'form' | 'default' | 'unset'
  change?: { kind: 'changed'; was: unknown } | { kind: 'new' }
}

type SpecComparison = { kind: 'diff'; diff: LineDiff } | { kind: 'none'; reason: string }

type Review = {
  version: number
  variablesJson: string
  sent: Record<string, unknown>
  jobName: string
  renderedSpec: string
  target: JobTarget
  rows: ValueRow[]
  comparison: SpecComparison
}

type LaunchResult = {
  message: string
  jobName: string
  jobConfigId: string
  command: string
}

const buildRows = (
  variables: readonly JobTemplateVariable[],
  sent: Record<string, unknown>,
  previous: Record<string, unknown> | undefined,
): ValueRow[] =>
  variables.map((variable) => {
    const fallback = parseDefault(variable)
    const origin: ValueRow['origin'] = hasOwn(sent, variable.key) ? 'form' : fallback !== undefined ? 'default' : 'unset'
    const value = origin === 'form' ? sent[variable.key] : origin === 'default' ? fallback : null
    let change: ValueRow['change']
    if (previous) {
      if (!hasOwn(previous, variable.key)) {
        change = { kind: 'new' }
      } else if (canonicalJson(previous[variable.key] ?? null, 0) !== canonicalJson(value, 0)) {
        change = { kind: 'changed', was: previous[variable.key] }
      }
    }
    return { key: variable.key, label: variable.label || variable.key, value, origin, change }
  })

const buildReview = (input: {
  template: JobTemplate
  sent: Record<string, unknown>
  variablesJson: string
  jobName: string
  renderedJobspec: string
  info: PromiseSettledResult<GetJobInfoReply>
  spec: PromiseSettledResult<GetJobSpecReply>
}): Review => {
  const { info, spec, jobName } = input
  const renderedObject = parseObject(input.renderedJobspec)
  // Sorted keys on both sides of the diff, so key order is never a change.
  const renderedSpec = renderedObject ? canonicalJson(renderedObject) : input.renderedJobspec

  let target: JobTarget
  let previous: Record<string, unknown> | undefined
  let comparison: SpecComparison
  if (info.status === 'fulfilled') {
    const provenance = info.value.template
    target = {
      kind: 'existing',
      status: info.value.job?.status ?? '',
      provenance: provenance ? { name: provenance.name, version: provenance.version } : undefined,
    }
    if (spec.status === 'fulfilled') {
      // Previous values only mean the same thing when the current config
      // came from this template: another template's keys could share a
      // name and mean something else entirely.
      if (provenance?.name === input.template.name) {
        const recorded = spec.value.submissions.find((submission) => submission.path === VARIABLES_SUBMISSION)
        previous = recorded ? parseObject(recorded.content) : undefined
      }
      const current = currentSpecFrom(spec.value.submissions)
      comparison =
        current && renderedObject
          ? { kind: 'diff', diff: diffLines(canonicalJson(current), renderedSpec) }
          : { kind: 'none', reason: 'Its current config has no JSON spec recorded to compare against, so this is the full spec it will be replaced with.' }
    } else {
      comparison = {
        kind: 'none',
        reason: `Could not load its current spec (${errorText(spec.reason)}), so this is the full spec it will be replaced with.`,
      }
    }
  } else if (hasCode(info.reason, Code.NotFound)) {
    target = { kind: 'new' }
    comparison = { kind: 'none', reason: 'Nothing to compare against yet: this is the full spec the job will be created with.' }
  } else {
    const denied = hasCode(info.reason, Code.PermissionDenied) || hasCode(info.reason, Code.Unauthenticated)
    target = {
      kind: 'unknown',
      reason: denied
        ? 'Your token cannot read this job, so the review cannot tell whether it already exists or what would change.'
        : `Could not look up the job: ${errorText(info.reason)}`,
    }
    comparison = { kind: 'none', reason: 'Without the current job there is nothing to compare against: this is the full rendered spec.' }
  }

  return {
    version: input.template.version,
    variablesJson: input.variablesJson,
    sent: input.sent,
    jobName,
    renderedSpec,
    target,
    rows: buildRows(input.template.variables, input.sent, previous),
    comparison,
  }
}

// ---------------------------------------------------------------------------
// Pieces
// ---------------------------------------------------------------------------

const VariableInput = (props: {
  variable: JobTemplateVariable
  value: unknown
  onChange: (value: unknown) => void
}) => {
  const variable = props.variable
  switch (variable.type) {
    case 'text':
      return (
        <textarea
          class={`${textareaClass} h-24`}
          value={typeof props.value === 'string' ? props.value : ''}
          onInput={(event) => props.onChange(event.currentTarget.value)}
        />
      )
    case 'number':
      return (
        <input
          type="number"
          class={inputClass}
          value={props.value === undefined ? '' : String(props.value)}
          onInput={(event) => props.onChange(event.currentTarget.value)}
        />
      )
    case 'boolean':
      return (
        <label class="inline-flex items-center gap-2 text-sm text-ink">
          <input
            type="checkbox"
            class={checkboxClass}
            checked={Boolean(props.value)}
            onChange={(event) => props.onChange(event.currentTarget.checked)}
          />
          <span>Enabled</span>
        </label>
      )
    case 'select':
      return (
        <select
          class={`${selectClass} w-full`}
          value={typeof props.value === 'string' ? props.value : ''}
          onChange={(event) => props.onChange(event.currentTarget.value)}
        >
          <option value="" disabled>
            Choose…
          </option>
          <For each={variable.options}>{(option) => <option value={option}>{option}</option>}</For>
        </select>
      )
    case 'multi_select': {
      const selected = () => (Array.isArray(props.value) ? (props.value as string[]) : [])
      const toggle = (option: string, on: boolean) => {
        const next = selected().filter((entry) => entry !== option)
        if (on) next.push(option)
        props.onChange(next)
      }
      return (
        <div class="flex flex-wrap gap-2">
          <For each={variable.options}>
            {(option) => (
              <label class="inline-flex cursor-pointer items-center gap-1.5 rounded-full border border-line px-3 py-1 text-xs text-ink transition-colors has-[:checked]:border-brass has-[:checked]:bg-brass/10">
                <input
                  type="checkbox"
                  class="sr-only"
                  checked={selected().includes(option)}
                  onChange={(event) => toggle(option, event.currentTarget.checked)}
                />
                <span>{option}</span>
              </label>
            )}
          </For>
        </div>
      )
    }
    case 'json':
      return (
        <textarea
          class={`${textareaClass} h-28 font-mono text-xs`}
          value={typeof props.value === 'string' ? props.value : ''}
          onInput={(event) => props.onChange(event.currentTarget.value)}
          spellcheck={false}
        />
      )
    default:
      return (
        <input
          class={inputClass}
          value={typeof props.value === 'string' ? props.value : ''}
          onInput={(event) => props.onChange(event.currentTarget.value)}
        />
      )
  }
}

type StepDef = { label: string; sub: string }

/**
 * Numbered progress for the launch flow. Completed steps are buttons back to
 * that step; the current step is marked with `aria-current="step"`; steps
 * ahead are disabled, because each one needs the previous one's result.
 */
function Stepper(props: { steps: readonly StepDef[]; current: number; onSelect: (index: number) => void; locked?: boolean }) {
  return (
    <ol aria-label="Launch steps" class="grid gap-2.5 sm:grid-cols-3">
      <Index each={props.steps}>
        {(step, index) => {
          const state = () => (index < props.current ? 'done' : index === props.current ? 'current' : 'todo')
          return (
            <li>
              <button
                type="button"
                aria-current={state() === 'current' ? 'step' : undefined}
                disabled={state() === 'todo' || (state() === 'done' && props.locked)}
                onClick={() => {
                  if (state() === 'done') props.onSelect(index)
                }}
                class={cx(
                  'flex min-h-[56px] w-full items-center gap-3 rounded-lg border bg-surface px-3.5 py-2.5 text-left transition-colors',
                  state() === 'current' && 'border-brass ring-[3px] ring-brass/15',
                  state() === 'done' && 'border-brass/40 hover:border-brass hover:bg-surface2/60 disabled:cursor-not-allowed',
                  state() === 'todo' && 'cursor-default border-line',
                )}
              >
                <span
                  class={cx(
                    'inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-xs font-semibold',
                    state() === 'current' && 'bg-brass text-canvas',
                    state() === 'done' && 'bg-brass/15 text-brass-strong',
                    state() === 'todo' && 'bg-surface2 text-muted',
                  )}
                >
                  <Show when={state() === 'done'} fallback={index + 1}>
                    <Icon name="check" size={14} strokeWidth={2.25} />
                    <span class="sr-only">Completed:</span>
                  </Show>
                </span>
                <span class="min-w-0">
                  <span class={cx('block text-sm font-semibold', state() === 'todo' ? 'text-muted' : 'text-ink')}>{step().label}</span>
                  <span class="block truncate text-xs text-faint">{step().sub}</span>
                </span>
              </button>
            </li>
          )
        }}
      </Index>
    </ol>
  )
}

function StepFooter(props: { error?: string; children: JSX.Element }) {
  return (
    <div class="space-y-3">
      <Show when={props.error}>
        <Note tone="danger">
          <span class="whitespace-pre-line">{props.error}</span>
        </Note>
      </Show>
      <div class="flex flex-wrap items-center justify-between gap-3 border-t border-line pt-4">{props.children}</div>
    </div>
  )
}

const diffLineClass: Record<DiffLine['kind'], string> = {
  add: 'bg-emerald-500/10 text-emerald-800 dark:text-emerald-300',
  remove: 'bg-red-500/10 text-red-800 dark:text-red-300',
  context: 'text-muted',
}
const diffSign: Record<DiffLine['kind'], string> = { add: '+', remove: '-', context: ' ' }

function DiffView(props: { diff: Extract<LineDiff, { kind: 'ok' }> }) {
  return (
    <div
      class="max-h-[28rem] overflow-auto rounded-md border border-line bg-surface font-mono text-xs leading-relaxed"
      role="region"
      aria-label="Jobspec diff"
      tabindex="0"
    >
      {/* min-w-full + w-max: each line's tint spans the widest line. */}
      <div class="w-max min-w-full py-1">
        <For each={props.diff.hunks}>
          {(hunk) => (
            <>
              <div class="bg-surface2 px-3 py-0.5 text-faint">{hunk.header}</div>
              <For each={hunk.lines}>
                {(line) => (
                  <div class={cx('whitespace-pre px-3', diffLineClass[line.kind])}>
                    {diffSign[line.kind]} {line.text}
                  </div>
                )}
              </For>
            </>
          )}
        </For>
      </div>
    </div>
  )
}

function CopyCommandButton(props: { command: string }) {
  const [copied, setCopied] = createSignal(false)
  const [failed, setFailed] = createSignal(false)
  let timer: ReturnType<typeof setTimeout> | undefined
  onCleanup(() => clearTimeout(timer))
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(props.command)
      setCopied(true)
      clearTimeout(timer)
      timer = setTimeout(() => setCopied(false), 1500)
    } catch {
      // No clipboard outside a secure context: show the command instead so
      // it can still be selected by hand.
      setFailed(true)
    }
  }
  return (
    <>
      <Button variant="ghost" size="md" icon={copied() ? 'check' : 'terminal'} onClick={() => void copy()}>
        {copied() ? 'Copied' : 'Copy as CLI'}
      </Button>
      <Show when={failed()}>
        <CodeBlock code={props.command} label="equivalent CLI command" class="basis-full" />
      </Show>
    </>
  )
}

const groupProgress = (group: RolloutGroupStatus, rolloutStatus: string) => {
  // step_index counts completed steps; a completed rollout shows full even
  // if its runtime state stopped short of the last index.
  const complete = rolloutStatus.toLowerCase() === 'completed'
  const total = group.totalSteps
  const doneSteps = complete ? total : Math.min(group.stepIndex, total)
  const fraction = total > 0 ? doneSteps / total : complete ? 1 : 0
  const fresh = group.allocations.filter((allocation) => allocation.role === 'replacement')
  const old = group.allocations.filter((allocation) => allocation.role === 'replacee').length
  const healthy = fresh.filter((allocation) => allocation.allocationHealthy === true).length
  return { doneSteps, total, fraction, fresh: fresh.length, healthy, old }
}

/**
 * Follows the rollout a launch started, at the live cadence, until it is
 * terminal. Mounted only while the Launch step is on screen, so leaving the
 * step (or the page) ends the poll with it.
 */
function RolloutProgress(props: { jobName: string; jobConfigId: string }) {
  const tick = usePoll()
  // A falsy source stops the resource refetching, which ends the poll.
  const [settled, setSettled] = createSignal(false)
  const [data] = createResource(
    () => (settled() ? false : ([props.jobName, props.jobConfigId, tick()] as const)),
    retainOnError(
      async ([jobId, configId]: readonly [string, string, number]) => {
        // Newest few rather than just the newest: the rollout to follow is
        // the one targeting this launch's config, and one someone started a
        // moment later must not be mistaken for it.
        const list = await managerClient.listRollouts({
          jobId,
          activeOnly: false,
          sortBy: 'created_at',
          sortOrder: 'desc',
          page: 1,
          pageSize: 5,
        })
        const match = list.rollouts.find((rollout) => rollout.toJobConfigId === configId)
        if (!match) return null
        const status = await managerClient.getRolloutStatus({ id: match.id })
        return { rollout: status.rollout ?? match, groups: status.groups }
      },
      ([jobId, configId]) => `${jobId}/${configId}`,
    ),
  )
  const denied = () => hasCode(data.error, Code.PermissionDenied) || hasCode(data.error, Code.Unauthenticated)
  createEffect(() => {
    // Check the error first: reading an errored resource throws.
    if (data.error) {
      if (denied()) setSettled(true)
      return
    }
    const rollout = data()?.rollout
    if (rollout && isRolloutDone(rollout.status)) setSettled(true)
  })

  return (
    <SectionCard title="Rollout">
      <Show
        when={!data.error}
        fallback={
          <Note tone={denied() ? 'neutral' : 'danger'}>
            {denied()
              ? 'Your token cannot read this job’s rollouts; follow progress on the job page.'
              : `Could not load rollout progress: ${errorText(data.error)}`}
          </Note>
        }
      >
        <Show when={data()} fallback={<Spinner label="Waiting for the rollout to start…" />}>
          {(found) => (
            <div class="space-y-4">
              <div class="flex flex-wrap items-center justify-between gap-2">
                <p class="text-sm text-ink">
                  Rollout{' '}
                  <A href={`/rollouts/${found().rollout.id}`} class={cx(linkClass, 'font-mono')}>
                    #{String(found().rollout.id)}
                  </A>
                  <Show when={found().rollout.message}>
                    <span class="ml-2 text-xs text-faint">{found().rollout.message}</span>
                  </Show>
                </p>
                <StatusBadge status={found().rollout.status} />
              </div>
              <Show when={found().rollout.status.toLowerCase().startsWith('waiting_manual')}>
                <Note tone="info">This rollout advances manually. Continue it from the rollout page.</Note>
              </Show>
              <Show
                when={found().groups.length > 0}
                fallback={<p class="text-sm text-muted">No task groups reported yet.</p>}
              >
                <ul class="space-y-3">
                  <Index each={found().groups}>
                    {(group) => {
                      const progress = () => groupProgress(group(), found().rollout.status)
                      return (
                        <li class="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)_auto] items-center gap-x-3 gap-y-0.5 text-sm">
                          <span class="truncate font-mono text-xs text-ink" title={group().group}>
                            {group().group}
                          </span>
                          <span
                            class="block h-1.5 overflow-hidden rounded-full bg-surface2"
                            role="progressbar"
                            aria-label={`${group().group} rollout steps`}
                            aria-valuemin={0}
                            aria-valuemax={progress().total}
                            aria-valuenow={progress().doneSteps}
                          >
                            <span
                              class={cx('block h-full rounded-full transition-[width]', dotToneClasses[statusTone(found().rollout.status)])}
                              style={{ width: `${Math.round(progress().fraction * 100)}%` }}
                            />
                          </span>
                          <span class="text-xs tabular-nums text-faint">
                            {progress().doneSteps}/{progress().total} steps
                          </span>
                          <span class="col-start-2 text-2xs text-faint">
                            {progress().fresh} new ({progress().healthy} healthy) · {progress().old} old
                          </span>
                        </li>
                      )
                    }}
                  </Index>
                </ul>
              </Show>
            </div>
          )}
        </Show>
      </Show>
    </SectionCard>
  )
}

/** The callout at the top of the review: create, update, or can't tell. */
function TargetCallout(props: { review: Review; templateName: string }) {
  const comparison = props.review.comparison
  const unchanged = comparison.kind === 'diff' && comparison.diff.kind === 'ok' && comparison.diff.hunks.length === 0
  const jobName = <span class="font-mono">{props.review.jobName}</span>
  const target = props.review.target
  switch (target.kind) {
    case 'new':
      return (
        <Note tone="ok">
          <p class="font-display text-base font-semibold">Creates a new job {jobName}</p>
          <p class="mt-0.5">No job by that name exists yet. Launching submits it and the scheduler places it.</p>
        </Note>
      )
    case 'existing': {
      const provenance = target.provenance
      const fromThisTemplate = provenance?.name === props.templateName
      // The manager's derived status (running, pending, complete, ...).
      const state = target.status || 'existing'
      return (
        <Note tone="warn">
          <p class="font-display text-base font-semibold">
            Updates the {state} job {jobName}
          </p>
          <p class="mt-0.5">
            <Show when={provenance} fallback="It was submitted directly, not from a template. ">
              {(from) => (
                <>
                  It was launched from{' '}
                  <Show when={!fromThisTemplate} fallback="this template">
                    template{' '}
                    <A href={`/templates/${encodeURIComponent(from().name)}`} class={cx(linkClass, 'font-mono')}>
                      {from().name}
                    </A>
                  </Show>{' '}
                  <span class="font-mono">v{from().version}</span>.{' '}
                </>
              )}
            </Show>
            {unchanged
              ? 'The rendered spec matches its current config, so launching changes nothing.'
              : 'Launching replaces its config and starts a rollout.'}
          </p>
          <Show when={!fromThisTemplate}>
            <p class="mt-1 text-xs">
              Its current config did not come from this template, so the manager accepts the update only if your
              token also holds plain job:write on it.
            </p>
          </Show>
        </Note>
      )
    }
    default:
      return (
        <Note tone="warn">
          <p class="font-display text-base font-semibold">Launches job {jobName}</p>
          <p class="mt-0.5">{target.reason}</p>
        </Note>
      )
  }
}

function ReviewValues(props: { review: Review }) {
  return (
    <Show
      when={props.review.rows.length > 0}
      fallback={<p class="p-4 text-sm text-muted">This template takes no variables.</p>}
    >
      <div class="overflow-x-auto">
        <table class={tableClass}>
          <thead class={theadClass}>
            <tr>
              <th class={thClass}>Variable</th>
              <th class={thClass}>Value</th>
              <th class={thClass}>
                <span class="sr-only">Change</span>
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-line">
            <For each={props.review.rows}>
              {(row) => (
                <tr>
                  <td class={cx(tdClass, 'align-top')}>
                    <span class="block">{row.label}</span>
                    <Show when={row.label !== row.key}>
                      <span class="block font-mono text-2xs text-faint">{row.key}</span>
                    </Show>
                  </td>
                  <td class={cx(tdClass, 'align-top')}>
                    <span
                      class={cx(
                        'block max-h-24 overflow-auto whitespace-pre-wrap break-words font-mono text-xs',
                        row.origin === 'unset' && 'text-faint',
                      )}
                    >
                      {formatValue(row.value)}
                    </span>
                    <Show when={row.origin !== 'form'}>
                      <span class="text-2xs text-faint">{row.origin === 'default' ? 'default' : 'not set'}</span>
                    </Show>
                  </td>
                  <td class={cx(tdClass, 'whitespace-nowrap text-right align-top')}>
                    <Show when={row.change}>
                      {(change) => {
                        const current = change()
                        return current.kind === 'new' ? (
                          <Badge tone="info" title="Not set on the job's current config">
                            new
                          </Badge>
                        ) : (
                          <Badge tone="warn" title={`Currently ${formatValue(current.was)}`}>
                            was {truncate(formatValue(current.was), 32)}
                          </Badge>
                        )
                      }}
                    </Show>
                  </td>
                </tr>
              )}
            </For>
          </tbody>
        </table>
      </div>
    </Show>
  )
}

function ReviewSpec(props: { review: Review }) {
  const [showFull, setShowFull] = createSignal(false)
  const comparison = props.review.comparison
  const diff = comparison.kind === 'diff' ? comparison.diff : undefined
  const okDiff = diff?.kind === 'ok' ? diff : undefined
  return (
    <SectionCard
      title={comparison.kind === 'diff' ? 'Jobspec changes' : 'Rendered jobspec'}
      description={comparison.kind === 'diff' ? 'The job’s current config against what this launch renders.' : undefined}
      actions={
        <Show when={diff}>
          <Show when={okDiff}>
            {(ok) => (
              <span class="text-xs tabular-nums text-muted">
                <Show when={ok().hunks.length > 0} fallback="No changes">
                  <span class="text-emerald-700 dark:text-emerald-300">+{ok().added}</span>{' '}
                  <span class="text-red-700 dark:text-red-300">−{ok().removed}</span> lines
                </Show>
              </span>
            )}
          </Show>
          <Button variant="ghost" size="xs" icon="eye" aria-expanded={showFull()} onClick={() => setShowFull((open) => !open)}>
            {showFull() ? 'Hide full rendered spec' : 'Show full rendered spec'}
          </Button>
        </Show>
      }
    >
      <div class="space-y-3">
        <Switch>
          <Match when={comparison.kind === 'none' && comparison}>
            {(none) => (
              <>
                <p class="text-xs text-muted">{none().reason}</p>
                <CodeBlock code={props.review.renderedSpec} maxHClass="max-h-[28rem]" label="rendered jobspec" />
              </>
            )}
          </Match>
          <Match when={diff?.kind === 'too-large' && diff}>
            {(large) => (
              <Note>
                The specs are too large to diff in the browser ({large().oldLines} and {large().newLines} lines). Show the
                full rendered spec to read what will be submitted.
              </Note>
            )}
          </Match>
          <Match when={okDiff && okDiff.hunks.length === 0}>
            <Note>
              No changes to the rendered spec. Launching resubmits the same config; the manager reports it unchanged
              and starts no rollout.
            </Note>
          </Match>
          <Match when={okDiff}>{(ok) => <DiffView diff={ok()} />}</Match>
        </Switch>
        <Show when={diff && showFull()}>
          <CodeBlock code={props.review.renderedSpec} maxHClass="max-h-[28rem]" label="rendered jobspec" />
        </Show>
      </div>
    </SectionCard>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

function TemplateDetailContent() {
  const params = useParams()
  const navigate = useNavigate()
  const templateName = () => decodeURIComponent(params.name)
  const [version, setVersion] = createSignal<number | undefined>(undefined)
  const [tab, setTab] = createSignal<'launch' | 'source'>('launch')

  const [data, { refetch }] = createResource(
    () => ({ name: templateName(), version: version() }),
    async (input) => managerClient.getJobTemplate({ name: input.name, version: input.version }),
  )
  const template = createMemo(() => data()?.template)

  const [values, setValues] = createSignal<FormValues>({})
  createEffect(() => {
    const current = template()
    if (current) setValues(seedValues(current.variables))
  })
  const setValue = (key: string, value: unknown) =>
    setValues((previous) => ({ ...previous, [key]: value }))

  // Guided launch: Variables -> Review -> Launch.
  const [step, setStep] = createSignal<Step>(1)
  const [review, setReview] = createSignal<Review | null>(null)
  const [reviewing, setReviewing] = createSignal(false)
  const [launching, setLaunching] = createSignal(false)
  const [launched, setLaunched] = createSignal<LaunchResult | null>(null)
  // The namespace the rendered job lands in; empty lets the template decide
  // (its own `namespace`, else `default`). A template that names one and a
  // different choice here is refused by the manager, not silently resolved.
  const [targetNamespace, setTargetNamespace] = createSignal('')
  const [flowError, setFlowError] = createSignal('')
  // Bumped whenever the inputs change, so a render that resolves after the
  // user has moved on is dropped instead of overwriting newer state.
  let reviewToken = 0

  // Editing values (or switching version, which reseeds them) invalidates
  // the review: it described a render of different inputs. A finished
  // launch stays on screen — it happened, whatever the form says now.
  createEffect(() => {
    values()
    untrack(() => {
      reviewToken += 1
      setReviewing(false)
      setReview(null)
      setFlowError('')
      if (step() === 2) setStep(1)
    })
  })

  const startReview = async () => {
    const current = template()
    if (!current) return
    reviewToken += 1
    const token = reviewToken
    setReviewing(true)
    setFlowError('')
    try {
      const sent = toVariablesObject(current.variables, values())
      const variablesJson = JSON.stringify(sent)
      const reply = await managerClient.renderJobTemplate({
        name: current.name,
        version: current.version,
        variablesJson,
        namespace: targetNamespace() || undefined,
      })
      if (token !== reviewToken) return
      // Both lookups at once: an existing job needs its spec anyway, and for
      // a new one the spec's NotFound is simply ignored.
      const [info, spec] = await Promise.allSettled([
        managerClient.getJobInfo({ name: reply.jobName }),
        managerClient.getJobSpec({ name: reply.jobName }),
      ])
      if (token !== reviewToken) return
      setReview(
        buildReview({
          template: current,
          sent,
          variablesJson,
          jobName: reply.jobName,
          renderedJobspec: reply.renderedJobspec,
          info,
          spec,
        }),
      )
      setStep(2)
    } catch (error) {
      if (token !== reviewToken) return
      setFlowError(errorText(error))
      setStep(1)
    } finally {
      if (token === reviewToken) setReviewing(false)
    }
  }

  const launch = async () => {
    const current = template()
    const reviewed = review()
    if (!current || !reviewed) return
    setLaunching(true)
    setFlowError('')
    setLaunched(null)
    setStep(3)
    try {
      // Exactly the inputs that were reviewed: any edit since would have
      // cleared the review and sent the user back to step 1.
      const reply = await managerClient.runJobTemplate({
        name: current.name,
        version: reviewed.version,
        variablesJson: reviewed.variablesJson,
        forceReschedule: false,
        namespace: targetNamespace() || undefined,
      })
      setLaunched({
        message: reply.message,
        jobName: reply.jobName,
        jobConfigId: reply.jobConfigId,
        command: cliCommand(current.name, reviewed.version, reviewed.sent),
      })
    } catch (error) {
      setFlowError(errorText(error))
      // Back to the review that was launched — or to the form, if a version
      // switch on the Source tab invalidated it meanwhile.
      setStep(review() ? 2 : 1)
    } finally {
      setLaunching(false)
    }
  }

  const goToStep = (target: Step) => {
    if (target >= step() || launching() || reviewing()) return
    if (target === 2) {
      // Only reachable from Launch: the old review predates the launch, so
      // re-render it against the job as it is now.
      void startReview()
      return
    }
    // Leaving Launch: the review described the job before this launch, so
    // it no longer says what the next one would change.
    if (step() === 3) setReview(null)
    setStep(target)
  }

  const steps = (): StepDef[] => {
    const current = template()
    const reviewed = review()
    const result = launched()
    let reviewSub = 'What will change'
    if (reviewing()) reviewSub = 'Rendering…'
    else if (reviewed) {
      reviewSub =
        reviewed.target.kind === 'new'
          ? `Creates ${reviewed.jobName}`
          : reviewed.target.kind === 'existing'
            ? `Updates ${reviewed.jobName}`
            : reviewed.jobName
    }
    let launchSub = 'Submit and watch'
    if (launching()) launchSub = 'Submitting…'
    else if (result && step() === 3) launchSub = `Submitted ${result.jobName}`
    return [
      { label: 'Variables', sub: current ? variableSummary(current.variables) : '' },
      { label: 'Review', sub: reviewSub },
      { label: 'Launch', sub: launchSub },
    ]
  }

  // New-version editor, seeded with the currently shown source.
  const [editorOpen, setEditorOpen] = createSignal(false)
  const [editorSource, setEditorSource] = createSignal('')
  const [publishing, setPublishing] = createSignal(false)
  const [publishError, setPublishError] = createSignal('')
  createEffect(() => {
    const current = template()
    if (current && !editorOpen()) setEditorSource(current.sourceNickel)
  })
  const openEditor = () => {
    setTab('source')
    setEditorOpen(true)
  }

  const publishVersion = async (event: Event) => {
    event.preventDefault()
    setPublishing(true)
    setPublishError('')
    try {
      await managerClient.upsertJobTemplate({
        name: templateName(),
        sourceNickel: editorSource(),
      })
      setEditorOpen(false)
      setVersion(undefined)
      await refetch()
    } catch (error) {
      setPublishError(errorText(error))
    } finally {
      setPublishing(false)
    }
  }

  const [deleteError, setDeleteError] = createSignal('')
  const deleteTemplate = async () => {
    setDeleteError('')
    try {
      await managerClient.deleteJobTemplate({ name: templateName() })
      navigate('/templates')
    } catch (error) {
      setDeleteError(errorText(error))
    }
  }

  const mayWrite = () => allows('template', 'write', { template: templateName() })
  // Submission needs job:write; the exact rendered name is only known after
  // rendering, so gate the buttons on the wildcard check and let the server
  // enforce the precise scope. Rendering the review needs the same access.
  const maySubmit = () => canWriteAny('job')

  return (
    <div class="space-y-6">
      <Show when={template()} fallback={data.loading ? <Spinner label="Loading template…" /> : <Note tone="danger">Failed to load template: {data.error ? errorText(data.error) : 'not found'}</Note>}>
        {(current) => (
          <>
            <PageHeader
              eyebrow="Template"
              title={current().name}
              badges={
                <>
                  <MonoBadge tone="accent">v{current().version}</MonoBadge>
                  <Show when={current().version !== current().latestVersion}>
                    <Badge tone="warn">latest is v{current().latestVersion}</Badge>
                  </Show>
                </>
              }
              subtitle={current().description || 'No description.'}
              actions={
                <Show when={mayWrite()}>
                  <div class="flex items-center gap-2">
                    <Button variant="subtle" size="sm" icon="plus" onClick={openEditor}>
                      New version
                    </Button>
                    <ConfirmButton label="Delete" size="sm" icon="trash" onConfirm={deleteTemplate} />
                  </div>
                </Show>
              }
            />
            <Show when={deleteError()}>
              <Note tone="danger">{deleteError()}</Note>
            </Show>

            <div>
              <Tabs
                tabs={[
                  { id: 'launch', label: 'Launch' },
                  { id: 'source', label: 'Source', count: data()?.versions.length },
                ]}
                active={tab()}
                onChange={(id) => setTab(id === 'source' ? 'source' : 'launch')}
              />

              <div class="pt-5">
                <Show
                  when={tab() === 'launch'}
                  fallback={
                    <div class="space-y-5">
                      <Show when={editorOpen()}>
                        <SectionCard
                          title="Publish new version"
                          description={`Stored as v${current().latestVersion + 1}; existing versions are immutable.`}
                          actions={
                            <Button variant="ghost" size="xs" onClick={() => setEditorOpen(false)}>
                              Cancel
                            </Button>
                          }
                        >
                          <form class="flex flex-col gap-3" onSubmit={publishVersion}>
                            <textarea
                              class={`${textareaClass} h-72 font-mono text-xs`}
                              value={editorSource()}
                              onInput={(event) => setEditorSource(event.currentTarget.value)}
                              spellcheck={false}
                            />
                            <div class="flex items-center gap-3">
                              <Button type="submit" variant="primary" size="md" disabled={publishing()}>
                                {publishing() ? 'Publishing…' : 'Publish'}
                              </Button>
                              <Show when={publishError()}>
                                <Note tone="danger" class="flex-1">{publishError()}</Note>
                              </Show>
                            </div>
                          </form>
                        </SectionCard>
                      </Show>

                      <div class="grid items-start gap-5 xl:grid-cols-[minmax(0,3fr)_minmax(0,1fr)]">
                        <SectionCard
                          title="Source"
                          description={`v${current().version}, created ${formatDate(current().createdAt)}${current().createdBy ? ` by ${current().createdBy}` : ''}.`}
                        >
                          <CodeBlock code={current().sourceNickel} maxHClass="max-h-[36rem]" label="template source" />
                        </SectionCard>

                        <SectionCard title="Versions" flush>
                          <ul class="divide-y divide-line">
                            <For each={data()?.versions ?? []}>
                              {(info, index) => {
                                const selected = () => info.version === current().version
                                return (
                                  <li>
                                    <button
                                      type="button"
                                      aria-current={selected() ? 'true' : undefined}
                                      // Picking the newest means "latest", like the
                                      // Launch tab's selector, so a publish moves it.
                                      onClick={() => setVersion(index() === 0 ? undefined : info.version)}
                                      class={cx(
                                        'flex w-full flex-col gap-0.5 px-4 py-2.5 text-left transition-colors',
                                        selected() ? 'bg-brass/10' : 'hover:bg-surface2/60',
                                      )}
                                    >
                                      <span class="flex items-center gap-2">
                                        <MonoBadge tone={selected() ? 'accent' : 'neutral'}>v{info.version}</MonoBadge>
                                        <Show when={index() === 0}>
                                          <Badge tone="ok">latest</Badge>
                                        </Show>
                                      </span>
                                      <span class="text-xs text-faint">
                                        {formatDate(info.createdAt)}
                                        {info.createdBy ? ` · ${info.createdBy}` : ''}
                                      </span>
                                    </button>
                                  </li>
                                )
                              }}
                            </For>
                          </ul>
                        </SectionCard>
                      </div>
                    </div>
                  }
                >
                  <div class="space-y-5">
                    <Stepper
                      steps={steps()}
                      current={step() - 1}
                      onSelect={(index) => goToStep((index + 1) as Step)}
                      locked={launching() || reviewing()}
                    />

                    <Switch>
                      <Match when={step() === 1}>
                        <form
                          class="space-y-5"
                          onSubmit={(event) => {
                            event.preventDefault()
                            void startReview()
                          }}
                        >
                          <div class="grid items-start gap-5 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
                            <SectionCard
                              title="Variables"
                              description="Validated against the declared types when the manager renders the template."
                              actions={
                                <>
                                  <Show when={isMultiNamespace()}>
                                    <select
                                      class={selectClass}
                                      aria-label="Target namespace"
                                      value={targetNamespace()}
                                      onChange={(event) => setTargetNamespace(event.currentTarget.value)}
                                    >
                                      <option value="">Namespace: as the template says</option>
                                      <For each={readableNamespaces()}>{(namespace) => <option value={namespace}>Namespace: {namespace}</option>}</For>
                                    </select>
                                  </Show>
                                  <Show when={(data()?.versions.length ?? 0) > 1}>
                                    <select
                                      class={selectClass}
                                      aria-label="Template version"
                                      value={version() === undefined ? 'latest' : String(version())}
                                      onChange={(event) => {
                                        const raw = event.currentTarget.value
                                        setVersion(raw === 'latest' ? undefined : Number(raw))
                                      }}
                                    >
                                      <option value="latest">Latest (v{data()?.versions[0]?.version ?? current().latestVersion})</option>
                                      <For each={data()?.versions ?? []}>
                                        {(info) => <option value={String(info.version)}>v{info.version} · {formatDate(info.createdAt)}</option>}
                                      </For>
                                    </select>
                                  </Show>
                                </>
                              }
                            >
                              <Show
                                when={current().variables.length > 0}
                                fallback={<p class="text-sm text-muted">This template takes no variables.</p>}
                              >
                                <div class="grid gap-4 md:grid-cols-2">
                                  <For each={current().variables}>
                                    {(variable) => (
                                      <Field
                                        label={
                                          <>
                                            {variable.label || variable.key}
                                            <Show when={variable.required}>
                                              <span class="ml-1 text-red-500 dark:text-red-400" title="Required">*</span>
                                            </Show>
                                          </>
                                        }
                                        hint={variable.description}
                                        class={variable.type === 'text' || variable.type === 'json' ? 'md:col-span-2' : undefined}
                                      >
                                        <VariableInput
                                          variable={variable}
                                          value={values()[variable.key]}
                                          onChange={(value) => setValue(variable.key, value)}
                                        />
                                      </Field>
                                    )}
                                  </For>
                                </div>
                              </Show>
                            </SectionCard>

                            <aside>
                              <SectionCard title="About this template">
                                <div class="space-y-3 text-sm">
                                  <p class="text-ink">{current().description || 'No description.'}</p>
                                  <p class="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-faint">
                                    <MonoBadge>v{current().version}</MonoBadge>
                                    <Show when={current().createdBy}>
                                      <span>by {current().createdBy}</span>
                                    </Show>
                                    <RelativeTime value={current().createdAt} />
                                  </p>
                                  <p class="text-xs text-muted">{variableSummary(current().variables)}</p>
                                  <Show when={current().version !== current().latestVersion}>
                                    <Note tone="warn">
                                      This is v{current().version}, not the latest (v{current().latestVersion}). Launching uses
                                      the version selected here.
                                    </Note>
                                  </Show>
                                </div>
                              </SectionCard>
                            </aside>
                          </div>

                          <StepFooter error={flowError()}>
                            <p class="text-xs text-faint">
                              {maySubmit()
                                ? 'Nothing is submitted until the last step.'
                                : 'Your token can browse this template but cannot submit jobs, so it cannot render a review.'}
                            </p>
                            <Button
                              type="submit"
                              variant="primary"
                              size="md"
                              disabled={reviewing() || !maySubmit()}
                              title={maySubmit() ? undefined : 'Your token cannot submit jobs'}
                            >
                              {reviewing() ? 'Rendering…' : 'Next: review'}
                            </Button>
                          </StepFooter>
                        </form>
                      </Match>

                      <Match when={step() === 2 ? review() : null} keyed>
                        {(reviewed) => (
                          <div class="space-y-5">
                            <TargetCallout review={reviewed} templateName={current().name} />
                            <div class="grid items-start gap-5 xl:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]">
                              <SectionCard
                                title="Your values"
                                description="What this launch sends; empty fields fall back to their defaults."
                                flush
                                actions={
                                  <Button variant="ghost" size="xs" onClick={() => setStep(1)}>
                                    Edit
                                  </Button>
                                }
                              >
                                <ReviewValues review={reviewed} />
                              </SectionCard>
                              <ReviewSpec review={reviewed} />
                            </div>
                            <StepFooter error={flowError()}>
                              <Button variant="subtle" size="md" icon="chevronLeft" onClick={() => setStep(1)}>
                                Back
                              </Button>
                              <Button
                                variant="primary"
                                size="md"
                                icon="play"
                                disabled={launching() || !maySubmit()}
                                title={maySubmit() ? undefined : 'Your token cannot submit jobs'}
                                onClick={() => void launch()}
                              >
                                {reviewed.target.kind === 'existing' ? 'Update job' : 'Launch job'}
                              </Button>
                            </StepFooter>
                          </div>
                        )}
                      </Match>

                      <Match when={step() === 3}>
                        <Show
                          when={launched()}
                          fallback={
                            <SectionCard>
                              <Spinner label="Submitting…" />
                            </SectionCard>
                          }
                        >
                          {(result) => {
                            // The reply's first line is the outcome; warnings
                            // follow on their own lines. Only an update starts
                            // a rollout — a new job is placed directly and an
                            // unchanged spec starts nothing.
                            const outcome = () => result().message.split('\n')[0]
                            const expectsRollout = () => /rollout/i.test(outcome())
                            const unchanged = () => /unchanged/i.test(outcome())
                            const jobHref = () => `${jobPath(result().jobName)}`
                            return (
                              <div class="space-y-5">
                                <Note tone="ok">
                                  <p class="font-display text-base font-semibold">
                                    Submitted <span class="font-mono">{result().jobName}</span>
                                  </p>
                                  <p class="mt-0.5 whitespace-pre-line">{result().message}</p>
                                  <p class="mt-1 text-xs">
                                    Config <span class="font-mono">{result().jobConfigId}</span> ·{' '}
                                    <A href={jobHref()} class={linkClass}>
                                      View job {result().jobName}
                                    </A>
                                  </p>
                                </Note>
                                <Switch>
                                  <Match when={expectsRollout()}>
                                    <RolloutProgress jobName={result().jobName} jobConfigId={result().jobConfigId} />
                                  </Match>
                                  <Match when={unchanged()}>
                                    <Note>The rendered spec matched the job’s current config, so nothing was rolled out.</Note>
                                  </Match>
                                  <Match when={true}>
                                    <Note>
                                      New jobs are placed directly by the scheduler, without a rollout. Follow placement on
                                      the job page.
                                    </Note>
                                  </Match>
                                </Switch>
                                <StepFooter>
                                  <div class="flex flex-wrap items-center gap-2">
                                    <Button variant="primary" size="md" icon="externalLink" onClick={() => navigate(jobHref())}>
                                      Open job
                                    </Button>
                                    <Button variant="subtle" size="md" icon="refresh" onClick={() => goToStep(1)}>
                                      Launch another
                                    </Button>
                                    <CopyCommandButton command={result().command} />
                                  </div>
                                </StepFooter>
                              </div>
                            )
                          }}
                        </Show>
                      </Match>
                    </Switch>
                  </div>
                </Show>
              </div>
            </div>
          </>
        )}
      </Show>
    </div>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function TemplateDetail() {
  return (
    <AccessGate resource="template">
      <TemplateDetailContent />
    </AccessGate>
  )
}
