import { A } from '@solidjs/router'
import { For, Match, Show, Switch, createMemo, createSignal } from 'solid-js'
import type { AppActionInfo, AppViewContext } from '../../gen/legion_pb'
import { managerClient } from '../../grpcClient'
import {
  errorText,
  formatDuration,
  formatNumber,
  formatTime,
  isHref,
  isRoute,
  parseInputs,
  parseView,
  type ActionButton,
  type Block,
  type Cell,
  type InputDecl,
  type SeriesLine,
  type ViewDocument,
  type ViewTone,
} from '../../utils/appViews'
import { textToneClasses } from '../../utils/status'
import { Badge, Button, Note, TableShell, cx, inputClass, linkClass, selectClass, tableClass, tdClass, thClass, theadClass } from '../ui'
import { SafeMarkdown } from './SafeMarkdown'

/*
 * The block renderer for app views (`legion-view/1`; docs/apps.md, "The view
 * schema"). Every block is drawn by Legion's own components from data the
 * manager validated; nothing an app sends is interpreted as markup.
 */

const toneText = (tone?: ViewTone) => (tone && tone !== 'neutral' ? textToneClasses[tone] : 'text-ink')

function BlockTitle(props: { title?: string }) {
  return (
    <Show when={props.title}>
      <p class="eyebrow mb-2">{props.title}</p>
    </Show>
  )
}

function LinkedText(props: { text: string; route?: string; href?: string; tone?: ViewTone }) {
  return (
    <Switch fallback={<span class={toneText(props.tone)}>{props.text}</span>}>
      <Match when={isRoute(props.route)}>
        <A href={props.route!} class={linkClass}>
          {props.text}
        </A>
      </Match>
      <Match when={isHref(props.href)}>
        <a href={props.href!} target="_blank" rel="noopener noreferrer" class={linkClass}>
          {props.text}
        </a>
      </Match>
    </Switch>
  )
}

function StatsView(props: { block: Extract<Block, { type: 'stats' }> }) {
  return (
    <div>
      <BlockTitle title={props.block.title} />
      <div class="grid grid-cols-2 gap-2.5 sm:grid-cols-3 lg:grid-cols-4">
        <For each={props.block.items}>
          {(stat) => (
            <div class="rounded-md border border-line bg-surface2/40 px-3 py-2">
              <p class="eyebrow">{stat.label}</p>
              <p class={cx('mt-1 font-display text-lg font-semibold tabular-nums', toneText(stat.tone))}>{String(stat.value)}</p>
              <Show when={stat.hint}>
                <p class="mt-0.5 truncate text-2xs text-faint" title={stat.hint}>
                  {stat.hint}
                </p>
              </Show>
            </div>
          )}
        </For>
      </div>
    </div>
  )
}

const SERIES_COLORS = ['rgb(var(--c-brass))', '#0ea5e9', '#10b981', '#f43f5e']

function SeriesView(props: { block: Extract<Block, { type: 'series' }> }) {
  const width = 640
  const height = 180
  const pad = { left: 44, right: 44, top: 10, bottom: 22 }
  const lines = () => props.block.series.slice(0, 4)
  const span = createMemo(() => {
    const times = lines().flatMap((line) => line.points.map(([at]) => at))
    const min = times.length > 0 ? Math.min(...times) : 0
    const max = times.length > 0 ? Math.max(...times) : 1
    return { min, max: max > min ? max : min + 1 }
  })
  const range = (axis: 'left' | 'right') => {
    const values = lines()
      .filter((line) => (line.axis ?? 'left') === axis)
      .flatMap((line) => line.points.map(([, value]) => value))
    if (values.length === 0) return undefined
    let min = Math.min(...values, 0)
    let max = Math.max(...values)
    if (max <= min) max = min + 1
    if (min > 0) min = 0
    return { min, max }
  }
  const left = createMemo(() => range('left'))
  const right = createMemo(() => range('right'))
  const x = (at: number) => pad.left + ((at - span().min) / (span().max - span().min)) * (width - pad.left - pad.right)
  const y = (value: number, axis: 'left' | 'right') => {
    const scale = (axis === 'right' ? right() : left()) ?? { min: 0, max: 1 }
    return pad.top + (1 - (value - scale.min) / (scale.max - scale.min)) * (height - pad.top - pad.bottom)
  }
  const path = (line: SeriesLine) =>
    line.points
      .map(([at, value], index) => `${index === 0 ? 'M' : 'L'}${x(at).toFixed(1)},${y(value, line.axis ?? 'left').toFixed(1)}`)
      .join(' ')
  const unitOf = (axis: 'left' | 'right') => lines().find((line) => (line.axis ?? 'left') === axis)?.unit ?? ''
  return (
    <div>
      <BlockTitle title={props.block.title} />
      <svg viewBox={`0 0 ${width} ${height}`} class="h-44 w-full" role="img" aria-label={props.block.title ?? 'series'}>
        <line x1={pad.left} x2={width - pad.right} y1={height - pad.bottom} y2={height - pad.bottom} stroke="currentColor" class="text-line" />
        <Show when={left()}>
          {(scale) => (
            <>
              <text x={pad.left - 6} y={pad.top + 8} text-anchor="end" class="fill-current text-[10px] text-faint">
                {formatNumber(scale().max)}
                {unitOf('left')}
              </text>
              <text x={pad.left - 6} y={height - pad.bottom} text-anchor="end" class="fill-current text-[10px] text-faint">
                {formatNumber(scale().min)}
              </text>
            </>
          )}
        </Show>
        <Show when={right()}>
          {(scale) => (
            <>
              <text x={width - pad.right + 6} y={pad.top + 8} class="fill-current text-[10px] text-faint">
                {formatNumber(scale().max)}
                {unitOf('right')}
              </text>
              <text x={width - pad.right + 6} y={height - pad.bottom} class="fill-current text-[10px] text-faint">
                {formatNumber(scale().min)}
              </text>
            </>
          )}
        </Show>
        <text x={pad.left} y={height - 6} class="fill-current text-[10px] text-faint">
          {formatTime(span().min)}
        </text>
        <text x={width - pad.right} y={height - 6} text-anchor="end" class="fill-current text-[10px] text-faint">
          {formatTime(span().max)}
        </text>
        <For each={lines()}>
          {(line, index) => (
            <Show
              when={line.points.length > 1}
              fallback={
                <Show when={line.points.length === 1}>
                  <circle cx={x(line.points[0][0])} cy={y(line.points[0][1], line.axis ?? 'left')} r="3" fill={SERIES_COLORS[index()]} />
                </Show>
              }
            >
              <path d={path(line)} fill="none" stroke={SERIES_COLORS[index()]} stroke-width="1.75" stroke-dasharray={line.axis === 'right' ? '4 3' : undefined} />
            </Show>
          )}
        </For>
      </svg>
      <div class="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-2xs text-muted">
        <For each={lines()}>
          {(line, index) => {
            const last = () => line.points[line.points.length - 1]?.[1]
            return (
              <span class="inline-flex items-center gap-1.5">
                <span class="inline-block h-0.5 w-3" style={{ background: SERIES_COLORS[index()] }} />
                {line.name}
                <Show when={line.axis === 'right'}>
                  <span class="text-faint">(right)</span>
                </Show>
                <Show when={last() !== undefined}>
                  <span class="font-mono text-ink">
                    {formatNumber(last()!)}
                    {line.unit ?? ''}
                  </span>
                </Show>
              </span>
            )
          }}
        </For>
      </div>
    </div>
  )
}

function CellView(props: { cell?: Cell; type?: string }) {
  const cell = () => props.cell
  return (
    <Switch fallback={<span>{cell() === undefined ? '' : String(cell())}</span>}>
      <Match when={typeof cell() === 'object' && cell() !== null ? (cell() as Exclude<Cell, number | boolean | string>) : undefined}>
        {(rich) => <LinkedText text={String(rich().text)} route={rich().route} href={rich().href} tone={rich().tone} />}
      </Match>
      <Match when={typeof cell() === 'number' && props.type === 'time'}>
        <span class="whitespace-nowrap text-muted">{formatTime(cell() as number)}</span>
      </Match>
      <Match when={typeof cell() === 'number' && props.type === 'duration'}>
        <span class="tabular-nums">{formatDuration(cell() as number)}</span>
      </Match>
      <Match when={typeof cell() === 'number'}>
        <span class="tabular-nums">{formatNumber(cell() as number)}</span>
      </Match>
    </Switch>
  )
}

function TableView(props: { block: Extract<Block, { type: 'table' }> }) {
  return (
    <div>
      <BlockTitle title={props.block.title} />
      <Show when={props.block.rows.length > 0} fallback={<p class="text-sm text-muted">{props.block.empty ?? 'Nothing to show.'}</p>}>
        <TableShell class="rounded-md border border-line">
          <table class={tableClass}>
            <thead class={theadClass}>
              <tr>
                <For each={props.block.columns}>{(column) => <th class={thClass}>{column.label}</th>}</For>
              </tr>
            </thead>
            <tbody class="divide-y divide-line">
              <For each={props.block.rows}>
                {(row) => (
                  <tr>
                    <For each={props.block.columns}>
                      {(column) => (
                        <td class={cx(tdClass, column.type === 'number' && 'text-right')}>
                          <CellView cell={row[column.key]} type={column.type} />
                        </td>
                      )}
                    </For>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </TableShell>
      </Show>
    </div>
  )
}

function TimelineView(props: { block: Extract<Block, { type: 'timeline' }> }) {
  return (
    <div>
      <BlockTitle title={props.block.title} />
      <Show when={props.block.items.length > 0} fallback={<p class="text-sm text-muted">Nothing yet.</p>}>
        <ol class="space-y-2 border-l border-line pl-3">
          <For each={props.block.items}>
            {(item) => (
              <li>
                <p class={cx('text-sm font-medium', toneText(item.tone))}>{item.title}</p>
                <p class="text-2xs text-faint">{formatTime(item.at)}</p>
                <Show when={item.detail}>
                  <p class="mt-0.5 text-xs text-muted">{item.detail}</p>
                </Show>
              </li>
            )}
          </For>
        </ol>
      </Show>
    </div>
  )
}

function KeyValueView(props: { block: Extract<Block, { type: 'keyValue' }> }) {
  return (
    <div>
      <BlockTitle title={props.block.title} />
      <dl class="grid gap-x-8 gap-y-2 text-sm sm:grid-cols-2">
        <For each={props.block.items}>
          {(item) => (
            <div class="min-w-0">
              <dt class="text-xs text-faint">{item.key}</dt>
              <dd class="mt-0.5 break-words font-medium">
                <LinkedText text={String(item.value)} route={item.route} tone={item.tone} />
              </dd>
            </div>
          )}
        </For>
      </dl>
    </div>
  )
}

/** One input of an action's form. */
function InputField(props: { name: string; decl: InputDecl; value: unknown; onInput: (value: unknown) => void }) {
  const label = () => props.decl.label ?? props.name
  return (
    <label class="block">
      <span class="mb-1 block text-xs font-medium text-muted">{label()}</span>
      <Switch
        fallback={
          <input
            class={inputClass}
            type={props.decl.type === 'number' ? 'number' : 'text'}
            placeholder={props.decl.type === 'duration' ? '1h, 30m, 90s' : undefined}
            value={props.value === undefined ? '' : String(props.value)}
            onInput={(event) => {
              const raw = event.currentTarget.value
              props.onInput(props.decl.type === 'number' ? (raw === '' ? undefined : Number(raw)) : raw === '' ? undefined : raw)
            }}
          />
        }
      >
        <Match when={props.decl.type === 'boolean'}>
          <input type="checkbox" checked={props.value === true} onChange={(event) => props.onInput(event.currentTarget.checked)} />
        </Match>
        <Match when={props.decl.type === 'enum'}>
          <select class={selectClass} value={props.value === undefined ? '' : String(props.value)} onChange={(event) => props.onInput(event.currentTarget.value)}>
            <For each={props.decl.values ?? []}>{(choice) => <option value={choice}>{choice}</option>}</For>
          </select>
        </Match>
      </Switch>
    </label>
  )
}

/**
 * A button bound to a manifest-declared action. A `write` action always
 * asks first (the manager words the confirmation); inputs the button does
 * not preset are asked for in the same form. The manager checks the
 * viewer's permission again, records the action, and only then asks the app.
 */
function ActionButtonView(props: {
  app: string
  button: ActionButton
  info?: AppActionInfo
  context: Partial<AppViewContext>
  onDone: (message: string, viewJson: string) => void
}) {
  const decls = createMemo(() => parseInputs(props.info?.inputsJson ?? '{}'))
  const open = Object.keys(decls()).filter((name) => !(props.button.inputs && name in props.button.inputs))
  const [values, setValues] = createSignal<Record<string, unknown>>(
    Object.fromEntries(open.map((name) => [name, decls()[name]?.default])),
  )
  const [asking, setAsking] = createSignal(false)
  const [running, setRunning] = createSignal(false)
  const [failure, setFailure] = createSignal<string>()
  const label = () => props.button.label ?? props.info?.title ?? props.button.action
  const needsForm = () => Boolean(props.info?.confirm) || open.length > 0
  const run = async () => {
    setRunning(true)
    setFailure(undefined)
    try {
      const inputs = { ...values(), ...(props.button.inputs ?? {}) }
      const clean = Object.fromEntries(Object.entries(inputs).filter(([, value]) => value !== undefined))
      const reply = await managerClient.invokeAppAction({
        app: props.app,
        action: props.button.action,
        context: props.context,
        inputsJson: JSON.stringify(clean),
        idempotencyKey: typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : undefined,
      })
      setAsking(false)
      props.onDone(reply.message, reply.viewJson)
    } catch (error) {
      setFailure(errorText(error))
    } finally {
      setRunning(false)
    }
  }
  const allowed = () => props.info?.allowed === true
  const permission = () => (props.info?.permission ? `${props.info.permission.resource}:${props.info.permission.action}` : '')
  return (
    <div class="inline-flex flex-col gap-2">
      <Button
        variant={props.info?.mutating ? 'accent' : 'subtle'}
        size="sm"
        disabled={!allowed() || running()}
        title={allowed() ? props.info?.title : `Needs ${permission()} here`}
        onClick={() => (needsForm() ? setAsking(true) : void run())}
      >
        {label()}
      </Button>
      <Show when={asking()}>
        <div class="min-w-[16rem] space-y-2 rounded-md border border-line bg-surface2/60 p-3" role="dialog" aria-label={label()}>
          <Show when={props.info?.confirm}>
            <p class="text-sm text-ink">{props.info!.confirm}</p>
          </Show>
          <For each={open}>
            {(name) => (
              <InputField
                name={name}
                decl={decls()[name]}
                value={values()[name]}
                onInput={(value) => setValues((current) => ({ ...current, [name]: value }))}
              />
            )}
          </For>
          <Show when={failure()}>
            <Note tone="danger">{failure()!}</Note>
          </Show>
          <div class="flex gap-2">
            <Button variant={props.info?.mutating ? 'primary' : 'subtle'} size="xs" disabled={running()} onClick={() => void run()}>
              {running() ? 'Running…' : label()}
            </Button>
            <Button variant="ghost" size="xs" onClick={() => setAsking(false)}>
              Cancel
            </Button>
          </div>
        </div>
      </Show>
      <Show when={!asking() && failure()}>
        <Note tone="danger">{failure()!}</Note>
      </Show>
    </div>
  )
}

function ActionsView(props: {
  app: string
  block: Extract<Block, { type: 'actions' }>
  actions: AppActionInfo[]
  context: Partial<AppViewContext>
  onDone: (message: string, viewJson: string) => void
}) {
  return (
    <div class="flex flex-wrap items-start gap-2">
      <For each={props.block.items}>
        {(button) => (
          <ActionButtonView
            app={props.app}
            button={button}
            info={props.actions.find((action) => action.name === button.action)}
            context={props.context}
            onDone={props.onDone}
          />
        )}
      </For>
    </div>
  )
}

/** A whole view document: its blocks, in order. */
export function ViewBlocks(props: {
  app: string
  document: ViewDocument
  actions: AppActionInfo[]
  context: Partial<AppViewContext>
  onAction?: (message: string, viewJson: string) => void
}) {
  const [message, setMessage] = createSignal<string>()
  const [override, setOverride] = createSignal<ViewDocument>()
  const document = () => override() ?? props.document
  const done = (text: string, viewJson: string) => {
    setMessage(text)
    if (viewJson) setOverride(parseView(viewJson))
    props.onAction?.(text, viewJson)
  }
  return (
    <div class="space-y-4">
      <Show when={message()}>
        <Note tone="ok" onDismiss={() => setMessage(undefined)}>
          {message()!}
        </Note>
      </Show>
      <For each={document().blocks ?? []}>
        {(block) => (
          <Switch>
            <Match when={block.type === 'stats' ? block : undefined}>{(stats) => <StatsView block={stats()} />}</Match>
            <Match when={block.type === 'series' ? block : undefined}>{(series) => <SeriesView block={series()} />}</Match>
            <Match when={block.type === 'table' ? block : undefined}>{(table) => <TableView block={table()} />}</Match>
            <Match when={block.type === 'timeline' ? block : undefined}>{(timeline) => <TimelineView block={timeline()} />}</Match>
            <Match when={block.type === 'keyValue' ? block : undefined}>{(keyValue) => <KeyValueView block={keyValue()} />}</Match>
            <Match when={block.type === 'notice' ? block : undefined}>
              {(notice) => (
                <Note tone={notice().tone ?? 'neutral'}>
                  <Show when={notice().title}>
                    <span class="font-semibold">{notice().title}: </span>
                  </Show>
                  {notice().text}
                </Note>
              )}
            </Match>
            <Match when={block.type === 'markdown' ? block : undefined}>{(markdown) => <SafeMarkdown text={markdown().text} />}</Match>
            <Match when={block.type === 'actions' ? block : undefined}>
              {(actions) => <ActionsView app={props.app} block={actions()} actions={props.actions} context={props.context} onDone={done} />}
            </Match>
          </Switch>
        )}
      </For>
    </div>
  )
}

/** A task row's badge from a `badge` view. */
export function ViewBadge(props: { document: ViewDocument; app: string }) {
  return (
    <Show when={!props.document.hidden && props.document.badge}>
      {(badge) => (
        <Badge tone={badge().tone ?? 'info'} title={`From the ${props.app} app`}>
          {badge().text}
        </Badge>
      )}
    </Show>
  )
}
