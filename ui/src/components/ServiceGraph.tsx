import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal } from 'solid-js'
import type { ServiceEdge } from '../gen/legion_pb'
import { Badge, EmptyState, MonoBadge, TableShell, cx, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'

// The service graph (docs/network-handlers.md, "Service graph"): nodes are
// services and the jobs that call them, edges are what every node's datapath
// observed over the window, folded by the manager. Declared-but-silent
// edges (a `connect.upstreams` entry that never called) are drawn dashed so
// "configured" and "actually talking" read differently at a glance.

export const edgeKey = (edge: ServiceEdge) => `${edge.callerJob}/${edge.callerTask}→${edge.service}${edge.tag ? `#${edge.tag}` : ''}`

const fmtRate = (value: number) => (value >= 10 ? value.toFixed(0) : value >= 1 ? value.toFixed(1) : value.toFixed(2))
const fmtPct = (value: number) => `${(value * 100).toFixed(value >= 0.1 ? 0 : 1)}%`
const fmtMs = (value: number) => (value >= 1000 ? `${(value / 1000).toFixed(1)}s` : `${Math.round(value)}ms`)
const fmtBytes = (value: bigint | number) => {
  const n = Number(value)
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GiB`
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MiB`
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KiB`
  return `${n} B`
}
const ago = (unix: bigint | number) => {
  const seconds = Math.max(0, Math.floor(Date.now() / 1000) - Number(unix))
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3600)}h ago`
}

export const edgeTone = (edge: ServiceEdge): 'ok' | 'warn' | 'danger' | 'neutral' => {
  if (!edge.allowed) return 'danger'
  if (Number(edge.denied) > 0) return 'danger'
  if (Number(edge.requests) === 0) return 'neutral'
  if (edge.errorRate >= 0.05) return 'danger'
  if (edge.errorRate > 0) return 'warn'
  return 'ok'
}

const strokeFor = (tone: ReturnType<typeof edgeTone>) =>
  tone === 'danger' ? 'var(--color-danger, #c0392b)' : tone === 'warn' ? 'var(--color-warn, #d68910)' : tone === 'ok' ? 'var(--color-ok, #1e8449)' : 'var(--color-line, #999)'

type Node = { id: string; label: string; kind: 'service' | 'caller'; x: number; y: number }

// Callers on the left, services on the right, each column spread evenly.
// A service that is also a caller appears once, on the right, and its
// outgoing edges leave from there — the layout stays readable for the
// dozens of services a cluster has, which is the audience of this view.
export function ServiceGraphSvg(props: { edges: ServiceEdge[]; selected?: string; onSelect?: (key: string) => void }) {
  const layout = createMemo(() => {
    const services = new Set<string>()
    const callers = new Set<string>()
    for (const edge of props.edges) services.add(edge.service)
    for (const edge of props.edges) {
      const caller = edge.callerJob
      if (!services.has(caller)) callers.add(caller)
    }
    const width = 880
    const rowH = 34
    const rows = Math.max(services.size, callers.size, 1)
    const height = Math.max(120, rows * rowH + 40)
    const place = (ids: string[], x: number, kind: Node['kind']): Node[] => {
      const gap = ids.length > 1 ? (height - 40) / (ids.length - 1) : 0
      return ids.map((id, index) => ({ id, label: id, kind, x, y: ids.length > 1 ? 20 + index * gap : height / 2 }))
    }
    const callerNodes = place([...callers].sort(), 120, 'caller')
    const serviceNodes = place([...services].sort(), width - 120, 'service')
    const byId = new Map<string, Node>()
    for (const node of [...callerNodes, ...serviceNodes]) byId.set(node.id, node)
    return { width, height, nodes: [...callerNodes, ...serviceNodes], byId }
  })

  const maxRps = createMemo(() => Math.max(1, ...props.edges.map((edge) => edge.rps)))

  return (
    <div class="overflow-x-auto">
      {/* Drawn at its natural size and never scaled up: stretched to a wide
          card, the labels and nodes would grow with it (2×+ on a desktop). */}
      <svg
        viewBox={`0 0 ${layout().width} ${layout().height}`}
        class="mx-auto block w-full min-w-[560px]"
        style={{ 'max-width': `${layout().width}px`, 'max-height': '520px' }}
      >
        <defs>
          <marker id="graph-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" />
          </marker>
        </defs>
        <For each={props.edges}>
          {(edge) => {
            const from = () => layout().byId.get(edge.callerJob)
            const to = () => layout().byId.get(edge.service)
            const tone = () => edgeTone(edge)
            const key = edgeKey(edge)
            const selected = () => props.selected === key
            return (
              <Show when={from() && to()}>
                {(() => {
                  const f = from()!
                  const t = to()!
                  // A self-edge (a service calling itself, or a caller that is
                  // also a service) loops out to the right.
                  const same = f.id === t.id
                  const x1 = f.x + 70
                  const x2 = t.x - 70
                  const path = same
                    ? `M ${f.x + 70} ${f.y - 6} C ${f.x + 130} ${f.y - 40}, ${f.x + 130} ${f.y + 40}, ${f.x + 70} ${f.y + 6}`
                    : `M ${x1} ${f.y} C ${(x1 + x2) / 2} ${f.y}, ${(x1 + x2) / 2} ${t.y}, ${x2} ${t.y}`
                  const width = 1 + 4 * (edge.rps / maxRps())
                  return (
                    <g
                      class="cursor-pointer"
                      style={{ color: strokeFor(tone()) }}
                      onClick={() => props.onSelect?.(key)}
                    >
                      <title>
                        {key}: {fmtRate(edge.rps)} rps, {fmtPct(edge.errorRate)} errors, {String(edge.denied)} denied
                      </title>
                      <path
                        d={path}
                        fill="none"
                        stroke="currentColor"
                        stroke-width={selected() ? width + 2 : width}
                        stroke-dasharray={Number(edge.requests) === 0 ? '6 4' : undefined}
                        opacity={props.selected && !selected() ? 0.35 : 0.9}
                        marker-end="url(#graph-arrow)"
                      />
                    </g>
                  )
                })()}
              </Show>
            )
          }}
        </For>
        <For each={layout().nodes}>
          {(node) => (
            <g>
              <rect
                x={node.x - 70}
                y={node.y - 12}
                width={140}
                height={24}
                rx={6}
                class={cx('stroke-line', node.kind === 'service' ? 'fill-surface2' : 'fill-surface')}
                stroke-width={1}
              />
              <text x={node.x} y={node.y + 4} text-anchor="middle" class="fill-ink font-mono" font-size="11">
                {node.label.length > 20 ? `${node.label.slice(0, 19)}…` : node.label}
              </text>
            </g>
          )}
        </For>
      </svg>
    </div>
  )
}

export function ServiceEdgesTable(props: {
  edges: ServiceEdge[]
  selected?: string
  onSelect?: (key: string) => void
  hideService?: boolean
  emptyTitle?: string
  emptyBody?: string
}) {
  return (
    <Show
      when={props.edges.length > 0}
      fallback={
        <EmptyState
          icon="network"
          title={props.emptyTitle ?? 'No edges yet'}
          body={props.emptyBody ?? 'Edges appear within one heartbeat of the first call a task makes through an upstream.'}
        />
      }
    >
      <TableShell>
        <table class={tableClass}>
          <thead class={theadClass}>
            <tr>
              <th class={thClass}>Caller</th>
              <Show when={!props.hideService}>
                <th class={thClass}>Service</th>
              </Show>
              <th class={thClass}>Tag</th>
              <th class={thClass}>State</th>
              <th class={cx(thClass, 'text-right')}>rps</th>
              <th class={cx(thClass, 'text-right')}>errors</th>
              <th class={cx(thClass, 'text-right')}>denied</th>
              <th class={cx(thClass, 'text-right')}>p·avg / max</th>
              <th class={cx(thClass, 'text-right')}>bytes ⇅</th>
              <th class={thClass}>Last seen</th>
            </tr>
          </thead>
          <tbody>
            <For each={props.edges}>
              {(edge) => {
                const key = edgeKey(edge)
                const tone = () => edgeTone(edge)
                return (
                  <tr
                    class={cx(trHoverClass, 'cursor-pointer', props.selected === key && 'bg-surface2/60')}
                    onClick={() => props.onSelect?.(key)}
                  >
                    <td class={tdClass}>
                      <A class={linkClass} href={`/jobs/${encodeURIComponent(edge.callerJob)}/tasks/${encodeURIComponent(edge.callerTask)}`}>
                        {edge.callerJob}/{edge.callerTask}
                      </A>
                    </td>
                    <Show when={!props.hideService}>
                      <td class={tdClass}>
                        <A class={linkClass} href={`/services/${encodeURIComponent(edge.service)}`}>
                          {edge.service}
                        </A>
                      </td>
                    </Show>
                    <td class={tdClass}>
                      <Show when={edge.tag} fallback={<span class="text-faint">—</span>}>
                        <MonoBadge tone="neutral">{edge.tag}</MonoBadge>
                      </Show>
                    </td>
                    <td class={tdClass}>
                      <div class="flex flex-wrap gap-1">
                        <Badge tone={tone()} dot>
                          {!edge.allowed
                            ? 'not allowed'
                            : Number(edge.denied) > 0
                              ? 'denials'
                              : Number(edge.requests) === 0
                                ? edge.declared
                                  ? 'declared, silent'
                                  : 'silent'
                                : edge.errorRate >= 0.05
                                  ? 'failing'
                                  : edge.errorRate > 0
                                    ? 'some errors'
                                    : 'healthy'}
                        </Badge>
                        <Show when={edge.declared}>
                          <Badge tone="neutral">declared</Badge>
                        </Show>
                        <Show when={edge.protocol}>
                          <Badge tone="neutral">{edge.protocol}</Badge>
                        </Show>
                      </div>
                    </td>
                    <td class={cx(tdClass, 'text-right tabular-nums')}>{fmtRate(edge.rps)}</td>
                    <td class={cx(tdClass, 'text-right tabular-nums')}>
                      {String(edge.errors)} <span class="text-faint">({fmtPct(edge.errorRate)})</span>
                    </td>
                    <td class={cx(tdClass, 'text-right tabular-nums', Number(edge.denied) > 0 && 'text-danger')}>{String(edge.denied)}</td>
                    <td class={cx(tdClass, 'text-right tabular-nums')}>
                      <Show when={edge.protocol === 'http' && Number(edge.requests) > 0} fallback={<span class="text-faint">—</span>}>
                        {fmtMs(edge.latencyMsAvg)} / {fmtMs(Number(edge.latencyMsMax))}
                      </Show>
                    </td>
                    <td class={cx(tdClass, 'text-right tabular-nums text-muted')}>
                      {fmtBytes(edge.bytesOut)} / {fmtBytes(edge.bytesIn)}
                    </td>
                    <td class={cx(tdClass, 'text-muted')}>
                      <Show when={Number(edge.lastSeenUnix) > 0} fallback={<span class="text-faint">never</span>}>
                        {ago(edge.lastSeenUnix)}
                      </Show>
                    </td>
                  </tr>
                )
              }}
            </For>
          </tbody>
        </table>
      </TableShell>
    </Show>
  )
}

/// The graph plus its table, sharing a selection.
export function ServiceGraph(props: { edges: ServiceEdge[]; windowSecs: number }) {
  const [selected, setSelected] = createSignal<string | undefined>()
  const toggle = (key: string) => setSelected((current) => (current === key ? undefined : key))
  const sorted = createMemo(() =>
    [...props.edges].sort((a, b) => {
      const order = { danger: 0, warn: 1, ok: 2, neutral: 3 }
      const byTone = order[edgeTone(a)] - order[edgeTone(b)]
      if (byTone !== 0) return byTone
      return b.rps - a.rps
    }),
  )
  return (
    <div class="space-y-3">
      <Show when={props.edges.length > 0}>
        <ServiceGraphSvg edges={props.edges} selected={selected()} onSelect={toggle} />
        <p class="text-xs text-faint">
          Line width follows request rate over the last {Math.round(props.windowSecs / 60)} min; dashed edges are declared upstreams that
          have not called; red edges carry denials, errors above 5%, or a caller the service's <code class="font-mono">allow</code> list
          does not admit.
        </p>
      </Show>
      <ServiceEdgesTable edges={sorted()} selected={selected()} onSelect={toggle} />
    </div>
  )
}
