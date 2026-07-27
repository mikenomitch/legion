import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal, onCleanup, onMount } from 'solid-js'
import type { JobTask, TaskInstanceSummary } from '../utils/jobspec'
import { taskStatusTone } from '../utils/jobspec'

// ── Layout constants ────────────────────────────────────────────────
const NODE_W = 232
const NODE_H = 84
const H_GAP = 120
const V_GAP = 32
const PAD_X = 56
const PAD_Y = 56

// Sidecars are docked underneath their host task (they run alongside it,
// they are not a downstream stage).
const SIDE_H = 56
const SIDE_GAP = 12 // gap between stacked sidecars
const DOCK_GAP = 30 // gap between host and its first sidecar (room for the link)
const DOCK_INSET = 22 // horizontal inset so a sidecar reads as "attached"

const MIN_SCALE = 0.25
const MAX_SCALE = 2.5

const SIDECAR_COLOR = '#6366f1'

// ── Types ───────────────────────────────────────────────────────────
type PlacedNode = { name: string; x: number; y: number; w: number; h: number; task: JobTask; isSidecar: boolean }
type Edge = { from: string; to: string; condition?: string; fromPos: PlacedNode; toPos: PlacedNode }
type DockLink = { parent: string; child: string; x1: number; y1: number; x2: number; y2: number }

type DagVisualizationProps = {
  tasks: JobTask[]
  layers: string[][]
  taskInstances: Map<string, TaskInstanceSummary>
  statusLoading?: boolean
  jobName: string
}

// ── Status styling ──────────────────────────────────────────────────
type Tone = 'success' | 'warning' | 'danger' | 'neutral'

const toneStyle: Record<Tone, { bar: string; bg: string; ring: string; dot: string; text: string }> = {
  success: { bar: 'bg-emerald-500', bg: 'bg-surface', ring: 'ring-emerald-400/40 dark:ring-emerald-500/30', dot: 'bg-emerald-500', text: 'text-emerald-700 dark:text-emerald-300' },
  warning: { bar: 'bg-amber-500', bg: 'bg-surface', ring: 'ring-amber-400/40 dark:ring-amber-500/30', dot: 'bg-amber-500', text: 'text-amber-700 dark:text-amber-300' },
  danger: { bar: 'bg-red-500', bg: 'bg-surface', ring: 'ring-red-400/40 dark:ring-red-500/30', dot: 'bg-red-500', text: 'text-red-700 dark:text-red-300' },
  neutral: { bar: 'bg-line-strong', bg: 'bg-surface', ring: 'ring-line/60', dot: 'bg-faint', text: 'text-muted' },
}

const conditionStroke: Record<string, string> = { success: '#10b981', complete: '#0ea5e9', start: '#f59e0b' }
const strokeFor = (condition?: string) => (condition && conditionStroke[condition]) || '#94a3b8'

const GROUP_PALETTE = [
  '#6366f1', '#0ea5e9', '#14b8a6', '#f59e0b', '#ec4899',
  '#8b5cf6', '#22c55e', '#ef4444', '#f97316', '#06b6d4',
]

const lifecycleGlyph = (lifecycle: string): string => {
  switch (lifecycle) {
    case 'sidecar': return '⛓'
    case 'batch': return '⧗'
    default: return '⟳'
  }
}

const clamp = (value: number, lo: number, hi: number) => Math.min(Math.max(value, lo), hi)

// ── Bezier path builder (left → right) ──────────────────────────────
function edgePath(from: PlacedNode, to: PlacedNode): string {
  const x1 = from.x + from.w
  const y1 = from.y + from.h / 2
  const x2 = to.x
  const y2 = to.y + to.h / 2
  const dx = Math.max(Math.abs(x2 - x1) * 0.5, 48)
  return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`
}

function edgeMid(from: PlacedNode, to: PlacedNode): { x: number; y: number } {
  return { x: (from.x + from.w + to.x) / 2, y: (from.y + from.h / 2 + to.y + to.h / 2) / 2 }
}

// ── Component ───────────────────────────────────────────────────────
export default function DagVisualization(props: DagVisualizationProps) {
  const taskMap = createMemo(() => new Map(props.tasks.map((t) => [t.name, t])))

  const groupColors = createMemo(() => {
    const groups = [...new Set(props.tasks.map((t) => t.group))]
    const map = new Map<string, string>()
    groups.forEach((g, i) => map.set(g, GROUP_PALETTE[i % GROUP_PALETTE.length]))
    return map
  })

  const sidecarNames = createMemo(() => new Set(props.tasks.filter((t) => t.lifecycle === 'sidecar').map((t) => t.name)))

  // Resolve each sidecar's host: the non-sidecar task it depends on, else the
  // first non-sidecar task in its group. Sidecars without a host stay inline.
  const hostOf = createMemo(() => {
    const sidecars = sidecarNames()
    const map = new Map<string, string>()
    for (const task of props.tasks) {
      if (task.lifecycle !== 'sidecar') continue
      let host = task.dependsOn.map((d) => d.task).find((n) => taskMap().has(n) && !sidecars.has(n))
      if (!host) host = props.tasks.find((o) => o.lifecycle !== 'sidecar' && o.group === task.group)?.name
      if (host) map.set(task.name, host)
    }
    return map
  })

  const dockedSidecars = createMemo(() => new Set(hostOf().keys()))

  const hostedBy = createMemo(() => {
    const map = new Map<string, string[]>()
    for (const [side, host] of hostOf()) {
      if (!map.has(host)) map.set(host, [])
      map.get(host)!.push(side)
    }
    return map
  })

  // Topological columns, with docked sidecars removed so they never form a stage.
  const primaryLayers = createMemo(() => {
    const docked = dockedSidecars()
    return props.layers.map((layer) => layer.filter((n) => !docked.has(n))).filter((layer) => layer.length > 0)
  })

  // Lay each column out as a vertical stack of "blocks" (host + its sidecars).
  const layout = createMemo(() => {
    const placed = new Map<string, PlacedNode>()
    const dockLinks: DockLink[] = []
    const layers = primaryLayers()

    const colBlocks = layers.map((layer) =>
      layer.map((name) => {
        const sides = hostedBy().get(name) ?? []
        const sideSpan = sides.length ? DOCK_GAP + sides.length * SIDE_H + (sides.length - 1) * SIDE_GAP : 0
        return { name, sides, h: NODE_H + sideSpan }
      }),
    )
    const colHeights = colBlocks.map((blocks) => blocks.reduce((acc, b) => acc + b.h, 0) + V_GAP * Math.max(0, blocks.length - 1))
    const maxH = Math.max(NODE_H, ...(colHeights.length ? colHeights : [NODE_H]))

    colBlocks.forEach((blocks, col) => {
      const x = PAD_X + col * (NODE_W + H_GAP)
      let y = PAD_Y + (maxH - colHeights[col]) / 2
      for (const block of blocks) {
        const task = taskMap().get(block.name)
        if (task) placed.set(block.name, { name: block.name, x, y, w: NODE_W, h: NODE_H, task, isSidecar: false })

        let parentBottomY = y + NODE_H
        let parentX = x + 28
        let parentName = block.name
        let sy = y + NODE_H + DOCK_GAP
        for (const sideName of block.sides) {
          const stask = taskMap().get(sideName)
          const sx = x + DOCK_INSET
          if (stask) {
            placed.set(sideName, { name: sideName, x: sx, y: sy, w: NODE_W - DOCK_INSET, h: SIDE_H, task: stask, isSidecar: true })
            dockLinks.push({ parent: parentName, child: sideName, x1: parentX, y1: parentBottomY, x2: sx + 18, y2: sy })
          }
          parentBottomY = sy + SIDE_H
          parentX = sx + 18
          parentName = sideName
          sy += SIDE_H + SIDE_GAP
        }
        y += block.h + V_GAP
      }
    })
    return { placed, dockLinks, columns: layers.length, maxH }
  })

  const edges = createMemo((): Edge[] => {
    const result: Edge[] = []
    const placed = layout().placed
    const docked = dockedSidecars()
    for (const task of props.tasks) {
      if (docked.has(task.name)) continue // shown via its dock link instead
      const toPos = placed.get(task.name)
      if (!toPos) continue
      for (const dep of task.dependsOn) {
        const fromPos = placed.get(dep.task)
        if (!fromPos) continue
        result.push({ from: dep.task, to: task.name, condition: dep.condition, fromPos, toPos })
      }
    }
    return result
  })

  const canvasW = createMemo(() => PAD_X * 2 + Math.max(1, layout().columns) * (NODE_W + H_GAP) - H_GAP)
  const canvasH = createMemo(() => PAD_Y * 2 + layout().maxH)

  const hasSidecars = createMemo(() => dockedSidecars().size > 0)

  const statusFor = (taskName: string): { status: string; tone: Tone } => {
    const runtime = props.taskInstances.get(taskName)
    const fallback = props.statusLoading ? 'loading…' : 'not allocated'
    const status = runtime?.statuses[0] ?? fallback
    return { status, tone: taskStatusTone(status) }
  }

  // ── Pan & zoom state ──────────────────────────────────────────────
  let viewport: HTMLDivElement | undefined
  const [scale, setScale] = createSignal(1)
  const [tx, setTx] = createSignal(0)
  const [ty, setTy] = createSignal(0)
  const [panning, setPanning] = createSignal(false)
  const [hovered, setHovered] = createSignal<string | null>(null)

  const fitToView = () => {
    if (!viewport) return
    const vw = viewport.clientWidth
    const vh = viewport.clientHeight
    const cw = canvasW()
    const ch = canvasH()
    if (cw <= 0 || ch <= 0) return
    const s = clamp(Math.min(vw / cw, vh / ch) * 0.92, MIN_SCALE, 1.4)
    setScale(s)
    setTx((vw - cw * s) / 2)
    setTy((vh - ch * s) / 2)
  }

  const zoomAround = (factor: number, cx: number, cy: number) => {
    const next = clamp(scale() * factor, MIN_SCALE, MAX_SCALE)
    const wx = (cx - tx()) / scale()
    const wy = (cy - ty()) / scale()
    setTx(cx - wx * next)
    setTy(cy - wy * next)
    setScale(next)
  }

  const zoomButton = (factor: number) => {
    if (!viewport) return
    zoomAround(factor, viewport.clientWidth / 2, viewport.clientHeight / 2)
  }

  const onWheel = (e: WheelEvent) => {
    if (!viewport) return
    e.preventDefault()
    const rect = viewport.getBoundingClientRect()
    zoomAround(Math.exp(-e.deltaY * 0.0015), e.clientX - rect.left, e.clientY - rect.top)
  }

  let panStart = { x: 0, y: 0, tx: 0, ty: 0 }
  const onPanMove = (e: MouseEvent) => {
    if (!panning()) return
    setTx(panStart.tx + (e.clientX - panStart.x))
    setTy(panStart.ty + (e.clientY - panStart.y))
  }
  const onPanEnd = () => setPanning(false)

  const onBackgroundMouseDown = (e: MouseEvent) => {
    if ((e.target as HTMLElement).closest('[data-dag-node]')) return
    e.preventDefault()
    panStart = { x: e.clientX, y: e.clientY, tx: tx(), ty: ty() }
    setPanning(true)
  }

  onMount(() => {
    window.addEventListener('mousemove', onPanMove)
    window.addEventListener('mouseup', onPanEnd)
    requestAnimationFrame(fitToView)
    onCleanup(() => {
      window.removeEventListener('mousemove', onPanMove)
      window.removeEventListener('mouseup', onPanEnd)
    })
  })

  // Highlight set: hovered node + its direct neighbors (edges and dock links).
  const neighbors = createMemo(() => {
    const h = hovered()
    if (!h) return null
    const set = new Set<string>([h])
    for (const e of edges()) {
      if (e.from === h) set.add(e.to)
      if (e.to === h) set.add(e.from)
    }
    for (const link of layout().dockLinks) {
      if (link.parent === h) set.add(link.child)
      if (link.child === h) set.add(link.parent)
    }
    return set
  })

  const edgeActive = (edge: Edge) => {
    const h = hovered()
    if (!h) return true
    return edge.from === h || edge.to === h
  }

  const isDimmed = (name: string) => {
    const set = neighbors()
    return set ? !set.has(name) : false
  }

  return (
    <div class="space-y-3">
      <style>{`
        @keyframes dag-flow { to { stroke-dashoffset: -16; } }
        .dag-edge-flow { stroke-dasharray: 5 7; animation: dag-flow 0.7s linear infinite; }
      `}</style>

      {/* Toolbar / legend */}
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-2xs text-muted">
          <span class="inline-flex items-center gap-1.5"><span class="h-2 w-2 rounded-full bg-emerald-500" /> Running</span>
          <span class="inline-flex items-center gap-1.5"><span class="h-2 w-2 rounded-full bg-amber-500" /> Pending</span>
          <span class="inline-flex items-center gap-1.5"><span class="h-2 w-2 rounded-full bg-red-500" /> Failed</span>
          <span class="inline-flex items-center gap-1.5"><span class="h-2 w-2 rounded-full bg-faint" /> Idle</span>
          <Show when={hasSidecars()}>
            <span class="inline-flex items-center gap-1.5">
              <svg width="20" height="8" class="shrink-0"><line x1="1" y1="4" x2="19" y2="4" stroke={SIDECAR_COLOR} stroke-width="2" stroke-dasharray="4 3" /></svg>
              Sidecar (runs alongside)
            </span>
          </Show>
          <span class="border-l border-line pl-3">drag to pan · scroll to zoom</span>
        </div>
        <div class="flex items-center gap-1">
          <button type="button" onClick={() => zoomButton(1 / 1.2)} class="h-7 w-7 rounded-md border border-line text-sm font-semibold text-muted transition-colors hover:bg-surface2 hover:text-ink" aria-label="Zoom out">−</button>
          <button type="button" onClick={() => zoomButton(1.2)} class="h-7 w-7 rounded-md border border-line text-sm font-semibold text-muted transition-colors hover:bg-surface2 hover:text-ink" aria-label="Zoom in">+</button>
          <button type="button" onClick={fitToView} class="h-7 rounded-md border border-line px-2 text-2xs font-semibold uppercase tracking-wide text-muted transition-colors hover:bg-surface2 hover:text-ink">Fit</button>
        </div>
      </div>

      {/* DAG canvas */}
      <div
        ref={viewport}
        onWheel={onWheel}
        onMouseDown={onBackgroundMouseDown}
        class="relative h-[560px] w-full overflow-hidden rounded-lg border border-line bg-canvas"
        style={{
          cursor: panning() ? 'grabbing' : 'grab',
          'background-image': 'radial-gradient(circle, rgb(var(--c-line-strong) / 0.35) 1px, transparent 1px)',
          'background-size': '22px 22px',
        }}
      >
        <div
          class="absolute left-0 top-0 origin-top-left"
          style={{ transform: `translate(${tx()}px, ${ty()}px) scale(${scale()})`, width: `${canvasW()}px`, height: `${canvasH()}px` }}
        >
          {/* SVG edge layer */}
          <svg class="absolute inset-0 overflow-visible" width={canvasW()} height={canvasH()}>
            <defs>
              <For each={['success', 'complete', 'start', 'none']}>
                {(key) => (
                  <marker id={`dag-arrow-${key}`} markerWidth="9" markerHeight="7" refX="7.5" refY="3.5" orient="auto">
                    <path d="M 0 0 L 9 3.5 L 0 7 Z" fill={strokeFor(key === 'none' ? undefined : key)} />
                  </marker>
                )}
              </For>
            </defs>

            {/* Stage column guides + labels */}
            <For each={primaryLayers()}>
              {(_layer, colIdx) => {
                const x = PAD_X + colIdx() * (NODE_W + H_GAP) - 16
                return (
                  <>
                    <rect x={x} y={20} width={NODE_W + 32} height={canvasH() - 40} rx="12" fill="rgb(var(--c-surface2) / 0.5)" />
                    <text x={x + (NODE_W + 32) / 2} y={40} text-anchor="middle" font-size="11" font-weight="700" letter-spacing="0.12em" fill="rgb(var(--c-faint))">
                      STAGE {colIdx() + 1}
                    </text>
                  </>
                )
              }}
            </For>

            {/* Dependency edges */}
            <For each={edges()}>
              {(edge) => {
                const path = edgePath(edge.fromPos, edge.toPos)
                const mid = edgeMid(edge.fromPos, edge.toPos)
                const color = strokeFor(edge.condition)
                const markerKey = edge.condition ?? 'none'
                const active = () => edgeActive(edge)
                const flowing = () => statusFor(edge.to).tone === 'warning'
                return (
                  <g style={{ opacity: active() ? 1 : 0.12, transition: 'opacity 0.15s' }}>
                    <path d={path} fill="none" stroke-width="5" stroke="rgb(var(--c-line) / 0.7)" stroke-linecap="round" />
                    <path d={path} fill="none" stroke={color} stroke-width={active() && hovered() ? 2.75 : 2} stroke-linecap="round" marker-end={`url(#dag-arrow-${markerKey})`} classList={{ 'dag-edge-flow': flowing() }} />
                    <Show when={edge.condition}>
                      <g>
                        <rect x={mid.x - 27} y={mid.y - 9} width="54" height="17" rx="8" fill={color} opacity="0.14" />
                        <text x={mid.x} y={mid.y + 3} text-anchor="middle" font-size="9" font-weight="700" letter-spacing="0.06em" fill={color}>{(edge.condition ?? '').toUpperCase()}</text>
                      </g>
                    </Show>
                  </g>
                )
              }}
            </For>

            {/* Sidecar dock links (dashed — "runs alongside", not a stage) */}
            <For each={layout().dockLinks}>
              {(link) => {
                const midY = (link.y1 + link.y2) / 2
                const path = `M ${link.x1} ${link.y1} C ${link.x1} ${midY}, ${link.x2} ${midY}, ${link.x2} ${link.y2}`
                const active = () => {
                  const h = hovered()
                  return !h || h === link.parent || h === link.child
                }
                return (
                  <g style={{ opacity: active() ? 1 : 0.12, transition: 'opacity 0.15s' }}>
                    <path d={path} fill="none" stroke={SIDECAR_COLOR} stroke-width="2" stroke-dasharray="4 3" stroke-linecap="round" />
                    <circle cx={link.x2} cy={link.y2} r="2.6" fill={SIDECAR_COLOR} />
                    <g>
                      <rect x={link.x2 + 6} y={midY - 8} width="58" height="16" rx="8" fill={SIDECAR_COLOR} opacity="0.14" />
                      <text x={link.x2 + 35} y={midY + 3} text-anchor="middle" font-size="8.5" font-weight="700" letter-spacing="0.06em" fill={SIDECAR_COLOR}>SIDECAR</text>
                    </g>
                  </g>
                )
              }}
            </For>
          </svg>

          {/* Node layer */}
          <For each={[...layout().placed.entries()]}>
            {([taskName, pos]) => {
              const info = () => statusFor(taskName)
              const tone = () => info().tone
              const style = () => toneStyle[tone()]
              const inProgress = () => tone() === 'warning'
              const dimmed = () => isDimmed(taskName)
              const groupColor = () => groupColors().get(pos.task.group) ?? '#64748b'
              const href = `/jobs/${encodeURIComponent(props.jobName)}/tasks/${encodeURIComponent(taskName)}`

              // ── Sidecar: compact, dashed-indigo, clearly badged ──
              if (pos.isSidecar) {
                return (
                  <A
                    href={href}
                    data-dag-node
                    onMouseEnter={() => setHovered(taskName)}
                    onMouseLeave={() => setHovered(null)}
                    draggable={false}
                    class={`absolute flex items-stretch overflow-hidden rounded-lg border border-dashed shadow-sm ring-1 transition-all duration-150 hover:-translate-y-0.5 hover:shadow-lg ${style().ring}`}
                    style={{
                      left: `${pos.x}px`, top: `${pos.y}px`, width: `${pos.w}px`, height: `${pos.h}px`,
                      'border-color': SIDECAR_COLOR,
                      'background-color': 'color-mix(in srgb, ' + SIDECAR_COLOR + ' 8%, rgb(var(--c-surface)))',
                      opacity: dimmed() ? 0.35 : 1,
                      'z-index': hovered() === taskName ? 20 : 1,
                    }}
                  >
                    <div class={`w-1.5 shrink-0 ${style().bar} ${inProgress() ? 'animate-pulse' : ''}`} />
                    <div class="flex flex-1 flex-col justify-center gap-0.5 px-2.5 py-1.5 min-w-0">
                      <div class="flex items-center gap-1.5 min-w-0">
                        <span class="shrink-0 text-[11px]" style={{ color: SIDECAR_COLOR }}>{lifecycleGlyph('sidecar')}</span>
                        <span class={`h-1.5 w-1.5 shrink-0 rounded-full ${style().dot} ${inProgress() ? 'animate-pulse' : ''}`} />
                        <span class="truncate text-[12px] font-semibold text-ink">{pos.task.name}</span>
                        <span class="ml-auto shrink-0 rounded px-1.5 py-0.5 text-[8px] font-bold uppercase tracking-wider text-white" style={{ 'background-color': SIDECAR_COLOR }}>Sidecar</span>
                      </div>
                      <div class="flex items-center gap-1.5 text-[9px] min-w-0">
                        <span class={`font-semibold uppercase tracking-wide ${style().text}`}>{info().status}</span>
                        <span class="ml-auto inline-flex shrink-0 items-center gap-1 truncate font-medium" style={{ color: groupColor() }}>
                          <span class="h-1.5 w-1.5 rounded-full" style={{ 'background-color': groupColor() }} />
                          {pos.task.group}
                        </span>
                      </div>
                    </div>
                  </A>
                )
              }

              // ── Primary task node ──
              return (
                <A
                  href={href}
                  data-dag-node
                  onMouseEnter={() => setHovered(taskName)}
                  onMouseLeave={() => setHovered(null)}
                  draggable={false}
                  class={`group absolute flex flex-col overflow-hidden rounded-xl border border-line shadow-sm ring-1 transition-all duration-150 hover:-translate-y-0.5 hover:shadow-lg ${style().ring} ${style().bg}`}
                  style={{ left: `${pos.x}px`, top: `${pos.y}px`, width: `${pos.w}px`, height: `${pos.h}px`, opacity: dimmed() ? 0.35 : 1, 'z-index': hovered() === taskName ? 20 : 1 }}
                >
                  <div class="h-1 w-full shrink-0" style={{ 'background-color': groupColor() }} />
                  <div class="flex flex-1 items-stretch">
                    <div class={`w-1.5 shrink-0 ${style().bar} ${inProgress() ? 'animate-pulse' : ''}`} />
                    <div class="flex flex-1 flex-col justify-center gap-1 px-3 py-2 min-w-0">
                      <div class="flex items-center gap-2 min-w-0">
                        <span class={`h-2 w-2 shrink-0 rounded-full ${style().dot} ${inProgress() ? 'animate-pulse' : ''}`} />
                        <span class="truncate text-[13px] font-semibold text-ink">{pos.task.name}</span>
                      </div>
                      <div class="flex items-center gap-1.5 text-[10px] text-muted min-w-0">
                        <span class="shrink-0 rounded bg-surface2 px-1.5 py-0.5 font-medium">{pos.task.runtime}</span>
                        <span class="inline-flex shrink-0 items-center gap-0.5" title={`${pos.task.lifecycle} lifecycle`}>{lifecycleGlyph(pos.task.lifecycle)} {pos.task.lifecycle}</span>
                        <span class="ml-auto inline-flex shrink-0 items-center gap-1 truncate font-medium" style={{ color: groupColor() }} title={`group: ${pos.task.group}`}>
                          <span class="h-1.5 w-1.5 rounded-full" style={{ 'background-color': groupColor() }} />
                          {pos.task.group}
                        </span>
                      </div>
                      <div class={`text-[10px] font-semibold uppercase tracking-wide ${style().text}`}>{info().status}</div>
                    </div>
                  </div>
                </A>
              )
            }}
          </For>
        </div>

        {/* Group legend overlay */}
        <Show when={groupColors().size > 1}>
          <div class="pointer-events-none absolute bottom-3 left-3 flex max-w-[70%] flex-wrap gap-x-3 gap-y-1 rounded-lg border border-line/80 bg-surface/85 px-3 py-2 text-[10px] shadow-sm backdrop-blur">
            <span class="font-semibold uppercase tracking-wide text-faint">Groups</span>
            <For each={[...groupColors().entries()]}>
              {([group, color]) => (
                <span class="inline-flex items-center gap-1 text-muted">
                  <span class="h-2 w-2 rounded-full" style={{ 'background-color': color }} />
                  {group}
                </span>
              )}
            </For>
          </div>
        </Show>

        {/* Zoom indicator */}
        <div class="pointer-events-none absolute bottom-3 right-3 rounded-md border border-line/80 bg-surface/85 px-2 py-1 text-[10px] font-semibold tabular-nums text-muted shadow-sm backdrop-blur">
          {Math.round(scale() * 100)}%
        </div>
      </div>
    </div>
  )
}
