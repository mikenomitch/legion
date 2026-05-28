import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal, onCleanup, onMount } from 'solid-js'
import type { JobTask, TaskRuntimeSummary } from '../utils/jobspec'
import { taskStatusTone } from '../utils/jobspec'

// ── Layout constants ────────────────────────────────────────────────
const NODE_W = 232
const NODE_H = 84
const H_GAP = 120
const V_GAP = 32
const PAD_X = 56
const PAD_Y = 56

const MIN_SCALE = 0.25
const MAX_SCALE = 2.5

// ── Types ───────────────────────────────────────────────────────────
type NodePos = { x: number; y: number; task: JobTask; stage: number }

type Edge = {
  from: string
  to: string
  condition?: string
  fromPos: NodePos
  toPos: NodePos
}

type DagVisualizationProps = {
  tasks: JobTask[]
  layers: string[][]
  taskRuntime: Map<string, TaskRuntimeSummary>
  jobName: string
}

// ── Status styling ──────────────────────────────────────────────────
type Tone = 'success' | 'warning' | 'danger' | 'neutral'

const toneStyle: Record<Tone, { bar: string; bg: string; ring: string; dot: string; text: string; label: string }> = {
  success: {
    bar: 'bg-emerald-500',
    bg: 'bg-white dark:bg-neutral-900',
    ring: 'ring-emerald-400/40 dark:ring-emerald-500/30',
    dot: 'bg-emerald-500',
    text: 'text-emerald-700 dark:text-emerald-300',
    label: 'Running',
  },
  warning: {
    bar: 'bg-amber-500',
    bg: 'bg-white dark:bg-neutral-900',
    ring: 'ring-amber-400/40 dark:ring-amber-500/30',
    dot: 'bg-amber-500',
    text: 'text-amber-700 dark:text-amber-300',
    label: 'Pending',
  },
  danger: {
    bar: 'bg-rose-500',
    bg: 'bg-white dark:bg-neutral-900',
    ring: 'ring-rose-400/40 dark:ring-rose-500/30',
    dot: 'bg-rose-500',
    text: 'text-rose-700 dark:text-rose-300',
    label: 'Failed',
  },
  neutral: {
    bar: 'bg-neutral-300 dark:bg-neutral-600',
    bg: 'bg-white dark:bg-neutral-900',
    ring: 'ring-neutral-300/40 dark:ring-neutral-600/30',
    dot: 'bg-neutral-400 dark:bg-neutral-500',
    text: 'text-neutral-500 dark:text-neutral-400',
    label: 'Idle',
  },
}

const conditionStroke: Record<string, string> = {
  success: '#10b981',
  complete: '#0ea5e9',
  start: '#f59e0b',
}
const strokeFor = (condition?: string) => (condition && conditionStroke[condition]) || '#94a3b8'

// Distinct, accessible palette for task groups (works in light + dark).
const GROUP_PALETTE = [
  '#6366f1', '#0ea5e9', '#14b8a6', '#f59e0b', '#ec4899',
  '#8b5cf6', '#22c55e', '#ef4444', '#f97316', '#06b6d4',
]

const lifecycleGlyph = (lifecycle: string): string => {
  switch (lifecycle) {
    case 'sidecar':
      return '⛓'
    case 'batch':
      return '⧗'
    default:
      return '⟳'
  }
}

const clamp = (value: number, lo: number, hi: number) => Math.min(Math.max(value, lo), hi)

// ── Bezier path builder (left → right) ──────────────────────────────
function edgePath(from: NodePos, to: NodePos): string {
  const x1 = from.x + NODE_W
  const y1 = from.y + NODE_H / 2
  const x2 = to.x
  const y2 = to.y + NODE_H / 2
  const dx = Math.max(Math.abs(x2 - x1) * 0.5, 48)
  return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`
}

function edgeMid(from: NodePos, to: NodePos): { x: number; y: number } {
  return {
    x: (from.x + NODE_W + to.x) / 2,
    y: (from.y + to.y) / 2 + NODE_H / 2,
  }
}

// ── Component ───────────────────────────────────────────────────────
export default function DagVisualization(props: DagVisualizationProps) {
  const taskMap = createMemo(() => new Map(props.tasks.map((t) => [t.name, t])))

  // Stable color per group.
  const groupColors = createMemo(() => {
    const groups = [...new Set(props.tasks.map((t) => t.group))]
    const map = new Map<string, string>()
    groups.forEach((g, i) => map.set(g, GROUP_PALETTE[i % GROUP_PALETTE.length]))
    return map
  })

  // Node positions from topological layers, each column vertically centered.
  const nodePositions = createMemo((): Map<string, NodePos> => {
    const positions = new Map<string, NodePos>()
    const layers = props.layers
    const maxLayerHeight = Math.max(1, ...layers.map((l) => l.length * (NODE_H + V_GAP) - V_GAP))

    layers.forEach((layer, col) => {
      const layerHeight = layer.length * (NODE_H + V_GAP) - V_GAP
      const yOffset = (maxLayerHeight - layerHeight) / 2
      layer.forEach((taskName, row) => {
        const task = taskMap().get(taskName)
        if (!task) return
        positions.set(taskName, {
          x: PAD_X + col * (NODE_W + H_GAP),
          y: PAD_Y + yOffset + row * (NODE_H + V_GAP),
          task,
          stage: col,
        })
      })
    })
    return positions
  })

  const edges = createMemo((): Edge[] => {
    const result: Edge[] = []
    const positions = nodePositions()
    for (const task of props.tasks) {
      const toPos = positions.get(task.name)
      if (!toPos) continue
      for (const dep of task.dependsOn) {
        const fromPos = positions.get(dep.task)
        if (!fromPos) continue
        result.push({ from: dep.task, to: task.name, condition: dep.condition, fromPos, toPos })
      }
    }
    return result
  })

  const canvasW = createMemo(() => PAD_X * 2 + props.layers.length * (NODE_W + H_GAP) - H_GAP)
  const canvasH = createMemo(() => {
    const maxInLayer = Math.max(1, ...props.layers.map((l) => l.length))
    return PAD_Y * 2 + maxInLayer * (NODE_H + V_GAP) - V_GAP
  })

  const hasDependencies = createMemo(() => props.tasks.some((t) => t.dependsOn.length > 0))

  const statusFor = (taskName: string): { status: string; tone: Tone } => {
    const runtime = props.taskRuntime.get(taskName)
    const status = runtime?.statuses[0] ?? 'not allocated'
    return { status, tone: taskStatusTone(status) as Tone }
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
    // Only pan from empty canvas, not from a node (so links keep working).
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

  // Highlight set: hovered node + its direct neighbors.
  const neighbors = createMemo(() => {
    const h = hovered()
    if (!h) return null
    const set = new Set<string>([h])
    for (const e of edges()) {
      if (e.from === h) set.add(e.to)
      if (e.to === h) set.add(e.from)
    }
    return set
  })

  const edgeActive = (edge: Edge) => {
    const h = hovered()
    if (!h) return true
    return edge.from === h || edge.to === h
  }

  return (
    <div class="space-y-3">
      <style>{`
        @keyframes dag-flow { to { stroke-dashoffset: -16; } }
        .dag-edge-flow { stroke-dasharray: 5 7; animation: dag-flow 0.7s linear infinite; }
      `}</style>

      {/* Toolbar / legend */}
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-neutral-500 dark:text-neutral-400">
          <span class="inline-flex items-center gap-1.5"><span class="h-2.5 w-2.5 rounded-full bg-emerald-500" /> Running</span>
          <span class="inline-flex items-center gap-1.5"><span class="h-2.5 w-2.5 rounded-full bg-amber-500" /> Pending</span>
          <span class="inline-flex items-center gap-1.5"><span class="h-2.5 w-2.5 rounded-full bg-rose-500" /> Failed</span>
          <span class="inline-flex items-center gap-1.5"><span class="h-2.5 w-2.5 rounded-full bg-neutral-400" /> Idle</span>
          <Show when={hasDependencies()}>
            <span class="border-l border-neutral-300 pl-3 dark:border-neutral-700">drag to pan · scroll to zoom</span>
          </Show>
        </div>
        <div class="flex items-center gap-1">
          <button type="button" onClick={() => zoomButton(1 / 1.2)} class="h-7 w-7 rounded border border-neutral-300 text-sm font-semibold text-neutral-600 transition hover:bg-neutral-100 dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-800" aria-label="Zoom out">−</button>
          <button type="button" onClick={() => zoomButton(1.2)} class="h-7 w-7 rounded border border-neutral-300 text-sm font-semibold text-neutral-600 transition hover:bg-neutral-100 dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-800" aria-label="Zoom in">+</button>
          <button type="button" onClick={fitToView} class="h-7 rounded border border-neutral-300 px-2 text-[11px] font-semibold uppercase tracking-wide text-neutral-600 transition hover:bg-neutral-100 dark:border-neutral-700 dark:text-neutral-300 dark:hover:bg-neutral-800">Fit</button>
        </div>
      </div>

      {/* DAG canvas */}
      <div
        ref={viewport}
        onWheel={onWheel}
        onMouseDown={onBackgroundMouseDown}
        class="relative h-[560px] w-full overflow-hidden rounded-xl border border-neutral-200 bg-neutral-50 dark:border-neutral-800 dark:bg-neutral-950"
        style={{
          cursor: panning() ? 'grabbing' : 'grab',
          'background-image':
            'radial-gradient(circle, rgba(120,120,140,0.18) 1px, transparent 1px)',
          'background-size': '22px 22px',
        }}
      >
        {/* Transformed world */}
        <div
          class="absolute left-0 top-0 origin-top-left"
          style={{
            transform: `translate(${tx()}px, ${ty()}px) scale(${scale()})`,
            width: `${canvasW()}px`,
            height: `${canvasH()}px`,
          }}
        >
          {/* SVG edge layer */}
          <svg
            class="absolute inset-0 overflow-visible"
            width={canvasW()}
            height={canvasH()}
          >
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
            <For each={props.layers}>
              {(_layer, colIdx) => {
                const x = PAD_X + colIdx() * (NODE_W + H_GAP) - 16
                return (
                  <>
                    <rect
                      x={x}
                      y={20}
                      width={NODE_W + 32}
                      height={canvasH() - 40}
                      rx="12"
                      fill="currentColor"
                      class="text-neutral-200/30 dark:text-neutral-800/40"
                    />
                    <text
                      x={x + (NODE_W + 32) / 2}
                      y={40}
                      text-anchor="middle"
                      font-size="11"
                      font-weight="700"
                      letter-spacing="0.12em"
                      fill="currentColor"
                      class="text-neutral-400 dark:text-neutral-500"
                    >
                      STAGE {colIdx() + 1}
                    </text>
                  </>
                )
              }}
            </For>

            {/* Edges */}
            <For each={edges()}>
              {(edge) => {
                const path = edgePath(edge.fromPos, edge.toPos)
                const mid = edgeMid(edge.fromPos, edge.toPos)
                const color = strokeFor(edge.condition)
                const markerKey = edge.condition ?? 'none'
                const active = () => edgeActive(edge)
                // Animate flow into nodes that are currently in progress.
                const flowing = () => statusFor(edge.to).tone === 'warning'
                return (
                  <g style={{ opacity: active() ? 1 : 0.12, transition: 'opacity 0.15s' }}>
                    <path d={path} fill="none" stroke-width="5" class="stroke-neutral-200/70 dark:stroke-neutral-800/70" stroke-linecap="round" />
                    <path
                      d={path}
                      fill="none"
                      stroke={color}
                      stroke-width={active() && hovered() ? 2.75 : 2}
                      stroke-linecap="round"
                      marker-end={`url(#dag-arrow-${markerKey})`}
                      classList={{ 'dag-edge-flow': flowing() }}
                    />
                    <Show when={edge.condition}>
                      <g>
                        <rect x={mid.x - 27} y={mid.y - 9} width="54" height="17" rx="8" fill={color} opacity="0.14" />
                        <text x={mid.x} y={mid.y + 3} text-anchor="middle" font-size="9" font-weight="700" letter-spacing="0.06em" fill={color}>
                          {(edge.condition ?? '').toUpperCase()}
                        </text>
                      </g>
                    </Show>
                  </g>
                )
              }}
            </For>
          </svg>

          {/* Node layer */}
          <For each={[...nodePositions().entries()]}>
            {([taskName, pos]) => {
              const info = () => statusFor(taskName)
              const tone = () => info().tone
              const style = () => toneStyle[tone()]
              const inProgress = () => tone() === 'warning'
              const dimmed = () => {
                const set = neighbors()
                return set ? !set.has(taskName) : false
              }
              const groupColor = () => groupColors().get(pos.task.group) ?? '#64748b'

              return (
                <A
                  href={`/jobs/${encodeURIComponent(props.jobName)}/tasks/${encodeURIComponent(taskName)}`}
                  data-dag-node
                  onMouseEnter={() => setHovered(taskName)}
                  onMouseLeave={() => setHovered(null)}
                  draggable={false}
                  class={`group absolute flex flex-col overflow-hidden rounded-xl border border-neutral-200 shadow-sm ring-1 transition-all duration-150 hover:-translate-y-0.5 hover:shadow-lg dark:border-neutral-700 ${style().ring} ${style().bg}`}
                  style={{
                    left: `${pos.x}px`,
                    top: `${pos.y}px`,
                    width: `${NODE_W}px`,
                    height: `${NODE_H}px`,
                    opacity: dimmed() ? 0.35 : 1,
                    'z-index': hovered() === taskName ? 20 : 1,
                  }}
                >
                  {/* Top accent strip in group color */}
                  <div class="h-1 w-full shrink-0" style={{ 'background-color': groupColor() }} />

                  <div class="flex flex-1 items-stretch">
                    {/* Status bar */}
                    <div class={`w-1.5 shrink-0 ${style().bar} ${inProgress() ? 'animate-pulse' : ''}`} />

                    <div class="flex flex-1 flex-col justify-center gap-1 px-3 py-2 min-w-0">
                      <div class="flex items-center gap-2 min-w-0">
                        <span class={`h-2 w-2 shrink-0 rounded-full ${style().dot} ${inProgress() ? 'animate-pulse' : ''}`} />
                        <span class="truncate text-[13px] font-semibold text-neutral-900 dark:text-neutral-100">{pos.task.name}</span>
                      </div>
                      <div class="flex items-center gap-1.5 text-[10px] text-neutral-500 dark:text-neutral-400 min-w-0">
                        <span class="shrink-0 rounded bg-neutral-100 px-1.5 py-0.5 font-medium dark:bg-neutral-800">{pos.task.kind}</span>
                        <span class="inline-flex shrink-0 items-center gap-0.5" title={`${pos.task.lifecycle} lifecycle`}>
                          {lifecycleGlyph(pos.task.lifecycle)} {pos.task.lifecycle}
                        </span>
                        <span
                          class="ml-auto inline-flex shrink-0 items-center gap-1 truncate font-medium"
                          style={{ color: groupColor() }}
                          title={`group: ${pos.task.group}`}
                        >
                          <span class="h-1.5 w-1.5 rounded-full" style={{ 'background-color': groupColor() }} />
                          {pos.task.group}
                        </span>
                      </div>
                      <div class={`text-[10px] font-semibold uppercase tracking-wide ${style().text}`}>
                        {info().status}
                      </div>
                    </div>
                  </div>
                </A>
              )
            }}
          </For>
        </div>

        {/* Group legend overlay */}
        <Show when={groupColors().size > 1}>
          <div class="pointer-events-none absolute bottom-3 left-3 flex max-w-[70%] flex-wrap gap-x-3 gap-y-1 rounded-lg border border-neutral-200/80 bg-white/85 px-3 py-2 text-[10px] shadow-sm backdrop-blur dark:border-neutral-700/80 dark:bg-neutral-900/85">
            <span class="font-semibold uppercase tracking-wide text-neutral-400">Groups</span>
            <For each={[...groupColors().entries()]}>
              {([group, color]) => (
                <span class="inline-flex items-center gap-1 text-neutral-600 dark:text-neutral-300">
                  <span class="h-2 w-2 rounded-full" style={{ 'background-color': color }} />
                  {group}
                </span>
              )}
            </For>
          </div>
        </Show>

        {/* Zoom indicator */}
        <div class="pointer-events-none absolute bottom-3 right-3 rounded-md border border-neutral-200/80 bg-white/85 px-2 py-1 text-[10px] font-semibold tabular-nums text-neutral-500 shadow-sm backdrop-blur dark:border-neutral-700/80 dark:bg-neutral-900/85 dark:text-neutral-400">
          {Math.round(scale() * 100)}%
        </div>
      </div>
    </div>
  )
}
