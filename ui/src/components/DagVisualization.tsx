import { A } from '@solidjs/router'
import { For, Show, createMemo } from 'solid-js'
import type { JobTask, TaskRuntimeSummary } from '../utils/jobspec'
import { taskStatusTone } from '../utils/jobspec'
import { DetailBadge } from './DetailPrimitives'

// ── Layout constants ────────────────────────────────────────────────
const NODE_W = 220
const NODE_H = 76
const H_GAP = 100
const V_GAP = 28
const PAD_X = 48
const PAD_Y = 40
const EDGE_LABEL_OFFSET = 6

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

// ── Status helpers ──────────────────────────────────────────────────
const toneToAccent: Record<string, { bar: string; bg: string; ring: string; dot: string }> = {
  success: {
    bar: 'bg-emerald-500',
    bg: 'bg-emerald-50 dark:bg-emerald-950/40',
    ring: 'ring-emerald-400/30 dark:ring-emerald-500/20',
    dot: 'bg-emerald-500',
  },
  warning: {
    bar: 'bg-amber-500',
    bg: 'bg-amber-50 dark:bg-amber-950/40',
    ring: 'ring-amber-400/30 dark:ring-amber-500/20',
    dot: 'bg-amber-500',
  },
  danger: {
    bar: 'bg-rose-500',
    bg: 'bg-rose-50 dark:bg-rose-950/40',
    ring: 'ring-rose-400/30 dark:ring-rose-500/20',
    dot: 'bg-rose-500',
  },
  neutral: {
    bar: 'bg-neutral-400 dark:bg-neutral-600',
    bg: 'bg-neutral-50 dark:bg-neutral-900/60',
    ring: 'ring-neutral-300/30 dark:ring-neutral-600/20',
    dot: 'bg-neutral-400 dark:bg-neutral-500',
  },
}

const conditionColor = (condition?: string): string => {
  if (!condition) return 'stroke-neutral-400 dark:stroke-neutral-500'
  switch (condition) {
    case 'success':
      return 'stroke-emerald-400 dark:stroke-emerald-600'
    case 'complete':
      return 'stroke-sky-400 dark:stroke-sky-600'
    case 'start':
      return 'stroke-amber-400 dark:stroke-amber-600'
    default:
      return 'stroke-neutral-400 dark:stroke-neutral-500'
  }
}

const conditionTextColor = (condition?: string): string => {
  if (!condition) return 'fill-neutral-500 dark:fill-neutral-400'
  switch (condition) {
    case 'success':
      return 'fill-emerald-600 dark:fill-emerald-400'
    case 'complete':
      return 'fill-sky-600 dark:fill-sky-400'
    case 'start':
      return 'fill-amber-600 dark:fill-amber-400'
    default:
      return 'fill-neutral-500 dark:fill-neutral-400'
  }
}

const lifecycleIcon = (lifecycle: string): string => {
  switch (lifecycle) {
    case 'sidecar':
      return '⛓'
    case 'batch':
      return '⧖'
    default:
      return '●'
  }
}

// ── Bezier path builder ─────────────────────────────────────────────
function edgePath(from: NodePos, to: NodePos): string {
  const x1 = from.x + NODE_W
  const y1 = from.y + NODE_H / 2
  const x2 = to.x
  const y2 = to.y + NODE_H / 2
  const dx = Math.abs(x2 - x1)
  const cp = Math.max(dx * 0.45, 40)
  return `M ${x1} ${y1} C ${x1 + cp} ${y1}, ${x2 - cp} ${y2}, ${x2} ${y2}`
}

function edgeMidpoint(from: NodePos, to: NodePos): { x: number; y: number } {
  const x1 = from.x + NODE_W
  const y1 = from.y + NODE_H / 2
  const x2 = to.x
  const y2 = to.y + NODE_H / 2
  return { x: (x1 + x2) / 2, y: (y1 + y2) / 2 }
}

// ── Component ───────────────────────────────────────────────────────
export default function DagVisualization(props: DagVisualizationProps) {
  const taskMap = createMemo(() => new Map(props.tasks.map((t) => [t.name, t])))

  // Compute node positions from topological layers
  const nodePositions = createMemo((): Map<string, NodePos> => {
    const positions = new Map<string, NodePos>()
    const layers = props.layers

    // Find the tallest column to center all columns vertically
    const maxLayerHeight = Math.max(...layers.map((l) => l.length * (NODE_H + V_GAP) - V_GAP))

    for (let col = 0; col < layers.length; col++) {
      const layer = layers[col]
      const layerHeight = layer.length * (NODE_H + V_GAP) - V_GAP
      const yOffset = (maxLayerHeight - layerHeight) / 2

      for (let row = 0; row < layer.length; row++) {
        const taskName = layer[row]
        const task = taskMap().get(taskName)
        if (!task) continue
        positions.set(taskName, {
          x: PAD_X + col * (NODE_W + H_GAP),
          y: PAD_Y + yOffset + row * (NODE_H + V_GAP),
          task,
          stage: col,
        })
      }
    }
    return positions
  })

  // Build edge list
  const edges = createMemo((): Edge[] => {
    const result: Edge[] = []
    const positions = nodePositions()
    for (const task of props.tasks) {
      const toPos = positions.get(task.name)
      if (!toPos) continue
      for (const dep of task.dependsOn) {
        const fromPos = positions.get(dep.task)
        if (!fromPos) continue
        result.push({
          from: dep.task,
          to: task.name,
          condition: dep.condition,
          fromPos,
          toPos,
        })
      }
    }
    return result
  })

  // Canvas size
  const canvasW = createMemo(() => {
    const layers = props.layers
    return PAD_X * 2 + layers.length * (NODE_W + H_GAP) - H_GAP
  })

  const canvasH = createMemo(() => {
    const maxInLayer = Math.max(1, ...props.layers.map((l) => l.length))
    return PAD_Y * 2 + maxInLayer * (NODE_H + V_GAP) - V_GAP
  })

  // Check if there are any dependencies at all
  const hasDependencies = createMemo(() => props.tasks.some((t) => t.dependsOn.length > 0))

  return (
    <div class="space-y-3">
      {/* Legend */}
      <div class="flex flex-wrap items-center gap-x-5 gap-y-1 text-xs text-neutral-500 dark:text-neutral-400">
        <span class="font-semibold uppercase tracking-wide">Legend</span>
        <span class="inline-flex items-center gap-1.5">
          <span class="inline-block h-2.5 w-2.5 rounded-full bg-emerald-500" />
          Running / Healthy
        </span>
        <span class="inline-flex items-center gap-1.5">
          <span class="inline-block h-2.5 w-2.5 rounded-full bg-amber-500" />
          Pending / Starting
        </span>
        <span class="inline-flex items-center gap-1.5">
          <span class="inline-block h-2.5 w-2.5 rounded-full bg-rose-500" />
          Failed / Error
        </span>
        <span class="inline-flex items-center gap-1.5">
          <span class="inline-block h-2.5 w-2.5 rounded-full bg-neutral-400" />
          Not allocated
        </span>
        <Show when={hasDependencies()}>
          <span class="ml-2 border-l border-neutral-300 pl-3 dark:border-neutral-700">
            Edge: dependency condition
          </span>
        </Show>
      </div>

      {/* DAG canvas */}
      <div class="overflow-x-auto rounded-lg border border-neutral-200 bg-neutral-25 dark:border-neutral-800 dark:bg-neutral-950/50">
        <div
          class="relative"
          style={{ width: `${canvasW()}px`, height: `${canvasH()}px`, "min-width": '100%' }}
        >
          {/* SVG edge layer */}
          <svg
            class="absolute inset-0 pointer-events-none"
            width={canvasW()}
            height={canvasH()}
            viewBox={`0 0 ${canvasW()} ${canvasH()}`}
          >
            <defs>
              {/* Arrowhead markers for each condition */}
              <marker id="dag-arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                <path d="M 0 0 L 8 3 L 0 6 Z" class="fill-neutral-400 dark:fill-neutral-500" />
              </marker>
              <marker id="dag-arrow-success" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                <path d="M 0 0 L 8 3 L 0 6 Z" class="fill-emerald-400 dark:fill-emerald-600" />
              </marker>
              <marker id="dag-arrow-complete" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                <path d="M 0 0 L 8 3 L 0 6 Z" class="fill-sky-400 dark:fill-sky-600" />
              </marker>
              <marker id="dag-arrow-start" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                <path d="M 0 0 L 8 3 L 0 6 Z" class="fill-amber-400 dark:fill-amber-600" />
              </marker>
            </defs>

            {/* Stage background columns */}
            <For each={props.layers}>
              {(_layer, colIdx) => {
                const x = PAD_X + colIdx() * (NODE_W + H_GAP) - 12
                return (
                  <rect
                    x={x}
                    y={8}
                    width={NODE_W + 24}
                    height={canvasH() - 16}
                    rx="8"
                    class="fill-neutral-100/60 dark:fill-neutral-800/30"
                  />
                )
              }}
            </For>

            {/* Stage labels */}
            <For each={props.layers}>
              {(_layer, colIdx) => {
                const x = PAD_X + colIdx() * (NODE_W + H_GAP) + NODE_W / 2
                return (
                  <text
                    x={x}
                    y={PAD_Y - 16}
                    text-anchor="middle"
                    class="fill-neutral-400 dark:fill-neutral-500"
                    font-size="11"
                    font-weight="600"
                    letter-spacing="0.08em"
                  >
                    STAGE {colIdx() + 1}
                  </text>
                )
              }}
            </For>

            {/* Edge paths */}
            <For each={edges()}>
              {(edge) => {
                const path = edgePath(edge.fromPos, edge.toPos)
                const mid = edgeMidpoint(edge.fromPos, edge.toPos)
                const markerId = edge.condition ? `dag-arrow-${edge.condition}` : 'dag-arrow'
                return (
                  <g>
                    {/* Shadow/glow for depth */}
                    <path
                      d={path}
                      fill="none"
                      stroke-width="4"
                      class="stroke-neutral-200/60 dark:stroke-neutral-700/40"
                      stroke-linecap="round"
                    />
                    {/* Main edge */}
                    <path
                      d={path}
                      fill="none"
                      stroke-width="2"
                      class={conditionColor(edge.condition)}
                      stroke-linecap="round"
                      marker-end={`url(#${markerId})`}
                    />
                    {/* Condition label */}
                    <Show when={edge.condition}>
                      <rect
                        x={mid.x - 26}
                        y={mid.y - EDGE_LABEL_OFFSET - 8}
                        width="52"
                        height="16"
                        rx="4"
                        class="fill-white dark:fill-neutral-900"
                        opacity="0.9"
                      />
                      <text
                        x={mid.x}
                        y={mid.y - EDGE_LABEL_OFFSET}
                        text-anchor="middle"
                        font-size="9"
                        font-weight="600"
                        letter-spacing="0.05em"
                        class={conditionTextColor(edge.condition)}
                      >
                        {(edge.condition ?? '').toUpperCase()}
                      </text>
                    </Show>
                  </g>
                )
              }}
            </For>
          </svg>

          {/* HTML node layer */}
          <For each={[...nodePositions().entries()]}>
            {([taskName, pos]) => {
              const runtime = () => props.taskRuntime.get(taskName)
              const primaryStatus = () => runtime()?.statuses[0] ?? 'not allocated'
              const tone = () => taskStatusTone(primaryStatus())
              const accent = () => toneToAccent[tone()] ?? toneToAccent.neutral
              const isInProgress = () => tone() === 'warning'

              return (
                <A
                  href={`/jobs/${encodeURIComponent(props.jobName)}/tasks/${encodeURIComponent(taskName)}`}
                  class={`absolute flex overflow-hidden rounded-lg border border-neutral-200 shadow-sm ring-1 transition-all duration-150 hover:shadow-md hover:scale-[1.03] hover:z-10 dark:border-neutral-700 ${accent().ring} ${accent().bg}`}
                  style={{
                    left: `${pos.x}px`,
                    top: `${pos.y}px`,
                    width: `${NODE_W}px`,
                    height: `${NODE_H}px`,
                  }}
                >
                  {/* Left accent bar */}
                  <div class={`w-1.5 shrink-0 ${accent().bar} ${isInProgress() ? 'animate-pulse' : ''}`} />

                  {/* Content */}
                  <div class="flex flex-1 flex-col justify-center gap-1 px-3 py-2 min-w-0">
                    <div class="flex items-center justify-between gap-2">
                      <span class="truncate text-sm font-semibold text-neutral-900 dark:text-neutral-100">
                        {pos.task.name}
                      </span>
                      <DetailBadge tone={tone()}>{primaryStatus()}</DetailBadge>
                    </div>
                    <div class="flex items-center gap-2 text-[11px] text-neutral-500 dark:text-neutral-400">
                      <span class="truncate">{pos.task.kind}</span>
                      <span class="text-neutral-300 dark:text-neutral-600">|</span>
                      <span class="inline-flex items-center gap-0.5">
                        {lifecycleIcon(pos.task.lifecycle)} {pos.task.lifecycle}
                      </span>
                      <span class="text-neutral-300 dark:text-neutral-600">|</span>
                      <span class="truncate">{pos.task.group}</span>
                    </div>
                  </div>
                </A>
              )
            }}
          </For>
        </div>
      </div>

      {/* Dependency list (compact, below graph) */}
      <Show when={hasDependencies()}>
        <div class="flex flex-wrap gap-2 text-xs">
          <For each={edges()}>
            {(edge) => {
              const condCls = () => {
                switch (edge.condition) {
                  case 'success': return 'border-emerald-300 text-emerald-700 dark:border-emerald-700 dark:text-emerald-400'
                  case 'complete': return 'border-sky-300 text-sky-700 dark:border-sky-700 dark:text-sky-400'
                  case 'start': return 'border-amber-300 text-amber-700 dark:border-amber-700 dark:text-amber-400'
                  default: return 'border-neutral-300 text-neutral-600 dark:border-neutral-600 dark:text-neutral-400'
                }
              }
              return (
                <span class={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 ${condCls()}`}>
                  <span class="font-semibold">{edge.from}</span>
                  <span class="text-neutral-400 dark:text-neutral-500">→</span>
                  <span class="font-semibold">{edge.to}</span>
                  <Show when={edge.condition}>
                    <span class="opacity-70">({edge.condition})</span>
                  </Show>
                </span>
              )
            }}
          </For>
        </div>
      </Show>
    </div>
  )
}
