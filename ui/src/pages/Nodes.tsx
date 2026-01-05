import { For, Show, type Resource } from 'solid-js'
import type { NodeMsg } from '../gen/legion_pb'

interface NodesProps {
  nodes: Resource<readonly NodeMsg[] | undefined>
}

export default function Nodes(props: NodesProps) {
  return (
    <Show when={!props.nodes.loading} fallback={<p class="text-neutral-500">Loading nodes…</p>}>
      <Show when={!props.nodes.error} fallback={<p class="text-red-500">Failed to load nodes.</p>}>
        {props.nodes() && props.nodes()!.length > 0 ? (
          <div class="grid gap-3 md:grid-cols-2">
            <For each={props.nodes()!}>
              {(node) => (
                <article class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] transition dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
                  <div class="flex items-start justify-between">
                    <div>
                      <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Node</p>
                      <p class="text-lg font-semibold text-neutral-900 dark:text-neutral-100">{node.id}</p>
                      <p class="text-sm text-neutral-500 dark:text-neutral-400">Pool: {node.pool || '—'}</p>
                    </div>
                    <span class="rounded border border-neutral-300 px-2 py-1 text-[11px] uppercase tracking-wide text-neutral-800 dark:border-neutral-700 dark:text-neutral-200">
                      {node.status}
                    </span>
                  </div>
                  <div class="mt-3 space-y-1 text-sm text-neutral-700 dark:text-neutral-300">
                    <p class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-950/60">
                      Availability: {node.availability || '—'}
                    </p>
                    <p class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-950/60">
                      Config: {node.configAttrs || '—'}
                    </p>
                    <p class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-950/60">
                      Fingerprint: {node.fingerprintAttrs || '—'}
                    </p>
                    <p class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-950/60">
                      Dynamic: {node.dynamicAttrs || '—'}
                    </p>
                    <p class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-950/60">
                      Last heartbeat: {node.lastHeartbeat || '—'}
                    </p>
                  </div>
                </article>
              )}
            </For>
          </div>
        ) : (
          <p class="text-neutral-500">No nodes discovered yet.</p>
        )}
      </Show>
    </Show>
  )
}
