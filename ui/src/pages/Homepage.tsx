import { A } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import { EmptyState, LoadingRows, MonoBadge, Note, SectionCard, StatCard, StatusBadge, TableShell, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../components/ui'
import { managerClient } from '../grpcClient'
import { asDisplay, formatRelativeTime, heartbeatFreshnessHint } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'
import { statusTone } from '../utils/status'
import { POLL_INTERVAL_MS } from '../utils/polling'

export default function Homepage() {
  // Poll while the page is mounted so the dashboard stays live; the previous
  // value is kept during refetches so the tables never flash.
  const [tick, setTick] = createSignal(0)
  onMount(() => {
    const timer = setInterval(() => setTick((value) => value + 1), POLL_INTERVAL_MS)
    onCleanup(() => clearInterval(timer))
  })

  // +1 because a 0 source is falsy to createResource and would skip the first fetch.
  // The overview is reachable by every identity, so each stat degrades to
  // empty when its RPC is denied rather than throwing into the page.
  const [ledger] = createResource(() => tick() + 1, async () => {
    try {
      return (await managerClient.getLedger({})).nodes
    } catch {
      return []
    }
  })
  const [rollouts] = createResource(
    () => tick() + 1,
    async () => {
      try {
        return (await managerClient.listRollouts({ activeOnly: true, page: 1, pageSize: 20 })).rollouts
      } catch {
        return []
      }
    },
  )
  const [services] = createResource(
    () => tick() + 1,
    async () => {
      try {
        return (await managerClient.listServices({})).services
      } catch {
        return []
      }
    },
  )

  const props = { ledger, rollouts }
  const workers = createMemo(() => props.ledger() ?? [])
  const allocations = createMemo(() =>
    workers().flatMap((entry) =>
      entry.allocations.map((allocation) => ({ allocation, nodeId: entry.node?.id ?? '' })),
    ),
  )
  // "Healthy" here means the status maps to the ok tone (ready/active/running/placed/…),
  // reusing the central status vocabulary instead of matching one exact word.
  const readyWorkers = createMemo(
    () => workers().filter((entry) => statusTone(entry.node?.status) === 'ok').length,
  )
  const runningAllocations = createMemo(
    () => allocations().filter(({ allocation }) => statusTone(allocation.status) === 'ok').length,
  )
  const jobCount = createMemo(() => new Set(allocations().map(({ allocation }) => allocation.jobId)).size)
  const activeRollouts = createMemo(() => props.rollouts() ?? [])
  // A service with no healthy endpoint is a cluster-level problem, so it earns
  // a slot on the overview next to workers and allocations.
  const allServices = createMemo(() => services() ?? [])
  const servicesUp = createMemo(() => allServices().filter((service) => service.healthyCount > 0).length)

  return (
    <Show when={props.ledger() !== undefined || !props.ledger.loading} fallback={<LoadingRows rows={6} class="p-0" />}>
      <Show when={!props.ledger.error} fallback={<Note tone="danger">Failed to reach the manager. Check that it is running and that the UI can reach its gRPC-Web endpoint.</Note>}>
        <div class="space-y-5">
          <Show when={props.rollouts.error}>
            <Note tone="danger">Failed to load rollouts.</Note>
          </Show>
          <div class="grid grid-cols-2 gap-3 lg:grid-cols-5">
            <StatCard
              label="Workers"
              icon="server"
              href="/nodes"
              value={
                <>
                  {readyWorkers()}
                  <span class="font-sans text-sm font-normal text-muted"> / {workers().length} up</span>
                </>
              }
              tone={workers().length > 0 && readyWorkers() < workers().length ? 'warn' : 'neutral'}
            />
            <StatCard
              label="Jobs placed"
              icon="layers"
              href="/jobs"
              value={jobCount()}
              hint="jobs with allocations"
            />
            <StatCard
              label="Allocations"
              icon="boxes"
              value={
                <>
                  {runningAllocations()}
                  <span class="font-sans text-sm font-normal text-muted"> / {allocations().length} healthy</span>
                </>
              }
              tone={allocations().length > 0 && runningAllocations() < allocations().length ? 'warn' : 'neutral'}
            />
            <StatCard
              label="Services"
              icon="radio"
              href="/services"
              value={
                <>
                  {servicesUp()}
                  <span class="font-sans text-sm font-normal text-muted"> / {allServices().length} up</span>
                </>
              }
              hint="with a healthy endpoint"
              tone={allServices().length > 0 && servicesUp() < allServices().length ? 'danger' : 'neutral'}
            />
            <StatCard
              label="Active rollouts"
              icon="rocket"
              href="/rollouts"
              value={activeRollouts().length}
              tone={activeRollouts().length > 0 ? 'info' : 'neutral'}
            />
          </div>

          <Show when={activeRollouts().length > 0}>
            <SectionCard title="Active rollouts" flush actions={<A href="/rollouts" class={linkClass}>View all</A>}>
              <TableShell>
                <table class={tableClass}>
                  <thead class={theadClass}>
                    <tr>
                      <th class={thClass}>Rollout</th>
                      <th class={thClass}>Status</th>
                      <th class={thClass}>Target config</th>
                      <th class={thClass + ' text-right'}>Updated</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-line">
                    <For each={activeRollouts()}>
                      {(rollout) => (
                        <tr class={trHoverClass}>
                          <td class={tdClass}>
                            <A href={`/rollouts/${rollout.id}`} class="font-medium hover:text-brass-strong">
                              {asDisplay(rollout.jobId)}
                            </A>
                            <span class="ml-2 text-xs text-faint">#{asDisplay(rollout.id)}</span>
                          </td>
                          <td class={tdClass}><StatusBadge status={rollout.status} /></td>
                          <td class={tdClass}><span class="font-mono text-xs text-muted">{asDisplay(rollout.toJobConfigId)}</span></td>
                          <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>{formatRelativeTime(rollout.updatedAt)}</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </TableShell>
            </SectionCard>
          </Show>

          <SectionCard title="Workers" flush actions={<A href="/nodes" class={linkClass}>View all</A>}>
            <Show
              when={workers().length > 0}
              fallback={
                <EmptyState
                  icon="server"
                  title="No workers connected"
                  body="Join a worker with `legion agent --join <manager>` and it will appear here."
                />
              }
            >
              <TableShell>
                <table class={tableClass}>
                  <thead class={theadClass}>
                    <tr>
                      <th class={thClass}>Worker</th>
                      <th class={thClass}>Status</th>
                      <th class={thClass}>Health</th>
                      <th class={thClass}>Pool</th>
                      <th class={thClass + ' text-right'}>Allocations</th>
                      <th class={thClass + ' text-right'}>Heartbeat</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-line">
                    <For each={workers()}>
                      {(entry) => {
                        const health = summarizeHealthChecks(entry.node?.healthChecks)
                        return (
                          <tr class={trHoverClass}>
                            <td class={tdClass}>
                              <A href={`/nodes/${encodeURIComponent(entry.node?.id ?? '')}`} class="font-medium hover:text-brass-strong">
                                {entry.node?.id ?? 'unknown'}
                              </A>
                            </td>
                            <td class={tdClass}><StatusBadge status={entry.node?.status} /></td>
                            <td class={tdClass}><StatusBadge status={health.label} /></td>
                            <td class={tdClass + ' text-muted'}>{entry.node?.pool || 'default'}</td>
                            <td class={tdClass + ' text-right tabular-nums text-muted'}>{entry.allocations.length}</td>
                            <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}><span title={heartbeatFreshnessHint(entry.node?.heartbeatSource)}>{formatRelativeTime(entry.node?.lastHeartbeat)}</span></td>
                          </tr>
                        )
                      }}
                    </For>
                  </tbody>
                </table>
              </TableShell>
            </Show>
          </SectionCard>

          <SectionCard title="Allocations" flush>
            <Show
              when={allocations().length > 0}
              fallback={
                <EmptyState
                  icon="boxes"
                  title="No allocations placed"
                  body="Submit a job and the scheduler will place its allocations on workers."
                />
              }
            >
              <TableShell>
                <table class={tableClass}>
                  <thead class={theadClass}>
                    <tr>
                      <th class={thClass}>Allocation</th>
                      <th class={thClass}>Job</th>
                      <th class={thClass}>Status</th>
                      <th class={thClass}>Worker</th>
                      <th class={thClass}>Config</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-line">
                    <For each={allocations()}>
                      {({ allocation, nodeId }) => (
                        <tr class={trHoverClass}>
                          <td class={tdClass}>
                            <span class="font-mono text-xs">#{asDisplay(allocation.id)}</span>
                            <Show when={allocation.instanceId}>
                              <MonoBadge title={`Stateful instance ${allocation.instanceId}`} class="ml-2">{allocation.instanceId}</MonoBadge>
                            </Show>
                          </td>
                          <td class={tdClass}>
                            <A href={`/jobs/${encodeURIComponent(allocation.jobId)}`} class="font-medium hover:text-brass-strong">
                              {allocation.jobId}
                            </A>
                          </td>
                          <td class={tdClass}><StatusBadge status={allocation.status} /></td>
                          <td class={tdClass}>
                            <A href={`/nodes/${encodeURIComponent(nodeId)}`} class="text-muted hover:text-brass-strong">{nodeId}</A>
                          </td>
                          <td class={tdClass}><span class="font-mono text-xs text-muted">{asDisplay(allocation.jobConfigId)}</span></td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </TableShell>
            </Show>
          </SectionCard>
        </div>
      </Show>
    </Show>
  )
}
