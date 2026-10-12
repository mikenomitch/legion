import { A } from '@solidjs/router'
import { LoginGate } from '../components/AccessGate'
import { For, Show, createMemo, createResource } from 'solid-js'
import { Badge, EmptyState, LoadingRows, MonoBadge, Note, RelativeTime, SectionCard, StatCard, StatusBadge, TableShell, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../components/ui'
import { managerClient } from '../grpcClient'
import { asDisplay, heartbeatFreshnessHint } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'
import { statusTone, type Tone } from '../utils/status'
import { retainOnError, swallowDenied, usePoll } from '../utils/polling'

const eventTone = (level: string): Tone => (statusTone(level) === 'info' ? 'neutral' : statusTone(level))

function HomepageContent() {
  // Poll while the page is mounted so the dashboard stays live; the previous
  // value is kept during refetches so the tables never flash.
  const tick = usePoll()

  // +1 because a 0 source is falsy to createResource and would skip the first fetch.
  // The overview is reachable by every identity, so each stat degrades to
  // empty when its RPC is denied rather than throwing into the page; an
  // outage keeps the last numbers up (the connection banner reports it).
  const [ledger] = createResource(
    () => tick() + 1,
    retainOnError(
      swallowDenied(async () => (await managerClient.getLedger({})).nodes, []),
      () => 'ledger',
    ),
  )
  const [rollouts] = createResource(
    () => tick() + 1,
    retainOnError(
      swallowDenied(async () => (await managerClient.listRollouts({ activeOnly: true, page: 1, pageSize: 20 })).rollouts, []),
      () => 'rollouts',
    ),
  )
  const [services] = createResource(
    () => tick() + 1,
    retainOnError(
      swallowDenied(async () => (await managerClient.listServices({})).services, []),
      () => 'services',
    ),
  )
  // The last few scheduler events give the overview a pulse: a placement, a
  // failed allocation, a node that went quiet. operator:read garnish.
  const [events] = createResource(
    () => tick() + 1,
    retainOnError(
      swallowDenied(async () => (await managerClient.listEvents({ limit: 8 })).events.slice().reverse(), []),
      () => 'events',
    ),
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
  const recentEvents = createMemo(() => events() ?? [])

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
                          <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}><RelativeTime value={rollout.updatedAt} /></td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </TableShell>
            </SectionCard>
          </Show>

          <div class="grid gap-5 2xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
            <SectionCard title="Workers" flush actions={<A href="/nodes" class={linkClass}>View all</A>}>
              <Show
                when={workers().length > 0}
                fallback={
                  <EmptyState
                    icon="server"
                    title="No workers connected"
                    body="Create a join token on the Workers page, run the printed `legion join <token>` on a host, and it will appear here."
                  >
                    <A href="/nodes" class={`${linkClass} mt-1 text-xs`}>Open Workers</A>
                  </EmptyState>
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
                                <A href={`/nodes/${encodeURIComponent(entry.node?.id ?? '')}`} class="whitespace-nowrap font-medium hover:text-brass-strong">
                                  {entry.node?.id ?? 'unknown'}
                                </A>
                                <Show when={entry.node?.availability && entry.node.availability !== 'available'}>
                                  <Badge tone="warn" class="ml-2 uppercase tracking-wide">{entry.node!.availability}</Badge>
                                </Show>
                              </td>
                              <td class={tdClass}><StatusBadge status={entry.node?.status} /></td>
                              <td class={tdClass}><StatusBadge status={health.label} tone={health.tone} /></td>
                              <td class={tdClass + ' text-muted'}>{entry.node?.pool || 'default'}</td>
                              <td class={tdClass + ' text-right tabular-nums text-muted'}>{entry.allocations.length}</td>
                              <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>
                                <RelativeTime value={entry.node?.lastHeartbeat} title={heartbeatFreshnessHint(entry.node?.heartbeatSource)} />
                              </td>
                            </tr>
                          )
                        }}
                      </For>
                    </tbody>
                  </table>
                </TableShell>
              </Show>
            </SectionCard>

            <SectionCard title="Recent activity" flush actions={<A href="/events" class={linkClass}>Live stream</A>}>
              <Show
                when={recentEvents().length > 0}
                fallback={
                  <EmptyState
                    icon="activity"
                    title="Nothing has happened yet"
                    body="Scheduler events — placements, failures, node changes — show up here as they occur."
                  />
                }
              >
                <ul class="divide-y divide-line">
                  <For each={recentEvents()}>
                    {(event) => (
                      <li class="flex items-start gap-2.5 px-4 py-2 text-sm">
                        <Badge tone={eventTone(event.level)} class="mt-0.5 shrink-0 uppercase">{event.level}</Badge>
                        <span class="min-w-0 flex-1">
                          <span class="block truncate text-ink" title={event.message}>{event.message}</span>
                          <span class="block truncate font-mono text-2xs text-faint">
                            {event.kind}
                            <Show when={event.jobId}>
                              {' · '}
                              <A href={`/jobs/${encodeURIComponent(event.jobId!)}`} class="hover:text-brass-strong">{event.jobId}</A>
                            </Show>
                            <Show when={event.nodeId}>
                              {' · '}
                              <A href={`/nodes/${encodeURIComponent(event.nodeId!)}`} class="hover:text-brass-strong">{event.nodeId}</A>
                            </Show>
                          </span>
                        </span>
                        <RelativeTime value={event.createdAt} class="shrink-0 whitespace-nowrap text-xs text-faint" />
                      </li>
                    )}
                  </For>
                </ul>
              </Show>
            </SectionCard>
          </div>

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

// Logged out on a cluster that enforces ACLs, every card here would be empty;
// show the same log-in prompt as the other pages instead.
export default function Homepage() {
  return (
    <LoginGate what="the overview">
      <HomepageContent />
    </LoginGate>
  )
}
