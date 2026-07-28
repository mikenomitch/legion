import { A } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import { ServiceTags } from '../components/ServiceEndpoints'
import {
  Badge,
  EmptyState,
  Icon,
  JumpHint,
  LoadingRows,
  Note,
  SectionCard,
  StatCard,
  TableShell,
  cx,
  inputClass,
  jumpHintGutterClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
  trHoverClass,
} from '../components/ui'
import { managerClient } from '../grpcClient'
import { clearCommandPaletteIndexItems, setCommandPaletteIndexItems } from '../utils/commandPalette'
import { indexHintKey, useIndexJumpTargets } from '../utils/jumpHints'
import { POLL_INTERVAL_MS } from '../utils/polling'

// Health of a service as a whole: every endpoint healthy, some, or none.
const serviceTone = (healthy: number, total: number) => {
  if (total === 0) return 'warn' as const
  if (healthy === 0) return 'danger' as const
  return healthy === total ? ('ok' as const) : ('warn' as const)
}

const serviceStatus = (healthy: number, total: number) => {
  if (total === 0) return 'no endpoints'
  if (healthy === 0) return 'unavailable'
  return healthy === total ? 'healthy' : 'degraded'
}

export default function Services() {
  const [search, setSearch] = createSignal('')
  const [tick, setTick] = createSignal(0)

  onMount(() => {
    const timer = setInterval(() => setTick((value) => value + 1), POLL_INTERVAL_MS)
    onCleanup(() => clearInterval(timer))
  })

  const [services] = createResource(
    () => tick() + 1,
    async () => (await managerClient.listServices({})).services,
  )

  const filtered = createMemo(() => {
    const query = search().trim().toLowerCase()
    const all = services() ?? []
    if (!query) return all
    return all.filter((service) =>
      `${service.service} ${service.jobId} ${service.taskName} ${service.tags.join(' ')}`
        .toLowerCase()
        .includes(query),
    )
  })

  useIndexJumpTargets(() => filtered().map((service) => `/services/${encodeURIComponent(service.service)}`))

  // Feed ⌘K so a service can be jumped to by name, like jobs and workers.
  createEffect(() => {
    setCommandPaletteIndexItems(
      (services() ?? []).map((service) => ({
        id: `Services:${service.service}`,
        label: service.service,
        subtitle: `${service.healthyCount}/${service.totalCount} healthy`,
        href: `/services/${encodeURIComponent(service.service)}`,
        source: 'Services',
      })),
    )
  })
  onCleanup(clearCommandPaletteIndexItems)

  const totals = createMemo(() => {
    const all = services() ?? []
    return {
      services: all.length,
      healthy: all.reduce((sum, service) => sum + service.healthyCount, 0),
      endpoints: all.reduce((sum, service) => sum + service.totalCount, 0),
      unavailable: all.filter((service) => service.healthyCount === 0).length,
    }
  })

  return (
    <div class="space-y-4">
      <Show when={services.error}>
        <Note tone="danger">Failed to load services.</Note>
      </Show>

      <div class="grid grid-cols-2 gap-3 lg:grid-cols-3">
        <StatCard label="Services" icon="radio" value={totals().services} hint="declared by a task" />
        <StatCard
          label="Healthy endpoints"
          icon="boxes"
          value={
            <>
              {totals().healthy}
              <span class="font-sans text-sm font-normal text-muted"> / {totals().endpoints}</span>
            </>
          }
          tone={totals().endpoints > 0 && totals().healthy < totals().endpoints ? 'warn' : 'neutral'}
        />
        <StatCard
          label="Unavailable"
          icon="alertTriangle"
          value={totals().unavailable}
          hint="no healthy endpoint"
          tone={totals().unavailable > 0 ? 'danger' : 'neutral'}
        />
      </div>

      <div class="relative">
        <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
        <input
          value={search()}
          onInput={(event) => setSearch(event.currentTarget.value)}
          placeholder="Search services…"
          class={cx(inputClass, 'pl-8')}
          aria-label="Search services"
        />
      </div>

      <SectionCard flush>
        <Show when={!services.loading || (services() ?? []).length > 0} fallback={<LoadingRows rows={4} />}>
          <Show
            when={filtered().length > 0}
            fallback={
              <EmptyState
                icon="radio"
                title={search().trim() ? `No services match "${search().trim()}"` : 'No services declared'}
                body="Add a `service` block to a task and its healthy allocations become resolvable endpoints."
              />
            }
          >
            <TableShell>
              <table class={tableClass}>
                <thead class={theadClass}>
                  <tr>
                    <th class={cx(thClass, jumpHintGutterClass())}>Service</th>
                    <th class={thClass}>Status</th>
                    <th class={thClass}>Endpoints</th>
                    <th class={thClass}>Job / task</th>
                    <th class={thClass}>Tags</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-line">
                  <For each={filtered()}>
                    {(service, index) => (
                      <tr class={trHoverClass}>
                        <td class={cx(tdClass, 'relative', jumpHintGutterClass())}>
                          <JumpHint keys={indexHintKey(index())} class="absolute left-3 top-1/2 -translate-y-1/2" />
                          <A href={`/services/${encodeURIComponent(service.service)}`} class="font-medium text-ink hover:text-brass-strong">
                            {service.service}
                          </A>
                        </td>
                        <td class={tdClass}>
                          <Badge tone={serviceTone(service.healthyCount, service.totalCount)} dot class="uppercase tracking-wide">
                            {serviceStatus(service.healthyCount, service.totalCount)}
                          </Badge>
                        </td>
                        <td class={cx(tdClass, 'tabular-nums text-muted')}>
                          {service.healthyCount} / {service.totalCount} healthy
                        </td>
                        <td class={tdClass}>
                          <A href={`/jobs/${encodeURIComponent(service.jobId)}`} class="text-muted hover:text-brass-strong">
                            {service.jobId}
                          </A>
                          <span class="text-faint"> · </span>
                          <A
                            href={`/jobs/${encodeURIComponent(service.jobId)}/tasks/${encodeURIComponent(service.taskName)}`}
                            class="text-muted hover:text-brass-strong"
                          >
                            {service.taskName}
                          </A>
                        </td>
                        <td class={tdClass}>
                          <ServiceTags tags={service.tags} meta={service.meta} />
                        </td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </TableShell>
          </Show>
        </Show>
      </SectionCard>
    </div>
  )
}
