import { A } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
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
  selectClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
  trHoverClass,
} from '../components/ui'
import { managerClient } from '../grpcClient'
import { clearCommandPaletteIndexItems, setCommandPaletteIndexItems } from '../utils/commandPalette'
import { formatRelativeTime } from '../utils/format'
import { indexHintKey, useIndexJumpTargets } from '../utils/jumpHints'
import { POLL_INTERVAL_MS } from '../utils/polling'
import {
  driverScope,
  driverScopeTone,
  shortVolumeId,
  volumePinsPlacement,
  volumeStateTone,
} from '../utils/volumes'

export default function Volumes() {
  const [search, setSearch] = createSignal('')
  const [stateFilter, setStateFilter] = createSignal('live')
  const [tick, setTick] = createSignal(0)

  onMount(() => {
    const timer = setInterval(() => setTick((value) => value + 1), POLL_INTERVAL_MS)
    onCleanup(() => clearInterval(timer))
  })

  const [volumes] = createResource(
    () => tick() + 1,
    async () => (await managerClient.listVolumes({})).volumes,
  )
  const [drivers] = createResource(
    () => tick() + 1,
    async () => (await managerClient.listVolumeDrivers({})).drivers,
  )

  const filtered = createMemo(() => {
    const query = search().trim().toLowerCase()
    const state = stateFilter()
    return (volumes() ?? [])
      // Destroyed volumes are kept as tombstones so you can still see where a
      // job's storage went, but they are noise by default.
      .filter((volume) => (state === 'live' ? volume.state !== 'destroyed' : state === 'all' || volume.state === state))
      .filter((volume) =>
        !query ||
        `${volume.volumeId} ${volume.name} ${volume.driver} ${volume.jobId ?? ''} ${volume.instanceId ?? ''} ${volume.nodeId ?? ''}`
          .toLowerCase()
          .includes(query),
      )
  })

  useIndexJumpTargets(() => filtered().map((volume) => `/volumes/${encodeURIComponent(volume.volumeId)}`))

  // ⌘K by volume id or by the instance that owns it — both are things you
  // arrive with from a log line.
  createEffect(() => {
    setCommandPaletteIndexItems(
      (volumes() ?? []).map((volume) => ({
        id: `Volumes:${volume.volumeId}`,
        label: volume.instanceId ? `${volume.instanceId} (${volume.driver})` : shortVolumeId(volume.volumeId),
        subtitle: `${volume.driver} · ${volume.state}`,
        href: `/volumes/${encodeURIComponent(volume.volumeId)}`,
        source: 'Volumes',
      })),
    )
  })
  onCleanup(clearCommandPaletteIndexItems)

  const totals = createMemo(() => {
    const all = (volumes() ?? []).filter((volume) => volume.state !== 'destroyed')
    return {
      total: all.length,
      pinned: all.filter(volumePinsPlacement).length,
      reclaiming: all.filter((volume) => volume.state === 'destroying').length,
      failing: all.filter((volume) => !!volume.lastError).length,
    }
  })

  const unusableDrivers = createMemo(() =>
    (drivers() ?? []).filter((driver) => driver.availableNodes.length === 0),
  )

  return (
    <div class="space-y-4">
      <Show when={volumes.error}>
        <Note tone="danger">Failed to load volumes.</Note>
      </Show>

      <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <StatCard label="Volumes" icon="hardDrive" value={totals().total} hint="excluding tombstones" />
        <StatCard
          label="Pinned"
          icon="server"
          value={totals().pinned}
          hint="instance tied to one node"
          tone={totals().pinned > 0 ? 'warn' : 'neutral'}
        />
        <StatCard
          label="Reclaiming"
          icon="trash"
          value={totals().reclaiming}
          hint="awaiting driver confirmation"
          tone={totals().reclaiming > 0 ? 'warn' : 'neutral'}
        />
        <StatCard
          label="Driver errors"
          icon="alertTriangle"
          value={totals().failing}
          hint="last operation failed"
          tone={totals().failing > 0 ? 'danger' : 'neutral'}
        />
      </div>

      <Show when={unusableDrivers().length > 0}>
        <Note tone="warn">
          {unusableDrivers().length === 1 ? 'One volume driver is' : `${unusableDrivers().length} volume drivers are`}{' '}
          installed but unusable on every node, so jobs claiming{' '}
          {unusableDrivers().length === 1 ? 'it' : 'them'} are rejected at submit:{' '}
          <For each={unusableDrivers()}>
            {(driver, index) => (
              <>
                <span class="font-mono">{driver.name}</span> ({driver.detail}){index() < unusableDrivers().length - 1 ? '; ' : ''}
              </>
            )}
          </For>
        </Note>
      </Show>

      <div class="flex flex-wrap items-center gap-2">
        <div class="relative min-w-0 flex-1">
          <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
          <input
            value={search()}
            onInput={(event) => setSearch(event.currentTarget.value)}
            placeholder="Search by volume, instance, job, node, or driver…"
            class={cx(inputClass, 'pl-8')}
            aria-label="Search volumes"
          />
        </div>
        <select
          value={stateFilter()}
          onChange={(event) => setStateFilter(event.currentTarget.value)}
          class={cx(selectClass, 'w-auto')}
          aria-label="Filter by state"
        >
          <option value="live">Live</option>
          <option value="all">All (incl. destroyed)</option>
          <option value="pending">Pending</option>
          <option value="created">Created</option>
          <option value="destroying">Destroying</option>
          <option value="destroyed">Destroyed</option>
        </select>
      </div>

      <SectionCard flush>
        <Show when={!volumes.loading || (volumes() ?? []).length > 0} fallback={<LoadingRows rows={4} />}>
          <Show
            when={filtered().length > 0}
            fallback={
              <EmptyState
                icon="hardDrive"
                title={search().trim() ? `No volumes match "${search().trim()}"` : 'No volumes'}
                body="A task gets one by declaring `stateful: { storage: { path: '/data' } }`, or claims existing storage with a `volumes` block."
              />
            }
          >
            <TableShell>
              <table class={tableClass}>
                <thead class={theadClass}>
                  <tr>
                    <th class={cx(thClass, jumpHintGutterClass())}>Volume</th>
                    <th class={thClass}>Driver</th>
                    <th class={thClass}>State</th>
                    <th class={thClass}>Owner</th>
                    <th class={thClass}>Node</th>
                    <th class={cx(thClass, 'text-right')}>Updated</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-line">
                  <For each={filtered()}>
                    {(volume, index) => (
                      <tr class={trHoverClass}>
                        <td class={cx(tdClass, 'relative', jumpHintGutterClass())}>
                          <JumpHint keys={indexHintKey(index())} class="absolute left-3 top-1/2 -translate-y-1/2" />
                          <A
                            href={`/volumes/${encodeURIComponent(volume.volumeId)}`}
                            class="font-mono text-xs font-medium text-ink hover:text-brass-strong"
                            title={volume.volumeId}
                          >
                            {shortVolumeId(volume.volumeId)}
                          </A>
                          <div class="text-2xs text-faint">{volume.name}</div>
                        </td>
                        <td class={tdClass}>
                          <Badge tone={volume.nodeLocal ? 'neutral' : 'ok'}>{volume.driver}</Badge>
                          <Show when={volume.volumeMode === 'block'}>
                            <Badge tone="accent" class="ml-1" title="A raw device or disk image, not a directory">
                              block
                            </Badge>
                          </Show>
                        </td>
                        <td class={tdClass}>
                          <Badge tone={volumeStateTone(volume)} dot class="uppercase tracking-wide">
                            {volume.state}
                          </Badge>
                          <Show when={volume.lastError}>
                            <div class="mt-0.5 max-w-[18rem] truncate text-2xs text-red-500" title={volume.lastError}>
                              {volume.lastError}
                            </div>
                          </Show>
                        </td>
                        <td class={tdClass}>
                          <Show when={volume.jobId} fallback={<span class="text-faint">—</span>}>
                            <A href={`/jobs/${encodeURIComponent(volume.jobId!)}`} class="text-muted hover:text-brass-strong">
                              {volume.jobId}
                            </A>
                          </Show>
                          <Show when={volume.instanceId}>
                            <div class="font-mono text-2xs text-faint">{volume.instanceId}</div>
                          </Show>
                        </td>
                        <td class={tdClass}>
                          <Show when={volume.nodeId} fallback={<span class="text-faint">—</span>}>
                            <A href={`/nodes/${encodeURIComponent(volume.nodeId!)}`} class="text-muted hover:text-brass-strong">
                              {volume.nodeId}
                            </A>
                            <Show when={volumePinsPlacement(volume)}>
                              <span
                                class="ml-1 text-brass-strong"
                                title="Node-local: this instance can only be replaced on this node"
                              >
                                *
                              </span>
                            </Show>
                          </Show>
                        </td>
                        <td class={cx(tdClass, 'whitespace-nowrap text-right text-xs text-faint')}>
                          {formatRelativeTime(volume.updatedAt)}
                        </td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </TableShell>
            <Show when={filtered().some(volumePinsPlacement)}>
              <p class="border-t border-line px-4 py-2 text-2xs text-faint">
                <span class="text-brass-strong">*</span> node-local: the owning instance can only be replaced on that node.
              </p>
            </Show>
          </Show>
        </Show>
      </SectionCard>

      <SectionCard
        title="Drivers"
        description="Storage drivers advertised by workers. The manager loads none itself — this is what job admission validates against."
        flush
      >
        <Show when={!drivers.loading || (drivers() ?? []).length > 0} fallback={<LoadingRows rows={3} />}>
          <Show
            when={(drivers() ?? []).length > 0}
            fallback={<EmptyState icon="boxes" title="No drivers advertised" body="Are any workers connected?" />}
          >
            <TableShell>
              <table class={tableClass}>
                <thead class={theadClass}>
                  <tr>
                    <th class={thClass}>Driver</th>
                    <th class={thClass}>Scope</th>
                    <th class={thClass}>Nodes</th>
                    <th class={thClass}>Required params</th>
                    <th class={thClass}>Detail</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-line">
                  <For each={drivers() ?? []}>
                    {(driver) => (
                      <tr class={trHoverClass}>
                        <td class={cx(tdClass, 'font-mono text-xs font-medium')}>{driver.name}</td>
                        <td class={tdClass}>
                          <Badge tone={driverScopeTone(driver)}>{driverScope(driver)}</Badge>
                        </td>
                        <td class={cx(tdClass, 'tabular-nums text-muted')}>
                          {driver.availableNodes.length} / {Math.max(driver.nodes.length, driver.availableNodes.length)}
                          <span class="text-faint"> usable</span>
                        </td>
                        <td class={cx(tdClass, 'font-mono text-xs text-muted')}>
                          {driver.requiredParams.length > 0 ? driver.requiredParams.join(', ') : '—'}
                          <Show when={driver.volumeModes.includes('block')}>
                            <div class="text-2xs text-faint">serves block mode</div>
                          </Show>
                        </td>
                        <td class={cx(tdClass, 'text-xs text-muted')}>{driver.detail}</td>
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
