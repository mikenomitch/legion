import { A, useParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { AllocationLink } from '../../components/AllocationLink'
import { For, Show, createMemo, createResource, type JSX } from 'solid-js'
import {
  Badge,
  CodeBlock,
  DefList,
  LoadingRows,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  cx,
  linkClass,
} from '../../components/ui'
import { managerClient } from '../../grpcClient'
import { retainOnError, usePoll } from '../../utils/polling'
import { parseJsonMap, volumePinsPlacement, volumeScope, volumeScopeHint, volumeStateTone } from '../../utils/volumes'
import { jobPath } from '../../utils/namespaces'

// One line per state, so the page answers "what is happening to my data right
// now" without sending the reader to the docs.
const STATE_COPY: Record<string, string> = {
  pending:
    'The volume exists as a record but no worker has materialized it yet. Provisioning is lazy — the first mount creates the storage.',
  created: 'A worker has mounted this volume. Its data is on disk and reattaches on every placement of the owning instance.',
  destroying:
    'Queued for reclamation. The owning worker runs the driver’s destroy and confirms; the manager retries every heartbeat until it does.',
  destroyed: 'Reclaimed. The row is kept as a tombstone so you can still see where this storage went.',
}

function VolumeDetailContent() {
  const params = useParams()
  const volumeId = createMemo(() => decodeURIComponent(params.id))
  const tick = usePoll()

  const [data] = createResource(
    () => [volumeId(), tick()] as const,
    retainOnError(
      async ([id]: readonly [string, number]) => {
        const [volume, drivers] = await Promise.all([
          managerClient.getVolume({ volumeId: id }),
          managerClient.listVolumeDrivers({}),
        ])
        // The allocation using it now, through its instance (job:read, like
        // the volume itself). Garnish: an instance that is gone or unreadable
        // just leaves the row out.
        const instanceId = volume.volume?.instanceId
        const instance = instanceId
          ? await managerClient
              .getInstance({ instanceId })
              .then((reply) => reply.instance)
              .catch(() => undefined)
          : undefined
        return { volume: volume.volume, drivers: drivers.drivers, instance }
      },
      ([id]) => id,
    ),
  )

  const volume = () => data()?.volume
  const driver = createMemo(() => data()?.drivers.find((entry) => entry.name === volume()?.driver))
  const params_ = createMemo(() => parseJsonMap(volume()?.params))
  const attrs = createMemo(() => parseJsonMap(volume()?.attrs))

  return (
    <div class="space-y-4">
      <Show when={data.error}>
        <Note tone="danger">Failed to load volume {volumeId()}.</Note>
      </Show>

      <Show when={!data.loading || volume()} fallback={<LoadingRows rows={5} />}>
        <Show when={volume()} fallback={<Note tone="warn">Volume {volumeId()} not found.</Note>}>
          {(vol) => (
            <>
              <PageHeader
                title={<span class="font-mono text-lg">{vol().volumeId}</span>}
                subtitle={
                  <span>
                    Volume <span class="font-mono">{vol().name}</span> · {vol().driver}
                  </span>
                }
                actions={
                  <div class="flex items-center gap-2">
                    <Badge tone={volumeStateTone(vol())} dot class="uppercase tracking-wide">
                      {vol().state}
                    </Badge>
                    <Badge tone={vol().nodeLocal ? 'neutral' : 'ok'} title={volumeScopeHint(vol())}>
                      {volumeScope(vol())}
                    </Badge>
                    <Show when={vol().readOnly}>
                      <Badge tone="warn">read-only</Badge>
                    </Show>
                  </div>
                }
              />

              <Show when={vol().lastError}>
                <Note tone="danger">
                  <span class="font-medium">Last driver error:</span> {vol().lastError}
                </Note>
              </Show>

              <Show when={volumePinsPlacement(vol())}>
                <Note tone="warn">
                  This volume is <span class="font-medium">node-local</span> and lives on{' '}
                  <A href={`/nodes/${encodeURIComponent(vol().nodeId!)}`} class={linkClass}>
                    {vol().nodeId}
                  </A>
                  . Its instance can only be replaced on that node — if the node is gone, the instance waits rather
                  than starting on an empty disk. Run{' '}
                  <code class="rounded bg-surface2 px-1 font-mono text-2xs">
                    legion volume delete --force {vol().volumeId}
                  </code>{' '}
                  to abandon the data and let it place elsewhere.
                </Note>
              </Show>

              <SectionCard title="Overview" description={STATE_COPY[vol().state]}>
                <DefList
                  cols={3}
                  items={[
                    ['Driver', <span class="font-mono text-xs">{vol().driver}</span>],
                    ['Volume name', <span class="font-mono text-xs">{vol().name}</span>],
                    [
                      'Mode',
                      <span title={
                        vol().volumeMode === 'block'
                          ? 'A raw device or disk image the task attaches — what a microVM kind needs.'
                          : 'A host directory the task reads and writes directly.'
                      }>
                        {vol().volumeMode || 'filesystem'}
                        <Show when={vol().fs}>
                          <span class="text-faint"> · {vol().fs}</span>
                        </Show>
                      </span>,
                    ],
                    [
                      'Storage handle',
                      <span class="font-mono text-xs" title="What the instance's storage_handle points at">
                        {vol().driver}/{vol().volumeId}
                      </span>,
                    ],
                    [
                      'Job',
                      vol().jobId ? (
                        <A href={`${jobPath(vol().jobId!)}`} class={linkClass}>
                          {vol().jobId}
                        </A>
                      ) : (
                        <span class="text-faint">—</span>
                      ),
                    ],
                    [
                      'Task',
                      vol().taskName && vol().jobId ? (
                        <A
                          href={`${jobPath(vol().jobId!)}/tasks/${encodeURIComponent(vol().taskName!)}`}
                          class={linkClass}
                        >
                          {vol().taskName}
                        </A>
                      ) : (
                        <span class="text-faint">—</span>
                      ),
                    ],
                    [
                      'Instance',
                      vol().instanceId ? (
                        vol().jobId ? (
                          <A
                            href={`${jobPath(vol().jobId!)}/instances/${encodeURIComponent(vol().instanceId!)}`}
                            class={cx(linkClass, 'font-mono text-xs')}
                          >
                            {vol().instanceId}
                          </A>
                        ) : (
                          <span class="font-mono text-xs">{vol().instanceId}</span>
                        )
                      ) : (
                        <span class="text-faint" title="A task volume claim: shared, with no per-instance lifecycle">
                          shared claim
                        </span>
                      ),
                    ],
                    [
                      'Node',
                      vol().nodeId ? (
                        <A href={`/nodes/${encodeURIComponent(vol().nodeId!)}`} class={linkClass}>
                          {vol().nodeId}
                        </A>
                      ) : (
                        <span class="text-faint">not yet mounted</span>
                      ),
                    ],
                    ...(vol().instanceId
                      ? ([
                          [
                            'Allocation',
                            <Show
                              when={data()?.instance?.currentAllocId}
                              fallback={<span class="text-faint" title="The instance has no running allocation">none running</span>}
                            >
                              {(allocationId) => (
                                <AllocationLink id={allocationId()} job={vol().jobId} variant="inherit" class={linkClass} title="The allocation that mounts this volume now" />
                              )}
                            </Show>,
                          ],
                        ] as Array<[string, JSX.Element]>)
                      : []),
                    ['Created', <RelativeTime value={vol().createdAt} />],
                    ['Updated', <RelativeTime value={vol().updatedAt} />],
                  ]}
                />
                <Show when={vol().mountPath}>
                  <div class="mt-4">
                    <div class="mb-1 text-xs text-faint">Host path (most recent mount)</div>
                    <CodeBlock code={vol().mountPath!} maxHClass="max-h-24" />
                  </div>
                </Show>
              </SectionCard>

              <div class="grid gap-4 lg:grid-cols-2">
                <SectionCard
                  title="Driver params"
                  description="Declared in the jobspec and passed to the driver verbatim."
                >
                  <Show
                    when={params_().length > 0}
                    fallback={<p class="text-sm text-faint">No params declared.</p>}
                  >
                    <DefList
                      cols={1}
                      items={params_().map(([key, value]) => [
                        <span class="font-mono text-xs">{key}</span>,
                        <span class="font-mono text-xs">{value}</span>,
                      ])}
                    />
                  </Show>
                </SectionCard>

                <SectionCard
                  title="Driver metadata"
                  description="Recorded by the plugin on mount and handed back on every later operation."
                >
                  <Show when={attrs().length > 0} fallback={<p class="text-sm text-faint">The driver recorded nothing.</p>}>
                    <DefList
                      cols={1}
                      items={attrs().map(([key, value]) => [
                        <span class="font-mono text-xs">{key}</span>,
                        <span class="font-mono text-xs">{value}</span>,
                      ])}
                    />
                  </Show>
                </SectionCard>
              </div>

              <Show when={driver()}>
                {(info) => (
                  <SectionCard title={`Driver: ${info().name}`} description={info().detail}>
                    <DefList
                      cols={3}
                      items={[
                        ['Scope', info().nodeLocal ? 'node-local' : 'network'],
                        [
                          'Volume modes',
                          <span class="font-mono text-xs">
                            {info().volumeModes.length > 0 ? info().volumeModes.join(', ') : 'filesystem'}
                          </span>,
                        ],
                        [
                          'Optional ops',
                          <span class="font-mono text-xs">
                            {info().ops.length > 0 ? info().ops.join(', ') : '—'}
                          </span>,
                        ],
                        ['Ephemeral', info().ephemeral ? 'yes — data does not survive an unmount' : 'no'],
                        ['Access modes', <span class="font-mono text-xs">{info().accessModes.join(', ')}</span>],
                        [
                          'Required params',
                          <span class="font-mono text-xs">
                            {info().requiredParams.length > 0 ? info().requiredParams.join(', ') : '—'}
                          </span>,
                        ],
                        [
                          'Optional params',
                          <span class="font-mono text-xs">
                            {info().optionalParams.length > 0 ? info().optionalParams.join(', ') : '—'}
                          </span>,
                        ],
                        [
                          'Usable on',
                          <span class={cx(info().availableNodes.length === 0 && 'text-red-500')}>
                            {info().availableNodes.length} of {Math.max(info().nodes.length, info().availableNodes.length)}{' '}
                            node(s)
                          </span>,
                        ],
                      ]}
                    />
                    <Show when={info().nodes.length > 0}>
                      <div class="mt-3 flex flex-wrap gap-1.5">
                        <For each={info().nodes}>
                          {(node) => (
                            <A href={`/nodes/${encodeURIComponent(node)}`}>
                              <Badge tone={info().availableNodes.includes(node) ? 'ok' : 'danger'}>{node}</Badge>
                            </A>
                          )}
                        </For>
                      </div>
                    </Show>
                  </SectionCard>
                )}
              </Show>
            </>
          )}
        </Show>
      </Show>
    </div>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function VolumeDetail() {
  return (
    <AccessGate resource="job">
      <VolumeDetailContent />
    </AccessGate>
  )
}
