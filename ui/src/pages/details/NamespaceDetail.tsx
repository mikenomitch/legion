/*
 * One namespace (docs/namespaces.md): what it holds, what its jobs are
 * charged, the namespace policies that govern it with the aggregate each is
 * checked against, and — for identities with `acl:read` — every ACL rule that
 * reaches it, which is the blast radius of creating it.
 */
import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { managerClient } from '../../grpcClient'
import type { BandUsage, NamespacePolicyStatus } from '../../gen/legion_pb'
import { retainOnError, usePoll } from '../../utils/polling'
import {
  Badge,
  DefList,
  EmptyState,
  LoadingRows,
  Meter,
  Note,
  PageHeader,
  RelativeTime,
  SectionCard,
  TableShell,
  linkClass,
  tableClass,
  tdClass,
  thClass,
  theadClass,
} from '../../components/ui'

const fmt = (value: number) => Math.round(value).toLocaleString()

function UsageTable(props: { usage: BandUsage[] }) {
  return (
    <Show
      when={props.usage.length > 0}
      fallback={<p class="px-4 py-3 text-sm text-muted">Nothing charged: no live allocations or declared demand.</p>}
    >
      <TableShell>
        <table class={tableClass}>
          <thead class={theadClass}>
            <tr>
              <th class={thClass}>Band</th>
              <th class={`${thClass} text-right`}>CPU (MHz)</th>
              <th class={`${thClass} text-right`}>Memory (MiB)</th>
              <th class={`${thClass} text-right`}>Cores</th>
              <th class={`${thClass} text-right`}>Live allocations</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-line">
            <For each={props.usage}>
              {(band) => (
                <tr>
                  <td class={`${tdClass} font-mono`}>{band.band}</td>
                  <td class={`${tdClass} text-right tabular-nums`}>{fmt(band.cpu)}</td>
                  <td class={`${tdClass} text-right tabular-nums`}>{fmt(band.memory)}</td>
                  <td class={`${tdClass} text-right tabular-nums`}>{band.cores}</td>
                  <td class={`${tdClass} text-right tabular-nums`}>{String(band.allocations)}</td>
                </tr>
              )}
            </For>
          </tbody>
        </table>
      </TableShell>
    </Show>
  )
}

type Limit = { label: string; used: number; cap: number; unit: string }

/** One line per limit the policy sets, with its aggregate usage. */
const limitsOf = (status: NamespacePolicyStatus): { limits: Limit[]; lists: [string, string][] } => {
  let object: Record<string, unknown> = {}
  try {
    object = JSON.parse(status.policy?.objectJson || '{}')
  } catch {
    object = {}
  }
  const limits: Limit[] = []
  const quota = (object.quota ?? {}) as Record<string, { cpu?: number; memory?: number; cores?: number }>
  for (const [band, caps] of Object.entries(quota)) {
    const used = status.usage.find((usage) => usage.band === band)
    if (caps.cpu !== undefined) limits.push({ label: `${band} CPU`, used: used?.cpu ?? 0, cap: caps.cpu, unit: 'MHz' })
    if (caps.memory !== undefined) limits.push({ label: `${band} memory`, used: used?.memory ?? 0, cap: caps.memory, unit: 'MiB' })
    if (caps.cores !== undefined) limits.push({ label: `${band} cores`, used: used?.cores ?? 0, cap: caps.cores, unit: 'cores' })
  }
  if (typeof object.maxJobs === 'number') limits.push({ label: 'Jobs', used: Number(status.jobs), cap: object.maxJobs, unit: '' })
  if (typeof object.maxNamespaces === 'number')
    limits.push({ label: 'Namespaces', used: Number(status.namespaces), cap: object.maxNamespaces, unit: '' })
  const lists: [string, string][] = []
  const pools = object.pools as { allowed?: string[]; default?: string } | undefined
  if (pools?.allowed) lists.push(['Allowed pools', pools.allowed.join(', ') || 'none'])
  if (pools?.default) lists.push(['Default pool', pools.default])
  if (Array.isArray(object.runtimes)) lists.push(['Allowed runtimes', (object.runtimes as string[]).join(', ') || 'none'])
  if (Array.isArray(object.hostVolumes))
    lists.push(['Allowed host volumes', (object.hostVolumes as string[]).join(', ') || 'none'])
  return { limits, lists }
}

function PolicyCard(props: { status: NamespacePolicyStatus }) {
  const view = createMemo(() => limitsOf(props.status))
  return (
    <SectionCard
      title={props.status.policy?.name}
      description={`Governs ${props.status.policy?.namespaces.join(', ')}; usage is summed over every namespace it matches.`}
    >
      <div class="space-y-3">
        <For each={view().limits}>
          {(limit) => {
            const percent = () => (limit.cap > 0 ? (limit.used / limit.cap) * 100 : limit.used > 0 ? 100 : 0)
            return (
              <div>
                <div class="flex items-baseline justify-between text-xs">
                  <span class="text-muted">{limit.label}</span>
                  <span class="tabular-nums text-ink">
                    {fmt(limit.used)} / {fmt(limit.cap)} {limit.unit}
                  </span>
                </div>
                <Meter percent={percent()} tone={percent() >= 100 ? 'danger' : percent() >= 80 ? 'warn' : 'ok'} label={limit.label} />
              </div>
            )
          }}
        </For>
        <Show when={view().lists.length > 0}>
          <DefList items={view().lists.map(([label, value]) => [label, <span class="font-mono text-xs">{value}</span>])} />
        </Show>
      </div>
    </SectionCard>
  )
}

export default function NamespaceDetail() {
  const params = useParams()
  const name = createMemo(() => decodeURIComponent(params.name))
  const tick = usePoll()
  const [data] = createResource(
    () => [name(), tick()] as const,
    retainOnError(async ([namespace]: readonly [string, number]) => managerClient.getNamespace({ name: namespace }), ([n]) => n),
  )
  const namespace = () => data()?.namespace

  return (
    <div class="space-y-4">
      <PageHeader
        eyebrow={<A href="/namespaces">Namespaces</A>}
        title={<span class="font-mono">{name()}</span>}
        subtitle={namespace()?.description || undefined}
        badges={<Show when={name() === 'default'}><Badge>default</Badge></Show>}
      />
      <Show when={!data.error} fallback={<Note tone="danger">Could not load namespace {name()}.</Note>}>
        <Show when={namespace()} fallback={<LoadingRows rows={4} />}>
          {(ns) => (
            <>
              <SectionCard title="Contents">
                <DefList
                  cols={3}
                  items={[
                    ['Jobs', <A class={linkClass} href={`/jobs?ns=${encodeURIComponent(ns().name)}`}>{ns().counts?.jobs ?? 0}</A>],
                    ['Volumes', ns().counts?.volumes ?? 0],
                    ['Snapshots', ns().counts?.snapshots ?? 0],
                    ['Created', <RelativeTime value={ns().createdAt} />],
                    ...Object.entries(ns().meta).map(([key, value]) => [key, value] as [string, string]),
                  ]}
                />
              </SectionCard>
              <SectionCard
                title="Charged usage"
                description="Per band, each job is charged the larger of what its spec declares and what its live allocations reserve."
                flush
              >
                <UsageTable usage={data()?.usage ?? []} />
              </SectionCard>
              <Show when={(data()?.policies ?? []).length > 0}>
                <h2 class="eyebrow pt-2">Namespace policies</h2>
                <For each={data()?.policies ?? []}>{(status) => <PolicyCard status={status} />}</For>
              </Show>
              <Show when={(data()?.reachingRules ?? []).length > 0}>
                <SectionCard
                  title="ACL rules that reach it"
                  description="Every allow rule whose namespaces selector matches, including through globs written before this namespace existed."
                  flush
                >
                  <TableShell>
                    <table class={tableClass}>
                      <thead class={theadClass}>
                        <tr>
                          <th class={thClass}>Policy</th>
                          <th class={thClass}>Rule</th>
                          <th class={thClass}>Grants</th>
                          <th class={thClass}>Namespaces</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-line">
                        <For each={data()?.reachingRules ?? []}>
                          {(reach) => (
                            <tr>
                              <td class={`${tdClass} font-mono`}>{reach.policy}</td>
                              <td class={`${tdClass} tabular-nums`}>{reach.ruleIndex}</td>
                              <td class={`${tdClass} font-mono text-xs`}>{reach.grants.join(', ')}</td>
                              <td class={`${tdClass} font-mono text-xs`}>{reach.namespaces.join(', ') || '* (every namespace)'}</td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </TableShell>
                </SectionCard>
              </Show>
              <Show when={(data()?.policies ?? []).length === 0 && (data()?.usage ?? []).length === 0}>
                <EmptyState icon="boxes" title="No policy governs this namespace" body="Quotas, pools and runtimes come from namespace policies (`legion namespace policy apply`)." />
              </Show>
            </>
          )}
        </Show>
      </Show>
    </div>
  )
}
