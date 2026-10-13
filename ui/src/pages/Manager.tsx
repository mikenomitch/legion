import { A } from '@solidjs/router'
import { For, Show, createMemo, createResource } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import { Badge, Dot, EmptyState, LoadingRows, Note, RelativeTime, SectionCard, StatusBadge, TableShell, linkClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from '../components/ui'
import type { ManagerMember } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { normalizeManagerTimestamp } from '../utils/managerTime'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'
import type { Tone } from '../utils/status'

const ROLE_TONE: Record<string, Tone> = {
  leader: 'accent',
  follower: 'info',
  learner: 'warn',
  standalone: 'neutral',
}

const ROLE_HINT: Record<string, string> = {
  leader: 'Leads the control lane: commits writes, handles joins and membership changes.',
  follower: 'A voter that replicates the log and can be elected leader.',
  learner: 'A non-voting member still catching up, or a joiner whose promotion has not settled.',
  standalone: 'Raft is off: this is the only manager, with no failover.',
}

// The roster comes from ListManagers, which reads the replicated raft
// membership. It used to come from the manager config table, which only the
// control-lane leader writes, so followers never appeared (spot-check P2-12).
function ManagerContent() {
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [data] = createResource(
    () => tick() + 1,
    retainOnError(async () => managerClient.listManagers({}), () => 'managers'),
  )

  const managers = () => data()?.managers ?? []
  // Replication lag is relative to the furthest member any probe reached.
  const maxApplied = createMemo(() =>
    managers().reduce((max, member) => (member.appliedIndex !== undefined && member.appliedIndex > max ? member.appliedIndex : max), 0n),
  )
  const unreachable = createMemo(() => managers().filter((member) => !member.reachable).length)
  const isRaft = () => data()?.persistenceBackend === 'raft'

  return (
    <div class="space-y-4">
      <Show when={data.error}>
        <Note tone="danger">Failed to load managers.</Note>
      </Show>
      <Show when={unreachable() > 0}>
        <Note tone="warn">
          {unreachable()} of {managers().length} managers did not answer {data()?.answeredBy || 'this manager'} just now. Hover the
          reachability column for the reason.
        </Note>
      </Show>

      <SectionCard
        flush
        title="Managers"
        description={
          isRaft()
            ? `Raft members as seen by ${data()?.answeredBy || 'this manager'}. Reachability is probed live from that manager.`
            : 'Raft is off, so this manager runs alone.'
        }
        actions={
          <A href="/raft" class={linkClass}>
            Raft status
          </A>
        }
      >
        <Show when={data() || !data.loading} fallback={<LoadingRows rows={3} />}>
          <Show
            when={managers().length > 0}
            fallback={<EmptyState icon="network" title="No managers listed" body="The raft engine on this manager is still starting. The list fills in once it has loaded its membership." />}
          >
            <TableShell>
              <table class={tableClass}>
                <thead class={theadClass}>
                  <tr>
                    <th class={thClass}>Manager</th>
                    <th class={thClass}>Role</th>
                    <th class={thClass}>Address</th>
                    <th class={thClass}>Reachable</th>
                    <th class={thClass + ' text-right'}>Term</th>
                    <th class={thClass + ' text-right'}>Applied</th>
                    <th class={thClass + ' text-right'}>Config</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-line">
                  <For each={managers()}>{(member) => <ManagerRow member={member} maxApplied={maxApplied()} />}</For>
                </tbody>
              </table>
            </TableShell>
          </Show>
        </Show>
      </SectionCard>
    </div>
  )
}

function ManagerRow(props: { member: ManagerMember; maxApplied: bigint }) {
  const member = () => props.member
  const lag = () => {
    const applied = member().appliedIndex
    return applied === undefined ? 0n : props.maxApplied - applied
  }
  const hasConfig = () => Boolean(member().configUpdatedAt)
  // Lanes topology only: name the lanes a manager leads when it is not
  // simply "the leader of everything".
  const laneNote = () => {
    const lanes = member().leaderLanes
    if (lanes.length === 0 || (member().role === 'leader' && lanes.length === 1)) return ''
    return `leads ${lanes.join(', ')}`
  }

  return (
    <tr class={trHoverClass}>
      <td class={tdClass}>
        <span class="flex flex-wrap items-center gap-2">
          <Show when={hasConfig()} fallback={<span class="font-mono text-xs font-medium">{member().nodeId}</span>}>
            <A href={`/manager/${encodeURIComponent(member().nodeId)}`} class="font-mono text-xs font-medium hover:text-brass-strong">
              {member().nodeId}
            </A>
          </Show>
          <Show when={member().isSelf}>
            <Badge tone="neutral" title="The manager this UI is talking to">this manager</Badge>
          </Show>
        </span>
      </td>
      <td class={tdClass}>
        <span class="flex flex-wrap items-center gap-2">
          <StatusBadge status={member().role} tone={ROLE_TONE[member().role] ?? 'neutral'} title={ROLE_HINT[member().role]} />
          <Show when={laneNote()}>
            <span class="text-2xs text-faint">{laneNote()}</span>
          </Show>
        </span>
      </td>
      <td class={tdClass}>
        <span class="font-mono text-xs text-muted">{member().advertiseAddr || '—'}</span>
      </td>
      <td class={tdClass}>
        <Show
          when={member().reachable}
          fallback={
            <span class="inline-flex items-center gap-1.5 text-xs text-red-600 dark:text-red-400" title={member().probeError}>
              <Dot tone="danger" /> unreachable
            </span>
          }
        >
          <span class="inline-flex items-center gap-1.5 text-xs text-muted">
            <Dot tone="ok" />
            {member().isSelf ? 'self' : `${member().probeLatencyMs?.toString() ?? '?'} ms`}
          </span>
        </Show>
      </td>
      <td class={tdClass + ' text-right tabular-nums text-muted'}>{member().term?.toString() ?? '—'}</td>
      <td class={tdClass + ' text-right tabular-nums text-muted'}>
        {member().appliedIndex?.toString() ?? '—'}
        <Show when={lag() > 0n}>
          <span class="ml-1.5 text-2xs text-amber-600 dark:text-amber-400" title="Entries behind the furthest member">
            −{lag().toString()}
          </span>
        </Show>
      </td>
      <td class={tdClass + ' whitespace-nowrap text-right text-xs text-faint'}>
        <Show when={hasConfig()} fallback={<span title="A manager publishes its config snapshot when it leads the control lane.">—</span>}>
          <RelativeTime value={normalizeManagerTimestamp(member().configUpdatedAt)} />
        </Show>
      </td>
    </tr>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function Manager() {
  return (
    <AccessGate resource="operator">
      <ManagerContent />
    </AccessGate>
  )
}
