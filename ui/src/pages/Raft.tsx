import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import { managerClient } from '../grpcClient'
import { allows } from '../utils/acl'
import { Badge, Button, CodeBlock, ConfirmButton, DefList, EmptyState, Field, Icon, Note, SectionCard, Spinner, StatusBadge, cx, inputClass } from '../components/ui'

const raftCommands = [
  { label: 'Trigger Election', command: 'trigger-election', destructive: false },
  { label: 'Step Down Leader', command: 'step-down', destructive: true },
  { label: 'Transfer Leader', command: 'transfer-leader', requiresTarget: true, destructive: false },
  { label: 'Add Voter', command: 'add-voter', requiresTarget: true, destructive: false },
  { label: 'Add Learner', command: 'add-learner', requiresTarget: true, destructive: false },
  { label: 'Remove Member', command: 'remove-member', requiresTarget: true, destructive: true },
]

function RaftContent() {
  const [target, setTarget] = createSignal('')
  const [commandResult, setCommandResult] = createSignal('')
  const [joinToken, setJoinToken] = createSignal('')
  const [joinTokenExpiry, setJoinTokenExpiry] = createSignal('')

  const [status, statusActions] = createResource(async () => managerClient.getRaftStatus({}))
  const [history, historyActions] = createResource(async () => managerClient.listRaftHistory({ limit: 100 }))

  const refresh = async () => {
    await Promise.all([statusActions.refetch(), historyActions.refetch()])
  }

  const canRunCommands = createMemo(() => (status()?.status?.enabled ?? false) && allows('operator', 'write'))
  // Only claim "disabled" once the manager has actually answered.
  const raftDisabled = createMemo(() => status() !== undefined && !status()?.status?.enabled)

  const runCommand = async (name: string, needsTarget = false) => {
    const targetNodeId = target().trim()
    if (needsTarget && !targetNodeId) {
      setCommandResult('Target node ID required for this command.')
      return
    }

    const response = await managerClient.issueRaftCommand({
      command: name,
      targetNodeId: targetNodeId || undefined,
      actor: 'ui',
    })
    setCommandResult(response.event?.message ?? 'Command submitted.')
    await refresh()
  }

  const issueJoinToken = async () => {
    const response = await managerClient.getRaftJoinToken({ actor: 'ui' })
    setJoinToken(response.token)
    setJoinTokenExpiry(response.expiresAt)
    await refresh()
  }

  return (
    <div class="space-y-4">
      <Show when={status.error}>
        <Note tone="danger">Failed to load Raft status.</Note>
      </Show>

      <Show when={raftDisabled()}>
        <Note tone="warn">
          <div class="flex items-start gap-2.5">
            <Icon name="alertTriangle" size={16} class="mt-0.5 shrink-0" />
            <div>
              <p class="font-semibold">Raft is disabled — this manager runs standalone.</p>
              <p class="mt-1 text-sm opacity-90">
                There is no leader election and no multi-manager failover: if this manager stops, the
                cluster has no control plane. Everything below is read-only until raft is enabled.
              </p>
              <p class="mt-2 text-sm opacity-90">
                Add a <code class="rounded bg-amber-500/15 px-1 font-mono text-xs">raft</code> block to the
                manager config (<code class="rounded bg-amber-500/15 px-1 font-mono text-xs">nodeId</code>,{' '}
                <code class="rounded bg-amber-500/15 px-1 font-mono text-xs">bootstrap</code>,{' '}
                <code class="rounded bg-amber-500/15 px-1 font-mono text-xs">peers</code>) and restart it — see
                docs/raft-operations.md.
              </p>
            </div>
          </div>
        </Note>
      </Show>

      <div class="grid items-start gap-4 lg:grid-cols-2">
        <SectionCard
          title="Consensus status"
          actions={<StatusBadge status={canRunCommands() ? 'enabled' : 'disabled'} />}
        >
          <Show when={status()?.status} fallback={<Spinner label="Loading status…" />}>
            {(raft) => (
              <DefList
                items={[
                  ['Enabled', <StatusBadge status={raft().enabled ? 'enabled' : 'disabled'} />],
                  ['Role', <StatusBadge status={raft().role || 'unknown'} />],
                  ['Leader', raft().leaderId ? <span class="font-mono text-xs">{raft().leaderId}</span> : <span class="text-faint">—</span>],
                  ['Node', raft().nodeId ? <span class="font-mono text-xs">{raft().nodeId}</span> : <span class="text-faint">—</span>],
                  ['Cluster', raft().clusterId ? <span class="font-mono text-xs">{raft().clusterId}</span> : <span class="text-faint">—</span>],
                  ['Backend', raft().backend || '—'],
                  ['Term', <span class="tabular-nums">{raft().term.toString()}</span>],
                  ['Commit / applied', <span class="tabular-nums">{raft().commitIndex.toString()} / {raft().appliedIndex.toString()}</span>],
                  ['Library', raft().raftLibrary || '—'],
                ]}
              />
            )}
          </Show>
        </SectionCard>

        <SectionCard title="Operator commands" description="Election, failover, and membership operations.">
          {/* Disabled buttons still read as "this might work" — with no
              consensus there is nothing to elect, transfer or add, so say so
              instead of showing seven dead controls. */}
          <Show
            when={!raftDisabled()}
            fallback={
              <EmptyState
                icon="gitBranch"
                title="Nothing to operate on"
                body="Elections, leadership transfer, and membership changes all need a raft cluster. Enable raft on this manager to use them."
              />
            }
          >
            <Field label="Target node ID" hint="Required for transfer/add/remove commands." class="mb-3">
              <input
                value={target()}
                onInput={(event) => setTarget(event.currentTarget.value)}
                class={cx(inputClass, 'font-mono')}
                placeholder="mgr-b"
              />
            </Field>
            <div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
              <For each={raftCommands}>
                {(entry) => (
                  <Show
                    when={entry.destructive}
                    fallback={
                      <Button
                        disabled={!canRunCommands()}
                        onClick={() => void runCommand(entry.command, entry.requiresTarget)}
                        class="justify-start"
                      >
                        {entry.label}
                      </Button>
                    }
                  >
                    <ConfirmButton
                      label={entry.label}
                      icon="alertTriangle"
                      disabled={!canRunCommands()}
                      onConfirm={() => void runCommand(entry.command, entry.requiresTarget)}
                      class="justify-start"
                    />
                  </Show>
                )}
              </For>
              <Button
                variant="accent"
                icon="keySquare"
                disabled={!canRunCommands()}
                onClick={() => void issueJoinToken()}
                class="justify-start"
              >
                Issue Join Token
              </Button>
            </div>
            <Show when={commandResult()}>
              <Note tone="info" class="mt-3" onDismiss={() => setCommandResult('')}>{commandResult()}</Note>
            </Show>
            <Show when={joinToken()}>
              <div class="mt-3 space-y-1.5">
                <p class="eyebrow">Join token <span class="normal-case text-faint">· expires {joinTokenExpiry()}</span></p>
                <CodeBlock code={joinToken()} maxHClass="max-h-24" />
              </div>
            </Show>
          </Show>
        </SectionCard>
      </div>

      <SectionCard title="History" flush>
        <Show when={history.error}>
          <Note tone="danger" class="m-4">Failed to load history.</Note>
        </Show>
        <Show
          when={(history()?.events?.length ?? 0) > 0}
          fallback={<EmptyState icon="gitBranch" title="No raft events yet" body="Elections, failovers, and membership changes will be recorded here." />}
        >
          <ul class="divide-y divide-line">
            <For each={history()?.events ?? []}>
              {(event) => (
                <li class="px-4 py-2.5 text-sm">
                  <div class="flex flex-wrap items-center gap-2">
                    <Badge tone="neutral" mono>#{event.eventId.toString()}</Badge>
                    <StatusBadge status={event.eventType} />
                    <span class="min-w-0 flex-1 text-ink">{event.message}</span>
                    <span class="whitespace-nowrap font-mono text-2xs text-faint">{event.createdAt}</span>
                  </div>
                  <p class="mt-1 text-xs text-faint">
                    actor {event.actor} · target {event.target ?? '—'} · term {event.term.toString()} · leader {event.leaderId ?? '—'}
                  </p>
                </li>
              )}
            </For>
          </ul>
        </Show>
      </SectionCard>
    </div>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function Raft() {
  return (
    <AccessGate resource="operator">
      <RaftContent />
    </AccessGate>
  )
}
