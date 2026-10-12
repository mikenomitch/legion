import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import { managerClient } from '../grpcClient'
import { allows } from '../utils/acl'
import { Badge, Button, CodeBlock, ConfirmButton, DefList, EmptyState, Field, Icon, Note, RelativeTime, SectionCard, Spinner, StatusBadge, cx, inputClass } from '../components/ui'
import { formatDateTime } from '../utils/format'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'

type RaftCommand = {
  label: string
  command: string
  destructive: boolean
  // Commands that act on another manager open an inline target form instead
  // of running on click; `targetHint` is that form's help line.
  requiresTarget?: boolean
  targetHint?: string
}

const raftCommands: RaftCommand[] = [
  { label: 'Trigger Election', command: 'trigger-election', destructive: false },
  { label: 'Step Down Leader', command: 'step-down', destructive: true },
  {
    label: 'Transfer Leader',
    command: 'transfer-leader',
    requiresTarget: true,
    destructive: false,
    targetHint: 'The voting manager to hand leadership to.',
  },
  {
    label: 'Add Voter',
    command: 'add-voter',
    requiresTarget: true,
    destructive: false,
    targetHint: 'The manager to make a voter. It must be reachable at its configured peer address.',
  },
  {
    label: 'Add Learner',
    command: 'add-learner',
    requiresTarget: true,
    destructive: false,
    targetHint: 'The manager to add as a non-voting learner that replicates the log.',
  },
  {
    label: 'Remove Member',
    command: 'remove-member',
    requiresTarget: true,
    destructive: true,
    targetHint: 'The voter or learner to remove from the cluster.',
  },
]

function RaftContent() {
  const [target, setTarget] = createSignal('')
  // The target-taking command whose form is open, if any.
  const [pending, setPending] = createSignal<RaftCommand>()
  const [commandResult, setCommandResult] = createSignal('')
  const [joinToken, setJoinToken] = createSignal('')
  const [joinTokenExpiry, setJoinTokenExpiry] = createSignal('')

  // Polled: an election or a membership change in progress is exactly what
  // someone on this page is waiting to see.
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [status, statusActions] = createResource(
    () => tick() + 1,
    retainOnError(async () => managerClient.getRaftStatus({}), () => 'status'),
  )
  const [history, historyActions] = createResource(
    () => tick() + 1,
    retainOnError(async () => managerClient.listRaftHistory({ limit: 100 }), () => 'history'),
  )

  const refresh = async () => {
    await Promise.all([statusActions.refetch(), historyActions.refetch()])
  }

  const canRunCommands = createMemo(() => (status()?.status?.enabled ?? false) && allows('operator', 'write'))
  // Only claim "disabled" once the manager has actually answered.
  const raftDisabled = createMemo(() => status() !== undefined && !status()?.status?.enabled)

  // Settled voters other than this manager. With none there is no one to
  // hand leadership to, so Transfer Leader is disabled rather than left to
  // fail. Unknown until the first status reply, which disables nothing.
  const otherVoters = createMemo(() => {
    const raft = status()?.status
    return raft ? raft.voters.filter((id) => id !== raft.nodeId) : undefined
  })
  const singleVoter = createMemo(() => otherVoters()?.length === 0)

  // Ids worth suggesting in the target field, per command.
  const targetSuggestions = createMemo(() => {
    const raft = status()?.status
    const command = pending()?.command
    if (!raft || !command) return []
    const voters = new Set(raft.voters)
    const learners = new Set(raft.learners)
    const known = [...new Set([...raft.voters, ...raft.learners, ...raft.peers])].filter((id) => id !== raft.nodeId)
    switch (command) {
      case 'transfer-leader':
        return otherVoters() ?? []
      case 'add-voter':
        return known.filter((id) => !voters.has(id))
      case 'add-learner':
        return known.filter((id) => !voters.has(id) && !learners.has(id))
      default:
        return known.filter((id) => voters.has(id) || learners.has(id))
    }
  })

  const openTargetForm = (entry: RaftCommand) => {
    if (pending()?.command === entry.command) {
      setPending(undefined)
      return
    }
    setPending(entry)
    setCommandResult('')
    // Prefill the one obvious answer (a two-manager transfer); otherwise
    // keep what was typed, since the same id often feeds several commands.
    const suggestions = targetSuggestions()
    if (suggestions.length === 1 && !target().trim()) setTarget(suggestions[0])
    queueMicrotask(() => targetInput?.focus())
  }

  let targetInput: HTMLInputElement | undefined

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
    if (needsTarget) setPending(undefined)
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
            <div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
              <For each={raftCommands}>
                {(entry) => {
                  const transferBlocked = () => entry.command === 'transfer-leader' && singleVoter()
                  return (
                    <Show
                      when={!entry.requiresTarget}
                      fallback={
                        // Opens the target form below; the form's own button runs it.
                        <Button
                          disabled={!canRunCommands() || transferBlocked()}
                          title={transferBlocked() ? 'This is the only voting manager — there is no one to transfer leadership to.' : undefined}
                          aria-expanded={pending()?.command === entry.command}
                          onClick={() => openTargetForm(entry)}
                          class={cx(
                            'justify-start',
                            pending()?.command === entry.command && 'border-brass/50 bg-brass/10 text-brass-strong',
                          )}
                        >
                          <span class="flex-1 text-left">{entry.label}</span>
                          <Icon name={pending()?.command === entry.command ? 'chevronUp' : 'chevronDown'} size={13} class="text-faint" />
                        </Button>
                      }
                    >
                      <Show
                        when={entry.destructive}
                        fallback={
                          <Button
                            disabled={!canRunCommands()}
                            onClick={() => void runCommand(entry.command)}
                            class="!justify-start"
                          >
                            {entry.label}
                          </Button>
                        }
                      >
                        <ConfirmButton
                          label={entry.label}
                          icon="alertTriangle"
                          disabled={!canRunCommands()}
                          onConfirm={() => void runCommand(entry.command)}
                          class="!justify-start"
                        />
                      </Show>
                    </Show>
                  )
                }}
              </For>
              <Button
                variant="accent"
                icon="keySquare"
                disabled={!canRunCommands()}
                onClick={() => void issueJoinToken()}
                class="!justify-start"
              >
                Issue Join Token
              </Button>
            </div>
            <Show when={singleVoter() && canRunCommands()}>
              <p class="mt-2 text-2xs text-faint">
                Transfer Leader needs a second voting manager; this cluster has one.
              </p>
            </Show>
            <Show when={pending()}>
              {(entry) => (
                <form
                  class="mt-3 rounded-md border border-line bg-surface2/50 p-3"
                  onSubmit={(event) => {
                    event.preventDefault()
                    // Enter must not skip the confirm step of a destructive command.
                    if (!entry().destructive) void runCommand(entry().command, true)
                  }}
                >
                  <Field label={`${entry().label} — target node ID`} hint={entry().targetHint}>
                    <input
                      ref={targetInput}
                      value={target()}
                      onInput={(event) => setTarget(event.currentTarget.value)}
                      onKeyDown={(event) => {
                        if (event.key === 'Escape') setPending(undefined)
                      }}
                      class={cx(inputClass, 'font-mono')}
                      placeholder={targetSuggestions()[0] ?? 'mgr-b'}
                      list="raft-target-suggestions"
                      autocomplete="off"
                      spellcheck={false}
                    />
                  </Field>
                  <datalist id="raft-target-suggestions">
                    <For each={targetSuggestions()}>{(id) => <option value={id} />}</For>
                  </datalist>
                  <div class="mt-2.5 flex items-center justify-end gap-2">
                    <Button variant="ghost" onClick={() => setPending(undefined)}>
                      Cancel
                    </Button>
                    <Show
                      when={entry().destructive}
                      fallback={
                        <Button type="submit" variant="primary" disabled={!canRunCommands() || !target().trim()}>
                          {entry().label}
                        </Button>
                      }
                    >
                      <ConfirmButton
                        label={entry().label}
                        icon="alertTriangle"
                        disabled={!canRunCommands() || !target().trim()}
                        onConfirm={() => void runCommand(entry().command, true)}
                      />
                    </Show>
                  </div>
                </form>
              )}
            </Show>
            <Show when={commandResult()}>
              <Note tone="info" class="mt-3" onDismiss={() => setCommandResult('')}>{commandResult()}</Note>
            </Show>
            <Show when={joinToken()}>
              <div class="mt-3 space-y-1.5">
                <p class="eyebrow">Join token <span class="normal-case text-faint">· expires {formatDateTime(joinTokenExpiry())}</span></p>
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
                    <RelativeTime value={event.createdAt} class="whitespace-nowrap text-2xs text-faint" />
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
