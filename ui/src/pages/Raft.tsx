import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'

const raftCommands = [
  { label: 'Trigger Election', command: 'trigger-election' },
  { label: 'Step Down Leader', command: 'step-down' },
  { label: 'Transfer Leader', command: 'transfer-leader', requiresTarget: true },
  { label: 'Add Voter', command: 'add-voter', requiresTarget: true },
  { label: 'Add Learner', command: 'add-learner', requiresTarget: true },
  { label: 'Remove Member', command: 'remove-member', requiresTarget: true },
]

export default function Raft() {
  const [target, setTarget] = createSignal('')
  const [commandResult, setCommandResult] = createSignal('')
  const [joinToken, setJoinToken] = createSignal('')
  const [joinTokenExpiry, setJoinTokenExpiry] = createSignal('')

  const [status, statusActions] = createResource(async () => managerClient.getRaftStatus({}))
  const [history, historyActions] = createResource(async () => managerClient.listRaftHistory({ limit: 100 }))

  const refresh = async () => {
    await Promise.all([statusActions.refetch(), historyActions.refetch()])
  }

  const canRunCommands = createMemo(() => status()?.status?.enabled ?? false)

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
    <section class="space-y-6">
      <header>
        <h1 class="text-2xl font-semibold">Raft Control Plane</h1>
        <p class="text-sm text-neutral-600 dark:text-neutral-300">Leader election/failover visibility and operator commands.</p>
      </header>

      <Show when={status.error}>
        <p class="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
          Failed to load Raft status.
        </p>
      </Show>

      <div class="grid gap-4 md:grid-cols-2">
        <article class="rounded border border-neutral-200 bg-white p-4 dark:border-neutral-800 dark:bg-neutral-950">
          <h2 class="mb-2 text-sm font-semibold uppercase tracking-wide text-neutral-500">Status</h2>
          <Show when={status()?.status} fallback={<p class="text-sm text-neutral-500">Loading status...</p>}>
            {(raft) => (
              <dl class="grid grid-cols-2 gap-y-2 text-sm">
                <dt class="text-neutral-500">Enabled</dt><dd>{String(raft().enabled)}</dd>
                <dt class="text-neutral-500">Backend</dt><dd>{raft().backend}</dd>
                <dt class="text-neutral-500">Node</dt><dd>{raft().nodeId || '—'}</dd>
                <dt class="text-neutral-500">Cluster</dt><dd>{raft().clusterId || '—'}</dd>
                <dt class="text-neutral-500">Role</dt><dd>{raft().role}</dd>
                <dt class="text-neutral-500">Leader</dt><dd>{raft().leaderId ?? '—'}</dd>
                <dt class="text-neutral-500">Term</dt><dd>{raft().term.toString()}</dd>
                <dt class="text-neutral-500">Commit / Applied</dt><dd>{raft().commitIndex.toString()} / {raft().appliedIndex.toString()}</dd>
                <dt class="text-neutral-500">Library</dt><dd>{raft().raftLibrary || '—'}</dd>
              </dl>
            )}
          </Show>
        </article>

        <article class="rounded border border-neutral-200 bg-white p-4 dark:border-neutral-800 dark:bg-neutral-950">
          <h2 class="mb-2 text-sm font-semibold uppercase tracking-wide text-neutral-500">Operations</h2>
          <label class="mb-3 block text-xs text-neutral-500">
            Target Node ID
            <input
              value={target()}
              onInput={(event) => setTarget(event.currentTarget.value)}
              class="mt-1 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-sm dark:border-neutral-700 dark:bg-neutral-900"
              placeholder="mgr-b"
            />
          </label>
          <div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <For each={raftCommands}>{(entry) => (
              <button
                type="button"
                class="rounded border border-neutral-300 px-2 py-1 text-left text-sm hover:bg-neutral-100 disabled:opacity-50 dark:border-neutral-700 dark:hover:bg-neutral-900"
                disabled={!canRunCommands()}
                onClick={() => void runCommand(entry.command, entry.requiresTarget)}
              >
                {entry.label}
              </button>
            )}</For>
            <button
              type="button"
              class="rounded border border-emerald-300 bg-emerald-50 px-2 py-1 text-left text-sm hover:bg-emerald-100 disabled:opacity-50 dark:border-emerald-900 dark:bg-emerald-950/40"
              disabled={!canRunCommands()}
              onClick={() => void issueJoinToken()}
            >
              Issue Join Token
            </button>
          </div>
          <Show when={commandResult()}>
            <p class="mt-3 rounded bg-neutral-100 px-2 py-1 text-xs dark:bg-neutral-900">{commandResult()}</p>
          </Show>
          <Show when={joinToken()}>
            <div class="mt-3 rounded border border-neutral-200 p-2 text-xs dark:border-neutral-800">
              <p class="font-semibold">Join Token</p>
              <p class="break-all font-mono">{joinToken()}</p>
              <p class="text-neutral-500">expires: {joinTokenExpiry()}</p>
            </div>
          </Show>
        </article>
      </div>

      <article class="rounded border border-neutral-200 bg-white p-4 dark:border-neutral-800 dark:bg-neutral-950">
        <h2 class="mb-3 text-sm font-semibold uppercase tracking-wide text-neutral-500">History</h2>
        <Show when={history.error}>
          <p class="mb-3 rounded bg-red-50 px-2 py-1 text-sm text-red-700 dark:bg-red-950/40 dark:text-red-200">Failed to load history.</p>
        </Show>
        <ul class="space-y-2 text-sm">
          <For each={history()?.events ?? []}>{(event) => (
            <li class="rounded border border-neutral-200 p-2 dark:border-neutral-800">
              <p class="font-mono text-xs text-neutral-500">#{event.eventId.toString()} · {event.createdAt} · {event.eventType}</p>
              <p>{event.message}</p>
              <p class="text-xs text-neutral-500">actor={event.actor} target={event.target ?? '—'} term={event.term.toString()} leader={event.leaderId ?? '—'}</p>
            </li>
          )}</For>
          <Show when={(history()?.events?.length ?? 0) === 0}>
            <li class="text-neutral-500">No events yet.</li>
          </Show>
        </ul>
      </article>
    </section>
  )
}
