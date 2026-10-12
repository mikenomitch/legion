import { For, Show, createMemo, createSignal, type JSX } from 'solid-js'
import { CompactNodeUsage, NodeUsageCard } from '../components/NodeUsagePanel'
import InstancesTable from '../components/InstancesTable'
import InstancesPanel, { type InstanceListing } from '../components/InstancesPanel'
import EnvelopeCard from '../components/EnvelopeCard'
import SessionNodesCard from '../components/SessionNodesCard'
import { largeSessionListing, listInstancesFrom, sessionIndex, sessionListing, sessionNodesFrom, type SessionIndex } from '../fixtures/sessions'
import { SESSION_PAGE, countMap, createSessionPager, isSession, promoteBlocker } from '../utils/sessions'
import { NodeContentCard } from '../components/NodeContentCard'
import { InstanceEventSummary, PressureExplanation, PressureHistoryCard, SnapshotEventSummary } from '../components/PressureHistory'
import { SnapshotHolders, SnapshotsTable } from '../components/SnapshotsPanel'
import { Button, Card, EmptyState, Note, PageHeader, SectionCard } from '../components/ui'
import {
  emptyNode,
  instanceEvents,
  instances,
  nodeContent,
  olderWorkerNode,
  pass2Snapshots,
  pressureEvents,
  pressuredNode,
  snapshotEvents,
  snapshots,
  unreportedContent,
  unreportedNode,
} from '../fixtures/suspendResume'
import { forkPlan, resumeBlocker, stopBlocker, suspendPlan } from '../utils/instances'
import { snapshotBudgets } from '../utils/usage'
import { GetNodeUsageReply, SnapshotBudget, SuspendedCapacity } from '../gen/legion_pb'

/*
 * Unlisted page (/testing) that renders components from fixture data, so a
 * view that a dev cluster cannot produce (a macOS worker never reports
 * usage, and nothing there runs under memory pressure) can still be looked
 * at. Fixtures live in src/fixtures. Nothing here calls the manager.
 */

function Fixture(props: { title: string; note: string; children: JSX.Element }) {
  return (
    <section class="space-y-2">
      <div>
        <h2 class="font-display text-base font-semibold text-ink">{props.title}</h2>
        <p class="text-xs text-muted">{props.note}</p>
      </div>
      {props.children}
    </section>
  )
}

// The large session listing, built on request: a quarter of a million
// messages takes the browser a moment to make.
const LARGE_SESSIONS = 250_000
const LARGE_NODES = 1200
// Sessions one envelope admits in the large listing, so its busiest nodes are full.
const LARGE_PER_ENVELOPE = 600

/**
 * The job page's paging over a fixture index: the same pager and request
 * (`session_limit: SESSION_PAGE`), answered by listInstancesFrom instead of
 * the manager.
 */
function PagedSessionsPanel(props: { index: SessionIndex; complete: boolean }) {
  const pager = createSessionPager(SESSION_PAGE)
  const reply = createMemo(() => listInstancesFrom(props.index, { sessionLimit: SESSION_PAGE, sessionPageToken: pager.token() }, props.complete))
  const listing = (): InstanceListing => ({
    complete: reply().complete,
    asOf: reply().asOf,
    paused: false,
    refreshing: false,
    onRefresh: () => undefined,
    sessions: {
      count: Number(reply().sessionCount),
      byNode: countMap(reply().sessionsByNode),
      offset: pager.offset(),
      onPage: reply().instances.filter(isSession).length,
      hasPrev: pager.hasPrev(),
      hasNext: Boolean(reply().nextSessionPageToken),
      onPrev: pager.prev,
      onNext: () => pager.next(reply().nextSessionPageToken),
      onFirst: pager.first,
    },
  })
  return <InstancesPanel jobName="agents" instances={reply().instances} toRow={(instance) => ({ instance })} actions={false} listing={listing()} />
}

export default function TestingUtils() {
  const [large, setLarge] = createSignal<SessionIndex>()
  const [buildMs, setBuildMs] = createSignal<number>()
  const buildLarge = () => {
    const started = performance.now()
    setLarge(sessionIndex(largeSessionListing(LARGE_SESSIONS, LARGE_NODES)))
    setBuildMs(Math.round(performance.now() - started))
  }
  const mixed = sessionIndex(sessionListing)
  return (
    <div class="space-y-8">
      <PageHeader
        eyebrow="Testing"
        title="Component fixtures"
        subtitle="Views rendered from fixture data, for shapes a dev cluster cannot produce. Nothing on this page talks to the manager."
      />

      <Fixture title="Worker usage: oversubscribed and under pressure" note="w-03 at ×1.5 with the response acting. The pressure order lists ranked allocations only.">
        <NodeUsageCard usage={pressuredNode} />
      </Fixture>

      <Fixture title="Worker usage: enforcement on, nothing placed" note="A fresh node: reported, response on, empty order.">
        <NodeUsageCard usage={emptyNode} />
      </Fixture>

      <Fixture title="Worker usage: older worker" note="Reports usage but predates the memory pressure response, so status is unknown and there is no order.">
        <NodeUsageCard usage={olderWorkerNode} />
      </Fixture>

      <Fixture title="Worker usage: not reported" note="What a macOS worker, or one with enforcement off, looks like: capacity and reservations only. CPU shows cores, since Apple Silicon reports no clock speed.">
        <NodeUsageCard usage={unreportedNode} />
      </Fixture>

      <Fixture title="Worker usage: older manager" note="GetNodeUsage is Unimplemented, so the worker page keeps its reservation-only card and shows no Usage card.">
        <Note tone="neutral">No Usage card is rendered. See the worker page's "Resource usage" card.</Note>
      </Fixture>

      <Fixture title="Workers index: memory column" note="One cell per shape above, in the same order.">
        <Card>
          <ul class="space-y-2">
            <For each={[pressuredNode, emptyNode, olderWorkerNode, unreportedNode, null]}>
              {(usage) => (
                <li class="flex items-center gap-4 text-sm">
                  <span class="w-24 font-mono text-xs text-muted">{usage?.nodeId ?? 'old manager'}</span>
                  <CompactNodeUsage usage={usage} />
                </li>
              )}
            </For>
          </ul>
        </Card>
      </Fixture>

      <Fixture title="Memory pressure history" note="The job page's card for the batch job (scope job). The worker page uses scope node.">
        <PressureHistoryCard events={pressureEvents} scope="job" />
      </Fixture>

      <Fixture title="Snapshots" note="Every snapshot state, plus a published suspend checkpoint. Actions are omitted.">
        <SectionCard title="Snapshots" flush>
          <SnapshotsTable snapshots={snapshots} />
        </SectionCard>
      </Fixture>

      <Fixture title="Event summaries" note="What the Events page shows above the raw JSON for node.pressure, snapshot.*, and instance.* rows.">
        <Card class="space-y-3 text-xs">
          <For each={pressureEvents}>{(event) => <PressureExplanation event={event} />}</For>
          <For each={snapshotEvents}>{(event) => <SnapshotEventSummary event={event} />}</For>
          <For each={instanceEvents}>
            {(event) => (
              <div>
                <p class="font-mono text-2xs text-faint">{event.kind}</p>
                <InstanceEventSummary event={event} />
              </div>
            )}
          </For>
        </Card>
      </Fixture>

      <Fixture
        title="Instances: suspend, resume, fork"
        note="Active with an idle countdown, restarted after a pressure eviction (last_stop), suspended to local memory, published memory, and a disk checkpoint, resuming, two forks with lineage, and a slot instance. The countdown ticks live."
      >
        <SectionCard title="Instances" flush>
          <InstancesTable rows={instances.map((instance) => ({ instance }))} />
        </SectionCard>
      </Fixture>

      <Fixture title="Instance actions" note="What each row's Suspend, Fork, and Resume would offer for the identity this page runs as. The manager's checkpoint_support decides memory and spread.">
        <Card class="space-y-1 text-xs">
          <For each={instances}>
            {(instance) => {
              const suspend = suspendPlan(instance, instance.jobId)
              const fork = forkPlan(instance, instance.jobId)
              return (
                <p>
                  <span class="mr-2 inline-block w-28 font-mono">{instance.instanceId}</span>
                  <span class="text-muted">
                    suspend: {suspend.blocker ?? `${suspend.defaultKind} (${suspend.choices.map((choice) => `${choice.kind} ${choice.blocker ? 'no' : 'yes'}`).join(', ')})`} · fork:{' '}
                    {fork.blocker ?? `yes, spread ${fork.spreadBlocker ? 'no' : 'yes'}`}
                    {instance.state === 'suspended' ? ` · resume: ${resumeBlocker(instance, instance.jobId) ?? 'yes'}` : ''}
                  </span>
                </p>
              )
            }}
          </For>
        </Card>
      </Fixture>

      <Fixture
        title="Sessions: a mixed listing"
        note="A session job's Instances card: a renamed promotion, a promotion that kept its id, and running and starting sessions in two envelopes on two nodes. Row actions are left off here, since they call the manager; the card under it says what each row would offer."
      >
        <PagedSessionsPanel index={mixed} complete />
        <Card class="space-y-1 text-xs">
          <For each={sessionListing}>
            {(instance) => (
              <p>
                <span class="mr-2 inline-block w-72 truncate align-bottom font-mono">{instance.instanceId}</span>
                <span class="text-muted">
                  promote: {promoteBlocker(instance, instance.jobId) ?? 'yes'} · stop: {stopBlocker(instance, instance.jobId) ?? 'yes'}
                </span>
              </p>
            )}
          </For>
        </Card>
      </Fixture>

      <Fixture
        title="Sessions: a quarter of a million"
        note={`${LARGE_SESSIONS.toLocaleString('en-US')} running sessions over ${LARGE_NODES.toLocaleString('en-US')} nodes, from a partial index, served the way ListInstances pages them: ${SESSION_PAGE} sessions a page with the manager's totals, 100 rows rendered at a time, and the Edge nodes card from a ListSessionNodes reply.`}
      >
        <Show
          when={large()}
          fallback={
            <Card class="flex flex-wrap items-center gap-3 text-sm text-muted">
              <Button size="sm" onClick={buildLarge}>
                Build the listing
              </Button>
              <span>Builds {LARGE_SESSIONS.toLocaleString('en-US')} InstanceInfo messages in this tab.</span>
            </Card>
          }
        >
          {(index) => (
            <>
              <p class="text-xs text-faint">Built and indexed in {buildMs()} ms.</p>
              <PagedSessionsPanel index={index()} complete={false} />
              <SessionNodesCard jobId="agents" taskName="workspace" perEnvelope={LARGE_PER_ENVELOPE} reply={sessionNodesFrom(index(), LARGE_PER_ENVELOPE)} />
            </>
          )}
        </Show>
      </Fixture>

      <Fixture title="Session envelope" note="The allocation page's card for an envelope, with its node's ListSessionNodes entry given rather than read, so nothing here calls the manager.">
        <EnvelopeCard
          allocationId={4101n}
          nodeId="w-17"
          servedTask="workspace"
          perEnvelope={60}
          initial={{ sessions: 3, complete: true, asOf: new Date().toISOString(), node: sessionNodesFrom(mixed, 60).nodes.find((node) => node.nodeId === 'w-17') }}
          reserved={[
            { key: 'cpu', label: 'CPU', value: '30000 MHz' },
            { key: 'memory', label: 'Memory', value: '61440 MiB' },
          ]}
        />
      </Fixture>

      <Fixture title="Snapshots: durability, names, fork and suspend checkpoints" note="Published with two names, a store snapshot whose publish is retrying, a fork checkpoint, a disk suspend checkpoint (size reads in place), and a manual disk snapshot.">
        <SectionCard title="Snapshots" flush>
          <SnapshotsTable snapshots={pass2Snapshots} onRemoveAlias={() => undefined} />
        </SectionCard>
        <Card class="text-xs">
          <p class="eyebrow mb-1.5">Held on (golden)</p>
          <SnapshotHolders snapshot={pass2Snapshots[0]} />
        </Card>
      </Fixture>

      <Fixture title="Content and locality" note="The worker page's card from GetNodeContent: a node holding a warmed published snapshot whose disk, not its snapshots budget, limits new checkpoints, then one with no report.">
        <NodeContentCard
          content={nodeContent}
          budgets={snapshotBudgets(
            new GetNodeUsageReply({
              suspended: new SuspendedCapacity({
                snapshotBudget: new SnapshotBudget({
                  budgetBytes: 50n * 1024n ** 3n,
                  usedBytes: 41n * 1024n ** 3n,
                  pinnedBytes: 12n * 1024n ** 3n,
                  diskFreeBytes: 7n * 1024n ** 3n,
                  reclaimableBytes: 2n * 1024n ** 3n,
                  minFreeDiskBytes: 5n * 1024n ** 3n,
                }),
              }),
            }),
          )}
        />
        <NodeContentCard content={unreportedContent} />
      </Fixture>

      <Card pad={false}>
        <EmptyState icon="boxes" title="Chaos tooling" body="Cluster test helpers will live here." />
      </Card>
    </div>
  )
}
