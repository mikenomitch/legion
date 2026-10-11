import { For, type JSX } from 'solid-js'
import { CompactNodeUsage, NodeUsageCard } from '../components/NodeUsagePanel'
import InstancesTable from '../components/InstancesTable'
import { NodeContentCard } from '../components/NodeContentCard'
import { InstanceEventSummary, PressureExplanation, PressureHistoryCard, SnapshotEventSummary } from '../components/PressureHistory'
import { SnapshotHolders, SnapshotsTable } from '../components/SnapshotsPanel'
import { Card, EmptyState, Note, PageHeader, SectionCard } from '../components/ui'
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
import { forkPlan, resumeBlocker, suspendPlan } from '../utils/instances'
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

export default function TestingUtils() {
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
