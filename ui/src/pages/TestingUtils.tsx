import { For, type JSX } from 'solid-js'
import { CompactNodeUsage, NodeUsageCard } from '../components/NodeUsagePanel'
import { PressureExplanation, PressureHistoryCard, SnapshotEventSummary } from '../components/PressureHistory'
import { SnapshotsTable } from '../components/SnapshotsPanel'
import { Card, EmptyState, Note, PageHeader, SectionCard } from '../components/ui'
import { emptyNode, olderWorkerNode, pressureEvents, pressuredNode, snapshotEvents, snapshots, unreportedNode } from '../fixtures/suspendResume'

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

      <Fixture title="Worker usage: not reported" note="What a macOS worker, or one with enforcement off, looks like: capacity and reservations only.">
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

      <Fixture title="Snapshots" note="Every state the manager sends today, plus a row with values a later manager will send (purpose suspend, state published). Actions are omitted.">
        <SectionCard title="Snapshots" flush>
          <SnapshotsTable snapshots={snapshots} />
        </SectionCard>
      </Fixture>

      <Fixture title="Event summaries" note="What the Events page shows above the raw JSON for node.pressure and snapshot.* rows.">
        <Card class="space-y-3 text-xs">
          <For each={pressureEvents}>{(event) => <PressureExplanation event={event} />}</For>
          <For each={snapshotEvents}>{(event) => <SnapshotEventSummary event={event} />}</For>
        </Card>
      </Fixture>

      <Card pad={false}>
        <EmptyState icon="boxes" title="Chaos tooling" body="Cluster test helpers will live here." />
      </Card>
    </div>
  )
}
