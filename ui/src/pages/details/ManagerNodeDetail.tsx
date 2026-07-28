import { useParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { Show, createResource } from 'solid-js'
import { CodeBlock, Note, PageHeader, SectionCard, Spinner } from '../../components/ui'
import { managerClient } from '../../grpcClient'
import { formatRelativeTime } from '../../utils/format'

function prettyJsonLine(line: string): string {
  try {
    return JSON.stringify(JSON.parse(line), null, 2)
  } catch {
    return line
  }
}

function ManagerNodeDetailContent() {
  const params = useParams()
  const [data] = createResource(() => params.id, async (id) => {
    const response = await managerClient.getManagerConfigs({ query: decodeURIComponent(id), page: 1, pageSize: 1 })
    return response.configs[0]
  })
  const [o11y] = createResource(async () => managerClient.getObservability({ maxLines: 200 }))

  return (
    <Show when={!data.loading} fallback={<div class="py-6"><Spinner label="Loading manager…" /></div>}>
      <Show when={!data.error && data()} fallback={<Note tone="danger">Failed to load manager.</Note>}>
        <div>
          <PageHeader
            eyebrow="Manager"
            title={data()!.nodeId}
            subtitle={<>Configuration snapshot · updated {formatRelativeTime(data()!.updatedAt)}</>}
          />
          <div class="space-y-4">
            <SectionCard title="Config snapshot">
              <CodeBlock code={JSON.stringify(data(), null, 2)} maxHClass="max-h-[520px]" />
            </SectionCard>

            <Show when={o11y()?.o11yEnabled}>
              <SectionCard title="Recent logs" description="Recent OTEL log lines captured by the manager.">
                <div class="max-h-[280px] overflow-auto rounded-md border border-line bg-surface2/70 p-3 font-mono text-xs leading-relaxed">
                  {(o11y()?.logs ?? []).map((line) => (
                    <pre class="whitespace-pre-wrap">{prettyJsonLine(line)}</pre>
                  ))}
                </div>
              </SectionCard>
            </Show>
          </div>
        </div>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function ManagerNodeDetail() {
  return (
    <AccessGate resource="operator">
      <ManagerNodeDetailContent />
    </AccessGate>
  )
}
