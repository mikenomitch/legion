import { useParams } from '@solidjs/router'
import { AccessGate } from '../../components/AccessGate'
import { For, Show, createResource } from 'solid-js'
import { CodeBlock, Note, PageHeader, SectionCard, Spinner } from '../../components/ui'
import { managerClient } from '../../grpcClient'

function JobSpecDetailContent() {
  const params = useParams()
  const [data] = createResource(() => params.configId, async (raw) => {
    const id = decodeURIComponent(raw)
    const response = await managerClient.getJobSpecByConfig({ configId: id })
    return { configId: id, submissions: response.submissions }
  })

  return (
    <Show when={!data.loading} fallback={<div class="py-6"><Spinner label="Loading spec…" /></div>}>
      <Show when={!data.error && data()} fallback={<Note tone="danger">Failed to load spec.</Note>}>
        <div>
          <PageHeader eyebrow="Job spec" title={<span class="font-mono">{data()!.configId}</span>} />
          <SectionCard title="Submissions">
            <div class="space-y-3">
              <For each={data()!.submissions}>
                {(submission) => <CodeBlock code={submission.content} label={submission.path} maxHClass="max-h-[520px]" />}
              </For>
            </div>
          </SectionCard>
        </div>
      </Show>
    </Show>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function JobSpecDetail() {
  return (
    <AccessGate resource="job">
      <JobSpecDetailContent />
    </AccessGate>
  )
}
