import { Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { Button, Field, Note, SectionCard, inputClass, textareaClass } from '../components/ui'
import { managerClient } from '../grpcClient'
import { allows, canWriteAny } from '../utils/acl'
import { debounced } from '../utils/debounce'
import { formatRelativeTime } from '../utils/format'

const errorText = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)

const STARTER_SOURCE = `{
  description: 'What this template runs.',
  variables: [
    { key: 'name', type: 'string', label: 'Job name suffix' },
    { key: 'replicas', type: 'number', default: 1 },
  ],
  job(vars):: {
    name: 'example-' + vars.name,
    tasks: [{
      name: 'main',
      runtime: 'exec',
      count: vars.replicas,
      config: { command: ['echo', 'hello from ' + vars.name] },
    }],
  },
}
`

export default function Templates() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const searchQuery = debounced(search)

  createEffect(() => {
    search()
    sort()
    setPage(1)
  })

  const [data, { refetch }] = createResource(
    () => ({ search: searchQuery() }),
    async (input) => managerClient.listJobTemplates({ query: input.search || undefined }),
  )

  const sorted = createMemo(() => {
    const templates = [...(data()?.templates ?? [])]
    if (sort() === 'name:asc') templates.sort((a, b) => a.name.localeCompare(b.name))
    // updated_at:desc is the server's order already.
    return templates
  })
  const pageItems = createMemo(() =>
    sorted().slice((page() - 1) * pageSize(), page() * pageSize()),
  )

  // New-template editor, shown only to identities holding template:write.
  const [newName, setNewName] = createSignal('')
  const [newSource, setNewSource] = createSignal(STARTER_SOURCE)
  const [creating, setCreating] = createSignal(false)
  const [createError, setCreateError] = createSignal('')
  const [createNotice, setCreateNotice] = createSignal('')

  const createTemplate = async (event: Event) => {
    event.preventDefault()
    setCreateError('')
    setCreateNotice('')
    setCreating(true)
    try {
      const reply = await managerClient.upsertJobTemplate({
        name: newName().trim(),
        sourceJsonnet: newSource(),
      })
      const stored = reply.template
      setCreateNotice(
        stored ? `Stored ${stored.name} v${stored.version}.` : 'Template stored.',
      )
      setNewName('')
      await refetch()
    } catch (error) {
      setCreateError(errorText(error))
    } finally {
      setCreating(false)
    }
  }

  return (
    <div class="space-y-6">
      <SharedIndex
        entityLabel="Templates"
        search={search()}
        onSearch={setSearch}
        sort={sort()}
        onSort={setSort}
        sortOptions={[
          { label: 'Updated (newest)', value: 'updated_at:desc' },
          { label: 'Name (A-Z)', value: 'name:asc' },
        ]}
        hideStatus
        columns={[
          { key: 'version', header: 'Latest', class: 'text-right' },
          { key: 'variables', header: 'Variables', class: 'text-right' },
        ]}
        loading={data.loading}
        error={data.error ? 'Failed to load templates.' : undefined}
        items={pageItems().map((template) => ({
          id: template.name,
          title: template.name,
          subtitle: template.description,
          updatedAt: formatRelativeTime(template.updatedAt),
          href: `/templates/${encodeURIComponent(template.name)}`,
          cells: {
            version: <span class="tabular-nums">v{template.latestVersion}</span>,
            variables: <span class="tabular-nums">{template.variableCount}</span>,
          },
        }))}
        page={page()}
        pageSize={pageSize()}
        totalCount={sorted().length}
        onPageChange={setPage}
        onPageSizeChange={(size) => {
          setPageSize(size)
          setPage(1)
        }}
        emptyLabel="No templates yet"
        emptyHint="Publish one with `legion template create <name> -f template.jsonnet` or the editor below."
      />

      <Show when={canWriteAny('template')}>
        <SectionCard
          title="New template"
          description="One self-contained Jsonnet file: metadata, typed variables, and a hidden job(vars):: function."
        >
          <form class="flex flex-col gap-3" onSubmit={createTemplate}>
            <Field label="Name" hint="Lowercase letters, digits, and dashes.">
              <input
                class={inputClass}
                value={newName()}
                onInput={(event) => setNewName(event.currentTarget.value)}
                placeholder="render-video"
                required
              />
            </Field>
            <Field label="Jsonnet source">
              <textarea
                class={`${textareaClass} h-72 font-mono text-xs`}
                value={newSource()}
                onInput={(event) => setNewSource(event.currentTarget.value)}
                spellcheck={false}
              />
            </Field>
            <div class="flex items-center gap-3">
              <Button type="submit" variant="primary" size="md" disabled={creating() || !allows('template', 'write', { template: newName().trim() || undefined, any: !newName().trim() })}>
                {creating() ? 'Publishing…' : 'Publish version'}
              </Button>
              <Show when={createError()}>
                <Note tone="danger" class="flex-1">{createError()}</Note>
              </Show>
              <Show when={createNotice()}>
                <Note tone="ok" class="flex-1">{createNotice()}</Note>
              </Show>
            </div>
          </form>
        </SectionCard>
      </Show>
    </div>
  )
}
