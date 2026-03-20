import { For, Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import type { TemplateVariableDefinition } from '../gen/legion_pb'

const inputForType = (kind: string) => {
  switch (kind) {
    case 'text':
      return 'textarea'
    case 'toggle':
      return 'toggle'
    case 'multi_select':
      return 'multi'
    case 'number':
      return 'number'
    case 'code':
      return 'code'
    case 'string':
    default:
      return 'string'
  }
}

export default function Templates() {
  const [query, setQuery] = createSignal('')
  const [selectedTemplateId, setSelectedTemplateId] = createSignal<string>('')
  const [selectedVersion, setSelectedVersion] = createSignal<number | undefined>(undefined)
  const [uploadTemplateId, setUploadTemplateId] = createSignal('')
  const [sourceJsonnet, setSourceJsonnet] = createSignal(`/* legion-template
{
  "name": "Demo template",
  "description": "Example template manifest embedded in source",
  "variables": [
    { "key": "job_name", "label": "Job name", "type": "string", "required": true, "description": "Unique job identifier" },
    { "key": "replicas", "label": "Replicas", "type": "number", "required": true, "defaultValue": "2" },
    { "key": "enable_debug", "label": "Enable debug", "type": "toggle", "required": false, "defaultValue": "false" }
  ]
}
*/
{
  name: std.extVar('job_name'),
  tasks: {
    app: {
      kind: 'noop',
      count: std.parseInt(std.extVar('replicas')),
      config: {
        debug: std.extVar('enable_debug') == 'true',
      },
    },
  },
}
`)
  const [uploadMessage, setUploadMessage] = createSignal('')
  const [submitMessage, setSubmitMessage] = createSignal('')
  const [variableValues, setVariableValues] = createSignal<Record<string, string>>({})

  const [templates, { refetch: refetchTemplates }] = createResource(
    () => query(),
    async (currentQuery) => managerClient.listTemplates({ query: currentQuery.trim() || undefined }),
  )

  const [templateVersion] = createResource(
    () => ({ id: selectedTemplateId(), version: selectedVersion() }),
    async (input) => {
      if (!input.id) return undefined
      return managerClient.getTemplateVersion({ templateId: input.id, version: input.version })
    },
  )

  const currentTemplate = createMemo(() => templateVersion()?.template)

  const resetVariableValues = (variables: readonly TemplateVariableDefinition[]) => {
    const next: Record<string, string> = {}
    for (const variable of variables) {
      next[variable.key] = variable.defaultValue ?? ''
    }
    setVariableValues(next)
  }

  createEffect(() => {
    const template = currentTemplate()
    if (!template) return
    resetVariableValues(template.variables)
  })

  const handleUpload = async () => {
    setUploadMessage('')
    try {
      const response = await managerClient.upsertTemplate({
        templateId: uploadTemplateId().trim() || undefined,
        sourceJsonnet: sourceJsonnet(),
      })
      const template = response.template
      if (template) {
        setSelectedTemplateId(template.templateId)
        setSelectedVersion(template.version)
      }
      setUploadMessage('Template uploaded successfully.')
      await refetchTemplates()
    } catch (error) {
      setUploadMessage(`Upload failed: ${String(error)}`)
    }
  }

  const handleSelect = async (templateId: string, version?: number) => {
    setSelectedTemplateId(templateId)
    setSelectedVersion(version)
    setSubmitMessage('')
  }

  const handleSubmitTemplate = async () => {
    const template = currentTemplate()
    if (!template) return
    setSubmitMessage('')
    try {
      const response = await managerClient.submitTemplateJob({
        templateId: template.templateId,
        version: template.version,
        variables: Object.entries(variableValues()).map(([key, value]) => ({ key, value })),
        forceReschedule: false,
      })
      setSubmitMessage(response.message || 'Template submitted.')
    } catch (error) {
      setSubmitMessage(`Submit failed: ${String(error)}`)
    }
  }

  return (
    <div class="grid gap-4 xl:grid-cols-[300px,minmax(0,1fr)]">
      <section class="rounded border border-neutral-200 bg-white/80 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
        <h2 class="text-sm font-semibold uppercase tracking-wide text-neutral-500">Template Library</h2>
        <input
          class="mt-3 w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-950"
          placeholder="Search templates"
          value={query()}
          onInput={(event) => setQuery(event.currentTarget.value)}
        />
        <div class="mt-3 space-y-2">
          <For each={templates()?.templates ?? []}>
            {(template) => (
              <button
                type="button"
                class={`w-full rounded border px-3 py-2 text-left text-sm ${selectedTemplateId() === template.templateId ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900' : 'border-neutral-200 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300'}`}
                onClick={() => handleSelect(template.templateId, template.latestVersion)}
              >
                <p class="font-semibold">{template.name}</p>
                <p class="text-xs opacity-80">{template.templateId} · v{template.latestVersion}</p>
                <Show when={template.description}><p class="mt-1 text-xs opacity-90">{template.description}</p></Show>
              </button>
            )}
          </For>
        </div>
      </section>

      <section class="space-y-4">
        <article class="rounded border border-neutral-200 bg-white/80 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
          <h2 class="text-base font-semibold">Upload Template</h2>
          <p class="mt-1 text-sm text-neutral-600 dark:text-neutral-300">
            Upload unparsed Jsonnet with a <code>/* legion-template {'{...}'} */</code> manifest so UI metadata and variables stay in one file.
          </p>
          <div class="mt-3 grid gap-3 md:grid-cols-[240px,minmax(0,1fr)]">
            <label class="text-sm">
              <span class="mb-1 block text-neutral-600 dark:text-neutral-300">Template ID (optional)</span>
              <input
                class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-950"
                value={uploadTemplateId()}
                onInput={(event) => setUploadTemplateId(event.currentTarget.value)}
                placeholder="team-service-deploy"
              />
            </label>
            <label class="text-sm">
              <span class="mb-1 block text-neutral-600 dark:text-neutral-300">Jsonnet source</span>
              <textarea
                class="h-72 w-full rounded border border-neutral-300 bg-white px-3 py-2 font-mono text-xs dark:border-neutral-700 dark:bg-neutral-950"
                value={sourceJsonnet()}
                onInput={(event) => setSourceJsonnet(event.currentTarget.value)}
              />
            </label>
          </div>
          <div class="mt-3 flex items-center gap-3">
            <button class="rounded border border-neutral-900 bg-neutral-900 px-3 py-2 text-sm font-semibold text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900" onClick={handleUpload}>Upload new version</button>
            <Show when={uploadMessage()}><p class="text-sm text-neutral-600 dark:text-neutral-300">{uploadMessage()}</p></Show>
          </div>
        </article>

        <Show when={currentTemplate()} keyed>
          {(template) => {
            const variables = template.variables
            return (
              <article class="rounded border border-neutral-200 bg-white/80 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
                <h2 class="text-base font-semibold">{template.name}</h2>
                <p class="text-xs text-neutral-500">{template.templateId} · version {template.version}</p>
                <Show when={template.description}><p class="mt-1 text-sm text-neutral-600 dark:text-neutral-300">{template.description}</p></Show>
                <div class="mt-4 grid gap-3 md:grid-cols-2">
                  <For each={variables}>
                    {(variable) => {
                      const currentValue = () => variableValues()[variable.key] ?? ''
                      const setValue = (value: string) =>
                        setVariableValues((prev) => ({ ...prev, [variable.key]: value }))
                      return (
                        <label class="text-sm">
                          <span class="mb-1 block font-medium">{variable.label || variable.key}</span>
                          <Show when={variable.description}><span class="mb-1 block text-xs text-neutral-500">{variable.description}</span></Show>
                          <Show
                            when={inputForType(variable.type) === 'textarea' || inputForType(variable.type) === 'code'}
                            fallback={
                              <Show
                                when={inputForType(variable.type) === 'toggle'}
                                fallback={
                                  <Show
                                    when={inputForType(variable.type) === 'multi'}
                                    fallback={
                                      <input
                                        type={inputForType(variable.type) === 'number' ? 'number' : 'text'}
                                        class="w-full rounded border border-neutral-300 bg-white px-3 py-2 dark:border-neutral-700 dark:bg-neutral-950"
                                        value={currentValue()}
                                        onInput={(event) => setValue(event.currentTarget.value)}
                                      />
                                    }
                                  >
                                    <select
                                      multiple
                                      class="min-h-24 w-full rounded border border-neutral-300 bg-white px-3 py-2 dark:border-neutral-700 dark:bg-neutral-950"
                                      onChange={(event) => {
                                        const selected = Array.from(event.currentTarget.selectedOptions).map((option) => option.value)
                                        setValue(selected.join(','))
                                      }}
                                    >
                                      <For each={variable.options}>{(option) => <option value={option}>{option}</option>}</For>
                                    </select>
                                  </Show>
                                }
                              >
                                <label class="inline-flex items-center gap-2">
                                  <input
                                    type="checkbox"
                                    checked={currentValue() === 'true'}
                                    onChange={(event) => setValue(event.currentTarget.checked ? 'true' : 'false')}
                                  />
                                  <span>Enabled</span>
                                </label>
                              </Show>
                            }
                          >
                            <textarea
                              class={`w-full rounded border border-neutral-300 bg-white px-3 py-2 font-mono ${inputForType(variable.type) === 'code' ? 'h-36 text-xs' : 'h-24 text-sm'} dark:border-neutral-700 dark:bg-neutral-950`}
                              value={currentValue()}
                              onInput={(event) => setValue(event.currentTarget.value)}
                            />
                          </Show>
                        </label>
                      )
                    }}
                  </For>
                </div>
                <div class="mt-4 flex items-center gap-3">
                  <button class="rounded border border-neutral-900 bg-neutral-900 px-3 py-2 text-sm font-semibold text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900" onClick={handleSubmitTemplate}>Submit job</button>
                  <Show when={submitMessage()}><p class="text-sm text-neutral-600 dark:text-neutral-300">{submitMessage()}</p></Show>
                </div>
              </article>
            )
          }}
        </Show>
      </section>
    </div>
  )
}
