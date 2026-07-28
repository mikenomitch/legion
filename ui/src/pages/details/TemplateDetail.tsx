import { A, useNavigate, useParams } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import {
  Badge,
  Button,
  CodeBlock,
  ConfirmButton,
  Field,
  MonoBadge,
  Note,
  PageHeader,
  SectionCard,
  Spinner,
  checkboxClass,
  inputClass,
  linkClass,
  selectClass,
  textareaClass,
} from '../../components/ui'
import type { JobTemplateVariable } from '../../gen/legion_pb'
import { managerClient } from '../../grpcClient'
import { allows, canWriteAny } from '../../utils/acl'
import { formatDate } from '../../utils/format'

const errorText = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)

const parseDefault = (variable: JobTemplateVariable): unknown => {
  if (variable.defaultJson === undefined) return undefined
  try {
    return JSON.parse(variable.defaultJson)
  } catch {
    return undefined
  }
}

/**
 * Form state is a map of variable key -> current value in a shape convenient
 * for inputs (strings for text-ish types, booleans, string arrays). It is
 * converted to the typed variables object right before render/run.
 */
type FormValues = Record<string, unknown>

const seedValues = (variables: readonly JobTemplateVariable[]): FormValues => {
  const seeded: FormValues = {}
  for (const variable of variables) {
    const fallback = parseDefault(variable)
    if (fallback !== undefined) {
      seeded[variable.key] = variable.type === 'json' ? JSON.stringify(fallback, null, 2) : fallback
    } else if (variable.type === 'multi_select') {
      seeded[variable.key] = []
    } else if (variable.type === 'boolean') {
      seeded[variable.key] = false
    } else {
      seeded[variable.key] = ''
    }
  }
  return seeded
}

/** Builds the request's variables object; throws on unparseable json vars. */
const toVariablesObject = (
  variables: readonly JobTemplateVariable[],
  values: FormValues,
): Record<string, unknown> => {
  const out: Record<string, unknown> = {}
  for (const variable of variables) {
    const value = values[variable.key]
    switch (variable.type) {
      case 'number': {
        if (value === '' || value === undefined) break
        const parsed = Number(value)
        if (Number.isNaN(parsed)) throw new Error(`variable '${variable.key}' expects a number`)
        out[variable.key] = parsed
        break
      }
      case 'boolean':
        out[variable.key] = Boolean(value)
        break
      case 'multi_select':
        if (Array.isArray(value) && value.length > 0) out[variable.key] = value
        break
      case 'json': {
        const text = typeof value === 'string' ? value.trim() : ''
        if (!text) break
        try {
          out[variable.key] = JSON.parse(text)
        } catch (error) {
          throw new Error(`variable '${variable.key}' is not valid JSON: ${errorText(error)}`)
        }
        break
      }
      default: {
        const text = typeof value === 'string' ? value : String(value ?? '')
        if (text !== '') out[variable.key] = text
      }
    }
  }
  return out
}

const VariableInput = (props: {
  variable: JobTemplateVariable
  value: unknown
  onChange: (value: unknown) => void
}) => {
  const variable = props.variable
  switch (variable.type) {
    case 'text':
      return (
        <textarea
          class={`${textareaClass} h-24`}
          value={typeof props.value === 'string' ? props.value : ''}
          onInput={(event) => props.onChange(event.currentTarget.value)}
        />
      )
    case 'number':
      return (
        <input
          type="number"
          class={inputClass}
          value={props.value === undefined ? '' : String(props.value)}
          onInput={(event) => props.onChange(event.currentTarget.value)}
        />
      )
    case 'boolean':
      return (
        <label class="inline-flex items-center gap-2 text-sm text-ink">
          <input
            type="checkbox"
            class={checkboxClass}
            checked={Boolean(props.value)}
            onChange={(event) => props.onChange(event.currentTarget.checked)}
          />
          <span>Enabled</span>
        </label>
      )
    case 'select':
      return (
        <select
          class={selectClass}
          value={typeof props.value === 'string' ? props.value : ''}
          onChange={(event) => props.onChange(event.currentTarget.value)}
        >
          <option value="" disabled>
            Choose…
          </option>
          <For each={variable.options}>{(option) => <option value={option}>{option}</option>}</For>
        </select>
      )
    case 'multi_select': {
      const selected = () => (Array.isArray(props.value) ? (props.value as string[]) : [])
      const toggle = (option: string, on: boolean) => {
        const next = selected().filter((entry) => entry !== option)
        if (on) next.push(option)
        props.onChange(next)
      }
      return (
        <div class="flex flex-wrap gap-2">
          <For each={variable.options}>
            {(option) => (
              <label class="inline-flex cursor-pointer items-center gap-1.5 rounded-full border border-line px-3 py-1 text-xs text-ink transition-colors has-[:checked]:border-brass has-[:checked]:bg-brass/10">
                <input
                  type="checkbox"
                  class="sr-only"
                  checked={selected().includes(option)}
                  onChange={(event) => toggle(option, event.currentTarget.checked)}
                />
                <span>{option}</span>
              </label>
            )}
          </For>
        </div>
      )
    }
    case 'json':
      return (
        <textarea
          class={`${textareaClass} h-28 font-mono text-xs`}
          value={typeof props.value === 'string' ? props.value : ''}
          onInput={(event) => props.onChange(event.currentTarget.value)}
          spellcheck={false}
        />
      )
    default:
      return (
        <input
          class={inputClass}
          value={typeof props.value === 'string' ? props.value : ''}
          onInput={(event) => props.onChange(event.currentTarget.value)}
        />
      )
  }
}

export default function TemplateDetail() {
  const params = useParams()
  const navigate = useNavigate()
  const templateName = () => decodeURIComponent(params.name)
  const [version, setVersion] = createSignal<number | undefined>(undefined)

  const [data, { refetch }] = createResource(
    () => ({ name: templateName(), version: version() }),
    async (input) => managerClient.getJobTemplate({ name: input.name, version: input.version }),
  )
  const template = createMemo(() => data()?.template)

  const [values, setValues] = createSignal<FormValues>({})
  createEffect(() => {
    const current = template()
    if (current) setValues(seedValues(current.variables))
  })
  const setValue = (key: string, value: unknown) =>
    setValues((previous) => ({ ...previous, [key]: value }))

  const [preview, setPreview] = createSignal<{ jobName: string; spec: string } | null>(null)
  const [previewError, setPreviewError] = createSignal('')
  const [previewing, setPreviewing] = createSignal(false)
  const [submitting, setSubmitting] = createSignal(false)
  const [submitError, setSubmitError] = createSignal('')
  const [submitResult, setSubmitResult] = createSignal<{ message: string; jobName: string } | null>(null)

  // Editing values invalidates a stale preview/submit outcome.
  createEffect(() => {
    values()
    setPreview(null)
    setPreviewError('')
    setSubmitError('')
  })

  const variablesJson = (): string => {
    const current = template()
    if (!current) throw new Error('template not loaded')
    return JSON.stringify(toVariablesObject(current.variables, values()))
  }

  const runPreview = async () => {
    const current = template()
    if (!current) return
    setPreviewing(true)
    setPreviewError('')
    try {
      const reply = await managerClient.renderJobTemplate({
        name: current.name,
        version: current.version,
        variablesJson: variablesJson(),
      })
      let pretty = reply.renderedJobspec
      try {
        pretty = JSON.stringify(JSON.parse(reply.renderedJobspec), null, 2)
      } catch {
        // Leave the raw text if it is somehow not JSON.
      }
      setPreview({ jobName: reply.jobName, spec: pretty })
    } catch (error) {
      setPreviewError(errorText(error))
    } finally {
      setPreviewing(false)
    }
  }

  const submit = async (event: Event) => {
    event.preventDefault()
    const current = template()
    if (!current) return
    setSubmitting(true)
    setSubmitError('')
    setSubmitResult(null)
    try {
      const reply = await managerClient.runJobTemplate({
        name: current.name,
        version: current.version,
        variablesJson: variablesJson(),
        forceReschedule: false,
      })
      setSubmitResult({ message: reply.message, jobName: reply.jobName })
    } catch (error) {
      setSubmitError(errorText(error))
    } finally {
      setSubmitting(false)
    }
  }

  // New-version editor, seeded with the currently shown source.
  const [editorOpen, setEditorOpen] = createSignal(false)
  const [editorSource, setEditorSource] = createSignal('')
  const [publishing, setPublishing] = createSignal(false)
  const [publishError, setPublishError] = createSignal('')
  createEffect(() => {
    const current = template()
    if (current && !editorOpen()) setEditorSource(current.sourceJsonnet)
  })

  const publishVersion = async (event: Event) => {
    event.preventDefault()
    setPublishing(true)
    setPublishError('')
    try {
      await managerClient.upsertJobTemplate({
        name: templateName(),
        sourceJsonnet: editorSource(),
      })
      setEditorOpen(false)
      setVersion(undefined)
      await refetch()
    } catch (error) {
      setPublishError(errorText(error))
    } finally {
      setPublishing(false)
    }
  }

  const [deleteError, setDeleteError] = createSignal('')
  const deleteTemplate = async () => {
    setDeleteError('')
    try {
      await managerClient.deleteJobTemplate({ name: templateName() })
      navigate('/templates')
    } catch (error) {
      setDeleteError(errorText(error))
    }
  }

  const mayWrite = () => allows('template', 'write', { template: templateName() })
  // Submission needs job:write; the exact rendered name is only known after
  // rendering, so gate the buttons on the wildcard check and let the server
  // enforce the precise scope.
  const maySubmit = () => canWriteAny('job')

  return (
    <div class="space-y-6">
      <Show when={template()} fallback={data.loading ? <Spinner label="Loading template…" /> : <Note tone="danger">Failed to load template: {data.error ? errorText(data.error) : 'not found'}</Note>}>
        {(current) => (
          <>
            <PageHeader
              eyebrow="Template"
              title={current().name}
              badges={
                <>
                  <MonoBadge tone="accent">v{current().version}</MonoBadge>
                  <Show when={current().version !== current().latestVersion}>
                    <Badge tone="warn">latest is v{current().latestVersion}</Badge>
                  </Show>
                </>
              }
              subtitle={current().description || 'No description.'}
              actions={
                <Show when={mayWrite()}>
                  <div class="flex items-center gap-2">
                    <Button variant="subtle" size="sm" icon="plus" onClick={() => setEditorOpen((open) => !open)}>
                      New version
                    </Button>
                    <ConfirmButton label="Delete" size="sm" icon="trash" onConfirm={deleteTemplate} />
                  </div>
                </Show>
              }
            />
            <Show when={deleteError()}>
              <Note tone="danger">{deleteError()}</Note>
            </Show>

            <Show when={editorOpen()}>
              <SectionCard
                title="Publish new version"
                description={`Stored as v${current().latestVersion + 1}; existing versions are immutable.`}
              >
                <form class="flex flex-col gap-3" onSubmit={publishVersion}>
                  <textarea
                    class={`${textareaClass} h-72 font-mono text-xs`}
                    value={editorSource()}
                    onInput={(event) => setEditorSource(event.currentTarget.value)}
                    spellcheck={false}
                  />
                  <div class="flex items-center gap-3">
                    <Button type="submit" variant="primary" size="md" disabled={publishing()}>
                      {publishing() ? 'Publishing…' : 'Publish'}
                    </Button>
                    <Show when={publishError()}>
                      <Note tone="danger" class="flex-1">{publishError()}</Note>
                    </Show>
                  </div>
                </form>
              </SectionCard>
            </Show>

            <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
              <SectionCard
                title="Run this template"
                description="Values are validated against the declared types; the manager renders and submits."
                actions={
                  <Show when={(data()?.versions.length ?? 0) > 1}>
                    <select
                      class={selectClass}
                      value={version() === undefined ? 'latest' : String(version())}
                      onChange={(event) => {
                        const raw = event.currentTarget.value
                        setVersion(raw === 'latest' ? undefined : Number(raw))
                      }}
                    >
                      <option value="latest">Latest (v{data()?.versions[0]?.version ?? current().latestVersion})</option>
                      <For each={data()?.versions ?? []}>
                        {(info) => <option value={String(info.version)}>v{info.version} · {formatDate(info.createdAt)}</option>}
                      </For>
                    </select>
                  </Show>
                }
              >
                <form class="flex flex-col gap-4" onSubmit={submit}>
                  <Show
                    when={current().variables.length > 0}
                    fallback={<p class="text-sm text-muted">This template takes no variables.</p>}
                  >
                    <div class="grid gap-4 md:grid-cols-2">
                      <For each={current().variables}>
                        {(variable) => (
                          <Field
                            label={
                              <>
                                {variable.label || variable.key}
                                <Show when={variable.required}>
                                  <span class="ml-1 text-danger" title="Required">*</span>
                                </Show>
                              </>
                            }
                            hint={variable.description}
                            class={variable.type === 'text' || variable.type === 'json' ? 'md:col-span-2' : undefined}
                          >
                            <VariableInput
                              variable={variable}
                              value={values()[variable.key]}
                              onChange={(value) => setValue(variable.key, value)}
                            />
                          </Field>
                        )}
                      </For>
                    </div>
                  </Show>
                  <div class="flex flex-wrap items-center gap-3">
                    <Button
                      type="submit"
                      variant="primary"
                      size="md"
                      icon="play"
                      disabled={submitting() || !maySubmit()}
                      title={maySubmit() ? undefined : 'Your token cannot submit jobs'}
                    >
                      {submitting() ? 'Submitting…' : 'Submit job'}
                    </Button>
                    <Button
                      type="button"
                      variant="subtle"
                      size="md"
                      icon="eye"
                      disabled={previewing() || !maySubmit()}
                      onClick={runPreview}
                    >
                      {previewing() ? 'Rendering…' : 'Preview rendered spec'}
                    </Button>
                  </div>
                  <Show when={submitError()}>
                    <Note tone="danger">{submitError()}</Note>
                  </Show>
                  <Show when={submitResult()}>
                    {(result) => (
                      <Note tone="ok">
                        {result().message}{' '}
                        <A href={`/jobs/${encodeURIComponent(result().jobName)}`} class={linkClass}>
                          View job {result().jobName}
                        </A>
                      </Note>
                    )}
                  </Show>
                  <Show when={previewError()}>
                    <Note tone="danger">{previewError()}</Note>
                  </Show>
                  <Show when={preview()}>
                    {(rendered) => (
                      <div class="space-y-2">
                        <p class="text-xs text-muted">
                          Renders job <span class="font-mono text-ink">{rendered().jobName}</span>:
                        </p>
                        <CodeBlock code={rendered().spec} maxHClass="max-h-96" label="rendered jobspec" />
                      </div>
                    )}
                  </Show>
                </form>
              </SectionCard>

              <SectionCard
                title="Source"
                description={`v${current().version}, created ${formatDate(current().createdAt)}${current().createdBy ? ` by ${current().createdBy}` : ''}.`}
              >
                <CodeBlock code={current().sourceJsonnet} maxHClass="max-h-[36rem]" label="template source" />
              </SectionCard>
            </div>
          </>
        )}
      </Show>
    </div>
  )
}
