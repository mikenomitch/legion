import { useLocation, useNavigate } from '@solidjs/router'
import {
  For,
  Match,
  Switch,
  createEffect,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
  Show,
} from 'solid-js'
import type { TemplateVersion, TemplateVariable } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { clearCommandPaletteIndexItems, setCommandPaletteIndexItems } from '../utils/commandPalette'

type TemplateInputValue = string | string[] | boolean

const readJsonArray = (value?: string): string[] => {
  if (!value) {
    return []
  }
  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? parsed.filter((item): item is string => typeof item === 'string') : []
  } catch {
    return []
  }
}

const readJsonValue = (value?: string): unknown => {
  if (!value) {
    return undefined
  }
  try {
    return JSON.parse(value)
  } catch {
    return undefined
  }
}

const normalizeInputType = (value: string) => value.trim().toLowerCase()

const defaultDisplayForVariable = (variable: TemplateVariable): TemplateInputValue => {
  const defaultValue = readJsonValue(variable.defaultValueJson)
  if (defaultValue === undefined) {
    if (normalizeInputType(variable.inputType) === 'multi_select') {
      return []
    }
    if (normalizeInputType(variable.inputType) === 'toggle') {
      return false
    }
    return ''
  }
  if (typeof defaultValue === 'boolean') {
    return defaultValue
  }
  if (typeof defaultValue === 'number') {
    return String(defaultValue)
  }
  if (Array.isArray(defaultValue)) {
    return defaultValue.filter((item): item is string => typeof item === 'string')
  }
  return typeof defaultValue === 'string' ? defaultValue : String(defaultValue)
}

export default function Templates() {
  const [selectedId, setSelectedId] = createSignal('')
  const [selectedVersion, setSelectedVersion] = createSignal(0)
  const [message, setMessage] = createSignal('')
  const [fieldErrors, setFieldErrors] = createSignal<Record<string, string>>({})
  const [values, setValues] = createSignal<Record<string, TemplateInputValue>>({})
  const [templateSearch, setTemplateSearch] = createSignal('')
  const location = useLocation()
  const navigate = useNavigate()

  const [templates] = createResource(async () => (await managerClient.listTemplates({})).templates)
  const normalizedSearch = createMemo(() => templateSearch().trim().toLowerCase())

  const visibleTemplates = createMemo(() => {
    const source = templates() ?? []
    const term = normalizedSearch()
    if (!term) {
      return source
    }
    return source.filter((template) => {
      const label = template.spec?.name || template.templateId
      const description = template.spec?.description || ''
      const searchText = `${label} ${template.templateId} v${template.version} ${description}`.toLowerCase()
      return searchText.includes(term)
    })
  })

  const selected = createMemo<TemplateVersion | undefined>(() =>
    templates()?.find((template) => template.templateId === selectedId() && template.version === selectedVersion()),
  )

  const selectedVariables = createMemo(() => selected()?.spec?.variables ?? [])
  const selectedTemplateLabel = (template: TemplateVersion) =>
    `${template.spec?.name || template.templateId} · v${template.version}`
  const templateLink = (template: TemplateVersion) =>
    `/templates?template=${encodeURIComponent(template.templateId)}&version=${template.version}`

  const setSelectedFromSearchParams = () => {
    const list = templates()
    if (!list || list.length === 0) return

    const query = new URLSearchParams(location.search)
    const templateId = query.get('template')
    if (!templateId) return

    const versionValue = query.get('version')
    const hasVersion = versionValue !== null && versionValue.trim().length > 0
    const version = hasVersion ? Number.parseInt(versionValue, 10) : NaN

    const match = hasVersion
      ? list.find((template) => template.templateId === templateId && template.version === version)
      : list.find((template) => template.templateId === templateId)

    const fallback = match ?? list.find((template) => template.templateId === templateId)
    if (fallback) {
      setSelectedTemplate(fallback)
    }
  }

  const updateTemplateQuery = (template: TemplateVersion) => {
    const normalized = `/templates?template=${encodeURIComponent(template.templateId)}&version=${template.version}`
    if (location.pathname !== '/templates' || location.search !== `?template=${encodeURIComponent(template.templateId)}&version=${template.version}`) {
      navigate(normalized, { replace: true })
    }
  }

  const selectTemplate = (template: TemplateVersion) => {
    setSelectedTemplate(template)
    updateTemplateQuery(template)
  }

  createEffect(() => {
    const current = templates() ?? []
    setCommandPaletteIndexItems(
      current.map((template) => ({
        id: `template:${template.templateId}:v${template.version}`,
        label: template.spec?.name || template.templateId,
        subtitle: `${template.templateId} · v${template.version}`,
        href: templateLink(template),
        source: 'Templates',
      })),
    )
  })

  onCleanup(() => {
    clearCommandPaletteIndexItems()
  })

  createEffect(setSelectedFromSearchParams)

  const isMissingValue = (inputType: string, value: TemplateInputValue | undefined) => {
    if (value === undefined) {
      return true
    }
    if (inputType === 'toggle') return false
    if (inputType === 'multi_select') return !Array.isArray(value) || value.length === 0
    if (inputType === 'number') return typeof value !== 'string' || !value.trim()
    if (typeof value === 'boolean') return false
    return !String(value).trim()
  }

  const textValue = (value: TemplateInputValue | undefined) => (typeof value === 'string' ? value : '')

  const renderCodeValue = (raw: string): string => {
    const trimmed = raw.trim()
    if (!trimmed) {
      return '""'
    }
    try {
      JSON.parse(trimmed)
      return trimmed
    } catch {
      return JSON.stringify(trimmed)
    }
  }

  const resetState = (template?: TemplateVersion) => {
    setMessage('')
    setFieldErrors({})
    if (!template) {
      setValues({})
      return
    }
    const next: Record<string, TemplateInputValue> = {}
    for (const variable of template.spec?.variables ?? []) {
      next[variable.key] = defaultDisplayForVariable(variable)
    }
    setValues(next)
  }

  const setSelectedTemplate = (template: TemplateVersion) => {
    setSelectedId(template.templateId)
    setSelectedVersion(template.version)
    resetState(template)
  }

  const setTextValue = (key: string, value: string) => {
    setValues((current) => ({ ...current, [key]: value }))
    setFieldErrors((current) => {
      const next = { ...current }
      delete next[key]
      return next
    })
  }

  const toggleValues = (key: string, nextChecked: boolean) => {
    setValues((current) => ({ ...current, [key]: nextChecked }))
    setFieldErrors((current) => {
      const next = { ...current }
      delete next[key]
      return next
    })
  }

  const toggleSelectValue = (key: string, option: string, checked: boolean) => {
    setValues((current) => {
      const existing = new Set(Array.isArray(current[key]) ? current[key] : [])
      if (checked) {
        existing.add(option)
      } else {
        existing.delete(option)
      }
      return { ...current, [key]: Array.from(existing) }
    })
    setFieldErrors((current) => {
      const next = { ...current }
      delete next[key]
      return next
    })
  }

  const submit = async () => {
    const template = selected()
    if (!template) return

    const requiredErrors: Record<string, string> = {}
    const inputs: { key: string; valueJson: string }[] = []

    for (const variable of template.spec?.variables ?? []) {
      const inputType = normalizeInputType(variable.inputType)
      let value = values()[variable.key]
      const label = variable.label || variable.key

      if (value === undefined) {
        value = inputType === 'multi_select' ? [] : inputType === 'toggle' ? false : ''
      }

      if (variable.required && isMissingValue(inputType, value)) {
        requiredErrors[variable.key] = `${label} is required`
        continue
      }

      if (inputType === 'multi_select' && Array.isArray(value)) {
        inputs.push({ key: variable.key, valueJson: JSON.stringify(value) })
        continue
      }

      if (inputType === 'toggle') {
        inputs.push({ key: variable.key, valueJson: String(Boolean(value)) })
        continue
      }

      if (inputType === 'number') {
        if (typeof value !== 'string') {
          requiredErrors[variable.key] = `${label} must be a valid number`
          continue
        }
        const trimmed = value.trim()
        if (trimmed === '') {
          if (variable.required) {
            requiredErrors[variable.key] = `${label} is required`
          }
          continue
        }
        const parsed = Number(trimmed)
        if (!Number.isFinite(parsed)) {
          requiredErrors[variable.key] = `${label} must be a valid number`
          continue
        }
        inputs.push({ key: variable.key, valueJson: String(parsed) })
        continue
      }

      if (inputType === 'code') {
        inputs.push({ key: variable.key, valueJson: renderCodeValue(typeof value === 'string' ? value : '') })
        continue
      }

      if (typeof value === 'boolean') {
        inputs.push({ key: variable.key, valueJson: String(value) })
        continue
      }

      if (Array.isArray(value)) {
        inputs.push({ key: variable.key, valueJson: JSON.stringify(value) })
        continue
      }

      inputs.push({ key: variable.key, valueJson: JSON.stringify(textValue(value)) })
    }

    if (Object.keys(requiredErrors).length > 0) {
      setFieldErrors(requiredErrors)
      setMessage('Please fix the field errors before submitting.')
      return
    }

    setFieldErrors({})
    const response = await managerClient.runTemplate({
      template: { templateId: template.templateId, version: template.version },
      inputs,
    })
    setMessage(response.message)
  }

  return (
    <div class="grid gap-4 xl:grid-cols-[360px_1fr]">
              <section class="rounded border border-neutral-200 bg-white p-3 shadow-[5px_5px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
        <h2 class="mb-3 text-sm font-semibold">Template versions</h2>
        <input
          value={templateSearch()}
          onInput={(event) => setTemplateSearch(event.currentTarget.value)}
          placeholder="Search templates..."
          class="mb-2 w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        />
        <Show
          when={templates.loading}
          fallback={<p class="mb-2 text-xs text-neutral-500">{visibleTemplates().length} templates</p>}
        >
          <p class="mb-2 text-xs text-neutral-500">Loading templates…</p>
        </Show>
        <div class="space-y-2">
          <Show
            when={visibleTemplates().length > 0}
            fallback={<p class="text-sm text-neutral-500">No templates match your search.</p>}
          >
            <For each={visibleTemplates()}>
            {(template) => (
              <button
                type="button"
                class={`w-full rounded border px-3 py-2 text-left text-sm transition ${
                  template.templateId === selectedId() && template.version === selectedVersion()
                    ? 'border-neutral-900 bg-neutral-900 text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900'
                    : 'border-neutral-200 dark:border-neutral-800'
                }`}
                onClick={() => selectTemplate(template)}
              >
                <p class="font-semibold">{template.spec?.name || template.templateId}</p>
                <p class="text-xs text-neutral-500 dark:text-neutral-400">
                  {template.templateId} v{template.version}
                </p>
              </button>
            )}
            </For>
          </Show>
        </div>
      </section>

      <section class="rounded border border-neutral-200 bg-white p-4 shadow-[5px_5px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
        <Show when={selected()} fallback={<p class="text-sm text-neutral-500">Choose a template to submit.</p>}>
          {(template) => (
            <>
              <header class="mb-4">
                <h2 class="text-lg font-semibold">{template().spec?.name || template().templateId}</h2>
                <p class="mb-1 text-sm text-neutral-500 dark:text-neutral-300">{template().spec?.description}</p>
                <p class="text-xs text-neutral-500 dark:text-neutral-400">{selectedTemplateLabel(template())}</p>
              </header>
              <form class="space-y-5" onSubmit={(event) => {
                event.preventDefault()
                submit()
              }}>
                <For each={selectedVariables()}>
                  {(variable) => {
                    const inputType = normalizeInputType(variable.inputType)
                    const options = readJsonArray(variable.optionsJson)
                    const hasError = () => Boolean(fieldErrors()[variable.key])

                    return (
                      <label class="block space-y-1 text-sm">
                        <div class="flex items-center justify-between gap-2">
                          <span class="font-medium">
                            {variable.label || variable.key}
                            <Show when={variable.required}>
                              <span class="ml-1 text-rose-500">*</span>
                            </Show>
                          </span>
                          <span class={`rounded border px-1.5 py-0.5 text-[11px] uppercase tracking-wide ${hasError() ? 'border-rose-500 text-rose-600 dark:text-rose-300' : 'border-neutral-300 text-neutral-500 dark:border-neutral-700'}`}>
                            {inputType || 'code'}
                          </span>
                        </div>
                        <Show when={variable.description}>
                          <p class="text-xs text-neutral-500 dark:text-neutral-400">{variable.description}</p>
                        </Show>
                        <Switch fallback={
                          <textarea
                            class="min-h-28 w-full rounded border border-neutral-300 bg-white px-2 py-1 text-sm dark:border-neutral-700 dark:bg-neutral-950"
                            value={textValue(values()[variable.key])}
                            onInput={(event) => setTextValue(variable.key, event.currentTarget.value)}
                            placeholder={`Enter ${variable.label || variable.key}`}
                          />
                        }>
                          <Match when={inputType === 'toggle'}>
                            <label class="inline-flex items-center gap-2">
                              <input
                                type="checkbox"
                                checked={Boolean(values()[variable.key])}
                                onChange={(event) => toggleValues(variable.key, event.currentTarget.checked)}
                              />
                              <span class="text-sm text-neutral-600 dark:text-neutral-300">Enabled</span>
                            </label>
                          </Match>
                          <Match when={inputType === 'multi_select' && options.length > 0}>
                            <div class="space-y-1">
                              <For each={options}>
                                {(option) => (
                                  <label class="flex items-center gap-2">
                                    <input
                                      type="checkbox"
                                      checked={Array.isArray(values()[variable.key]) && (values()[variable.key] as string[]).includes(option)}
                                      onChange={(event) => toggleSelectValue(variable.key, option, event.currentTarget.checked)}
                                    />
                                    <span class="text-sm text-neutral-700 dark:text-neutral-300">{option}</span>
                                  </label>
                                )}
                              </For>
                            </div>
                          </Match>
                          <Match when={inputType === 'text' || inputType === 'code'}>
                            <textarea
                              class="min-h-24 w-full rounded border border-neutral-300 bg-white px-2 py-1 font-mono text-sm dark:border-neutral-700 dark:bg-neutral-950"
                            value={textValue(values()[variable.key])}
                              onInput={(event) => setTextValue(variable.key, event.currentTarget.value)}
                              placeholder={`Enter ${inputType} value for ${variable.label || variable.key}`}
                            />
                          </Match>
                          <Match when={inputType === 'number'}>
                            <input
                              type="number"
                              inputMode="decimal"
                              class="w-full rounded border border-neutral-300 bg-white px-2 py-1 text-sm dark:border-neutral-700 dark:bg-neutral-950"
                              value={textValue(values()[variable.key])}
                              onInput={(event) => setTextValue(variable.key, event.currentTarget.value)}
                              placeholder="0"
                            />
                          </Match>
                          <Match when={inputType === 'string'}>
                            <input
                              type="text"
                              class="w-full rounded border border-neutral-300 bg-white px-2 py-1 text-sm dark:border-neutral-700 dark:bg-neutral-950"
                              value={textValue(values()[variable.key])}
                              onInput={(event) => setTextValue(variable.key, event.currentTarget.value)}
                              placeholder={`Enter ${variable.label || variable.key}`}
                            />
                          </Match>
                        </Switch>
                        <Show when={fieldErrors()[variable.key]}>
                          <p class="text-xs text-rose-500">{fieldErrors()[variable.key]}</p>
                        </Show>
                      </label>
                    )
                  }}
                </For>
                <button
                  type="submit"
                  class="rounded border border-neutral-900 bg-neutral-900 px-3 py-2 text-sm font-semibold text-white transition hover:bg-neutral-800 dark:border-neutral-100 dark:bg-neutral-100 dark:text-neutral-900"
                >
                  Submit template
                </button>
              </form>
              <Show when={message()}>
                <p class="mt-3 text-sm text-neutral-600 dark:text-neutral-300">{message()}</p>
              </Show>
            </>
          )}
        </Show>
      </section>
    </div>
  )
}
