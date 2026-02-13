import { For, Show, createEffect, createMemo, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import type { SavedStateQuery, StateStoreSchemaObject } from '../gen/legion_pb'

type QueryResult = {
  statementType: string
  columns: readonly string[]
  rows: readonly string[]
  affectedRows: bigint
}

type ExplorerTab = 'results' | 'schema'
type HighlightSegment = { text: string; matched: boolean }

const SQL_KEYWORDS = [
  'SELECT',
  'INSERT INTO',
  'UPDATE',
  'DELETE FROM',
  'CREATE TABLE',
  'ALTER TABLE',
  'DROP TABLE',
  'PRAGMA',
] as const

const SQL_FLOW_KEYWORDS = [
  'FROM',
  'WHERE',
  'JOIN',
  'LEFT JOIN',
  'INNER JOIN',
  'GROUP BY',
  'ORDER BY',
  'LIMIT',
  'AND',
  'OR',
  'COUNT(*)',
  'DISTINCT',
] as const

const TABLE_CONTEXT_PATTERN = /\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+[A-Za-z0-9_]*$/i
const DELETE_FROM_CONTEXT_PATTERN = /\bDELETE\s+FROM\s+[A-Za-z0-9_]*$/i
const TABLE_DOT_CONTEXT_PATTERN = /\b([A-Za-z_][A-Za-z0-9_]*)\.[A-Za-z0-9_]*$/i
const COLUMN_CONTEXT_PATTERN = /\b(?:SELECT|WHERE|AND|OR|ON|HAVING|SET)\s+[A-Za-z0-9_.]*$/i
const GROUP_OR_ORDER_CONTEXT_PATTERN = /\b(?:GROUP|ORDER)\s+BY\s+[A-Za-z0-9_.]*$/i
const QUERY_FRAGMENT_PATTERN = /([A-Za-z_][A-Za-z0-9_.]*)$/

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

const splitHighlightSegments = (text: string, rawTerm: string): HighlightSegment[] => {
  const term = rawTerm.trim()
  if (!term) {
    return [{ text, matched: false }]
  }
  const pattern = new RegExp(`(${escapeRegExp(term)})`, 'ig')
  return text
    .split(pattern)
    .filter((part) => part.length > 0)
    .map((part) => ({ text: part, matched: part.toLowerCase() === term.toLowerCase() }))
}

const matchesSchemaObject = (object: StateStoreSchemaObject, term: string) => {
  const normalized = term.trim().toLowerCase()
  if (!normalized) {
    return true
  }
  if (object.name.toLowerCase().includes(normalized)) {
    return true
  }
  if (object.kind.toLowerCase().includes(normalized)) {
    return true
  }
  if (object.createSql.toLowerCase().includes(normalized)) {
    return true
  }
  return object.columns.some((column) =>
    column.name.toLowerCase().includes(normalized)
    || column.dataType.toLowerCase().includes(normalized)
    || (column.primaryKey && 'primary key pk'.includes(normalized))
    || (column.notNull && 'not null required'.includes(normalized)),
  )
}

function HighlightedText(props: { text: string; term: string; class?: string }) {
  return (
    <span class={props.class}>
      <For each={splitHighlightSegments(props.text, props.term)}>
        {(segment) => (
          <Show
            when={segment.matched}
            fallback={<>{segment.text}</>}
          >
            <mark class="rounded bg-amber-200 px-0.5 text-neutral-900 dark:bg-amber-300 dark:text-neutral-900">{segment.text}</mark>
          </Show>
        )}
      </For>
    </span>
  )
}

export default function StateExplorer() {
  const [query, setQuery] = createSignal('SELECT name, type FROM sqlite_master ORDER BY name LIMIT 25;')
  const [allowWrite, setAllowWrite] = createSignal(false)
  const [isRunning, setIsRunning] = createSignal(false)
  const [isLoadingSchema, setIsLoadingSchema] = createSignal(false)
  const [result, setResult] = createSignal<QueryResult | null>(null)
  const [schema, setSchema] = createSignal<StateStoreSchemaObject[]>([])
  const [savedQueries, setSavedQueries] = createSignal<SavedStateQuery[]>([])
  const [saveName, setSaveName] = createSignal('')
  const [status, setStatus] = createSignal<string | null>(null)
  const [error, setError] = createSignal<string | null>(null)
  const [activeTab, setActiveTab] = createSignal<ExplorerTab>('results')
  const [schemaSearch, setSchemaSearch] = createSignal('')

  const tableObjects = createMemo(() =>
    schema().filter((object) => {
      const kind = object.kind.toLowerCase()
      return kind === 'table' || kind === 'view'
    }),
  )

  const tableNames = createMemo(() =>
    tableObjects()
      .map((object) => object.name)
      .sort((a, b) => a.localeCompare(b)),
  )

  const mentionedTables = createMemo(() =>
    tableObjects()
      .filter((object) => {
        const pattern = new RegExp(`\\b${escapeRegExp(object.name)}\\b`, 'i')
        return pattern.test(query())
      })
      .map((object) => object.name),
  )

  const columnTokensForMentionedTables = createMemo(() => {
    const mentioned = new Set(mentionedTables().map((name) => name.toLowerCase()))
    const values = tableObjects().flatMap((object) => {
      if (!mentioned.has(object.name.toLowerCase())) {
        return []
      }
      return object.columns.flatMap((column) => [`${object.name}.${column.name}`, column.name])
    })
    return [...new Set(values)].sort((a, b) => a.localeCompare(b))
  })

  const activeFragment = createMemo(() => query().match(QUERY_FRAGMENT_PATTERN)?.[1] ?? '')
  const filteredSchema = createMemo(() => schema().filter((object) => matchesSchemaObject(object, schemaSearch())))

  const smartSuggestions = createMemo(() => {
    const sql = query().trimEnd()
    const fragment = activeFragment()
    const fragmentUpper = fragment.toUpperCase()

    let label = 'Flow'
    let items: string[] = []

    if (!sql || sql.endsWith(';')) {
      label = 'Commands'
      items = [...SQL_KEYWORDS]
    } else if (TABLE_CONTEXT_PATTERN.test(sql) || DELETE_FROM_CONTEXT_PATTERN.test(sql)) {
      label = 'Tables'
      items = tableNames()
    } else if (TABLE_DOT_CONTEXT_PATTERN.test(sql)) {
      label = 'Columns'
      const tableName = sql.match(TABLE_DOT_CONTEXT_PATTERN)?.[1] ?? ''
      const table = tableObjects().find((object) => object.name.toLowerCase() === tableName.toLowerCase())
      items = table ? table.columns.map((column) => `${table.name}.${column.name}`) : columnTokensForMentionedTables()
    } else if (COLUMN_CONTEXT_PATTERN.test(sql) || GROUP_OR_ORDER_CONTEXT_PATTERN.test(sql)) {
      const mentionedColumns = columnTokensForMentionedTables()
      if (mentionedColumns.length > 0) {
        label = 'Columns'
        items = mentionedColumns
      } else {
        label = 'Flow'
        items = [...SQL_FLOW_KEYWORDS, ...tableNames()]
      }
    } else {
      label = 'Flow'
      items = [...SQL_FLOW_KEYWORDS]
    }

    const filtered = items.filter((item) => !fragment || item.toUpperCase().startsWith(fragmentUpper))
    const deduped = [...new Set(filtered.length > 0 ? filtered : items)]
    return { label, items: deduped.slice(0, 30) }
  })

  const applySuggestion = (suggestion: string) => {
    setQuery((value) => {
      const match = value.match(QUERY_FRAGMENT_PATTERN)
      if (match) {
        const prefix = value.slice(0, value.length - match[1].length)
        return `${prefix}${suggestion} `
      }
      const separator = value.length > 0 && !/\s$/.test(value) ? ' ' : ''
      return `${value}${separator}${suggestion} `
    })
  }

  const loadSchema = async () => {
    setIsLoadingSchema(true)
    try {
      const response = await managerClient.getStateStoreSchema({})
      setSchema(response.objects)
    } catch (err) {
      console.error(err)
      setError('Unable to load the SQLite schema')
    } finally {
      setIsLoadingSchema(false)
    }
  }

  const loadSavedQueries = async () => {
    try {
      const response = await managerClient.listSavedStateQueries({})
      setSavedQueries(response.queries)
    } catch (err) {
      console.error(err)
      setError('Unable to load saved queries')
    }
  }

  createEffect(() => {
    void loadSchema()
    void loadSavedQueries()
  })

  const runQuery = async () => {
    setError(null)
    setStatus(null)
    setActiveTab('results')
    setIsRunning(true)
    try {
      const response = await managerClient.queryStateStore({
        sql: query(),
        allowWrite: allowWrite(),
      })
      setResult({
        statementType: response.statementType,
        columns: response.columns,
        rows: response.rows.map((row) => row.json),
        affectedRows: response.affectedRows,
      })
      setStatus(`Executed ${response.statementType} successfully`)
      await loadSchema()
    } catch (err) {
      console.error(err)
      setError('Query failed. Check SQL and write permissions.')
    } finally {
      setIsRunning(false)
    }
  }

  const saveQuery = async () => {
    setError(null)
    setStatus(null)
    if (!saveName().trim()) {
      setError('Enter a query name before saving')
      return
    }
    try {
      await managerClient.upsertSavedStateQuery({ name: saveName().trim(), sql: query() })
      setStatus(`Saved query: ${saveName().trim()}`)
      await loadSavedQueries()
    } catch (err) {
      console.error(err)
      setError('Unable to save query')
    }
  }

  const deleteQuery = async (name: string) => {
    setError(null)
    setStatus(null)
    try {
      await managerClient.deleteSavedStateQuery({ name })
      setStatus(`Deleted query: ${name}`)
      await loadSavedQueries()
    } catch (err) {
      console.error(err)
      setError('Unable to delete query')
    }
  }

  return (
    <div class="space-y-6">
      <section class="rounded border border-neutral-200 bg-white/90 p-5 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/70 dark:shadow-[4px_4px_0_#0b0d12]">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p class="text-xs uppercase tracking-[0.18em] text-neutral-500 dark:text-neutral-400">Stare Explorer</p>
            <h2 class="text-2xl font-semibold text-neutral-900 dark:text-neutral-100">SQLite State Explorer</h2>
            <p class="text-sm text-neutral-600 dark:text-neutral-300">Run read queries safely by default, and opt into writes only when needed.</p>
          </div>
        </div>

        <div class="mt-4 grid gap-3">
          <textarea
            class="min-h-[180px] w-full rounded border border-neutral-300 bg-white px-3 py-2 font-mono text-sm shadow-sm focus:border-neutral-900 focus:outline-none dark:border-neutral-700 dark:bg-neutral-950"
            value={query()}
            onInput={(event) => setQuery(event.currentTarget.value)}
            onKeyDown={(event) => {
              if (event.metaKey && event.key === 'Enter') {
                event.preventDefault()
                void runQuery()
              }
            }}
            placeholder="SELECT * FROM job LIMIT 25;"
          />
          <div class="space-y-2">
            <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Suggestions: {smartSuggestions().label}</p>
            <div class="flex flex-wrap gap-2">
              <For each={smartSuggestions().items}>
                {(token) => (
                  <button type="button" class="rounded-full border border-neutral-300 px-2 py-0.5 text-xs dark:border-neutral-700" onClick={() => applySuggestion(token)}>
                    {token}
                  </button>
                )}
              </For>
            </div>
          </div>

          <div class="flex flex-wrap items-center justify-end gap-3">
            <label class="inline-flex items-center gap-2 text-sm font-medium text-amber-600 dark:text-amber-300">
              <input type="checkbox" checked={allowWrite()} onChange={(event) => setAllowWrite(event.currentTarget.checked)} />
              Allow write queries
            </label>
            <button
              type="button"
              class="rounded border border-neutral-900 bg-neutral-900 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900"
              onClick={() => void runQuery()}
              disabled={isRunning()}
            >
              {isRunning() ? 'Running…' : 'Run Query'}
            </button>
          </div>
        </div>
      </section>

      <section class="grid gap-6 xl:grid-cols-[340px_1fr]">
        <div class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/70 dark:shadow-[4px_4px_0_#0b0d12]">
          <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">Saved Queries</h3>
          <div class="space-y-2">
            <input class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-950" value={saveName()} onInput={(event) => setSaveName(event.currentTarget.value)} placeholder="weekly rollout report" />
            <button type="button" class="w-full rounded border border-neutral-300 px-3 py-2 text-xs font-semibold uppercase tracking-[0.08em] dark:border-neutral-700" onClick={() => void saveQuery()}>
              Save / Overwrite
            </button>
          </div>
          <div class="max-h-[360px] space-y-2 overflow-auto pr-1">
            <For each={savedQueries()}>
              {(saved) => (
                <article
                  class="cursor-pointer rounded border border-neutral-200 p-2 text-sm transition-colors hover:bg-neutral-50 dark:border-neutral-800 dark:hover:bg-neutral-800/50"
                  role="button"
                  tabindex={0}
                  onClick={() => {
                    setSaveName(saved.name)
                    setQuery(saved.sql)
                  }}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault()
                      setSaveName(saved.name)
                      setQuery(saved.sql)
                    }
                  }}
                >
                  <div class="flex items-center justify-between gap-2">
                    <p class="text-left font-semibold text-neutral-800 dark:text-neutral-100">{saved.name}</p>
                    <button
                      type="button"
                      class="text-xs text-red-600 dark:text-red-300"
                      onClick={(event) => {
                        event.stopPropagation()
                        void deleteQuery(saved.name)
                      }}
                    >
                      Delete
                    </button>
                  </div>
                  <p class="mt-1 line-clamp-2 font-mono text-xs text-neutral-500 dark:text-neutral-400">{saved.sql}</p>
                </article>
              )}
            </For>
            <Show when={savedQueries().length === 0}>
              <p class="text-sm text-neutral-500">No saved queries yet.</p>
            </Show>
          </div>
        </div>

        <div class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/70 dark:shadow-[4px_4px_0_#0b0d12]">
          <div class="flex items-center justify-between">
            <h3 class="text-sm font-semibold uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">Results & Schema</h3>
            <div class="inline-flex rounded border border-neutral-300 p-0.5 text-xs dark:border-neutral-700">
              <button
                type="button"
                class={`rounded px-2 py-1 ${activeTab() === 'results' ? 'bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900' : 'text-neutral-600 dark:text-neutral-300'}`}
                onClick={() => setActiveTab('results')}
              >
                Results
              </button>
              <button
                type="button"
                class={`rounded px-2 py-1 ${activeTab() === 'schema' ? 'bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900' : 'text-neutral-600 dark:text-neutral-300'}`}
                onClick={() => setActiveTab('schema')}
              >
                Schema
              </button>
            </div>
          </div>

          <Show when={status()}><p class="rounded border border-green-500/60 bg-green-50 px-3 py-2 text-sm text-green-700 dark:bg-green-900/30 dark:text-green-200">{status()}</p></Show>
          <Show when={error()}><p class="rounded border border-red-500/60 bg-red-50 px-3 py-2 text-sm text-red-700 dark:bg-red-900/30 dark:text-red-200">{error()}</p></Show>

          <Show when={activeTab() === 'results'}>
            <Show
              when={result()}
              fallback={<p class="text-sm text-neutral-500">Run a query to see results.</p>}
            >
              {(queryResult) => (
                <div class="space-y-2">
                  <p class="text-xs uppercase tracking-[0.12em] text-neutral-500">{queryResult().statementType} · affected {queryResult().affectedRows.toString()}</p>
                  <div class="max-h-[260px] overflow-auto rounded border border-neutral-200 dark:border-neutral-800">
                    <table class="min-w-full divide-y divide-neutral-200 text-sm dark:divide-neutral-800">
                      <thead class="bg-neutral-100/80 dark:bg-neutral-900/80">
                        <tr>
                          <For each={queryResult().columns}>{(column) => <th class="px-3 py-2 text-left font-semibold">{column}</th>}</For>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-neutral-200 dark:divide-neutral-800">
                        <For each={queryResult().rows}>
                          {(row) => (
                            <tr>
                              <td colspan={Math.max(1, queryResult().columns.length)} class="px-3 py-2 font-mono text-xs text-neutral-600 dark:text-neutral-300">{row}</td>
                            </tr>
                          )}
                        </For>
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </Show>
          </Show>

          <Show when={activeTab() === 'schema'}>
            <div class="space-y-3">
              <input
                class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-950"
                value={schemaSearch()}
                onInput={(event) => setSchemaSearch(event.currentTarget.value)}
                placeholder="Search schema (tables, columns, types, SQL)"
              />
              <div class="max-h-[360px] space-y-3 overflow-auto rounded border border-neutral-200 p-3 dark:border-neutral-800">
              <Show when={!isLoadingSchema()} fallback={<p class="text-sm text-neutral-500">Loading schema…</p>}>
                <For each={filteredSchema()}>
                  {(object) => (
                    <article class="rounded border border-neutral-200 p-3 dark:border-neutral-800">
                      <h4 class="text-sm font-semibold">
                        <HighlightedText text={`${object.kind}: ${object.name}`} term={schemaSearch()} />
                      </h4>
                      <p class="mt-1 text-xs text-neutral-500">
                        <HighlightedText text={object.createSql} term={schemaSearch()} />
                      </p>
                      <ul class="mt-2 space-y-1 text-xs text-neutral-600 dark:text-neutral-300">
                        <For each={object.columns}>
                          {(column) => (
                            <li>
                              <HighlightedText text={column.name} term={schemaSearch()} /> <span class="text-neutral-400"><HighlightedText text={column.dataType} term={schemaSearch()} /></span>
                              <Show when={column.primaryKey}><span class="ml-1 rounded bg-neutral-200 px-1 dark:bg-neutral-700"><HighlightedText text="PK" term={schemaSearch()} /></span></Show>
                              <Show when={column.notNull}><span class="ml-1 rounded bg-neutral-200 px-1 dark:bg-neutral-700"><HighlightedText text="NOT NULL" term={schemaSearch()} /></span></Show>
                            </li>
                          )}
                        </For>
                      </ul>
                    </article>
                  )}
                </For>
                <Show when={!isLoadingSchema() && filteredSchema().length === 0}>
                  <p class="text-sm text-neutral-500">No schema entries matched your search.</p>
                </Show>
              </Show>
              </div>
            </div>
          </Show>
        </div>
      </section>
    </div>
  )
}
