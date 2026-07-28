import { For, Show, createEffect, createMemo, createSignal } from 'solid-js'
import { managerClient } from '../grpcClient'
import { canWriteAny } from '../utils/acl'
import type { SavedStateQuery, StateStoreSchemaObject } from '../gen/legion_pb'
import { Badge, Button, ConfirmButton, Icon, Note, SectionCard, Spinner, Tabs, checkboxClass, cx, inputClass, textareaClass } from '../components/ui'

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
            <mark class="rounded bg-amber-500/30 px-0.5 text-ink">{segment.text}</mark>
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
    <div class="space-y-4">
      <SectionCard
        title="Query"
        description="Read queries run safely by default; opt into writes only when needed."
      >
        <div class="grid gap-3">
          <textarea
            class={cx(textareaClass, 'min-h-[160px] text-sm')}
            value={query()}
            onInput={(event) => setQuery(event.currentTarget.value)}
            onKeyDown={(event) => {
              if (event.metaKey && event.key === 'Enter') {
                event.preventDefault()
                void runQuery()
              }
            }}
            placeholder="SELECT * FROM job LIMIT 25;"
            spellcheck={false}
          />
          <div class="space-y-1.5">
            <p class="eyebrow">Suggestions · {smartSuggestions().label}</p>
            <div class="flex flex-wrap gap-1.5">
              <For each={smartSuggestions().items}>
                {(token) => (
                  <button
                    type="button"
                    class="rounded-full border border-line bg-surface px-2 py-0.5 font-mono text-2xs text-muted transition-colors hover:border-brass/50 hover:text-ink"
                    onClick={() => applySuggestion(token)}
                  >
                    {token}
                  </button>
                )}
              </For>
            </div>
          </div>

          <div class="flex flex-wrap items-center justify-end gap-3">
            <label class={cx(
              'inline-flex cursor-pointer items-center gap-2 rounded-md border px-2.5 py-1.5 text-xs font-medium transition-colors',
              allowWrite() ? 'border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-300' : 'border-line text-muted',
            )}>
              <input type="checkbox" class={checkboxClass} checked={allowWrite()} onChange={(event) => setAllowWrite(event.currentTarget.checked)} />
              Allow write queries
            </label>
            <Button variant="primary" size="md" icon="play" onClick={() => void runQuery()} disabled={isRunning()} title="⌘Enter">
              {isRunning() ? 'Running…' : 'Run query'}
            </Button>
          </div>
        </div>
      </SectionCard>

      <div class="grid items-start gap-4 xl:grid-cols-[320px_1fr]">
        <SectionCard title="Saved queries" flush>
          <div class="space-y-2 border-b border-line p-3">
            <input class={inputClass} value={saveName()} onInput={(event) => setSaveName(event.currentTarget.value)} placeholder="weekly rollout report" />
            <Button class="w-full" onClick={() => void saveQuery()} disabled={!canWriteAny('state')}>Save / overwrite</Button>
          </div>
          <div class="max-h-[360px] space-y-1.5 overflow-auto p-2">
            <For each={savedQueries()}>
              {(saved) => (
                <article
                  class="cursor-pointer rounded-md border border-line p-2 text-sm transition-colors hover:border-brass/40 hover:bg-surface2/50"
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
                    <p class="text-left text-xs font-semibold text-ink">{saved.name}</p>
                    <ConfirmButton
                      label="Delete"
                      size="xs"
                      onConfirm={() => void deleteQuery(saved.name)}
                      disabled={!canWriteAny('state')}
                    />
                  </div>
                  <p class="mt-1 line-clamp-2 font-mono text-2xs text-faint">{saved.sql}</p>
                </article>
              )}
            </For>
            <Show when={savedQueries().length === 0}>
              <p class="p-2 text-sm text-muted">No saved queries yet.</p>
            </Show>
          </div>
        </SectionCard>

        <SectionCard flush>
          <Tabs
            tabs={[
              { id: 'results', label: 'Results' },
              { id: 'schema', label: 'Schema', count: schema().length },
            ]}
            active={activeTab()}
            onChange={(id) => setActiveTab(id as ExplorerTab)}
          />
          <div class="space-y-3 p-3.5">
            <Show when={status()}><Note tone="ok" onDismiss={() => setStatus(null)}>{status()}</Note></Show>
            <Show when={error()}><Note tone="danger" onDismiss={() => setError(null)}>{error()}</Note></Show>

            <Show when={activeTab() === 'results'}>
              <Show
                when={result()}
                fallback={<p class="text-sm text-muted">Run a query to see results.</p>}
              >
                {(queryResult) => (
                  <div class="space-y-2">
                    <p class="flex items-center gap-2">
                      <Badge tone="accent" mono>{queryResult().statementType}</Badge>
                      <span class="text-xs text-faint">{queryResult().rows.length} row{queryResult().rows.length === 1 ? '' : 's'} · {queryResult().affectedRows.toString()} affected</span>
                    </p>
                    <div class="max-h-[320px] overflow-auto rounded-md border border-line">
                      <table class="min-w-full divide-y divide-line text-sm">
                        <thead class="sticky top-0 bg-surface2">
                          <tr>
                            <For each={queryResult().columns}>{(column) => <th class="px-3 py-2 text-left text-2xs font-semibold uppercase tracking-wider text-faint">{column}</th>}</For>
                          </tr>
                        </thead>
                        <tbody class="divide-y divide-line">
                          <For each={queryResult().rows}>
                            {(row) => {
                              // Rows arrive as JSON strings; render per-column cells when parseable.
                              let parsed: Record<string, unknown> | null = null
                              try {
                                const value = JSON.parse(row)
                                if (value && typeof value === 'object' && !Array.isArray(value)) parsed = value as Record<string, unknown>
                              } catch {
                                parsed = null
                              }
                              return (
                                <Show
                                  when={parsed && queryResult().columns.length > 0}
                                  fallback={
                                    <tr>
                                      <td colspan={Math.max(1, queryResult().columns.length)} class="px-3 py-2 font-mono text-xs text-muted">{row}</td>
                                    </tr>
                                  }
                                >
                                  <tr class="transition-colors hover:bg-surface2/50">
                                    <For each={queryResult().columns}>
                                      {(column) => {
                                        const cell = parsed![column]
                                        const text = cell === null || cell === undefined ? '' : typeof cell === 'object' ? JSON.stringify(cell) : String(cell)
                                        return <td class="max-w-[24rem] truncate px-3 py-1.5 font-mono text-xs text-muted" title={text}>{text || <span class="text-faint">—</span>}</td>
                                      }}
                                    </For>
                                  </tr>
                                </Show>
                              )
                            }}
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
                <div class="relative">
                  <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
                  <input
                    class={cx(inputClass, 'pl-8')}
                    value={schemaSearch()}
                    onInput={(event) => setSchemaSearch(event.currentTarget.value)}
                    placeholder="Search schema (tables, columns, types, SQL)"
                  />
                </div>
                <div class="max-h-[360px] space-y-2.5 overflow-auto">
                  <Show when={!isLoadingSchema()} fallback={<Spinner label="Loading schema…" />}>
                    <For each={filteredSchema()}>
                      {(object) => (
                        <article class="rounded-md border border-line bg-surface2/40 p-3">
                          <h4 class="font-mono text-xs font-semibold text-ink">
                            <HighlightedText text={`${object.kind}: ${object.name}`} term={schemaSearch()} />
                          </h4>
                          <p class="mt-1 font-mono text-2xs text-faint">
                            <HighlightedText text={object.createSql} term={schemaSearch()} />
                          </p>
                          <ul class="mt-2 space-y-1 font-mono text-xs text-muted">
                            <For each={object.columns}>
                              {(column) => (
                                <li>
                                  <HighlightedText text={column.name} term={schemaSearch()} /> <span class="text-faint"><HighlightedText text={column.dataType} term={schemaSearch()} /></span>
                                  <Show when={column.primaryKey}><span class="ml-1 rounded bg-brass/10 px-1 text-brass-strong"><HighlightedText text="PK" term={schemaSearch()} /></span></Show>
                                  <Show when={column.notNull}><span class="ml-1 rounded bg-surface2 px-1"><HighlightedText text="NOT NULL" term={schemaSearch()} /></span></Show>
                                </li>
                              )}
                            </For>
                          </ul>
                        </article>
                      )}
                    </For>
                    <Show when={!isLoadingSchema() && filteredSchema().length === 0}>
                      <p class="text-sm text-muted">No schema entries matched your search.</p>
                    </Show>
                  </Show>
                </div>
              </div>
            </Show>
          </div>
        </SectionCard>
      </div>
    </div>
  )
}
