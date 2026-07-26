import { For, Show, createResource, createSignal } from 'solid-js'
import { managerClient } from '../../grpcClient'
import { Badge, Button, ConfirmButton, CopyButton, Field, Note, SectionCard, TableShell, checkboxClass, cx, inputClass, tableClass, tdClass, textareaClass, thClass, theadClass, trHoverClass } from '../ui'

function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function Feedback(props: { error: string | null; notice: string | null }) {
  return (
    <>
      <Show when={props.error}>
        <Note tone="danger" class="mt-3">{props.error}</Note>
      </Show>
      <Show when={props.notice}>
        <Note tone="ok" class="mt-3">{props.notice}</Note>
      </Show>
    </>
  )
}

// One-time secret reveal with a copy button (tokens are shown exactly once).
function SecretReveal(props: { label: string; secret: string }) {
  return (
    <div class="mt-3 rounded-md border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
      <p class="font-semibold text-amber-800 dark:text-amber-200">
        {props.label} — shown only once, store it now.
      </p>
      <div class="mt-2 flex items-center gap-2">
        <code class="block flex-1 overflow-x-auto whitespace-nowrap rounded-md border border-line bg-surface px-2.5 py-1.5 font-mono text-xs">
          {props.secret}
        </code>
        <CopyButton text={props.secret} size="sm" />
      </div>
    </div>
  )
}

function PolicyPicker(props: {
  policies: readonly { name: string }[] | undefined
  selected: string[]
  onToggle: (name: string) => void
}) {
  return (
    <div class="flex flex-wrap gap-2">
      <For each={props.policies ?? []}>
        {(policy) => (
          <label
            class={cx(
              'flex cursor-pointer items-center gap-1.5 rounded-md border px-2 py-1 text-xs font-medium transition-colors',
              props.selected.includes(policy.name)
                ? 'border-brass/50 bg-brass/10 text-ink'
                : 'border-line text-muted hover:bg-surface2',
            )}
          >
            <input
              type="checkbox"
              class={checkboxClass}
              checked={props.selected.includes(policy.name)}
              onChange={() => props.onToggle(policy.name)}
            />
            {policy.name}
          </label>
        )}
      </For>
      <Show when={(props.policies ?? []).length === 0}>
        <p class="text-sm text-muted">Create a policy first.</p>
      </Show>
    </div>
  )
}

const EXAMPLE_RULES = `[
  { "resources": ["job"], "actions": ["read", "write"], "names": ["dev-*"] },
  { "resources": ["kv"], "actions": ["read"], "paths": ["dev/*"] },
  { "resources": ["node"], "actions": ["read"] }
]`

export function PoliciesPanel() {
  const [policies, { refetch }] = createResource(async () => (await managerClient.listAclPolicies({})).policies)
  const [name, setName] = createSignal('')
  const [description, setDescription] = createSignal('')
  const [rules, setRules] = createSignal(EXAMPLE_RULES)
  const [error, setError] = createSignal<string | null>(null)
  const [notice, setNotice] = createSignal<string | null>(null)

  const apply = async (event: Event) => {
    event.preventDefault()
    setError(null)
    setNotice(null)
    try {
      await managerClient.upsertAclPolicy({
        name: name(),
        description: description(),
        rulesJson: rules(),
      })
      setNotice(`Saved policy ${name()}.`)
      setName('')
      setDescription('')
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  const remove = async (policyName: string) => {
    setError(null)
    try {
      await managerClient.deleteAclPolicy({ name: policyName })
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  const edit = (policyName: string) => {
    const policy = policies()?.find((p) => p.name === policyName)
    if (!policy) return
    setName(policy.name)
    setDescription(policy.description)
    try {
      setRules(JSON.stringify(JSON.parse(policy.rulesJson), null, 2))
    } catch {
      setRules(policy.rulesJson)
    }
  }

  return (
    <div class="space-y-4">
      <SectionCard
        title="Policies"
        description="Named rule sets over resources (job, node, kv, state, operator, acl) and actions (read, write), optionally scoped by job names, node pools, or KV paths. Deny rules override allows."
        flush
      >
        <Show when={(policies() ?? []).length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No policies yet.</p>}>
          <TableShell>
            <table class={tableClass}>
              <thead class={theadClass}>
                <tr>
                  <th class={thClass}>Name</th>
                  <th class={thClass}>Description</th>
                  <th class={thClass}>Updated</th>
                  <th class={thClass} />
                </tr>
              </thead>
              <tbody class="divide-y divide-line">
                <For each={policies()}>
                  {(policy) => (
                    <tr class={trHoverClass}>
                      <td class={cx(tdClass, 'font-mono text-xs font-medium')}>{policy.name}</td>
                      <td class={cx(tdClass, 'text-muted')}>{policy.description}</td>
                      <td class={cx(tdClass, 'whitespace-nowrap text-xs text-faint')}>{policy.updatedAt}</td>
                      <td class={cx(tdClass, 'text-right')}>
                        <div class="inline-flex items-center gap-1.5">
                          <Button size="xs" onClick={() => edit(policy.name)}>Edit</Button>
                          <ConfirmButton label="Delete" size="xs" onConfirm={() => void remove(policy.name)} />
                        </div>
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </TableShell>
        </Show>
        <div class="px-4 pb-3">
          <Feedback error={error()} notice={notice()} />
        </div>
      </SectionCard>

      <SectionCard title="Create or update a policy">
        <form class="flex flex-col gap-3" onSubmit={apply}>
          <div class="grid gap-3 sm:grid-cols-2">
            <Field label="Name">
              <input class={cx(inputClass, 'font-mono')} placeholder="dev-read-write" value={name()} onInput={(e) => setName(e.currentTarget.value)} />
            </Field>
            <Field label="Description (optional)">
              <input class={inputClass} placeholder="What this policy grants" value={description()} onInput={(e) => setDescription(e.currentTarget.value)} />
            </Field>
          </div>
          <Field label="Rules (JSON)">
            <textarea
              class={cx(textareaClass, 'min-h-44')}
              value={rules()}
              onInput={(e) => setRules(e.currentTarget.value)}
              spellcheck={false}
            />
          </Field>
          <Button type="submit" variant="primary" size="md" class="self-start">
            Save policy
          </Button>
        </form>
      </SectionCard>
    </div>
  )
}

export function TokensPanel() {
  const [tokens, { refetch }] = createResource(async () => (await managerClient.listAclTokens({})).tokens)
  const [policies] = createResource(async () => (await managerClient.listAclPolicies({})).policies)
  const [name, setName] = createSignal('')
  const [selectedPolicies, setSelectedPolicies] = createSignal<string[]>([])
  const [ttl, setTtl] = createSignal('')
  const [secret, setSecret] = createSignal<string | null>(null)
  const [error, setError] = createSignal<string | null>(null)

  const toggle = (policy: string) => {
    setSelectedPolicies((current) =>
      current.includes(policy) ? current.filter((p) => p !== policy) : [...current, policy],
    )
  }

  const create = async (event: Event) => {
    event.preventDefault()
    setError(null)
    setSecret(null)
    try {
      const reply = await managerClient.createAclToken({
        name: name(),
        tokenType: 'client',
        policies: selectedPolicies(),
        ttl: ttl().trim().length > 0 ? ttl() : undefined,
      })
      setSecret(reply.secret)
      setName('')
      setSelectedPolicies([])
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  const revoke = async (accessorId: string) => {
    setError(null)
    try {
      await managerClient.deleteAclToken({ accessorId })
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  return (
    <div class="space-y-4">
      <SectionCard
        title="Tokens"
        description="Secrets are stored hashed and never shown after creation; revoke by accessor ID."
        flush
      >
        <Show when={(tokens() ?? []).length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No tokens yet.</p>}>
          <TableShell>
            <table class={tableClass}>
              <thead class={theadClass}>
                <tr>
                  <th class={thClass}>Accessor</th>
                  <th class={thClass}>Name</th>
                  <th class={thClass}>Type</th>
                  <th class={thClass}>Policies</th>
                  <th class={thClass}>Expires</th>
                  <th class={thClass} />
                </tr>
              </thead>
              <tbody class="divide-y divide-line">
                <For each={tokens()}>
                  {(token) => (
                    <tr class={trHoverClass}>
                      <td class={cx(tdClass, 'font-mono text-xs')}>{token.accessorId}</td>
                      <td class={tdClass}>{token.name}</td>
                      <td class={cx(tdClass, 'text-muted')}>{token.tokenType}</td>
                      <td class={tdClass}>
                        <span class="inline-flex flex-wrap gap-1">
                          <For each={token.policies}>{(policy) => <Badge tone="neutral">{policy}</Badge>}</For>
                        </span>
                      </td>
                      <td class={cx(tdClass, 'whitespace-nowrap text-xs text-faint')}>{token.expiresAt ?? 'never'}</td>
                      <td class={cx(tdClass, 'text-right')}>
                        <ConfirmButton label="Revoke" size="xs" onConfirm={() => void revoke(token.accessorId)} />
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </TableShell>
        </Show>
        <div class="px-4 pb-3">
          <Feedback error={error()} notice={null} />
        </div>
      </SectionCard>

      <SectionCard title="Create a client token">
        <form class="flex max-w-lg flex-col gap-3" onSubmit={create}>
          <Field label="Token name">
            <input class={inputClass} placeholder="e.g. ci-deployer" value={name()} onInput={(e) => setName(e.currentTarget.value)} />
          </Field>
          <Field label="Policies">
            <PolicyPicker policies={policies()} selected={selectedPolicies()} onToggle={toggle} />
          </Field>
          <Field label="TTL (optional)" hint="e.g. 720h — empty means no expiry.">
            <input class={cx(inputClass, 'font-mono')} placeholder="720h" value={ttl()} onInput={(e) => setTtl(e.currentTarget.value)} />
          </Field>
          <Button type="submit" variant="primary" size="md" class="self-start">
            Create token
          </Button>
        </form>
        <Show when={secret()}>{(value) => <SecretReveal label="Token secret" secret={value()} />}</Show>
      </SectionCard>
    </div>
  )
}

export function UsersPanel() {
  const [users, { refetch }] = createResource(async () => (await managerClient.listAclUsers({})).users)
  const [policies] = createResource(async () => (await managerClient.listAclPolicies({})).policies)
  const [username, setUsername] = createSignal('')
  const [password, setPassword] = createSignal('')
  const [selectedPolicies, setSelectedPolicies] = createSignal<string[]>([])
  const [error, setError] = createSignal<string | null>(null)
  const [notice, setNotice] = createSignal<string | null>(null)

  const toggle = (policy: string) => {
    setSelectedPolicies((current) =>
      current.includes(policy) ? current.filter((p) => p !== policy) : [...current, policy],
    )
  }

  const upsert = async (event: Event) => {
    event.preventDefault()
    setError(null)
    setNotice(null)
    try {
      await managerClient.upsertAclUser({
        username: username(),
        password: password(),
        policies: selectedPolicies(),
      })
      setNotice(`Saved user ${username()}. They can now run \`legion login -u ${username()}\` or use the form on this page.`)
      setUsername('')
      setPassword('')
      setSelectedPolicies([])
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  const remove = async (name: string) => {
    setError(null)
    try {
      await managerClient.deleteAclUser({ username: name })
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  return (
    <div class="space-y-4">
      <SectionCard
        title="Users"
        description="Username/password identities for the built-in login form (argon2id-hashed)."
        flush
      >
        <Show when={(users() ?? []).length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No users yet.</p>}>
          <TableShell>
            <table class={tableClass}>
              <thead class={theadClass}>
                <tr>
                  <th class={thClass}>Username</th>
                  <th class={thClass}>Policies</th>
                  <th class={thClass}>Updated</th>
                  <th class={thClass} />
                </tr>
              </thead>
              <tbody class="divide-y divide-line">
                <For each={users()}>
                  {(user) => (
                    <tr class={trHoverClass}>
                      <td class={cx(tdClass, 'font-mono text-xs font-medium')}>{user.username}</td>
                      <td class={tdClass}>
                        <span class="inline-flex flex-wrap gap-1">
                          <For each={user.policies}>{(policy) => <Badge tone="neutral">{policy}</Badge>}</For>
                        </span>
                      </td>
                      <td class={cx(tdClass, 'whitespace-nowrap text-xs text-faint')}>{user.updatedAt}</td>
                      <td class={cx(tdClass, 'text-right')}>
                        <ConfirmButton label="Delete" size="xs" onConfirm={() => void remove(user.username)} />
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </TableShell>
        </Show>
        <div class="px-4 pb-3">
          <Feedback error={error()} notice={notice()} />
        </div>
      </SectionCard>

      <SectionCard title="Create or update a user">
        <form class="flex max-w-lg flex-col gap-3" onSubmit={upsert}>
          <Field label="Username">
            <input class={inputClass} autocomplete="off" value={username()} onInput={(e) => setUsername(e.currentTarget.value)} />
          </Field>
          <Field label="Password" hint="Leave empty to keep the current password.">
            <input
              class={inputClass}
              type="password"
              autocomplete="new-password"
              value={password()}
              onInput={(e) => setPassword(e.currentTarget.value)}
            />
          </Field>
          <Field label="Policies">
            <PolicyPicker policies={policies()} selected={selectedPolicies()} onToggle={toggle} />
          </Field>
          <Button type="submit" variant="primary" size="md" class="self-start">
            Save user
          </Button>
        </form>
      </SectionCard>
    </div>
  )
}

// The create-a-join-token form on its own, so the Workers page can offer it
// too: "a worker is missing" is discovered there, not on the Access page.
export function CreateJoinTokenForm(props: { onCreated?: () => void }) {
  const [workerType, setWorkerType] = createSignal('default')
  const [ttl, setTtl] = createSignal('24h')
  const [description, setDescription] = createSignal('')
  const [joinCommand, setJoinCommand] = createSignal<string | null>(null)
  const [error, setError] = createSignal<string | null>(null)

  const create = async (event: Event) => {
    event.preventDefault()
    setError(null)
    setJoinCommand(null)
    try {
      const reply = await managerClient.createJoinToken({
        workerType: workerType(),
        ttl: ttl().trim().length > 0 ? ttl() : undefined,
        description: description(),
      })
      setJoinCommand(`legion join ${reply.token}`)
      setDescription('')
      props.onCreated?.()
    } catch (err) {
      setError(errorText(err))
    }
  }

  return (
    <>
      <form class="flex max-w-lg flex-col gap-3" onSubmit={create}>
        <Field label="Worker type">
          <input class={inputClass} value={workerType()} onInput={(e) => setWorkerType(e.currentTarget.value)} />
        </Field>
        <Field label="TTL" hint="e.g. 24h — use 'none' to disable expiry.">
          <input class={cx(inputClass, 'font-mono')} value={ttl()} onInput={(e) => setTtl(e.currentTarget.value)} />
        </Field>
        <Field label="Description (optional)">
          <input class={inputClass} value={description()} onInput={(e) => setDescription(e.currentTarget.value)} />
        </Field>
        <Button type="submit" variant="primary" size="md" class="self-start">
          Create join token
        </Button>
      </form>
      <Feedback error={error()} notice={null} />
      <Show when={joinCommand()}>
        {(value) => <SecretReveal label="Run on the worker host" secret={value()} />}
      </Show>
    </>
  )
}

export function JoinTokensPanel() {
  const [tokens, { refetch }] = createResource(async () => (await managerClient.listJoinTokens({})).tokens)
  const [error, setError] = createSignal<string | null>(null)

  const revoke = async (id: string) => {
    setError(null)
    try {
      await managerClient.deleteJoinToken({ id })
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

  return (
    <div class="space-y-4">
      <SectionCard
        title="Worker join tokens"
        description="A join token introduces a worker in one command. Create one, then run the printed command on the worker host; the token pins the worker type and the manager address."
        flush
      >
        <Show when={(tokens() ?? []).length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No join tokens.</p>}>
          <TableShell>
            <table class={tableClass}>
              <thead class={theadClass}>
                <tr>
                  <th class={thClass}>ID</th>
                  <th class={thClass}>Worker type</th>
                  <th class={thClass}>Description</th>
                  <th class={thClass}>Expires</th>
                  <th class={thClass} />
                </tr>
              </thead>
              <tbody class="divide-y divide-line">
                <For each={tokens()}>
                  {(token) => (
                    <tr class={trHoverClass}>
                      <td class={cx(tdClass, 'font-mono text-xs')}>{token.id}</td>
                      <td class={tdClass}>{token.workerType}</td>
                      <td class={cx(tdClass, 'text-muted')}>{token.description}</td>
                      <td class={cx(tdClass, 'whitespace-nowrap text-xs text-faint')}>{token.expiresAt ?? 'never'}</td>
                      <td class={cx(tdClass, 'text-right')}>
                        <ConfirmButton label="Revoke" size="xs" onConfirm={() => void revoke(token.id)} />
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </TableShell>
        </Show>
        <div class="px-4 pb-3">
          <Feedback error={error()} notice={null} />
        </div>
      </SectionCard>

      <SectionCard title="Create a join token">
        <CreateJoinTokenForm onCreated={() => void refetch()} />
      </SectionCard>
    </div>
  )
}
