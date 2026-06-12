import { For, Show, createResource, createSignal } from 'solid-js'
import { managerClient } from '../../grpcClient'

// Shared styling for the ACL admin panels.
export const sectionClass =
  'rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60'
export const inputClass =
  'w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm font-mono dark:border-neutral-700 dark:bg-neutral-900'
export const buttonClass =
  'rounded border border-neutral-900 bg-neutral-900 px-3 py-1.5 text-sm font-semibold text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900'
export const dangerButtonClass =
  'rounded border border-rose-300 px-2 py-1 text-xs font-semibold text-rose-600 hover:bg-rose-50 dark:border-rose-800 dark:text-rose-400 dark:hover:bg-rose-900/30'
const tableHeadClass = 'px-2 py-1.5 text-left text-xs font-medium uppercase tracking-wide text-neutral-500'
const tableCellClass = 'px-2 py-1.5 align-top'

function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function Feedback(props: { error: string | null; notice: string | null }) {
  return (
    <>
      <Show when={props.error}>
        <p class="mt-2 text-sm text-rose-600 dark:text-rose-400">{props.error}</p>
      </Show>
      <Show when={props.notice}>
        <p class="mt-2 text-sm text-emerald-600 dark:text-emerald-400">{props.notice}</p>
      </Show>
    </>
  )
}

// One-time secret reveal with a copy button (tokens are shown exactly once).
function SecretReveal(props: { label: string; secret: string }) {
  const [copied, setCopied] = createSignal(false)
  const copy = async () => {
    await navigator.clipboard.writeText(props.secret)
    setCopied(true)
  }
  return (
    <div class="mt-3 rounded border border-amber-300 bg-amber-50 p-3 text-sm dark:border-amber-700 dark:bg-amber-900/20">
      <p class="font-semibold text-amber-800 dark:text-amber-200">
        {props.label} — shown only once, store it now.
      </p>
      <div class="mt-2 flex items-center gap-2">
        <code class="block flex-1 overflow-x-auto whitespace-nowrap rounded bg-white/70 px-2 py-1 dark:bg-neutral-900/60">
          {props.secret}
        </code>
        <button type="button" class={buttonClass} onClick={copy}>
          {copied() ? 'Copied' : 'Copy'}
        </button>
      </div>
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
      <section class={sectionClass}>
        <h3 class="font-semibold">Policies</h3>
        <p class="mt-1 text-sm text-neutral-500">
          Named rule sets over resources (job, node, kv, state, operator, acl) and actions
          (read, write), optionally scoped by job names, node pools, or KV paths. Deny rules
          override allows.
        </p>
        <Show when={(policies() ?? []).length > 0} fallback={<p class="mt-3 text-sm text-neutral-500">No policies yet.</p>}>
          <table class="mt-3 w-full text-sm">
            <thead>
              <tr>
                <th class={tableHeadClass}>Name</th>
                <th class={tableHeadClass}>Description</th>
                <th class={tableHeadClass}>Updated</th>
                <th class={tableHeadClass} />
              </tr>
            </thead>
            <tbody>
              <For each={policies()}>
                {(policy) => (
                  <tr class="border-t border-neutral-200 dark:border-neutral-800">
                    <td class={`${tableCellClass} font-mono`}>{policy.name}</td>
                    <td class={tableCellClass}>{policy.description}</td>
                    <td class={`${tableCellClass} text-neutral-500`}>{policy.updatedAt}</td>
                    <td class={`${tableCellClass} text-right`}>
                      <button type="button" class="mr-2 text-xs underline" onClick={() => edit(policy.name)}>
                        edit
                      </button>
                      <button type="button" class={dangerButtonClass} onClick={() => void remove(policy.name)}>
                        delete
                      </button>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </Show>
        <Feedback error={error()} notice={notice()} />
      </section>

      <section class={sectionClass}>
        <h3 class="font-semibold">Create or update a policy</h3>
        <form class="mt-3 flex flex-col gap-3" onSubmit={apply}>
          <input class={inputClass} placeholder="policy name" value={name()} onInput={(e) => setName(e.currentTarget.value)} />
          <input
            class={inputClass}
            placeholder="description (optional)"
            value={description()}
            onInput={(e) => setDescription(e.currentTarget.value)}
          />
          <textarea
            class={`${inputClass} min-h-44`}
            value={rules()}
            onInput={(e) => setRules(e.currentTarget.value)}
            spellcheck={false}
          />
          <button type="submit" class={`self-start ${buttonClass}`}>
            Save policy
          </button>
        </form>
      </section>
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
      <section class={sectionClass}>
        <h3 class="font-semibold">Tokens</h3>
        <p class="mt-1 text-sm text-neutral-500">
          Secrets are stored hashed and never shown after creation; revoke by accessor ID.
        </p>
        <Show when={(tokens() ?? []).length > 0} fallback={<p class="mt-3 text-sm text-neutral-500">No tokens yet.</p>}>
          <table class="mt-3 w-full text-sm">
            <thead>
              <tr>
                <th class={tableHeadClass}>Accessor</th>
                <th class={tableHeadClass}>Name</th>
                <th class={tableHeadClass}>Type</th>
                <th class={tableHeadClass}>Policies</th>
                <th class={tableHeadClass}>Expires</th>
                <th class={tableHeadClass} />
              </tr>
            </thead>
            <tbody>
              <For each={tokens()}>
                {(token) => (
                  <tr class="border-t border-neutral-200 dark:border-neutral-800">
                    <td class={`${tableCellClass} font-mono text-xs`}>{token.accessorId}</td>
                    <td class={tableCellClass}>{token.name}</td>
                    <td class={tableCellClass}>{token.tokenType}</td>
                    <td class={tableCellClass}>{token.policies.join(', ')}</td>
                    <td class={`${tableCellClass} text-neutral-500`}>{token.expiresAt ?? 'never'}</td>
                    <td class={`${tableCellClass} text-right`}>
                      <button type="button" class={dangerButtonClass} onClick={() => void revoke(token.accessorId)}>
                        revoke
                      </button>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </Show>
        <Feedback error={error()} notice={null} />
      </section>

      <section class={sectionClass}>
        <h3 class="font-semibold">Create a client token</h3>
        <form class="mt-3 flex flex-col gap-3 sm:max-w-md" onSubmit={create}>
          <input class={inputClass} placeholder="token name (e.g. ci-deployer)" value={name()} onInput={(e) => setName(e.currentTarget.value)} />
          <div class="flex flex-wrap gap-2">
            <For each={policies() ?? []}>
              {(policy) => (
                <label class="flex items-center gap-1.5 rounded border border-neutral-300 px-2 py-1 text-sm dark:border-neutral-700">
                  <input
                    type="checkbox"
                    checked={selectedPolicies().includes(policy.name)}
                    onChange={() => toggle(policy.name)}
                  />
                  {policy.name}
                </label>
              )}
            </For>
            <Show when={(policies() ?? []).length === 0}>
              <p class="text-sm text-neutral-500">Create a policy first.</p>
            </Show>
          </div>
          <input class={inputClass} placeholder="ttl, e.g. 720h (optional)" value={ttl()} onInput={(e) => setTtl(e.currentTarget.value)} />
          <button type="submit" class={`self-start ${buttonClass}`}>
            Create token
          </button>
        </form>
        <Show when={secret()}>{(value) => <SecretReveal label="Token secret" secret={value()} />}</Show>
      </section>
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
      <section class={sectionClass}>
        <h3 class="font-semibold">Users</h3>
        <p class="mt-1 text-sm text-neutral-500">
          Username/password identities for the built-in login form (argon2id-hashed).
        </p>
        <Show when={(users() ?? []).length > 0} fallback={<p class="mt-3 text-sm text-neutral-500">No users yet.</p>}>
          <table class="mt-3 w-full text-sm">
            <thead>
              <tr>
                <th class={tableHeadClass}>Username</th>
                <th class={tableHeadClass}>Policies</th>
                <th class={tableHeadClass}>Updated</th>
                <th class={tableHeadClass} />
              </tr>
            </thead>
            <tbody>
              <For each={users()}>
                {(user) => (
                  <tr class="border-t border-neutral-200 dark:border-neutral-800">
                    <td class={`${tableCellClass} font-mono`}>{user.username}</td>
                    <td class={tableCellClass}>{user.policies.join(', ')}</td>
                    <td class={`${tableCellClass} text-neutral-500`}>{user.updatedAt}</td>
                    <td class={`${tableCellClass} text-right`}>
                      <button type="button" class={dangerButtonClass} onClick={() => void remove(user.username)}>
                        delete
                      </button>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </Show>
        <Feedback error={error()} notice={notice()} />
      </section>

      <section class={sectionClass}>
        <h3 class="font-semibold">Create or update a user</h3>
        <form class="mt-3 flex flex-col gap-3 sm:max-w-md" onSubmit={upsert}>
          <input class={inputClass} placeholder="username" value={username()} onInput={(e) => setUsername(e.currentTarget.value)} />
          <input
            class={inputClass}
            placeholder="password (leave empty to keep current)"
            type="password"
            value={password()}
            onInput={(e) => setPassword(e.currentTarget.value)}
          />
          <div class="flex flex-wrap gap-2">
            <For each={policies() ?? []}>
              {(policy) => (
                <label class="flex items-center gap-1.5 rounded border border-neutral-300 px-2 py-1 text-sm dark:border-neutral-700">
                  <input
                    type="checkbox"
                    checked={selectedPolicies().includes(policy.name)}
                    onChange={() => toggle(policy.name)}
                  />
                  {policy.name}
                </label>
              )}
            </For>
          </div>
          <button type="submit" class={`self-start ${buttonClass}`}>
            Save user
          </button>
        </form>
      </section>
    </div>
  )
}

export function JoinTokensPanel() {
  const [tokens, { refetch }] = createResource(async () => (await managerClient.listJoinTokens({})).tokens)
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
      void refetch()
    } catch (err) {
      setError(errorText(err))
    }
  }

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
      <section class={sectionClass}>
        <h3 class="font-semibold">Worker join tokens</h3>
        <p class="mt-1 text-sm text-neutral-500">
          A join token introduces a worker in one command. Create one, then run the printed
          command on the worker host; the token pins the worker type and the manager address.
        </p>
        <Show when={(tokens() ?? []).length > 0} fallback={<p class="mt-3 text-sm text-neutral-500">No join tokens.</p>}>
          <table class="mt-3 w-full text-sm">
            <thead>
              <tr>
                <th class={tableHeadClass}>ID</th>
                <th class={tableHeadClass}>Worker type</th>
                <th class={tableHeadClass}>Description</th>
                <th class={tableHeadClass}>Expires</th>
                <th class={tableHeadClass} />
              </tr>
            </thead>
            <tbody>
              <For each={tokens()}>
                {(token) => (
                  <tr class="border-t border-neutral-200 dark:border-neutral-800">
                    <td class={`${tableCellClass} font-mono text-xs`}>{token.id}</td>
                    <td class={tableCellClass}>{token.workerType}</td>
                    <td class={tableCellClass}>{token.description}</td>
                    <td class={`${tableCellClass} text-neutral-500`}>{token.expiresAt ?? 'never'}</td>
                    <td class={`${tableCellClass} text-right`}>
                      <button type="button" class={dangerButtonClass} onClick={() => void revoke(token.id)}>
                        revoke
                      </button>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </Show>
        <Feedback error={error()} notice={null} />
      </section>

      <section class={sectionClass}>
        <h3 class="font-semibold">Create a join token</h3>
        <form class="mt-3 flex flex-col gap-3 sm:max-w-md" onSubmit={create}>
          <input class={inputClass} placeholder="worker type" value={workerType()} onInput={(e) => setWorkerType(e.currentTarget.value)} />
          <input class={inputClass} placeholder="ttl, e.g. 24h ('none' to disable expiry)" value={ttl()} onInput={(e) => setTtl(e.currentTarget.value)} />
          <input class={inputClass} placeholder="description (optional)" value={description()} onInput={(e) => setDescription(e.currentTarget.value)} />
          <button type="submit" class={`self-start ${buttonClass}`}>
            Create join token
          </button>
        </form>
        <Show when={joinCommand()}>
          {(value) => <SecretReveal label="Run on the worker host" secret={value()} />}
        </Show>
      </section>
    </div>
  )
}
