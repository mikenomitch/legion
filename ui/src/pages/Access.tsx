import { Show, createResource, createSignal } from 'solid-js'
import { getStoredToken, managerClient, setStoredToken } from '../grpcClient'

// Access page: shows how the manager authenticates this browser session and
// lets the user paste an ACL token or log in with username/password.
export default function Access() {
  const [whoami, { refetch }] = createResource(async () => managerClient.whoAmI({}))
  const [tokenInput, setTokenInput] = createSignal('')
  const [username, setUsername] = createSignal('')
  const [password, setPassword] = createSignal('')
  const [error, setError] = createSignal<string | null>(null)
  const [notice, setNotice] = createSignal<string | null>(null)

  const hasStoredToken = () => getStoredToken() != null

  const applyToken = (secret: string, message: string) => {
    setStoredToken(secret)
    setTokenInput('')
    setPassword('')
    setError(null)
    setNotice(message)
    void refetch()
  }

  const saveToken = () => {
    if (tokenInput().trim().length === 0) return
    applyToken(tokenInput(), 'Token saved for this browser.')
  }

  const login = async (event: Event) => {
    event.preventDefault()
    setError(null)
    setNotice(null)
    try {
      const reply = await managerClient.login({
        method: 'userpass',
        username: username(),
        password: password(),
      })
      applyToken(reply.secret, `Logged in as ${reply.token?.name ?? username()}.`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const logout = () => {
    setStoredToken(null)
    setNotice('Token removed.')
    setError(null)
    void refetch()
  }

  const sectionClass =
    'rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60'
  const inputClass =
    'w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900'
  const buttonClass =
    'rounded border border-neutral-900 bg-neutral-900 px-3 py-2 text-sm font-semibold text-white dark:border-neutral-200 dark:bg-neutral-100 dark:text-neutral-900'

  return (
    <div class="space-y-5">
      <section class={sectionClass}>
        <h2 class="text-lg font-semibold">Current identity</h2>
        <Show when={whoami()} fallback={<p class="mt-2 text-sm text-neutral-500">Loading…</p>}>
          {(identity) => (
            <dl class="mt-3 grid gap-x-8 gap-y-2 text-sm sm:grid-cols-2">
              <div>
                <dt class="text-neutral-500">ACLs</dt>
                <dd class="font-medium">
                  {identity().aclEnabled
                    ? identity().auditOnly
                      ? 'Enabled (audit-only)'
                      : 'Enabled'
                    : 'Disabled — every caller has full access'}
                </dd>
              </div>
              <div>
                <dt class="text-neutral-500">Principal</dt>
                <dd class="font-medium">
                  {identity().principal} <span class="text-neutral-500">(via {identity().source})</span>
                </dd>
              </div>
              <div>
                <dt class="text-neutral-500">Token type</dt>
                <dd class="font-medium">{identity().tokenType}</dd>
              </div>
              <div>
                <dt class="text-neutral-500">Policies</dt>
                <dd class="font-medium">
                  {identity().policies.length > 0 ? identity().policies.join(', ') : '—'}
                </dd>
              </div>
              <Show when={identity().expiresAt}>
                <div>
                  <dt class="text-neutral-500">Expires</dt>
                  <dd class="font-medium">{identity().expiresAt}</dd>
                </div>
              </Show>
            </dl>
          )}
        </Show>
        <Show when={notice()}>
          <p class="mt-3 text-sm text-emerald-600 dark:text-emerald-400">{notice()}</p>
        </Show>
        <Show when={error()}>
          <p class="mt-3 text-sm text-rose-600 dark:text-rose-400">{error()}</p>
        </Show>
        <Show when={hasStoredToken()}>
          <button type="button" class={`mt-4 ${buttonClass}`} onClick={logout}>
            Remove stored token
          </button>
        </Show>
      </section>

      <section class={sectionClass}>
        <h2 class="text-lg font-semibold">Log in with username / password</h2>
        <p class="mt-1 text-sm text-neutral-500">
          Exchanges credentials for an ACL token (created with <code>legion acl user upsert</code>).
        </p>
        <form class="mt-3 flex flex-col gap-3 sm:max-w-md" onSubmit={login}>
          <input
            class={inputClass}
            placeholder="username"
            autocomplete="username"
            value={username()}
            onInput={(event) => setUsername(event.currentTarget.value)}
          />
          <input
            class={inputClass}
            placeholder="password"
            type="password"
            autocomplete="current-password"
            value={password()}
            onInput={(event) => setPassword(event.currentTarget.value)}
          />
          <button type="submit" class={`self-start ${buttonClass}`}>
            Log in
          </button>
        </form>
      </section>

      <section class={sectionClass}>
        <h2 class="text-lg font-semibold">Use an existing token</h2>
        <p class="mt-1 text-sm text-neutral-500">
          Paste an ACL token secret (or an OIDC/JWT accepted by a configured auth method). It is
          stored in this browser only and sent as a bearer token on every request.
        </p>
        <div class="mt-3 flex flex-col gap-3 sm:max-w-md">
          <input
            class={inputClass}
            placeholder="lgn_… or eyJ…"
            value={tokenInput()}
            onInput={(event) => setTokenInput(event.currentTarget.value)}
          />
          <button type="button" class={`self-start ${buttonClass}`} onClick={saveToken}>
            Save token
          </button>
        </div>
      </section>
    </div>
  )
}
