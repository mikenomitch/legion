import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { getStoredToken, managerClient, setStoredToken } from '../grpcClient'
import { beginOidcLogin, rememberClientId } from '../utils/oidc'
import {
  JoinTokensPanel,
  PoliciesPanel,
  TokensPanel,
  UsersPanel,
  buttonClass,
  inputClass,
  sectionClass,
} from '../components/acl/AdminPanels'

type Tab = 'identity' | 'policies' | 'tokens' | 'users' | 'workers'

const TABS: { id: Tab; label: string }[] = [
  { id: 'identity', label: 'Identity & Login' },
  { id: 'policies', label: 'Policies' },
  { id: 'tokens', label: 'Tokens' },
  { id: 'users', label: 'Users' },
  { id: 'workers', label: 'Worker Join' },
]

// Access: identity introspection, login (userpass / OIDC / JWT / raw token),
// and the ACL admin panels.
export default function Access() {
  const [whoami, { refetch }] = createResource(async () => managerClient.whoAmI({}))
  const [methods] = createResource(async () => (await managerClient.getAuthMethods({})).methods)
  const [tab, setTab] = createSignal<Tab>('identity')
  const [tokenInput, setTokenInput] = createSignal('')
  const [username, setUsername] = createSignal('')
  const [password, setPassword] = createSignal('')
  const [jwtInput, setJwtInput] = createSignal('')
  const [jwtMethod, setJwtMethod] = createSignal('')
  const [error, setError] = createSignal<string | null>(null)
  const [notice, setNotice] = createSignal<string | null>(null)

  const hasStoredToken = () => getStoredToken() != null
  const oidcMethods = createMemo(() => (methods() ?? []).filter((m) => m.kind === 'oidc'))
  const jwtMethods = createMemo(() => (methods() ?? []).filter((m) => m.kind === 'jwt' || m.kind === 'webhook' || m.kind === 'custom'))

  const applyToken = (secret: string, message: string) => {
    setStoredToken(secret)
    setTokenInput('')
    setPassword('')
    setJwtInput('')
    setError(null)
    setNotice(message)
    void refetch()
  }

  const saveToken = () => {
    if (tokenInput().trim().length === 0) return
    applyToken(tokenInput(), 'Token saved for this browser.')
  }

  const loginUserpass = async (event: Event) => {
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

  const loginJwt = async (event: Event) => {
    event.preventDefault()
    setError(null)
    setNotice(null)
    try {
      const reply = await managerClient.login({ method: jwtMethod(), jwt: jwtInput() })
      applyToken(reply.secret, `Logged in via ${jwtMethod()}.`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const loginOidc = async (methodName: string) => {
    setError(null)
    const method = oidcMethods().find((m) => m.name === methodName)
    if (!method) return
    if (!method.oidcIssuer || !method.oidcClientId) {
      setError(`auth method ${methodName} is missing its issuer or oidcClientId`)
      return
    }
    try {
      rememberClientId(method.oidcClientId)
      await beginOidcLogin({
        name: method.name,
        issuer: method.oidcIssuer,
        clientId: method.oidcClientId,
        scopes: method.oidcScopes,
      })
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

  const tabClass = (id: Tab) =>
    `rounded-t border-x border-t px-3 py-1.5 text-sm font-semibold ${
      tab() === id
        ? 'border-neutral-300 bg-white dark:border-neutral-700 dark:bg-neutral-900'
        : 'border-transparent text-neutral-500 hover:text-neutral-800 dark:hover:text-neutral-200'
    }`

  return (
    <div class="space-y-4">
      <div class="flex gap-1 border-b border-neutral-300 dark:border-neutral-700">
        <For each={TABS}>
          {(entry) => (
            <button type="button" class={tabClass(entry.id)} onClick={() => setTab(entry.id)}>
              {entry.label}
            </button>
          )}
        </For>
      </div>

      <Show when={tab() === 'identity'}>
        <div class="space-y-4">
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
                      {identity().principal}{' '}
                      <span class="text-neutral-500">(via {identity().source})</span>
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
                Log out (remove stored token)
              </button>
            </Show>
          </section>

          <Show when={oidcMethods().length > 0}>
            <section class={sectionClass}>
              <h2 class="text-lg font-semibold">Single sign-on</h2>
              <p class="mt-1 text-sm text-neutral-500">
                Redirects to your identity provider, then exchanges the ID token for a Legion ACL
                token.
              </p>
              <div class="mt-3 flex flex-wrap gap-2">
                <For each={oidcMethods()}>
                  {(method) => (
                    <button type="button" class={buttonClass} onClick={() => void loginOidc(method.name)}>
                      Continue with {method.name}
                    </button>
                  )}
                </For>
              </div>
            </section>
          </Show>

          <section class={sectionClass}>
            <h2 class="text-lg font-semibold">Log in with username / password</h2>
            <form class="mt-3 flex flex-col gap-3 sm:max-w-md" onSubmit={loginUserpass}>
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

          <Show when={jwtMethods().length > 0}>
            <section class={sectionClass}>
              <h2 class="text-lg font-semibold">Exchange an external token</h2>
              <p class="mt-1 text-sm text-neutral-500">
                Paste a JWT issued by your identity system; the selected auth method validates it
                and mints an ACL token.
              </p>
              <form class="mt-3 flex flex-col gap-3 sm:max-w-md" onSubmit={loginJwt}>
                <select
                  class={inputClass}
                  value={jwtMethod()}
                  onChange={(event) => setJwtMethod(event.currentTarget.value)}
                >
                  <option value="" disabled>
                    choose a method
                  </option>
                  <For each={jwtMethods()}>
                    {(method) => <option value={method.name}>{method.name}</option>}
                  </For>
                </select>
                <input
                  class={inputClass}
                  placeholder="eyJ…"
                  value={jwtInput()}
                  onInput={(event) => setJwtInput(event.currentTarget.value)}
                />
                <button type="submit" class={`self-start ${buttonClass}`} disabled={jwtMethod() === ''}>
                  Exchange
                </button>
              </form>
            </section>
          </Show>

          <section class={sectionClass}>
            <h2 class="text-lg font-semibold">Use an existing token</h2>
            <p class="mt-1 text-sm text-neutral-500">
              Paste an ACL token secret. It is stored in this browser only and sent as a bearer
              token on every request.
            </p>
            <div class="mt-3 flex flex-col gap-3 sm:max-w-md">
              <input
                class={inputClass}
                placeholder="lgn_…"
                value={tokenInput()}
                onInput={(event) => setTokenInput(event.currentTarget.value)}
              />
              <button type="button" class={`self-start ${buttonClass}`} onClick={saveToken}>
                Save token
              </button>
            </div>
          </section>
        </div>
      </Show>

      <Show when={tab() === 'policies'}>
        <PoliciesPanel />
      </Show>
      <Show when={tab() === 'tokens'}>
        <TokensPanel />
      </Show>
      <Show when={tab() === 'users'}>
        <UsersPanel />
      </Show>
      <Show when={tab() === 'workers'}>
        <JoinTokensPanel />
      </Show>
    </div>
  )
}
