import { For, Show, createMemo, createResource, createSignal } from 'solid-js'
import { getStoredToken, managerClient, setStoredToken } from '../grpcClient'
import { canRead } from '../utils/acl'
import { beginOidcLogin, rememberClientId } from '../utils/oidc'
import { identityError, managerIdentity, refreshManagerIdentity } from '../utils/connection'
import { Badge, Button, DefList, Field, Note, SectionCard, Spinner, StatusBadge, Tabs, cx, inputClass, selectClass } from '../components/ui'
import {
  JoinTokensPanel,
  PoliciesPanel,
  TokensPanel,
  UsersPanel,
} from '../components/acl/AdminPanels'

type Tab = 'identity' | 'policies' | 'tokens' | 'users' | 'workers'

const TABS: { id: Tab; label: string }[] = [
  { id: 'identity', label: 'Identity & Login' },
  { id: 'policies', label: 'Policies' },
  { id: 'tokens', label: 'Tokens' },
  { id: 'users', label: 'Users' },
  { id: 'workers', label: 'Worker Join' },
]

// Admin tabs need acl:read (worker join needs node:read); identity/login is
// for everyone. Cosmetic — the panels' RPCs are enforced server-side.
const visibleTabs = () =>
  TABS.filter((entry) => {
    if (entry.id === 'identity') return true
    if (entry.id === 'workers') return canRead('node')
    return canRead('acl')
  })

/** The control a message belongs to, so it renders where it was triggered. */
type FeedbackSource = 'identity' | 'userpass' | 'jwt' | 'sso' | 'token'

// Access: identity introspection, login (userpass / OIDC / JWT / raw token),
// and the ACL admin panels.
export default function Access() {
  // Identity is shared with the ACL banner, so logging in or out here updates
  // both. `refetch` re-reads it with whatever token is now stored.
  const whoami = managerIdentity
  const refetch = refreshManagerIdentity
  const [methods] = createResource(async () => (await managerClient.getAuthMethods({})).methods)
  const [tab, setTab] = createSignal<Tab>('identity')
  const [tokenInput, setTokenInput] = createSignal('')
  const [username, setUsername] = createSignal('')
  const [password, setPassword] = createSignal('')
  const [jwtInput, setJwtInput] = createSignal('')
  const [jwtMethod, setJwtMethod] = createSignal('')
  const [error, setError] = createSignal<string | null>(null)
  const [notice, setNotice] = createSignal<string | null>(null)
  // Which control produced the current message. The login forms sit well
  // below the identity card, so rendering every result up there reads as
  // "nothing happened" when you press a button further down the page.
  const [feedbackFrom, setFeedbackFrom] = createSignal<FeedbackSource>('identity')

  const report = (source: FeedbackSource, result: { error?: string; notice?: string }) => {
    setFeedbackFrom(source)
    setError(result.error ?? null)
    setNotice(result.notice ?? null)
  }

  const failed = (source: FeedbackSource, err: unknown) =>
    report(source, { error: err instanceof Error ? err.message : String(err) })

  /** Messages for one control; renders nothing for the others. */
  const Feedback = (props: { source: FeedbackSource }) => (
    <Show when={feedbackFrom() === props.source}>
      <Show when={notice()}>
        <Note tone="ok" class="mt-3" onDismiss={() => setNotice(null)}>{notice()}</Note>
      </Show>
      <Show when={error()}>
        <Note tone="danger" class="mt-3" onDismiss={() => setError(null)}>{error()}</Note>
      </Show>
    </Show>
  )

  const hasStoredToken = () => getStoredToken() != null
  const oidcMethods = createMemo(() => (methods() ?? []).filter((m) => m.kind === 'oidc'))
  const jwtMethods = createMemo(() => (methods() ?? []).filter((m) => m.kind === 'jwt' || m.kind === 'webhook' || m.kind === 'custom'))

  const applyToken = (source: FeedbackSource, secret: string, message: string) => {
    setStoredToken(secret)
    setTokenInput('')
    setPassword('')
    setJwtInput('')
    report(source, { notice: message })
    void refetch()
  }

  const saveToken = () => {
    if (tokenInput().trim().length === 0) {
      report('token', { error: 'Paste a token secret first.' })
      return
    }
    applyToken('token', tokenInput(), 'Token saved for this browser.')
  }

  const loginUserpass = async (event: Event) => {
    event.preventDefault()
    report('userpass', {})
    try {
      const reply = await managerClient.login({
        method: 'userpass',
        username: username(),
        password: password(),
      })
      applyToken('userpass', reply.secret, `Logged in as ${reply.token?.name ?? username()}.`)
    } catch (err) {
      failed('userpass', err)
    }
  }

  const loginJwt = async (event: Event) => {
    event.preventDefault()
    report('jwt', {})
    try {
      const reply = await managerClient.login({ method: jwtMethod(), jwt: jwtInput() })
      applyToken('jwt', reply.secret, `Logged in via ${jwtMethod()}.`)
    } catch (err) {
      failed('jwt', err)
    }
  }

  const loginOidc = async (methodName: string) => {
    report('sso', {})
    const method = oidcMethods().find((m) => m.name === methodName)
    if (!method) return
    if (!method.oidcIssuer || !method.oidcClientId) {
      report('sso', { error: `auth method ${methodName} is missing its issuer or oidcClientId` })
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
      failed('sso', err)
    }
  }

  const logout = () => {
    setStoredToken(null)
    report('identity', { notice: 'Token removed.' })
    void refetch()
  }

  return (
    <div>
      <Tabs class="mb-5" tabs={visibleTabs()} active={tab()} onChange={(id) => setTab(id as Tab)} />

      <Show when={tab() === 'identity'}>
        <div class="space-y-4">
          <SectionCard
            title="Current identity"
            actions={
              <Show when={hasStoredToken()}>
                <Button icon="logOut" onClick={logout}>Log out</Button>
              </Show>
            }
          >
            <Show
              when={whoami()}
              fallback={
                <Show when={identityError()} fallback={<Spinner label="Loading identity…" />}>
                  <Note tone="danger">
                    The manager rejected this browser's credential: {identityError()}. Log out to
                    clear it, or paste a valid token below.
                  </Note>
                </Show>
              }
            >
              {(identity) => (
                <DefList
                  cols={3}
                  items={[
                    [
                      'ACLs',
                      identity().aclEnabled ? (
                        <StatusBadge status={identity().auditOnly ? 'enabled (audit-only)' : 'enabled'} />
                      ) : (
                        <span class="inline-flex items-center gap-2">
                          <StatusBadge status="disabled" />
                          <span class="text-xs text-faint">every caller has full access</span>
                        </span>
                      ),
                    ],
                    [
                      'Principal',
                      <>
                        {identity().principal} <span class="text-faint">(via {identity().source})</span>
                      </>,
                    ],
                    ['Token type', identity().tokenType],
                    [
                      'Policies',
                      identity().policies.length > 0 ? (
                        <span class="inline-flex flex-wrap gap-1">
                          <For each={identity().policies}>{(policy) => <Badge tone="neutral">{policy}</Badge>}</For>
                        </span>
                      ) : (
                        <span class="text-faint">—</span>
                      ),
                    ],
                    ...(identity().expiresAt ? ([['Expires', identity().expiresAt]] as Array<[string, string]>) : []),
                  ]}
                />
              )}
            </Show>
            <Feedback source="identity" />
          </SectionCard>

          <Show when={oidcMethods().length > 0}>
            <SectionCard
              title="Single sign-on"
              description="Redirects to your identity provider, then exchanges the ID token for a Legion ACL token."
            >
              <div class="flex flex-wrap gap-2">
                <For each={oidcMethods()}>
                  {(method) => (
                    <Button variant="primary" size="md" onClick={() => void loginOidc(method.name)}>
                      Continue with {method.name}
                    </Button>
                  )}
                </For>
              </div>
              <Feedback source="sso" />
            </SectionCard>
          </Show>

          <div class="grid items-start gap-4 lg:grid-cols-2">
            <SectionCard title="Log in with username / password">
              <form class="flex flex-col gap-3" onSubmit={loginUserpass}>
                <Field label="Username">
                  <input
                    class={inputClass}
                    autocomplete="username"
                    value={username()}
                    onInput={(event) => setUsername(event.currentTarget.value)}
                  />
                </Field>
                <Field label="Password">
                  <input
                    class={inputClass}
                    type="password"
                    autocomplete="current-password"
                    value={password()}
                    onInput={(event) => setPassword(event.currentTarget.value)}
                  />
                </Field>
                <Button type="submit" variant="primary" size="md" class="self-start">
                  Log in
                </Button>
                <Feedback source="userpass" />
              </form>
            </SectionCard>

            <SectionCard
              title="Use an existing token"
              description="Paste an ACL token secret. It is stored in this browser only and sent as a bearer token on every request."
            >
              <div class="flex flex-col gap-3">
                <Field label="Token secret">
                  <input
                    class={cx(inputClass, 'font-mono')}
                    placeholder="lgn_…"
                    value={tokenInput()}
                    onInput={(event) => setTokenInput(event.currentTarget.value)}
                  />
                </Field>
                <Button size="md" class="self-start" onClick={saveToken}>
                  Save token
                </Button>
              </div>
            </SectionCard>
          </div>

          <Show when={jwtMethods().length > 0}>
            <SectionCard
              title="Exchange an external token"
              description="Paste a JWT issued by your identity system; the selected auth method validates it and mints an ACL token."
            >
              <form class="flex max-w-lg flex-col gap-3" onSubmit={loginJwt}>
                <Field label="Auth method">
                  <select
                    class={cx(selectClass, "w-full")}
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
                </Field>
                <Field label="JWT">
                  <input
                    class={cx(inputClass, 'font-mono')}
                    placeholder="eyJ…"
                    value={jwtInput()}
                    onInput={(event) => setJwtInput(event.currentTarget.value)}
                  />
                </Field>
                <Button type="submit" variant="primary" size="md" class="self-start" disabled={jwtMethod() === ''}>
                  Exchange
                </Button>
              </form>
            </SectionCard>
          </Show>
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
