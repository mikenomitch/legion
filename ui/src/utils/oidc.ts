// Browser-side OIDC login: standard authorization-code flow with PKCE for a
// public client. The UI redirects to the identity provider, receives the
// code on /oidc/callback, exchanges it for an ID token directly at the IdP's
// token endpoint, and finally trades that ID token for a Legion ACL token via
// the manager's Login RPC.
//
// The redirect URI (`<ui origin>/oidc/callback`) must be registered with the
// identity provider for the configured client ID.

const VERIFIER_KEY = 'legion-oidc-verifier'
const STATE_KEY = 'legion-oidc-state'
const METHOD_KEY = 'legion-oidc-method'
const TOKEN_ENDPOINT_KEY = 'legion-oidc-token-endpoint'

export type OidcMethodConfig = {
  name: string
  issuer: string
  clientId: string
  scopes: string[]
}

function base64Url(bytes: Uint8Array): string {
  let raw = ''
  bytes.forEach((b) => {
    raw += String.fromCharCode(b)
  })
  return btoa(raw).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function randomString(bytes: number): string {
  const buf = new Uint8Array(bytes)
  crypto.getRandomValues(buf)
  return base64Url(buf)
}

async function sha256(input: string): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(input))
  return new Uint8Array(digest)
}

export function callbackUri(): string {
  return `${window.location.origin}/oidc/callback`
}

async function discover(issuer: string): Promise<{ authorizationEndpoint: string; tokenEndpoint: string }> {
  const url = `${issuer.replace(/\/+$/, '')}/.well-known/openid-configuration`
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`failed to fetch OIDC discovery document (${response.status})`)
  }
  const doc = await response.json()
  if (!doc.authorization_endpoint || !doc.token_endpoint) {
    throw new Error('discovery document is missing authorization/token endpoints')
  }
  return { authorizationEndpoint: doc.authorization_endpoint, tokenEndpoint: doc.token_endpoint }
}

// Starts the redirect flow; navigates the browser away on success.
export async function beginOidcLogin(method: OidcMethodConfig): Promise<void> {
  const endpoints = await discover(method.issuer)
  const verifier = randomString(48)
  const state = randomString(16)
  const challenge = base64Url(await sha256(verifier))

  sessionStorage.setItem(VERIFIER_KEY, verifier)
  sessionStorage.setItem(STATE_KEY, state)
  sessionStorage.setItem(METHOD_KEY, method.name)
  sessionStorage.setItem(TOKEN_ENDPOINT_KEY, endpoints.tokenEndpoint)

  const url = new URL(endpoints.authorizationEndpoint)
  url.searchParams.set('response_type', 'code')
  url.searchParams.set('client_id', method.clientId)
  url.searchParams.set('redirect_uri', callbackUri())
  url.searchParams.set('scope', method.scopes.length > 0 ? method.scopes.join(' ') : 'openid')
  url.searchParams.set('state', state)
  url.searchParams.set('code_challenge', challenge)
  url.searchParams.set('code_challenge_method', 'S256')
  window.location.assign(url.toString())
}

// Completes the flow on /oidc/callback: validates state, exchanges the code,
// and returns the method name plus the ID token for the manager Login RPC.
export async function completeOidcLogin(
  query: URLSearchParams,
  clientId: string,
): Promise<{ method: string; idToken: string }> {
  const error = query.get('error')
  if (error) {
    throw new Error(`identity provider returned an error: ${error}`)
  }
  const state = sessionStorage.getItem(STATE_KEY)
  const verifier = sessionStorage.getItem(VERIFIER_KEY)
  const method = sessionStorage.getItem(METHOD_KEY)
  const tokenEndpoint = sessionStorage.getItem(TOKEN_ENDPOINT_KEY)
  sessionStorage.removeItem(STATE_KEY)
  sessionStorage.removeItem(VERIFIER_KEY)
  sessionStorage.removeItem(METHOD_KEY)
  sessionStorage.removeItem(TOKEN_ENDPOINT_KEY)

  if (!state || !verifier || !method || !tokenEndpoint) {
    throw new Error('no OIDC login in progress (start again from the Access page)')
  }
  if (query.get('state') !== state) {
    throw new Error('callback state mismatch (possible CSRF); aborting login')
  }
  const code = query.get('code')
  if (!code) {
    throw new Error('callback is missing the authorization code')
  }

  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: callbackUri(),
    client_id: clientId,
    code_verifier: verifier,
  })
  const response = await fetch(tokenEndpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload.error_description ?? payload.error ?? `token endpoint returned ${response.status}`)
  }
  if (!payload.id_token) {
    throw new Error('token endpoint response is missing id_token (is `openid` in the scopes?)')
  }
  return { method, idToken: payload.id_token }
}

// The pending method's client id survives the redirect via sessionStorage so
// the callback page can complete the exchange without refetching methods.
const CLIENT_ID_KEY = 'legion-oidc-client-id'

export function rememberClientId(clientId: string) {
  sessionStorage.setItem(CLIENT_ID_KEY, clientId)
}

export function pendingClientId(): string | null {
  const clientId = sessionStorage.getItem(CLIENT_ID_KEY)
  sessionStorage.removeItem(CLIENT_ID_KEY)
  return clientId
}
