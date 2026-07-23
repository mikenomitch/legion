import { useNavigate } from '@solidjs/router'
import { Show, createResource } from 'solid-js'
import { managerClient, setStoredToken } from '../grpcClient'
import { completeOidcLogin, pendingClientId } from '../utils/oidc'
import { Spinner } from '../components/ui'

// Lands here after the identity provider redirect: completes the PKCE code
// exchange, trades the ID token for a Legion ACL token, and returns to the
// Access page.
export default function OidcCallback() {
  const navigate = useNavigate()
  const [result] = createResource(async () => {
    const clientId = pendingClientId()
    if (!clientId) {
      throw new Error('no OIDC login in progress (start again from the Access page)')
    }
    const query = new URLSearchParams(window.location.search)
    const { method, idToken } = await completeOidcLogin(query, clientId)
    const reply = await managerClient.login({ method, jwt: idToken })
    setStoredToken(reply.secret)
    navigate('/access', { replace: true })
    return reply.token?.name ?? method
  })

  return (
    <div class="rounded-lg border border-line bg-surface p-4 text-sm shadow-card">
      <Show when={!result.error} fallback={
        <div>
          <p class="font-semibold text-red-600 dark:text-red-400">Single sign-on failed</p>
          <p class="mt-1 text-muted">{String(result.error)}</p>
          <a class="mt-3 inline-block font-medium text-brass-strong underline-offset-2 hover:underline" href="/access">
            Back to Access
          </a>
        </div>
      }>
        <Spinner label="Completing single sign-on…" />
      </Show>
    </div>
  )
}
