import { createPromiseClient, type Interceptor } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'
import { Manager } from './gen/legion_connect'

export const managerEndpoint =
  import.meta.env.VITE_MANAGER_ENDPOINT ?? 'http://127.0.0.1:4747'

const TOKEN_STORAGE_KEY = 'legion-acl-token'

export function getStoredToken(): string | null {
  return localStorage.getItem(TOKEN_STORAGE_KEY)
}

export function setStoredToken(token: string | null) {
  if (token && token.trim().length > 0) {
    localStorage.setItem(TOKEN_STORAGE_KEY, token.trim())
  } else {
    localStorage.removeItem(TOKEN_STORAGE_KEY)
  }
}

// Attach the stored ACL token (if any) to every request. With ACLs disabled
// on the manager this header is simply ignored.
const authInterceptor: Interceptor = (next) => async (req) => {
  const token = getStoredToken()
  if (token) {
    req.header.set('Authorization', `Bearer ${token}`)
  }
  return next(req)
}

const transport = createGrpcWebTransport({
  baseUrl: managerEndpoint,
  interceptors: [authInterceptor],
})

export const managerClient = createPromiseClient(Manager, transport)
