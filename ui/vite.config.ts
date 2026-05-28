import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'

// The manager serves its full gRPC-Web API on http://127.0.0.1:4747. We proxy
// those calls through the Vite dev server so the browser only ever talks to the
// Vite origin — this keeps the setup origin-agnostic (works behind forwarded
// ports / tunnels) and sidesteps cross-origin concerns.
//
// To use it, run the UI with an empty manager endpoint so the client issues
// same-origin relative requests:  VITE_MANAGER_ENDPOINT= npm run dev
const managerTarget = process.env.LEGION_MANAGER_PROXY ?? 'http://127.0.0.1:4747'

export default defineConfig({
  plugins: [solid()],
  server: {
    host: true,
    // Allow access through forwarded ports / tunnel domains (Vite otherwise
    // rejects non-localhost Host headers with "Blocked request").
    allowedHosts: true,
    proxy: {
      // All Manager RPCs are POSTed to /legion.Manager/<Method>.
      '/legion.Manager': {
        target: managerTarget,
        changeOrigin: true,
        secure: false,
        ws: false,
      },
    },
  },
})
