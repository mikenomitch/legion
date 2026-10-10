import path from 'node:path'
import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'
import { legionFixtures } from './dev/fixtures-plugin'

// The manager serves its full gRPC-Web API on http://127.0.0.1:4747. We proxy
// those calls through the Vite dev server so the browser only ever talks to the
// Vite origin — this keeps the setup origin-agnostic (works behind forwarded
// ports / tunnels) and sidesteps cross-origin concerns.
//
// To use it, run the UI with an empty manager endpoint so the client issues
// same-origin relative requests:  VITE_MANAGER_ENDPOINT= npm run dev
//
// Two design-lab modes (dev/fixtures-plugin.ts) serve the theme studio with
// live or recorded data, no cluster required:
//   npm run lab          live manager when it answers, recorded data otherwise
//   npm run lab:record   proxy to the manager and save every response
const managerTarget = process.env.LEGION_MANAGER_PROXY ?? 'http://127.0.0.1:4747'

export default defineConfig(({ mode }) => {
  const labMode = mode === 'record' ? 'record' : mode === 'lab' ? 'lab' : undefined
  const fixtureDir = path.resolve(__dirname, process.env.LEGION_UI_FIXTURES ?? 'dev/fixtures')
  const defaultPreference = process.env.LEGION_UI_DATA === 'fake' || process.env.LEGION_UI_DATA === 'live' ? process.env.LEGION_UI_DATA : 'auto'
  return {
    plugins: [
      solid(),
      ...(labMode ? [legionFixtures({ mode: labMode, dir: fixtureDir, target: managerTarget, defaultPreference })] : []),
    ],
    // Lab modes always route the UI's calls through the dev server.
    define: labMode ? { 'import.meta.env.VITE_MANAGER_ENDPOINT': JSON.stringify('') } : {},
    server: {
      host: true,
      // Allow access through forwarded ports / tunnel domains (Vite otherwise
      // rejects non-localhost Host headers with "Blocked request").
      allowedHosts: true,
      proxy: labMode
        ? undefined
        : {
            // All Manager RPCs are POSTed to /legion.Manager/<Method>.
            '/legion.Manager': {
              target: managerTarget,
              changeOrigin: true,
              secure: false,
              ws: false,
            },
          },
    },
  }
})
