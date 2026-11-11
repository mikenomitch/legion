// UI smoke tests (docs/testing-and-benchmarks.md, "UI smoke tests").
//
//   npm run build && npm run test:smoke
//
// Serves the built SPA (`dist/`) with `vite preview`, opens it in Playwright's
// Chromium, and fakes the manager's gRPC-Web answers inside the browser, so
// no Legion process is needed. Each suite is a module exporting
// `async ({ browser, base, check }) => {}`. Pass suite names to run some:
// `npm run test:smoke -- app-pages`.
let chromium
try {
  ;({ chromium } = require('playwright'))
} catch {
  console.error(
    'The smoke tests need the `playwright` package and its Chromium, which the UI does not depend on: ' +
      '`npm install -g playwright && npx playwright install chromium`.',
  )
  process.exit(2)
}
const { spawn } = require('child_process')
const fs = require('fs')
const path = require('path')

const SUITES = ['app-views', 'app-pages', 'autoscaler-panel']
const uiDir = path.resolve(__dirname, '..', '..')
const PORT = Number(process.env.SMOKE_PORT || 4179)
const base = `http://127.0.0.1:${PORT}`

async function main() {
  if (!fs.existsSync(path.join(uiDir, 'dist', 'index.html'))) {
    console.error('No built UI in ui/dist: run `npm run build` first.')
    process.exit(2)
  }
  const chosen = process.argv.slice(2).filter((arg) => !arg.startsWith('-'))
  const suites = chosen.length > 0 ? chosen : SUITES
  for (const suite of suites) {
    if (!SUITES.includes(suite)) {
      console.error(`Unknown suite ${suite}; the suites are ${SUITES.join(', ')}.`)
      process.exit(2)
    }
  }

  const preview = spawn(process.execPath, [path.join(uiDir, 'node_modules', 'vite', 'bin', 'vite.js'), 'preview', '--port', String(PORT), '--strictPort', '--host', '127.0.0.1'], {
    cwd: uiDir,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('vite preview did not start within 30s')), 30000)
    preview.stdout.on('data', (chunk) => {
      if (String(chunk).includes(String(PORT))) {
        clearTimeout(timer)
        resolve()
      }
    })
    preview.on('exit', (code) => reject(new Error(`vite preview exited (${code})`)))
  })

  const browser = await chromium.launch()
  const failures = []
  let passed = 0
  try {
    for (const suite of suites) {
      console.log(`${suite}:`)
      const check = (ok, what) => {
        console.log(`  ${ok ? 'ok  ' : 'FAIL'} ${what}`)
        if (ok) passed += 1
        else failures.push(`${suite}: ${what}`)
      }
      try {
        await require(`./${suite}.cjs`)({ browser, base, check })
      } catch (error) {
        console.log(`  FAIL ${suite} threw: ${error && error.stack ? error.stack : error}`)
        failures.push(`${suite}: threw ${error && error.message ? error.message : error}`)
      }
    }
  } finally {
    await browser.close()
    preview.kill()
  }
  if (failures.length > 0) {
    console.log(`\n${failures.length} check(s) failed, ${passed} passed:`)
    for (const failure of failures) console.log(`  ${failure}`)
    process.exit(1)
  }
  console.log(`\nall ${passed} checks passed`)
  process.exit(0)
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
