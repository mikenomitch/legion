// Visit every UI page against `npm run lab:record` so dev/fixtures captures
// what each one asks the manager for. Re-run after changing the cluster.
//
//   npm run lab:record        (in another terminal, against a live manager)
//   npm run lab:crawl -- [--base http://localhost:5173] [--job checkout] [--task api]
import { pathToFileURL } from 'node:url'

const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, value, index, all) => {
    if (value.startsWith('--')) pairs.push([value.slice(2), all[index + 1]])
    return pairs
  }, []),
)
const base = args.base ?? 'http://localhost:5173'

const loadPlaywright = async () => {
  for (const candidate of [process.env.PLAYWRIGHT_MODULE, 'playwright', '/opt/node-tools/node_modules/playwright/index.mjs'].filter(Boolean)) {
    try {
      return await import(candidate.startsWith('/') ? pathToFileURL(candidate).href : candidate)
    } catch {
      // next
    }
  }
  throw new Error('Playwright not found: npm i -g playwright, or set PLAYWRIGHT_MODULE')
}

const { chromium } = await loadPlaywright()
const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })

const visit = async (path) => {
  await page.goto(`${base}${path}`, { waitUntil: 'networkidle' }).catch(() => {})
  await page.waitForTimeout(1200)
  console.log('visited', path)
}

// Index pages first; their links tell us which detail pages exist.
const indexes = ['/', '/jobs', '/templates', '/services', '/network', '/rollouts', '/volumes', '/nodes', '/manager', '/raft', '/events', '/kv', '/access']
const details = new Set()
for (const path of indexes) {
  await visit(path)
  const hrefs = await page.$$eval('main a[href^="/"]', (links) => links.map((link) => link.getAttribute('href')))
  for (const href of hrefs) {
    if (href && /^\/(jobs|nodes|services|rollouts|templates|volumes|manager)\/[^/]+(\/tasks\/[^/]+)?$/.test(href)) details.add(href)
  }
}
for (const path of details) {
  await visit(path)
  // Task links live on job pages.
  if (path.startsWith('/jobs/') && !path.includes('/tasks/')) {
    const tasks = await page.$$eval('main a[href*="/tasks/"]', (links) => links.map((link) => link.getAttribute('href')))
    for (const task of new Set(tasks)) if (task) await visit(task)
  }
}
await browser.close()
