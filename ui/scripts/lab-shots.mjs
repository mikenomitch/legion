// Screenshot every design-lab theme on the Workers, Job and Task pages.
//
//   VITE_MANAGER_ENDPOINT= npm run dev          (against a running manager)
//   npm run lab:shots -- --out ../.lab-shots [--themes nord,swiss] [--job checkout --task api]
//
// Needs Playwright (`npm i -g playwright`, or PLAYWRIGHT_MODULE pointing at an
// install). Shots are full-length at 1440px wide, with the viewport grown to
// the page height so the sticky sidebar runs the whole way down.
import { mkdir } from 'node:fs/promises'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { DESIGN_THEMES } from '../src/themes/catalog.ts'

const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, value, index, all) => {
    if (value.startsWith('--')) pairs.push([value.slice(2), all[index + 1]])
    return pairs
  }, []),
)
const base = args.base ?? 'http://localhost:5173'
const out = path.resolve(args.out ?? '.lab-shots')
const job = args.job ?? 'checkout'
const task = args.task ?? 'api'
const only = args.themes ? new Set(args.themes.split(',')) : undefined
const width = Number(args.width ?? 1440)
const maxHeight = Number(args['max-height'] ?? 2600)

const loadPlaywright = async () => {
  const candidates = [process.env.PLAYWRIGHT_MODULE, 'playwright', '/opt/node-tools/node_modules/playwright/index.mjs'].filter(Boolean)
  for (const candidate of candidates) {
    try {
      return await import(candidate.startsWith('/') ? pathToFileURL(candidate).href : candidate)
    } catch {
      // try the next one
    }
  }
  throw new Error('Playwright not found: npm i -g playwright, or set PLAYWRIGHT_MODULE')
}

const pages = [
  { key: 'workers', path: '/nodes', ready: 'table tbody tr' },
  { key: 'job', path: `/jobs/${encodeURIComponent(job)}`, ready: '[data-ui="page-title"]' },
  { key: 'task', path: `/jobs/${encodeURIComponent(job)}/tasks/${encodeURIComponent(task)}`, ready: '[data-ui="page-title"]' },
]

const themes = [{ id: 'current' }, ...DESIGN_THEMES].filter((theme) => !only || only.has(theme.id))

const { chromium } = await loadPlaywright()
const browser = await chromium.launch()
await mkdir(out, { recursive: true })

for (const theme of themes) {
  const context = await browser.newContext({ viewport: { width, height: 900 }, deviceScaleFactor: 1 })
  // A clean slate per theme, minus the ACL-off banner (it is dismissable and
  // would otherwise sit at the top of every shot).
  await context.addInitScript(() => {
    localStorage.setItem('legion-acl-warning-dismissed', 'true')
    localStorage.setItem('ui-sidebar-collapsed', 'false')
  })
  const page = await context.newPage()
  for (const target of pages) {
    await page.setViewportSize({ width, height: 900 })
    await page.goto(`${base}${target.path}?design=${theme.id}&shot=1`, { waitUntil: 'networkidle' })
    await page.waitForSelector(target.ready, { timeout: 15000 })
    await page.evaluate(() => document.fonts.ready)
    await page.waitForTimeout(900)
    const height = Math.min(maxHeight, await page.evaluate(() => document.documentElement.scrollHeight))
    await page.setViewportSize({ width, height })
    await page.waitForTimeout(400)
    const file = path.join(out, `${theme.id}--${target.key}.png`)
    await page.screenshot({ path: file })
    console.log(file)
  }
  await context.close()
}
await browser.close()
