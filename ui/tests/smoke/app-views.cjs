// App views, tier 1 (apps plan §6.5, §9 A4; docs/apps.md, "Views and
// actions"): the sidebar lists an app's nav page, the page draws its view's
// blocks, the /testing fixture draws every block, a mutating action asks
// first and sends its inputs, and nothing an app sends becomes markup.
const { extension, msg, str, fakeManager } = require('./support.cjs')

const hostile = '<img src=x onerror="window.__pwned=1">'
const overviewView = {
  view: 'legion-view/1',
  title: 'Fake overview',
  blocks: [
    { type: 'stats', items: [{ label: 'Policies', value: '3' }, { label: 'Paused', value: '1', tone: 'warn' }] },
    { type: 'table', columns: [{ key: 'job', label: 'Job' }, { key: 'note', label: 'Note' }], rows: [{ job: { text: 'web', route: '/jobs/default/web' }, note: hostile }] },
    { type: 'markdown', text: 'Read **the docs** at [example](https://example.com/docs).' },
    { type: 'notice', tone: 'info', text: 'All quiet.' },
  ],
}

module.exports = async function appViews({ browser, base, check }) {
  const page = await browser.newPage()
  page.on('pageerror', (error) => console.log('  page error:', error.message))
  const calls = await fakeManager(page, {
    ListUiExtensions: () => msg(1, extension({ app: 'fake', slot: 'nav', title: 'Fake app', icon: 'gauge', view: 'overview', description: 'A fake app.' })),
    GetAppView: () => Buffer.concat([str(1, 'fake'), str(2, 'overview'), str(4, JSON.stringify(overviewView)), str(5, 'Fake overview')]),
    InvokeAppAction: () => Buffer.concat([str(1, 'paused web for 2h'), str(4, '{"for":"2h"}')]),
  })

  // The sidebar entry and the app page.
  await page.goto(`${base}/apps/fake/overview`)
  await page.waitForSelector('text=All quiet.', { timeout: 20000 })
  check((await page.locator('nav >> text=Fake app').count()) > 0, 'the sidebar lists the app page under Apps')
  check((await page.locator('text=Fake overview').count()) > 0, 'the page is titled by the view')
  check((await page.locator('text=Policies').count()) > 0, 'stats render')
  const link = page.locator('a', { hasText: 'web' }).first()
  check((await link.getAttribute('href')) === '/jobs/default/web', 'a table cell links to its in-app route')
  const external = page.locator('a', { hasText: 'example' }).first()
  check(
    (await external.getAttribute('target')) === '_blank' && (await external.getAttribute('href')) === 'https://example.com/docs',
    'markdown links open http(s) in a new tab',
  )
  check((await page.locator('strong', { hasText: 'the docs' }).count()) === 1, 'markdown emphasis renders')
  check((await page.locator(`text=${hostile}`).count()) === 1, 'a hostile string renders as text')
  check(
    (await page.evaluate(() => window.__pwned === undefined)) && (await page.locator('img[src="x"]').count()) === 0,
    'no markup from the app',
  )

  // The fixture page and an action.
  await page.goto(`${base}/testing`)
  await page.waitForSelector('text=App view: every block', { timeout: 20000 })
  const section = page.locator('section', { has: page.locator('text=App view: every block') }).first()
  for (const text of ['Replicas vs CPU (1h)', 'Policies', '4 → 6', 'Scaling is evaluated every 15 seconds.', 'builtin:cpu']) {
    check((await section.locator(`text=${text}`).count()) > 0, `the fixture shows "${text}"`)
  }
  check((await section.locator('svg path').count()) >= 2, 'the series draws its lines')
  await section.locator('button', { hasText: 'Pause…' }).click()
  check((await section.locator('text=Pause autoscaling for web?').count()) === 1, 'a mutating action asks first, with the manager-worded confirmation')
  const input = section.locator('[role=dialog] input').first()
  check((await input.inputValue()) === '1h', 'the input starts at its declared default')
  await input.fill('2h')
  await section.locator('[role=dialog] button', { hasText: 'Pause…' }).click()
  await page.waitForSelector('text=paused web for 2h', { timeout: 10000 })
  check(true, 'the app message shows after the action')
  const invoked = calls.filter((call) => call.method === 'InvokeAppAction')
  const sent = invoked[0] ? invoked[0].body.toString('latin1') : ''
  check(sent.includes('"for":"2h"') && sent.includes('pause'), 'InvokeAppAction carried the action and its inputs')
  check(/[0-9a-f]{8}-[0-9a-f]{4}-/.test(sent), 'the press carried an idempotency key')
  check(calls.every((call) => call.method !== 'GetAppView' || call.headers['legion-app-page'] === undefined), 'tier-1 calls carry no page header')
  await page.close()
}
