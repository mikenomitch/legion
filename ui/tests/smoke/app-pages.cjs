// Sandboxed app pages, tier 2 (apps plan §6.5, §9 A5, §12 A5; docs/apps.md,
// "Sandboxed pages"). A fake app's page is mounted from a faked GetAppPage
// reply, and driven from inside its frame:
//
// - the frame is `sandbox="allow-scripts"` and nothing else, with the CSP
//   prepended;
// - an allowlisted read runs as the viewer (the viewer's token, the
//   `legion-app-page` header) and its reply reaches the page;
// - the bridge refuses an RPC outside the allowlist, and the manager never
//   sees it;
// - a mutating call shows the SPA's confirmation, naming the app, the RPC,
//   and the target; cancelled, nothing is sent; confirmed, it runs;
// - the page cannot read localStorage (its own or the SPA's token), cannot
//   reach the SPA's DOM, cannot open a network connection, and cannot submit
//   a form;
// - messages from anything but the page's frame are ignored, and the bridge
//   closes once the frame navigates away.
const { appPage, extension, msg, fakeManager, MANAGER } = require('./support.cjs')

const TOKEN = 'viewer-secret-token'

// The page: buttons the test presses from inside the frame, and a form.
const PAGE_HTML = `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Console</title>
<style>body { font-family: sans-serif; }</style></head>
<body>
<h1 id="heading">Fake console</h1>
<p id="theme"></p>
<form id="steal" action="${MANAGER}/steal" method="post"><input name="x" value="1"></form>
<script>
  document.getElementById('theme').textContent = 'theme ' + legion.theme;
  window.outcome = function (promise) {
    return promise.then(function (result) { return 'ok ' + JSON.stringify(result); },
                        function (error) { return 'error ' + error.code + ': ' + error.message; });
  };
</script>
</body>
</html>`

module.exports = async function appPages({ browser, base, check }) {
  const page = await browser.newPage()
  page.on('pageerror', (error) => console.log('  page error:', error.message))
  // The viewer is logged in. (Init scripts run in every frame; the page's
  // sandboxed frame has no storage, which is the point.)
  await page.addInitScript((token) => {
    if (window === window.top) localStorage.setItem('legion-acl-token', token)
  }, TOKEN)
  const calls = await fakeManager(page, {
    ListUiExtensions: () =>
      msg(1, extension({ app: 'fake', slot: 'nav', title: 'Fake console', icon: 'terminal', page: 'console', description: 'A fake app.' })),
    GetAppPage: () =>
      appPage({
        app: 'fake',
        page: 'console',
        title: 'Fake console',
        html: PAGE_HTML,
        rpcs: [
          { method: 'GetJobInfo', mutating: false },
          { method: 'StopJob', mutating: true },
        ],
      }),
    // Empty replies: every field at its default.
    GetJobInfo: () => Buffer.alloc(0),
    StopJob: () => Buffer.alloc(0),
  })
  const seen = (method) => calls.filter((call) => call.method === method)

  await page.goto(`${base}/apps/fake/pages/console`)
  const iframe = page.locator('iframe[srcdoc]')
  await iframe.waitFor({ timeout: 20000 })
  check((await page.locator('nav >> text=Fake console').count()) > 0, 'the sidebar lists the sandboxed page under Apps')
  check((await iframe.getAttribute('sandbox')) === 'allow-scripts', 'the frame is sandboxed with allow-scripts and nothing else')
  const srcdoc = (await iframe.getAttribute('srcdoc')) || ''
  check(
    srcdoc.startsWith('<!doctype html><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'unsafe-inline\'; style-src \'unsafe-inline\'; img-src data:; connect-src \'none\'; form-action \'none\'; base-uri \'none\'">'),
    'the CSP is prepended, right after the doctype',
  )
  const frameHandle = await iframe.elementHandle()
  const frame = await frameHandle.contentFrame()
  await frame.waitForSelector('#heading', { timeout: 10000 })
  check((await frame.locator('#heading').textContent()) === 'Fake console', 'the page renders inside the frame')
  check(/theme (light|dark)/.test((await frame.locator('#theme').textContent()) || ''), 'the page knows the theme')

  // An allowlisted read, as the viewer.
  const read = await frame.evaluate(() => window.outcome(legion.call('GetJobInfo', { name: 'web' })))
  check(read === 'ok {}', `an allowlisted read returns its reply to the page (${read})`)
  const info = seen('GetJobInfo')[0]
  check(info && info.headers.authorization === `Bearer ${TOKEN}`, 'the call carries the viewer\'s token, which the page never sees')
  check(info && info.headers['legion-app-page'] === 'fake/console', 'the call is stamped legion-app-page for the manager\'s re-check')

  // Outside the allowlist: refused here, never sent.
  const refused = await frame.evaluate(() => window.outcome(legion.call('DestroyJob', { name: 'web' })))
  check(refused.startsWith('error not_allowed'), `the bridge refuses an RPC outside the allowlist (${refused})`)
  check(seen('DestroyJob').length === 0, 'the refused RPC never reaches the manager')
  const reallyRefused = await frame.evaluate(() => window.outcome(legion.call('ListJobs', {})))
  check(reallyRefused.startsWith('error not_allowed') && seen('ListJobs').length === 0, 'a read outside the allowlist is refused too')

  // A mutating call: the SPA asks, naming the app, the RPC, and the target.
  await frame.evaluate(() => {
    window.stopping = window.outcome(legion.call('StopJob', { name: 'web' }))
  })
  const dialog = page.locator('[role=alertdialog]')
  await dialog.waitFor({ timeout: 10000 })
  const title = (await page.locator('[data-testid=bridge-confirm-title]').textContent()) || ''
  check(title === 'fake wants to run StopJob on web as you', `a mutating call shows the confirmation (${title})`)
  check(seen('StopJob').length === 0, 'nothing is sent before the viewer answers')
  await dialog.locator('button', { hasText: 'Cancel' }).click()
  const cancelled = await frame.evaluate(() => window.stopping)
  check(cancelled.startsWith('error cancelled'), `cancelling refuses the call (${cancelled})`)
  check(seen('StopJob').length === 0, 'a cancelled call is never sent')
  await frame.evaluate(() => {
    window.stopping = window.outcome(legion.call('StopJob', { name: 'web' }))
  })
  await dialog.waitFor({ timeout: 10000 })
  check(await dialog.locator('[data-testid=bridge-confirm-run]').isDisabled(), '"Run as me" is not armed the moment the dialog appears')
  await dialog.locator('button', { hasText: 'Run as me' }).click()
  const stopped = await frame.evaluate(() => window.stopping)
  check(stopped === 'ok {}', `a confirmed call runs (${stopped})`)
  const stop = seen('StopJob')[0]
  check(stop && stop.headers['legion-app-page'] === 'fake/console' && stop.body.toString('latin1').includes('web'), 'the confirmed call carries its request and the page header')

  // A request that spells a field both ways could be confirmed for one
  // value and sent with the other: refused, never confirmed or sent.
  const before = seen('StopJob').length
  const aliased = await frame.evaluate(() =>
    window.outcome(legion.call('StopJob', { name: 'web', idempotencyKey: 'shown', idempotency_key: 'sent' })),
  )
  check(aliased.startsWith('error invalid_argument'), `a request with both spellings of a field is refused (${aliased})`)
  check(seen('StopJob').length === before, 'the aliased request is never sent')

  // The sandbox: no storage, no SPA DOM, no network, no forms.
  const storage = await frame.evaluate(() => {
    try {
      return 'read ' + window.localStorage.getItem('legion-acl-token')
    } catch (error) {
      return error.name
    }
  })
  check(storage === 'SecurityError', `the page cannot read localStorage (${storage})`)
  const parentStorage = await frame.evaluate(() => {
    try {
      return 'read ' + window.parent.localStorage.getItem('legion-acl-token')
    } catch (error) {
      return error.name
    }
  })
  check(parentStorage === 'SecurityError', `the page cannot read the SPA's token (${parentStorage})`)
  const parentDom = await frame.evaluate(() => {
    try {
      return 'read ' + window.parent.document.title
    } catch (error) {
      return error.name
    }
  })
  check(parentDom === 'SecurityError', `the page cannot reach the SPA's DOM (${parentDom})`)
  const before = calls.length
  const fetched = await frame.evaluate(async (manager) => {
    try {
      await fetch(manager + '/legion.Manager/ListJobs', { method: 'POST' })
      return 'fetched'
    } catch (error) {
      return error.name
    }
  }, MANAGER)
  check(fetched === 'TypeError' && calls.length === before, `the page cannot open a connection (${fetched})`)
  await frame.evaluate(() => {
    try {
      document.getElementById('steal').submit()
    } catch (error) {
      window.submitError = error.name
    }
  })
  await page.waitForTimeout(500)
  check(seen('steal').length === 0 && (await frame.locator('#heading').count()) === 1, 'the page cannot submit a form')

  // Only the page's own frame is heard.
  const infoCalls = seen('GetJobInfo').length
  await page.evaluate(() => window.postMessage({ legion: 'legion-bridge/1', id: 99, op: 'call', method: 'GetJobInfo', request: { name: 'web' } }, '*'))
  await page.waitForTimeout(300)
  check(seen('GetJobInfo').length === infoCalls, 'a message from another window is ignored')

  // legion.navigate moves the SPA, in-app routes only.
  const bad = await frame.evaluate(() => window.outcome(legion.navigate('https://example.com/')))
  check(bad.startsWith('error invalid_argument'), `navigate refuses an external URL (${bad})`)
  // Not awaited inside the frame: the navigation unmounts it.
  await frame.evaluate(() => {
    legion.navigate('/jobs/default/web')
  })
  await page.waitForURL('**/jobs/default/web', { timeout: 10000 })
  check(true, 'navigate moves the SPA to an in-app route')

  // Once the frame leaves its bundle, the bridge closes.
  await page.goto(`${base}/apps/fake/pages/console`)
  await page.locator('iframe[srcdoc]').waitFor({ timeout: 20000 })
  const fresh = await (await page.locator('iframe[srcdoc]').elementHandle()).contentFrame()
  await fresh.waitForSelector('#heading', { timeout: 10000 })
  await fresh
    .evaluate(() => {
      window.location.href = 'about:blank'
    })
    .catch(() => {})
  await page.waitForSelector('text=its bridge to Legion is closed', { timeout: 10000 })
  check(true, 'the bridge closes once the page navigates away')
  await page.close()
}
