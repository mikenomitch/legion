// The autoscaler's UI as app views (autoscaler-v2 plan §6.14, §12 P6;
// docs/autoscaling.md, "In the UI"): the job page draws the Autoscaling
// panel the app answers (now / want / bounds, the hour of replicas against
// the signal, the decisions, Pause), and the sidebar lists the Autoscaling
// page. The documents are shaped like `src/apps/autoscaler_ui.rs` builds
// them; the Rust tests check those against the manager's schema.
const { actionInfo, extension, msg, num, str, fakeManager } = require('./support.cjs')

const T = 1792412400
const jobPanel = {
  view: 'legion-view/1',
  title: 'Autoscaling',
  blocks: [
    {
      type: 'stats',
      items: [
        { label: 'Now', value: '6' },
        { label: 'Want', value: '6' },
        { label: 'Bounds', value: '2..8', hint: '2..8, cpu 70%, enforce' },
        { label: 'State', value: 'ok', tone: 'ok', hint: 'cpu 0.71 vs 0.70 (6 ready)' },
        { label: 'Mode', value: 'enforce' },
      ],
    },
    {
      type: 'series',
      title: 'Replicas vs cpu (1h)',
      series: [
        { name: 'replicas', points: [[T, 4], [T + 15, 4], [T + 30, 6]] },
        { name: 'cpu', axis: 'right', unit: '%', points: [[T, 91], [T + 15, 88], [T + 30, 71]] },
        { name: 'cpu target', axis: 'right', unit: '%', points: [[T, 70], [T + 30, 70]] },
      ],
    },
    { type: 'timeline', title: 'Decisions', items: [{ at: T + 30, title: '4 → 6', detail: 'cpu 0.91 vs 0.70 (4 ready) (by autoscaler)', tone: 'info' }] },
    { type: 'actions', items: [{ action: 'pause', label: 'Pause…' }] },
  ],
}

module.exports = async function autoscalerPanel({ browser, base, check }) {
  const page = await browser.newPage()
  page.on('pageerror', (error) => console.log('  page error:', error.message))
  await fakeManager(page, {
    ListUiExtensions: (request) => {
      const body = request.postDataBuffer() || Buffer.alloc(0)
      // With the job page's context: the job panel; at login: the sidebar.
      if (body.includes(Buffer.from('job'))) {
        return msg(1, extension({ app: 'autoscaler', slot: 'job', title: 'Autoscaling', view: 'job', refresh: 10 }))
      }
      return msg(1, extension({ app: 'autoscaler', slot: 'nav', title: 'Autoscaling', icon: 'gauge', view: 'policies', refresh: 10 }))
    },
    GetAppView: () =>
      Buffer.concat([
        str(1, 'autoscaler'),
        str(2, 'job'),
        str(4, JSON.stringify(jobPanel)),
        str(5, 'Autoscaling'),
        msg(
          6,
          actionInfo({
            name: 'pause',
            title: 'Pause autoscaling',
            confirm: 'Pause autoscaling for every autoscaled task of web? People can still scale it by hand.',
            inputsJson: '{"for":{"type":"duration","default":"1h","label":"For"}}',
            allowed: true,
            mutating: true,
          }),
        ),
        num(7, 10),
      ]),
    // The job page's own reads, empty.
    GetJobInfo: () => Buffer.alloc(0),
    ListRollouts: () => Buffer.alloc(0),
    GetJobSpec: () => Buffer.alloc(0),
    ListJobSpecHistory: () => Buffer.alloc(0),
    ListServices: () => Buffer.alloc(0),
  })

  await page.goto(`${base}/jobs/default/web`)
  const panel = page.locator('section', { has: page.locator('text=From the autoscaler app') }).first()
  await panel.waitFor({ timeout: 20000 })
  check((await panel.locator('text=Autoscaling').count()) > 0, 'the job page shows the Autoscaling panel')
  check((await panel.locator('text=2..8').count()) > 0, 'with the bounds')
  check((await panel.locator('text=Replicas vs cpu (1h)').count()) === 1 && (await panel.locator('svg path').count()) >= 3, 'with the hour of replicas against the signal')
  check((await panel.locator('text=4 → 6').count()) === 1, 'with the decisions')
  await panel.locator('button', { hasText: 'Pause…' }).click()
  check((await panel.locator('text=People can still scale it by hand.').count()) === 1, 'Pause asks first')
  check((await page.locator('nav >> text=Autoscaling').count()) > 0, 'the sidebar lists the Autoscaling page')
  await page.close()
}
