import { AppActionInfo, UiPermission } from '../gen/legion_pb'
import type { ViewDocument } from '../utils/appViews'

/*
 * An app view as the autoscaler's job panel might answer it (apps plan
 * §6.5's example, grown to every block), for the /testing page. The
 * renderer's whole block set in one document.
 */

const at = 1792412400

export const autoscalerJobView: ViewDocument = {
  view: 'legion-view/1',
  title: 'Autoscaling',
  blocks: [
    {
      type: 'stats',
      items: [
        { label: 'Now', value: '6' },
        { label: 'Bounds', value: '2–8' },
        { label: 'CPU', value: '91%', tone: 'warn', hint: 'target 70%' },
      ],
    },
    {
      type: 'series',
      title: 'Replicas vs CPU (1h)',
      series: [
        { name: 'replicas', points: Array.from({ length: 13 }, (_, index) => [at + index * 300, index < 4 ? 4 : 6] as [number, number]) },
        { name: 'cpu', axis: 'right', unit: '%', points: Array.from({ length: 13 }, (_, index) => [at + index * 300, 60 + ((index * 7) % 33)] as [number, number]) },
      ],
    },
    {
      type: 'timeline',
      items: [
        { at: at + 3600, title: '4 → 6', detail: 'cpu 0.91 vs 0.70 (4 ready)' },
        { at: at + 1200, title: 'held', detail: 'cooldown 2m left', tone: 'warn' },
      ],
    },
    {
      type: 'table',
      title: 'Policies',
      columns: [
        { key: 'task', label: 'Task' },
        { key: 'count', label: 'Count', type: 'number' },
        { key: 'cooldown', label: 'Cooldown', type: 'duration' },
        { key: 'last', label: 'Last change', type: 'time' },
      ],
      rows: [
        { task: { text: 'web', route: '/jobs/default/web/tasks/web' }, count: 6, cooldown: 120, last: at + 3600 },
        { task: 'worker', count: 2, cooldown: 300, last: at },
      ],
    },
    { type: 'keyValue', items: [{ key: 'Policy', value: 'cpu ≥ 70% → +2' }, { key: 'Plugin', value: 'builtin:cpu', tone: 'ok' }] },
    { type: 'notice', tone: 'info', text: 'Scaling is evaluated every 15 seconds.' },
    { type: 'markdown', text: '**Docs:** see [autoscaling](https://example.com/autoscaling) or the [job](/jobs/default/web).\n\n- one\n- two' },
    { type: 'actions', items: [{ action: 'pause', label: 'Pause 1h', inputs: { for: '1h' } }, { action: 'pause', label: 'Pause…' }] },
  ],
}

export const autoscalerActions: AppActionInfo[] = [
  new AppActionInfo({
    name: 'pause',
    title: 'Pause autoscaling',
    confirm: 'Pause autoscaling for web?',
    inputsJson: JSON.stringify({ for: { type: 'duration', default: '1h' } }),
    permission: new UiPermission({ resource: 'scale', action: 'write' }),
    allowed: true,
    mutating: true,
  }),
]
