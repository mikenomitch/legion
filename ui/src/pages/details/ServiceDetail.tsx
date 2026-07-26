import { A, useParams } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, onCleanup, onMount } from 'solid-js'
import { ServiceEndpointsTable, ServiceTags } from '../../components/ServiceEndpoints'
import {
  Badge,
  Button,
  CodeBlock,
  Icon,
  LoadingRows,
  Note,
  PageHeader,
  SectionCard,
  StatusBadge,
  cx,
  linkClass,
} from '../../components/ui'
import { managerClient } from '../../grpcClient'
import { POLL_INTERVAL_MS } from '../../utils/polling'
import { diagnoseService, type DiagnosisLevel } from '../../utils/services'

const stepIcon: Record<DiagnosisLevel, 'check' | 'x' | 'alertTriangle'> = {
  ok: 'check',
  fail: 'x',
  warn: 'alertTriangle',
}

const stepClass: Record<DiagnosisLevel, string> = {
  ok: 'text-emerald-600 dark:text-emerald-400',
  fail: 'text-red-600 dark:text-red-400',
  warn: 'text-amber-600 dark:text-amber-400',
}

export default function ServiceDetail() {
  const params = useParams()
  const serviceName = createMemo(() => decodeURIComponent(params.name))
  const [onlyHealthy, setOnlyHealthy] = createSignal(false)
  const [tick, setTick] = createSignal(0)

  onMount(() => {
    const timer = setInterval(() => setTick((value) => value + 1), POLL_INTERVAL_MS)
    onCleanup(() => clearInterval(timer))
  })

  // Always resolve with unhealthy included: this page exists to explain why an
  // endpoint is missing, and filtering happens client-side.
  const [data] = createResource(
    () => [serviceName(), tick()] as const,
    async ([name]) => {
      const [list, resolved] = await Promise.all([
        managerClient.listServices({}),
        managerClient.resolveService({ service: name, includeUnhealthy: true }),
      ])
      return {
        summary: list.services.find((entry) => entry.service === name),
        endpoints: resolved.endpoints,
      }
    },
  )

  const endpoints = () => data()?.endpoints ?? []
  const healthyEndpoints = () => endpoints().filter((endpoint) => endpoint.healthy)
  const visibleEndpoints = () => (onlyHealthy() ? healthyEndpoints() : endpoints())
  const diagnosis = createMemo(() => diagnoseService(serviceName(), data()?.summary, endpoints()))
  const worstLevel = createMemo<DiagnosisLevel>(() => {
    const steps = diagnosis()
    if (steps.some((step) => step.level === 'fail')) return 'fail'
    if (steps.some((step) => step.level === 'warn')) return 'warn'
    return 'ok'
  })

  return (
    <Show when={!data.loading || data()} fallback={<LoadingRows rows={5} class="p-0" />}>
      <Show when={!data.error} fallback={<Note tone="danger">Failed to load service.</Note>}>
        <div>
          <PageHeader
            eyebrow="Service"
            title={serviceName()}
            badges={
              <>
                <StatusBadge
                  status={
                    endpoints().length === 0
                      ? 'no endpoints'
                      : healthyEndpoints().length === 0
                        ? 'unavailable'
                        : healthyEndpoints().length === endpoints().length
                          ? 'healthy'
                          : 'degraded'
                  }
                />
                <Badge tone="neutral">
                  {healthyEndpoints().length} / {endpoints().length} endpoints healthy
                </Badge>
              </>
            }
            subtitle={
              <Show when={data()?.summary} fallback="Not declared by any job.">
                <>
                  Declared by task{' '}
                  <A
                    class={linkClass}
                    href={`/jobs/${encodeURIComponent(data()!.summary!.jobId)}/tasks/${encodeURIComponent(data()!.summary!.taskName)}`}
                  >
                    {data()!.summary!.taskName}
                  </A>{' '}
                  in job{' '}
                  <A class={linkClass} href={`/jobs/${encodeURIComponent(data()!.summary!.jobId)}`}>
                    {data()!.summary!.jobId}
                  </A>
                </>
              </Show>
            }
          />

          <div class="space-y-4">
            <Show when={(data()?.summary?.tags.length ?? 0) > 0 || Object.keys(data()?.summary?.meta ?? {}).length > 0}>
              <div class="flex flex-wrap items-center gap-2">
                <ServiceTags tags={data()?.summary?.tags ?? []} meta={data()?.summary?.meta} />
              </div>
            </Show>

            <SectionCard
              title="Endpoints"
              description="Where this service is reachable right now."
              flush
              actions={
                <Button size="xs" onClick={() => setOnlyHealthy((value) => !value)}>
                  {onlyHealthy() ? 'Show all' : 'Only healthy'}
                </Button>
              }
            >
              <ServiceEndpointsTable
                endpoints={visibleEndpoints()}
                emptyTitle={onlyHealthy() ? 'No healthy endpoints' : 'No endpoints'}
                emptyBody={
                  onlyHealthy()
                    ? 'Every endpoint is failing its checks or sitting on a dead node. See the diagnosis below.'
                    : 'Nothing is placed for this service yet. See the diagnosis below.'
                }
              />
            </SectionCard>

            {/* The same layered walk as `legion service doctor`: the useful
                answer to "why can't I reach this" is which layer broke. */}
            <SectionCard
              title="Diagnosis"
              description="Discovery layers, in order. The first failure is the one to fix."
              actions={
                <Badge tone={worstLevel() === 'ok' ? 'ok' : worstLevel() === 'warn' ? 'warn' : 'danger'} dot>
                  {worstLevel() === 'ok' ? 'resolving' : worstLevel() === 'warn' ? 'degraded' : 'not resolving'}
                </Badge>
              }
            >
              <ol class="space-y-2.5">
                <For each={diagnosis()}>
                  {(step) => (
                    <li class="flex items-start gap-2.5">
                      <Icon name={stepIcon[step.level]} size={15} class={cx('mt-0.5 shrink-0', stepClass[step.level])} />
                      <div class="min-w-0">
                        <p class={cx('text-sm', step.level === 'ok' ? 'text-ink' : 'font-medium text-ink')}>{step.title}</p>
                        <Show when={step.detail}>
                          <p class="mt-0.5 break-words font-mono text-xs text-muted">{step.detail}</p>
                        </Show>
                        <Show when={step.hint}>
                          <p class="mt-0.5 text-xs text-faint">{step.hint}</p>
                        </Show>
                      </div>
                    </li>
                  )}
                </For>
              </ol>
            </SectionCard>

            <SectionCard title="Resolve from a client" description="The same catalog, for scripts and other workloads.">
              <CodeBlock
                code={[
                  `legion service resolve ${serviceName()}`,
                  `legion service resolve ${serviceName()} --all    # include unhealthy`,
                  `legion service doctor ${serviceName()}`,
                ].join('\n')}
                maxHClass="max-h-32"
              />
              <p class="mt-2 text-xs text-faint">
                Inside a task, the assigned address is injected as{' '}
                <code class="font-mono text-muted">LEGION_ADDR_&lt;PORT_LABEL&gt;</code>.
              </p>
            </SectionCard>
          </div>
        </div>
      </Show>
    </Show>
  )
}
