import { AllocationUsage, GetNodeUsageReply, MemoryPressureStatus, NodeAllocationUsage, NodeResourceUsage, SchedulerEvent, SnapshotInfo } from '../gen/legion_pb'

/*
 * Fixture replies for the usage, pressure, and snapshot views, rendered at
 * /testing. A macOS dev worker has no cgroups, so it only ever produces the
 * "not reported" shape. These cover the rest: an older worker, a node with
 * enforcement on and nothing running, and a packed, oversubscribed node
 * under pressure. Numbers follow the examples in
 * docs/resource-enforcement.md so the fixture and the docs read alike.
 */

const MIB = 1024n * 1024n
const GIB = 1024n * MIB
const nowSeconds = () => BigInt(Math.floor(Date.now() / 1000))

const usage = (allocationId: bigint, usedMiB: number, extra: Partial<AllocationUsage> = {}) =>
  new AllocationUsage({
    allocationId,
    memoryCurrentBytes: BigInt(usedMiB) * MIB,
    memoryPeakBytes: BigInt(Math.round(usedMiB * 1.04)) * MIB,
    memoryAnonBytes: BigInt(Math.round(usedMiB * 0.8)) * MIB,
    cpuMillicores: 250n,
    lastActivityUnix: nowSeconds(),
    ...extra,
  })

const response = (underPressure: boolean, steps = ['reclaim', 'suspend', 'evict-bursting', 'evict-reserved']) =>
  new MemoryPressureStatus({
    enabled: true,
    underPressure,
    freeBelowBytes: 2n * GIB,
    stallAbovePercent: 10,
    settleSeconds: 5n,
    steps,
  })

/** A macOS worker, or one with enforcement off: capacity and reservations only. */
export const unreportedNode = new GetNodeUsageReply({
  nodeId: 'mac-dev',
  status: 'ready',
  usageReported: false,
  memory: new NodeResourceUsage({ capacity: 16384, reserved: 768, oversubscribeRatio: 1 }),
  cpu: new NodeResourceUsage({ capacity: 24000, reserved: 600, oversubscribeRatio: 1 }),
  allocations: [
    new NodeAllocationUsage({ allocationId: 7n, jobId: 'web', reservedMemoryMib: 256, reservedCpuMhz: 100, band: 'normal' }),
    new NodeAllocationUsage({ allocationId: 8n, jobId: 'workspaces', instanceId: 'user-42', reservedMemoryMib: 512, reservedCpuMhz: 500, memoryMaxMib: 2048, band: 'normal' }),
  ],
})

/** Enforcement on, response on, nothing placed yet. */
export const emptyNode = new GetNodeUsageReply({
  nodeId: 'w-01',
  status: 'ready',
  usageReported: true,
  sampledAtUnixMs: BigInt(Date.now() - 4000),
  reportAgeSeconds: 4n,
  memory: new NodeResourceUsage({
    capacity: 63488,
    reserved: 0,
    used: 0,
    physicalBytes: 64n * GIB,
    availableBytes: 61n * GIB,
    pressureSomeAvg10: 0,
    pressureAvailable: true,
    oversubscribeRatio: 1,
    pressureResponse: response(false, ['reclaim', 'suspend', 'evict-bursting']),
  }),
  cpu: new NodeResourceUsage({ capacity: 32000, reserved: 0, used: 0, oversubscribeRatio: 1 }),
  allocations: [],
})

/** A worker that reports usage but predates the pressure response. */
export const olderWorkerNode = new GetNodeUsageReply({
  nodeId: 'w-02',
  status: 'ready',
  usageReported: true,
  sampledAtUnixMs: BigInt(Date.now() - 7000),
  reportAgeSeconds: 7n,
  memory: new NodeResourceUsage({
    capacity: 31744,
    reserved: 4096,
    used: 2210,
    physicalBytes: 32n * GIB,
    availableBytes: 28n * GIB,
    oversubscribeRatio: 1,
  }),
  cpu: new NodeResourceUsage({ capacity: 16000, reserved: 2000, used: 640, oversubscribeRatio: 1 }),
  allocations: [
    new NodeAllocationUsage({
      allocationId: 311n,
      jobId: 'api',
      reservedMemoryMib: 4096,
      reservedCpuMhz: 2000,
      band: 'high',
      usage: usage(311n, 2210),
    }),
  ],
})

/** The docs' w-03: oversubscribed ×1.5, packed, and acting right now. */
export const pressuredNode = new GetNodeUsageReply({
  nodeId: 'w-03',
  status: 'ready',
  usageReported: true,
  sampledAtUnixMs: BigInt(Date.now() - 4000),
  reportAgeSeconds: 4n,
  memory: new NodeResourceUsage({
    capacity: 98304 * 1.5,
    reserved: 134144,
    used: 95300,
    physicalBytes: 96n * GIB,
    availableBytes: 1126n * MIB,
    pressureSomeAvg10: 12.4,
    pressureAvailable: true,
    oversubscribeRatio: 1.5,
    pressureResponse: response(true),
  }),
  cpu: new NodeResourceUsage({ capacity: 64000, reserved: 41200, used: 18900, oversubscribeRatio: 1 }),
  allocations: [
    new NodeAllocationUsage({
      allocationId: 1042n,
      jobId: 'workspace',
      instanceId: 'user-42',
      reservedMemoryMib: 512,
      reservedCpuMhz: 1000,
      memoryMaxMib: 4096,
      band: 'normal',
      usage: usage(1042n, 1830),
      pressureRank: 2,
      pressureSteps: ['reclaim', 'evict-bursting'],
    }),
    new NodeAllocationUsage({
      allocationId: 1063n,
      jobId: 'batch',
      reservedMemoryMib: 400,
      reservedCpuMhz: 500,
      memoryMaxMib: 2048,
      band: 'low',
      usage: usage(1063n, 1900, { cpuMillicores: 900n }),
      pressureRank: 1,
      pressureSteps: ['reclaim', 'evict-bursting'],
    }),
    new NodeAllocationUsage({
      allocationId: 1077n,
      jobId: 'api',
      reservedMemoryMib: 2048,
      reservedCpuMhz: 2000,
      band: 'high',
      usage: usage(1077n, 1900, { cpuMillicores: 400n, lastActivityUnix: nowSeconds() - 161n }),
      idleSeconds: 161n,
      pressureRank: 3,
      pressureSteps: ['evict-reserved'],
    }),
    new NodeAllocationUsage({
      allocationId: 1090n,
      jobId: 'postgres',
      instanceId: 'pg-0',
      reservedMemoryMib: 8192,
      reservedCpuMhz: 4000,
      band: 'critical',
      usage: usage(1090n, 6100),
    }),
  ],
})

const event = (id: bigint, kind: string, level: string, message: string, fields: Partial<SchedulerEvent>, meta: Record<string, unknown>) =>
  new SchedulerEvent({
    id,
    kind,
    level,
    message,
    // Higher ids are newer, as in the real journal.
    createdAt: new Date(Date.now() - (10 - Number(id)) * 60_000).toISOString(),
    metadataJson: JSON.stringify(meta),
    ...fields,
  })

export const pressureEvents: SchedulerEvent[] = [
  event(
    6n,
    'node.pressure',
    'warn',
    'Allocation 1063: evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation while free memory was 1.1 GiB (threshold 2 GiB)',
    { jobId: 'batch', nodeId: 'w-03', allocationId: 1063n },
    {
      step: 'evict-bursting',
      freed_bytes: 1900 * 1024 * 1024,
      used_bytes: 1900 * 1024 * 1024,
      reserved_bytes: 400 * 1024 * 1024,
      detail: 'evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation while free memory was 1.1 GiB (threshold 2 GiB)',
      cause: 'node_pressure',
      hint: 'raise resources.memory toward real usage, or move this job to a pool without oversubscription',
    },
  ),
  event(
    5n,
    'node.pressure',
    'info',
    'Allocation 1063: reclaimed 310 MiB above the 400 MiB reservation on w-03',
    { jobId: 'batch', nodeId: 'w-03', allocationId: 1063n },
    { step: 'reclaim', freed_bytes: 310 * 1024 * 1024, detail: 'reclaimed 310 MiB above the 400 MiB reservation on w-03' },
  ),
]

const snapshot = (fields: Partial<SnapshotInfo>) =>
  new SnapshotInfo({ jobId: 'workspaces', task: 'workspace', purpose: 'manual', nodeId: 'w-03', ...fields })

export const snapshots: SnapshotInfo[] = [
  snapshot({
    snapshotId: 'snap-3f9a0c21d4e5b6a7',
    instanceId: 'user-42',
    name: 'before-refactor',
    kind: 'memory',
    state: 'local',
    totalBytes: 1203n * MIB,
    durationMs: 740n,
    createdAt: new Date(Date.now() - 2 * 3600_000).toISOString(),
    expiresAt: new Date(Date.now() + 5 * 86400_000).toISOString(),
  }),
  snapshot({ snapshotId: 'snap-0c1d2e3f4a5b6c7d', instanceId: 'user-42', kind: 'disk', state: 'capturing', createdAt: new Date().toISOString() }),
  snapshot({
    snapshotId: 'snap-77e0aa41c0de9e12',
    instanceId: 'user-17',
    kind: 'disk',
    state: 'failed',
    error: 'volume driver local-dir: snapshot: no space left on device',
    createdAt: new Date(Date.now() - 86400_000).toISOString(),
  }),
  snapshot({ snapshotId: 'snap-1a4d9e0b7c6f5e4d', instanceId: 'user-09', kind: 'disk', state: 'deleting', totalBytes: 210n * MIB, createdAt: new Date(Date.now() - 3 * 86400_000).toISOString() }),
  snapshot({ snapshotId: 'snap-9b8a7c6d5e4f3a2b', instanceId: 'user-05', kind: 'memory', state: 'lost', totalBytes: 980n * MIB, createdAt: new Date(Date.now() - 6 * 86400_000).toISOString() }),
  // Values a later manager will send (a suspend checkpoint, published to the
  // content store). Here only to prove the table renders unknown values
  // instead of breaking on them.
  snapshot({ snapshotId: 'snap-5e5e5e5e5e5e5e5e', instanceId: 'user-31', kind: 'memory', purpose: 'suspend', state: 'published', totalBytes: 1100n * MIB, createdAt: new Date(Date.now() - 600_000).toISOString() }),
]

export const snapshotEvents: SchedulerEvent[] = [
  event(
    2n,
    'snapshot.created',
    'info',
    'Snapshot snap-3f9a0c21d4e5b6a7 (memory) of instance user-42 captured on w-03: 1261436928 bytes in 740 ms',
    { jobId: 'workspaces', nodeId: 'w-03' },
    { snapshotId: 'snap-3f9a0c21d4e5b6a7', instanceId: 'user-42', kind: 'memory', purpose: 'manual', name: 'before-refactor', totalBytes: 1261436928, durationMs: 740, error: null },
  ),
  event(
    4n,
    'snapshot.failed',
    'warn',
    'Snapshot snap-77e0aa41c0de9e12 of instance user-17 failed: volume driver local-dir: snapshot: no space left on device',
    { jobId: 'workspaces', nodeId: 'w-03' },
    { snapshotId: 'snap-77e0aa41c0de9e12', instanceId: 'user-17', kind: 'disk', purpose: 'manual', totalBytes: 0, durationMs: 0, error: 'volume driver local-dir: snapshot: no space left on device' },
  ),
]
