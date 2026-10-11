import {
  SnapshotBudget,
  SuspendedCapacity,
  AllocationUsage,
  CheckpointSupport,
  GetNodeContentReply,
  GetNodeUsageReply,
  InstanceInfo,
  InstanceStop,
  MemoryPressureStatus,
  NodeAllocationUsage,
  NodeContentEntry,
  NodeResourceUsage,
  NodeSnapshotContent,
  SchedulerEvent,
  SnapshotHolder,
  SnapshotInfo,
} from '../gen/legion_pb'

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
    minFreeBytes: 2n * GIB,
    maxStallPercent: 10,
    settleSeconds: 5n,
    steps,
  })

/** A macOS worker, or one with enforcement off: capacity and reservations only. */
export const unreportedNode = new GetNodeUsageReply({
  nodeId: 'mac-dev',
  status: 'ready',
  usageReported: false,
  memory: new NodeResourceUsage({ capacity: 16384, reserved: 768, oversubscribeRatio: 1 }),
  // Apple Silicon: cores, but no usable clock speed, so no MHz capacity.
  cpu: new NodeResourceUsage({ capacity: 0, reserved: 600, oversubscribeRatio: 1, cores: 10 }),
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
  suspended: new SuspendedCapacity({
    instances: 2,
    holding: 1,
    heldMemoryMib: 512,
    heldCpuMhz: 250,
    checkpointBytes: 1100n * MIB,
    snapshotBudget: new SnapshotBudget({ budgetBytes: 20n * 1024n * MIB, usedBytes: 6n * 1024n * MIB, pinnedBytes: 1100n * MIB, diskFreeBytes: 180n * 1024n * MIB, minFreeDiskBytes: 5n * 1024n * MIB, reclaimableBytes: 4n * 1024n * MIB }),
  }),
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

// Pass 2: suspend, resume, fork, durability, locality ------------------------

const ago = (ms: number) => new Date(Date.now() - ms).toISOString()

const sandboxSupport = new CheckpointSupport({
  snapshotMemory: true,
  snapshotDisk: true,
  suspendMemory: true,
  suspendDefault: 'memory',
  forkMemory: true,
  forkDisk: true,
  spread: false,
  spreadBlocker:
    '--spread places forks on other nodes, but instance user-42 keeps its data on a `local-dir` volume (stateful.storage of task "workspace"), and each fork\'s copy is cloned on the origin\'s node w-03, so a fork on another node could not get it\nHint: drop --spread (forks run on the origin\'s node), or drop stateful.storage so the whole instance is in its memory checkpoint',
  publish: false,
  publishBlocker: 'task "workspace" stores its data on a node-local `local-dir` volume, which cannot follow a published snapshot to another node',
})

const execSupport = new CheckpointSupport({
  snapshotDisk: true,
  snapshotMemoryBlocker: 'instance user-17 cannot take a memory snapshot: runtime "exec" supports disk snapshots only\nHint: use --disk, or run this task with runtime = "sandbox"',
  suspendMemoryBlocker: 'instance user-17 cannot take a memory checkpoint: runtime "exec" supports disk checkpoints only',
  suspendDefault: 'disk',
  forkDisk: true,
  forkMemoryBlocker: 'instance user-17 cannot take a memory checkpoint: runtime "exec" supports disk checkpoints only',
  spreadBlocker: '--spread places forks on other nodes, which restore from a published checkpoint, and a disk checkpoint cannot be published',
  publishBlocker: 'instance user-17 cannot take a memory snapshot: runtime "exec" supports disk snapshots only',
})

const instance = (fields: Partial<InstanceInfo>) =>
  new InstanceInfo({ jobId: 'workspaces', group: 'default', task: 'workspace', mode: 'keyed', createdAt: ago(86400_000), updatedAt: ago(60_000), ...fields })

/** One of every instance shape pass 2 renders. */
export const instances: InstanceInfo[] = [
  // Active, idle, with an idle policy: counts down to its suspend.
  instance({
    instanceId: 'user-42',
    state: 'active',
    currentAllocId: 1042n,
    storageHandle: 'local-dir/vol-user-42',
    usage: usage(1042n, 1830, { lastActivityUnix: nowSeconds() - 161n }),
    idleSince: ago(161_000),
    idleAfterSeconds: 180n,
    checkpointSupport: sandboxSupport,
  }),
  // Restarted after a memory pressure eviction: the row says why.
  instance({
    instanceId: 'user-17',
    task: 'shell',
    state: 'active',
    currentAllocId: 1077n,
    storageHandle: 'local-dir/vol-user-17',
    usage: usage(1077n, 402),
    checkpointSupport: execSupport,
    lastStop: new InstanceStop({
      cause: 'node_pressure',
      detail: 'evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation while free memory was 1.1 GiB (threshold 2 GiB)',
      hint: 'raise resources.memory toward real usage',
      at: ago(3 * 3600_000),
      allocationId: 1063n,
      nodeId: 'w-03',
    }),
  }),
  // Suspended by its idle policy to a local memory checkpoint.
  instance({
    instanceId: 'user-09',
    state: 'suspended',
    storageHandle: 'local-dir/vol-user-09',
    checkpointId: 'snap-77e0aa41c0de9e12',
    checkpointKind: 'memory',
    checkpointDurability: 'local',
    checkpointState: 'local',
    checkpointNodeId: 'w-03',
    checkpointBytes: 1100n * MIB,
    heldMemoryMib: 512,
    heldCpuMhz: 250,
    suspendedAt: ago(4 * 60_000),
    suspendReason: 'idle',
    lastSuspendMs: 740n,
    lastRestoreMs: 212n,
  }),
  // Suspended for node pressure to a published checkpoint: resumes anywhere.
  instance({
    instanceId: 'agent-3',
    state: 'suspended',
    checkpointId: 'snap-5e5e5e5e5e5e5e5e',
    checkpointKind: 'memory',
    checkpointDurability: 'store',
    checkpointState: 'published',
    checkpointNodeId: 'w-02',
    checkpointBytes: 512n * MIB,
    suspendedAt: ago(20 * 60_000),
    suspendReason: 'pressure',
    lastSuspendMs: 1280n,
  }),
  // A disk suspend: the volume stays in place.
  instance({
    instanceId: 'user-05',
    task: 'shell',
    state: 'suspended',
    storageHandle: 'local-dir/vol-user-05',
    checkpointId: 'snap-0d15c0ffee000001',
    checkpointKind: 'disk',
    checkpointDurability: 'local',
    checkpointState: 'local',
    checkpointNodeId: 'w-01',
    checkpointBytes: 0n,
    suspendedAt: ago(2 * 86400_000),
    suspendReason: 'manual',
    lastSuspendMs: 95n,
  }),
  // Resuming.
  instance({ instanceId: 'user-31', state: 'pending', pendingOp: 'resuming', restoreSnapshotId: 'snap-31aa31aa31aa31aa', checkpointId: 'snap-31aa31aa31aa31aa', checkpointKind: 'memory', checkpointState: 'local', checkpointNodeId: 'w-03' }),
  // Forks of user-42: one waiting for the checkpoint, one already running.
  instance({ instanceId: 'user-42-f1', state: 'active', currentAllocId: 1101n, forkedFrom: 'user-42', restoredFromSnapshotId: 'snap-f0f0f0f0f0f0f0f0', lastRestoreMs: 180n, checkpointSupport: sandboxSupport }),
  instance({ instanceId: 'user-42-f2', state: 'pending', pendingOp: 'forking', forkedFrom: 'user-42', restoreSnapshotId: 'snap-f0f0f0f0f0f0f0f0' }),
  // A slot instance: no suspend or fork.
  instance({ instanceId: 'cache-default-0', mode: 'slot', slotIndex: 0, state: 'active', currentAllocId: 990n }),
]

/** Snapshots pass 2 adds: published with names and holders, a fork checkpoint, a disk suspend. */
export const pass2Snapshots: SnapshotInfo[] = [
  snapshot({
    snapshotId: 'snap-90d1e90d1e90d1e9',
    instanceId: 'agent-3',
    name: 'golden',
    aliases: ['golden', 'golden-2026-10'],
    kind: 'memory',
    state: 'published',
    durability: 'store',
    totalBytes: 512n * MIB,
    durationMs: 1280n,
    pauseMs: 610n,
    publishedAt: ago(50 * 60_000),
    createdAt: ago(3600_000),
    holders: [
      new SnapshotHolder({ nodeId: 'w-02', partsHere: 3, partsTotal: 3, bytesHere: 512n * MIB, capturedHere: true }),
      new SnapshotHolder({ nodeId: 'w-04', partsHere: 3, partsTotal: 3, bytesHere: 512n * MIB }),
      new SnapshotHolder({ nodeId: 'w-05', partsHere: 1, partsTotal: 3, bytesHere: 480n * MIB }),
    ],
  }),
  snapshot({
    snapshotId: 'snap-7a7a7a7a7a7a7a7a',
    instanceId: 'agent-3',
    kind: 'memory',
    state: 'local',
    durability: 'store',
    error: 'publish attempt 1 of 3 failed, retrying in 1m: s3: access denied',
    totalBytes: 498n * MIB,
    createdAt: ago(10 * 60_000),
  }),
  snapshot({ snapshotId: 'snap-f0f0f0f0f0f0f0f0', instanceId: 'user-42', kind: 'memory', purpose: 'fork', state: 'local', durability: 'local', totalBytes: 1830n * MIB, pauseMs: 420n, createdAt: ago(5 * 60_000) }),
  snapshot({ snapshotId: 'snap-0d15c0ffee000001', instanceId: 'user-05', kind: 'disk', purpose: 'suspend', state: 'local', durability: 'local', nodeId: 'w-01', createdAt: ago(2 * 86400_000) }),
  snapshot({ snapshotId: 'snap-d15cd15cd15cd15c', instanceId: 'user-17', kind: 'disk', state: 'local', durability: 'local', totalBytes: 37n * MIB, createdAt: ago(30 * 60_000) }),
]

/** A node holding a warmed published snapshot and a captured one. */
export const nodeContent = new GetNodeContentReply({
  nodeId: 'w-04',
  indexReported: true,
  epoch: 42n,
  canPublish: true,
  canCheckpoint: true,
  advertisedBytes: 512n * MIB + 64n * MIB,
  entries: [
    new NodeContentEntry({ digest: 'sha256:3f9a0c21d4e5b6a7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1', bytes: 480n * MIB, snapshotId: 'snap-90d1e90d1e90d1e9', role: 'memory', holders: 3 }),
    new NodeContentEntry({ digest: 'sha256:aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00aa00', bytes: 64n * MIB, holders: 5 }),
    new NodeContentEntry({ digest: 'sha256:bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11bb11', bytes: 30n * MIB, snapshotId: 'snap-90d1e90d1e90d1e9', role: 'vm-state', holders: 2 }),
  ],
  snapshots: [
    new NodeSnapshotContent({ snapshot: pass2Snapshots[0], capturedHere: false, partsHere: 3, partsTotal: 3, bytesHere: 512n * MIB }),
    new NodeSnapshotContent({ snapshot: snapshot({ snapshotId: 'snap-c4c4c4c4c4c4c4c4', instanceId: 'user-88', kind: 'disk', state: 'local', nodeId: 'w-04', totalBytes: 12n * MIB, createdAt: ago(7200_000) }), capturedHere: true }),
  ],
})

/** A node with no content store, or one whose report has not reached this manager. */
export const unreportedContent = new GetNodeContentReply({ nodeId: 'mac-dev', indexReported: false, canCheckpoint: true })

export const instanceEvents: SchedulerEvent[] = [
  event(
    7n,
    'instance.suspended',
    'info',
    'Instance user-09 suspended (idle) to memory checkpoint snap-77e0aa41c0de9e12 on w-03: 1153433600 bytes in 740 ms',
    { jobId: 'workspaces', nodeId: 'w-03' },
    { snapshotId: 'snap-77e0aa41c0de9e12', instanceId: 'user-09', kind: 'memory', purpose: 'suspend', totalBytes: 1153433600, durationMs: 740, reason: 'idle', reserved: false },
  ),
  event(
    8n,
    'instance.resumed',
    'info',
    'Instance user-09 resumed from snap-77e0aa41c0de9e12 as allocation 1120 on w-03 (212 ms)',
    { jobId: 'workspaces', nodeId: 'w-03', allocationId: 1120n },
    { instanceId: 'user-09', snapshotId: 'snap-77e0aa41c0de9e12', restoreMs: 212 },
  ),
  event(
    9n,
    'instance.forked',
    'info',
    'Instance user-42 forked into user-42-f1, user-42-f2 (memory checkpoint snap-f0f0f0f0f0f0f0f0 on w-03, paused 420 ms)',
    { jobId: 'workspaces', nodeId: 'w-03' },
    { snapshotId: 'snap-f0f0f0f0f0f0f0f0', instanceId: 'user-42', kind: 'memory', purpose: 'fork', origin: 'user-42', instances: ['user-42-f1', 'user-42-f2'], pauseMs: 420 },
  ),
  event(
    10n,
    'instance.restore_failed',
    'warn',
    'Instance user-31 could not restore checkpoint snap-31aa31aa31aa31aa and booted cold on its retained storage: snapshot/load failed',
    { jobId: 'workspaces', nodeId: 'w-03' },
    { instanceId: 'user-31', snapshotId: 'snap-31aa31aa31aa31aa', error: 'restoring sandbox from checkpoint: snapshot/load failed', outcome: 'cold' },
  ),
]
