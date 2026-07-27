// Redis, twice: one durable instance and one pure cache.
//
// Guide: docs/guides/redis.md
//
//   legion job run configs/jobs/redis.jsonnet
//   legion volume list --job redis
//
// The two tasks exist to show the choice volumes force you to make: is this
// Redis a database (persist it, pin it, never lose it) or a cache (let it
// vanish)? They are genuinely different jobs-to-be-done, and Legion makes them
// look different in the spec.
{
  name: 'redis',
  tasks: [
    {
      // --- Durable: an AOF-backed Redis whose data survives everything. ---
      name: 'store',
      kind: 'exec',
      count: 1,
      group: 'store',

      stateful: {
        mode: 'slot',
        storage: {
          // exec tasks run in the host mount namespace, so this path is
          // informational — the command below reads $LEGION_STORAGE_DIR, which
          // carries the real host path (see docs/volumes.md).
          path: '/var/lib/redis',
          driver: 'local-dir',
          params: { size: '5G' },
        },
      },

      config: {
        command: [
          'redis-server',
          '--dir', '$LEGION_STORAGE_DIR',
          // appendonly is the point: without it a restart loses everything
          // written since the last background save, which makes the volume
          // mostly decorative.
          '--appendonly', 'yes',
          '--appendfsync', 'everysec',
          '--port', '$LEGION_PORT_REDIS',
        ],
      },

      network: {
        mode: 'auto',
        ports: [{ name: 'redis', to: 6379 }],
      },
      service: { name: 'redis', port: 'redis' },

      checks: [{
        type: 'health',
        name: 'ping',
        kind: 'exec',
        command: ['redis-cli', '-p', '$LEGION_PORT_REDIS', 'ping'],
        every: '10s',
        timeout: '3s',
      }],
    },
    {
      // --- Cache: no persistence, no pin, no volume lifecycle. ---
      name: 'cache',
      kind: 'exec',
      count: 2,
      group: 'cache',

      // No `stateful` block at all. A cache has nothing worth an identity:
      // losing a replica should cost a cold start, not a recovery procedure.
      // Scratch space comes from a `volumes` claim instead, which carries no
      // per-instance lifecycle.
      //
      // On Linux, swap this for a real RAM disk:
      //   volumes: [{ name: 'scratch', driver: 'tmpfs',
      //               params: { size: '1G' }, path: '/scratch' }],
      // `tmpfs` is ephemeral, which is exactly why admission would reject it
      // under stateful.storage.

      config: {
        command: [
          'redis-server',
          '--save', '',            // no RDB snapshots
          '--appendonly', 'no',    // no AOF
          '--maxmemory', '256mb',
          '--maxmemory-policy', 'allkeys-lru',
          '--port', '$LEGION_PORT_REDIS',
        ],
      },

      network: {
        mode: 'auto',
        ports: [{ name: 'redis', to: 6379 }],
      },
      service: { name: 'redis-cache', port: 'redis' },

      checks: [{
        type: 'health',
        name: 'ping',
        kind: 'exec',
        command: ['redis-cli', '-p', '$LEGION_PORT_REDIS', 'ping'],
        every: '10s',
        timeout: '3s',
      }],
    },
  ],
}
