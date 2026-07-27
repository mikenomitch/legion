// A single-instance Postgres with persistent storage.
//
// Guide: docs/guides/postgres.md
//
//   legion job run configs/jobs/postgres.jsonnet
//   legion volume list --job postgres
//
// Storage: one `local-dir` volume bound to the instance. It survives task
// restarts, redeploys, and reschedules, and pins the instance to whatever node
// it first landed on (local-dir is node-local). For a Postgres that can move
// between nodes, swap the plugin for `nfs` — see the guide.
{
  name: 'postgres',
  tasks: [{
    name: 'postgres',
    kind: 'container',
    // Exactly one writer. Postgres refuses to start twice against one data
    // directory anyway; slot mode makes that a scheduling guarantee rather
    // than a crash (see docs/stateful-workloads.md "Node loss and fencing").
    count: 1,
    group: 'db',

    stateful: {
      mode: 'slot',
      storage: {
        // Container tasks get a real bind mount at this path, so it can be
        // exactly what the image expects.
        path: '/var/lib/postgresql/data',
        plugin: 'local-dir',
        params: { size: '20G' },
        // Both default true. Keeping them explicit here because losing a
        // database to a scale-in is not a thing to leave implicit.
        retainOnScaleIn: true,
        retainOnStop: true,
      },
      onLost: {
        // A partitioned node must provably stop writing before a replacement
        // binds the same data directory. Defaults (90s/180s) are fine for most
        // deployments; lower them only if your failover budget demands it.
        selfTerminateAfter: '90s',
        replaceAfter: '3m',
      },
    },

    config: {
      image: 'postgres:16-alpine',
      command: ['postgres'],
      env: {
        // PGDATA must be a SUBDIRECTORY of the mount, not the mount itself:
        // the official image refuses to initdb into a directory that already
        // exists with content (and a fresh volume may carry lost+found).
        PGDATA: '/var/lib/postgresql/data/pgdata',
        // Demo only. In a real deployment inject this from KV:
        //   env: [{ name: 'POSTGRES_PASSWORD',
        //           value: { template: '{{ kv "secrets/postgres" }}' } }]
        POSTGRES_PASSWORD: 'legion-demo',
        POSTGRES_DB: 'legion',
      },
    },

    network: {
      mode: 'auto',
      ports: [{ name: 'pg', to: 5432 }],
    },
    // Discoverable as `postgres` — clients resolve it instead of hardcoding a
    // host (see docs/service-discovery.md).
    service: { name: 'postgres', port: 'pg' },

    checks: [{
      type: 'health',
      name: 'ready',
      kind: 'exec',
      command: ['pg_isready', '-U', 'postgres'],
      every: '10s',
      timeout: '5s',
      // Give initdb room on a fresh volume before failing the allocation.
      boot_grace_time: '60s',
    }],
  }],
}
