# A single-instance Postgres with persistent storage.
#
# Guide = docs/guides/postgres.md
#
#   legion job run configs/jobs/postgres.ncl
#   legion volume list --job postgres
#
# Storage = one `local-dir` volume bound to the instance. It survives task
# restarts, redeploys, and reschedules, and pins the instance to whatever node
# it first landed on (local-dir is node-local). For a Postgres that can move
# between nodes, swap the plugin for `nfs` — see the guide.
{
  name = "postgres",
  tasks = [{
    name = "postgres",
    runtime = "container",
    # Exactly one writer. Postgres refuses to start twice against one data
    # directory anyway; slot mode makes that a scheduling guarantee rather
    # than a crash (see docs/stateful-workloads.md "Node loss and fencing").
    count = 1,
    group = "db",

    stateful = {
      mode = "slot",
      storage = {
        # Container tasks get a real bind mount at this path, so it can be
        # exactly what the image expects.
        path = "/var/lib/postgresql/data",
        driver = "local-dir",
        params = { size = "20G" },
        # Both default true. Keeping them explicit here because losing a
        # database to a scale-in is not a thing to leave implicit.
        retainOnScaleIn = true,
        retainOnStop = true,
      },
      onLost = {
        # A partitioned node must provably stop writing before a replacement
        # binds the same data directory. Defaults (90s/180s) are fine for most
        # deployments; lower them only if your failover budget demands it.
        selfTerminateAfter = "90s",
        replaceAfter = "3m",
      },
    },

    config = {
      image = "postgres:16-alpine",
      # Run the image's entrypoint explicitly: on Linux, `command` replaces
      # the image's ENTRYPOINT as well as its CMD, and the entrypoint is what
      # runs initdb and drops from root to the postgres user. Listen on the
      # port Legion assigned: Linux containers share the host's network, so a
      # fixed 5432 would collide with a second Postgres on the node and would
      # not be the port the service advertises.
      command = ["sh", "-c", "exec docker-entrypoint.sh postgres -p \"$LEGION_PORT_PG\""],
      env = {
        # PGDATA must be a SUBDIRECTORY of the mount, not the mount itself:
        # the official image refuses to initdb into a directory that already
        # exists with content (and a fresh volume may carry lost+found).
        PGDATA = "/var/lib/postgresql/data/pgdata",
        # Demo only. In a real deployment inject this from KV:
        #   env = [{ name = "POSTGRES_PASSWORD",
        #           value = { template = "{{ kv \"secrets/postgres\" }}" } }]
        POSTGRES_PASSWORD = "legion-demo",
        POSTGRES_DB = "legion",
      },
    },

    network = {
      mode = "auto",
      # No `to`: Postgres listens on the assigned port itself (see command),
      # which is also what Apple `container` publishes on macOS.
      ports = [{ name = "pg" }],
    },
    # Discoverable as `postgres` — clients resolve it instead of hardcoding a
    # host (see docs/service-discovery.md).
    service = { name = "postgres", port = "pg" },

    # A TCP check, not `pg_isready`: exec checks run on the host, outside the
    # container, where neither the binary nor the task's env exists.
    checks = [{
      type = "health",
      name = "ready",
      kind = "tcp",
      port = "pg",
      every = "10s",
      timeout = "5s",
      # Give initdb room on a fresh volume before failing the allocation.
      boot_grace_time = "60s",
    }],

    # Day-2 operations, pre-baked (docs/exec-and-actions.md). Actions run
    # inside the container via the task's exec support, so `psql` is the
    # image's own binary and no client install is needed on the operator's
    # machine. They run with the task's env, so `sh -c` can pass the assigned
    # port to the client (the param stays a positional argument, never shell
    # text):
    #   legion action run postgres psql --tty      # interactive shell
    #   legion action run postgres sql --param query='SELECT 1'
    #   legion action run postgres dump > backup.sql
    actions = [
      {
        name = "psql",
        description = "Interactive psql shell inside the container",
        command = ["sh", "-c", "exec psql -p \"$LEGION_PORT_PG\" -U postgres -d legion"],
        tty = true,
      },
      {
        name = "sql",
        description = "Run one SQL statement and print the result",
        command = [
          "sh",
          "-c",
          "exec psql -p \"$LEGION_PORT_PG\" -U postgres -d legion -c \"$1\"",
          "sql",
          "${param.query}",
        ],
        params = [{ name = "query", description = "SQL to execute" }],
        timeout = "60s",
      },
      {
        name = "dump",
        description = "pg_dump the database to stdout (redirect to a file)",
        command = ["sh", "-c", "exec pg_dump -p \"$LEGION_PORT_PG\" -U postgres legion"],
        timeout = "10m",
      },
    ],
  }],

  # --- Scheduled backups (uncomment to enable) -------------------------------
  # Volumes are storage, not backups: a dead disk and `volume delete --force`
  # both lose data. A scheduled task dumping to object storage (or any path
  # outside the cluster) is the actual safety net. The `dump` action above is
  # the on-demand version of the same thing.
  #
  # Add this entry to the `tasks` array above:
  # {
  #   name = "backup",
  #   runtime = "container",
  #   group = "db",
  #   lifecycle = "batch",
  #   schedule = "0 3 * * *",            # 03:00 daily
  #   # Reach the database through service discovery. The worker load-balances
  #   # $LEGION_UPSTREAM_POSTGRES (127.0.0.1:<port>) to the healthy Postgres,
  #   # since its host port is dynamic. In a real deployment, inject the
  #   # password from KV instead of inlining it.
  #   connect = { upstreams = [{ service = "postgres" }] },
  #   config = {
  #     image = "postgres:16-alpine",
  #     command = ["sh", "-c", "pg_dump \"postgres://postgres:legion-demo@$LEGION_UPSTREAM_POSTGRES/legion\" | gzip > \"$LEGION_TASK_DIR/pg-$(date +%F).sql.gz\""],
  #   },
  #   # Ship the dump off-cluster: swap the command above for `aws s3 cp -`
  #   # (plus credentials via workload identity) or mount a `host` volume of
  #   # a backup disk via `volumes = [...]`.
  # }
}
