# A full-stack notes app wired through the service mesh:
#
#   browser ──► frontend (nginx, x2, :8080) ──► api (python, x2) ──┬─► postgres (stateful)
#                       mesh, HTTP mode ──────────────┘   mesh   └─► cache (redis)
#
# No tier knows another's address. Each dials a loopback port
# ($LEGION_UPSTREAM_<SERVICE>) and the node's forwarder picks a healthy
# endpoint, which may be on the other worker. Two frontends on a fixed port
# land one per worker, so cross-node mesh traffic is guaranteed.
#
#   legion kv put fullstack/db-password "$(openssl rand -hex 16)"
#   legion job run tests/spot-check/jobs/06-fullstack.ncl
#   open http://<worker-public-ip>:8080/     # add notes; watch `served by`
#   legion service resolve api
#   legion job scale fullstack --task api --count 4
let images = (import "lib/images.ncl") vars in

let db_password = [
  { name = "POSTGRES_PASSWORD", value = { template = "{{ kv \"fullstack/db-password\" }}" } },
]
in

{
  name = "fullstack",
  tasks = [
    {
      name = "frontend",
      group = "frontend",
      runtime = "container",
      lifecycle = "service",
      count = 2,
      dependsOn = [{ task = "api", condition = "start" }],
      config = { image = images.frontend, command = ["/start.sh"] },
      network = { ports = [{ name = "http", static = 8080 }] },
      service = { port = "http", meta = { protocol = "http" } },
      connect = {
        upstreams = [
          {
            service = "api",
            # HTTP mode: per-request balancing, retries, and outlier ejection.
            http = {
              timeout = "5s",
              retries = { attempts = 2, on = ["5xx", "connect-failure"] },
              outlier = { consecutive5xx = 5, ejectFor = "30s" },
            },
          },
        ],
      },
      checks = [{ type = "health", kind = "http", path = "/whoami", port = "http", every = "5s", timeout = "2s" }],
      resources = { cpu = 100, memory = 64 },
    },
    {
      name = "api",
      group = "api",
      runtime = "container",
      lifecycle = "service",
      count = 2,
      dependsOn = [
        { task = "postgres", condition = "start" },
        { task = "cache", condition = "start" },
      ],
      env = db_password,
      config = { image = images.api, command = ["python3", "/app/app.py"] },
      network = { ports = [{ name = "http" }] },
      # Only this job may reach the API through the mesh.
      service = { port = "http", meta = { protocol = "http" }, allow = ["fullstack"] },
      connect = {
        upstreams = [
          { service = "postgres", balance = "least_conn" },
          { service = "cache" },
        ],
      },
      checks = [
        { type = "health", kind = "http", path = "/api/healthz", port = "http", every = "5s", timeout = "2s", boot_grace_time = "60s" },
      ],
      resources = { cpu = 250, memory = 128 },
    },
    {
      name = "postgres",
      group = "db",
      runtime = "container",
      lifecycle = "service",
      count = 1,
      stateful = {
        mode = "slot",
        # Writable by the image's non-root postgres user (uid 70).
        storage = { path = "/var/lib/postgresql/data", driver = "local-dir", params = { size = "2G", mode = "0777" } },
      },
      env = db_password,
      config = {
        # The stock image's root entrypoint needs CAP_CHOWN and CAP_SETUID,
        # which Legion's Linux containers lack. images/db runs as postgres.
        image = images.db,
        command = ["sh", "-c", "exec docker-entrypoint.sh postgres -p \"$LEGION_PORT_PG\""],
        env = { PGDATA = "/var/lib/postgresql/data/pgdata", POSTGRES_DB = "app" },
      },
      network = { ports = [{ name = "pg" }] },
      service = { port = "pg", allow = ["fullstack"] },
      checks = [
        { type = "health", kind = "tcp", port = "pg", every = "10s", timeout = "5s", boot_grace_time = "60s" },
      ],
      resources = { cpu = 500, memory = 512 },
    },
    {
      name = "cache",
      group = "cache",
      runtime = "container",
      lifecycle = "service",
      count = 1,
      config = {
        image = "public.ecr.aws/docker/library/redis:7-alpine",
        # Protected mode would refuse mesh connections arriving from the other
        # worker's address.
        command = ["sh", "-c", "exec redis-server --port \"$LEGION_PORT_REDIS\" --save '' --appendonly no --protected-mode no"],
      },
      network = { ports = [{ name = "redis" }] },
      service = { port = "redis", allow = ["fullstack"] },
      checks = [{ type = "health", kind = "tcp", port = "redis", every = "5s", timeout = "2s" }],
      resources = { cpu = 100, memory = 128 },
    },
  ],
}
