# A three-tier app: an nginx frontend, a Python API, and Postgres, each in
# its own group so it places and scales on its own.
#
#   browser ──► frontend (nginx, x2) ──► api (python, x3) ──► postgres (x1)
#                  │                       │                    + postgres-exporter
#                  └ /api/* via the mesh   └ SQL via the mesh     (sidecar)
#
# No tier knows another tier's address. Each one dials a loopback port
# ($LEGION_UPSTREAM_<SERVICE>) and the node's forwarder load-balances across
# the healthy endpoints of the service behind it. Scale a tier and its callers
# pick up the new endpoints on the next heartbeat, with no redeploy.
#
# The database password lives in KV, which is encrypted at rest. Seed it
# before the first run:
#
#   legion kv put fullstack/db-password "$(openssl rand -hex 16)"
#   legion job run configs/jobs/fullstack.ncl
#   curl "http://$(legion service resolve frontend --one)/api/visits"
#
# Scale each tier on its own, without a new job version:
#
#   legion job scale fullstack --task api --count 6
#   legion job scale fullstack --task frontend --count 4
#
# Or set the starting counts at submit time:
#
#   legion job run configs/jobs/fullstack.ncl --var-code api_count=5
#
# Linux workers only: containers share the host network there, so they can
# reach the node's loopback upstreams. Apple `container` runs each container
# in its own VM, where the host's loopback is out of reach.

let params =
  {
    frontend_count | Number | default = 2,
    api_count | Number | default = 3,
  }
  & vars
in

# Postgres and the API both read the password from the same KV entry.
let db_password_env = [
  {
    name = "POSTGRES_PASSWORD",
    value = { template = "{{ kv \"fullstack/db-password\" }}" },
  },
]
in

# The whole API, inlined as a Nickel multi-line string. It counts visits in
# Postgres and reports which allocation answered, so repeated requests show
# the frontend's forwarder spreading load across the API replicas.
let api_py = m%"
import json, os, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pg8000.native

host, port = os.environ["LEGION_UPSTREAM_POSTGRES"].split(":")

def connect():
    # Postgres may still be starting, so retry instead of crash-looping.
    while True:
        try:
            return pg8000.native.Connection(
                "postgres", host=host, port=int(port), database="app",
                password=os.environ["POSTGRES_PASSWORD"])
        except Exception as e:
            print(f"waiting for postgres: {e}", flush=True)
            time.sleep(2)

db = connect()
db.run("CREATE TABLE IF NOT EXISTS visits (at timestamptz DEFAULT now())")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            return self.reply(200, {"ok": True})
        if self.path == "/api/visits":
            db.run("INSERT INTO visits DEFAULT VALUES")
            [[count]] = db.run("SELECT count(*) FROM visits")
            return self.reply(200, {"visits": count, "served_by": os.environ["LEGION_ALLOC_ID"]})
        self.reply(404, {"error": "not found"})

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

ThreadingHTTPServer(("0.0.0.0", int(os.environ["LEGION_PORT_HTTP"])), Handler).serve_forever()
"%
in

# nginx serves a static page and proxies /api/ to the mesh upstream. The
# config is written at start because the ports are only known then.
let nginx_start = m%"
cat > /etc/nginx/conf.d/default.conf <<CONF
server {
  listen $LEGION_PORT_HTTP;
  location /api/ { proxy_pass http://$LEGION_UPSTREAM_API; }
  location / { root /usr/share/nginx/html; }
}
CONF
cat > /usr/share/nginx/html/index.html <<'HTML'
<!doctype html>
<title>Legion full stack</title>
<pre id="out">loading...</pre>
<script>
  fetch("/api/visits").then(r => r.json())
    .then(j => out.textContent = JSON.stringify(j, null, 2));
</script>
HTML
exec nginx -g 'daemon off;'
"%
in

let http_check = fun check_path =>
  {
    type = "health",
    kind = "http",
    path = check_path,
    port = "http",
    every = "5s",
    timeout = "2s",
    unhealthy_after_count = 3,
  }
in

{
  name = "fullstack",
  tasks = [
    {
      name = "frontend",
      group = "frontend",
      runtime = "container",
      lifecycle = "service",
      count = params.frontend_count,
      dependsOn = [{ task = "api", condition = "start" }],
      config = {
        image = "nginx:1.27-alpine",
        command = ["sh", "-c", nginx_start],
      },
      network = { ports = [{ name = "http" }] },
      service = { port = "http", meta = { protocol = "http" } },
      connect = {
        upstreams = [
          {
            service = "api",
            # HTTP mode: per-request balancing, a deadline, retries on
            # idempotent requests, and ejecting a replica that keeps failing.
            http = {
              timeout = "5s",
              retries = { attempts = 2, on = ["5xx", "connect-failure"] },
              outlier = { consecutive5xx = 5, ejectFor = "30s" },
            },
          },
        ],
      },
      checks = [http_check "/"],
      resources = { cpus = 0.1, memory = 64 },
    },
    {
      name = "api",
      group = "api",
      runtime = "container",
      lifecycle = "service",
      count = params.api_count,
      dependsOn = [{ task = "postgres", condition = "start" }],
      env = db_password_env,
      config = {
        image = "python:3.12-alpine",
        # A demo shortcut. A real service bakes its dependencies into the
        # image instead of installing them at start.
        command = ["sh", "-c", "pip install --quiet pg8000 && exec python3 -c \"$APP_PY\""],
        env = { APP_PY = api_py, PYTHONUNBUFFERED = "1" },
      },
      network = { ports = [{ name = "http" }] },
      # Only the frontend may reach the API through the mesh.
      service = { port = "http", meta = { protocol = "http" }, allow = ["fullstack"] },
      # Least-connections, so a slow query doesn't pile more requests on
      # the replica that's already busy.
      connect = { upstreams = [{ service = "postgres", balance = "least_conn" }] },
      checks = [http_check "/healthz" & { boot_grace_time = "60s" }],
      resources = { cpus = 0.25, memory = 128 },
    },
    {
      name = "postgres",
      group = "db",
      runtime = "container",
      lifecycle = "service",
      # One writer, pinned to its volume. See postgres.ncl and
      # docs/guides/postgres.md for the details of each setting.
      count = 1,
      stateful = {
        mode = "slot",
        storage = { path = "/var/lib/postgresql/data", driver = "local-dir", params = { size = "10G" } },
      },
      env = db_password_env,
      config = {
        image = "postgres:16-alpine",
        command = ["sh", "-c", "exec docker-entrypoint.sh postgres -p \"$LEGION_PORT_PG\""],
        env = {
          PGDATA = "/var/lib/postgresql/data/pgdata",
          POSTGRES_DB = "app",
        },
      },
      network = { ports = [{ name = "pg" }] },
      service = { port = "pg", allow = ["fullstack"] },
      # A TCP check, not `pg_isready`: exec checks run on the host, outside
      # the container, where neither the binary nor the task env exists.
      checks = [
        { type = "health", kind = "tcp", port = "pg", every = "10s", timeout = "5s", boot_grace_time = "60s" },
      ],
      resources = { cpus = 0.5, memory = 512 },
    },
    # A sidecar runs beside its group's primary task and shares its node and
    # lifecycle. This one exports Postgres metrics for Prometheus to scrape.
    {
      name = "postgres-exporter",
      group = "db",
      runtime = "container",
      lifecycle = "sidecar",
      dependsOn = [{ task = "postgres", condition = "start" }],
      env = db_password_env,
      config = {
        image = "quay.io/prometheuscommunity/postgres-exporter:v0.15.0",
        command = [
          "sh",
          "-c",
          m%"
            export DATA_SOURCE_NAME="postgresql://postgres:$POSTGRES_PASSWORD@$LEGION_UPSTREAM_POSTGRES/app?sslmode=disable"
            exec postgres_exporter --web.listen-address=":$LEGION_PORT_METRICS"
          "%,
        ],
      },
      network = { ports = [{ name = "metrics" }] },
      service = { name = "postgres-metrics", port = "metrics", meta = { protocol = "http", path = "/metrics" } },
      connect = { upstreams = [{ service = "postgres" }] },
      resources = { cpus = 0.05, memory = 32 },
    },
  ],
}
