# KV updates reaching a running workload, both ways Legion offers:
#
# - GREETING is an env var from `spot-check/greeting`. Env cannot change under
#   a running process, so a change restarts the task (new `started` time).
# - message.txt is a file from `spot-check/message`, rewritten in place on
#   change (`onChange = "update"`). The process keeps running and simply
#   reads the new file on the next request (same `started` time).
#
#   legion kv put spot-check/greeting "hello"
#   legion kv put spot-check/message "first message"
#   legion job run tests/spot-check/jobs/04-kv.ncl
#   curl http://<worker-public-ip>:8083/
#   legion kv put spot-check/message "second message"   # live, no restart
#   legion kv put spot-check/greeting "bonjour"         # restarts the task
#
# Any image with python3 works. On macOS, pass a locally loaded one:
#   --var image=spot-check-agent:dev
let params = { image | String | default = "public.ecr.aws/docker/library/python:3.12-alpine" } & vars in
{
  name = "kv-demo",
  task = {
    name = "web",
    runtime = "container",
    lifecycle = "service",
    count = 1,
    env = [
      {
        name = "GREETING",
        value = { template = "{{ kv \"spot-check/greeting\" }}" },
        kv = { watch = true, onChange = "restart" },
      },
    ],
    files = [
      {
        # Container files must live under /legion/task, which is mounted from
        # the host so in-place rewrites are visible inside the container.
        path = "/legion/task/message.txt",
        mode = "0644",
        file = { template = "{{ kv \"spot-check/message\" }}" },
        kv = { watch = true, onChange = "update" },
        changeMode = "noop",
      },
    ],
    config = {
      image = params.image,
      command = ["python3", "-c", "import os; exec(os.environ['APP_PY'])"],
      env = {
        PYTHONUNBUFFERED = "1",
        # m%%"..."%% because the code below contains `"%`, which would close m%"..."%.
        APP_PY = m%%"
import json, os, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STARTED = time.strftime("%H:%M:%S")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        with open("/legion/task/message.txt") as f:
            message = f.read()
        body = json.dumps({
            "greeting (env, restarts)": os.environ["GREETING"],
            "message (file, live)": message,
            "started": STARTED,
            "alloc": os.environ["LEGION_ALLOC_ID"],
        }, indent=2) + "\n"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

print(f"kv-demo up at {STARTED} with GREETING={os.environ['GREETING']!r}", flush=True)
ThreadingHTTPServer(("0.0.0.0", int(os.environ["LEGION_PORT_HTTP"])), Handler).serve_forever()
"%%,
      },
    },
    network = { ports = [{ name = "http", static = 8083 }] },
    checks = [{ type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" }],
    # Apple `container` (macOS) refuses VMs under 200 MiB.
    resources = { cpu = 100, memory = 256 },
  },
}
