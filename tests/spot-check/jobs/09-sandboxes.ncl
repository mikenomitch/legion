# Sandboxes: keyed cloud-hypervisor microVMs on gated networking, for the
# RUNBOOK's section 12 and smoke.sh's sandbox-* scenarios. Submitting the job
# starts nothing: each sandbox is a keyed instance created by id.
#
#   legion kv put spot-check/echo-key sc-secret-123      # the injected header
#   legion job run tests/spot-check/jobs/09-sandboxes.ncl
#   legion instance create sandboxes --task ws --id sbx-1
#   legion exec --instance sbx-1 -it -- /bin/bash        # through the guest agent
#   legion instance suspend sbx-1 --wait -j              # memory checkpoint
#   legion instance start sbx-1 --wait -j                # restore in place
#   legion instance destroy sbx-1
#
# Two tasks, because they are checked differently:
#
# - `ws`: suspended and resumed by hand. Gated (no NIC: its only way out is
#   the node datapath over vsock), with an egress allow list, a host-guard
#   probe, an external upstream whose API key the node injects, an advertised
#   port, and `onPressure` so the memory-pressure drill suspends it instead
#   of evicting anything.
# - `idler`: suspends itself after 30 s of quiet, and `legion start gateway`
#   wakes it on a request.
#
# Needs, on the workers: `legion-guest` beside the worker binary (the agent
# image is musl, so the worker cannot copy itself in; RUNBOOK 12.0), and
# resourceEnforcement (already on in the rig's worker config) for the idle
# policy. The guest serves one JSON line on $LEGION_PORT_HTTP:
#
#   {"instance": ..., "alloc": ..., "process": <random per process start>,
#    "hits": <requests since the process started>, "boots": <cold boots>}
#
# `process` and `hits` live in guest memory, so they survive a memory
# restore and reset on a cold boot. `boots` counts lines in
# /workspace/boots, so it survives both and grows only on a cold boot.
let images = (import "lib/images.ncl") vars in
let serve = m%"
  echo "sandbox $LEGION_INSTANCE_ID booted: guest kernel $(uname -r)"
  mkdir -p /workspace && date +%s >> /workspace/boots
  cat > /tmp/serve.py <<'EOF'
  import http.server, json, os, uuid
  PROCESS = uuid.uuid4().hex[:12]
  HITS = [0]
  class Handler(http.server.BaseHTTPRequestHandler):
      def do_GET(self):
          HITS[0] += 1
          try:
              boots = sum(1 for _ in open("/workspace/boots"))
          except OSError:
              boots = 0
          body = json.dumps({
              "instance": os.environ.get("LEGION_INSTANCE_ID", ""),
              "alloc": os.environ.get("LEGION_ALLOC_ID", ""),
              "process": PROCESS,
              "hits": HITS[0],
              "boots": boots,
          }).encode() + b"\n"
          self.send_response(200)
          self.send_header("Content-Type", "application/json")
          self.send_header("Content-Length", str(len(body)))
          self.end_headers()
          self.wfile.write(body)
      def log_message(self, *args):
          pass
  port = int(os.environ.get("LEGION_PORT_HTTP", "8080"))
  print("serving on", port, "process", PROCESS, flush=True)
  http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
  EOF
  exec python3 -u /tmp/serve.py
"%
in
{
  name = "sandboxes",
  tasks = [
    {
      name = "ws",
      group = "ws",
      runtime = "sandbox",
      stateful = {
        mode = "keyed",
        # A block volume on the node (local-dir): survives stop/start and
        # suspend/resume, pins the instance to its node, reclaimed on destroy.
        storage = { path = "/workspace", mode = "block", params = { size = "1G" } },
      },
      config = {
        image = images.agent,
        command = ["/bin/sh", "-c", serve],
        vcpus = 1,
        hypervisor = "cloud-hypervisor",
      },
      # The guest boots with 512 MiB and the scheduler reserves 512 MiB.
      resources = { cpu = 500, memory = 512 },
      network = {
        mode = "gated",
        ports = [{ name = "http" }],
        labels = { tier = "spot-check" },
        egress = {
          protocol = "http",
          policy = "deny",
          # 169.254.169.254 is allowed here on purpose: the host guard sits
          # below policy, so the metadata service must still be refused, and
          # the denial must name `host-guard`, not the policy.
          allow = ["pypi.org", "files.pythonhosted.org", "httpbin.org", "169.254.169.254"],
          accessLog = true,
        },
      },
      connect = {
        upstreams = [
          {
            # In the guest: curl http://$LEGION_UPSTREAM_ECHO/headers
            # The node opens TLS to httpbin.org and sets the header from KV
            # on the way out. The key is never in the guest's env, files, or
            # memory checkpoints.
            name = "echo",
            to = "https://httpbin.org",
            credentials = [
              { header = "x-spot-check-key", from = { kv = "spot-check/echo-key" } },
            ],
          },
        ],
      },
      suspend = {
        checkpoint = "memory",
        onRestoreFailure = "cold",
        # Proves the resume handler ran: the file appears only after a restore.
        onResume = ["/bin/sh", "-c", "date +%s > /tmp/resumed-at"],
        # The memory-pressure drill (RUNBOOK 12.8) suspends this sandbox
        # before the node evicts anything.
        onPressure = true,
      },
      constraints = { os = "linux" },
    },
    {
      name = "idler",
      group = "idler",
      runtime = "sandbox",
      stateful = {
        mode = "keyed",
        storage = { path = "/workspace", mode = "block", params = { size = "1G" } },
      },
      config = {
        image = images.agent,
        command = ["/bin/sh", "-c", serve],
        vcpus = 1,
        hypervisor = "cloud-hypervisor",
      },
      resources = { cpu = 500, memory = 512 },
      network = {
        mode = "gated",
        ports = [{ name = "http" }],
        egress = { allow = ["pypi.org"] },
      },
      suspend = {
        checkpoint = "memory",
        idle = {
          # The shortest window the policy allows, so the check runs in about
          # a minute: suspended within `after` plus about 30 s.
          after = "30s",
          signals = ["network", "cpu", "exec"],
          # A nested-virtualization VMM ticks over at a few percent of a core
          # even when the guest is idle. 20% of the task's 500 MHz keeps that
          # noise from holding the sandbox awake forever.
          cpuBelow = 0.2,
        },
        # Requests through `legion start gateway` for this instance go to
        # port http, and a request for a suspended instance resumes it.
        wake = { port = "http", timeout = "60s" },
      },
      constraints = { os = "linux" },
    },
  ],
}
