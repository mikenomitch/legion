"""A tiny HTTP service that reports where it is running.

Each response names the allocation, job, and task that served it, so repeated
requests show load balancing and reschedules. Every request is logged to
stdout, which is what `legion job logs` should show.
"""

import json
import os
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STARTED = time.time()
hits = 0


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global hits
        if self.path == "/healthz":
            return self.reply(200, {"ok": True})
        hits += 1
        self.reply(
            200,
            {
                "hello": os.environ.get("GREETING", "from a Legion container"),
                "hostname": socket.gethostname(),
                "job": os.environ.get("LEGION_JOB_ID"),
                "task": os.environ.get("LEGION_TASK_NAME"),
                "alloc": os.environ.get("LEGION_ALLOC_ID"),
                "hits": hits,
                "uptime_s": round(time.time() - STARTED, 1),
            },
        )

    def reply(self, status, body):
        data = (json.dumps(body, indent=2) + "\n").encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} {fmt % args}", flush=True)


# LEGION_PORT_TO_HTTP is the in-container port when a job maps one (`to`),
# which Apple `container` publishes on macOS. Linux containers share the host
# network and listen on LEGION_PORT_HTTP directly.
port = int(
    os.environ.get("LEGION_PORT_TO_HTTP")
    or os.environ.get("LEGION_PORT_HTTP")
    or os.environ.get("PORT", "8080")
)
print(f"web listening on 0.0.0.0:{port}", flush=True)
ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
