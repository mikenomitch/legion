#!/usr/bin/env python3
"""Agent sandbox launcher for the Legion spot check.

A small local web app: launch a sandbox, watch it come up, open a terminal in
it, read its logs, stop, start, or destroy it. Each sandbox is a keyed
instance of the `agents` job (jobs/07-agents.ncl).

- Lifecycle calls shell out to the `legion` CLI with --json, so this file
  needs nothing beyond the standard library.
- The terminal does not go through this server. The browser speaks Legion's
  exec WebSocket bridge (ws://<manager>:<port+1>/v1/exec/ws) directly, the
  same path the Legion UI uses. That works from http://localhost because the
  manager always allows loopback origins.

    legion job run tests/spot-check/jobs/07-agents.ncl
    LEGION_ADDR=... LEGION_TOKEN=... python3 tests/spot-check/launcher/launcher.py
    open http://localhost:8088

This is a local test tool. It hands your token to your own browser so the page
can open exec sessions, so do not expose it beyond localhost.
"""

import json
import os
import pathlib
import subprocess
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

JOB = os.environ.get("LAUNCHER_JOB", "agents")
LEGION = os.environ.get("LEGION_BIN", "legion")
ADDR = os.environ.get("LEGION_ADDR", "http://127.0.0.1:4747")
PORT = int(os.environ.get("LAUNCHER_PORT", "8088"))
INDEX = (pathlib.Path(__file__).parent / "index.html").read_bytes()


def legion(*args):
    """Runs a legion CLI command with --json and returns (ok, data_or_error)."""
    proc = subprocess.run([LEGION, *args, "-j"], capture_output=True, text=True, timeout=60)
    try:
        envelope = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return False, {"message": (proc.stderr or proc.stdout).strip()[-2000:] or f"exit {proc.returncode}"}
    if envelope.get("ok"):
        return True, envelope.get("data")
    return False, envelope.get("error") or envelope


def ws_url():
    # The exec bridge listens on the manager's gRPC port + 1.
    parsed = urllib.parse.urlparse(ADDR)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.hostname}:{(parsed.port or 4747) + 1}/v1/exec/ws"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/":
            return self.send(200, INDEX, "text/html; charset=utf-8")
        if path == "/api/config":
            return self.json(200, {"job": JOB, "manager": ADDR, "wsUrl": ws_url(), "token": os.environ.get("LEGION_TOKEN", "")})
        if path == "/api/sandboxes":
            ok, data = legion("instance", "list", JOB)
            return self.result(ok, (data or {}).get("instances", []) if ok else data)
        parts = path.strip("/").split("/")
        if len(parts) == 3 and parts[:2] == ["api", "sandboxes"]:
            return self.result(*legion("instance", "get", parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "sandboxes"] and parts[3] == "logs":
            ok, data = legion("instance", "get", parts[2])
            alloc = ((data or {}).get("instance") or {}).get("current_alloc_id") if ok else None
            if not alloc:
                return self.json(409, {"message": "sandbox has no running allocation yet"})
            return self.result(*legion("job", "logs", JOB, "--allocation", str(alloc), "--tail", "--max-bytes", "20000"))
        self.json(404, {"message": "not found"})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        body = self.read_json()
        if path == "/api/sandboxes":
            args = ["instance", "create", JOB, "--task", body.get("flavor", "box")]
            if body.get("id"):
                args += ["--id", body["id"]]
            return self.result(*legion(*args))
        parts = path.strip("/").split("/")
        if len(parts) == 4 and parts[:2] == ["api", "sandboxes"] and parts[3] in ("stop", "start"):
            return self.result(*legion("instance", parts[3], parts[2]))
        self.json(404, {"message": "not found"})

    def do_DELETE(self):
        parts = urllib.parse.urlparse(self.path).path.strip("/").split("/")
        if len(parts) == 3 and parts[:2] == ["api", "sandboxes"]:
            return self.result(*legion("instance", "destroy", parts[2]))
        self.json(404, {"message": "not found"})

    def read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length) or b"{}")

    def result(self, ok, data):
        self.json(200 if ok else 502, data if ok else {"message": (data or {}).get("message", str(data)), "error": data})

    def json(self, status, body):
        self.send(status, json.dumps(body).encode(), "application/json")

    def send(self, status, data, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print(f"Launcher for job '{JOB}' via {ADDR}: http://localhost:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
