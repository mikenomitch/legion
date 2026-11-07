#!/usr/bin/env python3
"""A Legion scaling strategy plugin: replicas ahead of known batch windows.

It speaks the `legion-scaling/1` protocol (docs/autoscaling-plugins.md):
JSON-RPC 2.0, one request per line on stdin, one response per line on
stdout. Run it with `--http PORT` to serve the same messages as HTTP POSTs
instead. Python 3.8+, standard library only.

Install it in the autoscaler's app config, then name it from a policy:

    plugins = { calendar = { command = ["python3", "calendar_strategy.py"],
                             config = { windows = [{ start = "02:00", end = "03:30" }] } } }

    checks = [{ name = "batch", source = "legion",
                strategy = { name = "calendar", replicas = 6, lead = "15m" },
                scaleIn = false }]

Inside a window (from `lead` before `start` until `end`, UTC) it wants
`replicas`; outside it wants 0, which a `scaleIn = false` check never
counts, so other checks decide. A window may cross midnight (`22:00` to
`02:00`), and one that ends where it starts lasts all day.
"""

import datetime
import json
import os
import sys

PROTOCOL = "legion-scaling/1"
SCHEMA = {
    "type": "object",
    "properties": {
        "replicas": {"type": "integer", "minimum": 0},
        "lead": {"type": "string", "pattern": "^[0-9]+[smh]$"},
    },
    "required": ["replicas"],
    "additionalProperties": False,
}

windows = []


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def minutes(clock):
    hours, mins = clock.split(":")
    return int(hours) * 60 + int(mins)


def seconds(duration):
    unit = {"s": 1, "m": 60, "h": 3600}[duration[-1]]
    return int(duration[:-1]) * unit


def now():
    # CALENDAR_NOW (an ISO time) pins the clock, for tests.
    pinned = os.environ.get("CALENDAR_NOW")
    if pinned:
        return datetime.datetime.fromisoformat(pinned)
    return datetime.datetime.now(datetime.timezone.utc)


def initialize(params):
    global windows
    if params.get("protocol") != PROTOCOL:
        raise RpcError(-32602, "unsupported protocol %r" % params.get("protocol"))
    config = params.get("config") or {}
    windows = [(minutes(w["start"]), minutes(w["end"])) for w in config.get("windows", [])]
    return {
        "name": "calendar",
        "version": "0.1.0",
        "kinds": ["strategy"],
        "capabilities": {},
        "policyConfigSchema": SCHEMA,
    }


def recommend(params):
    config = params.get("config")
    current = params.get("current")
    if not isinstance(config, dict) or not isinstance(current, int):
        raise RpcError(-32602, "strategy/recommend needs config (a record) and current (a count)")
    replicas = config.get("replicas")
    if not isinstance(replicas, int):
        raise RpcError(-32602, "config.replicas must be a count")
    at = now()
    minute = at.hour * 60 + at.minute
    lead = seconds(config.get("lead", "0s")) // 60
    for start, end in windows:
        opens = start - lead
        # Minutes since the window (with its lead) opened, modulo a day, so a
        # window may cross midnight; a window that ends where it starts lasts
        # all day.
        span = (end - opens) % 1440 or 1440
        if (minute - opens) % 1440 < span:
            return {"desired": replicas, "note": "batch window %02d:%02d" % divmod(start, 60)}
    return {"desired": 0, "note": "no batch window"}


METHODS = {"initialize": initialize, "strategy/recommend": recommend}


def handle(line):
    try:
        request = json.loads(line)
    except ValueError:
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}
    rid = request.get("id") if isinstance(request, dict) else None
    try:
        if not isinstance(request, dict) or not isinstance(request.get("method"), str):
            raise RpcError(-32600, "invalid request")
        method = METHODS.get(request["method"])
        if method is None:
            raise RpcError(-32601, "method not found: %s" % request["method"])
        params = request.get("params")
        if not isinstance(params, dict):
            raise RpcError(-32602, "params must be an object")
        return {"jsonrpc": "2.0", "id": rid, "result": method(params)}
    except RpcError as err:
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": err.code, "message": str(err)}}
    except Exception as err:  # a bug here must not kill the plugin
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32603, "message": repr(err)}}


def serve_stdio():
    for line in sys.stdin:
        if line.strip():
            sys.stdout.write(json.dumps(handle(line)) + "\n")
            sys.stdout.flush()


def serve_http(port):
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            reply = json.dumps(handle(body.decode())).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)

        def log_message(self, *args):
            pass

    HTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--http":
        serve_http(int(sys.argv[2]))
    else:
        serve_stdio()
