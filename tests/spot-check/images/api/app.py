"""Notes API for the full-stack spot check.

Postgres holds the notes, and Redis caches the list for 10 seconds. The API
reaches both through the Legion mesh: it dials the loopback addresses in
LEGION_UPSTREAM_POSTGRES and LEGION_UPSTREAM_CACHE and never learns where the
backends actually run. Every response names the allocation that served it.
"""

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pg8000.native
import redis

ALLOC = os.environ.get("LEGION_ALLOC_ID", "?")


def connect_db():
    host, port = os.environ["LEGION_UPSTREAM_POSTGRES"].split(":")
    # Postgres may still be starting, so wait instead of crash-looping.
    while True:
        try:
            db = pg8000.native.Connection(
                "postgres", host=host, port=int(port), database="app",
                password=os.environ["POSTGRES_PASSWORD"],
            )
            db.run("CREATE TABLE IF NOT EXISTS notes (id serial PRIMARY KEY, body text NOT NULL, "
                   "created_at timestamptz DEFAULT now())")
            print("connected to postgres via the mesh", flush=True)
            return db
        except Exception as err:  # noqa: BLE001 - any failure means "not yet"
            print(f"waiting for postgres: {err}", flush=True)
            time.sleep(2)


cache_host, cache_port = os.environ["LEGION_UPSTREAM_CACHE"].split(":")
cache = redis.Redis(host=cache_host, port=int(cache_port), socket_timeout=2)
db = connect_db()


def list_notes():
    try:
        cached = cache.get("notes")
        if cached is not None:
            return json.loads(cached), "hit"
    except redis.RedisError as err:
        print(f"cache unavailable: {err}", flush=True)
    rows = db.run("SELECT id, body, created_at::text FROM notes ORDER BY id DESC LIMIT 50")
    notes = [{"id": r[0], "body": r[1], "at": r[2]} for r in rows]
    try:
        cache.set("notes", json.dumps(notes), ex=10)
    except redis.RedisError:
        pass
    return notes, "miss"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/healthz":
            return self.reply(200, {"ok": True})
        if self.path == "/api/notes":
            notes, cache_state = list_notes()
            return self.reply(200, {"notes": notes, "cache": cache_state, "served_by": ALLOC})
        self.reply(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/api/notes":
            return self.reply(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}").get("body", "").strip()
        if not body:
            return self.reply(400, {"error": "body is required"})
        [[note_id]] = db.run("INSERT INTO notes (body) VALUES (:body) RETURNING id", body=body[:500])
        try:
            cache.delete("notes")
        except redis.RedisError:
            pass
        self.reply(201, {"id": note_id, "served_by": ALLOC})

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} {fmt % args}", flush=True)


port = int(os.environ["LEGION_PORT_HTTP"])
print(f"api {ALLOC} listening on 0.0.0.0:{port}", flush=True)
ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
