# A stand-in for an interactive sandbox session: a process with some loaded
# state that changes a little at a time, a log, files written to disk, and
# short-lived subprocesses. Active for ACTIVE seconds, then idle.
import os, random, subprocess, time, json, sqlite3, urllib.request
ACTIVE = 160
start = time.time()
state = bytearray(os.urandom(48 * 1024 * 1024))
db = sqlite3.connect("/data/app.db")
db.execute("create table if not exists kv (k text primary key, v text)")
log = open("/var/log/session.log", "a")
i = 0
while True:
    now = time.time() - start
    if now > ACTIVE:
        time.sleep(1)
        continue
    i += 1
    for _ in range(16):
        off = random.randrange(0, len(state) - 4096)
        state[off:off + 4096] = os.urandom(4096)
    log.write(json.dumps({"i": i, "t": now}) + "\n"); log.flush()
    db.execute("insert or replace into kv values (?, ?)", (str(i % 500), "x" * 200)); db.commit()
    if i % 10 == 0:
        with open(f"/data/out-{i % 50}.bin", "wb") as f:
            f.write(os.urandom(256 * 1024)); f.flush(); os.fsync(f.fileno())
        subprocess.run(["ls", "-la", "/usr/lib"], capture_output=True)
        try:
            urllib.request.urlopen("http://127.0.0.1:8000/", timeout=2).read()
        except Exception:
            pass
    time.sleep(1)
