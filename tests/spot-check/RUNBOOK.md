# Spot-check runbook

A hands-on pass through Legion's headline features on the AWS rig and on
your Mac, before sharing Legion externally. Plan on about two hours. Work top
to bottom, tick the checklist at the end, and note anything that surprises
you. "Surprising" is the signal this pass exists to catch.

An agent already ran this once. What it found, with root causes, is in
[launch-spot-check-findings.md](../../internal_docs/launch-spot-check-findings.md).
Each step below says what good looks like and which known issue you will hit
along the way, so you can tell a new problem from a known one.

Commands are fish. Run them from the repo root on the `spot-check/launch`
branch.

## 0. Setup

You need the `legion-bench` AWS profile, OrbStack (or Docker Desktop), Rust,
and Apple `container` (`brew install container`).

**If the rig is down,** bring it up. The Linux build takes about an hour under
Rosetta, and the README has the command. Apply `shims.patch` before that build
while the fixes it stands in for are missing, and build the macOS CLI from the
same tree for section 10.

```fish
tests/spot-check/infra/up.sh
tests/spot-check/images/build-push.sh
tests/spot-check/infra/install.sh
```

**Point your CLI at the cluster.** The repo's `mise.toml` sets `LEGION_ADDR`
to localhost, so a bare `legion` talks to your Mac. Either load the rig's env
into this shell, or prefix commands with the `lg` wrapper, which always
targets the rig's current leader.

```fish
cargo build --bin legion && set -gx PATH (pwd)/target/debug $PATH   # this shell only
tests/spot-check/infra/cli-env.sh --fish | source
alias lg tests/spot-check/infra/lg          # optional
tests/spot-check/infra/ssh.sh sc-wkr-1       # a shell on a box, when you need one
```

Worker public IPs are in `tests/spot-check/.state/hosts`. App ports only
accept your IP as of `up.sh`. If you change networks, re-run `up.sh` (it is
idempotent and adds the new IP).

```fish
set w1 (awk '$1=="sc-wkr-1"{print $2}' tests/spot-check/.state/hosts)
set w2 (awk '$1=="sc-wkr-2"{print $2}' tests/spot-check/.state/hosts)
```

## 1. The cluster

```fish
legion raft status          # 3 voters, quorum 2
legion node list            # sc-wkr-1 and sc-wkr-2 active
legion volume drivers       # host and local-dir usable on 2/2
legion acl whoami           # the management token
```

**Good:** 3 voters, 2 active workers, volume drivers `2/2`.
**Known:** if `node list` says active but jobs fail with `No nodes available`,
you are in findings P0-6. Run `legion node uncordon sc-wkr-1` and the same
for `sc-wkr-2`.

## 2. Basic container, exec, and sandbox over HTTP

```fish
legion job run tests/spot-check/jobs/01-container.ncl
legion job run tests/spot-check/jobs/02-exec.ncl
legion job run tests/spot-check/jobs/03-sandbox.ncl
curl http://$w1:8081/ ; curl http://$w2:8081/   # container, from private ECR
curl http://$w1:8082/ ; curl http://$w2:8082/   # host process
curl http://$w1:8090/ ; curl http://$w2:8090/   # microVM: one of the two answers
```

**Good:** each container response names its allocation. The exec page shows
the host kernel. The microVM answers "from a Legion microVM" on whichever
worker it landed on, and its log shows a different, guest kernel.
**Look at:** `legion service resolve web` returns private IPs, not
`127.0.0.1` (the shim at work).
**Known:** a microVM is only reachable because the rig hand-builds a TAP
device and port forward (`infra/cloud-init/worker.sh`). Legion does not do
that part.

## 3. Logs and exec on all three

```fish
legion job logs hello-container -f          # curl again in another tab; lines appear
legion exec --job hello-container -it -- /bin/sh
legion exec --job hello-exec -it -- /bin/bash   # this is a shell ON THE HOST
legion action run hello-container env
legion job logs hello-vm                    # the guest's serial console
```

**Good:** live log lines, interactive shells with a working prompt and
resize, the action prints `LEGION_*` vars.
**Known:**
- `legion exec` into the microVM fails, because exec into microVMs isn't
  built yet. The error text is noisy (findings P2-2).
- The exec-task shell lacks the task's env (P2-8).
- `cat /proc/self/cgroup` in the exec task shows limits are not enforced
  (P1-1).

## 4. A somewhat complex DAG

```fish
legion job run tests/spot-check/jobs/05-dag.ncl --var run=human1
legion job logs airports-dag -f
```

Then open `http://<its worker>:8085/report.html` (the `serve` task's node).

**Good, by design:** `fetch` → `validate` → four parallel steps (`flaky` fails
once and retries) → `report` → `serve`, each step starting after the one
before it succeeds.
**Known, expect this to look wrong:** every step starts at once and
crash-loops until its inputs exist (P0-4), and finished batch steps restart
every 30 seconds forever (P0-2). Count `[fetch] fetched` lines in the logs.
Stop the job when you are done looking: `legion job stop airports-dag`.

## 5. A full-stack app on the service mesh

```fish
legion kv put fullstack/db-password (openssl rand -hex 16)
legion job run tests/spot-check/jobs/06-fullstack.ncl
open http://$w1:8080/ ; open http://$w2:8080/
legion service resolve api
legion job scale fullstack --task api --count 4
```

**Good:**
- Both frontends load.
- A note added on one shows up on the other.
- The `served by` line changes as you reload (HTTP-mode balancing), and
  `cache hit` or `cache miss` reflects the Redis cache.
- After scaling, the new replicas start answering without a redeploy.

**Known:** the stock nginx and Postgres images do not run in Legion's Linux
containers, so this uses rebuilt ones (P0-7). If nothing answers after a
couple of minutes, check `legion service doctor api`. "health checks
starting" forever means the P0-3 shim is missing from the binary.

## 6. Sandboxed agents, with the launcher

```fish
legion job run tests/spot-check/jobs/07-agents.ncl
tests/spot-check/launcher/run-rig.sh         # then open http://localhost:8089
```

In the launcher:

1. Launch a **box** sandbox. It appears with an allocation within about a
   minute.
2. Click **Terminal** and run `curl -sI https://pypi.org/simple/ | head -1`
   (allowed) and `curl -sI https://example.com/` (denied by the egress proxy
   with a 403). Then run `ls /workspace; tail journal.md`.
3. Click **Stop**, then **Start**. The terminal comes back with
   `/workspace/journal.md` intact and `.boots` incremented.
4. Launch a **vm** sandbox. **Logs** shows the agent booting inside a
   microVM. **Terminal** shows Legion's "does not support exec" error, which
   is the known gap.
5. **Destroy** both. It needs a second click within 5 seconds.

**Known:** a box restart occasionally fails on a network-namespace race and
then gets stuck "rescheduling" (P1-4 then P1-3).

## 7. KV updates reaching a workload

```fish
legion kv put spot-check/greeting hello
legion kv put spot-check/message "first message"
legion job run tests/spot-check/jobs/04-kv.ncl
curl http://$w1:8083/ ; curl http://$w2:8083/   # one answers
legion kv put spot-check/message "changed live"  # file: same `started`
legion kv put spot-check/greeting bonjour        # env: new `started`
```

**Good:** the message changes within a few seconds and `started` stays put.
The greeting change gives a new `started` time.
**Known:** if you edit the spec and re-run it, the update rollout can wedge on
the static port and take the service down (P1-2). `legion job stop kv-demo`,
then `job run` again.

## 8. Stateful Redis on AWS storage

```fish
legion job run tests/spot-check/jobs/08-redis.ncl
legion action run redis cli-cmd --param cmd="SET greeting persisted"
legion job run tests/spot-check/jobs/08-redis.ncl --force-reschedule
legion action run redis cli-cmd --param cmd="GET greeting"     # persisted
legion volume list --job redis
legion action run redis cli --tty                              # interactive redis-cli
```

Then the harder one: reboot the worker that holds it (`legion job logs
redis` names the node), and wait for it to come back.

```fish
tests/spot-check/infra/ssh.sh sc-wkr-1 sudo reboot
legion node list                                               # lost, then active
legion action run redis cli-cmd --param cmd="GET greeting"
```

**Good:** the value survives the reschedule and the reboot. The data lives in
`/data/redis/appendonlydir` on the EBS disk.
**Watch for:** after the reboot, whether the worker rejoins on its own,
whether Redis comes back without help (P1-3 or P1-5 may bite), and how long
it takes.

## 9. Failure drills (optional, and they break things)

**Leader failover.** Find the leader, stop it, and confirm that reads, exec,
and writes keep working against the new one.

```fish
tests/spot-check/infra/leader.sh
tests/spot-check/infra/ssh.sh sc-mgr-2 sudo systemctl stop legion-manager   # if mgr-2 leads
tests/spot-check/infra/cli-env.sh --fish | source                           # follow the new leader
legion job logs hello-container ; legion exec --job hello-container -- true
legion job scale hello-exec --task web --count 2
tests/spot-check/infra/ssh.sh sc-mgr-2 sudo systemctl start legion-manager
```

**Known, and the most important thing to see for yourself (P0-5):** stopping
or demoting **sc-mgr-1** strands both workers, because their join tokens
point at it and followers cannot forward heartbeats. To recover, make
sc-mgr-1 the leader again:

```fish
tests/spot-check/infra/on-manager.sh sc-mgr-1 raft command trigger-election
legion node uncordon sc-wkr-1 ; legion node uncordon sc-wkr-2               # P0-6
```

**Worker restart.** `tests/spot-check/infra/ssh.sh sc-wkr-2 sudo systemctl
restart legion-worker`, then check what happened to its tasks
(`legion node list`, the HTTP endpoints). Watch for orphaned processes
(P1-5).

## 10. Local development on macOS

Use a separate data dir so nothing collides with your usual local manager,
and check nothing already holds 4747 (`lsof -nP -iTCP:4747`).

```fish
container system start
tests/spot-check/mac/build-local.sh                 # spot-check-web:dev and spot-check-agent:dev
env LEGION_DATA_DIR=/tmp/legion-sc legion start manager --run-worker -c tests/spot-check/mac/manager.ncl
```

In another tab (with `LEGION_ADDR=http://127.0.0.1:4747`, which mise sets):

```fish
legion job run tests/spot-check/jobs/02-exec.ncl --var-code count=1
legion job run tests/spot-check/jobs/mac/mac-container.ncl
curl http://localhost:18081/
legion exec --job mac-container -it -- /bin/sh
legion kv put spot-check/greeting hi ; legion kv put spot-check/message local
legion job run tests/spot-check/jobs/04-kv.ncl --var image=spot-check-agent:dev
curl http://localhost:8083/ ; legion kv put spot-check/message changed ; curl http://localhost:8083/
legion job run tests/spot-check/jobs/mac/mac-agents.ncl
env LAUNCHER_JOB=mac-agents python3 tests/spot-check/launcher/launcher.py   # http://localhost:8088
```

The dev loop to try: edit `images/web/app.py`, rerun `mac/build-local.sh
web`, then `legion job run tests/spot-check/jobs/mac/mac-container.ncl
--force-reschedule`, and curl again.

**Good:** everything above works, including a launcher terminal into a
macOS sandbox VM.

**Known:**
- Run `02-exec.ncl` without `count=1` once to see P1-2: two replicas on one
  static port wedge the scheduler, and the job says `done` with nothing
  running. `legion job scale hello-exec --task web --count 1` recovers it.
- Apple `container` needs at least 200 MiB per task (P2-6).
- Also try the README quickstart in an empty directory outside the repo.
  It fails today (P2-1).

## 11. Optional

- **UI:** `cd ui && npm install && env VITE_MANAGER_ENDPOINT=http://<leader-ip>:4747 npm run dev`,
  then paste the management token (`tests/spot-check/.state/token`) on the
  Access page.
- **MCP:** `claude mcp add legion-sc --env LEGION_ADDR=http://<leader-ip>:4747 --env LEGION_TOKEN=<token> -- (pwd)/target/debug/legion mcp serve --allow-write`,
  then ask an agent to deploy and debug one of these jobs.

## Unattended rerun

`tests/spot-check/smoke.sh` runs sections 1-8 and the failover drill and
prints PASS or FAIL per check. On 2026-10-11 the failures it reports were all
real (P0-2, P0-4, P1-1, P1-2) once the harness was fixed.

## Teardown

```fish
tests/spot-check/infra/down.sh       # instances, volumes, groups, bucket, ECR repos
container system stop                # if you started it for section 10
```

## Checklist

| # | Area | Works? | Notes |
| --- | --- | --- | --- |
| 1 | Cluster formation, raft, ACLs | | |
| 2 | Container / exec / microVM over HTTP | | |
| 3 | Logs and exec on all three | | |
| 4 | DAG | | |
| 5 | Full stack on the mesh | | |
| 6 | Agent sandboxes and launcher | | |
| 7 | KV updates | | |
| 8 | Stateful Redis, reschedule and reboot | | |
| 9 | Failover drills | | |
| 10 | macOS local development | | |
