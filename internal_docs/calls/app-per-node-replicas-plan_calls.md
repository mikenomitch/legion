# Judgement calls: per-node replicas for apps

Calls made while building
[app-per-node-replicas-plan.md](../plans/app-per-node-replicas-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. The ingress runs one per node wherever it is placed, not on every node of the cluster

```nickel
# crates/common/src/bundled_apps/ingress.ncl
replicas = "per-node",
```

On managers that is one per manager. With `legion app set ingress
--placement workers --pool edge`, it is one per edge node, and a new edge
node gets one.

- **Alternatives.**
  - (a) Every node of the cluster by default: `placement = "any"` with
    per-node, the bare-metal DaemonSet shape.
  - (b) Keep a fixed count (1) and document `--replicas`.
- **Trade-off.** A cluster that wants an ingress on every worker sets it
  explicitly (`--placement any`, or `workers` on the default pool), and the
  docs warn about doing so. Per-node on managers costs each manager's
  system-worker lend the ingress's 500 MHz and 256 MiB, not just one
  manager's.
- **Why (ease, performance, safety).** "How many?" has one right answer for
  a front door: one on each node DNS or a load balancer points at. Per-node
  gives high availability and gap-free upgrades on three managers with no
  setting, and capacity that follows an edge pool. (a) was rejected on four
  counts:
  - every node it runs on holds the ingress's token and every site's TLS
    key, and tenants can run exec as root on shared workers;
  - it takes ports 80 and 443 from every tenant job in the pool;
  - each replica polls the catalog for every routed service every 2
    seconds, which is about 2,500 RPCs a second at 100 nodes and 50
    services;
  - DNS round-robin does not scale to every node anyway, and most workers
    have no public address.

  (b) leaves the single point of failure as the default.

## 2. `replicas` takes a number or `"per-node"`; the API adds `per_node` beside `replicas`

```console
$ legion app set ingress --replicas per-node
$ legion app set ingress --replicas 2
```

```text
Error: replicas: "two" is not a number or `per-node`
```

- **Alternatives.**
  - (a) A separate boolean everywhere (`perNode = true`, `--per-node`),
    with `replicas` ignored while it is set.
  - (b) Pass `count` through as the job spells it (`replicas = { num = 1,
    per = "id" }`), so any attribute could be the group.
  - (c) Reshape the proto's `replicas` into a oneof or a string.
- **Trade-off.** One field holds two kinds of value, so code reading it
  matches on a small enum instead of reading a number. On the wire the
  two fields can disagree; the server refuses a request that sets both,
  and reports `replicas = 0` with `per_node`.
- **Why (simplicity).** One question, "how many replicas?", with one place
  to answer it in every surface a person types. (a) gives two fields that
  can contradict each other in the manifest and config. (b) exposes
  scheduler vocabulary nobody has asked for; `per = "id"` is the only
  grouping an app needs. (c) breaks the wire format for no gain, and CI's
  `buf breaking` check guards it; an added field is compatible.

## 3. A per-node app's desired count is what the scheduler placed

`legion app status` shows `2/2` for a per-node app with two placed
allocations, both healthy.

- **Alternatives.** Count the eligible nodes in the reconciler (pool,
  constraints, node namespaces, build marker, ready state) and compare.
- **Trade-off.** A node the app should run on but where placement failed
  (port 80 taken by another job, no room in the lend) is not in the
  count, so the app can read `running` while one node lacks it. The job's
  blocked state (`legion job info legion-system/<app>`) says why.
- **Why (simplicity, correctness).** The scheduler is the one place that
  decides eligibility. A second implementation in the reconciler would
  drift from it and disagree in exactly the edge cases that matter. The
  latest version's live allocations are what the scheduler intends to run.

## 4. No cap on per-node replicas

- **Alternatives.** Apply the fixed-count cap (32) to per-node too.
- **Trade-off.** Per-node on a large shared pool runs as many copies as
  there are nodes, with the polling load in entry 1.
- **Why (simplicity).** The cap exists so a typo (`--replicas 300`) cannot
  flood the managers. With per-node, the pool bounds the count, and an
  edge pool is small by construction. A cap would make a 40-node edge pool
  quietly short.

## 5. The seed config gains `pool`

```nickel
apps = { ingress = { enabled = true, placement = "workers", pool = "edge" } },
```

- **Alternatives.** Leave seeding without a pool, and move the app with
  `legion app set --pool` after the first leader seeds it.
- **Trade-off.** One more seed field, read only on first seed, like the
  others.
- **Why (ease).** Per-node makes "the ingress on my edge pool" the shape
  most real clusters want. A new cluster should be able to start that way
  from its config, with no imperative step afterwards.
