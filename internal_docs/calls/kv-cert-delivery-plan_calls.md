# Judgement Calls: Live KV File Updates and KV-Backed Ingress Certificates

Plan: [`plans/kv-cert-delivery-plan.md`](../plans/kv-cert-delivery-plan.md).
Entries are ranked by project impact, highest first. The number is the rank.

## 1. The worker names allocations, never KV paths

**Call.** `GetIngressCertificates` takes `{ node_id, known: [{ allocation_id, version }] }`.
For each allocation the manager checks placement on the calling node,
recomputes the effective ingress chain, and reads the KV path from it. Naming
a path in `certFrom = { kv }` needs `kv:read` on it at `RunJob`, template runs,
and `UpsertNetPolicy`.

```text
PermissionDenied: ... (ingress tls.certFrom names kv certs/edge; the submitter needs kv read on it)
```

**Alternatives.** (a) Copy `GetArtifactCredential`: the worker sends the
path, and the manager serves any path to any authenticated worker. (b) Ship
the bundle in the heartbeat or in the policy body.

**Trade-off.** Every call rebuilds the node's desired allocations and their
merged policies on the manager, about one extra heartbeat's worth of work per
heartbeat, and only for nodes with KV-backed chains. A policy object written
by someone with `kv:read` can still put that key on every allocation its
selector matches. That is the policy author's grant, not a leak.

**Why (simplicity, security boundary).** Option (a) is the open "KV ACL
caveat" in `docs/artifacts.md`. Any node, or anyone with job submit, could
read any private key. Option (b) writes the key into `netd_state.json` in
plaintext, into the manager's policy cache, and into `net policy show`
output. Deriving the path keeps the rule explainable: a node reads only what
a job on it declares, and a job declares only what its author could read.

## 2. `update` is a new `onChange` action, not something `changeMode` implies

**Call.** A file opts in with `kv = { onChange = "update" }`. `changeMode`,
`changeSignal`, and `splay` only describe what the task gets afterwards, and
are rejected without `update`.

```nickel
kv = { watch = true, onChange = "update" },
changeMode = "signal", changeSignal = "SIGHUP",
```

**Alternatives.** (a) Nomad's shape: any `changeMode` on a file implies a live
rewrite, and `onChange` stays `none | restart | rollout | fail`. (b) Change
`onChange = "none"` to rewrite files live.

**Trade-off.** Two fields to set where Nomad has one. Strength order gains a
rung (`none < update < restart < rollout < fail`).

**Why (simplicity).** `onChange` already answers "what does the cluster do
when this key changes", with a strongest-wins merge across edges. A live
rewrite is one more answer to that question, and it has to compete in the
same merge. A file with `update` and an env entry with `restart` changing
together must restart, not rewrite. Making `changeMode` imply behavior would
create a second, hidden source for that answer. Option (b) silently changes
what existing jobs do.

## 3. `update` repoints allocations synchronously, not through an eval

**Call.** `reconcile_kv_changes` calls `migrate_allocations_to_job_config`
directly when the old and new configs are schedule-equivalent. If they are
not, it falls back to a rollout.

**Alternatives.** Enqueue a `job_submit` eval for the new config, whose handler
already migrates schedule-equivalent configs. That keeps all allocation
writes in the eval loop.

**Trade-off.** One more place that writes allocations outside an eval. It is
a replicated Scheduler-lane op, so lane ownership is the same.

**Why (correctness).** Until the allocations move, the job matches
`jobs_with_unrolled_latest_config`: a newest config with no allocations and
no rollout. `reconcile_missing_rollouts` repairs that by creating a rollout,
so an eval that has not run yet races a pass that turns the update into a
rollout. A leader crash between the two would lose the update entirely.

## 4. Only marked files are rewritten live

**Call.** The manager adds a `liveUpdate` object to a rendered `config.files`
entry only when its edge resolves to `update`. The worker rewrites only
entries that carry it. Any other difference waits for the next start.

**Alternatives.** Rewrite any file whose content differs from what the worker
holds.

**Trade-off.** One more key in the rendered config.

**Why (simplicity).** Schedule-equivalent resubmits already migrate
allocations in place. Without the marker, those resubmits would start
rewriting files under running processes that never asked for it.

## 5. One KV value holds the whole bundle

**Call.** `certFrom = { kv = "<path>" }` reads one PEM value: the chain, then
the key.

**Alternatives.** Two keys (`<path>/crt`, `<path>/key`), or a JSON object
`{ cert, key }`.

**Trade-off.** Users must concatenate (`cat fullchain.pem privkey.pem`).

**Why (simplicity, correctness).** Two keys need two puts, and a reader
between them pairs a new certificate with an old key. A JSON object is
atomic but cannot be passed straight to nginx or HAProxy. The concatenated
PEM is atomic, is what HAProxy wants, and nginx accepts it for both
`ssl_certificate` and `ssl_certificate_key`.

## 6. Fail-static: a bad bundle keeps the last good certificate

**Call.** The manager validates a bundle before sending it: certificate,
private key, and a key that matches the leaf (rustls `with_single_cert`). A
failure comes back as that allocation's `error`. The node keeps what it has,
and reports `tls_ready = true` with `tls_error` set while an older
certificate serves.

**Alternatives.** Fail closed: drop the listener's certificate when the
current value is bad.

**Trade-off.** A node can serve a certificate that is no longer the value in
KV, possibly until it expires. That is visible in status and the doctor, not
silent.

**Why (simplicity).** A typo in a `kv put` should not be an outage. Mesh
certificates and network policy are already fail-static on the same
heartbeat path.

## 7. One certificate fetch per heartbeat, with versions

**Call.** A node with KV-backed chains calls `GetIngressCertificates` on
every heartbeat, sending the versions it holds. An unchanged version gets no
material back.

**Alternatives.** Put `(allocation, version)` on the heartbeat reply and fetch
only when it moves, which costs no extra RPC.

**Trade-off.** One small RPC per heartbeat on those nodes.

**Why (simplicity).** No heartbeat format change and no manager-side version
tracking. It only costs anything on nodes that use the feature. Revisit if a
fleet runs many such nodes at a short heartbeat interval.

## 8. No `coalesce` on live files

**Call.** KV files get `changeMode`, `changeSignal`, and `splay`, but not
`coalesce`.

**Alternatives.** Full parity with `connect.templates`, which has all four.

**Trade-off.** The two template systems differ by one knob.

**Why (simplicity).** Bursts of KV writes are already coalesced by the
manager's `kvDebounceMs`. A second window on the worker would be a knob with
no new effect.

## 9. Start-time file writes became atomic too

**Call.** The `exec` and `container` start paths now write KV files with the
same writer as live updates and identity tokens: temp file created with the
declared mode, fsync, rename. The default mode is `0644` where none is set.

**Alternatives.** Leave the start path's plain `fs::write` plus chmod alone.

**Trade-off.** Unset modes become an explicit `0644` instead of the process
umask, which is the same on default systems.

**Why (correctness).** The plain write left a window where a private key was
readable at the umask, and a torn file was possible. With one writer, a file
looks the same whether it arrived at start or live.

## 10. Exec relative paths still resolve against the worker's directory

**Call.** For `exec`, a live rewrite resolves `path` exactly as the start path
does, as given. A relative path lands relative to the worker's working
directory, not the task directory.

**Alternatives.** Join relative paths onto the task directory.

**Trade-off.** A relative KV file path on `exec` is a footgun.

**Why.** Identity files document the same rule, so changing it is a
behavior change to two features. It is out of scope here and tracked in
TODO.md.
