# Judgement Calls: Pluggable Cluster CA Providers

Decisions made while planning and building
[`plans/cluster-ca-provider-plan.md`](../plans/cluster-ca-provider-plan.md).
**Entries are ranked by project impact, highest first; an entry's number is
its rank.** The public page is [`docs/cluster-ca.md`](../../docs/cluster-ca.md).

---

## 1. Legion decides the names, the provider only signs, and Legion checks what comes back

**Call.** Every signing request carries a `LeafSpec` Legion built from the
authenticated caller (`CsrSubject` for nodes and fleets, the manager's own
id for manager certificates), exactly as the built-in CA always did. A
`file` or `exec` provider's answer is verified before anything is handed
out: it chains to the provider's trust bundle (webpki, for each requested
usage), carries the CSR's public key, carries exactly the requested DNS and
URI SANs and no other SAN type, is not a CA and cannot sign certificates,
has the requested EKUs (entry 7), and has a sane validity (entry 8). A
refusal names the provider and the mismatch:

```console
error: the exec (legion-ca-vault --mount pki_legion --role legion) cluster CA provider
returned a certificate Legion will not hand out: its URI SANs are
[legion://node/node-1, spiffe://corp/x], but Legion asked for exactly [legion://node/node-1]
(FAILED_PRECONDITION)
```

**Alternatives.** (a) Trust the provider: it is the operator's own CA, and
Kubernetes' CSR API does not re-verify what a signer returns. (b) Let the
provider choose the names from a policy of its own (cert-manager issuers,
Vault roles), with Legion only reading the role back from the URI SAN.

**Trade-off.** One webpki verification and an x509 parse per certificate,
on a path that runs once per node every 8 hours. Strictness costs some
compatibility: a Vault role with `use_csr_sans` left at its default copies
a CSR's names, and Legion refuses the result rather than accepting a
superset. The conformance harness has a check for exactly that ("requested
names win"), and the Vault helper's README sets the role up so it passes.

**Why (security boundary, and simplicity).** Role is authority in Legion: a
certificate carrying `legion://manager/<id>` may dispatch work to every
worker. Whatever signs it must not be able to widen it, through a bug, a
permissive role, or a worker's crafted CSR. Checking at the one place every
certificate passes keeps the guarantee identical across providers, so the
security story in `docs/auth-and-acls.md` does not grow a per-provider
footnote. (b) would have moved Legion's most important authorization
decision into each operator's CA configuration.

## 2. Managers agree through the leader's recorded view, checked as trust compatibility

**Call.** The control-lane leader writes its provider's view (provider kind,
anchor pin, bundle pins) into a replicated one-row table,
`cluster_ca_record`, whenever that view changes. Every other manager
compares its own view with the record: a `builtin` manager agrees only with
a `builtin` record; a `file`/`exec` manager never agrees with a `builtin`
record, and agrees with another `file`/`exec` record when each one's anchor
is in the other's bundle. A manager that disagrees refuses to sign, to mint
its own certificates, and to mint join tokens, and says why:

```console
$ legion net ca show
Provider: file  (/etc/legion/ca/cluster-ca.pem)
Problem:  this manager's cluster CA (file, anchor sha256:51aa07...) and the cluster's
(file, anchor sha256:9c1e4f..., recorded by manager mgr-1 at 2026-10-14T09:12:03Z)
do not trust each other: ...
```

No record (a fresh cluster before its leader has looked at the CA) means
no check.

**Revised in review: the record is written in the background, never on a
request.** As first built, the leader wrote the record from the agreement
check itself, which every signature runs, so the first request that signed
anything after a start (a session start dialing its node, say) carried a
raft write. `tests/session_integration.rs` caught it: "a session start writes
no raft entry" saw one more entry. The check is now read-only on every path;
`ClusterCa::record` writes the leader's view from the manager's certificate
refresher (at start, then every minute) and after a rotation. The cost is a
window of up to a minute after a leadership change in which followers check
against the previous leader's record.

**Alternatives.** (a) Exact equality of the signing CA's pin, which the
original brief suggested. (b) Per-manager records, every manager checked
against every other. (c) No detection: document "configure every manager
the same".

**Trade-off.** Only disagreement *with the leader* is detected: two
followers that each trust the leader but not each other pass. The leader's
view wins by definition, so a misconfigured leader makes the correctly
configured followers refuse (their error names the leader, which is the
right manager to look at). A brand-new cluster has a window before the
first record. And "refuse to sign" is not "refuse to serve": a manager that
starts disagreeing keeps presenting the certificates it already holds until
they expire.

**Why (simplicity, and rotation that breaks nothing).** (a) breaks every
rolling rotation: the moment the first manager gets the new files, every
other manager refuses (or the reverse), for as long as the rollout takes.
Trust compatibility is the property that actually matters ("would the rest
of the cluster accept what I sign?") and admits every step of the
documented rotation. (b) needs followers to write, but control-lane writes
are leader-only in this codebase and a per-manager table needs garbage
collection for departed managers. (c) is the failure that motivated the
check: a manager restarted without the `clusterCa` block silently signs
with a fresh built-in CA, and half the fleet stops trusting the other half.

## 3. CA configuration is per manager, like the keyring, not replicated cluster state

**Call.** `clusterCa` lives in each manager's config file, like
`encryption.rootKey` and `server.tls`. Entry 2 detects managers that
disagree.

```nickel
clusterCa = { provider = "exec", command = ["legion-ca-vault", "--mount", "pki_legion", "--role", "legion"] }
```

**Alternative.** Consul's model: the CA configuration is cluster state in
Raft, set through an API (`consul connect ca set-config`), so every server
agrees by construction.

**Trade-off.** Agreement is detected rather than guaranteed, and an
operator must put the block (and for `file`, the files) on every manager.

**Why (simplicity, and keeping secrets out of the store).** A provider's
configuration points at credentials: a key file, a Vault token in the
helper's environment. Replicated state is in every snapshot and backup,
which is precisely what this plan takes the CA key out of. Per-manager
config also matches how operators already ship the keyring's root key and
`server.tls`, so there is one way to configure a secret-bearing seam.

## 4. The `file` trust bundle defaults to the signing CA, not the root above it

**Call.** Without `bundlePath`, holders trust only the CA in `certPath`
(typically an intermediate), not the organization's root.

```nickel
clusterCa = { provider = "file", certPath = "/etc/legion/ca/cluster-ca.pem", keyPath = "/etc/legion/ca/cluster-ca.key" }
# holders trust cluster-ca.pem itself; add bundlePath = ".../corp-root.pem" to trust the root
```

**Alternative.** Default to the root: the last certificate in `certPath`,
or require `bundlePath`.

**Trade-off.** Rotating the intermediate under the same root needs the
two-step bundle procedure (add, switch, remove) instead of being a
one-step file swap. An operator who wants one-step intermediate rotation
sets `bundlePath` to the root.

**Why (security).** Trust in Legion is "any certificate from a trusted CA
with the right URI SAN". Trusting the corporate root means any other
intermediate under it, run by any other team, that can be talked into a
`legion://manager/` URI SAN mints a manager. Scoping trust to Legion's own
intermediate is the safe default; widening it is a one-line, deliberate
choice. Linkerd and Istio make the same split between trust anchor and
issuer.

## 5. Heartbeats carry every bundle pin, and workers re-issue on any anchor or bundle change

**Call.** `HeartbeatReply.cluster_ca_pin` (the signing CA's pin) is
replaced by `repeated string cluster_ca_pins`: the whole trust bundle,
anchor first. A worker re-issues when its held bundle's first pin differs
or its set of pins differs. Field 14 is reserved; the new field is 22.

**Alternatives.** (a) Keep the single pin and let bundle-only changes
reach workers on their 8-hour refresh. (b) Keep `cluster_ca_pin` beside
the new list.

**Trade-off.** A break in a worker-facing message: across versions the
field the other side reads is empty, so during a rolling upgrade a CA change
reaches older nodes only on their 8-hour timer (buf's WIRE check passes,
because field 14 is reserved). Plus a few lines changed in
`src/worker/daemon.rs`. Each built-in rotation now causes a
second, harmless re-issue per node when its overlap ends (the old CA left
the bundle).

**Why (rotation that breaks nothing).** With `file`/`exec` the procedure is
"add the new CA to the bundle, then switch". Under (a), step 1 takes 8
hours to reach every worker, and an operator who switches sooner strands
the workers that have not refreshed: they no longer trust the manager they
must reach to learn the new bundle. Re-issuing when the anchor changes,
even when the new anchor is already in the held bundle, moves every leaf to
the new CA while the old one is still trusted, so removing the old CA later
breaks nothing either. (b) is a compatibility shim for an installed base
that does not exist.

## 6. Legion refuses to rotate `file`/`exec` CAs and names the procedure instead

**Call.** `legion net ca rotate` and `RotateMeshCa` return
`FailedPrecondition` for `file`/`exec`, with the provider and the steps:

```console
$ legion net ca rotate
error: the cluster CA comes from the `file` provider (/etc/legion/ca/cluster-ca.pem);
Legion cannot rotate it. Rotate it by replacing the files on every manager: add the new CA
to clusterCa.bundlePath on every manager and wait one heartbeat, switch
clusterCa.certPath/keyPath to the new CA, then remove the old CA from the bundle a day
later (docs/cluster-ca.md#rotation)
```

**Alternatives.** (a) Have the provider generate and cross-sign a new
intermediate (Consul's Vault provider does this against a root mount). (b)
Accept the call and do nothing for those providers.

**Trade-off.** Rotation of an external CA is a documented manual procedure,
not one command.

**Why (simplicity, and the provider owns its key).** Legion holds neither
key, so it cannot cross-sign, and generating intermediates would need a
much larger `exec` contract (root mounts, CSR for an intermediate,
set-signed) for something an organization's PKI team already does with its
own tools. Refusing loudly with the steps is better than a silent no-op.

## 7. The requested EKUs must be present, and only serverAuth/clientAuth may be

**Call.** A returned leaf must have an EKU extension that includes every
usage Legion asked for and nothing outside {serverAuth, clientAuth}.
`anyExtendedKeyUsage` and a missing extension are refused.

**Alternative.** Exact equality with the requested set.

**Trade-off.** A manager's client certificate may come back with
serverAuth too.

**Why (compatibility at no security cost).** Vault's PKI roles set both
flags by default for every certificate a role signs; exact equality would
force a separate role per manager-certificate kind. An extra serverAuth on
a manager's *client* certificate grants nothing: it carries no
`manager.legion` DNS SAN (entry 1 refuses extra names), so it cannot pose
as a manager server, and roles come from URI SANs anyway.

## 8. A returned certificate lives between 2 hours and the requested 24 hours plus 10 minutes

**Call.** Refuse a `notAfter` more than 10 minutes past `now + 24h`, a
lifetime under 2 hours, and a `notBefore` more than 5 minutes ahead of the
manager's clock. Manager certificates re-mint at a third of their actual
lifetime (at most 8 hours), not a fixed 8 hours.

**Alternatives.** Accept whatever the CA issues; or require exactly 24
hours.

**Trade-off.** A CA whose role caps TTLs at an hour is refused, and the
error tells the operator to raise it.

**Why (performance and safety).** Workers re-issue with under an hour left,
so a 1-hour certificate would re-issue on every heartbeat, a request storm
against the external CA. A longer-than-requested certificate outlives the
24-hour revocation story Legion documents. 10 minutes of slack absorbs
clock skew and CA backdating.

## 9. Common names are hostname-shaped for every role, the built-in CA included

**Call.** Leaves get `node-1.node.legion`, `sim.fleet.legion`,
`manager.legion`, and `mgr-1.manager.legion` as their CN (before: `node-1`,
`fleet sim`, `manager mgr-1`).

**Alternative.** Keep the old CNs for `builtin` and send hostname-shaped
ones only to `exec`.

**Trade-off.** A visible (if inert) change to the built-in CA's
certificates, which the brief asked to keep "exactly" as before.

**Why (simplicity).** Nothing verifies the CN (roles come from URI SANs,
names from DNS SANs), but external CAs validate it: a Vault role can only
be narrowed to `allowed_domains = "legion"` if every CN is a name under
`legion`. One spec for every provider beats two notions of a certificate's
name.

## 10. A broken `file` CA stops the manager at start, but fails static afterwards; `exec` never stops a start

**Call.** At start, `file` loads and self-tests its CA (sign a probe leaf,
verify it) and a failure stops the manager with the field named. Later, a
file change that does not load keeps the last good CA signing and reports
the problem in `legion net ca show`. `exec` warms `describe` in the
background and never fails the start.

**Alternative.** One behavior for both: always fail fast, or always start
and report.

**Trade-off.** Two behaviors to document.

**Why (who is present).** A file is local and wrong at start only because
the operator just configured it, so they are there to read the error
(mirrors `server.tls`). A signer over the network can be down for reasons
unrelated to the manager, and a manager that will not boot while Vault is
restarting is a worse incident than one that cannot issue for a minute.
After start, refusing to sign because someone half-copied a file would
turn an operator typo into an outage.

## 11. Manager certificates come from a CSR for every provider, the built-in CA included

**Call.** `issue_manager_cert` generates the manager's key and a CSR and
signs the CSR, for `builtin` too. Before, it signed the generated key
directly with the CA key.

**Alternative.** Keep the direct path for `builtin` and use CSRs only for
providers that need them.

**Trade-off.** A CSR serialization and parse per manager certificate (a
few per day per manager).

**Why (one path).** The same function serves every provider, and the
built-in path exercises the code the others depend on in every test.

## 12. `exec` `describe` is stale-while-revalidate; `sign` waits on the request path

**Call.** After the first call, a stale `describe` keeps serving while a
background thread refreshes it; a failed refresh keeps the last good
answer. Before the first answer, only the very first call in the process
waits (a manager makes that call in the background at start); every later
call gets `Unavailable` at once and triggers a background retry at most
every 5 seconds, so a signer that is down at boot never parks heartbeat or
dial paths behind it. The in-process worker (`--run-worker`) is the one
exception: it waits up to 30 seconds for its pin. `sign` runs synchronously
(in `spawn_blocking` for `IssueNodeCertificate`). A manager's own re-mint
through `exec` runs in the background while the current certificates keep
serving; `builtin`/`file` re-mint inline as before.

**Alternative.** Refresh `describe` synchronously when stale, or on a
fixed timer thread per manager.

**Trade-off.** A bundle change reaches a manager up to one
`refreshInterval` (default 1 minute) plus one refresh later.

**Why (performance).** `describe` feeds the heartbeat's pin list and the
worker-plane dial path; a synchronous refresh would put a process spawn and
a network round trip on those paths once a minute.

## 13. Switching an existing cluster's provider requires re-joining workers

**Call.** Documented as a limitation, not built: a worker trusts only the
CA it joined under, so moving a running cluster from `builtin` to `file`
(or between unrelated CAs) means reconfiguring every manager and
re-joining every worker.

**Alternative.** Cross-sign the new CA with the built-in CA's key (it is in
the store) and run an overlap, as a built-in rotation does.

**Trade-off.** Adopting an external CA on a running cluster is disruptive.

**Why (scope).** There is no installed base (AGENTS.md), and the migration
path touches every provider's semantics for a one-time event. It is a
follow-up in `internal_docs/TODO.md` if a real cluster needs it.

## 14. The conformance harness is catalogued `mutates: false`, `read_only_safe: false`

**Call.** `net.ca.provider.test` has no gRPC method, does not change the
cluster, and is not agent-safe.

**Alternative.** `read_only_safe: true`, since it never touches the
cluster.

**Trade-off.** An agent restricted to safe commands cannot run it.

**Why.** It runs an arbitrary local command, and an `exec` signer really
issues the test certificates. That matches `net.handler.test`, the other
harness that probes something outside the cluster. (`legion keyring
provider test` has no catalog entry at all; that is a separate gap.)

## 15. The exec tests drive a real helper by re-running the test binary

**Call.** The `exec` tests point the provider at a shell script that runs
the test binary itself (`--exact` on a helper test that is a no-op unless
an environment variable names its fixture directory), which signs with
rcgen and writes its JSON reply to a file the script prints.

**Alternatives.** A shell script calling `openssl x509 -req`; or a canned
reply.

**Trade-off.** Each helper call starts the test binary (tens of
milliseconds), and the indirection is unusual.

**Why.** It exercises the real contract (process spawn, JSON over stdio,
timeouts, exit codes) with real signatures, and depends on nothing outside
the build: no `openssl`, `jq`, or Python on the CI image. A canned reply
cannot sign the manager's own internally generated CSRs.

## 16. The Vault helper needs `jq`

**Call.** `plugins/cluster_ca/vault-pki/legion-ca-vault` parses requests
and Vault's replies with `jq`.

**Alternative.** `sed`, as the keyring helpers do.

**Trade-off.** One more dependency on the manager host.

**Why.** The `sign` request carries JSON arrays and Vault returns a
certificate chain as an array; `sed` parsing of either is fragile in a
security-relevant script. Vault operators almost always have `jq`.
