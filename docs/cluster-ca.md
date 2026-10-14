# The Cluster CA

One certificate authority anchors everything Legion's own processes present
to each other:

| Certificate | Names (SANs) | Used for |
| --- | --- | --- |
| node | `<node>.node.legion`, `legion://node/<id>` | the worker plane listener (port 4848), the mesh (`certFrom = "legion"`) |
| fleet | `<fleet>.fleet.legion`, `legion://fleet/<name>` | one simulator process serving many nodes |
| manager client | `legion://manager/<id>` | managers dialing the worker plane |
| manager server | `manager.legion`, `legion://manager/<id>` | workers dialing a manager (port 4747) |

Every join token pins this CA (`caHash`), and every holder verifies peers
against its trust bundle. The certificate's *role* comes from its URI SAN,
so "signed by our CA" is never enough on its own
([Auth & ACLs](auth-and-acls.md#the-worker-plane)).

**Most clusters need no configuration.** The default `builtin` provider
generates the CA on first use, keeps it in the replicated state store, and
rotates it with `legion net ca rotate`. Configure a provider only when your
organization's policy says every certificate must chain to its own PKI, or
when the CA key must not live in Legion's state store (and so in every
backup and snapshot).

## Providers

| Provider | Where the signing key lives | Rotation | Choose it when |
| --- | --- | --- | --- |
| `builtin` (default) | the replicated state store (`net_mesh_ca`) | `legion net ca rotate`, two steps, nothing breaks | you have no PKI requirement |
| `file` | a file on each manager's disk | replace the files ([Rotation](#rotation)) | you have an intermediate CA for Legion from your root |
| `exec` | an external CA (Vault PKI, step-ca, AWS Private CA, ...) | in the external CA | the key must never leave your CA |

Whatever the provider, **Legion decides the names**. The provider is asked to
sign a certificate request with names Legion chose from the caller's
authenticated identity, and Legion checks what comes back before handing it
out ([What Legion checks](#what-legion-checks)).

## Configuration

The `clusterCa` block in the manager config. Omit it for `builtin`.

```nickel
clusterCa = {
  provider = "builtin",          # "builtin" (default) | "file" | "exec"

  # file: an operator-provided CA, typically an intermediate under your root
  certPath = "/etc/legion/ca/cluster-ca.pem",  # the signing CA first, then any intermediates
  keyPath = "/etc/legion/ca/cluster-ca.key",   # its private key, PKCS#8 ("BEGIN PRIVATE KEY")
  bundlePath = "/etc/legion/ca/trust.pem",     # optional; default: the signing CA itself

  # exec: an external signer, JSON on stdin/stdout
  command = ["legion-ca-vault", "--mount", "pki_legion", "--role", "legion"],
  timeout = "10s",           # per call
  refreshInterval = "1m",    # how often the trust bundle is re-read
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `provider` | `builtin` | Which provider signs. |
| `certPath` | none | `file` only, required. The signing CA certificate, optionally followed by the intermediates that chain it to the trust bundle. |
| `keyPath` | none | `file` only, required. The signing CA's private key, PKCS#8 PEM: ECDSA P-256/P-384, Ed25519, or RSA. |
| `bundlePath` | the signing CA | `file` only. The CA certificates every holder trusts. |
| `command` | none | `exec` only, required. The signer to run. |
| `timeout` | `10s` | `exec` only. How long one call may take. |
| `refreshInterval` | `1m` | `exec` only. How often `describe` is re-run. Minimum `1s`. |

A malformed block (an unknown provider, a missing required field, a field
the provider does not use, a misspelled field) stops the manager at start
with the field named. **Every manager in a cluster needs the same block**
(and, for `file`, the same files); see
[Every manager must agree](#every-manager-must-agree).

### `builtin`

The CA Legion has always had. The control-lane leader generates it the first
time anything needs it (the first join token or node certificate), and it
replicates to every manager with the rest of the control lane, key included.
`QueryStateStore` and the State Explorer read the key column as `NULL`, but a
copy of `store.sqlite`, a backup, or a raft snapshot contains it.

### `file`

```nickel
clusterCa = {
  provider = "file",
  certPath = "/etc/legion/ca/cluster-ca.pem",
  keyPath = "/etc/legion/ca/cluster-ca.key",
}
```

The manager reads the files itself; the key is never written to the store, a
changeset, a snapshot, or a backup. Put the same files on every manager,
readable only by the user Legion runs as.

- **The trust bundle defaults to the signing CA itself**, not your root.
  Holders then trust certificates from Legion's intermediate only. If they
  trusted your root, any other intermediate under it that will issue a
  `legion://manager/` URI SAN could mint a manager. To trust the root (which
  makes rotating the intermediate a one-step file swap, see
  [Rotation](#rotation)), set `bundlePath` to it.
- **The key must be PKCS#8** (`-----BEGIN PRIVATE KEY-----`). A
  `BEGIN EC PRIVATE KEY` or `BEGIN RSA PRIVATE KEY` file is refused with the
  conversion command: `openssl pkcs8 -topk8 -nocrypt -in ca.key -out
  ca-pkcs8.key`.
- **The files are re-read when they change** (by modification time and size,
  checked whenever the manager reads the CA), so a new CA needs no restart.
- **Every load runs a self-test**: the manager signs a probe certificate and
  verifies it exactly as a holder would. That catches a key that does not
  match the certificate, a CA whose name constraints exclude `.legion`, an
  expired CA, and a CA subject Legion's signer cannot reproduce byte for byte
  (an unusual DN encoding), before any worker sees a certificate.
- **At start**, files that do not load stop the manager with the field and
  the reason. **Later**, files that do not load are reported in
  `legion net ca show`, logged once per change, and the previous CA keeps
  signing.

A test PKI, for trying it out:

```sh
# A root (keep its key offline) and an intermediate for Legion.
openssl req -x509 -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout root.key -out root.pem -days 3650 -subj "/O=Acme/CN=Acme Root" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout cluster-ca.key -out cluster-ca.csr -subj "/O=Acme/CN=Acme Legion Intermediate"
printf 'basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign\n' > ca.ext
openssl x509 -req -in cluster-ca.csr -CA root.pem -CAkey root.key -CAcreateserial \
  -days 1825 -extfile ca.ext -out cluster-ca.pem

legion net ca provider test --cert-path cluster-ca.pem --key-path cluster-ca.key
```

### `exec`

The manager runs `command` with one JSON document on stdin and reads one
from stdout, like the KV keyring's `exec` root key provider
([KV Encryption](kv-encryption.md#root-key-providers)). Two operations:

```json
{"op":"describe"}
→ {"bundle_pem":"<every CA holders trust>","chain_pem":"<the signing CA, then intermediates toward the bundle>"}

{"op":"sign","role":"node","csr_pem":"-----BEGIN CERTIFICATE REQUEST-----…",
 "common_name":"node-1.node.legion","dns_sans":["node-1.node.legion"],
 "uri_sans":["legion://node/node-1"],"ttl_seconds":86400,
 "usages":["server_auth","client_auth"]}
→ {"cert_pem":"<the leaf, optionally followed by its intermediates>"}
```

- `role` is `node`, `fleet`, `manager_server`, or `manager_client`, so a
  helper can map roles to separate CA roles. `usages` is `server_auth`
  and/or `client_auth`. The CSR's own names, if any, must be ignored: sign
  the names in the request.
- A non-zero exit, a timeout, or anything but a JSON object with the
  required field on stdout is a provider error. Stderr is included in the
  error.
- `describe` runs at manager start (in the background; a signer that is
  down never stops a manager from starting). Its answer is re-read in the
  background once it is older than `refreshInterval`, while the previous
  answer keeps serving. A `describe` that fails keeps the last good answer
  and is reported. Until the first answer arrives, anything that needs the
  CA gets `Unavailable` at once, and the signer is asked again at most every
  5 seconds.
- `sign` runs once per certificate: when a node needs a certificate (after
  registration, then every 8 hours, or when the CA changes) and when a
  manager re-mints its own two certificates (at a third of their lifetime,
  in the background). Never on a heartbeat or a KV path.

A reference helper for **HashiCorp Vault PKI** ships in
[`plugins/cluster_ca/vault-pki/`](../plugins/cluster_ca/README.md), with the
mount, role, and policy it needs. Copy it as the starting point for any other
CA: decode the request, call your CA, print the reply.

## What Legion checks

Legion builds every request from the caller's authenticated identity: a
node's certificate names that node, a manager's certificate names that
manager. Whatever a `file` or `exec` provider returns is checked before it is
handed to anyone:

- it is a certificate, and it is **not a CA** (no `CA:TRUE`, no
  `keyCertSign` or `cRLSign` key usage);
- its public key is **the CSR's**;
- its DNS SANs and URI SANs are **exactly** the requested ones, with no other
  SAN types (no IP or email SANs);
- it has an extended key usage with **every requested usage** and nothing
  beyond `serverAuth` and `clientAuth`;
- its `notBefore` is at most 5 minutes ahead of the manager's clock, and it
  is valid for **at most 24 hours plus 10 minutes and at least 2 hours**
  (workers re-issue with under an hour left, so a shorter certificate would
  re-issue on every heartbeat);
- it **chains to the trust bundle**, verified the way a holder verifies it,
  for each usage, including the CA's name constraints and path length.

A refusal is `FailedPrecondition` and names the provider and the mismatch:

```console
error: the exec (legion-ca-vault --mount pki_legion --role legion) cluster CA provider
returned a certificate Legion will not hand out: its URI SANs are
[legion://node/node-1, spiffe://corp/x], but Legion asked for exactly [legion://node/node-1]
```

A chain failure from `exec` re-reads `describe` once before refusing, so an
issuer rotated inside the external CA is picked up, not refused. Refusals
are counted in `legion_cluster_ca_provider_calls_total{outcome="refused"}`.

The built-in CA builds each certificate from the request itself and is not
re-checked.

## Testing a provider

Before pointing a manager at a provider, run the conformance harness. It runs
locally, the way a manager would, and never touches the cluster:

```console
$ legion net ca provider test -- legion-ca-vault --mount pki_legion --role legion
Testing cluster CA provider: legion-ca-vault --mount pki_legion --role legion
  [ok] describe                     bundle of 1 CA(s), anchor sha256:9c1e4f3a7b20...
  [ok] sign node                    legion://node/conformance-node verified, valid for 1day
  [ok] sign manager server          legion://manager/conformance verified, valid for 1day
  [ok] sign manager client          legion://manager/conformance verified, valid for 1day
  [ok] requested names win          the CSR asked for manager.legion; the certificate names only the requested node

Provider is usable. Configure it with:
  clusterCa = { provider = "exec", command = ["legion-ca-vault", "--mount", "pki_legion", "--role", "legion"] }
```

`--cert-path`, `--key-path`, and `--bundle-path` test `file` material
instead. `--timeout` sets the per-call limit for `exec`. `-j` prints the
standard JSON envelope with every check. An `exec` signer really issues the
four test certificates (for the node `conformance-node` and the manager id
`conformance`); they are valid for a day and are never installed anywhere.

The last check sends a CSR that asks for `manager.legion` and expects a
certificate for the node Legion named. A CA that copies a CSR's names (Vault's
`use_csr_sans`, which defaults to on) fails it, and would have its
certificates refused at runtime.

## Every manager must agree

All managers in a raft cluster must use the same CA, or workers that trust
one manager's CA cannot reach another. The control-lane leader records its
CA view in the replicated store: the provider, the **anchor** (the
trust-bundle CA the signing CA chains to, which join tokens pin), and the
pins of the whole trust bundle. Every other manager compares its own view
with that record:

- a `builtin` manager agrees only with a `builtin` record;
- a `file` or `exec` manager never agrees with a `builtin` record;
- two `file`/`exec` views agree when **each one's anchor is in the other's
  trust bundle**, which holds at every step of the
  [rotation procedure](#rotation).

A manager that disagrees **signs nothing**: no node certificates, no new
certificates of its own, no join tokens. It keeps the certificates it already
holds until they expire, and says why:

```console
$ legion net ca show
Provider: builtin
Problem:  this manager uses the built-in cluster CA, but the cluster's CA comes from the
`file` provider (recorded by manager mgr-1 at 2026-10-14T09:12:03Z); give this manager
the same `clusterCa` block as the others (docs/cluster-ca.md#every-manager-must-agree)
```

Because the leader's view is the record, a misconfigured leader makes the
correctly configured managers report the problem. Their error names the
leader: that is the manager to fix. A cluster that has never had a CA view
recorded (a fresh cluster before its leader has looked) is not checked.

## Rotation

### `builtin`

```console
$ legion net ca rotate
Rotated the cluster CA. The new CA (pin sha256:...) signs every certificate from now on.
The previous CA stays trusted until 2026-11-03T18:05:11Z, so no worker-plane or mesh connection breaks;
nodes re-issue their certificates within one heartbeat.
```

The new CA is cross-signed by the old one and signs at once; both stay in
every bundle for 24 hours ([Auth & ACLs](auth-and-acls.md#rotating-the-cluster-ca)).

### `file`

Legion does not hold your root, so it cannot cross-sign, and
`legion net ca rotate` refuses with `FailedPrecondition` and this procedure:

1. **Add the new CA to the bundle** on every manager: set `bundlePath` to a
   file holding both the old and the new CA. Within one heartbeat every
   worker re-issues and holds both.
2. **Switch the signing CA**: point `certPath`/`keyPath` at the new CA on
   every manager. Every node re-issues within one heartbeat (the anchor
   changed), so every certificate now comes from the new CA while the old one
   is still trusted.
3. **A day later, remove the old CA** from the bundle.

Every manager agrees with every other at each step, so the managers can be
updated one at a time. If `bundlePath` holds your root and the new
intermediate is under the same root, only step 2 is needed.

### `exec`

The same steps happen inside your CA: make the new CA part of the bundle
`describe` reports, wait a heartbeat (plus `refreshInterval`), switch the CA
the signer uses, and drop the old CA a day later. With the reference Vault
helper, whose bundle is every issuer in the mount: create a new issuer in the
mount, make it the default, and delete the old issuer a day later.

### How workers learn

Every heartbeat reply names the pins of the manager's whole trust bundle,
anchor first (`HeartbeatReply.cluster_ca_pins`). A worker whose bundle has a
different anchor or a different set of CAs re-issues its certificate at once,
and the reply carries the new bundle, which it then verifies managers and
peers against. A worker that was offline for a whole rotation (it never saw
the new CA before the old one was removed) must re-join.

## Status and diagnostics

```console
$ legion net ca show
Provider: file  (/etc/legion/ca/cluster-ca.pem, bundle /etc/legion/ca/trust.pem)
Pin:      sha256:9c1e4f3a7b2041d4...
Bundle:   1 CA(s), the anchor first. Leaves carry 1 intermediate(s) after them.
-----BEGIN CERTIFICATE-----
...
```

The PEM bundle goes to stdout and the rest to stderr, so
`legion net ca show > ca.pem` writes a file an outside client can trust. `-j`
adds `provider`, `providerLocation`, `chainPem`, and `problem`. The same
reply is `GetMeshCa` over gRPC and the `legion_net_ca_show` MCP tool. Each
manager answers for itself, so check every manager when diagnosing.

The metric `legion_cluster_ca_provider_calls_total{provider,op,outcome}`
counts `load` (`file`), `describe` (`exec`), and `sign` calls by outcome
(`ok`, `error`, `refused`). Alert on `outcome="refused"`: a provider is
handing out certificates Legion will not use.

## Failure modes

| What happens | What Legion does |
| --- | --- |
| `file` material unusable at start | The manager does not start. The error names the field, the file, and the reason. |
| `file` material changes and is unusable | The previous CA keeps signing; `net ca show` reports the problem. |
| `exec` signer down at start | The manager starts. Signing, join tokens, and the manager's own certificates wait for it (`Unavailable`). |
| `exec` `describe` fails later | The last good bundle is used and the problem is reported. |
| `exec` `sign` fails | `IssueNodeCertificate` returns `Unavailable`; the worker keeps its certificate and retries on its next heartbeat. |
| A provider returns a certificate Legion refuses | `FailedPrecondition` naming the provider and the mismatch; counted as `refused`. |
| A manager disagrees with the cluster's CA | It signs nothing and mints no join tokens; `net ca show` names both CAs and the recording manager. |
| `legion net ca rotate` on `file`/`exec` | `FailedPrecondition` with the procedure for that provider. |

## Known limitations

- **Changing provider on a running cluster means re-joining workers.** A
  worker trusts only the CA it joined under, and nothing cross-signs between
  an old built-in CA and a new external one. Reconfigure every manager, then
  re-join every worker with a fresh join token.
- **Agreement is checked against the leader only.** Two managers that each
  agree with the leader but not with each other are not detected.
- **No revocation.** Certificates live 24 hours, which is the revocation
  story, as with the built-in CA. An external CA's CRLs and OCSP are not
  consulted.
- **The `file` signer cannot reproduce every CA subject.** A CA whose
  subject has a multi-valued RDN or repeats an attribute type (two `OU`s) is
  refused by the load-time self-test. Use `exec` for such a CA.
- **The reference Vault helper is a starting point**: it needs the `vault`
  CLI and `jq`, and it is not exercised against a live Vault in Legion's CI.
  Run `legion net ca provider test` against your own setup.
- **Raft traffic between managers is not covered**, whichever provider is
  configured: it stays plaintext on the managers' network.
