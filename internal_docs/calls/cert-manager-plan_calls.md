# Judgement Calls: The Cert Manager (`certs` App)

Plan: [`plans/cert-manager-plan.md`](../plans/cert-manager-plan.md) ·
Public doc: [`docs/cert-manager.md`](../../docs/cert-manager.md)

Entries are ranked by project impact, highest first; an entry's number is
its rank.

## 1. The cert manager is a bundled app with object kinds, not a core subsystem

The request was "a cert manager as a built in app". It is a bundled
app (`src/apps/certs/`, manifest `src/apps/bundled/certs.ncl`), with two
object kinds, `certs/Issuer` and `certs/Certificate`, reconciled through the
apps framework.

**Alternatives.** (a) Caddy's shape in core: a port field such as
`tls = { acme = { domains = [...] } }` that the manager and the node datapath
fulfil, with no separate object. (b) A standalone daemon (`legion start
certs`), like the gateway.

**Trade-off.** Using a certificate is two declarations instead of one: a
Certificate object, and the KV key in the job's port.

```nickel
# what shipped                                   # (a), rejected
{ kind = "certs/Certificate", name = "shop",      ports = [{ name = "https", static = 443,
  spec = { issuer = "letsencrypt",                  tls = { acme = { names = ["shop.example.com"] } } }]
           dnsNames = ["shop.example.com"],
           kv = "certs/shop" } }
ports = [{ name = "https", static = 443,
           tls = { cert = { kv = "certs/shop" } } }]
```

**Why.** Simplicity and extensibility both: the app gets ACL'd objects,
schemas, CEL rules, status, `legion object`, MCP tools, events, a UI page,
upgrades with the binary, and `legion app disable`, all for free, and core
gains no ACME code on the manager's request path. (a) would put a network
client for an outside CA in core and tie certificates to one job's lifetime
(two jobs sharing a certificate would fight). The KV bundle the app writes
is the format core already serves (entry 4), so the second declaration is a
key path, not a concept.

## 2. Every user key is read and written acting for the object that names it

The app's grant is its own `legion/apps/certs/*` plus
`{ resources = ["kv"], actions = ["read", "write"], paths = ["*"], via = "objects" }`.
A Certificate `requires` kv read and write on its `kv` and `parts`; an
Issuer requires them on its CA keys and read on its DNS and EAB tokens. The
app reads and writes those keys with `legion-for-object`.

**Alternatives.** A plain grant: `kv:write` on `certs/*` (forcing every
certificate under one prefix), or on `*`.

**Trade-off.** A team needs both `object:write` on `certs/Certificate` and
`kv` read/write on the key to request a certificate:

```text
Error: writing certs/Certificate team-a/web needs kv:read on certs/elsewhere (from spec.kv), which your token does not hold
Hint: the kind requires it of whoever writes its objects (`legion object explain certs/Certificate`); have someone who holds it make the change
```

**Why.** Security boundary. A bundled app's grant is recorded by
`legion:bootstrap` with no check, so a plain `kv:write` on `*` would let
anyone who can write a Certificate overwrite any key in the cluster (a
database password, another team's certificate) through the app. Acting for
the object bounds the app by the writer, and the app holds no standing
access to any secret. The CA key is read for the *Issuer*, so a
Certificate's writer never needs access to it.

## 3. HTTP-01 is answered by a stateless responder people run, not by the app or the datapath

`legion cert http01 --issuer <name>` answers every
`/.well-known/acme-challenge/<token>` with `<token>.<thumbprint>` and
redirects the rest to HTTPS; `configs/certs/http01-responder.ncl` runs it
per node. DNS-01 is the recommended path.

**Alternatives.** (a) The node datapath answers challenges on ingress ports
(the old TODO entry, "answering HTTP-01 from the ingress datapath"). (b) The
app submits and maintains a responder job per Issuer (`http01 = { pool }`).
(c) No HTTP-01.

**Trade-off.** HTTP-01 is not zero-setup: someone runs one job, and port 80
must reach it. A user's own nginx can answer instead, with a copied
thumbprint.

**Why.** Legion has no cluster edge, so nothing knows which nodes a name
points at; (a) and (b) would both guess, and (a) puts app state (the
thumbprint) into the core datapath. The key authorization's second half is
the account's *public* thumbprint, so a responder needs no per-challenge
state and no secret: the simplest correct thing that works on every
topology. (a) stays the long-term shape once a cluster edge exists.

## 4. The output is the existing KV bundle, with optional `parts`

A certificate is written to `spec.kv` as the chain, leaf first, then the
PKCS#8 key: the format `ports[].tls.cert = { kv }` and live files already
consume. `parts = { cert, key, ca }` also writes each part to a key of its
own.

**Alternatives.** A Kubernetes-Secret-like JSON value (`{ "tls.crt",
"tls.key", "ca.crt" }`) in one key; always writing three keys.

**Trade-off.** `parts` are written one after another (key first), so a
reader of two parts can briefly see a new key with the old certificate. The
bundle never can.

**Why.** Ease: integrating with a job is a line it can already write, with
no template functions to add and no new datapath parser. Parts exist only
for software that insists on separate files (Postgres, Redis), and their
non-atomicity is documented.

## 5. One cluster-scoped Issuer kind, limited by `namespaces`

There is no namespaced `Issuer` / cluster `ClusterIssuer` split. An Issuer
is cluster-scoped and lists the namespaces (globs) that may use it,
default `["*"]`.

**Alternatives.** cert-manager's two kinds.

**Trade-off.** Teams cannot create their own Issuers without cluster-wide
`object:write` on `certs/Issuer`; an operator does it for them.

**Why.** Simplicity: one concept where Kubernetes has two, and the common
case (one Let's Encrypt issuer for everyone, one CA per trust domain) is
one object. `namespaces` gives the one control a shared CA needs.

## 6. A `ca` Issuer generates its own root when the key does not exist

cert-manager's private CA is three objects: a SelfSigned Issuer, a CA
Certificate it signs, and a CA Issuer that references the Certificate's
Secret. Here it is one: `ca = { kv, trust }`, generated (ECDSA P-256, 10
years, path length 0) when `ca.kv` is empty, loaded otherwise.

**Alternatives.** Require a CA in KV first; or cert-manager's three steps.

**Trade-off.** A typo in `ca.kv` silently makes a new CA (the Issuer reads
`Ready: Generated`, which says so). Generation uses `expected_version = 0`
so two passes never make two CAs.

**Why.** Ease: "internal TLS" is one apply. Bringing your own CA is still a
`kv put` before the apply.

## 7. Enabled by default on every cluster

The app is seeded enabled, like the autoscaler.

**Alternative.** Seeded disabled; `legion app enable certs` to opt in.

**Trade-off.** Every cluster runs one more process on its managers (100 MHz
and 128 MiB reserved) that does nothing until an Issuer exists.

**Why.** AGENTS.md: pick the default right for a new cluster. "Get core apps
off the ground" means the first `legion object apply` works with no
enable step to discover.

## 8. A failing renewal stays `Ready` while the certificate in KV is valid

Reason `RenewalFailing`, `Ready=true`, with the failure, the expiry, and
the next attempt in the message. `IssueFailed` (`Ready=false`) only when
there is no valid certificate.

**Alternative.** `Ready=false` on any failed attempt.

**Trade-off.** `legion object list` reads `Ready` for a certificate whose
renewal is failing; the reason, the Certificates page (which lists it as
failing, first), and the overview card are where it shows.

**Why.** Ready describes the workload's certificate, which is fine for
weeks; flipping it on a transient DNS hiccup would page people for nothing.
cert-manager does the same (its `Issuing` condition carries the failure).

## 9. The app's own ACME client, not `instant-acme`

`src/apps/certs/acme.rs` implements the RFC 8555 subset it needs on
`ring` and `reqwest`, with a fake CA (`test_support.rs`) that verifies
every JWS.

**Alternative.** The `instant-acme` crate.

**Trade-off.** About 600 lines of protocol code to maintain.

**Why.** No new dependency (every crypto and HTTP crate it needs is already
in the tree, with the TLS configuration Legion already uses), and an
in-process fake CA can drive every path in CI: badNonce retry, EAB,
wildcards over DNS-01, HTTP-01 through the real responder.

## 10. Backoff is the app's own: 5 minutes doubling to 6 hours

The object framework retries an `Err` from 1 second to 5 minutes. The cert
manager reports failures as outcomes and requeues itself on its own
schedule, persisted in status (`failedAttempts`, `nextAttemptTime`).

**Alternative.** The framework's backoff.

**Trade-off.** A fixed DNS token waits up to the next attempt unless someone
runs `legion cert renew` (a new attempt, which skips the backoff).

**Why.** Let's Encrypt allows five failed validations per name per account
per hour; the framework's schedule would exhaust that in a minute and lock
the name out. This one makes at most four attempts in any first hour, and
survives a restart.

## 11. Re-issue bookkeeping lives in status, seeded into memory

What the current certificate was issued for (spec hash, issuer identity,
serial, last renew request) and the failure count are status fields, read
back after a restart.

**Alternative.** A per-object record in the app's own KV.

**Trade-off.** A crash between writing the bundle and writing status
re-issues once on restart (the serial no longer matches).

**Why.** Status is where people already look, and a second store would be a
second thing to keep consistent. One extra issuance after a crash is cheap.

## 12. Forced renewal is a label, wrapped by `legion cert renew`

`legion cert renew <name>` sets `certs/renew=<now>` with an ordinary apply
(over the version it read); the app issues once per new value.

**Alternative.** An app action button; a spec field.

**Trade-off.** A renew is an object write (it needs what writing the
Certificate needs) and bumps its spec version.

**Why.** Actions only exist in job, task, and node slots, not on a
sidebar page, and a spec field would change the issuance hash. The label is
visible in `legion object get`, audited like any write, and works from
`legion object apply` too.

## 13. DNS-01 waits a fixed propagation delay instead of checking

The app publishes the TXT records, waits `propagationDelay` (default 30s,
at most 10m), then asks the CA.

**Alternative.** cert-manager's self-check: query the zone's authoritative
nameservers until the record shows.

**Trade-off.** A slow provider needs a longer delay set by hand.

**Why.** A self-check through a recursive resolver caches the NXDOMAIN it
sees before the record exists and can delay validation by the zone's
negative TTL; doing it right needs zone and NS discovery. The delay is
simple and right for Cloudflare and webhook bridges. A real self-check is a
TODO.

## 14. ECDSA keys only

`keyAlgorithm` is `ecdsa-p256` (default) or `ecdsa-p384`.

**Alternative.** RSA through `rcgen`'s `aws_lc_rs` backend.

**Trade-off.** Clients that cannot do ECDSA need a certificate from
elsewhere.

**Why.** `ring`, Legion's crypto library, cannot generate RSA keys, and a
second crypto backend is a large dependency for a shrinking need. CA keys
brought from outside may still be RSA.

## 15. One ACME account key per Issuer

Each Issuer's key lives at `legion/apps/certs/acme/<issuer uid>.key`.

**Alternative.** One account key for the whole cluster, so an HTTP-01
responder answers with a single thumbprint for every Issuer.

**Trade-off.** A responder serves one Issuer (`--issuer`); two HTTP-01
Issuers need two responders on different ports, or one Issuer.

**Why.** Isolation: deleting an Issuer ends its account, and Issuers at the
same CA with different contacts or EAB credentials do not share one.

## 16. Two DNS providers: Cloudflare and an `httpreq` webhook

**Alternative.** cert-manager's dozen (Route 53, Cloud DNS, Azure, RFC 2136,
...).

**Trade-off.** Everyone not on Cloudflare runs a bridge.

**Why.** The 80/20: Cloudflare is the common small-team DNS, and lego's
`httpreq` protocol already has bridges (acme-dns proxies, internal DNS
APIs). Each provider is a cloud SDK's worth of signing and testing; they
are TODO items, added when someone needs one.

## 17. A new key at every issuance

**Alternative.** Reuse the key across renewals.

**Trade-off.** Clients that pin the key (rare) break at each renewal.

**Why.** cert-manager's default since 1.18 (`rotationPolicy: Always`), and
the safer one; and nothing has to keep the old key around.

## 18. Deleting a Certificate leaves its key in KV

**Alternative.** Delete the KV key with the object (a cleanup window).

**Trade-off.** A deleted Certificate's certificate lingers until someone
deletes it.

**Why.** Deleting the object to stop renewals must not take down the
listener serving it; cert-manager keeps the Secret by default for the same
reason.

## 19. No jobspec key requests a certificate (cert-manager's ingress-shim)

**Alternative.** A `port` extension key, `certificate = { dnsNames }`, from
which the app makes a Certificate.

**Trade-off.** Two declarations (entry 1).

**Why.** An extension value has no recorded authority to act for, so the
app would need a plain `kv:write` grant (entry 2). Deferred until extension
values can carry authority, or until the shim can create a Certificate as
the job's submitter.

## 20. No job panel

**Alternative.** A panel on each job's page listing the certificates its
ports and files read.

**Trade-off.** On-call sees certificate expiry on the Certificates page, not
on the job.

**Why.** Finding which jobs read a key needs `job:read` across namespaces,
which would widen a grant that today holds no read of anything but the
app's own objects. Deferred.
