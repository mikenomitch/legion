# Cert Manager

Legion ships a cert manager: a bundled [app](apps.md) named `certs` that
gets TLS certificates, keeps them in [KV](kv-store.md), and renews them
before they expire. It covers the core of Kubernetes' cert-manager:

- **Let's Encrypt**, and any other ACME CA (ZeroSSL, Google Trust Services,
  step-ca, your own), proving names with DNS-01 or HTTP-01;
- **a private CA** for internal TLS and mTLS, generated for you or brought
  from your own PKI;
- **self-signed** certificates, for development;
- **renewal** two thirds of the way through a certificate's life, with
  backoff that keeps you inside the CA's rate limits, and status that says
  when the next renewal is and why the last one failed.

A certificate lands in KV as the PEM bundle the rest of Legion already
serves, so using it is one line in a job: the node datapath terminates TLS
with it (`ports[].tls.cert = { kv }`), or the task reads it as a file that
is rewritten in place when it renews.

```console
$ legion object apply -f issuer.ncl --wait
certs/Issuer letsencrypt: created (generation 1, spec version 3)
certs/Issuer letsencrypt: Ready (Registered: registered at https://acme-v02.api.letsencrypt.org/directory; proves names with dns01 (cloudflare))

$ legion object apply -f certificate.ncl --wait
certs/Certificate default/shop: created (generation 1, spec version 4)
certs/Certificate default/shop: Ready (Issued: valid until 2027-02-05T10:12:00Z; renews at 2027-01-06T10:12:00Z)

$ legion object list certificates -A
NAMESPACE  NAME  STATUS  AGE  ISSUER       KV          EXPIRES
default    shop  Ready   1m   letsencrypt  certs/shop  2027-02-05T10:12:00Z
```

The app is part of every cluster: the first manager to lead a cluster seeds
it, enabled, on the managers (`legion app status certs`). It does nothing
until someone applies an Issuer.

## Quick start: HTTPS for a website

You have a domain whose DNS is at Cloudflare, and a job that should serve
it over HTTPS.

**1. Put a DNS token in KV.** A Cloudflare API token with `Zone:Read` and
`DNS:Edit` on the zone:

```console
$ legion kv put secrets/cloudflare-dns "<token>"
```

**2. Apply an Issuer**
([`configs/certs/letsencrypt-issuer.ncl`](../configs/certs/letsencrypt-issuer.ncl)):

```nickel
{
  kind = "certs/Issuer",
  name = "letsencrypt",
  spec = {
    acme = {
      email = "ops@example.com",
      dns01 = { cloudflare = { apiToken = { kv = "secrets/cloudflare-dns" } } },
    },
  },
}
```

**3. Apply a Certificate**
([`configs/certs/certificate.ncl`](../configs/certs/certificate.ncl)):

```nickel
{
  kind = "certs/Certificate",
  name = "shop",
  namespace = "default",
  spec = {
    issuer = "letsencrypt",
    dnsNames = ["shop.example.com", "www.shop.example.com"],
    kv = "certs/shop",
  },
}
```

**4. Serve it** ([`configs/certs/web-https.ncl`](../configs/certs/web-https.ncl)):

```nickel
network = {
  ports = [
    { name = "https", static = 443, tls = { cert = { kv = "certs/shop" } } },
  ],
},
```

The node terminates TLS on 443 with the certificate and hands plain HTTP to
the task. When the certificate renews, about 60 days later, the node swaps
it in on its next heartbeat: open connections keep the old one, new ones
get the new one, nothing restarts
([Certificates and CAs from KV](network-handlers.md#certificates-and-cas-from-kv)).

Try `server = "letsencrypt-staging"` first if you are still getting DNS
right: its certificates are not trusted by browsers, but its rate limits
are generous. Changing `server` later re-issues every certificate the
Issuer issued.

## Issuers

A `certs/Issuer` is where certificates come from. It is cluster-scoped,
and is exactly one of `acme`, `ca`, and `selfSigned`:

| Field | Default | Meaning |
| --- | --- | --- |
| `namespaces` | `["*"]` | Namespaces (names or `*`-globs) whose Certificates may use it. `*` does not reach the reserved `legion-*` namespaces. A Certificate elsewhere reads `Rejected: IssuerNotAllowed`. |
| `acme` | | An ACME CA ([ACME](#acme)). |
| `ca` | | A private CA kept in KV ([A private CA](#a-private-ca)). |
| `selfSigned` | | `{}`: each certificate signs itself. For development, or for a client that pins the certificate. |

```console
$ legion object list issuers
NAME         STATUS  AGE  TYPE  SERVER
internal     Ready   3d   ca
letsencrypt  Ready   3d   acme  https://acme-v02.api.letsencrypt.org/directory
```

Its status fields: `type`; for `acme`, `server`, `accountUri`, `thumbprint`
(what HTTP-01 responders answer with), and `email`; for `ca`, `caSubject`,
`caNotAfter`, `caFingerprint`, and `generated`.

### ACME

```nickel
acme = {
  server = "letsencrypt",                 # | "letsencrypt-staging" | a directory URL
  email = "ops@example.com",              # optional
  eab = { kid = "...", hmacKey = { kv = "secrets/zerossl-eab" } },   # optional
  dns01 = {
    cloudflare = { apiToken = { kv = "secrets/cloudflare-dns" } },
    # or: webhook = { url = "https://dns-bridge.internal/acme", token = { kv = "secrets/dns-bridge" } },
    propagationDelay = "30s",
  },
  http01 = {},
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `server` | `letsencrypt` | `letsencrypt`, `letsencrypt-staging`, or any RFC 8555 directory URL. |
| `email` | none | A contact for the account. |
| `eab` | none | External account binding, for CAs that require one (ZeroSSL, Google Trust Services): the key id and, in KV, the MAC key as the CA gave it (base64url). Without it, such a CA's Issuer reads `SetupFailed: this ACME server requires external account binding`. |
| `dns01` | none | Prove names with a TXT record ([DNS-01](#dns-01)). |
| `http01` | none | `{}`: prove names over HTTP on port 80 ([HTTP-01](#http-01)). |

At least one of `dns01` and `http01` is required. With both, DNS-01 is
used for every name: it works for wildcards and needs nothing reachable
from the internet. Accepting the CA's terms of service is part of applying
the Issuer.

**The account.** Each ACME Issuer gets its own account key (ECDSA P-256),
generated the first time and kept in the app's own KV, at
`legion/apps/certs/acme/<issuer uid>.key`. Deleting and recreating an
Issuer makes a new account. The account key never leaves the app's KV
prefix, which a tenant's `paths = ["*"]` does not reach.

#### DNS-01

The CA looks up a TXT record at `_acme-challenge.<name>` (for
`*.example.com`, at `_acme-challenge.example.com`). The app publishes it,
waits `propagationDelay` (default `30s`, at most `10m`), asks the CA to
check, and removes it afterwards, whether the check passed or not. Every
name of a certificate is published first and covered by one wait.

| Provider | Setup |
| --- | --- |
| `cloudflare` | `apiToken = { kv }`: a Cloudflare API token with `Zone:Read` and `DNS:Edit` on the zone. The zone is found by walking up the name (`_acme-challenge.www.example.com` tries `www.example.com`, then `example.com`). Records are created with a TTL of 60. |
| `webhook` | `url`, and an optional `token = { kv }` sent as `Authorization: Bearer`. Any DNS API behind lego's [`httpreq`](https://go-acme.github.io/lego/dns/httpreq/) shape: `POST <url>/present` and `POST <url>/cleanup`, each with `{ "fqdn": "_acme-challenge.example.com.", "value": "..." }`; any `2xx` is success. |

The app reads the token from KV when it issues, acting for the Issuer, so a
rotated token is picked up on the next issuance with no change to the
Issuer.

#### HTTP-01

The CA fetches `http://<name>/.well-known/acme-challenge/<token>` on port
80, and expects `<token>.<thumbprint>`, where the thumbprint is the
Issuer's ACME account's. Legion has no cluster edge that knows which node a
name points at, so the cert manager does not try to answer itself. Instead,
because the thumbprint is public, **one stateless responder answers every
challenge**, for every name, with no secret and no list of pending
challenges:

```console
$ legion cert http01 --issuer letsencrypt --listen 0.0.0.0:80
```

Run it on every node your names point at, once per cluster
([`configs/certs/http01-responder.ncl`](../configs/certs/http01-responder.ncl)
runs one per node in the default pool, with `count = { num = 1, per = "id" }`):

| Request | Answer |
| --- | --- |
| `GET /.well-known/acme-challenge/<token>` | `200`, `<token>.<thumbprint>` (`503` until the thumbprint is known) |
| `GET /.well-known/legion-certs-http01` | `200 ok` once the thumbprint is known, `503` before: for a health check |
| anything else | `308` to `https://<host><path>` (`--no-redirect`: `404`) |

With `--issuer`, the responder reads the thumbprint from the Issuer's
status through `LEGION_ADDR` (in a job, the allocation's API socket), every
5 seconds until it has it and every minute after, and keeps answering with
the last one it read if the manager is unreachable. That needs
`object:read` on `certs/Issuer` in every namespace; the example job asks
for it in its `legion` identity. `--thumbprint` answers with a fixed one
instead and needs no access.

**Your own web server can answer instead.** In nginx, with the thumbprint
from `legion object get issuer letsencrypt`:

```nginx
location ~ "^/\.well-known/acme-challenge/([-_a-zA-Z0-9]+)$" {
  default_type text/plain;
  return 200 "$1.<thumbprint>";
}
```

HTTP-01 cannot prove a wildcard (a Certificate asking for one from an Issuer
without `dns01` reads `Rejected: WildcardNeedsDns01`), and it is the only
way an ACME CA proves an IP address (`IpNeedsHttp01` without it).

### A private CA

```nickel
{
  kind = "certs/Issuer",
  name = "internal",
  spec = {
    namespaces = ["prod-*"],
    ca = { kv = "certs/ca/internal", trust = "certs/ca/internal.pem" },
  },
}
```

| Field | Meaning |
| --- | --- |
| `ca.kv` | The CA: its certificate, any intermediates toward a root, then its PKCS#8 private key (`BEGIN PRIVATE KEY`). |
| `ca.trust` | Optional. A key the CA's certificate is published to, without its key, for clients to trust. Must differ from `ca.kv`. |

**Generated, or your own.** When `ca.kv` does not exist, the app generates a
CA there: ECDSA P-256, subject `O=Legion, CN=<issuer> CA`, valid for 10
years, able to sign leaves but not other CAs. The Issuer reads `Ready:
Generated`. To use a CA from your own PKI (an intermediate under your
root), put it in `ca.kv` before applying the Issuer:

```console
$ openssl pkcs8 -topk8 -nocrypt -in ca.key -out ca-pkcs8.key   # if it is not PKCS#8
$ legion kv put certs/ca/internal "$(cat ca.pem root.pem ca-pkcs8.key)"
```

Every time the app loads a CA it signs a probe certificate and verifies it
against the CA the way a client would, so a key that does not match, a CA
that is not a CA (`CA:TRUE`), or a subject the signer cannot reproduce is
reported on the Issuer (`SetupFailed: kv certs/ca/internal holds no usable
CA: ...`) before any Certificate is signed with it. ECDSA, Ed25519, and RSA
CA keys can sign; certificates the CA issues get ECDSA keys.

**Trusting it.** Point clients at `ca.trust`:

```nickel
# A mutual port: clients must present a certificate from this CA.
{ name = "api", static = 8443,
  tls = { cert = { kv = "certs/api" }, clients = { ca = { kv = "certs/ca/internal.pem" } } } },

# An upstream that verifies its server against this CA.
connect = { upstreams = [{ name = "db", to = "tls://db.internal:5432",
                           ca = { kv = "certs/ca/internal.pem" } }] },
```

**Re-reading it.** The app re-reads `ca.kv` every 5 minutes. Put a new CA
there and every certificate it issued is re-issued from the new one on its
next pass. Leaves never outlive their CA: a certificate's `notAfter` is cut
to the CA's. A CA that has expired reads `SetupFailed` and signs nothing.

**Who may use it.** A CA signs whatever names it is asked to. Anyone who may
write a Certificate in a namespace the Issuer serves can get a certificate
for any name from it, which clients that trust the CA will believe. Give
teams that do not trust each other their own CA Issuer, and limit each with
`namespaces`.

## Certificates

A `certs/Certificate` is namespaced:

```nickel
{
  kind = "certs/Certificate",
  name = "db",
  namespace = "prod-data",
  spec = {
    issuer = "internal",
    dnsNames = ["db.internal"],
    kv = "certs/db",
    duration = "720h",
    parts = { cert = "certs/db.crt", key = "certs/db.key", ca = "certs/db-ca.crt" },
  },
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `issuer` | required | The `certs/Issuer` that issues it. |
| `kv` | required | The KV key the certificate goes to: the chain, leaf first, then the private key (PKCS#8 PEM). |
| `dnsNames` | `[]` | DNS names. `*.example.com` is a wildcard, as the whole first label only. At most 100. |
| `ipAddresses` | `[]` | IP addresses. At most 100. A certificate names at least one DNS name or IP address. |
| `commonName` | the first name | The subject's CN (at most 64 characters). |
| `duration` | `2160h` (90 days) | How long it lasts, at least `1h` (`2160h`, `90d`). An ACME CA picks its own lifetime and ignores this. |
| `renewBefore` | a third of its lifetime | Renew this long before it expires. Shorter than `duration`. |
| `keyAlgorithm` | `ecdsa-p256` | `ecdsa-p256` or `ecdsa-p384`. A new key is made at every issuance. |
| `usages` | `["server", "client"]` | Extended key usages: authenticating a TLS server, a TLS client, or both. An ACME CA picks its own. |
| `parts` | none | Also write the parts to keys of their own: `cert` (the chain), `key` (the private key), `ca` (the issuing CA; for a self-signed certificate, itself). |

### Using it

| How the workload terminates TLS | What to write |
| --- | --- |
| The node does it (the task speaks plain HTTP) | `ports[].tls.cert = { kv = "<kv>" }` ([TLS on ports](network-handlers.md#tls-on-ports)). The task never holds the key. |
| The task does it, from one file (nginx, HAProxy) | A `files` entry rendering `{{ kv "<kv>" }}` with `kv = { watch = true, onChange = "update" }` and `changeMode = "signal"` ([Live file updates](kv-store.md#live-file-updates)). |
| The task does it, from separate files (Postgres, Redis) | `parts`, and one `files` entry per part, the same way. |
| A client that verifies a private CA | `ca = { kv = "<the Issuer's trust key>" }` on an upstream, or `tls.clients.ca` on a mutual port. |

[`configs/certs/`](../configs/certs) has each as a runnable file, and
[Serving TLS certificates](guides/tls-certificates.md) walks through the
two delivery routes. Prefer the node route where you can: the file route
renders the private key into the stored job config and the worker's state
file, which are not encrypted at rest.

The bundle is written in one KV put, so nothing ever reads a new certificate
with an old key. `parts` are written one after another, key first, so a
reader of two parts can briefly see a new key with the old certificate; a
file update's `splay` and the task's reload absorb that, and the bundle
never has the problem.

### When it is issued

The app looks at every Certificate when it changes, every 5 minutes, and at
its renewal time, and issues a new certificate (with a new key) when:

- `kv` holds no certificate, one that does not parse, or one that has
  expired;
- the certificate in `kv` names other names or has another key algorithm
  than the spec asks for;
- `kv` holds a certificate this Certificate did not issue (someone put
  another one there);
- the spec changed in a way that changes the certificate (its names, common
  name, duration, key algorithm, usages, or issuer; not `kv`, `parts`, or
  `renewBefore`);
- the Issuer changed what it issues from: another CA in `ca.kv`, or another
  ACME `server`;
- it is past its renewal time (`notAfter` minus `renewBefore`, or two thirds
  of the way through its life);
- a renewal was requested (`legion cert renew`, below).

Otherwise the certificate in KV stays, and the app only checks that the
`parts` keys still hold its parts, writing any that do not.

### Status

```console
$ legion object get certificate shop -n default
certs/Certificate default/shop (valid)  Ready (Issued: valid until 2027-02-05T10:12:00Z; renews at 2027-01-06T10:12:00Z)
...
status (reported by certs 2m ago):
  Accepted=true
  Ready=true (Issued): valid until 2027-02-05T10:12:00Z; renews at 2027-01-06T10:12:00Z
  fields: {"issuer":"letsencrypt","kv":"certs/shop","names":["shop.example.com","www.shop.example.com"],"notAfter":"2027-02-05T10:12:00Z",...}
```

| Reason | Summary | When |
| --- | --- | --- |
| `Issued` | `Ready` | The certificate in KV is current. |
| `RenewalFailing` | `Ready` | It is due, the last attempt failed, and the certificate in KV is still valid and still served. The message has the failure, the expiry, and the next attempt. |
| `IssueFailed` | `NotReady` | An attempt failed and there is no valid certificate in KV. |
| `IssuerNotFound` | `NotReady` | No Issuer has that name (looked for again every 15 seconds). |
| `IssuerNotReady` | `NotReady` | The Issuer cannot issue (its CA does not load, its account does not register). |
| `KvUnreadable` | `NotReady` | The app could not read `kv` for the Certificate (its writer's access to the key, as recorded, does not reach it). |
| `PartsNotWritten` | `NotReady` | The certificate is current, but a `parts` key could not be written; tried again every minute. |
| `IssuerNotAllowed` | `Rejected` | The Issuer's `namespaces` does not include this one. |
| `WildcardNeedsDns01`, `IpNeedsHttp01` | `Rejected` | The ACME Issuer cannot prove a name. |
| `InvalidSpec` | `Rejected` | A name, duration, or `renewBefore` the schema could not catch. |

The status fields: `issuer`, `kv`, `names`, `notBefore`, `notAfter`,
`renewalTime`, `serial`, `revision` (how many certificates the app has
issued for it), `failedAttempts`, `lastFailure`, `lastFailureTime`,
`nextAttemptTime`, and the app's own bookkeeping (`specHash`,
`issuerIdentity`, `renewRequest`). Status reports are written only when
something changed, and `object.status_changed` events fire when `Ready`
changes.

### Failures and rate limits

A failed issuance keeps the certificate KV holds, so a renewal that fails
does not take anything down until the certificate actually expires: with the
default renewal at two thirds of a 90-day certificate, that is 30 days of
retries. Attempts back off from 5 minutes, doubling, to at most 6 hours, so
a broken DNS token makes at most four attempts in any first hour, under
Let's Encrypt's five failed validations per name per hour. The backoff
survives an app restart (it is read back from status). A change to the
spec, the Issuer, or a renewal request is a new attempt and does not wait
out the previous attempt's backoff.

### Renewing now

```console
$ legion cert renew shop -n default --wait
certs/Certificate default/shop: renewal requested (certs/renew=2026-11-07T10:00:00.000Z); the certs app re-issues it within seconds
certs/Certificate default/shop: renewed (serial 4f0c..., valid until 2027-02-05T10:12:00Z)
```

`legion cert renew` sets the label `certs/renew` to the current time with
an ordinary apply (over the version it read, so it never undoes someone's
change), and the app issues once for each new value. Renewing needs what
writing the Certificate needs. Setting the label yourself with `legion
object apply` does the same. `--wait` waits (default `2m`, `--timeout`)
until the app reports the new certificate, or says why it could not.

### Deleting

Deleting a Certificate stops renewing it. The certificate stays in KV, and
a job that serves it keeps serving it until it expires; `legion kv delete`
removes it. Deleting an Issuer leaves its Certificates `NotReady:
IssuerNotFound`.

## Access

Both kinds are governed by the [`object` ACL resource](app-objects.md#access):
writing a Certificate takes `object:write` on `certs/Certificate` in its
namespace, and writing an Issuer, which is cluster-scoped, takes
`object:write` on `certs/Issuer` with `namespaces = ["*"]`:

```nickel
# A team that manages its own certificates under certs/team-a/.
[
  { resources = ["object"], actions = ["read", "write"], namespaces = ["team-a"], kinds = ["certs/Certificate"] },
  { resources = ["kv"], actions = ["read", "write"], paths = ["certs/team-a/*"] },
]
```

**A certificate request cannot reach further than its writer.** Each kind
`requires` the keys it names of whoever writes it:

| Kind | Writing it also requires |
| --- | --- |
| `certs/Certificate` | `kv` read and write on `kv`, and on each `parts` key |
| `certs/Issuer` | `kv` read and write on `ca.kv` and `ca.trust`; `kv` read on `dns01.cloudflare.apiToken.kv`, `dns01.webhook.token.kv`, and `eab.hmacKey.kv` |

```text
Error: writing certs/Certificate team-a/web needs kv:read on certs/elsewhere (from spec.kv), which your token does not hold
Hint: the kind requires it of whoever writes its objects (`legion object explain certs/Certificate`); have someone who holds it make the change
```

**The app acts for each object.** The cert manager's grant is two rules:

```nickel
{ resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/certs/*"] },
{ resources = ["kv"], actions = ["read", "write"], paths = ["*"], via = "objects" },
```

Its own credential reaches only its own KV prefix (the ACME account keys).
Every other key it reads or writes, it does acting for the object that names
it ([Acting for an object](app-objects.md#acting-for-an-object)): a
Certificate's keys within what that Certificate's writer was checked for, an
Issuer's CA and tokens within what the Issuer's writer was checked for. So a
Certificate can never make the app write a key its writer could not, and
the app holds no standing access to any secret. The `*` does not reach the
reserved `legion/` prefix.

The CA key is read for the Issuer, not for the Certificate: a Certificate's
writer never needs (and never gets) access to the CA key to have a
certificate signed by it. That is what an Issuer's `namespaces` limits.

## In the UI

The app adds a **Certificates** page to the sidebar's Apps section: counts
(ready, renewing within 14 days, failing), every certificate with its names,
issuer, state, expiry, and next renewal (failing ones first), and every
Issuer. The cluster overview gets a Certificates card with the same counts
and the certificates that need attention. Both show what the viewer may
read: certificates in the namespaces where they hold `object:read`, and
Issuers to viewers who hold it in every namespace.

```console
$ legion app view certs nav --view certificates
```

## Agents

Issuers and Certificates are objects, so the [object tools](agent-readiness.md)
cover them: `legion_object_kinds` returns both kinds with their schemas and
examples, `legion_object_apply` (with `wait`) requests a certificate, and
`legion_object_get` reads its status and reason. `legion object explain
certs/Certificate.spec.renewBefore` describes a field. `legion cert renew -j`
returns the standard JSON envelope.

## Reference

| CLI | What it does |
| --- | --- |
| `legion object apply -f <file>` | Create or update an Issuer or a Certificate |
| `legion object list certificates [-A]`, `legion object list issuers` | List them, with their expiry |
| `legion object get certificate <name> [-n NS]` | One Certificate's status |
| `legion object delete certificate <name>` | Stop renewing it |
| `legion cert renew <name> [-n NS] [--wait]` | Re-issue it now |
| `legion cert http01 (--issuer I \| --thumbprint T) [--listen ADDR] [--no-redirect]` | The HTTP-01 responder |
| `legion app status certs`, `legion app logs certs` | The app itself |

The app's config takes no fields: everything lives on the objects.

## Limitations

- **ECDSA keys only** for the certificates it issues (`ecdsa-p256`,
  `ecdsa-p384`): Legion's crypto library cannot generate RSA keys. Clients
  that cannot do ECDSA need a certificate from elsewhere.
- **Two DNS-01 providers**: Cloudflare and the webhook. Route 53, Google
  Cloud DNS, RFC 2136, and the rest need a webhook bridge.
- **No DNS self-check.** The app waits `propagationDelay` and then asks the
  CA, rather than checking the record itself first. A provider that is slow
  to publish needs a longer delay.
- **HTTP-01 needs a responder you run.** Nothing in Legion knows which nodes
  a name points at, so HTTP-01 works only where port 80 reaches a `legion
  cert http01` responder (or a server answering with the thumbprint).
  TLS-ALPN-01 is not supported.
- **Certificates are requested by objects, not by jobs.** There is no
  jobspec key that requests a certificate for a port (cert-manager's
  ingress-shim): apply a Certificate, and name its key in the job.
- **One replica.** The app runs one replica; `legion app set certs
  --replicas 2` would issue every certificate twice.
- **No revocation**, and no ACME Renewal Information (ARI): renewal is on a
  schedule, not when the CA asks.
- **A CA is not rotated for you.** A generated CA lasts 10 years; replacing
  it means putting a new one in `ca.kv` (every certificate is then
  re-issued) and making clients trust it before the old one goes.
- **Names are not restricted per Issuer.** `namespaces` limits who may use
  an Issuer, not which names they may ask for.
- **The page shows certificates by namespace, not by kind.** A viewer with
  `object:read` in a namespace for another app's kinds sees that namespace's
  certificates on the Certificates page (names and expiry, never keys).
- **Deleting leaves the key.** A deleted Certificate's certificate stays in
  KV until someone deletes it.
- **A stopped app renews nothing.** `legion app disable certs` stops
  renewal; certificates already in KV keep serving until they expire.
