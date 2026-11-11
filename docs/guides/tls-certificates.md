# Serving TLS Certificates from KV

A public TLS certificate has to be renewed every few months, and that window
keeps shrinking. If renewing means redeploying, or copying files onto every
node a task might land on, it eventually gets missed and a certificate
expires in production.

Legion keeps the certificate in KV instead. KV values are encrypted at rest
and scoped by ACL path, and two features turn a `kv put` into a live
rotation:

- **The node serves it** (`ports[].tls.cert = { kv }`). The node terminates TLS on
  the task's advertised port with the certificate from KV, and swaps it when
  the key changes. The task speaks plain HTTP and never holds the key.
- **The task gets it as a file** (`onChange = "update"`). For a task that
  terminates TLS itself, such as nginx, the certificate is rendered into a
  file. On a change the file is rewritten in place and the task is signaled
  to reload.

Neither restarts anything. Getting the certificate into KV, and renewing
it, is the bundled [cert manager](../cert-manager.md)'s job: apply a
`certs/Certificate` naming the key, and it keeps a Let's Encrypt (or
private-CA) certificate there. If you would rather run your own ACME
client, a small scheduled job does it ([below](#renew-it-yourself)).

Runnable specs:
[`configs/certs/`](../../configs/certs),
[`configs/jobs/tls-edge.ncl`](../../configs/jobs/tls-edge.ncl), and
[`configs/jobs/cert-renewer.ncl`](../../configs/jobs/cert-renewer.ncl).

## The format

One KV key holds one PEM bundle: the certificate chain, leaf first, then the
private key. One key keeps the pair atomic, so nothing ever reads a new
certificate with an old key.

```bash
legion kv put certs/app.example.com "$(cat fullchain.pem privkey.pem)"
```

That is all you need to try either consumer below without the cert
manager. With it, a Certificate puts the certificate there for you:

```nickel
{
  kind = "certs/Certificate",
  name = "app",
  namespace = "default",
  spec = { issuer = "letsencrypt", dnsNames = ["app.example.com"], kv = "certs/app.example.com" },
}
```

([Cert Manager](../cert-manager.md) sets up the `letsencrypt` Issuer.)

## Serve it from the node

```nickel
{
  name = "app",
  runtime = "exec",
  config = { command = ["sh", "-c", "exec python3 -m http.server \"$LEGION_PORT_HTTPS\""] },
  network = {
    ports = [
      { name = "https", static = 8443, tls = { cert = { kv = "certs/app.example.com" } } },
    ],
  },
}
```

The node owns port 8443 and the task binds a private port, so the task needs
no TLS code. On each heartbeat the node asks for any newer certificate. New
connections get it, and open ones keep the old one until they close.

Submitting this job needs `kv:read` on `certs/app.example.com` as well as
`job:write`, because the job hands that key to the datapath.

To make the port mutual, add the CA client certificates must verify against.
It can live in KV too, and rotates the same way:

```nickel
{ name = "https", static = 8443,
  tls = { cert = { kv = "certs/app.example.com" },
          clients = { ca = { kv = "certs/app-clients" } } } },
```

## Give it to the task

```nickel
files = [
  {
    path = "/legion/task/tls/bundle.pem",
    mode = "0400",
    file = { template = "{{ kv \"certs/app.example.com\" }}" },
    kv = { watch = true, onChange = "update", onDelete = "none" },
    changeMode = "signal",
    changeSignal = "SIGHUP",
    splay = "10s",
  },
],
```

nginx reads the certificate and the key from the same file
(`ssl_certificate` and `ssl_certificate_key` both point at it). On a change,
the worker rewrites the file atomically and sends `SIGHUP`, and nginx reloads
without dropping connections. `splay` spreads the reloads across a fleet.

## Renew it

The cert manager renews a Certificate two thirds of the way through its
life, writes the new bundle in one put, and both consumers above pick it up.
A failed renewal keeps the current certificate and retries with backoff;
`legion object get certificate app` says why, and `legion cert renew app`
renews now. See [Cert Manager](../cert-manager.md).

### Renew it yourself

To run your own ACME client instead, `cert-renewer.ncl` is a batch task that
runs every 12 hours:

1. It reads the current certificate from KV and exits if it is good for more
   than 30 days.
2. Otherwise it runs [lego](https://go-acme.github.io/lego/) with a DNS-01
   challenge, so nothing has to be reachable from the internet and wildcards
   work.
3. It writes the chain and key into KV in one put.

KV is its only state, so it can run on any node. A failed run changes
nothing, and the next one retries. With a 30-day window that is about 60
attempts before anything expires.

The job writes to KV as itself, using its `legion` workload identity. Grant it
with a control-plane binding to a policy that allows `kv` write on `certs/*`.
The comments at the top of the spec show the manager config.

## When something goes wrong

A bad value never takes a listener down. The manager checks each bundle
before sending it: it needs a certificate, a private key, and a key that
matches the leaf. If a renewal writes something broken, the node keeps
serving the previous certificate and the allocation reports it:

```bash
legion net doctor tls-edge --task app
```

```
tls  warn  allocation 4217: serving the previous certificate; the current kv certs/app.example.com value was rejected — kv certs/app.example.com holds no private key
```

An allocation that has never received a good certificate refuses
handshakes, and the doctor shows the `legion kv put` that fixes it.

## Limits

- A new certificate reaches nodes and tasks on their next heartbeat, a few
  seconds in a small cluster.
- A port's own `tls` and the mesh are exclusive: the service's mesh port
  (`service.mesh = true`) terminates with the node certificate instead.
- `onChange = "update"` is for files. Env values cannot change under a
  running process, so a KV-backed env var still needs `restart` or
  `rollout`.
- The file route renders the private key into the stored job config and the
  worker's state file, and those copies are not encrypted at rest (see
  [KV Encryption at Rest](../kv-encryption.md)). Prefer the node route
  (`ports[].tls.cert = { kv }`) where you can. It keeps the key in KV and in node
  memory only.
- The cert manager issues ECDSA keys only; an RSA certificate has to come
  from your own ACME client (the renewer job) or CA.

Reference: [Certificates and CAs from KV](../network-handlers.md#certificates-and-cas-from-kv)
and [Live file updates](../kv-store.md#live-file-updates).
