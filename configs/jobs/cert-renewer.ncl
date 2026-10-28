# Certificate renewer: keeps a Let's Encrypt certificate for one domain in
# Legion KV, and renews it before it expires.
#
# What it does, every 12 hours:
#   1. Reads the current certificate from KV and exits if it is still good
#      for more than 30 days. Most runs stop here.
#   2. Otherwise asks Let's Encrypt for a new one with lego, proving domain
#      ownership through a DNS TXT record (DNS-01). Nothing has to be
#      reachable from the internet, and wildcards work.
#   3. Writes the chain and the private key into KV as ONE value, so readers
#      never see a new certificate paired with an old key.
#
# Consumers pick the new value up on their own: see configs/jobs/tls-edge.ncl,
# which serves it straight from the node datapath (`ports[].tls.cert = { kv }`) and
# also hands it to nginx as a file that is rewritten in place on renewal
# (`onChange = "update"`, then SIGHUP).
#
# Why it is resilient without being clever:
#   - KV is the only state. The task keeps nothing between runs, so it can be
#     rescheduled to any node, and a lost node loses nothing.
#   - A failed run exits non-zero and changes nothing in KV: consumers keep
#     serving the current certificate, and the next run (12h later) tries
#     again. With a 30-day renewal window that is ~60 attempts before expiry.
#   - The KV write is a single put of a complete bundle. Legion's datapath
#     also validates a bundle before using it, and keeps the previous
#     certificate if a bad one ever lands.
#
# Before you run it:
#   - The worker node needs `legion` on PATH and `openssl`.
#   - Workload identity enabled on the manager (`workloadIdentity.enabled =
#     true`). Nothing else on the manager: no binding, no policy, no address.
#     The job asks for what it needs (`permissions` on its `legion` identity,
#     below), and Legion grants it at submit because the token that submits
#     the job holds it (docs/workload-identity.md, "Permissions").
#   - Submit it with a token that holds `kv:read` and `kv:write` on `certs/*`
#     (and `kv:read` on the DNS token below, which the env template reads).
#     A token that does not is refused before anything is stored:
#
#       Error: failed to run job: cert-renewer-app-example-com: identity
#       'legion' asks for kv:write on certs/*, which your token does not hold
#       [code=PermissionDenied]
#       Hint: have someone with that access submit this job, or ask for it
#       to be granted to you
#
#   - Inside the task, `legion` reaches the cluster through the worker's
#     per-allocation socket: the worker sets
#     LEGION_ADDR=unix://$LEGION_SECRETS_DIR/legion.sock and attaches this
#     allocation's current identity token to every call, so the task holds
#     no token and needs no route to a manager.
#   - A Cloudflare API token with Zone.DNS edit rights, stored in KV:
#       legion kv put secrets/cloudflare-dns-token "<token>"
#     (lego supports 150+ DNS providers; swap `--dns cloudflare` and the env
#     var for yours: https://go-acme.github.io/lego/dns/)
#
# Run and inspect:
#   legion job run configs/jobs/cert-renewer.ncl
#   legion acl grants list --job cert-renewer-app-example-com
#   legion job logs cert-renewer-app-example-com
#   legion kv get certs/app.example.com
#
# Test against Let's Encrypt staging first (untrusted certs, generous rate
# limits): set ACME_SERVER to the staging URL below.
let domain = "app.example.com" in
let email = "ops@example.com" in
{
  name = "cert-renewer-app-example-com",
  task = {
    name = "renew",
    runtime = "exec",
    lifecycle = "batch",
    # Twice a day. Cheap when there is nothing to do: one KV read and one
    # openssl check.
    schedule = { every = "12h" },

    # The ACME client. Pin a current release from
    # https://github.com/go-acme/lego/releases (and add its `checksum` to make
    # the download immutable). Archives unpack into the task directory.
    artifacts = [
      "https://github.com/go-acme/lego/releases/download/v4.21.0/lego_v4.21.0_linux_amd64.tar.gz",
    ],

    # Talking to Legion itself, as this job. `permissions` is what the job
    # may do (the same rule shape `legion acl policy apply` takes); the
    # submitter must hold it. The worker serves the allocation a local API
    # socket that authenticates as this identity and refreshes it on its
    # own, so nothing here restarts when the token rotates.
    identities = [
      {
        name = "legion",
        permissions = [
          { resources = ["kv"], actions = ["read", "write"], paths = ["certs/*"] },
        ],
      },
    ],

    # The DNS provider credential, read from KV when the run starts.
    env = [
      {
        name = "CLOUDFLARE_DNS_API_TOKEN",
        value = { template = "{{ kv \"secrets/cloudflare-dns-token\" }}" },
      },
    ],

    config = {
      env = {
        DOMAIN = domain,
        EMAIL = email,
        KV_KEY = "certs/%{domain}",
        # Renew when fewer than this many days remain.
        RENEW_DAYS = "30",
        # Staging: https://acme-staging-v02.api.letsencrypt.org/directory
        ACME_SERVER = "https://acme-v02.api.letsencrypt.org/directory",
      },
      command = ["sh", "-c", m%"
        set -eu

        # 1. Is the certificate in KV still good? `kv get` prints the value
        #    after a "Value:" line; openssl reads the first certificate in the
        #    bundle (the leaf) and ignores the key after it.
        current="$(legion kv get "$KV_KEY" 2>/dev/null | sed '1,/^Value:$/d' || true)"
        if [ -n "$current" ] && printf '%s\n' "$current" |
            openssl x509 -noout -checkend $((RENEW_DAYS * 86400)) >/dev/null 2>&1; then
          echo "certificate for $DOMAIN is valid for more than $RENEW_DAYS days; nothing to do"
          exit 0
        fi

        # 2. Issue a new one. A fresh ACME account each time keeps the task
        #    stateless; Let's Encrypt allows that comfortably at this cadence.
        echo "issuing a certificate for $DOMAIN"
        ./lego --accept-tos --email "$EMAIL" --server "$ACME_SERVER" \
          --dns cloudflare --domains "$DOMAIN" --path ./.lego run

        # 3. One value: the full chain, then the key. Check both halves are
        #    there before touching KV, so a broken run can never overwrite a
        #    good certificate.
        crt="./.lego/certificates/$DOMAIN.crt"
        key="./.lego/certificates/$DOMAIN.key"
        openssl x509 -noout -in "$crt"
        openssl pkey -noout -in "$key"
        legion kv put "$KV_KEY" "$(cat "$crt" "$key")"
        echo "stored a new certificate for $DOMAIN in kv $KV_KEY"
      "%],
    },
    resources = { cpus = 0.1, memory = 64 },
  },
}
