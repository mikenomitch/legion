# Serving a KV-held certificate, two ways. Pairs with
# configs/jobs/cert-renewer.ncl, which keeps `certs/app.example.com` renewed.
#
# The KV value is one PEM bundle: the certificate chain, then the private key.
# Put one by hand to try this without the renewer:
#   legion kv put certs/app.example.com "$(cat fullchain.pem privkey.pem)"
#
# 1. `app`: TLS from the node datapath (`certFrom = { kv }`).
#    The app speaks plain HTTP on a private port. The node terminates TLS on
#    the advertised port with the certificate from KV, so the app never holds
#    the key. On a `kv put`, new connections get the new certificate and open
#    ones keep theirs. No restart, no redeploy.
#
# 2. `nginx`: TLS in the task, from a file (`onChange = "update"`).
#    The bundle is rendered into the task directory. On a `kv put`, the file
#    is rewritten in place (atomically, mode 0400) and nginx gets SIGHUP,
#    which reloads it gracefully. The allocation keeps running.
#
# Either way, a bad value (no key, key does not match) never takes a listener
# down: the datapath keeps the last good certificate and reports the problem,
# and `legion net doctor tls-edge` says what to fix.
#
# Submitting this job needs `kv:read` on certs/app.example.com, since it hands
# that key to the datapath.
#
# Run and inspect:
#   legion job run configs/jobs/tls-edge.ncl
#   curl -v https://app.example.com:8443/     # served by the datapath
#   curl -v https://app.example.com:9443/     # served by nginx
#   legion net doctor tls-edge --task app
{
  name = "tls-edge",
  tasks = [
    {
      name = "app",
      runtime = "exec",
      lifecycle = "service",
      config = {
        # Any plain HTTP server. It binds LEGION_PORT_HTTPS, which is a
        # private port: the node owns the advertised 8443 and splices
        # decrypted connections through to it.
        command = ["sh", "-c", "exec python3 -m http.server \"$LEGION_PORT_HTTPS\""],
      },
      network = {
        ports = [{ name = "https", static = 8443 }],
        ingress = {
          tls = {
            mode = "server",
            certFrom = { kv = "certs/app.example.com" },
          },
        },
      },
      resources = { cpus = 0.1, memory = 64 },
    },
    {
      name = "nginx",
      runtime = "container",
      lifecycle = "service",
      config = {
        image = "nginx:1.27-alpine",
        command = ["nginx", "-c", "/legion/task/nginx.conf", "-g", "daemon off;"],
      },
      network = {
        # nginx listens on the advertised port itself. A `to` remap would
        # only apply on macOS (Apple container); on Linux the container
        # shares the host's network, so it must bind 9443 directly.
        ports = [{ name = "https", static = 9443 }],
      },
      files = [
        {
          # nginx reads the certificate and the key from the same file.
          path = "/legion/task/tls/bundle.pem",
          mode = "0400",
          file = { template = "{{ kv \"certs/app.example.com\" }}" },
          kv = {
            watch = true,
            # Rewrite the file under the running task instead of replacing
            # the allocation...
            onChange = "update",
            # ...and keep serving the last certificate if the key is deleted,
            # rather than failing the job.
            onDelete = "none",
          },
          # ...then tell nginx to reload. A fleet spreads its reloads over
          # up to 10s.
          changeMode = "signal",
          changeSignal = "SIGHUP",
          splay = "10s",
        },
        {
          # Static config: a template with no KV lookups is just a file.
          path = "/legion/task/nginx.conf",
          file = {
            template = m%"
              events {}
              http {
                server {
                  listen 9443 ssl;
                  ssl_certificate     /legion/task/tls/bundle.pem;
                  ssl_certificate_key /legion/task/tls/bundle.pem;
                  location / { return 200 "hello over TLS\n"; }
                }
              }
            "%,
          },
        },
      ],
      resources = { cpus = 0.2, memory = 128 },
    },
  ],
}
