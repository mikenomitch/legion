# A web service served over HTTPS with the certificate the cert manager
# keeps in certs/shop (configs/certs/certificate.ncl).
#
# The node terminates TLS on 443 with the certificate from KV and splices
# plain HTTP to the task's private port, so the task holds no key and needs
# no TLS code. A renewal reaches the node on its next heartbeat; open
# connections keep the old certificate, new ones get the new one.
#
# Submitting it needs kv:read on certs/shop (it hands that key to the
# datapath).
#
#   legion job run configs/certs/web-https.ncl
#   curl -v https://shop.example.com/
{
  name = "shop",
  task = {
    name = "web",
    runtime = "exec",
    lifecycle = "service",
    config = {
      command = ["sh", "-c", "exec python3 -m http.server \"$LEGION_PORT_HTTPS\""],
    },
    network = {
      ports = [
        { name = "https", static = 443, tls = { cert = { kv = "certs/shop" } } },
      ],
    },
    resources = { cpu = 100, memory = 64 },
  },
}
