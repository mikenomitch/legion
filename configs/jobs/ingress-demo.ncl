# Ingress demo: two services behind one public hostname (docs/ingress.md).
#
# Turn the ingress on once per cluster, then run this job:
#   legion app enable ingress
#   legion job run configs/jobs/ingress-demo.ncl
#
# Try it before DNS points anywhere, against a node the ingress runs on:
#   curl -H 'Host: demo.example.com' http://<node>/            # the site
#   curl -H 'Host: demo.example.com' http://<node>/api/        # the API, /api stripped
#   legion app view ingress nav                                # routes, endpoints, certificates
#
# Point demo.example.com where `legion service resolve legion-system/ingress`
# says (the Ingress page's "Point DNS here" shows the same), and the cert
# manager gets it a Let's Encrypt certificate: it is served over HTTPS, and
# plain HTTP redirects to it.
{
    name = "ingress-demo",
    tasks = [
        {
            name = "site",
            runtime = "exec",
            lifecycle = "service",
            count = 2,
            config = {
                command = ["sh", "-c", "exec python3 -m http.server $LEGION_PORT_HTTP"],
            },
            network = { ports = [{ name = "http" }] },
            service = {
                port = "http",
                # Everything on demo.example.com (and www.) not claimed by a
                # longer path.
                ingress = { host = ["demo.example.com", "www.demo.example.com"] },
            },
            checks = [
                { type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" },
            ],
            resources = { memory = 128 },
        },
        {
            name = "api",
            runtime = "exec",
            lifecycle = "service",
            config = {
                command = ["sh", "-c", "exec python3 -m http.server $LEGION_PORT_HTTP"],
            },
            network = { ports = [{ name = "http" }] },
            service = {
                port = "http",
                # /api and below, arriving at the service without the prefix.
                ingress = { host = "demo.example.com", path = "/api", stripPath = true },
            },
            checks = [
                { type = "health", kind = "http", path = "/", port = "http", every = "5s", timeout = "2s" },
            ],
            resources = { memory = 128 },
        },
    ],
}
