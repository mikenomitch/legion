# A long-running web service in a container, on a dynamic host port.
#
#   legion job run configs/jobs/container-service.ncl
#   curl "http://$(legion service resolve nginx --one)"
#
# Linux containers share the host network, so nginx listens on the port
# Legion assigned ($LEGION_PORT_HTTP) instead of 80. Two replicas on one
# node would otherwise collide.
{
  name = "container-service",
  task = {
    name = "nginx",
    runtime = "container",
    lifecycle = "service",
    count = 2,
    config = {
      image = "nginx:1.27-alpine",
      command = [
        "sh",
        "-c",
        "sed -i \"s/listen  *80;/listen $LEGION_PORT_HTTP;/\" /etc/nginx/conf.d/default.conf && exec nginx -g 'daemon off;'",
      ],
    },
    network = { ports = [{ name = "http" }] },
    service = { port = "http", meta = { protocol = "http" } },
    checks = [
      { type = "health", kind = "http", path = "/", port = "http", every = "5s", timeout = "2s" },
    ],
    resources = { cpus = 0.25, memory = 128 },
  },
}
