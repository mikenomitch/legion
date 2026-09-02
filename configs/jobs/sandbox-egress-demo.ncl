# Egress policy on the connect datapath (docs/network-handlers.md).
#
#   cargo run -- job run configs/jobs/sandbox-egress-demo.ncl
#   cargo run -- net policy show sandbox-egress-demo
#   cargo run -- net policy test sandbox-egress-demo --to https://api.github.com/user
#
# The `agent` task reaches the public internet only through two named
# upstreams. Each is a loopback port on the node; Legion originates TLS, so
# the task speaks plaintext HTTP to $LEGION_UPSTREAM_GITHUB and the body is
# visible to the egress policy without anyone's TLS being intercepted.
{
    name = "sandbox-egress-demo",
    tasks = [
        {
            name = "agent",
            runtime = "exec",
            config = {
                # Print the two gateways the task was handed, then keep running.
                command = [
                    "sh",
                    "-c",
                    "echo github=$LEGION_UPSTREAM_GITHUB pypi=$LEGION_UPSTREAM_PYPI; sleep 3600",
                ],
            },
            network = {
                labels = { user = "alice", tier = "free" },
                egress = {
                    protocol = "http",
                    policy = "deny",
                    allow = [
                        "api.github.com/repos/**",
                        "pypi.org",
                        "files.pythonhosted.org",
                    ],
                    rules = [
                        {
                            match = { header = { authorization = "*" } },
                            action = "deny",
                            reason = "credentials must not leave the sandbox",
                        },
                    ],
                    accessLog = true,
                },
            },
            connect = {
                upstreams = [
                    # External upstreams: `name` + `to`. The scheme decides
                    # whether Legion originates TLS (https/tls) and parses
                    # HTTP (http/https).
                    { name = "github", to = "https://api.github.com" },
                    { name = "pypi", to = "https://pypi.org" },
                ],
            },
        },
    ],
}
