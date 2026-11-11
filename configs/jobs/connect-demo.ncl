# Connect demo: two `web` backends behind ONE local address, plus a client that
# dials them through the node-local forwarder and load-balances across them.
#
# Everything here is stock `sh` + `python3` + `curl`, so it runs unchanged on
# macOS and Linux.
#
#   legion job run configs/jobs/connect-demo.ncl
#   legion service resolve web             # two healthy endpoints
#   legion service doctor web              # incl. the service.allow list below
#   legion job logs connect-demo --tail    # client alternating between them
#
# The client never learns a backend address: it dials $LEGION_UPSTREAM_WEB
# (its upstream address: a VIP on Linux, a loopback port elsewhere) and the worker forwards each connection to a
# healthy endpoint of `web`. See docs/service-discovery.md.
{
    name = "connect-demo",
    tasks = [
        {
            name = "web",
            # Two allocations, so the balancing is actually observable.
            count = 2,
            runtime = "exec",
            lifecycle = "service",
            config = {
                # Serve a one-line index.html naming this allocation, so the
                # client's log shows WHICH backend answered each request.
                # No mktemp: BSD and GNU disagree about template-less usage.
                command = [
                    "sh",
                    "-c",
                    m%"
                        d="${TMPDIR:-/tmp}/legion-connect-demo-$LEGION_ALLOC_ID"
                        mkdir -p "$d"
                        echo "hello from web alloc $LEGION_ALLOC_ID" > "$d/index.html"
                        cd "$d"
                        exec python3 -m http.server "$LEGION_PORT_HTTP"
"%,
                ],
            },
            network = {
                ports = [{ name = "http" }],  # no static -> dynamic port assigned
            },
            service = {
                port = "http",
                tags = ["demo"],
                # Connect intentions: only these job ids may reach `web`
                # through an upstream. The client below lives in this job, so
                # it is permitted. Drop the field (or use ["*"]) to allow every
                # job; a denied consumer just gets an empty endpoint list.
                allow = ["connect-demo"],
            },
            checks = [
                {
                    type = "health",
                    kind = "tcp",
                    port = "http",   # label -> resolves to the assigned dynamic port
                    every = "5s",
                    timeout = "2s",
                    unhealthyAfterCount = 2,
                },
            ],
            resources = { memory = 128 },
        },
        {
            name = "client",
            runtime = "exec",
            lifecycle = "service",
            # The whole consumer-side surface: one upstream, one env var.
            connect = {
                upstreams = [
                    {
                        to = "web",
                        # Even spread, so the demo visibly alternates. Omit
                        # `balance` for the default, least_latency;
                        # "least_conn" and "local_first" are the others.
                        balance = "round_robin",
                        # The task dials $LEGION_UPSTREAM_WEB. Pin a port
                        # only for clients that need a literal
                        # 127.0.0.1:<port>:
                        # port = 8081,
                    },
                ],
            },
            config = {
                # Until `web` has a healthy endpoint the forwarder closes the
                # connection immediately, so curl fails fast and the loop says
                # so — that is the honest "no endpoints yet" signal, not a hang.
                command = [
                    "sh",
                    "-c",
                    m%"
                        echo "client dialing $LEGION_UPSTREAM_WEB"
                        while true; do
                            curl -s --max-time 5 "http://$LEGION_UPSTREAM_WEB/" || echo "no healthy web endpoint yet"
                            sleep 2
                        done
"%,
                ],
            },
            resources = { memory = 64 },
        },
    ],
}
