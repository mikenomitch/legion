// Connect demo: two `web` backends behind ONE local address, plus a client that
// dials them through the node-local forwarder and load-balances across them.
//
// Everything here is stock `sh` + `python3` + `curl`, so it runs unchanged on
// macOS and Linux.
//
//   legion job run configs/jobs/connect-demo.jsonnet
//   legion service resolve web             # two healthy endpoints
//   legion service doctor web              # incl. the service.allow list below
//   legion job logs connect-demo --tail    # client alternating between them
//
// The client never learns a backend address: it dials $LEGION_UPSTREAM_WEB
// (127.0.0.1:<assigned port>) and the worker forwards each connection to a
// healthy endpoint of `web`. See docs/service-discovery.md.
{
    name: "connect-demo",
    tasks: [
        {
            name: "web",
            // Two allocations, so the balancing is actually observable.
            count: 2,
            kind: "exec",
            lifecycle: "service",
            config: {
                // Serve a one-line index.html naming this allocation, so the
                // client's log shows WHICH backend answered each request.
                // No mktemp: BSD and GNU disagree about template-less usage.
                command: [
                    "sh",
                    "-c",
                    |||
                        d="${TMPDIR:-/tmp}/legion-connect-demo-$LEGION_ALLOC_ID"
                        mkdir -p "$d"
                        echo "hello from web alloc $LEGION_ALLOC_ID" > "$d/index.html"
                        cd "$d"
                        exec python3 -m http.server "$LEGION_PORT_HTTP"
                    |||,
                ],
            },
            network: {
                ports: [{ name: "http" }],  // no static -> dynamic port assigned
            },
            service: {
                port: "http",
                tags: ["demo"],
                // Connect intentions: only these job ids may reach `web`
                // through an upstream. The client below lives in this job, so
                // it is permitted. Drop the field (or use ["*"]) to allow every
                // job; a denied consumer just gets an empty endpoint list.
                allow: ["connect-demo"],
            },
            checks: [
                {
                    type: "health",
                    kind: "tcp",
                    port: "http",   // label -> resolves to the assigned dynamic port
                    every: "5s",
                    timeout: "2s",
                    unhealthy_after_count: 2,
                },
            ],
            resources: { memory: 128 },
        },
        {
            name: "client",
            kind: "exec",
            lifecycle: "service",
            // The whole consumer-side surface: one upstream, one env var.
            connect: {
                upstreams: [
                    {
                        service: "web",
                        // The default; "random" and "least_conn" are the other
                        // built-ins. Omit `balance` to get round_robin.
                        balance: "round_robin",
                        // Without localPort the loopback port is assigned at
                        // placement out of the same per-node range as host
                        // ports. Pin it only for clients that cannot read env
                        // vars:
                        // localPort: 8081,
                    },
                ],
            },
            config: {
                // Until `web` has a healthy endpoint the forwarder closes the
                // connection immediately, so curl fails fast and the loop says
                // so — that is the honest "no endpoints yet" signal, not a hang.
                command: [
                    "sh",
                    "-c",
                    |||
                        echo "client dialing $LEGION_UPSTREAM_WEB"
                        while true; do
                            curl -s --max-time 5 "http://$LEGION_UPSTREAM_WEB/" || echo "no healthy web endpoint yet"
                            sleep 2
                        done
                    |||,
                ],
            },
            resources: { memory: 64 },
        },
    ],
}
