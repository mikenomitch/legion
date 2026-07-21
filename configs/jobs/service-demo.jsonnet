// Service discovery demo: a web service on a dynamic port.
//
// The task reads its assigned port from LEGION_PORT_http and binds it.
// Verify with:
//   legion service list
//   legion service resolve web
//   legion service doctor web
//   curl "$(legion service resolve web -j | jq -r '.[0] | "\(.address):\(.port)"')"
{
    name: "service-demo",
    tasks: [
        {
            name: "web",
            kind: "exec",
            lifecycle: "service",
            config: {
                command: ["sh", "-c", "exec python3 -m http.server $LEGION_PORT_HTTP"],
            },
            network: {
                ports: [{ name: "http" }],   // no static -> dynamic port assigned
            },
            service: {
                port: "http",
                tags: ["demo"],
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
    ],
}
