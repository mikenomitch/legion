// Minimal demo of KV-driven task updates (see docs/kv-store.md,
// "Reactive updates"). One exec task with a watched env injection and a
// watched file injection on the same key:
//
//   legion kv put demo/banner hello     # seed before running the job
//   legion job run configs/jobs/kv-reactive-demo.jsonnet
//   legion kv put demo/banner world     # onChange: restart -> task restarts
//   legion kv delete demo/banner        # onDelete: fail -> job marked failed
//   legion kv put demo/banner back      # recovery: restart with new value
{
    name: "kv-reactive-demo",
    task: {
        kind: "exec",
        kv: {
            watch: true,
            onChange: "restart",
            onDelete: "fail",
        },
        env: [
            {
                name: "BANNER",
                value: {
                    template: '{{ kv "demo/banner" }}',
                },
            },
        ],
        files: [
            {
                path: "/tmp/legion/kv-demo.conf",
                mode: "0644",
                file: {
                    template: 'banner={{ kv "demo/banner" }}\n',
                },
            },
        ],
        config: {
            command: ["sh", "-c", 'echo "banner=$BANNER"; sleep 600'],
        },
    },
}
