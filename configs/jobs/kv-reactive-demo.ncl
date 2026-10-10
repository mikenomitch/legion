# Minimal demo of KV-driven task updates (see docs/kv-store.md,
# "Reactive updates"). One exec task with a watched env injection and a
# watched file injection on the same key. KV values are encrypted at rest
# by default (docs/kv-encryption.md), so this is also how a task takes a
# secret without putting it in the jobspec:
#
#   legion kv put demo/banner hello     # seed before running the job
#   legion job run configs/jobs/kv-reactive-demo.ncl
#   legion kv put demo/banner world     # onChange: restart -> task restarts
#   legion kv delete demo/banner        # onDelete: fail -> job marked failed
#   legion kv put demo/banner back      # recovery: restart with new value
{
    name = "kv-reactive-demo",
    task = {
        runtime = "exec",
        kv = {
            watch = true,
            onChange = "restart",
            onDelete = "fail",
        },
        env = [
            {
                name = "BANNER",
                value = {
                    template = "{{ kv \"demo/banner\" }}",
                },
            },
        ],
        files = [
            {
                path = "/tmp/legion/kv-demo.conf",
                mode = "0644",
                file = {
                    template = "banner={{ kv \"demo/banner\" }}\n",
                },
            },
        ],
        config = {
            command = ["sh", "-c", "echo \"banner=$BANNER\"; sleep 600"],
        },
    },
}
