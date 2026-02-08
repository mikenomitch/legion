{
    name: "rollout-plugin-slow-start-demo",
    rollout: {
        // Requires building plugins/rollouts/slow_start first.
        strategy: "slow_start",
        replacement: "createFirst",
        progression: "auto",
        inPlace: false,
        minHealthyTime: "10s",
        healthyDeadline: "5m",
    },
    tasks: {
        api: {
            kind: "noop",
            count: 10,
            config: {
                message: "slow-start rollout plugin demo",
            },
        },
    },
}
