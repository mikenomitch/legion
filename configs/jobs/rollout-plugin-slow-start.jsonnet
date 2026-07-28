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
            runtime: "exec",
            count: 10,
            config: {
                command: ["sleep", "1"],
            },
        },
    },
}
