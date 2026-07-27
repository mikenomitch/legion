{
    name: "dag-exec-mac-sidecars",
    tasks: [
        {
            name: "root-bootstrap",
            runtime: "exec",
            lifecycle: "batch",
            group: "bootstrap",
            config: {
                command: ["sleep", "9"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "bootstrap-sidecar",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "bootstrap",
            dependsOn: [
                {
                    task: "root-bootstrap",
                    condition: "start",
                },
            ],
            config: {
                command: ["sleep", "11"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "parser",
            runtime: "exec",
            // Batch: downstream stages gate on this task's success, and only
            // batch tasks can satisfy a complete/success condition (services
            // never exit).
            lifecycle: "batch",
            group: "pipeline-a",
            dependsOn: [
                {
                    task: "root-bootstrap",
                    condition: "complete",
                },
            ],
            config: {
                command: ["sleep", "10"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "parser-sidecar",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "pipeline-a",
            dependsOn: [
                {
                    task: "parser",
                    condition: "start",
                },
            ],
            config: {
                command: ["sleep", "8"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "transformer",
            runtime: "exec",
            lifecycle: "batch",
            group: "pipeline-b",
            dependsOn: [
                {
                    task: "root-bootstrap",
                    condition: "complete",
                },
            ],
            config: {
                command: ["sleep", "9"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "transformer-sidecar",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "pipeline-b",
            dependsOn: [
                {
                    task: "transformer",
                    condition: "start",
                },
            ],
            config: {
                command: ["sleep", "12"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "joiner",
            runtime: "exec",
            lifecycle: "batch",
            group: "join",
            dependsOn: [
                {
                    task: "parser",
                    condition: "success",
                },
                {
                    task: "transformer",
                    condition: "success",
                },
            ],
            config: {
                command: ["sleep", "10"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "joiner-sidecar",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "join",
            dependsOn: [
                {
                    task: "joiner",
                    condition: "start",
                },
            ],
            config: {
                command: ["sleep", "9"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "sink",
            runtime: "exec",
            lifecycle: "service",
            group: "sink",
            dependsOn: [
                {
                    task: "joiner",
                    condition: "success",
                },
            ],
            config: {
                command: ["sleep", "8"],
            },
            constraints: {
                os: "macos",
            },
        },
        {
            name: "sink-sidecar",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "sink",
            dependsOn: [
                {
                    task: "sink",
                    condition: "start",
                },
            ],
            config: {
                command: ["sleep", "11"],
            },
            constraints: {
                os: "macos",
            },
        },
    ],
}
