{
    name: "dag-exec-mac-sidecars",
    tasks: [
        {
            name: "root-bootstrap",
            kind: "exec",
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
            kind: "exec",
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
            kind: "exec",
            lifecycle: "service",
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
            kind: "exec",
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
            kind: "exec",
            lifecycle: "service",
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
            kind: "exec",
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
            kind: "exec",
            lifecycle: "service",
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
            kind: "exec",
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
            kind: "exec",
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
            kind: "exec",
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
