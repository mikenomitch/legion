{
  name: "mac-dag",
  tasks: [
    {
      name: "bootstrap-init",
      kind: "exec",
      lifecycle: "batch",
      group: "bootstrap",
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.2,
        memory: 256,
      },
    },
    {
      name: "bootstrap-observer",
      kind: "exec",
      lifecycle: "sidecar",
      group: "bootstrap",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "bootstrap-init",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "5"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "config-loader",
      kind: "exec",
      lifecycle: "service",
      group: "control",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "bootstrap-observer",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.2,
        memory: 256,
      },
    },
    {
      name: "policy-audit",
      kind: "exec",
      lifecycle: "batch",
      group: "control",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "config-loader",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "1"],
      },
      on_unhealthy: {
        restartCount: 1,
        restartPeriod: "1m",
        restartDelay: "10s",
      },
      constraints: {
        os: "macos",
      },
      resources: {
        memory: 128,
      },
    },
    {
      name: "policy-cache",
      kind: "exec",
      lifecycle: "sidecar",
      group: "control",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "policy-audit",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "5"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "ingest-scan",
      kind: "exec",
      lifecycle: "batch",
      group: "ingest",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "policy-cache",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.2,
        memory: 256,
      },
    },
    {
      name: "ingest-parse",
      kind: "exec",
      lifecycle: "service",
      group: "ingest",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "ingest-scan",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "ingest-shape",
      kind: "exec",
      lifecycle: "batch",
      group: "ingest",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "ingest-parse",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "1"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "transform-index",
      kind: "exec",
      lifecycle: "service",
      group: "transform",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "ingest-shape",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.3,
        memory: 512,
      },
    },
    {
      name: "transform-enrich",
      kind: "exec",
      lifecycle: "batch",
      group: "transform",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "transform-index",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "transform-sidecar",
      kind: "exec",
      lifecycle: "sidecar",
      group: "transform",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "transform-enrich",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "5"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "route-fast",
      kind: "exec",
      lifecycle: "service",
      group: "routing",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "transform-sidecar",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "route-slow",
      kind: "exec",
      lifecycle: "batch",
      group: "routing",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "route-fast",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "route-branch",
      kind: "exec",
      lifecycle: "service",
      group: "routing",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "route-slow",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "1"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "route-join",
      kind: "exec",
      lifecycle: "batch",
      group: "routing",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "route-branch",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.4,
        memory: 512,
      },
    },
    {
      name: "route-join-sidecar",
      kind: "exec",
      lifecycle: "sidecar",
      group: "routing",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "route-join",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "5"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "sink-archive",
      kind: "exec",
      lifecycle: "batch",
      group: "sink",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "route-join-sidecar",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "3"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.2,
        memory: 256,
      },
    },
    {
      name: "sink-compact",
      kind: "exec",
      lifecycle: "service",
      group: "sink",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "sink-archive",
          condition: "success",
        },
      ],
      config: {
        command: ["sleep", "2"],
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
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "sink-compact",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "5"],
      },
      constraints: {
        os: "macos",
      },
    },
    {
      name: "telemetry-collector",
      kind: "exec",
      lifecycle: "batch",
      group: "telemetry",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "sink-sidecar",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "3"],
      },
      constraints: {
        os: "macos",
      },
      resources: {
        cpus: 0.1,
        memory: 128,
      },
    },
    {
      name: "telemetry-sidecar",
      kind: "exec",
      lifecycle: "sidecar",
      group: "telemetry",
      affinities: {
        same_node_as: "bootstrap-init",
      },
      dependsOn: [
        {
          task: "telemetry-collector",
          condition: "start",
        },
      ],
      config: {
        command: ["sleep", "5"],
      },
      constraints: {
        os: "macos",
      },
    },
  ],
}
