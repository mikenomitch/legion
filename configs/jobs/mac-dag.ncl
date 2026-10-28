{
  name = "mac-dag",
  tasks = [
    {
      name = "bootstrap-init",
      runtime = "exec",
      lifecycle = "batch",
      group = "bootstrap",
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 200,
        memory = 256,
      },
    },
    {
      name = "bootstrap-observer",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "bootstrap",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "bootstrap-init",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "5"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "config-loader",
      runtime = "exec",
      lifecycle = "service",
      group = "control",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "bootstrap-observer",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 200,
        memory = 256,
      },
    },
    {
      name = "policy-audit",
      runtime = "exec",
      lifecycle = "batch",
      group = "control",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "config-loader",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "1"],
      },
      onUnhealthy = {
        restartCount = 1,
        restartPeriod = "1m",
        restartDelay = "10s",
      },
      constraints = {
        os = "macos",
      },
      resources = {
        memory = 128,
      },
    },
    {
      name = "policy-cache",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "control",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "policy-audit",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "5"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "ingest-scan",
      runtime = "exec",
      lifecycle = "batch",
      group = "ingest",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "policy-cache",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 200,
        memory = 256,
      },
    },
    {
      name = "ingest-parse",
      runtime = "exec",
      lifecycle = "service",
      group = "ingest",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "ingest-scan",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "ingest-shape",
      runtime = "exec",
      lifecycle = "batch",
      group = "ingest",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "ingest-parse",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "1"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "transform-index",
      runtime = "exec",
      lifecycle = "service",
      group = "transform",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "ingest-shape",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 300,
        memory = 512,
      },
    },
    {
      name = "transform-enrich",
      runtime = "exec",
      lifecycle = "batch",
      group = "transform",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "transform-index",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "transform-sidecar",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "transform",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "transform-enrich",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "5"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "route-fast",
      runtime = "exec",
      lifecycle = "service",
      group = "routing",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "transform-sidecar",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "route-slow",
      runtime = "exec",
      lifecycle = "batch",
      group = "routing",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "route-fast",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "route-branch",
      runtime = "exec",
      lifecycle = "service",
      group = "routing",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "route-slow",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "1"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "route-join",
      runtime = "exec",
      lifecycle = "batch",
      group = "routing",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "route-branch",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 400,
        memory = 512,
      },
    },
    {
      name = "route-join-sidecar",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "routing",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "route-join",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "5"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "sink-archive",
      runtime = "exec",
      lifecycle = "batch",
      group = "sink",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "route-join-sidecar",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "3"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 200,
        memory = 256,
      },
    },
    {
      name = "sink-compact",
      runtime = "exec",
      lifecycle = "service",
      group = "sink",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "sink-archive",
          condition = "success",
        },
      ],
      config = {
        command = ["sleep", "2"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "sink-sidecar",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "sink",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "sink-compact",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "5"],
      },
      constraints = {
        os = "macos",
      },
    },
    {
      name = "telemetry-collector",
      runtime = "exec",
      lifecycle = "batch",
      group = "telemetry",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "sink-sidecar",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "3"],
      },
      constraints = {
        os = "macos",
      },
      resources = {
        cpu = 100,
        memory = 128,
      },
    },
    {
      name = "telemetry-sidecar",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "telemetry",
      affinities = {
        same_node_as = "bootstrap-init",
      },
      dependsOn = [
        {
          task = "telemetry-collector",
          condition = "start",
        },
      ],
      config = {
        command = ["sleep", "5"],
      },
      constraints = {
        os = "macos",
      },
    },
  ],
}
