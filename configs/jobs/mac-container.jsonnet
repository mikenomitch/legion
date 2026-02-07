{
  name: 'mac-container-demo',
  task: {
    name: 'hello-container',
    kind: 'container',
    config: {
      image: 'alpine:latest',
      command: ['echo', 'hello', 'from', 'macos', 'container'],
      env: {
        LEGION_ENV: 'macos',
      },
    },
    resources: {
      memory: 128,
    },
    constraints: {
      arch: 'aarch64',
      os: 'macos',
    },
    count: 1,
  },
}
