{
  name: 'two-task-node-pin',
  tasks: [
    {
      name: 'task-one',
      kind: 'exec',
      config: {
        command: ['sleep', '1'],
      },
      resources: {
        memory: 256,
      },
      constraints: {
        arch: 'aarch64',
        os: 'macos',
        foo: 'bar',
      },
      count: 1,
    },
    {
      name: 'task-two',
      kind: 'exec',
      config: {
        command: ['sleep', '1'],
      },
      resources: {
        memory: 256,
      },
      constraints: {
        arch: 'aarch64',
        os: 'macos',
        foo: 'bar',
      },
      count: 1,
    },
  ],
}
