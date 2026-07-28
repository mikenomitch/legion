{
  name: 'two-task-node-pin',
  tasks: [
    {
      name: 'task-one',
      runtime: 'exec',
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
      runtime: 'exec',
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
