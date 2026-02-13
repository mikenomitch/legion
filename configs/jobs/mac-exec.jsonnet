{
  name: 'hi-jamie',
  tasks: [
    {
      kind: 'exec',
      config: {
        command: ['touch'],
        args: ['hihihi'],
        cwd: '/Users/mike/Desktop',
      },
      constraints: {
        arch: 'aarch64',
        os: 'macos',
        foo: 'bar',
      },
      count: 10,
    },
  ],
}
