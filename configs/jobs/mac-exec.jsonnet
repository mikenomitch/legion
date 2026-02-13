{
  name: 'exec-me',
  tasks: [
    {
      kind: 'exec',
      config: {
        command: ['touch'],
        args: ['hi'],
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
