{
  name: 'exec-me',
  tasks: [
    {
      runtime: 'exec',
      config: {
        command: ['touch', 'hi'],
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
