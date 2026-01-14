{
  name: 'hi-mike',
  tasks: [
    {
      kind: 'exec',
      config: {
        command: 'touch',
        args: ['woot'],
        cwd: '/Users/mike/Code/legion',
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
