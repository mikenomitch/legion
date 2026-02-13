{
  name: 'min',
  task: {
    name: 'hello',
    kind: 'container',
    config: {
      image: 'mnomitch/hello_world_logger',
      command: ['echo', 'hello', 'from', 'min'],
    },
  },
}
