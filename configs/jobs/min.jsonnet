{
  name: 'min',
  task: {
    name: 'hello',
    runtime: 'container',
    config: {
      image: 'mnomitch/hello_world_logger',
      command: ['echo', 'hello', 'from', 'min'],
    },
  },
}
