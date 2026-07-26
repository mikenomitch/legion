// Batch container task on macOS (Apple `container` runtime).
//
// Prerequisites on the worker node:
//   brew install container
//   container system start
//
// Run and inspect:
//   legion job run configs/jobs/mac-container.jsonnet
//   legion job logs mac-container-demo
//
// `resources` is not just a scheduling reservation for this kind: Apple
// `container` boots a VM per container, so memory/cores size the guest.
{
  name: 'mac-container-demo',
  task: {
    name: 'hello-container',
    kind: 'container',
    lifecycle: 'batch',
    config: {
      image: 'alpine:latest',
      // Anything after the image is the container's argv, flags included.
      command: [
        'sh',
        '-c',
        'echo "greeting: $GREETING"; echo "alloc: $LEGION_ALLOC_ID job: $LEGION_JOB_ID"; nproc; free -m | head -2',
      ],
      env: {
        GREETING: 'hello from macos container',
      },
    },
    resources: {
      memory: 256,
      cores: 1,
    },
    constraints: {
      os: 'macos',
    },
    count: 1,
  },
}
