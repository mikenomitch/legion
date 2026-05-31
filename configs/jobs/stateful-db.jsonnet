// Example: a slot-mode stateful service.
//
// count = number of slots → stable instances db:data:0, db:data:1, db:data:2.
// Each instance survives rescheduling with the same id/ordinal, gets a stable
// DNS-style network name, and records the node it last ran on for volume
// affinity. See docs/stateful-workloads.md.
{
  name: 'db',
  tasks: [
    {
      name: 'data',
      kind: 'container',
      group: 'data',
      count: 3,
      config: {
        image: 'postgres:16',
        command: ['postgres'],
      },
      stateful: {
        mode: 'slot',
        stableNetworkId: true,
        volumeAffinity: true,
      },
      // Optional: federate each instance with its own per-instance identity.
      identities: [
        {
          name: 'aws',
          audience: ['sts.amazonaws.com'],
          file: { path: '/var/run/legion/identity/aws/token', mode: '0400' },
          // {instance} is only valid because this task declares `stateful`.
          subTemplate: '{region}:{job}:{group}:{task}:{instance}',
        },
      ],
    },
  ],
}
