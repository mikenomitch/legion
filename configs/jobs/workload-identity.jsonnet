// Example: a task with two independent workload identities.
//
// Requires the manager to be started with workload identity enabled (see
// docs/workload-identity.md). Each identity gets its own short-lived,
// asymmetrically-signed OIDC token with a provider-scoped audience.
{
  name: 'workload-identity-demo',
  tasks: [
    {
      name: 'api',
      runtime: 'exec',
      config: {
        command: ['sleep', '3600'],
      },
      identities: [
        {
          // Federates into AWS via AssumeRoleWithWebIdentity.
          name: 'aws',
          audience: ['sts.amazonaws.com'],
          ttl: '15m',
          file: {
            // SDKs re-read the token file on each call, so noop is enough.
            path: '/var/run/legion/identity/aws/token',
            mode: '0400',
          },
          changeMode: 'noop',
        },
        {
          // Federates into Vault via JWT auth; delivered as an env var, so the
          // app must be signaled to pick up a refreshed token. Env-delivered
          // tokens are also mirrored to a backing file
          // (.legion/identity/<alloc>/vault/token, advertised via
          // VAULT_WORKLOAD_TOKEN_FILE) so the SIGHUP handler has somewhere to
          // re-read the fresh value from — the live process env cannot change.
          name: 'vault',
          audience: ['vault.internal'],
          ttl: '5m',
          env: 'VAULT_WORKLOAD_TOKEN',
          changeMode: 'signal',
          changeSignal: 'SIGHUP',
          extraClaims: {
            'app.example.com/tier': 'backend',
          },
        },
      ],
    },
    {
      // Stateful task: each replica is a stable instance (slot mode), and its
      // identity can be keyed per instance. The {instance} placeholder renders
      // the stable instance id (e.g. workload-identity-demo-default-0), so an
      // external policy can grant replica 0 different access than replica 1 —
      // and the subject survives reschedules (unlike alloc/node, which are
      // forbidden in sub for exactly that reason).
      name: 'replica',
      runtime: 'exec',
      count: 2,
      stateful: {
        mode: 'slot',
      },
      config: {
        command: ['sleep', '3600'],
      },
      identities: [
        {
          name: 'aws',
          audience: ['sts.amazonaws.com'],
          ttl: '15m',
          // sub = auto:workload-identity-demo:default:replica:<instance-id>
          subTemplate: '{region}:{job}:{group}:{task}:{instance}',
          file: {
            path: '/var/run/legion/identity/replica-aws/token',
            mode: '0400',
          },
          changeMode: 'noop',
        },
      ],
    },
  ],
}
