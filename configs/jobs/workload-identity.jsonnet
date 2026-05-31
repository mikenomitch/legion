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
      kind: 'exec',
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
          // app must be signaled to pick up a refreshed token.
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
  ],
}
