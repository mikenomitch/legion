# Writing autoscaling plugins

Legion's autoscaler reads signals from **sources**, turns them into a
desired count with **strategies**, and changes the size of **targets**.
The common cases are built in ([autoscaling.md](autoscaling.md#sources)):
the `legion`, `prometheus` and `http` sources, five strategies, and the
`exec` and `noop` targets. Anything else is a plugin: a small program, in
any language, that speaks the `legion-scaling/1` protocol.

This page is for the person writing one. Installing and using plugins is in
[autoscaling.md, "Plugins"](autoscaling.md#plugins).

## The shape of a plugin

A plugin is one of two things:

- **A program** the autoscaler starts once and keeps running. It reads one
  JSON-RPC 2.0 request per line on stdin and writes one response per line
  on stdout. stderr is its log: the autoscaler keeps the last few kilobytes
  for error messages. Anything else written to stdout breaks the protocol.
- **A URL** the autoscaler POSTs each request to, with the response in the
  body (HTTP 200 for every JSON-RPC response, errors included). This lets a
  plugin be a Legion service of its own, deployed and scaled on its own.

One plugin may implement any mix of the three kinds. It says which in its
handshake.

The protocol is JSON-RPC 2.0: requests carry `jsonrpc`, `id`, `method` and
`params`; a response carries the same `id` and either `result` or
`error = { code, message }`. The autoscaler sends one request at a time to a
program and waits for its response, so a plugin may be single-threaded.

| Method | Kind | Timeout |
| --- | --- | --- |
| `initialize` | all | 10s |
| `source/query` | source | 10s |
| `strategy/recommend` | strategy | 10s |
| `target/status` | target | 120s |
| `target/scale` | target | 120s |
| `target/terminate` | target, node groups only | 120s |

## `initialize`

Sent once, after the program starts (and after every restart), and before
anything else on a URL plugin.

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize",
   "params":{"protocol":"legion-scaling/1","name":"calendar",
             "config":{"windows":[{"start":"02:00","end":"03:30"}]}}}
← {"jsonrpc":"2.0","id":1,"result":{
     "name":"calendar","version":"0.1.0","kinds":["strategy"],
     "capabilities":{},
     "policyConfigSchema":{"type":"object","properties":{"replicas":{"type":"integer"}},
                           "required":["replicas"],"additionalProperties":false}}}
```

- `params.protocol` is `legion-scaling/1`. Refuse anything else with error
  -32602.
- `params.name` is the name the operator installed the plugin under.
- `params.config` is the operator's plugin-level config from the
  autoscaler's app config: addresses, regions, request headers. It is
  stored as written and readable by anyone with `app:read` on the
  autoscaler, so a credential placed there is visible to them; prefer a
  credential the plugin reads from its own environment or files.
  Only the operator sets it.
- `result.kinds` lists `source`, `strategy`, `target`. A policy that uses
  the plugin as a kind it does not list is reported `invalid`.
- `result.capabilities.nodes` is `true` for a target whose members are
  Legion nodes (a node group). Such a target implements `target/terminate`.
- `result.policyConfigSchema` is optional: a JSON Schema (draft 7, local
  `$ref`s only, at most 64 KiB) that the **policy-level** config must
  match. That is a strategy's settings (every field of the policy's
  `strategy` record but `name`) or a target's `target.config`. The
  autoscaler checks every config against it before a call carries one, so
  the plugin never sees a config its schema refuses. A policy whose config
  does not match is held `invalid`, with the schema's error, and a
  `scale.check_failed` event. Give it `type: object`.

## Sources: `source/query`

```json
→ {"jsonrpc":"2.0","id":2,"method":"source/query",
   "params":{"policy":"ingest/default/worker","check":"lag",
             "query":"sum(kafka_consumergroup_lag{group=\"ingest\"})",
             "window":"2m","windowSeconds":120}}
← {"jsonrpc":"2.0","id":2,"result":{"value":2310}}
```

The `query` is the policy's, as written: a tenant's data. Treat it as
untrusted input (it must not name files, URLs, or commands for the plugin
to open). The result is one of:

- `{ "value": <number> }`: the reading, already averaged over `window` if
  the source can do that (a Prometheus range, say);
- `{ "series": [[<unix ms>, <number>], …] }`: points the autoscaler
  averages over the window itself;
- `{ "value": null, "reason": "<why>" }`: no data. The check counts as
  missing: nothing scales in while it lasts (`onMissing = "hold"`).

The autoscaler asks each distinct `(source, query, window)` once per
evaluation (every 15 seconds by default), whatever the number of policies
using it.

## Strategies: `strategy/recommend`

```json
→ {"jsonrpc":"2.0","id":3,"method":"strategy/recommend",
   "params":{"policy":"batch/default/batch","check":"window",
             "current":4,"ready":4,"reading":null,"config":{"replicas":6,"lead":"15m"}}}
← {"jsonrpc":"2.0","id":3,"result":{"desired":6,"note":"batch window 02:00"}}
```

- `current` is the target's count, `ready` its ready replicas (healthy and
  past warmup).
- `reading` is the check's reading, or `null` when the check has no
  `query` (a strategy that decides from the clock or a calendar).
- `config` is the policy's `strategy` record without `name`, already
  checked against `policyConfigSchema`.
- `desired` is a count; `note` is what `legion scaling explain` shows next
  to it.

A strategy is one vote. The pipeline after it (the merge with the other
checks, stabilization, step limits, bounds, pins, pauses, dry-run) applies
exactly as to the built-in strategies ([How it
decides](autoscaling.md#how-it-decides)). A check with `scaleIn = false`
counts only when it wants more than the current count, so a strategy that
should only add capacity can answer 0 outside its window.

## Targets: `target/status`, `target/scale`, `target/terminate`

```json
→ {"jsonrpc":"2.0","id":4,"method":"target/status",
   "params":{"policy":"media/render-farm","config":{"pool":"gpu"}}}
← {"jsonrpc":"2.0","id":4,"result":{"count":7,"ready":6,
     "members":[{"id":"i-0abc","state":"running"}]}}

→ {"jsonrpc":"2.0","id":5,"method":"target/scale",
   "params":{"policy":"media/render-farm","config":{"pool":"gpu"},
             "desired":9,"decision":"d-81f3a2c0"}}
← {"jsonrpc":"2.0","id":5,"result":{}}
```

- `target/status` returns `count` (required), `ready` (default: `count`)
  and `members` (optional). The autoscaler calls it every evaluation; the
  count is the policy's current count.
- `target/scale` moves the target to `desired`. **It must be idempotent**:
  after a restart the autoscaler may send the same decision again. The
  autoscaler records the decision before it calls (so a pause can refuse
  it on the server), and records a `failed` row if the call fails.
- `target/terminate` (`params.ids`, provider ids) stops specific members.
  It belongs to node-group targets (`capabilities.nodes = true`), whose
  `target/scale` also carries `join = { token, attributes }`: the join
  token new members register with, a secret never to log. See [Node-group
  targets](#node-group-targets).

## Node-group targets

A target that runs a node group declares `capabilities.nodes = true` in its
`initialize` answer. The autoscaler refuses to run a node group on a target
that does not (the policy reads `invalid`), and the built-in `noop` target
does not; the `exec` builtin does. A node target:

- **`target/status`** returns `count` (the instances the provider holds,
  its desired count) and `members`: one `{ "id": "<provider id>", "state":
  "running" }` per instance. `id` is what a member joins with as
  `legion.provider-id`; the autoscaler cross-checks it, and never drains a
  node whose provider id the provider does not list (or that two nodes
  claim). A `state` of `terminated`, `terminating`, `shutting-down`,
  `stopping`, or `stopped` counts as gone. Listing members is what lets the
  autoscaler confirm a termination and forget the node; a target that lists
  none (and holds a count above 0) has its members' provider ids taken on
  trust.
- **`target/scale`** receives `join = { "token": "<join token>",
  "attributes": { "legion.nodegroup": "workers-general", … } }` with every
  scale-up. The token admits exactly the nodes being added (`max_uses`) and
  stamps the attributes on them; it is a secret, never to be logged. The
  plugin's job is to hand it to the new machines, which run `legion join
  --token-file … --attr legion.provider-id=<their instance id>`. A
  node-group scale is only ever upwards: scale-in terminates chosen
  members instead of lowering the count.
- **`target/terminate`** receives `ids` (provider ids) and `decision`,
  and stops exactly those instances, lowering the provider's count with
  them. It must be idempotent: an instance already gone is success. It is
  only called for a member Legion has drained.

## The `aws-asg` plugin

[`plugins/scaling/aws-asg/`](../plugins/scaling/aws-asg/) is Legion's
first-party node target for AWS Auto Scaling groups.
It is its own program (`cargo build --release` in that directory, or
`LEGION_BUILD_PLUGINS=1 cargo build` from the root), outside Legion's
workspace, with no AWS SDK: SigV4 is written out, over the HTTP client
Legion already uses.

| Method | AWS call |
| --- | --- |
| `target/status` | `DescribeAutoScalingGroups`: `count` is `DesiredCapacity`, ready members are `InService` and healthy, every instance is a member |
| `target/scale` | `ssm:PutParameter` (the join token, as a `SecureString`, when `joinTokenParameter` is set), then `SetDesiredCapacity` |
| `target/terminate` | `TerminateInstanceInAutoScalingGroup` with `ShouldDecrementDesiredCapacity`, per instance |

Install it in the autoscaler's app config, and name it from a node group:

```nickel
# legion app config set autoscaler -f autoscaler.ncl
{ plugins = { aws-asg = {
    source = "https://…/aws-asg-linux-amd64.tar.gz", checksum = "sha256:…",
    config = {
      region = "us-east-1",
      # Credentials without keys: the app's second workload identity, exchanged
      # with STS (AssumeRoleWithWebIdentity).
      webIdentityTokenFile = "/secrets/aws/token",
      roleArn = "arn:aws:iam::123456789012:role/legion-autoscaler",
    },
} } }
```

| Plugin config | Default | Meaning |
| --- | --- | --- |
| `region` | `AWS_REGION` | The group's region. |
| `webIdentityTokenFile`, `roleArn` | none | A web identity token to exchange for the role's credentials with STS `AssumeRoleWithWebIdentity` (read fresh at each refresh). |
| `roleSessionName` | `legion-aws-asg` | The STS session name. |
| `imds` | `true` | Fall back to the EC2 instance profile (IMDSv2). |
| `endpoint`, `ssmEndpoint`, `stsEndpoint`, `imdsEndpoint` | AWS's | Other endpoints (a VPC endpoint, or a test double). |

Without `webIdentityTokenFile`, credentials come from the standard
environment (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`,
or `AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN`), then the instance
profile. Temporary credentials are refreshed five minutes before they
expire.

| Policy config (`target.nodes.config`) | Meaning |
| --- | --- |
| `asg` | The Auto Scaling group's name (required). |
| `joinTokenParameter` | An SSM parameter the plugin writes the group's join token to before each scale-up. The launch template's user data reads it (`aws ssm get-parameter --with-decryption`) and runs `legion join`. Without it, new instances need a token from elsewhere. |
| `honorCooldown` | Pass `HonorCooldown` to `SetDesiredCapacity` (default `false`: the autoscaler paces itself). |

The role needs `autoscaling:DescribeAutoScalingGroups`,
`autoscaling:SetDesiredCapacity`,
`autoscaling:TerminateInstanceInAutoScalingGroup`, and `ssm:PutParameter`
on the parameter (with `kms:Encrypt` on its key). Instances need
`ssm:GetParameter` on it. Its unit tests check the signing against AWS's
published SigV4 examples and the parsing against recorded responses, and
drive the whole plugin against a local mock of the AWS API; none need AWS.

## Errors, timeouts, restarts

- **Errors.** Answer a method you do not implement with error -32601
  (method not found), and parameters that do not parse with -32602
  (invalid params). Use -32000 to -32099 for your own failures. The
  message is shown to people. An error is a valid answer: the plugin is
  healthy, and the check reads as missing (a source, a strategy) or the
  target reports `error`.
- **Timeouts.** A call that misses its timeout (10 seconds; 120 for
  targets) fails, and a program is killed, because its late answer would
  arrive as the reply to the next request.
- **Restarts.** A program that exits, times out, or writes a line that is
  not a JSON-RPC response is restarted on the next call: at once the first
  time, then after 1, 2, 4 … up to 60 seconds while it keeps failing. A
  successful call resets the backoff. Each start sends `initialize` again.
- **Secrets.** The autoscaler logs requests at `debug` with every value
  under a key naming a credential (`token`, `secret`, `password`,
  `credential`, `apiKey`, `authorization`, `privateKey`) and the join token
  replaced by `<redacted>`.

## Testing a plugin

`legion scaling plugin test` runs the conformance suite against a program
or a URL, the way the autoscaler would drive it. It needs no cluster.

```console
$ legion scaling plugin test \
    --config '{"windows":[{"start":"02:00","end":"03:30"}]}' \
    --policy-config '{"replicas":6,"lead":"15m"}' \
    python3 examples/scaling-plugins/python/calendar_strategy.py
calendar 0.1.0 (strategy)
 CHECK              | RESULT | MS | DETAIL
--------------------+--------+----+------------------------------------------------
 handshake          | pass   | 41 | calendar 0.1.0 implements strategy
 schema             | pass   |  0 | the policy config matches; a config that is not a record is refused …
 strategy/recommend | pass   |  1 | at 4 replicas reading 2310 it wants 0: no batch window
 unknown method     | pass   |  0 | answered -32601 (method not found)
 malformed request  | pass   |  1 | answered -32602 (invalid params); still answering after it
 crash and restart  | pass   | 39 | killed, it started again, handshook, and answered strategy/recommend
 timeouts           | pass   |  0 | every call answered in time; the slowest was initialize, 41ms of its 10s
`python3 examples/scaling-plugins/python/calendar_strategy.py`: conformant
```

| Check | Passes when |
| --- | --- |
| `handshake` | `initialize` answers with `name`, `version`, and known `kinds`, and the schema (if any) compiles |
| `schema` | `--policy-config` matches the schema, and the schema refuses a config that is not a record (a warning otherwise; a missing schema is a warning too) |
| `source/query`, `strategy/recommend`, `target/status`, `target/scale`, `target/terminate` | each method of each declared kind answers in the shapes above. `target/scale` asks for the count `target/status` reported, so the test changes nothing; `target/terminate` is sent no ids |
| `unknown method` | an unknown method answers -32601 (another error code is a warning) |
| `malformed request` | parameters of the wrong type answer an error (-32602 ideally), and the plugin answers the next call |
| `crash and restart` | a killed program starts again, handshakes, and answers (URL plugins: skipped) |
| `timeouts` | every call answered within its timeout (a warning past half of it) |

Options go before the plugin: everything from the command on is the
command's own arguments. `--config` is the plugin-level config `initialize`
sends, `--policy-config` the policy-level one, `--query` what
`source/query` asks (default `conformance`), and `--timeout` replaces every
timeout (to test a plugin's behavior under a tight one). A URL plugin is
tested the same way: `legion scaling plugin test http://127.0.0.1:9100/rpc`.
With `--json` a conformant plugin prints the whole report; a failing one
prints the error envelope naming every failed check, and exits non-zero.

## SDKs and examples

The repository's [`examples/scaling-plugins/`](../examples/scaling-plugins/)
has:

- **`rust/`**: the `legion-scaling-plugin` crate. Implement its `Plugin`
  trait (the methods of the kinds you declare) and call `serve`; it does the
  framing, the error codes, and turns a panic into an error response
  instead of a dead plugin. `examples/queue_ratio.rs` is a strategy built
  on it (`cargo build --release --example queue-ratio`).
- **`python/calendar_strategy.py`**: a strategy (standard library only)
  that adds replicas ahead of known batch windows. It also serves HTTP with
  `--http PORT`.
- **`go/spool/`**: a source and a target (standard library only) over a
  spool directory: a queue's depth, and a count kept in a file as a
  stand-in for a provider's API.

## Packaging

An operator installs a program plugin one of two ways
([autoscaling.md, "Plugins"](autoscaling.md#plugins)):

- `source = "https://…/my-plugin.tar.gz"` with `checksum = "sha256:…"`:
  the artifact is fetched into the autoscaler's task directory, under
  `code/plugins/<name>/` (archives are unpacked), and the program runs in
  that directory. Without `command`, the program is `./<name>`: ship an
  executable named like the plugin, or tell the operator the `command`
  (`["python3", "calendar_strategy.py"]`).
- `command = ["/opt/legion/plugins/forecast"]`: a program already on the
  hosts the autoscaler runs on.

The autoscaler runs on the managers' system workers by default, as user
`nobody`, so a plugin must not need root or a home directory.
