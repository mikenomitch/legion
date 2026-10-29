# App actions run as the viewer: on-behalf-of delegation

> **Judgement calls:** [app-actions-as-viewer-plan_calls.md](../calls/app-actions-as-viewer-plan_calls.md)

**Status: IN EXECUTION** (planned 2026-10-29, on PR #60). Deliberately
deferred: delegation for views and sandboxed
pages (non-goal), a single-use or audience-bound manager-side check of the
assertion (the 60-second TTL bounds reuse by the same app), naming the
delegation in the `app.action` event, and a `legion app test` that checks a
third-party app sends the header (with the rest of `legion app test`).

Supersedes the "the action runs as the app" half of
[apps calls entry 68](../calls/legion-apps-plan_calls.md) and
[autoscaler-v2 calls entry 83](../calls/autoscaler-v2-plan_calls.md); the
apps plan's §6.5 authorization (the manager checks the declared permission
as the viewer before forwarding) stands unchanged.

## 1. Problem statement

An app action today is authorized as the viewer and executed as the app
([apps plan §6.5](legion-apps-plan.md#65-ui-extensions)):

1. the manager checks that the viewer holds the action's declared permission
   (`scale:write`) on the context (`default/web`);
2. it forwards the request to the app with a signed viewer assertion;
3. the app then calls Legion with **its own grant**, which is usually far
   broader (the autoscaler holds `scale:write` in every namespace).

Two things go wrong:

- **The grant boundary is the app's code.** Nothing but the app's own
  correctness stops a button press on `default/web` from writing to
  `payments/api`. A buggy or malicious app turns a viewer's narrow
  permission into the app's broad one. The manifest's declared permission is
  a promise the manager cannot hold the app to.
- **History records the app, not the person.** A pause from the UI is
  recorded as made by the autoscaler's identity. The viewer survives only as
  free text in the reason (`paused from the UI by alice`), which no filter,
  audit query, or `scaling history` column can rely on.

What a user has to do instead today: trust every installed app's code with
its whole grant on every press, and grep reasons to find who paused what.

## 2. Prior art

- **OAuth 2.0 Token Exchange (RFC 8693) and on-behalf-of flows** (Microsoft
  identity platform's OBO, Okta). A middle-tier service exchanges the user's
  token for one that names both the user (`sub`) and the actor (`act`), with
  a narrowed scope and audience. *Take*: the delegated credential names both
  parties, and its scope is narrower than either party's. *Reject*: a
  separate exchange endpoint and a minted second token. Legion's manager
  already signs a short-lived, audience-bound, context-bound assertion for
  every action; it can carry the delegation directly, with no extra round
  trip and no new token type to revoke.
- **GCP downscoped credentials (Credential Access Boundaries).** A token is
  combined with a boundary that can only remove permissions, never add them.
  *Take*: the effective permission is an intersection, so a delegation can
  never exceed what the presenting service itself holds.
- **Kubernetes impersonation** (`Impersonate-User`, `Impersonate-Group`
  headers). The caller authenticates as itself, then asks to act as someone
  else; the API server checks the caller's `impersonate` permission, and
  audit logs record both `user` and `impersonatedUser`. *Take*: a request
  header alongside the caller's own credential, and audit records naming
  both. *Reject*: a standing `impersonate` grant. Impersonation lets the
  caller become *anyone* it may impersonate, with *all* of their rights;
  ours is per press, per context, per declared permission, 60 seconds.
- **OpenShift console dynamic plugins**. A plugin's backend proxy calls the
  cluster API with the logged-in user's bearer token, so plugin actions are
  the user's own. *Take*: actions run as the user. *Reject*: forwarding the
  user's real token to the plugin. An app holding a viewer's token could do
  everything the viewer can, for as long as the token lives; our app gets
  only what the button was declared to do, on the context it was pressed on.
- **Nomad and Consul** have no delegated identity for add-ons; Nomad
  Autoscaler and Consul ESM act with their own tokens, which is today's
  model.

## 3. Jobs to be done

**Security / platform admin**
- When I install an app that holds broad access, I want a button a teammate
  presses to do only what that teammate could have done themselves, so a bug
  in the app can't turn a narrow permission into the app's broad one.
- When I approve an app's install prompt, I want the permission each button
  declares to be a limit the cluster enforces, not a promise, so reading the
  prompt is enough to know what a press can do.
- When I revoke someone's token, I want their in-flight button presses to
  stop working at once, so revocation means what it says.

**On-call responder**
- When I see that autoscaling for a job was paused, I want to see who
  paused it, as a name I can filter on, so I know whom to ask before I
  resume it.

**Job author / app developer**
- When I write an app action, I want one call that gives me a client acting
  as the person who pressed the button, so I don't have to re-implement
  permission checks in my app.

**Cluster operator**
- When I read the event stream or scaling history, I want a person's action
  through an app recorded as that person through that app, so audits don't
  have to parse free text.

*Not in scope:* the autonomous agent persona gets nothing new: an agent that
calls `InvokeAppAction` is a viewer like anyone else, and its actions now run
as it, through the app (§6.7).

### User scenarios

1. Alice holds `scale:write` on `default/web` only. She presses **Pause** on
   web's Autoscaling panel. The pause is recorded by `alice via app
   autoscaler`; `legion scaling history web` and the `scale.paused` event say
   so.
2. A buggy app answers Alice's press on `web` by pausing `default/api` too.
   The second call is refused (`PermissionDenied`): Alice's delegation covers
   `scale` on `default/web` only.
3. Bob's token is revoked while his press is being handled. The app's call
   made on his behalf is refused (`Unauthenticated`), even though the
   assertion has 50 seconds left.

## 4. Goals and non-goals

**Goals**
- An action's writes run as **`<viewer> via app <app>`**, bounded by the
  action's declared permission on its context **and** by the app's own grant.
- Built on the assertion the manager already signs: no new RPC, no new token
  type, no new store table.
- One header, one framework helper, one place the manager honors it.
- Revoking the viewer's ACL token stops a delegation at once.
- A bad delegation is refused clearly and audited; it never falls back to
  the app's own identity.
- The bundled autoscaler's Pause, Resume and Release use it.

**Non-goals**
- **Delegation for views and pages.** Views read with the app's grant, which
  is fine: the manager checked the viewer's read permission first, and a
  view writes nothing. Sandboxed pages already call as the viewer through
  the bridge.
- **Forcing apps to use it.** An app may still call with its own client in an
  action handler; its code is trusted with its grant (apps trust model).
  Delegation is how a well-behaved app acts as the viewer, and the manifest's
  declared permission is its bound.
- **Long-lived delegation.** No refresh, no streams: a delegation covers
  unary calls for the assertion's 60 seconds.
- **A general impersonation feature** for operators or services.

## 5. Background: what exists today

- `InvokeAppAction` (`src/manager/server/app_ui_rpcs.rs`) checks `app:read`,
  then `enforce_ui_permission` checks the declared permission on the scope
  `ui_permission_scope` derives from the slot: the job (namespace, name,
  template provenance) for `job`/`task`, the node's pool for a node-slot
  `node` permission, `unowned` for a namespaced one there.
- It signs a `ViewerAssertion` (`src/structs/app_ui.rs`) as the
  `legion.io/viewer` claim of an ES256 JWT, `aud = app:<name>`, 60 seconds,
  with the workload-identity key (`WorkloadIdentityState::issue_viewer_assertion`).
- The framework (`src/apps/ui.rs`) verifies it and runs the handler with an
  `ActionRequest` (context, inputs, viewer). The token itself is dropped.
- Apps reach Legion through `LEGION_ADDR`, the allocation's API socket
  (`src/worker/api_socket.rs`), which forwards each request as is and
  attaches the app's `legion` identity token when the request carries no
  `authorization`. That token's policies hold `grant:alloc/<id>`, which
  resolves to the app's recorded grant.
- `ManagerDaemonService::authenticate_acl` (`src/manager/server.rs`) is the
  one funnel every handler authenticates through; `is_autoscaler_identity`
  (`src/manager/server/autoscale_rpcs.rs`) and `calling_app`
  (`src/manager/server/extension_rpcs.rs`) each map a credential to an app.
- The autoscaler's actions (`src/apps/autoscaler_ui.rs`) call `PauseScaling`
  and `ScaleTask` with the app's client, putting `by <viewer>` in the reason.

## 6. Proposed design

### 6.1 Mint: the assertion carries a delegation

After the viewer passes the action's permission gate on its context,
`InvokeAppAction` adds a `delegation` to the action's assertion:

```json
"legion.io/viewer": {
  "kind": "action", "app": "autoscaler", "name": "pause",
  "principal": "alice", "accessor": "8f2c…",
  "permission": "scale:write", "scope": "context",
  "context": { "slot": "job", "namespace": "default", "job": "web" },
  "inputs": "3b1f…",
  "delegation": {
    "resource": "scale",
    "actions": ["write", "read"],
    "scope": { "namespace": "default", "name": "web" }
  }
}
```

- `resource` and the first action are the declared permission. `read` on the
  same resource follows only when the viewer holds it there too, at mint.
- `scope` is exactly what was checked: `namespace` + `name` (+ `template`
  when the job came from one) for job and task slots, `pool` for a node-slot
  `node` permission, `{}` for a cluster-scoped resource.
- TTL and audience are unchanged (60 seconds, `aud = app:<name>`). View,
  page, and validate assertions never carry one.

### 6.2 Present: the app framework's viewer client

```rust
context.action("pause", |request: ActionRequest| async move {
    let mut legion = request.client_as_viewer(&context).await?;
    legion.pause_scaling(PauseScalingRequest { job, ..Default::default() }).await?;
    Ok(ActionOutcome::done("paused"))
});
```

`client_as_viewer` dials `LEGION_ADDR` like `AppContext::client`; the API
socket still attaches the app's own token, and every call also carries:

```text
legion-on-behalf-of: <the action's viewer assertion>
```

The header name is one constant, `ON_BEHALF_OF_HEADER` in
`src/structs/app_ui.rs`, shared by the manager and the framework.

### 6.3 Honor: the manager builds a delegated identity

In `authenticate_acl`, after the caller authenticates as today, a request
carrying the header gets a **delegated identity** instead:

1. The caller must be the app: its credential carries a job-grant reference
   to an allocation of `legion-system/<app>` (the generalized
   `is_autoscaler_identity`). A management token, a person's token, or
   another app presenting someone's assertion is refused, so an assertion
   cannot be replayed by anyone but its app.
2. The assertion verifies against the cluster's keys: signature, issuer,
   `aud = app:<that app>`, unexpired (no leeway), a 60-second-shaped life;
   it is an action assertion with a `delegation`.
3. If it names the viewer's ACL token accessor, the token is re-resolved
   (`Authenticator::identity_for_accessor`): gone or expired is refused; an
   action the viewer no longer holds on the scope is dropped, and losing the
   declared one is refused. A JWT or SSO viewer has no accessor; the TTL
   bounds it.
4. The delegated identity is:
   - principal `alice via app autoscaler`;
   - one synthesized allow rule, exactly the delegated resource and actions
     on the delegated scope, **intersected with the app's grant**: an action
     the app does not itself hold there is dropped; with none left, refused;
   - the app's and the viewer's deny rules, so the rule never reaches what
     either is denied;
   - not management, no policies, `source = delegated`, `audit_only` as the
     caller's, expiring with the assertion.

What a refusal looks like:

```text
Error: PermissionDenied: legion-on-behalf-of: only an app's own identity presents a viewer assertion; this credential (bootstrap) is not an app's
Error: Unauthenticated: legion-on-behalf-of: the viewer assertion expired at 1793280120 (manager clock 1793280125)
Error: Unauthenticated: legion-on-behalf-of: the viewer's ACL token (accessor 8f2c…) was revoked
Error: PermissionDenied: legion-on-behalf-of: app gitops's own grant does not hold scale:write on team-b/other, so it cannot act there as alice
```

Every refusal is an `acl_audit` "ACL denial" log line with the caller, the
app, and the reason.

### 6.4 Where a delegated identity is refused

- **App-plane endpoints** (`ReportScalingStatus`, `RecordScalingAction`,
  `WatchAppExtensions`) take the app's own identity; a delegated one is
  `PermissionDenied`.
- **Worker-plane endpoints** need a node token; a delegated identity is not
  one.
- **Streams** (`StreamEvents`, `WatchScalingPolicies`, `WatchAppExtensions`,
  `ExecAllocation`, `StreamWork`) refuse the header: they re-authenticate
  every 30 seconds from the bearer credential alone, which would turn a
  delegation into the app's own identity mid-stream.

### 6.5 Use it: the autoscaler

Pause, Resume and Release call `PauseScaling` and `ScaleTask` through the
viewer client. The reason is `paused from the UI` (no `by <viewer>`), and the
pause's `by` is `alice via app autoscaler`. The app's grant (`scale:read`,
`scale:write` in every namespace) covers every delegation a press can carry,
except in reserved namespaces, which `*` does not reach. Reads that refresh
the panel stay on the app's own client, like views.

### 6.6 ACLs disabled

With ACLs off every caller is `acl-disabled`; the header is ignored, and the
action runs as it did. Nothing is enforced, so nothing is lost.

### 6.7 Agents

An agent calling `InvokeAppAction` is the viewer; the action's writes are
recorded as `<agent token name> via app <app>`.

## 7. Proto, struct, config, and persistence changes

- **No proto fields.** The header is gRPC metadata, documented in
  `docs/auth-and-acls.md` and in the proto file's header comment;
  `InvokeAppAction`'s and `WhoAmIReply.source`'s comments gain the
  delegation and `delegated`.
- `ViewerAssertion.delegation: Option<Delegation>` (new optional claim).
- `AclSource::Delegated`.
- `ActionRequest` keeps the raw assertion; `Viewer.delegation`;
  `ActionRequest::client_as_viewer`, `client_as_viewer_over`.
- `cmd::helpers::ManagerConnection` (a channel plus the credential, from
  which the app's own client and viewer clients are made);
  `AuthInterceptor` stamps `legion-on-behalf-of` when built for a viewer.
- No config, no job-config fields, no persistence, no migration.

## 8. Implementation phases

1. **Mint and honor** (manager): the claim, the header, the delegated
   identity, the refusals, app-plane, worker-plane and stream guards.
   Valuable alone: any app can opt in by sending the header by hand.
2. **Framework helper**: `client_as_viewer`, the raw assertion on
   `ActionRequest`, `ManagerConnection`. Valuable alone: Rust apps get it in
   one call.
3. **The autoscaler uses it**: Pause, Resume and Release as the viewer; the
   `by <viewer>` reason workaround removed.

All three ship together in one change; each is independently testable.

## 9. Testing plan

- **Unit** (`src/manager/delegation.rs`, `src/structs/app_ui.rs`): the
  synthesized rule allows exactly the scope and actions; the intersection
  drops what the app lacks; deny rules carry over; a `*` in a scope value is
  refused; the claim round-trips and view assertions refuse one.
- **Integration** (`tests/integration/app_action_delegation.rs`, in-process
  manager with ACLs and workload identity on, a fake app on loopback that
  calls back through `client_as_viewer_over` against the service on a port):
  1. a delegated call succeeds on the context and is recorded as
     `<viewer> via app <app>`;
  2. the same assertion on another job, or for `job:write`, is refused;
  3. presented by another app's identity, a management token, or the
     viewer's own token, it is refused;
  4. an expired assertion is refused;
  5. revoking the viewer's token fails the delegation at once;
  6. a delegation never exceeds the app's grant;
  7. streams and app-plane endpoints refuse the header.
- **The autoscaler** (`tests/integration/autoscaler_ui.rs`): its pause
  through `InvokeAppAction` is recorded by `alice via app autoscaler`, in
  status and history; the views call with the app's real `legion` token.
- `acl_endpoints::` keeps passing (no new RPC).

## 10. Rollout and backward compatibility

No installed base (AGENTS.md): this changes what an app action records and
what an app may do with a press, on every cluster, with no flag. Apps built
on the framework that keep using their own client behave exactly as before;
the autoscaler switches. Third-party apps in other languages opt in by
sending the header. An assertion minted by an older manager carries no
delegation, so a mixed-version cluster refuses the viewer client's calls
(`PermissionDenied: … carries no delegation`) until every manager runs the
new build; the autoscaler's actions fail during that window, and its views do
not.

## 11. Open questions

None open. Resolved during implementation: see the calls doc.

## 12. Acceptance criteria

- A UI pause on an autoscaled job shows `by alice via app autoscaler` in
  `legion scaling status`, `legion scaling history`, and the `scale.paused`
  event.
- A call with the header on another job, another resource, by another
  credential, after expiry, or after the viewer's token is revoked is
  refused with a message naming why, and an `acl_audit` line.
- `WhoAmI` with the header reports `source = delegated` and the one
  synthesized rule.
