# Judgement Calls: app-actions-as-viewer-plan

Decisions made while designing and building
[`plans/app-actions-as-viewer-plan.md`](../plans/app-actions-as-viewer-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.**

This plan supersedes the "the action runs as the app" half of
[apps calls entry 68](legion-apps-plan_calls.md) (noted there as entry 98)
and [autoscaler-v2 calls entry 83](autoscaler-v2-plan_calls.md).

## 1. The delegation rides on the viewer assertion the manager already signs, presented back in a header

**Call.** `InvokeAppAction` adds a `delegation` claim to the action's viewer
assertion (the declared resource, the declared action plus `read` when the
viewer holds it, and the exact scope checked). The app presents the same JWT
back on its own calls in one header, alongside its own credential:

```text
authorization: Bearer <the app's legion token>      (the API socket attaches it)
legion-on-behalf-of: <the action's viewer assertion>
```

The manager turns the pair into a delegated identity,
`alice via app autoscaler`.

**Alternatives.** (a) RFC 8693 token exchange: a `ExchangeViewerAssertion`
RPC that mints a separate delegated token. (b) OpenShift console plugins'
model: forward the viewer's own token to the app. (c) Kubernetes
impersonation: a standing `impersonate` grant on the app, and an
`Impersonate-User` header naming whom to act as.

**Trade-off.** The assertion now does two jobs (telling the app who pressed,
and authorizing the app's calls), so its claim set grows and its
verification lives in two places (framework and manager). A third-party app
must send a Legion-specific header rather than a standard token-exchange
grant.

**Why (simplicity, security boundary).** (a) adds an RPC, a second token
type, and a round trip per press for nothing the signed assertion lacks:
it is already short-lived, audience-bound to the app, context-bound and
inputs-bound. (b) hands the app every right the viewer has, for the token's
whole life; a press would be as dangerous as a stolen session. (c) lets the
app become anyone, with all of their rights, at any time, which is the
opposite of "this button, this job, this minute". One header on top of an
artifact that already exists is the smallest surface that makes the
manifest's declared permission an enforced bound.

## 2. Only the app the assertion was signed for may present it

**Call.** The caller must authenticate as the app: its credential carries a
job-grant reference (`grant:alloc/<id>`) to an allocation of
`legion-system/<app>`, the test `is_autoscaler_identity` used, generalized
into `app_of_identity` (which `is_autoscaler_identity` and
`WatchAppExtensions`' `calling_app` now share). The assertion is verified
for `aud = app:<that app>`. A management token, a person's token, or another
app presenting someone's assertion is refused:

```text
PermissionDenied: legion-on-behalf-of: only an app's own identity presents a viewer assertion; this credential (bootstrap) is not an app's
Unauthenticated: legion-on-behalf-of: the viewer assertion does not verify for app gitops: …InvalidAudience
```

**Alternatives.** (a) Bearer semantics: whoever holds the assertion may use
it. (b) Also accept management tokens, as `is_autoscaler_identity` does for
the app plane.

**Trade-off.** An app with no grant cannot use delegation at all (it has no
grant reference, and nothing to intersect with anyway), and an operator
cannot replay a press by hand to debug it.

**Why (security).** Assertions travel through the worker plane and app code;
(a) would make any leak a 60-second credential. (b) would let the most
powerful credential on the cluster borrow a person's name in the audit
trail, which is the record this feature exists to make trustworthy.

## 3. The delegated identity is one synthesized `AclRule`, intersected with the app's grant per action, plus both parties' deny rules, and `AclSource::Delegated`

**Call.** The delegated identity has no policies, is not management, and
holds:

- one allow rule: the delegated resource, the actions that survive, and
  exact selectors for the scope (`namespaces = [ns]`, `names = [job]`,
  `templates = [t]` when the job came from one, or `pools = [pool]`);
- the app's deny rules and the viewer's (when re-resolved), so the allow
  rule never reaches what either is denied.

An action survives only if the app's own identity allows it on the
delegated scope; with none left the call is refused. The identity's
`source` is a new `AclSource::Delegated` (`WhoAmI` reports `delegated`), so
app-plane endpoints and `calling_app` can recognize and refuse it without
string matching.

**Alternatives.** A `delegation` field on `AclIdentity` that `allows` and
`would_allow` AND with the caller's own evaluation on every check: exact
intersection on every request, whatever attributes it carries.

**Trade-off.** The intersection is computed once, on the delegated scope,
not per request. Request attributes the synthesized rule has no selector for
(`bands`, `actionNames`, `host`) are covered by an argument rather than a
check: a held allow rule that matches the scope as checked (which carries
none of them) cannot name them either, so it covers every value; and a
`host` scope is never matched by a rule without `host = true`, so a
delegation never reaches a host-level job write. The deny rules are carried
over precisely because a deny with such a selector is the one case the
argument misses. Revisit if `AclScope` grows an attribute that is set on
reads.

**Why (simplicity).** `AclIdentity` is built as a struct literal in dozens of
places, and every check path (`allows`, `would_allow`, `namespaces_granting`,
`effective_rules_json`) would need to learn the AND. A real rule makes the
delegated identity an ordinary identity: `enforce` and its audit log,
`WhoAmI.rules_json`, list filtering, and denial hints all work unchanged.

## 4. The viewer's token is re-resolved on every delegated call

**Call.** When the assertion names the viewer's ACL token accessor, every
call made with it re-resolves that token
(`Authenticator::identity_for_accessor`): deleted is `Unauthenticated`,
expired is `Unauthenticated`, an action the viewer no longer holds on the
scope is dropped, and losing the declared one is `PermissionDenied`. A
viewer who authenticated with a JWT (SSO bearer) has no accessor, and the
60-second TTL bounds them.

**Alternatives.** Rely on the 60-second TTL for everyone.

**Trade-off.** One token and policy lookup per delegated call, on the
store's read path.

**Why (security).** "Revoke the token" must mean the person can do nothing
more, at once. Sixty seconds of a revoked person's writes through an app is
exactly the window an incident responder revokes to close.

## 5. The manager does not make an assertion single-use; the app may use it for its 60 seconds

**Call.** The framework spends an action assertion when the action runs
(apps calls entry 69), but the manager does not track it: the app may make
any number of delegated calls with it until it expires.

**Alternatives.** Track spent `jti`s on the manager, per manager or
replicated.

**Trade-off.** A compromised or buggy app can repeat the delegated call (on
the same context, same resource) within the minute.

**Why (ease, simplicity).** A handler legitimately makes several calls with
one press: Release ends every pin of a job, one `ScaleTask` per task, and an
app may read before it writes. Single-use per call would need one assertion
per call; a replicated spent-set would put a raft write on every delegated
call. The repeated call can do nothing the first could not.

## 6. Streams refuse the header

**Call.** `StreamEvents`, `WatchScalingPolicies`, `WatchAppExtensions`,
`ExecAllocation` and `StreamWork` refuse a request carrying
`legion-on-behalf-of`:

```text
PermissionDenied: legion-on-behalf-of covers single calls for the assertion's 60 seconds; streams take the caller's own credential
```

**Alternatives.** Carry the header into the stream's credential, so the
30-second re-authentication re-verifies the delegation and ends the stream
when the assertion expires.

**Trade-off.** An app cannot watch anything as the viewer.

**Why (security, simplicity).** Re-authentication goes through the
`Authenticator` with the bearer credential alone; without this refusal a
delegated stream would quietly become the app's own identity at its first
re-check. A stream that dies within a minute is useless anyway, and no
action needs one.

## 7. A bad delegation is always refused, also in audit-only mode, and never falls back to the app

**Call.** Every failure (no workload identity, not the app, bad signature,
wrong audience, expired, not an action, no delegation, viewer revoked,
nothing left after the intersection) is `Unauthenticated` or
`PermissionDenied`, logged under `acl_audit` as an `ACL denial` with
`enforced = true`.

**Alternatives.** In audit-only mode, log and continue as the app's own
identity, as `enforce` does for policy denials.

**Trade-off.** An audit-only cluster can refuse an app's call it would
otherwise have allowed.

**Why (security).** A failed delegation is a failed credential, not a policy
miss: the token checks never honor audit-only either. Falling back to the
app would silently re-create the problem this plan removes.

## 8. The delegation adds `read` on the same resource when the viewer holds it, and nothing else

**Call.** `actions` is the declared action, then `read` on the same resource
and scope if the viewer holds it at mint.

**Alternatives.** (a) The declared action only. (b) Everything the viewer
holds on the context.

**Trade-off.** An action declared `scale:write` can read the job's scaling
state as the viewer, which its declaration did not name.

**Why (ease).** Most writes are preceded or followed by a read of the same
thing (a `ScaleTask` reply, a status refresh), and a viewer who may write
but not read is rare; (a) would push every handler back to its own client
for that read. (b) would turn a button into a session.

## 9. With ACLs disabled the header is ignored

**Call.** When `acl.enabled = false`, `authenticate_acl` returns the
`acl-disabled` identity as before and does not look at the header.

**Alternatives.** Verify the assertion anyway and record
`<viewer> via app <app>`.

**Trade-off.** On an ACL-disabled cluster a UI pause is recorded by
`acl-disabled`, as is everything else.

**Why (simplicity).** With ACLs off there is no viewer identity to name
(InvokeAppAction's viewer is `acl-disabled` too), and nothing to enforce.

## 10. The manager checks expiry with no leeway

**Call.** After the signature verifies, the manager refuses an assertion
whose `exp` is not after its own clock, and one whose life is longer than
two TTLs.

**Alternatives.** `jsonwebtoken`'s default 60-second leeway, or the app
framework's 30 seconds of skew.

**Trade-off.** A manager whose clock runs ahead of the minting manager's
shortens the window by the skew.

**Why (security).** The assertion is minted and honored by the same
cluster's managers, whose clocks raft already expects close; leeway would
double a 60-second credential.

## 11. The delegated identity has no accessor, and expires with the assertion

**Call.** `accessor_id` is unset (the viewer's accessor appears in the
`acl_audit` and debug lines instead); `expires_at` is the earlier of the
assertion's `exp` and the app credential's expiry.

**Alternatives.** Carry the viewer's accessor.

**Trade-off.** Records keyed by accessor (idempotency scoping) key the
delegated principal instead.

**Why (security).** Exec tickets, session and boot tickets, and
`scoped_idempotency_key` treat an accessor as "this person's token". A
delegation carrying it could mint a ticket that is later re-checked as the
viewer's full token, outliving the delegation.

## 12. The autoscaler delegates its writes and keeps its reads on its own client

**Call.** Pause, Resume and Release call `PauseScaling` and `ScaleTask`
through `client_as_viewer_over`. The status read before acting and the panel
refresh after stay on the app's client, like views. The reason loses
`by <viewer>`: `paused from the UI`.

**Alternatives.** Every call of the handler through the viewer client.

**Trade-off.** The precondition read ("is anything pinned?") sees what the
app sees, not what the viewer sees; on a context the viewer may write but
not read that is still only the context's own job.

**Why (ease).** The refreshed panel is a view, and views read with the
app's grant (non-goal of the plan); a viewer without `scale:read` still
gets a sensible panel back.

## 13. A connection, not a client, is what an app holds

**Call.** `cmd::helpers::ManagerConnection` (a channel and the credential it
presents) makes the app's own client (`client()`) and viewer clients
(`client_on_behalf_of(assertion)`) over one HTTP/2 connection.
`AppContext::connection()` dials `LEGION_ADDR`; `ActionRequest` offers
`client_as_viewer(&context)` and `client_as_viewer_over(&connection)`.

**Alternatives.** (a) A request-level helper,
`request.as_viewer(message) -> tonic::Request`, for the app's existing
client. (b) Only `client_as_viewer(&context)`, dialing `LEGION_ADDR` from
the environment.

**Trade-off.** One more public type in `cmd::helpers`.

**Why (ease).** (a) works, but one forgotten wrapper silently runs a call as
the app; a client that always sends the header is the safer default. (b)
cannot be tested in process, where the integration tests serve one manager
on a port and all share one environment.

## 14. A scope value with `*` in it gets no delegation

**Call.** At mint, a namespace, job, template or pool name containing `*`
(other than the `unowned` marker) is refused with `InvalidArgument`, and the
manager refuses such a claim again when honoring it.

**Alternatives.** Escape globs in the synthesized rule's selectors.

**Trade-off.** None in practice: no real namespace, job or pool can be
named so; the context of a job need not exist, which is the only way to
reach this.

**Why (security).** ACL selectors are globs with no escape; a literal value
`web*` would become a wildcard over every `web…` job.

## 15. The `app.action` event still names the viewer, and the delegation shows on what the action did

**Call.** `InvokeAppAction`'s `app.action` event keeps `by = alice`; the
writes the app makes on her behalf are recorded by `alice via app
autoscaler` (the `scale.paused` event, `scaling history`, `scaling status`).

**Alternatives.** Name the delegation (resource, actions, scope) in the
`app.action` event's meta.

**Trade-off.** An auditor reads two records to see what the press allowed and
what it did.

**Why (simplicity).** The event records who pressed what; the delegation is
what the app was allowed to do, which the manifest and the context already
determine. Left as a follow-up if audits ask for it.
