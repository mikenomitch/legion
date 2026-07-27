# Judgement Calls — Workload Exec & Actions

Companion to [`plans/exec-and-actions-plan.md`](../plans/exec-and-actions-plan.md).
These were the plan's eight open questions; all were decided before
implementation. Each entry: the call and the rationale.

Entries are ranked by project impact, highest first.

- **Q3 — sensitive params: yes, in the initial schema.**
  `params[].sensitive: true` redacts the value in audit events, CLI echo,
  and UI history. Added now because events are persisted — retrofitting
  redaction cannot fix already-written history.
- **Q6 — job-scoped target: newest running alloc, with a stateful guard.**
  `legion exec --job` / `legion action run <job>` without `--alloc` picks
  the newest *running* allocation and prints which one it picked. When the
  target task is stateful (allocations are not interchangeable), an explicit
  `--alloc` is required. Random selection (Nomad's choice) hides the
  decision from the operator; newest-with-echo keeps it debuggable.
- **Q1 — youki exec: ship without it.** Container exec lands for the docker
  and Apple `container` runtimes; the youki path reports
  `exec_support().exec = false` (honest capability reporting, S4). Youki
  exec needs its own flag handling and its image-loading story is itself
  still pending — blocking every other surface on it buys nothing.
- **Q2 — selector naming: `action_names`.** ACL rules already carry an
  `actions: ["read","write"]` field; reusing `actions` as a scope selector
  would make one JSON key mean two things in the same object. Verbose but
  unambiguous, and consistent with the plural selector style (`names`,
  `paths`, `pools`).
- **Q8 — staging allocations: active-only in v1.** Exec targets `active`
  allocations; anything else fails with the allocation's current state in
  the error. Exec-into-staging is deferred until the artifact-cache plan
  settles staging observability.
- **Q4 — Windows workers: experimental.** `portable-pty` gives us ConPTY,
  but with zero Windows CI the honest label is experimental; documented as
  such in the capability tables. No code is gated — it just isn't promised.
- **Q5 — libghostty: xterm.js now, revisit on trigger.** Adopt a
  ghostty-based renderer only when (a) an official libghostty-vt WASM
  release exists and (b) a maintained renderer passes a terminal acceptance
  check (vim, resize, mouse, 256color). The `<Terminal>` component keeps to
  the xterm.js-compatible surface so the swap stays a one-file change.
- **Q7 — SSH gateway: nothing extra needed now.** The frame protocol already
  carries everything an SSH front-end would bridge (argv, tty flag,
  winsize/resize, stdin half-close, in-band exit code, `session_id`). A
  future gateway is additive; out of scope here.
