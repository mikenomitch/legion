# Judgement calls: gated networking on macOS

For [macos-gated-plan.md](../plans/macos-gated-plan.md). Ranked by project
impact, highest first.

## 1. Ship it before running it on a Mac, behind an explicit `gated`

- **The call.** Implement phases 1–3 with every macOS command built by a pure
  function tested on Linux. Take the new path only for an explicit
  `network.mode = "gated"` on a macOS node, which used to fail at start.
  `auto` keeps resolving to `host` on macOS.
- **Alternatives.**
  - Wait for macOS hardware before writing any of it.
  - Make it the default (`auto` becomes `gated` for VMs on macOS) now.
- **Trade-off.** The code is unverified where it runs. An open question
  (rdr to `127.0.0.1` without forwarding) could mean a phase-4 fix.
- **Why it won.** The surface and the failure modes are the same as Linux
  gated (simplicity), the pure parts are tested, and the worst case of a
  wrong assumption is a start failure that names the step. A gated task never
  runs unconfined, because every step fails closed: the base `pf` block lands
  before the VM's address is known, and a missing Seatbelt fails the start.
  Defaulting it on before a Mac has run it would put that risk on every Mac
  user.
- **The user sees:**

  ```
  network = { mode = "gated", ... }    # macOS: confined; before: refused at start
  ```

## 2. One meaning for `gated`, two mechanisms

- **The call.** Keep `network.mode = "gated"` and make it mean the same thing
  on macOS ("the datapath is the only way out"). Implement it with Seatbelt
  for host processes and `pf` for VMs, and add no macOS-only jobspec field.
- **Alternatives.**
  - A new mode (`confined`, `macos-gated`).
  - Per-runtime knobs (`config.seatbelt`, `config.pf`).
- **Trade-off.** "Gated" covers three enforcement mechanisms, which an
  operator debugging a Mac must know about (`docs` names them).
- **Why it won.** One jobspec runs the same way on a laptop and in
  production, which is the developer's whole job to be done. A second mode
  name would be a dual surface for one intent (simplicity).

## 3. Seatbelt over a Network Extension for `exec`

- **The call.** Confine a host process with `sandbox-exec` and an SBPL
  profile.
- **Alternatives.**
  - A Network Extension content filter (`NEFilterDataProvider`).
  - `pf` rules keyed by user (`user` in pf), with a per-allocation uid.
- **Trade-off.** `sandbox-exec` is deprecated: Apple could remove it, and
  then a confined `exec` fails to start rather than running unconfined.
- **Why it won.**
  - **A Network Extension** needs a signed, notarized system extension and a
    user approval, which no dev-mode worker can ship.
  - **`pf` by user** needs root plus a user per allocation, and still cannot
    stop a process from binding `0.0.0.0`.
  - **Seatbelt** needs neither, confines the process tree, and is what
    Chrome and Claude Code rely on today (ease).

## 4. A fixed gated subnet, `192.168.239.0/24`

- **The call.** The gated network's subnet, name (`legion-gated`) and anchor
  (`com.apple/legion`) are constants.
- **Alternatives.** A worker config block (`network.macos { subnet = … }`).
- **Trade-off.** A Mac already using `192.168.239.0/24` (a VPN, another VM
  tool) conflicts. The fix then needs a code change, until the knob exists.
- **Why it won.** Each knob is surface to document and support. This one
  has no user yet, and the phase-4 run on a real Mac is what tells us whether
  anyone needs it (simplicity). It is in the plan's deferred list.

## 5. No DNS for confined tasks

- **The call.** Seatbelt denies the `mDNSResponder` socket, and the `pf`
  block hides vmnet's resolver. A confined task resolves nothing itself.
- **Alternatives.**
  - Allow system DNS.
  - Run a per-allocation resolver on a loopback port (Linux gated has one
    when the task declares upstreams).
- **Trade-off.** A tool that resolves names before using a proxy fails. So
  does `localhost` by name inside an `exec` task: macOS resolves it through
  `mDNSResponder`. The env vars use `127.0.0.1`.
- **Why it won.** Allowed DNS is an exfiltration channel past the policy,
  and a per-allocation resolver on macOS is new machinery for a case Linux
  gated already handles without one (proxy-aware tools send names to the
  proxy). It is deferred, not refused.
