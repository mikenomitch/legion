# TODO close-out (2026-11-11): judgement calls

Calls made while closing open items in [`TODO.md`](../TODO.md) that have no
plan of their own. Calls that belong to a plan live in that plan's calls doc
and are cited from the Done entries. Entries are ranked by project impact,
highest first; the number is the rank.

## 1. Content-budget floors give way in proportion when they don't fit the disk

**The call.** When the default budgets (50% / 20% / 10% of the filesystem,
with floors of 1 / 2 / 1 GiB) add up to more than the filesystem, each budget
a floor raised above its share is lowered toward that share, in proportion to
how far the floor raised it. No budget goes below its share, and a budget set
to a fixed size is never touched (`fit_floors_to_disk` in
`crates/common/src/content_store.rs`). On a 1.2 GB disk the defaults come to
about 634 / 377 / 189 MB; disks of 8 GiB and up are unchanged.

**The alternatives.** Cap each floor at the disk size, which still lets the
three budgets add up past the disk. Or drop the floors entirely once they
don't fit, which makes every budget jump at one disk size.

**The trade-off.** On a small disk the budgets are smaller than the floors
promise, so a large checkpoint or derived base that fit before can now be
refused for room. `minFreeDisk` still applies on top.

**Why.** Simplicity: the budgets never promise more than the disk has, and
the change is continuous, so nothing shifts abruptly at a threshold and no
disk that already fit sees a different number.

## 2. `legion ledger --job` filters on the client

**The call.** `legion ledger --job <JOB>` asks the manager for the whole
ledger (`GetLedger`, unchanged) and keeps that job's allocations, and only
the nodes that hold one.

```console
$ legion ledger --job web -j
```

**The alternative.** A `job` field on `GetLedgerRequest`, or a new
`ListAllocations` RPC scoped by `job:read`.

**The trade-off.** The reply still carries every node the caller can read, so
on a large fleet the command costs a whole-ledger read, and it needs
`node:read` rather than `job:read`. The docs say not to poll it.

**Why.** Simplicity: it closes "no CLI lists a job's allocations" with no new
RPC, ACL entry, or plan. A server-side filter is a compatible addition later,
when a caller with only `job:read` needs it.

## 3. Plugins share one target dir; artifacts are copied out by cargo's own report

**The call.** `build.rs` builds every plugin into
`target/plugin-build/shared/` and then copies each plugin's own artifacts
(its dylib, or the binary of an exec-style plugin such as `aws-asg`) into
`target/plugin-build/<kind>/<name>/release/`, the layout the loader and the
rig's bundle script already read. Which files are the plugin's own comes from
`cargo build --message-format=json` (`compiler-artifact` lines whose
`manifest_path` is the plugin's), read with a few lines of string matching,
since the build script has no JSON parser.

**The alternatives.** Point the loader at the shared directory instead, or
guess artifact names from each `Cargo.toml`.

**The trade-off.** Two plugins whose packages share a name overwrite each
other's outputs in the shared dir; they build in a fixed order and each is
copied out before the next builds, so the result is right but the second
rebuilds the first's crate. The JSON reading handles only the escapes a path
carries.

**Why.** Performance of the developer loop: the ~470 crates the plugins have
in common compile once rather than once per plugin, and no runtime code or
deploy script changes. Reading cargo's report rather than guessing names
keeps binaries, `cdylib`s and `dylib`s right without a rule per crate type.

## 4. The lossy log writers queue 16,384 lines

**The call.** The console, file, and span writers queue at most 16,384 lines
each (`LOG_BUFFER_LINES` in `crates/common/src/tracing_util.rs`), down from
`tracing_appender`'s default of 128,000.

**The alternative.** Keep the default, or size the bound in bytes.

**The trade-off.** A reader that falls behind for longer loses lines sooner.
The writers exist so that a slow reader never stalls a heartbeat, so a dropped
line is already the designed outcome.

**Why.** Performance: the bound caps what a stuck reader can hold in memory
per writer, and 16,384 lines still absorbs the 1,200-start burst the writers
were built for. `tracing_appender` bounds lines, not bytes, so a byte bound
would need a wrapper.
