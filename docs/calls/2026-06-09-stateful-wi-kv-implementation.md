# Judgement Calls — Stateful Workloads, Workload Identity, Reactive KV (2026-06-09)

Decisions made while implementing the three plans in `docs/plans/` where the plans
were ambiguous, contradictory, or silent. Each entry: the call, the alternatives,
and why.

## 1. "CNI compatibility layer" in the volume plugin plan → CSI

The request asked for a volume plugin system "similar to CNI or Nomad's volume
plugins" with "a CNI compatibility layer (optional)". CNI is the *networking* plugin
standard; the storage equivalent is **CSI**. We interpreted the intent as two separate
things and did both in the plan ([volume-plugin-system-plan.md](../plans/volume-plugin-system-plan.md)):

- Adopt the **CNI-style exec-plugin model** (JSON over stdin/stdout, versioned
  contract, manifest + bin-dir discovery) as the native volume ABI.
- Provide an optional **CSI compatibility bridge** so the existing storage-driver
  ecosystem is reachable.

Alternative considered: literally bridging to CNI plugins — rejected as a category
error (CNI plugins configure network interfaces, not storage).

## 2. Volume plugins use an exec ABI, not the existing `cdylib` ABI

Legion's existing plugins (task kinds, scheduling, node checks, rollouts) are
in-process `cdylib`s. The volume plan deliberately diverges to CNI-style external
executables because volume ops are low-frequency, frequently shell out to privileged
tools, and must not crash the worker. The plan ships `local-dir` and `host` as
in-process built-ins behind the same trait so the zero-dependency path needs no
external binaries.

---

*(Further entries are appended below as implementation proceeds.)*
