# Judgement calls: bounded-candidate-scoring-plan

Plan: [`../plans/bounded-candidate-scoring-plan.md`](../plans/bounded-candidate-scoring-plan.md)

## 1. Opt-in, exact-by-default — against the industry default

- **The call:** `score_sample` ships default-off; Legion keeps exact
  scoring at every fleet size unless the operator arms the knob.
- **Alternatives:** Nomad and Kubernetes both sample *by default*
  (log2 n / ~10%); shipping their default would make the flat curve the
  out-of-box story.
- **Trade-off:** out-of-box benchmark numbers at very large fleets show
  the O(feasible) scoring curve until the knob is armed; competitors'
  defaults look faster at 50k+ nodes.
- **Why:** exact placement is a stated differentiator ("exact when it's
  cheap, bounded when it matters"), placement-quality changes must never
  arrive silently in an upgrade, and the honest quality-cost numbers
  this plan records are what make arming the knob an informed decision.
  (Simplicity of the contract; honesty of the public claim.)

## 2. `score_sample` added beside `nodes_considered` instead of replacing it

- **The call:** the existing `nodes_considered` knob (unconditional
  shuffle-truncate) stays; `score_sample` is a new threshold-gated knob;
  when both are set, `nodes_considered` applies first.
- **Alternatives:** extend `nodes_considered` with a `min_nodes` gate;
  deprecate it in favor of `score_sample`.
- **Trade-off:** two overlapping knobs an operator can combine
  senselessly (documented as not recommended).
- **Why:** `nodes_considered` is shipped surface with its own semantics
  (a hard cap, meaningful *below* any threshold) and tests; silently
  changing its meaning breaks existing configs, and removal is a
  deprecation exercise this plan does not need. The two compose
  deterministically. (Extensibility without breaking shipped surface.)

## 3. Fixed sample size, not adaptive percentage

- **The call:** `sample` is a constant count (default suggestion 128),
  not a K8s-style percentage that adapts to fleet size.
- **Alternatives:** percentage with floor (K8s), log2 n (Nomad).
- **Trade-off:** one number cannot be ideal at both 2k and 200k nodes;
  operators of huge fleets may want to raise it.
- **Why:** power-of-k theory says quality depends on k, not on k/N — a
  fixed k is the honest knob, trivially explainable ("we score the best
  of 128 random feasible nodes"), and flat by construction. Percentages
  reintroduce the O(fleet) growth this feature exists to remove.
  (Performance with simplicity.)

## 4. Sampling seam is `select_node_id`, after all feasibility filters

- **The call:** the sample is drawn from the per-placement `eligible`
  list (post permit/distinct/resource-fit), so a sampled node is always
  a valid placement; only score *ranking* is approximated.
- **Alternatives:** sample earlier (pre-feasibility, like K8s's
  node-percentage) to also bound constraint evaluation.
- **Trade-off:** feasibility filtering stays O(candidates); at very
  large fleets with weak constraints the filter pass itself still grows
  (the scheduling index is the existing answer to that).
- **Why:** sampling before feasibility can strand a placement (sample
  contains no feasible node → false "no capacity"), which is a
  correctness-shaped failure, not a quality trade. Composing
  index-narrowing (feasibility) with score sampling (ranking) keeps each
  mechanism sound on its own. (Correctness first; mechanisms compose.)
