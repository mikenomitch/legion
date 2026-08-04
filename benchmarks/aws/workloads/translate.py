#!/usr/bin/env python3
"""Translate a benchmark scenario into per-system workload artifacts.

One scenario file (benchmarks/scheduler/scenarios/*.json — the schema in
tests/benchmark_support/mod.rs) becomes:

  legion : one Nickel jobspec per job (.ncl, submitted with
           `legion job run <file>`)
  nomad  : one .nomad.hcl job per job (mock driver — nodesim fleets
           advertise it, so placements stay pure-scheduler)
  k8s    : one Deployment manifest per (job, task) as JSON (YAML is a
           superset; kubectl applies JSON), nodeSelector'd onto KWOK
           fake nodes

plus a manifest.json the driver reads: every artifact with its expected
allocation count, so completeness is asserted, never assumed.

Resource units: scenario `cpu` is MHz and `memory` MiB (Legion pool
units). Nomad uses the same units natively. K8s requests are converted
at the fleet's fingerprinted 3000 MHz/core (see fingerprint_for in
src/worker/simulate.rs): cpu_millicores = MHz / 3. That keeps the
*bin-packing pressure* — task share of a node — identical across
systems, which is the property that matters for a scheduler benchmark.

Constraint mapping (scenario constraints are attr-equality):
  legion : verbatim (`constraints: { class: "web" }`)
  nomad  : class -> ${node.class}; anything else -> ${meta.<key>}
           (nodesim node templates set class + meta accordingly)
  k8s    : nodeSelector labels (KWOK nodes are labeled from the same
           node_profiles file)

Usage:
  translate.py --scenario ../scheduler/scenarios/web_fleet.json \
      --system legion --out out/legion [--count-multiplier 2.0]
"""

import argparse
import json
import math
import os
import sys

PAUSE_IMAGE = "registry.k8s.io/pause:3.9"
MHZ_PER_CORE = 3000  # matches the sim fleet's fingerprinted frequency


def load_scenario(path):
    with open(path) as f:
        scenario = json.load(f)
    return scenario


def effective_job_types(scenario):
    """v1 uniform fields normalize to one job type, like the Rust runner."""
    if scenario.get("job_types"):
        return scenario["job_types"]
    if scenario.get("job_count") is None:
        raise ValueError(f"scenario {scenario.get('name')}: need job_types or job_count")
    return [
        {
            "name": "uniform",
            "count": scenario["job_count"],
            "tasks_per_job": scenario.get("tasks_per_job", 1),
            "task_count_per_task": scenario.get("task_count_per_task", 1),
            "resources": None,
            "constraints": None,
        }
    ]


def scaled_count(count, multiplier):
    return max(1, math.ceil(count * multiplier))


def iter_jobs(scenario, multiplier):
    """Yield (job_name, job_type, job_idx) for every job in the workload."""
    for job_type in effective_job_types(scenario):
        for idx in range(scaled_count(job_type["count"], multiplier)):
            yield f"bench-{job_type['name']}-{idx}", job_type, idx


def expected_allocations(job_type):
    return job_type["tasks_per_job"] * job_type["task_count_per_task"]


# --- Legion -----------------------------------------------------------


def legion_jobspec(job_name, job_type):
    tasks = []
    for task_idx in range(job_type["tasks_per_job"]):
        task = {
            "name": f"t{task_idx}",
            "runtime": "noop",
            # A bare number; the jobspec's `count` is number-or-{num,per}.
            "count": job_type["task_count_per_task"],
            # Distinct groups so each task places independently (the
            # default group co-places a job's tasks on one node).
            "group": f"g{task_idx}",
        }
        if job_type.get("resources"):
            task["resources"] = job_type["resources"]
        if job_type.get("constraints"):
            task["constraints"] = job_type["constraints"]
        tasks.append(task)
    return {"name": job_name, "tasks": tasks}


# Nickel keywords that cannot appear as bare record field names.
NICKEL_KEYWORDS = {
    "if", "then", "else", "forall", "in", "let", "rec", "match", "null",
    "true", "false", "fun", "import", "merge", "default", "doc", "optional",
    "priority", "force", "not_exported",
}


def nickel(value, indent=0):
    """Render a JSON-shaped Python value as Nickel source.

    JSON is not a subset of Nickel — records use `=` rather than `:` — so
    the jobspecs are emitted rather than dumped.
    """
    pad = "  " * indent
    inner = "  " * (indent + 1)
    if isinstance(value, dict):
        if not value:
            return "{}"
        fields = []
        for key, item in value.items():
            name = (
                key
                if key.isidentifier() and key not in NICKEL_KEYWORDS
                else json.dumps(key)
            )
            fields.append(f"{inner}{name} = {nickel(item, indent + 1)},")
        return "{\n" + "\n".join(fields) + f"\n{pad}}}"
    if isinstance(value, list):
        if not value:
            return "[]"
        items = [f"{inner}{nickel(item, indent + 1)}," for item in value]
        return "[\n" + "\n".join(items) + f"\n{pad}]"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return json.dumps(value)


# --- Nomad ------------------------------------------------------------


def nomad_constraint(key, value):
    attribute = "${node.class}" if key == "class" else "${meta.%s}" % key
    return (
        "    constraint {\n"
        f'      attribute = "{attribute}"\n'
        f'      value     = "{value}"\n'
        "    }\n"
    )


def nomad_job(job_name, job_type):
    resources = job_type.get("resources") or {}
    cpu = int(resources.get("cpu", 100))
    memory = int(resources.get("memory", 64))
    constraints = job_type.get("constraints") or {}

    groups = []
    for task_idx in range(job_type["tasks_per_job"]):
        constraint_blocks = "".join(
            nomad_constraint(key, value) for key, value in sorted(constraints.items())
        )
        groups.append(
            f'  group "g{task_idx}" {{\n'
            f"    count = {job_type['task_count_per_task']}\n"
            f"{constraint_blocks}"
            f'    task "t" {{\n'
            f'      driver = "mock"\n'
            f"      config {{}}\n"
            f"      resources {{\n"
            f"        cpu    = {cpu}\n"
            f"        memory = {memory}\n"
            f"      }}\n"
            f"    }}\n"
            f"  }}\n"
        )

    return (
        f'job "{job_name}" {{\n'
        f'  datacenters = ["dc1"]\n'
        f'  type = "service"\n'
        + "".join(groups)
        + "}\n"
    )


# --- Kubernetes -------------------------------------------------------


def k8s_deployment(job_name, job_type, task_idx):
    resources = job_type.get("resources") or {}
    millicores = max(1, round(int(resources.get("cpu", 100)) * 1000 / MHZ_PER_CORE))
    memory_mi = int(resources.get("memory", 64))

    node_selector = {"type": "kwok"}
    for key, value in (job_type.get("constraints") or {}).items():
        node_selector[key] = str(value)

    name = f"{job_name}-g{task_idx}"
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "labels": {"bench": "1", "bench-job": job_name}},
        "spec": {
            "replicas": job_type["task_count_per_task"],
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name, "bench": "1", "bench-job": job_name}},
                "spec": {
                    "nodeSelector": node_selector,
                    "tolerations": [
                        {
                            "key": "kwok.x-k8s.io/node",
                            "operator": "Equal",
                            "value": "fake",
                            "effect": "NoSchedule",
                        }
                    ],
                    "containers": [
                        {
                            "name": "t",
                            "image": PAUSE_IMAGE,
                            "resources": {
                                "requests": {
                                    "cpu": f"{millicores}m",
                                    "memory": f"{memory_mi}Mi",
                                }
                            },
                        }
                    ],
                },
            },
        },
    }


# --- Driver manifest --------------------------------------------------


def translate(scenario, system, out_dir, multiplier):
    os.makedirs(out_dir, exist_ok=True)
    manifest = {
        "scenario": scenario.get("name"),
        "system": system,
        "count_multiplier": multiplier,
        "jobs": [],
    }

    for job_name, job_type, _idx in iter_jobs(scenario, multiplier):
        expected = expected_allocations(job_type)
        if system == "legion":
            filename = f"{job_name}.ncl"
            with open(os.path.join(out_dir, filename), "w") as f:
                f.write(nickel(legion_jobspec(job_name, job_type)))
                f.write("\n")
        elif system == "nomad":
            filename = f"{job_name}.nomad.hcl"
            with open(os.path.join(out_dir, filename), "w") as f:
                f.write(nomad_job(job_name, job_type))
        elif system == "k8s":
            filename = f"{job_name}.json"
            manifests = [
                k8s_deployment(job_name, job_type, task_idx)
                for task_idx in range(job_type["tasks_per_job"])
            ]
            with open(os.path.join(out_dir, filename), "w") as f:
                json.dump({"apiVersion": "v1", "kind": "List", "items": manifests}, f, indent=2)
                f.write("\n")
        else:
            raise ValueError(f"unknown system {system}")

        manifest["jobs"].append(
            {"name": job_name, "file": filename, "expected_allocations": expected}
        )

    manifest["total_expected_allocations"] = sum(
        job["expected_allocations"] for job in manifest["jobs"]
    )
    with open(os.path.join(out_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--system", required=True, choices=["legion", "nomad", "k8s"])
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--count-multiplier",
        type=float,
        default=1.0,
        help="Scale every job type's job count (allocation scaling per step)",
    )
    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
    manifest = translate(scenario, args.system, args.out, args.count_multiplier)
    print(
        f"{manifest['scenario']} -> {args.system}: {len(manifest['jobs'])} jobs, "
        f"{manifest['total_expected_allocations']} expected allocations in {args.out}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
