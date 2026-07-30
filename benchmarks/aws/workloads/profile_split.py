#!/usr/bin/env python3
"""Split N simulated nodes across a scenario's node_profiles by their
count weights — the same largest-remainder rule as Legion's
`--simulate` (src/worker/simulate.rs::distribute), so every system's
fake fleet has the same class mix at every scale step.

Emits one line per profile:  name cpus memory_gb count [key=value...]

Used by scripts/start-sim-fleet.sh for the KWOK node objects and the
nodesim node templates (Legion's sim mode does this internally).
"""

import argparse
import json
import sys


def split(total, profiles):
    weights = [profile.get("count", 1) for profile in profiles]
    weight_sum = sum(weights)
    if weight_sum <= 0:
        raise ValueError("profile weights sum to zero")
    exact = [total * weight for weight in weights]
    base = [value // weight_sum for value in exact]
    remainders = [(value % weight_sum, -idx) for idx, value in enumerate(exact)]
    leftover = total - sum(base)
    for _, neg_idx in sorted(remainders, reverse=True)[:leftover]:
        base[-neg_idx] += 1
    return base


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", required=True, help="scenario/profiles JSON file")
    parser.add_argument("--total", type=int, required=True)
    args = parser.parse_args()

    with open(args.profiles) as f:
        profiles = json.load(f)["node_profiles"]

    for profile, count in zip(profiles, split(args.total, profiles)):
        attrs = " ".join(
            f"{key}={value}" for key, value in sorted((profile.get("attrs") or {}).items())
        )
        print(
            f"{profile['name']} {profile.get('cpus', 4)} "
            f"{profile.get('memory_gb', 32)} {count} {attrs}".rstrip()
        )


if __name__ == "__main__":
    sys.exit(main())
