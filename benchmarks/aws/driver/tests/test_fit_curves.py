#!/usr/bin/env python3
"""Sanity tests for the projection math in analysis/fit_curves.py:
perfect synthetic curves must recover their parameters and project
exactly, and the model chooser must pick the generating model."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "analysis"))

from fit_curves import fit_inputs, fit_series, ols, parse_cell_name, saturation  # noqa: E402


def cell(name, throughput=100.0, complete=True, placed=10, expected=10):
    parsed = parse_cell_name(name)
    parsed["complete"] = complete
    parsed["metrics"] = {
        "throughput_per_sec": throughput,
        "p99_ms": 12.0,
        "placed_total": placed,
        "expected_total": expected,
        "complete": complete,
    }
    return parsed


class FitMath(unittest.TestCase):
    def test_linear_recovery(self):
        nodes = [1000, 2000, 5000, 10000]
        values = [10 + 0.005 * n for n in nodes]
        a, b, rmse, _, _, _ = ols(nodes, values)
        self.assertAlmostEqual(a, 10.0, places=6)
        self.assertAlmostEqual(b, 0.005, places=9)
        self.assertAlmostEqual(rmse, 0.0, places=6)

    def test_power_law_wins_on_power_data(self):
        nodes = [1000, 2000, 5000, 10000]
        values = [0.002 * (n**1.3) for n in nodes]
        fits, winner = fit_series(nodes, values)
        self.assertEqual(winner, "power")
        self.assertAlmostEqual(fits["power"]["b"], 1.3, places=6)
        # Projection through the recovered model is exact on clean data.
        self.assertAlmostEqual(
            fits["power"]["predict"](20000), 0.002 * (20000**1.3), places=3
        )

    def test_linear_wins_on_linear_data(self):
        nodes = [1000, 2000, 5000, 10000]
        values = [50 + 0.01 * n for n in nodes]
        _, winner = fit_series(nodes, values)
        self.assertEqual(winner, "linear")

    def test_interval_widens_with_extrapolation(self):
        nodes = [1000, 2000, 5000, 10000]
        values = [10.2, 19.7, 51.1, 98.9]  # noisy-linear
        fits, winner = fit_series(nodes, values)
        chosen = fits[winner]
        self.assertGreater(chosen["interval"](50000), chosen["interval"](20000))

    def test_negative_values_skip_power_model(self):
        nodes = [1000, 2000, 5000]
        values = [-1.0, 0.5, 2.0]
        fits, winner = fit_series(nodes, values)
        self.assertNotIn("power", fits)
        self.assertEqual(winner, "linear")


class CellNames(unittest.TestCase):
    """run-step.sh names a cell after the sizing it was given. Both forms
    have to parse, and the allocation form has to be distinguishable per
    point — every sweep point shares a node count, so a name that dropped
    the allocation count made them overwrite each other."""

    def test_allocation_sized_cell(self):
        parsed = parse_cell_name("legion-a-web_fleet-5000n-a25000")
        self.assertEqual(parsed["system"], "legion-a")
        self.assertEqual(parsed["scenario"], "web_fleet")
        self.assertEqual(parsed["nodes"], 5000)
        self.assertEqual(parsed["allocations"], 25000)
        self.assertEqual(parsed["allocs_per_node"], 5)

    def test_multiplier_sized_cell_still_parses(self):
        parsed = parse_cell_name("legion-a-web_fleet-1000n-x1.0")
        self.assertEqual(parsed["nodes"], 1000)
        self.assertIsNone(parsed["allocations"])
        self.assertEqual(parsed["multiplier"], 1.0)

    def test_junk_is_ignored(self):
        self.assertIsNone(parse_cell_name("not-a-cell"))
        self.assertIsNone(parse_cell_name("experiments"))

    def test_sweep_points_do_not_collide_in_the_fit_input(self):
        # Three densities at one node count: three series of one step
        # each, not one series that kept the last cell to be read.
        cells = [
            cell("legion-a-web_fleet-5000n-a25000"),
            cell("legion-a-web_fleet-5000n-a50000"),
            cell("legion-a-web_fleet-5000n-a100000"),
        ]
        series = fit_inputs(cells)
        self.assertEqual(len(series), 3)
        self.assertEqual(
            sorted(key[2] for key in series), [5, 10, 20]
        )

    def test_scale_steps_at_one_density_are_one_series(self):
        cells = [cell(f"legion-a-web_fleet-{n}n-a{n * 5}") for n in (1000, 2000, 5000)]
        series = fit_inputs(cells)
        self.assertEqual(len(series), 1)
        self.assertEqual(sorted(next(iter(series.values()))), [1000, 2000, 5000])


class SaturationCurve(unittest.TestCase):
    def test_points_ordered_and_incomplete_cells_kept(self):
        # The saturated point is the one that did not converge; dropping
        # it (as the scaling fit must) would end the curve exactly where
        # the answer is.
        cells = [
            cell("legion-a-web_fleet-5000n-a50000", throughput=900.0),
            cell("legion-a-web_fleet-5000n-a25000", throughput=800.0),
            cell(
                "legion-a-web_fleet-5000n-a100000",
                throughput=400.0,
                complete=False,
                placed=61000,
                expected=100000,
            ),
        ]
        [entry] = saturation(cells)
        self.assertEqual([p["allocations"] for p in entry["points"]], [25000, 50000, 100000])
        self.assertEqual([p["converged"] for p in entry["points"]], [True, True, False])
        self.assertEqual(entry["peak_throughput_at_allocations"], 50000)

    def test_single_point_is_not_a_curve(self):
        self.assertEqual(saturation([cell("legion-a-web_fleet-5000n-a25000")]), [])


if __name__ == "__main__":
    unittest.main()
