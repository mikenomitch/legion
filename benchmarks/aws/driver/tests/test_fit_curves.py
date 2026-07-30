#!/usr/bin/env python3
"""Sanity tests for the projection math in analysis/fit_curves.py:
perfect synthetic curves must recover their parameters and project
exactly, and the model chooser must pick the generating model."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "analysis"))

from fit_curves import fit_series, ols  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
