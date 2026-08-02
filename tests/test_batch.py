import unittest

from crypto_analytics.batch import MarketEvent, compute_baselines


class BatchBaselineTests(unittest.TestCase):
    def test_compute_baseline_from_two_complete_event_time_windows(self):
        rows = [
            MarketEvent("BTCUSDT", index * 60_000, 100.0 + index, 1_000.0, 10, index * 60_000 + 25, index)
            for index in range(10)
        ]
        baselines = compute_baselines(rows, updated_at="2026-07-01T00:00:00+00:00")
        self.assertEqual(len(baselines), 1)
        baseline = baselines[0]
        self.assertEqual(baseline.sample_count, 2)
        self.assertAlmostEqual(baseline.mean_return_5m, ((4.0 / 100.0) + (4.0 / 105.0)) * 50)
        self.assertAlmostEqual(baseline.mean_quote_volume_5m, 5_000.0)
        self.assertAlmostEqual(baseline.mean_trade_count_5m, 50.0)


if __name__ == "__main__":
    unittest.main()
