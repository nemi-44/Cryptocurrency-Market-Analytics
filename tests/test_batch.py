import unittest

from crypto_analytics.batch import Kline, compute_baselines


class BatchBaselineTests(unittest.TestCase):
    def test_compute_baseline_from_five_klines(self):
        rows = [
            Kline("BTCUSDT", 0, 100.0, 101.0, 1_000.0, 10),
            Kline("BTCUSDT", 60_000, 101.0, 102.0, 1_100.0, 11),
            Kline("BTCUSDT", 120_000, 102.0, 103.0, 1_200.0, 12),
            Kline("BTCUSDT", 180_000, 103.0, 104.0, 1_300.0, 13),
            Kline("BTCUSDT", 240_000, 104.0, 105.0, 1_400.0, 14),
        ]
        baselines = compute_baselines(rows, updated_at="2026-07-01T00:00:00+00:00")
        self.assertEqual(len(baselines), 1)
        baseline = baselines[0]
        self.assertEqual(baseline.sample_count, 1)
        self.assertAlmostEqual(baseline.mean_return_5m, 5.0)
        self.assertAlmostEqual(baseline.mean_quote_volume_5m, 6_000.0)
        self.assertAlmostEqual(baseline.mean_trade_count_5m, 60.0)


if __name__ == "__main__":
    unittest.main()

