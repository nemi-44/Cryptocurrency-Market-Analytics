import unittest

from crypto_analytics.scoring import SymbolBaseline, price_return_pct, score_window


class ScoringTests(unittest.TestCase):
    def baseline(self):
        return SymbolBaseline(
            symbol="BTCUSDT",
            mean_return_5m=0.0,
            std_return_5m=1.0,
            mean_quote_volume_5m=10_000.0,
            std_quote_volume_5m=2_000.0,
            median_quote_volume_5m=12_000.0,
            sample_count=100,
            updated_at="2026-07-01T00:00:00+00:00",
            mean_trade_count_5m=100.0,
            std_trade_count_5m=20.0,
        )

    def test_return_pct(self):
        self.assertAlmostEqual(price_return_pct(100.0, 103.0), 3.0)

    def test_spike_flag(self):
        result = score_window(
            symbol="BTCUSDT",
            start_price=100.0,
            end_price=103.0,
            quote_volume_5m=20_000.0,
            trade_count_5m=140,
            window_start=1000,
            window_end=2000,
            observed_at=2500,
            baseline=self.baseline(),
        )
        self.assertIsNotNone(result)
        self.assertTrue(result.is_spike)
        self.assertEqual(result.latency_ms, 500)
        self.assertGreater(result.trend_score, 0)

    def test_low_liquidity_is_filtered(self):
        low_liquidity = SymbolBaseline(
            symbol="BTCUSDT",
            mean_return_5m=0.0,
            std_return_5m=1.0,
            mean_quote_volume_5m=100.0,
            std_quote_volume_5m=1.0,
            median_quote_volume_5m=100.0,
            sample_count=10,
            updated_at="2026-07-01T00:00:00+00:00",
        )
        result = score_window(
            symbol="BTCUSDT",
            start_price=100.0,
            end_price=110.0,
            quote_volume_5m=20_000.0,
            trade_count_5m=10,
            window_start=1000,
            window_end=2000,
            observed_at=2500,
            baseline=low_liquidity,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

