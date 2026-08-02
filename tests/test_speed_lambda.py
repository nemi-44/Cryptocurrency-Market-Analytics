import unittest

from crypto_analytics.scoring import SymbolBaseline
from crypto_analytics.speed_lambda import merge_window_state, score_durable_window


class DurableSpeedLayerTests(unittest.TestCase):
    def baseline(self):
        return SymbolBaseline(
            symbol="BTCUSDT",
            mean_return_5m=0.0,
            std_return_5m=0.5,
            mean_quote_volume_5m=1_000.0,
            std_quote_volume_5m=100.0,
            median_quote_volume_5m=1_000.0,
            sample_count=500,
            updated_at="2026-08-02T00:00:00+00:00",
            mean_trade_count_5m=10.0,
            std_trade_count_5m=2.0,
        )

    def test_merge_is_idempotent_by_shard_sequence(self):
        record = {
            "symbol": "BTCUSDT",
            "event_time": 1000,
            "last_price": 100.0,
            "quote_volume": 600.0,
            "trade_count": 1,
            "trade_id": 10,
            "_shard_id": "shard-000",
            "_sequence": "100",
        }
        first = merge_window_state({}, [record])
        retried = merge_window_state(first, [record])
        self.assertEqual(len(retried["buckets"]), 1)
        self.assertEqual(retried["buckets"][0]["quote_volume"], 600.0)
        self.assertEqual(retried["checkpoints"]["shard-000"], "100")

    def test_compact_buckets_score_hybrid_window(self):
        records = [
            {
                "symbol": "BTCUSDT",
                "event_time": 1000,
                "last_price": 100.0,
                "quote_volume": 600.0,
                "trade_count": 4,
                "trade_id": 10,
                "_shard_id": "shard-000",
                "_sequence": "100",
            },
            {
                "symbol": "BTCUSDT",
                "event_time": 2500,
                "last_price": 102.0,
                "quote_volume": 700.0,
                "trade_count": 5,
                "trade_id": 11,
                "_shard_id": "shard-000",
                "_sequence": "101",
            },
        ]
        state = merge_window_state({}, records)
        result = score_durable_window(
            "BTCUSDT",
            state,
            self.baseline(),
            observed_at=3000,
            min_liquidity_usdt=0.0,
            spike_zscore_threshold=3.0,
            spike_abs_return_pct=1.5,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.quote_volume_5m, 1300.0)
        self.assertEqual(result.trade_count_5m, 9)
        self.assertTrue(result.is_spike)
        self.assertEqual(result.baseline_sample_count, 500)


if __name__ == "__main__":
    unittest.main()
