import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from crypto_analytics.scoring import SymbolBaseline
from crypto_analytics.speed import SlidingWindowAggregator, load_baselines


class SpeedLayerTests(unittest.TestCase):
    def test_aggregator_scores_trends_and_spikes(self):
        baseline = SymbolBaseline(
            symbol="BTCUSDT",
            mean_return_5m=0.0,
            std_return_5m=0.5,
            mean_quote_volume_5m=10_000.0,
            std_quote_volume_5m=1_000.0,
            median_quote_volume_5m=12_000.0,
            sample_count=100,
            updated_at="2026-07-01T00:00:00+00:00",
            mean_trade_count_5m=100.0,
            std_trade_count_5m=10.0,
        )
        aggregator = SlidingWindowAggregator({"BTCUSDT": baseline}, window_seconds=300)
        aggregator.add({"symbol": "BTCUSDT", "event_time": 1000, "last_price": 100.0, "quote_volume_1h": 10_000.0, "trade_count_1h": 100, "ingest_time": 1000})
        aggregator.add({"symbol": "BTCUSDT", "event_time": 301000, "last_price": 104.0, "quote_volume_1h": 25_000.0, "trade_count_1h": 230, "ingest_time": 301500})

        trending = aggregator.top_trending(1)
        spikes = aggregator.abnormal_spikes(1)

        self.assertEqual(len(trending), 1)
        self.assertEqual(trending[0].symbol, "BTCUSDT")
        self.assertTrue(spikes[0].is_spike)

    def test_load_baselines_accepts_spark_json_directory(self):
        with TemporaryDirectory() as temp_dir:
            part_file = Path(temp_dir) / "part-00000.json"
            part_file.write_text(
                '{"symbol":"BTCUSDT","mean_return_5m":0,"std_return_5m":1,'
                '"mean_quote_volume_5m":10000,"std_quote_volume_5m":1000,'
                '"median_quote_volume_5m":12000,"sample_count":5,'
                '"updated_at":"2026-07-01T00:00:00+00:00"}\n',
                encoding="utf-8",
            )
            baselines = load_baselines(Path(temp_dir))
        self.assertIn("BTCUSDT", baselines)
        self.assertEqual(baselines["BTCUSDT"].sample_count, 5)


if __name__ == "__main__":
    unittest.main()
