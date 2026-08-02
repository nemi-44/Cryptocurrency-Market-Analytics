import unittest

from crypto_analytics.historical import normalize_historical_row


class HistoricalDataTests(unittest.TestCase):
    def test_normalizes_binance_vision_aggregate_trade(self):
        record = normalize_historical_row(
            ["100", "250.0", "2.5", "200", "202", "1782864000000", "True", "True"],
            "BTCUSDT",
        )
        self.assertEqual(record["symbol"], "BTCUSDT")
        self.assertEqual(record["quote_volume"], 625.0)
        self.assertEqual(record["trade_count"], 3)
        self.assertEqual(record["source"], "binance-vision-historical")

    def test_normalizes_recent_microsecond_archive_timestamp_to_milliseconds(self):
        record = normalize_historical_row(
            ["100", "250.0", "2.5", "200", "202", "1782864000054233", "True", "True"],
            "BTCUSDT",
        )
        self.assertEqual(record["event_time"], 1782864000054)
        self.assertEqual(record["ingest_time"], 1782864000054)


if __name__ == "__main__":
    unittest.main()
