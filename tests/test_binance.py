import json
import unittest

from crypto_analytics.binance import is_usdt_spot_symbol, normalize_timestamp_ms, parse_ticker_message


class BinanceParsingTests(unittest.TestCase):
    def test_timestamp_normalizes_microseconds(self):
        self.assertEqual(normalize_timestamp_ms(1735689600000000), 1735689600000)
        self.assertEqual(normalize_timestamp_ms(1735689600000), 1735689600000)

    def test_symbol_filter_keeps_usdt_spot_pairs(self):
        self.assertTrue(is_usdt_spot_symbol("BTCUSDT"))
        self.assertFalse(is_usdt_spot_symbol("BTCBUSD"))
        self.assertFalse(is_usdt_spot_symbol("ETHUPUSDT"))

    def test_parse_ticker_message_filters_and_normalizes(self):
        message = json.dumps(
            [
                {"e": "1hTicker", "E": 1735689600000, "s": "BTCUSDT", "o": "100", "h": "110", "l": "90", "c": "105", "q": "12000", "n": 30},
                {"e": "1hTicker", "E": 1735689600000, "s": "ETHBTC", "o": "1", "h": "1", "l": "1", "c": "1", "q": "1", "n": 1},
            ]
        )
        records = parse_ticker_message(message, ingest_time=1735689600500)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].symbol, "BTCUSDT")
        self.assertEqual(records[0].quote_volume_1h, 12000.0)
        self.assertEqual(records[0].ingest_time, 1735689600500)


if __name__ == "__main__":
    unittest.main()

