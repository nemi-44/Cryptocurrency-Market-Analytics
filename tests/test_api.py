import json
import unittest
from decimal import Decimal

from crypto_analytics.api import latest_payload_from_items, response


class ApiTests(unittest.TestCase):
    def test_latest_payload_groups_latest_window(self):
        payload = latest_payload_from_items(
            [
                {"window_end": "100", "result_type": "trend", "rank": Decimal("1"), "symbol": "OLDUSDT"},
                {"window_end": "200", "result_type": "spike", "rank": Decimal("1"), "symbol": "ETHUSDT"},
                {"window_end": "200", "result_type": "trend", "rank": Decimal("2"), "symbol": "BTCUSDT"},
                {"window_end": "200", "result_type": "trend", "rank": Decimal("1"), "symbol": "SOLUSDT"},
            ]
        )

        self.assertEqual(payload["latest_window"], "200")
        self.assertEqual([item["symbol"] for item in payload["trending"]], ["SOLUSDT", "BTCUSDT"])
        self.assertEqual(payload["spikes"][0]["symbol"], "ETHUSDT")

    def test_response_has_cors_headers(self):
        result = response(200, {"ok": True})
        self.assertEqual(result["headers"]["Access-Control-Allow-Origin"], "*")
        self.assertEqual(json.loads(result["body"]), {"ok": True})


if __name__ == "__main__":
    unittest.main()

