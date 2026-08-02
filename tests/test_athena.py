import unittest
from pathlib import Path


class AthenaInfrastructureTests(unittest.TestCase):
    def test_cloudformation_catalogs_json_and_parquet_data(self):
        template = Path("infra/cloudformation.yaml").read_text(encoding="utf-8")

        self.assertIn("Type: AWS::Glue::Database", template)
        self.assertEqual(template.count("Type: AWS::Glue::Table"), 2)
        self.assertIn("Type: AWS::Athena::WorkGroup", template)
        self.assertIn("Name: raw_market_events", template)
        self.assertIn("Name: batch_windows", template)
        self.assertIn('projection.enabled: "true"', template)
        self.assertIn("athena-results/", template)

    def test_required_sql_views_are_versioned(self):
        sql = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(Path("sql/athena").glob("*.sql"))
        )

        for view in (
            "latest_market_prices",
            "trending_coins",
            "abnormal_price_spikes",
            "market_summary",
        ):
            self.assertIn(f"CREATE OR REPLACE VIEW {view}", sql)


if __name__ == "__main__":
    unittest.main()
