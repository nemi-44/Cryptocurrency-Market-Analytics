import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from crypto_analytics.benchmark import benchmark_batch
from crypto_analytics.load_benchmark import benchmark_speed_layer, percentile
from crypto_analytics.plot_metrics import plot_metrics
from crypto_analytics.synthetic import generate_market_events


class BenchmarkTests(unittest.TestCase):
    def test_percentile_interpolates(self):
        self.assertEqual(percentile([1.0, 2.0, 3.0], 0.5), 2.0)
        self.assertAlmostEqual(percentile([0.0, 10.0], 0.95), 9.5)

    def test_benchmarks_write_metrics_and_figures(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            batch_csv = root / "batch.csv"
            speed_csv = root / "speed.csv"
            benchmark_batch(
                events=list(generate_market_events(count=2_000, symbol_count=4)),
                workers=[1],
                output_csv=batch_csv,
                repeats=1,
            )
            benchmark_speed_layer(
                rates=[100],
                duration_seconds=0.01,
                symbol_count=4,
                refresh_records=10,
                output_csv=speed_csv,
                pace=False,
            )
            with batch_csv.open(newline="", encoding="utf-8") as handle:
                batch_row = next(csv.DictReader(handle))
            with speed_csv.open(newline="", encoding="utf-8") as handle:
                speed_row = next(csv.DictReader(handle))
            self.assertEqual(batch_row["data_source"], "synthetic")
            self.assertIn("speedup", batch_row)
            self.assertEqual(speed_row["data_source"], "synthetic-controlled-load")
            self.assertIn("p95_processing_latency_ms", speed_row)

            figures = plot_metrics(batch_csv, speed_csv, root / "figures")
            self.assertEqual(len(figures), 3)
            self.assertTrue(all(path.stat().st_size > 0 for path in figures))


if __name__ == "__main__":
    unittest.main()
