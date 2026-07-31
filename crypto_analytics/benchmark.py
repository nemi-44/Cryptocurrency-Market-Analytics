"""Local benchmarking helpers for report graphs."""

from __future__ import annotations

import argparse
import csv
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Sequence

from .batch import Kline, compute_baselines, iter_kline_csv, iter_input_files


def load_file(path: Path) -> list[Kline]:
    return list(iter_kline_csv(path))


def benchmark_batch(input_path: Path, workers: list[int], output_csv: Path) -> None:
    files = iter_input_files(input_path)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["worker_count", "runtime_seconds", "baseline_count"])
        writer.writeheader()
        for worker_count in workers:
            started = time.perf_counter()
            if worker_count <= 1:
                klines = []
                for file_path in files:
                    klines.extend(load_file(file_path))
            else:
                with ProcessPoolExecutor(max_workers=worker_count) as pool:
                    chunks = list(pool.map(load_file, files))
                klines = [item for chunk in chunks for item in chunk]
            baselines = compute_baselines(klines)
            runtime = time.perf_counter() - started
            writer.writerow(
                {
                    "worker_count": worker_count,
                    "runtime_seconds": f"{runtime:.6f}",
                    "baseline_count": len(baselines),
                }
            )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark local batch baseline generation.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--output-csv", type=Path, default=Path("data/serving/batch_benchmark.csv"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    workers = [int(item.strip()) for item in args.workers.split(",") if item.strip()]
    benchmark_batch(args.input, workers, args.output_csv)
    print(f"Wrote benchmark CSV to {args.output_csv}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

