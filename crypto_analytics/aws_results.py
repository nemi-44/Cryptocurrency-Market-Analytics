"""Summarize real AWS benchmark run CSV files for the H1 report."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Sequence


def summarize_emr_runs(input_csv: Path, output_csv: Path) -> list[dict[str, object]]:
    with input_csv.open("r", encoding="utf-8", newline="") as handle:
        runs = list(csv.DictReader(handle))
    if not runs:
        raise ValueError("The EMR run CSV is empty")

    grouped: dict[int, list[dict[str, str]]] = defaultdict(list)
    for run in runs:
        if run["state"] != "COMPLETED":
            continue
        grouped[int(run["worker_count"])].append(run)
    if 1 not in grouped:
        raise ValueError("A completed one-worker baseline is required")

    baseline_runtime = statistics.median(
        float(run["runtime_seconds"]) for run in grouped[1]
    )
    rows: list[dict[str, object]] = []
    for worker_count in sorted(grouped):
        samples = [float(run["runtime_seconds"]) for run in grouped[worker_count]]
        median_runtime = statistics.median(samples)
        speedup = baseline_runtime / median_runtime
        input_records = int(grouped[worker_count][0]["input_records"])
        rows.append(
            {
                "data_source": grouped[worker_count][0]["data_source"],
                "environment": grouped[worker_count][0]["environment"],
                "worker_count": worker_count,
                "repeats": len(samples),
                "input_records": input_records,
                "median_runtime_seconds": round(median_runtime, 3),
                "runtime_stdev_seconds": round(
                    statistics.stdev(samples) if len(samples) > 1 else 0.0,
                    3,
                ),
                "minimum_runtime_seconds": round(min(samples), 3),
                "maximum_runtime_seconds": round(max(samples), 3),
                "speedup": round(speedup, 4),
                "parallel_efficiency": round(speedup / worker_count, 4),
                "records_per_second": round(input_records / median_runtime, 2),
            }
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize real EMR benchmark runs.")
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=Path("results/aws/emr_benchmark_runs.csv"),
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/aws/emr_benchmark_summary.csv"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    rows = summarize_emr_runs(args.input_csv, args.output_csv)
    print(f"Wrote {len(rows)} EMR summary rows to {args.output_csv}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
