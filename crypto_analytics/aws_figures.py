"""Create report-ready figures from real AWS benchmark CSV files."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .plot_metrics import _draw_chart, read_rows


def plot_aws_metrics(
    batch_csv: Path,
    speed_csv: Path,
    scaling_csv: Path,
    output_dir: Path,
) -> list[Path]:
    batch = read_rows(batch_csv)
    speed = read_rows(speed_csv)
    scaling = read_rows(scaling_csv)
    figures = [
        (
            [float(row["worker_count"]) for row in batch],
            [float(row["median_runtime_seconds"]) for row in batch],
            "EMR core workers",
            "Median runtime (seconds)",
            "Real Binance batch runtime on AWS EMR",
            "emr_runtime_vs_workers.png",
        ),
        (
            [float(row["worker_count"]) for row in batch],
            [float(row["speedup"]) for row in batch],
            "EMR core workers",
            "Speedup",
            "Real Binance batch speedup on AWS EMR",
            "emr_speedup_vs_workers.png",
        ),
        (
            [float(row["worker_count"]) for row in batch],
            [float(row["parallel_efficiency"]) for row in batch],
            "EMR core workers",
            "Parallel efficiency",
            "Real Binance batch parallel efficiency",
            "emr_efficiency_vs_workers.png",
        ),
        (
            [float(row["worker_count"]) for row in batch],
            [float(row["records_per_second"]) for row in batch],
            "EMR core workers",
            "Records processed per second",
            "Real Binance batch throughput on AWS EMR",
            "emr_throughput_vs_workers.png",
        ),
        (
            [float(row["target_ingestion_rate"]) for row in speed],
            [float(row["p95_end_to_end_latency_ms"]) for row in speed],
            "Controlled input rate (records/s)",
            "p95 end-to-end latency (ms)",
            "AWS hybrid-layer latency under controlled replay",
            "aws_latency_vs_ingestion_rate.png",
        ),
        (
            [float(row["target_ingestion_rate"]) for row in speed],
            [float(row["achieved_throughput"]) for row in speed],
            "Target input rate (records/s)",
            "Achieved throughput (records/s)",
            "AWS Kinesis achieved throughput",
            "aws_throughput_vs_ingestion_rate.png",
        ),
        (
            [float(row["elapsed_seconds"]) for row in scaling],
            [float(row["running_workers"]) for row in scaling],
            "Elapsed experiment time (seconds)",
            "Running EMR core workers",
            "EMR auto-scaling worker timeline",
            "emr_scaling_workers.png",
        ),
        (
            [float(row["elapsed_seconds"]) for row in scaling],
            [float(row["yarn_memory_available_pct"]) for row in scaling],
            "Elapsed experiment time (seconds)",
            "YARN memory available (%)",
            "EMR auto-scaling trigger metric",
            "emr_scaling_yarn_memory.png",
        ),
    ]
    outputs: list[Path] = []
    for x_values, y_values, x_label, y_label, title, filename in figures:
        output = output_dir / filename
        _draw_chart(
            x_values=x_values,
            y_values=y_values,
            x_label=x_label,
            y_label=y_label,
            title=title,
            output=output,
        )
        outputs.append(output)
    return outputs


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plot real AWS experiment metrics.")
    parser.add_argument(
        "--batch-csv",
        type=Path,
        default=Path("results/aws/emr_benchmark_summary.csv"),
    )
    parser.add_argument(
        "--speed-csv",
        type=Path,
        default=Path("results/aws/speed_load_benchmark.csv"),
    )
    parser.add_argument(
        "--scaling-csv",
        type=Path,
        default=Path("results/aws/emr_scaling_events.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results/aws/figures"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    outputs = plot_aws_metrics(
        args.batch_csv,
        args.speed_csv,
        args.scaling_csv,
        args.output_dir,
    )
    print("\n".join(str(path) for path in outputs))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
