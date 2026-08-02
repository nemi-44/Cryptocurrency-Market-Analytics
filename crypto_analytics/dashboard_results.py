"""Build the small JSON benchmark payload displayed by the static dashboard."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


def _read(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_dashboard_results(
    batch_csv: Path,
    speed_csv: Path,
    scaling_csv: Path,
    output_json: Path,
) -> dict[str, object]:
    batch = _read(batch_csv)
    speed = _read(speed_csv)
    scaling = _read(scaling_csv)
    requested = [int(row["requested_workers"]) for row in scaling]
    running = [int(row["running_workers"]) for row in scaling]
    yarn = [float(row["yarn_memory_available_pct"]) for row in scaling]
    payload: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "batch": batch,
        "speed": speed,
        "scaling": {
            "initial_workers": running[0],
            "peak_requested_workers": max(requested),
            "peak_running_workers": max(running),
            "final_workers": running[-1],
            "minimum_yarn_available_pct": min(yarn),
            "maximum_yarn_available_pct": max(yarn),
            "observation_seconds": int(scaling[-1]["elapsed_seconds"]),
        },
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build dashboard benchmark JSON.")
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
        "--output-json",
        type=Path,
        default=Path("results/aws/dashboard_benchmarks.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    build_dashboard_results(
        args.batch_csv,
        args.speed_csv,
        args.scaling_csv,
        args.output_json,
    )
    print(args.output_json)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
