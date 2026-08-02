"""Repeatable sequential-versus-parallel full-history batch benchmark."""

from __future__ import annotations

import argparse
import csv
import statistics
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Sequence

from .batch import MarketEvent, compute_baselines, read_events
from .synthetic import generate_market_events


def _compute_symbol_baseline(item: tuple[str, list[MarketEvent]]):
    _, events = item
    baselines = compute_baselines(events, updated_at="benchmark")
    return baselines[0] if baselines else None


def _group_by_symbol(events: list[MarketEvent]) -> list[tuple[str, list[MarketEvent]]]:
    grouped: dict[str, list[MarketEvent]] = {}
    for event in events:
        grouped.setdefault(event.symbol, []).append(event)
    return sorted(grouped.items())


def _run_once(
    events: list[MarketEvent],
    grouped: list[tuple[str, list[MarketEvent]]],
    worker_count: int,
) -> int:
    if worker_count <= 1:
        return len(compute_baselines(events, updated_at="benchmark"))

    actual_workers = min(worker_count, len(grouped))
    with ProcessPoolExecutor(max_workers=actual_workers) as pool:
        results = list(pool.map(_compute_symbol_baseline, grouped))
    return sum(result is not None for result in results)


def benchmark_batch(
    *,
    events: list[MarketEvent],
    workers: list[int],
    output_csv: Path,
    repeats: int = 5,
) -> list[dict[str, object]]:
    if not events:
        raise ValueError("The benchmark requires at least one market event")
    if repeats < 1:
        raise ValueError("repeats must be at least 1")

    grouped = _group_by_symbol(events)
    timings: dict[int, list[float]] = {}
    baseline_counts: dict[int, int] = {}
    for worker_count in workers:
        worker_timings: list[float] = []
        for _ in range(repeats):
            started = time.perf_counter()
            baseline_counts[worker_count] = _run_once(events, grouped, worker_count)
            worker_timings.append(time.perf_counter() - started)
        timings[worker_count] = worker_timings

    sequential_runtime = statistics.median(timings[min(workers)])
    rows: list[dict[str, object]] = []
    for worker_count in workers:
        runtime = statistics.median(timings[worker_count])
        speedup = sequential_runtime / runtime if runtime else 0.0
        rows.append(
            {
                "data_source": "synthetic" if all(event.symbol.startswith("COIN") for event in events[:100]) else "archived-real",
                "environment": "local",
                "worker_count": worker_count,
                "repeats": repeats,
                "input_records": len(events),
                "symbols": len(grouped),
                "median_runtime_seconds": round(runtime, 6),
                "speedup": round(speedup, 4),
                "parallel_efficiency": round(speedup / worker_count, 4),
                "records_per_second": round(len(events) / runtime, 2) if runtime else 0.0,
                "baseline_count": baseline_counts[worker_count],
            }
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark the full-history batch computation.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="Canonical JSON Lines event file/directory.")
    source.add_argument("--synthetic-events", type=int, help="Generate this many deterministic events.")
    parser.add_argument("--synthetic-symbols", type=int, default=32)
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output-csv", type=Path, default=Path("results/batch_benchmark.csv"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    workers = sorted({int(item.strip()) for item in args.workers.split(",") if item.strip()})
    if not workers or workers[0] != 1:
        raise ValueError("workers must include 1 as the sequential baseline")

    if args.synthetic_events:
        events = list(
            generate_market_events(
                count=args.synthetic_events,
                symbol_count=args.synthetic_symbols,
            )
        )
    else:
        events = read_events(args.input)
    rows = benchmark_batch(
        events=events,
        workers=workers,
        output_csv=args.output_csv,
        repeats=args.repeats,
    )
    print(f"Wrote {len(rows)} batch benchmark rows to {args.output_csv}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
