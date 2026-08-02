"""Controlled speed-layer load benchmark with latency percentiles."""

from __future__ import annotations

import argparse
import csv
import math
import time
from pathlib import Path
from typing import Sequence

from .batch import compute_baselines
from .speed import SlidingWindowAggregator
from .synthetic import generate_market_events


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def benchmark_speed_layer(
    *,
    rates: list[int],
    duration_seconds: float,
    symbol_count: int,
    refresh_records: int,
    output_csv: Path,
    pace: bool = True,
) -> list[dict[str, object]]:
    history = list(
        generate_market_events(
            count=max(20_000, symbol_count * 1_000),
            symbol_count=symbol_count,
        )
    )
    baselines = {item.symbol: item for item in compute_baselines(history, updated_at="benchmark")}
    rows: list[dict[str, object]] = []

    for rate in rates:
        record_count = max(1, int(rate * duration_seconds))
        interval_seconds = 1.0 / rate
        events = generate_market_events(
            count=record_count,
            symbol_count=symbol_count,
            interval_ms=max(1, int(1000 / rate)),
        )
        aggregator = SlidingWindowAggregator(
            baselines,
            min_liquidity_usdt=0.0,
        )
        processing_latencies: list[float] = []
        schedule_lags: list[float] = []
        refreshes = 0
        started = time.perf_counter()

        for index, event in enumerate(events):
            scheduled = index * interval_seconds
            if pace:
                remaining = scheduled - (time.perf_counter() - started)
                if remaining > 0:
                    time.sleep(remaining)

            operation_started = time.perf_counter()
            aggregator.add(
                {
                    "symbol": event.symbol,
                    "event_time": event.event_time,
                    "last_price": event.last_price,
                    "quote_volume": event.quote_volume,
                    "trade_count": event.trade_count,
                    "ingest_time": event.ingest_time,
                    "trade_id": event.trade_id,
                }
            )
            if (index + 1) % refresh_records == 0:
                aggregator.serving_views(10, observed_at=event.ingest_time)
                refreshes += 1
            operation_finished = time.perf_counter()
            processing_latencies.append((operation_finished - operation_started) * 1000)
            schedule_lags.append(max(0.0, (operation_finished - started - scheduled) * 1000))

        elapsed = time.perf_counter() - started
        rows.append(
            {
                "data_source": "synthetic-controlled-load",
                "environment": "local",
                "target_ingestion_rate": rate,
                "processed_records": record_count,
                "elapsed_seconds": round(elapsed, 6),
                "achieved_throughput": round(record_count / elapsed, 2),
                "p50_processing_latency_ms": round(percentile(processing_latencies, 0.50), 6),
                "p95_processing_latency_ms": round(percentile(processing_latencies, 0.95), 6),
                "p99_processing_latency_ms": round(percentile(processing_latencies, 0.99), 6),
                "p95_schedule_lag_ms": round(percentile(schedule_lags, 0.95), 6),
                "refreshes": refreshes,
                "symbols": symbol_count,
            }
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark speed-layer latency under controlled load.")
    parser.add_argument("--rates", default="100,500,1000")
    parser.add_argument("--duration-seconds", type=float, default=10.0)
    parser.add_argument("--symbols", type=int, default=32)
    parser.add_argument("--refresh-records", type=int, default=100)
    parser.add_argument("--no-pace", action="store_true", help="Run at maximum speed for CI/smoke testing.")
    parser.add_argument("--output-csv", type=Path, default=Path("results/speed_benchmark.csv"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    rates = sorted({int(item.strip()) for item in args.rates.split(",") if item.strip()})
    rows = benchmark_speed_layer(
        rates=rates,
        duration_seconds=args.duration_seconds,
        symbol_count=args.symbols,
        refresh_records=args.refresh_records,
        output_csv=args.output_csv,
        pace=not args.no_pace,
    )
    print(f"Wrote {len(rows)} speed benchmark rows to {args.output_csv}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
