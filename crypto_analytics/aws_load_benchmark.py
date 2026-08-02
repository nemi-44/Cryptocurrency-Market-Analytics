"""Replay controlled records into AWS Kinesis and sample hybrid-view latency."""

from __future__ import annotations

import argparse
import csv
import json
import math
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from .binance import LiveTradeRecord


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


def _number(value: Any) -> float:
    if isinstance(value, Decimal):
        return float(value)
    return float(value or 0)


def benchmark_aws_speed_layer(
    *,
    region: str,
    stream_name: str,
    latest_table_name: str,
    rates: list[int],
    duration_seconds: float,
    settle_seconds: float,
    symbols: list[str],
    output_csv: Path,
) -> list[dict[str, object]]:
    import boto3

    kinesis = boto3.client("kinesis", region_name=region)
    table = boto3.resource("dynamodb", region_name=region).Table(latest_table_name)
    prices = {
        "BTCUSDT": 63_000.0,
        "ETHUSDT": 1_850.0,
        "BNBUSDT": 584.0,
        "SOLUSDT": 73.0,
        "XRPUSDT": 1.08,
    }
    rows: list[dict[str, object]] = []

    for rate in rates:
        started_at = datetime.now(timezone.utc)
        total_records = max(1, int(rate * duration_seconds))
        latency_samples: list[float] = []
        observed_windows: set[tuple[str, int]] = set()
        stop_sampling = threading.Event()
        benchmark_started_ms = int(time.time() * 1000)

        def sample_latest_view() -> None:
            while not stop_sampling.is_set():
                response = table.scan(
                    ProjectionExpression="symbol,window_end,latency_ms",
                    ConsistentRead=True,
                )
                for item in response.get("Items", []):
                    window_end = int(item.get("window_end", 0))
                    key = (str(item.get("symbol", "")), window_end)
                    if window_end >= benchmark_started_ms and key not in observed_windows:
                        observed_windows.add(key)
                        latency_samples.append(_number(item.get("latency_ms", 0)))
                stop_sampling.wait(0.10)

        sampler = threading.Thread(target=sample_latest_view, daemon=True)
        sampler.start()
        failed_records = 0
        sent_records = 0
        sequence = 0
        started = time.perf_counter()

        while sent_records < total_records:
            batch_size = min(
                500,
                # Use roughly half-second batches. Tiny batches make the client
                # network round trip, rather than Kinesis, the throughput limit.
                max(100, int(rate * 0.50)),
                total_records - sent_records,
            )
            now_ms = int(time.time() * 1000)
            records = []
            for _ in range(batch_size):
                symbol = symbols[sequence % len(symbols)]
                base_price = prices.get(symbol, 100.0)
                price = base_price * (1.0 + ((sequence % 101) - 50) / 100_000.0)
                event = LiveTradeRecord(
                    symbol=symbol,
                    event_time=now_ms,
                    last_price=price,
                    quote_volume=250.0 + (sequence % 500),
                    trade_count=1,
                    ingest_time=now_ms,
                    trade_id=(now_ms * 1_000_000) + sequence,
                    event_type="controlledReplay",
                )
                payload = event.to_dict()
                payload["source"] = "aws-controlled-replay"
                records.append(
                    {
                        "Data": (
                            json.dumps(payload, separators=(",", ":"), sort_keys=True)
                            + "\n"
                        ).encode("utf-8"),
                        "PartitionKey": symbol,
                    }
                )
                sequence += 1

            response = kinesis.put_records(StreamName=stream_name, Records=records)
            failed_records += int(response.get("FailedRecordCount", 0))
            sent_records += batch_size
            target_elapsed = sent_records / rate
            remaining = target_elapsed - (time.perf_counter() - started)
            if remaining > 0:
                time.sleep(remaining)

        send_elapsed = time.perf_counter() - started
        time.sleep(settle_seconds)
        stop_sampling.set()
        sampler.join(timeout=2)
        successful_records = sent_records - failed_records
        ended_at = datetime.now(timezone.utc)
        rows.append(
            {
                "data_source": "synthetic-controlled-replay",
                "environment": "aws-learner-lab",
                "target_ingestion_rate": rate,
                "sent_records": sent_records,
                "successful_records": successful_records,
                "failed_records": failed_records,
                "send_elapsed_seconds": round(send_elapsed, 6),
                "achieved_throughput": round(successful_records / send_elapsed, 2),
                "latency_samples": len(latency_samples),
                "p50_end_to_end_latency_ms": round(percentile(latency_samples, 0.50), 3),
                "p95_end_to_end_latency_ms": round(percentile(latency_samples, 0.95), 3),
                "p99_end_to_end_latency_ms": round(percentile(latency_samples, 0.99), 3),
                "minimum_latency_ms": round(min(latency_samples), 3)
                if latency_samples
                else 0.0,
                "maximum_latency_ms": round(max(latency_samples), 3)
                if latency_samples
                else 0.0,
                "symbols": len(symbols),
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
            }
        )
        time.sleep(2)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an AWS Kinesis controlled-load benchmark.")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--latest-table-name", required=True)
    parser.add_argument("--rates", default="100,500,1000")
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--settle-seconds", type=float, default=5.0)
    parser.add_argument(
        "--symbols",
        default="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/aws/speed_load_benchmark.csv"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    rates = sorted({int(item) for item in args.rates.split(",") if item})
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    rows = benchmark_aws_speed_layer(
        region=args.region,
        stream_name=args.stream_name,
        latest_table_name=args.latest_table_name,
        rates=rates,
        duration_seconds=args.duration_seconds,
        settle_seconds=args.settle_seconds,
        symbols=symbols,
        output_csv=args.output_csv,
    )
    print(f"Wrote {len(rows)} AWS speed benchmark rows to {args.output_csv}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
