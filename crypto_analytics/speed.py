"""Five-minute speed-layer window aggregation."""

from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse

from .config import load_analytics_config
from .scoring import ServingResult, SymbolBaseline, score_window
from .storage import LocalServingWriter, ServingWriter


class SlidingWindowAggregator:
    """Maintains per-symbol rolling windows from normalized ticker records."""

    def __init__(
        self,
        baselines: dict[str, SymbolBaseline],
        window_seconds: int = 300,
        min_liquidity_usdt: float = 10_000.0,
        spike_zscore_threshold: float = 3.0,
        spike_abs_return_pct: float = 1.5,
    ) -> None:
        self.baselines = baselines
        self.window_ms = window_seconds * 1000
        self.min_liquidity_usdt = min_liquidity_usdt
        self.spike_zscore_threshold = spike_zscore_threshold
        self.spike_abs_return_pct = spike_abs_return_pct
        self.records: dict[str, deque[dict[str, object]]] = defaultdict(deque)
        self.trade_ids: dict[str, set[int]] = defaultdict(set)

    def add(self, record: dict[str, object]) -> bool:
        symbol = str(record["symbol"]).upper()
        event_time = int(record["event_time"])
        trade_id = record.get("trade_id")
        normalized_trade_id = int(trade_id) if trade_id is not None else None
        if normalized_trade_id is not None and normalized_trade_id in self.trade_ids[symbol]:
            return False

        symbol_records = self.records[symbol]
        symbol_records.append(record)
        if normalized_trade_id is not None:
            self.trade_ids[symbol].add(normalized_trade_id)
        if len(symbol_records) > 1 and event_time < int(symbol_records[-2]["event_time"]):
            self.records[symbol] = symbol_records = deque(
                sorted(
                    symbol_records,
                    key=lambda item: (int(item["event_time"]), int(item.get("trade_id", -1))),
                )
            )

        cutoff = int(symbol_records[-1]["event_time"]) - self.window_ms
        while symbol_records and int(symbol_records[0]["event_time"]) < cutoff:
            expired = symbol_records.popleft()
            expired_trade_id = expired.get("trade_id")
            if expired_trade_id is not None:
                self.trade_ids[symbol].discard(int(expired_trade_id))
        return True

    def add_many(self, records: Iterable[dict[str, object]]) -> None:
        for record in records:
            self.add(record)

    def score_all(self, observed_at: int | None = None) -> list[ServingResult]:
        observed = observed_at or int(time.time() * 1000)
        results: list[ServingResult] = []
        for symbol, symbol_records in self.records.items():
            baseline = self.baselines.get(symbol)
            if baseline is None or len(symbol_records) < 2:
                continue

            first = symbol_records[0]
            last = symbol_records[-1]
            quote_volume = sum(float(item.get("quote_volume", 0.0)) for item in symbol_records)
            trade_count = sum(int(item.get("trade_count", 1)) for item in symbol_records)
            result = score_window(
                symbol=symbol,
                start_price=float(first["last_price"]),
                end_price=float(last["last_price"]),
                quote_volume_5m=quote_volume,
                trade_count_5m=trade_count,
                window_start=int(first["event_time"]),
                window_end=int(last["event_time"]),
                observed_at=observed,
                baseline=baseline,
                min_liquidity_usdt=self.min_liquidity_usdt,
                spike_zscore_threshold=self.spike_zscore_threshold,
                spike_abs_return_pct=self.spike_abs_return_pct,
            )
            if result is not None:
                results.append(result)
        return results

    def top_trending(self, n: int = 10) -> list[ServingResult]:
        return sorted(self.score_all(), key=lambda item: item.trend_score, reverse=True)[:n]

    def abnormal_spikes(self, n: int = 10) -> list[ServingResult]:
        spikes = [item for item in self.score_all() if item.is_spike]
        return sorted(spikes, key=lambda item: abs(item.spike_zscore), reverse=True)[:n]

    def serving_views(
        self,
        n: int = 10,
        observed_at: int | None = None,
    ) -> tuple[list[ServingResult], list[ServingResult]]:
        """Score once so trend and spike views share one coherent observation."""

        results = self.score_all(observed_at)
        trending = sorted(results, key=lambda item: item.trend_score, reverse=True)[:n]
        spikes = sorted(
            (item for item in results if item.is_spike),
            key=lambda item: abs(item.spike_zscore),
            reverse=True,
        )[:n]
        return trending, spikes


def _read_s3_text(uri: str, region_name: str | None = None) -> str:
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 baseline URI: {uri}")

    import boto3

    client = boto3.client("s3", region_name=region_name)
    if key.endswith("/"):
        response = client.list_objects_v2(Bucket=bucket, Prefix=key)
        keys = sorted(
            item["Key"]
            for item in response.get("Contents", [])
            if Path(item["Key"]).name.startswith("part-") and item["Key"].endswith((".json", ".jsonl"))
        )
        if not keys:
            raise FileNotFoundError(f"No Spark JSON part file under {uri}")
        key = keys[0]
    return client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def load_baselines(
    path: str | Path,
    region_name: str | None = None,
) -> dict[str, SymbolBaseline]:
    raw_path = str(path)
    if raw_path.startswith("s3://"):
        text = _read_s3_text(raw_path, region_name)
    else:
        local_path = Path(path)
        if local_path.is_dir():
            candidates = sorted(
                [
                    *local_path.glob("part-*.json"),
                    *local_path.glob("*.jsonl"),
                    *local_path.glob("*.json"),
                ]
            )
            if not candidates:
                raise FileNotFoundError(f"No JSON baseline part file found in {local_path}")
            local_path = candidates[0]
        text = local_path.read_text(encoding="utf-8")

    try:
        loaded = json.loads(text)
        if isinstance(loaded, dict) and "baselines" in loaded:
            items = loaded["baselines"]
        elif isinstance(loaded, dict):
            items = [loaded]
        else:
            items = loaded
    except json.JSONDecodeError:
        items = [json.loads(line) for line in text.splitlines() if line.strip()]
    baselines = [SymbolBaseline.from_dict(item) for item in items]
    return {baseline.symbol: baseline for baseline in baselines}


def iter_jsonl(path: Path | None) -> Iterable[dict[str, object]]:
    if path is None:
        import sys

        for line in sys.stdin:
            if line.strip():
                yield json.loads(line)
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def run_local_speed_layer(
    *,
    input_path: Path | None,
    baseline_path: str | Path,
    writer: ServingWriter,
    top_n: int,
    config_window_seconds: int,
    min_liquidity_usdt: float,
    refresh_records: int,
) -> None:
    baselines = load_baselines(baseline_path)
    aggregator = SlidingWindowAggregator(
        baselines=baselines,
        window_seconds=config_window_seconds,
        min_liquidity_usdt=min_liquidity_usdt,
    )
    seen = 0
    for record in iter_jsonl(input_path):
        aggregator.add(record)
        seen += 1
        if seen % refresh_records == 0:
            trending, spikes = aggregator.serving_views(top_n)
            writer.write(trending=trending, spikes=spikes)
    trending, spikes = aggregator.serving_views(top_n)
    writer.write(trending=trending, spikes=spikes)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local five-minute speed layer over normalized JSONL records.")
    parser.add_argument("--input-jsonl", type=Path, help="Normalized live records. Omit to read stdin.")
    parser.add_argument("--baseline-json", required=True, help="Local path or s3:// URI.")
    parser.add_argument("--output-json", type=Path, default=Path("data/serving/latest.json"))
    parser.add_argument("--top-n", type=int)
    parser.add_argument("--refresh-records", type=int, default=100)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    cfg = load_analytics_config()
    run_local_speed_layer(
        input_path=args.input_jsonl,
        baseline_path=args.baseline_json,
        writer=LocalServingWriter(args.output_json),
        top_n=args.top_n or cfg.top_n,
        config_window_seconds=cfg.window_seconds,
        min_liquidity_usdt=cfg.min_liquidity_usdt,
        refresh_records=args.refresh_records,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
