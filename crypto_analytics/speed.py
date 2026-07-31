"""Five-minute speed-layer window aggregation."""

from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Iterable, Sequence

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

    def add(self, record: dict[str, object]) -> None:
        symbol = str(record["symbol"]).upper()
        event_time = int(record["event_time"])
        symbol_records = self.records[symbol]
        symbol_records.append(record)
        cutoff = event_time - self.window_ms
        while symbol_records and int(symbol_records[0]["event_time"]) < cutoff:
            symbol_records.popleft()

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
            quote_volume = max(0.0, float(last.get("quote_volume_1h", 0.0)) - float(first.get("quote_volume_1h", 0.0)))
            trade_count = max(0, int(last.get("trade_count_1h", 0)) - int(first.get("trade_count_1h", 0)))
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


def load_baselines(path: Path) -> dict[str, SymbolBaseline]:
    if path.is_dir():
        candidates = sorted([*path.glob("part-*.json"), *path.glob("*.jsonl"), *path.glob("*.json")])
        if not candidates:
            raise FileNotFoundError(f"No JSON baseline part file found in {path}")
        path = candidates[0]

    text = path.read_text(encoding="utf-8")
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
    baseline_path: Path,
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
            writer.write(
                trending=aggregator.top_trending(top_n),
                spikes=aggregator.abnormal_spikes(top_n),
            )
    writer.write(trending=aggregator.top_trending(top_n), spikes=aggregator.abnormal_spikes(top_n))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local five-minute speed layer over normalized JSONL records.")
    parser.add_argument("--input-jsonl", type=Path, help="Normalized live records. Omit to read stdin.")
    parser.add_argument("--baseline-json", type=Path, required=True)
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
