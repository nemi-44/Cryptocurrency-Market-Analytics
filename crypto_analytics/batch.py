"""Local full-history batch view over canonical archived market events."""

from __future__ import annotations

import argparse
import gzip
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence, TextIO

from .binance import normalize_timestamp_ms
from .scoring import SymbolBaseline


@dataclass(frozen=True)
class MarketEvent:
    symbol: str
    event_time: int
    last_price: float
    quote_volume: float
    trade_count: int
    ingest_time: int
    trade_id: int | None = None

    @classmethod
    def from_dict(cls, item: dict[str, object]) -> "MarketEvent":
        return cls(
            symbol=str(item["symbol"]).upper(),
            event_time=normalize_timestamp_ms(item["event_time"]),
            last_price=float(item["last_price"]),
            quote_volume=float(item.get("quote_volume", 0.0)),
            trade_count=int(item.get("trade_count", 1)),
            ingest_time=normalize_timestamp_ms(item.get("ingest_time", item["event_time"])),
            trade_id=int(item["trade_id"]) if item.get("trade_id") is not None else None,
        )


def iter_input_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    supported = {".json", ".jsonl", ".gz"}
    return sorted(
        path
        for path in input_path.rglob("*")
        if path.is_file() and (path.suffix.lower() in supported or not path.suffix)
    )


def _iter_json_lines(handle: TextIO) -> Iterator[dict[str, object]]:
    for line_number, line in enumerate(handle, start=1):
        if not line.strip():
            continue
        try:
            loaded = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON Lines record at line {line_number}") from exc
        if not isinstance(loaded, dict):
            raise ValueError(f"Expected an object at line {line_number}")
        yield loaded


def iter_market_events(path: Path) -> Iterator[MarketEvent]:
    """Read uncompressed or Firehose GZIP JSON Lines events."""

    opener = gzip.open if path.suffix.lower() == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for item in _iter_json_lines(handle):
            yield MarketEvent.from_dict(item)


def build_5m_samples(
    events: Iterable[MarketEvent],
    *,
    window_seconds: int = 300,
) -> dict[str, list[dict[str, float]]]:
    """Build complete, event-time-aligned five-minute historical windows."""

    window_ms = window_seconds * 1000
    buckets: dict[tuple[str, int], list[MarketEvent]] = {}
    for event in events:
        bucket_start = (event.event_time // window_ms) * window_ms
        buckets.setdefault((event.symbol, bucket_start), []).append(event)

    samples: dict[str, list[dict[str, float]]] = {}
    for (symbol, _), rows in sorted(buckets.items()):
        ordered = sorted(rows, key=lambda item: (item.event_time, item.trade_id or -1))
        if len(ordered) < 2 or ordered[0].last_price <= 0:
            continue
        samples.setdefault(symbol, []).append(
            {
                "return_5m": ((ordered[-1].last_price / ordered[0].last_price) - 1.0) * 100.0,
                "quote_volume_5m": sum(item.quote_volume for item in ordered),
                "trade_count_5m": float(sum(item.trade_count for item in ordered)),
            }
        )
    return samples


def stdev_or_zero(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def compute_baselines(
    events: Iterable[MarketEvent],
    updated_at: str | None = None,
) -> list[SymbolBaseline]:
    timestamp = updated_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    samples_by_symbol = build_5m_samples(events)
    baselines: list[SymbolBaseline] = []
    for symbol, samples in samples_by_symbol.items():
        returns = [sample["return_5m"] for sample in samples]
        volumes = [sample["quote_volume_5m"] for sample in samples]
        trades = [sample["trade_count_5m"] for sample in samples]
        baselines.append(
            SymbolBaseline(
                symbol=symbol,
                mean_return_5m=statistics.fmean(returns),
                std_return_5m=stdev_or_zero(returns),
                mean_quote_volume_5m=statistics.fmean(volumes),
                std_quote_volume_5m=stdev_or_zero(volumes),
                median_quote_volume_5m=statistics.median(volumes),
                sample_count=len(samples),
                updated_at=timestamp,
                mean_trade_count_5m=statistics.fmean(trades),
                std_trade_count_5m=stdev_or_zero(trades),
            )
        )
    return sorted(baselines, key=lambda item: item.symbol)


def read_events(input_path: Path) -> list[MarketEvent]:
    rows: list[MarketEvent] = []
    for path in iter_input_files(input_path):
        rows.extend(iter_market_events(path))
    return rows


def write_baselines(output_path: Path, baselines: list[SymbolBaseline]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "view_type": "batch_baseline",
        "baselines": [baseline.to_dict() for baseline in baselines],
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute the full-history batch baseline from archived canonical JSON Lines events."
    )
    parser.add_argument("--input", type=Path, required=True, help="Event JSONL/GZIP file or directory.")
    parser.add_argument("--output", type=Path, default=Path("data/baselines/baselines.json"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    events = read_events(args.input)
    baselines = compute_baselines(events)
    write_baselines(args.output, baselines)
    print(f"Wrote {len(baselines)} baselines from {len(events)} archived events to {args.output}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
