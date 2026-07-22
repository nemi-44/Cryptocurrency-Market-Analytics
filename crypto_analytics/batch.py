"""Batch-layer baseline calculation from Binance 1-minute kline CSV files."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from .binance import normalize_timestamp_ms
from .scoring import SymbolBaseline


@dataclass(frozen=True)
class Kline:
    symbol: str
    open_time: int
    open_price: float
    close_price: float
    quote_volume: float
    trade_count: int


def infer_symbol_from_path(path: Path) -> str:
    name = path.name
    if "-1m-" in name:
        return name.split("-1m-", 1)[0].upper()
    return name.split(".", 1)[0].upper()


def parse_kline_row(row: list[str], symbol: str) -> Kline | None:
    if not row or row[0].lower() == "open time":
        return None
    return Kline(
        symbol=symbol.upper(),
        open_time=normalize_timestamp_ms(row[0]),
        open_price=float(row[1]),
        close_price=float(row[4]),
        quote_volume=float(row[7]),
        trade_count=int(float(row[8])),
    )


def iter_kline_csv(path: Path, symbol: str | None = None) -> Iterator[Kline]:
    inferred = symbol or infer_symbol_from_path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                return
            with archive.open(csv_names[0]) as raw:
                text_rows = (line.decode("utf-8").strip().split(",") for line in raw if line.strip())
                for row in text_rows:
                    parsed = parse_kline_row(row, inferred)
                    if parsed:
                        yield parsed
        return

    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.reader(handle):
            parsed = parse_kline_row(row, inferred)
            if parsed:
                yield parsed


def iter_input_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    return sorted([*input_path.glob("*.csv"), *input_path.glob("*.zip")])


def build_5m_samples(klines: Iterable[Kline]) -> dict[str, list[dict[str, float]]]:
    by_symbol: dict[str, list[Kline]] = {}
    for kline in klines:
        by_symbol.setdefault(kline.symbol, []).append(kline)

    samples: dict[str, list[dict[str, float]]] = {}
    for symbol, rows in by_symbol.items():
        ordered = sorted(rows, key=lambda item: item.open_time)
        symbol_samples: list[dict[str, float]] = []
        for index in range(0, max(0, len(ordered) - 4)):
            window = ordered[index : index + 5]
            first = window[0]
            last = window[-1]
            if first.open_price <= 0:
                continue
            symbol_samples.append(
                {
                    "return_5m": ((last.close_price / first.open_price) - 1.0) * 100.0,
                    "quote_volume_5m": sum(item.quote_volume for item in window),
                    "trade_count_5m": float(sum(item.trade_count for item in window)),
                }
            )
        samples[symbol] = symbol_samples
    return samples


def stdev_or_zero(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def compute_baselines(klines: Iterable[Kline], updated_at: str | None = None) -> list[SymbolBaseline]:
    timestamp = updated_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    samples_by_symbol = build_5m_samples(klines)
    baselines: list[SymbolBaseline] = []
    for symbol, samples in samples_by_symbol.items():
        if not samples:
            continue
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


def read_klines(input_path: Path) -> list[Kline]:
    rows: list[Kline] = []
    for path in iter_input_files(input_path):
        rows.extend(iter_kline_csv(path))
    return rows


def write_baselines(output_path: Path, baselines: list[SymbolBaseline]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "baselines": [baseline.to_dict() for baseline in baselines],
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute historical five-minute baselines from Binance klines.")
    parser.add_argument("--input", type=Path, required=True, help="Directory or file containing Binance 1m kline CSV/ZIP files.")
    parser.add_argument("--output", type=Path, default=Path("data/baselines/baselines.json"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    baselines = compute_baselines(read_klines(args.input))
    write_baselines(args.output, baselines)
    print(f"Wrote {len(baselines)} baselines to {args.output}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
