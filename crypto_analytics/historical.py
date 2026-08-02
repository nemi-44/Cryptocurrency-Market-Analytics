"""Download real Binance Vision aggregate trades into the canonical schema."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import tempfile
import urllib.error
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Sequence

from .binance import is_usdt_spot_symbol, normalize_timestamp_ms

LOGGER = logging.getLogger(__name__)
BINANCE_VISION_DAILY = (
    "https://data.binance.vision/data/spot/daily/aggTrades/"
    "{symbol}/{symbol}-aggTrades-{day}.zip"
)


def dates_between(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def normalize_historical_row(row: list[str], symbol: str) -> dict[str, object]:
    """Normalize a Binance Vision aggTrades CSV row."""

    aggregate_trade_id = int(row[0])
    price = float(row[1])
    quantity = float(row[2])
    first_trade_id = int(row[3])
    last_trade_id = int(row[4])
    # Binance Vision switched recent archives from millisecond to microsecond
    # timestamps. Keep the canonical event schema in milliseconds, matching the
    # live WebSocket producer and the speed layer.
    event_time = normalize_timestamp_ms(row[5])
    normalized_symbol = symbol.upper()
    return {
        "schema_version": 2,
        "event_type": "aggTrade",
        "symbol": normalized_symbol,
        "base_asset": normalized_symbol[:-4],
        "event_time": event_time,
        "last_price": price,
        "quote_volume": price * quantity,
        "trade_count": max(1, last_trade_id - first_trade_id + 1),
        "ingest_time": event_time,
        "trade_id": aggregate_trade_id,
        "source": "binance-vision-historical",
    }


def download_day(
    symbol: str,
    day: date,
    output_dir: Path,
    *,
    overwrite: bool = False,
) -> Path | None:
    normalized = symbol.upper()
    day_text = day.isoformat()
    url = BINANCE_VISION_DAILY.format(symbol=normalized, day=day_text)
    output = output_dir / normalized / f"{normalized}-aggTrades-{day_text}.jsonl.gz"
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not overwrite:
        return output

    try:
        with tempfile.NamedTemporaryFile(suffix=".zip") as archive_file:
            LOGGER.info("Downloading %s", url)
            with urllib.request.urlopen(url, timeout=60) as response:
                archive_file.write(response.read())
                archive_file.flush()
            with zipfile.ZipFile(archive_file.name) as archive:
                csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
                if not csv_names:
                    raise ValueError(f"No CSV found in {url}")
                with archive.open(csv_names[0]) as raw, gzip.open(output, "wt", encoding="utf-8") as target:
                    reader = csv.reader(line.decode("utf-8") for line in raw)
                    for row in reader:
                        if not row or not row[0].isdigit():
                            continue
                        target.write(
                            json.dumps(
                                normalize_historical_row(row, normalized),
                                separators=(",", ":"),
                                sort_keys=True,
                            )
                        )
                        target.write("\n")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            LOGGER.warning("No Binance Vision file for %s on %s", normalized, day_text)
            return None
        raise
    return output


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download real historical Binance aggregate trades.")
    parser.add_argument("--symbols", required=True, help="Comma-separated USDT symbols.")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/binance-vision"))
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload and replace existing canonical JSONL files.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.end < args.start:
        raise ValueError("--end must not be before --start")
    symbols = [
        symbol.strip().upper()
        for symbol in args.symbols.split(",")
        if symbol.strip() and is_usdt_spot_symbol(symbol.strip())
    ]
    if not symbols:
        raise ValueError("At least one valid USDT spot symbol is required")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    outputs = [
        output
        for symbol in symbols
        for day in dates_between(args.start, args.end)
        if (
            output := download_day(
                symbol,
                day,
                args.output_dir,
                overwrite=args.overwrite,
            )
        )
        is not None
    ]
    print(f"Downloaded {len(outputs)} real-data files under {args.output_dir}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
