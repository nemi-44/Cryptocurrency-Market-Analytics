"""Binance aggregate-trade ingestion and normalization.

Aggregate trades contain an exact price and quantity for every event. This is
deliberately preferred over rolling 1-hour/24-hour ticker counters: subtracting
two rolling counters does not produce an exact five-minute volume because old
trades also leave the source window.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlencode

from .config import load_analytics_config, load_aws_config
from .kinesis import KinesisPublisher, LocalJsonlPublisher, StdoutPublisher

LOGGER = logging.getLogger(__name__)

BINANCE_COMBINED_STREAM_URL = "wss://data-stream.binance.vision/stream"
LEVERAGED_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")


@dataclass(frozen=True)
class LiveTradeRecord:
    """Canonical record shared by Kinesis, Firehose, Spark, and the speed layer."""

    symbol: str
    event_time: int
    last_price: float
    quote_volume: float
    trade_count: int
    ingest_time: int
    trade_id: int
    event_type: str = "aggTrade"

    @property
    def base_asset(self) -> str:
        return self.symbol[:-4] if self.symbol.endswith("USDT") else self.symbol

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 2,
            "event_type": self.event_type,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "event_time": self.event_time,
            "last_price": self.last_price,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
            "ingest_time": self.ingest_time,
            "trade_id": self.trade_id,
        }


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_timestamp_ms(value: int | float | str) -> int:
    """Normalize Binance millisecond or microsecond timestamps to milliseconds."""

    timestamp = int(float(value))
    if timestamp > 99_999_999_999_999:
        return timestamp // 1000
    return timestamp


def is_usdt_spot_symbol(symbol: str) -> bool:
    """Keep likely spot USDT pairs and drop leveraged-token style symbols."""

    normalized = symbol.upper()
    if not normalized.endswith("USDT"):
        return False
    return not any(normalized.endswith(suffix) for suffix in LEVERAGED_SUFFIXES)


def normalize_agg_trade_payload(
    payload: dict[str, object],
    ingest_time: int | None = None,
) -> LiveTradeRecord | None:
    """Convert one Binance aggregate-trade payload to the canonical event."""

    symbol = str(payload.get("s", "")).upper()
    if not is_usdt_spot_symbol(symbol):
        return None

    try:
        price = float(payload["p"])
        quantity = float(payload["q"])
        return LiveTradeRecord(
            symbol=symbol,
            event_time=normalize_timestamp_ms(payload["E"]),
            last_price=price,
            quote_volume=price * quantity,
            trade_count=1,
            ingest_time=ingest_time or now_ms(),
            trade_id=int(payload["a"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        LOGGER.debug("Skipping invalid aggregate trade for %s: %s", symbol or "<unknown>", exc)
        return None


def parse_market_message(
    message: str | bytes,
    ingest_time: int | None = None,
) -> list[LiveTradeRecord]:
    """Parse raw or Binance combined-stream aggregate-trade messages."""

    decoded = message.decode("utf-8") if isinstance(message, bytes) else message
    payload = json.loads(decoded)
    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if payload.get("e") != "aggTrade":
        return []

    record = normalize_agg_trade_payload(payload, ingest_time)
    return [record] if record is not None else []


def parse_symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {
        part.strip().upper()
        for part in value.split(",")
        if part.strip() and is_usdt_spot_symbol(part.strip())
    }
    return symbols or None


def build_agg_trade_url(symbols: Iterable[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in sorted(set(symbols)))
    if not streams:
        raise ValueError("At least one valid USDT spot symbol is required")
    return f"{BINANCE_COMBINED_STREAM_URL}?{urlencode({'streams': streams})}"


class StopFlag:
    def __init__(self) -> None:
        self.stop = False

    def request_stop(self, *_: object) -> None:
        self.stop = True


async def stream_binance_to_sink(
    sink: object,
    *,
    symbols: set[str],
    url: str | None = None,
    max_records: int | None = None,
    max_session_seconds: int = 23 * 60 * 60 + 50 * 60,
) -> int:
    """Read Binance aggregate trades and publish canonical records to a sink."""

    import websockets

    stream_url = url or build_agg_trade_url(symbols)
    stop_flag = StopFlag()
    try:
        signal.signal(signal.SIGINT, stop_flag.request_stop)
        signal.signal(signal.SIGTERM, stop_flag.request_stop)
    except ValueError:
        pass

    published = 0
    while not stop_flag.stop:
        started = time.monotonic()
        try:
            async with websockets.connect(
                stream_url,
                ping_interval=20,
                ping_timeout=60,
                max_queue=4096,
            ) as websocket:
                LOGGER.info("Connected to aggregate trades for %s", ",".join(sorted(symbols)))
                while not stop_flag.stop and time.monotonic() - started < max_session_seconds:
                    records = parse_market_message(await websocket.recv())
                    if records:
                        await _publish_records(sink, (record.to_dict() for record in records))
                        published += len(records)
                    if max_records and published >= max_records:
                        return published
        except Exception as exc:  # pragma: no cover - live network path
            LOGGER.warning("Binance stream disconnected: %s", exc)
            await asyncio.sleep(5)
    return published


async def _publish_records(sink: object, records: Iterable[dict[str, object]]) -> None:
    batch = list(records)
    maybe_coro = sink.publish(batch)
    if asyncio.iscoroutine(maybe_coro):
        await maybe_coro


def build_sink(args: argparse.Namespace) -> object:
    if args.stdout:
        return StdoutPublisher()
    if args.output_jsonl:
        return LocalJsonlPublisher(Path(args.output_jsonl))
    aws = load_aws_config()
    return KinesisPublisher(
        stream_name=args.stream_name or aws.kinesis_stream_name,
        region_name=args.region or aws.region,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest exact Binance aggregate-trade events.")
    parser.add_argument("--url", help="Optional WebSocket URL override.")
    parser.add_argument("--stream-name", help="Kinesis stream name. Defaults to KINESIS_STREAM_NAME.")
    parser.add_argument("--region", help="AWS region. Defaults to AWS_REGION/AWS_DEFAULT_REGION.")
    parser.add_argument("--output-jsonl", help="Write canonical records locally instead of to Kinesis.")
    parser.add_argument("--stdout", action="store_true", help="Print canonical records instead of writing to AWS.")
    parser.add_argument(
        "--symbols",
        help="Comma-separated USDT symbols. Defaults to MARKET_SYMBOLS.",
    )
    parser.add_argument("--max-records", type=int, help="Stop after publishing this many records.")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    configured_symbols = set(load_analytics_config().market_symbols)
    symbols = parse_symbol_filter(args.symbols) or configured_symbols
    sink = build_sink(args)
    count = asyncio.run(
        stream_binance_to_sink(
            sink=sink,
            symbols=symbols,
            url=args.url,
            max_records=args.max_records,
        )
    )
    LOGGER.info("Published %s canonical aggregate-trade records", count)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
