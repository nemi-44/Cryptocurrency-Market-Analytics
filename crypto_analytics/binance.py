"""Binance WebSocket payload parsing and live producer support."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from .config import load_aws_config
from .kinesis import KinesisPublisher, LocalJsonlPublisher, StdoutPublisher

LOGGER = logging.getLogger(__name__)

BINANCE_MARKET_STREAM_URL = "wss://data-stream.binance.vision/ws/!ticker_1h@arr"
LEVERAGED_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")


@dataclass(frozen=True)
class LiveTickerRecord:
    symbol: str
    event_time: int
    last_price: float
    open_price_1h: float
    high_1h: float
    low_1h: float
    quote_volume_1h: float
    trade_count_1h: int
    ingest_time: int

    @property
    def base_asset(self) -> str:
        return self.symbol[:-4] if self.symbol.endswith("USDT") else self.symbol

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "event_time": self.event_time,
            "last_price": self.last_price,
            "open_price_1h": self.open_price_1h,
            "high_1h": self.high_1h,
            "low_1h": self.low_1h,
            "quote_volume_1h": self.quote_volume_1h,
            "trade_count_1h": self.trade_count_1h,
            "ingest_time": self.ingest_time,
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


def normalize_ticker_payload(payload: dict[str, object], ingest_time: int | None = None) -> LiveTickerRecord | None:
    """Convert a Binance rolling-window ticker payload into the live record schema."""

    symbol = str(payload.get("s", "")).upper()
    if not is_usdt_spot_symbol(symbol):
        return None

    try:
        return LiveTickerRecord(
            symbol=symbol,
            event_time=normalize_timestamp_ms(payload["E"]),
            last_price=float(payload["c"]),
            open_price_1h=float(payload["o"]),
            high_1h=float(payload["h"]),
            low_1h=float(payload["l"]),
            quote_volume_1h=float(payload["q"]),
            trade_count_1h=int(payload.get("n", 0)),
            ingest_time=ingest_time or now_ms(),
        )
    except (KeyError, TypeError, ValueError) as exc:
        LOGGER.debug("Skipping invalid ticker payload for %s: %s", symbol or "<unknown>", exc)
        return None


def parse_ticker_message(message: str | bytes, ingest_time: int | None = None) -> list[LiveTickerRecord]:
    """Parse a raw Binance WebSocket message into normalized records."""

    decoded = message.decode("utf-8") if isinstance(message, bytes) else message
    payload = json.loads(decoded)
    items: Iterable[dict[str, object]]

    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
        items = payload["data"]  # combined-stream format
    elif isinstance(payload, dict):
        items = [payload]
    else:
        return []

    observed = ingest_time or now_ms()
    records = [normalize_ticker_payload(item, observed) for item in items]
    return [record for record in records if record is not None]


class StopFlag:
    def __init__(self) -> None:
        self.stop = False

    def request_stop(self, *_: object) -> None:
        self.stop = True


async def stream_binance_to_sink(
    sink: object,
    url: str = BINANCE_MARKET_STREAM_URL,
    max_records: int | None = None,
    symbols: set[str] | None = None,
    max_session_seconds: int = 23 * 60 * 60 + 50 * 60,
) -> int:
    """Read Binance ticker arrays and publish normalized records to a sink."""

    import websockets

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
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as websocket:
                LOGGER.info("Connected to %s", url)
                while not stop_flag.stop and time.monotonic() - started < max_session_seconds:
                    raw_message = await websocket.recv()
                    records = parse_ticker_message(raw_message)
                    if symbols:
                        records = [record for record in records if record.symbol in symbols]
                    if records:
                        await _publish_records(sink, (record.to_dict() for record in records))
                        published += len(records)
                    if max_records and published >= max_records:
                        return published
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Binance stream disconnected: %s", exc)
            await asyncio.sleep(5)
    return published


async def _publish_records(sink: object, records: Iterable[dict[str, object]]) -> None:
    batch = list(records)
    maybe_coro = sink.publish(batch)
    if asyncio.iscoroutine(maybe_coro):
        await maybe_coro


def parse_symbol_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    return {part.strip().upper() for part in value.split(",") if part.strip()}


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
    parser = argparse.ArgumentParser(description="Ingest Binance all-market 1h ticker stream.")
    parser.add_argument("--url", default=BINANCE_MARKET_STREAM_URL)
    parser.add_argument("--stream-name", help="Kinesis stream name. Defaults to KINESIS_STREAM_NAME.")
    parser.add_argument("--region", help="AWS region. Defaults to AWS_REGION/AWS_DEFAULT_REGION.")
    parser.add_argument("--output-jsonl", help="Write normalized records to a local JSONL file instead of Kinesis.")
    parser.add_argument("--stdout", action="store_true", help="Print normalized records instead of writing to AWS.")
    parser.add_argument("--symbols", help="Comma-separated symbol allow-list, for example BTCUSDT,ETHUSDT.")
    parser.add_argument("--max-records", type=int, help="Stop after publishing this many records.")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    sink = build_sink(args)
    count = asyncio.run(
        stream_binance_to_sink(
            sink=sink,
            url=args.url,
            max_records=args.max_records,
            symbols=parse_symbol_filter(args.symbols),
        )
    )
    LOGGER.info("Published %s normalized records", count)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

