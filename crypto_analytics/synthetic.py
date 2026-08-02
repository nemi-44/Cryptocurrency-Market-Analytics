"""Deterministic synthetic event generation for repeatable load tests."""

from __future__ import annotations

import math
from collections.abc import Iterator

from .batch import MarketEvent


def generate_market_events(
    *,
    count: int,
    symbol_count: int = 32,
    start_time_ms: int = 1_800_000_000_000,
    interval_ms: int = 100,
) -> Iterator[MarketEvent]:
    if count < 1 or symbol_count < 1:
        return

    for index in range(count):
        symbol_number = index % symbol_count
        symbol = f"COIN{symbol_number:03d}USDT"
        event_time = start_time_ms + (index * interval_ms)
        base_price = 10.0 + symbol_number
        price = base_price * (1.0 + 0.01 * math.sin(index / 250.0))
        quote_volume = 100.0 + ((index * 17) % 900)
        yield MarketEvent(
            symbol=symbol,
            event_time=event_time,
            last_price=price,
            quote_volume=quote_volume,
            trade_count=1,
            ingest_time=event_time + 25 + (index % 20),
            trade_id=index,
        )
