"""Runtime configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AwsConfig:
    region: str = "us-east-1"
    kinesis_stream_name: str = "crypto-analytics-market-events"
    dynamodb_table_name: str = "crypto-analytics-serving"


@dataclass(frozen=True)
class AnalyticsConfig:
    window_seconds: int = 300
    refresh_seconds: int = 10
    min_liquidity_usdt: float = 10_000.0
    spike_zscore_threshold: float = 3.0
    spike_abs_return_pct: float = 1.5
    top_n: int = 10
    market_symbols: tuple[str, ...] = (
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "XRPUSDT",
    )


def load_aws_config() -> AwsConfig:
    return AwsConfig(
        region=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
        kinesis_stream_name=os.getenv("KINESIS_STREAM_NAME", "crypto-analytics-market-events"),
        dynamodb_table_name=os.getenv("DYNAMODB_TABLE_NAME", "crypto-analytics-serving"),
    )


def load_analytics_config() -> AnalyticsConfig:
    symbols = tuple(
        symbol.strip().upper()
        for symbol in os.getenv(
            "MARKET_SYMBOLS",
            "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT",
        ).split(",")
        if symbol.strip()
    )
    return AnalyticsConfig(
        window_seconds=int(os.getenv("WINDOW_SECONDS", "300")),
        refresh_seconds=int(os.getenv("REFRESH_SECONDS", "10")),
        min_liquidity_usdt=float(os.getenv("MIN_LIQUIDITY_USDT", "10000")),
        spike_zscore_threshold=float(os.getenv("SPIKE_ZSCORE_THRESHOLD", "3.0")),
        spike_abs_return_pct=float(os.getenv("SPIKE_ABS_RETURN_PCT", "1.5")),
        top_n=int(os.getenv("TOP_N", "10")),
        market_symbols=symbols,
    )
