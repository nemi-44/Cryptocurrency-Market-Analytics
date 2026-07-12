"""Runtime configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AwsConfig:
    region: str = "eu-west-1"
    kinesis_stream_name: str = "crypto-market-events"
    dynamodb_table_name: str = "crypto-trend-serving"


@dataclass(frozen=True)
class AnalyticsConfig:
    window_seconds: int = 300
    refresh_seconds: int = 10
    min_liquidity_usdt: float = 10_000.0
    spike_zscore_threshold: float = 3.0
    spike_abs_return_pct: float = 1.5
    top_n: int = 10


def load_aws_config() -> AwsConfig:
    return AwsConfig(
        region=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-west-1")),
        kinesis_stream_name=os.getenv("KINESIS_STREAM_NAME", "crypto-market-events"),
        dynamodb_table_name=os.getenv("DYNAMODB_TABLE_NAME", "crypto-trend-serving"),
    )


def load_analytics_config() -> AnalyticsConfig:
    return AnalyticsConfig(
        window_seconds=int(os.getenv("WINDOW_SECONDS", "300")),
        refresh_seconds=int(os.getenv("REFRESH_SECONDS", "10")),
        min_liquidity_usdt=float(os.getenv("MIN_LIQUIDITY_USDT", "10000")),
        spike_zscore_threshold=float(os.getenv("SPIKE_ZSCORE_THRESHOLD", "3.0")),
        spike_abs_return_pct=float(os.getenv("SPIKE_ABS_RETURN_PCT", "1.5")),
        top_n=int(os.getenv("TOP_N", "10")),
    )

