"""Shard-parallel Kinesis Lambda speed layer with durable DynamoDB windows."""

from __future__ import annotations

import base64
import json
import os
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any

from .scoring import ServingResult, SymbolBaseline, score_window
from .speed import load_baselines
from .storage import to_dynamodb_item

_BASELINES: dict[str, SymbolBaseline] = {}
_BASELINES_LOADED_AT = 0.0


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def decode_lambda_record(record: dict[str, Any]) -> dict[str, object]:
    payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode("utf-8"))
    event_id = str(record.get("eventID", "unknown:0"))
    shard_id, _, sequence_from_id = event_id.partition(":")
    payload["_shard_id"] = shard_id
    payload["_sequence"] = str(record["kinesis"].get("sequenceNumber", sequence_from_id or "0"))
    return payload


def _sequence_is_new(sequence: str, previous: str | None) -> bool:
    if previous is None:
        return True
    try:
        return int(sequence) > int(previous)
    except ValueError:
        return sequence > previous


def merge_window_state(
    state: dict[str, Any],
    records: list[dict[str, object]],
    *,
    window_ms: int = 300_000,
    bucket_ms: int = 1_000,
) -> dict[str, Any]:
    """Merge ordered records idempotently into compact one-second buckets."""

    checkpoints = {
        str(key): str(value)
        for key, value in state.get("checkpoints", {}).items()
    }
    buckets = {
        int(item["bucket_start"]): dict(item)
        for item in state.get("buckets", [])
    }
    newest_event_time = int(state.get("newest_event_time", 0))

    for record in sorted(
        records,
        key=lambda item: (
            int(item["event_time"]),
            int(item.get("trade_id", -1)),
        ),
    ):
        shard_id = str(record.get("_shard_id", "local"))
        sequence = str(record.get("_sequence", record.get("trade_id", "0")))
        if not _sequence_is_new(sequence, checkpoints.get(shard_id)):
            continue

        event_time = int(record["event_time"])
        trade_id = int(record.get("trade_id", -1))
        price = float(record["last_price"])
        bucket_start = (event_time // bucket_ms) * bucket_ms
        bucket = buckets.get(bucket_start)
        if bucket is None:
            bucket = {
                "bucket_start": bucket_start,
                "first_event_time": event_time,
                "last_event_time": event_time,
                "first_trade_id": trade_id,
                "last_trade_id": trade_id,
                "first_price": price,
                "last_price": price,
                "quote_volume": 0.0,
                "trade_count": 0,
            }
            buckets[bucket_start] = bucket

        first_key = (int(bucket["first_event_time"]), int(bucket["first_trade_id"]))
        last_key = (int(bucket["last_event_time"]), int(bucket["last_trade_id"]))
        event_key = (event_time, trade_id)
        if event_key < first_key:
            bucket["first_event_time"] = event_time
            bucket["first_trade_id"] = trade_id
            bucket["first_price"] = price
        if event_key > last_key:
            bucket["last_event_time"] = event_time
            bucket["last_trade_id"] = trade_id
            bucket["last_price"] = price
        bucket["quote_volume"] = float(bucket["quote_volume"]) + float(record.get("quote_volume", 0.0))
        bucket["trade_count"] = int(bucket["trade_count"]) + int(record.get("trade_count", 1))
        checkpoints[shard_id] = sequence
        newest_event_time = max(newest_event_time, event_time)

    cutoff = newest_event_time - window_ms
    compact = [
        buckets[key]
        for key in sorted(buckets)
        if int(buckets[key]["last_event_time"]) >= cutoff
    ]
    return {
        "buckets": compact,
        "checkpoints": checkpoints,
        "newest_event_time": newest_event_time,
    }


def score_durable_window(
    symbol: str,
    state: dict[str, Any],
    baseline: SymbolBaseline,
    *,
    observed_at: int,
    min_liquidity_usdt: float,
    spike_zscore_threshold: float,
    spike_abs_return_pct: float,
) -> ServingResult | None:
    buckets = state.get("buckets", [])
    if len(buckets) < 2:
        return None
    first = buckets[0]
    last = buckets[-1]
    return score_window(
        symbol=symbol,
        start_price=float(first["first_price"]),
        end_price=float(last["last_price"]),
        quote_volume_5m=sum(float(item["quote_volume"]) for item in buckets),
        trade_count_5m=sum(int(item["trade_count"]) for item in buckets),
        window_start=int(first["first_event_time"]),
        window_end=int(last["last_event_time"]),
        observed_at=observed_at,
        baseline=baseline,
        min_liquidity_usdt=min_liquidity_usdt,
        spike_zscore_threshold=spike_zscore_threshold,
        spike_abs_return_pct=spike_abs_return_pct,
    )


def _get_baselines() -> dict[str, SymbolBaseline]:
    global _BASELINES, _BASELINES_LOADED_AT
    refresh_seconds = int(os.getenv("BASELINE_REFRESH_SECONDS", "300"))
    if not _BASELINES or time.monotonic() - _BASELINES_LOADED_AT >= refresh_seconds:
        _BASELINES = load_baselines(
            os.environ["BASELINE_S3_URI"],
            region_name=os.getenv("AWS_REGION"),
        )
        _BASELINES_LOADED_AT = time.monotonic()
    return _BASELINES


def _update_symbol(
    *,
    symbol: str,
    records: list[dict[str, object]],
    state_table: Any,
    latest_table: Any,
    baseline: SymbolBaseline,
    observed_at: int,
) -> None:
    from boto3.dynamodb.conditions import Attr
    from botocore.exceptions import ClientError

    window_ms = int(os.getenv("WINDOW_SECONDS", "300")) * 1000
    bucket_ms = int(os.getenv("STATE_BUCKET_MS", "1000"))
    retention_seconds = int(os.getenv("STATE_RETENTION_SECONDS", "900"))
    for _ in range(5):
        response = state_table.get_item(Key={"symbol": symbol}, ConsistentRead=True)
        stored = _plain(response.get("Item", {}))
        version = int(stored.get("version", 0))
        merged = merge_window_state(
            stored,
            records,
            window_ms=window_ms,
            bucket_ms=bucket_ms,
        )
        state_item = {
            "symbol": symbol,
            **merged,
            "version": version + 1,
            "updated_at": observed_at,
            "expires_at": int(time.time()) + retention_seconds,
        }
        try:
            kwargs: dict[str, Any] = {"Item": to_dynamodb_item(state_item)}
            if version:
                kwargs["ConditionExpression"] = Attr("version").eq(version)
            else:
                kwargs["ConditionExpression"] = Attr("version").not_exists()
            state_table.put_item(**kwargs)
            break
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
    else:
        raise RuntimeError(f"Could not checkpoint {symbol} after concurrent updates")

    result = score_durable_window(
        symbol,
        merged,
        baseline,
        observed_at=observed_at,
        min_liquidity_usdt=float(os.getenv("MIN_LIQUIDITY_USDT", "10000")),
        spike_zscore_threshold=float(os.getenv("SPIKE_ZSCORE_THRESHOLD", "3.0")),
        spike_abs_return_pct=float(os.getenv("SPIKE_ABS_RETURN_PCT", "1.5")),
    )
    if result is not None:
        latest_item = result.to_dict()
        latest_item["symbol"] = symbol
        latest_item["expires_at"] = int(time.time()) + retention_seconds
        latest_table.put_item(Item=to_dynamodb_item(latest_item))


def handler(event: dict[str, Any], context: object) -> dict[str, list[Any]]:
    """Process one Lambda Kinesis batch; Lambda parallelizes across shards."""

    import boto3

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in event.get("Records", []):
        decoded = decode_lambda_record(record)
        grouped[str(decoded["symbol"]).upper()].append(decoded)

    dynamodb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION"))
    state_table = dynamodb.Table(os.environ["SPEED_STATE_TABLE_NAME"])
    latest_table = dynamodb.Table(os.environ["LATEST_VIEW_TABLE_NAME"])
    baselines = _get_baselines()
    observed_at = int(time.time() * 1000)
    for symbol, records in grouped.items():
        baseline = baselines.get(symbol)
        if baseline is not None:
            _update_symbol(
                symbol=symbol,
                records=records,
                state_table=state_table,
                latest_table=latest_table,
                baseline=baseline,
                observed_at=observed_at,
            )
    return {"batchItemFailures": []}
