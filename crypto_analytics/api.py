"""API Gateway Lambda handler for the static S3 dashboard."""

from __future__ import annotations

import json
import os
from decimal import Decimal
from typing import Any


def decimal_to_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, dict):
        return {key: decimal_to_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decimal_to_json(item) for item in value]
    return value


def latest_payload_from_items(
    items: list[dict[str, Any]],
    freshness_ms: int = 30_000,
) -> dict[str, Any]:
    converted = [decimal_to_json(item) for item in items]
    if not converted:
        return {"trending": [], "spikes": [], "latest_window": None}

    latest_window_number = max(int(item.get("window_end", 0)) for item in converted)
    latest_window = str(latest_window_number)
    latest = [
        item
        for item in converted
        if int(item.get("window_end", 0)) >= latest_window_number - freshness_ms
    ]

    # The distributed speed layer stores one latest hybrid row per symbol.
    if any(item.get("view_type") == "hybrid" and "result_key" not in item for item in latest):
        unique = {
            str(item["symbol"]): item
            for item in latest
            if item.get("view_type") == "hybrid"
        }
        trending = sorted(
            unique.values(),
            key=lambda item: float(item.get("trend_score", 0.0)),
            reverse=True,
        )
        spikes = sorted(
            (item for item in unique.values() if item.get("is_spike")),
            key=lambda item: abs(float(item.get("spike_zscore", 0.0))),
            reverse=True,
        )
        for rank, item in enumerate(trending, start=1):
            item["rank"] = rank
            item["result_type"] = "trend"
        for rank, item in enumerate(spikes, start=1):
            item["spike_rank"] = rank
        return {
            "trending": trending,
            "spikes": spikes,
            "latest_window": latest_window,
        }

    latest = [item for item in latest if str(item.get("window_end", "")) == latest_window]
    trending = sorted(
        [item for item in latest if item.get("result_type") == "trend"],
        key=lambda item: int(item.get("rank", 9999)),
    )
    spikes = sorted(
        [item for item in latest if item.get("result_type") == "spike"],
        key=lambda item: int(item.get("rank", 9999)),
    )
    return {"trending": trending, "spikes": spikes, "latest_window": latest_window}


def response(status_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "content-type",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(payload, separators=(",", ":"), sort_keys=True),
    }


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    method = (
        event.get("requestContext", {})
        .get("http", {})
        .get("method", event.get("httpMethod", "GET"))
    )
    if method == "OPTIONS":
        return response(204, {})

    table_name = os.environ["DYNAMODB_TABLE_NAME"]
    import boto3
    from boto3.dynamodb.conditions import Key

    dynamodb = boto3.resource("dynamodb")
    latest_table_name = os.getenv("LATEST_VIEW_TABLE_NAME")
    if latest_table_name:
        latest_items = dynamodb.Table(latest_table_name).scan(Limit=500).get("Items", [])
        return response(200, latest_payload_from_items(latest_items))

    table = dynamodb.Table(table_name)
    trend_result = table.query(
        IndexName="result-type-index",
        KeyConditionExpression=Key("result_type").eq("trend"),
        ScanIndexForward=False,
        Limit=100,
    )
    trends = trend_result.get("Items", [])
    if not trends:
        return response(200, latest_payload_from_items([]))

    latest_window = max(str(item["window_end"]) for item in trends)
    latest_trends = [
        item for item in trends if str(item.get("window_end", "")) == latest_window
    ]
    spike_result = table.query(
        IndexName="result-type-index",
        KeyConditionExpression=(
            Key("result_type").eq("spike") & Key("window_end").eq(latest_window)
        ),
        ScanIndexForward=False,
        Limit=100,
    )
    return response(
        200,
        latest_payload_from_items(latest_trends + spike_result.get("Items", [])),
    )
