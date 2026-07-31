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


def latest_payload_from_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    converted = [decimal_to_json(item) for item in items]
    if not converted:
        return {"trending": [], "spikes": [], "latest_window": None}

    latest_window = max(str(item.get("window_end", "")) for item in converted)
    latest = [item for item in converted if str(item.get("window_end", "")) == latest_window]
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

    table = boto3.resource("dynamodb").Table(table_name)
    items: list[dict[str, Any]] = []
    scan_kwargs: dict[str, Any] = {"Limit": 500}
    while True:
        result = table.scan(**scan_kwargs)
        items.extend(result.get("Items", []))
        if "LastEvaluatedKey" not in result or len(items) >= 500:
            break
        scan_kwargs["ExclusiveStartKey"] = result["LastEvaluatedKey"]

    return response(200, latest_payload_from_items(items))

