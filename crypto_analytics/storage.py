"""Serving-layer writers for local JSON and DynamoDB."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from collections.abc import Mapping
from typing import Protocol

from .scoring import ServingResult


class ServingWriter(Protocol):
    def write(self, trending: list[ServingResult], spikes: list[ServingResult]) -> None:
        ...


class LocalServingWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, trending: list[ServingResult], spikes: list[ServingResult]) -> None:
        payload = {
            "trending": [item.to_dict() for item in trending],
            "spikes": [item.to_dict() for item in spikes],
        }
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


class DynamoDbServingWriter:
    def __init__(self, table_name: str, region_name: str | None = None) -> None:
        import boto3

        self.table = boto3.resource("dynamodb", region_name=region_name).Table(table_name)

    def write(self, trending: list[ServingResult], spikes: list[ServingResult]) -> None:
        with self.table.batch_writer() as batch:
            for rank, item in enumerate(trending, start=1):
                record = to_dynamodb_item(item.to_dict())
                record["result_type"] = "trend"
                record["result_key"] = f"trend#{item.symbol}"
                record["rank"] = rank
                record["window_end"] = str(record["window_end"])
                batch.put_item(Item=record)
            for rank, item in enumerate(spikes, start=1):
                record = to_dynamodb_item(item.to_dict())
                record["result_type"] = "spike"
                record["result_key"] = f"spike#{item.symbol}"
                record["rank"] = rank
                record["window_end"] = str(record["window_end"])
                batch.put_item(Item=record)


def to_dynamodb_item(value: object) -> object:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, Mapping):
        return {key: to_dynamodb_item(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [to_dynamodb_item(item) for item in value]
    return value
