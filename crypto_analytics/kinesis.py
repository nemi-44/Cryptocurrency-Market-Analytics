"""Publishers for Kinesis, local files, and stdout."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


class StdoutPublisher:
    def publish(self, records: Iterable[dict[str, object]]) -> None:
        for record in records:
            print(json.dumps(record, separators=(",", ":"), sort_keys=True))


class LocalJsonlPublisher:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def publish(self, records: Iterable[dict[str, object]]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":"), sort_keys=True))
                handle.write("\n")


class KinesisPublisher:
    """Small Kinesis PutRecords wrapper with lazy boto3 import."""

    def __init__(self, stream_name: str, region_name: str | None = None, client: object | None = None) -> None:
        self.stream_name = stream_name
        if client is not None:
            self.client = client
        else:
            import boto3

            self.client = boto3.client("kinesis", region_name=region_name)

    def publish(self, records: Iterable[dict[str, object]]) -> None:
        batch: list[dict[str, str]] = []
        for record in records:
            batch.append(
                {
                    "Data": json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8"),
                    "PartitionKey": str(record.get("symbol", "UNKNOWN")),
                }
            )
            if len(batch) == 500:
                self._flush(batch)
                batch = []
        if batch:
            self._flush(batch)

    def _flush(self, batch: list[dict[str, str]]) -> None:
        response = self.client.put_records(StreamName=self.stream_name, Records=batch)
        failed = int(response.get("FailedRecordCount", 0))
        if not failed:
            return
        retry_records = [
            source
            for source, result in zip(batch, response.get("Records", []), strict=False)
            if "ErrorCode" in result
        ]
        if retry_records:
            retry_response = self.client.put_records(StreamName=self.stream_name, Records=retry_records)
            retry_failed = int(retry_response.get("FailedRecordCount", 0))
            if retry_failed:
                raise RuntimeError(f"Kinesis PutRecords failed for {retry_failed} records after retry")

