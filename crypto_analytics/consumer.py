"""Kinesis speed-layer consumer that writes serving views to DynamoDB or JSON."""

from __future__ import annotations

import argparse
import base64
import json
import time
from pathlib import Path
from typing import Sequence

from .config import load_analytics_config, load_aws_config
from .speed import SlidingWindowAggregator, load_baselines
from .storage import DynamoDbServingWriter, LocalServingWriter, ServingWriter


def decode_kinesis_data(data: object) -> dict[str, object]:
    if isinstance(data, bytes):
        raw = data
    elif isinstance(data, str):
        raw = base64.b64decode(data)
    else:
        raw = bytes(data)
    return json.loads(raw.decode("utf-8"))


class KinesisPollingConsumer:
    """Simple shard poller suitable for EC2/EMR demos."""

    def __init__(self, stream_name: str, region_name: str | None = None) -> None:
        import boto3

        self.client = boto3.client("kinesis", region_name=region_name)
        self.stream_name = stream_name

    def shard_ids(self) -> list[str]:
        response = self.client.describe_stream_summary(StreamName=self.stream_name)
        stream_arn = response["StreamDescriptionSummary"]["StreamARN"]
        shards = self.client.list_shards(StreamARN=stream_arn)["Shards"]
        return [shard["ShardId"] for shard in shards]

    def iter_records(self, iterator_type: str = "LATEST", sleep_seconds: float = 1.0):
        iterators = []
        for shard_id in self.shard_ids():
            response = self.client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType=iterator_type,
            )
            iterators.append(response["ShardIterator"])

        while True:
            next_iterators = []
            for iterator in iterators:
                response = self.client.get_records(ShardIterator=iterator, Limit=1000)
                next_iterator = response.get("NextShardIterator")
                if next_iterator:
                    next_iterators.append(next_iterator)
                for record in response.get("Records", []):
                    yield decode_kinesis_data(record["Data"])
            iterators = next_iterators
            if not iterators:
                time.sleep(sleep_seconds)
                iterators = [
                    self.client.get_shard_iterator(
                        StreamName=self.stream_name,
                        ShardId=shard_id,
                        ShardIteratorType=iterator_type,
                    )["ShardIterator"]
                    for shard_id in self.shard_ids()
                ]
            time.sleep(sleep_seconds)


def run_consumer(
    *,
    baseline_path: str,
    writer: ServingWriter,
    stream_name: str,
    region: str,
    refresh_seconds: int,
    top_n: int,
    max_records: int | None,
) -> None:
    analytics = load_analytics_config()
    baselines = load_baselines(baseline_path, region_name=region)
    aggregator = SlidingWindowAggregator(
        baselines=baselines,
        window_seconds=analytics.window_seconds,
        min_liquidity_usdt=analytics.min_liquidity_usdt,
        spike_zscore_threshold=analytics.spike_zscore_threshold,
        spike_abs_return_pct=analytics.spike_abs_return_pct,
    )
    consumer = KinesisPollingConsumer(stream_name=stream_name, region_name=region)
    next_publish = time.monotonic() + refresh_seconds
    seen = 0
    for record in consumer.iter_records():
        aggregator.add(record)
        seen += 1
        if time.monotonic() >= next_publish:
            trending, spikes = aggregator.serving_views(top_n)
            writer.write(trending, spikes)
            next_publish = time.monotonic() + refresh_seconds
        if max_records and seen >= max_records:
            trending, spikes = aggregator.serving_views(top_n)
            writer.write(trending, spikes)
            return


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume Kinesis events and maintain five-minute serving views.")
    parser.add_argument("--baseline-json", required=True, help="Local baseline path or s3:// URI.")
    parser.add_argument("--stream-name")
    parser.add_argument("--region")
    parser.add_argument("--output-json", type=Path, help="Write local serving JSON instead of DynamoDB.")
    parser.add_argument("--dynamodb-table")
    parser.add_argument("--refresh-seconds", type=int)
    parser.add_argument("--top-n", type=int)
    parser.add_argument("--max-records", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    aws = load_aws_config()
    analytics = load_analytics_config()
    writer: ServingWriter
    if args.output_json:
        writer = LocalServingWriter(args.output_json)
    else:
        writer = DynamoDbServingWriter(table_name=args.dynamodb_table or aws.dynamodb_table_name, region_name=args.region or aws.region)
    run_consumer(
        baseline_path=args.baseline_json,
        writer=writer,
        stream_name=args.stream_name or aws.kinesis_stream_name,
        region=args.region or aws.region,
        refresh_seconds=args.refresh_seconds or analytics.refresh_seconds,
        top_n=args.top_n or analytics.top_n,
        max_records=args.max_records,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
