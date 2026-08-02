"""Hold a large YARN executor container long enough to exercise EMR auto scaling."""

from __future__ import annotations

import argparse
import time
from typing import Iterator, Sequence


def _hold_partition(values: Iterator[int], duration_seconds: int) -> Iterator[int]:
    count = sum(1 for _ in values)
    time.sleep(duration_seconds)
    yield count


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EMR auto-scaling stress step.")
    parser.add_argument("--duration-seconds", type=int, default=420)
    parser.add_argument("--partitions", type=int, default=4)
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("crypto-emr-scaling-evidence").getOrCreate()
    counts = (
        spark.sparkContext.parallelize(range(args.partitions), args.partitions)
        .mapPartitions(
            lambda values: _hold_partition(values, args.duration_seconds)
        )
        .collect()
    )
    print(f"Held {len(counts)} partitions for {args.duration_seconds} seconds")
    spark.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
