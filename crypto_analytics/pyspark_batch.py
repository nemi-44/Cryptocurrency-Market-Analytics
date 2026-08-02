"""EMR PySpark full-history batch view over Firehose JSON Lines archives."""

from __future__ import annotations

import argparse
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PySpark batch view for canonical market events.")
    parser.add_argument("--input", required=True, help="S3/local raw JSON Lines path, including GZIP files.")
    parser.add_argument("--output", required=True, help="S3/local output path for baseline Parquet.")
    parser.add_argument("--json-output", help="Optional JSON baseline path consumed by the speed layer.")
    parser.add_argument(
        "--batch-view-output",
        help="Optional Parquet path for all complete historical five-minute windows.",
    )
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--minimum-window-records", type=int, default=2)
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    spark = SparkSession.builder.appName("crypto-full-history-batch-view").getOrCreate()
    schema = StructType(
        [
            StructField("schema_version", LongType(), True),
            StructField("event_type", StringType(), True),
            StructField("symbol", StringType(), False),
            StructField("base_asset", StringType(), True),
            StructField("event_time", LongType(), False),
            StructField("last_price", DoubleType(), False),
            StructField("quote_volume", DoubleType(), False),
            StructField("trade_count", LongType(), False),
            StructField("ingest_time", LongType(), False),
            StructField("trade_id", LongType(), True),
        ]
    )

    raw = (
        spark.read.option("recursiveFileLookup", "true")
        .schema(schema)
        .json(args.input)
    )
    valid = (
        raw.where(F.col("symbol").isNotNull())
        .where(F.col("event_time").isNotNull())
        .where(F.col("last_price") > 0)
        .where(F.col("quote_volume") >= 0)
        # Be defensive with pre-normalization Binance Vision archives, whose
        # recent timestamps can be microseconds rather than milliseconds.
        .withColumn(
            "event_time",
            F.when(
                F.abs(F.col("event_time")) >= F.lit(100_000_000_000_000),
                (F.col("event_time") / F.lit(1000)).cast("long"),
            ).otherwise(F.col("event_time")),
        )
        .withColumn(
            "ingest_time",
            F.when(
                F.abs(F.col("ingest_time")) >= F.lit(100_000_000_000_000),
                (F.col("ingest_time") / F.lit(1000)).cast("long"),
            ).otherwise(F.col("ingest_time")),
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.from_unixtime(F.col("event_time") / F.lit(1000.0))),
        )
    )

    windows = (
        valid.groupBy("symbol", F.window("event_timestamp", args.window).alias("event_window"))
        .agg(
            F.min_by("last_price", "event_time").alias("first_price"),
            F.max_by("last_price", "event_time").alias("last_price"),
            F.sum("quote_volume").alias("quote_volume_5m"),
            F.sum("trade_count").alias("trade_count_5m"),
            F.count("*").alias("record_count"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
            F.max("ingest_time").alias("last_ingest_time"),
        )
        .where(F.col("record_count") >= args.minimum_window_records)
        .withColumn(
            "return_5m",
            ((F.col("last_price") / F.col("first_price")) - F.lit(1.0)) * F.lit(100.0),
        )
        .withColumn("window_start", (F.col("event_window.start").cast("double") * 1000).cast("long"))
        .withColumn("window_end", (F.col("event_window.end").cast("double") * 1000).cast("long"))
        .withColumn("latency_ms", F.greatest(F.lit(0), F.col("last_ingest_time") - F.col("last_event_time")))
        .drop("event_window")
    )

    if args.batch_view_output:
        windows.write.mode("overwrite").partitionBy("symbol").parquet(args.batch_view_output)

    baselines = (
        windows.groupBy("symbol")
        .agg(
            F.avg("return_5m").alias("mean_return_5m"),
            F.stddev_samp("return_5m").alias("std_return_5m"),
            F.avg("quote_volume_5m").alias("mean_quote_volume_5m"),
            F.stddev_samp("quote_volume_5m").alias("std_quote_volume_5m"),
            F.expr("percentile_approx(quote_volume_5m, 0.5)").alias("median_quote_volume_5m"),
            F.count("*").alias("sample_count"),
            F.avg("trade_count_5m").alias("mean_trade_count_5m"),
            F.stddev_samp("trade_count_5m").alias("std_trade_count_5m"),
        )
        .fillna(
            {
                "std_return_5m": 0.0,
                "std_quote_volume_5m": 0.0,
                "std_trade_count_5m": 0.0,
            }
        )
        .withColumn("updated_at", F.current_timestamp().cast("string"))
        .withColumn("view_type", F.lit("batch_baseline"))
        .withColumn("schema_version", F.lit(2))
    )

    baselines.write.mode("overwrite").parquet(args.output)
    if args.json_output:
        baselines.coalesce(1).write.mode("overwrite").json(args.json_output)
    spark.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
