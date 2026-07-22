"""EMR PySpark job for historical five-minute baseline calculation."""

from __future__ import annotations

import argparse
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PySpark baseline job for Binance 1m klines.")
    parser.add_argument("--input", required=True, help="S3/local path to Binance kline CSV files.")
    parser.add_argument("--output", required=True, help="S3/local output path for baseline Parquet.")
    parser.add_argument("--json-output", help="Optional S3/local output path for JSONL baselines used by the speed consumer.")
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    spark = SparkSession.builder.appName("crypto-batch-baselines").getOrCreate()
    schema = StructType(
        [
            StructField("open_time_raw", LongType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("close_time_raw", LongType(), False),
            StructField("quote_volume", DoubleType(), False),
            StructField("trade_count", LongType(), False),
            StructField("taker_buy_base_volume", DoubleType(), True),
            StructField("taker_buy_quote_volume", DoubleType(), True),
            StructField("ignore", StringType(), True),
        ]
    )

    raw = (
        spark.read.option("header", "false")
        .schema(schema)
        .csv(args.input)
        .withColumn("source_file", F.input_file_name())
    )
    symbol = F.regexp_extract("source_file", r"([A-Z0-9]+)-1m-\d{4}-\d{2}-\d{2}", 1)
    normalized = raw.withColumn("symbol", symbol).withColumn(
        "open_time",
        F.when(F.col("open_time_raw") > F.lit(99_999_999_999_999), F.col("open_time_raw") / F.lit(1000)).otherwise(
            F.col("open_time_raw")
        ),
    )

    ordered = Window.partitionBy("symbol").orderBy("open_time")
    five_rows = ordered.rowsBetween(0, 4)
    samples = (
        normalized.withColumn("rows_in_window", F.count("*").over(five_rows))
        .withColumn("close_5m", F.last("close").over(five_rows))
        .withColumn("quote_volume_5m", F.sum("quote_volume").over(five_rows))
        .withColumn("trade_count_5m", F.sum("trade_count").over(five_rows))
        .where(F.col("rows_in_window") == 5)
        .where(F.col("open") > 0)
        .withColumn("return_5m", ((F.col("close_5m") / F.col("open")) - F.lit(1.0)) * F.lit(100.0))
    )

    baselines = (
        samples.groupBy("symbol")
        .agg(
            F.avg("return_5m").alias("mean_return_5m"),
            F.stddev_samp("return_5m").alias("std_return_5m"),
            F.avg("quote_volume_5m").alias("mean_quote_volume_5m"),
            F.stddev_samp("quote_volume_5m").alias("std_quote_volume_5m"),
            F.expr("percentile_approx(quote_volume_5m, 0.5)").alias("median_quote_volume_5m"),
            F.count("*").alias("sample_count"),
            F.avg("trade_count_5m").alias("mean_trade_count_5m"),
            F.stddev_samp("trade_count_5m").alias("std_trade_count_5m"),
            F.current_timestamp().alias("updated_at"),
        )
        .fillna({"std_return_5m": 0.0, "std_quote_volume_5m": 0.0, "std_trade_count_5m": 0.0})
    )
    baselines.write.mode("overwrite").parquet(args.output)
    if args.json_output:
        baselines.coalesce(1).write.mode("overwrite").json(args.json_output)
    spark.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
