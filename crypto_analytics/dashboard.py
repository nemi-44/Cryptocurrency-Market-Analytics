"""Streamlit dashboard for local JSON or DynamoDB serving views."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from .config import load_aws_config


def decimal_to_float(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    return value


def load_local_serving(path: Path) -> dict[str, list[dict[str, object]]]:
    if not path.exists():
        return {"trending": [], "spikes": []}
    return json.loads(path.read_text(encoding="utf-8"))


def load_dynamodb_serving(table_name: str, region_name: str | None = None) -> dict[str, list[dict[str, object]]]:
    import boto3

    table = boto3.resource("dynamodb", region_name=region_name).Table(table_name)
    response = table.scan(Limit=500)
    items = [{key: decimal_to_float(value) for key, value in item.items()} for item in response.get("Items", [])]
    items.sort(key=lambda item: (str(item.get("window_end", "")), int(item.get("rank", 9999))), reverse=True)
    latest_window = str(items[0].get("window_end", "")) if items else ""
    latest = [item for item in items if str(item.get("window_end", "")) == latest_window]
    return {
        "trending": sorted([item for item in latest if item.get("result_type") == "trend"], key=lambda item: int(item.get("rank", 9999))),
        "spikes": sorted([item for item in latest if item.get("result_type") == "spike"], key=lambda item: int(item.get("rank", 9999))),
    }


def render_dashboard(payload: dict[str, list[dict[str, object]]], refresh_seconds: int) -> None:
    import streamlit as st
    import streamlit.components.v1 as components

    st.set_page_config(page_title="Crypto Trend & Spike Analytics", layout="wide")
    components.html(f"<script>setTimeout(() => window.parent.location.reload(), {refresh_seconds * 1000});</script>", height=0)

    trending = payload.get("trending", [])
    spikes = payload.get("spikes", [])
    latest_window = ""
    if trending:
        latest_window = str(trending[0].get("window_end", ""))
    elif spikes:
        latest_window = str(spikes[0].get("window_end", ""))

    st.title("Crypto Trend & Spike Analytics")
    metric_cols = st.columns(4)
    metric_cols[0].metric("Trending Rows", len(trending))
    metric_cols[1].metric("Spike Rows", len(spikes))
    metric_cols[2].metric("Refresh", f"{refresh_seconds}s")
    metric_cols[3].metric("Latest Window", latest_window or "waiting")

    left, right = st.columns(2)
    with left:
        st.subheader("Trending Now")
        st.dataframe(
            trending,
            use_container_width=True,
            column_order=["symbol", "price", "return_5m_pct", "quote_volume_5m", "trend_score", "latency_ms"],
        )
    with right:
        st.subheader("Abnormal Price Spikes")
        st.dataframe(
            spikes,
            use_container_width=True,
            column_order=["symbol", "price", "return_5m_pct", "quote_volume_5m", "spike_zscore", "latency_ms"],
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the Streamlit dashboard.")
    parser.add_argument("--local-json", type=Path, default=Path("data/serving/latest.json"))
    parser.add_argument("--dynamodb", action="store_true")
    parser.add_argument("--table-name")
    parser.add_argument("--region")
    parser.add_argument("--refresh-seconds", type=int, default=10)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    aws = load_aws_config()
    if args.dynamodb:
        payload = load_dynamodb_serving(table_name=args.table_name or aws.dynamodb_table_name, region_name=args.region or aws.region)
    else:
        payload = load_local_serving(args.local_json)
    render_dashboard(payload, args.refresh_seconds)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

