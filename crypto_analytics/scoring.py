"""Trend ranking and abnormal price spike scoring."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolBaseline:
    symbol: str
    mean_return_5m: float
    std_return_5m: float
    mean_quote_volume_5m: float
    std_quote_volume_5m: float
    median_quote_volume_5m: float
    sample_count: int
    updated_at: str
    mean_trade_count_5m: float = 0.0
    std_trade_count_5m: float = 0.0

    @classmethod
    def from_dict(cls, item: dict[str, object]) -> "SymbolBaseline":
        return cls(
            symbol=str(item["symbol"]).upper(),
            mean_return_5m=float(item.get("mean_return_5m", 0.0)),
            std_return_5m=float(item.get("std_return_5m", 0.0)),
            mean_quote_volume_5m=float(item.get("mean_quote_volume_5m", 0.0)),
            std_quote_volume_5m=float(item.get("std_quote_volume_5m", 0.0)),
            median_quote_volume_5m=float(item.get("median_quote_volume_5m", 0.0)),
            sample_count=int(item.get("sample_count", 0)),
            updated_at=str(item.get("updated_at", "")),
            mean_trade_count_5m=float(item.get("mean_trade_count_5m", 0.0)),
            std_trade_count_5m=float(item.get("std_trade_count_5m", 0.0)),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "mean_return_5m": self.mean_return_5m,
            "std_return_5m": self.std_return_5m,
            "mean_quote_volume_5m": self.mean_quote_volume_5m,
            "std_quote_volume_5m": self.std_quote_volume_5m,
            "median_quote_volume_5m": self.median_quote_volume_5m,
            "sample_count": self.sample_count,
            "updated_at": self.updated_at,
            "mean_trade_count_5m": self.mean_trade_count_5m,
            "std_trade_count_5m": self.std_trade_count_5m,
        }


@dataclass(frozen=True)
class ServingResult:
    symbol: str
    base_asset: str
    price: float
    return_5m_pct: float
    quote_volume_5m: float
    trend_score: float
    spike_zscore: float
    is_spike: bool
    window_start: int
    window_end: int
    latency_ms: int
    result_type: str
    volume_zscore: float = 0.0
    trade_activity_zscore: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "price": self.price,
            "return_5m_pct": self.return_5m_pct,
            "quote_volume_5m": self.quote_volume_5m,
            "trend_score": self.trend_score,
            "spike_zscore": self.spike_zscore,
            "is_spike": self.is_spike,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "latency_ms": self.latency_ms,
            "result_type": self.result_type,
            "volume_zscore": self.volume_zscore,
            "trade_activity_zscore": self.trade_activity_zscore,
        }


def safe_zscore(value: float, mean: float, stddev: float) -> float:
    if stddev <= 1e-12:
        return 0.0
    return (value - mean) / stddev


def price_return_pct(start_price: float, end_price: float) -> float:
    if start_price <= 0:
        return 0.0
    return ((end_price / start_price) - 1.0) * 100.0


def score_window(
    *,
    symbol: str,
    start_price: float,
    end_price: float,
    quote_volume_5m: float,
    trade_count_5m: int,
    window_start: int,
    window_end: int,
    observed_at: int,
    baseline: SymbolBaseline,
    min_liquidity_usdt: float = 10_000.0,
    spike_zscore_threshold: float = 3.0,
    spike_abs_return_pct: float = 1.5,
) -> ServingResult | None:
    """Score one symbol window. Returns None when liquidity is too low."""

    if baseline.median_quote_volume_5m < min_liquidity_usdt or quote_volume_5m < min_liquidity_usdt:
        return None

    return_pct = price_return_pct(start_price, end_price)
    return_zscore = safe_zscore(return_pct, baseline.mean_return_5m, baseline.std_return_5m)
    volume_zscore = safe_zscore(quote_volume_5m, baseline.mean_quote_volume_5m, baseline.std_quote_volume_5m)
    trade_activity_zscore = safe_zscore(float(trade_count_5m), baseline.mean_trade_count_5m, baseline.std_trade_count_5m)
    trend_score = (0.45 * volume_zscore) + (0.35 * abs(return_zscore)) + (0.20 * trade_activity_zscore)
    is_spike = abs(return_zscore) >= spike_zscore_threshold and abs(return_pct) >= spike_abs_return_pct

    normalized = symbol.upper()
    return ServingResult(
        symbol=normalized,
        base_asset=normalized[:-4] if normalized.endswith("USDT") else normalized,
        price=end_price,
        return_5m_pct=return_pct,
        quote_volume_5m=quote_volume_5m,
        trend_score=trend_score,
        spike_zscore=return_zscore,
        is_spike=is_spike,
        window_start=window_start,
        window_end=window_end,
        latency_ms=max(0, observed_at - window_end),
        result_type="spike" if is_spike else "trend",
        volume_zscore=volume_zscore,
        trade_activity_zscore=trade_activity_zscore,
    )

