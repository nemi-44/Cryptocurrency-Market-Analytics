CREATE OR REPLACE VIEW abnormal_price_spikes AS
WITH latest_window AS (
    SELECT max(window_end) AS window_end
    FROM batch_windows
),
historical_stats AS (
    SELECT
        symbol,
        avg(return_5m) AS mean_return_5m,
        stddev_samp(return_5m) AS std_return_5m
    FROM batch_windows
    GROUP BY symbol
),
scored_latest AS (
    SELECT
        latest.symbol,
        latest.last_price,
        latest.return_5m,
        latest.quote_volume_5m,
        latest.trade_count_5m,
        latest.window_start,
        latest.window_end,
        (
            latest.return_5m - stats.mean_return_5m
        ) / nullif(stats.std_return_5m, 0) AS spike_zscore
    FROM batch_windows AS latest
    CROSS JOIN latest_window
    JOIN historical_stats AS stats
      ON latest.symbol = stats.symbol
    WHERE latest.window_end = latest_window.window_end
)
SELECT
    symbol,
    last_price,
    return_5m AS return_5m_pct,
    quote_volume_5m,
    trade_count_5m,
    spike_zscore,
    from_unixtime(window_start / 1000.0) AS window_start,
    from_unixtime(window_end / 1000.0) AS window_end
FROM scored_latest
WHERE abs(spike_zscore) >= 3
ORDER BY abs(spike_zscore) DESC;
