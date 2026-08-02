CREATE OR REPLACE VIEW trending_coins AS
WITH latest_window AS (
    SELECT max(window_end) AS window_end
    FROM batch_windows
),
historical_stats AS (
    SELECT
        symbol,
        avg(return_5m) AS mean_return_5m,
        stddev_samp(return_5m) AS std_return_5m,
        avg(quote_volume_5m) AS mean_quote_volume_5m,
        stddev_samp(quote_volume_5m) AS std_quote_volume_5m
    FROM batch_windows
    GROUP BY symbol
),
latest_rows AS (
    SELECT windows.*
    FROM batch_windows AS windows
    CROSS JOIN latest_window
    WHERE windows.window_end = latest_window.window_end
)
SELECT
    row_number() OVER (
        ORDER BY
            (
                coalesce(
                    (latest.return_5m - stats.mean_return_5m)
                    / nullif(stats.std_return_5m, 0),
                    0
                )
                + coalesce(
                    (latest.quote_volume_5m - stats.mean_quote_volume_5m)
                    / nullif(stats.std_quote_volume_5m, 0),
                    0
                )
            ) DESC
    ) AS trend_rank,
    latest.symbol,
    latest.last_price,
    latest.return_5m AS return_5m_pct,
    latest.quote_volume_5m,
    latest.trade_count_5m,
    (
        coalesce(
            (latest.return_5m - stats.mean_return_5m)
            / nullif(stats.std_return_5m, 0),
            0
        )
        + coalesce(
            (latest.quote_volume_5m - stats.mean_quote_volume_5m)
            / nullif(stats.std_quote_volume_5m, 0),
            0
        )
    ) AS trend_score,
    from_unixtime(latest.window_start / 1000.0) AS window_start,
    from_unixtime(latest.window_end / 1000.0) AS window_end
FROM latest_rows AS latest
JOIN historical_stats AS stats
  ON latest.symbol = stats.symbol;
