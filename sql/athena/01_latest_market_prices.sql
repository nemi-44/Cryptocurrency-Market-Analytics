CREATE OR REPLACE VIEW latest_market_prices AS
WITH ranked_events AS (
    SELECT
        symbol,
        base_asset,
        last_price,
        quote_volume,
        event_time,
        ingest_time,
        row_number() OVER (
            PARTITION BY symbol
            ORDER BY event_time DESC, trade_id DESC
        ) AS event_rank
    FROM raw_market_events
    WHERE symbol IS NOT NULL
      AND last_price > 0
)
SELECT
    symbol,
    base_asset,
    last_price,
    quote_volume,
    from_unixtime(event_time / 1000.0) AS event_timestamp,
    greatest(0, ingest_time - event_time) AS ingestion_latency_ms
FROM ranked_events
WHERE event_rank = 1;
