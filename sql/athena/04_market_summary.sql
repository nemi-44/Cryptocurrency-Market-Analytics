CREATE OR REPLACE VIEW market_summary AS
SELECT
    symbol,
    count(*) AS event_count,
    min(from_unixtime(event_time / 1000.0)) AS first_event_timestamp,
    max(from_unixtime(event_time / 1000.0)) AS latest_event_timestamp,
    sum(quote_volume) AS total_quote_volume,
    avg(last_price) AS average_trade_price,
    max(ingest_time - event_time) AS maximum_ingestion_latency_ms
FROM raw_market_events
WHERE symbol IS NOT NULL
GROUP BY symbol;
