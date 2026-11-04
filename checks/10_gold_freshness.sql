-- name: gold_freshness
-- severity: major
-- layer: gold
-- description: Gold must not lag silver by more than 30 seconds. Catches a stalled or silently disabled aggregation job.
SELECT max_silver_update,
       last_gold_refresh,
       max_silver_update - last_gold_refresh AS lag -- NULL when gold never refreshed
FROM (
    SELECT (SELECT max(updated_at) FROM silver.shipments)        AS max_silver_update,
           (SELECT max(refreshed_at) FROM gold.refresh_log)       AS last_gold_refresh
) f
WHERE max_silver_update > COALESCE(last_gold_refresh, '-infinity') + interval '30 seconds';
