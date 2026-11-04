-- name: gold_reconciles_with_silver
-- severity: critical
-- layer: silver->gold
-- description: Delivered counts per day, tenant and carrier in gold must equal what silver says. Only silver rows older than the last gold refresh are compared, so in-flight data does not cause false alarms.
WITH last_refresh AS (
    SELECT COALESCE(max(refreshed_at), 'infinity'::timestamptz) AS at FROM gold.refresh_log
),
expected AS (
    SELECT (delivered_at AT TIME ZONE 'UTC')::date AS day,
           tenant_id,
           carrier,
           count(*)::int AS delivered
    FROM silver.shipments, last_refresh
    WHERE delivered_at IS NOT NULL
      AND label_created_at IS NOT NULL
      AND updated_at < last_refresh.at
    GROUP BY 1, 2, 3
)
SELECT COALESCE(e.day, g.day)             AS day,
       COALESCE(e.tenant_id, g.tenant_id) AS tenant_id,
       COALESCE(e.carrier, g.carrier)     AS carrier,
       e.delivered                        AS silver_delivered,
       g.shipments_delivered              AS gold_delivered
FROM expected e
FULL OUTER JOIN gold.carrier_daily_performance g
  ON g.day = e.day AND g.tenant_id = e.tenant_id AND g.carrier = e.carrier
WHERE e.delivered IS DISTINCT FROM g.shipments_delivered;
