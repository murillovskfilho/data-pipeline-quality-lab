-- name: gold_percentiles_sane
-- severity: major
-- layer: gold
-- description: Transit-time percentiles must be non-negative and monotonic (p50 <= p95), and every row must aggregate at least one shipment.
SELECT day,
       tenant_id,
       carrier,
       shipments_delivered,
       p50_transit_hours,
       p95_transit_hours
FROM gold.carrier_daily_performance
WHERE shipments_delivered <= 0
   OR p50_transit_hours < 0
   OR p50_transit_hours > p95_transit_hours;
