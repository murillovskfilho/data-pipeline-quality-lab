-- name: bronze_to_silver_completeness
-- severity: critical
-- layer: bronze->silver
-- description: Every bronze message must be either promoted to silver, deduplicated against an existing silver event, or quarantined with a reason. Anything else was silently dropped.
SELECT b.id AS bronze_id,
       b.payload ->> 'event_id' AS event_id,
       b.ingested_at
FROM bronze.shipment_events b
WHERE NOT EXISTS (SELECT 1 FROM silver.tracking_events t WHERE t.bronze_id = b.id)
  AND NOT EXISTS (SELECT 1 FROM silver.rejected_events r WHERE r.bronze_id = b.id)
  AND NOT EXISTS (SELECT 1 FROM silver.tracking_events t WHERE t.event_id = b.payload ->> 'event_id');
