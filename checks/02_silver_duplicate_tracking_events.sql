-- name: silver_duplicate_tracking_events
-- severity: critical
-- layer: silver
-- description: The same business event (shipment + type + event time) must exist once in silver, no matter how many times the producer replayed it.
SELECT shipment_id,
       event_type,
       occurred_at,
       count(*) AS copies
FROM silver.tracking_events
GROUP BY shipment_id, event_type, occurred_at
HAVING count(*) > 1;
