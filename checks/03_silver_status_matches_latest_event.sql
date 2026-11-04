-- name: silver_status_matches_latest_event
-- severity: critical
-- layer: silver
-- description: A shipment's status must reflect its latest event by event time, not by arrival time. Catches out-of-order events rolling the status back.
WITH latest AS (
    SELECT DISTINCT ON (shipment_id) shipment_id, event_type, occurred_at
    FROM silver.tracking_events
    ORDER BY shipment_id, occurred_at DESC
)
SELECT s.shipment_id,
       s.status        AS current_status,
       l.event_type    AS latest_event,
       l.occurred_at   AS latest_event_at
FROM silver.shipments s
JOIN latest l USING (shipment_id)
WHERE s.status <> l.event_type;
