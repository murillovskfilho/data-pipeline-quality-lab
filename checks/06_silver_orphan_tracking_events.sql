-- name: silver_orphan_tracking_events
-- severity: major
-- layer: silver
-- description: Every tracking event must belong to a known shipment (referential integrity, enforced by the pipeline rather than by a foreign key).
SELECT t.event_id,
       t.shipment_id
FROM silver.tracking_events t
LEFT JOIN silver.shipments s USING (shipment_id)
WHERE s.shipment_id IS NULL;
