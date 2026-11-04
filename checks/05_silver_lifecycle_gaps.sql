-- name: silver_lifecycle_gaps
-- severity: minor
-- layer: silver
-- description: A delivered shipment must have a LABEL_CREATED event. A gap means an upstream event never arrived, and the shipment is excluded from transit-time metrics.
SELECT s.shipment_id,
       s.carrier,
       s.delivered_at
FROM silver.shipments s
WHERE s.delivered_at IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.tracking_events t
      WHERE t.shipment_id = s.shipment_id AND t.event_type = 'LABEL_CREATED'
  );
