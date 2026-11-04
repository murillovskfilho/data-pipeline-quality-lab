-- name: silver_delivery_timestamps
-- severity: major
-- layer: silver
-- description: Delivered shipments need a delivery timestamp, and nothing can be delivered before its label was created.
SELECT shipment_id,
       status,
       label_created_at,
       delivered_at
FROM silver.shipments
WHERE (status = 'DELIVERED' AND delivered_at IS NULL)
   OR delivered_at < label_created_at;
