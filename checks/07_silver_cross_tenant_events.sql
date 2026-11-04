-- name: silver_cross_tenant_events
-- severity: critical
-- layer: silver
-- description: Tenant isolation. A tracking event must never be attached to a shipment owned by a different tenant.
SELECT t.event_id,
       t.shipment_id,
       t.tenant_id AS event_tenant,
       s.tenant_id AS shipment_tenant
FROM silver.tracking_events t
JOIN silver.shipments s USING (shipment_id)
WHERE t.tenant_id <> s.tenant_id;
