import { db } from './resources';

/** Read-side helpers over the lake. Numeric columns come back from pg as strings; parse them here. */

export interface ShipmentRow {
  shipment_id: string;
  tenant_id: string;
  carrier: string;
  status: string;
  label_created_at: Date | null;
  delivered_at: Date | null;
  last_event_at: Date;
  weight_oz: string | null;
}

export async function getShipment(shipmentId: string): Promise<ShipmentRow | undefined> {
  const { rows } = await db().query<ShipmentRow>('SELECT * FROM silver.shipments WHERE shipment_id = $1', [shipmentId]);
  return rows[0];
}

export async function getTrackingEvents(shipmentId: string): Promise<{ event_id: string; event_type: string }[]> {
  const { rows } = await db().query(
    'SELECT event_id, event_type FROM silver.tracking_events WHERE shipment_id = $1 ORDER BY occurred_at',
    [shipmentId],
  );
  return rows;
}

export async function countBronze(shipmentId: string): Promise<number> {
  const { rows } = await db().query(
    "SELECT count(*) AS n FROM bronze.shipment_events WHERE payload ->> 'shipment_id' = $1",
    [shipmentId],
  );
  return Number(rows[0].n);
}

export async function getRejections(shipmentId: string): Promise<string[]> {
  const { rows } = await db().query<{ reason: string }>(
    `SELECT r.reason
     FROM silver.rejected_events r
     JOIN bronze.shipment_events b ON b.id = r.bronze_id
     WHERE b.payload ->> 'shipment_id' = $1
     ORDER BY b.id`,
    [shipmentId],
  );
  return rows.map((r) => r.reason);
}

export async function getRejectionsByRawValue(fragment: string): Promise<string[]> {
  const { rows } = await db().query<{ reason: string }>(
    `SELECT r.reason
     FROM silver.rejected_events r
     JOIN bronze.shipment_events b ON b.id = r.bronze_id
     WHERE strpos(b.raw_value, $1) > 0`,
    [fragment],
  );
  return rows.map((r) => r.reason);
}

export interface GoldRowDb {
  day: string;
  carrier: string;
  shipmentsDelivered: number;
  p50TransitHours: number;
  p95TransitHours: number;
}

export async function getGold(tenantId: string): Promise<GoldRowDb[]> {
  const { rows } = await db().query(
    `SELECT to_char(day, 'YYYY-MM-DD') AS day, carrier, shipments_delivered, p50_transit_hours, p95_transit_hours
     FROM gold.carrier_daily_performance
     WHERE tenant_id = $1
     ORDER BY day, carrier`,
    [tenantId],
  );
  return rows.map((r) => ({
    day: r.day,
    carrier: r.carrier,
    shipmentsDelivered: Number(r.shipments_delivered),
    p50TransitHours: Number(r.p50_transit_hours),
    p95TransitHours: Number(r.p95_transit_hours),
  }));
}
