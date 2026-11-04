import type { Pool, PoolClient } from 'pg';
import type { Fault } from '../config';
import { validateEvent, type RejectionReason, type ShipmentEvent } from '../domain/events';

export interface RawMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string | null;
}

export type Outcome = 'processed' | 'duplicate' | 'rejected' | 'already_ingested';

/**
 * Lands one Kafka message in bronze and promotes it to silver, atomically.
 * Bronze keeps everything (even garbage); silver only gets valid, deduplicated, ordered state.
 */
export async function processMessage(
  pool: Pool,
  msg: RawMessage,
  faults: ReadonlySet<Fault>,
): Promise<{ outcome: Outcome; reason?: RejectionReason }> {
  const raw = msg.value ?? '';
  let payload: unknown;
  let malformed = false;
  try {
    payload = JSON.parse(raw);
  } catch {
    malformed = true;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const bronze = await client.query<{ id: string }>(
      `INSERT INTO bronze.shipment_events (topic, kafka_partition, kafka_offset, message_key, raw_value, payload)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
       RETURNING id`,
      [msg.topic, msg.partition, msg.offset, msg.key, raw, malformed ? null : JSON.stringify(payload)],
    );
    // Same offset seen again (consumer restart / rebalance): already handled.
    if (bronze.rowCount === 0) {
      await client.query('COMMIT');
      return { outcome: 'already_ingested' };
    }
    const bronzeId = bronze.rows[0].id;

    const validation = malformed ? ({ ok: false, reason: 'malformed_json' } as const) : validateEvent(payload);
    if (!validation.ok) {
      await reject(client, bronzeId, validation.reason);
      await client.query('COMMIT');
      return { outcome: 'rejected', reason: validation.reason };
    }
    const event = validation.event;

    const owner = await client.query<{ tenant_id: string }>(
      'SELECT tenant_id FROM silver.shipments WHERE shipment_id = $1 FOR UPDATE',
      [event.shipment_id],
    );
    if (owner.rowCount && owner.rows[0].tenant_id !== event.tenant_id) {
      await reject(client, bronzeId, 'tenant_mismatch');
      await client.query('COMMIT');
      return { outcome: 'rejected', reason: 'tenant_mismatch' };
    }

    // FAULT dup_events: the dedup key accidentally includes the offset, so producer replays slip through.
    const storedEventId = faults.has('dup_events') ? `${event.event_id}#${msg.offset}` : event.event_id;
    const tracking = await client.query(
      `INSERT INTO silver.tracking_events (event_id, shipment_id, tenant_id, event_type, occurred_at, bronze_id)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (event_id) DO NOTHING`,
      [storedEventId, event.shipment_id, event.tenant_id, event.event_type, event.occurred_at, bronzeId],
    );
    if (tracking.rowCount === 0) {
      await client.query('COMMIT');
      return { outcome: 'duplicate' };
    }

    await upsertShipment(client, event, faults.has('ignore_order'));
    await client.query('COMMIT');
    return { outcome: 'processed' };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

async function reject(client: PoolClient, bronzeId: string, reason: RejectionReason): Promise<void> {
  await client.query('INSERT INTO silver.rejected_events (bronze_id, reason) VALUES ($1, $2)', [bronzeId, reason]);
}

async function upsertShipment(client: PoolClient, e: ShipmentEvent, ignoreOrder: boolean): Promise<void> {
  const d = e.details ?? {};
  await client.query(
    `INSERT INTO silver.shipments AS s
       (shipment_id, tenant_id, carrier, service_level, origin_zip, destination_zip, weight_oz,
        status, label_created_at, delivered_at, last_event_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     ON CONFLICT (shipment_id) DO UPDATE SET
       service_level    = COALESCE(EXCLUDED.service_level, s.service_level),
       origin_zip       = COALESCE(EXCLUDED.origin_zip, s.origin_zip),
       destination_zip  = COALESCE(EXCLUDED.destination_zip, s.destination_zip),
       weight_oz        = COALESCE(EXCLUDED.weight_oz, s.weight_oz),
       label_created_at = COALESCE(s.label_created_at, EXCLUDED.label_created_at),
       delivered_at     = COALESCE(EXCLUDED.delivered_at, s.delivered_at),
       -- Late (out-of-order) events must not roll the status back.
       -- FAULT ignore_order: last-write-wins instead of latest-event-wins.
       status        = CASE WHEN $12::boolean OR EXCLUDED.last_event_at >= s.last_event_at
                            THEN EXCLUDED.status ELSE s.status END,
       last_event_at = CASE WHEN $12::boolean THEN EXCLUDED.last_event_at
                            ELSE GREATEST(s.last_event_at, EXCLUDED.last_event_at) END,
       updated_at    = now()`,
    [
      e.shipment_id,
      e.tenant_id,
      e.carrier,
      d.service_level ?? null,
      d.origin_zip ?? null,
      d.destination_zip ?? null,
      d.weight_oz ?? null,
      e.event_type,
      e.event_type === 'LABEL_CREATED' ? e.occurred_at : null,
      e.event_type === 'DELIVERED' ? e.occurred_at : null,
      e.occurred_at,
      ignoreOrder,
    ],
  );
}
