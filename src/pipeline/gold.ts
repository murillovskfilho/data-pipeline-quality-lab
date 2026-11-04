import type { Pool } from 'pg';

/**
 * Rebuilds the gold layer from silver in a single transaction, so readers never see a
 * half-built table. A full rebuild is fine at lab scale and keeps the logic auditable.
 */
export async function refreshGold(pool: Pool): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM gold.carrier_daily_performance');
    const inserted = await client.query(`
      INSERT INTO gold.carrier_daily_performance
        (day, tenant_id, carrier, shipments_delivered, p50_transit_hours, p95_transit_hours)
      SELECT (delivered_at AT TIME ZONE 'UTC')::date,
             tenant_id,
             carrier,
             count(*),
             round(percentile_cont(0.50) WITHIN GROUP (ORDER BY transit_hours)::numeric, 2),
             round(percentile_cont(0.95) WITHIN GROUP (ORDER BY transit_hours)::numeric, 2)
      FROM (
        SELECT tenant_id, carrier, delivered_at,
               extract(epoch FROM delivered_at - label_created_at) / 3600.0 AS transit_hours
        FROM silver.shipments
        WHERE delivered_at IS NOT NULL AND label_created_at IS NOT NULL
      ) delivered
      GROUP BY 1, 2, 3`);
    await client.query('INSERT INTO gold.refresh_log (rows_written) VALUES ($1)', [inserted.rowCount ?? 0]);
    await client.query('COMMIT');
    return inserted.rowCount ?? 0;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}
