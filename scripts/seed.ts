/**
 * Publishes a deterministic, intentionally messy dataset (replays, out-of-order events, invalid
 * messages) and waits until the pipeline has absorbed it. Re-running with the same seed is safe:
 * every event is a replay, which is exactly what idempotent ingestion must tolerate.
 *
 *   npm run seed -- --seed 42 --shipments 300 --replay-ratio 0.05 --out-of-order-ratio 0.1
 */
import { parseArgs } from 'node:util';
import { Pool } from 'pg';
import { config } from '../src/config';
import { createPublisher } from '../src/kafka/publisher';
import { applyChaos } from '../src/seed/chaos';
import { generateDataset } from '../src/seed/generator';
import { invalidCases } from '../src/seed/invalid';
import { pollUntil } from '../src/support/poll';

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      seed: { type: 'string', default: '42' },
      shipments: { type: 'string', default: '300' },
      tenant: { type: 'string' },
      'replay-ratio': { type: 'string', default: '0.05' },
      'out-of-order-ratio': { type: 'string', default: '0.1' },
      'skip-invalid': { type: 'boolean', default: false },
    },
  });
  const seed = Number(values.seed);
  const tenantId = values.tenant ?? `seed${seed}`;

  const shipments = generateDataset({ seed, tenantId, shipments: Number(values.shipments) });
  const { messages, stats } = applyChaos(shipments, {
    seed,
    replayRatio: Number(values['replay-ratio']),
    outOfOrderRatio: Number(values['out-of-order-ratio']),
  });
  const invalid = values['skip-invalid'] ? [] : invalidCases(tenantId).map((c) => c.message);
  const uniqueEvents = shipments.reduce((n, s) => n + s.events.length, 0);

  const publisher = await createPublisher('dq-lab-seed');
  await publisher.publish([...messages, ...invalid, '{"this is": not json']);
  await publisher.close();

  console.log(
    `Published ${messages.length + invalid.length + 1} messages for tenant "${tenantId}": ` +
      `${uniqueEvents} unique events, ${stats.replays} replays, ${stats.outOfOrder} out-of-order, ` +
      `${invalid.length + 1} invalid.`,
  );

  const pool = new Pool({ connectionString: config.databaseUrl });
  try {
    await pollUntil(
      async () =>
        Number(
          (await pool.query('SELECT count(*) AS n FROM silver.tracking_events WHERE tenant_id = $1', [tenantId])).rows[0].n,
        ),
      (n) => n >= uniqueEvents,
      { description: `${uniqueEvents} silver tracking events for ${tenantId}`, timeoutMs: 120_000 },
    );
    console.log('Silver caught up.');

    // Soft wait: a healthy pipeline refreshes gold within seconds; a broken one is the DQ suite's job to flag.
    // Compares refresh ids rather than timestamps, so host/container clock skew cannot matter.
    const lastRefreshId = async () =>
      Number((await pool.query('SELECT COALESCE(max(id), 0) AS id FROM gold.refresh_log')).rows[0].id);
    const caughtUpAt = await lastRefreshId();
    await pollUntil(lastRefreshId, (id) => id > caughtUpAt, {
      description: 'a gold refresh after silver caught up',
      timeoutMs: 15_000,
    })
      .then(() => console.log('Gold refreshed.'))
      .catch(() => console.warn('WARNING: gold has not refreshed since the seed was published.'));
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
