export const KNOWN_FAULTS = ['dup_events', 'ignore_order', 'skip_gold'] as const;
export type Fault = (typeof KNOWN_FAULTS)[number];

/**
 * Faults are deliberate defects injected into the pipeline. The canary CI job turns them on
 * and asserts that the data-quality suite catches every one of them ("testing the tests").
 */
export function parseFaults(raw: string | undefined): ReadonlySet<Fault> {
  const names = (raw ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  for (const name of names) {
    if (!(KNOWN_FAULTS as readonly string[]).includes(name)) {
      throw new Error(`Unknown fault "${name}". Known faults: ${KNOWN_FAULTS.join(', ')}`);
    }
  }
  return new Set(names as Fault[]);
}

export const config = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  topic: process.env.KAFKA_TOPIC ?? 'shipment-events',
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://lab:lab@localhost:55432/lake',
  apiUrl: process.env.API_URL ?? 'http://localhost:8088',
  httpPort: Number(process.env.HTTP_PORT ?? 8088),
  goldRefreshMs: Number(process.env.GOLD_REFRESH_MS ?? 2000),
  faults: parseFaults(process.env.FAULTS),
};
