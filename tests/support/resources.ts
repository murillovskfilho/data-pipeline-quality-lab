import { Pool } from 'pg';
import { config } from '../../src/config';
import { createPublisher, type Publisher } from '../../src/kafka/publisher';

// One pool and one producer per Jest worker, created lazily and closed in teardown.ts.
let pool: Pool | undefined;
let publisher: Promise<Publisher> | undefined;

export const db = (): Pool => (pool ??= new Pool({ connectionString: config.databaseUrl, max: 4 }));
export const kafka = (): Promise<Publisher> => (publisher ??= createPublisher(`dq-lab-tests-${process.pid}`));

export async function closeAll(): Promise<void> {
  await Promise.allSettled([publisher?.then((p) => p.close()), pool?.end()]);
  pool = undefined;
  publisher = undefined;
}
