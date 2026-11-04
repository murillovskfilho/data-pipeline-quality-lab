import { config } from '../../src/config';
import { pollUntil } from '../../src/support/poll';

/** Fail fast with a clear message instead of 30 confusing timeouts when the stack is down. */
export default async function globalSetup(): Promise<void> {
  await pollUntil(
    async () => (await fetch(`${config.apiUrl}/health`)).status,
    (status) => status === 200,
    { description: `pipeline health at ${config.apiUrl} (did you run "npm run up"?)`, timeoutMs: 60_000 },
  );
}
