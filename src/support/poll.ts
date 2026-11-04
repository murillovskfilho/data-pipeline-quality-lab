export interface PollOptions {
  /** What we are waiting for, used in the timeout message. */
  description: string;
  timeoutMs?: number;
  intervalMs?: number;
  maxIntervalMs?: number;
}

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Polls `probe` until `isDone` accepts its value. Built for eventually consistent systems:
 * no fixed sleeps, exponential backoff, and a timeout error that carries the last observed
 * value so a failure explains itself without re-running.
 */
export async function pollUntil<T>(
  probe: () => Promise<T>,
  isDone: (value: T) => boolean,
  { description, timeoutMs = 30_000, intervalMs = 200, maxIntervalMs = 2_000 }: PollOptions,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T | undefined;
  let lastError: unknown;
  let wait = intervalMs;

  while (Date.now() < deadline) {
    try {
      last = await probe();
      lastError = undefined;
      if (isDone(last)) return last;
    } catch (err) {
      lastError = err;
    }
    await sleep(Math.min(wait, Math.max(0, deadline - Date.now())));
    wait = Math.min(wait * 1.5, maxIntervalMs);
  }

  const detail = lastError ? `Last error: ${String(lastError)}` : `Last value: ${JSON.stringify(last)}`;
  throw new Error(`Timed out after ${timeoutMs}ms waiting for ${description}. ${detail}`);
}
