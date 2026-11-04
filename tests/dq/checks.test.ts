import { loadChecks, runCheck } from '../../src/dq/checks';
import { db } from '../support/resources';

/**
 * The SQL checks as a Jest suite, for local feedback. CI uses scripts/dq-report.ts, which runs the
 * same checks and also publishes a report. Run after `npm run seed` (or the e2e suite) so there is data.
 */
describe('Data-quality checks', () => {
  it.each(loadChecks().map((c) => [c.name, c] as const))('%s', async (_name, check) => {
    const result = await runCheck(db(), check);

    if (result.status === 'error') throw new Error(`check errored: ${result.error}`);
    if (result.violations > 0) {
      throw new Error(
        `${result.violations} violation(s) [${result.severity}]: ${result.description}\n` +
          JSON.stringify(result.sample, null, 2),
      );
    }
  });
});
