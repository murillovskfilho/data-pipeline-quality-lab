/**
 * Canary: "who tests the tests?"
 * The pipeline was started with injected faults. Each fault must be caught by the check designed
 * for it. If a fault slips through, the DQ suite has a blind spot and this script fails the build.
 */
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { config, type Fault } from '../src/config';
import type { CheckResult } from '../src/dq/checks';

const EXPECTED_DETECTIONS: Record<Fault, string[]> = {
  dup_events: ['silver_duplicate_tracking_events'],
  ignore_order: ['silver_status_matches_latest_event'],
  skip_gold: ['gold_reconciles_with_silver', 'gold_freshness'],
};

const report = JSON.parse(readFileSync(resolve(__dirname, '..', 'reports', 'dq-report.json'), 'utf8')) as {
  results: CheckResult[];
};
const byName = new Map(report.results.map((r) => [r.name, r]));

if (config.faults.size === 0) {
  console.error('FAULTS is empty: the canary must run against a pipeline with injected faults.');
  process.exit(1);
}

let missed = 0;
for (const fault of config.faults) {
  for (const checkName of EXPECTED_DETECTIONS[fault]) {
    const result = byName.get(checkName);
    const caught = result?.status === 'fail';
    if (!caught) missed++;
    console.log(
      `${caught ? '✅ caught' : '❌ MISSED'}  fault=${fault.padEnd(13)} check=${checkName}` +
        (result ? ` (${result.violations} violations)` : ' (check not found)'),
    );
  }
}

const errored = report.results.filter((r) => r.status === 'error');
for (const r of errored) console.log(`⚠️ check errored: ${r.name}: ${r.error}`);

if (missed > 0 || errored.length > 0) process.exit(1);
console.log('\nEvery injected fault was detected by the data-quality suite.');
