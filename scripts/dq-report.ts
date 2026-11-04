/**
 * Runs every SQL check and writes reports/dq-report.json + reports/dq-report.md.
 * In GitHub Actions the Markdown is also appended to the job summary.
 * Exits non-zero when a critical check fails or any check errors, unless --no-fail is passed.
 */
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { Pool } from 'pg';
import { config } from '../src/config';
import { loadChecks, runCheck, type CheckResult } from '../src/dq/checks';

const ICON: Record<CheckResult['status'], string> = { pass: '✅', fail: '❌', error: '⚠️' };

function toMarkdown(results: CheckResult[], generatedAt: string): string {
  const failed = results.filter((r) => r.status !== 'pass');
  const lines = [
    '# Data-quality report',
    '',
    `Generated ${generatedAt} — **${results.length - failed.length}/${results.length} checks passing**.`,
    '',
    '| Status | Check | Layer | Severity | Violations | Time |',
    '|---|---|---|---|---:|---:|',
    ...results.map(
      (r) =>
        `| ${ICON[r.status]} | \`${r.name}\` | ${r.layer} | ${r.severity} | ${r.violations} | ${r.durationMs} ms |`,
    ),
  ];
  for (const r of failed) {
    lines.push('', `## ${ICON[r.status]} ${r.name}`, '', r.description, '');
    if (r.error) lines.push('```', r.error, '```');
    else lines.push('Sample of violating rows:', '', '```json', JSON.stringify(r.sample, null, 2), '```');
  }
  return lines.join('\n') + '\n';
}

async function main(): Promise<void> {
  const { values } = parseArgs({ options: { 'no-fail': { type: 'boolean', default: false } } });
  const pool = new Pool({ connectionString: config.databaseUrl });
  const results: CheckResult[] = [];
  try {
    for (const check of loadChecks()) results.push(await runCheck(pool, check));
  } finally {
    await pool.end();
  }

  const generatedAt = new Date().toISOString();
  const outDir = resolve(__dirname, '..', 'reports');
  mkdirSync(outDir, { recursive: true });
  const markdown = toMarkdown(results, generatedAt);
  writeFileSync(resolve(outDir, 'dq-report.json'), JSON.stringify({ generatedAt, results }, null, 2));
  writeFileSync(resolve(outDir, 'dq-report.md'), markdown);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);

  for (const r of results) {
    console.log(`${ICON[r.status]} ${r.name.padEnd(40)} ${r.severity.padEnd(8)} ${String(r.violations).padStart(5)} violations`);
  }

  const blocking = results.filter((r) => r.status === 'error' || (r.status === 'fail' && r.severity === 'critical'));
  if (blocking.length > 0 && !values['no-fail']) {
    console.error(`\n${blocking.length} blocking check(s): ${blocking.map((r) => r.name).join(', ')}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
