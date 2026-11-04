import { readFileSync, readdirSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';
import type { Pool } from 'pg';

export type Severity = 'critical' | 'major' | 'minor';

export interface Check {
  name: string;
  severity: Severity;
  layer: string;
  description: string;
  sql: string;
}

export interface CheckResult {
  name: string;
  severity: Severity;
  layer: string;
  description: string;
  status: 'pass' | 'fail' | 'error';
  violations: number;
  sample: Record<string, unknown>[];
  durationMs: number;
  error?: string;
}

export const CHECKS_DIR = resolve(__dirname, '..', '..', 'checks');

/**
 * Each check is a plain .sql file that SELECTs the violating rows; zero rows means healthy.
 * Metadata lives in `-- key: value` header comments so analysts can add checks without code.
 */
export function loadChecks(dir = CHECKS_DIR): Check[] {
  return readdirSync(dir)
    .filter((f) => f.endsWith('.sql'))
    .sort()
    .map((file) => {
      const text = readFileSync(join(dir, file), 'utf8');
      const meta: Record<string, string> = {};
      for (const m of text.matchAll(/^--\s*(\w+):\s*(.+)$/gm)) meta[m[1]] = m[2].trim();
      const sql = text.replace(/;\s*$/, '').trim();
      const severity = (meta.severity ?? 'major') as Severity;
      if (!['critical', 'major', 'minor'].includes(severity)) throw new Error(`${file}: bad severity "${severity}"`);
      return {
        name: meta.name ?? basename(file, '.sql'),
        severity,
        layer: meta.layer ?? 'unknown',
        description: meta.description ?? '',
        sql,
      };
    });
}

export async function runCheck(pool: Pool, check: Check, sampleSize = 5): Promise<CheckResult> {
  const started = Date.now();
  const base = { name: check.name, severity: check.severity, layer: check.layer, description: check.description };
  try {
    const count = await pool.query<{ n: string }>(`SELECT count(*) AS n FROM (${check.sql}) v`);
    const violations = Number(count.rows[0].n);
    const sample = violations > 0 ? (await pool.query(`SELECT * FROM (${check.sql}) v LIMIT ${sampleSize}`)).rows : [];
    return { ...base, status: violations > 0 ? 'fail' : 'pass', violations, sample, durationMs: Date.now() - started };
  } catch (err) {
    return { ...base, status: 'error', violations: 0, sample: [], durationMs: Date.now() - started, error: String(err) };
  }
}
