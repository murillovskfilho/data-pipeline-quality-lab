import type { SeededShipment } from './generator';

/** Same definition as PostgreSQL percentile_cont: linear interpolation between closest ranks. */
export function percentileCont(values: number[], p: number): number {
  if (values.length === 0) throw new Error('percentile of empty set');
  const sorted = [...values].sort((a, b) => a - b);
  const pos = p * (sorted.length - 1);
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

export interface GoldRow {
  day: string; // YYYY-MM-DD (UTC)
  carrier: string;
  shipmentsDelivered: number;
  p50TransitHours: number;
  p95TransitHours: number;
}

/** Independent oracle for the gold layer, computed from the seed instead of from the database. */
export function expectedGold(shipments: SeededShipment[]): GoldRow[] {
  const groups = new Map<string, { day: string; carrier: string; hours: number[] }>();
  for (const s of shipments) {
    if (!s.deliveredAt) continue;
    const day = s.deliveredAt.toISOString().slice(0, 10);
    const key = `${day}|${s.spec.carrier}`;
    const group = groups.get(key) ?? { day, carrier: s.spec.carrier, hours: [] };
    group.hours.push((s.deliveredAt.getTime() - s.spec.labelCreatedAt.getTime()) / 3_600_000);
    groups.set(key, group);
  }
  return [...groups.values()]
    .map((g) => ({
      day: g.day,
      carrier: g.carrier,
      shipmentsDelivered: g.hours.length,
      p50TransitHours: round2(percentileCont(g.hours, 0.5)),
      p95TransitHours: round2(percentileCont(g.hours, 0.95)),
    }))
    .sort((a, b) => (a.day + a.carrier).localeCompare(b.day + b.carrier));
}

const round2 = (n: number) => Math.round(n * 100) / 100;
