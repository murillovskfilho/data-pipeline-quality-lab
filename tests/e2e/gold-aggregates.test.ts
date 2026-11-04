import { expectedGold } from '../../src/seed/expectations';
import { generateDataset } from '../../src/seed/generator';
import { pollUntil } from '../../src/support/poll';
import { uniqueTenant } from '../support/ids';
import { getGold } from '../support/lake';
import { kafka } from '../support/resources';

const byKey = <T extends { day: string; carrier: string }>(rows: T[]) =>
  [...rows].sort((a, b) => `${a.day}|${a.carrier}`.localeCompare(`${b.day}|${b.carrier}`));

describe('Gold layer: carrier daily performance', () => {
  it('matches an independent oracle computed from the seed', async () => {
    const tenant = uniqueTenant('gold');
    const shipments = generateDataset({ seed: 20260105, tenantId: tenant, shipments: 120 });
    const expected = byKey(expectedGold(shipments));
    const expectedDelivered = expected.reduce((n, r) => n + r.shipmentsDelivered, 0);

    await (await kafka()).publish(shipments.flatMap((s) => s.events));

    const actual = byKey(
      await pollUntil(
        () => getGold(tenant),
        (rows) => rows.reduce((n, r) => n + r.shipmentsDelivered, 0) === expectedDelivered,
        { description: `${expectedDelivered} delivered shipments in gold`, timeoutMs: 60_000 },
      ),
    );

    const counts = (rows: typeof actual) => rows.map(({ day, carrier, shipmentsDelivered }) => ({ day, carrier, shipmentsDelivered }));
    expect(counts(actual)).toEqual(counts(expected));

    // Both sides round to 2 decimals, so allow one hundredth of an hour of rounding drift.
    actual.forEach((row, i) => {
      expect(Math.abs(row.p50TransitHours - expected[i].p50TransitHours)).toBeLessThanOrEqual(0.011);
      expect(Math.abs(row.p95TransitHours - expected[i].p95TransitHours)).toBeLessThanOrEqual(0.011);
      expect(row.p50TransitHours).toBeLessThanOrEqual(row.p95TransitHours);
    });
  });
});
