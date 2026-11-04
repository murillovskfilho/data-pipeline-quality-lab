import { invalidCases } from '../../src/seed/invalid';
import { pollUntil } from '../../src/support/poll';
import { deliveredShipment, waitForShipment } from '../support/fixtures';
import { uniqueTenant } from '../support/ids';
import { getRejections, getRejectionsByRawValue, getShipment, getTrackingEvents } from '../support/lake';
import { kafka } from '../support/resources';

describe('Quarantine of invalid events', () => {
  const tenant = uniqueTenant('bad');
  const cases = invalidCases(tenant);

  beforeAll(async () => {
    await (await kafka()).publish(cases.map((c) => c.message));
  });

  it.each(cases.map((c) => [c.name, c] as const))('%s is quarantined with the right reason', async (_name, c) => {
    const shipmentId = c.shipmentId as string;
    const reasons = await pollUntil(() => getRejections(shipmentId), (r) => r.length > 0, {
      description: `rejection for ${shipmentId}`,
    });

    expect(reasons).toEqual([c.expectedReason]);
    expect(await getShipment(shipmentId)).toBeUndefined();
  });

  it('quarantines a message that is not JSON at all', async () => {
    const marker = uniqueTenant('garbage');
    await (await kafka()).publish([`{"marker": "${marker}", oops`]);

    const reasons = await pollUntil(() => getRejectionsByRawValue(marker), (r) => r.length > 0, {
      description: 'rejection of the malformed message',
    });
    expect(reasons).toEqual(['malformed_json']);
  });

  it("refuses an event that would attach to another tenant's shipment", async () => {
    const shipment = deliveredShipment(uniqueTenant('owner'));
    await (await kafka()).publish([shipment.events[0]]);
    await waitForShipment(shipment.spec.shipmentId, { status: 'LABEL_CREATED', trackingEvents: 1 });

    const intruder = { ...shipment.events[1], tenant_id: uniqueTenant('intruder') };
    await (await kafka()).publish([intruder]);

    const reasons = await pollUntil(() => getRejections(shipment.spec.shipmentId), (r) => r.length > 0, {
      description: 'tenant_mismatch rejection',
    });
    expect(reasons).toEqual(['tenant_mismatch']);
    expect(await getTrackingEvents(shipment.spec.shipmentId)).toHaveLength(1);
    expect((await getShipment(shipment.spec.shipmentId))?.status).toBe('LABEL_CREATED');
  });
});
