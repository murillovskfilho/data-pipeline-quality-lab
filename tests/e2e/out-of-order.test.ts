import { deliveredShipment, waitForShipment } from '../support/fixtures';
import { uniqueTenant } from '../support/ids';
import { kafka } from '../support/resources';

describe('Out-of-order events', () => {
  it('does not roll a delivered shipment back when an older event arrives late', async () => {
    const shipment = deliveredShipment(uniqueTenant('ooo'));
    const [label, inTransit, outForDelivery, delivered] = shipment.events;

    await (await kafka()).publish([label, inTransit, delivered, outForDelivery]);

    const { shipment: row } = await waitForShipment(shipment.spec.shipmentId, { status: 'DELIVERED', trackingEvents: 4 });
    expect(row.last_event_at.toISOString()).toBe(delivered.occurred_at);
    expect(row.delivered_at?.toISOString()).toBe(delivered.occurred_at);
  });

  it('fills label data when LABEL_CREATED arrives after delivery', async () => {
    const shipment = deliveredShipment(uniqueTenant('latelabel'));
    const [label, ...rest] = shipment.events;

    await (await kafka()).publish([...rest, label]);

    const { shipment: row } = await waitForShipment(shipment.spec.shipmentId, { status: 'DELIVERED', trackingEvents: 4 });
    expect(row.label_created_at?.toISOString()).toBe(label.occurred_at);
    expect(row.weight_oz).toBe('32.00');
  });
});
