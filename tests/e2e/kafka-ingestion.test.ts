import { deliveredShipment, waitForShipment } from '../support/fixtures';
import { uniqueTenant } from '../support/ids';
import { countBronze } from '../support/lake';
import { kafka } from '../support/resources';

describe('Kafka ingestion: bronze -> silver', () => {
  it('promotes a full shipment lifecycle published to Kafka', async () => {
    const tenant = uniqueTenant('kafka');
    const shipment = deliveredShipment(tenant);

    await (await kafka()).publish(shipment.events);

    const { shipment: row, events } = await waitForShipment(shipment.spec.shipmentId, {
      status: 'DELIVERED',
      trackingEvents: 4,
    });
    expect(events.map((e) => e.event_type)).toEqual(['LABEL_CREATED', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED']);
    expect(row).toMatchObject({ tenant_id: tenant, carrier: 'UPS', weight_oz: '32.00' });
    expect(row.label_created_at?.toISOString()).toBe('2026-01-05T08:00:00.000Z');
    expect(row.delivered_at?.toISOString()).toBe('2026-01-06T14:00:00.000Z');
    expect(await countBronze(shipment.spec.shipmentId)).toBe(4);
  });

  it('keeps an undelivered shipment in transit without delivery timestamps', async () => {
    const shipment = deliveredShipment(uniqueTenant('transit'));
    const [label, inTransit] = shipment.events;

    await (await kafka()).publish([label, inTransit]);

    const { shipment: row } = await waitForShipment(shipment.spec.shipmentId, { status: 'IN_TRANSIT', trackingEvents: 2 });
    expect(row.delivered_at).toBeNull();
  });
});
