import { pollUntil } from '../../src/support/poll';
import { deliveredShipment, waitForShipment } from '../support/fixtures';
import { uniqueTenant } from '../support/ids';
import { countBronze, getTrackingEvents } from '../support/lake';
import { kafka } from '../support/resources';

describe('Idempotent ingestion (at-least-once producers)', () => {
  it('keeps every replay in bronze but only one copy in silver', async () => {
    const shipment = deliveredShipment(uniqueTenant('replay'));
    const publisher = await kafka();

    await publisher.publish(shipment.events);
    await publisher.publish(shipment.events); // full replay
    await publisher.publish([shipment.events[3]]); // and DELIVERED once more

    // Bronze is the audit trail: all 9 messages are kept...
    await pollUntil(() => countBronze(shipment.spec.shipmentId), (n) => n === 9, { description: '9 bronze rows' });
    // ...but silver promotes each business event exactly once.
    await waitForShipment(shipment.spec.shipmentId, { status: 'DELIVERED', trackingEvents: 4 });
    expect(await getTrackingEvents(shipment.spec.shipmentId)).toHaveLength(4);
  });
});
