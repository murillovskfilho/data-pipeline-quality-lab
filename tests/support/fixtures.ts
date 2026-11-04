import { buildLifecycle, type SeededShipment } from '../../src/seed/generator';
import { pollUntil } from '../../src/support/poll';
import { getShipment, getTrackingEvents, type ShipmentRow } from './lake';

export function deliveredShipment(tenantId: string, suffix = '1'): SeededShipment {
  return buildLifecycle({
    tenantId,
    shipmentId: `${tenantId}-SHP-${suffix}`,
    carrier: 'UPS',
    labelCreatedAt: new Date('2026-01-05T08:00:00Z'),
    transitMinutes: 30 * 60,
    delivered: true,
  });
}

interface Landed {
  shipment: ShipmentRow;
  events: { event_id: string; event_type: string }[];
}

/** Waits until silver holds the shipment with the given status and number of tracking events. */
export async function waitForShipment(
  shipmentId: string,
  expected: { status: string; trackingEvents: number },
): Promise<Landed> {
  const landed = await pollUntil(
    async () => ({ shipment: await getShipment(shipmentId), events: await getTrackingEvents(shipmentId) }),
    ({ shipment, events }) => shipment?.status === expected.status && events.length === expected.trackingEvents,
    { description: `${shipmentId} to be ${expected.status} with ${expected.trackingEvents} tracking events` },
  );
  return landed as Landed;
}
