import { config } from '../../src/config';
import { deliveredShipment, waitForShipment } from '../support/fixtures';
import { uniqueTenant } from '../support/ids';

const postEvents = (body: string) =>
  fetch(`${config.apiUrl}/events`, { method: 'POST', headers: { 'content-type': 'application/json' }, body });

describe('HTTP ingestion edge', () => {
  it('accepts a batch with 202 and the events reach silver', async () => {
    const shipment = deliveredShipment(uniqueTenant('http'));

    const res = await postEvents(JSON.stringify(shipment.events));

    expect(res.status).toBe(202);
    expect(await res.json()).toEqual({ accepted: 4 });
    await waitForShipment(shipment.spec.shipmentId, { status: 'DELIVERED', trackingEvents: 4 });
  });

  it('accepts a single event object', async () => {
    const shipment = deliveredShipment(uniqueTenant('http1'));

    const res = await postEvents(JSON.stringify(shipment.events[0]));

    expect(res.status).toBe(202);
    await waitForShipment(shipment.spec.shipmentId, { status: 'LABEL_CREATED', trackingEvents: 1 });
  });

  it.each([
    ['malformed JSON', '{"event_id": ', 'body is not valid JSON'],
    ['an empty batch', '[]', 'no events'],
    ['a batch with a non-object', '[{"a":1}, 42]', 'every event must be a JSON object'],
  ])('rejects %s with 400 before it reaches Kafka', async (_case, body, error) => {
    const res = await postEvents(body);

    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error });
  });

  it('returns 404 for unknown routes', async () => {
    const res = await fetch(`${config.apiUrl}/nope`);
    expect(res.status).toBe(404);
  });
});
