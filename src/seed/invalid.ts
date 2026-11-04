import type { RejectionReason } from '../domain/events';

export interface InvalidCase {
  name: string;
  /** Shipment id used to find the message in bronze (null when the payload has none). */
  shipmentId: string | null;
  message: unknown;
  expectedReason: RejectionReason;
}

/** One message per rejection rule. Each must end up in quarantine with exactly this reason. */
export function invalidCases(tenantId: string): InvalidCase[] {
  const id = (suffix: string) => `${tenantId}-BAD-${suffix}`;
  const valid = (suffix: string) => ({
    event_id: `${id(suffix)}:LABEL_CREATED`,
    event_type: 'LABEL_CREATED',
    shipment_id: id(suffix),
    tenant_id: tenantId,
    carrier: 'UPS',
    occurred_at: '2026-01-05T10:00:00.000Z',
    details: { weight_oz: 10, origin_zip: '30301', destination_zip: '94105' },
  });

  return [
    { name: 'unknown carrier', shipmentId: id('CARRIER'), message: { ...valid('CARRIER'), carrier: 'PIGEON' }, expectedReason: 'unknown_carrier' },
    { name: 'unknown event type', shipmentId: id('TYPE'), message: { ...valid('TYPE'), event_type: 'TELEPORTED' }, expectedReason: 'unknown_event_type' },
    { name: 'unparseable timestamp', shipmentId: id('TS'), message: { ...valid('TS'), occurred_at: 'yesterday-ish' }, expectedReason: 'invalid_occurred_at' },
    { name: 'negative weight', shipmentId: id('WEIGHT'), message: { ...valid('WEIGHT'), details: { weight_oz: -5 } }, expectedReason: 'invalid_weight' },
    { name: 'malformed zip', shipmentId: id('ZIP'), message: { ...valid('ZIP'), details: { origin_zip: '3030' } }, expectedReason: 'invalid_zip' },
    { name: 'missing tenant', shipmentId: id('TENANT'), message: { ...valid('TENANT'), tenant_id: '' }, expectedReason: 'missing_tenant_id' },
    { name: 'missing event id', shipmentId: id('EVID'), message: { ...valid('EVID'), event_id: undefined }, expectedReason: 'missing_event_id' },
  ];
}
