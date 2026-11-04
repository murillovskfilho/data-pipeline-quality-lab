export const CARRIERS = ['UPS', 'FEDEX', 'USPS', 'DHL', 'ONTRAC'] as const;
export type Carrier = (typeof CARRIERS)[number];

export const EVENT_TYPES = [
  'LABEL_CREATED',
  'IN_TRANSIT',
  'OUT_FOR_DELIVERY',
  'DELIVERED',
  'EXCEPTION',
] as const;
export type EventType = (typeof EVENT_TYPES)[number];

export interface ShipmentDetails {
  service_level?: string;
  origin_zip?: string;
  destination_zip?: string;
  weight_oz?: number;
}

export interface ShipmentEvent {
  event_id: string;
  event_type: EventType;
  shipment_id: string;
  tenant_id: string;
  carrier: Carrier;
  occurred_at: string; // ISO-8601
  details?: ShipmentDetails;
}

export type RejectionReason =
  | 'malformed_json'
  | 'not_an_object'
  | 'missing_event_id'
  | 'missing_shipment_id'
  | 'missing_tenant_id'
  | 'unknown_event_type'
  | 'unknown_carrier'
  | 'invalid_occurred_at'
  | 'invalid_weight'
  | 'invalid_zip'
  | 'tenant_mismatch';

export type Validation = { ok: true; event: ShipmentEvent } | { ok: false; reason: RejectionReason };

const isNonEmptyString = (v: unknown): v is string => typeof v === 'string' && v.trim().length > 0;
const ZIP = /^\d{5}$/;

export function validateEvent(raw: unknown): Validation {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    return { ok: false, reason: 'not_an_object' };
  }
  const e = raw as Record<string, unknown>;

  if (!isNonEmptyString(e.event_id)) return { ok: false, reason: 'missing_event_id' };
  if (!isNonEmptyString(e.shipment_id)) return { ok: false, reason: 'missing_shipment_id' };
  if (!isNonEmptyString(e.tenant_id)) return { ok: false, reason: 'missing_tenant_id' };
  if (!(EVENT_TYPES as readonly unknown[]).includes(e.event_type)) {
    return { ok: false, reason: 'unknown_event_type' };
  }
  if (!(CARRIERS as readonly unknown[]).includes(e.carrier)) return { ok: false, reason: 'unknown_carrier' };
  if (!isNonEmptyString(e.occurred_at) || Number.isNaN(Date.parse(e.occurred_at))) {
    return { ok: false, reason: 'invalid_occurred_at' };
  }

  const details = (e.details ?? {}) as Record<string, unknown>;
  if (details.weight_oz !== undefined) {
    const w = details.weight_oz;
    if (typeof w !== 'number' || !Number.isFinite(w) || w <= 0) return { ok: false, reason: 'invalid_weight' };
  }
  for (const key of ['origin_zip', 'destination_zip'] as const) {
    const zip = details[key];
    if (zip !== undefined && (typeof zip !== 'string' || !ZIP.test(zip))) {
      return { ok: false, reason: 'invalid_zip' };
    }
  }

  return { ok: true, event: e as unknown as ShipmentEvent };
}
