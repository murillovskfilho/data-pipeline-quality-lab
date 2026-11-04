import { CARRIERS, type Carrier, type ShipmentEvent } from '../domain/events';

/** Small, fast, seedable PRNG. Same seed => same dataset, on every machine and every run. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const HOUR = 3_600_000;
const MINUTE = 60_000;
export const DEFAULT_BASE_DATE = new Date('2026-01-05T00:00:00Z');

export interface LifecycleSpec {
  tenantId: string;
  shipmentId: string;
  carrier: Carrier;
  labelCreatedAt: Date;
  /** Minutes between label creation and delivery. Must be > 3h so the steps stay ordered. */
  transitMinutes: number;
  delivered: boolean;
  serviceLevel?: string;
  originZip?: string;
  destinationZip?: string;
  weightOz?: number;
}

export interface SeededShipment {
  spec: LifecycleSpec;
  deliveredAt?: Date;
  events: ShipmentEvent[];
}

/** LABEL_CREATED -> IN_TRANSIT (+2h) -> OUT_FOR_DELIVERY (-3h) -> DELIVERED, in event-time order. */
export function buildLifecycle(spec: LifecycleSpec): SeededShipment {
  const at = (ms: number) => new Date(ms).toISOString();
  const base = {
    shipment_id: spec.shipmentId,
    tenant_id: spec.tenantId,
    carrier: spec.carrier,
  };
  const labelMs = spec.labelCreatedAt.getTime();
  const deliveredMs = labelMs + spec.transitMinutes * MINUTE;

  const events: ShipmentEvent[] = [
    {
      ...base,
      event_id: `${spec.shipmentId}:LABEL_CREATED`,
      event_type: 'LABEL_CREATED',
      occurred_at: at(labelMs),
      details: {
        service_level: spec.serviceLevel ?? 'GROUND',
        origin_zip: spec.originZip ?? '30301',
        destination_zip: spec.destinationZip ?? '94105',
        weight_oz: spec.weightOz ?? 32,
      },
    },
    { ...base, event_id: `${spec.shipmentId}:IN_TRANSIT`, event_type: 'IN_TRANSIT', occurred_at: at(labelMs + 2 * HOUR) },
  ];
  if (!spec.delivered) return { spec, events };

  events.push(
    {
      ...base,
      event_id: `${spec.shipmentId}:OUT_FOR_DELIVERY`,
      event_type: 'OUT_FOR_DELIVERY',
      occurred_at: at(deliveredMs - 3 * HOUR),
    },
    { ...base, event_id: `${spec.shipmentId}:DELIVERED`, event_type: 'DELIVERED', occurred_at: at(deliveredMs) },
  );
  return { spec, deliveredAt: new Date(deliveredMs), events };
}

export interface DatasetOptions {
  seed: number;
  tenantId: string;
  shipments: number;
  baseDate?: Date;
  /** Share of shipments still in transit (no delivery events). */
  undeliveredRatio?: number;
}

export function generateDataset(opts: DatasetOptions): SeededShipment[] {
  const rng = mulberry32(opts.seed);
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rng() * xs.length)];
  const zip = () => String(10000 + Math.floor(rng() * 89999));
  const base = (opts.baseDate ?? DEFAULT_BASE_DATE).getTime();

  return Array.from({ length: opts.shipments }, (_, i) =>
    buildLifecycle({
      tenantId: opts.tenantId,
      shipmentId: `${opts.tenantId}-SHP-${String(i + 1).padStart(5, '0')}`,
      carrier: pick(CARRIERS),
      labelCreatedAt: new Date(base + Math.floor(rng() * 72) * HOUR + Math.floor(rng() * 60) * MINUTE),
      transitMinutes: 12 * 60 + Math.floor(rng() * 108 * 60), // 12h .. 120h
      delivered: rng() >= (opts.undeliveredRatio ?? 0.15),
      serviceLevel: pick(['GROUND', '2DAY', 'OVERNIGHT']),
      originZip: zip(),
      destinationZip: zip(),
      weightOz: Math.round((1 + rng() * 159) * 10) / 10,
    }),
  );
}
