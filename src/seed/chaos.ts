import type { ShipmentEvent } from '../domain/events';
import { mulberry32, type SeededShipment } from './generator';

export interface ChaosOptions {
  seed: number;
  /** Probability that an event is published twice (at-least-once producers do this). */
  replayRatio: number;
  /** Probability that DELIVERED reaches Kafka before OUT_FOR_DELIVERY. */
  outOfOrderRatio: number;
}

export interface ChaosStats {
  replays: number;
  outOfOrder: number;
}

/** Turns clean lifecycles into realistic, messy publish order — deterministically. */
export function applyChaos(
  shipments: SeededShipment[],
  opts: ChaosOptions,
): { messages: ShipmentEvent[]; stats: ChaosStats } {
  const rng = mulberry32(opts.seed ^ 0x9e3779b9);
  const messages: ShipmentEvent[] = [];
  const stats: ChaosStats = { replays: 0, outOfOrder: 0 };

  for (const s of shipments) {
    const events = [...s.events];
    const n = events.length;
    if (n >= 4 && events[n - 1].event_type === 'DELIVERED' && rng() < opts.outOfOrderRatio) {
      [events[n - 2], events[n - 1]] = [events[n - 1], events[n - 2]];
      stats.outOfOrder++;
    }
    const replays = events.filter(() => rng() < opts.replayRatio);
    stats.replays += replays.length;
    messages.push(...events, ...replays);
  }
  return { messages, stats };
}
