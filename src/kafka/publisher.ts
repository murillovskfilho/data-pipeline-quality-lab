import { Kafka, logLevel, Partitioners, type Producer } from 'kafkajs';
import { config } from '../config';

export interface Publisher {
  /** Objects are JSON-encoded; strings are sent verbatim (useful for malformed payloads). */
  publish(messages: ReadonlyArray<unknown>): Promise<void>;
  close(): Promise<void>;
}

function keyOf(message: unknown): string | null {
  if (typeof message === 'object' && message !== null) {
    const id = (message as Record<string, unknown>).shipment_id;
    if (typeof id === 'string') return id;
  }
  return null;
}

export async function createPublisher(clientId = 'dq-lab-publisher'): Promise<Publisher> {
  const kafka = new Kafka({ clientId, brokers: config.kafkaBrokers, logLevel: logLevel.WARN });
  const producer: Producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    // One in-flight request keeps per-partition order intact even when a send is retried.
    maxInFlightRequests: 1,
    allowAutoTopicCreation: true,
  });
  await producer.connect();

  return {
    async publish(messages) {
      if (messages.length === 0) return;
      await producer.send({
        topic: config.topic,
        messages: messages.map((m) => ({
          // Keyed by shipment: all events of a shipment land on the same partition, in order.
          key: keyOf(m),
          value: typeof m === 'string' ? m : JSON.stringify(m),
        })),
      });
    },
    close: () => producer.disconnect(),
  };
}
