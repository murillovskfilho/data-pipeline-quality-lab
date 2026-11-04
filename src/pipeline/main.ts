import { Kafka, logLevel } from 'kafkajs';
import { Pool } from 'pg';
import { config } from '../config';
import { createPublisher } from '../kafka/publisher';
import { log } from '../support/log';
import { refreshGold } from './gold';
import { createHttpServer } from './http';
import { processMessage } from './processor';

async function main(): Promise<void> {
  const faults = config.faults;
  if (faults.size > 0) log('warn', 'pipeline started WITH INJECTED FAULTS', { faults: [...faults] });

  const pool = new Pool({ connectionString: config.databaseUrl, max: 10 });
  const kafka = new Kafka({ clientId: 'dq-lab-pipeline', brokers: config.kafkaBrokers, logLevel: logLevel.WARN });

  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: config.topic, numPartitions: 3 }], waitForLeaders: true });
  await admin.disconnect();

  let consumerReady = false;
  const consumer = kafka.consumer({ groupId: 'lake-pipeline' });
  consumer.on(consumer.events.GROUP_JOIN, () => {
    consumerReady = true;
    log('info', 'consumer joined group');
  });
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const result = await processMessage(
        pool,
        {
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString() ?? null,
          value: message.value?.toString() ?? null,
        },
        faults,
      );
      if (result.outcome === 'rejected') {
        log('warn', 'event quarantined', { partition, offset: message.offset, reason: result.reason });
      }
    },
  });

  let refreshing = false;
  const goldTimer = setInterval(async () => {
    // FAULT skip_gold: the aggregation job silently stops running.
    if (faults.has('skip_gold') || refreshing) return;
    refreshing = true;
    try {
      await refreshGold(pool);
    } catch (err) {
      log('error', 'gold refresh failed', { error: String(err) });
    } finally {
      refreshing = false;
    }
  }, config.goldRefreshMs);

  const publisher = await createPublisher('dq-lab-http');
  const server = createHttpServer(publisher, () => consumerReady);
  server.listen(config.httpPort, () => log('info', 'http listening', { port: config.httpPort }));

  const shutdown = async (signal: string) => {
    log('info', 'shutting down', { signal });
    clearInterval(goldTimer);
    server.close();
    await Promise.allSettled([consumer.disconnect(), publisher.close()]);
    await pool.end();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  log('error', 'pipeline crashed', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
