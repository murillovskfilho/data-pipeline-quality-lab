import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { Publisher } from '../kafka/publisher';

const MAX_BODY_BYTES = 1_000_000;

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

function send(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > MAX_BODY_BYTES) throw new HttpError(413, 'payload too large');
    chunks.push(chunk as Buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'body is not valid JSON');
  }
}

/**
 * HTTP ingestion edge: accepts one event or an array of events and forwards them to Kafka.
 * It only checks the envelope shape; business validation happens in the pipeline so that
 * every rejected event is still visible in bronze and in the quarantine table.
 */
export function createHttpServer(publisher: Publisher, isReady: () => boolean): Server {
  return createServer(async (req, res) => {
    try {
      if (req.method === 'GET' && req.url === '/health') {
        return isReady() ? send(res, 200, { status: 'ok' }) : send(res, 503, { status: 'starting' });
      }
      if (req.method === 'POST' && req.url === '/events') {
        const body = await readJson(req);
        const events = Array.isArray(body) ? body : [body];
        if (events.length === 0) throw new HttpError(400, 'no events');
        if (events.some((e) => typeof e !== 'object' || e === null || Array.isArray(e))) {
          throw new HttpError(400, 'every event must be a JSON object');
        }
        await publisher.publish(events);
        return send(res, 202, { accepted: events.length });
      }
      send(res, 404, { error: 'not found' });
    } catch (err) {
      if (err instanceof HttpError) return send(res, err.status, { error: err.message });
      send(res, 500, { error: 'internal error' });
    }
  });
}
