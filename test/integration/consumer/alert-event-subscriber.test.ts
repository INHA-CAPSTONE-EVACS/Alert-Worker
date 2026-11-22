import Redis from 'ioredis';
import {loadConfig} from '../../../src/config/config';
import {AlertEventSubscriber} from '../../../src/consumer/alert-event-subscriber';
import type {AlertStreamEvent} from '../../../src/domain/contracts';
import {requested} from '../../support/fixtures';
import {TEST_DATABASE_URL, TEST_REDIS_URL, uniqueId} from '../../support/redis';

const redis = new Redis(TEST_REDIS_URL);
let stream: string;
let dlq: string;
let subscriber: AlertEventSubscriber | undefined;

const config = (overrides: Record<string, string> = {}) => loadConfig({DATABASE_URL: TEST_DATABASE_URL,
  ALERT_STREAM: stream, DLQ_STREAM: dlq, CONSUMER_GROUP: 'test-group', CONSUMER_NAME: 'c1', BLOCK_MS: '100',
  PENDING_MIN_IDLE_MS: '50', ...overrides});
const entry = (payload = requested()) => JSON.stringify({eventId: uniqueId('e'), eventType: 'YIELD_ALERT_REQUESTED',
  schemaVersion: 1, occurredAt: '2026-10-12T08:00:00.000Z', producer: 'emergency-worker', payload});
const pendingCount = async () => (await redis.xpending(stream, 'test-group') as [number])[0];
const waitFor = async (condition: () => Promise<boolean>, timeoutMs = 3000) => {
  const deadline = Date.now() + timeoutMs;
  while (!await condition()) {
    if (Date.now() > deadline) throw new Error('condition not met');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

beforeEach(() => {
  stream = uniqueId('test:alert-stream');
  dlq = uniqueId('test:alert-dlq');
});
afterEach(async () => {
  await subscriber?.stop();
  subscriber = undefined;
  await redis.del(stream, dlq);
});
afterAll(() => redis.disconnect());

test('valid entry is routed then XACKed', async () => {
  const seen: AlertStreamEvent[] = [];
  subscriber = new AlertEventSubscriber(redis, config(), {route: async event => { seen.push(event); }});
  await subscriber.start();
  expect(subscriber.ready).toBe(true);
  await redis.xadd(stream, '*', 'eventKey', 'k', 'event', entry());
  await waitFor(async () => seen.length === 1);
  await waitFor(async () => await pendingCount() === 0);
  expect(seen[0]!.payload).toEqual(requested());
});

test('entries written before the group existed are not replayed', async () => {
  await redis.xadd(stream, '*', 'event', entry());
  const seen: AlertStreamEvent[] = [];
  subscriber = new AlertEventSubscriber(redis, config(), {route: async event => { seen.push(event); }});
  await subscriber.start();
  await new Promise(resolve => setTimeout(resolve, 300));
  expect(seen).toHaveLength(0);
});

test('malformed entry goes to DLQ and is acked', async () => {
  subscriber = new AlertEventSubscriber(redis, config(), {route: async () => undefined});
  await subscriber.start();
  await redis.xadd(stream, '*', 'event', '{not json');
  await waitFor(async () => await redis.xlen(dlq) === 1);
  const [[, fields]] = await redis.xrange(dlq, '-', '+') as [string, string[]][] as [[string, string[]]];
  expect(fields).toEqual(expect.arrayContaining(['reason', 'Invalid event JSON', 'event', '{not json']));
  expect(await pendingCount()).toBe(0);
});

test('router throw leaves entry pending, reclaimed after idle', async () => {
  let attempts = 0;
  subscriber = new AlertEventSubscriber(redis, config(), {route: async () => {
    attempts++;
    if (attempts === 1) throw new Error('redis down');
  }});
  await subscriber.start();
  await redis.xadd(stream, '*', 'event', entry());
  await waitFor(async () => attempts === 2);
  await waitFor(async () => await pendingCount() === 0);
  expect(await redis.xlen(dlq)).toBe(0);
});

test('entry exceeding maxDeliveries goes to DLQ', async () => {
  subscriber = new AlertEventSubscriber(redis, config({MAX_DELIVERIES: '2'}), {route: async () => {
    throw new Error('always fails');
  }});
  await subscriber.start();
  await redis.xadd(stream, '*', 'event', entry());
  await waitFor(async () => await redis.xlen(dlq) === 1);
  await waitFor(async () => await pendingCount() === 0);
});

test('stop() returns within blockMs + 1s', async () => {
  subscriber = new AlertEventSubscriber(redis, config({BLOCK_MS: '5000'}), {route: async () => undefined});
  await subscriber.start();
  const started = Date.now();
  await subscriber.stop();
  expect(Date.now() - started).toBeLessThan(6000);
  expect(subscriber.ready).toBe(false);
});
