import Redis from 'ioredis';
import {loadConfig} from '../../src/config/config';
import {Worker} from '../../src/worker';
import {TEST_DATABASE_URL, TEST_REDIS_URL, uniqueId} from '../support/redis';

const stream = uniqueId('test:alert-stream');
const raw = new Redis(TEST_REDIS_URL);
let worker: Worker;

beforeEach(async () => {
  worker = Worker.create(loadConfig({DATABASE_URL: TEST_DATABASE_URL, REDIS_URL: TEST_REDIS_URL,
    ALERT_STREAM: stream, CONSUMER_GROUP: uniqueId('group'), HEALTH_PORT: '0', BLOCK_MS: '100'}));
  await worker.start();
});
afterEach(() => worker.stop());
afterAll(async () => {
  await raw.del(stream);
  raw.disconnect();
});

const get = (path: string) => fetch(`http://127.0.0.1:${worker.healthPort}${path}`);

test('livez is always 200', async () => {
  expect((await get('/livez')).status).toBe(200);
});

test('readyz is 200 after start and 503 after redis disconnect', async () => {
  const ready = await get('/readyz');
  expect(ready.status).toBe(200);
  expect(await ready.json()).toEqual({ready: true, checks: {redis: true, postgres: true, subscriber: true}});
  worker.redis.disconnect();
  const notReady = await get('/readyz');
  expect(notReady.status).toBe(503);
  expect((await notReady.json()).checks.redis).toBe(false);
});

test('unknown path is 404', async () => {
  expect((await get('/nope')).status).toBe(404);
});

test('stop() resolves and closes connections', async () => {
  await worker.stop();
  expect(worker.redis.status).toBe('end');
  await expect(get('/livez')).rejects.toThrow();
});
