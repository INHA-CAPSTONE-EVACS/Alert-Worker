import Redis from 'ioredis';
import {AlertDeduplicator} from '../../../src/alert/alert-deduplicator';
import {RedisPubSubDeliveryPublisher} from '../../../src/delivery/redis-pubsub-delivery-publisher';
import {RedisSessionLocator} from '../../../src/delivery/session-locator';
import {ALERT_EXPIRY_KEY, activeAlertKey, alertStateKey, gatewayChannel} from '../../../src/domain/contracts';
import {AlertStateRepository} from '../../../src/store/alert-state-repository';
import {RedisRouteVersionStore} from '../../../src/store/route-version-store';
import {frame, state} from '../../support/fixtures';
import {TEST_REDIS_URL, uniqueId, uniqueRedis} from '../../support/redis';

const {redis, close} = uniqueRedis();
afterAll(close);

describe('AlertStateRepository', () => {
  let repo: AlertStateRepository;
  let alertId: string;
  const future = () => new Date(Date.now() + 20_000).toISOString();
  beforeEach(() => {
    repo = new AlertStateRepository(redis);
    alertId = uniqueId('ALT');
  });

  test('save keeps higher version', async () => {
    expect(await repo.save(state({alertId, alertVersion: 4, expiresAt: future()}))).toBe(true);
    expect(await repo.save(state({alertId, alertVersion: 3, expiresAt: future()}))).toBe(false);
    expect((await repo.findById(alertId))!.alertVersion).toBe(4);
  });

  test('save sets a ttl beyond expiresAt', async () => {
    await repo.save(state({alertId, expiresAt: future()}));
    const ttl = await redis.pttl(alertStateKey(alertId));
    expect(ttl).toBeGreaterThan(20_000);
    expect(ttl).toBeLessThanOrEqual(20_000 + 300_000);
  });

  test('findActive resolves through dispatch/vehicle index', async () => {
    await repo.save(state({alertId, dispatchId: 'D-1', targetVehicleId: 'V-1', expiresAt: future()}));
    expect((await repo.findActive('D-1', 'V-1'))!.alertId).toBe(alertId);
    expect(await repo.findActive('D-1', 'V-2')).toBeNull();
  });

  test('transition applies patch when status and version match', async () => {
    await repo.save(state({alertId, expiresAt: future()}));
    const next = await repo.transition(alertId, ['SENT'], 'ACKNOWLEDGED', {acknowledgedAt: 'x'}, 3);
    expect(next).toMatchObject({status: 'ACKNOWLEDGED', acknowledgedAt: 'x', alertVersion: 3});
    expect(await repo.findById(alertId)).toEqual(next);
  });

  test('transition rejects unexpected status', async () => {
    await repo.save(state({alertId, status: 'CLEARED', expiresAt: future()}));
    expect(await repo.transition(alertId, ['SENT', 'ACKNOWLEDGED'], 'EXPIRED', {})).toBeNull();
    expect((await repo.findById(alertId))!.status).toBe('CLEARED');
  });

  test('transition with expectedVersion mismatch returns null', async () => {
    await repo.save(state({alertId, alertVersion: 4, expiresAt: future()}));
    expect(await repo.transition(alertId, ['SENT'], 'ACKNOWLEDGED', {}, 3)).toBeNull();
  });

  test('transition of unknown alert returns null', async () => {
    expect(await repo.transition(alertId, ['SENT'], 'CLEARED', {})).toBeNull();
  });

  test('terminal transition removes from expiry index and active index', async () => {
    await repo.save(state({alertId, dispatchId: 'D-2', targetVehicleId: 'V-2', expiresAt: future()}));
    expect(await redis.zscore(ALERT_EXPIRY_KEY, alertId)).not.toBeNull();
    await repo.transition(alertId, ['SENT'], 'CLEARED', {clearedAt: 'y'});
    expect(await redis.zscore(ALERT_EXPIRY_KEY, alertId)).toBeNull();
    expect(await redis.get(activeAlertKey('D-2', 'V-2'))).toBeNull();
    expect(await repo.findActive('D-2', 'V-2')).toBeNull();
    expect((await repo.findById(alertId))!.status).toBe('CLEARED');
  });

  test('saving a FAILED state is not scheduled for expiry', async () => {
    await repo.save(state({alertId, status: 'FAILED', expiresAt: future()}));
    expect(await redis.zscore(ALERT_EXPIRY_KEY, alertId)).toBeNull();
  });

  test('dueForExpiry returns only alerts whose expiresAt <= now', async () => {
    const due = uniqueId('ALT');
    const now = Date.now();
    await repo.save(state({alertId: due, expiresAt: new Date(now - 1).toISOString()}));
    await repo.save(state({alertId, expiresAt: new Date(now + 60_000).toISOString()}));
    const ids = await repo.dueForExpiry(now, 100);
    expect(ids).toContain(due);
    expect(ids).not.toContain(alertId);
  });
});

describe('AlertDeduplicator', () => {
  test('suppresses same fingerprint within ttl, allows different fingerprint, forget resets', async () => {
    const dedup = new AlertDeduplicator(redis, 10_000);
    const dispatchId = uniqueId('D');
    expect(await dedup.shouldSend(dispatchId, 'V', 'A|REAR')).toBe(true);
    await dedup.remember(dispatchId, 'V', 'A|REAR');
    expect(await dedup.shouldSend(dispatchId, 'V', 'A|REAR')).toBe(false);
    expect(await dedup.shouldSend(dispatchId, 'V', 'A|LEFT')).toBe(true);
    await dedup.forget(dispatchId, 'V');
    expect(await dedup.shouldSend(dispatchId, 'V', 'A|REAR')).toBe(true);
  });

  test('record expires after ttl', async () => {
    const dedup = new AlertDeduplicator(redis, 50);
    const dispatchId = uniqueId('D');
    await dedup.remember(dispatchId, 'V', 'A|REAR');
    await new Promise(resolve => setTimeout(resolve, 120));
    expect(await dedup.shouldSend(dispatchId, 'V', 'A|REAR')).toBe(true);
  });
});

describe('RedisSessionLocator', () => {
  const locator = new RedisSessionLocator(redis);

  test('returns the gateway session', async () => {
    const session = {vehicleId: 'V-OK', gatewayId: 'gw-1', sessionId: 's-1', connectedAt: '2026-10-12T08:00:00.000Z'};
    await redis.set('ws:vehicle:V-OK', JSON.stringify(session));
    expect(await locator.findGateway('V-OK')).toEqual(session);
  });

  test.each([
    ['missing', undefined],
    ['malformed json', '{'],
    ['blank gatewayId', JSON.stringify({vehicleId: 'V', gatewayId: '', sessionId: 's'})],
    ['missing sessionId', JSON.stringify({vehicleId: 'V', gatewayId: 'gw'})],
  ])('returns null for %s ws:vehicle', async (_, value) => {
    const vehicleId = uniqueId('V');
    if (value !== undefined) await redis.set(`ws:vehicle:${vehicleId}`, value);
    expect(await locator.findGateway(vehicleId)).toBeNull();
  });
});

describe('RedisPubSubDeliveryPublisher', () => {
  const publisher = new RedisPubSubDeliveryPublisher(redis);

  test('publish reports zero receivers', async () => {
    expect(await publisher.publish(uniqueId('gw'), frame())).toBe(0);
  });

  test('publish delivers frame JSON to subscribed channel', async () => {
    const gatewayId = uniqueId('gw');
    const subscriber = new Redis(TEST_REDIS_URL);
    try {
      await subscriber.subscribe(gatewayChannel(gatewayId));
      const received = new Promise<string>(resolve => subscriber.on('message', (_channel, message) => resolve(message)));
      expect(await publisher.publish(gatewayId, frame())).toBe(1);
      expect(JSON.parse(await received)).toEqual(frame());
    } finally {
      subscriber.disconnect();
    }
  });
});

describe('RedisRouteVersionStore', () => {
  test('route version store only advances', async () => {
    const routes = new RedisRouteVersionStore(redis);
    const dispatchId = uniqueId('D');
    expect(await routes.isLatest(dispatchId, 1)).toBe(true);
    await routes.markDelivered(dispatchId, 3);
    expect(await routes.isLatest(dispatchId, 3)).toBe(false);
    expect(await routes.isLatest(dispatchId, 2)).toBe(false);
    await routes.markDelivered(dispatchId, 2);
    expect(await routes.isLatest(dispatchId, 4)).toBe(true);
    expect(await routes.isLatest(dispatchId, 3)).toBe(false);
  });
});
