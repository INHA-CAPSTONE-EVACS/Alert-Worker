import Redis from 'ioredis';
import {loadConfig} from '../../src/config/config';
import {gatewayChannel} from '../../src/domain/contracts';
import type {AlertState, GatewayFrame} from '../../src/domain/types';
import {createPool} from '../../src/infra/postgres';
import {PgAlertDeliveryLogRepository} from '../../src/store/alert-delivery-log-repository';
import {Worker} from '../../src/worker';
import {TEST_DATABASE_URL, TEST_REDIS_URL, uniqueId} from '../support/redis';

/**
 * Runs the whole worker against the sandbox Redis/Postgres and plays the Gateway's part of the contract:
 * it registers ws:vehicle sessions, subscribes to its channel and writes client ACKs to the stream.
 */
const stream = uniqueId('e2e:alert-stream');
const dlq = uniqueId('e2e:alert-dlq');
const gatewayId = uniqueId('gw-e2e');
const dispatchId = uniqueId('DSP');
const vehicle = uniqueId('EVACS_CAR');
const expiringVehicle = uniqueId('EVACS_CAR');
const offlineGatewayVehicle = uniqueId('EVACS_CAR');
const ambulance = uniqueId('AMB');
const alertId = uniqueId('ALT');

const redis = new Redis(TEST_REDIS_URL);
const channel = new Redis(TEST_REDIS_URL);
const pool = createPool(TEST_DATABASE_URL);
const logs = new PgAlertDeliveryLogRepository(pool);
const frames: {frame: GatewayFrame; at: number}[] = [];
let worker: Worker;

const iso = (offsetMs = 0) => new Date(Date.now() + offsetMs).toISOString();
const xadd = (eventType: string, payload: object, producer = 'emergency-worker') =>
  redis.xadd(stream, '*', 'eventKey', uniqueId('k'), 'event', JSON.stringify({eventId: uniqueId('evt'), eventType,
    schemaVersion: 1, occurredAt: iso(), producer, payload}));
const requested = (overrides: object = {}) => ({alertId, emergencyId: 'EMG-E2E', dispatchId,
  emergencyVehicleId: ambulance, targetVehicleId: vehicle, approachDirection: 'REAR', estimatedArrivalSeconds: 15,
  recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD', decisionVersion: 1, expiresAt: iso(60_000),
  ...overrides});
const alertState = async (id: string) =>
  JSON.parse(await redis.get(`alert:${id}:state`) ?? 'null') as AlertState | null;

async function waitFor<T>(probe: () => Promise<T | undefined> | T | undefined, timeoutMs = 3000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}
const nextFrame = (from: number, predicate: (frame: GatewayFrame) => boolean = () => true) =>
  waitFor(() => frames.slice(from).find(entry => predicate(entry.frame)));
const session = (vehicleId: string, gateway = gatewayId) => redis.set(`ws:vehicle:${vehicleId}`,
  JSON.stringify({vehicleId, gatewayId: gateway, sessionId: `s-${vehicleId}`, connectedAt: iso()}));

beforeAll(async () => {
  await channel.subscribe(gatewayChannel(gatewayId));
  channel.on('message', (_name, message) => frames.push({frame: JSON.parse(message) as GatewayFrame, at: Date.now()}));
  await Promise.all([session(vehicle), session(expiringVehicle), session(ambulance),
    session(offlineGatewayVehicle, uniqueId('gw-nobody'))]);
  worker = Worker.create(loadConfig({DATABASE_URL: TEST_DATABASE_URL, REDIS_URL: TEST_REDIS_URL,
    ALERT_STREAM: stream, DLQ_STREAM: dlq, CONSUMER_GROUP: uniqueId('e2e-group'), HEALTH_PORT: '0', BLOCK_MS: '200',
    EXPIRY_SWEEP_MS: '200'}));
  await worker.start();
});

afterAll(async () => {
  await worker.stop();
  const keys = await redis.keys(`*${dispatchId}*`);
  await redis.del(stream, dlq, ...keys, ...[vehicle, expiringVehicle, ambulance, offlineGatewayVehicle]
    .map(id => `ws:vehicle:${id}`));
  channel.disconnect();
  redis.disconnect();
  await pool.end();
});

test('yield alert lifecycle: issue, ack, suppress, update, clear', async () => {
  // 1. First request reaches the Gateway channel addressed to the vehicle's session.
  let mark = frames.length;
  const sentAt = Date.now();
  await xadd('YIELD_ALERT_REQUESTED', requested());
  const issued = await nextFrame(mark);
  console.log(`stream XADD -> gateway frame: ${issued.at - sentAt} ms`);
  expect(issued.frame).toMatchObject({targetVehicleId: vehicle, sessionId: `s-${vehicle}`,
    message: {type: 'YIELD_ALERT_ISSUED', alertId, alertVersion: 1, approachDirection: 'REAR',
      message: '후방에서 긴급차량이 접근하고 있습니다. 안전한 상황에서 양보해 주세요.'}});
  expect(await alertState(alertId)).toMatchObject({status: 'SENT', alertVersion: 1});
  const [row] = await waitFor(async () => {
    const rows = await logs.findByDispatchId(dispatchId);
    return rows.length ? rows : undefined;
  });
  expect(row).toMatchObject({alertId, alertStatus: 'SENT', targetVehicleId: vehicle});

  // 2. Client ACK relayed by the Gateway.
  const receivedAt = iso();
  await xadd('YIELD_ALERT_ACKNOWLEDGED', {alertId, vehicleId: vehicle, alertVersion: 1, receivedAt}, 'gateway');
  await waitFor(async () => (await alertState(alertId))?.status === 'ACKNOWLEDGED' || undefined);
  expect((await logs.findByDispatchId(dispatchId))[0]).toMatchObject({alertStatus: 'ACKNOWLEDGED',
    acknowledgedAt: receivedAt});

  // 3. Same guidance within 10 s is not repeated (F-ALT-04).
  mark = frames.length;
  await xadd('YIELD_ALERT_REQUESTED', requested({decisionVersion: 2, estimatedArrivalSeconds: 12}));
  await new Promise(resolve => setTimeout(resolve, 600));
  expect(frames.slice(mark)).toHaveLength(0);

  // 4. Changed approach direction is re-sent as an update in the same episode.
  await xadd('YIELD_ALERT_REQUESTED', requested({decisionVersion: 3, approachDirection: 'LEFT'}));
  const updated = await nextFrame(mark);
  expect(updated.frame.message).toMatchObject({type: 'YIELD_ALERT_ISSUED', alertVersion: 3,
    approachDirection: 'LEFT'});
  expect(await logs.findByDispatchId(dispatchId)).toHaveLength(1);

  // 5. Clear request ends the alert (F-ALT-05).
  mark = frames.length;
  await xadd('YIELD_ALERT_CLEAR_REQUESTED', {alertId, dispatchId, targetVehicleId: vehicle,
    reason: 'EMERGENCY_PASSED'});
  const cleared = await nextFrame(mark);
  expect(cleared.frame.message).toEqual({type: 'YIELD_ALERT_CLEARED', eventType: 'YIELD_ALERT_CLEARED', alertId,
    targetVehicleId: vehicle, reason: 'EMERGENCY_PASSED'});
  expect((await alertState(alertId))!.status).toBe('CLEARED');
  const [closed] = await waitFor(async () => {
    const rows = await logs.findByDispatchId(dispatchId);
    return rows[0]?.clearedAt ? rows : undefined;
  });
  expect(closed).toMatchObject({alertStatus: 'CLEARED', clearReason: 'EMERGENCY_PASSED'});
});

test('alert without a clear request expires and the client is told', async () => {
  const id = uniqueId('ALT');
  const mark = frames.length;
  await xadd('YIELD_ALERT_REQUESTED', requested({alertId: id, targetVehicleId: expiringVehicle,
    expiresAt: iso(1500)}));
  await nextFrame(mark, frame => frame.message.type === 'YIELD_ALERT_ISSUED');
  const expired = await nextFrame(mark, frame => frame.message.type === 'YIELD_ALERT_CLEARED');
  expect(expired.frame.message).toMatchObject({alertId: id, reason: 'EXPIRED'});
  expect((await alertState(id))!.status).toBe('EXPIRED');
});

test('route updates reach the emergency vehicle once, newest version only', async () => {
  const route = (routeVersion: number) => ({dispatchId, targetVehicleId: ambulance, routeVersion,
    destination: {latitude: 37.451, longitude: 126.654},
    waypoints: [{latitude: 37.449, longitude: 126.65}, {latitude: 37.451, longitude: 126.654}],
    congestedSegments: ['EDGE-101'], estimatedTravelSeconds: 180});
  const mark = frames.length;
  await xadd('ROUTE_UPDATE_REQUESTED', route(2));
  await xadd('ROUTE_UPDATE_REQUESTED', route(1));
  const delivered = await nextFrame(mark);
  await new Promise(resolve => setTimeout(resolve, 500));
  expect(frames.slice(mark)).toHaveLength(1);
  expect(delivered.frame).toMatchObject({targetVehicleId: ambulance,
    message: {type: 'EMERGENCY_ROUTE_UPDATED', routeVersion: 2, congestedSegments: ['EDGE-101']}});
});

test('alert to a gateway nobody listens on is recorded as FAILED', async () => {
  const id = uniqueId('ALT');
  await xadd('YIELD_ALERT_REQUESTED', requested({alertId: id, targetVehicleId: offlineGatewayVehicle}));
  const failed = await waitFor(async () => {
    const value = await alertState(id);
    return value?.status === 'FAILED' ? value : undefined;
  });
  expect(failed.lastSentAt).toBeNull();
});

test('nothing is left pending or dead-lettered', async () => {
  const groups = await redis.xinfo('GROUPS', stream) as unknown[][];
  const pending = groups.map(group => group[group.indexOf('pending') + 1]);
  expect(pending).toEqual([0]);
  expect(await redis.xlen(dlq)).toBe(0);
});
