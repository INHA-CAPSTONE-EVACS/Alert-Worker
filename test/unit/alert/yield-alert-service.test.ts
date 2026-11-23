import {AlertMessageFactory} from '../../../src/alert/alert-message-factory';
import {YieldAlertService} from '../../../src/alert/yield-alert-service';
import {AlertDeliverer} from '../../../src/delivery/alert-deliverer';
import {NOW, meta, requested} from '../../support/fixtures';
import {FakeDedup, FakeLogs, FakePublisher, FakeSessions, FakeStates} from '../../support/fakes';

let states: FakeStates;
let dedup: FakeDedup;
let sessions: FakeSessions;
let publisher: FakePublisher;
let logs: FakeLogs;
let svc: YieldAlertService;
let now: string;

beforeEach(() => {
  now = NOW;
  states = new FakeStates();
  dedup = new FakeDedup();
  sessions = new FakeSessions();
  publisher = new FakePublisher();
  logs = new FakeLogs();
  svc = new YieldAlertService({states, dedup, logs, factory: new AlertMessageFactory(),
    deliverer: new AlertDeliverer(sessions, publisher), clock: () => new Date(now)});
  sessions.connect('VEH-001');
});

const clear = {alertId: 'ALT-001', dispatchId: 'DSP-001', targetVehicleId: 'VEH-001',
  reason: 'EMERGENCY_PASSED' as const};

test('first request publishes ISSUED to session gateway and records SENT', async () => {
  expect(await svc.handleRequested(requested())).toEqual({outcome: 'SENT', alertId: 'ALT-001'});
  expect(publisher.frames).toEqual([{gatewayId: 'gw-1', frame: {targetVehicleId: 'VEH-001', sessionId: 's-1',
    message: expect.objectContaining({type: 'YIELD_ALERT_ISSUED', alertId: 'ALT-001', alertVersion: 3})}}]);
  expect(states.get('ALT-001')).toMatchObject({status: 'SENT', alertVersion: 3, lastSentAt: NOW,
    dispatchId: 'DSP-001', targetVehicleId: 'VEH-001'});
  const [row] = [...logs.rows.values()];
  expect(row).toMatchObject({alertLogId: states.get('ALT-001')!.alertLogId, alertStatus: 'SENT', alertVersion: 3,
    selectionReason: 'SAME_ROUTE_AHEAD', estimatedArrivalSeconds: 15, firstSentAt: NOW});
});

test('no session marks FAILED and logs it', async () => {
  sessions.sessions.clear();
  expect((await svc.handleRequested(requested())).outcome).toBe('NO_SESSION');
  expect(publisher.frames).toHaveLength(0);
  expect(states.get('ALT-001')!.status).toBe('FAILED');
  expect([...logs.rows.values()][0]).toMatchObject({alertStatus: 'FAILED', firstSentAt: null});
});

test('no gateway subscriber marks FAILED', async () => {
  publisher.receivers = 0;
  expect((await svc.handleRequested(requested())).outcome).toBe('NO_SUBSCRIBER');
  expect(states.get('ALT-001')!.status).toBe('FAILED');
  expect([...logs.rows.values()][0]!.alertStatus).toBe('FAILED');
});

test('stale request is dropped', async () => {
  await svc.handleRequested(requested({decisionVersion: 4}));
  expect((await svc.handleRequested(requested({decisionVersion: 3, approachDirection: 'LEFT'}))).outcome)
    .toBe('STALE');
  expect(publisher.frames).toHaveLength(1);
  expect(states.get('ALT-001')!.alertVersion).toBe(4);
});

test('redelivered request does not publish twice', async () => {
  await svc.handleRequested(requested());
  expect((await svc.handleRequested(requested())).outcome).toBe('DUPLICATE');
  expect(publisher.frames).toHaveLength(1);
  expect(logs.insertCalls).toBe(2);
  expect(logs.rows.size).toBe(1);
});

test('same action and direction within dedup window is SUPPRESSED', async () => {
  await svc.handleRequested(requested());
  expect((await svc.handleRequested(requested({decisionVersion: 4, estimatedArrivalSeconds: 12}))).outcome)
    .toBe('SUPPRESSED');
  expect(publisher.frames).toHaveLength(1);
  expect(states.get('ALT-001')!.alertVersion).toBe(3);
});

test('changed approachDirection re-sends as update in same episode', async () => {
  await svc.handleRequested(requested());
  const logId = states.get('ALT-001')!.alertLogId;
  expect((await svc.handleRequested(requested({decisionVersion: 4, approachDirection: 'REAR_LEFT',
    estimatedArrivalSeconds: 9}))).outcome).toBe('SENT');
  expect(publisher.frames).toHaveLength(2);
  expect(publisher.frames[1]!.frame.message).toMatchObject({alertVersion: 4, approachDirection: 'REAR_LEFT'});
  expect(states.get('ALT-001')).toMatchObject({alertLogId: logId, alertVersion: 4, approachDirection: 'REAR_LEFT'});
  expect(logs.rows.size).toBe(1);
  expect(logs.updates).toEqual([{alertLogId: logId, status: 'SENT', at: NOW,
    extra: {alertVersion: 4, estimatedArrivalSeconds: 9, expiresAt: '2026-10-12T08:00:20.000Z'}}]);
});

test('update after an acknowledged alert resets acknowledgement', async () => {
  await svc.handleRequested(requested());
  await states.transition('ALT-001', ['SENT'], 'ACKNOWLEDGED', {acknowledgedAt: NOW});
  await svc.handleRequested(requested({decisionVersion: 4, recommendedAction: 'MOVE_RIGHT_WHEN_SAFE'}));
  expect(states.get('ALT-001')).toMatchObject({status: 'SENT', alertVersion: 4, acknowledgedAt: null});
});

test('already expired request is not sent', async () => {
  expect((await svc.handleRequested(requested({expiresAt: NOW}))).outcome).toBe('EXPIRED');
  expect(publisher.frames).toHaveLength(0);
  expect(states.get('ALT-001')).toBeUndefined();
});

test('clear sends CLEARED, transitions, forgets dedup', async () => {
  await svc.handleRequested(requested());
  const logId = states.get('ALT-001')!.alertLogId;
  expect(await svc.handleClearRequested(clear)).toEqual({outcome: 'SENT', alertId: 'ALT-001'});
  expect(publisher.frames[1]!.frame.message).toEqual({type: 'YIELD_ALERT_CLEARED',
    eventType: 'YIELD_ALERT_CLEARED', alertId: 'ALT-001', targetVehicleId: 'VEH-001', reason: 'EMERGENCY_PASSED'});
  expect(states.get('ALT-001')).toMatchObject({status: 'CLEARED', clearedAt: NOW});
  expect(dedup.records.size).toBe(0);
  expect(logs.updates.at(-1)).toEqual({alertLogId: logId, status: 'CLEARED', at: NOW,
    extra: {clearReason: 'EMERGENCY_PASSED'}});
});

test('clear still closes the alert when the vehicle is offline', async () => {
  await svc.handleRequested(requested());
  sessions.sessions.clear();
  expect((await svc.handleClearRequested(clear)).outcome).toBe('NO_SESSION');
  expect(states.get('ALT-001')!.status).toBe('CLEARED');
});

test('clear without active alert is NO_ACTIVE_ALERT', async () => {
  expect((await svc.handleClearRequested(clear)).outcome).toBe('NO_ACTIVE_ALERT');
  expect(publisher.frames).toHaveLength(0);
});

test('clear of an already cleared alert is NO_ACTIVE_ALERT', async () => {
  await svc.handleRequested(requested());
  await svc.handleClearRequested(clear);
  expect((await svc.handleClearRequested(clear)).outcome).toBe('NO_ACTIVE_ALERT');
  expect(publisher.frames).toHaveLength(2);
});

test('re-request after clear starts new episode', async () => {
  await svc.handleRequested(requested());
  const firstLogId = states.get('ALT-001')!.alertLogId;
  await svc.handleClearRequested(clear);
  expect((await svc.handleRequested(requested({decisionVersion: 5}))).outcome).toBe('SENT');
  expect(states.get('ALT-001')).toMatchObject({status: 'SENT', alertVersion: 5, clearedAt: null});
  expect(states.get('ALT-001')!.alertLogId).not.toBe(firstLogId);
  expect(logs.rows.size).toBe(2);
});

test('re-request after FAILED delivery retries as new episode', async () => {
  sessions.sessions.clear();
  await svc.handleRequested(requested());
  sessions.connect('VEH-001');
  expect((await svc.handleRequested(requested({decisionVersion: 4}))).outcome).toBe('SENT');
  expect(logs.rows.size).toBe(2);
});

const later = (ms: number) => new Date(Date.parse(NOW) + ms).toISOString();

test('redelivery after expiresAt still repairs missing history', async () => {
  logs.failNext = 1;
  await expect(svc.handleRequested(requested())).rejects.toThrow('database');
  expect(publisher.frames).toHaveLength(1);
  now = later(30_000);
  expect((await svc.handleRequested(requested())).outcome).toBe('DUPLICATE');
  expect(publisher.frames).toHaveLength(1);
  expect([...logs.rows.values()]).toEqual([expect.objectContaining({alertStatus: 'SENT', alertVersion: 3})]);
});

test('redelivered update repairs the log version', async () => {
  await svc.handleRequested(requested());
  logs.failNext = 1;
  const update = requested({decisionVersion: 4, approachDirection: 'LEFT'});
  await expect(svc.handleRequested(update)).rejects.toThrow('database');
  expect((await svc.handleRequested(update)).outcome).toBe('DUPLICATE');
  expect(publisher.frames).toHaveLength(2);
  expect([...logs.rows.values()][0]!.alertVersion).toBe(4);
});

test('redelivered clear repairs the cleared log', async () => {
  await svc.handleRequested(requested(), meta(NOW));
  logs.failNext = 1;
  await expect(svc.handleClearRequested(clear, meta(later(1000)))).rejects.toThrow('database');
  expect((await svc.handleClearRequested(clear, meta(later(1000)))).outcome).toBe('NO_ACTIVE_ALERT');
  expect([...logs.rows.values()][0]).toMatchObject({alertStatus: 'CLEARED', clearReason: 'EMERGENCY_PASSED'});
  expect(publisher.frames).toHaveLength(2);
});

test('redelivered old clear does not end a newer episode', async () => {
  await svc.handleRequested(requested(), meta(NOW));
  await svc.handleClearRequested(clear, meta(later(1000)));
  await svc.handleRequested(requested({decisionVersion: 5}), meta(later(2000)));
  expect((await svc.handleClearRequested(clear, meta(later(1000)))).outcome).toBe('STALE');
  expect(states.get('ALT-001')).toMatchObject({status: 'SENT', alertVersion: 5});
  expect(publisher.frames).toHaveLength(3);
});

test('an expiry sweep cannot end an update that is being sent', async () => {
  await svc.handleRequested(requested());
  const publish = publisher.publish.bind(publisher);
  let sweepResult: unknown = 'not run';
  publisher.publish = async (gatewayId, frame) => {
    sweepResult = await states.transition('ALT-001', ['SENT', 'ACKNOWLEDGED'], 'EXPIRED', {});
    return publish(gatewayId, frame);
  };
  expect((await svc.handleRequested(requested({decisionVersion: 4, approachDirection: 'LEFT'}))).outcome)
    .toBe('SENT');
  expect(sweepResult).toBeNull();
  expect(states.get('ALT-001')).toMatchObject({status: 'SENT', alertVersion: 4});
});

test('publish failure leaves the alert PENDING and redelivery sends it', async () => {
  const publish = publisher.publish.bind(publisher);
  publisher.publish = async () => { throw new Error('redis down'); };
  await expect(svc.handleRequested(requested())).rejects.toThrow('redis down');
  expect(states.get('ALT-001')!.status).toBe('PENDING');
  publisher.publish = publish;
  expect((await svc.handleRequested(requested())).outcome).toBe('SENT');
  expect(states.get('ALT-001')!.status).toBe('SENT');
  expect(publisher.frames).toHaveLength(1);
});

test('a suppressed request is kept as a refresh with its later expiry', async () => {
  await svc.handleRequested(requested());
  const refresh = requested({decisionVersion: 4, estimatedArrivalSeconds: 12, expiresAt: later(25_000)});
  expect((await svc.handleRequested(refresh)).outcome).toBe('SUPPRESSED');
  expect(states.get('ALT-001')).toMatchObject({alertVersion: 3, refresh});
});

test('state records the deciding event time', async () => {
  await svc.handleRequested(requested(), meta(later(500)));
  expect(states.get('ALT-001')!.decidedAt).toBe(later(500));
});
