import {AlertExpirationScheduler} from '../../../src/alert/alert-expiration-scheduler';
import {AlertMessageFactory} from '../../../src/alert/alert-message-factory';
import {AlertDeliverer} from '../../../src/delivery/alert-deliverer';
import type {AlertState, AlertStatus} from '../../../src/domain/types';
import {state} from '../../support/fixtures';
import {FakeDedup, FakeLogs, FakePublisher, FakeSessions, FakeStates} from '../../support/fakes';

const NOW = new Date('2026-10-12T08:00:30.000Z');
const PAST = '2026-10-12T08:00:20.000Z';
const FUTURE = '2026-10-12T08:01:00.000Z';
let states: FakeStates;
let dedup: FakeDedup;
let sessions: FakeSessions;
let publisher: FakePublisher;
let logs: FakeLogs;
let scheduler: AlertExpirationScheduler;

beforeEach(() => {
  states = new FakeStates();
  dedup = new FakeDedup();
  sessions = new FakeSessions();
  publisher = new FakePublisher();
  logs = new FakeLogs();
  scheduler = new AlertExpirationScheduler({states, dedup, logs, factory: new AlertMessageFactory(),
    deliverer: new AlertDeliverer(sessions, publisher), clock: () => NOW});
  sessions.connect('VEH-001');
  sessions.connect('VEH-002');
});

const seed = async (overrides: Partial<AlertState>) => {
  const value = state(overrides);
  await states.save(value);
  await dedup.remember(value.dispatchId, value.targetVehicleId, 'x');
  return value;
};

test.each<AlertStatus>(['SENT', 'ACKNOWLEDGED'])('expired %s alert becomes EXPIRED and client gets CLEARED', async status => {
  const alert = await seed({status, expiresAt: PAST});
  expect(await scheduler.sweepExpired(NOW)).toBe(1);
  expect(states.get('ALT-001')).toMatchObject({status: 'EXPIRED', clearedAt: NOW.toISOString()});
  expect(publisher.frames.map(entry => entry.frame.message)).toEqual([{type: 'YIELD_ALERT_CLEARED',
    eventType: 'YIELD_ALERT_CLEARED', alertId: 'ALT-001', targetVehicleId: 'VEH-001', reason: 'EXPIRED'}]);
  expect(dedup.records.size).toBe(0);
  expect(logs.updates).toEqual([{alertLogId: alert.alertLogId, status: 'EXPIRED', at: NOW.toISOString(),
    extra: {clearReason: 'EXPIRED'}}]);
});

test('not-yet-expired alert untouched', async () => {
  await seed({expiresAt: FUTURE});
  expect(await scheduler.sweepExpired(NOW)).toBe(0);
  expect(states.get('ALT-001')!.status).toBe('SENT');
  expect(publisher.frames).toHaveLength(0);
});

test('sweep skips cleared alert', async () => {
  await seed({expiresAt: PAST});
  // Simulates a clear that lands between dueForExpiry and the transition.
  const due = states.dueForExpiry.bind(states);
  states.dueForExpiry = async (nowMs: number) => {
    const ids = await due(nowMs);
    await states.transition('ALT-001', ['SENT'], 'CLEARED', {});
    return ids;
  };
  expect(await scheduler.sweepExpired(NOW)).toBe(0);
  expect(states.get('ALT-001')!.status).toBe('CLEARED');
  expect(publisher.frames).toHaveLength(0);
});

test('one failing alert does not stop others', async () => {
  await seed({alertId: 'ALT-001', targetVehicleId: 'VEH-001', expiresAt: PAST});
  await seed({alertId: 'ALT-002', targetVehicleId: 'VEH-002', expiresAt: PAST,
    alertLogId: '00000000-0000-4000-8000-000000000002'});
  const publish = publisher.publish.bind(publisher);
  publisher.publish = async (gatewayId, frame) => {
    if (frame.targetVehicleId === 'VEH-001') throw new Error('publish failed');
    return publish(gatewayId, frame);
  };
  expect(await scheduler.sweepExpired(NOW)).toBe(1);
  expect(states.get('ALT-002')!.status).toBe('EXPIRED');
});

test('start runs sweeps on an interval and stop halts them', async () => {
  await seed({expiresAt: PAST});
  scheduler.start(10);
  await new Promise(resolve => setTimeout(resolve, 50));
  scheduler.stop();
  expect(states.get('ALT-001')!.status).toBe('EXPIRED');
});

test('due entries that cannot expire are unscheduled', async () => {
  states.dueForExpiry = async () => ['ALT-GONE'];
  expect(await scheduler.sweepExpired(NOW)).toBe(0);
  expect(states.unscheduled).toEqual(['ALT-GONE']);
});
