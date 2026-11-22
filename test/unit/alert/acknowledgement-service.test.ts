import {AcknowledgementService} from '../../../src/alert/acknowledgement-service';
import {state} from '../../support/fixtures';
import {FakeLogs, FakeStates} from '../../support/fakes';

let states: FakeStates;
let logs: FakeLogs;
let svc: AcknowledgementService;
const ack = {alertId: 'ALT-001', vehicleId: 'VEH-001', alertVersion: 3, receivedAt: '2026-10-12T08:00:01.200Z'};

beforeEach(async () => {
  states = new FakeStates();
  logs = new FakeLogs();
  svc = new AcknowledgementService({states, logs});
  await states.save(state());
});

test('ack for current version transitions to ACKNOWLEDGED with receivedAt', async () => {
  await svc.handleAck(ack);
  expect(states.get('ALT-001')).toMatchObject({status: 'ACKNOWLEDGED', acknowledgedAt: ack.receivedAt});
  expect(logs.updates).toEqual([{alertLogId: state().alertLogId, status: 'ACKNOWLEDGED', at: ack.receivedAt,
    extra: undefined}]);
});

test('ack for older alertVersion is ignored', async () => {
  await svc.handleAck({...ack, alertVersion: 2});
  expect(states.get('ALT-001')!.status).toBe('SENT');
  expect(logs.updates).toHaveLength(0);
});

test('ack from different vehicle is ignored', async () => {
  await svc.handleAck({...ack, vehicleId: 'VEH-999'});
  expect(states.get('ALT-001')!.status).toBe('SENT');
});

test('ack for unknown alert is ignored', async () => {
  await svc.handleAck({...ack, alertId: 'ALT-404'});
  expect(logs.updates).toHaveLength(0);
});

test('duplicate ack is idempotent', async () => {
  await svc.handleAck(ack);
  await svc.handleAck(ack);
  expect(logs.updates).toHaveLength(1);
});

test('ack after clear does not reopen the alert', async () => {
  await states.transition('ALT-001', ['SENT'], 'CLEARED', {});
  await svc.handleAck(ack);
  expect(states.get('ALT-001')!.status).toBe('CLEARED');
});

test('isCurrentVersion compares alertVersion', () => {
  expect(svc.isCurrentVersion(state(), 3)).toBe(true);
  expect(svc.isCurrentVersion(state(), 2)).toBe(false);
});
